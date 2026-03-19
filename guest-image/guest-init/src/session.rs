use std::pin::Pin;
use std::task::Poll;

use anyhow::Context;
use async_executor::LocalExecutor;
use async_io::Async;
use futures::io::AsyncRead;

use crate::transport::TransportListener;
use crate::yamux_driver::YamuxHandle;
use crate::{net, util, vsock};
use distvirt_guest_protocol::{GuestMessage, HostMessage, StreamHeader};

/// Mount a volume device at /volumes/<name>.
fn mount_volume(name: &str, device: &str, read_only: bool) -> anyhow::Result<()> {
    let mount_point = format!("/volumes/{}", name);
    let flags = if read_only {
        libc::MS_RDONLY as libc::c_ulong
    } else {
        0
    };
    util::mount(device, &mount_point, "ext4", flags, None)?;
    log::info!("mounted volume '{}' at {}", name, mount_point);
    Ok(())
}

/// Result of executing a host command.
pub enum CommandResult {
    /// A response message to send back on the control stream.
    Response(GuestMessage),
    /// Guest should prepare for suspend and signal ready.
    PrepareSuspend,
    /// Guest should shut down.
    Shutdown,
}

/// Execute a host command without requiring a yamux connection.
///
/// This is the unified command handler used by both the vsock event loop and
/// the config drive. Commands that previously needed yamux (like StartContainer
/// opening output streams) now just set up local buffers and fill tasks.
pub fn execute_command(
    cmd: HostMessage,
    containers: &mut crate::container::ContainerManager,
    ex: &LocalExecutor<'_>,
) -> CommandResult {
    match cmd {
        HostMessage::MountVolume {
            name,
            device,
            read_only,
        } => {
            log::info!("MountVolume: name={}, device={}, read_only={}", name, device, read_only);
            match mount_volume(&name, &device, read_only) {
                Ok(()) => CommandResult::Response(GuestMessage::VolumeMounted { name }),
                Err(e) => {
                    log::error!("MountVolume failed: {:#}", e);
                    CommandResult::Response(GuestMessage::Error {
                        message: format!("{:#}", e),
                    })
                }
            }
        }
        HostMessage::AddContainer {
            id,
            device,
            dns_servers,
            volume_mounts,
        } => {
            log::info!("AddContainer: id={}, device={}", id, device);
            match containers.add(id.clone(), device, &dns_servers, &volume_mounts) {
                Ok(()) => CommandResult::Response(GuestMessage::ContainerAdded { id }),
                Err(e) => {
                    log::error!("AddContainer failed: {:#}", e);
                    CommandResult::Response(GuestMessage::Error {
                        message: format!("{:#}", e),
                    })
                }
            }
        }
        HostMessage::StartContainer {
            id,
            entrypoint,
            args,
            env,
            working_dir,
            uid,
            gid,
            hostname,
            capture_output,
            stdin,
        } => {
            log::info!(
                "StartContainer: id={}, entrypoint={}, capture_output={}, stdin={}",
                id,
                entrypoint,
                capture_output,
                stdin
            );
            match containers.start(
                &id,
                &entrypoint,
                &args,
                &env,
                working_dir.as_deref(),
                uid,
                gid,
                hostname.as_deref(),
                capture_output,
                stdin,
                ex,
            ) {
                Ok(pid) => CommandResult::Response(GuestMessage::ContainerStarted { id, pid }),
                Err(e) => {
                    log::error!("StartContainer failed: {:#}", e);
                    CommandResult::Response(GuestMessage::Error {
                        message: format!("{:#}", e),
                    })
                }
            }
        }
        HostMessage::ConfigureNetwork {
            interface,
            ip,
            netmask,
            gateway,
        } => {
            log::info!(
                "ConfigureNetwork: {}={}, netmask={}, gw={}",
                interface,
                ip,
                netmask,
                gateway
            );
            match net::configure_network(&interface, &ip, &netmask, &gateway) {
                Ok(()) => CommandResult::Response(GuestMessage::NetworkConfigured),
                Err(e) => {
                    log::error!("ConfigureNetwork failed: {:#}", e);
                    CommandResult::Response(GuestMessage::Error {
                        message: format!("{:#}", e),
                    })
                }
            }
        }
        HostMessage::SignalContainer { id, signal } => {
            log::info!("SignalContainer: id={}, signal={}", id, signal);
            match containers.signal_container(&id, signal) {
                Ok(()) => CommandResult::Response(GuestMessage::ContainerSignaled { id }),
                Err(e) => {
                    log::error!("SignalContainer failed: {:#}", e);
                    CommandResult::Response(GuestMessage::Error {
                        message: format!("{:#}", e),
                    })
                }
            }
        }
        HostMessage::SetClock {
            epoch_secs,
            epoch_nanos,
        } => {
            log::info!(
                "SetClock: epoch_secs={}, epoch_nanos={}",
                epoch_secs,
                epoch_nanos
            );
            let ts = libc::timespec {
                tv_sec: epoch_secs as libc::time_t,
                tv_nsec: epoch_nanos as libc::c_long,
            };
            let ret = unsafe { libc::clock_settime(libc::CLOCK_REALTIME, &ts) };
            if ret == 0 {
                log::info!("system clock set successfully");
                CommandResult::Response(GuestMessage::ClockSet)
            } else {
                let e = std::io::Error::last_os_error();
                log::error!("clock_settime failed: {}", e);
                CommandResult::Response(GuestMessage::Error {
                    message: format!("clock_settime failed: {}", e),
                })
            }
        }
        HostMessage::PrepareSuspend => {
            log::info!("PrepareSuspend received");
            // Install a plug qdisc to buffer outbound packets in the kernel.
            if let Err(e) = net::suspend() {
                log::warn!("failed to install plug qdisc: {:#}", e);
            }
            CommandResult::PrepareSuspend
        }
        HostMessage::Shutdown => {
            log::info!("shutdown requested");
            CommandResult::Shutdown
        }
    }
}

/// Buffered reader for the yamux control stream.
///
/// Accumulates bytes from `poll_read` and yields complete length-prefixed JSON
/// messages. Safe to use inside a droppable select arm because partial read
/// progress is preserved in the struct, not in stack temporaries.
pub struct ControlReader {
    stream: yamux::Stream,
    buf: Vec<u8>,
}

impl ControlReader {
    pub fn new(stream: yamux::Stream) -> Self {
        ControlReader {
            stream,
            buf: Vec::new(),
        }
    }

    /// Poll for a complete length-prefixed JSON message.
    pub fn poll_recv<T: serde::de::DeserializeOwned>(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<anyhow::Result<T>> {
        // Read whatever is available from the yamux stream.
        loop {
            let mut tmp = [0u8; 8192];
            match Pin::new(&mut self.stream).poll_read(cx, &mut tmp) {
                Poll::Ready(Ok(0)) => {
                    return Poll::Ready(Err(anyhow::anyhow!("control stream EOF")));
                }
                Poll::Ready(Ok(n)) => {
                    self.buf.extend_from_slice(&tmp[..n]);
                }
                Poll::Ready(Err(e)) => {
                    return Poll::Ready(Err(anyhow::Error::from(e).context("read control stream")));
                }
                Poll::Pending => break,
            }
        }

        // Check for a complete message.
        if self.buf.len() < 4 {
            return Poll::Pending;
        }
        let len = u32::from_le_bytes(self.buf[..4].try_into().unwrap()) as usize;
        if len > 1024 * 1024 {
            return Poll::Ready(Err(anyhow::anyhow!("message too large: {} bytes", len)));
        }
        if self.buf.len() < 4 + len {
            return Poll::Pending;
        }

        let result = serde_json::from_slice(&self.buf[4..4 + len]).context("deserialize message");
        self.buf.drain(..4 + len);
        Poll::Ready(result)
    }

    /// Send a length-prefixed JSON message on the control stream.
    pub async fn send<T: serde::Serialize>(&mut self, msg: &T) -> anyhow::Result<()> {
        vsock::send_msg(&mut self.stream, msg).await
    }
}

/// Why the inner event loop exited.
pub enum LoopExit {
    /// Host sent Shutdown — kill containers and reboot.
    Shutdown,
    /// Yamux connection lost — wait for reconnect (suspend/resume path).
    Disconnected,
}

/// Result of the 3-phase yamux handshake.
pub struct Session {
    pub handle: YamuxHandle,
    pub yamux_task: async_executor::Task<()>,
    pub control: ControlReader,
    pub event_stream: yamux::Stream,
}

impl Session {
    /// Accept a transport connection and perform the 3-phase yamux handshake.
    ///
    /// 1. Accept the control inbound stream and read its StreamHeader::Control.
    /// 2. Open an outbound event stream.
    /// 3. Send StreamHeader::Events on the event stream and GuestMessage::Ready on control.
    pub async fn connect(
        listener: &TransportListener,
        running_containers: Vec<String>,
        pre_config_responses: &[GuestMessage],
        ex: &LocalExecutor<'_>,
    ) -> anyhow::Result<Session> {
        let accepted = listener.accept().await?;
        let async_socket = Async::new(accepted).context("wrap transport fd in Async")?;

        let conn =
            yamux::Connection::new(async_socket, yamux::Config::default(), yamux::Mode::Server);

        let (handle, yamux_task) = YamuxHandle::spawn(conn, ex);

        // Phase 1: Accept the control inbound stream.
        let mut control_stream = handle
            .next_inbound()
            .await
            .ok_or_else(|| anyhow::anyhow!("yamux closed before control stream"))?;

        let header: StreamHeader = vsock::recv_msg(&mut control_stream)
            .await
            .context("read StreamHeader on control stream")?;
        match header {
            StreamHeader::Control => log::info!("control stream established"),
            other => anyhow::bail!("expected StreamHeader::Control, got {:?}", other),
        }

        // Phase 2: Open event stream outbound.
        let mut event_stream = handle
            .open_stream()
            .await
            .context("open yamux event stream")?;

        // Phase 3: Send event stream header and Ready.
        vsock::send_msg(&mut event_stream, &StreamHeader::Events)
            .await
            .context("send StreamHeader::Events")?;
        log::info!("event stream opened");

        if running_containers.is_empty() {
            log::info!("host connected, sending Ready (cold boot)");
        } else {
            log::info!(
                "host reconnected, sending Ready (resume, running: {:?})",
                running_containers
            );
        }
        vsock::send_msg(
            &mut control_stream,
            &GuestMessage::Ready {
                running_containers,
                pre_config_responses: pre_config_responses.to_vec(),
            },
        )
        .await?;

        let control = ControlReader::new(control_stream);

        Ok(Session {
            handle,
            yamux_task,
            control,
            event_stream,
        })
    }
}
