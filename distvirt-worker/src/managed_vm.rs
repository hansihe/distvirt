use std::time::{Duration, SystemTime};

use anyhow::{bail, Context};

use distvirt_guest_protocol::{GuestEvent, GuestMessage, HostMessage, VSOCK_CONTROL_PORT};
use distvirt_worker_protocol::ContainerConfig;

use crate::io_session::IoSession;
use crate::vmm::{NetConfig, SnapshotArtifacts, VmInstance};
use crate::task_handle::TaskHandle;
use crate::vsock_client::{DriverExitSignal, GuestSession};

/// A launched VM with an established yamux session.
///
/// The control stream is strictly request-response — each method sends a
/// command and expects the next message on the control stream to be the
/// corresponding reply. Async events (container exits) arrive on a
/// dedicated event stream, so they never interfere with control traffic.
///
/// **Ordering safety**: this works because guest-init processes one command
/// at a time on a single thread and never sends unsolicited messages on the
/// control stream. If the guest were to send messages out-of-order, replies
/// would be misattributed. The event stream exists precisely to keep async
/// events (container exits) off the control stream.
pub struct ManagedVm<I> {
    instance: I,
    session: GuestSession,
    yamux_driver: Option<TaskHandle<anyhow::Result<()>>>,
    driver_exit_signal: Option<DriverExitSignal>,
    started_containers: Vec<String>,
}

impl<I: VmInstance> ManagedVm<I> {
    /// Create a ManagedVm from an already-launched VM instance.
    ///
    /// Connects to the guest over vsock, establishes a yamux session,
    /// and waits for the Ready message.
    pub async fn connect(instance: I) -> anyhow::Result<Self> {
        log::info!("connecting vsock");
        let stream = instance
            .connect_vsock(VSOCK_CONTROL_PORT)
            .await
            .context("connect vsock")?;

        let (mut session, yamux_driver, driver_exit_signal) = GuestSession::new(stream)
            .await
            .context("establish yamux session")?;

        let msg: GuestMessage = session.recv().await.context("receive Ready")?;
        match msg {
            GuestMessage::Ready { running_containers } => {
                if running_containers.is_empty() {
                    log::info!("guest is ready");
                } else {
                    log::info!("guest is ready (resumed, running containers: {:?})", running_containers);
                }
            }
            other => bail!("expected Ready, got {:?}", other),
        }

        // Accept the event stream — the guest opens it before sending Ready,
        // so it is the first item in the incoming stream queue.
        session.accept_event_stream().await.context("accept event stream")?;

        let mut vm = Self { instance, session, yamux_driver: Some(yamux_driver), driver_exit_signal: Some(driver_exit_signal), started_containers: Vec::new() };
        vm.set_clock().await.context("set guest clock")?;
        Ok(vm)
    }

    /// Take the driver exit signal out of the VM.
    ///
    /// Used by callers (e.g. pod_monitor) that need to select on driver
    /// death concurrently with other VM operations. The `TaskHandle` stays
    /// inside `ManagedVm` so `drain_yamux_driver` always works.
    pub fn take_driver_exit_signal(&mut self) -> Option<DriverExitSignal> {
        self.driver_exit_signal.take()
    }

    /// Set the guest's system clock to the host's current wall-clock time.
    async fn set_clock(&mut self) -> anyhow::Result<()> {
        let now = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .context("system clock before Unix epoch")?;

        self.session
            .send(&HostMessage::SetClock {
                epoch_secs: now.as_secs(),
                epoch_nanos: now.subsec_nanos(),
            })
            .await
            .context("send SetClock")?;

        let msg: GuestMessage = self.session.recv().await.context("receive ClockSet")?;
        match msg {
            GuestMessage::ClockSet => log::info!("guest clock synchronized"),
            GuestMessage::Error { message } => bail!("SetClock failed: {}", message),
            other => bail!("expected ClockSet, got {:?}", other),
        }

        Ok(())
    }

    /// Configure the guest's network interface.
    pub async fn configure_network(
        &mut self,
        interface: &str,
        net_config: &NetConfig,
    ) -> anyhow::Result<()> {
        self.session
            .send(&HostMessage::ConfigureNetwork {
                interface: interface.to_string(),
                ip: net_config.guest_ip.clone(),
                netmask: net_config.netmask.clone(),
                gateway: net_config.gateway.clone(),
            })
            .await
            .context("send ConfigureNetwork")?;

        let msg: GuestMessage = self
            .session
            .recv()
            .await
            .context("receive NetworkConfigured")?;
        match msg {
            GuestMessage::NetworkConfigured => log::info!("guest network configured"),
            GuestMessage::Error { message } => bail!("ConfigureNetwork failed: {}", message),
            other => bail!("expected NetworkConfigured, got {:?}", other),
        }

        Ok(())
    }

    /// Add a container filesystem to the guest.
    pub async fn add_container(
        &mut self,
        id: &str,
        device: &str,
        dns_servers: &[String],
    ) -> anyhow::Result<()> {
        self.session
            .send(&HostMessage::AddContainer {
                id: id.to_string(),
                device: device.to_string(),
                dns_servers: dns_servers.to_vec(),
            })
            .await
            .context("send AddContainer")?;

        let msg: GuestMessage = self.session.recv().await.context("receive ContainerAdded")?;
        match msg {
            GuestMessage::ContainerAdded { id } => log::info!("container added: {}", id),
            GuestMessage::Error { message } => bail!("AddContainer failed: {}", message),
            other => bail!("expected ContainerAdded, got {:?}", other),
        }

        Ok(())
    }

    /// Start a container process inside the guest.
    pub async fn start_container(
        &mut self,
        id: &str,
        config: &ContainerConfig,
    ) -> anyhow::Result<u32> {
        self.session
            .send(&HostMessage::StartContainer {
                id: id.to_string(),
                entrypoint: config.entrypoint.first().cloned().unwrap_or_default(),
                args: {
                    let mut a = config.entrypoint.get(1..).unwrap_or_default().to_vec();
                    a.extend(config.args.iter().cloned());
                    a
                },
                env: config.env.clone(),
                working_dir: config.working_dir.clone(),
                uid: config.uid,
                gid: config.gid,
                hostname: config.hostname.clone(),
                capture_output: config.capture_output,
                stdin: config.stdin,
            })
            .await
            .context("send StartContainer")?;

        let msg: GuestMessage = self
            .session
            .recv()
            .await
            .context("receive ContainerStarted")?;
        match msg {
            GuestMessage::ContainerStarted { id, pid } => {
                log::info!("container {} started with pid {}", id, pid);
                self.started_containers.push(id);
                Ok(pid)
            }
            GuestMessage::Error { message } => bail!("StartContainer failed: {}", message),
            other => bail!("expected ContainerStarted, got {:?}", other),
        }
    }

    /// Send a signal to a running container inside the guest.
    #[allow(dead_code)]
    pub async fn signal_container(&mut self, id: &str, signal: i32) -> anyhow::Result<()> {
        self.session
            .send(&HostMessage::SignalContainer {
                id: id.to_string(),
                signal,
            })
            .await
            .context("send SignalContainer")?;

        let msg: GuestMessage = self
            .session
            .recv()
            .await
            .context("receive ContainerSignaled")?;
        match msg {
            GuestMessage::ContainerSignaled { id } => {
                log::info!("container {} signaled", id);
                Ok(())
            }
            GuestMessage::Error { message } => bail!("SignalContainer failed: {}", message),
            other => bail!("expected ContainerSignaled, got {:?}", other),
        }
    }

    /// Wait for a container to exit.
    pub async fn wait_container_exit(&mut self) -> anyhow::Result<(String, i32)> {
        let event: GuestEvent = self
            .session
            .recv_event()
            .await
            .context("receive ContainerExited event")?;
        match event {
            GuestEvent::ContainerExited { id, code } => {
                log::info!("container {} exited with code {}", id, code);
                self.started_containers.retain(|c| c != &id);
                Ok((id, code))
            }
        }
    }

    /// Accept the next output stream opened by the guest.
    pub async fn accept_output_stream(&mut self) -> anyhow::Result<(String, IoSession)> {
        let (container_id, stream) = self
            .session
            .accept_output_stream()
            .await
            .context("accept output stream")?;
        Ok((container_id, IoSession::new(stream)))
    }

    /// Update the balloon device size (memory to reclaim from guest, in MiB).
    pub async fn set_balloon(&self, amount_mib: u32) -> anyhow::Result<()> {
        self.instance.set_balloon(amount_mib).await
    }

    /// Suspend the VM: handshake with guest, snapshot, then kill.
    ///
    /// Sends `PrepareSuspend` to the guest, waits for `SuspendReady` (with
    /// timeout), takes a Firecracker snapshot, and kills the VM process.
    /// Returns the snapshot artifacts for later restore via `Vmm::restore()`
    /// + `ManagedVm::connect()`.
    pub async fn suspend(
        &mut self,
        snapshot_dir: &std::path::Path,
        timeout: Duration,
    ) -> anyhow::Result<SnapshotArtifacts> {
        // 1. Tell guest to flush output buffers.
        self.session
            .send(&HostMessage::PrepareSuspend)
            .await
            .context("send PrepareSuspend")?;

        // 2. Wait for SuspendReady (with timeout).
        let msg: GuestMessage = tokio::time::timeout(timeout, self.session.recv())
            .await
            .context("timeout waiting for SuspendReady")?
            .context("receive SuspendReady")?;
        match msg {
            GuestMessage::SuspendReady => log::info!("guest is ready for suspend"),
            other => bail!("expected SuspendReady, got {:?}", other),
        }

        // 3. Snapshot the VM (pauses vCPUs, writes files).
        let artifacts = self
            .instance
            .snapshot(snapshot_dir)
            .await
            .context("snapshot VM")?;

        // 4. Kill the VM process.
        self.instance.kill().await.context("kill VM after snapshot")?;

        // 5. Abort the yamux driver now that the VM is dead.
        self.drain_yamux_driver();

        Ok(artifacts)
    }

    /// Gracefully shut down containers, then the VM.
    ///
    /// Sends SIGTERM to each started container, waits for ContainerExited
    /// events up to `timeout`, then sends Shutdown to the guest.
    pub async fn graceful_shutdown(&mut self, timeout: Duration) -> anyhow::Result<()> {
        // Signal all started containers with SIGTERM (best-effort).
        // We fire-and-forget each signal because the VM may already be
        // dying. The 500ms timeout on the ack drain is a compromise: long
        // enough for a healthy guest to respond, short enough to not block
        // shutdown if the guest is unresponsive.
        for id in self.started_containers.clone() {
            let _ = self.session.send(&HostMessage::SignalContainer {
                id,
                signal: libc::SIGTERM,
            }).await;
            // Drain the ContainerSignaled ack (best-effort).
            let _ = tokio::time::timeout(Duration::from_millis(500), self.session.recv::<GuestMessage>()).await;
        }

        // Wait for ContainerExited events on the event stream until all
        // containers are accounted for or timeout.
        let mut remaining = self.started_containers.len();
        if remaining > 0 {
            let deadline = tokio::time::Instant::now() + timeout;
            while remaining > 0 {
                match tokio::time::timeout_at(deadline, self.session.recv_event::<GuestEvent>()).await {
                    Ok(Ok(GuestEvent::ContainerExited { id, code })) => {
                        log::info!("container {} exited with code {} during graceful shutdown", id, code);
                        remaining -= 1;
                    }
                    Ok(Err(e)) => {
                        log::warn!("recv error during graceful shutdown: {:#}", e);
                        break;
                    }
                    Err(_) => {
                        log::warn!("{} container(s) did not exit within timeout", remaining);
                        break;
                    }
                }
            }
        }

        self.shutdown().await
    }

    /// Shut down the guest and wait for the VM to exit.
    pub async fn shutdown(&mut self) -> anyhow::Result<()> {
        self.session
            .send(&HostMessage::Shutdown)
            .await
            .context("send Shutdown")?;
        self.instance.wait().await.context("wait for VM")?;
        // Abort the yamux driver now that the VM is dead.
        self.drain_yamux_driver();
        Ok(())
    }

    /// Forcibly kill the VM process.
    pub async fn force_kill(&mut self) -> anyhow::Result<()> {
        self.instance.kill().await.context("kill VM")?;
        self.drain_yamux_driver();
        Ok(())
    }

    /// Abort the yamux driver task.
    ///
    /// Called after the VM is dead — the underlying socket is gone so
    /// there is nothing useful for the driver to do. Abort immediately
    /// instead of waiting for it to notice the broken socket.
    fn drain_yamux_driver(&mut self) {
        if let Some(driver) = self.yamux_driver.take() {
            driver.abort();
        }
    }
}
