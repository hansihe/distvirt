use std::time::{Duration, SystemTime};

use anyhow::{bail, Context};

use distvirt_guest_protocol::{GuestMessage, HostMessage, VSOCK_CONTROL_PORT};
use distvirt_worker_protocol::ContainerConfig;

use crate::image_provider::containerd::{parse_user_numeric, ImageConfig};
use crate::io_session::IoSession;
use crate::vmm::{NetConfig, SnapshotArtifacts, VmInstance};
use crate::task_handle::TaskHandle;
use crate::vsock_client::GuestSession;

/// Overrides that can be specified on the CLI to override image config.
pub(crate) struct ImageOverrides {
    pub entrypoint: Option<String>,
    pub args: Vec<String>,
    pub env: Vec<String>,
    pub working_dir: Option<String>,
    pub uid: Option<u32>,
    pub gid: Option<u32>,
    pub hostname: Option<String>,
}

/// A launched VM with an established yamux session.
///
/// NOTE: The control protocol is strictly request-response — each method sends
/// a command and expects the next message to be the corresponding reply.
/// Unsolicited async events (e.g. `ContainerExited`) arriving between send and
/// recv will cause the caller to bail. This is unlikely in practice (the guest
/// handles commands synchronously before polling for child exits), but a proper
/// fix would involve separating request-response from async events on the
/// control stream (e.g. tag-based correlation or a dedicated event channel).
pub(crate) struct ManagedVm<I> {
    instance: I,
    session: GuestSession,
    started_containers: Vec<String>,
}

impl<I: VmInstance> ManagedVm<I> {
    /// Create a ManagedVm from an already-launched VM instance.
    ///
    /// Connects to the guest over vsock, establishes a yamux session,
    /// and waits for the Ready message.
    pub async fn connect(instance: I) -> anyhow::Result<(Self, TaskHandle<anyhow::Result<()>>)> {
        log::info!("connecting vsock");
        let stream = instance
            .connect_vsock(VSOCK_CONTROL_PORT)
            .await
            .context("connect vsock")?;

        let (mut session, yamux_driver) = GuestSession::new(stream)
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

        let mut vm = Self { instance, session, started_containers: Vec::new() };
        vm.set_clock().await.context("set guest clock")?;
        Ok((vm, yamux_driver))
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
                entrypoint: config.entrypoint.clone(),
                args: config.args.clone(),
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
        let msg: GuestMessage = self
            .session
            .recv()
            .await
            .context("receive ContainerExited")?;
        match msg {
            GuestMessage::ContainerExited { id, code } => {
                log::info!("container {} exited with code {}", id, code);
                self.started_containers.retain(|c| c != &id);
                Ok((id, code))
            }
            GuestMessage::Error { message } => bail!("container error: {}", message),
            other => bail!("expected ContainerExited, got {:?}", other),
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

        Ok(artifacts)
    }

    /// Gracefully shut down containers, then the VM.
    ///
    /// Sends SIGTERM to each started container, waits for ContainerExited
    /// events up to `timeout`, then sends Shutdown to the guest.
    pub async fn graceful_shutdown(&mut self, timeout: Duration) -> anyhow::Result<()> {
        // Signal all started containers with SIGTERM (best-effort).
        for id in self.started_containers.clone() {
            let _ = self.session.send(&HostMessage::SignalContainer {
                id,
                signal: libc::SIGTERM,
            }).await;
            // Drain the ContainerSignaled ack (best-effort).
            let _ = tokio::time::timeout(Duration::from_millis(500), self.session.recv::<GuestMessage>()).await;
        }

        // Wait for ContainerExited events until all containers are accounted for or timeout.
        let mut remaining = self.started_containers.len();
        if remaining > 0 {
            let deadline = tokio::time::Instant::now() + timeout;
            while remaining > 0 {
                match tokio::time::timeout_at(deadline, self.session.recv::<GuestMessage>()).await {
                    Ok(Ok(GuestMessage::ContainerExited { id, code })) => {
                        log::info!("container {} exited with code {} during graceful shutdown", id, code);
                        remaining -= 1;
                    }
                    Ok(Ok(other)) => {
                        log::debug!("ignoring message during graceful shutdown: {:?}", other);
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
        Ok(())
    }

    /// Forcibly kill the VM process.
    pub async fn force_kill(&mut self) -> anyhow::Result<()> {
        self.instance.kill().await.context("kill VM")
    }
}

/// Merge image config with CLI overrides following OCI entrypoint/cmd resolution rules.
pub(crate) fn merge_config(
    image: &ImageConfig,
    overrides: &ImageOverrides,
) -> anyhow::Result<ContainerConfig> {
    let (entrypoint, args) = if let Some(ref ep) = overrides.entrypoint {
        (ep.clone(), overrides.args.clone())
    } else if !image.entrypoint.is_empty() {
        let args = if !overrides.args.is_empty() {
            overrides.args.clone()
        } else {
            image.cmd.clone()
        };
        (image.entrypoint[0].clone(), {
            let mut a: Vec<String> = image.entrypoint[1..].to_vec();
            a.extend(args);
            a
        })
    } else if !overrides.args.is_empty() {
        // No entrypoint override and no image entrypoint, but override args provided.
        // Treat override args as the full command (compose `command:` replaces CMD).
        (overrides.args[0].clone(), overrides.args[1..].to_vec())
    } else if !image.cmd.is_empty() {
        (image.cmd[0].clone(), image.cmd[1..].to_vec())
    } else {
        bail!("image has no entrypoint or cmd, and none was specified on the command line");
    };

    let mut env = image.env.clone();
    env.extend(overrides.env.iter().cloned());

    let (img_uid, img_gid) = image
        .user
        .as_deref()
        .map(parse_user_numeric)
        .transpose()?
        .unwrap_or((None, None));

    Ok(ContainerConfig {
        entrypoint,
        args,
        env,
        working_dir: overrides.working_dir.clone().or_else(|| image.working_dir.clone()),
        uid: overrides.uid.or(img_uid),
        gid: overrides.gid.or(img_gid),
        hostname: overrides.hostname.clone(),
        capture_output: false,
        stdin: false,
    })
}
