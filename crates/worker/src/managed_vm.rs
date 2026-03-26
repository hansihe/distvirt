use std::collections::HashMap;
use std::process::ExitStatus;
use std::time::{Duration, SystemTime};

use anyhow::{Context, bail};
use tokio::sync::watch;

use distvirt_guest_protocol::{GuestEvent, GuestMessage, HostMessage, VSOCK_CONTROL_PORT};
use distvirt_worker_protocol::ContainerConfig;

use crate::io_session::IoSession;
use crate::task_handle::TaskHandle;
use crate::vmm::{NetConfig, SnapshotArtifacts, VmInstance};
use crate::vsock_client::{DriverExitSignal, GuestEventStream, GuestSession};

// ---------------------------------------------------------------------------
// EventDispatch — single background consumer of the guest event stream
// ---------------------------------------------------------------------------

/// Accumulated state from guest events. Updated by the background dispatch task.
#[derive(Clone, Debug, Default)]
pub struct EventDispatchState {
    /// Containers that have exited, with their exit codes.
    pub exited: HashMap<String, i32>,
    /// Total output bytes dropped across all containers (buffer full during
    /// final drain). Non-zero means some container output was lost.
    pub output_bytes_dropped: u64,
    /// Latest balloon size requested by the guest (last-value-wins).
    pub balloon_mib: Option<u32>,
    /// Fatal task error from guest-init. Set once, never cleared.
    pub task_error: Option<(String, String)>,
    /// Set to true when the event stream closes or errors.
    pub stream_closed: bool,
    /// If the stream closed with an error, the message.
    pub stream_error: Option<String>,
    /// Whether the guest is currently memory-constrained.
    pub memory_constrained: bool,
    /// The reason for memory constraint, if any.
    pub memory_constraint_reason: Option<distvirt_guest_protocol::ConstraintReason>,
    /// Total OOM kills observed.
    pub oom_kill_count: u64,
}

/// Owns the background task that reads the guest event stream and publishes
/// state via a `watch` channel.
pub struct EventDispatch {
    state_rx: watch::Receiver<EventDispatchState>,
    _task: TaskHandle<()>,
}

impl EventDispatch {
    /// Spawn the background dispatch task that consumes the event stream.
    pub fn spawn(mut stream: GuestEventStream) -> Self {
        let (state_tx, state_rx) = watch::channel(EventDispatchState::default());

        let task = TaskHandle::spawn(async move {
            loop {
                match stream.next().await {
                    Ok(Some(GuestEvent::ContainerExited { id, code, output_bytes_dropped })) => {
                        log::info!("EventDispatch: container {} exited with code {}", id, code);
                        if output_bytes_dropped > 0 {
                            log::warn!(
                                "EventDispatch: container {} dropped {} bytes of output",
                                id, output_bytes_dropped
                            );
                        }
                        state_tx.send_modify(|s| {
                            s.exited.insert(id, code);
                            s.output_bytes_dropped += output_bytes_dropped;
                        });
                    }
                    Ok(Some(GuestEvent::BalloonSet { amount_mib })) => {
                        state_tx.send_modify(|s| {
                            s.balloon_mib = Some(amount_mib);
                        });
                    }
                    Ok(Some(GuestEvent::TaskError { task, message })) => {
                        log::error!(
                            "EventDispatch: task error: task={}, message={}",
                            task,
                            message
                        );
                        state_tx.send_modify(|s| {
                            if s.task_error.is_none() {
                                s.task_error = Some((task, message));
                            }
                        });
                    }
                    Ok(Some(GuestEvent::MemoryConstrained { reason })) => {
                        log::warn!("EventDispatch: memory constrained: {:?}", reason);
                        state_tx.send_modify(|s| {
                            s.memory_constrained = true;
                            s.memory_constraint_reason = Some(reason);
                        });
                    }
                    Ok(Some(GuestEvent::MemoryConstraintCleared)) => {
                        log::info!("EventDispatch: memory constraint cleared");
                        state_tx.send_modify(|s| {
                            s.memory_constrained = false;
                            s.memory_constraint_reason = None;
                        });
                    }
                    Ok(Some(GuestEvent::OomKill { count })) => {
                        log::warn!("EventDispatch: OOM kill, {} process(es) killed", count);
                        state_tx.send_modify(|s| {
                            s.oom_kill_count += count;
                        });
                    }
                    Ok(None) => {
                        log::info!("EventDispatch: event stream closed cleanly");
                        state_tx.send_modify(|s| {
                            s.stream_closed = true;
                        });
                        break;
                    }
                    Err(e) => {
                        let msg = format!("{:#}", e);
                        log::error!("EventDispatch: event stream error: {}", msg);
                        state_tx.send_modify(|s| {
                            s.stream_closed = true;
                            s.stream_error = Some(msg);
                        });
                        break;
                    }
                }
            }
        });

        Self {
            state_rx,
            _task: task,
        }
    }

    /// Get an independent receiver for watching state changes.
    pub fn subscribe(&self) -> watch::Receiver<EventDispatchState> {
        self.state_rx.clone()
    }

    /// Read current state without affecting any receiver's seen marker.
    #[allow(dead_code)]
    pub fn borrow(&self) -> watch::Ref<'_, EventDispatchState> {
        self.state_rx.borrow()
    }
}

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
    event_dispatch: Option<EventDispatch>,
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
        let started_containers = match msg {
            GuestMessage::Ready {
                running_containers,
                pre_config_responses,
            } => {
                if running_containers.is_empty() {
                    log::info!("guest is ready");
                } else {
                    log::info!(
                        "guest is ready (resumed, running containers: {:?})",
                        running_containers
                    );
                }
                if !pre_config_responses.is_empty() {
                    log::info!(
                        "guest executed {} config drive command(s): {:?}",
                        pre_config_responses.len(),
                        pre_config_responses
                    );
                }
                running_containers
            }
            other => bail!("expected Ready, got {:?}", other),
        };

        // Accept the event stream — the guest opens it before sending Ready,
        // so it is the first item in the incoming stream queue.
        session
            .accept_event_stream()
            .await
            .context("accept event stream")?;

        // Take the event stream and spawn the background dispatch task.
        let event_stream = session
            .take_event_stream()
            .context("event stream not available after accept")?;
        let event_dispatch = EventDispatch::spawn(event_stream);

        let mut vm = Self {
            instance,
            session,
            yamux_driver: Some(yamux_driver),
            driver_exit_signal: Some(driver_exit_signal),
            started_containers,
            event_dispatch: Some(event_dispatch),
        };
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

    /// Take the event dispatch out of the VM.
    ///
    /// Used by callers (e.g. pod_monitor) that need to watch guest event
    /// state independently. The background task continues running.
    pub fn take_event_dispatch(&mut self) -> Option<EventDispatch> {
        self.event_dispatch.take()
    }

    /// Take the VM process exit signal.
    ///
    /// Used by callers (e.g. pod_monitor) that need to `select!` on
    /// unexpected VM process death.
    pub fn take_exit_signal(&mut self) -> Option<watch::Receiver<Option<ExitStatus>>> {
        self.instance.take_exit_signal()
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

    /// Mount a volume in the guest.
    pub async fn mount_volume(
        &mut self,
        name: &str,
        source: distvirt_guest_protocol::VolumeSource,
        read_only: bool,
    ) -> anyhow::Result<()> {
        self.session
            .send(&HostMessage::MountVolume {
                name: name.to_string(),
                source,
                read_only,
            })
            .await
            .context("send MountVolume")?;

        let msg: GuestMessage = self
            .session
            .recv()
            .await
            .context("receive VolumeMounted")?;
        match msg {
            GuestMessage::VolumeMounted { name } => log::info!("volume mounted: {}", name),
            GuestMessage::Error { message } => bail!("MountVolume failed: {}", message),
            other => bail!("expected VolumeMounted, got {:?}", other),
        }

        Ok(())
    }

    /// Add a container filesystem to the guest.
    pub async fn add_container(
        &mut self,
        id: &str,
        rootfs: distvirt_guest_protocol::ContainerRootfs,
        dns_servers: &[String],
        volume_mounts: Vec<distvirt_guest_protocol::VolumeMount>,
    ) -> anyhow::Result<()> {
        self.session
            .send(&HostMessage::AddContainer {
                id: id.to_string(),
                rootfs,
                dns_servers: dns_servers.to_vec(),
                volume_mounts,
            })
            .await
            .context("send AddContainer")?;

        let msg: GuestMessage = self
            .session
            .recv()
            .await
            .context("receive ContainerAdded")?;
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
        uid: Option<u32>,
        gid: Option<u32>,
    ) -> anyhow::Result<u32> {
        self.session
            .send(&HostMessage::StartContainer {
                id: id.to_string(),
                argv: {
                    let command = config.command.as_deref().unwrap_or(&[]);
                    let args = config.args.as_deref().unwrap_or(&[]);
                    let mut argv = Vec::with_capacity(command.len() + args.len());
                    argv.extend(command.iter().cloned());
                    argv.extend(args.iter().cloned());
                    argv
                },
                env: config.env.clone(),
                working_dir: config.working_dir.clone(),
                uid,
                gid,
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
    pub async fn set_balloon(&mut self, amount_mib: u32) -> anyhow::Result<()> {
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
        self.instance
            .kill()
            .await
            .context("kill VM after snapshot")?;

        // 5. Abort the yamux driver now that the VM is dead.
        self.drain_yamux_driver();

        Ok(artifacts)
    }

    /// Send SIGTERM to all started containers and wait for them to exit.
    ///
    /// Best-effort: if containers don't exit within `timeout`, logs a warning
    /// and returns. Does **not** send `Shutdown` to the guest or kill the VM.
    pub async fn stop_containers(
        &mut self,
        timeout: Duration,
        rx: &mut watch::Receiver<EventDispatchState>,
    ) {
        // Signal all started containers with SIGTERM (best-effort).
        for id in self.started_containers.clone() {
            let _ = self
                .session
                .send(&HostMessage::SignalContainer {
                    id,
                    signal: libc::SIGTERM,
                })
                .await;
            // Drain the ContainerSignaled ack (best-effort).
            let _ = tokio::time::timeout(
                Duration::from_millis(500),
                self.session.recv::<GuestMessage>(),
            )
            .await;
        }

        // Wait until all started containers appear in the exited map.
        let started = self.started_containers.clone();
        if !started.is_empty() {
            // Map the result to a simple enum so we drop the watch::Ref
            // before potentially re-borrowing rx.
            let timed_out = {
                let result = tokio::time::timeout(
                    timeout,
                    rx.wait_for(|s| started.iter().all(|c| s.exited.contains_key(c))),
                )
                .await;
                match result {
                    Ok(Ok(_ref)) => {
                        drop(_ref);
                        false
                    }
                    Ok(Err(_)) => {
                        log::warn!("event dispatch closed during stop_containers");
                        false
                    }
                    Err(_) => true,
                }
            };
            if !timed_out {
                log::info!("all containers exited during stop_containers");
            } else {
                let state = rx.borrow();
                let remaining: Vec<_> = started
                    .iter()
                    .filter(|c| !state.exited.contains_key(*c))
                    .collect();
                log::warn!(
                    "{} container(s) did not exit within timeout: {:?}",
                    remaining.len(),
                    remaining
                );
            }
        }
    }

    /// Gracefully shut down containers, then the VM.
    ///
    /// Sends SIGTERM to each started container, waits for the `EventDispatch`
    /// state to show all containers exited (up to `timeout`), then sends
    /// Shutdown to the guest.
    ///
    /// The caller passes its own `watch::Receiver` (obtained from
    /// `EventDispatch::subscribe()`). This avoids any event-stream race:
    /// the dispatch task updates state concurrently, and `wait_for` holds
    /// the borrow across predicate check and re-registration.
    pub async fn graceful_shutdown(
        &mut self,
        timeout: Duration,
        rx: &mut watch::Receiver<EventDispatchState>,
    ) -> anyhow::Result<()> {
        self.stop_containers(timeout, rx).await;
        self.shutdown().await
    }

    /// Shut down the guest and wait for the VM to exit.
    pub async fn shutdown(&mut self) -> anyhow::Result<()> {
        self.session
            .send(&HostMessage::Shutdown)
            .await
            .context("send Shutdown")?;
        let _status = self.instance.wait().await.context("wait for VM")?;
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
    pub fn drain_yamux_driver(&mut self) {
        if let Some(driver) = self.yamux_driver.take() {
            driver.abort();
        }
    }
}
