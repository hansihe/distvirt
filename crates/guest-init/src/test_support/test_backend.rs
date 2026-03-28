//! Channel-based `ContainerBackend` for testing.
//!
//! No real processes, no filesystem operations. The test harness controls
//! container behavior through [`BackendHandle`] and per-container
//! [`ContainerHandle`]s.

use std::collections::HashMap;
use std::os::unix::io::OwnedFd;

use distvirt_guest_protocol::VolumeMount;

use crate::container::backend::{ContainerBackend, ContainerExit, ContainerStartConfig};
use crate::spawner::LocalSpawner;

/// Per-container state in the test backend.
struct TestContainer {
    id: String,
    running: bool,
    /// Output channel sender — test harness pushes encoded output chunks here.
    /// This is the same sender that was given to ContainerManager for the
    /// OutputBuffer, so chunks injected here flow through the real drain pipeline.
    output_tx: Option<async_channel::Sender<Vec<u8>>>,
    /// Mock pid (incrementing counter).
    pid: u32,
    /// Signal log — records (signal, timestamp) for test assertions.
    signals: Vec<i32>,
}

/// Handle for controlling all containers in a test backend instance.
///
/// Returned by `TestContainerBackend::new()`. The test harness uses this to:
/// - Trigger container exits
/// - Query container state for assertions
pub struct BackendHandle {
    exit_tx: async_channel::Sender<ContainerExit>,
}

impl BackendHandle {
    /// Trigger a container exit. The supervisor will receive this through
    /// the exit channel and process it normally.
    pub async fn trigger_exit(&self, id: &str, code: i32) {
        self.exit_tx
            .send(ContainerExit {
                id: id.to_string(),
                code,
                output_bytes_dropped: 0,
            })
            .await
            .expect("exit channel closed");
    }
}

/// Handle for controlling a specific container.
///
/// Returned when the supervisor starts a container. The test harness can
/// inject output and observe signals.
pub struct ContainerHandle {
    /// Output channel sender for injecting pre-encoded output chunks.
    pub output_tx: Option<async_channel::Sender<Vec<u8>>>,
}

/// Channel-based `ContainerBackend` for testing.
///
/// All operations are no-ops or channel-based. No real processes, no
/// filesystem operations. The test harness controls container behavior
/// through `BackendHandle` and `ContainerHandle`.
pub struct TestContainerBackend {
    containers: HashMap<String, TestContainer>,
    exit_tx: async_channel::Sender<ContainerExit>,
    exit_rx: async_channel::Receiver<ContainerExit>,
    next_pid: u32,
    /// Handles for started containers, retrievable by the test harness.
    container_handles: HashMap<String, ContainerHandle>,
}

impl TestContainerBackend {
    /// Create a new test backend and its control handle.
    pub fn new() -> (Self, BackendHandle) {
        let (exit_tx, exit_rx) = async_channel::unbounded();
        let backend = TestContainerBackend {
            containers: HashMap::new(),
            exit_tx: exit_tx.clone(),
            exit_rx,
            next_pid: 1000,
            container_handles: HashMap::new(),
        };
        let handle = BackendHandle { exit_tx };
        (backend, handle)
    }

    /// Get a container handle for injecting output etc.
    /// Available after the container is started.
    pub fn container_handle(&self, id: &str) -> Option<&ContainerHandle> {
        self.container_handles.get(id)
    }

    /// Get the signal log for a container (for test assertions).
    pub fn signals(&self, id: &str) -> Option<&[i32]> {
        self.containers.get(id).map(|c| c.signals.as_slice())
    }

    /// Snapshot all container states for suspend/clone.
    pub fn snapshot_containers(&self) -> Vec<TestContainerSnapshot> {
        self.containers
            .values()
            .map(|c| TestContainerSnapshot {
                id: c.id.clone(),
                running: c.running,
                pid: c.pid,
            })
            .collect()
    }

    /// Current next_pid value (for restoring the counter).
    pub fn next_pid(&self) -> u32 {
        self.next_pid
    }

    /// Reconstruct a TestContainerBackend from snapshot state.
    pub fn new_from_snapshot(
        container_snapshots: &[TestContainerSnapshot],
        next_pid: u32,
        output_txs: HashMap<String, async_channel::Sender<Vec<u8>>>,
    ) -> (Self, BackendHandle) {
        let (exit_tx, exit_rx) = async_channel::unbounded();
        let mut containers = HashMap::new();
        let mut container_handles = HashMap::new();

        for snap in container_snapshots {
            let output_tx = output_txs.get(&snap.id).cloned();
            containers.insert(
                snap.id.clone(),
                TestContainer {
                    id: snap.id.clone(),
                    running: snap.running,
                    output_tx: output_tx.clone(),
                    pid: snap.pid,
                    signals: Vec::new(),
                },
            );
            container_handles.insert(
                snap.id.clone(),
                ContainerHandle { output_tx },
            );
        }

        let backend = TestContainerBackend {
            containers,
            exit_tx: exit_tx.clone(),
            exit_rx,
            next_pid,
            container_handles,
        };
        let handle = BackendHandle { exit_tx };
        (backend, handle)
    }
}

/// Snapshot of a single test container's state.
#[derive(Clone)]
pub struct TestContainerSnapshot {
    pub id: String,
    pub running: bool,
    pub pid: u32,
}

impl ContainerBackend for TestContainerBackend {
    fn add(
        &mut self,
        id: &str,
        _rootfs: &distvirt_guest_protocol::ContainerRootfs,
        _dns_servers: &[String],
        _volume_mounts: &[VolumeMount],
    ) -> anyhow::Result<()> {
        log::info!("[test-backend] add container {}", id);
        // No-op: no filesystem to set up.
        Ok(())
    }

    fn start<S: LocalSpawner>(
        &mut self,
        id: &str,
        _config: &ContainerStartConfig,
        output_tx: Option<async_channel::Sender<Vec<u8>>>,
        _spawner: &S,
    ) -> anyhow::Result<u32> {
        let pid = self.next_pid;
        self.next_pid += 1;

        log::info!("[test-backend] start container {} (pid={})", id, pid);

        // Store a container handle for the test harness.
        self.container_handles.insert(
            id.to_string(),
            ContainerHandle {
                output_tx: output_tx.clone(),
            },
        );

        self.containers.insert(
            id.to_string(),
            TestContainer {
                id: id.to_string(),
                running: true,
                output_tx,
                pid,
                signals: Vec::new(),
            },
        );

        Ok(pid)
    }

    fn signal(&mut self, id: &str, signal: i32) -> anyhow::Result<()> {
        log::info!("[test-backend] signal container {} with {}", id, signal);
        let container = self
            .containers
            .get_mut(id)
            .ok_or_else(|| anyhow::anyhow!("container {} not found", id))?;
        container.signals.push(signal);
        // Simulate immediate exit on SIGTERM/SIGKILL.
        if signal == libc::SIGTERM || signal == libc::SIGKILL {
            let _ = self.exit_tx.try_send(ContainerExit {
                id: id.to_string(),
                code: 128 + signal,
                output_bytes_dropped: 0,
            });
        }
        Ok(())
    }

    fn signal_all_running(&mut self, signal: i32) {
        let running: Vec<String> = self
            .containers
            .values()
            .filter(|c| c.running)
            .map(|c| c.id.clone())
            .collect();
        log::info!(
            "[test-backend] signal_all_running({}) to {:?}",
            signal,
            running
        );
        for id in &running {
            if let Some(c) = self.containers.get_mut(id) {
                c.signals.push(signal);
            }
        }
        // Simulate immediate exit on SIGTERM/SIGKILL.
        if signal == libc::SIGTERM || signal == libc::SIGKILL {
            for id in &running {
                let _ = self.exit_tx.try_send(ContainerExit {
                    id: id.clone(),
                    code: 128 + signal,
                    output_bytes_dropped: 0,
                });
            }
        }
    }

    fn has_running_containers(&self) -> bool {
        self.containers.values().any(|c| c.running)
    }

    fn running_container_ids(&self) -> Vec<String> {
        self.containers
            .values()
            .filter(|c| c.running)
            .map(|c| c.id.clone())
            .collect()
    }

    fn dup_stdin_fd(&self, _id: &str) -> Option<OwnedFd> {
        // Test backend doesn't support stdin pipes (no real fds).
        // The supervisor will skip stdin relay for this container.
        None
    }

    fn mark_exited(&mut self, id: &str) {
        if let Some(c) = self.containers.get_mut(id) {
            log::info!("[test-backend] mark_exited container {}", id);
            c.running = false;
        }
    }

    fn remove(&mut self, id: &str) {
        log::info!("[test-backend] remove container {}", id);
        self.containers.remove(id);
        self.container_handles.remove(id);
    }

    fn exit_receiver(&self) -> async_channel::Receiver<ContainerExit> {
        self.exit_rx.clone()
    }
}
