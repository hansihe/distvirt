use std::collections::HashMap;
use std::os::unix::io::OwnedFd;

use anyhow::{Context, bail};

use super::backend::{ContainerBackend, ContainerStartConfig};
use crate::spawner::LocalSpawner;
use crate::buffer::OutputBuffer;

/// Per-container state tracked by the manager.
struct Container {
    id: String,
    /// Per-container output buffer (when capture_output is enabled).
    /// Chunks are produced by the backend's fill task and consumed by a
    /// per-connection drain task.
    output_buffer: Option<OutputBuffer>,
}

/// Manages container state and delegates OS operations to a backend.
///
/// Tracks which containers exist, their lifecycle state (added/running/exited),
/// and their output buffers. All OS-specific operations (mount, process spawning,
/// signal delivery, cleanup) are handled by the backend.
pub struct ContainerManager<B: ContainerBackend> {
    backend: B,
    containers: HashMap<String, Container>,
}

impl<B: ContainerBackend> ContainerManager<B> {
    pub fn new(backend: B) -> Self {
        ContainerManager {
            backend,
            containers: HashMap::new(),
        }
    }

    /// Set up the container rootfs at /containers/<id>, write resolv.conf,
    /// and bind-mount any volume mounts into the container rootfs.
    pub fn add(
        &mut self,
        id: String,
        rootfs: distvirt_guest_protocol::ContainerRootfs,
        dns_servers: &[String],
        volume_mounts: &[distvirt_guest_protocol::VolumeMount],
    ) -> anyhow::Result<()> {
        if self.containers.contains_key(&id) {
            bail!("container {} already exists", id);
        }

        self.backend.add(&id, &rootfs, dns_servers, volume_mounts)?;

        self.containers.insert(
            id.clone(),
            Container {
                id,
                output_buffer: None,
            },
        );
        Ok(())
    }

    /// Start a container process via the backend.
    pub fn start<S: LocalSpawner>(
        &mut self,
        id: &str,
        program: &str,
        args: &[String],
        env: &[String],
        working_dir: Option<&str>,
        uid: Option<u32>,
        gid: Option<u32>,
        hostname: Option<&str>,
        capture_output: bool,
        stdin: bool,
        spawner: &S,
    ) -> anyhow::Result<u32> {
        let container = self
            .containers
            .get_mut(id)
            .with_context(|| format!("container {} not found", id))?;

        // Create output buffer if capture_output is enabled.
        let output_tx = if capture_output {
            let buffer = OutputBuffer::new(256);
            let tx = buffer.sender();
            container.output_buffer = Some(buffer);
            Some(tx)
        } else {
            None
        };

        let config = ContainerStartConfig {
            program,
            args,
            env,
            working_dir,
            uid,
            gid,
            hostname,
            capture_output,
            stdin,
        };

        let pid = self.backend.start(id, &config, output_tx, spawner)?;

        Ok(pid)
    }

    /// Mark a container as exited.
    pub fn mark_exited(&mut self, id: &str) {
        self.backend.mark_exited(id);
    }

    /// Get the output buffer receiver for a container (for spawning a drain task).
    pub fn output_buffer_receiver(&self, id: &str) -> Option<async_channel::Receiver<Vec<u8>>> {
        self.containers
            .get(id)
            .and_then(|c| c.output_buffer.as_ref().map(|b| b.receiver()))
    }

    /// Return containers that have an output buffer (for drain task setup on connect).
    pub fn containers_with_output(&self) -> Vec<(String, async_channel::Receiver<Vec<u8>>)> {
        self.containers
            .values()
            .filter_map(|c| {
                c.output_buffer
                    .as_ref()
                    .map(|b| (c.id.clone(), b.receiver()))
            })
            .collect()
    }

    /// Send a signal to all running containers. Logs errors but does not fail.
    pub fn signal_all_running(&mut self, signal: i32) {
        self.backend.signal_all_running(signal);
    }

    /// Returns true if any container has a running process.
    pub fn has_running_containers(&self) -> bool {
        self.backend.has_running_containers()
    }

    /// Send a signal to a running container.
    pub fn signal_container(&mut self, id: &str, signal: i32) -> anyhow::Result<()> {
        self.backend.signal(id, signal)
    }

    /// Remove a container from the map and clean up via backend.
    pub fn remove(&mut self, id: &str) {
        self.containers.remove(id);
        self.backend.remove(id);
    }

    /// Return IDs of containers that have a running process.
    pub fn running_container_ids(&self) -> Vec<String> {
        self.backend.running_container_ids()
    }

    /// Duplicate the stdin pipe write-end for a container.
    pub fn dup_stdin_fd(&self, id: &str) -> Option<OwnedFd> {
        self.backend.dup_stdin_fd(id)
    }

    /// Get the exit receiver from the backend.
    pub fn exit_receiver(&self) -> async_channel::Receiver<super::backend::ContainerExit> {
        self.backend.exit_receiver()
    }
}

#[cfg(feature = "test-support")]
impl<B: ContainerBackend> ContainerManager<B> {
    /// Return the IDs of all containers (not just running).
    pub fn container_ids(&self) -> Vec<String> {
        self.containers.keys().cloned().collect()
    }

    /// Check if a container has an output buffer.
    pub fn has_output_buffer(&self, id: &str) -> bool {
        self.containers
            .get(id)
            .map(|c| c.output_buffer.is_some())
            .unwrap_or(false)
    }

    /// Drain all pending chunks from a container's output buffer.
    pub fn drain_output_buffer(&self, id: &str) -> Vec<Vec<u8>> {
        let mut chunks = Vec::new();
        if let Some(c) = self.containers.get(id) {
            if let Some(ref buf) = c.output_buffer {
                let rx = buf.receiver();
                while let Ok(chunk) = rx.try_recv() {
                    chunks.push(chunk);
                }
            }
        }
        chunks
    }

    /// Access the backend for snapshot extraction.
    pub fn backend(&self) -> &B {
        &self.backend
    }

    /// Reconstruct a ContainerManager with pre-existing containers.
    pub fn new_from_snapshot(
        backend: B,
        container_ids: Vec<String>,
        mut output_buffers: HashMap<String, OutputBuffer>,
    ) -> Self {
        let containers = container_ids
            .into_iter()
            .map(|id| {
                let buf = output_buffers.remove(&id);
                (id.clone(), Container { id, output_buffer: buf })
            })
            .collect();
        ContainerManager { backend, containers }
    }
}
