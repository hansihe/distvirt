use std::os::unix::io::OwnedFd;

use distvirt_guest_protocol::VolumeMount;

use crate::spawner::LocalSpawner;

/// A container exit notification from the backend.
pub struct ContainerExit {
    pub id: String,
    pub code: i32,
    pub output_bytes_dropped: u64,
}

/// Configuration for starting a container process.
pub struct ContainerStartConfig<'a> {
    pub program: &'a str,
    pub args: &'a [String],
    pub env: &'a [String],
    pub working_dir: Option<&'a str>,
    pub uid: Option<u32>,
    pub gid: Option<u32>,
    pub hostname: Option<&'a str>,
    pub capture_output: bool,
    pub stdin: bool,
}

/// Abstraction over container OS operations and per-container task management.
///
/// The backend owns all per-container OS interaction: filesystem setup, process
/// spawning, signal delivery, stdin pipes, and cleanup. It also owns the
/// long-lived per-container tasks (fill tasks for output capture, exit monitors
/// via pidfd). The supervisor receives container exits through a single channel.
///
/// Production: `VmContainerBackend` — real mount/clone3/pidfd/kill.
/// Tests: `TestContainerBackend` — no-op filesystem, channel-based exit/output.
pub trait ContainerBackend {
    /// Prepare filesystem for a container (mount rootfs, volumes, config files).
    fn add(
        &mut self,
        id: &str,
        rootfs: &distvirt_guest_protocol::ContainerRootfs,
        dns_servers: &[String],
        volume_mounts: &[VolumeMount],
    ) -> anyhow::Result<()>;

    /// Start the container process.
    ///
    /// The backend is responsible for:
    /// - Spawning the process (or mock equivalent)
    /// - Setting up output capture (reading stdout/stderr into `output_tx`)
    /// - Monitoring for exit (sending `ContainerExit` to the exit channel)
    /// - Coordinating fill task shutdown on container exit
    ///
    /// When `config.capture_output` is false, `output_tx` is `None` — the
    /// backend should wire stdout/stderr to /dev/console (production) or
    /// discard them (test).
    ///
    /// Returns an opaque pid (real pid or mock id).
    fn start<S: LocalSpawner>(
        &mut self,
        id: &str,
        config: &ContainerStartConfig,
        output_tx: Option<async_channel::Sender<Vec<u8>>>,
        spawner: &S,
    ) -> anyhow::Result<u32>;

    /// Send a signal to a running container.
    fn signal(&mut self, id: &str, signal: i32) -> anyhow::Result<()>;

    /// Send a signal to all running containers. Logs errors but does not fail.
    fn signal_all_running(&mut self, signal: i32);

    /// Returns true if any container has a running process.
    fn has_running_containers(&self) -> bool;

    /// Return IDs of containers that have a running process.
    fn running_container_ids(&self) -> Vec<String>;

    /// Get a duplicated stdin pipe fd for a container.
    ///
    /// Can be called multiple times (once per connection/reconnect). Each
    /// call returns a new dup'd fd; dropping the fd does NOT close stdin
    /// (the backend retains the underlying pipe).
    fn dup_stdin_fd(&self, id: &str) -> Option<OwnedFd>;

    /// Mark a container as exited (pid cleared).
    /// Called by the supervisor after receiving a ContainerExit from the channel.
    fn mark_exited(&mut self, id: &str);

    /// Clean up container resources (unmount, remove cgroup).
    fn remove(&mut self, id: &str);

    /// Receive container exit notifications.
    ///
    /// All container exits from this backend arrive through this channel.
    /// Returns a cloned receiver handle (async_channel receivers are cheap
    /// to clone). The supervisor obtains this once at startup and holds it
    /// across the select loop.
    fn exit_receiver(&self) -> async_channel::Receiver<ContainerExit>;
}
