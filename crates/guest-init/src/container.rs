use std::cell::RefCell;
use std::collections::HashMap;
use std::ffi::CString;
use std::os::unix::io::{AsFd, AsRawFd, BorrowedFd, FromRawFd, IntoRawFd, OwnedFd, RawFd};
use std::ptr;
use std::rc::Rc;

use anyhow::{Context, bail};
use async_executor::LocalExecutor;
use async_io::Async;

use crate::buffer::OutputBuffer;
use crate::cgroup;
use crate::container_init::ContainerInitConfig;
use crate::output::{self, FillTaskHandle};
use crate::util;
use distvirt_guest_protocol::GuestEvent;

/// Newtype wrapper for pipe read-end fds.
///
/// `Async<T>` requires `T: AsRawFd + AsFd`. While `OwnedFd` satisfies these
/// traits, using a distinct type makes it clear at the type level that this
/// fd is the read end of a pipe (not a socket, file, etc.).
pub struct PipeFd {
    fd: OwnedFd,
}

impl PipeFd {
    pub fn new(fd: OwnedFd) -> Self {
        PipeFd { fd }
    }
}

impl AsFd for PipeFd {
    fn as_fd(&self) -> BorrowedFd<'_> {
        self.fd.as_fd()
    }
}

impl AsRawFd for PipeFd {
    fn as_raw_fd(&self) -> RawFd {
        self.fd.as_raw_fd()
    }
}

/// Extra mount points created for a virtiofs overlay container.
/// Tracked so they can be unmounted in reverse order on removal.
struct OverlayMounts {
    /// The virtiofs lower layer mount (e.g. /mnt/rootfs-<id>).
    rootfs_mount: String,
    /// The overlay device mount (e.g. /mnt/overlay-<id>).
    overlay_device_mount: String,
}

struct Container {
    id: String,
    mount_point: String,
    /// Extra mounts to clean up for virtiofs overlay containers.
    overlay_mounts: Option<OverlayMounts>,
    pid: Option<libc::pid_t>,
    /// Write end of stdin pipe (when stdin forwarding is enabled).
    pub stdin_fd: Option<OwnedFd>,
    /// Path to this container's cgroup (if cgroups are available).
    pub cgroup_path: Option<String>,
    /// Per-container output buffer (when capture_output is enabled).
    /// Chunks are produced by the fill task and consumed by a per-connection drain task.
    pub output_buffer: Option<OutputBuffer>,
    /// Handle to the fill task that reads pipes and fills the output buffer.
    pub fill_task_handle: Option<FillTaskHandle>,
}

pub struct ContainerManager {
    containers: HashMap<String, Container>,
}

/// Request from connection loop to root supervisor to spawn a container task.
pub struct ContainerTaskRequest {
    pub id: String,
    pub pid: libc::pid_t,
}

impl ContainerManager {
    pub fn new() -> Self {
        if let Err(e) = cgroup::init_container_cgroup_root() {
            log::warn!("failed to init cgroup root: {:#}", e);
        }
        ContainerManager {
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

        let mount_point = format!("/containers/{}", id);

        let overlay_mounts = match rootfs {
            distvirt_guest_protocol::ContainerRootfs::Device { device } => {
                util::mount(&device, &mount_point, "ext4", 0, None)
                    .with_context(|| format!("mount {} on {}", device, mount_point))?;
                log::info!("mounted {} at {}", device, mount_point);
                None
            }
            distvirt_guest_protocol::ContainerRootfs::VirtioFsOverlay {
                tag,
                overlay_device,
            } => {
                // 1. Mount virtiofs read-only lower layer.
                let rootfs_mount = format!("/mnt/rootfs-{}", id);
                util::mount(
                    &tag,
                    &rootfs_mount,
                    "virtiofs",
                    libc::MS_RDONLY as libc::c_ulong,
                    None,
                )
                .with_context(|| format!("mount virtiofs tag '{}' on {}", tag, rootfs_mount))?;
                log::info!("mounted virtiofs '{}' at {}", tag, rootfs_mount);

                // 2. Mount overlay device for upper/work dirs.
                let overlay_device_mount = format!("/mnt/overlay-{}", id);
                util::mount(&overlay_device, &overlay_device_mount, "ext4", 0, None)
                    .with_context(|| {
                        format!(
                            "mount overlay device {} on {}",
                            overlay_device, overlay_device_mount
                        )
                    })?;

                let upper = format!("{}/upper", overlay_device_mount);
                let work = format!("{}/work", overlay_device_mount);
                std::fs::create_dir_all(&upper).context("create overlay upper dir")?;
                std::fs::create_dir_all(&work).context("create overlay work dir")?;

                // 3. Mount overlayfs combining lower (virtiofs) + upper (block device).
                let overlay_opts = format!(
                    "lowerdir={},upperdir={},workdir={}",
                    rootfs_mount, upper, work
                );
                util::mount("overlay", &mount_point, "overlay", 0, Some(&overlay_opts))
                    .with_context(|| {
                        format!("mount overlayfs on {}", mount_point)
                    })?;
                log::info!(
                    "mounted overlayfs at {} (lower={}, overlay_device={})",
                    mount_point,
                    rootfs_mount,
                    overlay_device
                );

                Some(OverlayMounts {
                    rootfs_mount,
                    overlay_device_mount,
                })
            }
        };

        // Bind-mount volumes into the container rootfs.
        for vm in volume_mounts {
            let source = format!("/volumes/{}", vm.name);
            let target = format!(
                "{}/{}",
                mount_point,
                vm.mount_path.trim_start_matches('/')
            );
            std::fs::create_dir_all(&target)
                .with_context(|| format!("create mount target {}", target))?;
            util::mount(&source, &target, "", libc::MS_BIND as libc::c_ulong, None)
                .with_context(|| {
                    format!("bind mount volume '{}' at {}", vm.name, target)
                })?;
            log::info!("bind-mounted volume '{}' at {}", vm.name, target);
        }

        // Ensure /etc exists in the rootfs.
        let etc_dir = format!("{}/etc", mount_point);
        let _ = std::fs::create_dir_all(&etc_dir);

        // Write /etc/resolv.conf with DNS nameservers.
        if !dns_servers.is_empty() {
            let resolv_path = format!("{}/etc/resolv.conf", mount_point);
            let mut content = String::new();
            for server in dns_servers {
                content.push_str(&format!("nameserver {}\n", server));
            }
            std::fs::write(&resolv_path, &content)
                .with_context(|| format!("writing {}", resolv_path))?;
            log::info!(
                "wrote {} with {} nameserver(s)",
                resolv_path,
                dns_servers.len()
            );
        }

        // Write /etc/hostname with the container id.
        let hostname_path = format!("{}/etc/hostname", mount_point);
        std::fs::write(&hostname_path, format!("{}\n", id))
            .with_context(|| format!("writing {}", hostname_path))?;
        log::info!("wrote {}", hostname_path);

        // Write /etc/hosts with localhost entries.
        let hosts_path = format!("{}/etc/hosts", mount_point);
        std::fs::write(&hosts_path, "127.0.0.1\tlocalhost\n::1\t\tlocalhost\n")
            .with_context(|| format!("writing {}", hosts_path))?;
        log::info!("wrote {}", hosts_path);

        self.containers.insert(
            id.clone(),
            Container {
                id,
                mount_point,
                overlay_mounts,
                pid: None,
                stdin_fd: None,
                cgroup_path: None,
                output_buffer: None,
                fill_task_handle: None,
            },
        );
        Ok(())
    }

    /// Fork a child process, chroot into the container rootfs, and exec the program.
    pub fn start(
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
        ex: &LocalExecutor<'_>,
    ) -> anyhow::Result<u32> {
        let container = self
            .containers
            .get_mut(id)
            .with_context(|| format!("container {} not found", id))?;

        if container.pid.is_some() {
            bail!("container {} is already running", id);
        }

        // Create pipes for stdout/stderr if capture_output is requested.
        // OwnedFd handles cleanup automatically if fork fails.
        let stdout_pipe = if capture_output {
            Some(util::create_pipe().context("create stdout pipe")?)
        } else {
            None
        };
        let stderr_pipe = if capture_output {
            Some(util::create_pipe().context("create stderr pipe")?)
        } else {
            None
        };
        // Create stdin pipe if stdin forwarding is requested.
        // (read_end, write_end) — child reads from read_end, parent writes to write_end.
        let stdin_pipe = if stdin {
            Some(util::create_pipe().context("create stdin pipe")?)
        } else {
            None
        };

        // Set up cgroup for this container before clone3.
        let cgroup_path = cgroup::setup_container_cgroup(id)
            .with_context(|| format!("create cgroup for container {}", id))?;

        // Open the cgroup directory as an fd for CLONE_INTO_CGROUP.
        let cgroup_dir_c = CString::new(cgroup_path.as_str()).context("invalid cgroup path")?;
        let cgroup_dir_fd = unsafe {
            libc::open(
                cgroup_dir_c.as_ptr(),
                libc::O_RDONLY | libc::O_CLOEXEC | libc::O_DIRECTORY,
            )
        };
        if cgroup_dir_fd < 0 {
            bail!(
                "open cgroup dir {}: {}",
                cgroup_path,
                std::io::Error::last_os_error()
            );
        }
        // Ensure the fd is closed in the parent even on error paths.
        let cgroup_dir_fd_owned = unsafe { OwnedFd::from_raw_fd(cgroup_dir_fd) };

        // Create a pipe to pass config to the child after exec.
        let (config_read, config_write) =
            util::create_pipe().context("create config pipe")?;

        // Build the config that the child will read after exec.
        let init_config = ContainerInitConfig {
            mount_point: container.mount_point.clone(),
            program: program.to_string(),
            args: args.to_vec(),
            env: env.to_vec(),
            working_dir: working_dir.map(|s| s.to_string()),
            uid,
            gid,
            hostname: hostname.map(|s| s.to_string()),
            domainname: None,
            stdout_write_fd: stdout_pipe.as_ref().map(|(_, w)| w.as_raw_fd()),
            stderr_write_fd: stderr_pipe.as_ref().map(|(_, w)| w.as_raw_fd()),
            stdin_read_fd: stdin_pipe.as_ref().map(|(r, _)| r.as_raw_fd()),
        };

        // Serialize config before fork so we don't allocate in the child.
        let config_json =
            serde_json::to_vec(&init_config).context("serialize container init config")?;

        // clone3 with CLONE_NEWPID | CLONE_INTO_CGROUP: the child is born
        // into a new PID namespace (PID 1) and directly into its cgroup.
        let pid = clone3_into_cgroup(cgroup_dir_fd_owned.as_raw_fd())?;

        if pid == 0 {
            // Child process — exec self as container init.
            // Clear O_CLOEXEC on fds the child needs to inherit across exec.
            clear_cloexec(config_read.as_raw_fd());
            if let Some(fd) = init_config.stdout_write_fd {
                clear_cloexec(fd);
            }
            if let Some(fd) = init_config.stderr_write_fd {
                clear_cloexec(fd);
            }
            if let Some(fd) = init_config.stdin_read_fd {
                clear_cloexec(fd);
            }

            // Exec ourselves with --container-init <pipe-fd>.
            let exe = CString::new("/proc/self/exe").unwrap();
            let arg0 = CString::new("init").unwrap();
            let arg1 = CString::new("--container-init").unwrap();
            let arg2 = CString::new(config_read.as_raw_fd().to_string()).unwrap();
            let argv: [*const libc::c_char; 4] =
                [arg0.as_ptr(), arg1.as_ptr(), arg2.as_ptr(), ptr::null()];
            // Empty environment — the container-init process doesn't need
            // guest-init's env; the container's env is in the config.
            let envp: [*const libc::c_char; 1] = [ptr::null()];
            unsafe { libc::execve(exe.as_ptr(), argv.as_ptr(), envp.as_ptr()) };
            // If execve fails, exit immediately.
            unsafe { libc::_exit(127) }
        }

        // Parent — close the read end and write config to the child.
        drop(config_read);
        {
            use std::io::Write;
            let mut config_file =
                unsafe { std::fs::File::from_raw_fd(config_write.into_raw_fd()) };
            config_file
                .write_all(&config_json)
                .context("write config to child pipe")?;
            // config_file dropped here, closing the write end (child sees EOF).
        }

        // Parent — keep read ends, drop write ends (via destructuring).
        // Wrap pipes in Async and spawn a fill task that reads them into an OutputBuffer.
        if let (Some((stdout_read, _stdout_write)), Some((stderr_read, _stderr_write))) =
            (stdout_pipe, stderr_pipe)
        {
            let stdout_async =
                Some(Async::new(PipeFd::new(stdout_read)).context("wrap stdout pipe in Async")?);
            let stderr_async =
                Some(Async::new(PipeFd::new(stderr_read)).context("wrap stderr pipe in Async")?);

            let buffer = OutputBuffer::new(256);
            let fill_handle = output::spawn_fill_task(
                id.to_string(),
                stdout_async,
                stderr_async,
                buffer.sender(),
                ex,
            );
            container.output_buffer = Some(buffer);
            container.fill_task_handle = Some(fill_handle);
        }

        // Parent — keep write end of stdin pipe, drop read end.
        // Set O_NONBLOCK on the write end now. Since dup() shares the open file
        // description, all future dup'd fds (for relay_stdin) inherit this
        // automatically — no need to set it again per-connection.
        if let Some((_stdin_read, stdin_write)) = stdin_pipe {
            let raw_fd = stdin_write.as_raw_fd();
            let flags = unsafe { libc::fcntl(raw_fd, libc::F_GETFL) };
            if flags >= 0 {
                unsafe { libc::fcntl(raw_fd, libc::F_SETFL, flags | libc::O_NONBLOCK) };
            }
            container.stdin_fd = Some(stdin_write);
        }

        container.cgroup_path = Some(cgroup_path);

        container.pid = Some(pid);
        log::info!(
            "container {} started with pid {}{}",
            id,
            pid,
            if capture_output {
                " (output captured)"
            } else {
                ""
            },
        );
        Ok(pid as u32)
    }

    /// Mark a container as exited without calling waitpid (pidfd handles reaping).
    pub fn mark_exited(&mut self, id: &str) {
        if let Some(c) = self.containers.get_mut(id) {
            c.pid = None;
        }
    }

    /// Duplicate the stdin pipe write-end for a container.
    /// The original fd stays in the container so it survives reconnects.
    pub fn dup_stdin_fd(&self, id: &str) -> Option<OwnedFd> {
        self.containers.get(id).and_then(|c| {
            c.stdin_fd.as_ref().and_then(|fd| {
                let new_fd = unsafe { libc::dup(fd.as_raw_fd()) };
                if new_fd < 0 {
                    log::warn!(
                        "dup stdin fd for {}: {}",
                        id,
                        std::io::Error::last_os_error()
                    );
                    None
                } else {
                    Some(unsafe { OwnedFd::from_raw_fd(new_fd) })
                }
            })
        })
    }

    /// Get the output buffer receiver for a container (for spawning a drain task).
    pub fn output_buffer_receiver(&self, id: &str) -> Option<async_channel::Receiver<Vec<u8>>> {
        self.containers
            .get(id)
            .and_then(|c| c.output_buffer.as_ref().map(|b| b.receiver()))
    }

    /// Take the fill task handle for a container (for signaling on exit).
    pub fn take_fill_task_handle(&mut self, id: &str) -> Option<FillTaskHandle> {
        self.containers
            .get_mut(id)
            .and_then(|c| c.fill_task_handle.take())
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
    pub fn signal_all_running(&self, signal: i32) {
        for container in self.containers.values() {
            if let Some(pid) = container.pid {
                let ret = unsafe { libc::kill(pid, signal) };
                if ret != 0 {
                    log::warn!(
                        "kill({}, {}) for container {}: {}",
                        pid,
                        signal,
                        container.id,
                        std::io::Error::last_os_error()
                    );
                }
            }
        }
    }

    /// Returns true if any container has a running process.
    pub fn has_running_containers(&self) -> bool {
        self.containers.values().any(|c| c.pid.is_some())
    }

    /// Send a signal to a running container.
    pub fn signal_container(&self, id: &str, signal: i32) -> anyhow::Result<()> {
        let container = self
            .containers
            .get(id)
            .with_context(|| format!("container {} not found", id))?;
        let pid = container
            .pid
            .with_context(|| format!("container {} is not running", id))?;
        let ret = unsafe { libc::kill(pid, signal) };
        if ret != 0 {
            bail!(
                "kill({}, {}): {}",
                pid,
                signal,
                std::io::Error::last_os_error()
            );
        }
        Ok(())
    }

    /// Remove a container from the map and best-effort unmount its filesystems.
    pub fn remove(&mut self, id: &str) {
        if let Some(container) = self.containers.remove(id) {
            // Unmount in reverse order: overlay → overlay device → virtiofs.
            if let Err(e) = util::umount(&container.mount_point) {
                log::warn!("umount {}: {:#}", container.mount_point, e);
            } else {
                log::info!("unmounted {}", container.mount_point);
            }

            if let Some(overlay) = &container.overlay_mounts {
                if let Err(e) = util::umount(&overlay.overlay_device_mount) {
                    log::warn!("umount {}: {:#}", overlay.overlay_device_mount, e);
                } else {
                    log::info!("unmounted {}", overlay.overlay_device_mount);
                }
                if let Err(e) = util::umount(&overlay.rootfs_mount) {
                    log::warn!("umount {}: {:#}", overlay.rootfs_mount, e);
                } else {
                    log::info!("unmounted {}", overlay.rootfs_mount);
                }
            }

            if container.cgroup_path.is_some() {
                cgroup::remove_cgroup(&container.id);
            }
        }
    }

    /// Return IDs of containers that have a running process.
    pub fn running_container_ids(&self) -> Vec<String> {
        self.containers
            .values()
            .filter(|c| c.pid.is_some())
            .map(|c| c.id.clone())
            .collect()
    }
}

// ---------------------------------------------------------------------------
// pidfd helpers
// ---------------------------------------------------------------------------

/// Fork via `clone3` with `CLONE_NEWPID | CLONE_INTO_CGROUP`.
///
/// The child is born as PID 1 in a new PID namespace and is placed directly
/// into the given cgroup (no post-fork race). Returns 0 in the child, child
/// pid in the parent.
fn clone3_into_cgroup(cgroup_fd: RawFd) -> anyhow::Result<libc::pid_t> {
    // CLONE_INTO_CGROUP (0x200000000, Linux 5.7+) — not defined in musl's libc bindings.
    const CLONE_INTO_CGROUP: u64 = 0x200000000;

    let mut args: libc::clone_args = unsafe { std::mem::zeroed() };
    args.flags = (libc::CLONE_NEWPID as u64) | (libc::CLONE_NEWIPC as u64) | CLONE_INTO_CGROUP;
    args.exit_signal = libc::SIGCHLD as u64;
    args.cgroup = cgroup_fd as u64;
    let ret = unsafe {
        libc::syscall(
            libc::SYS_clone3,
            &args as *const libc::clone_args,
            std::mem::size_of::<libc::clone_args>(),
        )
    };
    if ret < 0 {
        bail!("clone3: {}", std::io::Error::last_os_error());
    }
    Ok(ret as libc::pid_t)
}

/// Open a pidfd for the given pid (Linux 5.3+, syscall 434 on x86_64).
fn pidfd_open(pid: libc::pid_t) -> anyhow::Result<OwnedFd> {
    let fd = unsafe { libc::syscall(libc::SYS_pidfd_open, pid, 0 as libc::c_uint) };
    if fd < 0 {
        bail!("pidfd_open({}): {}", pid, std::io::Error::last_os_error());
    }
    Ok(unsafe { OwnedFd::from_raw_fd(fd as RawFd) })
}

/// Wait for exit status via pidfd using `waitid(P_PIDFD, ...)`.
fn waitid_pidfd(pidfd: &OwnedFd) -> anyhow::Result<i32> {
    const P_PIDFD: libc::idtype_t = 3;
    let mut siginfo: libc::siginfo_t = unsafe { std::mem::zeroed() };
    let ret = unsafe {
        libc::waitid(
            P_PIDFD,
            pidfd.as_raw_fd() as libc::id_t,
            &mut siginfo,
            libc::WEXITED,
        )
    };
    if ret < 0 {
        bail!("waitid(P_PIDFD): {}", std::io::Error::last_os_error());
    }
    // Extract exit code from siginfo_t.
    // si_status contains the exit code (for CLD_EXITED) or signal number (for CLD_KILLED/CLD_DUMPED).
    let si_code = siginfo.si_code;
    let si_status = unsafe { siginfo.si_status() };
    let code = if si_code == libc::CLD_EXITED {
        si_status
    } else {
        // Killed by signal — use 128+signal convention.
        128 + si_status
    };
    Ok(code)
}

// ---------------------------------------------------------------------------
// Per-container supervised task
// ---------------------------------------------------------------------------

/// Supervised task for a single container. Waits for the container process to
/// exit via pidfd, drains output, pushes the exit event, and cleans up.
pub async fn container_task(
    id: String,
    pid: libc::pid_t,
    containers: Rc<RefCell<ContainerManager>>,
    event_tx: async_channel::Sender<GuestEvent>,
) -> anyhow::Result<()> {
    let pidfd = pidfd_open(pid)?;

    // Set O_NONBLOCK so Async can use epoll on it.
    let flags = unsafe { libc::fcntl(pidfd.as_raw_fd(), libc::F_GETFL) };
    if flags >= 0 {
        unsafe { libc::fcntl(pidfd.as_raw_fd(), libc::F_SETFL, flags | libc::O_NONBLOCK) };
    }

    let async_pidfd = Async::new_nonblocking(pidfd).context("wrap pidfd in Async")?;

    // Wait for process exit (pidfd becomes readable).
    async_pidfd.readable().await?;

    let code = waitid_pidfd(async_pidfd.get_ref())?;
    log::info!("container {} (pid {}) exited with code {}", id, pid, code);

    // Mark exited in container manager.
    containers.borrow_mut().mark_exited(&id);

    // Signal fill task for final output drain.
    let fill_handle = containers.borrow_mut().take_fill_task_handle(&id);
    let output_bytes_dropped = if let Some(handle) = fill_handle {
        handle.signal_exit().await
    } else {
        0
    };

    // Push exit event (buffered, survives disconnects).
    if let Err(e) = event_tx
        .send(GuestEvent::ContainerExited {
            id: id.clone(),
            code,
            output_bytes_dropped,
        })
        .await
    {
        log::error!("event buffer send failed (closed): {}", e);
    }

    // Cleanup: unmount, remove cgroup.
    containers.borrow_mut().remove(&id);

    Ok(())
}

/// Clear the O_CLOEXEC flag on a file descriptor so it survives across exec.
fn clear_cloexec(fd: RawFd) {
    unsafe {
        let flags = libc::fcntl(fd, libc::F_GETFD);
        if flags >= 0 {
            libc::fcntl(fd, libc::F_SETFD, flags & !libc::FD_CLOEXEC);
        }
    }
}
