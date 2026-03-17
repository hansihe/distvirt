use std::cell::RefCell;
use std::collections::HashMap;
use std::ffi::CString;
use std::os::unix::io::{AsFd, AsRawFd, BorrowedFd, FromRawFd, OwnedFd, RawFd};
use std::ptr;
use std::rc::Rc;

use anyhow::{Context, bail};
use async_executor::LocalExecutor;
use async_io::Async;

use crate::buffer::OutputBuffer;
use crate::cgroup;
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

struct Container {
    id: String,
    mount_point: String,
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

    /// Mount the block device as ext4 at /containers/<id> and write resolv.conf.
    pub fn add(
        &mut self,
        id: String,
        device: String,
        dns_servers: &[String],
    ) -> anyhow::Result<()> {
        if self.containers.contains_key(&id) {
            bail!("container {} already exists", id);
        }

        let mount_point = format!("/containers/{}", id);
        util::mount(&device, &mount_point, "ext4", 0, None)
            .with_context(|| format!("mount {} on {}", device, mount_point))?;
        log::info!("mounted {} at {}", device, mount_point);

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
                pid: None,
                stdin_fd: None,
                cgroup_path: None,
                output_buffer: None,
                fill_task_handle: None,
            },
        );
        Ok(())
    }

    /// Fork a child process, chroot into the container rootfs, and exec the entrypoint.
    pub fn start(
        &mut self,
        id: &str,
        entrypoint: &str,
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

        // clone3 with CLONE_NEWPID | CLONE_INTO_CGROUP: the child is born
        // into a new PID namespace (PID 1) and directly into its cgroup.
        let pid = clone3_into_cgroup(cgroup_dir_fd_owned.as_raw_fd())?;

        if pid == 0 {
            // Child process — pass write-end raw fds to child_exec.
            // The child will dup2 them onto stdout/stderr and close the originals.
            // We must not run OwnedFd destructors in the child (we're about to exec).
            let stdout_write_fd = stdout_pipe.as_ref().map(|(_, w)| w.as_raw_fd());
            let stderr_write_fd = stderr_pipe.as_ref().map(|(_, w)| w.as_raw_fd());
            let stdin_read_fd = stdin_pipe.as_ref().map(|(r, _)| r.as_raw_fd());
            child_exec(
                &container.mount_point,
                entrypoint,
                args,
                env,
                working_dir,
                uid,
                gid,
                hostname,
                stdout_write_fd,
                stderr_write_fd,
                stdin_read_fd,
            );
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

    /// Remove a container from the map and best-effort unmount its filesystem.
    pub fn remove(&mut self, id: &str) {
        if let Some(container) = self.containers.remove(id) {
            let mount_point_c = CString::new(container.mount_point.as_str()).ok();
            if let Some(mp) = mount_point_c {
                let ret = unsafe { libc::umount(mp.as_ptr()) };
                if ret != 0 {
                    log::warn!(
                        "umount {}: {}",
                        container.mount_point,
                        std::io::Error::last_os_error()
                    );
                } else {
                    log::info!("unmounted {}", container.mount_point);
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
    args.flags = (libc::CLONE_NEWPID as u64) | CLONE_INTO_CGROUP;
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
    if let Some(handle) = fill_handle {
        handle.signal_exit().await;
    }

    // Push exit event (buffered, survives disconnects).
    if let Err(e) = event_tx
        .send(GuestEvent::ContainerExited {
            id: id.clone(),
            code,
        })
        .await
    {
        log::error!("event buffer send failed (closed): {}", e);
    }

    // Cleanup: unmount, remove cgroup.
    containers.borrow_mut().remove(&id);

    Ok(())
}

/// Runs in the child process after fork. Never returns.
fn child_exec(
    mount_point: &str,
    entrypoint: &str,
    args: &[String],
    env: &[String],
    working_dir: Option<&str>,
    uid: Option<u32>,
    gid: Option<u32>,
    hostname: Option<&str>,
    stdout_write_fd: Option<RawFd>,
    stderr_write_fd: Option<RawFd>,
    stdin_read_fd: Option<RawFd>,
) -> ! {
    let result = child_exec_inner(
        mount_point,
        entrypoint,
        args,
        env,
        working_dir,
        uid,
        gid,
        hostname,
        stdout_write_fd,
        stderr_write_fd,
        stdin_read_fd,
    );
    if let Err(e) = result {
        eprintln!("container child exec failed: {:#}", e);
    }
    unsafe { libc::_exit(127) }
}

fn child_exec_inner(
    mount_point: &str,
    entrypoint: &str,
    args: &[String],
    env: &[String],
    working_dir: Option<&str>,
    uid: Option<u32>,
    gid: Option<u32>,
    hostname: Option<&str>,
    stdout_write_fd: Option<RawFd>,
    stderr_write_fd: Option<RawFd>,
    stdin_read_fd: Option<RawFd>,
) -> anyhow::Result<()> {
    // New session so the container process is a session leader.
    if unsafe { libc::setsid() } < 0 {
        bail!("setsid: {}", std::io::Error::last_os_error());
    }

    // Isolate mount and UTS namespaces so mounts and hostname changes are
    // scoped to this container and don't affect other containers or guest-init.
    if unsafe { libc::unshare(libc::CLONE_NEWNS | libc::CLONE_NEWUTS) } != 0 {
        bail!(
            "unshare(CLONE_NEWNS|CLONE_NEWUTS): {}",
            std::io::Error::last_os_error()
        );
    }

    // Set hostname (now scoped to this container's UTS namespace).
    if let Some(name) = hostname {
        let name_c = CString::new(name)?;
        if unsafe { libc::sethostname(name_c.as_ptr(), name.len()) } != 0 {
            bail!("sethostname: {}", std::io::Error::last_os_error());
        }
    }

    // Chroot into the container rootfs.
    let mount_point_c = CString::new(mount_point)?;
    if unsafe { libc::chroot(mount_point_c.as_ptr()) } != 0 {
        bail!("chroot: {}", std::io::Error::last_os_error());
    }

    // Change to working directory (default /).
    let wd = working_dir.unwrap_or("/");
    let wd_c = CString::new(wd)?;
    if unsafe { libc::chdir(wd_c.as_ptr()) } != 0 {
        bail!("chdir {}: {}", wd, std::io::Error::last_os_error());
    }

    // Mount essential filesystems inside the container.
    util::mount(
        "proc",
        "/proc",
        "proc",
        libc::MS_NOSUID | libc::MS_NODEV | libc::MS_NOEXEC,
        None,
    )?;
    util::mount(
        "sysfs",
        "/sys",
        "sysfs",
        libc::MS_NOSUID | libc::MS_NODEV | libc::MS_NOEXEC,
        None,
    )?;
    util::mount("devtmpfs", "/dev", "devtmpfs", libc::MS_NOSUID, None)?;
    util::mount(
        "tmpfs",
        "/tmp",
        "tmpfs",
        libc::MS_NOSUID | libc::MS_NODEV,
        None,
    )?;

    // Set up controlling terminal from /dev/console (separate from stdin).
    let console = CString::new("/dev/console").unwrap();
    let console_fd = unsafe { libc::open(console.as_ptr(), libc::O_RDWR) };
    if console_fd >= 0 {
        unsafe { libc::ioctl(console_fd, libc::TIOCSCTTY as _, 0) };
    }

    // Set up stdin: pipe from host if requested, otherwise console or /dev/null.
    if let Some(fd) = stdin_read_fd {
        unsafe {
            libc::dup2(fd, 0);
            if fd > 2 {
                libc::close(fd);
            }
        }
    } else if console_fd >= 0 {
        unsafe {
            libc::dup2(console_fd, 0); // stdin = console
        }
    } else {
        // Fallback: /dev/null for stdin if console unavailable.
        let devnull = CString::new("/dev/null").unwrap();
        let null_fd = unsafe { libc::open(devnull.as_ptr(), libc::O_RDONLY) };
        if null_fd >= 0 {
            unsafe {
                libc::dup2(null_fd, 0);
                if null_fd > 2 {
                    libc::close(null_fd);
                }
            }
        }
    }

    if let (Some(stdout_fd), Some(stderr_fd)) = (stdout_write_fd, stderr_write_fd) {
        // Capture mode: redirect stdout/stderr to pipes.
        unsafe {
            libc::dup2(stdout_fd, 1);
            libc::dup2(stderr_fd, 2);
            if stdout_fd > 2 {
                libc::close(stdout_fd);
            }
            if stderr_fd > 2 {
                libc::close(stderr_fd);
            }
        }
    } else {
        // Legacy mode: use console for stdout/stderr too.
        if console_fd >= 0 {
            unsafe {
                libc::dup2(console_fd, 1);
                libc::dup2(console_fd, 2);
            }
        }
    }

    if console_fd >= 0 && console_fd > 2 {
        unsafe {
            libc::close(console_fd);
        }
    }

    // Close all fds > 2 that aren't stdin/stdout/stderr.
    // O_CLOEXEC handles most, but this catches any leaks from the parent
    // (vsock listener, epoll, inotify, other containers' pipe write-ends).
    //
    // Uses raw libc opendir/readdir to avoid std::fs::ReadDir, whose Drop
    // impl calls closedir and panics if the underlying fd was already closed.
    unsafe {
        let path = b"/proc/self/fd\0";
        let dir = libc::opendir(path.as_ptr() as *const libc::c_char);
        if !dir.is_null() {
            let dir_fd = libc::dirfd(dir);
            loop {
                let entry = libc::readdir(dir);
                if entry.is_null() {
                    break;
                }
                let name = std::ffi::CStr::from_ptr((*entry).d_name.as_ptr());
                if let Ok(fd) = name.to_str().unwrap_or("").parse::<i32>() {
                    if fd > 2 && fd != dir_fd {
                        libc::close(fd);
                    }
                }
            }
            libc::closedir(dir);
        }
    }

    // Set gid before uid (after setuid we may lack permission for setgid).
    if let Some(g) = gid {
        if unsafe { libc::setgid(g) } != 0 {
            bail!("setgid({}): {}", g, std::io::Error::last_os_error());
        }
    }
    if let Some(u) = uid {
        if unsafe { libc::setuid(u) } != 0 {
            bail!("setuid({}): {}", u, std::io::Error::last_os_error());
        }
    }

    // Resolve entrypoint via PATH if it's not an absolute/relative path.
    let resolved_entrypoint = if entrypoint.contains('/') {
        entrypoint.to_string()
    } else {
        resolve_in_path(entrypoint, env).unwrap_or_else(|| entrypoint.to_string())
    };

    // Build argv for execve.
    let entrypoint_c = CString::new(resolved_entrypoint.as_str())?;
    let args_c: Vec<CString> = std::iter::once(CString::new(entrypoint)?)
        .chain(
            args.iter()
                .map(|a| CString::new(a.as_str()).context("invalid argument"))
                .collect::<Result<Vec<_>, _>>()?,
        )
        .collect();
    let mut argv: Vec<*const libc::c_char> = args_c.iter().map(|a| a.as_ptr()).collect();
    argv.push(ptr::null());

    // Build envp for execve — explicit env, no leaking guest-init env.
    let env_c: Vec<CString> = env
        .iter()
        .map(|e| CString::new(e.as_str()).context("invalid env var"))
        .collect::<Result<Vec<_>, _>>()?;
    let mut envp: Vec<*const libc::c_char> = env_c.iter().map(|e| e.as_ptr()).collect();
    envp.push(ptr::null());

    unsafe { libc::execve(entrypoint_c.as_ptr(), argv.as_ptr(), envp.as_ptr()) };
    bail!(
        "execve {}: {}",
        resolved_entrypoint,
        std::io::Error::last_os_error()
    );
}

/// Resolve a bare command name by searching PATH from the provided env list.
///
/// Looks for `PATH=...` in `env`, splits on ':', and checks each directory
/// for an executable file with the given name. Returns the first match.
fn resolve_in_path(name: &str, env: &[String]) -> Option<String> {
    use std::os::unix::fs::PermissionsExt;
    let path_val = env.iter().find_map(|e| e.strip_prefix("PATH="))?;
    for dir in path_val.split(':') {
        if dir.is_empty() {
            continue;
        }
        let candidate = format!("{}/{}", dir, name);
        if let Ok(meta) = std::fs::metadata(&candidate) {
            if meta.is_file() && (meta.permissions().mode() & 0o111) != 0 {
                return Some(candidate);
            }
        }
    }
    None
}
