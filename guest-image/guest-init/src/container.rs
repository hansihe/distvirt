use std::collections::HashMap;
use std::ffi::CString;
use std::os::unix::io::{AsFd, AsRawFd, BorrowedFd, OwnedFd, RawFd};
use std::ptr;

use anyhow::{bail, Context};
use async_io::Async;

use crate::util;

/// RAII wrapper around the read end of a pipe.
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
    /// Read end of stdout pipe (when capture_output is enabled).
    pub stdout_fd: Option<Async<PipeFd>>,
    /// Read end of stderr pipe (when capture_output is enabled).
    pub stderr_fd: Option<Async<PipeFd>>,
}

pub struct ContainerManager {
    containers: HashMap<String, Container>,
}

/// Result of reaping a child process.
pub struct ChildExit {
    pub id: String,
    pub code: i32,
}

impl ContainerManager {
    pub fn new() -> Self {
        ContainerManager {
            containers: HashMap::new(),
        }
    }

    /// Mount the block device as ext4 at /containers/<id> and write resolv.conf.
    pub fn add(&mut self, id: String, device: String, dns_servers: &[String]) -> anyhow::Result<()> {
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
            log::info!("wrote {} with {} nameserver(s)", resolv_path, dns_servers.len());
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
                stdout_fd: None,
                stderr_fd: None,
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

        let pid = unsafe { libc::fork() };
        if pid < 0 {
            // OwnedFds drop automatically here.
            bail!("fork: {}", std::io::Error::last_os_error());
        }

        if pid == 0 {
            // Child process — pass write-end raw fds to child_exec.
            // The child will dup2 them onto stdout/stderr and close the originals.
            // We must not run OwnedFd destructors in the child (we're about to exec).
            let stdout_write_fd = stdout_pipe.as_ref().map(|(_, w)| w.as_raw_fd());
            let stderr_write_fd = stderr_pipe.as_ref().map(|(_, w)| w.as_raw_fd());
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
            );
        }

        // Parent — keep read ends, drop write ends (via destructuring).
        if let (Some((stdout_read, _stdout_write)), Some((stderr_read, _stderr_write))) =
            (stdout_pipe, stderr_pipe)
        {
            container.stdout_fd = Some(
                Async::new(PipeFd::new(stdout_read))
                    .context("wrap stdout pipe in Async")?
            );
            container.stderr_fd = Some(
                Async::new(PipeFd::new(stderr_read))
                    .context("wrap stderr pipe in Async")?
            );
        }

        container.pid = Some(pid);
        log::info!(
            "container {} started with pid {}{}",
            id,
            pid,
            if capture_output { " (output captured)" } else { "" },
        );
        Ok(pid as u32)
    }

    /// Reap any exited children and return their container IDs and exit codes.
    pub fn reap_children(&mut self) -> Vec<ChildExit> {
        let mut exits = Vec::new();
        loop {
            let mut status: libc::c_int = 0;
            let pid = unsafe { libc::waitpid(-1, &mut status, libc::WNOHANG) };
            if pid <= 0 {
                break;
            }

            let code = if libc::WIFEXITED(status) {
                libc::WEXITSTATUS(status)
            } else if libc::WIFSIGNALED(status) {
                128 + libc::WTERMSIG(status)
            } else {
                -1
            };

            // Find which container this PID belongs to.
            let id = self
                .containers
                .values()
                .find(|c| c.pid == Some(pid))
                .map(|c| c.id.clone());

            if let Some(id) = id {
                log::info!("container {} (pid {}) exited with code {}", id, pid, code);
                if let Some(c) = self.containers.get_mut(&id) {
                    c.pid = None;
                }
                exits.push(ChildExit { id, code });
            } else {
                log::warn!("reaped unknown pid {} with code {}", pid, code);
            }
        }
        exits
    }

    /// Take ownership of the stdout pipe for a container.
    pub fn take_stdout_fd(&mut self, id: &str) -> Option<Async<PipeFd>> {
        self.containers.get_mut(id).and_then(|c| c.stdout_fd.take())
    }

    /// Take ownership of the stderr pipe for a container.
    pub fn take_stderr_fd(&mut self, id: &str) -> Option<Async<PipeFd>> {
        self.containers.get_mut(id).and_then(|c| c.stderr_fd.take())
    }

    /// Get the raw fd for a container's stdout pipe (without taking ownership).
    pub fn stdout_raw_fd(&self, id: &str) -> Option<i32> {
        self.containers.get(id).and_then(|c| c.stdout_fd.as_ref().map(|p| p.as_raw_fd()))
    }

    /// Get the raw fd for a container's stderr pipe (without taking ownership).
    pub fn stderr_raw_fd(&self, id: &str) -> Option<i32> {
        self.containers.get(id).and_then(|c| c.stderr_fd.as_ref().map(|p| p.as_raw_fd()))
    }

    /// Remove a container from the map and best-effort unmount its filesystem.
    pub fn remove(&mut self, id: &str) {
        if let Some(container) = self.containers.remove(id) {
            let mount_point_c = CString::new(container.mount_point.as_str()).ok();
            if let Some(mp) = mount_point_c {
                let ret = unsafe { libc::umount(mp.as_ptr()) };
                if ret != 0 {
                    log::warn!("umount {}: {}", container.mount_point, std::io::Error::last_os_error());
                } else {
                    log::info!("unmounted {}", container.mount_point);
                }
            }
        }
    }

    /// Return all container IDs that have capture output enabled (have pipe fds).
    pub fn captured_container_ids(&self) -> Vec<String> {
        self.containers
            .values()
            .filter(|c| c.stdout_fd.is_some() || c.stderr_fd.is_some())
            .map(|c| c.id.clone())
            .collect()
    }

    /// Return references to all active pipe Async wrappers (for readability polling).
    pub fn pipe_refs(&self) -> Vec<&Async<PipeFd>> {
        let mut pipes = Vec::new();
        for c in self.containers.values() {
            if let Some(ref p) = c.stdout_fd {
                pipes.push(p);
            }
            if let Some(ref p) = c.stderr_fd {
                pipes.push(p);
            }
        }
        pipes
    }
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
) -> ! {
    let result = child_exec_inner(
        mount_point, entrypoint, args, env, working_dir, uid, gid, hostname,
        stdout_write_fd, stderr_write_fd,
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
) -> anyhow::Result<()> {
    // New session so the container process is a session leader.
    if unsafe { libc::setsid() } < 0 {
        bail!("setsid: {}", std::io::Error::last_os_error());
    }

    // Set hostname before chroot (needs root, do before setuid).
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
    util::mount("proc", "/proc", "proc", libc::MS_NOSUID | libc::MS_NODEV | libc::MS_NOEXEC, None)?;
    util::mount("sysfs", "/sys", "sysfs", libc::MS_NOSUID | libc::MS_NODEV | libc::MS_NOEXEC, None)?;
    util::mount("devtmpfs", "/dev", "devtmpfs", libc::MS_NOSUID, None)?;
    util::mount("tmpfs", "/tmp", "tmpfs", libc::MS_NOSUID | libc::MS_NODEV, None)?;

    if let (Some(stdout_fd), Some(stderr_fd)) = (stdout_write_fd, stderr_write_fd) {
        // Capture mode: redirect stdout/stderr to pipes.
        // Open /dev/null for stdin.
        let devnull = CString::new("/dev/null").unwrap();
        let null_fd = unsafe { libc::open(devnull.as_ptr(), libc::O_RDONLY) };
        if null_fd < 0 {
            bail!("open /dev/null: {}", std::io::Error::last_os_error());
        }
        unsafe {
            libc::dup2(null_fd, 0);
            if null_fd > 2 {
                libc::close(null_fd);
            }
        }
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
        // Legacy mode: use /dev/console for all I/O.
        let console = CString::new("/dev/console").unwrap();
        let fd = unsafe { libc::open(console.as_ptr(), libc::O_RDWR) };
        if fd >= 0 {
            unsafe { libc::ioctl(fd, libc::TIOCSCTTY as _, 0) };
            unsafe {
                libc::dup2(fd, 0);
                libc::dup2(fd, 1);
                libc::dup2(fd, 2);
                if fd > 2 {
                    libc::close(fd);
                }
            }
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

    // Build argv for execve.
    let entrypoint_c = CString::new(entrypoint)?;
    let args_c: Vec<CString> = std::iter::once(CString::new(entrypoint)?)
        .chain(args.iter().map(|a| CString::new(a.as_str()).context("invalid argument")).collect::<Result<Vec<_>, _>>()?)
        .collect();
    let mut argv: Vec<*const libc::c_char> = args_c.iter().map(|a| a.as_ptr()).collect();
    argv.push(ptr::null());

    // Build envp for execve — explicit env, no leaking guest-init env.
    let env_c: Vec<CString> = env.iter().map(|e| CString::new(e.as_str()).context("invalid env var")).collect::<Result<Vec<_>, _>>()?;
    let mut envp: Vec<*const libc::c_char> = env_c.iter().map(|e| e.as_ptr()).collect();
    envp.push(ptr::null());

    unsafe { libc::execve(entrypoint_c.as_ptr(), argv.as_ptr(), envp.as_ptr()) };
    bail!("execve: {}", std::io::Error::last_os_error());
}

