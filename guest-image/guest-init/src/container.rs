use std::collections::HashMap;
use std::ffi::CString;
use std::ptr;

use anyhow::{bail, Context};

struct Container {
    id: String,
    mount_point: String,
    pid: Option<libc::pid_t>,
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

    /// Mount the block device as ext4 at /containers/<id>.
    pub fn add(&mut self, id: String, device: String) -> anyhow::Result<()> {
        if self.containers.contains_key(&id) {
            bail!("container {} already exists", id);
        }

        let mount_point = format!("/containers/{}", id);
        std::fs::create_dir_all(&mount_point)
            .with_context(|| format!("creating {}", mount_point))?;

        let device_c = CString::new(device.as_str()).unwrap();
        let mount_point_c = CString::new(mount_point.as_str()).unwrap();
        let fstype_c = CString::new("ext4").unwrap();

        let ret = unsafe {
            libc::mount(
                device_c.as_ptr(),
                mount_point_c.as_ptr(),
                fstype_c.as_ptr(),
                0,
                ptr::null(),
            )
        };
        if ret != 0 {
            bail!(
                "mount {} on {}: {}",
                device,
                mount_point,
                std::io::Error::last_os_error()
            );
        }

        log::info!("mounted {} at {}", device, mount_point);
        self.containers.insert(
            id.clone(),
            Container {
                id,
                mount_point,
                pid: None,
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
    ) -> anyhow::Result<u32> {
        let container = self
            .containers
            .get_mut(id)
            .with_context(|| format!("container {} not found", id))?;

        if container.pid.is_some() {
            bail!("container {} is already running", id);
        }

        let pid = unsafe { libc::fork() };
        if pid < 0 {
            bail!("fork: {}", std::io::Error::last_os_error());
        }

        if pid == 0 {
            // Child process — set up container environment and exec.
            // Any error here must _exit, not unwind.
            child_exec(&container.mount_point, entrypoint, args, env, working_dir, uid, gid, hostname);
        }

        // Parent
        container.pid = Some(pid);
        log::info!(
            "container {} started with pid {}",
            id,
            pid
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
) -> ! {
    let result = child_exec_inner(mount_point, entrypoint, args, env, working_dir, uid, gid, hostname);
    if let Err(e) = result {
        // Can't use log here since we may have forked in a bad state for the logger.
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
) -> anyhow::Result<()> {
    // New session so the container process is a session leader.
    unsafe { libc::setsid() };

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

    // Mount /proc inside the container.
    mount_in_container("proc", "/proc", "proc", libc::MS_NOSUID | libc::MS_NODEV | libc::MS_NOEXEC)?;
    // Mount /sys inside the container.
    mount_in_container("sysfs", "/sys", "sysfs", libc::MS_NOSUID | libc::MS_NODEV | libc::MS_NOEXEC)?;
    // Mount /dev as devtmpfs.
    mount_in_container("devtmpfs", "/dev", "devtmpfs", libc::MS_NOSUID)?;
    // Mount /tmp as tmpfs.
    mount_in_container("tmpfs", "/tmp", "tmpfs", libc::MS_NOSUID | libc::MS_NODEV)?;

    // Acquire /dev/console as the controlling terminal so that Ctrl+C
    // on the serial console delivers SIGINT to the container process.
    {
        let console = CString::new("/dev/console").unwrap();
        let fd = unsafe { libc::open(console.as_ptr(), libc::O_RDWR) };
        if fd >= 0 {
            // TIOCSCTTY: set controlling terminal for this session.
            unsafe { libc::ioctl(fd, libc::TIOCSCTTY as _, 0) };
            // Point stdin/stdout/stderr at the console.
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
        .chain(args.iter().map(|a| CString::new(a.as_str()).unwrap()))
        .collect();
    let mut argv: Vec<*const libc::c_char> = args_c.iter().map(|a| a.as_ptr()).collect();
    argv.push(ptr::null());

    // Build envp for execve — explicit env, no leaking guest-init env.
    let env_c: Vec<CString> = env.iter().map(|e| CString::new(e.as_str()).unwrap()).collect();
    let mut envp: Vec<*const libc::c_char> = env_c.iter().map(|e| e.as_ptr()).collect();
    envp.push(ptr::null());

    unsafe { libc::execve(entrypoint_c.as_ptr(), argv.as_ptr(), envp.as_ptr()) };
    bail!("execve: {}", std::io::Error::last_os_error());
}

fn mount_in_container(
    source: &str,
    target: &str,
    fstype: &str,
    flags: libc::c_ulong,
) -> anyhow::Result<()> {
    // Create mount point if it doesn't exist (best effort).
    let _ = std::fs::create_dir_all(target);

    let source_c = CString::new(source).unwrap();
    let target_c = CString::new(target).unwrap();
    let fstype_c = CString::new(fstype).unwrap();

    let ret = unsafe {
        libc::mount(
            source_c.as_ptr(),
            target_c.as_ptr(),
            fstype_c.as_ptr(),
            flags,
            ptr::null(),
        )
    };
    if ret != 0 {
        bail!(
            "mount {} on {}: {}",
            source,
            target,
            std::io::Error::last_os_error()
        );
    }
    Ok(())
}
