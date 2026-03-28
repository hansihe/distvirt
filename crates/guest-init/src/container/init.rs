//! Container init entry point.
//!
//! When guest-init is exec'd with `--container-init <pipe-fd>`, this module
//! takes over. It reads a [`ContainerInitConfig`] from the pipe, then performs
//! all container setup (pivot_root, mounts, setuid, exec) in a clean,
//! single-threaded process.

use std::ffi::CString;
use std::io::{self, BufRead, Read};
use std::os::unix::io::FromRawFd;
use std::ptr;

use anyhow::{Context, bail};
use serde::{Deserialize, Serialize};

/// Everything the container-init process needs to set up and exec the
/// container workload. Serialized as JSON over a pipe from the parent.
#[derive(Serialize, Deserialize)]
pub struct ContainerInitConfig {
    pub mount_point: String,
    pub program: String,
    pub args: Vec<String>,
    pub env: Vec<String>,
    pub working_dir: Option<String>,
    pub uid: Option<u32>,
    pub gid: Option<u32>,
    pub hostname: Option<String>,
    pub domainname: Option<String>,
    /// Raw fd numbers for stdio pipes. The parent clears O_CLOEXEC on these
    /// before exec so they survive into this process.
    pub stdout_write_fd: Option<i32>,
    pub stderr_write_fd: Option<i32>,
    pub stdin_read_fd: Option<i32>,
}

/// Entry point for `--container-init <pipe-fd>`. Never returns.
pub fn container_init_main(pipe_fd: i32) -> ! {
    let result = exec_container(pipe_fd);
    if let Err(e) = result {
        eprintln!("container-init failed: {:#}", e);
    }
    unsafe { libc::_exit(127) }
}

fn read_config(pipe_fd: i32) -> anyhow::Result<ContainerInitConfig> {
    let mut file = unsafe { std::fs::File::from_raw_fd(pipe_fd) };
    let mut buf = Vec::new();
    file.read_to_end(&mut buf)
        .context("read config from pipe")?;
    // file is dropped here, closing the pipe fd
    serde_json::from_slice(&buf).context("deserialize container init config")
}

fn exec_container(pipe_fd: i32) -> anyhow::Result<()> {
    let mut config = read_config(pipe_fd)?;

    // Sane default umask regardless of what guest-init had.
    unsafe { libc::umask(0o022) };

    // Set Docker-compatible default rlimits.
    set_default_rlimits()?;

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

    // Prevent mount events from propagating to/from the host.
    crate::util::remount_private("/")?;

    if let Some(ref name) = config.hostname {
        let name_c = CString::new(name.as_str())?;
        if unsafe { libc::sethostname(name_c.as_ptr(), name.len()) } != 0 {
            bail!("sethostname: {}", std::io::Error::last_os_error());
        }
    }

    if let Some(ref name) = config.domainname {
        let name_c = CString::new(name.as_str())?;
        if unsafe { libc::setdomainname(name_c.as_ptr(), name.len()) } != 0 {
            bail!("setdomainname: {}", std::io::Error::last_os_error());
        }
    }

    // pivot_root into the container rootfs using the "self-pivot" trick:
    // chdir to new root, pivot_root(".", "."), then detach old root.
    let mount_point_c = CString::new(config.mount_point.as_str())?;
    if unsafe { libc::chdir(mount_point_c.as_ptr()) } != 0 {
        bail!(
            "chdir {}: {}",
            config.mount_point,
            std::io::Error::last_os_error()
        );
    }
    pivot_root(".", ".")?;
    // Detach the old root (now stacked on ".") so it's inaccessible.
    if unsafe { libc::umount2(CString::new(".")?.as_ptr(), libc::MNT_DETACH) } != 0 {
        bail!("umount2 old root: {}", std::io::Error::last_os_error());
    }

    let wd = config.working_dir.as_deref().unwrap_or("/");
    let wd_c = CString::new(wd)?;
    if unsafe { libc::chdir(wd_c.as_ptr()) } != 0 {
        bail!("chdir {}: {}", wd, std::io::Error::last_os_error());
    }

    // Mount essential filesystems inside the container.
    mount_container_filesystems()?;

    // Harden /proc: mask sensitive paths and make others readonly.
    mask_paths()?;
    readonly_paths()?;

    // Set up stdio (pipes from parent, or /dev/console fallback).
    setup_stdio(&config)?;

    // Parse passwd/group once for user identity setup.
    let target_uid = config.uid.unwrap_or(0);
    let passwd = PasswdEntry::lookup(target_uid);
    let groups = resolve_supplementary_groups(config.gid, passwd.as_ref());

    // Set supplementary groups before changing uid/gid.
    if unsafe { libc::setgroups(groups.len(), groups.as_ptr()) } != 0 {
        bail!("setgroups: {}", std::io::Error::last_os_error());
    }

    // Set gid before uid (after setuid we may lack permission for setgid).
    if let Some(g) = config.gid {
        if unsafe { libc::setgid(g) } != 0 {
            bail!("setgid({}): {}", g, std::io::Error::last_os_error());
        }
    }
    if let Some(u) = config.uid {
        if unsafe { libc::setuid(u) } != 0 {
            bail!("setuid({}): {}", u, std::io::Error::last_os_error());
        }
    }

    // Inject HOME if not already in env.
    if !config.env.iter().any(|e| e.starts_with("HOME=")) {
        if let Some(ref entry) = passwd {
            config.env.push(format!("HOME={}", entry.home));
        }
    }

    // Build argv for execve.
    let program_c = CString::new(config.program.as_str())?;
    let args_c: Vec<CString> = std::iter::once(CString::new(config.program.as_str())?)
        .chain(
            config
                .args
                .iter()
                .map(|a| CString::new(a.as_str()).context("invalid argument"))
                .collect::<Result<Vec<_>, _>>()?,
        )
        .collect();
    let mut argv: Vec<*const libc::c_char> = args_c.iter().map(|a| a.as_ptr()).collect();
    argv.push(ptr::null());

    // Build envp for execve — explicit env, no leaking guest-init env.
    let env_c: Vec<CString> = config
        .env
        .iter()
        .map(|e| CString::new(e.as_str()).context("invalid env var"))
        .collect::<Result<Vec<_>, _>>()?;
    let mut envp: Vec<*const libc::c_char> = env_c.iter().map(|e| e.as_ptr()).collect();
    envp.push(ptr::null());

    // Use execvpe-style behavior: if the program has no path separator,
    // search PATH. Otherwise use execve directly.
    if config.program.contains('/') {
        unsafe { libc::execve(program_c.as_ptr(), argv.as_ptr(), envp.as_ptr()) };
    } else {
        // execvpe searches PATH from envp. Not POSIX but supported by both
        // glibc and musl.
        unsafe { libc::execvpe(program_c.as_ptr(), argv.as_ptr(), envp.as_ptr()) };
    }
    bail!(
        "exec {}: {}",
        config.program,
        std::io::Error::last_os_error()
    );
}

fn mount_container_filesystems() -> anyhow::Result<()> {
    crate::util::mount(
        "proc",
        "/proc",
        "proc",
        libc::MS_NOSUID | libc::MS_NODEV | libc::MS_NOEXEC,
        None,
    )?;
    crate::util::mount(
        "sysfs",
        "/sys",
        "sysfs",
        libc::MS_NOSUID | libc::MS_NODEV | libc::MS_NOEXEC,
        None,
    )?;
    crate::util::mount("devtmpfs", "/dev", "devtmpfs", libc::MS_NOSUID, None)?;
    // Use mode=666 so non-root containers can allocate PTYs without being in
    // group 5 (tty). The proper fix is to support supplementary groups via
    // setgroups()/initgroups() and use gid=5,mode=620 instead.
    crate::util::mount(
        "devpts",
        "/dev/pts",
        "devpts",
        libc::MS_NOSUID | libc::MS_NOEXEC,
        Some("mode=666"),
    )?;

    std::os::unix::fs::symlink("/proc/self/fd", "/dev/fd")?;
    std::os::unix::fs::symlink("/proc/self/fd/0", "/dev/stdin")?;
    std::os::unix::fs::symlink("/proc/self/fd/1", "/dev/stdout")?;
    std::os::unix::fs::symlink("/proc/self/fd/2", "/dev/stderr")?;

    let _ = std::fs::remove_file("/dev/ptmx");
    std::os::unix::fs::symlink("pts/ptmx", "/dev/ptmx")?;

    crate::util::mount(
        "tmpfs",
        "/dev/shm",
        "tmpfs",
        libc::MS_NOSUID | libc::MS_NODEV,
        None,
    )?;
    crate::util::mount(
        "tmpfs",
        "/tmp",
        "tmpfs",
        libc::MS_NOSUID | libc::MS_NODEV,
        None,
    )?;
    crate::util::mount(
        "mqueue",
        "/dev/mqueue",
        "mqueue",
        libc::MS_NOSUID | libc::MS_NODEV | libc::MS_NOEXEC,
        None,
    )?;

    Ok(())
}

/// Set up stdin/stdout/stderr for the container.
///
/// When capture pipes are provided, they're dup2'd onto the corresponding
/// stdio fds. Otherwise /dev/console is used as a fallback (and /dev/null
/// for stdin if console is unavailable).
///
/// After exec, O_CLOEXEC closed all fds except the stdio pipes (which had
/// O_CLOEXEC explicitly cleared by the parent). After dup2 onto 0/1/2 the
/// original pipe fds are closed, leaving only stdio open.
fn setup_stdio(config: &ContainerInitConfig) -> anyhow::Result<()> {
    // Open /dev/console for controlling terminal and as fallback stdio.
    let console = CString::new("/dev/console").unwrap();
    let console_fd = unsafe { libc::open(console.as_ptr(), libc::O_RDWR) };
    if console_fd >= 0 {
        unsafe { libc::ioctl(console_fd, libc::TIOCSCTTY as _, 0) };
    }

    // stdin
    dup2_or_fallback(config.stdin_read_fd, 0, console_fd, libc::O_RDONLY);

    // stdout/stderr
    if let (Some(out_fd), Some(err_fd)) = (config.stdout_write_fd, config.stderr_write_fd) {
        dup2_and_close(out_fd, 1);
        dup2_and_close(err_fd, 2);
    } else if console_fd >= 0 {
        unsafe {
            libc::dup2(console_fd, 1);
            libc::dup2(console_fd, 2);
        }
    }

    if console_fd > 2 {
        unsafe { libc::close(console_fd) };
    }

    Ok(())
}

/// Dup `source_fd` onto `target`, or fall back to console, or /dev/null.
fn dup2_or_fallback(source_fd: Option<i32>, target: i32, console_fd: i32, devnull_flags: i32) {
    if let Some(fd) = source_fd {
        dup2_and_close(fd, target);
    } else if console_fd >= 0 {
        unsafe { libc::dup2(console_fd, target) };
    } else {
        let devnull = CString::new("/dev/null").unwrap();
        let fd = unsafe { libc::open(devnull.as_ptr(), devnull_flags) };
        if fd >= 0 {
            dup2_and_close(fd, target);
        }
    }
}

/// Dup `fd` onto `target` and close the original if it's above stderr.
fn dup2_and_close(fd: i32, target: i32) {
    unsafe {
        libc::dup2(fd, target);
        if fd > 2 {
            libc::close(fd);
        }
    }
}

/// A parsed `/etc/passwd` entry.
struct PasswdEntry {
    name: String,
    home: String,
}

impl PasswdEntry {
    /// Find the entry for `uid` in `/etc/passwd`.
    fn lookup(uid: u32) -> Option<PasswdEntry> {
        let file = std::fs::File::open("/etc/passwd").ok()?;
        let reader = io::BufReader::new(file);
        for line in reader.lines().flatten() {
            // name:password:uid:gid:gecos:home:shell
            let fields: Vec<&str> = line.split(':').collect();
            if fields.len() >= 6 {
                if let Ok(entry_uid) = fields[2].parse::<u32>() {
                    if entry_uid == uid {
                        return Some(PasswdEntry {
                            name: fields[0].to_string(),
                            home: fields[5].to_string(),
                        });
                    }
                }
            }
        }
        None
    }
}

/// Build the supplementary group list from `/etc/group`.
/// Always includes `primary_gid` (if set), plus any groups the user is a member of.
fn resolve_supplementary_groups(
    primary_gid: Option<u32>,
    passwd: Option<&PasswdEntry>,
) -> Vec<libc::gid_t> {
    let mut groups: Vec<libc::gid_t> = Vec::new();

    if let Some(g) = primary_gid {
        groups.push(g);
    }

    if let Some(entry) = passwd {
        if let Ok(file) = std::fs::File::open("/etc/group") {
            let reader = io::BufReader::new(file);
            for line in reader.lines().flatten() {
                // name:password:gid:members
                let fields: Vec<&str> = line.split(':').collect();
                if fields.len() >= 4 {
                    if let Ok(gid) = fields[2].parse::<libc::gid_t>() {
                        let members = fields[3].split(',');
                        if members.into_iter().any(|m| m == entry.name) && !groups.contains(&gid) {
                            groups.push(gid);
                        }
                    }
                }
            }
        }
    }

    groups
}

/// Set Docker-compatible default resource limits.
fn set_default_rlimits() -> anyhow::Result<()> {
    let defaults: &[(libc::c_int, libc::rlimit)] = &[
        // NOFILE: 1024 soft, 1048576 hard (Docker default).
        (
            libc::RLIMIT_NOFILE as _,
            libc::rlimit {
                rlim_cur: 1024,
                rlim_max: 1048576,
            },
        ),
        // NPROC: unlimited (Docker default).
        (
            libc::RLIMIT_NPROC as _,
            libc::rlimit {
                rlim_cur: libc::RLIM_INFINITY,
                rlim_max: libc::RLIM_INFINITY,
            },
        ),
        // CORE: unlimited soft+hard so core dumps are possible if configured.
        (
            libc::RLIMIT_CORE as _,
            libc::rlimit {
                rlim_cur: libc::RLIM_INFINITY,
                rlim_max: libc::RLIM_INFINITY,
            },
        ),
    ];
    for &(resource, ref limit) in defaults {
        if unsafe { libc::setrlimit(resource as _, limit) } != 0 {
            bail!(
                "setrlimit({}): {}",
                resource,
                std::io::Error::last_os_error()
            );
        }
    }
    Ok(())
}

/// `pivot_root` syscall wrapper (not exposed by libc crate).
fn pivot_root(new_root: &str, put_old: &str) -> anyhow::Result<()> {
    let new_root_c = CString::new(new_root)?;
    let put_old_c = CString::new(put_old)?;
    let ret = unsafe {
        libc::syscall(
            libc::SYS_pivot_root,
            new_root_c.as_ptr(),
            put_old_c.as_ptr(),
        )
    };
    if ret != 0 {
        bail!("pivot_root: {}", std::io::Error::last_os_error());
    }
    Ok(())
}

/// Mask sensitive /proc and /sys paths by bind-mounting /dev/null over them.
fn mask_paths() -> anyhow::Result<()> {
    let null = CString::new("/dev/null").unwrap();
    let paths = [
        "/proc/kcore",
        "/proc/keys",
        "/proc/timer_list",
        "/proc/sched_debug",
    ];
    for path in &paths {
        let path_c = CString::new(*path)?;
        // Skip paths that don't exist in this kernel.
        if unsafe { libc::access(path_c.as_ptr(), libc::F_OK) } != 0 {
            continue;
        }
        let ret = unsafe {
            libc::mount(
                null.as_ptr(),
                path_c.as_ptr(),
                ptr::null(),
                libc::MS_BIND,
                ptr::null(),
            )
        };
        if ret != 0 {
            let err = std::io::Error::last_os_error();
            // ENOENT/ENOTDIR are fine — the path may not exist in this rootfs.
            if err.raw_os_error() != Some(libc::ENOENT)
                && err.raw_os_error() != Some(libc::ENOTDIR)
            {
                bail!("mask {}: {}", path, err);
            }
        }
    }
    Ok(())
}

/// Make certain /proc paths readonly to prevent container writes.
fn readonly_paths() -> anyhow::Result<()> {
    let paths = [
        "/proc/bus",
        "/proc/fs",
        "/proc/irq",
        "/proc/sys",
        "/proc/sysrq-trigger",
    ];
    for path in &paths {
        let path_c = CString::new(*path)?;
        if unsafe { libc::access(path_c.as_ptr(), libc::F_OK) } != 0 {
            continue;
        }
        // Bind mount onto itself, then remount readonly.
        let ret = unsafe {
            libc::mount(
                path_c.as_ptr(),
                path_c.as_ptr(),
                ptr::null(),
                libc::MS_BIND | libc::MS_REC,
                ptr::null(),
            )
        };
        if ret != 0 {
            let err = std::io::Error::last_os_error();
            if err.raw_os_error() != Some(libc::ENOENT)
                && err.raw_os_error() != Some(libc::ENOTDIR)
            {
                bail!("bind {}: {}", path, err);
            }
            continue;
        }
        let ret = unsafe {
            libc::mount(
                ptr::null(),
                path_c.as_ptr(),
                ptr::null(),
                libc::MS_BIND | libc::MS_REC | libc::MS_RDONLY | libc::MS_REMOUNT,
                ptr::null(),
            )
        };
        if ret != 0 {
            bail!("readonly remount {}: {}", path, std::io::Error::last_os_error());
        }
    }
    Ok(())
}
