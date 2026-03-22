use std::fs;
use std::os::unix::io::{AsRawFd, FromRawFd, OwnedFd};

use anyhow::{Context, bail};
use async_io::Async;

pub const CGROUP_ROOT: &str = "/sys/fs/cgroup/containers";

/// Create the parent cgroup for all containers and enable the memory controller.
pub fn init_container_cgroup_root() -> anyhow::Result<()> {
    fs::create_dir_all(CGROUP_ROOT).with_context(|| format!("create {}", CGROUP_ROOT))?;

    // Enable memory controller in the parent so child cgroups get memory.pressure.
    fs::write("/sys/fs/cgroup/cgroup.subtree_control", "+memory")
        .context("enable memory controller on root cgroup")?;

    log::info!(
        "cgroup root {} initialized with memory controller",
        CGROUP_ROOT
    );
    Ok(())
}

/// Create a per-container cgroup directory. Returns the path.
pub fn setup_container_cgroup(id: &str) -> anyhow::Result<String> {
    let path = format!("{}/{}", CGROUP_ROOT, id);
    fs::create_dir_all(&path).with_context(|| format!("create cgroup {}", path))?;
    log::info!("created cgroup {}", path);
    Ok(path)
}

/// Two PSI trigger file descriptors for two-level memory pressure monitoring.
pub struct PsiTriggers {
    /// Early warning: "some 50000 1000000" — 50ms partial stall in any 1s window.
    pub some_fd: OwnedFd,
    /// Critical: "full 20000 500000" — 20ms full stall in any 500ms window.
    pub full_fd: OwnedFd,
}

/// Open a PSI trigger on a cgroup's memory.pressure file.
///
/// Opens memory.pressure, writes a trigger string, and returns the fd.
/// The fd becomes readable when the PSI threshold is exceeded.
/// Reading from it re-arms the trigger.
fn open_psi_trigger(pressure_path: &str, trigger: &[u8]) -> anyhow::Result<OwnedFd> {
    let path_c = std::ffi::CString::new(pressure_path).context("invalid pressure path")?;
    let fd = unsafe { libc::open(path_c.as_ptr(), libc::O_RDWR | libc::O_CLOEXEC) };
    if fd < 0 {
        bail!(
            "open {}: {}",
            pressure_path,
            std::io::Error::last_os_error()
        );
    }
    let owned_fd = unsafe { OwnedFd::from_raw_fd(fd) };

    let written =
        unsafe { libc::write(fd, trigger.as_ptr() as *const libc::c_void, trigger.len()) };
    if written < 0 {
        bail!(
            "write PSI trigger to {}: {}",
            pressure_path,
            std::io::Error::last_os_error()
        );
    }

    Ok(owned_fd)
}

/// Set up two-level PSI monitoring on a cgroup's memory.pressure file.
///
/// Returns two fds: `some` (early warning) and `full` (critical, action needed).
pub fn setup_psi_monitor(cgroup_path: &str) -> anyhow::Result<PsiTriggers> {
    let pressure_path = format!("{}/memory.pressure", cgroup_path);

    let some_fd = open_psi_trigger(&pressure_path, b"some 50000 1000000")
        .context("register 'some' PSI trigger")?;
    let full_fd = open_psi_trigger(&pressure_path, b"full 20000 500000")
        .context("register 'full' PSI trigger")?;

    log::info!("PSI monitors set up on {} (some + full)", pressure_path);
    Ok(PsiTriggers { some_fd, full_fd })
}

/// Severity of a PSI memory pressure event.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PsiLevel {
    Some,
    Full,
}

/// Async PSI monitor that wraps an epoll fd in `Async` for direct integration
/// with the smol reactor. No dedicated thread or pipes needed.
pub struct AsyncPsiMonitor {
    epoll_fd: Async<OwnedFd>,
    triggers: PsiTriggers,
}

impl AsyncPsiMonitor {
    /// Create a new async PSI monitor from pre-opened triggers.
    pub fn new(triggers: PsiTriggers) -> anyhow::Result<Self> {
        let epoll_fd = unsafe { libc::epoll_create1(libc::EPOLL_CLOEXEC) };
        if epoll_fd < 0 {
            bail!("epoll_create1: {}", std::io::Error::last_os_error());
        }
        let epoll_fd = unsafe { OwnedFd::from_raw_fd(epoll_fd) };

        // Register PSI fds with EPOLLPRI (data 0=some, 1=full).
        let mut ev_some = libc::epoll_event {
            events: libc::EPOLLPRI as u32,
            u64: 0,
        };
        if unsafe {
            libc::epoll_ctl(
                epoll_fd.as_raw_fd(),
                libc::EPOLL_CTL_ADD,
                triggers.some_fd.as_raw_fd(),
                &mut ev_some,
            )
        } < 0
        {
            bail!("epoll_ctl ADD some: {}", std::io::Error::last_os_error());
        }
        let mut ev_full = libc::epoll_event {
            events: libc::EPOLLPRI as u32,
            u64: 1,
        };
        if unsafe {
            libc::epoll_ctl(
                epoll_fd.as_raw_fd(),
                libc::EPOLL_CTL_ADD,
                triggers.full_fd.as_raw_fd(),
                &mut ev_full,
            )
        } < 0
        {
            bail!("epoll_ctl ADD full: {}", std::io::Error::last_os_error());
        }

        let epoll_fd = Async::new_nonblocking(epoll_fd).context("wrap epoll fd in Async")?;

        Ok(AsyncPsiMonitor { epoll_fd, triggers })
    }

    /// Wait for a PSI event and return the highest level seen.
    pub async fn wait(&self) -> PsiLevel {
        loop {
            // The epoll fd becomes readable when it has pending events.
            self.epoll_fd.readable().await.ok();

            // Non-blocking drain of all pending epoll events.
            let mut events = [libc::epoll_event { events: 0, u64: 0 }; 4];
            let n = unsafe {
                libc::epoll_wait(
                    self.epoll_fd.as_raw_fd(),
                    events.as_mut_ptr(),
                    events.len() as i32,
                    0, // non-blocking
                )
            };

            if n <= 0 {
                continue; // spurious wakeup, wait again
            }

            let mut highest = PsiLevel::Some;
            for i in 0..n as usize {
                if events[i].u64 == 1 {
                    highest = PsiLevel::Full;
                }

                // Re-arm the PSI trigger by reading from it.
                let fd = if events[i].u64 == 0 {
                    self.triggers.some_fd.as_raw_fd()
                } else {
                    self.triggers.full_fd.as_raw_fd()
                };
                let mut buf = [0u8; 256];
                unsafe {
                    libc::read(fd, buf.as_mut_ptr() as *mut libc::c_void, buf.len());
                }
            }

            return highest;
        }
    }
}

/// Set memory.high and memory.max on a cgroup.
pub fn set_memory_limits(cgroup_path: &str, high_bytes: u64, max_bytes: u64) -> anyhow::Result<()> {
    let max_path = format!("{}/memory.max", cgroup_path);
    let high_path = format!("{}/memory.high", cgroup_path);

    // Write max first (must be >= high).
    fs::write(&max_path, max_bytes.to_string()).with_context(|| format!("write {}", max_path))?;
    fs::write(&high_path, high_bytes.to_string())
        .with_context(|| format!("write {}", high_path))?;

    log::info!(
        "set cgroup {} memory limits: high={}, max={}",
        cgroup_path,
        high_bytes,
        max_bytes
    );
    Ok(())
}

/// Read a cgroup file as bytes (e.g. memory.current, memory.high).
/// Returns `u64::MAX` for "max" (no limit).
pub fn read_cgroup_bytes(cgroup_path: &str, file: &str) -> anyhow::Result<u64> {
    let path = format!("{}/{}", cgroup_path, file);
    let content = fs::read_to_string(&path).with_context(|| format!("read {}", path))?;
    let trimmed = content.trim();
    if trimmed == "max" {
        Ok(u64::MAX)
    } else {
        Ok(trimmed.parse::<u64>()?)
    }
}

/// Parsed counters from a cgroup `memory.events` file.
#[derive(Default, PartialEq, Debug, Clone)]
pub struct MemoryEvents {
    pub low: u64,
    pub high: u64,
    pub max: u64,
    pub oom: u64,
    pub oom_kill: u64,
    pub oom_group_kill: u64,
}

impl MemoryEvents {
    /// Compute per-field deltas: `self - previous` (saturating).
    pub fn diff(&self, previous: &MemoryEvents) -> MemoryEvents {
        MemoryEvents {
            low: self.low.saturating_sub(previous.low),
            high: self.high.saturating_sub(previous.high),
            max: self.max.saturating_sub(previous.max),
            oom: self.oom.saturating_sub(previous.oom),
            oom_kill: self.oom_kill.saturating_sub(previous.oom_kill),
            oom_group_kill: self.oom_group_kill.saturating_sub(previous.oom_group_kill),
        }
    }

    pub fn parse(content: &str) -> Self {
        let mut ev = Self::default();
        for line in content.lines() {
            let mut parts = line.split_whitespace();
            if let (Some(key), Some(val)) = (parts.next(), parts.next()) {
                if let Ok(n) = val.parse::<u64>() {
                    match key {
                        "low" => ev.low = n,
                        "high" => ev.high = n,
                        "max" => ev.max = n,
                        "oom" => ev.oom = n,
                        "oom_kill" => ev.oom_kill = n,
                        "oom_group_kill" => ev.oom_group_kill = n,
                        _ => {}
                    }
                }
            }
        }
        ev
    }
}

/// Read and parse `memory.events` from a cgroup path.
pub fn read_memory_events(cgroup_path: &str) -> anyhow::Result<MemoryEvents> {
    let path = format!("{}/memory.events", cgroup_path);
    let content = fs::read_to_string(&path).with_context(|| format!("read {}", path))?;
    Ok(MemoryEvents::parse(&content))
}

/// Async monitor for cgroup `memory.events` changes using inotify.
pub struct AsyncMemoryEventsMonitor {
    inotify_fd: Async<OwnedFd>,
    cgroup_path: String,
    previous: MemoryEvents,
}

impl AsyncMemoryEventsMonitor {
    pub fn new(cgroup_path: &str) -> anyhow::Result<Self> {
        let fd = unsafe { libc::inotify_init1(libc::IN_NONBLOCK | libc::IN_CLOEXEC) };
        if fd < 0 {
            bail!("inotify_init1: {}", std::io::Error::last_os_error());
        }
        let owned_fd = unsafe { OwnedFd::from_raw_fd(fd) };

        let events_path = format!("{}/memory.events", cgroup_path);
        let path_c =
            std::ffi::CString::new(events_path.as_str()).context("invalid memory.events path")?;
        let wd = unsafe { libc::inotify_add_watch(fd, path_c.as_ptr(), libc::IN_MODIFY) };
        if wd < 0 {
            bail!(
                "inotify_add_watch {}: {}",
                events_path,
                std::io::Error::last_os_error()
            );
        }

        let inotify_fd = Async::new_nonblocking(owned_fd).context("wrap inotify fd in Async")?;

        let previous = read_memory_events(cgroup_path).unwrap_or_default();

        Ok(Self {
            inotify_fd,
            cgroup_path: cgroup_path.to_string(),
            previous,
        })
    }

    /// Wait until `memory.events` counters actually change.
    /// Returns `(diff, absolute)` — the per-field deltas and the new absolute counters.
    pub async fn wait_for_change(&mut self) -> (MemoryEvents, MemoryEvents) {
        loop {
            self.inotify_fd.readable().await.ok();

            // Drain inotify events.
            let mut buf = [0u8; 4096];
            loop {
                let n = unsafe {
                    libc::read(
                        self.inotify_fd.as_raw_fd(),
                        buf.as_mut_ptr() as *mut libc::c_void,
                        buf.len(),
                    )
                };
                if n <= 0 {
                    break;
                }
            }

            let current = read_memory_events(&self.cgroup_path).unwrap_or_default();
            if current != self.previous {
                let diff = current.diff(&self.previous);
                self.previous = current.clone();
                return (diff, current);
            }
        }
    }
}

/// Remove a per-container cgroup directory.
pub fn remove_cgroup(id: &str) {
    let path = format!("{}/{}", CGROUP_ROOT, id);
    if let Err(e) = fs::remove_dir(&path) {
        log::warn!("remove cgroup {}: {}", path, e);
    } else {
        log::info!("removed cgroup {}", path);
    }
}
