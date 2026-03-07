use std::os::unix::io::{AsRawFd, FromRawFd, OwnedFd};

use anyhow::{bail, Context};
use async_io::Async;

/// A change in balloon page count observed via sysfs.
pub struct BalloonChange {
    pub old_pages: u32,
    pub new_pages: u32,
}

/// Find the sysfs `num_pages` file exposed by our patched virtio_balloon driver.
fn find_num_pages_path() -> anyhow::Result<String> {
    // Walk /sys/bus/virtio/devices/*/num_pages — the attribute lives on the
    // virtio device node itself.
    let virtio_devices = "/sys/bus/virtio/devices";
    let dir = std::fs::read_dir(virtio_devices)
        .with_context(|| format!("read_dir {}", virtio_devices))?;
    for entry in dir {
        let entry = entry?;
        let candidate = format!("{}/num_pages", entry.path().display());
        if std::path::Path::new(&candidate).exists() {
            return Ok(candidate);
        }
    }
    bail!("no virtio device with num_pages attribute found under {}", virtio_devices);
}

/// Monitor the virtio_balloon `num_pages` sysfs attribute for changes.
///
/// Uses epoll with `EPOLLPRI` on the sysfs file — the kernel sends
/// `sysfs_notify()` whenever the balloon size changes, which wakes epoll.
/// On each wake we re-read the value and send a `BalloonChange` through
/// the channel.
///
/// Exits if the channel closes.
pub async fn run(tx: async_channel::Sender<BalloonChange>) -> anyhow::Result<()> {
    let path = find_num_pages_path()?;
    log::info!("[balloon_monitor] watching {}", path);

    // Open the sysfs file for reading.
    let path_c = std::ffi::CString::new(path.as_str())?;
    let fd = unsafe { libc::open(path_c.as_ptr(), libc::O_RDONLY | libc::O_CLOEXEC) };
    if fd < 0 {
        bail!("open {}: {}", path, std::io::Error::last_os_error());
    }
    let sysfs_fd = unsafe { OwnedFd::from_raw_fd(fd) };

    // Do an initial read to prime the value and consume the initial EPOLLPRI.
    let mut current_pages = read_sysfs_u32(&sysfs_fd)?;
    log::info!("[balloon_monitor] initial num_pages={}", current_pages);

    // Create an epoll fd and register the sysfs fd with EPOLLPRI | EPOLLET.
    let epoll_fd = unsafe { libc::epoll_create1(libc::EPOLL_CLOEXEC) };
    if epoll_fd < 0 {
        bail!("epoll_create1: {}", std::io::Error::last_os_error());
    }
    let epoll_fd = unsafe { OwnedFd::from_raw_fd(epoll_fd) };

    let mut ev = libc::epoll_event {
        events: (libc::EPOLLPRI | libc::EPOLLET) as u32,
        u64: 0,
    };
    if unsafe {
        libc::epoll_ctl(
            epoll_fd.as_raw_fd(),
            libc::EPOLL_CTL_ADD,
            sysfs_fd.as_raw_fd(),
            &mut ev,
        )
    } < 0
    {
        bail!("epoll_ctl ADD: {}", std::io::Error::last_os_error());
    }

    // Wrap the epoll fd in Async so the smol reactor can drive it.
    let async_epoll = Async::new_nonblocking(epoll_fd)
        .context("wrap epoll fd in Async")?;

    loop {
        // Wait until the epoll fd is readable (i.e. EPOLLPRI fired).
        async_epoll.readable().await.ok();

        // Drain epoll events (non-blocking).
        let mut events = [libc::epoll_event { events: 0, u64: 0 }; 4];
        unsafe {
            libc::epoll_wait(
                async_epoll.as_raw_fd(),
                events.as_mut_ptr(),
                events.len() as i32,
                0,
            );
        }

        // Re-read the value.
        match read_sysfs_u32(&sysfs_fd) {
            Ok(new_pages) => {
                if new_pages != current_pages {
                    let direction = if new_pages > current_pages {
                        "inflate"
                    } else {
                        "deflate"
                    };
                    log::info!(
                        "[balloon_monitor] {} {} -> {} pages ({} -> {} MiB)",
                        direction,
                        current_pages,
                        new_pages,
                        balloon_pages_to_mib(current_pages),
                        balloon_pages_to_mib(new_pages),
                    );

                    let change = BalloonChange {
                        old_pages: current_pages,
                        new_pages,
                    };
                    current_pages = new_pages;

                    // Exit if the receiver is gone.
                    if tx.send(change).await.is_err() {
                        log::info!("[balloon_monitor] channel closed, exiting");
                        return Ok(());
                    }
                }
            }
            Err(e) => {
                log::warn!("[balloon_monitor] failed to read num_pages: {:#}", e);
            }
        }
    }
}

/// Read a sysfs file as a u32. Seeks to the beginning before reading.
fn read_sysfs_u32(fd: &OwnedFd) -> anyhow::Result<u32> {
    unsafe {
        libc::lseek(fd.as_raw_fd(), 0, libc::SEEK_SET);
    }
    let mut buf = [0u8; 32];
    let n = unsafe {
        libc::read(
            fd.as_raw_fd(),
            buf.as_mut_ptr() as *mut libc::c_void,
            buf.len(),
        )
    };
    if n < 0 {
        bail!("read sysfs: {}", std::io::Error::last_os_error());
    }
    let s = std::str::from_utf8(&buf[..n as usize])
        .context("sysfs value not utf8")?
        .trim();
    s.parse::<u32>().with_context(|| format!("parse '{}' as u32", s))
}

/// Virtio balloon pages are always 4K per the virtio spec,
/// regardless of the system's native page size.
pub const VIRTIO_BALLOON_PAGE_SIZE: u32 = 4096;
pub const VIRTIO_BALLOON_PAGES_PER_MIB: u32 = (1024 * 1024) / VIRTIO_BALLOON_PAGE_SIZE;

/// Convert balloon pages (4K each) to MiB.
fn balloon_pages_to_mib(pages: u32) -> u32 {
    pages / VIRTIO_BALLOON_PAGES_PER_MIB
}
