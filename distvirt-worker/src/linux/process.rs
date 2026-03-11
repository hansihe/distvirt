//! Process monitoring helpers.

use std::os::fd::{FromRawFd, OwnedFd};
use std::process::ExitStatus;
use std::time::Duration;

use tokio::io::unix::AsyncFd;

/// Asynchronously wait for a process to exit using Linux's `pidfd_open` syscall.
///
/// Uses `pidfd_open` to get a file descriptor that becomes readable when the
/// process exits, then wraps it in `tokio::io::AsyncFd` for async polling.
/// The exit status is peeked via `waitid(WNOHANG | WNOWAIT)` so the child
/// is not reaped — `Child::wait()` can still reap it later.
pub async fn wait_for_exit_pidfd(pid: u32) -> ExitStatus {
    use std::os::unix::process::ExitStatusExt;

    let pidfd = unsafe { libc::syscall(libc::SYS_pidfd_open, pid as libc::c_int, 0) };
    if pidfd < 0 {
        log::warn!(
            "pidfd_open failed for pid {}: {}, falling back to polling",
            pid,
            std::io::Error::last_os_error()
        );
        loop {
            if !std::path::Path::new(&format!("/proc/{}", pid)).exists() {
                return ExitStatus::from_raw(255 << 8);
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    // SAFETY: pidfd is a valid fd returned by the kernel.
    let owned_fd = unsafe { OwnedFd::from_raw_fd(pidfd as i32) };
    let async_fd = AsyncFd::new(owned_fd).expect("AsyncFd::new on pidfd");

    // Wait for the fd to become readable (= process exited).
    let _ = async_fd.readable().await.expect("pidfd readable");

    peek_exit_status(pid)
}

/// Peek at a process exit status with `waitid(WNOHANG | WNOWAIT)` — does not reap.
fn peek_exit_status(pid: u32) -> ExitStatus {
    use std::os::unix::process::ExitStatusExt;

    let mut siginfo: libc::siginfo_t = unsafe { std::mem::zeroed() };
    let ret = unsafe {
        libc::waitid(
            libc::P_PID,
            pid,
            &mut siginfo,
            libc::WEXITED | libc::WNOHANG | libc::WNOWAIT,
        )
    };

    if ret == 0 {
        let si_status = unsafe { siginfo.si_status() };
        let si_code = siginfo.si_code;

        let raw_status = if si_code == libc::CLD_EXITED {
            si_status << 8
        } else {
            si_status
        };
        ExitStatus::from_raw(raw_status)
    } else {
        log::warn!(
            "waitid for pid {} failed: {}",
            pid,
            std::io::Error::last_os_error()
        );
        ExitStatus::from_raw(255 << 8)
    }
}
