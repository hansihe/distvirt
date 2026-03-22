use std::ffi::CString;
use std::io;
use std::os::unix::io::{FromRawFd, OwnedFd};
use std::ptr;

use anyhow::{Context, bail};

/// Mount a filesystem. Creates the target directory if it doesn't exist.
pub fn mount(
    source: &str,
    target: &str,
    fstype: &str,
    flags: libc::c_ulong,
    data: Option<&str>,
) -> anyhow::Result<()> {
    let source_c = CString::new(source).context("invalid source string")?;
    let target_c = CString::new(target).context("invalid target string")?;
    let fstype_c = CString::new(fstype).context("invalid fstype string")?;
    let data_c = data
        .map(|d| CString::new(d))
        .transpose()
        .context("invalid data string")?;

    std::fs::create_dir_all(target).with_context(|| format!("creating mount point {}", target))?;

    let ret = unsafe {
        libc::mount(
            source_c.as_ptr(),
            target_c.as_ptr(),
            fstype_c.as_ptr(),
            flags,
            data_c
                .as_ref()
                .map(|d| d.as_ptr() as *const libc::c_void)
                .unwrap_or(ptr::null()),
        )
    };
    if ret != 0 {
        bail!(
            "mount {} on {} ({}): {}",
            source,
            target,
            fstype,
            io::Error::last_os_error(),
        );
    }
    Ok(())
}

/// Create a pipe with `O_CLOEXEC`. Returns `(read_end, write_end)`.
pub fn create_pipe() -> anyhow::Result<(OwnedFd, OwnedFd)> {
    let mut fds: [libc::c_int; 2] = [-1, -1];
    if unsafe { libc::pipe2(fds.as_mut_ptr(), libc::O_CLOEXEC) } != 0 {
        bail!("pipe2: {}", io::Error::last_os_error());
    }
    let read_end = unsafe { OwnedFd::from_raw_fd(fds[0]) };
    let write_end = unsafe { OwnedFd::from_raw_fd(fds[1]) };
    Ok((read_end, write_end))
}

/// Result of reading from a non-blocking pipe.
pub enum ReadPipeResult {
    /// Data was read.
    Data(Vec<u8>),
    /// No data available right now (EAGAIN/EWOULDBLOCK).
    WouldBlock,
    /// End of file — the write end of the pipe has been closed.
    Eof,
}

/// Read available data from a non-blocking pipe fd.
pub fn read_pipe(fd: i32) -> io::Result<ReadPipeResult> {
    let mut buf = [0u8; 8192];
    loop {
        let n = unsafe { libc::read(fd, buf.as_mut_ptr() as *mut libc::c_void, buf.len()) };
        if n > 0 {
            return Ok(ReadPipeResult::Data(buf[..n as usize].to_vec()));
        } else if n == 0 {
            return Ok(ReadPipeResult::Eof);
        } else {
            let err = io::Error::last_os_error();
            if err.raw_os_error() == Some(libc::EINTR) {
                continue;
            }
            if err.raw_os_error() == Some(libc::EAGAIN)
                || err.raw_os_error() == Some(libc::EWOULDBLOCK)
            {
                return Ok(ReadPipeResult::WouldBlock);
            }
            return Err(err);
        }
    }
}
