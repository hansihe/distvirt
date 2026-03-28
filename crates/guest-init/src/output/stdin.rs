use std::os::unix::io::{AsRawFd, OwnedFd};
use std::pin::Pin;

use async_io::Async;
use futures::io::AsyncRead;

/// Relay data from a yamux inbound stream to a container's stdin pipe.
///
/// The stdin pipe fd is already O_NONBLOCK (set at creation time in
/// ContainerManager::start — dup shares the file description). We wrap it
/// in `Async` so writes yield to the reactor instead of blocking.
pub async fn relay_stdin(mut yamux_stream: yamux::Stream, stdin_write_fd: OwnedFd) {
    let async_fd = match Async::new_nonblocking(stdin_write_fd) {
        Ok(fd) => fd,
        Err(e) => {
            log::warn!("wrap stdin pipe in Async: {}", e);
            return;
        }
    };

    let mut buf = [0u8; 8192];
    loop {
        let result =
            std::future::poll_fn(|cx| Pin::new(&mut yamux_stream).poll_read(cx, &mut buf)).await;
        match result {
            Ok(0) => break, // EOF from host
            Ok(n) => {
                let mut offset = 0;
                while offset < n {
                    match async_fd
                        .write_with(|fd| {
                            let written = unsafe {
                                libc::write(
                                    fd.as_raw_fd(),
                                    buf[offset..n].as_ptr() as *const libc::c_void,
                                    n - offset,
                                )
                            };
                            if written < 0 {
                                Err(std::io::Error::last_os_error())
                            } else {
                                Ok(written as usize)
                            }
                        })
                        .await
                    {
                        Ok(written) => offset += written,
                        Err(e) => {
                            log::warn!("write to stdin pipe: {}", e);
                            return;
                        }
                    }
                }
            }
            Err(e) => {
                log::warn!("read from yamux stdin stream: {}", e);
                break;
            }
        }
    }
    // Drop async_fd -> closes stdin_write_fd -> container sees EOF on stdin.
}
