//! File descriptor passing over Unix domain sockets using `SCM_RIGHTS`.

use std::os::fd::{AsRawFd, FromRawFd, OwnedFd, RawFd};
use std::path::Path;

use anyhow::{Context, bail};

/// Create a `SOCK_STREAM` Unix domain socket listener bound to `path`.
pub fn listen(path: &Path) -> anyhow::Result<OwnedFd> {
    let fd = unsafe { libc::socket(libc::AF_UNIX, libc::SOCK_STREAM, 0) };
    if fd < 0 {
        bail!(
            "socket(AF_UNIX, SOCK_STREAM): {}",
            std::io::Error::last_os_error()
        );
    }
    let sock = unsafe { OwnedFd::from_raw_fd(fd) };

    let mut addr: libc::sockaddr_un = unsafe { std::mem::zeroed() };
    addr.sun_family = libc::AF_UNIX as _;
    let path_bytes = path
        .as_os_str()
        .as_encoded_bytes();
    if path_bytes.len() >= addr.sun_path.len() {
        bail!("socket path too long");
    }
    unsafe {
        std::ptr::copy_nonoverlapping(
            path_bytes.as_ptr(),
            addr.sun_path.as_mut_ptr() as *mut u8,
            path_bytes.len(),
        );
    }

    let addr_len =
        std::mem::offset_of!(libc::sockaddr_un, sun_path) + path_bytes.len() + 1;
    let ret = unsafe {
        libc::bind(
            sock.as_raw_fd(),
            &addr as *const libc::sockaddr_un as *const libc::sockaddr,
            addr_len as libc::socklen_t,
        )
    };
    if ret < 0 {
        bail!("bind: {}", std::io::Error::last_os_error());
    }

    let ret = unsafe { libc::listen(sock.as_raw_fd(), 1) };
    if ret < 0 {
        bail!("listen: {}", std::io::Error::last_os_error());
    }

    Ok(sock)
}

/// Accept a single connection on a listener socket.
pub fn accept(listener: &OwnedFd) -> anyhow::Result<OwnedFd> {
    let fd = unsafe { libc::accept(listener.as_raw_fd(), std::ptr::null_mut(), std::ptr::null_mut()) };
    if fd < 0 {
        bail!("accept: {}", std::io::Error::last_os_error());
    }
    Ok(unsafe { OwnedFd::from_raw_fd(fd) })
}

/// Connect to a `SOCK_STREAM` Unix domain socket at `path`.
pub fn connect(path: &Path) -> anyhow::Result<OwnedFd> {
    let fd = unsafe { libc::socket(libc::AF_UNIX, libc::SOCK_STREAM, 0) };
    if fd < 0 {
        bail!(
            "socket(AF_UNIX, SOCK_STREAM): {}",
            std::io::Error::last_os_error()
        );
    }
    let sock = unsafe { OwnedFd::from_raw_fd(fd) };

    let mut addr: libc::sockaddr_un = unsafe { std::mem::zeroed() };
    addr.sun_family = libc::AF_UNIX as _;
    let path_bytes = path.as_os_str().as_encoded_bytes();
    if path_bytes.len() >= addr.sun_path.len() {
        bail!("socket path too long");
    }
    unsafe {
        std::ptr::copy_nonoverlapping(
            path_bytes.as_ptr(),
            addr.sun_path.as_mut_ptr() as *mut u8,
            path_bytes.len(),
        );
    }

    let addr_len =
        std::mem::offset_of!(libc::sockaddr_un, sun_path) + path_bytes.len() + 1;
    let ret = unsafe {
        libc::connect(
            sock.as_raw_fd(),
            &addr as *const libc::sockaddr_un as *const libc::sockaddr,
            addr_len as libc::socklen_t,
        )
    };
    if ret < 0 {
        bail!("connect: {}", std::io::Error::last_os_error());
    }

    Ok(sock)
}

/// Send a length-prefixed payload over a connected Unix socket, optionally
/// passing a file descriptor via `SCM_RIGHTS`.
pub fn send_msg(sock: &OwnedFd, fd: Option<RawFd>, payload: &[u8]) -> anyhow::Result<()> {
    let len_bytes = (payload.len() as u32).to_le_bytes();
    let iovs = [
        libc::iovec {
            iov_base: len_bytes.as_ptr() as *mut libc::c_void,
            iov_len: len_bytes.len(),
        },
        libc::iovec {
            iov_base: payload.as_ptr() as *mut libc::c_void,
            iov_len: payload.len(),
        },
    ];

    let cmsg_space = unsafe { libc::CMSG_SPACE(std::mem::size_of::<RawFd>() as u32) } as usize;
    let mut cmsg_buf = vec![0u8; cmsg_space];

    let mut msg: libc::msghdr = unsafe { std::mem::zeroed() };
    msg.msg_iov = iovs.as_ptr() as *mut libc::iovec;
    msg.msg_iovlen = 2;

    if let Some(fd) = fd {
        msg.msg_control = cmsg_buf.as_mut_ptr() as *mut libc::c_void;
        msg.msg_controllen = cmsg_space as _;

        let cmsg = unsafe { libc::CMSG_FIRSTHDR(&msg) };
        unsafe {
            (*cmsg).cmsg_level = libc::SOL_SOCKET;
            (*cmsg).cmsg_type = libc::SCM_RIGHTS;
            (*cmsg).cmsg_len = libc::CMSG_LEN(std::mem::size_of::<RawFd>() as u32) as _;
            std::ptr::copy_nonoverlapping(
                &fd as *const RawFd as *const u8,
                libc::CMSG_DATA(cmsg),
                std::mem::size_of::<RawFd>(),
            );
        }
    }

    let ret = unsafe { libc::sendmsg(sock.as_raw_fd(), &msg, 0) };
    if ret < 0 {
        bail!("sendmsg: {}", std::io::Error::last_os_error());
    }
    Ok(())
}

/// Receive a length-prefixed message that may contain a file descriptor via `SCM_RIGHTS`.
///
/// Returns `(payload_bytes, optional_fd)`.  The caller's buffer must be large
/// enough for the payload (the 4-byte length prefix is consumed internally).
pub fn recv_fd(sock: &OwnedFd, buf: &mut [u8]) -> anyhow::Result<(usize, Option<OwnedFd>)> {
    let cmsg_space = unsafe { libc::CMSG_SPACE(std::mem::size_of::<RawFd>() as u32) } as usize;

    // Read the 4-byte length prefix (may require multiple reads on SOCK_STREAM).
    // The fd, if any, is attached to the first byte of stream data, so we pass
    // cmsg space only on the first recvmsg call.
    let mut header_buf = [0u8; 4];
    let mut header_read = 0usize;
    let mut received_fd: Option<OwnedFd> = None;
    while header_read < 4 {
        let mut iov = libc::iovec {
            iov_base: header_buf[header_read..].as_mut_ptr() as *mut libc::c_void,
            iov_len: 4 - header_read,
        };

        let mut cmsg_buf = vec![0u8; cmsg_space];
        let mut msg: libc::msghdr = unsafe { std::mem::zeroed() };
        msg.msg_iov = &mut iov;
        msg.msg_iovlen = 1;

        // Only pass cmsg space on the first read (fd arrives with first byte).
        if header_read == 0 {
            msg.msg_control = cmsg_buf.as_mut_ptr() as *mut libc::c_void;
            msg.msg_controllen = cmsg_space as _;
        }

        let n = unsafe { libc::recvmsg(sock.as_raw_fd(), &mut msg, 0) };
        if n < 0 {
            bail!("recvmsg: {}", std::io::Error::last_os_error());
        }
        if n == 0 {
            bail!("connection closed before length header was received");
        }

        // Extract fd from control message if present.
        if received_fd.is_none() {
            let mut cmsg = unsafe { libc::CMSG_FIRSTHDR(&msg) };
            while !cmsg.is_null() {
                unsafe {
                    if (*cmsg).cmsg_level == libc::SOL_SOCKET
                        && (*cmsg).cmsg_type == libc::SCM_RIGHTS
                    {
                        let mut fd: RawFd = 0;
                        std::ptr::copy_nonoverlapping(
                            libc::CMSG_DATA(cmsg),
                            &mut fd as *mut RawFd as *mut u8,
                            std::mem::size_of::<RawFd>(),
                        );
                        received_fd = Some(OwnedFd::from_raw_fd(fd));
                    }
                    cmsg = libc::CMSG_NXTHDR(&msg, cmsg);
                }
            }
        }

        header_read += n as usize;
    }

    let payload_len = u32::from_le_bytes(header_buf) as usize;
    if payload_len > buf.len() {
        bail!(
            "incoming message payload ({} bytes) exceeds buffer size ({} bytes)",
            payload_len,
            buf.len()
        );
    }

    // Read the payload.
    let mut payload_read = 0usize;
    while payload_read < payload_len {
        let n = unsafe {
            libc::read(
                sock.as_raw_fd(),
                buf[payload_read..payload_len].as_mut_ptr() as *mut libc::c_void,
                payload_len - payload_read,
            )
        };
        if n < 0 {
            bail!("read: {}", std::io::Error::last_os_error());
        }
        if n == 0 {
            bail!("connection closed before full payload was received");
        }
        payload_read += n as usize;
    }

    Ok((payload_len, received_fd))
}

/// Helper: set up the parent side — create temp dir, bind listener, return
/// `(listener_fd, socket_path, temp_dir)`.
///
/// The caller must keep `temp_dir` alive until communication is complete.
pub fn setup_listener() -> anyhow::Result<(OwnedFd, std::path::PathBuf, tempfile::TempDir)> {
    let tmp_dir = tempfile::Builder::new()
        .prefix("dv-tun-")
        .tempdir()
        .context("create temp dir for fd-passing socket")?;

    // Ensure the directory is only accessible by the current user.
    let ret = unsafe {
        libc::chmod(
            std::ffi::CString::new(tmp_dir.path().as_os_str().as_encoded_bytes())
                .unwrap()
                .as_ptr(),
            0o700,
        )
    };
    if ret < 0 {
        bail!("chmod: {}", std::io::Error::last_os_error());
    }

    let sock_path = tmp_dir.path().join("tun.sock");
    let listener = listen(&sock_path)?;

    Ok((listener, sock_path, tmp_dir))
}
