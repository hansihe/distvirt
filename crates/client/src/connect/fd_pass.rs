//! File descriptor passing over Unix domain sockets using `SCM_RIGHTS`.

use std::os::fd::{AsRawFd, FromRawFd, OwnedFd, RawFd};
use std::path::Path;

use anyhow::{Context, bail};

/// Create a `SOCK_SEQPACKET` Unix domain socket listener bound to `path`.
pub fn listen(path: &Path) -> anyhow::Result<OwnedFd> {
    let fd = unsafe { libc::socket(libc::AF_UNIX, libc::SOCK_SEQPACKET, 0) };
    if fd < 0 {
        bail!(
            "socket(AF_UNIX, SOCK_SEQPACKET): {}",
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

/// Connect to a `SOCK_SEQPACKET` Unix domain socket at `path`.
pub fn connect(path: &Path) -> anyhow::Result<OwnedFd> {
    let fd = unsafe { libc::socket(libc::AF_UNIX, libc::SOCK_SEQPACKET, 0) };
    if fd < 0 {
        bail!(
            "socket(AF_UNIX, SOCK_SEQPACKET): {}",
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

/// Send a file descriptor and a payload message over a connected Unix socket.
pub fn send_fd(sock: &OwnedFd, fd: RawFd, payload: &[u8]) -> anyhow::Result<()> {
    let iov = libc::iovec {
        iov_base: payload.as_ptr() as *mut libc::c_void,
        iov_len: payload.len(),
    };

    // Build control message with SCM_RIGHTS.
    let cmsg_space = unsafe { libc::CMSG_SPACE(std::mem::size_of::<RawFd>() as u32) } as usize;
    let mut cmsg_buf = vec![0u8; cmsg_space];

    let mut msg: libc::msghdr = unsafe { std::mem::zeroed() };
    msg.msg_iov = &iov as *const libc::iovec as *mut libc::iovec;
    msg.msg_iovlen = 1;
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

    let ret = unsafe { libc::sendmsg(sock.as_raw_fd(), &msg, 0) };
    if ret < 0 {
        bail!("sendmsg: {}", std::io::Error::last_os_error());
    }
    Ok(())
}

/// Send a payload message without a file descriptor.
pub fn send_msg(sock: &OwnedFd, payload: &[u8]) -> anyhow::Result<()> {
    let iov = libc::iovec {
        iov_base: payload.as_ptr() as *mut libc::c_void,
        iov_len: payload.len(),
    };

    let mut msg: libc::msghdr = unsafe { std::mem::zeroed() };
    msg.msg_iov = &iov as *const libc::iovec as *mut libc::iovec;
    msg.msg_iovlen = 1;

    let ret = unsafe { libc::sendmsg(sock.as_raw_fd(), &msg, 0) };
    if ret < 0 {
        bail!("sendmsg: {}", std::io::Error::last_os_error());
    }
    Ok(())
}

/// Receive a message that may contain a file descriptor via `SCM_RIGHTS`.
///
/// Returns `(payload_bytes, optional_fd)`.
pub fn recv_fd(sock: &OwnedFd, buf: &mut [u8]) -> anyhow::Result<(usize, Option<OwnedFd>)> {
    let mut iov = libc::iovec {
        iov_base: buf.as_mut_ptr() as *mut libc::c_void,
        iov_len: buf.len(),
    };

    let cmsg_space = unsafe { libc::CMSG_SPACE(std::mem::size_of::<RawFd>() as u32) } as usize;
    let mut cmsg_buf = vec![0u8; cmsg_space];

    let mut msg: libc::msghdr = unsafe { std::mem::zeroed() };
    msg.msg_iov = &mut iov;
    msg.msg_iovlen = 1;
    msg.msg_control = cmsg_buf.as_mut_ptr() as *mut libc::c_void;
    msg.msg_controllen = cmsg_space as _;

    let n = unsafe { libc::recvmsg(sock.as_raw_fd(), &mut msg, 0) };
    if n < 0 {
        bail!("recvmsg: {}", std::io::Error::last_os_error());
    }

    // Extract fd from control message if present.
    let mut received_fd = None;
    let mut cmsg = unsafe { libc::CMSG_FIRSTHDR(&msg) };
    while !cmsg.is_null() {
        unsafe {
            if (*cmsg).cmsg_level == libc::SOL_SOCKET && (*cmsg).cmsg_type == libc::SCM_RIGHTS {
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

    Ok((n as usize, received_fd))
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
