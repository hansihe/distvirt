use std::io;
use std::os::unix::io::{AsFd, AsRawFd, BorrowedFd, FromRawFd, OwnedFd};

use anyhow::{bail, Context};
use futures_lite::io::{AsyncReadExt, AsyncWriteExt};

const AF_VSOCK: i32 = 40;
const VMADDR_CID_ANY: u32 = u32::MAX;

#[repr(C)]
struct sockaddr_vm {
    svm_family: libc::sa_family_t,
    svm_reserved1: u16,
    svm_port: u32,
    svm_cid: u32,
    svm_zero: [u8; 4],
}

pub struct VsockListener {
    fd: OwnedFd,
}

impl VsockListener {
    pub fn bind(port: u32) -> anyhow::Result<Self> {
        let fd = unsafe { libc::socket(AF_VSOCK, libc::SOCK_STREAM | libc::SOCK_CLOEXEC, 0) };
        if fd < 0 {
            bail!("socket(AF_VSOCK): {}", io::Error::last_os_error());
        }
        let fd = unsafe { OwnedFd::from_raw_fd(fd) };

        let addr = sockaddr_vm {
            svm_family: AF_VSOCK as libc::sa_family_t,
            svm_reserved1: 0,
            svm_port: port,
            svm_cid: VMADDR_CID_ANY,
            svm_zero: [0; 4],
        };

        let ret = unsafe {
            libc::bind(
                fd.as_raw_fd(),
                &addr as *const sockaddr_vm as *const libc::sockaddr,
                std::mem::size_of::<sockaddr_vm>() as libc::socklen_t,
            )
        };
        if ret != 0 {
            bail!("bind(vsock port {}): {}", port, io::Error::last_os_error());
        }

        let ret = unsafe { libc::listen(fd.as_raw_fd(), 4) };
        if ret != 0 {
            bail!("listen: {}", io::Error::last_os_error());
        }

        Ok(VsockListener { fd })
    }

    /// Blocking accept — returns a raw File for the accepted connection.
    pub fn accept(&self) -> anyhow::Result<std::fs::File> {
        let fd = unsafe {
            libc::accept4(
                self.fd.as_raw_fd(),
                std::ptr::null_mut(),
                std::ptr::null_mut(),
                libc::SOCK_CLOEXEC,
            )
        };
        if fd < 0 {
            bail!("accept: {}", io::Error::last_os_error());
        }
        Ok(unsafe { std::fs::File::from_raw_fd(fd) })
    }
}

impl AsFd for VsockListener {
    fn as_fd(&self) -> BorrowedFd<'_> {
        self.fd.as_fd()
    }
}

impl AsRawFd for VsockListener {
    fn as_raw_fd(&self) -> std::os::unix::io::RawFd {
        self.fd.as_raw_fd()
    }
}

/// Send a length-prefixed JSON message over an async writer.
pub async fn send_msg<T: serde::Serialize>(
    writer: &mut (impl futures_lite::io::AsyncWrite + Unpin),
    msg: &T,
) -> anyhow::Result<()> {
    let json = serde_json::to_vec(msg).context("serialize message")?;
    if json.len() > 1024 * 1024 {
        bail!("message too large to send: {} bytes", json.len());
    }
    let len = (json.len() as u32).to_le_bytes();
    writer.write_all(&len).await.context("write length")?;
    writer.write_all(&json).await.context("write payload")?;
    writer.flush().await.context("flush")?;
    Ok(())
}

/// Receive a length-prefixed JSON message from an async reader.
pub async fn recv_msg<T: serde::de::DeserializeOwned>(
    reader: &mut (impl futures_lite::io::AsyncRead + Unpin),
) -> anyhow::Result<T> {
    let mut len_buf = [0u8; 4];
    reader.read_exact(&mut len_buf).await.context("read length")?;
    let len = u32::from_le_bytes(len_buf) as usize;
    if len > 1024 * 1024 {
        bail!("message too large: {} bytes", len);
    }
    let mut buf = vec![0u8; len];
    reader.read_exact(&mut buf).await.context("read payload")?;
    serde_json::from_slice(&buf).context("deserialize message")
}
