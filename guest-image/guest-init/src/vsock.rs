use std::io::{self, BufReader, BufWriter, Read, Write};
use std::os::unix::io::{AsRawFd, FromRawFd, OwnedFd};

use anyhow::{bail, Context};

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
        let fd = unsafe { libc::socket(AF_VSOCK, libc::SOCK_STREAM, 0) };
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

        let ret = unsafe { libc::listen(fd.as_raw_fd(), 1) };
        if ret != 0 {
            bail!("listen: {}", io::Error::last_os_error());
        }

        Ok(VsockListener { fd })
    }

    pub fn accept(&self) -> anyhow::Result<VsockStream> {
        let fd = unsafe {
            libc::accept(self.fd.as_raw_fd(), std::ptr::null_mut(), std::ptr::null_mut())
        };
        if fd < 0 {
            bail!("accept: {}", io::Error::last_os_error());
        }
        let file = unsafe { std::fs::File::from_raw_fd(fd) };
        Ok(VsockStream::new(file))
    }
}

pub struct VsockStream {
    reader: BufReader<std::fs::File>,
    writer: BufWriter<std::fs::File>,
    raw_fd: i32,
}

impl VsockStream {
    fn new(file: std::fs::File) -> Self {
        let raw_fd = file.as_raw_fd();
        let reader = BufReader::new(file.try_clone().expect("clone vsock fd"));
        let writer = BufWriter::new(file);
        VsockStream { reader, writer, raw_fd }
    }

    pub fn as_raw_fd(&self) -> i32 {
        self.raw_fd
    }

    /// Returns true if the internal read buffer has data available,
    /// meaning recv() can be called without blocking on the fd.
    pub fn has_buffered_data(&self) -> bool {
        self.reader.buffer().len() > 0
    }

    /// Send a length-prefixed JSON message.
    pub fn send<T: serde::Serialize>(&mut self, msg: &T) -> anyhow::Result<()> {
        let json = serde_json::to_vec(msg).context("serialize message")?;
        let len = (json.len() as u32).to_le_bytes();
        self.writer.write_all(&len).context("write length")?;
        self.writer.write_all(&json).context("write payload")?;
        self.writer.flush().context("flush")?;
        Ok(())
    }

    /// Receive a length-prefixed JSON message.
    pub fn recv<T: serde::de::DeserializeOwned>(&mut self) -> anyhow::Result<T> {
        let mut len_buf = [0u8; 4];
        self.reader.read_exact(&mut len_buf).context("read length")?;
        let len = u32::from_le_bytes(len_buf) as usize;
        if len > 1024 * 1024 {
            bail!("message too large: {} bytes", len);
        }
        let mut buf = vec![0u8; len];
        self.reader.read_exact(&mut buf).context("read payload")?;
        serde_json::from_slice(&buf).context("deserialize message")
    }
}
