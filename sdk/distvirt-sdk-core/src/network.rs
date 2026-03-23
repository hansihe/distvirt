use std::net::SocketAddr;
use std::sync::Arc;

use pyo3::prelude::*;
use tokio::io::{self, AsyncReadExt, AsyncWriteExt};
use tokio::sync::Mutex;

use distvirt_client::connect;

use crate::client::PyClient;
use crate::ApiError;

// ---------------------------------------------------------------------------
// UserspaceNetwork
// ---------------------------------------------------------------------------

#[pyclass(name = "UserspaceNetwork")]
pub struct PyUserspaceNetwork {
    network: Arc<Mutex<Option<connect::userspace::UserspaceNetwork>>>,
    namespace_id: String,
    client_ip: String,
    subnet: String,
}

#[pymethods]
impl PyUserspaceNetwork {
    /// Provision a WireGuard tunnel and start a userspace network.
    #[staticmethod]
    fn connect<'py>(
        py: Python<'py>,
        client: &PyClient,
        namespace_id: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let mut grpc_client = client.take_client_ref()?;
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let tunnel = connect::ProvisionedTunnel::connect(&mut grpc_client, &namespace_id)
                .await
                .map_err(|e| ApiError::new_err(format!("{e}")))?;

            let info = tunnel.info();
            let client_ip = info.client_ip.to_string();
            let subnet = info.subnet.clone();

            let network = tunnel
                .into_userspace()
                .await
                .map_err(|e| ApiError::new_err(format!("{e}")))?;

            Ok(PyUserspaceNetwork {
                network: Arc::new(Mutex::new(Some(network))),
                namespace_id,
                client_ip,
                subnet,
            })
        })
    }

    /// Resolve a hostname to an IPv4 address using the namespace's DNS server.
    fn resolve<'py>(&self, py: Python<'py>, name: String) -> PyResult<Bound<'py, PyAny>> {
        let network = Arc::clone(&self.network);
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let guard = network.lock().await;
            let net = guard
                .as_ref()
                .ok_or_else(|| ApiError::new_err("network is closed"))?;

            let ip = net
                .resolve(&name)
                .await
                .map_err(|e| ApiError::new_err(format!("{e}")))?;
            Ok(ip.to_string())
        })
    }

    /// Open a TCP connection to an address inside the namespace.
    /// Accepts both IP addresses and hostnames (resolved via namespace DNS).
    fn connect_tcp<'py>(
        &self,
        py: Python<'py>,
        host: String,
        port: u16,
    ) -> PyResult<Bound<'py, PyAny>> {
        let network = Arc::clone(&self.network);
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let guard = network.lock().await;
            let net = guard
                .as_ref()
                .ok_or_else(|| ApiError::new_err("network is closed"))?;

            let stream = net
                .connect_tcp_host(&host, port)
                .await
                .map_err(|e| ApiError::new_err(format!("{e}")))?;

            let peer = stream.peer_addr().to_string();
            let (read_half, write_half) = io::split(stream);

            Ok(PyTcpStream {
                reader: Arc::new(Mutex::new(Some(read_half))),
                writer: Arc::new(Mutex::new(Some(write_half))),
                peer_addr: peer,
            })
        })
    }

    /// Bind a UDP socket inside the namespace.
    #[pyo3(signature = (port=0))]
    fn bind_udp<'py>(&self, py: Python<'py>, port: u16) -> PyResult<Bound<'py, PyAny>> {
        let network = Arc::clone(&self.network);
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let guard = network.lock().await;
            let net = guard
                .as_ref()
                .ok_or_else(|| ApiError::new_err("network is closed"))?;

            let socket = net
                .bind_udp(port)
                .await
                .map_err(|e| ApiError::new_err(format!("{e}")))?;

            let local_port = socket.local_port();

            Ok(PyUdpSocket {
                socket: Arc::new(Mutex::new(Some(socket))),
                local_port,
            })
        })
    }

    /// Disconnect the tunnel and deregister with the server.
    fn disconnect<'py>(&self, py: Python<'py>, client: &PyClient) -> PyResult<Bound<'py, PyAny>> {
        let network = Arc::clone(&self.network);
        let mut grpc_client = client.take_client_ref()?;
        let namespace_id = self.namespace_id.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let net = {
                let mut guard = network.lock().await;
                guard.take()
            };
            if let Some(net) = net {
                net.disconnect(&mut grpc_client, &namespace_id)
                    .await
                    .map_err(|e| ApiError::new_err(format!("{e}")))?;
            }
            Ok(())
        })
    }

    /// Drop the network without gRPC disconnect.
    fn close(&self) {
        let network = Arc::clone(&self.network);
        if network.try_lock().ok().as_mut().map(|g| g.take()).is_none() {
            let rt = pyo3_async_runtimes::tokio::get_runtime();
            rt.spawn(async move {
                let mut guard = network.lock().await;
                guard.take();
            });
        }
    }

    #[getter]
    fn client_ip(&self) -> &str {
        &self.client_ip
    }

    #[getter]
    fn subnet(&self) -> &str {
        &self.subnet
    }
}

// ---------------------------------------------------------------------------
// TcpStream
// ---------------------------------------------------------------------------

#[pyclass(name = "TcpStream")]
pub struct PyTcpStream {
    reader: Arc<Mutex<Option<io::ReadHalf<connect::userspace::TcpStream>>>>,
    writer: Arc<Mutex<Option<io::WriteHalf<connect::userspace::TcpStream>>>>,
    peer_addr: String,
}

#[pymethods]
impl PyTcpStream {
    /// Read up to n bytes.
    #[pyo3(signature = (n=4096))]
    fn read<'py>(&self, py: Python<'py>, n: usize) -> PyResult<Bound<'py, PyAny>> {
        let reader = Arc::clone(&self.reader);
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let mut guard = reader.lock().await;
            let r = guard
                .as_mut()
                .ok_or_else(|| ApiError::new_err("stream is closed"))?;

            let mut buf = vec![0u8; n];
            let read = r
                .read(&mut buf)
                .await
                .map_err(|e| ApiError::new_err(format!("{e}")))?;
            buf.truncate(read);
            Ok(buf)
        })
    }

    /// Write data, returning bytes written.
    fn write<'py>(&self, py: Python<'py>, data: Vec<u8>) -> PyResult<Bound<'py, PyAny>> {
        let writer = Arc::clone(&self.writer);
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let mut guard = writer.lock().await;
            let w = guard
                .as_mut()
                .ok_or_else(|| ApiError::new_err("stream is closed"))?;

            let written = w
                .write(&data)
                .await
                .map_err(|e| ApiError::new_err(format!("{e}")))?;
            Ok(written)
        })
    }

    /// Write all data.
    fn write_all<'py>(&self, py: Python<'py>, data: Vec<u8>) -> PyResult<Bound<'py, PyAny>> {
        let writer = Arc::clone(&self.writer);
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let mut guard = writer.lock().await;
            let w = guard
                .as_mut()
                .ok_or_else(|| ApiError::new_err("stream is closed"))?;

            w.write_all(&data)
                .await
                .map_err(|e| ApiError::new_err(format!("{e}")))?;
            Ok(())
        })
    }

    /// Shut down the write half.
    fn shutdown<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let writer = Arc::clone(&self.writer);
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let mut guard = writer.lock().await;
            let w = guard
                .as_mut()
                .ok_or_else(|| ApiError::new_err("stream is closed"))?;

            w.shutdown()
                .await
                .map_err(|e| ApiError::new_err(format!("{e}")))?;
            Ok(())
        })
    }

    /// Close the stream.
    fn close(&self) {
        // Drop the reader half.
        let reader = Arc::clone(&self.reader);
        if reader.try_lock().ok().as_mut().map(|g| g.take()).is_none() {
            let rt = pyo3_async_runtimes::tokio::get_runtime();
            rt.spawn(async move {
                let mut guard = reader.lock().await;
                guard.take();
            });
        }
        // Drop the writer half.
        let writer = Arc::clone(&self.writer);
        if writer.try_lock().ok().as_mut().map(|g| g.take()).is_none() {
            let rt = pyo3_async_runtimes::tokio::get_runtime();
            rt.spawn(async move {
                let mut guard = writer.lock().await;
                guard.take();
            });
        }
    }

    #[getter]
    fn peer_addr(&self) -> &str {
        &self.peer_addr
    }
}

// ---------------------------------------------------------------------------
// UdpSocket
// ---------------------------------------------------------------------------

#[pyclass(name = "UdpSocket")]
pub struct PyUdpSocket {
    socket: Arc<Mutex<Option<connect::userspace::UdpSocket>>>,
    local_port: u16,
}

#[pymethods]
impl PyUdpSocket {
    /// Send a datagram to the given address.
    fn send_to<'py>(
        &self,
        py: Python<'py>,
        data: Vec<u8>,
        host: String,
        port: u16,
    ) -> PyResult<Bound<'py, PyAny>> {
        let socket = Arc::clone(&self.socket);
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let addr: SocketAddr = format!("{}:{}", host, port)
                .parse()
                .map_err(|e| ApiError::new_err(format!("invalid address: {e}")))?;

            let guard = socket.lock().await;
            let s = guard
                .as_ref()
                .ok_or_else(|| ApiError::new_err("socket is closed"))?;

            let sent = s
                .send_to(&data, addr)
                .await
                .map_err(|e| ApiError::new_err(format!("{e}")))?;
            Ok(sent)
        })
    }

    /// Receive a datagram. Returns (data, "ip:port").
    #[pyo3(signature = (bufsize=65536))]
    fn recv_from<'py>(&self, py: Python<'py>, bufsize: usize) -> PyResult<Bound<'py, PyAny>> {
        let socket = Arc::clone(&self.socket);
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let guard = socket.lock().await;
            let s = guard
                .as_ref()
                .ok_or_else(|| ApiError::new_err("socket is closed"))?;

            let mut buf = vec![0u8; bufsize];
            let (n, addr) = s
                .recv_from(&mut buf)
                .await
                .map_err(|e| ApiError::new_err(format!("{e}")))?;
            buf.truncate(n);
            Ok((buf, addr.to_string()))
        })
    }

    /// Close the socket.
    fn close(&self) {
        let socket = Arc::clone(&self.socket);
        if socket.try_lock().ok().as_mut().map(|g| g.take()).is_none() {
            let rt = pyo3_async_runtimes::tokio::get_runtime();
            rt.spawn(async move {
                let mut guard = socket.lock().await;
                guard.take();
            });
        }
    }

    #[getter]
    fn local_port(&self) -> u16 {
        self.local_port
    }
}
