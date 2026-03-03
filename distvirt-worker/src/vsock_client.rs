use std::future::poll_fn;

use anyhow::{bail, Context};
use futures_lite::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::mpsc;
use tokio_util::compat::TokioAsyncReadCompatExt;

use crate::task_handle::TaskHandle;

use distvirt_guest_protocol::StreamHeader;

type YamuxStream = yamux::Stream;

/// Host-side yamux session to the guest over vsock.
///
/// The host acts as yamux client (opens the control stream).
/// The guest opens output streams that arrive via `accept_output_stream()`.
pub struct GuestSession {
    control: YamuxStream,
    incoming_rx: mpsc::UnboundedReceiver<YamuxStream>,
}

impl GuestSession {
    /// Create a new GuestSession from a tokio UnixStream.
    ///
    /// Opens the control stream, sends `StreamHeader::Control`, and spawns
    /// a background task that drives the yamux connection and collects
    /// incoming streams from the guest.
    pub async fn new(socket: tokio::net::UnixStream) -> anyhow::Result<(Self, TaskHandle<anyhow::Result<()>>)> {
        // Convert tokio socket to futures-io compatible.
        let compat_socket = socket.compat();

        let mut conn = yamux::Connection::new(
            compat_socket,
            yamux::Config::default(),
            yamux::Mode::Client,
        );

        // Open the control stream while driving the connection.
        // We must poll both poll_new_outbound (to create the stream) and
        // poll_next_inbound (to process yamux frames) simultaneously.
        let mut control_opt: Option<YamuxStream> = None;
        let mut early_inbound: Vec<YamuxStream> = Vec::new();

        poll_fn(|cx| {
            // Drive: process incoming frames (also handles yamux internal bookkeeping).
            loop {
                match conn.poll_next_inbound(cx) {
                    std::task::Poll::Ready(Some(Ok(stream))) => early_inbound.push(stream),
                    std::task::Poll::Ready(Some(Err(e))) => {
                        return std::task::Poll::Ready(Err(anyhow::Error::from(e)))
                    }
                    std::task::Poll::Ready(None) => {
                        return std::task::Poll::Ready(Err(anyhow::anyhow!(
                            "yamux connection closed"
                        )))
                    }
                    std::task::Poll::Pending => break,
                }
            }
            // Try to open outbound stream.
            match conn.poll_new_outbound(cx) {
                std::task::Poll::Ready(Ok(stream)) => {
                    control_opt = Some(stream);
                    std::task::Poll::Ready(Ok(()))
                }
                std::task::Poll::Ready(Err(e)) => {
                    std::task::Poll::Ready(Err(anyhow::Error::from(e)))
                }
                std::task::Poll::Pending => std::task::Poll::Pending,
            }
        })
        .await?;

        let control = control_opt.unwrap();

        // Spawn the driver task. This moves the Connection into the task,
        // which continuously drives yamux frame processing.
        let (incoming_tx, incoming_rx) = mpsc::unbounded_channel();
        for stream in early_inbound {
            let _ = incoming_tx.send(stream);
        }

        let yamux_driver = TaskHandle::spawn(async move {
            loop {
                match poll_fn(|cx| conn.poll_next_inbound(cx)).await {
                    Some(Ok(stream)) => {
                        if incoming_tx.send(stream).is_err() {
                            return Ok(()); // receiver dropped
                        }
                    }
                    Some(Err(e)) => {
                        log::error!("yamux connection error: {}", e);
                        return Err(anyhow::anyhow!("yamux connection error: {}", e));
                    }
                    None => {
                        log::info!("yamux connection closed by guest");
                        return Ok(());
                    }
                }
            }
        });

        // Write the stream header. The driver task is now running and
        // will flush outgoing data when the connection is polled.
        let mut control = control;
        let json = serde_json::to_vec(&StreamHeader::Control).context("serialize StreamHeader")?;
        let len = (json.len() as u32).to_le_bytes();
        control
            .write_all(&len)
            .await
            .context("write header length")?;
        control
            .write_all(&json)
            .await
            .context("write header payload")?;
        control.flush().await.context("flush header")?;

        Ok((GuestSession {
            control,
            incoming_rx,
        }, yamux_driver))
    }

    /// Send a length-prefixed JSON message on the control stream.
    pub async fn send<T: serde::Serialize>(&mut self, msg: &T) -> anyhow::Result<()> {
        let json = serde_json::to_vec(msg).context("serialize message")?;
        let len = (json.len() as u32).to_le_bytes();
        self.control.write_all(&len).await.context("write length")?;
        self.control
            .write_all(&json)
            .await
            .context("write payload")?;
        self.control.flush().await.context("flush")?;
        Ok(())
    }

    /// Receive a length-prefixed JSON message from the control stream.
    pub async fn recv<T: serde::de::DeserializeOwned>(&mut self) -> anyhow::Result<T> {
        let mut len_buf = [0u8; 4];
        self.control
            .read_exact(&mut len_buf)
            .await
            .context("read length")?;
        let len = u32::from_le_bytes(len_buf) as usize;
        if len > 1024 * 1024 {
            bail!("message too large: {} bytes", len);
        }
        let mut buf = vec![0u8; len];
        self.control
            .read_exact(&mut buf)
            .await
            .context("read payload")?;
        serde_json::from_slice(&buf).context("deserialize message")
    }

    /// Open a new yamux stream for forwarding stdin to a container.
    ///
    /// Opens an outbound yamux stream, sends `StreamHeader::ContainerInput`,
    /// and returns the raw stream for the caller to write stdin data to.
    ///
    /// Stub: not yet integrated into the pod lifecycle. Requires changes to
    /// the yamux driver to support opening outbound streams from the host side.
    pub async fn open_input_stream(&mut self, _container_id: &str) -> anyhow::Result<YamuxStream> {
        bail!("open_input_stream not yet integrated into yamux driver")
    }

    /// Accept the next output stream opened by the guest.
    ///
    /// Reads the `StreamHeader::ContainerOutput` header and returns
    /// the container ID and the raw yamux stream (for wrapping in `IoSession`).
    pub async fn accept_output_stream(&mut self) -> anyhow::Result<(String, YamuxStream)> {
        let mut stream = self
            .incoming_rx
            .recv()
            .await
            .context("yamux connection closed, no more incoming streams")?;

        // Read the stream header.
        let mut len_buf = [0u8; 4];
        stream
            .read_exact(&mut len_buf)
            .await
            .context("read stream header length")?;
        let len = u32::from_le_bytes(len_buf) as usize;
        if len > 1024 * 1024 {
            bail!("stream header too large: {} bytes", len);
        }
        let mut buf = vec![0u8; len];
        stream
            .read_exact(&mut buf)
            .await
            .context("read stream header payload")?;
        let header: StreamHeader =
            serde_json::from_slice(&buf).context("deserialize StreamHeader")?;

        match header {
            StreamHeader::ContainerOutput { container_id } => {
                log::info!("accepted output stream for container {}", container_id);
                Ok((container_id, stream))
            }
            other => {
                bail!("expected StreamHeader::ContainerOutput, got {:?}", other);
            }
        }
    }
}
