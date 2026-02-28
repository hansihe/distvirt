//! Yamux-based connections for the orchestrator and worker sides of the protocol.
//!
//! Both sides share the same yamux session over an arbitrary async byte stream
//! (`tokio::io::duplex` for local mode, TCP/TLS for distributed mode). The
//! orchestrator is the yamux Client, the worker is the yamux Server.
//!
//! # Example: In-Process Connection (Local Mode)
//!
//! ```rust,no_run
//! use distvirt_worker_protocol::{OrchestratorConnection, WorkerConnection};
//!
//! # async fn example() -> anyhow::Result<()> {
//! let (orch_transport, worker_transport) = tokio::io::duplex(64 * 1024);
//!
//! let (mut orch, mut worker) = tokio::try_join!(
//!     OrchestratorConnection::connect(orch_transport),
//!     WorkerConnection::accept(worker_transport),
//! )?;
//!
//! // Orchestrator sends commands, worker receives them:
//! // orch.send_command(&cmd).await?;
//! // let cmd = worker.recv_command().await?;
//!
//! // Worker sends events, orchestrator receives them:
//! // worker.send_event(&event).await?;
//! // let event = orch.recv_event().await?;
//! # Ok(())
//! # }
//! ```

use std::future::poll_fn;

use anyhow::Context;
use tokio::sync::mpsc;
use tokio_util::compat::TokioAsyncReadCompatExt;

use crate::codec::{recv_msg, send_msg};
use crate::types::{LogStreamHeader, WorkerCommand, WorkerEvent};

type YamuxStream = yamux::Stream;

/// Orchestrator-side connection to a worker over yamux.
///
/// The orchestrator is the yamux Client — it opens the control stream.
/// Log streams are initiated by the worker and accepted here.
pub struct OrchestratorConnection {
    control: YamuxStream,
    incoming_rx: mpsc::UnboundedReceiver<YamuxStream>,
}

impl OrchestratorConnection {
    /// Establish a connection over the given transport.
    ///
    /// Creates a yamux Client connection, opens the control stream,
    /// and spawns a background task to drive yamux and collect incoming streams.
    pub async fn connect(
        transport: impl tokio::io::AsyncRead + tokio::io::AsyncWrite + Send + Unpin + 'static,
    ) -> anyhow::Result<Self> {
        let compat = transport.compat();
        let mut conn =
            yamux::Connection::new(compat, yamux::Config::default(), yamux::Mode::Client);

        // Open control stream while driving the connection.
        let mut control_opt: Option<YamuxStream> = None;
        let mut early_inbound: Vec<YamuxStream> = Vec::new();

        poll_fn(|cx| {
            // Drive inbound.
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
            // Open outbound.
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

        let (incoming_tx, incoming_rx) = mpsc::unbounded_channel();
        for stream in early_inbound {
            let _ = incoming_tx.send(stream);
        }

        // Spawn driver task.
        tokio::spawn(async move {
            loop {
                match poll_fn(|cx| conn.poll_next_inbound(cx)).await {
                    Some(Ok(stream)) => {
                        if incoming_tx.send(stream).is_err() {
                            break;
                        }
                    }
                    Some(Err(e)) => {
                        log::error!("orchestrator yamux error: {}", e);
                        break;
                    }
                    None => {
                        log::info!("orchestrator yamux connection closed");
                        break;
                    }
                }
            }
        });

        Ok(OrchestratorConnection {
            control,
            incoming_rx,
        })
    }

    /// Send a command to the worker.
    pub async fn send_command(&mut self, cmd: &WorkerCommand) -> anyhow::Result<()> {
        send_msg(&mut self.control, cmd)
            .await
            .context("send command")
    }

    /// Receive an event from the worker.
    pub async fn recv_event(&mut self) -> anyhow::Result<WorkerEvent> {
        recv_msg(&mut self.control)
            .await
            .context("recv event")
    }

    /// Accept a worker-initiated log stream.
    ///
    /// Returns the log stream header and the raw yamux stream for reading
    /// container output data.
    pub async fn accept_log_stream(&mut self) -> anyhow::Result<(LogStreamHeader, YamuxStream)> {
        let mut stream = self
            .incoming_rx
            .recv()
            .await
            .context("yamux connection closed, no more incoming streams")?;

        let header: LogStreamHeader = recv_msg(&mut stream)
            .await
            .context("read log stream header")?;

        log::info!(
            "accepted log stream for {}/{}/{}",
            header.namespace_id,
            header.pod_id,
            header.container_id
        );

        Ok((header, stream))
    }

    /// Take the incoming stream receiver for use in a separate task.
    ///
    /// After calling this, `accept_log_stream` will no longer work.
    /// Use this to spawn a background log stream acceptor task.
    pub fn take_log_stream_receiver(&mut self) -> mpsc::UnboundedReceiver<YamuxStream> {
        let (_, empty_rx) = mpsc::unbounded_channel();
        std::mem::replace(&mut self.incoming_rx, empty_rx)
    }
}

/// Clonable handle for opening log streams from background tasks.
#[derive(Clone)]
pub struct LogStreamOpener {
    conn_tx: mpsc::UnboundedSender<NewStreamRequest>,
}

impl LogStreamOpener {
    /// Open a new yamux stream for container log data.
    pub async fn open_log_stream(
        &self,
        header: &LogStreamHeader,
    ) -> anyhow::Result<YamuxStream> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        self.conn_tx
            .send(NewStreamRequest { reply: tx })
            .map_err(|_| anyhow::anyhow!("yamux driver task gone"))?;

        let mut stream = rx
            .await
            .map_err(|_| anyhow::anyhow!("yamux driver task gone"))?
            .context("open outbound yamux stream")?;

        send_msg(&mut stream, header)
            .await
            .context("send log stream header")?;

        Ok(stream)
    }
}

/// Worker-side connection to the orchestrator over yamux.
///
/// The worker is the yamux Server — it accepts the control stream.
/// Log streams are opened by the worker toward the orchestrator.
pub struct WorkerConnection {
    control: YamuxStream,
    conn_tx: mpsc::UnboundedSender<NewStreamRequest>,
}

struct NewStreamRequest {
    reply: tokio::sync::oneshot::Sender<anyhow::Result<YamuxStream>>,
}

impl WorkerConnection {
    /// Accept a connection from the given transport.
    ///
    /// Creates a yamux Server connection, accepts the control stream,
    /// and spawns a background driver task.
    pub async fn accept(
        transport: impl tokio::io::AsyncRead + tokio::io::AsyncWrite + Send + Unpin + 'static,
    ) -> anyhow::Result<Self> {
        let compat = transport.compat();
        let mut conn =
            yamux::Connection::new(compat, yamux::Config::default(), yamux::Mode::Server);

        // Accept the control stream (first inbound stream from the client).
        let control = poll_fn(|cx| conn.poll_next_inbound(cx))
            .await
            .context("yamux connection closed before control stream")?
            .context("yamux error accepting control stream")?;

        let (conn_tx, mut conn_rx) = mpsc::unbounded_channel::<NewStreamRequest>();

        // Spawn driver task that handles both inbound polling and outbound stream requests.
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    inbound = poll_fn(|cx| conn.poll_next_inbound(cx)) => {
                        match inbound {
                            Some(Ok(_stream)) => {
                                // Worker doesn't expect inbound streams from orchestrator.
                                log::warn!("worker: unexpected inbound yamux stream, ignoring");
                            }
                            Some(Err(e)) => {
                                log::error!("worker yamux error: {}", e);
                                break;
                            }
                            None => {
                                log::info!("worker yamux connection closed");
                                break;
                            }
                        }
                    }
                    req = conn_rx.recv() => {
                        match req {
                            Some(req) => {
                                let result = poll_fn(|cx| conn.poll_new_outbound(cx)).await;
                                let _ = req.reply.send(result.map_err(Into::into));
                            }
                            None => break,
                        }
                    }
                }
            }
        });

        Ok(WorkerConnection { control, conn_tx })
    }

    /// Receive a command from the orchestrator.
    pub async fn recv_command(&mut self) -> anyhow::Result<WorkerCommand> {
        recv_msg(&mut self.control)
            .await
            .context("recv command")
    }

    /// Send an event to the orchestrator.
    pub async fn send_event(&mut self, event: &WorkerEvent) -> anyhow::Result<()> {
        send_msg(&mut self.control, event)
            .await
            .context("send event")
    }

    /// Get a clonable handle for opening log streams from background tasks.
    pub fn log_stream_opener(&self) -> LogStreamOpener {
        LogStreamOpener {
            conn_tx: self.conn_tx.clone(),
        }
    }

    /// Open a new yamux stream for container log data.
    ///
    /// Sends the LogStreamHeader as the first message, then returns
    /// the stream for writing raw output data.
    pub async fn open_log_stream(
        &self,
        header: &LogStreamHeader,
    ) -> anyhow::Result<YamuxStream> {
        self.log_stream_opener().open_log_stream(header).await
    }
}
