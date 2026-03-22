//! Guest-init protocol simulator for testing.
//!
//! Speaks the guest-side yamux protocol over a `UnixStream`, implementing
//! enough of the guest-init behavior to drive the host-side `ManagedVm`
//! through its full lifecycle.

use std::future::poll_fn;

use anyhow::Context;
use futures_lite::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::UnixStream;
use tokio::sync::{mpsc, oneshot};
use tokio_util::compat::TokioAsyncReadCompatExt;

use distvirt_guest_protocol::{GuestEvent, GuestMessage, HostMessage, StreamHeader};

/// How the simulated guest responds to PrepareSuspend.
#[derive(Clone, Default)]
pub enum SuspendBehavior {
    /// Send SuspendReady immediately (current behavior).
    #[default]
    Immediate,
    /// Never respond to PrepareSuspend (triggers SUSPEND_TIMEOUT).
    Hang,
}

/// Configuration for the guest simulator.
pub struct GuestSimConfig {
    pub container_behavior: ContainerBehavior,
    pub suspend_behavior: SuspendBehavior,
    pub fail_before_ready: bool,
}

/// How the simulated container behaves after being started.
#[derive(Clone)]
pub enum ContainerBehavior {
    /// Container exits immediately with the given exit code.
    ExitImmediately(i32),
    /// Container runs until it receives a signal, then exits with code 0.
    RunUntilSignaled,
}

/// Request to open an outbound yamux stream from the driver task.
struct OpenStreamRequest {
    reply: oneshot::Sender<anyhow::Result<yamux::Stream>>,
}

/// Handle to the yamux driver task, mirroring guest-init's YamuxHandle pattern.
struct YamuxHandle {
    inbound_rx: mpsc::UnboundedReceiver<yamux::Stream>,
    open_tx: mpsc::Sender<OpenStreamRequest>,
}

impl YamuxHandle {
    /// Spawn the driver task and return a handle for stream operations.
    fn spawn(
        conn: yamux::Connection<tokio_util::compat::Compat<UnixStream>>,
    ) -> (Self, tokio::task::JoinHandle<()>) {
        let (inbound_tx, inbound_rx) = mpsc::unbounded_channel();
        let (open_tx, mut open_rx) = mpsc::channel::<OpenStreamRequest>(4);

        let task = tokio::spawn(async move {
            driver_loop(conn, inbound_tx, &mut open_rx).await;
        });

        (
            YamuxHandle {
                inbound_rx,
                open_tx,
            },
            task,
        )
    }

    /// Accept the next inbound stream from the peer.
    async fn next_inbound(&mut self) -> Option<yamux::Stream> {
        self.inbound_rx.recv().await
    }

    /// Open a new outbound stream (drives connection concurrently).
    async fn open_stream(&self) -> anyhow::Result<yamux::Stream> {
        let (tx, rx) = oneshot::channel();
        self.open_tx
            .send(OpenStreamRequest { reply: tx })
            .await
            .map_err(|_| anyhow::anyhow!("yamux driver gone"))?;
        rx.await.map_err(|_| anyhow::anyhow!("yamux driver gone"))?
    }
}

/// Yamux driver loop: polls inbound frames and handles open-stream requests.
async fn driver_loop(
    mut conn: yamux::Connection<tokio_util::compat::Compat<UnixStream>>,
    inbound_tx: mpsc::UnboundedSender<yamux::Stream>,
    open_rx: &mut mpsc::Receiver<OpenStreamRequest>,
) {
    loop {
        tokio::select! {
            result = poll_fn(|cx| conn.poll_next_inbound(cx)) => {
                match result {
                    Some(Ok(stream)) => {
                        if inbound_tx.send(stream).is_err() {
                            break;
                        }
                    }
                    Some(Err(e)) => {
                        log::debug!("guest_sim: yamux error: {}", e);
                        break;
                    }
                    None => break,
                }
            }
            Some(req) = open_rx.recv() => {
                // Open outbound while driving inbound.
                let result = open_outbound(&mut conn, &inbound_tx).await;
                let _ = req.reply.send(result);
            }
        }
    }
}

/// Open an outbound stream while continuing to drive inbound processing.
async fn open_outbound(
    conn: &mut yamux::Connection<tokio_util::compat::Compat<UnixStream>>,
    inbound_tx: &mpsc::UnboundedSender<yamux::Stream>,
) -> anyhow::Result<yamux::Stream> {
    poll_fn(|cx| {
        // Drive inbound to process yamux bookkeeping frames.
        loop {
            match conn.poll_next_inbound(cx) {
                std::task::Poll::Ready(Some(Ok(stream))) => {
                    let _ = inbound_tx.send(stream);
                }
                std::task::Poll::Ready(Some(Err(e))) => {
                    return std::task::Poll::Ready(Err(anyhow::anyhow!("yamux error: {}", e)));
                }
                std::task::Poll::Ready(None) => {
                    return std::task::Poll::Ready(Err(anyhow::anyhow!("yamux closed")));
                }
                std::task::Poll::Pending => break,
            }
        }
        match conn.poll_new_outbound(cx) {
            std::task::Poll::Ready(Ok(s)) => std::task::Poll::Ready(Ok(s)),
            std::task::Poll::Ready(Err(e)) => {
                std::task::Poll::Ready(Err(anyhow::anyhow!("yamux outbound: {}", e)))
            }
            std::task::Poll::Pending => std::task::Poll::Pending,
        }
    })
    .await
}

/// Run the guest-side simulator over the given socket.
///
/// This function acts as the yamux server (Mode::Server), matching what the
/// real guest-init does. It handles the full protocol flow that
/// `ManagedVm::connect` expects.
pub async fn run_guest_sim(socket: UnixStream, config: GuestSimConfig) -> anyhow::Result<()> {
    let compat_socket = socket.compat();

    let conn = yamux::Connection::new(compat_socket, yamux::Config::default(), yamux::Mode::Server);

    // Spawn driver immediately so all stream I/O works.
    let (mut handle, driver_task) = YamuxHandle::spawn(conn);

    // Phase 1: Accept control stream.
    let mut control = handle
        .next_inbound()
        .await
        .context("yamux closed before control stream")?;

    let header = read_framed::<StreamHeader>(&mut control)
        .await
        .context("read control stream header")?;
    match header {
        StreamHeader::Control => {}
        other => anyhow::bail!("expected StreamHeader::Control, got {:?}", other),
    }

    // Phase 2: Open event stream (outbound).
    let mut event_stream = handle.open_stream().await.context("open event stream")?;

    write_framed(&mut event_stream, &StreamHeader::Events)
        .await
        .context("write event stream header")?;

    // Phase 3: Optionally fail before sending Ready.
    if config.fail_before_ready {
        // Drop everything to simulate a VM crash before becoming ready.
        drop(control);
        drop(event_stream);
        driver_task.abort();
        anyhow::bail!("guest_sim: simulated failure before Ready");
    }

    // Send Ready.
    write_framed(
        &mut control,
        &GuestMessage::Ready {
            running_containers: vec![],
            pre_config_responses: vec![],
        },
    )
    .await
    .context("send Ready")?;

    // Track state for container exit signaling.
    let mut waiting_for_signal: Option<String> = None;

    // Control loop: read host messages, respond.
    loop {
        let msg: HostMessage = read_framed(&mut control)
            .await
            .context("read host message")?;

        match msg {
            HostMessage::SetClock { .. } => {
                write_framed(&mut control, &GuestMessage::ClockSet).await?;
            }
            HostMessage::ConfigureNetwork { .. } => {
                write_framed(&mut control, &GuestMessage::NetworkConfigured).await?;
            }
            HostMessage::MountVolume { name, .. } => {
                write_framed(&mut control, &GuestMessage::VolumeMounted { name }).await?;
            }
            HostMessage::AddContainer { id, .. } => {
                write_framed(&mut control, &GuestMessage::ContainerAdded { id }).await?;
            }
            HostMessage::StartContainer {
                id, capture_output, ..
            } => {
                // If capture_output, open an output stream and close it.
                if capture_output {
                    match handle.open_stream().await {
                        Ok(mut output_stream) => {
                            let _ = write_framed(
                                &mut output_stream,
                                &StreamHeader::ContainerOutput {
                                    container_id: id.clone(),
                                },
                            )
                            .await;
                            drop(output_stream);
                        }
                        Err(e) => {
                            log::warn!("guest_sim: failed to open output stream: {:#}", e);
                        }
                    }
                }

                write_framed(
                    &mut control,
                    &GuestMessage::ContainerStarted {
                        id: id.clone(),
                        pid: 1,
                    },
                )
                .await?;

                // Schedule container behavior.
                match &config.container_behavior {
                    ContainerBehavior::ExitImmediately(code) => {
                        write_framed(
                            &mut event_stream,
                            &GuestEvent::ContainerExited { id, code: *code },
                        )
                        .await
                        .context("send ContainerExited event")?;
                    }
                    ContainerBehavior::RunUntilSignaled => {
                        waiting_for_signal = Some(id);
                    }
                }
            }
            HostMessage::SignalContainer { id, .. } => {
                write_framed(
                    &mut control,
                    &GuestMessage::ContainerSignaled { id: id.clone() },
                )
                .await?;

                // If RunUntilSignaled and this is the container, trigger exit.
                if waiting_for_signal.as_deref() == Some(&id) {
                    waiting_for_signal = None;
                    write_framed(
                        &mut event_stream,
                        &GuestEvent::ContainerExited { id, code: 0 },
                    )
                    .await
                    .context("send ContainerExited event after signal")?;
                }
            }
            HostMessage::PrepareSuspend => {
                match config.suspend_behavior {
                    SuspendBehavior::Immediate => {
                        write_framed(&mut control, &GuestMessage::SuspendReady).await?;
                    }
                    SuspendBehavior::Hang => {
                        // Never respond — let the host-side timeout fire.
                        futures_lite::future::pending::<()>().await;
                    }
                }
            }
            HostMessage::Shutdown => {
                drop(control);
                drop(event_stream);
                driver_task.abort();
                return Ok(());
            }
        }
    }
}

/// Read a length-prefixed JSON message from a yamux stream.
async fn read_framed<T: serde::de::DeserializeOwned>(
    stream: &mut yamux::Stream,
) -> anyhow::Result<T> {
    let mut len_buf = [0u8; 4];
    stream
        .read_exact(&mut len_buf)
        .await
        .context("read length")?;
    let len = u32::from_le_bytes(len_buf) as usize;
    if len > 1024 * 1024 {
        anyhow::bail!("message too large: {} bytes", len);
    }
    let mut buf = vec![0u8; len];
    stream.read_exact(&mut buf).await.context("read payload")?;
    serde_json::from_slice(&buf).context("deserialize message")
}

/// Write a length-prefixed JSON message to a yamux stream.
async fn write_framed<T: serde::Serialize>(
    stream: &mut yamux::Stream,
    msg: &T,
) -> anyhow::Result<()> {
    let json = serde_json::to_vec(msg).context("serialize message")?;
    let len = (json.len() as u32).to_le_bytes();
    stream.write_all(&len).await.context("write length")?;
    stream.write_all(&json).await.context("write payload")?;
    stream.flush().await.context("flush")?;
    Ok(())
}
