use std::task::Poll;

use futures::io::{AsyncRead, AsyncWrite};

use crate::spawner::{LocalSpawner, TaskHandle};

enum DriverRequest {
    OpenStream(async_channel::Sender<anyhow::Result<yamux::Stream>>),
    Close(async_channel::Sender<anyhow::Result<()>>),
}

pub struct YamuxHandle {
    inbound_rx: async_channel::Receiver<yamux::Stream>,
    request_tx: async_channel::Sender<DriverRequest>,
}

impl YamuxHandle {
    /// Spawn a yamux driver task for the given connection.
    ///
    /// Generic over the transport stream type — production uses
    /// `Async<std::fs::File>`, tests can use any `AsyncRead + AsyncWrite`.
    pub fn spawn<T>(
        conn: yamux::Connection<T>,
        spawner: &impl LocalSpawner,
    ) -> (Self, TaskHandle)
    where
        T: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    {
        let (inbound_tx, inbound_rx) = async_channel::bounded(16);
        let (request_tx, request_rx) = async_channel::bounded(4);

        let task = spawner.spawn_local(driver_loop(conn, inbound_tx, request_rx));

        (
            YamuxHandle {
                inbound_rx,
                request_tx,
            },
            task,
        )
    }

    pub async fn open_stream(&self) -> anyhow::Result<yamux::Stream> {
        let (tx, rx) = async_channel::bounded(1);
        self.request_tx
            .send(DriverRequest::OpenStream(tx))
            .await
            .map_err(|_| anyhow::anyhow!("yamux driver task gone"))?;
        rx.recv()
            .await
            .map_err(|_| anyhow::anyhow!("yamux driver task gone"))?
    }

    pub async fn close(&self) -> anyhow::Result<()> {
        let (tx, rx) = async_channel::bounded(1);
        self.request_tx
            .send(DriverRequest::Close(tx))
            .await
            .map_err(|_| anyhow::anyhow!("yamux driver task gone"))?;
        rx.recv()
            .await
            .map_err(|_| anyhow::anyhow!("yamux driver task gone"))?
    }

    pub async fn next_inbound(&self) -> Option<yamux::Stream> {
        self.inbound_rx.recv().await.ok()
    }
}

async fn driver_loop<T>(
    mut conn: yamux::Connection<T>,
    inbound_tx: async_channel::Sender<yamux::Stream>,
    request_rx: async_channel::Receiver<DriverRequest>,
)
where
    T: AsyncRead + AsyncWrite + Unpin + 'static,
{
    loop {
        let drive_and_inbound = async {
            match std::future::poll_fn(|cx| conn.poll_next_inbound(cx)).await {
                Some(Ok(stream)) => DriveResult::Inbound(stream),
                Some(Err(e)) => DriveResult::Error(e),
                None => DriveResult::Closed,
            }
        };
        let handle_request = async {
            match request_rx.recv().await {
                Ok(req) => DriveResult::Request(req),
                Err(_) => DriveResult::HandleDropped,
            }
        };

        match futures::future::select(
            std::pin::pin!(drive_and_inbound),
            std::pin::pin!(handle_request),
        )
        .await
        .factor_first()
        .0
        {
            DriveResult::Inbound(stream) => {
                if inbound_tx.send(stream).await.is_err() {
                    break;
                }
            }
            DriveResult::Request(DriverRequest::OpenStream(reply)) => {
                let result = open_outbound(&mut conn, &inbound_tx).await;
                let _ = reply.send(result).await;
            }
            DriveResult::Request(DriverRequest::Close(reply)) => {
                let result = std::future::poll_fn(|cx| conn.poll_close(cx))
                    .await
                    .map_err(|e| anyhow::anyhow!("yamux close: {}", e));
                let _ = reply.send(result).await;
                break;
            }
            DriveResult::Error(e) => {
                log::error!("yamux driver error: {}", e);
                break;
            }
            DriveResult::Closed | DriveResult::HandleDropped => {
                break;
            }
        }
    }
}

enum DriveResult {
    Inbound(yamux::Stream),
    Request(DriverRequest),
    Error(yamux::ConnectionError),
    Closed,
    HandleDropped,
}

/// Open an outbound stream while continuing to drive inbound processing.
///
/// Any inbound streams that arrive while waiting for the outbound stream
/// are queued into `inbound_tx` instead of being dropped.
async fn open_outbound<T>(
    conn: &mut yamux::Connection<T>,
    inbound_tx: &async_channel::Sender<yamux::Stream>,
) -> anyhow::Result<yamux::Stream>
where
    T: AsyncRead + AsyncWrite + Unpin + 'static,
{
    let mut stream_opt: Option<yamux::Stream> = None;
    std::future::poll_fn(|cx| {
        // Drive inbound to process yamux bookkeeping frames.
        loop {
            match conn.poll_next_inbound(cx) {
                Poll::Ready(Some(Ok(stream))) => {
                    if let Err(_) = inbound_tx.try_send(stream) {
                        log::warn!("inbound stream channel full during open_outbound, dropped");
                    }
                }
                Poll::Ready(Some(Err(e))) => {
                    return Poll::Ready(Err(anyhow::anyhow!("yamux error: {}", e)));
                }
                Poll::Ready(None) => {
                    return Poll::Ready(Err(anyhow::anyhow!("yamux closed during open_outbound")));
                }
                Poll::Pending => break,
            }
        }
        match conn.poll_new_outbound(cx) {
            Poll::Ready(Ok(s)) => {
                stream_opt = Some(s);
                Poll::Ready(Ok(()))
            }
            Poll::Ready(Err(e)) => Poll::Ready(Err(anyhow::anyhow!("yamux outbound: {}", e))),
            Poll::Pending => Poll::Pending,
        }
    })
    .await?;
    Ok(stream_opt.expect("poll_fn completed without setting stream"))
}
