use tokio::sync::mpsc;

use crate::io_session::{IoEvent, IoSession};

/// Identifies which output stream a log line came from.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Stream {
    Stdout,
    Stderr,
}

/// A single log line/chunk from a container.
#[derive(Debug)]
pub struct LogLine {
    pub service: String,
    pub stream: Stream,
    pub data: Vec<u8>,
}

/// Collects log output from multiple containers.
///
/// Each container's IoSession is run in a spawned task that forwards
/// events to the shared channel.
pub struct LogCollector {
    tx: mpsc::Sender<LogLine>,
    rx: mpsc::Receiver<LogLine>,
}

impl LogCollector {
    pub fn new(buffer_size: usize) -> Self {
        let (tx, rx) = mpsc::channel(buffer_size);
        LogCollector { tx, rx }
    }

    /// Get a sender handle for spawning collection tasks.
    pub fn sender(&self) -> mpsc::Sender<LogLine> {
        self.tx.clone()
    }

    /// Spawn a task that reads from an IoSession and forwards to the collector.
    pub fn collect(&self, service_name: String, mut session: IoSession) {
        let tx = self.tx.clone();
        tokio::spawn(async move {
            loop {
                match session.next_event().await {
                    Ok(IoEvent::Stdout(data)) => {
                        if tx
                            .send(LogLine {
                                service: service_name.clone(),
                                stream: Stream::Stdout,
                                data,
                            })
                            .await
                            .is_err()
                        {
                            break;
                        }
                    }
                    Ok(IoEvent::Stderr(data)) => {
                        if tx
                            .send(LogLine {
                                service: service_name.clone(),
                                stream: Stream::Stderr,
                                data,
                            })
                            .await
                            .is_err()
                        {
                            break;
                        }
                    }
                    Ok(IoEvent::Eof) => {
                        break;
                    }
                    Err(e) => {
                        log::warn!("log stream for {} ended: {:#}", service_name, e);
                        break;
                    }
                }
            }
        });
    }

    /// Receive the next log line. Returns None when all senders are dropped.
    pub async fn next(&mut self) -> Option<LogLine> {
        self.rx.recv().await
    }
}
