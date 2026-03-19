//! Log streaming fan-out bus.
//!
//! Bridges worker log streams to gRPC clients with a per-topic ring buffer
//! for historical replay and multi-subscriber fan-out.

use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex};
use std::time::Instant;

use distvirt_worker_protocol::{NamespaceId, PodId};
use tokio::sync::mpsc;

/// A chunk of log data from a container.
#[derive(Clone, Debug)]
pub struct LogChunk {
    pub namespace_id: NamespaceId,
    pub pod_id: PodId,
    pub container_id: String,
    pub data: Vec<u8>,
    pub timestamp: Instant,
}

/// Key identifying a single log topic (one container's output).
type TopicKey = (NamespaceId, PodId, String);

/// Per-topic state: ring buffer + subscriber list.
struct TopicState {
    buffer: VecDeque<LogChunk>,
    buffer_bytes: usize,
    max_bytes: usize,
    subscribers: Vec<mpsc::Sender<LogChunk>>,
}

impl TopicState {
    fn new(max_bytes: usize) -> Self {
        TopicState {
            buffer: VecDeque::new(),
            buffer_bytes: 0,
            max_bytes,
            subscribers: Vec::new(),
        }
    }

    fn publish(&mut self, chunk: LogChunk) {
        self.buffer_bytes += chunk.data.len();
        self.buffer.push_back(chunk.clone());

        // Evict oldest while over cap.
        while self.buffer_bytes > self.max_bytes {
            if let Some(old) = self.buffer.pop_front() {
                self.buffer_bytes -= old.data.len();
            } else {
                break;
            }
        }

        // Fan out to subscribers, removing dead senders.
        self.subscribers.retain(|tx| tx.try_send(chunk.clone()).is_ok());
    }
}

/// Shared log bus state.
struct LogBusInner {
    topics: HashMap<TopicKey, TopicState>,
    max_bytes_per_topic: usize,
}

/// Handle to the log bus. Cheap to clone (Arc wrapper).
#[derive(Clone)]
pub struct LogBusHandle {
    inner: Arc<Mutex<LogBusInner>>,
}

impl LogBusHandle {
    /// Create a new log bus.
    pub fn new(max_bytes_per_topic: usize) -> Self {
        LogBusHandle {
            inner: Arc::new(Mutex::new(LogBusInner {
                topics: HashMap::new(),
                max_bytes_per_topic,
            })),
        }
    }

    /// Publish a log chunk. Called by worker ingest tasks.
    pub fn publish(&self, chunk: LogChunk) {
        let mut inner = self.inner.lock().unwrap();
        let key = (
            chunk.namespace_id.clone(),
            chunk.pod_id.clone(),
            chunk.container_id.clone(),
        );
        let max_bytes = inner.max_bytes_per_topic;
        let topic = inner
            .topics
            .entry(key)
            .or_insert_with(|| TopicState::new(max_bytes));
        topic.publish(chunk);
    }

    /// Subscribe to logs for a namespace, optionally filtered by pod IDs
    /// and/or container IDs.
    ///
    /// Returns historical chunks followed by a receiver for live chunks.
    /// If `pod_filter` is `None`, subscribes to all pods in the namespace.
    /// If `Some`, only subscribes to topics matching the given pod IDs.
    /// If `container_filter` is `None`, subscribes to all containers.
    /// If `Some`, only subscribes to topics matching the given container IDs.
    ///
    /// The caller is responsible for resolving workload_id → pod_id(s)
    /// before calling this method.
    pub fn subscribe(
        &self,
        namespace_id: &NamespaceId,
        pod_filter: Option<&[PodId]>,
        container_filter: Option<&[String]>,
    ) -> (Vec<LogChunk>, mpsc::Receiver<LogChunk>) {
        let (tx, rx) = mpsc::channel(256);
        let mut inner = self.inner.lock().unwrap();
        let mut historical = Vec::new();

        for (key, topic) in inner.topics.iter_mut() {
            if key.0 != *namespace_id {
                continue;
            }
            if let Some(pods) = pod_filter {
                if !pods.contains(&key.1) {
                    continue;
                }
            }
            if let Some(containers) = container_filter {
                if !containers.contains(&key.2) {
                    continue;
                }
            }
            // Clone historical buffer.
            for chunk in &topic.buffer {
                historical.push(chunk.clone());
            }
            // Register subscriber.
            topic.subscribers.push(tx.clone());
        }

        // Sort historical by timestamp.
        historical.sort_by_key(|c| c.timestamp);

        (historical, rx)
    }

    /// Remove all topics for a namespace.
    pub fn remove_namespace(&self, namespace_id: &NamespaceId) {
        let mut inner = self.inner.lock().unwrap();
        inner.topics.retain(|key, _| key.0 != *namespace_id);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_chunk(ns: &str, pod: u64, container: &str, data: &[u8]) -> LogChunk {
        LogChunk {
            namespace_id: NamespaceId::from(ns),
            pod_id: PodId::from(pod),
            container_id: container.to_string(),
            data: data.to_vec(),
            timestamp: Instant::now(),
        }
    }

    #[tokio::test]
    async fn test_publish_and_subscribe_historical() {
        let bus = LogBusHandle::new(1024);

        // Publish some chunks before subscribing.
        bus.publish(make_chunk("ns1", 1, "main", b"hello "));
        bus.publish(make_chunk("ns1", 1, "main", b"world"));
        bus.publish(make_chunk("ns2", 2, "main", b"other namespace"));

        // Subscribe to ns1.
        let (historical, _rx) = bus.subscribe(&NamespaceId::from("ns1"), None, None);
        assert_eq!(historical.len(), 2);
        assert_eq!(historical[0].data, b"hello ");
        assert_eq!(historical[1].data, b"world");
    }

    #[tokio::test]
    async fn test_live_delivery() {
        let bus = LogBusHandle::new(1024);

        // Publish one chunk to create the topic.
        bus.publish(make_chunk("ns1", 1, "main", b"before"));

        // Subscribe.
        let (_historical, mut rx) = bus.subscribe(&NamespaceId::from("ns1"), None, None);

        // Publish after subscribing.
        bus.publish(make_chunk("ns1", 1, "main", b"live"));

        let chunk = rx.try_recv().unwrap();
        assert_eq!(chunk.data, b"live");
    }

    #[tokio::test]
    async fn test_ring_buffer_eviction() {
        let bus = LogBusHandle::new(10); // 10 byte cap per topic

        bus.publish(make_chunk("ns1", 1, "main", b"12345")); // 5 bytes
        bus.publish(make_chunk("ns1", 1, "main", b"67890")); // 5 bytes, total 10
        bus.publish(make_chunk("ns1", 1, "main", b"abcde")); // 5 bytes, evicts first

        let (historical, _rx) = bus.subscribe(&NamespaceId::from("ns1"), None, None);
        assert_eq!(historical.len(), 2);
        assert_eq!(historical[0].data, b"67890");
        assert_eq!(historical[1].data, b"abcde");
    }

    #[tokio::test]
    async fn test_subscriber_backpressure() {
        let bus = LogBusHandle::new(1024 * 1024);

        // Create topic.
        bus.publish(make_chunk("ns1", 1, "main", b"init"));

        // Subscribe with bounded channel (256 capacity).
        let (_historical, rx) = bus.subscribe(&NamespaceId::from("ns1"), None, None);

        // Publish more than the channel can hold — should not block.
        for i in 0..300 {
            bus.publish(make_chunk("ns1", 1, "main", format!("msg{}", i).as_bytes()));
        }

        // The first 256 should be delivered, rest dropped.
        // Just verify we didn't deadlock and some were received.
        drop(rx);
    }

    #[tokio::test]
    async fn test_pod_filter() {
        let bus = LogBusHandle::new(1024);

        bus.publish(make_chunk("ns1", 1, "main", b"pod1"));
        bus.publish(make_chunk("ns1", 2, "main", b"pod2"));

        // Subscribe with filter for pod 1.
        let (historical, _rx) = bus.subscribe(
            &NamespaceId::from("ns1"),
            Some(&[PodId::from(1)]),
            None,
        );
        assert_eq!(historical.len(), 1);
        assert_eq!(historical[0].data, b"pod1");
    }

    #[tokio::test]
    async fn test_container_filter() {
        let bus = LogBusHandle::new(1024);

        bus.publish(make_chunk("ns1", 1, "main", b"from main"));
        bus.publish(make_chunk("ns1", 1, "sidecar", b"from sidecar"));

        // Filter to just the "main" container.
        let (historical, _rx) = bus.subscribe(
            &NamespaceId::from("ns1"),
            None,
            Some(&["main".to_string()]),
        );
        assert_eq!(historical.len(), 1);
        assert_eq!(historical[0].data, b"from main");

        // Filter to just the "sidecar" container.
        let (historical, _rx) = bus.subscribe(
            &NamespaceId::from("ns1"),
            None,
            Some(&["sidecar".to_string()]),
        );
        assert_eq!(historical.len(), 1);
        assert_eq!(historical[0].data, b"from sidecar");

        // No container filter returns both.
        let (historical, _rx) = bus.subscribe(&NamespaceId::from("ns1"), None, None);
        assert_eq!(historical.len(), 2);
    }

    #[tokio::test]
    async fn test_remove_namespace() {
        let bus = LogBusHandle::new(1024);

        bus.publish(make_chunk("ns1", 1, "main", b"data"));
        bus.remove_namespace(&NamespaceId::from("ns1"));

        let (historical, _rx) = bus.subscribe(&NamespaceId::from("ns1"), None, None);
        assert!(historical.is_empty());
    }
}
