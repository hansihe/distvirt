//! Log streaming fan-out bus.
//!
//! Bridges worker log streams to gRPC clients with a per-topic ring buffer
//! for historical replay and multi-subscriber fan-out.

use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use distvirt_worker_protocol::{NamespaceId, PodId};
use tokio::sync::mpsc;

/// Default TTL for retired topics (5 minutes).
const DEFAULT_RETIRED_TTL: Duration = Duration::from_secs(5 * 60);

/// Sweep retired topics when topic count exceeds this threshold.
const SWEEP_THRESHOLD: usize = 64;

/// Channel capacity for log subscriber channels.
/// Sized to absorb bursts (e.g. final output from a dying pod)
/// without dropping chunks.
const SUBSCRIBER_CHANNEL_CAPACITY: usize = 4096;

/// A chunk of log data from a container.
#[derive(Clone, Debug)]
pub struct LogChunk {
    pub namespace_id: NamespaceId,
    pub pod_id: PodId,
    pub container_id: String,
    pub workload_name: Option<String>,
    pub data: Vec<u8>,
    pub timestamp: Instant,
    /// Monotonic sequence number assigned at the source (guest-init fill task).
    /// Gaps in the sequence indicate dropped chunks.
    pub seq: u64,
}

/// Key identifying a single log topic (one container's output).
type TopicKey = (NamespaceId, PodId, String);

/// Per-topic state: ring buffer + subscriber list.
struct TopicState {
    buffer: VecDeque<LogChunk>,
    buffer_bytes: usize,
    max_bytes: usize,
    subscribers: Vec<mpsc::Sender<LogChunk>>,
    workload_name: Option<String>,
    retired_at: Option<Instant>,
}

impl TopicState {
    fn new(max_bytes: usize, workload_name: Option<String>) -> Self {
        TopicState {
            buffer: VecDeque::new(),
            buffer_bytes: 0,
            max_bytes,
            subscribers: Vec::new(),
            workload_name,
            retired_at: None,
        }
    }

    fn publish(&mut self, mut chunk: LogChunk) {
        // Stamp workload_name from topic metadata if not already set on chunk.
        if chunk.workload_name.is_none() {
            chunk.workload_name = self.workload_name.clone();
        }

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

        // Fan out to subscribers, removing only dead senders.
        // On backpressure (Full), we drop the chunk but keep the subscriber
        // so it continues receiving future messages.
        self.subscribers.retain(|tx| {
            match tx.try_send(chunk.clone()) {
                Ok(()) => true,
                Err(mpsc::error::TrySendError::Full(_)) => true,
                Err(mpsc::error::TrySendError::Closed(_)) => false,
            }
        });
    }
}

/// A standing subscription for all topics matching a workload name within a namespace.
struct WorkloadSubscription {
    namespace_id: NamespaceId,
    workload_name: String,
    container_filter: Option<Vec<String>>,
    tx: mpsc::Sender<LogChunk>,
}

/// Shared log bus state.
struct LogBusInner {
    topics: HashMap<TopicKey, TopicState>,
    max_bytes_per_topic: usize,
    retired_ttl: Duration,
    workload_subscriptions: Vec<WorkloadSubscription>,
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
                retired_ttl: DEFAULT_RETIRED_TTL,
                workload_subscriptions: Vec::new(),
            })),
        }
    }

    /// Publish a log chunk. Called by worker ingest tasks.
    ///
    /// `workload_name` is resolved by the caller (e.g. via IdRegistryMap).
    /// On topic creation, the workload_name is stored as metadata and used for
    /// workload-scoped subscriptions. If the topic already exists with no
    /// workload_name and one is provided, it is backfilled.
    pub fn publish(&self, chunk: LogChunk, workload_name: Option<String>) {
        let mut inner = self.inner.lock().unwrap();
        let key = (
            chunk.namespace_id.clone(),
            chunk.pod_id.clone(),
            chunk.container_id.clone(),
        );
        let max_bytes = inner.max_bytes_per_topic;

        let is_new_topic = !inner.topics.contains_key(&key);

        // For new topics or topics being backfilled with a workload_name,
        // collect matching workload subscription senders.
        let needs_sub_matching = if is_new_topic {
            workload_name.is_some()
        } else {
            // Backfill case: topic exists without workload_name, now we have one.
            workload_name.is_some()
                && inner
                    .topics
                    .get(&key)
                    .map_or(false, |t| t.workload_name.is_none())
        };

        let mut matching_subs = Vec::new();
        if needs_sub_matching {
            if let Some(ref wl_name) = workload_name {
                // Prune dead workload subscriptions while we're here.
                inner.workload_subscriptions.retain(|sub| !sub.tx.is_closed());

                for sub in &inner.workload_subscriptions {
                    if sub.namespace_id != key.0 {
                        continue;
                    }
                    if sub.workload_name != *wl_name {
                        continue;
                    }
                    if let Some(ref containers) = sub.container_filter {
                        if !containers.contains(&key.2) {
                            continue;
                        }
                    }
                    matching_subs.push(sub.tx.clone());
                }
            }
        }

        let topic = inner
            .topics
            .entry(key)
            .or_insert_with(|| TopicState::new(max_bytes, workload_name.clone()));

        // Backfill workload_name if it was previously unknown.
        if topic.workload_name.is_none() && workload_name.is_some() {
            topic.workload_name = workload_name;
        }

        // Clear retirement when new data arrives (stream reconnected).
        topic.retired_at = None;

        // Register collected workload subscription senders on the new topic
        // (or newly-backfilled topic).
        for tx in matching_subs {
            topic.subscribers.push(tx);
        }

        topic.publish(chunk);

        // Lazy sweep of retired topics past TTL.
        if inner.topics.len() > SWEEP_THRESHOLD {
            let now = Instant::now();
            let ttl = inner.retired_ttl;
            inner.topics.retain(|_, t| {
                t.retired_at.map_or(true, |at| now.duration_since(at) < ttl)
            });
        }
    }

    /// Mark a topic as retired. Called when a log stream closes.
    ///
    /// The topic's buffer is preserved until the TTL expires, allowing
    /// subscribers to still read historical logs from recently-dead pods.
    pub fn retire_topic(
        &self,
        namespace_id: &NamespaceId,
        pod_id: &PodId,
        container_id: &str,
    ) {
        let mut inner = self.inner.lock().unwrap();
        let key = (namespace_id.clone(), pod_id.clone(), container_id.to_string());
        if let Some(topic) = inner.topics.get_mut(&key) {
            topic.retired_at = Some(Instant::now());
        }
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
        let (tx, rx) = mpsc::channel(SUBSCRIBER_CHANNEL_CAPACITY);
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

    /// Subscribe to logs for a workload within a namespace.
    ///
    /// Returns historical chunks from all existing topics for the workload,
    /// plus a receiver that will also deliver chunks from topics created later
    /// (e.g. new pods for the same workload).
    pub fn subscribe_by_workload(
        &self,
        namespace_id: &NamespaceId,
        workload_name: &str,
        container_filter: Option<&[String]>,
    ) -> (Vec<LogChunk>, mpsc::Receiver<LogChunk>) {
        let (tx, rx) = mpsc::channel(SUBSCRIBER_CHANNEL_CAPACITY);
        let mut inner = self.inner.lock().unwrap();
        let mut historical = Vec::new();

        // Register sender on all existing matching topics.
        for (key, topic) in inner.topics.iter_mut() {
            if key.0 != *namespace_id {
                continue;
            }
            if topic.workload_name.as_deref() != Some(workload_name) {
                continue;
            }
            if let Some(containers) = container_filter {
                if !containers.contains(&key.2) {
                    continue;
                }
            }
            for chunk in &topic.buffer {
                historical.push(chunk.clone());
            }
            topic.subscribers.push(tx.clone());
        }

        // Register standing subscription for future topics.
        inner.workload_subscriptions.push(WorkloadSubscription {
            namespace_id: namespace_id.clone(),
            workload_name: workload_name.to_string(),
            container_filter: container_filter.map(|c| c.to_vec()),
            tx,
        });

        historical.sort_by_key(|c| c.timestamp);
        (historical, rx)
    }

    /// Remove all topics and workload subscriptions for a namespace.
    pub fn remove_namespace(&self, namespace_id: &NamespaceId) {
        let mut inner = self.inner.lock().unwrap();
        inner.topics.retain(|key, _| key.0 != *namespace_id);
        inner
            .workload_subscriptions
            .retain(|sub| sub.namespace_id != *namespace_id);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_chunk(ns: &str, pod: u64, container: &str, data: &[u8]) -> LogChunk {
        make_chunk_seq(ns, pod, container, data, 0)
    }

    fn make_chunk_seq(
        ns: &str,
        pod: u64,
        container: &str,
        data: &[u8],
        seq: u64,
    ) -> LogChunk {
        LogChunk {
            namespace_id: NamespaceId::from(ns),
            pod_id: PodId::from(pod),
            container_id: container.to_string(),
            workload_name: None,
            data: data.to_vec(),
            timestamp: Instant::now(),
            seq,
        }
    }

    #[tokio::test]
    async fn test_publish_and_subscribe_historical() {
        let bus = LogBusHandle::new(1024);

        // Publish some chunks before subscribing.
        bus.publish(make_chunk("ns1", 1, "main", b"hello "), None);
        bus.publish(make_chunk("ns1", 1, "main", b"world"), None);
        bus.publish(make_chunk("ns2", 2, "main", b"other namespace"), None);

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
        bus.publish(make_chunk("ns1", 1, "main", b"before"), None);

        // Subscribe.
        let (_historical, mut rx) = bus.subscribe(&NamespaceId::from("ns1"), None, None);

        // Publish after subscribing.
        bus.publish(make_chunk("ns1", 1, "main", b"live"), None);

        let chunk = rx.try_recv().unwrap();
        assert_eq!(chunk.data, b"live");
    }

    #[tokio::test]
    async fn test_ring_buffer_eviction() {
        let bus = LogBusHandle::new(10); // 10 byte cap per topic

        bus.publish(make_chunk("ns1", 1, "main", b"12345"), None); // 5 bytes
        bus.publish(make_chunk("ns1", 1, "main", b"67890"), None); // 5 bytes, total 10
        bus.publish(make_chunk("ns1", 1, "main", b"abcde"), None); // 5 bytes, evicts first

        let (historical, _rx) = bus.subscribe(&NamespaceId::from("ns1"), None, None);
        assert_eq!(historical.len(), 2);
        assert_eq!(historical[0].data, b"67890");
        assert_eq!(historical[1].data, b"abcde");
    }

    #[tokio::test]
    async fn test_subscriber_backpressure() {
        let bus = LogBusHandle::new(1024 * 1024);

        // Create topic.
        bus.publish(make_chunk("ns1", 1, "main", b"init"), None);

        // Subscribe.
        let (_historical, rx) = bus.subscribe(&NamespaceId::from("ns1"), None, None);

        // Publish more than the channel can hold — should not block.
        let send_count = SUBSCRIBER_CHANNEL_CAPACITY + 100;
        for i in 0..send_count {
            bus.publish(
                make_chunk("ns1", 1, "main", format!("msg{}", i).as_bytes()),
                None,
            );
        }

        // Chunks that fit in the channel are delivered, excess are dropped,
        // but the subscriber must NOT be evicted — it should still receive
        // future messages after the burst.
        drop(rx);
    }

    #[tokio::test]
    async fn test_subscriber_survives_backpressure() {
        let bus = LogBusHandle::new(1024 * 1024);

        // Create topic.
        bus.publish(make_chunk("ns1", 1, "main", b"init"), None);

        // Subscribe.
        let (_historical, mut rx) = bus.subscribe(&NamespaceId::from("ns1"), None, None);

        // Fill the channel beyond capacity.
        let send_count = SUBSCRIBER_CHANNEL_CAPACITY + 100;
        for i in 0..send_count {
            bus.publish(
                make_chunk("ns1", 1, "main", format!("burst{}", i).as_bytes()),
                None,
            );
        }

        // Drain all buffered messages.
        while rx.try_recv().is_ok() {}

        // Publish another message — subscriber must still receive it.
        bus.publish(make_chunk("ns1", 1, "main", b"after-burst"), None);
        let chunk = rx.try_recv().unwrap();
        assert_eq!(chunk.data, b"after-burst");
    }

    #[tokio::test]
    async fn test_pod_filter() {
        let bus = LogBusHandle::new(1024);

        bus.publish(make_chunk("ns1", 1, "main", b"pod1"), None);
        bus.publish(make_chunk("ns1", 2, "main", b"pod2"), None);

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

        bus.publish(make_chunk("ns1", 1, "main", b"from main"), None);
        bus.publish(make_chunk("ns1", 1, "sidecar", b"from sidecar"), None);

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

        bus.publish(make_chunk("ns1", 1, "main", b"data"), None);
        bus.remove_namespace(&NamespaceId::from("ns1"));

        let (historical, _rx) = bus.subscribe(&NamespaceId::from("ns1"), None, None);
        assert!(historical.is_empty());
    }

    #[tokio::test]
    async fn test_workload_name_tagging() {
        let bus = LogBusHandle::new(1024);

        bus.publish(
            make_chunk("ns1", 1, "main", b"hello"),
            Some("my-app".to_string()),
        );

        let (historical, _rx) = bus.subscribe(&NamespaceId::from("ns1"), None, None);
        assert_eq!(historical.len(), 1);
        assert_eq!(historical[0].workload_name.as_deref(), Some("my-app"));
    }

    #[tokio::test]
    async fn test_workload_name_backfill() {
        let bus = LogBusHandle::new(1024);

        // First publish without workload_name.
        bus.publish(make_chunk("ns1", 1, "main", b"before"), None);
        // Second publish with workload_name — should backfill topic metadata.
        bus.publish(
            make_chunk("ns1", 1, "main", b"after"),
            Some("my-app".to_string()),
        );

        let (historical, _rx) = bus.subscribe(&NamespaceId::from("ns1"), None, None);
        // First chunk won't have workload_name (was published before backfill).
        assert_eq!(historical[0].workload_name, None);
        // Second chunk has it because publish stamps it from the topic's metadata.
        // Actually, workload_name on the chunk comes from the chunk itself, not topic.
        // The topic metadata is used for subscribe_by_workload matching.
        assert_eq!(historical.len(), 2);
    }

    #[tokio::test]
    async fn test_subscribe_by_workload_historical() {
        let bus = LogBusHandle::new(1024);

        bus.publish(
            make_chunk("ns1", 1, "main", b"pod1-data"),
            Some("my-app".to_string()),
        );
        bus.publish(
            make_chunk("ns1", 2, "main", b"pod2-data"),
            Some("my-app".to_string()),
        );
        bus.publish(
            make_chunk("ns1", 3, "main", b"other-app-data"),
            Some("other-app".to_string()),
        );

        let (historical, _rx) =
            bus.subscribe_by_workload(&NamespaceId::from("ns1"), "my-app", None);
        assert_eq!(historical.len(), 2);
        let data: Vec<&[u8]> = historical.iter().map(|c| c.data.as_slice()).collect();
        assert!(data.contains(&b"pod1-data".as_slice()));
        assert!(data.contains(&b"pod2-data".as_slice()));
    }

    #[tokio::test]
    async fn test_subscribe_by_workload_live_new_topic() {
        let bus = LogBusHandle::new(1024);

        // Existing topic for the workload.
        bus.publish(
            make_chunk("ns1", 1, "main", b"existing"),
            Some("my-app".to_string()),
        );

        // Subscribe by workload.
        let (_historical, mut rx) =
            bus.subscribe_by_workload(&NamespaceId::from("ns1"), "my-app", None);

        // Publish to a NEW pod for the same workload — should be auto-registered.
        bus.publish(
            make_chunk("ns1", 2, "main", b"new-pod"),
            Some("my-app".to_string()),
        );

        let chunk = rx.try_recv().unwrap();
        assert_eq!(chunk.data, b"new-pod");
    }

    #[tokio::test]
    async fn test_subscribe_by_workload_late_name_backfill() {
        // Simulates the race condition where a pod's log stream opens before
        // sync_dynamic_ids has populated the id registry. The first chunks
        // arrive with workload_name=None, then later chunks arrive with the
        // correct name. The standing subscription should attach on backfill.
        let bus = LogBusHandle::new(1024);

        // Existing topic so we can subscribe by workload.
        bus.publish(
            make_chunk("ns1", 1, "main", b"existing"),
            Some("my-app".to_string()),
        );

        // Subscribe by workload — registers standing subscription.
        let (_historical, mut rx) =
            bus.subscribe_by_workload(&NamespaceId::from("ns1"), "my-app", None);

        // New pod's log stream opens before registry is populated.
        // First chunk arrives with workload_name=None.
        bus.publish(make_chunk("ns1", 2, "main", b"early-no-name"), None);

        // Nothing should be received yet (topic has no workload_name).
        assert!(rx.try_recv().is_err());

        // Registry catches up — next chunk has the workload_name.
        // This should backfill the topic and attach the standing subscription.
        bus.publish(
            make_chunk("ns1", 2, "main", b"after-backfill"),
            Some("my-app".to_string()),
        );

        let chunk = rx.try_recv().unwrap();
        assert_eq!(chunk.data, b"after-backfill");
    }

    #[tokio::test]
    async fn test_subscribe_by_workload_container_filter() {
        let bus = LogBusHandle::new(1024);

        bus.publish(
            make_chunk("ns1", 1, "main", b"from-main"),
            Some("my-app".to_string()),
        );
        bus.publish(
            make_chunk("ns1", 1, "sidecar", b"from-sidecar"),
            Some("my-app".to_string()),
        );

        let (historical, _rx) = bus.subscribe_by_workload(
            &NamespaceId::from("ns1"),
            "my-app",
            Some(&["main".to_string()]),
        );
        assert_eq!(historical.len(), 1);
        assert_eq!(historical[0].data, b"from-main");
    }

    #[tokio::test]
    async fn test_retire_topic() {
        let bus = LogBusHandle::new(1024);

        bus.publish(make_chunk("ns1", 1, "main", b"data"), None);
        bus.retire_topic(
            &NamespaceId::from("ns1"),
            &PodId::from(1u64),
            "main",
        );

        // Retired topic is still queryable.
        let (historical, _rx) = bus.subscribe(&NamespaceId::from("ns1"), None, None);
        assert_eq!(historical.len(), 1);
        assert_eq!(historical[0].data, b"data");
    }

    #[tokio::test]
    async fn test_retire_clears_on_new_data() {
        let bus = LogBusHandle::new(1024);

        bus.publish(make_chunk("ns1", 1, "main", b"data"), None);
        bus.retire_topic(
            &NamespaceId::from("ns1"),
            &PodId::from(1u64),
            "main",
        );

        // New data clears retirement.
        bus.publish(make_chunk("ns1", 1, "main", b"reconnected"), None);

        // Verify topic is no longer retired (check internal state).
        let inner = bus.inner.lock().unwrap();
        let key = (
            NamespaceId::from("ns1"),
            PodId::from(1u64),
            "main".to_string(),
        );
        assert!(inner.topics.get(&key).unwrap().retired_at.is_none());
    }

    #[tokio::test]
    async fn test_remove_namespace_clears_workload_subscriptions() {
        let bus = LogBusHandle::new(1024);

        bus.publish(
            make_chunk("ns1", 1, "main", b"data"),
            Some("my-app".to_string()),
        );

        let (_historical, _rx) =
            bus.subscribe_by_workload(&NamespaceId::from("ns1"), "my-app", None);

        bus.remove_namespace(&NamespaceId::from("ns1"));

        let inner = bus.inner.lock().unwrap();
        assert!(inner.topics.is_empty());
        assert!(inner.workload_subscriptions.is_empty());
    }
}
