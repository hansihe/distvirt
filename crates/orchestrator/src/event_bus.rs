//! Observability event fan-out bus.
//!
//! Similar to `log_bus.rs`: per-namespace ring buffer of events with
//! multi-subscriber fan-out. Subscribers receive historical events followed
//! by a live stream.

use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex};

use tokio::sync::mpsc;

use crate::adapter::observability::ObservabilityEvent;
use crate::types::NamespaceId;

/// Per-namespace state: ring buffer + subscriber list.
struct NamespaceState {
    buffer: VecDeque<ObservabilityEvent>,
    max_events: usize,
    subscribers: Vec<mpsc::Sender<ObservabilityEvent>>,
}

impl NamespaceState {
    fn new(max_events: usize) -> Self {
        NamespaceState {
            buffer: VecDeque::new(),
            max_events,
            subscribers: Vec::new(),
        }
    }

    fn publish(&mut self, event: ObservabilityEvent) {
        self.buffer.push_back(event.clone());

        // Evict oldest while over cap.
        while self.buffer.len() > self.max_events {
            self.buffer.pop_front();
        }

        // Fan out to subscribers, removing dead senders.
        self.subscribers
            .retain(|tx| tx.try_send(event.clone()).is_ok());
    }
}

/// Shared event bus state.
struct EventBusInner {
    namespaces: HashMap<NamespaceId, NamespaceState>,
    max_events_per_namespace: usize,
}

/// Handle to the event bus. Cheap to clone (Arc wrapper).
#[derive(Clone)]
pub struct EventBusHandle {
    inner: Arc<Mutex<EventBusInner>>,
}

impl EventBusHandle {
    /// Create a new event bus.
    pub fn new(max_events_per_namespace: usize) -> Self {
        EventBusHandle {
            inner: Arc::new(Mutex::new(EventBusInner {
                namespaces: HashMap::new(),
                max_events_per_namespace,
            })),
        }
    }

    /// Publish a batch of observability events for a namespace.
    pub fn publish(&self, namespace_id: &NamespaceId, events: Vec<ObservabilityEvent>) {
        if events.is_empty() {
            return;
        }
        let mut inner = self.inner.lock().unwrap();
        let max = inner.max_events_per_namespace;
        let state = inner
            .namespaces
            .entry(namespace_id.clone())
            .or_insert_with(|| NamespaceState::new(max));
        for event in events {
            state.publish(event);
        }
    }

    /// Subscribe to observability events for a namespace.
    ///
    /// Returns historical events followed by a receiver for live events.
    pub fn subscribe(
        &self,
        namespace_id: &NamespaceId,
    ) -> (Vec<ObservabilityEvent>, mpsc::Receiver<ObservabilityEvent>) {
        let (tx, rx) = mpsc::channel(256);
        let mut inner = self.inner.lock().unwrap();
        let max = inner.max_events_per_namespace;
        let state = inner
            .namespaces
            .entry(namespace_id.clone())
            .or_insert_with(|| NamespaceState::new(max));

        let historical: Vec<ObservabilityEvent> = state.buffer.iter().cloned().collect();
        state.subscribers.push(tx);

        (historical, rx)
    }

    /// Remove all state for a namespace.
    pub fn remove_namespace(&self, namespace_id: &NamespaceId) {
        let mut inner = self.inner.lock().unwrap();
        inner.namespaces.remove(namespace_id);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::adapter::observability::{
        WorkloadEventKind, WorkloadObservabilityEvent,
    };
    use crate::sm::WlStatus;

    fn make_event(wl_id: u64, old: WlStatus, new: WlStatus) -> ObservabilityEvent {
        ObservabilityEvent::Workload(WorkloadObservabilityEvent {
            workload_id: crate::sm::WorkloadId(wl_id),
            event: WorkloadEventKind::StatusChanged { old, new },
        })
    }

    #[tokio::test]
    async fn test_publish_and_subscribe_historical() {
        let bus = EventBusHandle::new(100);
        let ns = NamespaceId::new("ns1", 0);

        bus.publish(
            &ns,
            vec![make_event(1, WlStatus::Dormant, WlStatus::Launching)],
        );
        bus.publish(
            &ns,
            vec![make_event(1, WlStatus::Launching, WlStatus::Running)],
        );

        let (historical, _rx) = bus.subscribe(&ns);
        assert_eq!(historical.len(), 2);
    }

    #[tokio::test]
    async fn test_live_delivery() {
        let bus = EventBusHandle::new(100);
        let ns = NamespaceId::new("ns1", 0);

        // Create namespace state.
        bus.publish(
            &ns,
            vec![make_event(1, WlStatus::Dormant, WlStatus::Launching)],
        );

        let (_historical, mut rx) = bus.subscribe(&ns);

        bus.publish(
            &ns,
            vec![make_event(1, WlStatus::Launching, WlStatus::Running)],
        );

        let event = rx.try_recv().unwrap();
        assert_eq!(
            event,
            make_event(1, WlStatus::Launching, WlStatus::Running)
        );
    }

    #[tokio::test]
    async fn test_ring_buffer_eviction() {
        let bus = EventBusHandle::new(2);
        let ns = NamespaceId::new("ns1", 0);

        bus.publish(
            &ns,
            vec![
                make_event(1, WlStatus::Dormant, WlStatus::Launching),
                make_event(1, WlStatus::Launching, WlStatus::Running),
                make_event(1, WlStatus::Running, WlStatus::Suspending),
            ],
        );

        let (historical, _rx) = bus.subscribe(&ns);
        assert_eq!(historical.len(), 2);
        // Should have the last two events.
        assert_eq!(
            historical[0],
            make_event(1, WlStatus::Launching, WlStatus::Running)
        );
        assert_eq!(
            historical[1],
            make_event(1, WlStatus::Running, WlStatus::Suspending)
        );
    }

    #[tokio::test]
    async fn test_remove_namespace() {
        let bus = EventBusHandle::new(100);
        let ns = NamespaceId::new("ns1", 0);

        bus.publish(
            &ns,
            vec![make_event(1, WlStatus::Dormant, WlStatus::Launching)],
        );
        bus.remove_namespace(&ns);

        let (historical, _rx) = bus.subscribe(&ns);
        assert!(historical.is_empty());
    }
}
