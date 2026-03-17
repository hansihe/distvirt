use std::collections::{HashMap, HashSet, VecDeque};

use tokio::sync::mpsc;

use crate::types::*;

/// Maximum number of log chunks buffered per workload.
const LOG_BUFFER_CAP: usize = 500;

#[derive(Clone)]
pub struct LogChunkData {
    pub namespace_id: NamespaceId,
    pub workload_id: WorkloadId,
    pub data: Vec<u8>,
}

pub(super) struct LogSubscriber {
    pub namespace_id: NamespaceId,
    pub workload_id: Option<WorkloadId>,
    pub tx: mpsc::Sender<LogChunkData>,
}

#[derive(Clone)]
pub struct EventData {
    pub namespace_id: NamespaceId,
    pub event: SmNamespaceEvent,
}

pub(super) struct EventSubscriber {
    pub namespace_id: NamespaceId,
    pub workload_ids: HashSet<WorkloadId>,
    pub service_ids: HashSet<ServiceId>,
    pub tx: mpsc::Sender<EventData>,
}

pub(super) struct Subscriptions {
    pub log_subscribers: Vec<LogSubscriber>,
    pub log_buffers: HashMap<(NamespaceId, WorkloadId), VecDeque<LogChunkData>>,
    pub event_subscribers: Vec<EventSubscriber>,
}

impl Subscriptions {
    pub fn new() -> Self {
        Subscriptions {
            log_subscribers: Vec::new(),
            log_buffers: HashMap::new(),
            event_subscribers: Vec::new(),
        }
    }

    pub fn handle_log_data(
        &mut self,
        namespace_id: NamespaceId,
        workload_id: WorkloadId,
        data: Vec<u8>,
    ) {
        let chunk = LogChunkData {
            namespace_id: namespace_id.clone(),
            workload_id: workload_id.clone(),
            data,
        };

        // Buffer the chunk.
        let buf = self
            .log_buffers
            .entry((namespace_id.clone(), workload_id.clone()))
            .or_insert_with(VecDeque::new);
        buf.push_back(chunk.clone());
        if buf.len() > LOG_BUFFER_CAP {
            buf.pop_front();
        }

        // Distribute to matching live subscribers, removing closed ones.
        self.log_subscribers.retain(|sub| {
            if sub.namespace_id != namespace_id {
                return true;
            }
            if let Some(ref wl) = sub.workload_id {
                if *wl != workload_id {
                    return true;
                }
            }
            // Skip full channels, remove closed ones.
            match sub.tx.try_send(chunk.clone()) {
                Ok(()) => true,
                Err(mpsc::error::TrySendError::Full(_)) => true,
                Err(mpsc::error::TrySendError::Closed(_)) => false,
            }
        });
    }

    pub fn subscribe_logs(
        &mut self,
        namespace_id: NamespaceId,
        workload_id: Option<WorkloadId>,
        log_tx: mpsc::Sender<LogChunkData>,
    ) {
        // Replay buffered history to the new subscriber.
        for ((ns, wl), buf) in &self.log_buffers {
            if *ns != namespace_id {
                continue;
            }
            if let Some(ref filter_wl) = workload_id {
                if *wl != *filter_wl {
                    continue;
                }
            }
            for chunk in buf {
                if log_tx.try_send(chunk.clone()).is_err() {
                    break;
                }
            }
        }

        // Add to live subscriber list.
        self.log_subscribers.push(LogSubscriber {
            namespace_id,
            workload_id,
            tx: log_tx,
        });
    }

    pub fn distribute_events(&mut self, ns_id: &NamespaceId, events: &[SmNamespaceEvent]) {
        for sm_event in events {
            // Extract workload_id and service_id for filtering.
            let (event_wl_id, event_svc_id) = match sm_event {
                SmNamespaceEvent::Workload { workload_id, .. } => (Some(workload_id), None),
                SmNamespaceEvent::Service {
                    workload_id,
                    service_id,
                    ..
                } => (Some(workload_id), Some(service_id)),
            };

            let event_data = EventData {
                namespace_id: ns_id.clone(),
                event: sm_event.clone(),
            };

            self.event_subscribers.retain(|sub| {
                if sub.namespace_id != *ns_id {
                    return true;
                }
                // Apply workload filter (empty = no filter).
                if !sub.workload_ids.is_empty() {
                    if event_wl_id.map_or(true, |wl| !sub.workload_ids.contains(wl)) {
                        return true; // Keep subscriber, just doesn't match this event.
                    }
                }
                // Apply service filter (empty = no filter).
                if !sub.service_ids.is_empty() {
                    if event_svc_id.map_or(true, |svc| !sub.service_ids.contains(svc)) {
                        return true;
                    }
                }
                match sub.tx.try_send(event_data.clone()) {
                    Ok(()) => true,
                    Err(mpsc::error::TrySendError::Full(_)) => true,
                    Err(mpsc::error::TrySendError::Closed(_)) => false,
                }
            });
        }
    }

    pub fn subscribe_events(
        &mut self,
        namespace_id: NamespaceId,
        workload_ids: HashSet<WorkloadId>,
        service_ids: HashSet<ServiceId>,
        event_tx: mpsc::Sender<EventData>,
    ) {
        self.event_subscribers.push(EventSubscriber {
            namespace_id,
            workload_ids,
            service_ids,
            tx: event_tx,
        });
    }
}
