//! High-level namespace watcher — owns a `NamespaceModel` and an event stream.
//!
//! Bootstraps by subscribing to events *before* fetching status (no missed
//! window), then drives the model from the stream.

use tonic::Streaming;

use distvirt_client_protocol as proto;

use crate::connection::{handle_grpc_error, Client};
use crate::model::{NamespaceModel, StateChange};

/// A live view of a namespace that applies events to a local model.
pub struct NamespaceWatcher {
    model: NamespaceModel,
    events: Streaming<proto::NamespaceEvent>,
}

impl NamespaceWatcher {
    /// Bootstrap: subscribe to events, fetch status, return a consistent watcher.
    ///
    /// Events are subscribed *before* the status fetch so that no events are
    /// lost between the snapshot and the stream.
    pub async fn start(client: &mut Client, namespace_id: &str) -> anyhow::Result<Self> {
        // Subscribe first
        let events = client
            .stream_events(proto::StreamEventsRequest {
                namespace_id: namespace_id.to_string(),
                workload_ids: vec![],
                service_ids: vec![],
            })
            .await
            .map_err(handle_grpc_error)?
            .into_inner();

        // Then fetch snapshot
        let resp = client
            .get_namespace_status(proto::GetNamespaceStatusRequest {
                namespace_id: namespace_id.to_string(),
            })
            .await
            .map_err(handle_grpc_error)?;

        let status = resp
            .into_inner()
            .status
            .ok_or_else(|| anyhow::anyhow!("server returned empty status"))?;

        let model = NamespaceModel::from_status(&status);

        Ok(NamespaceWatcher { model, events })
    }

    /// Block until the next event, apply it to the model, return what changed.
    ///
    /// Returns `Ok(None)` when the event stream ends.
    pub async fn next(&mut self) -> anyhow::Result<Option<StateChange>> {
        loop {
            match self.events.message().await {
                Ok(Some(event)) => {
                    if let Some(change) = self.model.apply_event(&event) {
                        return Ok(Some(change));
                    }
                    // Event didn't produce a state change — keep reading.
                }
                Ok(None) => return Ok(None),
                Err(status) => return Err(handle_grpc_error(status)),
            }
        }
    }

    /// Block until the predicate returns true, applying events along the way.
    ///
    /// Returns `Ok(())` when the predicate is satisfied.
    /// Returns `Err` if the stream ends before the predicate is satisfied,
    /// or on a gRPC error.
    pub async fn wait_for<F>(&mut self, predicate: F) -> anyhow::Result<()>
    where
        F: Fn(&NamespaceModel) -> bool,
    {
        // Check immediately — the predicate might already be true.
        if predicate(&self.model) {
            return Ok(());
        }

        loop {
            match self.next().await? {
                Some(_) => {
                    if predicate(&self.model) {
                        return Ok(());
                    }
                }
                None => {
                    return Err(anyhow::anyhow!(
                        "event stream ended before predicate was satisfied"
                    ));
                }
            }
        }
    }

    /// Access the current model state.
    pub fn model(&self) -> &NamespaceModel {
        &self.model
    }

    /// Mutable access to the model (for tests or advanced usage).
    pub fn model_mut(&mut self) -> &mut NamespaceModel {
        &mut self.model
    }

    /// Consume the watcher, returning the model and event stream separately.
    pub fn into_parts(self) -> (NamespaceModel, Streaming<proto::NamespaceEvent>) {
        (self.model, self.events)
    }
}
