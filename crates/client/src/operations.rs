use distvirt_client_protocol::*;
use tonic::Streaming;

use crate::connection::{handle_grpc_error, Client};
use crate::errors::ApiError;

pub enum ApplyOutcome {
    Created,
    Patched,
}

/// Apply a spec: create the namespace if new, patch (upsert) workloads/services if it exists.
pub async fn apply(
    client: &mut Client,
    namespace_id: &str,
    spec: &NamespaceSpec,
) -> Result<ApplyOutcome, ApiError> {
    let result = client
        .create_namespace(CreateNamespaceRequest {
            namespace_id: namespace_id.to_string(),
            spec: Some(spec.clone()),
        })
        .await;

    match result {
        Ok(_) => Ok(ApplyOutcome::Created),
        Err(status) if status.code() == tonic::Code::AlreadyExists => {
            client
                .patch_namespace(PatchNamespaceRequest {
                    namespace_id: namespace_id.to_string(),
                    workloads: spec.workloads.clone(),
                    services: spec.services.clone(),
                    remove_workloads: vec![],
                    remove_services: vec![],
                })
                .await
                .map_err(handle_grpc_error)?;
            Ok(ApplyOutcome::Patched)
        }
        Err(status) => Err(handle_grpc_error(status)),
    }
}

pub enum SyncOutcome {
    Created,
    Synced,
}

/// Sync a spec: create the namespace if new, fully replace the spec if it exists.
pub async fn sync(
    client: &mut Client,
    namespace_id: &str,
    spec: &NamespaceSpec,
) -> Result<SyncOutcome, ApiError> {
    let result = client
        .create_namespace(CreateNamespaceRequest {
            namespace_id: namespace_id.to_string(),
            spec: Some(spec.clone()),
        })
        .await;

    match result {
        Ok(_) => Ok(SyncOutcome::Created),
        Err(status) if status.code() == tonic::Code::AlreadyExists => {
            client
                .update_namespace(UpdateNamespaceRequest {
                    namespace_id: namespace_id.to_string(),
                    spec: Some(spec.clone()),
                })
                .await
                .map_err(handle_grpc_error)?;
            Ok(SyncOutcome::Synced)
        }
        Err(status) => Err(handle_grpc_error(status)),
    }
}

/// Delete a namespace.
pub async fn down(client: &mut Client, namespace_id: &str) -> Result<(), ApiError> {
    client
        .delete_namespace(DeleteNamespaceRequest {
            namespace_id: namespace_id.to_string(),
        })
        .await
        .map_err(handle_grpc_error)?;
    Ok(())
}

/// Clone a namespace.
pub async fn clone_namespace(
    client: &mut Client,
    source: &str,
    target: &str,
) -> Result<(), ApiError> {
    client
        .clone_namespace(CloneNamespaceRequest {
            source_namespace_id: source.to_string(),
            target_namespace_id: target.to_string(),
            overrides: None,
        })
        .await
        .map_err(handle_grpc_error)?;
    Ok(())
}

pub struct DeactivateOutcome {
    pub deactivated: bool,
    pub reason: String,
}

/// Hint the orchestrator to deactivate a workload.
pub async fn deactivate(
    client: &mut Client,
    namespace_id: &str,
    workload_id: &str,
) -> Result<DeactivateOutcome, ApiError> {
    let resp = client
        .deactivate_workload(DeactivateWorkloadRequest {
            namespace_id: namespace_id.to_string(),
            workload_id: workload_id.to_string(),
        })
        .await
        .map_err(handle_grpc_error)?;

    let resp = resp.into_inner();
    Ok(DeactivateOutcome {
        deactivated: resp.deactivated,
        reason: resp.reason,
    })
}

/// Stream log output from a namespace, optionally filtered to a workload.
pub async fn stream_logs(
    client: &mut Client,
    namespace_id: &str,
    workload_name: Option<&str>,
) -> Result<Streaming<StreamLogsResponse>, ApiError> {
    let stream = client
        .stream_logs(StreamLogsRequest {
            namespace_id: namespace_id.to_string(),
            workload_name: workload_name.map(|w| w.to_string()),
            container_ids: vec![],
            pod_ids: vec![],
        })
        .await
        .map_err(handle_grpc_error)?
        .into_inner();
    Ok(stream)
}

/// Stream events from a namespace, optionally filtered to specific workloads/services.
pub async fn stream_events(
    client: &mut Client,
    namespace_id: &str,
    workload_ids: &[String],
    service_ids: &[String],
) -> Result<Streaming<NamespaceEvent>, ApiError> {
    let stream = client
        .stream_events(StreamEventsRequest {
            namespace_id: namespace_id.to_string(),
            workload_ids: workload_ids.to_vec(),
            service_ids: service_ids.to_vec(),
        })
        .await
        .map_err(handle_grpc_error)?
        .into_inner();
    Ok(stream)
}

/// Fetch the current status of a namespace.
pub async fn get_status(
    client: &mut Client,
    namespace_id: &str,
) -> Result<NamespaceStatusReport, ApiError> {
    let resp = client
        .get_namespace_status(GetNamespaceStatusRequest {
            namespace_id: namespace_id.to_string(),
        })
        .await
        .map_err(handle_grpc_error)?;

    resp.into_inner()
        .status
        .ok_or(ApiError::EmptyResponse)
}

/// Subscribe to events then fetch status, ensuring no events are missed.
/// Returns the status report and the event stream.
pub async fn watch_status(
    client: &mut Client,
    namespace_id: &str,
) -> Result<(NamespaceStatusReport, Streaming<NamespaceEvent>), ApiError> {
    // Subscribe to events *before* fetching status to avoid missing events
    let event_stream = stream_events(client, namespace_id, &[], &[]).await?;
    let report = get_status(client, namespace_id).await?;
    Ok((report, event_stream))
}
