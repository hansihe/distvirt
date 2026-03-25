use anyhow::Context;
use containerd_client::services::v1::{
    AddResourceRequest,
    CreateRequest as LeaseCreateRequest,
    DeleteRequest as LeaseDeleteRequest,
    Resource as LeaseResource,
    leases_client::LeasesClient,
};
use containerd_client::with_namespace;
use tonic::Request;
use tonic::transport::Channel;

use super::generate_id;
use super::resource;

/// Manages containerd leases for resource lifecycle.
///
/// Shared across all VMs on a worker. Spawns a background task that
/// handles lease cleanup when `ContainerdLease` handles are dropped.
pub struct LeaseManager {
    channel: Channel,
    namespace: String,
    cleanup_tx: tokio::sync::mpsc::UnboundedSender<String>,
    _cleanup_task: tokio::task::JoinHandle<()>,
}

impl LeaseManager {
    pub fn new(channel: Channel, namespace: String) -> Self {
        let (cleanup_tx, mut cleanup_rx) = tokio::sync::mpsc::unbounded_channel::<String>();

        let task_channel = channel.clone();
        let task_namespace = namespace.clone();
        let cleanup_task = tokio::spawn(async move {
            while let Some(lease_id) = cleanup_rx.recv().await {
                let mut client = LeasesClient::new(task_channel.clone());
                let req = LeaseDeleteRequest {
                    id: lease_id.clone(),
                    sync: true,
                };
                if let Err(e) = client.delete(with_namespace!(req, &task_namespace)).await {
                    log::warn!("failed to delete lease {}: {}", lease_id, e);
                } else {
                    log::debug!("deleted lease {}", lease_id);
                }
            }
        });

        Self {
            channel,
            namespace,
            cleanup_tx,
            _cleanup_task: cleanup_task,
        }
    }

    /// Create a new containerd lease.
    ///
    /// The lease protects resources added to it from garbage collection.
    /// When the returned `ContainerdLease` is dropped, the lease is deleted
    /// asynchronously via the background cleanup task.
    pub async fn create_lease(&self) -> anyhow::Result<ContainerdLease> {
        let mut client = LeasesClient::new(self.channel.clone());

        let lease_id = format!("distvirt-{}", generate_id());
        let req = LeaseCreateRequest {
            id: lease_id.clone(),
            labels: Default::default(),
        };
        client
            .create(with_namespace!(req, &self.namespace))
            .await
            .with_context(|| format!("creating lease {}", lease_id))?;

        log::debug!("created lease {}", lease_id);

        Ok(ContainerdLease {
            id: lease_id,
            channel: self.channel.clone(),
            namespace: self.namespace.clone(),
            cleanup_tx: self.cleanup_tx.clone(),
        })
    }
}

/// A containerd lease handle that protects resources from garbage collection.
///
/// Add resources (snapshots, content) to the lease to prevent them from
/// being collected. When dropped, the lease is deleted via the `LeaseManager`'s
/// background task, making unreferenced resources eligible for GC.
pub struct ContainerdLease {
    id: String,
    channel: Channel,
    namespace: String,
    cleanup_tx: tokio::sync::mpsc::UnboundedSender<String>,
}

impl ContainerdLease {
    /// Add a resource to this lease, protecting it from garbage collection.
    ///
    /// Returns an error if the resource does not exist or the gRPC call fails.
    pub async fn add_resource(&self, resource: &impl resource::LeaseResource) -> anyhow::Result<()> {
        let mut client = LeasesClient::new(self.channel.clone());
        let req = AddResourceRequest {
            id: self.id.clone(),
            resource: Some(LeaseResource {
                id: resource.resource_id().to_string(),
                r#type: resource.resource_type(),
            }),
        };
        client
            .add_resource(with_namespace!(req, &self.namespace))
            .await
            .with_context(|| {
                format!(
                    "adding resource {}:{} to lease {}",
                    resource.resource_type(),
                    resource.resource_id(),
                    self.id
                )
            })?;
        Ok(())
    }

    /// Try to add a resource to this lease.
    ///
    /// Returns `Ok(true)` if the resource was successfully added (it exists
    /// and is now protected). Returns `Ok(false)` if the resource does not
    /// exist (e.g. it was garbage collected). Other errors are propagated.
    pub async fn try_add_resource(
        &self,
        resource: &impl resource::LeaseResource,
    ) -> anyhow::Result<bool> {
        let mut client = LeasesClient::new(self.channel.clone());
        let req = AddResourceRequest {
            id: self.id.clone(),
            resource: Some(LeaseResource {
                id: resource.resource_id().to_string(),
                r#type: resource.resource_type(),
            }),
        };
        match client
            .add_resource(with_namespace!(req, &self.namespace))
            .await
        {
            Ok(_) => Ok(true),
            Err(e) if e.code() == tonic::Code::NotFound => Ok(false),
            Err(e) => Err(e).with_context(|| {
                format!(
                    "adding resource {}:{} to lease {}",
                    resource.resource_type(),
                    resource.resource_id(),
                    self.id
                )
            }),
        }
    }
}

impl Drop for ContainerdLease {
    fn drop(&mut self) {
        // Best-effort: if the receiver is gone, the manager was dropped.
        let _ = self.cleanup_tx.send(self.id.clone());
    }
}
