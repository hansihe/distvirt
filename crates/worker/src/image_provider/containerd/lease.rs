use std::collections::HashMap;

use anyhow::Context;
use containerd_client::services::v1::{
    AddResourceRequest,
    CreateRequest as LeaseCreateRequest,
    DeleteRequest as LeaseDeleteRequest,
    Resource as LeaseResourceProto,
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

    /// Create a new containerd lease with a 1-hour expiry safety net.
    ///
    /// The lease protects resources created under it from garbage collection.
    /// When the returned `ContainerdLease` is dropped, the lease is deleted
    /// asynchronously via the background cleanup task.
    ///
    /// The expiry label ensures leaked leases (from crashes) are eventually
    /// cleaned up by containerd.
    pub async fn create_lease(&self) -> anyhow::Result<ContainerdLease> {
        let mut client = LeasesClient::new(self.channel.clone());

        let lease_id = format!("distvirt-{}", generate_id());
        let expire = std::time::SystemTime::now() + std::time::Duration::from_secs(3600);
        let expire_rfc3339 = format_rfc3339(expire);
        let mut labels = HashMap::new();
        labels.insert(
            "containerd.io/gc.expire".to_string(),
            expire_rfc3339,
        );

        let req = LeaseCreateRequest {
            id: lease_id.clone(),
            labels,
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
/// Use `request()` to build gRPC requests with the `containerd-lease` header.
/// Resources created under such requests are automatically added to the lease
/// in the same database transaction as creation (zero TOCTOU window).
///
/// Use `add_resource()` to manually protect existing resources (e.g. images,
/// content blobs you are reading but not creating).
///
/// When dropped, the lease is deleted via the `LeaseManager`'s background task.
pub struct ContainerdLease {
    id: String,
    channel: Channel,
    namespace: String,
    cleanup_tx: tokio::sync::mpsc::UnboundedSender<String>,
}

#[allow(dead_code)]
impl ContainerdLease {
    /// Get the lease ID.
    pub fn id(&self) -> &str {
        &self.id
    }

    /// Get the containerd namespace this lease belongs to.
    pub fn namespace(&self) -> &str {
        &self.namespace
    }

    /// Get the gRPC channel.
    pub fn channel(&self) -> &Channel {
        &self.channel
    }

    /// Build a tonic Request with `containerd-namespace` and `containerd-lease`
    /// metadata headers.
    ///
    /// Resources created by gRPC calls using this request are automatically
    /// added to the lease in the same database transaction as creation,
    /// eliminating the TOCTOU window between resource creation and lease
    /// protection.
    pub fn request<T>(&self, msg: T) -> tonic::Request<T> {
        let mut req = tonic::Request::new(msg);
        let md = req.metadata_mut();
        md.insert(
            "containerd-namespace",
            self.namespace.parse().expect("valid namespace"),
        );
        md.insert(
            "containerd-lease",
            self.id.parse().expect("valid lease ID"),
        );
        req
    }

    /// Manually add an existing resource to this lease, protecting it from GC.
    ///
    /// Use this for resources you are reading (not creating), such as image
    /// records or content blobs. For resources you create, prefer using
    /// `request()` which provides automatic lease protection.
    pub async fn add_resource(&self, resource: &impl resource::LeaseResource) -> anyhow::Result<()> {
        let mut client = LeasesClient::new(self.channel.clone());
        let req = AddResourceRequest {
            id: self.id.clone(),
            resource: Some(LeaseResourceProto {
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
}

impl Drop for ContainerdLease {
    fn drop(&mut self) {
        // Best-effort: if the receiver is gone, the manager was dropped.
        let _ = self.cleanup_tx.send(self.id.clone());
    }
}

/// Format a SystemTime as RFC 3339 (e.g. "2026-03-25T16:00:00Z").
fn format_rfc3339(t: std::time::SystemTime) -> String {
    let dur = t
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default();
    let secs = dur.as_secs();

    // Split into days and time-of-day
    const SECS_PER_DAY: u64 = 86400;
    let mut days = (secs / SECS_PER_DAY) as i64;
    let day_secs = secs % SECS_PER_DAY;
    let hour = day_secs / 3600;
    let minute = (day_secs % 3600) / 60;
    let second = day_secs % 60;

    // Convert days since epoch to y/m/d (civil calendar from days since 1970-01-01)
    // Algorithm from Howard Hinnant
    days += 719468;
    let era = if days >= 0 { days } else { days - 146096 } / 146097;
    let doe = (days - era * 146097) as u64;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    let y = yoe as i64 + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = if m <= 2 { y + 1 } else { y };

    format!("{:04}-{:02}-{:02}T{:02}:{:02}:{:02}Z", y, m, d, hour, minute, second)
}
