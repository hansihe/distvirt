mod launch;
mod restore;

pub(crate) use launch::pod_launch;
pub(crate) use restore::pod_restore;

use std::process::ExitStatus;

use distvirt_worker_protocol::PodNetworkConfig;
use tokio::sync::watch;
use tokio_util::sync::CancellationToken;

use crate::fabric::{Fabric, FabricPort};
use crate::managed_vm::ManagedVm;
use crate::task_handle::TaskHandle;
use crate::vmm::{VmArtifacts, VmInstance};

/// RAII handle for resources that must stay alive for the entire pod lifetime.
///
/// Holds the prepared volume handles (whose cleanup handles keep ConfigData
/// temp directories alive for virtiofsd), and the volume tmpdir (which holds
/// the overlay image and block device volume images).
pub(crate) struct PodResources {
    pub(crate) _prepared_volumes: Vec<crate::volume::PreparedVolume>,
    pub(crate) _vol_tmpdir: Option<tempfile::TempDir>,
    /// Cleanup handles for config data directories recreated during restore.
    pub(crate) _config_data_dirs: Vec<tempfile::TempDir>,
}

/// Wire a freshly launched/restored VM instance into the fabric and establish
/// the yamux control connection.
pub(crate) async fn setup_instance<I: VmInstance>(
    artifacts: VmArtifacts<I>,
    fabric: &Fabric<FabricPort>,
    pod_id: &distvirt_worker_protocol::PodId,
    network: &PodNetworkConfig,
    cancel: &CancellationToken,
) -> anyhow::Result<(ManagedVm<I>, Option<TaskHandle<()>>)> {
    let VmArtifacts {
        instance,
        vsock_stream,
        fabric_port,
        exit_signal,
    } = artifacts;

    let port_task = if let Some(port) = fabric_port {
        let (_port_id, task) = fabric.add_port_raw_with_ip(port, network.ip);
        log::info!("worker: pod '{}' network port added to fabric", pod_id);
        Some(task)
    } else {
        None
    };

    // Clone exit signal so we can detect VM death during connect,
    // while still passing the original into ManagedVm.
    let mut vm_exit_rx = exit_signal.clone();
    let mut vm_died = std::pin::pin!(wait_for_vm_exit(&mut vm_exit_rx));


    let vm = tokio::select! {
        result = ManagedVm::connect(instance, vsock_stream, exit_signal) => { result? }
        _ = cancel.cancelled() => {
            anyhow::bail!("cancelled during VM connect");
        }
        _ = &mut vm_died => {
            anyhow::bail!("VM process exited during setup");
        }
    };

    Ok((vm, port_task))
}

/// Create a future that resolves when the VM process exits.
pub(crate) async fn wait_for_vm_exit(rx: &mut watch::Receiver<Option<ExitStatus>>) {
    let _ = rx.wait_for(|s| s.is_some()).await;
}
