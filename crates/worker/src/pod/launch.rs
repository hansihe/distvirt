use anyhow::Context;
use distvirt_worker_protocol::{
    ContainerSpec, LogStreamHeader, LogStreamOpener, NamespaceId, PodId, PodNetworkConfig,
    WorkerEvent,
};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use crate::fabric::{Fabric, FabricPort};
use crate::image_provider::ImageProvider;
use crate::io_session::IoSession;
use crate::managed_vm::ManagedVm;
use crate::oci;
use crate::task_handle::TaskHandle;
use crate::vmm::{
    BaseVmConfig, BalloonConfig, GuestDevice, MountRequest, MountRestoreInfo, MountRestoreKind,
    NetConfig, ProvidedAccess, VmBuilder, Vmm,
};
use crate::worker::supervisor::send_event;

use super::{setup_instance, wait_for_vm_exit, PodResources};

/// Perform all fallible pod setup: image prep, VM launch, vsock connect,
/// network config, container start, log stream setup.
pub(crate) async fn pod_launch<V: Vmm + 'static, P: ImageProvider + 'static>(
    vmm: &V,
    image_provider: &P,
    fabric: &Fabric<FabricPort>,
    kernel_path: &std::path::PathBuf,
    rootfs_image_path: &std::path::PathBuf,
    log_opener: &LogStreamOpener,
    event_tx: &mpsc::Sender<WorkerEvent>,
    namespace_id: &NamespaceId,
    pod_id: &PodId,
    network: PodNetworkConfig,
    containers: Vec<ContainerSpec>,
    resources: Option<distvirt_worker_protocol::ResourceRequirements>,
    volumes: Vec<distvirt_worker_protocol::VolumeSpec>,
    cancel: &CancellationToken,
) -> anyhow::Result<(
    ManagedVm<V::Instance>,
    Option<(IoSession, yamux::Stream)>,
    Option<TaskHandle<()>>,
    PodResources,
)> {
    if containers.len() > 1 {
        anyhow::bail!(
            "multi-container pods are not supported (got {} containers)",
            containers.len()
        );
    }
    let container = containers
        .into_iter()
        .next()
        .context("pod must have at least one container")?;

    log::info!("pod '{}': preparing image {}", pod_id, container.image_ref);
    let artifact = tokio::select! {
        result = image_provider.prepare(&container.image_ref) => {
            result.context("preparing image")?
        }
        _ = cancel.cancelled() => {
            anyhow::bail!("cancelled during image prepare");
        }
    };
    log::info!("pod '{}': image prepared", pod_id);

    let container_id = container.container_id.clone();
    let container_volume_mounts = container.config.volume_mounts.clone();
    let config = if let Some(oci_config) = artifact.oci_config() {
        oci::merge_config(oci_config, &container.config)?
    } else {
        container.config
    };

    // Resolve user string to numeric uid/gid using the image's /etc/passwd.
    let (resolved_uid, resolved_gid) = if let Some(ref user) = config.user {
        let passwd = artifact
            .oci_config()
            .map(|c| c.passwd_entries.as_slice())
            .unwrap_or(&[]);
        let groups = artifact
            .oci_config()
            .map(|c| c.group_entries.as_slice())
            .unwrap_or(&[]);
        let (uid, gid) = oci::resolve_user(user, passwd, groups)?;
        (Some(uid), gid)
    } else {
        (None, None)
    };

    let net_config = NetConfig::from(&network);

    let (vcpu_count, mem_size_mib) = resources
        .as_ref()
        .and_then(|r| r.limits.as_ref())
        .map(|l| {
            (
                if l.vcpus > 0 { l.vcpus } else { 1 },
                if l.memory_mib > 0 {
                    l.memory_mib as u32
                } else {
                    128u32
                },
            )
        })
        .unwrap_or((1, 128));

    let balloon = resources.as_ref().and_then(|r| {
        let limits = r.limits.as_ref()?;
        let requests = r.requests.as_ref()?;
        if requests.memory_mib < limits.memory_mib && limits.memory_mib > 0 {
            Some(BalloonConfig {
                amount_mib: (limits.memory_mib - requests.memory_mib) as u32,
                deflate_on_oom: true,
                stats_polling_interval_s: 1,
            })
        } else {
            None
        }
    });

    // Prepare volume images in a temp directory.
    log::info!("pod '{}': preparing {} volume(s)", pod_id, volumes.len());
    let vol_tmpdir = tempfile::tempdir().context("create tmpdir for volumes")?;
    let prepared_volumes = crate::volume::prepare_volumes(&volumes, vol_tmpdir.path())
        .await
        .context("prepare volumes")?;
    for pv in &prepared_volumes {
        match pv {
            crate::volume::PreparedVolume::Block { name, image_path, read_only } => {
                let size = std::fs::metadata(image_path).map(|m| m.len()).unwrap_or(0);
                log::info!(
                    "pod '{}': prepared volume '{}': block image at {}, size={} bytes, read_only={}",
                    pod_id, name, image_path.display(), size, read_only
                );
            }
            crate::volume::PreparedVolume::Directory { name, dir_path, read_only, .. } => {
                log::info!(
                    "pod '{}': prepared volume '{}': directory at {}, read_only={}",
                    pod_id, name, dir_path.display(), read_only
                );
            }
        }
    }
    log::info!("pod '{}': volumes prepared", pod_id);

    // Extract image_ref before consuming the artifact.
    let image_ref = artifact.image_ref_str().to_string();

    // Create the builder.
    let mut builder = vmm.builder(BaseVmConfig {
        kernel_path: kernel_path.clone(),
        rootfs_image_path: rootfs_image_path.clone(),
        vcpu_count,
        mem_size_mib,
        net: Some(net_config.clone()),
        serial_console: true,
        balloon,
    })?;

    // Add container image mount.
    let container_plan = builder.add_mount(MountRequest {
        tag: "container".to_string(),
        source: artifact.into_mount_source(),
    })?;

    // If the VMM provides a read-only virtiofs share for the container, we
    // need a scratch device for the overlay upper/work dirs.
    if matches!(
        container_plan.provided,
        ProvidedAccess::VirtioFs { read_only: true }
    ) {
        builder.add_scratch_device("container-overlay", 256)?;
    }

    // Add volume mounts.
    for pv in &prepared_volumes {
        builder.add_mount(pv.to_mount_request())?;
    }

    // Build mount restore info for snapshot context.
    let mut mount_restore_info = Vec::new();

    // Container image restore info.
    mount_restore_info.push(MountRestoreInfo {
        tag: "container".to_string(),
        kind: MountRestoreKind::ImageRef {
            image_ref: image_ref.clone(),
        },
    });

    // Volume restore info.
    for vol_spec in &volumes {
        match &vol_spec.volume_type {
            distvirt_worker_protocol::VolumeType::ConfigData { files } => {
                mount_restore_info.push(MountRestoreInfo {
                    tag: format!("vol-{}", vol_spec.name),
                    kind: MountRestoreKind::ConfigData {
                        files: files.clone(),
                    },
                });
            }
            distvirt_worker_protocol::VolumeType::EmptyDir { .. } => {
                mount_restore_info.push(MountRestoreInfo {
                    tag: format!("vol-{}", vol_spec.name),
                    kind: MountRestoreKind::Persisted,
                });
            }
        }
    }

    builder.set_snapshot_context(mount_restore_info);

    // Launch the VM.
    log::info!("pod '{}': launching VM", pod_id);
    let (artifacts, resolved_mounts) = tokio::select! {
        result = builder.launch() => {
            result.context("launch VM")?
        }
        _ = cancel.cancelled() => {
            anyhow::bail!("cancelled during VM launch");
        }
    };
    log::info!("pod '{}': VM launched", pod_id);

    log::info!(
        "pod '{}': setting up instance (fabric + vsock connect)",
        pod_id
    );
    let (mut vm, port_task) = setup_instance(artifacts, fabric, pod_id, &network, cancel).await?;
    log::info!("pod '{}': instance setup complete", pod_id);

    // Take exit signal for the setup phase below.
    let mut vm_exit_rx = vm.exit_signal();
    let mut vm_died = std::pin::pin!(wait_for_vm_exit(&mut vm_exit_rx));

    let io_session = tokio::select! {
        result = async {
            log::info!("pod '{}': configuring guest network", pod_id);
            vm.configure_network("eth0", &net_config).await?;

            // Mount pod-scoped volumes using resolved mount info from the builder.
            for pv in &prepared_volumes {
                let vol_tag = format!("vol-{}", pv.name());
                if let Some(entry) = resolved_mounts.get(&vol_tag) {
                    let source = match &entry.guest {
                        GuestDevice::VirtioFs { virtiofs_tag } => {
                            distvirt_guest_protocol::VolumeSource::VirtioFs {
                                tag: virtiofs_tag.clone(),
                            }
                        }
                        GuestDevice::Device { path } => {
                            distvirt_guest_protocol::VolumeSource::Device {
                                device: path.clone(),
                            }
                        }
                    };
                    log::info!("pod '{}': mounting volume '{}'", pod_id, pv.name());
                    vm.mount_volume(pv.name(), source, pv.read_only()).await?;
                }
            }

            let dns_servers = vec![network.gateway.to_string()];

            // Build volume mounts for this container from the container's config.
            let volume_mounts: Vec<distvirt_guest_protocol::VolumeMount> = container_volume_mounts
                .iter()
                .map(|vm| distvirt_guest_protocol::VolumeMount {
                    name: vm.name.clone(),
                    mount_path: vm.mount_path.clone(),
                })
                .collect();

            // Map resolved mounts to guest protocol container rootfs type.
            let container_rootfs = {
                let container_entry = resolved_mounts
                    .get("container")
                    .context("no resolved mount for 'container'")?;
                let overlay_entry = resolved_mounts.get("container-overlay");

                match (&container_entry.guest, overlay_entry) {
                    (GuestDevice::VirtioFs { virtiofs_tag }, Some(overlay)) => {
                        let overlay_device = match &overlay.guest {
                            GuestDevice::Device { path } => path.clone(),
                            _ => anyhow::bail!("container-overlay resolved as non-device"),
                        };
                        distvirt_guest_protocol::ContainerRootfs::VirtioFsOverlay {
                            tag: virtiofs_tag.clone(),
                            overlay_device,
                        }
                    }
                    (GuestDevice::Device { path }, _) => {
                        distvirt_guest_protocol::ContainerRootfs::Device {
                            device: path.clone(),
                        }
                    }
                    (GuestDevice::VirtioFs { .. }, None) => {
                        anyhow::bail!("container is virtiofs read-only but no overlay device was resolved");
                    }
                }
            };

            log::info!("pod '{}': adding container '{}'", pod_id, container_id);
            vm.add_container(&container_id, container_rootfs, &dns_servers, volume_mounts)
                .await?;

            log::info!("pod '{}': starting container '{}'", pod_id, container_id);
            vm.start_container(&container_id, &config, resolved_uid, resolved_gid)
                .await?;

            // Set up log streaming via yamux log streams.
            let io_session = if config.capture_output {
                log::info!("pod '{}': accepting output stream", pod_id);
                match vm.accept_output_stream().await {
                    Ok((_cid, session)) => {
                        let header = LogStreamHeader {
                            namespace_id: namespace_id.clone(),
                            pod_id: pod_id.clone(),
                            container_id: container_id.to_string(),
                        };
                        match log_opener.open_log_stream(&header).await {
                            Ok(log_stream) => Some((session, log_stream)),
                            Err(e) => {
                                log::error!("pod '{}': failed to open log stream: {:#}", pod_id, e);
                                send_event(
                                    event_tx,
                                    WorkerEvent::PodLogStreamError {
                                        namespace_id: namespace_id.clone(),
                                        pod_id: pod_id.clone(),
                                        container_id: container_id.to_string(),
                                        phase: "open_stream".to_string(),
                                        error: format!("{:#}", e),
                                    },
                                )
                                .await;
                                None
                            }
                        }
                    }
                    Err(e) => {
                        log::error!("pod '{}': failed to accept output stream: {:#}", pod_id, e);
                        send_event(
                            event_tx,
                            WorkerEvent::PodLogStreamError {
                                namespace_id: namespace_id.clone(),
                                pod_id: pod_id.clone(),
                                container_id: container_id.to_string(),
                                phase: "connect".to_string(),
                                error: format!("{:#}", e),
                            },
                        )
                        .await;
                        None
                    }
                }
            } else {
                None
            };

            Ok::<_, anyhow::Error>(io_session)
        } => { result? }
        _ = cancel.cancelled() => {
            anyhow::bail!("cancelled during VM setup");
        }
        _ = &mut vm_died => {
            anyhow::bail!("VM process exited during setup");
        }
    };

    let resources = PodResources {
        _prepared_volumes: prepared_volumes,
        _vol_tmpdir: Some(vol_tmpdir),
        _config_data_dirs: Vec::new(),
    };

    Ok((vm, io_session, port_task, resources))
}
