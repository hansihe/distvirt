use std::path::Path;

use anyhow::{bail, Context};

use distvirt_guest_protocol::{GuestMessage, HostMessage, VSOCK_PORT};

use crate::containerd::{parse_user_numeric, ImageConfig};
use crate::image_provider::ImageProvider;
use crate::vmm::{NetConfig, VmConfig, VmInstance, Vmm};
use crate::vsock_client::GuestConnection;

/// Container execution configuration.
struct ContainerConfig {
    pub entrypoint: String,
    pub args: Vec<String>,
    pub env: Vec<String>,
    pub working_dir: Option<String>,
    pub uid: Option<u32>,
    pub gid: Option<u32>,
    pub hostname: Option<String>,
}

/// Overrides that can be specified on the CLI to override image config.
pub struct ImageOverrides {
    pub entrypoint: Option<String>,
    pub args: Vec<String>,
    pub env: Vec<String>,
    pub working_dir: Option<String>,
    pub uid: Option<u32>,
    pub gid: Option<u32>,
    pub hostname: Option<String>,
}

/// Run a container using an image provider.
///
/// The provider prepares the container filesystem (ext4 image or block device),
/// and if it returns an OCI config, that config is merged with the overrides.
/// If no OCI config is available (bare rootfs), the overrides must supply at
/// least an entrypoint.
pub async fn run(
    vmm: &impl Vmm,
    kernel_path: &Path,
    rootfs_image_path: &Path,
    provider: &impl ImageProvider,
    image_ref: &str,
    overrides: &ImageOverrides,
) -> anyhow::Result<i32> {
    let artifact = provider.prepare(image_ref).await.context("preparing image")?;

    let config = if let Some(ref oci_config) = artifact.oci_config {
        merge_config(oci_config, overrides)?
    } else {
        config_from_overrides(overrides)?
    };

    run_with_image(vmm, kernel_path, rootfs_image_path, &artifact.image_path, &config).await
}

/// Run a container from a pre-built ext4 image.
async fn run_with_image(
    vmm: &impl Vmm,
    kernel_path: &Path,
    rootfs_image_path: &Path,
    container_image_path: &Path,
    config: &ContainerConfig,
) -> anyhow::Result<i32> {
    let vm_config = VmConfig {
        kernel_path: kernel_path.to_path_buf(),
        rootfs_image_path: rootfs_image_path.to_path_buf(),
        container_image_path: container_image_path.to_path_buf(),
        vcpu_count: 1,
        mem_size_mib: 128,
        net: Some(NetConfig {
            guest_ip: "172.16.0.2".to_string(),
            netmask: "255.255.255.0".to_string(),
            gateway: "172.16.0.1".to_string(),
        }),
    };

    let mut instance = vmm.launch(&vm_config).await.context("launch VM")?;
    log::info!("VM launched");

    // Start the L2 fabric switch and gateway before vsock (guest boot takes seconds).
    let _fabric = if let Some(tap) = instance.take_tap() {
        let mut fabric = crate::fabric::Fabric::new();

        // Start the fabric gateway (smoltcp + TUN + DNS forwarding).
        // Must be set up before add_port so port tasks get the gateway channel.
        let (gateway, egress_tx, ingress_rx) =
            crate::fabric::gateway::FabricGateway::new()
                .context("create fabric gateway")?;
        fabric.set_gateway(egress_tx, ingress_rx);
        tokio::spawn(gateway.run());

        let tap_name = tap.name.clone();
        fabric
            .add_port(tap)
            .map_err(|e| anyhow::anyhow!("fabric add_port for {}: {}", tap_name, e))?;

        log::info!("fabric: started L2 switch with gateway on {}", tap_name);
        Some(fabric)
    } else {
        None
    };

    // Connect to guest over vsock.
    log::info!("connecting vsock");
    let stream = instance
        .connect_vsock(VSOCK_PORT)
        .await
        .context("connect vsock")?;
    let mut conn = GuestConnection::new(stream);

    // Wait for Ready.
    let msg: GuestMessage = conn.recv().await.context("receive Ready")?;
    match msg {
        GuestMessage::Ready => log::info!("guest is ready"),
        other => bail!("expected Ready, got {:?}", other),
    }

    // Configure network if enabled.
    if let Some(ref net_config) = vm_config.net {
        conn.send(&HostMessage::ConfigureNetwork {
            interface: "eth0".to_string(),
            ip: net_config.guest_ip.clone(),
            netmask: net_config.netmask.clone(),
            gateway: net_config.gateway.clone(),
        })
        .await
        .context("send ConfigureNetwork")?;

        let msg: GuestMessage = conn.recv().await.context("receive NetworkConfigured")?;
        match msg {
            GuestMessage::NetworkConfigured => log::info!("guest network configured"),
            GuestMessage::Error { message } => bail!("ConfigureNetwork failed: {}", message),
            other => bail!("expected NetworkConfigured, got {:?}", other),
        }
    }

    // Add container (second virtio block device = /dev/vdb).
    let container_id = "default".to_string();
    conn.send(&HostMessage::AddContainer {
        id: container_id.clone(),
        device: "/dev/vdb".to_string(),
        dns_servers: vec!["172.16.0.1".to_string()],
    })
    .await
    .context("send AddContainer")?;

    let msg: GuestMessage = conn.recv().await.context("receive ContainerAdded")?;
    match msg {
        GuestMessage::ContainerAdded { id } => log::info!("container added: {}", id),
        GuestMessage::Error { message } => bail!("AddContainer failed: {}", message),
        other => bail!("expected ContainerAdded, got {:?}", other),
    }

    // Start container.
    conn.send(&HostMessage::StartContainer {
        id: container_id.clone(),
        entrypoint: config.entrypoint.clone(),
        args: config.args.clone(),
        env: config.env.clone(),
        working_dir: config.working_dir.clone(),
        uid: config.uid,
        gid: config.gid,
        hostname: config.hostname.clone(),
    })
    .await
    .context("send StartContainer")?;

    let msg: GuestMessage = conn.recv().await.context("receive ContainerStarted")?;
    match msg {
        GuestMessage::ContainerStarted { id, pid } => {
            log::info!("container {} started with pid {}", id, pid)
        }
        GuestMessage::Error { message } => bail!("StartContainer failed: {}", message),
        other => bail!("expected ContainerStarted, got {:?}", other),
    }

    // Wait for container to exit.
    let msg: GuestMessage = conn.recv().await.context("receive ContainerExited")?;
    let exit_code = match msg {
        GuestMessage::ContainerExited { id, code } => {
            log::info!("container {} exited with code {}", id, code);
            code
        }
        GuestMessage::Error { message } => bail!("container error: {}", message),
        other => bail!("expected ContainerExited, got {:?}", other),
    };

    // Shut down the guest.
    conn.send(&HostMessage::Shutdown)
        .await
        .context("send Shutdown")?;

    // Wait for the VM to exit.
    instance.wait().await.context("wait for VM")?;

    Ok(exit_code)
}

/// Build a ContainerConfig from overrides only (no OCI image config).
fn config_from_overrides(overrides: &ImageOverrides) -> anyhow::Result<ContainerConfig> {
    let entrypoint = overrides
        .entrypoint
        .clone()
        .context("no entrypoint specified and image has no OCI config")?;

    Ok(ContainerConfig {
        entrypoint,
        args: overrides.args.clone(),
        env: overrides.env.clone(),
        working_dir: overrides.working_dir.clone(),
        uid: overrides.uid,
        gid: overrides.gid,
        hostname: overrides.hostname.clone(),
    })
}

/// Merge image config with CLI overrides following OCI entrypoint/cmd resolution rules.
fn merge_config(
    image: &ImageConfig,
    overrides: &ImageOverrides,
) -> anyhow::Result<ContainerConfig> {
    let (entrypoint, args) = if let Some(ref ep) = overrides.entrypoint {
        (ep.clone(), overrides.args.clone())
    } else if !image.entrypoint.is_empty() {
        let args = if !overrides.args.is_empty() {
            overrides.args.clone()
        } else {
            image.cmd.clone()
        };
        (image.entrypoint[0].clone(), {
            let mut a: Vec<String> = image.entrypoint[1..].to_vec();
            a.extend(args);
            a
        })
    } else if !image.cmd.is_empty() {
        (image.cmd[0].clone(), image.cmd[1..].to_vec())
    } else {
        bail!("image has no entrypoint or cmd, and none was specified on the command line");
    };

    let mut env = image.env.clone();
    env.extend(overrides.env.iter().cloned());

    let (img_uid, img_gid) = image
        .user
        .as_deref()
        .map(parse_user_numeric)
        .transpose()?
        .unwrap_or((None, None));

    Ok(ContainerConfig {
        entrypoint,
        args,
        env,
        working_dir: overrides.working_dir.clone().or_else(|| image.working_dir.clone()),
        uid: overrides.uid.or(img_uid),
        gid: overrides.gid.or(img_gid),
        hostname: overrides.hostname.clone(),
    })
}
