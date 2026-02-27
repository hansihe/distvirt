use std::path::Path;

use anyhow::{bail, Context};

use distvirt_guest_protocol::{GuestMessage, HostMessage, IoMode, VSOCK_CONTROL_PORT, VSOCK_IO_PORT};

use crate::containerd::{parse_user_numeric, ImageConfig};
use crate::image_provider::ImageProvider;
use crate::io_session::IoSession;
use crate::vmm::{NetConfig, VmConfig, VmInstance, Vmm};
use crate::vsock_client::GuestConnection;

/// Container execution configuration.
pub struct ContainerConfig {
    pub entrypoint: String,
    pub args: Vec<String>,
    pub env: Vec<String>,
    pub working_dir: Option<String>,
    pub uid: Option<u32>,
    pub gid: Option<u32>,
    pub hostname: Option<String>,
    pub capture_output: bool,
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

/// A launched VM with an established vsock connection.
///
/// Encapsulates the single-VM lifecycle: launch, protocol handshake,
/// network configuration, container management, and shutdown.
pub struct ManagedVm<I> {
    instance: I,
    conn: GuestConnection,
}

impl<I: VmInstance> ManagedVm<I> {
    /// Create a ManagedVm from an already-launched VM instance.
    ///
    /// Connects to the guest over vsock and waits for the Ready message.
    /// The caller should take the TAP device (via `instance.take_tap()`)
    /// before calling this if fabric networking is needed, since the fabric
    /// must be running before the guest finishes booting.
    pub async fn connect(instance: I) -> anyhow::Result<Self> {
        log::info!("connecting vsock");
        let stream = instance
            .connect_vsock(VSOCK_CONTROL_PORT)
            .await
            .context("connect vsock")?;
        let mut conn = GuestConnection::new(stream);

        let msg: GuestMessage = conn.recv().await.context("receive Ready")?;
        match msg {
            GuestMessage::Ready => log::info!("guest is ready"),
            other => bail!("expected Ready, got {:?}", other),
        }

        Ok(ManagedVm { instance, conn })
    }

    /// Configure the guest's network interface.
    pub async fn configure_network(
        &mut self,
        interface: &str,
        net_config: &NetConfig,
    ) -> anyhow::Result<()> {
        self.conn
            .send(&HostMessage::ConfigureNetwork {
                interface: interface.to_string(),
                ip: net_config.guest_ip.clone(),
                netmask: net_config.netmask.clone(),
                gateway: net_config.gateway.clone(),
            })
            .await
            .context("send ConfigureNetwork")?;

        let msg: GuestMessage = self
            .conn
            .recv()
            .await
            .context("receive NetworkConfigured")?;
        match msg {
            GuestMessage::NetworkConfigured => log::info!("guest network configured"),
            GuestMessage::Error { message } => bail!("ConfigureNetwork failed: {}", message),
            other => bail!("expected NetworkConfigured, got {:?}", other),
        }

        Ok(())
    }

    /// Add a container filesystem to the guest.
    pub async fn add_container(
        &mut self,
        id: &str,
        device: &str,
        dns_servers: &[String],
    ) -> anyhow::Result<()> {
        self.conn
            .send(&HostMessage::AddContainer {
                id: id.to_string(),
                device: device.to_string(),
                dns_servers: dns_servers.to_vec(),
            })
            .await
            .context("send AddContainer")?;

        let msg: GuestMessage = self.conn.recv().await.context("receive ContainerAdded")?;
        match msg {
            GuestMessage::ContainerAdded { id } => log::info!("container added: {}", id),
            GuestMessage::Error { message } => bail!("AddContainer failed: {}", message),
            other => bail!("expected ContainerAdded, got {:?}", other),
        }

        Ok(())
    }

    /// Start a container process inside the guest.
    ///
    /// Returns the PID of the container process inside the guest.
    pub async fn start_container(
        &mut self,
        id: &str,
        config: &ContainerConfig,
    ) -> anyhow::Result<u32> {
        self.conn
            .send(&HostMessage::StartContainer {
                id: id.to_string(),
                entrypoint: config.entrypoint.clone(),
                args: config.args.clone(),
                env: config.env.clone(),
                working_dir: config.working_dir.clone(),
                uid: config.uid,
                gid: config.gid,
                hostname: config.hostname.clone(),
                capture_output: config.capture_output,
            })
            .await
            .context("send StartContainer")?;

        let msg: GuestMessage = self
            .conn
            .recv()
            .await
            .context("receive ContainerStarted")?;
        match msg {
            GuestMessage::ContainerStarted { id, pid } => {
                log::info!("container {} started with pid {}", id, pid);
                Ok(pid)
            }
            GuestMessage::Error { message } => bail!("StartContainer failed: {}", message),
            other => bail!("expected ContainerStarted, got {:?}", other),
        }
    }

    /// Wait for a container to exit.
    ///
    /// Returns the (container_id, exit_code).
    pub async fn wait_container_exit(&mut self) -> anyhow::Result<(String, i32)> {
        let msg: GuestMessage = self
            .conn
            .recv()
            .await
            .context("receive ContainerExited")?;
        match msg {
            GuestMessage::ContainerExited { id, code } => {
                log::info!("container {} exited with code {}", id, code);
                Ok((id, code))
            }
            GuestMessage::Error { message } => bail!("container error: {}", message),
            other => bail!("expected ContainerExited, got {:?}", other),
        }
    }

    /// Open a log streaming session for a container.
    ///
    /// Connects to the guest's I/O port and returns an IoSession that
    /// can be polled for stdout/stderr events.
    pub async fn stream_logs(&self, container_id: &str) -> anyhow::Result<IoSession> {
        let stream = self
            .instance
            .connect_vsock(VSOCK_IO_PORT)
            .await
            .context("connect vsock I/O port")?;
        IoSession::connect(stream, container_id, IoMode::Logs)
            .await
            .context("I/O session handshake")
    }

    /// Shut down the guest and wait for the VM to exit.
    pub async fn shutdown(mut self) -> anyhow::Result<()> {
        self.conn
            .send(&HostMessage::Shutdown)
            .await
            .context("send Shutdown")?;
        self.instance.wait().await.context("wait for VM")?;
        Ok(())
    }
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

    // Connect to guest and wait for ready.
    let mut vm = ManagedVm::connect(instance).await?;

    // Configure network if enabled.
    if let Some(ref net_config) = vm_config.net {
        vm.configure_network("eth0", net_config).await?;
    }

    // Add and start container.
    let container_id = "default";
    vm.add_container(container_id, "/dev/vdb", &["172.16.0.1".to_string()])
        .await?;

    vm.start_container(container_id, config).await?;

    // Wait for container to exit.
    let (_id, exit_code) = vm.wait_container_exit().await?;

    // Shut down the guest.
    vm.shutdown().await?;

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
        capture_output: false,
    })
}

/// Merge image config with CLI overrides following OCI entrypoint/cmd resolution rules.
pub fn merge_config(
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
        capture_output: false,
    })
}
