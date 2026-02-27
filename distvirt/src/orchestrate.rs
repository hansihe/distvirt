use std::path::Path;

use anyhow::{bail, Context};

use distvirt_guest_protocol::{GuestMessage, HostMessage, VSOCK_PORT};

use crate::image;
use crate::vmm::{VmConfig, VmInstance, Vmm};
use crate::vsock_client::GuestConnection;

/// Run a container end-to-end: build image, launch VM, execute container, shut down.
pub fn run_container(
    vmm: &impl Vmm,
    kernel_path: &Path,
    rootfs_image_path: &Path,
    container_rootfs: &Path,
    entrypoint: &str,
    args: &[String],
) -> anyhow::Result<i32> {
    // Build ext4 image from container rootfs.
    let container_image = tempfile::NamedTempFile::new().context("create temp file")?;
    let container_image_path = container_image.path().to_path_buf();
    image::build_ext4_image(container_rootfs, &container_image_path)
        .context("build container image")?;

    log::info!("built container image at {}", container_image_path.display());

    // Launch VM.
    let config = VmConfig {
        kernel_path: kernel_path.to_path_buf(),
        rootfs_image_path: rootfs_image_path.to_path_buf(),
        container_image_path,
        vcpu_count: 1,
        mem_size_mib: 128,
    };

    let mut instance = vmm.launch(&config).context("launch VM")?;
    log::info!("VM launched, connecting vsock");

    // Connect to guest over vsock.
    let stream = instance
        .connect_vsock(VSOCK_PORT)
        .context("connect vsock")?;
    let mut conn = GuestConnection::new(stream);

    // Wait for Ready.
    let msg: GuestMessage = conn.recv().context("receive Ready")?;
    match msg {
        GuestMessage::Ready => log::info!("guest is ready"),
        other => bail!("expected Ready, got {:?}", other),
    }

    // Add container (second virtio block device = /dev/vdb).
    let container_id = "default".to_string();
    conn.send(&HostMessage::AddContainer {
        id: container_id.clone(),
        device: "/dev/vdb".to_string(),
    })
    .context("send AddContainer")?;

    let msg: GuestMessage = conn.recv().context("receive ContainerAdded")?;
    match msg {
        GuestMessage::ContainerAdded { id } => log::info!("container added: {}", id),
        GuestMessage::Error { message } => bail!("AddContainer failed: {}", message),
        other => bail!("expected ContainerAdded, got {:?}", other),
    }

    // Start container.
    conn.send(&HostMessage::StartContainer {
        id: container_id.clone(),
        entrypoint: entrypoint.to_string(),
        args: args.to_vec(),
    })
    .context("send StartContainer")?;

    let msg: GuestMessage = conn.recv().context("receive ContainerStarted")?;
    match msg {
        GuestMessage::ContainerStarted { id, pid } => {
            log::info!("container {} started with pid {}", id, pid)
        }
        GuestMessage::Error { message } => bail!("StartContainer failed: {}", message),
        other => bail!("expected ContainerStarted, got {:?}", other),
    }

    // Wait for container to exit.
    let msg: GuestMessage = conn.recv().context("receive ContainerExited")?;
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
        .context("send Shutdown")?;

    // Wait for the VM to exit.
    instance.wait().context("wait for VM")?;

    Ok(exit_code)
}
