use anyhow::{bail, Context};

use distvirt_guest_protocol::{GuestMessage, HostMessage, VSOCK_CONTROL_PORT};
use distvirt_worker_protocol::ContainerConfig;

use crate::image_provider::containerd::{parse_user_numeric, ImageConfig};
use crate::io_session::IoSession;
use crate::vmm::{NetConfig, VmInstance};
use crate::task_handle::TaskHandle;
use crate::vsock_client::GuestSession;

/// Overrides that can be specified on the CLI to override image config.
pub(crate) struct ImageOverrides {
    pub entrypoint: Option<String>,
    pub args: Vec<String>,
    pub env: Vec<String>,
    pub working_dir: Option<String>,
    pub uid: Option<u32>,
    pub gid: Option<u32>,
    pub hostname: Option<String>,
}

/// A launched VM with an established yamux session.
pub(crate) struct ManagedVm<I> {
    instance: I,
    session: GuestSession,
}

impl<I: VmInstance> ManagedVm<I> {
    /// Create a ManagedVm from an already-launched VM instance.
    ///
    /// Connects to the guest over vsock, establishes a yamux session,
    /// and waits for the Ready message.
    pub async fn connect(instance: I) -> anyhow::Result<(Self, TaskHandle<anyhow::Result<()>>)> {
        log::info!("connecting vsock");
        let stream = instance
            .connect_vsock(VSOCK_CONTROL_PORT)
            .await
            .context("connect vsock")?;

        let (mut session, yamux_driver) = GuestSession::new(stream)
            .await
            .context("establish yamux session")?;

        let msg: GuestMessage = session.recv().await.context("receive Ready")?;
        match msg {
            GuestMessage::Ready => log::info!("guest is ready"),
            other => bail!("expected Ready, got {:?}", other),
        }

        Ok((ManagedVm { instance, session }, yamux_driver))
    }

    /// Configure the guest's network interface.
    pub async fn configure_network(
        &mut self,
        interface: &str,
        net_config: &NetConfig,
    ) -> anyhow::Result<()> {
        self.session
            .send(&HostMessage::ConfigureNetwork {
                interface: interface.to_string(),
                ip: net_config.guest_ip.clone(),
                netmask: net_config.netmask.clone(),
                gateway: net_config.gateway.clone(),
            })
            .await
            .context("send ConfigureNetwork")?;

        let msg: GuestMessage = self
            .session
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
        self.session
            .send(&HostMessage::AddContainer {
                id: id.to_string(),
                device: device.to_string(),
                dns_servers: dns_servers.to_vec(),
            })
            .await
            .context("send AddContainer")?;

        let msg: GuestMessage = self.session.recv().await.context("receive ContainerAdded")?;
        match msg {
            GuestMessage::ContainerAdded { id } => log::info!("container added: {}", id),
            GuestMessage::Error { message } => bail!("AddContainer failed: {}", message),
            other => bail!("expected ContainerAdded, got {:?}", other),
        }

        Ok(())
    }

    /// Start a container process inside the guest.
    pub async fn start_container(
        &mut self,
        id: &str,
        config: &ContainerConfig,
    ) -> anyhow::Result<u32> {
        self.session
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
            .session
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
    pub async fn wait_container_exit(&mut self) -> anyhow::Result<(String, i32)> {
        let msg: GuestMessage = self
            .session
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

    /// Accept the next output stream opened by the guest.
    pub async fn accept_output_stream(&mut self) -> anyhow::Result<(String, IoSession)> {
        let (container_id, stream) = self
            .session
            .accept_output_stream()
            .await
            .context("accept output stream")?;
        Ok((container_id, IoSession::new(stream)))
    }

    /// Shut down the guest and wait for the VM to exit.
    pub async fn shutdown(&mut self) -> anyhow::Result<()> {
        self.session
            .send(&HostMessage::Shutdown)
            .await
            .context("send Shutdown")?;
        self.instance.wait().await.context("wait for VM")?;
        Ok(())
    }

    /// Forcibly kill the VM process.
    pub async fn force_kill(&mut self) -> anyhow::Result<()> {
        self.instance.kill().await.context("kill VM")
    }
}

/// Merge image config with CLI overrides following OCI entrypoint/cmd resolution rules.
pub(crate) fn merge_config(
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
    } else if !overrides.args.is_empty() {
        // No entrypoint override and no image entrypoint, but override args provided.
        // Treat override args as the full command (compose `command:` replaces CMD).
        (overrides.args[0].clone(), overrides.args[1..].to_vec())
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
