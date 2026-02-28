use std::net::Ipv4Addr;
use std::path::PathBuf;

use anyhow::Context;
use clap::{Parser, Subcommand};

use distvirt_worker::image_provider::containerd_overlayfs::ContainerdOverlayfsProvider;
use distvirt_worker_protocol::{
    ContainerConfig, ContainerSpec, NetworkConfig, OrchestratorConnection, PodNetworkConfig,
    WorkerCommand, WorkerConnection, WorkerEvent,
};

#[derive(Parser)]
#[command(name = "distvirt", about = "Lightweight VM-based container runner")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Run a Docker Compose deployment
    ComposeUp {
        /// Path to the compose file (e.g. docker-compose.yml)
        #[arg(short, long, default_value = "docker-compose.yml")]
        file: PathBuf,
        /// Path to the kernel (vmlinux)
        #[arg(long)]
        kernel: PathBuf,
        /// Path to the guest rootfs ext4 image
        #[arg(long)]
        rootfs_image: PathBuf,
        /// Path to the containerd socket
        #[arg(long, default_value = "/run/containerd/containerd.sock")]
        containerd_socket: PathBuf,
        /// Containerd namespace
        #[arg(long, default_value = "default")]
        namespace: String,
        /// Set up host NAT routing for guest internet access
        #[arg(long)]
        setup_routing: bool,
        /// Path to the firecracker binary
        #[arg(long, default_value = "firecracker")]
        firecracker: PathBuf,
    },
    /// Run an OCI image in a Firecracker VM
    RunImage {
        /// OCI image reference (e.g. alpine:latest)
        #[arg(long)]
        image: String,
        /// Path to the kernel (vmlinux)
        #[arg(long)]
        kernel: PathBuf,
        /// Path to the guest rootfs ext4 image
        #[arg(long)]
        rootfs_image: PathBuf,
        /// Override entrypoint
        #[arg(long)]
        entrypoint: Option<String>,
        /// Arguments to the entrypoint
        #[arg(long, allow_hyphen_values = true)]
        args: Vec<String>,
        /// Environment variables (KEY=VALUE)
        #[arg(short, long)]
        env: Vec<String>,
        /// Working directory inside the container
        #[arg(long)]
        workdir: Option<String>,
        /// User (UID or UID:GID)
        #[arg(long)]
        user: Option<String>,
        /// Hostname for the container
        #[arg(long)]
        hostname: Option<String>,
        /// Path to the containerd socket
        #[arg(long, default_value = "/run/containerd/containerd.sock")]
        containerd_socket: PathBuf,
        /// Containerd namespace
        #[arg(long, default_value = "default")]
        namespace: String,
        /// Set up host NAT routing (ip_forward + iptables masquerade) for guest internet access
        #[arg(long)]
        setup_routing: bool,
        /// Path to the firecracker binary
        #[arg(long, default_value = "firecracker")]
        firecracker: PathBuf,
    },
}

/// Set up host NAT so the 172.16.0.0/24 subnet can reach the internet.
fn setup_host_routing() -> anyhow::Result<()> {
    use std::process::Command;

    log::info!("setting up host NAT routing for 172.16.0.0/24");

    std::fs::write("/proc/sys/net/ipv4/ip_forward", "1")
        .context("enable ip_forward (are you root?)")?;

    let check = Command::new("iptables")
        .args(["-t", "nat", "-C", "POSTROUTING", "-s", "172.16.0.0/24", "!", "-o", "tun+", "-j", "MASQUERADE"])
        .output()
        .context("run iptables -C")?;

    if !check.status.success() {
        let add = Command::new("iptables")
            .args(["-t", "nat", "-A", "POSTROUTING", "-s", "172.16.0.0/24", "!", "-o", "tun+", "-j", "MASQUERADE"])
            .output()
            .context("run iptables -A")?;

        if !add.status.success() {
            anyhow::bail!(
                "iptables -A MASQUERADE failed: {}",
                String::from_utf8_lossy(&add.stderr)
            );
        }
        log::info!("added iptables MASQUERADE rule for 172.16.0.0/24");
    } else {
        log::info!("iptables MASQUERADE rule already exists");
    }

    Ok(())
}

/// Parse a "UID" or "UID:GID" string into optional numeric uid/gid.
fn parse_user_numeric(user: &str) -> anyhow::Result<(Option<u32>, Option<u32>)> {
    if user.is_empty() {
        return Ok((None, None));
    }
    if let Some((uid_str, gid_str)) = user.split_once(':') {
        let uid: u32 = uid_str
            .parse()
            .with_context(|| format!("non-numeric uid: {}", uid_str))?;
        let gid: u32 = gid_str
            .parse()
            .with_context(|| format!("non-numeric gid: {}", gid_str))?;
        Ok((Some(uid), Some(gid)))
    } else {
        let uid: u32 = user
            .parse()
            .with_context(|| format!("non-numeric user: {}", user))?;
        Ok((Some(uid), None))
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    env_logger::Builder::new()
        .filter_level(log::LevelFilter::Info)
        .parse_default_env()
        .init();

    let cli = Cli::parse();

    match cli.command {
        Commands::ComposeUp {
            file,
            kernel,
            rootfs_image,
            containerd_socket,
            namespace,
            setup_routing,
            firecracker,
        } => {
            if setup_routing {
                setup_host_routing().context("setup host routing")?;
            }

            let deployment = distvirt_compose::parse(&file)
                .with_context(|| format!("parsing compose file '{}'", file.display()))?;

            let vmm = distvirt_worker::vmm::firecracker::Firecracker::new(firecracker);
            let image_provider = ContainerdOverlayfsProvider {
                socket: containerd_socket.to_str().unwrap().to_string(),
                namespace,
            };

            // Create a duplex transport for local in-process communication.
            let (orch_half, worker_half) = tokio::io::duplex(64 * 1024);

            // Spawn the worker on one half.
            let worker_task = tokio::spawn(async move {
                let conn = WorkerConnection::accept(worker_half).await?;
                let worker = distvirt_worker::worker::Worker::new(
                    kernel,
                    rootfs_image,
                    vmm,
                    image_provider,
                    None,
                );
                worker.run(conn).await
            });

            // Orchestrator on the other half.
            let mut conn = OrchestratorConnection::connect(orch_half)
                .await
                .context("connect orchestrator")?;

            let result = distvirt_compose::orchestrate::run_compose(&deployment, &mut conn).await;

            // The worker task will end when the connection drops.
            drop(conn);
            let _ = worker_task.await;

            result.context("compose up")?;
        }
        Commands::RunImage {
            image,
            kernel,
            rootfs_image,
            entrypoint,
            args,
            env,
            workdir,
            user,
            hostname,
            containerd_socket,
            namespace,
            setup_routing,
            firecracker,
        } => {
            if setup_routing {
                setup_host_routing().context("setup host routing")?;
            }

            let (uid, gid) = if let Some(ref u) = user {
                parse_user_numeric(u).context("parsing --user")?
            } else {
                (None, None)
            };

            let vmm = distvirt_worker::vmm::firecracker::Firecracker::new(firecracker);
            let image_provider = ContainerdOverlayfsProvider {
                socket: containerd_socket.to_str().unwrap().to_string(),
                namespace,
            };

            // Create a duplex transport for local in-process communication.
            let (orch_half, worker_half) = tokio::io::duplex(64 * 1024);

            // Spawn the worker on one half.
            let worker_task = tokio::spawn(async move {
                let conn = WorkerConnection::accept(worker_half).await?;
                let worker = distvirt_worker::worker::Worker::new(
                    kernel,
                    rootfs_image,
                    vmm,
                    image_provider,
                    None,
                );
                worker.run(conn).await
            });

            // Orchestrator on the other half.
            let mut conn = OrchestratorConnection::connect(orch_half)
                .await
                .context("connect orchestrator")?;

            let namespace_id = "default".to_string();
            let pod_id = "run".to_string();

            // 1. Create namespace.
            conn.send_command(&WorkerCommand::CreateNamespace {
                namespace_id: namespace_id.clone(),
                network: NetworkConfig {
                    subnet: Ipv4Addr::new(172, 16, 0, 0),
                    gateway: Ipv4Addr::new(172, 16, 0, 1),
                    prefix_len: 24,
                },
            })
            .await
            .context("send create namespace")?;

            // Wait for NamespaceCreated.
            let event = conn.recv_event().await.context("recv namespace created")?;
            match event {
                WorkerEvent::NamespaceCreated { .. } => {
                    log::info!("namespace created");
                }
                other => {
                    anyhow::bail!("expected NamespaceCreated, got {:?}", other);
                }
            }

            // 2. Launch pod with a single container.
            let container_config = ContainerConfig {
                entrypoint: entrypoint.unwrap_or_default(),
                args,
                env,
                working_dir: workdir,
                uid,
                gid,
                hostname,
                capture_output: false,
            };

            let container_spec = ContainerSpec {
                container_id: "main".to_string(),
                image_ref: image,
                config: container_config,
            };

            conn.send_command(&WorkerCommand::LaunchPod {
                namespace_id: namespace_id.clone(),
                pod_id: pod_id.clone(),
                network: PodNetworkConfig {
                    ip: Ipv4Addr::new(172, 16, 0, 2),
                    mac: [0x02, 0x00, 0xAC, 0x10, 0x00, 0x02],
                    gateway: Ipv4Addr::new(172, 16, 0, 1),
                    netmask: "255.255.255.0".to_string(),
                },
                containers: vec![container_spec],
            })
            .await
            .context("send launch pod")?;

            // 3. Wait for pod exit.
            let exit_code = loop {
                let event = conn.recv_event().await.context("recv event")?;
                match event {
                    WorkerEvent::PodRunning { .. } => {
                        log::info!("pod is running");
                    }
                    WorkerEvent::PodExited { exit_code, .. } => {
                        log::info!("pod exited with code {}", exit_code);
                        break exit_code;
                    }
                    WorkerEvent::PodFailed { error, .. } => {
                        anyhow::bail!("pod failed: {}", error);
                    }
                    WorkerEvent::NamespaceFailed { error, .. } => {
                        anyhow::bail!("namespace failed: {}", error);
                    }
                    other => {
                        log::debug!("ignoring event: {:?}", other);
                    }
                }
            };

            // 4. Clean up: destroy namespace.
            conn.send_command(&WorkerCommand::DestroyNamespace {
                namespace_id: namespace_id.clone(),
            })
            .await
            .context("send destroy namespace")?;

            // Drop connection so worker task ends.
            drop(conn);
            let _ = worker_task.await;

            std::process::exit(exit_code);
        }
    }

    Ok(())
}
