use std::path::PathBuf;

use anyhow::Context;
use clap::{Parser, Subcommand};

use distvirt_worker::image_provider::containerd_overlayfs::ContainerdOverlayfsProvider;
use distvirt_worker::image_provider::rootfs_dir::RootfsDirProvider;
use distvirt_worker::managed_vm::ImageOverrides;
use distvirt_worker_protocol::{OrchestratorConnection, WorkerConnection};

#[derive(Parser)]
#[command(name = "distvirt", about = "Lightweight VM-based container runner")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Build an ext4 image from a rootfs directory
    BuildImage {
        /// Path to the rootfs directory
        #[arg(long)]
        rootfs: PathBuf,
        /// Output path for the ext4 image
        #[arg(long)]
        output: PathBuf,
    },
    /// Run a container in a Firecracker VM
    Run {
        /// Path to the kernel (vmlinux)
        #[arg(long)]
        kernel: PathBuf,
        /// Path to the guest rootfs ext4 image
        #[arg(long)]
        rootfs_image: PathBuf,
        /// Path to the container rootfs directory
        #[arg(long)]
        container_rootfs: PathBuf,
        /// Entrypoint command
        #[arg(long)]
        entrypoint: String,
        /// Arguments to the entrypoint
        #[arg(long, allow_hyphen_values = true)]
        args: Vec<String>,
        /// Environment variables (KEY=VALUE)
        #[arg(short, long)]
        env: Vec<String>,
        /// Working directory inside the container
        #[arg(long)]
        workdir: Option<String>,
        /// User ID to run as
        #[arg(long)]
        uid: Option<u32>,
        /// Group ID to run as
        #[arg(long)]
        gid: Option<u32>,
        /// Hostname for the container
        #[arg(long)]
        hostname: Option<String>,
        /// Path to the firecracker binary
        #[arg(long, default_value = "firecracker")]
        firecracker: PathBuf,
    },
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

/// Run a single container using worker crate primitives directly (no protocol overhead).
async fn run_single_container(
    vmm: &impl distvirt_worker::vmm::Vmm,
    kernel_path: &std::path::Path,
    rootfs_image_path: &std::path::Path,
    provider: &impl distvirt_worker::image_provider::ImageProvider,
    image_ref: &str,
    overrides: &ImageOverrides,
) -> anyhow::Result<i32> {
    use distvirt_worker::fabric::{self, FabricGateway};
    use distvirt_worker::managed_vm::{config_from_overrides, merge_config, ManagedVm};
    use distvirt_worker::vmm::{NetConfig, VmConfig, VmInstance};

    let artifact = provider.prepare(image_ref).await.context("preparing image")?;

    let config = if let Some(ref oci_config) = artifact.oci_config {
        merge_config(oci_config, overrides)?
    } else {
        config_from_overrides(overrides)?
    };

    let vm_config = VmConfig {
        kernel_path: kernel_path.to_path_buf(),
        rootfs_image_path: rootfs_image_path.to_path_buf(),
        container_image_path: artifact.image_path.clone(),
        vcpu_count: 1,
        mem_size_mib: 128,
        net: Some(NetConfig {
            guest_ip: "172.16.0.2".to_string(),
            netmask: "255.255.255.0".to_string(),
            gateway: "172.16.0.1".to_string(),
        }),
        serial_console: true,
    };

    let mut instance = vmm.launch(&vm_config).await.context("launch VM")?;
    log::info!("VM launched");

    let _fabric = if let Some(tap) = instance.take_tap() {
        let mut fab = fabric::Fabric::new();

        let registry = std::sync::Arc::new(std::sync::RwLock::new(
            std::collections::HashMap::<String, std::net::Ipv4Addr>::new(),
        ));
        let (gateway, egress_tx, ingress_rx) =
            FabricGateway::new(registry).context("create fabric gateway")?;
        fab.set_gateway(egress_tx, ingress_rx);
        tokio::spawn(gateway.run());

        let tap_name = tap.name.clone();
        fab.add_port(tap)
            .map_err(|e| anyhow::anyhow!("fabric add_port for {}: {}", tap_name, e))?;

        log::info!("fabric: started L2 switch with gateway on {}", tap_name);
        Some(fab)
    } else {
        None
    };

    let mut vm = ManagedVm::connect(instance).await?;

    if let Some(ref net_config) = vm_config.net {
        vm.configure_network("eth0", net_config).await?;
    }

    let container_id = "default";
    vm.add_container(container_id, "/dev/vdb", &["172.16.0.1".to_string()])
        .await?;

    vm.start_container(container_id, &config).await?;

    let (_id, exit_code) = vm.wait_container_exit().await?;

    vm.shutdown().await?;

    Ok(exit_code)
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
                );
                worker.run(conn).await
            });

            // Orchestrator on the other half.
            let mut conn = OrchestratorConnection::connect(orch_half)
                .await
                .context("connect orchestrator")?;

            let result = distvirt::orchestrate_compose::run_compose(&deployment, &mut conn).await;

            // The worker task will end when the connection drops.
            drop(conn);
            let _ = worker_task.await;

            result.context("compose up")?;
        }
        Commands::BuildImage { rootfs, output } => {
            distvirt_worker::image::build_ext4_image(&rootfs, &output)
                .context("build ext4 image")?;
            log::info!("image written to {}", output.display());
        }
        Commands::Run {
            kernel,
            rootfs_image,
            container_rootfs,
            entrypoint,
            args,
            env,
            workdir,
            uid,
            gid,
            hostname,
            firecracker,
        } => {
            let vmm = distvirt_worker::vmm::firecracker::Firecracker::new(firecracker);
            let provider = RootfsDirProvider;
            let overrides = ImageOverrides {
                entrypoint: Some(entrypoint),
                args,
                env,
                working_dir: workdir,
                uid,
                gid,
                hostname,
            };
            let exit_code = run_single_container(
                &vmm,
                &kernel,
                &rootfs_image,
                &provider,
                container_rootfs.to_str().unwrap(),
                &overrides,
            )
            .await
            .context("run container")?;

            log::info!("container exited with code {}", exit_code);
            std::process::exit(exit_code);
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
                distvirt_worker::containerd::parse_user_numeric(u)
                    .context("parsing --user")?
            } else {
                (None, None)
            };

            let vmm = distvirt_worker::vmm::firecracker::Firecracker::new(firecracker);
            let provider = ContainerdOverlayfsProvider {
                socket: containerd_socket.to_str().unwrap().to_string(),
                namespace,
            };
            let overrides = ImageOverrides {
                entrypoint,
                args,
                env,
                working_dir: workdir,
                uid,
                gid,
                hostname,
            };
            let exit_code = run_single_container(
                &vmm,
                &kernel,
                &rootfs_image,
                &provider,
                &image,
                &overrides,
            )
            .await
            .context("run image")?;

            log::info!("container exited with code {}", exit_code);
            std::process::exit(exit_code);
        }
    }

    Ok(())
}
