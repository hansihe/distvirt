use std::path::PathBuf;

use clap::Parser;
use distvirt_worker_protocol::WorkerConnection;

#[derive(Parser)]
#[command(name = "distvirt-worker")]
struct Cli {
    /// Path to the guest kernel (vmlinux)
    #[arg(long)]
    kernel: PathBuf,

    /// Path to the guest rootfs ext4 image
    #[arg(long)]
    rootfs_image: PathBuf,

    /// Path to containerd socket
    #[arg(long, default_value = "/run/containerd/containerd.sock")]
    containerd_socket: String,

    /// Containerd namespace
    #[arg(long, default_value = "distvirt")]
    containerd_namespace: String,

    /// Orchestrator address (host:port) to connect to via TCP
    #[arg(long)]
    orchestrator: String,

    /// Optional directory containing WASM activator components
    #[arg(long)]
    component_dir: Option<PathBuf>,

    /// Public IP/hostname where this worker is reachable by other workers
    #[arg(long, default_value = "")]
    public_endpoint: String,

    /// Shared secret for authenticating with the orchestrator
    #[arg(long)]
    worker_secret: String,

    /// Path to Docker config.json for registry auth
    #[arg(long, default_value = "/root/.docker/config.json")]
    docker_config: PathBuf,

    /// VMM backend to use (firecracker or cloud-hypervisor)
    #[arg(long, default_value = "cloud-hypervisor")]
    vmm: String,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    env_logger::init();

    let cli = Cli::parse();

    let image_provider =
        distvirt_worker::image_provider::containerd_overlayfs::ContainerdOverlayfsProvider::new(
            &cli.containerd_socket,
            &cli.containerd_namespace,
            Some(cli.docker_config),
        )
        .await?;

    // Share the containerd connection with the VMM for unpack/view operations.
    let containerd_config = distvirt_worker::vmm::cloud_hypervisor::ContainerdConfig {
        channel: image_provider.channel().clone(),
        namespace: image_provider.namespace().to_string(),
        unpack_coordinator:
            distvirt_worker::image_provider::UnpackCoordinator::default(),
    };

    log::info!("connecting to orchestrator at {}", cli.orchestrator);
    let stream = tokio::net::TcpStream::connect(&cli.orchestrator).await?;
    let conn = WorkerConnection::accept(stream).await?;

    let activity = std::sync::Arc::new(distvirt_common::ActivityTracker::new());

    macro_rules! run_worker {
        ($vmm:expr) => {{
            let worker = distvirt_worker::worker::Worker::<
                _,
                _,
                _,
                distvirt_worker::TokioFs,
                distvirt_worker::HostResourceMonitor,
            >::new(
                cli.kernel,
                cli.rootfs_image,
                $vmm,
                image_provider,
                cli.component_dir,
                cli.public_endpoint,
                distvirt_worker::TunGatewayProvider,
                activity,
            );
            worker.run(conn, cli.worker_secret).await
        }};
    }

    match cli.vmm.as_str() {
        "cloud-hypervisor" => {
            log::info!("using Cloud Hypervisor VMM backend");
            let vmm = distvirt_worker::vmm::cloud_hypervisor::CloudHypervisor::new(
                "cloud-hypervisor",
                "virtiofsd",
                Some(containerd_config),
            );
            run_worker!(vmm)
        }
        other => anyhow::bail!("unknown VMM backend: {}", other),
    }
}
