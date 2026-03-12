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
    #[arg(long, default_value = "default")]
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
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    env_logger::init();

    let cli = Cli::parse();

    let vmm = distvirt_worker::vmm::firecracker::Firecracker::new("firecracker");
    let image_provider =
        distvirt_worker::image_provider::containerd_overlayfs::ContainerdOverlayfsProvider {
            socket: cli.containerd_socket,
            namespace: cli.containerd_namespace,
        };

    log::info!("connecting to orchestrator at {}", cli.orchestrator);
    let stream = tokio::net::TcpStream::connect(&cli.orchestrator).await?;
    let conn = WorkerConnection::accept(stream).await?;

    let worker = distvirt_worker::worker::Worker::<_, _, _, distvirt_worker::TokioFs, distvirt_worker::HostResourceMonitor>::new(
        cli.kernel,
        cli.rootfs_image,
        vmm,
        image_provider,
        cli.component_dir,
        cli.public_endpoint,
        distvirt_worker::TunGatewayProvider,
    );

    worker.run(conn).await
}
