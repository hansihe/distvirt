use std::path::PathBuf;

use anyhow::Context;
use clap::{Parser, Subcommand};

use distvirt::orchestrate::ImageOverrides;
use distvirt::image_provider::containerd_overlayfs::ContainerdOverlayfsProvider;
use distvirt::image_provider::rootfs_dir::RootfsDirProvider;

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
        /// Path to the firecracker binary
        #[arg(long, default_value = "firecracker")]
        firecracker: PathBuf,
    },
}

fn main() -> anyhow::Result<()> {
    env_logger::Builder::new()
        .filter_level(log::LevelFilter::Info)
        .parse_default_env()
        .init();

    let cli = Cli::parse();

    match cli.command {
        Commands::BuildImage { rootfs, output } => {
            distvirt::image::build_ext4_image(&rootfs, &output)
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
            let vmm = distvirt::vmm::firecracker::Firecracker::new(firecracker);
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
            let exit_code = distvirt::orchestrate::run(
                &vmm,
                &kernel,
                &rootfs_image,
                &provider,
                container_rootfs.to_str().unwrap(),
                &overrides,
            )
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
            firecracker,
        } => {
            let (uid, gid) = if let Some(ref u) = user {
                distvirt::containerd::parse_user_numeric(u)
                    .context("parsing --user")?
            } else {
                (None, None)
            };

            let vmm = distvirt::vmm::firecracker::Firecracker::new(firecracker);
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
            let exit_code = distvirt::orchestrate::run(
                &vmm,
                &kernel,
                &rootfs_image,
                &provider,
                &image,
                &overrides,
            )
            .context("run image")?;

            log::info!("container exited with code {}", exit_code);
            std::process::exit(exit_code);
        }
    }

    Ok(())
}
