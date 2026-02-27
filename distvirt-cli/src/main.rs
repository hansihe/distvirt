use std::path::PathBuf;

use anyhow::Context;
use clap::{Parser, Subcommand};

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
            firecracker,
        } => {
            let vmm = distvirt::vmm::firecracker::Firecracker::new(firecracker);
            let exit_code = distvirt::orchestrate::run_container(
                &vmm,
                &kernel,
                &rootfs_image,
                &container_rootfs,
                &entrypoint,
                &args,
            )
            .context("run container")?;

            log::info!("container exited with code {}", exit_code);
            std::process::exit(exit_code);
        }
    }

    Ok(())
}
