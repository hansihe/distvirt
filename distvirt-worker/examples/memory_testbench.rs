//! Memory management testbench.
//!
//! Boots a Firecracker VM with balloon support, launches a container running
//! `test-containers mem-stress`, and provides a foundation for experimenting
//! with guest memory management (balloon inflation/deflation).
//!
//! Requires root (for /dev/kvm and TAP devices).
//!
//! ```bash
//! ./distvirt-worker/examples/run-memory-testbench.sh
//! ```

use std::path::PathBuf;
use std::time::Duration;

use anyhow::{Context, Result};
use clap::Parser;
use tokio_util::sync::CancellationToken;

use distvirt_worker::image_provider::containerd_overlayfs::ContainerdOverlayfsProvider;
use distvirt_worker::image_provider::ImageProvider;
use distvirt_worker::io_session::IoEvent;
use distvirt_worker::managed_vm::ManagedVm;
use distvirt_worker::vmm::firecracker::Firecracker;
use distvirt_worker::vmm::{BalloonConfig, VmConfig, Vmm};
use distvirt_worker_protocol::ContainerConfig;

#[derive(Parser)]
#[command(about = "Memory management testbench for guest balloon system")]
struct Args {
    /// Path to the vmlinux kernel image.
    #[arg(long)]
    kernel: Option<PathBuf>,

    /// Path to the rootfs ext4 image.
    #[arg(long)]
    rootfs: Option<PathBuf>,

    /// Path to the firecracker binary.
    #[arg(long, default_value = "firecracker")]
    firecracker_bin: String,

    /// Container image reference (e.g. docker.io/library/distvirt-test-containers:latest).
    #[arg(long)]
    container_image: String,

    /// Containerd socket path.
    #[arg(long, default_value = "/run/containerd/containerd.sock")]
    containerd_socket: String,

    /// Containerd namespace.
    #[arg(long, default_value = "default")]
    containerd_namespace: String,

    /// VM memory size in MiB.
    #[arg(long, default_value = "512")]
    mem_size_mib: u32,

    /// Initial balloon size in MiB (memory reclaimed from guest).
    #[arg(long, default_value = "256")]
    balloon_amount_mib: u32,

    /// Number of vCPUs.
    #[arg(long, default_value = "1")]
    vcpu_count: u32,

    /// Target MiB for mem-stress container allocation.
    #[arg(long, default_value = "200")]
    stress_target_mib: u64,

    /// Step MiB for mem-stress container allocation.
    #[arg(long, default_value = "32")]
    stress_step_mib: u64,

    /// Interval in ms between mem-stress allocation steps.
    #[arg(long, default_value = "1000")]
    stress_interval_ms: u64,

    /// Enable serial console output (kernel boot logs).
    #[arg(long)]
    serial_console: bool,
}

fn resolve_kernel(args: &Args) -> PathBuf {
    if let Some(ref p) = args.kernel {
        return p.clone();
    }
    let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    manifest_dir.join("../guest-image/result-kernel/vmlinux")
}

fn resolve_rootfs(args: &Args) -> PathBuf {
    if let Some(ref p) = args.rootfs {
        return p.clone();
    }
    let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    manifest_dir.join("../guest-image/result-rootfs")
}

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::init();
    let args = Args::parse();

    let kernel = resolve_kernel(&args);
    let rootfs = resolve_rootfs(&args);

    assert!(kernel.exists(), "kernel not found at {}", kernel.display());
    assert!(rootfs.exists(), "rootfs not found at {}", rootfs.display());

    let effective_guest_mem = args.mem_size_mib.saturating_sub(args.balloon_amount_mib);

    eprintln!("=== Memory Management Testbench ===");
    eprintln!();
    eprintln!("  VM config:");
    eprintln!("    kernel:          {}", kernel.display());
    eprintln!("    rootfs:          {}", rootfs.display());
    eprintln!("    firecracker:     {}", args.firecracker_bin);
    eprintln!("    vcpus:           {}", args.vcpu_count);
    eprintln!("    mem_size:        {} MiB", args.mem_size_mib);
    eprintln!("    balloon_init:    {} MiB (reclaimed from guest)", args.balloon_amount_mib);
    eprintln!("    effective guest:  ~{} MiB", effective_guest_mem);
    eprintln!("    deflate_on_oom:  true");
    eprintln!("    serial_console:  {}", args.serial_console);
    eprintln!();
    eprintln!("  Container config:");
    eprintln!("    image:           {}", args.container_image);
    eprintln!("    containerd:      {} (ns={})", args.containerd_socket, args.containerd_namespace);
    eprintln!("    entrypoint:      /bin/test-containers mem-stress");
    eprintln!("    stress target:   {} MiB", args.stress_target_mib);
    eprintln!("    stress step:     {} MiB", args.stress_step_mib);
    eprintln!("    stress interval: {} ms", args.stress_interval_ms);
    eprintln!();

    // Shutdown trigger: SIGINT/SIGTERM forwarded by the wrapper script.
    let shutdown = CancellationToken::new();
    {
        let shutdown = shutdown.clone();
        tokio::spawn(async move {
            use tokio::signal::unix::{signal, SignalKind};
            let mut sigint = signal(SignalKind::interrupt()).expect("register SIGINT handler");
            let mut sigterm = signal(SignalKind::terminate()).expect("register SIGTERM handler");
            tokio::select! {
                _ = sigint.recv() => eprintln!("\n  [testbench] received SIGINT"),
                _ = sigterm.recv() => eprintln!("\n  [testbench] received SIGTERM"),
            }
            shutdown.cancel();
        });
    }

    // Prepare container image via containerd.
    eprintln!("[1/5] Preparing container image...");
    let image_provider = ContainerdOverlayfsProvider {
        socket: args.containerd_socket.clone(),
        namespace: args.containerd_namespace.clone(),
    };
    let artifact = image_provider
        .prepare(&args.container_image)
        .await
        .context("prepare container image")?;
    eprintln!("       -> image ready at {}", artifact.image_path.display());

    // Launch VM.
    eprintln!("[2/5] Launching VM...");
    let vmm = Firecracker::new(&args.firecracker_bin);
    let config = VmConfig {
        kernel_path: kernel,
        rootfs_image_path: rootfs,
        container_image_path: artifact.image_path.clone(),
        vcpu_count: args.vcpu_count,
        mem_size_mib: args.mem_size_mib,
        net: None,
        serial_console: args.serial_console,
        balloon: Some(BalloonConfig {
            amount_mib: args.balloon_amount_mib,
            deflate_on_oom: true,
            stats_polling_interval_s: 1,
        }),
    };
    let instance = vmm.launch(&config).await.context("launch VM")?;
    eprintln!("       -> VM process launched");

    // Connect to guest.
    eprintln!("[3/5] Connecting to guest via vsock...");
    let mut vm = ManagedVm::connect(instance)
        .await
        .context("connect to guest")?;
    eprintln!("       -> guest ready, yamux session established");

    // Add and start container.
    eprintln!("[4/5] Starting container...");
    let container_id = "mem-stress-0";

    eprintln!("       -> adding container filesystem (device=/dev/vdb)...");
    vm.add_container(container_id, "/dev/vdb", &[])
        .await
        .context("add container")?;
    eprintln!("       -> container filesystem added");

    let container_config = ContainerConfig {
        entrypoint: vec![
            "/bin/test-containers".to_string(),
            "mem-stress".to_string(),
        ],
        args: vec![
            format!("--target-mib={}", args.stress_target_mib),
            format!("--step-mib={}", args.stress_step_mib),
            format!("--interval-ms={}", args.stress_interval_ms),
        ],
        env: vec![],
        working_dir: None,
        uid: None,
        gid: None,
        hostname: Some("mem-testbench".to_string()),
        capture_output: true,
        stdin: false,
    };

    let pid = vm
        .start_container(container_id, &container_config)
        .await
        .context("start container")?;
    eprintln!("       -> container started (guest pid={})", pid);

    // Accept output stream.
    let (output_container_id, mut io_session) = vm
        .accept_output_stream()
        .await
        .context("accept output stream")?;
    eprintln!("       -> output stream accepted (container={})", output_container_id);

    // Monitoring phase.
    eprintln!("[5/5] Monitoring (Ctrl+C to shut down)...");
    eprintln!();

    // Spawn output reader task.
    let _output_handle = tokio::spawn(async move {
        loop {
            match io_session.next_event().await {
                Ok(IoEvent::Stdout(data)) => {
                    let text = String::from_utf8_lossy(&data);
                    for line in text.lines() {
                        eprintln!("  [stdout] {}", line);
                    }
                }
                Ok(IoEvent::Stderr(data)) => {
                    let text = String::from_utf8_lossy(&data);
                    for line in text.lines() {
                        eprintln!("  [stderr] {}", line);
                    }
                }
                Ok(IoEvent::Eof) => {
                    eprintln!("  [output] EOF");
                    break;
                }
                Err(e) => {
                    eprintln!("  [output] stream error: {:#}", e);
                    break;
                }
            }
        }
    });

    // Balloon adjustment: run inline before entering the wait loop, so that
    // we don't need to use wait_container_exit in a select loop (it is NOT
    // cancel-safe because recv_event does two sequential read_exact calls).
    let initial_balloon = args.balloon_amount_mib;
    tokio::time::sleep(Duration::from_secs(3)).await;
    let new_amount = initial_balloon / 2;
    eprintln!(
        "  [balloon] adjusting: {} -> {} MiB (releasing {} MiB to guest)",
        initial_balloon, new_amount, initial_balloon - new_amount,
    );
    match vm.set_balloon(new_amount).await {
        Ok(()) => eprintln!("  [balloon] set to {} MiB", new_amount),
        Err(e) => eprintln!("  [balloon] set_balloon failed: {:#}", e),
    }
    eprintln!("  [testbench] waiting for container exit or Ctrl+C...");

    // Wait for container exit or shutdown signal. No loop needed — both futures
    // are polled exactly once, avoiding the cancel-safety problem.
    let mut container_exited = false;
    tokio::select! {
        result = vm.wait_container_exit() => {
            match result {
                Ok((id, code)) => eprintln!("  [testbench] container {} exited (code={})", id, code),
                Err(e) => eprintln!("  [testbench] wait_container_exit error: {:#}", e),
            }
            container_exited = true;
        }
        _ = shutdown.cancelled() => {
            eprintln!();
            eprintln!("  [testbench] shutdown requested");
        }
    }

    // Shutdown.
    if container_exited {
        eprintln!("  [testbench] shutting down VM...");
        match vm.shutdown().await {
            Ok(()) => eprintln!("  [testbench] VM shut down cleanly"),
            Err(e) => eprintln!("  [testbench] shutdown error: {:#}", e),
        }
    } else {
        eprintln!("  [testbench] graceful shutdown (timeout=5s)...");
        match vm.graceful_shutdown(Duration::from_secs(5)).await {
            Ok(()) => eprintln!("  [testbench] VM shut down cleanly"),
            Err(e) => {
                eprintln!("  [testbench] graceful shutdown failed: {:#}, force killing...", e);
                let _ = vm.force_kill().await;
            }
        }
    }

    eprintln!();
    eprintln!("=== Testbench complete ===");
    Ok(())
}
