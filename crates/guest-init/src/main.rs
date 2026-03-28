use std::cell::RefCell;
use std::rc::Rc;
use std::sync::Arc;
use parking_lot::Mutex;

use anyhow::Context as _;
use async_executor::LocalExecutor;
use futures::FutureExt;

use distvirt_guest_protocol::GuestEvent;

use guest_init::buffer::EventBuffer;
use guest_init::config::{GuestConfig, TransportConfig};
use guest_init::container::{ContainerManager, VmContainerBackend};
use guest_init::platform::{Platform, VmPlatform};
use guest_init::supervisor::run_supervisor;
use guest_init::transport::TransportListener;

// ---------------------------------------------------------------------------
// Production entry point
// ---------------------------------------------------------------------------

fn run() -> anyhow::Result<()> {
    let boot_platform = VmPlatform;
    boot_platform.mount_essential_filesystems();
    boot_platform.configure_network_loopback();
    boot_platform.configure_memory()?;
    boot_platform.setup_cgroup_root();

    // Read VM memory size for memory manager initialization.
    let vm_mem_mib = guest_init::memory::init::read_memtotal_mib()?;

    let config = GuestConfig::from_cmdline()?;

    let listener = match &config.transport {
        TransportConfig::VirtioSerial { device } => {
            let path = match device {
                Some(p) => p.clone(),
                None => guest_init::transport::find_virtio_serial_port("transport")
                    .context("find virtio-serial transport device")?,
            };
            log::info!("using virtio-serial transport: {}", path.display());
            TransportListener::VirtioSerial { path }
        }
        TransportConfig::Vsock { port } => {
            log::info!("starting vsock listener on port {}", port);
            let vsock_listener =
                guest_init::vsock::VsockListener::bind(*port).context("bind vsock listener")?;
            TransportListener::Vsock(vsock_listener)
        }
    };

    let ex = LocalExecutor::new();

    async_io::block_on(ex.run(async {
        // Container backend — owns per-container OS interaction and tasks.
        let vm_backend = VmContainerBackend::new();

        // Containers persist across reconnects.
        let containers = Arc::new(Mutex::new(ContainerManager::new(vm_backend)));

        // Event buffer persists across reconnects.
        let event_buffer = EventBuffer::new();

        // Create memory manager if balloon size is configured.
        let memory_manager = match config.balloon_mib {
            Some(balloon_mib) => {
                log::info!(
                    "memory manager: balloon={} MiB, vm_mem={} MiB, initial_limit=~{} MiB",
                    balloon_mib,
                    vm_mem_mib,
                    vm_mem_mib
                        .saturating_sub(balloon_mib)
                        .saturating_sub(guest_init::memory::KERNEL_BUFFER_MIB),
                );
                Some(Rc::new(RefCell::new(guest_init::memory::MemoryManager::new(
                    balloon_mib,
                    vm_mem_mib,
                ))))
            }
            None => {
                log::info!("no distvirt.balloon_mib configured, memory manager disabled");
                None
            }
        };

        // Set initial cgroup memory limits BEFORE creating PSI triggers.
        if let Some(ref mm) = memory_manager {
            let (high_bytes, max_bytes) = mm.borrow_mut().initial_limits();
            log::info!(
                "[balloon] setting initial cgroup limits: path={}, high={} MiB, max={} MiB",
                guest_init::cgroup::CGROUP_ROOT,
                high_bytes / (1024 * 1024),
                max_bytes / (1024 * 1024),
            );
            if let Err(e) =
                guest_init::cgroup::set_memory_limits(guest_init::cgroup::CGROUP_ROOT, high_bytes, max_bytes)
            {
                log::warn!("failed to set initial cgroup limits: {:#}", e);
            }
        }

        // Now that memory limits are set, create async PSI monitor.
        let async_psi = match guest_init::cgroup::setup_psi_monitor(guest_init::cgroup::CGROUP_ROOT) {
            Ok(triggers) => match guest_init::cgroup::AsyncPsiMonitor::new(triggers) {
                Ok(monitor) => Some(Rc::new(monitor)),
                Err(e) => {
                    log::warn!("failed to create async PSI monitor: {:#}", e);
                    None
                }
            },
            Err(e) => {
                log::warn!("failed to set up PSI triggers: {:#}", e);
                None
            }
        };

        // Set up inotify-based memory.events monitor.
        let mem_events_holder = Rc::new(RefCell::new(
            match guest_init::cgroup::AsyncMemoryEventsMonitor::new(guest_init::cgroup::CGROUP_ROOT) {
                Ok(monitor) => Some(monitor),
                Err(e) => {
                    log::warn!("failed to create memory.events monitor: {:#}", e);
                    None
                }
            },
        ));

        // Balloon monitor and task.
        let (balloon_monitor_tx, balloon_monitor_rx) =
            async_channel::bounded::<guest_init::memory::monitor::BalloonChange>(8);
        let balloon_monitor_task = if memory_manager.is_some() {
            Some(ex.spawn(async {
                if let Err(e) = guest_init::memory::monitor::run(balloon_monitor_tx).await {
                    log::warn!("balloon monitor exited: {:#}", e);
                }
            }))
        } else {
            drop(balloon_monitor_tx);
            log::info!("balloon monitor disabled (no distvirt.balloon_mib)");
            None
        };

        let balloon_task = if let (Some(mm), Some(psi)) = (&memory_manager, &async_psi) {
            let event_tx = event_buffer.sender();
            Some(ex.spawn(guest_init::memory::task::run(
                mm.clone(),
                psi.clone(),
                event_tx,
                mem_events_holder.clone(),
                balloon_monitor_rx.clone(),
            )))
        } else {
            None
        };

        let platform = VmPlatform;

        // Config drive pre-configuration is not currently used.
        // Assert early so we notice if it gets enabled without the plumbing.
        assert!(
            config.config_device.is_none(),
            "config_device is set but pre-config support has been removed from the supervisor. \
             Re-add pre_config_responses threading if this feature is needed."
        );

        // Core supervisor (runtime-agnostic): connection loop + exit handling.
        let mut supervisor = std::pin::pin!(run_supervisor(
            &config,
            &platform,
            containers.clone(),
            &event_buffer,
            &listener,
            &ex,
        ).fuse());

        // Balloon futures that go pending if disabled.
        let balloon_monitor_fut = async {
            match balloon_monitor_task {
                Some(task) => task.await,
                None => futures::future::pending::<()>().await,
            }
        };
        let mut balloon_monitor_fut = std::pin::pin!(balloon_monitor_fut.fuse());

        let balloon_task_fut = async {
            match balloon_task {
                Some(task) => task.await,
                None => futures::future::pending::<()>().await,
            }
        };
        let mut balloon_task_fut = std::pin::pin!(balloon_task_fut.fuse());

        // Production wrapper: supervisor + balloon monitoring.
        // Balloon task failure is fatal — sends TaskError and exits.
        // The VM will reboot via main() regardless.
        futures::select! {
            result = supervisor => {
                result?;
            }
            _ = balloon_monitor_fut => {
                log::error!("balloon monitor exited unexpectedly");
                event_buffer.send(GuestEvent::TaskError {
                    task: "balloon_monitor".to_string(),
                    message: "balloon monitor exited unexpectedly".to_string(),
                }).await;
            }
            _ = balloon_task_fut => {
                log::error!("balloon task exited unexpectedly");
                event_buffer.send(GuestEvent::TaskError {
                    task: "balloon_task".to_string(),
                    message: "balloon task exited unexpectedly".to_string(),
                }).await;
            }
        }

        Ok(())
    }))
}

fn main() {
    // Check for --container-init before any runtime setup.
    let args: Vec<String> = std::env::args().collect();
    if args.len() >= 3 && args[1] == "--container-init" {
        let pipe_fd: i32 = args[2]
            .parse()
            .expect("--container-init: invalid pipe fd");
        guest_init::container::container_init_main(pipe_fd);
    }

    env_logger::Builder::new()
        .filter_level(log::LevelFilter::Info)
        .parse_default_env()
        .init();

    log::info!("guest-init started");

    if let Err(e) = run() {
        log::error!("fatal: {:#}", e);
    }

    log::info!("shutting down");
    unsafe {
        libc::sync();
    }
    let cmd = match guest_init::memory::init::read_cmdline_param("distvirt.shutdown").as_deref() {
        Some("poweroff") => {
            log::info!("using ACPI power-off (distvirt.shutdown=poweroff)");
            libc::RB_POWER_OFF
        }
        _ => {
            log::info!("using reboot/triple-fault shutdown");
            libc::RB_AUTOBOOT
        }
    };
    unsafe {
        libc::reboot(cmd);
    }
    loop {
        unsafe {
            libc::pause();
        }
    }
}
