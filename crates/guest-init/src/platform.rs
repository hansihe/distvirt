use distvirt_guest_protocol::VolumeSource;

/// Abstraction over VM-specific operations.
///
/// Covers boot-time initialization, runtime host commands (privileged syscalls),
/// and suspend/resume hooks. `VmPlatform` is the production implementation;
/// `NullPlatform` is the test implementation (all no-ops).
pub trait Platform {
    // ── Boot-time initialization ──────────────────────────────────────

    fn mount_essential_filesystems(&self);
    fn configure_network_loopback(&self);
    fn configure_memory(&self) -> anyhow::Result<()>;
    fn setup_cgroup_root(&self);

    // ── Runtime host commands (called from execute_command) ───────────

    fn mount_volume(
        &self,
        name: &str,
        source: &VolumeSource,
        read_only: bool,
    ) -> anyhow::Result<()>;

    fn configure_network(
        &self,
        interface: &str,
        ip: &str,
        netmask: &str,
        gateway: &str,
    ) -> anyhow::Result<()>;

    fn set_clock(&self, epoch_secs: u64, epoch_nanos: u32) -> anyhow::Result<()>;

    // ── Suspend/resume hooks ─────────────────────────────────────────

    fn on_suspend(&self);
    fn on_resume(&self);
}

/// Production platform: real mount(2), ioctl, clock_settime, tc qdisc, etc.
pub struct VmPlatform;

impl Platform for VmPlatform {
    fn mount_essential_filesystems(&self) {
        crate::init::mount_essential_filesystems();
    }

    fn configure_network_loopback(&self) {
        if let Err(e) = crate::net::bring_up_loopback() {
            log::warn!("failed to bring up loopback: {:#}", e);
        }
    }

    fn configure_memory(&self) -> anyhow::Result<()> {
        // Allow unlimited overcommit.
        if let Err(e) = std::fs::write("/proc/sys/vm/overcommit_memory", "1") {
            log::warn!("failed to set vm.overcommit_memory=1: {}", e);
        }

        let vm_mem_mib = crate::memory::init::read_memtotal_mib()?;
        let vm_config = crate::memory::init::VmMemoryConfig::from_vm_mem(vm_mem_mib);
        crate::memory::init::setup_zram_swap(&vm_config);
        crate::memory::init::set_tcp_memory_caps(&vm_config);
        Ok(())
    }

    fn setup_cgroup_root(&self) {
        if let Err(e) = crate::cgroup::init_container_cgroup_root() {
            log::warn!("failed to init cgroup root: {:#}", e);
        }
    }

    fn mount_volume(
        &self,
        name: &str,
        source: &VolumeSource,
        read_only: bool,
    ) -> anyhow::Result<()> {
        let mount_point = format!("/volumes/{}", name);
        let flags = if read_only {
            libc::MS_RDONLY as libc::c_ulong
        } else {
            0
        };
        match source {
            VolumeSource::Device { device } => {
                crate::util::mount(device, &mount_point, "ext4", flags, None)?;
                log::info!(
                    "mounted volume '{}' (device {}) at {}",
                    name,
                    device,
                    mount_point
                );
            }
            VolumeSource::VirtioFs { tag } => {
                crate::util::mount(tag, &mount_point, "virtiofs", flags, None)?;
                log::info!(
                    "mounted volume '{}' (virtiofs '{}') at {}",
                    name,
                    tag,
                    mount_point
                );
            }
        }
        Ok(())
    }

    fn configure_network(
        &self,
        interface: &str,
        ip: &str,
        netmask: &str,
        gateway: &str,
    ) -> anyhow::Result<()> {
        crate::net::configure_network(interface, ip, netmask, gateway)
    }

    fn set_clock(&self, epoch_secs: u64, epoch_nanos: u32) -> anyhow::Result<()> {
        let ts = libc::timespec {
            tv_sec: epoch_secs as libc::time_t,
            tv_nsec: epoch_nanos as libc::c_long,
        };
        let ret = unsafe { libc::clock_settime(libc::CLOCK_REALTIME, &ts) };
        if ret == 0 {
            log::info!("system clock set successfully");
            Ok(())
        } else {
            let e = std::io::Error::last_os_error();
            Err(anyhow::anyhow!("clock_settime failed: {}", e))
        }
    }

    fn on_suspend(&self) {
        if let Err(e) = crate::net::suspend() {
            log::warn!("failed to install plug qdisc: {:#}", e);
        }
    }

    fn on_resume(&self) {
        if let Err(e) = crate::net::resume() {
            log::warn!("failed to unplug qdisc on resume: {:#}", e);
        }
    }
}

/// Test platform: all operations are no-ops.
pub struct NullPlatform;

impl Platform for NullPlatform {
    fn mount_essential_filesystems(&self) {}
    fn configure_network_loopback(&self) {}
    fn configure_memory(&self) -> anyhow::Result<()> {
        Ok(())
    }
    fn setup_cgroup_root(&self) {}
    fn mount_volume(
        &self,
        _name: &str,
        _source: &VolumeSource,
        _read_only: bool,
    ) -> anyhow::Result<()> {
        Ok(())
    }
    fn configure_network(
        &self,
        _interface: &str,
        _ip: &str,
        _netmask: &str,
        _gateway: &str,
    ) -> anyhow::Result<()> {
        Ok(())
    }
    fn set_clock(&self, _epoch_secs: u64, _epoch_nanos: u32) -> anyhow::Result<()> {
        Ok(())
    }
    fn on_suspend(&self) {}
    fn on_resume(&self) {}
}
