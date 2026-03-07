use std::fs;
use std::os::unix::io::{FromRawFd, OwnedFd};

use anyhow::{bail, Context};

const CGROUP_ROOT: &str = "/sys/fs/cgroup/containers";

/// Create the parent cgroup for all containers and enable the memory controller.
pub fn init_container_cgroup_root() -> anyhow::Result<()> {
    fs::create_dir_all(CGROUP_ROOT)
        .with_context(|| format!("create {}", CGROUP_ROOT))?;

    // Enable memory controller in the parent so child cgroups get memory.pressure.
    fs::write("/sys/fs/cgroup/cgroup.subtree_control", "+memory")
        .context("enable memory controller on root cgroup")?;

    log::info!("cgroup root {} initialized with memory controller", CGROUP_ROOT);
    Ok(())
}

/// Create a per-container cgroup directory. Returns the path.
pub fn setup_container_cgroup(id: &str) -> anyhow::Result<String> {
    let path = format!("{}/{}", CGROUP_ROOT, id);
    fs::create_dir_all(&path)
        .with_context(|| format!("create cgroup {}", path))?;
    log::info!("created cgroup {}", path);
    Ok(path)
}

/// Move a process into a cgroup by writing its PID to cgroup.procs.
pub fn move_to_cgroup(cgroup_path: &str, pid: libc::pid_t) -> anyhow::Result<()> {
    let procs_path = format!("{}/cgroup.procs", cgroup_path);
    fs::write(&procs_path, pid.to_string())
        .with_context(|| format!("write pid {} to {}", pid, procs_path))?;
    log::info!("moved pid {} to cgroup {}", pid, cgroup_path);
    Ok(())
}

/// Set up a PSI trigger on a cgroup's memory.pressure file.
///
/// Opens memory.pressure, writes a trigger string, and returns the fd.
/// The fd becomes readable when the PSI threshold is exceeded.
/// Reading from it re-arms the trigger.
///
/// Trigger: "some 100000 1000000" = some stall for 100ms in any 1s window.
pub fn setup_psi_monitor(cgroup_path: &str) -> anyhow::Result<OwnedFd> {
    let pressure_path = format!("{}/memory.pressure", cgroup_path);

    // Open with O_RDWR | O_CLOEXEC for PSI trigger registration.
    let path_c = std::ffi::CString::new(pressure_path.as_str())
        .context("invalid pressure path")?;
    let fd = unsafe { libc::open(path_c.as_ptr(), libc::O_RDWR | libc::O_CLOEXEC) };
    if fd < 0 {
        bail!(
            "open {}: {}",
            pressure_path,
            std::io::Error::last_os_error()
        );
    }
    let owned_fd = unsafe { OwnedFd::from_raw_fd(fd) };

    // Write the PSI trigger string.
    let trigger = b"some 100000 1000000";
    let written = unsafe {
        libc::write(
            fd,
            trigger.as_ptr() as *const libc::c_void,
            trigger.len(),
        )
    };
    if written < 0 {
        bail!(
            "write PSI trigger to {}: {}",
            pressure_path,
            std::io::Error::last_os_error()
        );
    }

    log::info!("PSI monitor set up on {}", pressure_path);
    Ok(owned_fd)
}

/// Remove a per-container cgroup directory.
pub fn remove_cgroup(id: &str) {
    let path = format!("{}/{}", CGROUP_ROOT, id);
    if let Err(e) = fs::remove_dir(&path) {
        log::warn!("remove cgroup {}: {}", path, e);
    } else {
        log::info!("removed cgroup {}", path);
    }
}
