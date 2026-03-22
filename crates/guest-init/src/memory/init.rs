use anyhow::{Context, bail};

/// Read a parameter value from /proc/cmdline by key prefix (e.g. "distvirt.balloon_mib").
pub fn read_cmdline_param(key: &str) -> Option<String> {
    let cmdline = std::fs::read_to_string("/proc/cmdline").ok()?;
    let prefix = format!("{}=", key);
    for token in cmdline.split_whitespace() {
        if let Some(val) = token.strip_prefix(&prefix) {
            return Some(val.to_string());
        }
    }
    None
}

/// Parse MemTotal from /proc/meminfo in MiB.
pub fn read_memtotal_mib() -> anyhow::Result<u32> {
    let meminfo =
        std::fs::read_to_string("/proc/meminfo").context("failed to read /proc/meminfo")?;
    for line in meminfo.lines() {
        if let Some(rest) = line.strip_prefix("MemTotal:") {
            let rest = rest.trim();
            if let Some(kb_str) = rest.strip_suffix("kB") {
                let kb = kb_str
                    .trim()
                    .parse::<u64>()
                    .context("failed to parse MemTotal value")?;
                return Ok((kb / 1024) as u32);
            }
        }
    }
    bail!("MemTotal not found in /proc/meminfo")
}

/// Centralized memory-derived configuration scaled to VM size.
pub struct VmMemoryConfig {
    pub vm_mem_mib: u32,
    pub zram_size_bytes: u64,
    pub tcp_buf_min: u32,
    pub tcp_buf_default: u32,
    pub tcp_buf_max: u32,
}

impl VmMemoryConfig {
    pub fn from_vm_mem(vm_mem_mib: u32) -> Self {
        let zram_mib = (vm_mem_mib / 4).clamp(64, 256);
        let tcp_buf_max =
            ((vm_mem_mib as u64) * 1024).clamp(2 * 1024 * 1024, 8 * 1024 * 1024) as u32;

        VmMemoryConfig {
            vm_mem_mib,
            zram_size_bytes: zram_mib as u64 * 1024 * 1024,
            tcp_buf_min: 4096,
            tcp_buf_default: 131072,
            tcp_buf_max,
        }
    }
}

/// Set up zram swap as a safety net for memory pressure bursts.
///
/// Configures a zram device sized according to `VmMemoryConfig` with lz4 compression
/// and enables it as swap.
/// Non-fatal: if the kernel lacks CONFIG_ZRAM or any step fails, we just log and continue.
pub fn setup_zram_swap(config: &VmMemoryConfig) {
    let zram_path = std::path::Path::new("/sys/block/zram0");
    if !zram_path.exists() {
        log::warn!("zram: /sys/block/zram0 not found (kernel needs CONFIG_ZRAM), skipping");
        return;
    }

    // Reset the device in case it was previously configured.
    if let Err(e) = std::fs::write("/sys/block/zram0/reset", "1") {
        log::warn!("zram: failed to reset: {}", e);
        return;
    }

    if let Err(e) = std::fs::write("/sys/block/zram0/comp_algorithm", "lz4") {
        log::warn!("zram: failed to set comp_algorithm: {}", e);
        return;
    }

    if let Err(e) = std::fs::write(
        "/sys/block/zram0/disksize",
        config.zram_size_bytes.to_string(),
    ) {
        log::warn!("zram: failed to set disksize: {}", e);
        return;
    }

    // mkswap via command
    let mkswap = std::process::Command::new("mkswap")
        .arg("/dev/zram0")
        .output();
    match mkswap {
        Ok(out) if out.status.success() => {}
        Ok(out) => {
            log::warn!(
                "zram: mkswap failed: {}",
                String::from_utf8_lossy(&out.stderr)
            );
            return;
        }
        Err(e) => {
            log::warn!("zram: mkswap exec failed: {}", e);
            return;
        }
    }

    // swapon via libc
    let path = std::ffi::CString::new("/dev/zram0").unwrap();
    let ret = unsafe { libc::swapon(path.as_ptr(), 0) };
    if ret != 0 {
        log::warn!("zram: swapon failed: {}", std::io::Error::last_os_error());
        return;
    }

    log::info!(
        "zram: {} MiB zram0 swap enabled (lz4)",
        config.zram_size_bytes / (1024 * 1024)
    );
}

/// Cap TCP buffer sizes to prevent runaway kernel memory usage from network buffers.
pub fn set_tcp_memory_caps(config: &VmMemoryConfig) {
    let value = format!(
        "{}\t{}\t{}",
        config.tcp_buf_min, config.tcp_buf_default, config.tcp_buf_max
    );
    for file in &["tcp_rmem", "tcp_wmem"] {
        let path = format!("/proc/sys/net/ipv4/{}", file);
        if let Err(e) = std::fs::write(&path, &value) {
            log::warn!("tcp caps: failed to write {}: {}", path, e);
        }
    }
    log::info!(
        "tcp caps: set tcp_rmem and tcp_wmem limits (max={} bytes)",
        config.tcp_buf_max
    );
}
