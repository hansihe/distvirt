use std::path::Path;

use anyhow::Context;

use crate::linux::net::PersistentTap;
use crate::vmm::{BalloonConfig, NetConfig};

/// An additional block device to attach to the VM.
pub(super) struct AdditionalDrive {
    pub filename: String,
    pub read_only: bool,
}

/// Result of building the VM configuration.
pub(super) struct BuiltVmConfig {
    pub config_json: serde_json::Value,
    pub tap: Option<PersistentTap>,
}

/// Build the Cloud Hypervisor `vm.create` JSON configuration.
///
/// Creates a TAP device if networking is configured (the TAP name and MAC
/// address go directly into the JSON config).
pub(super) fn build(
    kernel_path: &Path,
    vcpu_count: u32,
    mem_size_mib: u32,
    balloon: Option<&BalloonConfig>,
    serial_console: bool,
    shared_memory: bool,
    additional_drives: &[AdditionalDrive],
    virtiofs_tags: &[String],
    net: Option<&NetConfig>,
) -> anyhow::Result<BuiltVmConfig> {
    let kernel_path_str = kernel_path
        .to_str()
        .ok_or_else(|| anyhow::anyhow!("kernel_path is not valid UTF-8"))?;

    // Disks: vda = rootfs, vdb+ = additional drives (scratch, volumes, etc.)
    // image_type must be set explicitly — CH v51+ autodetects raw images and
    // silently disables sector 0 writes, which breaks ext4 superblock updates.
    let mut disks = vec![
        serde_json::json!({"path": "./rootfs.ext4", "readonly": false, "image_type": "Raw"}),
    ];
    for drive in additional_drives {
        disks.push(
            serde_json::json!({"path": format!("./{}", drive.filename), "readonly": drive.read_only, "image_type": "Raw"}),
        );
    }

    let boot_args = {
        let mut args = "console=hvc0 reboot=k panic=-1 root=/dev/vda init=/sbin/init distvirt.shutdown=poweroff".to_string();
        if let Some(balloon) = balloon {
            args.push_str(&format!(" distvirt.balloon_mib={}", balloon.amount_mib));
        }
        args
    };

    let mut config_json = serde_json::json!({
        "payload": {
            "kernel": kernel_path_str,
            "cmdline": boot_args,
        },
        "disks": disks,
        "vsock": {
            "cid": 3,
            "socket": "./vsock.sock",
        },
        "cpus": {
            "boot_vcpus": vcpu_count,
            "max_vcpus": vcpu_count,
        },
        "memory": {
            "size": (mem_size_mib as u64) * 1024 * 1024,
            "shared": shared_memory,
        },
        "serial": {
            "mode": "Off",
        },
        "console": {
            "mode": if serial_console { "Tty" } else { "Off" },
        },
    });

    if let Some(balloon) = balloon {
        config_json["balloon"] = serde_json::json!({
            "size": (balloon.amount_mib as u64) * 1024 * 1024,
            "deflate_on_oom": balloon.deflate_on_oom,
        });
    }

    if !virtiofs_tags.is_empty() {
        let fs_array: Vec<serde_json::Value> = virtiofs_tags
            .iter()
            .map(|tag| {
                serde_json::json!({
                    "tag": tag,
                    "socket": format!("./virtiofs-{}.sock", tag),
                    "num_queues": 1,
                    "queue_size": 1024,
                })
            })
            .collect();
        config_json["fs"] = serde_json::json!(fs_array);
    }

    let tap = if let Some(net) = net {
        let tap = PersistentTap::create().context("create TAP device")?;
        tap.bring_up().context("bring TAP interface up")?;
        let mac_str = format!(
            "{:02X}:{:02X}:{:02X}:{:02X}:{:02X}:{:02X}",
            net.guest_mac[0], net.guest_mac[1], net.guest_mac[2],
            net.guest_mac[3], net.guest_mac[4], net.guest_mac[5]
        );
        config_json["net"] = serde_json::json!([{
            "tap": tap.name(),
            "mac": mac_str,
            "offload_tso": false,
            "offload_ufo": false,
            "offload_csum": false,
        }]);
        log::info!("configured network: tap={}, guest_ip={}", tap.name(), net.guest_ip);
        Some(tap)
    } else {
        None
    };

    Ok(BuiltVmConfig { config_json, tap })
}
