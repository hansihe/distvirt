use std::path::Path;

use anyhow::Context;

use crate::vmm::{SnapshotArtifacts, copy_file_writable};

/// Copy snapshot artifacts (rootfs, overlay, volume images, CH state files)
/// into the working directory for restore.
pub(super) async fn copy_snapshot_to_tmpdir(
    snapshot: &SnapshotArtifacts,
    working_dir: &Path,
) -> anyhow::Result<()> {
    let metadata = &snapshot.metadata;
    let snapshot_dir = &snapshot.snapshot_dir;

    // Copy rootfs from the original source path.
    copy_file_writable(
        &metadata.rootfs_source_path,
        &working_dir.join("rootfs.ext4"),
    )
    .await?;

    // Copy overlay image from snapshot.
    copy_file_writable(
        &snapshot_dir.join("overlay.ext4"),
        &working_dir.join("overlay.ext4"),
    )
    .await?;

    // Copy volume images from snapshot.
    for vd in &metadata.volume_drives {
        copy_file_writable(
            &snapshot_dir.join(&vd.filename),
            &working_dir.join(&vd.filename),
        )
        .await
        .with_context(|| format!("copy volume image '{}' from snapshot", vd.filename))?;
    }

    // Copy CH snapshot files.
    for filename in &["config.json", "state.json", "memory-ranges"] {
        tokio::fs::copy(
            snapshot_dir.join(filename),
            working_dir.join(filename),
        )
        .await
        .with_context(|| format!("copy {} from snapshot", filename))?;
    }

    Ok(())
}

/// Patch a CH snapshot's `config.json` in a single read-modify-write pass.
///
/// Updates:
/// - vsock socket path (always)
/// - virtiofs socket paths (if any fs entries exist)
/// - TAP device name (if `tap_name` is `Some`)
pub(super) async fn patch_snapshot_config(
    working_dir: &Path,
    tap_name: Option<&str>,
) -> anyhow::Result<()> {
    let config_path = working_dir.join("config.json");
    let data = tokio::fs::read_to_string(&config_path)
        .await
        .context("read config.json for patching")?;
    let mut config: serde_json::Value =
        serde_json::from_str(&data).context("parse config.json for patching")?;

    // Patch vsock socket path.
    if let Some(vsock) = config.get_mut("vsock").and_then(|v| v.as_object_mut()) {
        let new_socket = working_dir.join("vsock.sock");
        let socket_str = new_socket
            .to_str()
            .context("vsock socket path is not valid UTF-8")?;
        vsock.insert("socket".to_string(), serde_json::json!(socket_str));
    }

    // Patch virtiofs socket paths.
    if let Some(fs_array) = config.get_mut("fs").and_then(|f| f.as_array_mut()) {
        for fs_entry in fs_array {
            if let Some(obj) = fs_entry.as_object_mut() {
                if let Some(tag) = obj.get("tag").and_then(|t| t.as_str()).map(String::from) {
                    let new_socket = working_dir.join(format!("virtiofs-{}.sock", tag));
                    let socket_str = new_socket
                        .to_str()
                        .context("virtiofs socket path is not valid UTF-8")?;
                    obj.insert("socket".to_string(), serde_json::json!(socket_str));
                }
            }
        }
    }

    // Patch TAP device name.
    if let Some(tap) = tap_name {
        if let Some(nets) = config.get_mut("net").and_then(|n| n.as_array_mut()) {
            for net in nets {
                if let Some(obj) = net.as_object_mut() {
                    obj.insert("tap".to_string(), serde_json::json!(tap));
                }
            }
        }
    }

    let patched =
        serde_json::to_string_pretty(&config).context("serialize patched config.json")?;
    tokio::fs::write(&config_path, patched)
        .await
        .context("write patched config.json")?;

    Ok(())
}
