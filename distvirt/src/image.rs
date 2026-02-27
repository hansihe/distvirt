use std::path::Path;
use std::process::Command;

use anyhow::{bail, Context};

/// Build an ext4 image from a rootfs directory.
///
/// Shells out to `du`, `truncate`, `mkfs.ext4`, and `resize2fs` (from e2fsprogs).
pub fn build_ext4_image(rootfs: &Path, output: &Path) -> anyhow::Result<()> {
    // Calculate size of rootfs.
    let du_output = Command::new("du")
        .args(["-sb"])
        .arg(rootfs)
        .output()
        .context("running du")?;
    if !du_output.status.success() {
        bail!(
            "du failed: {}",
            String::from_utf8_lossy(&du_output.stderr)
        );
    }
    let du_str = String::from_utf8_lossy(&du_output.stdout);
    let size_bytes: u64 = du_str
        .split_whitespace()
        .next()
        .context("parsing du output")?
        .parse()
        .context("parsing size")?;

    // Allocate image with 20% overhead + 10MB.
    let image_size = (size_bytes as f64 * 1.2) as u64 + 10 * 1024 * 1024;

    // Create sparse file.
    let status = Command::new("truncate")
        .args(["-s", &image_size.to_string()])
        .arg(output)
        .status()
        .context("running truncate")?;
    if !status.success() {
        bail!("truncate failed");
    }

    // Create ext4 filesystem populated with rootfs contents.
    let status = Command::new("mkfs.ext4")
        .args(["-d"])
        .arg(rootfs)
        .arg(output)
        .status()
        .context("running mkfs.ext4")?;
    if !status.success() {
        bail!("mkfs.ext4 failed");
    }

    // Shrink to minimum size.
    let status = Command::new("resize2fs")
        .arg("-M")
        .arg(output)
        .status()
        .context("running resize2fs")?;
    if !status.success() {
        bail!("resize2fs failed");
    }

    Ok(())
}
