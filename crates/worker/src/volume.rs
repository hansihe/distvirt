use std::path::{Path, PathBuf};
use std::process::Stdio;

use anyhow::Context;
use distvirt_worker_protocol::{ConfigDataFile, VolumeSpec, VolumeType};
use tokio::process::Command;

/// A volume whose image has been created and is ready to attach.
pub struct PreparedVolume {
    pub name: String,
    pub image_path: PathBuf,
    pub read_only: bool,
}

/// Create ext4 images for all volumes in `work_dir`.
///
/// Subprocess stdio is explicitly set to `Stdio::null()` to work around a
/// suspected tokio bug: when the test binary's stderr is a TTY and multiple
/// `current_thread` + `start_paused` runtimes run in parallel (as in
/// `cargo test`), inheriting the TTY causes intermittent subprocess failures
/// ("No such file or directory while setting up superblock" from mkfs.ext4).
/// Suppressing stdio eliminates the issue. See also `TestVmm` in test_vmm.rs
/// for a similar `std::fs` workaround.
pub async fn prepare_volumes(
    volumes: &[VolumeSpec],
    work_dir: &Path,
) -> anyhow::Result<Vec<PreparedVolume>> {
    let mut prepared = Vec::with_capacity(volumes.len());
    for vol in volumes {
        let image_path = work_dir.join(format!("vol-{}.ext4", vol.name));
        match &vol.volume_type {
            VolumeType::EmptyDir { size_mb } => {
                create_empty_dir_image(&image_path, *size_mb)
                    .await
                    .with_context(|| format!("create empty_dir volume '{}'", vol.name))?;
                prepared.push(PreparedVolume {
                    name: vol.name.clone(),
                    image_path,
                    read_only: false,
                });
            }
            VolumeType::ConfigData { files } => {
                create_config_data_image(&image_path, files)
                    .await
                    .with_context(|| format!("create config_data volume '{}'", vol.name))?;
                prepared.push(PreparedVolume {
                    name: vol.name.clone(),
                    image_path,
                    read_only: true,
                });
            }
        }
    }
    Ok(prepared)
}

/// Create an empty ext4 image of the given size in megabytes.
async fn create_empty_dir_image(path: &Path, size_mb: u64) -> anyhow::Result<()> {
    anyhow::ensure!(size_mb > 0, "empty_dir size_mb must be greater than 0");

    let status = Command::new("truncate")
        .arg("-s")
        .arg(format!("{}M", size_mb))
        .arg(path)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .await
        .context("run truncate")?;
    if !status.success() {
        anyhow::bail!("truncate failed with {}", status);
    }

    let status = Command::new("mkfs.ext4")
        .arg("-F")
        .arg("-q")
        .arg(path)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .await
        .context("run mkfs.ext4")?;
    if !status.success() {
        anyhow::bail!("mkfs.ext4 failed with {}", status);
    }

    Ok(())
}

/// Create an ext4 image populated with the given files using `mke2fs -d`.
async fn create_config_data_image(
    path: &Path,
    files: &[ConfigDataFile],
) -> anyhow::Result<()> {
    let tmp_dir = tempfile::tempdir().context("create temp dir for config_data")?;

    for file in files {
        let file_path = tmp_dir.path().join(file.path.trim_start_matches('/'));
        if let Some(parent) = file_path.parent() {
            tokio::fs::create_dir_all(parent)
                .await
                .with_context(|| format!("create parent dirs for {}", file.path))?;
        }
        tokio::fs::write(&file_path, &file.content)
            .await
            .with_context(|| format!("write config file {}", file.path))?;
        if file.mode != 0 {
            use std::os::unix::fs::PermissionsExt;
            tokio::fs::set_permissions(&file_path, std::fs::Permissions::from_mode(file.mode))
                .await
                .with_context(|| format!("set permissions on {}", file.path))?;
        }
    }

    // Calculate a reasonable image size: at least 1MB, or enough for the files + overhead.
    let total_bytes: usize = files.iter().map(|f| f.content.len()).sum();
    let size_kb = std::cmp::max(1024, (total_bytes / 1024) + 512);

    // Create the image file first with the right size.
    let status = Command::new("truncate")
        .arg("-s")
        .arg(format!("{}K", size_kb))
        .arg(path)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .await
        .context("run truncate for config_data")?;
    if !status.success() {
        anyhow::bail!("truncate failed with {}", status);
    }

    let status = Command::new("mke2fs")
        .arg("-t")
        .arg("ext4")
        .arg("-d")
        .arg(tmp_dir.path())
        .arg("-F")
        .arg("-q")
        .arg(path)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .await
        .context("run mke2fs")?;
    if !status.success() {
        anyhow::bail!("mke2fs failed with {}", status);
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use distvirt_worker_protocol::{ConfigDataFile, VolumeSpec, VolumeType};

    #[tokio::test]
    async fn prepare_empty_dir_volume() {
        let tmp = tempfile::tempdir().unwrap();
        let volumes = vec![VolumeSpec {
            name: "data".to_string(),
            volume_type: VolumeType::EmptyDir { size_mb: 1 },
        }];
        let result = prepare_volumes(&volumes, tmp.path()).await.unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].name, "data");
        assert!(!result[0].read_only);
        assert!(result[0].image_path.exists());
    }

    #[tokio::test]
    async fn prepare_config_data_volume() {
        let tmp = tempfile::tempdir().unwrap();
        let volumes = vec![VolumeSpec {
            name: "config".to_string(),
            volume_type: VolumeType::ConfigData {
                files: vec![ConfigDataFile {
                    path: "hello.txt".to_string(),
                    content: "world".to_string(),
                    mode: 0o644,
                }],
            },
        }];
        let result = prepare_volumes(&volumes, tmp.path()).await.unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].name, "config");
        assert!(result[0].read_only);
        assert!(result[0].image_path.exists());
    }
}
