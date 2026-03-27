use std::path::{Path, PathBuf};
use std::process::Stdio;

use anyhow::Context;
use distvirt_worker_protocol::{ConfigDataFile, VolumeSpec, VolumeType};
use tokio::process::Command;

/// A prepared volume ready to attach to a VM.
pub enum PreparedVolume {
    /// Block device volume (EmptyDir).
    Block {
        name: String,
        image_path: PathBuf,
        read_only: bool,
    },
    /// Directory to share (ConfigData).
    Directory {
        name: String,
        dir_path: PathBuf,
        read_only: bool,
        /// RAII cleanup handle — keeps temp directory alive.
        _cleanup: Box<dyn std::any::Any + Send + Sync>,
    },
}

impl PreparedVolume {
    /// Convert to a `MountRequest` for the VMM builder interface.
    pub fn to_mount_request(&self) -> crate::vmm::MountRequest {
        match self {
            PreparedVolume::Block {
                name,
                image_path,
                read_only,
            } => crate::vmm::MountRequest {
                tag: format!("vol-{}", name),
                source: crate::vmm::VmMountSource::BlockImage {
                    path: image_path.clone(),
                    read_only: *read_only,
                },
            },
            PreparedVolume::Directory {
                name,
                dir_path,
                read_only: _,
                ..
            } => crate::vmm::MountRequest {
                tag: format!("vol-{}", name),
                source: crate::vmm::VmMountSource::Directory {
                    path: dir_path.clone(),
                },
            },
        }
    }

    /// Get the volume name.
    pub fn name(&self) -> &str {
        match self {
            PreparedVolume::Block { name, .. } | PreparedVolume::Directory { name, .. } => name,
        }
    }

    /// Get the read-only flag.
    pub fn read_only(&self) -> bool {
        match self {
            PreparedVolume::Block { read_only, .. }
            | PreparedVolume::Directory { read_only, .. } => *read_only,
        }
    }
}

/// Prepare volumes for a VM.
///
/// EmptyDir volumes produce ext4 block images. ConfigData volumes produce
/// directories to share via virtiofs.
pub async fn prepare_volumes(
    volumes: &[VolumeSpec],
    work_dir: &Path,
) -> anyhow::Result<Vec<PreparedVolume>> {
    let mut prepared = Vec::with_capacity(volumes.len());
    for vol in volumes {
        match &vol.volume_type {
            VolumeType::EmptyDir { size_mb } => {
                let image_path = work_dir.join(format!("vol-{}.ext4", vol.name));
                create_empty_dir_image(&image_path, *size_mb)
                    .await
                    .with_context(|| format!("create empty_dir volume '{}'", vol.name))?;
                prepared.push(PreparedVolume::Block {
                    name: vol.name.clone(),
                    image_path,
                    read_only: false,
                });
            }
            VolumeType::ConfigData { files } => {
                let dir = create_config_data_dir(files)
                    .await
                    .with_context(|| format!("create config_data volume '{}'", vol.name))?;
                let dir_path = dir.path().to_path_buf();
                prepared.push(PreparedVolume::Directory {
                    name: vol.name.clone(),
                    dir_path,
                    read_only: true,
                    _cleanup: Box::new(dir),
                });
            }
        }
    }
    Ok(prepared)
}

/// Create overlay ext4 image of the given size in megabytes.
pub async fn create_overlay_image(path: &Path, size_mb: u64) -> anyhow::Result<()> {
    anyhow::ensure!(size_mb > 0, "overlay size_mb must be greater than 0");
    let file = tokio::fs::File::create(path)
        .await
        .context("create overlay image file")?;
    file.set_len(size_mb * 1024 * 1024)
        .await
        .context("set overlay image file size")?;
    drop(file);
    let status = Command::new("mkfs.ext4")
        .arg("-F")
        .arg("-q")
        .arg("-E")
        .arg("lazy_itable_init=1,lazy_journal_init=1")
        .arg(path)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .await
        .context("run mkfs.ext4 for overlay")?;
    if !status.success() {
        anyhow::bail!("mkfs.ext4 for overlay failed with {}", status);
    }
    Ok(())
}

/// Create an empty ext4 image of the given size in megabytes.
async fn create_empty_dir_image(path: &Path, size_mb: u64) -> anyhow::Result<()> {
    anyhow::ensure!(size_mb > 0, "empty_dir size_mb must be greater than 0");

    let file = tokio::fs::File::create(path)
        .await
        .context("create volume image file")?;
    file.set_len(size_mb * 1024 * 1024)
        .await
        .context("set volume image file size")?;
    drop(file);

    let status = Command::new("mkfs.ext4")
        .arg("-F")
        .arg("-q")
        .arg("-E")
        .arg("lazy_itable_init=1,lazy_journal_init=1")
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

/// Recreate ConfigData volumes from snapshot metadata.
///
/// Each `SnapshotConfigVolume` produces a temp directory with the config files,
/// suitable for passing to virtiofsd. Returns `(tag, dir_path, cleanup_handle)`
/// triples — the caller must keep the cleanup handles alive for the VM lifetime.
pub async fn prepare_config_volumes_from_snapshot(
    config_volumes: &[crate::vmm::SnapshotConfigVolume],
) -> anyhow::Result<Vec<(String, PathBuf, tempfile::TempDir)>> {
    let mut result = Vec::with_capacity(config_volumes.len());
    for cv in config_volumes {
        let dir = create_config_data_dir(&cv.files)
            .await
            .with_context(|| format!("recreate config volume '{}' from snapshot", cv.name))?;
        let dir_path = dir.path().to_path_buf();
        result.push((cv.tag.clone(), dir_path, dir));
    }
    Ok(result)
}

/// Create a temporary directory populated with ConfigData files.
pub(crate) async fn create_config_data_dir(
    files: &[ConfigDataFile],
) -> anyhow::Result<tempfile::TempDir> {
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

    Ok(tmp_dir)
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
        match &result[0] {
            PreparedVolume::Block { name, read_only, image_path } => {
                assert_eq!(name, "data");
                assert!(!read_only);
                assert!(image_path.exists());
            }
            _ => panic!("expected Block variant"),
        }
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
        match &result[0] {
            PreparedVolume::Directory { name, read_only, dir_path, .. } => {
                assert_eq!(name, "config");
                assert!(read_only);
                assert!(dir_path.exists());
                assert!(dir_path.join("hello.txt").exists());
            }
            _ => panic!("expected Directory variant"),
        }
    }
}
