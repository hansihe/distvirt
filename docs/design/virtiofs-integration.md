# virtiofs Integration for Container Rootfs and ConfigData Volumes

> **Scope note:** Firecracker is deprecated; this design targets Cloud Hypervisor
> only. The config drive virtiofs migration (Phase 6) is deferred —
> `initial_commands` is not currently used in production.

## Motivation

Pod launch currently builds ext4 block images for every artifact passed to the
VM. The most expensive step is the container rootfs: the blockfile snapshotter
produces an ext4 image from OCI layers and the worker **copies** the entire file
into a per-VM tmpdir before launch. For a 500 MB image this is 500 MB of
synchronous I/O on every pod start.

By sharing the container rootfs (and ConfigData volumes) into the VM via
virtio-fs we eliminate the copy entirely. The virtiofsd process serves the
containerd snapshot directory directly—no intermediate image format, no copy.

## Design Overview

| Artifact | Current mechanism | New mechanism | Migration story |
|---|---|---|---|
| Guest OS rootfs | ext4 block device (vda) | ext4 block device (vda) — **unchanged** | Copied into tmpdir as before |
| Container rootfs | Full ext4 block copy (vdb) | **virtiofs** read-only share + small ext4 overlay device (vdb) | Destination unpacks same OCI image; overlay device shipped in snapshot |
| ConfigData volumes | ext4 image via `mke2fs -d` | **virtiofs** read-only share | Destination writes same config files |
| EmptyDir volumes | ext4 sparse image via `mkfs.ext4` | ext4 block device — **unchanged** | Shipped in snapshot as before |
| Config drive | Raw length-prefixed JSON block device (vdc) | Deferred (not currently used in production) | — |

### Key decisions

1. **Overlay for container writes.** The virtiofs share is read-only (the OCI
   image). A small ext4 block device provides the overlayfs upper/work dirs
   inside the guest. This keeps writable state in a self-contained file that
   ships naturally with snapshots.

2. **One virtiofsd per share.** Each virtiofs mount gets its own virtiofsd
   process. For the initial implementation this means 1 (container rootfs) + N
   (ConfigData volumes) processes per VM. Sharing one virtiofsd across multiple
   pods using the same image is a future optimisation.

3. **Containerd overlayfs snapshotter.** The blockfile snapshotter is no longer
   needed for the container rootfs path. We switch to containerd's native
   overlayfs snapshotter. A `View` of the final chain ID gives us a merged
   read-only directory that virtiofsd can serve.

4. **Block devices remain for writable state.** EmptyDir volumes and the overlay
   device stay as ext4 block devices. This keeps snapshot/restore simple for
   cross-host migration—writable files are shipped as opaque blobs with no
   filesystem-level sync needed.

5. **Config drive migration deferred.** The config drive (`initial_commands`) is
   not currently used in production (always `vec![]` in supervisor). The virtiofs
   migration for config drive is deferred until it's actually needed. The
   conditional device offset simplification still applies once ConfigData moves
   to virtiofs.

## Component Changes

### 1. Guest Protocol (`distvirt-guest-protocol`)

The `AddContainer` and `MountVolume` messages need to express virtiofs sources
alongside block device sources.

```rust
#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum HostMessage {
    AddContainer {
        id: String,
        /// How to obtain the container rootfs.
        rootfs: ContainerRootfs,
        #[serde(default)]
        dns_servers: Vec<String>,
        #[serde(default)]
        volume_mounts: Vec<VolumeMount>,
    },
    MountVolume {
        name: String,
        /// Where the volume data comes from.
        source: VolumeSource,
        read_only: bool,
    },
    // ... remaining variants unchanged
}

/// How the guest should set up the container root filesystem.
#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "mode")]
pub enum ContainerRootfs {
    /// Legacy: mount a block device directly as ext4 (writable).
    Device { device: String },
    /// Mount a read-only virtiofs tag as lower layer, use a block device
    /// for an overlayfs upper/work directory.
    VirtioFsOverlay {
        /// virtiofs tag to mount as read-only lower layer.
        tag: String,
        /// Block device for the overlay upper + work dirs.
        overlay_device: String,
    },
}

/// Where volume data comes from inside the guest.
#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "mode")]
pub enum VolumeSource {
    /// Block device (ext4).
    Device { device: String },
    /// virtiofs tag (typically read-only).
    VirtioFs { tag: String },
}
```

Using tagged enums with `#[serde(default)]` keeps the wire format backwards
compatible—old guests that see an unknown `mode` will fail with a clear
deserialization error rather than silent corruption.

### 2. Guest-Init (`guest-init`)

#### Container mounting (`container.rs` — `add()`)

Current flow:
```
mount(device, /containers/<id>, ext4, 0)
```

New flow for `VirtioFsOverlay`:
```
mkdir /mnt/rootfs-<id>
mount(tag, /mnt/rootfs-<id>, virtiofs, MS_RDONLY)

mount(overlay_device, /mnt/overlay-<id>, ext4, 0)
mkdir /mnt/overlay-<id>/upper
mkdir /mnt/overlay-<id>/work

mount(overlay, /containers/<id>, overlay, 0,
      lowerdir=/mnt/rootfs-<id>,
      upperdir=/mnt/overlay-<id>/upper,
      workdir=/mnt/overlay-<id>/work)
```

The `Device` variant keeps the existing single-mount path for backwards
compatibility.

#### Volume mounting (`session.rs` — `mount_volume()`)

Current flow:
```
mount(device, /volumes/<name>, ext4, flags)
```

New flow for `VirtioFs`:
```
mount(tag, /volumes/<name>, virtiofs, flags)
```

The `Device` variant keeps the existing ext4 mount.

#### Container cleanup (`container.rs` — `remove()`)

Currently unmounts `/containers/<id>`. With overlay, cleanup becomes:

```
umount /containers/<id>         # overlay
umount /mnt/overlay-<id>        # ext4 upper device
umount /mnt/rootfs-<id>         # virtiofs
```

#### Config drive (`config_drive.rs`)

**Deferred.** The config drive (`initial_commands`) is not currently used in
production — `initial_commands` is always `vec![]` in supervisor.rs. The
virtiofs migration for config drive will be addressed if/when it's needed.

#### Kernel requirements

The guest kernel needs these enabled (built-in, not modules—guest-init loads no
modules):

```
CONFIG_FUSE_FS=y
CONFIG_VIRTIO_FS=y
CONFIG_OVERLAY_FS=y
```

### 3. VMM Layer (`worker/vmm/`)

#### VmConfig changes (`mod.rs`)

```rust
pub struct VmConfig {
    pub kernel_path: PathBuf,
    pub rootfs_image_path: PathBuf,
    /// Small ext4 image for container overlay upper/work dirs.
    pub overlay_image_path: PathBuf,
    pub vcpu_count: u32,
    pub mem_size_mib: u32,
    pub net: Option<NetConfig>,
    pub serial_console: bool,
    pub balloon: Option<BalloonConfig>,
    /// Block device volumes (EmptyDir only). Attached as virtio-blk.
    pub additional_drives: Vec<AdditionalDrive>,
    /// virtiofs shares (rootfs, ConfigData volumes).
    /// Each entry spawns a virtiofsd process; socket created in tmpdir.
    pub virtiofs_mounts: Vec<VirtiofsMount>,
}

pub struct VirtiofsMount {
    /// Tag visible inside the guest (used in `mount -t virtiofs <tag> ...`).
    pub tag: String,
    /// Host directory to share.
    pub source_dir: PathBuf,
}
```

#### Cloud Hypervisor VM config JSON (`cloud_hypervisor.rs`)

Add an `fs` array alongside the existing `disks` array:

```json
{
  "fs": [
    {
      "tag": "container-rootfs",
      "socket": "/tmp/xxx/virtiofsd-rootfs.sock",
      "num_queues": 1,
      "queue_size": 1024
    },
    {
      "tag": "configdata-myconfig",
      "socket": "/tmp/xxx/virtiofsd-config-myconfig.sock",
      "num_queues": 1,
      "queue_size": 1024
    }
  ]
}
```

#### Block device assignment changes

Current:
- vda: guest OS rootfs
- vdb: container image (full writable copy)
- vdc: config drive (optional — shifts volume device letters when present)
- vdd+: volumes (EmptyDir + ConfigData)

New:
- vda: guest OS rootfs (unchanged)
- vdb: container overlay device (small empty ext4)
- vdc+: EmptyDir volumes only

No more conditional device offset — the config drive and ConfigData volumes
move to virtiofs, so EmptyDir volumes always start at vdc.

virtiofs tags:
- `container-rootfs`: container image layers (read-only)
- `configdata-<name>`: per-ConfigData-volume (read-only)

#### Launch flow changes

Current:
1. Copy rootfs.ext4 to tmpdir
2. Copy container.ext4 to tmpdir
3. Copy volume images to tmpdir
4. Spawn CH, create VM, boot

New:
1. Copy rootfs.ext4 to tmpdir (unchanged)
2. Create small empty overlay.ext4 in tmpdir (new, replaces full image copy)
3. Copy EmptyDir volume images to tmpdir (unchanged, but fewer now)
4. **Start virtiofsd processes** — one per virtiofs mount (new):
   - Container rootfs share
   - ConfigData volume shares
5. **Wait for virtiofsd sockets** to appear (new)
6. Spawn CH, create VM with `fs` array, boot

#### Snapshot flow changes

Current snapshot saves:
- `container.ext4` (full writable container image)
- `vol-*.ext4` (all volumes)
- CH state files

New snapshot saves:
- `overlay.ext4` (small overlay upper — replaces container.ext4)
- `vol-*.ext4` (EmptyDir only — ConfigData and config drive no longer block devices)
- CH state files (now includes virtiofsd state via vhost-user backend state)

#### Restore flow changes

Current:
1. Copy rootfs from original source
2. Copy container.ext4 from snapshot
3. Copy volumes from snapshot
4. Spawn CH, restore

New:
1. Copy rootfs from original source (unchanged)
2. Copy overlay.ext4 from snapshot (replaces container.ext4)
3. Copy EmptyDir volumes from snapshot (unchanged)
4. **Prepare virtiofs shares on destination** (new):
   - Container rootfs: ensure OCI image is unpacked, create containerd view, point virtiofsd at it
   - ConfigData: recreate config directories from snapshot metadata, point virtiofsd at them
5. **Start virtiofsd processes** (new)
6. **Patch CH `config.json`** (new): rewrite `fs[].socket` paths to point at the
   new virtiofsd sockets, similar to the existing `patch_snapshot_config_tap()`
   that rewrites `net[].tap` for fresh TAP devices. This may be combined into a
   single patching pass.
7. Spawn CH, restore (CH reconnects to virtiofsd sockets)

#### Snapshot metadata changes

```rust
pub struct SnapshotMetadata {
    pub kernel_path: PathBuf,
    pub rootfs_source_path: PathBuf,
    pub balloon_configured: bool,
    pub serial_console: bool,
    pub volume_drives: Vec<SnapshotVolumeDrive>,
    // NEW: information needed to reconstruct virtiofs shares on restore
    pub container_image_ref: String,
    pub virtiofs_config_volumes: Vec<SnapshotConfigVolume>,
}

pub struct SnapshotConfigVolume {
    pub name: String,
    pub files: Vec<ConfigDataFile>,
}
```

### 4. Image Provider (`image_provider/`)

#### PreparedArtifact changes

```rust
pub struct PreparedArtifact {
    /// Directory to share via virtiofs (read-only merged view of OCI layers).
    pub rootfs_dir: PathBuf,
    /// OCI image config, if available.
    pub oci_config: Option<ImageConfig>,
    /// OCI image reference (needed for snapshot metadata).
    pub image_ref: String,
    /// RAII cleanup handle — keeps containerd view alive, unmounts on drop.
    _cleanup: Box<dyn Any + Send>,
}
```

The `image_path` field (pointing to an ext4 file) is replaced by `rootfs_dir`
(pointing to a directory).

**Lifetime change:** The `PreparedArtifact` must now live for the **entire pod
lifetime**, not just the launch phase. Currently the artifact is consumed during
launch (the ext4 image is copied to tmpdir and the artifact can be dropped).
With virtiofs, virtiofsd serves directly from the containerd snapshot directory,
so the artifact (and its containerd lease) must remain alive as long as the pod
is running. The supervisor must store the `PreparedArtifact` alongside the
`ManagedVm`.

#### Containerd provider changes

Switch from blockfile snapshotter to overlayfs snapshotter:

1. Pull image and unpack layers (same as today, but with overlayfs snapshotter)
2. Create a **View** of the final chain ID snapshot
3. Mount the view's overlay on a temp mountpoint
4. Return the mountpoint path as `rootfs_dir`
5. On drop: unmount the overlay, remove the containerd view, drop lease

The `_cleanup` handle holds a struct that:
- Stores the mountpoint path
- Stores the view key (for containerd snapshot removal)
- Holds a `ContainerdLease` (keeps snapshot + layers protected from GC)
- On `Drop`: calls `umount()` then `snapshots.remove(view_key)`, then drops lease

#### LeaseManager changes

The existing `LeaseManager` creates leases with a 1-hour expiry as a crash
safety net. With virtiofs, containerd leases must live for the entire pod
lifetime, which can exceed 1 hour.

**Persistent leases:** The containerd lease API has no `Update` RPC, so lease
expiry labels cannot be renewed in-place. Instead of a complex
delete-recreate-migrate renewal loop, long-lived leases are created **without**
the `gc.expire` label via `create_persistent_lease()`.

- **Normal operation:** Dropped cleanly on pod shutdown via RAII.
- **Crash/SIGKILL:** No drop runs, lease persists until next worker startup.
  `cleanup_stale_leases()` is called at worker startup (in the
  `ContainerdOverlayfsProvider` constructor) and deletes all orphaned
  `distvirt-*` leases, making their resources eligible for containerd GC.

Short-lived leases (e.g. for the blockfile provider) retain the 1-hour expiry
via the existing `create_lease()` method.

#### RootfsDirProvider changes

The existing `RootfsDirProvider` currently builds an ext4 image from a
directory. With virtiofs, it can simply return the directory path directly
(the directory *is* the rootfs). The ext4 build step is no longer needed.

### 5. Volume Provisioning (`volume.rs`)

#### ConfigData volumes

No longer need ext4 images. Instead, create a temp directory with the config
files and return its path for virtiofs sharing.

```rust
pub enum PreparedVolume {
    /// Block device volume (EmptyDir, or future containerd-backed block volumes).
    Block {
        name: String,
        image_path: PathBuf,
        read_only: bool,
        /// RAII cleanup handle. Holds temp file ownership, or a containerd
        /// lease for containerd-backed block devices.
        _cleanup: Box<dyn Any + Send>,
    },
    /// Directory to share via virtiofs (ConfigData).
    VirtioFs {
        name: String,
        dir_path: PathBuf,
        read_only: bool,
        /// RAII cleanup handle. Holds TempDir ownership, or a containerd
        /// lease if the directory is a containerd snapshot.
        _cleanup: Box<dyn Any + Send>,
    },
}
```

Both variants use `Box<dyn Any + Send>` for the cleanup handle, keeping the
interface uniform. Today `Block._cleanup` holds a `NamedTempFile` or `()`, and
`VirtioFs._cleanup` holds a `TempDir`. In the future, either variant could hold
a `ContainerdLease` + view handle for containerd-backed resources.

#### EmptyDir volumes

Unchanged — still creates sparse ext4 images with `mkfs.ext4`.

### 6. Pod Supervisor (`worker/supervisor.rs`)

#### virtiofsd process management

The supervisor needs a new component: a `VirtiofsdManager` that:

1. Starts a virtiofsd process for each virtiofs share
2. Waits for the vhost-user socket to appear
3. Returns socket paths for the VmConfig
4. Kills all virtiofsd processes on pod cleanup

```rust
struct VirtiofsdProcess {
    child: tokio::process::Child,
    socket_path: PathBuf,
}

struct VirtiofsdManager {
    processes: Vec<VirtiofsdProcess>,
}

impl VirtiofsdManager {
    /// Start a virtiofsd for a read-only share.
    async fn add_readonly(
        &mut self,
        virtiofsd_bin: &Path,
        tag: &str,
        shared_dir: &Path,
        socket_dir: &Path,
    ) -> anyhow::Result<VirtiofsMount>;

    /// Kill all virtiofsd processes.
    async fn shutdown(&mut self);
}
```

virtiofsd invocation:
```
virtiofsd \
  --socket-path=/tmp/xxx/virtiofsd-rootfs.sock \
  --shared-dir=/path/to/containerd/snapshot/merged \
  --readonly \
  --sandbox=none \
  --migration-mode=find-paths \
  --migration-on-error=abort
```

Flags explained:
- `--readonly`: Container rootfs and ConfigData are read-only
- `--sandbox=none`: We're already in a confined environment, don't double sandbox
- `--migration-mode=find-paths`: Good for read-only shares (no DAC_READ_SEARCH capability required)
- `--migration-on-error=abort`: Fail loudly rather than silently presenting errors to guest

#### Overlay device creation

The supervisor creates a small empty ext4 image for the overlay device. This
replaces the full container image copy:

```rust
// Create overlay device — small empty ext4
let overlay_path = vol_tmpdir.path().join("overlay.ext4");
create_empty_ext4(&overlay_path, overlay_size_mb).await?;
```

Default size: configurable, with a reasonable default (e.g. 256 MB). Most
containers write very little at runtime (logs, tmp files, pid files).

#### Updated launch flow

```
1. image_provider.prepare(image_ref)
   → returns rootfs_dir (directory path), oci_config, image_ref

2. Create overlay.ext4 (small empty ext4)

3. Prepare volumes:
   - EmptyDir → ext4 image (unchanged)
   - ConfigData → temp directory with files (new, no mkfs)

4. Start virtiofsd processes:
   - virtiofsd for rootfs_dir → socket path
   - virtiofsd per ConfigData dir → socket paths

5. Build VmConfig:
   - overlay_image_path = overlay.ext4
   - additional_drives = [EmptyDir volumes only]
   - virtiofs_mounts = [rootfs, ConfigData sockets]

6. vmm.launch(&vm_config)

7. Guest setup over vsock:
   - configure_network(...)
   - mount_volume(EmptyDir volumes → Device source)
   - mount_volume(ConfigData volumes → VirtioFs source)
   - add_container(rootfs = VirtioFsOverlay { tag, overlay_device })
   - start_container(...)
```

The device offset calculation in supervisor.rs simplifies from:
```rust
// Before: conditional offset
let vol_device_offset: u8 = 2 + if vm_config.initial_commands.is_empty() { 0 } else { 1 };
let device = format!("/dev/vd{}", (b'a' + vol_device_offset + i as u8) as char);
```
to:
```rust
// After: fixed offset (vda=rootfs, vdb=overlay, vdc+=EmptyDir)
let device = format!("/dev/vd{}", (b'c' + i as u8) as char);
```

### 7. Snapshot/Restore with Cross-Host Migration

#### Container rootfs (read-only virtiofs)

**Snapshot**: No data to save—the virtiofs share is read-only and its contents
are the OCI image layers. The snapshot metadata records `container_image_ref`.

**Restore on destination**:
1. `image_provider.prepare(container_image_ref)` — ensures image is pulled and
   unpacked on the destination worker
2. Start virtiofsd pointing at the new snapshot directory
3. virtiofsd's `--migration-mode=find-paths` reconstructs its internal state
   from paths in the CH snapshot. Since the image content is identical, all
   paths resolve successfully.

#### Container writes (overlay block device)

**Snapshot**: Copy `overlay.ext4` from tmpdir to snapshot directory (same
pattern as current `container.ext4`).

**Restore**: Copy from snapshot to new tmpdir (same as today). The overlay
upper captures exactly the diff from the base image.

#### ConfigData volumes (read-only virtiofs)

**Snapshot**: Save the `ConfigDataFile` list in snapshot metadata.

**Restore**: Recreate the config directory from the file list, start virtiofsd.
Content is identical by construction.

#### EmptyDir volumes (block device)

Unchanged — copied in snapshot, restored from snapshot.

## Device Assignment Summary (post-change)

### Block devices (virtio-blk)
```
vda  → guest OS rootfs (ext4, re-copied from source on restore)
vdb  → container overlay upper/work (ext4, small, shipped in snapshot)
vdc+ → EmptyDir volumes (ext4, shipped in snapshot)
```

### virtiofs shares
```
container-rootfs       → OCI image merged view (read-only)
configdata-<name>      → config files directory (read-only)
```

## Migration Safety Analysis

| Share | Read-only? | Same content on src/dst? | virtiofsd migration mode | Risk |
|---|---|---|---|---|
| Container rootfs | Yes | Yes (same OCI image) | `find-paths` | Low — paths are stable, no writes |
| ConfigData | Yes | Yes (same file list) | `find-paths` | Low — small static directories |
| Overlay device | N/A (block) | N/A | N/A | None — shipped as file |
| EmptyDir | N/A (block) | N/A | N/A | None — shipped as file |

The only virtiofs shares are read-only with identical content on both sides.
This is the simplest and safest virtiofsd migration configuration per the
upstream docs.

## Implementation Order

### Phase 1: Guest-init virtiofs support ✓
- ✓ Add `ContainerRootfs` and `VolumeSource` enums to guest protocol
- ✓ Implement virtiofs + overlayfs mount path in guest-init `container.rs`
- ✓ Implement virtiofs volume mount path in guest-init `session.rs`
- ✓ Add overlay cleanup to `container.rs` `remove()`
- ✓ Update worker-side callers (`managed_vm.rs`, `supervisor.rs`) to use new
  enum types (currently wired to `Device` variants; will switch to virtiofs in
  later phases)
- Update guest kernel config with `FUSE_FS`, `VIRTIO_FS`, `OVERLAY_FS`
- **Test**: unit tests with mocked mounts

### Phase 2: Worker virtiofsd management & supervisor wiring ✓
- ✓ Add `VirtiofsMount` to `VmConfig` and `virtiofs_mounts` field
- ✓ Add `VirtiofsdProcess` struct with Drop-based kill (same pattern as CH
  child process)
- ✓ Spawn virtiofsd processes in `launch()` before `vm.create` (CH needs
  sockets at create time), store in `CloudHypervisorInstance`
- ✓ Add `fs` array to CH VM config JSON
- ✓ Add `virtiofsd_bin` path to `CloudHypervisor` struct
- ✓ Persist `SnapshotVirtiofsMount` in `SnapshotMetadata`; `restore()` respawns
  virtiofsd processes from metadata before `vm.restore`
- ✓ Create overlay device (small empty ext4, 256 MB) instead of copying full image
- ✓ Remove `container_image_path` and `initial_commands` from `VmConfig`,
  replace with `overlay_image_path`
- ✓ Update all VMM backends (cloud_hypervisor, firecracker, qemu, test_vmm)
- ✓ Supervisor creates overlay image, populates `virtiofs_mounts` with
  container rootfs share + ConfigData volumes
- ✓ Supervisor uses `ContainerRootfs::VirtioFsOverlay` for container setup
- ✓ Simplified device offset: `vdc+` for EmptyDir (no conditional config drive)
- ✓ `PreparedArtifact` returns `rootfs_dir` (directory) instead of `image_path`.
  `RootfsDirProvider` returns directory directly (no ext4 build).
  `ContainerdBlockfileProvider` still returns ext4 path (needs Phase 3 rewrite).
- ✓ `PreparedVolume` is now an enum with `Block` (EmptyDir) and `VirtioFs`
  (ConfigData) variants. ConfigData creates a temp directory, not ext4.
- ✓ Supervisor sends `VolumeSource::VirtioFs` for ConfigData volumes
- Update guest kernel config with `FUSE_FS`, `VIRTIO_FS`, `OVERLAY_FS`
- **Test**: integration test with real virtiofsd + CH

### Phase 3: Containerd image provider switch ✓
- ✓ Implement `ContainerdOverlayfsProvider` (`containerd_overlayfs.rs`): uses
  overlayfs snapshotter, creates a View, mounts the overlay on a temp
  directory, returns the mounted directory as `rootfs_dir`. Old
  `ContainerdBlockfileProvider` kept for reference.
- ✓ RAII cleanup struct (`OverlayfsCleanup`): on drop, unmounts the overlay,
  spawns async task to remove the containerd view, then drops the lease.
- ✓ `create_overlayfs_view()` in `snapshot.rs`: creates a View via the
  overlayfs snapshotter and returns mount descriptors + view key.
- ✓ `LeaseManager::create_persistent_lease()`: creates leases without
  `gc.expire` for long-lived use. `cleanup_stale_leases()` deletes orphaned
  `distvirt-*` leases at startup (called in provider constructor).
- ✓ `PodResources` struct in supervisor: holds `PreparedArtifact`,
  `PreparedVolume`s, and `vol_tmpdir` for the entire pod lifetime. Threaded
  through `run_pod_supervisor` as `Box<dyn Any + Send>`.
- ✓ `main.rs` wired to `ContainerdOverlayfsProvider`.
- **Test**: verify image preparation returns valid directory, startup cleanup works

### Phase 4: Snapshot/restore ✓
- ✓ `SnapshotMetadata` extended with `container_image_ref: Option<String>` and
  `config_volumes: Vec<SnapshotConfigVolume>` (with `#[serde(default)]` for
  backwards compat). New `SnapshotConfigVolume` struct stores name, tag, and
  `ConfigDataFile` list.
- ✓ `VmConfig` extended with same fields, threaded through
  `CloudHypervisorInstance` to `snapshot()` metadata.
- ✓ Snapshot already saves `overlay.ext4` (done in Phase 2).
- ✓ `patch_snapshot_config_fs()` rewrites `fs[].socket` paths in CH
  `config.json` on restore, deriving new socket paths from the `tag` field.
  Called in `restore()` after spawning virtiofsd.
- ✓ `pod_resume_supervisor` now accepts `image_provider`, prepares container
  rootfs via `image_provider.prepare()` and recreates ConfigData directories
  from snapshot metadata. Updates `virtiofs_mounts` source_dirs before calling
  `vmm.restore()`. Holds `ResumeResources` for pod lifetime.
- ✓ `prepare_config_volumes_from_snapshot()` helper in `volume.rs`.
- **Test**: snapshot + cross-tmpdir restore, then full cross-host

### Phase 5: Cleanup
- Remove blockfile snapshotter dependency
- Remove ext4 image building for container rootfs (`image.rs`)
- Remove `mke2fs`-based ConfigData image creation (already done)
- Clean up dead code (`build_ext4_image`, `mount`/`umount_detach` in linux/mount.rs)

## Open Questions

1. ~~**Overlay device default size.**~~ **Resolved:** Hardcoded at 256 MB for now.
   Making it configurable per-pod is a future enhancement.

2. ~~**virtiofsd binary path.**~~ **Resolved:** Added as `virtiofsd_bin` field
   on the `CloudHypervisor` struct.

3. **Shared virtiofsd for same image.** Multiple pods using the same OCI image
   could share one virtiofsd process. This reduces process count but adds
   reference counting and lifecycle complexity. Worth doing in a follow-up.

4. **DAX window.** Cloud Hypervisor supports a DAX window for virtiofs that
   allows the guest to memory-map shared files directly, bypassing FUSE
   overhead. Worth benchmarking, but adds complexity to memory accounting
   (balloon, cgroup limits). Defer to follow-up.

5. **`--sandbox` mode.** Using `--sandbox=none` is simplest. Using
   `--sandbox=chroot` would add isolation but complicates the setup. Since
   distvirt targets staging environments, `none` is acceptable.
