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

> **Updated by Phase 5.5 refactoring.** The VMM now owns the entire rootfs
> attachment pipeline: unpacking, view creation, mounting, virtiofsd, overlay
> device creation, and device assignment. The supervisor passes a high-level
> `VmConfig` and receives `LaunchResult` instructions for the guest.

#### VmConfig (high-level, VMM-agnostic)

```rust
pub struct VmConfig {
    pub kernel_path: PathBuf,
    pub rootfs_image_path: PathBuf,
    pub vcpu_count: u32,
    pub mem_size_mib: u32,
    pub net: Option<NetConfig>,
    pub serial_console: bool,
    pub balloon: Option<BalloonConfig>,
    /// Container image — VMM decides how to expose to guest.
    pub container_image: PreparedArtifact,
    /// Volumes — VMM decides attachment mechanism.
    pub volumes: Vec<VmVolume>,
    /// Context persisted by VMM in snapshot metadata.
    pub snapshot_context: SnapshotContext,
}

pub struct VmVolume {
    pub name: String,
    pub source: VmVolumeSource,
    pub read_only: bool,
}

pub enum VmVolumeSource {
    BlockImage { image_path: PathBuf },
    Directory { dir_path: PathBuf },
}
```

#### LaunchResult (VMM → supervisor)

The VMM returns guest-facing instructions. The supervisor relays these to the
guest without interpreting device names or tags:

```rust
pub struct LaunchResult {
    pub container_rootfs: ContainerRootfs,
    pub volume_mounts: Vec<VolumeMountInstruction>,
}
```

#### RestoreContext (supervisor → VMM)

```rust
pub struct RestoreContext {
    pub net: Option<NetConfig>,
    pub container_image: Option<PreparedArtifact>,
    pub config_volumes: Vec<SnapshotConfigVolume>,
}
```

#### Vmm trait

```rust
pub trait Vmm: Send + Sync {
    type Instance: VmInstance;
    fn launch(&self, config: VmConfig)
        -> impl Future<Output = Result<(Self::Instance, LaunchResult)>> + Send;
    fn restore(&self, snapshot: &SnapshotArtifacts, ctx: RestoreContext)
        -> impl Future<Output = Result<Self::Instance>> + Send;
}
```

`launch` takes `VmConfig` by value — owns `PreparedArtifact` (which contains
the containerd lease for the `Containerd` variant).

#### Cloud Hypervisor launch pipeline (internal)

Cloud Hypervisor's `launch()` handles the full pipeline internally:

1. Match on `PreparedArtifact::Containerd` / `Directory`
2. For Containerd: `ensure_unpacked_with_gc_labels()` → `create_overlayfs_view()`
   → `mount_containerd_mounts()` → `spawn_virtiofsd("container-rootfs", ...)`
3. For Directory: `spawn_virtiofsd("container-rootfs", path)`
4. Create overlay.ext4 (256 MB) in tmpdir
5. Process volumes: BlockImage → copy + disk, Directory → virtiofsd
6. Assign devices internally: vda=rootfs, vdb=overlay, vdc+=block volumes
7. Build CH config JSON with `fs` array, create+boot VM
8. Return `LaunchResult` with guest-facing device names and tags

#### Cloud Hypervisor restore pipeline (internal)

1. If `ctx.container_image` is `Some(Containerd)`: unpack, view, mount, virtiofsd
2. Recreate ConfigData volumes from `ctx.config_volumes` (calls
   `volume::prepare_config_volumes_from_snapshot()` internally, owns TempDirs)
3. Spawn virtiofsd for each config volume
4. Patch CH config.json socket paths + TAP name
5. Restore CH

#### CloudHypervisorInstance cleanup

```rust
pub struct CloudHypervisorInstance {
    child: tokio::process::Child,        // killed first (field order = drop order)
    _virtiofsd_processes: Vec<VirtiofsdProcess>,
    _overlayfs_cleanup: Option<OverlayfsCleanup>,  // unmount + remove view
    _lease: Option<ContainerdLease>,                // keeps blobs alive
    _config_vol_tmpdirs: Vec<TempDir>,              // config volume directories
    // ... snapshot metadata fields, API socket, etc.
}
```

Drop ordering ensures: CH killed → virtiofsd killed → overlayfs unmounted →
view removed → lease dropped.

#### Shared virtiofs module (`vmm/virtiofs.rs`)

`VirtiofsdProcess` and `spawn_virtiofsd` are extracted into a shared module
reusable by any VMM backend.

### 4. Image Provider (`image_provider/`)

> **Updated by Phase 5.5 refactoring.** The image provider boundary is now
> "image pulled" — it only pulls, resolves manifest, and extracts OCI config.
> The VMM handles unpacking, view creation, and mounting.

#### PreparedArtifact (enum)

```rust
pub enum PreparedArtifact {
    /// Image pulled in containerd. VMM handles unpack + view + mount.
    Containerd {
        image_ref: String,
        oci_config: Option<ImageConfig>,
        resolved: ResolvedImage,
        lease: ContainerdLease,
    },
    /// Local directory (testing, development, legacy blockfile).
    Directory {
        path: PathBuf,
        oci_config: Option<ImageConfig>,
        _cleanup: Option<Box<dyn Any + Send>>,
    },
}
```

The `Containerd` variant carries the resolved image metadata and lease. The VMM
calls utility functions (`ensure_unpacked_with_gc_labels()`,
`create_overlayfs_view()`, `mount_containerd_mounts()`) to materialize the
rootfs during launch. This allows future VMMs to choose different snapshotters
(e.g. devmapper → block device, no virtiofsd needed).

**Lifetime:** The `ContainerdLease` is transferred into the VMM instance via
`VmConfig`. The VMM holds it for the VM's lifetime. The containerd view cleanup
(`OverlayfsCleanup`) also lives in the VMM instance.

#### ContainerdOverlayfsProvider

`prepare()` now only:
1. Creates a persistent lease
2. Pulls image if not present locally (`ensure_image()`)
3. Resolves manifest + config (`ResolvedImage::resolve()`)
4. Extracts OCI config + passwd/group from layer tarballs

It does NOT unpack layers, create views, or mount anything. Returns
`PreparedArtifact::Containerd { image_ref, oci_config, resolved, lease }`.

The `ContainerdOverlayfsProvider` shares its containerd channel with the VMM
via `CloudHypervisor::ContainerdConfig`.

#### Shared utility: `ensure_unpacked_with_gc_labels()`

Combines `ensure_unpacked()` + `set_snapshot_gc_label()` into a single utility
called by the VMM during launch. Idempotent.

#### LeaseManager, RootfsDirProvider

Unchanged from original design. `LeaseManager` still creates persistent leases
(called by the image provider). `RootfsDirProvider` returns
`PreparedArtifact::Directory`.

### 5. Volume Provisioning (`volume.rs`)

#### PreparedVolume (updated)

```rust
pub enum PreparedVolume {
    Block { name: String, image_path: PathBuf, read_only: bool },
    Directory { name: String, dir_path: PathBuf, read_only: bool,
                _cleanup: Box<dyn Any + Send + Sync> },
}
```

Renamed from `VirtioFs` to `Directory` — the volume layer doesn't know or care
whether the directory becomes virtiofs or something else. The `tag` field was
removed (VMM assigns tags internally).

`to_vm_volume()` helper converts to `VmVolume` for passing to the VMM.

#### EmptyDir volumes

Unchanged — still creates sparse ext4 images with `mkfs.ext4`.

### 6. Pod Supervisor (`worker/supervisor.rs`)

> **Updated by Phase 5.5 refactoring.** The supervisor no longer knows about
> device names, virtiofs tags, overlay images, or volume categorization. It
> builds a high-level `VmConfig` and follows the VMM's `LaunchResult`.

#### Updated launch flow

```
1. image_provider.prepare(image_ref)
   → returns PreparedArtifact (Containerd or Directory)

2. Prepare volumes:
   - EmptyDir → ext4 image (unchanged)
   - ConfigData → temp directory with files

3. Build VmConfig:
   - container_image = artifact (moved into VmConfig)
   - volumes = [VmVolume from each PreparedVolume]
   - snapshot_context = { container_image_ref, config_volumes }

4. vmm.launch(vm_config)
   → VMM handles: unpack, view, mount, virtiofsd, overlay, device assignment
   → returns (instance, LaunchResult)

5. Guest setup over vsock (following LaunchResult):
   - configure_network(...)
   - for mount in launch_result.volume_mounts: mount_volume(...)
   - add_container(launch_result.container_rootfs, ...)
   - start_container(...)
```

#### PodResources (unified)

```rust
struct PodResources {
    _prepared_volumes: Vec<PreparedVolume>,
    _vol_tmpdir: Option<TempDir>,
}
```

Used for both launch and resume paths. The artifact/lease is now owned by the
VMM instance — no longer held in PodResources.

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
- ✓ Snapshot metadata populated from `VmConfig.snapshot_context` fields stored
  in `CloudHypervisorInstance`.
- ✓ Snapshot already saves `overlay.ext4` (done in Phase 2).
- ✓ `patch_snapshot_config_fs()` rewrites `fs[].socket` paths in CH
  `config.json` on restore, deriving new socket paths from the `tag` field.
  Called in `restore()` after spawning virtiofsd.
- ✓ `pod_resume_supervisor` passes `RestoreContext` to `vmm.restore()`.
  VMM handles virtiofs reconstruction (image unpack + view + mount) and
  config volume recreation internally.
- ✓ `prepare_config_volumes_from_snapshot()` helper in `volume.rs` (called
  by VMM during restore).
- **Test**: snapshot + cross-tmpdir restore, then full cross-host

### Phase 5.5: VMM boundary refactoring ✓

Moved rootfs/volume attachment responsibility from the supervisor into the VMM.
The boundary between image provider and VMM is now "image pulled" — the image
provider only pulls/resolves/extracts OCI config, the VMM handles everything
else (unpack, view creation, mounting, virtiofsd, overlay, device assignment).

- ✓ Extracted `VirtiofsdProcess` + `spawn_virtiofsd` into shared
  `vmm/virtiofs.rs` module
- ✓ Made containerd utility functions (`mount_containerd_mounts`,
  `create_overlayfs_view`, etc.) accessible to VMM code
- ✓ Created `ensure_unpacked_with_gc_labels()` shared utility
- ✓ `PreparedArtifact` is now an enum (`Containerd` / `Directory`). The
  `Containerd` variant carries `ResolvedImage` + `ContainerdLease`, not a
  pre-materialized directory
- ✓ `VmConfig` is high-level: `container_image: PreparedArtifact`,
  `volumes: Vec<VmVolume>`, `snapshot_context: SnapshotContext`. No overlay
  path, no drives list, no virtiofs mounts list
- ✓ `Vmm::launch()` returns `(Instance, LaunchResult)` — VMM tells supervisor
  how to configure the guest
- ✓ `Vmm::restore()` takes `RestoreContext` — VMM handles virtiofs
  reconstruction and config volume recreation internally
- ✓ CloudHypervisor handles full pipeline: unpack → view → mount → virtiofsd →
  overlay → device assignment → LaunchResult
- ✓ Supervisor dramatically simplified: builds high-level VmConfig, follows
  LaunchResult for guest setup
- ✓ `PodResources` / `ResumeResources` unified into single `PodResources`
- ✓ `PreparedVolume::VirtioFs` renamed to `Directory`, tag field removed (VMM
  assigns tags)
- ✓ `ContainerdOverlayfsProvider` simplified: only pulls + resolves + OCI
  config extraction (no unpack, no view creation, no mounting)
- ✓ Containerd connection shared between image provider and VMM via
  `CloudHypervisor::ContainerdConfig`
- **Test**: 584 workspace tests passing

### Phase 5: Cleanup
- Remove `image_provider/image.rs` (`build_ext4_image` — dead code)
- Remove blockfile snapshotter dependency (if no longer needed for e2e tests)
- Clean up dead code
- Consider removing `containerd_blockfile.rs` or migrating e2e tests to
  overlayfs provider
- Split `pod_launch()` / `pod_monitor()` into smaller functions (deferred from
  Phase 5.5)

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
