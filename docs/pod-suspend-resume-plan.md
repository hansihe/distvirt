# Pod Suspend/Resume Implementation Plan

## Context

Adding suspend/resume to distvirt-worker enables scale-to-zero with fast restore (~5-10ms vs ~100ms+ cold boot). This is also the foundation for live migration (not implemented here, but the design accounts for it). The design doc is at `docs/snapshots-migration.md`.

## Architecture Overview

Suspend path: orchestrator sends `SuspendPod` → worker tells guest to prepare → Firecracker pauses vCPUs → snapshot to disk → kill VM → emit `PodSuspended`.

Resume path: orchestrator sends `ResumePod` → worker recreates TAP → starts new Firecracker with snapshot load → reconnects vsock → guest re-accepts connection → emit `PodRunning`.

## Implementation Steps

### Step 1: Use relative drive paths and Firecracker working directory ✅

**File:** `distvirt-worker/src/vmm/firecracker.rs`

Set Firecracker's **working directory** to the tmpdir and use **relative paths** for drives. This way, the snapshot stores relative paths, and on restore we just need the same file layout in the new Firecracker's working directory — no path patching needed, and clones work naturally (each gets its own tmpdir with its own copies).

Changes to `launch()`:
- Set `cmd.current_dir(tmpdir.path())` on the Firecracker process
- Use `"./rootfs.ext4"` for rootfs path_on_host
- Copy the container image into tmpdir as `container.ext4`, use `"./container.ext4"` for container path_on_host
- Use `"./vsock.sock"` for vsock uds_path
- Kernel path stays absolute (only used for boot, not stored in snapshot)

This is a refactor of the existing launch path, not a new feature. It makes all per-VM state self-contained in the tmpdir.

**No named TAP creation needed.** Firecracker's `PUT /snapshot/load` supports a [`network_overrides`](https://github.com/firecracker-microvm/firecracker/blob/main/docs/snapshotting/network-for-clones.md) parameter that remaps TAP devices at restore time. So we create a fresh TAP with any auto-assigned name and pass it as an override. This works for both resume and clones.

### Step 2: Firecracker snapshot/restore API ✅

**Files:** `distvirt-worker/src/vmm/mod.rs`, `distvirt-worker/src/vmm/firecracker.rs`

**New types in `vmm/mod.rs`:**
- `SnapshotArtifacts` — snapshot dir path, sizes. The dir has a known layout (see below).
- `SnapshotMetadata` (serde) — persisted as `metadata.json` in snapshot dir; includes original source paths (kernel, rootfs image) needed to reconstruct the VM environment on restore

**Snapshot directory layout** (self-contained, clonable):
```
<snapshot_dir>/
  metadata.json        # SnapshotMetadata (rootfs source path, kernel path, sizes)
  snapshot.bin          # Firecracker device state
  mem.bin               # VM memory dump
  container.ext4        # Container drive (copy with runtime writes)
```
Note: rootfs is NOT in the snapshot dir — it's a shared read-only image, re-copied from source on restore.

**New trait methods:**
- `VmInstance::snapshot(&mut self, snapshot_dir: &Path) -> Result<SnapshotArtifacts>` — pauses vCPUs, creates snapshot, copies container disk
- `Vmm::restore(&self, snapshot: &SnapshotArtifacts) -> Result<Self::Instance>` — starts new Firecracker, loads snapshot with network_overrides

Add these directly to the existing `Vmm`/`VmInstance` traits with default methods that return "not supported" errors. Implement on `Firecracker`/`FirecrackerInstance`.

**Drive layout recap:**
- `rootfs` (`/drives/rootfs`): Common guest OS image (guest-init, etc). Copied into tmpdir at launch. Effectively read-only. On restore, re-copy from original source.
- `container` (`/drives/container`): Container filesystem ext4, writable. Runtime writes live here. Copied into snapshot dir.

**Firecracker snapshot flow (`snapshot` on FirecrackerInstance):**
1. `PUT /vm {"state":"Paused"}` — pause vCPUs
2. `PUT /snapshot/create` with relative paths (`./snapshot.bin`, `./mem.bin`) — Firecracker writes to its cwd (which is the tmpdir from Step 1)
3. Copy `container.ext4` from tmpdir into snapshot dir
4. Copy `snapshot.bin` and `mem.bin` from tmpdir into snapshot dir
5. Write `metadata.json` with rootfs source path, kernel path, sizes
6. Return `SnapshotArtifacts`
7. Caller kills VM process after this returns

**Firecracker restore flow (`restore()`):**
1. Create new tmpdir
2. Copy rootfs from original source path (from metadata) into tmpdir as `rootfs.ext4`
3. Copy `container.ext4` from snapshot dir into tmpdir
4. Copy `snapshot.bin` and `mem.bin` from snapshot dir into tmpdir
5. Create fresh TAP with auto-assigned name, bring it up
6. Spawn `firecracker --api-sock ./firecracker.sock` with **cwd = tmpdir**
7. Wait for API socket
8. `PUT /snapshot/load` with:
   - `snapshot_path: "./snapshot.bin"`, `mem_backend.backend_path: "./mem.bin"`
   - `network_overrides: [{"iface_id": "eth0", "host_dev_name": "<new_tap_name>"}]`
   - `resume_vm: true`
9. Open AF_PACKET socket on the new TAP
10. Return `FirecrackerInstance`

**Why this works for clones:** Each restore gets its own tmpdir with its own file copies and a fresh TAP with a unique auto-assigned name. The `network_overrides` parameter remaps the guest's `eth0` to whatever TAP name we created. No path conflicts, no TAP name conflicts. Multiple clones from the same snapshot "just work."

**Reference:** [Firecracker network-for-clones docs](https://github.com/firecracker-microvm/firecracker/blob/main/docs/snapshotting/network-for-clones.md)

### Step 3: Guest protocol — PrepareSuspend handshake and reconnect loop ✅

**File:** `distvirt-guest-protocol/src/lib.rs`

Add to `HostMessage`: `PrepareSuspend` — tells guest to flush output buffers.
Add to `GuestMessage`: `SuspendReady` — guest has flushed and is ready for vCPU freeze.

**File:** `guest-image/guest-init/src/main.rs`

Currently the guest-init accepts ONE vsock connection, runs the main loop, and on disconnect shuts down the VM (SIGTERM containers → SIGKILL → reboot). This needs to change for resume support.

**Changes:**
1. Wrap the accept + main-loop in an **outer loop**. On yamux disconnect, instead of shutting down, loop back to `listener.accept()`.
2. Add `PrepareSuspend` handling in `handle_message()`: flush all container output streams, send `SuspendReady`.
3. On reconnect (second iteration of outer loop), send `GuestMessage::Ready` (same as cold boot — the host distinguishes resume from cold boot by context, not by message type). Include running container state so the host knows what's still alive.
4. Keep containers running across reconnect iterations — only shut them down on `Shutdown` message or fatal error.

**Key detail:** The guest uses `async-executor` + `async-io` (not tokio). The reconnect loop will use the same executor. The yamux driver future from the previous session is dropped, which cleans up the old connection.

### Step 4: ManagedVm suspend/reconnect methods ✅

**File:** `distvirt-worker/src/managed_vm.rs`

**`suspend(&mut self, snapshot_dir: &Path, timeout: Duration) -> Result<SnapshotArtifacts>`:**
1. Send `PrepareSuspend` to guest
2. Wait for `SuspendReady` (with timeout)
3. Call `self.instance.snapshot(snapshot_dir)` (pauses vCPUs, writes files)
4. Kill VM process
5. Return artifacts

**`reconnect(instance: I) -> Result<(Self, TaskHandle)>`:**
Same as `connect()` but used after restore. The guest re-accepts the vsock connection and sends `Ready`. We reuse `connect()` directly — it already does `connect_vsock()` → yamux → wait for `Ready`. No separate method needed.

So actually: `connect()` works for both cold boot and resume. The only difference is that on resume, the guest's containers are already running. The host tracks this from before the suspend (it knows which containers were started).

### Step 5: Worker protocol additions ✅

**Files:** `distvirt-worker-protocol/src/lib.rs` (types), `distvirt-worker-protocol/schema/worker_protocol.capnp` (schema), `distvirt-worker-protocol/src/convert.rs` (serialization)

**New commands:**
- `SuspendPod { namespace_id, pod_id, snapshot_id: String }`
- `ResumePod { namespace_id, pod_id, snapshot_id: String, network: PodNetworkConfig }`
- `DeleteSnapshot { snapshot_id: String }`

**New events:**
- `PodSuspended { namespace_id, pod_id, snapshot_id, snapshot_size_bytes: u64 }`
- `PodSuspendFailed { namespace_id, pod_id, error: String }`

Follow existing patterns for Cap'n Proto schema and conversion code.

### Step 6: Supervisor and worker integration ✅

**File:** `distvirt-worker/src/worker/supervisor.rs`

**Suspend integration:** The pod monitor (`pod_monitor()`) `select!`s on container exit, yamux death, port death, cancel token, and now a suspend request channel.

- `SuspendRequest` struct with `snapshot_id`, `snapshot_dir`, and a `oneshot::Sender` for the result.
- `PodState` has an `mpsc::Sender<SuspendRequest>` (capacity 1), created in `handle_launch_pod` before spawning the supervisor.
- The receiver is passed into `pod_supervisor` → `pod_monitor`.
- When suspend request arrives in monitor: calls `vm.suspend()`, calculates snapshot size via `dir_size()`, sends artifacts back via oneshot, emits `PodSuspended` event, exits monitor (VM is dead). On failure, emits `PodSuspendFailed` and force-kills the VM.

**Resume integration:** `pod_resume_supervisor()` calls `vmm.restore()` instead of `vmm.launch()`, adds TAP to fabric, calls `ManagedVm::connect()`, emits `PodRunning`, then enters the same `pod_monitor()` loop. Skips container setup (containers already running in restored VM). Helper `pod_restore()` encapsulates the fallible restore logic.

**File:** `distvirt-worker/src/worker/mod.rs`

- `snapshot_base_dir: PathBuf` on worker state (created as `$TMPDIR/distvirt-snapshots-<pid>` in `new()`).
- `handle_suspend_pod()`: looks up pod, clones its `suspend_tx`, sends `SuspendRequest` with `snapshot_dir = snapshot_base_dir/<snapshot_id>`, awaits oneshot reply. Emits `PodSuspendFailed` if pod not found or supervisor already exited.
- `handle_resume_pod()`: reads `metadata.json` from snapshot dir, constructs `SnapshotArtifacts`, registers pod MAC with WireGuard adapter, spawns `pod_resume_supervisor()`.
- `handle_delete_snapshot()`: `tokio::fs::remove_dir_all(snapshot_base_dir/<snapshot_id>)`.
- Background event loop cleans up `PodSuspended` and `PodSuspendFailed` events (removes finished pod from namespace, same as `PodExited`/`PodFailed`).

**File:** `distvirt-worker/src/worker/namespace.rs`

No structural changes needed — pods are already tracked in a map. Suspended pods are removed from the map (VM is dead). The orchestrator tracks snapshot state.

**E2E test:** `test_suspend_resume_pod` in `distvirt-worker/tests/e2e.rs` — launches a long-running pod, suspends it, verifies `PodSuspended` with non-zero snapshot size, resumes with same snapshot, verifies `PodRunning`, stops, and cleans up via `DeleteSnapshot`.

## Migration and clone alignment

This design naturally supports future live migration and cloning:
- **SuspendPod** on source worker produces a self-contained snapshot directory
- A future **TransferSnapshot** command streams that directory to the target worker
- **ResumePod** on target worker restores from transferred snapshot
- The `network` parameter on `ResumePod` allows assigning a different IP/MAC on the target
- Relative drive paths + `network_overrides` mean no path/TAP conflicts — each restore is independent
- **Clones** work by simply restoring the same snapshot dir multiple times, each getting its own tmpdir and fresh TAP

## Verification

1. **Unit tests:** Mock VmInstance implementing snapshot() that writes dummy files. Test ManagedVm::suspend() handshake sequence.
2. **E2E test** (via `./distvirt-worker/tests/run-e2e.sh`):
   - Launch a pod running a simple echo server
   - Suspend it — verify snapshot files exist on disk
   - Resume it — verify PodRunning event
   - Send traffic to the resumed pod — verify the echo server responds
   - This validates the full stack: Firecracker snapshot/restore, TAP recreation, guest reconnect, fabric re-integration

## Suggested implementation order

Steps 1, 3, and 5 can be done in parallel (independent files). Step 2 depends on 1. Step 4 depends on 2+3. Step 6 depends on 4+5.

Practically: **1 → 2 → 3 → 4 → 5 → 6**, doing them sequentially so each layer can be tested before building on it. Step 1 is a refactor of existing code (no new features), so it's a safe starting point.
