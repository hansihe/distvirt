# Worker Crate Post-Refactor Review

Findings from code review of VMM, pod, supervisor, volume, and image provider code.

## Architectural Issues

### 1. ~~`pod_monitor()` is a 17-branch `select!` — hard to reason about~~ DONE
**File:** `src/worker/supervisor.rs`

**Refactored** into a two-phase structure:
1. **Event loop** — a flat `select!` → `match` mapping each signal source to a `PodEvent` enum. Guest state processing delegated to `StateTracker::process()`. Loop breaks with a typed `LoopOutcome`.
2. **Outcome handling** — each outcome delegates to a focused handler (`handle_container_exit`, `handle_cancel`, `handle_suspend`).

New types: `PodEvent`, `LoopOutcome`, `MonitorOutcome`, `StateTracker`. Exit paths (container exit, cancel) now race drain/shutdown against VM death via nested `select!`, preventing hangs if the VM crashes mid-shutdown.

- [x] Refactor pod_monitor into state machine

### 2. Containerd types leak through VMM boundary
`VmMountSource::ContainerdImage` carries `ResolvedImage` + `ContainerdLease` — the VMM layer is coupled to containerd internals.

**Suggestion:** Wrap in an opaque type in image_provider, or abstract so VMM only sees paths/handles.

- [ ] Decouple containerd types from VMM interface

### 3. No `VolumeProvider` abstraction
Volumes are always local ext4/tempdir. No extension point for remote storage or copy-on-write.

- [ ] Consider adding VolumeProvider trait (low priority)

## Dead Code & Cleanup

### 4. `firecracker.rs` — broken and commented out
`vmm/mod.rs` line 2 has it commented out. Uses old `VmConfig`-based API that no longer exists — won't compile.

- [ ] Delete `firecracker.rs`

### 5. Dead `#[allow(dead_code)]` items
- `QmpConnection::execute()` in `qemu.rs`
- `ApiClient::request_with_timeout()` in `cloud_hypervisor/api_client.rs`
- `VirtiofsdProcess::socket_path` in `virtiofs.rs`
- `QemuBuilder::mount_restore_info` in `qemu.rs` — set but never read

- [ ] Remove dead code items

### 6. `SnapshotVirtiofsMount::source_dir` is misleading
Stored in snapshot metadata but explicitly "not meaningful for snapshot." On restore, virtiofs mounts are reconstructed from `mount_restore_info`. The field serves no purpose.

- [ ] Remove or document `source_dir` field

## Error Handling

### 7. Cloud Hypervisor API error responses not parsed
`api_request()` in `vmm/mod.rs` checks for HTTP 200/201/204 but doesn't parse the error response body — just raw HTML/JSON in the error message.

- [ ] Parse CH API error responses into structured errors

### 8. Vsock connection timeouts swallow the actual error
Both CH and QEMU vsock connect loops retry silently until timeout, then bail with generic "timeout connecting to guest." The final connection error is lost.

- [ ] Include final connection error in vsock timeout message

### 9. Default trait impls for unsupported features bail generically
`snapshot()`, `set_balloon()`, `restore()` all bail with "not supported by this VMM." No way for callers to check capability before calling.

- [ ] Add VMM capability query method or feature flags

## QEMU Gaps

### 10. QEMU implementation is very limited vs. the trait surface
- No networking (hardcoded `-nic none`)
- No virtiofs (rejects directory mounts and containerd images)
- No snapshots, no balloon
- `QmpConnection::execute()` is dead code

If QEMU is TCG-only testing, document it. If it's meant as a real backend, needs work.

- [ ] Document QEMU limitations on the struct

## Smaller Items

### 11. No size validation for EmptyDir volumes
`volume.rs` creates sparse files at user-requested sizes with no upper bound check.

- [ ] Add size validation for EmptyDir volumes

### 12. `extract_files_from_layers()` decompresses all layers into memory
Could be a problem with large multi-layer images.

- [ ] Consider streaming decompression for layer extraction

### 13. RFC3339 time formatter reimplemented
`containerd/lease.rs` has a custom implementation instead of using `chrono` or `time` crate.

- [ ] Replace custom RFC3339 formatter with standard crate

### 14. TestVmm device assignment oversimplified
Treats virtiofs mounts as block devices in device letter sequence. Fine for current tests but would break if tests start caring about device paths.

- [ ] Fix TestVmm device assignment if needed

### 15. Adapter manager / tunnel init failures are silent
`namespace.rs` logs warnings but continues. Orchestrator may assume these features are available.

- [ ] Decide on failure policy for adapter/tunnel init
