# Code Quality Review

Reviewed all crates except `distvirt-compose`. March 2026.

---

## HIGH SEVERITY

### 1. ~~Fallback to "unknown" for critical IDs during suspend~~ RESOLVED
**`distvirt-orchestrator/src/namespace/output.rs:160-217`**

When emitting a `SuspendPod` command, missing `pool_id` or `pod_id` silently falls back to `"unknown"` instead of erroring. This sends a corrupted command to the worker — potentially causing orphaned pods or lost snapshots.

**Status:** `pod_id` lookup converted to `.expect()` — it's a true invariant
(workload is always in `Suspending` state when `SuspendRequest` is emitted).
`pool_id` lookup now fails gracefully: if the worker has no storage pool, emits
`SmWorkloadEvent::PodSuspendFailed` and feeds `WorkloadInput::PodSuspendFailed`
back to the workload SM, which cancels the suspend timeout and re-evaluates
demand. No corrupted command is sent.

### 2. ~~Resume pod returns FatalError on file I/O failures~~ RESOLVED
**`distvirt-worker/src/worker/mod.rs:715-822`**

Missing/corrupted snapshot metadata causes `FatalError`, crashing the entire worker process. The code has a TODO noting this should emit `PodFailed` instead. A single bad snapshot kills all pods on the worker.

**Status:** Both `tokio::fs::read()` and `serde_json::from_slice()` errors now use
`match` + `send_event(WorkerEvent::PodFailed { ... })` + `return Ok(())`, scoping the
failure to the single affected pod. The namespace-not-found check correctly remains a
`FatalError` (true invariant violation).

### 3. ~~Panicking unwraps in reconciliation path~~ RESOLVED
**`distvirt-orchestrator/src/namespace/reconciliation.rs:38, 50, 53, 55, 62`**

Multiple `.unwrap()` calls assume service/workload existence in `spec`. If spec and state machine become inconsistent (e.g. during a race), these panic instead of degrading gracefully.

**Status:** Converted to descriptive `.expect("invariant: ...")` messages. These are
internal map lookups where keys are known to exist by construction (the reconciler
iterates spec-derived data). The panics are now documented as intentional invariant
assertions. See `docs/unwrap-cleanup.md` Category 6.

### 4. ~~Unchecked `.unwrap()` on Mutex locks in hot packet path~~ PARTIALLY RESOLVED
**`distvirt-worker/src/fabric/forwarding.rs:152-192` (and throughout fabric)**

All lock acquisitions in the packet forwarding path use `.unwrap()`. A single panicked task poisons the lock and crashes the worker. Consider `parking_lot::Mutex` (no poisoning) or explicit recovery.

**Status:** All ~30 lock `.unwrap()` calls across `fabric/mod.rs`, `forwarding.rs`,
`tunnel.rs`, and `gateway/dns.rs` converted to `.expect("poisoned")`. Panics are now
intentional. Migrating to `parking_lot::Mutex` (which has no poisoning) remains a
potential future improvement. See `docs/unwrap-cleanup.md` Category 1.

### 5. ~~Unwraps in activator stream manager~~ RESOLVED
**`distvirt-activator/src/stream_manager.rs:253, 295, 357, 816`**

Multiple unwraps that will crash the activator (and thus all services using TCP activation) if stream state invariants are violated.

**Status:** `create_listener` now returns `Option<SocketHandle>` with a port-zero guard
and graceful `listen()` error handling. `local_endpoint().unwrap()` replaced with
`match` + `continue` (handles RST-during-handshake edge case). `streams.get_mut().unwrap()`
replaced with `let-else` + `continue`. Line 816 left as-is (test assertion).

### 6. ~~Path `.to_str().unwrap()` in Firecracker VMM~~ RESOLVED
**`distvirt-worker/src/vmm/firecracker.rs:93`** and **`distvirt-cli/src/commands/legacy.rs:103, 160`**

Non-UTF-8 paths panic the worker or CLI. These are user-controlled paths.

**Status:** All sites now use `.to_str().ok_or_else(|| anyhow!(...))` with `?`
propagation. Also fixed the same pattern in `image_provider/containerd.rs:347`
(mount dir). See `docs/unwrap-cleanup.md` Category 4.

---

## MEDIUM SEVERITY

### 7. ~~`graceful` flag in StopPod has no effect~~ RESOLVED
**`distvirt-worker/src/worker/mod.rs:662-724`**

Both graceful and force-kill paths execute identically (cancel token + same timeout). The API contract is misleading — `graceful=false` still does a graceful shutdown.

**Status:** `graceful=false` now aborts the supervisor task immediately (VM process
killed via `Drop` / SIGKILL) with a 2s cleanup window (`FORCE_STOP_TIMEOUT`),
skipping the graceful container shutdown sequence entirely. `graceful=true` is
unchanged (cancel token → SIGTERM containers → VM Shutdown → 15s outer timeout).

### 8. ~~Client response routing doesn't match request IDs~~ RESOLVED
**`distvirt-orchestrator/src/shell.rs`**

Responses were popped from a `BTreeMap` by key order, not matched to the originating request. Multiple in-flight requests from the same client could get misrouted responses.

**Status:** Replaced `BTreeMap<u64, oneshot::Sender>` with `Option<oneshot::Sender>`,
enforcing single-request-per-client structurally. Removed unused `request_id` /
`NEXT_REQUEST_ID` machinery. This is safe because `grpc.rs::unary_command` allocates
a fresh `ClientId` per gRPC call, so concurrent requests on the same client never occur.

### 9. Log stream data loss on quick pod exit
**`distvirt-orchestrator/src/shell.rs:328-391`**

If a pod exits between log stream header read and workload resolution, logs are silently dropped with a warning. No buffering by pod_id.

### 10. Silent spec update without migration
**`distvirt-orchestrator/src/namespace/commands.rs:181-199`**

Workload spec changes are logged as warnings but silently applied without redeploying pods. Users won't see their config changes take effect.

### 11. ~~Unsafe `libc::kill` without PID validation~~ RESOLVED
**`distvirt-cli/src/commands/connect.rs:194-196`**

Sends SIGTERM to a stored PID without checking if the process still exists or if PID was reused. Could kill an unrelated process.

**Status:** Now validates PID > 0, probes with `kill(pid, 0)` before sending SIGTERM,
and warns if the process is no longer running.

### 12. WireGuard peer IP offset overflow
**`distvirt-orchestrator/src/wg_peers.rs:75`**

Uses `u16` for host offset calculation — overflows/wraps for subnets larger than /17 (>32k addresses).

### 13. Connection state file has no permissions or staleness checks
**`distvirt-cli/src/commands/connect.rs:22-26, 120-121`**

State files written world-readable, no timestamps, no PID liveness check.

### 14. ~~Snapshot size calculation swallows errors~~ RESOLVED
**`distvirt-worker/src/worker/supervisor.rs:533`**

`dir_size().unwrap_or(0)` silently reports 0 bytes without logging the error, giving the orchestrator incorrect capacity data.

**Status:** Both call sites (`supervisor.rs` and `artifact_transfer.rs`) now log a
warning on error before falling back to 0. The duplicate `dir_size` was also
consolidated (see #20).

---

## LOW SEVERITY

### 15. Namespace deletion resets state without events
**`distvirt-orchestrator/src/namespace/commands.rs:274-282`**

Workloads/services are force-reset to Dormant/Idle without emitting state change events. External observers see a gap.

### 16. ~~Lost pod info on worker disconnect~~ RESOLVED
**`distvirt-orchestrator/src/namespace/events.rs:326`**

`_lost_pods` is discarded without logging which pods were lost.

**Status:** Now logs a warning with the count and list of dropped pod IDs.

### 17. TAP device name silently truncated
**`distvirt-worker/src/tap.rs:107-109`**

Names exceeding `IFNAMSIZ` are truncated, potentially causing TAP device leaks if the cleanup name doesn't match.

### 18. NAT table has no max entry count
**`distvirt-worker/src/fabric/nat.rs`**

GC runs every 60s but no cap on entries. High-flow-count services could exhaust memory before GC runs.

### 19. Duplicate error paths for unimplemented commands
**`distvirt-orchestrator/src/grpc.rs` + `namespace/mod.rs`**

Splice/Clone return errors at both the gRPC layer and the SM layer — inconsistent and could diverge.

---

## ARCHITECTURE & PATTERNS

**Strengths:**
- Pure state machine design in the orchestrator — clean separation of I/O from logic
- Two-layer orchestrator (outer + per-namespace SM) keeps things testable
- Fabric architecture is well-designed — clean port abstraction, proper NAT tracking, service entities as first-class concepts
- Cap'n Proto protocol is well-structured with good serialization patterns
- Good use of yamux for multiplexing (separate log streams avoid head-of-line blocking)

**Weaknesses:**
- **`.unwrap()` proliferation** — largely addressed. Mutex locks now use `.expect("poisoned")`, internal invariant lookups use descriptive `.expect()`, path conversions return `Result`, and guard-pattern unwraps refactored to `let-else`. Remaining unwraps are in test code or are documented intentional assertions. See `docs/unwrap-cleanup.md` for full breakdown. `parking_lot::Mutex` migration remains a potential future step.
- **Fallback values instead of hard errors** — "unknown" strings for missing IDs mask real bugs. Fail loud.
- ~~**No request-response correlation**~~ — resolved; `BTreeMap` replaced with `Option`, enforcing single-request-per-client structurally. `request_id` machinery removed.
- **Testing gaps** — no tests for error paths (lock poisoning, snapshot corruption, worker disconnect during suspend), no race condition tests, no NAT table overflow tests. The E2E tests requiring root/firecracker/containerd make CI hard.

---

## TOP PRIORITIES

1. ~~**Fix the "unknown" fallbacks**~~ — resolved; pool_id missing fails gracefully via `PodSuspendFailed`, pod_id is an `.expect()` invariant
2. ~~**Fix resume pod FatalError**~~ — resolved; errors now scoped to single pod via `PodFailed`
3. ~~**Implement request-response matching**~~ — resolved; `Option` enforces single-request-per-client structurally
4. ~~**Replace `.unwrap()` on Mutexes**~~ — converted to `.expect("poisoned")`; `parking_lot` migration optional
5. ~~**Implement the `graceful` flag** in StopPod~~ — resolved; `graceful=false` now aborts immediately via SIGKILL

---

## DISTVIRT-WORKER FOCUSED REVIEW (March 2026)

Deep review of the `distvirt-worker` crate specifically.

### Bugs / Correctness

#### 20. ~~`dir_size` duplicated and non-recursive~~ RESOLVED
**`supervisor.rs:604` and `artifact_transfer.rs:273`**

Two identical `dir_size` functions that only count files in the immediate directory. If snapshot or artifact directories ever contain subdirectories, sizes will be under-reported. Should be extracted into a shared recursive utility.

**Status:** Consolidated into a single `pub(crate)` function in `supervisor.rs`, now
recursive (uses a stack to walk subdirectories). `artifact_transfer.rs` imports it.

#### 21. `handle_stop_pod` removes pod before supervisor finishes
**`worker/mod.rs:701`**

The pod is removed from `ns.pods` via `.remove()` immediately, then the supervisor is awaited. If the supervisor sends a `PodExited`/`PodFailed` event during graceful shutdown, the `remove_finished_pod` call in the main loop (line 276) operates on an already-removed entry. Not a crash (it's a no-op), but the pod is no longer tracked locally when the event fires — the main loop forwards the event to the orchestrator but can't do any local bookkeeping.

#### 22. ~~`local_pool_copy` only copies files, not subdirectories~~ RESOLVED
**`artifact_transfer.rs:258`**

Only copies immediate child files. If an artifact has nested directories, they are silently dropped. The TCP transfer path uses `tar` which handles this correctly, creating an inconsistency between local and remote transfers.

**Status:** Now uses a stack-based recursive walk matching the `dir_size` pattern,
copying subdirectories with `create_dir_all`. Consistent with the tar-based TCP path.

#### 23. ~~Missing `ArtifactWriteFailed` event on suspend failure~~ RESOLVED (already handled)
**`supervisor.rs:521-569`**

When `vm.suspend()` fails, `PodSuspendFailed` is emitted but the `ArtifactWriteStarted` event was already sent (line 521). The orchestrator sees "write started" followed by "suspend failed" but never gets a matching write-failed/aborted event — may leave stale artifact tracking state.

**Status:** Already handled in the orchestrator. `events.rs:284-293` cleans up `Writing`
placement entries when `PodSuspendFailed` is received. No protocol change needed.

#### 24. ~~`api_request` silently swallows read timeout~~ RESOLVED
**`vmm/firecracker.rs:599-603`**

If the Firecracker API response read times out, the function proceeds to check whatever partial response was received (`Err(_) => {}`) rather than returning an error. This could mask issues where Firecracker is hung — a partial response might happen to contain a "200" substring and pass validation.

**Status:** Timeout now logs a warning with the API path before proceeding to check
the partial response.

### Design / Maintainability

#### 25. `FabricPacketMut` duplicates all `FabricPacket` accessors
**`packet/frame.rs:119-186`**

All read-only accessors from `FabricPacket` are copy-pasted into `FabricPacketMut`. A `Deref` impl or shared trait would eliminate this duplication. Additionally, `FabricPacketMut` is not used anywhere — all mutation goes through the free functions (`rewrite_ipv4_dst`, etc.). Could be removed entirely.

#### 26. Lock contention on service table in hot path
**`forwarding.rs:152-154`**

The service table and IP port table are locked simultaneously on every packet through the service VIP path. Documented in the lock ordering comment (`mod.rs:11-20`), but will become a bottleneck at scale.

#### 27. Tunnel `recv_loop` holds write lock for entire handshake
**`tunnel.rs:357`**

The write lock on `TunnelState` is held for the entire handshake message processing including the response send (via `tokio::spawn`). During handshake, this blocks all egress and other ingress lookups. With many peers connecting simultaneously, this causes frame drops.

#### 28. Hardcoded worker capabilities
**`worker/mod.rs:155-158`**

```rust
max_pods: 10,
available_memory_mb: 1024,
```

These are hardcoded rather than derived from system resources. `available_memory_mb` especially should come from `/proc/meminfo` or similar.

#### 29. `schedule_poll_timer` is recursive via `tokio::spawn`
**`forwarding.rs:538-563`**

Each timeout reschedules itself by spawning a new task. If smoltcp gets stuck returning very small delays, this could create a large chain of timer tasks. A single persistent timer task per service IP (or bounded retry count) would be more robust.

#### 30. `NetConfig` fields are `String` but carry IP semantics
**`vmm/mod.rs` via `NetConfig`**

`guest_ip`, `netmask`, and `gateway` are `String` types that originate from `Ipv4Addr` values (`network.ip.to_string()` in `supervisor.rs:219`). These are parsed back to strings for the guest protocol JSON. Using typed fields throughout would prevent format errors and make the code more self-documenting.

### Security

#### 31. Artifact transfer listener has no authentication
**`artifact_transfer.rs:46`**

The TCP transfer listener accepts connections from any source with no authentication or authorization. An attacker on the network could inject arbitrary artifacts into any pool. The `TransferHeader` has a `_reserved` byte, and a shared secret from the orchestrator handshake could be used to add HMAC verification.
