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

### 11. Unsafe `libc::kill` without PID validation
**`distvirt-cli/src/commands/connect.rs:194-196`**

Sends SIGTERM to a stored PID without checking if the process still exists or if PID was reused. Could kill an unrelated process.

### 12. WireGuard peer IP offset overflow
**`distvirt-orchestrator/src/wg_peers.rs:75`**

Uses `u16` for host offset calculation — overflows/wraps for subnets larger than /17 (>32k addresses).

### 13. Connection state file has no permissions or staleness checks
**`distvirt-cli/src/commands/connect.rs:22-26, 120-121`**

State files written world-readable, no timestamps, no PID liveness check.

### 14. Snapshot size calculation swallows errors
**`distvirt-worker/src/worker/supervisor.rs:510`**

`dir_size().unwrap_or(0)` silently reports 0 bytes without logging the error, giving the orchestrator incorrect capacity data.

---

## LOW SEVERITY

### 15. Namespace deletion resets state without events
**`distvirt-orchestrator/src/namespace/commands.rs:274-282`**

Workloads/services are force-reset to Dormant/Idle without emitting state change events. External observers see a gap.

### 16. Lost pod info on worker disconnect
**`distvirt-orchestrator/src/namespace/events.rs:294`**

`_lost_pods` is discarded without logging which pods were lost.

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
