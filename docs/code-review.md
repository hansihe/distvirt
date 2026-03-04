# Code Quality Review

Reviewed all crates except `distvirt-compose`. March 2026.

---

## HIGH SEVERITY

### 1. Fallback to "unknown" for critical IDs during suspend
**`distvirt-orchestrator/src/namespace/output.rs:164-170`**

When emitting a `SuspendPod` command, missing `pool_id` or `pod_id` silently falls back to `"unknown"` instead of erroring. This sends a corrupted command to the worker — potentially causing orphaned pods or lost snapshots.

### 2. Resume pod returns FatalError on file I/O failures
**`distvirt-worker/src/worker/mod.rs:647-674`**

Missing/corrupted snapshot metadata causes `FatalError`, crashing the entire worker process. The code has a TODO noting this should emit `PodFailed` instead. A single bad snapshot kills all pods on the worker.

### 3. Panicking unwraps in reconciliation path
**`distvirt-orchestrator/src/namespace/reconciliation.rs:38, 50, 53, 55, 62`**

Multiple `.unwrap()` calls assume service/workload existence in `spec`. If spec and state machine become inconsistent (e.g. during a race), these panic instead of degrading gracefully.

### 4. Unchecked `.unwrap()` on Mutex locks in hot packet path
**`distvirt-worker/src/fabric/forwarding.rs:152-192` (and throughout fabric)**

All lock acquisitions in the packet forwarding path use `.unwrap()`. A single panicked task poisons the lock and crashes the worker. Consider `parking_lot::Mutex` (no poisoning) or explicit recovery.

### 5. Unwraps in activator stream manager
**`distvirt-activator/src/stream_manager.rs:253, 295, 357, 816`**

Multiple unwraps that will crash the activator (and thus all services using TCP activation) if stream state invariants are violated.

### 6. Path `.to_str().unwrap()` in Firecracker VMM
**`distvirt-worker/src/vmm/firecracker.rs:93`** and **`distvirt-cli/src/commands/legacy.rs:103, 160`**

Non-UTF-8 paths panic the worker or CLI. These are user-controlled paths.

---

## MEDIUM SEVERITY

### 7. `graceful` flag in StopPod has no effect
**`distvirt-worker/src/worker/mod.rs:510-579`**

Both graceful and force-kill paths execute identically (cancel token + same timeout). The API contract is misleading — `graceful=false` still does a graceful shutdown.

### 8. Client response routing doesn't match request IDs
**`distvirt-orchestrator/src/shell.rs:811-817`**

Responses are popped from a `BTreeMap` by key order, not matched to the originating request. Multiple in-flight requests from the same client can get misrouted responses.

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
- **`.unwrap()` proliferation** — the single biggest systemic issue. Production paths (packet forwarding, state machine transitions, file I/O) use unwrap where they should use error handling. Consider a crate-level lint or `parking_lot::Mutex`.
- **Fallback values instead of hard errors** — "unknown" strings for missing IDs mask real bugs. Fail loud.
- **No request-response correlation** — the shell routes client events by FIFO order, not by request ID. This will break under concurrent requests.
- **Testing gaps** — no tests for error paths (lock poisoning, snapshot corruption, worker disconnect during suspend), no race condition tests, no NAT table overflow tests. The E2E tests requiring root/firecracker/containerd make CI hard.

---

## TOP PRIORITIES

1. **Fix the "unknown" fallbacks** in suspend output — these corrupt commands
2. **Fix resume pod FatalError** — a bad snapshot shouldn't kill the worker
3. **Implement request-response matching** in the shell — concurrent clients will break
4. **Replace `.unwrap()` on Mutexes** with `parking_lot` or explicit recovery — poisoned locks cascade
5. **Implement the `graceful` flag** in StopPod — the API contract is currently broken
