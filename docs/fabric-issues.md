# Fabric / Service Forwarding Issues

Tracking known issues, test gaps, and investigation notes for the service
forwarding path in `distvirt-worker`.

## Bug 1: `update_backend` clears buffered frames — FIXED

**File:** `distvirt-worker/src/fabric/service.rs`, `update_backend`

`update_backend` unconditionally called `entity.buffer.clear()`.  The
orchestrator calls `UpdateServiceBackend` followed by `ServiceReady`.  Since
`update_backend` runs first, all buffered frames were destroyed before
`mark_ready` could flush them.

Sequence in `test_service_backend_buffer_and_flush`:

1. Client sends TCP SYN to service VIP -> buffered (service not ready)
2. `UpdateServiceBackend` -> `update_backend` -> **`buffer.clear()`**
3. `ServiceReady` -> `mark_ready` -> buffer is empty -> nothing to flush
4. Backend never receives connection -> hangs forever

### Fix

`update_backend` now only clears the buffer when:
- Backend is removed (`Some → None`)
- Backend MAC changes to a different pod (`Some(old) → Some(new)`)

Setting a backend for the first time (`None → Some`) preserves the buffer —
those frames are exactly what `mark_ready` should flush.

**Test:** `service::tests::update_backend_preserves_buffered_frames`

## Bug 2: Forward silently drops when backend MAC not in mac_table — FIXED

**File:** `distvirt-worker/src/fabric/forwarding.rs`, `handle_unknown_unicast`
**File:** `distvirt-worker/src/fabric/service.rs`, `lookup_and_buffer`
**File:** `distvirt-worker/src/fabric/mod.rs`, `add_port_inner`

When a service was ready and `lookup_and_buffer` returned
`ServiceAction::Forward`, the forwarding code resolved the backend MAC via
`ctx.inner.resolve_mac(&pod_mac)`.  If the MAC was not in the mac_table, the
frame was silently dropped (only a `log::debug!`).

This happened when the backend pod was passive (e.g. `nc -l -p 80`).  The
backend never sent an outbound frame, so its MAC was never learned in the
mac_table.  Every TCP retransmit from the client got dropped.

### Fix (two parts)

**Reachability check in `lookup_and_buffer`:** The method now takes a generic
`is_reachable: F` closure parameter.  When a service is ready but the backend
MAC is not reachable (closure returns false), it falls through to the buffering
path instead of returning `Forward`.  The call site in `handle_unknown_unicast`
passes `|mac| mac_table.lookup(mac).is_some()`.

**Service buffer flush on port add:** `add_port_inner` now calls
`ServiceTable::flush_by_backend_mac` after pre-learning the MAC.  This drains
buffered frames from all ready services whose backend MAC matches the newly
added port, sending them via `flush_service_frames` (which handles DNAT
rewriting and NAT entry insertion).

**Test:** `fabric::tests::service_forward_without_learned_backend_mac`

## Latent Bug: `handle_service_ready` L4 catch-all

**File:** `distvirt-worker/src/worker.rs`, `handle_service_ready`, ~line 726

```rust
match result {
    MarkReadyResult::Passthrough { .. } => { /* handled */ }
    MarkReadyResult::L4(ServiceAction::L4Result { .. }) => { /* handled */ }
    _ => {} // silently drops all other L4 variants
}
```

`process_l4_output` can return any `ServiceAction` variant (e.g.
`ActivatorActions`, `Forward`, `Buffered`, `Drop`), but only `L4Result` is
handled.  This means flush actions for Http2-activator services can be
silently lost.

*Not* the cause of the current E2E failures (TCP activator does not create a
`StreamManager`, so `mark_ready` takes the `Passthrough` path), but will bite
when Http2 activators are used.

## Unit Test Gaps

| Area | Status | Notes |
|------|--------|-------|
| `ServiceTable`: buffer / forward / mark_ready | Covered | |
| `ServiceTable`: L4 mark_ready | Covered | |
| `ServiceTable`: TCP activator L3 flush | Covered | |
| Fabric: DNAT forwarding | Covered | Pre-learns backend MAC |
| Fabric: SNAT return traffic | Covered | |
| Fabric: service ARP reply | Covered | |
| `update_backend` preserves buffer | Covered | `update_backend_preserves_buffered_frames` |
| Fabric: Forward with unlearned backend MAC | Covered | `service_forward_without_learned_backend_mac` — tests buffer→add port→flush flow |
| **Worker `handle_service_ready` happy path** | **Missing** | Only error case (missing namespace) is tested |
| **Worker `handle_service_ready` with buffered frames** | **Missing** | Never verifies flush reaches the fabric |
| **Fabric: full round-trip (SYN -> DNAT -> SYN-ACK -> SNAT)** | **Missing** | Each direction tested independently, not together |
| **Fabric: ARP race (service ARP vs pod ARP)** | **Missing** | Nondeterminism from dual ARP reply untested |
| **Worker `handle_service_ready` L4 catch-all** | **Missing** | The `_ => {}` silently drops actions; no test catches it |
