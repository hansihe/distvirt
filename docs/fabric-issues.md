# Fabric / Service Forwarding Issues

Tracking known issues, test gaps, and investigation notes for the service
forwarding path in `distvirt-worker`.

## Bug 1: `update_backend` clears buffered frames (primary)

**File:** `distvirt-worker/src/fabric/service.rs`, `update_backend`, line 208

`update_backend` unconditionally calls `entity.buffer.clear()`.  The
orchestrator calls `UpdateServiceBackend` followed by `ServiceReady`.  Since
`update_backend` runs first, all buffered frames are destroyed before
`mark_ready` can flush them.

Sequence in `test_service_backend_buffer_and_flush`:

1. Client sends TCP SYN to service VIP -> buffered (service not ready)
2. `UpdateServiceBackend` -> `update_backend` -> **`buffer.clear()`**
3. `ServiceReady` -> `mark_ready` -> buffer is empty -> nothing to flush
4. Backend never receives connection -> hangs forever

Confirmed by debug logs: ARP reply is sent (client resolved VIP), then
`updated service backend` + `service marked ready` with **zero** forwarding
log messages afterwards.  No "flush_service_frames", no "service forward" --
the buffer is empty.

### Fix options

- **Don't clear the buffer when setting a backend** -- only clear when
  *removing* a backend (`backend: None`).  Frames buffered before a backend
  existed are exactly the frames that should be flushed.
- Alternatively, move the buffer drain into `update_backend` itself and return
  the frames, so the caller can flush immediately.

## Bug 2: Forward silently drops when backend MAC not in mac_table

**File:** `distvirt-worker/src/fabric/forwarding.rs`, `handle_unknown_unicast`,
lines 441-480

When a service is ready and `lookup_and_buffer` returns
`ServiceAction::Forward`, the forwarding code resolves the backend MAC via
`ctx.inner.resolve_mac(&pod_mac)`.  If the MAC is not in the mac_table, the
frame is silently dropped (only a `log::debug!`).

This happens when the backend pod is passive (e.g. `nc -l -p 80`).  The
backend never sends an outbound frame, so its MAC is never learned in the
mac_table.  Every TCP retransmit from the client gets dropped.

This affects both E2E tests:

- `test_service_backend_buffer_and_flush`: after bug 1 clears the buffer,
  retransmits hit Forward but backend MAC is not learned (backend VM is still
  booting at the time of `mark_ready`).
- `test_service_backend_ready_forward`: service is ready before the client
  sends, so all traffic takes the Forward path.  If the backend hasn't sent
  any frames yet, the MAC lookup fails and frames are dropped.

### Fix options

- **Pre-populate the mac_table when a pod's TAP port is added** -- the worker
  knows the pod's MAC and port at launch time.  Register it in the mac_table
  so the DNAT forward path can always find it.
- Alternative: when Forward can't resolve the MAC, fall back to flooding or
  send an ARP request for the backend IP.

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
| Fabric: DNAT forwarding | Covered | Pre-learns backend MAC; doesn't catch bug 2 |
| Fabric: SNAT return traffic | Covered | |
| Fabric: service ARP reply | Covered | |
| **`update_backend` preserves buffer** | **Missing** | Would catch bug 1 directly |
| **Fabric: Forward with unlearned backend MAC** | **Missing** | Would catch bug 2 directly |
| **Worker `handle_service_ready` happy path** | **Missing** | Only error case (missing namespace) is tested |
| **Worker `handle_service_ready` with buffered frames** | **Missing** | Never verifies flush reaches the fabric |
| **Fabric: full round-trip (SYN -> DNAT -> SYN-ACK -> SNAT)** | **Missing** | Each direction tested independently, not together |
| **Fabric: ARP race (service ARP vs pod ARP)** | **Missing** | Nondeterminism from dual ARP reply untested |
| **Worker `handle_service_ready` L4 catch-all** | **Missing** | The `_ => {}` silently drops actions; no test catches it |
