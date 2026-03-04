1. Config-from-file for the guest agent — biggest bang for buck on boot latency. Bake container config into the rootfs (or a small config virtio-blk device) so the guest can mount+fork immediately without waiting for vsock. Vsock becomes a runtime control channel, not the boot critical path.
2. libkrun backend — macOS support for dev experience, and it validates the VMM trait abstraction with a second implementation.
3. Flow control on IO stream from VMs

---

## Fix 3: TCP RST injection on fabric port removal (low priority, defense-in-depth)

**File:** `distvirt-worker/src/fabric/forwarding.rs`

### Problem

When a VM dies (crash, force-kill, or even graceful shutdown that races),
its fabric port is removed. Any established TCP connections from peers
will hang until TCP keepalive fires (2 hours by default on Linux).

### Desired behavior

When a port is removed from the fabric:
1. Scan the NAT table for entries involving the removed port's IP/MAC
2. For each entry, craft a TCP RST packet and send it to the peer port
3. Remove the NAT entries

This is defense-in-depth — with Fix 1, graceful shutdown should handle
most cases. But crashes and force-kills will always exist.

### Implementation notes

- The NAT table has `NatFlowKey` (5-tuple) and `NatEntry` (service_ip,
  backend_ip, service_mac)
- Need to identify entries by backend_ip or iterate all entries
- RST packet construction: needs correct seq/ack numbers, which the
  NAT table doesn't currently store
- Alternative: send RST with seq=0 and let the peer's TCP stack handle
  the challenge ACK → RST sequence (simpler but less clean)
- Could also just rely on shorter TCP keepalive timeouts in guests

---

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

---

### 3. Port classification for tunnel ports

The fabric currently treats all ports uniformly. For multi-worker tunneling, we need:

- **Local vs tunnel port distinction** in the port map (metadata on port entries, not a type-level split).
- **Scoped flooding**: `flood_frame` currently sends to all ports. With tunnel ports, broadcast/unknown-unicast should flood locally, and only send to tunnels when the orchestrator's routing info says to. Without this, every broadcast is O(workers) fan-out.
- **`RemoteWorker` route action wiring**: Currently a stub that logs and drops. Needs to resolve `worker_id` → tunnel `PortId`, which requires a new lookup in `FabricContextInner`.

**Scope**: Small structural change (port metadata enum, flood scope parameter), but architecturally significant. Design the port classification scheme before implementing tunnel ports.

### 7. TAP drain for migration suspend path

The suspend flow (docs/snapshots-migration.md step 5) requires draining remaining frames from the TAP fd after vCPU pause but before port teardown. The current `Port`/`PortGuard` RAII path doesn't have a "drain then remove" step — dropping the port guard immediately removes the port from the map and the read loop exits.

Need a `drain_and_remove(port_id)` method or similar that: stops the read loop, reads remaining frames from the fd, forwards them into the fabric, then removes the port. Design this before implementing `SuspendPod`.

### 9. `ServiceTable.lookup_and_buffer` borrow gymnastics

Re-borrows `self.by_ip.get_mut(&dst_ip).unwrap()` after touching `self.last_activation` due to split borrow issues. Activation debounce tracking should move into `ServiceEntity` or be factored into a separate struct.
