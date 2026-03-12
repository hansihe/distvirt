---
title: "Next Steps"
---

1. Config-from-file for the guest agent — biggest bang for buck on boot latency. Bake container config into the rootfs (or a small config virtio-blk device) so the guest can mount+fork immediately without waiting for vsock. Vsock becomes a runtime control channel, not the boot critical path.
2. libkrun backend — macOS support for dev experience, and it validates the VMM trait abstraction with a second implementation.
3. Flow control on IO stream from VMs

---

## TCP RST injection on fabric port removal (low priority, defense-in-depth)

**File:** `distvirt-worker/src/fabric/forwarding.rs`

When a VM dies (crash, force-kill, or even graceful shutdown that races),
its fabric port is removed. Any established TCP connections from peers
will hang until TCP keepalive fires (2 hours by default on Linux).

When a port is removed from the fabric:
1. Scan the endpoint table for entries involving the removed port's IP
2. For each entry with active flows, craft a TCP RST packet and send it to the peer port
3. Clean up flow tracker state

Implementation options:
- RST with seq=0 and let the peer's TCP stack handle the challenge ACK -> RST sequence (simplest)
- Shorter TCP keepalive timeouts in guests (alternative, no fabric changes needed)

---

## TAP drain for migration suspend path

The suspend flow (docs/snapshots-migration.md step 5) requires draining remaining frames from the TAP fd after vCPU pause but before port teardown. The current `Port`/`PortGuard` RAII path doesn't have a "drain then remove" step — dropping the port guard immediately removes the port from the map and the read loop exits.

Need a `drain_and_remove(port_id)` method or similar that: stops the read loop, reads remaining frames from the fd, forwards them into the fabric, then removes the port. Design this before implementing `SuspendPod`.

---

## Completed

- **Port classification for tunnel ports**: `tunnel_ports` map, `add_tunnel_port`/`remove_tunnel_port`, `RemoteWorker` route action wired to resolve `worker_id` -> tunnel `PortId`. No flooding in L3 fabric so scoped flooding is N/A.
- **`handle_service_ready` L4 catch-all**: `other =>` arm now logs unexpected variants via `log::debug!` instead of silently dropping.
- **`lookup_and_buffer` borrow gymnastics**: Refactored into phase-based dispatch (`dispatch_service`, `dispatch_service_buffer`, etc.) with `last_activation` inside `Endpoint` and `check_activation_debounce` taking `&mut Endpoint`.
