# Worker & Fabric Refactor Notes

Structural review of `distvirt-worker/src/` — focusing on what matters for the next round of work (tunnel ports, live migration).

Previous refactor items (FabricContext, resolve_mac helper, dispatch dedup, frame wrapper) are all done and working well.

## High Impact

### 1. ~~Extract pod supervisor from `worker.rs`~~ ✅ Done

Extracted to `supervisor.rs` (~350 lines): `PodState`, `send_event`, `pod_supervisor`, `pod_launch`, `pod_monitor`, `GRACEFUL_SHUTDOWN_TIMEOUT`, `STOP_POD_TIMEOUT`. Three pod lifecycle tests moved with their mock types.

### 2. ~~Extract namespace management from `worker.rs`~~ ✅ Done

Extracted to `namespace.rs` (~450 lines): `NamespaceState` with methods (`new`, `destroy`, `registry_sync`, `registry_update`, `route_sync`, `route_update`, `create_service`, `update_service_backend`, `service_ready`, `destroy_service`), plus `FatalError`. `create_service` takes `Option<&ActivatorRuntime>` to decouple from Worker. Seven namespace-scoped tests moved. `worker.rs` is now ~500 lines — thin command router as intended.

### 3. Port classification for tunnel ports

The fabric currently treats all ports uniformly. For multi-worker tunneling, we need:

- **Local vs tunnel port distinction** in the port map (metadata on port entries, not a type-level split).
- **Scoped flooding**: `flood_frame` currently sends to all ports. With tunnel ports, broadcast/unknown-unicast should flood locally, and only send to tunnels when the orchestrator's routing info says to. Without this, every broadcast is O(workers) fan-out.
- **`RemoteWorker` route action wiring**: Currently a stub that logs and drops. Needs to resolve `worker_id` → tunnel `PortId`, which requires a new lookup in `FabricContextInner`.

**Scope**: Small structural change (port metadata enum, flood scope parameter), but architecturally significant. Design the port classification scheme before implementing tunnel ports.

### 4. ~~Split service.rs L4/activator concerns~~ ✅ Done

Extracted to `service_activator.rs` (~190 lines): `ServiceProcessor` enum with three variants (`Passthrough`, `L3 { activator, flow_tracker }`, `L4 { activator, stream_manager }`). `ServiceEntity` replaced its three separate fields with a single `processor: ServiceProcessor`. Methods `process_frame`, `on_mark_ready`, `on_backend_update`, `handle_timeout` encapsulate all L4/L3 activator logic. `service.rs` core state machine now delegates to the processor and is readable for migration work.

## Medium Impact

### 5. ~~`FabricGateway` decomposition~~ ✅ Done

Extracted `DnsForwarder` into `gateway/dns.rs` (~270 lines): registry-based local resolution, upstream forwarding with pending map, stale entry sweeping. Extracted `TunEgress` into `gateway/tun.rs` (~325 lines): TUN device creation/configuration, ip_mac_table with MAC learning, vnet header adjustment, egress/ingress frame building, stale entry sweeping. `FabricGateway` in `gateway/mod.rs` is now a thin coordinator (~350 lines) owning smoltcp, the two sub-components, and the `run()` select loop that delegates to them.

### 6. ~~`Fabric` async Mutex elimination~~ ✅ Done

`next_port_id` is now `AtomicUsize` (fetch_add with Relaxed ordering). `gateway_tx` and `event_tx` moved into `FabricContextInner` as `OnceLock<mpsc::Sender<…>>`. Task handles wrapped in `std::sync::Mutex`. All `add_port*`/`set_gateway`/`set_event_channel` methods take `&self`. The outer `Arc<tokio::sync::Mutex<Fabric>>` in `NamespaceState` and `supervisor` replaced with `Arc<Fabric>`. GC task handle now stored on `Fabric` (fixes item 8).

### 7. TAP drain for migration suspend path

The suspend flow (docs/snapshots-migration.md step 5) requires draining remaining frames from the TAP fd after vCPU pause but before port teardown. The current `Port`/`PortGuard` RAII path doesn't have a "drain then remove" step — dropping the port guard immediately removes the port from the map and the read loop exits.

Need a `drain_and_remove(port_id)` method or similar that: stops the read loop, reads remaining frames from the fd, forwards them into the fabric, then removes the port. Design this before implementing `SuspendPod`.

## Low Priority / Cleanup

### 8. ~~Orphaned MAC table GC task~~ ✅ Done (fixed as part of item 6)

GC task handle now stored as `_gc_task: Mutex<Option<TaskHandle<()>>>` on `Fabric`, set in `set_gateway`.

### 9. `ServiceTable.lookup_and_buffer` borrow gymnastics

Re-borrows `self.by_ip.get_mut(&dst_ip).unwrap()` after touching `self.last_activation` due to split borrow issues. Activation debounce tracking should move into `ServiceEntity` or be factored into a separate struct.

### ~~10. Gateway imports fabric internals~~ ✅

Moved `gateway/` under `fabric/gateway/` and grouped `namespace.rs`/`supervisor.rs` under `worker/`. Added re-exports to `fabric/mod.rs` so consumers use `crate::fabric::{DnsRegistry, FabricGateway, ServiceProcessor, MarkReadyResult, ServiceAction}` instead of reaching into submodules.

## Suggested Order

1. ~~**Extract pod supervisor**~~ ✅
2. ~~**Extract namespace management**~~ ✅
3. **Port classification design** — needed before tunnel port implementation
4. ~~**Split service.rs L4/activator**~~ ✅
5. ~~**FabricGateway decomposition**~~ ✅
6. ~~**Fabric async Mutex elimination**~~ ✅
7. **TAP drain for suspend** — design alongside SuspendPod implementation
8. Remaining items as encountered
