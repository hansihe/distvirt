# Fabric Module Refactor Notes

Review of `distvirt-worker/src/fabric/` — structural issues and missing abstractions.

## High Impact

### 1. Introduce `FabricContext<P>` (parameter bag anti-pattern)

Every function in `forwarding.rs` passes the same 5-7 `Arc<Mutex<...>>` arguments:

- `port_read_loop` — 8 params
- `gateway_ingress_task` — 6 params
- `handle_unknown_unicast` — 8 params
- `dispatch_action` — 7 params
- `schedule_poll_timer` — 6 params
- `send_l4_frames` — 3 params

These are all the same shared state. A context struct would collapse this:

```rust
struct FabricContext<P: FramePort> {
    ports: Arc<Mutex<HashMap<PortId, SharedPort<P>>>>,
    mac_table: Arc<Mutex<MacTable>>,
    route_table: Arc<Mutex<RouteTable>>,
    service_table: Arc<Mutex<ServiceTable>>,
    gateway_tx: Option<mpsc::Sender<Vec<u8>>>,
    event_tx: Option<mpsc::Sender<FabricEvent>>,
}
```

Turns `port_read_loop(port_id, port, ports, mac_table, route_table, service_table, gateway_tx, event_tx)` into `port_read_loop(port_id, port, ctx)`. Adding new shared state (metrics, config) becomes non-invasive.

### 2. Extract "resolve MAC -> get port -> send" helper

This exact sequence appears at least 6 times across `mod.rs` and `forwarding.rs`:

```rust
let port_id = { mac_table.lock().unwrap().lookup(&mac) };
let port = { ports.lock().unwrap().get(&port_id).cloned() };
if let Some(port) = port {
    port.send_frame(&frame).await;
}
```

Locations:
- `Fabric::send_l4_frames`
- `Fabric::flush_service_frames`
- `handle_unknown_unicast` (ServiceAction::Forward branch)
- `dispatch_action` (ReplayPacket branch)
- `gateway_ingress_task` (unicast branch)
- `port_read_loop` (unicast branch)

Should be a single method on `FabricContext`:

```rust
impl<P: FramePort> FabricContext<P> {
    fn send_to_mac(&self, mac: &[u8; 6], frame: &[u8]) -> Option<impl Future<...>>
}
```

### 3. Deduplicate frame dispatch (`port_read_loop` vs `gateway_ingress_task`)

Both follow the same structure: parse header -> broadcast check -> MAC lookup -> `handle_unknown_unicast`. Differences:

- `port_read_loop` does MAC learning
- `port_read_loop` sends broadcast copies to the gateway
- `port_read_loop` calls `try_service_arp_reply`
- `gateway_ingress_task` uses `PortId::MAX` as source

Could share a common `dispatch_frame()` with a flag/enum for source type.

### 4. Remove duplicated methods on `Fabric` vs free functions in `forwarding.rs`

`Fabric::send_l4_frames` (mod.rs:250-285) is nearly identical to `forwarding::send_l4_frames` (forwarding.rs:256-289). `Fabric::flush_service_frames` also duplicates the MAC-lookup-and-send pattern. With `FabricContext`, these would be methods on the context with no duplication.

## Medium Impact

### 5. Frame wrapper type (raw `Vec<u8>` everywhere)

Every frame is `Vec<u8>` / `&[u8]` with manual offset math (`frame[VNET_HDR_SZ..]`, `frame[VNET_HDR_SZ..VNET_HDR_SZ + 6]`, `eth_frame[14 + 16..14 + 20]`). A thin zero-copy wrapper would prevent offset bugs and make code self-documenting:

```rust
struct FabricFrame<'a> {
    raw: &'a [u8], // includes vnet header
}

impl FabricFrame<'_> {
    fn vnet_hdr(&self) -> &[u8; VNET_HDR_SZ] { ... }
    fn eth_frame(&self) -> &[u8] { &self.raw[VNET_HDR_SZ..] }
    fn dst_mac(&self) -> [u8; 6] { ... }
    fn src_mac(&self) -> [u8; 6] { ... }
    fn ethertype(&self) -> u16 { ... }
    fn ipv4_dst(&self) -> Option<Ipv4Addr> { ... }
}
```

Also centralizes the vnet header concern (multiple places prepend `vec![0u8; VNET_HDR_SZ]`).

### 6. Break up `FabricGateway` monolith

`gateway.rs` is 530 lines combining 4 distinct responsibilities in one struct and one `run()` loop:

1. **smoltcp IP stack** — ARP handling
2. **TUN device I/O** — internet egress/ingress
3. **DNS forwarding** — local registry + upstream resolution
4. **IP-MAC mapping** — for return traffic routing

Potential extractions:
- `DnsForwarder` — `process_dns_queries`, `handle_dns_response`, `pending_dns` map, `upstream_socket`
- `TunEgress` — `ip_mac_table`, TUN read/write logic

This would make the `run()` loop readable and each piece independently testable (currently the gateway has zero async tests).

## Low Priority / Cleanup

### 7. Orphaned MAC table GC task

Spawned in `set_gateway` with bare `tokio::spawn` — the JoinHandle is dropped. If the fabric is torn down, this task leaks. Should be stored as a `TaskHandle` on the struct.

### 8. `ServiceTable.lookup_and_buffer` borrow gymnastics

The method re-borrows `self.by_ip.get_mut(&dst_ip).unwrap()` after touching `self.last_activation` due to split borrow issues. Sign that activation debounce tracking should either move into `ServiceEntity` or be factored out.

### 9. Dead code cleanup

Compiler warnings flag unused fields and methods:
- `FabricEvent::RouteMiss::dst_ip`
- `Fabric::add_port`, `add_port_raw`, `add_port_raw_with_ip`
- `ServiceEntity::ip`
- `ServiceAction::Forward::pod_ip`

## Suggested Order

1. **`FabricContext<P>`** — highest leverage, eliminates parameter bag and provides home for helpers
2. **`send_to_mac` helper** — flows naturally from (1), removes 6x duplication
3. **Deduplicate dispatch logic** — flows naturally from (1)+(2)
4. **Remove `Fabric` method duplication** — trivial once (1) exists
5. **Frame wrapper** — can be done independently, incremental
6. **Gateway decomposition** — independent, can be done later
