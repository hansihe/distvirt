# Fabric Module Refactor Notes

Review of `distvirt-worker/src/fabric/` — structural issues and missing abstractions.

## High Impact

### 1. ~~Introduce `FabricContext<P>` (parameter bag anti-pattern)~~ DONE

Introduced `FabricContext<P>` in `forwarding.rs` holding all shared fabric state (`ports`, `mac_table`, `route_table`, `service_table`, `gateway_tx`, `event_tx`). All forwarding functions now take `&FabricContext<P>` or `FabricContext<P>` instead of 5-7 individual `Arc<Mutex<...>>` arguments. `Fabric<P>` stores a `FabricContext<P>` field internally.

### 2. ~~Extract "resolve MAC -> get port -> send" helper~~ DONE

Added `FabricContextInner::resolve_mac(&self, mac: &[u8; 6]) -> Option<SharedPort<P>>` which locks `mac_table` then `ports` in one call. Replaced the duplicated two-lock pattern in 5 of 6 locations: `Fabric::send_l4_frames`, `Fabric::flush_service_frames`, `handle_unknown_unicast` (Forward branch), `dispatch_action` (ReplayPacket branch), `gateway_ingress_task` (unicast branch). `port_read_loop` keeps a separate mac_table lookup for the loopback port-ID check.

### 3. ~~Deduplicate frame dispatch (`port_read_loop` vs `gateway_ingress_task`)~~ DONE

Extracted `FrameSource` enum (`Port` / `Gateway`) and a shared `dispatch_frame()` function in `forwarding.rs`. Both `port_read_loop` and `gateway_ingress_task` now delegate to `dispatch_frame`, which handles parsing, optional MAC learning, broadcast/multicast flooding, gateway-MAC forwarding, unicast lookup with loopback avoidance, and unknown-unicast fallback. Port-specific behavior (MAC learning, gateway forwarding, service ARP replies) is gated on the `FrameSource::Port` variant. Gateway uses `PortId::MAX` so loopback avoidance is a no-op.

### 4. ~~Remove duplicated methods on `Fabric` vs free functions in `forwarding.rs`~~ DONE

`Fabric::send_l4_frames` now delegates to `forwarding::send_l4_frames`. The forwarding function was made `pub(super)` and got the debug log for unknown MACs that the `Fabric` method had.

## Medium Impact

### 5. ~~Frame wrapper type (raw `Vec<u8>` everywhere)~~ DONE

Added `FabricFrame<'a>` zero-copy wrapper in `switch.rs` with accessors (`dst_mac`, `src_mac`, `ethertype`, `eth_payload`, `vnet_hdr`, `ipv4_dst`). Validates minimum frame size (`VNET_HDR_SZ + ETH_HEADER_LEN`) on construction. Also added `with_vnet_header(eth_frame)` builder and `rewrite_dst_mac(frame, mac)` helper. Updated all consumers:
- `dispatch_frame` — uses `FabricFrame::new` instead of `parse_ethernet_header(&frame[VNET_HDR_SZ..])`
- `handle_unknown_unicast` — uses `ff.ipv4_dst()` instead of `extract_ipv4_dst(&frame[VNET_HDR_SZ..])`
- `try_service_arp_reply` — uses `ff.eth_payload()`, `ff.ethertype()`, `ff.src_mac()`
- `dispatch_action`, `handle_unknown_unicast` — use `rewrite_dst_mac` for MAC rewrites
- `send_l4_frames` — uses `with_vnet_header`
- `flush_service_frames` in `mod.rs` — uses `rewrite_dst_mac`
- `gateway.rs` — uses `FabricFrame` for egress frame parsing, `ff.vnet_hdr()`/`ff.src_mac()` for TUN path, `with_vnet_header` for smoltcp→fabric frames
- `service.rs` — uses `FabricFrame` to access `eth_payload()` in L4 and L3 activator paths
- Test helpers — use `with_vnet_header` for frame construction, `FabricFrame` for assertions

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

1. ~~**`FabricContext<P>`**~~ — DONE
2. ~~**`resolve_mac` helper**~~ — DONE. Method on `FabricContextInner`, removes 5x duplication.
3. ~~**Deduplicate dispatch logic**~~ — DONE. `dispatch_frame()` + `FrameSource` enum.
4. ~~**Remove `Fabric` method duplication**~~ — DONE. `Fabric::send_l4_frames` delegates to `forwarding::send_l4_frames`.
5. ~~**Frame wrapper**~~ — DONE. `FabricFrame<'a>` + `with_vnet_header` + `rewrite_dst_mac`.
6. **Gateway decomposition** — independent, can be done later
