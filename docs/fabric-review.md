# Fabric Review & Improvement Checklist

## Overview

The fabric is a per-namespace userspace L2 Ethernet switch (`distvirt-worker/src/fabric/`).
It switches Ethernet frames between pod TAP devices, provides ARP/DNS/ICMP via a
smoltcp-based gateway, handles internet egress through a TUN device, and manages
service entities with virtual IPs that support activation-on-demand and L4 proxying.

**Modules:** `mod.rs`, `port.rs`, `switch.rs`, `route.rs`, `service.rs`,
`forwarding.rs`, `gateway.rs`, `dns.rs`, `tun.rs`, `tests.rs`

---

## Tech Debt

### Code Duplication

- [x] **Duplicated `add_port` methods** — `mod.rs` has 4 near-identical methods
  (`add_port`, `add_port_with_ip`, `add_port_raw`, `add_port_raw_with_ip`).
  The only differences are Port vs generic P and whether route-buffer flush
  happens. Refactor into a shared inner helper.
  *Fixed: extracted `add_port_inner(&mut self, port, pod_ip)` — all 4 methods delegate to it.*

- [x] **Duplicated action dispatch** — `Fabric::execute_service_actions` (`mod.rs:342`)
  and `dispatch_action` (`forwarding.rs:195`) implement the same
  ReplayPacket/SetBackendNeed/Log logic. The `mod.rs` version runs in a sync
  context (holds locks), the `forwarding.rs` version is async. Unify or extract
  shared logic.
  *Fixed: removed `execute_service_actions`; added async `Fabric::dispatch_actions` that delegates to `forwarding::dispatch_action`. Caller in `worker.rs` updated to `.await`.*

### Stubs & Incomplete Features

- [ ] **`RemoteWorker` route is a stub** — `forwarding.rs:446-450` logs and drops.
  The route table models it but nothing forwards cross-worker yet.

### Resource Leaks / No Eviction

- [x] **No MAC table aging** — `MacTable` learns entries but never evicts stale ones.
  Pod churn in long-lived namespaces will accumulate garbage. Add TTL-based or
  capacity-based eviction.
  *Fixed: entries now track `Instant`; `gc(max_age)` method added; periodic task (60s interval, 5min TTL) spawned from `set_gateway()`.*

- [x] **Gateway `ip_mac_table` never evicts** — `gateway.rs:113` learns
  src_ip→src_mac on egress but never cleans up. Same aging concern.
  *Fixed: entries now track `Instant`; swept every 5s via `sweep_stale_entries()` (5min TTL).*

- [x] **DNS `pending_dns` has no timeout** — `gateway.rs:125` stores query ID →
  client endpoint but never expires entries. Lost upstream responses leak entries
  and can eventually collide in the u16 ID space.
  *Fixed: entries now track `Instant`; swept every 5s via `sweep_stale_entries()` (10s TTL).*

### Performance

- [ ] **`flood_frame` serializes across ports** — `forwarding.rs:541-562` sends to
  each port sequentially. A slow port blocks all others. Consider concurrent
  sends (spawn or join).

### Locking

- [x] **`send_l4_frames` double-locks** — `forwarding.rs:263-264` holds both
  `mac_table` and `ports` mutexes simultaneously. Lock ordering appears
  consistent today but is fragile. Document the ordering or restructure.
  *Fixed: all 4 double-lock sites (`forwarding::send_l4_frames`, `Fabric::send_l4_frames`, `Fabric::flush_service_frames`, `dispatch_action` ReplayPacket arm) now lock each mutex in a minimal scope — lock `mac_table` to get `port_id`, release, then lock `ports` to clone Arc, release.*

### Silent Failures

- [x] **`try_send` drops on backpressure** — Event channel and gateway channel use
  `try_send` which silently drops. This is likely intentional for non-critical
  paths but worth auditing — some dropped events (e.g. `ServiceBackendNeed`)
  could cause visible misbehavior.
  *Fixed: `ServiceBackendNeed` now uses `.send().await` in both `forwarding::dispatch_action` and the worker bridge task (non-retriggerable signal — must not be dropped). `RouteMiss` and `ServiceActivation` keep `try_send` (self-healing via retransmission) but now log warnings on drop. Gateway channel drops left as-is (protocol-level retries).*

---

## Test Coverage Gaps

### Untested Fabric-level Methods

- [ ] **`send_l4_frames`** — no direct unit test
- [ ] **`flush_service_frames`** — no direct unit test (including error paths:
  backend MAC not in mac_table, port gone)
- [ ] **`dispatch_actions`** — no direct unit test (replaced `execute_service_actions`)

### Untested Lifecycle

- [ ] **Port removal via PortGuard** — no test verifies dropping a TaskHandle
  removes the port from the map
- [ ] **Service buffer timeout** — the service table's `timeout_ms` expiry path
  in `lookup_and_buffer` is untested (route table's equivalent IS tested)

### Untested Async Paths

- [ ] **`schedule_poll_timer`** — recursive timer scheduling in
  `forwarding.rs:290-325` has no test
- [ ] **Gateway `run()` loop** — the main select loop is completely untested
  (TUN + smoltcp + DNS upstream flow)
- [ ] **DNS upstream forwarding** — full query→upstream→response round-trip is
  untested

### Missing Stress / Concurrency Tests

- [ ] **Multi-port concurrency** — no test exercises concurrent frame forwarding
  across many ports
- [ ] **Lock contention under load** — no stress test for the shared-mutex
  architecture

### Conditional Tests

- [ ] **Activator tests skip silently** — 6 tests return early with
  `eprintln!("SKIP")` if WASM components aren't built. These should ideally be
  `#[ignore]` with a CI step that builds components, or use a test fixture that
  fails loudly in CI.
- [ ] **`tun.rs`** — no tests at all (requires root privileges). Could add
  integration tests behind a feature flag or in the E2E suite.
