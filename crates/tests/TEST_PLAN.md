# Simulation Test Plan

This document tracks what the `distvirt-tests` simulation test suite covers and what gaps remain. Tests use `TestCluster` with fake time, simulated workers, and `SimGatewayProvider` for deterministic full-stack testing.

## Current Coverage (38 tests)

| Module | Tests | What's Covered |
|---|---|---|
| `pod_lifecycle` | 1 | Create, run, delete namespace |
| `activation_lifecycle` | 1 | Dormant → activate → deactivate → suspend |
| `suspend_resume` | 3 | Suspend/resume, suspend timeout fallback, artifact-pinned resume |
| `multi_worker` | 1 | Reschedule on worker disconnect |
| `drain` | 2 | Drain excludes scheduling, existing pods continue |
| `multi_service` | 2 | Shared workload, late-joiner service |
| `fabric_routing` | 2 | Route-miss activation (dormant + suspended) |
| `pressure` | 6 | Pressure scheduling, timeout shortening, capacity blocking, preemption, multi-namespace pressure recovery |
| `retry_backoff` | 2 | Crash retry loop, launch failure recovery |
| `spec_reconciliation` | 4 | Image change restart, add/remove workload, image change on suspended |
| `edge_cases` | 7 | Rapid create/delete, two namespaces same worker, delete while suspended, disconnect during suspend, namespace deletion sibling safety, preemption namespace-scoped, many namespaces competing (BROKEN) |
| `known_bugs` | 1 | EndpointFlowStatus via service_id |
| `flow_tracking` | 3 | Active flows prevent suspend, flow end triggers idle, flow status on disconnect (BROKEN) |
| `transition_races` | 4 | Delete during launch, traffic during suspend, rapid activate/deactivate, spec update during suspend |

## Gaps

### 1. Multi-Worker Fabric & Routing

Only one test exercises multiple workers. The fabric's cross-worker tunneling, registry sync, and segment-based namespace isolation are untested at the simulation level.

- [ ] **Cross-worker traffic delivery** — activate a service on worker A, send traffic from worker B's gateway, verify it reaches the pod
- [ ] **Worker registry sync** — add a third worker mid-flight, verify tunnel topology updates propagate
- [ ] **Segment ID isolation** — two namespaces on the same workers, verify packets don't cross namespaces

### 2. Transition Intent Races

The orchestrator has its own scenario tests for these, but full-stack simulation should validate the complete path.

- [x] **Traffic during suspend** — send activation traffic while a pod is mid-suspend, verify it resumes (PendingIntent::Demand overrides)
- [x] **Delete during launch** — delete namespace while pod is still launching
- [x] **Spec update during suspend** — image change arrives while pod is suspending, verify snapshot is invalidated and cold-start happens
- [x] **Rapid activate/deactivate cycles** — multiple activate/deactivate in quick succession

### 3. Endpoint Buffering & Flush

The fabric buffers up to 64 frames for 30s when a backend isn't ready. No simulation tests exercise this.

- [ ] **Buffer flush on ready** — send N packets to a dormant service, verify all are delivered after pod reaches Running
- [ ] **Buffer overflow** — send >64 packets, verify oldest are dropped gracefully
- [ ] **Buffer timeout** — activate but delay pod readiness past 30s, verify buffer drains

### 4. Worker Reconnection

- [ ] **Clean reconnect** — disconnect worker, reconnect a new worker, verify old state cleaned up and new worker gets full namespace sync
- [ ] **Reconnect with stale state** — worker reconnects after pods were rescheduled elsewhere, verify no conflicts

### 5. Snapshot Placement & Artifact Affinity

Only one test (`resume_pinned_to_artifact_worker`) touches this area.

- [ ] **Resume on different worker after artifact transfer** — suspend on worker A, disconnect A, verify transfer + resume on worker B
- [ ] **Snapshot invalidation chain** — suspend → image change → verify cold-start, not snapshot resume
- [ ] **Eviction under storage pressure** — inject pool capacity updates, verify snapshot-lost condition and cold-start fallback

### 6. Multi-Namespace Interactions

- [x] **Many namespaces competing for capacity** — 3 namespaces on 1 worker under pressure. **BROKEN**: not all WaitingForCapacity workloads rescheduled after pressure drop
- [x] **Namespace deletion doesn't affect siblings** — delete one namespace while others are active
- [x] **Preemption is namespace-scoped** — verify preemption can't cross namespace boundaries

### 7. WireGuard Peer Lifecycle

The docs describe a full WireGuard ingress adapter, but no simulation tests cover it.

- [ ] **Add/remove peer** — verify endpoint routing updates
- [ ] **Traffic through WireGuard endpoint** — peer sends packet, verify it reaches pod
- [ ] **Peer on disconnected worker** — verify UnplacedPod buffering

### 8. Resource Leases

- [ ] **Lease timeout** — pod launch takes longer than lease deadline (60s), verify lease expires and capacity is released
- [ ] **Lease cleanup on disconnect** — worker disconnects mid-launch, verify lease released and pod rescheduled
- [ ] **Double-booking prevention** — verify two pods can't claim the same slot via racing lease grants

### 9. Flow Tracking (EndpointFlowStatus)

Only one `known_bugs` test touches this.

- [x] **Active flows prevent suspend** — activate service, report `has_active_flows=true`, deactivate service, verify pod stays running
- [x] **Flow end triggers idle** — clear `has_active_flows`, verify idle timeout starts
- [x] **Flow status on worker disconnect** — verify flows are considered dead. **BROKEN**: active_flows not cleared on worker disconnect

### 10. Scale & Convergence Stress

- [ ] **Many workloads in one namespace** — 10+ workloads, verify all schedule and reach Running
- [ ] **Rapid spec churn** — update spec 5 times in quick succession, verify final state matches final spec
- [ ] **All workers disconnect simultaneously** — verify graceful degradation to WaitingForCapacity

### 11. Pressure Band Transitions

- [ ] **Hysteresis** — pressure oscillates around a threshold, verify band doesn't flap
- [ ] **Critical pressure preemption cascade** — one worker hits Critical, verify preemption + WaitingForCapacity but no cascade (one preemption per attempt)
- [x] **Pressure recovery** — pressure drops, verify waiting pods schedule immediately (works for 2 namespaces; 3+ is broken, see #6)

## Prioritization

Rough prioritization of the gaps above, considering bug-finding likelihood, complexity of the code under test, and how well the orchestrator's own tests already cover the area.

### High Value

**Transition Intent Races (#2)** — This is where the nastiest production bugs live. The orchestrator has its own scenario tests for the state machine, but the full-stack simulation adds the protocol/async layer where timing-dependent bugs actually manifest. The state machine tests prove the logic is correct given the right inputs; these tests prove the inputs actually arrive correctly through the full stack. Low harness investment needed — mostly just careful sequencing of existing `send_activation_traffic` / `disconnect_worker` / `update_namespace` calls without `converge()` between them.

**Multi-Worker Fabric & Routing (#1)** — The multi-worker path is the most under-tested critical path. Only one test uses multiple workers, and it was written specifically because of a bug (SimGatewayProvider flat HashMap). Cross-worker traffic delivery and segment isolation are correctness-critical for production and currently have zero full-stack coverage. Moderate harness investment — `SimGatewayProvider` may need work to route packets between workers rather than just capturing them.

**Flow Tracking (#9)** — `EndpointFlowStatus` directly controls whether a workload stays alive or gets suspended. Getting this wrong means killing active connections. There's only one regression test for it. The interaction between service demand and flow-based demand is subtle and worth exercising. No harness investment needed — `inject_worker_event` already supports this.

### Medium Value

**Worker Reconnection (#4)** — Important for production resilience, but the failure mode (stale state conflicts) is somewhat guarded by the orchestrator treating reconnection as disconnect+fresh connect. Still worth testing because the full namespace re-sync on reconnect is complex. Low harness investment — just need an `add_worker` that reuses a previous worker's identity.

**Snapshot Placement & Artifacts (#5)** — Matters for the suspend/resume experience (cold-start vs fast restore). The artifact transfer path especially — suspend on A, lose A, resume on B — is a real production scenario that's completely untested. Moderate harness investment — needs `inject_pool_capacity()` and TestVmm artifact tracking.

**Multi-Namespace Interactions (#6)** — Mostly a sanity check that namespaces don't interfere. The "preemption is namespace-scoped" test is the most valuable one here since a bug there could preempt the wrong user's workload. Low harness investment.

**Pressure Band Transitions (#11)** — The hysteresis behavior and cascade prevention are important scheduling properties. The existing pressure tests cover the happy path well, but the edge cases (oscillation, recovery) are where scheduling instability would show up. No harness investment needed.

### Lower Value (Still Worth Doing)

**Endpoint Buffering & Flush (#3)** — Important for user experience (no dropped requests during activation), but the buffering logic lives in the fabric on the worker side. These tests would validate the end-to-end path but the core logic is better tested at the unit level. Higher harness investment — needs packet capture on SimGateway.

**Resource Leases (#8)** — Leases prevent overcommit, but the failure modes (timeout, double-booking) are somewhat self-healing — a timed-out lease just means a retry. Worth testing but less likely to find critical bugs. Moderate harness investment — needs lease introspection.

**Scale & Convergence (#10)** — Useful as a regression signal (did convergence get slower?) but less likely to find correctness bugs. The fake-time deterministic model means these tests are reliable, which is nice. Low harness investment but potentially slow to write well.

**WireGuard Peer Lifecycle (#7)** — Valuable feature coverage, but requires the most harness investment of any gap. The orchestrator already has 15 WireGuard unit tests. Worth doing once the WireGuard integration stabilizes, but not the best ROI right now.

## Harness Investments

Capabilities the test harness would need to support some of the above gaps, roughly ordered by ROI.

| Investment | Effort | Tests Unlocked | Notes |
|---|---|---|---|
| **Multi-event injection helpers** (rapid-fire sequences) | Low | Race conditions, transition intents, churn | Mostly convenience wrappers around existing methods, skipping `converge()` between calls |
| **`inject_pool_capacity()`** | Low | Storage pressure, eviction, snapshot-lost | Similar pattern to existing `inject_pressure()` |
| **Lease introspection** (`active_leases()` on orchestrator) | Low | Lease timeout, cleanup, double-booking | Read-only accessor on orchestrator state |
| **Packet capture on SimGateway** (count/inspect forwarded packets) | Medium | Buffering, flush, cross-worker routing, flow tracking | Needs `Arc<Mutex<Vec<CapturedPacket>>>` or similar on SimGateway; also needs cross-worker gateway wiring |
| **Convergence metrics** (rounds-to-converge tracking) | Medium | Scale/performance regression detection | Instrument the `converge()` loop, return round count |
| **WireGuard peer simulation** | High | All WireGuard tests | Needs ConnectNetwork/DisconnectNetwork command injection + synthetic peer packet generation |
