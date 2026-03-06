# Fabric Endpoint Migration Audit

Audit performed after Phase 1+2 completion. Covers worker, orchestrator, worker protocol, and test infrastructure.

## Overall Assessment

Migration is sound. Phase 1+2 are well-executed. The main issues are dead code that should be cleaned up and a few behavioral concerns.

---

## 1. Dead Code / Deprecated Protocol (High Priority for Phase 3) — FIXED

~~The old protocol commands are **still defined** but the worker ignores them with a deprecation warning, and the orchestrator **no longer emits them**.~~

**Done:**
- E2E tests migrated from old commands (`CreateService`, `UpdateServiceBackend`, `ServiceReady`, `FabricRouteSync`) to `EndpointSync`/`EndpointUpdate` in `services.rs`, `tunnel.rs`, `suspend_resume.rs`
- Dead `FabricRouteMiss`/`ServiceActivation` event branches removed from `distvirt-compose/src/orchestrate.rs`
- Bridge layer in `orchestrator/src/shell/worker_protocol.rs` marked as deprecated (kept for backward compat with older workers)

**Remaining (Phase 3):**
- Old enum variants still in `WorkerCommand`/`WorkerEvent` + Cap'n Proto schema (wire compat)
- Supporting types (`RouteDestination`, `FabricRouteEntry`, `ServiceBackend`, `BufferPolicy`) still defined
- Worker catch-all for deprecated commands still present in `worker/mod.rs`
- These can be removed once wire compatibility is no longer needed

---

## 2. The `route_miss_wake` Bug (Medium Priority)

The known demand leak is **partially mitigated** but not fixed:

- `workload.rs:39` — flag defined, `workload.rs:383` — explicitly NOT cleared on PodRunning with a comment documenting the bug
- `events.rs:260` — set on `EndpointActivation { service_id: None }`
- `events.rs:279` — cleared on `EndpointFlowStatus { has_active_flows: false }` (partial fix)
- **Not cleared** when a service activates and takes over demand — the demand leak in `test_route_miss_demand_leak` (marked `#[should_panic]` in `fabric_routing.rs:255`)

The flow-tracking path provides a clearing mechanism, but if no `EndpointFlowStatus` event arrives (e.g. traffic stops without explicit TCP close), the flag stays set forever.

---

## 3. Worker Namespace Handler Issues (Medium Priority) — FIXED

**Done:**
- Extracted shared `handle_endpoint_effects()` helper to deduplicate sync/update effect processing
- Added `log::warn!` when `resolve_ip()` returns `None` during pod buffer flush
- Removed unused `_svc_id` variable
- Improved catch-all in `MarkReadyResult` to log unexpected variants instead of silently ignoring

---

## 4. Fabric Unit Test Helpers (Medium Priority for Phase 3) — FIXED

**Done:**
- Removed old `#[cfg(test)]` methods (`create_service()`, `destroy_service()`, `update_service_backend()`) from `endpoint.rs`
- Added new test helpers that build `EndpointSpec` objects and call `apply_endpoint_sync()`/`apply_endpoint_update()`
- Migrated all fabric unit tests to use the new helpers
- `mark_service_ready()` kept — it's a production API, not a test-only method

---

## 5. Orchestrator Test Scenarios Use Old Events — FIXED

**Done:**
- All orchestrator test scenarios migrated from `ServiceActivation`/`FabricRouteMiss` to unified `EndpointActivation`
- Updated `activate_service()` helper in test harness
- Files updated: `fabric_routing.rs`, `multi_service.rs`, `transition_intents.rs`, `suspend_resume.rs`, `failure_recovery.rs`, `activation.rs`, `worker.rs`

---

## 6. Stateright Model Gaps

- `stateright_model.rs:96` — snapshots `route_miss_wake` (known broken)
- Doesn't track `removed_ips` from `EndpointUpdate` — only verifies endpoints are present, not that stale ones are absent

---

## 7. Minor Concerns

- **RemoteSegment endpoints don't buffer** — intentional per design but not documented in code comments. When a pod transitions from Unplaced to RemoteSegment, the new endpoint starts with an empty buffer and Ready state.
- **Hardcoded buffer policy** for `UnplacedPod` (64 frames, 30s timeout) in `endpoint.rs:397-401` — not configurable.
- **Lock ordering** (`mod.rs:11-16`) documented as comment only, not enforced by type system.
- **Stale comment** in `reconciliation.rs:53` references "CreateService already emitted above".

---

## Remaining Work

1. **Fix `route_miss_wake`** — clear when service activates and takes over demand (item 2)
2. **Stateright model** — track `removed_ips`, replace `route_miss_wake` snapshot (item 6)
3. **Remove old protocol variants** — once wire compat no longer needed (item 1 remainder)
4. **Minor concerns** — document RemoteSegment no-buffer semantic, extract buffer policy constant (item 7)
