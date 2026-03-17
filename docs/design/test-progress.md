# Scenario Test Progress — Old → New Migration

## Remaining Work

Tracks what remains before the old codepaths (`orchestrator/`, `namespace/`,
`shell/`, `sm/`) can be removed. The core reconciliation loop (adapters, tasks,
SM layer, event loop) is complete. The gaps below are features the old code
provides that the new code does not yet cover.

### Must have (system non-functional without)

- [x] **Endpoint delivery to workers** — `EndpointAdapter` produces actions and
  the namespace task translates them to `WorkerCommand::EndpointUpdate` via
  `build_endpoint_command()`, broadcasting to all connected workers. `Update`
  actions send an `EndpointSpec` with the service backend populated; `Remove`
  actions send an `EndpointSpec` with `backend: None` (preserving the service
  in the worker's endpoint table for traffic buffering). Protocol worker IDs
  are tracked via `proto_worker_ids` map, populated from
  `NamespaceEvent::WorkerConnected`.

- [x] **Service registry sync** — Folded into `EndpointAdapter`. The adapter
  tracks a `registry: HashMap<String, Ipv4Addr>` cache alongside the endpoint
  cache. `update_registry()` diffs against the cache and returns
  `RegistryAction::Update { added, removed }`. `build_registry_sync()` returns
  a full `RegistryAction::Sync` for initial worker population. The namespace
  task calls `update_registry()` on `UpdateSpec` and broadcasts the delta to
  all workers. On `WorkerConnected`, it sends `build_registry_sync()` to the
  new worker. The namespace task translates `RegistryAction` to
  `WorkerCommand::RegistrySync`/`RegistryUpdate` via `build_registry_command()`.

- [x] **Worker registry sync (inter-worker tunnels)** — Implemented in
  `task/worker_state.rs`. The `WorkerStateTracker` now tracks tunnel info
  (public key, listen port from `WorkerReady` handshake), protocol worker IDs,
  writer handles, and per-worker namespace segment sets. On worker
  connect/disconnect or segment assignment changes, it rebuilds
  `Vec<WorkerPeerInfo>` and broadcasts `WorkerCommand::WorkerRegistrySync` to
  all workers. The shell sends `WorkerStateEvent::RegisterNamespaceSegment` /
  `UnregisterNamespaceSegment` on namespace create/destroy, and
  `NamespaceAssigned` / `NamespaceUnassigned` when workers join namespaces.
  Segment IDs are allocated sequentially by the shell (starting at 1).

- [x] **Artifact placement tracking** — `PlacementTable` in the global
  scheduler tracks which artifacts exist on which workers. Worker reader
  routes `ArtifactWriteStarted`/`ArtifactWriteCommitted`/
  `ArtifactTransferReceived`/`TransferFailed` to the scheduler via
  `SchedulerInput::ArtifactEvent`. `select_worker()` uses soft affinity:
  workers with a ready copy of the pod's `resume_artifact` are preferred,
  with graceful fallback to any eligible worker. Placements are purged on
  `WorkerRemoved`. Artifact ID conversion between `sm_new::ArtifactId(u64)`
  and `protocol::ArtifactId(String)` happens at the namespace task boundary
  via bidirectional maps in `IdMaps`. The scheduler and placement table
  operate exclusively on protocol artifact IDs — no type conversion in
  pure scheduling logic. Protocol artifact IDs should include a UUID or
  namespace prefix for global uniqueness (generated when sending
  `SuspendPod` — not yet implemented).

- [x] **Endpoint flow event routing** — `EndpointActivation` and
  `EndpointFlowStatus` events are now routed from `task/worker_reader.rs` to
  namespace tasks. `EndpointActivation` with `service_id` sends
  `ActivateService(true)` to the service SM (triggering idle→active).
  `EndpointFlowStatus` with `service_id` uses `FlowDemandAdapter`
  (`adapter/flow_demand/`) — a push-based adapter that creates BackendNeed
  ports (reusing the existing port type). The service SM's
  `BackendNeedAggregator` sees both worker-reported need and flow-sourced
  need, taking the max — this keeps services alive while flows exist and
  prevents idle timeout. Ports are cleaned up on worker disconnect. Events
  without `service_id` (direct IP access) are not yet supported.

- [x] **Namespace creation on workers** — The shell (`task/shell.rs`) sends
  `WorkerCommand::CreateNamespace` (with `NetworkConfig` including segment_id)
  to workers during namespace-worker assignment, before sending
  `WorkerConnected` to the namespace task. The namespace task defers
  `router.create_worker()` until `NamespaceCreated` arrives — workers don't
  exist in the router until fabric is ready, naturally preventing pod
  scheduling. `NamespaceFailed` removes the worker from pending and logs the
  error. Worker reader routes `NamespaceCreated`/`NamespaceFailed` events to
  namespace tasks.

- [x] **Segment ID allocation** — `task/shell.rs` allocates segment IDs with
  wrapping and reuse (mirrors old `alloc_segment_id()`/`free_segment_id()`).
  `task/worker_state.rs` tracks namespace→segment mappings and per-worker
  segment sets for worker registry broadcasts.

### Important (features break without)

- [ ] **WireGuard client VPN** — Old code has `WgPeerManager` for client
  connect/disconnect: allocates IPs from subnet, sends
  `AddWireGuardPeer`/`RemoveWireGuardPeer` to workers. `ClientCommand` enum
  needs Connect/Disconnect variants, namespace task needs WG peer state.
  Old code: `namespace/wireguard.rs`.

- [ ] **Client command coverage** — Old code handles: `ListNamespaces`,
  `ListWorkers`, `GetWorker`, `ListPods`, `GetNamespaceStatus`,
  `DrainWorker`/`UndrainWorker`, `Connect`/`Disconnect`. Audit new
  `ClientCommand` enum and shell for coverage. Old code:
  `orchestrator/client.rs`.

- [ ] **Preemption** — Old code has `try_preempt_for_workload()` with priority
  scoring (active traffic > idle-but-demanded > always-on). The new SM layer
  may handle demand via signals, but the triggering logic (when capacity is
  tight) needs to exist somewhere. Old code:
  `orchestrator/scheduling.rs` `try_preempt_for_workload()`.

### Nice to have (observability, minor events)

- [ ] **Log and event subscriptions** — Old `shell/subscriptions.rs` manages
  streaming pod logs and SM events to clients. Needed for `StreamLogs` and
  observability dashboards.

- [ ] **Unhandled protocol events** — `ShuttingDown`, `TunnelStatus`,
  `PodLogStreamError` are silently dropped in `worker_reader.rs`.

## Scenario Test Harness — Transplant Progress

**Date:** 2026-03-16 (updated 2026-03-17)

The old scenario test harness (`tests/harness/`) has been transplanted from
`OrchestratorShell` to `SyncShell`. The scenario tests now exercise the new
`core/` + `sm_new/` + `adapter/` layer through the synchronous test shell.

### Results

- **84 scenario tests:** 60 pass, 18 fail, 6 ignored

The 18 failing tests serve as a precise TODO list of behavioral differences
and missing features.

### Passing tests (55)

| Category | Test |
|----------|------|
| activation | `test_activation_idle_cycle` |
| activation | `test_always_on_service_lifecycle` |
| fabric_routing | `test_fabric_route_lifecycle_with_suspend_resume` |
| fabric_routing | `test_fabric_route_update_on_pod_launch` |
| fabric_routing | `test_route_miss_ignored_for_unknown_ip` |
| fabric_routing | `test_route_miss_ignored_when_already_running` |
| failure_recovery | `test_failed_workload_recovery_via_spec_change` |
| failure_recovery | `test_pod_exit_code_zero_no_backoff` |
| failure_recovery | `test_pod_exit_while_running` |
| failure_recovery | `test_pod_launch_failure_recovery_on_success` |
| failure_recovery | `test_pod_launch_failure_retries` |
| failure_recovery | `test_failed_condition_in_status_report` |
| failure_recovery | `test_failed_condition_lifecycle` |
| failure_recovery | `test_retry_backoff_condition_in_status_report` |
| failure_recovery | `test_retry_backoff_condition_lifecycle` |
| failure_recovery | `test_resume_failure_falls_back_to_cold_launch` |
| multi_service | `test_add_service_to_running_workload` |
| multi_service | `test_add_service_to_suspended_workload` |
| multi_service | `test_always_on_multi_service_both_get_create_service` |
| multi_service | `test_late_joining_worker_receives_create_service` |
| multi_service | `test_remove_only_active_service_drops_demand` |
| multi_service | `test_service_activation_while_already_running` |
| multi_service | `test_remove_service_updates_demand` |
| multi_service | `test_two_services_one_workload_shared_demand` |
| preemption | `test_no_preemption_of_active_traffic_workloads` |
| preemption | `test_no_preemption_when_capacity_exists` |
| pressure | `test_all_workers_high_pressure_no_scheduling` |
| pressure | `test_normal_pressure_keeps_full_timeout` |
| pressure | `test_pod_count_tiebreaker_at_same_pressure` |
| pressure | `test_pod_scheduled_on_lower_pressure_worker` |
| pressure | `test_pressure_relief_triggers_scheduling` |
| registry | `test_registry_sync_on_namespace_create` |
| registry | `test_registry_sync_sent_to_new_worker` |
| registry | `test_registry_update_on_service_change` |
| registry | `test_non_tunnel_workers_excluded_from_registry_entries` |
| registry | `test_worker_registry_sync_with_tunnel_workers` |
| snapshot_placement | `test_resume_pinned_to_artifact_worker` |
| spec_reconciliation | `test_add_workload_to_existing_namespace` |
| spec_reconciliation | `test_image_change_restarts_running_workload` |
| spec_reconciliation | `test_remove_workload_from_namespace` |
| spec_reconciliation | `test_suspend_on_idle_flag_change` |
| suspend_resume | `test_activation_no_suspend_cold_start` |
| suspend_resume | `test_delete_during_resume` |
| suspend_resume | `test_pod_exit_during_suspend` |
| suspend_resume | `test_pod_exited_during_suspend` |
| suspend_resume | `test_resume_from_suspended` |
| suspend_resume | `test_spec_change_during_resume` |
| suspend_resume | `test_suspend_failure_fallback_to_stop` |
| suspend_resume | `test_suspend_timeout_fallback_to_stop` |
| transition_intents | `test_demand_during_suspend_immediate_resume` |
| transition_intents | `test_demand_up_during_resume` |
| transition_intents | `test_force_deactivate_during_launch` |
| transition_intents | `test_spec_change_during_launch` |
| worker | `test_all_workers_disconnect` |
| worker | `test_worker_disconnect_and_recovery` |
| worker | `test_worker_disconnect_during_launch` |
| worker | `test_worker_disconnect_during_resume` |
| worker | `test_worker_disconnect_during_suspend` |
| worker | `test_multi_worker_reschedule` |
| pressure | `test_workload_reschedules_to_lower_pressure_worker_after_disconnect` |

### Failure categories

The remaining failures break down into these root causes:

#### 1. Worker disconnect — suspended artifact not cleared (1 test)

| Test | Expected | Got |
|------|----------|-----|
| `worker::test_worker_disconnect_clears_placements` | Dormant | Suspended |

**Fixed (2026-03-18):** `SyncShell::disconnect_worker()` was missing
`process_new_worker_commands()` after `execute_effects()`, so worker
commands generated by the disconnect (e.g. `LaunchPod` for rescheduled
pods) were never auto-responded to. Fixed `test_multi_worker_reschedule`
and `test_workload_reschedules_to_lower_pressure_worker_after_disconnect`.

**Remaining:** `test_worker_disconnect_clears_placements` fails because
the workload stays `Suspended` when the worker hosting its artifact
disconnects. **Root cause:** When a workload is Suspended, its pod has
already been reaped (destroyed). The signal path Worker→Pod→Workload is
severed. When the worker disconnects, `router.destroy_worker()` fires,
but there is no pod connected to relay `Displaced` to the workload. The
workload's `on_pod_displaced()` handler (which clears
`suspended_artifact`) is never reached, so the workload stays in
`Suspended` status instead of transitioning to `Dormant`.

#### ~~2. Multi-service: late-joining service stuck in NeedBackend~~ — FIXED (2026-03-17)

**Fixed 2 of 3 tests.** Added `last_readiness` cache to `ServiceSm` so that
`Idle → NeedBackend` transitions skip directly to `Active` when readiness was
delivered while idle (the router suppresses re-delivery of unchanged signals).
Stateright model extended with two new safety properties:
`"NeedBackend implies no readiness"` and `"last_readiness consistent with env"`.

Remaining test `test_remove_service_updates_demand` fails for a different
reason — asserts `EndpointUpdate` with `removed_ips` on service deletion,
which is a missing endpoint cleanup path (moved to #7).

#### ~~3. Harness stubs — unimplemented features~~ — PARTIALLY FIXED (2026-03-17)

**Fixed (2026-03-17):** Workload condition tracking (4 tests). The
`workload_conditions()` harness stub was returning an empty map. The new
`WorkloadSm` already tracks `consecutive_failures`, `max_retries`, and
`in_backoff` — the fix derives `"failed"` and `"retry-backoff"` conditions
from this state, matching the old `ConditionSet`/`ConditionClear` output
behavior.

**Remaining (7 tests):** These still panic at unimplemented harness methods.

| Stub | Tests | Count |
|------|-------|-------|
| `drain_worker` / `undrain_worker` | All `drain::*` tests | 5 |
| `assert_service_condition_set/clear` | `activation_pending_condition_lifecycle`, `activation_pending_in_status_report` | 2 |

#### ~~4. Pressure-based scheduling and idle timeout~~ — PARTIALLY FIXED (2026-03-17)

**Fixed (2026-03-17):** Pod count tracking and PSI-based pressure scheduling.

- Added `WorkerStateCoreEvent::PodCountChange` — orchestrator emits `+1` on
  `Grant`, `-1` on `Revoke` (also added `worker_id` to `SchedulerDecision::Revoke`).
  `WorkerStateCore` updates `pod_count` and recomputes pressure/candidate.
- Rewrote 4 scheduling tests to inject explicit PSI metrics via
  `send_pressure_update()` instead of relying on synthetic memory accounting.
- **4a** (2 tests) and **4c** (2 tests): now pass.

**Ignored (6 tests):** Pressure-adjusted idle timeout is not yet plumbed into
the new service SM (`sm_new/service.rs` uses `idle_timeout` directly from spec
with no pressure adjustment). These tests are `#[ignore]`d:

| Test |
|------|
| `pressure::test_elevated_pressure_shortens_idle_timeout` |
| `pressure::test_high_pressure_quarter_timeout` |
| `pressure::test_critical_pressure_uses_floor_timeout` |
| `pressure::test_pressure_change_between_cycles_updates_timeout` |
| `pressure::test_psi_pressure_after_activation_shortens_idle_timeout` |
| `pressure::test_reactivation_cancels_shortened_idle_timer` |

#### 5. Route-miss activation (3 tests)

| Test | Symptom |
|------|---------|
| `fabric_routing::test_route_miss_activates_dormant_workload` | expected Running, got Dormant |
| `fabric_routing::test_route_miss_activates_suspended_workload` | expected Running, got Suspended |
| `fabric_routing::test_route_miss_demand_leak` | expected Dormant, got Running |

**Root cause:** Tests inject `EndpointActivation` without a `service_id`
(direct IP access). The namespace core's event handler for
`EndpointActivation { service_id: None, .. }` likely doesn't resolve the
IP to a service and activate it. The old code looked up the service by IP
in the endpoint table and sent `ActivateService`.

#### ~~6. Registry sync (2 tests remaining)~~ — FIXED (2026-03-17)

**Fixed (2026-03-17):** DNS service registry moved from spec-driven side-channel
in `EndpointAdapter` to a proper router port (`DnsRegistry`). Services signal
`DnsEntry(Option<DnsEntryInfo>)` to a singleton `DnsRegistry` port via
`ServiceToDnsRegistry` edges. `DnsRegistryAdapter` uses incremental aggregation
and maintains a cache for new-worker full sync. Tests updated to expect
`RegistryUpdate` (incremental) instead of `RegistrySync` (full) on initial
spec application. `WorkloadToDnsRegistry` edge also declared for future use.

**Fixed (2026-03-17):** Worker registry sync (inter-worker tunnel peer
discovery via `WorkerRegistrySync`). The core logic in `WorkerStateCore` was
already complete — `SyncShell::add_worker()` was hardcoding `tunnel_info: None`
instead of passing through the `MockWorkerConfig`'s tunnel info. Added
`tunnel_info` field to `MockWorkerConfig`, populated in `with_tunnel()`, and
wired through to `WorkerConnectedInfo`.

#### 7. Remaining feature gaps (6 tests)

| Test | Root cause |
|------|-----------|
| `preemption::test_basic_preemption` | Preemption trigger logic not implemented |
| `preemption::test_preempted_workload_can_reactivate` | Preemption trigger logic not implemented |
| `worker::test_worker_condition_stored_on_event` | Worker condition tracking not in new core |
| `worker::test_worker_condition_in_status_report` | Worker condition tracking not in new core |
| ~~`spec_reconciliation::test_image_change_on_suspended_workload`~~ | ~~expected Suspended, got Running~~ |
| ~~`transition_intents::test_spec_change_during_suspend`~~ | ~~expected Suspending, got Running~~ |
| `snapshot_placement::test_artifact_lost_on_worker_disconnect_cold_launch` | Same root cause as #1 (suspended artifact not cleared on worker disconnect) |

**Partially fixed (2026-03-17):**
`test_image_change_on_suspended_workload` and `test_spec_change_during_suspend` —
WorkloadSm now clears `suspended_artifact` when image changes (stale artifact
from old spec), and discards artifacts arriving via `PodSuspended` when
`spec_version != launched_with_spec_version`. Stateright model strengthened:
added `"suspended artifact matches current spec"` safety property and expanded
`ChangeImage` action guard to cover Suspended state (was previously gated out).
Both tests now pass the status assertion (Dormant instead of Suspended) but
still fail on `DeleteArtifact` — the SM discards the artifact internally but
the namespace boundary layer doesn't yet translate that into a
`WorkerCommand::DeleteArtifact` to clean up the worker's disk.
| ~~`multi_service::test_remove_service_updates_demand`~~ | ~~Missing EndpointUpdate with removed_ips on service deletion~~ |

**Fixed (2026-03-17):** `test_remove_service_updates_demand` —
`build_endpoint_command()` for `EndpointAction::Remove` was upserting an
endpoint with `backend: None` instead of putting the service IP into
`removed_ips`. Fixed to use `removed_ips` so the worker fully removes the
endpoint rather than keeping it for buffering.

**Fixed (2026-03-17):** `test_late_joining_worker_receives_create_service` — endpoint
signals made self-contained (`ServiceEndpointInfo` carries `service_ip`, `policy`,
`pod_ip`, `worker_id`), so `build_sync()` and `build_endpoint_command()` no longer
need spec lookups. New workers receive correct endpoint state from the cache directly.

### Recommended priority order

| Priority | Fix | Tests unlocked | Effort |
|----------|-----|---------------|--------|
| ~~**P0**~~ | ~~Worker disconnect remaining edge cases (#1)~~ | ~~2~~ | ~~PARTIAL — 2026-03-18~~ |
| ~~**P1**~~ | ~~Multi-service late-joining readiness (#2)~~ | ~~3~~ | ~~DONE~~ |
| **P2** | Route-miss IP→service lookup (#5) | 3 | Small |
| **P3** | Pressure-adjusted idle timeout (#4 remaining) | 6 (currently ignored) | Medium |
| ~~**P4**~~ | ~~Workload condition derivation (#3 partial)~~ | ~~4~~ | ~~DONE — 2026-03-17~~ |
| ~~**P5**~~ | ~~DNS registry + worker registry via router port (#6)~~ | ~~4~~ | ~~DONE — 2026-03-17~~ |
| ~~**P6**~~ | ~~Pressure scheduling / pod_count tracking (#4a,c)~~ | ~~4~~ | ~~DONE — 2026-03-17~~ |
| **P7** | Suspended artifact + worker disconnect (#1 remaining) | 2 | Medium |
| **P8** | Preemption, conditions, misc (#7) | 4 | Large |

P4 + P5 + P6 done. Current: 60 pass, 18 fail, 6 ignored. Two tests partially
fixed (status correct, pending DeleteArtifact plumbing). P2 would take us to ~63/84 passing.

### Visibility changes

To make `SyncShell`, `NamespaceCore`, and SM types accessible from integration
tests (`tests/e2e.rs`), the following modules were made `pub`:

- `lib.rs`: `sm_new`, `adapter`, `task`, `core`, `shell_new`
- `core/mod.rs`: `types`, `namespace`, `orchestrator`, `worker_event`, `worker_state`
- `adapter/mod.rs`: `management`, `timer`
- All key types in `sm_new/` (struct fields, enums, constants)
- `OrchestratorCore`, `NamespaceCore` structs

### Harness architecture

The new `TestHarness` wraps `SyncShell` and uses `RefCell`-based interior
mutability for the `WorkerProxy` pattern (`h.worker(&w1).send_event(...)` works
through shared references). `converge()` drains pending events, runs
`shell.drain()`, then loops with 1ms time advances until quiescent.

Key helper methods on `TestHarness`:
- `workload_proto_pod_id(ns, wl)` — maps router PodId → protocol PodId
- `workload_global_worker_id(ns, wl)` — maps router WorkerId → GlobalWorkerId
- `workload_status(ns, wl)` — derives `WlStatus` from `WorkloadSm` fields
- `service_status(ns, svc)` — derives `SvcStatus` from `ServiceSm` state

New accessors on `NamespaceCore`:
- `current_spec()` — access the stored `NamespaceSpec`
- `router_worker_to_global()` — map router WorkerId → GlobalWorkerId
- `router_pod_to_proto()` — map router PodId → protocol PodId
