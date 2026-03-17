---
title: "Orchestrator Design"
---

## Overview

The orchestrator is the central control plane for distvirt. It manages workers, drives namespace lifecycle, handles scale-to-zero activation, and exposes a client protocol (gRPC) for CLI/UI control. The primary use case is scale-to-zero staging environments where idle services consume no resources and activate transparently on first traffic.

The orchestrator is a **pure state machine** at its core. All logic lives in synchronous, deterministic code with no I/O. An async shell dispatches inputs from network connections and timers, and sends outputs to workers and clients. This separation enables:

- **Deterministic unit tests**: Feed a sequence of inputs, assert state and outputs.
- **Property-based testing**: Generate random input sequences, check invariants after every step.
- **Model checking**: Stateright exhaustively explores all interleavings.
- **Fuzz testing**: Coverage-guided fuzzer drives the step function.
- **Reasoning**: No hidden state changes from async timing, channel backpressure, etc.

See [Upcoming Work & Open Questions](#upcoming-work--open-questions) for unimplemented features and known issues.

---

## Developer Experience Contract

### Latency Targets

| Operation | Target | Notes |
|---|---|---|
| Resume from snapshot | < 200ms end-to-end | Firecracker restore ~5-10ms + fabric flush |
| Cold start (VM boot) | < 2s to pod running | ~100ms VM boot + app startup |
| Activation response (resume) | < 500ms from SYN to first response | Resume + buffer flush + TCP handshake |
| Activation response (cold) | < 5s from SYN to first response | Boot + app startup + buffer flush |
| Buffer timeout | Must exceed activation latency | Default: 30s |

The core DX contract: **hitting a dormant service feels like a slow first request, not an error**. HTTP clients with default timeouts (30-60s) should never see a connection failure due to activation.

### Status Visibility

`dv status` reads active conditions from all entity types:

```
Namespace: staging/myapp
Workers: 3 — Pressure: normal

  web          running   (worker-east, pod-42)
  api          suspended
  worker       dormant, snapshot evicted     <- snapshot-lost condition
  migrations   failed: ImagePullError (5/5)  <- failed condition

Services:
  web:8080     active    (BackendNeed::Active)
  api:8080     idle
  worker:9090  idle
```

### Defaults for Staging

- Every service gets `activation: { idle_timeout: 30s }` unless explicitly overridden
- Every workload gets `suspend_on_idle: true` unless explicitly overridden
- A freshly deployed staging environment has zero running pods until traffic arrives

---

## Architecture

### Signal Router

The orchestrator's internal communication is built on a **signal router** — a generic framework for reactive state propagation between state machines. See [Signal Router Design](../design/signal-router.md) for the full framework design and motivation.

The key insight: instead of SMs sending messages to each other, they produce **persistent signals** (current-value outputs) that the router automatically **projects** through typed **edges** (structural relationships) to consuming SMs. Consumers declare **aggregated inputs** that combine signals from multiple sources. This eliminates the "notify all dependents" problem — when a signal changes, the router handles fan-out mechanically.

**Signals** represent "what is the current state" (service demand, workload readiness, pod status). **Events** represent "what just happened" (timer fires, admin commands, worker status reports). Signals are persistent and aggregated; events are one-shot and delivered once.

**Ports** represent external participants in the signal graph — workers, management layer, timers, artifacts. They produce signals and send events using the same instance model as SMs, but have no state machine lifecycle.

**Adapters** sit between the router and the async shell, converting signal deltas into protocol commands (endpoint updates, timer start/cancel, schedule requests) using incremental aggregation.

### Two-Layer Design

The orchestrator has two layers:

1. **Outer layer** (`OrchestratorCore`) — manages worker connections, routes events to namespaces, handles cross-namespace operations (clone, list), allocates network segment IDs, maintains the inter-worker mesh registry, and owns the global scheduler and timer wheel.
2. **Namespace core** (`NamespaceCore`) — a pure, self-contained state machine for a single namespace. Contains the signal router with all SMs, ports, edges, and adapters. All service lifecycle, activation, suspend/resume, and reconciliation logic lives here.

This separation keeps the per-namespace state machine small and independently testable. Cross-namespace interactions are minimal (limited to clones) and handled at the outer layer.

The outer layer also handles pod scheduling: when a namespace emits a schedule request, the global scheduler selects a worker and grants a lease back to the namespace. Worker disconnect fans out to every namespace that had pods on that worker — the namespace removes the worker port, and the router automatically re-aggregates for all affected SMs.

### Per-Namespace Signal Graph

The namespace state machine is built from three SM types and nine port types, connected by typed edges:

**State machines:**
1. **WorkloadSm**: Pod lifecycle — decides whether to create/destroy/suspend pods based on demand and spec. Produces readiness signals for services.
2. **ServiceSm**: Activation and idle timeout — tracks traffic demand, manages idle timers, produces demand signals for workloads and endpoint info for workers.
3. **PodSm**: Individual pod lifecycle — manages launch, run, suspend, and terminal states. Produces schedule requests and status signals.

**Ports (external participants):**
- **Worker** — represents a connected worker; produces `Info` signal; receives pod placement edges; delivers pod status events.
- **Management** — one per managed SM; delivers spec as a signal and admin commands as events. No separate "init" vs "update" code path.
- **Timer** — receives wanted-timer signals from all SMs; fires timer events back. The SM declares what timers it wants; the adapter handles start/cancel.
- **ScheduleRequest** — receives pod schedule requests via incremental aggregation; the boundary layer translates these to scheduler messages.
- **ScheduleLease** — one per granted lease; delivers lease info (worker assignment) to a pod.
- **Endpoint** — receives service endpoint signals; the adapter maintains a cache and emits update/remove actions.
- **DnsRegistry** — receives DNS entry signals from services and workloads; the adapter maintains a cache for registry sync.
- **BackendNeed** — one per (worker, service) pair; delivers fabric traffic demand to services.
- **Artifact** — one per suspended artifact; delivers validity signal to workloads; lifecycle managed by the adapter.

**Key edges and signal flow:**

```
Management --[spec signal]--> Workload --[readiness signal]--> Service
                                  |                               |
                                  |  <--[demand signal]----------/
                                  |
                                  \--[intent+spec signal]--> Pod --[status signal]--\
                                                              |                      |
                                  /--[worker info signal]----/                      |
                                  |                                                  |
                              Worker <--[placement edge]-- Pod                      |
                                                                                     |
                              ScheduleLease --[lease signal]--> Pod                  |
                                                                                     |
                              Workload <--[pod status signal]----------------------/
```

Multiple services can share a single workload. The router handles demand aggregation — the workload runs as long as any service has `Demand(true)`.

This split reduces the model-checking state space from O(states^N) (monolithic, all services interleaved) to O(states x N) (each sub-SM checked independently). WorkloadSm has ~7 observable states; ServiceSm has ~3. Both are small enough for exhaustive stateright exploration.

### Adapters

Adapters convert between the signal router's internal representation and the protocol commands the shell needs to send. They use **incremental aggregation** — the router emits per-item deltas (added/removed/changed) rather than full state, so adapters can produce precise actions without diffing.

| Adapter | Purpose | Aggregation |
|---|---|---|
| **ManagementAdapter** | Translates namespace spec into SM/port creation and spec signal delivery | Full (spec diff) |
| **TimerAdapter** | Converts wanted-timer signals into start/cancel actions; dispatches timer fires | Incremental |
| **ScheduleRequestAdapter** | Drains pod schedule request deltas for the global scheduler | Incremental |
| **PodAssignmentAdapter** | Translates pod placement into Launch/Resume/Stop/Suspend commands | Incremental |
| **EndpointAdapter** | Maintains endpoint cache; emits update/remove for worker sync | Incremental |
| **DnsRegistryAdapter** | Maintains DNS cache; emits add/remove for registry sync | Incremental |
| **EndpointDemandAdapter** | Creates/updates BackendNeed ports from fabric traffic events | Push-based |
| **ArtifactAdapter** | Manages artifact port lifecycle and deduped reference tracking | Incremental |

### Boundary Layer

The **namespace boundary layer** wraps `NamespaceCore` and handles concerns that cross the pure/impure boundary:

- **ID translation**: Protocol IDs (strings) <-> router IDs (typed numeric). Bidirectional maps maintained by the boundary.
- **Port lifecycle**: Creates/destroys worker ports, lease ports, artifact ports in response to external events.
- **Worker command building**: Translates adapter actions into protocol commands (`LaunchPod`, `SuspendPod`, `EndpointUpdate`, `RegistrySync`, etc.).
- **Event routing**: Translates protocol events (worker reports `PodRunning`, `PodSuspended`, etc.) into router events.

### Async Shell

The async runtime is a thin shell that:
- Drives the timer wheel (advances time, fires deadlines)
- Routes worker protocol events to the orchestrator core
- Routes orchestrator outputs to worker commands (via protocol writers)
- Manages client request/response matching via oneshot channels
- Buffers and distributes log streams to subscribers
- Distributes SM events to gRPC streaming subscribers

### Worker Mesh Networking

Workers form a mesh network to carry fabric traffic between namespaces spanning multiple workers. The orchestrator maintains a **worker registry** — a list of all workers with their tunnel endpoints and assigned network segments — and broadcasts it to all workers whenever the segment set changes.

Each namespace is assigned a unique **segment ID** (`u16`, allocated by the outer layer, skipping 0). Workers use segment IDs to route fabric traffic to the correct namespace. The segment ID is freed when the namespace is destroyed.

The worker registry entry contains:
- `worker_id` — identifies the worker
- `endpoint` — `public_endpoint:tunnel_listen_port`
- `public_key` — from the worker's `tunnel_config` (separate from WireGuard)
- `segments` — list of segment IDs for namespaces assigned to this worker

The registry is pushed as `WorkerCommand::WorkerRegistrySync` to all workers on: worker connect/disconnect, namespace create/delete (segment set changes).

This is distinct from WireGuard config (used for developer network access) — the tunnel config is for worker-to-worker mesh connectivity.

### Worker State

Worker state is tracked globally by `WorkerStateCore` in the outer layer:
- `pressure: WorkerPressure` — raw normalized scores per dimension
- `pressure_bands: PressureBands` — hysteresis state per dimension
- `psi: Option<WorkerPsi>` — cached PSI metrics from last `PressureUpdate`
- `capabilities: WorkerCapabilities` — max pods, adapters, pools
- `conditions: HashMap<String, bool>` — custom conditions (e.g., `"draining"`)
- `pod_count: usize` — current pod count on worker
- `tunnel_info: Option<WorkerTunnelInfo>` — inter-worker mesh tunnel config
- `segments: HashSet<u16>` — network segments assigned to this worker

Within namespaces, workers are **ports** in the signal graph. Each worker port produces an `Info` signal with worker identity and capabilities. Pods receive worker info through `WorkerAssignment` edges — a pod doesn't store "which worker am I on" as internal state; it's always a projected signal.

The scheduler reads global worker state for placement decisions. Pressure updates flow from `WorkerStateCore` to the scheduler. The namespace signal graph never writes back to global state.

---

## Core Abstractions

### Worker Pressure Score

**Problem**: N independent signals (PSI at multiple averages, pool watermarks, pod count, memory committed) each wired to M policy decisions creates an N*M matrix of threshold tuning with inconsistent behavior.

**Design**: A normalized `WorkerPressure` per resource dimension (compute, memory, storage, network), each 0.0-1.0. Each dimension is the **max** of its available inputs, normalized. On non-Linux workers (libkrun/macOS), PSI inputs are absent — the score falls back to static accounting. Same thresholds, same policy code, no special-casing.

Input mapping:
- `compute`: PSI cpu some_avg10 / 100 (no static fallback — 0.0 without PSI)
- `memory`: max(PSI memory some_avg10 / 100, pods_memory_committed / available_memory_mb)
- `storage`: max(PSI io some_avg10 / 100, pool_used / pool_capacity)
- `network`: fabric tunnel utilization (future extension, initially 0.0)

#### Hysteresis

Pressure band thresholds use hysteresis to prevent oscillation at boundaries:

| Band | Enter At | Leave At |
|---|---|---|
| Normal | -- | -- |
| Elevated | 0.50 | 0.40 |
| High | 0.80 | 0.70 |
| Critical | 0.95 | 0.85 |

The hysteresis state is per-dimension. The effective band for policy decisions is the **maximum** band across all dimensions.

#### Policy Band Effects

**Scheduling** — the max band across all dimensions determines eligibility:

| Band | Scheduling Effect |
|---|---|
| Normal | Full priority |
| Elevated | Deprioritize |
| High | Exclude |
| Critical | Exclude |

**Dimension-specific responses**:

| Dimension | Elevated | High | Critical |
|---|---|---|---|
| compute | Shorten idle timeout | Preempt priority 3-4 | Preempt priority 2-4 |
| memory | Shorten idle timeout | Preempt priority 3-4 | Preempt priority 2-4 |
| storage | Proactive snapshot migration | Aggressive eviction | Emergency eviction |
| network | (future) | (future) | (future) |

**Idle timeout under pressure** (compute/memory only):

| Pressure Band | Idle Timeout |
|---|---|
| Normal | Configured timeout |
| Elevated | 75% of configured |
| High | 25% of configured (minimum 5s floor) |
| Critical | Immediate (5s floor) |

The 5s floor prevents thrashing where a workload activates, immediately idles, activates again.

#### Update Cadence

Pressure scores are recomputed on: periodic `PoolCapacityUpdate` events (30s), `PressureUpdate` events (10s periodic + immediate on threshold crossings), and pod start/stop. The pressure score is a derived value in the orchestrator's `WorkerStateCore`.

---

### Demand and Transition Management

**Problem**: A state transition is in flight (async) when a contradicting signal arrives. Examples: traffic arrives while mid-suspend, spec changes during pod launch, demand drops during resume.

**Design**: Three mechanisms work together, replacing the old `PendingIntent` enum approach:

#### `has_demand` — signal-driven demand

Demand is not a message (`SetDemand`). Each service produces a `Demand(bool)` signal. The router aggregates all service demand signals through `ServiceDemand` edges using `DemandAggregator`, which counts services with `demand=true`. The workload receives a single `DemandInput` with the count. No manual enumeration of services, no incremental DemandUp/DemandDown — demand is always consistent with actual service states.

Active flow tracking (TCP sessions in progress) is handled by `EndpointDemandAdapter`, which creates `BackendNeed` ports per (worker, service) pair. The service's `BackendNeedAggregator` takes the max of worker-reported need and flow-sourced need, keeping services alive while flows exist.

#### `committed_to_boot` — demand stability during boot

Once demand transitions 0->non-zero, the workload sets `committed_to_boot = true`. This prevents fluctuating demand from aborting a pod launch:

- Traffic arrives (demand=1) -> pod creates, starts launching
- Brief traffic drop (demand=0 momentarily) -> pod continues (committed)
- Traffic returns (demand=1) -> already launching, no disruption

Cleared when: pod reaches Running (commitment fulfilled), `Scavenge` admin command, or pod destroyed with no remaining demand.

#### `spec_version` — image change detection

Instead of a `PendingIntent::Restart`, the workload tracks a `spec_version` counter (incremented on image changes) and `launched_with_spec_version` (the version when the current pod was created). When a pod reaches Running or Suspended, the workload checks whether the versions match. If not, the image changed during the transition — destroy the pod and restart with the new spec.

This handles all the old `PendingIntent::Restart` cases:
- Image changes during launch -> pod reaches Running, versions don't match -> restart
- Image changes during suspend -> pod reaches Suspended, versions don't match -> discard artifact, go Dormant, relaunch
- Image changes while Dormant -> next launch uses new spec naturally

#### `PodIntent` — workload-to-pod intent signal

The workload communicates intent to its pod via a `PodIntent` signal (not a queued intent):
- `Want` — workload wants the pod running
- `Suspend` — workload wants the pod to suspend (snapshot for fast resume)
- `None` — no active intention (pod should wind down)

The pod reacts to the **current** intent, not a pending one. If intent changes while a transition is in flight, the pod sees the new intent on its next handler invocation and adjusts accordingly.

The principle remains: **never discard a signal because the system is busy**. But the mechanism is now reactive signals rather than priority-ordered intent slots.

---

### Condition Model

**Problem**: Policy-relevant state is scattered across enum variants, log messages, and implicit behavior. Status display and event streaming need custom visibility logic for each failure mode.

**Design**: Conditions are derived from SM state rather than explicit set/clear operations. The workload SM exposes `ConsecutiveFailures(u32)` and `Status(WlStatus)` signals — conditions like `failed` and `retry-backoff` are derived at the query layer from these signals.

**Worker conditions**: `storage/pool/<id>/pressure-soft`, `storage/pool/<id>/pressure-hard`, `pressure/compute`, `pressure/memory`, `draining`, `pool/<id>/degraded`, `unresponsive`.

**Workload conditions** (derived from SM state): `failed` (consecutive_failures >= max_retries), `retry-backoff` (in_backoff = true), `snapshot-lost` (artifact invalidated, will cold-start).

**Service conditions** (derived from SM state): `activation-pending` (state = NeedBackend), `backend-not-ready` (pod running but app not ready — future, needs readiness probes).

**Observability**: `dv status` = snapshot all active conditions per entity. `dv events` = stream of condition transitions. Alerting = condition active longer than threshold.

---

### Resource Leases

**Problem**: Resources (pod slots, memory, artifact entries) are claimed during async operations that can fail. On failure, resources are "leaked." Concurrent scheduling can race past capacity limits.

**Design**: The global scheduler tracks leases. When a pod needs capacity, the scheduler grants a lease on a worker and creates a `ScheduleLease` port in the namespace's signal graph. The lease port delivers `Lease { worker_id }` to the pod via a `PodLease` edge. The pod then creates a `PodPlacement` edge to the worker, completing placement.

**Lifecycle**:
1. **Grant**: Scheduler selects worker, creates lease port, signals lease to pod. Worker capacity is immediately decremented.
2. **Commit**: Pod reaches Running. Lease remains active.
3. **Revoke**: Scheduler revokes lease (e.g., worker disconnect). Lease port destroyed; pod sees lease disappear and transitions to Displaced.
4. **Release**: Pod reaches terminal state and is reaped. Scheduler releases the capacity reservation.

Leases are tracked in the scheduler, not in the namespace. The namespace sees leases only as port instances in the signal graph.

---

## Scheduling & Capacity

### Worker Selection

When a pod emits a `ScheduleRequest` signal, the adapter drains it and forwards to the global scheduler. The scheduler selects a worker via two-phase selection:

1. **Hard constraints** (filter):
   - Worker has active fabric for the namespace
   - Not draining
   - Pressure score below High threshold on all dimensions

2. **Soft preferences** (rank):
   - Lowest pressure band, then fewest pods
   - Snapshot locality (prefer worker holding the artifact for resume)

3. **Reserve capacity** (lease):
   - Grant a lease, create `ScheduleLease` port in the namespace
   - Pod receives lease signal and creates placement edge to worker

When no worker has capacity, the pod stays in Pending and preemption is considered.

### Preemption

Preemption is namespace-scoped. Priority is derived from runtime state, not spec-declared:

| Priority | Description | Preemptable? |
|---|---|---|
| 1 (highest) | **Activated** — traffic just arrived, client is blocked waiting | Never |
| 2 | **Active with traffic** — BackendNeed::Active/Traffic, sessions in progress | Never |
| 3 | **Active but idle** — running, BackendNeed::None, idle timer ticking | Yes |
| 4 | **Always-on, no traffic** — running by policy, no current demand signal | Yes |
| 5 (lowest) | **Suspended** — consuming storage only | Evict snapshot if storage-pressured |

**Preemption flow**: When the scheduler finds no worker with capacity, it scans same-namespace workloads for running preemptable candidates (priority 3-4), selects a victim, and dispatches `ForceDeactivate`. The victim follows the normal deactivation path (suspend or stop). The waiting pod is naturally retried once the victim's slot frees.

One preemption per scheduling attempt (avoids cascading evictions). Under elevated pressure (score > 0.8), preemption can be triggered proactively even when pod count hasn't hit a hard limit.

### Worker Drain

Drain uses the existing condition model — `DrainWorker` sets a `"draining"` condition on `WorkerStateCore.conditions`. Scheduling excludes draining workers. Existing pods deactivate on their normal idle timeout. `UndrainWorker` clears the condition.

---

## Lifecycle & Activation

### Service Activation Flow

1. Traffic arrives at a service IP on the fabric
2. Protocol activator (TCP/H2/Postgres) inspects the packet — meaningful traffic (TCP SYN, H2 HEADERS) -> `EndpointActivation` event
3. Boundary layer routes to namespace; `EndpointDemandAdapter` creates/updates a `BackendNeed` port
4. Service SM receives `BackendNeedInput`, transitions Idle -> NeedBackend (or directly to Active if `last_readiness` cached)
5. Service updates `Demand(true)` signal; router aggregates and delivers to workload via `ServiceDemand` edge
6. Workload SM: sets `committed_to_boot`, creates PodSm, sets `PodIntent::Want`
7. Pod SM: emits `ScheduleRequest`; scheduler grants lease; pod creates placement edge to worker
8. Worker receives `LaunchPod` command; VM boots; worker reports `PodRunning`
9. Pod SM: status -> Running; workload sees pod status, emits `Readiness` signal
10. Router projects readiness to service via `WorkloadReadiness` edge
11. Service SM: NeedBackend -> Active; updates `EndpointInfo` signal
12. `EndpointAdapter` emits update; boundary sends `EndpointUpdate` to workers; fabric flushes buffered packets

### Idle Timeout

1. Protocol activator signals `BackendNeed::None` (TCP: no recent SYNs; H2: last stream closed)
2. Service SM: if Active with `has_activation`, starts idle timer via `WantedTimers` signal
3. `TimerAdapter` starts the timer
4. If `BackendNeed::Traffic` or `BackendNeed::Active` arrives before timeout -> cancel timer (signal update)
5. If timeout fires -> service transitions to Idle, sets `Demand(false)`
6. Router propagates demand change to workload
7. If this was the last service with demand (demand_count -> 0):
   - `suspend_on_idle == true`: workload sets `PodIntent::Suspend`, pod suspends (snapshot for fast resume)
   - `suspend_on_idle == false`: workload destroys pod, goes Dormant

### Active Flow Tracking

Workers report `EndpointFlowStatus { ip, has_active_flows }` when active TCP flows begin or end on a workload's endpoint. The `EndpointDemandAdapter` creates `BackendNeed` ports per (worker, service) pair. The service's `BackendNeedAggregator` sees both worker-reported need and flow-sourced need, taking the max — this keeps services alive while flows exist and prevents idle timeout during active traffic.

### Shared Workload Demand

Multiple services can share a single workload. The router handles demand aggregation via `DemandAggregator` — the workload receives a single `DemandInput` with the count of services that have `Demand(true)`. The workload creates `WorkloadReadiness` edges to **all** services in the demand input (not just those with active demand), so every service always knows whether its backing workload is ready. This enables instant Idle -> Active transitions when traffic returns to a service whose workload is already running.

### Activation Racing with Suspend

If traffic arrives while a workload is mid-suspend:
1. Service transitions to NeedBackend, sets `Demand(true)`
2. Workload receives increased demand count, but `awaiting_suspend` is true — pod is in flight
3. Pod's suspend completes (or times out), reaches terminal state
4. Workload sees terminal pod status, clears `awaiting_suspend`
5. Workload's `reconcile()` runs: demand > 0, no pod -> creates new pod (resumes from artifact if available)

The `committed_to_boot` flag ensures demand that arrived during suspend persists through the transition.

### Suspend Timeout Behavior

When a suspend exceeds its timeout:
1. Pod SM transitions to Failed (terminal) — the VM is killed, no snapshot produced
2. Workload sees Failed pod status, clears `awaiting_suspend`
3. If the pod was launched with a stale spec (`spec_version != launched_with_spec_version`): discard any artifact
4. The workload transitions to Dormant; next activation will cold-start
5. If demand > 0 or `committed_to_boot`, the workload immediately re-launches via cold start

The orchestrator does **not** retry the suspend. Suspend failures are typically caused by conditions that won't resolve on immediate retry.

### Pod Failure & Retry

#### Exponential Backoff

On pod failure (`Failed` terminal status from launch timeout, non-zero exit, or error), the workload retries with exponential backoff (`2^min(failures-1, 5)` seconds):

```
Failure 1: 1s delay
Failure 2: 2s delay
Failure 3: 4s delay
Failure 4: 8s delay
Failure 5: 16s delay -> Failed state
```

After 5 consecutive failures (MAX_RETRIES), the workload enters a terminal Failed status:
- Stops retrying automatically
- Services remain in `NeedBackend`
- `consecutive_failures` signal exposed for condition derivation
- `in_backoff` state exposed for retry-backoff condition

Pod displacement (worker disconnect) does **not** increment `consecutive_failures` — it's infrastructure failure, not application failure. The workload allows immediate rescheduling.

#### Auto-Recovery on Spec Update

When a workload is in Failed status and its spec changes (new image), the workload clears `consecutive_failures` and `in_backoff`, then retries. The intent: "I fixed the image, try again." This does not require a separate `dv restart`.

#### Interaction with Preemption

A workload in Failed status does not block capacity — it has no pod. The failed condition surfaces in `dv status` so the developer can fix the root cause.

---

## Spec Reconciliation

When `UpdateNamespace` delivers a new spec, the `ManagementAdapter` diffs against current state and reconciles by creating, updating, or removing SM instances and management ports:

| Change | Mechanism |
|---|---|
| New service added | Create ServiceSm + management port; spec signal delivered; service creates `ServiceDemand` edge to workload |
| Service removed | Destroy management port; service receives no-spec input, self-destructs; router removes all edges |
| New workload added | Create WorkloadSm + management port; spec signal delivered; workload waits for services to create demand edges |
| Workload removed | Destroy management port; workload destroys its pod; router cleanup handles the rest |
| Image changed | Management port updates spec signal; workload detects image change, increments `spec_version`; running pod destroyed, new pod launched |
| Service policy changed | Management port updates spec signal; service updates `EndpointInfo` and `DnsEntry` signals |
| `suspend_on_idle` changed | Spec signal update; workload updates flag, takes effect on next idle transition |
| `idle_timeout` changed | Spec signal update; service updates `idle_timeout`, resets timer if active |
| Network config changed | Complex — may require namespace recreation |

### Image Change -> Restart

Image change detection is local to the workload SM. When the spec signal delivers a new image:
1. `spec_version` increments
2. If a pod is Running: destroy it; `reconcile()` will create a new pod with the new spec
3. If a pod is mid-transition (Launching, Suspending): the pod continues; when it reaches a terminal state, the workload checks `spec_version != launched_with_spec_version` and discards stale artifacts / restarts as needed
4. If Dormant or Suspended with artifact: clear `artifact_port` (stale artifact from old image); next activation cold-starts

### Endpoint Sync & Registry Sync

Two adapter-driven broadcast systems deliver routing information to workers:

**Endpoint Sync** (`EndpointAdapter`) — service IP -> pod IP mappings for traffic routing. Services produce `EndpointInfo` signals; the adapter maintains a cache and emits incremental update/remove actions. The boundary layer translates these to `WorkerCommand::EndpointUpdate`. When a new worker joins, `build_sync()` provides the full endpoint state from cache.

**Registry Sync** (`DnsRegistryAdapter`) — DNS-like service name -> IP entries. Services and workloads produce `DnsEntry` signals; the adapter maintains a cache and emits incremental add/remove actions. The boundary layer translates to `WorkerCommand::RegistrySync`/`RegistryUpdate`. When a new worker joins, `build_sync()` provides the full registry.

Both systems use incremental aggregation — no full-state diffing on every change.

---

## Failure Handling

### Worker Disconnect

When a worker disconnects, the outer layer removes it from all namespaces. Within each namespace, the worker **port** is destroyed:

1. Router removes all edges to/from the worker port
2. Pods assigned to that worker re-aggregate `WorkerInput` to `None` — the worker signal disappears
3. Pod SM: transitions to `Displaced` (terminal state). Displacement is **not** counted as a failure — it's infrastructure, not application
4. Workload SM: sees pod status change to Displaced via `PodReport` edge; clears pod; if demand > 0, `reconcile()` creates a new pod (rescheduled to a different worker)
5. Service SM: workload clears `Readiness` signal -> service receives `None` readiness -> if activation-enabled, stays in current state (demand persists); if Active, transitions to NeedBackend
6. Scheduler: revokes all leases for that worker
7. Worker registry: re-broadcast to remaining workers

No manual `WorkerLost` fan-out is needed — the router handles it mechanically through port removal and re-aggregation.

### Worker Reconnection

If a worker connects with an ID that already exists (e.g., worker process restarted while orchestrator kept running), the orchestrator first processes a full disconnect for the old instance — removing the port, revoking leases, triggering re-aggregation — then treats it as a fresh connection. This ensures no stale state leaks across reconnections.

### Fabric Creation Failure

When a worker fails to create the namespace fabric (`NamespaceFailed { error }`), the boundary layer removes the worker from the pending set and logs the error. The worker port is never created in the signal graph, so no pods can be scheduled there. If no workers have active fabric, the namespace cannot serve traffic until another worker succeeds.

### Namespace Deletion

Namespace deletion is handled at the outer layer:

1. Outer layer sends `DestroyNamespace` to every worker assigned to the namespace
2. Removes the namespace from the namespace map
3. Cleans up timers and segment allocation
4. Broadcasts updated worker registry (segment removed)

### Storage & Eviction

#### Eviction Priority

When storage pressure exceeds thresholds, evict in order (easiest to hardest):

1. **SharedRO artifacts with no active consumers** — cache entries, freely evictable
2. **CopyOnUse templates with no local working copies** — re-fetchable
3. **SharedRO artifacts with active consumers** — only if re-fetchable
4. **Snapshots of suspended workloads (LRU)** — workload loses artifact reference, can still cold-start
5. **Exclusive artifacts for suspended pods** — same consequence as #4
6. **Exclusive artifacts for running pods** — never evict without stopping the pod

#### Artifact Lifecycle

Artifacts are tracked via `Artifact` ports in the signal graph and `ArtifactAdapter`:

1. Pod suspends successfully -> boundary creates `Artifact` port with artifact ID
2. Workload creates `WorkloadArtifactRef` edge to artifact port, signals `ArtifactRef(true)`
3. `ArtifactAdapter` emits `Referenced` -> scheduler tracks artifact placement
4. Scheduler confirms validity -> artifact port signals `Valid(true)` back to workload
5. Workload confirms artifact, clears confirmation timer

When an artifact is invalidated (worker disconnect, eviction):
- Scheduler broadcasts `ArtifactInvalidated`
- Boundary destroys the artifact port
- Workload receives `None` on `ArtifactInput`, clears `artifact_port`
- Next activation will cold-start instead of resume

#### Artifact Write Consistency

Artifact writes are tracked by the scheduler's placement table:
- `ArtifactWriteStarted` -> artifact tracked as Writing (not resumable)
- `ArtifactWriteCommitted` -> artifact tracked as Ready
- Never schedule a resume from a Writing artifact
- On worker disconnect, all Writing artifacts are purged

### Orchestrator Death

When the orchestrator dies, **all cluster state is lost**. Workers detect the disconnect and immediately tear down all resources. On restart, the orchestrator starts with a clean slate. Workers reconnect as fresh. Namespaces must be re-created by clients.

This is a deliberate simplicity choice. No state persistence, no WAL, no recovery protocol. This avoids a large class of consistency problems.

---

## Client Protocol

### Command/Event Model

The orchestrator exposes a control API over gRPC (tonic). The gRPC layer translates between protobuf messages and the SM's internal types.

**Commands** (Client -> Orchestrator): `CreateNamespace`, `UpdateNamespace`, `DeleteNamespace`, `GetNamespaceStatus`, `ListNamespaces`, `Splice`, `Unsplice`, `CloneNamespace`, `ListWorkers`, `GetWorker`, `ListPods`, `StreamLogs`, `Connect`, `Disconnect`, `DeactivateWorkload`, `DrainWorker`, `UndrainWorker`.

**Events** (Orchestrator -> Client): `NamespaceStatus`, `NamespaceList`, `WorkerList`, `WorkerStatus`, `PodList`, `LogChunk`, `Error`, `Ok`, `ConnectResult`, `DeactivateWorkloadResult`.

### gRPC Service Shape

**Unary RPCs**: Namespace CRUD, splice, clone, worker/pod queries, network connect/disconnect, drain/undrain.

**Streaming RPCs**: `StreamLogs` (workload log output), `StreamEvents` (namespace state machine events).

All unary RPCs use a pattern where the gRPC handler creates a temporary client connection, sends a command, and waits for the response via a oneshot channel. Streaming RPCs subscribe through the shell handle and receive events via unbounded channels.

### Imperative Commands

The policy is primarily spec-driven, but operators need escape hatches:

| Command | Effect | Persistent? |
|---|---|---|
| `dv restart <workload>` | Destroy pod, clear failure state, reset retries, re-launch if demand > 0 | No — one-shot |
| `dv stop <workload>` | Destroy pod, set `stopped` condition, suppress all demand | Yes — until `dv start` or spec update |
| `dv start <workload>` | Clear `stopped` condition, allow demand signals through | No — one-shot |
| `dv drain <worker>` | Set `draining` condition, exclude from scheduling | Yes — until `dv undrain` or reconnect |
| `dv undrain <worker>` | Clear `draining` condition | No — one-shot |

**Design principles**:
1. Imperative commands are delivered as events on management port edges (`AdminCommand::Restart`, `AdminCommand::Scavenge`)
2. Spec updates override manual stops (matches auto-recovery for failed)
3. All commands are reflected in `dv status` via derived conditions

---

## Splice & Clones (Planned)

### Splice

Splice allows injecting a local pod into a remote namespace, replacing a workload's backend with a locally-running instance. The primary developer experience feature — edit code locally, receive real traffic from the staging environment.

Splice operates at the **workload level**, not the service level. Moving the pod automatically updates all services sharing that workload (via the workload's `Readiness` signal projection).

**Planned flow**: User runs a local distvirt worker, sends `Splice { namespace_id, workload_id, local_worker_id }`. The outer layer adds the local worker to the namespace, creates the fabric segment, the workload stops the existing cloud pod and launches on the local worker. Other services see no change — same service IP, traffic routes through the tunnel.

**Requirements**: Multi-worker fabric tunneling (likely yamux stream between workers or orchestrator-mediated relay).

### Namespace Clones

Clone creates a new namespace from an existing one with an exact copy of the spec. The clone is an independent, isolated copy — namespaces are isolated at the network level. IPs, MACs, and network config are identical between source and clone since each has its own isolated fabric.

The clone preserves activation policies as-is: always-on services spin up immediately, activation-enabled services start idle. Once created, the clone's spec is decoupled from the source.

**Snapshot-accelerated clones** (future): Instead of cold-booting, restore from source pod snapshots. Firecracker restore is ~5-10ms vs ~100ms+ cold boot. Network config is identical, so no reconfiguration needed.

**Cost model**: Without snapshots, a clone is just metadata + service entities (essentially free). With snapshots, creation triggers one-time snapshot of source pods, then each activation is a fast restore.

---

## Testing Strategy

### Four Layers

1. **Stateright model checking** — exhaustive DFS exploration of reachable states
   - Workload SM: demand bounds, retry/failure reachability, spec version invariants, committed_to_boot consistency
   - Service SM: idle timer consistency, activation reachability, last_readiness correctness
   - Combined: multi-SM coordination, spec update restart, artifact lifecycle

2. **Proptest** — randomized input sequences with invariant checking
   - PodMap shadow consistency, namespace invariant fuzzing, orchestrator panic testing

3. **Scenario tests** — mock workers over real shell/protocol stack with paused time
   - ~84 scenarios across: activation, suspend/resume, failure recovery, fabric routing, preemption, pressure, registry, worker lifecycle, multi-service, snapshot placement, spec reconciliation, transition intents, drain

4. **Shell integration** — end-to-end lifecycle with yamux/capnp handshake

### Signal Invariants

The router supports declaring **invariants** on signals — boolean expressions that must hold at propagation quiescence. Transient violations during propagation are normal; violations at quiescence indicate domain logic bugs. Violations emit `TraceEvent::InvariantViolation` with full causality context.

### Debug Invariant Checking

In debug builds, invariants and consistency checks run after every orchestrator step. The router's depth limiting (configurable, crashes at depth N, warns at N-1) catches accidental cycles during development.

### Methodology

Write stateright model properties first, then implement SM changes to satisfy them. Proptest catches edge cases the model's action space doesn't cover. Scenario tests verify the full stack.

---

## Upcoming Work & Open Questions

### Known Issues

1. **No application readiness signal** — `PodRunning` means VM booted + guest agent connected, not that the application is ready. Traffic forwarded during app startup causes connection refused / 502 errors. This directly threatens the "feels like a slow first request" DX contract. Options: readiness probe (TCP/HTTP check, separate `PodReady` event), protocol activator implicit check, or defer to application.

2. **No worker heartbeat / liveness detection** — Worker loss is detected only by TCP drop. A hung worker (connected but not processing) is invisible. Fix: Use `PoolCapacityUpdate` (30s) as implicit heartbeat. If no message within 60s, set `unresponsive` condition; after further timeout, treat as disconnected.

3. **No pool degradation handling** — A worker's storage pool can become degraded while the worker remains connected. Suspend operations will fail, burning the timeout. Fix: Workers report pool health via conditions; orchestrator excludes degraded workers from suspend-target selection.

4. **Hardcoded per-pod resource sizing** — `available_memory_mb` is derived from `/proc/meminfo`, but per-pod sizes (`vcpus=1`, `memory=128MB`) are hardcoded. Resource leases track in-flight operations with these fictional sizes.

5. **Buffer overflow during slow cold starts** — The fabric buffers up to 64 frames with a 30s timeout. For workloads with slow startup, the buffer can fill. Consider making buffer size configurable per-service.

6. **Suspended artifact not cleared on worker disconnect** — When a workload is Suspended, its pod has already been reaped. The signal path Worker -> Pod -> Workload is severed. Worker disconnect does not reach the workload's artifact clearing logic. The workload stays Suspended instead of transitioning to Dormant.

### Unimplemented Features

- **Pressure-adjusted idle timeout** — Design defined (see [Worker Pressure Score](#worker-pressure-score)), not yet plumbed into the service SM. The service uses `idle_timeout` from spec directly.
- **Imperative commands** (`dv restart/stop/start`) — `AdminCommand::Restart` and `AdminCommand::Scavenge` are implemented as events on management port edges. `dv stop/start` not yet implemented.
- **Route-miss activation** — `EndpointActivation` without a `service_id` (direct IP access) does not yet resolve IP to service.
- **Splice / Unsplice** — handlers are no-ops
- **CloneNamespace** — returns error
- **WatchNamespaceStatus** — proto defined, not wired

### Post-V1 Open Questions

- **Affinity / anti-affinity** — co-location or spreading preferences for workloads
- **Resume on different worker** — cross-worker snapshot transfer when holding worker is pressured
- **Bin-packing vs spreading** — staging favors packing (fewer active workers); resilience favors spreading. Pressure scores naturally encourage spreading.
- **Cross-namespace preemption** — depends on isolation/tenancy model and namespace resource quotas
- **Always-on preemption** — what happens when a workload with no activation spec is preempted? Options: stay in `WaitingForCapacity`, or add activation retroactively.
- **Cross-worker failure retry** — try different worker on Nth retry to diagnose worker-specific vs workload-specific failures
- **Cross-worker snapshot migration before eviction** — transfer to a less-pressured worker before evicting entirely
- **Partial spec updates** — full spec vs diffs (full is simpler, no merge logic)
- **Graceful drain on suspend** — configurable drain period between backend removal and suspend
- **Worker provisioner interface** — emit `ProvisionWorker` output that shell maps to infrastructure API
- **Suspend failure retry** — single retry before fallback-to-stop
- **Per-pod resource sizing in spec** — once pods vary in size, preemption becomes a bin-packing problem
- **Namespace-level resource quotas** — bound per-namespace consumption

---

## Design Notes for Implementers

### Pod SM Reaping Rule

Pods self-destruct when **both** conditions are met: status is terminal (Suspended, Finished, Failed, Displaced) **and** `workload_id` is None (ownership edge removed). This gives the workload time to read the pod's terminal status — especially `Suspended { artifact_id }` — before the pod vanishes from the signal graph.

Two paths to pod death:
- **Natural**: Pod reaches terminal -> workload reads status -> workload removes ownership edge -> pod reaps
- **Abandon**: Workload removes edge while pod is live -> pod marks as Failed -> pod reaps

### `last_readiness` Cache in ServiceSm

The service caches the last readiness it received from its workload. This is necessary because the router uses `PartialEq` change detection — if the workload's readiness hasn't changed since the service was last Active, the router won't re-deliver it. When an Idle service activates (traffic arrives), it checks `last_readiness`: if cached, it skips NeedBackend and goes directly to Active. Without this, re-activation after idle timeout would stall in NeedBackend indefinitely.

### Timers

The pure state machine uses **semantic timer keys** — each timer is identified by its purpose (`IdleTimeout`, `LaunchTimeout`, `SuspendTimeout`, `RetryBackoff`, `ArtifactConfirm`). SMs emit `WantedTimers` signals declaring what timers they need; the `TimerAdapter` diffs against current state and emits start/cancel actions. Timer fires are delivered as events and treated as **hints, not commands** — each SM checks whether its current state still warrants the timer's action. This makes the system naturally tolerant of races between timer fires and state transitions.

Timer identity includes a generation counter — when a timer is cancelled and restarted, the generation increments. Stale fires (wrong generation) are silently ignored.

### Namespace Spec Frontends

Frontends run client-side (in the CLI) and translate their format into `NamespaceSpec`. The compose frontend maps each compose service to both a `WorkloadSpec` and a `ServiceSpec`. A future k8s-lite frontend would map Deployments and Services similarly.

### Developer Network Access (WireGuard)

Each namespace has a `WireGuardPeerManager` that allocates IPs from the top of the namespace's subnet for developer machine access. The connect flow: client sends public key -> namespace allocates IP -> `AddWireGuardPeer` sent to worker -> client configures local WireGuard interface with server key, endpoint, and assigned IP.
