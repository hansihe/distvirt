# Orchestrator Design

## Overview

The orchestrator is the central control plane for distvirt. It manages workers, drives namespace lifecycle, handles scale-to-zero activation, and exposes a client protocol (gRPC) for CLI/UI control. The primary use case is scale-to-zero staging environments where idle services consume no resources and activate transparently on first traffic.

The orchestrator is a **pure state machine** at its core. All logic lives in a synchronous `step(input) -> output` function with no I/O. An async shell dispatches inputs from network connections and timers, and sends outputs to workers and clients. This separation enables:

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
  worker       dormant, snapshot evicted     ← snapshot-lost condition
  migrations   failed: ImagePullError (5/5)  ← failed condition

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

### Two-Layer Design

The orchestrator has two layers:

1. **Outer layer** — routes inputs to the correct namespace state machine, manages worker connections, handles cross-namespace operations (clone, list), generates pod IDs, selects workers for pod scheduling, allocates network segment IDs, and maintains the inter-worker mesh registry.
2. **Namespace state machine** — a pure, self-contained state machine for a single namespace. All service lifecycle, activation, suspend/resume, and reconciliation logic lives here.

This separation keeps the per-namespace state machine small and independently testable. Cross-namespace interactions are minimal (limited to clones) and handled at the outer layer.

Most inputs are routed directly to a namespace. `WorkerDisconnected` fans out a `WorkerLost` event to every namespace that had pods on that worker. `CreateNamespace` instantiates a new state machine. `ListNamespaces` reads across all state machines.

The outer layer also handles pod scheduling: when a namespace emits a `PodRequest`, the outer layer selects a worker, generates a pod ID, and injects `LaunchPod` back into the namespace. Similarly for `ResumeRequest`.

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

### Per-Namespace State Machine — Three-Layer Split

The namespace state machine is split into three layers:

1. **NamespaceStateMachine** (thin coordinator): fabric management, namespace lifecycle, event routing between sub-state-machines, WireGuard peer management.
2. **WorkloadStateMachine**: pod lifecycle (scheduling, launching, running, suspend/resume). Driven by demand signals from services.
3. **ServiceStateMachine**: activation, idle timeout, backend routing. Driven by activation events and workload readiness.

Multiple services can share a single workload. The coordinator maintains the mapping and forwards signals between them.

This split reduces the model-checking state space from O(states^N) (monolithic, all services interleaved) to O(states × N) (each sub-SM checked independently). WorkloadStateMachine has ~7 states; ServiceStateMachine has ~4 × 3 `BackendNeed` values. Both are small enough for exhaustive stateright exploration.

### Async Shell

The async runtime is a thin shell that:
- Manages tokio timers (spawns/aborts for each timer key from SM output)
- Routes worker protocol events → SM inputs
- Routes SM outputs → worker commands (via worker protocol writers)
- Manages client request/response matching via oneshot channels
- Buffers and distributes log streams to subscribers
- Distributes SM events to gRPC streaming subscribers

### Two-Layer Worker State

Worker state is split between two levels:

**`WorkerState`** (global, in `Orchestrator.workers`) — cluster-scoped:
- Capabilities (`available_memory_mb`, pools, public endpoint)
- `wg_config` — WireGuard config (listen port, public key) for developer network access
- `tunnel_config` — inter-worker mesh tunnel config (listen port, public key)
- `transfer_listen_port` — port for artifact transfers between workers
- Worker conditions (from protocol events)
- `pressure: WorkerPressure` — raw normalized scores per dimension
- `pressure_bands: PressureBands` — hysteresis state per dimension
- `psi: Option<WorkerPsi>` — cached PSI metrics from last `PressureUpdate`

**`NamespaceWorkerState`** (namespace-scoped, in `NamespaceStateMachine.workers`):
- `fabric_status: FabricStatus` — Creating/Active/Destroying for this namespace
- `primary_pool_id: Option<PoolId>` — resolved at assignment time for suspend operations
- `pressure_band: PressureBand` — propagated from global `WorkerState.pressure_bands.max_band()`

Propagation is one-directional (global → namespace) via `propagate_pressure_to_namespaces()` after each pressure recomputation. The namespace SM never writes back to global state.

**Scheduling reads global state** (`select_worker_for_pod` reads `Orchestrator.workers`). **Idle timeout reads namespace state** (`pressure_adjusted_idle_timeout` reads `NamespaceWorkerState.pressure_band`).

---

## Core Abstractions

Four cross-cutting abstractions shape the design:

### Worker Pressure Score

**Problem**: N independent signals (PSI at multiple averages, pool watermarks, pod count, memory committed) each wired to M policy decisions creates an N×M matrix of threshold tuning with inconsistent behavior.

**Design**: A normalized `WorkerPressure` per resource dimension (compute, memory, storage, network), each 0.0–1.0. Each dimension is the **max** of its available inputs, normalized. On non-Linux workers (libkrun/macOS), PSI inputs are absent — the score falls back to static accounting. Same thresholds, same policy code, no special-casing.

Input mapping:
- `compute`: PSI cpu some_avg10 / 100 (no static fallback — 0.0 without PSI)
- `memory`: max(PSI memory some_avg10 / 100, pods_memory_committed / available_memory_mb)
- `storage`: max(PSI io some_avg10 / 100, pool_used / pool_capacity)
- `network`: fabric tunnel utilization (future extension, initially 0.0)

#### Hysteresis

Pressure band thresholds use hysteresis to prevent oscillation at boundaries:

| Band | Enter At | Leave At |
|---|---|---|
| Normal | — | — |
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
| compute | Shorten idle timeout | Preempt priority 3–4 | Preempt priority 2–4 |
| memory | Shorten idle timeout | Preempt priority 3–4 | Preempt priority 2–4 |
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

Pressure scores are recomputed on: periodic `PoolCapacityUpdate` events (30s), `PressureUpdate` events (10s periodic + immediate on threshold crossings), and pod start/stop. The pressure score is a derived value in the orchestrator's `WorkerState`.

---

### Transition Intents

**Problem**: A state transition is in flight (async) when a contradicting signal arrives, and the system has no place to record the signal until the transition completes. Examples: traffic arrives while mid-suspend, `ForceDeactivate` during pod launch, spec change during resume.

**Design**: Every in-flight transition state carries a **pending intent slot** — a priority-ordered enum:

```
PendingIntent: None < Demand < Deactivate < Restart
```

When a transition completes, the state machine checks the pending intent before choosing the next state:

- `Suspending` completes + `Demand` → emit `ResumeRequest` immediately (skip `Suspended` state)
- `Suspending` completes + `Deactivate` → enter `Suspended`/`Dormant` as normal
- `Suspending` completes + `Restart` → go Dormant, relaunch with new spec
- `Launching` completes + `Deactivate` → immediately begin deactivation
- `Launching` completes + `Restart` → stop pod, relaunch with new spec
- `Resuming` completes + `Deactivate` → immediately begin deactivation
- `Resuming` completes + `Restart` → stop pod, relaunch with new spec

**Conflict resolution**: Later signals upgrade the intent — a new signal only replaces the current intent if it has higher priority.

The principle: **never discard a signal because the system is busy — record it as an intent and resolve it when the transition completes.**

---

### Condition Model

**Problem**: Policy-relevant state is scattered across enum variants, log messages, and implicit behavior. Status display and event streaming need custom visibility logic for each failure mode.

**Design**: A uniform `conditions: HashMap<String, String>` on all entity types (workers, workloads, services). Each condition has a key and message.

**Worker conditions**: `storage/pool/<id>/pressure-soft`, `storage/pool/<id>/pressure-hard`, `pressure/compute`, `pressure/memory`, `draining`, `pool/<id>/degraded`, `unresponsive`.

**Workload conditions**: `failed` (exceeded max retries), `retry-backoff` (during backoff delay), `preempted` (evicted for higher-priority workload), `snapshot-lost` (snapshot evicted, will cold-start).

**Service conditions**: `activation-pending` (traffic buffered, waiting for backend), `backend-not-ready` (pod running but app not ready — future, needs readiness probes).

**Observability**: `dv status` = snapshot all active conditions per entity. `dv events` = stream of condition transitions. Alerting = condition active longer than threshold. The condition model subsumes most status visibility in one mechanism.

---

### Resource Leases

**Problem**: Resources (pod slots, memory, artifact entries) are claimed during async operations that can fail. On failure, resources are "leaked." Concurrent scheduling can race past capacity limits.

**Design**: A `LeaseTable` keyed by `PodId` with automatic expiry. Leases carry `worker_id`, `LeaseIntent` (PodLaunch or PodResume), and `memory_mb`.

**Lifecycle**:
1. **Grant**: When the orchestrator dispatches `LaunchPod`/`ResumePod`, it grants a lease. Available capacity is immediately decremented.
2. **Commit**: When `PodRunning` arrives, the lease converts to actual occupancy.
3. **Expire**: On timeout or worker disconnect, the capacity reservation is automatically released.
4. **Release**: On pod stop/fail/suspend, the lease is released.

Leases prevent overcommit when multiple pods are being scheduled simultaneously. They also unify artifact write consistency — `ArtifactWriteStarted` grants a lease; `ArtifactWriteCommitted` commits it; worker disconnect expires all writing leases.

---

## Scheduling & Capacity

### Worker Selection

When a workload needs a pod (`PodRequest`), the orchestrator selects a worker via two-phase selection:

1. **Hard constraints** (filter):
   - `fabric_status == Active` for the namespace
   - Not draining
   - Pressure score below High threshold on all dimensions

2. **Soft preferences** (rank):
   - Lowest pressure band, then fewest pods
   - Snapshot locality (prefer worker holding the snapshot for resume)

3. **Reserve capacity** (lease):
   - Grant a pod slot lease on the selected worker
   - Lease deadline = launch timeout (60s)

When no worker has capacity, the workload stays in `WaitingForCapacity` and preemption is considered.

### Preemption

Preemption is namespace-scoped. Priority is derived from runtime state, not spec-declared:

| Priority | Description | Preemptable? |
|---|---|---|
| 1 (highest) | **Activated** — traffic just arrived, client is blocked waiting | Never |
| 2 | **Active with traffic** — BackendNeed::Active/Traffic, sessions in progress | Never |
| 3 | **Active but idle** — running, BackendNeed::None, idle timer ticking | Yes |
| 4 | **Always-on, no traffic** — running by policy, no current demand signal | Yes |
| 5 (lowest) | **Suspended** — consuming storage only | Evict snapshot if storage-pressured |

**Preemption flow**: When `select_worker_for_pod` finds no worker with capacity, the orchestrator scans same-namespace workloads for running preemptable candidates (priority 3–4), selects a victim, and dispatches `ForceDeactivate`. The victim follows the normal deactivation path (suspend or stop). The waiting workload is naturally retried by `schedule_waiting_pods()` once the victim's slot frees.

One preemption per scheduling attempt (avoids cascading evictions). Under elevated pressure (score > 0.8), preemption can be triggered proactively even when pod count hasn't hit a hard limit.

### Worker Drain

Drain uses the existing condition model — `DrainWorker` sets a `"draining"` condition on `WorkerState.conditions`. Scheduling excludes draining workers. Existing pods deactivate on their normal idle timeout. `UndrainWorker` clears the condition and triggers `schedule_waiting_pods` to pick up waiting workloads.

---

## Lifecycle & Activation

### Service Activation Flow

1. Traffic arrives at a service IP on the fabric
2. Protocol activator (TCP/H2/Postgres) inspects the packet — meaningful traffic (TCP SYN, H2 HEADERS) → `ServiceActivation` event
3. Service SM: Idle → NeedBackend, sets `activation-pending` condition
4. Reconciliation computes effective demand from service states, sends `SetDemand` to workload
5. Workload SM: Dormant → WaitingForCapacity → Launching → Running
6. Reconciliation sees workload Running + service NeedBackend → sends `WorkloadReady` to service
7. Service SM: NeedBackend → Active, `UpdateServiceBackend` + `ServiceReady` sent to workers
8. Service clears `activation-pending` condition, fabric flushes buffered packets

### Idle Timeout

1. Protocol activator signals `BackendNeed::None` (TCP: no recent SYNs; H2: last stream closed)
2. Service SM starts idle timer (configurable per-service, default 30s)
3. Effective timeout adjusted by pressure band
4. If `BackendNeed::Traffic` or `BackendNeed::Active` arrives before timeout → cancel timer
5. If timeout fires → `UpdateServiceBackend(None)`, service returns to Idle, emits `DemandDown`
6. If this was the last service demanding the workload (current_demand → 0):
   - `suspend_on_idle == true`: workload suspends (snapshot for fast resume)
   - `suspend_on_idle == false`: workload stops, goes Dormant

### Active Flow Tracking

Workers report `EndpointFlowStatus { ip, has_active_flows }` when active TCP flows begin or end on a workload's endpoint. The workload SM tracks `has_active_flows: bool`, which feeds into demand calculation: `effective_demand = service_demand + has_active_flows`. This ensures a workload stays Running as long as traffic is flowing, even if no service explicitly `wants_backend()` (e.g., during idle timer transitions).

### Shared Workload Demand

Multiple services can back the same workload. Demand is derived by reconciliation — the workload runs as long as any service `wants_backend()` or `has_active_flows` is true. When a workload is already Running and a second service enters NeedBackend, the coordinator immediately forwards `WorkloadReady` and the service transitions straight to Active.

### Activation Racing with Suspend

If traffic arrives while a workload is mid-suspend, the service transitions to NeedBackend. Reconciliation computes increased demand and sends `SetDemand` to the workload, which upgrades the pending intent to `PendingIntent::Demand`. When `PodSuspended` arrives, the state machine sees the pending demand and immediately emits `ResumeRequest` — skipping the `Suspended` state entirely.

### Suspend Timeout Behavior

When a suspend exceeds its timeout:
1. The orchestrator emits `StopPod` — the VM is killed, no snapshot produced
2. If the workload had a prior snapshot that diverged (resumed, ran, re-suspend failed): set `snapshot-lost` condition
3. The workload transitions to Dormant; next activation will cold-start
4. If `pending == PendingIntent::Demand`, the workload immediately re-launches via cold start

The orchestrator does **not** retry the suspend. Suspend failures are typically caused by conditions that won't resolve on immediate retry.

### Pod Failure & Retry

#### Exponential Backoff

On pod start failure (`PodFailed`/`PodExited` during launch, or launch timeout), the workload retries with exponential backoff (`2^min(failures-1, 5)` seconds):

```
Failure 1: 1s delay
Failure 2: 2s delay
Failure 3: 4s delay
Failure 4: 8s delay
Failure 5: 16s delay → Failed state
```

After 5 consecutive failures (MAX_RETRIES), the workload transitions to a `Failed` terminal state:
- Stops retrying automatically
- Services remain in `NeedBackend`
- `failed` condition set with last error and attempt count
- `retry-backoff` condition active during backoff delays

#### Auto-Recovery on Spec Update

When a workload is in `Failed` state and its spec changes (new image, changed env), the orchestrator clears the `failed` condition and retries. The intent: "I fixed the image, try again." This does not require a separate `dv restart`.

#### Interaction with Preemption

A workload in `Failed` state does not block capacity — it's not consuming a pod slot. The `failed` condition surfaces in `dv status` so the developer can fix the root cause.

---

## Spec Reconciliation

When `UpdateNamespace` delivers a new spec, the orchestrator diffs against current state and reconciles:

| Change | Impact |
|---|---|
| New service added | Create service entity on workers, start in Idle or NeedBackend |
| Service removed | Destroy service entity, emit DemandDown if active |
| New workload added | Add to workload map, wait for services to demand it |
| Workload removed | Stop pod if running, clean up services pointing to it |
| Image changed | Restart the workload (stop old pod, launch new with new image) |
| Service policy changed | Update service entity on workers |
| `suspend_on_idle` changed | Update workload flag, takes effect on next idle transition |
| `idle_timeout` changed | Update service timeout, reset timer if active |
| Network config changed | Complex — may require namespace recreation |

### Image Change → Restart

On image change detection during reconciliation, the orchestrator stops the current pod and re-launches with the new image. For staging, a hard cut (stop then start) is sufficient. If a workload is mid-transition, the `PendingIntent::Restart` intent ensures the new image is applied when the transition completes.

### Endpoint Sync & Registry Sync

The namespace maintains two separate broadcast systems for workers:

**Endpoint Sync** — workload/service IP→backend mappings for traffic routing. Built from all pods, services, and WireGuard peers. Sent as `WorkerCommand::EndpointSync` (full sync) or `WorkerCommand::EndpointUpdate` (incremental upsert). Full sync is sent when a worker's fabric becomes Active. Incremental updates are sent on pod state changes and service backend changes.

**Registry Sync** — DNS-like service name→IP entries (`RegistryEntry { name, ip }`). Sent as `WorkerCommand::RegistrySync` to all active workers. Emitted on namespace becoming Active, service configuration changes, and worker loss. Naturally idempotent.

Both systems have per-worker variants (`emit_endpoint_sync_to_worker`, `emit_registry_sync_to_worker`) used when a single worker joins an existing namespace.

---

## Failure Handling

### Worker Disconnect

When a worker disconnects, the outer layer fans out `WorkerLost` to every namespace that had the worker:

1. **WorkloadStateMachine** handles based on current state:
   - `Running` → emits `BecameUnready`, transitions to `WaitingForCapacity` (if demand > 0) or `Dormant`
   - `Suspended` → snapshot is lost with the worker, falls back to cold boot
   - `Launching`/`Suspending`/`Resuming` → cancels timeouts, transitions based on demand
2. **ServiceStateMachine** receives `WorkloadUnready`:
   - If activation-enabled → transition to `Idle`, emit `DemandDown`
   - If always-on → stay in `NeedBackend`
3. Coordinator removes the worker from its `workers` map, cancels associated timers
4. All leases for that worker are released
5. Worker registry is re-broadcast to remaining workers

### Worker Reconnection

If a worker connects with an ID that already exists (e.g., worker process restarted while orchestrator kept running), the orchestrator first processes a full disconnect for the old instance — releasing leases, fanning out `WorkerLost`, cleaning up state — then treats it as a fresh connection. This ensures no stale state leaks across reconnections.

### Fabric Creation Failure

When a worker fails to create the namespace fabric (`NamespaceFailed { error }`), the namespace handles this at the coordinator level. The worker's `NamespaceWorkerState` is cleaned up. If no workers have active fabric, the namespace may be unable to serve traffic until another worker succeeds.

### Namespace Deletion

Namespace deletion is a **stateful teardown**:

1. Namespace transitions to `Destroying`: cancels all timers, stops accepting new inputs, emits `DestroyNamespace` to every worker
2. As each worker confirms destruction (or disconnects), the namespace removes it
3. When `self.workers` is empty, the namespace sets `destroyed: true`
4. The outer layer removes the namespace

While in `Destroying`, the namespace only processes worker events and worker loss.

### Storage & Eviction

#### Eviction Priority

When storage pressure exceeds thresholds, evict in order (easiest to hardest):

1. **SharedRO artifacts with no active consumers** — cache entries, freely evictable
2. **CopyOnUse templates with no local working copies** — re-fetchable
3. **SharedRO artifacts with active consumers** — only if re-fetchable
4. **Snapshots of suspended workloads (LRU)** — workload sets `snapshot-lost` condition, can still cold-start
5. **Exclusive artifacts for suspended pods** — same consequence as #4
6. **Exclusive artifacts for running pods** — never evict without stopping the pod

#### Snapshot-Lost Degradation

When a snapshot is evicted (sole copy lost):
- The workload spec is preserved — cold start is still possible on next activation
- `snapshot-lost` condition set: "storage pressure evicted snapshot, will cold-start on next activation"
- Condition cleared on next successful suspend

#### Artifact Write Consistency

Artifact writes use the lease model:
- `ArtifactWriteStarted` → lease granted (status: Writing, not readable)
- `ArtifactWriteCommitted` → lease committed (status: Ready)
- Never schedule a resume from a Writing artifact
- On worker disconnect, all Writing leases expire

### Orchestrator Death

When the orchestrator dies, **all cluster state is lost**. Workers detect the disconnect and immediately tear down all resources. On restart, the orchestrator starts with a clean slate. Workers reconnect as fresh. Namespaces must be re-created by clients.

This is a deliberate simplicity choice. No state persistence, no WAL, no recovery protocol. This avoids a large class of consistency problems.

---

## Client Protocol

### Command/Event Model

The orchestrator exposes a control API over gRPC (tonic). The gRPC layer translates between protobuf messages and the SM's internal types.

**Commands** (Client → Orchestrator): `CreateNamespace`, `UpdateNamespace`, `DeleteNamespace`, `GetNamespaceStatus`, `ListNamespaces`, `Splice`, `Unsplice`, `CloneNamespace`, `ListWorkers`, `GetWorker`, `ListPods`, `StreamLogs`, `Connect`, `Disconnect`, `DeactivateWorkload`, `DrainWorker`, `UndrainWorker`.

**Events** (Orchestrator → Client): `NamespaceStatus`, `NamespaceList`, `WorkerList`, `WorkerStatus`, `PodList`, `LogChunk`, `Error`, `Ok`, `ConnectResult`, `DeactivateWorkloadResult`.

### gRPC Service Shape

**Unary RPCs**: Namespace CRUD, splice, clone, worker/pod queries, network connect/disconnect, drain/undrain.

**Streaming RPCs**: `StreamLogs` (workload log output), `StreamEvents` (namespace state machine events).

All unary RPCs use a pattern where the gRPC handler creates a temporary client connection, sends a command, and waits for the response via a oneshot channel. Streaming RPCs subscribe through the shell handle and receive events via unbounded channels.

### Imperative Commands

The policy is primarily spec-driven, but operators need escape hatches:

| Command | Effect | Persistent? |
|---|---|---|
| `dv restart <workload>` | Stop pod, clear `failed` condition, reset retries, re-launch if demand > 0 | No — one-shot |
| `dv stop <workload>` | Stop pod, set `stopped` condition, suppress all demand | Yes — until `dv start` or spec update |
| `dv start <workload>` | Clear `stopped` condition, allow demand signals through | No — one-shot |
| `dv drain <worker>` | Set `draining` condition, exclude from scheduling | Yes — until `dv undrain` or reconnect |
| `dv undrain <worker>` | Clear `draining` condition | No — one-shot |

**Design principles**:
1. Imperative commands set conditions/intents, not new state machine states
2. Spec updates override manual stops (matches auto-recovery for `failed`)
3. All commands are reflected in `dv status` and `dv events` via the condition model

---

## Splice & Clones (Planned)

### Splice

Splice allows injecting a local pod into a remote namespace, replacing a workload's backend with a locally-running instance. The primary developer experience feature — edit code locally, receive real traffic from the staging environment.

Splice operates at the **workload level**, not the service level. Moving the pod automatically updates all services sharing that workload.

**Planned flow**: User runs a local distvirt worker, sends `Splice { namespace_id, workload_id, local_worker_id }`. The coordinator adds the local worker to the namespace, creates the fabric segment, stops the existing cloud pod, launches on the local worker. Other services see no change — same service IP, traffic routes through the tunnel.

**Requirements**: Multi-worker fabric tunneling (likely yamux stream between workers or orchestrator-mediated relay).

### Namespace Clones

Clone creates a new namespace from an existing one with an exact copy of the spec. The clone is an independent, isolated copy — namespaces are isolated at the network level. IPs, MACs, and network config are identical between source and clone since each has its own isolated fabric.

The clone preserves activation policies as-is: always-on services spin up immediately, activation-enabled services start idle. Once created, the clone's spec is decoupled from the source.

**Clone + destroy interaction**: If `Delete` arrives during `Cloning`, `pending_destroy: true` is set instead of immediately destroying. When the clone completes, the outer layer checks and transitions to `Destroying`. This avoids reading partially-destroyed state.

**Snapshot-accelerated clones** (future): Instead of cold-booting, restore from source pod snapshots. Firecracker restore is ~5-10ms vs ~100ms+ cold boot. Network config is identical, so no reconfiguration needed.

**Cost model**: Without snapshots, a clone is just metadata + service entities (essentially free). With snapshots, creation triggers one-time snapshot of source pods, then each activation is a fast restore.

---

## Testing Strategy

### Four Layers

1. **Stateright model checking** — exhaustive DFS exploration of reachable states
   - Workload SM: 15 scenarios (timer consistency, demand bounds, pending intent invariants, retry/failure reachability, ForceDeactivate)
   - Service SM: 4 scenarios (idle timer consistency, activation reachability)
   - Namespace SM: 11 scenarios (referential integrity, multi-SM coordination, spec update restart, worker loss)

2. **Proptest** — randomized input sequences with invariant checking
   - PodMap shadow consistency, namespace invariant fuzzing, orchestrator panic testing

3. **Scenario tests** — mock workers over real shell/protocol stack with paused time
   - ~60 scenarios across: activation, suspend/resume, failure recovery, fabric routing, preemption, pressure, registry, worker lifecycle, multi-service, snapshot placement, spec reconciliation, transition intents, drain

4. **Shell integration** — end-to-end lifecycle with yamux/capnp handshake

### Debug Invariant Checking

In debug builds, `check_invariants()` runs after every orchestrator step. It verifies:
- Bidirectional consistency: if a namespace lists a worker, that worker must list the namespace (and vice versa)
- All leases reference existing workers
- No stale references across the two-layer worker state

This catches state corruption early during development and testing.

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

### Unimplemented Features

- **Imperative commands** (`dv restart/stop/start`) — design defined (see [Client Protocol](#imperative-commands)), not yet implemented. `DeactivateWorkload` and `DrainWorker`/`UndrainWorker` are implemented.
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

### `current_demand`, `needs_successful_boot`, and `PendingIntent`

Three mechanisms work together for demand management:

- **`current_demand`**: set authoritatively by reconciliation via `SetDemand { count }`. Computed as `effective_demand = service_demand + has_active_flows` where `service_demand` is the count of mapped services with `wants_backend() == true`, and `has_active_flows` contributes 1 if active TCP flows exist on the workload's endpoint. No incremental DemandUp/DemandDown — demand is always consistent with actual service and flow states.
- **`needs_successful_boot`**: once demand goes 0→non-zero, the workload is committed to reaching Running before it can go Dormant via SetDemand(0). Also set on WorkerLost and PodGone. Cleared on PodRunning→Running or entering Failed. Prevents demand fluctuations during boot/retry from prematurely stopping the workload.
- **`PendingIntent`**: captures contradicting signals during transitions. Consumed once when the transition completes.

### The `Pending` Service State

`ServiceState::Pending` serves dual duty: initial state (pre-reconciliation) and temporary placeholder during `mem::replace`. The reconciliation logic only matches on `(Pending, Dormant)` — it should also handle `(Pending, Suspended)` for correctness after worker loss.

### Suspend Timeout and `snapshot-lost`

The `Suspending` state carries `artifact_id` — but this is the *new* artifact being written, not a prior one. The prior artifact was deleted on successful resume. So suspend timeout after resume never has a prior snapshot to lose. In practice, `snapshot-lost` matters for the eviction case (storage pressure deletes a Suspended workload's artifact), not the suspend-timeout case.

### Timers

The pure state machine uses **semantic timer keys** — each timer is identified by its purpose (`IdleTimeout`, `LaunchTimeout`, `SuspendTimeout`, `ResumeTimeout`, `RetryBackoffTimeout`). Timer fires are treated as **hints, not commands** — each sub-SM checks whether its current state still warrants the timer's action. This makes the system naturally tolerant of races between timer fires and state transitions.

### Namespace Spec Frontends

Frontends run client-side (in the CLI) and translate their format into `NamespaceSpec`. The compose frontend maps each compose service to both a `WorkloadSpec` and a `ServiceSpec`. A future k8s-lite frontend would map Deployments and Services similarly.

### Developer Network Access (WireGuard)

Each namespace has a `WireGuardPeerManager` that allocates IPs from the top of the namespace's subnet for developer machine access. The connect flow: client sends public key → namespace allocates IP → `AddWireGuardPeer` sent to worker → client configures local WireGuard interface with server key, endpoint, and assigned IP.
