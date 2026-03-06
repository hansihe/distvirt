# Orchestrator Policy

## Overview

The orchestrator makes all scheduling, activation, eviction, and retry decisions. Workers are executors — they report state and execute commands. This document describes the full set of policy concerns, inputs, and desired behavior, with a focus on developer experience for the primary use case: scale-to-zero staging environments.

Four cross-cutting abstractions shape the policy design:

1. **Worker Pressure Score** — a normalized per-dimension score (compute, memory, storage, network) that collapses many signals into one value all policy decisions consult.
2. **Transition Intents** — a pending intent enum on in-flight state transitions, ensuring signals are never lost during async operations.
3. **Condition Model** — a uniform key/active/message triple applied to workers, workloads, and services, providing the single observability layer for `dv status` and `dv events`.
4. **Resource Leases** — time-bounded claims on capacity (pod slots, memory, artifact entries) that auto-expire on holder disconnect or timeout.

These are described in detail in [Core Abstractions](#core-abstractions) and referenced throughout the policy sections.

---

## Inputs to Policy Decisions

### From Workers

| Input | Source |
|---|---|
| Worker capabilities (available_memory_mb) | WorkerHello handshake |
| Pool info (pool_id, capacity, available) | WorkerHello + periodic PoolCapacityUpdate |
| Pool watermark events (soft 85%, hard 95%) | WorkerCondition events |
| PSI pressure (cpu, memory, io) | `WorkerEvent::PressureUpdate` (10s periodic) |
| Pod count per worker | PodMap tracking |
| ServiceActivation (first traffic to idle service) | Fabric → worker → orchestrator |
| ServiceBackendNeed (None/Traffic/Active) | Protocol activator → worker → orchestrator |
| PodRunning / PodExited / PodFailed | Worker events |
| PodSuspended / PodSuspendFailed | Worker events |
| Heartbeat / liveness | **Not implemented** — needed for worker hang detection (see [Known Issues](#known-issues)) |

### From Spec (User-Declared)

| Input | Source |
|---|---|
| `suspend_on_idle` (per-workload) | WorkloadSpec |
| `activation` + `idle_timeout` (per-service) | ServiceSpec.ActivationSpec |
| Protocol activator type (TCP/H2/Postgres) | ServiceSpec.ServicePolicy |
| Container image, env, entrypoint | WorkloadSpec.ContainerSpec |
| Network config (subnet, IPs) | NamespaceSpec.NetworkConfig |
| Resource requests (cpu, memory) | **Not in spec** |
| Priority / preemption class | **Not in spec** |
| Namespace resource quota (max pods, max memory) | **Not in spec** |

### From Internal State

| Input | Source |
|---|---|
| Workload state (Dormant/Running/Suspended/...) | WorkloadStateMachine |
| Service state (Idle/NeedBackend/Active) | ServiceStateMachine |
| current_demand per workload (derived from service states by reconciliation) | WorkloadStateMachine |
| BackendNeed per active service | ServiceState::Active |
| Snapshot placement (artifact → pool/worker) | PlacementTable |
| Consecutive failure count per workload | WorkloadStateMachine (`consecutive_failures`) |
| Worker pressure scores (compute, memory, storage) | `WorkerState` — recomputed from PSI + static accounting |

---

## Core Abstractions

### Worker Pressure Score

#### Problem

The policy has N independent signals (PSI cpu/memory/io at multiple averages, pool watermarks at soft/hard thresholds, pod count, memory committed vs available) each independently wired to M policy decisions (scheduling, preemption, adaptive idle timeout, eviction, alerting). This creates an N×M matrix of threshold tuning with inconsistent behavior.

#### Design

Compute a normalized `WorkerPressure` per resource dimension, each 0.0–1.0, from whatever signals are available:

```rust
struct WorkerPressure {
    compute: f32,  // 0.0 = idle, 1.0 = fully stalled
    memory: f32,
    storage: f32,
    network: f32,
}
```

Each dimension is the **max** of its available inputs, normalized:

```
memory_pressure = max(
    psi_memory_some_avg10 / 100.0,               // PSI (if available)
    pods_memory_committed / worker_available_mb,   // static accounting fallback
)

storage_pressure = max(
    psi_io_some_avg10 / 100.0,                    // PSI IO (if available)
    pool_used / pool_capacity,                     // pool utilization
)

compute_pressure = max(
    psi_cpu_some_avg10 / 100.0,                   // PSI (if available)
    // No static fallback — without PSI, compute pressure is unknown (0.0)
    // PSI is the primary input; static accounting for compute is unreliable
)

network_pressure = max(
    fabric_tunnel_tx_utilization,                  // tunnel bandwidth used / capacity (if measurable)
    // No PSI equivalent — derived from fabric-level metrics
)
```

The `network` dimension is a future extension — initially absent (0.0). When fabric tunnel metrics are available, network pressure can feed into scheduling (avoid saturated workers) and eviction (prefer migrating snapshots away from network-bottlenecked workers). The same pressure bands apply.

On non-Linux workers (libkrun/macOS), PSI inputs are absent — the score falls back to static accounting. Same thresholds, same policy code, no special-casing.

#### Hysteresis

Pressure band thresholds use hysteresis to prevent oscillation at boundaries. A worker enters a band at the upper threshold and leaves it at a lower one:

| Band | Enter At | Leave At |
|---|---|---|
| Normal | — | — |
| Elevated | 0.50 | 0.40 |
| High | 0.80 | 0.70 |
| Critical | 0.95 | 0.85 |

Without hysteresis, a worker fluctuating at 0.79–0.81 would rapidly flip between "deprioritize" and "exclude" in scheduling decisions. The gap between enter/leave thresholds should be wide enough to absorb normal measurement noise (10% of the band width).

The hysteresis state is per-dimension — each dimension tracks its current band independently. The effective band for policy decisions is the **maximum** band across all dimensions (same as the pressure score itself).

#### Policy Bands

#### Scheduling (cross-dimension)

The maximum band across all dimensions determines scheduling eligibility:

| Band | Scheduling Effect |
|---|---|
| Normal | Full priority |
| Elevated | Deprioritize |
| High | Exclude |
| Critical | Exclude |

#### Dimension-Specific Responses

Each pressure dimension triggers different policy responses at each band:

| Dimension | Elevated | High | Critical |
|---|---|---|---|
| compute | Shorten idle timeout | Preempt priority 3–4 | Preempt priority 2–4 |
| memory | Shorten idle timeout | Preempt priority 3–4 | Preempt priority 2–4 |
| storage | Proactive snapshot migration | Aggressive eviction | Emergency eviction |
| network | (future) | (future) | (future) |

Scheduling exclusion uses the max band across all dimensions — if any dimension is High, the worker is excluded. But the *response* to pressure is dimension-specific: storage pressure triggers eviction (free disk), not preemption (free compute). Compute/memory pressure triggers preemption (free pods), not eviction.

#### Idle Timeout Under Pressure

Idle timeout adjusts in steps aligned with pressure bands, applied only when compute or memory pressure is elevated:

| Pressure Band | Idle Timeout |
|---|---|
| Normal | Configured timeout |
| Elevated | 75% of configured |
| High | 25% of configured (minimum 5s floor) |
| Critical | Immediate (5s floor) |

The 5s floor prevents pathological thrashing where a workload activates, immediately idles, activates again. This is enough time for a request to complete, short enough to free resources promptly.

#### Update Cadence

Pressure scores are recomputed on:
- Periodic `PoolCapacityUpdate` events (every 30s currently)
- `PressureUpdate` events (when implemented — 10s periodic + immediate on threshold crossings)
- Pod start/stop (pod count changes)

The pressure score is a derived value, not a new wire protocol message. It lives in the orchestrator's `WorkerState`.

---

### Transition Intents

#### Problem

Several race conditions share the same structural cause: a state transition is in flight (async, takes time) when a contradicting signal arrives, and the system has no place to record the signal until the transition completes. Examples:

- Traffic arrives while a workload is mid-suspend → pays full suspend + resume round-trip
- `ForceDeactivate` arrives while a pod is launching → must wait for launch to complete
- Worker drain requested while pods are running → no mechanism to prevent new scheduling while existing pods wind down
- Activation debounce suppresses a re-activation after rapid idle→activate→idle cycle

Currently, `demand_count` partially serves this role for workloads, but it's ad-hoc and only covers the demand-up/down case.

#### Design

Every state machine transition that takes time carries a **pending intent slot** using a priority-ordered enum:

```rust
/// Pending intent recorded during an in-flight transition.
/// Higher variants take priority over lower ones (Restart > Deactivate > Demand).
enum PendingIntent {
    /// No contradicting signal arrived during transition.
    None,
    /// DemandUp arrived — resume/relaunch after transition completes.
    Demand,
    /// ForceDeactivate or preemption — deactivate after transition completes.
    Deactivate,
    /// Spec changed (image update) — full restart after transition completes.
    Restart,
}
```

Each transition state carries a single `pending: PendingIntent`:

```rust
Suspending {
    pod_id: PodId,
    worker_id: WorkerId,
    artifact_id: ArtifactId,
    suspend_timeout: TimerKey,
    pending: PendingIntent,
}

Launching {
    pod_id: PodId,
    worker_id: WorkerId,
    launch_timeout: TimerKey,
    pending: PendingIntent,
}

Resuming {
    pod_id: PodId,
    worker_id: WorkerId,
    artifact_id: ArtifactId,
    resume_timeout: TimerKey,
    pending: PendingIntent,
}
```

When a transition completes, the state machine checks the pending intent before choosing the next state:

- `Suspending` completes + `Demand` → emit `ResumeRequest` immediately (skip entering `Suspended` state, avoid unnecessary cleanup)
- `Suspending` completes + `Deactivate` → enter `Suspended` / `Dormant` as normal (deactivation was the goal)
- `Suspending` completes + `Restart` → don't resume, go Dormant, relaunch with new spec
- `Launching` completes + `Demand` → already launching, no-op (workload is running)
- `Launching` completes + `Deactivate` → immediately begin deactivation (suspend or stop)
- `Launching` completes + `Restart` → stop pod, relaunch with new spec
- `Resuming` completes + `Deactivate` → immediately begin deactivation
- `Resuming` completes + `Restart` → stop pod, relaunch with new spec

**Conflict resolution**: Later signals upgrade the intent — `Demand` → `Deactivate` replaces `Demand`; `Restart` replaces anything. The enum ordering enforces this: a new signal only replaces the current intent if it has higher priority (higher variant).

The principle: **never discard a signal because the system is busy — record it as an intent and resolve it when the transition completes.**

#### Worker Drain as an Intent

Worker drain becomes a worker-level intent rather than a new state:

```rust
struct NamespaceWorkerState {
    pub fabric_status: FabricStatus,
    pub primary_pool_id: Option<PoolId>,
    pub draining: bool,  // ← intent: no new pods, existing pods deactivate on idle
}
```

When `draining == true`:
- `select_worker_for_pod` excludes this worker (same as a hard constraint)
- Existing workloads continue running until their idle timeout fires or all services go idle
- Once the worker has zero pods, it's safe to disconnect

#### Activation Debounce Fix

The fabric's 1-second activation debounce becomes state-aware by recording intents:

- Debounce tracks the service state, not just wall-clock time
- If the service has returned to `Idle` since the last activation event, the debounce resets immediately
- This prevents the pathological case where traffic is buffered with no activation event to trigger launch

This is a **known correctness issue** in the current implementation — see [Known Issues](#known-issues).

---

### Condition Model

#### Problem

Policy-relevant state is scattered across enum variants, log messages, and implicit behavior. `dv status` must assemble a human-readable view from many sources, and there's no event stream for `dv events`. Failed workloads, preempted workloads, snapshot eviction, worker pressure — each needs custom visibility logic.

#### Design

Extend the existing `WorkerCondition { key, active, message }` pattern to all entity types:

```rust
struct Condition {
    key: String,
    active: bool,
    message: String,
    since: Instant,
}

// Applied uniformly:
struct WorkerState {
    // ... existing fields ...
    pub conditions: HashMap<String, Condition>,
}

// New:
struct WorkloadStateMachine {
    // ... existing fields ...
    pub conditions: HashMap<String, Condition>,
}

struct ServiceStateMachine {
    // ... existing fields ...
    pub conditions: HashMap<String, Condition>,
}
```

#### Condition Keys by Entity

**Worker conditions** (already exist in protocol):
- `storage/pool/<id>/pressure-soft` — pool above 85%
- `storage/pool/<id>/pressure-hard` — pool above 95%
- `pressure/compute` — compute pressure elevated (with band label)
- `pressure/memory` — memory pressure elevated
- `draining` — worker is draining (no new pods)
- `pool/<id>/degraded` — pool is unhealthy but worker is alive (read-only filesystem, IO errors). Message includes the error. Scheduling avoids this worker for suspend; running pods are unaffected. If the only pool on a worker is degraded, suspend falls back to stop (workload goes Dormant on deactivation instead of Suspended).
- `unresponsive` — no messages received within heartbeat deadline (see [Known Issues](#known-issues), item 5). Excluded from scheduling; existing pods assumed lost after further timeout.

**Workload conditions** (new):
- `failed` — active when workload has exceeded max retries. Message: last error + attempt count. Cleared on spec update or manual restart.
- `retry-backoff` — active during backoff delay. Message: "attempt 3/5, next retry in 4s"
- `preempted` — active when workload was preempted. Message: "evicted for higher-priority workload". Cleared on re-activation.
- `snapshot-lost` — active when snapshot was evicted. Message: "storage pressure evicted snapshot, will cold-start on next activation". Cleared on next successful suspend.

**Service conditions** (new):
- `activation-pending` — active while service is in NeedBackend waiting for workload. Message: "traffic buffered, waiting for backend"
- `backend-not-ready` — active when pod is running but application hasn't passed readiness check (once readiness probes are implemented)

#### Observability Integration

- **`dv status`** = snapshot all active conditions per entity, grouped and formatted
- **`dv events`** = stream of condition transitions (set/cleared) — no separate event system needed
- **Alerting** = condition with `active == true` for longer than threshold

The condition model subsumes most of the "status visibility" requirements in one mechanism.

---

### Resource Leases

#### Problem

Resources (pod slots, memory, artifact entries, storage space) are claimed during async operations (launch, suspend, transfer) that can fail mid-way. On failure or worker disconnect, the resource is "leaked" — the orchestrator must have cleanup paths for each resource type and each failure mode. Currently these are ad-hoc: `PlacementTable` has `locked_by` for artifacts, but there's no equivalent for pod slot or memory reservations.

The same pattern recurs:
- Worker claims a pod slot during `LaunchPod` → worker dies → slot is "leaked" until orchestrator notices
- Artifact starts writing during suspend → worker dies → `Writing` entry with no owner
- Worker reports 800MB free → orchestrator schedules two 500MB pods simultaneously → overcommit

#### Design

A **lease** abstraction with automatic expiry:

```rust
struct Lease<T> {
    resource: T,
    holder: WorkerId,
    purpose: LeaseIntent,
    granted_at: Instant,
    deadline: Instant,
}

enum LeaseIntent {
    PodLaunch { pod_id: PodId },
    PodResume { pod_id: PodId, artifact_id: ArtifactId },
    ArtifactWrite { artifact_id: ArtifactId },
    ArtifactTransfer { artifact_id: ArtifactId },
}
```

#### Lifecycle

1. **Grant**: When the orchestrator selects a worker for a pod, it grants a lease on the pod slot (and memory, if resource requests exist). The worker's available capacity is immediately decremented.
2. **Commit**: When `PodRunning` arrives, the lease converts to actual occupancy. The lease deadline is cleared.
3. **Expire**: If the lease deadline passes without a commit (launch timeout, worker disconnect), the capacity reservation is automatically released. No separate cleanup path needed.
4. **Release**: When a pod stops, the occupancy is released.

#### Unifying Artifact Write Consistency

The existing `ArtifactStatus::Writing` / `ArtifactStatus::Ready` two-phase protocol becomes a special case of leasing:

- `ArtifactWriteStarted` → lease granted on the placement entry
- `ArtifactWriteCommitted` → lease committed, entry becomes `Ready`
- Worker disconnect → all `Writing` leases for that worker expire:
  - Local pool entries: dropped (pool is gone with the worker)
  - Shared pool entries: orchestrator issues cleanup command to another worker with access

The lease model also prevents scheduling a resume from a `Writing` artifact — the entry is leased, not `Ready`.

#### Capacity Reservation During Scheduling

When `select_worker_for_pod` chooses a worker, it takes a capacity lease:

```
Worker A: active_pods=7, leased_pods=1 → available = memory_remaining / per_pod_memory - leased_pods
```

If two pods are being scheduled simultaneously, the second sees reduced availability after the first's lease, preventing overcommit. If the first pod's launch fails, the lease expires and capacity returns.

#### Implementation Ordering

Leases depend on accurate capacity inputs. Recommended implementation order:

1. **PSI integration** — real pressure measurement (compute, memory, IO)
2. **Real memory detection** — replace hardcoded `available_memory_mb` with actual host memory
3. **Lease model** — now operating on real data

Until steps 1–2 are done, the existing ad-hoc artifact locking (`PlacementTable.locked_by`) is sufficient for artifact write consistency. Pod slot leases add little value when capacity declarations are fictional.

---

## Scheduling Policy

### Worker Selection

When a workload needs a pod (`PodRequest`), the orchestrator selects a worker.

**Current**: Filter workers with `fabric_status == Active` for the namespace, pick the one with the fewest pods (`min_by_key` on pod count).

**Target**: Two-phase selection using pressure scores and leases:

1. **Hard constraints** (filter):
   - `fabric_status == Active` for the namespace
   - Not draining (transition intent)
   - Sufficient memory for resource request (if specified) accounting for leases
   - Pressure score below Critical threshold (0.95) on all dimensions

2. **Soft preferences** (rank by composite score):
   - Lowest pressure score (weighted: memory > compute > storage)
   - Snapshot locality (prefer worker that already holds the snapshot for resume)
   - Pool locality (prefer worker with required artifacts in a bootable pool)

3. **Reserve capacity** (lease):
   - Grant a pod slot lease on the selected worker
   - Lease deadline = `LAUNCH_TIMEOUT_SECS` (currently 60s)

### Capacity Enforcement

The scheduler refuses to place a pod on a worker that exceeds capacity limits rather than silently overcommitting. Leases ensure that concurrent scheduling decisions don't race past limits. When no worker has capacity, the workload stays in `WaitingForCapacity` and preemption is considered.

### Preemption

When no worker has free capacity for an activated workload (traffic arrived, user is waiting), the orchestrator can preempt a lower-priority workload to make room.

#### Workload Priority Hierarchy

Priority is based on whether someone is actively waiting on the workload:

| Priority | Description | Preemptable? |
|---|---|---|
| 1 (highest) | **Activated** — traffic just arrived, client is blocked waiting | Never |
| 2 | **Active with traffic** — BackendNeed::Active/Traffic, sessions in progress | Avoid |
| 3 | **Active but idle** — running, BackendNeed::None, idle timer ticking | Yes — equivalent to early idle timeout |
| 4 | **Always-on, no traffic** — running by policy, no current demand signal | Yes — suspend/stop, reactivate on traffic |
| 5 (lowest) | **Suspended** — consuming storage only | Evict snapshot if storage-pressured |

#### Preemption Flow

1. `select_worker_for_pod` finds no worker with free capacity (accounting for leases)
2. Scan all workers for preemptable workloads (priority 3 and 4)
3. Select the lowest-priority, longest-idle workload on the best-fit worker
4. **Skip the idle timeout wait** — immediately trigger the normal deactivation path:
   - If `suspend_on_idle`: suspend (snapshot preserved for fast resume later). The suspend itself is fast (~ms for Firecracker snapshot) — the idle timeout wait is what we're skipping, not the suspend operation.
   - If not: stop pod, workload goes Dormant
5. The preempted workload's services return to Idle, ready to re-activate when capacity frees up
6. Set `preempted` condition on the preempted workload
7. Grant a capacity lease for the activated workload on the freed slot

**Suspend timeout edge case**: If the suspend hangs (hits `suspend_timeout`), the fallback to `StopPod` kicks in as usual. The activated workload's capacity lease is granted when the preempted pod's slot is freed — either on `PodSuspended` or on the `StopPod` fallback. The worst-case latency for the activated workload is bounded by `suspend_timeout`.

From the preempted workload's perspective, this looks identical to its idle timeout firing early. No new workload states are needed — just the orchestrator injecting a `ForceDeactivate` input or a `Preempt` input that triggers the same deactivation path.

#### Pressure-Driven Preemption

Under elevated pressure (pressure score > 0.8 on any dimension), preemption can be triggered proactively — even when pod count hasn't hit a hard limit. This prevents degradation before hitting hard limits. The pressure score provides a single threshold to tune instead of separate PSI/watermark/capacity checks.

---

## Activation & Idle Lifecycle

### Service Activation

1. Traffic arrives at a service IP on the fabric
2. Protocol activator (TCP/H2/Postgres) inspects the packet
3. Meaningful traffic (TCP SYN, H2 HEADERS) → `ServiceActivation` event
4. Service SM: Idle → NeedBackend (service now `wants_backend()`)
5. Reconciliation pass computes effective demand from service states, sends `SetDemand` to workload
6. Service sets `activation-pending` condition ("traffic buffered, waiting for backend")
7. Workload SM: Dormant → WaitingForCapacity → Launching → Running
8. Reconciliation sees workload Running + service NeedBackend → sends `WorkloadReady` to service
9. Service SM: NeedBackend → Active, `UpdateServiceBackend` + `ServiceReady` sent to workers
10. Service clears `activation-pending` condition
11. Fabric flushes buffered packets to the now-ready backend

### Idle Timeout

1. Protocol activator signals `BackendNeed::None` (TCP: no recent SYNs; H2: last stream closed)
2. Service SM starts idle timer (configurable per-service, default 30s)
3. Effective timeout adjusted by pressure band — see [Idle Timeout Under Pressure](#idle-timeout-under-pressure)
4. If `BackendNeed::Traffic` or `BackendNeed::Active` arrives before timeout → cancel timer
5. If timeout fires → `UpdateServiceBackend(None)`, service returns to Idle (no longer `wants_backend()`)
6. Reconciliation recomputes demand — if no services want the backend (current_demand → 0):
   - `suspend_on_idle == true`: workload suspends (snapshot for fast resume)
   - `suspend_on_idle == false`: workload stops, goes Dormant (cold start on next activation)

### Shared Workload Demand

Multiple services can back the same workload. Demand is derived by reconciliation from service states — the workload runs as long as any service `wants_backend()` (`current_demand > 0`). This means:

- Service A activates → reconciliation computes demand, workload launches → reconciliation sees workload Running + Service B in NeedBackend → Service B immediately gets `WorkloadReady`
- Service A goes idle → reconciliation recomputes demand → but Service B is still active → demand unchanged → workload stays Running
- Only when all services stop wanting a backend does current_demand reach 0 and suspend/stop triggers

### Activation Racing with Suspend (Transition Intent)

If traffic arrives while a workload is mid-suspend (`Suspending` state), the service transitions to NeedBackend. Reconciliation computes increased demand and sends `SetDemand` to the workload, which upgrades the pending intent to `PendingIntent::Demand`. When `PodSuspended` arrives, the state machine sees the pending demand and immediately emits `ResumeRequest` — skipping the `Suspended` state entirely.

The transition intent makes the optimization explicit and enables future improvements like aborting the suspend if the VM hasn't committed the snapshot yet.

### Suspend Timeout Behavior

When a suspend operation exceeds its `suspend_timeout` deadline:

1. The orchestrator emits `StopPod` — the VM is killed, no new snapshot is produced
2. If the workload had a prior snapshot (was previously `Suspended` with an `artifact_id`, was resumed, ran, and now the re-suspend failed):
   - The old snapshot is **stale** — state has diverged since the workload ran after being resumed
   - Set `snapshot-lost` condition: "suspend failed, prior snapshot invalidated (state diverged)"
   - The workload transitions to Dormant
   - Next activation will cold-start
3. If the workload had no prior snapshot (first suspend attempt after a cold launch):
   - `snapshot-lost` condition is **not** set (there was no prior snapshot to lose)
   - The workload transitions to Dormant
4. If `pending == PendingIntent::Demand` on the transition intent, the workload immediately re-launches via cold start

The orchestrator does **not** retry the suspend. Suspend failures are typically caused by conditions that won't resolve on immediate retry (disk full, VM in a bad state). The workload can still cold-start on next activation. If the failure was caused by transient storage pressure, the next deactivation cycle will attempt suspend again with potentially more space available.

See [Open Questions](#open-questions) item 15 for discussion of whether a single retry before fallback-to-stop is warranted.

### Graceful Drain on Suspend

When a workload begins deactivation (idle timeout or preemption):

1. For HTTP/2 services: the activator sends GOAWAY, giving clients a clean signal
2. For TCP services: connections break silently when the backend disappears
3. The protocol activator's `BackendNeed` signal serves as an implicit drain check — `BackendNeed::None` means no active flows
4. `UpdateServiceBackend(None)` is sent, then `SuspendPod` follows

See [Open Questions](#open-questions) for whether a configurable drain timeout should be added between steps 3 and 4.

---

## Pod Start Failure & Retry Policy

### Problem

Currently, if a pod fails to start (PodFailed/PodExited during launch, or launch timeout), the workload immediately re-emits `PodRequest` and retries if demand > 0. This loops forever on persistent failures (bad image, missing config, OOM on start).

### Desired Behavior

**Exponential backoff** with eventual failure state:

```
Attempt 1: immediate retry
Attempt 2: 1s delay
Attempt 3: 2s delay
Attempt 4: 4s delay
Attempt 5: 8s delay
...
Attempt N: min(2^(N-2), max_backoff) delay
```

After 5 consecutive failures (or failures within a time window), the workload transitions to a `Failed` state:

- Stops retrying automatically
- Services remain in `NeedBackend` (they still want the workload, it just can't deliver)
- `failed` condition set: `"ImagePullError: registry timeout (attempt 5/5)"`
- `dv status` shows the condition prominently
- Manual recovery: spec update (fix the image/config) clears the `failed` condition and retries, or explicit `dv restart` command

During backoff, the `retry-backoff` condition is active: `"attempt 3/5, next retry in 4s"` — visible in `dv status` so the developer knows the system is working on it.

### State Machine Impact

New additions to WorkloadStateMachine:

- `consecutive_failures: u32` counter (reset on successful `PodRunning`)
- `RetryBackoff { workload_id }` timer key
- Possibly a `Failed { last_error, retry_count }` state, or a sub-state within `WaitingForCapacity`
- `failed` and `retry-backoff` conditions (see [Condition Model](#condition-model))

### Interaction with Preemption

A workload in `Failed` state should not block capacity — it's not consuming a pod slot (no active lease). But it should not be invisible: the `failed` condition surfaces in `dv status` so the developer can fix the root cause.

---

## PSI (Pressure Stall Information) Integration

### What PSI Provides

Linux PSI (`/proc/pressure/{cpu,memory,io}`) measures the fraction of time tasks are stalled waiting for resources. Unlike capacity declarations (static) or utilization metrics (noisy), PSI directly measures degradation:

- **some**: at least one task is stalled (partial stall)
- **full**: all tasks are stalled (complete stall)

Each has 10s, 60s, and 300s rolling averages.

### Worker → Orchestrator Flow

Workers periodically report PSI levels (new `WorkerEvent` variant):

```rust
WorkerEvent::PressureUpdate {
    cpu: PsiMetrics,
    memory: PsiMetrics,
    io: PsiMetrics,
}

struct PsiMetrics {
    some_avg10: f64,
    some_avg60: f64,
    full_avg10: f64,
    full_avg60: f64,
}
```

Reporting cadence: periodic (e.g. every 10s), with immediate report on threshold crossings.

### How PSI Feeds the Pressure Score

PSI is one input to the [Worker Pressure Score](#worker-pressure-score), not a standalone policy driver. The pressure score normalizes PSI alongside static capacity metrics:

| Pressure Score Dimension | PSI Input | Static Fallback |
|---|---|---|
| `compute` | `psi_cpu_some_avg10 / 100` | None (0.0 without PSI) |
| `memory` | `psi_memory_some_avg10 / 100` | `pods_memory_committed / available_memory_mb` |
| `storage` | `psi_io_some_avg10 / 100` | `pool_used / pool_capacity` |
| `network` | N/A | `fabric_tunnel_tx_utilization` (when available) |

On non-Linux workers (libkrun/macOS), PSI is absent. The pressure score gracefully degrades to the static fallback — same thresholds, same policy bands, same code paths. No platform-specific branching in policy logic.

---

## Storage & Eviction Policy

### Eviction Priority

When a worker's storage pool exceeds watermarks (storage pressure score > 0.8), evict in this order (easiest to hardest):

1. **SharedRO artifacts with no active consumers** — cache entries, freely evictable
2. **CopyOnUse templates with no local working copies** — re-fetchable
3. **SharedRO artifacts with active consumers** — only if re-fetchable from registry/remote pool
4. **Snapshots of suspended workloads (LRU)** — workload sets `snapshot-lost` condition, can still cold-start
5. **Exclusive artifacts for suspended pods** — same consequence as #4
6. **Exclusive artifacts for running pods** — never evict without stopping the pod

### SnapshotLost Degradation

When a snapshot is evicted (sole copy lost):

- Workload transitions from `Suspended { artifact_id }` to a degraded state
- The workload spec is preserved — cold start is still possible on next activation
- `snapshot-lost` condition set: "storage pressure evicted snapshot, will cold-start on next activation"
- Next activation does a full cold start instead of fast resume
- Condition cleared on next successful suspend

### Capacity-Driven Eviction

When pod capacity is needed (preemption scenario), eviction targets running pods, not just storage:

1. First try to find a worker with free capacity accounting for leases (no eviction needed)
2. Then try to preempt a low-priority running workload (suspend it, freeing pod slot + memory)
3. The suspended workload's snapshot goes to the local pool
4. If the local pool is also full, cascade: evict a cold snapshot first, then suspend the preempted workload

### Artifact Write Consistency

Artifact writes use the [Resource Lease](#resource-leases) model, which subsumes the two-phase protocol (see `storage-pools-artifacts.md`):

- `ArtifactWriteStarted` → lease granted on the placement entry (status: `Writing`, not readable)
- `ArtifactWriteCommitted` → lease committed, entry updated to status: `Ready`

This matters for eviction and scheduling:

- **Never schedule a resume from a leased (Writing) artifact** — the snapshot is incomplete.
- **On worker disconnect, all Writing leases expire**: local pool entries are dropped (pool is gone with the worker); shared pool entries need cleanup by another worker with access to the pool.
- **Eviction should not target leased (Writing) artifacts** — let the write complete or expire, don't race with it.

---

## Spec Update Reconciliation

When `UpdateNamespace` delivers a new spec, the orchestrator must diff against current state and reconcile.

### What Can Change

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

Changing a container image without restarting the workload violates developer expectations badly. When someone pushes a new image and runs `dv up`, nothing changes until the pod cycles.

On image change detection during reconciliation, the orchestrator stops the current pod and re-launches with the new image. For staging, a hard cut (stop then start) is sufficient — rolling updates (start new before stopping old) add complexity for minimal benefit in the staging use case.

If a workload is mid-transition when the image change arrives, the `PendingIntent::Restart` intent ensures the new image is applied when the transition completes.

### Auto-Recovery on Spec Update

When a workload is in `Failed` state and its spec changes (new image, changed env), the orchestrator should clear the `failed` condition and retry. The intent is obvious: "I fixed the image, try again." This should not require a separate `dv restart`.

---

## DX Contract

### Latency Targets

| Operation | Target | Notes |
|---|---|---|
| Resume from snapshot | < 200ms end-to-end | Firecracker restore ~5-10ms + fabric flush |
| Cold start (VM boot) | < 2s to pod running | ~100ms VM boot + app startup |
| Activation response (resume path) | < 500ms from SYN to first response | Resume + buffer flush + TCP handshake |
| Activation response (cold path) | < 5s from SYN to first response | Boot + app startup + buffer flush |
| Buffer timeout | Must exceed activation latency | Default: 30s (covers cold start + slow apps) |

The core DX contract: **hitting a dormant service feels like a slow first request, not an error**. HTTP clients with default timeouts (30-60s) should never see a connection failure due to activation.

### Application Readiness Gap

`PodRunning` means the VM booted and the guest agent connected — not that the application inside is ready to handle traffic. Traffic forwarded during application startup causes errors (connection refused, 502s). For the scale-to-zero staging use case, the gap between VM-ready and app-ready is the difference between "slow first request" and "first request fails."

This directly threatens the core DX contract. See [Known Issues](#known-issues) for options.

### Status Visibility (Condition Model)

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

- **Capacity**: derived from pressure scores across workers
- **Workload health**: running, dormant, suspended + active conditions (failed, preempted, snapshot-lost)
- **Activation flow**: service state, backend need, `activation-pending` condition when buffering
- **Pressure**: shown when any worker's pressure score is Elevated or above

### Event Stream

`dv events` streams condition transitions — no separate event system:

```
12:03:01  service/web:8080  activation-pending  "traffic buffered, waiting for backend"
12:03:01  workload/web      [Dormant → Launching]
12:03:02  workload/web      [Launching → Running]
12:03:02  service/web:8080  activation-pending  cleared
12:05:32  workload/api      preempted  "evicted for higher-priority workload on worker-east"
12:05:33  workload/api      [Running → Suspended]
12:10:01  workload/api      snapshot-lost  "storage pressure on worker-east"
```

### Defaults for Staging

The CLI/gRPC path should default to scale-to-zero behavior for the staging use case:

- Every service gets `activation: { idle_timeout: 30s }` unless explicitly overridden
- Every workload gets `suspend_on_idle: true` unless explicitly overridden
- This means a freshly deployed staging environment has zero running pods until traffic arrives

> ~~**Implementation gap**~~: Fixed (Task 1.5). Both the compose conversion path and native spec path now default to `suspend_on_idle: true` and `idle_timeout_ms: 30_000`. Note: the compose frontend (`distvirt-compose`) bypasses the orchestrator entirely, sending `LaunchPod`/`CreateService` directly to workers — it doesn't use these defaults.

---

## Imperative Commands

The policy is primarily spec-driven and signal-driven, but operators need escape hatches when the policy does the wrong thing or when manual intervention is required.

### Command Semantics

| Command | Effect | Persistent? | Interaction with Policy |
|---|---|---|---|
| `dv restart <workload>` | Stop current pod (if any), clear `failed` condition, reset retry counter, re-launch if demand > 0 | No — one-shot | Policy resumes normal control after restart. If demand is 0 and no services are active, the workload goes Dormant after stop. |
| `dv stop <workload>` | Stop current pod, set `stopped` condition, suppress all demand | Yes — until `dv start` or spec update | Overrides demand signals. Services seeing traffic will buffer (activation-pending) but the workload will not launch. `dv status` shows `stopped` condition. Cleared by `dv start` or a spec update (same as `failed` auto-recovery). |
| `dv start <workload>` | Clear `stopped` condition, allow demand signals through | No — one-shot | If services have pending demand, workload activates immediately. Otherwise waits for traffic. |
| `dv drain <worker>` | Set `draining` intent on worker | Yes — until `dv undrain` or worker reconnect | No new pods scheduled. Existing pods deactivate on their normal idle timeout. Once pod count reaches 0, worker is safe to disconnect. |
| `dv undrain <worker>` | Clear `draining` intent | No — one-shot | Worker becomes eligible for scheduling again. |

### Design Principles

1. **Imperative commands set conditions/intents, not new state machine states.** `dv stop` sets a `stopped` condition and a suppression flag — it doesn't add a `Stopped` variant to the workload state machine. This keeps the state machine simple and the command's effect visible through the standard condition model.

2. **Spec updates override manual stops.** A `dv stop` is an operator saying "hold on, something is wrong." A spec update is the operator saying "I've fixed it, try again." The spec update takes precedence — this matches the auto-recovery behavior for `failed` conditions and avoids the common footgun of "I stopped it, forgot, deployed a fix, and nothing happened."

3. **No `dv scale`.** Multi-replica workloads are out of scope (see overview). The unit of control is start/stop/restart per workload.

4. **All commands are reflected in `dv status` and `dv events`** via the condition model. `dv drain worker-east` → condition `draining` set on worker-east → visible in status and event stream.

---

## Known Issues

Issues promoted from open questions because they are correctness bugs or high-priority DX gaps that should be fixed, not design choices to deliberate.

### 1. No Application Readiness Signal

`PodRunning` means VM booted + guest agent connected. The application inside may not be ready to handle traffic. Traffic forwarded during app startup causes connection refused / 502 errors.

**Options** (in order of preference):
1. **Readiness probe** — TCP connect or HTTP check, configurable in spec. Separate `PodReady` event from `PodRunning`. Service SM waits for `PodReady` before setting backend. Adds latency to activation but prevents errors.
2. **Protocol activator implicit check** — the activator detects upstream health (only works if activator can probe the backend). Limited applicability.
3. **Defer to application** — rely on retry-aware clients. Poor DX for the "feels like a slow first request" contract.

### 2. No Worker Heartbeat / Liveness Detection

The worker protocol has no heartbeat mechanism. Worker loss is detected only by TCP connection drop. A hung worker (not disconnected, but not processing) is invisible to the orchestrator — PSI updates stop, but there's no deadline on them.

**Fix**: Use the periodic `PoolCapacityUpdate` (30s interval) as an implicit heartbeat. If no message arrives within 2× the interval (60s), the orchestrator should treat the worker as unhealthy (set `unresponsive` condition, exclude from scheduling). After a further timeout, treat as disconnected.

### 3. No Pool Degradation Handling

A worker's storage pool can become degraded (read-only filesystem, IO errors, disk corruption) while the worker itself remains healthy and connected. Currently there is no mechanism to detect or respond to this — suspend operations will fail, burning the suspend timeout before falling back to stop.

**Fix**: Workers should report pool health via conditions (`pool/<id>/degraded`). The orchestrator uses this to: (a) exclude the worker from suspend-target selection (use another worker's pool or fall back to stop immediately, don't wait for the suspend timeout), (b) surface the degradation in `dv status`, (c) optionally trigger proactive snapshot migration off the degraded pool if another worker has healthy storage at Normal pressure.

### 4. Hardcoded Per-Pod Resource Sizing

`available_memory_mb` is derived from actual host memory via `/proc/meminfo`. However, `vcpus=1/pod` and `memory=128MB/pod` (`DEFAULT_POD_MEMORY_MB`) are still hardcoded. PSI integration provides real pressure feedback for compute, but static capacity accounting (used for scheduling admission and memory pressure fallback) operates on these fictional per-pod sizes. Resource leases (Task 4.1, done) track in-flight operations with `DEFAULT_POD_MEMORY_MB` but will provide more accurate reservation once per-pod resource requests exist in the spec.

### 5. ~~`route_miss_wake` Demand Leak~~ (Fixed)

**Resolved**: `route_miss_wake` field removed. `EndpointActivation` with no `service_id` now sets `has_active_flows = true` instead, which is naturally cleared by `EndpointFlowStatus(has_active_flows=false)`.

### 6. ~~`PlacementTable.locked_by` is Dead Code~~ (Fixed)

**Resolved**: `locked_by` field, `lock()`, and `unlock()` methods removed from `PlacementTable`/`ArtifactPlacement`. The actual double-resume guard is the `Suspended → Resuming` state transition.

---

## State Machine Architecture

### Hierarchy

```
Orchestrator
 ├── WorkerState (passive — capabilities, conditions, pressure, PSI, tunnel config)
 └── NamespaceStateMachine (per namespace)
      ├── WorkloadStateMachine (per workload)
      ├── ServiceStateMachine (per service)
      ├── PodMap (pod → workload/worker tracking)
      └── PlacementTable (artifact → pool/worker tracking, shared with orchestrator)
```

All state machines use a pure `step(input) → Vec<Output>` pattern with no I/O side effects. The namespace SM composes the lower-level SMs and forwards outputs between them. The shell/gRPC layer handles I/O, timers, and wire formats.

### State Definitions

**WorkloadState** (`types/states.rs`):
```
Dormant → WaitingForCapacity → Launching{pod_id, worker_id, launch_timeout, pending}
                                    → Running{pod_id, worker_id}
                                        → Suspending{pod_id, worker_id, artifact_id, suspend_timeout, pending}
                                            → Suspended{artifact_id}
                                                → Resuming{pod_id, worker_id, artifact_id, resume_timeout, pending}
                                                    → Running
                              → RetryBackoff{backoff_timer}
                              → Failed
```

`pending: PendingIntent` on transition states captures contradicting signals during async operations (see [Transition Intents](#transition-intents)).

Fields on `WorkloadStateMachine`: `workload_id`, `state`, `current_demand`, `needs_successful_boot`, `has_active_flows`, `suspend_on_idle`, `consecutive_failures`, `max_retries`, `last_failure_reason`, `conditions`.

**ServiceState** (`types/states.rs`):
```
Pending → Idle → NeedBackend → Active{pod_id, worker_id, backend_need, idle_timer}
```

Fields on `ServiceStateMachine`: `service_id`, `state`, `workload_id`, `has_activation`, `idle_timeout`, `conditions`.

**TimerKey** (`types/mod.rs`):
```rust
enum TimerKey {
    IdleTimeout { service_id },
    LaunchTimeout { workload_id, pod_id },
    SuspendTimeout { workload_id, pod_id },
    ResumeTimeout { workload_id, pod_id },
    RetryBackoffTimeout { workload_id },
}
```

### Two-Layer Worker State

Worker state is split between two levels, each serving a different purpose:

**`WorkerState`** (global, in `Orchestrator.workers`) — cluster-scoped:
- Capabilities (`available_memory_mb`, pools, public endpoint)
- WireGuard and tunnel config
- Worker conditions (from protocol events)
- `pressure: WorkerPressure` — raw normalized scores per dimension
- `pressure_bands: PressureBands` — hysteresis state per dimension
- `psi: Option<WorkerPsi>` — cached PSI metrics from last `PressureUpdate`

**`NamespaceWorkerState`** (namespace-scoped, in `NamespaceStateMachine.workers`):
- `fabric_status: FabricStatus` — Creating/Active/Destroying for this namespace
- `primary_pool_id: Option<PoolId>` — resolved at assignment time for suspend operations
- `pressure_band: PressureBand` — **propagated** from global `WorkerState.pressure_bands.max_band()`

Propagation is one-directional (global → namespace) via `propagate_pressure_to_namespaces()` after each pressure recomputation. The namespace SM never writes back to global state.

**Scheduling reads global state** (`select_worker_for_pod` reads `Orchestrator.workers`). **Idle timeout reads namespace state** (`pressure_adjusted_idle_timeout` reads `NamespaceWorkerState.pressure_band`). This is consistent but means Phase 4 additions must decide which layer owns new fields — see [Phase 4 Architectural Considerations](#phase-4-architectural-considerations).

### Testing Infrastructure

Three complementary methods:

1. **Stateright model checking** — exhaustive DFS exploration of reachable states
   - `stateright_workload.rs`: 15 scenarios — timer consistency, demand bounds, pending intent invariants, retry/failure reachability, ForceDeactivate
   - `stateright_service.rs`: 4 scenarios — idle timer consistency, activation reachability
   - `stateright_model.rs` (namespace): 11 scenarios — referential integrity, multi-SM coordination, spec update restart, worker loss

2. **Proptest** — randomized input sequences with invariant checking
   - `proptest.rs`: PodMap shadow consistency, namespace invariant fuzzing, orchestrator panic testing

3. **Scenario tests** — mock workers over real shell/protocol stack with paused time
   - ~60 scenario tests across: activation, suspend/resume, failure recovery, fabric routing, preemption, pressure, registry, worker lifecycle, multi-service, snapshot placement, spec reconciliation, transition intents

4. **Shell integration** — end-to-end lifecycle with yamux/capnp handshake
   - `shell_integration.rs`: full protocol roundtrip

**Methodology**: Write stateright model properties first, then implement SM changes to satisfy them. Proptest catches edge cases the model's action space doesn't cover. Scenario tests verify the full stack (shell + orchestrator + namespace + workload/service SMs).

---

## Implementation Plan

### Current State Summary

| Area | Status | Code Reference |
|---|---|---|
| Worker selection | Done — two-phase pressure-aware: hard constraints (Active + pressure < High), soft (lowest band, then fewest pods) | `orchestrator/scheduling.rs` |
| Service activation lifecycle | Done — activation, idle timeout, reconciliation-based demand | `service.rs`, `namespace/events.rs`, `namespace/reconciliation.rs` |
| Suspend/resume | Done — end-to-end with artifact tracking | `workload.rs`, `namespace/events.rs`, `namespace/output.rs` |
| Transition intents | Done — `PendingIntent` enum on transition states, `ForceDeactivate` input | `types/states.rs`, `workload.rs` |
| Retry/backoff | Done — exponential backoff with `Failed` terminal state, recovery via `SpecChanged`/`ManualRestart` | `workload.rs`, `types/states.rs` |
| Spec reconciliation | Done — image change dispatches `SpecChanged`, `suspend_on_idle` change updates field | `namespace/commands.rs` |
| Activation debounce | Done — debounce cleared on backend removal | `distvirt-worker/src/fabric/service.rs` |
| CLI/gRPC defaults | Done — `suspend_on_idle: true`, `idle_timeout_ms: 30_000` | `distvirt-cli/` |
| Condition model | Done — `conditions: HashMap<String, String>` on workload/service SMs, included in status reports | `workload.rs`, `service.rs`, `namespace/output.rs`, `types/client.rs` |
| Pressure scores | Done — `WorkerPressure` + `PressureBands` with hysteresis, recomputed on pool updates and pod changes | `types/states.rs`, `orchestrator/mod.rs`, `shell/mod.rs` |
| PSI integration | Done — workers read `/proc/pressure/` every 10s, orchestrator feeds into pressure scores | `worker/mod.rs`, `types/states.rs`, `shell/mod.rs` |
| Pressure-driven idle timeout | Done — namespace layer adjusts `IdleTimeout` by pressure band (75%/25%/5s floor) | `namespace/output.rs` |
| Real memory detection | Done — `/proc/meminfo` at startup, fallback 1024 MB | `worker/mod.rs` |
| Resource leases | Done — `LeaseTable` tracks in-flight pod launches/resumes, released on lifecycle events/timeouts/worker disconnect, included in pressure accounting | `types/states.rs`, `orchestrator/mod.rs`, `orchestrator/scheduling.rs`, `orchestrator/workers.rs` |
| Preemption | Done — namespace-scoped, priority derived from runtime state (idle/no-demand), dispatches `ForceDeactivate` to victim, sets `preempted` condition | `orchestrator/scheduling.rs`, `namespace/commands.rs` |
| Worker drain | **Not implemented** | — |
| Worker heartbeat | **Not implemented** — loss detected only by TCP drop | — |
| Imperative commands | **Not implemented** | — |

Phases 1–3 are complete. Phase 4: Resource Leases (4.1) and Preemption (4.2) are done. See git history for detailed per-task changelogs.

---

### Pre-Phase-4 Cleanup

Issues that should be addressed before starting Phase 4 tasks. These are not feature work — they fix existing bugs and remove dead code that would cause confusion or incorrect behavior in Phase 4.

#### ~~Fix `route_miss_wake` demand leak~~ (Done)

Resolved: `route_miss_wake` removed. `EndpointActivation` with no `service_id` now sets `has_active_flows = true`.

#### ~~Remove dead `PlacementTable.locked_by`~~ (Done)

Resolved: `locked_by`, `lock()`, `unlock()` removed from `ArtifactPlacement`/`PlacementTable`.

#### Fix stale `PendingIntent::Restart` comment

**Priority**: Trivial.

`types/states.rs` comment on `Restart` says "stubbed — no input produces it yet" but `WorkloadInput::SpecChanged` has produced it since Task 1.3.

---

### Phase 4 — Advanced Policy

#### Phase 4 Architectural Considerations

Several cross-cutting concerns affect multiple Phase 4 tasks:

**Two-layer worker state boundary**: Scheduling (`select_worker_for_pod`) runs at the orchestrator level and reads `Orchestrator.workers` (global). But worker drain (Task 4.3) adds `draining: bool` to `NamespaceWorkerState` (namespace-scoped per the policy design). The drain check in scheduling needs access to namespace-level state. Options:
1. Pass drain state up to the orchestrator level (add `draining` to global `WorkerState` or to scheduling context)
2. Move the drain check into the namespace layer — emit `PodRequest` only for non-draining workers, so `select_worker_for_pod` never sees them
3. Make drain a global worker-level concept (simpler, but prevents per-namespace drain)

Recommendation: Option 1 — drain is rare enough that global scope is fine for v1.

**Timer routing for heartbeat**: The shell's `timer_ns: HashMap<TimerKey, NamespaceId>` only handles namespace-scoped timers. Worker heartbeat (Task 4.4) is cluster-scoped. The policy doc's recommendation (use `PoolCapacityUpdate` as implicit heartbeat, track `last_message_at` in the shell layer) avoids SM timers entirely. Implement as a periodic sweep in the shell rather than per-worker SM timers.

**Priority field**: Preemption (Task 4.2) needs a priority hierarchy, but `WorkloadSpec` has no priority field. The policy doc defines priority based on runtime state (activated > active-with-traffic > idle > always-on > suspended), not spec-declared priority. This means preemption can work without a spec change — priority is derived from workload/service state at preemption time.

#### ~~Task 4.1: Resource Leases~~ (Done)

**Problem**: Resources (pod slots, memory, artifact entries) are claimed during async operations that can fail. On failure, resources are "leaked" until ad-hoc cleanup runs. With preemption, freed capacity could be assigned to a different waiting workload than the one that triggered preemption.

**Changes**: `LeaseTable` keyed by `PodId` tracks in-flight pod operations. `Lease` carries `worker_id`, `LeaseIntent` (PodLaunch or PodResume), and `memory_mb`. Leases are granted on `LaunchPod`/`ResumePod` dispatch and released on pod lifecycle events (`PodRunning`, `PodExited`, `PodFailed`, `PodSuspended`, `PodSuspendFailed`), timeouts (`LaunchTimeout`, `ResumeTimeout`), and worker disconnect.

**Implementation**:
1. `types/states.rs`: `LeaseIntent`, `Lease`, `LeaseTable` with methods (`grant`, `release`, `release_worker_leases`, `leased_memory_mb`, `leased_pod_count`, `iter`)
2. `orchestrator/mod.rs`: `lease_table` field on `Orchestrator`, `recompute_worker_pressure` includes `leased_memory_mb` in committed memory, `check_invariants` verifies lease worker references, `route_namespace_input` releases leases before forwarding lifecycle events/timeouts
3. `orchestrator/scheduling.rs`: grants leases after `LaunchPod`/`ResumePod` dispatch in `process_namespace_output` and `schedule_waiting_pods`
4. `orchestrator/workers.rs`: `handle_worker_disconnected` releases all worker leases before removal

**Note**: With `DEFAULT_POD_MEMORY_MB = 128` still hardcoded for all pods, leases primarily prevent pod *slot* overcommit. Memory-based lease value is limited until per-pod resource requests exist in the spec.

---

#### ~~Task 4.2: Preemption~~ (Done)

**Problem**: When no worker has capacity for an activated workload, it waits forever in `WaitingForCapacity`.

**Changes**: Namespace-scoped preemption via `NamespaceInput::PreemptWorkload`. Priority derived from runtime state: priority 3 (active but idle — `BackendNeed::None`) and priority 4 (always-on, `current_demand == 0`) are preemptable. Active-with-traffic workloads (`BackendNeed::Active/Traffic`) are never preempted.

**Implementation**:
1. `process_namespace_output()`: when `select_worker_for_pod()` returns `None` for a `PodRequest`, calls `try_preempt_for_workload()`
2. `try_preempt_for_workload()` scans same-namespace workloads for Running preemptable candidates (priority 3–4), selects victim by (highest priority number, most pods on worker, workload ID for determinism)
3. Dispatches `NamespaceInput::PreemptWorkload { workload_id }` to the namespace SM
4. `handle_preempt_workload()` force-deactivates the victim's services, steps the workload SM with `ForceDeactivate`, sets `preempted` condition
5. `schedule_waiting_pods()` naturally retries the waiting workload once the victim's pod slot frees

**Key design decisions**:
- One preemption per scheduling attempt (avoids cascading evictions)
- No capacity reservation/lease — relies on `schedule_waiting_pods()` called after every `process_namespace_output()`
- Reuses `WorkloadInput::ForceDeactivate` — from the victim's perspective, identical to an early idle timeout

**Files**: `orchestrator/scheduling.rs`, `namespace/commands.rs`, `types/namespace_io.rs`, `namespace/mod.rs`

---

#### Task 4.3: Worker Drain

**Problem**: No mechanism to gracefully drain a worker before maintenance.

**Changes**: Add `draining: bool` to worker state (see [Architectural Considerations](#phase-4-architectural-considerations) for layer decision). Scheduling excludes draining workers. Existing pods deactivate on their normal idle timeout. Once pod count reaches zero, the worker is safe to disconnect.

**Implementation sketch**:
1. Add `DrainWorker { worker_id }` / `UndrainWorker { worker_id }` to `OrchestratorInput` (client command)
2. Add `draining: bool` to `WorkerState` (global level — simplest for v1)
3. `select_worker_for_pod` adds `!draining` to hard constraints
4. Set `draining` condition on the worker (visible in `dv status`)
5. No active eviction — pods drain naturally via idle timeouts

**Depends on**: Nothing (self-contained). Can be implemented first as a warm-up for Phase 4.

---

#### Task 4.4: Worker Heartbeat / Liveness

**Problem**: Hung workers (connected but unresponsive) are invisible to the orchestrator.

**Changes**: Use existing message flow as implicit heartbeat. Track `last_message_at: Instant` per worker in the shell layer. Periodic sweep (every 30s) checks for workers exceeding the 60s deadline.

**Implementation sketch**:
1. Add `last_message_at: Instant` to shell-level worker tracking (not SM state)
2. Update on any message from the worker (events, capacity updates, PSI)
3. Add a periodic sweep (30s interval) that checks all workers
4. Workers exceeding 60s deadline: set `unresponsive` condition, exclude from scheduling
5. Workers exceeding a further deadline (e.g., 120s): treat as disconnected (trigger `WorkerDisconnected`)

**Depends on**: Nothing (self-contained). Does not require SM timer changes.

---

#### Phase 4 Task Dependencies & Recommended Order

```
Pre-Phase-4 cleanup (done: route_miss_wake fix, locked_by removal)
    │
    ├──→ Task 4.3 (Worker Drain) ← simplest, self-contained warm-up
    │
    ├──→ Task 4.4 (Worker Heartbeat) ← self-contained, shell-layer only
    │
    ├──→ Task 4.1 (Resource Leases) ← done
    │
    └──→ Task 4.2 (Preemption) ← done
```

Recommended implementation order: cleanup → 4.3 → 4.4 → 4.1. Tasks 4.3 and 4.4 can be done in parallel since they're independent. Task 4.1 (Resource Leases) and Task 4.2 (Preemption) are complete.

#### Test Harness Additions for Phase 4

The existing test harness (mock workers, paused time, event injection, command inspection) is structurally ready. Minor additions needed:

| Phase 4 Task | Harness Addition |
|---|---|
| Worker Drain | `drain_worker(id)` helper, `assert_worker_draining()` assertion |
| Worker Heartbeat | `MockWorkerConfig::with_silent()` (connects but sends no events), `assert_worker_unresponsive()` |
| Preemption | Done — 4 scenario tests in `tests/scenarios/preemption.rs` (basic preemption, no-preempt-active-traffic, no-preempt-with-capacity, reactivation after preemption) |
| Resource Leases | Done — unit tests for `LeaseTable` operations in `types/states.rs` |

### Design Notes for Implementers

#### `current_demand`, `needs_successful_boot`, and `PendingIntent`

Three mechanisms work together for demand management:

- **`current_demand`**: set authoritatively by reconciliation via `SetDemand { count }`. Reconciliation derives this from service states (`wants_backend()`) plus `has_active_flows`. No incremental DemandUp/DemandDown — demand is always consistent with actual service states.
- **`needs_successful_boot`**: once demand goes 0→non-zero, the workload is committed to reaching Running before it can go Dormant via SetDemand(0). Also set on WorkerLost and PodGone. Cleared on PodRunning→Running or entering Failed. Prevents demand fluctuations during boot/retry from prematurely stopping the workload.
- **`PendingIntent`**: captures contradicting signals that arrive *during transitions*. Consumed once when the transition completes.

`PodSuspended` completion checks the `pending` intent — `Demand` triggers immediate `ResumeRequest`, `Deactivate` stays `Suspended`, `Restart` deletes artifact and transitions on demand. `PodRunning` from `Resuming` with `pending == None` falls back to `current_demand` check.

#### The `Pending` Service State

`ServiceState::Pending` serves dual duty: initial state (pre-reconciliation) and temporary placeholder during `mem::replace`. This is fragile but not worth splitting until the service SM gets more complex. The reconciliation logic (`reconciliation.rs`) only matches on `(Pending, Dormant)` — it should also handle `(Pending, Suspended)` for correctness after worker loss.

#### Suspend Timeout and `snapshot-lost`

The policy says: when suspend times out and the workload had a prior snapshot (was previously Suspended, resumed, and now re-suspend failed), set `snapshot-lost` condition. The SM doesn't track prior snapshot existence. The `Suspending` state carries `artifact_id` — but this is the *new* artifact being written, not the prior one. The prior artifact was deleted on successful resume (`workload.rs`). So suspend timeout after resume never has a prior snapshot to lose — the condition only applies to first-suspend failures where a prior snapshot existed from a *different* suspend cycle that was somehow preserved.

In practice: `snapshot-lost` matters for the eviction case (storage pressure deletes a `Suspended` workload's artifact), not the suspend-timeout case. The suspend-timeout path already handles this correctly — the workload goes `Dormant` and cold-starts next time.

---

## Open Questions

### V1 (Staging)

13. **Buffer overflow during slow cold starts**: The fabric buffers up to 64 frames with a 30s timeout. For workloads with slow application startup (common in staging — heavy frameworks, DB migrations), the buffer can fill before the backend is ready. Frames beyond 64 are silently dropped. Options: (a) increase default buffer size, (b) make buffer size configurable per-service in `ServicePolicy`, (c) accept drops and rely on client retransmission (TCP) or retry (HTTP). The 30s timeout is probably fine; the 64-frame limit may be too low for bursty traffic during activation. Making it configurable per-service is low effort and avoids one-size-fits-all problems.

### Post-V1

1. **Affinity / anti-affinity**: Should workloads be able to express preferences for co-location (e.g., "run on the same worker as the database") or spreading? Not needed for v1 staging use case, but matters for multi-worker production.

2. **Resume on different worker**: Currently resume is pinned to the worker holding the snapshot. With shared pools or artifact transfer, resume could target any worker. When should the orchestrator prefer a different worker (e.g., the snapshot's worker is under pressure)? The pressure score provides a clear signal — if the holding worker is Elevated+, prefer transferring the snapshot to a Normal worker if transfer time < cold start time.

3. **Bin-packing vs spreading**: For staging (many dormant, few active), bin-packing makes sense — pack active workloads onto fewer workers so others can be idle/terminated. For resilience, spreading is better. What's the right default? The pressure score naturally encourages spreading (packing raises pressure, deprioritizing the packed worker), but explicit bin-packing would require actively preferring higher-pressure workers below a threshold.

4. **Preemption scope**: Should preemption cross namespace boundaries? If namespace A is at capacity and namespace B has idle workloads on the same worker, can A preempt B's workloads? This depends on isolation/tenancy model. Namespace-level resource quotas (not yet in spec) would inform this — a namespace can only use capacity it's been allocated.

5. **Always-on preemption**: If a workload is always-on (no activation spec) and gets preempted, what happens? It can't go to Idle (no activation). Options: stay in `WaitingForCapacity` and re-launch when capacity frees, or add activation retroactively for preempted always-on workloads.

7. **Failure scope**: If a workload fails on worker A, should it retry on worker B? The failure might be worker-specific (bad local state) or workload-specific (bad image). Trying a different worker on the Nth retry could help diagnose. The lease model makes this straightforward — release the lease on worker A and grant a new one on worker B.

9. **Cross-worker snapshot migration before eviction**: When a worker needs to evict a snapshot, should the orchestrator first try to transfer it to a less-pressured worker (preserving fast resume) before evicting it entirely? This trades transfer time/bandwidth for better resume latency later. The pressure score makes this decision clear: if any other worker has Normal storage pressure, transfer; otherwise, evict.

11. **Partial spec updates**: Should `UpdateNamespace` support partial diffs (only changed services/workloads), or always require the full spec? Full spec is simpler (no merge logic) but wasteful for small changes.

12. **Graceful drain on suspend**: Should there be a configurable drain period between `UpdateServiceBackend(None)` and `SuspendPod`? The H2 activator sends GOAWAY, giving HTTP/2 clients a clean signal. TCP has no equivalent — connections break silently. Options: (a) configurable drain timeout (adds latency to suspend), (b) rely on the protocol activator to signal readiness for suspend (H2: last stream closed; TCP: no active flows), (c) accept connection resets as the cost of suspend (simple, but poor DX for long-lived TCP connections like database clients).

14. **Worker provisioner interface**: The orchestrator currently has no concept of provisioning new workers. When all workers are at capacity and preemption isn't sufficient, what happens? Options: (a) `WaitingForCapacity` indefinitely (current), (b) emit a `ProvisionWorker` output that the shell maps to an infrastructure API, (c) external autoscaler watches orchestrator state (reads pressure scores and conditions).

15. **Suspend failure retry**: When `PodSuspendFailed` fires, the current behavior is to fall back to `StopPod` — the snapshot is lost and the workload goes Dormant on next deactivation. Should the orchestrator retry the suspend before giving up? One retry might recover from transient failures (disk full momentarily, timeout due to load) and preserve the fast-resume path. The transition intent model supports this naturally — set a `pending_retry_suspend` intent before falling back to stop.

16. **Per-pod resource sizing**: Resource requests (cpu, memory) are not in the spec. Currently all pods are 128MB/1vCPU. Once pods vary in size, preemption becomes a bin-packing problem (evicting one large pod vs. two small ones). The pressure score handles the "is this worker stressed?" question regardless of pod sizes, but the scheduler needs to ensure the freed capacity actually fits the incoming workload.

17. **Namespace-level resource quotas**: Should namespaces have pod count or memory limits? In multi-tenant staging, one namespace could monopolize all workers. Quotas would bound per-namespace consumption and inform cross-namespace preemption scope (question 4).
