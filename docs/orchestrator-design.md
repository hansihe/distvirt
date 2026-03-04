# Orchestrator Design

## Overview

The orchestrator is the central control plane for distvirt. It manages workers, drives namespace lifecycle, handles scale-to-zero activation, and exposes a client protocol (gRPC) for CLI/UI control.

The orchestrator is a **pure state machine** at its core. All logic lives in a synchronous `step(input) -> output` function with no I/O. An async shell dispatches inputs from network connections and timers, and sends outputs to workers and clients. This separation is the foundation for testing, model checking, and fuzzing.

### Implementation Status

The core orchestrator (namespace/workload/service state machines, worker management, gRPC server, suspend/resume lifecycle, log/event streaming) is implemented and functional. The following features described in this document are **stubs or not yet wired**:

- **Splice / Unsplice** — handlers exist but are no-ops (return Ok without modifying state). Planned.
- **CloneNamespace** — returns "not yet implemented" error. Planned.
- **WatchNamespaceStatus** — defined in the proto but not yet wired to the event streaming system

---

## Core Architecture

### Two-Layer Design

The orchestrator has two layers:

1. **Outer layer** — routes inputs to the correct namespace state machine, manages worker connections, handles cross-namespace operations (clone, list), generates pod IDs, and selects workers for pod scheduling.
2. **Namespace state machine** — a pure, self-contained state machine for a single namespace. All service lifecycle, activation, suspend/resume, and reconciliation logic lives here.

This separation keeps the per-namespace state machine small and independently testable. Cross-namespace interactions are minimal (limited to clones) and handled at the outer layer.

```rust
struct Orchestrator {
    namespaces: HashMap<NamespaceId, NamespaceStateMachine>,
    workers: HashMap<WorkerId, WorkerState>,
    clients: HashSet<ClientId>,
    next_pod_id: u64,
}
```

The outer layer receives top-level inputs and dispatches to namespace state machines:

```rust
/// Top-level inputs to the orchestrator.
enum OrchestratorInput {
    ClientConnected { client_id: ClientId },
    ClientDisconnected { client_id: ClientId },
    ClientCommand { client_id: ClientId, command: ClientCommand },
    WorkerConnected { worker_id: WorkerId, capabilities: WorkerCapabilities, wg_config: Option<WorkerWgConfig> },
    WorkerDisconnected { worker_id: WorkerId },
    NamespaceInput { namespace_id: NamespaceId, input: NamespaceInput },
}
```

Most inputs are routed directly to a namespace. `WorkerDisconnected` fans out a `WorkerLost` event to every namespace that had pods on that worker. `CreateNamespace` instantiates a new state machine. `ListNamespaces` reads across all state machines.

The outer layer also handles pod scheduling: when a namespace emits a `PodRequest`, the outer layer selects a worker, generates a pod ID, and injects `LaunchPod` back into the namespace. Similarly for `ResumeRequest`, the outer layer generates a new pod ID and injects `ResumePod`.

### Per-Namespace State Machine — Three-Layer Split

The namespace state machine is split into three layers to keep each piece small and independently testable:

1. **NamespaceStateMachine** (thin coordinator): fabric management, namespace lifecycle, event routing between sub-state-machines, WireGuard peer management.
2. **WorkloadStateMachine**: pod lifecycle (scheduling, launching, running, suspend/resume). Driven by demand signals from services.
3. **ServiceStateMachine**: activation, idle timeout, backend routing. Driven by activation events and workload readiness.

Multiple services can share a single workload. The coordinator maintains the mapping and forwards signals between them.

This split reduces the model-checking state space from O(states^N) (monolithic, all services interleaved) to O(states × N) (each sub-SM checked independently).

```rust
struct NamespaceStateMachine {
    namespace_id: NamespaceId,
    spec: NamespaceSpec,
    status: NamespaceStatus,
    workers: HashMap<WorkerId, NamespaceWorkerState>,

    /// Derived from ServiceSpec.workload_id. Multiple services can share
    /// a workload (e.g. multiple entry points into the same pod).
    service_workload: HashMap<ServiceId, WorkloadId>,
    workloads: HashMap<WorkloadId, WorkloadStateMachine>,
    services: HashMap<ServiceId, ServiceStateMachine>,

    /// Tracks all active pods and their workload/worker association.
    pods: HashMap<PodId, PodInfo>,
    /// WireGuard peer IP allocation and tracking for developer network access.
    wg_peer_manager: WireGuardPeerManager,
}

/// Workload sub-state-machine. Manages the pod lifecycle for one workload.
struct WorkloadStateMachine {
    workload_id: WorkloadId,
    state: WorkloadState,
    /// Number of services currently requesting this workload be running.
    demand_count: u32,
    /// Whether to suspend instead of stop when demand drops to zero.
    suspend_on_idle: bool,
}

/// Service sub-state-machine. Manages activation, idle timeout, and
/// backend routing for one service.
struct ServiceStateMachine {
    service_id: ServiceId,
    state: ServiceState,
    workload_id: WorkloadId,
    has_activation: bool,
    idle_timeout: Duration,
}
```

The coordinator dispatches external inputs and routes internal signals between sub-SMs:

```rust
/// Inputs to a single namespace state machine.
enum NamespaceInput {
    WorkerEvent { worker_id: WorkerId, event: WorkerEvent },
    WorkerLost { worker_id: WorkerId },
    TimerFired { timer_key: TimerKey },
    // Client commands that target this namespace
    UpdateSpec { client_id: ClientId, spec: NamespaceSpec },
    Delete { client_id: ClientId },
    GetStatus { client_id: ClientId },
    Splice { client_id: ClientId, workload_id: WorkloadId, worker_id: WorkerId },
    Unsplice { client_id: ClientId, workload_id: WorkloadId },
    StreamLogs { client_id: ClientId, service_id: Option<ServiceId> },
    /// Outer layer injects this when it has selected a worker and
    /// generated a pod ID for a workload's PodRequest.
    LaunchPod { workload_id: WorkloadId, worker_id: WorkerId, pod_id: PodId },
    /// Outer layer injects this to resume a suspended workload from snapshot.
    ResumePod { workload_id: WorkloadId, worker_id: WorkerId, pod_id: PodId, snapshot_id: SnapshotId },
    /// Developer network access via WireGuard.
    Connect { client_id: ClientId, client_public_key: [u8; 32], worker_wg_public_key: [u8; 32], worker_endpoint: String },
    Disconnect { client_id: ClientId, client_public_key: [u8; 32] },
}

struct NamespaceOutput {
    worker_commands: Vec<(WorkerId, WorkerCommand)>,
    client_events: Vec<(ClientId, ClientEvent)>,
    timers_set: Vec<(TimerKey, Duration)>,
    timers_cancel: Vec<TimerKey>,
    /// Workloads that need a pod. The outer layer selects a worker,
    /// generates a pod ID, and injects LaunchPod back.
    pod_requests: Vec<PodRequest>,
    /// Workloads that need to resume from snapshot. The outer layer
    /// generates a pod ID and injects ResumePod back.
    resume_requests: Vec<ResumeRequest>,
    /// State machine events for observability/streaming.
    events: Vec<SmNamespaceEvent>,
    /// True when the namespace has been fully destroyed and should
    /// be removed from the outer layer.
    destroyed: bool,
}

struct PodRequest {
    workload_id: WorkloadId,
}

struct ResumeRequest {
    workload_id: WorkloadId,
    snapshot_id: SnapshotId,
    worker_id: WorkerId,
}
```

#### Sub-SM Input/Output Types

The coordinator communicates with sub-SMs via typed internal signals:

```rust
/// Inputs to the workload sub-state-machine.
enum WorkloadInput {
    /// A service needs this workload running.
    DemandUp,
    /// A service no longer needs this workload running.
    DemandDown,
    /// Outer layer has assigned a pod and worker — launch it.
    /// The workload doesn't select workers; the outer layer does.
    LaunchPod { worker_id: WorkerId, pod_id: PodId },
    /// Outer layer has generated a pod_id for resuming from snapshot.
    ResumePod { worker_id: WorkerId, pod_id: PodId, snapshot_id: SnapshotId },
    /// Pod is now running (from worker event).
    PodRunning { pod_id: PodId },
    /// Pod exited or failed (from worker event).
    PodGone { pod_id: PodId },
    /// Pod was successfully suspended to snapshot.
    PodSuspended { pod_id: PodId, snapshot_id: SnapshotId },
    /// Pod suspend operation failed.
    PodSuspendFailed { pod_id: PodId },
    /// Worker hosting this workload's pod disconnected.
    WorkerLost { worker_id: WorkerId },
    /// Timer fired (launch timeout, suspend timeout, or resume timeout).
    TimerFired { timer_key: TimerKey },
}

/// Outputs from the workload sub-state-machine.
enum WorkloadOutput {
    /// The workload needs a pod — outer layer should select a worker,
    /// generate a pod ID, and inject LaunchPod back.
    PodRequest,
    /// The workload needs to resume from snapshot — outer layer should
    /// generate a pod ID and inject ResumePod back.
    ResumeRequest { snapshot_id: SnapshotId, worker_id: WorkerId },
    /// The workload's pod is now running and reachable.
    BecameReady { pod_id: PodId, worker_id: WorkerId },
    /// The workload's pod is no longer available.
    BecameUnready,
    /// Worker commands to emit.
    WorkerCommand(WorkerId, WorkerCommand),
    /// Timer management.
    TimerSet(TimerKey, Duration),
    TimerCancel(TimerKey),
}

/// Inputs to the service sub-state-machine.
enum ServiceInput {
    /// The service's workload became ready (pod running).
    WorkloadReady { pod_id: PodId, worker_id: WorkerId, backend: ServiceBackend },
    /// The service's workload became unready (pod lost).
    WorkloadUnready,
    /// Activation event from worker (first traffic hit).
    ServiceActivation,
    /// Ongoing backend need signal from worker.
    ServiceBackendNeed { need: BackendNeed },
    /// Timer fired (idle timeout).
    TimerFired { timer_key: TimerKey },
}

/// Outputs from the service sub-state-machine.
enum ServiceOutput {
    /// This service needs its workload running.
    DemandUp,
    /// This service no longer needs its workload running.
    DemandDown,
    /// Worker command to emit to a specific worker.
    WorkerCommand(WorkerId, WorkerCommand),
    /// Worker command to emit to all active workers in the namespace.
    BroadcastWorkerCommand(WorkerCommand),
    /// Timer management.
    TimerSet(TimerKey, Duration),
    TimerCancel(TimerKey),
}
```

The coordinator's `step` function is thin routing logic:

```rust
impl NamespaceStateMachine {
    /// Pure state transition. No I/O, no async, no side effects.
    pub fn step(&mut self, input: NamespaceInput) -> NamespaceOutput {
        let mut out = NamespaceOutput::default();

        // 1. Route input to the appropriate sub-SM(s).
        // 2. Collect sub-SM outputs.
        // 3. Forward internal signals (e.g. ServiceOutput::DemandUp
        //    becomes WorkloadInput::DemandUp).
        // 4. Forward WorkloadOutput::BecameReady to all services
        //    mapped to that workload.
        // 5. Broadcast service commands to all active workers.
        // 6. Collect worker commands, timer ops, events
        //    into NamespaceOutput.

        out
    }
}
```

### Async Shell

The async runtime is a thin shell that dispatches inputs and sends outputs:

```rust
struct OrchestratorShell {
    orchestrator: Orchestrator,
    workers: HashMap<WorkerId, WorkerHandle>,
    clients: HashMap<ClientId, ClientSender>,
    msg_tx: mpsc::UnboundedSender<ShellMsg>,
    msg_rx: mpsc::UnboundedReceiver<ShellMsg>,
    timer_handles: HashMap<TimerKey, tokio::task::JoinHandle<()>>,
    timer_ns: HashMap<TimerKey, NamespaceId>,
    // ... log/event subscriber management
}
```

The shell:
- Manages tokio timers (spawns/aborts for each timer key from SM output)
- Routes worker protocol events → SM inputs
- Routes SM outputs → worker commands (via worker protocol writers)
- Manages client request/response matching via oneshot channels
- Buffers and distributes log streams to subscribers
- Distributes SM events to subscribers for gRPC streaming

### Why Pure

- **Deterministic unit tests**: Feed a sequence of inputs, assert state and outputs.
- **Property-based testing**: Generate random input sequences, check invariants after every step.
- **Fuzz testing**: Coverage-guided fuzzer drives the step function, finds edge cases.
- **Model checking**: Stateright can exhaustively explore all interleavings.
- **Reasoning**: No hidden state changes from async timing, channel backpressure, etc.
- **Per-namespace isolation**: Each namespace state machine is independently testable.
- **Sub-SM isolation**: Workload and service state machines have tiny state spaces (~7 and ~4 states respectively), enabling exhaustive model checking that would be intractable on the monolithic design.

---

## Data Model

### Namespace Spec (Desired State)

The namespace spec is the declarative description of what should exist. Frontends (compose, k8s-lite) produce this; the orchestrator reconciles toward it.

Workloads and services are both **top-level** in the spec. A workload describes what to run — when scheduled, it becomes a pod (a microVM that can host multiple containers). A service is a network entity that NATs to its backing pod, referenced via `workload_id`. This binding is mutable — changing a service's `workload_id` retargets it. Multiple services can share a single workload.

```rust
struct NamespaceSpec {
    network: NetworkConfig,
    workloads: HashMap<WorkloadId, WorkloadSpec>,
    services: HashMap<ServiceId, ServiceSpec>,
}

struct WorkloadSpec {
    containers: Vec<ContainerSpec>,
    network: PodNetworkConfig,             // pod IP, MAC (assigned by orchestrator)
    /// If true, suspend the pod instead of stopping it when demand drops to zero.
    /// Enables fast resume from snapshot on re-activation.
    suspend_on_idle: bool,
}

struct ContainerSpec {
    name: String,
    image: String,
    config: ContainerConfig,
}

struct ServiceSpec {
    workload_id: WorkloadId,               // which workload backs this service
    ip: Ipv4Addr,                          // service IP (assigned by orchestrator)
    mac: [u8; 6],                          // service MAC (assigned by orchestrator)
    policy: ServicePolicy,
    activation: Option<ActivationSpec>,    // None = always-on
    // expose: Vec<ExposeSpec>,            // future
}

struct ActivationSpec {
    idle_timeout: Duration,                // orchestrator-side idle timer
}
```

### Namespace State (Actual State)

```rust
enum NamespaceStatus {
    /// Waiting for initial worker assignment and CreateNamespace ack.
    Creating,
    /// Fabric is up, services are being reconciled.
    Active,
    /// DestroyNamespace sent to all workers, waiting for cleanup.
    Destroying,
}

/// Per-worker state within a namespace. A namespace's fabric can span
/// multiple workers (e.g. during splice).
struct NamespaceWorkerState {
    /// Status of the fabric segment on this worker.
    fabric_status: FabricStatus,
    /// Pods hosted on this worker for this namespace.
    pods: HashSet<PodId>,
}

enum FabricStatus {
    /// CreateNamespace sent, waiting for NamespaceCreated.
    Creating,
    /// Fabric segment is up and ready for pods.
    Active,
    /// DestroyNamespace sent, waiting for cleanup.
    Destroying,
}

/// Tracks pod-to-workload and pod-to-worker associations at the namespace level.
struct PodInfo {
    workload_id: WorkloadId,
    worker_id: WorkerId,
}
```

### Worker Events

The orchestrator defines its own view of worker events, separate from the wire protocol. The namespace coordinator unpacks these and routes specific events to the appropriate sub-SM:

```rust
enum WorkerEvent {
    NamespaceCreated,
    NamespaceFailed { error: String },
    NamespaceDestroyed,
    PodRunning { pod_id: PodId },
    PodExited { pod_id: PodId, exit_code: i32 },
    PodFailed { pod_id: PodId, error: String },
    PodSuspended { pod_id: PodId, snapshot_id: SnapshotId },
    PodSuspendFailed { pod_id: PodId, error: String },
    ServiceActivation { service_id: ServiceId },
    ServiceBackendNeed { service_id: ServiceId, need: BackendNeed },
}
```

The coordinator maps `PodRunning`, `PodExited`, `PodFailed` to `WorkloadInput::PodRunning` / `WorkloadInput::PodGone`. It maps `PodSuspended` and `PodSuspendFailed` to the corresponding workload inputs. `ServiceActivation` and `ServiceBackendNeed` are routed to the appropriate service SM.

### BackendNeed

`BackendNeed` is a signal from the worker's protocol activator indicating the current traffic level for a service. The orchestrator uses this to drive idle timeout and scale-down decisions.

```rust
enum BackendNeed {
    /// No meaningful traffic. The backend may be released / scaled to zero.
    None,
    /// Pulse signal: meaningful traffic detected (e.g. TCP SYN). The
    /// orchestrator should ensure a backend is running. If no further
    /// Traffic or Active signal arrives within the idle timeout, the
    /// orchestrator may release the backend.
    Traffic,
    /// Level signal: active sessions require a backend (e.g. open H2
    /// streams). The backend must stay up as long as this is asserted.
    /// Cleared when the last active session ends.
    Active,
}
```

The worker reports `BackendNeed` via two distinct events:
- **`ServiceActivation`** — first traffic hits a service with no backend at all. Triggers pod launch.
- **`ServiceBackendNeed`** — ongoing signal from the protocol activator about traffic level. Drives idle timeout reset/scale-down while a backend exists.

### Workload State Machine

Each workload manages the pod lifecycle independently, driven by demand signals from services. The workload supports an optional **suspend-on-idle** mode where instead of stopping the pod when demand drops to zero, the pod is suspended to a snapshot and can be resumed quickly on re-activation.

```
                    +---------------+
                    |  Dormant      |  no services need this workload
                    +-------+-------+
                            | DemandUp (demand_count 0 -> 1)
                            v
               +------------------------------+
               |  WaitingForCapacity          |
               |  emits PodRequest            |
               +------------+-----------------+
                            | LaunchPod (from outer layer)
                            v
                  +-------------------------+
                  |  Launching              |
                  |  pod starting up        |
                  |  (60s launch timeout)   |
                  +------------+------------+
                               | PodRunning
                               | -> emit BecameReady
                               v
                  +-------------------------+
     DemandDown   |  Running               |  PodGone / WorkerLost
     (demand_count|  pod running, demand>0 |  -> emit BecameUnready
      -> 0)       +------------+------------+  -> WaitingForCapacity
                  |                              (if demand > 0)
                  |                              or Dormant
                  v (if suspend_on_idle)
                  +-------------------------+
                  |  Suspending             |
                  |  SuspendPod sent        |
                  |  (30s suspend timeout)  |
                  +------------+------------+
                               | PodSuspended
                               v
                  +-------------------------+
     DemandUp     |  Suspended              |
     (demand -> 1)|  snapshot on worker     |
     -> emit      +------------+------------+
        ResumeRequest          | ResumePod (from outer layer)
                               v
                  +-------------------------+
                  |  Resuming               |
                  |  RestorePod sent        |
                  |  (60s resume timeout)   |
                  +------------+------------+
                               | PodRunning
                               | -> delete snapshot
                               | -> emit BecameReady
                               v
                  +-------------------------+
                  |  Running                |
                  +-------------------------+

Without suspend_on_idle:
  DemandDown (demand -> 0) -> StopPod -> Dormant (emit BecameUnready)
```

The workload doesn't know about activation, idle timeouts, service routing, or worker selection — it only knows whether any service needs it running (demand_count > 0). Pod ID generation and worker selection are the outer layer's responsibility.

```rust
enum WorkloadState {
    /// No services need this workload. No pod running.
    Dormant,
    /// At least one service needs this workload, but no pod assigned yet.
    /// Emits PodRequest. Transitions to Launching when the outer layer
    /// injects LaunchPod with an assigned worker and pod ID.
    WaitingForCapacity,
    /// Pod is starting up. Waiting for PodRunning from worker.
    Launching {
        pod_id: PodId,
        worker_id: WorkerId,
        launch_timeout: TimerKey,
    },
    /// Pod is running. Stays here as long as demand_count > 0.
    Running {
        pod_id: PodId,
        worker_id: WorkerId,
    },
    /// Pod is being suspended. SuspendPod sent, waiting for PodSuspended.
    Suspending {
        pod_id: PodId,
        worker_id: WorkerId,
        snapshot_id: SnapshotId,
        suspend_timeout: TimerKey,
    },
    /// Pod is suspended. Snapshot exists on worker.
    Suspended {
        worker_id: WorkerId,
        snapshot_id: SnapshotId,
    },
    /// Pod is being resumed from snapshot. ResumePod sent, waiting for PodRunning.
    Resuming {
        pod_id: PodId,
        worker_id: WorkerId,
        snapshot_id: SnapshotId,
        resume_timeout: TimerKey,
    },
}
```

Key behaviors:
- **DemandUp while Suspended**: emits `ResumeRequest` (fast path via snapshot restore instead of cold boot).
- **DemandUp while Suspending**: noted via demand_count; on `PodSuspended`, immediately emits `ResumeRequest`.
- **DemandDown while Resuming**: on `PodRunning`, immediately re-suspends or stops (depending on `suspend_on_idle`).
- **PodGone while Resuming**: deletes the (potentially corrupted) snapshot and falls back to cold boot via `PodRequest`.
- **WorkerLost while Suspended**: snapshot is gone with the worker; falls back to `WaitingForCapacity` (cold boot) if demand > 0.

### Service State Machine

Each service manages activation, idle timeout, and backend routing. It signals demand to its workload via the coordinator:

```
                    +---------------+
                    |  Pending      |  spec exists, nothing on worker yet
                    +-------+-------+
                            | CreateService
                            v
               +-------------------------+
    +-------->|  Idle (with activation)  |<-----------------+
    |         |  service entity exists,  |                  |
    |         |  workload not demanded   |                  |
    |         +------------+-------------+                  |
    |                      | ServiceActivation              |
    |                      | -> emit DemandUp               |
    |                      v                                |
    |         +-------------------------+                   |
    |         |  NeedBackend            |                   |
    |         |  waiting for workload   |                   |
    |         |  to become ready        |                   |
    |         +------------+------------+                   |
    |                      | WorkloadReady                  |
    |                      | + UpdateServiceBackend         |
    |                      | + ServiceReady                 |
    |                      v                                |
    |         +-------------------------+   idle timeout    |
    |         |  Active                 |   expired &       |
    |         |  workload running,      +---BackendNeed-----+
    |         |  traffic flowing        |   ::None
    |         +------------+------------+   + emit DemandDown
    |                      |                + UpdateServiceBackend(None)
    |                      | WorkloadUnready
    |                      | (unexpected pod loss)
    +-----------<----------+
      re-activate (back to Idle or NeedBackend)

Without activation (always-on):
  Pending -> NeedBackend (emit DemandUp) -> Active -> (re-activate on loss)
```

Note: `NeedBackend` is typically short-lived — if the workload is already `Running` (shared by another service that already demanded it), the coordinator immediately forwards `WorkloadReady` and the service transitions straight to `Active`.

**Idle timeout lifecycle**: When a service is `Active` and receives `ServiceBackendNeed(None)`, the service starts its idle timer. If `ServiceBackendNeed(Traffic)` or `ServiceBackendNeed(Active)` arrives before the timer fires, the timer is cancelled. If the timer fires while `BackendNeed` is still `None`, the service emits `DemandDown` + `UpdateServiceBackend(None)` and transitions back to `Idle`. If this was the last service demanding the workload (demand_count drops to 0), the workload suspends or stops the pod depending on `suspend_on_idle`.

```rust
enum ServiceState {
    /// Spec exists but service entity hasn't been created on any worker yet.
    Pending,
    /// Service entity exists on worker(s), workload not demanded.
    /// Only valid for services with activation enabled.
    Idle,
    /// Service needs a backend but workload isn't ready yet.
    /// DemandUp has been emitted; waiting for WorkloadReady.
    NeedBackend,
    /// Workload is running, service is routing traffic to it.
    Active {
        pod_id: PodId,
        worker_id: WorkerId,
        backend_need: BackendNeed,
        idle_timer: Option<TimerKey>,
    },
}
```

Note that `pod_id` and `worker_id` in `ServiceState::Active` are cached copies from the `WorkloadReady` signal — the source of truth for pod location is the `WorkloadStateMachine`.

Service commands (`UpdateServiceBackend`, `ServiceReady`) are emitted as `BroadcastWorkerCommand`, meaning they are sent to all active workers in the namespace rather than a specific worker. This ensures all workers have consistent service routing state.

### Coupling Interface

The workload and service sub-SMs communicate through exactly four signals, routed by the coordinator:

| Signal | Direction | Meaning |
|---|---|---|
| `DemandUp` | Service → Workload | A service needs the workload running. Increments `demand_count`. |
| `DemandDown` | Service → Workload | A service no longer needs the workload. Decrements `demand_count`. |
| `BecameReady` | Workload → Service(s) | Pod is running. Carries `pod_id`, `worker_id`, and `ServiceBackend`. |
| `BecameUnready` | Workload → Service(s) | Pod is no longer available (exited, failed, worker lost, suspending). |

The coordinator maintains `service_workload: HashMap<ServiceId, WorkloadId>` and fans out workload signals to all services mapped to that workload.

### Worker State

Worker state is tracked at the outer layer (not per-namespace):

```rust
struct WorkerState {
    capabilities: WorkerCapabilities,
    /// Namespaces this worker is assigned to.
    namespaces: HashSet<NamespaceId>,
    /// WireGuard configuration, if the worker supports it.
    wg_config: Option<WorkerWgConfig>,
}

struct WorkerCapabilities {
    max_pods: u32,
    available_memory_mb: u64,
    /// Worker's public endpoint (IP or hostname) for WireGuard connections.
    public_endpoint: String,
}

struct WorkerWgConfig {
    listen_port: u16,
    public_key: [u8; 32],
}
```

---

## Timers

The pure state machine needs a way to express "fire this timer in N seconds" without real time. Timers use **semantic keys** — each timer is identified by its purpose, not an opaque ID.

```rust
enum TimerKey {
    /// Idle timeout for a service with activation.
    /// Owned by ServiceStateMachine.
    IdleTimeout { service_id: ServiceId },
    /// Launch timeout — pod took too long to start (60s).
    /// Owned by WorkloadStateMachine.
    LaunchTimeout { workload_id: WorkloadId, pod_id: PodId },
    /// Suspend timeout — pod took too long to suspend (30s).
    /// Owned by WorkloadStateMachine.
    SuspendTimeout { workload_id: WorkloadId, pod_id: PodId },
    /// Resume timeout — pod took too long to resume from snapshot (60s).
    /// Owned by WorkloadStateMachine.
    ResumeTimeout { workload_id: WorkloadId, pod_id: PodId },
}
```

Each timer is owned by a specific sub-SM. The coordinator routes `TimerFired` to the owning sub-SM based on the `TimerKey` variant. Setting a new timer with the same key implicitly cancels the previous one. The `NamespaceOutput` has both `timers_set` and `timers_cancel` for explicit lifecycle management.

### Timeout Constants

| Timer | Duration | Purpose |
|---|---|---|
| `LaunchTimeout` | 60s | Pod failed to start — kill and retry or go dormant |
| `SuspendTimeout` | 30s | Suspend didn't complete — force-kill pod |
| `ResumeTimeout` | 60s | Resume didn't complete — kill pod, delete snapshot, retry |
| `IdleTimeout` | configurable (default 30s) | No traffic — scale down to idle |

### Stale Timer Handling

Timer fires are treated as **hints, not commands**. Each sub-SM checks whether its current state still warrants the timer's action:

- `IdleTimeout` fires but service is already `Idle` → no-op (ServiceStateMachine).
- `IdleTimeout` fires but `backend_need` is `Active` → no-op (timer should have been cancelled, but this is a safe fallback).
- `LaunchTimeout` fires but workload is already `Running` → no-op (WorkloadStateMachine).
- `SuspendTimeout` fires but workload is already `Suspended` → no-op.
- `ResumeTimeout` fires but workload is already `Running` → no-op.

This makes the system naturally tolerant of races between timer fires and state transitions. The semantic key structure also makes cleanup easy — destroying a namespace cancels all timers with keys that belong to it.

### Async Shell Timer Integration

The async shell maintains a mapping from `TimerKey` to a `tokio::task::JoinHandle`. When the output contains `timers_set`, it spawns a new sleep task (aborting any existing one for the same key). When the sleep completes, it sends `NamespaceInput::TimerFired { timer_key }` to the state machine. Timer cancellation aborts the join handle.

---

## Event Streaming

The state machine emits structured events during state transitions for observability. These are separate from the command outputs — they describe what happened, not what to do.

```rust
enum SmNamespaceEvent {
    Workload { workload_id: WorkloadId, event: SmWorkloadEvent },
    Service { service_id: ServiceId, workload_id: WorkloadId, event: SmServiceEvent },
}

enum SmWorkloadEvent {
    DemandChanged { demanding_services: u32 },
    PodLaunching { pod_id: PodId, worker_id: WorkerId },
    PodRunning { pod_id: PodId, worker_id: WorkerId },
    PodStopped { exit_code: i32 },
    PodFailed { reason: String },
    PodSuspending { pod_id: PodId, worker_id: WorkerId },
    PodSuspended { worker_id: WorkerId, snapshot_id: SnapshotId },
    PodSuspendFailed { reason: String },
    PodResuming { pod_id: PodId, worker_id: WorkerId },
}

enum SmServiceEvent {
    Activated { trigger: ServiceActivationTrigger },
    BackendReady,
    IdleTimerStarted { timeout: Duration },
    IdleTimerCancelled { reason: IdleTimerCancelReason },
    IdleTimeoutFired,
    Deactivated { reason: ServiceDeactivationReason },
}
```

Events are collected in `NamespaceOutput::events` and forwarded to the async shell. The shell distributes them to gRPC `StreamEvents` subscribers. This enables real-time monitoring of namespace lifecycle without polling.

---

## Developer Network Access (WireGuard)

The orchestrator supports connecting developer machines directly to a namespace's network via WireGuard. This enables developers to reach services by their internal IPs from their local machine.

### WireGuard Peer Manager

Each namespace has a `WireGuardPeerManager` that allocates IPs from the top of the namespace's subnet downward:

```rust
struct WireGuardPeerManager {
    peers: HashMap<[u8; 32], WgPeerInfo>,  // client_public_key -> info
    next_host_offset: u16,                 // IP allocation counter
    subnet: Ipv4Addr,
    prefix_len: u8,
}
```

### Connect Flow

1. Client sends `Connect { namespace_id, client_public_key }` (X25519 public key).
2. Outer layer looks up the namespace's active worker and its WireGuard config.
3. Routes to namespace SM as `NamespaceInput::Connect { ..., worker_wg_public_key, worker_endpoint }`.
4. Namespace's `WireGuardPeerManager` allocates a client IP (idempotent by public key).
5. Emits `AddWireGuardPeer` worker command and returns `ConnectResult` with server public key, endpoint, client IP, and subnet.
6. Client configures local WireGuard interface with this info.

### Disconnect Flow

1. Client sends `Disconnect { namespace_id, client_public_key }`.
2. `WireGuardPeerManager` removes the peer and emits `RemoveWireGuardPeer` worker command.

---

## Worker Capacity Management

The orchestrator currently uses a simple worker scheduling model. Pod scheduling is handled at the outer layer:

1. When a namespace emits a `PodRequest`, the outer layer calls `select_worker_for_pod()` which picks the first worker with an active fabric segment for that namespace.
2. If no worker is available, the workload stays in `WaitingForCapacity`.
3. When a new worker connects, the outer layer scans all namespaces for workloads in `WaitingForCapacity` and schedules them.

Resume requests are simpler — the snapshot is on a specific worker, so the outer layer just generates a new pod ID and injects `ResumePod` targeting that worker.

### Future: Capacity Manager

A more sophisticated capacity manager is planned:

```rust
struct CapacityManager {
    headroom_policy: HeadroomPolicy,
    pending_workers: Vec<ProvisioningWorker>,
    waiting: Vec<(NamespaceId, CapacityRequest)>,
}
```

This would add proactive worker provisioning (keep spare capacity) and reactive provisioning (provision when blocked). See the "Open Design Questions" section.

---

## Reconciliation

The orchestrator is a **level-triggered controller**. On every input, it can re-evaluate the desired vs actual state for affected resources and emit commands to close the gap. This makes it naturally idempotent and resilient to missed events.

The coordinator routes inputs to the appropriate sub-SM and forwards internal signals between them. The forwarding logic handles the full chain: a `ServiceOutput::DemandUp` gets routed to the workload SM, which may emit `PodRequest` (bubbled up to the outer layer) or `BecameReady` (fanned out to all services on the workload).

### Registry Sync

Rather than emitting individual service update commands per service, the namespace broadcasts a full **RegistrySync** — the complete set of service entries (IP, MAC, backend info) — to all active workers in the namespace. This is emitted on key state transitions:

- Namespace becomes Active (initial reconciliation)
- A service backend changes (pod ready, backend cleared)
- Worker loss (surviving workers get updated registry)

This approach is simpler and naturally idempotent — workers always converge to the correct state regardless of missed individual updates.

---

## Client Protocol

The orchestrator exposes a control API over gRPC (tonic). The gRPC layer translates between protobuf messages and the SM's internal types.

### Commands (Client -> Orchestrator)

```rust
enum ClientCommand {
    // Namespace lifecycle
    CreateNamespace { namespace_id: NamespaceId, spec: NamespaceSpec },
    UpdateNamespace { namespace_id: NamespaceId, spec: NamespaceSpec },
    DeleteNamespace { namespace_id: NamespaceId },
    GetNamespaceStatus { namespace_id: NamespaceId },
    ListNamespaces,

    // Splice: route a workload to a local worker (planned)
    Splice { namespace_id: NamespaceId, workload_id: WorkloadId, worker_id: WorkerId },
    Unsplice { namespace_id: NamespaceId, workload_id: WorkloadId },

    // Namespace cloning
    CloneNamespace {
        source_namespace_id: NamespaceId,
        target_namespace_id: NamespaceId,
    },

    // Worker/pod queries
    ListWorkers,
    GetWorker { worker_id: WorkerId },
    ListPods { namespace_id: NamespaceId },

    // Observability
    StreamLogs { namespace_id: NamespaceId, service_id: Option<ServiceId> },

    // Developer network access
    Connect { namespace_id: NamespaceId, client_public_key: [u8; 32] },
    Disconnect { namespace_id: NamespaceId, client_public_key: [u8; 32] },
}
```

### Events (Orchestrator -> Client)

```rust
enum ClientEvent {
    NamespaceStatus { namespace_id: NamespaceId, status: NamespaceStatusReport },
    NamespaceList { namespaces: Vec<NamespaceStatusReport> },
    WorkerList { workers: Vec<WorkerStatusReport> },
    WorkerStatus { worker: WorkerStatusReport },
    PodList { pods: Vec<PodStatusReport> },
    LogChunk { namespace_id: NamespaceId, service_id: ServiceId, data: Vec<u8> },
    Error { message: String },
    Ok,
    ConnectResult {
        server_public_key: [u8; 32],
        endpoint: String,
        client_ip: String,
        subnet: String,
    },
}

struct NamespaceStatusReport {
    namespace_id: NamespaceId,
    status: NamespaceStatus,
    services: HashMap<ServiceId, ServiceStatusReport>,
}

struct ServiceStatusReport {
    service_state: String,              // "pending", "idle", "need_backend", "active"
    workload_id: WorkloadId,
    workload_state: String,             // "dormant", "waiting_for_capacity", "launching",
                                        // "running", "suspending", "suspended", "resuming"
    pod_id: Option<PodId>,
    worker_id: Option<WorkerId>,
    backend_need: Option<BackendNeed>,
    activation_enabled: bool,
}

struct WorkerStatusReport {
    worker_id: WorkerId,
    max_pods: u32,
    available_memory_mb: u64,
    active_pods: u32,
}

struct PodStatusReport {
    pod_id: PodId,
    workload_id: WorkloadId,
    worker_id: WorkerId,
    ip: String,
    mac: String,
    state: PodStatus,  // Launching, Running, Suspending, Suspended, Resuming
}
```

### gRPC Service

The gRPC layer (tonic) defines unary and streaming RPCs:

**Unary RPCs**: `CreateNamespace`, `UpdateNamespace`, `DeleteNamespace`, `GetNamespaceStatus`, `ListNamespaces`, `Splice`, `Unsplice`, `CloneNamespace`, `ListWorkers`, `GetWorker`, `ListPods`, `ConnectNetwork`, `DisconnectNetwork`.

**Streaming RPCs**: `StreamLogs` (workload log output), `StreamEvents` (namespace state machine events).

All unary RPCs use a pattern where the gRPC handler creates a temporary client connection, sends a command, and waits for the response via a oneshot channel. Streaming RPCs subscribe through the shell handle and receive events via unbounded channels.

### Client Sessions

Clients maintain persistent connections for streaming (logs, events). The outer layer tracks connected clients via `ClientConnected`/`ClientDisconnected` inputs. When a client disconnects, any active log subscriptions for that client are cleaned up. Commands are idempotent so clients can reconnect and retry.

---

## Splice (Planned)

Splice allows a user to inject a local pod into a remote namespace, replacing a workload's backend with a locally-running instance. This is the primary developer experience feature — edit code locally, have it receive real traffic from the staging environment.

Splice/unsplice handlers exist in the codebase but are currently no-ops (return Ok without modifying state). The design is outlined below for future implementation.

### State Model

Splice operates at the **workload level**, not the service level. The pod belongs to the workload, and moving it automatically updates all services that share that workload.

### Planned Flow

1. User runs a local distvirt worker on their machine, connects to orchestrator.
2. User sends `Splice { namespace_id, workload_id, local_worker_id }`.
3. Namespace coordinator:
   - Adds the local worker to `self.workers` if not already present.
   - Sends `CreateNamespace` to the local worker (if first time this worker participates in this namespace).
   - Waits for `NamespaceCreated` from the local worker.
   - Stops the existing pod on the cloud worker (if running).
   - Updates fabric routes between workers.
   - Launches the pod on the local worker instead.
   - On `PodRunning` → emits `BecameReady` with the new worker_id.
4. From the perspective of other services, nothing changed — same service IP/MAC, traffic just routes through the tunnel now.

### Requirements

- **Multi-worker fabric tunneling**: route destinations need a real transport (likely a yamux stream between workers, or orchestrator-mediated relay).
- **Worker-to-worker or worker-to-orchestrator-to-worker frame forwarding**: simplest initial approach is orchestrator-mediated relay. Direct worker-to-worker tunnels are an optimization.

---

## Namespace Clones

Clone creates a new namespace from an existing one with an exact copy of the spec. The clone is an independent, isolated copy — namespaces are isolated at the network level, so from the perspective of workloads they see no difference between the clone and the original environment. IPs, MACs, and all network config are identical between source and clone since each namespace has its own isolated fabric.

The clone preserves the source's activation policies as-is: always-on services spin up immediately in the clone, activation-enabled services start idle and activate on demand.

Clones are independent — once created, the clone's spec is decoupled from the source. Updating or destroying the source has no effect on the clone.

Currently returns "not yet implemented" error. When implemented, `NamespaceStatus` will gain a `Cloning` variant.

### Planned Flow

1. Client sends `CloneNamespace { source, target }`.
2. Outer layer:
   - Sets source namespace status to `Cloning { pending_destroy: false }`.
   - Copies `NamespaceSpec` from source namespace's state machine (exact copy, including network config).
   - Creates a new `NamespaceStateMachine` for the target with the copied spec.
   - Returns source namespace to `Active` (or transitions to `Destroying` if `pending_destroy` was set during the clone).
3. The target namespace state machine proceeds normally — creates its own isolated fabric, reconciles services. Always-on services immediately emit `DemandUp`; activation-enabled services start in `Idle`.

### Clone + Destroy Interaction

If a `Delete` command arrives while the namespace is in `Cloning` state:
- The state machine sets `pending_destroy: true` instead of immediately destroying.
- When the clone operation completes, the outer layer checks `pending_destroy` and transitions to `Destroying`.
- This avoids races where a clone reads partially-destroyed state.

### Snapshot-Accelerated Clones (Future)

For faster clone activation, the orchestrator can snapshot source workload pods and restore them in the clone. This builds on the existing suspend/resume infrastructure:

- Instead of cold-booting from the image, restore from the source workload's pod snapshot.
- Firecracker snapshot restore is ~5-10ms vs ~100ms+ cold boot.
- Network config is identical, so the restored VM needs no reconfiguration.

### Cost Model

Without snapshots: a clone is just metadata + service entities. Essentially free.
With snapshots: clone creation triggers snapshot of each source workload's pod (one-time cost), then each activation in the clone is a fast restore.

---

## Failure Model

### Worker Disconnect

When a worker disconnects, the outer layer fans out a `WorkerLost` event to every namespace state machine that had the worker in its `workers` map. The coordinator routes this through the sub-SM layers:

1. **Coordinator** forwards `WorkerLost` to all workload SMs with pods on that worker.
2. **WorkloadStateMachine** handles it based on current state:
   - `Running` → emits `BecameUnready`, transitions to `WaitingForCapacity` (if demand > 0) or `Dormant`.
   - `Launching` → cancels launch timeout, emits `BecameUnready`, transitions based on demand.
   - `Suspending` → cancels suspend timeout, transitions based on demand (BecameUnready was already emitted on entry).
   - `Suspended` → snapshot is lost with the worker, transitions to `WaitingForCapacity` (cold boot) if demand > 0, else `Dormant`.
   - `Resuming` → cancels resume timeout, emits `BecameUnready`, transitions based on demand.
3. **Coordinator** forwards `BecameUnready` to all services mapped to affected workloads.
4. **ServiceStateMachine** receives `WorkloadUnready`:
   - If activation-enabled → transition to `Idle` (ready to re-activate on next traffic), emit `DemandDown`.
   - If always-on → stay in `NeedBackend` (will get `WorkloadReady` when workload re-launches).
5. Coordinator removes the worker from its `workers` map.
6. Cancels any timers associated with pods on that worker.
7. No commands are sent to the disconnected worker.

### Namespace Deletion

Namespace deletion is a **stateful teardown**, not fire-and-forget. This matters because a namespace can span multiple workers (especially with splice), and each worker must clean up its fabric segment and pods.

1. Client sends `Delete { client_id }`.
2. Namespace transitions to `Destroying`:
   - Cancels all timers (idle timeouts, launch timeouts, suspend timeouts, resume timeouts).
   - Stops accepting new inputs (activation events, spec updates).
   - Emits `DestroyNamespace` to every worker in `self.workers`.
3. As each worker confirms destruction (or disconnects), the namespace removes it from `self.workers`.
4. When `self.workers` is empty, the namespace sets `destroyed: true` in its output.
5. The outer layer removes the namespace from its map.

While in `Destroying`, the namespace ignores all inputs except `WorkerEvent` (to process destruction confirmations) and `WorkerLost` (to remove disconnected workers). This ensures cleanup completes even if workers are slow to respond.

### Orchestrator Death

When the orchestrator dies, **all cluster state is lost**. Workers detect the orchestrator disconnect and immediately tear down all their resources (namespaces, pods, fabric segments). Workers then enter a reconnect loop, attempting to re-establish a connection to the orchestrator.

On orchestrator restart, it starts with a clean slate. Workers reconnect and register as fresh workers with no existing state. Namespaces must be re-created by clients.

This is a deliberate simplicity choice. The orchestrator is the single source of truth. There is no state persistence, no WAL, no recovery protocol. This avoids a large class of consistency problems and keeps the system simple.

### Service Creation Failure

The workload and service sub-SMs define failure transitions regardless of whether the worker protocol currently sends such events. The workload SM handles `PodGone` gracefully — emitting `BecameUnready` and transitioning to `Dormant` or `WaitingForCapacity` depending on demand. The service SM handles `WorkloadUnready` by transitioning back to `Idle` or `NeedBackend`. This ensures the orchestrator is ready when the worker protocol adds failure events, and makes each sub-SM robust under model checking (stateright can inject failures at any point).

---

## Namespace Spec Frontends

Frontends run client-side (in the CLI) and translate their format into `NamespaceSpec`.

### Compose Frontend

Parses `docker-compose.yml`, maps to `NamespaceSpec`. Each compose service produces **both** a `WorkloadSpec` and a `ServiceSpec` with matching names. The compose frontend synthesizes both entities — the orchestrator sees the same flat workloads + services model regardless of the frontend.

| Compose concept | NamespaceSpec mapping |
|---|---|
| `services.<name>` | Creates `WorkloadSpec` with name `<name>` and `ServiceSpec` with name `<name>` pointing at `workload_id: <name>` |
| `services.<name>.image` | `WorkloadSpec.containers[0].image` |
| `services.<name>.command` | `WorkloadSpec.containers[0].config.entrypoint/args` |
| `services.<name>.environment` | `WorkloadSpec.containers[0].config.env` |
| `services.<name>.ports` | `ServiceSpec.expose` |
| `services.<name>.depends_on` | Launch ordering hint (not modeled in spec, handled by orchestrator) |
| Network assignment | Orchestrator auto-assigns IPs/MACs from namespace subnet (service IPs and pod IPs separately) |

### K8s-Lite Frontend (Future)

Subset of Kubernetes resources. Like the compose frontend, the k8s-lite frontend synthesizes both `WorkloadSpec` and `ServiceSpec` entries from k8s resources:

| K8s resource | NamespaceSpec mapping |
|---|---|
| `Deployment` | `WorkloadSpec` (containers from pod template) + `ServiceSpec` if a matching k8s `Service` exists |
| `Service` (ClusterIP) | `ServiceSpec.network` (virtual IP) with `workload_id` pointing at the backing `Deployment`'s workload |
| `ConfigMap` | Injected into `WorkloadSpec.containers[].config.env` or volume mount |
| `Ingress` | `ServiceSpec.expose` |

Only enough to cover the common case. Not a full k8s implementation.

---

## Testing Strategy

The workload/service split is designed primarily to improve testability. Each sub-SM has a tiny state space that can be model-checked exhaustively, while the coordinator is thin routing logic testable with property-based tests.

### Key Payoff: Independent Model Checking

The monolithic design had state space O(states^N) — all services interleaved. The split gives O(states × N):

- **WorkloadStateMachine**: ~7 states (Dormant, WaitingForCapacity, Launching, Running, Suspending, Suspended, Resuming). With demand_count, the full state space is still small enough for exhaustive stateright exploration.
- **ServiceStateMachine**: ~4 states (Pending, Idle, NeedBackend, Active) × 3 backend_need values. Also tiny.
- **Coordinator**: Pure routing logic (no business decisions). Testable with proptest — generate random signal sequences, verify signals are forwarded correctly.

Each sub-SM can be checked in isolation with a mock environment, then composition correctness is verified at the coordinator level.

### Layer 1: Deterministic Unit Tests

Direct step-by-step tests of sub-SMs and the full coordinator. Feed specific input sequences, assert exact state and outputs.

```rust
#[test]
fn workload_demand_lifecycle() {
    let mut wl = WorkloadStateMachine::new(wl_id(), false);
    assert!(matches!(wl.state, WorkloadState::Dormant));

    // First service demands the workload
    let out = wl.step(WorkloadInput::DemandUp, &ns_id());
    assert!(matches!(wl.state, WorkloadState::WaitingForCapacity));

    // Outer layer assigns pod
    let out = wl.step(WorkloadInput::LaunchPod {
        worker_id: w1(), pod_id: p1(),
    }, &ns_id());
    assert!(matches!(wl.state, WorkloadState::Launching { .. }));

    // Pod starts running
    let out = wl.step(WorkloadInput::PodRunning { pod_id: p1() }, &ns_id());
    assert!(matches!(wl.state, WorkloadState::Running { .. }));
    assert!(out.iter().any(|o| matches!(o, WorkloadOutput::BecameReady { .. })));

    // Last service drops demand
    let out = wl.step(WorkloadInput::DemandDown, &ns_id());
    assert!(matches!(wl.state, WorkloadState::Dormant));
    assert!(out.iter().any(|o| matches!(o, WorkloadOutput::BecameUnready)));
}

#[test]
fn workload_suspend_resume_lifecycle() {
    let mut wl = WorkloadStateMachine::new(wl_id(), true); // suspend_on_idle
    // ... get to Running state ...

    // Last service drops demand -> suspend instead of stop
    let out = wl.step(WorkloadInput::DemandDown, &ns_id());
    assert!(matches!(wl.state, WorkloadState::Suspending { .. }));
    assert!(out.iter().any(|o| matches!(o, WorkloadOutput::BecameUnready)));

    // Worker confirms suspend
    let out = wl.step(WorkloadInput::PodSuspended {
        pod_id: p1(), snapshot_id: snap1(),
    }, &ns_id());
    assert!(matches!(wl.state, WorkloadState::Suspended { .. }));

    // New demand -> resume from snapshot
    let out = wl.step(WorkloadInput::DemandUp, &ns_id());
    assert!(out.iter().any(|o| matches!(o, WorkloadOutput::ResumeRequest { .. })));
}
```

### Layer 2: Property-Based Testing (proptest)

Generate random sequences of valid inputs and check invariants hold after every step. Tests are written at three levels:

**Sub-SM level** — test each sub-SM in isolation:

```rust
proptest! {
    #[test]
    fn workload_invariants(inputs in vec(arb_workload_input(), 0..100)) {
        let mut wl = WorkloadStateMachine::new(wl_id(), false);
        for input in inputs {
            let outputs = wl.step(input, &ns_id());
            // demand_count never goes negative
            // BecameReady only emitted when transitioning to Running
            // BecameUnready only emitted when leaving Running/Launching/Resuming
            // PodRequest only emitted when in WaitingForCapacity
            // ResumeRequest only emitted when in Suspended
            check_workload_invariants(&wl, &outputs);
        }
    }
}
```

**Coordinator level** — test signal routing correctness:

```rust
proptest! {
    #[test]
    fn coordinator_routing(inputs in vec(arb_namespace_input(), 0..200)) {
        let mut ns = NamespaceStateMachine::new(ns_id(), arb_spec());
        for input in inputs {
            let output = ns.step(input);
            check_coordinator_invariants(&ns, &output);
        }
    }
}

fn check_coordinator_invariants(ns: &NamespaceStateMachine, output: &NamespaceOutput) {
    // No commands sent to workers not in our workers map
    // Every service's workload_id points to a valid workload
    // A service in Active state has a workload in Running state
    // DemandUp/DemandDown are always balanced (demand_count never goes negative)
    // BecameReady/BecameUnready are forwarded to all services on the workload
    // Idle timer is set when BackendNeed::None is received for an active service with activation
    // Idle timer is cancelled when BackendNeed::Traffic or BackendNeed::Active arrives
    // WorkerLost triggers BecameUnready for all workloads on that worker
    // Stale timer fires are no-ops
    // A workload in WaitingForCapacity always has a corresponding PodRequest output
}
```

### Layer 3: Fuzz Testing (cargo-fuzz)

Coverage-guided fuzzing of each sub-SM's step function. Separate fuzz targets for workload, service, and coordinator.

### Layer 4: Model Checking (Stateright)

This is where the split pays off most. Each sub-SM is small enough for exhaustive exploration.

State space for WorkloadStateMachine: ~7 states × demand_count range. Still exhaustible despite the suspend/resume additions.

State space for ServiceStateMachine: ~4 states × 3 backend_need × idle_timer presence. Easily exhaustible.

The coordinator is thin routing logic — no business decisions, just signal forwarding. Composition correctness reduces to:
1. Each sub-SM is correct in isolation (verified by stateright).
2. The coordinator routes signals correctly (verified by proptest).
3. The `service_workload` mapping is maintained correctly (verified by proptest invariants).

### Integration / E2E Tests

The existing e2e test infrastructure (see `distvirt-worker/tests/e2e.rs`) covers the worker side. Orchestrator e2e tests would:
- Spin up an in-process orchestrator + worker (like compose does today).
- Send client commands, verify end-to-end behavior.
- These are slow and non-exhaustive, but verify the async shell + real worker integration.

### Test Priority

1. **Pure step tests**: Write these as you implement each feature. Fast, easy, high coverage. Test sub-SMs individually first, then coordinator integration.
2. **proptest invariants**: Set up the framework early for each sub-SM. Continuously add invariants as you discover them. These find the "I didn't think of that combination" bugs.
3. **Stateright models**: Build for workload SM and service SM independently. The tiny state spaces make exhaustive checking fast (~seconds). This is now practical where the monolithic design was not.
4. **Fuzz harness**: Set up once per sub-SM, run in CI. Finds edge cases proptest misses through coverage guidance.

---

## Open Design Questions

1. **Worker scheduling policy**: `select_worker_for_pod()` currently picks the first active worker. Needs a real strategy: round-robin, least-loaded, locality-aware (splice prefers same region), bin-packing vs spreading.
2. **Fabric tunnel transport**: Orchestrator-mediated relay (simple, higher latency) vs direct worker-to-worker tunnels (complex, lower latency). Start with relay. Required for splice.
3. **Snapshot storage**: Where do VM snapshots live? Currently local to the worker (fast, not portable). Shared storage would enable cross-worker resume but adds complexity.
4. **Spec diffing**: When `UpdateNamespace` changes a service's image, what happens? Rolling update (launch new pod, drain old)? Or hard cut (stop old, launch new)?
5. **Capacity manager**: Currently simple (first available worker). Planned: proactive headroom provisioning, scale-down of idle workers. What headroom policy values work in practice?
6. **Provisioner interface**: Future capacity manager would emit `ProvisionWorker`/`TerminateWorker` outputs. The async shell maps these to infrastructure APIs. What's the right abstraction boundary?
