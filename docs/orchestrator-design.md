# Orchestrator Design

## Overview

The orchestrator is the central control plane for distvirt. It manages workers, drives namespace lifecycle, handles scale-to-zero activation, and exposes a client protocol for CLI/UI control.

The orchestrator is a **pure state machine** at its core. All logic lives in a synchronous `step(input) -> output` function with no I/O. An async shell dispatches inputs from network connections and timers, and sends outputs to workers and clients. This separation is the foundation for testing, model checking, and fuzzing.

---

## Core Architecture

### Two-Layer Design

The orchestrator has two layers:

1. **Outer layer** — routes inputs to the correct namespace state machine, manages worker connections, handles cross-namespace operations (clone, list).
2. **Namespace state machine** — a pure, self-contained state machine for a single namespace. All service lifecycle, activation, splice, and reconciliation logic lives here.

This separation keeps the per-namespace state machine small and independently testable. Cross-namespace interactions are minimal (limited to clones) and handled at the outer layer.

```rust
struct Orchestrator {
    namespaces: HashMap<NamespaceId, NamespaceStateMachine>,
    workers: HashMap<WorkerId, WorkerState>,
    clients: HashSet<ClientId>,
    capacity: CapacityManager,
}
```

The outer layer receives top-level inputs and dispatches to namespace state machines:

```rust
/// Top-level inputs to the orchestrator.
enum OrchestratorInput {
    ClientConnected { client_id: ClientId },
    ClientDisconnected { client_id: ClientId },
    ClientCommand { client_id: ClientId, command: ClientCommand },
    WorkerConnected { worker_id: WorkerId, capabilities: WorkerCapabilities },
    WorkerDisconnected { worker_id: WorkerId },
    NamespaceInput { namespace_id: NamespaceId, input: NamespaceInput },
}
```

Most inputs are routed directly to a namespace. `WorkerDisconnected` fans out a `WorkerLost` event to every namespace that had pods on that worker. `CreateNamespace` instantiates a new state machine. `CloneNamespace` reads the spec from the source and creates a new state machine with a modified copy. `ListNamespaces` reads across all state machines.

### Per-Namespace State Machine — Three-Layer Split

The namespace state machine is split into three layers to keep each piece small and independently testable:

1. **NamespaceStateMachine** (thin coordinator): fabric management, namespace lifecycle, event routing between sub-state-machines.
2. **WorkloadStateMachine**: pod lifecycle (scheduling, launching, running). Driven by demand signals from services.
3. **ServiceStateMachine**: activation, idle timeout, backend routing. Driven by activation events and workload readiness.

Multiple services can share a single workload. The coordinator maintains the mapping and forwards signals between them.

This split reduces the model-checking state space from O(states^N) (monolithic, all services interleaved) to O(states × N) (each sub-SM checked independently).

```rust
struct NamespaceStateMachine {
    spec: NamespaceSpec,
    status: NamespaceStatus,
    workers: HashMap<WorkerId, NamespaceWorkerState>,

    /// Maps each service to its workload. Multiple services can share
    /// a workload (e.g. sidecar pattern, or services with identical pods).
    service_workload: HashMap<ServiceId, WorkloadId>,
    workloads: HashMap<WorkloadId, WorkloadStateMachine>,
    services: HashMap<ServiceId, ServiceStateMachine>,
}

/// Workload sub-state-machine. Manages the pod lifecycle for one workload.
struct WorkloadStateMachine {
    state: WorkloadState,
    spec: WorkloadSpec,
    /// Number of services currently requesting this workload be running.
    demand_count: u32,
}

/// Service sub-state-machine. Manages activation, idle timeout, and
/// backend routing for one service.
struct ServiceStateMachine {
    state: ServiceState,
    spec: ServiceSpec,
    workload_id: WorkloadId,
}
```

The coordinator dispatches external inputs and routes internal signals between sub-SMs:

```rust
/// Inputs to a single namespace state machine.
/// No namespace_id fields — the state machine knows which namespace it is.
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
    /// A new worker is available. Re-reconcile workloads that may be
    /// waiting for capacity.
    CapacityAvailable,
}

struct NamespaceOutput {
    worker_commands: Vec<(WorkerId, WorkerCommand)>,
    client_events: Vec<(ClientId, ClientEvent)>,
    timers_set: Vec<(TimerKey, Duration)>,
    timers_cancel: Vec<TimerKey>,
    /// Signals to the outer layer that this namespace needs worker
    /// capacity it couldn't find. Drives the CapacityManager.
    capacity_requests: Vec<CapacityRequest>,
}

struct CapacityRequest {
    workload_id: WorkloadId,
    memory_mb: u64,
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
    /// Worker capacity became available (retry scheduling).
    CapacityAvailable,
    /// Worker reports pod status change.
    WorkerEvent { worker_id: WorkerId, event: WorkerEvent },
    /// Worker hosting this workload's pod disconnected.
    WorkerLost { worker_id: WorkerId },
    /// Timer fired (launch timeout).
    TimerFired { timer_key: TimerKey },
}

/// Outputs from the workload sub-state-machine.
enum WorkloadOutput {
    /// The workload's pod is now running and reachable.
    BecameReady { pod_id: PodId, worker_id: WorkerId },
    /// The workload's pod is no longer available.
    BecameUnready,
    /// Need worker capacity — forward to outer layer.
    NeedCapacity { memory_mb: u64 },
    /// Worker commands to emit.
    WorkerCommand { worker_id: WorkerId, command: WorkerCommand },
    /// Timer management.
    SetTimer { key: TimerKey, duration: Duration },
    CancelTimer { key: TimerKey },
}

/// Inputs to the service sub-state-machine.
enum ServiceInput {
    /// The service's workload became ready (pod running).
    WorkloadReady { pod_id: PodId, worker_id: WorkerId },
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
    /// Worker commands to emit (UpdateServiceBackend, ServiceReady).
    WorkerCommand { worker_id: WorkerId, command: WorkerCommand },
    /// Timer management.
    SetTimer { key: TimerKey, duration: Duration },
    CancelTimer { key: TimerKey },
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
        // 5. Collect worker commands, timer ops, capacity requests
        //    into NamespaceOutput.

        out
    }
}
```

### Async Shell

The async runtime is a thin shell that dispatches inputs and sends outputs:

```rust
async fn run(mut orch: Orchestrator, /* connections */) {
    loop {
        let input = select! {
            (cid) = client_join_rx.recv() => OrchestratorInput::ClientConnected { cid },
            (cid) = client_leave_rx.recv() => OrchestratorInput::ClientDisconnected { cid },
            (cid, cmd) = client_rx.recv() => OrchestratorInput::ClientCommand { cid, cmd },
            (wid, caps) = worker_join_rx.recv() => OrchestratorInput::WorkerConnected { wid, caps },
            wid = worker_leave_rx.recv() => OrchestratorInput::WorkerDisconnected { wid },
            (nsid, input) = ns_rx.recv() => OrchestratorInput::NamespaceInput { nsid, input },
        };
        let outputs = orch.step(input);
        dispatch(outputs).await;
    }
}
```

### Why Pure

- **Deterministic unit tests**: Feed a sequence of inputs, assert state and outputs.
- **Property-based testing**: Generate random input sequences, check invariants after every step.
- **Fuzz testing**: Coverage-guided fuzzer drives the step function, finds edge cases.
- **Model checking**: Stateright can exhaustively explore all interleavings.
- **Reasoning**: No hidden state changes from async timing, channel backpressure, etc.
- **Per-namespace isolation**: Each namespace state machine is independently testable with a small state space.

---

## Data Model

### Namespace Spec (Desired State)

The namespace spec is the declarative description of what should exist. Frontends (compose, k8s-lite) produce this; the orchestrator reconciles toward it.

```rust
struct NamespaceSpec {
    network: NetworkConfig,
    services: HashMap<ServiceId, ServiceSpec>,
}

struct ServiceSpec {
    image: String,
    container_config: ContainerConfig,
    network: ServiceNetworkConfig,     // ip, mac (assigned by orchestrator)
    activation: Option<ActivationSpec>,
    expose: Vec<ExposeSpec>,
}

struct ActivationSpec {
    activator: ActivatorConfig,        // Tcp { ports, .. } or Http2 { }
    buffer_policy: ServicePolicy,
    idle_timeout: Duration,            // orchestrator-side idle timer
}
```

### Namespace State (Actual State)

```rust
struct NamespaceStateMachine {
    spec: NamespaceSpec,
    status: NamespaceStatus,
    workers: HashMap<WorkerId, NamespaceWorkerState>,
    services: HashMap<ServiceId, ServiceState>,
    pods: HashMap<PodId, PodInfo>,
}

enum NamespaceStatus {
    /// Waiting for initial worker assignment and CreateNamespace ack.
    Creating,
    /// Fabric is up, services are being reconciled.
    Active,
    /// A clone operation is reading from this namespace's spec.
    /// Destroy commands are deferred until cloning completes.
    Cloning { pending_destroy: bool },
    /// DestroyNamespace sent, waiting for cleanup.
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
```

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

### Service State Machine

Each service has an independent lifecycle driven by reconciliation:

```
                         +---------------+
                         |  Pending      |  spec exists, nothing on worker yet
                         +-------+-------+
                                 | CreateService
                                 v
                    +-------------------------+
         +-------->|  Idle (with activation)  |<-----------------+
         |         |  service entity exists,  |                  |
         |         |  no pod running          |                  |
         |         +------------+-------------+                  |
         |                      | ServiceActivation              |
         |                      v                                |
         |      +------------------------------+                 |
         |      |  WaitingForCapacity          |                 |
         |      |  need pod, no worker fits    |                 |
         |      +------------+-----------------+                 |
         |                   | CapacityAvailable                 |
         |                   | (worker found)                    |
         |                   v                                   |
         |         +-------------------------+                   |
         |         |  Launching              |                   |
         |         |  pod starting up        |                   |
         |         +------------+------------+                   |
         |                      | PodRunning                     |
         |                      | + UpdateServiceBackend         |
         |                      | + ServiceReady                 |
         |                      v                                |
         |         +-------------------------+   idle timeout    |
         |         |  Active                 |   expired &       |
         |         |  pod running, traffic   +---BackendNeed-----+
         |         |  flowing                |   ::None
         |         +------------+------------+   + StopPod
         |                      |                + UpdateServiceBackend(None)
         |                      | PodExited / PodFailed
         |                      | (unexpected)
         +-----------<----------+
           restart / re-activate

Without activation (always-on):
  Pending -> WaitingForCapacity (if needed) -> Launching -> Active -> (restart on exit)
```

Note: `WaitingForCapacity` is skipped when a suitable worker is immediately available. The service transitions directly from `Idle`/`Pending` to `Launching` in that case.

**Idle timeout lifecycle**: When a service is `Active` and receives `ServiceBackendNeed(None)`, the orchestrator starts the idle timer. If `ServiceBackendNeed(Traffic)` or `ServiceBackendNeed(Active)` arrives before the timer fires, the timer is cancelled. If the timer fires while `BackendNeed` is still `None`, the service scales down: `StopPod` + `UpdateServiceBackend(None)` transitions it back to `Idle`.

```rust
enum ServiceState {
    /// Spec exists but service entity hasn't been created on any worker yet.
    Pending,
    /// Service entity exists on worker(s), no pod running.
    /// Only valid for services with activation enabled.
    Idle,
    /// Need to launch a pod but no worker has capacity.
    /// Emits a CapacityRequest. Transitions to Launching when
    /// CapacityAvailable arrives and a worker can be selected.
    WaitingForCapacity,
    /// Pod is starting up. Waiting for PodRunning.
    Launching {
        pod_id: PodId,
        worker_id: WorkerId,
        launch_timeout: TimerKey,
    },
    /// Pod is running and serving traffic.
    Active {
        pod_id: PodId,
        worker_id: WorkerId,
        hosting: ServiceHosting,
        backend_need: BackendNeed,
        idle_timer: Option<TimerKey>,
    },
}

/// Tracks whether a service's pod is running on its normal worker
/// or has been spliced to a different worker.
enum ServiceHosting {
    /// Pod runs on the namespace's normal worker.
    Normal,
    /// Pod has been spliced to a different worker.
    /// Stores the original worker so unsplice knows where to put it back.
    Spliced { original_worker_id: WorkerId },
}
```

### Worker State

Worker state is tracked at the outer layer (not per-namespace):

```rust
struct WorkerState {
    capabilities: WorkerCapabilities,
    status: WorkerStatus,
}

enum WorkerStatus {
    Connected,
    // Future: Draining, etc.
}

struct WorkerCapabilities {
    max_pods: u32,
    available_memory_mb: u64,
    // future: location, labels for scheduling
}
```

---

## Timers

The pure state machine needs a way to express "fire this timer in N seconds" without real time. Timers use **semantic keys** — each timer is identified by its purpose, not an opaque ID.

```rust
enum TimerKey {
    /// Idle timeout for a service with activation.
    IdleTimeout { service_id: ServiceId },
    /// Launch timeout — pod took too long to start.
    LaunchTimeout { service_id: ServiceId, pod_id: PodId },
}
```

Setting a new timer with the same key implicitly cancels the previous one. The `NamespaceOutput` has both `timers_set` and `timers_cancel` for explicit lifecycle management.

### Stale Timer Handling

Timer fires are treated as **hints, not commands**. The `step` function checks whether the current state still warrants the timer's action:

- `IdleTimeout` fires but service is already `Idle` → no-op.
- `IdleTimeout` fires but `backend_need` is `Active` → no-op (timer should have been cancelled, but this is a safe fallback).
- `LaunchTimeout` fires but pod is already `Active` → no-op.

This makes the system naturally tolerant of races between timer fires and state transitions. The semantic key structure also makes cleanup easy — destroying a namespace cancels all timers with keys that belong to it.

### Async Shell Timer Integration

The async shell maintains a mapping from `TimerKey` to a `tokio::time::Sleep` future. When the output contains `timers_set`, it creates/replaces the sleep. When a sleep fires, it sends `NamespaceInput::TimerFired { timer_key }` to the state machine.

---

## Worker Capacity Management

The orchestrator manages worker provisioning and scale-down at the outer layer. Namespace state machines don't know about provisioning — they just report when they can't find a worker.

### Capacity Manager

```rust
struct CapacityManager {
    /// Policy for proactive scaling.
    headroom_policy: HeadroomPolicy,
    /// Workers currently being provisioned (not yet connected).
    pending_workers: Vec<ProvisioningWorker>,
    /// Namespaces with services waiting for capacity.
    waiting: Vec<(NamespaceId, CapacityRequest)>,
}

struct HeadroomPolicy {
    /// Always keep at least this many pod slots free across the cluster.
    min_free_slots: u32,
    /// Maximum workers to provision concurrently.
    max_pending: u32,
}

struct ProvisioningWorker {
    request_id: RequestId,
    requested_at: Instant, // tracked by async shell, not the SM
}
```

### Two Triggers

The capacity manager runs on two triggers:

1. **Proactive (headroom)** — after every `step()`, the outer layer checks total cluster free capacity against `min_free_slots`. If headroom is below threshold, it provisions workers *before* anything is blocked. This means workers are often already booting by the time demand arrives.

2. **Reactive (capacity requests)** — when a namespace's `step()` output contains `capacity_requests`, the outer layer queues them and provisions if no pending workers will satisfy the need.

### Worker Connected Flow

When a new `WorkerConnected` arrives (whether from proactive provisioning or an externally-added worker):

1. Outer layer adds the worker to its `workers` map.
2. Drains the `waiting` queue — injects `CapacityAvailable` into each waiting namespace.
3. Those namespaces re-run `reconcile_all_services()`. Services in `WaitingForCapacity` retry `select_worker_for_pod()` — if a worker fits, they proceed to `Launching`. If still nothing fits, they stay parked.

### Provisioning Output

The outer layer emits provisioning commands as orchestrator-level outputs:

```rust
enum OrchestratorOutput {
    // ... existing worker/client dispatch ...
    /// Request a new worker be provisioned.
    ProvisionWorker { request_id: RequestId, requirements: WorkerRequirements },
    /// Terminate an idle worker.
    TerminateWorker { worker_id: WorkerId },
}
```

The async shell maps these to the backing infrastructure (EC2 API, etc.). The provisioner is pluggable — the state machine just says "I need a worker" and "I don't need this worker anymore."

### Scale-Down

When cluster headroom exceeds an upper bound (too many idle workers), the capacity manager can emit `TerminateWorker` for empty workers. The outer layer drains the worker first — moves pods off via the normal reconciliation path, then disconnects and terminates.

### Latency Characteristics

Worker provisioning is slow (e.g. ~30s for an EC2 instance). This latency is invisible to the namespace state machine — it just sees the service go to `WaitingForCapacity`, then eventually `CapacityAvailable` arrives when the worker connects. The proactive headroom policy is the primary mitigation: by keeping spare capacity, most scheduling requests never hit `WaitingForCapacity` at all.

---

## Reconciliation

The orchestrator is a **level-triggered controller**. On every input, it can re-evaluate the desired vs actual state for affected resources and emit commands to close the gap. This makes it naturally idempotent and resilient to missed events.

```rust
impl NamespaceStateMachine {
    fn step(&mut self, input: NamespaceInput) -> NamespaceOutput {
        let mut out = NamespaceOutput::default();

        match input {
            NamespaceInput::WorkerEvent { worker_id, event } => {
                self.apply_worker_event(&worker_id, &event);
                self.reconcile_from_event(&worker_id, &event, &mut out);
            }
            NamespaceInput::WorkerLost { worker_id } => {
                self.handle_worker_loss(&worker_id, &mut out);
            }
            NamespaceInput::TimerFired { timer_key } => {
                self.handle_timer(&timer_key, &mut out);
            }
            NamespaceInput::CapacityAvailable => {
                self.reconcile_all_services(&mut out);
            }
            NamespaceInput::UpdateSpec { client_id, spec } => {
                self.spec = spec;
                self.reconcile_all_services(&mut out);
                out.client_events.push((client_id, ClientEvent::Ok));
            }
            NamespaceInput::Delete { client_id } => {
                self.begin_destroy(&mut out);
                out.client_events.push((client_id, ClientEvent::Ok));
            }
            // ...
        }

        out
    }

    fn reconcile_service(
        &mut self,
        svc_id: &ServiceId,
        out: &mut NamespaceOutput,
    ) {
        let spec = &self.spec.services[svc_id];
        let state = &self.services[svc_id];

        match (spec.activation.is_some(), state) {
            // Has activation, currently pending -> create idle service
            (true, ServiceState::Pending) => {
                // Send CreateService to all workers with active fabric segments
                for (wid, ws) in &self.workers {
                    if ws.fabric_status == FabricStatus::Active {
                        out.worker_commands.push((*wid, WorkerCommand::CreateService { .. }));
                    }
                }
            }
            // No activation, currently pending -> create service + launch pod
            (false, ServiceState::Pending) | (_, ServiceState::WaitingForCapacity) => {
                match self.select_worker_for_pod() {
                    Some(target_worker) => {
                        out.worker_commands.push((target_worker, WorkerCommand::CreateService { .. }));
                        out.worker_commands.push((target_worker, WorkerCommand::LaunchPod { .. }));
                    }
                    None => {
                        // No worker has capacity. Park the service and signal
                        // the outer layer to provision more compute.
                        self.services.insert(*svc_id, ServiceState::WaitingForCapacity);
                        out.capacity_requests.push(CapacityRequest {
                            service_id: *svc_id,
                            memory_mb: spec.container_config.memory_mb,
                        });
                    }
                }
            }
            // Active with idle timeout expired -> scale down
            (true, ServiceState::Active {
                idle_timer: None,
                backend_need: BackendNeed::None,
                ..
            }) => {
                // StopPod + UpdateServiceBackend(None) -> transition to Idle
            }
            // ... other transitions
            _ => {}
        }
    }
}
```

---

## Client Protocol

The orchestrator exposes a control API over the same yamux+postcard transport used for workers.

### Commands (Client -> Orchestrator)

```rust
enum ClientCommand {
    // Namespace lifecycle
    CreateNamespace { namespace_id: String, spec: NamespaceSpec },
    UpdateNamespace { namespace_id: String, spec: NamespaceSpec },
    DeleteNamespace { namespace_id: String },
    GetNamespaceStatus { namespace_id: String },
    ListNamespaces,

    // Splice: route a service to a local worker
    Splice {
        namespace_id: String,
        service_id: String,
        local_worker_id: String,
    },
    Unsplice {
        namespace_id: String,
        service_id: String,
    },

    // Namespace cloning
    CloneNamespace {
        source_namespace_id: String,
        target_namespace_id: String,
        overrides: NamespaceOverrides,
    },

    // Observability
    StreamLogs {
        namespace_id: String,
        service_id: Option<String>,
    },
}
```

### Events (Orchestrator -> Client)

```rust
enum ClientEvent {
    NamespaceStatus { namespace_id: String, status: NamespaceStatusReport },
    NamespaceList { namespaces: Vec<NamespaceStatusReport> },
    LogChunk { namespace_id: String, service_id: String, data: Vec<u8> },
    Error { message: String },
    Ok,
}

struct NamespaceStatusReport {
    namespace_id: String,
    status: NamespaceStatus,
    services: HashMap<String, ServiceStatusReport>,
}

struct ServiceStatusReport {
    state: String,              // "pending", "idle", "waiting_for_capacity", "launching", "active"
    pod_id: Option<String>,
    worker_id: Option<String>,
    backend_need: Option<BackendNeed>,
    activation_enabled: bool,
    spliced: bool,
}
```

### Client Sessions

Clients maintain persistent connections for streaming (logs, status watches). The outer layer tracks connected clients via `ClientConnected`/`ClientDisconnected` inputs. When a client disconnects, any active log subscriptions for that client are cleaned up. Commands are idempotent so clients can reconnect and retry.

---

## Splice

Splice allows a user to inject a local pod into a remote namespace, replacing a service's backend with a locally-running instance. This is the primary developer experience feature — edit code locally, have it receive real traffic from the staging environment.

### State Model

Splice is represented in the service state via `ServiceHosting`:

```rust
enum ServiceHosting {
    Normal,
    Spliced { original_worker_id: WorkerId },
}
```

When a service is spliced, the namespace expands to span multiple workers. The `NamespaceWorkerState` map tracks fabric segments on each participating worker.

### Flow

1. User runs a local distvirt worker on their machine, connects to orchestrator.
2. User sends `Splice { namespace_id, service_id, local_worker_id }`.
3. Namespace state machine:
   - Adds the local worker to `self.workers` if not already present.
   - Sends `CreateNamespace` to the local worker (if first time this worker participates in this namespace).
   - Waits for `NamespaceCreated` from the local worker.
   - Stops the existing pod for that service on the cloud worker (if running).
   - Sends `CreateService` to the local worker for all services (services are projected to all participating workers).
   - Updates fabric routes: sends `FabricRouteUpdate` to both workers so L2 frames tunnel between them.
   - Launches the pod on the local worker instead.
   - `UpdateServiceBackend` on all participating workers points at the local pod's IP/MAC.
   - `ServiceReady` flushes any buffered traffic.
   - Sets `ServiceHosting::Spliced { original_worker_id }` on the service.
4. From the perspective of other services, nothing changed — same service IP/MAC, traffic just routes through the tunnel now.

### Unsplice

Reverse the process: stop local pod, re-launch on the `original_worker_id` from the `Spliced` state, update routes, set hosting back to `Normal`. If the local worker no longer hosts any pods in this namespace, the namespace state machine can tear down its fabric segment and remove it from `self.workers`.

### Requirements

- **Multi-worker fabric tunneling**: `RemoteWorker` route destinations need a real transport (likely a yamux stream between workers, or orchestrator-mediated relay).
- **Worker-to-worker or worker-to-orchestrator-to-worker frame forwarding**: The simplest initial approach is orchestrator-mediated relay (all cross-worker frames go through orchestrator). Direct worker-to-worker tunnels are an optimization.

---

## Namespace Clones

Clone creates a new namespace from an existing one. "Controlplane only" means: copy the spec, create service entities with activation policies, but don't launch any pods. Everything starts dormant and activates on demand.

Clones are independent — once created, the clone's spec is decoupled from the source. Updating or destroying the source has no effect on the clone.

### Flow

1. Client sends `CloneNamespace { source, target, overrides }`.
2. Outer layer:
   - Sets source namespace status to `Cloning { pending_destroy: false }`.
   - Copies `NamespaceSpec` from source namespace's state machine.
   - Applies overrides (different image tags, env vars, etc.).
   - Assigns new network identity (IPs, MACs) for the target namespace.
   - Creates a new `NamespaceStateMachine` for the target with the modified spec.
   - Sets all services to have activation enabled (even if source had always-on services).
   - Returns source namespace to `Active` (or transitions to `Destroying` if `pending_destroy` was set during the clone).
3. The target namespace state machine proceeds normally — creates fabric, creates services in `Idle` state.
4. First traffic to any service in the clone triggers activation.

### Clone + Destroy Interaction

If a `Delete` command arrives while the namespace is in `Cloning` state:
- The state machine sets `pending_destroy: true` instead of immediately destroying.
- When the clone operation completes, the outer layer checks `pending_destroy` and transitions to `Destroying`.
- This avoids races where a clone reads partially-destroyed state.

### Snapshot-Accelerated Clones (Future)

For faster clone activation, the orchestrator can snapshot source service pods and restore them in the clone:

```rust
// New worker protocol commands needed:
WorkerCommand::SnapshotPod { namespace_id, pod_id, snapshot_id }
WorkerCommand::LaunchPodFromSnapshot { namespace_id, pod_id, snapshot_id, network }
```

When a cloned service activates:
- Instead of cold-booting from the image, restore from the source service's snapshot.
- Firecracker snapshot restore is ~5-10ms vs ~100ms+ cold boot.
- The restored VM gets new network config (different IP) injected post-restore.

### Cost Model

Without snapshots: a clone is just metadata + service entities. Essentially free.
With snapshots: clone creation triggers snapshot of each source pod (one-time cost), then each activation in the clone is a fast restore.

---

## Failure Model

### Worker Disconnect

When a worker disconnects, the outer layer fans out a `WorkerLost` event to every namespace state machine that had the worker in its `workers` map. Each namespace state machine:

- Marks all pods on that worker as lost.
- For services with pods on the lost worker:
  - If `ServiceHosting::Spliced { original_worker_id }` and the splice worker is lost → re-launch on the original worker (unsplice).
  - If `ServiceHosting::Normal` → transition service to `Idle` (if activation-enabled) or `WaitingForCapacity`/`Launching` (if always-on, to trigger re-launch on another worker — `WaitingForCapacity` if no other worker has room).
- Removes the worker from its `workers` map.
- Cancels any timers associated with pods on that worker.
- No commands are sent to the disconnected worker.

### Orchestrator Death

When the orchestrator dies, **all cluster state is lost**. Workers detect the orchestrator disconnect and immediately tear down all their resources (namespaces, pods, fabric segments). Workers then enter a reconnect loop, attempting to re-establish a connection to the orchestrator.

On orchestrator restart, it starts with a clean slate. Workers reconnect and register as fresh workers with no existing state. Namespaces must be re-created by clients.

This is a deliberate simplicity choice. The orchestrator is the single source of truth. There is no state persistence, no WAL, no recovery protocol. This avoids a large class of consistency problems and keeps the system simple.

### Service Creation Failure

The namespace state machine defines a `ServiceFailed` transition regardless of whether the worker protocol currently sends such an event. The state machine should handle it gracefully — transitioning the service back to `Pending` (for retry) or to an error state depending on the failure mode. This ensures the orchestrator is ready when the worker protocol adds the event, and makes the state machine robust under model checking (stateright can inject failures at any point).

Currently the worker protocol does not define a `ServiceFailed` event — this is a gap to be addressed. The orchestrator assigns IPs/MACs, so conflicts shouldn't happen in a correctly functioning system, but defensive handling in the state machine costs nothing and prevents undefined behavior if something unexpected occurs.

---

## Namespace Spec Frontends

Frontends run client-side (in the CLI) and translate their format into `NamespaceSpec`.

### Compose Frontend

Parses `docker-compose.yml`, maps to `NamespaceSpec`:

| Compose concept | NamespaceSpec mapping |
|---|---|
| `services.<name>.image` | `ServiceSpec.image` |
| `services.<name>.command` | `ServiceSpec.container_config.entrypoint/args` |
| `services.<name>.environment` | `ServiceSpec.container_config.env` |
| `services.<name>.ports` | `ServiceSpec.expose` |
| `services.<name>.depends_on` | Launch ordering hint (not modeled in spec, handled by orchestrator) |
| Network assignment | Orchestrator auto-assigns IPs/MACs from namespace subnet |

### K8s-Lite Frontend (Future)

Subset of Kubernetes resources:

| K8s resource | NamespaceSpec mapping |
|---|---|
| `Deployment` | `ServiceSpec` (one service per deployment) |
| `Service` (ClusterIP) | `ServiceSpec.network` (virtual IP) |
| `ConfigMap` | Injected into `container_config.env` or volume mount |
| `Ingress` | `ServiceSpec.expose` |

Only enough to cover the common case. Not a full k8s implementation.

---

## Testing Strategy

The pure state machine architecture enables a layered testing approach, from fast property tests to exhaustive model checking. The per-namespace state machine boundary keeps the state space small and tractable.

### Layer 1: Deterministic Unit Tests

Direct step-by-step tests of the namespace state machine. Feed specific input sequences, assert exact state and outputs.

```rust
#[test]
fn service_activation_lifecycle() {
    let spec = NamespaceSpec { /* ... service with activation ... */ };
    let mut ns = NamespaceStateMachine::new(spec);

    // Worker confirms namespace created
    let out = ns.step(NamespaceInput::WorkerEvent {
        worker_id: w1,
        event: WorkerEvent::NamespaceCreated { .. },
    });
    assert!(out.worker_commands.contains(&(w1, WorkerCommand::CreateService { .. })));

    // Traffic arrives, activation fires
    let out = ns.step(NamespaceInput::WorkerEvent {
        worker_id: w1,
        event: WorkerEvent::ServiceActivation { .. },
    });
    assert!(out.worker_commands.contains(&(w1, WorkerCommand::LaunchPod { .. })));

    // Pod running, wire up backend
    let out = ns.step(NamespaceInput::WorkerEvent {
        worker_id: w1,
        event: WorkerEvent::PodRunning { .. },
    });
    assert!(out.worker_commands.contains(&(w1, WorkerCommand::UpdateServiceBackend { .. })));
    assert!(out.worker_commands.contains(&(w1, WorkerCommand::ServiceReady { .. })));

    // Backend need drops to None, idle timer starts
    let out = ns.step(NamespaceInput::WorkerEvent {
        worker_id: w1,
        event: WorkerEvent::ServiceBackendNeed { need: BackendNeed::None, .. },
    });
    assert!(out.timers_set.iter().any(|(k, _)| matches!(k, TimerKey::IdleTimeout { .. })));

    // Idle timer fires, service scales down
    let out = ns.step(NamespaceInput::TimerFired {
        timer_key: TimerKey::IdleTimeout { service_id: svc1 },
    });
    assert!(out.worker_commands.contains(&(w1, WorkerCommand::StopPod { .. })));
    assert!(out.worker_commands.contains(&(w1, WorkerCommand::UpdateServiceBackend {
        backend: None, ..
    })));
}
```

### Layer 2: Property-Based Testing (proptest)

Generate random sequences of valid inputs and check invariants hold after every step.

```rust
proptest! {
    #[test]
    fn invariants_hold(inputs in vec(arb_namespace_input(), 0..200)) {
        let mut ns = NamespaceStateMachine::new(arb_spec());
        for input in inputs {
            let output = ns.step(input);
            check_invariants(&ns, &output);
        }
    }
}

fn check_invariants(ns: &NamespaceStateMachine, output: &NamespaceOutput) {
    for (svc_id, svc) in &ns.services {
        // A service in Active state always has a valid pod_id
        if let ServiceState::Active { pod_id, worker_id, .. } = svc {
            assert!(ns.pods.contains_key(pod_id));
            assert!(ns.workers.contains_key(worker_id));
        }

        // A service in Idle state never has ServiceReady in the output
        if let ServiceState::Idle { .. } = svc {
            assert!(!output.worker_commands.iter().any(|(_, cmd)| matches!(
                cmd,
                WorkerCommand::ServiceReady { service_id, .. }
                if service_id == svc_id
            )));
        }
    }

    // No commands sent to workers not in our workers map
    for (wid, _) in &output.worker_commands {
        assert!(ns.workers.contains_key(wid));
    }
}
```

Key invariants to check:
- No `ServiceReady` without a backend assigned.
- No `LaunchPod` for a service that already has a running pod.
- No commands sent to workers not in the namespace's worker map.
- Idle timer is set when `BackendNeed::None` is received for an active service with activation.
- Idle timer is cancelled when `BackendNeed::Traffic` or `BackendNeed::Active` arrives.
- `WorkerLost` triggers cleanup of all pods on that worker.
- A spliced service tracks its `original_worker_id` correctly.
- Stale timer fires are no-ops (don't cause invalid state transitions).
- A service in `WaitingForCapacity` always has a corresponding `CapacityRequest` in the output (or one was emitted previously).
- `CapacityAvailable` never leaves a service in `WaitingForCapacity` if a suitable worker exists.

### Layer 3: Fuzz Testing (cargo-fuzz)

Coverage-guided fuzzing of the step function. The fuzzer learns which byte sequences (mapped to input sequences via `Arbitrary`) explore new code paths.

```rust
// fuzz/fuzz_targets/namespace.rs
#![no_main]
use libfuzzer_sys::fuzz_target;
use arbitrary::Arbitrary;

fuzz_target!(|data: &[u8]| {
    if let Ok(inputs) = Vec::<NamespaceInput>::arbitrary(&mut Unstructured::new(data)) {
        let mut ns = NamespaceStateMachine::new(default_spec());
        for input in inputs {
            let output = ns.step(input);
            check_invariants(&ns, &output);
        }
    }
});
```

### Layer 4: Model Checking (Stateright)

Exhaustive exploration of all possible message orderings for critical subsystems. The per-namespace state machine is the natural unit for model checking — small enough for tractable exploration.

Particularly valuable for:

- **Activation lifecycle**: Worker disconnect during activation, concurrent activation + scale-down, activation of service being destroyed.
- **Splice flow**: Concurrent splice + unsplice, worker disconnect during splice, splice of activating service.
- **Idle timeout races**: `BackendNeed::Traffic` arriving just as idle timer fires, timer fire after service already scaled down.

```rust
use stateright::*;
use stateright::actor::*;

struct NamespaceModel;

impl Actor for NamespaceModel {
    type Msg = NamespaceInput;
    type State = NamespaceStateMachine;
    type Timer = TimerKey;

    fn on_msg(
        &self,
        _id: Id,
        state: &mut Cow<Self::State>,
        _src: Id,
        msg: Self::Msg,
        o: &mut Out<Self>,
    ) {
        let output = state.to_mut().step(msg);
        for (worker_id, cmd) in output.worker_commands {
            o.send(worker_id_to_actor(worker_id), cmd.into());
        }
        for (timer_key, duration) in output.timers_set {
            o.set_timer(timer_key, duration);
        }
    }

    fn on_timeout(
        &self,
        _id: Id,
        state: &mut Cow<Self::State>,
        timer: &Self::Timer,
        o: &mut Out<Self>,
    ) {
        let output = state.to_mut().step(NamespaceInput::TimerFired {
            timer_key: timer.clone(),
        });
        // dispatch outputs...
    }

    fn properties(&self) -> Vec<Property<ActorModel<Self>>> {
        vec![
            // Safety: no service reports ready without a backend
            Property::<ActorModel<Self>>::always("ready implies backend", |_, state| {
                // check all services
                true // placeholder
            }),
            // Safety: stale timer fires never cause invalid transitions
            Property::<ActorModel<Self>>::always("timer safety", |_, state| {
                true // placeholder
            }),
            // Liveness: activation eventually leads to active or idle
            Property::<ActorModel<Self>>::eventually("activation resolves", |_, state| {
                // no service stuck in Launching forever
                true // placeholder
            }),
        ]
    }
}

// Mock worker actor that responds with realistic events
struct MockWorker;
impl Actor for MockWorker {
    type Msg = NamespaceInput;
    type State = MockWorkerState;
    type Timer = ();

    fn on_msg(&self, _id: Id, state: &mut Cow<Self::State>, src: Id, msg: Self::Msg, o: &mut Out<Self>) {
        // On LaunchPod: non-deterministically respond with PodRunning or PodFailed
        // On CreateNamespace: respond with NamespaceCreated or NamespaceFailed
        // Randomly: disconnect (triggers WorkerLost at the namespace level)
    }
}
```

Stateright will explore all possible orderings of messages between the namespace state machine and mock workers, including worker failures at every possible point, and verify that safety/liveness properties hold in every reachable state.

### Integration / E2E Tests

The existing e2e test infrastructure (see `distvirt-worker/tests/e2e.rs`) covers the worker side. Orchestrator e2e tests would:
- Spin up an in-process orchestrator + worker (like compose does today).
- Send client commands, verify end-to-end behavior.
- These are slow and non-exhaustive, but verify the async shell + real worker integration.

### Test Priority

1. **Pure step tests**: Write these as you implement each feature. Fast, easy, high coverage.
2. **proptest invariants**: Set up the framework early. Continuously add invariants as you discover them. These find the "I didn't think of that combination" bugs.
3. **Fuzz harness**: Set up once, run in CI. Finds edge cases proptest misses through coverage guidance.
4. **Stateright model**: Build for activation + splice specifically. These subsystems have the most interleaving complexity and benefit most from exhaustive checking.

---

## Open Design Questions

1. **Worker scheduling policy**: `select_worker_for_pod()` needs a strategy. Simple: round-robin or least-loaded. Complex: locality-aware (splice prefers same region), bin-packing vs spreading. The capacity management framework handles the "no worker available" case, but the selection policy among available workers is still open.
2. **Fabric tunnel transport**: Orchestrator-mediated relay (simple, higher latency) vs direct worker-to-worker tunnels (complex, lower latency). Start with relay.
3. **Snapshot storage**: Where do VM snapshots live? Local to worker (fast, not portable) vs shared storage (portable, slower). Affects clone across workers.
4. **Spec diffing**: When `UpdateNamespace` changes a service's image, what happens? Rolling update (launch new pod, drain old)? Or hard cut (stop old, launch new)?
5. **Capacity manager tuning**: What headroom policy values work in practice? How aggressively should scale-down reclaim idle workers? Should the policy be configurable per-deployment or global?
6. **Provisioner interface**: The capacity manager emits `ProvisionWorker`/`TerminateWorker` outputs. The async shell maps these to infrastructure APIs. What's the right abstraction boundary? Should the provisioner report estimated time-to-ready so the orchestrator can make better decisions?
