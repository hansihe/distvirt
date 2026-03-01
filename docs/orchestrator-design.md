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
    /// Outer layer injects this when it has selected a worker and
    /// generated a pod ID for a workload's PodRequest.
    LaunchPod { workload_id: WorkloadId, pod_id: PodId, worker_id: WorkerId },
}

struct NamespaceOutput {
    worker_commands: Vec<(WorkerId, WorkerCommand)>,
    client_events: Vec<(ClientId, ClientEvent)>,
    timers_set: Vec<(TimerKey, Duration)>,
    timers_cancel: Vec<TimerKey>,
    /// Workloads that need a pod. The outer layer selects a worker,
    /// generates a pod ID, and injects LaunchPod back.
    pod_requests: Vec<PodRequest>,
}

struct PodRequest {
    workload_id: WorkloadId,
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
    LaunchPod { pod_id: PodId, worker_id: WorkerId },
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
    /// The workload needs a pod — outer layer should select a worker,
    /// generate a pod ID, and inject LaunchPod back.
    PodRequest,
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
- **Per-namespace isolation**: Each namespace state machine is independently testable.
- **Sub-SM isolation**: Workload and service state machines have tiny state spaces (~4 states each), enabling exhaustive model checking that would be intractable on the monolithic design.

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
    service_workload: HashMap<ServiceId, WorkloadId>,
    workloads: HashMap<WorkloadId, WorkloadStateMachine>,
    services: HashMap<ServiceId, ServiceStateMachine>,
}

enum NamespaceStatus {
    /// Waiting for initial worker assignment and CreateNamespace ack.
    Creating,
    /// Fabric is up, services are being reconciled.
    Active,
    /// A clone operation is reading from this namespace's spec.
    /// Destroy commands are deferred until cloning completes.
    Cloning { pending_destroy: bool },
    /// DestroyNamespace sent to all workers, waiting for cleanup.
    /// The namespace emits DestroyNamespace commands to each worker
    /// and waits for confirmation or worker disconnect. The outer layer
    /// removes the namespace once all workers have confirmed or disconnected.
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

### Workload State Machine

Each workload manages the pod lifecycle independently, driven by demand signals from services:

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
                  +------------+------------+
                               | PodRunning
                               | -> emit BecameReady
                               v
                  +-------------------------+
     DemandDown   |  Running               |  WorkerLost / PodFailed
     (demand_count|  pod running, demand>0 |  -> emit BecameUnready
      -> 0)       +------------+------------+  -> transition to Dormant
     -> StopPod                                   or WaitingForCapacity
     -> emit BecameUnready                        (if demand > 0)
     -> Dormant

WaitingForCapacity emits PodRequest to the outer layer. The outer
layer selects a worker, generates a pod ID, and injects LaunchPod.
This keeps worker selection out of the workload SM entirely.
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
        hosting: WorkloadHosting,
    },
}

/// Tracks whether a workload's pod is running on its normal worker
/// or has been spliced to a different worker.
enum WorkloadHosting {
    /// Pod runs on the namespace's normal worker.
    Normal,
    /// Pod has been spliced to a different worker.
    /// Stores the original worker so unsplice knows where to put it back.
    Spliced { original_worker_id: WorkerId },
}
```

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

**Idle timeout lifecycle**: When a service is `Active` and receives `ServiceBackendNeed(None)`, the service starts its idle timer. If `ServiceBackendNeed(Traffic)` or `ServiceBackendNeed(Active)` arrives before the timer fires, the timer is cancelled. If the timer fires while `BackendNeed` is still `None`, the service emits `DemandDown` + `UpdateServiceBackend(None)` and transitions back to `Idle`. If this was the last service demanding the workload (demand_count drops to 0), the workload stops the pod.

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

Note that `pod_id` and `worker_id` in `ServiceState::Active` are cached copies from the `WorkloadReady` signal — the source of truth for pod location is the `WorkloadStateMachine`. Similarly, `WorkloadHosting` (splice state) lives in the workload, not the service.

### Coupling Interface

The workload and service sub-SMs communicate through exactly four signals, routed by the coordinator:

| Signal | Direction | Meaning |
|---|---|---|
| `DemandUp` | Service → Workload | A service needs the workload running. Increments `demand_count`. |
| `DemandDown` | Service → Workload | A service no longer needs the workload. Decrements `demand_count`. |
| `BecameReady` | Workload → Service(s) | Pod is running. Carries `pod_id` and `worker_id`. |
| `BecameUnready` | Workload → Service(s) | Pod is no longer available (exited, failed, worker lost). |

The coordinator maintains `service_workload: HashMap<ServiceId, WorkloadId>` and fans out workload signals to all services mapped to that workload.

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
    /// Owned by ServiceStateMachine.
    IdleTimeout { service_id: ServiceId },
    /// Launch timeout — pod took too long to start.
    /// Owned by WorkloadStateMachine.
    LaunchTimeout { workload_id: WorkloadId, pod_id: PodId },
}
```

Each timer is owned by a specific sub-SM. The coordinator routes `TimerFired` to the owning sub-SM based on the `TimerKey` variant. Setting a new timer with the same key implicitly cancels the previous one. The `NamespaceOutput` has both `timers_set` and `timers_cancel` for explicit lifecycle management.

### Stale Timer Handling

Timer fires are treated as **hints, not commands**. Each sub-SM checks whether its current state still warrants the timer's action:

- `IdleTimeout` fires but service is already `Idle` → no-op (ServiceStateMachine).
- `IdleTimeout` fires but `backend_need` is `Active` → no-op (timer should have been cancelled, but this is a safe fallback).
- `LaunchTimeout` fires but workload is already `Running` → no-op (WorkloadStateMachine).

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

2. **Reactive (pod requests)** — when a namespace's `step()` output contains `pod_requests` and no worker has capacity, the outer layer queues them and provisions if no pending workers will satisfy the need.

### Worker Connected Flow

When a new `WorkerConnected` arrives (whether from proactive provisioning or an externally-added worker):

1. Outer layer adds the worker to its `workers` map.
2. Drains the pending pod request queue — for each queued `PodRequest`, the outer layer tries `select_worker_for_pod()`. If a worker fits, it generates a pod ID and injects `LaunchPod` into the namespace. If nothing fits, the request stays queued.

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

Worker provisioning is slow (e.g. ~30s for an EC2 instance). This latency is invisible to the namespace state machine — it just sees the workload emit `PodRequest`, then eventually the outer layer injects `LaunchPod` when a worker is available. The proactive headroom policy is the primary mitigation: by keeping spare capacity, most scheduling requests are fulfilled immediately.

---

## Reconciliation

The orchestrator is a **level-triggered controller**. On every input, it can re-evaluate the desired vs actual state for affected resources and emit commands to close the gap. This makes it naturally idempotent and resilient to missed events.

The coordinator routes inputs to the appropriate sub-SM and forwards internal signals between them:

```rust
impl NamespaceStateMachine {
    fn step(&mut self, input: NamespaceInput) -> NamespaceOutput {
        let mut out = NamespaceOutput::default();

        match input {
            NamespaceInput::WorkerEvent { worker_id, event } => {
                self.apply_worker_event(&worker_id, &event);
                // Route to the appropriate workload SM based on the event's pod/service.
                // Forward any BecameReady/BecameUnready outputs to service SMs.
                self.route_worker_event(&worker_id, &event, &mut out);
            }
            NamespaceInput::WorkerLost { worker_id } => {
                // Forward WorkerLost to all workloads with pods on that worker.
                // Workloads emit BecameUnready, which we forward to their services.
                self.handle_worker_loss(&worker_id, &mut out);
            }
            NamespaceInput::TimerFired { timer_key } => {
                // Route to owning sub-SM based on timer key variant.
                match &timer_key {
                    TimerKey::IdleTimeout { service_id } => {
                        let svc_outputs = self.services[service_id]
                            .step(ServiceInput::TimerFired { timer_key });
                        self.forward_service_outputs(service_id, svc_outputs, &mut out);
                    }
                    TimerKey::LaunchTimeout { workload_id, .. } => {
                        let wl_outputs = self.workloads[workload_id]
                            .step(WorkloadInput::TimerFired { timer_key });
                        self.forward_workload_outputs(workload_id, wl_outputs, &mut out);
                    }
                }
            }
            NamespaceInput::LaunchPod { workload_id, pod_id, worker_id } => {
                // Outer layer selected a worker and generated a pod ID.
                // Forward to the workload SM.
                let wl_outputs = self.workloads[&workload_id]
                    .step(WorkloadInput::LaunchPod { pod_id, worker_id });
                self.forward_workload_outputs(&workload_id, wl_outputs, &mut out);
            }
            NamespaceInput::UpdateSpec { client_id, spec } => {
                self.spec = spec;
                self.reconcile_all(&mut out);
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

    /// Forward outputs from a service sub-SM. DemandUp/DemandDown
    /// get routed to the service's workload.
    fn forward_service_outputs(
        &mut self,
        service_id: &ServiceId,
        outputs: Vec<ServiceOutput>,
        out: &mut NamespaceOutput,
    ) {
        let workload_id = &self.service_workload[service_id];
        for svc_out in outputs {
            match svc_out {
                ServiceOutput::DemandUp => {
                    let wl_outputs = self.workloads[workload_id]
                        .step(WorkloadInput::DemandUp);
                    self.forward_workload_outputs(workload_id, wl_outputs, out);
                }
                ServiceOutput::DemandDown => {
                    let wl_outputs = self.workloads[workload_id]
                        .step(WorkloadInput::DemandDown);
                    self.forward_workload_outputs(workload_id, wl_outputs, out);
                }
                ServiceOutput::WorkerCommand { worker_id, command } => {
                    out.worker_commands.push((worker_id, command));
                }
                ServiceOutput::SetTimer { key, duration } => {
                    out.timers_set.push((key, duration));
                }
                ServiceOutput::CancelTimer { key } => {
                    out.timers_cancel.push(key);
                }
            }
        }
    }

    /// Forward outputs from a workload sub-SM. BecameReady/BecameUnready
    /// get fanned out to all services mapped to this workload.
    fn forward_workload_outputs(
        &mut self,
        workload_id: &WorkloadId,
        outputs: Vec<WorkloadOutput>,
        out: &mut NamespaceOutput,
    ) {
        for wl_out in outputs {
            match wl_out {
                WorkloadOutput::BecameReady { pod_id, worker_id } => {
                    // Fan out to all services on this workload.
                    for (svc_id, wl_id) in &self.service_workload {
                        if wl_id == workload_id {
                            let svc_outputs = self.services[svc_id]
                                .step(ServiceInput::WorkloadReady { pod_id, worker_id });
                            self.forward_service_outputs(svc_id, svc_outputs, out);
                        }
                    }
                }
                WorkloadOutput::BecameUnready => {
                    for (svc_id, wl_id) in &self.service_workload {
                        if wl_id == workload_id {
                            let svc_outputs = self.services[svc_id]
                                .step(ServiceInput::WorkloadUnready);
                            self.forward_service_outputs(svc_id, svc_outputs, out);
                        }
                    }
                }
                WorkloadOutput::PodRequest => {
                    out.pod_requests.push(PodRequest {
                        workload_id: *workload_id,
                    });
                }
                WorkloadOutput::WorkerCommand { worker_id, command } => {
                    out.worker_commands.push((worker_id, command));
                }
                WorkloadOutput::SetTimer { key, duration } => {
                    out.timers_set.push((key, duration));
                }
                WorkloadOutput::CancelTimer { key } => {
                    out.timers_cancel.push(key);
                }
            }
        }
    }
}
```

Note how the coordinator is pure routing — no business logic about activation, idle timeouts, or pod scheduling. This makes it easy to test with proptest (generate random signal sequences, verify routing correctness) while the sub-SMs are small enough for exhaustive model checking.

### Registry Sync

Rather than emitting individual `UpdateServiceBackend` commands per service, the namespace broadcasts a full **RegistrySync** — the complete set of service entries (IP, MAC, backend info) — to all active workers in the namespace. This is emitted on key state transitions:

- Namespace becomes Active (initial reconciliation)
- A service backend changes (pod ready, backend cleared)
- Worker loss (surviving workers get updated registry)

This approach is simpler and naturally idempotent — workers always converge to the correct state regardless of missed individual updates. It replaces the per-service `UpdateServiceBackend` + `ServiceReady` commands described elsewhere in this doc with a single broadcast.

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

    // Splice: route a workload to a local worker
    Splice {
        namespace_id: String,
        workload_id: String,
        local_worker_id: String,
    },
    Unsplice {
        namespace_id: String,
        workload_id: String,
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
    state: String,              // "pending", "idle", "need_backend", "active"
    workload_id: String,
    workload_state: String,     // "dormant", "waiting_for_capacity", "launching", "running"
    pod_id: Option<String>,
    worker_id: Option<String>,
    backend_need: Option<BackendNeed>,
    activation_enabled: bool,
    spliced: bool,              // from workload hosting state
}
```

### Client Sessions

Clients maintain persistent connections for streaming (logs, status watches). The outer layer tracks connected clients via `ClientConnected`/`ClientDisconnected` inputs. When a client disconnects, any active log subscriptions for that client are cleaned up. Commands are idempotent so clients can reconnect and retry.

---

## Splice

Splice allows a user to inject a local pod into a remote namespace, replacing a workload's backend with a locally-running instance. This is the primary developer experience feature — edit code locally, have it receive real traffic from the staging environment.

### State Model

Splice operates at the **workload level**, not the service level. The pod belongs to the workload, and moving it automatically updates all services that share that workload.

Splice state is tracked in `WorkloadHosting`:

```rust
enum WorkloadHosting {
    Normal,
    Spliced { original_worker_id: WorkerId },
}
```

When a workload is spliced, the namespace expands to span multiple workers. The `NamespaceWorkerState` map tracks fabric segments on each participating worker.

### Flow

1. User runs a local distvirt worker on their machine, connects to orchestrator.
2. User sends `Splice { namespace_id, workload_id, local_worker_id }`.
3. Namespace coordinator:
   - Adds the local worker to `self.workers` if not already present.
   - Sends `CreateNamespace` to the local worker (if first time this worker participates in this namespace).
   - Waits for `NamespaceCreated` from the local worker.
   - Forwards the splice to the workload SM, which:
     - Stops the existing pod on the cloud worker (if running) → emits `BecameUnready`.
     - Sends `CreateService` to the local worker for all services (services are projected to all participating workers).
     - Updates fabric routes: sends `FabricRouteUpdate` to both workers so L2 frames tunnel between them.
     - Launches the pod on the local worker instead.
     - On `PodRunning` → emits `BecameReady` with the new worker_id.
     - Sets `WorkloadHosting::Spliced { original_worker_id }`.
   - Coordinator forwards `BecameReady` to all services on this workload.
   - Services emit `UpdateServiceBackend` + `ServiceReady` pointing at the new pod.
4. From the perspective of other services, nothing changed — same service IP/MAC, traffic just routes through the tunnel now.

### Unsplice

Reverse the process: workload SM stops local pod (emits `BecameUnready`), re-launches on the `original_worker_id` from the `Spliced` state (emits `BecameReady`), sets hosting back to `Normal`. Coordinator forwards the signals to services, which update their backends. If the local worker no longer hosts any pods in this namespace, the namespace coordinator can tear down its fabric segment and remove it from `self.workers`.

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

When a worker disconnects, the outer layer fans out a `WorkerLost` event to every namespace state machine that had the worker in its `workers` map. The coordinator routes this through the sub-SM layers:

1. **Coordinator** forwards `WorkerLost` to all workload SMs with pods on that worker.
2. **WorkloadStateMachine** marks the pod as lost and emits `BecameUnready`. Then:
   - If `WorkloadHosting::Spliced { original_worker_id }` and the splice worker is lost → transition to `Launching` on the original worker (unsplice).
   - If `WorkloadHosting::Normal` and `demand_count > 0` → transition to `WaitingForCapacity` (to re-launch on another worker).
   - If `demand_count == 0` → transition to `Dormant`.
3. **Coordinator** forwards `BecameUnready` to all services mapped to affected workloads.
4. **ServiceStateMachine** receives `WorkloadUnready`:
   - If activation-enabled → transition to `Idle` (ready to re-activate on next traffic).
   - If always-on → transition to `NeedBackend` (will get `WorkloadReady` when workload re-launches).
5. Coordinator removes the worker from its `workers` map.
6. Cancels any timers associated with pods on that worker.
7. No commands are sent to the disconnected worker.

### Namespace Deletion

Namespace deletion is a **stateful teardown**, not fire-and-forget. This matters because a namespace can span multiple workers (especially with splice), and each worker must clean up its fabric segment and pods.

1. Client sends `Delete { client_id }`.
2. Namespace transitions to `Destroying`:
   - Cancels all timers (idle timeouts, launch timeouts).
   - Stops accepting new inputs (activation events, spec updates).
   - Emits `DestroyNamespace` to every worker in `self.workers`.
3. As each worker confirms destruction (or disconnects), the namespace removes it from `self.workers`.
4. When `self.workers` is empty, the namespace signals it is fully destroyed.
5. The outer layer removes the namespace from its map.

While in `Destroying`, the namespace ignores all inputs except `WorkerEvent` (to process destruction confirmations) and `WorkerLost` (to remove disconnected workers). This ensures cleanup completes even if workers are slow to respond.

The `Cloning { pending_destroy }` status defers entry into `Destroying` until the clone operation completes reading the spec.

### Orchestrator Death

When the orchestrator dies, **all cluster state is lost**. Workers detect the orchestrator disconnect and immediately tear down all their resources (namespaces, pods, fabric segments). Workers then enter a reconnect loop, attempting to re-establish a connection to the orchestrator.

On orchestrator restart, it starts with a clean slate. Workers reconnect and register as fresh workers with no existing state. Namespaces must be re-created by clients.

This is a deliberate simplicity choice. The orchestrator is the single source of truth. There is no state persistence, no WAL, no recovery protocol. This avoids a large class of consistency problems and keeps the system simple.

### Service Creation Failure

The workload and service sub-SMs define failure transitions regardless of whether the worker protocol currently sends such events. The workload SM handles `PodFailed` gracefully — emitting `BecameUnready` and transitioning to `Dormant` or `WaitingForCapacity` depending on demand. The service SM handles `WorkloadUnready` by transitioning back to `Idle` or `NeedBackend`. This ensures the orchestrator is ready when the worker protocol adds failure events, and makes each sub-SM robust under model checking (stateright can inject failures at any point).

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

The workload/service split is designed primarily to improve testability. Each sub-SM has a tiny state space that can be model-checked exhaustively, while the coordinator is thin routing logic testable with property-based tests.

### Key Payoff: Independent Model Checking

The monolithic design had state space O(states^N) — all services interleaved. The split gives O(states × N):

- **WorkloadStateMachine**: ~4 states (Dormant, WaitingForCapacity, Launching, Running). With demand_count and hosting variant, the full state space is small enough for exhaustive stateright exploration.
- **ServiceStateMachine**: ~4 states (Pending, Idle, NeedBackend, Active) × 3 backend_need values. Also tiny.
- **Coordinator**: Pure routing logic (no business decisions). Testable with proptest — generate random signal sequences, verify signals are forwarded correctly.

Each sub-SM can be checked in isolation with a mock environment, then composition correctness is verified at the coordinator level.

### Layer 1: Deterministic Unit Tests

Direct step-by-step tests of sub-SMs and the full coordinator. Feed specific input sequences, assert exact state and outputs.

```rust
#[test]
fn workload_demand_lifecycle() {
    let mut wl = WorkloadStateMachine::new(workload_spec());
    assert!(matches!(wl.state, WorkloadState::Dormant));

    // First service demands the workload
    let out = wl.step(WorkloadInput::DemandUp);
    // Workload should try to launch (or emit NeedCapacity)
    assert!(matches!(wl.state, WorkloadState::Launching { .. })
         || matches!(wl.state, WorkloadState::WaitingForCapacity));

    // Pod starts running
    let out = wl.step(WorkloadInput::WorkerEvent {
        worker_id: w1,
        event: WorkerEvent::PodRunning { .. },
    });
    assert!(matches!(wl.state, WorkloadState::Running { .. }));
    assert!(out.iter().any(|o| matches!(o, WorkloadOutput::BecameReady { .. })));

    // Last service drops demand
    let out = wl.step(WorkloadInput::DemandDown);
    assert!(matches!(wl.state, WorkloadState::Dormant));
    assert!(out.iter().any(|o| matches!(o, WorkloadOutput::BecameUnready)));
}

#[test]
fn service_activation_lifecycle() {
    let mut svc = ServiceStateMachine::new(service_spec_with_activation(), wl_id);
    assert!(matches!(svc.state, ServiceState::Idle));

    // Traffic arrives
    let out = svc.step(ServiceInput::ServiceActivation);
    assert!(matches!(svc.state, ServiceState::NeedBackend));
    assert!(out.iter().any(|o| matches!(o, ServiceOutput::DemandUp)));

    // Workload becomes ready
    let out = svc.step(ServiceInput::WorkloadReady { pod_id: p1, worker_id: w1 });
    assert!(matches!(svc.state, ServiceState::Active { .. }));
    assert!(out.iter().any(|o| matches!(o,
        ServiceOutput::WorkerCommand { command: WorkerCommand::UpdateServiceBackend { .. }, .. }
    )));

    // Backend need drops to None, idle timer starts
    let out = svc.step(ServiceInput::ServiceBackendNeed { need: BackendNeed::None });
    assert!(out.iter().any(|o| matches!(o, ServiceOutput::SetTimer { .. })));

    // Idle timer fires, service scales down
    let out = svc.step(ServiceInput::TimerFired {
        timer_key: TimerKey::IdleTimeout { service_id: svc1 },
    });
    assert!(matches!(svc.state, ServiceState::Idle));
    assert!(out.iter().any(|o| matches!(o, ServiceOutput::DemandDown)));
}
```

### Layer 2: Property-Based Testing (proptest)

Generate random sequences of valid inputs and check invariants hold after every step. Tests are written at three levels:

**Sub-SM level** — test each sub-SM in isolation:

```rust
proptest! {
    #[test]
    fn workload_invariants(inputs in vec(arb_workload_input(), 0..100)) {
        let mut wl = WorkloadStateMachine::new(arb_workload_spec());
        for input in inputs {
            let outputs = wl.step(input);
            // demand_count is always >= 0
            assert!(wl.demand_count >= 0);
            // BecameReady only emitted when transitioning to Running
            // BecameUnready only emitted when leaving Running
            // NeedCapacity only emitted when in WaitingForCapacity
            check_workload_invariants(&wl, &outputs);
        }
    }

    #[test]
    fn service_invariants(inputs in vec(arb_service_input(), 0..100)) {
        let mut svc = ServiceStateMachine::new(arb_service_spec(), arb_workload_id());
        for input in inputs {
            let outputs = svc.step(input);
            check_service_invariants(&svc, &outputs);
        }
    }
}
```

**Coordinator level** — test signal routing correctness:

```rust
proptest! {
    #[test]
    fn coordinator_routing(inputs in vec(arb_namespace_input(), 0..200)) {
        let mut ns = NamespaceStateMachine::new(arb_spec());
        for input in inputs {
            let output = ns.step(input);
            check_coordinator_invariants(&ns, &output);
        }
    }
}

fn check_coordinator_invariants(ns: &NamespaceStateMachine, output: &NamespaceOutput) {
    // No commands sent to workers not in our workers map
    for (wid, _) in &output.worker_commands {
        assert!(ns.workers.contains_key(wid));
    }

    // Every service's workload_id points to a valid workload
    for (svc_id, wl_id) in &ns.service_workload {
        assert!(ns.workloads.contains_key(wl_id));
    }

    // A service in Active state has a workload in Running state
    for (svc_id, svc) in &ns.services {
        if let ServiceState::Active { pod_id, worker_id, .. } = &svc.state {
            let wl_id = &ns.service_workload[svc_id];
            let wl = &ns.workloads[wl_id];
            assert!(matches!(wl.state, WorkloadState::Running { .. }));
        }
    }

    // A service in Idle state never has ServiceReady in the output
    for (svc_id, svc) in &ns.services {
        if let ServiceState::Idle = &svc.state {
            assert!(!output.worker_commands.iter().any(|(_, cmd)| matches!(
                cmd,
                WorkerCommand::ServiceReady { service_id, .. }
                if service_id == svc_id
            )));
        }
    }
}
```

Key invariants:
- No `ServiceReady` without a backend assigned.
- No `LaunchPod` for a workload that already has a running pod.
- No commands sent to workers not in the namespace's worker map.
- `DemandUp`/`DemandDown` are always balanced (demand_count never goes negative).
- `BecameReady`/`BecameUnready` are forwarded to all services on the workload.
- Idle timer is set when `BackendNeed::None` is received for an active service with activation.
- Idle timer is cancelled when `BackendNeed::Traffic` or `BackendNeed::Active` arrives.
- `WorkerLost` triggers `BecameUnready` for all workloads on that worker, which cascades to services.
- Stale timer fires are no-ops (don't cause invalid state transitions).
- A workload in `WaitingForCapacity` always has a corresponding `PodRequest` output.

### Layer 3: Fuzz Testing (cargo-fuzz)

Coverage-guided fuzzing of each sub-SM's step function. Separate fuzz targets for workload, service, and coordinator:

```rust
// fuzz/fuzz_targets/workload_sm.rs
fuzz_target!(|data: &[u8]| {
    if let Ok(inputs) = Vec::<WorkloadInput>::arbitrary(&mut Unstructured::new(data)) {
        let mut wl = WorkloadStateMachine::new(default_workload_spec());
        for input in inputs {
            let outputs = wl.step(input);
            check_workload_invariants(&wl, &outputs);
        }
    }
});

// fuzz/fuzz_targets/service_sm.rs
fuzz_target!(|data: &[u8]| {
    if let Ok(inputs) = Vec::<ServiceInput>::arbitrary(&mut Unstructured::new(data)) {
        let mut svc = ServiceStateMachine::new(default_service_spec(), default_workload_id());
        for input in inputs {
            let outputs = svc.step(input);
            check_service_invariants(&svc, &outputs);
        }
    }
});
```

### Layer 4: Model Checking (Stateright)

This is where the split pays off most. Each sub-SM is small enough for exhaustive exploration.

#### WorkloadStateMachine Model

```rust
struct WorkloadModel {
    /// Number of services that can send DemandUp/DemandDown.
    num_services: usize,
}

impl Actor for WorkloadModel {
    type Msg = WorkloadInput;
    type State = WorkloadStateMachine;
    type Timer = TimerKey;

    fn on_msg(&self, _id: Id, state: &mut Cow<Self::State>, src: Id, msg: Self::Msg, o: &mut Out<Self>) {
        let outputs = state.to_mut().step(msg);
        for wl_out in outputs {
            match wl_out {
                WorkloadOutput::SetTimer { key, duration } => o.set_timer(key, duration),
                WorkloadOutput::WorkerCommand { worker_id, command } => {
                    o.send(worker_id_to_actor(worker_id), command.into());
                }
                _ => {} // BecameReady/BecameUnready checked via properties
            }
        }
    }

    fn properties(&self) -> Vec<Property<ActorModel<Self>>> {
        vec![
            Property::always("no pod without demand", |_, state| {
                // If demand_count == 0, state must be Dormant
                let wl = &state.actor_states[0];
                wl.demand_count > 0 || matches!(wl.state, WorkloadState::Dormant)
            }),
            Property::always("launch timeout respected", |_, state| {
                // Launching state always has a launch timeout timer set
                true // check timer exists
            }),
        ]
    }
}

// Mock service actors that non-deterministically send DemandUp/DemandDown.
// Mock worker actor that non-deterministically responds to LaunchPod.
```

State space: ~4 states × demand_count range × hosting variant. Easily exhaustible.

#### ServiceStateMachine Model

```rust
struct ServiceModel;

impl Actor for ServiceModel {
    type Msg = ServiceInput;
    type State = ServiceStateMachine;
    type Timer = TimerKey;

    fn on_msg(&self, _id: Id, state: &mut Cow<Self::State>, _src: Id, msg: Self::Msg, o: &mut Out<Self>) {
        let outputs = state.to_mut().step(msg);
        for svc_out in outputs {
            match svc_out {
                ServiceOutput::SetTimer { key, duration } => o.set_timer(key, duration),
                ServiceOutput::CancelTimer { key } => o.cancel_timer(key),
                _ => {}
            }
        }
    }

    fn properties(&self) -> Vec<Property<ActorModel<Self>>> {
        vec![
            Property::always("no ready without backend", |_, state| {
                // ServiceReady only emitted when transitioning to Active
                true
            }),
            Property::always("idle timeout safety", |_, state| {
                // Stale idle timeout fires don't cause invalid transitions
                true
            }),
            Property::eventually("activation resolves", |_, state| {
                // No service stuck in NeedBackend forever
                // (assuming workload eventually becomes ready or fails)
                true
            }),
        ]
    }
}

// Mock workload that non-deterministically sends WorkloadReady/WorkloadUnready.
// Mock activator that non-deterministically sends ServiceActivation/BackendNeed.
```

State space: ~4 states × 3 backend_need × idle_timer presence. Easily exhaustible.

#### Composition Testing

The coordinator is thin routing logic — no business decisions, just signal forwarding. This means composition correctness reduces to:
1. Each sub-SM is correct in isolation (verified by stateright above).
2. The coordinator routes signals correctly (verified by proptest).
3. The `service_workload` mapping is maintained correctly (verified by proptest invariants).

This avoids the combinatorial explosion of model-checking the full composed system while still providing strong correctness guarantees.

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

1. **Worker scheduling policy**: `select_worker_for_pod()` needs a strategy. Simple: round-robin or least-loaded. Complex: locality-aware (splice prefers same region), bin-packing vs spreading. The capacity management framework handles the "no worker available" case, but the selection policy among available workers is still open.
2. **Fabric tunnel transport**: Orchestrator-mediated relay (simple, higher latency) vs direct worker-to-worker tunnels (complex, lower latency). Start with relay.
3. **Snapshot storage**: Where do VM snapshots live? Local to worker (fast, not portable) vs shared storage (portable, slower). Affects clone across workers.
4. **Spec diffing**: When `UpdateNamespace` changes a service's image, what happens? Rolling update (launch new pod, drain old)? Or hard cut (stop old, launch new)?
5. **Capacity manager tuning**: What headroom policy values work in practice? How aggressively should scale-down reclaim idle workers? Should the policy be configurable per-deployment or global?
6. **Provisioner interface**: The capacity manager emits `ProvisionWorker`/`TerminateWorker` outputs. The async shell maps these to infrastructure APIs. What's the right abstraction boundary? Should the provisioner report estimated time-to-ready so the orchestrator can make better decisions?
