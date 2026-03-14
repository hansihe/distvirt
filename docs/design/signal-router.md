# Signal Router — Design Document

## Motivation

The orchestrator's state machines (SMs) manage the lifecycle of services, workloads, and pods. In the current architecture, all inter-SM communication happens through discrete messages. This works at the base level, but it forces every SM to handle a class of problems that SMs are fundamentally bad at: **tracking and synchronizing external state**.

Concrete pain points:

- **"Initial state" vs "update" duality.** When a relationship is created (e.g., a new service points at a workload), the creating code must send the current state to bring the other side up to date. This code path is always subtly different from the ongoing update path, and they drift apart.
- **"Notify all dependents on change."** When a spec changes, or a workload becomes ready, or demand shifts, the code must manually enumerate everyone who cares and send them updates. This is scattered across ~6 call sites in reconciliation code and is a persistent source of missed notifications.
- **Worker loss propagation.** When a worker disappears, every SM with a relationship to that worker needs to respond. Each SM handles this independently, leading to inconsistent cleanup.
- **Demand tracking.** Demand for a workload is aggregated from multiple sources (services, active flows). This aggregation lives half in the reconciler and half in the workload SM, with `needs_successful_boot` and `PendingIntent::Demand` as SM-level state that really belongs to the demand concern.

All of these are instances of the same fundamental problem: SMs are forced to track relationships and keep derived state in sync, which is combinatorial (every state x every relationship x every change type) and inherently error-prone. This is work that is trivial to get right when done systematically in normal code, but produces a steady stream of bugs when distributed across SM handlers.

## Core Concepts

### SM (State Machine)

A state machine instance with an identity (type + ID), internal state, signal outputs, and a handler for incoming inputs. SMs contain domain logic — lifecycle transitions, decision-making, business rules. They do not contain relationship bookkeeping.

### Port

An external boundary point that participates in the signal/edge system but is not a full state machine. Ports represent things outside the SM system — worker connections, the management layer, fabric endpoints. A port can produce signals and send events, but has no state machine lifecycle.

Port types are declared once; multiple instances of the same port type can be created. Each port instance has its own identity, signal values, and edges — the same instance model as SMs. For example, the management layer creates one `ManagementPort` instance per managed SM, each producing its respective spec as a regular instance-wide signal.

### Output Signal

A persistent, current-value that an SM or port instance produces. Declared per SM/port type — an SM says "I produce signal X" without knowing who consumes it. Signals are instance-wide: each instance produces one value per signal type, and that value is projected through all outgoing edges. Signals are keyed by (instance ID, signal type). They represent "what is the current state," not "what just happened."

Signal types must implement `PartialEq`. The router uses equality comparison to detect changes — when an SM sets a signal to the same value it already has, no propagation occurs. This avoids unnecessary re-aggregation and is the primary mechanism for preventing redundant work within a round.

Examples: service demand (bool), workload readiness (Option\<ReadyInfo\>), pod status, worker liveness, spec configuration.

### Edge

A unidirectional, typed, structural relationship from one SM/port instance to another. Edges do not carry signals directly — they represent that a relationship exists. Signals flow through edges as determined by aggregated input declarations on the consuming side.

Edges can be created and destroyed reactively — an SM can update its outgoing edges in response to an aggregated input it receives. This is the mechanism by which the graph evolves in response to state changes.

### Aggregated Input

A declared input on a consuming SM type that specifies: a list of `(EdgeType, Signal)` source pairs, and an aggregator that combines them into a single typed input. Each source pair declares that signals of a given type should be collected through edges of a given type. The macro validates that the edge's source type actually produces the referenced signal.

A single aggregated input can pull from multiple source pairs. For example, a workload's demand input can aggregate service demand signals (through `ServiceToWorkload` edges) and fabric flow signals (through `FabricToWorkload` edges) into one combined demand value. For multi-source inputs, the macro generates an enum with one variant per source pair, and the aggregator receives a slice of this enum.

### Aggregator

The function that reduces N signal values (from N incoming edges) into one aggregated input value. Aggregators are defined per aggregated input and are fully customizable — they are normal Rust trait implementations.

Aggregators always handle the empty-input case (zero edges) — this produces the "no data" baseline (e.g., demand count = 0, empty list). There is no concept of an "uninitialized" aggregation.

Examples: count of services with demand=true, set of service IDs, max of some numeric value, any-true boolean, or a list of values from all sources.

### Event Channel

A declared channel between SM/port types for one-shot, discrete events. Events are not persistent and not aggregated — they are delivered once and forgotten.

Event channels are **directional in declaration** — the declaration specifies which side sends and which side receives. However, the **connectivity check is undirected**: the router validates that any edge exists between the two instances in either direction, regardless of which direction the edge points. This means the edge graph establishes "these two things are related" for event purposes, without requiring edges in both directions just to allow bidirectional event communication.

Events are for things that genuinely "happen" rather than things that "are": restart commands, timer firings, activation triggers.

Event channels are declared as a separate block in the `router!` macro and their payloads appear as variants in the receiving SM's input enum, giving exhaustive match coverage.

### Router

The central coordinator that maintains the edge graph and propagates signals. The router has **zero domain knowledge** — it does not know what "demand" or "readiness" means. It mechanically:

1. Maintains the set of edges between instances
2. When a signal changes, finds affected edges, re-aggregates, delivers updated inputs
3. When an SM updates its outgoing edges, adjusts the graph and re-aggregates for affected targets
4. When a port is added or removed, cleans up edges and re-aggregates
5. Delivers events along existing edges (checking connectivity in either direction)

The router is the single piece of infrastructure that, if correct, eliminates the entire class of synchronization bugs described in the motivation.

## Design Principles

### Unidirectional edges only

All edges flow in one direction. There is no concept of a bidirectional edge with "forward" and "backward" signal types. Where data needs to flow both ways between two SM types (e.g., demand from service to workload AND readiness from workload back to service), this is modeled as two separate edge types in opposite directions.

**Rationale:** Bidirectional edges require defining forward vs backward signals, ordering semantics for updates to both sides, and special handling for aggregation in each direction. Unidirectional edges are simpler — each edge has one signal type, one aggregator, one direction. The composition of two unidirectional edges achieves the same result with less conceptual overhead.

### Events require graph locality

An SM can only send events to instances it has an edge relationship with. The router rejects events where no edge exists in either direction between the sender and receiver.

**Rationale:** The edge graph becomes the single source of truth for "who can communicate with whom." This prevents stale references (sending events to SMs that you no longer have a relationship with) and makes the communication topology inspectable. Using either-direction connectivity avoids needing reverse edges solely for event delivery.

### Router has zero domain knowledge

The router does not contain any domain-specific logic. It does not know what services, workloads, or pods are. It only knows about SM instances, edges, signals, and aggregators.

**Rationale:** This separation means the router can be tested and verified independently of domain logic. It also means new SM types can be added without modifying the router.

### Signals are projected, not pushed

SMs do not "send" signal updates. They update their signal output value, and the router projects the new value to all consumers. The SM doesn't know or care who is consuming its signals.

**Rationale:** This eliminates the "notify all dependents" problem entirely. The SM updates its own state; the router handles fan-out.

### Edges are created by the source only

An edge is always created by its source side — the SM or port that the edge originates from. The target does not participate in edge creation. This applies to both external code (e.g., management port creating edges to SMs it manages) and SM handlers (e.g., workload creating `WorkloadToService` edges).

**Rationale:** Single-owner creation eliminates conflicts — there is never a question of "who created this edge" or "what happens if both sides try to create the same edge." The source owns the edge; the target only observes the resulting aggregated inputs.

### Runtime depth limiting

Signal propagation can trigger reactive edge changes, which trigger further signal propagation. Rather than statically proving the graph is acyclic, the router uses a runtime depth counter. The router emits a warning when propagation reaches depth N-1 and crashes at depth N (configurable, e.g., 16).

**Rationale:** In practice, propagation chains are short (3-4 steps). Static cycle prevention would add significant complexity to the type system for little practical benefit. A runtime depth limit catches bugs immediately during development and testing while keeping the system simple. The warning at N-1 provides observability before a crash, making it easier to diagnose topology issues.

### Management port is just a port

The management layer (which creates/destroys SMs, updates specs, sends admin commands) is modeled as port instances — the same primitive used for workers and fabric endpoints. Each managed SM gets its own management port instance, which produces the SM's spec as a regular instance-wide signal. Admin commands are events on the same edges.

**Rationale:** This means there is exactly one mechanism for delivering configuration and commands to SMs. No special "init with spec" vs "update spec" code paths. No separate "external command" delivery mechanism. Management ports are just another participant in the edge graph, using the same instance model as everything else.

## Propagation Model — Rounds

Signal propagation is organized into **rounds**. A round is the unit of execution — all cascading effects from a trigger are resolved within a single round before the system is quiescent again.

### Round execution

1. **Trigger.** An external action starts the round: a signal update, an edge change, a port addition/removal, or an event delivery.
2. **Aggregation.** The router identifies all (target SM, input) pairs affected by the trigger. For each, it runs the aggregator over the current edge/signal state and delivers the result to the SM handler.
3. **SM handler execution.** The SM handler runs and may update its own output signals and/or outgoing edges via the context.
4. **Cascade.** Signal updates and edge changes from step 3 feed back into step 2. The router processes these within the same round.
5. **Quiescence.** The round ends when no more updates propagate. If propagation depth exceeds the configured limit, the router crashes (with a warning at limit-1).

### Key properties

- **Multiple deliveries per round.** If multiple inputs to the same SM change in one round, the SM receives one delivery per changed input. There is no coalescing of multiple input changes into a single delivery.
- **No ordering guarantees between inputs.** If both `DemandInput` and `SpecInput` change for a workload in the same round, the SM cannot assume which it receives first. Each handler invocation should be self-contained.
- **Aggregators see latest state.** When an aggregator runs, it always operates on the current edge and signal state, including changes made earlier in the same round. There is no stale data within a round.
- **SM creation does not trigger a delivery.** When an SM is created, its aggregations over empty edge sets are well-defined (every aggregator handles zero inputs), but the router does not eagerly deliver these empty-set results. The SM's first delivery occurs when edges are established and the round processes the resulting aggregation. In practice, SM creation and initial edge setup happen in the same round, so the SM's first handler call already includes meaningful data.
- **Change detection via `PartialEq`.** The router compares signal values before and after an SM handler runs. Only signals whose value actually changed trigger downstream propagation. This deduplicates naturally within a round — if an SM sets a signal to its current value, nothing happens.

### Round-complete callback

After all input deliveries for a round have been processed for a given SM, the router calls a `round_complete` callback on the SM. This allows SMs to defer side-effects that depend on multiple inputs being current — for example, avoiding creating a pod when the spec hasn't arrived yet.

**Warning: this is an escape hatch, not a primary pattern.** SMs should be written so that handling each input independently converges to the correct state regardless of delivery order. This is the **lattice property**: any path through the state space should reach the same final state. If an SM's correctness depends on `round_complete`, that is a design smell — it means the SM is making decisions that are sensitive to intermediate states.

The lattice property is highly testable. Tools like stateright can explore all delivery orderings within a round and verify that the SM converges to the same outcome. SMs that rely on `round_complete` for correctness defeat this verification.

Use `round_complete` for optimization (avoid unnecessary intermediate work), not for correctness.

## Signal Invariants

### Concept

Signals can declare **invariants** — boolean expressions over the signal value that are expected to hold when propagation is complete. Transient violations during propagation are normal (intermediate states are inconsistent), but violations at quiescence indicate a problem in domain logic.

### Syntax

```rust
router! {
    signals {
        Pod::Healthy(bool),
        Pod::Retries(u32),
        Pod::Status(PodStatus),
    }
    invariants {
        Pod::Healthy(*value),          // bool — check truthiness
        Pod::Retries(*value < 5),      // comparison
        Pod::Status(value.is_ready()), // method call
    }
}
```

Each entry is `Node::Signal(expr)` where `expr` is any Rust expression. In generated code, `value: &SignalType` is bound, and the expression must evaluate to `bool`.

### Semantics

Invariants are checked once per `propagate()` call, after all rounds complete, before `PropagateEnd`. The router iterates all instances of signals with invariants and evaluates the expression. Multiple invariants on the same signal are allowed — each generates an independent check.

### Integration with tracing

Violations emit `TraceEvent::InvariantViolation` with the node type, instance ID, signal name, current value, and the stringified invariant expression. Different tracers handle this differently:

- **Test tracers** (`PanicTracer` + `RecordingTracer`): dump full causality traces so you can see what sequence of events led to the violated state.
- **Production tracers** (`RingTracer`): provide rolling history context — when a violation surfaces, the recent trace buffer shows the events leading up to it.
- **Custom tracers**: can log, alert, or integrate with monitoring systems.

### Design rationale

Invariants are modeled as a signal property rather than a separate mechanism because they reuse all existing signal machinery — per-instance storage, `PartialEq`/`Debug` bounds, and the BTreeMap iteration pattern. The invariant expression is simply a post-propagation check on existing state.

### Example

A workload's `Healthy` signal — during pod replacement it may temporarily be `false`, but by the time propagation completes, cascading effects should have restored it (new pod created, became running, readiness propagated back). If not, the invariant violation surfaces the problem with full trace context, showing exactly which handler failed to restore the expected state.

## Modeling Examples

### 1. Service demand aggregation

**Setup:** Three services (S1, S2, S3) each have a `ServiceToWorkload` edge pointing at workload W1. Each service produces a `ServiceDemand(bool)` signal.

**S2 activates (demand goes true):**
1. S2 updates its signal: `ServiceDemand(false)` -> `ServiceDemand(true)`
2. Router finds all `ServiceToWorkload` edges targeting W1
3. Aggregator runs: counts `demand=true` signals. S1=false, S2=true, S3=false -> count=1
4. Router delivers `ServicesChanged { demand_count: 1, service_ids: [S1, S2, S3] }` to W1
5. W1 handles the input, decides to activate, updates its outgoing edges accordingly

**S3 also activates:**
1. S3 updates its signal: `ServiceDemand(true)`
2. Router re-aggregates: S1=false, S2=true, S3=true -> count=2
3. Router delivers `ServicesChanged { demand_count: 2, service_ids: [S1, S2, S3] }` to W1
4. W1 sees demand increased, no action needed beyond updating internal state

**S2 deactivates:**
1. S2 updates its signal: `ServiceDemand(false)`
2. Router re-aggregates: count=1
3. W1 sees demand decreased, still > 0, stays active

### 2. Workload readiness — all services receive readiness

**Setup:** Services S1 and S2 each have a `ServiceToWorkload` edge to workload W1.

The aggregator for `ServiceToWorkload` always delivers the **full set** of service IDs to the workload, regardless of their demand state. The workload always targets readiness edges back at all of them. This means every service knows whether its backing workload is ready, even when the service is idle — useful for deciding whether an incoming activation can be served immediately or needs to wait for a boot.

**Edges established:**
1. S1 and S2 create `ServiceToWorkload` edges to W1 (based on their specs)
2. Router aggregates and delivers to W1: `ServicesChanged { demand_count: 0, service_ids: [S1, S2] }`
3. W1's handler targets readiness edges at the full service set: `ctx.set_edges::<WorkloadToService>(vec![S1, S2])`
4. Router projects W1's `WorkloadReadiness` signal (currently `None`) to both S1 and S2
5. Both services know their backend isn't ready yet

**W1's pod starts running:**
1. W1 updates signal: `WorkloadReadiness(Some(ReadyInfo { pod_id, worker_id }))`
2. Router finds all `WorkloadToService` edges from W1 (S1 and S2)
3. Router delivers `BackendReadiness { ready: Some(...) }` to both S1 and S2
4. Both services know the backend is ready — whether or not they have active demand

**New service S3 is added, pointing at W1:**
1. S3 creates a `ServiceToWorkload` edge to W1
2. Router re-aggregates: `ServicesChanged { demand_count: 0, service_ids: [S1, S2, S3] }`
3. W1's handler updates edges: `ctx.set_edges::<WorkloadToService>(vec![S1, S2, S3])`
4. Router projects current readiness to S3 — S3 is immediately up to date

No cascading, no conditional edge creation. The full service set always gets readiness. Demand is just a field in the aggregation, not a filter on who participates.

### 3. Worker loss — port removal

**Setup:** Pod P1 has a `PodOnWorker` edge to worker port WK1. WK1 produces `WorkerInfo { ... }`.

**Worker connection drops:**
1. External code removes the WK1 port instance
2. Router removes all edges to/from WK1, including the `PodOnWorker` edge from P1
3. Router re-aggregates P1's incoming worker signals: now empty
4. Router delivers `WorkerStatus { info: None }` to P1
5. P1 handles the loss — same code path as "never had a worker," no special "WorkerLost" event

**Contrast with today:** Currently, worker loss triggers a `WorkerLost` event that must be propagated to every affected SM. Each SM has a separate handler for this event, and missing one means stale state. With port removal, the router handles it mechanically — every SM with an edge to that port sees the retraction automatically.

### 4. Spec update via management port

**Setup:** Management port instance M1 has an edge to workload W1. M1 produces a `WorkloadSpec` signal.

**Namespace spec update changes W1's config:**
1. M1 updates its signal: `WorkloadSpec(old)` -> `WorkloadSpec(new)`
2. Router projects the new spec to W1
3. W1 receives `SpecChanged { spec: new }` as an aggregated input
4. W1 handles the new spec (may need to restart pods, update config, etc.)

**New service S2 is added to the namespace:**
1. External code creates S2 SM instance
2. External code creates a new management port instance M2 with `ServiceSpec(spec)` signal
3. External code creates edge from M2 to S2
4. Router projects the spec to S2 — this is S2's "initialization"
5. S2 creates its `ServiceToWorkload` edge based on the spec
6. Router propagates S2's signals to the target workload

There is no separate "create and initialize" code path. SM creation is: create instance, create management port instance with spec signal, create edge, let signals flow.

### 5. Pod placement

**Setup:** Workload W1 decides it needs a pod running.

**Pod request and placement:**
1. W1 creates pod SM P1 and a `WorkloadToPod` edge to it (carrying `PodSpec` signal)
2. Router projects `PodSpec` to P1 — P1 now knows what it should run
3. P1 emits a `NeedCapacity` output (or the router/scheduler notices an unplaced pod)
4. External scheduler selects worker WK1, creates `WorkerToPod` edge from WK1 to P1
5. Router projects `WorkerInfo` signal from WK1 to P1
6. P1 receives `WorkerAssigned { info: Some(...) }` — proceeds with launching
7. P1 updates its `PodStatus` signal: `Launching` -> `Running`
8. P1's `PodToWorkload` edge carries this status back to W1
9. W1 receives `PodStatusChanged { ... }` — knows its pod is running

**Worker dies while pod is running:**
1. WK1 port removed
2. `WorkerToPod` edge severed
3. P1 receives `WorkerAssigned { info: None }`
4. P1 updates `PodStatus` signal to reflect loss
5. W1 receives updated pod status, decides what to do (retry, give up, etc.)

The pod SM never stores "which worker am I on" as internal state. It's always a projected signal.

### 6. Admin restart command (event)

**Setup:** Management port instance M1 has an edge to workload W1.

**Admin issues restart:**
1. Management port sends event: `ctx.send_event::<AdminCommand>(W1, AdminCommand::Restart)`
2. Router verifies an edge exists between M1 and W1 in either direction (it does — the spec edge from M1 to W1)
3. Router delivers `AdminCommand::Restart` to W1
4. W1 handles the restart — tears down current pod, starts fresh

If W1 had already been deleted (edge removed), the router would reject the event delivery.

## Edge Lifecycle

### Creation

Edges are always created by their source — the SM or port that the edge originates from:
- **External code** — when setting up initial topology (management port edges, worker port edges)
- **SM handlers** — reactively, in response to an aggregated signal input (`ctx.set_edges::<T>(...)`)

The target never creates incoming edges. When an edge is created, the router re-aggregates the target's affected input (in the current or next round) using the source's current signal value. This means the target always receives a consistent view — no "you missed updates that happened before the edge existed" problem.

### Destruction and dangling edges

Edges are explicitly destroyed when:
- **SM handlers** update outgoing edges (edges not in the new set are removed)
- **Port removal** removes all edges from/to the port
- **SM/port removal (source side)** removes all outgoing edges from the dying node

When an outgoing edge is removed, the router re-aggregates for the target without the removed edge's signal. If this was the last edge of that type, the target receives the aggregator's "empty" value (e.g., demand count = 0, empty list).

**Target death — dangling edges.** When an SM dies, its *incoming* edges (where it is the target) are **not** cleaned up. They remain as dangling edges. This is well-defined and harmless: signals flow source→target, so a dead target simply means the signal goes nowhere. The source is never explicitly notified of target death — it discovers the change reactively through its own aggregated inputs (e.g., the management port or spec change that caused the target's removal will eventually cause the source to update its edges). Setting an edge to a dead SM is also a well-defined no-op.

This asymmetry follows from source ownership: the source created the edge, the source is responsible for updating or removing it. The router does not walk incoming edges on SM death.

### Reactive edge creation (the key pattern)

The most important pattern in the system: an SM updates its outgoing edges in response to an aggregated signal input.

```
Signal change on A
  -> Router aggregates and delivers to B
  -> B's handler calls ctx.set_edges::<T>(new_set)
  -> Router adjusts edges from B, projects signals to new targets
  -> New targets receive aggregated inputs
  -> (may trigger further reactive updates, bounded by depth limit)
```

This is how the workload creates readiness edges to services, how pods get placed on workers, and how the graph evolves in response to state changes — all without any central orchestration logic.

## Open Questions

- **Client subscriptions.** Signals naturally support a subscription model — a client session could be a port with edges to SMs it cares about, receiving signal updates through the same mechanism. Worth exploring but not needed for the initial implementation.

## Rust Implementation

The signal router is implemented as a framework with a central `router!` macro that declares the full topology — SM types, ports, output signals, edges, aggregated inputs, and event channels. The macro has full visibility over the topology and generates input enums, aggregator wiring, and router dispatch code.

### Central topology declaration

```rust
router! {
    state_machines {
        ServiceSm,
        WorkloadSm,
        PodSm,
    }

    ports {
        WorkerPort,
        ManagementPort,
        FabricPort,
    }

    // Output signals — what each SM/port produces (no target specified)
    // Signal types must implement PartialEq for change detection.
    signals {
        ServiceSm::Demand(bool),
        WorkloadSm::Readiness(Option<ReadyInfo>),
        WorkloadSm::PodSpec(PodSpecData),
        PodSm::Status(PodStatus),
        WorkerPort::Info(WorkerInfo),
        FabricPort::ActiveFlow(bool),
        ManagementPort::WorkloadSpec(WorkloadSpec),
        ManagementPort::ServiceSpec(ServiceSpec),
    }

    // Structural relationships between instance types
    edges {
        ServiceToWorkload: ServiceSm -> WorkloadSm,
        WorkloadToService: WorkloadSm -> ServiceSm,
        WorkloadToPod: WorkloadSm -> PodSm,
        PodToWorkload: PodSm -> WorkloadSm,
        WorkerToPod: WorkerPort -> PodSm,
        FabricToWorkload: FabricPort -> WorkloadSm,
        ManagementToWorkload: ManagementPort -> WorkloadSm,
        ManagementToService: ManagementPort -> ServiceSm,
    }

    // Event channels — directional in declaration, but the connectivity
    // check uses any edge in either direction between the two instances.
    // Event payloads appear as variants in the receiving SM's input enum.
    events {
        AdminCommand(AdminCommandPayload): ManagementPort -> WorkloadSm,
    }

    // Aggregated inputs — what each SM consumes, from where, and how.
    // Each source is an (EdgeType, Signal) pair — the macro validates that
    // the edge's source type actually produces the referenced signal.
    // For multi-source inputs, the macro generates an enum with one
    // variant per source pair.
    inputs {
        // Workload demand: aggregated from services AND fabric
        WorkloadSm::DemandInput {
            sources: [
                (ServiceToWorkload, ServiceSm::Demand),
                (FabricToWorkload, FabricPort::ActiveFlow),
            ],
            aggregator: DemandAggregator,
        },
        // Workload spec: from management port
        WorkloadSm::SpecInput {
            sources: [(ManagementToWorkload, ManagementPort::WorkloadSpec)],
            aggregator: ListAggregator,
        },
        // Workload pod statuses: from all pods
        WorkloadSm::PodStatusInput {
            sources: [(PodToWorkload, PodSm::Status)],
            aggregator: PodStatusAggregator,
        },
        // Service readiness: from backing workload
        ServiceSm::ReadinessInput {
            sources: [(WorkloadToService, WorkloadSm::Readiness)],
            aggregator: ListAggregator,
        },
        // Service spec: from management port
        ServiceSm::SpecInput {
            sources: [(ManagementToService, ManagementPort::ServiceSpec)],
            aggregator: ListAggregator,
        },
        // Pod spec: from workload
        PodSm::SpecInput {
            sources: [(WorkloadToPod, WorkloadSm::PodSpec)],
            aggregator: ListAggregator,
        },
        // Pod worker info: from worker port
        PodSm::WorkerInput {
            sources: [(WorkerToPod, WorkerPort::Info)],
            aggregator: ListAggregator,
        },
    }
}
```

### Generated input enums

The macro generates a typed input enum for each SM, with one variant per declared aggregated input and one variant per received event channel. This gives exhaustive match checking — adding a new input or event to an SM produces a compile error until the handler covers it.

```rust
// Generated by the macro
enum WorkloadSmInput {
    DemandInput(DemandAggregated),        // custom aggregator
    SpecInput(Vec<WorkloadSpec>),          // ListAggregator — SM expects len 0..1
    PodStatusInput(PodStatusAggregated),   // custom aggregator
    AdminCommand(AdminCommandPayload),     // event channel
}

enum ServiceSmInput {
    ReadinessInput(Vec<Option<ReadyInfo>>), // ListAggregator — SM expects len 0..1
    SpecInput(Vec<ServiceSpec>),            // ListAggregator — SM expects len 0..1
}

enum PodSmInput {
    SpecInput(Vec<PodSpecData>),           // ListAggregator — SM expects len 0..1
    WorkerInput(Vec<WorkerInfo>),          // ListAggregator — SM expects len 0..1
}
```

### Generated aggregator input enums

For aggregated inputs with multiple source pairs, the macro generates an enum with one variant per source pair. The aggregator receives a slice of this enum.

```rust
// Generated for WorkloadSm::DemandInput which has two source pairs
enum DemandInputSource {
    ServiceDemand(ServiceSmId, bool),      // from (ServiceToWorkload, ServiceSm::Demand)
    FabricFlow(FabricPortId, bool),        // from (FabricToWorkload, FabricPort::ActiveFlow)
}
```

### Aggregator trait

Aggregators are normal Rust trait implementations. For multi-source inputs, they receive the generated source enum. For single-source inputs, they receive the signal values directly.

```rust
trait Aggregator {
    type Input;
    type Output;
    fn aggregate(&self, inputs: &[Self::Input]) -> Self::Output;
}

// Custom aggregator: combine demand from services and fabric
struct DemandAggregator;
impl Aggregator for DemandAggregator {
    type Input = DemandInputSource;
    type Output = DemandAggregated;

    fn aggregate(&self, inputs: &[DemandInputSource]) -> DemandAggregated {
        let mut demand_count = 0u32;
        let mut service_ids = Vec::new();

        for input in inputs {
            match input {
                DemandInputSource::ServiceDemand(id, demand) => {
                    service_ids.push(*id);
                    if *demand { demand_count += 1; }
                }
                DemandInputSource::FabricFlow(_, active) => {
                    if *active { demand_count += 1; }
                }
            }
        }

        DemandAggregated { demand_count, service_ids }
    }
}

// Built-in aggregator: collects all signal values into a Vec.
// SMs enforce cardinality invariants themselves (e.g., expect len 0..1).
struct ListAggregator;
```

### SM handler

The SM implements a handler over its generated input enum. The handler can update output signals and outgoing edges via the context. An optional `round_complete` callback is available for deferring side-effects.

```rust
impl StateMachine for WorkloadSm {
    type Input = WorkloadSmInput;  // generated enum

    fn handle(&mut self, input: Self::Input, ctx: &mut Ctx) {
        match input {
            WorkloadSmInput::DemandInput(demand) => {
                // Retarget readiness edges to full service set
                ctx.set_edges::<WorkloadToService>(
                    demand.service_ids.iter().copied()
                );

                // Update internal demand state
                self.handle_demand(demand.demand_count, ctx);
            }
            WorkloadSmInput::SpecInput(specs) => {
                // Expect exactly one management edge; handle 0 (no spec yet)
                if let Some(spec) = specs.into_iter().next() {
                    ctx.set_signal(WorkloadSm::PodSpec(spec.into()));
                }
            }
            WorkloadSmInput::PodStatusInput(statuses) => {
                // React to pod status changes
                self.handle_pod_statuses(statuses, ctx);
            }
            WorkloadSmInput::AdminCommand(cmd) => {
                // Handle admin commands
                self.handle_admin_command(cmd, ctx);
            }
        }
    }

    // Optional — called after all input deliveries for this SM in a round.
    // Use for optimization (avoiding intermediate work), NOT for correctness.
    // If your SM's correctness depends on this, that's a design smell.
    fn round_complete(&mut self, ctx: &mut Ctx) {
        // e.g., now apply deferred pod creation decisions
    }
}
```

### Key properties of this approach

- **Single source of truth.** The `router!` macro declares the full topology in one place — SM types, signals, edges, event channels, and aggregated inputs. Easy to review, easy to validate at compile time.
- **Compile-time exhaustiveness.** Generated input enums ensure every SM handles all its inputs and events. Adding a new input, signal source, or event channel produces a compile error until handled.
- **Multi-source aggregation is first-class.** An aggregated input can pull from multiple edge types and signal types in one declaration. The macro generates a source enum so the aggregator can distinguish origins.
- **Aggregators are normal code.** Fully testable in isolation, no macro magic.
- **Router stays generic.** The router implementation is domain-agnostic. It tracks edges, detects signal changes, calls aggregators, delivers inputs. The `router!` macro generates the glue that connects domain types to the generic router.
- **Macro validation.** The `(EdgeType, Signal)` source pairs are validated at compile time — each signal must be produced by the source type of the paired edge. E.g., `(ServiceToWorkload, ServiceSm::Demand)` is valid because `ServiceToWorkload` originates from `ServiceSm` which produces `Demand`.
- **Uniform instance model.** Both SMs and ports use the same instance model — each instance has its own identity, signal values, and edges. No special per-edge signal storage or singleton port semantics.
