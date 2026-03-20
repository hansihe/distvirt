# SM Subsystem (Signal Router)

The orchestrator's core logic lives here as interconnected state machines driven by `distvirt-sm-router`. See `distvirt-sm-router/src/lib.rs` for full framework docs.

## Core Concepts

**Signal Router** — a framework where SMs communicate through typed signals, edges, and events. The router maintains the connectivity graph and propagates changes. SMs contain only domain logic.

- **Signal**: Persistent current-value output per SM instance. Set via `ctx.set_{signal}()`. Uses `PartialEq` to suppress no-op updates. Starts at `Default::default()`.
- **Edge**: Unidirectional typed relationship between instances. Set via `ctx.set_{edge}_edges()` with set semantics (replaces entire edge set).
- **Event**: One-shot discrete message between connected instances. Requires an edge instance at runtime.
- **Aggregated Input**: Collects signal values from connected instances via edges. Two modes:
  - **Batch** (`Aggregator`): All values → `aggregate()` → single output, `PartialEq`-suppressed.
  - **Incremental** (`IncrementalAggregator`): Per-source `added/removed/changed` diffs, no suppression. Used for port-facing inputs.
- **Port**: External boundary (no handler). Driven by application code. Bridges router to async I/O.

## Runtime Semantics

1. All `ctx` effects are **buffered** and applied atomically after handler returns.
2. `propagate()` resolves all cascading effects: dirty inputs → re-aggregate → deliver → repeat until quiescence.
3. Within a sub-round, SM handlers are **independent** — no handler observes another's effects in the same sub-round.
4. Apply order: creates → signals → edges → events → self-destruct.

## Topology (mod.rs)

**State Machines**: `Service`, `Workload`, `Pod` (auto-id), `Endpoint` (auto-id)

**Ports** (11): `Worker`, `EndpointDemand`, `Management`, `Timer`, `ScheduleRequest`, `ScheduleLease`, `FabricEndpoint`, `DnsRegistry`, `Artifact`, `WireGuardPeer`, `Observability`

## SM Roles

- **Service** (`service.rs`): Thin wrapper. Owns an Endpoint, relays `EndpointConfig` from management spec. Forwards activation events.
- **Workload** (`workload.rs`): Core lifecycle manager. Creates/destroys Pods based on demand + spec. Handles retry/backoff, spec versioning, artifact suspend/resume, committed-to-boot guards.
- **Pod** (`pod.rs`): Individual pod lifecycle: `Pending → Running → Suspending → Suspended/Failed/Finished/Displaced`. Self-destructs when terminal + no owner. Manages lease, timer, schedule request signals.
- **Endpoint** (`endpoint.rs`): Traffic demand + idle timeout + activation. Derives state from (demand, readiness). Emits endpoint info for fabric/DNS.

## Key Patterns

- **Ownership = lifecycle**: Removing ownership edge drives child to terminal. Child self-destructs when terminal AND no owner (reaping).
- **Committed-to-boot**: Prevents demand fluctuation from killing a launching pod.
- **Spec versioning**: `spec_version` counter detects stale pods on Running → restart.
- **Generation counters**: Timers carry generation to distinguish fresh from stale fires.
- **Singleton ports**: `TIMER`, `SCHEDULE_REQUEST`, etc. are constants. SMs connect to them in `initialize`.

## Handler Pattern

```rust
impl<C: WorkloadCtx> SmHandler<C> for WorkloadSm {
    type Input = WorkloadInput;
    fn initialize(&mut self, ctx: &mut C) {
        ctx.set_workload_timers_edges(vec![TIMER]);
    }
    fn handle(&mut self, input: Self::Input, ctx: &mut C) {
        match input {
            WorkloadInput::SpecInput(spec) => { /* ... */ }
            WorkloadInput::DemandInput(info) => { /* ... */ }
            // ...
        }
    }
}
```

## Tests

- **Behavioral tests**: `basic.rs`, `multi.rs`, `retry.rs`, `suspend.rs`, `transitions.rs`, `misc.rs`, `endpoint_idle.rs`, `service_idle.rs`
- **Stateright model tests**: `stateright_{pod,workload,service,endpoint}.rs` — exhaustive state space exploration
- **Helpers** in `tests/mod.rs`: `setup_workload_with_pending_pod()`, `make_pod_running()`, `schedule_pod()`, etc.
