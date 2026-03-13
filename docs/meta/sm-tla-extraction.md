---
title: "State Machine TLA+ Extraction"
---

## Motivation

The orchestrator's core logic is implemented as state machines (workload, pod, service). We already use [stateright](https://github.com/stateright/stateright) for bounded model checking of these SMs, which has caught real bugs.

But stateright has limits:

- **Bounded exploration.** Tests run to `max_steps`. Bugs that require longer traces are invisible.
- **No temporal logic.** Liveness properties ("if demand exists, a pod eventually runs") require fairness conditions. Stateright's `sometimes` is a weaker approximation.
- **Composition is manual.** The namespace-level stateright model (`stateright_model.rs`) manually mirrors SM state, tracks timers, generates actions. It's ~800 lines of hand-written model harness. Adding a new SM or property means maintaining this in parallel.
- **No proofs.** Stateright explores states; it doesn't prove invariants hold for all possible behaviors.

TLA+ (with TLC for model checking and TLAPS for proofs) addresses all of these. The question is how to maintain both a runnable Rust implementation and a TLA+ specification without them diverging.

The answer: **extract TLA+ modules directly from the Rust implementation.** Write SMs in a constrained subset of Rust, and generate TLA+ mechanically. The Rust code *is* the spec.

## Architecture: Uniform State Machines with Message Passing

### The SM Trait

Rather than a single `step` function that matches on an `Inbox` enum, the build step generates a **handler trait** with one method per inbound message type. The SM implementor provides a handler for each message it needs to handle:

```rust
// Generated from #[sm_message(to = WorkloadSm)] declarations
trait WorkloadSmHandlers {
    type State;
    fn handle_set_demand(state: Self::State, msg: SetDemand, ctx: &mut SmContext) -> Self::State;
    fn handle_force_deactivate(state: Self::State, msg: ForceDeactivate, ctx: &mut SmContext) -> Self::State;
}
```

Each handler is a self-contained extractable function — match on state internally, return new state. The extractor processes handlers independently, each producing one TLA+ operator.

Key design choices:

- **`State` by value, not `&mut self`.** Handlers take ownership of the old state and return the new state. This eliminates `mem::replace`, the `Transitioning` sentinel, and mutable borrows into sub-state. It maps directly to TLA+'s `state' = ...`.
- **Side effects through `ctx` only.** `ctx.send(target, msg)` is the sole mechanism for interacting with other SMs. No direct function calls between SMs.
- **One handler per message type.** No `Inbox` enum, no compound `(state, msg)` match. Each handler matches on state independently, using the full power of or-patterns and guards. The compiler enforces completeness — every message routed to this SM must have a handler. Irrelevant (state, message) combinations use a `_ => state` catch-all arm in the handler body.

### Hierarchical State

SM state can use nested enums to group related variants. This avoids the combinatorial explosion that comes from flattening all sub-states to the top level, while remaining straightforwardly extractable.

```rust
#[derive(Extractable)]
struct WorkloadSmState {
    phase: WorkloadPhase,
    current_demand: u32,
    suspend_on_idle: bool,
    consecutive_failures: u32,
    max_retries: u32,
    needs_successful_boot: bool,
    retiring: SmSet<PodSmId>,
}

#[derive(Extractable)]
enum WorkloadPhase {
    Dormant,
    WaitingForCapacity,
    Active { sub: ActiveSub, pod_id: PodSmId, worker_id: WorkerId },
    Suspended { artifact_id: ArtifactId },
    RetryBackoff { backoff_timer: TimerKey },
    Failed,
}

#[derive(Extractable)]
enum ActiveSub {
    Launching { pending: PendingIntent },
    Running,
    Suspending { pending: PendingIntent },
    Resuming { pending: PendingIntent },
}
```

This lets handler match arms operate at the right level of specificity:

```rust
fn handle_worker_lost(state: WorkloadSmState, msg: WorkerLost, ctx: &mut SmContext) -> WorkloadSmState {
    match state.phase {
        // Matches ANY active sub-state (one arm instead of four)
        WorkloadPhase::Active { pod_id, worker_id, .. }
            if worker_id == msg.worker_id => { ... }
        _ => state,
    }
}

fn handle_force_deactivate(state: WorkloadSmState, msg: ForceDeactivate, ctx: &mut SmContext) -> WorkloadSmState {
    match state.phase {
        // Matches only transitioning sub-states via or-pattern
        WorkloadPhase::Active { sub: ActiveSub::Launching { pending }
                                   | ActiveSub::Suspending { pending, .. }
                                   | ActiveSub::Resuming { pending, .. }, .. } => {
            // upgrade pending
        }
        _ => state,
    }
}
```

In TLA+, this is nested record access: `state.phase.type = "Active" /\ state.phase.sub.type = "Launching"`. No special extraction logic needed — the extractor handles nested `#[derive(Extractable)]` enums uniformly.

### All SMs Are Peers

There is no nesting. The current pod SM lives inside the workload SM (called synchronously via `PodSlot::step()`). In this model, the pod SM is a peer — a separate SM instance that communicates with its workload SM through messages.

```
Namespace SM(s)  <-->  Workload SM  <-->  Pod SM
                       Service SM(s)
```

This uniformity means:

- One extraction path: every SM produces the same TLA+ structure (a process with state and an inbox).
- Composition is modeled naturally: TLA+ processes communicating through message channels.
- Any SM can message any other SM (if the routing is declared), enabling flexible architectures without special-casing parent/child relationships.

### SM Identity and Lifecycle

Each SM type has a typed ID (`PodSmId`, `WorkloadSmId`, etc.). SMs reference each other by ID in their state — these are the "addresses" for message passing.

For TLC model checking, SM instances come from a bounded, pre-allocated pool per type. "Spawning" an SM means sending an `Init` message to an idle instance. This avoids dynamic process creation, which TLC handles poorly.

In the Rust runtime, the pool can be dynamic — the extraction boundary only requires the pool to be bounded for verification.

## Message Routing

### Shared Message Types

Rather than each SM defining separate input/output enums with manual conversion between them, messages are standalone types with routing metadata:

```rust
#[sm_message(from = WorkloadSm, to = PodSm, route_by = pod_id)]
struct StopPod {
    pod_id: PodSmId,
    reason: StopReason,
}

#[sm_message(from = PlacementSm, to = WorkloadSm, route_by = workload_id)]
struct ResumePod {
    workload_id: WorkloadSmId,
    worker_id: WorkerId,
    pod_id: PodId,
    artifact_id: ArtifactId,
}

#[sm_message(from = EndpointSm, to = WorkerSm, broadcast)]
struct EndpointSync {
    endpoints: Vec<Endpoint>,
}
```

The `#[sm_message]` attribute declares:

- **`from`**: which SM type can emit this message.
- **`to`**: which SM type receives it.
- **`route_by`**: which field identifies the target instance. For singletons or broadcasts, use `singleton` or `broadcast` instead.

### Code Generation

A build step (not a proc macro — cross-module aggregation requires it) collects all `#[sm_message]` declarations and generates:

- **Handler trait per SM type.** For each SM, a trait with one method per message type where `to = ThatSm`.
- **Routing dispatch.** A function that takes an SM's output message, extracts the routing key, and delivers to the correct handler on the target SM.
- **TLA+ channel declarations.** For each `(from, to)` pair, a typed message channel in the TLA+ spec.

This means:

- Adding a new message = one struct + one attribute. The handler trait, routing, and TLA+ channels update automatically.
- The routing topology is a static, inspectable graph — useful for both documentation and verification.
- No manual conversion code between output and input types. The message struct is the same at both ends.

### Routing Patterns

| Pattern | Attribute | Example |
|---------|-----------|---------|
| Direct (known target) | `route_by = field` | Workload sends `StopPod` to a specific pod |
| Singleton | `singleton` | Service sends `DemandChanged` to the (single) demand reconciliation SM |
| Broadcast | `broadcast` | Endpoint SM sends `EndpointSync` to all worker SMs |
| Via intermediary | Two messages | Workload sends `ResumeRequest` to placement SM, placement SM resolves and sends `ResumePod` back |

Conditional routing (where the target depends on state or lookups) is handled by routing through an intermediary SM. The sender emits a request; the intermediary enriches and forwards. This is explicit in the message graph and visible in the TLA+ spec.

### Dispatch Layer

The generated dispatch layer takes the next message from the inbox and calls the appropriate handler method. Every handler returns `State` — the message is always consumed.

```rust
// Generated — user never writes or sees this
fn dispatch(
    sm: &impl WorkloadSmHandlers,
    state: WorkloadSmState,
    inbox: &mut VecDeque<InboxMsg>,
    ctx: &mut SmContext,
) -> WorkloadSmState {
    let Some(msg) = inbox.pop_front() else {
        return state; // empty inbox, no progress this tick
    };
    match msg {
        InboxMsg::SetDemand(msg) => sm.handle_set_demand(state, msg, ctx),
        InboxMsg::PodBecameRunning(msg) => sm.handle_pod_became_running(state, msg, ctx),
        // ... one branch per message type
    }
}
```

This has several benefits:

- **User writes zero boilerplate.** No `Inbox` enum matching. Each handler is purely transition logic for one message type, with a `_ => state` catch-all for irrelevant states.
- **Simple dispatch.** Messages are consumed in FIFO order. No inbox scanning or reordering.
- **Trivial extraction.** Each handler method is independently extractable — one method becomes one TLA+ operator. No need to decompose a compound match.

**TLA+ mapping:** Each handler becomes a TLA+ operator. A handler that returns state unchanged (catch-all arm) maps to an enabled action with `UNCHANGED state` that consumes the message.

**Compile-time validation:** Every message type routed to this SM must have a handler implementation. Guaranteed by the trait system — a missing handler is a compile error.

## Extractable Type System

The extractor only supports a fixed set of Rust types, each with a known TLA+ mapping:

| Rust | TLA+ |
|------|------|
| `bool` | `BOOLEAN` |
| `u32`, `u64`, `i32`, `i64` | `Nat` / `Int` |
| `String` | `STRING` |
| Enum (no data) | Model value / constant set |
| Enum (with data) | Tagged record: `[type \|-> "Variant", field \|-> ...]` |
| `Option<T>` | `T \union {None}` |
| `SmMap<K, V>` | Function `[K -> V]` (partial: `[K -> V \union {None}]`) |
| `SmSet<T>` | Set `SUBSET T` |
| `Vec<T>` | Sequence `Seq(T)` |
| SM ID types | Values from a bounded model set |

State structs use `#[derive(Extractable)]` which validates at compile time that all fields use supported types. Unsupported types are a compile error, not a runtime surprise.

#### Collection Wrapper Types

Standard library collections (`BTreeMap`, `BTreeSet`) have mutable APIs that don't fit value semantics — e.g., `BTreeSet::insert` mutates in place and returns `bool`. The framework provides wrapper types with value-returning methods:

```rust
impl<T> SmSet<T> {
    fn contains(&self, x: &T) -> bool;       // x ∈ S
    fn insert(self, x: T) -> SmSet<T>;       // S ∪ {x}
    fn remove(self, x: &T) -> SmSet<T>;      // S \ {x}
    fn filter(self, pred: fn(&T) -> bool) -> SmSet<T>;  // {x ∈ S : P(x)}
    fn is_empty(&self) -> bool;              // S = {}
    fn len(&self) -> u32;                    // Cardinality(S)
}

impl<K, V> SmMap<K, V> {
    fn get(&self, k: &K) -> Option<&V>;      // map[k] if k ∈ DOMAIN map
    fn contains_key(&self, k: &K) -> bool;   // k ∈ DOMAIN map
    fn insert(self, k: K, v: V) -> SmMap<K, V>;  // [map EXCEPT ![k] = v]
    fn remove(self, k: &K) -> SmMap<K, V>;   // domain restriction
}
```

Each method has a direct TLA+ translation. The `filter` predicate must itself be extractable (simple boolean expression over fields). At runtime, these delegate to standard collections; the wrapper exists to enforce value semantics and extractability.

### Extractable Rust Subset

Handler methods and any helper functions they call must use only constructs with known TLA+ translations. This is a "Rust-surface DSL" — valid Rust that happens to be mechanically extractable.

**Control flow:**

- `match` with nested `match` expressions
- `match` guards (`if expr`)
- Or-patterns in match arms (`Variant::A { x, .. } | Variant::B { x, .. }`)
- `if`/`else` expressions (→ TLA+ `IF/THEN/ELSE`)
- `let` bindings for intermediate values

**Expressions:**

- Field access (`state.field`)
- Comparison operators (`==`, `!=`, `>`, `>=`, `<`, `<=`)
- Boolean operators (`&&`, `||`, `!`)
- Integer arithmetic (`+`, `-`, `*`; saturating preferred)
- Struct construction, including struct update syntax (`MyStruct { field: val, ..old }` → TLA+ `[old EXCEPT !.field = val]`)
- Enum variant construction

**Side effects:**

- `ctx.send(target, msg)` for message passing
- `ctx.self_id()` for the current SM's ID

**Helper functions:**

Extractable helper functions may be called from handler methods but not from each other (call depth is limited to 1). They can use `ctx.send()` and return values. This is essential — the workload SM has helpers like `transition_on_demand` called from ~8 different handlers.

The extractor handles helpers by **inlining** them at each call site in the generated TLA+. At each call site, the helper body is substituted with arguments bound, and any `ctx.send()` calls within the helper merge with the calling handler's sends. This is straightforward because depth-1 means there's no recursive inlining — each helper body is a self-contained decision tree. The depth-1 limit is enforced by the `#[extractable_fn]` proc macro.

```rust
// Extractable helper — compiles to TLA+ operator
fn transition_on_demand(
    current_demand: u32,
    consecutive_failures: u32,
    max_retries: u32,
    needs_successful_boot: bool,
    ctx: &mut SmContext,
) -> WorkloadPhase {
    if current_demand > 0 || needs_successful_boot {
        if consecutive_failures >= max_retries {
            WorkloadPhase::Failed
        } else if consecutive_failures > 0 {
            let timer_key = TimerKey::RetryBackoffTimeout { workload_id: ctx.self_id() };
            ctx.send(timer_sm, TimerSet { timer_key });
            WorkloadPhase::RetryBackoff { backoff_timer: timer_key }
        } else {
            ctx.send(placement_sm, PodRequest { workload_id: ctx.self_id() });
            WorkloadPhase::WaitingForCapacity
        }
    } else {
        WorkloadPhase::Dormant
    }
}
```

**Compile-time validation of handler bodies:**

Each handler method (and any helper it calls) is wrapped in a proc macro attribute (e.g., `#[extractable_fn]`) that validates the body at compile time against the extractable subset. Non-extractable constructs — loops, closures, unsupported method calls, early returns, etc. — produce compile errors with spans pointing at the offending expression. This catches violations immediately, not at a later extraction step.

**Explicitly disallowed:**

No loops, closures, trait objects, heap allocation, iterators, `&mut` borrows, method calls on arbitrary types, string formatting, or early returns. The body is a decision tree: match → emit → return new state.

### Observability Fields

Some SM state is purely for observability and doesn't affect transitions — e.g., `conditions: BTreeMap<String, String>` for human-readable status, `last_failure_reason: Option<String>` for diagnostics. These fields:

- Exist in the Rust implementation but are excluded from the TLA+ extraction
- Are marked with `#[not_extractable]` (or similar) on the struct field
- May use non-extractable types (`String`, `format!()`, etc.)
- Must not appear in `match` guards or transition logic (enforced at compile time)

## Namespace Decomposition

The current namespace layer (`src/namespace/`) is a monolith that handles routing, reconciliation, effect translation, endpoint management, and more. In the uniform SM model, it decomposes into several independent singleton SMs:

### Proposed SM Breakdown

**Demand Reconciliation SM** — Receives demand signals from service SMs (`DemandChanged`), computes effective demand per workload, sends `SetDemand` to workload SMs. Currently lives in `reconciliation.rs`.

**Readiness SM** — Receives `BecameReady` / `BecameUnready` from workload SMs, forwards `WorkloadReady` / `WorkloadUnready` to service SMs. Handles the activation-service re-activation logic (preserving demand through recovery). Currently spread across `reconciliation.rs` and `output.rs`.

**Placement SM** — Tracks artifact locations (the placement table). Receives `SuspendRequest` from workload SMs, resolves pool/worker, generates artifact IDs (keeping this non-extractable concern out of the workload SM), either forwards the suspend command or sends back `PodSuspendFailed`. Handles `ResumeRequest` → `ResumePod` resolution. Currently in `output.rs`.

**Endpoint SM** — Computes endpoint state from pod map + service backends + network config, broadcasts to worker SMs on changes. Currently in `mod.rs`.

**WireGuard Peer SM** — Manages peer IP allocation, handles connect/disconnect, broadcasts peer updates. Currently in `wireguard.rs`.

**Namespace Lifecycle SM** — Manages the `Creating → Active → Destroying` lifecycle, worker tracking, spec updates. The "parent" that spawns workload/service SMs on spec changes.

### What This Gains

Each of these SMs is small (3-5 states, handful of message types), independently testable, and independently extractable to TLA+. The value of TLA+ is then proving properties about their *composition*:

- "If all services drop demand, the workload eventually suspends" (liveness under fairness)
- "A workload never receives `ResumePod` for a deleted artifact" (safety across placement + workload SMs)
- "Endpoint state eventually converges after a pod migration" (convergence across endpoint + pod SMs)

### Incremental Adoption

These SMs don't all need to be extracted at once. Each can be pulled out of the namespace monolith independently, wrapped in the SM trait, and optionally extracted to TLA+. The namespace monolith shrinks over time.

## Excepted SMs: The Extraction Boundary

Not every SM needs to be extracted from Rust. Some SMs — particularly those bridging the "real world" — have implementations that don't fit the narrow extractable subset. These are **excepted SMs**: they participate in the message-passing framework (declaring messages, using typed channels) but their Rust implementation is hand-written and their TLA+ model is provided manually.

### How It Works

An excepted SM still declares its messages with `#[sm_message]`:

```rust
#[sm_message(from = TimerSm, to = WorkloadSm, route_by = workload_id)]
struct TimerFired {
    workload_id: WorkloadSmId,
    timer_key: TimerKey,
}

#[sm_message(from = WorkloadSm, to = TimerSm, singleton)]
struct TimerSet {
    timer_key: TimerKey,
    duration: Duration,
    reply_to: WorkloadSmId,
}

#[sm_message(from = WorkloadSm, to = TimerSm, singleton)]
struct TimerCancel {
    timer_key: TimerKey,
}
```

The build step generates a **typed channel handle** that exposes only the channels this SM is allowed to send on:

```rust
// Generated — the only interface the timer impl gets
struct TimerSmChannels {
    fn send_to_workload(&self, target: WorkloadSmId, msg: TimerFired) -> Result<()>;
    fn send_to_service(&self, target: ServiceSmId, msg: TimerFired) -> Result<()>;
    // Cannot send anything else — no access to other channels
}
```

For **receiving** messages, the build step generates typed receivers from all `#[sm_message(to = TimerSm)]` declarations. Rather than a single `Inbox` enum (which would force synchronous matching), excepted SMs get individual receivers per message type:

```rust
// Generated — the receiving interface for the timer impl
struct TimerSmReceivers {
    timer_set: Receiver<TimerSet>,
    timer_cancel: Receiver<TimerCancel>,
}
```

This is more natural for async excepted SMs — the timer impl can `select!` across its receivers, waiting on both incoming messages and its own clock events simultaneously. Extracted SMs don't need this; they implement a generated handler trait with one method per message type.

The hand-written Rust implementation receives both the channel handle (for sending) and the receivers (for receiving). It can use async tasks, real clocks, whatever it needs — but it can only interact with the rest of the system through the declared message types. The routing topology remains statically known and complete.

On the TLA+ side, the excepted SM gets a hand-written module. For timers, this is a natural fit:

```tla
\* Timer SM: non-deterministically fires any pending timer
TimerStep ==
    \E tk \in DOMAIN pending_timers :
        /\ inbox' = [inbox EXCEPT ![pending_timers[tk].reply_to] =
               Append(@, [type |-> "TimerFired", timer_key |-> tk])]
        /\ pending_timers' = [tk2 \in (DOMAIN pending_timers \ {tk}) |-> pending_timers[tk2]]
```

### Safety Guarantees

Excepted SMs get weaker guarantees than extracted SMs, but stronger than no framework at all:

- **Static channel safety.** The generated channel handle prevents the impl from sending undeclared message types or routing to SMs it shouldn't touch. This is enforced at compile time.
- **Message contract.** Both the Rust impl and the TLA+ model must honor the same `#[sm_message]` declarations. The message types are the shared contract.
- **Manual TLA+ review.** The hand-written TLA+ model must be reviewed for faithfulness to the Rust impl. This is the one place where divergence is possible, but the surface area is small (just the excepted SMs, not the whole system).

### Use Cases Beyond Timers

Any SM that bridges an external system is a candidate for exception:

- **Worker connection SM** — manages real TCP/gRPC connections, hand-written in Rust, modeled in TLA+ as a process that non-deterministically connects/disconnects/loses messages.
- **External API handler** — receives HTTP requests, translates to SM messages. TLA+ model: non-deterministic input from the environment.
- **Scheduler** — placement decisions may involve heuristics that aren't worth extracting. TLA+ model: non-deterministic choice from the set of valid placements.

The pattern is the same in each case: declare messages, get a typed channel handle, write the impl freely, provide a TLA+ model manually.

## Extraction Pipeline

### What the Extractor Produces

For each SM, the build step generates:

**Rust side:**
- Handler trait (one method per inbound message type)
- Dispatch function (scans inbox, calls appropriate handler)
- Routing dispatch code
- Stateright `Model` impl (optional — derives actions from message types, state from SM state)

**TLA+ side:**
- One `.tla` module per SM with:
  - State variable declaration
  - One operator per handler method (e.g., `WorkloadHandleSetDemand(self, msg)`)
  - A top-level `WorkloadStep` that is the disjunction of all handler operators
  - Message send as `Append(inbox[target], msg)`
  - `state' = ...` from return value
- One composition module that wires all SM processes together with their inboxes

### Handlers → TLA+ Operators

Each handler method becomes an independent TLA+ operator:

```rust
// Rust handler
fn handle_pod_became_running(state: WorkloadSmState, msg: PodBecameRunning, ctx: &mut SmContext) -> WorkloadSmState {
    match state.phase {
        WorkloadPhase::Active { sub: ActiveSub::Launching { pending }, pod_id, .. }
            if pod_id == msg.pod_id =>
        {
            ctx.send(readiness_sm, BecameReady { workload_id: ctx.self_id(), pod_id });
            WorkloadSmState {
                phase: WorkloadPhase::Active { sub: ActiveSub::Running, pod_id, ..state.phase },
                consecutive_failures: 0,
                ..state
            }
        }
        _ => state,
    }
}
```

```tla
\* TLA+ — one operator per handler
\* Takes inbox_after (inbox with head already consumed) so handlers
\* only need to append outgoing messages.
WorkloadHandlePodBecameRunning(self, msg, inbox_after) ==
    /\ msg.type = "PodBecameRunning"
    /\ state[self].phase.type = "Active"
    /\ state[self].phase.sub.type = "Launching"
    /\ state[self].phase.pod_id = msg.pod_id
    /\ inbox' = [inbox_after EXCEPT ![readiness_sm] =
           Append(@, [type |-> "BecameReady", workload_id |-> self,
                      pod_id |-> state[self].phase.pod_id])]
    /\ state' = [state EXCEPT ![self] =
           [@ EXCEPT !.phase.sub = [type |-> "Running"],
                     !.consecutive_failures = 0]]

\* Top-level step: consume head of inbox, dispatch by type
WorkloadStep(self) ==
    /\ Len(inbox[self]) > 0
    /\ LET msg == Head(inbox[self])
           rest == [inbox EXCEPT ![self] = Tail(@)]
       IN \/ WorkloadHandleSetDemand(self, msg, rest)
          \/ WorkloadHandlePodBecameRunning(self, msg, rest)
          \/ WorkloadHandlePodIsGone(self, msg, rest)
          \/ ...
```

Because each handler is an independent function with a single message type, extraction is straightforward — no need to decompose a compound `(state, msg)` match.

### Stateright Model Derivation

The hand-written stateright models (`stateright_workload.rs`, `stateright_model.rs`) can be replaced by generated ones. The `actions()` method enumerates valid messages for the current state (derived from `#[sm_message]` metadata). The `next_state()` method calls the appropriate handler and snapshots. Timer tracking is handled by the generated harness, not manually.

## Verification Strategy

### Per-SM Properties

Invariants on individual SM state, similar to what the current stateright tests check:

- "Launching state always has a pending launch timeout timer"
- "Failed state implies max retries exhausted"
- "No `Transitioning` sentinel survives a step"

These can be checked with both stateright (fast, in CI) and TLC (unbounded).

### Compositional Properties

Properties about SM interactions, only checkable on the composed system:

- **Safety:** "A pod SM never receives a `Suspend` message while in `Launching` state" — provable from the composition of workload + pod message flow.
- **Liveness:** "If demand > 0 and fairness holds, a pod eventually reaches `Running`" — requires TLA+ temporal logic with fairness conditions.
- **Convergence:** "After any sequence of failures, if the system stabilizes (no more failures), all SMs eventually reach a consistent steady state."

### Relationship to Stateright

Stateright remains valuable for fast CI feedback. The generated stateright models replace the hand-written ones. TLA+/TLC is used for deeper verification (unbounded, temporal, compositional). TLAPS for proofs of critical invariants.

## Migration Path

### Phase 1: Message Types and Handler Traits

Define `#[sm_message]` and the build step that generates handler traits, dispatch, and routing. Convert the current `PodOutput`/`WorkloadOutput`/`ServiceOutput` enums to shared message types. Refactor the existing pod, workload, and service SMs to implement the generated handler traits.

Validate: existing stateright tests still pass (adapted to the new interface). Existing behavior preserved, routing is now declarative.

### Phase 2: Extract One SM to TLA+

Pick the pod SM (smallest, most self-contained). Implement the extractor: each handler method → one TLA+ operator. Verify the generated TLA+ module with TLC against the same properties as the stateright tests.

Validate: TLC finds the same state space as stateright. If it finds more (unbounded), investigate.

### Phase 3: Pod SM as Peer

Move the pod SM from being inlined in the workload to a peer SM communicating via messages. External events (`PodRunning`, `PodGone`, etc.) go directly to the pod SM; the pod SM processes them and sends outcome messages (`PodBecameRunning`, `PodIsGone`, `PodSuspendComplete`) to the workload. This is cleaner than a `call` primitive — the pod SM handles its own timer management internally, and stale/irrelevant events (e.g., timer for wrong state → Noop) never reach the workload.

The workload SM's `Active` state uses hierarchical sub-states (`Launching`, `Running`, `Suspending`, `Resuming`) to track the pod lifecycle phase without embedding the pod SM's state. The workload also tracks `worker_id` in its own state, since it needs this for `WorkerLost` handling and `StopPod` routing.

Validate: composed TLA+ spec of workload + pod checked with TLC.

### Phase 4: Namespace Decomposition

Incrementally pull concerns out of the namespace monolith into independent SMs (demand reconciliation first — clearest boundary, most critical logic). Extract each to TLA+. Verify composition.

### Phase 5: Compositional Verification

With multiple SMs extracted, write and verify cross-cutting temporal properties. This is where the investment pays off — proving things about the system that no bounded test can reach.

## Prior Art and Influences

### P Language (Microsoft)

P is the closest analogue — a dedicated language for communicating state machines with model checking and code generation (to C). Used in production for Windows USB drivers and extensively at AWS (S3, DynamoDB, EC2). Key differences from our approach:

- **P is a standalone language; we embed in Rust.** P requires maintaining a separate model alongside the implementation. Our approach eliminates divergence by making the implementation the spec.
- **P's communication is open; ours is closed.** Any P machine can send any event to any reference it holds. Our `#[sm_message]` routing is statically declared and compile-time enforced — more restrictive but provides guarantees P cannot.
- **P is imperative within handlers** (loops, blocking receives, multi-step sequences). Our handlers are pure decision trees — more restrictive but trivially extractable.

**Concepts adopted from P:**

- **`defer` / `ignore` semantics.** P lets you declare per-state that certain messages should be silently dropped (`ignore`) or buffered (`defer`). Our handlers achieve `ignore` naturally via `_ => state` catch-all arms — no special framework support needed. P's `defer` (buffering messages for later) is a potential future extension — see [Deferred Message Processing](#deferred-message-processing).

- **Specification monitors.** P has `spec machine` that passively observes events via `announce`, separate from system SMs. These are verification-only actors that track ghost state and assert invariants or liveness. Our equivalent: specification SMs that observe message channels, existing only in the TLA+ extraction.

- **Hot/cold states for liveness.** P's liveness model is more intuitive than raw TLA+ temporal formulas — a hot state means "must eventually leave." We should support a `#[hot]` annotation on states that auto-generates the appropriate temporal property.

- **`choose()` for nondeterminism.** Useful for modeling environmental decisions within extracted SMs (e.g., scheduler picks a worker). Maps to `\E x \in Set` in TLA+.

### Ivy (Microsoft)

Ivy verifies protocols using decidable logic fragments (EPR). It targets parameterized distributed protocols ("for all N nodes, safety holds") — a different problem shape than our fixed-topology control plane. Its code extraction goes in the opposite direction: spec → C++, not implementation → spec.

**Concepts to consider (not MVP):**

- **Assume/guarantee contracts on messages.** Ivy's `require`/`ensure` on actions enables modular verification — check each SM independently assuming incoming messages satisfy their contracts. This could be expressed as annotations on `#[sm_message]` types and would directly address TLC's scalability ceiling for large compositions. Worth exploring post-MVP when composition complexity grows.

- **Module system with substitution.** Ivy can swap implementations for compositional testing (`Coordinator -> MockCoordinator`). Useful for managing TLC state space — verify component A against a mock of B, then verify B against a mock of A, then verify A+B together. Not needed initially but valuable as the SM count grows.

### Stateright

We already use stateright for bounded model checking. Our design preserves its strengths while addressing its limitations.

**What we preserve from stateright:**

- **Tests the real handler functions**, not a separate model. The generated stateright models still call the actual SM implementation.
- **`sometimes` properties** for reachability sanity checks ("can reach Running", "can reach Failed") — catches over-constrained models.
- **Fast CI feedback** — stateright tests run in seconds within `cargo test`.
- **Symmetry reduction** — generated models should implement `Representative` for SM instance pools to reduce state space.

**What our design adds beyond stateright:**

- Temporal logic with fairness (TLA+/TLC) for true liveness properties.
- Unbounded model checking — no `max_steps` ceiling.
- Generated model harness — eliminates the ~800 lines of hand-written model code.
- TLAPS for formal proofs of critical invariants.

### Verus

SMT-based formal verification for Rust. Very powerful (best papers at OSDI 2024) but targets a different problem — functional correctness of individual functions, not compositional properties of interacting SMs. Not recommended as a near-term investment; the stateright + TLA+ combination covers the verification spectrum better.

The Anvil project (VMware Research, OSDI 2024) used Verus to verify Kubernetes controllers with an "Eventually Stable Reconciliation" pattern: `<>[]desired_state`. This is exactly the convergence property shape our TLA+ extraction should verify.

## Future Directions

### Cross-SM Invariants

Some important properties span multiple SMs — for example, "Launching state always has a pending timer" requires inspecting both the workload SM's state and the timer SM's state. A possible mechanism:

```rust
#[cross_invariant]
fn launching_has_timer(wl: &WorkloadState, timers: &TimerSmState) -> bool {
    match wl {
        WorkloadState::Launching { launch_timeout, .. } => {
            timers.pending.contains_key(launch_timeout)
        }
        _ => true,
    }
}
```

This is extractable (just a predicate over SM states), checkable by both stateright and TLC, and doesn't require special annotation machinery. Not MVP, but important as the SM count grows.

### Read-Only State Queries (`peek`)

Some cross-SM decisions require reading another SM's state without modifying it. For example, when `WorkerLost` arrives, the namespace layer needs to know which pods are on that worker. With pure message passing, this requires either tracking redundant state or a multi-step request/response.

A `ctx.peek(sm_id)` primitive could provide read-only access to another SM's state within a step. In TLA+, this is simply referencing `state[other_id]` in the action's guard — no primed variables, no message passing. In Rust, it's a synchronous read from shared state.

This is a weaker coupling than `call` (no state modification of the target) and avoids the architectural complications of synchronous cross-SM mutation. Worth exploring when the SM count grows and redundant state tracking becomes painful.

### Deferred Message Processing

The current design always consumes messages from the head of the inbox — every handler returns `State`, and irrelevant messages are handled as no-ops (`ignore`). This is simple and keeps the dispatch layer and TLA+ extraction straightforward.

An alternative, inspired by P's `defer` semantics, would allow handlers to signal "I can't handle this message in my current state — leave it in the inbox and try the next one." This would be expressed as handlers returning `Option<State>`, where `None` means defer. The dispatch layer would scan the inbox looking for a message some handler can process, skipping deferred ones.

**Why this is deferred:**

- **Inbox leak risk.** Deferred messages can accumulate if the condition for handling them never materializes (e.g., a `PodIsGone` deferred because the SM isn't `Active` with that pod, and it never will be). This requires garbage collection or TTLs — more complexity.
- **State space explosion.** With defer, the inbox becomes a bag rather than a queue. TLC must explore `O(n)` message positions per step, significantly increasing the state space.
- **Progress reasoning.** Proving that deferred messages eventually get handled requires showing the *right* state eventually occurs — a harder property than simple liveness.
- **Explicit buffering is clearer.** When a message genuinely arrives before the SM is ready, an explicit "waiting for X" sub-state that buffers the specific data in SM state is visible, extractable, and bounded. It makes the buffering part of the state machine logic rather than hidden in the dispatch layer.

**When to reconsider:** If multiple SMs end up needing "waiting" sub-states solely to buffer premature messages, that's the signal that `defer` would reduce boilerplate enough to justify its complexity. Until then, catch-all arms + explicit state covers the known use cases.

### Channel Delivery Semantics

Message channels are **FIFO per `(from_type, to_type)` pair** by default. This maps directly to TLA+ `Seq`, matches typical runtime behavior (tokio channels are ordered), and is the conservative starting point — the TLA+ model matches the Rust runtime from day one. The `#[sm_message]` attribute could later support an `ordering` parameter to weaken to unordered (TLA+ set) or unordered with duplication (for modeling unreliable networks), which would let you prove properties hold under weaker assumptions. The generated stateright model uses the matching network variant.

### Runtime Monitoring

P's PObserve feature checks production logs against specification monitors. A similar capability could be added to the framework: specification SMs (or message contract assertions) that run in production to catch spec violations at runtime, not just at verification time.

## Limitations and Open Questions

- **Runtime overhead of message passing.** The current synchronous `pod.step()` call becomes async message delivery. For SMs that always run on the same thread, the runtime can optimize this to a direct function call while preserving the message-passing semantics for extraction.
- **TLC state space.** More SM instances = larger state space. Bounded pools help, but the composition of many SMs may require aggressive abstraction or symmetry reduction.
- **Message ordering.** Channels default to FIFO per `(from_type, to_type)` pair (see [Channel Delivery Semantics](#channel-delivery-semantics)). Weakening to unordered for specific channels may be useful later for proving properties under weaker assumptions.
- **Excepted SM faithfulness.** Excepted SMs have hand-written TLA+ models that could diverge from the Rust implementation. The typed channel handles limit the blast radius, but the TLA+ model itself requires manual review. Keeping these SMs small and few reduces the risk.
- **Build step complexity.** The code generator that collects `#[sm_message]` across modules and produces Rust + TLA+ is a meaningful piece of infrastructure to build and maintain.

## Appendix: Workload SM Translation Sketch

A concrete dry-run translation of the current workload SM into the proposed format is in [`sm-workload-translation-sketch.md`](sm-workload-translation-sketch.md). It validates the extractable subset against real code and documents specific friction points encountered. Key findings:

- Helper functions with `ctx.send()` are load-bearing (~8 call sites for `transition_on_demand`)
- Hierarchical state (`Active { sub: ActiveSub, .. }`) recovers the natural grouping lost by flattening
- `_ => state` catch-all arms handle irrelevant (state, message) combinations naturally
- Pod-as-peer with outcome messages is cleaner than `call` for the workload↔pod interaction
- Collection wrapper types (`SmSet`, `SmMap`) are needed for value-semantics APIs
- Observability fields (`conditions`, `last_failure_reason`) should be excluded from extraction
