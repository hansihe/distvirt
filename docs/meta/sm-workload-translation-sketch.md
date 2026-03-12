---
title: "Workload SM Translation Sketch"
status: exploratory
---

## Purpose

Dry-run translation of the current workload SM (`src/sm/workload.rs`) into the
proposed extractable format from `sm-tla-extraction.md`. Goal: find where the
format breaks, what expression subset is needed, and whether `call` is essential.

## Current Structure Summary

The workload SM has:

- **State enum** (`WorkloadState`): `Dormant`, `WaitingForCapacity`,
  `Active { pod: PodSlot, pending }`, `Suspended { artifact_id }`,
  `RetryBackoff { backoff_timer }`, `Failed`, `Transitioning`
- **Side-car fields** on `WorkloadStateMachine`: `current_demand`, `suspend_on_idle`,
  `consecutive_failures`, `max_retries`, `needs_successful_boot`, `retiring: Vec<RetiredPod>`,
  `conditions: BTreeMap<String, String>`, `last_failure_reason`
- **Embedded pod SM**: `PodSlot` lives inside `Active`, called synchronously via `pod.step()`
- **Helper methods**: `transition_on_demand`, `transition_on_intent`, `upgrade_pending`,
  `retire_pod`, `collect_pod_outputs`
- **The `Transitioning` sentinel**: used with `mem::replace` to destructure state

## Proposed Translation

### State Type

With pod as a peer SM, the workload no longer embeds `PodSlot`. Instead, the
workload tracks which phase it's in with respect to the pod. The pod's internal
state (launch_timeout timer key, etc.) lives in the pod SM.

```rust
#[derive(Extractable)]
struct WorkloadSmState {
    phase: WorkloadPhase,
    current_demand: u32,
    suspend_on_idle: bool,
    consecutive_failures: u32,
    max_retries: u32,
    needs_successful_boot: bool,
    // Set of pod IDs told to stop but not yet confirmed gone.
    // Simplified from Vec<RetiredPod> — worker_id not needed if pod SM
    // tracks its own worker.
    retiring: BTreeSet<PodSmId>,
}

#[derive(Extractable)]
enum WorkloadPhase {
    Dormant,
    WaitingForCapacity,
    Launching { pod_id: PodSmId, pending: PendingIntent },
    Running { pod_id: PodSmId },
    Suspending { pod_id: PodSmId, pending: PendingIntent },
    Resuming { pod_id: PodSmId, pending: PendingIntent },
    Suspended { artifact_id: ArtifactId },
    RetryBackoff { backoff_timer: TimerKey },
    Failed,
}
```

**Key change:** The `Active { pod: PodSlot { pod_state, .. }, pending }` tree
flattens into separate variants: `Launching`, `Running`, `Suspending`, `Resuming`.
The workload directly encodes what lifecycle phase the pod is in, because it needs
to make decisions based on that (and with peer pod SM, it no longer peeks at
`pod.pod_state`).

### What Drops Out

| Current feature | Disposition |
|---|---|
| `Transitioning` sentinel | Gone — state by value |
| `mem::replace` | Gone — state by value |
| `PodSlot` embedding | Gone — pod is peer |
| `collect_pod_outputs` | Gone — pod sends its own messages |
| `conditions: BTreeMap<String, String>` | Excepted — observability, not verifiable (uses `format!`) |
| `last_failure_reason: Option<PodGoneReason>` | Excepted — observability, `PodGoneReason` contains `String` |

The `conditions` and `last_failure_reason` fields are purely for observability.
They don't affect transitions. Two options:
1. Exclude from extractable state entirely (they exist in the Rust impl but not TLA+)
2. Model as abstract tags in TLA+ (e.g., a boolean `has_failure_condition`)

Option 1 is simpler and correct — these fields are side-effects, not decision inputs.

### Messages

```rust
// --- Inbound ---

#[sm_message(from = DemandSm, to = WorkloadSm, route_by = workload_id)]
struct SetDemand {
    workload_id: WorkloadSmId,
    count: u32,
}

#[sm_message(from = PlacementSm, to = WorkloadSm, route_by = workload_id)]
struct LaunchPod {
    workload_id: WorkloadSmId,
    worker_id: WorkerId,
    pod_id: PodSmId,
}

#[sm_message(from = PlacementSm, to = WorkloadSm, route_by = workload_id)]
struct ResumePod {
    workload_id: WorkloadSmId,
    worker_id: WorkerId,
    pod_id: PodSmId,
    artifact_id: ArtifactId,
}

// Pod outcomes — sent by pod SM back to workload after processing
#[sm_message(from = PodSm, to = WorkloadSm, route_by = workload_id)]
struct PodBecameRunning {
    workload_id: WorkloadSmId,
    pod_id: PodSmId,
}

#[sm_message(from = PodSm, to = WorkloadSm, route_by = workload_id)]
struct PodIsGone {
    workload_id: WorkloadSmId,
    pod_id: PodSmId,
    was_resuming: bool,   // pod knows this about itself
    was_suspending: bool,
    is_failure: bool,
}

#[sm_message(from = PodSm, to = WorkloadSm, route_by = workload_id)]
struct PodSuspendComplete {
    workload_id: WorkloadSmId,
    pod_id: PodSmId,
    artifact_id: ArtifactId,
}

#[sm_message(from = PodSm, to = WorkloadSm, route_by = workload_id)]
struct PodSuspendFailed {
    workload_id: WorkloadSmId,
    pod_id: PodSmId,
}

#[sm_message(from = TimerSm, to = WorkloadSm, route_by = workload_id)]
struct TimerFired {
    workload_id: WorkloadSmId,
    timer_key: TimerKey,
}

#[sm_message(from = NamespaceSm, to = WorkloadSm, route_by = workload_id)]
struct WorkerLost {
    workload_id: WorkloadSmId,
    worker_id: WorkerId,
}

#[sm_message(from = NamespaceSm, to = WorkloadSm, route_by = workload_id)]
struct ForceDeactivate {
    workload_id: WorkloadSmId,
}

#[sm_message(from = NamespaceSm, to = WorkloadSm, route_by = workload_id)]
struct SpecChanged {
    workload_id: WorkloadSmId,
}

#[sm_message(from = NamespaceSm, to = WorkloadSm, route_by = workload_id)]
struct ManualRestart {
    workload_id: WorkloadSmId,
}

// --- Outbound ---

#[sm_message(from = WorkloadSm, to = PlacementSm, singleton)]
struct PodRequest {
    workload_id: WorkloadSmId,
}

#[sm_message(from = WorkloadSm, to = PlacementSm, singleton)]
struct SuspendRequest {
    workload_id: WorkloadSmId,
    pod_id: PodSmId,
}

#[sm_message(from = WorkloadSm, to = PlacementSm, singleton)]
struct ResumeRequest {
    workload_id: WorkloadSmId,
    artifact_id: ArtifactId,
}

#[sm_message(from = WorkloadSm, to = PodSm, route_by = pod_id)]
struct StopPod {
    pod_id: PodSmId,
    graceful: bool,
}

#[sm_message(from = WorkloadSm, to = ReadinessSm, singleton)]
struct BecameReady {
    workload_id: WorkloadSmId,
    pod_id: PodSmId,
}

#[sm_message(from = WorkloadSm, to = ReadinessSm, singleton)]
struct BecameUnready {
    workload_id: WorkloadSmId,
}

#[sm_message(from = WorkloadSm, to = TimerSm, singleton)]
struct TimerSet {
    timer_key: TimerKey,
    // Duration not extractable as-is — see friction point #4
}

#[sm_message(from = WorkloadSm, to = TimerSm, singleton)]
struct TimerCancel {
    timer_key: TimerKey,
}
```

### Step Function: Key Arms

Here's the core of the translation. I'll show representative arms, not all of them.

```rust
impl StateMachine for WorkloadSm {
    type State = WorkloadSmState;
    // Inbox generated from #[sm_message(to = WorkloadSm)]

    fn step(state: WorkloadSmState, msg: Inbox, ctx: &mut SmContext) -> WorkloadSmState {
        match (state.phase, msg) {

            // ── SetDemand: 0→non-zero, dormant ──
            (WorkloadPhase::Dormant, Inbox::SetDemand(SetDemand { count, .. }))
                if count > 0 =>
            {
                ctx.send(placement_sm, PodRequest { workload_id: ctx.self_id() });
                WorkloadSmState {
                    phase: WorkloadPhase::WaitingForCapacity,
                    current_demand: count,
                    needs_successful_boot: true,
                    ..state
                }
            }

            // ── SetDemand: 0→non-zero, suspended ──
            (WorkloadPhase::Suspended { artifact_id }, Inbox::SetDemand(SetDemand { count, .. }))
                if count > 0 =>
            {
                ctx.send(placement_sm, ResumeRequest {
                    workload_id: ctx.self_id(),
                    artifact_id,
                });
                WorkloadSmState {
                    // phase stays Suspended until ResumePod arrives
                    current_demand: count,
                    needs_successful_boot: true,
                    ..state
                }
            }

            // ── SetDemand: non-zero→0, running, suspend_on_idle ──
            (WorkloadPhase::Running { pod_id }, Inbox::SetDemand(SetDemand { count: 0, .. }))
                if state.suspend_on_idle && !state.needs_successful_boot =>
            {
                ctx.send(ctx.self_id(), BecameUnready { workload_id: ctx.self_id() }); // PROBLEM: see below
                ctx.send(pod_id, SuspendPod { /* ... */ }); // PROBLEM: who generates artifact_id?
                WorkloadSmState {
                    phase: WorkloadPhase::Suspending { pod_id, pending: PendingIntent::None },
                    current_demand: 0,
                    ..state
                }
            }

            // ── LaunchPod: placement resolved ──
            (WorkloadPhase::WaitingForCapacity, Inbox::LaunchPod(LaunchPod { pod_id, worker_id, .. })) =>
            {
                // Tell pod SM to initialize in Launching state
                ctx.send(pod_id, InitPod {
                    workload_id: ctx.self_id(),
                    worker_id,
                    mode: PodInitMode::Launch,
                });
                WorkloadSmState {
                    phase: WorkloadPhase::Launching {
                        pod_id,
                        pending: PendingIntent::None,
                    },
                    ..state
                }
            }

            // ── PodBecameRunning: launch complete ──
            (WorkloadPhase::Launching { pod_id, pending }, Inbox::PodBecameRunning(msg))
                if msg.pod_id == pod_id =>
            {
                match pending {
                    PendingIntent::Deactivate => {
                        ctx.send(pod_id, StopPod { pod_id, graceful: false });
                        let retiring = state.retiring.insert(pod_id); // PROBLEM: not valid Rust
                        WorkloadSmState {
                            phase: WorkloadPhase::Dormant,
                            consecutive_failures: 0,
                            needs_successful_boot: false,
                            retiring, // PROBLEM: BTreeSet::insert returns bool
                            ..state
                        }
                    }
                    PendingIntent::Restart => {
                        ctx.send(pod_id, StopPod { pod_id, graceful: false });
                        // PROBLEM: need to call transition_on_demand helper
                        // This is where helper functions are essential
                        todo!("transition_on_demand")
                    }
                    PendingIntent::Demand | PendingIntent::None => {
                        ctx.send(readiness_sm, BecameReady {
                            workload_id: ctx.self_id(),
                            pod_id,
                        });
                        WorkloadSmState {
                            phase: WorkloadPhase::Running { pod_id },
                            consecutive_failures: 0,
                            needs_successful_boot: false,
                            ..state
                        }
                    }
                }
            }

            // ── PodIsGone: active pod died ──
            (phase, Inbox::PodIsGone(msg))
                if phase.active_pod_id() == Some(msg.pod_id)
                && !state.retiring.contains(&msg.pod_id) =>
            {
                let consecutive_failures = if msg.is_failure && !msg.was_suspending {
                    state.consecutive_failures + 1
                } else {
                    state.consecutive_failures
                };
                let needs_successful_boot = if !msg.was_suspending {
                    true
                } else {
                    state.needs_successful_boot
                };

                let was_running = matches!(phase, WorkloadPhase::Running { .. });
                let pending = phase.pending().unwrap_or(PendingIntent::None);

                // PROBLEM: transition_on_demand / transition_on_intent
                // logic needs to be inlined or callable
                if was_running {
                    ctx.send(readiness_sm, BecameUnready { workload_id: ctx.self_id() });
                }

                // PROBLEM: the transition_on_demand logic branches into
                // WaitingForCapacity, RetryBackoff, Failed, or Dormant
                // depending on demand, failures, and max_retries.
                // Each branch may send different messages.
                // This is a nested decision tree that repeats across many arms.
                let new_phase = transition_on_demand(
                    state.current_demand,
                    consecutive_failures,
                    state.max_retries,
                    needs_successful_boot,
                    ctx,
                );

                WorkloadSmState {
                    phase: new_phase,
                    consecutive_failures,
                    needs_successful_boot,
                    ..state
                }
            }

            // ── PodIsGone for a retiring pod ──
            (phase, Inbox::PodIsGone(msg))
                if state.retiring.contains(&msg.pod_id) =>
            {
                WorkloadSmState {
                    retiring: state.retiring.without(&msg.pod_id), // PROBLEM: no such method
                    ..state
                }
            }

            // ── TimerFired: retry backoff expired ──
            (WorkloadPhase::RetryBackoff { backoff_timer }, Inbox::TimerFired(msg))
                if msg.timer_key == backoff_timer =>
            {
                ctx.send(placement_sm, PodRequest { workload_id: ctx.self_id() });
                WorkloadSmState {
                    phase: WorkloadPhase::WaitingForCapacity,
                    ..state
                }
            }

            // ── ForceDeactivate while running ──
            (WorkloadPhase::Running { pod_id }, Inbox::ForceDeactivate(_))
                if state.suspend_on_idle =>
            {
                ctx.send(readiness_sm, BecameUnready { workload_id: ctx.self_id() });
                ctx.send(pod_id, SuspendPod { /* ... */ });
                WorkloadSmState {
                    phase: WorkloadPhase::Suspending {
                        pod_id,
                        pending: PendingIntent::Deactivate,
                    },
                    needs_successful_boot: false,
                    ..state
                }
            }

            // ── WorkerLost ──
            (phase, Inbox::WorkerLost(msg))
                if phase.active_pod_id().map(|id| /* need worker lookup */) // PROBLEM
            =>
            {
                // PROBLEM: current code checks pod.worker_id == worker_id
                // With peer pod SM, the workload doesn't know the pod's worker_id!
                // Options:
                //   1. Workload tracks worker_id in its state (Launching { pod_id, worker_id, .. })
                //   2. WorkerLost is sent to pod SM, pod notifies workload
                //   3. Namespace layer resolves and sends targeted message
                todo!()
            }

            // ── Catch-all: message not applicable to current state ──
            (phase, _) => {
                WorkloadSmState { phase, ..state }
            }
        }
    }
}
```

### Helper Function: `transition_on_demand`

This is called from ~8 different match arms. It must be extractable:

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
            let timer_key = TimerKey::RetryBackoffTimeout {
                workload_id: ctx.self_id(),
            };
            ctx.send(timer_sm, TimerSet { timer_key, /* duration */ });
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

Maps to TLA+:
```tla
TransitionOnDemand(current_demand, consecutive_failures, max_retries, needs_boot, self) ==
    IF current_demand > 0 \/ needs_boot THEN
        IF consecutive_failures >= max_retries THEN
            [phase |-> "Failed"]
        ELSE IF consecutive_failures > 0 THEN
            LET tk == [type |-> "RetryBackoffTimeout", workload_id |-> self] IN
            /\ inbox' = [inbox EXCEPT ![timer_sm] = Append(@, [type |-> "TimerSet", timer_key |-> tk])]
            /\ [phase |-> "RetryBackoff", backoff_timer |-> tk]
        ELSE
            /\ inbox' = [inbox EXCEPT ![placement_sm] = Append(@, [type |-> "PodRequest", workload_id |-> self])]
            /\ [phase |-> "WaitingForCapacity"]
    ELSE
        [phase |-> "Dormant"]
```

This is extractable, but note the **side effects** (`ctx.send`) mixed with the
return value. The TLA+ translation must handle "this function both produces
inbox updates AND returns a value." This is non-trivial — TLA+ operators don't
have side effects; the sends need to be accumulated and merged with the caller's
sends.

---

## Friction Points Discovered

### 1. Helper functions with side effects are essential

`transition_on_demand` is called from ~8 arms. Without helper functions, you'd
duplicate 15+ lines in each arm. But these helpers both modify `ctx` (send
messages) AND return values. The extractor must:

- Allow calling other extractable functions from `step`
- Thread `ctx.send()` effects through the call (accumulate all sends from
  step + helpers, merge them in the TLA+ action)

This is the single biggest requirement the doc undersells. **The extractable
subset needs extractable helper functions that can call `ctx.send()`.**

TLA+ translation: helper becomes an operator that returns a record of
`{phase, sends}`, caller merges the sends. Alternatively, use a let/in
binding in TLA+ and rely on primed variable accumulation.

### 2. Collection operations needed

The `retiring` set requires:
- `contains(&pod_id)` — `pod_id \in retiring`
- `insert(pod_id)` — `retiring \union {pod_id}`
- `remove(&pod_id)` / `retain(|r| r != pod_id)` — `retiring \ {pod_id}`
- `retain(|r| r.worker_id != worker_id)` — `{r \in retiring : r.worker_id /= worker_id}`

If `retiring` is `BTreeSet<PodSmId>`, the first three are clean. The last
(filter by a field) requires set comprehension, which implies `retiring` should
actually be a `BTreeSet<RetiredPod>` struct with fields. Filter-by-field maps
to TLA+ `{r \in retiring : r.worker_id /= worker_id}`.

**Needed Rust surface for sets:**
- `set.contains(&x)` → `x \in S`
- `set.union_one(x)` or similar → `S \union {x}` (NOTE: `BTreeSet::insert`
  mutates and returns bool — can't use directly with value semantics)
- `set.without(&x)` → `S \ {x}`
- `set.filter(|r| predicate)` → `{r \in S : P(r)}`

The Rust `BTreeSet` API doesn't fit value semantics well. We probably need a
wrapper type (`SmSet<T>`) with methods that return new sets:
```rust
impl<T> SmSet<T> {
    fn insert(self, x: T) -> SmSet<T>;   // returns new set
    fn remove(self, x: &T) -> SmSet<T>;  // returns new set
    fn contains(&self, x: &T) -> bool;
    fn filter(self, pred: impl Fn(&T) -> bool) -> SmSet<T>;
}
```

### 3. `..state` (struct update syntax) is very useful but has extraction implications

The pattern `WorkloadSmState { phase: new_phase, ..state }` is convenient and
maps to TLA+ `[state EXCEPT !.phase = new_phase]`. But the extractor needs to
understand Rust struct update syntax and know which fields are NOT being
overridden.

This is probably fine — struct update is well-defined in Rust's AST.

### 4. ArtifactId generation is not extractable

Currently: `ArtifactId::from(format!("{}-{}-{}", namespace_id.0, workload_id.0, pod_id.0))`

This can't be extracted. Options:
- Model artifact IDs as values from a bounded set (TLA+ model constant)
- Use `ctx.new_artifact_id()` which is a nondeterministic choice in TLA+
  (`\E aid \in ArtifactIds : ...`)
- Make artifact ID generation an excepted concern (the placement SM generates
  them in response to suspend requests)

**Recommended:** Move artifact ID creation to the placement SM. The workload
says "suspend this pod," the placement SM generates the artifact ID and manages
the placement table entry. This is cleaner architecturally anyway — the workload
shouldn't know about artifact naming.

### 5. Worker ID tracking problem with peer pod SM

Currently the workload knows each pod's `worker_id` because `PodSlot` is
embedded. With peer pod SM:

- `WorkerLost { worker_id }` needs to find which pod is on that worker
- `retire_pod` needs the worker_id to send `StopPod`
- `BecameReady` includes worker_id

Options:
1. **Workload tracks worker_id in its phase variants:**
   `Launching { pod_id, worker_id, pending }`, etc.
   This is the simplest — adds one field to each variant.
2. **WorkerLost goes to pod SM first, pod notifies workload.**
   Cleaner separation but adds latency and intermediate states.
3. **`ctx.call` / `ctx.peek` to query pod SM's worker.**
   Convenient but architecturally couples them.

**Recommended:** Option 1. The worker_id is a routing key the workload needs
for its own decisions. It's lightweight to track.

### 6. Nested match on `pending` inside `(state, msg)` match

Many arms match `(state, msg)` and then do an inner `match pending { ... }`.
This is a nested decision tree. Example:

```rust
(Launching { pod_id, pending }, PodBecameRunning(msg)) if msg.pod_id == pod_id => {
    match pending {
        Deactivate => { /* retire + dormant */ }
        Restart => { /* retire + transition_on_demand */ }
        Demand | None => { /* became ready + running */ }
    }
}
```

This is natural in Rust and maps to TLA+ as nested CASE or conjunctive guards:
```tla
CASE state.phase.type = "Launching" /\ msg.type = "PodBecameRunning"
     /\ msg.pod_id = state.phase.pod_id ->
    CASE state.phase.pending = "Deactivate" -> ...
    [] state.phase.pending = "Restart" -> ...
    [] OTHER -> ...
```

**This works fine.** The extractor just needs to handle nested `match` expressions,
not just top-level `match (state, msg)`.

### 7. Guard expressions need method-like syntax

Several arms use guards like:
- `if count > 0` — simple comparison, fine
- `if state.suspend_on_idle && !state.needs_successful_boot` — boolean combo, fine
- `if phase.active_pod_id() == Some(msg.pod_id)` — method call on enum, PROBLEM

`phase.active_pod_id()` is a helper that extracts the pod_id from whichever
variant is active. In the extractable subset, we'd either:
- Match more specifically (separate arms per variant), or
- Allow a limited set of "accessor" methods on extractable enums that the
  extractor understands

**Recommended:** Match more specifically. The "catch multiple variants" pattern
can use `|` in match arms:
```rust
(WorkloadPhase::Launching { pod_id, .. }
 | WorkloadPhase::Running { pod_id }
 | WorkloadPhase::Suspending { pod_id, .. }
 | WorkloadPhase::Resuming { pod_id, .. },
 Inbox::PodIsGone(msg))
    if pod_id == msg.pod_id && !state.retiring.contains(&msg.pod_id) =>
{ ... }
```

This is more verbose but directly extractable.

### 8. `ctx.send` to self / readiness SM routing

The current code emits `BecameReady` and `BecameUnready` as outputs that the
namespace layer translates. In the peer model, these become messages to a
readiness SM. But the workload SM needs the readiness SM's ID. Options:

- **SM-level configuration:** The workload SM knows its associated readiness
  SM ID (set at init time, part of its state or context).
- **`ctx` provides it:** `ctx.readiness_sm()` or similar.
- **Fixed singleton:** If there's one readiness SM per namespace, it's a
  known constant.

**Recommended:** Singleton addressing. `#[sm_message(from = WorkloadSm, to = ReadinessSm, singleton)]`
handles this — no need for the workload to know an ID.

### 9. The `call` primitive: verdict

Looking at the actual code, the pod SM interaction has a specific pattern:
1. Workload receives external event (PodRunning, PodGone, etc.)
2. Workload forwards to pod SM: `pod.step(input)`
3. Pod SM returns `(PodOutcome, Vec<PodOutput>)`
4. Workload matches on `PodOutcome` to decide its own transition
5. Workload converts `PodOutput`s to `WorkloadOutput`s

With peer pod SM, this becomes:
1. External event goes directly to pod SM (or workload forwards it)
2. Pod SM processes, sends outcome message to workload
3. Workload receives outcome, makes its transition

**`call` is NOT essential here.** The current code looks like it needs `call`
because pod is embedded, but once pod is a peer, the events (PodRunning, PodGone,
etc.) should go directly to the pod SM. The pod SM processes them and sends
outcome messages (PodBecameRunning, PodIsGone) to the workload.

This is actually *cleaner* than `call`:
- No synchronous coupling
- Pod SM can handle events the workload doesn't need to know about (e.g., stale
  timer → Noop, never forwarded to workload)
- The pod SM's timer management is fully internal

**Where `call` IS useful:** Querying state. Example: when `WorkerLost` arrives,
the namespace layer needs to know which pods are on that worker. With pure
message passing, this requires either tracking redundant state or multi-step
request/response. `ctx.peek(pod_id).worker_id` would be much simpler.

### 10. `Duration` is not extractable

Timer durations (`LAUNCH_TIMEOUT_SECS`, `backoff_delay()`) use `Duration`.
In TLA+, durations are just natural numbers (if modeled at all — often abstracted
away since TLA+ doesn't model real time).

Options:
- Timers in TLA+ just fire nondeterministically (no duration)
- Model duration as Nat, timer SM fires when "clock >= set_time + duration"

The former is simpler and sufficient for safety/liveness properties.
`backoff_delay` (exponential backoff via bit shift) doesn't need extraction —
the TLA+ model just needs "some delay > 0" or even "nondeterministic fire."

### 11. Number of match arms

The current workload SM has ~30 meaningful (state, input) combinations in its
`step` function. Flattening the pod state into workload phase variants
INCREASES the number of arms, because `Active { pod: PodSlot { pod_state: Running, .. } }`
and `Active { pod: PodSlot { pod_state: Launching { .. }, .. } }` become
separate top-level variants.

Rough estimate: the workload SM step function will have **40-50 match arms**
after translation. This is manageable but reinforces the need for:
- `defer`/`ignore` annotations to handle "message irrelevant in this state"
- Helper functions to avoid duplicating transition logic

---

## Expression Subset: Minimum Required

Based on this translation, the extractable Rust subset needs at minimum:

**Control flow:**
- `match` with nested matches
- `match` guards with `if expr`
- `|` in match arm patterns (or-patterns)
- `if`/`else` expressions (for helpers like `transition_on_demand`)

**Expressions:**
- Field access (`state.field`)
- Comparison operators (`==`, `!=`, `>`, `>=`, `<`, `<=`)
- Boolean operators (`&&`, `||`, `!`)
- Arithmetic (`+`, `-`, saturating preferred)
- Struct construction with `..rest` syntax
- Enum variant construction
- `let` bindings (for intermediate values)

**Collection operations (via wrapper types):**
- `SmSet::contains(&x)` → `x \in S`
- `SmSet::insert(self, x)` → `S \union {x}`
- `SmSet::remove(self, x)` → `S \ {x}`
- `SmSet::filter(self, pred)` → `{x \in S : P(x)}`
- `SmMap::get(&self, k)` → `map[k]` (returns `Option<&V>`)
- `SmMap::insert(self, k, v)` → `[map EXCEPT ![k] = v]`
- `SmMap::remove(self, k)` → domain restriction
- `SmMap::contains_key(&self, k)` → `k \in DOMAIN map`

**Functions:**
- Extractable helper functions (called from `step`, can use `ctx.send()`)
- Must compose cleanly in TLA+ (operator calls)

**Explicitly NOT needed (for workload SM):**
- Loops / iterators
- Closures (except in `filter`, which is a known combinator)
- `Vec` operations (use `SmSet` instead where order doesn't matter)
- Method calls on arbitrary types
- `format!`, string operations
- `mem::replace`, `&mut` borrows

---

## Conclusion

The translation is feasible but reveals several things the design doc underplays:

1. **Helper functions with `ctx.send()` are load-bearing.** Without them, the
   step function is unmanageably repetitive. The extractor MUST support them.

2. **Custom collection types** (`SmSet`, `SmMap`) are needed because std types
   don't have value-semantics APIs.

3. **`call` is NOT essential for the pod interaction** — making pod a true peer
   with outcome messages is cleaner. `peek` (read-only state query) would be
   useful for cross-SM lookups but is not blocking.

4. **The workload SM will get LARGER**, not smaller, after translation. The
   flattened phase enum has more variants, and without `defer`/`ignore`,
   catch-all arms add up. `defer`/`ignore` should be in the core design.

5. **Observability concerns** (conditions, failure reasons) should be explicitly
   excluded from the extractable state. They're side effects that don't affect
   transitions and would bloat the TLA+ model.
