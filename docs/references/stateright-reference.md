---
title: "Stateright Reference Guide"
---

A comprehensive reference for using [Stateright](https://www.stateright.rs) (v0.31.0) to model-check state machines, with a focus on non-distributed / orchestrator-style models.

---

## Table of Contents

1. [Overview](#overview)
2. [The Model Trait (Core API)](#the-model-trait)
3. [Properties and Expectations](#properties-and-expectations)
4. [Checker Configuration and Strategies](#checker-configuration)
5. [The Actor Framework](#the-actor-framework)
6. [Symmetry Reduction](#symmetry-reduction)
7. [Explorer (Interactive Debugging)](#explorer)
8. [Utilities](#utilities)
9. [Patterns and Idioms](#patterns-and-idioms)
10. [Complete Examples](#complete-examples)
11. [Performance Tips](#performance-tips)

---

## 1. Overview <a name="overview"></a>

Stateright is a Rust model checker that exhaustively explores all reachable states of a system to verify safety and liveness properties. Unlike TLA+, Stateright verifies the **implementation** directly — the same Rust types used in the model can be used in production.

**Key capabilities:**
- Exhaustive state space exploration (BFS, DFS)
- Random simulation for large state spaces
- Safety properties (invariants that must always hold)
- Liveness properties (conditions that must eventually hold)
- Reachability properties (conditions that should be possible)
- Interactive web-based state explorer for debugging
- Symmetry reduction to shrink state spaces
- Multi-threaded checking

**Crate:** `stateright = "0.31"`

---

## 2. The Model Trait (Core API) <a name="the-model-trait"></a>

The `Model` trait is the primary abstraction. You define your state machine by implementing it.

**Source:** `src/lib.rs:152-260`

```rust
use stateright::Model;

pub trait Model: Sized {
    /// The type representing the full state of your system.
    type State;

    /// The type representing transitions between states.
    type Action;

    /// Returns all possible initial states.
    fn init_states(&self) -> Vec<Self::State>;

    /// Populates `actions` with all actions enabled in `state`.
    fn actions(&self, state: &Self::State, actions: &mut Vec<Self::Action>);

    /// Computes the next state after applying `action` to `last_state`.
    /// Return `None` to indicate the action is not applicable (state is dropped).
    fn next_state(&self, last_state: &Self::State, action: Self::Action) -> Option<Self::State>;

    /// Properties to verify (safety, liveness, reachability).
    fn properties(&self) -> Vec<Property<Self>> { vec![] }

    /// Optional state space boundary. Return `false` to prune exploration.
    fn within_boundary(&self, _state: &Self::State) -> bool { true }

    /// Entry point to configure and launch a checker.
    fn checker(self) -> CheckerBuilder<Self>;
}
```

### Trait Bounds on Associated Types

For model checking to work, `State` and `Action` must satisfy:

| Type | Required Bounds |
|------|----------------|
| `State` | `Clone + Debug + Hash + PartialEq` (for fingerprinting and deduplication) |
| `Action` | `Clone + Debug + PartialEq` (for path recording) |

### Minimal Implementation Pattern

```rust
use stateright::{Model, Property};

#[derive(Clone)]
struct MySystem {
    param: usize,
}

#[derive(Clone, Debug, Hash, PartialEq)]
struct MyState {
    counter: u32,
    phase: Phase,
}

#[derive(Clone, Debug, Hash, PartialEq)]
enum Phase { Init, Running, Done }

#[derive(Clone, Debug, PartialEq)]
enum MyAction {
    Start,
    Increment,
    Finish,
}

impl Model for MySystem {
    type State = MyState;
    type Action = MyAction;

    fn init_states(&self) -> Vec<Self::State> {
        // Usually a single deterministic initial state
        vec![MyState { counter: 0, phase: Phase::Init }]
    }

    fn actions(&self, state: &Self::State, actions: &mut Vec<Self::Action>) {
        // Enumerate all enabled actions based on current state
        match state.phase {
            Phase::Init => actions.push(MyAction::Start),
            Phase::Running => {
                if state.counter < self.param as u32 {
                    actions.push(MyAction::Increment);
                }
                actions.push(MyAction::Finish);
            }
            Phase::Done => {} // terminal state — no actions
        }
    }

    fn next_state(&self, last_state: &Self::State, action: Self::Action) -> Option<Self::State> {
        let mut state = last_state.clone();
        match action {
            MyAction::Start => state.phase = Phase::Running,
            MyAction::Increment => state.counter += 1,
            MyAction::Finish => state.phase = Phase::Done,
        }
        Some(state)
    }

    fn properties(&self) -> Vec<Property<Self>> {
        vec![
            Property::<Self>::always("counter bounded", |model, state| {
                state.counter <= model.param as u32
            }),
            Property::<Self>::eventually("reaches done", |_, state| {
                state.phase == Phase::Done
            }),
            Property::<Self>::sometimes("can increment", |_, state| {
                state.counter > 0
            }),
        ]
    }
}
```

### `within_boundary` — Limiting State Space

Use `within_boundary` to prune the exploration and keep the state space finite. States where `within_boundary` returns `false` are not explored further (and their "eventually" properties are not checked).

```rust
fn within_boundary(&self, state: &Self::State) -> bool {
    state.counter <= 100  // don't explore beyond this
}
```

### `next_state` Returning `None`

If `next_state` returns `None`, the transition is silently dropped. This is useful for conditional actions where the action was listed in `actions()` but turns out to be inapplicable after closer inspection.

### Multiple Initial States

`init_states()` can return multiple states to model nondeterministic initialization:

```rust
fn init_states(&self) -> Vec<Self::State> {
    vec![
        MyState { counter: 0, phase: Phase::Init },
        MyState { counter: 1, phase: Phase::Init },
    ]
}
```

---

## 3. Properties and Expectations <a name="properties-and-expectations"></a>

**Source:** `src/lib.rs:267-340`

Properties are the specifications you verify. Each has a name, an expectation kind, and a predicate.

### Expectation Kinds

| Kind | Meaning | Use Case |
|------|---------|----------|
| `Always` | Must hold in **every** reachable state | Safety invariants: "no two tasks assigned to same worker", "counter never negative" |
| `Eventually` | Must hold on **every** path to a terminal state (or boundary) | Liveness: "all tasks eventually complete", "system reaches steady state" |
| `Sometimes` | Must hold in **at least one** reachable state | Sanity checks: "it's possible to schedule a task", "error path is reachable" |

### Constructing Properties

```rust
use stateright::Property;

// Safety — must always hold
Property::<Self>::always("name", |model, state| -> bool {
    // return true if invariant holds
    true
});

// Liveness — must eventually hold on all paths
Property::<Self>::eventually("name", |model, state| -> bool {
    // return true when the desired condition is met
    state.all_done()
});

// Reachability — must be possible
Property::<Self>::sometimes("name", |model, state| -> bool {
    // return true when the interesting state is observed
    state.has_error_recovery()
});
```

### How Properties Are Checked

- **Always**: If the checker finds ANY state where the predicate returns `false`, it records a **discovery** (counterexample) — the path from an initial state to the violating state.
- **Eventually**: If the checker finds a path to a terminal state (no outgoing actions or outside boundary) where the predicate was NEVER `true` along that path, it records a discovery.
- **Sometimes**: If after full exploration no state satisfies the predicate, it records a discovery (the property was never witnessed).

### Asserting Results

```rust
let checker = MySystem { param: 5 }
    .checker()
    .spawn_dfs()
    .join();

// Assert ALL properties pass (panics with counterexample if any fail)
checker.assert_properties();

// Assert a specific property was discovered (useful for "sometimes")
checker.assert_any_discovery("can increment");

// Assert a specific property was NOT discovered (no counterexample found)
checker.assert_no_discovery("counter bounded");
```

---

## 4. Checker Configuration and Strategies <a name="checker-configuration"></a>

**Source:** `src/checker.rs:55-292`

### CheckerBuilder

The `Model::checker()` method returns a `CheckerBuilder` for fluent configuration:

```rust
MySystem { param: 5 }
    .checker()                              // create builder
    .threads(num_cpus::get())               // multi-threaded
    .target_max_depth(100.try_into().ok())  // optional depth limit
    .spawn_dfs()                            // choose strategy
    .join()                                 // wait for completion
    .assert_properties();                   // verify
```

### Builder Methods

| Method | Description |
|--------|-------------|
| `.threads(n)` | Number of worker threads (default: available CPUs) |
| `.symmetry()` | Enable symmetry reduction (requires `Representative` impl on `State`) |
| `.target_state_count(n)` | Stop after exploring `n` states |
| `.target_max_depth(n)` | Stop after reaching depth `n` |
| `.finish_when(cond)` | Stop when condition is met (see below) |
| `.timeout(duration)` | Stop after timeout |
| `.visitor(v)` | Attach a visitor for custom observation |

### Checking Strategies

#### BFS — Breadth-First Search

```rust
.spawn_bfs().join()
```

- Finds **shortest** counterexample paths
- Higher memory usage (stores parent pointers for path reconstruction)
- Good for small-to-medium state spaces when minimal counterexamples matter

#### DFS — Depth-First Search (default)

```rust
.spawn_dfs().join()
```

- Lower memory usage (only stores visited set)
- May find longer counterexample paths
- Good default for most models
- Better for checking "eventually" properties

#### Simulation — Random Walk

```rust
use stateright::checker::simulation::UniformChooser;

.spawn_simulation::<UniformChooser>(seed, UniformChooser).join()
```

- Does NOT exhaustively check; randomly samples paths
- Good for very large state spaces where exhaustive checking is infeasible
- Pluggable `Chooser` trait for custom action selection strategies
- Seed for reproducibility

#### On-Demand — Lazy Exploration

```rust
.spawn_on_demand()
```

- Explores states only when requested (e.g., by the web explorer)
- Used internally by `.serve()`

### Finish Conditions

```rust
use stateright::HasDiscoveries;

.finish_when(HasDiscoveries::Any)          // stop on first discovery
.finish_when(HasDiscoveries::AnyFailures)  // stop on first failure (Always/Eventually violation)
.finish_when(HasDiscoveries::AllFailures)  // stop when all failure properties found
.finish_when(HasDiscoveries::All)          // stop when all properties discovered
```

### Reporting Progress

```rust
use stateright::report::WriteReporter;

MySystem { param: 5 }
    .checker()
    .spawn_dfs()
    .report(&mut WriteReporter::new(&mut std::io::stdout()));
```

This prints periodic progress updates showing state count, depth, and elapsed time.

### Checker Results

```rust
let checker = model.checker().spawn_dfs().join();

checker.state_count();         // total states generated
checker.unique_state_count();  // unique states (after deduplication)
checker.max_depth();           // deepest path explored
checker.discoveries();         // HashMap<&str, Path<State, Action>>
checker.is_done();             // whether exploration completed
```

---

## 5. The Actor Framework <a name="the-actor-framework"></a>

Stateright includes a higher-level Actor framework built on top of `Model`, designed for message-passing systems. This is optional — for an orchestrator state machine, using the raw `Model` trait directly is often simpler and more appropriate.

**Source:** `src/actor.rs:302-412`

### When to Use Actor vs. Raw Model

| Use Actor Framework | Use Raw Model |
|---------------------|---------------|
| Multiple communicating processes | Single state machine |
| Need to model network failures | Shared-memory concurrency |
| Message-passing semantics | Direct state transitions |
| Want built-in linearizability testing | Custom state structure |
| Distributed protocols (Paxos, Raft) | Orchestrator / scheduler logic |

### Actor Trait

```rust
pub trait Actor: Sized {
    type Msg: Clone + Debug + Eq + Hash;
    type Timer: Clone + Debug + Eq + Hash;
    type State: Clone + Debug + PartialEq + Hash;
    type Storage: Clone + Debug + PartialEq + Hash;  // persistent across crashes
    type Random: Clone + Debug + Eq + Hash + Ord;

    fn on_start(&self, id: Id, storage: &Option<Self::Storage>, o: &mut Out<Self>) -> Self::State;
    fn on_msg(&self, id: Id, state: &mut Cow<Self::State>, src: Id, msg: Self::Msg, o: &mut Out<Self>);
    fn on_timeout(&self, id: Id, state: &mut Cow<Self::State>, timer: &Self::Timer, o: &mut Out<Self>);
}
```

### ActorModel

`ActorModel` wraps actors into a `Model` implementation:

```rust
use stateright::actor::{ActorModel, Network};

ActorModel::new((), ())                     // (config, init_history)
    .actor(MyActor::Server)
    .actor(MyActor::Client { puts: 2 })
    .property(Expectation::Always, "safe", |_, state| {
        // state is ActorModelState<MyActor>
        state.actor_states[0].is_valid()
    })
    .checker()
    .spawn_dfs()
    .join()
    .assert_properties();
```

### ActorModelState

The composite state of all actors:

```rust
pub struct ActorModelState<A: Actor, H = ()> {
    pub actor_states: Vec<Arc<A::State>>,   // each actor's volatile state
    pub network: Network<A::Msg>,           // pending messages
    pub timers_set: Vec<Timers<A::Timer>>,  // active timers per actor
    pub crashed: Vec<bool>,                 // crash status per actor
    pub history: H,                         // auxiliary variable
    pub actor_storages: Vec<Option<A::Storage>>,  // persistent state
}
```

### Network Semantics

Three built-in network models:

| Variant | Ordering | Delivery | Use Case |
|---------|----------|----------|----------|
| `UnorderedDuplicating` | Unordered | Lossy, may duplicate | Real-world UDP-like |
| `UnorderedNonDuplicating` | Unordered | Lossy, no duplicates | Slightly idealized |
| `Ordered` | FIFO per (src, dst) | Reliable | TCP-like |

### Out — Actor Output Commands

```rust
o.send(dst_id, msg);                    // send message
o.broadcast(&[id1, id2], &msg);         // send to multiple
o.set_timer(timer, duration_range);      // schedule timeout
o.cancel_timer(timer);                   // cancel timeout
o.save(storage);                         // persist non-volatile state
o.choose_random("key", vec![a, b, c]);   // nondeterministic choice
```

### ActorModelAction

Actions generated by the actor model:

```rust
pub enum ActorModelAction<Msg, Timer, Random> {
    Deliver { src: Id, dst: Id, msg: Msg },   // deliver a pending message
    Drop(Envelope<Msg>),                       // drop a message (lossy network)
    Timeout(Id, Timer),                        // fire a timer
    Crash(Id),                                 // crash an actor
    Recover(Id),                               // recover a crashed actor
    SelectRandom { actor: Id, key: String, random: Random },
}
```

---

## 6. Symmetry Reduction <a name="symmetry-reduction"></a>

Symmetry reduction collapses equivalent states to shrink the state space. If your model has interchangeable components (e.g., identical workers), states that differ only by permutation are equivalent.

**Source:** `src/checker/representative.rs`, `src/checker/rewrite.rs`

### The Representative Trait

```rust
pub trait Representative {
    /// Return a canonical form of this state.
    /// States in the same equivalence class must return the same representative.
    fn representative(&self) -> Self;
}
```

### Enabling Symmetry Reduction

1. Implement `Representative` on your `State` type
2. Call `.symmetry()` on the checker builder

```rust
impl Representative for MyState {
    fn representative(&self) -> Self {
        let mut workers = self.workers.clone();
        workers.sort();  // canonical ordering
        Self { workers, ..self.clone() }
    }
}

// Usage
model.checker().symmetry().spawn_dfs().join();
```

### Using RewritePlan (Advanced)

For more complex symmetry involving indexed references:

```rust
use stateright::{Representative, Rewrite, RewritePlan};

impl Representative for TwoPhaseState {
    fn representative(&self) -> Self {
        // Create a plan that sorts rm_state and remaps all indices
        let plan = RewritePlan::from_values_to_sort(&self.rm_state);
        Self {
            rm_state: plan.reindex(&self.rm_state),
            tm_prepared: plan.reindex(&self.tm_prepared),
            msgs: self.msgs.iter().map(|m| match m {
                Message::Prepared { rm } => Message::Prepared { rm: plan.rewrite(rm) },
                other => other.clone(),
            }).collect(),
            ..self.clone()
        }
    }
}
```

### Impact Example (from 2PC)

| Resource Managers | States (no symmetry) | States (with symmetry) | Reduction |
|-------------------|---------------------|----------------------|-----------|
| 5 | 8,832 | 665 | 13x |

---

## 7. Explorer (Interactive Debugging) <a name="explorer"></a>

Stateright includes a web-based state space explorer for interactively debugging counterexamples and understanding state transitions.

**Source:** `src/checker/explorer.rs`

```rust
MySystem { param: 5 }
    .checker()
    .threads(num_cpus::get())
    .serve("localhost:3000");
```

Then open `http://localhost:3000` in a browser.

**Features:**
- Visual state transition graph
- Step through counterexample paths
- Sequence diagrams (for actor models)
- Jump between discovered property violations
- BFS-backed for shortest paths

**Best for:** State spaces up to hundreds of thousands of states. For larger spaces, use CLI-based checking.

---

## 8. Utilities <a name="utilities"></a>

### HashableHashSet / HashableHashMap

**Source:** `src/util.rs`

Wrappers around standard `HashSet`/`HashMap` that implement `Hash` (by sorting elements). Required when your state contains sets or maps.

```rust
use stateright::util::HashableHashSet;

#[derive(Clone, Debug, Hash, PartialEq)]
struct MyState {
    pending: HashableHashSet<TaskId>,
}
```

### VectorClock

```rust
use stateright::util::VectorClock;
```

Standard vector clock implementation for causal ordering.

### Fingerprint

States are identified by a `Fingerprint` (stable `u64` hash). This is used internally for deduplication and is generally not needed directly.

---

## 9. Patterns and Idioms <a name="patterns-and-idioms"></a>

### Pattern: Model Struct Holds Configuration

The `Model` implementor typically holds system parameters, not the state itself:

```rust
#[derive(Clone)]
struct Scheduler {
    worker_count: usize,
    task_count: usize,
    max_retries: u32,
}

impl Model for Scheduler {
    type State = SchedulerState;  // separate type
    type Action = SchedulerAction;
    // ...
}
```

### Pattern: Self as State

For simple models, the model struct itself can be the state (as in the increment example):

```rust
impl Model for State {
    type State = State;
    type Action = Action;

    fn init_states(&self) -> Vec<Self::State> {
        vec![self.clone()]
    }
    // ...
}
```

### Pattern: Nondeterministic Choices via Multiple Actions

Model nondeterminism by returning multiple actions from `actions()`:

```rust
fn actions(&self, state: &Self::State, actions: &mut Vec<Self::Action>) {
    for task in &state.pending_tasks {
        for worker in &state.available_workers {
            actions.push(Action::Assign { task: *task, worker: *worker });
        }
    }
    // The checker explores ALL combinations
}
```

### Pattern: Interleaving Concurrent Operations

Model concurrent subsystems by allowing any one to step:

```rust
fn actions(&self, state: &Self::State, actions: &mut Vec<Self::Action>) {
    // Any pending request can be processed
    for (i, req) in state.requests.iter().enumerate() {
        if req.status == Status::Pending {
            actions.push(Action::ProcessRequest(i));
        }
    }
    // Any callback can fire
    for (i, cb) in state.callbacks.iter().enumerate() {
        if cb.ready {
            actions.push(Action::FireCallback(i));
        }
    }
    // External events can arrive
    if state.can_accept_work {
        actions.push(Action::NewTaskArrives);
    }
}
```

### Pattern: Bounding the State Space

Use `within_boundary` to keep exploration finite:

```rust
fn within_boundary(&self, state: &Self::State) -> bool {
    state.total_tasks_created <= 3
        && state.time_steps <= 10
}
```

Or generate a bounded number of actions:

```rust
fn actions(&self, state: &Self::State, actions: &mut Vec<Self::Action>) {
    // Only allow new tasks up to a limit
    if state.tasks.len() < self.max_tasks {
        actions.push(Action::SubmitTask);
    }
}
```

### Pattern: Testing in `#[test]`

```rust
#[cfg(test)]
#[test]
fn check_scheduler_properties() {
    let checker = Scheduler { worker_count: 2, task_count: 3, max_retries: 1 }
        .checker()
        .spawn_dfs()
        .join();
    checker.assert_properties();
}
```

### Pattern: CLI with Check/Explore Subcommands

```rust
fn main() {
    match args.subcommand().as_deref() {
        Some("check") => {
            model.checker()
                .threads(num_cpus::get())
                .spawn_dfs()
                .report(&mut WriteReporter::new(&mut std::io::stdout()));
        }
        Some("explore") => {
            model.checker()
                .threads(num_cpus::get())
                .serve("localhost:3000");
        }
        _ => { /* usage */ }
    }
}
```

---

## 10. Complete Examples <a name="complete-examples"></a>

### Example: Two-Phase Commit (from `examples/2pc.rs`)

This is the best reference for a non-actor, protocol-level model. It demonstrates:
- Modeling a coordinator + multiple participants
- Message sets as part of state
- Safety property (consistency: no RM committed while another aborted)
- Reachability properties (both commit and abort agreement are possible)
- Symmetry reduction with `Representative` and `RewritePlan`

Key structure:

```rust
// State includes all participant states, coordinator state, and messages
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct TwoPhaseState {
    rm_state: Vec<RmState>,       // per-participant state
    tm_state: TmState,            // coordinator state
    tm_prepared: Vec<bool>,       // coordinator's knowledge of participants
    msgs: BTreeSet<Message>,      // in-flight messages
}

// Actions model all possible steps any component can take
#[derive(Clone, Debug, PartialEq)]
enum Action {
    TmRcvPrepared(R),
    TmCommit,
    TmAbort,
    RmPrepare(R),
    RmChooseToAbort(R),
    RmRcvCommitMsg(R),
    RmRcvAbortMsg(R),
}
```

### Example: Concurrent Increment (from `examples/increment.rs`)

Demonstrates shared-memory concurrency modeling:
- Multiple threads with program counters
- Shared mutable state
- Race condition detection
- Simple symmetry reduction (sort thread states)

### Other Examples in Repository

| File | Protocol | Key Concepts |
|------|----------|-------------|
| `examples/paxos.rs` | Single Decree Paxos | Consensus, quorums, actor framework |
| `examples/raft.rs` | Raft | Leader election, log replication |
| `examples/2pc.rs` | Two-Phase Commit | Raw Model trait, symmetry |
| `examples/increment.rs` | Concurrent counter | Shared memory, race conditions |
| `examples/linearizable-register.rs` | ABD algorithm | Linearizability, quorum reads/writes |

---

## 11. Performance Tips <a name="performance-tips"></a>

### Always Use Release Mode

Model checking is compute-intensive. Always build and test with `--release`:

```sh
cargo test --release
cargo run --release --example 2pc -- check 7
```

For maximum performance:

```sh
RUSTFLAGS='-C target-cpu=native' cargo test --release
```

### State Space Size

The state space grows **exponentially** with model parameters. Start small and increase gradually:

```rust
#[test]
fn small_model() {
    // Start with minimal parameters
    Scheduler { workers: 2, tasks: 2, max_retries: 1 }
        .checker().spawn_dfs().join()
        .assert_properties();
}

#[test]
#[ignore]  // run with: cargo test --release -- --ignored
fn larger_model() {
    Scheduler { workers: 3, tasks: 4, max_retries: 2 }
        .checker().threads(num_cpus::get()).spawn_dfs().join()
        .assert_properties();
}
```

### Reducing State Space

1. **Minimize state representation** — only include what's necessary for correctness
2. **Use `within_boundary`** — bound counters, queue lengths, etc.
3. **Symmetry reduction** — collapse equivalent states
4. **Avoid unnecessary nondeterminism** — don't model choices that don't affect correctness
5. **Use `BTreeSet`/`BTreeMap`** over `HashSet`/`HashMap` in state for deterministic hashing
6. **Prune impossible actions** — be precise in `actions()` about what's truly enabled

### Choosing a Checker Strategy

| Strategy | When to Use |
|----------|-------------|
| DFS | Default choice; good memory efficiency |
| BFS | When you need shortest counterexamples |
| Simulation | State space too large for exhaustive search |
| On-Demand | Interactive exploration via web UI |

### Multi-threading

```rust
.checker()
    .threads(num_cpus::get())  // use all cores
    .spawn_dfs()
```

Stateright uses lock-free concurrent data structures (`DashMap`/`DashSet`) internally, so multi-threading scales well.

---

## Appendix: Derive Requirements Summary

| State field type | Required derives/traits |
|-----------------|------------------------|
| Enums in state | `Clone, Debug, Hash, PartialEq, Eq` |
| Collections in state | Use `BTreeSet`/`BTreeMap` (implement `Hash`), or `HashableHashSet`/`HashableHashMap` |
| `Vec<T>` in state | `T` must be `Hash + PartialEq + Clone + Debug` |
| Actions | `Clone, Debug, PartialEq` |
| For symmetry | Implement `Representative` on State |

## Appendix: Key Imports

```rust
// Core model checking
use stateright::{Model, Property, Checker};

// Reporting
use stateright::report::WriteReporter;

// Symmetry reduction
use stateright::{Representative, Rewrite, RewritePlan};

// Actor framework (if needed)
use stateright::actor::{Actor, ActorModel, ActorModelState, Id, Out, Network};

// Utilities
use stateright::util::{HashableHashSet, HashableHashMap};

// Consistency testing
use stateright::semantics::{LinearizabilityTester, SequentialSpec};
```
