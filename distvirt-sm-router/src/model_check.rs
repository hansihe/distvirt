//! # Model Checking Support for the Signal Router
//!
//! This module documents the design for integrating stateright model checking
//! with the signal router framework. The goal is to enable both individual SM
//! verification and full compositional model checking of the router + all SMs
//! together.
//!
//! ## Why model check the router?
//!
//! The old SMs are already checked by stateright individually. The new framework
//! enables something the old architecture couldn't do: **compositional model
//! checking** — verifying the entire SM graph together, including signal
//! propagation, edge topology changes, and delivery ordering effects.
//!
//! The design doc's **lattice property** ("any path through the state space
//! should reach the same final state regardless of delivery order within a
//! sub-round") is exactly what stateright is built to verify.
//!
//! ## Two levels of model checking
//!
//! ### Level 1: Individual SM model checking
//!
//! Same as the existing stateright tests, but cleaner:
//! - The generated input enum gives an exhaustive, typed list of every possible input.
//! - Outputs are declarative (signal values + edge sets) rather than imperative side-effect lists.
//! - The SM never touches relationship bookkeeping, so the state space is smaller.
//!
//! This works today without any framework changes — just instantiate the SM,
//! feed inputs, inspect state. The `SmHandler` trait is already decoupled from
//! the router.
//!
//! ### Level 2: Compositional model checking
//!
//! Model-check the **entire router** with all SMs. The stateright model state
//! is the full router state. External actions (port signal changes, port
//! add/remove, events) trigger sub-rounds, and stateright explores all valid
//! delivery orderings within each sub-round.
//!
//! This catches interaction bugs, cascade issues, and delivery-order-dependent
//! behavior — things no amount of individual SM testing finds.
//!
//! ## Key insight: per-SM independence within a sub-round
//!
//! Within a single sub-round (input or event), SM handlers are **fully
//! independent** — no handler observes another handler's effects in the same
//! sub-round. All outputs (signal changes, edge changes, events, creates) go
//! into queues processed in subsequent sub-rounds.
//!
//! This means the order in which *different* SMs are processed within a
//! sub-round does not affect the outcome. The **only meaningful reordering** is
//! when a single SM receives multiple deliveries in the same sub-round (e.g.,
//! two dirty inputs, or three events). The handler is called sequentially for
//! each, and the SM's internal state mutates between calls.
//!
//! ### State space reduction
//!
//! This independence property dramatically reduces the state space. Instead of
//! exploring all permutations of N pending deliveries (`N!`), we only need to
//! explore permutations **per SM**. If SM A has 2 pending inputs and SM B has
//! 3 pending inputs, the total orderings to explore are `2! × 3! = 12`, not
//! `5! = 120`. SMs with only 1 pending delivery (the common case) contribute
//! a factor of 1.
//!
//! Furthermore, the model checker respects the sub-round phasing that the
//! runtime guarantees: all dirty inputs are processed before any events within
//! a loop iteration. There is no need to explore interleavings across this
//! barrier — such orderings cannot occur in production, so exploring them
//! would only bloat the state space with unreachable states.
//!
//! ## `model_checkable` macro flag
//!
//! The `router!` macro accepts an optional `model_checkable` keyword that
//! enables model checking support. This:
//!
//! 1. Adds `Clone + Hash + Eq` bounds on SM handler types and signal value types.
//! 2. Generates a `RouterSnapshot` type for state deduplication.
//! 3. Generates step-by-step propagation methods.
//! 4. Generates signal/edge/instance accessor methods.
//! 5. Uses per-creator-instance ID generation instead of a global counter (see
//!    "Deterministic ID generation" below).
//!
//! ```rust,ignore
//! router! {
//!     model_checkable
//!
//!     state_machines { ... }
//!     ports { ... }
//!     signals { ... }
//!     edges { ... }
//!     events { ... }
//!     inputs { ... }
//! }
//! ```
//!
//! Production topologies omit `model_checkable` and pay no cost.
//!
//! ## Core features
//!
//! ### Router Clone
//!
//! Stateright's fundamental operation is `clone state -> apply action -> get new
//! state`. The macro derives `Clone` on the generated `Router` struct when
//! `model_checkable` is set. This propagates: all SM handler types, signal
//! value types, and aggregator output types must be `Clone`.
//!
//! ### RouterSnapshot (Hash + Eq for state dedup)
//!
//! Stateright deduplicates states via `Hash + Eq`. Rather than requiring these
//! on `Router` directly (which includes transient state like the dirty queue),
//! the macro generates a `RouterSnapshot` struct containing only meaningful
//! state:
//!
//! - SM instance states (cloned from instance maps)
//! - All signal values
//! - All edge sets (forward maps only — reverse is derived)
//! - Auto-ID counters (but see "Deterministic ID generation" below)
//!
//! Excluded from snapshot (transient processing state):
//! - Dirty queue (`VecDeque<DirtyInput>`)
//! - Pending events queue (`VecDeque<PendingEvent>`)
//!
//! Two routers with the same SMs/signals/edges but different dirty queue
//! contents are semantically the same state at different points in processing.
//!
//! ```rust,ignore
//! impl Router {
//!     /// Capture meaningful state for stateright dedup.
//!     fn snapshot(&self) -> RouterSnapshot;
//!
//!     /// Reconstruct a working Router from a snapshot.
//!     fn from_snapshot(snapshot: &RouterSnapshot, depth_limit: usize) -> Self;
//! }
//! ```
//!
//! ### Step-by-step propagation
//!
//! The existing `propagate()` resolves all cascading effects atomically. For
//! model checking, we need to explore delivery orderings within sub-rounds.
//!
//! The router tracks a `ManualPhase` state machine:
//!
//! ```text
//! Idle ──begin_manual_propagate()──→ Inputs(N)
//!          materialize creates,
//!          drain dirty
//!
//! Inputs(0) ──begin_manual_propagate()──→ Events(M)
//!               drain pending_events
//!
//! Events(0) ──begin_manual_propagate()──→ Inputs(N)
//!               materialize creates,        (new round)
//!               drain dirty
//!
//! Inputs(n>0) or Events(n>0) ──begin_manual_propagate()──→ PANIC
//! ```
//!
//! The API uses an external `ManualPropagate` controller that sorts deliveries
//! by target SM and yields them one group at a time:
//!
//! ```rust,ignore
//! impl Router {
//!     /// Begin the next sub-round of step-by-step propagation.
//!     ///
//!     /// From `Idle` or `Events(0)`: materializes creates, drains dirty
//!     /// inputs, transitions to `Inputs(N)`.
//!     /// From `Inputs(0)`: drains pending events, transitions to `Events(M)`.
//!     /// Panics if outstanding deliveries remain.
//!     fn begin_manual_propagate(&mut self) -> ManualPropagate<PendingDelivery>;
//!
//!     /// Process exactly one pending delivery. Decrements the outstanding
//!     /// delivery counter in the current phase. Effects are buffered for
//!     /// subsequent sub-rounds.
//!     fn deliver_one(&mut self, delivery: PendingDelivery);
//!
//!     /// True when no pending deliveries remain and no dirty state exists.
//!     /// Valid from `Idle` or `Events(0)` only.
//!     fn is_quiescent(&self) -> bool;
//!
//!     /// Existing method — resolves everything. Convenience, not for model
//!     /// checking. Valid from `Idle` or `Events(0)`.
//!     fn propagate(&mut self);
//! }
//! ```
//!
//! #### Usage protocol
//!
//! Each round requires exactly two `begin_manual_propagate` calls — one for
//! the inputs sub-round and one for the events sub-round:
//!
//! ```rust,ignore
//! loop {
//!     // Inputs sub-round
//!     let mut mp = router.begin_manual_propagate();
//!     while let Some(group) = mp.next_group() {
//!         for d in group { router.deliver_one(d); }
//!     }
//!     // Events sub-round
//!     let mut mp = router.begin_manual_propagate();
//!     while let Some(group) = mp.next_group() {
//!         for d in group { router.deliver_one(d); }
//!     }
//!     if router.is_quiescent() { break; }
//! }
//! ```
//!
//! `next_group()` returns all deliveries targeting the same SM within the
//! current sub-round. The model checker only needs to permute within each
//! group — different SMs are independent within a sub-round.
//!
//! #### Per-SM permutation grouping
//!
//! Because different SMs are independent within a sub-round, `ManualPropagate`
//! sorts deliveries by target SM and yields one group at a time via
//! `next_group()`. The model checker only explores permutations within each
//! group, taking the cartesian product across groups.
//!
//! ### Signal, edge, and instance accessors
//!
//! For writing stateright properties ("workload has demand iff demand_count >
//! 0", "no orphan pods", etc.):
//!
//! ```rust,ignore
//! impl Router {
//!     // Signal value accessors (generated per signal):
//!     fn get_service_demand(&self, id: &ServiceId) -> Option<&bool>;
//!     fn get_workload_readiness(&self, id: &WorkloadId) -> Option<&Option<ReadyInfo>>;
//!
//!     // Edge accessors (generated per edge type):
//!     fn get_service_to_workload_targets(&self, source: &ServiceId) -> &[WorkloadId];
//!     fn get_service_to_workload_sources(&self, target: &WorkloadId) -> &BTreeSet<ServiceId>;
//!
//!     // Instance enumeration (generated per SM/port type):
//!     fn service_ids(&self) -> impl Iterator<Item = ServiceId>;
//!     fn workload_ids(&self) -> impl Iterator<Item = WorkloadId>;
//!     fn has_service(&self, id: &ServiceId) -> bool;
//! }
//! ```
//!
//! ## Deterministic ID generation
//!
//! ### The problem
//!
//! Auto-generated IDs use a global counter (`next_pod_id: u64`). If two
//! delivery orderings both cause pod creation, but in different order relative
//! to other creations, they assign different IDs to semantically identical pods.
//! Stateright sees these as different states — the state space explodes with
//! false branches.
//!
//! Example: Workload A and Workload B both create a pod in the same round.
//! - Ordering 1: A creates first -> `PodId(5)`, B creates -> `PodId(6)`
//! - Ordering 2: B creates first -> `PodId(5)`, A creates -> `PodId(6)`
//!
//! Same topology, different IDs. False divergence.
//!
//! Note: the per-SM independence property means creates from different SMs
//! within a sub-round are never reordered relative to each other (creates are
//! buffered and materialized deterministically). However, creates from
//! different sub-rounds (e.g., cascading effects) can still hit this problem.
//!
//! ### The solution: per-creator-instance counters
//!
//! Under `model_checkable`, auto-ID generation changes from a global counter to
//! a **per-creator-instance counter**. The ID encodes `(creator_id,
//! creation_sequence_number)`:
//!
//! - Workload A's 1st pod: `PodId(WorkloadId(A), 0)`
//! - Workload A's 2nd pod: `PodId(WorkloadId(A), 1)`
//! - Workload B's 1st pod: `PodId(WorkloadId(B), 0)`
//!
//! Now delivery order doesn't matter — Workload A's 1st pod always gets the
//! same ID regardless of when B's creation runs. IDs are deterministic per
//! causal origin.
//!
//! The per-creator counter is stored per SM instance in the router (e.g., a
//! `BTreeMap<WorkloadId, u64>` for pod creation counts). The `Ctx` loads the
//! current counter for the active SM and increments it on each `create_*` call.
//!
//! This only affects `model_checkable` mode. Production code keeps the simpler
//! global counter.
//!
//! ### ID type under model_checkable
//!
//! For auto-ID types, the generated ID becomes a tuple-like struct:
//!
//! ```rust,ignore
//! // Normal mode:
//! #[derive(Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug)]
//! struct PodId(u64);
//!
//! // model_checkable mode:
//! #[derive(Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug)]
//! struct PodId(u64, u64);  // (creator_id_numeric, sequence)
//! // or: struct PodId { creator: u64, seq: u64 }
//! ```
//!
//! Since this only exists under `model_checkable`, the compound ID type doesn't
//! affect production code. SMs interact with IDs opaquely (they never inspect
//! the inner value), so the change is transparent to SM logic.
//!
//! Edge case: IDs created via `router.create_pod()` (external, not from an SM
//! handler) need a creator identity too. This could use a reserved sentinel
//! creator ID (e.g., `u64::MAX`) with its own sequence counter, or the caller
//! could provide a creator tag.
//!
//! ## Stateright model structure
//!
//! The intended stateright model uses a two-phase approach:
//!
//! ```rust,ignore
//! #[derive(Clone, Hash, Eq, PartialEq)]
//! struct ModelState {
//!     snapshot: RouterSnapshot,
//!     phase: Phase,
//! }
//!
//! #[derive(Clone, Hash, Eq, PartialEq)]
//! enum Phase {
//!     /// Quiescent — ready for external actions.
//!     External,
//!     /// Processing pending deliveries within a sub-round.
//!     Delivering,
//! }
//!
//! enum Action {
//!     // External triggers (only in External phase)
//!     SetPortSignal { /* ... */ },
//!     AddPort { /* ... */ },
//!     RemovePort { /* ... */ },
//!     SendEvent { /* ... */ },
//!
//!     // Internal delivery choice (only in Delivering phase)
//!     Deliver(PendingDelivery),
//! }
//! ```
//!
//! When `phase == External`:
//! - Actions are external triggers (port signal changes, port add/remove,
//!   events).
//! - Applying an action transitions to `Delivering`.
//!
//! When `phase == Delivering`:
//! - Call `pending_deliveries()` once to get the full sub-round set.
//! - If empty: transition back to `External`.
//! - If non-empty: the stateright model generates one successor state per
//!   permutation of the deliveries (grouped per-SM — see below). Each
//!   permutation calls `deliver_one()` for every item in the set, then
//!   calls `pending_deliveries()` again. If the next call returns items,
//!   remain in `Delivering`; if empty, transition to `External`.
//!
//! ### Optimized exploration via per-SM grouping
//!
//! A naive model checker would explore all `N!` permutations of pending
//! deliveries. Since different SMs are independent within a sub-round, an
//! optimized checker can:
//!
//! 1. Group pending deliveries by target SM.
//! 2. For each SM with >1 pending delivery, explore all permutations of that
//!    SM's deliveries.
//! 3. Take the cartesian product across SM groups.
//!
//! This reduces the state space from `N!` to `∏(nᵢ!)` where `nᵢ` is the
//! number of pending deliveries for SM `i`. In practice most SMs have 0 or 1
//! pending delivery per sub-round, so the branching factor is small.
//!
//! ### Lattice property verification
//!
//! The lattice property falls out naturally from this model structure. If two
//! delivery orderings within a sub-round lead to different quiescent states,
//! stateright's state space branches at the `Delivering` phase. Safety
//! properties checked in the `External` phase will detect any divergence.
//!
//! For example, if the property "pod count equals demand when quiescent" holds
//! in all `External`-phase states, then delivery ordering can't affect it. Any
//! ordering-dependent bug shows up as a property violation in at least one of
//! the explored orderings.
//!
//! For direct lattice checking: a property can verify that all `External`-phase
//! states reachable from the same preceding `External`-phase state + same
//! external action are identical. This requires tracking the "parent external
//! state" which can be encoded in the model state if needed.
//!
//! ### Standalone lattice verifier
//!
//! For targeted testing outside of full stateright exploration:
//!
//! ```rust,ignore
//! /// Check that all delivery orderings within each sub-round reach the
//! /// same quiescent state. Call after an external action, before propagate().
//! ///
//! /// Only permutes deliveries per-SM (exploiting the independence property),
//! /// keeping the number of orderings tractable.
//! fn verify_lattice(router: &Router) -> Result<(), LatticeViolation> {
//!     let mut mp = router.begin_manual_propagate();
//!     // Collect all groups
//!     let mut groups = Vec::new();
//!     while let Some(group) = mp.next_group() {
//!         groups.push(group);
//!     }
//!
//!     // Only groups with >1 delivery need permutation
//!     let multi: Vec<&Vec<PendingDelivery>> = groups
//!         .iter()
//!         .filter(|g| g.len() > 1)
//!         .collect();
//!
//!     if multi.is_empty() { return Ok(()); }
//!
//!     // Explore cartesian product of per-SM permutations
//!     let mut reference: Option<RouterSnapshot> = None;
//!     for ordering in cartesian_permutations(&multi) {
//!         let mut r = router.clone();
//!         for delivery in ordering {
//!             r.deliver_one(delivery);
//!         }
//!         // Resolve remaining cascades canonically
//!         r.propagate();
//!         let snap = r.snapshot();
//!         match &reference {
//!             None => reference = Some(snap),
//!             Some(ref_snap) if snap != *ref_snap => {
//!                 return Err(LatticeViolation { /* ... */ });
//!             }
//!             _ => {}
//!         }
//!     }
//!     Ok(())
//! }
//! ```
//!
//! ## Zero-overhead design
//!
//! The step-by-step propagation methods (`begin_manual_propagate`, `deliver_one`,
//! `is_quiescent`) share the same per-item processing logic as `propagate()`.
//! The difference is only in the driving loop:
//!
//! - **Production:** `propagate()` eagerly drains all queues in a tight loop.
//!   No virtual dispatch, no trait objects, no overhead from the model checking
//!   API existing.
//! - **Model checking:** The caller drives the loop externally via
//!   `begin_manual_propagate()` + `next_group()` + `deliver_one()`, allowing
//!   stateright to explore ordering choices.
//!
//! Both paths call the same generated aggregate, handler invocation, and effect
//! application methods. The model checking entry points simply aren't called in
//! production code.
//!
//! ## Implementation plan
//!
//! 1. **Signal/edge/instance accessors** — generate read-only getters on
//!    `Router`. Low effort, immediately useful for writing properties in
//!    existing tests too.
//!
//! 2. **`is_quiescent()`** — trivial, useful immediately.
//!
//! 3. **`Router: Clone`** under `model_checkable` — derive Clone on generated
//!    Router, add Clone bound to SM/signal types.
//!
//! 4. **Step-by-step propagation** — `pending_deliveries()` + `deliver_one()`.
//!    Extract per-item processing from the propagation loop into shared methods.
//!    `pending_deliveries()` respects sub-round phasing internally.
//!
//! 5. **`RouterSnapshot`** with Hash+Eq — for stateright state dedup.
//!
//! 6. **Per-creator-instance IDs** — change auto-ID generation under
//!    `model_checkable` to eliminate false state divergence from ID ordering.
//!
//! 7. **Lattice verifier utility** — standalone function using per-SM
//!    permutation grouping for tractable targeted testing (e.g., with proptest
//!    generating random topologies).
