//! # Model Checking
//!
//! Support for verifying signal router topologies with
//! [stateright](https://docs.rs/stateright) or similar model checkers.
//!
//! ## Getting started
//!
//! Add the `model_checkable` flag to your `router!` declaration:
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
//! This enables:
//! - `Clone` on the generated `Router` (requires all SM handler types, signal
//!   value types, and aggregator output types to be `Clone`)
//! - `RouterSnapshot` with `Hash + Eq` for state deduplication
//! - Step-by-step propagation methods for exploring delivery orderings
//! - Signal, edge, and instance accessor methods for writing properties
//!
//! Note: `model_checkable` does **not** change the ID allocation strategy.
//! The default [`SequentialIds`](distvirt_sm_router::SequentialIds) allocator
//! is still used. For deterministic IDs (important for state deduplication),
//! implement a custom [`IdAllocator`](distvirt_sm_router::IdAllocator) — see
//! [Deterministic ID generation](#deterministic-id-generation) below.
//!
//! Production topologies omit `model_checkable` and pay no cost.
//!
//! ## Two levels of model checking
//!
//! ### Level 1: Individual SM verification
//!
//! Test a single SM type in isolation using the generated `{Sm}CtxConcrete`.
//! Feed inputs, inspect effects and internal state. The `SmHandler` trait is
//! decoupled from the router, so no router setup is needed:
//!
//! ```rust,ignore
//! let mut alloc = SequentialIds::<NodeKind>::new();
//! let mut ctx = WorkloadCtxConcrete::new(wl_id, &mut alloc);
//! sm.handle(WorkloadInput::DemandInput(demand), &mut ctx);
//! let effects = ctx.into_effects();
//! // check effects.readiness, effects.pending_creates, etc.
//! ```
//!
//! The generated input enum gives an exhaustive list of possible inputs,
//! making it easy to enumerate the SM's input space in a stateright model.
//!
//! ### Level 2: Compositional verification
//!
//! Model-check the **entire router** with all SMs together. This catches
//! interaction bugs, cascade issues, and delivery-order-dependent behavior
//! that individual SM testing cannot find.
//!
//! ## RouterSnapshot
//!
//! `RouterSnapshot` captures the **meaningful** router state for stateright's
//! `Hash + Eq` state deduplication:
//!
//! - SM instance states
//! - All signal values
//! - All edge sets (forward maps only — reverse maps are derived)
//! - ID allocator state
//!
//! Excluded (transient processing state):
//! - Dirty queue, pending events queue, pending creates
//!
//! Two routers with the same SMs/signals/edges but different queue contents
//! are the same state at different points in processing.
//!
//! ```rust,ignore
//! // Capture state
//! let snap = router.snapshot();
//!
//! // Reconstruct a working router (rebuilds reverse maps, initializes queues)
//! let restored = Router::from_snapshot_traced(&snap, 16, NoopTracer);
//! ```
//!
//! ## Step-by-step propagation
//!
//! The normal `propagate()` resolves everything atomically. For model checking,
//! step-by-step propagation lets you explore delivery orderings within
//! sub-rounds.
//!
//! ### ManualPhase state machine
//!
//! ```text
//! Idle ──begin_manual_propagate()──→ Inputs(N)
//!          materialize creates,
//!          drain dirty
//!
//! Inputs(0) ──begin_manual_propagate()──→ Events(M)
//!               drain pending_events
//!
//! Events(0) ──begin_manual_propagate()──→ Inputs(N)   (new round)
//!
//! Inputs(n>0) or Events(n>0) ──begin_manual_propagate()──→ PANIC
//! ```
//!
//! ### Usage protocol
//!
//! Each round requires two `begin_manual_propagate` calls — one for inputs,
//! one for events:
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
//! `next_group()` returns all deliveries targeting the same SM. The model
//! checker only permutes *within* each group (different SMs are independent
//! within a sub-round), then takes the cartesian product across groups.
//!
//! ### State space reduction
//!
//! Because different SMs are independent within a sub-round, the state space
//! is `∏(nᵢ!)` (product of per-SM delivery counts) instead of `N!` (all
//! permutations). In practice most SMs have 0 or 1 pending delivery per
//! sub-round, so the branching factor is small.
//!
//! ## Stateright integration
//!
//! A typical stateright model uses a two-phase state:
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
//!     External,    // quiescent — ready for external actions
//!     Delivering,  // processing pending deliveries
//! }
//!
//! enum Action {
//!     // External triggers (only in External phase)
//!     SetPortSignal { /* ... */ },
//!     AddPort { /* ... */ },
//!     RemovePort { /* ... */ },
//!     SendEvent { /* ... */ },
//!     // Internal delivery (only in Delivering phase)
//!     Deliver(PendingDelivery),
//! }
//! ```
//!
//! When `External`: apply an external action, transition to `Delivering`.
//! When `Delivering`: get pending deliveries, explore per-SM permutations,
//! transition back to `External` when quiescent.
//!
//! ### Writing properties
//!
//! Use the generated accessor methods to write safety/liveness properties:
//!
//! ```rust,ignore
//! // Signal accessors:
//! router.get_service_demand(&svc_id)        // -> Option<&bool>
//! router.get_workload_readiness(&wl_id)     // -> Option<&Option<ReadyInfo>>
//!
//! // Edge accessors:
//! router.get_service_to_workload_targets(&svc_id)  // -> &[WorkloadId]
//! router.get_service_to_workload_sources(&wl_id)   // -> &BTreeSet<ServiceId>
//!
//! // Instance enumeration:
//! router.service_ids()     // -> impl Iterator<Item = ServiceId>
//! router.has_service(&id)  // -> bool
//! ```
//!
//! ### Lattice property verification
//!
//! The **lattice property** — any delivery ordering within a sub-round
//! produces the same quiescent state — falls out naturally. If two orderings
//! diverge, stateright's state space branches at the `Delivering` phase, and
//! safety properties will detect the difference.
//!
//! For targeted lattice checking without full exploration:
//!
//! ```rust,ignore
//! fn verify_lattice(router: &Router) -> Result<(), LatticeViolation> {
//!     let mut mp = router.begin_manual_propagate();
//!     let mut groups = Vec::new();
//!     while let Some(group) = mp.next_group() {
//!         groups.push(group);
//!     }
//!     // Explore cartesian product of per-SM permutations
//!     let mut reference: Option<RouterSnapshot> = None;
//!     for ordering in cartesian_permutations(&groups) {
//!         let mut r = router.clone();
//!         for d in ordering { r.deliver_one(d); }
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
//! ## Deterministic ID generation
//!
//! ### The problem
//!
//! The default [`SequentialIds`](distvirt_sm_router::SequentialIds) allocator
//! uses a global counter. If two delivery orderings both cause SM creation
//! but in different order, they assign different IDs to semantically identical
//! instances. Stateright sees these as different states — the state space
//! explodes with false branches.
//!
//! ### The solution
//!
//! The router and `{Sm}CtxConcrete` are generic over
//! [`IdAllocator`](distvirt_sm_router::IdAllocator), which you can implement
//! to provide deterministic allocation. No deterministic allocator is provided
//! out of the box — you write one tailored to your model.
//!
//! A common approach is **per-creator-instance counters**, where the ID
//! encodes `(creator_id, sequence_number)`:
//!
//! ```rust,ignore
//! // Default (SequentialIds):
//! // Workload A creates pod -> PodId(5)
//! // Workload B creates pod -> PodId(6)
//! // Order-dependent — different orderings produce different IDs
//!
//! // Custom deterministic allocator:
//! // Workload A's 1st pod -> PodId(A, 0)
//! // Workload B's 1st pod -> PodId(B, 0)
//! // Order-independent — same result regardless of creation order
//! ```
//!
//! The `IdAllocator::alloc()` method receives a `creator` parameter
//! (`Some((kind, raw_id))` when called from an SM handler) which you can
//! use to implement per-creator counters. SMs interact with IDs opaquely
//! (never inspect inner values), so the ID scheme is transparent to SM logic.
//!
//! ## Design rationale
//!
//! ### Per-SM independence
//!
//! Within a sub-round, SM handlers are fully independent — no handler observes
//! another's effects. All outputs go into queues for subsequent sub-rounds.
//! This means cross-SM ordering within a sub-round doesn't matter, and only
//! intra-SM delivery ordering needs exploration.
//!
//! ### Zero overhead
//!
//! Step-by-step propagation shares the same per-item processing logic as
//! `propagate()`. The difference is only the driving loop:
//!
//! - **Production:** `propagate()` eagerly drains all queues.
//! - **Model checking:** the caller drives via `begin_manual_propagate()` +
//!   `deliver_one()`.
//!
//! Both call the same generated aggregation, handler invocation, and effect
//! application code. Model checking entry points aren't called in production.
