# Signal Router — Implementation Plan

Implements the design in [signal-router.md](signal-router.md).

Scope: phases 1-4 (core engine through macro). Integration into orchestrator (phase 5) is separate work.

## Decisions

- **Crate name:** `distvirt-sm-router`
- **Macro approach:** Use [crabtime](https://github.com/audulus/crabtime) for the `router!` macro — keeps everything in one crate instead of needing a separate proc-macro crate.
- **Testing strategy:** Phases 1-3 use hand-written manual wiring. Phase 4 replicates a subset of those tests using macro-generated code, verifying identical behavior. The hand-written tests serve as the spec the macro must match.
- **SM ownership model:** SM instances are user-defined structs stored inside the router. There is a single `SmHandler` trait (defined in `lib.rs`, topology-independent) with associated types `Input` and `Ctx`. The macro generates the `Input` enum and `Ctx` struct per SM type; the user implements `SmHandler` on their SM struct. The router owns `HashMap<SmId, SmStruct>` and dispatches via `sm.handle(input, &mut ctx)` — no external handler object, no per-SM-type traits. SM state is colocated with its handler. Uses take-call-reinsert pattern during dispatch to avoid borrow conflicts. Side effects that escape the SM system (gRPC calls, scheduler requests, etc.) flow through the `Ctx` or are collected as descriptors returned after the round — orthogonal to SM storage.

## Phase 1 — Core types and manual wiring

Core engine without the macro. All wiring done manually so we can validate propagation semantics in isolation.

### Deliverables
- Core types: instance IDs, signal storage, edge graph representation
- `Aggregator` trait
- Router struct: edge management, signal storage, propagation loop
- Round execution: trigger -> aggregate -> deliver -> cascade -> quiescence
- Change detection via `PartialEq` (no propagation if signal unchanged)
- Runtime depth limiting with warning at N-1, panic at N

### Tests
- [x] Basic signal propagation: set signal, verify aggregated input delivered
- [x] Change detection: set same value, verify no delivery
- [x] Multi-edge aggregation: N sources into one target
- [x] Cascading: signal change -> edge change -> further propagation
- [x] Depth limiting: artificial cycle hits limit
- [x] Edge removal triggers re-aggregation (bonus)
- [x] Batched changes propagate in single round (bonus)

### Notes

Phase 1 complete. Key implementation decisions:

- **No type erasure.** Instead of a generic router with `Box<dyn Any>` storage, the approach generates concrete typed code per topology. All signal storage, edge storage, dirty queues, and dispatch use topology-specific enums and typed hashmaps. No downcasting anywhere. The `router!` macro (phase 4) will generate this code; phases 1-3 hand-write it for a test topology.
- **Test topology:** `AlphaSm` (produces `Demand(bool)`, source of `AlphaToBeta` edges) and `BetaSm` (produces `Status(u32)`, source of `BetaToAlpha` edges). `BetaSm::DemandInput` aggregates demand through `AlphaToBeta` via `CountTrueAggregator`. `AlphaSm::StatusInput` aggregates status through `BetaToAlpha` via `ListAggregator`.
- **Per-type typed storage:** Signals stored as `HashMap<AlphaId, bool>`, `HashMap<BetaId, u32>` — not a single type-erased map. Edges stored as per-edge-type forward (`HashMap<SourceId, Vec<TargetId>>`) and reverse (`HashMap<TargetId, HashSet<SourceId>>`) indices with typed IDs.
- **Typed SM contexts:** Each SM type gets its own `Ctx` struct exposing only valid operations (signals it can produce, edge types it's the source of).
- **SmHandler trait and owned SM instances.** The router stores SM structs in `HashMap<Id, SmStruct>` and dispatches via the `SmHandler` trait (`sm.handle(input, &mut ctx)`). Test SM structs record deliveries internally. Uses take-call-reinsert during dispatch to avoid borrow conflicts.
- **Dirty queue:** `enum DirtyInput` with one variant per aggregated input, carrying the typed target ID. Deduped per wave via `HashSet`.
- **Propagation:** Wave-based depth counting. External triggers (`set_*` methods) enqueue dirty entries without propagating — `propagate()` is called explicitly, allowing batching of multiple changes into one round.
- **Framework:** Only the `Aggregator` trait is topology-independent (`lib.rs`). Everything else is "generated" (hand-written in `tests.rs` for phases 1-3).
- **Aggregated output change detection.** The router tracks the last delivered aggregation result per (target, input). If re-aggregation produces the same value, delivery is skipped. This prevents redundant handler invocations from edge churn that doesn't affect the aggregated result.
- **Default signal values.** Output signals are initialized to `Default::default()` at instance creation. This makes "no signal set" well-defined rather than silently absent from aggregation.
- **Edge no-change short-circuit.** Edge setters diff old vs new target sets and return early if identical, avoiding unnecessary dirty queue entries from reactive handlers that re-set the same edges.


## Phase 2 — SM and port lifecycle

Instance creation/destruction, port removal, dangling edges, `round_complete`.

### Deliverables
- SM instance creation and destruction
- Port instance creation and removal with automatic edge cleanup
- Dangling edge semantics (target death leaves incoming edges, no cleanup)

### Tests
- [x] Port removal: edges cleaned up, targets re-aggregated
- [x] Dangling edges: target dies, source edges remain, source unaffected
- [x] SM creation: no eager delivery for empty edge sets
- [x] Round semantics: multiple inputs change in one round, each delivered independently
- [x] SM destruction removes outgoing edges, triggers re-aggregation
- [x] Dangling edge to dead/never-created SM is a no-op

### Notes

Phase 2 complete. Key implementation decisions:

- **Instance registries.** `HashSet<AlphaId>`, `HashSet<BetaId>`, `HashSet<GammaId>` track which instances are alive. The propagation loop checks the registry before delivering — dirty entries for dead instances are silently skipped.
- **Port type added to test topology.** `GammaPort` produces `Value(u32)`, with `GammaToBeta` edges feeding `BetaSm::ConfigInput` via `ListAggregator`. This gives BetaSm two inputs (DemandInput + ConfigInput), needed for testing independent delivery.
- **SM destruction vs port removal.** SM destruction (`destroy_alpha/beta`) removes outgoing edges only — incoming edges remain as dangling edges (source still has them in its forward index, they just target a dead instance). Port removal (`remove_gamma`) removes all edges from/to the port. This matches the design: sources own their edges and discover target death reactively.
- **Dangling edge semantics.** When a signal change propagates through a dangling edge, the dirty entry is enqueued but skipped in the propagation loop (instance registry check). No panic, no error — well-defined no-op.
- **Ctx carries instance ID.** Each SM's `Ctx` struct includes the instance's own ID, accessible via `ctx.id()`. Handlers need this for patterns like targeting edges back at specific instances.
- **`round_complete` deferred.** Removed from hand-written code for now — trivial to add back once the macro handles generation. The propagation semantics around `round_complete` (should it re-enter the propagation loop?) are better resolved once the macro is in place.


## Phase 3 — Event channels

Discrete event delivery along edges.

### Deliverables
- Event delivery mechanism
- Either-direction connectivity check (any edge between sender and receiver)
- Rejection when no edge exists

### Tests
- [ ] Event delivery along forward edge
- [ ] Event delivery with only reverse edge (either-direction check)
- [ ] Event rejected when no edge exists between instances
- [ ] Event to removed/nonexistent target rejected

### Notes


## Phase 4 — The `router!` macro

Crabtime macro that generates all wiring from a topology declaration.

### Deliverables
- `router!` macro parsing the topology declaration (SM types, ports, signals, edges, inputs, events)
- Generated input enums per SM type (one variant per aggregated input + events)
- Generated source enums for multi-source aggregated inputs
- Router struct owns SM instances (`HashMap<AlphaId, AlphaSm>`) and dispatches via `SmHandler` trait
- SM creation takes the user's struct instance; destruction returns it
- Generated Ctx structs per SM type with instance ID and typed setters for signals/edges
- Aggregator wiring and dispatch code generation
- Compile-time validation: `(EdgeType, Signal)` source pairs checked against topology

### Tests
- [ ] Declare multi-SM topology, verify generated enums compile and match exhaustively
- [ ] Multi-source input: verify source enum generated with correct variants
- [ ] Invalid `(EdgeType, Signal)` pair fails to compile
- [ ] End-to-end: replicate phase 1-3 hand-written tests using macro-generated wiring, verify identical behavior

### Notes

