# Signal Router — Implementation Plan

Implements the design in [signal-router.md](signal-router.md).

Scope: phases 1-4 (core engine through macro). Integration into orchestrator (phase 5) is separate work.

## Decisions

- **Crate name:** `distvirt-sm-router`
- **Macro approach:** Traditional proc-macro in a separate `distvirt-sm-router-macros` crate, using `syn`/`quote` for parsing and code generation. Re-exported from `distvirt-sm-router` as `router!`.
- **Testing strategy:** All tests use the `router!` macro to generate wiring. The test topology (Alpha/Beta SMs + Gamma port) exercises all propagation semantics. Hand-written manual wiring was used during phases 1-3 development and removed once the macro replicated all tests.
- **SM ownership model:** SM instances are user-defined structs stored inside the router. There is a single `SmHandler` trait (defined in `lib.rs`, topology-independent) with associated types `Input` and `Ctx`. The macro generates the `Input` enum and `Ctx` struct per SM type; the user implements `SmHandler` on their SM struct. The router owns `HashMap<SmId, SmStruct>` and dispatches via `sm.handle(input, &mut ctx)` — no external handler object, no per-SM-type traits. SM state is colocated with its handler. Uses take-call-reinsert pattern during dispatch to avoid borrow conflicts. Side effects that escape the SM system (gRPC calls, scheduler requests, etc.) flow through the `Ctx` or are collected as descriptors returned after the round — orthogonal to SM storage.

## Phase 1–2 — Core engine (complete)

Core types, propagation semantics, SM/port lifecycle. Originally developed with hand-written manual wiring; hand-written code removed after phase 4 macro replicated all tests.

### Key design decisions

- **No type erasure.** Concrete typed code per topology — all signal storage, edge storage, dirty queues, and dispatch use topology-specific enums and typed hashmaps. No downcasting.
- **Per-type typed storage.** Signals stored as `HashMap<AlphaId, bool>`, `HashMap<BetaId, u32>` etc. Edges stored as per-edge-type forward (`HashMap<SourceId, Vec<TargetId>>`) and reverse (`HashMap<TargetId, HashSet<SourceId>>`) indices.
- **Typed SM contexts.** Each SM type gets its own `Ctx` struct exposing only valid operations (signals it can produce, edge types it's the source of).
- **SmHandler trait and owned SM instances.** Router stores SM structs in `HashMap<Id, SmStruct>` and dispatches via `SmHandler` trait. Uses take-call-reinsert during dispatch to avoid borrow conflicts.
- **Dirty queue.** `enum DirtyInput` with one variant per aggregated input, carrying the typed target ID. Deduped per wave via `HashSet`.
- **Propagation.** Wave-based depth counting. External triggers (`set_*` methods) enqueue dirty entries without propagating — `propagate()` is called explicitly, allowing batching of multiple changes into one round.
- **Aggregated output change detection.** Router tracks last delivered aggregation result per (target, input). If re-aggregation produces the same value, delivery is skipped.
- **Default signal values.** Output signals initialized to `Default::default()` at instance creation.
- **Edge no-change short-circuit.** Edge setters diff old vs new target sets and return early if identical.
- **SM destruction vs port removal.** SM destruction removes outgoing edges only — incoming edges remain as dangling. Port removal removes all edges from/to the port.
- **Dangling edge semantics.** Dirty entries for dead instances are silently skipped in the propagation loop. No panic, no error.
- **Ctx carries instance ID.** Accessible via `ctx.id()`.
- **`round_complete` deferred.** Not yet implemented — trivial to add once needed.

### Tests (all via `router!` macro)

- [x] Basic signal propagation: set signal, verify aggregated input delivered
- [x] Change detection: set same value, verify no delivery
- [x] Multi-edge aggregation: N sources into one target
- [x] Cascading: signal change -> edge change -> further propagation
- [x] Depth limiting: artificial cycle hits limit
- [x] Edge removal triggers re-aggregation
- [x] Batched changes propagate in single round
- [x] Port removal: edges cleaned up, targets re-aggregated
- [x] Dangling edges: target dies, source edges remain, source unaffected
- [x] SM creation: no eager delivery for empty edge sets
- [x] Round semantics: multiple inputs change in one round, each delivered independently
- [x] SM destruction removes outgoing edges, triggers re-aggregation
- [x] Dangling edge to dead/never-created SM is a no-op


## Phase 3 — Event channels

Discrete event delivery along edges.

### Deliverables
- Event delivery mechanism
- Either-direction connectivity check (any edge between sender and receiver)
- Rejection when no edge exists

### Tests
- [x] Event delivery along forward edge
- [x] Event delivery with only reverse edge (either-direction check)
- [x] Event rejected when no edge exists between instances
- [x] Event to removed/nonexistent target rejected
- [x] Event sent from SM handler via Ctx

### Notes

Phase 3 complete. Key implementation decisions:

- **Events go through `PendingEvent` enum.** Separate from the `DirtyInput` dirty queue — events carry payloads and are not aggregated/deduplicated.
- **Ctx-based sending for SMs.** SM handlers send events via `ctx.send_event_name(target, payload)`. Events are collected in the Ctx and drained into `pending_events` during `apply_effects`.
- **Public Router methods for ports.** Port-sourced events use `router.send_event_name(sender, receiver, payload)` which pushes directly to `pending_events`.
- **Either-direction connectivity check.** Generated code checks all edge types connecting sender and receiver node types (both directions) using fwd/rev maps.
- **Validation.** Macro validates at expansion time that sender/receiver are known nodes, receiver is an SM (not a port), and at least one edge type connects the two node types.
- **Event variants in input enum.** Event payloads appear as additional variants in the receiver SM's generated input enum, giving exhaustive match coverage.


## Phase 4 — The `router!` macro

Proc macro in `distvirt-sm-router-macros` that generates all wiring from a topology declaration.

### Deliverables
- `router!` macro parsing the topology declaration (SM types, ports, signals, edges, events, inputs)
- Generated input enums per SM type (one variant per aggregated input)
- Router struct owns SM instances (`HashMap<AlphaId, AlphaSm>`) and dispatches via `SmHandler` trait
- SM creation takes the user's struct instance; destruction returns it
- Generated Ctx structs per SM type with instance ID and typed setters for signals/edges
- Aggregator wiring and dispatch code generation
- Compile-time validation: `(EdgeType, Signal)` source pairs checked against topology

### Tests
- [x] Declare multi-SM topology, verify generated enums compile and match exhaustively
- [ ] Multi-source input: verify source enum generated with correct variants
- [ ] Invalid `(EdgeType, Signal)` pair fails to compile
- [x] All 13 phase 1-2 tests run via macro-generated wiring (hand-written wiring removed)

### Notes

Phase 4 complete (phases 1-2 scope). Key implementation decisions:

- **Separate proc-macro crate.** Traditional `distvirt-sm-router-macros` proc-macro crate using `syn` (with `full` feature) for parsing and `quote` for code generation. Re-exported from `distvirt-sm-router` via `pub use`.
- **DSL parsing via `syn::Parse`.** One `impl Parse` per DSL element (TopologyDef, SmDef, PortDef, SignalDef, EdgeDef, InputDef, SourcePair). Uses `syn::custom_keyword!` for section keywords, `Punctuated` for comma-separated lists, and standard `syn` helpers (`braced!`, `bracketed!`, `parenthesized!`, `Token![->]`).
- **Code generation via `quote`.** All generated code uses `quote!` with `format_ident!` for dynamic identifier construction. PascalCase-to-snake_case conversion for field/method names.
- **Aggregator construction via `Default`.** Generated aggregation code uses `<AggType as Default>::default().aggregate(&inputs)`. Added `Default` impl to `ListAggregator`; user aggregators must implement `Default`.
- **Aggregator output types.** Generated enums use `<AggType as crate::Aggregator>::Output` — no user burden, compiler resolves associated types.
- **Path qualification.** Generated code uses `crate::Aggregator` and `crate::SmHandler` for framework traits, `std::collections::HashMap`/`HashSet`/`VecDeque` for stdlib. User types (IDs, SM structs, aggregators) are unqualified.
- **Validation.** Macro validates cross-references at expansion time: signals reference existing nodes, edges reference valid source/target nodes, inputs target SM nodes (not ports), input source pairs reference valid edges and signals.

