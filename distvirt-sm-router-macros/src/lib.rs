use proc_macro::TokenStream;

mod generate;
mod parse;
mod validate;

/// Declares a signal router topology and generates all dispatch code.
///
/// See the `distvirt_sm_router` crate-level docs for concepts, usage guide,
/// and patterns. This doc is the **syntax and generated-types reference**.
///
/// # Syntax
///
/// ```rust,ignore
/// router! {
///     // -- Optional flags (before any section) --
///
///     // Makes all generated fields and methods `pub`. Without this, internal
///     // fields are private and only the public API is exposed. Useful for
///     // test assertions that inspect router internals.
///     expose_internals_for_testing
///
///     // Enables model checking support: adds Clone/Hash/Eq bounds on SM and
///     // signal types, generates RouterSnapshot, step-by-step propagation
///     // methods, and signal/edge/instance accessors. See the `model_check`
///     // module docs. Omit for production — zero cost when absent.
///     model_checkable
///
///     // -- Required sections (in any order) --
///
///     state_machines {
///         // Name(IdType, SmStruct)
///         Service(ServiceId, ServiceSm),
///         Workload(auto, WorkloadSm),    // `auto` generates WorkloadId(u64)
///     }
///
///     ports {
///         // Name(IdType) — no handler struct
///         Worker(WorkerId),
///         Management(auto),              // generates ManagementId(u64)
///     }
///
///     signals {
///         // Node::SignalName(ValueType)
///         // ValueType must impl: PartialEq + Debug + Default
///         Service::Demand(bool),
///         Workload::Readiness(Option<ReadyInfo>),
///         Worker::Info(WorkerInfo),
///     }
///
///     edges {
///         // EdgeName: SourceNode -> TargetNode
///         ServiceToWorkload: Service -> Workload,
///         WorkloadToService: Workload -> Service,
///         WorkerToPod: Worker -> Pod,
///     }
///
///     events {
///         // EventName(PayloadType): SenderNode -> ReceiverNode
///         // Receiver must be an SM. At least one edge type must connect
///         // sender and receiver node types (compile-time check).
///         AdminCommand(CommandPayload): Management -> Workload,
///     }
///
///     inputs {
///         // -- Batch aggregated input (SM or port) --
///         // Single-source: aggregator receives &[(SourceId, Value)]
///         Workload::DemandInput {
///             sources: [(ServiceToWorkload, Service::Demand)],
///             aggregator: CountTrueAggregator,
///         },
///
///         // Multi-source: generates an enum, aggregator receives &[EnumType]
///         Workload::CombinedInput {
///             sources: [
///                 (ServiceToWorkload, Service::Demand),
///                 (FabricToWorkload, Fabric::ActiveFlow),
///             ],
///             aggregator: CombinedAggregator,
///         },
///
///         // -- Incremental aggregated input --
///         // Per-source diffs: added/removed/changed callbacks
///         Timer::WorkloadTimersInput {
///             sources: [(WorkloadToTimer, Workload::WantedTimers)],
///             incremental_aggregator: TimerAggregator,
///         },
///
///         // Using the built-in ListAggregator:
///         Pod::SpecInput {
///             sources: [(WorkloadToPod, Workload::PodSpec)],
///             aggregator: ListAggregator<WorkloadId, PodSpecData>,
///         },
///     }
///
///     invariants {
///         // Node::Signal(boolean_expr)
///         // `value` refers to the signal value. Checked at quiescence.
///         Worker::Info(value.capacity > 0),
///     }
/// }
/// ```
///
/// # Generated types reference
///
/// ## `Router<T: Tracer>` struct
///
/// The central coordinator, generic over a [`Tracer`](distvirt_sm_router::trace::Tracer).
///
/// **Constructors:**
/// - `Router::new(depth_limit)` — untraced (uses `NoopTracer`, zero overhead)
/// - `Router::new_traced(depth_limit, tracer)` — with a tracer attached
///
/// **SM lifecycle:**
/// - `create_{sm}(id, sm_struct)` — register with manual ID
/// - `create_{sm}(sm_struct) -> Id` — register with auto ID (returns the generated ID)
/// - `destroy_{sm}(id) -> Option<SmStruct>` — remove, clean up edges, re-aggregate
/// - `get_{sm}(id) -> Option<&SmStruct>` — read-only access to SM state
/// - `initialize_{sm}(id)` — call the SM's `initialize` hook (automatically called
///   by `create_*`, but available for manual use)
///
/// **Port lifecycle:**
/// - `create_{port}(id)` / `create_{port}() -> Id` — register a port
/// - `remove_{port}(id)` — remove, clean up all edges in both directions
///
/// **Port signals and edges** (external API for ports):
/// - `set_{port}_{signal}(id, value)` — update a port's signal value
/// - `set_{edge}_edges(source_id, targets)` — set outgoing edges from a port
///
/// SM signals and edges are *not* set through these methods. SMs are the sole
/// authority for their own outgoing signals and edges — they manage them
/// exclusively via `ctx.set_{signal}()` and `ctx.set_{edge}_edges()` in their
/// `initialize` and `handle` methods.
///
/// **Events:**
/// - `send_{event}(sender, receiver, payload)` — from port to SM
///   (for SM-to-SM events, use `ctx.send_{event}()` in the handler)
///
/// **Propagation:**
/// - `propagate()` — resolve all cascading effects to quiescence
/// - `begin_manual_propagate()` — step-by-step propagation (model checking)
/// - `deliver_one(delivery)` — process one delivery (model checking)
/// - `is_quiescent() -> bool` — true when nothing is pending
///
/// **Port input draining:**
/// - `drain_{port}_inputs(id) -> impl Iterator<Item = {Port}Input>` — read
///   incremental changes that propagated to a port. Call after `propagate()`.
///
/// **With `model_checkable`:**
/// - `snapshot() -> RouterSnapshot` — capture state for dedup
/// - `from_snapshot_traced(snapshot, depth_limit, tracer) -> Router` — reconstruct
/// - Signal/edge/instance accessors — see [`model_check`](distvirt_sm_router::model_check)
///
/// ## `{SmName}Input` enum
///
/// One variant per aggregated input, plus one per received event type:
///
/// ```rust,ignore
/// enum WorkloadInput {
///     DemandInput(DemandInfo),         // from aggregator output type
///     SpecInput(Option<WorkloadSpec>), // from aggregator output type
///     AdminCommand(ManagementId, AdminCmd), // (sender_id, payload)
///     // ...
/// }
/// ```
///
/// Exhaustive — add a match arm for every variant.
///
/// ## `{SmName}Ctx` trait
///
/// The handler's interface to the router. Methods are scoped to what this SM
/// type can do (only its own signals, its own outgoing edge types, etc.):
///
/// - `fn id(&self) -> SmId`
/// - `fn set_{signal}(&mut self, value: T)`
/// - `fn set_{edge}_edges(&mut self, targets: Vec<TargetId>)`
/// - `fn send_{event}(&mut self, target: ReceiverId, payload: T)`
/// - `fn create_{sm}(&mut self, sm: SmStruct) -> SmId` (auto-ID)
/// - `fn create_{sm}(&mut self, id: SmId, sm: SmStruct)` (manual-ID)
/// - `fn self_destruct(&mut self)`
///
/// ## `{SmName}CtxConcrete<'a, A: IdAllocator>` struct
///
/// Standalone context for testing SM handlers without a router:
///
/// ```rust,ignore
/// let mut alloc = SequentialIds::<NodeKind>::new();
/// let mut ctx = WorkloadCtxConcrete::new(workload_id, &mut alloc);
/// sm.handle(input, &mut ctx);
/// let effects = ctx.into_effects();
/// // inspect effects.readiness, effects.pending_events, etc.
/// ```
///
/// ## `{SmName}Effects` struct
///
/// All effects produced by a single handler invocation:
/// - `{signal}: Option<T>` — one field per signal (None = not set)
/// - `{edge}_edges: Option<Vec<TargetId>>` — one field per outgoing edge type
/// - `pending_events: Vec<PendingEvent>`
/// - `pending_creates: Vec<PendingCreate>`
/// - `pending_self_destruct: bool`
///
/// ## `PendingEvent` enum
///
/// One variant per event channel: `EventName(SenderId, ReceiverId, Payload)`.
///
/// ## `PendingCreate` enum
///
/// One variant per SM type: `SmName(Id, SmStruct)`.
///
/// ## `PendingDelivery` enum
///
/// Wraps either a `DirtyInput` or a `PendingEvent`. Used with
/// `begin_manual_propagate()` for step-by-step propagation.
///
/// ## `DirtyInput` enum
///
/// One variant per declared input. Identifies which input on which SM instance
/// needs re-aggregation.
///
/// ## `NodeKind` enum
///
/// One variant per SM and port type. Used by [`IdAllocator`](distvirt_sm_router::IdAllocator)
/// for per-kind ID counters. Auto-ID variants come first.
///
/// ## `{InputName}Source` enum (multi-source inputs only)
///
/// Generated when an input has 2+ source pairs. One variant per source pair:
/// `{NodeName}{SignalName}(NodeId, SignalValue)`.
///
/// ## `{PortName}Input` enum (port inputs only)
///
/// One variant per input declared on a port. Yielded by
/// `drain_{port}_inputs()`.
///
/// ## Auto-generated ID types
///
/// For nodes declared with `auto`:
/// ```rust,ignore
/// #[derive(Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug)]
/// struct {NodeName}Id(u64);
/// ```
///
/// # Compile-time validation
///
/// The macro validates:
/// - No duplicate names across SMs, ports, edges, signals, events, and inputs
/// - All node names referenced in signals, edges, events, and inputs exist
/// - Edge source nodes actually produce the signal referenced in source pairs
/// - Event receivers are SMs (not ports)
/// - At least one edge type connects event sender and receiver node types
/// - Signal value types implement `PartialEq` and `Debug` (via generated const assertions)
/// - Input source lists are non-empty and contain no duplicate source pairs
#[proc_macro]
pub fn router(input: TokenStream) -> TokenStream {
    let def = syn::parse_macro_input!(input as parse::TopologyDef);
    if let Err(err) = validate::validate(&def) {
        return err.to_compile_error().into();
    }
    generate::generate(&def).into()
}
