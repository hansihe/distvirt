use proc_macro::TokenStream;

mod generate;
mod parse;
mod validate;

/// Declares a signal router topology and generates all dispatch code.
///
/// The macro takes a full topology declaration — SM types, ports, signals, edges,
/// events, and aggregated inputs — and generates a `Router` struct, per-SM input
/// enums, per-SM context structs, and all wiring code.
///
/// # Syntax
///
/// ```rust,ignore
/// router! {
///     // Optional: makes all generated fields and methods `pub` for testing.
///     // Without this, internal fields are private and only the public API
///     // (create, destroy, get, set_*_edges, set_*_{signal}, propagate) is exposed.
///     expose_internals_for_testing
///
///     // State machine types. Each entry is:
///     //   Name(IdType, SmStruct)
///     //
///     // IdType can be a user-defined type (must impl Copy+Clone+Eq+Ord+Hash+Debug)
///     // or `auto` to generate a newtype wrapper with auto-incrementing IDs.
///     state_machines {
///         Service(ServiceId, ServiceSm),
///         Workload(auto, WorkloadSm),    // generates WorkloadId(u64)
///     }
///
///     // Port types. Same ID rules as state machines, but no SM struct.
///     // Ports are external boundary points — they produce signals and send events
///     // but have no handler.
///     ports {
///         Worker(WorkerId),
///         Management(auto),              // generates ManagementId(u64)
///     }
///
///     // Output signals. Each SM or port type can declare signals it produces.
///     // The value type must implement PartialEq (enforced at compile time).
///     // Each instance produces one value per signal type.
///     signals {
///         Service::Demand(bool),
///         Workload::Readiness(Option<ReadyInfo>),
///         Worker::Info(WorkerInfo),
///     }
///
///     // Typed unidirectional edges between node types.
///     //   EdgeName: SourceNode -> TargetNode
///     edges {
///         ServiceToWorkload: Service -> Workload,
///         WorkloadToService: Workload -> Service,
///         WorkerToPod: Worker -> Pod,
///     }
///
///     // Event channels for discrete one-shot messages.
///     //   EventName(PayloadType): SenderNode -> ReceiverNode
///     //
///     // The receiver must be an SM (not a port). Connectivity is checked at
///     // runtime using edges in *either* direction between sender and receiver.
///     // At least one edge type must connect the sender and receiver node types.
///     events {
///         AdminCommand(CommandPayload): Management -> Workload,
///     }
///
///     // Aggregated inputs — what each SM consumes.
///     // Each input declares source pairs and an aggregator:
///     //   SmType::InputName {
///     //       sources: [(EdgeType, SourceNode::Signal), ...],
///     //       aggregator: AggregatorType,
///     //   }
///     //
///     // The macro validates that each edge's source node actually produces
///     // the referenced signal.
///     inputs {
///         // Single-source: aggregator receives &[(SourceId, Value)]
///         Workload::DemandInput {
///             sources: [(ServiceToWorkload, Service::Demand)],
///             aggregator: CountTrueAggregator,
///         },
///         // Multi-source: macro generates an enum, aggregator receives &[EnumType]
///         Workload::CombinedInput {
///             sources: [
///                 (ServiceToWorkload, Service::Demand),
///                 (FabricToWorkload, Fabric::ActiveFlow),
///             ],
///             aggregator: CombinedAggregator,
///         },
///         // Using the built-in ListAggregator:
///         Pod::SpecInput {
///             sources: [(WorkloadToPod, Workload::PodSpec)],
///             aggregator: ListAggregator<WorkloadId, PodSpecData>,
///         },
///     }
/// }
/// ```
///
/// # Generated code
///
/// ## `Router` struct
///
/// The central coordinator. Key methods:
///
/// - **`Router::new(depth_limit: usize)`** — create a new router with the given
///   propagation depth limit.
/// - **`create_{sm}(id, sm_struct)` / `create_{sm}(sm_struct) -> Id`** — register
///   an SM instance. The second form is used with `auto` IDs.
/// - **`destroy_{sm}(id) -> Option<SmStruct>`** — remove an SM, cleaning up its
///   outgoing edges and triggering re-aggregation for affected targets.
/// - **`get_{sm}(id) -> Option<&SmStruct>`** — read access to an SM's internal state.
/// - **`create_{port}(id)` / `create_{port}() -> Id`** — register a port instance.
/// - **`remove_{port}(id)`** — remove a port, cleaning up all edges in both
///   directions and re-aggregating affected targets.
/// - **`set_{node}_{signal}(id, value)`** — update a signal value for an SM or port.
///   For ports this is the public API; for SMs, signals are typically set from within
///   the handler via the context.
/// - **`set_{edge}_edges(source_id, targets: Vec<TargetId>)`** — set the complete
///   list of outgoing edges of a given type from a source. Edges not in the new set
///   are removed; new edges are added. Re-aggregation is triggered for affected targets.
/// - **`send_{event}(sender, receiver, payload)`** — send a discrete event from a
///   port to an SM (for SM-sourced events, use `ctx.send_{event}()` instead).
/// - **`propagate()`** — resolve all pending signal changes, edge updates, and events.
///   This is the main driver — call it after making external changes.
///
/// ## Per-SM input enum: `{SmName}Input`
///
/// One variant per aggregated input declared for this SM, plus one variant per event
/// channel where this SM is the receiver. Exhaustive matching ensures all inputs are
/// handled.
///
/// ## Per-SM context: `{SmName}Ctx`
///
/// Passed to the handler. Provides `id()`, signal setters, edge setters, and event
/// senders scoped to this SM type.
///
/// ## Multi-source input enum: `{InputName}Source`
///
/// Generated for inputs with 2+ source pairs. One variant per source pair:
/// `{NodeName}{SignalName}(NodeId, SignalValue)`.
///
/// ## Auto-generated ID types
///
/// For nodes declared with `auto`, generates:
/// `struct {NodeName}Id(u64)` with derives for
/// `Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug`.
///
/// # Compile-time validation
///
/// The macro validates:
/// - All node names referenced in signals, edges, events, and inputs exist
/// - Edge source nodes actually produce the signal referenced in source pairs
/// - Event receivers are SMs (not ports)
/// - At least one edge type connects event sender and receiver node types
/// - Signal value types implement `PartialEq` (via a generated const assertion)
#[proc_macro]
pub fn router(input: TokenStream) -> TokenStream {
    let def = syn::parse_macro_input!(input as parse::TopologyDef);
    if let Err(err) = validate::validate(&def) {
        return err.to_compile_error().into();
    }
    generate::generate(&def).into()
}
