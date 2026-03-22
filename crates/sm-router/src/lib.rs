//! # Signal Router
//!
//! A framework for coordinating state machines (SMs) that communicate through
//! typed signals, structural edges, and discrete events. The router maintains
//! the connectivity graph and mechanically propagates signal changes — SMs
//! contain only domain logic, never relationship bookkeeping.
//!
//! ## Overview
//!
//! In systems with many interacting state machines, each SM ends up tracking
//! which other SMs it's connected to, reacting to their changes, and forwarding
//! updates — relationship bookkeeping that obscures the actual domain logic.
//!
//! The signal router inverts this: you declare a **topology** (which SM types
//! exist, how they connect, what signals flow between them) and the framework
//! handles all the wiring. SMs receive typed inputs and produce typed outputs
//! through a context — they never touch the connectivity graph directly.
//!
//! ## Core concepts
//!
//! ### Node types: SM vs Port
//!
//! The topology contains two kinds of nodes:
//!
//! - **SM (State Machine):** An instance with identity, internal state, output
//!   signals, and a handler that processes incoming inputs. SMs are where
//!   domain logic lives.
//!
//! - **Port:** An external boundary point that bridges the router to the
//!   outside world (network connections, timers, schedulers). Ports produce
//!   signals and can send events, but have no handler — they are driven
//!   externally by your application code.
//!
//! ### Signals
//!
//! A signal is a **persistent current-value** that an SM or port instance
//! produces. Each instance has one value per declared signal type. Signals
//! start at `Default::default()`.
//!
//! **Ownership:** Each SM is the **sole authority** for its own outgoing
//! signals. An SM sets its signals via `ctx.set_{signal}()` in its handler
//! or `initialize` hook — external code never sets signals on an SM's behalf.
//! Port signals are set externally by application code via
//! `router.set_{port}_{signal}()`.
//!
//! The router uses `PartialEq` to detect changes — setting a signal to its
//! current value is a no-op (no downstream effects). Signal value types must
//! implement `PartialEq` (enforced at compile time) and `Debug`.
//!
//! Signals don't propagate transitively. If SM A produces a signal consumed by
//! SM B, and SM B wants to relay that information to SM C, B must explicitly
//! set its own output signal in its handler. This makes data flow explicit.
//!
//! ### Edges
//!
//! An edge is a **unidirectional typed relationship** between instances. Edges
//! define structure — they don't carry data directly. Instead, aggregated input
//! declarations use edges to determine which signal values to collect.
//!
//! **Ownership:** Each SM is the **sole authority** for its own outgoing edges.
//! An SM sets its outgoing edges via `ctx.set_{edge}_edges()` in its handler
//! or `initialize` hook — external code never sets edges on an SM's behalf.
//! Ports are different: their edges are set externally by application code via
//! `router.set_{edge}_edges()`.
//!
//! Edge setters use **set semantics**, not add/remove: `set_{edge}_edges(source,
//! targets)` replaces the entire edge set for that source. Targets not in the
//! new set are removed; new targets are added. Re-aggregation is triggered for
//! all affected targets (both removed and added).
//!
//! ### Aggregated inputs
//!
//! An aggregated input declares what a consuming SM receives. It specifies one
//! or more `(EdgeType, Signal)` source pairs and an aggregation strategy:
//!
//! - **Batch** ([`Aggregator`]): All source values from connected instances are
//!   collected and passed to `aggregate()` as a slice. The result is compared
//!   via `PartialEq` — delivery is suppressed when the output hasn't changed.
//!   Use batch when the handler needs the full picture (e.g., "how many services
//!   have demand?").
//!
//! - **Incremental** ([`IncrementalAggregator`]): The router tracks previous
//!   values per source and calls `added()`, `removed()`, or `changed()` for
//!   each individual diff. Each `Some(output)` is unconditionally delivered as
//!   a separate handler call — no `PartialEq` suppression. Use incremental for
//!   port-facing inputs where you need to translate individual changes into
//!   external side-effects (e.g., updating timer registrations, sending protocol
//!   commands).
//!
//! ### Events
//!
//! An event is a **one-shot discrete message** between two instances.
//! Connectivity is checked at two levels:
//!
//! - **Compile time:** The macro validates that at least one declared edge type
//!   connects the sender and receiver *node types*.
//! - **Runtime:** When sending, the router verifies that an edge *instance*
//!   exists between the specific sender and receiver (in either direction).
//!   Events sent without a connecting edge are silently rejected.
//!
//! Events are not aggregated — each is delivered individually to the receiver's
//! handler.
//!
//! ### Invariants
//!
//! Signals can have **invariants** — boolean expressions that must hold at
//! quiescence (after all propagation rounds complete). Violations are reported
//! via the [tracing infrastructure](trace). See the [`trace`] module docs.
//!
//! ## Quick start
//!
//! ```rust,ignore
//! use distvirt_sm_router::{router, Aggregator, SmHandler};
//!
//! // 1. Define ID types (or use `auto` in the topology to generate them)
//! #[derive(Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug)]
//! struct ServiceId(u64);
//!
//! #[derive(Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug)]
//! struct WorkloadId(u64);
//!
//! // 2. Define an aggregator
//! #[derive(Default)]
//! struct DemandAggregator;
//!
//! impl Aggregator for DemandAggregator {
//!     type Input = (ServiceId, bool);
//!     type Output = u32;
//!
//!     fn aggregate(&self, inputs: &[(ServiceId, bool)]) -> u32 {
//!         inputs.iter().filter(|(_, demand)| *demand).count() as u32
//!     }
//! }
//!
//! // 3. Declare the topology
//! router! {
//!     state_machines {
//!         Service(ServiceId, ServiceSm),
//!         Workload(WorkloadId, WorkloadSm),
//!     }
//!     ports {}
//!     signals {
//!         Service::Demand(bool),
//!     }
//!     edges {
//!         ServiceToWorkload: Service -> Workload,
//!     }
//!     events {}
//!     inputs {
//!         Workload::DemandInput {
//!             sources: [(ServiceToWorkload, Service::Demand)],
//!             aggregator: DemandAggregator,
//!         },
//!     }
//! }
//!
//! // 4. Implement SM handlers (types are generated by the macro)
//! struct ServiceSm {
//!     target: WorkloadId,
//! }
//!
//! impl<C: ServiceCtx> SmHandler<C> for ServiceSm {
//!     type Input = ServiceInput;
//!
//!     fn initialize(&mut self, ctx: &mut C) {
//!         // SMs are the sole authority for their own edges and signals.
//!         // Set up initial edges and signals in initialize:
//!         ctx.set_service_to_workload_edges(vec![self.target]);
//!         ctx.set_demand(true);
//!     }
//!
//!     fn handle(&mut self, input: Self::Input, ctx: &mut C) {
//!         // ServiceInput is empty — no inputs declared for Service
//!     }
//! }
//!
//! struct WorkloadSm { demand_count: u32 }
//!
//! impl<C: WorkloadCtx> SmHandler<C> for WorkloadSm {
//!     type Input = WorkloadInput;
//!     fn handle(&mut self, input: Self::Input, ctx: &mut C) {
//!         match input {
//!             WorkloadInput::DemandInput(count) => {
//!                 self.demand_count = count;
//!             }
//!         }
//!     }
//! }
//!
//! // 5. Drive the router
//! let mut router = Router::new(16);
//!
//! let w1 = WorkloadId(1);
//! let s1 = ServiceId(1);
//!
//! router.create_workload(w1, WorkloadSm { demand_count: 0 });
//! // Service sets its own edges and demand signal in initialize:
//! router.create_service(s1, ServiceSm { target: w1 });
//!
//! // Propagate resolves all cascading effects
//! router.propagate();
//! // WorkloadSm received DemandInput(1)
//! ```
//!
//! ## Declaring a topology
//!
//! The [`router!`] macro accepts these sections (see the macro's own doc for
//! full syntax reference):
//!
//! ### `state_machines`
//!
//! Each entry declares a name, an ID type, and the handler struct:
//! `Name(IdType, SmStruct)`. The ID type can be a user-defined type (must impl
//! `Copy + Clone + Eq + Ord + Hash + Debug`) or `auto` to generate a newtype
//! wrapper `NameId(u64)` with auto-incrementing IDs.
//!
//! ### `ports`
//!
//! Same as state machines but without a handler struct: `Name(IdType)`. Ports
//! represent external boundaries — worker connections, management APIs, timers,
//! schedulers. They produce signals and can send events, but are driven by your
//! application code rather than by the router.
//!
//! ### `signals`
//!
//! Each entry declares which node type produces the signal and its value type:
//! `Node::SignalName(ValueType)`. Every instance of that node type carries one
//! value of this signal, initialized to `Default::default()`. The value type
//! must implement `PartialEq` and `Debug`.
//!
//! ### `edges`
//!
//! Typed unidirectional relationships: `EdgeName: SourceNode -> TargetNode`.
//! An edge type is a *kind* of relationship — actual edge instances are created
//! at runtime via `set_{edge}_edges()`.
//!
//! ### `events`
//!
//! One-shot message channels: `EventName(PayloadType): Sender -> Receiver`.
//! The receiver must be an SM (not a port). At least one declared edge type must
//! connect the sender and receiver node types (validated at compile time).
//! Instance-level connectivity is checked at runtime.
//!
//! ### `inputs`
//!
//! What each SM consumes. Each input declares source pairs and an aggregation
//! strategy:
//!
//! ```rust,ignore
//! SmType::InputName {
//!     sources: [(EdgeType, SourceNode::Signal), ...],
//!     aggregator: AggregatorType,           // batch
//!     // OR
//!     incremental_aggregator: AggregatorType,  // incremental
//! }
//! ```
//!
//! The macro validates that each edge's source node actually produces the
//! referenced signal.
//!
//! Ports can also have inputs (using `incremental_aggregator` only). Port
//! inputs are not delivered to a handler — instead, changes accumulate and
//! are read via `drain_{port}_inputs()` after propagation.
//!
//! ### `invariants`
//!
//! Boolean expressions on signal values checked at quiescence:
//! `Node::Signal(expr)`. Inside the expression, `value` refers to the signal
//! value. Violations are reported via the tracer — see [`trace`].
//!
//! ## Implementing SM handlers
//!
//! For each SM type declared in the topology, the macro generates:
//!
//! - **`{Sm}Input`** (enum): One variant per aggregated input plus one per
//!   received event type. Exhaustive matching ensures all inputs are handled.
//!
//! - **`{Sm}Ctx`** (trait): The context interface. Methods include:
//!   - `ctx.id()` — this instance's ID
//!   - `ctx.set_{signal}(value)` — update an output signal
//!   - `ctx.set_{edge}_edges(targets)` — update outgoing edges
//!   - `ctx.send_{event}(target, payload)` — send a discrete event
//!   - `ctx.create_{sm}(sm)` — create a child SM (returns ID for auto-ID types)
//!   - `ctx.self_destruct()` — destroy this instance after effects are applied
//!
//! - **`{Sm}CtxConcrete`** (struct): A standalone context implementation,
//!   useful for testing SM handlers in isolation. See
//!   [Isolated SM testing](#isolated-sm-testing) below.
//!
//! - **`{Sm}Effects`** (struct): All effects a handler produced. Public fields
//!   for each signal (`Option<T>`), each outgoing edge (`Option<Vec<TargetId>>`),
//!   plus `pending_events`, `pending_creates`, and `pending_self_destruct`.
//!
//! Your SM struct implements [`SmHandler`]:
//!
//! ```rust,ignore
//! struct WorkloadSm {
//!     demand_count: u32,
//!     spec: Option<WorkloadSpec>,
//! }
//!
//! impl<C: WorkloadCtx> SmHandler<C> for WorkloadSm {
//!     type Input = WorkloadInput;
//!
//!     fn initialize(&mut self, ctx: &mut C) {
//!         // Called once after creation. Set up initial edges:
//!         ctx.set_workload_to_timer_edges(vec![TIMER]);
//!     }
//!
//!     fn handle(&mut self, input: Self::Input, ctx: &mut C) {
//!         match input {
//!             WorkloadInput::DemandInput(info) => {
//!                 self.demand_count = info.demand_count;
//!                 self.reconcile(ctx);
//!             }
//!             WorkloadInput::SpecInput(spec) => {
//!                 self.spec = spec;
//!                 self.reconcile(ctx);
//!             }
//!             // ... handle other inputs and events
//!         }
//!     }
//! }
//! ```
//!
//! ### The `initialize` hook
//!
//! `initialize` is called once, immediately after SM creation (both for
//! external `router.create_*()` calls and internal `ctx.create_*()` calls).
//! This is the place to set up **initial edges** — an SM has no connectivity
//! until it creates edges. Common patterns:
//!
//! - Connect to singleton ports (timer, scheduler, endpoint)
//! - Set initial signal values that differ from `Default::default()`
//! - Create child SMs
//!
//! Has a default no-op implementation.
//!
//! ### Effect buffering
//!
//! All changes made through the context are **queued** and applied atomically
//! after the handler returns. This is fundamental — you cannot observe the
//! effect of a `ctx.set_*()` call within the same handler invocation. The
//! router applies effects in this order:
//!
//! 1. **Creates** — buffered for materialization at the start of the next
//!    input sub-round
//! 2. **Signal changes** — applied immediately, dirty inputs enqueued
//! 3. **Edge changes** — applied immediately, dirty inputs enqueued
//! 4. **Events** — buffered into the pending events queue
//! 5. **Self-destruct** — instance removed, edges cleared (enqueues dirty inputs)
//!
//! ### Self-destruct
//!
//! When a handler calls `ctx.self_destruct()`:
//!
//! - Events queued by the handler are delivered *before* destruction effects
//!   (edge removal, re-aggregation) take place.
//! - No further `handle()` calls will be made to the instance after effects
//!   are applied — it is removed from the instances map.
//! - For incremental inputs, if the SM self-destructs during one delivery,
//!   remaining deliveries in that batch are skipped.
//!
//! ## Driving the router
//!
//! The router is driven from external code in a cycle:
//!
//! 1. **Create/destroy** SMs and ports
//! 2. **Set signals and edges on ports** — external code only touches port
//!    signals and edges. SM signals and edges are managed exclusively by the
//!    SM itself (via `ctx` in `initialize` and `handle`).
//! 3. **Send events** from ports to SMs
//! 4. **Call `propagate()`** — all cascading effects resolve before it returns
//! 5. **Read results** — query signal values via `signal_{node}_{signal}()`,
//!    iterate SMs via `iter_{sm}()`, inspect SM state via `get_{sm}()`,
//!    drain port inputs via `drain_{port}_inputs()`
//!
//! ```rust,ignore
//! // External code (e.g., gRPC handler, timer tick, scheduler callback)
//! router.set_worker_info(worker_id, WorkerInfo { capacity: 4 });
//! router.propagate();
//!
//! // Read changes that propagated to ports
//! for delta in router.drain_schedule_request_inputs(SCHEDULE_REQUEST) {
//!     match delta {
//!         ScheduleRequestInput::PodRequestsInput(req) => {
//!             // Forward to scheduler...
//!         }
//!     }
//! }
//! ```
//!
//! ### The adapter pattern
//!
//! Ports bridge the router to async I/O. A useful pattern is an **adapter**
//! module per port type that translates between the router's signal/event
//! world and external systems:
//!
//! - On external input (gRPC message, timer fire): set port signals/edges,
//!   send events, call `propagate()`
//! - After propagation: `drain_{port}_inputs()` to get incremental changes,
//!   translate to external actions (send protocol commands, register timers)
//!
//! ### Tracing
//!
//! The router is generic over a [`trace::Tracer`]. Use `Router::new(depth)`
//! for a no-overhead untraced router, or `Router::new_traced(depth, tracer)`
//! to attach a tracer. In tests, [`trace::PanicTracer`] auto-dumps the full
//! propagation trace on assertion failure — invaluable for debugging.
//!
//! ## Propagation model
//!
//! When `propagate()` is called, all cascading effects resolve before it
//! returns. The propagation loop alternates between two sub-round types:
//!
//! 1. **Input sub-round:** All dirty aggregated inputs are re-aggregated.
//!    For batch inputs, the result is compared via `PartialEq` — delivery is
//!    suppressed if unchanged. For incremental inputs, per-source diffs produce
//!    individual deliveries. Pending creates are materialized before delivery.
//! 2. **Event sub-round:** All pending events are delivered (connectivity
//!    verified per instance).
//!
//! Each handler call may update signals, edges, queue events, create or destroy
//! SMs. These effects feed into subsequent sub-rounds. The loop terminates at
//! **quiescence** — when no dirty inputs, pending events, or pending creates
//! remain.
//!
//! A configurable depth limit (passed to `Router::new()`) prevents infinite
//! loops. The router warns at `limit - 1` and panics at `limit`.
//!
//! ### Isolation within a sub-round
//!
//! Within a single sub-round, SM handlers are **fully independent**: no handler
//! observes the effects of another handler in the same sub-round. All outputs
//! go into queues processed in subsequent sub-rounds. This means the order in
//! which different SMs are processed within a sub-round does not affect the
//! outcome.
//!
//! The only meaningful ordering is when a **single SM** receives multiple
//! deliveries (e.g., two dirty inputs). The handler is called sequentially for
//! each. The **lattice property** asserts that any permutation of deliveries to
//! a single SM within a sub-round should produce the same final state at
//! quiescence. See [`model_check`] for verification support.
//!
//! ### Sub-round phasing
//!
//! The implementation processes input and event sub-rounds in a single loop
//! iteration (inputs first, then events). Events queued during an input
//! sub-round are delivered in the immediately following event sub-round.
//! Events queued during an event sub-round are delivered in the next
//! iteration's event sub-round.
//!
//! ## Isolated SM testing
//!
//! The generated `{Sm}CtxConcrete` lets you test SM handlers without a router:
//!
//! ```rust,ignore
//! let mut alloc = SequentialIds::<NodeKind>::new();
//! let mut ctx = WorkloadCtxConcrete::new(workload_id, &mut alloc);
//! sm.handle(WorkloadInput::DemandInput(demand_info), &mut ctx);
//! let effects = ctx.into_effects();
//!
//! // Inspect effects
//! assert_eq!(effects.readiness, Some(None)); // signal was set
//! assert!(effects.pending_self_destruct);     // SM self-destructed
//! ```
//!
//! ## Patterns and recipes
//!
//! ### Relaying signals across layers
//!
//! Signals don't propagate transitively — each SM must explicitly relay data
//! by reading an input and setting its own output signal:
//!
//! ```rust,ignore
//! // Management port sets WlSpec signal.
//! // Workload receives it via SpecInput, stores it, and relays to pods:
//! fn handle(&mut self, input: Self::Input, ctx: &mut C) {
//!     match input {
//!         WorkloadInput::SpecInput(spec) => {
//!             self.spec = spec.clone();
//!             // Relay to pods via Workload's own PodLaunchSpec signal
//!             ctx.set_pod_launch_spec(spec);
//!         }
//!         // ...
//!     }
//! }
//! ```
//!
//! ### Singleton ports
//!
//! For global resources (timer service, scheduler, endpoint registry), define
//! a manual ID type with a constant:
//!
//! ```rust,ignore
//! #[derive(Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug)]
//! struct TimerId(u64);
//! const TIMER: TimerId = TimerId(0);
//!
//! // In router setup:
//! router.create_timer(TIMER);
//!
//! // In SM initialize:
//! fn initialize(&mut self, ctx: &mut C) {
//!     ctx.set_workload_to_timer_edges(vec![TIMER]);
//! }
//! ```
//!
//! ### At-most-one aggregators
//!
//! Many inputs expect exactly 0 or 1 source (e.g., a pod has one owner
//! workload, one lease). Write a simple aggregator that takes `.first()`:
//!
//! ```rust,ignore
//! #[derive(Default)]
//! struct OwnerAggregator;
//!
//! impl Aggregator for OwnerAggregator {
//!     type Input = (WorkloadId, PodIntent);
//!     type Output = Option<(WorkloadId, PodIntent)>;
//!
//!     fn aggregate(&self, inputs: &[(WorkloadId, PodIntent)])
//!         -> Option<(WorkloadId, PodIntent)>
//!     {
//!         inputs.first().cloned()
//!     }
//! }
//! ```
//!
//! ### Generation counters for timers
//!
//! When using timer signals, include a generation counter to distinguish fresh
//! timer fires from stale ones. Increment the generation each time you request
//! a new timer; ignore fires whose generation doesn't match:
//!
//! ```rust,ignore
//! struct WorkloadSm {
//!     timer_generation: u64,
//!     // ...
//! }
//!
//! // When requesting a timer:
//! self.timer_generation += 1;
//! ctx.set_wanted_timers(vec![TimerRequest {
//!     key: WorkloadTimerKey::RetryBackoff,
//!     generation: self.timer_generation,
//!     duration: Duration::from_secs(30),
//! }]);
//!
//! // When receiving a timer fire:
//! WorkloadInput::WorkloadTimerFired(key, generation) => {
//!     if generation != self.timer_generation { return; } // stale
//!     // handle timer...
//! }
//! ```
//!
//! ### Spec-version tracking for restarts
//!
//! When an SM manages child SMs whose behavior depends on a spec, track a
//! version counter. On spec change, compare versions to decide whether to
//! restart children:
//!
//! ```rust,ignore
//! struct WorkloadSm {
//!     spec_version: u64,
//!     pod_spec_version: u64,  // version when pod was created
//!     // ...
//! }
//!
//! // On spec change:
//! self.spec_version += 1;
//! // On pod running: if pod_spec_version != spec_version, restart
//! ```
//!
//! ### Hysteresis / committed-to-boot guards
//!
//! Prevent demand fluctuations from killing a child SM that is still
//! launching. Set a flag when creating the child; clear it when the child
//! reports running. While the flag is set, ignore demand drops:
//!
//! ```rust,ignore
//! if self.committed_to_boot {
//!     // Pod is still launching — don't destroy it even if demand dropped
//!     return;
//! }
//! ```
//!
//! ### Reaping: self-destruct on terminal + no owner
//!
//! A child SM should self-destruct when it reaches a terminal state AND has no
//! owner (incoming ownership edges removed). Check both conditions in the
//! handler whenever either changes:
//!
//! ```rust,ignore
//! fn maybe_reap(&self, ctx: &mut C) {
//!     if self.status.is_terminal() && self.owner.is_none() {
//!         ctx.self_destruct();
//!     }
//! }
//! ```
//!
//! ### Signal defaults and empty aggregation
//!
//! Signals initialize to `Default::default()`. When all edges to an input are
//! removed, the aggregator receives an empty slice. The aggregator's
//! empty-input return value becomes the new aggregated result. If this differs
//! from the previous aggregated value, a delivery is triggered.
//!
//! Design aggregators so the empty-input case returns a meaningful "no data"
//! baseline (e.g., `0`, `None`, empty vec).

/// Reduces N signal values (from N incoming edges) into one aggregated input value.
///
/// The `Input` type is determined by the topology declaration:
/// - **Single-source inputs:** `(SourceId, SignalValue)` tuple
/// - **Multi-source inputs:** a generated enum with one variant per `(EdgeType, Signal)`
///   source pair, e.g. `enum DemandInputSource { ServiceDemand(ServiceId, bool), ... }`
///
/// Aggregators **must** handle the empty-input case (zero edges) — this produces the
/// "no data" baseline (e.g., `0`, empty vec). There is no concept of "uninitialized".
///
/// # Example
///
/// ```rust,ignore
/// #[derive(Default)]
/// struct CountTrueAggregator;
///
/// impl Aggregator for CountTrueAggregator {
///     type Input = (ServiceId, bool);
///     type Output = u32;
///
///     fn aggregate(&self, inputs: &[(ServiceId, bool)]) -> u32 {
///         inputs.iter().filter(|(_, v)| *v).count() as u32
///     }
/// }
/// ```
pub trait Aggregator {
    type Input;
    type Output;
    fn aggregate(&self, inputs: &[Self::Input]) -> Self::Output;
}

/// Reacts to individual source changes (added, removed, value changed) instead
/// of reprocessing the entire input set each time.
///
/// Unlike [`Aggregator`], which receives all current source values as a batch
/// and relies on `PartialEq` suppression, `IncrementalAggregator` receives
/// per-item diffs and produces zero or more deliveries per change. Each
/// `Some(output)` is unconditionally delivered to the SM handler — there is no
/// `PartialEq` comparison on the output.
///
/// The `Input` type follows the same convention as [`Aggregator::Input`]:
/// - **Single-source inputs:** `(SourceId, SignalValue)` tuple
/// - **Multi-source inputs:** a generated enum with one variant per source pair
///
/// # Example
///
/// ```rust,ignore
/// #[derive(Default)]
/// struct MembershipAggregator;
///
/// impl IncrementalAggregator for MembershipAggregator {
///     type Input = (ServiceId, bool);
///     type Output = MembershipChange;
///
///     fn added(&self, input: &(ServiceId, bool)) -> Option<MembershipChange> {
///         Some(MembershipChange::Added(input.0))
///     }
///
///     fn removed(&self, input: &(ServiceId, bool)) -> Option<MembershipChange> {
///         Some(MembershipChange::Removed(input.0))
///     }
///
///     fn changed(&self, _old: &(ServiceId, bool), new: &(ServiceId, bool)) -> Option<MembershipChange> {
///         Some(MembershipChange::Updated(new.0, new.1))
///     }
/// }
/// ```
pub trait IncrementalAggregator {
    type Input;
    type Output;

    /// Called when a new source is connected (edge added or new source instance created).
    fn added(&self, input: &Self::Input) -> Option<Self::Output>;

    /// Called when a source is disconnected (edge removed or source instance destroyed).
    fn removed(&self, input: &Self::Input) -> Option<Self::Output>;

    /// Called when a connected source's signal value changes.
    fn changed(&self, old: &Self::Input, new: &Self::Input) -> Option<Self::Output>;
}

/// Diff `current` against `prev`, calling the aggregator's `added`/`removed`/`changed`
/// methods for each difference. Outputs are pushed into `outputs`.
///
/// The `make_input` closure constructs the aggregator's `Input` type from a
/// `(key, value)` pair — this is what allows the same diff logic to work for
/// both single-source inputs (where `Input = (Id, V)`) and multi-source inputs
/// (where `Input` is a generated enum variant).
///
/// Returns the `current` map, which the caller should store as the new `prev`.
#[doc(hidden)]
pub fn incremental_diff<K, V, I, O>(
    current: &std::collections::BTreeMap<K, V>,
    prev: &std::collections::BTreeMap<K, V>,
    agg: &dyn IncrementalAggregator<Input = I, Output = O>,
    make_input: impl Fn(&K, &V) -> I,
    outputs: &mut Vec<O>,
) where
    K: Ord,
    V: PartialEq,
{
    // Added or changed
    for (id, val) in current {
        match prev.get(id) {
            None => {
                if let Some(out) = agg.added(&make_input(id, val)) {
                    outputs.push(out);
                }
            }
            Some(old) if old != val => {
                if let Some(out) = agg.changed(&make_input(id, old), &make_input(id, val)) {
                    outputs.push(out);
                }
            }
            _ => {} // unchanged
        }
    }
    // Removed
    for (id, val) in prev {
        if !current.contains_key(id) {
            if let Some(out) = agg.removed(&make_input(id, val)) {
                outputs.push(out);
            }
        }
    }
}

/// Handler trait for state machines. Each SM type implements this on its struct.
///
/// ## Generated types per SM
///
/// The [`router!`] macro generates several types for each declared SM (e.g. `Workload`):
///
/// - **`{Sm}Input`** (enum): One variant per aggregated input plus one per event
///   channel where this SM is the receiver. Exhaustive matching ensures all inputs
///   are handled.
///
/// - **`{Sm}Ctx`** (trait): The context interface that handlers program against.
///   Methods:
///   - `ctx.id()` — get this instance's ID
///   - `ctx.set_{signal}(value)` — update an output signal
///   - `ctx.set_{edge}_edges(targets)` — update outgoing edges
///   - `ctx.send_{event}(target, payload)` — send a discrete event
///   - `ctx.create_{sm}(sm)` — create a new SM (returns ID for auto-ID types)
///   - `ctx.self_destruct()` — destroy this instance after effects are applied
///
/// - **`{Sm}CtxConcrete`** (struct): The concrete [`Ctx`] implementation, generic
///   over an [`IdAllocator`]. Construct with `::new(id, &mut allocator)`, pass to
///   a handler, then call `.into_effects()` to extract the buffered effects. This
///   is useful for testing SM handlers in isolation or for model checking — you
///   supply your own allocator and inspect the resulting effects without a router.
///
/// - **`{Sm}Effects`** (struct): Owned record of all effects a handler produced.
///   Public fields for each signal (`Option<T>`), each outgoing edge
///   (`Option<Vec<TargetId>>`), plus `pending_events`, `pending_creates`, and
///   `pending_self_destruct`. The router applies these via internal
///   `apply_{sm}_effects`, but they can also be inspected directly for testing
///   or model checking.
///
/// - **`PendingEvent`** (enum): One variant per declared event channel. Each
///   variant holds `(SenderId, ReceiverId, Payload)`.
///
/// - **`PendingCreate`** (enum): One variant per declared SM type. Each variant
///   holds `(Id, HandlerStruct)`.
///
/// ## Effect buffering
///
/// All changes made through the context are queued and applied after the handler
/// returns. Creates are applied first (so edges can reference new SMs), then
/// signals, edges, events, and finally self-destruct. The router then cascades
/// any resulting changes within the same round.
///
/// ## Lifecycle
///
/// - **`initialize`** is called once, immediately after the SM is created (both
///   for external `router.create_*()` calls and internal `ctx.create_*()` calls).
///   Use it to set initial signals, edges, or spawn child SMs. Has a default
///   no-op implementation.
///
/// - **`handle`** is called each time an aggregated input or event is delivered.
pub trait SmHandler<Ctx> {
    type Input;
    fn handle(&mut self, input: Self::Input, ctx: &mut Ctx);
    fn initialize(&mut self, _ctx: &mut Ctx) {}
}

/// Built-in aggregator that collects all signal values into a `Vec`.
///
/// Works with any single-source `(Id, Value)` input. The ID is discarded; only
/// values are collected. SMs enforce cardinality invariants themselves — for
/// example, a spec input expects `len 0..1` and can use `.into_iter().next()`
/// to extract the single value.
pub struct ListAggregator<Id, V>(std::marker::PhantomData<(Id, V)>);

impl<Id, V> ListAggregator<Id, V> {
    pub fn new() -> Self {
        ListAggregator(std::marker::PhantomData)
    }
}

impl<Id, V> Default for ListAggregator<Id, V> {
    fn default() -> Self {
        Self::new()
    }
}

// Allow the crate to refer to itself by extern name, so that the proc-macro
// generated code (`::distvirt_sm_router::trace::...`) resolves both from within
// this crate and from external crates.
extern crate self as distvirt_sm_router;

pub use distvirt_sm_router_macros::router;
pub use untyped_vec::UntypedVec;

mod edge_map;
pub use edge_map::{EdgeDiff, EdgeMap};

impl<Id, V: Clone> Aggregator for ListAggregator<Id, V> {
    type Input = (Id, V);
    type Output = Vec<V>;

    fn aggregate(&self, inputs: &[(Id, V)]) -> Vec<V> {
        inputs.iter().map(|(_, v)| v.clone()).collect()
    }
}

pub mod model_check;
pub mod trace;

/// Phase state machine for manual (step-by-step) propagation.
///
/// Tracks which sub-round the router is currently in:
/// - `Idle`: no manual propagation in progress.
/// - `Inputs(n)`: delivering dirty inputs; `n` outstanding.
/// - `Events(n)`: delivering events; `n` outstanding.
#[derive(Clone, Debug)]
pub enum ManualPhase {
    Idle,
    Inputs(usize),
    Events(usize),
}

/// Trait for delivery items that can be grouped by target SM.
pub trait Delivery {
    /// Returns the SM index for grouping. Items with the same key target
    /// the same SM within the current sub-round.
    fn group_key(&self) -> usize;
}

/// Step-by-step propagation controller.
///
/// Holds sorted deliveries and yields them one SM group at a time.
/// Created by `router.begin_manual_propagate()`.
pub struct ManualPropagate<D> {
    /// Sorted by group_key (ascending). We pop from the back.
    deliveries: Vec<D>,
}

impl<D: Delivery> ManualPropagate<D> {
    pub fn new(mut deliveries: Vec<D>) -> Self {
        // Sort ascending by sm_index — we pop groups from the back
        deliveries.sort_by_key(|d| d.group_key());
        ManualPropagate { deliveries }
    }

    /// Returns all deliveries for one SM group, or None if empty.
    /// Groups are returned in reverse order (last sorted group first),
    /// but since SM groups are independent, order doesn't matter.
    pub fn next_group(&mut self) -> Option<Vec<D>> {
        if self.deliveries.is_empty() {
            return None;
        }
        let last_key = self.deliveries.last().unwrap().group_key();
        let group_start = self
            .deliveries
            .iter()
            .rposition(|d| d.group_key() != last_key)
            .map(|i| i + 1)
            .unwrap_or(0);
        Some(self.deliveries.split_off(group_start))
    }

    /// Number of remaining deliveries across all groups.
    pub fn remaining(&self) -> usize {
        self.deliveries.len()
    }
}

/// Trait implemented by the generated `NodeKind` enum.
///
/// `NodeKind` has one variant per declared SM and port type. Auto-ID variants
/// come first (in declaration order), followed by manual-ID variants. The
/// `index()` method returns the discriminant, which is used by [`IdAllocator`]
/// to maintain per-kind counters.
pub trait IdKind: Copy + Clone + 'static {
    /// Number of node kinds that use auto-generated IDs.
    const AUTO_COUNT: usize;
    /// Total number of node kinds (auto + manual).
    const COUNT: usize;
    /// Discriminant index for this kind.
    fn index(self) -> usize;
    /// Human-readable name (e.g. `"Workload"`).
    fn name(self) -> &'static str;
    /// Whether this kind uses auto-generated IDs.
    fn is_auto(self) -> bool {
        self.index() < Self::AUTO_COUNT
    }
}

/// Swappable ID allocation strategy for auto-ID node types.
///
/// The router and [`CtxConcrete`] are generic over this trait. The default
/// implementation is [`SequentialIds`]. Custom allocators can be used for
/// model checking (e.g. per-creator-instance counters to avoid false state
/// divergence from ID ordering).
///
/// To use `CtxConcrete` outside the router (for isolated SM testing or model
/// checking), supply any `IdAllocator` implementation:
///
/// ```rust,ignore
/// let mut alloc = SequentialIds::<NodeKind>::new();
/// let mut ctx = WorkloadCtxConcrete::new(workload_id, &mut alloc);
/// sm.handle(input, &mut ctx);
/// let effects = ctx.into_effects();
/// // inspect effects...
/// ```
pub trait IdAllocator<K: IdKind>: Clone {
    /// Snapshot representation for serialization.
    type Snapshot: Clone;
    /// Allocate a new u64 for the given auto-ID node kind.
    /// `creator` is `Some((kind, raw_id))` when called from an SM handler.
    fn alloc(&mut self, kind: K, creator: Option<(K, u64)>) -> u64;
    /// Export state for snapshotting.
    fn snapshot(&self) -> Self::Snapshot;
    /// Reconstruct from a snapshot.
    fn from_snapshot(snapshot: &Self::Snapshot) -> Self;
}

/// Default allocator: simple sequential counters per auto-ID node kind.
#[derive(Clone, Debug)]
pub struct SequentialIds<K: IdKind> {
    counters: Vec<u64>,
    _kind: std::marker::PhantomData<K>,
}

impl<K: IdKind> SequentialIds<K> {
    pub fn new() -> Self {
        Self {
            counters: vec![0; K::AUTO_COUNT],
            _kind: std::marker::PhantomData,
        }
    }
}

impl<K: IdKind> Default for SequentialIds<K> {
    fn default() -> Self {
        Self::new()
    }
}

impl<K: IdKind> IdAllocator<K> for SequentialIds<K> {
    type Snapshot = Vec<u64>;
    fn alloc(&mut self, kind: K, _creator: Option<(K, u64)>) -> u64 {
        let idx = kind.index();
        let id = self.counters[idx];
        self.counters[idx] += 1;
        id
    }
    fn snapshot(&self) -> Vec<u64> {
        self.counters.clone()
    }
    fn from_snapshot(snapshot: &Vec<u64>) -> Self {
        Self {
            counters: snapshot.clone(),
            _kind: std::marker::PhantomData,
        }
    }
}

#[cfg(test)]
mod tests;
