//! # Propagation Tracing
//!
//! Observability for the signal router. The [`Tracer`] trait receives a single
//! [`TraceEvent`] callback at every decision point during propagation — signal
//! changes, edge changes, input deliveries, event routing, SM lifecycle.
//!
//! ## Key types
//!
//! - [`TraceEvent<'a>`]: An enum representing one trace event. When `'a` is a
//!   concrete lifetime it borrows `&dyn Debug` values cheaply (zero formatting
//!   cost). Calling [`TraceEvent::into_owned`] formats all debug values and
//!   returns a `TraceEvent<'static>` that can be stored.
//!
//! - [`DebugValue<'a>`]: Either a borrowed `&'a dyn Debug` or an owned `String`.
//!   Used inside `TraceEvent` for all dynamic values (IDs, signal values, etc.).
//!
//! - [`Tracer`]: One method — `fn trace(&mut self, event: TraceEvent<'_>)`.
//!
//! ## Built-in tracers
//!
//! - [`NoopTracer`]: Does nothing. Default. Zero-cost when monomorphized.
//! - [`RecordingTracer`]: Captures all events into a `Vec<TraceEvent<'static>>`.
//!   Implements `Display` for human-readable indented tree output.
//! - [`RingTracer`]: Bounded rolling buffer. Always recording, oldest events
//!   evicted. Use `.snapshot()` to grab current contents on error.
//! - [`PanicTracer`]: Wraps any tracer and auto-dumps the trace on panic
//!   (via `Drop` + `std::thread::panicking()`).
//!
//! ## Utilities
//!
//! - [`TraceSummary`]: Compact stats (counts of rounds, deliveries, creates,
//!   etc.) computed from a `&[TraceEvent<'static>]`. Has its own `Display`.
//! - [`fmt_trace_tree`]: Formats trace events as an indented tree showing
//!   causality (effects indented under the handler that caused them).
//!
//! ## Composition
//!
//! With a single `trace()` method, composition is trivial — just delegate:
//!
//! ```rust,ignore
//! struct NodeFilter<T: Tracer> {
//!     node: &'static str,
//!     inner: T,
//! }
//!
//! impl<T: Tracer> Tracer for NodeFilter<T> {
//!     fn trace(&mut self, event: TraceEvent<'_>) {
//!         if event.node() == Some(self.node) {
//!             self.inner.trace(event);
//!         }
//!     }
//! }
//! ```
//!
//! ## Ordering guarantees
//!
//! Within a `propagate()` call, events arrive in this order:
//!
//! ```text
//! PropagateStart
//! ├─ RoundStart(1)
//! │  ├─ InputDelivered / InputSuppressed  (one per dirty input)
//! │  │  ├─ EffectsStart
//! │  │  │  ├─ SmCreated          (from handler's ctx.create_*())
//! │  │  │  ├─ SignalChanged      (from handler's ctx.set_*())
//! │  │  │  ├─ EdgeChanged        (from handler's ctx.set_*_edges())
//! │  │  │  ├─ EventQueued        (from handler's ctx.send_*())
//! │  │  │  └─ SmDestroyed        (from handler's ctx.self_destruct())
//! │  │  └─ EffectsEnd
//! │  ├─ EventDelivered             (one per pending event)
//! │  │  └─ EffectsStart/End        (same pattern)
//! │  └─ RoundEnd(1)
//! ├─ RoundStart(2)
//! │  └─ ...                         (cascading changes)
//! ├─ InvariantViolation              (one per violated invariant at quiescence)
//! └─ PropagateEnd(total_rounds)
//! ```
//!
//! ## Extension ideas
//!
//! The `Tracer` trait + `TraceEvent` enum are the base facility. Some downstream
//! uses they enable:
//!
//! - **Selective/filtered tracing**: The `NodeFilter` example above costs nothing
//!   for filtered-out events — `event.node()` is a `&'static str` comparison,
//!   no formatting happens. Chain multiple filters, or build an allowlist tracer
//!   that only records events for specific SM types or signal names.
//!
//! - **Causality analysis**: `EffectsStart`/`EffectsEnd` bracketing lets you
//!   build a causality DAG post-hoc: "this signal change was an effect of this
//!   handler invocation, which was caused by this input delivery, which was
//!   triggered by this earlier signal change." Walk backwards from any unexpected
//!   state to find its root cause.
//!
//! - **State diffing**: `SignalChanged`/`EdgeChanged` entries are exactly the diff
//!   between pre- and post-propagate states. A utility could format a compact
//!   summary: "this propagate() changed 3 signals, added 2 edges, created 1 SM."
//!   (See [`TraceSummary`] for a starting point.)
//!
//! - **Performance profiling**: Count rounds, deliveries, suppressions per SM
//!   type to find hot spots or excessive cascade depth. The `RingTracer` is
//!   useful here — profile the last N events without unbounded memory growth.
//!
//! ## Invariants
//!
//! Invariants are boolean expressions declared on signals in the `router!` macro's
//! `invariants {}` block. They may be transiently violated during propagation
//! (intermediate states are inconsistent by nature), but at quiescence — after all
//! rounds complete — violations are checked and emitted as [`TraceEvent::InvariantViolation`]
//! events. Tracers can act on violations: [`PanicTracer`] dumps the full trace,
//! [`RingTracer`] provides recent history for debugging, and production tracers can
//! log or alert.
//!
//! - **Invariant-based alerting**: `InvariantViolation` events are emitted at
//!   quiescence for any signal whose declared invariant expression evaluates to
//!   `false`. Test tracers can dump the full causality trace on violation,
//!   production tracers can log/alert, and `PanicTracer` auto-dumps context.
//!
//! - **Production structured logging**: A tracer that writes to `tracing`/`log`
//!   crate, filtering by severity (e.g., only lifecycle events and errors).
//!   Since formatting is deferred via `DebugValue`, unlogged events pay zero
//!   formatting cost.
//!
//! - **Multiplexing**: A `TeeTracer<A, B>` that forwards each event to two
//!   inner tracers. With a single `trace()` method this is straightforward —
//!   clone the owned event or use `clone_as_event` to re-borrow.

use std::collections::VecDeque;
use std::fmt::{self, Debug, Display, Write as FmtWrite};

// ============================================================================
// DebugValue — Cow-like wrapper for &dyn Debug
// ============================================================================

/// Either a borrowed `&dyn Debug` reference or an already-formatted `String`.
///
/// Used inside [`TraceEvent`] for all dynamic values (IDs, signal values).
/// Formatting is deferred until [`DebugValue::into_owned`] or display.
pub enum DebugValue<'a> {
    Borrowed(&'a dyn Debug),
    Owned(String),
}

impl<'a> DebugValue<'a> {
    /// Format the value (if borrowed) and return an owned version.
    pub fn into_owned(self) -> DebugValue<'static> {
        match self {
            DebugValue::Borrowed(v) => DebugValue::Owned(format!("{:?}", v)),
            DebugValue::Owned(s) => DebugValue::Owned(s),
        }
    }

    /// Get the string representation without consuming.
    pub fn to_string_lossy(&self) -> String {
        match self {
            DebugValue::Borrowed(v) => format!("{:?}", v),
            DebugValue::Owned(s) => s.clone(),
        }
    }
}

impl Debug for DebugValue<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DebugValue::Borrowed(v) => v.fmt(f),
            DebugValue::Owned(s) => f.write_str(s),
        }
    }
}

impl Display for DebugValue<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DebugValue::Borrowed(v) => write!(f, "{:?}", v),
            DebugValue::Owned(s) => f.write_str(s),
        }
    }
}

impl Clone for DebugValue<'static> {
    fn clone(&self) -> Self {
        match self {
            DebugValue::Borrowed(_) => unreachable!("'static DebugValue is always Owned"),
            DebugValue::Owned(s) => DebugValue::Owned(s.clone()),
        }
    }
}

// ============================================================================
// TraceEvent — unified event enum
// ============================================================================

/// A single trace event. Borrows values when `'a` is a concrete lifetime;
/// owns them as formatted strings when `'a = 'static`.
///
/// Construct with borrowed `DebugValue::Borrowed(&val)` in generated code
/// (zero allocation). Call `.into_owned()` to format and store.
pub enum TraceEvent<'a> {
    PropagateStart,
    PropagateEnd {
        rounds: usize,
    },
    RoundStart {
        depth: usize,
    },
    RoundEnd {
        depth: usize,
    },

    InputDelivered {
        node: &'static str,
        id: DebugValue<'a>,
        input: &'static str,
        value: DebugValue<'a>,
    },
    InputSuppressed {
        node: &'static str,
        id: DebugValue<'a>,
        input: &'static str,
    },

    EffectsStart {
        node: &'static str,
        id: DebugValue<'a>,
    },
    EffectsEnd {
        node: &'static str,
        id: DebugValue<'a>,
    },

    SignalChanged {
        node: &'static str,
        id: DebugValue<'a>,
        signal: &'static str,
        old: DebugValue<'a>,
        new: DebugValue<'a>,
    },
    EdgeChanged {
        edge: &'static str,
        source: DebugValue<'a>,
        added: DebugValue<'a>,
        removed: DebugValue<'a>,
    },

    EventQueued {
        event: &'static str,
        sender: DebugValue<'a>,
        receiver: DebugValue<'a>,
        payload: DebugValue<'a>,
    },
    EventDelivered {
        event: &'static str,
        sender: DebugValue<'a>,
        receiver: DebugValue<'a>,
        payload: DebugValue<'a>,
    },

    SmCreated {
        node: &'static str,
        id: DebugValue<'a>,
    },
    SmInitialized {
        node: &'static str,
        id: DebugValue<'a>,
    },
    SmDestroyed {
        node: &'static str,
        id: DebugValue<'a>,
    },
    PortCreated {
        node: &'static str,
        id: DebugValue<'a>,
    },
    PortDestroyed {
        node: &'static str,
        id: DebugValue<'a>,
    },

    InvariantViolation {
        node: &'static str,
        id: DebugValue<'a>,
        signal: &'static str,
        value: DebugValue<'a>,
        invariant_expr: &'static str,
    },
}

impl<'a> TraceEvent<'a> {
    /// Format all borrowed debug values and return an owned event.
    pub fn into_owned(self) -> TraceEvent<'static> {
        match self {
            TraceEvent::PropagateStart => TraceEvent::PropagateStart,
            TraceEvent::PropagateEnd { rounds } => TraceEvent::PropagateEnd { rounds },
            TraceEvent::RoundStart { depth } => TraceEvent::RoundStart { depth },
            TraceEvent::RoundEnd { depth } => TraceEvent::RoundEnd { depth },
            TraceEvent::InputDelivered {
                node,
                id,
                input,
                value,
            } => TraceEvent::InputDelivered {
                node,
                id: id.into_owned(),
                input,
                value: value.into_owned(),
            },
            TraceEvent::InputSuppressed { node, id, input } => TraceEvent::InputSuppressed {
                node,
                id: id.into_owned(),
                input,
            },
            TraceEvent::EffectsStart { node, id } => TraceEvent::EffectsStart {
                node,
                id: id.into_owned(),
            },
            TraceEvent::EffectsEnd { node, id } => TraceEvent::EffectsEnd {
                node,
                id: id.into_owned(),
            },
            TraceEvent::SignalChanged {
                node,
                id,
                signal,
                old,
                new,
            } => TraceEvent::SignalChanged {
                node,
                id: id.into_owned(),
                signal,
                old: old.into_owned(),
                new: new.into_owned(),
            },
            TraceEvent::EdgeChanged {
                edge,
                source,
                added,
                removed,
            } => TraceEvent::EdgeChanged {
                edge,
                source: source.into_owned(),
                added: added.into_owned(),
                removed: removed.into_owned(),
            },
            TraceEvent::EventQueued {
                event,
                sender,
                receiver,
                payload,
            } => TraceEvent::EventQueued {
                event,
                sender: sender.into_owned(),
                receiver: receiver.into_owned(),
                payload: payload.into_owned(),
            },
            TraceEvent::EventDelivered {
                event,
                sender,
                receiver,
                payload,
            } => TraceEvent::EventDelivered {
                event,
                sender: sender.into_owned(),
                receiver: receiver.into_owned(),
                payload: payload.into_owned(),
            },
            TraceEvent::SmCreated { node, id } => TraceEvent::SmCreated {
                node,
                id: id.into_owned(),
            },
            TraceEvent::SmInitialized { node, id } => TraceEvent::SmInitialized {
                node,
                id: id.into_owned(),
            },
            TraceEvent::SmDestroyed { node, id } => TraceEvent::SmDestroyed {
                node,
                id: id.into_owned(),
            },
            TraceEvent::PortCreated { node, id } => TraceEvent::PortCreated {
                node,
                id: id.into_owned(),
            },
            TraceEvent::PortDestroyed { node, id } => TraceEvent::PortDestroyed {
                node,
                id: id.into_owned(),
            },
            TraceEvent::InvariantViolation {
                node,
                id,
                signal,
                value,
                invariant_expr,
            } => TraceEvent::InvariantViolation {
                node,
                id: id.into_owned(),
                signal,
                value: value.into_owned(),
                invariant_expr,
            },
        }
    }

    /// Returns the node name if this event is associated with one.
    pub fn node(&self) -> Option<&'static str> {
        match self {
            TraceEvent::PropagateStart
            | TraceEvent::PropagateEnd { .. }
            | TraceEvent::RoundStart { .. }
            | TraceEvent::RoundEnd { .. } => None,
            TraceEvent::InputDelivered { node, .. }
            | TraceEvent::InputSuppressed { node, .. }
            | TraceEvent::EffectsStart { node, .. }
            | TraceEvent::EffectsEnd { node, .. }
            | TraceEvent::SignalChanged { node, .. }
            | TraceEvent::SmCreated { node, .. }
            | TraceEvent::SmInitialized { node, .. }
            | TraceEvent::SmDestroyed { node, .. }
            | TraceEvent::PortCreated { node, .. }
            | TraceEvent::PortDestroyed { node, .. }
            | TraceEvent::InvariantViolation { node, .. } => Some(node),
            TraceEvent::EdgeChanged { .. }
            | TraceEvent::EventQueued { .. }
            | TraceEvent::EventDelivered { .. } => None,
        }
    }
}

impl Clone for TraceEvent<'static> {
    fn clone(&self) -> Self {
        match self {
            TraceEvent::PropagateStart => TraceEvent::PropagateStart,
            TraceEvent::PropagateEnd { rounds } => TraceEvent::PropagateEnd { rounds: *rounds },
            TraceEvent::RoundStart { depth } => TraceEvent::RoundStart { depth: *depth },
            TraceEvent::RoundEnd { depth } => TraceEvent::RoundEnd { depth: *depth },
            TraceEvent::InputDelivered {
                node,
                id,
                input,
                value,
            } => TraceEvent::InputDelivered {
                node,
                id: id.clone(),
                input,
                value: value.clone(),
            },
            TraceEvent::InputSuppressed { node, id, input } => TraceEvent::InputSuppressed {
                node,
                id: id.clone(),
                input,
            },
            TraceEvent::EffectsStart { node, id } => TraceEvent::EffectsStart {
                node,
                id: id.clone(),
            },
            TraceEvent::EffectsEnd { node, id } => TraceEvent::EffectsEnd {
                node,
                id: id.clone(),
            },
            TraceEvent::SignalChanged {
                node,
                id,
                signal,
                old,
                new,
            } => TraceEvent::SignalChanged {
                node,
                id: id.clone(),
                signal,
                old: old.clone(),
                new: new.clone(),
            },
            TraceEvent::EdgeChanged {
                edge,
                source,
                added,
                removed,
            } => TraceEvent::EdgeChanged {
                edge,
                source: source.clone(),
                added: added.clone(),
                removed: removed.clone(),
            },
            TraceEvent::EventQueued {
                event,
                sender,
                receiver,
                payload,
            } => TraceEvent::EventQueued {
                event,
                sender: sender.clone(),
                receiver: receiver.clone(),
                payload: payload.clone(),
            },
            TraceEvent::EventDelivered {
                event,
                sender,
                receiver,
                payload,
            } => TraceEvent::EventDelivered {
                event,
                sender: sender.clone(),
                receiver: receiver.clone(),
                payload: payload.clone(),
            },
            TraceEvent::SmCreated { node, id } => TraceEvent::SmCreated {
                node,
                id: id.clone(),
            },
            TraceEvent::SmInitialized { node, id } => TraceEvent::SmInitialized {
                node,
                id: id.clone(),
            },
            TraceEvent::SmDestroyed { node, id } => TraceEvent::SmDestroyed {
                node,
                id: id.clone(),
            },
            TraceEvent::PortCreated { node, id } => TraceEvent::PortCreated {
                node,
                id: id.clone(),
            },
            TraceEvent::PortDestroyed { node, id } => TraceEvent::PortDestroyed {
                node,
                id: id.clone(),
            },
            TraceEvent::InvariantViolation {
                node,
                id,
                signal,
                value,
                invariant_expr,
            } => TraceEvent::InvariantViolation {
                node,
                id: id.clone(),
                signal,
                value: value.clone(),
                invariant_expr,
            },
        }
    }
}

impl Debug for TraceEvent<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TraceEvent::PropagateStart => write!(f, "PropagateStart"),
            TraceEvent::PropagateEnd { rounds } => {
                write!(f, "PropagateEnd {{ rounds: {} }}", rounds)
            }
            TraceEvent::RoundStart { depth } => write!(f, "RoundStart {{ depth: {} }}", depth),
            TraceEvent::RoundEnd { depth } => write!(f, "RoundEnd {{ depth: {} }}", depth),
            TraceEvent::InputDelivered {
                node,
                id,
                input,
                value,
            } => write!(
                f,
                "InputDelivered {{ node: {:?}, id: {}, input: {:?}, value: {} }}",
                node, id, input, value
            ),
            TraceEvent::InputSuppressed { node, id, input } => write!(
                f,
                "InputSuppressed {{ node: {:?}, id: {}, input: {:?} }}",
                node, id, input
            ),
            TraceEvent::EffectsStart { node, id } => {
                write!(f, "EffectsStart {{ node: {:?}, id: {} }}", node, id)
            }
            TraceEvent::EffectsEnd { node, id } => {
                write!(f, "EffectsEnd {{ node: {:?}, id: {} }}", node, id)
            }
            TraceEvent::SignalChanged {
                node,
                id,
                signal,
                old,
                new,
            } => write!(
                f,
                "SignalChanged {{ node: {:?}, id: {}, signal: {:?}, old: {}, new: {} }}",
                node, id, signal, old, new
            ),
            TraceEvent::EdgeChanged {
                edge,
                source,
                added,
                removed,
            } => write!(
                f,
                "EdgeChanged {{ edge: {:?}, source: {}, added: {}, removed: {} }}",
                edge, source, added, removed
            ),
            TraceEvent::EventQueued {
                event,
                sender,
                receiver,
                payload,
            } => write!(
                f,
                "EventQueued {{ event: {:?}, sender: {}, receiver: {}, payload: {} }}",
                event, sender, receiver, payload
            ),
            TraceEvent::EventDelivered {
                event,
                sender,
                receiver,
                payload,
            } => write!(
                f,
                "EventDelivered {{ event: {:?}, sender: {}, receiver: {}, payload: {} }}",
                event, sender, receiver, payload
            ),
            TraceEvent::SmCreated { node, id } => {
                write!(f, "SmCreated {{ node: {:?}, id: {} }}", node, id)
            }
            TraceEvent::SmInitialized { node, id } => {
                write!(f, "SmInitialized {{ node: {:?}, id: {} }}", node, id)
            }
            TraceEvent::SmDestroyed { node, id } => {
                write!(f, "SmDestroyed {{ node: {:?}, id: {} }}", node, id)
            }
            TraceEvent::PortCreated { node, id } => {
                write!(f, "PortCreated {{ node: {:?}, id: {} }}", node, id)
            }
            TraceEvent::PortDestroyed { node, id } => {
                write!(f, "PortDestroyed {{ node: {:?}, id: {} }}", node, id)
            }
            TraceEvent::InvariantViolation {
                node,
                id,
                signal,
                value,
                invariant_expr,
            } => write!(
                f,
                "InvariantViolation {{ node: {:?}, id: {}, signal: {:?}, value: {}, invariant_expr: {:?} }}",
                node, id, signal, value, invariant_expr
            ),
        }
    }
}

// ============================================================================
// Tracer trait — single method
// ============================================================================

/// Receives trace events during signal propagation.
///
/// One method — implement `trace()` and pattern-match on the event variants
/// you care about. Unknown variants are ignored with a `_ => {}` arm, so
/// adding new event types is non-breaking.
///
/// All methods on the generated `Router` call `self.tracer.trace(...)` with
/// a `TraceEvent` that borrows values. The tracer decides whether to format
/// and store (`.into_owned()`) or discard.
pub trait Tracer {
    fn trace(&mut self, event: TraceEvent<'_>);
}

// ============================================================================
// NoopTracer — zero-cost default
// ============================================================================

/// A tracer that does nothing. The empty `trace()` method monomorphizes away
/// entirely, so untraced routers pay zero cost.
pub struct NoopTracer;

impl Tracer for NoopTracer {
    #[inline(always)]
    fn trace(&mut self, _event: TraceEvent<'_>) {}
}

// ============================================================================
// RecordingTracer — captures all events
// ============================================================================

/// Captures all trace events into a `Vec<TraceEvent<'static>>`.
///
/// Use `Display` to get human-readable indented tree output showing causality.
pub struct RecordingTracer {
    entries: Vec<TraceEvent<'static>>,
}

impl RecordingTracer {
    pub fn new() -> Self {
        RecordingTracer {
            entries: Vec::new(),
        }
    }

    pub fn entries(&self) -> &[TraceEvent<'static>] {
        &self.entries
    }

    pub fn into_entries(self) -> Vec<TraceEvent<'static>> {
        self.entries
    }

    pub fn clear(&mut self) {
        self.entries.clear();
    }

    pub fn summary(&self) -> TraceSummary {
        TraceSummary::from_events(&self.entries)
    }
}

impl Default for RecordingTracer {
    fn default() -> Self {
        Self::new()
    }
}

impl Tracer for RecordingTracer {
    fn trace(&mut self, event: TraceEvent<'_>) {
        self.entries.push(event.into_owned());
    }
}

impl fmt::Display for RecordingTracer {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt_trace_tree(&self.entries, f)
    }
}

// ============================================================================
// RingTracer — bounded rolling buffer
// ============================================================================

/// Bounded rolling buffer tracer. Always recording; oldest events evicted
/// when capacity is reached.
///
/// Designed for production use: memory is bounded and predictable. On error,
/// call `.snapshot()` to grab the recent trace history.
pub struct RingTracer {
    buf: VecDeque<TraceEvent<'static>>,
    capacity: usize,
}

impl RingTracer {
    pub fn new(capacity: usize) -> Self {
        RingTracer {
            buf: VecDeque::with_capacity(capacity),
            capacity,
        }
    }

    /// Take a snapshot of the current buffer contents.
    pub fn snapshot(&self) -> Vec<TraceEvent<'static>> {
        self.buf.iter().cloned().collect()
    }

    /// Number of events currently in the buffer.
    pub fn len(&self) -> usize {
        self.buf.len()
    }

    pub fn is_empty(&self) -> bool {
        self.buf.is_empty()
    }

    pub fn capacity(&self) -> usize {
        self.capacity
    }

    pub fn clear(&mut self) {
        self.buf.clear();
    }

    pub fn summary(&self) -> TraceSummary {
        let events: Vec<_> = self.buf.iter().cloned().collect();
        TraceSummary::from_events(&events)
    }
}

impl Tracer for RingTracer {
    fn trace(&mut self, event: TraceEvent<'_>) {
        if self.buf.len() >= self.capacity {
            self.buf.pop_front();
        }
        self.buf.push_back(event.into_owned());
    }
}

impl fmt::Display for RingTracer {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let events: Vec<_> = self.buf.iter().cloned().collect();
        fmt_trace_tree(&events, f)
    }
}

// ============================================================================
// PanicTracer — auto-dump on panic
// ============================================================================

/// Wraps any tracer and prints the trace to stderr if the thread is panicking
/// when this tracer is dropped.
///
/// Usage in tests:
/// ```rust,ignore
/// let tracer = PanicTracer::new(RecordingTracer::new());
/// let mut router = Router::new_traced(16, tracer);
/// // ... if any assertion fails, the trace is printed automatically
/// ```
pub struct PanicTracer<T: Tracer> {
    inner: T,
    /// Shadow recording for display on panic. We always record so we have
    /// something to print even if the inner tracer filters events.
    shadow: Vec<TraceEvent<'static>>,
}

impl<T: Tracer> PanicTracer<T> {
    pub fn new(inner: T) -> Self {
        PanicTracer {
            inner,
            shadow: Vec::new(),
        }
    }

    /// Access the inner tracer.
    pub fn inner(&self) -> &T {
        &self.inner
    }

    /// Mutably access the inner tracer.
    pub fn inner_mut(&mut self) -> &mut T {
        &mut self.inner
    }

    /// Consume and return the inner tracer.
    pub fn into_inner(self) -> T {
        // Use ManuallyDrop to prevent Drop from running (which would
        // try to print the trace). We're intentionally consuming.
        let this = std::mem::ManuallyDrop::new(self);
        // SAFETY: We're reading inner out of a ManuallyDrop wrapper.
        // The shadow Vec will leak, but that's acceptable since this
        // is a consume operation.
        unsafe { std::ptr::read(&this.inner) }
    }
}

impl<T: Tracer> Tracer for PanicTracer<T> {
    fn trace(&mut self, event: TraceEvent<'_>) {
        self.shadow.push(event.into_owned());
        // Re-create a borrowed event from the just-stored owned one
        // to pass to the inner tracer. The owned event in shadow has
        // DebugValue::Owned(String) values, so we pass those through.
        let last = self.shadow.last().unwrap();
        // We need to pass the event to inner too. Since we already consumed
        // the original event with into_owned(), construct a new owned event
        // from the shadow copy for the inner tracer.
        self.inner.trace(clone_as_event(last));
    }
}

/// Helper: create a TraceEvent<'_> that borrows from a TraceEvent<'static>.
/// Since all DebugValues in a 'static event are Owned(String), we can
/// re-wrap them as Borrowed(&String) since String: Debug.
fn clone_as_event<'a>(event: &'a TraceEvent<'static>) -> TraceEvent<'a> {
    match event {
        TraceEvent::PropagateStart => TraceEvent::PropagateStart,
        TraceEvent::PropagateEnd { rounds } => TraceEvent::PropagateEnd { rounds: *rounds },
        TraceEvent::RoundStart { depth } => TraceEvent::RoundStart { depth: *depth },
        TraceEvent::RoundEnd { depth } => TraceEvent::RoundEnd { depth: *depth },
        TraceEvent::InputDelivered {
            node,
            id,
            input,
            value,
        } => TraceEvent::InputDelivered {
            node,
            id: borrow_debug_value(id),
            input,
            value: borrow_debug_value(value),
        },
        TraceEvent::InputSuppressed { node, id, input } => TraceEvent::InputSuppressed {
            node,
            id: borrow_debug_value(id),
            input,
        },
        TraceEvent::EffectsStart { node, id } => TraceEvent::EffectsStart {
            node,
            id: borrow_debug_value(id),
        },
        TraceEvent::EffectsEnd { node, id } => TraceEvent::EffectsEnd {
            node,
            id: borrow_debug_value(id),
        },
        TraceEvent::SignalChanged {
            node,
            id,
            signal,
            old,
            new,
        } => TraceEvent::SignalChanged {
            node,
            id: borrow_debug_value(id),
            signal,
            old: borrow_debug_value(old),
            new: borrow_debug_value(new),
        },
        TraceEvent::EdgeChanged {
            edge,
            source,
            added,
            removed,
        } => TraceEvent::EdgeChanged {
            edge,
            source: borrow_debug_value(source),
            added: borrow_debug_value(added),
            removed: borrow_debug_value(removed),
        },
        TraceEvent::EventQueued {
            event,
            sender,
            receiver,
            payload,
        } => TraceEvent::EventQueued {
            event,
            sender: borrow_debug_value(sender),
            receiver: borrow_debug_value(receiver),
            payload: borrow_debug_value(payload),
        },
        TraceEvent::EventDelivered {
            event,
            sender,
            receiver,
            payload,
        } => TraceEvent::EventDelivered {
            event,
            sender: borrow_debug_value(sender),
            receiver: borrow_debug_value(receiver),
            payload: borrow_debug_value(payload),
        },
        TraceEvent::SmCreated { node, id } => TraceEvent::SmCreated {
            node,
            id: borrow_debug_value(id),
        },
        TraceEvent::SmInitialized { node, id } => TraceEvent::SmInitialized {
            node,
            id: borrow_debug_value(id),
        },
        TraceEvent::SmDestroyed { node, id } => TraceEvent::SmDestroyed {
            node,
            id: borrow_debug_value(id),
        },
        TraceEvent::PortCreated { node, id } => TraceEvent::PortCreated {
            node,
            id: borrow_debug_value(id),
        },
        TraceEvent::PortDestroyed { node, id } => TraceEvent::PortDestroyed {
            node,
            id: borrow_debug_value(id),
        },
        TraceEvent::InvariantViolation {
            node,
            id,
            signal,
            value,
            invariant_expr,
        } => TraceEvent::InvariantViolation {
            node,
            id: borrow_debug_value(id),
            signal,
            value: borrow_debug_value(value),
            invariant_expr,
        },
    }
}

fn borrow_debug_value<'a>(v: &'a DebugValue<'static>) -> DebugValue<'a> {
    match v {
        DebugValue::Owned(s) => DebugValue::Borrowed(s),
        DebugValue::Borrowed(_) => unreachable!("'static DebugValue is always Owned"),
    }
}

impl<T: Tracer> Drop for PanicTracer<T> {
    fn drop(&mut self) {
        if std::thread::panicking() && !self.shadow.is_empty() {
            let mut buf = String::new();
            let _ = writeln!(buf, "\n╔══ Signal Router Trace (PanicTracer auto-dump) ══");
            let summary = TraceSummary::from_events(&self.shadow);
            let _ = writeln!(buf, "║ {}", summary);
            let _ = writeln!(buf, "╠══ Full trace ══");
            let _ = write_trace_tree(&self.shadow, "║ ", &mut buf);
            let _ = writeln!(buf, "╚══════════════════════════════════════════════════");
            eprintln!("{}", buf);
        }
    }
}

// ============================================================================
// TraceSummary — compact stats
// ============================================================================

/// Compact summary statistics computed from trace events.
#[derive(Debug, Clone, Default)]
pub struct TraceSummary {
    pub propagations: usize,
    pub total_rounds: usize,
    pub inputs_delivered: usize,
    pub inputs_suppressed: usize,
    pub signal_changes: usize,
    pub edge_changes: usize,
    pub events_queued: usize,
    pub events_delivered: usize,
    pub sms_created: usize,
    pub sms_destroyed: usize,
    pub ports_created: usize,
    pub ports_destroyed: usize,
    pub invariant_violations: usize,
}

impl TraceSummary {
    pub fn from_events(events: &[TraceEvent<'static>]) -> Self {
        let mut s = TraceSummary::default();
        for event in events {
            match event {
                TraceEvent::PropagateEnd { .. } => s.propagations += 1,
                TraceEvent::RoundEnd { .. } => s.total_rounds += 1,
                TraceEvent::InputDelivered { .. } => s.inputs_delivered += 1,
                TraceEvent::InputSuppressed { .. } => s.inputs_suppressed += 1,
                TraceEvent::SignalChanged { .. } => s.signal_changes += 1,
                TraceEvent::EdgeChanged { .. } => s.edge_changes += 1,
                TraceEvent::EventQueued { .. } => s.events_queued += 1,
                TraceEvent::EventDelivered { .. } => s.events_delivered += 1,
                TraceEvent::SmCreated { .. } => s.sms_created += 1,
                TraceEvent::SmInitialized { .. } => {}

                TraceEvent::SmDestroyed { .. } => s.sms_destroyed += 1,
                TraceEvent::PortCreated { .. } => s.ports_created += 1,
                TraceEvent::PortDestroyed { .. } => s.ports_destroyed += 1,
                TraceEvent::InvariantViolation { .. } => s.invariant_violations += 1,
                _ => {}
            }
        }
        s
    }
}

impl fmt::Display for TraceSummary {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut parts = Vec::new();
        if self.propagations > 0 {
            parts.push(format!(
                "{} propagation{}",
                self.propagations,
                if self.propagations != 1 { "s" } else { "" }
            ));
        }
        if self.total_rounds > 0 {
            parts.push(format!(
                "{} round{}",
                self.total_rounds,
                if self.total_rounds != 1 { "s" } else { "" }
            ));
        }
        if self.inputs_delivered > 0 {
            let mut s = format!("{} delivered", self.inputs_delivered);
            if self.inputs_suppressed > 0 {
                s.push_str(&format!(" ({} suppressed)", self.inputs_suppressed));
            }
            parts.push(s);
        } else if self.inputs_suppressed > 0 {
            parts.push(format!("{} suppressed", self.inputs_suppressed));
        }
        if self.signal_changes > 0 {
            parts.push(format!(
                "{} signal change{}",
                self.signal_changes,
                if self.signal_changes != 1 { "s" } else { "" }
            ));
        }
        if self.edge_changes > 0 {
            parts.push(format!(
                "{} edge change{}",
                self.edge_changes,
                if self.edge_changes != 1 { "s" } else { "" }
            ));
        }
        if self.events_delivered > 0 {
            parts.push(format!("{} events", self.events_delivered));
        }
        if self.sms_created > 0 || self.sms_destroyed > 0 {
            parts.push(format!(
                "{} SM created, {} destroyed",
                self.sms_created, self.sms_destroyed
            ));
        }
        if self.ports_created > 0 || self.ports_destroyed > 0 {
            parts.push(format!(
                "{} ports created, {} destroyed",
                self.ports_created, self.ports_destroyed
            ));
        }
        if self.invariant_violations > 0 {
            parts.push(format!(
                "{} invariant violation{}",
                self.invariant_violations,
                if self.invariant_violations != 1 { "s" } else { "" }
            ));
        }
        if parts.is_empty() {
            write!(f, "(empty trace)")
        } else {
            write!(f, "{}", parts.join(", "))
        }
    }
}

// ============================================================================
// Tree display — human-readable indented trace output
// ============================================================================

/// Format trace events as an indented tree showing causality.
///
/// Effects (signal changes, edge changes, creates, destroys) are indented
/// under the handler invocation that caused them. Propagation rounds are
/// shown as nested levels.
pub fn fmt_trace_tree(entries: &[TraceEvent<'static>], f: &mut fmt::Formatter<'_>) -> fmt::Result {
    write_trace_tree(entries, "", f)
}

/// Write trace events as an indented tree to any `fmt::Write` target.
///
/// `prefix` is prepended to every line (e.g. `"║ "` for box-drawing output).
pub fn write_trace_tree(
    entries: &[TraceEvent<'static>],
    prefix: &str,
    w: &mut dyn FmtWrite,
) -> fmt::Result {
    let mut depth: usize = 0;

    for entry in entries {
        let indent = "  ".repeat(depth);
        match entry {
            TraceEvent::PropagateStart => {
                writeln!(w, "{}{}=== propagate ===", prefix, indent)?;
                depth += 1;
            }
            TraceEvent::PropagateEnd { rounds } => {
                depth = depth.saturating_sub(1);
                let indent = "  ".repeat(depth);
                writeln!(w, "{}{}=== complete ({} rounds) ===", prefix, indent, rounds)?;
            }
            TraceEvent::RoundStart { depth: d } => {
                writeln!(w, "{}{}Round {}:", prefix, indent, d)?;
                depth += 1;
            }
            TraceEvent::RoundEnd { .. } => {
                depth = depth.saturating_sub(1);
            }
            TraceEvent::InputDelivered {
                node,
                id,
                input,
                value,
            } => {
                writeln!(w, "{}{}deliver {}({}) <- {}({})", prefix, indent, node, id, input, value)?;
            }
            TraceEvent::InputSuppressed { node, id, input } => {
                writeln!(
                    w,
                    "{}{}suppress {}({}) <- {} (unchanged)",
                    prefix, indent, node, id, input
                )?;
            }
            TraceEvent::EffectsStart { node, id } => {
                writeln!(w, "{}{}effects {}({}):", prefix, indent, node, id)?;
                depth += 1;
            }
            TraceEvent::EffectsEnd { .. } => {
                depth = depth.saturating_sub(1);
            }
            TraceEvent::SignalChanged {
                node,
                id,
                signal,
                old,
                new,
            } => {
                writeln!(
                    w,
                    "{}{}signal {}({})::{}:  {} -> {}",
                    prefix, indent, node, id, signal, old, new
                )?;
            }
            TraceEvent::EdgeChanged {
                edge,
                source,
                added,
                removed,
            } => {
                writeln!(
                    w,
                    "{}{}edge {}: {} (+{} -{})",
                    prefix, indent, edge, source, added, removed
                )?;
            }
            TraceEvent::EventQueued {
                event,
                sender,
                receiver,
                payload,
            } => {
                writeln!(
                    w,
                    "{}{}queue {} {} -> {} ({})",
                    prefix, indent, event, sender, receiver, payload
                )?;
            }
            TraceEvent::EventDelivered {
                event,
                sender,
                receiver,
                payload,
            } => {
                writeln!(
                    w,
                    "{}{}event {} {} -> {} ({})",
                    prefix, indent, event, sender, receiver, payload
                )?;
            }
            TraceEvent::SmCreated { node, id } => {
                writeln!(w, "{}{}create {}({})", prefix, indent, node, id)?;
            }
            TraceEvent::SmInitialized { node, id } => {
                writeln!(w, "{}{}initialize {}({})", prefix, indent, node, id)?;
            }
            TraceEvent::SmDestroyed { node, id } => {
                writeln!(w, "{}{}destroy {}({})", prefix, indent, node, id)?;
            }
            TraceEvent::PortCreated { node, id } => {
                writeln!(w, "{}{}create port {}({})", prefix, indent, node, id)?;
            }
            TraceEvent::PortDestroyed { node, id } => {
                writeln!(w, "{}{}destroy port {}({})", prefix, indent, node, id)?;
            }
            TraceEvent::InvariantViolation {
                node,
                id,
                signal,
                value,
                invariant_expr,
            } => {
                writeln!(
                    w,
                    "{}{}INVARIANT VIOLATED {}({})::{}:  {} (expected: {})",
                    prefix, indent, node, id, signal, value, invariant_expr
                )?;
            }
        }
    }
    Ok(())
}
