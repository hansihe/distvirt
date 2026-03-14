//! # Propagation Tracing
//!
//! Observability for the signal router. The [`Tracer`] trait receives callbacks
//! at every decision point during propagation — signal changes, edge changes,
//! input deliveries, event routing, SM lifecycle. All methods have default
//! empty implementations so you only override what you care about.
//!
//! The generated `Router` is generic: `Router<T: Tracer = NoopTracer>`.
//! `NoopTracer` methods are all empty and monomorphize away, so untraced
//! routers pay zero cost. To trace, construct with a concrete tracer:
//!
//! ```rust,ignore
//! use distvirt_sm_router::trace::RecordingTracer;
//!
//! let mut router = Router::new_traced(16, RecordingTracer::new());
//! // ... setup and operations ...
//! router.propagate();
//! println!("{}", router.tracer());
//! ```
//!
//! ## Built-in tracers
//!
//! - [`NoopTracer`]: Does nothing. Default. Zero-cost when monomorphized.
//! - [`RecordingTracer`]: Captures all events into a `Vec<TraceEntry>`.
//!   Implements `Display` for human-readable indented output showing causality.
//!
//! ## Composition
//!
//! Tracers compose naturally: wrap an inner tracer to filter, transform, or
//! multiplex trace events. This is more flexible than configuration flags.
//!
//! ```rust,ignore
//! /// Only passes events for a specific node type to the inner tracer.
//! struct NodeFilter<T: Tracer> {
//!     node: &'static str,
//!     inner: T,
//! }
//!
//! impl<T: Tracer> Tracer for NodeFilter<T> {
//!     fn input_delivered(&mut self, node: &'static str, id: &dyn Debug,
//!                        input: &'static str, value: &dyn Debug) {
//!         if node == self.node {
//!             self.inner.input_delivered(node, id, input, value);
//!         }
//!     }
//!     // ... delegate other methods with same filter ...
//! }
//! ```
//!
//! ## Extension ideas
//!
//! The `Tracer` trait is the base facility. Some downstream uses it enables:
//!
//! - **Selective/filtered tracing**: Compose a filter tracer around a recording
//!   tracer to capture only events for specific SM types, signals, or ID values.
//!   Only pays formatting cost for matching events.
//!
//! - **Causality analysis**: The `EffectsStart`/`EffectsEnd` bracketing in the
//!   trace lets you build a causality DAG post-hoc: "this signal change was an
//!   effect of this handler invocation, which was caused by this input delivery,
//!   which was triggered by this earlier signal change." Walk backwards from any
//!   unexpected state to find its root cause.
//!
//! - **State diffing**: Signal/edge change entries are exactly the diff between
//!   pre- and post-propagate states. A utility could format a compact summary:
//!   "this propagate() changed 3 signals, added 2 edges, created 1 SM."
//!
//! - **Performance profiling**: Count rounds, deliveries, suppressions per SM
//!   type to find hot spots or excessive cascade depth.
//!
//! - **Invariant checking**: A tracer that asserts on `round_end` or inspects
//!   the trace after `propagate_end` to verify domain invariants. For full
//!   router state access during propagation, a separate `propagate_checked()`
//!   mechanism (closure called after each round with `&Router`) is cleaner
//!   since the tracer can't borrow the router.
//!
//! - **Production structured logging**: A tracer that writes to tracing/log
//!   crate, filtering by severity (e.g., only lifecycle events and errors).
//!
//! - **Auto-dump on test failure**: A `PanicTracer` that prints the trace in
//!   its `Drop` impl when `std::thread::panicking()` is true.

use std::fmt::{self, Debug};

/// Receives callbacks at every decision point during signal propagation.
///
/// All IDs and values are passed as `&dyn Debug` to keep the trait
/// topology-independent. The `&'static str` arguments (node, signal, edge,
/// input, event names) are baked in by the macro at code generation time.
///
/// All methods have default no-op implementations. Override only what you need.
///
/// ## Ordering guarantees
///
/// Within a `propagate()` call, events arrive in this order:
///
/// ```text
/// propagate_start
/// ├─ round_start(1)
/// │  ├─ input_delivered / input_suppressed  (one per dirty input)
/// │  │  ├─ effects_start
/// │  │  │  ├─ sm_created          (from handler's ctx.create_*())
/// │  │  │  ├─ signal_changed      (from handler's ctx.set_*())
/// │  │  │  ├─ edge_changed        (from handler's ctx.set_*_edges())
/// │  │  │  ├─ event_queued        (from handler's ctx.send_*())
/// │  │  │  └─ sm_destroyed        (from handler's ctx.self_destruct())
/// │  │  └─ effects_end
/// │  ├─ event_delivered             (one per pending event)
/// │  │  └─ effects_start/end        (same pattern)
/// │  └─ round_end(1)
/// ├─ round_start(2)
/// │  └─ ...                         (cascading changes)
/// └─ propagate_end(total_rounds)
/// ```
#[allow(unused_variables)]
pub trait Tracer {
    /// Called when `propagate()` begins.
    fn propagate_start(&mut self) {}

    /// Called when `propagate()` completes. `rounds` is the total number of
    /// rounds executed (0 if nothing was dirty).
    fn propagate_end(&mut self, rounds: usize) {}

    /// A new propagation round begins. `depth` starts at 1.
    fn round_start(&mut self, depth: usize) {}

    /// A propagation round ends.
    fn round_end(&mut self, depth: usize) {}

    // -- Input processing --

    /// An aggregated input was re-computed and delivered to the SM handler
    /// because the aggregated value changed.
    fn input_delivered(
        &mut self,
        node: &'static str,
        id: &dyn Debug,
        input: &'static str,
        value: &dyn Debug,
    ) {
    }

    /// An aggregated input was re-computed but the value was unchanged
    /// (PartialEq matched last delivery), so the handler was NOT called.
    fn input_suppressed(&mut self, node: &'static str, id: &dyn Debug, input: &'static str) {}

    // -- Handler effects bracketing --

    /// A handler has been invoked; the following signal/edge/lifecycle events
    /// are effects of this handler until the matching `effects_end`.
    fn effects_start(&mut self, node: &'static str, id: &dyn Debug) {}

    /// Effects from the preceding handler have been fully applied.
    fn effects_end(&mut self, node: &'static str, id: &dyn Debug) {}

    // -- Signal changes --

    /// A signal value changed. `old` is the previous value (or `None` if this
    /// is the first non-default value). Emitted from signal setters, which run
    /// during effect application or from external API calls.
    fn signal_changed(
        &mut self,
        node: &'static str,
        id: &dyn Debug,
        signal: &'static str,
        old: &dyn Debug,
        new: &dyn Debug,
    ) {
    }

    // -- Edge changes --

    /// An edge set was modified. `added` and `removed` are the target IDs that
    /// changed.
    fn edge_changed(
        &mut self,
        edge: &'static str,
        source: &dyn Debug,
        added: &dyn Debug,
        removed: &dyn Debug,
    ) {
    }

    // -- Events --

    /// An event was queued for delivery (will be delivered later in this round).
    fn event_queued(
        &mut self,
        event: &'static str,
        sender: &dyn Debug,
        receiver: &dyn Debug,
        payload: &dyn Debug,
    ) {
    }

    /// An event was delivered to its target SM handler.
    fn event_delivered(
        &mut self,
        event: &'static str,
        sender: &dyn Debug,
        receiver: &dyn Debug,
        payload: &dyn Debug,
    ) {
    }

    // -- Lifecycle --

    /// A new SM instance was created.
    fn sm_created(&mut self, node: &'static str, id: &dyn Debug) {}

    /// An SM instance was destroyed (via self_destruct or external destroy).
    fn sm_destroyed(&mut self, node: &'static str, id: &dyn Debug) {}

    /// A new port instance was created.
    fn port_created(&mut self, node: &'static str, id: &dyn Debug) {}

    /// A port instance was destroyed.
    fn port_destroyed(&mut self, node: &'static str, id: &dyn Debug) {}
}

// ============================================================================
// NoopTracer — zero-cost default
// ============================================================================

/// A tracer that does nothing. All methods are empty no-ops that the compiler
/// eliminates entirely when monomorphized.
///
/// This is the default tracer for `Router<NoopTracer>`, which is what you get
/// from `Router::new(depth_limit)`.
pub struct NoopTracer;

impl Tracer for NoopTracer {}

// ============================================================================
// TraceEntry — stringified trace record for RecordingTracer
// ============================================================================

/// A single trace event with all values formatted as strings.
///
/// Captured by [`RecordingTracer`]. The `&'static str` fields are node/signal/
/// edge/input/event names baked in by the macro.
#[derive(Debug, Clone)]
pub enum TraceEntry {
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
        id: String,
        input: &'static str,
        value: String,
    },
    InputSuppressed {
        node: &'static str,
        id: String,
        input: &'static str,
    },

    EffectsStart {
        node: &'static str,
        id: String,
    },
    EffectsEnd {
        node: &'static str,
        id: String,
    },

    SignalChanged {
        node: &'static str,
        id: String,
        signal: &'static str,
        old: String,
        new: String,
    },
    EdgeChanged {
        edge: &'static str,
        source: String,
        added: String,
        removed: String,
    },

    EventQueued {
        event: &'static str,
        sender: String,
        receiver: String,
        payload: String,
    },
    EventDelivered {
        event: &'static str,
        sender: String,
        receiver: String,
        payload: String,
    },

    SmCreated {
        node: &'static str,
        id: String,
    },
    SmDestroyed {
        node: &'static str,
        id: String,
    },
    PortCreated {
        node: &'static str,
        id: String,
    },
    PortDestroyed {
        node: &'static str,
        id: String,
    },
}

// ============================================================================
// RecordingTracer
// ============================================================================

/// Captures all trace events into a `Vec<TraceEntry>`.
///
/// Use `Display` to get a human-readable indented trace showing causality
/// (effects indented under the handler invocation that caused them).
pub struct RecordingTracer {
    entries: Vec<TraceEntry>,
}

impl RecordingTracer {
    pub fn new() -> Self {
        RecordingTracer {
            entries: Vec::new(),
        }
    }

    /// Access the captured trace entries.
    pub fn entries(&self) -> &[TraceEntry] {
        &self.entries
    }

    /// Consume and return the entries.
    pub fn into_entries(self) -> Vec<TraceEntry> {
        self.entries
    }

    /// Clear all recorded entries.
    pub fn clear(&mut self) {
        self.entries.clear();
    }
}

impl Default for RecordingTracer {
    fn default() -> Self {
        Self::new()
    }
}

impl Tracer for RecordingTracer {
    fn propagate_start(&mut self) {
        self.entries.push(TraceEntry::PropagateStart);
    }

    fn propagate_end(&mut self, rounds: usize) {
        self.entries.push(TraceEntry::PropagateEnd { rounds });
    }

    fn round_start(&mut self, depth: usize) {
        self.entries.push(TraceEntry::RoundStart { depth });
    }

    fn round_end(&mut self, depth: usize) {
        self.entries.push(TraceEntry::RoundEnd { depth });
    }

    fn input_delivered(
        &mut self,
        node: &'static str,
        id: &dyn Debug,
        input: &'static str,
        value: &dyn Debug,
    ) {
        self.entries.push(TraceEntry::InputDelivered {
            node,
            id: format!("{:?}", id),
            input,
            value: format!("{:?}", value),
        });
    }

    fn input_suppressed(&mut self, node: &'static str, id: &dyn Debug, input: &'static str) {
        self.entries.push(TraceEntry::InputSuppressed {
            node,
            id: format!("{:?}", id),
            input,
        });
    }

    fn effects_start(&mut self, node: &'static str, id: &dyn Debug) {
        self.entries.push(TraceEntry::EffectsStart {
            node,
            id: format!("{:?}", id),
        });
    }

    fn effects_end(&mut self, node: &'static str, id: &dyn Debug) {
        self.entries.push(TraceEntry::EffectsEnd {
            node,
            id: format!("{:?}", id),
        });
    }

    fn signal_changed(
        &mut self,
        node: &'static str,
        id: &dyn Debug,
        signal: &'static str,
        old: &dyn Debug,
        new: &dyn Debug,
    ) {
        self.entries.push(TraceEntry::SignalChanged {
            node,
            id: format!("{:?}", id),
            signal,
            old: format!("{:?}", old),
            new: format!("{:?}", new),
        });
    }

    fn edge_changed(
        &mut self,
        edge: &'static str,
        source: &dyn Debug,
        added: &dyn Debug,
        removed: &dyn Debug,
    ) {
        self.entries.push(TraceEntry::EdgeChanged {
            edge,
            source: format!("{:?}", source),
            added: format!("{:?}", added),
            removed: format!("{:?}", removed),
        });
    }

    fn event_queued(
        &mut self,
        event: &'static str,
        sender: &dyn Debug,
        receiver: &dyn Debug,
        payload: &dyn Debug,
    ) {
        self.entries.push(TraceEntry::EventQueued {
            event,
            sender: format!("{:?}", sender),
            receiver: format!("{:?}", receiver),
            payload: format!("{:?}", payload),
        });
    }

    fn event_delivered(
        &mut self,
        event: &'static str,
        sender: &dyn Debug,
        receiver: &dyn Debug,
        payload: &dyn Debug,
    ) {
        self.entries.push(TraceEntry::EventDelivered {
            event,
            sender: format!("{:?}", sender),
            receiver: format!("{:?}", receiver),
            payload: format!("{:?}", payload),
        });
    }

    fn sm_created(&mut self, node: &'static str, id: &dyn Debug) {
        self.entries.push(TraceEntry::SmCreated {
            node,
            id: format!("{:?}", id),
        });
    }

    fn sm_destroyed(&mut self, node: &'static str, id: &dyn Debug) {
        self.entries.push(TraceEntry::SmDestroyed {
            node,
            id: format!("{:?}", id),
        });
    }

    fn port_created(&mut self, node: &'static str, id: &dyn Debug) {
        self.entries.push(TraceEntry::PortCreated {
            node,
            id: format!("{:?}", id),
        });
    }

    fn port_destroyed(&mut self, node: &'static str, id: &dyn Debug) {
        self.entries.push(TraceEntry::PortDestroyed {
            node,
            id: format!("{:?}", id),
        });
    }
}

// ============================================================================
// Display — human-readable indented trace output
// ============================================================================

impl fmt::Display for RecordingTracer {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt_trace_entries(&self.entries, f)
    }
}

/// Format a slice of trace entries as indented human-readable text.
///
/// Effects (signal changes, edge changes, creates, destroys) are indented
/// under the handler invocation that caused them. This makes causality
/// immediately visible.
pub fn fmt_trace_entries(entries: &[TraceEntry], f: &mut fmt::Formatter<'_>) -> fmt::Result {
    let mut in_effects = false;

    for entry in entries {
        match entry {
            TraceEntry::PropagateStart => {
                writeln!(f, "=== propagate ===")?;
            }
            TraceEntry::PropagateEnd { rounds } => {
                writeln!(f, "=== complete ({} rounds) ===", rounds)?;
            }
            TraceEntry::RoundStart { depth } => {
                writeln!(f, "  Round {}:", depth)?;
            }
            TraceEntry::RoundEnd { .. } => {}
            TraceEntry::InputDelivered {
                node,
                id,
                input,
                value,
            } => {
                writeln!(f, "    deliver {}({}) <- {}({})", node, id, input, value)?;
            }
            TraceEntry::InputSuppressed { node, id, input } => {
                writeln!(f, "    suppress {}({}) <- {} (unchanged)", node, id, input)?;
            }
            TraceEntry::EffectsStart { .. } => {
                in_effects = true;
            }
            TraceEntry::EffectsEnd { .. } => {
                in_effects = false;
            }
            TraceEntry::SignalChanged {
                node,
                id,
                signal,
                old,
                new,
            } => {
                let indent = if in_effects { "      " } else { "    " };
                writeln!(
                    f,
                    "{}signal {}({})::{}:  {} -> {}",
                    indent, node, id, signal, old, new
                )?;
            }
            TraceEntry::EdgeChanged {
                edge,
                source,
                added,
                removed,
            } => {
                let indent = if in_effects { "      " } else { "    " };
                writeln!(
                    f,
                    "{}edge {}: {} (+{} -{})",
                    indent, edge, source, added, removed
                )?;
            }
            TraceEntry::EventQueued {
                event,
                sender,
                receiver,
                payload,
            } => {
                let indent = if in_effects { "      " } else { "    " };
                writeln!(
                    f,
                    "{}queue {} {} -> {} ({})",
                    indent, event, sender, receiver, payload
                )?;
            }
            TraceEntry::EventDelivered {
                event,
                sender,
                receiver,
                payload,
            } => {
                writeln!(
                    f,
                    "    event {} {} -> {} ({})",
                    event, sender, receiver, payload
                )?;
            }
            TraceEntry::SmCreated { node, id } => {
                let indent = if in_effects { "      " } else { "    " };
                writeln!(f, "{}create {}({})", indent, node, id)?;
            }
            TraceEntry::SmDestroyed { node, id } => {
                let indent = if in_effects { "      " } else { "    " };
                writeln!(f, "{}destroy {}({})", indent, node, id)?;
            }
            TraceEntry::PortCreated { node, id } => {
                let indent = if in_effects { "      " } else { "    " };
                writeln!(f, "{}create port {}({})", indent, node, id)?;
            }
            TraceEntry::PortDestroyed { node, id } => {
                let indent = if in_effects { "      " } else { "    " };
                writeln!(f, "{}destroy port {}({})", indent, node, id)?;
            }
        }
    }
    Ok(())
}
