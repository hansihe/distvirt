use crate::parse::*;
use proc_macro2::TokenStream;
use quote::{format_ident, quote};

use super::helpers::*;

pub(super) fn gen_signal_setters(
    def: &TopologyDef,
    public_methods: &mut Vec<TokenStream>,
    internal_methods: &mut Vec<TokenStream>,
) {
    for sig in &def.signals {
        let method = format_ident!(
            "set_{}_{}",
            to_snake_case(&sig.node.to_string()),
            to_snake_case(&sig.signal.to_string())
        );
        let id_type = node_id_type(def, &sig.node);
        let vt = &sig.value_type;

        // Find which inputs consume this signal and through which edges
        let enqueue_code: Vec<_> = def
            .inputs
            .iter()
            .flat_map(|inp| {
                inp.sources
                    .iter()
                    .filter(|sp| sp.node == sig.node && sp.signal == sig.signal)
                    .map(move |sp| {
                        let edge_field = format_ident!("{}", to_snake_case(&sp.edge.to_string()));
                        let dv = dirty_variant(inp);
                        quote! {
                            if let Some(targets) = self.#edge_field.targets(&id) {
                                for &target_id in targets {
                                    self.dirty.push_back(DirtyInput::#dv(target_id));
                                }
                            }
                        }
                    })
            })
            .collect();

        let node_str = sig.node.to_string();
        let signal_str = sig.signal.to_string();

        let node_state = signal_state_field(&sig.node);
        let out_field = out_field_name(&sig.signal);

        let state_struct = signal_state_struct_name(&sig.node);

        let body = quote! {
            fn #method(&mut self, id: #id_type, value: #vt) {
                if let Some(state) = self.#node_state.get(&id) {
                    if state.#out_field == value {
                        return;
                    }
                    self.tracer.trace(::distvirt_sm_router::trace::TraceEvent::SignalChanged {
                        node: #node_str,
                        id: ::distvirt_sm_router::trace::DebugValue::Borrowed(&id as &(dyn std::fmt::Debug + Send + Sync)),
                        signal: #signal_str,
                        old: ::distvirt_sm_router::trace::DebugValue::Borrowed(&state.#out_field as &(dyn std::fmt::Debug + Send + Sync)),
                        new: ::distvirt_sm_router::trace::DebugValue::Borrowed(&value as &(dyn std::fmt::Debug + Send + Sync)),
                    });
                } else {
                    self.tracer.trace(::distvirt_sm_router::trace::TraceEvent::SignalChanged {
                        node: #node_str,
                        id: ::distvirt_sm_router::trace::DebugValue::Borrowed(&id as &(dyn std::fmt::Debug + Send + Sync)),
                        signal: #signal_str,
                        old: ::distvirt_sm_router::trace::DebugValue::Borrowed(&None::<()> as &(dyn std::fmt::Debug + Send + Sync)),
                        new: ::distvirt_sm_router::trace::DebugValue::Borrowed(&value as &(dyn std::fmt::Debug + Send + Sync)),
                    });
                }
                self.#node_state.entry(id).or_insert_with(#state_struct::default).#out_field = value;
                #(#enqueue_code)*
            }
        };

        // Port signal setters are public, SM signal setters are internal
        if is_sm_node(def, &sig.node) {
            internal_methods.push(body);
        } else {
            public_methods.push(body);
        }
    }
}

pub(super) fn gen_edge_setters(
    def: &TopologyDef,
    public_methods: &mut Vec<TokenStream>,
    internal_methods: &mut Vec<TokenStream>,
) {
    for edge in &def.edges {
        let method = format_ident!("set_{}_edges", edge_snake(edge));
        let edge_field = format_ident!("{}", edge_snake(edge));
        let src_id = node_id_type(def, &edge.source);
        let tgt_id = node_id_type(def, &edge.target);

        // Find all inputs that use this edge
        let dirty_enqueues: Vec<_> = def
            .inputs
            .iter()
            .filter(|inp| inp.sources.iter().any(|sp| sp.edge == edge.name))
            .map(|inp| {
                let dv = dirty_variant(inp);
                quote! { self.dirty.push_back(DirtyInput::#dv(*tgt)); }
            })
            .collect();

        let edge_str = edge.name.to_string();

        let body = quote! {
            fn #method(&mut self, source: #src_id, new_targets: impl IntoIterator<Item = #tgt_id>) {
                if let Some(diff) = self.#edge_field.set_edges(source, new_targets) {
                    self.tracer.trace(::distvirt_sm_router::trace::TraceEvent::EdgeChanged {
                        edge: #edge_str,
                        source: ::distvirt_sm_router::trace::DebugValue::Borrowed(&source as &(dyn std::fmt::Debug + Send + Sync)),
                        added: ::distvirt_sm_router::trace::DebugValue::Borrowed(&diff.added as &(dyn std::fmt::Debug + Send + Sync)),
                        removed: ::distvirt_sm_router::trace::DebugValue::Borrowed(&diff.removed as &(dyn std::fmt::Debug + Send + Sync)),
                    });
                    for tgt in diff.all_changed() {
                        #(#dirty_enqueues)*
                    }
                }
            }
        };

        // Port-sourced edge setters are public, SM-sourced are internal
        if is_sm_node(def, &edge.source) {
            internal_methods.push(body);
        } else {
            public_methods.push(body);
        }
    }
}

/// Generate public event send methods for port-sourced events.
pub(super) fn gen_event_send_methods(def: &TopologyDef, methods: &mut Vec<TokenStream>) {
    for ev in &def.events {
        // Only ports get public send methods on Router.
        // SM-sourced events are sent via Ctx.
        if is_sm_node(def, &ev.sender) {
            continue;
        }

        let method = format_ident!("send_{}", to_snake_case(&ev.name.to_string()));
        let sender_id = node_id_type(def, &ev.sender);
        let receiver_id = node_id_type(def, &ev.receiver);
        let payload = &ev.payload_type;
        let variant = &ev.name;
        let event_str = ev.name.to_string();

        methods.push(quote! {
            fn #method(&mut self, sender_id: #sender_id, receiver_id: #receiver_id, payload: #payload) {
                self.tracer.trace(::distvirt_sm_router::trace::TraceEvent::EventQueued {
                    event: #event_str,
                    sender: ::distvirt_sm_router::trace::DebugValue::Borrowed(&sender_id as &(dyn std::fmt::Debug + Send + Sync)),
                    receiver: ::distvirt_sm_router::trace::DebugValue::Borrowed(&receiver_id as &(dyn std::fmt::Debug + Send + Sync)),
                    payload: ::distvirt_sm_router::trace::DebugValue::Borrowed(&payload as &(dyn std::fmt::Debug + Send + Sync)),
                });
                self.pending_events.push_back(PendingEvent::#variant(sender_id, receiver_id, payload));
            }
        });
    }
}
