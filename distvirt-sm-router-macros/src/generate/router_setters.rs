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
        let field = signal_field(sig);
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
                        let edge_fwd = format_ident!("{}_fwd", to_snake_case(&sp.edge.to_string()));
                        let dv = dirty_variant(inp);
                        quote! {
                            if let Some(targets) = self.#edge_fwd.get(&id) {
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

        let body = quote! {
            fn #method(&mut self, id: #id_type, value: #vt) {
                if self.#field.get(&id) == Some(&value) {
                    return;
                }
                self.tracer.trace(::distvirt_sm_router::trace::TraceEvent::SignalChanged {
                    node: #node_str,
                    id: ::distvirt_sm_router::trace::DebugValue::Borrowed(&id as &dyn std::fmt::Debug),
                    signal: #signal_str,
                    old: ::distvirt_sm_router::trace::DebugValue::Borrowed(&self.#field.get(&id) as &dyn std::fmt::Debug),
                    new: ::distvirt_sm_router::trace::DebugValue::Borrowed(&value as &dyn std::fmt::Debug),
                });
                self.#field.insert(id, value);
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
        let fwd = format_ident!("{}_fwd", edge_snake(edge));
        let rev = format_ident!("{}_rev", edge_snake(edge));
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
                let new_targets: Vec<#tgt_id> = new_targets.into_iter().collect();
                let old_set: std::collections::BTreeSet<#tgt_id> = self.#fwd
                    .get(&source)
                    .map(|v| v.iter().copied().collect())
                    .unwrap_or_default();
                let new_set: std::collections::BTreeSet<#tgt_id> = new_targets.iter().copied().collect();

                let removed: Vec<#tgt_id> = old_set.difference(&new_set).copied().collect();
                let added: Vec<#tgt_id> = new_set.difference(&old_set).copied().collect();

                if removed.is_empty() && added.is_empty() {
                    return;
                }

                self.tracer.trace(::distvirt_sm_router::trace::TraceEvent::EdgeChanged {
                    edge: #edge_str,
                    source: ::distvirt_sm_router::trace::DebugValue::Borrowed(&source as &dyn std::fmt::Debug),
                    added: ::distvirt_sm_router::trace::DebugValue::Borrowed(&added as &dyn std::fmt::Debug),
                    removed: ::distvirt_sm_router::trace::DebugValue::Borrowed(&removed as &dyn std::fmt::Debug),
                });

                if new_targets.is_empty() {
                    self.#fwd.remove(&source);
                } else {
                    self.#fwd.insert(source, new_targets);
                }

                for &tgt in &removed {
                    if let Some(sources) = self.#rev.get_mut(&tgt) {
                        sources.remove(&source);
                        if sources.is_empty() {
                            self.#rev.remove(&tgt);
                        }
                    }
                }
                for &tgt in &added {
                    self.#rev.entry(tgt).or_default().insert(source);
                }

                for tgt in removed.iter().chain(added.iter()) {
                    #(#dirty_enqueues)*
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
                    sender: ::distvirt_sm_router::trace::DebugValue::Borrowed(&sender_id as &dyn std::fmt::Debug),
                    receiver: ::distvirt_sm_router::trace::DebugValue::Borrowed(&receiver_id as &dyn std::fmt::Debug),
                    payload: ::distvirt_sm_router::trace::DebugValue::Borrowed(&payload as &dyn std::fmt::Debug),
                });
                self.pending_events.push_back(PendingEvent::#variant(sender_id, receiver_id, payload));
            }
        });
    }
}
