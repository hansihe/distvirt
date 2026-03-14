use crate::parse::*;
use proc_macro2::TokenStream;
use quote::{format_ident, quote, ToTokens};

use super::helpers::*;

pub(super) fn gen_router_module(def: &TopologyDef) -> TokenStream {
    let expose = def.expose_internals;

    // Determine field visibility
    let field_vis: TokenStream = if expose {
        quote! { pub }
    } else {
        quote! {}
    };

    // Build struct fields and new() initializers
    let mut fields = Vec::new();
    let mut inits = Vec::new();

    // SM instances
    for sm in &def.state_machines {
        let f = format_ident!("{}_instances", snake_ident(&sm.name.to_string()));
        let id = sm_id_type(sm);
        let handler = &sm.handler_type;
        let vis = &field_vis;
        fields.push(quote! { #vis #f: std::collections::BTreeMap<#id, #handler> });
        inits.push(quote! { #f: std::collections::BTreeMap::new() });
    }

    // Port instances
    for port in &def.ports {
        let f = format_ident!("{}_instances", snake_ident(&port.name.to_string()));
        let id = port_id_type(port);
        let vis = &field_vis;
        fields.push(quote! { #vis #f: std::collections::BTreeSet<#id> });
        inits.push(quote! { #f: std::collections::BTreeSet::new() });
    }

    // Signals
    for sig in &def.signals {
        let f = signal_field(sig);
        let id = node_id_type(def, &sig.node);
        let vt = &sig.value_type;
        let vis = &field_vis;
        fields.push(quote! { #vis #f: std::collections::BTreeMap<#id, #vt> });
        inits.push(quote! { #f: std::collections::BTreeMap::new() });
    }

    // Edges (fwd + rev)
    for edge in &def.edges {
        let snake = edge_snake(edge);
        let fwd = format_ident!("{}_fwd", snake);
        let rev = format_ident!("{}_rev", snake);
        let src_id = node_id_type(def, &edge.source);
        let tgt_id = node_id_type(def, &edge.target);
        let vis = &field_vis;
        fields.push(quote! { #vis #fwd: std::collections::BTreeMap<#src_id, Vec<#tgt_id>> });
        fields.push(quote! { #vis #rev: std::collections::BTreeMap<#tgt_id, std::collections::BTreeSet<#src_id>> });
        inits.push(quote! { #fwd: std::collections::BTreeMap::new() });
        inits.push(quote! { #rev: std::collections::BTreeMap::new() });
    }

    // Last delivered
    for inp in &def.inputs {
        let f = last_field(inp);
        let id = node_id_type(def, &inp.node);
        let agg = &inp.aggregator;
        let vis = &field_vis;
        fields.push(
            quote! { #vis #f: std::collections::BTreeMap<#id, <#agg as crate::Aggregator>::Output> },
        );
        inits.push(quote! { #f: std::collections::BTreeMap::new() });
    }

    // Auto-ID counters
    for sm in &def.state_machines {
        if sm.id_type.is_none() {
            let counter = format_ident!("next_{}_id", to_snake_case(&sm.name.to_string()));
            fields.push(quote! { #counter: u64 });
            inits.push(quote! { #counter: 0 });
        }
    }
    for port in &def.ports {
        if port.id_type.is_none() {
            let counter = format_ident!("next_{}_id", to_snake_case(&port.name.to_string()));
            fields.push(quote! { #counter: u64 });
            inits.push(quote! { #counter: 0 });
        }
    }

    // Port input output queues
    for port in &def.ports {
        let has_inputs = def.inputs.iter().any(|inp| inp.node == port.name);
        if has_inputs {
            let f = format_ident!("{}_pending_inputs", to_snake_case(&port.name.to_string()));
            let id = port_id_type(port);
            let port_input_enum = format_ident!("{}PortInput", port.name);
            let vis = &field_vis;
            fields.push(quote! { #vis #f: Vec<(#id, #port_input_enum)> });
            inits.push(quote! { #f: Vec::new() });
        }
    }

    // Dirty queue, pending events, depth limit, tracer
    fields.push(quote! { dirty: std::collections::VecDeque<DirtyInput> });
    fields.push(quote! { pending_events: std::collections::VecDeque<PendingEvent> });
    fields.push(quote! { depth_limit: usize });
    fields.push(quote! { tracer: __Tracer });
    inits.push(quote! { dirty: std::collections::VecDeque::new() });
    inits.push(quote! { pending_events: std::collections::VecDeque::new() });
    inits.push(quote! { depth_limit });
    // tracer init is handled separately for each constructor

    // Collect methods by visibility category
    let mut public_methods = Vec::new();
    let mut internal_methods = Vec::new();

    // Tracer accessor
    public_methods.push(quote! {
        fn tracer(&self) -> &__Tracer {
            &self.tracer
        }
    });
    public_methods.push(quote! {
        fn tracer_mut(&mut self) -> &mut __Tracer {
            &mut self.tracer
        }
    });

    // Always public: SM lifecycle, port lifecycle, propagate, SM accessors
    gen_create_methods(def, &mut public_methods);
    gen_destroy_methods(def, &mut internal_methods);
    gen_remove_methods(def, &mut public_methods);
    gen_sm_accessors(def, &mut public_methods);
    gen_propagate(def, &mut public_methods);

    // Always public: port input drain methods
    gen_drain_port_inputs(def, &mut public_methods);

    // Always public: port signal/edge setters, port event senders
    // Internal: SM signal/edge setters
    gen_signal_setters(def, &mut public_methods, &mut internal_methods);
    gen_edge_setters(def, &mut public_methods, &mut internal_methods);
    gen_event_send_methods(def, &mut public_methods);

    // Always internal: aggregate, apply_effects
    gen_aggregate_methods(def, &mut internal_methods);
    gen_apply_effects(def, &mut internal_methods);

    // If expose_internals, internal methods become pub too
    let internal_vis: TokenStream = if expose {
        quote! { pub }
    } else {
        quote! {}
    };

    quote! {
        #[allow(dead_code)]
        mod __router {
            use super::*;

            pub struct Router<__Tracer: crate::trace::Tracer = crate::trace::NoopTracer> {
                #(#fields,)*
            }

            #[allow(dead_code)]
            impl Router<crate::trace::NoopTracer> {
                pub fn new(depth_limit: usize) -> Self {
                    Router {
                        #(#inits,)*
                        tracer: crate::trace::NoopTracer,
                    }
                }
            }

            #[allow(dead_code)]
            impl<__Tracer: crate::trace::Tracer> Router<__Tracer> {
                pub fn new_traced(depth_limit: usize, tracer: __Tracer) -> Self {
                    Router {
                        #(#inits,)*
                        tracer,
                    }
                }

                #(pub #public_methods)*
                #(#internal_vis #internal_methods)*
            }
        }

        #[allow(unused_imports)]
        use __router::Router;
    }
}

fn gen_create_methods(def: &TopologyDef, methods: &mut Vec<TokenStream>) {
    // SM creation
    for sm in &def.state_machines {
        let method = format_ident!("create_{}", to_snake_case(&sm.name.to_string()));
        let instances = format_ident!("{}_instances", to_snake_case(&sm.name.to_string()));
        let id_type = sm_id_type(sm);
        let handler = &sm.handler_type;
        let node_str = sm.name.to_string();

        let sig_inits: Vec<_> = def
            .signals
            .iter()
            .filter(|s| s.node == sm.name)
            .map(|sig| {
                let f = signal_field(sig);
                quote! { self.#f.insert(id, Default::default()); }
            })
            .collect();

        if sm.id_type.is_none() {
            // Auto-ID: generate ID internally, return it
            let id_name = format_ident!("{}Id", sm.name);
            let counter = format_ident!("next_{}_id", to_snake_case(&sm.name.to_string()));
            methods.push(quote! {
                fn #method(&mut self, sm: #handler) -> #id_type {
                    let id = #id_name(self.#counter);
                    self.#counter += 1;
                    self.tracer.trace(crate::trace::TraceEvent::SmCreated {
                        node: #node_str,
                        id: crate::trace::DebugValue::Borrowed(&id as &dyn std::fmt::Debug),
                    });
                    self.#instances.insert(id, sm);
                    #(#sig_inits)*
                    id
                }
            });
        } else {
            // User-provided ID
            methods.push(quote! {
                fn #method(&mut self, id: #id_type, sm: #handler) {
                    self.tracer.trace(crate::trace::TraceEvent::SmCreated {
                        node: #node_str,
                        id: crate::trace::DebugValue::Borrowed(&id as &dyn std::fmt::Debug),
                    });
                    self.#instances.insert(id, sm);
                    #(#sig_inits)*
                }
            });
        }
    }

    // Port creation
    for port in &def.ports {
        let method = format_ident!("create_{}", to_snake_case(&port.name.to_string()));
        let instances = format_ident!("{}_instances", to_snake_case(&port.name.to_string()));
        let id_type = port_id_type(port);
        let node_str = port.name.to_string();

        let sig_inits: Vec<_> = def
            .signals
            .iter()
            .filter(|s| s.node == port.name)
            .map(|sig| {
                let f = signal_field(sig);
                quote! { self.#f.insert(id, Default::default()); }
            })
            .collect();

        if port.id_type.is_none() {
            // Auto-ID: generate ID internally, return it
            let id_name = format_ident!("{}Id", port.name);
            let counter = format_ident!("next_{}_id", to_snake_case(&port.name.to_string()));
            methods.push(quote! {
                fn #method(&mut self) -> #id_type {
                    let id = #id_name(self.#counter);
                    self.#counter += 1;
                    self.tracer.trace(crate::trace::TraceEvent::PortCreated {
                        node: #node_str,
                        id: crate::trace::DebugValue::Borrowed(&id as &dyn std::fmt::Debug),
                    });
                    self.#instances.insert(id);
                    #(#sig_inits)*
                    id
                }
            });
        } else {
            // User-provided ID
            methods.push(quote! {
                fn #method(&mut self, id: #id_type) {
                    self.tracer.trace(crate::trace::TraceEvent::PortCreated {
                        node: #node_str,
                        id: crate::trace::DebugValue::Borrowed(&id as &dyn std::fmt::Debug),
                    });
                    self.#instances.insert(id);
                    #(#sig_inits)*
                }
            });
        }
    }
}

fn gen_destroy_methods(def: &TopologyDef, methods: &mut Vec<TokenStream>) {
    for sm in &def.state_machines {
        let method = format_ident!("destroy_{}", to_snake_case(&sm.name.to_string()));
        let instances = format_ident!("{}_instances", to_snake_case(&sm.name.to_string()));
        let id_type = sm_id_type(sm);
        let handler = &sm.handler_type;

        let sig_removes: Vec<_> = def
            .signals
            .iter()
            .filter(|s| s.node == sm.name)
            .map(|sig| {
                let f = signal_field(sig);
                quote! { self.#f.remove(&id); }
            })
            .collect();

        let last_removes: Vec<_> = def
            .inputs
            .iter()
            .filter(|inp| inp.node == sm.name)
            .map(|inp| {
                let f = last_field(inp);
                quote! { self.#f.remove(&id); }
            })
            .collect();

        let edge_clears: Vec<_> = def
            .edges
            .iter()
            .filter(|e| e.source == sm.name)
            .map(|edge| {
                let setter = format_ident!("set_{}_edges", edge_snake(edge));
                quote! { self.#setter(id, vec![]); }
            })
            .collect();

        // Note: sm_destroyed trace is emitted from apply_effects for
        // self-destruct, or NOT emitted here because destroy_ is called
        // from apply_effects. External destroy calls that bypass
        // apply_effects should add their own tracing if needed.
        methods.push(quote! {
            fn #method(&mut self, id: #id_type) -> Option<#handler> {
                let sm = self.#instances.remove(&id);
                #(#sig_removes)*
                #(#last_removes)*
                #(#edge_clears)*
                sm
            }
        });
    }
}

fn gen_remove_methods(def: &TopologyDef, methods: &mut Vec<TokenStream>) {
    for port in &def.ports {
        let method = format_ident!("destroy_{}", to_snake_case(&port.name.to_string()));
        let instances = format_ident!("{}_instances", to_snake_case(&port.name.to_string()));
        let id_type = port_id_type(port);
        let node_str = port.name.to_string();

        let sig_removes: Vec<_> = def
            .signals
            .iter()
            .filter(|s| s.node == port.name)
            .map(|sig| {
                let f = signal_field(sig);
                quote! { self.#f.remove(&id); }
            })
            .collect();

        let edge_clears: Vec<_> = def
            .edges
            .iter()
            .filter(|e| e.source == port.name)
            .map(|edge| {
                let setter = format_ident!("set_{}_edges", edge_snake(edge));
                quote! { self.#setter(id, vec![]); }
            })
            .collect();

        let incoming_edge_clears: Vec<_> = def
            .edges
            .iter()
            .filter(|e| e.target == port.name)
            .map(|edge| {
                let fwd = format_ident!("{}_fwd", edge_snake(edge));
                let rev = format_ident!("{}_rev", edge_snake(edge));
                quote! {
                    if let Some(sources) = self.#rev.remove(&id) {
                        for source_id in sources {
                            if let Some(targets) = self.#fwd.get_mut(&source_id) {
                                targets.retain(|t| *t != id);
                                if targets.is_empty() {
                                    self.#fwd.remove(&source_id);
                                }
                            }
                        }
                    }
                }
            })
            .collect();

        let last_removes: Vec<_> = def
            .inputs
            .iter()
            .filter(|inp| inp.node == port.name)
            .map(|inp| {
                let f = last_field(inp);
                quote! { self.#f.remove(&id); }
            })
            .collect();

        let queue_retains: Vec<_> = if def.inputs.iter().any(|inp| inp.node == port.name) {
            let pending_field = format_ident!("{}_pending_inputs", to_snake_case(&port.name.to_string()));
            vec![quote! { self.#pending_field.retain(|(pid, _)| *pid != id); }]
        } else {
            vec![]
        };

        methods.push(quote! {
            fn #method(&mut self, id: #id_type) {
                self.tracer.trace(crate::trace::TraceEvent::PortDestroyed {
                    node: #node_str,
                    id: crate::trace::DebugValue::Borrowed(&id as &dyn std::fmt::Debug),
                });
                self.#instances.remove(&id);
                #(#sig_removes)*
                #(#last_removes)*
                #(#edge_clears)*
                #(#incoming_edge_clears)*
                #(#queue_retains)*
            }
        });
    }
}

fn gen_drain_port_inputs(def: &TopologyDef, methods: &mut Vec<TokenStream>) {
    for port in &def.ports {
        let has_inputs = def.inputs.iter().any(|inp| inp.node == port.name);
        if !has_inputs {
            continue;
        }
        let method = format_ident!("drain_{}_inputs", to_snake_case(&port.name.to_string()));
        let field = format_ident!("{}_pending_inputs", to_snake_case(&port.name.to_string()));
        let id_type = port_id_type(port);
        let port_input_enum = format_ident!("{}PortInput", port.name);

        methods.push(quote! {
            fn #method(&mut self) -> Vec<(#id_type, #port_input_enum)> {
                std::mem::take(&mut self.#field)
            }
        });
    }
}

fn gen_sm_accessors(def: &TopologyDef, methods: &mut Vec<TokenStream>) {
    for sm in &def.state_machines {
        let get_method = format_ident!("get_{}", to_snake_case(&sm.name.to_string()));
        let instances = format_ident!("{}_instances", to_snake_case(&sm.name.to_string()));
        let id_type = sm_id_type(sm);
        let handler = &sm.handler_type;

        methods.push(quote! {
            fn #get_method(&self, id: &#id_type) -> Option<&#handler> {
                self.#instances.get(id)
            }
        });
    }
}

fn gen_signal_setters(
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
                self.tracer.trace(crate::trace::TraceEvent::SignalChanged {
                    node: #node_str,
                    id: crate::trace::DebugValue::Borrowed(&id as &dyn std::fmt::Debug),
                    signal: #signal_str,
                    old: crate::trace::DebugValue::Borrowed(&self.#field.get(&id) as &dyn std::fmt::Debug),
                    new: crate::trace::DebugValue::Borrowed(&value as &dyn std::fmt::Debug),
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

fn gen_edge_setters(
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

                self.tracer.trace(crate::trace::TraceEvent::EdgeChanged {
                    edge: #edge_str,
                    source: crate::trace::DebugValue::Borrowed(&source as &dyn std::fmt::Debug),
                    added: crate::trace::DebugValue::Borrowed(&added as &dyn std::fmt::Debug),
                    removed: crate::trace::DebugValue::Borrowed(&removed as &dyn std::fmt::Debug),
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

/// Generate connectivity check code for an event: returns a TokenStream that
/// evaluates to `bool` — true if any edge connects `sender_id` and `receiver_id`
/// in either direction.
fn gen_connectivity_check(def: &TopologyDef, ev: &EventDef) -> TokenStream {
    let checks: Vec<_> = def
        .edges
        .iter()
        .filter(|e| {
            (e.source == ev.sender && e.target == ev.receiver)
                || (e.source == ev.receiver && e.target == ev.sender)
        })
        .map(|e| {
            let fwd = format_ident!("{}_fwd", edge_snake(e));
            let rev = format_ident!("{}_rev", edge_snake(e));
            if e.source == ev.sender && e.target == ev.receiver {
                // sender is source: check fwd map
                quote! {
                    if let Some(targets) = self.#fwd.get(&sender_id) {
                        if targets.contains(&receiver_id) { return true; }
                    }
                }
            } else {
                // sender is target: check rev map
                quote! {
                    if let Some(sources) = self.#rev.get(&sender_id) {
                        if sources.contains(&receiver_id) { return true; }
                    }
                }
            }
        })
        .collect();

    // Wrap in a closure that we call immediately
    quote! {
        (|| -> bool {
            #(#checks)*
            false
        })()
    }
}

/// Generate public event send methods for port-sourced events.
fn gen_event_send_methods(def: &TopologyDef, methods: &mut Vec<TokenStream>) {
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
                self.tracer.trace(crate::trace::TraceEvent::EventQueued {
                    event: #event_str,
                    sender: crate::trace::DebugValue::Borrowed(&sender_id as &dyn std::fmt::Debug),
                    receiver: crate::trace::DebugValue::Borrowed(&receiver_id as &dyn std::fmt::Debug),
                    payload: crate::trace::DebugValue::Borrowed(&payload as &dyn std::fmt::Debug),
                });
                self.pending_events.push_back(PendingEvent::#variant(sender_id, receiver_id, payload));
            }
        });
    }
}

fn gen_aggregate_methods(def: &TopologyDef, methods: &mut Vec<TokenStream>) {
    for inp in &def.inputs {
        let method = format_ident!(
            "aggregate_{}_{}",
            to_snake_case(&inp.node.to_string()),
            to_snake_case(&inp.input_name.to_string())
        );
        let target_id = node_id_type(def, &inp.node);
        let agg = &inp.aggregator;
        let multi_source = inp.sources.len() >= 2;

        let collect_code: Vec<_> = inp
            .sources
            .iter()
            .map(|sp| {
                let rev = format_ident!("{}_rev", to_snake_case(&sp.edge.to_string()));
                let sig_field = format_ident!(
                    "{}_{}",
                    to_snake_case(&sp.node.to_string()),
                    to_snake_case(&sp.signal.to_string())
                );
                if multi_source {
                    let enum_name = format_ident!("{}Source", inp.input_name);
                    let variant_name = format_ident!("{}{}", sp.node, sp.signal);
                    quote! {
                        if let Some(sources) = self.#rev.get(&target_id) {
                            for &source_id in sources {
                                if let Some(value) = self.#sig_field.get(&source_id) {
                                    inputs.push(#enum_name::#variant_name(source_id, value.clone()));
                                }
                            }
                        }
                    }
                } else {
                    quote! {
                        if let Some(sources) = self.#rev.get(&target_id) {
                            for &source_id in sources {
                                if let Some(value) = self.#sig_field.get(&source_id) {
                                    inputs.push((source_id, value.clone()));
                                }
                            }
                        }
                    }
                }
            })
            .collect();

        let vec_type = if multi_source {
            let enum_name = format_ident!("{}Source", inp.input_name);
            quote! { Vec<#enum_name> }
        } else {
            quote! { Vec<_> }
        };

        methods.push(quote! {
            fn #method(&self, target_id: #target_id) -> <#agg as crate::Aggregator>::Output {
                let mut inputs: #vec_type = Vec::new();
                #(#collect_code)*
                <#agg as Default>::default().aggregate(&inputs)
            }
        });
    }
}

fn gen_apply_effects(def: &TopologyDef, methods: &mut Vec<TokenStream>) {
    for sm in &def.state_machines {
        let method = format_ident!("apply_{}_effects", to_snake_case(&sm.name.to_string()));
        let ctx_name = format_ident!("{}Ctx", sm.name);
        let id_type = sm_id_type(sm);
        let node_str = sm.name.to_string();

        // 1. Create new SMs (so they exist before edges reference them)
        let create_applies: Vec<_> = def
            .state_machines
            .iter()
            .map(|target_sm| {
                let target_snake = to_snake_case(&target_sm.name.to_string());
                let target_str = target_sm.name.to_string();
                let ctx_field = format_ident!("pending_create_{}", target_snake);
                let instances = format_ident!("{}_instances", target_snake);

                let sig_inits: Vec<_> = def
                    .signals
                    .iter()
                    .filter(|s| s.node == target_sm.name)
                    .map(|sig| {
                        let f = signal_field(sig);
                        quote! { self.#f.insert(new_id, Default::default()); }
                    })
                    .collect();

                quote! {
                    for (new_id, new_sm) in ctx.#ctx_field {
                        self.tracer.trace(crate::trace::TraceEvent::SmCreated {
                            node: #target_str,
                            id: crate::trace::DebugValue::Borrowed(&new_id as &dyn std::fmt::Debug),
                        });
                        self.#instances.insert(new_id, new_sm);
                        #(#sig_inits)*
                    }
                }
            })
            .collect();

        // 2. Update auto-ID counters
        let counter_updates: Vec<_> = def
            .state_machines
            .iter()
            .filter(|s| s.id_type.is_none())
            .map(|target_sm| {
                let target_snake = to_snake_case(&target_sm.name.to_string());
                let counter = format_ident!("next_{}_id", target_snake);
                quote! { self.#counter = ctx.#counter; }
            })
            .collect();

        // 3. Apply signals
        let signal_applies: Vec<_> = def
            .signals
            .iter()
            .filter(|s| s.node == sm.name)
            .map(|sig| {
                let ctx_field =
                    format_ident!("{}", to_snake_case(&sig.signal.to_string()));
                let setter = format_ident!(
                    "set_{}_{}",
                    to_snake_case(&sm.name.to_string()),
                    to_snake_case(&sig.signal.to_string())
                );
                quote! {
                    if let Some(value) = ctx.#ctx_field {
                        self.#setter(id, value);
                    }
                }
            })
            .collect();

        // 4. Apply edges (may reference newly created SMs)
        let edge_applies: Vec<_> = def
            .edges
            .iter()
            .filter(|e| e.source == sm.name)
            .map(|edge| {
                let ctx_field = format_ident!("{}", edge_snake(edge));
                let setter = format_ident!("set_{}_edges", edge_snake(edge));
                quote! {
                    if let Some(targets) = ctx.#ctx_field {
                        self.#setter(id, targets);
                    }
                }
            })
            .collect();

        // 5. Queue events
        let event_applies: Vec<_> = def
            .events
            .iter()
            .filter(|ev| ev.sender == sm.name)
            .map(|ev| {
                let ctx_field = format_ident!("pending_{}", to_snake_case(&ev.name.to_string()));
                let variant = &ev.name;
                let event_str = ev.name.to_string();
                quote! {
                    for (receiver_id, payload) in ctx.#ctx_field {
                        self.tracer.trace(crate::trace::TraceEvent::EventQueued {
                            event: #event_str,
                            sender: crate::trace::DebugValue::Borrowed(&id as &dyn std::fmt::Debug),
                            receiver: crate::trace::DebugValue::Borrowed(&receiver_id as &dyn std::fmt::Debug),
                            payload: crate::trace::DebugValue::Borrowed(&payload as &dyn std::fmt::Debug),
                        });
                        self.pending_events.push_back(PendingEvent::#variant(id, receiver_id, payload));
                    }
                }
            })
            .collect();

        // 6. Self-destruct
        let destroy_method = format_ident!("destroy_{}", to_snake_case(&sm.name.to_string()));
        let self_destruct_apply = quote! {
            if ctx.pending_self_destruct {
                self.#destroy_method(id);
            }
        };

        methods.push(quote! {
            fn #method(&mut self, id: #id_type, ctx: #ctx_name) -> bool {
                self.tracer.trace(crate::trace::TraceEvent::EffectsStart {
                    node: #node_str,
                    id: crate::trace::DebugValue::Borrowed(&id as &dyn std::fmt::Debug),
                });
                #(#create_applies)*
                #(#counter_updates)*
                #(#signal_applies)*
                #(#edge_applies)*
                #(#event_applies)*
                if ctx.pending_self_destruct {
                    self.tracer.trace(crate::trace::TraceEvent::SmDestroyed {
                        node: #node_str,
                        id: crate::trace::DebugValue::Borrowed(&id as &dyn std::fmt::Debug),
                    });
                }
                #self_destruct_apply
                self.tracer.trace(crate::trace::TraceEvent::EffectsEnd {
                    node: #node_str,
                    id: crate::trace::DebugValue::Borrowed(&id as &dyn std::fmt::Debug),
                });
                ctx.pending_self_destruct
            }
        });
    }
}

fn gen_propagate(def: &TopologyDef, methods: &mut Vec<TokenStream>) {
    // Generate counter passing code for Ctx::new calls
    let counter_passes: Vec<_> = def
        .state_machines
        .iter()
        .filter(|s| s.id_type.is_none())
        .map(|sm| {
            let f = format_ident!("next_{}_id", to_snake_case(&sm.name.to_string()));
            quote! { , self.#f }
        })
        .collect();

    let match_arms: Vec<_> = def
        .inputs
        .iter()
        .map(|inp| {
            let variant = dirty_variant(inp);
            let instances =
                format_ident!("{}_instances", to_snake_case(&inp.node.to_string()));
            let aggregate = format_ident!(
                "aggregate_{}_{}",
                to_snake_case(&inp.node.to_string()),
                to_snake_case(&inp.input_name.to_string())
            );
            let last = last_field(inp);
            let node_str = inp.node.to_string();
            let input_str = inp.input_name.to_string();

            if is_sm_node(def, &inp.node) {
                // SM path: remove instance → create ctx → call handler → apply effects → reinsert
                let sm = def
                    .state_machines
                    .iter()
                    .find(|s| s.name == inp.node)
                    .unwrap();
                let input_enum = format_ident!("{}Input", sm.name);
                let ctx_name = format_ident!("{}Ctx", sm.name);
                let input_variant = &inp.input_name;
                let apply = format_ident!(
                    "apply_{}_effects",
                    to_snake_case(&inp.node.to_string())
                );
                let counter_passes = &counter_passes;

                quote! {
                    DirtyInput::#variant(target_id) => {
                        if !self.#instances.contains_key(&target_id) {
                            continue;
                        }
                        let result = self.#aggregate(target_id);
                        if self.#last.get(&target_id) == Some(&result) {
                            self.tracer.trace(crate::trace::TraceEvent::InputSuppressed {
                                node: #node_str,
                                id: crate::trace::DebugValue::Borrowed(&target_id as &dyn std::fmt::Debug),
                                input: #input_str,
                            });
                            continue;
                        }
                        self.#last.insert(target_id, result.clone());

                        self.tracer.trace(crate::trace::TraceEvent::InputDelivered {
                            node: #node_str,
                            id: crate::trace::DebugValue::Borrowed(&target_id as &dyn std::fmt::Debug),
                            input: #input_str,
                            value: crate::trace::DebugValue::Borrowed(&result as &dyn std::fmt::Debug),
                        });

                        let mut sm = self.#instances.remove(&target_id).unwrap();
                        let mut ctx = #ctx_name::new(target_id #(#counter_passes)*);
                        sm.handle(#input_enum::#input_variant(result), &mut ctx);

                        let self_destructed = self.#apply(target_id, ctx);
                        if !self_destructed {
                            self.#instances.insert(target_id, sm);
                        }
                    }
                }
            } else {
                // Port path: check existence → aggregate → dedup → push to output queue
                let port_input_enum = format_ident!("{}PortInput", inp.node);
                let input_variant = &inp.input_name;
                let pending_field = format_ident!("{}_pending_inputs", to_snake_case(&inp.node.to_string()));

                quote! {
                    DirtyInput::#variant(target_id) => {
                        if !self.#instances.contains(&target_id) {
                            continue;
                        }
                        let result = self.#aggregate(target_id);
                        if self.#last.get(&target_id) == Some(&result) {
                            self.tracer.trace(crate::trace::TraceEvent::InputSuppressed {
                                node: #node_str,
                                id: crate::trace::DebugValue::Borrowed(&target_id as &dyn std::fmt::Debug),
                                input: #input_str,
                            });
                            continue;
                        }
                        self.#last.insert(target_id, result.clone());

                        self.tracer.trace(crate::trace::TraceEvent::InputDelivered {
                            node: #node_str,
                            id: crate::trace::DebugValue::Borrowed(&target_id as &dyn std::fmt::Debug),
                            input: #input_str,
                            value: crate::trace::DebugValue::Borrowed(&result as &dyn std::fmt::Debug),
                        });

                        self.#pending_field.push((target_id, #port_input_enum::#input_variant(result)));
                    }
                }
            }
        })
        .collect();

    // Generate event processing arms
    let event_arms: Vec<_> = def
        .events
        .iter()
        .map(|ev| {
            let variant = &ev.name;
            let instances =
                format_ident!("{}_instances", to_snake_case(&ev.receiver.to_string()));
            let input_enum = format_ident!("{}Input", ev.receiver);
            let ctx_name = format_ident!("{}Ctx", ev.receiver);
            let apply = format_ident!(
                "apply_{}_effects",
                to_snake_case(&ev.receiver.to_string())
            );
            let counter_passes = &counter_passes;
            let event_str = ev.name.to_string();

            let connectivity = gen_connectivity_check(def, ev);

            quote! {
                PendingEvent::#variant(sender_id, receiver_id, payload) => {
                    let connected = #connectivity;
                    if !connected {
                        panic!(
                            "Event {:?} rejected: no edge between sender {:?} and receiver {:?}",
                            stringify!(#variant), sender_id, receiver_id
                        );
                    }

                    if let Some(mut sm) = self.#instances.remove(&receiver_id) {
                        self.tracer.trace(crate::trace::TraceEvent::EventDelivered {
                            event: #event_str,
                            sender: crate::trace::DebugValue::Borrowed(&sender_id as &dyn std::fmt::Debug),
                            receiver: crate::trace::DebugValue::Borrowed(&receiver_id as &dyn std::fmt::Debug),
                            payload: crate::trace::DebugValue::Borrowed(&payload as &dyn std::fmt::Debug),
                        });
                        let mut ctx = #ctx_name::new(receiver_id #(#counter_passes)*);
                        sm.handle(#input_enum::#variant(payload), &mut ctx);

                        let self_destructed = self.#apply(receiver_id, ctx);
                        if !self_destructed {
                            self.#instances.insert(receiver_id, sm);
                        }
                    }
                }
            }
        })
        .collect();

    let invariant_checks: Vec<TokenStream> = def
        .invariants
        .iter()
        .map(|inv| {
            let sig = def
                .signals
                .iter()
                .find(|s| s.node == inv.node && s.signal == inv.signal)
                .expect("invariant references valid signal (validated)");
            let field = signal_field(sig);
            let expr = &inv.expr;
            let node_str = inv.node.to_string();
            let signal_str = inv.signal.to_string();
            let expr_str = expr.to_token_stream().to_string();
            quote! {
                for (id, value) in &self.#field {
                    if !(#expr) {
                        self.tracer.trace(crate::trace::TraceEvent::InvariantViolation {
                            node: #node_str,
                            id: crate::trace::DebugValue::Borrowed(id as &dyn std::fmt::Debug),
                            signal: #signal_str,
                            value: crate::trace::DebugValue::Borrowed(value as &dyn std::fmt::Debug),
                            invariant_expr: #expr_str,
                        });
                    }
                }
            }
        })
        .collect();

    methods.push(quote! {
        fn propagate(&mut self) {
            self.tracer.trace(crate::trace::TraceEvent::PropagateStart);
            let mut depth = 0;

            while !self.dirty.is_empty() || !self.pending_events.is_empty() {
                depth += 1;
                if depth == self.depth_limit {
                    panic!(
                        "Signal router depth limit ({}) exceeded",
                        self.depth_limit
                    );
                }
                if depth == self.depth_limit - 1 {
                    eprintln!(
                        "WARNING: Signal router approaching depth limit ({}/{})",
                        depth, self.depth_limit
                    );
                }

                self.tracer.trace(crate::trace::TraceEvent::RoundStart { depth });

                // Process dirty signal queue
                let wave: Vec<DirtyInput> = self.dirty.drain(..).collect();
                let mut seen = std::collections::BTreeSet::new();

                for entry in wave {
                    if !seen.insert(entry.clone()) {
                        continue;
                    }

                    match entry {
                        #(#match_arms)*
                    }
                }

                // Process pending events
                let events: Vec<PendingEvent> = self.pending_events.drain(..).collect();
                for event in events {
                    match event {
                        #(#event_arms)*
                    }
                }

                self.tracer.trace(crate::trace::TraceEvent::RoundEnd { depth });
            }

            #(#invariant_checks)*

            self.tracer.trace(crate::trace::TraceEvent::PropagateEnd { rounds: depth });
        }
    });
}
