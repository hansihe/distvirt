use crate::parse::*;
use proc_macro2::TokenStream;
use quote::{format_ident, quote};
use syn::Ident;

fn to_snake_case(s: &str) -> String {
    let mut result = String::new();
    for (i, c) in s.chars().enumerate() {
        if c.is_uppercase() {
            if i > 0 {
                result.push('_');
            }
            result.extend(c.to_lowercase());
        } else {
            result.push(c);
        }
    }
    result
}

fn snake_ident(s: &str) -> Ident {
    format_ident!("{}", to_snake_case(s))
}

/// Returns the ID type tokens for a node. For auto-ID nodes, generates `{Name}Id`.
fn node_id_type(def: &TopologyDef, node: &Ident) -> TokenStream {
    if let Some(sm) = def.state_machines.iter().find(|s| s.name == *node) {
        sm_id_type(sm)
    } else if let Some(port) = def.ports.iter().find(|p| p.name == *node) {
        port_id_type(port)
    } else {
        panic!("unknown node: {}", node)
    }
}

fn sm_id_type(sm: &SmDef) -> TokenStream {
    match &sm.id_type {
        Some(ty) => quote! { #ty },
        None => {
            let id = format_ident!("{}Id", sm.name);
            quote! { #id }
        }
    }
}

fn port_id_type(port: &PortDef) -> TokenStream {
    match &port.id_type {
        Some(ty) => quote! { #ty },
        None => {
            let id = format_ident!("{}Id", port.name);
            quote! { #id }
        }
    }
}

fn is_sm_node(def: &TopologyDef, node: &Ident) -> bool {
    def.state_machines.iter().any(|s| s.name == *node)
}

fn signal_field(sig: &SignalDef) -> Ident {
    format_ident!(
        "{}_{}",
        to_snake_case(&sig.node.to_string()),
        to_snake_case(&sig.signal.to_string())
    )
}

fn edge_snake(edge: &EdgeDef) -> String {
    to_snake_case(&edge.name.to_string())
}

fn last_field(inp: &InputDef) -> Ident {
    format_ident!(
        "last_{}_{}",
        to_snake_case(&inp.node.to_string()),
        to_snake_case(&inp.input_name.to_string())
    )
}

fn dirty_variant(inp: &InputDef) -> Ident {
    format_ident!("{}{}", inp.node, inp.input_name)
}

fn validate(def: &TopologyDef) {
    let is_node = |name: &Ident| -> bool {
        def.state_machines.iter().any(|s| s.name == *name)
            || def.ports.iter().any(|p| p.name == *name)
    };

    for sig in &def.signals {
        assert!(
            is_node(&sig.node),
            "unknown node in signal: {}",
            sig.node
        );
    }
    for ev in &def.events {
        assert!(
            is_node(&ev.sender),
            "unknown sender in event {}: {}",
            ev.name,
            ev.sender
        );
        assert!(
            def.state_machines.iter().any(|s| s.name == ev.receiver),
            "event {} receiver {} must be a state machine",
            ev.name,
            ev.receiver
        );
        // Verify at least one edge type connects sender and receiver (either direction)
        let has_connecting_edge = def.edges.iter().any(|e| {
            (e.source == ev.sender && e.target == ev.receiver)
                || (e.source == ev.receiver && e.target == ev.sender)
        });
        assert!(
            has_connecting_edge,
            "event {}: no edge type connects {} and {}",
            ev.name,
            ev.sender,
            ev.receiver
        );
    }
    for edge in &def.edges {
        assert!(
            is_node(&edge.source),
            "unknown source in edge {}: {}",
            edge.name,
            edge.source
        );
        assert!(
            is_node(&edge.target),
            "unknown target in edge {}: {}",
            edge.name,
            edge.target
        );
    }
    for inp in &def.inputs {
        assert!(
            def.state_machines.iter().any(|s| s.name == inp.node),
            "input {} targets non-SM node {}",
            inp.input_name,
            inp.node
        );
        for sp in &inp.sources {
            assert!(
                def.edges.iter().any(|e| e.name == sp.edge),
                "input {}::{} references unknown edge {}",
                inp.node,
                inp.input_name,
                sp.edge
            );
            assert!(
                def.signals
                    .iter()
                    .any(|s| s.node == sp.node && s.signal == sp.signal),
                "input {}::{} references unknown signal {}::{}",
                inp.node,
                inp.input_name,
                sp.node,
                sp.signal
            );
            let edge_def = def.edges.iter().find(|e| e.name == sp.edge).unwrap();
            assert!(
                edge_def.source == sp.node,
                "input {}::{}: edge {} source is {}, but signal is on {}",
                inp.node,
                inp.input_name,
                sp.edge,
                edge_def.source,
                sp.node
            );
            assert!(
                edge_def.target == inp.node,
                "input {}::{}: edge {} target is {}, but input is on {}",
                inp.node,
                inp.input_name,
                sp.edge,
                edge_def.target,
                inp.node
            );
        }
    }
}

pub fn generate(def: &TopologyDef) -> TokenStream {
    validate(def);

    let auto_id_types = gen_auto_id_types(def);
    let signal_bounds = gen_signal_bound_checks(def);
    let source_enums = gen_source_enums(def);
    let input_enums = gen_input_enums(def);
    let ctx_structs = gen_ctx_structs(def);
    let dirty_enum = gen_dirty_enum(def);
    let pending_event_enum = gen_pending_event_enum(def);
    let router_module = gen_router_module(def);

    quote! {
        #auto_id_types
        #signal_bounds
        #source_enums
        #input_enums
        #ctx_structs
        #dirty_enum
        #pending_event_enum
        #router_module
    }
}

/// Generate compile-time checks that signal value types implement PartialEq.
/// Produces clear error messages pointing at the user's type rather than generated code.
fn gen_signal_bound_checks(def: &TopologyDef) -> TokenStream {
    let checks: Vec<_> = def
        .signals
        .iter()
        .map(|sig| {
            let vt = &sig.value_type;
            let fn_name = format_ident!(
                "__assert_signal_partial_eq_{}_{}",
                to_snake_case(&sig.node.to_string()),
                to_snake_case(&sig.signal.to_string())
            );
            quote! {
                #[doc(hidden)]
                const fn #fn_name<T: PartialEq>() {}
                const _: () = #fn_name::<#vt>();
            }
        })
        .collect();

    quote! { #(#checks)* }
}

/// Generate newtype ID structs for auto-ID nodes.
fn gen_auto_id_types(def: &TopologyDef) -> TokenStream {
    let mut types = Vec::new();

    for sm in &def.state_machines {
        if sm.id_type.is_none() {
            let id_name = format_ident!("{}Id", sm.name);
            types.push(quote! {
                #[derive(Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug)]
                #[allow(dead_code)]
                struct #id_name(u64);
            });
        }
    }

    for port in &def.ports {
        if port.id_type.is_none() {
            let id_name = format_ident!("{}Id", port.name);
            types.push(quote! {
                #[derive(Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug)]
                #[allow(dead_code)]
                struct #id_name(u64);
            });
        }
    }

    quote! { #(#types)* }
}

fn gen_source_enums(def: &TopologyDef) -> TokenStream {
    let enums: Vec<_> = def
        .inputs
        .iter()
        .filter(|inp| inp.sources.len() >= 2)
        .map(|inp| {
            let enum_name = format_ident!("{}Source", inp.input_name);
            let variants: Vec<_> = inp
                .sources
                .iter()
                .map(|sp| {
                    let variant_name = format_ident!("{}{}", sp.node, sp.signal);
                    let src_id = node_id_type(def, &sp.node);
                    let sig = def
                        .signals
                        .iter()
                        .find(|s| s.node == sp.node && s.signal == sp.signal)
                        .unwrap();
                    let vt = &sig.value_type;
                    quote! { #variant_name(#src_id, #vt) }
                })
                .collect();

            quote! {
                #[derive(Debug, Clone, PartialEq)]
                #[allow(dead_code)]
                enum #enum_name {
                    #(#variants,)*
                }
            }
        })
        .collect();

    quote! { #(#enums)* }
}

fn gen_input_enums(def: &TopologyDef) -> TokenStream {
    let enums: Vec<_> = def
        .state_machines
        .iter()
        .map(|sm| {
            let enum_name = format_ident!("{}Input", sm.name);

            let input_variants: Vec<_> = def
                .inputs
                .iter()
                .filter(|inp| inp.node == sm.name)
                .map(|inp| {
                    let variant = &inp.input_name;
                    let agg = &inp.aggregator;
                    quote! {
                        #variant(<#agg as crate::Aggregator>::Output)
                    }
                })
                .collect();

            let event_variants: Vec<_> = def
                .events
                .iter()
                .filter(|ev| ev.receiver == sm.name)
                .map(|ev| {
                    let variant = &ev.name;
                    let payload = &ev.payload_type;
                    quote! {
                        #variant(#payload)
                    }
                })
                .collect();

            quote! {
                #[derive(Debug, PartialEq)]
                #[allow(dead_code)]
                enum #enum_name {
                    #(#input_variants,)*
                    #(#event_variants,)*
                }
            }
        })
        .collect();

    quote! { #(#enums)* }
}

fn gen_ctx_structs(def: &TopologyDef) -> TokenStream {
    let structs: Vec<_> = def
        .state_machines
        .iter()
        .map(|sm| {
            let ctx_name = format_ident!("{}Ctx", sm.name);
            let id_type = sm_id_type(sm);

            // Signals this SM produces
            let signals: Vec<_> = def
                .signals
                .iter()
                .filter(|s| s.node == sm.name)
                .collect();

            // Outgoing edges from this SM
            let out_edges: Vec<_> =
                def.edges.iter().filter(|e| e.source == sm.name).collect();

            // Events this SM can send
            let out_events: Vec<_> = def
                .events
                .iter()
                .filter(|ev| ev.sender == sm.name)
                .collect();

            // Struct fields
            let signal_fields: Vec<_> = signals
                .iter()
                .map(|sig| {
                    let f = format_ident!("{}", to_snake_case(&sig.signal.to_string()));
                    let vt = &sig.value_type;
                    quote! { #f: Option<#vt> }
                })
                .collect();

            let edge_fields: Vec<_> = out_edges
                .iter()
                .map(|edge| {
                    let f = format_ident!("{}", edge_snake(edge));
                    let tgt_id = node_id_type(def, &edge.target);
                    quote! { #f: Option<Vec<#tgt_id>> }
                })
                .collect();

            let event_fields: Vec<_> = out_events
                .iter()
                .map(|ev| {
                    let f = format_ident!("pending_{}", to_snake_case(&ev.name.to_string()));
                    let receiver_id = node_id_type(def, &ev.receiver);
                    let payload = &ev.payload_type;
                    quote! { #f: Vec<(#receiver_id, #payload)> }
                })
                .collect();

            // Constructor init
            let signal_inits: Vec<_> = signals
                .iter()
                .map(|sig| {
                    let f = format_ident!("{}", to_snake_case(&sig.signal.to_string()));
                    quote! { #f: None }
                })
                .collect();

            let event_inits: Vec<_> = out_events
                .iter()
                .map(|ev| {
                    let f = format_ident!("pending_{}", to_snake_case(&ev.name.to_string()));
                    quote! { #f: Vec::new() }
                })
                .collect();

            let edge_inits: Vec<_> = out_edges
                .iter()
                .map(|edge| {
                    let f = format_ident!("{}", edge_snake(edge));
                    quote! { #f: None }
                })
                .collect();

            // Signal setters
            let signal_setters: Vec<_> = signals
                .iter()
                .map(|sig| {
                    let field =
                        format_ident!("{}", to_snake_case(&sig.signal.to_string()));
                    let method =
                        format_ident!("set_{}", to_snake_case(&sig.signal.to_string()));
                    let vt = &sig.value_type;
                    quote! {
                        #[allow(dead_code)]
                        pub fn #method(&mut self, value: #vt) {
                            self.#field = Some(value);
                        }
                    }
                })
                .collect();

            // Event send methods
            let event_senders: Vec<_> = out_events
                .iter()
                .map(|ev| {
                    let method = format_ident!("send_{}", to_snake_case(&ev.name.to_string()));
                    let field = format_ident!("pending_{}", to_snake_case(&ev.name.to_string()));
                    let receiver_id = node_id_type(def, &ev.receiver);
                    let payload = &ev.payload_type;
                    quote! {
                        #[allow(dead_code)]
                        pub fn #method(&mut self, target: #receiver_id, payload: #payload) {
                            self.#field.push((target, payload));
                        }
                    }
                })
                .collect();

            // Edge setters
            let edge_setters: Vec<_> = out_edges
                .iter()
                .map(|edge| {
                    let method = format_ident!("set_{}_edges", edge_snake(edge));
                    let field = format_ident!("{}", edge_snake(edge));
                    let tgt_id = node_id_type(def, &edge.target);
                    quote! {
                        #[allow(dead_code)]
                        pub fn #method(&mut self, targets: impl IntoIterator<Item = #tgt_id>) {
                            self.#field = Some(targets.into_iter().collect());
                        }
                    }
                })
                .collect();

            quote! {
                #[allow(dead_code)]
                struct #ctx_name {
                    #[allow(dead_code)]
                    id: #id_type,
                    #(#signal_fields,)*
                    #(#edge_fields,)*
                    #(#event_fields,)*
                }

                #[allow(dead_code)]
                impl #ctx_name {
                    fn new(id: #id_type) -> Self {
                        #ctx_name {
                            id,
                            #(#signal_inits,)*
                            #(#edge_inits,)*
                            #(#event_inits,)*
                        }
                    }

                    #[allow(dead_code)]
                    pub fn id(&self) -> #id_type {
                        self.id
                    }

                    #(#signal_setters)*
                    #(#edge_setters)*
                    #(#event_senders)*
                }
            }
        })
        .collect();

    quote! { #(#structs)* }
}

fn gen_dirty_enum(def: &TopologyDef) -> TokenStream {
    let variants: Vec<_> = def
        .inputs
        .iter()
        .map(|inp| {
            let variant = dirty_variant(inp);
            let id_type = node_id_type(def, &inp.node);
            quote! { #variant(#id_type) }
        })
        .collect();

    quote! {
        #[derive(Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug)]
        #[allow(dead_code)]
        enum DirtyInput {
            #(#variants,)*
        }
    }
}

fn gen_pending_event_enum(def: &TopologyDef) -> TokenStream {
    if def.events.is_empty() {
        return quote! {
            #[derive(Debug)]
            #[allow(dead_code)]
            enum PendingEvent {}
        };
    }

    let variants: Vec<_> = def
        .events
        .iter()
        .map(|ev| {
            let variant = &ev.name;
            let sender_id = node_id_type(def, &ev.sender);
            let receiver_id = node_id_type(def, &ev.receiver);
            let payload = &ev.payload_type;
            quote! { #variant(#sender_id, #receiver_id, #payload) }
        })
        .collect();

    quote! {
        #[derive(Debug)]
        #[allow(dead_code)]
        enum PendingEvent {
            #(#variants,)*
        }
    }
}

fn gen_router_module(def: &TopologyDef) -> TokenStream {
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

    // Dirty queue, pending events, depth limit
    fields.push(quote! { dirty: std::collections::VecDeque<DirtyInput> });
    fields.push(quote! { pending_events: std::collections::VecDeque<PendingEvent> });
    fields.push(quote! { depth_limit: usize });
    inits.push(quote! { dirty: std::collections::VecDeque::new() });
    inits.push(quote! { pending_events: std::collections::VecDeque::new() });
    inits.push(quote! { depth_limit });

    // Collect methods by visibility category
    let mut public_methods = Vec::new();
    let mut internal_methods = Vec::new();

    // Always public: SM lifecycle, port lifecycle, propagate, SM accessors
    gen_create_methods(def, &mut public_methods);
    gen_destroy_methods(def, &mut public_methods);
    gen_remove_methods(def, &mut public_methods);
    gen_sm_accessors(def, &mut public_methods);
    gen_propagate(def, &mut public_methods);

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

            pub struct Router {
                #(#fields,)*
            }

            #[allow(dead_code)]
            impl Router {
                pub fn new(depth_limit: usize) -> Self {
                    Router {
                        #(#inits,)*
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
                    self.#instances.insert(id, sm);
                    #(#sig_inits)*
                    id
                }
            });
        } else {
            // User-provided ID
            methods.push(quote! {
                fn #method(&mut self, id: #id_type, sm: #handler) {
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
                    self.#instances.insert(id);
                    #(#sig_inits)*
                    id
                }
            });
        } else {
            // User-provided ID
            methods.push(quote! {
                fn #method(&mut self, id: #id_type) {
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

        methods.push(quote! {
            fn #method(&mut self, id: #id_type) {
                self.#instances.remove(&id);
                #(#sig_removes)*
                #(#edge_clears)*
                #(#incoming_edge_clears)*
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

        let body = quote! {
            fn #method(&mut self, id: #id_type, value: #vt) {
                if self.#field.get(&id) == Some(&value) {
                    return;
                }
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
fn gen_connectivity_check(def: &TopologyDef, ev: &crate::parse::EventDef) -> TokenStream {
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

        methods.push(quote! {
            fn #method(&mut self, sender_id: #sender_id, receiver_id: #receiver_id, payload: #payload) {
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

        let event_applies: Vec<_> = def
            .events
            .iter()
            .filter(|ev| ev.sender == sm.name)
            .map(|ev| {
                let ctx_field = format_ident!("pending_{}", to_snake_case(&ev.name.to_string()));
                let variant = &ev.name;
                quote! {
                    for (receiver_id, payload) in ctx.#ctx_field {
                        self.pending_events.push_back(PendingEvent::#variant(id, receiver_id, payload));
                    }
                }
            })
            .collect();

        methods.push(quote! {
            fn #method(&mut self, id: #id_type, ctx: #ctx_name) {
                #(#signal_applies)*
                #(#edge_applies)*
                #(#event_applies)*
            }
        });
    }
}

fn gen_propagate(def: &TopologyDef, methods: &mut Vec<TokenStream>) {
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

            quote! {
                DirtyInput::#variant(target_id) => {
                    if !self.#instances.contains_key(&target_id) {
                        continue;
                    }
                    let result = self.#aggregate(target_id);
                    if self.#last.get(&target_id) == Some(&result) {
                        continue;
                    }
                    self.#last.insert(target_id, result.clone());

                    let mut sm = self.#instances.remove(&target_id).unwrap();
                    let mut ctx = #ctx_name::new(target_id);
                    sm.handle(#input_enum::#input_variant(result), &mut ctx);
                    self.#instances.insert(target_id, sm);

                    self.#apply(target_id, ctx);
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
                        let mut ctx = #ctx_name::new(receiver_id);
                        sm.handle(#input_enum::#variant(payload), &mut ctx);
                        self.#instances.insert(receiver_id, sm);
                        self.#apply(receiver_id, ctx);
                    }
                }
            }
        })
        .collect();

    methods.push(quote! {
        fn propagate(&mut self) {
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
            }
        }
    });
}
