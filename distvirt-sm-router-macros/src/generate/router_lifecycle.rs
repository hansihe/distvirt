use crate::parse::*;
use proc_macro2::TokenStream;
use quote::{format_ident, quote};

use super::helpers::*;

pub(super) fn gen_create_methods(def: &TopologyDef, methods: &mut Vec<TokenStream>) {
    // SM creation — just accumulate into pending_creates
    for sm in &def.state_machines {
        let method = format_ident!("create_{}", to_snake_case(&sm.name.to_string()));
        let id_type = sm_id_type(sm);
        let handler = &sm.handler_type;
        let variant = &sm.name;

        if sm.id_type.is_none() {
            // Auto-ID: generate ID internally, return it
            let id_name = format_ident!("{}Id", sm.name);
            methods.push(quote! {
                fn #method(&mut self, sm: #handler) -> #id_type {
                    let id = #id_name(self.id_alloc.alloc(NodeKind::#variant, None));
                    self.pending_creates.push(PendingCreate::#variant(id, sm));
                    id
                }
            });
        } else {
            // User-provided ID
            methods.push(quote! {
                fn #method(&mut self, id: #id_type, sm: #handler) {
                    self.pending_creates.push(PendingCreate::#variant(id, sm));
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
            let port_variant = &port.name;
            methods.push(quote! {
                fn #method(&mut self) -> #id_type {
                    let id = #id_name(self.id_alloc.alloc(NodeKind::#port_variant, None));
                    self.tracer.trace(::distvirt_sm_router::trace::TraceEvent::PortCreated {
                        node: #node_str,
                        id: ::distvirt_sm_router::trace::DebugValue::Borrowed(&id as &dyn std::fmt::Debug),
                    });
                    self.instances.#instances.insert(id);
                    #(#sig_inits)*
                    id
                }
            });
        } else {
            // User-provided ID
            methods.push(quote! {
                fn #method(&mut self, id: #id_type) {
                    self.tracer.trace(::distvirt_sm_router::trace::TraceEvent::PortCreated {
                        node: #node_str,
                        id: ::distvirt_sm_router::trace::DebugValue::Borrowed(&id as &dyn std::fmt::Debug),
                    });
                    self.instances.#instances.insert(id);
                    #(#sig_inits)*
                }
            });
        }
    }
}

pub(super) fn gen_destroy_methods(def: &TopologyDef, methods: &mut Vec<TokenStream>) {
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
                let sm = self.instances.#instances.remove(&id);
                #(#sig_removes)*
                #(#last_removes)*
                #(#edge_clears)*
                sm
            }
        });
    }
}

pub(super) fn gen_remove_methods(def: &TopologyDef, methods: &mut Vec<TokenStream>) {
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
                self.tracer.trace(::distvirt_sm_router::trace::TraceEvent::PortDestroyed {
                    node: #node_str,
                    id: ::distvirt_sm_router::trace::DebugValue::Borrowed(&id as &dyn std::fmt::Debug),
                });
                self.instances.#instances.remove(&id);
                #(#sig_removes)*
                #(#last_removes)*
                #(#edge_clears)*
                #(#incoming_edge_clears)*
                #(#queue_retains)*
            }
        });
    }
}

pub(super) fn gen_drain_port_inputs(def: &TopologyDef, methods: &mut Vec<TokenStream>) {
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

pub(super) fn gen_sm_accessors(def: &TopologyDef, methods: &mut Vec<TokenStream>) {
    for sm in &def.state_machines {
        let get_method = format_ident!("get_{}", to_snake_case(&sm.name.to_string()));
        let instances = format_ident!("{}_instances", to_snake_case(&sm.name.to_string()));
        let id_type = sm_id_type(sm);
        let handler = &sm.handler_type;

        methods.push(quote! {
            fn #get_method(&self, id: &#id_type) -> Option<&#handler> {
                self.instances.#instances.get(id)
            }
        });
    }
}

pub(super) fn gen_initialize_methods(def: &TopologyDef, methods: &mut Vec<TokenStream>) {
    for sm in &def.state_machines {
        let method = format_ident!("initialize_{}_sm", to_snake_case(&sm.name.to_string()));
        let instances = format_ident!("{}_instances", to_snake_case(&sm.name.to_string()));
        let id_type = sm_id_type(sm);
        let ctx_name = format_ident!("{}CtxConcrete", sm.name);
        let apply = format_ident!("apply_{}_effects", to_snake_case(&sm.name.to_string()));
        let node_str = sm.name.to_string();

        methods.push(quote! {
            fn #method(&mut self, id: #id_type) {
                self.tracer.trace(::distvirt_sm_router::trace::TraceEvent::SmInitialized {
                    node: #node_str,
                    id: ::distvirt_sm_router::trace::DebugValue::Borrowed(&id as &dyn std::fmt::Debug),
                });
                let effects = {
                    let sm = self.instances.#instances.get_mut(&id).unwrap();
                    let mut ctx = #ctx_name::new(id, &mut self.id_alloc);
                    sm.initialize(&mut ctx);
                    ctx.into_effects()
                };
                let self_destructed = self.#apply(id, effects);
                if self_destructed {
                    // SM already removed by destroy in apply_effects
                }
            }
        });
    }
}

pub(super) fn gen_materialize_methods(def: &TopologyDef, methods: &mut Vec<TokenStream>) {
    // has_pending_creates
    methods.push(quote! {
        fn has_pending_creates(&self) -> bool {
            !self.pending_creates.is_empty()
        }
    });

    // materialize_pending_creates - match on PendingCreate variants
    let match_arms: Vec<_> = def
        .state_machines
        .iter()
        .map(|sm| {
            let variant = &sm.name;
            let snake = to_snake_case(&sm.name.to_string());
            let instances = format_ident!("{}_instances", snake);
            let node_str = sm.name.to_string();
            let initialize = format_ident!("initialize_{}_sm", snake);

            let sig_inits: Vec<_> = def
                .signals
                .iter()
                .filter(|s| s.node == sm.name)
                .map(|sig| {
                    let f = signal_field(sig);
                    quote! { self.#f.entry(new_id).or_insert_with(Default::default); }
                })
                .collect();

            quote! {
                PendingCreate::#variant(new_id, new_sm) => {
                    self.tracer.trace(::distvirt_sm_router::trace::TraceEvent::SmCreated {
                        node: #node_str,
                        id: ::distvirt_sm_router::trace::DebugValue::Borrowed(&new_id as &dyn std::fmt::Debug),
                    });
                    self.instances.#instances.insert(new_id, new_sm);
                    #(#sig_inits)*
                    self.#initialize(new_id);
                }
            }
        })
        .collect();

    methods.push(quote! {
        fn materialize_pending_creates(&mut self) {
            while !self.pending_creates.is_empty() {
                let wave = std::mem::take(&mut self.pending_creates);
                for pending in wave {
                    match pending {
                        #(#match_arms)*
                    }
                }
            }
        }
    });
}
