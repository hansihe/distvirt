use crate::parse::*;
use proc_macro2::TokenStream;
use quote::{format_ident, quote};

use super::helpers::*;
use super::router_lifecycle::*;
use super::router_propagate::*;
use super::router_setters::*;
use super::snapshot;

fn gen_clone_impls(
    def: &TopologyDef,
    _instance_fields: &[TokenStream],
    _instance_inits: &[TokenStream],
    _fields: &[TokenStream],
    _inits: &[TokenStream],
) -> TokenStream {
    // RouterInstances Clone impl
    let mut inst_clone_fields = Vec::new();
    let mut inst_where_bounds = Vec::new();

    for sm in &def.state_machines {
        let f = format_ident!("{}_instances", snake_ident(&sm.name.to_string()));
        let handler = &sm.handler_type;
        inst_clone_fields.push(quote! { #f: self.#f.clone() });
        inst_where_bounds.push(quote! { #handler: Clone });
    }
    for port in &def.ports {
        let f = format_ident!("{}_instances", snake_ident(&port.name.to_string()));
        inst_clone_fields.push(quote! { #f: self.#f.clone() });
    }

    let inst_where = if inst_where_bounds.is_empty() {
        quote! {}
    } else {
        quote! { where #(#inst_where_bounds,)* }
    };

    let instances_clone = quote! {
        impl Clone for RouterInstances #inst_where {
            fn clone(&self) -> Self {
                RouterInstances {
                    #(#inst_clone_fields,)*
                }
            }
        }
    };

    // Router Clone impl
    let mut router_clone_fields = Vec::new();
    let mut router_where_bounds = Vec::new();

    router_clone_fields.push(quote! { instances: self.instances.clone() });
    router_where_bounds.push(quote! { RouterInstances: Clone });

    // Signal value types
    for sig in &def.signals {
        let vt = &sig.value_type;
        router_where_bounds.push(quote! { #vt: Clone });
    }

    // Event payload types
    for ev in &def.events {
        let pt = &ev.payload_type;
        router_where_bounds.push(quote! { #pt: Clone });
    }

    // Aggregator output types
    for inp in &def.inputs {
        let agg = inp.aggregator.ty();
        if inp.aggregator.is_incremental() {
            // Incremental inputs store prev maps with signal value types,
            // which are already covered by signal value Clone bounds above.
            // No additional bound needed.
        } else {
            router_where_bounds
                .push(quote! { <#agg as ::distvirt_sm_router::Aggregator>::Output: Clone });
        }
    }

    // SignalState maps
    for node in nodes_with_signal_state(def) {
        let f = signal_state_field(node);
        router_clone_fields.push(quote! { #f: self.#f.clone() });
    }

    // Edges (fwd + rev)
    for edge in &def.edges {
        let snake = edge_snake(edge);
        let fwd = format_ident!("{}_fwd", snake);
        let rev = format_ident!("{}_rev", snake);
        router_clone_fields.push(quote! { #fwd: self.#fwd.clone() });
        router_clone_fields.push(quote! { #rev: self.#rev.clone() });
    }

    // Pending creates
    router_clone_fields.push(quote! { pending_creates: self.pending_creates.clone() });

    // ID allocator
    router_clone_fields.push(quote! { id_alloc: self.id_alloc.clone() });

    // Port input output queues
    for port in &def.ports {
        let has_inputs = def.inputs.iter().any(|inp| inp.node == port.name);
        if has_inputs {
            let f = format_ident!("{}_pending_inputs", to_snake_case(&port.name.to_string()));
            router_clone_fields.push(quote! { #f: self.#f.clone() });
        }
    }

    // Transient fields
    router_clone_fields.push(quote! { dirty: self.dirty.clone() });
    router_clone_fields.push(quote! { pending_events: self.pending_events.clone() });
    router_clone_fields.push(quote! { depth_limit: self.depth_limit });
    router_clone_fields.push(quote! { manual_phase: self.manual_phase.clone() });
    router_clone_fields.push(quote! { tracer: self.tracer.clone() });
    // Scratch buffers — NOT cloned, initialized empty
    router_clone_fields.push(quote! { dedup_wave: Vec::new() });
    router_clone_fields.push(quote! { dedup_seen: std::collections::BTreeSet::new() });
    router_clone_fields.push(quote! { event_wave: Vec::new() });

    let router_clone = quote! {
        impl<__Tracer: ::distvirt_sm_router::trace::Tracer + Clone, __IdAlloc: ::distvirt_sm_router::IdAllocator<NodeKind> + Clone>
            Clone for Router<__Tracer, __IdAlloc>
        where
            #(#router_where_bounds,)*
        {
            fn clone(&self) -> Self {
                Router {
                    #(#router_clone_fields,)*
                }
            }
        }
    };

    quote! {
        #instances_clone
        #router_clone
    }
}

pub(super) fn gen_router_module(def: &TopologyDef) -> TokenStream {
    let expose = def.expose_internals;

    // Determine field visibility
    let field_vis: TokenStream = if expose {
        quote! { pub }
    } else {
        quote! {}
    };

    // Build RouterInstances fields and inits
    let mut instance_fields = Vec::new();
    let mut instance_inits = Vec::new();

    // SM instances
    for sm in &def.state_machines {
        let f = format_ident!("{}_instances", snake_ident(&sm.name.to_string()));
        let id = sm_id_type(sm);
        let handler = &sm.handler_type;
        let vis = &field_vis;
        instance_fields.push(quote! { #vis #f: std::collections::BTreeMap<#id, #handler> });
        instance_inits.push(quote! { #f: std::collections::BTreeMap::new() });
    }

    // Port instances
    for port in &def.ports {
        let f = format_ident!("{}_instances", snake_ident(&port.name.to_string()));
        let id = port_id_type(port);
        let vis = &field_vis;
        instance_fields.push(quote! { #vis #f: std::collections::BTreeSet<#id> });
        instance_inits.push(quote! { #f: std::collections::BTreeSet::new() });
    }

    // Build Router fields (non-instance) and inits
    let mut fields = Vec::new();
    let mut inits = Vec::new();

    // SignalState map per node (consolidated signals + last-delivered)
    for node in nodes_with_signal_state(def) {
        let state_field = signal_state_field(node);
        let state_struct = signal_state_struct_name(node);
        let id = node_id_type(def, node);
        let vis = &field_vis;
        fields.push(quote! { #vis #state_field: std::collections::BTreeMap<#id, #state_struct> });
        inits.push(quote! { #state_field: std::collections::BTreeMap::new() });
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

    // Pending creates
    fields.push(quote! { pending_creates: Vec<PendingCreate> });
    inits.push(quote! { pending_creates: Vec::new() });

    // ID allocator
    fields.push(quote! { id_alloc: __IdAlloc });
    // id_alloc init is handled separately for each constructor (like tracer)

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

    // Dirty queue, pending events, depth limit, propagation phase, tracer
    fields.push(quote! { dirty: std::collections::VecDeque<DirtyInput> });
    fields.push(quote! { pending_events: std::collections::VecDeque<PendingEvent> });
    fields.push(quote! { depth_limit: usize });
    fields.push(quote! { manual_phase: ::distvirt_sm_router::ManualPhase });
    fields.push(quote! { tracer: __Tracer });
    // Reusable scratch buffers for propagation (transient, not cloned/snapshotted)
    fields.push(quote! { dedup_wave: Vec<DirtyInput> });
    fields.push(quote! { dedup_seen: std::collections::BTreeSet<DirtyInput> });
    fields.push(quote! { event_wave: Vec<PendingEvent> });
    inits.push(quote! { dirty: std::collections::VecDeque::new() });
    inits.push(quote! { pending_events: std::collections::VecDeque::new() });
    inits.push(quote! { depth_limit });
    inits.push(quote! { manual_phase: ::distvirt_sm_router::ManualPhase::Idle });
    inits.push(quote! { dedup_wave: Vec::new() });
    inits.push(quote! { dedup_seen: std::collections::BTreeSet::new() });
    inits.push(quote! { event_wave: Vec::new() });
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
    gen_propagate(def, &mut public_methods, &mut internal_methods);

    // Always public: port input drain methods
    gen_drain_port_inputs(def, &mut public_methods);

    // Always public: port signal/edge setters, port event senders
    // Internal: SM signal/edge setters
    gen_signal_setters(def, &mut public_methods, &mut internal_methods);
    gen_edge_setters(def, &mut public_methods, &mut internal_methods);
    gen_event_send_methods(def, &mut public_methods);

    // Always internal: aggregate, apply_effects, initialize, materialize
    gen_aggregate_methods(def, &mut internal_methods);
    gen_apply_effects(def, &mut internal_methods);
    gen_initialize_methods(def, &mut internal_methods);
    gen_materialize_methods(def, &mut internal_methods);

    // If expose_internals, internal methods become pub too
    let internal_vis: TokenStream = if expose {
        quote! { pub }
    } else {
        quote! {}
    };

    // Generate SignalState structs
    let mut signal_state_structs = Vec::new();
    for node in nodes_with_signal_state(def) {
        let struct_name = signal_state_struct_name(node);

        let mut ss_fields = Vec::new();
        let mut ss_defaults = Vec::new();

        for sig in def.signals.iter().filter(|s| s.node == *node) {
            let out_f = out_field_name(&sig.signal);
            let vt = &sig.value_type;
            ss_fields.push(quote! { pub #out_f: #vt });
            ss_defaults.push(quote! { #out_f: Default::default() });
        }

        for inp in def.inputs.iter().filter(|i| i.node == *node) {
            if inp.aggregator.is_incremental() {
                // Incremental inputs: one BTreeMap per source pair to track previous values
                for sp in &inp.sources {
                    let prev_f = prev_field_name(inp, sp);
                    let src_id = node_id_type(def, &sp.node);
                    let sig = def
                        .signals
                        .iter()
                        .find(|s| s.node == sp.node && s.signal == sp.signal)
                        .unwrap();
                    let vt = &sig.value_type;
                    ss_fields.push(
                        quote! { pub #prev_f: std::collections::BTreeMap<#src_id, #vt> },
                    );
                    ss_defaults.push(quote! { #prev_f: std::collections::BTreeMap::new() });
                }
            } else {
                let in_f = in_field_name(&inp.input_name);
                let agg = inp.aggregator.ty();
                ss_fields.push(
                    quote! { pub #in_f: Option<<#agg as ::distvirt_sm_router::Aggregator>::Output> },
                );
                ss_defaults.push(quote! { #in_f: None });
            }
        }

        signal_state_structs.push(quote! {
            #[derive(Clone)]
            pub struct #struct_name {
                #(#ss_fields,)*
            }
            impl Default for #struct_name {
                fn default() -> Self {
                    #struct_name {
                        #(#ss_defaults,)*
                    }
                }
            }
        });
    }

    let snapshot_tokens = snapshot::gen_snapshot(def);

    let clone_impls = if def.model_checkable {
        gen_clone_impls(def, &instance_fields, &instance_inits, &fields, &inits)
    } else {
        quote! {}
    };

    let instances_vis: TokenStream = if expose {
        quote! { pub }
    } else {
        quote! {}
    };

    quote! {
        #[allow(dead_code)]
        mod __router {
            use super::*;

            #(#signal_state_structs)*

            pub struct RouterInstances {
                #(#instance_fields,)*
            }

            pub struct Router<
                __Tracer: ::distvirt_sm_router::trace::Tracer = ::distvirt_sm_router::trace::NoopTracer,
                __IdAlloc: ::distvirt_sm_router::IdAllocator<NodeKind> = ::distvirt_sm_router::SequentialIds<NodeKind>,
            > {
                #instances_vis instances: RouterInstances,
                #(#fields,)*
            }

            #[allow(dead_code)]
            impl Router<::distvirt_sm_router::trace::NoopTracer, ::distvirt_sm_router::SequentialIds<NodeKind>> {
                pub fn new(depth_limit: usize) -> Self {
                    Router {
                        instances: RouterInstances {
                            #(#instance_inits,)*
                        },
                        #(#inits,)*
                        tracer: ::distvirt_sm_router::trace::NoopTracer,
                        id_alloc: <::distvirt_sm_router::SequentialIds<NodeKind>>::new(),
                    }
                }
            }

            #[allow(dead_code)]
            impl<__Tracer: ::distvirt_sm_router::trace::Tracer> Router<__Tracer, ::distvirt_sm_router::SequentialIds<NodeKind>> {
                pub fn new_traced(depth_limit: usize, tracer: __Tracer) -> Self {
                    Router {
                        instances: RouterInstances {
                            #(#instance_inits,)*
                        },
                        #(#inits,)*
                        tracer,
                        id_alloc: <::distvirt_sm_router::SequentialIds<NodeKind>>::new(),
                    }
                }
            }

            #[allow(dead_code)]
            impl<__Tracer: ::distvirt_sm_router::trace::Tracer, __IdAlloc: ::distvirt_sm_router::IdAllocator<NodeKind>> Router<__Tracer, __IdAlloc> {
                pub fn new_with_allocator(depth_limit: usize, id_alloc: __IdAlloc, tracer: __Tracer) -> Self {
                    Router {
                        instances: RouterInstances {
                            #(#instance_inits,)*
                        },
                        #(#inits,)*
                        tracer,
                        id_alloc,
                    }
                }

                #(pub #public_methods)*
                #(#internal_vis #internal_methods)*
            }

            #clone_impls

            #snapshot_tokens
        }

        #[allow(unused_imports)]
        pub use __router::Router;

        #[allow(unused_imports)]
        pub use __router::RouterSnapshot;
    }
}
