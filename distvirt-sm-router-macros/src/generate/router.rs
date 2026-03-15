use crate::parse::*;
use proc_macro2::TokenStream;
use quote::{format_ident, quote};

use super::helpers::*;
use super::router_lifecycle::*;
use super::router_propagate::*;
use super::router_setters::*;
use super::snapshot;

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
            quote! { #vis #f: std::collections::BTreeMap<#id, <#agg as ::distvirt_sm_router::Aggregator>::Output> },
        );
        inits.push(quote! { #f: std::collections::BTreeMap::new() });
    }

    // Pending creates
    fields.push(quote! { pending_creates: Vec<PendingCreate> });
    inits.push(quote! { pending_creates: Vec::new() });

    // ID allocator
    let auto_count = auto_id_count(def);
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
    inits.push(quote! { dirty: std::collections::VecDeque::new() });
    inits.push(quote! { pending_events: std::collections::VecDeque::new() });
    inits.push(quote! { depth_limit });
    inits.push(quote! { manual_phase: ::distvirt_sm_router::ManualPhase::Idle });
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

    let snapshot_tokens = snapshot::gen_snapshot(def);

    let instances_vis: TokenStream = if expose {
        quote! { pub }
    } else {
        quote! {}
    };

    quote! {
        #[allow(dead_code)]
        mod __router {
            use super::*;

            pub struct RouterInstances {
                #(#instance_fields,)*
            }

            pub struct Router<
                __Tracer: ::distvirt_sm_router::trace::Tracer = ::distvirt_sm_router::trace::NoopTracer,
                __IdAlloc: ::distvirt_sm_router::IdAllocator<NodeKind> = ::distvirt_sm_router::SequentialIds,
            > {
                #instances_vis instances: RouterInstances,
                #(#fields,)*
            }

            #[allow(dead_code)]
            impl Router<::distvirt_sm_router::trace::NoopTracer, ::distvirt_sm_router::SequentialIds> {
                pub fn new(depth_limit: usize) -> Self {
                    Router {
                        instances: RouterInstances {
                            #(#instance_inits,)*
                        },
                        #(#inits,)*
                        tracer: ::distvirt_sm_router::trace::NoopTracer,
                        id_alloc: ::distvirt_sm_router::SequentialIds::new(#auto_count),
                    }
                }
            }

            #[allow(dead_code)]
            impl<__Tracer: ::distvirt_sm_router::trace::Tracer> Router<__Tracer, ::distvirt_sm_router::SequentialIds> {
                pub fn new_traced(depth_limit: usize, tracer: __Tracer) -> Self {
                    Router {
                        instances: RouterInstances {
                            #(#instance_inits,)*
                        },
                        #(#inits,)*
                        tracer,
                        id_alloc: ::distvirt_sm_router::SequentialIds::new(#auto_count),
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

            #snapshot_tokens
        }

        #[allow(unused_imports)]
        use __router::Router;

        #[allow(unused_imports)]
        use __router::RouterSnapshot;
    }
}
