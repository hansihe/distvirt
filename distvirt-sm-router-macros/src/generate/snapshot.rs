use crate::parse::*;
use proc_macro2::TokenStream;
use quote::{format_ident, quote};

use super::helpers::*;

/// Generate the `RouterSnapshot` struct and `snapshot()` / `from_snapshot()` methods.
///
/// The snapshot captures all meaningful state:
/// - SM instance states
/// - Port instance sets
/// - Signal values
/// - Edge sets (forward only — reverse is derived)
/// - Auto-ID counters
/// - Last delivered values
///
/// Excluded (transient processing state):
/// - Dirty queue
/// - Pending events queue
/// - Pending creates
/// - Port pending input queues
/// - Tracer
///
/// The `RouterSnapshot` struct is generated with no trait derives. Trait impls
/// (Clone, Debug, PartialEq, Eq, Hash) depend on the user's SM handler and
/// signal value types implementing those traits. The `snapshot()` and
/// `from_snapshot()` methods have `where` bounds requiring `Clone` on the
/// relevant types, so they're only callable when the bounds are satisfied.
pub(super) fn gen_snapshot(def: &TopologyDef) -> TokenStream {
    let snapshot_struct = gen_snapshot_struct(def);
    let snapshot_methods = gen_snapshot_methods(def);

    quote! {
        #snapshot_struct
        #snapshot_methods
    }
}

fn gen_snapshot_struct(def: &TopologyDef) -> TokenStream {
    let mut fields = Vec::new();

    // SM instances
    for sm in &def.state_machines {
        let f = format_ident!("{}_instances", snake_ident(&sm.name.to_string()));
        let id = sm_id_type(sm);
        let handler = &sm.handler_type;
        fields.push(quote! { pub #f: std::collections::BTreeMap<#id, #handler> });
    }

    // Port instances
    for port in &def.ports {
        let f = format_ident!("{}_instances", snake_ident(&port.name.to_string()));
        let id = port_id_type(port);
        fields.push(quote! { pub #f: std::collections::BTreeSet<#id> });
    }

    // Signals
    for sig in &def.signals {
        let f = signal_field(sig);
        let id = node_id_type(def, &sig.node);
        let vt = &sig.value_type;
        fields.push(quote! { pub #f: std::collections::BTreeMap<#id, #vt> });
    }

    // Edges (forward only — reverse is derived)
    for edge in &def.edges {
        let snake = edge_snake(edge);
        let fwd = format_ident!("{}_fwd", snake);
        let src_id = node_id_type(def, &edge.source);
        let tgt_id = node_id_type(def, &edge.target);
        fields.push(quote! { pub #fwd: std::collections::BTreeMap<#src_id, Vec<#tgt_id>> });
    }

    // Last delivered values
    for inp in &def.inputs {
        let f = last_field(inp);
        let id = node_id_type(def, &inp.node);
        let agg = &inp.aggregator;
        fields.push(
            quote! { pub #f: std::collections::BTreeMap<#id, <#agg as ::distvirt_sm_router::Aggregator>::Output> },
        );
    }

    // ID allocator counter snapshot
    fields.push(quote! { pub id_alloc_counters: Vec<u64> });

    quote! {
        #[allow(dead_code)]
        pub struct RouterSnapshot {
            #(#fields,)*
        }
    }
}

fn gen_snapshot_methods(def: &TopologyDef) -> TokenStream {
    let mut snapshot_fields = Vec::new();
    let mut from_snapshot_fields = Vec::new();

    // SM instances
    for sm in &def.state_machines {
        let f = format_ident!("{}_instances", snake_ident(&sm.name.to_string()));
        snapshot_fields.push(quote! { #f: self.#f.clone() });
        from_snapshot_fields.push(quote! { #f: snapshot.#f.clone() });
    }

    // Port instances
    for port in &def.ports {
        let f = format_ident!("{}_instances", snake_ident(&port.name.to_string()));
        snapshot_fields.push(quote! { #f: self.#f.clone() });
        from_snapshot_fields.push(quote! { #f: snapshot.#f.clone() });
    }

    // Signals
    for sig in &def.signals {
        let f = signal_field(sig);
        snapshot_fields.push(quote! { #f: self.#f.clone() });
        from_snapshot_fields.push(quote! { #f: snapshot.#f.clone() });
    }

    // Edges (forward in snapshot, rebuild both fwd + rev in from_snapshot)
    for edge in &def.edges {
        let snake = edge_snake(edge);
        let fwd = format_ident!("{}_fwd", snake);
        let rev = format_ident!("{}_rev", snake);

        snapshot_fields.push(quote! { #fwd: self.#fwd.clone() });

        from_snapshot_fields.push(quote! { #fwd: snapshot.#fwd.clone() });

        // Rebuild reverse map from forward map
        from_snapshot_fields.push(quote! {
            #rev: {
                let mut rev_map = std::collections::BTreeMap::new();
                for (src, targets) in &snapshot.#fwd {
                    for tgt in targets {
                        rev_map.entry(*tgt)
                            .or_insert_with(std::collections::BTreeSet::new)
                            .insert(*src);
                    }
                }
                rev_map
            }
        });
    }

    // Last delivered values
    for inp in &def.inputs {
        let f = last_field(inp);
        snapshot_fields.push(quote! { #f: self.#f.clone() });
        from_snapshot_fields.push(quote! { #f: snapshot.#f.clone() });
    }

    // ID allocator counters
    snapshot_fields.push(quote! { id_alloc_counters: ::distvirt_sm_router::IdAllocator::<NodeKind>::counter_snapshot(&self.id_alloc) });
    from_snapshot_fields.push(quote! { id_alloc: <__IdAlloc as ::distvirt_sm_router::IdAllocator<NodeKind>>::from_counter_snapshot(snapshot.id_alloc_counters.clone()) });

    // Transient fields initialized to empty in from_snapshot
    let mut transient_inits = Vec::new();
    transient_inits.push(quote! { pending_creates: Vec::new() });
    transient_inits.push(quote! { dirty: std::collections::VecDeque::new() });
    transient_inits.push(quote! { pending_events: std::collections::VecDeque::new() });

    // Port pending input queues
    for port in &def.ports {
        let has_inputs = def.inputs.iter().any(|inp| inp.node == port.name);
        if has_inputs {
            let f = format_ident!("{}_pending_inputs", to_snake_case(&port.name.to_string()));
            transient_inits.push(quote! { #f: Vec::new() });
        }
    }

    quote! {
        #[allow(dead_code)]
        impl<__Tracer: ::distvirt_sm_router::trace::Tracer, __IdAlloc: ::distvirt_sm_router::IdAllocator<NodeKind>> Router<__Tracer, __IdAlloc> {
            pub fn snapshot(&self) -> RouterSnapshot {
                RouterSnapshot {
                    #(#snapshot_fields,)*
                }
            }

            pub fn from_snapshot_traced(snapshot: &RouterSnapshot, depth_limit: usize, tracer: __Tracer) -> Self {
                Router {
                    #(#from_snapshot_fields,)*
                    #(#transient_inits,)*
                    depth_limit,
                    tracer,
                }
            }
        }
    }
}
