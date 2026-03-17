use crate::parse::*;
use proc_macro2::TokenStream;
use quote::{format_ident, quote};

use super::helpers::*;

/// Generate compile-time checks that signal value types implement PartialEq and Debug.
/// Produces clear error messages pointing at the user's type rather than generated code.
pub(super) fn gen_signal_bound_checks(def: &TopologyDef) -> TokenStream {
    let checks: Vec<_> = def
        .signals
        .iter()
        .map(|sig| {
            let vt = &sig.value_type;
            let eq_fn = format_ident!(
                "__assert_signal_partial_eq_{}_{}",
                to_snake_case(&sig.node.to_string()),
                to_snake_case(&sig.signal.to_string())
            );
            let dbg_fn = format_ident!(
                "__assert_signal_debug_{}_{}",
                to_snake_case(&sig.node.to_string()),
                to_snake_case(&sig.signal.to_string())
            );
            quote! {
                #[doc(hidden)]
                const fn #eq_fn<T: PartialEq>() {}
                const _: () = #eq_fn::<#vt>();
                #[doc(hidden)]
                const fn #dbg_fn<T: std::fmt::Debug>() {}
                const _: () = #dbg_fn::<#vt>();
            }
        })
        .collect();

    quote! { #(#checks)* }
}

/// Generate newtype ID structs for auto-ID nodes.
pub(super) fn gen_auto_id_types(def: &TopologyDef) -> TokenStream {
    let mut types = Vec::new();

    for sm in &def.state_machines {
        if sm.id_type.is_none() {
            let id_name = format_ident!("{}Id", sm.name);
            types.push(quote! {
                #[derive(Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug)]
                #[allow(dead_code)]
                pub struct #id_name(pub u64);
            });
        }
    }

    for port in &def.ports {
        if port.id_type.is_none() {
            let id_name = format_ident!("{}Id", port.name);
            types.push(quote! {
                #[derive(Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug)]
                #[allow(dead_code)]
                pub struct #id_name(pub u64);
            });
        }
    }

    quote! { #(#types)* }
}

pub(super) fn gen_source_enums(def: &TopologyDef) -> TokenStream {
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
                pub enum #enum_name {
                    #(#variants,)*
                }
            }
        })
        .collect();

    quote! { #(#enums)* }
}

pub(super) fn gen_input_enums(def: &TopologyDef) -> TokenStream {
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
                    let agg = inp.aggregator.ty();
                    if inp.aggregator.is_incremental() {
                        quote! {
                            #variant(<#agg as ::distvirt_sm_router::IncrementalAggregator>::Output)
                        }
                    } else {
                        quote! {
                            #variant(<#agg as ::distvirt_sm_router::Aggregator>::Output)
                        }
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
                #[derive(Debug, Clone, PartialEq)]
                #[allow(dead_code)]
                pub enum #enum_name {
                    #(#input_variants,)*
                    #(#event_variants,)*
                }
            }
        })
        .collect();

    quote! { #(#enums)* }
}

pub(super) fn gen_port_input_enums(def: &TopologyDef) -> TokenStream {
    let enums: Vec<_> = def
        .ports
        .iter()
        .filter(|port| def.inputs.iter().any(|inp| inp.node == port.name))
        .map(|port| {
            let enum_name = format_ident!("{}PortInput", port.name);

            let input_variants: Vec<_> = def
                .inputs
                .iter()
                .filter(|inp| inp.node == port.name)
                .map(|inp| {
                    let variant = &inp.input_name;
                    let agg = inp.aggregator.ty();
                    if inp.aggregator.is_incremental() {
                        quote! {
                            #variant(<#agg as ::distvirt_sm_router::IncrementalAggregator>::Output)
                        }
                    } else {
                        quote! {
                            #variant(<#agg as ::distvirt_sm_router::Aggregator>::Output)
                        }
                    }
                })
                .collect();

            quote! {
                #[derive(Debug, Clone, PartialEq)]
                #[allow(dead_code)]
                pub enum #enum_name {
                    #(#input_variants,)*
                }
            }
        })
        .collect();

    quote! { #(#enums)* }
}

pub(super) fn gen_node_kind_enum(def: &TopologyDef) -> TokenStream {
    let auto_count = auto_id_count(def);
    let total_count = def.state_machines.len() + def.ports.len();

    // Build variants: auto-ID first (SMs then ports), then manual-ID (SMs then ports)
    let mut variants = Vec::new();
    let mut match_arms = Vec::new();
    let mut idx = 0usize;

    // Auto-ID SMs
    for sm in &def.state_machines {
        if sm.id_type.is_none() {
            let name = &sm.name;
            let name_str = name.to_string();
            variants.push(quote! { #name = #idx });
            match_arms.push(quote! { NodeKind::#name => #name_str });
            idx += 1;
        }
    }
    // Auto-ID ports
    for port in &def.ports {
        if port.id_type.is_none() {
            let name = &port.name;
            let name_str = name.to_string();
            variants.push(quote! { #name = #idx });
            match_arms.push(quote! { NodeKind::#name => #name_str });
            idx += 1;
        }
    }
    // Manual-ID SMs
    for sm in &def.state_machines {
        if sm.id_type.is_some() {
            let name = &sm.name;
            let name_str = name.to_string();
            variants.push(quote! { #name = #idx });
            match_arms.push(quote! { NodeKind::#name => #name_str });
            idx += 1;
        }
    }
    // Manual-ID ports
    for port in &def.ports {
        if port.id_type.is_some() {
            let name = &port.name;
            let name_str = name.to_string();
            variants.push(quote! { #name = #idx });
            match_arms.push(quote! { NodeKind::#name => #name_str });
            idx += 1;
        }
    }

    quote! {
        #[derive(Copy, Clone, Debug)]
        #[repr(usize)]
        #[allow(dead_code)]
        pub enum NodeKind {
            #(#variants,)*
        }

        impl NodeKind {
            pub const AUTO_COUNT: usize = #auto_count;
            pub const COUNT: usize = #total_count;
        }

        impl ::distvirt_sm_router::IdKind for NodeKind {
            const AUTO_COUNT: usize = #auto_count;
            const COUNT: usize = #total_count;
            fn index(self) -> usize { self as usize }
            fn name(self) -> &'static str {
                match self {
                    #(#match_arms,)*
                }
            }
        }
    }
}

pub(super) fn gen_dirty_enum(def: &TopologyDef) -> TokenStream {
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

pub(super) fn gen_pending_create_enum(def: &TopologyDef) -> TokenStream {
    let clone_derive = if def.model_checkable {
        quote! { #[derive(Clone)] }
    } else {
        quote! {}
    };

    if def.state_machines.is_empty() {
        return quote! {
            #clone_derive
            #[allow(dead_code)]
            pub enum PendingCreate {}
        };
    }

    let variants: Vec<_> = def
        .state_machines
        .iter()
        .map(|sm| {
            let variant = &sm.name;
            let id = sm_id_type(sm);
            let handler = &sm.handler_type;
            quote! { #variant(#id, #handler) }
        })
        .collect();

    quote! {
        #clone_derive
        #[allow(dead_code)]
        pub enum PendingCreate {
            #(#variants,)*
        }
    }
}

pub(super) fn gen_pending_delivery_enum(def: &TopologyDef) -> TokenStream {
    // Build group_key match arms for DirtyInput
    let dirty_group_arms: Vec<_> = def
        .inputs
        .iter()
        .map(|inp| {
            let variant = dirty_variant(inp);
            let idx = node_group_index(def, &inp.node);
            quote! { DirtyInput::#variant(_) => #idx }
        })
        .collect();

    // Build group_key match arms for PendingEvent
    let event_group_arms: Vec<_> = def
        .events
        .iter()
        .map(|ev| {
            let variant = &ev.name;
            let idx = node_group_index(def, &ev.receiver);
            quote! { PendingEvent::#variant(_, _, _) => #idx }
        })
        .collect();

    // DirtyInput::group_key() — may be empty if no inputs
    let dirty_group_key = if dirty_group_arms.is_empty() {
        quote! {
            impl DirtyInput {
                fn group_key(&self) -> usize {
                    match *self {}
                }
            }
        }
    } else {
        quote! {
            impl DirtyInput {
                fn group_key(&self) -> usize {
                    match self {
                        #(#dirty_group_arms,)*
                    }
                }
            }
        }
    };

    // PendingEvent::group_key() — may be empty if no events
    let event_group_key = if event_group_arms.is_empty() {
        quote! {
            impl PendingEvent {
                fn group_key(&self) -> usize {
                    match *self {}
                }
            }
        }
    } else {
        quote! {
            impl PendingEvent {
                fn group_key(&self) -> usize {
                    match self {
                        #(#event_group_arms,)*
                    }
                }
            }
        }
    };

    let clone_derive = if def.model_checkable {
        quote! { , Clone }
    } else {
        quote! {}
    };

    quote! {
        #[derive(Debug #clone_derive)]
        #[allow(dead_code)]
        pub enum PendingDelivery {
            DirtyInput(DirtyInput),
            Event(PendingEvent),
        }

        #dirty_group_key
        #event_group_key

        impl ::distvirt_sm_router::Delivery for PendingDelivery {
            fn group_key(&self) -> usize {
                match self {
                    PendingDelivery::DirtyInput(d) => d.group_key(),
                    PendingDelivery::Event(e) => e.group_key(),
                }
            }
        }
    }
}

pub(super) fn gen_pending_event_enum(def: &TopologyDef) -> TokenStream {
    let clone_derive = if def.model_checkable {
        quote! { , Clone }
    } else {
        quote! {}
    };

    if def.events.is_empty() {
        return quote! {
            #[derive(Debug #clone_derive)]
            #[allow(dead_code)]
            pub enum PendingEvent {}
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
        #[derive(Debug #clone_derive)]
        #[allow(dead_code)]
        pub enum PendingEvent {
            #(#variants,)*
        }
    }
}
