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
                enum #enum_name {
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
                    let agg = &inp.aggregator;
                    quote! {
                        #variant(<#agg as crate::Aggregator>::Output)
                    }
                })
                .collect();

            quote! {
                #[derive(Debug, PartialEq)]
                #[allow(dead_code)]
                enum #enum_name {
                    #(#input_variants,)*
                }
            }
        })
        .collect();

    quote! { #(#enums)* }
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

pub(super) fn gen_pending_event_enum(def: &TopologyDef) -> TokenStream {
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
