use crate::parse::*;
use proc_macro2::TokenStream;
use quote::{format_ident, quote};
use syn::Ident;

pub(super) fn to_snake_case(s: &str) -> String {
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

pub(super) fn snake_ident(s: &str) -> Ident {
    format_ident!("{}", to_snake_case(s))
}

/// Returns the ID type tokens for a node. For auto-ID nodes, generates `{Name}Id`.
pub(super) fn node_id_type(def: &TopologyDef, node: &Ident) -> TokenStream {
    if let Some(sm) = def.state_machines.iter().find(|s| s.name == *node) {
        sm_id_type(sm)
    } else if let Some(port) = def.ports.iter().find(|p| p.name == *node) {
        port_id_type(port)
    } else {
        panic!("unknown node: {}", node)
    }
}

pub(super) fn sm_id_type(sm: &SmDef) -> TokenStream {
    match &sm.id_type {
        Some(ty) => quote! { #ty },
        None => {
            let id = format_ident!("{}Id", sm.name);
            quote! { #id }
        }
    }
}

pub(super) fn port_id_type(port: &PortDef) -> TokenStream {
    match &port.id_type {
        Some(ty) => quote! { #ty },
        None => {
            let id = format_ident!("{}Id", port.name);
            quote! { #id }
        }
    }
}

pub(super) fn is_sm_node(def: &TopologyDef, node: &Ident) -> bool {
    def.state_machines.iter().any(|s| s.name == *node)
}

pub(super) fn signal_field(sig: &SignalDef) -> Ident {
    format_ident!(
        "{}_{}",
        to_snake_case(&sig.node.to_string()),
        to_snake_case(&sig.signal.to_string())
    )
}

pub(super) fn edge_snake(edge: &EdgeDef) -> String {
    to_snake_case(&edge.name.to_string())
}

pub(super) fn last_field(inp: &InputDef) -> Ident {
    format_ident!(
        "last_{}_{}",
        to_snake_case(&inp.node.to_string()),
        to_snake_case(&inp.input_name.to_string())
    )
}

pub(super) fn dirty_variant(inp: &InputDef) -> Ident {
    format_ident!("{}{}", inp.node, inp.input_name)
}

/// Returns the auto-ID index for a node, or None for manual-ID nodes.
/// SMs come first (in declaration order), then ports.
pub(super) fn auto_id_index(def: &TopologyDef, node_name: &Ident) -> Option<usize> {
    let mut idx = 0usize;
    for sm in &def.state_machines {
        if sm.id_type.is_none() {
            if sm.name == *node_name {
                return Some(idx);
            }
            idx += 1;
        }
    }
    for port in &def.ports {
        if port.id_type.is_none() {
            if port.name == *node_name {
                return Some(idx);
            }
            idx += 1;
        }
    }
    None
}

/// Returns a unique group index for a node (SM or port) for delivery grouping.
/// SMs are indexed first (in declaration order), then ports.
pub(super) fn node_group_index(def: &TopologyDef, node_name: &Ident) -> usize {
    for (i, sm) in def.state_machines.iter().enumerate() {
        if sm.name == *node_name {
            return i;
        }
    }
    for (i, port) in def.ports.iter().enumerate() {
        if port.name == *node_name {
            return def.state_machines.len() + i;
        }
    }
    panic!("unknown node for group_index: {}", node_name)
}

/// Total count of auto-ID nodes (SMs + ports).
pub(super) fn auto_id_count(def: &TopologyDef) -> usize {
    let sm_count = def
        .state_machines
        .iter()
        .filter(|s| s.id_type.is_none())
        .count();
    let port_count = def.ports.iter().filter(|p| p.id_type.is_none()).count();
    sm_count + port_count
}
