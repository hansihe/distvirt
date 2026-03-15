mod ctx;
mod enums;
mod helpers;
mod router;
mod snapshot;

use crate::parse::*;
use proc_macro2::TokenStream;
use quote::quote;

pub fn generate(def: &TopologyDef) -> TokenStream {
    let auto_id_types = enums::gen_auto_id_types(def);
    let node_kind_enum = enums::gen_node_kind_enum(def);
    let signal_bounds = enums::gen_signal_bound_checks(def);
    let source_enums = enums::gen_source_enums(def);
    let input_enums = enums::gen_input_enums(def);
    let port_input_enums = enums::gen_port_input_enums(def);
    let ctx_structs = ctx::gen_ctx_structs(def);
    let dirty_enum = enums::gen_dirty_enum(def);
    let pending_create_enum = enums::gen_pending_create_enum(def);
    let pending_event_enum = enums::gen_pending_event_enum(def);
    let router_module = router::gen_router_module(def);

    quote! {
        #auto_id_types
        #node_kind_enum
        #signal_bounds
        #source_enums
        #input_enums
        #port_input_enums
        #ctx_structs
        #dirty_enum
        #pending_create_enum
        #pending_event_enum
        #router_module
    }
}
