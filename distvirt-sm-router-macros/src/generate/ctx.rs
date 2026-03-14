use crate::parse::*;
use proc_macro2::TokenStream;
use quote::{format_ident, quote};

use super::helpers::*;

pub(super) fn gen_ctx_structs(def: &TopologyDef) -> TokenStream {
    let structs: Vec<_> = def
        .state_machines
        .iter()
        .map(|sm| {
            let ctx_trait_name = format_ident!("{}Ctx", sm.name);
            let ctx_concrete_name = format_ident!("{}CtxConcrete", sm.name);
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

            // ================================================================
            // Trait definition
            // ================================================================

            let trait_signal_setters: Vec<_> = signals
                .iter()
                .map(|sig| {
                    let method =
                        format_ident!("set_{}", to_snake_case(&sig.signal.to_string()));
                    let vt = &sig.value_type;
                    quote! {
                        fn #method(&mut self, value: #vt);
                    }
                })
                .collect();

            let trait_edge_setters: Vec<_> = out_edges
                .iter()
                .map(|edge| {
                    let method = format_ident!("set_{}_edges", edge_snake(edge));
                    let tgt_id = node_id_type(def, &edge.target);
                    quote! {
                        fn #method(&mut self, targets: Vec<#tgt_id>);
                    }
                })
                .collect();

            let trait_event_senders: Vec<_> = out_events
                .iter()
                .map(|ev| {
                    let method = format_ident!("send_{}", to_snake_case(&ev.name.to_string()));
                    let receiver_id = node_id_type(def, &ev.receiver);
                    let payload = &ev.payload_type;
                    quote! {
                        fn #method(&mut self, target: #receiver_id, payload: #payload);
                    }
                })
                .collect();

            let trait_create_methods: Vec<_> = def
                .state_machines
                .iter()
                .map(|target_sm| {
                    let target_snake = to_snake_case(&target_sm.name.to_string());
                    let method = format_ident!("create_{}", target_snake);
                    let tid = sm_id_type(target_sm);
                    let handler = &target_sm.handler_type;

                    if target_sm.id_type.is_none() {
                        quote! {
                            fn #method(&mut self, sm: #handler) -> #tid;
                        }
                    } else {
                        quote! {
                            fn #method(&mut self, id: #tid, sm: #handler);
                        }
                    }
                })
                .collect();

            let trait_destroy_methods: Vec<TokenStream> = vec![
                quote! {
                    fn self_destruct(&mut self);
                },
            ];

            // ================================================================
            // Concrete struct fields
            // ================================================================

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

            let event_fields: Vec<_> = vec![
                quote! { pending_events: Vec<PendingEvent> },
            ];

            let create_fields: Vec<_> = vec![
                quote! { pending_creates: Vec<PendingCreate> },
            ];

            let destroy_fields: Vec<TokenStream> = vec![
                quote! { pending_self_destruct: bool },
            ];

            let counter_fields: Vec<_> = def
                .state_machines
                .iter()
                .filter(|s| s.id_type.is_none())
                .map(|target_sm| {
                    let f = format_ident!(
                        "next_{}_id",
                        to_snake_case(&target_sm.name.to_string())
                    );
                    quote! { #f: u64 }
                })
                .collect();

            // ================================================================
            // Constructor
            // ================================================================

            let signal_inits: Vec<_> = signals
                .iter()
                .map(|sig| {
                    let f = format_ident!("{}", to_snake_case(&sig.signal.to_string()));
                    quote! { #f: None }
                })
                .collect();

            let event_inits: Vec<_> = vec![
                quote! { pending_events: Vec::new() },
            ];

            let edge_inits: Vec<_> = out_edges
                .iter()
                .map(|edge| {
                    let f = format_ident!("{}", edge_snake(edge));
                    quote! { #f: None }
                })
                .collect();

            let create_inits: Vec<_> = vec![
                quote! { pending_creates: Vec::new() },
            ];

            let destroy_inits: Vec<TokenStream> = vec![
                quote! { pending_self_destruct: false },
            ];

            let counter_params: Vec<_> = def
                .state_machines
                .iter()
                .filter(|s| s.id_type.is_none())
                .map(|target_sm| {
                    let f = format_ident!(
                        "next_{}_id",
                        to_snake_case(&target_sm.name.to_string())
                    );
                    quote! { #f: u64 }
                })
                .collect();

            let counter_inits: Vec<_> = def
                .state_machines
                .iter()
                .filter(|s| s.id_type.is_none())
                .map(|target_sm| {
                    let f = format_ident!(
                        "next_{}_id",
                        to_snake_case(&target_sm.name.to_string())
                    );
                    quote! { #f }
                })
                .collect();

            // ================================================================
            // Trait impl for concrete struct
            // ================================================================

            let impl_signal_setters: Vec<_> = signals
                .iter()
                .map(|sig| {
                    let field =
                        format_ident!("{}", to_snake_case(&sig.signal.to_string()));
                    let method =
                        format_ident!("set_{}", to_snake_case(&sig.signal.to_string()));
                    let vt = &sig.value_type;
                    quote! {
                        fn #method(&mut self, value: #vt) {
                            self.#field = Some(value);
                        }
                    }
                })
                .collect();

            let impl_edge_setters: Vec<_> = out_edges
                .iter()
                .map(|edge| {
                    let method = format_ident!("set_{}_edges", edge_snake(edge));
                    let field = format_ident!("{}", edge_snake(edge));
                    let tgt_id = node_id_type(def, &edge.target);
                    quote! {
                        fn #method(&mut self, targets: Vec<#tgt_id>) {
                            self.#field = Some(targets);
                        }
                    }
                })
                .collect();

            let impl_event_senders: Vec<_> = out_events
                .iter()
                .map(|ev| {
                    let method = format_ident!("send_{}", to_snake_case(&ev.name.to_string()));
                    let receiver_id = node_id_type(def, &ev.receiver);
                    let payload = &ev.payload_type;
                    let variant = &ev.name;
                    quote! {
                        fn #method(&mut self, target: #receiver_id, payload: #payload) {
                            self.pending_events.push(PendingEvent::#variant(self.id, target, payload));
                        }
                    }
                })
                .collect();

            let impl_create_methods: Vec<_> = def
                .state_machines
                .iter()
                .map(|target_sm| {
                    let target_snake = to_snake_case(&target_sm.name.to_string());
                    let method = format_ident!("create_{}", target_snake);
                    let tid = sm_id_type(target_sm);
                    let handler = &target_sm.handler_type;
                    let variant = &target_sm.name;

                    if target_sm.id_type.is_none() {
                        let id_name = format_ident!("{}Id", target_sm.name);
                        let counter = format_ident!("next_{}_id", target_snake);
                        quote! {
                            fn #method(&mut self, sm: #handler) -> #tid {
                                let id = #id_name(self.#counter);
                                self.#counter += 1;
                                self.pending_creates.push(PendingCreate::#variant(id, sm));
                                id
                            }
                        }
                    } else {
                        quote! {
                            fn #method(&mut self, id: #tid, sm: #handler) {
                                self.pending_creates.push(PendingCreate::#variant(id, sm));
                            }
                        }
                    }
                })
                .collect();

            let impl_destroy_methods: Vec<TokenStream> = vec![
                quote! {
                    fn self_destruct(&mut self) {
                        self.pending_self_destruct = true;
                    }
                },
            ];

            quote! {
                // Trait — public API for SM handlers
                #[allow(dead_code)]
                trait #ctx_trait_name {
                    fn id(&self) -> #id_type;
                    #(#trait_signal_setters)*
                    #(#trait_edge_setters)*
                    #(#trait_event_senders)*
                    #(#trait_create_methods)*
                    #(#trait_destroy_methods)*
                }

                // Concrete struct — used internally by the router
                #[allow(dead_code)]
                struct #ctx_concrete_name {
                    #[allow(dead_code)]
                    id: #id_type,
                    #(#signal_fields,)*
                    #(#edge_fields,)*
                    #(#event_fields,)*
                    #(#create_fields,)*
                    #(#destroy_fields,)*
                    #(#counter_fields,)*
                }

                #[allow(dead_code)]
                impl #ctx_concrete_name {
                    fn new(id: #id_type #(, #counter_params)*) -> Self {
                        #ctx_concrete_name {
                            id,
                            #(#signal_inits,)*
                            #(#edge_inits,)*
                            #(#event_inits,)*
                            #(#create_inits,)*
                            #(#destroy_inits,)*
                            #(#counter_inits,)*
                        }
                    }
                }

                // Trait impl for concrete struct
                #[allow(dead_code)]
                impl #ctx_trait_name for #ctx_concrete_name {
                    fn id(&self) -> #id_type {
                        self.id
                    }

                    #(#impl_signal_setters)*
                    #(#impl_edge_setters)*
                    #(#impl_event_senders)*
                    #(#impl_create_methods)*
                    #(#impl_destroy_methods)*
                }
            }
        })
        .collect();

    quote! { #(#structs)* }
}
