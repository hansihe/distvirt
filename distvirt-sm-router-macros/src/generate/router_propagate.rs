use crate::parse::*;
use proc_macro2::TokenStream;
use quote::{ToTokens, format_ident, quote};

use super::helpers::*;

/// Generate connectivity check code for an event: returns a TokenStream that
/// evaluates to `bool` — true if any edge connects `sender_id` and `receiver_id`
/// in either direction.
pub(super) fn gen_connectivity_check(def: &TopologyDef, ev: &EventDef) -> TokenStream {
    let checks: Vec<_> = def
        .edges
        .iter()
        .filter(|e| {
            (e.source == ev.sender && e.target == ev.receiver)
                || (e.source == ev.receiver && e.target == ev.sender)
        })
        .map(|e| {
            let rev = format_ident!("{}_rev", edge_snake(e));
            if e.source == ev.sender && e.target == ev.receiver {
                // sender is source: check rev map (O(log n) BTreeSet lookup)
                quote! {
                    if let Some(sources) = self.#rev.get(&receiver_id) {
                        if sources.contains(&sender_id) { return true; }
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

pub(super) fn gen_aggregate_methods(def: &TopologyDef, methods: &mut Vec<TokenStream>) {
    for inp in &def.inputs {
        let method = format_ident!(
            "aggregate_{}_{}",
            to_snake_case(&inp.node.to_string()),
            to_snake_case(&inp.input_name.to_string())
        );
        let target_id_type = node_id_type(def, &inp.node);
        let agg = inp.aggregator.ty();
        let multi_source = inp.sources.len() >= 2;

        if inp.aggregator.is_incremental() {
            // Incremental aggregator: diff against prev maps, produce Vec of outputs
            let node_state = signal_state_field(&inp.node);

            let diff_blocks: Vec<_> = inp.sources.iter().map(|sp| {
                let rev = format_ident!("{}_rev", to_snake_case(&sp.edge.to_string()));
                let source_node_state = signal_state_field(&sp.node);
                let out_f = out_field_name(&sp.signal);
                let prev_f = prev_field_name(inp, sp);
                let src_id_type = node_id_type(def, &sp.node);
                let sig = def.signals.iter().find(|s| s.node == sp.node && s.signal == sp.signal).unwrap();
                let vt = &sig.value_type;

                // Build the input expression for agg calls — wrap in enum variant for multi-source
                let make_input = if multi_source {
                    let enum_name = format_ident!("{}Source", inp.input_name);
                    let variant_name = format_ident!("{}{}", sp.node, sp.signal);
                    quote! { #enum_name::#variant_name }
                } else {
                    quote! {} // will be used as tuple constructor
                };

                if multi_source {
                    quote! {
                        {
                            let mut current: std::collections::BTreeMap<#src_id_type, #vt> = std::collections::BTreeMap::new();
                            if let Some(sources) = self.#rev.get(&target_id) {
                                for &source_id in sources {
                                    if let Some(state) = self.#source_node_state.get(&source_id) {
                                        current.insert(source_id, state.#out_f.clone());
                                    }
                                }
                            }

                            let prev = &self.#node_state.get(&target_id).unwrap().#prev_f;

                            // Added or changed
                            for (id, val) in &current {
                                match prev.get(id) {
                                    None => {
                                        let input = #make_input(*id, val.clone());
                                        if let Some(out) = agg.added(&input) { outputs.push(out); }
                                    }
                                    Some(old) if old != val => {
                                        let old_input = #make_input(*id, old.clone());
                                        let new_input = #make_input(*id, val.clone());
                                        if let Some(out) = agg.changed(&old_input, &new_input) { outputs.push(out); }
                                    }
                                    _ => {} // unchanged
                                }
                            }
                            // Removed
                            for (id, val) in prev {
                                if !current.contains_key(id) {
                                    let input = #make_input(*id, val.clone());
                                    if let Some(out) = agg.removed(&input) { outputs.push(out); }
                                }
                            }

                            // Update prev
                            self.#node_state.get_mut(&target_id).unwrap().#prev_f = current;
                        }
                    }
                } else {
                    quote! {
                        {
                            let mut current: std::collections::BTreeMap<#src_id_type, #vt> = std::collections::BTreeMap::new();
                            if let Some(sources) = self.#rev.get(&target_id) {
                                for &source_id in sources {
                                    if let Some(state) = self.#source_node_state.get(&source_id) {
                                        current.insert(source_id, state.#out_f.clone());
                                    }
                                }
                            }

                            let prev = &self.#node_state.get(&target_id).unwrap().#prev_f;

                            // Added or changed
                            for (id, val) in &current {
                                match prev.get(id) {
                                    None => {
                                        if let Some(out) = agg.added(&(*id, val.clone())) { outputs.push(out); }
                                    }
                                    Some(old) if old != val => {
                                        if let Some(out) = agg.changed(&(*id, old.clone()), &(*id, val.clone())) { outputs.push(out); }
                                    }
                                    _ => {} // unchanged
                                }
                            }
                            // Removed
                            for (id, val) in prev {
                                if !current.contains_key(id) {
                                    if let Some(out) = agg.removed(&(*id, val.clone())) { outputs.push(out); }
                                }
                            }

                            // Update prev
                            self.#node_state.get_mut(&target_id).unwrap().#prev_f = current;
                        }
                    }
                }
            }).collect();

            methods.push(quote! {
                fn #method(&mut self, target_id: #target_id_type) -> Vec<<#agg as ::distvirt_sm_router::IncrementalAggregator>::Output> {
                    let agg = <#agg as Default>::default();
                    let mut outputs = Vec::new();
                    #(#diff_blocks)*
                    outputs
                }
            });
        } else {
            // Batch aggregator: existing logic
            let collect_code: Vec<_> = inp
                .sources
                .iter()
                .map(|sp| {
                    let rev = format_ident!("{}_rev", to_snake_case(&sp.edge.to_string()));
                    let source_node_state = signal_state_field(&sp.node);
                    let out_f = out_field_name(&sp.signal);
                    if multi_source {
                        let enum_name = format_ident!("{}Source", inp.input_name);
                        let variant_name = format_ident!("{}{}", sp.node, sp.signal);
                        quote! {
                            if let Some(sources) = self.#rev.get(&target_id) {
                                for &source_id in sources {
                                    if let Some(state) = self.#source_node_state.get(&source_id) {
                                        inputs.push(#enum_name::#variant_name(source_id, state.#out_f.clone()));
                                    }
                                }
                            }
                        }
                    } else {
                        quote! {
                            if let Some(sources) = self.#rev.get(&target_id) {
                                for &source_id in sources {
                                    if let Some(state) = self.#source_node_state.get(&source_id) {
                                        inputs.push((source_id, state.#out_f.clone()));
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
                fn #method(&self, target_id: #target_id_type) -> <#agg as ::distvirt_sm_router::Aggregator>::Output {
                    let mut inputs: #vec_type = Vec::new();
                    #(#collect_code)*
                    <#agg as Default>::default().aggregate(&inputs)
                }
            });
        }
    }
}

pub(super) fn gen_apply_effects(def: &TopologyDef, methods: &mut Vec<TokenStream>) {
    for sm in &def.state_machines {
        let method = format_ident!("apply_{}_effects", to_snake_case(&sm.name.to_string()));
        let effects_name = format_ident!("{}Effects", sm.name);
        let id_type = sm_id_type(sm);
        let node_str = sm.name.to_string();

        // 1. Accumulate creates into pending_creates (materialized end-of-round)
        let create_apply = quote! {
            self.pending_creates.extend(effects.pending_creates);
        };

        // 2. Apply signals
        let signal_applies: Vec<_> = def
            .signals
            .iter()
            .filter(|s| s.node == sm.name)
            .map(|sig| {
                let ctx_field = format_ident!("{}", to_snake_case(&sig.signal.to_string()));
                let setter = format_ident!(
                    "set_{}_{}",
                    to_snake_case(&sm.name.to_string()),
                    to_snake_case(&sig.signal.to_string())
                );
                quote! {
                    if let Some(value) = effects.#ctx_field {
                        self.#setter(id, value);
                    }
                }
            })
            .collect();

        // 3. Apply edges (may reference newly created SMs)
        let edge_applies: Vec<_> = def
            .edges
            .iter()
            .filter(|e| e.source == sm.name)
            .map(|edge| {
                let ctx_field = format_ident!("{}", edge_snake(edge));
                let setter = format_ident!("set_{}_edges", edge_snake(edge));
                quote! {
                    if let Some(targets) = effects.#ctx_field {
                        self.#setter(id, targets);
                    }
                }
            })
            .collect();

        // 4. Queue events - trace each one, then extend
        let event_trace_arms: Vec<_> = def
            .events
            .iter()
            .filter(|ev| ev.sender == sm.name)
            .map(|ev| {
                let variant = &ev.name;
                let event_str = ev.name.to_string();
                quote! {
                    PendingEvent::#variant(sender_id, receiver_id, payload) => {
                        self.tracer.trace(::distvirt_sm_router::trace::TraceEvent::EventQueued {
                            event: #event_str,
                            sender: ::distvirt_sm_router::trace::DebugValue::Borrowed(sender_id as &(dyn std::fmt::Debug + Send + Sync)),
                            receiver: ::distvirt_sm_router::trace::DebugValue::Borrowed(receiver_id as &(dyn std::fmt::Debug + Send + Sync)),
                            payload: ::distvirt_sm_router::trace::DebugValue::Borrowed(payload as &(dyn std::fmt::Debug + Send + Sync)),
                        });
                    }
                }
            })
            .collect();

        // Check if this SM sends any events at all
        let has_outgoing_events = def.events.iter().any(|ev| ev.sender == sm.name);

        let event_apply = if has_outgoing_events {
            quote! {
                for event in &effects.pending_events {
                    match event {
                        #(#event_trace_arms)*
                        _ => {}
                    }
                }
                self.pending_events.extend(effects.pending_events);
            }
        } else {
            quote! {
                self.pending_events.extend(effects.pending_events);
            }
        };

        // 5. Self-destruct
        let destroy_method = format_ident!("destroy_{}", to_snake_case(&sm.name.to_string()));
        let self_destruct_apply = quote! {
            if effects.pending_self_destruct {
                self.#destroy_method(id);
            }
        };

        methods.push(quote! {
            fn #method(&mut self, id: #id_type, effects: #effects_name) -> bool {
                self.tracer.trace(::distvirt_sm_router::trace::TraceEvent::EffectsStart {
                    node: #node_str,
                    id: ::distvirt_sm_router::trace::DebugValue::Borrowed(&id as &(dyn std::fmt::Debug + Send + Sync)),
                });
                #create_apply
                #(#signal_applies)*
                #(#edge_applies)*
                #event_apply
                if effects.pending_self_destruct {
                    self.tracer.trace(::distvirt_sm_router::trace::TraceEvent::SmDestroyed {
                        node: #node_str,
                        id: ::distvirt_sm_router::trace::DebugValue::Borrowed(&id as &(dyn std::fmt::Debug + Send + Sync)),
                    });
                }
                #self_destruct_apply
                self.tracer.trace(::distvirt_sm_router::trace::TraceEvent::EffectsEnd {
                    node: #node_str,
                    id: ::distvirt_sm_router::trace::DebugValue::Borrowed(&id as &(dyn std::fmt::Debug + Send + Sync)),
                });
                effects.pending_self_destruct
            }
        });
    }
}

pub(super) fn gen_propagate(
    def: &TopologyDef,
    methods: &mut Vec<TokenStream>,
    internal_methods: &mut Vec<TokenStream>,
) {
    // Generate match arms for process_dirty_input (uses `return` instead of `continue`)
    let dirty_match_arms: Vec<_> = def
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
            let node_str = inp.node.to_string();
            let input_str = inp.input_name.to_string();

            if inp.aggregator.is_incremental() {
                // Incremental aggregator path
                if is_sm_node(def, &inp.node) {
                    let sm = def.state_machines.iter().find(|s| s.name == inp.node).unwrap();
                    let input_enum = format_ident!("{}Input", sm.name);
                    let ctx_name = format_ident!("{}CtxConcrete", sm.name);
                    let input_variant = &inp.input_name;
                    let apply = format_ident!("apply_{}_effects", to_snake_case(&inp.node.to_string()));

                    quote! {
                        DirtyInput::#variant(target_id) => {
                            if !self.instances.#instances.contains_key(&target_id) {
                                return;
                            }
                            let results = self.#aggregate(target_id);
                            for result in results {
                                self.tracer.trace(::distvirt_sm_router::trace::TraceEvent::InputDelivered {
                                    node: #node_str,
                                    id: ::distvirt_sm_router::trace::DebugValue::Borrowed(&target_id as &(dyn std::fmt::Debug + Send + Sync)),
                                    input: #input_str,
                                    value: ::distvirt_sm_router::trace::DebugValue::Borrowed(&result as &(dyn std::fmt::Debug + Send + Sync)),
                                });
                                let effects = {
                                    let sm = self.instances.#instances.get_mut(&target_id).unwrap();
                                    let mut ctx = #ctx_name::new(target_id, &mut self.id_alloc);
                                    sm.handle(#input_enum::#input_variant(result), &mut ctx);
                                    ctx.into_effects()
                                };
                                let destroyed = self.#apply(target_id, effects);
                                if destroyed { return; }
                            }
                        }
                    }
                } else {
                    let port_input_enum = format_ident!("{}PortInput", inp.node);
                    let input_variant = &inp.input_name;
                    let pending_field = format_ident!("{}_pending_inputs", to_snake_case(&inp.node.to_string()));

                    quote! {
                        DirtyInput::#variant(target_id) => {
                            if !self.instances.#instances.contains(&target_id) {
                                return;
                            }
                            let results = self.#aggregate(target_id);
                            for result in results {
                                self.tracer.trace(::distvirt_sm_router::trace::TraceEvent::InputDelivered {
                                    node: #node_str,
                                    id: ::distvirt_sm_router::trace::DebugValue::Borrowed(&target_id as &(dyn std::fmt::Debug + Send + Sync)),
                                    input: #input_str,
                                    value: ::distvirt_sm_router::trace::DebugValue::Borrowed(&result as &(dyn std::fmt::Debug + Send + Sync)),
                                });
                                self.#pending_field.push((target_id, #port_input_enum::#input_variant(result)));
                            }
                        }
                    }
                }
            } else {
                // Batch aggregator path (existing logic)
                let node_state = signal_state_field(&inp.node);
                let in_f = in_field_name(&inp.input_name);

                if is_sm_node(def, &inp.node) {
                    let sm = def
                        .state_machines
                        .iter()
                        .find(|s| s.name == inp.node)
                        .unwrap();
                    let input_enum = format_ident!("{}Input", sm.name);
                    let ctx_name = format_ident!("{}CtxConcrete", sm.name);
                    let input_variant = &inp.input_name;
                    let apply = format_ident!(
                        "apply_{}_effects",
                        to_snake_case(&inp.node.to_string())
                    );

                    quote! {
                        DirtyInput::#variant(target_id) => {
                            if !self.instances.#instances.contains_key(&target_id) {
                                return;
                            }
                            let result = self.#aggregate(target_id);
                            if self.#node_state.get(&target_id).and_then(|s| s.#in_f.as_ref()) == Some(&result) {
                                self.tracer.trace(::distvirt_sm_router::trace::TraceEvent::InputSuppressed {
                                    node: #node_str,
                                    id: ::distvirt_sm_router::trace::DebugValue::Borrowed(&target_id as &(dyn std::fmt::Debug + Send + Sync)),
                                    input: #input_str,
                                });
                                return;
                            }
                            self.#node_state.get_mut(&target_id).unwrap().#in_f = Some(result.clone());

                            self.tracer.trace(::distvirt_sm_router::trace::TraceEvent::InputDelivered {
                                node: #node_str,
                                id: ::distvirt_sm_router::trace::DebugValue::Borrowed(&target_id as &(dyn std::fmt::Debug + Send + Sync)),
                                input: #input_str,
                                value: ::distvirt_sm_router::trace::DebugValue::Borrowed(&result as &(dyn std::fmt::Debug + Send + Sync)),
                            });

                            let effects = {
                                let sm = self.instances.#instances.get_mut(&target_id).unwrap();
                                let mut ctx = #ctx_name::new(target_id, &mut self.id_alloc);
                                sm.handle(#input_enum::#input_variant(result), &mut ctx);
                                ctx.into_effects()
                            };
                            self.#apply(target_id, effects);
                        }
                    }
                } else {
                    let port_input_enum = format_ident!("{}PortInput", inp.node);
                    let input_variant = &inp.input_name;
                    let pending_field = format_ident!("{}_pending_inputs", to_snake_case(&inp.node.to_string()));

                    quote! {
                        DirtyInput::#variant(target_id) => {
                            if !self.instances.#instances.contains(&target_id) {
                                return;
                            }
                            let result = self.#aggregate(target_id);
                            if self.#node_state.get(&target_id).and_then(|s| s.#in_f.as_ref()) == Some(&result) {
                                self.tracer.trace(::distvirt_sm_router::trace::TraceEvent::InputSuppressed {
                                    node: #node_str,
                                    id: ::distvirt_sm_router::trace::DebugValue::Borrowed(&target_id as &(dyn std::fmt::Debug + Send + Sync)),
                                    input: #input_str,
                                });
                                return;
                            }
                            self.#node_state.get_mut(&target_id).unwrap().#in_f = Some(result.clone());

                            self.tracer.trace(::distvirt_sm_router::trace::TraceEvent::InputDelivered {
                                node: #node_str,
                                id: ::distvirt_sm_router::trace::DebugValue::Borrowed(&target_id as &(dyn std::fmt::Debug + Send + Sync)),
                                input: #input_str,
                                value: ::distvirt_sm_router::trace::DebugValue::Borrowed(&result as &(dyn std::fmt::Debug + Send + Sync)),
                            });

                            self.#pending_field.push((target_id, #port_input_enum::#input_variant(result)));
                        }
                    }
                }
            }
        })
        .collect();

    // Generate event processing arms for process_event
    let event_match_arms: Vec<_> = def
        .events
        .iter()
        .map(|ev| {
            let variant = &ev.name;
            let instances =
                format_ident!("{}_instances", to_snake_case(&ev.receiver.to_string()));
            let input_enum = format_ident!("{}Input", ev.receiver);
            let ctx_name = format_ident!("{}CtxConcrete", ev.receiver);
            let apply = format_ident!(
                "apply_{}_effects",
                to_snake_case(&ev.receiver.to_string())
            );
            let event_str = ev.name.to_string();

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

                    if self.instances.#instances.contains_key(&receiver_id) {
                        self.tracer.trace(::distvirt_sm_router::trace::TraceEvent::EventDelivered {
                            event: #event_str,
                            sender: ::distvirt_sm_router::trace::DebugValue::Borrowed(&sender_id as &(dyn std::fmt::Debug + Send + Sync)),
                            receiver: ::distvirt_sm_router::trace::DebugValue::Borrowed(&receiver_id as &(dyn std::fmt::Debug + Send + Sync)),
                            payload: ::distvirt_sm_router::trace::DebugValue::Borrowed(&payload as &(dyn std::fmt::Debug + Send + Sync)),
                        });
                        let effects = {
                            let sm = self.instances.#instances.get_mut(&receiver_id).unwrap();
                            let mut ctx = #ctx_name::new(receiver_id, &mut self.id_alloc);
                            sm.handle(#input_enum::#variant(payload), &mut ctx);
                            ctx.into_effects()
                        };
                        self.#apply(receiver_id, effects);
                    }
                }
            }
        })
        .collect();

    let invariant_checks: Vec<TokenStream> = def
        .invariants
        .iter()
        .map(|inv| {
            let node_state = signal_state_field(&inv.node);
            let out_f = out_field_name(&inv.signal);
            let expr = &inv.expr;
            let node_str = inv.node.to_string();
            let signal_str = inv.signal.to_string();
            let expr_str = expr.to_token_stream().to_string();
            quote! {
                for (id, state) in &self.#node_state {
                    let value = &state.#out_f;
                    if !(#expr) {
                        self.tracer.trace(::distvirt_sm_router::trace::TraceEvent::InvariantViolation {
                            node: #node_str,
                            id: ::distvirt_sm_router::trace::DebugValue::Borrowed(id as &(dyn std::fmt::Debug + Send + Sync)),
                            signal: #signal_str,
                            value: ::distvirt_sm_router::trace::DebugValue::Borrowed(value as &(dyn std::fmt::Debug + Send + Sync)),
                            invariant_expr: #expr_str,
                        });
                    }
                }
            }
        })
        .collect();

    // Shared per-item methods (called by both propagate and deliver_one)
    internal_methods.push(quote! {
        fn process_dirty_input(&mut self, entry: DirtyInput) {
            match entry {
                #(#dirty_match_arms)*
            }
        }
    });

    internal_methods.push(quote! {
        fn process_event(&mut self, event: PendingEvent) {
            match event {
                #(#event_match_arms)*
            }
        }
    });

    internal_methods.push(quote! {
        fn check_invariants(&mut self) {
            #(#invariant_checks)*
        }
    });

    // Drain dirty queue, deduplicate into reusable dedup_wave buffer
    internal_methods.push(quote! {
        fn drain_and_dedup_dirty(&mut self) {
            self.dedup_seen.clear();
            self.dedup_wave.clear();
            for entry in self.dirty.drain(..) {
                if self.dedup_seen.insert(entry.clone()) {
                    self.dedup_wave.push(entry);
                }
            }
        }
    });

    // Eager propagation (production path)
    methods.push(quote! {
        fn propagate(&mut self) {
            match self.manual_phase {
                ::distvirt_sm_router::ManualPhase::Idle => {}
                ::distvirt_sm_router::ManualPhase::Events(0) => {
                    self.manual_phase = ::distvirt_sm_router::ManualPhase::Idle;
                }
                _ => panic!(
                    "propagate() called while step-by-step propagation is in progress (phase: {:?})",
                    self.manual_phase
                ),
            }

            self.tracer.trace(::distvirt_sm_router::trace::TraceEvent::PropagateStart);
            let mut depth = 0;

            while !self.dirty.is_empty() || !self.pending_events.is_empty()
                || self.has_pending_creates()
            {
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

                self.tracer.trace(::distvirt_sm_router::trace::TraceEvent::RoundStart { depth });

                // Start-of-round: materialize pending creates
                self.materialize_pending_creates();

                // Process dirty signal queue (reuse scratch buffers)
                self.drain_and_dedup_dirty();
                let mut wave = std::mem::take(&mut self.dedup_wave);
                for entry in wave.drain(..) {
                    self.process_dirty_input(entry);
                }
                self.dedup_wave = wave;

                // Process pending events (reuse scratch buffer)
                let mut events = std::mem::take(&mut self.event_wave);
                events.extend(self.pending_events.drain(..));
                for event in events.drain(..) {
                    self.process_event(event);
                }
                self.event_wave = events;

                self.tracer.trace(::distvirt_sm_router::trace::TraceEvent::RoundEnd { depth });
            }

            self.check_invariants();

            self.tracer.trace(::distvirt_sm_router::trace::TraceEvent::PropagateEnd { rounds: depth });
        }
    });

    // Step-by-step propagation API
    methods.push(quote! {
        fn begin_manual_propagate(&mut self) -> ::distvirt_sm_router::ManualPropagate<PendingDelivery> {
            match self.manual_phase {
                ::distvirt_sm_router::ManualPhase::Idle | ::distvirt_sm_router::ManualPhase::Events(0) => {
                    // Start new round: materialize creates, drain dirty inputs
                    self.materialize_pending_creates();
                    self.drain_and_dedup_dirty();
                    let deliveries: Vec<PendingDelivery> = self.dedup_wave.drain(..)
                        .map(PendingDelivery::DirtyInput)
                        .collect();
                    self.manual_phase = ::distvirt_sm_router::ManualPhase::Inputs(deliveries.len());
                    ::distvirt_sm_router::ManualPropagate::new(deliveries)
                }
                ::distvirt_sm_router::ManualPhase::Inputs(0) => {
                    // Inputs sub-round done, drain pending events
                    self.event_wave.clear();
                    self.event_wave.extend(self.pending_events.drain(..));
                    let deliveries: Vec<PendingDelivery> = self.event_wave.drain(..)
                        .map(PendingDelivery::Event)
                        .collect();
                    self.manual_phase = ::distvirt_sm_router::ManualPhase::Events(deliveries.len());
                    ::distvirt_sm_router::ManualPropagate::new(deliveries)
                }
                ::distvirt_sm_router::ManualPhase::Inputs(n) => {
                    panic!(
                        "begin_manual_propagate() called with {} input deliveries still outstanding",
                        n
                    );
                }
                ::distvirt_sm_router::ManualPhase::Events(n) => {
                    panic!(
                        "begin_manual_propagate() called with {} event deliveries still outstanding",
                        n
                    );
                }
            }
        }
    });

    methods.push(quote! {
        fn deliver_one(&mut self, delivery: PendingDelivery) {
            match &mut self.manual_phase {
                ::distvirt_sm_router::ManualPhase::Inputs(n) if *n > 0 => {
                    *n -= 1;
                }
                ::distvirt_sm_router::ManualPhase::Events(n) if *n > 0 => {
                    *n -= 1;
                }
                _ => panic!(
                    "deliver_one() called in invalid phase: {:?}",
                    self.manual_phase
                ),
            }
            match delivery {
                PendingDelivery::DirtyInput(entry) => {
                    self.process_dirty_input(entry);
                }
                PendingDelivery::Event(event) => {
                    self.process_event(event);
                }
            }
        }
    });

    methods.push(quote! {
        fn is_quiescent(&self) -> bool {
            match self.manual_phase {
                ::distvirt_sm_router::ManualPhase::Idle
                | ::distvirt_sm_router::ManualPhase::Events(0) => {}
                _ => panic!(
                    "is_quiescent() called in invalid phase: {:?}",
                    self.manual_phase
                ),
            }
            self.dirty.is_empty()
                && self.pending_events.is_empty()
                && !self.has_pending_creates()
        }
    });
}
