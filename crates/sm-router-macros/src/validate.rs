use std::collections::HashSet;

use crate::parse::*;

fn check_duplicates(
    errors: &mut Vec<syn::Error>,
    items: impl IntoIterator<Item = (String, proc_macro2::Span)>,
    kind: &str,
) {
    let mut seen = HashSet::new();
    for (name, span) in items {
        if !seen.insert(name.clone()) {
            errors.push(syn::Error::new(span, format!("duplicate {kind} `{name}`")));
        }
    }
}

fn check_duplicates_composite(
    errors: &mut Vec<syn::Error>,
    items: impl IntoIterator<Item = (String, String, proc_macro2::Span)>,
    kind: &str,
    format_name: impl Fn(&str, &str) -> String,
) {
    let mut seen = HashSet::new();
    for (a, b, span) in items {
        if !seen.insert((a.clone(), b.clone())) {
            errors.push(syn::Error::new(
                span,
                format!("duplicate {kind} `{}`", format_name(&a, &b)),
            ));
        }
    }
}

pub fn validate(def: &TopologyDef) -> syn::Result<()> {
    let mut errors: Vec<syn::Error> = Vec::new();

    // --- Duplicate name checks ---

    // 1. Duplicate SM names
    check_duplicates(
        &mut errors,
        def.state_machines
            .iter()
            .map(|s| (s.name.to_string(), s.name.span())),
        "state machine",
    );

    // 2. Duplicate port names
    check_duplicates(
        &mut errors,
        def.ports
            .iter()
            .map(|p| (p.name.to_string(), p.name.span())),
        "port",
    );

    // 3. SM/port name collision
    {
        let sm_names: HashSet<String> = def
            .state_machines
            .iter()
            .map(|s| s.name.to_string())
            .collect();
        for port in &def.ports {
            if sm_names.contains(&port.name.to_string()) {
                errors.push(syn::Error::new(
                    port.name.span(),
                    format!("port `{}` has the same name as a state machine", port.name),
                ));
            }
        }
    }

    // 4. Duplicate edge names
    check_duplicates(
        &mut errors,
        def.edges
            .iter()
            .map(|e| (e.name.to_string(), e.name.span())),
        "edge",
    );

    // 5. Duplicate signal names on the same node
    check_duplicates_composite(
        &mut errors,
        def.signals
            .iter()
            .map(|s| (s.node.to_string(), s.signal.to_string(), s.signal.span())),
        "signal",
        |node, signal| format!("{node}::{signal}"),
    );

    // 6. Duplicate event names
    check_duplicates(
        &mut errors,
        def.events
            .iter()
            .map(|e| (e.name.to_string(), e.name.span())),
        "event",
    );

    // 7. Duplicate input names on the same node
    check_duplicates_composite(
        &mut errors,
        def.inputs.iter().map(|i| {
            (
                i.node.to_string(),
                i.input_name.to_string(),
                i.input_name.span(),
            )
        }),
        "input",
        |node, input| format!("{node}::{input}"),
    );

    // --- Structural checks ---

    for inp in &def.inputs {
        // 8. Empty input sources
        if inp.sources.is_empty() {
            errors.push(syn::Error::new(
                inp.input_name.span(),
                format!("input `{}::{}` has no sources", inp.node, inp.input_name),
            ));
        }

        // 9. Duplicate source pairs within an input
        {
            let mut seen = HashSet::new();
            for sp in &inp.sources {
                let key = (
                    sp.edge.to_string(),
                    sp.node.to_string(),
                    sp.signal.to_string(),
                );
                if !seen.insert(key) {
                    errors.push(syn::Error::new(
                        sp.edge.span(),
                        format!(
                            "duplicate source ({}, {}::{}) in input `{}::{}`",
                            sp.edge, sp.node, sp.signal, inp.node, inp.input_name
                        ),
                    ));
                }
            }
        }
    }

    // --- Referential integrity checks ---

    let is_node = |name: &syn::Ident| -> bool {
        def.state_machines.iter().any(|s| s.name == *name)
            || def.ports.iter().any(|p| p.name == *name)
    };

    for sig in &def.signals {
        if !is_node(&sig.node) {
            errors.push(syn::Error::new(
                sig.node.span(),
                format!("unknown node `{}`", sig.node),
            ));
        }
    }

    for ev in &def.events {
        if !is_node(&ev.sender) {
            errors.push(syn::Error::new(
                ev.sender.span(),
                format!("unknown sender `{}`", ev.sender),
            ));
        }
        if !def.state_machines.iter().any(|s| s.name == ev.receiver) {
            errors.push(syn::Error::new(
                ev.receiver.span(),
                format!("event receiver `{}` must be a state machine", ev.receiver),
            ));
        }
        let has_connecting_edge = def.edges.iter().any(|e| {
            (e.source == ev.sender && e.target == ev.receiver)
                || (e.source == ev.receiver && e.target == ev.sender)
        });
        if !has_connecting_edge {
            errors.push(syn::Error::new(
                ev.name.span(),
                format!(
                    "no edge type connects `{}` and `{}`",
                    ev.sender, ev.receiver
                ),
            ));
        }
    }

    for edge in &def.edges {
        if !is_node(&edge.source) {
            errors.push(syn::Error::new(
                edge.source.span(),
                format!("unknown edge source `{}`", edge.source),
            ));
        }
        if !is_node(&edge.target) {
            errors.push(syn::Error::new(
                edge.target.span(),
                format!("unknown edge target `{}`", edge.target),
            ));
        }
    }

    for inp in &def.inputs {
        if !is_node(&inp.node) {
            errors.push(syn::Error::new(
                inp.node.span(),
                format!("input targets unknown node `{}`", inp.node),
            ));
        }
        for sp in &inp.sources {
            if !def.edges.iter().any(|e| e.name == sp.edge) {
                errors.push(syn::Error::new(
                    sp.edge.span(),
                    format!("unknown edge `{}`", sp.edge),
                ));
                continue;
            }
            if !def
                .signals
                .iter()
                .any(|s| s.node == sp.node && s.signal == sp.signal)
            {
                errors.push(syn::Error::new(
                    sp.signal.span(),
                    format!("unknown signal `{}::{}`", sp.node, sp.signal),
                ));
            }
            let edge_def = def.edges.iter().find(|e| e.name == sp.edge).unwrap();
            if edge_def.source != sp.node {
                errors.push(syn::Error::new(
                    sp.node.span(),
                    format!(
                        "edge `{}` source is `{}`, but signal is on `{}`",
                        sp.edge, edge_def.source, sp.node
                    ),
                ));
            }
            if edge_def.target != inp.node {
                errors.push(syn::Error::new(
                    sp.edge.span(),
                    format!(
                        "edge `{}` target is `{}`, but input is on `{}`",
                        sp.edge, edge_def.target, inp.node
                    ),
                ));
            }
        }
    }

    // --- Invariant referential integrity ---

    for inv in &def.invariants {
        if !is_node(&inv.node) {
            errors.push(syn::Error::new(
                inv.node.span(),
                format!("invariant references unknown node `{}`", inv.node),
            ));
        } else if !def
            .signals
            .iter()
            .any(|s| s.node == inv.node && s.signal == inv.signal)
        {
            errors.push(syn::Error::new(
                inv.signal.span(),
                format!(
                    "invariant references unknown signal `{}::{}`",
                    inv.node, inv.signal
                ),
            ));
        }
    }

    if let Some(first) = errors.into_iter().reduce(|mut combined, e| {
        combined.combine(e);
        combined
    }) {
        Err(first)
    } else {
        Ok(())
    }
}
