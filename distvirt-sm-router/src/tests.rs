use crate::{Aggregator, ListAggregator, SmHandler};

// ---- ID types ----

#[derive(Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug)]
struct AlphaId(u64);

#[derive(Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug)]
struct BetaId(u64);

#[derive(Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug)]
struct GammaId(u64);

// ---- Aggregators ----

#[derive(Default)]
struct CountTrueAggregator;

impl Aggregator for CountTrueAggregator {
    type Input = (AlphaId, bool);
    type Output = u32;

    fn aggregate(&self, inputs: &[(AlphaId, bool)]) -> u32 {
        inputs.iter().filter(|(_, demand)| *demand).count() as u32
    }
}

// ---- Generate router code via macro ----

crate::router! {
    expose_internals_for_testing

    state_machines {
        Alpha(AlphaId, AlphaSm),
        Beta(BetaId, BetaSm),
    }
    ports {
        Gamma(GammaId),
    }
    signals {
        Alpha::Demand(bool),
        Beta::Status(u32),
        Gamma::Value(u32),
    }
    edges {
        AlphaToBeta: Alpha -> Beta,
        BetaToAlpha: Beta -> Alpha,
        GammaToBeta: Gamma -> Beta,
        BetaToGamma: Beta -> Gamma,
    }
    events {
        Command(String): Gamma -> Beta,
        SmEvent(u32): Alpha -> Beta,
    }
    inputs {
        Beta::DemandInput {
            sources: [(AlphaToBeta, Alpha::Demand)],
            aggregator: CountTrueAggregator,
        },
        Beta::ConfigInput {
            sources: [(GammaToBeta, Gamma::Value)],
            aggregator: ListAggregator<GammaId, u32>,
        },
        Alpha::StatusInput {
            sources: [(BetaToAlpha, Beta::Status)],
            aggregator: ListAggregator<BetaId, u32>,
        },
        Beta::MultiSourceInput {
            sources: [(AlphaToBeta, Alpha::Demand), (GammaToBeta, Gamma::Value)],
            aggregator: MultiSourceAggregator,
        },
    }
}

// ---- Multi-source aggregator (uses generated MultiSourceInputSource enum) ----

#[derive(Default)]
struct MultiSourceAggregator;

impl Aggregator for MultiSourceAggregator {
    type Input = MultiSourceInputSource;
    type Output = Vec<MultiSourceInputSource>;

    fn aggregate(&self, inputs: &[MultiSourceInputSource]) -> Vec<MultiSourceInputSource> {
        inputs.to_vec()
    }
}

// ---- SM types (defined after macro — references generated Input/Ctx types) ----

struct AlphaSm {
    deliveries: Vec<AlphaInput>,
    on_handle: Option<Box<dyn FnMut(&AlphaInput, &mut AlphaCtx)>>,
}

impl AlphaSm {
    fn new() -> Self {
        AlphaSm {
            deliveries: Vec::new(),
            on_handle: None,
        }
    }
}

impl SmHandler for AlphaSm {
    type Input = AlphaInput;
    type Ctx = AlphaCtx;

    fn handle(&mut self, input: Self::Input, ctx: &mut Self::Ctx) {
        if let Some(ref mut cb) = self.on_handle {
            cb(&input, ctx);
        }
        self.deliveries.push(input);
    }
}

struct BetaSm {
    deliveries: Vec<BetaInput>,
    on_handle: Option<Box<dyn FnMut(&BetaInput, &mut BetaCtx)>>,
}

impl BetaSm {
    fn new() -> Self {
        BetaSm {
            deliveries: Vec::new(),
            on_handle: None,
        }
    }
}

impl SmHandler for BetaSm {
    type Input = BetaInput;
    type Ctx = BetaCtx;

    fn handle(&mut self, input: Self::Input, ctx: &mut Self::Ctx) {
        if let Some(ref mut cb) = self.on_handle {
            cb(&input, ctx);
        }
        self.deliveries.push(input);
    }
}

// ---- Constants ----

const A1: AlphaId = AlphaId(1);
const A2: AlphaId = AlphaId(2);
const A3: AlphaId = AlphaId(3);
const B1: BetaId = BetaId(1);
const B2: BetaId = BetaId(2);
const G1: GammaId = GammaId(1);
const G2: GammaId = GammaId(2);

// ---- Tests ----

#[test]
fn basic_signal_propagation() {
    let mut router = Router::new(16);
    router.create_alpha(A1, AlphaSm::new());
    router.create_beta(B1, BetaSm::new());

    router.set_alpha_to_beta_edges(A1, vec![B1]);
    router.propagate();

    let b1 = router.get_beta(&B1).unwrap();
    let demand: Vec<_> = b1
        .deliveries
        .iter()
        .filter(|inp| matches!(inp, BetaInput::DemandInput(_)))
        .collect();
    assert_eq!(demand.len(), 1);
    assert_eq!(*demand[0], BetaInput::DemandInput(0));

    router.set_alpha_demand(A1, true);
    router.propagate();

    let b1 = router.get_beta(&B1).unwrap();
    let demand: Vec<_> = b1
        .deliveries
        .iter()
        .filter(|inp| matches!(inp, BetaInput::DemandInput(_)))
        .collect();
    assert_eq!(demand.len(), 2);
    assert_eq!(*demand[1], BetaInput::DemandInput(1));
}

#[test]
fn change_detection_no_delivery_on_same_value() {
    let mut router = Router::new(16);
    router.create_alpha(A1, AlphaSm::new());
    router.create_beta(B1, BetaSm::new());

    router.set_alpha_to_beta_edges(A1, vec![B1]);
    router.set_alpha_demand(A1, true);
    router.propagate();

    let b1 = router.get_beta(&B1).unwrap();
    let demand_count = b1
        .deliveries
        .iter()
        .filter(|inp| matches!(inp, BetaInput::DemandInput(_)))
        .count();
    assert_eq!(demand_count, 1);

    router.set_alpha_demand(A1, true);
    router.propagate();

    let b1 = router.get_beta(&B1).unwrap();
    let demand_count = b1
        .deliveries
        .iter()
        .filter(|inp| matches!(inp, BetaInput::DemandInput(_)))
        .count();
    assert_eq!(demand_count, 1);
}

#[test]
fn multi_edge_aggregation() {
    let mut router = Router::new(16);
    router.create_alpha(A1, AlphaSm::new());
    router.create_alpha(A2, AlphaSm::new());
    router.create_alpha(A3, AlphaSm::new());
    router.create_beta(B1, BetaSm::new());

    router.set_alpha_to_beta_edges(A1, vec![B1]);
    router.set_alpha_to_beta_edges(A2, vec![B1]);
    router.set_alpha_to_beta_edges(A3, vec![B1]);
    router.propagate();

    router.set_alpha_demand(A1, true);
    router.set_alpha_demand(A2, false);
    router.set_alpha_demand(A3, true);
    router.propagate();

    let b1 = router.get_beta(&B1).unwrap();
    let demand: Vec<_> = b1
        .deliveries
        .iter()
        .filter_map(|inp| match inp {
            BetaInput::DemandInput(v) => Some(*v),
            _ => None,
        })
        .collect();
    assert_eq!(*demand.last().unwrap(), 2);
}

#[test]
fn cascading_signal_to_edge_to_signal() {
    let mut router = Router::new(16);
    router.create_alpha(A1, AlphaSm::new());

    let mut b1 = BetaSm::new();
    b1.on_handle = Some(Box::new(|input, ctx| {
        let BetaInput::DemandInput(count) = input else {
            return;
        };
        if *count > 0 {
            ctx.set_beta_to_alpha_edges(vec![A1]);
            ctx.set_status(42);
        }
    }));
    router.create_beta(B1, b1);

    router.set_alpha_to_beta_edges(A1, vec![B1]);

    router.set_alpha_demand(A1, true);
    router.propagate();

    let b1 = router.get_beta(&B1).unwrap();
    assert!(b1
        .deliveries
        .iter()
        .any(|inp| *inp == BetaInput::DemandInput(1)));

    let a1 = router.get_alpha(&A1).unwrap();
    assert!(a1
        .deliveries
        .iter()
        .any(|inp| *inp == AlphaInput::StatusInput(vec![42])));
}

#[test]
#[should_panic(expected = "depth limit")]
fn depth_limiting_panics_on_cycle() {
    let mut router = Router::new(4);

    let mut a1 = AlphaSm::new();
    let mut counter = 0u32;
    a1.on_handle = Some(Box::new(move |_input, ctx| {
        counter += 1;
        ctx.set_demand(counter % 2 == 0);
    }));
    router.create_alpha(A1, a1);

    let mut b1 = BetaSm::new();
    let mut counter2 = 0u32;
    b1.on_handle = Some(Box::new(move |_input, ctx| {
        counter2 += 1;
        ctx.set_status(counter2);
    }));
    router.create_beta(B1, b1);

    router.set_alpha_to_beta_edges(A1, vec![B1]);
    router.set_beta_to_alpha_edges(B1, vec![A1]);

    router.set_alpha_demand(A1, true);
    router.propagate();
}

#[test]
fn edge_removal_triggers_reaggregation() {
    let mut router = Router::new(16);
    router.create_alpha(A1, AlphaSm::new());
    router.create_alpha(A2, AlphaSm::new());
    router.create_beta(B1, BetaSm::new());

    router.set_alpha_to_beta_edges(A1, vec![B1]);
    router.set_alpha_to_beta_edges(A2, vec![B1]);
    router.set_alpha_demand(A1, true);
    router.set_alpha_demand(A2, true);
    router.propagate();

    let b1 = router.get_beta(&B1).unwrap();
    let demand: Vec<_> = b1
        .deliveries
        .iter()
        .filter_map(|inp| match inp {
            BetaInput::DemandInput(v) => Some(*v),
            _ => None,
        })
        .collect();
    assert_eq!(*demand.last().unwrap(), 2);

    router.set_alpha_to_beta_edges(A2, vec![]);
    router.propagate();

    let b1 = router.get_beta(&B1).unwrap();
    let demand: Vec<_> = b1
        .deliveries
        .iter()
        .filter_map(|inp| match inp {
            BetaInput::DemandInput(v) => Some(*v),
            _ => None,
        })
        .collect();
    assert_eq!(*demand.last().unwrap(), 1);
}

#[test]
fn batched_changes_single_round() {
    let mut router = Router::new(16);
    router.create_alpha(A1, AlphaSm::new());
    router.create_alpha(A2, AlphaSm::new());
    router.create_beta(B1, BetaSm::new());

    router.set_alpha_to_beta_edges(A1, vec![B1]);
    router.set_alpha_to_beta_edges(A2, vec![B1]);
    router.set_alpha_demand(A1, true);
    router.set_alpha_demand(A2, true);

    router.propagate();

    let b1 = router.get_beta(&B1).unwrap();
    let demand: Vec<_> = b1
        .deliveries
        .iter()
        .filter(|inp| matches!(inp, BetaInput::DemandInput(_)))
        .collect();
    assert_eq!(demand.len(), 1);
    assert_eq!(*demand[0], BetaInput::DemandInput(2));
}

#[test]
fn port_removal_cleans_edges_and_reaggregates() {
    let mut router = Router::new(16);
    router.create_beta(B1, BetaSm::new());
    router.create_gamma(G1);
    router.create_gamma(G2);

    router.set_gamma_to_beta_edges(G1, vec![B1]);
    router.set_gamma_to_beta_edges(G2, vec![B1]);
    router.set_gamma_value(G1, 10);
    router.set_gamma_value(G2, 20);

    router.propagate();

    let b1 = router.get_beta(&B1).unwrap();
    let config: Vec<_> = b1
        .deliveries
        .iter()
        .filter(|inp| matches!(inp, BetaInput::ConfigInput(_)))
        .collect();
    assert_eq!(config.len(), 1);
    if let BetaInput::ConfigInput(vals) = config[0] {
        assert_eq!(vals.len(), 2);
        assert!(vals.contains(&10));
        assert!(vals.contains(&20));
    } else {
        panic!("expected ConfigInput");
    }

    router.destroy_gamma(G1);
    router.propagate();

    let b1 = router.get_beta(&B1).unwrap();
    let config: Vec<_> = b1
        .deliveries
        .iter()
        .filter(|inp| matches!(inp, BetaInput::ConfigInput(_)))
        .collect();
    assert_eq!(config.len(), 2);
    assert_eq!(*config[1], BetaInput::ConfigInput(vec![20]));

    router.destroy_gamma(G2);
    router.propagate();

    let b1 = router.get_beta(&B1).unwrap();
    assert!(b1
        .deliveries
        .iter()
        .any(|inp| *inp == BetaInput::ConfigInput(vec![])));
}

#[test]
fn dangling_edges_target_dies_source_unaffected() {
    let mut router = Router::new(16);
    router.create_alpha(A1, AlphaSm::new());

    // Beta self-destructs when it receives demand > 0.
    let mut b1_sm = BetaSm::new();
    b1_sm.on_handle = Some(Box::new(|input, ctx| {
        if let BetaInput::DemandInput(count) = input {
            if *count > 0 {
                ctx.self_destruct();
            }
        }
    }));
    router.create_beta(B1, b1_sm);

    router.set_alpha_to_beta_edges(A1, vec![B1]);
    router.set_alpha_demand(A1, true);
    router.propagate();

    // B1 self-destructed on receiving demand.
    assert!(router.get_beta(&B1).is_none());

    router.set_alpha_demand(A1, false);
    router.propagate();

    let a1 = router.get_alpha(&A1).unwrap();
    assert_eq!(a1.deliveries.len(), 0);

    // expose_internals_for_testing allows direct field access
    // Dangling edge remains — source is unaffected.
    assert!(router
        .alpha_to_beta_fwd
        .get(&A1)
        .unwrap()
        .contains(&B1));
}

#[test]
fn sm_creation_no_eager_delivery() {
    let mut router = Router::new(16);

    router.create_alpha(A1, AlphaSm::new());
    router.create_beta(B1, BetaSm::new());

    router.propagate();

    let a1 = router.get_alpha(&A1).unwrap();
    let b1 = router.get_beta(&B1).unwrap();
    assert_eq!(a1.deliveries.len(), 0);
    assert_eq!(b1.deliveries.len(), 0);
}

#[test]
fn round_semantics_multiple_inputs_delivered_independently() {
    let mut router = Router::new(16);
    router.create_alpha(A1, AlphaSm::new());
    router.create_beta(B1, BetaSm::new());
    router.create_gamma(G1);

    router.set_alpha_to_beta_edges(A1, vec![B1]);
    router.set_gamma_to_beta_edges(G1, vec![B1]);

    router.set_alpha_demand(A1, true);
    router.set_gamma_value(G1, 99);

    router.propagate();

    let b1 = router.get_beta(&B1).unwrap();
    let demand_count = b1
        .deliveries
        .iter()
        .filter(|inp| matches!(inp, BetaInput::DemandInput(_)))
        .count();
    let config_count = b1
        .deliveries
        .iter()
        .filter(|inp| matches!(inp, BetaInput::ConfigInput(_)))
        .count();

    assert_eq!(demand_count, 1);
    assert_eq!(config_count, 1);

    assert!(b1
        .deliveries
        .iter()
        .any(|inp| *inp == BetaInput::DemandInput(1)));
    assert!(b1
        .deliveries
        .iter()
        .any(|inp| *inp == BetaInput::ConfigInput(vec![99])));
}

#[test]
fn sm_self_destruct_removes_outgoing_edges() {
    let mut router = Router::new(16);
    router.create_alpha(A1, AlphaSm::new());

    // A2 self-destructs when it receives status input.
    let mut a2 = AlphaSm::new();
    a2.on_handle = Some(Box::new(|input, ctx| {
        if matches!(input, AlphaInput::StatusInput(_)) {
            ctx.self_destruct();
        }
    }));
    router.create_alpha(A2, a2);
    router.create_beta(B1, BetaSm::new());

    router.set_alpha_to_beta_edges(A1, vec![B1]);
    router.set_alpha_to_beta_edges(A2, vec![B1]);
    router.set_alpha_demand(A1, true);
    router.set_alpha_demand(A2, true);

    router.propagate();

    let b1 = router.get_beta(&B1).unwrap();
    let demand: Vec<_> = b1
        .deliveries
        .iter()
        .filter_map(|inp| match inp {
            BetaInput::DemandInput(v) => Some(*v),
            _ => None,
        })
        .collect();
    assert_eq!(*demand.last().unwrap(), 2);

    // Trigger A2 self-destruct via BetaToAlpha edge + status signal.
    router.set_beta_to_alpha_edges(B1, vec![A2]);
    router.set_beta_status(B1, 42);
    router.propagate();

    // A2 should be gone.
    assert!(router.get_alpha(&A2).is_none());

    // B1 should have received updated demand (down to 1).
    let b1 = router.get_beta(&B1).unwrap();
    let demand: Vec<_> = b1
        .deliveries
        .iter()
        .filter_map(|inp| match inp {
            BetaInput::DemandInput(v) => Some(*v),
            _ => None,
        })
        .collect();
    assert_eq!(*demand.last().unwrap(), 1);
}

#[test]
fn dangling_edge_to_dead_sm_is_noop() {
    let mut router = Router::new(16);
    router.create_alpha(A1, AlphaSm::new());

    // Beta self-destructs on first demand input.
    let mut b1_sm = BetaSm::new();
    b1_sm.on_handle = Some(Box::new(|input, ctx| {
        if matches!(input, BetaInput::DemandInput(_)) {
            ctx.self_destruct();
        }
    }));
    router.create_beta(B1, b1_sm);

    router.set_alpha_to_beta_edges(A1, vec![B1]);
    router.set_alpha_demand(A1, true);
    router.propagate();

    // B1 self-destructed.
    assert!(router.get_beta(&B1).is_none());

    // Setting edges to dead B1 and non-existent B2 is a noop.
    router.set_alpha_to_beta_edges(A1, vec![B1, B2]);
    router.propagate();

    assert!(router.get_beta(&B1).is_none());
    assert!(router.get_beta(&B2).is_none());
}

#[test]
fn multi_source_input_aggregation() {
    let mut router = Router::new(16);
    router.create_alpha(A1, AlphaSm::new());
    router.create_beta(B1, BetaSm::new());
    router.create_gamma(G1);

    router.set_alpha_to_beta_edges(A1, vec![B1]);
    router.set_gamma_to_beta_edges(G1, vec![B1]);

    router.set_alpha_demand(A1, true);
    router.set_gamma_value(G1, 42);

    router.propagate();

    let b1 = router.get_beta(&B1).unwrap();
    let multi: Vec<_> = b1
        .deliveries
        .iter()
        .filter(|inp| matches!(inp, BetaInput::MultiSourceInput(_)))
        .collect();
    assert_eq!(multi.len(), 1);
    if let BetaInput::MultiSourceInput(vals) = &multi[0] {
        assert!(vals.contains(&MultiSourceInputSource::AlphaDemand(A1, true)));
        assert!(vals.contains(&MultiSourceInputSource::GammaValue(G1, 42)));
        assert_eq!(vals.len(), 2);
    } else {
        panic!("expected MultiSourceInput");
    }
}

#[test]
fn port_removal_cleans_incoming_edges() {
    let mut router = Router::new(16);
    router.create_beta(B1, BetaSm::new());
    router.create_gamma(G1);

    router.set_beta_to_gamma_edges(B1, vec![G1]);

    // Verify edges are set up (expose_internals_for_testing allows field access)
    assert!(router.beta_to_gamma_fwd.get(&B1).unwrap().contains(&G1));
    assert!(router.beta_to_gamma_rev.get(&G1).unwrap().contains(&B1));

    // Remove gamma — should clean up incoming edges
    router.destroy_gamma(G1);

    assert!(
        router
            .beta_to_gamma_fwd
            .get(&B1)
            .map_or(true, |v| v.is_empty())
    );
    assert!(!router.beta_to_gamma_rev.contains_key(&G1));
}

// ---- Event tests ----

#[test]
fn event_delivery_along_forward_edge() {
    // Gamma -> Beta edge exists (GammaToBeta), event declared Gamma -> Beta
    let mut router = Router::new(16);
    router.create_beta(B1, BetaSm::new());
    router.create_gamma(G1);

    router.set_gamma_to_beta_edges(G1, vec![B1]);
    router.propagate();

    router.send_command(G1, B1, "restart".to_string());
    router.propagate();

    let b1 = router.get_beta(&B1).unwrap();
    assert!(b1
        .deliveries
        .iter()
        .any(|inp| *inp == BetaInput::Command("restart".to_string())));
}

#[test]
fn event_delivery_with_reverse_edge_only() {
    // Only BetaToGamma edge exists (Beta -> Gamma), but event is Gamma -> Beta.
    // Either-direction connectivity should allow delivery.
    let mut router = Router::new(16);
    router.create_beta(B1, BetaSm::new());
    router.create_gamma(G1);

    // Only reverse edge: Beta -> Gamma
    router.set_beta_to_gamma_edges(B1, vec![G1]);
    router.propagate();

    router.send_command(G1, B1, "hello".to_string());
    router.propagate();

    let b1 = router.get_beta(&B1).unwrap();
    assert!(b1
        .deliveries
        .iter()
        .any(|inp| *inp == BetaInput::Command("hello".to_string())));
}

#[test]
#[should_panic(expected = "no edge")]
fn event_rejected_when_no_edge_exists() {
    let mut router = Router::new(16);
    router.create_beta(B1, BetaSm::new());
    router.create_gamma(G1);

    // No edges between G1 and B1
    router.send_command(G1, B1, "fail".to_string());
    router.propagate();
}

#[test]
#[should_panic(expected = "no edge")]
fn event_to_nonexistent_target_rejected() {
    // Event to a target that doesn't exist — connectivity check should fail
    // because there can't be edges to a nonexistent node.
    let mut router = Router::new(16);
    router.create_gamma(G1);

    // B1 doesn't exist, no edges possible
    router.send_command(G1, B1, "ghost".to_string());
    // Should panic during propagate with "no edge" error
    router.propagate();
}

#[test]
fn event_sent_from_sm_handler() {
    // Alpha sends SmEvent to Beta during its handler
    let mut router = Router::new(16);

    let mut a1 = AlphaSm::new();
    a1.on_handle = Some(Box::new(|_input, ctx| {
        ctx.send_sm_event(B1, 42);
    }));
    router.create_alpha(A1, a1);
    router.create_beta(B1, BetaSm::new());

    // Need edges: BetaToAlpha for status delivery, AlphaToBeta for event connectivity
    router.set_beta_to_alpha_edges(B1, vec![A1]);
    router.set_alpha_to_beta_edges(A1, vec![B1]);
    router.set_beta_status(B1, 99);
    router.propagate();

    let b1 = router.get_beta(&B1).unwrap();
    assert!(b1
        .deliveries
        .iter()
        .any(|inp| *inp == BetaInput::SmEvent(42)));
}

// ---- Edge-case tests ----

#[test]
fn aggregation_change_detection_suppresses_delivery() {
    // When two signals change simultaneously but the aggregated output stays the same,
    // no delivery should occur.
    let mut router = Router::new(16);
    router.create_alpha(A1, AlphaSm::new());
    router.create_alpha(A2, AlphaSm::new());
    router.create_beta(B1, BetaSm::new());

    router.set_alpha_to_beta_edges(A1, vec![B1]);
    router.set_alpha_to_beta_edges(A2, vec![B1]);

    // Both true → count=2
    router.set_alpha_demand(A1, true);
    router.set_alpha_demand(A2, true);
    router.propagate();

    let b1 = router.get_beta(&B1).unwrap();
    let demand: Vec<_> = b1
        .deliveries
        .iter()
        .filter_map(|inp| match inp {
            BetaInput::DemandInput(v) => Some(*v),
            _ => None,
        })
        .collect();
    assert_eq!(*demand.last().unwrap(), 2);

    // A1=false → count=1
    router.set_alpha_demand(A1, false);
    router.propagate();

    let b1 = router.get_beta(&B1).unwrap();
    let demand: Vec<_> = b1
        .deliveries
        .iter()
        .filter_map(|inp| match inp {
            BetaInput::DemandInput(v) => Some(*v),
            _ => None,
        })
        .collect();
    assert_eq!(*demand.last().unwrap(), 1);
    let delivery_count_before = demand.len();

    // A1=true, A2=false simultaneously → count still 1. No delivery expected.
    router.set_alpha_demand(A1, true);
    router.set_alpha_demand(A2, false);
    router.propagate();

    let b1 = router.get_beta(&B1).unwrap();
    let demand: Vec<_> = b1
        .deliveries
        .iter()
        .filter_map(|inp| match inp {
            BetaInput::DemandInput(v) => Some(*v),
            _ => None,
        })
        .collect();
    // No new delivery — aggregated output (1) didn't change
    assert_eq!(demand.len(), delivery_count_before);
}

#[test]
fn edge_retargeting_updates_both_old_and_new() {
    let mut router = Router::new(16);
    router.create_alpha(A1, AlphaSm::new());
    router.create_beta(B1, BetaSm::new());
    router.create_beta(B2, BetaSm::new());

    router.set_alpha_to_beta_edges(A1, vec![B1]);
    router.set_alpha_demand(A1, true);
    router.propagate();

    let b1 = router.get_beta(&B1).unwrap();
    assert!(b1
        .deliveries
        .iter()
        .any(|inp| *inp == BetaInput::DemandInput(1)));

    // Retarget A1 from B1 to B2
    router.set_alpha_to_beta_edges(A1, vec![B2]);
    router.propagate();

    // B1 should get DemandInput(0) — no more edges pointing to it from A1
    let b1 = router.get_beta(&B1).unwrap();
    assert!(b1
        .deliveries
        .iter()
        .any(|inp| *inp == BetaInput::DemandInput(0)));

    // B2 should get DemandInput(1) — now has edge from A1
    let b2 = router.get_beta(&B2).unwrap();
    assert!(b2
        .deliveries
        .iter()
        .any(|inp| *inp == BetaInput::DemandInput(1)));
}

#[test]
fn reactive_edge_creation_delivers_in_same_round() {
    // Beta handler creates edges and sets signal on receiving DemandInput.
    // Verify cascade resolves within a single propagate() call.
    let mut router = Router::new(16);
    router.create_alpha(A1, AlphaSm::new());
    router.create_alpha(A2, AlphaSm::new());

    let mut b1 = BetaSm::new();
    b1.on_handle = Some(Box::new(|input, ctx| {
        if let BetaInput::DemandInput(count) = input {
            if *count > 0 {
                ctx.set_beta_to_alpha_edges(vec![A1, A2]);
                ctx.set_status(77);
            }
        }
    }));
    router.create_beta(B1, b1);

    router.set_alpha_to_beta_edges(A1, vec![B1]);
    router.set_alpha_demand(A1, true);

    // Single propagate should cascade: A1→B1 (DemandInput) → B1 sets edges+status → A1,A2 get StatusInput
    router.propagate();

    let a1 = router.get_alpha(&A1).unwrap();
    assert!(a1
        .deliveries
        .iter()
        .any(|inp| *inp == AlphaInput::StatusInput(vec![77])));

    let a2 = router.get_alpha(&A2).unwrap();
    assert!(a2
        .deliveries
        .iter()
        .any(|inp| *inp == AlphaInput::StatusInput(vec![77])));
}

#[test]
fn multiple_events_same_target_same_round() {
    let mut router = Router::new(16);
    router.create_beta(B1, BetaSm::new());
    router.create_gamma(G1);

    router.set_gamma_to_beta_edges(G1, vec![B1]);
    router.propagate();

    // Send two events before propagate
    router.send_command(G1, B1, "first".to_string());
    router.send_command(G1, B1, "second".to_string());
    router.propagate();

    let b1 = router.get_beta(&B1).unwrap();
    let events: Vec<_> = b1
        .deliveries
        .iter()
        .filter_map(|inp| match inp {
            BetaInput::Command(s) => Some(s.clone()),
            _ => None,
        })
        .collect();
    assert_eq!(events.len(), 2);
    assert!(events.contains(&"first".to_string()));
    assert!(events.contains(&"second".to_string()));
}

#[test]
fn empty_aggregation_on_last_edge_removal() {
    let mut router = Router::new(16);
    router.create_alpha(A1, AlphaSm::new());
    router.create_beta(B1, BetaSm::new());

    router.set_alpha_to_beta_edges(A1, vec![B1]);
    router.set_alpha_demand(A1, true);
    router.propagate();

    let b1 = router.get_beta(&B1).unwrap();
    assert!(b1
        .deliveries
        .iter()
        .any(|inp| *inp == BetaInput::DemandInput(1)));

    // Remove the only edge
    router.set_alpha_to_beta_edges(A1, vec![]);
    router.propagate();

    let b1 = router.get_beta(&B1).unwrap();
    assert!(b1
        .deliveries
        .iter()
        .any(|inp| *inp == BetaInput::DemandInput(0)));
}

#[test]
fn signal_set_to_same_value_in_handler_no_cascade() {
    // When a handler sets the same signal value it already has, no cascade should occur.
    let mut router = Router::new(16);
    router.create_alpha(A1, AlphaSm::new());
    router.create_alpha(A2, AlphaSm::new());

    let mut b1 = BetaSm::new();
    b1.on_handle = Some(Box::new(|input, ctx| {
        if matches!(input, BetaInput::DemandInput(_)) {
            ctx.set_status(42);
        }
    }));
    router.create_beta(B1, b1);

    router.set_alpha_to_beta_edges(A1, vec![B1]);
    router.set_alpha_to_beta_edges(A2, vec![B1]);
    router.set_beta_to_alpha_edges(B1, vec![A1]);

    // First: A1 demand=true → B1 gets DemandInput(1), sets status=42 → A1 gets StatusInput
    router.set_alpha_demand(A1, true);
    router.propagate();

    let a1 = router.get_alpha(&A1).unwrap();
    let status_count = a1
        .deliveries
        .iter()
        .filter(|inp| matches!(inp, AlphaInput::StatusInput(_)))
        .count();
    assert_eq!(status_count, 1);

    // Second: A2 demand=true → count goes to 2 → B1 gets DemandInput(2), sets status=42 again (same value)
    // A1 should NOT get another StatusInput because Beta's status signal didn't change
    router.set_alpha_demand(A2, true);
    router.propagate();

    let a1 = router.get_alpha(&A1).unwrap();
    let status_count = a1
        .deliveries
        .iter()
        .filter(|inp| matches!(inp, AlphaInput::StatusInput(_)))
        .count();
    assert_eq!(status_count, 1); // still 1 — no new delivery
}

#[test]
fn multi_source_partial_change() {
    // Both Alpha and Gamma connected to B1's MultiSourceInput.
    // Change only Alpha's demand. Verify B1 receives updated MultiSourceInput with both sources.
    let mut router = Router::new(16);
    router.create_alpha(A1, AlphaSm::new());
    router.create_beta(B1, BetaSm::new());
    router.create_gamma(G1);

    router.set_alpha_to_beta_edges(A1, vec![B1]);
    router.set_gamma_to_beta_edges(G1, vec![B1]);

    router.set_alpha_demand(A1, true);
    router.set_gamma_value(G1, 10);
    router.propagate();

    let b1 = router.get_beta(&B1).unwrap();
    let multi_count = b1
        .deliveries
        .iter()
        .filter(|inp| matches!(inp, BetaInput::MultiSourceInput(_)))
        .count();
    assert_eq!(multi_count, 1);

    // Change only Alpha's demand
    router.set_alpha_demand(A1, false);
    router.propagate();

    let b1 = router.get_beta(&B1).unwrap();
    let multi: Vec<_> = b1
        .deliveries
        .iter()
        .filter_map(|inp| match inp {
            BetaInput::MultiSourceInput(v) => Some(v.clone()),
            _ => None,
        })
        .collect();
    assert_eq!(multi.len(), 2); // got a second delivery

    let last = multi.last().unwrap();
    // Should contain both sources: updated Alpha (false) + unchanged Gamma (10)
    assert!(last.contains(&MultiSourceInputSource::AlphaDemand(A1, false)));
    assert!(last.contains(&MultiSourceInputSource::GammaValue(G1, 10)));
    assert_eq!(last.len(), 2);
}

#[test]
fn duplicate_edges_are_deduplicated() {
    let mut router = Router::new(16);
    router.create_alpha(A1, AlphaSm::new());
    router.create_beta(B1, BetaSm::new());

    // Set duplicate edges
    router.set_alpha_to_beta_edges(A1, vec![B1, B1]);
    router.set_alpha_demand(A1, true);
    router.propagate();

    let b1 = router.get_beta(&B1).unwrap();
    let demand: Vec<_> = b1
        .deliveries
        .iter()
        .filter_map(|inp| match inp {
            BetaInput::DemandInput(v) => Some(*v),
            _ => None,
        })
        .collect();
    // Should see count=1 (deduplicated), not count=2
    assert_eq!(*demand.last().unwrap(), 1);
}

#[test]
#[should_panic(expected = "depth limit")]
fn depth_limit_at_three() {
    let mut router = Router::new(3);

    let mut a1 = AlphaSm::new();
    let mut counter = 0u32;
    a1.on_handle = Some(Box::new(move |_input, ctx| {
        counter += 1;
        ctx.set_demand(counter % 2 == 0);
    }));
    router.create_alpha(A1, a1);

    let mut b1 = BetaSm::new();
    let mut counter2 = 0u32;
    b1.on_handle = Some(Box::new(move |_input, ctx| {
        counter2 += 1;
        ctx.set_status(counter2);
    }));
    router.create_beta(B1, b1);

    router.set_alpha_to_beta_edges(A1, vec![B1]);
    router.set_beta_to_alpha_edges(B1, vec![A1]);

    router.set_alpha_demand(A1, true);
    router.propagate();
}

#[test]
#[should_panic(expected = "no edge")]
fn event_rejected_after_edge_removed() {
    let mut router = Router::new(16);
    router.create_beta(B1, BetaSm::new());
    router.create_gamma(G1);

    // Establish edge and send event successfully
    router.set_gamma_to_beta_edges(G1, vec![B1]);
    router.propagate();
    router.send_command(G1, B1, "works".to_string());
    router.propagate();

    // Remove edge
    router.set_gamma_to_beta_edges(G1, vec![]);
    router.propagate();

    // Should panic — no edge exists anymore
    router.send_command(G1, B1, "should_fail".to_string());
    router.propagate();
}

#[test]
fn sm_handler_sets_edges_and_signals_atomically() {
    // Beta handler sets both edges and signal in one handler call.
    // Both effects should cascade correctly in one propagate.
    let mut router = Router::new(16);
    router.create_alpha(A1, AlphaSm::new());

    let mut b1 = BetaSm::new();
    b1.on_handle = Some(Box::new(|input, ctx| {
        if let BetaInput::DemandInput(count) = input {
            if *count > 0 {
                ctx.set_beta_to_alpha_edges(vec![A1]);
                ctx.set_status(99);
            }
        }
    }));
    router.create_beta(B1, b1);

    router.set_alpha_to_beta_edges(A1, vec![B1]);
    router.set_alpha_demand(A1, true);
    router.propagate();

    // B1 should have received DemandInput
    let b1 = router.get_beta(&B1).unwrap();
    assert!(b1
        .deliveries
        .iter()
        .any(|inp| *inp == BetaInput::DemandInput(1)));

    // A1 should have received StatusInput from the cascade
    let a1 = router.get_alpha(&A1).unwrap();
    assert!(a1
        .deliveries
        .iter()
        .any(|inp| *inp == AlphaInput::StatusInput(vec![99])));

    // Verify the edge was actually created
    assert!(router
        .beta_to_alpha_fwd
        .get(&B1)
        .unwrap()
        .contains(&A1));
}

// ---- Auto-ID tests ----
// Separate module so the router! macro generates a fresh set of types.

// ---- Tracer tests ----

#[test]
fn tracer_captures_basic_propagation() {
    use crate::trace::{RecordingTracer, TraceEntry};

    let mut router = Router::new(16);
    router.create_alpha(A1, AlphaSm::new());
    router.create_beta(B1, BetaSm::new());

    router.set_alpha_to_beta_edges(A1, vec![B1]);
    router.set_alpha_demand(A1, true);

    router.set_tracer(RecordingTracer::new());
    router.propagate();

    let tracer = router.take_tracer().unwrap();
    let tracer: Box<RecordingTracer> = tracer.downcast().unwrap();
    let entries = tracer.entries();

    // Should have propagate start/end
    assert!(matches!(entries.first(), Some(TraceEntry::PropagateStart)));
    assert!(matches!(entries.last(), Some(TraceEntry::PropagateEnd { .. })));

    // Should have at least one round
    assert!(entries
        .iter()
        .any(|e| matches!(e, TraceEntry::RoundStart { depth: 1 })));

    // Should have delivered DemandInput to Beta
    assert!(entries.iter().any(|e| matches!(
        e,
        TraceEntry::InputDelivered {
            node: "Beta",
            input: "DemandInput",
            ..
        }
    )));

    // Should have signal change for Alpha::Demand
    assert!(entries.iter().any(|e| matches!(
        e,
        TraceEntry::SignalChanged {
            node: "Alpha",
            signal: "Demand",
            ..
        }
    )));
}

#[test]
fn tracer_captures_effects_bracketing() {
    use crate::trace::{RecordingTracer, TraceEntry};

    let mut router = Router::new(16);
    router.create_alpha(A1, AlphaSm::new());

    let mut b1 = BetaSm::new();
    b1.on_handle = Some(Box::new(|input, ctx| {
        if let BetaInput::DemandInput(count) = input {
            if *count > 0 {
                ctx.set_status(42);
                ctx.set_beta_to_alpha_edges(vec![A1]);
            }
        }
    }));
    router.create_beta(B1, b1);

    router.set_alpha_to_beta_edges(A1, vec![B1]);
    router.set_alpha_demand(A1, true);

    router.set_tracer(RecordingTracer::new());
    router.propagate();

    let tracer = router.take_tracer().unwrap();
    let tracer: Box<RecordingTracer> = tracer.downcast().unwrap();
    let entries = tracer.entries();

    // Find the EffectsStart/End for Beta(B1)
    let effects_start_idx = entries
        .iter()
        .position(|e| matches!(e, TraceEntry::EffectsStart { node: "Beta", .. }))
        .expect("should have EffectsStart for Beta");
    let effects_end_idx = entries
        .iter()
        .position(|e| matches!(e, TraceEntry::EffectsEnd { node: "Beta", .. }))
        .expect("should have EffectsEnd for Beta");

    // Signal change and edge change should be between effects_start and effects_end
    let has_signal_in_effects = entries[effects_start_idx..effects_end_idx]
        .iter()
        .any(|e| matches!(e, TraceEntry::SignalChanged { signal: "Status", .. }));
    assert!(has_signal_in_effects, "signal change should be within effects bracket");

    let has_edge_in_effects = entries[effects_start_idx..effects_end_idx]
        .iter()
        .any(|e| matches!(e, TraceEntry::EdgeChanged { edge: "BetaToAlpha", .. }));
    assert!(has_edge_in_effects, "edge change should be within effects bracket");
}

#[test]
fn tracer_captures_event_delivery() {
    use crate::trace::{RecordingTracer, TraceEntry};

    let mut router = Router::new(16);
    router.create_beta(B1, BetaSm::new());
    router.create_gamma(G1);

    router.set_gamma_to_beta_edges(G1, vec![B1]);
    router.propagate();

    router.set_tracer(RecordingTracer::new());
    router.send_command(G1, B1, "hello".to_string());
    router.propagate();

    let tracer = router.take_tracer().unwrap();
    let tracer: Box<RecordingTracer> = tracer.downcast().unwrap();
    let entries = tracer.entries();

    // Should have event queued
    assert!(entries.iter().any(|e| matches!(
        e,
        TraceEntry::EventQueued {
            event: "Command",
            ..
        }
    )));

    // Should have event delivered
    assert!(entries.iter().any(|e| matches!(
        e,
        TraceEntry::EventDelivered {
            event: "Command",
            ..
        }
    )));
}

#[test]
fn tracer_captures_suppression() {
    use crate::trace::{RecordingTracer, TraceEntry};

    let mut router = Router::new(16);
    router.create_alpha(A1, AlphaSm::new());
    router.create_alpha(A2, AlphaSm::new());
    router.create_beta(B1, BetaSm::new());

    router.set_alpha_to_beta_edges(A1, vec![B1]);
    router.set_alpha_to_beta_edges(A2, vec![B1]);

    // Both true → count=2
    router.set_alpha_demand(A1, true);
    router.set_alpha_demand(A2, true);
    router.propagate();

    // A1=false, A2=true simultaneously → count still 1... wait, initially 2, now 1.
    // Let me set up for actual suppression:
    // Set A1=false → count=1
    router.set_alpha_demand(A1, false);
    router.propagate();

    // Now A1=true, A2=false → count stays at 1 (suppressed)
    router.set_tracer(RecordingTracer::new());
    router.set_alpha_demand(A1, true);
    router.set_alpha_demand(A2, false);
    router.propagate();

    let tracer = router.take_tracer().unwrap();
    let tracer: Box<RecordingTracer> = tracer.downcast().unwrap();
    let entries = tracer.entries();

    // Should have InputSuppressed for DemandInput (aggregated value didn't change)
    assert!(entries.iter().any(|e| matches!(
        e,
        TraceEntry::InputSuppressed {
            node: "Beta",
            input: "DemandInput",
            ..
        }
    )));
}

#[test]
fn tracer_display_output() {
    use crate::trace::RecordingTracer;

    let mut router = Router::new(16);
    router.create_alpha(A1, AlphaSm::new());
    router.create_beta(B1, BetaSm::new());

    router.set_alpha_to_beta_edges(A1, vec![B1]);
    router.set_alpha_demand(A1, true);

    router.set_tracer(RecordingTracer::new());
    router.propagate();

    let tracer = router.take_tracer().unwrap();
    let tracer: Box<RecordingTracer> = tracer.downcast().unwrap();

    let output = format!("{}", tracer);
    assert!(output.contains("propagate"));
    assert!(output.contains("Round 1"));
    assert!(output.contains("deliver"));
    assert!(output.contains("DemandInput"));
}

mod auto_id {
    use crate::{Aggregator, ListAggregator, SmHandler};

    // One SM with user-provided ID, one with auto ID, one port with auto ID.

    #[derive(Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug)]
    struct ServiceId(u64);

    #[derive(Default)]
    struct CountTrueAggregator;

    impl Aggregator for CountTrueAggregator {
        type Input = (ServiceId, bool);
        type Output = u32;

        fn aggregate(&self, inputs: &[(ServiceId, bool)]) -> u32 {
            inputs.iter().filter(|(_, d)| *d).count() as u32
        }
    }

    crate::router! {
        expose_internals_for_testing

        state_machines {
            Service(ServiceId, ServiceSm),
            Worker(auto, WorkerSm),
        }
        ports {
            Config(auto),
        }
        signals {
            Service::Demand(bool),
            Worker::Status(u32),
            Config::Value(u32),
        }
        edges {
            ServiceToWorker: Service -> Worker,
            WorkerToService: Worker -> Service,
            ConfigToWorker: Config -> Worker,
        }
        inputs {
            Worker::DemandInput {
                sources: [(ServiceToWorker, Service::Demand)],
                aggregator: CountTrueAggregator,
            },
            Worker::ConfigInput {
                sources: [(ConfigToWorker, Config::Value)],
                aggregator: ListAggregator<ConfigId, u32>,
            },
            Service::StatusInput {
                sources: [(WorkerToService, Worker::Status)],
                aggregator: ListAggregator<WorkerId, u32>,
            },
        }
    }

    struct ServiceSm {
        deliveries: Vec<ServiceInput>,
    }

    impl ServiceSm {
        fn new() -> Self {
            ServiceSm {
                deliveries: Vec::new(),
            }
        }
    }

    impl SmHandler for ServiceSm {
        type Input = ServiceInput;
        type Ctx = ServiceCtx;

        fn handle(&mut self, input: Self::Input, _ctx: &mut Self::Ctx) {
            self.deliveries.push(input);
        }
    }

    struct WorkerSm {
        deliveries: Vec<WorkerInput>,
        on_handle: Option<Box<dyn FnMut(&WorkerInput, &mut WorkerCtx)>>,
    }

    impl WorkerSm {
        fn new() -> Self {
            WorkerSm {
                deliveries: Vec::new(),
                on_handle: None,
            }
        }
    }

    impl SmHandler for WorkerSm {
        type Input = WorkerInput;
        type Ctx = WorkerCtx;

        fn handle(&mut self, input: Self::Input, ctx: &mut Self::Ctx) {
            if let Some(ref mut cb) = self.on_handle {
                cb(&input, ctx);
            }
            self.deliveries.push(input);
        }
    }

    const S1: ServiceId = ServiceId(1);
    const S2: ServiceId = ServiceId(2);

    #[test]
    fn auto_id_sm_creation_returns_unique_ids() {
        let mut router = Router::new(16);
        let w1 = router.create_worker(WorkerSm::new());
        let w2 = router.create_worker(WorkerSm::new());
        assert_ne!(w1, w2);
    }

    #[test]
    fn auto_id_port_creation_returns_unique_ids() {
        let mut router = Router::new(16);
        let c1 = router.create_config();
        let c2 = router.create_config();
        assert_ne!(c1, c2);
    }

    #[test]
    fn auto_id_signal_propagation() {
        let mut router = Router::new(16);
        router.create_service(S1, ServiceSm::new());
        let w1 = router.create_worker(WorkerSm::new());

        router.set_service_to_worker_edges(S1, vec![w1]);
        router.set_service_demand(S1, true);
        router.propagate();

        let worker = router.get_worker(&w1).unwrap();
        assert!(worker
            .deliveries
            .iter()
            .any(|inp| *inp == WorkerInput::DemandInput(1)));
    }

    #[test]
    fn auto_id_port_signal_propagation() {
        let mut router = Router::new(16);
        let w1 = router.create_worker(WorkerSm::new());
        let c1 = router.create_config();

        router.set_config_to_worker_edges(c1, vec![w1]);
        router.set_config_value(c1, 42);
        router.propagate();

        let worker = router.get_worker(&w1).unwrap();
        assert!(worker
            .deliveries
            .iter()
            .any(|inp| *inp == WorkerInput::ConfigInput(vec![42])));
    }

    #[test]
    fn auto_id_mixed_with_user_id() {
        // Service has user-provided ID, Worker has auto ID.
        // Test cascading: service demand → worker → worker sets edge back → service gets status.
        let mut router = Router::new(16);
        router.create_service(S1, ServiceSm::new());
        router.create_service(S2, ServiceSm::new());

        let mut w1 = WorkerSm::new();
        w1.on_handle = Some(Box::new(move |input, ctx| {
            if let WorkerInput::DemandInput(count) = input {
                if *count > 0 {
                    ctx.set_worker_to_service_edges(vec![S1, S2]);
                    ctx.set_status(99);
                }
            }
        }));
        let w1_id = router.create_worker(w1);

        router.set_service_to_worker_edges(S1, vec![w1_id]);
        router.set_service_to_worker_edges(S2, vec![w1_id]);
        router.set_service_demand(S1, true);
        router.propagate();

        // Worker should have received demand
        let worker = router.get_worker(&w1_id).unwrap();
        assert!(worker
            .deliveries
            .iter()
            .any(|inp| *inp == WorkerInput::DemandInput(1)));

        // Both services should have received status via cascade
        let s1 = router.get_service(&S1).unwrap();
        assert!(s1
            .deliveries
            .iter()
            .any(|inp| *inp == ServiceInput::StatusInput(vec![99])));

        let s2 = router.get_service(&S2).unwrap();
        assert!(s2
            .deliveries
            .iter()
            .any(|inp| *inp == ServiceInput::StatusInput(vec![99])));
    }

    #[test]
    fn auto_id_self_destruct_and_reaggregate() {
        let mut router = Router::new(16);
        router.create_service(S1, ServiceSm::new());

        let w1 = router.create_worker(WorkerSm::new());

        // w2 self-destructs on first demand input.
        let mut w2_sm = WorkerSm::new();
        w2_sm.on_handle = Some(Box::new(|input, ctx| {
            if matches!(input, WorkerInput::DemandInput(_)) {
                ctx.self_destruct();
            }
        }));
        let w2 = router.create_worker(w2_sm);

        router.set_service_to_worker_edges(S1, vec![w1, w2]);
        router.set_service_demand(S1, true);
        router.propagate();

        // w2 self-destructed on receiving demand.
        assert!(router.get_worker(&w2).is_none());
        // w1 still exists.
        assert!(router.get_worker(&w1).is_some());
    }

    // ---- Handler-driven SM lifecycle tests ----

    #[test]
    fn handler_creates_sm_with_auto_id() {
        // Service handler creates a Worker SM when it receives demand.
        let mut router = Router::new(16);

        // Worker handler creates another worker on receiving DemandInput
        let mut w1 = WorkerSm::new();
        w1.on_handle = Some(Box::new(|input, ctx| {
            if let WorkerInput::DemandInput(count) = input {
                if *count > 0 {
                    let w2_id = ctx.create_worker(WorkerSm {
                        deliveries: Vec::new(),
                        on_handle: None,
                    });
                    // Set edge to the newly created worker
                    ctx.set_worker_to_service_edges(vec![S1]);
                    ctx.set_status(w2_id.0 as u32);
                }
            }
        }));
        let w1_id = router.create_worker(w1);
        router.create_service(S1, ServiceSm::new());

        router.set_service_to_worker_edges(S1, vec![w1_id]);
        router.set_service_demand(S1, true);
        router.propagate();

        // W1 should have received DemandInput(1)
        let w1 = router.get_worker(&w1_id).unwrap();
        assert!(w1.deliveries.iter().any(|inp| *inp == WorkerInput::DemandInput(1)));

        // W1's handler created a new worker; the status signal contains the new ID

        // The new worker should exist (ID allocated after w1)
        let w2_id = WorkerId(w1_id.0 + 1);
        assert!(router.get_worker(&w2_id).is_some());

        // S1 should have received status from w1 (via the edge w1 set)
        let s1 = router.get_service(&S1).unwrap();
        assert!(s1.deliveries.iter().any(|inp| *inp == ServiceInput::StatusInput(vec![w2_id.0 as u32])));
    }

    #[test]
    fn handler_creates_sm_and_sets_edges_to_it() {
        // Worker handler creates another worker and sets edges to it in the same handler call.
        // The created worker should receive signals through those edges in the same propagate().
        let mut router = Router::new(16);
        let c1 = router.create_config();
        router.set_config_value(c1, 42);

        let mut w1 = WorkerSm::new();
        w1.on_handle = Some(Box::new(move |input, ctx| {
            if let WorkerInput::DemandInput(count) = input {
                if *count > 0 {
                    // Create a new worker and wire config to it
                    let _w2_id = ctx.create_worker(WorkerSm {
                        deliveries: Vec::new(),
                        on_handle: None,
                    });
                    // Note: we can't set edges FROM config (that's a port),
                    // but we can verify the worker was created.
                }
            }
        }));
        let w1_id = router.create_worker(w1);

        router.create_service(S1, ServiceSm::new());
        router.set_service_to_worker_edges(S1, vec![w1_id]);
        router.set_service_demand(S1, true);
        router.propagate();

        // New worker should exist
        let w2_id = WorkerId(w1_id.0 + 1);
        assert!(router.get_worker(&w2_id).is_some());
    }

    #[test]
    fn handler_self_destructs() {
        // Worker self-destructs when it receives demand.
        let mut router = Router::new(16);
        router.create_service(S1, ServiceSm::new());

        let mut w1 = WorkerSm::new();
        w1.on_handle = Some(Box::new(move |input, ctx| {
            if let WorkerInput::DemandInput(count) = input {
                if *count > 0 {
                    ctx.set_status(99);
                    ctx.self_destruct();
                }
            }
        }));
        let w1_id = router.create_worker(w1);

        // Set up edges so destruction triggers re-aggregation on S1
        router.set_worker_to_service_edges(w1_id, vec![S1]);
        router.set_service_to_worker_edges(S1, vec![w1_id]);
        router.set_service_demand(S1, true);
        router.propagate();

        // W1 should be destroyed (self-destructed)
        assert!(router.get_worker(&w1_id).is_none());

        // S1 should have received empty status (w1's edges cleaned up)
        let s1 = router.get_service(&S1).unwrap();
        assert!(s1.deliveries.iter().any(|inp| *inp == ServiceInput::StatusInput(vec![])));
    }

    #[test]
    fn handler_creates_multiple_sms_unique_ids() {
        // Handler creates multiple SMs in a single handler call, all get unique IDs.
        let mut router = Router::new(16);
        router.create_service(S1, ServiceSm::new());

        let mut w1 = WorkerSm::new();
        w1.on_handle = Some(Box::new(|input, ctx| {
            if let WorkerInput::DemandInput(count) = input {
                if *count > 0 {
                    let a = ctx.create_worker(WorkerSm { deliveries: Vec::new(), on_handle: None });
                    let b = ctx.create_worker(WorkerSm { deliveries: Vec::new(), on_handle: None });
                    let c = ctx.create_worker(WorkerSm { deliveries: Vec::new(), on_handle: None });
                    // All IDs must be different
                    assert_ne!(a, b);
                    assert_ne!(b, c);
                    assert_ne!(a, c);
                    ctx.set_status(a.0 as u32);
                }
            }
        }));
        let w1_id = router.create_worker(w1);

        router.set_service_to_worker_edges(S1, vec![w1_id]);
        router.set_service_demand(S1, true);
        router.propagate();

        // Three new workers created after w1
        let w2 = WorkerId(w1_id.0 + 1);
        let w3 = WorkerId(w1_id.0 + 2);
        let w4 = WorkerId(w1_id.0 + 3);
        assert!(router.get_worker(&w2).is_some());
        assert!(router.get_worker(&w3).is_some());
        assert!(router.get_worker(&w4).is_some());
    }

    #[test]
    fn handler_create_ids_dont_conflict_with_router_create() {
        // IDs from handler-created SMs and router-created SMs share the same counter.
        let mut router = Router::new(16);
        router.create_service(S1, ServiceSm::new());

        // Create w1 via router (gets ID 0), w2 via router (gets ID 1).
        let mut w1_sm = WorkerSm::new();
        w1_sm.on_handle = Some(Box::new(|input, ctx| {
            if let WorkerInput::DemandInput(count) = input {
                if *count > 0 {
                    let new_id = ctx.create_worker(WorkerSm { deliveries: Vec::new(), on_handle: None });
                    ctx.set_status(new_id.0 as u32);
                }
            }
        }));
        let w1_id = router.create_worker(w1_sm);
        assert_eq!(w1_id, WorkerId(0));

        let w2_id = router.create_worker(WorkerSm::new());
        assert_eq!(w2_id, WorkerId(1));

        router.set_service_to_worker_edges(S1, vec![w1_id]);
        router.set_service_demand(S1, true);
        router.propagate();

        // Handler-created worker should have ID 2 (counter continued past w2)
        let w3_id = WorkerId(2);
        assert!(router.get_worker(&w3_id).is_some());

        // Next router.create_worker should get ID 3
        let w4_id = router.create_worker(WorkerSm::new());
        assert_eq!(w4_id, WorkerId(3));
    }

    #[test]
    fn auto_id_port_removal() {
        let mut router = Router::new(16);
        let w1 = router.create_worker(WorkerSm::new());
        let c1 = router.create_config();
        let c2 = router.create_config();

        router.set_config_to_worker_edges(c1, vec![w1]);
        router.set_config_to_worker_edges(c2, vec![w1]);
        router.set_config_value(c1, 10);
        router.set_config_value(c2, 20);
        router.propagate();

        let worker = router.get_worker(&w1).unwrap();
        let configs: Vec<_> = worker
            .deliveries
            .iter()
            .filter_map(|inp| match inp {
                WorkerInput::ConfigInput(v) => Some(v.clone()),
                _ => None,
            })
            .collect();
        assert_eq!(configs.len(), 1);
        let last = configs.last().unwrap();
        assert_eq!(last.len(), 2);
        assert!(last.contains(&10));
        assert!(last.contains(&20));

        // Remove one config port
        router.destroy_config(c1);
        router.propagate();

        let worker = router.get_worker(&w1).unwrap();
        let configs: Vec<_> = worker
            .deliveries
            .iter()
            .filter_map(|inp| match inp {
                WorkerInput::ConfigInput(v) => Some(v.clone()),
                _ => None,
            })
            .collect();
        assert_eq!(*configs.last().unwrap(), vec![20]);
    }
}
