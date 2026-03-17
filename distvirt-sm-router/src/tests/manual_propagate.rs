use crate::{Aggregator, Delivery, ListAggregator, SmHandler};

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

// ---- Topology ----

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
        GammaToAlpha: Gamma -> Alpha,
        GammaToBeta: Gamma -> Beta,
    }
    events {
        GammaEvent(u32): Gamma -> Beta,
        SmEvent(u32): Alpha -> Beta,
    }
    inputs {
        Beta::DemandInput {
            sources: [(AlphaToBeta, Alpha::Demand)],
            aggregator: CountTrueAggregator,
        },
        Alpha::StatusInput {
            sources: [(BetaToAlpha, Beta::Status)],
            aggregator: ListAggregator<BetaId, u32>,
        },
        Alpha::GammaValueInput {
            sources: [(GammaToAlpha, Gamma::Value)],
            aggregator: ListAggregator<GammaId, u32>,
        },
    }
}

// ---- SM types ----

struct AlphaSm {
    deliveries: Vec<AlphaInput>,
    on_handle: Option<Box<dyn FnMut(&AlphaInput, &mut dyn AlphaCtx)>>,
}

impl Clone for AlphaSm {
    fn clone(&self) -> Self {
        AlphaSm {
            deliveries: self.deliveries.clone(),
            on_handle: None,
        }
    }
}

impl AlphaSm {
    fn new() -> Self {
        AlphaSm {
            deliveries: Vec::new(),
            on_handle: None,
        }
    }
}

impl<C: AlphaCtx> SmHandler<C> for AlphaSm {
    type Input = AlphaInput;

    fn handle(&mut self, input: Self::Input, ctx: &mut C) {
        if let Some(ref mut cb) = self.on_handle {
            cb(&input, ctx);
        }
        self.deliveries.push(input);
    }
}

struct BetaSm {
    deliveries: Vec<BetaInput>,
    on_handle: Option<Box<dyn FnMut(&BetaInput, &mut dyn BetaCtx)>>,
}

impl Clone for BetaSm {
    fn clone(&self) -> Self {
        BetaSm {
            deliveries: self.deliveries.clone(),
            on_handle: None,
        }
    }
}

impl BetaSm {
    fn new() -> Self {
        BetaSm {
            deliveries: Vec::new(),
            on_handle: None,
        }
    }
}

impl<C: BetaCtx> SmHandler<C> for BetaSm {
    type Input = BetaInput;

    fn handle(&mut self, input: Self::Input, ctx: &mut C) {
        if let Some(ref mut cb) = self.on_handle {
            cb(&input, ctx);
        }
        self.deliveries.push(input);
    }
}

// ---- Constants ----

const A1: AlphaId = AlphaId(1);
const B1: BetaId = BetaId(1);
const B2: BetaId = BetaId(2);
const G1: GammaId = GammaId(1);

// ---- Helper: drain one sub-round ----

fn drain_all(router: &mut Router, mp: &mut crate::ManualPropagate<PendingDelivery>) {
    while let Some(group) = mp.next_group() {
        for delivery in group {
            router.deliver_one(delivery);
        }
    }
}

/// Drain a full round (inputs sub-round + events sub-round).
fn drain_round(router: &mut Router) {
    let mut mp = router.begin_manual_propagate(); // Inputs
    drain_all(router, &mut mp);
    let mut mp = router.begin_manual_propagate(); // Events
    drain_all(router, &mut mp);
}

// ---- Tests ----

#[test]
fn manual_propagate_basic() {
    let mut router = Router::new(16);
    router.create_alpha(A1, AlphaSm::new());
    router.create_beta(B1, BetaSm::new());

    router.set_alpha_to_beta_edges(A1, vec![B1]);
    router.propagate();

    // Now set demand and use manual propagation
    router.set_alpha_demand(A1, true);

    // Two begin calls per round: inputs then events
    drain_round(&mut router);
    assert!(router.is_quiescent());

    let b1 = router.get_beta(&B1).unwrap();
    assert!(
        b1.deliveries
            .iter()
            .any(|inp| *inp == BetaInput::DemandInput(1))
    );
}

#[test]
fn manual_propagate_grouping() {
    let mut router = Router::new(16);
    router.create_alpha(A1, AlphaSm::new());
    router.create_beta(B1, BetaSm::new());
    router.create_beta(B2, BetaSm::new());
    router.create_gamma(G1);

    router.set_alpha_to_beta_edges(A1, vec![B1, B2]);
    router.set_gamma_to_alpha_edges(G1, vec![A1]);
    router.propagate();

    // Dirty Beta's DemandInput (via Alpha::Demand) and Alpha's GammaValueInput (via Gamma::Value)
    router.set_alpha_demand(A1, true);
    router.set_gamma_value(G1, 42);

    // First begin: inputs sub-round only
    let mut mp = router.begin_manual_propagate();

    // Collect all groups and their keys
    let mut groups: Vec<Vec<PendingDelivery>> = Vec::new();
    while let Some(group) = mp.next_group() {
        groups.push(group);
    }

    // Should have at least 2 groups: one for Beta inputs, one for Alpha inputs
    assert!(
        groups.len() >= 2,
        "expected at least 2 groups, got {}",
        groups.len()
    );

    // All deliveries within a group should have the same group_key
    for group in &groups {
        let key = group[0].group_key();
        for d in group {
            assert_eq!(d.group_key(), key);
        }
    }

    // Different groups should have different keys
    let keys: Vec<_> = groups.iter().map(|g| g[0].group_key()).collect();
    for i in 0..keys.len() {
        for j in (i + 1)..keys.len() {
            assert_ne!(keys[i], keys[j], "groups {} and {} have same key", i, j);
        }
    }

    // Deliver everything
    for group in groups {
        for delivery in group {
            router.deliver_one(delivery);
        }
    }

    // Complete the round with events sub-round
    let mut mp = router.begin_manual_propagate();
    drain_all(&mut router, &mut mp);
}

#[test]
fn manual_propagate_dirty_and_events() {
    let mut router = Router::new(16);
    router.create_alpha(A1, AlphaSm::new());
    router.create_beta(B1, BetaSm::new());
    router.create_gamma(G1);

    router.set_alpha_to_beta_edges(A1, vec![B1]);
    router.set_gamma_to_beta_edges(G1, vec![B1]);
    router.propagate();

    // Create both dirty input and pre-queued event targeting Beta
    router.set_alpha_demand(A1, true);
    router.send_gamma_event(G1, B1, 99);

    // First begin: inputs sub-round only (dirty inputs)
    let mut mp = router.begin_manual_propagate();
    let mut input_deliveries = Vec::new();
    while let Some(group) = mp.next_group() {
        input_deliveries.extend(group);
    }
    // Should only have dirty inputs
    assert!(
        !input_deliveries.is_empty(),
        "expected dirty inputs in first sub-round"
    );
    for d in &input_deliveries {
        assert!(
            matches!(d, PendingDelivery::DirtyInput(_)),
            "first sub-round should only contain dirty inputs"
        );
    }
    for d in input_deliveries {
        router.deliver_one(d);
    }

    // Second begin: events sub-round (pre-queued event)
    let mut mp = router.begin_manual_propagate();
    let mut event_deliveries = Vec::new();
    while let Some(group) = mp.next_group() {
        event_deliveries.extend(group);
    }
    // Should have the pre-queued event
    assert!(
        !event_deliveries.is_empty(),
        "expected events in second sub-round"
    );
    for d in &event_deliveries {
        assert!(
            matches!(d, PendingDelivery::Event(_)),
            "second sub-round should only contain events"
        );
    }
    for d in event_deliveries {
        router.deliver_one(d);
    }

    let b1 = router.get_beta(&B1).unwrap();
    assert!(
        b1.deliveries
            .iter()
            .any(|inp| *inp == BetaInput::DemandInput(1))
    );
    assert!(
        b1.deliveries
            .iter()
            .any(|inp| *inp == BetaInput::GammaEvent(99))
    );
}

#[test]
fn manual_propagate_cascading() {
    let mut router = Router::new(16);
    router.create_alpha(A1, AlphaSm::new());

    // Beta sets Status when it receives DemandInput > 0
    let mut b1 = BetaSm::new();
    b1.on_handle = Some(Box::new(|input, ctx| {
        if let BetaInput::DemandInput(count) = input {
            if *count > 0 {
                ctx.set_beta_to_alpha_edges(vec![A1]);
                ctx.set_status(42);
            }
        }
    }));
    router.create_beta(B1, b1);

    router.set_alpha_to_beta_edges(A1, vec![B1]);
    router.propagate();

    // Trigger cascade: Alpha::Demand -> Beta::DemandInput -> Beta::Status -> Alpha::StatusInput
    router.set_alpha_demand(A1, true);

    // First round: delivers DemandInput to Beta (inputs + events)
    drain_round(&mut router);

    // Not quiescent: Beta set Status, so Alpha's StatusInput is now dirty
    assert!(!router.is_quiescent());

    // Second round: delivers StatusInput to Alpha (inputs + events)
    drain_round(&mut router);

    assert!(router.is_quiescent());

    let a1 = router.get_alpha(&A1).unwrap();
    assert!(
        a1.deliveries
            .iter()
            .any(|inp| *inp == AlphaInput::StatusInput(vec![42]))
    );
}

#[test]
fn manual_propagate_empty() {
    let mut router = Router::new(16);
    router.create_alpha(A1, AlphaSm::new());
    router.create_beta(B1, BetaSm::new());
    router.set_alpha_to_beta_edges(A1, vec![B1]);
    router.propagate();

    // No pending work — still need both begin calls to complete a round
    let mut mp = router.begin_manual_propagate();
    assert_eq!(mp.remaining(), 0);
    assert!(mp.next_group().is_none());

    let mut mp = router.begin_manual_propagate();
    assert_eq!(mp.remaining(), 0);
    assert!(mp.next_group().is_none());

    assert!(router.is_quiescent());
}

#[test]
fn manual_propagate_materializes_creates() {
    let mut router = Router::new(16);

    // Alpha creates Beta in its handler
    let mut a1 = AlphaSm::new();
    a1.on_handle = Some(Box::new(|input, ctx| {
        if let AlphaInput::GammaValueInput(_) = input {
            ctx.create_beta(B1, BetaSm::new());
            ctx.set_alpha_to_beta_edges(vec![B1]);
        }
    }));
    router.create_alpha(A1, a1);
    router.create_gamma(G1);

    router.set_gamma_to_alpha_edges(G1, vec![A1]);
    router.propagate();

    // Trigger Alpha handler which creates Beta
    router.set_gamma_value(G1, 10);

    // First round: delivers GammaValueInput to Alpha, which creates Beta
    drain_round(&mut router);

    // Not quiescent: Beta was just created and has dirty inputs from the edge
    assert!(
        !router.is_quiescent(),
        "should not be quiescent after creating Beta with edges"
    );

    // Second round: should materialize Beta and deliver its dirty inputs
    drain_round(&mut router);

    // Beta should now exist
    assert!(
        router.get_beta(&B1).is_some(),
        "Beta should be materialized"
    );
}

#[test]
fn manual_propagate_remaining_tracks() {
    let mut router = Router::new(16);
    router.create_alpha(A1, AlphaSm::new());
    router.create_beta(B1, BetaSm::new());
    router.create_beta(B2, BetaSm::new());
    router.create_gamma(G1);

    router.set_alpha_to_beta_edges(A1, vec![B1, B2]);
    router.set_gamma_to_alpha_edges(G1, vec![A1]);
    router.propagate();

    // Create multiple dirty inputs
    router.set_alpha_demand(A1, true);
    router.set_gamma_value(G1, 5);

    let mut mp = router.begin_manual_propagate();
    let initial = mp.remaining();
    assert!(initial > 0);

    let mut delivered = 0;
    while let Some(group) = mp.next_group() {
        delivered += group.len();
        // remaining should have decreased
        assert_eq!(mp.remaining(), initial - delivered);
        for delivery in group {
            router.deliver_one(delivery);
        }
    }

    assert_eq!(mp.remaining(), 0);
}

#[test]
#[should_panic(expected = "begin_manual_propagate() called with")]
fn manual_propagate_panics_on_double_begin() {
    let mut router = Router::new(16);
    router.create_alpha(A1, AlphaSm::new());
    router.create_beta(B1, BetaSm::new());

    router.set_alpha_to_beta_edges(A1, vec![B1]);
    router.propagate();

    router.set_alpha_demand(A1, true);

    let _mp = router.begin_manual_propagate();
    // Don't deliver — call begin again (should panic: outstanding inputs)
    let _mp2 = router.begin_manual_propagate();
}

#[test]
#[should_panic(expected = "propagate() called while step-by-step propagation is in progress")]
fn manual_propagate_panics_on_propagate_during() {
    let mut router = Router::new(16);
    router.create_alpha(A1, AlphaSm::new());
    router.create_beta(B1, BetaSm::new());

    router.set_alpha_to_beta_edges(A1, vec![B1]);
    router.propagate();

    router.set_alpha_demand(A1, true);

    let _mp = router.begin_manual_propagate();
    // Don't deliver — call propagate
    router.propagate();
}

/// Test that events queued by input handlers are captured in the events sub-round.
/// This is the key bug that the phasing fix addresses.
#[test]
fn manual_propagate_events_from_input_handlers() {
    let mut router = Router::new(16);

    // Alpha sends an event to Beta when it receives GammaValueInput
    let mut a1 = AlphaSm::new();
    a1.on_handle = Some(Box::new(|input, ctx| {
        if let AlphaInput::GammaValueInput(_) = input {
            ctx.send_sm_event(B1, 777);
        }
    }));
    router.create_alpha(A1, a1);
    router.create_beta(B1, BetaSm::new());
    router.create_gamma(G1);

    router.set_gamma_to_alpha_edges(G1, vec![A1]);
    router.set_alpha_to_beta_edges(A1, vec![B1]);
    router.propagate();

    // Trigger: Gamma sets value -> Alpha gets GammaValueInput -> Alpha sends SmEvent to Beta
    router.set_gamma_value(G1, 42);

    // First begin: inputs sub-round — delivers GammaValueInput to Alpha
    let mut mp = router.begin_manual_propagate();
    let mut inputs = Vec::new();
    while let Some(group) = mp.next_group() {
        inputs.extend(group);
    }
    assert!(!inputs.is_empty(), "should have dirty input for Alpha");
    for d in inputs {
        router.deliver_one(d);
    }

    // Second begin: events sub-round — should contain the SmEvent queued by Alpha's handler
    let mut mp = router.begin_manual_propagate();
    let mut events = Vec::new();
    while let Some(group) = mp.next_group() {
        events.extend(group);
    }
    assert!(
        !events.is_empty(),
        "events sub-round should contain the SmEvent queued during input handling"
    );
    for d in &events {
        assert!(matches!(d, PendingDelivery::Event(_)));
    }
    for d in events {
        router.deliver_one(d);
    }

    // Beta should have received the event
    let b1 = router.get_beta(&B1).unwrap();
    assert!(
        b1.deliveries
            .iter()
            .any(|inp| *inp == BetaInput::SmEvent(777)),
        "Beta should have received SmEvent(777), got: {:?}",
        b1.deliveries
    );
}
