use crate::{Aggregator, SmHandler};

// ---- Aggregator ----

#[derive(Default)]
struct SumAggregator;

impl Aggregator for SumAggregator {
    type Input = (AId, u32);
    type Output = u32;

    fn aggregate(&self, inputs: &[(AId, u32)]) -> u32 {
        inputs.iter().map(|(_, v)| v).sum()
    }
}

// ---- Router ----

crate::router! {
    expose_internals_for_testing
    model_checkable

    state_machines {
        A(auto, ASm),
        B(auto, BSm),
    }
    ports {}
    signals {
        A::Level(u32),
        B::Status(u32),
    }
    edges {
        AToB: A -> B,
    }
    events {
        Cmd(String): A -> B,
    }
    inputs {
        B::LevelInput {
            sources: [(AToB, A::Level)],
            aggregator: SumAggregator,
        },
    }
}

// ---- SM handler types (Clone-able) ----

#[derive(Clone)]
struct ASm {
    deliveries: Vec<AInput>,
}

impl ASm {
    fn new() -> Self {
        ASm { deliveries: Vec::new() }
    }
}

impl<C: ACtx> SmHandler<C> for ASm {
    type Input = AInput;
    fn handle(&mut self, input: Self::Input, _ctx: &mut C) {
        self.deliveries.push(input);
    }
}

#[derive(Clone)]
struct BSm {
    deliveries: Vec<BInput>,
}

impl BSm {
    fn new() -> Self {
        BSm { deliveries: Vec::new() }
    }
}

impl<C: BCtx> SmHandler<C> for BSm {
    type Input = BInput;
    fn handle(&mut self, input: Self::Input, _ctx: &mut C) {
        self.deliveries.push(input);
    }
}

// ---- Tests ----

#[test]
fn clone_router_snapshot_equals_original() {
    let mut router = Router::new(10);

    let a1 = router.create_a(ASm::new());
    let b1 = router.create_b(BSm::new());
    router.set_a_to_b_edges(a1, vec![b1]);
    router.set_a_level(a1, 42);
    router.propagate();

    let snap1 = router.snapshot();
    let snap2 = snap1.clone();

    assert_eq!(
        snap1.a_signal_state.get(&a1).map(|s| s.out_level),
        snap2.a_signal_state.get(&a1).map(|s| s.out_level),
    );
    assert_eq!(
        snap1.b_signal_state.get(&b1).map(|s| &s.in_level_input),
        snap2.b_signal_state.get(&b1).map(|s| &s.in_level_input),
    );
}

#[test]
fn clone_router_mutate_clone_does_not_affect_original() {
    let mut router = Router::new(10);

    let a1 = router.create_a(ASm::new());
    let b1 = router.create_b(BSm::new());
    router.set_a_to_b_edges(a1, vec![b1]);
    router.set_a_level(a1, 10);
    router.propagate();

    let mut cloned = router.clone();

    // Mutate the clone
    cloned.set_a_level(a1, 99);
    cloned.propagate();

    // Original should be unchanged
    assert_eq!(
        router.a_signal_state.get(&a1).map(|s| s.out_level),
        Some(10),
    );
    assert_eq!(
        cloned.a_signal_state.get(&a1).map(|s| s.out_level),
        Some(99),
    );
}
