// Edge source type doesn't match signal node — should fail validation.

use distvirt_sm_router::{trace, Aggregator, ListAggregator, SmHandler};

#[derive(Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug)]
struct AlphaId(u64);

#[derive(Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug)]
struct BetaId(u64);

struct AlphaSm;
impl SmHandler for AlphaSm {
    type Input = AlphaInput;
    type Ctx = AlphaCtx;
    fn handle(&mut self, _input: Self::Input, _ctx: &mut Self::Ctx) {}
}

struct BetaSm;
impl SmHandler for BetaSm {
    type Input = BetaInput;
    type Ctx = BetaCtx;
    fn handle(&mut self, _input: Self::Input, _ctx: &mut Self::Ctx) {}
}

distvirt_sm_router::router! {
    state_machines {
        Alpha(AlphaId, AlphaSm),
        Beta(BetaId, BetaSm),
    }
    ports {}
    signals {
        Alpha::Demand(bool),
        Beta::Status(u32),
    }
    edges {
        AlphaToBeta: Alpha -> Beta,
    }
    inputs {
        // BUG: AlphaToBeta source is Alpha, but we reference Beta::Status
        Beta::StatusInput {
            sources: [(AlphaToBeta, Beta::Status)],
            aggregator: ListAggregator<BetaId, u32>,
        },
    }
}

fn main() {}
