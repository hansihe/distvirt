// Signal value type doesn't implement PartialEq — should fail.

use distvirt_sm_router::{Aggregator, ListAggregator, SmHandler};

#[derive(Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug)]
struct AlphaId(u64);

#[derive(Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug)]
struct BetaId(u64);

// Deliberately missing PartialEq
#[derive(Clone, Debug, Default)]
struct BadSignalType(u32);

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

#[derive(Default)]
struct BadAggregator;
impl Aggregator for BadAggregator {
    type Input = (AlphaId, BadSignalType);
    type Output = Vec<BadSignalType>;
    fn aggregate(&self, inputs: &[(AlphaId, BadSignalType)]) -> Vec<BadSignalType> {
        inputs.iter().map(|(_, v)| v.clone()).collect()
    }
}

distvirt_sm_router::router! {
    state_machines {
        Alpha(AlphaId, AlphaSm),
        Beta(BetaId, BetaSm),
    }
    ports {}
    signals {
        Alpha::Value(BadSignalType),
        Beta::Status(u32),
    }
    edges {
        AlphaToBeta: Alpha -> Beta,
    }
    inputs {
        Beta::ValueInput {
            sources: [(AlphaToBeta, Alpha::Value)],
            aggregator: BadAggregator,
        },
    }
}

fn main() {}
