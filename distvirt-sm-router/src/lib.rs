/// Aggregator trait: reduces N input values into one output.
///
/// The `Input` type is determined by the topology:
/// - Single-source inputs: `(SourceId, SignalValue)` tuple
/// - Multi-source inputs: a generated enum with one variant per source pair
///
/// Aggregators must handle the empty-input case (zero edges).
pub trait Aggregator {
    type Input;
    type Output;
    fn aggregate(&self, inputs: &[Self::Input]) -> Self::Output;
}

/// SM handler trait. Each SM type implements this on its struct.
///
/// Associated types are generated per-SM-type by the `router!` macro:
/// - `Input`: enum with one variant per aggregated input + event channel
/// - `Ctx`: struct exposing only the signals and edges this SM type can produce
pub trait SmHandler {
    type Input;
    type Ctx;
    fn handle(&mut self, input: Self::Input, ctx: &mut Self::Ctx);
}

/// Built-in aggregator: collects all signal values into a Vec.
/// Works with any `(Id, Value)` input tuple. SMs enforce cardinality
/// invariants themselves (e.g., expect len 0..1).
pub struct ListAggregator<Id, V>(std::marker::PhantomData<(Id, V)>);

impl<Id, V> ListAggregator<Id, V> {
    pub fn new() -> Self {
        ListAggregator(std::marker::PhantomData)
    }
}

impl<Id, V: Clone> Aggregator for ListAggregator<Id, V> {
    type Input = (Id, V);
    type Output = Vec<V>;

    fn aggregate(&self, inputs: &[(Id, V)]) -> Vec<V> {
        inputs.iter().map(|(_, v)| v.clone()).collect()
    }
}

#[cfg(test)]
mod tests;
