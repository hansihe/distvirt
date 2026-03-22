use crate::{IncrementalAggregator, SmHandler};

// ---- ID types ----

#[derive(Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug)]
struct SrcId(u64);

#[derive(Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug)]
struct DstId(u64);

#[derive(Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug)]
struct AuxId(u64);

// ---- Incremental aggregator ----

#[derive(Debug, Clone, PartialEq)]
enum Change {
    Added(SrcId, bool),
    Removed(SrcId, bool),
    Changed(SrcId, bool, bool),
}

#[derive(Default)]
struct TrackingAggregator;

impl IncrementalAggregator for TrackingAggregator {
    type Input = (SrcId, bool);
    type Output = Change;

    fn added(&self, input: &(SrcId, bool)) -> Option<Change> {
        Some(Change::Added(input.0, input.1))
    }

    fn removed(&self, input: &(SrcId, bool)) -> Option<Change> {
        Some(Change::Removed(input.0, input.1))
    }

    fn changed(&self, old: &(SrcId, bool), new: &(SrcId, bool)) -> Option<Change> {
        Some(Change::Changed(new.0, old.1, new.1))
    }
}

// ---- Filtering aggregator (returns None sometimes) ----

#[derive(Default)]
struct FilteringAggregator;

impl IncrementalAggregator for FilteringAggregator {
    type Input = (SrcId, bool);
    type Output = Change;

    fn added(&self, input: &(SrcId, bool)) -> Option<Change> {
        if input.1 {
            Some(Change::Added(input.0, input.1))
        } else {
            None
        }
    }

    fn removed(&self, input: &(SrcId, bool)) -> Option<Change> {
        Some(Change::Removed(input.0, input.1))
    }

    fn changed(&self, old: &(SrcId, bool), new: &(SrcId, bool)) -> Option<Change> {
        Some(Change::Changed(new.0, old.1, new.1))
    }
}

// ---- Multi-source incremental aggregator ----

#[derive(Debug, Clone, PartialEq)]
enum MultiChange {
    Added(MultiInputSource),
    Removed(MultiInputSource),
    Changed { old: MultiInputSource, new: MultiInputSource },
}

#[derive(Default)]
struct MultiTrackingAggregator;

impl IncrementalAggregator for MultiTrackingAggregator {
    type Input = MultiInputSource;
    type Output = MultiChange;

    fn added(&self, input: &MultiInputSource) -> Option<MultiChange> {
        Some(MultiChange::Added(input.clone()))
    }

    fn removed(&self, input: &MultiInputSource) -> Option<MultiChange> {
        Some(MultiChange::Removed(input.clone()))
    }

    fn changed(&self, old: &MultiInputSource, new: &MultiInputSource) -> Option<MultiChange> {
        Some(MultiChange::Changed { old: old.clone(), new: new.clone() })
    }
}

// ---- Router declaration ----

crate::router! {
    expose_internals_for_testing

    state_machines {
        Src(SrcId, SrcSm),
        Dst(DstId, DstSm),
    }
    ports {
        Aux(AuxId),
    }
    signals {
        Src::Active(bool),
        Aux::Value(u32),
    }
    edges {
        SrcToDst: Src -> Dst,
        AuxToDst: Aux -> Dst,
        SrcToAux: Src -> Aux,
    }
    events {}
    inputs {
        Dst::TrackInput {
            sources: [(SrcToDst, Src::Active)],
            incremental_aggregator: TrackingAggregator,
        },
        Dst::FilterInput {
            sources: [(SrcToDst, Src::Active)],
            incremental_aggregator: FilteringAggregator,
        },
        Dst::MultiInput {
            sources: [(SrcToDst, Src::Active), (AuxToDst, Aux::Value)],
            incremental_aggregator: MultiTrackingAggregator,
        },
        Aux::PortTrackInput {
            sources: [(SrcToAux, Src::Active)],
            incremental_aggregator: TrackingAggregator,
        },
    }
}

// ---- SM types ----

#[derive(Clone)]
struct SrcSm;

impl<C: SrcCtx> SmHandler<C> for SrcSm {
    type Input = SrcInput;
    fn handle(&mut self, _input: Self::Input, _ctx: &mut C) {}
}

struct DstSm {
    deliveries: Vec<DstInput>,
    on_handle: Option<Box<dyn FnMut(&DstInput, &mut dyn DstCtx)>>,
}

impl Clone for DstSm {
    fn clone(&self) -> Self {
        DstSm {
            deliveries: self.deliveries.clone(),
            on_handle: None,
        }
    }
}

impl DstSm {
    fn new() -> Self {
        DstSm {
            deliveries: Vec::new(),
            on_handle: None,
        }
    }

    fn track_changes(&self) -> Vec<&Change> {
        self.deliveries
            .iter()
            .filter_map(|d| match d {
                DstInput::TrackInput(c) => Some(c),
                _ => None,
            })
            .collect()
    }

    fn filter_changes(&self) -> Vec<&Change> {
        self.deliveries
            .iter()
            .filter_map(|d| match d {
                DstInput::FilterInput(c) => Some(c),
                _ => None,
            })
            .collect()
    }

    fn multi_changes(&self) -> Vec<&MultiChange> {
        self.deliveries
            .iter()
            .filter_map(|d| match d {
                DstInput::MultiInput(c) => Some(c),
                _ => None,
            })
            .collect()
    }
}

impl<C: DstCtx> SmHandler<C> for DstSm {
    type Input = DstInput;

    fn handle(&mut self, input: Self::Input, ctx: &mut C) {
        if let Some(ref mut cb) = self.on_handle {
            cb(&input, ctx);
        }
        self.deliveries.push(input);
    }
}

// ---- Constants ----

const S1: SrcId = SrcId(1);
const S2: SrcId = SrcId(2);
const S3: SrcId = SrcId(3);
const D1: DstId = DstId(1);
const X1: AuxId = AuxId(1);

// ============================================================================
// Basic incremental aggregation
// ============================================================================

#[test]
fn added_on_edge_creation() {
    let mut router = Router::new(16);
    router.create_src(S1, SrcSm);
    router.create_dst(D1, DstSm::new());

    // Set signal before edge
    router.set_src_active(S1, true);
    router.propagate();

    // No deliveries yet (no edge)
    assert!(router.get_dst(&D1).unwrap().track_changes().is_empty());

    // Create edge → triggers added
    router.set_src_to_dst_edges(S1, vec![D1]);
    router.propagate();

    let changes = router.get_dst(&D1).unwrap().track_changes();
    assert_eq!(changes.len(), 1);
    assert_eq!(*changes[0], Change::Added(S1, true));
}

#[test]
fn removed_on_edge_removal() {
    let mut router = Router::new(16);
    router.create_src(S1, SrcSm);
    router.create_dst(D1, DstSm::new());

    router.set_src_to_dst_edges(S1, vec![D1]);
    router.set_src_active(S1, true);
    router.propagate();

    // Remove edge → triggers removed
    router.set_src_to_dst_edges(S1, vec![]);
    router.propagate();

    let changes = router.get_dst(&D1).unwrap().track_changes();
    assert_eq!(changes.len(), 2); // added + removed
    assert_eq!(*changes[1], Change::Removed(S1, true));
}

#[test]
fn changed_on_signal_update() {
    let mut router = Router::new(16);
    router.create_src(S1, SrcSm);
    router.create_dst(D1, DstSm::new());

    router.set_src_to_dst_edges(S1, vec![D1]);
    router.set_src_active(S1, false);
    router.propagate();

    // Change signal → triggers changed
    router.set_src_active(S1, true);
    router.propagate();

    let changes = router.get_dst(&D1).unwrap().track_changes();
    assert_eq!(changes.len(), 2); // added(false) + changed(false→true)
    assert_eq!(*changes[1], Change::Changed(S1, false, true));
}

#[test]
fn no_delivery_when_value_unchanged() {
    let mut router = Router::new(16);
    router.create_src(S1, SrcSm);
    router.create_dst(D1, DstSm::new());

    router.set_src_to_dst_edges(S1, vec![D1]);
    router.set_src_active(S1, true);
    router.propagate();

    let count_before = router.get_dst(&D1).unwrap().track_changes().len();

    // Set same value → no delivery (signal setter detects no change, no dirty enqueued)
    router.set_src_active(S1, true);
    router.propagate();

    let count_after = router.get_dst(&D1).unwrap().track_changes().len();
    assert_eq!(count_before, count_after, "unchanged signal should not trigger delivery");
}

// ============================================================================
// Multi-delivery per round
// ============================================================================

#[test]
fn multiple_sources_deliver_individually() {
    let mut router = Router::new(16);
    router.create_src(S1, SrcSm);
    router.create_src(S2, SrcSm);
    router.create_src(S3, SrcSm);
    router.create_dst(D1, DstSm::new());

    router.set_src_to_dst_edges(S1, vec![D1]);
    router.set_src_to_dst_edges(S2, vec![D1]);
    router.set_src_to_dst_edges(S3, vec![D1]);
    router.set_src_active(S1, true);
    router.set_src_active(S2, false);
    router.set_src_active(S3, true);
    router.propagate();

    let changes = router.get_dst(&D1).unwrap().track_changes();
    // Should get 3 individual Added deliveries (one per source)
    assert_eq!(changes.len(), 3);
    // All should be Added (BTreeMap iteration order: S1, S2, S3)
    assert!(changes.iter().all(|c| matches!(c, Change::Added(..))));
}

#[test]
fn mixed_add_remove_change_in_one_round() {
    let mut router = Router::new(16);
    router.create_src(S1, SrcSm);
    router.create_src(S2, SrcSm);
    router.create_src(S3, SrcSm);
    router.create_dst(D1, DstSm::new());

    // Initial state: S1 and S2 connected
    router.set_src_to_dst_edges(S1, vec![D1]);
    router.set_src_to_dst_edges(S2, vec![D1]);
    router.set_src_active(S1, true);
    router.set_src_active(S2, false);
    router.set_src_active(S3, true);
    router.propagate();

    let count_before = router.get_dst(&D1).unwrap().track_changes().len();

    // In one round: change S1, remove S2, add S3
    router.set_src_active(S1, false);
    router.set_src_to_dst_edges(S2, vec![]);
    router.set_src_to_dst_edges(S3, vec![D1]);
    router.propagate();

    let changes = router.get_dst(&D1).unwrap().track_changes();
    let new_changes: Vec<_> = changes[count_before..].to_vec();

    // Should have: Changed(S1), Removed(S2), Added(S3) — though order may vary
    assert_eq!(new_changes.len(), 3);
    assert!(new_changes.iter().any(|c| matches!(c, Change::Changed(id, true, false) if *id == S1)));
    assert!(new_changes.iter().any(|c| matches!(c, Change::Removed(id, false) if *id == S2)));
    assert!(new_changes.iter().any(|c| matches!(c, Change::Added(id, true) if *id == S3)));
}

// ============================================================================
// Filtering (None suppresses delivery)
// ============================================================================

#[test]
fn none_return_suppresses_delivery() {
    let mut router = Router::new(16);
    router.create_src(S1, SrcSm);
    router.create_dst(D1, DstSm::new());

    // FilteringAggregator only fires added() when value is true
    router.set_src_to_dst_edges(S1, vec![D1]);
    router.set_src_active(S1, false); // added returns None for false
    router.propagate();

    let changes = router.get_dst(&D1).unwrap().filter_changes();
    assert_eq!(changes.len(), 0, "added(false) should return None, no delivery");

    // Now set to true → changed fires (always returns Some)
    router.set_src_active(S1, true);
    router.propagate();

    let changes = router.get_dst(&D1).unwrap().filter_changes();
    assert_eq!(changes.len(), 1);
    assert_eq!(*changes[0], Change::Changed(S1, false, true));
}

// ============================================================================
// Self-destruct during incremental delivery
// ============================================================================

#[test]
fn self_destruct_stops_remaining_deliveries() {
    let mut router = Router::new(16);
    router.create_src(S1, SrcSm);
    router.create_src(S2, SrcSm);
    router.create_src(S3, SrcSm);

    let mut dst = DstSm::new();
    let mut call_count = 0;
    dst.on_handle = Some(Box::new(move |_input, ctx| {
        call_count += 1;
        if call_count == 2 {
            ctx.self_destruct();
        }
    }));
    router.create_dst(D1, dst);

    router.set_src_to_dst_edges(S1, vec![D1]);
    router.set_src_to_dst_edges(S2, vec![D1]);
    router.set_src_to_dst_edges(S3, vec![D1]);
    router.set_src_active(S1, true);
    router.set_src_active(S2, true);
    router.set_src_active(S3, true);
    router.propagate();

    // SM should be destroyed
    assert!(router.get_dst(&D1).is_none());
}

// ============================================================================
// Source destruction triggers removed
// ============================================================================

#[test]
fn source_destruction_triggers_removed() {
    let mut router = Router::new(16);
    router.create_src(S1, SrcSm);
    router.create_dst(D1, DstSm::new());

    router.set_src_to_dst_edges(S1, vec![D1]);
    router.set_src_active(S1, true);
    router.propagate();

    // Destroy source
    router.destroy_src(S1);
    router.propagate();

    let changes = router.get_dst(&D1).unwrap().track_changes();
    assert_eq!(changes.len(), 2); // added + removed
    assert_eq!(*changes[1], Change::Removed(S1, true));
}

// ============================================================================
// Multi-source incremental aggregation
// ============================================================================

#[test]
fn multi_source_wraps_in_enum_variants() {
    let mut router = Router::new(16);
    router.create_src(S1, SrcSm);
    router.create_dst(D1, DstSm::new());
    router.create_aux(X1);

    router.set_src_to_dst_edges(S1, vec![D1]);
    router.set_aux_to_dst_edges(X1, vec![D1]);
    router.set_src_active(S1, true);
    router.set_aux_value(X1, 42);
    router.propagate();

    let changes = router.get_dst(&D1).unwrap().multi_changes();
    assert_eq!(changes.len(), 2);

    // Should have Added for both source pairs, wrapped in enum variants
    assert!(changes.iter().any(|c| matches!(c,
        MultiChange::Added(MultiInputSource::SrcActive(id, true)) if *id == S1
    )));
    assert!(changes.iter().any(|c| matches!(c,
        MultiChange::Added(MultiInputSource::AuxValue(id, 42)) if *id == X1
    )));
}

#[test]
fn multi_source_change_one_pair() {
    let mut router = Router::new(16);
    router.create_src(S1, SrcSm);
    router.create_dst(D1, DstSm::new());
    router.create_aux(X1);

    router.set_src_to_dst_edges(S1, vec![D1]);
    router.set_aux_to_dst_edges(X1, vec![D1]);
    router.set_src_active(S1, true);
    router.set_aux_value(X1, 10);
    router.propagate();

    let count_before = router.get_dst(&D1).unwrap().multi_changes().len();

    // Only change one source pair
    router.set_aux_value(X1, 20);
    router.propagate();

    let changes = router.get_dst(&D1).unwrap().multi_changes();
    let new_changes: Vec<_> = changes[count_before..].to_vec();

    // Only the changed pair should fire
    assert_eq!(new_changes.len(), 1);
    assert!(matches!(new_changes[0],
        MultiChange::Changed {
            old: MultiInputSource::AuxValue(_, 10),
            new: MultiInputSource::AuxValue(_, 20),
        }
    ));
}

// ============================================================================
// Port incremental input
// ============================================================================

#[test]
fn port_incremental_input_delivers_individually() {
    let mut router = Router::new(16);
    router.create_src(S1, SrcSm);
    router.create_src(S2, SrcSm);
    router.create_aux(X1);

    router.set_src_to_aux_edges(S1, vec![X1]);
    router.set_src_to_aux_edges(S2, vec![X1]);
    router.set_src_active(S1, true);
    router.set_src_active(S2, false);
    router.propagate();

    // Drain and verify individual deliveries
    let pending = router.drain_aux_inputs();
    let changes: Vec<_> = pending
        .into_iter()
        .map(|(id, AuxPortInput::PortTrackInput(c))| (id, c))
        .collect();

    assert_eq!(changes.len(), 2);
    // Each delivery is individual, both targeting X1
    assert!(changes.iter().all(|(id, _)| *id == X1));
    assert!(changes.iter().any(|(_, c)| *c == Change::Added(S1, true)));
    assert!(changes.iter().any(|(_, c)| *c == Change::Added(S2, false)));
}

#[test]
fn port_incremental_input_tracks_changes() {
    let mut router = Router::new(16);
    router.create_src(S1, SrcSm);
    router.create_aux(X1);

    router.set_src_to_aux_edges(S1, vec![X1]);
    router.set_src_active(S1, false);
    router.propagate();
    router.drain_aux_inputs(); // clear initial added

    // Change signal
    router.set_src_active(S1, true);
    router.propagate();

    let pending = router.drain_aux_inputs();
    let changes: Vec<_> = pending
        .into_iter()
        .map(|(_, AuxPortInput::PortTrackInput(c))| c)
        .collect();

    assert_eq!(changes.len(), 1);
    assert_eq!(changes[0], Change::Changed(S1, false, true));
}

#[test]
fn port_incremental_input_removed_on_edge_removal() {
    let mut router = Router::new(16);
    router.create_src(S1, SrcSm);
    router.create_aux(X1);

    router.set_src_to_aux_edges(S1, vec![X1]);
    router.set_src_active(S1, true);
    router.propagate();
    router.drain_aux_inputs(); // clear initial added

    // Remove edge
    router.set_src_to_aux_edges(S1, vec![]);
    router.propagate();

    let pending = router.drain_aux_inputs();
    let changes: Vec<_> = pending
        .into_iter()
        .map(|(_, AuxPortInput::PortTrackInput(c))| c)
        .collect();

    assert_eq!(changes.len(), 1);
    assert_eq!(changes[0], Change::Removed(S1, true));
}
