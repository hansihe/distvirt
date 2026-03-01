# Stateright Model Optimization

Analysis of state space explosion in `distvirt-orchestrator` stateright harnesses, with concrete optimization strategies.

## Current Performance (release mode)

| Test | Unique States | Time |
|------|--------------|------|
| `check_single_service_activation` | ~5k | fast |
| `check_two_services` | 82,756 | ~3s |
| `check_activation_with_worker_failure` | 377,542 | ~8s |
| `check_two_workers_two_services` | 42,432 | ~2s |
| `check_delete_lifecycle` | ~5k | fast |
| `check_delete_with_worker_failure` | ~10k | fast |

Service and workload sub-models are instant (<1ms, <1k states). The bottleneck is entirely `stateright_model.rs` (the integrated namespace model).

Total suite: ~14s in release.

---

## 1. Remove `step_count` from state (use `target_max_depth` instead)

**Impact: very high (estimated 10x+ state reduction)**

`step_count` is part of `ModelState`'s `Hash`/`PartialEq`. This means the same logical state reached at step 5 and step 7 are treated as different states, completely defeating stateright's state deduplication for convergent paths.

The state machine has many cycles (activate -> idle timeout -> activate, pod fail -> relaunch -> pod fail). Without `step_count` in the state, these cycles collapse to already-visited states. With it, every cycle iteration creates an entirely new frontier of states.

### Fix

Remove `step_count` from `ModelState`. Replace `within_boundary` with stateright's built-in depth limiter:

```rust
.checker()
    .target_max_depth(15.try_into().ok())
    .spawn_dfs()
    .join()
```

`target_max_depth` limits the *path length* during exploration without polluting state identity. The checker still deduplicates identical logical states reached via different-length paths.

---

## 2. Recycle pod IDs instead of monotonic counter

**Impact: high**

`next_pod_id` is a monotonic counter in `ModelState`. Every pod creation permanently diverges the state space. Two execution paths that arrive at the same logical configuration but created different numbers of pods along the way are treated as distinct:

- Path A: launch pod-0, it fails, launch pod-1, it runs -> `next_pod_id=2`
- Path B: launch pod-0, it runs -> `next_pod_id=1`

Even though the logical situation is identical (one running pod), they hash differently.

### Fix

Use a small fixed pool of pod IDs and allocate the lowest free one:

```rust
fn next_pod_id(state: &ModelState) -> PodId {
    for i in 0.. {
        let candidate = PodId(format!("pod-{}", i));
        if !state.namespace.pods.contains_key(&candidate) {
            return candidate;
        }
    }
    unreachable!()
}
```

This way, states converge: after a pod fails and a new one launches, it gets the same ID as if the first one had never existed. The `next_pod_id` field can be removed from `ModelState` entirely.

---

## 3. Deduplicate worker-agnostic actions

**Impact: moderate (reduces branching factor ~2x for multi-worker tests)**

In `actions()`, `ServiceBackendNeed` and `ServiceActivation` events are generated from every worker, but the reporting worker doesn't affect the outcome. For 2 workers x 3 backend need values = 6 actions where 3 would suffice.

### Fix

Generate worker-agnostic service events from only a single canonical worker (e.g., the first in iteration order):

```rust
// Instead of iterating all workers for service events,
// pick one representative worker.
let canonical_worker = ns.workers.keys().next();
if let Some(wid) = canonical_worker {
    // Generate service events only from this worker
}
```

The checker will still explore all reachable states since the worker ID doesn't influence the state transition for these events.

---

## 4. Eliminate snapshot conversion overhead

**Impact: moderate (pure performance, not state space reduction)**

Every `next_state` call performs `to_state_machine()` (BTreeMap->HashMap) then `from_state_machine()` (HashMap->BTreeMap). With hundreds of thousands of states and a branching factor of ~6-10, this means millions of full map conversions with string allocation.

### Fix options

**Option A: BTreeMap in production types.** Make `NamespaceStateMachine` use `BTreeMap`/`BTreeSet` and derive `Hash`/`PartialEq` directly, eliminating the snapshot layer. Downside: BTreeMap is slightly slower for runtime lookups.

**Option B: Integer IDs in the model.** Use `u32` or `u16` newtype IDs instead of `String`-based IDs within the stateright model, mapping to/from string IDs at the boundary. Makes clone/hash/eq much cheaper.

**Option C: Persistent data structures.** Use something like `im::HashMap` that shares structure on clone, reducing allocation in `next_state`.

---

## 5. Worker symmetry reduction

**Impact: moderate-to-high for multi-worker tests**

When workers are interchangeable (same capabilities, no distinguishing state), states that differ only in worker assignment are equivalent. Stateright's `Representative` trait can collapse these.

### Fix

Implement `Representative` on `ModelState` to canonicalize worker assignments:

```rust
impl Representative for ModelState {
    fn representative(&self) -> Self {
        // Sort worker entries by a canonical ordering
        // Remap all worker_id references (in pods, workloads, services)
        // to match the new canonical ordering
    }
}
```

Then enable on the checker:

```rust
.checker()
    .symmetry()
    .spawn_dfs()
```

The stateright reference shows 13x reduction for 2PC with 5 participants. For 2 workers the reduction would be smaller but still meaningful.

---

## Implementation Order

1. **Remove `step_count`** — easiest change, biggest expected win
2. **Pod ID recycling** — moderate change, removes permanent divergence
3. **Deduplicate worker-agnostic actions** — small targeted change
4. **Snapshot conversion** — more invasive, optimize after measuring
5. **Worker symmetry** — requires careful remapping logic, do last
