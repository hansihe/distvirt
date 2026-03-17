use std::collections::{BTreeMap, BTreeSet};

use crate::types::{PodId, PodInfo, WorkerId};

/// Bidirectional pod↔worker tracking.
///
/// Maintains both `pods: BTreeMap<PodId, PodInfo>` and
/// `worker_pods: BTreeMap<WorkerId, BTreeSet<PodId>>` in sync.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PodMap {
    pods: BTreeMap<PodId, PodInfo>,
    worker_pods: BTreeMap<WorkerId, BTreeSet<PodId>>,
}

impl Default for PodMap {
    fn default() -> Self {
        Self::new()
    }
}

impl PodMap {
    pub fn new() -> Self {
        PodMap {
            pods: BTreeMap::new(),
            worker_pods: BTreeMap::new(),
        }
    }

    /// Insert a pod. Panics (debug) if the pod already exists.
    pub fn insert(&mut self, pod_id: PodId, info: PodInfo) {
        debug_assert!(
            !self.pods.contains_key(&pod_id),
            "PodMap::insert: pod {:?} already exists (mapped to worker {:?})",
            pod_id,
            self.pods.get(&pod_id).map(|i| &i.worker_id),
        );
        self.worker_pods
            .entry(info.worker_id.clone())
            .or_default()
            .insert(pod_id.clone());
        self.pods.insert(pod_id, info);
    }

    pub fn remove(&mut self, pod_id: &PodId) -> Option<PodInfo> {
        let info = self.pods.remove(pod_id)?;
        if let Some(set) = self.worker_pods.get_mut(&info.worker_id) {
            set.remove(pod_id);
            if set.is_empty() {
                self.worker_pods.remove(&info.worker_id);
            }
        }
        Some(info)
    }

    /// Remove all pods belonging to a worker. Returns the removed pod IDs.
    pub fn remove_worker_pods(&mut self, worker_id: &WorkerId) -> Vec<PodId> {
        let pod_ids = match self.worker_pods.remove(worker_id) {
            Some(set) => set.into_iter().collect::<Vec<_>>(),
            None => return Vec::new(),
        };
        for pid in &pod_ids {
            self.pods.remove(pid);
        }
        pod_ids
    }

    pub fn clear(&mut self) {
        self.pods.clear();
        self.worker_pods.clear();
    }

    pub fn get(&self, pod_id: &PodId) -> Option<&PodInfo> {
        self.pods.get(pod_id)
    }

    pub fn contains(&self, pod_id: &PodId) -> bool {
        self.pods.contains_key(pod_id)
    }

    pub fn iter(&self) -> impl Iterator<Item = (&PodId, &PodInfo)> {
        self.pods.iter()
    }

    pub fn worker_pod_count(&self, worker_id: &WorkerId) -> usize {
        self.worker_pods.get(worker_id).map_or(0, |set| set.len())
    }

    pub fn len(&self) -> usize {
        self.pods.len()
    }

    pub fn is_empty(&self) -> bool {
        self.pods.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::WorkloadId;

    fn wid(s: &str) -> WorkerId {
        WorkerId(s.into())
    }

    fn pid(s: &str) -> PodId {
        PodId(s.into())
    }

    fn info(worker: &str, workload: &str) -> PodInfo {
        PodInfo {
            worker_id: wid(worker),
            workload_id: WorkloadId(workload.into()),
        }
    }

    /// Verify forward/reverse map consistency.
    fn assert_consistency(map: &PodMap) {
        // Every pod's worker_id must have that pod in worker_pods.
        for (pod_id, pod_info) in &map.pods {
            let set = map
                .worker_pods
                .get(&pod_info.worker_id)
                .expect("worker_pods missing entry for pod's worker_id");
            assert!(
                set.contains(pod_id),
                "worker_pods set for {:?} missing pod {:?}",
                pod_info.worker_id,
                pod_id
            );
        }

        // Per-worker counts from iterating pods must match worker_pods set sizes.
        let mut counts: BTreeMap<WorkerId, usize> = BTreeMap::new();
        for (_pid, pod_info) in &map.pods {
            *counts.entry(pod_info.worker_id.clone()).or_default() += 1;
        }
        for (worker_id, set) in &map.worker_pods {
            assert_eq!(
                set.len(),
                *counts.get(worker_id).unwrap_or(&0),
                "worker_pods set size mismatch for {:?}",
                worker_id
            );
        }
        // No stale worker_pods entries.
        for (worker_id, _) in &map.worker_pods {
            assert!(
                counts.contains_key(worker_id),
                "worker_pods has stale entry for {:?}",
                worker_id
            );
        }

        // len() matches iter().count()
        assert_eq!(map.len(), map.iter().count());
    }

    #[test]
    fn insert_and_get() {
        let mut m = PodMap::new();
        m.insert(pid("p1"), info("w1", "wl1"));
        assert_eq!(m.len(), 1);
        assert!(m.contains(&pid("p1")));
        assert_eq!(m.get(&pid("p1")).unwrap().worker_id, wid("w1"));
        assert_eq!(m.worker_pod_count(&wid("w1")), 1);
        assert_consistency(&m);
    }

    #[test]
    fn insert_multiple_same_worker() {
        let mut m = PodMap::new();
        m.insert(pid("p1"), info("w1", "wl1"));
        m.insert(pid("p2"), info("w1", "wl2"));
        assert_eq!(m.len(), 2);
        assert_eq!(m.worker_pod_count(&wid("w1")), 2);
        assert_consistency(&m);
    }

    #[test]
    fn insert_multiple_different_workers() {
        let mut m = PodMap::new();
        m.insert(pid("p1"), info("w1", "wl1"));
        m.insert(pid("p2"), info("w2", "wl2"));
        assert_eq!(m.worker_pod_count(&wid("w1")), 1);
        assert_eq!(m.worker_pod_count(&wid("w2")), 1);
        assert_consistency(&m);
    }

    #[test]
    fn remove_decrements_count() {
        let mut m = PodMap::new();
        m.insert(pid("p1"), info("w1", "wl1"));
        m.insert(pid("p2"), info("w1", "wl2"));
        let removed = m.remove(&pid("p1"));
        assert!(removed.is_some());
        assert_eq!(m.worker_pod_count(&wid("w1")), 1);
        assert_eq!(m.len(), 1);
        assert_consistency(&m);
    }

    #[test]
    fn remove_last_clears_worker() {
        let mut m = PodMap::new();
        m.insert(pid("p1"), info("w1", "wl1"));
        m.remove(&pid("p1"));
        assert_eq!(m.worker_pod_count(&wid("w1")), 0);
        assert!(m.is_empty());
        assert_consistency(&m);
    }

    #[test]
    fn remove_nonexistent() {
        let mut m = PodMap::new();
        m.insert(pid("p1"), info("w1", "wl1"));
        assert!(m.remove(&pid("p999")).is_none());
        assert_eq!(m.len(), 1);
        assert_consistency(&m);
    }

    #[test]
    fn remove_worker_pods_returns_correct_ids() {
        let mut m = PodMap::new();
        m.insert(pid("p1"), info("w1", "wl1"));
        m.insert(pid("p2"), info("w1", "wl2"));
        m.insert(pid("p3"), info("w2", "wl3"));
        let removed = m.remove_worker_pods(&wid("w1"));
        assert_eq!(removed, vec![pid("p1"), pid("p2")]);
        assert_eq!(m.len(), 1);
        assert_eq!(m.worker_pod_count(&wid("w1")), 0);
        assert_consistency(&m);
    }

    #[test]
    fn remove_worker_pods_empty() {
        let mut m = PodMap::new();
        let removed = m.remove_worker_pods(&wid("w_unknown"));
        assert!(removed.is_empty());
        assert_consistency(&m);
    }

    #[test]
    fn clear_resets_everything() {
        let mut m = PodMap::new();
        m.insert(pid("p1"), info("w1", "wl1"));
        m.insert(pid("p2"), info("w2", "wl2"));
        m.clear();
        assert!(m.is_empty());
        assert_eq!(m.worker_pod_count(&wid("w1")), 0);
        assert_consistency(&m);
    }

    #[test]
    #[cfg(debug_assertions)]
    fn duplicate_insert_panics_debug() {
        let mut m = PodMap::new();
        m.insert(pid("p1"), info("w1", "wl1"));
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            m.insert(pid("p1"), info("w2", "wl2"));
        }));
        assert!(
            result.is_err(),
            "duplicate insert should panic in debug mode"
        );
    }
}
