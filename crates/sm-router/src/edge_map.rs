use std::collections::{BTreeMap, BTreeSet};

/// Diff produced by [`EdgeMap::set_edges`] when edges actually changed.
pub struct EdgeDiff<Tgt> {
    pub added: Vec<Tgt>,
    pub removed: Vec<Tgt>,
}

impl<Tgt> EdgeDiff<Tgt> {
    /// Iterate over all changed targets (removed then added).
    pub fn all_changed(&self) -> impl Iterator<Item = &Tgt> {
        self.removed.iter().chain(self.added.iter())
    }
}

/// Bidirectional edge map maintaining forward (source -> targets) and reverse
/// (target -> sources) indices.
///
/// Used by the generated router to track edge relationships between nodes.
/// The forward map stores the ordered target list per source. The reverse map
/// is a derived index for efficient source lookups by target.
#[derive(Clone)]
pub struct EdgeMap<Src, Tgt> {
    fwd: BTreeMap<Src, Vec<Tgt>>,
    rev: BTreeMap<Tgt, BTreeSet<Src>>,
}

impl<Src: Ord + Copy, Tgt: Ord + Copy> EdgeMap<Src, Tgt> {
    pub fn new() -> Self {
        Self {
            fwd: BTreeMap::new(),
            rev: BTreeMap::new(),
        }
    }

    /// Get the target list for a source.
    pub fn targets(&self, source: &Src) -> Option<&[Tgt]> {
        self.fwd.get(source).map(|v| v.as_slice())
    }

    /// Get the source set for a target.
    pub fn sources(&self, target: &Tgt) -> Option<&BTreeSet<Src>> {
        self.rev.get(target)
    }

    /// Read access to the full forward map.
    pub fn fwd(&self) -> &BTreeMap<Src, Vec<Tgt>> {
        &self.fwd
    }

    /// Reconstruct an EdgeMap from a forward map (rebuilds reverse index).
    pub fn from_fwd(fwd: BTreeMap<Src, Vec<Tgt>>) -> Self {
        let mut rev: BTreeMap<Tgt, BTreeSet<Src>> = BTreeMap::new();
        for (src, targets) in &fwd {
            for tgt in targets {
                rev.entry(*tgt).or_default().insert(*src);
            }
        }
        Self { fwd, rev }
    }

    /// Update edges from `source` to `new_targets`.
    ///
    /// Returns `Some(diff)` if the edge set changed, `None` if unchanged.
    pub fn set_edges(
        &mut self,
        source: Src,
        new_targets: impl IntoIterator<Item = Tgt>,
    ) -> Option<EdgeDiff<Tgt>> {
        let new_targets: Vec<Tgt> = new_targets.into_iter().collect();

        // Fast path: clearing all edges
        if new_targets.is_empty() {
            if let Some(old_targets) = self.fwd.remove(&source) {
                if old_targets.is_empty() {
                    return None;
                }
                for tgt in &old_targets {
                    if let Some(sources) = self.rev.get_mut(tgt) {
                        sources.remove(&source);
                        if sources.is_empty() {
                            self.rev.remove(tgt);
                        }
                    }
                }
                return Some(EdgeDiff {
                    added: Vec::new(),
                    removed: old_targets,
                });
            }
            return None;
        }

        let old_set: BTreeSet<Tgt> = self
            .fwd
            .get(&source)
            .map(|v| v.iter().copied().collect())
            .unwrap_or_default();
        let new_set: BTreeSet<Tgt> = new_targets.iter().copied().collect();

        let removed: Vec<Tgt> = old_set.difference(&new_set).copied().collect();
        let added: Vec<Tgt> = new_set.difference(&old_set).copied().collect();

        if removed.is_empty() && added.is_empty() {
            return None;
        }

        self.fwd.insert(source, new_targets);

        for &tgt in &removed {
            if let Some(sources) = self.rev.get_mut(&tgt) {
                sources.remove(&source);
                if sources.is_empty() {
                    self.rev.remove(&tgt);
                }
            }
        }
        for &tgt in &added {
            self.rev.entry(tgt).or_default().insert(source);
        }

        Some(EdgeDiff { added, removed })
    }

    /// Remove a target from all edges (cleanup when a target node is destroyed).
    ///
    /// This removes the target from the reverse index and cleans up forward entries.
    /// Does not produce dirty notifications — the caller handles that if needed.
    pub fn remove_target(&mut self, target: &Tgt) {
        if let Some(sources) = self.rev.remove(target) {
            for source_id in sources {
                if let Some(targets) = self.fwd.get_mut(&source_id) {
                    targets.retain(|t| t != target);
                    if targets.is_empty() {
                        self.fwd.remove(&source_id);
                    }
                }
            }
        }
    }
}

impl<Src: Ord + Copy, Tgt: Ord + Copy> Default for EdgeMap<Src, Tgt> {
    fn default() -> Self {
        Self::new()
    }
}
