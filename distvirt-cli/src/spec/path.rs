use std::fmt;

// ---------------------------------------------------------------------------
// Structured YAML paths for error reporting and span lookup
// ---------------------------------------------------------------------------

/// A segment in a YAML path, used both for display and for resolving
/// spans in a MarkedYaml tree.
#[derive(Debug, Clone)]
pub(super) enum PathSegment {
    /// A YAML mapping key (e.g. `workloads`, `api`, `ip`).
    Key(String),
    /// A YAML sequence index (e.g. `containers[0]`).
    Index(usize),
    /// Context for an include entry. Not traversable in YAML tree;
    /// indicates which fragment file an error originates from.
    IncludeEntry { index: usize, file_path: String },
}

/// A structured path into a YAML document.
///
/// Used for:
/// - Formatting human-readable error locations (e.g. `workloads.api.ip`)
/// - Resolving byte spans in a `MarkedYaml` tree for snippet rendering
#[derive(Debug, Clone)]
pub(super) struct YamlPath {
    segments: Vec<PathSegment>,
}

impl YamlPath {
    pub fn root() -> Self {
        Self {
            segments: Vec::new(),
        }
    }

    /// Append a mapping key segment.
    pub fn key(&self, k: impl Into<String>) -> Self {
        let mut new = self.clone();
        new.segments.push(PathSegment::Key(k.into()));
        new
    }

    /// Append a sequence index segment.
    pub fn index(&self, i: usize) -> Self {
        let mut new = self.clone();
        new.segments.push(PathSegment::Index(i));
        new
    }

    /// Append an include entry context segment.
    pub fn include_entry(&self, index: usize, file_path: impl Into<String>) -> Self {
        let mut new = self.clone();
        new.segments.push(PathSegment::IncludeEntry {
            index,
            file_path: file_path.into(),
        });
        new
    }

    /// Return only the segments that can be traversed in a YAML tree
    /// (i.e. `Key` and `Index`, skipping `IncludeEntry`).
    pub fn traversable_segments(&self) -> impl Iterator<Item = &PathSegment> {
        self.segments.iter().filter(|s| matches!(s, PathSegment::Key(_) | PathSegment::Index(_)))
    }
}

impl fmt::Display for YamlPath {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut first = true;
        let mut after_include = false;

        for seg in &self.segments {
            match seg {
                PathSegment::Key(k) => {
                    if after_include {
                        write!(f, " > {}", k)?;
                        after_include = false;
                    } else if first {
                        write!(f, "{}", k)?;
                    } else {
                        write!(f, ".{}", k)?;
                    }
                    first = false;
                }
                PathSegment::Index(i) => {
                    write!(f, "[{}]", i)?;
                    first = false;
                    after_include = false;
                }
                PathSegment::IncludeEntry { index, file_path } => {
                    if !first {
                        write!(f, ".")?;
                    }
                    write!(f, "include[{}] ({})", index, file_path)?;
                    first = false;
                    after_include = true;
                }
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn simple_key_path() {
        let p = YamlPath::root().key("workloads").key("api").key("ip");
        assert_eq!(p.to_string(), "workloads.api.ip");
    }

    #[test]
    fn path_with_index() {
        let p = YamlPath::root()
            .key("workloads")
            .key("api")
            .key("containers")
            .index(0)
            .key("image");
        assert_eq!(p.to_string(), "workloads.api.containers[0].image");
    }

    #[test]
    fn include_entry_path() {
        let p = YamlPath::root()
            .include_entry(0, "app.yaml")
            .key("workloads")
            .key("api");
        assert_eq!(p.to_string(), "include[0] (app.yaml) > workloads.api");
    }

    #[test]
    fn include_entry_nested() {
        let p = YamlPath::root()
            .include_entry(1, "db.yaml")
            .key("workloads")
            .key("db")
            .key("containers")
            .index(0)
            .key("image");
        assert_eq!(
            p.to_string(),
            "include[1] (db.yaml) > workloads.db.containers[0].image"
        );
    }

    #[test]
    fn empty_path() {
        let p = YamlPath::root();
        assert_eq!(p.to_string(), "");
    }
}
