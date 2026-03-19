use saphyr::{LoadableYamlNode, MarkedYaml};

use super::path::{PathSegment, YamlPath};

// ---------------------------------------------------------------------------
// Span resolution: walk a MarkedYaml tree following a YamlPath
// ---------------------------------------------------------------------------

/// A resolved byte range in a source file.
#[derive(Debug, Clone)]
pub(super) struct ResolvedSpan {
    /// Byte offset of the start of the node.
    pub start: usize,
    /// Byte offset of the end of the node.
    pub end: usize,
}

/// Try to resolve a `YamlPath` to a byte span in the given YAML source.
///
/// Parses the source with saphyr's `MarkedYaml` loader, then walks the tree
/// following the path segments. Returns `None` if parsing fails or the path
/// doesn't exist in the tree.
pub(super) fn resolve_span(source: &str, path: &YamlPath) -> Option<ResolvedSpan> {
    let docs = MarkedYaml::load_from_str(source).ok()?;
    let doc = docs.into_iter().next()?;

    let mut current = &doc;

    for segment in path.traversable_segments() {
        match segment {
            PathSegment::Key(key) => {
                current = current.data.as_mapping_get(key)?;
            }
            PathSegment::Index(idx) => {
                current = current.data.as_sequence_get(*idx)?;
            }
            PathSegment::IncludeEntry { .. } => {
                // Not traversable — should have been filtered out
                unreachable!("IncludeEntry should not appear in traversable_segments");
            }
        }
    }

    let start = current.span.start.index();
    let end = current.span.end.index();

    // Ensure we have a non-empty span
    if start == end {
        return None;
    }

    Some(ResolvedSpan { start, end })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::spec::path::YamlPath;

    #[test]
    fn resolve_simple_key() {
        let yaml = "apiVersion: v1\nkind: Namespace\n";
        let path = YamlPath::root().key("kind");
        let span = resolve_span(yaml, &path).unwrap();
        assert_eq!(&yaml[span.start..span.end], "Namespace");
    }

    #[test]
    fn resolve_nested_key() {
        let yaml = "network:\n  subnet: 172.16.0.0/24\n";
        let path = YamlPath::root().key("network").key("subnet");
        let span = resolve_span(yaml, &path).unwrap();
        assert_eq!(&yaml[span.start..span.end], "172.16.0.0/24");
    }

    #[test]
    fn resolve_sequence_index() {
        let yaml = "items:\n  - first\n  - second\n  - third\n";
        let path = YamlPath::root().key("items").index(1);
        let span = resolve_span(yaml, &path).unwrap();
        assert_eq!(&yaml[span.start..span.end], "second");
    }

    #[test]
    fn resolve_mapping_in_sequence() {
        let yaml = "containers:\n  - image: foo\n  - image: bar\n";
        let path = YamlPath::root().key("containers").index(1).key("image");
        let span = resolve_span(yaml, &path).unwrap();
        assert_eq!(&yaml[span.start..span.end], "bar");
    }

    #[test]
    fn resolve_skips_include_entry() {
        // IncludeEntry segments are skipped; only Key/Index are traversed
        let yaml = "workloads:\n  api:\n    ip: 10.0.0.1\n";
        let path = YamlPath::root()
            .include_entry(0, "frag.yaml")
            .key("workloads")
            .key("api")
            .key("ip");
        let span = resolve_span(yaml, &path).unwrap();
        assert_eq!(&yaml[span.start..span.end], "10.0.0.1");
    }

    #[test]
    fn resolve_nonexistent_returns_none() {
        let yaml = "a: 1\n";
        let path = YamlPath::root().key("b");
        assert!(resolve_span(yaml, &path).is_none());
    }
}
