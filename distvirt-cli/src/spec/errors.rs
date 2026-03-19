use std::fmt;

use annotate_snippets::{Level, Renderer, Snippet};

use super::path::YamlPath;
use super::snippet::resolve_span;

// ---------------------------------------------------------------------------
// SpecErrors — multi-error collector with source-aware rendering
// ---------------------------------------------------------------------------

/// Identifies which source file an error's path should be resolved against.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct SourceId(usize);

/// A single validation error with a structured path indicating where in the spec it occurred.
#[derive(Debug)]
struct SpecError {
    path: YamlPath,
    message: String,
    source_id: SourceId,
}

/// A registered source file for span resolution.
struct Source {
    name: String,
    content: String,
}

/// Collects validation errors and warnings. If any errors are present after
/// validation, they are all reported together with source snippets.
pub(super) struct SpecErrors {
    sources: Vec<Source>,
    errors: Vec<SpecError>,
    warnings: Vec<SpecError>,
    default_source: SourceId,
}

impl SpecErrors {
    pub(super) fn new() -> Self {
        Self {
            sources: Vec::new(),
            errors: Vec::new(),
            warnings: Vec::new(),
            default_source: SourceId(0),
        }
    }

    /// Register a source file for span resolution. Returns a `SourceId` that
    /// can be passed to `error_in` / `warn_in`.
    pub(super) fn add_source(&mut self, name: impl Into<String>, content: impl Into<String>) -> SourceId {
        let id = SourceId(self.sources.len());
        self.sources.push(Source {
            name: name.into(),
            content: content.into(),
        });
        id
    }

    pub(super) fn error(&mut self, path: YamlPath, msg: impl Into<String>) {
        let source_id = self.default_source;
        self.error_in(source_id, path, msg);
    }

    pub(super) fn warn(&mut self, path: YamlPath, msg: impl Into<String>) {
        let source_id = self.default_source;
        self.warn_in(source_id, path, msg);
    }

    pub(super) fn error_in(&mut self, source_id: SourceId, path: YamlPath, msg: impl Into<String>) {
        self.errors.push(SpecError {
            path,
            message: msg.into(),
            source_id,
        });
    }

    pub(super) fn warn_in(&mut self, source_id: SourceId, path: YamlPath, msg: impl Into<String>) {
        self.warnings.push(SpecError {
            path,
            message: msg.into(),
            source_id,
        });
    }

    pub(super) fn has_errors(&self) -> bool {
        !self.errors.is_empty()
    }

    /// Format all errors/warnings into a single anyhow::Error, or Ok(()) if none.
    pub(super) fn into_result(self) -> anyhow::Result<()> {
        if self.errors.is_empty() && self.warnings.is_empty() {
            return Ok(());
        }
        if self.errors.is_empty() {
            // Warnings only — log them but don't fail
            for w in &self.warnings {
                log::warn!("{} — {}", w.path, w.message);
            }
            return Ok(());
        }
        Err(anyhow::anyhow!("{}", self))
    }

    /// Render a single error/warning as an annotate-snippets message.
    fn render_entry(&self, entry: &SpecError, level: Level) -> String {
        let source = self.sources.get(entry.source_id.0);
        let resolved = source.and_then(|src| resolve_span(&src.content, &entry.path));

        let path_str = entry.path.to_string();
        let title = format!("{} — {}", path_str, entry.message);

        match (source, resolved) {
            (Some(src), Some(span)) => {
                let message = level
                    .title(&title)
                    .snippet(
                        Snippet::source(&src.content)
                            .origin(&src.name)
                            .fold(true)
                            .annotation(level.span(span.start..span.end)),
                    );
                let renderer = Renderer::plain();
                format!("{}", renderer.render(message))
            }
            _ => {
                // No source or span — fall back to plain text
                let label = match level {
                    Level::Error => "error",
                    Level::Warning => "warning",
                    _ => "note",
                };
                format!("  {}: {}", label, title)
            }
        }
    }
}

impl fmt::Display for SpecErrors {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let n_err = self.errors.len();
        let n_warn = self.warnings.len();
        match (n_err, n_warn) {
            (e, 0) => writeln!(
                f,
                "spec validation failed ({} error{}):\n",
                e,
                if e == 1 { "" } else { "s" }
            )?,
            (e, w) => writeln!(
                f,
                "spec validation failed ({} error{}, {} warning{}):\n",
                e,
                if e == 1 { "" } else { "s" },
                w,
                if w == 1 { "" } else { "s" }
            )?,
        }
        for e in &self.errors {
            writeln!(f, "{}", self.render_entry(e, Level::Error))?;
        }
        if !self.warnings.is_empty() && !self.errors.is_empty() {
            writeln!(f)?;
        }
        for w in &self.warnings {
            writeln!(f, "{}", self.render_entry(w, Level::Warning))?;
        }
        Ok(())
    }
}
