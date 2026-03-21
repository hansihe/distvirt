use std::fmt;

use annotate_snippets::{Level, Renderer, Snippet};
use snafu::prelude::*;

use crate::spec::path::YamlPath;
use crate::spec::snippet::resolve_span;

// ---------------------------------------------------------------------------
// ConfigError
// ---------------------------------------------------------------------------

#[derive(Debug, Snafu)]
#[snafu(visibility(pub(crate)))]
pub enum ConfigError {
    #[snafu(display("failed to read {path}"))]
    ReadFile { path: String, source: std::io::Error },

    #[snafu(display("failed to write {path}"))]
    WriteFile { path: String, source: std::io::Error },

    #[snafu(display("failed to create directory {path}"))]
    CreateDir { path: String, source: std::io::Error },

    #[snafu(display("failed to parse credentials"))]
    ParseCredentials { source: toml::de::Error },

    #[snafu(display("failed to serialize credentials"))]
    SerializeCredentials { source: toml::ser::Error },
}

// ---------------------------------------------------------------------------
// SpecError
// ---------------------------------------------------------------------------

#[derive(Debug, Snafu)]
#[snafu(visibility(pub(crate)))]
pub enum SpecError {
    #[snafu(display("failed to read {path}"))]
    ReadSpec { path: String, source: std::io::Error },

    #[snafu(display("{message}"))]
    YamlParse { message: String },

    #[snafu(display("{message}"))]
    Validation { message: String },

    #[snafu(display("{message}"))]
    Resolution { message: String },
}

// ---------------------------------------------------------------------------
// ConnectionError
// ---------------------------------------------------------------------------

#[derive(Debug, Snafu)]
#[snafu(visibility(pub(crate)))]
pub enum ConnectionError {
    #[snafu(transparent)]
    Config { source: ConfigError },

    #[snafu(display("connection failed: {source}"))]
    Transport { source: tonic::transport::Error },

    #[snafu(display("{message}"))]
    InvalidEndpoint { message: String },
}

// ---------------------------------------------------------------------------
// ApiError
// ---------------------------------------------------------------------------

#[derive(Debug, Snafu)]
#[snafu(visibility(pub(crate)))]
pub enum ApiError {
    #[snafu(display("{message}"))]
    Status { message: String },

    #[snafu(display("server returned empty response"))]
    EmptyResponse,

    #[snafu(display("event stream ended unexpectedly"))]
    StreamEnded,
}

// ---------------------------------------------------------------------------
// SpecErrors — multi-error collector with source-aware rendering
// ---------------------------------------------------------------------------

/// Identifies which source file an error's path should be resolved against.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct SourceId(usize);

/// A single validation error with a structured path indicating where in the spec it occurred.
#[derive(Debug)]
struct SpecErrorEntry {
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
pub(crate) struct SpecErrors {
    sources: Vec<Source>,
    errors: Vec<SpecErrorEntry>,
    warnings: Vec<SpecErrorEntry>,
    default_source: SourceId,
}

impl SpecErrors {
    pub(crate) fn new() -> Self {
        Self {
            sources: Vec::new(),
            errors: Vec::new(),
            warnings: Vec::new(),
            default_source: SourceId(0),
        }
    }

    /// Register a source file for span resolution. Returns a `SourceId` that
    /// can be passed to `error_in` / `warn_in`.
    pub(crate) fn add_source(&mut self, name: impl Into<String>, content: impl Into<String>) -> SourceId {
        let id = SourceId(self.sources.len());
        self.sources.push(Source {
            name: name.into(),
            content: content.into(),
        });
        id
    }

    pub(crate) fn error(&mut self, path: YamlPath, msg: impl Into<String>) {
        let source_id = self.default_source;
        self.error_in(source_id, path, msg);
    }

    pub(crate) fn warn(&mut self, path: YamlPath, msg: impl Into<String>) {
        let source_id = self.default_source;
        self.warn_in(source_id, path, msg);
    }

    pub(crate) fn error_in(&mut self, source_id: SourceId, path: YamlPath, msg: impl Into<String>) {
        self.errors.push(SpecErrorEntry {
            path,
            message: msg.into(),
            source_id,
        });
    }

    pub(crate) fn warn_in(&mut self, source_id: SourceId, path: YamlPath, msg: impl Into<String>) {
        self.warnings.push(SpecErrorEntry {
            path,
            message: msg.into(),
            source_id,
        });
    }

    pub(crate) fn has_errors(&self) -> bool {
        !self.errors.is_empty()
    }

    /// Format all errors/warnings into a SpecError, or Ok(()) if none.
    pub(crate) fn into_result(self) -> Result<(), SpecError> {
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
        Err(SpecError::Validation {
            message: format!("{}", self),
        })
    }

    /// Render a single error/warning as an annotate-snippets message.
    fn render_entry(&self, entry: &SpecErrorEntry, level: Level) -> String {
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
