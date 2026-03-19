use std::fmt;

// ---------------------------------------------------------------------------
// SpecErrors — multi-error collector
// ---------------------------------------------------------------------------

/// A single validation error with a path indicating where in the spec it occurred.
#[derive(Debug)]
pub(super) struct SpecError {
    path: String,
    message: String,
}

/// Collects validation errors and warnings. If any errors are present after
/// validation, they are all reported together.
pub(super) struct SpecErrors {
    errors: Vec<SpecError>,
    warnings: Vec<SpecError>,
}

impl SpecErrors {
    pub(super) fn new() -> Self {
        Self {
            errors: Vec::new(),
            warnings: Vec::new(),
        }
    }

    pub(super) fn error(&mut self, path: &str, msg: impl Into<String>) {
        self.errors.push(SpecError {
            path: path.to_string(),
            message: msg.into(),
        });
    }

    pub(super) fn warn(&mut self, path: &str, msg: impl Into<String>) {
        self.warnings.push(SpecError {
            path: path.to_string(),
            message: msg.into(),
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
            writeln!(f, "  error: {} — {}", e.path, e.message)?;
        }
        if !self.warnings.is_empty() && !self.errors.is_empty() {
            writeln!(f)?;
        }
        for w in &self.warnings {
            writeln!(f, "  warning: {} — {}", w.path, w.message)?;
        }
        Ok(())
    }
}
