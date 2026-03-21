pub mod config;
pub mod connect;
pub mod connection;
pub mod format;
pub mod operations;
pub mod spec;

mod errors;
pub mod model;
pub mod watcher;

#[cfg(test)]
mod tests;

pub use spec::convert::spec_to_namespace_spec;
pub use spec::includes::resolve_includes;
pub use errors::{ApiError, ConfigError, ConnectionError, SpecError};
pub use spec::parse::{try_parse, ParsedSpec};
