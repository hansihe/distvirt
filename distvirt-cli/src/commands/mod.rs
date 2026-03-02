pub mod auth;
pub mod legacy;
pub mod namespace;
pub mod resource;
pub mod splice;
pub mod streaming;

pub use legacy::LegacyCommands;

#[derive(Clone, Debug, clap::ValueEnum)]
pub enum OutputFormat {
    Text,
    Json,
}
