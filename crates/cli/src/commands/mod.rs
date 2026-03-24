pub mod attach;
pub mod auth;
pub mod connect;
mod escalate;
pub mod internal;
pub mod namespace;
pub mod resource;
pub mod splice;
pub mod streaming;

#[derive(Clone, Debug, clap::ValueEnum)]
pub enum OutputFormat {
    Text,
    Json,
}
