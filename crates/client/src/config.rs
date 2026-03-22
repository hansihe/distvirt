use std::collections::BTreeMap;
use std::path::PathBuf;

use serde::{Deserialize, Serialize};
use snafu::ResultExt;

use crate::errors::*;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CredentialsFile {
    #[serde(default = "default_context_name")]
    pub current_context: String,
    #[serde(default)]
    pub contexts: BTreeMap<String, Context>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Context {
    pub server: String,
    pub token: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tls: Option<bool>,
}

fn default_context_name() -> String {
    "default".to_string()
}

impl Default for CredentialsFile {
    fn default() -> Self {
        Self {
            current_context: default_context_name(),
            contexts: BTreeMap::new(),
        }
    }
}

pub fn credentials_path() -> PathBuf {
    dirs::config_dir()
        .unwrap_or_else(|| PathBuf::from("~/.config"))
        .join("distvirt")
        .join("credentials.toml")
}

pub fn load() -> Result<CredentialsFile, ConfigError> {
    let path = credentials_path();
    if !path.exists() {
        return Ok(CredentialsFile::default());
    }
    let contents = std::fs::read_to_string(&path)
        .context(ReadFileSnafu { path: path.display().to_string() })?;
    let creds: CredentialsFile = toml::from_str(&contents)
        .context(ParseCredentialsSnafu)?;
    Ok(creds)
}

pub fn save(creds: &CredentialsFile) -> Result<(), ConfigError> {
    let path = credentials_path();
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)
            .context(CreateDirSnafu { path: parent.display().to_string() })?;
    }
    let contents = toml::to_string_pretty(creds)
        .context(SerializeCredentialsSnafu)?;
    std::fs::write(&path, contents)
        .context(WriteFileSnafu { path: path.display().to_string() })?;
    Ok(())
}
