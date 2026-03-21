use std::collections::BTreeMap;
use std::path::PathBuf;

use serde::{Deserialize, Serialize};

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

pub fn load() -> anyhow::Result<CredentialsFile> {
    let path = credentials_path();
    if !path.exists() {
        return Ok(CredentialsFile::default());
    }
    let contents = std::fs::read_to_string(&path)?;
    let creds: CredentialsFile = toml::from_str(&contents)?;
    Ok(creds)
}

pub fn save(creds: &CredentialsFile) -> anyhow::Result<()> {
    let path = credentials_path();
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let contents = toml::to_string_pretty(creds)?;
    std::fs::write(&path, contents)?;
    Ok(())
}
