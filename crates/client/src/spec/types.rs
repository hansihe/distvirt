use std::collections::HashMap;

use serde::Deserialize;

// ---------------------------------------------------------------------------
// Serde types matching the native YAML spec format
// ---------------------------------------------------------------------------

/// Placeholder for YAML fields that are parsed but not yet used.
/// Accepts any valid YAML value during deserialization.
#[derive(Debug, Clone, Deserialize)]
#[serde(from = "serde::de::IgnoredAny")]
pub struct UnsupportedField;

impl From<serde::de::IgnoredAny> for UnsupportedField {
    fn from(_: serde::de::IgnoredAny) -> Self {
        UnsupportedField
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct SpecFile {
    #[allow(dead_code)]
    pub api_version: String,
    #[allow(dead_code)]
    pub kind: String,
    pub metadata: Option<SpecMetadata>,
    pub network: Option<SpecNetwork>,
    pub workloads: Option<HashMap<String, SpecWorkload>>,
    pub services: Option<HashMap<String, SpecService>>,
    pub defaults: Option<SpecDefaults>,
    pub include: Option<Vec<SpecInclude>>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SpecInclude {
    pub path: String,
    #[serde(default)]
    pub values: HashMap<String, String>,
    pub overrides: Option<SpecIncludeOverrides>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SpecIncludeOverrides {
    pub env: Option<HashMap<String, String>>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SpecMetadata {
    pub name: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SpecNetwork {
    pub subnet: String,
    pub gateway: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SpecWorkload {
    pub ip: Option<String>,
    pub suspend_on_idle: Option<bool>,
    /// If true, the workload respects demand signals and starts dormant.
    /// If false (default), the workload is always-on.
    #[serde(default)]
    pub respects_demand: bool,
    pub activation: Option<SpecWorkloadActivation>,
    pub volumes: Option<Vec<SpecVolume>>,
    pub containers: Vec<SpecContainer>,
    pub resources: Option<SpecResources>,
    pub healthcheck: Option<UnsupportedField>,
    pub services: Option<HashMap<String, SpecInlineService>>,
    pub labels: Option<HashMap<String, String>>,
}

/// Workload-level activation config.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SpecWorkloadActivation {
    pub idle_timeout: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SpecContainer {
    pub name: Option<String>,
    pub image: String,
    pub command: Option<Vec<String>>,
    pub args: Option<Vec<String>>,
    pub env: Option<HashMap<String, String>>,
    pub working_dir: Option<String>,
    pub user: Option<String>,
    pub hostname: Option<String>,
    #[serde(default)]
    pub tty: bool,
    pub volume_mounts: Option<Vec<SpecVolumeMount>>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SpecVolume {
    pub name: String,
    pub empty_dir: Option<SpecEmptyDir>,
    pub config_data: Option<SpecConfigData>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SpecEmptyDir {
    pub size_mb: Option<u64>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SpecConfigData {
    pub default_mode: Option<String>,
    pub files: Vec<SpecConfigDataFile>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SpecConfigDataFile {
    pub path: String,
    pub content: String,
    pub mode: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SpecVolumeMount {
    pub name: String,
    pub mount_path: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SpecResources {
    pub requests: Option<SpecResourceValues>,
    pub limits: Option<SpecResourceValues>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SpecResourceValues {
    pub memory_mb: Option<u64>,
    pub vcpus: Option<u32>,
}

/// Inline service declared under workloads.<id>.services
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SpecInlineService {
    pub ip: Option<String>,
    pub ports: Option<Vec<SpecPort>>,
    pub idle_timeout: Option<String>,
    pub buffer: Option<SpecBuffer>,
    pub labels: Option<HashMap<String, String>>,
}

/// Top-level service
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SpecService {
    pub workload: String,
    pub ip: Option<String>,
    pub ports: Option<Vec<SpecPort>>,
    pub idle_timeout: Option<String>,
    pub buffer: Option<SpecBuffer>,
    pub labels: Option<HashMap<String, String>>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SpecPort {
    pub port: u32,
    pub target: Option<u32>,
    pub activator: Option<SpecPortActivator>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields, tag = "type", rename_all = "snake_case")]
pub enum SpecPortActivator {
    Tcp {
        max_flows: Option<u32>,
    },
    Http2,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SpecBuffer {
    pub frames: Option<u32>,
    pub timeout: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SpecDefaults {
    pub suspend_on_idle: Option<bool>,
    pub resources: Option<SpecResources>,
}

