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
    pub activation: Option<SpecWorkloadActivation>,
    pub containers: Vec<SpecContainer>,
    pub resources: Option<SpecResources>,
    pub healthcheck: Option<UnsupportedField>,
    pub services: Option<HashMap<String, SpecInlineService>>,
}

/// Workload-level activation. Only `passthrough` is valid here.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SpecWorkloadActivation {
    pub passthrough: Option<SpecPassthroughActivator>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SpecContainer {
    pub name: Option<String>,
    pub image: String,
    pub entrypoint: Option<Vec<String>>,
    pub args: Option<Vec<String>>,
    pub env: Option<HashMap<String, String>>,
    pub working_dir: Option<String>,
    pub user: Option<String>,
    pub hostname: Option<String>,
    #[serde(default)]
    pub tty: bool,
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
    pub activation: Option<SpecActivation>,
    pub expose: Option<Vec<SpecExpose>>,
}

/// Top-level service
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SpecService {
    pub workload: String,
    pub ip: Option<String>,
    pub activation: Option<SpecActivation>,
    pub expose: Option<Vec<SpecExpose>>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SpecActivation {
    pub passthrough: Option<SpecPassthroughActivator>,
    pub tcp: Option<SpecTcpActivator>,
    pub http2: Option<UnsupportedField>,
    pub postgres: Option<UnsupportedField>,
    pub buffer: Option<SpecBuffer>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SpecPassthroughActivator {
    pub idle_timeout: String,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SpecTcpActivator {
    pub ports: Option<Vec<u32>>,
    pub idle_timeout: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SpecBuffer {
    pub frames: Option<u32>,
    pub timeout: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SpecExpose {
    pub container_port: u32,
    pub host_port: u32,
    pub protocol: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SpecDefaults {
    pub suspend_on_idle: Option<bool>,
    pub resources: Option<SpecResources>,
    pub activation: Option<SpecActivation>,
}

