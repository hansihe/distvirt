use std::collections::HashMap;

use serde::Deserialize;

// ---------------------------------------------------------------------------
// Serde types matching the native YAML spec format
// ---------------------------------------------------------------------------

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
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
pub struct SpecInclude {
    pub path: String,
    #[serde(default)]
    pub values: HashMap<String, String>,
    pub overrides: Option<SpecIncludeOverrides>,
}

#[derive(Debug, Deserialize)]
pub struct SpecIncludeOverrides {
    pub env: Option<HashMap<String, String>>,
}

#[derive(Debug, Deserialize)]
pub struct SpecMetadata {
    pub name: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct SpecNetwork {
    pub subnet: String,
    pub gateway: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct SpecWorkload {
    pub ip: Option<String>,
    pub suspend_on_idle: Option<bool>,
    pub activation: Option<SpecWorkloadActivation>,
    pub containers: Vec<SpecContainer>,
    pub resources: Option<SpecResources>,
    pub healthcheck: Option<serde_yaml::Value>,
    pub services: Option<HashMap<String, SpecInlineService>>,
}

/// Workload-level activation. Only `passthrough` is valid here.
#[derive(Debug, Deserialize)]
pub struct SpecWorkloadActivation {
    pub passthrough: Option<SpecPassthroughActivator>,
}

#[derive(Debug, Deserialize)]
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
pub struct SpecResources {
    pub requests: Option<SpecResourceValues>,
    pub limits: Option<SpecResourceValues>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct SpecResourceValues {
    pub memory_mb: Option<u64>,
    pub vcpus: Option<u32>,
}

/// Inline service declared under workloads.<id>.services
#[derive(Debug, Deserialize)]
pub struct SpecInlineService {
    pub ip: Option<String>,
    pub activation: Option<SpecActivation>,
    pub expose: Option<Vec<SpecExpose>>,
}

/// Top-level service
#[derive(Debug, Deserialize)]
pub struct SpecService {
    pub workload: String,
    pub ip: Option<String>,
    pub activation: Option<SpecActivation>,
    pub expose: Option<Vec<SpecExpose>>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct SpecActivation {
    pub passthrough: Option<SpecPassthroughActivator>,
    pub tcp: Option<SpecTcpActivator>,
    pub http2: Option<serde_yaml::Value>,
    pub postgres: Option<serde_yaml::Value>,
    pub buffer: Option<SpecBuffer>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct SpecPassthroughActivator {
    pub idle_timeout: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct SpecTcpActivator {
    pub ports: Option<Vec<u32>>,
    pub idle_timeout: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct SpecBuffer {
    pub frames: Option<u32>,
    pub timeout: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct SpecExpose {
    pub container_port: u32,
    pub host_port: u32,
    pub protocol: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct SpecDefaults {
    pub suspend_on_idle: Option<bool>,
    pub resources: Option<SpecResources>,
    pub activation: Option<SpecActivation>,
}

