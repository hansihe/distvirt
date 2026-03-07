use std::collections::{HashMap, HashSet};
use std::net::Ipv4Addr;
use std::path::Path;

use anyhow::{bail, Context};
use serde::Deserialize;

use distvirt_client_protocol::*;

// ---------------------------------------------------------------------------
// IpAllocator — stable, hash-based IP assignment
// ---------------------------------------------------------------------------

/// Allocates IPs from a subnet using deterministic name-based hashing.
///
/// Explicit IPs are reserved first, then auto-assigned names get a slot via
/// `hash(name) % available_slots` with linear probing on collision.
pub struct IpAllocator {
    base: u32,
    num_hosts: u32,
    occupied: HashSet<u32>,
}

impl IpAllocator {
    /// Create an allocator for the given CIDR subnet.
    /// Reserves .0 (network) and .1 (gateway) automatically.
    pub fn new(cidr: &str) -> anyhow::Result<Self> {
        let (base_ip, prefix) = parse_cidr(cidr)?;
        let base = ip_to_u32(base_ip);
        let host_bits = 32 - prefix as u32;
        let total_addrs = 1u32
            .checked_shl(host_bits)
            .ok_or_else(|| anyhow::anyhow!("invalid prefix length: {}", prefix))?;
        let num_hosts = total_addrs.saturating_sub(2);
        if num_hosts == 0 {
            bail!("subnet {} has no usable host addresses", cidr);
        }
        Ok(Self {
            base,
            num_hosts,
            occupied: HashSet::new(),
        })
    }

    /// Reserve an explicit IP address.
    pub fn reserve(&mut self, ip: Ipv4Addr) -> anyhow::Result<()> {
        let ip_u32 = ip_to_u32(ip);
        let first_host = self.base + 2;
        if ip_u32 < first_host || ip_u32 >= first_host + self.num_hosts {
            bail!("IP {} is outside the allocatable range of the subnet", ip);
        }
        let offset = ip_u32 - first_host;
        if !self.occupied.insert(offset) {
            bail!("IP {} is already allocated", ip);
        }
        Ok(())
    }

    /// Auto-assign an IP for the given name using deterministic hashing.
    pub fn assign(&mut self, name: &str) -> anyhow::Result<Ipv4Addr> {
        if self.occupied.len() as u32 >= self.num_hosts {
            bail!("no more IPs available in subnet");
        }
        let hash = fnv1a_hash(name);
        let start = hash % self.num_hosts;
        for i in 0..self.num_hosts {
            let offset = (start + i) % self.num_hosts;
            if !self.occupied.contains(&offset) {
                self.occupied.insert(offset);
                let ip = u32_to_ip(self.base + 2 + offset);
                return Ok(ip);
            }
        }
        bail!("no more IPs available in subnet")
    }
}

/// FNV-1a hash — deterministic, not seeded like HashMap's hasher.
fn fnv1a_hash(s: &str) -> u32 {
    let mut hash: u32 = 0x811c_9dc5;
    for byte in s.as_bytes() {
        hash ^= *byte as u32;
        hash = hash.wrapping_mul(0x0100_0193);
    }
    hash
}

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
    pub containers: Vec<SpecContainer>,
    pub resources: Option<SpecResources>,
    pub healthcheck: Option<serde_yaml::Value>,
    pub services: Option<HashMap<String, SpecInlineService>>,
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
    pub activator: Option<SpecActivator>,
    pub idle_timeout: Option<String>,
    pub buffer: Option<SpecBuffer>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct SpecActivator {
    pub tcp: Option<SpecTcpActivator>,
    pub http2: Option<serde_yaml::Value>,
    pub postgres: Option<serde_yaml::Value>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct SpecTcpActivator {
    pub ports: Option<Vec<u32>>,
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

// ---------------------------------------------------------------------------
// Parsing
// ---------------------------------------------------------------------------

/// Try to parse a file as a native distvirt spec.
/// Returns None if the file doesn't look like a native spec (no `kind` field
/// or kind is not Namespace/WorkloadFragment).
pub fn try_parse(path: &Path) -> anyhow::Result<Option<SpecFile>> {
    let contents = std::fs::read_to_string(path)
        .with_context(|| format!("reading spec file '{}'", path.display()))?;

    // Quick check: does it look like a native spec?
    let probe: serde_yaml::Value = serde_yaml::from_str(&contents)
        .with_context(|| format!("parsing YAML from '{}'", path.display()))?;

    let kind = probe.get("kind").and_then(|v| v.as_str());
    match kind {
        Some("Namespace") | Some("WorkloadFragment") => {}
        _ => return Ok(None),
    }

    let spec: SpecFile = serde_yaml::from_str(&contents)
        .with_context(|| format!("parsing native spec from '{}'", path.display()))?;

    Ok(Some(spec))
}

// ---------------------------------------------------------------------------
// Conversion to proto NamespaceSpec
// ---------------------------------------------------------------------------

/// Convert a parsed native spec into (namespace_id, NamespaceSpec).
/// The namespace_id comes from metadata.name.
pub fn spec_to_namespace_spec(spec: &SpecFile) -> anyhow::Result<(Option<String>, NamespaceSpec)> {
    let namespace_id = spec.metadata.as_ref().and_then(|m| m.name.clone());

    let subnet_str = spec
        .network
        .as_ref()
        .map(|n| n.subnet.as_str())
        .unwrap_or("172.16.0.0/24");

    if let Some(ref net) = spec.network {
        if net.gateway.is_some() {
            log::warn!("network.gateway is not yet supported in the client protocol; ignored");
        }
    }

    let mut allocator = IpAllocator::new(subnet_str)?;

    // Pass 1: reserve all explicit IPs
    if let Some(ref spec_workloads) = spec.workloads {
        for (_, wl) in spec_workloads {
            if let Some(ref ip) = wl.ip {
                allocator.reserve(ip.parse().context("invalid workload IP")?)?;
            }
            if let Some(ref inline_services) = wl.services {
                for (_, svc) in inline_services {
                    if let Some(ref ip) = svc.ip {
                        allocator.reserve(ip.parse().context("invalid inline service IP")?)?;
                    }
                }
            }
        }
    }
    if let Some(ref top_services) = spec.services {
        for (_, svc) in top_services {
            if let Some(ref ip) = svc.ip {
                allocator.reserve(ip.parse().context("invalid service IP")?)?;
            }
        }
    }

    // Pass 2: build workloads and services with stable IP assignment
    let mut workloads = HashMap::new();
    let mut services = HashMap::new();

    if let Some(ref spec_workloads) = spec.workloads {
        // Sort workload names for deterministic iteration
        let mut wl_names: Vec<&String> = spec_workloads.keys().collect();
        wl_names.sort();

        for wid in wl_names {
            let wl = &spec_workloads[wid];
            let default_suspend = spec
                .defaults
                .as_ref()
                .and_then(|d| d.suspend_on_idle)
                .unwrap_or(true);
            let suspend_on_idle = wl.suspend_on_idle.unwrap_or(default_suspend);

            // Pod IP
            let pod_ip = match &wl.ip {
                Some(ip) => ip.clone(),
                None => allocator.assign(&format!("workload:{}", wid))?.to_string(),
            };
            let pod_mac = ip_to_mac(&pod_ip);

            // Resources (workload-level)
            let (requests, limits) = resolve_resources(&wl.resources, &spec.defaults);
            let resources = if requests.is_some() || limits.is_some() {
                Some(ResourceRequirements {
                    requests: requests.map(|r| ResourceValues {
                        memory_mb: r.memory_mb.unwrap_or(0),
                        vcpus: r.vcpus.unwrap_or(0),
                    }),
                    limits: limits.map(|l| ResourceValues {
                        memory_mb: l.memory_mb.unwrap_or(0),
                        vcpus: l.vcpus.unwrap_or(0),
                    }),
                })
            } else {
                None
            };

            // Containers
            let containers: Vec<ContainerSpec> = wl
                .containers
                .iter()
                .enumerate()
                .map(|(i, c)| {
                    let name = c
                        .name
                        .clone()
                        .unwrap_or_else(|| if i == 0 { "main".to_string() } else { format!("container-{}", i) });

                    ContainerSpec {
                        name,
                        image: c.image.clone(),
                        config: Some(ContainerConfig {
                            entrypoint: c.entrypoint.clone().unwrap_or_default(),
                            args: c.args.clone().unwrap_or_default(),
                            env: c.env.clone().unwrap_or_default(),
                            working_dir: c.working_dir.clone().unwrap_or_default(),
                            user: c.user.clone().unwrap_or_default(),
                            hostname: c.hostname.clone().unwrap_or_default(),
                        }),
                    }
                })
                .collect();

            if wl.healthcheck.is_some() {
                log::warn!(
                    "workload '{}': healthcheck is not yet supported in client protocol; ignored",
                    wid
                );
            }

            workloads.insert(
                wid.clone(),
                WorkloadSpec {
                    network: Some(PodNetworkConfig {
                        ip: pod_ip,
                        mac: pod_mac,
                    }),
                    containers,
                    suspend_on_idle,
                    resources,
                },
            );

            // Inline services — sorted for determinism
            if let Some(ref inline_services) = wl.services {
                let mut svc_names: Vec<&String> = inline_services.keys().collect();
                svc_names.sort();

                for sid in svc_names {
                    let svc = &inline_services[sid];
                    let svc_ip = match &svc.ip {
                        Some(ip) => ip.clone(),
                        None => allocator.assign(&format!("service:{}", sid))?.to_string(),
                    };
                    let svc_mac = ip_to_mac(&svc_ip);

                    let activation = resolve_activation(&svc.activation, &spec.defaults);
                    let expose = convert_expose(&svc.expose);

                    services.insert(
                        sid.clone(),
                        ServiceSpec {
                            workload_id: wid.clone(),
                            network: Some(ServiceNetworkConfig {
                                ip: svc_ip,
                                mac: svc_mac,
                            }),
                            activation,
                            expose,
                        },
                    );
                }
            }
        }
    }

    // Top-level services — sorted for determinism
    if let Some(ref top_services) = spec.services {
        let mut svc_names: Vec<&String> = top_services.keys().collect();
        svc_names.sort();

        for sid in svc_names {
            let svc = &top_services[sid];
            let svc_ip = match &svc.ip {
                Some(ip) => ip.clone(),
                None => allocator.assign(&format!("service:{}", sid))?.to_string(),
            };
            let svc_mac = ip_to_mac(&svc_ip);

            let activation = resolve_activation(&svc.activation, &spec.defaults);
            let expose = convert_expose(&svc.expose);

            services.insert(
                sid.clone(),
                ServiceSpec {
                    workload_id: svc.workload.clone(),
                    network: Some(ServiceNetworkConfig {
                        ip: svc_ip,
                        mac: svc_mac,
                    }),
                    activation,
                    expose,
                },
            );
        }
    }

    Ok((
        namespace_id,
        NamespaceSpec {
            network: Some(NetworkConfig {
                subnet: subnet_str.to_string(),
            }),
            workloads,
            services,
        },
    ))
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn resolve_resources(
    workload_res: &Option<SpecResources>,
    defaults: &Option<SpecDefaults>,
) -> (Option<SpecResourceValues>, Option<SpecResourceValues>) {
    let default_res = defaults.as_ref().and_then(|d| d.resources.as_ref());

    let limits = workload_res
        .as_ref()
        .and_then(|r| r.limits.clone())
        .or_else(|| default_res.and_then(|r| r.limits.clone()));

    let requests = workload_res
        .as_ref()
        .and_then(|r| r.requests.clone())
        .or_else(|| default_res.and_then(|r| r.requests.clone()));

    (requests, limits)
}

fn resolve_activation(
    svc_activation: &Option<SpecActivation>,
    defaults: &Option<SpecDefaults>,
) -> Option<ActivationSpec> {
    let activation = svc_activation
        .as_ref()
        .or_else(|| defaults.as_ref().and_then(|d| d.activation.as_ref()));

    let activation = match activation {
        Some(a) => a,
        None => return None,
    };

    let activator = activation.activator.as_ref().and_then(|a| {
        if a.postgres.is_some() {
            log::warn!("postgres activator is not yet supported in client protocol; ignored");
            None
        } else if a.http2.is_some() {
            Some(ActivatorConfig {
                activator: Some(activator_config::Activator::Http2(Http2Activator {})),
            })
        } else if let Some(ref tcp) = a.tcp {
            Some(ActivatorConfig {
                activator: Some(activator_config::Activator::Tcp(TcpActivator {
                    ports: tcp.ports.clone().unwrap_or_default(),
                })),
            })
        } else {
            None
        }
    });

    let idle_timeout_ms = activation
        .idle_timeout
        .as_ref()
        .map(|s| parse_duration_ms(s))
        .transpose()
        .unwrap_or_else(|e| {
            log::warn!("failed to parse idle_timeout: {}", e);
            None
        })
        .unwrap_or(30_000); // default 30s (scale-to-zero)

    if let Some(ref buf) = activation.buffer {
        if buf.frames.is_some() || buf.timeout.is_some() {
            // TODO: ServicePolicy message is empty in proto
            log::warn!("activation.buffer fields are not yet in client protocol; ignored");
        }
    }

    Some(ActivationSpec {
        activator,
        buffer_policy: None,
        idle_timeout_ms,
    })
}

fn convert_expose(expose: &Option<Vec<SpecExpose>>) -> Vec<ExposeSpec> {
    expose
        .as_ref()
        .map(|specs| {
            specs
                .iter()
                .map(|e| {
                    let protocol = match e.protocol.as_deref() {
                        Some("udp") | Some("UDP") => ExposeProtocol::Udp.into(),
                        _ => ExposeProtocol::Tcp.into(),
                    };
                    ExposeSpec {
                        container_port: e.container_port,
                        host_port: e.host_port,
                        protocol,
                    }
                })
                .collect()
        })
        .unwrap_or_default()
}

fn parse_cidr(cidr: &str) -> anyhow::Result<(Ipv4Addr, u8)> {
    let (ip_str, prefix_str) = cidr
        .split_once('/')
        .ok_or_else(|| anyhow::anyhow!("invalid CIDR: {}", cidr))?;
    let ip: Ipv4Addr = ip_str.parse().context("invalid IP in CIDR")?;
    let prefix: u8 = prefix_str.parse().context("invalid prefix in CIDR")?;
    Ok((ip, prefix))
}

fn ip_to_u32(ip: Ipv4Addr) -> u32 {
    u32::from(ip)
}

fn u32_to_ip(val: u32) -> Ipv4Addr {
    Ipv4Addr::from(val)
}

fn ip_to_mac(ip: &str) -> String {
    // Generate a deterministic MAC from IP: 02:00:xx:xx:xx:xx
    let addr: Ipv4Addr = ip.parse().unwrap_or(Ipv4Addr::new(0, 0, 0, 0));
    let octets = addr.octets();
    format!(
        "02:00:{:02X}:{:02X}:{:02X}:{:02X}",
        octets[0], octets[1], octets[2], octets[3]
    )
}

/// Parse a human-readable duration string into milliseconds.
/// Supports: "30s", "5m", "1h", "500ms"
fn parse_duration_ms(s: &str) -> anyhow::Result<u64> {
    let s = s.trim();
    if let Some(val) = s.strip_suffix("ms") {
        return Ok(val.trim().parse::<u64>()?);
    }
    if let Some(val) = s.strip_suffix('s') {
        return Ok(val.trim().parse::<u64>()? * 1_000);
    }
    if let Some(val) = s.strip_suffix('m') {
        return Ok(val.trim().parse::<u64>()? * 60_000);
    }
    if let Some(val) = s.strip_suffix('h') {
        return Ok(val.trim().parse::<u64>()? * 3_600_000);
    }
    bail!("unsupported duration format: '{}' (use ms/s/m/h suffix)", s);
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::NamedTempFile;

    fn parse_yaml(yaml: &str) -> SpecFile {
        let mut f = NamedTempFile::new().unwrap();
        f.write_all(yaml.as_bytes()).unwrap();
        try_parse(f.path()).unwrap().unwrap()
    }

    fn convert(yaml: &str) -> (Option<String>, NamespaceSpec) {
        let spec = parse_yaml(yaml);
        spec_to_namespace_spec(&spec).unwrap()
    }

    // --- (a) Full example parse + convert ---

    #[test]
    fn full_example_parse_and_convert() {
        let yaml = r#"
apiVersion: v1
kind: Namespace
metadata:
  name: my-staging-env
network:
  subnet: 172.16.0.0/24
workloads:
  api:
    containers:
      - name: main
        image: docker.io/myorg/api:latest
        entrypoint: ["/app/server"]
        args: ["--port", "8080"]
        env:
          DATABASE_URL: "postgres://db:5432/myapp"
        working_dir: /app
        user: "1000:1000"
        hostname: api
    resources:
      limits:
        memory_mb: 512
        vcpus: 2
    services:
      api:
        activation:
          activator:
            tcp: { ports: [8080] }
          idle_timeout: 5m
  database:
    containers:
      - name: main
        image: docker.io/library/postgres:16
        env:
          POSTGRES_PASSWORD: "dev"
    services:
      database:
        activation:
          activator:
            postgres: {}
          idle_timeout: 10m
  frontend:
    containers:
      - name: main
        image: docker.io/myorg/frontend:latest
    services:
      frontend: {}
"#;
        let (ns_id, proto) = convert(yaml);
        assert_eq!(ns_id.as_deref(), Some("my-staging-env"));
        assert_eq!(proto.workloads.len(), 3);
        assert_eq!(proto.services.len(), 3);

        // Check container fields on api workload
        let api = &proto.workloads["api"];
        assert_eq!(api.containers.len(), 1);
        let c = &api.containers[0];
        assert_eq!(c.name, "main");
        assert_eq!(c.image, "docker.io/myorg/api:latest");
        let cfg = c.config.as_ref().unwrap();
        assert_eq!(cfg.entrypoint, vec!["/app/server"]);
        assert_eq!(cfg.args, vec!["--port", "8080"]);
        assert_eq!(cfg.working_dir, "/app");
        assert_eq!(cfg.user, "1000:1000");
        assert_eq!(cfg.hostname, "api");
        // Resources are now on the workload level
        let res = api.resources.as_ref().unwrap();
        let limits = res.limits.as_ref().unwrap();
        assert_eq!(limits.memory_mb, 512);
        assert_eq!(limits.vcpus, 2);

        // Check activation on api service
        let api_svc = &proto.services["api"];
        assert_eq!(api_svc.workload_id, "api");
        let act = api_svc.activation.as_ref().unwrap();
        assert_eq!(act.idle_timeout_ms, 300_000);
    }

    // --- (b) IP stability on addition ---

    #[test]
    fn ip_stable_on_addition() {
        let yaml_base = r#"
apiVersion: v1
kind: Namespace
network:
  subnet: 172.16.0.0/24
workloads:
  api:
    containers:
      - image: img
  database:
    containers:
      - image: img
"#;
        let (_, proto1) = convert(yaml_base);
        let api_ip1 = &proto1.workloads["api"].network.as_ref().unwrap().ip;
        let db_ip1 = &proto1.workloads["database"].network.as_ref().unwrap().ip;

        let yaml_added = r#"
apiVersion: v1
kind: Namespace
network:
  subnet: 172.16.0.0/24
workloads:
  api:
    containers:
      - image: img
  database:
    containers:
      - image: img
  cache:
    containers:
      - image: img
"#;
        let (_, proto2) = convert(yaml_added);
        let api_ip2 = &proto2.workloads["api"].network.as_ref().unwrap().ip;
        let db_ip2 = &proto2.workloads["database"].network.as_ref().unwrap().ip;

        assert_eq!(api_ip1, api_ip2, "api IP should be stable after adding cache");
        assert_eq!(db_ip1, db_ip2, "database IP should be stable after adding cache");
    }

    // --- (c) IP stability on removal ---

    #[test]
    fn ip_stable_on_removal() {
        let yaml_full = r#"
apiVersion: v1
kind: Namespace
network:
  subnet: 172.16.0.0/24
workloads:
  api:
    containers:
      - image: img
  database:
    containers:
      - image: img
  frontend:
    containers:
      - image: img
"#;
        let (_, proto1) = convert(yaml_full);
        let api_ip1 = &proto1.workloads["api"].network.as_ref().unwrap().ip;
        let fe_ip1 = &proto1.workloads["frontend"].network.as_ref().unwrap().ip;

        let yaml_removed = r#"
apiVersion: v1
kind: Namespace
network:
  subnet: 172.16.0.0/24
workloads:
  api:
    containers:
      - image: img
  frontend:
    containers:
      - image: img
"#;
        let (_, proto2) = convert(yaml_removed);
        let api_ip2 = &proto2.workloads["api"].network.as_ref().unwrap().ip;
        let fe_ip2 = &proto2.workloads["frontend"].network.as_ref().unwrap().ip;

        assert_eq!(api_ip1, api_ip2, "api IP should be stable after removing database");
        assert_eq!(fe_ip1, fe_ip2, "frontend IP should be stable after removing database");
    }

    // --- (d) Explicit IP respected ---

    #[test]
    fn explicit_ip_respected() {
        let yaml = r#"
apiVersion: v1
kind: Namespace
network:
  subnet: 172.16.0.0/24
workloads:
  fixed:
    ip: 172.16.0.50
    containers:
      - image: img
  auto1:
    containers:
      - image: img
  auto2:
    containers:
      - image: img
"#;
        let (_, proto) = convert(yaml);
        let fixed_ip = &proto.workloads["fixed"].network.as_ref().unwrap().ip;
        let auto1_ip = &proto.workloads["auto1"].network.as_ref().unwrap().ip;
        let auto2_ip = &proto.workloads["auto2"].network.as_ref().unwrap().ip;

        assert_eq!(fixed_ip, "172.16.0.50");
        assert_ne!(auto1_ip, "172.16.0.50", "auto-assigned should not collide with explicit");
        assert_ne!(auto2_ip, "172.16.0.50", "auto-assigned should not collide with explicit");
        assert_ne!(auto1_ip, auto2_ip, "auto-assigned IPs should be distinct");
    }

    // --- (e) Defaults merging: suspend_on_idle ---

    #[test]
    fn defaults_suspend_on_idle() {
        let yaml = r#"
apiVersion: v1
kind: Namespace
network:
  subnet: 172.16.0.0/24
defaults:
  suspend_on_idle: true
workloads:
  inherits:
    containers:
      - image: img
  overrides:
    suspend_on_idle: false
    containers:
      - image: img
"#;
        let (_, proto) = convert(yaml);
        assert!(proto.workloads["inherits"].suspend_on_idle, "should inherit default true");
        assert!(!proto.workloads["overrides"].suspend_on_idle, "should override to false");
    }

    // --- (f) Defaults activation ---

    #[test]
    fn defaults_activation_inherited_and_overridden() {
        let yaml = r#"
apiVersion: v1
kind: Namespace
network:
  subnet: 172.16.0.0/24
defaults:
  activation:
    activator:
      tcp: { ports: [80] }
    idle_timeout: 5m
workloads:
  app:
    containers:
      - image: img
    services:
      inherits: {}
      overrides:
        activation:
          activator:
            tcp: { ports: [9090] }
          idle_timeout: 30s
"#;
        let (_, proto) = convert(yaml);

        // Service that inherits default activation
        let inherits_act = proto.services["inherits"].activation.as_ref().unwrap();
        assert_eq!(inherits_act.idle_timeout_ms, 300_000);

        // Service that overrides activation
        let overrides_act = proto.services["overrides"].activation.as_ref().unwrap();
        assert_eq!(overrides_act.idle_timeout_ms, 30_000);
    }

    // --- (g) Duration parsing ---

    #[test]
    fn duration_parsing() {
        assert_eq!(parse_duration_ms("5m").unwrap(), 300_000);
        assert_eq!(parse_duration_ms("30s").unwrap(), 30_000);
        assert_eq!(parse_duration_ms("500ms").unwrap(), 500);
        assert_eq!(parse_duration_ms("1h").unwrap(), 3_600_000);
        assert!(parse_duration_ms("invalid").is_err());
        assert!(parse_duration_ms("5x").is_err());
    }

    // --- (h) Compose fallback detection ---

    #[test]
    fn compose_yaml_returns_none() {
        let compose = r#"
version: "3"
services:
  web:
    image: nginx
    ports:
      - "80:80"
"#;
        let mut f = NamedTempFile::new().unwrap();
        f.write_all(compose.as_bytes()).unwrap();
        let result = try_parse(f.path()).unwrap();
        assert!(result.is_none(), "docker-compose YAML should not parse as native spec");
    }

    // --- IpAllocator unit tests ---

    #[test]
    fn allocator_deterministic() {
        let mut a1 = IpAllocator::new("10.0.0.0/24").unwrap();
        let mut a2 = IpAllocator::new("10.0.0.0/24").unwrap();
        let ip1 = a1.assign("foo").unwrap();
        let ip2 = a2.assign("foo").unwrap();
        assert_eq!(ip1, ip2, "same name should always get same IP");
    }

    #[test]
    fn allocator_reserve_prevents_collision() {
        let mut alloc = IpAllocator::new("10.0.0.0/24").unwrap();
        let reserved: Ipv4Addr = "10.0.0.2".parse().unwrap();
        alloc.reserve(reserved).unwrap();

        // Assign many names, none should get the reserved IP
        for i in 0..50 {
            let ip = alloc.assign(&format!("name-{}", i)).unwrap();
            assert_ne!(ip, reserved, "should not assign reserved IP");
        }
    }

    #[test]
    fn allocator_reserve_duplicate_errors() {
        let mut alloc = IpAllocator::new("10.0.0.0/24").unwrap();
        alloc.reserve("10.0.0.5".parse().unwrap()).unwrap();
        assert!(alloc.reserve("10.0.0.5".parse().unwrap()).is_err());
    }

    #[test]
    fn allocator_exhaustion() {
        // /30 = 4 addresses, minus .0 and .1 = 2 usable
        let mut alloc = IpAllocator::new("10.0.0.0/30").unwrap();
        alloc.assign("a").unwrap();
        alloc.assign("b").unwrap();
        assert!(alloc.assign("c").is_err(), "should be exhausted");
    }
}
