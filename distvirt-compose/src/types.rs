use std::collections::HashMap;

/// A set of services to run. Source-agnostic — could come from compose,
/// API calls, or a distributed orchestrator.
pub struct Deployment {
    pub name: String,
    pub services: HashMap<String, ServiceSpec>,
}

/// Specification for a single service.
pub struct ServiceSpec {
    pub image: String,
    pub command: Option<Vec<String>>,
    pub entrypoint: Option<Vec<String>>,
    pub environment: HashMap<String, String>,
    pub ports: Vec<PortMapping>,
    pub depends_on: Vec<Dependency>,
    pub hostname: Option<String>,
    pub user: Option<String>,
    pub working_dir: Option<String>,
}

/// A port mapping from host to container.
pub struct PortMapping {
    pub host_port: u16,
    pub container_port: u16,
    pub protocol: PortProtocol,
}

/// Protocol for a port mapping.
pub enum PortProtocol {
    Tcp,
    Udp,
}

/// A dependency on another service.
pub struct Dependency {
    pub service: String,
}
