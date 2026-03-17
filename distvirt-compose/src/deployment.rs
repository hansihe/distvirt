use std::collections::{HashMap, HashSet, VecDeque};
use std::net::Ipv4Addr;

use anyhow::bail;

use crate::types::Deployment;

// Default network constants for single-worker namespaces.
pub const DEFAULT_SUBNET: Ipv4Addr = Ipv4Addr::new(172, 16, 0, 0);
pub const DEFAULT_GATEWAY: Ipv4Addr = Ipv4Addr::new(172, 16, 0, 1);
pub const DEFAULT_PREFIX_LEN: u8 = 24;
pub const DEFAULT_NETMASK: &str = "255.255.255.0";

/// An execution plan with IP/MAC assignments for each service.
pub struct ExecutionPlan {
    pub services: Vec<PlannedService>,
}

/// A service with assigned IP and MAC addresses.
///
/// Each service gets two IPs: a service IP (virtual, used for DNS/traffic)
/// and a pod IP (assigned to the actual VM/pod). This separation enables
/// readiness gating via fabric-level service entities.
pub struct PlannedService {
    pub name: String,
    /// Virtual service IP (used for DNS resolution and service entity).
    pub service_ip: Ipv4Addr,
    /// Pod IP (assigned to the actual VM network interface).
    pub pod_ip: Ipv4Addr,
}

/// Produce an execution plan from a deployment.
///
/// Assigns IPs from 172.16.0.2 upward (gateway is .1). Each service gets
/// two IPs: service IPs first (.2 to .N+1), then pod IPs (.N+2 to .2N+1).
/// Services are ordered by dependency (best-effort topological sort).
pub fn plan(deployment: &Deployment) -> anyhow::Result<ExecutionPlan> {
    if deployment.services.len() > 126 {
        bail!("too many services ({}, max 126)", deployment.services.len());
    }

    let ordered = topo_sort(deployment);
    let n = ordered.len();

    let services = ordered
        .into_iter()
        .enumerate()
        .map(|(i, name)| {
            // Service IPs: .2, .3, .4, ... .N+1
            let svc_octet = (i + 2) as u8;
            let service_ip = Ipv4Addr::new(172, 16, 0, svc_octet);

            // Pod IPs: .N+2, .N+3, ... .2N+1
            let pod_octet = (n + i + 2) as u8;
            let pod_ip = Ipv4Addr::new(172, 16, 0, pod_octet);

            PlannedService {
                name,
                service_ip,
                pod_ip,
            }
        })
        .collect();

    Ok(ExecutionPlan { services })
}

/// Best-effort topological sort using Kahn's algorithm.
/// On cycles, emits a warning and appends remaining services.
fn topo_sort(deployment: &Deployment) -> Vec<String> {
    let service_names: HashSet<&str> = deployment.services.keys().map(|s| s.as_str()).collect();

    // Build in-degree map and adjacency list.
    let mut in_degree: HashMap<&str, usize> = HashMap::new();
    let mut dependents: HashMap<&str, Vec<&str>> = HashMap::new();

    for (name, spec) in &deployment.services {
        in_degree.entry(name.as_str()).or_insert(0);
        for dep in &spec.depends_on {
            if service_names.contains(dep.service.as_str()) {
                *in_degree.entry(name.as_str()).or_insert(0) += 1;
                dependents
                    .entry(dep.service.as_str())
                    .or_default()
                    .push(name.as_str());
            }
        }
    }

    let mut queue: VecDeque<&str> = in_degree
        .iter()
        .filter(|(_, deg)| **deg == 0)
        .map(|(&name, _)| name)
        .collect();

    // Sort the initial queue for deterministic output.
    let mut sorted_queue: Vec<&str> = queue.drain(..).collect();
    sorted_queue.sort();
    queue.extend(sorted_queue);

    let mut result = Vec::with_capacity(deployment.services.len());

    while let Some(name) = queue.pop_front() {
        result.push(name.to_string());
        if let Some(deps) = dependents.get(name) {
            let mut next: Vec<&str> = Vec::new();
            for &dep in deps {
                let deg = in_degree
                    .get_mut(dep)
                    .expect("invariant: dependent must exist in in_degree map");
                *deg -= 1;
                if *deg == 0 {
                    next.push(dep);
                }
            }
            next.sort();
            queue.extend(next);
        }
    }

    // If there's a cycle, append remaining services with a warning.
    if result.len() < deployment.services.len() {
        log::warn!("dependency cycle detected, appending remaining services in arbitrary order");
        let in_result: HashSet<&str> = result.iter().map(|s| s.as_str()).collect();
        let mut remaining: Vec<&str> = service_names
            .iter()
            .filter(|&&n| !in_result.contains(n))
            .copied()
            .collect();
        remaining.sort();
        result.extend(remaining.into_iter().map(String::from));
    }

    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{Dependency, ServiceSpec};

    fn make_service(deps: Vec<&str>) -> ServiceSpec {
        ServiceSpec {
            image: "test:latest".into(),
            command: None,
            entrypoint: None,
            environment: HashMap::new(),
            ports: vec![],
            depends_on: deps
                .into_iter()
                .map(|s| Dependency {
                    service: s.to_string(),
                })
                .collect(),
            hostname: None,
            user: None,
            working_dir: None,
        }
    }

    fn make_deployment(services: Vec<(&str, Vec<&str>)>) -> Deployment {
        Deployment {
            name: "test".into(),
            services: services
                .into_iter()
                .map(|(name, deps)| (name.to_string(), make_service(deps)))
                .collect(),
        }
    }

    #[test]
    fn plan_single_service() {
        let d = make_deployment(vec![("web", vec![])]);
        let p = plan(&d).unwrap();
        assert_eq!(p.services.len(), 1);
        assert_eq!(p.services[0].name, "web");
        assert_eq!(p.services[0].service_ip, Ipv4Addr::new(172, 16, 0, 2));
        assert_eq!(p.services[0].pod_ip, Ipv4Addr::new(172, 16, 0, 3));
    }

    #[test]
    fn plan_ip_assignment_sequential() {
        let d = make_deployment(vec![
            ("charlie", vec![]),
            ("alpha", vec![]),
            ("bravo", vec![]),
        ]);
        let p = plan(&d).unwrap();
        assert_eq!(p.services.len(), 3);
        // Alphabetical order for independent services.
        // Service IPs: .2, .3, .4; Pod IPs: .5, .6, .7
        assert_eq!(p.services[0].name, "alpha");
        assert_eq!(p.services[0].service_ip, Ipv4Addr::new(172, 16, 0, 2));
        assert_eq!(p.services[0].pod_ip, Ipv4Addr::new(172, 16, 0, 5));
        assert_eq!(p.services[1].name, "bravo");
        assert_eq!(p.services[1].service_ip, Ipv4Addr::new(172, 16, 0, 3));
        assert_eq!(p.services[1].pod_ip, Ipv4Addr::new(172, 16, 0, 6));
        assert_eq!(p.services[2].name, "charlie");
        assert_eq!(p.services[2].service_ip, Ipv4Addr::new(172, 16, 0, 4));
        assert_eq!(p.services[2].pod_ip, Ipv4Addr::new(172, 16, 0, 7));
    }

    #[test]
    fn plan_dependency_ordering() {
        let d = make_deployment(vec![("web", vec!["db"]), ("db", vec![])]);
        let p = plan(&d).unwrap();
        let names: Vec<&str> = p.services.iter().map(|s| s.name.as_str()).collect();
        let db_pos = names.iter().position(|&n| n == "db").unwrap();
        let web_pos = names.iter().position(|&n| n == "web").unwrap();
        assert!(db_pos < web_pos, "db should come before web");
    }

    #[test]
    fn plan_diamond_deps() {
        // A depends on B and C; B depends on D; C depends on D
        let d = make_deployment(vec![
            ("a", vec!["b", "c"]),
            ("b", vec!["d"]),
            ("c", vec!["d"]),
            ("d", vec![]),
        ]);
        let p = plan(&d).unwrap();
        let names: Vec<&str> = p.services.iter().map(|s| s.name.as_str()).collect();
        assert_eq!(names[0], "d");
        // b and c should be sorted alphabetically between d and a
        assert!(names.contains(&"b"));
        assert!(names.contains(&"c"));
        assert_eq!(*names.last().unwrap(), "a");
    }

    #[test]
    fn plan_cycle_warning() {
        let d = make_deployment(vec![("a", vec!["b"]), ("b", vec!["a"])]);
        let p = plan(&d).unwrap();
        assert_eq!(p.services.len(), 2);
    }

    #[test]
    fn plan_too_many_services() {
        let services: Vec<(&str, Vec<&str>)> = Vec::new();
        let mut d = make_deployment(services);
        for i in 0..127 {
            d.services
                .insert(format!("svc{i:03}"), make_service(vec![]));
        }
        assert!(plan(&d).is_err());
    }

    #[test]
    fn plan_ignores_unknown_deps() {
        let d = make_deployment(vec![("web", vec!["nonexistent"])]);
        let p = plan(&d).unwrap();
        assert_eq!(p.services.len(), 1);
        assert_eq!(p.services[0].name, "web");
    }
}
