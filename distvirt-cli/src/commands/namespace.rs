use std::path::Path;

use anyhow::{Context, bail};
use distvirt_client_protocol::*;

use crate::client::{self, Client};
use crate::format;
use crate::spec;

/// Find the spec file to use. Checks distvirt.yaml, distvirt.yml, then
/// docker-compose.yml in the current directory.
fn find_default_file() -> anyhow::Result<std::path::PathBuf> {
    for candidate in &["distvirt.yaml", "distvirt.yml", "docker-compose.yml"] {
        let p = std::path::PathBuf::from(candidate);
        if p.exists() {
            return Ok(p);
        }
    }
    bail!(
        "no spec file found (looked for distvirt.yaml, distvirt.yml, docker-compose.yml). Use -f to specify a file."
    )
}

/// Parse a spec file (native or compose) and return (optional namespace_id, NamespaceSpec).
fn parse_spec_file(file: &Path) -> anyhow::Result<(Option<String>, NamespaceSpec)> {
    // Try native spec first
    if let Some(native) = spec::try_parse(file)? {
        let (ns_id, proto_spec) = spec::spec_to_namespace_spec(&native)?;
        return Ok((ns_id, proto_spec));
    }

    // Fall back to compose
    let deployment = distvirt_compose::parse(file)
        .with_context(|| format!("parsing compose file '{}'", file.display()))?;
    let proto_spec = deployment_to_spec(&deployment)?;
    Ok((None, proto_spec))
}

/// Render a spec file to resolved proto output (no server connection needed).
pub fn render(file: &Path) -> anyhow::Result<()> {
    let (ns_id, proto_spec) = parse_spec_file(file)?;
    if let Some(ref id) = ns_id {
        println!("namespace: {}", id);
    }
    println!("{:#?}", proto_spec);
    Ok(())
}

pub async fn up(
    mut client: Client,
    namespace_id: Option<&str>,
    file: Option<&Path>,
) -> anyhow::Result<()> {
    let file = match file {
        Some(f) => f.to_path_buf(),
        None => find_default_file()?,
    };

    let (spec_ns_id, spec) = parse_spec_file(&file)?;

    // Determine namespace ID: CLI arg > spec metadata.name
    let namespace_id = match namespace_id {
        Some(id) => id.to_string(),
        None => spec_ns_id.ok_or_else(|| {
            anyhow::anyhow!(
                "namespace ID required: specify as argument or set metadata.name in spec file"
            )
        })?,
    };

    // Try create first; if it already exists, update instead
    let result = client
        .create_namespace(CreateNamespaceRequest {
            namespace_id: namespace_id.to_string(),
            spec: Some(spec.clone()),
        })
        .await;

    match result {
        Ok(_) => {
            eprintln!("namespace '{}' created", namespace_id);
        }
        Err(status) if status.code() == tonic::Code::AlreadyExists => {
            client
                .update_namespace(UpdateNamespaceRequest {
                    namespace_id: namespace_id.to_string(),
                    spec: Some(spec),
                })
                .await
                .map_err(client::handle_grpc_error)?;
            eprintln!("namespace '{}' updated", namespace_id);
        }
        Err(status) => return Err(client::handle_grpc_error(status)),
    }

    Ok(())
}

pub async fn down(mut client: Client, namespace_id: &str) -> anyhow::Result<()> {
    client
        .delete_namespace(DeleteNamespaceRequest {
            namespace_id: namespace_id.to_string(),
        })
        .await
        .map_err(client::handle_grpc_error)?;

    eprintln!("namespace '{}' deleted", namespace_id);
    Ok(())
}

pub async fn status(mut client: Client, target: &str) -> anyhow::Result<()> {
    let (namespace_id, workload_id) = parse_target(target);

    let resp = client
        .get_namespace_status(GetNamespaceStatusRequest {
            namespace_id: namespace_id.to_string(),
        })
        .await
        .map_err(client::handle_grpc_error)?;

    let report = resp
        .into_inner()
        .status
        .ok_or_else(|| anyhow::anyhow!("server returned empty status"))?;

    if let Some(wid) = workload_id {
        // Show specific workload detail
        let workload = report
            .workloads
            .get(wid)
            .ok_or_else(|| anyhow::anyhow!("workload '{}' not found in namespace", wid))?;

        let state = workload
            .state
            .as_ref()
            .map(|s| format!("{:?}", s.state))
            .unwrap_or_else(|| "unknown".to_string());
        let spliced = if workload.spliced { " [spliced]" } else { "" };

        println!("Workload: {}/{}", namespace_id, wid);
        println!("State:    {}{}", state, spliced);
        println!();

        // Show services for this workload
        let services: Vec<_> = report
            .services
            .iter()
            .filter(|(_, s)| s.workload_id == *wid)
            .collect();

        if !services.is_empty() {
            println!("Services:");
            for (svc_id, _svc) in &services {
                println!("  service/{}", svc_id);
            }
        }
    } else {
        format::print_namespace_overview(&report);
    }

    Ok(())
}

pub async fn deactivate(mut client: Client, target: &str) -> anyhow::Result<()> {
    let (namespace_id, workload_id) = parse_target(target);
    let workload_id = workload_id
        .ok_or_else(|| anyhow::anyhow!("target must be namespace/workload (e.g. myapp/api)"))?;

    let resp = client
        .deactivate_workload(DeactivateWorkloadRequest {
            namespace_id: namespace_id.to_string(),
            workload_id: workload_id.to_string(),
        })
        .await
        .map_err(client::handle_grpc_error)?;

    let resp = resp.into_inner();
    if resp.deactivated {
        eprintln!(
            "Workload {} deactivated — pod stopping, services returning to idle.",
            workload_id
        );
    } else {
        eprintln!(
            "Workload {} has active demand — not deactivating.",
            workload_id
        );
        if !resp.reason.is_empty() {
            eprintln!("  Reason: {}", resp.reason);
        }
    }

    Ok(())
}

pub async fn clone_namespace(mut client: Client, source: &str, target: &str) -> anyhow::Result<()> {
    client
        .clone_namespace(CloneNamespaceRequest {
            source_namespace_id: source.to_string(),
            target_namespace_id: target.to_string(),
            overrides: None,
        })
        .await
        .map_err(client::handle_grpc_error)?;

    eprintln!("cloned '{}' -> '{}'", source, target);
    Ok(())
}

/// Parse "ns" or "ns/workload" target string
fn parse_target(target: &str) -> (&str, Option<&str>) {
    match target.split_once('/') {
        Some((ns, workload)) => (ns, Some(workload)),
        None => (target, None),
    }
}

/// Convert a compose Deployment into a proto NamespaceSpec
fn deployment_to_spec(deployment: &distvirt_compose::Deployment) -> anyhow::Result<NamespaceSpec> {
    let mut workloads = std::collections::HashMap::new();
    let mut services = std::collections::HashMap::new();

    // Each compose service becomes both a workload and a service in the proto spec.
    // IP/MAC assignment uses a simple counter starting at .2 in the 172.16.0.0/24 subnet.
    let mut ip_counter: u8 = 2;

    for (name, svc) in &deployment.services {
        let ip = format!("172.16.0.{}", ip_counter);
        let mac = format!("02:00:AC:10:00:{:02X}", ip_counter);
        ip_counter = ip_counter
            .checked_add(1)
            .ok_or_else(|| anyhow::anyhow!("too many services (>254)"))?;

        let svc_ip = format!("172.16.0.{}", ip_counter);
        let svc_mac = format!("02:00:AC:10:00:{:02X}", ip_counter);
        ip_counter = ip_counter
            .checked_add(1)
            .ok_or_else(|| anyhow::anyhow!("too many services (>254)"))?;

        let mut entrypoint = Vec::new();
        if let Some(ep) = &svc.entrypoint {
            entrypoint = ep.clone();
        }

        let args = svc.command.clone().unwrap_or_default();

        let env: std::collections::HashMap<String, String> = svc.environment.clone();

        let container = ContainerSpec {
            name: name.clone(),
            image: svc.image.clone(),
            config: Some(ContainerConfig {
                entrypoint,
                args,
                env,
                working_dir: svc.working_dir.clone().unwrap_or_default(),
                user: svc.user.clone().unwrap_or_default(),
                hostname: svc.hostname.clone().unwrap_or_default(),
            }),
        };

        let workload = WorkloadSpec {
            network: Some(PodNetworkConfig { ip, mac }),
            containers: vec![container],
            suspend_on_idle: true,
            resources: None,
            activation: None,
        };

        let expose: Vec<ExposeSpec> = svc
            .ports
            .iter()
            .map(|p| ExposeSpec {
                container_port: p.container_port as u32,
                host_port: p.host_port as u32,
                protocol: match p.protocol {
                    distvirt_compose::PortProtocol::Tcp => ExposeProtocol::Tcp.into(),
                    distvirt_compose::PortProtocol::Udp => ExposeProtocol::Udp.into(),
                },
            })
            .collect();

        // Determine activation config from exposed ports
        let tcp_ports: Vec<u32> = svc.ports.iter().map(|p| p.container_port as u32).collect();
        let activation = if !tcp_ports.is_empty() {
            Some(ActivationSpec {
                activator: Some(ActivatorConfig {
                    activator: Some(activator_config::Activator::Tcp(TcpActivator {
                        ports: tcp_ports,
                        idle_timeout_ms: 30_000, // 30s default
                    })),
                }),
                buffer_policy: None,
            })
        } else {
            None
        };

        let service = ServiceSpec {
            workload_id: name.clone(),
            network: Some(ServiceNetworkConfig {
                ip: svc_ip,
                mac: svc_mac,
            }),
            activation,
            expose,
        };

        workloads.insert(name.clone(), workload);
        services.insert(name.clone(), service);
    }

    Ok(NamespaceSpec {
        network: Some(NetworkConfig {
            subnet: "172.16.0.0/24".to_string(),
        }),
        workloads,
        services,
    })
}
