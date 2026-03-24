use distvirt_client::connection::{handle_grpc_error, Client};
use distvirt_client_protocol::*;

use crate::commands::OutputFormat;
use crate::entity_ref::ResourceType;
use crate::format;

pub async fn get(
    mut client: Client,
    resource: &str,
    namespace: Option<&str>,
    output: &OutputFormat,
) -> anyhow::Result<()> {
    match normalize_resource(resource) {
        "namespaces" => {
            let resp = client
                .list_namespaces(ListNamespacesRequest {})
                .await
                .map_err(handle_grpc_error)?;
            let namespaces = &resp.into_inner().namespaces;
            match output {
                OutputFormat::Text => format::print_namespace_table(namespaces),
                OutputFormat::Json => {
                    println!(
                        "{}",
                        serde_json::to_string_pretty(&format::namespaces_to_json(namespaces))?
                    );
                }
            }
        }
        "workers" => {
            let resp = client
                .list_workers(ListWorkersRequest {})
                .await
                .map_err(handle_grpc_error)?;
            let workers = &resp.into_inner().workers;
            match output {
                OutputFormat::Text => format::print_worker_table(workers),
                OutputFormat::Json => {
                    println!(
                        "{}",
                        serde_json::to_string_pretty(&format::workers_to_json(workers))?
                    );
                }
            }
        }
        "pods" => {
            let ns = namespace
                .ok_or_else(|| anyhow::anyhow!("--namespace is required for listing pods"))?;
            let resp = client
                .list_pods(ListPodsRequest {
                    namespace_id: ns.to_string(),
                })
                .await
                .map_err(handle_grpc_error)?;
            let pods = &resp.into_inner().pods;
            match output {
                OutputFormat::Text => format::print_pod_table(pods),
                OutputFormat::Json => {
                    println!(
                        "{}",
                        serde_json::to_string_pretty(&format::pods_to_json(pods))?
                    );
                }
            }
        }
        "workloads" => {
            let ns = namespace
                .ok_or_else(|| anyhow::anyhow!("--namespace is required for listing workloads"))?;
            let resp = client
                .get_namespace_status(GetNamespaceStatusRequest {
                    namespace_id: ns.to_string(),
                })
                .await
                .map_err(handle_grpc_error)?;
            let report = resp
                .into_inner()
                .status
                .ok_or_else(|| anyhow::anyhow!("server returned empty status"))?;
            match output {
                OutputFormat::Text => format::print_workload_table(&report.workloads),
                OutputFormat::Json => {
                    println!(
                        "{}",
                        serde_json::to_string_pretty(&format::workloads_to_json(
                            &report.workloads
                        ))?
                    );
                }
            }
        }
        "services" => {
            let ns = namespace
                .ok_or_else(|| anyhow::anyhow!("--namespace is required for listing services"))?;
            let resp = client
                .get_namespace_status(GetNamespaceStatusRequest {
                    namespace_id: ns.to_string(),
                })
                .await
                .map_err(handle_grpc_error)?;
            let report = resp
                .into_inner()
                .status
                .ok_or_else(|| anyhow::anyhow!("server returned empty status"))?;
            match output {
                OutputFormat::Text => format::print_service_table(&report.services),
                OutputFormat::Json => {
                    println!(
                        "{}",
                        serde_json::to_string_pretty(&format::services_to_json(&report.services))?
                    );
                }
            }
        }
        other => {
            anyhow::bail!(
                "unknown resource type: '{}'. Try: namespaces, workers, pods, workloads, services",
                other
            );
        }
    }
    Ok(())
}

pub async fn describe(
    mut client: Client,
    resource: &str,
    name: &str,
    output: &OutputFormat,
) -> anyhow::Result<()> {
    match normalize_resource(resource) {
        "namespaces" => {
            let resp = client
                .get_namespace_status(GetNamespaceStatusRequest {
                    namespace_id: name.to_string(),
                })
                .await
                .map_err(handle_grpc_error)?;
            let report = resp
                .into_inner()
                .status
                .ok_or_else(|| anyhow::anyhow!("server returned empty status"))?;
            match output {
                OutputFormat::Text => format::print_namespace_overview(&report),
                OutputFormat::Json => {
                    println!(
                        "{}",
                        serde_json::to_string_pretty(&format::namespace_status_to_json(&report))?
                    );
                }
            }
        }
        "workers" => {
            let resp = client
                .get_worker(GetWorkerRequest {
                    worker_id: name.to_string(),
                })
                .await
                .map_err(handle_grpc_error)?;
            let worker = resp
                .into_inner()
                .worker
                .ok_or_else(|| anyhow::anyhow!("server returned empty worker"))?;
            match output {
                OutputFormat::Text => format::print_worker_detail(&worker),
                OutputFormat::Json => {
                    println!(
                        "{}",
                        serde_json::to_string_pretty(&format::worker_to_json(&worker))?
                    );
                }
            }
        }
        other => {
            anyhow::bail!(
                "describe not supported for '{}'. Try: namespaces, workers",
                other
            );
        }
    }
    Ok(())
}

pub async fn delete(
    mut client: Client,
    resource: &str,
    name: &str,
    namespace: Option<&str>,
) -> anyhow::Result<()> {
    match normalize_resource(resource) {
        "namespaces" => {
            client
                .delete_namespace(DeleteNamespaceRequest {
                    namespace_id: name.to_string(),
                })
                .await
                .map_err(handle_grpc_error)?;
            eprintln!("namespace '{}' deleted", name);
        }
        "workloads" => {
            let ns = namespace
                .ok_or_else(|| anyhow::anyhow!("--namespace is required for deleting workloads"))?;
            client
                .patch_namespace(PatchNamespaceRequest {
                    namespace_id: ns.to_string(),
                    remove_workloads: vec![name.to_string()],
                    ..Default::default()
                })
                .await
                .map_err(handle_grpc_error)?;
            eprintln!("workload '{}' deleted from namespace '{}'", name, ns);
        }
        "services" => {
            let ns = namespace
                .ok_or_else(|| anyhow::anyhow!("--namespace is required for deleting services"))?;
            client
                .patch_namespace(PatchNamespaceRequest {
                    namespace_id: ns.to_string(),
                    remove_services: vec![name.to_string()],
                    ..Default::default()
                })
                .await
                .map_err(handle_grpc_error)?;
            eprintln!("service '{}' deleted from namespace '{}'", name, ns);
        }
        other => {
            anyhow::bail!(
                "delete not supported for '{}'. Try: namespaces, workloads, services",
                other
            );
        }
    }
    Ok(())
}

/// Describe an individual namespaced resource.
pub async fn describe_namespaced(
    mut client: Client,
    namespace_id: &str,
    resource_type: ResourceType,
    name: &str,
    output: &OutputFormat,
) -> anyhow::Result<()> {
    match resource_type {
        ResourceType::Workload => {
            let resp = client
                .get_namespace_status(GetNamespaceStatusRequest {
                    namespace_id: namespace_id.to_string(),
                })
                .await
                .map_err(handle_grpc_error)?;
            let report = resp
                .into_inner()
                .status
                .ok_or_else(|| anyhow::anyhow!("server returned empty status"))?;
            let workload = report
                .workloads
                .get(name)
                .ok_or_else(|| anyhow::anyhow!("workload '{}' not found in namespace", name))?;
            match output {
                OutputFormat::Text => {
                    let state = workload
                        .state
                        .as_ref()
                        .map(|s| distvirt_client::format::workload_state_detail(s))
                        .unwrap_or_else(|| "unknown".to_string());
                    let spliced = if workload.spliced { " [spliced]" } else { "" };
                    println!("Workload: {}/{}", namespace_id, name);
                    println!("State:    {}{}", state, spliced);
                    if !workload.ip.is_empty() {
                        println!("IP:       {}", workload.ip);
                    }

                    let services: Vec<_> = report
                        .services
                        .iter()
                        .filter(|(_, s)| s.workload_id == name)
                        .collect();
                    if !services.is_empty() {
                        println!("\nServices:");
                        for (svc_id, _) in &services {
                            println!("  service/{}", svc_id);
                        }
                    }
                }
                OutputFormat::Json => {
                    println!(
                        "{}",
                        serde_json::to_string_pretty(&format::workloads_to_json(
                            &[(name.to_string(), workload.clone())].into_iter().collect()
                        ))?
                    );
                }
            }
        }
        ResourceType::Service => {
            let resp = client
                .get_namespace_status(GetNamespaceStatusRequest {
                    namespace_id: namespace_id.to_string(),
                })
                .await
                .map_err(handle_grpc_error)?;
            let report = resp
                .into_inner()
                .status
                .ok_or_else(|| anyhow::anyhow!("server returned empty status"))?;
            let service = report
                .services
                .get(name)
                .ok_or_else(|| anyhow::anyhow!("service '{}' not found in namespace", name))?;
            match output {
                OutputFormat::Text => {
                    format::print_service_table(
                        &[(name.to_string(), service.clone())].into_iter().collect(),
                    );
                }
                OutputFormat::Json => {
                    println!(
                        "{}",
                        serde_json::to_string_pretty(&format::services_to_json(
                            &[(name.to_string(), service.clone())].into_iter().collect()
                        ))?
                    );
                }
            }
        }
        ResourceType::Pod => {
            anyhow::bail!(
                "describing individual pods is not supported. Try: dv get /{}/po",
                namespace_id
            );
        }
    }
    Ok(())
}

/// Normalize resource type strings to canonical plural form
fn normalize_resource(r: &str) -> &str {
    match r {
        "namespace" | "namespaces" | "ns" => "namespaces",
        "worker" | "workers" => "workers",
        "pod" | "pods" => "pods",
        "service" | "services" | "svc" => "services",
        "workload" | "workloads" => "workloads",
        other => other,
    }
}
