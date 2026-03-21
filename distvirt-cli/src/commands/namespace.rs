use std::path::Path;

use anyhow::bail;
use distvirt_client_protocol::*;

use crate::client::{self, Client};
use crate::format;

/// Find the spec file to use. Checks distvirt.yaml, distvirt.yml, then
/// docker-compose.yml in the current directory.
fn find_default_file() -> anyhow::Result<std::path::PathBuf> {
    for candidate in &["distvirt.yaml", "distvirt.yml"] {
        let p = std::path::PathBuf::from(candidate);
        if p.exists() {
            return Ok(p);
        }
    }
    bail!(
        "no spec file found (looked for distvirt.yaml, distvirt.yml). Use -f to specify a file."
    )
}

/// Parse a spec file and return (optional namespace_id, NamespaceSpec).
fn parse_spec_file(file: &Path) -> anyhow::Result<(Option<String>, NamespaceSpec)> {
    if let Some(mut native) = distvirt_client::try_parse(file)? {
        distvirt_client::resolve_includes(&mut native, file)?;
        let (ns_id, proto_spec) = distvirt_client::spec_to_namespace_spec(&native)?;
        return Ok((ns_id, proto_spec));
    }

    bail!("failed to parse spec file '{}'", file.display())
}

/// Validate a spec file: parse, resolve includes, and run validation.
/// Prints errors/warnings and exits with an error if validation fails.
pub fn validate(file: Option<&Path>) -> anyhow::Result<()> {
    let file = match file {
        Some(f) => f.to_path_buf(),
        None => find_default_file()?,
    };

    let mut parsed = match distvirt_client::try_parse(&file)? {
        Some(p) => p,
        None => bail!("'{}' is not a native distvirt spec file", file.display()),
    };
    distvirt_client::resolve_includes(&mut parsed, &file)?;
    let (ns_id, proto) = distvirt_client::spec_to_namespace_spec(&parsed)?;

    let n_workloads = proto.workloads.len();
    let n_services = proto.services.len();

    eprintln!("spec '{}' is valid", file.display());
    if let Some(id) = ns_id {
        eprintln!("  namespace:  {}", id);
    }
    eprintln!("  workloads:  {}", n_workloads);
    eprintln!("  services:   {}", n_services);
    Ok(())
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

/// Resolve namespace ID from CLI arg or spec file, and parse the spec.
fn resolve_spec(
    namespace_id: Option<&str>,
    file: Option<&Path>,
) -> anyhow::Result<(String, NamespaceSpec)> {
    let file = match file {
        Some(f) => f.to_path_buf(),
        None => find_default_file()?,
    };

    let (spec_ns_id, spec) = parse_spec_file(&file)?;

    let namespace_id = match namespace_id {
        Some(id) => id.to_string(),
        None => spec_ns_id.ok_or_else(|| {
            anyhow::anyhow!(
                "namespace ID required: specify as argument or set metadata.name in spec file"
            )
        })?,
    };

    Ok((namespace_id, spec))
}

/// Apply a spec: create the namespace if new, patch (upsert) workloads/services if it exists.
pub async fn apply(
    mut client: Client,
    namespace_id: Option<&str>,
    file: Option<&Path>,
) -> anyhow::Result<()> {
    let (namespace_id, spec) = resolve_spec(namespace_id, file)?;

    let result = client
        .create_namespace(CreateNamespaceRequest {
            namespace_id: namespace_id.clone(),
            spec: Some(spec.clone()),
        })
        .await;

    match result {
        Ok(_) => {
            eprintln!("namespace '{}' created", namespace_id);
        }
        Err(status) if status.code() == tonic::Code::AlreadyExists => {
            client
                .patch_namespace(PatchNamespaceRequest {
                    namespace_id: namespace_id.clone(),
                    workloads: spec.workloads,
                    services: spec.services,
                    remove_workloads: vec![],
                    remove_services: vec![],
                })
                .await
                .map_err(client::handle_grpc_error)?;
            eprintln!("namespace '{}' patched", namespace_id);
        }
        Err(status) => return Err(client::handle_grpc_error(status)),
    }

    Ok(())
}

/// Sync a spec: create the namespace if new, fully replace the spec if it exists.
pub async fn sync(
    mut client: Client,
    namespace_id: Option<&str>,
    file: Option<&Path>,
) -> anyhow::Result<()> {
    let (namespace_id, spec) = resolve_spec(namespace_id, file)?;

    let result = client
        .create_namespace(CreateNamespaceRequest {
            namespace_id: namespace_id.clone(),
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
                    namespace_id: namespace_id.clone(),
                    spec: Some(spec),
                })
                .await
                .map_err(client::handle_grpc_error)?;
            eprintln!("namespace '{}' synced", namespace_id);
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

pub async fn status(mut client: Client, target: &str, watch: bool) -> anyhow::Result<()> {
    let (namespace_id, workload_id) = parse_target(target);

    if watch && workload_id.is_none() {
        // Subscribe to events *before* fetching status to avoid missing events
        let event_stream = client
            .stream_events(StreamEventsRequest {
                namespace_id: namespace_id.to_string(),
                workload_ids: vec![],
                service_ids: vec![],
            })
            .await
            .map_err(client::handle_grpc_error)?
            .into_inner();

        return crate::status_watch::run(client, namespace_id, event_stream).await;
    }

    // Non-watch mode (or workload-specific view)
    let mut event_stream = if watch {
        Some(
            client
                .stream_events(StreamEventsRequest {
                    namespace_id: namespace_id.to_string(),
                    workload_ids: vec![],
                    service_ids: vec![],
                })
                .await
                .map_err(client::handle_grpc_error)?
                .into_inner(),
        )
    } else {
        None
    };

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
            .map(|s| format::workload_state_detail(s))
            .unwrap_or_else(|| "unknown".to_string());
        let spliced = if workload.spliced { " [spliced]" } else { "" };

        println!("Workload: {}/{}", namespace_id, wid);
        println!("State:    {}{}", state, spliced);
        if !workload.ip.is_empty() {
            println!("IP:       {}", workload.ip);
        }
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

    if let Some(ref mut stream) = event_stream {
        println!();
        println!("--- watching events ---");

        while let Some(event) = stream.message().await.map_err(client::handle_grpc_error)? {
            format::print_event_line(&event);
        }
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

