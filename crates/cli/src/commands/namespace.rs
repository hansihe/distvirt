use std::path::Path;

use distvirt_client::connection::Client;

use crate::format;

/// Validate a spec file: parse, resolve includes, and run validation.
/// Prints errors/warnings and exits with an error if validation fails.
pub fn validate(file: Option<&Path>) -> anyhow::Result<()> {
    let file = match file {
        Some(f) => f.to_path_buf(),
        None => distvirt_client::spec::find_default_file()?,
    };

    let mut parsed = match distvirt_client::try_parse(&file)? {
        Some(p) => p,
        None => anyhow::bail!("'{}' is not a native distvirt spec file", file.display()),
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
    let (ns_id, proto_spec) = distvirt_client::spec::parse_spec_file(file)?;
    if let Some(ref id) = ns_id {
        println!("namespace: {}", id);
    }
    println!("{:#?}", proto_spec);
    Ok(())
}

/// Apply a spec: create the namespace if new, patch (upsert) workloads/services if it exists.
pub async fn apply(
    mut client: Client,
    namespace_id: Option<&str>,
    file: Option<&Path>,
) -> anyhow::Result<()> {
    let (namespace_id, spec) =
        distvirt_client::spec::resolve_spec(namespace_id, file)?;

    match distvirt_client::operations::apply(&mut client, &namespace_id, &spec).await? {
        distvirt_client::operations::ApplyOutcome::Created => {
            eprintln!("namespace '{}' created", namespace_id);
        }
        distvirt_client::operations::ApplyOutcome::Patched => {
            eprintln!("namespace '{}' patched", namespace_id);
        }
    }

    Ok(())
}

/// Sync a spec: create the namespace if new, fully replace the spec if it exists.
pub async fn sync(
    mut client: Client,
    namespace_id: Option<&str>,
    file: Option<&Path>,
) -> anyhow::Result<()> {
    let (namespace_id, spec) =
        distvirt_client::spec::resolve_spec(namespace_id, file)?;

    match distvirt_client::operations::sync(&mut client, &namespace_id, &spec).await? {
        distvirt_client::operations::SyncOutcome::Created => {
            eprintln!("namespace '{}' created", namespace_id);
        }
        distvirt_client::operations::SyncOutcome::Synced => {
            eprintln!("namespace '{}' synced", namespace_id);
        }
    }

    Ok(())
}

pub async fn down(mut client: Client, namespace_id: &str) -> anyhow::Result<()> {
    distvirt_client::operations::down(&mut client, namespace_id).await?;
    eprintln!("namespace '{}' deleted", namespace_id);
    Ok(())
}

pub async fn status(mut client: Client, target: &str, watch: bool) -> anyhow::Result<()> {
    let (namespace_id, workload_id) = parse_target(target);

    if watch && workload_id.is_none() {
        let watcher =
            distvirt_client::watcher::NamespaceWatcher::start(&mut client, namespace_id).await?;
        return crate::status_watch::run(watcher).await;
    }

    // Non-watch mode (or workload-specific view)
    let (report, mut event_stream) = if watch {
        let (report, stream) =
            distvirt_client::operations::watch_status(&mut client, namespace_id).await?;
        (report, Some(stream))
    } else {
        let report =
            distvirt_client::operations::get_status(&mut client, namespace_id).await?;
        (report, None)
    };

    if let Some(wid) = workload_id {
        // Show specific workload detail
        let workload = report
            .workloads
            .get(wid)
            .ok_or_else(|| anyhow::anyhow!("workload '{}' not found in namespace", wid))?;

        let state = workload
            .state
            .as_ref()
            .map(|s| distvirt_client::format::workload_state_detail(s))
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

        while let Some(event) = stream.message().await.map_err(distvirt_client::connection::handle_grpc_error)? {
            format::print_event_line(&event);
        }
    }

    Ok(())
}

pub async fn deactivate(mut client: Client, target: &str) -> anyhow::Result<()> {
    let (namespace_id, workload_id) = parse_target(target);
    let workload_id = workload_id
        .ok_or_else(|| anyhow::anyhow!("target must be namespace/workload (e.g. myapp/api)"))?;

    let outcome = distvirt_client::operations::deactivate(
        &mut client,
        namespace_id,
        workload_id,
    )
    .await?;

    if outcome.deactivated {
        eprintln!(
            "Workload {} deactivated — pod stopping, services returning to idle.",
            workload_id
        );
    } else {
        eprintln!(
            "Workload {} has active demand — not deactivating.",
            workload_id
        );
        if !outcome.reason.is_empty() {
            eprintln!("  Reason: {}", outcome.reason);
        }
    }

    Ok(())
}

pub async fn clone_namespace(
    mut client: Client,
    source: &str,
    target: &str,
) -> anyhow::Result<()> {
    distvirt_client::operations::clone_namespace(&mut client, source, target).await?;
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
