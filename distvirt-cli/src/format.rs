use distvirt_client::format::{
    namespace_state_label, pod_state_label, render_event_line, render_namespace_overview,
    service_state_label, workload_state_label,
};
use distvirt_client_protocol::*;
use tabled::{Table, Tabled, settings::Style};

// --- Namespace overview ---

pub fn print_namespace_overview(report: &NamespaceStatusReport) {
    print!("{}", render_namespace_overview(report));
}

// --- Tables ---

#[derive(Tabled)]
struct NamespaceRow {
    #[tabled(rename = "NAME")]
    name: String,
    #[tabled(rename = "STATE")]
    state: &'static str,
    #[tabled(rename = "WORKLOADS")]
    workloads: usize,
    #[tabled(rename = "SERVICES")]
    services: usize,
}

pub fn print_namespace_table(namespaces: &[NamespaceStatusReport]) {
    let rows: Vec<_> = namespaces
        .iter()
        .map(|ns| NamespaceRow {
            name: ns.namespace_id.clone(),
            state: namespace_state_label(ns.state()),
            workloads: ns.workloads.len(),
            services: ns.services.len(),
        })
        .collect();
    println!("{}", Table::new(rows).with(Style::blank()));
}

#[derive(Tabled)]
struct WorkerRow {
    #[tabled(rename = "WORKER")]
    worker: String,
    #[tabled(rename = "ACTIVE")]
    active: u32,
    #[tabled(rename = "MAX PODS")]
    max_pods: u32,
    #[tabled(rename = "AVAIL MB")]
    avail_mb: u64,
}

pub fn print_worker_table(workers: &[WorkerInfo]) {
    let rows: Vec<_> = workers
        .iter()
        .map(|w| WorkerRow {
            worker: w.worker_id.clone(),
            active: w.active_pods,
            max_pods: w.max_pods,
            avail_mb: w.available_memory_mb,
        })
        .collect();
    println!("{}", Table::new(rows).with(Style::blank()));
}

pub fn print_worker_detail(worker: &WorkerInfo) {
    println!("Worker:           {}", worker.worker_id);
    println!("Active Pods:      {}", worker.active_pods);
    println!("Max Pods:         {}", worker.max_pods);
    println!("Available Memory: {} MB", worker.available_memory_mb);
}

#[derive(Tabled)]
struct PodRow {
    #[tabled(rename = "POD")]
    pod: String,
    #[tabled(rename = "WORKLOAD")]
    workload: String,
    #[tabled(rename = "WORKER")]
    worker: String,
    #[tabled(rename = "IP")]
    ip: String,
    #[tabled(rename = "STATE")]
    state: &'static str,
}

pub fn print_pod_table(pods: &[PodInfo]) {
    let rows: Vec<_> = pods
        .iter()
        .map(|p| PodRow {
            pod: p.pod_id.clone(),
            workload: p.workload_id.clone(),
            worker: p.worker_id.clone(),
            ip: p.ip.clone(),
            state: pod_state_label(p.state()),
        })
        .collect();
    println!("{}", Table::new(rows).with(Style::blank()));
}

#[derive(Tabled)]
struct WorkloadRow {
    #[tabled(rename = "WORKLOAD")]
    workload: String,
    #[tabled(rename = "STATE")]
    state: String,
    #[tabled(rename = "IP")]
    ip: String,
    #[tabled(rename = "SPLICED")]
    spliced: &'static str,
}

pub fn print_workload_table(workloads: &std::collections::HashMap<String, WorkloadStatusReport>) {
    let mut entries: Vec<_> = workloads.iter().collect();
    entries.sort_by_key(|(id, _)| (*id).clone());
    let rows: Vec<_> = entries
        .into_iter()
        .map(|(wl_id, wl)| {
            let state = wl
                .state
                .as_ref()
                .map(|s| workload_state_label(s))
                .unwrap_or_else(|| "unknown".into());
            WorkloadRow {
                workload: wl_id.clone(),
                state,
                ip: wl.ip.clone(),
                spliced: if wl.spliced { "yes" } else { "no" },
            }
        })
        .collect();
    println!("{}", Table::new(rows).with(Style::blank()));
}

#[derive(Tabled)]
struct ServiceRow {
    #[tabled(rename = "SERVICE")]
    service: String,
    #[tabled(rename = "WORKLOAD")]
    workload: String,
    #[tabled(rename = "IP")]
    ip: String,
    #[tabled(rename = "MAC")]
    mac: String,
    #[tabled(rename = "STATE")]
    state: &'static str,
    #[tabled(rename = "ACTIVATION")]
    activation: &'static str,
}

pub fn print_service_table(services: &std::collections::HashMap<String, ServiceStatusReport>) {
    let mut entries: Vec<_> = services.iter().collect();
    entries.sort_by_key(|(id, _)| (*id).clone());
    let rows: Vec<_> = entries
        .into_iter()
        .map(|(svc_id, svc)| {
            let state = svc
                .state
                .as_ref()
                .map(|s| service_state_label(s))
                .unwrap_or("unknown");
            let activation = if svc.activation_enabled {
                "yes"
            } else {
                "no"
            };
            ServiceRow {
                service: svc_id.clone(),
                workload: svc.workload_id.clone(),
                ip: svc.ip.clone(),
                mac: svc.mac.clone(),
                state,
                activation,
            }
        })
        .collect();
    println!("{}", Table::new(rows).with(Style::blank()));
}

// --- Events ---

pub fn print_event_line(event: &NamespaceEvent) {
    println!("{}", render_event_line(event));
}

pub fn print_log_chunk(chunk: &LogChunk) {
    let text = String::from_utf8_lossy(&chunk.data);
    for line in text.lines() {
        if chunk.container_id.is_empty() {
            println!("[{}] {}", chunk.workload_id, line);
        } else {
            println!("[{}/{}] {}", chunk.workload_id, chunk.container_id, line);
        }
    }
}

// --- JSON output helpers ---

pub fn namespaces_to_json(namespaces: &[NamespaceStatusReport]) -> serde_json::Value {
    serde_json::json!(
        namespaces
            .iter()
            .map(|ns| {
                serde_json::json!({
                    "namespace_id": ns.namespace_id,
                    "state": namespace_state_label(ns.state()),
                    "workloads": ns.workloads.len(),
                    "services": ns.services.len(),
                })
            })
            .collect::<Vec<_>>()
    )
}

pub fn workers_to_json(workers: &[WorkerInfo]) -> serde_json::Value {
    serde_json::json!(
        workers
            .iter()
            .map(|w| {
                serde_json::json!({
                    "worker_id": w.worker_id,
                    "active_pods": w.active_pods,
                    "max_pods": w.max_pods,
                    "available_memory_mb": w.available_memory_mb,
                })
            })
            .collect::<Vec<_>>()
    )
}

pub fn pods_to_json(pods: &[PodInfo]) -> serde_json::Value {
    serde_json::json!(
        pods.iter()
            .map(|p| {
                serde_json::json!({
                    "pod_id": p.pod_id,
                    "workload_id": p.workload_id,
                    "worker_id": p.worker_id,
                    "ip": p.ip,
                    "mac": p.mac,
                    "state": pod_state_label(p.state()),
                })
            })
            .collect::<Vec<_>>()
    )
}

pub fn services_to_json(
    services: &std::collections::HashMap<String, ServiceStatusReport>,
) -> serde_json::Value {
    serde_json::json!(
        services
            .iter()
            .map(|(id, s)| {
                let state = s
                    .state
                    .as_ref()
                    .map(|st| service_state_label(st))
                    .unwrap_or("unknown");
                serde_json::json!({
                    "service_id": id,
                    "workload_id": s.workload_id,
                    "ip": s.ip,
                    "mac": s.mac,
                    "state": state,
                    "activation_enabled": s.activation_enabled,
                    "spliced": s.spliced,
                })
            })
            .collect::<Vec<_>>()
    )
}

pub fn workloads_to_json(
    workloads: &std::collections::HashMap<String, WorkloadStatusReport>,
) -> serde_json::Value {
    serde_json::json!(
        workloads
            .iter()
            .map(|(id, w)| {
                let state = w
                    .state
                    .as_ref()
                    .map(|s| workload_state_label(s))
                    .unwrap_or_else(|| "unknown".into());
                serde_json::json!({
                    "workload_id": id,
                    "state": state,
                    "ip": w.ip,
                    "spliced": w.spliced,
                })
            })
            .collect::<Vec<_>>()
    )
}

pub fn worker_to_json(w: &WorkerInfo) -> serde_json::Value {
    serde_json::json!({
        "worker_id": w.worker_id,
        "active_pods": w.active_pods,
        "max_pods": w.max_pods,
        "available_memory_mb": w.available_memory_mb,
    })
}

pub fn namespace_status_to_json(report: &NamespaceStatusReport) -> serde_json::Value {
    serde_json::json!({
        "namespace_id": report.namespace_id,
        "state": namespace_state_label(report.state()),
        "workloads": report.workloads.iter().map(|(id, w)| {
            let state: String = w.state.as_ref().map(|s| workload_state_label(s)).unwrap_or_else(|| "unknown".into());
            serde_json::json!({
                "workload_id": id,
                "state": state,
                "spliced": w.spliced,
            })
        }).collect::<Vec<_>>(),
        "services": report.services.iter().map(|(id, s)| {
            let state = s.state.as_ref().map(|st| service_state_label(st)).unwrap_or("unknown");
            serde_json::json!({
                "service_id": id,
                "workload_id": s.workload_id,
                "state": state,
                "activation_enabled": s.activation_enabled,
                "spliced": s.spliced,
            })
        }).collect::<Vec<_>>(),
    })
}
