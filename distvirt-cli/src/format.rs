use distvirt_client_protocol::*;
use tabled::{Table, Tabled, settings::Style};

// --- Namespace overview ---

pub fn print_namespace_overview(report: &NamespaceStatusReport) {
    println!(
        "Namespace: {}  State: {}",
        report.namespace_id,
        namespace_state_label(report.state())
    );
    println!();

    if report.workloads.is_empty() && report.services.is_empty() {
        println!("  (no workloads)");
        return;
    }

    // Group services by workload_id
    for (workload_id, workload) in &report.workloads {
        let state = workload
            .state
            .as_ref()
            .map(|s| workload_state_label(s))
            .unwrap_or("unknown");
        let spliced = if workload.spliced { " [spliced]" } else { "" };
        println!("  workload/{:<20} {}{}", workload_id, state, spliced);

        for (svc_id, svc) in &report.services {
            if svc.workload_id == *workload_id {
                let svc_state = svc
                    .state
                    .as_ref()
                    .map(|s| service_state_label(s))
                    .unwrap_or("unknown");
                let activation = if svc.activation_enabled {
                    " (activation)"
                } else {
                    ""
                };
                println!("    service/{:<18} {}{}", svc_id, svc_state, activation);
            }
        }
    }
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
            let activation = if svc.activation_enabled { "yes" } else { "no" };
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

// --- Events ---

pub fn print_event_line(event: &NamespaceEvent) {
    let ts = format_timestamp(event.timestamp_unix_ms);
    match &event.event {
        Some(namespace_event::Event::WorkloadEvent(we)) => {
            let desc = workload_event_description(we);
            println!("{}  workload/{}  {}", ts, we.workload_id, desc);
        }
        Some(namespace_event::Event::ServiceEvent(se)) => {
            let desc = service_event_description(se);
            println!("{}  service/{}  {}", ts, se.service_id, desc);
        }
        None => {
            println!("{}  (unknown event)", ts);
        }
    }
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

// --- State labels ---

fn namespace_state_label(state: NamespaceState) -> &'static str {
    match state {
        NamespaceState::Unspecified => "unknown",
        NamespaceState::Creating => "creating",
        NamespaceState::Active => "active",
        NamespaceState::Destroying => "destroying",
    }
}

fn workload_state_label(state: &WorkloadState) -> &'static str {
    match &state.state {
        Some(workload_state::State::Dormant(_)) => "dormant",
        Some(workload_state::State::WaitingForCapacity(_)) => "waiting",
        Some(workload_state::State::Launching(_)) => "launching",
        Some(workload_state::State::Running(_)) => "running",
        None => "unknown",
    }
}

fn service_state_label(state: &ServiceState) -> &'static str {
    match &state.state {
        Some(service_state::State::Pending(_)) => "pending",
        Some(service_state::State::Idle(_)) => "idle",
        Some(service_state::State::NeedBackend(_)) => "need-backend",
        Some(service_state::State::Active(_)) => "active",
        None => "unknown",
    }
}

fn pod_state_label(state: PodState) -> &'static str {
    match state {
        PodState::Unspecified => "unknown",
        PodState::Launching => "launching",
        PodState::Running => "running",
        PodState::Suspending => "suspending",
        PodState::Suspended => "suspended",
        PodState::Resuming => "resuming",
    }
}

// --- Event descriptions ---

fn workload_event_description(we: &WorkloadEvent) -> String {
    match &we.event {
        Some(workload_event::Event::DemandChanged(d)) => {
            format!("demand changed ({} services)", d.demanding_services)
        }
        Some(workload_event::Event::PodLaunching(l)) => {
            format!("pod launching on {}", l.worker_id)
        }
        Some(workload_event::Event::PodRunning(r)) => {
            format!("pod running on {}", r.worker_id)
        }
        Some(workload_event::Event::PodStopped(s)) => {
            format!("pod stopped: exited with code {}", s.exit_code)
        }
        Some(workload_event::Event::PodFailed(f)) => format!("pod failed: {}", f.reason),
        Some(workload_event::Event::Spliced(s)) => format!("spliced to {}", s.worker_id),
        Some(workload_event::Event::Unspliced(_)) => "unspliced".to_string(),
        Some(workload_event::Event::PodSuspending(s)) => {
            format!("pod suspending on {}", s.worker_id)
        }
        Some(workload_event::Event::PodSuspended(s)) => {
            format!("pod suspended (artifact: {})", s.snapshot_id)
        }
        Some(workload_event::Event::PodSuspendFailed(f)) => {
            format!("pod suspend failed: {}", f.reason)
        }
        Some(workload_event::Event::PodResuming(r)) => {
            format!("pod resuming on {}", r.worker_id)
        }
        None => "unknown event".to_string(),
    }
}

fn service_event_description(se: &ServiceEvent) -> String {
    match &se.event {
        Some(service_event::Event::Activated(a)) => {
            let trigger = ServiceActivationTrigger::try_from(a.trigger)
                .unwrap_or(ServiceActivationTrigger::Unspecified);
            let label = match trigger {
                ServiceActivationTrigger::Traffic => "traffic",
                _ => "unknown",
            };
            format!("activated ({})", label)
        }
        Some(service_event::Event::BackendReady(_)) => "backend ready".to_string(),
        Some(service_event::Event::IdleTimerStarted(t)) => {
            format!("idle timer started ({}ms)", t.timeout_ms)
        }
        Some(service_event::Event::IdleTimerCancelled(c)) => {
            let reason = IdleTimerCancelReason::try_from(c.reason)
                .unwrap_or(IdleTimerCancelReason::Unspecified);
            let label = match reason {
                IdleTimerCancelReason::NewTraffic => "new traffic",
                _ => "unknown",
            };
            format!("idle timer cancelled: {}", label)
        }
        Some(service_event::Event::IdleTimeoutFired(_)) => "idle timeout fired".to_string(),
        Some(service_event::Event::Deactivated(d)) => {
            let reason = ServiceDeactivationReason::try_from(d.reason)
                .unwrap_or(ServiceDeactivationReason::Unspecified);
            let label = match reason {
                ServiceDeactivationReason::IdleTimeout => "idle timeout",
                _ => "unknown",
            };
            format!("deactivated: {}", label)
        }
        None => "unknown event".to_string(),
    }
}

// --- Helpers ---

fn format_timestamp(unix_ms: i64) -> String {
    let secs = unix_ms / 1000;
    let h = (secs / 3600) % 24;
    let m = (secs % 3600) / 60;
    let s = secs % 60;
    format!("{:02}:{:02}:{:02}", h, m, s)
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
            let state = w.state.as_ref().map(|s| workload_state_label(s)).unwrap_or("unknown");
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
