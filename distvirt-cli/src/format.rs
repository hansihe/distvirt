use distvirt_client_protocol::*;

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

pub fn print_namespace_table(namespaces: &[NamespaceStatusReport]) {
    println!("{:<24} {:<12} {:>10} {:>10}", "NAME", "STATE", "WORKLOADS", "SERVICES");
    for ns in namespaces {
        println!(
            "{:<24} {:<12} {:>10} {:>10}",
            ns.namespace_id,
            namespace_state_label(ns.state()),
            ns.workloads.len(),
            ns.services.len(),
        );
    }
}

pub fn print_worker_table(workers: &[WorkerInfo]) {
    println!(
        "{:<24} {:>10} {:>12} {:>12}",
        "WORKER", "ACTIVE", "MAX PODS", "AVAIL MB"
    );
    for w in workers {
        println!(
            "{:<24} {:>10} {:>12} {:>12}",
            w.worker_id, w.active_pods, w.max_pods, w.available_memory_mb,
        );
    }
}

pub fn print_worker_detail(worker: &WorkerInfo) {
    println!("Worker:           {}", worker.worker_id);
    println!("Active Pods:      {}", worker.active_pods);
    println!("Max Pods:         {}", worker.max_pods);
    println!("Available Memory: {} MB", worker.available_memory_mb);
}

pub fn print_pod_table(pods: &[PodInfo]) {
    println!(
        "{:<20} {:<20} {:<20} {:<16} {:<10}",
        "POD", "WORKLOAD", "WORKER", "IP", "STATE"
    );
    for p in pods {
        println!(
            "{:<20} {:<20} {:<20} {:<16} {:<10}",
            p.pod_id,
            p.workload_id,
            p.worker_id,
            p.ip,
            pod_state_label(p.state()),
        );
    }
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
        println!("[{}] {}", chunk.workload_id, line);
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
        Some(workload_event::Event::PodStopped(s)) => format!("pod stopped: {}", s.reason),
        Some(workload_event::Event::PodFailed(f)) => format!("pod failed: {}", f.reason),
        Some(workload_event::Event::Spliced(s)) => format!("spliced to {}", s.worker_id),
        Some(workload_event::Event::Unspliced(_)) => "unspliced".to_string(),
        None => "unknown event".to_string(),
    }
}

fn service_event_description(se: &ServiceEvent) -> String {
    match &se.event {
        Some(service_event::Event::Activated(a)) => format!("activated ({})", a.trigger),
        Some(service_event::Event::BackendReady(_)) => "backend ready".to_string(),
        Some(service_event::Event::IdleTimerStarted(t)) => {
            format!("idle timer started ({}ms)", t.timeout_ms)
        }
        Some(service_event::Event::IdleTimerCancelled(c)) => {
            format!("idle timer cancelled: {}", c.reason)
        }
        Some(service_event::Event::IdleTimeoutFired(_)) => "idle timeout fired".to_string(),
        Some(service_event::Event::Deactivated(d)) => format!("deactivated: {}", d.reason),
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
    serde_json::json!(namespaces
        .iter()
        .map(|ns| {
            serde_json::json!({
                "namespace_id": ns.namespace_id,
                "state": namespace_state_label(ns.state()),
                "workloads": ns.workloads.len(),
                "services": ns.services.len(),
            })
        })
        .collect::<Vec<_>>())
}

pub fn workers_to_json(workers: &[WorkerInfo]) -> serde_json::Value {
    serde_json::json!(workers
        .iter()
        .map(|w| {
            serde_json::json!({
                "worker_id": w.worker_id,
                "active_pods": w.active_pods,
                "max_pods": w.max_pods,
                "available_memory_mb": w.available_memory_mb,
            })
        })
        .collect::<Vec<_>>())
}

pub fn pods_to_json(pods: &[PodInfo]) -> serde_json::Value {
    serde_json::json!(pods
        .iter()
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
        .collect::<Vec<_>>())
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
