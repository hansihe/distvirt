use distvirt_client_protocol::*;

// ---------------------------------------------------------------------------
// State labels
// ---------------------------------------------------------------------------

pub fn namespace_state_label(state: NamespaceState) -> &'static str {
    match state {
        NamespaceState::Unspecified => "unknown",
        NamespaceState::Creating => "creating",
        NamespaceState::Active => "active",
        NamespaceState::Destroying => "destroying",
    }
}

pub fn workload_state_label(state: &WorkloadState) -> String {
    match &state.state {
        Some(workload_state::State::Dormant(_)) => "dormant".into(),
        Some(workload_state::State::WaitingForSpec(_)) => "waiting".into(),
        Some(workload_state::State::Launching(_)) => "launching".into(),
        Some(workload_state::State::Running(_)) => "running".into(),
        Some(workload_state::State::Suspending(_)) => "suspending".into(),
        Some(workload_state::State::Suspended(_)) => "suspended".into(),
        Some(workload_state::State::RetryBackoff(_)) => "retry-backoff".into(),
        Some(workload_state::State::Failed(f)) => {
            let mut s = "failed".to_string();
            if let Some(code) = f.exit_code {
                s.push_str(&format!(" (exit {})", code));
            }
            if !f.reason.is_empty() {
                s.push_str(&format!(": {}", f.reason));
            }
            s
        }
        Some(workload_state::State::Completed(c)) => {
            format!("completed (exit {})", c.exit_code)
        }
        None => "unknown".into(),
    }
}

/// Verbose workload state for the single-workload detail view.
pub fn workload_state_detail(state: &WorkloadState) -> String {
    match &state.state {
        Some(workload_state::State::Dormant(_)) => "dormant".into(),
        Some(workload_state::State::WaitingForSpec(_)) => "waiting for spec".into(),
        Some(workload_state::State::Launching(l)) => {
            format!("launching (pod {} on worker {})", l.pod_id, l.worker_id)
        }
        Some(workload_state::State::Running(r)) => {
            format!("running (pod {} on worker {})", r.pod_id, r.worker_id)
        }
        Some(workload_state::State::Suspending(s)) => {
            format!("suspending (pod {} on worker {})", s.pod_id, s.worker_id)
        }
        Some(workload_state::State::Suspended(_)) => "suspended".into(),
        Some(workload_state::State::RetryBackoff(_)) => "retry backoff".into(),
        Some(workload_state::State::Failed(f)) => {
            let mut s = "failed".to_string();
            if let Some(code) = f.exit_code {
                s.push_str(&format!(" (exit code {})", code));
            }
            if !f.reason.is_empty() {
                s.push_str(&format!(": {}", f.reason));
            }
            s
        }
        Some(workload_state::State::Completed(c)) => {
            format!("completed (exit code {})", c.exit_code)
        }
        None => "unknown".into(),
    }
}

pub fn service_state_label(state: &ServiceState) -> &'static str {
    match &state.state {
        Some(service_state::State::Pending(_)) => "pending",
        Some(service_state::State::Idle(_)) => "idle",
        Some(service_state::State::NeedBackend(_)) => "need-backend",
        Some(service_state::State::Active(_)) => "active",
        None => "unknown",
    }
}

pub fn pod_state_label(state: PodState) -> &'static str {
    match state {
        PodState::Unspecified => "unknown",
        PodState::Launching => "launching",
        PodState::Running => "running",
        PodState::Suspending => "suspending",
        PodState::Suspended => "suspended",
        PodState::Resuming => "resuming",
        PodState::Finished => "finished",
        PodState::Failed => "failed",
        PodState::Displaced => "displaced",
    }
}

// ---------------------------------------------------------------------------
// Event descriptions
// ---------------------------------------------------------------------------

pub fn workload_event_description(we: &WorkloadEvent) -> String {
    match &we.event {
        Some(workload_event::Event::DemandChanged(d)) => {
            format!("demand changed ({} services)", d.demanding_services)
        }
        Some(workload_event::Event::Spliced(s)) => format!("spliced to {}", s.worker_id),
        Some(workload_event::Event::Unspliced(_)) => "unspliced".to_string(),
        Some(workload_event::Event::StateChanged(sc)) => {
            let old = sc
                .old_state
                .as_ref()
                .map(|s| workload_state_label(s))
                .unwrap_or_else(|| "unknown".into());
            let new = sc
                .new_state
                .as_ref()
                .map(|s| workload_state_label(s))
                .unwrap_or_else(|| "unknown".into());
            format!("{} -> {}", old, new)
        }
        None => "unknown event".to_string(),
    }
}

pub fn pod_event_description(pe: &PodEvent) -> String {
    match &pe.event {
        Some(pod_event::Event::Created(_)) => "created".to_string(),
        Some(pod_event::Event::Scheduled(s)) => format!("scheduled on {}", s.worker_id),
        Some(pod_event::Event::Running(r)) => {
            if r.worker_id.is_empty() {
                "running".to_string()
            } else {
                format!("running on {}", r.worker_id)
            }
        }
        Some(pod_event::Event::Stopped(s)) => {
            format!("stopped (exit code {})", s.exit_code)
        }
        Some(pod_event::Event::Failed(f)) => format!("failed: {}", f.reason),
        Some(pod_event::Event::Suspending(s)) => {
            if s.worker_id.is_empty() {
                "suspending".to_string()
            } else {
                format!("suspending on {}", s.worker_id)
            }
        }
        Some(pod_event::Event::Suspended(s)) => {
            if s.snapshot_id.is_empty() {
                "suspended".to_string()
            } else {
                format!("suspended (snapshot: {})", s.snapshot_id)
            }
        }
        Some(pod_event::Event::SuspendFailed(f)) => {
            format!("suspend failed: {}", f.reason)
        }
        Some(pod_event::Event::Resuming(r)) => {
            if r.worker_id.is_empty() {
                "resuming".to_string()
            } else {
                format!("resuming on {}", r.worker_id)
            }
        }
        Some(pod_event::Event::Displaced(_)) => "displaced".to_string(),
        Some(pod_event::Event::Reaped(_)) => "reaped".to_string(),
        None => "unknown event".to_string(),
    }
}

pub fn endpoint_event_description(ee: &EndpointEvent) -> String {
    match &ee.event {
        Some(endpoint_event::Event::Activated(a)) => {
            let trigger = EndpointActivationTrigger::try_from(a.trigger)
                .unwrap_or(EndpointActivationTrigger::Unspecified);
            let label = match trigger {
                EndpointActivationTrigger::Traffic => "traffic",
                _ => "unknown",
            };
            format!("activated ({})", label)
        }
        Some(endpoint_event::Event::BackendReady(_)) => "backend ready".to_string(),
        Some(endpoint_event::Event::IdleTimerStarted(t)) => {
            format!("idle timer started ({}ms)", t.timeout_ms)
        }
        Some(endpoint_event::Event::IdleTimerCancelled(c)) => {
            let reason = IdleTimerCancelReason::try_from(c.reason)
                .unwrap_or(IdleTimerCancelReason::Unspecified);
            let label = match reason {
                IdleTimerCancelReason::NewTraffic => "new traffic",
                _ => "unknown",
            };
            format!("idle timer cancelled: {}", label)
        }
        Some(endpoint_event::Event::IdleTimeoutFired(_)) => "idle timeout fired".to_string(),
        Some(endpoint_event::Event::Deactivated(d)) => {
            let reason = EndpointDeactivationReason::try_from(d.reason)
                .unwrap_or(EndpointDeactivationReason::Unspecified);
            let label = match reason {
                EndpointDeactivationReason::IdleTimeout => "idle timeout",
                _ => "unknown",
            };
            format!("deactivated: {}", label)
        }
        None => "unknown event".to_string(),
    }
}

// ---------------------------------------------------------------------------
// Timestamp formatting
// ---------------------------------------------------------------------------

pub fn format_timestamp(unix_ms: i64) -> String {
    let secs = unix_ms / 1000;
    let h = (secs / 3600) % 24;
    let m = (secs % 3600) / 60;
    let s = secs % 60;
    format!("{:02}:{:02}:{:02}", h, m, s)
}

// ---------------------------------------------------------------------------
// Rendering (proto → String, no terminal dependencies)
// ---------------------------------------------------------------------------

pub fn render_event_line(event: &NamespaceEvent) -> String {
    let ts = format_timestamp(event.timestamp_unix_ms);
    match &event.event {
        Some(namespace_event::Event::Workload(we)) => {
            let desc = workload_event_description(we);
            format!("{}  workload/{}  {}", ts, we.workload_id, desc)
        }
        Some(namespace_event::Event::Pod(pe)) => {
            let desc = pod_event_description(pe);
            format!(
                "{}  pod/{} (workload/{})  {}",
                ts, pe.pod_id, pe.workload_id, desc
            )
        }
        Some(namespace_event::Event::Endpoint(ee)) => {
            let desc = endpoint_event_description(ee);
            let owner = if let Some(ref svc) = ee.service_id {
                format!("service/{}", svc)
            } else if let Some(ref wl) = ee.workload_id {
                format!("workload/{}", wl)
            } else {
                "unknown".to_string()
            };
            format!("{}  endpoint/{} ({})  {}", ts, ee.endpoint_id, owner, desc)
        }
        None => {
            format!("{}  (unknown event)", ts)
        }
    }
}

pub fn render_namespace_overview(report: &NamespaceStatusReport) -> String {
    use std::fmt::Write;
    let mut buf = String::new();

    writeln!(
        &mut buf,
        "Namespace: {}  State: {}",
        report.namespace_id,
        namespace_state_label(report.state())
    )
    .unwrap();
    writeln!(&mut buf).unwrap();

    if report.workloads.is_empty() && report.services.is_empty() {
        writeln!(&mut buf, "  (no workloads)").unwrap();
        return buf;
    }

    let mut sorted_workloads: Vec<_> = report.workloads.iter().collect();
    sorted_workloads.sort_by_key(|(id, _)| id.as_str());
    let mut sorted_services: Vec<_> = report.services.iter().collect();
    sorted_services.sort_by_key(|(id, _)| id.as_str());
    for (workload_id, workload) in &sorted_workloads {
        let state = workload
            .state
            .as_ref()
            .map(|s| workload_state_label(s))
            .unwrap_or_else(|| "unknown".into());
        let restarts = if workload.restart_count > 0 {
            format!(" ({} restarts)", workload.restart_count)
        } else {
            String::new()
        };
        let spliced = if workload.spliced { " [spliced]" } else { "" };
        let ip = if workload.ip.is_empty() {
            ""
        } else {
            &workload.ip
        };
        if ip.is_empty() {
            writeln!(&mut buf, "  workload/{:<20} {}{}{}", workload_id, state, restarts, spliced).unwrap();
        } else {
            writeln!(
                &mut buf,
                "  workload/{:<20} {}{}  {}{}",
                workload_id, state, restarts, ip, spliced
            )
            .unwrap();
        }

        for (svc_id, svc) in &sorted_services {
            if svc.workload_id.as_str() == workload_id.as_str() {
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
                writeln!(
                    &mut buf,
                    "    service/{:<18} {}{}",
                    svc_id, svc_state, activation
                )
                .unwrap();
            }
        }
    }

    buf
}
