use std::cell::RefCell;
use std::time::Duration;

use distvirt_orchestrator::adapter::timer::TimerConfig;
use distvirt_orchestrator::core::namespace::NamespaceCore;
use distvirt_orchestrator::shell_new::sync::{MockWorkerConfig, SyncShell};
use distvirt_orchestrator::sm_new::{ServiceSm, ServiceState, SvcStatus, WlStatus, WorkloadSm};
use distvirt_orchestrator::task::{ClientCommand, GlobalWorkerId};
use distvirt_orchestrator::types::*;
use distvirt_worker_protocol::{PsiMetrics, WorkerCommand, WorkerEvent};

fn test_timer_config() -> TimerConfig {
    TimerConfig {
        retry_backoff: Duration::from_millis(500),
        launch_timeout: Duration::from_secs(30),
        suspend_timeout: Duration::from_secs(30),
        idle_timeout: Duration::from_secs(60),
    }
}

pub struct TestHarness {
    pub shell: SyncShell,
    pending_events: RefCell<Vec<(GlobalWorkerId, WorkerEvent)>>,
}

impl TestHarness {
    pub fn new() -> Self {
        TestHarness {
            shell: SyncShell::new(test_timer_config()),
            pending_events: RefCell::new(Vec::new()),
        }
    }

    // =========================================================================
    // Worker lifecycle
    // =========================================================================

    pub fn add_worker(&mut self) -> GlobalWorkerId {
        self.add_worker_with(MockWorkerConfig::default())
    }

    pub fn add_worker_with(&mut self, config: MockWorkerConfig) -> GlobalWorkerId {
        let wid = self.shell.add_worker(config);
        self.shell.drain();
        wid
    }

    pub fn disconnect_worker(&mut self, worker_id: &GlobalWorkerId) {
        self.shell.disconnect_worker(*worker_id);
    }

    /// Access a worker handle proxy for event injection and command inspection.
    pub fn worker(&self, worker_id: &GlobalWorkerId) -> WorkerProxy<'_> {
        assert!(
            self.shell.has_worker(worker_id),
            "worker {:?} not found",
            worker_id
        );
        WorkerProxy {
            shell: &self.shell,
            worker_id: *worker_id,
            pending_events: &self.pending_events,
        }
    }

    // =========================================================================
    // Namespace lifecycle
    // =========================================================================

    pub fn create_namespace(&mut self, ns_id: &str, spec: NamespaceSpec) {
        let namespace_id = NamespaceId::from(ns_id);
        self.shell
            .create_namespace(namespace_id.clone(), spec.network.clone());
        self.shell
            .client_command(&namespace_id, ClientCommand::UpdateSpec(spec));
        self.shell.drain();
    }

    pub fn update_namespace(&mut self, ns_id: &str, spec: NamespaceSpec) {
        let namespace_id = NamespaceId::from(ns_id);
        self.shell
            .client_command(&namespace_id, ClientCommand::UpdateSpec(spec));
        self.shell.drain();
    }

    pub fn delete_namespace(&mut self, ns_id: &str) {
        let namespace_id = NamespaceId::from(ns_id);
        self.shell.destroy_namespace(&namespace_id);
    }

    // =========================================================================
    // Converge / time
    // =========================================================================

    pub fn converge(&mut self) {
        // Drain any events queued through WorkerProxy (shared-ref path).
        for (wid, event) in self.pending_events.borrow_mut().drain(..) {
            self.shell.queue_worker_event(wid, event);
        }
        self.shell.drain();
    }

    pub fn advance_time(&mut self, duration: Duration) {
        self.shell.advance_time(duration);
        self.converge();
    }

    // =========================================================================
    // State access
    // =========================================================================

    pub fn namespace(&self, ns_id: &str) -> &NamespaceCore {
        self.shell
            .namespace(&NamespaceId::from(ns_id))
            .unwrap_or_else(|| panic!("namespace '{}' not found", ns_id))
    }

    pub fn workload_state(&self, ns_id: &str, wl_name: &str) -> &WorkloadSm {
        let ns = self.namespace(ns_id);
        let wl_id = ns
            .management()
            .lookup_workload(wl_name)
            .unwrap_or_else(|| panic!("workload '{}' not found in namespace '{}'", wl_name, ns_id));
        ns.router()
            .get_workload(&wl_id)
            .unwrap_or_else(|| panic!("workload SM '{}' not found in router", wl_name))
    }

    pub fn service_state_sm(&self, ns_id: &str, svc_name: &str) -> &ServiceSm {
        let ns = self.namespace(ns_id);
        let svc_id = ns
            .management()
            .lookup_service(svc_name)
            .unwrap_or_else(|| panic!("service '{}' not found in namespace '{}'", svc_name, ns_id));
        ns.router()
            .get_service(&svc_id)
            .unwrap_or_else(|| panic!("service SM '{}' not found in router", svc_name))
    }

    /// Get the protocol PodId for a workload (maps from router-internal PodId).
    pub fn workload_proto_pod_id(
        &self,
        ns_id: &str,
        wl_name: &str,
    ) -> Option<distvirt_worker_protocol::PodId> {
        let wl = self.workload_state(ns_id, wl_name);
        let ns = self.namespace(ns_id);
        wl.pod_id
            .and_then(|pid| ns.router_pod_to_proto(&pid).cloned())
    }

    /// Get the GlobalWorkerId for the worker hosting a workload.
    pub fn workload_global_worker_id(&self, ns_id: &str, wl_name: &str) -> Option<GlobalWorkerId> {
        let wl = self.workload_state(ns_id, wl_name);
        let ns = self.namespace(ns_id);
        wl.pod_worker_id
            .and_then(|wid| ns.router_worker_to_global(&wid))
    }

    pub fn workload_status(&self, ns_id: &str, wl_name: &str) -> WlStatus {
        let wl = self.workload_state(ns_id, wl_name);
        let is_failed =
            wl.consecutive_failures >= wl.max_retries && (wl.has_demand || wl.committed_to_boot);
        if is_failed {
            WlStatus::Failed
        } else if wl.in_backoff {
            WlStatus::RetryBackoff
        } else if wl.awaiting_suspend {
            WlStatus::Suspending
        } else if wl.suspended_artifact.is_some() && wl.pod_id.is_none() {
            WlStatus::Suspended
        } else if wl.pod_running {
            WlStatus::Running
        } else if wl.pod_id.is_some() {
            WlStatus::Launching
        } else if !wl.has_spec && (wl.has_demand || wl.committed_to_boot) {
            WlStatus::WaitingForSpec
        } else {
            WlStatus::Dormant
        }
    }

    pub fn service_status(&self, ns_id: &str, svc_name: &str) -> SvcStatus {
        let svc = self.service_state_sm(ns_id, svc_name);
        match &svc.state {
            ServiceState::Idle => SvcStatus::Idle,
            ServiceState::NeedBackend => SvcStatus::NeedBackend,
            ServiceState::Active { .. } => SvcStatus::Active,
        }
    }

    /// Stub: workload conditions not tracked in the new system.
    pub fn workload_conditions(
        &self,
        _ns_id: &str,
        _wl_id: &str,
    ) -> std::collections::BTreeMap<String, String> {
        // New system doesn't have workload conditions. Return empty.
        // Tests depending on this will fail their assertions.
        std::collections::BTreeMap::new()
    }

    /// Stub: service conditions not tracked in the new system.
    pub fn service_conditions(
        &self,
        _ns_id: &str,
        _svc_id: &str,
    ) -> std::collections::BTreeMap<String, String> {
        std::collections::BTreeMap::new()
    }

    // =========================================================================
    // Service helpers
    // =========================================================================

    pub fn service_ip(&self, ns_id: &str, svc_id: &str) -> std::net::Ipv4Addr {
        let ns = self.namespace(ns_id);
        let spec = ns
            .current_spec()
            .unwrap_or_else(|| panic!("namespace '{}' has no spec", ns_id));
        spec.services
            .get(&ServiceId::from(svc_id))
            .unwrap_or_else(|| {
                panic!(
                    "service '{}' not found in namespace '{}' spec",
                    svc_id, ns_id
                )
            })
            .ip
    }

    fn workload_for_service(&self, ns_id: &str, svc_id: &str) -> String {
        let ns = self.namespace(ns_id);
        let spec = ns.current_spec().unwrap();
        let svc_spec = spec.services.get(&ServiceId::from(svc_id)).unwrap();
        svc_spec.workload_id.0.clone()
    }

    pub fn activate_service(&mut self, ns_id: &str, svc_id: &str) {
        let namespace_id = NamespaceId::from(ns_id);
        let svc_ip = self.service_ip(ns_id, svc_id);
        let wl_name = self.workload_for_service(ns_id, svc_id);

        let worker_id = *self
            .shell
            .worker_ids()
            .next()
            .expect("no workers in harness");

        self.shell.queue_worker_event(
            worker_id,
            WorkerEvent::EndpointActivation {
                namespace_id,
                ip: svc_ip,
                service_id: Some(distvirt_worker_protocol::ServiceId::from(svc_id)),
            },
        );
        self.converge();
        self.assert_workload_running(ns_id, &wl_name);
        self.assert_service_active(ns_id, svc_id);
    }

    pub fn deactivate_service(&mut self, ns_id: &str, svc_id: &str) {
        let namespace_id = NamespaceId::from(ns_id);

        let worker_id = *self
            .shell
            .worker_ids()
            .next()
            .expect("no workers in harness");

        self.shell.queue_worker_event(
            worker_id,
            WorkerEvent::ServiceBackendNeed {
                namespace_id,
                service_id: distvirt_worker_protocol::ServiceId::from(svc_id),
                need: distvirt_worker_protocol::BackendNeed::None,
            },
        );
        self.converge();
    }

    pub fn advance_past_idle_timeout(&mut self, ns_id: &str, svc_id: &str) {
        let ns = self.namespace(ns_id);
        let spec = ns.current_spec().unwrap();
        let svc_spec = spec
            .services
            .get(&ServiceId::from(svc_id))
            .unwrap_or_else(|| {
                panic!(
                    "service '{}' not found in namespace '{}' spec",
                    svc_id, ns_id
                )
            });
        let timeout = svc_spec
            .activation
            .as_ref()
            .unwrap_or_else(|| panic!("service '{}/{}' has no activation spec", ns_id, svc_id))
            .idle_timeout;
        self.advance_time(timeout + Duration::from_secs(1));
    }

    pub fn run_activation_suspend_cycle(&mut self, ns_id: &str, svc_id: &str, wl_id: &str) {
        self.activate_service(ns_id, svc_id);
        self.deactivate_service(ns_id, svc_id);
        self.advance_past_idle_timeout(ns_id, svc_id);
        self.assert_workload_suspended(ns_id, wl_id);
    }

    pub fn run_activation_stop_cycle(&mut self, ns_id: &str, svc_id: &str, wl_id: &str) {
        self.activate_service(ns_id, svc_id);
        self.deactivate_service(ns_id, svc_id);
        self.advance_past_idle_timeout(ns_id, svc_id);
        self.assert_workload_dormant(ns_id, wl_id);
    }

    // =========================================================================
    // Event injection
    // =========================================================================

    pub fn send_event_to_workload(&self, _ns_id: &str, _wl_id: &str, _event: WorkerEvent) {
        panic!("send_event_to_workload not implemented for SyncShell harness");
    }

    pub fn send_event_to_service_worker(&self, _ns_id: &str, _svc_id: &str, _event: WorkerEvent) {
        panic!("send_event_to_service_worker not implemented for SyncShell harness");
    }

    pub fn send_pressure_update(&mut self, worker_id: &GlobalWorkerId, memory_psi_pct: f64) {
        self.shell.inject_pressure_update(
            *worker_id,
            PsiMetrics::default(),
            PsiMetrics {
                some_avg10: memory_psi_pct,
                ..Default::default()
            },
            PsiMetrics::default(),
        );
        self.converge();
    }

    // =========================================================================
    // Worker drain (not implemented in new system)
    // =========================================================================

    pub fn drain_worker(&mut self, _worker_id: &GlobalWorkerId) {
        panic!("drain_worker not implemented in SyncShell harness");
    }

    pub fn undrain_worker(&mut self, _worker_id: &GlobalWorkerId) {
        panic!("undrain_worker not implemented in SyncShell harness");
    }

    // =========================================================================
    // Legacy accessors (stubs for compilation)
    // =========================================================================

    /// Stub returning a dummy struct so scenario code accessing
    /// `h.orchestrator().workers` etc. compiles (will panic at runtime).
    pub fn orchestrator(&self) -> OrchestratorStub {
        OrchestratorStub
    }

    pub fn orchestrator_mut(&mut self) -> OrchestratorStub {
        OrchestratorStub
    }

    // =========================================================================
    // Worker command inspection
    // =========================================================================

    pub fn worker_command_count(
        &self,
        worker_id: &GlobalWorkerId,
        predicate: impl Fn(&WorkerCommand) -> bool,
    ) -> usize {
        self.shell
            .worker_commands(worker_id)
            .iter()
            .filter(|cmd| predicate(cmd))
            .count()
    }
}

// =============================================================================
// WorkerProxy — enables h.worker(&wid).send_event(...) and .commands()
// =============================================================================

pub struct WorkerProxy<'a> {
    shell: &'a SyncShell,
    worker_id: GlobalWorkerId,
    pending_events: &'a RefCell<Vec<(GlobalWorkerId, WorkerEvent)>>,
}

impl<'a> WorkerProxy<'a> {
    pub fn send_event(&self, event: WorkerEvent) {
        self.pending_events
            .borrow_mut()
            .push((self.worker_id, event));
    }

    pub fn commands(&self) -> Vec<WorkerCommand> {
        self.shell.worker_commands(&self.worker_id).to_vec()
    }
}

// =============================================================================
// OrchestratorStub — enables h.orchestrator().workers etc. to compile
// =============================================================================

pub struct OrchestratorStub;

impl OrchestratorStub {
    /// Stub: panics at runtime. Scenarios accessing .workers will fail.
    pub fn __stub_field(&self) -> ! {
        panic!("orchestrator() stub: direct field access not available in SyncShell harness")
    }
}

// Allow `h.orchestrator().workers` to compile via Deref to a type with a workers field.
// Actually, that's too complex. Instead, we'll provide a `workers` field directly.
impl OrchestratorStub {
    // This won't work for field access like `.workers[&w1]`. Instead, tests
    // that access `h.orchestrator().workers` will fail to compile — which is
    // acceptable per our policy (failing tests are expected).
}
