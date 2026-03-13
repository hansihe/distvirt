use std::collections::HashMap;
use std::time::Duration;

use distvirt_orchestrator::shell::OrchestratorShell;
use distvirt_orchestrator::types::*;
use distvirt_worker_protocol::{BackendNeed, OrchestratorConnection, PsiMetrics, WorkerEvent};

use super::mock_worker::{MockWorkerConfig, MockWorkerHandle, spawn_mock_worker};

pub struct TestHarness {
    pub shell: OrchestratorShell,
    pub workers: HashMap<WorkerId, MockWorkerHandle>,
    next_client_id: u64,
}

impl TestHarness {
    pub const TEST_SECRET: &str = "test-secret";

    pub fn new() -> Self {
        TestHarness {
            shell: OrchestratorShell::new(51820, false, vec![], Self::TEST_SECRET.to_string()),
            workers: HashMap::new(),
            next_client_id: 1,
        }
    }

    /// Add a mock worker with default config, perform handshake, return worker ID.
    pub async fn add_worker(&mut self) -> WorkerId {
        self.add_worker_with(MockWorkerConfig::default()).await
    }

    /// Add a mock worker with custom config.
    pub async fn add_worker_with(&mut self, config: MockWorkerConfig) -> WorkerId {
        let (orch_half, handle) = spawn_mock_worker(config);

        let orch_conn = OrchestratorConnection::connect(orch_half)
            .await
            .expect("orchestrator connect failed");

        let worker_id = self
            .shell
            .add_worker(orch_conn)
            .await
            .expect("add_worker failed");

        self.workers.insert(worker_id.clone(), handle);
        worker_id
    }

    /// Disconnect a worker (drops transport, aborts task).
    pub fn disconnect_worker(&mut self, worker_id: &WorkerId) {
        if let Some(handle) = self.workers.remove(worker_id) {
            handle.disconnect();
        }
    }

    /// Access a worker handle for event injection.
    pub fn worker(&self, worker_id: &WorkerId) -> &MockWorkerHandle {
        self.workers
            .get(worker_id)
            .expect("worker not found")
    }

    /// Send a CreateNamespace client command.
    pub async fn create_namespace(&mut self, ns_id: &str, spec: NamespaceSpec) {
        let client_id = ClientId(self.next_client_id);
        self.next_client_id += 1;
        self.shell
            .client_command(
                client_id,
                ClientCommand::CreateNamespace {
                    namespace_id: NamespaceId::from(ns_id),
                    spec,
                },
            )
            .await;
    }

    /// Send an UpdateNamespace client command (spec change).
    pub async fn update_namespace(&mut self, ns_id: &str, spec: NamespaceSpec) {
        let client_id = ClientId(self.next_client_id);
        self.next_client_id += 1;
        self.shell
            .client_command(
                client_id,
                ClientCommand::UpdateNamespace {
                    namespace_id: NamespaceId::from(ns_id),
                    spec,
                },
            )
            .await;
    }

    /// Send a DeleteNamespace client command.
    pub async fn delete_namespace(&mut self, ns_id: &str) {
        let client_id = ClientId(self.next_client_id);
        self.next_client_id += 1;
        self.shell
            .client_command(
                client_id,
                ClientCommand::DeleteNamespace {
                    namespace_id: NamespaceId::from(ns_id),
                },
            )
            .await;
    }

    /// Converge: drain + step in a loop until quiescent. Panics after 5s.
    pub async fn converge(&mut self) {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        loop {
            self.shell.drain().await;
            // Yield to let background tasks (mock workers) process and send events.
            tokio::time::sleep(Duration::from_millis(10)).await;
            // Drain again after yield to pick up new messages.
            let had_messages = self.shell.step().await;
            if had_messages {
                // More messages arrived, keep going.
                self.shell.drain().await;
                continue;
            }
            // One more yield + check cycle to confirm quiescence.
            tokio::time::sleep(Duration::from_millis(10)).await;
            if !self.shell.step().await {
                break;
            }
            self.shell.drain().await;
            if tokio::time::Instant::now() > deadline {
                panic!("converge() timed out after 5 seconds");
            }
        }
    }

    /// Advance time by `duration` then converge. Use with `#[tokio::test(start_paused = true)]`.
    pub async fn advance_time(&mut self, duration: Duration) {
        tokio::time::advance(duration).await;
        self.converge().await;
    }

    /// Access the orchestrator state.
    pub fn orchestrator(&self) -> &distvirt_orchestrator::orchestrator::Orchestrator {
        self.shell.orchestrator()
    }

    /// Mutable access to the orchestrator state (for test setup, e.g. injecting pressure).
    pub fn orchestrator_mut(&mut self) -> &mut distvirt_orchestrator::orchestrator::Orchestrator {
        self.shell.orchestrator_mut()
    }

    /// Get namespace state machine.
    pub fn namespace(
        &self,
        ns_id: &str,
    ) -> &distvirt_orchestrator::namespace::NamespaceStateMachine {
        self.shell
            .orchestrator()
            .namespaces
            .get(&NamespaceId::from(ns_id))
            .unwrap_or_else(|| panic!("namespace '{}' not found", ns_id))
    }

    /// Get workload state.
    pub fn workload_state(&self, ns_id: &str, wl_id: &str) -> &WorkloadState {
        let ns = self.namespace(ns_id);
        let wl = ns
            .workloads
            .get(&WorkloadId(wl_id.to_string()))
            .unwrap_or_else(|| panic!("workload '{}' not found in namespace '{}'", wl_id, ns_id));
        &wl.state
    }

    /// Get service state.
    pub fn service_state(&self, ns_id: &str, svc_id: &str) -> &ServiceState {
        let ns = self.namespace(ns_id);
        let svc = ns
            .services
            .get(&ServiceId::from(svc_id))
            .unwrap_or_else(|| panic!("service '{}' not found in namespace '{}'", svc_id, ns_id));
        &svc.state
    }

    /// Get service conditions.
    pub fn service_conditions(&self, ns_id: &str, svc_id: &str) -> &std::collections::BTreeMap<String, String> {
        let ns = self.namespace(ns_id);
        let svc = ns
            .services
            .get(&ServiceId::from(svc_id))
            .unwrap_or_else(|| panic!("service '{}' not found in namespace '{}'", svc_id, ns_id));
        &svc.conditions
    }

    /// Get workload conditions.
    pub fn workload_conditions(&self, ns_id: &str, wl_id: &str) -> &std::collections::BTreeMap<String, String> {
        let ns = self.namespace(ns_id);
        let wl = ns
            .workloads
            .get(&WorkloadId(wl_id.to_string()))
            .unwrap_or_else(|| panic!("workload '{}' not found in namespace '{}'", wl_id, ns_id));
        &wl.conditions
    }

    /// Activate a service, deactivate it, advance past idle timeout, assert suspended.
    /// For activation specs with suspend_on_idle=true.
    pub async fn run_activation_suspend_cycle(&mut self, ns_id: &str, svc_id: &str, wl_id: &str) {
        self.activate_service(ns_id, svc_id).await;
        self.deactivate_service(ns_id, svc_id).await;
        self.advance_past_idle_timeout(ns_id, svc_id).await;
        self.assert_workload_suspended(ns_id, wl_id);
    }

    /// Same as run_activation_suspend_cycle but asserts dormant (for suspend_on_idle=false specs).
    pub async fn run_activation_stop_cycle(&mut self, ns_id: &str, svc_id: &str, wl_id: &str) {
        self.activate_service(ns_id, svc_id).await;
        self.deactivate_service(ns_id, svc_id).await;
        self.advance_past_idle_timeout(ns_id, svc_id).await;
        self.assert_workload_dormant(ns_id, wl_id);
    }

    /// Get the service IP from the namespace spec.
    pub fn service_ip(&self, ns_id: &str, svc_id: &str) -> std::net::Ipv4Addr {
        let ns = self.namespace(ns_id);
        ns.spec
            .services
            .get(&ServiceId::from(svc_id))
            .unwrap_or_else(|| panic!("service '{}' not found in namespace '{}' spec", svc_id, ns_id))
            .ip
    }

    /// Send an event to the worker hosting a workload. Panics if workload has no worker.
    pub fn send_event_to_workload(&self, ns_id: &str, wl_id: &str, event: WorkerEvent) {
        let worker_id = self
            .workload_state(ns_id, wl_id)
            .worker_id()
            .unwrap_or_else(|| {
                panic!(
                    "workload '{}/{}' has no worker (state: {:?})",
                    ns_id,
                    wl_id,
                    self.workload_state(ns_id, wl_id)
                )
            })
            .clone();
        self.worker(&worker_id).send_event(event);
    }

    /// Send an event to the worker hosting a service's workload.
    pub fn send_event_to_service_worker(&self, ns_id: &str, svc_id: &str, event: WorkerEvent) {
        let ns = self.namespace(ns_id);
        let svc = ns
            .services
            .get(&ServiceId::from(svc_id))
            .unwrap_or_else(|| panic!("service '{}' not found in namespace '{}'", svc_id, ns_id));
        let wl_id = svc.workload_id.0.clone();
        self.send_event_to_workload(ns_id, &wl_id, event);
    }

    /// Send a DrainWorker client command for the given worker.
    pub async fn drain_worker(&mut self, worker_id: &WorkerId) {
        let client_id = ClientId(self.next_client_id);
        self.next_client_id += 1;
        self.shell
            .client_command(
                client_id,
                ClientCommand::DrainWorker {
                    worker_id: worker_id.clone(),
                },
            )
            .await;
    }

    /// Send an UndrainWorker client command for the given worker.
    pub async fn undrain_worker(&mut self, worker_id: &WorkerId) {
        let client_id = ClientId(self.next_client_id);
        self.next_client_id += 1;
        self.shell
            .client_command(
                client_id,
                ClientCommand::UndrainWorker {
                    worker_id: worker_id.clone(),
                },
            )
            .await;
    }

    /// Send a PressureUpdate event from a worker with the given memory PSI some_avg10 value,
    /// then converge. CPU and IO PSI default to 0.
    pub async fn send_pressure_update(&mut self, worker_id: &WorkerId, memory_psi_pct: f64) {
        self.worker(worker_id).send_event(WorkerEvent::PressureUpdate {
            cpu: PsiMetrics::default(),
            memory: PsiMetrics {
                some_avg10: memory_psi_pct,
                ..Default::default()
            },
            io: PsiMetrics::default(),
        });
        self.converge().await;
    }

    /// Activate a service: send EndpointActivation with the correct IP, converge,
    /// assert workload running and service active.
    pub async fn activate_service(&mut self, ns_id: &str, svc_id: &str) {
        let svc_ip = self.service_ip(ns_id, svc_id);
        let ns = self.namespace(ns_id);
        let svc = ns
            .services
            .get(&ServiceId::from(svc_id))
            .unwrap_or_else(|| panic!("service '{}' not found in namespace '{}'", svc_id, ns_id));
        let wl_id = svc.workload_id.0.clone();

        // Find target worker: workload's worker if assigned, otherwise first worker
        // with FabricStatus::Active for this namespace.
        let worker_id = if let Some(wid) = self.workload_state(ns_id, &wl_id).worker_id() {
            wid.clone()
        } else {
            let ns = self.namespace(ns_id);
            ns.workers
                .iter()
                .find(|(_, ws)| ws.fabric_status == FabricStatus::Active)
                .map(|(wid, _)| wid.clone())
                .unwrap_or_else(|| {
                    // Fall back to first worker in harness
                    self.workers
                        .keys()
                        .next()
                        .expect("no workers in harness")
                        .clone()
                })
        };

        self.worker(&worker_id).send_event(WorkerEvent::EndpointActivation {
            namespace_id: ns_id.into(),
            ip: svc_ip,
            service_id: Some(ServiceId::from(svc_id)),
        });
        self.converge().await;
        self.assert_workload_running(ns_id, &wl_id);
        self.assert_service_active(ns_id, svc_id);
    }

    /// Deactivate a service: send ServiceBackendNeed::None to workload's worker, converge.
    /// Does NOT advance time (caller handles idle timeout if needed).
    pub async fn deactivate_service(&mut self, ns_id: &str, svc_id: &str) {
        self.send_event_to_service_worker(
            ns_id,
            svc_id,
            WorkerEvent::ServiceBackendNeed {
                namespace_id: ns_id.into(),
                service_id: ServiceId::from(svc_id),
                need: BackendNeed::None,
            },
        );
        self.converge().await;
    }

    /// Advance time past the service's configured idle timeout, then converge.
    /// Panics if service has no activation spec.
    pub async fn advance_past_idle_timeout(&mut self, ns_id: &str, svc_id: &str) {
        let ns = self.namespace(ns_id);
        let svc_spec = ns
            .spec
            .services
            .get(&ServiceId::from(svc_id))
            .unwrap_or_else(|| panic!("service '{}' not found in namespace '{}' spec", svc_id, ns_id));
        let timeout = svc_spec
            .activation
            .as_ref()
            .unwrap_or_else(|| {
                panic!(
                    "service '{}/{}' has no activation spec (needed for idle timeout)",
                    ns_id, svc_id
                )
            })
            .idle_timeout;
        self.advance_time(timeout + Duration::from_secs(1)).await;
    }
}
