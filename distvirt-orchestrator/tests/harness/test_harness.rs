use std::collections::HashMap;
use std::time::Duration;

use distvirt_orchestrator::shell::OrchestratorShell;
use distvirt_orchestrator::types::*;
use distvirt_worker_protocol::OrchestratorConnection;

use super::mock_worker::{MockWorkerConfig, MockWorkerHandle, spawn_mock_worker};

pub struct TestHarness {
    pub shell: OrchestratorShell,
    pub workers: HashMap<WorkerId, MockWorkerHandle>,
    next_client_id: u64,
}

impl TestHarness {
    pub fn new() -> Self {
        TestHarness {
            shell: OrchestratorShell::new(51820, false, vec![]),
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
}
