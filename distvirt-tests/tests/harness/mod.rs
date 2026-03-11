pub mod spec_builders;

use std::path::PathBuf;
use std::time::Duration;

use distvirt_orchestrator::shell::OrchestratorShell;
use distvirt_orchestrator::types::*;
use distvirt_worker::image_provider::stub::StubImageProvider;
use distvirt_worker::vmm::guest_sim::ContainerBehavior;
use distvirt_worker::vmm::test_vmm::TestVmm;
use distvirt_worker_protocol::{OrchestratorConnection, WorkerConnection, WorkerId};
use tokio::task::JoinHandle;

pub struct TestCluster {
    pub shell: OrchestratorShell,
    worker_handles: Vec<(WorkerId, JoinHandle<anyhow::Result<()>>)>,
    next_client_id: u64,
}

impl TestCluster {
    pub fn new() -> Self {
        let _ = env_logger::try_init();
        TestCluster {
            shell: OrchestratorShell::new(0, false, vec![]),
            worker_handles: Vec::new(),
            next_client_id: 1,
        }
    }

    pub async fn add_worker(&mut self) -> WorkerId {
        self.add_worker_with(ContainerBehavior::RunUntilSignaled).await
    }

    pub async fn add_worker_with(&mut self, behavior: ContainerBehavior) -> WorkerId {
        let vmm = TestVmm::new(behavior);
        let image_provider = StubImageProvider;

        let (orch_half, worker_half) = tokio::io::duplex(64 * 1024);

        let worker_handle = tokio::spawn(async move {
            let conn = WorkerConnection::accept(worker_half).await.unwrap();
            let worker = distvirt_worker::worker::Worker::new(
                PathBuf::from("/dev/null"),
                PathBuf::from("/dev/null"),
                vmm,
                image_provider,
                None,
                String::new(),
            )
            .with_sim_gateway();
            worker.run(conn).await
        });

        let orch_conn = OrchestratorConnection::connect(orch_half)
            .await
            .expect("orchestrator connect failed");

        let worker_id = self
            .shell
            .add_worker(orch_conn)
            .await
            .expect("add_worker failed");

        self.worker_handles.push((worker_id.clone(), worker_handle));
        worker_id
    }

    /// Converge: drain + step in a loop until quiescent.
    ///
    /// With real workers (not mocks), the full command cascade requires many
    /// cooperative yield points. We yield generously and require several
    /// consecutive quiet rounds before declaring quiescence.
    pub async fn converge(&mut self) {
        let max_rounds = 500;
        let quiet_rounds_needed = 5;
        let mut quiet_rounds = 0;

        for _ in 0..max_rounds {
            self.shell.drain().await;

            // Yield multiple times to let worker tasks make progress.
            for _ in 0..10 {
                tokio::task::yield_now().await;
            }
            // Also advance time slightly so any short sleeps/timeouts fire.
            tokio::time::advance(Duration::from_millis(1)).await;

            let had_messages = self.shell.step().await;
            if had_messages {
                quiet_rounds = 0;
                continue;
            }

            quiet_rounds += 1;
            if quiet_rounds >= quiet_rounds_needed {
                return;
            }
        }
        panic!("converge() did not reach quiescence after {max_rounds} rounds");
    }

    pub async fn advance_time(&mut self, duration: Duration) {
        tokio::time::advance(duration).await;
        self.converge().await;
    }

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

    pub fn orchestrator(&self) -> &distvirt_orchestrator::orchestrator::Orchestrator {
        self.shell.orchestrator()
    }

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

    pub fn workload_state(&self, ns_id: &str, wl_id: &str) -> &WorkloadState {
        let ns = self.namespace(ns_id);
        let wl = ns
            .workloads
            .get(&WorkloadId(wl_id.to_string()))
            .unwrap_or_else(|| panic!("workload '{}' not found in namespace '{}'", wl_id, ns_id));
        &wl.state
    }

    // --- Assertions ---

    pub fn assert_workload_running(&self, ns_id: &str, wl_id: &str) {
        let state = self.workload_state(ns_id, wl_id);
        assert!(
            matches!(state, WorkloadState::Running { .. }),
            "workload '{}/{}': expected Running, got {:?}",
            ns_id, wl_id, state
        );
    }

    pub fn assert_workload_dormant(&self, ns_id: &str, wl_id: &str) {
        let state = self.workload_state(ns_id, wl_id);
        assert!(
            matches!(state, WorkloadState::Dormant),
            "workload '{}/{}': expected Dormant, got {:?}",
            ns_id, wl_id, state
        );
    }

    pub fn assert_namespace_absent(&self, ns_id: &str) {
        let orch = self.orchestrator();
        assert!(
            !orch.namespaces.contains_key(&NamespaceId::from(ns_id)),
            "namespace '{}' should be absent but still exists",
            ns_id
        );
    }
}
