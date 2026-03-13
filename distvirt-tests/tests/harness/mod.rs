//! Sim test harness for distvirt integration tests.
//!
//! All sim tests run under `#[tokio::test(flavor = "current_thread", start_paused = true)]`.
//! This gives us a single-threaded async runtime with fake time — `tokio::time::advance()`
//! moves the clock without wall-clock delay, and `tokio::task::yield_now()` yields to
//! other async tasks on that same thread.
//!
//! The `Fs` trait (`SyncFs` implementation) ensures all worker file I/O runs inline
//! on the current thread via `std::fs`, avoiding `spawn_blocking` / blocking pool
//! interactions that cause flakiness under fake time.
//!
//! Per-instance snapshot directories ensure parallel tests don't stomp each other's
//! snapshot artifacts.

pub mod spec_builders;

use std::collections::HashSet;
use std::net::Ipv4Addr;
use std::path::PathBuf;
use std::time::Duration;

use distvirt_orchestrator::shell::{EventData, OrchestratorShell};
use distvirt_orchestrator::types::*;
use distvirt_worker::image_provider::stub::StubImageProvider;
use distvirt_worker::sim_traffic::SimGatewayProvider;
use distvirt_worker::vmm::guest_sim::ContainerBehavior;
use distvirt_worker::vmm::test_vmm::TestVmm;
use distvirt_worker_protocol::{
    BackendNeed, OrchestratorConnection, WorkerConnection, WorkerEvent, WorkerId,
};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

/// Craft a TCP SYN packet wrapped in a fabric header.
///
/// Follows the pattern in `distvirt-worker/src/fabric/tests.rs` (make_tcp_frame).
pub fn craft_tcp_syn(
    src_ip: Ipv4Addr,
    dst_ip: Ipv4Addr,
    src_port: u16,
    dst_port: u16,
) -> Vec<u8> {
    use etherparse::PacketBuilder;

    let builder =
        PacketBuilder::ipv4(src_ip.octets(), dst_ip.octets(), 64).tcp(src_port, dst_port, 1000, 65535);

    let mut ip_packet = Vec::new();
    builder.write(&mut ip_packet, &[]).unwrap();

    // Set TCP SYN flag: IP header is 20 bytes, TCP flags at offset 13 within TCP header.
    ip_packet[20 + 13] = 0x02;

    distvirt_worker::packet::frame::with_fabric_header(0, 0, &ip_packet)
}

pub struct TestCluster {
    pub shell: OrchestratorShell,
    worker_handles: Vec<(WorkerId, JoinHandle<anyhow::Result<()>>)>,
    next_client_id: u64,
    gateway_provider: SimGatewayProvider,
}

impl TestCluster {
    pub fn new() -> Self {
        let _ = env_logger::try_init();
        TestCluster {
            shell: OrchestratorShell::new(0, false, vec![], "test-secret".to_string()),
            worker_handles: Vec::new(),
            next_client_id: 1,
            gateway_provider: SimGatewayProvider::new(),
        }
    }

    pub async fn add_worker(&mut self) -> WorkerId {
        self.add_worker_with(ContainerBehavior::RunUntilSignaled).await
    }

    pub async fn add_worker_with(&mut self, behavior: ContainerBehavior) -> WorkerId {
        self.add_worker_with_vmm(TestVmm::new(behavior)).await
    }

    pub async fn add_worker_with_vmm(&mut self, vmm: TestVmm) -> WorkerId {
        let image_provider = StubImageProvider;
        let gateway_provider = self.gateway_provider.clone();

        let (orch_half, worker_half) = tokio::io::duplex(64 * 1024);

        let worker_handle = tokio::spawn(async move {
            let conn = WorkerConnection::accept(worker_half).await.unwrap();
            let worker = distvirt_worker::worker::Worker::<_, _, _, distvirt_worker::SyncFs, distvirt_worker::NullResourceMonitor>::new(
                PathBuf::from("/dev/null"),
                PathBuf::from("/dev/null"),
                vmm,
                image_provider,
                None,
                String::new(),
                gateway_provider,
            );
            worker.run(conn, "test-secret".to_string()).await
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
            // Advance time slightly so any short sleeps/timeouts fire.
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

    pub fn service_state(&self, ns_id: &str, svc_id: &str) -> &ServiceState {
        let ns = self.namespace(ns_id);
        let svc = ns
            .services
            .get(&ServiceId::from(svc_id))
            .unwrap_or_else(|| {
                panic!("service '{}' not found in namespace '{}'", svc_id, ns_id)
            });
        &svc.state
    }

    pub fn service_ip(&self, ns_id: &str, svc_id: &str) -> Ipv4Addr {
        let ns = self.namespace(ns_id);
        ns.spec
            .services
            .get(&ServiceId::from(svc_id))
            .unwrap_or_else(|| {
                panic!(
                    "service spec '{}' not found in namespace '{}'",
                    svc_id, ns_id
                )
            })
            .ip
    }

    // --- Traffic / event injection ---

    /// Inject a TCP SYN packet into the fabric via the sim gateway's internet_tx channel.
    /// This exercises the full activation path: packet -> fabric -> EndpointActivation -> orchestrator.
    pub async fn send_activation_traffic(&mut self, ns_id: &str, svc_id: &str) {
        let svc_ip = self.service_ip(ns_id, svc_id);
        let packet = craft_tcp_syn(Ipv4Addr::new(1, 2, 3, 4), svc_ip, 12345, 80);
        let ns_id_key = NamespaceId::from(ns_id);
        let tx = self
            .gateway_provider
            .get(&ns_id_key)
            .unwrap_or_else(|| panic!("no traffic handle for namespace '{}'", ns_id));
        tx.send(packet).await.unwrap();
        self.converge().await;
    }

    /// Inject a ServiceBackendNeed::None event to deactivate a service.
    /// This is needed because the WASM TCP activator is not available in tests.
    pub async fn deactivate_service(&mut self, ns_id: &str, svc_id: &str, worker_id: &WorkerId) {
        self.shell.inject_worker_event(
            worker_id.clone(),
            WorkerEvent::ServiceBackendNeed {
                namespace_id: NamespaceId::from(ns_id),
                service_id: distvirt_worker_protocol::ServiceId::from(svc_id),
                need: BackendNeed::None,
            },
        );
        self.converge().await;
    }

    /// Disconnect a worker by aborting its task handle (closes the duplex, triggers WorkerDisconnected).
    pub async fn disconnect_worker(&mut self, worker_id: &WorkerId) {
        let idx = self
            .worker_handles
            .iter()
            .position(|(id, _)| id == worker_id)
            .unwrap_or_else(|| panic!("worker {:?} not found", worker_id));
        let (_, handle) = self.worker_handles.remove(idx);
        handle.abort();
        self.converge().await;
    }

    /// Advance time past a service's idle timeout and converge.
    pub async fn advance_past_idle_timeout(&mut self, ns_id: &str, svc_id: &str) {
        let ns = self.namespace(ns_id);
        let svc_spec = ns
            .spec
            .services
            .get(&ServiceId::from(svc_id))
            .unwrap_or_else(|| {
                panic!(
                    "service spec '{}' not found in namespace '{}'",
                    svc_id, ns_id
                )
            });
        let idle_timeout = svc_spec
            .activation
            .as_ref()
            .map(|a| a.idle_timeout)
            .unwrap_or(Duration::from_secs(30));
        self.advance_time(idle_timeout + Duration::from_secs(1))
            .await;
    }

    // --- Namespace mutation ---

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
        self.converge().await;
    }

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
        self.converge().await;
    }

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
        self.converge().await;
    }

    pub async fn inject_pressure(&mut self, worker_id: &WorkerId, memory_psi_avg10: f64) {
        use distvirt_worker_protocol::PsiMetrics;
        let metrics = PsiMetrics {
            some_avg10: memory_psi_avg10,
            some_avg60: memory_psi_avg10,
            full_avg10: 0.0,
            full_avg60: 0.0,
        };
        self.shell.inject_worker_event(
            worker_id.clone(),
            WorkerEvent::PressureUpdate {
                cpu: metrics.clone(),
                memory: metrics.clone(),
                io: metrics,
            },
        );
        self.converge().await;
    }

    pub fn orchestrator_mut(&mut self) -> &mut distvirt_orchestrator::orchestrator::Orchestrator {
        self.shell.orchestrator_mut()
    }

    // --- Event subscriptions ---

    /// Subscribe to all events for a namespace.
    ///
    /// Uses a large channel buffer because tests subscribe early and many events
    /// accumulate during converge cycles before `wait_for_event` drains them.
    /// The production `subscribe_events` on OrchestratorShell uses 256; we
    /// override with a direct subscription here to get a bigger buffer.
    pub fn subscribe_events(&mut self, ns_id: &str) -> mpsc::Receiver<EventData> {
        let (tx, rx) = mpsc::channel(8192);
        self.shell.subscribe_events_with_sender(
            NamespaceId::from(ns_id),
            HashSet::new(),
            HashSet::new(),
            tx,
        );
        rx
    }

    /// Drive system forward until predicate matches a received event.
    /// Each round: drain + yield + advance 1ms + step.
    /// Default 5000 rounds — generous to tolerate blocking-pool contention
    /// when many tests run in parallel (spawn_blocking shares a thread pool).
    pub async fn wait_for_event(
        &mut self,
        rx: &mut mpsc::Receiver<EventData>,
        predicate: impl Fn(&SmNamespaceEvent) -> bool,
    ) {
        self.wait_for_event_rounds(rx, 5000, predicate).await;
    }

    pub async fn wait_for_event_rounds(
        &mut self,
        rx: &mut mpsc::Receiver<EventData>,
        max_rounds: usize,
        predicate: impl Fn(&SmNamespaceEvent) -> bool,
    ) {
        for _ in 0..max_rounds {
            while let Ok(data) = rx.try_recv() {
                if predicate(&data.event) {
                    return;
                }
            }
            self.shell.drain().await;
            for _ in 0..10 {
                tokio::task::yield_now().await;
            }
            tokio::time::advance(Duration::from_millis(1)).await;
            self.shell.step().await;
        }
        panic!("wait_for_event: expected event not received after {max_rounds} rounds");
    }

    // --- Assertions ---

    pub fn assert_workload_running(&self, ns_id: &str, wl_id: &str) {
        let state = self.workload_state(ns_id, wl_id);
        assert!(
            state.is_running(),
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

    pub fn assert_workload_suspended(&self, ns_id: &str, wl_id: &str) {
        let state = self.workload_state(ns_id, wl_id);
        assert!(
            matches!(state, WorkloadState::Suspended { .. }),
            "workload '{}/{}': expected Suspended, got {:?}",
            ns_id, wl_id, state
        );
    }

    /// Wait for a workload to reach Suspended state, handling the Suspending → Suspended
    /// transition which involves async I/O (snapshot writes) on the blocking pool.
    pub async fn wait_workload_suspended(&mut self, ns_id: &str, wl_id: &str) {
        for _ in 0..50 {
            let state = self.workload_state(ns_id, wl_id);
            if matches!(state, WorkloadState::Suspended { .. }) {
                return;
            }
            assert!(
                matches!(
                    state,
                    WorkloadState::Active { pod: PodSlot { pod_state: PodState::Suspending { .. }, .. }, .. }
                    | WorkloadState::Suspended { .. }
                ),
                "workload '{}/{}': expected Suspending or Suspended, got {:?}",
                ns_id, wl_id, state
            );
            // Give the blocking pool real wall-clock time to complete I/O,
            // then converge to process the resulting events.
            tokio::task::yield_now().await;
            self.converge().await;
        }
        panic!(
            "workload '{}/{}' did not reach Suspended after retries (still {:?})",
            ns_id, wl_id, self.workload_state(ns_id, wl_id)
        );
    }

    pub fn assert_workload_waiting_for_capacity(&self, ns_id: &str, wl_id: &str) {
        let state = self.workload_state(ns_id, wl_id);
        assert!(
            matches!(state, WorkloadState::WaitingForCapacity),
            "workload '{}/{}': expected WaitingForCapacity, got {:?}",
            ns_id, wl_id, state
        );
    }

    pub fn assert_workload_retry_backoff(&self, ns_id: &str, wl_id: &str) {
        let state = self.workload_state(ns_id, wl_id);
        assert!(
            matches!(state, WorkloadState::RetryBackoff { .. }),
            "workload '{}/{}': expected RetryBackoff, got {:?}",
            ns_id, wl_id, state
        );
    }

    pub fn assert_service_active(&self, ns_id: &str, svc_id: &str) {
        let state = self.service_state(ns_id, svc_id);
        assert!(
            matches!(state, ServiceState::Active { .. }),
            "service '{}/{}': expected Active, got {:?}",
            ns_id, svc_id, state
        );
    }

    pub fn assert_service_idle(&self, ns_id: &str, svc_id: &str) {
        let state = self.service_state(ns_id, svc_id);
        assert!(
            matches!(state, ServiceState::Idle),
            "service '{}/{}': expected Idle, got {:?}",
            ns_id, svc_id, state
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

    pub fn assert_workload_failed(&self, ns_id: &str, wl_id: &str) {
        let state = self.workload_state(ns_id, wl_id);
        assert!(
            matches!(state, WorkloadState::Failed),
            "workload '{}/{}': expected Failed, got {:?}",
            ns_id, wl_id, state
        );
    }

    pub fn assert_service_pending(&self, ns_id: &str, svc_id: &str) {
        let state = self.service_state(ns_id, svc_id);
        assert!(
            matches!(state, ServiceState::Pending),
            "service '{}/{}': expected Pending, got {:?}",
            ns_id, svc_id, state
        );
    }

    pub fn assert_service_need_backend(&self, ns_id: &str, svc_id: &str) {
        let state = self.service_state(ns_id, svc_id);
        assert!(
            matches!(state, ServiceState::NeedBackend),
            "service '{}/{}': expected NeedBackend, got {:?}",
            ns_id, svc_id, state
        );
    }

    pub fn worker_id_for_workload(&self, ns_id: &str, wl_id: &str) -> WorkerId {
        self.workload_state(ns_id, wl_id)
            .worker_id()
            .unwrap_or_else(|| {
                panic!(
                    "workload '{}/{}' has no worker_id (state: {:?})",
                    ns_id,
                    wl_id,
                    self.workload_state(ns_id, wl_id)
                )
            })
            .clone()
    }
}
