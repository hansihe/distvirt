//! Integration test harness for distvirt.
//!
//! All integration tests run under `#[tokio::test(flavor = "current_thread", start_paused = true)]`.
//! This gives us a single-threaded async runtime with fake time — `tokio::time::advance()`
//! moves the clock without wall-clock delay, and `tokio::task::yield_now()` yields to
//! other async tasks on that same thread.
//!
//! The harness uses the async shell (`ShellHandle`) with real workers connected via
//! duplex channels. Convergence is detected via the shell's activity counter — we
//! yield to let workers and shell run, then check if the activity counter stopped
//! incrementing.

#[allow(dead_code)]
pub mod spec_builders;

use std::collections::BTreeMap;
use std::net::Ipv4Addr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use distvirt_common::ActivityTracker;
use distvirt_orchestrator::adapter::timer::TimerConfig;
use distvirt_orchestrator::core::EndpointDemandSignal;
use distvirt_orchestrator::core::GlobalWorkerId;
use distvirt_orchestrator::core::WorkerNamespaceEventKind;
use distvirt_orchestrator::core::types::WorkerStateCoreEvent;
use distvirt_orchestrator::event_bus::EventBusHandle;
use distvirt_orchestrator::id_registry::IdRegistryMap;
use distvirt_orchestrator::shell::r#async::{self, ShellHandle};
use distvirt_orchestrator::types::*;
use distvirt_worker::image_provider::stub::StubImageProvider;
use distvirt_worker::sim_traffic::SimGatewayProvider;
use distvirt_worker::vmm::guest_sim::ContainerBehavior;
use distvirt_worker::vmm::test_vmm::TestVmm;
use distvirt_worker_protocol::{OrchestratorConnection, WorkerConnection};
use tokio::task::JoinHandle;

/// Craft a TCP SYN packet wrapped in a fabric header.
///
/// Follows the pattern in `distvirt-worker/src/fabric/tests.rs` (make_tcp_frame).
pub fn craft_tcp_syn(src_ip: Ipv4Addr, dst_ip: Ipv4Addr, src_port: u16, dst_port: u16) -> Vec<u8> {
    use etherparse::PacketBuilder;

    let builder = PacketBuilder::ipv4(src_ip.octets(), dst_ip.octets(), 64)
        .tcp(src_port, dst_port, 1000, 65535);

    let mut ip_packet = Vec::new();
    builder.write(&mut ip_packet, &[]).unwrap();

    // Set TCP SYN flag: IP header is 20 bytes, TCP flags at offset 13 within TCP header.
    ip_packet[20 + 13] = 0x02;

    distvirt_worker::packet::frame::with_fabric_header(0, 0, &ip_packet)
}

fn test_timer_config() -> TimerConfig {
    TimerConfig {
        retry_backoff: Duration::from_secs(5),
        launch_timeout: Duration::from_secs(60),
        suspend_timeout: Duration::from_secs(60),
        idle_timeout: Duration::from_secs(30),
    }
}

pub struct TestCluster {
    pub shell: ShellHandle,
    pub event_bus: EventBusHandle,
    pub id_registry_map: IdRegistryMap,
    activity: Arc<ActivityTracker>,
    _shell_task: JoinHandle<()>,
    worker_handles: Vec<(GlobalWorkerId, JoinHandle<anyhow::Result<()>>)>,
    gateway_provider: SimGatewayProvider,
    /// Cache of namespace specs for idle_timeout lookups.
    specs: BTreeMap<String, NamespaceSpec>,
    /// Cache of allocated IPs for service_ip lookups.
    allocs: BTreeMap<String, IpAllocResult>,
}

impl TestCluster {
    pub fn new() -> Self {
        let _ = env_logger::try_init();
        let activity = Arc::new(ActivityTracker::new());
        let (shell, _log_bus, event_bus, id_registry_map, shell_task) = r#async::spawn("test-secret".to_string(), test_timer_config(), true, 51820, Arc::clone(&activity));
        TestCluster {
            shell,
            event_bus,
            id_registry_map,
            activity,
            _shell_task: shell_task,
            worker_handles: Vec::new(),
            gateway_provider: SimGatewayProvider::new(),
            specs: BTreeMap::new(),
            allocs: BTreeMap::new(),
        }
    }

    // -------------------------------------------------------------------------
    // Worker management
    // -------------------------------------------------------------------------

    pub async fn add_worker(&mut self) -> GlobalWorkerId {
        self.add_worker_with(ContainerBehavior::RunUntilSignaled)
            .await
    }

    pub async fn add_worker_with(&mut self, behavior: ContainerBehavior) -> GlobalWorkerId {
        self.add_worker_with_vmm(TestVmm::new(behavior)).await
    }

    pub async fn add_worker_with_vmm(&mut self, vmm: TestVmm) -> GlobalWorkerId {
        let image_provider = StubImageProvider;
        let gateway_provider = self.gateway_provider.clone();

        // Record workers before connection to find the new one.
        let before: Vec<GlobalWorkerId> = self
            .shell
            .list_workers()
            .await
            .unwrap_or_default()
            .iter()
            .map(|w| w.worker_id)
            .collect();

        let (orch_half, worker_half) = tokio::io::duplex(64 * 1024);

        let activity = Arc::clone(&self.activity);

        let worker_handle = tokio::spawn(async move {
            let conn = WorkerConnection::accept(worker_half).await.unwrap();
            let worker = distvirt_worker::worker::Worker::<
                _,
                _,
                _,
                distvirt_worker::SyncFs,
                distvirt_worker::NullResourceMonitor,
            >::new(
                PathBuf::from("/dev/null"),
                PathBuf::from("/dev/null"),
                vmm,
                image_provider,
                None,
                String::new(),
                gateway_provider,
                activity,
            );
            worker.run(conn, "test-secret".to_string()).await
        });

        let orch_conn = OrchestratorConnection::connect(orch_half)
            .await
            .expect("orchestrator connect failed");

        self.shell.worker_connection(orch_conn);

        // Let the handshake complete.
        self.converge().await;

        // Find the new worker ID.
        let after: Vec<GlobalWorkerId> = self
            .shell
            .list_workers()
            .await
            .unwrap_or_default()
            .iter()
            .map(|w| w.worker_id)
            .collect();

        let worker_id = after
            .iter()
            .find(|id| !before.contains(id))
            .copied()
            .expect("no new worker found after connection");

        self.worker_handles.push((worker_id, worker_handle));
        worker_id
    }

    /// Disconnect a worker by aborting its task handle (closes the duplex, triggers WorkerDisconnected).
    pub async fn disconnect_worker(&mut self, worker_id: &GlobalWorkerId) {
        let idx = self
            .worker_handles
            .iter()
            .position(|(id, _)| id == worker_id)
            .unwrap_or_else(|| panic!("worker {:?} not found", worker_id));
        let (_, handle) = self.worker_handles.remove(idx);
        handle.abort();
        self.converge().await;
    }

    // -------------------------------------------------------------------------
    // Convergence
    // -------------------------------------------------------------------------

    /// Converge: yield to let workers + shell run, using the activity counter
    /// to detect quiescence.
    pub async fn converge(&mut self) {
        let max_rounds = 500;
        let quiet_rounds_needed = 5;
        let mut quiet_rounds = 0;

        for _ in 0..max_rounds {
            let before = self.activity.activity_count();

            // Yield multiple times to let worker tasks make progress.
            for _ in 0..10 {
                tokio::task::yield_now().await;
            }
            // Advance time slightly so any short sleeps/timeouts fire.
            tokio::time::advance(Duration::from_millis(1)).await;
            // Yield again to let shell process timer events.
            for _ in 0..10 {
                tokio::task::yield_now().await;
            }

            let after = self.activity.activity_count();
            if before == after && !self.activity.is_busy() {
                quiet_rounds += 1;
                if quiet_rounds >= quiet_rounds_needed {
                    return;
                }
            } else {
                quiet_rounds = 0;
            }
        }
        panic!("converge() did not reach quiescence after {max_rounds} rounds");
    }

    pub async fn advance_time(&mut self, duration: Duration) {
        tokio::time::advance(duration).await;
        self.converge().await;
    }

    // -------------------------------------------------------------------------
    // Namespace lifecycle
    // -------------------------------------------------------------------------

    pub async fn create_namespace(&mut self, ns_id: &str, spec: NamespaceSpec) {
        self.specs.insert(ns_id.to_string(), spec.clone());
        self.shell
            .create_namespace(NamespaceId::from(ns_id), spec.network.clone())
            .await
            .expect("create_namespace failed");
        let input = NamespaceSpecInput::from_resolved(&spec);
        let alloc = self.shell
            .update_namespace(NamespaceId::from(ns_id), input)
            .await
            .expect("update_namespace (initial spec) failed");
        self.allocs.insert(ns_id.to_string(), alloc);
    }

    pub async fn delete_namespace(&mut self, ns_id: &str) {
        self.specs.remove(ns_id);
        self.shell
            .destroy_namespace(NamespaceId::from(ns_id))
            .await
            .expect("destroy_namespace failed");
    }

    pub async fn update_namespace(&mut self, ns_id: &str, spec: NamespaceSpec) {
        self.specs.insert(ns_id.to_string(), spec.clone());
        let input = NamespaceSpecInput::from_resolved(&spec);
        let alloc = self.shell
            .update_namespace(NamespaceId::from(ns_id), input)
            .await
            .expect("update_namespace failed");
        self.allocs.insert(ns_id.to_string(), alloc);
        self.converge().await;
    }

    // -------------------------------------------------------------------------
    // State queries (async — go through shell channel)
    // -------------------------------------------------------------------------

    pub async fn namespace_status(&self, ns_id: &str) -> NamespaceStatusReport {
        self.shell
            .get_namespace_status(NamespaceId::from(ns_id))
            .await
            .unwrap_or_else(|e| panic!("namespace '{}' status failed: {:?}", ns_id, e))
    }

    pub async fn workload_status(&self, ns_id: &str, wl_id: &str) -> WorkloadStatus {
        let status = self.namespace_status(ns_id).await;
        status
            .workloads
            .get(&WorkloadName(wl_id.to_string()))
            .unwrap_or_else(|| {
                panic!(
                    "workload '{}' not found in namespace '{}' (have: {:?})",
                    wl_id,
                    ns_id,
                    status.workloads.keys().collect::<Vec<_>>()
                )
            })
            .state
            .clone()
    }

    pub async fn service_status(&self, ns_id: &str, svc_id: &str) -> ServiceStatus {
        let status = self.namespace_status(ns_id).await;
        status
            .services
            .get(svc_id)
            .unwrap_or_else(|| {
                panic!(
                    "service '{}' not found in namespace '{}' (have: {:?})",
                    svc_id,
                    ns_id,
                    status.services.keys().collect::<Vec<_>>()
                )
            })
            .service_state
            .clone()
    }

    /// Look up the service IP from the cached allocation result.
    pub fn service_ip(&self, ns_id: &str, svc_id: &str) -> Ipv4Addr {
        let alloc = self
            .allocs
            .get(ns_id)
            .unwrap_or_else(|| panic!("no cached alloc for namespace '{}'", ns_id));
        alloc.service_ips
            .get(svc_id)
            .unwrap_or_else(|| {
                panic!(
                    "service '{}' not found in allocation for namespace '{}'",
                    svc_id, ns_id
                )
            })
            .ip
    }

    /// Get the worker hosting a workload, from the pod status reports.
    pub async fn worker_id_for_workload(&self, ns_id: &str, wl_id: &str) -> GlobalWorkerId {
        let status = self.namespace_status(ns_id).await;
        let wl = status
            .workloads
            .get(&WorkloadName(wl_id.to_string()))
            .unwrap_or_else(|| panic!("workload '{}' not found in namespace '{}'", wl_id, ns_id));
        let pod_id = wl.pod_id.as_ref().unwrap_or_else(|| {
            panic!(
                "workload '{}/{}' has no pod_id (state: {})",
                ns_id, wl_id, wl.state
            )
        });
        let pod = status
            .pods
            .get(pod_id)
            .unwrap_or_else(|| panic!("pod {:?} not found in namespace '{}'", pod_id, ns_id));
        // GlobalWorkerId is a type alias for distvirt_worker_protocol::WorkerId.
        distvirt_worker_protocol::WorkerId(pod.worker_id.0)
    }

    // -------------------------------------------------------------------------
    // Traffic / event injection
    // -------------------------------------------------------------------------

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

    /// Deactivate a service by injecting an EndpointDemand { active: false } event.
    pub async fn deactivate_service(
        &mut self,
        ns_id: &str,
        svc_id: &str,
        worker_id: &GlobalWorkerId,
    ) {
        let svc_ip = self.service_ip(ns_id, svc_id);
        self.shell
            .inject_namespace_event(
                NamespaceId::from(ns_id),
                *worker_id,
                WorkerNamespaceEventKind::EndpointDemand {
                    ip: svc_ip,
                    service_id: None,
                    signal: EndpointDemandSignal::Active { active: false },
                },
            )
            .await;
        self.converge().await;
    }

    /// Inject memory pressure on a worker.
    pub async fn inject_pressure(&mut self, worker_id: &GlobalWorkerId, memory_psi_avg10: f64) {
        use distvirt_worker_protocol::PsiMetrics;
        let metrics = PsiMetrics {
            some_avg10: memory_psi_avg10,
            some_avg60: memory_psi_avg10,
            full_avg10: 0.0,
            full_avg60: 0.0,
        };
        self.shell
            .inject_worker_state_event(WorkerStateCoreEvent::PressureUpdate {
                worker_id: *worker_id,
                cpu: metrics.clone(),
                memory: metrics.clone(),
                io: metrics,
            })
            .await;
        self.converge().await;
    }

    pub async fn drain_worker(&mut self, _worker_id: &GlobalWorkerId) {
        todo!("drain_worker not yet implemented on async shell")
    }

    pub async fn undrain_worker(&mut self, _worker_id: &GlobalWorkerId) {
        todo!("undrain_worker not yet implemented on async shell")
    }

    /// Advance time past a service's idle timeout and converge.
    pub async fn advance_past_idle_timeout(&mut self, ns_id: &str, svc_id: &str) {
        let spec = self
            .specs
            .get(ns_id)
            .unwrap_or_else(|| panic!("no cached spec for namespace '{}'", ns_id));
        let svc_spec = spec.services.get(svc_id).unwrap_or_else(|| {
            panic!(
                "service spec '{}' not found in namespace '{}'",
                svc_id, ns_id
            )
        });
        let idle_timeout = if svc_spec.has_activation && svc_spec.idle_timeout > Duration::ZERO {
            svc_spec.idle_timeout
        } else {
            Duration::from_secs(30)
        };
        self.advance_time(idle_timeout + Duration::from_secs(1))
            .await;
    }

    // -------------------------------------------------------------------------
    // Assertions (all async — query state via shell)
    // -------------------------------------------------------------------------

    pub async fn assert_workload_running(&self, ns_id: &str, wl_id: &str) {
        let state = self.workload_status(ns_id, wl_id).await;
        assert_eq!(
            state, WorkloadStatus::Running,
            "workload '{}/{}': expected running, got {}",
            ns_id, wl_id, state
        );
    }

    pub async fn assert_workload_dormant(&self, ns_id: &str, wl_id: &str) {
        let state = self.workload_status(ns_id, wl_id).await;
        assert_eq!(
            state, WorkloadStatus::Dormant,
            "workload '{}/{}': expected dormant, got {}",
            ns_id, wl_id, state
        );
    }

    pub async fn assert_workload_suspended(&self, ns_id: &str, wl_id: &str) {
        let state = self.workload_status(ns_id, wl_id).await;
        assert_eq!(
            state, WorkloadStatus::Suspended,
            "workload '{}/{}': expected suspended, got {}",
            ns_id, wl_id, state
        );
    }

    #[allow(dead_code)]
    pub async fn assert_workload_failed(&self, ns_id: &str, wl_id: &str) {
        let state = self.workload_status(ns_id, wl_id).await;
        assert!(
            matches!(state, WorkloadStatus::Failed { .. }),
            "workload '{}/{}': expected failed, got {}",
            ns_id, wl_id, state
        );
    }

    pub async fn assert_workload_retry_backoff(&self, ns_id: &str, wl_id: &str) {
        let state = self.workload_status(ns_id, wl_id).await;
        assert_eq!(
            state, WorkloadStatus::RetryBackoff,
            "workload '{}/{}': expected retry_backoff, got {}",
            ns_id, wl_id, state
        );
    }

    pub async fn assert_workload_waiting_for_capacity(&self, ns_id: &str, wl_id: &str) {
        // In the new SM, "waiting for capacity" maps to "launching" (pod created,
        // waiting for scheduler lease) or potentially "waiting_for_spec".
        // Check for launching as the closest equivalent.
        let state = self.workload_status(ns_id, wl_id).await;
        assert_eq!(
            state, WorkloadStatus::Launching,
            "workload '{}/{}': expected launching (waiting for capacity), got {}",
            ns_id, wl_id, state
        );
    }

    #[allow(dead_code)]
    pub async fn assert_workload_not_running(&self, ns_id: &str, wl_id: &str) {
        let state = self.workload_status(ns_id, wl_id).await;
        assert_ne!(
            state, WorkloadStatus::Running,
            "workload '{}/{}': expected NOT running, got {}",
            ns_id, wl_id, state
        );
    }

    pub async fn assert_service_active(&self, ns_id: &str, svc_id: &str) {
        let state = self.service_status(ns_id, svc_id).await;
        assert_eq!(
            state, ServiceStatus::Active,
            "service '{}/{}': expected active, got {}",
            ns_id, svc_id, state
        );
    }

    pub async fn assert_service_idle(&self, ns_id: &str, svc_id: &str) {
        let state = self.service_status(ns_id, svc_id).await;
        assert_eq!(
            state, ServiceStatus::Idle,
            "service '{}/{}': expected idle, got {}",
            ns_id, svc_id, state
        );
    }

    #[allow(dead_code)]
    pub async fn assert_service_need_backend(&self, ns_id: &str, svc_id: &str) {
        let state = self.service_status(ns_id, svc_id).await;
        assert_eq!(
            state, ServiceStatus::NeedBackend,
            "service '{}/{}': expected need_backend, got {}",
            ns_id, svc_id, state
        );
    }

    pub async fn assert_namespace_absent(&self, ns_id: &str) {
        let result = self
            .shell
            .get_namespace_status(NamespaceId::from(ns_id))
            .await;
        assert!(
            result.is_err(),
            "namespace '{}' should be absent but still exists",
            ns_id
        );
    }

    /// Wait for a workload to reach Suspended state, handling the Suspending → Suspended
    /// transition which involves async I/O (snapshot writes).
    pub async fn wait_workload_suspended(&mut self, ns_id: &str, wl_id: &str) {
        for _ in 0..50 {
            let state = self.workload_status(ns_id, wl_id).await;
            if state == WorkloadStatus::Suspended {
                return;
            }
            assert!(
                state == WorkloadStatus::Suspending
                    || state == WorkloadStatus::Suspended
                    || state == WorkloadStatus::Running,
                "workload '{}/{}': expected suspending/suspended/running, got {}",
                ns_id,
                wl_id,
                state
            );
            tokio::task::yield_now().await;
            self.converge().await;
        }
        let final_state = self.workload_status(ns_id, wl_id).await;
        panic!(
            "workload '{}/{}' did not reach suspended after retries (still {})",
            ns_id, wl_id, final_state
        );
    }
}
