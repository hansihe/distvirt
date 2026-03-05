use distvirt_orchestrator::types::*;

use super::test_harness::TestHarness;

impl TestHarness {
    pub fn assert_namespace_status(&self, ns_id: &str, expected: NamespaceStatus) {
        let ns = self.namespace(ns_id);
        assert_eq!(
            ns.status, expected,
            "namespace '{}': expected status {:?}, got {:?}",
            ns_id, expected, ns.status
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

    pub fn assert_workload_waiting_for_capacity(&self, ns_id: &str, wl_id: &str) {
        let state = self.workload_state(ns_id, wl_id);
        assert!(
            matches!(state, WorkloadState::WaitingForCapacity),
            "workload '{}/{}': expected WaitingForCapacity, got {:?}",
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

    pub fn assert_workload_failed(&self, ns_id: &str, wl_id: &str) {
        let state = self.workload_state(ns_id, wl_id);
        assert!(
            matches!(state, WorkloadState::Failed),
            "workload '{}/{}': expected Failed, got {:?}",
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

    pub fn assert_service_pending(&self, ns_id: &str, svc_id: &str) {
        let state = self.service_state(ns_id, svc_id);
        assert!(
            matches!(state, ServiceState::Pending),
            "service '{}/{}': expected Pending, got {:?}",
            ns_id, svc_id, state
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

    pub fn assert_workload_launching(&self, ns_id: &str, wl_id: &str) {
        let state = self.workload_state(ns_id, wl_id);
        assert!(
            matches!(state, WorkloadState::Launching { .. }),
            "workload '{}/{}': expected Launching, got {:?}",
            ns_id, wl_id, state
        );
    }

    pub fn assert_workload_suspending(&self, ns_id: &str, wl_id: &str) {
        let state = self.workload_state(ns_id, wl_id);
        assert!(
            matches!(state, WorkloadState::Suspending { .. }),
            "workload '{}/{}': expected Suspending, got {:?}",
            ns_id, wl_id, state
        );
    }

    pub fn assert_workload_resuming(&self, ns_id: &str, wl_id: &str) {
        let state = self.workload_state(ns_id, wl_id);
        assert!(
            matches!(state, WorkloadState::Resuming { .. }),
            "workload '{}/{}': expected Resuming, got {:?}",
            ns_id, wl_id, state
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

    pub fn assert_worker_count(&self, expected: usize) {
        let actual = self.orchestrator().workers.len();
        assert_eq!(
            actual, expected,
            "expected {} workers, got {}",
            expected, actual
        );
    }

    /// Assert that a worker received at least one command matching the predicate.
    /// Panics with a list of all commands if no match is found.
    pub fn assert_worker_received_command_matching(
        &self,
        worker_id: &WorkerId,
        description: &str,
        predicate: impl Fn(&distvirt_worker_protocol::WorkerCommand) -> bool,
    ) {
        let handle = self.workers.get(worker_id)
            .unwrap_or_else(|| panic!("worker {:?} not found in harness", worker_id));
        let commands = handle.commands();
        let found = commands.iter().any(|cmd| predicate(cmd));
        assert!(
            found,
            "worker {:?}: expected command matching '{}', but none found.\nAll commands: {:#?}",
            worker_id, description, commands
        );
    }

    /// Assert that a worker did NOT receive any command matching the predicate.
    pub fn assert_worker_did_not_receive_command_matching(
        &self,
        worker_id: &WorkerId,
        description: &str,
        predicate: impl Fn(&distvirt_worker_protocol::WorkerCommand) -> bool,
    ) {
        let handle = self.workers.get(worker_id)
            .unwrap_or_else(|| panic!("worker {:?} not found in harness", worker_id));
        let commands = handle.commands();
        let matching: Vec<_> = commands.iter().filter(|cmd| predicate(cmd)).collect();
        assert!(
            matching.is_empty(),
            "worker {:?}: expected NO command matching '{}', but found: {:#?}",
            worker_id, description, matching
        );
    }
}
