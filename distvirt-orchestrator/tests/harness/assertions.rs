use distvirt_orchestrator::core::GlobalWorkerId;
use distvirt_orchestrator::sm_new::{SvcStatus, WlStatus};
use distvirt_orchestrator::types::NamespaceId;

use super::test_harness::TestHarness;

impl TestHarness {
    pub fn assert_namespace_status(
        &self,
        ns_id: &str,
        expected: distvirt_orchestrator::types::NamespaceStatus,
    ) {
        use distvirt_orchestrator::types::NamespaceStatus;
        let ns_id_typed = distvirt_orchestrator::types::NamespaceId::from(ns_id);
        let actual = match self.shell.namespace(&ns_id_typed) {
            None => {
                panic!(
                    "namespace '{}': expected {:?} but namespace does not exist",
                    ns_id, expected
                );
            }
            Some(ns) => {
                if ns.active_workers().is_empty() {
                    NamespaceStatus::Creating
                } else {
                    NamespaceStatus::Active
                }
            }
        };
        assert_eq!(
            actual, expected,
            "namespace '{}': expected {:?}, got {:?}",
            ns_id, expected, actual
        );
    }

    pub fn assert_namespace_absent(&self, ns_id: &str) {
        assert!(
            self.shell.namespace(&NamespaceId::from(ns_id)).is_none(),
            "namespace '{}' should be absent but still exists",
            ns_id
        );
    }

    pub fn assert_workload_running(&self, ns_id: &str, wl_id: &str) {
        let status = self.workload_status(ns_id, wl_id);
        assert!(
            matches!(status, WlStatus::Running),
            "workload '{}/{}': expected Running, got {:?}",
            ns_id,
            wl_id,
            status
        );
    }

    pub fn assert_workload_dormant(&self, ns_id: &str, wl_id: &str) {
        let status = self.workload_status(ns_id, wl_id);
        assert!(
            matches!(status, WlStatus::Dormant),
            "workload '{}/{}': expected Dormant, got {:?}",
            ns_id,
            wl_id,
            status
        );
    }

    pub fn assert_workload_waiting_for_capacity(&self, ns_id: &str, wl_id: &str) {
        // In the new system, "WaitingForCapacity" maps to: has demand, has spec,
        // pod exists but is Pending (waiting for scheduler grant).
        // Or: has demand, has spec, no pod yet (scheduler hasn't granted).
        let wl = self.workload_state(ns_id, wl_id);
        let status = self.workload_status(ns_id, wl_id);
        let is_waiting = matches!(status, WlStatus::Launching)
            || (wl.wants_pod && wl.pod_id.is_none() && !wl.in_backoff);
        assert!(
            is_waiting,
            "workload '{}/{}': expected WaitingForCapacity-like state, got status {:?}, wl: {:?}",
            ns_id, wl_id, status, wl
        );
    }

    pub fn assert_workload_suspended(&self, ns_id: &str, wl_id: &str) {
        let status = self.workload_status(ns_id, wl_id);
        assert!(
            matches!(status, WlStatus::Suspended),
            "workload '{}/{}': expected Suspended, got {:?}",
            ns_id,
            wl_id,
            status
        );
    }

    pub fn assert_workload_failed(&self, ns_id: &str, wl_id: &str) {
        let status = self.workload_status(ns_id, wl_id);
        assert!(
            matches!(status, WlStatus::Failed),
            "workload '{}/{}': expected Failed, got {:?}",
            ns_id,
            wl_id,
            status
        );
    }

    pub fn assert_workload_retry_backoff(&self, ns_id: &str, wl_id: &str) {
        let status = self.workload_status(ns_id, wl_id);
        assert!(
            matches!(status, WlStatus::RetryBackoff),
            "workload '{}/{}': expected RetryBackoff, got {:?}",
            ns_id,
            wl_id,
            status
        );
    }

    pub fn assert_workload_launching(&self, ns_id: &str, wl_id: &str) {
        let status = self.workload_status(ns_id, wl_id);
        assert!(
            matches!(status, WlStatus::Launching),
            "workload '{}/{}': expected Launching, got {:?}",
            ns_id,
            wl_id,
            status
        );
    }

    pub fn assert_workload_suspending(&self, ns_id: &str, wl_id: &str) {
        let status = self.workload_status(ns_id, wl_id);
        assert!(
            matches!(status, WlStatus::Suspending),
            "workload '{}/{}': expected Suspending, got {:?}",
            ns_id,
            wl_id,
            status
        );
    }

    pub fn assert_workload_resuming(&self, ns_id: &str, wl_id: &str) {
        // In the new system, "Resuming" is: pod exists, pod is Pending,
        // and the pod has a resume_artifact. From WlStatus perspective it's Launching.
        let status = self.workload_status(ns_id, wl_id);
        assert!(
            matches!(status, WlStatus::Launching),
            "workload '{}/{}': expected Resuming (Launching with resume artifact), got {:?}",
            ns_id,
            wl_id,
            status
        );
    }

    pub fn assert_service_active(&self, ns_id: &str, svc_id: &str) {
        let status = self.service_status(ns_id, svc_id);
        assert!(
            matches!(status, SvcStatus::Active),
            "service '{}/{}': expected Active, got {:?}",
            ns_id,
            svc_id,
            status
        );
    }

    pub fn assert_service_idle(&self, ns_id: &str, svc_id: &str) {
        let status = self.service_status(ns_id, svc_id);
        assert!(
            matches!(status, SvcStatus::Idle),
            "service '{}/{}': expected Idle, got {:?}",
            ns_id,
            svc_id,
            status
        );
    }

    pub fn assert_service_need_backend(&self, ns_id: &str, svc_id: &str) {
        let status = self.service_status(ns_id, svc_id);
        assert!(
            matches!(status, SvcStatus::NeedBackend),
            "service '{}/{}': expected NeedBackend, got {:?}",
            ns_id,
            svc_id,
            status
        );
    }

    pub fn assert_worker_draining(&self, _worker_id: &GlobalWorkerId) {
        panic!("assert_worker_draining not implemented for SyncShell harness");
    }

    pub fn assert_worker_not_draining(&self, _worker_id: &GlobalWorkerId) {
        panic!("assert_worker_not_draining not implemented for SyncShell harness");
    }

    pub fn assert_worker_count(&self, expected: usize) {
        let actual = self.shell.worker_ids().count();
        assert_eq!(
            actual, expected,
            "expected {} workers, got {}",
            expected, actual
        );
    }

    pub fn assert_worker_received_command_matching(
        &self,
        worker_id: &GlobalWorkerId,
        description: &str,
        predicate: impl Fn(&distvirt_worker_protocol::WorkerCommand) -> bool,
    ) {
        let commands = self.shell.worker_commands(worker_id);
        let found = commands.iter().any(|cmd| predicate(cmd));
        assert!(
            found,
            "worker {:?}: expected command matching '{}', but none found.\nAll commands: {:#?}",
            worker_id, description, commands
        );
    }

    pub fn assert_worker_command_count(
        &self,
        worker_id: &GlobalWorkerId,
        description: &str,
        expected: usize,
        predicate: impl Fn(&distvirt_worker_protocol::WorkerCommand) -> bool,
    ) {
        let actual = self.worker_command_count(worker_id, predicate);
        assert_eq!(
            actual, expected,
            "worker {:?}: expected {} '{}' commands, got {}",
            worker_id, expected, description, actual
        );
    }

    pub fn assert_worker_did_not_receive_command_matching(
        &self,
        worker_id: &GlobalWorkerId,
        description: &str,
        predicate: impl Fn(&distvirt_worker_protocol::WorkerCommand) -> bool,
    ) {
        let commands = self.shell.worker_commands(worker_id);
        let matching: Vec<_> = commands.iter().filter(|cmd| predicate(cmd)).collect();
        assert!(
            matching.is_empty(),
            "worker {:?}: expected NO command matching '{}', but found: {:#?}",
            worker_id,
            description,
            matching
        );
    }

    pub fn assert_service_condition_set(&self, _ns_id: &str, _svc_id: &str, _key: &str) {
        panic!("assert_service_condition_set not implemented for SyncShell harness");
    }

    pub fn assert_service_condition_clear(&self, _ns_id: &str, _svc_id: &str, _key: &str) {
        panic!("assert_service_condition_clear not implemented for SyncShell harness");
    }
}
