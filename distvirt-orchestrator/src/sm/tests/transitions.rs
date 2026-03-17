use super::*;

/// 11. Demand drops during pod launch — committed_to_boot keeps the pod alive.
///     When the pod reaches Running, demand is re-checked and workload deactivates.
#[test]
fn demand_drop_during_launch_committed_to_boot() {
    let mut router = Router::new(16);
    let (mgmt, worker, pod_id) = setup_workload_with_pending_pod(&mut router);

    // Verify committed state.
    let wl = router.get_workload(&W1).unwrap();
    assert!(wl.committed_to_boot);
    assert!(wl.has_demand);

    // Deactivate the service — demand drops to 0.
    router.send_activate_service(mgmt, S1, false);
    router.propagate();

    // Pod should still exist (committed_to_boot keeps it alive).
    let wl = router.get_workload(&W1).unwrap();
    assert!(!wl.has_demand);
    assert!(wl.committed_to_boot);
    assert!(wl.pod_id.is_some());

    // Pod reaches Running — workload checks demand, finds 0, deactivates.
    make_pod_running(&mut router, worker, pod_id);

    let wl = router.get_workload(&W1).unwrap();
    assert!(!wl.committed_to_boot); // cleared on Running
    assert!(!wl.pod_running); // deactivated — pod destroyed
    assert!(wl.pod_id.is_none());
}

/// 12. Demand drops then reappears during pod launch — no restart needed,
///     pod stays alive throughout and becomes active normally.
#[test]
fn demand_fluctuation_during_launch() {
    let mut router = Router::new(16);
    let (mgmt, worker, pod_id) = setup_workload_with_pending_pod(&mut router);

    // Demand drops.
    router.send_activate_service(mgmt, S1, false);
    router.propagate();

    let wl = router.get_workload(&W1).unwrap();
    assert!(!wl.has_demand);
    assert!(wl.committed_to_boot);
    let original_pod = wl.pod_id.unwrap();

    // Demand reappears.
    router.send_activate_service(mgmt, S1, true);
    router.propagate();

    // Same pod, still launching.
    let wl = router.get_workload(&W1).unwrap();
    assert!(wl.has_demand);
    assert_eq!(wl.pod_id, Some(original_pod));

    // Pod reaches Running — demand is back, stays active.
    make_pod_running(&mut router, worker, pod_id);

    let wl = router.get_workload(&W1).unwrap();
    assert!(wl.pod_running);
    assert!(wl.pod_id.is_some());

    // Service should be active.
    let s1 = router.get_service(&S1).unwrap();
    assert!(matches!(s1.state, ServiceState::Active { .. }));
}

/// 13. Spec changes during pod launch — detected at Running, triggers restart.
///     Replaces PendingIntent::Restart with spec version comparison.
#[test]
fn spec_change_during_launch_triggers_restart() {
    let mut router = Router::new(16);
    let (mgmt, worker, pod_id) = setup_workload_with_pending_pod(&mut router);

    let wl = router.get_workload(&W1).unwrap();
    let original_pod = wl.pod_id.unwrap();

    // Spec changes while pod is launching.
    router.set_management_wl_spec(
        mgmt,
        WorkloadSpec {
            image: "app:v2".into(),
            ..Default::default()
        },
    );
    router.propagate();

    // Pod should still exist (launch continues).
    let wl = router.get_workload(&W1).unwrap();
    assert_eq!(wl.pod_id, Some(original_pod));

    // Pod reaches Running — workload detects spec mismatch, destroys and recreates.
    make_pod_running(&mut router, worker, pod_id);

    let wl = router.get_workload(&W1).unwrap();
    // Old pod destroyed, new pod created.
    assert!(wl.pod_id.is_some());
    assert_ne!(wl.pod_id.unwrap(), original_pod);
    assert!(!wl.pod_running); // new pod is Pending
}

/// 14. Spec changes while pod is Running — immediate restart (no pending).
#[test]
fn spec_change_while_running_restarts_immediately() {
    let mut router = Router::new(16);
    let (mgmt, worker, pod_id) = setup_workload_with_pending_pod(&mut router);

    // Get pod running.
    make_pod_running(&mut router, worker, pod_id);

    let wl = router.get_workload(&W1).unwrap();
    assert!(wl.pod_running);
    let original_pod = wl.pod_id.unwrap();

    // Spec changes while running.
    router.set_management_wl_spec(
        mgmt,
        WorkloadSpec {
            image: "app:v2".into(),
            ..Default::default()
        },
    );
    router.propagate();

    // Workload should have restarted: old pod destroyed, new pod created.
    let wl = router.get_workload(&W1).unwrap();
    assert!(wl.pod_id.is_some());
    assert_ne!(wl.pod_id.unwrap(), original_pod);
    assert!(!wl.pod_running); // new pod is Pending

    // Readiness should be cleared.
    let s1 = router.get_service(&S1).unwrap();
    assert_eq!(s1.state, ServiceState::NeedBackend);
}

/// 15. Scavenge with no demand — idle workload deactivated.
#[test]
fn scavenge_idle_workload() {
    let mut router = Router::new(16);
    let (mgmt, worker, pod_id) = setup_workload_with_pending_pod(&mut router);

    // Get pod running.
    make_pod_running(&mut router, worker, pod_id);

    let wl = router.get_workload(&W1).unwrap();
    assert!(wl.pod_running);

    // Drop demand: switch service to activation-based.
    router.set_management_svc_spec(
        mgmt,
        ServiceSpec {
            workload: W1,
            has_activation: true,
            ..Default::default()
        },
    );
    router.propagate();

    // Pod was destroyed because demand dropped and on_pod_running already ran
    // (pod was Running when demand dropped → reconcile destroys it).
    // But let's test a scenario where scavenge actually matters:
    // workload with activation-based service, pod running, then service deactivates.

    // Start fresh for a clean scavenge scenario.
    let mut router = Router::new(16);
    router.create_timer(TIMER);
    let worker = WK1;
    router.create_worker(worker);
    router.set_worker_info(worker, WorkerInfo { capacity: 10 });
    router.create_schedule_request(SCHEDULE_REQUEST);

    let mgmt = router.create_management();
    router.create_workload(W1, WorkloadSm::new());
    router.set_workload_config_edges(mgmt, vec![W1]);
    router.set_management_wl_spec(
        mgmt,
        WorkloadSpec {
            image: "app:v1".into(),
            ..Default::default()
        },
    );

    // Activation-based service.
    router.create_service(S1, ServiceSm::new(true));
    router.set_service_config_edges(mgmt, vec![S1]);
    router.set_management_svc_spec(
        mgmt,
        ServiceSpec {
            workload: W1,
            has_activation: true,
            ..Default::default()
        },
    );
    router.propagate();

    // No demand yet — workload dormant.
    let wl = router.get_workload(&W1).unwrap();
    assert!(!wl.has_demand);
    assert!(wl.pod_id.is_none());

    // Activate service → demand → pod created.
    router.send_activate_service(mgmt, S1, true);
    router.propagate();

    let wl = router.get_workload(&W1).unwrap();
    assert!(wl.has_demand);
    let pod_id = wl.pod_id.unwrap();

    // Make pod running.
    make_pod_running(&mut router, worker, pod_id);

    let wl = router.get_workload(&W1).unwrap();
    assert!(wl.pod_running);

    // Service deactivates — demand drops to 0.
    router.send_activate_service(mgmt, S1, false);
    router.propagate();

    // Workload destroyed the pod in reconcile (no demand, not committed).
    let wl = router.get_workload(&W1).unwrap();
    assert!(!wl.has_demand);
    assert!(wl.pod_id.is_none());

    // Scavenge on already-idle workload is a noop.
    router.send_admin_command(mgmt, W1, AdminCmd::Scavenge);
    router.propagate();

    let wl = router.get_workload(&W1).unwrap();
    assert!(wl.pod_id.is_none());
}

/// 16. Scavenge with active demand — noop, workload stays active.
#[test]
fn scavenge_with_demand_is_noop() {
    let mut router = Router::new(16);
    let (mgmt, worker, pod_id) = setup_workload_with_pending_pod(&mut router);

    // Get pod running.
    make_pod_running(&mut router, worker, pod_id);

    let wl = router.get_workload(&W1).unwrap();
    assert!(wl.pod_running);
    assert!(wl.has_demand);
    let original_pod = wl.pod_id.unwrap();

    // Scavenge while actively demanded → noop.
    router.send_admin_command(mgmt, W1, AdminCmd::Scavenge);
    router.propagate();

    let wl = router.get_workload(&W1).unwrap();
    assert!(wl.pod_running); // still running
    assert_eq!(wl.pod_id, Some(original_pod)); // same pod
}

/// 17. Scavenge aborts a committed-to-boot launch when demand is gone.
#[test]
fn scavenge_aborts_committed_launch() {
    let mut router = Router::new(16);
    let (mgmt, _worker, _pod_id) = setup_workload_with_pending_pod(&mut router);

    // Drop demand while pod is launching.
    router.send_activate_service(mgmt, S1, false);
    router.propagate();

    // Pod still alive due to committed_to_boot.
    let wl = router.get_workload(&W1).unwrap();
    assert!(!wl.has_demand);
    assert!(wl.committed_to_boot);
    assert!(wl.pod_id.is_some());

    // Scavenge overrides committed_to_boot — pod destroyed.
    router.send_admin_command(mgmt, W1, AdminCmd::Scavenge);
    router.propagate();

    let wl = router.get_workload(&W1).unwrap();
    assert!(!wl.committed_to_boot);
    assert!(wl.pod_id.is_none());
    assert!(!wl.wants_pod);
}

/// 18. Spec change + demand drop during launch — spec change wins
///     (pod restarts on Running, then deactivates because no demand).
#[test]
fn spec_change_and_demand_drop_during_launch() {
    let mut router = Router::new(16);
    let (mgmt, worker, pod_id) = setup_workload_with_pending_pod(&mut router);

    let original_pod = router.get_workload(&W1).unwrap().pod_id.unwrap();

    // Both happen during launch: spec changes and demand drops.
    router.set_management_wl_spec(
        mgmt,
        WorkloadSpec {
            image: "app:v2".into(),
            ..Default::default()
        },
    );
    router.send_activate_service(mgmt, S1, false);
    router.propagate();

    // Pod still alive (committed_to_boot).
    let wl = router.get_workload(&W1).unwrap();
    assert!(!wl.has_demand);
    assert!(wl.committed_to_boot);
    assert_eq!(wl.pod_id, Some(original_pod));

    // Pod reaches Running — spec mismatch detected first (priority 1),
    // destroys pod and reconciles. No demand → no new pod.
    make_pod_running(&mut router, worker, pod_id);

    let wl = router.get_workload(&W1).unwrap();
    assert!(wl.pod_id.is_none()); // no new pod (no demand)
    assert!(!wl.committed_to_boot);
    assert!(!wl.wants_pod);
}

/// 19. Restart during pod launch — destroys and recreates immediately.
#[test]
fn restart_during_launch() {
    let mut router = Router::new(16);
    let (mgmt, worker, _pod_id) = setup_workload_with_pending_pod(&mut router);

    let original_pod = router.get_workload(&W1).unwrap().pod_id.unwrap();

    // Admin restart while pod is launching.
    router.send_admin_command(mgmt, W1, AdminCmd::Restart);
    router.propagate();

    // Old pod destroyed, new pod created (has spec + demand).
    let wl = router.get_workload(&W1).unwrap();
    assert!(wl.pod_id.is_some());
    assert_ne!(wl.pod_id.unwrap(), original_pod);

    // New pod should be Pending.
    let new_pod = wl.pod_id.unwrap();
    let pod = router.get_pod(&new_pod).unwrap();
    assert_eq!(pod.status, PodStatus::Pending);

    // Make new pod running — should become active normally.
    make_pod_running(&mut router, worker, new_pod);

    let wl = router.get_workload(&W1).unwrap();
    assert!(wl.pod_running);
}
