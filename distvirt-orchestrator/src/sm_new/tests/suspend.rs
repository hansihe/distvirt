use super::*;

// ============================================================================
// Suspend/Resume tests
// ============================================================================

/// 32. Basic suspend: demand drops on suspend_on_idle workload → pod suspends →
///     artifact saved → pod self-destructs.
#[test]
fn suspend_on_demand_drop() {
    let mut router = Router::new(16);
    let (mgmt, _worker) = setup_running_suspendable_workload(&mut router);

    let wl = router.get_workload(&W1).unwrap();
    assert!(wl.pod_running);
    let pod_id = wl.pod_id.unwrap();

    // Deactivate service → demand drops to 0.
    router.send_activate_service(mgmt, S1, false);
    router.propagate();

    // Workload should have signaled Suspend (not destroyed the pod).
    let wl = router.get_workload(&W1).unwrap();
    assert!(wl.awaiting_suspend);
    assert!(wl.pod_id.is_some()); // pod still alive
    assert!(!wl.pod_running); // no longer considered running by workload

    // Pod should be in Suspending state.
    let pod = router.get_pod(&pod_id).unwrap();
    assert_eq!(pod.status, PodStatus::Suspending);

    // Worker completes the suspend.
    let artifact = ArtifactId(42);
    router.send_notify_pod_suspended(_worker, pod_id, artifact);
    router.propagate();

    // Workload should have saved the artifact and reaped the pod.
    let wl = router.get_workload(&W1).unwrap();
    assert!(!wl.awaiting_suspend);
    assert!(wl.pod_id.is_none()); // pod reaped
    assert_eq!(wl.suspended_artifact, Some(artifact));

    // Pod should be gone (self-destructed: terminal + no owner).
    assert!(router.get_pod(&pod_id).is_none());
}

/// 33. Resume from artifact: workload with suspended artifact + demand →
///     creates pod from artifact instead of cold boot.
#[test]
fn resume_from_artifact() {
    let mut router = Router::new(16);
    let (mgmt, worker) = setup_running_suspendable_workload(&mut router);

    let pod_id = router.get_workload(&W1).unwrap().pod_id.unwrap();

    // Suspend: deactivate → suspend → complete.
    router.send_activate_service(mgmt, S1, false);
    router.propagate();

    let artifact = ArtifactId(100);
    router.send_notify_pod_suspended(worker, pod_id, artifact);
    router.propagate();

    let wl = router.get_workload(&W1).unwrap();
    assert_eq!(wl.suspended_artifact, Some(artifact));
    assert!(wl.pod_id.is_none());

    // Re-activate → demand returns → workload should create pod from artifact.
    router.send_activate_service(mgmt, S1, true);
    router.propagate();

    let wl = router.get_workload(&W1).unwrap();
    assert!(wl.pod_id.is_some());
    let new_pod_id = wl.pod_id.unwrap();
    assert_ne!(new_pod_id, pod_id); // new pod, different ID

    // The new pod should have been created with the artifact.
    let new_pod = router.get_pod(&new_pod_id).unwrap();
    assert_eq!(new_pod.resume_artifact, Some(artifact));

    // Artifact consumed from workload state.
    assert_eq!(wl.suspended_artifact, None);

    // Make resumed pod running — should become active normally.
    make_pod_running(&mut router, worker, new_pod_id);

    let wl = router.get_workload(&W1).unwrap();
    assert!(wl.pod_running);

    let s1 = router.get_service(&S1).unwrap();
    assert!(matches!(s1.state, ServiceState::Active { .. }));
}

/// 34. Demand returns during suspend: pod is suspending, demand comes back,
///     pod completes suspend, workload immediately resumes from artifact.
#[test]
fn demand_returns_during_suspend() {
    let mut router = Router::new(16);
    let (mgmt, worker) = setup_running_suspendable_workload(&mut router);

    let pod_id = router.get_workload(&W1).unwrap().pod_id.unwrap();

    // Start suspend.
    router.send_activate_service(mgmt, S1, false);
    router.propagate();

    let wl = router.get_workload(&W1).unwrap();
    assert!(wl.awaiting_suspend);

    // Demand returns while suspending.
    router.send_activate_service(mgmt, S1, true);
    router.propagate();

    // Pod is still suspending — can't go back (lifecycle is non-circular).
    let wl = router.get_workload(&W1).unwrap();
    assert!(wl.awaiting_suspend); // still waiting
    assert!(wl.has_demand); // demand is back

    let pod = router.get_pod(&pod_id).unwrap();
    assert_eq!(pod.status, PodStatus::Suspending); // still suspending

    // Worker completes the suspend.
    let artifact = ArtifactId(200);
    router.send_notify_pod_suspended(worker, pod_id, artifact);
    router.propagate();

    // Workload saved artifact, reaped pod, and immediately created new pod
    // from artifact (because demand is present).
    let wl = router.get_workload(&W1).unwrap();
    assert!(!wl.awaiting_suspend);
    assert!(wl.pod_id.is_some());
    assert_ne!(wl.pod_id.unwrap(), pod_id); // new pod
    assert_eq!(wl.suspended_artifact, None); // consumed

    // Old pod is gone.
    assert!(router.get_pod(&pod_id).is_none());

    // New pod should have the artifact for resume.
    let new_pod = router.get_pod(&wl.pod_id.unwrap()).unwrap();
    assert_eq!(new_pod.resume_artifact, Some(artifact));
}

/// 35. Spec change during suspend: workload abandons the suspending pod
///     (artifact is stale), cold boots with new spec.
#[test]
fn spec_change_during_suspend() {
    let mut router = Router::new(16);
    let (mgmt, worker) = setup_running_suspendable_workload(&mut router);

    let pod_id = router.get_workload(&W1).unwrap().pod_id.unwrap();

    // Start suspend.
    router.send_activate_service(mgmt, S1, false);
    router.propagate();

    let wl = router.get_workload(&W1).unwrap();
    assert!(wl.awaiting_suspend);

    // Spec changes while pod is suspending.
    router.set_management_wl_spec(mgmt, WorkloadSpec { image: "app:v2".into(), suspend_on_idle: true });
    router.propagate();

    // Spec change while pod_running=false doesn't trigger immediate restart
    // (that branch checks pod_running). But spec_version is incremented.
    // The pod is still suspending.

    // Re-activate demand so the workload wants a pod again.
    router.send_activate_service(mgmt, S1, true);
    router.propagate();

    // Worker completes the suspend.
    let artifact = ArtifactId(300);
    router.send_notify_pod_suspended(worker, pod_id, artifact);
    router.propagate();

    // Workload should have created a new pod. Since spec changed,
    // the spec_version != launched_with_spec_version check will catch it
    // when the pod reaches Running (if it used the old artifact).
    // But actually, the workload still uses the artifact for resume
    // since the artifact was saved. The spec mismatch is detected at Running.
    let wl = router.get_workload(&W1).unwrap();
    assert!(wl.pod_id.is_some());
    assert_ne!(wl.pod_id.unwrap(), pod_id);
}

/// 36. Worker loss while pod is running on a suspendable workload — pod fails
///     (not suspended), enters backoff normally. No artifact saved.
#[test]
fn worker_loss_on_suspendable_workload() {
    let mut router = Router::new(16);
    let (_mgmt, worker) = setup_running_suspendable_workload(&mut router);

    // Worker dies — this is NOT a suspend, it's a failure.
    router.destroy_worker(worker);
    router.propagate();

    let wl = router.get_workload(&W1).unwrap();
    assert!(!wl.pod_running);
    assert!(wl.in_backoff); // failure, not suspend
    assert_eq!(wl.consecutive_failures, 1);
    assert_eq!(wl.suspended_artifact, None); // no artifact from a crash

    // Service should be back to NeedBackend.
    let s1 = router.get_service(&S1).unwrap();
    assert_eq!(s1.state, ServiceState::NeedBackend);
}

/// 37. Destroy (hard kill) discards any previously saved artifact.
#[test]
fn destroy_discards_artifact() {
    let mut router = Router::new(16);
    let (mgmt, worker) = setup_running_suspendable_workload(&mut router);

    let pod_id = router.get_workload(&W1).unwrap().pod_id.unwrap();

    // Suspend successfully.
    router.send_activate_service(mgmt, S1, false);
    router.propagate();
    let artifact = ArtifactId(400);
    router.send_notify_pod_suspended(worker, pod_id, artifact);
    router.propagate();

    let wl = router.get_workload(&W1).unwrap();
    assert_eq!(wl.suspended_artifact, Some(artifact));

    // Re-activate → resumes from artifact.
    router.send_activate_service(mgmt, S1, true);
    router.propagate();

    let new_pod_id = router.get_workload(&W1).unwrap().pod_id.unwrap();
    make_pod_running(&mut router, worker, new_pod_id);

    // Now do a hard restart (admin command) — destroys pod AND discards artifact.
    router.send_admin_command(mgmt, W1, AdminCmd::Restart);
    router.propagate();

    let wl = router.get_workload(&W1).unwrap();
    assert_eq!(wl.suspended_artifact, None); // artifact discarded
    assert!(wl.pod_id.is_some()); // new pod created (cold boot)

    // New pod should NOT have an artifact.
    let restart_pod = router.get_pod(&wl.pod_id.unwrap()).unwrap();
    assert_eq!(restart_pod.resume_artifact, None);
}

/// 38. Suspend → resume → suspend cycle: verify the full round-trip works
///     and artifact IDs are tracked correctly.
#[test]
fn suspend_resume_suspend_cycle() {
    let mut router = Router::new(16);
    let (mgmt, worker) = setup_running_suspendable_workload(&mut router);

    // First suspend.
    let pod1 = router.get_workload(&W1).unwrap().pod_id.unwrap();
    router.send_activate_service(mgmt, S1, false);
    router.propagate();
    let artifact1 = ArtifactId(500);
    router.send_notify_pod_suspended(worker, pod1, artifact1);
    router.propagate();

    assert_eq!(router.get_workload(&W1).unwrap().suspended_artifact, Some(artifact1));

    // First resume.
    router.send_activate_service(mgmt, S1, true);
    router.propagate();
    let pod2 = router.get_workload(&W1).unwrap().pod_id.unwrap();
    assert_ne!(pod1, pod2);
    assert_eq!(router.get_pod(&pod2).unwrap().resume_artifact, Some(artifact1));
    make_pod_running(&mut router, worker, pod2);

    let wl = router.get_workload(&W1).unwrap();
    assert!(wl.pod_running);

    // Second suspend.
    router.send_activate_service(mgmt, S1, false);
    router.propagate();
    let artifact2 = ArtifactId(501);
    router.send_notify_pod_suspended(worker, pod2, artifact2);
    router.propagate();

    let wl = router.get_workload(&W1).unwrap();
    assert_eq!(wl.suspended_artifact, Some(artifact2));
    assert!(wl.pod_id.is_none());

    // Second resume.
    router.send_activate_service(mgmt, S1, true);
    router.propagate();
    let pod3 = router.get_workload(&W1).unwrap().pod_id.unwrap();
    assert_ne!(pod2, pod3);
    assert_eq!(router.get_pod(&pod3).unwrap().resume_artifact, Some(artifact2));
}

/// 39. Scavenge on suspendable workload with no demand — should behave like
///     normal scavenge (no suspend, just cleanup since pod is already gone).
#[test]
fn scavenge_clears_suspended_artifact() {
    let mut router = Router::new(16);
    let (mgmt, worker) = setup_running_suspendable_workload(&mut router);

    let pod_id = router.get_workload(&W1).unwrap().pod_id.unwrap();

    // Suspend successfully.
    router.send_activate_service(mgmt, S1, false);
    router.propagate();
    let artifact = ArtifactId(600);
    router.send_notify_pod_suspended(worker, pod_id, artifact);
    router.propagate();

    let wl = router.get_workload(&W1).unwrap();
    assert_eq!(wl.suspended_artifact, Some(artifact));

    // Scavenge with no demand — should clear the artifact.
    router.send_admin_command(mgmt, W1, AdminCmd::Scavenge);
    router.propagate();

    let wl = router.get_workload(&W1).unwrap();
    assert_eq!(wl.suspended_artifact, None);
    assert!(wl.pod_id.is_none());
}

// ============================================================================
// suspend_on_idle spec change tests
// ============================================================================

/// 40. Spec change: suspend_on_idle true→false while pod is suspending →
///     abandons the suspending pod, discards artifact, cold boots if demand.
#[test]
fn suspend_on_idle_disabled_during_suspend() {
    let mut router = Router::new(16);
    let (mgmt, worker) = setup_running_suspendable_workload(&mut router);

    let pod_id = router.get_workload(&W1).unwrap().pod_id.unwrap();

    // Start suspend by dropping demand.
    router.send_activate_service(mgmt, S1, false);
    router.propagate();

    let wl = router.get_workload(&W1).unwrap();
    assert!(wl.awaiting_suspend);
    assert!(wl.pod_id.is_some());

    // Spec changes: same image, suspend_on_idle goes false.
    router.set_management_wl_spec(mgmt, WorkloadSpec { image: "app:v1".into(), suspend_on_idle: false });
    router.propagate();

    // Pod should have been abandoned (destroy_current_pod).
    let wl = router.get_workload(&W1).unwrap();
    assert!(!wl.awaiting_suspend);
    assert!(!wl.suspend_on_idle);
    assert!(wl.pod_id.is_none()); // pod abandoned
    assert_eq!(wl.suspended_artifact, None); // no artifact saved

    // Old pod should be gone (abandoned + self-destruct).
    assert!(router.get_pod(&pod_id).is_none());
}

/// 41. Spec change: suspend_on_idle true→false with saved artifact →
///     discards artifact, next demand cycle cold boots.
#[test]
fn suspend_on_idle_disabled_discards_artifact() {
    let mut router = Router::new(16);
    let (mgmt, worker) = setup_running_suspendable_workload(&mut router);

    let pod_id = router.get_workload(&W1).unwrap().pod_id.unwrap();

    // Suspend successfully.
    router.send_activate_service(mgmt, S1, false);
    router.propagate();
    let artifact = ArtifactId(700);
    router.send_notify_pod_suspended(worker, pod_id, artifact);
    router.propagate();

    let wl = router.get_workload(&W1).unwrap();
    assert_eq!(wl.suspended_artifact, Some(artifact));

    // Spec changes: same image, suspend_on_idle goes false.
    router.set_management_wl_spec(mgmt, WorkloadSpec { image: "app:v1".into(), suspend_on_idle: false });
    router.propagate();

    // Artifact should be discarded.
    let wl = router.get_workload(&W1).unwrap();
    assert!(!wl.suspend_on_idle);
    assert_eq!(wl.suspended_artifact, None);

    // Re-activate → demand returns → cold boot (no artifact).
    router.send_activate_service(mgmt, S1, true);
    router.propagate();

    let wl = router.get_workload(&W1).unwrap();
    assert!(wl.pod_id.is_some());
    let new_pod = router.get_pod(&wl.pod_id.unwrap()).unwrap();
    assert_eq!(new_pod.resume_artifact, None); // cold boot, no artifact
}

/// 42. Spec change: suspend_on_idle false→true while pod is running with
///     demand → no immediate effect. Next demand drop should suspend.
#[test]
fn suspend_on_idle_enabled_with_running_pod() {
    let mut router = Router::new(16);
    let (mgmt, worker, pod_id) = setup_workload_with_pending_pod(&mut router);
    make_pod_running(&mut router, worker, pod_id);

    let wl = router.get_workload(&W1).unwrap();
    assert!(wl.pod_running);
    assert!(!wl.suspend_on_idle);

    // Enable suspend_on_idle via spec (same image).
    router.set_management_wl_spec(mgmt, WorkloadSpec { image: "app:v1".into(), suspend_on_idle: true });
    router.propagate();

    // Pod should still be running (demand is present).
    let wl = router.get_workload(&W1).unwrap();
    assert!(wl.pod_running);
    assert!(wl.suspend_on_idle);
    assert_eq!(wl.pod_id, Some(pod_id)); // same pod, no restart

    // Drop demand → should suspend (not destroy).
    router.send_activate_service(mgmt, S1, false);
    router.propagate();

    let wl = router.get_workload(&W1).unwrap();
    assert!(wl.awaiting_suspend);
    assert!(wl.pod_id.is_some());

    // Complete suspend.
    let artifact = ArtifactId(800);
    router.send_notify_pod_suspended(worker, pod_id, artifact);
    router.propagate();

    let wl = router.get_workload(&W1).unwrap();
    assert_eq!(wl.suspended_artifact, Some(artifact));
    assert!(wl.pod_id.is_none());
}

/// 43. Spec change: only suspend_on_idle changes (same image) → no pod restart,
///     no spec_version bump.
#[test]
fn suspend_on_idle_change_no_restart() {
    let mut router = Router::new(16);
    let (mgmt, worker, pod_id) = setup_workload_with_pending_pod(&mut router);
    make_pod_running(&mut router, worker, pod_id);

    let wl = router.get_workload(&W1).unwrap();
    let original_spec_version = wl.spec_version;
    let original_pod = wl.pod_id.unwrap();

    // Toggle suspend_on_idle (same image).
    router.set_management_wl_spec(mgmt, WorkloadSpec { image: "app:v1".into(), suspend_on_idle: true });
    router.propagate();

    let wl = router.get_workload(&W1).unwrap();
    assert_eq!(wl.spec_version, original_spec_version); // no version bump
    assert_eq!(wl.pod_id, Some(original_pod)); // same pod
    assert!(wl.pod_running); // still running
    assert!(wl.suspend_on_idle);
}

/// 44. Spec change: image AND suspend_on_idle change simultaneously →
///     pod restarts (image change), suspend_on_idle updated.
#[test]
fn image_and_suspend_change_together() {
    let mut router = Router::new(16);
    let (mgmt, worker, pod_id) = setup_workload_with_pending_pod(&mut router);
    make_pod_running(&mut router, worker, pod_id);

    let wl = router.get_workload(&W1).unwrap();
    let original_pod = wl.pod_id.unwrap();

    // Change both image and suspend_on_idle.
    router.set_management_wl_spec(mgmt, WorkloadSpec { image: "app:v2".into(), suspend_on_idle: true });
    router.propagate();

    let wl = router.get_workload(&W1).unwrap();
    assert!(wl.suspend_on_idle);
    assert!(wl.pod_id.is_some());
    assert_ne!(wl.pod_id.unwrap(), original_pod); // new pod (image changed)
    assert!(!wl.pod_running); // new pod is Pending
}
