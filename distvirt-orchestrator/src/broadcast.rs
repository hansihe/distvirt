use std::collections::HashMap;

use crate::types::*;

pub fn broadcast_to_active_workers(
    workers: &HashMap<WorkerId, NamespaceWorkerState>,
    out: &mut NamespaceOutput,
    make_cmd: impl Fn(&WorkerId) -> WorkerCommand,
) {
    for (wid, ws) in workers {
        if ws.fabric_status == FabricStatus::Active {
            out.worker_commands.push((wid.clone(), make_cmd(wid)));
        }
    }
}

pub fn broadcast_to_active_workers_except(
    workers: &HashMap<WorkerId, NamespaceWorkerState>,
    exclude: &WorkerId,
    out: &mut NamespaceOutput,
    make_cmd: impl Fn(&WorkerId) -> WorkerCommand,
) {
    for (wid, ws) in workers {
        if ws.fabric_status == FabricStatus::Active && wid != exclude {
            out.worker_commands.push((wid.clone(), make_cmd(wid)));
        }
    }
}

pub fn send_to_worker(
    worker_id: &WorkerId,
    out: &mut NamespaceOutput,
    cmd: WorkerCommand,
) {
    out.worker_commands.push((worker_id.clone(), cmd));
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;

    fn wid(s: &str) -> WorkerId {
        WorkerId(s.into())
    }

    fn make_workers(entries: &[(&str, FabricStatus)]) -> HashMap<WorkerId, NamespaceWorkerState> {
        entries
            .iter()
            .map(|(id, status)| {
                (
                    wid(id),
                    NamespaceWorkerState {
                        fabric_status: status.clone(),
                        primary_pool_id: None,
                    },
                )
            })
            .collect()
    }

    fn dummy_cmd(_wid: &WorkerId) -> WorkerCommand {
        WorkerCommand::DestroyNamespace {
            namespace_id: NamespaceId::from("test-ns"),
        }
    }

    #[test]
    fn broadcast_all_active() {
        let workers = make_workers(&[
            ("w1", FabricStatus::Active),
            ("w2", FabricStatus::Active),
            ("w3", FabricStatus::Active),
        ]);
        let mut out = NamespaceOutput::default();
        broadcast_to_active_workers(&workers, &mut out, dummy_cmd);
        assert_eq!(out.worker_commands.len(), 3);
    }

    #[test]
    fn broadcast_mixed_status() {
        let workers = make_workers(&[
            ("w1", FabricStatus::Active),
            ("w2", FabricStatus::Creating),
            ("w3", FabricStatus::Destroying),
        ]);
        let mut out = NamespaceOutput::default();
        broadcast_to_active_workers(&workers, &mut out, dummy_cmd);
        assert_eq!(out.worker_commands.len(), 1);
        assert_eq!(out.worker_commands[0].0, wid("w1"));
    }

    #[test]
    fn broadcast_none_active() {
        let workers = make_workers(&[
            ("w1", FabricStatus::Creating),
            ("w2", FabricStatus::Destroying),
        ]);
        let mut out = NamespaceOutput::default();
        broadcast_to_active_workers(&workers, &mut out, dummy_cmd);
        assert_eq!(out.worker_commands.len(), 0);
    }

    #[test]
    fn broadcast_empty_workers() {
        let workers = HashMap::new();
        let mut out = NamespaceOutput::default();
        broadcast_to_active_workers(&workers, &mut out, dummy_cmd);
        assert_eq!(out.worker_commands.len(), 0);
    }

    #[test]
    fn broadcast_except_excludes() {
        let workers = make_workers(&[
            ("w1", FabricStatus::Active),
            ("w2", FabricStatus::Active),
        ]);
        let mut out = NamespaceOutput::default();
        broadcast_to_active_workers_except(&workers, &wid("w1"), &mut out, dummy_cmd);
        assert_eq!(out.worker_commands.len(), 1);
        assert_eq!(out.worker_commands[0].0, wid("w2"));
    }

    #[test]
    fn broadcast_except_nonexistent() {
        let workers = make_workers(&[
            ("w1", FabricStatus::Active),
            ("w2", FabricStatus::Active),
        ]);
        let mut out = NamespaceOutput::default();
        broadcast_to_active_workers_except(&workers, &wid("w_unknown"), &mut out, dummy_cmd);
        assert_eq!(out.worker_commands.len(), 2);
    }

    #[test]
    fn send_to_worker_appends_one() {
        let mut out = NamespaceOutput::default();
        send_to_worker(&wid("w1"), &mut out, WorkerCommand::DestroyNamespace {
            namespace_id: NamespaceId::from("test-ns"),
        });
        assert_eq!(out.worker_commands.len(), 1);
        assert_eq!(out.worker_commands[0].0, wid("w1"));
    }

    #[test]
    fn make_cmd_receives_correct_ids() {
        let workers = make_workers(&[
            ("w1", FabricStatus::Active),
            ("w2", FabricStatus::Active),
            ("w3", FabricStatus::Creating),
        ]);
        let mut out = NamespaceOutput::default();
        broadcast_to_active_workers(&workers, &mut out, dummy_cmd);
        // Verify that only Active workers received commands.
        let cmd_worker_ids: HashSet<_> = out.worker_commands.iter().map(|(w, _)| w.clone()).collect();
        assert!(cmd_worker_ids.contains(&wid("w1")));
        assert!(cmd_worker_ids.contains(&wid("w2")));
        assert!(!cmd_worker_ids.contains(&wid("w3")));
        assert_eq!(cmd_worker_ids.len(), 2);
    }
}
