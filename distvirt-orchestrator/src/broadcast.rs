use std::collections::BTreeMap;

use crate::types::*;

pub fn broadcast_to_active_workers(
    workers: &BTreeMap<WorkerId, NamespaceWorkerState>,
    out: &mut NamespaceOutput,
    make_cmd: impl Fn(&WorkerId) -> WorkerCommand,
) {
    for (wid, ws) in workers {
        if ws.fabric_status == FabricStatus::Active {
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

    fn make_workers(entries: &[(&str, FabricStatus)]) -> BTreeMap<WorkerId, NamespaceWorkerState> {
        entries
            .iter()
            .map(|(id, status)| {
                (
                    wid(id),
                    NamespaceWorkerState {
                        fabric_status: status.clone(),
                        primary_pool_id: None,
                        pressure_band: PressureBand::Normal,
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
        let workers = BTreeMap::new();
        let mut out = NamespaceOutput::default();
        broadcast_to_active_workers(&workers, &mut out, dummy_cmd);
        assert_eq!(out.worker_commands.len(), 0);
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
