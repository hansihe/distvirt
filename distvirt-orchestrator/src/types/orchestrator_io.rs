use std::time::Duration;

use super::*;

#[derive(Debug, Clone, PartialEq)]
pub enum OrchestratorInput {
    ClientConnected {
        client_id: ClientId,
    },
    ClientDisconnected {
        client_id: ClientId,
    },
    ClientCommand {
        client_id: ClientId,
        command: ClientCommand,
    },
    WorkerConnected {
        worker_id: WorkerId,
        capabilities: WorkerCapabilities,
        wg_config: Option<WorkerWgConfig>,
        tunnel_config: Option<WorkerTunnelConfig>,
    },
    WorkerDisconnected {
        worker_id: WorkerId,
    },
    NamespaceInput {
        namespace_id: NamespaceId,
        input: NamespaceInput,
    },
    WorkerPressureUpdate {
        worker_id: WorkerId,
        cpu: PsiMetrics,
        memory: PsiMetrics,
        io: PsiMetrics,
    },
    WorkerPoolCapacityUpdate {
        worker_id: WorkerId,
        pools: Vec<PoolInfo>,
    },
    WorkerArtifactTransferReceived {
        worker_id: WorkerId,
        transfer_id: u64,
        dest_artifact_id: ArtifactId,
        dest_pool_id: PoolId,
        size_bytes: u64,
    },
    WorkerTransferFailed {
        worker_id: WorkerId,
        transfer_id: u64,
        source_artifact_id: ArtifactId,
        dest_artifact_id: ArtifactId,
        error: String,
    },
    WorkerConditionUpdate {
        worker_id: WorkerId,
        key: String,
        active: bool,
        message: String,
    },
}

#[derive(Debug, Clone, PartialEq, Default)]
pub struct OrchestratorOutput {
    pub worker_commands: Vec<(WorkerId, WorkerCommand)>,
    pub client_events: Vec<(ClientId, ClientEvent)>,
    pub timers_set: Vec<(TimerKey, Duration)>,
    pub timers_cancel: Vec<TimerKey>,
    pub namespace_outputs: Vec<(NamespaceId, NamespaceOutput)>,
}

impl OrchestratorOutput {
    /// Merge a namespace's output into this orchestrator output.
    pub fn merge_namespace(&mut self, namespace_id: NamespaceId, ns_out: NamespaceOutput) {
        self.worker_commands
            .extend(ns_out.worker_commands.iter().cloned());
        self.timers_set.extend(ns_out.timers_set.iter().cloned());
        self.timers_cancel
            .extend(ns_out.timers_cancel.iter().cloned());
        if ns_out != NamespaceOutput::default() {
            self.namespace_outputs.push((namespace_id, ns_out));
        }
    }
}
