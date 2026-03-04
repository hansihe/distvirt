use std::time::Duration;

use super::*;

#[derive(Debug, Clone, PartialEq)]
pub enum OrchestratorInput {
    ClientConnected { client_id: ClientId },
    ClientDisconnected { client_id: ClientId },
    ClientCommand { client_id: ClientId, command: ClientCommand },
    WorkerConnected { worker_id: WorkerId, capabilities: WorkerCapabilities, wg_config: Option<WorkerWgConfig> },
    WorkerDisconnected { worker_id: WorkerId },
    NamespaceInput { namespace_id: NamespaceId, input: NamespaceInput },
}

#[derive(Debug, Clone, PartialEq, Default)]
pub struct OrchestratorOutput {
    pub worker_commands: Vec<(WorkerId, WorkerCommand)>,
    pub client_events: Vec<(ClientId, ClientEvent)>,
    pub timers_set: Vec<(TimerKey, Duration)>,
    pub timers_cancel: Vec<TimerKey>,
    pub namespace_outputs: Vec<(NamespaceId, NamespaceOutput)>,
}
