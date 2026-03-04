use crate::types::*;
use crate::wg_peers::{ConnectResult, WgPeerOutput};

use super::NamespaceStateMachine;

impl NamespaceStateMachine {
    pub(super) fn handle_connect(
        &mut self,
        client_id: ClientId,
        client_public_key: [u8; 32],
        worker_wg_public_key: [u8; 32],
        worker_endpoint: String,
        out: &mut NamespaceOutput,
    ) {
        if self.status != NamespaceStatus::Active {
            out.client_events.push((
                client_id,
                ClientEvent::Error {
                    message: "namespace is not active".to_string(),
                },
            ));
            return;
        }

        match self.wg_peer_manager.connect(client_public_key) {
            ConnectResult::Ok { client_ip, outputs } => {
                self.apply_wg_outputs(outputs, out);
                let subnet_cidr = self.wg_peer_manager.subnet_cidr();
                out.client_events.push((
                    client_id,
                    ClientEvent::ConnectResult {
                        server_public_key: worker_wg_public_key,
                        endpoint: worker_endpoint,
                        client_ip: client_ip.to_string(),
                        subnet: subnet_cidr,
                    },
                ));
            }
            ConnectResult::Error { message } => {
                out.client_events.push((client_id, ClientEvent::Error { message }));
            }
        }
    }

    pub(super) fn handle_disconnect(
        &mut self,
        client_id: ClientId,
        client_public_key: [u8; 32],
        out: &mut NamespaceOutput,
    ) {
        let outputs = self.wg_peer_manager.disconnect(client_public_key);
        self.apply_wg_outputs(outputs, out);
        out.client_events.push((client_id, ClientEvent::Ok));
    }

    pub(crate) fn apply_wg_outputs(&self, outputs: Vec<WgPeerOutput>, out: &mut NamespaceOutput) {
        for wg_out in outputs {
            match wg_out {
                WgPeerOutput::AddPeer { peer_public_key, peer_ip } => {
                    if let Some(worker_id) = self.active_worker_ids().into_iter().next() {
                        out.worker_commands.push((
                            worker_id,
                            WorkerCommand::AddWireGuardPeer {
                                namespace_id: self.namespace_id.clone(),
                                peer_public_key,
                                peer_ip,
                                preshared_key: None,
                            },
                        ));
                    }
                }
                WgPeerOutput::RemovePeer { peer_public_key } => {
                    for worker_id in self.active_worker_ids() {
                        out.worker_commands.push((
                            worker_id,
                            WorkerCommand::RemoveWireGuardPeer {
                                peer_public_key,
                            },
                        ));
                    }
                }
            }
        }
    }
}
