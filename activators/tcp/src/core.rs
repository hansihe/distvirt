use std::collections::HashMap;

use activator_types::*;

const MAX_FLOWS: usize = 1024;

struct FlowState {
    raw_frame: Vec<u8>,
}

pub struct TcpActivator {
    flows: HashMap<u64, FlowState>,
}

impl TcpActivator {
    pub fn new() -> Self {
        Self {
            flows: HashMap::new(),
        }
    }
}

impl Activator for TcpActivator {
    fn process_events(&mut self, events: Vec<Event>) -> Vec<Action> {
        let mut actions = Vec::new();

        for event in events {
            match event {
                Event::Packet(info) => {
                    let flow = info.flow;
                    let tcp_flags = info.tcp_flags.unwrap_or(0);
                    let is_syn = tcp_flags & 0x02 != 0;
                    let is_rst = tcp_flags & 0x04 != 0;

                    if is_rst {
                        actions.push(Action::PacketDecision {
                            flow,
                            decision: PacketDecision::Drop,
                        });
                        continue;
                    }

                    let known = self.flows.contains_key(&flow);

                    if is_syn {
                        if !known {
                            // New SYN flow
                            if self.flows.len() < MAX_FLOWS {
                                self.flows.insert(
                                    flow,
                                    FlowState {
                                        raw_frame: info.raw_frame.clone(),
                                    },
                                );
                            }
                            actions.push(Action::PacketDecision {
                                flow,
                                decision: PacketDecision::Buffered,
                            });
                            actions.push(Action::SetBackendNeed(BackendNeed::Traffic));
                        } else {
                            // SYN retransmit on known flow
                            actions.push(Action::PacketDecision {
                                flow,
                                decision: PacketDecision::Buffered,
                            });
                        }
                    } else if known {
                        // Non-SYN on known flow
                        actions.push(Action::PacketDecision {
                            flow,
                            decision: PacketDecision::Buffered,
                        });
                    } else {
                        // Non-SYN on unknown flow — store and signal
                        if self.flows.len() < MAX_FLOWS {
                            self.flows.insert(
                                flow,
                                FlowState {
                                    raw_frame: info.raw_frame.clone(),
                                },
                            );
                        }
                        actions.push(Action::PacketDecision {
                            flow,
                            decision: PacketDecision::Buffered,
                        });
                        actions.push(Action::SetBackendNeed(BackendNeed::Traffic));
                    }
                }
                Event::BackendAvailable(available) => {
                    if available {
                        let frames: Vec<Vec<u8>> = self
                            .flows
                            .values()
                            .map(|s| s.raw_frame.clone())
                            .collect();
                        self.flows.clear();
                        for frame in frames {
                            actions.push(Action::ReplayPacket(frame));
                        }
                    }
                }
                // TCP activator ignores stream events
                _ => {}
            }
        }

        actions
    }
}
