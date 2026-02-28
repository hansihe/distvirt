#[allow(warnings)]
mod bindings;

use std::cell::RefCell;
use std::collections::HashMap;

use bindings::*;

struct Component;

const MAX_FLOWS: usize = 1024;

struct FlowState {
    raw_frame: Vec<u8>,
}

thread_local! {
    static FLOWS: RefCell<HashMap<u64, FlowState>> = RefCell::new(HashMap::new());
}

export!(Component);

impl Guest for Component {
    fn process_events(events: Vec<Event>) -> Vec<Action> {
        let mut actions = Vec::new();

        for event in events {
            match event {
                Event::Packet(info) => {
                    let flow = info.flow;
                    let tcp_flags = info.tcp_flags.unwrap_or(0);
                    let is_syn = tcp_flags & 0x02 != 0;
                    let is_rst = tcp_flags & 0x04 != 0;

                    if is_rst {
                        actions.push(Action::PacketDecision((flow, PacketDecision::Drop)));
                        continue;
                    }

                    let known = FLOWS.with(|f| f.borrow().contains_key(&flow));

                    if is_syn {
                        if !known {
                            // New SYN flow
                            let can_insert = FLOWS.with(|f| f.borrow().len() < MAX_FLOWS);
                            if can_insert {
                                FLOWS.with(|f| {
                                    f.borrow_mut().insert(
                                        flow,
                                        FlowState {
                                            raw_frame: info.raw_frame.clone(),
                                        },
                                    );
                                });
                            }
                            actions.push(Action::PacketDecision((flow, PacketDecision::Buffered)));
                            actions.push(Action::SetBackendNeed(BackendNeed::Traffic));
                        } else {
                            // SYN retransmit on known flow
                            actions.push(Action::PacketDecision((flow, PacketDecision::Buffered)));
                        }
                    } else if known {
                        // Non-SYN on known flow
                        actions.push(Action::PacketDecision((flow, PacketDecision::Buffered)));
                    } else {
                        // Non-SYN on unknown flow — store and signal
                        let can_insert = FLOWS.with(|f| f.borrow().len() < MAX_FLOWS);
                        if can_insert {
                            FLOWS.with(|f| {
                                f.borrow_mut().insert(
                                    flow,
                                    FlowState {
                                        raw_frame: info.raw_frame.clone(),
                                    },
                                );
                            });
                        }
                        actions.push(Action::PacketDecision((flow, PacketDecision::Buffered)));
                        actions.push(Action::SetBackendNeed(BackendNeed::Traffic));
                    }
                }
                Event::BackendAvailable(available) => {
                    if available {
                        let frames: Vec<Vec<u8>> = FLOWS.with(|f| {
                            let mut flows = f.borrow_mut();
                            let frames: Vec<Vec<u8>> =
                                flows.values().map(|s| s.raw_frame.clone()).collect();
                            flows.clear();
                            frames
                        });
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
