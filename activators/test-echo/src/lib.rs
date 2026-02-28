#[allow(warnings)]
mod bindings;

use std::cell::RefCell;

use bindings::*;

struct Component;

thread_local! {
    static BUFFERED: RefCell<Vec<Vec<u8>>> = RefCell::new(Vec::new());
}

export!(Component);

impl Guest for Component {
    fn process_events(events: Vec<Event>) -> Vec<Action> {
        let mut actions = Vec::new();

        for event in events {
            match event {
                Event::Packet(info) => {
                    let flow = info.flow;
                    let raw = info.raw_frame.clone();

                    actions.push(Action::PacketDecision((flow, PacketDecision::Buffered)));
                    actions.push(Action::SetBackendNeed(BackendNeed::Traffic));
                    actions.push(Action::Log(LogAction {
                        level: LogLevel::Info,
                        message: format!("packet:{}", flow),
                    }));

                    BUFFERED.with(|b| b.borrow_mut().push(raw));
                }
                Event::BackendAvailable(available) => {
                    if available {
                        actions.push(Action::Log(LogAction {
                            level: LogLevel::Info,
                            message: "backend:available".into(),
                        }));

                        let frames: Vec<Vec<u8>> =
                            BUFFERED.with(|b| b.borrow_mut().drain(..).collect());
                        for frame in frames {
                            actions.push(Action::ReplayPacket(frame));
                        }
                    } else {
                        actions.push(Action::Log(LogAction {
                            level: LogLevel::Info,
                            message: "backend:unavailable".into(),
                        }));
                        actions.push(Action::SetBackendNeed(BackendNeed::None));
                    }
                }
                Event::Tick => {
                    actions.push(Action::Log(LogAction {
                        level: LogLevel::Debug,
                        message: "tick".into(),
                    }));
                }
                Event::StreamOpen(s) => {
                    actions.push(Action::Log(LogAction {
                        level: LogLevel::Info,
                        message: format!("stream-open:{}", s),
                    }));
                    actions.push(Action::SetBackendNeed(BackendNeed::Active));
                }
                Event::StreamData(ev) => {
                    actions.push(Action::DownstreamSend((ev.s, ev.data)));
                }
                Event::StreamClose(s) => {
                    actions.push(Action::Log(LogAction {
                        level: LogLevel::Info,
                        message: format!("stream-close:{}", s),
                    }));
                }
                Event::UpstreamConnectResult(ev) => match ev.outcome {
                    ConnectResult::Ok => {
                        actions.push(Action::UpstreamSend((ev.s, b"hello".to_vec())));
                    }
                    ConnectResult::Refused | ConnectResult::Timeout => {
                        actions.push(Action::Log(LogAction {
                            level: LogLevel::Warn,
                            message: format!("upstream-failed:{}", ev.s),
                        }));
                    }
                },
                Event::UpstreamData(ev) => {
                    actions.push(Action::DownstreamSend((ev.s, ev.data)));
                }
                Event::UpstreamClose(s) => {
                    actions.push(Action::Log(LogAction {
                        level: LogLevel::Info,
                        message: format!("upstream-close:{}", s),
                    }));
                }
            }
        }

        actions
    }
}
