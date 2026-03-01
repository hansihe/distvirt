use activator_types::*;

pub struct TestEcho {
    buffered: Vec<Vec<u8>>,
}

impl TestEcho {
    pub fn new() -> Self {
        Self {
            buffered: Vec::new(),
        }
    }
}

impl Activator for TestEcho {
    fn process_events(&mut self, events: Vec<Event>) -> Vec<Action> {
        let mut actions = Vec::new();

        for event in events {
            match event {
                Event::Packet(info) => {
                    let flow = info.flow;
                    let raw = info.raw_frame.clone();

                    actions.push(Action::PacketDecision {
                        flow,
                        decision: PacketDecision::Buffered,
                    });
                    actions.push(Action::SetBackendNeed(BackendNeed::Traffic));
                    actions.push(Action::Log(LogAction {
                        level: LogLevel::Info,
                        message: format!("packet:{}", flow),
                    }));

                    self.buffered.push(raw);
                }
                Event::BackendAvailable(available) => {
                    if available {
                        actions.push(Action::Log(LogAction {
                            level: LogLevel::Info,
                            message: "backend:available".into(),
                        }));

                        for frame in self.buffered.drain(..) {
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
                Event::StreamData { stream, data } => {
                    actions.push(Action::DownstreamSend { stream, data });
                }
                Event::StreamClose(s) => {
                    actions.push(Action::Log(LogAction {
                        level: LogLevel::Info,
                        message: format!("stream-close:{}", s),
                    }));
                }
                Event::UpstreamConnectResult { stream, ok } => {
                    if ok {
                        actions.push(Action::UpstreamSend {
                            stream,
                            data: b"hello".to_vec(),
                        });
                    } else {
                        actions.push(Action::Log(LogAction {
                            level: LogLevel::Warn,
                            message: format!("upstream-failed:{}", stream),
                        }));
                    }
                }
                Event::UpstreamData { stream, data } => {
                    actions.push(Action::DownstreamSend { stream, data });
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
