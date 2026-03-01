#[allow(warnings)]
mod bindings;

pub mod connection;
pub mod core;
pub mod frame;

use std::cell::RefCell;

use bindings::*;

struct Component;

thread_local! {
    static INSTANCE: RefCell<core::Http2Activator> = RefCell::new(core::Http2Activator::new());
}

export!(Component);

impl Guest for Component {
    fn process_events(events: Vec<Event>) -> Vec<Action> {
        INSTANCE.with(|inst| {
            let mut inst = inst.borrow_mut();
            let shared_events = events.into_iter().map(event_from_wit).collect();
            let shared_actions = activator_types::Activator::process_events(&mut *inst, shared_events);
            shared_actions.into_iter().map(action_to_wit).collect()
        })
    }
}

fn event_from_wit(event: Event) -> activator_types::Event {
    match event {
        Event::BackendAvailable(b) => activator_types::Event::BackendAvailable(b),
        Event::Tick => activator_types::Event::Tick,
        Event::Packet(info) => activator_types::Event::Packet(activator_types::PacketInfo {
            flow: info.flow,
            src_addr: info.src_addr,
            dst_addr: info.dst_addr,
            src_port: info.src_port,
            dst_port: info.dst_port,
            protocol: match info.protocol {
                IpProtocol::Tcp => activator_types::IpProtocol::Tcp,
                IpProtocol::Udp => activator_types::IpProtocol::Udp,
                IpProtocol::Other => activator_types::IpProtocol::Other,
            },
            tcp_flags: info.tcp_flags,
            payload_len: info.payload.len(),
            raw_frame: info.raw_frame,
        }),
        Event::StreamOpen(s) => activator_types::Event::StreamOpen(s),
        Event::StreamData(ev) => activator_types::Event::StreamData {
            stream: ev.s,
            data: ev.data,
        },
        Event::StreamClose(s) => activator_types::Event::StreamClose(s),
        Event::UpstreamConnectResult(ev) => activator_types::Event::UpstreamConnectResult {
            stream: ev.s,
            ok: matches!(ev.outcome, ConnectResult::Ok),
        },
        Event::UpstreamData(ev) => activator_types::Event::UpstreamData {
            stream: ev.s,
            data: ev.data,
        },
        Event::UpstreamClose(s) => activator_types::Event::UpstreamClose(s),
    }
}

fn action_to_wit(action: activator_types::Action) -> Action {
    match action {
        activator_types::Action::SetBackendNeed(need) => {
            Action::SetBackendNeed(match need {
                activator_types::BackendNeed::None => BackendNeed::None,
                activator_types::BackendNeed::Traffic => BackendNeed::Traffic,
                activator_types::BackendNeed::Active => BackendNeed::Active,
            })
        }
        activator_types::Action::Log(log) => Action::Log(LogAction {
            level: match log.level {
                activator_types::LogLevel::Trace => LogLevel::Trace,
                activator_types::LogLevel::Debug => LogLevel::Debug,
                activator_types::LogLevel::Info => LogLevel::Info,
                activator_types::LogLevel::Warn => LogLevel::Warn,
                activator_types::LogLevel::Error => LogLevel::Error,
            },
            message: log.message,
        }),
        activator_types::Action::PacketDecision { flow, decision } => {
            Action::PacketDecision((
                flow,
                match decision {
                    activator_types::PacketDecision::Buffered => PacketDecision::Buffered,
                    activator_types::PacketDecision::Drop => PacketDecision::Drop,
                },
            ))
        }
        activator_types::Action::PacketReply { flow, data } => Action::PacketReply((flow, data)),
        activator_types::Action::ReplayPacket(data) => Action::ReplayPacket(data),
        activator_types::Action::DownstreamSend { stream, data } => {
            Action::DownstreamSend((stream, data))
        }
        activator_types::Action::DownstreamClose(s) => Action::DownstreamClose(s),
        activator_types::Action::PauseDownstream(s) => Action::PauseDownstream(s),
        activator_types::Action::ResumeDownstream(s) => Action::ResumeDownstream(s),
        activator_types::Action::UpstreamConnect { port } => Action::UpstreamConnect(port),
        activator_types::Action::UpstreamSend { stream, data } => {
            Action::UpstreamSend((stream, data))
        }
        activator_types::Action::UpstreamClose(s) => Action::UpstreamClose(s),
        activator_types::Action::PauseUpstream(s) => Action::PauseUpstream(s),
        activator_types::Action::ResumeUpstream(s) => Action::ResumeUpstream(s),
    }
}
