use activator_types::test_helpers::*;
use activator_types::*;
use test_echo::core::TestEcho;

fn new_echo() -> TestEcho {
    TestEcho::new()
}

#[test]
fn packet_roundtrip() {
    let mut echo = new_echo();
    let raw = vec![0xDE, 0xAD];
    let actions = echo.process_events(vec![Event::Packet(make_syn_packet(42, raw))]);

    assert!(matches!(
        &actions[0],
        Action::PacketDecision {
            flow: 42,
            decision: PacketDecision::Buffered
        }
    ));
    assert!(matches!(
        &actions[1],
        Action::SetBackendNeed(BackendNeed::Traffic)
    ));
    assert!(
        matches!(&actions[2], Action::Log(log) if log.level == LogLevel::Info && log.message == "packet:42")
    );
}

#[test]
fn backend_available_replays() {
    let mut echo = new_echo();
    let raw = vec![0xCA, 0xFE];
    echo.process_events(vec![Event::Packet(make_syn_packet(1, raw.clone()))]);

    let actions = echo.process_events(vec![Event::BackendAvailable(true)]);

    assert!(matches!(&actions[0], Action::Log(log) if log.message == "backend:available"));
    assert!(matches!(&actions[1], Action::ReplayPacket(data) if data == &raw));
}

#[test]
fn backend_unavailable() {
    let mut echo = new_echo();
    let actions = echo.process_events(vec![Event::BackendAvailable(false)]);

    assert!(matches!(&actions[0], Action::Log(log) if log.message == "backend:unavailable"));
    assert!(matches!(
        &actions[1],
        Action::SetBackendNeed(BackendNeed::None)
    ));
}

#[test]
fn tick() {
    let mut echo = new_echo();
    let actions = echo.process_events(vec![Event::Tick]);

    assert_eq!(actions.len(), 1);
    assert!(
        matches!(&actions[0], Action::Log(log) if log.level == LogLevel::Debug && log.message == "tick")
    );
}

#[test]
fn stream_open() {
    let mut echo = new_echo();
    let actions = echo.process_events(vec![Event::StreamOpen(7)]);

    assert!(matches!(&actions[0], Action::Log(log) if log.message == "stream-open:7"));
    assert!(matches!(
        &actions[1],
        Action::SetBackendNeed(BackendNeed::Active)
    ));
}

#[test]
fn stream_data_echo() {
    let mut echo = new_echo();
    let data = b"hello world".to_vec();
    let actions = echo.process_events(vec![Event::StreamData {
        stream: 5,
        data: data.clone(),
    }]);

    assert_eq!(actions.len(), 1);
    assert!(
        matches!(&actions[0], Action::DownstreamSend { stream: 5, data: d } if d == &data)
    );
}

#[test]
fn stream_close() {
    let mut echo = new_echo();
    let actions = echo.process_events(vec![Event::StreamClose(3)]);

    assert_eq!(actions.len(), 1);
    assert!(matches!(&actions[0], Action::Log(log) if log.message == "stream-close:3"));
}

#[test]
fn upstream_connect_ok() {
    let mut echo = new_echo();
    let actions = echo.process_events(vec![Event::UpstreamConnectResult {
        stream: 10,
        ok: true,
    }]);

    assert_eq!(actions.len(), 1);
    assert!(
        matches!(&actions[0], Action::UpstreamSend { stream: 10, data } if data == b"hello")
    );
}

#[test]
fn upstream_connect_refused() {
    let mut echo = new_echo();
    let actions = echo.process_events(vec![Event::UpstreamConnectResult {
        stream: 10,
        ok: false,
    }]);

    assert_eq!(actions.len(), 1);
    assert!(
        matches!(&actions[0], Action::Log(log) if log.level == LogLevel::Warn && log.message == "upstream-failed:10")
    );
}

#[test]
fn upstream_data_proxy() {
    let mut echo = new_echo();
    let data = b"response".to_vec();
    let actions = echo.process_events(vec![Event::UpstreamData {
        stream: 8,
        data: data.clone(),
    }]);

    assert_eq!(actions.len(), 1);
    assert!(
        matches!(&actions[0], Action::DownstreamSend { stream: 8, data: d } if d == &data)
    );
}

#[test]
fn upstream_close() {
    let mut echo = new_echo();
    let actions = echo.process_events(vec![Event::UpstreamClose(4)]);

    assert_eq!(actions.len(), 1);
    assert!(matches!(&actions[0], Action::Log(log) if log.message == "upstream-close:4"));
}

#[test]
fn batch_multiple_events() {
    let mut echo = new_echo();
    let actions = echo.process_events(vec![
        Event::Tick,
        Event::StreamOpen(1),
        Event::StreamClose(1),
    ]);

    assert_eq!(actions.len(), 4);
    assert!(matches!(&actions[0], Action::Log(log) if log.message == "tick"));
    assert!(matches!(&actions[1], Action::Log(log) if log.message == "stream-open:1"));
    assert!(matches!(
        &actions[2],
        Action::SetBackendNeed(BackendNeed::Active)
    ));
    assert!(matches!(&actions[3], Action::Log(log) if log.message == "stream-close:1"));
}
