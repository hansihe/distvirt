//! Integration tests for ActivatorRuntime and ActivatorInstance using WASM components.

use std::net::IpAddr;
use std::path::PathBuf;

use activator_types::test_helpers::{
    H2_PREFACE, collect_frames, h2_data, h2_headers, h2_ping, h2_settings, parse_settings_payload,
};
use distvirt_activator::types::{
    Action, BackendNeed, Event, IpProtocol, LogLevel, PacketDecision, PacketInfo,
};
use distvirt_activator::{ActivatorInstance, ActivatorRuntime};

fn components_dir() -> PathBuf {
    let manifest = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    manifest.join("../activators/target/components")
}

fn require_components() -> ActivatorRuntime {
    let dir = components_dir();
    let runtime = ActivatorRuntime::new(&dir).expect("failed to create runtime");
    if !runtime.has_component("test-echo") {
        panic!(
            "WASM components not found at {:?}. Run ./activators/build.sh first.",
            dir
        );
    }
    runtime
}

fn make_packet(flow: u64, raw_frame: Vec<u8>) -> PacketInfo {
    PacketInfo {
        flow,
        src_addr: IpAddr::from([10, 0, 0, 1]),
        dst_addr: IpAddr::from([10, 0, 0, 2]),
        src_port: 12345,
        dst_port: 80,
        protocol: IpProtocol::Tcp,
        tcp_flags: Some(0x02),
        payload_len: 0,
        raw_frame,
    }
}

fn make_tcp_packet(flow: u64, tcp_flags: u8, raw_frame: Vec<u8>) -> PacketInfo {
    PacketInfo {
        flow,
        src_addr: IpAddr::from([10, 0, 0, 1]),
        dst_addr: IpAddr::from([10, 0, 0, 2]),
        src_port: 12345,
        dst_port: 80,
        protocol: IpProtocol::Tcp,
        tcp_flags: Some(tcp_flags),
        payload_len: 0,
        raw_frame,
    }
}

// --- ActivatorRuntime tests ---

#[test]
fn runtime_loads_components() {
    let runtime = require_components();
    assert!(runtime.has_component("test-echo"));
    assert!(runtime.get_component("test-echo").is_some());
    let names: Vec<&str> = runtime.component_names().collect();
    assert!(names.contains(&"test-echo"));
}

#[test]
fn runtime_nonexistent_dir() {
    let dir = PathBuf::from("/tmp/distvirt-test-nonexistent-dir-12345");
    let runtime = ActivatorRuntime::new(&dir).expect("should succeed with missing dir");
    assert!(!runtime.has_component("anything"));
}

#[test]
fn runtime_empty_dir() {
    let dir = tempfile::tempdir().unwrap();
    let runtime = ActivatorRuntime::new(dir.path()).expect("should succeed with empty dir");
    assert!(!runtime.has_component("anything"));
    assert_eq!(runtime.component_names().count(), 0);
}

// --- ActivatorInstance tests (test-echo) ---

#[test]
fn instance_packet_roundtrip() {
    let runtime = require_components();
    let component = runtime.get_component("test-echo").unwrap();
    let mut inst = ActivatorInstance::new(runtime.engine(), component).unwrap();

    let raw = vec![0xDE, 0xAD];
    inst.push_event(Event::Packet(make_packet(42, raw)));
    let actions = inst.process_events().unwrap();

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
fn instance_backend_available_replays() {
    let runtime = require_components();
    let component = runtime.get_component("test-echo").unwrap();
    let mut inst = ActivatorInstance::new(runtime.engine(), component).unwrap();

    let raw = vec![0xCA, 0xFE];
    inst.push_event(Event::Packet(make_packet(1, raw.clone())));
    inst.process_events().unwrap();

    inst.push_event(Event::BackendAvailable(true));
    let actions = inst.process_events().unwrap();

    assert!(matches!(&actions[0], Action::Log(log) if log.message == "backend:available"));
    assert!(matches!(&actions[1], Action::ReplayPacket(data) if data == &raw));
}

#[test]
fn instance_backend_unavailable() {
    let runtime = require_components();
    let component = runtime.get_component("test-echo").unwrap();
    let mut inst = ActivatorInstance::new(runtime.engine(), component).unwrap();

    inst.push_event(Event::BackendAvailable(false));
    let actions = inst.process_events().unwrap();

    assert!(matches!(&actions[0], Action::Log(log) if log.message == "backend:unavailable"));
    assert!(matches!(
        &actions[1],
        Action::SetBackendNeed(BackendNeed::None)
    ));
}

#[test]
fn instance_tick() {
    let runtime = require_components();
    let component = runtime.get_component("test-echo").unwrap();
    let mut inst = ActivatorInstance::new(runtime.engine(), component).unwrap();

    inst.push_event(Event::Tick);
    let actions = inst.process_events().unwrap();

    assert_eq!(actions.len(), 1);
    assert!(
        matches!(&actions[0], Action::Log(log) if log.level == LogLevel::Debug && log.message == "tick")
    );
}

#[test]
fn instance_stream_open() {
    let runtime = require_components();
    let component = runtime.get_component("test-echo").unwrap();
    let mut inst = ActivatorInstance::new(runtime.engine(), component).unwrap();

    inst.push_event(Event::StreamOpen(7));
    let actions = inst.process_events().unwrap();

    assert!(matches!(&actions[0], Action::Log(log) if log.message == "stream-open:7"));
    assert!(matches!(
        &actions[1],
        Action::SetBackendNeed(BackendNeed::Active)
    ));
}

#[test]
fn instance_stream_data_echo() {
    let runtime = require_components();
    let component = runtime.get_component("test-echo").unwrap();
    let mut inst = ActivatorInstance::new(runtime.engine(), component).unwrap();

    let data = b"hello world".to_vec();
    inst.push_event(Event::StreamData {
        stream: 5,
        data: data.clone(),
    });
    let actions = inst.process_events().unwrap();

    assert_eq!(actions.len(), 1);
    assert!(matches!(&actions[0], Action::DownstreamSend { stream: 5, data: d } if d == &data));
}

#[test]
fn instance_stream_close() {
    let runtime = require_components();
    let component = runtime.get_component("test-echo").unwrap();
    let mut inst = ActivatorInstance::new(runtime.engine(), component).unwrap();

    inst.push_event(Event::StreamClose(3));
    let actions = inst.process_events().unwrap();

    assert_eq!(actions.len(), 1);
    assert!(matches!(&actions[0], Action::Log(log) if log.message == "stream-close:3"));
}

#[test]
fn instance_upstream_connect_ok() {
    let runtime = require_components();
    let component = runtime.get_component("test-echo").unwrap();
    let mut inst = ActivatorInstance::new(runtime.engine(), component).unwrap();

    inst.push_event(Event::UpstreamConnectResult {
        stream: 10,
        ok: true,
    });
    let actions = inst.process_events().unwrap();

    assert_eq!(actions.len(), 1);
    assert!(matches!(&actions[0], Action::UpstreamSend { stream: 10, data } if data == b"hello"));
}

#[test]
fn instance_upstream_connect_refused() {
    let runtime = require_components();
    let component = runtime.get_component("test-echo").unwrap();
    let mut inst = ActivatorInstance::new(runtime.engine(), component).unwrap();

    inst.push_event(Event::UpstreamConnectResult {
        stream: 10,
        ok: false,
    });
    let actions = inst.process_events().unwrap();

    assert_eq!(actions.len(), 1);
    assert!(
        matches!(&actions[0], Action::Log(log) if log.level == LogLevel::Warn && log.message == "upstream-failed:10")
    );
}

#[test]
fn instance_upstream_data_proxy() {
    let runtime = require_components();
    let component = runtime.get_component("test-echo").unwrap();
    let mut inst = ActivatorInstance::new(runtime.engine(), component).unwrap();

    let data = b"response".to_vec();
    inst.push_event(Event::UpstreamData {
        stream: 8,
        data: data.clone(),
    });
    let actions = inst.process_events().unwrap();

    assert_eq!(actions.len(), 1);
    assert!(matches!(&actions[0], Action::DownstreamSend { stream: 8, data: d } if d == &data));
}

#[test]
fn instance_upstream_close() {
    let runtime = require_components();
    let component = runtime.get_component("test-echo").unwrap();
    let mut inst = ActivatorInstance::new(runtime.engine(), component).unwrap();

    inst.push_event(Event::UpstreamClose(4));
    let actions = inst.process_events().unwrap();

    assert_eq!(actions.len(), 1);
    assert!(matches!(&actions[0], Action::Log(log) if log.message == "upstream-close:4"));
}

#[test]
fn instance_empty_queue() {
    let runtime = require_components();
    let component = runtime.get_component("test-echo").unwrap();
    let mut inst = ActivatorInstance::new(runtime.engine(), component).unwrap();

    let actions = inst.process_events().unwrap();
    assert!(actions.is_empty());
}

#[test]
fn instance_has_pending_events() {
    let runtime = require_components();
    let component = runtime.get_component("test-echo").unwrap();
    let mut inst = ActivatorInstance::new(runtime.engine(), component).unwrap();

    assert!(!inst.has_pending_events());
    inst.push_event(Event::Tick);
    assert!(inst.has_pending_events());
    inst.process_events().unwrap();
    assert!(!inst.has_pending_events());
}

#[test]
fn instance_backend_need_tracking() {
    let runtime = require_components();
    let component = runtime.get_component("test-echo").unwrap();
    let mut inst = ActivatorInstance::new(runtime.engine(), component).unwrap();

    assert_eq!(inst.backend_need(), BackendNeed::None);

    // Packet -> Traffic
    inst.push_event(Event::Packet(make_packet(1, vec![0x01])));
    inst.process_events().unwrap();
    assert_eq!(inst.backend_need(), BackendNeed::Traffic);

    // StreamOpen -> Active
    inst.push_event(Event::StreamOpen(1));
    inst.process_events().unwrap();
    assert_eq!(inst.backend_need(), BackendNeed::Active);

    // BackendAvailable(false) -> None
    inst.push_event(Event::BackendAvailable(false));
    inst.process_events().unwrap();
    assert_eq!(inst.backend_need(), BackendNeed::None);
}

#[test]
fn instance_batch_multiple_events() {
    let runtime = require_components();
    let component = runtime.get_component("test-echo").unwrap();
    let mut inst = ActivatorInstance::new(runtime.engine(), component).unwrap();

    inst.push_event(Event::Tick);
    inst.push_event(Event::StreamOpen(1));
    inst.push_event(Event::StreamClose(1));
    let actions = inst.process_events().unwrap();

    // Tick -> Log(Debug, "tick")
    // StreamOpen -> Log(Info, "stream-open:1") + SetBackendNeed(Active)
    // StreamClose -> Log(Info, "stream-close:1")
    assert_eq!(actions.len(), 4);
    assert!(matches!(&actions[0], Action::Log(log) if log.message == "tick"));
    assert!(matches!(&actions[1], Action::Log(log) if log.message == "stream-open:1"));
    assert!(matches!(
        &actions[2],
        Action::SetBackendNeed(BackendNeed::Active)
    ));
    assert!(matches!(&actions[3], Action::Log(log) if log.message == "stream-close:1"));
}

// --- Fuel exhaustion test (spin) ---

#[test]
fn instance_fuel_exhaustion() {
    let runtime = require_components();
    let component = runtime
        .get_component("spin")
        .expect("spin component not found");
    let mut inst = ActivatorInstance::new(runtime.engine(), component).unwrap();

    inst.push_event(Event::Tick);
    let result = inst.process_events();
    assert!(result.is_err(), "expected fuel exhaustion error");
}

// --- Fuel accumulation regression test ---

#[test]
fn instance_fuel_does_not_accumulate() {
    let runtime = require_components();
    let component = runtime.get_component("tcp").unwrap();
    let mut inst = ActivatorInstance::new(runtime.engine(), component).unwrap();

    // Call process_events 100 times with trivial events to let fuel "accumulate"
    // if the bug is present.
    for _ in 0..100 {
        inst.push_event(Event::Tick);
        inst.process_events().unwrap();
    }

    // Now load a spin component and verify it still traps — fuel didn't secretly grow.
    let spin_component = runtime
        .get_component("spin")
        .expect("spin component not found");
    let mut spin_inst = ActivatorInstance::new(runtime.engine(), spin_component).unwrap();
    spin_inst.push_event(Event::Tick);
    let result = spin_inst.process_events();
    assert!(
        result.is_err(),
        "spin should still trap after idle calls on another instance"
    );
}

// --- TCP activator tests ---

#[test]
fn tcp_syn_buffers_and_signals() {
    let runtime = require_components();
    let component = runtime
        .get_component("tcp")
        .expect("tcp component not found");
    let mut inst = ActivatorInstance::new(runtime.engine(), component).unwrap();

    let pkt = make_tcp_packet(1, 0x02, vec![0xAA]); // SYN
    inst.push_event(Event::Packet(pkt));
    let actions = inst.process_events().unwrap();

    assert!(matches!(
        &actions[0],
        Action::PacketDecision {
            flow: 1,
            decision: PacketDecision::Buffered
        }
    ));
    assert!(matches!(
        &actions[1],
        Action::SetBackendNeed(BackendNeed::Traffic)
    ));
}

#[test]
fn tcp_syn_retransmit_no_signal() {
    let runtime = require_components();
    let component = runtime.get_component("tcp").unwrap();
    let mut inst = ActivatorInstance::new(runtime.engine(), component).unwrap();

    // First SYN
    inst.push_event(Event::Packet(make_tcp_packet(1, 0x02, vec![0xAA])));
    inst.process_events().unwrap();

    // SYN retransmit (same flow)
    inst.push_event(Event::Packet(make_tcp_packet(1, 0x02, vec![0xBB])));
    let actions = inst.process_events().unwrap();

    assert_eq!(actions.len(), 1);
    assert!(matches!(
        &actions[0],
        Action::PacketDecision {
            flow: 1,
            decision: PacketDecision::Buffered
        }
    ));
}

#[test]
fn tcp_rst_dropped() {
    let runtime = require_components();
    let component = runtime.get_component("tcp").unwrap();
    let mut inst = ActivatorInstance::new(runtime.engine(), component).unwrap();

    inst.push_event(Event::Packet(make_tcp_packet(1, 0x04, vec![0xCC]))); // RST
    let actions = inst.process_events().unwrap();

    assert_eq!(actions.len(), 1);
    assert!(matches!(
        &actions[0],
        Action::PacketDecision {
            flow: 1,
            decision: PacketDecision::Drop
        }
    ));
}

#[test]
fn tcp_non_syn_known_flow() {
    let runtime = require_components();
    let component = runtime.get_component("tcp").unwrap();
    let mut inst = ActivatorInstance::new(runtime.engine(), component).unwrap();

    // Establish flow with SYN
    inst.push_event(Event::Packet(make_tcp_packet(1, 0x02, vec![0xAA])));
    inst.process_events().unwrap();

    // ACK on known flow
    inst.push_event(Event::Packet(make_tcp_packet(1, 0x10, vec![0xDD]))); // ACK
    let actions = inst.process_events().unwrap();

    assert_eq!(actions.len(), 1);
    assert!(matches!(
        &actions[0],
        Action::PacketDecision {
            flow: 1,
            decision: PacketDecision::Buffered
        }
    ));
}

#[test]
fn tcp_non_syn_unknown_flow_signals() {
    let runtime = require_components();
    let component = runtime.get_component("tcp").unwrap();
    let mut inst = ActivatorInstance::new(runtime.engine(), component).unwrap();

    // ACK on unknown flow
    inst.push_event(Event::Packet(make_tcp_packet(99, 0x10, vec![0xEE])));
    let actions = inst.process_events().unwrap();

    assert_eq!(actions.len(), 2);
    assert!(matches!(
        &actions[0],
        Action::PacketDecision {
            flow: 99,
            decision: PacketDecision::Buffered
        }
    ));
    assert!(matches!(
        &actions[1],
        Action::SetBackendNeed(BackendNeed::Traffic)
    ));
}

#[test]
fn tcp_backend_available_replays_syns() {
    let runtime = require_components();
    let component = runtime.get_component("tcp").unwrap();
    let mut inst = ActivatorInstance::new(runtime.engine(), component).unwrap();

    let raw1 = vec![0x01, 0x02];
    let raw2 = vec![0x03, 0x04];

    inst.push_event(Event::Packet(make_tcp_packet(1, 0x02, raw1.clone())));
    inst.push_event(Event::Packet(make_tcp_packet(2, 0x02, raw2.clone())));
    inst.process_events().unwrap();

    inst.push_event(Event::BackendAvailable(true));
    let actions = inst.process_events().unwrap();

    // Should have 2 ReplayPacket actions (order may vary due to HashMap)
    let replay_actions: Vec<&Action> = actions
        .iter()
        .filter(|a| matches!(a, Action::ReplayPacket(_)))
        .collect();
    assert_eq!(replay_actions.len(), 2);

    let mut replayed: Vec<Vec<u8>> = replay_actions
        .iter()
        .map(|a| match a {
            Action::ReplayPacket(d) => d.clone(),
            _ => unreachable!(),
        })
        .collect();
    replayed.sort();
    let mut expected = vec![raw1, raw2];
    expected.sort();
    assert_eq!(replayed, expected);
}

#[test]
fn tcp_replay_clears_state() {
    let runtime = require_components();
    let component = runtime.get_component("tcp").unwrap();
    let mut inst = ActivatorInstance::new(runtime.engine(), component).unwrap();

    // SYN, replay, then same flow SYN should be treated as new
    inst.push_event(Event::Packet(make_tcp_packet(1, 0x02, vec![0xAA])));
    inst.process_events().unwrap();

    inst.push_event(Event::BackendAvailable(true));
    inst.process_events().unwrap();

    // Same flow SYN again — should be new (signals Traffic)
    inst.push_event(Event::Packet(make_tcp_packet(1, 0x02, vec![0xBB])));
    let actions = inst.process_events().unwrap();

    assert_eq!(actions.len(), 2);
    assert!(matches!(
        &actions[1],
        Action::SetBackendNeed(BackendNeed::Traffic)
    ));
}

// --- HTTP/2 activator tests ---

fn get_http2_instance() -> ActivatorInstance {
    let runtime = require_components();
    let component = runtime
        .get_component("http2")
        .expect("http2 component not found — run ./activators/build.sh first");
    ActivatorInstance::new(runtime.engine(), component).unwrap()
}

/// Extract all downstream-send data for a given stream from host actions.
fn downstream_data_for(actions: &[Action], stream: u64) -> Vec<u8> {
    let mut result = Vec::new();
    for action in actions {
        if let Action::DownstreamSend { stream: s, data } = action {
            if *s == stream {
                result.extend_from_slice(data);
            }
        }
    }
    result
}

/// Send H2 preface + client SETTINGS, return actions from the activator.
fn do_h2_handshake(inst: &mut ActivatorInstance, stream: u64) -> Vec<Action> {
    // Open the TCP stream
    inst.push_event(Event::StreamOpen(stream));
    inst.process_events().unwrap();

    // Send preface + empty SETTINGS
    let mut data = H2_PREFACE.to_vec();
    data.extend_from_slice(&h2_settings(&[]));
    inst.push_event(Event::StreamData { stream, data });
    let actions = inst.process_events().unwrap();
    actions
}

#[test]
fn h2_preface_and_settings_exchange() {
    let mut inst = get_http2_instance();
    let actions = do_h2_handshake(&mut inst, 1);

    // Should get DownstreamSend with our SETTINGS + SETTINGS ACK
    let data = downstream_data_for(&actions, 1);
    assert!(
        data.len() >= 9,
        "expected at least one frame in response to preface"
    );

    let frames = collect_frames(&data);
    // First frame: our SETTINGS (type=0x4, no ACK flag)
    assert_eq!(frames[0].0, 0x4, "first frame should be SETTINGS");
    assert_eq!(frames[0].1 & 0x01, 0, "should not be ACK");

    // Verify HEADER_TABLE_SIZE=0 is in our settings
    let params = parse_settings_payload(&frames[0].3);
    assert!(
        params.iter().any(|&(id, val)| id == 0x1 && val == 0),
        "HEADER_TABLE_SIZE should be 0"
    );

    // Second frame: SETTINGS ACK (type=0x4, ACK flag)
    assert_eq!(frames[1].0, 0x4, "second frame should be SETTINGS");
    assert_eq!(frames[1].1 & 0x01, 1, "should be ACK");
    assert_eq!(frames[1].3.len(), 0, "ACK payload should be empty");
}

#[test]
fn h2_ping_handling() {
    let mut inst = get_http2_instance();
    do_h2_handshake(&mut inst, 1);

    // Send PING
    let opaque: [u8; 8] = [0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08];
    inst.push_event(Event::StreamData {
        stream: 1,
        data: h2_ping(&opaque),
    });
    let actions = inst.process_events().unwrap();

    let data = downstream_data_for(&actions, 1);
    let frames = collect_frames(&data);
    assert_eq!(frames.len(), 1, "should get exactly one PING ACK");
    assert_eq!(frames[0].0, 0x6, "should be PING frame");
    assert_eq!(frames[0].1 & 0x01, 1, "should be ACK");
    assert_eq!(frames[0].3, opaque, "should echo opaque bytes");
}

#[test]
fn h2_headers_signals_backend_need_active() {
    let mut inst = get_http2_instance();
    do_h2_handshake(&mut inst, 1);

    // Send HEADERS for stream 1 (new H2 stream)
    let header_block = vec![0x82]; // minimal pseudo-header
    inst.push_event(Event::StreamData {
        stream: 1,
        data: h2_headers(1, false, &header_block),
    });
    let actions = inst.process_events().unwrap();

    assert!(
        actions
            .iter()
            .any(|a| matches!(a, Action::SetBackendNeed(BackendNeed::Active))),
        "should signal backend need active on first H2 stream"
    );
}

#[test]
fn h2_buffering_while_no_backend() {
    let mut inst = get_http2_instance();
    do_h2_handshake(&mut inst, 1);

    // Send HEADERS + DATA without backend available
    let header_block = vec![0x82];
    let mut client_data = h2_headers(1, false, &header_block);
    client_data.extend_from_slice(&h2_data(1, false, b"request body"));
    inst.push_event(Event::StreamData {
        stream: 1,
        data: client_data,
    });
    let actions = inst.process_events().unwrap();

    // Should NOT have any UpstreamSend (no backend connected)
    assert!(
        !actions
            .iter()
            .any(|a| matches!(a, Action::UpstreamSend { .. })),
        "should not send upstream without backend"
    );

    // Should NOT have UpstreamConnect (backend not available yet)
    assert!(
        !actions
            .iter()
            .any(|a| matches!(a, Action::UpstreamConnect { .. })),
        "should not connect upstream without backend available"
    );
}

#[test]
fn h2_upstream_connect_on_backend_available() {
    let mut inst = get_http2_instance();
    do_h2_handshake(&mut inst, 1);

    // Send HEADERS to create buffered frames
    let header_block = vec![0x82];
    inst.push_event(Event::StreamData {
        stream: 1,
        data: h2_headers(1, false, &header_block),
    });
    inst.process_events().unwrap();

    // Now backend becomes available
    inst.push_event(Event::BackendAvailable(true));
    let actions = inst.process_events().unwrap();

    assert!(
        actions
            .iter()
            .any(|a| matches!(a, Action::UpstreamConnect { port: 80 })),
        "should issue upstream-connect(80) when backend becomes available"
    );
}

#[test]
fn h2_upstream_connect_result_sends_preface_and_buffered() {
    let mut inst = get_http2_instance();
    do_h2_handshake(&mut inst, 1);

    // Buffer some frames
    let header_block = vec![0x82];
    inst.push_event(Event::StreamData {
        stream: 1,
        data: h2_headers(1, false, &header_block),
    });
    inst.process_events().unwrap();

    // Backend available -> upstream connect
    inst.push_event(Event::BackendAvailable(true));
    inst.process_events().unwrap();

    // Upstream connect succeeds
    inst.push_event(Event::UpstreamConnectResult {
        stream: 100, // upstream handle
        ok: true,
    });
    let actions = inst.process_events().unwrap();

    // Should send H2 preface + SETTINGS + buffered frames to upstream
    let upstream_sends: Vec<&Vec<u8>> = actions
        .iter()
        .filter_map(|a| match a {
            Action::UpstreamSend { stream: 100, data } => Some(data),
            _ => None,
        })
        .collect();

    assert!(
        !upstream_sends.is_empty(),
        "should send data to upstream after connect"
    );

    // First upstream send should start with H2 preface
    let first_send = &upstream_sends[0];
    assert!(
        first_send.starts_with(H2_PREFACE),
        "first upstream send should start with H2 preface"
    );
}

#[test]
fn h2_bidirectional_proxy() {
    let mut inst = get_http2_instance();
    do_h2_handshake(&mut inst, 1);

    // Send HEADERS
    let header_block = vec![0x82];
    inst.push_event(Event::StreamData {
        stream: 1,
        data: h2_headers(1, false, &header_block),
    });
    inst.process_events().unwrap();

    // Backend available + connect
    inst.push_event(Event::BackendAvailable(true));
    inst.process_events().unwrap();

    inst.push_event(Event::UpstreamConnectResult {
        stream: 100,
        ok: true,
    });
    inst.process_events().unwrap();

    // Now send backend SETTINGS (non-ACK) to trigger upstream to become Ready
    inst.push_event(Event::UpstreamData {
        stream: 100,
        data: h2_settings(&[]),
    });
    inst.process_events().unwrap();

    // Client sends DATA -> should go to upstream
    inst.push_event(Event::StreamData {
        stream: 1,
        data: h2_data(1, false, b"client payload"),
    });
    let actions = inst.process_events().unwrap();

    assert!(
        actions
            .iter()
            .any(|a| matches!(a, Action::UpstreamSend { stream: 100, .. })),
        "client data should be forwarded to upstream"
    );

    // Upstream sends response DATA -> should go to client
    inst.push_event(Event::UpstreamData {
        stream: 100,
        data: h2_data(1, false, b"backend response"),
    });
    let actions = inst.process_events().unwrap();

    assert!(
        actions
            .iter()
            .any(|a| matches!(a, Action::DownstreamSend { stream: 1, .. })),
        "upstream data should be forwarded to client"
    );
}

#[test]
fn h2_stream_lifecycle_backend_need_none() {
    let mut inst = get_http2_instance();
    do_h2_handshake(&mut inst, 1);

    // Send HEADERS with END_STREAM (client half-close)
    let header_block = vec![0x82];
    inst.push_event(Event::StreamData {
        stream: 1,
        data: h2_headers(1, true, &header_block),
    });
    inst.process_events().unwrap();

    // Backend available + connect + connect result
    inst.push_event(Event::BackendAvailable(true));
    inst.process_events().unwrap();

    inst.push_event(Event::UpstreamConnectResult {
        stream: 100,
        ok: true,
    });
    inst.process_events().unwrap();

    // Backend sends response HEADERS with END_STREAM (backend half-close)
    // First make upstream Ready by sending backend SETTINGS
    inst.push_event(Event::UpstreamData {
        stream: 100,
        data: h2_settings(&[]),
    });
    inst.process_events().unwrap();

    // Backend response with END_STREAM -> stream fully closed
    inst.push_event(Event::UpstreamData {
        stream: 100,
        data: h2_headers(1, true, &[0x88]),
    });
    let actions = inst.process_events().unwrap();

    // Should signal backend need none since all H2 streams are closed
    assert!(
        actions
            .iter()
            .any(|a| matches!(a, Action::SetBackendNeed(BackendNeed::None))),
        "should signal backend need none when last H2 stream closes"
    );
}

#[test]
fn h2_upstream_connect_refused() {
    let mut inst = get_http2_instance();
    do_h2_handshake(&mut inst, 1);

    // Buffer frames
    let header_block = vec![0x82];
    inst.push_event(Event::StreamData {
        stream: 1,
        data: h2_headers(1, false, &header_block),
    });
    inst.process_events().unwrap();

    inst.push_event(Event::BackendAvailable(true));
    inst.process_events().unwrap();

    // Connect refused
    inst.push_event(Event::UpstreamConnectResult {
        stream: 100,
        ok: false,
    });
    let actions = inst.process_events().unwrap();

    // Should send GOAWAY and close downstream
    let data = downstream_data_for(&actions, 1);
    if !data.is_empty() {
        let frames = collect_frames(&data);
        assert!(
            frames.iter().any(|f| f.0 == 0x7), // GOAWAY
            "should send GOAWAY on connect failure"
        );
    }
    assert!(
        actions
            .iter()
            .any(|a| matches!(a, Action::DownstreamClose(1))),
        "should close downstream on connect failure"
    );
}

#[test]
fn h2_window_update_on_data() {
    let mut inst = get_http2_instance();
    do_h2_handshake(&mut inst, 1);

    // Send HEADERS then DATA
    let header_block = vec![0x82];
    inst.push_event(Event::StreamData {
        stream: 1,
        data: h2_headers(1, false, &header_block),
    });
    inst.process_events().unwrap();

    let payload = b"hello world data";
    inst.push_event(Event::StreamData {
        stream: 1,
        data: h2_data(1, false, payload),
    });
    let actions = inst.process_events().unwrap();

    // Should get WINDOW_UPDATE frames back to client
    let data = downstream_data_for(&actions, 1);
    let frames = collect_frames(&data);

    let window_updates: Vec<_> = frames.iter().filter(|f| f.0 == 0x8).collect();
    assert!(
        window_updates.len() >= 2,
        "should send connection-level and stream-level WINDOW_UPDATEs, got {}",
        window_updates.len()
    );

    // Connection-level (stream_id=0) WINDOW_UPDATE
    assert!(
        window_updates.iter().any(|f| f.2 == 0),
        "should have connection-level WINDOW_UPDATE"
    );
    // Stream-level WINDOW_UPDATE
    assert!(
        window_updates.iter().any(|f| f.2 == 1),
        "should have stream-level WINDOW_UPDATE"
    );
}
