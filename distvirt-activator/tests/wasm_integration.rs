//! Integration tests for ActivatorRuntime and ActivatorInstance using WASM components.

use std::net::IpAddr;
use std::path::PathBuf;

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
    assert!(matches!(&actions[2], Action::Log(log) if log.level == LogLevel::Info && log.message == "packet:42"));
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
    assert!(matches!(&actions[0], Action::Log(log) if log.level == LogLevel::Debug && log.message == "tick"));
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
    assert!(
        matches!(&actions[0], Action::DownstreamSend { stream: 5, data: d } if d == &data)
    );
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
    assert!(
        matches!(&actions[0], Action::UpstreamSend { stream: 10, data } if data == b"hello")
    );
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
    assert!(matches!(&actions[0], Action::Log(log) if log.level == LogLevel::Warn && log.message == "upstream-failed:10"));
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
    assert!(
        matches!(&actions[0], Action::DownstreamSend { stream: 8, data: d } if d == &data)
    );
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

    // Packet → Traffic
    inst.push_event(Event::Packet(make_packet(1, vec![0x01])));
    inst.process_events().unwrap();
    assert_eq!(inst.backend_need(), BackendNeed::Traffic);

    // StreamOpen → Active
    inst.push_event(Event::StreamOpen(1));
    inst.process_events().unwrap();
    assert_eq!(inst.backend_need(), BackendNeed::Active);

    // BackendAvailable(false) → None
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

    // Tick → Log(Debug, "tick")
    // StreamOpen → Log(Info, "stream-open:1") + SetBackendNeed(Active)
    // StreamClose → Log(Info, "stream-close:1")
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
    assert!(result.is_err(), "spin should still trap after idle calls on another instance");
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
