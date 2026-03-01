use activator_types::test_helpers::*;
use activator_types::*;
use tcp_activator::core::TcpActivator;

fn new_tcp() -> TcpActivator {
    TcpActivator::new()
}

#[test]
fn syn_buffers_and_signals() {
    let mut tcp = new_tcp();
    let pkt = make_tcp_packet(1, 0x02, vec![0xAA]); // SYN
    let actions = tcp.process_events(vec![Event::Packet(pkt)]);

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
fn syn_retransmit_no_signal() {
    let mut tcp = new_tcp();

    // First SYN
    tcp.process_events(vec![Event::Packet(make_tcp_packet(1, 0x02, vec![0xAA]))]);

    // SYN retransmit (same flow)
    let actions = tcp.process_events(vec![Event::Packet(make_tcp_packet(1, 0x02, vec![0xBB]))]);

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
fn rst_dropped() {
    let mut tcp = new_tcp();
    let actions = tcp.process_events(vec![Event::Packet(make_tcp_packet(1, 0x04, vec![0xCC]))]); // RST

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
fn non_syn_known_flow() {
    let mut tcp = new_tcp();

    // Establish flow with SYN
    tcp.process_events(vec![Event::Packet(make_tcp_packet(1, 0x02, vec![0xAA]))]);

    // ACK on known flow
    let actions = tcp.process_events(vec![Event::Packet(make_tcp_packet(1, 0x10, vec![0xDD]))]); // ACK

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
fn non_syn_unknown_flow_signals() {
    let mut tcp = new_tcp();

    // ACK on unknown flow
    let actions = tcp.process_events(vec![Event::Packet(make_tcp_packet(99, 0x10, vec![0xEE]))]);

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
fn backend_available_replays_syns() {
    let mut tcp = new_tcp();
    let raw1 = vec![0x01, 0x02];
    let raw2 = vec![0x03, 0x04];

    tcp.process_events(vec![
        Event::Packet(make_tcp_packet(1, 0x02, raw1.clone())),
        Event::Packet(make_tcp_packet(2, 0x02, raw2.clone())),
    ]);

    let actions = tcp.process_events(vec![Event::BackendAvailable(true)]);

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
fn replay_clears_state() {
    let mut tcp = new_tcp();

    // SYN, replay, then same flow SYN should be treated as new
    tcp.process_events(vec![Event::Packet(make_tcp_packet(1, 0x02, vec![0xAA]))]);
    tcp.process_events(vec![Event::BackendAvailable(true)]);

    // Same flow SYN again — should be new (signals Traffic)
    let actions = tcp.process_events(vec![Event::Packet(make_tcp_packet(1, 0x02, vec![0xBB]))]);

    assert_eq!(actions.len(), 2);
    assert!(matches!(
        &actions[1],
        Action::SetBackendNeed(BackendNeed::Traffic)
    ));
}
