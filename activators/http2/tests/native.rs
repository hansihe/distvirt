use activator_types::test_helpers::*;
use activator_types::*;
use http2_activator::core::Http2Activator;

fn new_http2() -> Http2Activator {
    Http2Activator::new()
}

/// Send H2 preface + client SETTINGS, return actions from the activator.
fn do_h2_handshake(inst: &mut Http2Activator, stream: u64) -> Vec<Action> {
    // Open the TCP stream
    inst.process_events(vec![Event::StreamOpen(stream)]);

    // Send preface + empty SETTINGS
    let mut data = H2_PREFACE.to_vec();
    data.extend_from_slice(&h2_settings(&[]));
    inst.process_events(vec![Event::StreamData { stream, data }])
}

#[test]
fn preface_and_settings_exchange() {
    let mut inst = new_http2();
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
fn ping_handling() {
    let mut inst = new_http2();
    do_h2_handshake(&mut inst, 1);

    // Send PING
    let opaque: [u8; 8] = [0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08];
    let actions = inst.process_events(vec![Event::StreamData {
        stream: 1,
        data: h2_ping(&opaque),
    }]);

    let data = downstream_data_for(&actions, 1);
    let frames = collect_frames(&data);
    assert_eq!(frames.len(), 1, "should get exactly one PING ACK");
    assert_eq!(frames[0].0, 0x6, "should be PING frame");
    assert_eq!(frames[0].1 & 0x01, 1, "should be ACK");
    assert_eq!(frames[0].3, opaque, "should echo opaque bytes");
}

#[test]
fn headers_signals_backend_need_active() {
    let mut inst = new_http2();
    do_h2_handshake(&mut inst, 1);

    // Send HEADERS for stream 1 (new H2 stream)
    let header_block = vec![0x82]; // minimal pseudo-header
    let actions = inst.process_events(vec![Event::StreamData {
        stream: 1,
        data: h2_headers(1, false, &header_block),
    }]);

    assert!(
        actions
            .iter()
            .any(|a| matches!(a, Action::SetBackendNeed(BackendNeed::Active))),
        "should signal backend need active on first H2 stream"
    );
}

#[test]
fn buffering_while_no_backend() {
    let mut inst = new_http2();
    do_h2_handshake(&mut inst, 1);

    // Send HEADERS + DATA without backend available
    let header_block = vec![0x82];
    let mut client_data = h2_headers(1, false, &header_block);
    client_data.extend_from_slice(&h2_data(1, false, b"request body"));
    let actions = inst.process_events(vec![Event::StreamData {
        stream: 1,
        data: client_data,
    }]);

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
fn upstream_connect_on_backend_available() {
    let mut inst = new_http2();
    do_h2_handshake(&mut inst, 1);

    // Send HEADERS to create buffered frames
    let header_block = vec![0x82];
    inst.process_events(vec![Event::StreamData {
        stream: 1,
        data: h2_headers(1, false, &header_block),
    }]);

    // Now backend becomes available
    let actions = inst.process_events(vec![Event::BackendAvailable(true)]);

    assert!(
        actions
            .iter()
            .any(|a| matches!(a, Action::UpstreamConnect { port: 80 })),
        "should issue upstream-connect(80) when backend becomes available"
    );
}

#[test]
fn upstream_connect_result_sends_preface_and_buffered() {
    let mut inst = new_http2();
    do_h2_handshake(&mut inst, 1);

    // Buffer some frames
    let header_block = vec![0x82];
    inst.process_events(vec![Event::StreamData {
        stream: 1,
        data: h2_headers(1, false, &header_block),
    }]);

    // Backend available -> upstream connect
    inst.process_events(vec![Event::BackendAvailable(true)]);

    // Upstream connect succeeds
    let actions = inst.process_events(vec![Event::UpstreamConnectResult {
        stream: 100, // upstream handle
        ok: true,
    }]);

    // Should send H2 preface + SETTINGS + buffered frames to upstream
    let upstream_sends: Vec<&Vec<u8>> = actions
        .iter()
        .filter_map(|a| match a {
            Action::UpstreamSend {
                stream: 100, data, ..
            } => Some(data),
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
fn bidirectional_proxy() {
    let mut inst = new_http2();
    do_h2_handshake(&mut inst, 1);

    // Send HEADERS
    let header_block = vec![0x82];
    inst.process_events(vec![Event::StreamData {
        stream: 1,
        data: h2_headers(1, false, &header_block),
    }]);

    // Backend available + connect
    inst.process_events(vec![Event::BackendAvailable(true)]);

    inst.process_events(vec![Event::UpstreamConnectResult {
        stream: 100,
        ok: true,
    }]);

    // Now send backend SETTINGS (non-ACK) to trigger upstream to become Ready
    inst.process_events(vec![Event::UpstreamData {
        stream: 100,
        data: h2_settings(&[]),
    }]);

    // Client sends DATA -> should go to upstream
    let actions = inst.process_events(vec![Event::StreamData {
        stream: 1,
        data: h2_data(1, false, b"client payload"),
    }]);

    assert!(
        actions
            .iter()
            .any(|a| matches!(a, Action::UpstreamSend { stream: 100, .. })),
        "client data should be forwarded to upstream"
    );

    // Upstream sends response DATA -> should go to client
    let actions = inst.process_events(vec![Event::UpstreamData {
        stream: 100,
        data: h2_data(1, false, b"backend response"),
    }]);

    assert!(
        actions
            .iter()
            .any(|a| matches!(a, Action::DownstreamSend { stream: 1, .. })),
        "upstream data should be forwarded to client"
    );
}

#[test]
fn stream_lifecycle_backend_need_none() {
    let mut inst = new_http2();
    do_h2_handshake(&mut inst, 1);

    // Send HEADERS with END_STREAM (client half-close)
    let header_block = vec![0x82];
    inst.process_events(vec![Event::StreamData {
        stream: 1,
        data: h2_headers(1, true, &header_block),
    }]);

    // Backend available + connect + connect result
    inst.process_events(vec![Event::BackendAvailable(true)]);

    inst.process_events(vec![Event::UpstreamConnectResult {
        stream: 100,
        ok: true,
    }]);

    // Backend sends SETTINGS to become Ready
    inst.process_events(vec![Event::UpstreamData {
        stream: 100,
        data: h2_settings(&[]),
    }]);

    // Backend response with END_STREAM -> stream fully closed
    let actions = inst.process_events(vec![Event::UpstreamData {
        stream: 100,
        data: h2_headers(1, true, &[0x88]),
    }]);

    // Should signal backend need none since all H2 streams are closed
    assert!(
        actions
            .iter()
            .any(|a| matches!(a, Action::SetBackendNeed(BackendNeed::None))),
        "should signal backend need none when last H2 stream closes"
    );
}

#[test]
fn upstream_connect_refused() {
    let mut inst = new_http2();
    do_h2_handshake(&mut inst, 1);

    // Buffer frames
    let header_block = vec![0x82];
    inst.process_events(vec![Event::StreamData {
        stream: 1,
        data: h2_headers(1, false, &header_block),
    }]);

    inst.process_events(vec![Event::BackendAvailable(true)]);

    // Connect refused
    let actions = inst.process_events(vec![Event::UpstreamConnectResult {
        stream: 100,
        ok: false,
    }]);

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
fn window_update_on_data() {
    let mut inst = new_http2();
    do_h2_handshake(&mut inst, 1);

    // Send HEADERS then DATA
    let header_block = vec![0x82];
    inst.process_events(vec![Event::StreamData {
        stream: 1,
        data: h2_headers(1, false, &header_block),
    }]);

    let payload = b"hello world data";
    let actions = inst.process_events(vec![Event::StreamData {
        stream: 1,
        data: h2_data(1, false, payload),
    }]);

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
