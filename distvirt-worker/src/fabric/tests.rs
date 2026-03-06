use super::*;
use super::service_activator::ServiceProcessor;
use crate::packet::{FabricPacket, FABRIC_HDR_SZ, with_fabric_header};
use std::net::Ipv4Addr;
use tokio::sync::mpsc as tokio_mpsc;

/// Channel-backed test double for FramePort.
struct TestPort {
    /// Test injects frames here; recv_frame reads from this.
    rx: tokio::sync::Mutex<tokio_mpsc::Receiver<Vec<u8>>>,
    /// send_frame writes here; test reads captured frames from tx_out.
    tx: tokio_mpsc::Sender<Vec<u8>>,
}

struct TestPortHandle {
    /// Send frames into the port (simulates wire ingress).
    inject_tx: tokio_mpsc::Sender<Vec<u8>>,
    /// Receive frames that the fabric sent to this port.
    capture_rx: tokio::sync::Mutex<tokio_mpsc::Receiver<Vec<u8>>>,
}

fn make_test_port() -> (TestPort, TestPortHandle) {
    let (inject_tx, inject_rx) = tokio_mpsc::channel(64);
    let (capture_tx, capture_rx) = tokio_mpsc::channel(64);
    (
        TestPort {
            rx: tokio::sync::Mutex::new(inject_rx),
            tx: capture_tx,
        },
        TestPortHandle {
            inject_tx,
            capture_rx: tokio::sync::Mutex::new(capture_rx),
        },
    )
}

impl FramePort for TestPort {
    async fn recv_frame(&self, buf: &mut [u8]) -> std::io::Result<usize> {
        let mut rx = self.rx.lock().await;
        match rx.recv().await {
            Some(data) => {
                let len = data.len().min(buf.len());
                buf[..len].copy_from_slice(&data[..len]);
                Ok(len)
            }
            None => Ok(0), // EOF
        }
    }

    async fn send_frame(&self, buf: &[u8]) -> std::io::Result<usize> {
        self.tx
            .send(buf.to_vec())
            .await
            .map_err(|_| std::io::Error::new(std::io::ErrorKind::BrokenPipe, "channel closed"))?;
        Ok(buf.len())
    }
}

/// Helper: try to receive a frame with a timeout. Returns None if no frame arrives.
async fn try_recv(handle: &TestPortHandle) -> Option<Vec<u8>> {
    let mut rx = handle.capture_rx.lock().await;
    tokio::time::timeout(std::time::Duration::from_millis(100), rx.recv())
        .await
        .ok()
        .flatten()
}

/// Helper: assert no frame arrives within timeout.
async fn assert_no_frame(handle: &TestPortHandle) {
    assert!(try_recv(handle).await.is_none(), "expected no frame but got one");
}

// Test subnet
const TEST_SUBNET: Ipv4Addr = Ipv4Addr::new(10, 0, 0, 0);
const TEST_PREFIX: u8 = 24;
// Test IP addresses (in subnet)
const IP_A: Ipv4Addr = Ipv4Addr::new(10, 0, 0, 10);
const IP_B: Ipv4Addr = Ipv4Addr::new(10, 0, 0, 11);
const IP_C: Ipv4Addr = Ipv4Addr::new(10, 0, 0, 12);

// External IP (outside subnet, goes to gateway)
const EXTERNAL_IP: Ipv4Addr = Ipv4Addr::new(8, 8, 8, 8);

fn make_test_fabric() -> Fabric<TestPort> {
    Fabric::new(TEST_SUBNET, TEST_PREFIX)
}

// --- L3 frame helpers ---

/// Build a valid L3 fabric frame: [fabric_hdr(3)][ip_hdr(20)]
/// with specific src and dst IP.
fn make_ipv4_frame_full(src_ip: Ipv4Addr, dst_ip: Ipv4Addr) -> Vec<u8> {
    let mut ip_hdr = [0u8; 20];
    ip_hdr[0] = 0x45; // version=4, IHL=5
    ip_hdr[2..4].copy_from_slice(&20u16.to_be_bytes()); // total length
    ip_hdr[12..16].copy_from_slice(&src_ip.octets());
    ip_hdr[16..20].copy_from_slice(&dst_ip.octets());
    with_fabric_header(0, 0, &ip_hdr)
}

/// Convenience wrapper: build IPv4 frame with default src IP (10.0.0.1).
fn make_ipv4_frame(dst_ip: Ipv4Addr) -> Vec<u8> {
    make_ipv4_frame_full(Ipv4Addr::new(10, 0, 0, 1), dst_ip)
}

/// Build a valid IPv4+TCP fabric frame [fabric_hdr(3)][IP+TCP].
/// Uses etherparse::PacketBuilder for correct headers, then overwrites TCP flags.
fn make_tcp_frame(
    src_ip: [u8; 4],
    dst_ip: [u8; 4],
    src_port: u16,
    dst_port: u16,
    tcp_flags: u8,
) -> Vec<u8> {
    use etherparse::PacketBuilder;

    let builder = PacketBuilder::ipv4(src_ip, dst_ip, 64)
        .tcp(src_port, dst_port, 1000, 65535);

    let mut ip_packet = Vec::new();
    builder.write(&mut ip_packet, &[]).unwrap();

    // Overwrite TCP flags: ip(20) + tcp flags at byte 13
    let tcp_start = 20;
    ip_packet[tcp_start + 13] = tcp_flags;

    with_fabric_header(0, 0, &ip_packet)
}

/// Helper: try to receive a FabricEvent with timeout.
async fn try_recv_event(rx: &mut tokio_mpsc::Receiver<FabricEvent>) -> Option<FabricEvent> {
    tokio::time::timeout(std::time::Duration::from_millis(200), rx.recv())
        .await
        .ok()
        .flatten()
}

// --- L3 routing tests ---

#[tokio::test]
async fn ipv4_frame_routes_to_correct_port_by_ip() {
    let fabric = make_test_fabric();
    let (port0, handle0) = make_test_port();
    let (port1, handle1) = make_test_port();

    let (_id0, _task0) = fabric.add_port_raw_with_ip(port0, IP_A);
    let (_id1, _task1) = fabric.add_port_raw_with_ip(port1, IP_B);

    // Port 0 sends IPv4 frame to IP_B → should go to port 1.
    let frame = make_ipv4_frame(IP_B);
    handle0.inject_tx.send(frame).await.unwrap();

    let received = try_recv(&handle1).await;
    assert!(received.is_some(), "port 1 should receive frame destined to IP_B");

    // Verify it's a valid fabric packet with the right dst IP.
    let received = received.unwrap();
    let fp = FabricPacket::new(&received).unwrap();
    assert_eq!(fp.ipv4_dst(), IP_B, "dst IP should be IP_B");
}

#[tokio::test]
async fn unknown_in_subnet_ip_dropped() {
    let fabric = make_test_fabric();
    let (port0, handle0) = make_test_port();
    let (port1, handle1) = make_test_port();

    let (_id0, _task0) = fabric.add_port_raw_with_ip(port0, IP_A);
    let (_id1, _task1) = fabric.add_port_raw_with_ip(port1, IP_B);

    // Port 0 sends IPv4 frame to IP_C (in subnet but no port registered) → should be dropped.
    let frame = make_ipv4_frame(IP_C);
    handle0.inject_tx.send(frame).await.unwrap();

    // Neither port should get the frame (no flooding in L3 mode).
    assert_no_frame(&handle1).await;
}

#[tokio::test]
async fn frame_to_own_port_ip_is_delivered() {
    // In L3 mode, frames are routed by dst IP. A frame from port 0 to
    // port 0's own IP is delivered back to port 0 (hairpin is valid).
    let fabric = make_test_fabric();
    let (port0, handle0) = make_test_port();
    let (port1, _handle1) = make_test_port();

    let (_id0, _task0) = fabric.add_port_raw_with_ip(port0, IP_A);
    let (_id1, _task1) = fabric.add_port_raw_with_ip(port1, IP_B);

    let frame = make_ipv4_frame(IP_A);
    handle0.inject_tx.send(frame).await.unwrap();

    // In L3 mode, this is delivered to port 0 since IP_A resolves there.
    let received = try_recv(&handle0).await;
    assert!(received.is_some(), "frame should be delivered back to port 0 (hairpin)");
}

// --- Gateway routing tests ---

#[tokio::test]
async fn external_ip_sent_to_gateway() {
    let fabric = make_test_fabric();
    let (port0, handle0) = make_test_port();
    let (port1, handle1) = make_test_port();

    let (gw_tx, mut gw_rx) = tokio_mpsc::channel(64);
    let (_ingress_tx, ingress_rx) = tokio_mpsc::channel(64);
    fabric.set_gateway(gw_tx, ingress_rx);

    let (_id0, _task0) = fabric.add_port_raw_with_ip(port0, IP_A);
    let (_id1, _task1) = fabric.add_port_raw_with_ip(port1, IP_B);

    // Port 0 sends IPv4 frame to external IP → should go to gateway.
    let frame = make_ipv4_frame(EXTERNAL_IP);
    handle0.inject_tx.send(frame).await.unwrap();

    let gw_frame = tokio::time::timeout(
        std::time::Duration::from_millis(100),
        gw_rx.recv(),
    )
    .await;
    assert!(gw_frame.is_ok() && gw_frame.unwrap().is_some(), "gateway should get frame");

    // Port 1 should NOT receive it.
    assert_no_frame(&handle1).await;
}

#[tokio::test]
async fn gateway_ingress_routes_to_port_by_ip() {
    let fabric = make_test_fabric();
    let (port0, handle0) = make_test_port();
    let (port1, handle1) = make_test_port();

    let (gw_tx, _gw_rx) = tokio_mpsc::channel(64);
    let (ingress_tx, ingress_rx) = tokio_mpsc::channel(64);
    fabric.set_gateway(gw_tx, ingress_rx);

    let (_id0, _task0) = fabric.add_port_raw_with_ip(port0, IP_A);
    let (_id1, _task1) = fabric.add_port_raw_with_ip(port1, IP_B);

    // Gateway sends IPv4 frame to IP_A → should go to port 0 only.
    let gw_frame = make_ipv4_frame(IP_A);
    ingress_tx.send(gw_frame).await.unwrap();

    assert!(try_recv(&handle0).await.is_some(), "port 0 should receive gateway ingress");
    assert_no_frame(&handle1).await;
}

// --- Edge case tests ---

#[tokio::test]
async fn runt_frame_dropped() {
    let fabric = make_test_fabric();
    let (port0, handle0) = make_test_port();
    let (port1, handle1) = make_test_port();

    let (_id0, _task0) = fabric.add_port_raw_with_ip(port0, IP_A);
    let (_id1, _task1) = fabric.add_port_raw_with_ip(port1, IP_B);

    // Send a frame that is too short (< FABRIC_HDR_SZ + minimal IP header).
    let runt = vec![0u8; FABRIC_HDR_SZ + 19];
    handle0.inject_tx.send(runt).await.unwrap();

    assert_no_frame(&handle1).await;
}

// --- Route-aware forwarding tests ---

#[tokio::test]
async fn placeholder_route_buffers_instead_of_flooding() {
    use distvirt_worker_protocol::{
        BufferPolicy, FabricRouteEntry, RouteDestination,
    };

    let fabric = make_test_fabric();
    let (event_tx, mut event_rx) = tokio_mpsc::channel(64);
    fabric.set_event_channel(event_tx);

    let pod_ip = Ipv4Addr::new(172, 16, 0, 10);

    // Add a placeholder route for pod_ip.
    {
        let tables = fabric.tables();
        let mut et = tables.endpoint_table.lock().unwrap();
        et.route_sync(vec![FabricRouteEntry {
            ip: pod_ip,
            destination: RouteDestination::Placeholder {
                buffer_policy: BufferPolicy {
                    buffer_frames: 10,
                    timeout_ms: 5000,
                },
            },
        }]);
    }

    let (port0, handle0) = make_test_port();
    let (port1, handle1) = make_test_port();

    let (_id0, _task0) = fabric.add_port_raw_with_ip(port0, IP_A);
    let (_id1, _task1) = fabric.add_port_raw_with_ip(port1, IP_B);

    // Port 0 sends an IPv4 frame to the placeholder IP.
    let frame = make_ipv4_frame(pod_ip);
    handle0.inject_tx.send(frame).await.unwrap();

    // Port 1 should NOT receive the frame (it was buffered, not flooded).
    assert_no_frame(&handle1).await;

    // An endpoint activation event (route miss) should have been emitted.
    let event = try_recv_event(&mut event_rx).await;
    assert!(matches!(event, Some(FabricEvent::EndpointActivation { dst_ip: ip, service_id: None, .. }) if ip == pod_ip));
}

#[tokio::test]
async fn no_route_external_ip_goes_to_gateway() {
    let fabric = make_test_fabric();

    let (gw_tx, mut gw_rx) = tokio_mpsc::channel(64);
    let (_ingress_tx, ingress_rx) = tokio_mpsc::channel(64);
    fabric.set_gateway(gw_tx, ingress_rx);

    let (port0, handle0) = make_test_port();
    let (port1, handle1) = make_test_port();

    let (_id0, _task0) = fabric.add_port_raw_with_ip(port0, IP_A);
    let (_id1, _task1) = fabric.add_port_raw_with_ip(port1, IP_B);

    // Port 0 sends IPv4 frame to an IP outside the fabric subnet.
    let external_ip = Ipv4Addr::new(172, 16, 0, 99);
    let frame = make_ipv4_frame(external_ip);
    handle0.inject_tx.send(frame).await.unwrap();

    // Gateway should receive it (external IP → gateway egress).
    let gw_frame = tokio::time::timeout(
        std::time::Duration::from_millis(100),
        gw_rx.recv(),
    )
    .await;
    assert!(gw_frame.is_ok() && gw_frame.unwrap().is_some(), "gateway should get frame");

    // Other ports should NOT receive it.
    assert_no_frame(&handle1).await;
}

#[tokio::test]
async fn buffered_frames_flushed_to_new_port() {
    use distvirt_worker_protocol::{
        BufferPolicy, FabricRouteEntry, RouteDestination,
    };

    let fabric = make_test_fabric();

    let pod_ip = Ipv4Addr::new(172, 16, 0, 10);

    // Add a placeholder route.
    {
        let tables = fabric.tables();
        let mut et = tables.endpoint_table.lock().unwrap();
        et.route_sync(vec![FabricRouteEntry {
            ip: pod_ip,
            destination: RouteDestination::Placeholder {
                buffer_policy: BufferPolicy {
                    buffer_frames: 10,
                    timeout_ms: 5000,
                },
            },
        }]);
    }

    let (port0, handle0) = make_test_port();
    let (_id0, _task0) = fabric.add_port_raw_with_ip(port0, IP_A);

    // Send 3 frames to the placeholder IP.
    for _ in 0..3 {
        let frame = make_ipv4_frame(pod_ip);
        handle0.inject_tx.send(frame).await.unwrap();
    }

    // Let the port read loop process the frames.
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    // Now add a new port "for" that IP — buffered frames should be flushed to it.
    let (port_new, handle_new) = make_test_port();
    let (_id_new, _task_new) = fabric.add_port_raw_with_ip(port_new, pod_ip);

    // The new port should receive the 3 buffered frames.
    for i in 0..3 {
        let frame = try_recv(&handle_new).await;
        assert!(frame.is_some(), "new port should receive buffered frame {}", i);
    }
}

#[tokio::test]
async fn route_miss_debounced_on_rapid_frames() {
    use distvirt_worker_protocol::{
        BufferPolicy, FabricRouteEntry, RouteDestination,
    };

    let fabric = make_test_fabric();
    let (event_tx, mut event_rx) = tokio_mpsc::channel(64);
    fabric.set_event_channel(event_tx);

    let pod_ip = Ipv4Addr::new(172, 16, 0, 10);

    {
        let tables = fabric.tables();
        let mut et = tables.endpoint_table.lock().unwrap();
        et.route_sync(vec![FabricRouteEntry {
            ip: pod_ip,
            destination: RouteDestination::Placeholder {
                buffer_policy: BufferPolicy {
                    buffer_frames: 100,
                    timeout_ms: 5000,
                },
            },
        }]);
    }

    let (port0, handle0) = make_test_port();
    let (_id0, _task0) = fabric.add_port_raw_with_ip(port0, IP_A);

    // Send multiple frames rapidly.
    for _ in 0..5 {
        let frame = make_ipv4_frame(pod_ip);
        handle0.inject_tx.send(frame).await.unwrap();
    }

    // Wait for processing.
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    // Should get exactly one activation event (debounced).
    let event1 = try_recv_event(&mut event_rx).await;
    assert!(event1.is_some(), "should get one endpoint activation event");

    // No second event within debounce window.
    let event2 = try_recv_event(&mut event_rx).await;
    assert!(event2.is_none(), "second activation should be debounced");
}

// --- Activator integration tests ---

/// Try to load the TCP activator component. Returns None if WASM components
/// haven't been built (allows tests to skip gracefully).
fn try_load_tcp_activator() -> Option<(distvirt_activator::ActivatorRuntime, distvirt_activator::ActivatorInstance)> {
    let component_dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../activators/target/components");
    let runtime = distvirt_activator::ActivatorRuntime::new(&component_dir).ok()?;
    let component = runtime.get_component("tcp")?;
    let instance = distvirt_activator::ActivatorInstance::new(runtime.engine(), component).ok()?;
    Some((runtime, instance))
}

const SVC_IP: Ipv4Addr = Ipv4Addr::new(172, 16, 0, 50);
const POD_IP: Ipv4Addr = Ipv4Addr::new(172, 16, 0, 130);

#[tokio::test]
#[ignore = "requires WASM activators — run with --include-ignored"]
async fn activator_tcp_syn_emits_backend_need() {
    let (_runtime, instance) = try_load_tcp_activator()
        .expect("TCP activator WASM not built — run activators/build.sh");

    let fabric = make_test_fabric();
    let (event_tx, mut event_rx) = tokio_mpsc::channel(64);
    fabric.set_event_channel(event_tx);

    // Create service with TCP activator.
    {
        let tables = fabric.tables();
        let mut st = tables.endpoint_table.lock().unwrap();
        st.create_service(
            "svc-tcp".into(),
            SVC_IP,
            distvirt_worker_protocol::ServicePolicy {
                buffer_frames: 64,
                timeout_ms: 30000,
                activator: Some(distvirt_worker_protocol::ActivatorConfig::Tcp {
                    ports: None,
                    tcp_only: false,
                    max_flows: 1024,
                }),
            },
            ServiceProcessor::L3 {
                activator: instance,
                flow_tracker: distvirt_activator::FlowTracker::new(),
            },
        );
    }

    let (port0, handle0) = make_test_port();
    let (port1, handle1) = make_test_port();
    let (_id0, _task0) = fabric.add_port_raw_with_ip(port0, IP_A);
    let (_id1, _task1) = fabric.add_port_raw_with_ip(port1, IP_B);

    // Inject TCP SYN addressed to service IP from port 0.
    let syn_frame = make_tcp_frame(
        [10, 0, 0, 1], SVC_IP.octets(),
        12345, 80,
        0x02, // SYN
    );
    handle0.inject_tx.send(syn_frame).await.unwrap();

    // Wait for processing.
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    // Should get a ServiceBackendNeed(Traffic) event.
    let mut got_backend_need = false;
    while let Some(event) = try_recv_event(&mut event_rx).await {
        if let FabricEvent::ServiceBackendNeed { need, .. } = &event {
            assert_eq!(*need, distvirt_worker_protocol::BackendNeed::Traffic);
            got_backend_need = true;
        }
    }
    assert!(got_backend_need, "should emit ServiceBackendNeed(Traffic) on TCP SYN");

    // Frame should NOT be forwarded to other ports (no backend).
    assert_no_frame(&handle1).await;
}

#[tokio::test]
#[ignore = "requires WASM activators — run with --include-ignored"]
async fn activator_tcp_rst_dropped() {
    let (_runtime, instance) = try_load_tcp_activator()
        .expect("TCP activator WASM not built — run activators/build.sh");

    let fabric = make_test_fabric();
    let (event_tx, mut event_rx) = tokio_mpsc::channel(64);
    fabric.set_event_channel(event_tx);

    {
        let tables = fabric.tables();
        let mut st = tables.endpoint_table.lock().unwrap();
        st.create_service(
            "svc-tcp".into(),
            SVC_IP,
            distvirt_worker_protocol::ServicePolicy {
                buffer_frames: 64,
                timeout_ms: 30000,
                activator: Some(distvirt_worker_protocol::ActivatorConfig::Tcp {
                    ports: None,
                    tcp_only: false,
                    max_flows: 1024,
                }),
            },
            ServiceProcessor::L3 {
                activator: instance,
                flow_tracker: distvirt_activator::FlowTracker::new(),
            },
        );
    }

    let (port0, handle0) = make_test_port();
    let (port1, handle1) = make_test_port();
    let (_id0, _task0) = fabric.add_port_raw_with_ip(port0, IP_A);
    let (_id1, _task1) = fabric.add_port_raw_with_ip(port1, IP_B);

    // Inject TCP RST.
    let rst_frame = make_tcp_frame(
        [10, 0, 0, 1], SVC_IP.octets(),
        12345, 80,
        0x04, // RST
    );
    handle0.inject_tx.send(rst_frame).await.unwrap();

    // Wait for processing.
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    // No ServiceBackendNeed event (RST is dropped by activator).
    let event = try_recv_event(&mut event_rx).await;
    // May get a ServiceActivation but should NOT get ServiceBackendNeed.
    if let Some(ref ev) = event {
        assert!(
            !matches!(ev, FabricEvent::ServiceBackendNeed { .. }),
            "RST should not emit ServiceBackendNeed"
        );
    }

    // Frame should not be forwarded.
    assert_no_frame(&handle1).await;
}

#[tokio::test]
#[ignore = "requires WASM activators — run with --include-ignored"]
async fn activator_forwards_when_ready() {
    let (_runtime, instance) = try_load_tcp_activator()
        .expect("TCP activator WASM not built — run activators/build.sh");

    let fabric = make_test_fabric();

    // Create service with TCP activator.
    {
        let tables = fabric.tables();
        let mut st = tables.endpoint_table.lock().unwrap();
        st.create_service(
            "svc-tcp".into(),
            SVC_IP,
            distvirt_worker_protocol::ServicePolicy {
                buffer_frames: 64,
                timeout_ms: 30000,
                activator: Some(distvirt_worker_protocol::ActivatorConfig::Tcp {
                    ports: None,
                    tcp_only: false,
                    max_flows: 1024,
                }),
            },
            ServiceProcessor::L3 {
                activator: instance,
                flow_tracker: distvirt_activator::FlowTracker::new(),
            },
        );
        // Set backend and mark ready.
        st.update_service_backend("svc-tcp", Some(POD_IP));
        st.mark_service_ready("svc-tcp");
    }

    let (port0, handle0) = make_test_port();
    let (port1, handle1) = make_test_port();
    // Register port 1 with POD_IP/POD_MAC so fabric can route to it.
    let (_id0, _task0) = fabric.add_port_raw_with_ip(port0, IP_A);
    let (_id1, _task1) = fabric.add_port_raw_with_ip(port1, POD_IP);

    // Inject TCP SYN to service IP from port 0.
    let syn_frame = make_tcp_frame(
        [10, 0, 0, 1], SVC_IP.octets(),
        12345, 80,
        0x02, // SYN
    );
    handle0.inject_tx.send(syn_frame).await.unwrap();

    // Should be forwarded to port 1 (backend) with DNAT applied.
    let received = try_recv(&handle1).await;
    assert!(received.is_some(), "frame should be forwarded to backend port");
    let received = received.unwrap();
    let fp = FabricPacket::new(&received).unwrap();
    assert_eq!(fp.ipv4_dst(), POD_IP, "dst IP should be DNAT'd to backend IP");
}

// --- NAT tests ---

/// Regression test: service is marked ready before the backend pod's port is added.
#[tokio::test]
async fn service_forward_without_registered_backend_port() {
    let fabric = make_test_fabric();

    // Create a service with backend, mark ready (no activator — pure L3 passthrough).
    {
        let tables = fabric.tables();
        let mut st = tables.endpoint_table.lock().unwrap();
        st.create_service(
            "svc-fwd".into(),
            SVC_IP,
            distvirt_worker_protocol::ServicePolicy {
                buffer_frames: 64,
                timeout_ms: 30000,
                activator: None,
            },
            ServiceProcessor::Passthrough,
        );
        st.update_service_backend("svc-fwd", Some(POD_IP));
        st.mark_service_ready("svc-fwd");
    }

    // Add client port (port 0).
    let (port0, handle0) = make_test_port();
    let (_id0, _task0) = fabric.add_port_raw_with_ip(port0, IP_A);

    // Send TCP SYN from client (port 0) to service VIP.
    let syn_frame = make_tcp_frame(
        [10, 0, 0, 1], SVC_IP.octets(),
        12345, 80,
        0x02, // SYN
    );
    handle0.inject_tx.send(syn_frame).await.unwrap();

    // Give the fabric a moment to process the injected frame.
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    // Now add the backend port with IP+MAC — triggers flush_by_backend_ip.
    let (port1, handle1) = make_test_port();
    let (_id1, _task1) = fabric.add_port_raw_with_ip(port1, POD_IP);

    // The buffered frame should now arrive at port 1 (backend) with DNAT applied.
    let received = try_recv(&handle1).await;
    assert!(
        received.is_some(),
        "frame should be flushed to backend port when port is added"
    );
    let received = received.unwrap();

    // Verify DNAT was applied.
    let fp = FabricPacket::new(&received).unwrap();
    assert_eq!(fp.ipv4_dst(), POD_IP, "dst IP should be DNAT'd to backend IP");

    // Client port should not receive anything.
    assert_no_frame(&handle0).await;
}

const CLIENT_IP: std::net::Ipv4Addr = std::net::Ipv4Addr::new(10, 0, 0, 1);

#[tokio::test]
async fn service_nat_dnat_rewrites_dst_ip() {
    let fabric = make_test_fabric();

    // Create a service with backend, mark ready.
    {
        let tables = fabric.tables();
        let mut st = tables.endpoint_table.lock().unwrap();
        st.create_service(
            "svc-nat".into(),
            SVC_IP,
            distvirt_worker_protocol::ServicePolicy {
                buffer_frames: 64,
                timeout_ms: 30000,
                activator: None,
            },
            ServiceProcessor::Passthrough,
        );
        st.update_service_backend("svc-nat", Some(POD_IP));
        st.mark_service_ready("svc-nat");
    }

    let (port0, handle0) = make_test_port();
    let (port1, handle1) = make_test_port();
    let (_id0, _task0) = fabric.add_port_raw_with_ip(port0, CLIENT_IP);
    let (_id1, _task1) = fabric.add_port_raw_with_ip(port1, POD_IP);

    // Send TCP SYN from client to service IP.
    let syn_frame = make_tcp_frame(
        CLIENT_IP.octets(), SVC_IP.octets(),
        12345, 80,
        0x02, // SYN
    );
    handle0.inject_tx.send(syn_frame).await.unwrap();

    // Frame should arrive at port 1 (backend) with DNAT applied.
    let received = try_recv(&handle1).await;
    assert!(received.is_some(), "frame should be forwarded to backend port");
    let received = received.unwrap();

    let fp = FabricPacket::new(&received).unwrap();
    // dst IP should be rewritten from SVC_IP to POD_IP.
    assert_eq!(fp.ipv4_dst(), POD_IP, "dst IP should be DNAT'd to backend IP");
    // src IP should be unchanged.
    assert_eq!(fp.ipv4_src(), CLIENT_IP, "src IP should be unchanged");
}

#[tokio::test]
async fn service_nat_snat_rewrites_return_traffic() {
    let fabric = make_test_fabric();

    // Create a service with backend, mark ready.
    {
        let tables = fabric.tables();
        let mut st = tables.endpoint_table.lock().unwrap();
        st.create_service(
            "svc-nat".into(),
            SVC_IP,
            distvirt_worker_protocol::ServicePolicy {
                buffer_frames: 64,
                timeout_ms: 30000,
                activator: None,
            },
            ServiceProcessor::Passthrough,
        );
        st.update_service_backend("svc-nat", Some(POD_IP));
        st.mark_service_ready("svc-nat");
    }

    let (port0, handle0) = make_test_port();
    let (port1, handle1) = make_test_port();
    let (_id0, _task0) = fabric.add_port_raw_with_ip(port0, CLIENT_IP);
    let (_id1, _task1) = fabric.add_port_raw_with_ip(port1, POD_IP);

    // Step 1: Send forward traffic (client→service) to install NAT entry.
    let syn_frame = make_tcp_frame(
        CLIENT_IP.octets(), SVC_IP.octets(),
        12345, 80,
        0x02, // SYN
    );
    handle0.inject_tx.send(syn_frame).await.unwrap();

    // Drain the DNAT'd frame from port 1.
    let dnat_frame = try_recv(&handle1).await;
    assert!(dnat_frame.is_some(), "DNAT'd frame should arrive at backend");

    // Step 2: Send return traffic (backend→client) — should be SNAT'd.
    let syn_ack_frame = make_tcp_frame(
        POD_IP.octets(), CLIENT_IP.octets(),
        80, 12345,
        0x12, // SYN+ACK
    );
    handle1.inject_tx.send(syn_ack_frame).await.unwrap();

    // Frame should arrive at port 0 with SNAT applied.
    let received = try_recv(&handle0).await;
    assert!(received.is_some(), "return frame should arrive at client port");
    let received = received.unwrap();

    let fp = FabricPacket::new(&received).unwrap();
    // src IP should be rewritten from POD_IP to SVC_IP.
    assert_eq!(fp.ipv4_src(), SVC_IP, "src IP should be SNAT'd to service IP");
    // dst should be unchanged.
    assert_eq!(fp.ipv4_dst(), CLIENT_IP, "dst IP should be unchanged");
}

#[tokio::test]
async fn non_natted_unicast_ip_unchanged() {
    // Regular unicast traffic that doesn't match any NAT entry should have
    // IPs unchanged.
    let fabric = make_test_fabric();

    let src_ip = Ipv4Addr::new(10, 0, 0, 2);
    let dst_ip = Ipv4Addr::new(10, 0, 0, 1);

    let (port0, handle0) = make_test_port();
    let (port1, handle1) = make_test_port();
    let (_id0, _task0) = fabric.add_port_raw_with_ip(port0, dst_ip);
    let (_id1, _task1) = fabric.add_port_raw_with_ip(port1, src_ip);

    // Port 1 sends unicast to port 0's IP — not NAT'd.
    let frame = make_tcp_frame(
        src_ip.octets(), dst_ip.octets(),
        5000, 8080,
        0x02,
    );
    handle1.inject_tx.send(frame).await.unwrap();

    let received = try_recv(&handle0).await;
    assert!(received.is_some(), "frame should be forwarded");
    let received = received.unwrap();

    let fp = FabricPacket::new(&received).unwrap();
    // IPs should be unchanged (no NAT).
    assert_eq!(fp.ipv4_src(), src_ip, "src IP should be unchanged");
    assert_eq!(fp.ipv4_dst(), dst_ip, "dst IP should be unchanged");
}

#[tokio::test]
async fn service_nat_ip_checksum_valid() {
    // Verify that the IP header checksum is still valid after DNAT.
    let fabric = make_test_fabric();

    {
        let tables = fabric.tables();
        let mut st = tables.endpoint_table.lock().unwrap();
        st.create_service(
            "svc-nat".into(),
            SVC_IP,
            distvirt_worker_protocol::ServicePolicy {
                buffer_frames: 64,
                timeout_ms: 30000,
                activator: None,
            },
            ServiceProcessor::Passthrough,
        );
        st.update_service_backend("svc-nat", Some(POD_IP));
        st.mark_service_ready("svc-nat");
    }

    let (port0, handle0) = make_test_port();
    let (port1, handle1) = make_test_port();
    let (_id0, _task0) = fabric.add_port_raw_with_ip(port0, CLIENT_IP);
    let (_id1, _task1) = fabric.add_port_raw_with_ip(port1, POD_IP);

    let syn = make_tcp_frame(
        CLIENT_IP.octets(), SVC_IP.octets(),
        12345, 80,
        0x02,
    );
    handle0.inject_tx.send(syn).await.unwrap();

    let received = try_recv(&handle1).await.unwrap();
    let fp = FabricPacket::new(&received).unwrap();
    let ip = fp.ip_packet();

    // Verify IP header checksum: compute from scratch and compare.
    let ip_hdr = &ip[..20]; // 20-byte IP header
    let mut sum: u32 = 0;
    for i in (0..20).step_by(2) {
        sum += u16::from_be_bytes([ip_hdr[i], ip_hdr[i + 1]]) as u32;
    }
    while sum >> 16 != 0 {
        sum = (sum & 0xffff) + (sum >> 16);
    }
    assert_eq!(!sum as u16, 0, "IP header checksum should be valid after DNAT");
}

// --- Tunnel routing tests ---

#[tokio::test]
async fn test_remote_worker_route_forwards_to_tunnel_port() {
    use distvirt_worker_protocol::{FabricRouteEntry, RouteDestination, WorkerId};

    let fabric = make_test_fabric();

    let remote_pod_ip = Ipv4Addr::new(10, 0, 0, 50);
    let worker_id = "remote-worker-1";

    // Create a TestPort to act as the tunnel port.
    let (tunnel_port, tunnel_handle) = make_test_port();

    // Register it as a tunnel port.
    let (_port_id, _task) = fabric.add_tunnel_port(worker_id.to_string(), tunnel_port);

    // Add a RemoteWorker route for the remote pod IP.
    {
        let tables = fabric.tables();
        let mut et = tables.endpoint_table.lock().unwrap();
        et.route_sync(vec![FabricRouteEntry {
            ip: remote_pod_ip,
            destination: RouteDestination::RemoteWorker {
                worker_id: WorkerId::from(worker_id),
            },
        }]);
    }

    // Add a local port that sends a frame to the remote pod IP.
    let (port0, handle0) = make_test_port();
    let (_id0, _task0) = fabric.add_port_raw_with_ip(port0, IP_A);

    let frame = make_ipv4_frame(remote_pod_ip);
    handle0.inject_tx.send(frame).await.unwrap();

    // The tunnel port should receive the frame (captured via send_frame).
    let received = try_recv(&tunnel_handle).await;
    assert!(
        received.is_some(),
        "tunnel port should receive frame for RemoteWorker route"
    );

    // Verify the frame has the correct dst IP.
    let received = received.unwrap();
    let fp = FabricPacket::new(&received).unwrap();
    assert_eq!(fp.ipv4_dst(), remote_pod_ip);
}
