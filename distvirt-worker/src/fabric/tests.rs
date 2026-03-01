use super::*;
use super::forwarding::flood_frame;
use switch::{ETH_HEADER_LEN, FabricFrame, GATEWAY_MAC, VNET_HDR_SZ, with_vnet_header};
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

/// Build a valid test frame: [vnet_hdr (10 bytes)][eth_hdr (14 bytes)][payload...]
fn make_frame(dst_mac: [u8; 6], src_mac: [u8; 6], ethertype: u16, payload: &[u8]) -> Vec<u8> {
    let mut eth = Vec::with_capacity(14 + payload.len());
    eth.extend_from_slice(&dst_mac);
    eth.extend_from_slice(&src_mac);
    eth.extend_from_slice(&ethertype.to_be_bytes());
    eth.extend_from_slice(payload);
    with_vnet_header(&eth)
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

// Some test MAC addresses
const MAC_A: [u8; 6] = [0x02, 0x00, 0x00, 0x00, 0x00, 0x0a];
const MAC_B: [u8; 6] = [0x02, 0x00, 0x00, 0x00, 0x00, 0x0b];
const MAC_C: [u8; 6] = [0x02, 0x00, 0x00, 0x00, 0x00, 0x0c];
const BROADCAST: [u8; 6] = [0xff; 6];
const MULTICAST: [u8; 6] = [0x01, 0x00, 0x5e, 0x00, 0x00, 0x01];

// --- Unicast forwarding tests ---

#[tokio::test]
async fn known_dst_delivers_to_correct_port() {
    let mut fabric: Fabric<TestPort> = Fabric::new();
    let (port0, handle0) = make_test_port();
    let (port1, handle1) = make_test_port();

    let (_id0, _task0) = fabric.add_port_raw(port0);
    let (_id1, _task1) = fabric.add_port_raw(port1);

    // Port 0 sends a frame with src=MAC_A, causing MAC_A to be learned on port 0.
    // Then port 1 sends a frame with dst=MAC_A, which should be delivered to port 0.
    let frame_learn = make_frame(MAC_B, MAC_A, 0x0800, &[0u8; 10]);
    handle0.inject_tx.send(frame_learn).await.unwrap();

    // Wait for learning to happen; the frame floods since MAC_B is unknown.
    let _ = try_recv(&handle1).await;

    // Now port 1 sends to MAC_A (known on port 0).
    let frame_to_a = make_frame(MAC_A, MAC_B, 0x0800, &[0u8; 10]);
    handle1.inject_tx.send(frame_to_a).await.unwrap();

    // Port 0 should receive the frame.
    let received = try_recv(&handle0).await;
    assert!(received.is_some(), "port 0 should receive frame destined to MAC_A");
}

#[tokio::test]
async fn unknown_dst_floods_to_all_other_ports() {
    let mut fabric: Fabric<TestPort> = Fabric::new();
    let (port0, handle0) = make_test_port();
    let (port1, handle1) = make_test_port();
    let (port2, handle2) = make_test_port();

    let (_id0, _task0) = fabric.add_port_raw(port0);
    let (_id1, _task1) = fabric.add_port_raw(port1);
    let (_id2, _task2) = fabric.add_port_raw(port2);

    // Port 0 sends a frame to unknown MAC_C.
    let frame = make_frame(MAC_C, MAC_A, 0x0800, &[0u8; 10]);
    handle0.inject_tx.send(frame).await.unwrap();

    // Both port 1 and port 2 should receive the flooded frame.
    assert!(try_recv(&handle1).await.is_some(), "port 1 should receive flooded frame");
    assert!(try_recv(&handle2).await.is_some(), "port 2 should receive flooded frame");
}

#[tokio::test]
async fn no_loopback_to_source_port() {
    let mut fabric: Fabric<TestPort> = Fabric::new();
    let (port0, handle0) = make_test_port();
    let (port1, _handle1) = make_test_port();

    let (_id0, _task0) = fabric.add_port_raw(port0);
    let (_id1, _task1) = fabric.add_port_raw(port1);

    // Port 0 sends a frame; it should never come back to port 0.
    let frame = make_frame(MAC_B, MAC_A, 0x0800, &[0u8; 10]);
    handle0.inject_tx.send(frame).await.unwrap();

    assert_no_frame(&handle0).await;
}

// --- Broadcast/multicast tests ---

#[tokio::test]
async fn broadcast_floods_to_all_other_ports_and_gateway() {
    let mut fabric: Fabric<TestPort> = Fabric::new();
    let (port0, handle0) = make_test_port();
    let (port1, handle1) = make_test_port();

    let (gw_tx, mut gw_rx) = tokio_mpsc::channel(64);
    let (_ingress_tx, ingress_rx) = tokio_mpsc::channel(64);
    fabric.set_gateway(gw_tx, ingress_rx);

    let (_id0, _task0) = fabric.add_port_raw(port0);
    let (_id1, _task1) = fabric.add_port_raw(port1);

    let frame = make_frame(BROADCAST, MAC_A, 0x0806, &[0u8; 10]);
    handle0.inject_tx.send(frame).await.unwrap();

    // Port 1 should receive the flooded frame.
    assert!(try_recv(&handle1).await.is_some(), "port 1 should get broadcast");

    // Gateway should also receive it.
    let gw_frame = tokio::time::timeout(
        std::time::Duration::from_millis(100),
        gw_rx.recv(),
    )
    .await;
    assert!(gw_frame.is_ok() && gw_frame.unwrap().is_some(), "gateway should get broadcast");
}

#[tokio::test]
async fn multicast_floods_to_all_other_ports_and_gateway() {
    let mut fabric: Fabric<TestPort> = Fabric::new();
    let (port0, handle0) = make_test_port();
    let (port1, handle1) = make_test_port();

    let (gw_tx, mut gw_rx) = tokio_mpsc::channel(64);
    let (_ingress_tx, ingress_rx) = tokio_mpsc::channel(64);
    fabric.set_gateway(gw_tx, ingress_rx);

    let (_id0, _task0) = fabric.add_port_raw(port0);
    let (_id1, _task1) = fabric.add_port_raw(port1);

    let frame = make_frame(MULTICAST, MAC_A, 0x0800, &[0u8; 10]);
    handle0.inject_tx.send(frame).await.unwrap();

    assert!(try_recv(&handle1).await.is_some(), "port 1 should get multicast");
    let gw_frame = tokio::time::timeout(
        std::time::Duration::from_millis(100),
        gw_rx.recv(),
    )
    .await;
    assert!(gw_frame.is_ok() && gw_frame.unwrap().is_some(), "gateway should get multicast");
}

// --- Gateway routing tests ---

#[tokio::test]
async fn gateway_mac_dst_sent_to_gateway_only() {
    let mut fabric: Fabric<TestPort> = Fabric::new();
    let (port0, handle0) = make_test_port();
    let (port1, handle1) = make_test_port();

    let (gw_tx, mut gw_rx) = tokio_mpsc::channel(64);
    let (_ingress_tx, ingress_rx) = tokio_mpsc::channel(64);
    fabric.set_gateway(gw_tx, ingress_rx);

    let (_id0, _task0) = fabric.add_port_raw(port0);
    let (_id1, _task1) = fabric.add_port_raw(port1);

    let frame = make_frame(GATEWAY_MAC, MAC_A, 0x0800, &[0u8; 10]);
    handle0.inject_tx.send(frame).await.unwrap();

    // Gateway should receive it.
    let gw_frame = tokio::time::timeout(
        std::time::Duration::from_millis(100),
        gw_rx.recv(),
    )
    .await;
    assert!(gw_frame.is_ok() && gw_frame.unwrap().is_some(), "gateway should get frame");

    // Port 1 should NOT receive it.
    assert_no_frame(&handle1).await;
}

// --- MAC learning tests ---

#[tokio::test]
async fn mac_learning_and_forwarding() {
    let mut fabric: Fabric<TestPort> = Fabric::new();
    let (port0, handle0) = make_test_port();
    let (port1, handle1) = make_test_port();
    let (port2, handle2) = make_test_port();

    let (_id0, _task0) = fabric.add_port_raw(port0);
    let (_id1, _task1) = fabric.add_port_raw(port1);
    let (_id2, _task2) = fabric.add_port_raw(port2);

    // Port 0 sends a frame with src=MAC_A (learn MAC_A on port 0).
    let frame1 = make_frame(BROADCAST, MAC_A, 0x0806, &[0u8; 10]);
    handle0.inject_tx.send(frame1).await.unwrap();

    // Drain the broadcast flood.
    let _ = try_recv(&handle1).await;
    let _ = try_recv(&handle2).await;

    // Port 1 sends a frame with dst=MAC_A → should go to port 0 only.
    let frame2 = make_frame(MAC_A, MAC_B, 0x0800, &[0u8; 10]);
    handle1.inject_tx.send(frame2).await.unwrap();

    assert!(try_recv(&handle0).await.is_some(), "port 0 should receive frame to MAC_A");
    assert_no_frame(&handle2).await;
}

#[tokio::test]
async fn mac_migration() {
    let mut fabric: Fabric<TestPort> = Fabric::new();
    let (port0, handle0) = make_test_port();
    let (port1, handle1) = make_test_port();
    let (port2, handle2) = make_test_port();

    let (_id0, _task0) = fabric.add_port_raw(port0);
    let (_id1, _task1) = fabric.add_port_raw(port1);
    let (_id2, _task2) = fabric.add_port_raw(port2);

    // Learn MAC_A on port 0.
    let frame1 = make_frame(BROADCAST, MAC_A, 0x0806, &[0u8; 10]);
    handle0.inject_tx.send(frame1).await.unwrap();
    let _ = try_recv(&handle1).await;
    let _ = try_recv(&handle2).await;

    // Migrate MAC_A to port 1.
    let frame2 = make_frame(BROADCAST, MAC_A, 0x0806, &[0u8; 10]);
    handle1.inject_tx.send(frame2).await.unwrap();
    let _ = try_recv(&handle0).await;
    let _ = try_recv(&handle2).await;

    // Port 2 sends to MAC_A → should now go to port 1.
    let frame3 = make_frame(MAC_A, MAC_C, 0x0800, &[0u8; 10]);
    handle2.inject_tx.send(frame3).await.unwrap();

    assert!(try_recv(&handle1).await.is_some(), "port 1 should receive frame after migration");
    assert_no_frame(&handle0).await;
}

// --- Gateway ingress tests ---

#[tokio::test]
async fn gateway_ingress_known_unicast() {
    let mut fabric: Fabric<TestPort> = Fabric::new();
    let (port0, handle0) = make_test_port();
    let (port1, handle1) = make_test_port();

    let (gw_tx, _gw_rx) = tokio_mpsc::channel(64);
    let (ingress_tx, ingress_rx) = tokio_mpsc::channel(64);
    fabric.set_gateway(gw_tx, ingress_rx);

    let (_id0, _task0) = fabric.add_port_raw(port0);
    let (_id1, _task1) = fabric.add_port_raw(port1);

    // Learn MAC_A on port 0 by sending a frame from port 0.
    let frame_learn = make_frame(BROADCAST, MAC_A, 0x0806, &[0u8; 10]);
    handle0.inject_tx.send(frame_learn).await.unwrap();
    let _ = try_recv(&handle1).await;

    // Gateway sends a frame to MAC_A → should go to port 0 only.
    let gw_frame = make_frame(MAC_A, GATEWAY_MAC, 0x0800, &[0u8; 10]);
    ingress_tx.send(gw_frame).await.unwrap();

    assert!(try_recv(&handle0).await.is_some(), "port 0 should receive gateway ingress");
    assert_no_frame(&handle1).await;
}

#[tokio::test]
async fn gateway_ingress_unknown_unicast_floods() {
    let mut fabric: Fabric<TestPort> = Fabric::new();
    let (port0, handle0) = make_test_port();
    let (port1, handle1) = make_test_port();

    let (gw_tx, _gw_rx) = tokio_mpsc::channel(64);
    let (ingress_tx, ingress_rx) = tokio_mpsc::channel(64);
    fabric.set_gateway(gw_tx, ingress_rx);

    let (_id0, _task0) = fabric.add_port_raw(port0);
    let (_id1, _task1) = fabric.add_port_raw(port1);

    // Gateway sends to unknown MAC_C → should flood to all ports.
    let gw_frame = make_frame(MAC_C, GATEWAY_MAC, 0x0800, &[0u8; 10]);
    ingress_tx.send(gw_frame).await.unwrap();

    assert!(try_recv(&handle0).await.is_some(), "port 0 should receive flooded frame");
    assert!(try_recv(&handle1).await.is_some(), "port 1 should receive flooded frame");
}

#[tokio::test]
async fn gateway_ingress_broadcast_floods_to_all() {
    let mut fabric: Fabric<TestPort> = Fabric::new();
    let (port0, handle0) = make_test_port();
    let (port1, handle1) = make_test_port();

    let (gw_tx, _gw_rx) = tokio_mpsc::channel(64);
    let (ingress_tx, ingress_rx) = tokio_mpsc::channel(64);
    fabric.set_gateway(gw_tx, ingress_rx);

    let (_id0, _task0) = fabric.add_port_raw(port0);
    let (_id1, _task1) = fabric.add_port_raw(port1);

    let gw_frame = make_frame(BROADCAST, GATEWAY_MAC, 0x0806, &[0u8; 10]);
    ingress_tx.send(gw_frame).await.unwrap();

    assert!(try_recv(&handle0).await.is_some(), "port 0 should receive broadcast");
    assert!(try_recv(&handle1).await.is_some(), "port 1 should receive broadcast");
}

// --- Edge case tests ---

#[tokio::test]
async fn runt_frame_dropped() {
    let mut fabric: Fabric<TestPort> = Fabric::new();
    let (port0, handle0) = make_test_port();
    let (port1, handle1) = make_test_port();

    let (_id0, _task0) = fabric.add_port_raw(port0);
    let (_id1, _task1) = fabric.add_port_raw(port1);

    // Send a frame that is too short (< VNET_HDR_SZ + ETH_HEADER_LEN).
    let runt = vec![0u8; VNET_HDR_SZ + ETH_HEADER_LEN - 1];
    handle0.inject_tx.send(runt).await.unwrap();

    assert_no_frame(&handle1).await;
}

#[tokio::test]
async fn flood_frame_with_empty_ports_no_panic() {
    let ports: Arc<Mutex<HashMap<PortId, SharedPort<TestPort>>>> =
        Arc::new(Mutex::new(HashMap::new()));
    let frame = make_frame(MAC_A, MAC_B, 0x0800, &[0u8; 10]);
    // Should not panic.
    flood_frame::<TestPort>(&frame, 0, &ports).await;
}

// --- Route-aware forwarding tests ---

/// Build a valid IPv4 test frame with specific dst IP.
/// Layout: [vnet_hdr(10)][eth_hdr(14)][ip_hdr(20)]
fn make_ipv4_frame(dst_mac: [u8; 6], src_mac: [u8; 6], dst_ip: Ipv4Addr) -> Vec<u8> {
    let mut eth = Vec::with_capacity(14 + 20);
    // Ethernet header
    eth.extend_from_slice(&dst_mac);
    eth.extend_from_slice(&src_mac);
    eth.extend_from_slice(&0x0800u16.to_be_bytes());
    // Minimal IP header (20 bytes)
    let mut ip_hdr = [0u8; 20];
    ip_hdr[0] = 0x45; // version=4, IHL=5
    // src IP at offset 12..16
    ip_hdr[12..16].copy_from_slice(&[10, 0, 0, 1]);
    // dst IP at offset 16..20
    ip_hdr[16..20].copy_from_slice(&dst_ip.octets());
    eth.extend_from_slice(&ip_hdr);
    with_vnet_header(&eth)
}

/// Helper: try to receive a FabricEvent with timeout.
async fn try_recv_event(rx: &mut tokio_mpsc::Receiver<FabricEvent>) -> Option<FabricEvent> {
    tokio::time::timeout(std::time::Duration::from_millis(200), rx.recv())
        .await
        .ok()
        .flatten()
}

#[tokio::test]
async fn placeholder_route_buffers_instead_of_flooding() {
    use distvirt_worker_protocol::{
        BufferPolicy, FabricRouteEntry, RouteDestination,
    };

    let mut fabric: Fabric<TestPort> = Fabric::new();
    let (event_tx, mut event_rx) = tokio_mpsc::channel(64);
    fabric.set_event_channel(event_tx);

    let pod_ip = Ipv4Addr::new(172, 16, 0, 10);

    // Add a placeholder route for pod_ip.
    {
        let tables = fabric.tables();
        let mut rt = tables.route_table.lock().unwrap();
        rt.sync(vec![FabricRouteEntry {
            ip: pod_ip,
            mac: MAC_C,
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

    let (_id0, _task0) = fabric.add_port_raw(port0);
    let (_id1, _task1) = fabric.add_port_raw(port1);

    // Port 0 sends an IPv4 frame to the placeholder IP with unknown dst MAC.
    let frame = make_ipv4_frame(MAC_C, MAC_A, pod_ip);
    handle0.inject_tx.send(frame).await.unwrap();

    // Port 1 should NOT receive the frame (it was buffered, not flooded).
    assert_no_frame(&handle1).await;

    // A route miss event should have been emitted.
    let event = try_recv_event(&mut event_rx).await;
    assert!(matches!(event, Some(FabricEvent::RouteMiss { dst_ip: ip, .. }) if ip == pod_ip));
}

#[tokio::test]
async fn no_route_still_floods() {
    let mut fabric: Fabric<TestPort> = Fabric::new();

    let (port0, handle0) = make_test_port();
    let (port1, handle1) = make_test_port();
    let (port2, handle2) = make_test_port();

    let (_id0, _task0) = fabric.add_port_raw(port0);
    let (_id1, _task1) = fabric.add_port_raw(port1);
    let (_id2, _task2) = fabric.add_port_raw(port2);

    // Port 0 sends IPv4 frame to unknown MAC with no route entry.
    let pod_ip = Ipv4Addr::new(172, 16, 0, 99);
    let frame = make_ipv4_frame(MAC_C, MAC_A, pod_ip);
    handle0.inject_tx.send(frame).await.unwrap();

    // Both other ports should receive the flooded frame.
    assert!(try_recv(&handle1).await.is_some(), "port 1 should receive flooded frame");
    assert!(try_recv(&handle2).await.is_some(), "port 2 should receive flooded frame");
}

#[tokio::test]
async fn buffered_frames_flushed_to_new_port() {
    use distvirt_worker_protocol::{
        BufferPolicy, FabricRouteEntry, RouteDestination,
    };

    let mut fabric: Fabric<TestPort> = Fabric::new();

    let pod_ip = Ipv4Addr::new(172, 16, 0, 10);

    // Add a placeholder route.
    {
        let tables = fabric.tables();
        let mut rt = tables.route_table.lock().unwrap();
        rt.sync(vec![FabricRouteEntry {
            ip: pod_ip,
            mac: MAC_C,
            destination: RouteDestination::Placeholder {
                buffer_policy: BufferPolicy {
                    buffer_frames: 10,
                    timeout_ms: 5000,
                },
            },
        }]);
    }

    let (port0, handle0) = make_test_port();
    let (_id0, _task0) = fabric.add_port_raw(port0);

    // Send 3 frames to the placeholder IP.
    for _ in 0..3 {
        let frame = make_ipv4_frame(MAC_C, MAC_A, pod_ip);
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

    let mut fabric: Fabric<TestPort> = Fabric::new();
    let (event_tx, mut event_rx) = tokio_mpsc::channel(64);
    fabric.set_event_channel(event_tx);

    let pod_ip = Ipv4Addr::new(172, 16, 0, 10);

    {
        let tables = fabric.tables();
        let mut rt = tables.route_table.lock().unwrap();
        rt.sync(vec![FabricRouteEntry {
            ip: pod_ip,
            mac: MAC_C,
            destination: RouteDestination::Placeholder {
                buffer_policy: BufferPolicy {
                    buffer_frames: 100,
                    timeout_ms: 5000,
                },
            },
        }]);
    }

    let (port0, handle0) = make_test_port();
    let (_id0, _task0) = fabric.add_port_raw(port0);

    // Send multiple frames rapidly.
    for _ in 0..5 {
        let frame = make_ipv4_frame(MAC_C, MAC_A, pod_ip);
        handle0.inject_tx.send(frame).await.unwrap();
    }

    // Wait for processing.
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    // Should get exactly one miss event (debounced).
    let event1 = try_recv_event(&mut event_rx).await;
    assert!(event1.is_some(), "should get one route miss event");

    // No second event within debounce window.
    let event2 = try_recv_event(&mut event_rx).await;
    assert!(event2.is_none(), "second miss should be debounced");
}

// --- Activator integration tests ---

/// Build a valid Ethernet+IPv4+TCP frame with vnet header.
/// Uses etherparse::PacketBuilder for correct headers, then overwrites TCP flags.
fn make_tcp_frame(
    dst_mac: [u8; 6],
    src_mac: [u8; 6],
    src_ip: [u8; 4],
    dst_ip: [u8; 4],
    src_port: u16,
    dst_port: u16,
    tcp_flags: u8,
) -> Vec<u8> {
    use etherparse::PacketBuilder;

    let builder = PacketBuilder::ethernet2(src_mac, dst_mac)
        .ipv4(src_ip, dst_ip, 64)
        .tcp(src_port, dst_port, 1000, 65535);

    let mut eth_frame = Vec::new();
    builder.write(&mut eth_frame, &[]).unwrap();

    // Overwrite TCP flags: eth(14) + ip(20) + tcp flags at byte 13
    let tcp_start = 14 + 20;
    eth_frame[tcp_start + 13] = tcp_flags;

    with_vnet_header(&eth_frame)
}

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

/// Build an ARP request frame with vnet header.
fn make_arp_request(
    sender_mac: [u8; 6],
    sender_ip: [u8; 4],
    target_ip: [u8; 4],
) -> Vec<u8> {
    let mut eth = Vec::with_capacity(14 + 28);
    // Ethernet header: broadcast dst, sender src, ARP ethertype
    eth.extend_from_slice(&BROADCAST);
    eth.extend_from_slice(&sender_mac);
    eth.extend_from_slice(&0x0806u16.to_be_bytes());
    // ARP payload (28 bytes)
    let mut arp = [0u8; 28];
    arp[0..2].copy_from_slice(&[0x00, 0x01]); // hardware type: Ethernet
    arp[2..4].copy_from_slice(&[0x08, 0x00]); // protocol type: IPv4
    arp[4] = 6; // hardware size
    arp[5] = 4; // protocol size
    arp[6..8].copy_from_slice(&[0x00, 0x01]); // operation: request
    arp[8..14].copy_from_slice(&sender_mac);   // sender hardware address
    arp[14..18].copy_from_slice(&sender_ip);    // sender protocol address
    // target hardware address [18..24] = zeroed (unknown)
    arp[24..28].copy_from_slice(&target_ip);    // target protocol address
    eth.extend_from_slice(&arp);
    with_vnet_header(&eth)
}

const SVC_IP: Ipv4Addr = Ipv4Addr::new(172, 16, 0, 50);
const SVC_MAC: [u8; 6] = [0x06, 0x00, 0xAC, 0x10, 0x00, 0x32];
const POD_IP: Ipv4Addr = Ipv4Addr::new(172, 16, 0, 130);
const POD_MAC: [u8; 6] = [0x06, 0x00, 0xAC, 0x10, 0x00, 0x82];

#[tokio::test]
async fn activator_tcp_syn_emits_backend_need() {
    let Some((_runtime, instance)) = try_load_tcp_activator() else {
        eprintln!("SKIP: TCP activator WASM not built");
        return;
    };

    let mut fabric: Fabric<TestPort> = Fabric::new();
    let (event_tx, mut event_rx) = tokio_mpsc::channel(64);
    fabric.set_event_channel(event_tx);

    // Create service with TCP activator.
    {
        let tables = fabric.tables();
        let mut st = tables.service_table.lock().unwrap();
        st.create(
            "svc-tcp".into(),
            SVC_IP,
            SVC_MAC,
            distvirt_worker_protocol::ServicePolicy {
                buffer_frames: 64,
                timeout_ms: 30000,
                activator: Some(distvirt_worker_protocol::ActivatorConfig::Tcp {
                    ports: None,
                    tcp_only: false,
                    max_flows: 1024,
                }),
            },
            Some(instance),
            None,
        );
    }

    let (port0, handle0) = make_test_port();
    let (port1, handle1) = make_test_port();
    let (_id0, _task0) = fabric.add_port_raw(port0);
    let (_id1, _task1) = fabric.add_port_raw(port1);

    // Inject TCP SYN addressed to service MAC/IP from port 0.
    let syn_frame = make_tcp_frame(
        SVC_MAC, MAC_A,
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
async fn activator_tcp_rst_dropped() {
    let Some((_runtime, instance)) = try_load_tcp_activator() else {
        eprintln!("SKIP: TCP activator WASM not built");
        return;
    };

    let mut fabric: Fabric<TestPort> = Fabric::new();
    let (event_tx, mut event_rx) = tokio_mpsc::channel(64);
    fabric.set_event_channel(event_tx);

    {
        let tables = fabric.tables();
        let mut st = tables.service_table.lock().unwrap();
        st.create(
            "svc-tcp".into(),
            SVC_IP,
            SVC_MAC,
            distvirt_worker_protocol::ServicePolicy {
                buffer_frames: 64,
                timeout_ms: 30000,
                activator: Some(distvirt_worker_protocol::ActivatorConfig::Tcp {
                    ports: None,
                    tcp_only: false,
                    max_flows: 1024,
                }),
            },
            Some(instance),
            None,
        );
    }

    let (port0, handle0) = make_test_port();
    let (port1, handle1) = make_test_port();
    let (_id0, _task0) = fabric.add_port_raw(port0);
    let (_id1, _task1) = fabric.add_port_raw(port1);

    // Inject TCP RST.
    let rst_frame = make_tcp_frame(
        SVC_MAC, MAC_A,
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
async fn activator_forwards_when_ready() {
    let Some((_runtime, instance)) = try_load_tcp_activator() else {
        eprintln!("SKIP: TCP activator WASM not built");
        return;
    };

    let mut fabric: Fabric<TestPort> = Fabric::new();

    // Create service with TCP activator.
    {
        let tables = fabric.tables();
        let mut st = tables.service_table.lock().unwrap();
        st.create(
            "svc-tcp".into(),
            SVC_IP,
            SVC_MAC,
            distvirt_worker_protocol::ServicePolicy {
                buffer_frames: 64,
                timeout_ms: 30000,
                activator: Some(distvirt_worker_protocol::ActivatorConfig::Tcp {
                    ports: None,
                    tcp_only: false,
                    max_flows: 1024,
                }),
            },
            Some(instance),
            None,
        );
        // Set backend and mark ready.
        st.update_backend("svc-tcp", Some((POD_IP, POD_MAC)));
        st.mark_ready("svc-tcp");
    }

    let (port0, handle0) = make_test_port();
    let (port1, handle1) = make_test_port();
    let (_id0, _task0) = fabric.add_port_raw(port0);
    let (_id1, _task1) = fabric.add_port_raw(port1);

    // Learn POD_MAC on port 1 (so the fabric knows where to forward).
    let learn_frame = make_frame(BROADCAST, POD_MAC, 0x0806, &[0u8; 28]);
    handle1.inject_tx.send(learn_frame).await.unwrap();
    // Drain the broadcast flood to port 0.
    let _ = try_recv(&handle0).await;

    // Now inject TCP SYN to service IP from port 0.
    let syn_frame = make_tcp_frame(
        SVC_MAC, MAC_A,
        [10, 0, 0, 1], SVC_IP.octets(),
        12345, 80,
        0x02, // SYN
    );
    handle0.inject_tx.send(syn_frame).await.unwrap();

    // Should be forwarded to port 1 (backend) with dst MAC rewritten to POD_MAC.
    let received = try_recv(&handle1).await;
    assert!(received.is_some(), "frame should be forwarded to backend port");
    let received = received.unwrap();
    // Check dst MAC was rewritten.
    let ff = FabricFrame::new(&received).unwrap();
    assert_eq!(ff.dst_mac(), POD_MAC, "dst MAC should be rewritten to backend MAC");
}

#[tokio::test]
async fn activator_service_arp_reply() {
    // ARP replies work independently of activators, but verify they work
    // when a service has an activator attached.
    let Some((_runtime, instance)) = try_load_tcp_activator() else {
        eprintln!("SKIP: TCP activator WASM not built");
        return;
    };

    let mut fabric: Fabric<TestPort> = Fabric::new();

    {
        let tables = fabric.tables();
        let mut st = tables.service_table.lock().unwrap();
        st.create(
            "svc-tcp".into(),
            SVC_IP,
            SVC_MAC,
            distvirt_worker_protocol::ServicePolicy {
                buffer_frames: 64,
                timeout_ms: 30000,
                activator: Some(distvirt_worker_protocol::ActivatorConfig::Tcp {
                    ports: None,
                    tcp_only: false,
                    max_flows: 1024,
                }),
            },
            Some(instance),
            None,
        );
    }

    let (port0, handle0) = make_test_port();
    let (_id0, _task0) = fabric.add_port_raw(port0);

    // Inject ARP request for service IP.
    let arp_frame = make_arp_request(
        MAC_A,
        [10, 0, 0, 1],
        SVC_IP.octets(),
    );
    handle0.inject_tx.send(arp_frame).await.unwrap();

    // Should receive ARP reply on port 0.
    let reply = try_recv(&handle0).await;
    assert!(reply.is_some(), "should receive ARP reply for service IP");
    let reply = reply.unwrap();

    // Verify it's an ARP reply with service MAC.
    let ff = FabricFrame::new(&reply).unwrap();
    assert_eq!(ff.ethertype(), 0x0806, "should be ARP");
    let arp = &ff.eth_payload()[14..]; // ARP payload after eth header
    let arp_op = u16::from_be_bytes([arp[6], arp[7]]);
    assert_eq!(arp_op, 2, "should be ARP reply");
    let sender_mac: [u8; 6] = arp[8..14].try_into().unwrap();
    assert_eq!(sender_mac, SVC_MAC, "ARP reply should have service MAC");
}

// --- NAT tests ---

const CLIENT_IP: std::net::Ipv4Addr = std::net::Ipv4Addr::new(10, 0, 0, 1);
const CLIENT_MAC: [u8; 6] = [0x02, 0x00, 0x00, 0x00, 0x00, 0x0a];

#[tokio::test]
async fn service_nat_dnat_rewrites_dst_ip() {
    let mut fabric: Fabric<TestPort> = Fabric::new();

    // Create a service with backend, mark ready.
    {
        let tables = fabric.tables();
        let mut st = tables.service_table.lock().unwrap();
        st.create(
            "svc-nat".into(),
            SVC_IP,
            SVC_MAC,
            distvirt_worker_protocol::ServicePolicy {
                buffer_frames: 64,
                timeout_ms: 30000,
                activator: None,
            },
            None,
            None,
        );
        st.update_backend("svc-nat", Some((POD_IP, POD_MAC)));
        st.mark_ready("svc-nat");
    }

    let (port0, handle0) = make_test_port();
    let (port1, handle1) = make_test_port();
    let (_id0, _task0) = fabric.add_port_raw(port0);
    let (_id1, _task1) = fabric.add_port_raw(port1);

    // Learn POD_MAC on port 1.
    let learn_frame = make_frame(BROADCAST, POD_MAC, 0x0806, &[0u8; 28]);
    handle1.inject_tx.send(learn_frame).await.unwrap();
    let _ = try_recv(&handle0).await;

    // Send TCP SYN from client to service IP.
    let syn_frame = make_tcp_frame(
        SVC_MAC, CLIENT_MAC,
        CLIENT_IP.octets(), SVC_IP.octets(),
        12345, 80,
        0x02, // SYN
    );
    handle0.inject_tx.send(syn_frame).await.unwrap();

    // Frame should arrive at port 1 (backend) with DNAT applied.
    let received = try_recv(&handle1).await;
    assert!(received.is_some(), "frame should be forwarded to backend port");
    let received = received.unwrap();

    let ff = FabricFrame::new(&received).unwrap();
    // dst MAC should be rewritten to POD_MAC.
    assert_eq!(ff.dst_mac(), POD_MAC, "dst MAC should be rewritten to backend MAC");
    // dst IP should be rewritten from SVC_IP to POD_IP.
    assert_eq!(ff.ipv4_dst(), Some(POD_IP), "dst IP should be DNAT'd to backend IP");
    // src IP should be unchanged.
    assert_eq!(ff.ipv4_src(), Some(CLIENT_IP), "src IP should be unchanged");
}

#[tokio::test]
async fn service_nat_snat_rewrites_return_traffic() {
    let mut fabric: Fabric<TestPort> = Fabric::new();

    // Create a service with backend, mark ready.
    {
        let tables = fabric.tables();
        let mut st = tables.service_table.lock().unwrap();
        st.create(
            "svc-nat".into(),
            SVC_IP,
            SVC_MAC,
            distvirt_worker_protocol::ServicePolicy {
                buffer_frames: 64,
                timeout_ms: 30000,
                activator: None,
            },
            None,
            None,
        );
        st.update_backend("svc-nat", Some((POD_IP, POD_MAC)));
        st.mark_ready("svc-nat");
    }

    let (port0, handle0) = make_test_port();
    let (port1, handle1) = make_test_port();
    let (_id0, _task0) = fabric.add_port_raw(port0);
    let (_id1, _task1) = fabric.add_port_raw(port1);

    // Learn CLIENT_MAC on port 0, POD_MAC on port 1.
    let learn0 = make_frame(BROADCAST, CLIENT_MAC, 0x0806, &[0u8; 28]);
    handle0.inject_tx.send(learn0).await.unwrap();
    let _ = try_recv(&handle1).await;

    let learn1 = make_frame(BROADCAST, POD_MAC, 0x0806, &[0u8; 28]);
    handle1.inject_tx.send(learn1).await.unwrap();
    let _ = try_recv(&handle0).await;

    // Step 1: Send forward traffic (client→service) to install NAT entry.
    let syn_frame = make_tcp_frame(
        SVC_MAC, CLIENT_MAC,
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
        CLIENT_MAC, POD_MAC,
        POD_IP.octets(), CLIENT_IP.octets(),
        80, 12345,
        0x12, // SYN+ACK
    );
    handle1.inject_tx.send(syn_ack_frame).await.unwrap();

    // Frame should arrive at port 0 with SNAT applied.
    let received = try_recv(&handle0).await;
    assert!(received.is_some(), "return frame should arrive at client port");
    let received = received.unwrap();

    let ff = FabricFrame::new(&received).unwrap();
    // src MAC should be rewritten to SVC_MAC.
    assert_eq!(ff.src_mac(), SVC_MAC, "src MAC should be SNAT'd to service MAC");
    // src IP should be rewritten from POD_IP to SVC_IP.
    assert_eq!(ff.ipv4_src(), Some(SVC_IP), "src IP should be SNAT'd to service IP");
    // dst should be unchanged.
    assert_eq!(ff.dst_mac(), CLIENT_MAC, "dst MAC should be unchanged");
    assert_eq!(ff.ipv4_dst(), Some(CLIENT_IP), "dst IP should be unchanged");
}

#[tokio::test]
async fn non_natted_unicast_not_affected() {
    // Regular unicast traffic that doesn't match any NAT entry should pass through unchanged.
    let mut fabric: Fabric<TestPort> = Fabric::new();

    let (port0, handle0) = make_test_port();
    let (port1, handle1) = make_test_port();
    let (_id0, _task0) = fabric.add_port_raw(port0);
    let (_id1, _task1) = fabric.add_port_raw(port1);

    // Learn MAC_A on port 0.
    let learn = make_frame(BROADCAST, MAC_A, 0x0806, &[0u8; 28]);
    handle0.inject_tx.send(learn).await.unwrap();
    let _ = try_recv(&handle1).await;

    // Port 1 sends unicast to MAC_A — not NAT'd, should pass through unchanged.
    let frame = make_tcp_frame(
        MAC_A, MAC_B,
        [10, 0, 0, 2], [10, 0, 0, 1],
        5000, 8080,
        0x02,
    );
    handle1.inject_tx.send(frame.clone()).await.unwrap();

    let received = try_recv(&handle0).await;
    assert!(received.is_some(), "frame should be forwarded");
    let received = received.unwrap();

    // Frame should be byte-identical (no NAT rewrite).
    assert_eq!(received, frame, "non-NAT'd unicast should pass through unchanged");
}

#[tokio::test]
async fn service_nat_ip_checksum_valid() {
    // Verify that the IP header checksum is still valid after DNAT.
    let mut fabric: Fabric<TestPort> = Fabric::new();

    {
        let tables = fabric.tables();
        let mut st = tables.service_table.lock().unwrap();
        st.create(
            "svc-nat".into(),
            SVC_IP,
            SVC_MAC,
            distvirt_worker_protocol::ServicePolicy {
                buffer_frames: 64,
                timeout_ms: 30000,
                activator: None,
            },
            None,
            None,
        );
        st.update_backend("svc-nat", Some((POD_IP, POD_MAC)));
        st.mark_ready("svc-nat");
    }

    let (port0, handle0) = make_test_port();
    let (port1, handle1) = make_test_port();
    let (_id0, _task0) = fabric.add_port_raw(port0);
    let (_id1, _task1) = fabric.add_port_raw(port1);

    // Learn POD_MAC on port 1.
    let learn = make_frame(BROADCAST, POD_MAC, 0x0806, &[0u8; 28]);
    handle1.inject_tx.send(learn).await.unwrap();
    let _ = try_recv(&handle0).await;

    let syn = make_tcp_frame(
        SVC_MAC, CLIENT_MAC,
        CLIENT_IP.octets(), SVC_IP.octets(),
        12345, 80,
        0x02,
    );
    handle0.inject_tx.send(syn).await.unwrap();

    let received = try_recv(&handle1).await.unwrap();
    let ff = FabricFrame::new(&received).unwrap();
    let eth = ff.eth_payload();

    // Verify IP header checksum: compute from scratch and compare.
    let ip_hdr = &eth[14..34]; // 20-byte IP header
    let mut sum: u32 = 0;
    for i in (0..20).step_by(2) {
        sum += u16::from_be_bytes([ip_hdr[i], ip_hdr[i + 1]]) as u32;
    }
    while sum >> 16 != 0 {
        sum = (sum & 0xffff) + (sum >> 16);
    }
    assert_eq!(!sum as u16, 0, "IP header checksum should be valid after DNAT");
}
