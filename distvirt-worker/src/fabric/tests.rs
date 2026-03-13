use super::*;
use super::ServiceProcessor;
use super::endpoint::EndpointTable;
use crate::packet::{FabricPacket, FABRIC_HDR_SZ, with_fabric_header};
use distvirt_worker_protocol::{
    EndpointKind, EndpointPlacement, EndpointPodBackend, EndpointSpec, ServiceId, ServicePolicy,
    WorkerId,
};
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

/// Create a LocalPod endpoint entry for an IP so that add_port_raw_with_ip can attach to it.
fn create_local_pod_endpoint(fabric: &Fabric<TestPort>, ip: Ipv4Addr) {
    use crate::fabric::ServiceProcessor;
    let tables = fabric.tables();
    let mut et = tables.endpoint_table.lock().unwrap();
    let mut noop = |_: &str, _: &ServicePolicy, _: Ipv4Addr| ServiceProcessor::Passthrough;
    et.apply_endpoint_update(
        vec![EndpointSpec {
            ip,
            kind: EndpointKind::Pod {
                placement: Some(EndpointPlacement {
                    worker_id: WorkerId::from(OWN_WORKER),
                }),
            },
        }],
        vec![],
        OWN_WORKER,
        &mut noop,
        None,
    );
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

/// Yield to the tokio runtime repeatedly, allowing background tasks
/// (port_read_loop, flush tasks) to make progress.
/// In the single-threaded test runtime, this is deterministic.
async fn yield_until_idle() {
    for _ in 0..20 {
        tokio::task::yield_now().await;
    }
}

// --- L3 routing tests ---

#[tokio::test]
async fn ipv4_frame_routes_to_correct_port_by_ip() {
    let fabric = make_test_fabric();
    let (port0, handle0) = make_test_port();
    let (port1, handle1) = make_test_port();

    create_local_pod_endpoint(&fabric, IP_A);
    create_local_pod_endpoint(&fabric, IP_B);
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

    create_local_pod_endpoint(&fabric, IP_A);
    create_local_pod_endpoint(&fabric, IP_B);
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

    create_local_pod_endpoint(&fabric, IP_A);
    create_local_pod_endpoint(&fabric, IP_B);
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

    create_local_pod_endpoint(&fabric, IP_A);
    create_local_pod_endpoint(&fabric, IP_B);
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

    create_local_pod_endpoint(&fabric, IP_A);
    create_local_pod_endpoint(&fabric, IP_B);
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

    create_local_pod_endpoint(&fabric, IP_A);
    create_local_pod_endpoint(&fabric, IP_B);
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
    use crate::fabric::ServiceProcessor;

    let fabric = make_test_fabric();
    let (event_tx, mut event_rx) = tokio_mpsc::channel(64);
    fabric.set_event_channel(event_tx);

    let pod_ip = Ipv4Addr::new(172, 16, 0, 10);

    // Add an unplaced pod endpoint for pod_ip.
    {
        let tables = fabric.tables();
        let mut et = tables.endpoint_table.lock().unwrap();
        let mut noop = |_: &str, _: &ServicePolicy, _: Ipv4Addr| ServiceProcessor::Passthrough;
        et.apply_endpoint_sync(vec![EndpointSpec {
            ip: pod_ip,
            kind: EndpointKind::Pod { placement: None },
        }], "local-worker", &mut noop, None);
    }

    let (port0, handle0) = make_test_port();
    let (port1, handle1) = make_test_port();

    create_local_pod_endpoint(&fabric, IP_A);
    create_local_pod_endpoint(&fabric, IP_B);
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

    create_local_pod_endpoint(&fabric, IP_A);
    create_local_pod_endpoint(&fabric, IP_B);
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
    use crate::fabric::ServiceProcessor;

    let fabric = make_test_fabric();

    let pod_ip = Ipv4Addr::new(172, 16, 0, 10);

    // Add an unplaced pod endpoint.
    {
        let tables = fabric.tables();
        let mut et = tables.endpoint_table.lock().unwrap();
        let mut noop = |_: &str, _: &ServicePolicy, _: Ipv4Addr| ServiceProcessor::Passthrough;
        et.apply_endpoint_sync(vec![EndpointSpec {
            ip: pod_ip,
            kind: EndpointKind::Pod { placement: None },
        }], "local-worker", &mut noop, None);
    }

    let (port0, handle0) = make_test_port();
    create_local_pod_endpoint(&fabric, IP_A);
    let (_id0, _task0) = fabric.add_port_raw_with_ip(port0, IP_A);

    // Send 3 frames to the placeholder IP.
    for _ in 0..3 {
        let frame = make_ipv4_frame(pod_ip);
        handle0.inject_tx.send(frame).await.unwrap();
    }

    // Let the port read loop process the frames.
    yield_until_idle().await;

    // Update pod_ip from UnplacedPod to LocalPod so attach_port works.
    create_local_pod_endpoint(&fabric, pod_ip);

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
    use crate::fabric::ServiceProcessor;

    let fabric = make_test_fabric();
    let (event_tx, mut event_rx) = tokio_mpsc::channel(64);
    fabric.set_event_channel(event_tx);

    let pod_ip = Ipv4Addr::new(172, 16, 0, 10);

    {
        let tables = fabric.tables();
        let mut et = tables.endpoint_table.lock().unwrap();
        let mut noop = |_: &str, _: &ServicePolicy, _: Ipv4Addr| ServiceProcessor::Passthrough;
        et.apply_endpoint_sync(vec![EndpointSpec {
            ip: pod_ip,
            kind: EndpointKind::Pod { placement: None },
        }], "local-worker", &mut noop, None);
    }

    let (port0, handle0) = make_test_port();
    create_local_pod_endpoint(&fabric, IP_A);
    let (_id0, _task0) = fabric.add_port_raw_with_ip(port0, IP_A);

    // Send multiple frames rapidly.
    for _ in 0..5 {
        let frame = make_ipv4_frame(pod_ip);
        handle0.inject_tx.send(frame).await.unwrap();
    }

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
const OWN_WORKER: &str = "local-worker";

/// Default make_processor that returns Passthrough for all services.
fn passthrough_processor(_: &str, _: &ServicePolicy, _: Ipv4Addr) -> ServiceProcessor {
    ServiceProcessor::Passthrough
}

fn default_service_policy() -> ServicePolicy {
    ServicePolicy {
        buffer_frames: 64,
        timeout_ms: 30000,
        activator: None,
    }
}

fn l4_tcp_policy() -> ServicePolicy {
    ServicePolicy {
        buffer_frames: 64,
        timeout_ms: 30000,
        activator: Some(distvirt_worker_protocol::ActivatorConfig::Tcp {
            ports: None,
            tcp_only: false,
            max_flows: 1024,
        }),
    }
}

/// Create a service endpoint with no backend via apply_endpoint_sync on a locked table.
fn table_create_service(
    et: &mut EndpointTable,
    service_id: &str,
    ip: Ipv4Addr,
    policy: ServicePolicy,
    make_processor: &mut dyn FnMut(&str, &ServicePolicy, Ipv4Addr) -> ServiceProcessor,
) {
    et.apply_endpoint_sync(
        vec![EndpointSpec {
            ip,
            kind: EndpointKind::Service {
                service_id: ServiceId::from(service_id),
                policy,
                backend: None,
            },
        }],
        OWN_WORKER,
        make_processor,
        None,
    );
}

/// Update a service's backend via apply_endpoint_update (sets Pending or Buffering).
fn table_update_backend(
    et: &mut EndpointTable,
    service_id: &str,
    ip: Ipv4Addr,
    policy: ServicePolicy,
    backend_ip: Option<Ipv4Addr>,
    make_processor: &mut dyn FnMut(&str, &ServicePolicy, Ipv4Addr) -> ServiceProcessor,
) {
    let backend = backend_ip.map(|pod_ip| EndpointPodBackend {
        pod_ip,
        placement: None,
        ready: false,
    });
    et.apply_endpoint_update(
        vec![EndpointSpec {
            ip,
            kind: EndpointKind::Service {
                service_id: ServiceId::from(service_id),
                policy,
                backend,
            },
        }],
        vec![],
        OWN_WORKER,
        make_processor,
        None,
    );
}

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
        let mut make_l3 = {
            let mut instance_opt = Some(instance);
            move |_: &str, _: &ServicePolicy, _: Ipv4Addr| -> ServiceProcessor {
                ServiceProcessor::L3 {
                    activator: instance_opt.take().unwrap(),
                    flow_tracker: distvirt_activator::FlowTracker::new(),
                }
            }
        };
        table_create_service(&mut st, "svc-tcp", SVC_IP, l4_tcp_policy(), &mut make_l3);
    }

    let (port0, handle0) = make_test_port();
    let (port1, handle1) = make_test_port();
    create_local_pod_endpoint(&fabric, IP_A);
    create_local_pod_endpoint(&fabric, IP_B);
    let (_id0, _task0) = fabric.add_port_raw_with_ip(port0, IP_A);
    let (_id1, _task1) = fabric.add_port_raw_with_ip(port1, IP_B);

    // Inject TCP SYN addressed to service IP from port 0.
    let syn_frame = make_tcp_frame(
        [10, 0, 0, 1], SVC_IP.octets(),
        12345, 80,
        0x02, // SYN
    );
    handle0.inject_tx.send(syn_frame).await.unwrap();

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
        let mut make_l3 = {
            let mut instance_opt = Some(instance);
            move |_: &str, _: &ServicePolicy, _: Ipv4Addr| -> ServiceProcessor {
                ServiceProcessor::L3 {
                    activator: instance_opt.take().unwrap(),
                    flow_tracker: distvirt_activator::FlowTracker::new(),
                }
            }
        };
        table_create_service(&mut st, "svc-tcp", SVC_IP, l4_tcp_policy(), &mut make_l3);
    }

    let (port0, handle0) = make_test_port();
    let (port1, handle1) = make_test_port();
    create_local_pod_endpoint(&fabric, IP_A);
    create_local_pod_endpoint(&fabric, IP_B);
    let (_id0, _task0) = fabric.add_port_raw_with_ip(port0, IP_A);
    let (_id1, _task1) = fabric.add_port_raw_with_ip(port1, IP_B);

    // Inject TCP RST.
    let rst_frame = make_tcp_frame(
        [10, 0, 0, 1], SVC_IP.octets(),
        12345, 80,
        0x04, // RST
    );
    handle0.inject_tx.send(rst_frame).await.unwrap();

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

    // Create service with TCP activator, set backend, and mark ready.
    {
        let tables = fabric.tables();
        let mut st = tables.endpoint_table.lock().unwrap();
        let mut make_l3 = {
            let mut instance_opt = Some(instance);
            move |_: &str, _: &ServicePolicy, _: Ipv4Addr| -> ServiceProcessor {
                ServiceProcessor::L3 {
                    activator: instance_opt.take().unwrap(),
                    flow_tracker: distvirt_activator::FlowTracker::new(),
                }
            }
        };
        table_create_service(&mut st, "svc-tcp", SVC_IP, l4_tcp_policy(), &mut make_l3);
        table_update_backend(&mut st, "svc-tcp", SVC_IP, l4_tcp_policy(), Some(POD_IP), &mut passthrough_processor);
        st.mark_service_ready("svc-tcp");
    }

    let (port0, handle0) = make_test_port();
    let (port1, handle1) = make_test_port();
    // Register port 1 with POD_IP/POD_MAC so fabric can route to it.
    create_local_pod_endpoint(&fabric, IP_A);
    create_local_pod_endpoint(&fabric, POD_IP);
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
        table_create_service(&mut st, "svc-fwd", SVC_IP, default_service_policy(), &mut passthrough_processor);
        table_update_backend(&mut st, "svc-fwd", SVC_IP, default_service_policy(), Some(POD_IP), &mut passthrough_processor);
        st.mark_service_ready("svc-fwd");
    }

    // Add client port (port 0).
    let (port0, handle0) = make_test_port();
    create_local_pod_endpoint(&fabric, IP_A);
    let (_id0, _task0) = fabric.add_port_raw_with_ip(port0, IP_A);

    // Send TCP SYN from client (port 0) to service VIP.
    let syn_frame = make_tcp_frame(
        [10, 0, 0, 1], SVC_IP.octets(),
        12345, 80,
        0x02, // SYN
    );
    handle0.inject_tx.send(syn_frame).await.unwrap();

    // Let the port read loop process the frame.
    yield_until_idle().await;

    // Now add the backend port with IP+MAC — triggers flush_by_backend_ip.
    let (port1, handle1) = make_test_port();
    create_local_pod_endpoint(&fabric, POD_IP);
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
        table_create_service(&mut st, "svc-nat", SVC_IP, default_service_policy(), &mut passthrough_processor);
        table_update_backend(&mut st, "svc-nat", SVC_IP, default_service_policy(), Some(POD_IP), &mut passthrough_processor);
        st.mark_service_ready("svc-nat");
    }

    let (port0, handle0) = make_test_port();
    let (port1, handle1) = make_test_port();
    create_local_pod_endpoint(&fabric, CLIENT_IP);
    create_local_pod_endpoint(&fabric, POD_IP);
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
        table_create_service(&mut st, "svc-nat", SVC_IP, default_service_policy(), &mut passthrough_processor);
        table_update_backend(&mut st, "svc-nat", SVC_IP, default_service_policy(), Some(POD_IP), &mut passthrough_processor);
        st.mark_service_ready("svc-nat");
    }

    let (port0, handle0) = make_test_port();
    let (port1, handle1) = make_test_port();
    create_local_pod_endpoint(&fabric, CLIENT_IP);
    create_local_pod_endpoint(&fabric, POD_IP);
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
    create_local_pod_endpoint(&fabric, dst_ip);
    create_local_pod_endpoint(&fabric, src_ip);
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
        table_create_service(&mut st, "svc-nat", SVC_IP, default_service_policy(), &mut passthrough_processor);
        table_update_backend(&mut st, "svc-nat", SVC_IP, default_service_policy(), Some(POD_IP), &mut passthrough_processor);
        st.mark_service_ready("svc-nat");
    }

    let (port0, handle0) = make_test_port();
    let (port1, handle1) = make_test_port();
    create_local_pod_endpoint(&fabric, CLIENT_IP);
    create_local_pod_endpoint(&fabric, POD_IP);
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
    let fabric = make_test_fabric();

    let remote_pod_ip = Ipv4Addr::new(10, 0, 0, 50);
    let worker_id = "remote-worker-1";

    // Create a TestPort to act as the tunnel port.
    let (tunnel_port, tunnel_handle) = make_test_port();

    // Register it as a tunnel port.
    let (_port_id, _task) = fabric.add_tunnel_port(worker_id.to_string(), tunnel_port);

    // Add a remote pod endpoint for the remote pod IP.
    {
        use crate::fabric::ServiceProcessor;
        let tables = fabric.tables();
        let mut et = tables.endpoint_table.lock().unwrap();
        let mut noop = |_: &str, _: &ServicePolicy, _: Ipv4Addr| ServiceProcessor::Passthrough;
        et.apply_endpoint_sync(vec![EndpointSpec {
            ip: remote_pod_ip,
            kind: EndpointKind::Pod {
                placement: Some(EndpointPlacement {
                    worker_id: WorkerId::from(worker_id),
                }),
            },
        }], "local-worker", &mut noop, None);
    }

    // Add a local port that sends a frame to the remote pod IP.
    let (port0, handle0) = make_test_port();
    create_local_pod_endpoint(&fabric, IP_A);
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

// --- PortGuard integration test ---

#[tokio::test]
async fn port_guard_drop_returns_endpoint_to_buffering() {
    let fabric = make_test_fabric();

    let pod_ip = Ipv4Addr::new(10, 0, 0, 50);
    create_local_pod_endpoint(&fabric, pod_ip);

    // Add a port for pod_ip.
    let (port0, handle0) = make_test_port();
    let (sender_port, sender_handle) = make_test_port();
    create_local_pod_endpoint(&fabric, IP_A);
    let (_sender_id, _sender_task) = fabric.add_port_raw_with_ip(sender_port, IP_A);
    let (_pod_port_id, pod_task) = fabric.add_port_raw_with_ip(port0, pod_ip);

    // Sanity: frames route to the port.
    let frame = make_ipv4_frame(pod_ip);
    sender_handle.inject_tx.send(frame).await.unwrap();
    let received = try_recv(&handle0).await;
    assert!(received.is_some(), "frame should reach pod port before drop");

    // Drop the TaskHandle — this aborts the port read loop, which drops
    // the PortGuard, which calls detach_port.
    drop(pod_task);

    // Let the runtime process the abort and run PortGuard::drop.
    yield_until_idle().await;

    // Verify the endpoint went back to buffering: send another frame.
    // It shouldn't arrive at the (now closed) port, and should be buffered.
    let frame2 = make_ipv4_frame(pod_ip);
    sender_handle.inject_tx.send(frame2).await.unwrap();

    // The old port handle should NOT receive a new frame.
    assert_no_frame(&handle0).await;

    // Verify that the endpoint table is in buffering mode by checking
    // that frames are buffered (we can verify by adding a new port and
    // getting the buffered frames).
    let (port_new, handle_new) = make_test_port();
    create_local_pod_endpoint(&fabric, pod_ip);
    let (_new_id, _new_task) = fabric.add_port_raw_with_ip(port_new, pod_ip);

    let flushed = try_recv(&handle_new).await;
    assert!(flushed.is_some(), "new port should receive the buffered frame from after drop");
}

// --- UDP frame helper ---

/// Build a valid IPv4+UDP fabric frame [fabric_hdr(3)][IP+UDP+payload].
fn make_udp_frame(
    src_ip: [u8; 4],
    dst_ip: [u8; 4],
    src_port: u16,
    dst_port: u16,
    payload: &[u8],
) -> Vec<u8> {
    use etherparse::PacketBuilder;

    let builder = PacketBuilder::ipv4(src_ip, dst_ip, 64).udp(src_port, dst_port);

    let mut ip_packet = Vec::new();
    builder.write(&mut ip_packet, payload).unwrap();

    with_fabric_header(0, 0, &ip_packet)
}

/// Build a minimal DNS A-record query in wire format.
fn make_dns_query(id: u16, name: &str) -> Vec<u8> {
    let mut buf = Vec::new();

    // Header: ID
    buf.extend_from_slice(&id.to_be_bytes());
    // Flags: standard query (RD set)
    buf.push(0x01);
    buf.push(0x00);
    // QDCOUNT = 1
    buf.push(0x00);
    buf.push(0x01);
    // ANCOUNT, NSCOUNT, ARCOUNT = 0
    buf.extend_from_slice(&[0u8; 6]);

    // QNAME: length-prefixed labels
    for label in name.split('.') {
        buf.push(label.len() as u8);
        buf.extend_from_slice(label.as_bytes());
    }
    buf.push(0); // terminator

    // QTYPE = A (1)
    buf.push(0x00);
    buf.push(0x01);
    // QCLASS = IN (1)
    buf.push(0x00);
    buf.push(0x01);

    buf
}

// =========================================================================
// Phase 1: Flow tracking integration tests
// =========================================================================

#[tokio::test]
async fn flow_event_on_tcp_syn_to_local_pod() {
    let fabric = make_test_fabric();
    let (event_tx, mut event_rx) = tokio_mpsc::channel(64);
    fabric.set_event_channel(event_tx);

    let (port0, handle0) = make_test_port();
    let (port1, handle1) = make_test_port();
    create_local_pod_endpoint(&fabric, IP_A);
    create_local_pod_endpoint(&fabric, IP_B);
    let (_id0, _task0) = fabric.add_port_raw_with_ip(port0, IP_A);
    let (_id1, _task1) = fabric.add_port_raw_with_ip(port1, IP_B);

    // Send TCP SYN from IP_A to IP_B (Opening — no flow event).
    let syn = make_tcp_frame(IP_A.octets(), IP_B.octets(), 12345, 80, 0x02);
    handle0.inject_tx.send(syn).await.unwrap();
    let _ = try_recv(&handle1).await;

    // Opening flows don't count as active, so no event.
    let event = try_recv_event(&mut event_rx).await;
    assert!(event.is_none(), "SYN-only (Opening) should not produce a flow event, got {:?}", event);

    // Send ACK to transition to Established.
    let ack = make_tcp_frame(IP_A.octets(), IP_B.octets(), 12345, 80, 0x10);
    handle0.inject_tx.send(ack).await.unwrap();
    let _ = try_recv(&handle1).await;

    // Should get EndpointFlowStatus { has_active_flows: true } for IP_B.
    let event = try_recv_event(&mut event_rx).await;
    assert!(
        matches!(event, Some(FabricEvent::EndpointFlowStatus { ip, has_active_flows: true, .. }) if ip == IP_B),
        "expected EndpointFlowStatus(active=true) for IP_B, got {:?}",
        event
    );
}

#[tokio::test]
async fn flow_event_inactive_after_rst() {
    let fabric = make_test_fabric();
    let (event_tx, mut event_rx) = tokio_mpsc::channel(64);
    fabric.set_event_channel(event_tx);

    let (port0, handle0) = make_test_port();
    let (port1, handle1) = make_test_port();
    create_local_pod_endpoint(&fabric, IP_A);
    create_local_pod_endpoint(&fabric, IP_B);
    let (_id0, _task0) = fabric.add_port_raw_with_ip(port0, IP_A);
    let (_id1, _task1) = fabric.add_port_raw_with_ip(port1, IP_B);

    // Send TCP SYN (Opening — no event yet).
    let syn = make_tcp_frame(IP_A.octets(), IP_B.octets(), 12345, 80, 0x02);
    handle0.inject_tx.send(syn).await.unwrap();
    let _ = try_recv(&handle1).await;

    // Send ACK → Established → active.
    let ack = make_tcp_frame(IP_A.octets(), IP_B.octets(), 12345, 80, 0x10);
    handle0.inject_tx.send(ack).await.unwrap();
    let _ = try_recv(&handle1).await;

    let event = try_recv_event(&mut event_rx).await;
    assert!(
        matches!(event, Some(FabricEvent::EndpointFlowStatus { has_active_flows: true, .. })),
        "first event should be active=true"
    );

    // Send TCP RST → inactive.
    let rst = make_tcp_frame(IP_A.octets(), IP_B.octets(), 12345, 80, 0x04);
    handle0.inject_tx.send(rst).await.unwrap();
    let _ = try_recv(&handle1).await;

    let event = try_recv_event(&mut event_rx).await;
    assert!(
        matches!(event, Some(FabricEvent::EndpointFlowStatus { has_active_flows: false, .. })),

        "second event should be active=false"
    );
}

#[tokio::test]
async fn no_flow_event_on_non_tcp() {
    let fabric = make_test_fabric();
    let (event_tx, mut event_rx) = tokio_mpsc::channel(64);
    fabric.set_event_channel(event_tx);

    let (port0, handle0) = make_test_port();
    let (port1, handle1) = make_test_port();
    create_local_pod_endpoint(&fabric, IP_A);
    create_local_pod_endpoint(&fabric, IP_B);
    let (_id0, _task0) = fabric.add_port_raw_with_ip(port0, IP_A);
    let (_id1, _task1) = fabric.add_port_raw_with_ip(port1, IP_B);

    // Send UDP frame from IP_A to IP_B.
    let udp = make_udp_frame(IP_A.octets(), IP_B.octets(), 5000, 5001, b"hello");
    handle0.inject_tx.send(udp).await.unwrap();

    // Frame should be delivered.
    let received = try_recv(&handle1).await;
    assert!(received.is_some(), "UDP frame should be delivered");

    // No EndpointFlowStatus event (flow tracking is TCP-only).
    let event = try_recv_event(&mut event_rx).await;
    assert!(
        !matches!(event, Some(FabricEvent::EndpointFlowStatus { .. })),

        "UDP traffic should not produce flow status events"
    );
}

#[tokio::test]
async fn dnat_rewrite_does_not_double_count_flows() {
    let fabric = make_test_fabric();
    let (event_tx, mut event_rx) = tokio_mpsc::channel(64);
    fabric.set_event_channel(event_tx);

    // Create a passthrough service with ready backend.
    {
        let tables = fabric.tables();
        let mut st = tables.endpoint_table.lock().unwrap();
        table_create_service(&mut st, "svc-flow", SVC_IP, default_service_policy(), &mut passthrough_processor);
        table_update_backend(&mut st, "svc-flow", SVC_IP, default_service_policy(), Some(POD_IP), &mut passthrough_processor);
        st.mark_service_ready("svc-flow");
    }

    let (port0, handle0) = make_test_port();
    let (port1, handle1) = make_test_port();
    create_local_pod_endpoint(&fabric, CLIENT_IP);
    create_local_pod_endpoint(&fabric, POD_IP);
    let (_id0, _task0) = fabric.add_port_raw_with_ip(port0, CLIENT_IP);
    let (_id1, _task1) = fabric.add_port_raw_with_ip(port1, POD_IP);

    // Send TCP SYN to service VIP → DNAT → re-dispatch to backend.
    let syn = make_tcp_frame(CLIENT_IP.octets(), SVC_IP.octets(), 12345, 80, 0x02);
    handle0.inject_tx.send(syn).await.unwrap();

    // Frame should arrive at backend.
    let received = try_recv(&handle1).await;
    assert!(received.is_some(), "frame should reach backend");

    // Collect all flow status events — should be at most one.
    let mut flow_events = Vec::new();
    while let Some(event) = try_recv_event(&mut event_rx).await {
        if matches!(event, FabricEvent::EndpointFlowStatus { .. }) {
            flow_events.push(event);
        }
    }
    assert!(
        flow_events.len() <= 1,
        "expected at most 1 flow status event (skip_flow_tracking on re-dispatch), got {}",
        flow_events.len()
    );
}

// =========================================================================
// Phase 2: Service edge case tests
// =========================================================================

#[tokio::test]
async fn service_ready_backend_unreachable_buffers() {
    let fabric = make_test_fabric();

    // Create service with backend IP, mark ready, but do NOT register a port for the backend IP.
    {
        let tables = fabric.tables();
        let mut st = tables.endpoint_table.lock().unwrap();
        table_create_service(&mut st, "svc-unreach", SVC_IP, default_service_policy(), &mut passthrough_processor);
        table_update_backend(&mut st, "svc-unreach", SVC_IP, default_service_policy(), Some(POD_IP), &mut passthrough_processor);
        st.mark_service_ready("svc-unreach");
    }

    // Only register the client port.
    let (port0, handle0) = make_test_port();
    create_local_pod_endpoint(&fabric, CLIENT_IP);
    let (_id0, _task0) = fabric.add_port_raw_with_ip(port0, CLIENT_IP);

    // Send frame to VIP — backend not reachable, should be buffered.
    let syn = make_tcp_frame(CLIENT_IP.octets(), SVC_IP.octets(), 12345, 80, 0x02);
    handle0.inject_tx.send(syn).await.unwrap();

    // Let the port read loop buffer the frame.
    yield_until_idle().await;

    // Client should not receive anything (frame was buffered, not bounced).
    assert_no_frame(&handle0).await;

    // Now add the backend port → should flush the buffered frame.
    let (port1, handle1) = make_test_port();
    create_local_pod_endpoint(&fabric, POD_IP);
    let (_id1, _task1) = fabric.add_port_raw_with_ip(port1, POD_IP);

    let received = try_recv(&handle1).await;
    assert!(received.is_some(), "buffered frame should flush to newly added backend port");
}

#[tokio::test]
async fn service_buffer_capacity_drops_excess() {
    let fabric = make_test_fabric();

    let small_policy = ServicePolicy {
        buffer_frames: 2,
        timeout_ms: 30000,
        activator: None,
    };

    // Create service with buffer_frames=2.
    {
        let tables = fabric.tables();
        let mut st = tables.endpoint_table.lock().unwrap();
        table_create_service(&mut st, "svc-cap", SVC_IP, small_policy.clone(), &mut passthrough_processor);
    }

    let (port0, handle0) = make_test_port();
    create_local_pod_endpoint(&fabric, CLIENT_IP);
    let (_id0, _task0) = fabric.add_port_raw_with_ip(port0, CLIENT_IP);

    // Send 5 frames — only first 2 should be buffered.
    for i in 0..5u16 {
        let frame = make_tcp_frame(
            CLIENT_IP.octets(), SVC_IP.octets(),
            10000 + i, 80, 0x02,
        );
        handle0.inject_tx.send(frame).await.unwrap();
    }

    // Let the port read loop buffer the frames.
    yield_until_idle().await;

    // Add backend port first, then mark ready so flush_service_frames can deliver.
    let (port1, handle1) = make_test_port();
    create_local_pod_endpoint(&fabric, POD_IP);
    let (_id1, _task1) = fabric.add_port_raw_with_ip(port1, POD_IP);

    // Mark ready with backend — returns buffered frames.
    let flush_data = {
        let tables = fabric.tables();
        let mut st = tables.endpoint_table.lock().unwrap();
        table_update_backend(&mut st, "svc-cap", SVC_IP, small_policy, Some(POD_IP), &mut passthrough_processor);
        st.mark_service_ready("svc-cap")
    };

    // Manually flush the frames through the fabric (as worker code does).
    if let Some(super::MarkReadyResult::Passthrough { frames, backend_ip, service_ip, .. }) = flush_data {
        fabric.flush_service_frames(frames, backend_ip, service_ip);
    }

    // Let the spawned flush task complete.
    yield_until_idle().await;

    // Count flushed frames.
    let mut count = 0;
    while try_recv(&handle1).await.is_some() {
        count += 1;
    }
    assert_eq!(count, 2, "only 2 frames should be buffered (capacity limit)");
}

#[tokio::test]
async fn service_buffer_timeout_clears() {
    let fabric = make_test_fabric();

    let short_timeout_policy = ServicePolicy {
        buffer_frames: 64,
        timeout_ms: 1, // 1ms timeout
        activator: None,
    };

    {
        let tables = fabric.tables();
        let mut st = tables.endpoint_table.lock().unwrap();
        table_create_service(&mut st, "svc-timeout", SVC_IP, short_timeout_policy.clone(), &mut passthrough_processor);
    }

    let (port0, handle0) = make_test_port();
    create_local_pod_endpoint(&fabric, CLIENT_IP);
    let (_id0, _task0) = fabric.add_port_raw_with_ip(port0, CLIENT_IP);

    // Send first frame — starts buffer timer.
    let frame1 = make_tcp_frame(CLIENT_IP.octets(), SVC_IP.octets(), 10000, 80, 0x02);
    handle0.inject_tx.send(frame1).await.unwrap();

    // Wait for timeout to expire (uses std::time::Instant, needs real wall-clock time).
    std::thread::sleep(std::time::Duration::from_millis(2));
    // Let the port read loop process the first frame.
    yield_until_idle().await;

    // Send second frame — buffer has timed out, should be dropped.
    let frame2 = make_tcp_frame(CLIENT_IP.octets(), SVC_IP.octets(), 10001, 80, 0x02);
    handle0.inject_tx.send(frame2).await.unwrap();

    yield_until_idle().await;

    // Mark ready + add backend port → verify no frames delivered (all expired/dropped).
    {
        let tables = fabric.tables();
        let mut st = tables.endpoint_table.lock().unwrap();
        table_update_backend(&mut st, "svc-timeout", SVC_IP, short_timeout_policy, Some(POD_IP), &mut passthrough_processor);
        st.mark_service_ready("svc-timeout");
    }
    let (port1, handle1) = make_test_port();
    create_local_pod_endpoint(&fabric, POD_IP);
    let (_id1, _task1) = fabric.add_port_raw_with_ip(port1, POD_IP);

    assert_no_frame(&handle1).await;
}

#[tokio::test]
async fn no_gateway_drops_external_frames() {
    // Create fabric WITHOUT setting a gateway.
    let fabric = make_test_fabric();

    let (port0, handle0) = make_test_port();
    let (port1, handle1) = make_test_port();
    create_local_pod_endpoint(&fabric, IP_A);
    create_local_pod_endpoint(&fabric, IP_B);
    let (_id0, _task0) = fabric.add_port_raw_with_ip(port0, IP_A);
    let (_id1, _task1) = fabric.add_port_raw_with_ip(port1, IP_B);

    // Send frame to external IP — should be silently dropped (no panic, no delivery).
    let frame = make_ipv4_frame(EXTERNAL_IP);
    handle0.inject_tx.send(frame).await.unwrap();

    // Neither port should receive it.
    assert_no_frame(&handle1).await;
    // No panics = success.
}

// =========================================================================
// Phase 3: Gateway DNS via channel
// =========================================================================

#[tokio::test]
async fn gateway_dns_local_resolve() {
    use super::gateway::{DnsRegistry, FabricGateway};
    use std::sync::RwLock;

    // Create registry with a local name.
    let registry: DnsRegistry = std::sync::Arc::new(RwLock::new(
        [("myservice".to_string(), Ipv4Addr::new(10, 0, 0, 99))].into_iter().collect(),
    ));

    let gw_ip = [10, 0, 0, 1];
    let prefix_len = 24;

    let (gateway, fabric_egress_tx, fabric_ingress_rx, _internet_rx, _internet_tx) =
        FabricGateway::new_channel(registry, gw_ip, prefix_len).unwrap();

    // Create fabric with gateway_ip = 10.0.0.1 (must match gw_ip for routing).
    let fabric: Fabric<TestPort> = Fabric::new(Ipv4Addr::new(10, 0, 0, 1), prefix_len);
    let (event_tx, _event_rx) = tokio_mpsc::channel(64);
    fabric.set_event_channel(event_tx);
    fabric.set_gateway(fabric_egress_tx, fabric_ingress_rx);

    // Spawn gateway.
    let gw_handle = tokio::spawn(async move { gateway.run().await });

    // Create a pod port.
    let pod_ip = Ipv4Addr::new(10, 0, 0, 10);
    let (port0, handle0) = make_test_port();
    create_local_pod_endpoint(&fabric, pod_ip);
    let (_id0, _task0) = fabric.add_port_raw_with_ip(port0, pod_ip);

    // Build DNS query for "myservice".
    let dns_query = make_dns_query(0x1234, "myservice");

    // Send UDP packet from pod to gateway IP:53.
    let dns_frame = make_udp_frame(pod_ip.octets(), gw_ip, 5353, 53, &dns_query);
    handle0.inject_tx.send(dns_frame).await.unwrap();

    // Wait for DNS response to arrive back at pod port.
    // The gateway needs to process: fabric→egress_rx→smoltcp→DNS forwarder→smoltcp→ingress_tx→fabric→pod_port.
    let response = tokio::time::timeout(
        std::time::Duration::from_secs(2),
        async {
            let mut rx = handle0.capture_rx.lock().await;
            rx.recv().await.unwrap()
        },
    )
    .await;

    assert!(response.is_ok(), "should receive DNS response within timeout");
    let response = response.unwrap();

    // Parse the response: skip fabric header + IP + UDP headers to get DNS payload.
    let fp = FabricPacket::new(&response).unwrap();
    let ip_pkt = fp.ip_packet();
    let ihl = ((ip_pkt[0] & 0x0f) as usize) * 4;
    let udp_payload = &ip_pkt[ihl + 8..]; // skip UDP header (8 bytes)

    // Verify DNS response: ID should match, QR bit should be set, and answer should contain 10.0.0.99.
    assert_eq!(udp_payload[0], 0x12, "DNS ID high byte");
    assert_eq!(udp_payload[1], 0x34, "DNS ID low byte");
    assert_ne!(udp_payload[2] & 0x80, 0, "QR bit should be set (response)");

    // Last 4 bytes of the answer section should be the IP 10.0.0.99.
    let len = udp_payload.len();
    assert_eq!(&udp_payload[len - 4..], &[10, 0, 0, 99], "DNS A record should be 10.0.0.99");

    gw_handle.abort();
}

#[tokio::test]
async fn gateway_subnet_filter_drops_external_dst() {
    use super::gateway::FabricGateway;

    let registry = std::sync::Arc::new(std::sync::RwLock::new(std::collections::HashMap::new()));
    let gw_ip = [10, 0, 0, 1];
    let prefix_len = 24;

    let (gateway, _fabric_egress_tx, mut fab_rx, _internet_rx, inet_tx) =
        FabricGateway::new_channel(registry, gw_ip, prefix_len).unwrap();

    let gw_handle = tokio::spawn(async move { gateway.run().await });

    // Inject packet with dst IP outside pod subnet (8.8.8.8) from internet side.
    let external_frame = make_ipv4_frame_full(
        Ipv4Addr::new(1, 2, 3, 4),
        Ipv4Addr::new(8, 8, 8, 8),
    );
    inet_tx.send(external_frame).await.unwrap();

    // Should NOT arrive on fabric ingress.
    let result = tokio::time::timeout(
        std::time::Duration::from_millis(200),
        fab_rx.recv(),
    )
    .await;
    assert!(result.is_err() || result.unwrap().is_none(), "external dst should be filtered out");

    // Inject packet with dst IP inside pod subnet (10.0.0.50).
    let internal_frame = make_ipv4_frame_full(
        Ipv4Addr::new(1, 2, 3, 4),
        Ipv4Addr::new(10, 0, 0, 50),
    );
    inet_tx.send(internal_frame).await.unwrap();

    // Should arrive on fabric ingress.
    let result = tokio::time::timeout(
        std::time::Duration::from_millis(500),
        fab_rx.recv(),
    )
    .await;
    assert!(result.is_ok() && result.unwrap().is_some(), "in-subnet dst should pass through");

    gw_handle.abort();
}

// =========================================================================
// Phase 4: flush_service_frames NAT integration
// =========================================================================

#[tokio::test]
async fn flush_populates_nat_for_return_traffic() {
    let fabric = make_test_fabric();

    // Create a service with backend but do NOT register the backend port yet.
    {
        let tables = fabric.tables();
        let mut st = tables.endpoint_table.lock().unwrap();
        table_create_service(&mut st, "svc-flush-nat", SVC_IP, default_service_policy(), &mut passthrough_processor);
        table_update_backend(&mut st, "svc-flush-nat", SVC_IP, default_service_policy(), Some(POD_IP), &mut passthrough_processor);
        st.mark_service_ready("svc-flush-nat");
    }

    // Add client port.
    let (port0, handle0) = make_test_port();
    create_local_pod_endpoint(&fabric, CLIENT_IP);
    let (_id0, _task0) = fabric.add_port_raw_with_ip(port0, CLIENT_IP);

    // Send TCP SYN from client to service VIP → buffered (backend port not yet added).
    let syn = make_tcp_frame(CLIENT_IP.octets(), SVC_IP.octets(), 12345, 80, 0x02);
    handle0.inject_tx.send(syn).await.unwrap();

    // Let the port read loop buffer the frame.
    yield_until_idle().await;

    // Now add backend port → triggers flush_by_backend_ip with DNAT.
    let (port1, handle1) = make_test_port();
    create_local_pod_endpoint(&fabric, POD_IP);
    let (_id1, _task1) = fabric.add_port_raw_with_ip(port1, POD_IP);

    // Drain DNAT'd frame.
    let flushed = try_recv(&handle1).await;
    assert!(flushed.is_some(), "flushed frame should arrive at backend");

    // Now send return traffic from backend → client. If flush_service_frames
    // correctly inserted reverse NAT entries, SNAT should rewrite src from POD_IP to SVC_IP.
    let syn_ack = make_tcp_frame(POD_IP.octets(), CLIENT_IP.octets(), 80, 12345, 0x12);
    handle1.inject_tx.send(syn_ack).await.unwrap();

    let return_frame = try_recv(&handle0).await;
    assert!(return_frame.is_some(), "return frame should arrive at client");
    let return_frame = return_frame.unwrap();
    let fp = FabricPacket::new(&return_frame).unwrap();
    assert_eq!(
        fp.ipv4_src(), SVC_IP,
        "return traffic src should be SNAT'd from backend IP to service VIP"
    );
    assert_eq!(fp.ipv4_dst(), CLIENT_IP, "return traffic dst should be unchanged");
}
