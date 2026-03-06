use super::*;
use crate::packet::with_fabric_header;
use distvirt_worker_protocol::{EndpointKind, EndpointPodBackend, EndpointSpec, ServiceId, ServicePolicy};

const SVC_IP: Ipv4Addr = Ipv4Addr::new(172, 16, 0, 2);
const POD_IP: Ipv4Addr = Ipv4Addr::new(172, 16, 0, 130);
const FRAME: &[u8] = &[0xDE, 0xAD, 0xBE, 0xEF];
const OWN_WORKER: &str = "test-worker";

fn default_policy() -> ServicePolicy {
    ServicePolicy {
        buffer_frames: 64,
        timeout_ms: 30000,
        activator: None,
    }
}

/// Default make_processor that returns Passthrough for all services.
fn passthrough_processor(_: &str, _: &ServicePolicy, _: Ipv4Addr) -> ServiceProcessor {
    ServiceProcessor::Passthrough
}

/// Create a service endpoint with no backend (Buffering state) via apply_endpoint_sync.
fn sync_create_service(
    table: &mut EndpointTable,
    service_id: &str,
    ip: Ipv4Addr,
    policy: ServicePolicy,
    make_processor: &mut dyn FnMut(&str, &ServicePolicy, Ipv4Addr) -> ServiceProcessor,
) -> Vec<EndpointSyncEffect> {
    table.apply_endpoint_sync(
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
    )
}

/// Update a service's backend via apply_endpoint_update (sets Pending or Buffering).
fn sync_update_backend(
    table: &mut EndpointTable,
    service_id: &str,
    ip: Ipv4Addr,
    policy: ServicePolicy,
    backend_ip: Option<Ipv4Addr>,
    make_processor: &mut dyn FnMut(&str, &ServicePolicy, Ipv4Addr) -> ServiceProcessor,
) -> Vec<EndpointSyncEffect> {
    let backend = backend_ip.map(|pod_ip| EndpointPodBackend {
        pod_ip,
        placement: None,
        ready: false,
    });
    table.apply_endpoint_update(
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
    )
}

/// Remove a service by IP via apply_endpoint_update.
fn sync_remove_service(
    table: &mut EndpointTable,
    ip: Ipv4Addr,
    make_processor: &mut dyn FnMut(&str, &ServicePolicy, Ipv4Addr) -> ServiceProcessor,
) -> Vec<EndpointSyncEffect> {
    table.apply_endpoint_update(vec![], vec![ip], OWN_WORKER, make_processor, None)
}

#[test]
fn unknown_ip_returns_not_found() {
    let mut table = EndpointTable::new();
    let (action, _, _) = table.lookup_and_buffer(SVC_IP, FRAME);
    assert!(matches!(action, EndpointAction::NotFound));
}

#[test]
fn buffers_when_not_ready() {
    let mut table = EndpointTable::new();
    sync_create_service(&mut table, "svc1", SVC_IP, default_policy(), &mut passthrough_processor);

    let (action, activate, _) = table.lookup_and_buffer(SVC_IP, FRAME);
    assert!(matches!(action, EndpointAction::Buffered { service_id: Some(_) }));
    assert!(activate);
}

#[test]
fn forwards_when_ready() {
    let mut table = EndpointTable::new();

    // Create a LocalPod endpoint for the backend pod so it's reachable.
    table.apply_endpoint_sync(
        vec![
            EndpointSpec {
                ip: SVC_IP,
                kind: EndpointKind::Service {
                    service_id: ServiceId::from("svc1"),
                    policy: default_policy(),
                    backend: Some(EndpointPodBackend { pod_ip: POD_IP, placement: Some(distvirt_worker_protocol::EndpointPlacement { worker_id: distvirt_worker_protocol::WorkerId::from(OWN_WORKER) }), ready: true }),
                },
            },
            EndpointSpec {
                ip: POD_IP,
                kind: EndpointKind::Pod {
                    placement: Some(distvirt_worker_protocol::EndpointPlacement {
                        worker_id: distvirt_worker_protocol::WorkerId::from(OWN_WORKER),
                    }),
                },
            },
        ],
        OWN_WORKER,
        &mut passthrough_processor,
        None,
    );
    // Attach a fake port so the LocalPod is reachable.
    table.attach_port(POD_IP, 99);

    let (action, activate, _) = table.lookup_and_buffer(SVC_IP, FRAME);
    assert!(matches!(
        action,
        EndpointAction::ServiceForward { pod_ip, .. }
        if pod_ip == POD_IP
    ));
    assert!(!activate);
}

#[test]
fn mark_ready_returns_buffered_frames() {
    let mut table = EndpointTable::new();
    sync_create_service(&mut table, "svc1", SVC_IP, default_policy(), &mut passthrough_processor);

    // Buffer some frames (no backend yet).
    for _ in 0..3 {
        table.lookup_and_buffer(SVC_IP, FRAME);
    }

    // Set backend (Pending) — preserves buffer since None→Some.
    sync_update_backend(&mut table, "svc1", SVC_IP, default_policy(), Some(POD_IP), &mut passthrough_processor);

    let result = table.mark_service_ready("svc1");
    match result.unwrap() {
        MarkReadyResult::Passthrough { frames, service_ip, .. } => {
            assert_eq!(frames.len(), 3);
            assert_eq!(service_ip, SVC_IP);
        }
        _ => panic!("expected Passthrough result"),
    }
}

#[test]
fn update_backend_clears_ready_and_buffer() {
    let mut table = EndpointTable::new();
    sync_create_service(&mut table, "svc1", SVC_IP, default_policy(), &mut passthrough_processor);
    sync_update_backend(&mut table, "svc1", SVC_IP, default_policy(), Some(POD_IP), &mut passthrough_processor);
    table.mark_service_ready("svc1");

    // Service is ready — now remove backend (Buffering state).
    sync_update_backend(&mut table, "svc1", SVC_IP, default_policy(), None, &mut passthrough_processor);

    let (action, _, _) = table.lookup_and_buffer(SVC_IP, FRAME);
    assert!(matches!(action, EndpointAction::Buffered { .. }));
}

#[test]
fn destroy_removes_service() {
    let mut table = EndpointTable::new();
    sync_create_service(&mut table, "svc1", SVC_IP, default_policy(), &mut passthrough_processor);
    sync_remove_service(&mut table, SVC_IP, &mut passthrough_processor);
    let (action, _, _) = table.lookup_and_buffer(SVC_IP, FRAME);
    assert!(matches!(action, EndpointAction::NotFound));
}

#[test]
fn activation_debounced() {
    let mut table = EndpointTable::new();
    sync_create_service(&mut table, "svc1", SVC_IP, default_policy(), &mut passthrough_processor);

    let (_, activate1, _) = table.lookup_and_buffer(SVC_IP, FRAME);
    assert!(activate1);

    let (_, activate2, _) = table.lookup_and_buffer(SVC_IP, FRAME);
    assert!(!activate2);
}

#[test]
fn buffer_capacity_drops_excess() {
    let policy = ServicePolicy {
        buffer_frames: 2,
        timeout_ms: 30000,
        activator: None,
    };
    let mut table = EndpointTable::new();
    sync_create_service(&mut table, "svc1", SVC_IP, policy, &mut passthrough_processor);

    table.lookup_and_buffer(SVC_IP, FRAME);
    table.lookup_and_buffer(SVC_IP, FRAME);
    let (action, _, _) = table.lookup_and_buffer(SVC_IP, FRAME);
    assert!(matches!(action, EndpointAction::Drop { .. }));
}

/// Regression test for Bug 1: setting a backend preserves buffered frames.
#[test]
fn update_backend_preserves_buffered_frames() {
    let mut table = EndpointTable::new();
    sync_create_service(&mut table, "svc1", SVC_IP, default_policy(), &mut passthrough_processor);

    // Buffer 3 frames while there is no backend yet.
    for _ in 0..3 {
        let (action, _, _) = table.lookup_and_buffer(SVC_IP, FRAME);
        assert!(matches!(action, EndpointAction::Buffered { .. }));
    }

    // Set the backend — this should NOT clear the buffer.
    sync_update_backend(&mut table, "svc1", SVC_IP, default_policy(), Some(POD_IP), &mut passthrough_processor);

    // Mark ready — should return the 3 buffered frames.
    let result = table.mark_service_ready("svc1");
    match result.unwrap() {
        MarkReadyResult::Passthrough { frames, .. } => {
            assert_eq!(
                frames.len(),
                3,
                "setting backend should not clear frames buffered before backend was set"
            );
        }
        _ => panic!("expected Passthrough result"),
    }
}

// --- Activator / L4 tests ---

/// Try to load the TCP activator. Returns None if WASM components aren't built.
fn try_load_tcp_activator() -> Option<(distvirt_activator::ActivatorRuntime, distvirt_activator::ActivatorInstance)> {
    let component_dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../activators/target/components");
    let runtime = distvirt_activator::ActivatorRuntime::new(&component_dir).ok()?;
    let component = runtime.get_component("tcp")?;
    let instance = distvirt_activator::ActivatorInstance::new(runtime.engine(), component).ok()?;
    Some((runtime, instance))
}

/// Build a valid TCP SYN frame with fabric header using etherparse.
/// Produces L3 fabric format: [fabric_hdr(3)][IP+TCP].
fn make_tcp_frame_for_service(
    src_ip: [u8; 4],
    dst_ip: [u8; 4],
    src_port: u16,
    dst_port: u16,
) -> Vec<u8> {
    use etherparse::PacketBuilder;

    let builder = PacketBuilder::ipv4(src_ip, dst_ip, 64)
        .tcp(src_port, dst_port, 1000, 65535);

    let mut ip_packet = Vec::new();
    builder.write(&mut ip_packet, &[]).unwrap();

    // Set SYN flag: ip(20) + tcp flags at byte 13
    let tcp_start = 20;
    ip_packet[tcp_start + 13] = 0x02; // SYN

    with_fabric_header(0, 0, &ip_packet)
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

#[test]
#[ignore = "requires WASM activators — run with --include-ignored"]
fn l4_mark_ready_processes_backend_available() {
    let (_runtime, instance) = try_load_tcp_activator()
        .expect("TCP activator WASM not built — run activators/build.sh");

    let sm = distvirt_activator::StreamManager::new(
        distvirt_activator::StreamManagerConfig {
            service_ip: SVC_IP,
            listen_ports: vec![80],
            tcp_buffer_size: 4096,
            listen_pool_size: 2,
        },
    );

    let mut table = EndpointTable::new();
    let mut make_l4 = {
        let mut instance_opt = Some(instance);
        let mut sm_opt = Some(sm);
        move |_: &str, _: &ServicePolicy, _: Ipv4Addr| -> ServiceProcessor {
            ServiceProcessor::L4 {
                activator: Some(instance_opt.take().unwrap()),
                stream_manager: sm_opt.take().unwrap(),
            }
        }
    };
    sync_create_service(&mut table, "svc1", SVC_IP, l4_tcp_policy(), &mut make_l4);

    // Feed a TCP SYN to the L4 path (after vnet header).
    let syn_frame = make_tcp_frame_for_service(
        [10, 0, 0, 1],
        SVC_IP.octets(),
        12345,
        80,
    );
    let (action, _, _) = table.lookup_and_buffer(SVC_IP, &syn_frame);
    assert!(
        matches!(action, EndpointAction::L4Result { .. }),
        "SYN should trigger L4Result"
    );

    // Set backend and mark ready.
    sync_update_backend(&mut table, "svc1", SVC_IP, l4_tcp_policy(), Some(POD_IP), &mut passthrough_processor);
    let ready_result = table.mark_service_ready("svc1");
    assert!(ready_result.is_some(), "mark_service_ready should return Some");

    match ready_result.unwrap() {
        MarkReadyResult::L4(EndpointAction::L4Result { .. }) => {
            // In the L4 path, the stream manager handles TCP buffering
            // (via smoltcp), not the activator's flow map. So
            // BackendAvailable(true) won't produce ReplayPacket actions
            // here — the SM replays traffic through its own TCP state
            // machine. We just verify the L4 result path is taken.
        }
        other => panic!("expected L4 result, got: {:?}", other),
    }
}

#[test]
fn handle_timeout_for_ip_returns_l4_result() {
    let sm = distvirt_activator::StreamManager::new(
        distvirt_activator::StreamManagerConfig {
            service_ip: SVC_IP,
            listen_ports: vec![80],
            tcp_buffer_size: 4096,
            listen_pool_size: 2,
        },
    );

    let mut table = EndpointTable::new();
    let mut make_l4 = {
        let mut sm_opt = Some(sm);
        move |_: &str, _: &ServicePolicy, _: Ipv4Addr| -> ServiceProcessor {
            ServiceProcessor::L4 {
                activator: None,
                stream_manager: sm_opt.take().unwrap(),
            }
        }
    };
    sync_create_service(&mut table, "svc1", SVC_IP, default_policy(), &mut make_l4);

    // handle_timeout_for_ip on a service with a StreamManager should return Some(L4Result).
    let result = table.handle_timeout_for_ip(SVC_IP);
    assert!(result.is_some(), "handle_timeout_for_ip should return Some for L4 service");
    assert!(
        matches!(result.unwrap(), EndpointAction::L4Result { .. }),
        "should return L4Result"
    );
}

#[test]
fn handle_timeout_for_ip_returns_none_for_l3() {
    let mut table = EndpointTable::new();
    sync_create_service(&mut table, "svc1", SVC_IP, default_policy(), &mut passthrough_processor);

    // L3 service (no StreamManager) should return None.
    let result = table.handle_timeout_for_ip(SVC_IP);
    assert!(result.is_none(), "handle_timeout_for_ip should return None for L3 service");
}

#[test]
#[ignore = "requires WASM activators — run with --include-ignored"]
fn activator_mark_ready_returns_replay_actions() {
    let (_runtime, instance) = try_load_tcp_activator()
        .expect("TCP activator WASM not built — run activators/build.sh");

    let mut table = EndpointTable::new();
    let mut make_l3 = {
        let mut instance_opt = Some(instance);
        move |_: &str, _: &ServicePolicy, _: Ipv4Addr| -> ServiceProcessor {
            ServiceProcessor::L3 {
                activator: instance_opt.take().unwrap(),
                flow_tracker: distvirt_activator::FlowTracker::new(),
            }
        }
    };
    sync_create_service(&mut table, "svc1", SVC_IP, l4_tcp_policy(), &mut make_l3);

    // Feed a TCP SYN frame via lookup_and_buffer.
    let syn_frame = make_tcp_frame_for_service(
        [10, 0, 0, 1],
        SVC_IP.octets(),
        12345,
        80,
    );
    let (action, _, _) = table.lookup_and_buffer(SVC_IP, &syn_frame);
    assert!(
        matches!(action, EndpointAction::ActivatorActions { .. }),
        "SYN should trigger activator actions"
    );

    // Set backend and mark ready.
    sync_update_backend(&mut table, "svc1", SVC_IP, l4_tcp_policy(), Some(POD_IP), &mut passthrough_processor);
    let ready_result = table.mark_service_ready("svc1");
    assert!(ready_result.is_some(), "mark_service_ready should return Some");

    match ready_result.unwrap() {
        MarkReadyResult::Passthrough { service_ip, actions, .. } => {
            assert_eq!(service_ip, SVC_IP);
            let replay_count = actions
                .iter()
                .filter(|a| matches!(a, distvirt_activator::types::Action::ReplayPacket(_)))
                .count();
            assert!(replay_count > 0, "mark_service_ready should return ReplayPacket actions for buffered SYN");
        }
        _ => panic!("expected Passthrough result"),
    }
}

// --- LocalAdapter (WireGuard peer) tests ---

const WG_PEER_IP: Ipv4Addr = Ipv4Addr::new(172, 16, 0, 254);
const ADAPTER_PORT_ID: PortId = 42;

#[test]
fn wg_peer_local_creates_local_adapter_endpoint() {
    use distvirt_worker_protocol::EndpointPlacement;

    let mut table = EndpointTable::new();
    let effects = table.apply_endpoint_sync(
        vec![EndpointSpec {
            ip: WG_PEER_IP,
            kind: EndpointKind::WireGuardPeer {
                placement: Some(EndpointPlacement {
                    worker_id: distvirt_worker_protocol::WorkerId::from(OWN_WORKER),
                }),
            },
        }],
        OWN_WORKER,
        &mut passthrough_processor,
        Some(ADAPTER_PORT_ID),
    );
    assert!(effects.is_empty());

    // Should return LocalAdapter action.
    let (action, _, _) = table.lookup_and_buffer(WG_PEER_IP, FRAME);
    assert!(
        matches!(action, EndpointAction::LocalAdapter { port_id } if port_id == ADAPTER_PORT_ID),
        "expected LocalAdapter action, got {:?}", action
    );
}

#[test]
fn wg_peer_remote_creates_remote_segment() {
    use distvirt_worker_protocol::{EndpointPlacement, WorkerId};

    let mut table = EndpointTable::new();
    table.apply_endpoint_sync(
        vec![EndpointSpec {
            ip: WG_PEER_IP,
            kind: EndpointKind::WireGuardPeer {
                placement: Some(EndpointPlacement {
                    worker_id: WorkerId::from("other-worker"),
                }),
            },
        }],
        OWN_WORKER,
        &mut passthrough_processor,
        Some(ADAPTER_PORT_ID),
    );

    let (action, _, _) = table.lookup_and_buffer(WG_PEER_IP, FRAME);
    assert!(
        matches!(action, EndpointAction::RemoteWorker { ref worker_id } if worker_id == "other-worker"),
        "expected RemoteWorker action, got {:?}", action
    );
}

#[test]
fn wg_peer_unplaced_buffers_and_activates() {
    let mut table = EndpointTable::new();
    table.apply_endpoint_sync(
        vec![EndpointSpec {
            ip: WG_PEER_IP,
            kind: EndpointKind::WireGuardPeer { placement: None },
        }],
        OWN_WORKER,
        &mut passthrough_processor,
        Some(ADAPTER_PORT_ID),
    );

    let (action, activate, _) = table.lookup_and_buffer(WG_PEER_IP, FRAME);
    assert!(matches!(action, EndpointAction::Buffered { service_id: None }));
    assert!(activate, "should emit activation for unplaced peer");
}

#[test]
fn wg_peer_transition_flushes_buffer() {
    use distvirt_worker_protocol::{EndpointPlacement, WorkerId};

    let mut table = EndpointTable::new();
    // Start unplaced.
    table.apply_endpoint_sync(
        vec![EndpointSpec {
            ip: WG_PEER_IP,
            kind: EndpointKind::WireGuardPeer { placement: None },
        }],
        OWN_WORKER,
        &mut passthrough_processor,
        Some(ADAPTER_PORT_ID),
    );

    // Buffer some frames.
    for _ in 0..3 {
        table.lookup_and_buffer(WG_PEER_IP, FRAME);
    }

    // Now place locally — should flush buffered frames.
    let effects = table.apply_endpoint_update(
        vec![EndpointSpec {
            ip: WG_PEER_IP,
            kind: EndpointKind::WireGuardPeer {
                placement: Some(EndpointPlacement {
                    worker_id: WorkerId::from(OWN_WORKER),
                }),
            },
        }],
        vec![],
        OWN_WORKER,
        &mut passthrough_processor,
        Some(ADAPTER_PORT_ID),
    );

    // Should have a FlushAdapterBuffer effect with 3 frames.
    let flush = effects.iter().find(|e| matches!(e, EndpointSyncEffect::FlushAdapterBuffer { .. }));
    assert!(flush.is_some(), "should emit FlushAdapterBuffer effect");
    match flush.unwrap() {
        EndpointSyncEffect::FlushAdapterBuffer { ip, port_id, frames } => {
            assert_eq!(*ip, WG_PEER_IP);
            assert_eq!(*port_id, ADAPTER_PORT_ID);
            assert_eq!(frames.len(), 3);
        }
        _ => unreachable!(),
    }

    // Endpoint should now be LocalAdapter.
    let (action, _, _) = table.lookup_and_buffer(WG_PEER_IP, FRAME);
    assert!(matches!(action, EndpointAction::LocalAdapter { .. }));
}

// --- LocalPod tests ---

const LOCAL_POD_IP: Ipv4Addr = Ipv4Addr::new(172, 16, 0, 200);

#[test]
fn local_pod_pending_buffers_frames() {
    use distvirt_worker_protocol::EndpointPlacement;

    let mut table = EndpointTable::new();
    table.apply_endpoint_sync(
        vec![EndpointSpec {
            ip: LOCAL_POD_IP,
            kind: EndpointKind::Pod {
                placement: Some(EndpointPlacement {
                    worker_id: distvirt_worker_protocol::WorkerId::from(OWN_WORKER),
                }),
            },
        }],
        OWN_WORKER,
        &mut passthrough_processor,
        None,
    );

    // No port attached yet — should buffer.
    let (action, activate, _) = table.lookup_and_buffer(LOCAL_POD_IP, FRAME);
    assert!(matches!(action, EndpointAction::Buffered { service_id: None }));
    assert!(activate);
}

#[test]
fn local_pod_attach_flushes_buffer() {
    use distvirt_worker_protocol::EndpointPlacement;

    let mut table = EndpointTable::new();
    table.apply_endpoint_sync(
        vec![EndpointSpec {
            ip: LOCAL_POD_IP,
            kind: EndpointKind::Pod {
                placement: Some(EndpointPlacement {
                    worker_id: distvirt_worker_protocol::WorkerId::from(OWN_WORKER),
                }),
            },
        }],
        OWN_WORKER,
        &mut passthrough_processor,
        None,
    );

    // Buffer 3 frames.
    for _ in 0..3 {
        table.lookup_and_buffer(LOCAL_POD_IP, FRAME);
    }

    // Attach port — should return buffered frames.
    let frames = table.attach_port(LOCAL_POD_IP, 42);
    assert_eq!(frames.len(), 3);

    // Now should forward to the port.
    let (action, _, _) = table.lookup_and_buffer(LOCAL_POD_IP, FRAME);
    assert!(matches!(action, EndpointAction::LocalPod { port_id: 42 }));
}

#[test]
fn local_pod_detach_returns_to_pending() {
    use distvirt_worker_protocol::EndpointPlacement;

    let mut table = EndpointTable::new();
    table.apply_endpoint_sync(
        vec![EndpointSpec {
            ip: LOCAL_POD_IP,
            kind: EndpointKind::Pod {
                placement: Some(EndpointPlacement {
                    worker_id: distvirt_worker_protocol::WorkerId::from(OWN_WORKER),
                }),
            },
        }],
        OWN_WORKER,
        &mut passthrough_processor,
        None,
    );

    table.attach_port(LOCAL_POD_IP, 42);

    // Detach port.
    table.detach_port(42);

    // Should buffer again (no port).
    let (action, _, _) = table.lookup_and_buffer(LOCAL_POD_IP, FRAME);
    assert!(matches!(action, EndpointAction::Buffered { service_id: None }));
}

#[test]
fn local_pod_get_port_id() {
    use distvirt_worker_protocol::EndpointPlacement;

    let mut table = EndpointTable::new();
    table.apply_endpoint_sync(
        vec![EndpointSpec {
            ip: LOCAL_POD_IP,
            kind: EndpointKind::Pod {
                placement: Some(EndpointPlacement {
                    worker_id: distvirt_worker_protocol::WorkerId::from(OWN_WORKER),
                }),
            },
        }],
        OWN_WORKER,
        &mut passthrough_processor,
        None,
    );

    // No port attached — None.
    assert!(table.get_port_id(&LOCAL_POD_IP).is_none());

    // Attach port.
    table.attach_port(LOCAL_POD_IP, 42);
    assert_eq!(table.get_port_id(&LOCAL_POD_IP), Some(42));

    // Detach port.
    table.detach_port(42);
    assert!(table.get_port_id(&LOCAL_POD_IP).is_none());
}
