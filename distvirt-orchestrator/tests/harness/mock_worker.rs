use std::sync::{Arc, Mutex};

use tokio::sync::mpsc;
use distvirt_worker_protocol::{
    PoolId, PoolInfo, WorkerCapabilities, WorkerCommand, WorkerConnection, WorkerEvent,
    WorkerHello, WorkerReady,
};

/// Handler that can override the default command→event mapping.
/// Return `None` to fall through to default, `Some(vec![])` to suppress, `Some(events)` to override.
pub type CommandHandler =
    Box<dyn Fn(&WorkerCommand) -> Option<Vec<WorkerEvent>> + Send + Sync + 'static>;

pub struct MockWorkerConfig {
    pub handler: Option<CommandHandler>,
    pub capabilities: WorkerCapabilities,
    pub auth_token: String,
}

impl Default for MockWorkerConfig {
    fn default() -> Self {
        MockWorkerConfig {
            handler: None,
            capabilities: WorkerCapabilities {
                has_kvm: true,
                has_containerd: true,
                available_adapters: vec![],
                max_pods: 10,
                available_memory_mb: 1024,
                public_endpoint: String::new(),
                pools: vec![],
            },
            auth_token: super::test_harness::TestHarness::TEST_SECRET.to_string(),
        }
    }
}

impl MockWorkerConfig {
    /// Set a custom command handler.
    pub fn with_handler(mut self, handler: CommandHandler) -> Self {
        self.handler = Some(handler);
        self
    }

    /// Handler that returns PodFailed on LaunchPod.
    pub fn with_launch_failure() -> Self {
        MockWorkerConfig {
            handler: Some(Box::new(|cmd| match cmd {
                WorkerCommand::LaunchPod {
                    namespace_id,
                    pod_id,
                    ..
                } => Some(vec![WorkerEvent::PodFailed {
                    namespace_id: namespace_id.clone(),
                    pod_id: pod_id.clone(),
                    error: "mock launch failure".to_string(),
                }]),
                _ => None,
            })),
            ..Default::default()
        }
    }

    /// Handler that returns PodSuspendFailed on SuspendPod.
    pub fn with_suspend_failure() -> Self {
        MockWorkerConfig {
            handler: Some(Box::new(|cmd| match cmd {
                WorkerCommand::SuspendPod {
                    namespace_id,
                    pod_id,
                    ..
                } => Some(vec![WorkerEvent::PodSuspendFailed {
                    namespace_id: namespace_id.clone(),
                    pod_id: pod_id.clone(),
                    error: "mock suspend failure".to_string(),
                }]),
                _ => None,
            })),
            ..Default::default()
        }
    }

    /// Handler that returns empty vec on LaunchPod (no response, simulates hang/timeout).
    pub fn with_launch_hang() -> Self {
        MockWorkerConfig {
            handler: Some(Box::new(|cmd| match cmd {
                WorkerCommand::LaunchPod { .. } => Some(vec![]),
                _ => None,
            })),
            ..Default::default()
        }
    }

    /// Handler that returns empty vec on SuspendPod (no response, simulates hang/timeout).
    pub fn with_suspend_hang() -> Self {
        MockWorkerConfig {
            handler: Some(Box::new(|cmd| match cmd {
                WorkerCommand::SuspendPod { .. } => Some(vec![]),
                _ => None,
            })),
            ..Default::default()
        }
    }

    /// Config with a local storage pool (needed for suspend/resume).
    pub fn with_pool() -> Self {
        MockWorkerConfig {
            capabilities: WorkerCapabilities {
                has_kvm: true,
                has_containerd: true,
                available_adapters: vec![],
                max_pods: 10,
                available_memory_mb: 1024,
                public_endpoint: String::new(),
                pools: vec![PoolInfo {
                    pool_id: PoolId::from("local"),
                    path: "/tmp/pool".to_string(),
                    capacity_bytes: 1024 * 1024 * 1024,
                    available_bytes: 1024 * 1024 * 1024,
                }],
            },
            ..Default::default()
        }
    }

    /// Add a local storage pool to an existing config (chainable).
    pub fn add_pool(mut self) -> Self {
        self.capabilities.pools.push(PoolInfo {
            pool_id: PoolId::from("local"),
            path: "/tmp/pool".to_string(),
            capacity_bytes: 1024 * 1024 * 1024,
            available_bytes: 1024 * 1024 * 1024,
        });
        self
    }

    /// Handler that returns PodFailed on ResumePod (simulates corrupt snapshot / VM restore error).
    pub fn with_resume_failure() -> Self {
        MockWorkerConfig {
            handler: Some(Box::new(|cmd| match cmd {
                WorkerCommand::ResumePod {
                    namespace_id,
                    pod_id,
                    ..
                } => Some(vec![WorkerEvent::PodFailed {
                    namespace_id: namespace_id.clone(),
                    pod_id: pod_id.clone(),
                    error: "mock resume failure".to_string(),
                }]),
                _ => None,
            })),
            ..MockWorkerConfig::with_pool()
        }
    }

    /// Config with a local storage pool and limited memory.
    /// Useful for pressure tests: with DEFAULT_POD_MEMORY_MB=128,
    /// one pod on a 256MB worker → 0.5 pressure → Elevated.
    pub fn with_pool_and_memory(available_memory_mb: u64) -> Self {
        MockWorkerConfig {
            capabilities: WorkerCapabilities {
                has_kvm: true,
                has_containerd: true,
                available_adapters: vec![],
                max_pods: 10,
                available_memory_mb,
                public_endpoint: String::new(),
                pools: vec![PoolInfo {
                    pool_id: PoolId::from("local"),
                    path: "/tmp/pool".to_string(),
                    capacity_bytes: 1024 * 1024 * 1024,
                    available_bytes: 1024 * 1024 * 1024,
                }],
            },
            ..Default::default()
        }
    }

    /// Config with tunnel capabilities (public_endpoint, pool, etc.).
    pub fn with_tunnel(endpoint: &str, _public_key: [u8; 32]) -> Self {
        MockWorkerConfig {
            capabilities: WorkerCapabilities {
                has_kvm: true,
                has_containerd: true,
                available_adapters: vec![],
                max_pods: 10,
                available_memory_mb: 1024,
                public_endpoint: endpoint.to_string(),
                pools: vec![PoolInfo {
                    pool_id: PoolId::from("local"),
                    path: "/tmp/pool".to_string(),
                    capacity_bytes: 1024 * 1024 * 1024,
                    available_bytes: 1024 * 1024 * 1024,
                }],
            },
            ..Default::default()
        }
    }
}

/// Handle returned to the test for interacting with a running mock worker.
pub struct MockWorkerHandle {
    inject_tx: mpsc::UnboundedSender<WorkerEvent>,
    commands: Arc<Mutex<Vec<WorkerCommand>>>,
    task: tokio::task::JoinHandle<()>,
}

impl MockWorkerHandle {
    /// Inject an event from the worker side (e.g. EndpointActivation).
    pub fn send_event(&self, event: WorkerEvent) {
        let _ = self.inject_tx.send(event);
    }

    /// Disconnect the mock worker (drop transport, abort task).
    pub fn disconnect(self) {
        self.task.abort();
    }

    /// Snapshot of all commands received by this worker.
    pub fn commands(&self) -> Vec<WorkerCommand> {
        self.commands.lock().unwrap().clone()
    }
}

/// Default happy-path handler: maps commands to expected events.
fn default_handle(cmd: &WorkerCommand) -> Vec<WorkerEvent> {
    match cmd {
        WorkerCommand::CreateNamespace { namespace_id, .. } => {
            vec![WorkerEvent::NamespaceCreated {
                namespace_id: namespace_id.clone(),
            }]
        }
        WorkerCommand::LaunchPod {
            namespace_id,
            pod_id,
            ..
        } => vec![WorkerEvent::PodRunning {
            namespace_id: namespace_id.clone(),
            pod_id: pod_id.clone(),
        }],
        WorkerCommand::StopPod {
            namespace_id,
            pod_id,
            ..
        } => vec![WorkerEvent::PodExited {
            namespace_id: namespace_id.clone(),
            pod_id: pod_id.clone(),
            exit_code: 0,
        }],
        WorkerCommand::DestroyNamespace { namespace_id } => {
            vec![WorkerEvent::NamespaceDestroyed {
                namespace_id: namespace_id.clone(),
            }]
        }
        WorkerCommand::SuspendPod {
            namespace_id,
            pod_id,
            artifact_id,
            pool_id,
            ..
        } => vec![
            WorkerEvent::ArtifactWriteStarted {
                namespace_id: namespace_id.clone(),
                artifact_id: artifact_id.clone(),
                pool_id: pool_id.clone(),
            },
            WorkerEvent::ArtifactWriteCommitted {
                namespace_id: namespace_id.clone(),
                artifact_id: artifact_id.clone(),
                pool_id: pool_id.clone(),
                size_bytes: 1024,
            },
            WorkerEvent::PodSuspended {
                namespace_id: namespace_id.clone(),
                pod_id: pod_id.clone(),
                artifact_id: artifact_id.clone(),
                artifact_size_bytes: 1024,
                pool_id: pool_id.clone(),
            },
        ],
        WorkerCommand::ResumePod {
            namespace_id,
            pod_id,
            ..
        } => vec![WorkerEvent::PodRunning {
            namespace_id: namespace_id.clone(),
            pod_id: pod_id.clone(),
        }],
        WorkerCommand::Shutdown => vec![WorkerEvent::ShuttingDown],
        // Everything else: no response needed
        _ => vec![],
    }
}

/// Spawn a mock worker background task. Returns the orchestrator-side transport half
/// and the MockWorkerHandle for the test.
pub fn spawn_mock_worker(
    config: MockWorkerConfig,
) -> (
    tokio::io::DuplexStream,
    MockWorkerHandle,
) {
    let (orch_half, worker_half) = tokio::io::duplex(64 * 1024);
    let commands: Arc<Mutex<Vec<WorkerCommand>>> = Arc::new(Mutex::new(Vec::new()));
    let commands_clone = commands.clone();
    let (inject_tx, mut inject_rx) = mpsc::unbounded_channel::<WorkerEvent>();
    let handler = config.handler;
    let capabilities = config.capabilities;
    let auth_token = config.auth_token;

    // Determine tunnel info from capabilities: if we have a public_endpoint,
    // send tunnel info in WorkerReady (simulates a tunnel-capable worker).
    let has_tunnel = !capabilities.public_endpoint.is_empty();
    // Use a deterministic key based on whether tunnel is enabled.
    let tunnel_port: Option<u16> = if has_tunnel { Some(9000) } else { None };
    let tunnel_key: Option<[u8; 32]> = if has_tunnel {
        // Derive a deterministic key from the endpoint string for test predictability.
        let mut key = [0u8; 32];
        let bytes = capabilities.public_endpoint.as_bytes();
        for (i, b) in bytes.iter().enumerate() {
            key[i % 32] ^= *b;
        }
        Some(key)
    } else {
        None
    };

    let task = tokio::spawn(async move {
        // Accept and handshake
        let mut conn = match WorkerConnection::accept(worker_half).await {
            Ok(c) => c,
            Err(_) => return,
        };

        if conn
            .send_hello(&WorkerHello {
                auth_token,
                capabilities,
            })
            .await
            .is_err()
        {
            return;
        }

        if conn.recv_accepted().await.is_err() {
            return;
        }

        if conn
            .send_ready(&WorkerReady {
                tunnel_listen_port: tunnel_port,
                tunnel_public_key: tunnel_key,
                transfer_listen_port: None,
            })
            .await
            .is_err()
        {
            return;
        }

        // Command loop
        loop {
            tokio::select! {
                cmd_result = conn.recv_command() => {
                    match cmd_result {
                        Ok(cmd) => {
                            let is_shutdown = matches!(cmd, WorkerCommand::Shutdown);
                            commands_clone.lock().unwrap().push(cmd.clone());

                            // Determine events to send
                            let events = if let Some(ref h) = handler {
                                match h(&cmd) {
                                    Some(evts) => evts,
                                    None => default_handle(&cmd),
                                }
                            } else {
                                default_handle(&cmd)
                            };

                            for event in events {
                                if conn.send_event(&event).await.is_err() {
                                    return;
                                }
                            }

                            if is_shutdown {
                                return;
                            }
                        }
                        Err(_) => return,
                    }
                }
                injected = inject_rx.recv() => {
                    match injected {
                        Some(event) => {
                            if conn.send_event(&event).await.is_err() {
                                return;
                            }
                        }
                        None => return,
                    }
                }
            }
        }
    });

    let handle = MockWorkerHandle {
        inject_tx,
        commands,
        task,
    };

    (orch_half, handle)
}
