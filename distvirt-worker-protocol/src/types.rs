use std::net::Ipv4Addr;

use serde::{Deserialize, Serialize};

/// Network configuration for a namespace's fabric segment.
///
/// Defines the IP subnet for all pods and services within the namespace.
/// The gateway address is used by the fabric's smoltcp gateway, which provides
/// DNS resolution and TUN-based egress for pods.
///
/// The gateway MAC is a fixed locally-administered address (`02:00:00:00:00:01`).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NetworkConfig {
    /// Subnet address (e.g., `172.16.0.0`).
    pub subnet: Ipv4Addr,
    /// Gateway IP within the subnet (e.g., `172.16.0.1`).
    pub gateway: Ipv4Addr,
    /// Subnet prefix length (e.g., `24` for a `/24`).
    pub prefix_len: u8,
}

/// Network configuration assigned to a specific pod.
///
/// The orchestrator derives this from the namespace's [`NetworkConfig`] and the
/// pod's assigned IP/MAC. The worker uses it to configure the guest VM's network
/// interface.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PodNetworkConfig {
    /// The pod's IP address on the namespace fabric.
    pub ip: Ipv4Addr,
    /// The pod's MAC address (used for the TAP device on the fabric).
    pub mac: [u8; 6],
    /// Gateway IP for the pod's network config (typically the fabric gateway).
    pub gateway: Ipv4Addr,
    /// Subnet mask string (e.g., `"255.255.255.0"`).
    pub netmask: String,
}

/// Container execution configuration.
///
/// When a [`ContainerSpec`] references an OCI image, the worker parses the image's
/// config (entrypoint, cmd, env, working_dir, user) and merges it with these
/// overrides. Explicit values here take precedence; `None`/empty fields fall
/// through to image defaults.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ContainerConfig {
    /// Main executable path.
    pub entrypoint: String,
    /// Arguments to the entrypoint.
    pub args: Vec<String>,
    /// Environment variables in `KEY=VALUE` format (OCI convention).
    pub env: Vec<String>,
    /// Working directory inside the container. Falls back to image default.
    pub working_dir: Option<String>,
    /// User ID to run as. Falls back to image default.
    pub uid: Option<u32>,
    /// Group ID to run as. Falls back to image default.
    pub gid: Option<u32>,
    /// Hostname for the container.
    pub hostname: Option<String>,
    /// Whether to capture stdout/stderr and stream it to the orchestrator
    /// via a log stream. See [`LogStreamHeader`].
    pub capture_output: bool,
}

/// Specification for a container within a pod.
///
/// Pods can contain multiple containers sharing the same VM and network namespace.
/// Currently only single-container pods are used, but the protocol supports
/// multiple containers from day one for future sidecar/init container support.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ContainerSpec {
    /// Unique identifier for this container within its pod.
    pub container_id: String,
    /// OCI image reference (e.g., `"docker.io/library/nginx:latest"`).
    pub image_ref: String,
    /// Execution configuration (merged with OCI image defaults).
    pub config: ContainerConfig,
}

/// A DNS registry entry mapping a name to an IP address.
///
/// The orchestrator owns the authoritative name-to-IP mapping per namespace.
/// Workers hold a projected copy, kept in sync via
/// [`WorkerCommand::RegistrySync`] (full replacement) and future incremental
/// updates.
///
/// DNS entries typically map service names to **service IPs** (not pod IPs).
/// The fabric gateway's DNS server queries this registry to answer DNS queries
/// from pods. Names not found are forwarded to upstream DNS.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RegistryEntry {
    /// DNS name (e.g., `"api"`, `"database"`).
    pub name: String,
    /// IP address the name resolves to (typically a service IP).
    pub ip: Ipv4Addr,
}

/// Buffer policy for placeholder routes and basic service buffering.
///
/// Controls what happens to frames destined for a pod that isn't currently
/// running (suspended, scaled-to-zero). This provides basic best-effort
/// buffering only — rich activation features (readiness gating, protocol
/// activators) live on services via [`ServicePolicy`].
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BufferPolicy {
    /// Maximum number of frames to buffer. `0` means drop immediately
    /// (the route miss is still reported so the orchestrator can react).
    pub buffer_frames: u32,
    /// How long to buffer before giving up, in milliseconds.
    pub timeout_ms: u32,
}

/// Protocol activator configuration for a service entity.
///
/// Protocol activators replace the default "buffer everything, activate on any
/// frame" behavior with protocol-specific logic. They are implemented as WASM
/// components running on the fabric. See the protocol-activators design doc for
/// full details.
///
/// When [`ServicePolicy::activator`] is `None`, the service uses the default
/// passthrough behavior (buffer all frames, activate on first frame).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ActivatorConfig {
    /// TCP SYN-based activation.
    ///
    /// An L3 (packet-level) activator that detects new TCP connections by
    /// inspecting SYN flags. Only SYN packets trigger activation — RSTs and
    /// stale keepalives are filtered. Buffered SYN packets are replayed to
    /// the backend once it becomes available, letting the client's TCP stack
    /// complete the handshake normally.
    ///
    /// Signals [`BackendNeed::Traffic`] on new SYN arrivals. The fabric
    /// applies a timeout policy to determine when to release the backend.
    Tcp {
        /// TCP destination ports to apply activation to. `None` means all ports.
        ports: Option<Vec<u16>>,
        /// If `true`, non-TCP frames are silently dropped. If `false`, they are
        /// buffered alongside TCP traffic.
        tcp_only: bool,
        /// Maximum number of tracked flows (source IP + port combinations).
        /// Default: 1024.
        max_flows: u32,
    },
    /// HTTP/2 stream-aware activation (future).
    ///
    /// An L4 (stream-level) activator that acts as a full H2 proxy. It
    /// maintains H2 connections with clients (SETTINGS, PING/ACK), detects
    /// new requests via HEADERS frames, and proxies traffic to the backend.
    ///
    /// Signals [`BackendNeed::Active`] when H2 streams are open and
    /// [`BackendNeed::None`] when the last stream closes — providing precise
    /// scale-to-zero without timeout guessing.
    Http2 {
        // H2-specific configuration TBD.
    },
}

/// Backend need level signaled by a protocol activator.
///
/// Protocol activators observe traffic patterns and signal whether a backend
/// pod is needed. The orchestrator uses this to decide when to schedule or
/// release backend pods.
///
/// The key distinction is between **pulse** (`Traffic`) and **level** (`Active`)
/// signals:
/// - Non-session-aware activators (TCP) use `Traffic` — "something meaningful
///   just happened." The fabric applies a timeout to determine when it's over.
/// - Session-aware activators (H2) use `Active` — "work is in progress." They
///   know exactly when the last session ends.
///
/// Reported via [`WorkerEvent::ServiceBackendNeed`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum BackendNeed {
    /// No meaningful traffic. The backend may be released / scaled to zero.
    None,
    /// Pulse: meaningful traffic detected (e.g., TCP SYN). The orchestrator
    /// should ensure a backend is running. The fabric applies its own timeout —
    /// if no further `Traffic` (or transition to `Active`) within the timeout
    /// window, the orchestrator may release the backend.
    Traffic,
    /// Level: active sessions require a backend (e.g., open H2 streams).
    /// The backend must stay up as long as this is asserted. Cleared when the
    /// last active session ends.
    Active,
}

/// Policy for a fabric-level service entity.
///
/// Controls buffering behavior and optional protocol-aware activation for a
/// service. Passed via [`WorkerCommand::CreateService`].
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServicePolicy {
    /// Maximum number of frames to buffer while waiting for readiness.
    pub buffer_frames: u32,
    /// How long to buffer before giving up, in milliseconds.
    pub timeout_ms: u32,
    /// Optional protocol activator. When `None`, the service uses default
    /// passthrough behavior (buffer all frames, activate on first frame).
    /// When `Some`, the specified WASM activator handles traffic with
    /// protocol-specific logic.
    pub activator: Option<ActivatorConfig>,
}

/// Backend target for a service entity.
///
/// Identifies the pod that should receive traffic for a service. The backing
/// pod can be local (has a TAP on this worker's fabric) or remote (reached
/// via the fabric routing table).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServiceBackend {
    /// The backing pod's IP address on the namespace fabric.
    pub pod_ip: Ipv4Addr,
    /// The backing pod's MAC address (used to locate the port on the fabric).
    pub pod_mac: [u8; 6],
}

/// Where a fabric route entry points.
///
/// Each entry in the fabric routing table maps a pod IP/MAC to a destination.
/// This handles **direct pod-to-pod** forwarding (not service traffic, which
/// goes through service entities).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum RouteDestination {
    /// The pod is live on another worker. The fabric forwards frames through
    /// a tunnel to that worker.
    RemoteWorker { worker_id: String },
    /// The pod is not currently running (suspended, scaled-to-zero). The fabric
    /// buffers frames per the policy and reports a
    /// [`WorkerEvent::FabricRouteMiss`] so the orchestrator can react.
    Placeholder { buffer_policy: BufferPolicy },
}

/// A single entry in the fabric routing table.
///
/// Routes for pods that are **local** to this worker (have a TAP on the local
/// fabric) don't need entries — the fabric already knows about them. Route
/// entries are only needed for remote pods and placeholders.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FabricRouteEntry {
    /// The pod's IP address.
    pub ip: Ipv4Addr,
    /// The pod's MAC address.
    pub mac: [u8; 6],
    /// Where to send traffic for this pod.
    pub destination: RouteDestination,
}

/// Output stream identifier for log framing.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum OutputStream {
    Stdout,
    Stderr,
}

/// Commands sent from the orchestrator to the worker.
///
/// The orchestrator drives all state by sending commands over the control
/// stream. The worker executes them and reports results as [`WorkerEvent`]s.
///
/// # Resolution Order for Traffic
///
/// When the fabric receives a frame for a destination IP, it resolves in order:
/// 1. **Local TAP port** — pod is on this worker, forward directly.
/// 2. **Service entity** — destination is a service IP. Handles buffering,
///    activation, and forwarding to the backing pod.
/// 3. **Route table** — destination is a pod IP with a route entry (remote
///    worker or placeholder).
/// 4. **Flood** — unknown destination, standard L2 behavior.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum WorkerCommand {
    /// Create a namespace's local fabric segment.
    ///
    /// The worker creates an L2 switch, a smoltcp gateway at the specified IP,
    /// and TUN egress. The worker acknowledges with [`WorkerEvent::NamespaceCreated`]
    /// when the fabric segment is ready to host pods.
    CreateNamespace {
        namespace_id: String,
        network: NetworkConfig,
    },

    /// Tear down a namespace on this worker.
    ///
    /// All pods in the namespace are cancelled (with a graceful shutdown window),
    /// all services and routes are removed, and the fabric segment is destroyed.
    DestroyNamespace {
        namespace_id: String,
    },

    /// Full-state replacement of the DNS registry for a namespace.
    ///
    /// The worker discards its local registry and adopts the provided entries.
    /// Sent when the worker first joins a namespace, or when the orchestrator
    /// wants to force reconciliation.
    RegistrySync {
        namespace_id: String,
        entries: Vec<RegistryEntry>,
    },

    /// Launch a pod (Firecracker VM) in a namespace.
    ///
    /// The worker will:
    /// 1. Prepare container images (pull if needed, parse OCI config)
    /// 2. Merge OCI image config with provided overrides
    /// 3. Launch a Firecracker VM with the specified network config
    /// 4. Attach the VM's TAP to the namespace's fabric
    /// 5. Configure guest networking (IP, gateway, DNS pointing at fabric gateway)
    /// 6. Start containers
    /// 7. Report [`WorkerEvent::PodRunning`] when all containers are started
    ///
    /// On failure, reports [`WorkerEvent::PodFailed`] and cleans up partial state.
    LaunchPod {
        namespace_id: String,
        pod_id: String,
        network: PodNetworkConfig,
        containers: Vec<ContainerSpec>,
    },

    /// Stop a running pod.
    ///
    /// If `graceful` is `true`, the worker cancels the pod's token, triggering
    /// a graceful VM shutdown with a timeout before force-killing. If `false`,
    /// the pod supervisor is aborted immediately (VM process killed via Drop).
    StopPod {
        namespace_id: String,
        pod_id: String,
        /// `true` = graceful shutdown with timeout, `false` = immediate kill.
        graceful: bool,
    },

    /// Full-state replacement of the fabric routing table for a namespace.
    ///
    /// Sent when the worker joins a namespace. The worker discards its local
    /// routing table and adopts the provided routes.
    ///
    /// In local mode (single worker), the routing table is typically empty
    /// since all pods are local.
    FabricRouteSync {
        namespace_id: String,
        routes: Vec<FabricRouteEntry>,
    },

    /// Incremental update to the fabric routing table.
    ///
    /// When a pod launches on another worker, the orchestrator sends a route
    /// update so this worker knows how to forward frames. When a pod is
    /// suspended, the orchestrator updates the entry from `RemoteWorker`
    /// to `Placeholder`.
    FabricRouteUpdate {
        namespace_id: String,
        added: Vec<FabricRouteEntry>,
        removed_ips: Vec<Ipv4Addr>,
    },

    /// Create a service entity on the namespace's fabric.
    ///
    /// The service gets a virtual IP and MAC on the fabric. It starts with no
    /// backend — traffic is buffered per policy and
    /// [`WorkerEvent::ServiceActivation`] events fire so the orchestrator can
    /// schedule a backing pod.
    ///
    /// Services are projected to **all** workers participating in a namespace.
    CreateService {
        namespace_id: String,
        service_id: String,
        /// The service's virtual IP on the fabric.
        ip: Ipv4Addr,
        /// The service's virtual MAC on the fabric.
        mac: [u8; 6],
        /// Buffering and activation policy.
        policy: ServicePolicy,
    },

    /// Assign or remove the backing pod for a service.
    ///
    /// When `backend` is `Some`, traffic will be forwarded to the specified pod
    /// once [`WorkerCommand::ServiceReady`] is received. Until then, traffic is
    /// still buffered (the pod may not be listening yet).
    ///
    /// Setting `backend` to `None` returns the service to the no-backend state
    /// (scale-to-zero). Any subsequent traffic triggers activation again.
    UpdateServiceBackend {
        namespace_id: String,
        service_id: String,
        backend: Option<ServiceBackend>,
    },

    /// Mark a service as ready to receive traffic.
    ///
    /// Buffered frames are flushed to the backing pod. The orchestrator decides
    /// when readiness is achieved (container started, health check passed,
    /// etc.) — this is orchestrator policy, not a worker concern.
    ServiceReady {
        namespace_id: String,
        service_id: String,
    },

    /// Remove a service entity from the fabric.
    ///
    /// Any buffered frames are dropped. The service IP is no longer reachable.
    DestroyService {
        namespace_id: String,
        service_id: String,
    },

    /// Shut down the worker entirely.
    ///
    /// The worker acknowledges with [`WorkerEvent::ShuttingDown`], cancels all
    /// namespaces and pods, awaits cleanup, then exits.
    Shutdown,
}

/// Events emitted by the worker back to the orchestrator.
///
/// Events report lifecycle transitions and fabric-level signals. The worker
/// never makes scheduling decisions — it only reports what happened so the
/// orchestrator can react.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum WorkerEvent {
    /// The namespace's fabric segment is up and ready for pods.
    ///
    /// The L2 switch, smoltcp gateway, and DNS registry are initialized.
    /// Sent in response to [`WorkerCommand::CreateNamespace`].
    NamespaceCreated {
        namespace_id: String,
    },

    /// The pod's VM is booted and all containers are started.
    ///
    /// The pod is on the fabric and reachable at its assigned IP/MAC.
    /// Sent in response to [`WorkerCommand::LaunchPod`].
    PodRunning {
        namespace_id: String,
        pod_id: String,
    },

    /// The pod's main container exited.
    ///
    /// The exit code is from the main container (first in the containers list).
    PodExited {
        namespace_id: String,
        pod_id: String,
        exit_code: i32,
    },

    /// The pod could not start.
    ///
    /// Possible causes: image pull failed, VM failed to boot, network setup
    /// failed, etc. The worker has cleaned up any partial state.
    PodFailed {
        namespace_id: String,
        pod_id: String,
        /// Human-readable error description.
        error: String,
    },

    /// The namespace's gateway exited unexpectedly.
    ///
    /// All pods in the namespace are cancelled. The orchestrator should
    /// consider the namespace dead on this worker.
    NamespaceFailed {
        namespace_id: String,
        error: String,
    },

    /// Acknowledges a [`WorkerCommand::Shutdown`]. The worker is tearing down.
    ShuttingDown,

    /// A non-fatal error occurred while setting up or streaming container logs.
    ///
    /// The pod continues running; only log delivery is affected.
    PodLogStreamError {
        namespace_id: String,
        pod_id: String,
        container_id: String,
        /// Which phase of log streaming failed (e.g., "setup", "streaming").
        phase: String,
        error: String,
    },

    /// The fabric received a frame for a pod IP that can't be delivered locally.
    ///
    /// Fires for both unknown destinations (no route entry) and placeholders
    /// (route entry exists but destination is [`RouteDestination::Placeholder`]).
    /// For placeholders, the fabric applies the basic buffer policy before
    /// reporting the miss.
    ///
    /// This is the **pod-to-pod activation path** — simpler and more limited
    /// than service activation. The orchestrator can respond by scheduling a
    /// suspended pod, updating the route from placeholder to remote worker, etc.
    FabricRouteMiss {
        namespace_id: String,
        dst_ip: Ipv4Addr,
        dst_mac: [u8; 6],
    },

    /// Traffic arrived at a service with no backend (or whose backend isn't ready).
    ///
    /// The service entity buffers frames per its [`ServicePolicy`] and emits
    /// this event so the orchestrator can schedule a pod, assign it as the
    /// backend, and eventually send [`WorkerCommand::ServiceReady`].
    ///
    /// Debounced per service to avoid event floods.
    ///
    /// This is the primary activation signal for services **without** a
    /// protocol activator. Services with an activator use
    /// [`WorkerEvent::ServiceBackendNeed`] instead for more nuanced signaling.
    ServiceActivation {
        namespace_id: String,
        service_id: String,
        /// The service's IP that received traffic.
        dst_ip: Ipv4Addr,
    },

    /// A protocol activator is signaling its backend need level.
    ///
    /// Only emitted for services that have an [`ActivatorConfig`] in their
    /// [`ServicePolicy`]. The orchestrator should use this to decide when to
    /// schedule or release backend pods.
    ///
    /// See [`BackendNeed`] for the signal semantics (pulse vs. level).
    ServiceBackendNeed {
        namespace_id: String,
        service_id: String,
        need: BackendNeed,
    },
}

/// Header sent at the start of each log yamux stream.
///
/// When a container has [`ContainerConfig::capture_output`] set to `true`, the
/// worker opens a new yamux stream toward the orchestrator, sends this header
/// as the first message, then writes raw output bytes. The orchestrator decides
/// what to do with the data (stream to CLI, store, discard).
///
/// On the orchestrator side, use [`OrchestratorConnection::accept_log_stream`]
/// to receive these streams.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LogStreamHeader {
    pub namespace_id: String,
    pub pod_id: String,
    pub container_id: String,
}
