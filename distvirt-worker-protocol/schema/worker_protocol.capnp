@0xb7c5e3a1d9f24680;

# Worker protocol schema for communication between orchestrator and worker.
#
# Covers handshake messages, control stream commands/events, and log stream headers.

# --- Scalar Helpers ---

struct Ipv4Addr {
  raw @0 :UInt32;
  # Network byte order (big-endian). Convert via Ipv4Addr::from(u32::from_be(raw)).
}

struct MacAddr {
  b0 @0 :UInt8;
  b1 @1 :UInt8;
  b2 @2 :UInt8;
  b3 @3 :UInt8;
  b4 @4 :UInt8;
  b5 @5 :UInt8;
}

# --- Config Structs ---

# Network configuration for a namespace's fabric segment.
#
# Defines the IP subnet for all pods and services within the namespace.
# The gateway address is used by the fabric's smoltcp gateway, which provides
# DNS resolution and TUN-based egress for pods.
#
# The gateway MAC is a fixed locally-administered address (02:00:00:00:00:01).
struct NetworkConfig {
  subnet @0 :Ipv4Addr;
  # Subnet address (e.g., 172.16.0.0).
  gateway @1 :Ipv4Addr;
  # Gateway IP within the subnet (e.g., 172.16.0.1).
  prefixLen @2 :UInt8;
  # Subnet prefix length (e.g., 24 for a /24).
}

# Network configuration assigned to a specific pod.
#
# The orchestrator derives this from the namespace's NetworkConfig and the
# pod's assigned IP/MAC. The worker uses it to configure the guest VM's network
# interface.
struct PodNetworkConfig {
  ip @0 :Ipv4Addr;
  # The pod's IP address on the namespace fabric.
  mac @1 :MacAddr;
  # The pod's MAC address (used for the TAP device on the fabric).
  gateway @2 :Ipv4Addr;
  # Gateway IP for the pod's network config (typically the fabric gateway).
  netmask @3 :Text;
  # Subnet mask string (e.g., "255.255.255.0").
}

# Container execution configuration.
#
# When a ContainerSpec references an OCI image, the worker parses the image's
# config (entrypoint, cmd, env, working_dir, user) and merges it with these
# overrides. Explicit values here take precedence; unset fields fall through
# to image defaults.
struct ContainerConfig {
  entrypoint @0 :Text;
  # Main executable path.
  args @1 :List(Text);
  # Arguments to the entrypoint.
  env @2 :List(Text);
  # Environment variables in KEY=VALUE format (OCI convention).
  workingDir @3 :Text;
  # Working directory inside the container. Empty string = not set (falls back to image default).
  hasUid @4 :Bool;
  uid @5 :UInt32;
  # User ID to run as. hasUid=false means fall back to image default.
  hasGid @6 :Bool;
  gid @7 :UInt32;
  # Group ID to run as. hasGid=false means fall back to image default.
  hostname @8 :Text;
  # Hostname for the container. Empty string = not set.
  captureOutput @9 :Bool;
  # Whether to capture stdout/stderr and stream it to the orchestrator
  # via a log stream. See LogStreamHeader.
  stdin @10 :Bool;
  # Whether to enable stdin forwarding for this container.
}

# Specification for a container within a pod.
#
# Pods can contain multiple containers sharing the same VM and network namespace.
# Currently only single-container pods are used, but the protocol supports
# multiple containers from day one for future sidecar/init container support.
struct ContainerSpec {
  containerId @0 :Text;
  # Unique identifier for this container within its pod.
  imageRef @1 :Text;
  # OCI image reference (e.g., "docker.io/library/nginx:latest").
  config @2 :ContainerConfig;
  # Execution configuration (merged with OCI image defaults).
}

# A DNS registry entry mapping a name to an IP address.
#
# The orchestrator owns the authoritative name-to-IP mapping per namespace.
# Workers hold a projected copy, kept in sync via RegistrySync (full replacement)
# and future incremental updates.
#
# DNS entries typically map service names to service IPs (not pod IPs).
# The fabric gateway's DNS server queries this registry to answer DNS queries
# from pods. Names not found are forwarded to upstream DNS.
struct RegistryEntry {
  name @0 :Text;
  # DNS name (e.g., "api", "database").
  ip @1 :Ipv4Addr;
  # IP address the name resolves to (typically a service IP).
}

# Buffer policy for placeholder routes and basic service buffering.
#
# Controls what happens to frames destined for a pod that isn't currently
# running (suspended, scaled-to-zero). This provides basic best-effort
# buffering only -- rich activation features (readiness gating, protocol
# activators) live on services via ServicePolicy.
struct BufferPolicy {
  bufferFrames @0 :UInt32;
  # Maximum number of frames to buffer. 0 means drop immediately
  # (the route miss is still reported so the orchestrator can react).
  timeoutMs @1 :UInt32;
  # How long to buffer before giving up, in milliseconds.
}

# Policy for a fabric-level service entity.
#
# Controls buffering behavior and optional protocol-aware activation for a
# service. Passed via WorkerCommand.createService.
struct ServicePolicy {
  bufferFrames @0 :UInt32;
  # Maximum number of frames to buffer while waiting for readiness.
  timeoutMs @1 :UInt32;
  # How long to buffer before giving up, in milliseconds.
  hasActivator @2 :Bool;
  activator @3 :ActivatorConfig;
  # Optional protocol activator. When hasActivator=false, the service uses default
  # passthrough behavior (buffer all frames, activate on first frame).
  # When hasActivator=true, the specified WASM activator handles traffic with
  # protocol-specific logic.
}

# Protocol activator configuration for a service entity.
#
# Protocol activators replace the default "buffer everything, activate on any
# frame" behavior with protocol-specific logic. They are implemented as WASM
# components running on the fabric.
#
# When ServicePolicy.hasActivator is false, the service uses the default
# passthrough behavior (buffer all frames, activate on first frame).
struct ActivatorConfig {
  union {
    tcp :group {
      # TCP SYN-based activation.
      #
      # An L3 (packet-level) activator that detects new TCP connections by
      # inspecting SYN flags. Only SYN packets trigger activation -- RSTs and
      # stale keepalives are filtered. Buffered SYN packets are replayed to
      # the backend once it becomes available, letting the client's TCP stack
      # complete the handshake normally.
      #
      # Signals BackendNeed.traffic on new SYN arrivals. The fabric applies a
      # timeout policy to determine when to release the backend.
      hasPorts @0 :Bool;
      ports @1 :List(UInt16);
      # TCP destination ports to apply activation to. hasPorts=false means all ports.
      tcpOnly @2 :Bool;
      # If true, non-TCP frames are silently dropped. If false, they are
      # buffered alongside TCP traffic.
      maxFlows @3 :UInt32;
      # Maximum number of tracked flows (source IP + port combinations).
      # Default: 1024.
    }
    http2 @4 :Void;
    # HTTP/2 stream-aware activation (future).
    #
    # An L4 (stream-level) activator that acts as a full H2 proxy. It
    # maintains H2 connections with clients (SETTINGS, PING/ACK), detects
    # new requests via HEADERS frames, and proxies traffic to the backend.
    #
    # Signals BackendNeed.active when H2 streams are open and
    # BackendNeed.none when the last stream closes -- providing precise
    # scale-to-zero without timeout guessing.
  }
}

# Backend need level signaled by a protocol activator.
#
# Protocol activators observe traffic patterns and signal whether a backend
# pod is needed. The orchestrator uses this to decide when to schedule or
# release backend pods.
#
# The key distinction is between pulse (traffic) and level (active) signals:
# - Non-session-aware activators (TCP) use traffic -- "something meaningful
#   just happened." The fabric applies a timeout to determine when it's over.
# - Session-aware activators (H2) use active -- "work is in progress." They
#   know exactly when the last session ends.
#
# Reported via WorkerEvent.serviceBackendNeed.
enum BackendNeed {
  none @0;
  # No meaningful traffic. The backend may be released / scaled to zero.
  traffic @1;
  # Pulse: meaningful traffic detected (e.g., TCP SYN). The orchestrator
  # should ensure a backend is running. The fabric applies its own timeout --
  # if no further traffic (or transition to active) within the timeout
  # window, the orchestrator may release the backend.
  active @2;
  # Level: active sessions require a backend (e.g., open H2 streams).
  # The backend must stay up as long as this is asserted. Cleared when the
  # last active session ends.
}

# Backend target for a service entity.
#
# Identifies the pod that should receive traffic for a service. The backing
# pod can be local (has a TAP on this worker's fabric) or remote (reached
# via the fabric routing table).
struct ServiceBackend {
  podIp @0 :Ipv4Addr;
  # The backing pod's IP address on the namespace fabric.
  podMac @1 :MacAddr;
  # The backing pod's MAC address (used to locate the port on the fabric).
}

# A single entry in the fabric routing table.
#
# Routes for pods that are local to this worker (have a TAP on the local
# fabric) don't need entries -- the fabric already knows about them. Route
# entries are only needed for remote pods and placeholders.
struct FabricRouteEntry {
  ip @0 :Ipv4Addr;
  # The pod's IP address.
  mac @1 :MacAddr;
  # The pod's MAC address.
  destination @2 :RouteDestination;
  # Where to send traffic for this pod.
}

# Where a fabric route entry points.
#
# Each entry in the fabric routing table maps a pod IP/MAC to a destination.
# This handles direct pod-to-pod forwarding (not service traffic, which
# goes through service entities).
struct RouteDestination {
  union {
    remoteWorker :group {
      # The pod is live on another worker. The fabric forwards frames through
      # a tunnel to that worker.
      workerId @0 :Text;
    }
    placeholder :group {
      # The pod is not currently running (suspended, scaled-to-zero). The fabric
      # buffers frames per the policy and reports a FabricRouteMiss event so the
      # orchestrator can react.
      bufferPolicy @1 :BufferPolicy;
    }
  }
}

# --- Handshake Messages ---

# Sent by the worker immediately after the yamux session is established.
#
# The authToken is validated against the cluster identity.
# capabilities tells the orchestrator what this worker can do.
struct WorkerHello {
  authToken @0 :Text;
  # Cluster-derived auth credential.
  capabilities @1 :WorkerCapabilities;
  # What this worker can do.
}

# Advertised capabilities of a worker.
struct WorkerCapabilities {
  hasKvm @0 :Bool;
  hasContainerd @1 :Bool;
  availableAdapters @2 :List(Text);
  # e.g. ["wireguard", "reverse_proxy", "os_routing"]
  maxPods @3 :UInt32;
  availableMemoryMb @4 :UInt64;
  publicEndpoint @5 :Text;
  # Public IP/hostname where this worker is reachable (e.g. "203.0.113.5" or
  # "worker1.example.com"). Empty string means no public endpoint advertised.
}

# Sent by the orchestrator after validating WorkerHello.
#
# Assigns a stable worker ID and pushes adapter configuration.
struct WorkerAccepted {
  workerId @0 :Text;
  adapters @1 :List(AdapterConfig);
}

# Configuration for an ingress adapter assigned to a worker.
struct AdapterConfig {
  union {
    wireguard :group {
      listenPort @0 :UInt16;
      privateKey @1 :Data;
      # 32-byte private key derived from cluster identity.
    }
    reverseProxy :group {
      listenPort @2 :UInt16;
      tlsCert @3 :Data;
      tlsKey @4 :Data;
    }
    osRouting :group {
      interface @5 :Text;
    }
  }
}

# Sent by the worker after it has initialized all assigned adapters.
#
# After this, the orchestrator may begin sending namespace/pod commands.
struct WorkerReady {
}

# --- Control Stream: Command Payloads ---

# Create a namespace's local fabric segment.
#
# The worker creates an L2 switch, a smoltcp gateway at the specified IP,
# and TUN egress. The worker acknowledges with WorkerEvent.namespaceCreated
# when the fabric segment is ready to host pods.
struct CreateNamespaceCmd {
  namespaceId @0 :Text;
  network @1 :NetworkConfig;
}

# Tear down a namespace on this worker.
#
# All pods in the namespace are cancelled (with a graceful shutdown window),
# all services and routes are removed, and the fabric segment is destroyed.
struct DestroyNamespaceCmd {
  namespaceId @0 :Text;
}

# Full-state replacement of the DNS registry for a namespace.
#
# The worker discards its local registry and adopts the provided entries.
# Sent when the worker first joins a namespace, or when the orchestrator
# wants to force reconciliation.
struct RegistrySyncCmd {
  namespaceId @0 :Text;
  entries @1 :List(RegistryEntry);
}

# Incremental update to the DNS registry for a namespace.
#
# The worker applies additions and removals to its local registry.
struct RegistryUpdateCmd {
  namespaceId @0 :Text;
  added @1 :List(RegistryEntry);
  removed @2 :List(Text);
}

# Launch a pod (Firecracker VM) in a namespace.
#
# The worker will:
# 1. Prepare container images (pull if needed, parse OCI config)
# 2. Merge OCI image config with provided overrides
# 3. Launch a Firecracker VM with the specified network config
# 4. Attach the VM's TAP to the namespace's fabric
# 5. Configure guest networking (IP, gateway, DNS pointing at fabric gateway)
# 6. Start containers
# 7. Report WorkerEvent.podRunning when all containers are started
#
# On failure, reports WorkerEvent.podFailed and cleans up partial state.
struct LaunchPodCmd {
  namespaceId @0 :Text;
  podId @1 :Text;
  network @2 :PodNetworkConfig;
  containers @3 :List(ContainerSpec);
}

# Stop a running pod.
#
# If graceful is true, the worker cancels the pod's token, triggering
# a graceful VM shutdown with a timeout before force-killing. If false,
# the pod supervisor is aborted immediately (VM process killed via Drop).
struct StopPodCmd {
  namespaceId @0 :Text;
  podId @1 :Text;
  graceful @2 :Bool;
  # true = graceful shutdown with timeout, false = immediate kill.
}

# Full-state replacement of the fabric routing table for a namespace.
#
# Sent when the worker joins a namespace. The worker discards its local
# routing table and adopts the provided routes.
#
# In local mode (single worker), the routing table is typically empty
# since all pods are local.
struct FabricRouteSyncCmd {
  namespaceId @0 :Text;
  routes @1 :List(FabricRouteEntry);
}

# Incremental update to the fabric routing table.
#
# When a pod launches on another worker, the orchestrator sends a route
# update so this worker knows how to forward frames. When a pod is
# suspended, the orchestrator updates the entry from remoteWorker
# to placeholder.
struct FabricRouteUpdateCmd {
  namespaceId @0 :Text;
  added @1 :List(FabricRouteEntry);
  removedIps @2 :List(Ipv4Addr);
}

# Create a service entity on the namespace's fabric.
#
# The service gets a virtual IP and MAC on the fabric. It starts with no
# backend -- traffic is buffered per policy and ServiceActivation events
# fire so the orchestrator can schedule a backing pod.
#
# Services are projected to all workers participating in a namespace.
struct CreateServiceCmd {
  namespaceId @0 :Text;
  serviceId @1 :Text;
  ip @2 :Ipv4Addr;
  # The service's virtual IP on the fabric.
  mac @3 :MacAddr;
  # The service's virtual MAC on the fabric.
  policy @4 :ServicePolicy;
  # Buffering and activation policy.
}

# Assign or remove the backing pod for a service.
#
# When hasBackend is true, traffic will be forwarded to the specified pod
# once ServiceReady is received. Until then, traffic is still buffered
# (the pod may not be listening yet).
#
# Setting hasBackend to false returns the service to the no-backend state
# (scale-to-zero). Any subsequent traffic triggers activation again.
struct UpdateServiceBackendCmd {
  namespaceId @0 :Text;
  serviceId @1 :Text;
  hasBackend @2 :Bool;
  backend @3 :ServiceBackend;
}

# Mark a service as ready to receive traffic.
#
# Buffered frames are flushed to the backing pod. The orchestrator decides
# when readiness is achieved (container started, health check passed,
# etc.) -- this is orchestrator policy, not a worker concern.
struct ServiceReadyCmd {
  namespaceId @0 :Text;
  serviceId @1 :Text;
}

# Remove a service entity from the fabric.
#
# Any buffered frames are dropped. The service IP is no longer reachable.
struct DestroyServiceCmd {
  namespaceId @0 :Text;
  serviceId @1 :Text;
}

# Add a WireGuard peer to the adapter.
#
# The peer will be associated with the specified namespace. Multiple
# peers can map to the same namespace. The adapter handles L3-L2
# translation so the peer appears as a host on the fabric.
struct AddWireGuardPeerCmd {
  namespaceId @0 :Text;
  peerPublicKey @1 :Data;
  # 32-byte X25519 public key of the peer.
  peerIp @2 :Ipv4Addr;
  # IP address the peer uses inside the namespace.
  hasPresharedKey @3 :Bool;
  presharedKey @4 :Data;
  # Optional 32-byte preshared key for additional security.
}

# Remove a WireGuard peer from the adapter.
struct RemoveWireGuardPeerCmd {
  peerPublicKey @0 :Data;
  # 32-byte X25519 public key identifying the peer to remove.
}

# --- Control Stream: Commands (orchestrator -> worker) ---

# Commands sent from the orchestrator to the worker.
#
# The orchestrator drives all state by sending commands over the control
# stream. The worker executes them and reports results as WorkerEvents.
#
# Resolution order for traffic:
#
# When the fabric receives a frame for a destination IP, it resolves in order:
# 1. Local TAP port -- pod is on this worker, forward directly.
# 2. Service entity -- destination is a service IP. Handles buffering,
#    activation, and forwarding to the backing pod.
# 3. Route table -- destination is a pod IP with a route entry (remote
#    worker or placeholder).
# 4. Flood -- unknown destination, standard L2 behavior.
struct WorkerCommand {
  union {
    createNamespace @0 :CreateNamespaceCmd;
    destroyNamespace @1 :DestroyNamespaceCmd;
    registrySync @2 :RegistrySyncCmd;
    registryUpdate @3 :RegistryUpdateCmd;
    launchPod @4 :LaunchPodCmd;
    stopPod @5 :StopPodCmd;
    fabricRouteSync @6 :FabricRouteSyncCmd;
    fabricRouteUpdate @7 :FabricRouteUpdateCmd;
    createService @8 :CreateServiceCmd;
    updateServiceBackend @9 :UpdateServiceBackendCmd;
    serviceReady @10 :ServiceReadyCmd;
    destroyService @11 :DestroyServiceCmd;
    addWireGuardPeer @12 :AddWireGuardPeerCmd;
    removeWireGuardPeer @13 :RemoveWireGuardPeerCmd;
    shutdown @14 :Void;
    # Shut down the worker entirely.
    #
    # The worker acknowledges with WorkerEvent.shuttingDown, cancels all
    # namespaces and pods, awaits cleanup, then exits.
  }
}

# --- Control Stream: Event Payloads ---

# The namespace's fabric segment is up and ready for pods.
#
# The L2 switch, smoltcp gateway, and DNS registry are initialized.
# Sent in response to WorkerCommand.createNamespace.
struct NamespaceCreatedEvt {
  namespaceId @0 :Text;
}

# The namespace's gateway exited unexpectedly.
#
# All pods in the namespace are cancelled. The orchestrator should
# consider the namespace dead on this worker.
struct NamespaceFailedEvt {
  namespaceId @0 :Text;
  error @1 :Text;
}

# The namespace has been fully torn down on this worker.
#
# All pods have been stopped and cleaned up, all services and routes
# removed, and the fabric segment destroyed. Sent in response to
# WorkerCommand.destroyNamespace.
struct NamespaceDestroyedEvt {
  namespaceId @0 :Text;
}

# The pod's VM is booted and all containers are started.
#
# The pod is on the fabric and reachable at its assigned IP/MAC.
# Sent in response to WorkerCommand.launchPod.
struct PodRunningEvt {
  namespaceId @0 :Text;
  podId @1 :Text;
}

# The pod's main container exited.
#
# The exit code is from the main container (first in the containers list).
struct PodExitedEvt {
  namespaceId @0 :Text;
  podId @1 :Text;
  exitCode @2 :Int32;
}

# The pod could not start.
#
# Possible causes: image pull failed, VM failed to boot, network setup
# failed, etc. The worker has cleaned up any partial state.
struct PodFailedEvt {
  namespaceId @0 :Text;
  podId @1 :Text;
  error @2 :Text;
  # Human-readable error description.
}

# A non-fatal error occurred while setting up or streaming container logs.
#
# The pod continues running; only log delivery is affected.
struct PodLogStreamErrorEvt {
  namespaceId @0 :Text;
  podId @1 :Text;
  containerId @2 :Text;
  phase @3 :Text;
  # Which phase of log streaming failed (e.g., "setup", "streaming").
  error @4 :Text;
}

# Traffic arrived at a service with no backend (or whose backend isn't ready).
#
# The service entity buffers frames per its ServicePolicy and emits
# this event so the orchestrator can schedule a pod, assign it as the
# backend, and eventually send WorkerCommand.serviceReady.
#
# Debounced per service to avoid event floods.
#
# This is the primary activation signal for services without a
# protocol activator. Services with an activator use
# WorkerEvent.serviceBackendNeed instead for more nuanced signaling.
struct ServiceActivationEvt {
  namespaceId @0 :Text;
  serviceId @1 :Text;
  dstIp @2 :Ipv4Addr;
  # The service's IP that received traffic.
}

# A protocol activator is signaling its backend need level.
#
# Only emitted for services that have an ActivatorConfig in their
# ServicePolicy. The orchestrator should use this to decide when to
# schedule or release backend pods.
#
# See BackendNeed for the signal semantics (pulse vs. level).
struct ServiceBackendNeedEvt {
  namespaceId @0 :Text;
  serviceId @1 :Text;
  need @2 :BackendNeed;
}

# The fabric received a frame for a pod IP that can't be delivered locally.
#
# Fires for both unknown destinations (no route entry) and placeholders
# (route entry exists but destination is RouteDestination.placeholder).
# For placeholders, the fabric applies the basic buffer policy before
# reporting the miss.
#
# This is the pod-to-pod activation path -- simpler and more limited
# than service activation. The orchestrator can respond by scheduling a
# suspended pod, updating the route from placeholder to remote worker, etc.
struct FabricRouteMissEvt {
  namespaceId @0 :Text;
  dstIp @1 :Ipv4Addr;
  dstMac @2 :MacAddr;
}

# --- Control Stream: Events (worker -> orchestrator) ---

# Events emitted by the worker back to the orchestrator.
#
# Events report lifecycle transitions and fabric-level signals. The worker
# never makes scheduling decisions -- it only reports what happened so the
# orchestrator can react.
struct WorkerEvent {
  union {
    namespaceCreated @0 :NamespaceCreatedEvt;
    namespaceFailed @1 :NamespaceFailedEvt;
    namespaceDestroyed @2 :NamespaceDestroyedEvt;
    podRunning @3 :PodRunningEvt;
    podExited @4 :PodExitedEvt;
    podFailed @5 :PodFailedEvt;
    shuttingDown @6 :Void;
    # Acknowledges a WorkerCommand.shutdown. The worker is tearing down.
    podLogStreamError @7 :PodLogStreamErrorEvt;
    serviceActivation @8 :ServiceActivationEvt;
    serviceBackendNeed @9 :ServiceBackendNeedEvt;
    fabricRouteMiss @10 :FabricRouteMissEvt;
  }
}

# --- Log Stream Header ---

# Header sent at the start of each log yamux stream.
#
# When a container has ContainerConfig.captureOutput set to true, the
# worker opens a new yamux stream toward the orchestrator, sends this header
# as the first message, then writes raw output bytes. The orchestrator decides
# what to do with the data (stream to CLI, store, discard).
struct LogStreamHeader {
  namespaceId @0 :Text;
  podId @1 :Text;
  containerId @2 :Text;
}
