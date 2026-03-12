---
title: "Worker Protocol Design"
---

## Overview

The worker protocol defines the interface between the **orchestrator** (the brain) and **workers** (the muscle). Workers are dumb executors — they launch pods, manage fabrics, and report events. All planning, scheduling, and state ownership lives in the orchestrator.

This protocol is transport-agnostic. The same message types flow over:
- **Unix domain socket** — local mode, CLI acting as orchestrator
- **TCP/TLS** — distributed mode, remote workers connecting to a central orchestrator

The transport is a **yamux**-multiplexed bidirectional stream. The primary control stream carries length-prefixed **Cap'n Proto** messages (commands and events). Additional yamux streams carry out-of-band data like container log output.

The wire format is Cap'n Proto, with the schema at `distvirt-worker-protocol/schema/worker_protocol.capnp`.

---

## Cluster Identity

The cluster has a single root identity established at cluster creation time. All cryptographic material in the system derives from this root:

- **Worker authentication** — workers present a token or certificate derived from the cluster identity when connecting. The orchestrator validates against the cluster root. No per-worker allowlists needed.
- **Ingress adapter keys** — WireGuard private keys, TLS certificates for reverse proxies, etc. are derived from the cluster identity and pushed to workers during the handshake. This means a developer's WireGuard config doesn't break when their namespace moves to a different worker — the cluster identity is the same, only the endpoint address changes.
- **Inter-worker tunnels** — tunnel encryption keys derive from the cluster identity. Workers authenticate via Noise static keys.

The orchestrator holds the root secret (or delegates to a secret manager). Workers never see the root — they receive only the derived key material they need for their assigned adapters.

**Worker bootstrap** is minimal: a worker image (AMI, container, etc.) only needs:
- Orchestrator address
- Auth token or mTLS certificate (derived from cluster identity)

Everything else — which adapters to run, listen ports, key material, namespace assignments — comes from the orchestrator during the handshake.

---

## Connection Lifecycle

Workers connect to the orchestrator, not the other way around. This means:
- No discovery problem — workers know the orchestrator address
- NAT-friendly — workers behind NATs connect outbound
- Ephemeral workers — spin up identical instances, they auto-register on boot; terminate them, orchestrator detects the disconnect
- Local mode — same flow, just `unix:///run/distvirt.sock`

```
Worker                              Orchestrator
  |                                      |
  |──── Connect (TCP/UDS) ──────────────>|
  |──── Establish yamux session ────────>|
  |                                      |
  |──── WorkerHello (capabilities) ────>|
  |<─── WorkerAccepted (config) ────────|
  |                                      |
  |   (worker sets up adapters, etc.)    |
  |──── WorkerReady ───────────────────>|
  |                                      |
  |   control stream (commands/events)   |
  |<──── commands ───────────────────────|
  |────── events ───────────────────────>|
  |                                      |
  |   log streams (worker-initiated)     |
  |────── LogStreamHeader ─────────────>|
  |────── raw output bytes ────────────>|
  |                                      |
  |     (disconnect = worker is gone)    |
```

The orchestrator is the yamux Client (opens the control stream). The worker is the yamux Server (accepts the control stream, opens log streams back toward the orchestrator).

### Handshake

The first messages on the control stream are a three-step handshake before normal command/event flow begins:

1. **`WorkerHello`** — the worker identifies itself and advertises its capabilities (what it can do, not what it should do).
2. **`WorkerAccepted`** — the orchestrator assigns a stable `worker_id`, pushes adapter configuration, tunnel settings, and storage pool assignments.
3. **`WorkerReady`** — the worker confirms it has set up its assigned adapters and reports tunnel/transfer listen ports and its tunnel public key.

In local mode (single in-process worker), the handshake is still performed but with a fixed worker ID and no adapter config (adapters are not used in local mode).

After `WorkerReady`, normal command/event flow begins. The orchestrator will not send namespace or pod commands until it has received `WorkerReady`.

On disconnect, the orchestrator considers all pods on that worker lost. It may reschedule them to other workers depending on policy.

---

## Core Concepts

### Worker

A worker process running on a machine (physical or virtual). It owns local resources: CPU, memory, disk, network interfaces. A single worker can host pods from multiple namespaces.

The worker is responsible for:
- Managing the local VMM (Firecracker)
- Managing local fabric segments (one per namespace it participates in)
- Running ingress adapters as assigned by the orchestrator during handshake
- Preparing container images
- Reporting pod lifecycle events
- Managing inter-worker tunnels autonomously based on the worker peer registry
- Reporting resource pressure metrics (PSI) and storage pool capacity
- Executing suspend/resume and artifact transfer operations

The worker is NOT responsible for:
- Deciding what to run or where
- IP assignment
- Dependency ordering
- Service discovery ownership
- Cross-worker networking decisions

### Namespace

An isolated environment with its own L3 network fabric. Different users/deployments get different namespaces. A namespace's fabric can span multiple workers — each worker runs a local fabric segment, and segments are connected via tunnel ports using Noise-encrypted inter-worker tunnels.

The orchestrator creates and destroys namespaces on workers as needed. If a worker has no pods in a namespace, the orchestrator can tear down that worker's namespace segment.

Each namespace can be assigned a `segment_id` for inter-worker tunnel routing. When present, the segment ID is used to multiplex traffic from multiple namespaces over a single tunnel connection between workers.

### Pod

The smallest schedulable unit. A pod is a Firecracker VM with:
- A single IP and MAC on its namespace's fabric
- One or more containers sharing the VM's network namespace
- A lifecycle managed as a unit (start, stop, suspend, resume)
- Optional resource limits (vCPUs and memory)

For now, pods contain a single container. The protocol supports multiple from day one to avoid a redesign when sidecars or init containers arrive.

Pods support **suspend/resume**: a running pod can be suspended (Firecracker snapshot written to a storage pool), and later resumed from that snapshot on the same or a different worker. This enables scale-to-zero with fast warm-start.

### Service

A service is a stable network identity with its own virtual IP and MAC on the namespace's fabric. Services are the recommended way for pods to communicate — DNS names resolve to service IPs, not pod IPs.

The key separation: a **service IP** is the stable addressable identity; a **pod IP** is an ephemeral backend. The service entity on the fabric is the boundary for buffering, activation, readiness gating, and protocol enforcement. This cleanly separates pod lifecycle (VM booted, network configured) from service readiness (application listening, health check passed).

```
Client pod → Service IP (virtual) → [buffer / activate / ready?] → Pod IP (real)
```

A service can be in one of these states:
- **No backend** — the service IP exists on the fabric but no pod is assigned. Traffic is buffered per policy and a `ServiceActivation` event fires so the orchestrator can schedule a pod. This is the scale-to-zero state.
- **Backend assigned, not ready** — a pod is assigned but hasn't passed readiness. Traffic is buffered, no activation event (orchestrator already knows).
- **Ready** — traffic flows through to the backing pod.

The orchestrator manages service lifecycle via `CreateService`, `UpdateServiceBackend`, `ServiceReady`, and `DestroyService` commands. Services are projected to all workers participating in a namespace (same as DNS entries).

### Endpoints

Endpoints are a unified abstraction for all addressable entities on the namespace fabric. An endpoint has an IP and a kind (service, pod, or WireGuard peer), along with placement and backend information.

The orchestrator uses `EndpointSync` (full-state replacement) and `EndpointUpdate` (incremental upsert/remove) to keep each worker's endpoint table in sync. Workers report endpoint-level activation and flow status events.

Endpoint kinds:
- **Service** — a service endpoint with its policy, optional backend, and readiness state.
- **Pod** — a pod endpoint with optional placement (which worker hosts it).
- **WireGuardPeer** — a WireGuard peer endpoint with optional placement.

### DNS Registry

The orchestrator owns the authoritative name-to-IP mapping for each namespace. Workers hold a projected copy, kept in sync via full-state syncs and incremental deltas. The local fabric gateway uses this projection to answer DNS queries from pods.

DNS entries typically map service names to **service IPs** (not pod IPs). The DNS registry is structurally unchanged — it's just `name → IP` — but the IPs now refer to service entities on the fabric rather than pods directly.

### Fabric Routing Table (Pod-to-Pod)

Separate from services, the fabric routing table handles **direct pod-to-pod** forwarding. Every pod has an IP and MAC, and any pod can reach any other pod by IP, even without going through a service.

The orchestrator owns the authoritative routing table: a mapping of IP/MAC to a **destination** for each namespace. Workers hold a projected copy, kept in sync the same way as the DNS registry (full sync + deltas).

Each route entry has one of two destination types:

- **Remote worker** — the pod is live on another worker. The fabric forwards frames through the tunnel to that worker.
- **Placeholder** — the pod is not currently running (suspended, scaled-to-zero, pending). The fabric applies a basic buffering policy and reports a route miss to the orchestrator.

This is a single unified table. When a suspended pod gets scheduled and boots on a worker, the orchestrator simply updates the entry from placeholder → remote worker (or it becomes local on the hosting worker and the entry is removed). No coordinating across separate tables.

Pods that are local to this worker (have a TAP on the local fabric) don't need route entries — the fabric already knows about them.

When the fabric receives a frame for a destination that has no local TAP, no service entity, and no route entry at all, it reports a **route miss** with no buffering (unknown destination). When it hits a placeholder entry, it applies the placeholder's buffer policy and also reports a route miss. The orchestrator can then schedule the pod, update routes, and the buffered frames get delivered.

**Pod placeholder buffer policies** are deliberately limited compared to service policies — they provide basic best-effort buffering only:
- **Buffer frames** — queue up to N frames for up to M milliseconds, then drop.
- **Drop** — discard immediately (still report the miss so the orchestrator can react).

Rich activation features (readiness gating, protocol activators, TCP SYN hold) live on services, not on pod placeholders. The pod routing table is the "it just works" fallback for direct communication, not the primary traffic path.

In local mode (single worker), the routing table is typically empty — all pods are local. But the protocol supports it from day one so multi-worker doesn't require a redesign.

### Services vs. Pod Routes

Traffic to a destination IP is resolved in this order:

1. **Local port (IP table)** — the pod is on this worker, forward directly via IP-to-port table.
2. **Service entity** — the destination is a service IP. The service entity handles buffering, activation, and forwarding to the backing pod.
3. **Route table** — the destination is a pod IP with a route entry (remote worker or placeholder). Basic forwarding or buffering.
4. **Drop** — unknown destination, no match.

Services are the recommended path for inter-service communication. Pod routes preserve the flat network illusion for cases where pods connect directly by IP.

### Storage Pools

Workers advertise their available storage pools during the handshake via `WorkerCapabilities`. Each pool has an ID, filesystem path, and capacity/availability info. The orchestrator assigns pools back to the worker via `WorkerAccepted`.

Storage pools are used for:
- **Snapshot storage** — suspend snapshots are written to a specific pool
- **Artifact transfers** — artifacts can be copied between pools (locally or across workers)

Workers periodically report pool capacity changes via `PoolCapacityUpdate` events so the orchestrator has fresh data for placement and eviction decisions.

### Worker Conditions

Workers report level-triggered status conditions via `WorkerCondition` events. Conditions represent ongoing states like "low storage", "spot preemption imminent", or "tunnel peer unreachable". Active conditions persist until explicitly deasserted (`active: false`). On worker disconnect, all conditions are implicitly cleared.

### Pressure Metrics

Workers with Linux PSI support periodically report CPU, memory, and I/O pressure metrics via `PressureUpdate` events. Each resource dimension includes partial and full stall percentages over 10-second and 60-second rolling windows. The orchestrator uses these for real-time pressure-aware scheduling. Workers without PSI support (non-Linux) never send this event; the orchestrator falls back to static accounting.

---

## Messages: Handshake

```
WorkerHello {
  auth_token: String,             // cluster-derived auth credential
  capabilities: WorkerCapabilities,
}

WorkerCapabilities {
  has_kvm: bool,
  has_containerd: bool,
  available_adapters: Vec<String>, // e.g. ["wireguard", "reverse_proxy", "os_routing"]
  max_pods: u32,
  available_memory_mb: u64,
  public_endpoint: String,        // public IP/hostname, empty = none
  pools: Vec<PoolInfo>,           // storage pools available on this worker
}

PoolInfo {
  pool_id: String,
  path: String,
  capacity_bytes: u64,
  available_bytes: u64,
}

WorkerAccepted {
  worker_id: String,
  adapters: Vec<AdapterConfig>,
  tunnel_encrypted: bool,         // whether inter-worker tunnels use encryption
  pools: Vec<PoolInfo>,           // storage pool assignments for this worker
}

enum AdapterConfig {
  WireGuard {
    listen_port: u16,
    private_key: [u8; 32],        // derived from cluster identity
  },
  ReverseProxy {
    listen_port: u16,
    tls_cert: Vec<u8>,            // derived from cluster identity
    tls_key: Vec<u8>,
  },
  OsRouting {
    interface: String,
  },
}

WorkerReady {
  tunnel_listen_port: Option<u16>,   // port for inter-worker tunnel connections
  tunnel_public_key: Option<[u8; 32]>, // 32-byte Noise static public key
  transfer_listen_port: Option<u16>, // port for artifact transfer connections
}
```

`WorkerHello` — sent by the worker immediately after the yamux session is established. The `auth_token` is validated against the cluster identity. `capabilities` tells the orchestrator what this worker can do — the orchestrator uses this to decide what config to assign. Capabilities now include resource capacity (`max_pods`, `available_memory_mb`), public endpoint information, and available storage pools.

`WorkerAccepted` — the orchestrator assigns a stable `worker_id`, pushes adapter configuration, tunnel encryption settings, and storage pool assignments. The adapter list is the intersection of what the worker can do (capabilities) and what the orchestrator wants it to do (cluster policy). Key material (WireGuard private key, TLS certs) is derived from the cluster identity — all workers sharing the same adapter type get the same keys, so clients aren't affected by namespace migration between workers.

`WorkerReady` — the worker has initialized all assigned adapters (bound sockets, loaded keys) and is ready to accept namespace and pod commands. The worker reports its tunnel listen port and public key (for inter-worker tunnels) and its transfer listen port (for artifact transfers).

If authentication fails, the orchestrator closes the connection without sending `WorkerAccepted`.

---

## Messages: Orchestrator to Worker (Commands)

### Namespace Lifecycle

```
CreateNamespace {
  namespace_id: String,
  network: NetworkConfig,
}

NetworkConfig {
  subnet: Ipv4Addr,        // e.g. 172.16.0.0
  prefix_len: u8,           // e.g. 24
  gateway: Ipv4Addr,        // e.g. 172.16.0.1
  segment_id: Option<u16>,  // optional segment ID for inter-worker tunnel routing
}

DestroyNamespace {
  namespace_id: String,
}
```

`CreateNamespace` tells the worker to stand up a local fabric segment: create the L3 IP fabric, create the smoltcp gateway at the specified IP, set up TUN egress. If a `segment_id` is provided, the namespace's traffic is tagged with that ID for multiplexing over inter-worker tunnels. The worker acknowledges with a `NamespaceCreated` event.

`DestroyNamespace` tears down all pods in the namespace on this worker (cancelling them with a graceful shutdown window), then tears down the fabric segment.

### DNS Registry Sync

```
RegistrySync {
  namespace_id: String,
  entries: Vec<RegistryEntry>,
}

RegistryUpdate {
  namespace_id: String,
  added: Vec<RegistryEntry>,
  removed: Vec<String>,
}

RegistryEntry {
  name: String,
  ip: Ipv4Addr,
}
```

`RegistrySync` is a full-state replacement — the worker discards its local registry for this namespace and adopts the provided entries. Sent when the worker first joins a namespace, or when the orchestrator wants to force reconciliation.

`RegistryUpdate` is an incremental delta. The worker applies additions and removals to its local registry.

The gateway's DNS server queries this local registry. Names not found are forwarded to upstream DNS (for external resolution).

### Service Lifecycle

```
CreateService {
  namespace_id: String,
  service_id: String,
  ip: Ipv4Addr,
  mac: MacAddr,             // service's virtual MAC on the fabric
  policy: ServicePolicy,
}

ServicePolicy {
  buffer_frames: u32,       // max packets to buffer (0 = drop immediately)
  timeout_ms: u32,          // how long to buffer before giving up
  activator: Option<ActivatorConfig>,  // protocol-aware activation (None = default passthrough)
}

enum ActivatorConfig {
  // TCP SYN-based activation. Detects new connections via SYN flags,
  // filters RSTs and stale keepalives, replays buffered SYNs to backend.
  Tcp {
    ports: Option<Vec<u16>>,  // destination ports to activate on (None = all)
    tcp_only: bool,           // drop non-TCP packets if true
    max_flows: u32,           // max tracked source IP+port combinations
  },
  // HTTP/2 stream-aware activation (future). Full H2 proxy that maintains
  // client connections and signals precise backend need based on open streams.
  Http2 {},
}

UpdateServiceBackend {
  namespace_id: String,
  service_id: String,
  backend: Option<ServiceBackend>,
}

ServiceBackend {
  pod_ip: Ipv4Addr,
  pod_mac: MacAddr,         // backing pod's MAC (used to locate the port on the fabric)
}

ServiceReady {
  namespace_id: String,
  service_id: String,
}

DestroyService {
  namespace_id: String,
  service_id: String,
}
```

`CreateService` tells the worker to create a service entity on the namespace's fabric with a virtual IP and MAC. The service starts with no backend (traffic is buffered per policy, activation events fire). Services are projected to all workers participating in a namespace.

`UpdateServiceBackend` assigns or removes the backing pod for a service. When a backend is assigned, traffic is still buffered until `ServiceReady` is received — the pod may not be listening yet. Setting `backend: None` returns the service to the no-backend state (scale-to-zero). The backing pod can be local (has a TAP on this worker's fabric) or remote (reached via the route table). The `ServiceBackend` includes both the pod's IP and MAC so the fabric can locate the correct port.

`ServiceReady` tells the worker that the service's backing pod is ready to receive traffic. Buffered packets are flushed to the backing pod. The orchestrator decides when readiness is achieved (container started, health check passed, etc.) — this is orchestrator policy, not a worker concern.

`DestroyService` removes the service entity from the fabric. Any buffered packets are dropped.

### Fabric Routing (Pod-to-Pod)

```
FabricRouteSync {
  namespace_id: String,
  routes: Vec<FabricRouteEntry>,
}

FabricRouteUpdate {
  namespace_id: String,
  added: Vec<FabricRouteEntry>,
  removed_ips: Vec<Ipv4Addr>,
}

FabricRouteEntry {
  ip: Ipv4Addr,
  mac: MacAddr,             // pod's MAC address
  destination: RouteDestination,
}

enum RouteDestination {
  RemoteWorker { worker_id: String },
  Placeholder { buffer_policy: BufferPolicy },
}

BufferPolicy {
  buffer_frames: u32,       // max packets to buffer (0 = drop immediately)
  timeout_ms: u32,          // how long to buffer before giving up
}
```

`FabricRouteSync` is a full-state replacement of the routing table for a namespace on this worker. Sent when the worker joins a namespace.

`FabricRouteUpdate` is an incremental delta. When a new pod launches on Worker B, the orchestrator sends a route update to Worker A so it knows how to forward frames. When a pod is suspended, the orchestrator updates the entry from `RemoteWorker` to `Placeholder` with a basic buffer policy.

Routes for pods that are local to this worker don't need entries — the fabric already knows about them via the local TAP port.

Note: `BufferPolicy` on pod routes is deliberately simpler than `ServicePolicy`. Rich activation features (readiness gating, protocol activators) live on services. Pod routes provide basic best-effort buffering only.

### Pod Lifecycle

```
LaunchPod {
  namespace_id: String,
  pod_id: String,
  network: PodNetworkConfig,
  containers: Vec<ContainerSpec>,
  resources: Option<ResourceRequirements>,
}

PodNetworkConfig {
  ip: Ipv4Addr,
  mac: MacAddr,              // pod's MAC address (used for the TAP device)
  gateway: Ipv4Addr,         // gateway IP for the pod's network config
  netmask: String,            // e.g. "255.255.255.0"
}

ResourceValues {
  memory_mib: u64,
  vcpus: u32,
}

ResourceRequirements {
  requests: Option<ResourceValues>,
  limits: Option<ResourceValues>,
}

ContainerSpec {
  container_id: String,
  image_ref: String,          // image reference (e.g. "docker.io/library/nginx:latest")
  config: ContainerConfig,
}

ContainerConfig {
  entrypoint: Vec<String>,    // entrypoint command (e.g. ["/bin/sh", "-c"])
  args: Vec<String>,
  env: Vec<String>,           // KEY=VALUE format (OCI convention)
  working_dir: Option<String>,
  uid: Option<u32>,
  gid: Option<u32>,
  hostname: Option<String>,
  capture_output: bool,
  stdin: bool,                // whether to enable stdin forwarding
}

StopPod {
  namespace_id: String,
  pod_id: String,
  graceful: bool,             // true = send shutdown, wait; false = kill immediately
}

Shutdown {}
```

`PodNetworkConfig` includes the full network configuration the pod needs to configure its guest interface, including the pod's MAC address for the TAP device. The orchestrator derives these from the namespace's `NetworkConfig` and the pod's assigned IP/MAC.

When a `ContainerSpec` references an OCI image, the worker parses the image's config (entrypoint, cmd, env, working_dir, user) and merges it with the `ContainerConfig` overrides. Explicit overrides take precedence; empty/None fields fall through to the image defaults.

`LaunchPod` tells the worker to:
1. Prepare container images (pull if needed, parse OCI config)
2. Merge OCI image config with provided overrides
3. Launch a Firecracker VM with the specified network config and resource limits
4. Attach the VM's TAP to the namespace's fabric
5. Configure guest networking (IP, MAC, gateway, DNS pointing at fabric gateway)
6. Add and start containers
7. Report `PodRunning` when all containers are started

The optional `resources` field specifies resource requirements. `requests` is the minimum the pod needs (used for scheduling decisions by the orchestrator). `limits` is the maximum the pod can use (the worker configures the VM accordingly — vCPU count and memory cap).

`StopPod` tells the worker to shut down the pod. Graceful cancels the pod's token, triggering a graceful VM shutdown with a timeout before force-killing. Non-graceful aborts the pod supervisor immediately (VM process killed via Drop).

`Shutdown` tells the worker to shut down entirely. The worker acknowledges with `ShuttingDown`, cancels all namespaces and pods, awaits cleanup, then exits.

### Suspend / Resume

```
SuspendPod {
  namespace_id: String,
  pod_id: String,
  snapshot_id: String,        // artifact ID assigned by orchestrator
  pool_id: String,            // storage pool to write snapshot to
}

ResumePod {
  namespace_id: String,
  pod_id: String,
  snapshot_id: String,        // artifact ID of the snapshot to restore from
  network: PodNetworkConfig,  // network config for restored pod (may differ from original)
  pool_id: String,            // storage pool where snapshot is stored
}

DeleteSnapshot {
  snapshot_id: String,        // artifact ID to delete
  pool_id: String,            // storage pool where artifact is stored
}
```

`SuspendPod` tells the worker to snapshot a running pod's state to disk. The worker sends `PrepareSuspend` to the guest, waits for `SuspendReady`, takes a Firecracker snapshot, and kills the VM. The snapshot is stored in the specified storage pool keyed by `snapshot_id`. On success, the worker emits `PodSuspended`; on failure, `PodSuspendFailed`. If the pod exits or crashes before the suspend completes, the worker emits `PodFailed` instead (no snapshot artifact is created).

`ResumePod` tells the worker to restore a previously suspended pod from a snapshot. The worker restores the Firecracker VM, reconnects the vsock session, and re-attaches the pod to the fabric with the provided network config (which may differ from the original — the pod can resume on a different worker with a different IP). On success, the worker emits `PodRunning`; on failure (corrupt snapshot, VM restore error, etc.), `PodFailed`. The orchestrator may fall back to a cold launch via `LaunchPod`.

`DeleteSnapshot` removes a snapshot artifact from disk. Idempotent — succeeds even if the artifact doesn't exist.

### Artifact Transfer

```
TransferArtifact {
  transfer_id: u64,           // correlation ID assigned by orchestrator
  source_artifact_id: String,
  source_pool_id: String,
  dest_artifact_id: String,   // new artifact ID for the copy
  dest_pool_id: String,
  dest_endpoint: Option<String>, // "host:port" of dest worker, None = local copy
}
```

`TransferArtifact` tells the worker to copy an artifact from one pool to another. If `dest_endpoint` is `None`, the copy is local (same worker, different pool). If provided, the worker streams the artifact over TCP to the destination worker's transfer listener. On success, the destination emits `ArtifactTransferReceived`; on failure, the source emits `TransferFailed`. The `transfer_id` is carried through all related events for correlation.

### WireGuard Peer Management

```
AddWireGuardPeer {
  namespace_id: String,
  peer_public_key: [u8; 32],   // X25519 public key
  peer_ip: Ipv4Addr,           // IP address the peer uses inside the namespace
  preshared_key: Option<[u8; 32]>, // optional preshared key
}

RemoveWireGuardPeer {
  peer_public_key: [u8; 32],   // X25519 public key identifying the peer
}
```

`AddWireGuardPeer` adds a WireGuard peer to the adapter, associated with a specific namespace. Multiple peers can map to the same namespace. The adapter handles L3-L2 translation so the peer appears as a host on the fabric.

`RemoveWireGuardPeer` removes a peer by its public key.

### Inter-Worker Tunnel Management

```
WorkerRegistrySync {
  workers: Vec<WorkerPeerInfo>,
}

WorkerPeerInfo {
  worker_id: String,
  endpoint: String,           // "host:port" for tunnel connections
  public_key: [u8; 32],       // 32-byte Noise static public key
  segments: Vec<u16>,         // segment IDs this worker participates in
}
```

`WorkerRegistrySync` is a full-state replacement of the worker peer registry. Sent to all workers when the set of tunnel-capable workers changes. Each worker uses this registry to **autonomously** establish or tear down tunnels to peers — the orchestrator does not micromanage individual tunnel connections. Workers report tunnel status changes via `TunnelStatus` events.

This replaces the originally planned `ConnectFabric` command. Instead of the orchestrator explicitly telling each worker pair to connect, the orchestrator broadcasts the full peer registry and lets workers manage their own connections. This is simpler and more resilient to transient failures (workers can reconnect autonomously).

### Endpoint Sync

```
EndpointSync {
  namespace_id: String,
  endpoints: Vec<EndpointSpec>,
}

EndpointUpdate {
  namespace_id: String,
  upserted: Vec<EndpointSpec>,
  removed_ips: Vec<Ipv4Addr>,
}

EndpointSpec {
  ip: Ipv4Addr,
  kind: EndpointKind,
}

enum EndpointKind {
  Service {
    service_id: String,
    policy: ServicePolicy,
    backend: Option<EndpointPodBackend>,
  },
  Pod {
    placement: Option<EndpointPlacement>,
  },
  WireGuardPeer {
    placement: Option<EndpointPlacement>,
  },
}

EndpointPodBackend {
  pod_ip: Ipv4Addr,
  placement: Option<EndpointPlacement>,
  ready: bool,
}

EndpointPlacement {
  worker_id: String,
}
```

`EndpointSync` is a full-state replacement of the endpoint table for a namespace. `EndpointUpdate` is an incremental delta (upsert and remove by IP).

The endpoint protocol provides a unified view of all addressable entities on the fabric. Each endpoint has an IP and a kind:

- **Service** — includes the service's policy, and optionally its backend pod (with placement and readiness info).
- **Pod** — includes optional placement information (which worker hosts it).
- **WireGuardPeer** — includes optional placement (which worker hosts the WireGuard adapter).

Workers report `EndpointActivation` when traffic arrives at an endpoint that needs orchestrator attention, and `EndpointFlowStatus` to signal active/idle flow state.

---

## Messages: Worker to Orchestrator (Events)

### Control Stream Events

```
NamespaceCreated {
  namespace_id: String,
}

NamespaceFailed {
  namespace_id: String,
  error: String,
}

NamespaceDestroyed {
  namespace_id: String,
}

PodRunning {
  namespace_id: String,
  pod_id: String,
}

PodExited {
  namespace_id: String,
  pod_id: String,
  exit_code: i32,
}

PodFailed {
  namespace_id: String,
  pod_id: String,
  error: String,
}

ShuttingDown {}

PodLogStreamError {
  namespace_id: String,
  pod_id: String,
  container_id: String,
  phase: String,
  error: String,
}
```

`NamespaceCreated` — the namespace's fabric, gateway, and DNS registry are up and ready for pods.

`NamespaceFailed` — the namespace's gateway exited unexpectedly. All pods in the namespace are cancelled. The orchestrator should consider the namespace dead.

`PodRunning` — the VM is booted, all containers are started, the pod is on the fabric. Also emitted on successful resume from a snapshot.

`PodExited` — the main container exited. The exit code is from the main container (first in the containers list). The VM may still be running if there are other containers; pod exit policy is a future concern.

`PodFailed` — the pod could not start (image pull failed, VM failed to boot, etc.). The worker has cleaned up any partial state. Also emitted if a pod crashes during a suspend operation.

`ShuttingDown` — acknowledges a `Shutdown` command. The worker is tearing down.

`NamespaceDestroyed` — the namespace has been fully torn down on this worker. All pods stopped, all services and routes removed, fabric segment destroyed. Sent in response to `DestroyNamespace`.

`PodLogStreamError` — a non-fatal error occurred while setting up or streaming container logs. The pod continues running; only log delivery is affected.

### Suspend / Resume Events

```
PodSuspended {
  namespace_id: String,
  pod_id: String,
  snapshot_id: String,       // artifact ID of the snapshot
  snapshot_size_bytes: u64,  // total size on disk
  pool_id: String,           // storage pool where snapshot was written
}

PodSuspendFailed {
  namespace_id: String,
  pod_id: String,
  error: String,
}
```

`PodSuspended` — the pod has been successfully suspended, the snapshot written to disk, and the VM killed. The snapshot can be used to resume the pod later via `ResumePod`.

`PodSuspendFailed` — the pod could not be suspended. The pod may still be running (if the error occurred before the VM was killed) or may be in an undefined state. The orchestrator should stop the pod if it needs to recover.

### Artifact Events

```
ArtifactWriteStarted {
  namespace_id: String,
  artifact_id: String,
  pool_id: String,
}

ArtifactWriteCommitted {
  namespace_id: String,
  artifact_id: String,
  pool_id: String,
  size_bytes: u64,
}

ArtifactTransferReceived {
  transfer_id: u64,
  source_artifact_id: String,
  source_pool_id: String,
  dest_artifact_id: String,
  dest_pool_id: String,
  size_bytes: u64,
}

TransferFailed {
  transfer_id: u64,
  source_artifact_id: String,
  source_pool_id: String,
  dest_artifact_id: String,
  dest_pool_id: String,
  error: String,
}
```

`ArtifactWriteStarted` — emitted before beginning a snapshot write. The orchestrator records the placement as in-progress (Writing status) so other workers don't try to read a half-written artifact.

`ArtifactWriteCommitted` — emitted after a snapshot is fully written and durable on disk. The orchestrator transitions the placement from Writing to Ready, making it available for resume operations.

`ArtifactTransferReceived` — emitted by the destination worker after a transferred artifact is fully written and durable.

`TransferFailed` — emitted by the source worker when it cannot complete a transfer (network error, missing artifact, etc.).

### Tunnel Events

```
TunnelStatus {
  peer_worker_id: String,
  status: TunnelPeerStatus,
}

enum TunnelPeerStatus {
  Connected,
  Disconnected { error: String },
  HandshakeFailed { error: String },
}
```

`TunnelStatus` — reports the status of an inter-worker tunnel connection. Workers manage tunnels autonomously based on the peer registry; this event lets the orchestrator track connectivity for diagnostics and scheduling.

### Worker Condition Events

```
WorkerCondition {
  key: String,              // e.g. "storage/root-low", "spot/preemption"
  active: bool,             // true = assert, false = deassert
  message: String,          // human-readable detail
}
```

`WorkerCondition` — a level-triggered status condition. Active conditions persist until explicitly deasserted. On worker disconnect, all conditions are implicitly cleared.

### Storage Pool Events

```
PoolCapacityUpdate {
  pools: Vec<PoolInfo>,     // fresh capacity data for all pools
}
```

`PoolCapacityUpdate` — sent periodically when capacity has meaningfully changed. The orchestrator uses this for eviction and placement decisions.

### Pressure Events

```
PressureUpdate {
  cpu: PsiMetrics,
  memory: PsiMetrics,
  io: PsiMetrics,
}

PsiMetrics {
  some_avg10: f64,          // partial stall %, 10s rolling average
  some_avg60: f64,          // partial stall %, 60s rolling average
  full_avg10: f64,          // full stall %, 10s rolling average
  full_avg60: f64,          // full stall %, 60s rolling average
}
```

`PressureUpdate` — periodic PSI pressure metrics (sent every ~10 seconds or on threshold crossings). Only sent on Linux workers with PSI support. The orchestrator uses these for real-time pressure-aware scheduling.

### Fabric Events

```
ServiceActivation {
  namespace_id: String,
  service_id: String,
  dst_ip: Ipv4Addr,
}

FabricRouteMiss {
  namespace_id: String,
  dst_ip: Ipv4Addr,
  dst_mac: MacAddr,
}

ServiceBackendNeed {
  namespace_id: String,
  service_id: String,
  need: BackendNeed,
}

enum BackendNeed {
  None,      // no meaningful traffic, backend may be released
  Traffic,   // pulse: meaningful traffic detected (e.g. TCP SYN), start/extend timeout
  Active,    // level: active sessions require backend (e.g. open H2 streams)
}
```

`ServiceActivation` — traffic arrived at a service that has no backend (or whose backend isn't ready). The service entity buffers packets per its policy and emits this event so the orchestrator can schedule a pod, assign it as the backend, and eventually send `ServiceReady`. Debounced per service to avoid event floods.

`FabricRouteMiss` — the worker's fabric received a packet for a **pod IP** (not a service IP) that it can't deliver locally. This fires for both unknown destinations (no route entry at all) and placeholders (route entry exists but destination is a `Placeholder`). For placeholders, the fabric applies the basic buffer policy before reporting the miss. The miss includes both the destination IP and MAC. The orchestrator can respond by scheduling a suspended pod, updating the route entry from placeholder to remote worker, etc. This is the pod-to-pod activation path — simpler and more limited than service activation.

`ServiceBackendNeed` — a protocol activator is signaling its backend need level. Only emitted for services with an `ActivatorConfig`. The `BackendNeed` value distinguishes pulse signals (`Traffic` — something meaningful happened, start/extend a timeout) from level signals (`Active` — sessions are open, keep the backend up). Services without an activator use `ServiceActivation` instead.

### Endpoint Events

```
EndpointActivation {
  namespace_id: String,
  ip: Ipv4Addr,
  service_id: Option<String>,
}

EndpointFlowStatus {
  namespace_id: String,
  ip: Ipv4Addr,
  service_id: Option<String>,
  has_active_flows: bool,
}
```

`EndpointActivation` — traffic arrived at an endpoint that needs attention. If the endpoint is a service, `service_id` is set.

`EndpointFlowStatus` — signals whether an endpoint has active flows. The orchestrator uses this to decide when to release backends.

### Log Streams (Out-of-Band)

Container output (stdout/stderr) is delivered over **separate yamux streams**, not as events on the control stream. This avoids head-of-line blocking — a pod producing heavy output doesn't block command/event processing.

```
LogStreamHeader {
  namespace_id: String,
  pod_id: String,
  container_id: String,
}
```

When a container has `capture_output: true`, the worker opens a new yamux stream toward the orchestrator, sends a `LogStreamHeader` as the first message, then writes raw output bytes. The orchestrator decides what to do with the data (stream to CLI, store, discard).

The stream carries interleaved stdout/stderr as framed chunks from the guest's output session.

---

## Message Framing

The handshake uses its own message types (exchanged before normal command/event flow). All subsequent messages are tagged enums (Rust `enum`) serialized as length-prefixed Cap'n Proto on the yamux control stream:

```
// Handshake (exchanged first, in order)
WorkerHello { ... }        // worker → orchestrator
WorkerAccepted { ... }     // orchestrator → worker
WorkerReady { ... }        // worker → orchestrator

// Normal operation (after handshake)
WorkerCommand = CreateNamespace { ... }
             | DestroyNamespace { ... }
             | RegistrySync { ... }
             | RegistryUpdate { ... }
             | CreateService { ... }
             | UpdateServiceBackend { ... }
             | ServiceReady { ... }
             | DestroyService { ... }
             | FabricRouteSync { ... }
             | FabricRouteUpdate { ... }
             | LaunchPod { ... }
             | StopPod { ... }
             | SuspendPod { ... }
             | ResumePod { ... }
             | DeleteSnapshot { ... }
             | AddWireGuardPeer { ... }
             | RemoveWireGuardPeer { ... }
             | WorkerRegistrySync { ... }
             | TransferArtifact { ... }
             | EndpointSync { ... }
             | EndpointUpdate { ... }
             | Shutdown

WorkerEvent = NamespaceCreated { ... }
           | NamespaceFailed { ... }
           | NamespaceDestroyed { ... }
           | PodRunning { ... }
           | PodExited { ... }
           | PodFailed { ... }
           | ShuttingDown
           | PodLogStreamError { ... }
           | ServiceActivation { ... }
           | ServiceBackendNeed { ... }
           | FabricRouteMiss { ... }
           | PodSuspended { ... }
           | PodSuspendFailed { ... }
           | TunnelStatus { ... }
           | WorkerCondition { ... }
           | PoolCapacityUpdate { ... }
           | ArtifactWriteStarted { ... }
           | ArtifactWriteCommitted { ... }
           | ArtifactTransferReceived { ... }
           | TransferFailed { ... }
           | PressureUpdate { ... }
           | EndpointActivation { ... }
           | EndpointFlowStatus { ... }
```

Over the wire: `[u32 LE length][Cap'n Proto payload]` on the yamux control stream. Log streams use the same framing for the initial `LogStreamHeader`, then raw bytes.

### Transport

The protocol runs over **yamux** on any async bidirectional byte stream. This gives us:
- Multiplexed streams (control + N log streams) over a single connection
- Backpressure and flow control per stream
- No HTTP/2 dependency

In local mode, the transport is a `tokio::io::duplex` (in-process byte pipe). In distributed mode, the transport is a TCP/TLS connection. The yamux session sits on top either way — same protocol, same code paths.

Disconnect means the worker is gone. Since yamux owns the full connection lifecycle (no intermediate load balancer inserting GOAWAYs), there's no ambiguity about transport vs. session disconnects.

---

## Orchestrator Responsibilities

The orchestrator is the brain. It:

- **Accepts worker connections** and tracks available capacity (pods, memory, storage pools)
- **Owns the service registry** per namespace — authoritative source of name-to-IP mappings
- **Plans execution** — IP assignment, dependency ordering, worker placement
- **Drives pod lifecycle** — sends LaunchPod/StopPod/SuspendPod/ResumePod commands to specific workers
- **Manages suspend/resume** — decides when to suspend idle pods and resume them on activation
- **Projects registry state** — sends RegistrySync on namespace join, RegistryUpdate on changes
- **Manages inter-worker tunnels** — broadcasts the worker peer registry so workers can establish tunnels autonomously
- **Manages artifact lifecycle** — tracks snapshot placements, orchestrates cross-worker transfers
- **Monitors worker health** — tracks conditions, pressure metrics, and pool capacity
- **Handles failures** — detects worker disconnects, reschedules pods
- **Exposes a user-facing API** — the CLI talks to the orchestrator, not to workers directly

### Local Mode (compose up)

In local mode, the CLI embeds a minimal orchestrator in-process:
1. Creates a `tokio::io::duplex` pair and connects orchestrator/worker over it via yamux
2. Starts an in-process worker on one end
3. Parses compose file into a `Deployment`
4. Plans execution (IP assignment for pods and services, ordering)
5. Sends `CreateNamespace`, `RegistrySync`, `CreateService` for each service, then `LaunchPod` for each pod
6. As pods report `PodRunning`, sends `UpdateServiceBackend` + `ServiceReady` for their services
7. Accepts log streams from the worker and streams output to the terminal
8. On Ctrl-C, sends `StopPod` for each pod, `DestroyService` for each service, then `DestroyNamespace`

The orchestrator logic is trivial in this mode, but the worker sees the exact same protocol it would in distributed mode.

### Distributed Mode

The orchestrator runs as a long-lived server. Workers on different machines connect over TCP/TLS. The orchestrator:
- Schedules pods across workers based on resource availability, pressure metrics, and storage pool capacity
- Sends `CreateNamespace` to a worker the first time it places a pod from that namespace there
- Keeps all workers participating in a namespace in sync via `RegistryUpdate` and `FabricRouteUpdate`
- Broadcasts `WorkerRegistrySync` so workers establish inter-worker tunnels autonomously
- Manages suspend/resume lifecycle — suspends idle pods, transfers snapshots between workers, resumes on demand
- Detects worker failure and reschedules pods

---

## Fabric Topology

### Single Worker (local mode)

```
         Namespace "myapp"
  ┌────────────────────────────┐
  │  Fabric (L3 router)         │
  │                            │
  │  [TAP: pod-web]            │
  │  [TAP: pod-api]            │
  │  [TAP: pod-db]             │
  │  [Gateway: smoltcp+TUN]    │
  │                            │
  └────────────────────────────┘
```

### Multi-Worker (distributed mode)

```
  Worker A                           Worker B
  ┌──────────────────────┐          ┌──────────────────────┐
  │ Namespace "myapp"    │          │ Namespace "myapp"    │
  │ Fabric segment       │          │ Fabric segment       │
  │  [TAP: pod-web]      │          │  [TAP: pod-db]       │
  │  [TAP: pod-api]      │          │  [Gateway: smoltcp]  │
  │  [Gateway: smoltcp]  │          │  [Tunnel ←──────────]│←┐
  │  [Tunnel ──────────→]│──────────│→                     │ │
  └──────────────────────┘  Noise   └──────────────────────┘ │
                           tunnel                             │
                         (segment-                            │
                         multiplexed)                         │
```

Each worker runs a local fabric segment for each namespace it participates in. Tunnel ports connect segments across workers using Noise-encrypted connections, multiplexed by segment ID. The orchestrator decides which workers participate in which namespaces; workers manage tunnel connections autonomously based on the peer registry.

---

## Future Extensions

These are out of scope but the protocol is designed to accommodate them:

- **Port Forwarding**: `AddPortForward { namespace, host_port, pod_id, container_port }` — the worker binds a host port and proxies to a pod via the fabric gateway.
- **Health Checks**: `HealthCheck` in `ContainerSpec` — the worker runs health checks and reports `PodHealthy`/`PodUnhealthy` events.
- **Exec**: `ExecInPod { pod_id, container_id, command }` — run a command in a running container.
- **HTTP/2 Activation**: The `ActivatorConfig::Http2` variant is defined in the schema but not yet implemented. When built, it will provide precise scale-to-zero without timeout guessing by tracking open H2 streams.
