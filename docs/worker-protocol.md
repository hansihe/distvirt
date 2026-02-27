# Worker Protocol Design

## Overview

The worker protocol defines the interface between the **orchestrator** (the brain) and **workers** (the muscle). Workers are dumb executors — they launch pods, manage fabrics, and report events. All planning, scheduling, and state ownership lives in the orchestrator.

This protocol is transport-agnostic. The same message types flow over:
- **Unix domain socket** — local mode, CLI acting as orchestrator
- **TCP/TLS** — distributed mode, remote workers connecting to a central orchestrator

The target wire format is **Protobuf** over a bidirectional stream (gRPC or length-prefixed frames).

---

## Connection Lifecycle

Workers connect to the orchestrator, not the other way around. This means:
- No discovery problem — workers know the orchestrator address (config, AMI bake, etc.)
- NAT-friendly — workers behind NATs connect outbound
- Ephemeral workers — spin up EC2 instances with a pre-built AMI, they auto-register on boot; terminate them, orchestrator detects the disconnect
- Local mode — same flow, just `unix:///run/distvirt.sock`

```
Worker                              Orchestrator
  |                                      |
  |──── Connect (TCP/UDS) ──────────────>|
  |──── WorkerHello { ... } ───────────>|
  |<─── WorkerAccepted { worker_id } ───|
  |                                      |
  |        bidirectional stream          |
  |<──── commands ───────────────────────|
  |────── events ───────────────────────>|
  |                                      |
  |     (disconnect = worker is gone)    |
```

On disconnect, the orchestrator considers all pods on that worker lost. It may reschedule them to other workers depending on policy.

---

## Core Concepts

### Worker

A worker process running on a machine (physical or virtual). It owns local resources: CPU, memory, disk, network interfaces. A single worker can host pods from multiple namespaces.

The worker is responsible for:
- Managing the local VMM (Firecracker)
- Managing local fabric segments (one per namespace it participates in)
- Preparing container images
- Reporting pod lifecycle events

The worker is NOT responsible for:
- Deciding what to run or where
- IP/MAC assignment
- Dependency ordering
- Service discovery ownership
- Cross-worker networking decisions

### Namespace

An isolated environment with its own L2 network fabric. Different users/deployments get different namespaces. A namespace's fabric can span multiple workers — each worker runs a local fabric segment, and segments are connected via tunnel ports (future).

The orchestrator creates and destroys namespaces on workers as needed. If a worker has no pods in a namespace, the orchestrator can tear down that worker's namespace segment.

### Pod

The smallest schedulable unit. A pod is a Firecracker VM with:
- A single IP and MAC on its namespace's fabric
- One or more containers sharing the VM's network namespace
- A lifecycle managed as a unit (start, stop, suspend, resume)

For now, pods contain a single container. The protocol supports multiple from day one to avoid a redesign when sidecars, init containers, or suspend/resume arrive.

### DNS Registry

The orchestrator owns the authoritative name-to-IP mapping for each namespace. Workers hold a projected copy, kept in sync via full-state syncs and incremental deltas. The local fabric gateway uses this projection to answer DNS queries from pods.

### Fabric Routing Table

Separate from DNS, the fabric needs to know how to forward packets across workers. When a pod on Worker A sends a frame to a pod on Worker B, Worker A's fabric segment needs to know "this MAC/IP lives on Worker B, send it through the tunnel to B."

The orchestrator owns the authoritative routing table: a mapping of IP/MAC to a **destination** for each namespace. Workers hold a projected copy, kept in sync the same way as the DNS registry (full sync + deltas).

Each route entry has one of two destination types:

- **Remote worker** — the pod is live on another worker. The fabric forwards frames through the tunnel to that worker.
- **Placeholder** — the pod is not currently running (suspended, scaled-to-zero, pending). The fabric applies a buffering policy and reports a route miss to the orchestrator.

This is a single unified table. When a suspended pod gets scheduled and boots on a worker, the orchestrator simply updates the entry from placeholder → remote worker (or it becomes local on the hosting worker and the entry is removed). No coordinating across separate tables.

Pods that are local to this worker (have a TAP on the local fabric) don't need route entries — the fabric already knows about them.

When the fabric receives a frame for a destination that has no local TAP and no route entry at all, it reports a **route miss** with no buffering (unknown destination). When it hits a placeholder entry, it applies the placeholder's buffer policy and also reports a route miss. The orchestrator can then schedule the pod, update routes, and the buffered frames get delivered.

**Placeholder buffer policies** control what happens to traffic while the pod is activating:
- **Hold TCP SYN** — the smoltcp gateway holds the TCP connection while the pod boots. From the sender's perspective, it's just a slow connection.
- **Buffer frames** — queue up to N frames for up to M milliseconds, then drop.
- **Drop** — discard immediately (still report the miss so the orchestrator can react).

This is the foundation for transparent activation — the sending pod doesn't know or care that the target was suspended and is now booting.

In local mode (single worker), the routing table is typically empty — all pods are local. But the protocol supports it from day one so multi-worker doesn't require a redesign.

---

## Messages: Orchestrator to Worker (Commands)

### Worker Registration

```protobuf
message WorkerHello {
  string version = 1;
  WorkerResources resources = 2;
}

message WorkerResources {
  uint32 cpu_cores = 1;
  uint64 memory_bytes = 2;
  uint64 disk_bytes = 3;
}

message WorkerAccepted {
  string worker_id = 1;
}
```

### Namespace Lifecycle

```protobuf
message CreateNamespace {
  string namespace_id = 1;
  NetworkConfig network = 2;
}

message NetworkConfig {
  string subnet = 1;        // e.g. "172.16.0.0/24"
  string gateway_ip = 2;    // e.g. "172.16.0.1"
  bytes gateway_mac = 3;    // 6 bytes
}

message DestroyNamespace {
  string namespace_id = 1;
}
```

`CreateNamespace` tells the worker to stand up a local fabric segment: create the L2 switch, create the smoltcp gateway at the specified IP/MAC, set up TUN egress. The worker is ready to accept pods on this namespace after acknowledging.

`DestroyNamespace` tears down all pods in the namespace on this worker, then tears down the fabric segment.

### DNS Registry Sync

```protobuf
message RegistrySync {
  string namespace_id = 1;
  repeated RegistryEntry entries = 2;
}

message RegistryUpdate {
  string namespace_id = 1;
  repeated RegistryEntry added = 2;
  repeated string removed = 3;
}

message RegistryEntry {
  string name = 1;
  string ip = 2;
}
```

`RegistrySync` is a full-state replacement — the worker discards its local registry for this namespace and adopts the provided entries. Sent when the worker first joins a namespace, or when the orchestrator wants to force reconciliation.

`RegistryUpdate` is an incremental delta. The worker applies additions and removals to its local registry.

The gateway's DNS server queries this local registry. Names not found are forwarded to upstream DNS (for external resolution).

### Fabric Routing

```protobuf
message FabricRouteSync {
  string namespace_id = 1;
  repeated FabricRouteEntry routes = 2;
}

message FabricRouteUpdate {
  string namespace_id = 1;
  repeated FabricRouteEntry added = 2;
  repeated string removed_ips = 3;
}

message FabricRouteEntry {
  string ip = 1;
  bytes mac = 2;              // 6 bytes

  oneof destination {
    RemoteWorker remote = 3;
    Placeholder placeholder = 4;
  }
}

message RemoteWorker {
  string worker_id = 1;
}

message Placeholder {
  BufferPolicy buffer_policy = 1;
}

message BufferPolicy {
  bool hold_tcp_syn = 1;     // hold TCP SYN + buffer connection (smoltcp gateway)
  uint32 buffer_frames = 2;  // max frames to buffer (0 = drop immediately)
  uint32 timeout_ms = 3;     // how long to buffer before giving up
}
```

`FabricRouteSync` is a full-state replacement of the routing table for a namespace on this worker. Sent when the worker joins a namespace.

`FabricRouteUpdate` is an incremental delta. When a new pod launches on Worker B, the orchestrator sends a route update to Worker A so it knows how to forward frames. When a pod is suspended, the orchestrator updates the entry from `RemoteWorker` to `Placeholder` with an appropriate buffer policy.

Routes for pods that are local to this worker don't need entries — the fabric already knows about them via the local TAP port.

### Pod Lifecycle

```protobuf
message LaunchPod {
  string namespace_id = 1;
  string pod_id = 2;
  PodNetworkConfig network = 3;
  repeated ContainerSpec containers = 4;
}

message PodNetworkConfig {
  string ip = 1;
  bytes mac = 2;             // 6 bytes
}

message ContainerSpec {
  string container_id = 1;
  string image = 2;          // image reference (e.g. "docker.io/library/nginx:latest")
  repeated string entrypoint = 3;
  repeated string args = 4;
  map<string, string> env = 5;
  string working_dir = 6;
  string user = 7;           // "uid" or "uid:gid"
  string hostname = 8;
  bool capture_output = 9;
}

message StopPod {
  string namespace_id = 1;
  string pod_id = 2;
  bool graceful = 3;         // true = send shutdown, wait; false = kill immediately
}
```

`LaunchPod` tells the worker to:
1. Prepare container images (pull if needed)
2. Launch a Firecracker VM with the specified network config
3. Attach the VM's TAP to the namespace's fabric
4. Configure guest networking (IP, gateway, DNS)
5. Add and start containers in order
6. Report `PodRunning` when all containers are started

`StopPod` tells the worker to shut down the pod. Graceful sends a shutdown command to the guest and waits; non-graceful kills the VM process.

---

## Messages: Worker to Orchestrator (Events)

```protobuf
message PodRunning {
  string namespace_id = 1;
  string pod_id = 2;
}

message PodExited {
  string namespace_id = 1;
  string pod_id = 2;
  int32 exit_code = 3;       // exit code of the "main" container
}

message PodFailed {
  string namespace_id = 1;
  string pod_id = 2;
  string error = 3;
}

message PodOutput {
  string namespace_id = 1;
  string pod_id = 2;
  string container_id = 3;
  OutputStream stream = 4;
  bytes data = 5;
}

enum OutputStream {
  STDOUT = 0;
  STDERR = 1;
}
```

`PodRunning` — the VM is booted, all containers are started, the pod is on the fabric.

`PodExited` — the main container exited. The exit code is from the main container (first in the containers list). The VM may still be running if there are other containers; pod exit policy is a future concern.

`PodFailed` — the pod could not start (image pull failed, VM failed to boot, etc.). The worker has cleaned up any partial state.

`PodOutput` — stdout/stderr from a container, if `capture_output` was set. The orchestrator decides what to do with it (stream to CLI, store, discard).

### Fabric Events

```protobuf
message FabricRouteMiss {
  string namespace_id = 1;
  string dst_ip = 2;
  bytes dst_mac = 3;
}
```

`FabricRouteMiss` — the worker's fabric received a frame for a destination it can't deliver locally. This fires for both unknown destinations (no route entry at all) and placeholders (route entry exists but destination is a `Placeholder`). For placeholders, the fabric applies the buffer policy before reporting the miss. The orchestrator can respond by scheduling a suspended pod, updating the route entry from placeholder to remote worker, etc.

---

## Message Framing

All messages are wrapped in a tagged union:

```protobuf
message WorkerCommand {
  oneof command {
    // Namespace lifecycle
    CreateNamespace create_namespace = 1;
    DestroyNamespace destroy_namespace = 2;

    // DNS registry
    RegistrySync registry_sync = 3;
    RegistryUpdate registry_update = 4;

    // Fabric routing (includes both live routes and placeholders)
    FabricRouteSync fabric_route_sync = 5;
    FabricRouteUpdate fabric_route_update = 6;

    // Pod lifecycle
    LaunchPod launch_pod = 7;
    StopPod stop_pod = 8;
  }
}

message WorkerEvent {
  oneof event {
    // Pod lifecycle
    PodRunning pod_running = 1;
    PodExited pod_exited = 2;
    PodFailed pod_failed = 3;
    PodOutput pod_output = 4;

    // Fabric
    FabricRouteMiss fabric_route_miss = 5;
  }
}
```

Over the wire: length-prefixed protobuf frames on a bidirectional stream.

**gRPC consideration:** gRPC bidirectional streaming is attractive (ecosystem, codegen, TLS), but some gRPC client/server implementations periodically close and re-establish transport connections (e.g., `GOAWAY` frames, max connection age settings, load balancer idle timeouts). Since our protocol is stateful — the worker holds namespaces, running pods, fabric state — a transport disconnect must NOT be interpreted as "worker is gone, reschedule everything."

Options:
1. **Application-level session over gRPC** — the worker identifies itself with a stable `worker_id` on reconnect. The orchestrator matches it to existing state. The gRPC stream is just a transport; the session survives reconnects.
2. **Raw framing over TCP/UDS** — simpler, no HTTP/2 dependency, full control over connection lifecycle. Disconnect genuinely means the worker is gone (or the network is). No ambiguity.
3. **Hybrid** — use gRPC for the initial handshake and RPCs (health checks, resource reporting), but a raw TCP/UDS stream for the command/event channel.

For milestone 1 (local, in-process), this is moot — we use channels. The transport decision can be deferred, but the protocol messages are designed to be transport-agnostic either way.

---

## Orchestrator Responsibilities

The orchestrator is the brain. It:

- **Accepts worker connections** and tracks available capacity
- **Owns the service registry** per namespace — authoritative source of name-to-IP mappings
- **Plans execution** — IP/MAC assignment, dependency ordering, worker placement
- **Drives pod lifecycle** — sends LaunchPod/StopPod commands to specific workers
- **Projects registry state** — sends RegistrySync on namespace join, RegistryUpdate on changes
- **Handles failures** — detects worker disconnects, reschedules pods
- **Exposes a user-facing API** — the CLI talks to the orchestrator, not to workers directly

### Local Mode (compose up)

In local mode, the CLI embeds a minimal orchestrator in-process:
1. Starts listening on a Unix domain socket (or uses in-process channels)
2. Starts an in-process worker that connects
3. Parses compose file into a `Deployment`
4. Plans execution (IP assignment, ordering)
5. Sends `CreateNamespace`, `RegistrySync`, then `LaunchPod` for each service
6. Streams `PodOutput` events to the terminal
7. On Ctrl-C, sends `StopPod` for each pod, then `DestroyNamespace`

The orchestrator logic is trivial in this mode, but the worker sees the exact same protocol it would in distributed mode.

### Distributed Mode (future)

The orchestrator runs as a long-lived server. Workers on different machines connect over TCP/TLS. The orchestrator:
- Schedules pods across workers based on resource availability
- Sends `CreateNamespace` to a worker the first time it places a pod from that namespace there
- Keeps all workers participating in a namespace in sync via `RegistryUpdate`
- Detects worker failure and reschedules pods

---

## Fabric Topology

### Single Worker (local mode)

```
         Namespace "myapp"
  ┌────────────────────────────┐
  │  Fabric (L2 switch)        │
  │                            │
  │  [TAP: pod-web]            │
  │  [TAP: pod-api]            │
  │  [TAP: pod-db]             │
  │  [Gateway: smoltcp+TUN]    │
  │                            │
  └────────────────────────────┘
```

### Multi-Worker (distributed mode, future)

```
  Worker A                           Worker B
  ┌──────────────────────┐          ┌──────────────────────┐
  │ Namespace "myapp"    │          │ Namespace "myapp"    │
  │ Fabric segment       │          │ Fabric segment       │
  │  [TAP: pod-web]      │          │  [TAP: pod-db]       │
  │  [TAP: pod-api]      │          │  [Gateway: smoltcp]  │
  │  [Gateway: smoltcp]  │          │  [Tunnel ←──────────]│←┐
  │  [Tunnel ──────────→]│──────────│→                     │ │
  └──────────────────────┘  tunnel  └──────────────────────┘ │
                             port                             │
                            (future)                          │
```

Each worker runs a local fabric segment for each namespace it participates in. In distributed mode, tunnel ports connect segments across workers. The orchestrator decides which workers participate in which namespaces.

---

## Future Extensions

These are out of scope but the protocol is designed to accommodate them:

- **Suspend/Resume**: `SuspendPod { pod_id }` / `ResumePod { pod_id }` — snapshot VM state, resume later. Multiple containers in a pod suspend/resume together.
- **Port Forwarding**: `AddPortForward { namespace, host_port, pod_id, container_port }` — the worker binds a host port and proxies to a pod via the fabric gateway.
- **Resource Limits**: `PodResources { vcpus, memory_mib }` in `LaunchPod` — the worker configures the VM accordingly.
- **Health Checks**: `HealthCheck` in `ContainerSpec` — the worker runs health checks and reports `PodHealthy`/`PodUnhealthy` events.
- **Exec**: `ExecInPod { pod_id, container_id, command }` — run a command in a running container.
- **Tunnel Management**: `ConnectFabric { namespace, peer_worker, tunnel_config }` — orchestrator tells workers to establish tunnel ports between each other.
- **Autoscaling / Scale-to-Zero**: orchestrator-level concerns that don't change the worker protocol — the orchestrator just sends LaunchPod/StopPod/SuspendPod/ResumePod as needed.
