# Worker Protocol Design

## Overview

The worker protocol defines the interface between the **orchestrator** (the brain) and **workers** (the muscle). Workers are dumb executors — they launch pods, manage fabrics, and report events. All planning, scheduling, and state ownership lives in the orchestrator.

This protocol is transport-agnostic. The same message types flow over:
- **Unix domain socket** — local mode, CLI acting as orchestrator
- **TCP/TLS** — distributed mode, remote workers connecting to a central orchestrator

The transport is a **yamux**-multiplexed bidirectional stream. The primary control stream carries length-prefixed JSON messages (commands and events). Additional yamux streams carry out-of-band data like container log output. The wire format may move to Protobuf in the future, but the message semantics are format-agnostic.

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
  |──── Establish yamux session ────────>|
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

**Worker registration** (future): For distributed mode, the first message on the control stream will be a `WorkerHello`/`WorkerAccepted` handshake to assign a stable `worker_id`. In local mode (single in-process worker), this is skipped.

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
}

DestroyNamespace {
  namespace_id: String,
}
```

`CreateNamespace` tells the worker to stand up a local fabric segment: create the L2 switch, create the smoltcp gateway at the specified IP, set up TUN egress. The gateway MAC is a fixed locally-administered address (`02:00:00:00:00:01`). The worker acknowledges with a `NamespaceCreated` event.

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

### Fabric Routing

```
FabricRouteSync {
  namespace_id: String,
  routes: Vec<FabricRouteEntry>,
}

FabricRouteUpdate {
  namespace_id: String,
  added: Vec<FabricRouteEntry>,
  removed_ips: Vec<String>,
}

FabricRouteEntry {
  ip: Ipv4Addr,
  mac: [u8; 6],
  destination: RouteDestination,
}

enum RouteDestination {
  RemoteWorker { worker_id: String },
  Placeholder { buffer_policy: BufferPolicy },
}

BufferPolicy {
  hold_tcp_syn: bool,       // hold TCP SYN + buffer connection (smoltcp gateway)
  buffer_frames: u32,       // max frames to buffer (0 = drop immediately)
  timeout_ms: u32,          // how long to buffer before giving up
}
```

`FabricRouteSync` is a full-state replacement of the routing table for a namespace on this worker. Sent when the worker joins a namespace.

`FabricRouteUpdate` is an incremental delta. When a new pod launches on Worker B, the orchestrator sends a route update to Worker A so it knows how to forward frames. When a pod is suspended, the orchestrator updates the entry from `RemoteWorker` to `Placeholder` with an appropriate buffer policy.

Routes for pods that are local to this worker don't need entries — the fabric already knows about them via the local TAP port.

### Pod Lifecycle

```
LaunchPod {
  namespace_id: String,
  pod_id: String,
  network: PodNetworkConfig,
  containers: Vec<ContainerSpec>,
}

PodNetworkConfig {
  ip: Ipv4Addr,
  mac: [u8; 6],
  gateway: Ipv4Addr,         // gateway IP for the pod's network config
  netmask: String,            // e.g. "255.255.255.0"
}

ContainerSpec {
  container_id: String,
  image_ref: String,          // image reference (e.g. "docker.io/library/nginx:latest")
  config: ContainerConfig,
}

ContainerConfig {
  entrypoint: String,         // main executable
  args: Vec<String>,
  env: Vec<String>,           // KEY=VALUE format (OCI convention)
  working_dir: Option<String>,
  uid: Option<u32>,
  gid: Option<u32>,
  hostname: Option<String>,
  capture_output: bool,
}

StopPod {
  namespace_id: String,
  pod_id: String,
  graceful: bool,             // true = send shutdown, wait; false = kill immediately
}

Shutdown {}
```

`PodNetworkConfig` includes the full network configuration the pod needs to configure its guest interface. The orchestrator derives these from the namespace's `NetworkConfig` and the pod's assigned IP/MAC.

When a `ContainerSpec` references an OCI image, the worker parses the image's config (entrypoint, cmd, env, working_dir, user) and merges it with the `ContainerConfig` overrides. Explicit overrides take precedence; empty/None fields fall through to the image defaults.

`LaunchPod` tells the worker to:
1. Prepare container images (pull if needed, parse OCI config)
2. Merge OCI image config with provided overrides
3. Launch a Firecracker VM with the specified network config
4. Attach the VM's TAP to the namespace's fabric
5. Configure guest networking (IP, gateway, DNS pointing at fabric gateway)
6. Add and start containers
7. Report `PodRunning` when all containers are started

`StopPod` tells the worker to shut down the pod. Graceful cancels the pod's token, triggering a graceful VM shutdown with a timeout before force-killing. Non-graceful aborts the pod supervisor immediately (VM process killed via Drop).

`Shutdown` tells the worker to shut down entirely. The worker acknowledges with `ShuttingDown`, cancels all namespaces and pods, awaits cleanup, then exits.

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

`PodRunning` — the VM is booted, all containers are started, the pod is on the fabric.

`PodExited` — the main container exited. The exit code is from the main container (first in the containers list). The VM may still be running if there are other containers; pod exit policy is a future concern.

`PodFailed` — the pod could not start (image pull failed, VM failed to boot, etc.). The worker has cleaned up any partial state.

`ShuttingDown` — acknowledges a `Shutdown` command. The worker is tearing down.

`PodLogStreamError` — a non-fatal error occurred while setting up or streaming container logs. The pod continues running; only log delivery is affected.

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

### Fabric Events

```
FabricRouteMiss {
  namespace_id: String,
  dst_ip: Ipv4Addr,
  dst_mac: [u8; 6],
}
```

`FabricRouteMiss` — the worker's fabric received a frame for a destination it can't deliver locally. This fires for both unknown destinations (no route entry at all) and placeholders (route entry exists but destination is a `Placeholder`). For placeholders, the fabric applies the buffer policy before reporting the miss. The orchestrator can respond by scheduling a suspended pod, updating the route entry from placeholder to remote worker, etc.

---

## Message Framing

All commands and events are tagged enums (Rust `enum`) serialized as length-prefixed JSON on the yamux control stream:

```
WorkerCommand = CreateNamespace { ... }
             | DestroyNamespace { ... }
             | RegistrySync { ... }
             | RegistryUpdate { ... }
             | FabricRouteSync { ... }
             | FabricRouteUpdate { ... }
             | LaunchPod { ... }
             | StopPod { ... }
             | Shutdown

WorkerEvent = NamespaceCreated { ... }
           | NamespaceFailed { ... }
           | PodRunning { ... }
           | PodExited { ... }
           | PodFailed { ... }
           | ShuttingDown
           | PodLogStreamError { ... }
           | FabricRouteMiss { ... }
```

Over the wire: `[u32 LE length][JSON payload]` on the yamux control stream. Log streams use the same framing for the initial `LogStreamHeader`, then raw bytes.

### Transport

The protocol runs over **yamux** on any async bidirectional byte stream. This gives us:
- Multiplexed streams (control + N log streams) over a single connection
- Backpressure and flow control per stream
- No HTTP/2 dependency

In local mode, the transport is a `tokio::io::duplex` (in-process byte pipe). In distributed mode (future), the transport is a TCP/TLS connection. The yamux session sits on top either way — same protocol, same code paths.

Disconnect means the worker is gone. Since yamux owns the full connection lifecycle (no intermediate load balancer inserting GOAWAYs), there's no ambiguity about transport vs. session disconnects.

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
1. Creates a `tokio::io::duplex` pair and connects orchestrator/worker over it via yamux
2. Starts an in-process worker on one end
3. Parses compose file into a `Deployment`
4. Plans execution (IP assignment, ordering)
5. Sends `CreateNamespace`, `RegistrySync`, then `LaunchPod` for each service
6. Accepts log streams from the worker and streams output to the terminal
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
