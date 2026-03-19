---
title: "Client Protocol"
---

## Overview

The client protocol defines the interface between **clients** (CLI, UI, automation tools) and the **orchestrator**. It uses **gRPC** over HTTP/2, providing strong typing via protobuf, natural request-response correlation, and built-in streaming for subscriptions.

This is a control plane protocol -- all operations are management commands and status queries. There is no data plane traffic on this interface.

The protocol has two primary entities:

- **Workloads**: the spec for a schedulable unit. A workload describes what a pod should look like (image, containers, config). When scheduled, it becomes a **pod** -- a microVM with its own IP on the fabric. A pod can host multiple containers.
- **Services**: network entities with their own IP on the fabric, with a programmable activation layer. Each service points at a workload via `workload_id`. For L3 traffic, the service NATs to the backing pod's IP. The activation mechanism is what enables scale-to-zero -- the service entity exists on the fabric and can intercept traffic even when no pod is running.

Both workloads and services are top-level in the spec and status. At runtime, both pods (the workload's runtime instance) and services have their own IP on the fabric. The service->workload binding is mutable: retargeting a service from one workload to another is a valid operation (e.g. blue/green, canary, splice). The CLI reassembles the workload-grouped view (`dv status` shows services nested under workloads) from the flat data.

### Why gRPC

- **Request-response for free**: Each RPC is its own HTTP/2 stream. No need for request ID correlation.
- **Streaming built-in**: Server-streaming RPCs for log tailing, event streaming, and status watches, with HTTP/2 flow control handling backpressure.
- **Strong typing**: Protobuf defines the exact shape of every message, including workload and service state variants. Code generation produces idiomatic types in every target language.
- **Language-agnostic**: Future CLIs, web UIs, or third-party integrations can codegen from the same `.proto` files.
- **Consistency**: The worker protocol will migrate to gRPC in the future. Using gRPC for the client protocol first lets us build out the infrastructure (proto definitions, codegen, server setup) incrementally.

---

## Transport

Clients connect to the orchestrator's gRPC server over TCP. TLS is required in distributed mode, optional in local mode (unix socket).

```
Client                               Orchestrator
  |                                       |
  |---- gRPC connect (TCP/UDS) --------->|
  |                                       |
  |  Unary RPCs (create, delete, etc.)   |
  |---- request ----------------------->|
  |<---- response ----------------------|
  |                                       |
  |  Server-streaming RPCs (watch, logs) |
  |---- request ----------------------->|
  |<---- stream of responses ------------|
  |<---- stream of responses ------------|
  |<---- ... ---------------------------|
  |                                       |
```

Most RPCs are either unary (one request, one response) or server-streaming (one request, many responses). The exception is `AttachWorkload`, which uses bidirectional streaming to forward stdin/stdout/stderr between the client and a running workload's entrypoint process.

### Connection Lifecycle

- Clients connect on demand. There is no persistent session registration -- each RPC is independent.
- Server-streaming RPCs (watch, logs, events) maintain a long-lived connection. The orchestrator tracks active subscriptions internally and cleans up when the stream is cancelled or the connection drops.
- Clients can open multiple concurrent RPCs on the same connection (HTTP/2 multiplexing).
- Authentication: API key tokens in gRPC metadata (see Authentication section below).

---

## Authentication

The client sends an API key as a bearer token in the `authorization` gRPC metadata header via a tonic interceptor on the client side.

**Current status: server-side validation is NOT implemented.** The orchestrator accepts all requests regardless of token. The client-side interceptor attaches the token, but the server has no corresponding interceptor to check it. This means authentication is effectively a no-op right now.

### Token model (client-side)

- Tokens are **opaque API keys** -- random strings stored client-side.
- All tokens are **global scope** -- when server-side validation is added, a valid token would grant access to all namespaces and operations.
- Tokens are managed out-of-band (CLI command, config file, env var). There is no token-management RPC in this protocol.

### Wire format

The client sets the `authorization` metadata key on every RPC (unary and streaming):

```
authorization: Bearer <token>
```

The token is optional -- if not configured, the client omits the header entirely and RPCs still succeed (since the server does not validate).

### Client implementation

The CLI resolves connection parameters with this precedence:

1. CLI flags (`--server`, `--token`)
2. Environment variables (`DV_SERVER`, `DV_TOKEN`)
3. Active context from credentials file (`~/.config/distvirt/credentials`)
4. Default server: `http://[::1]:9090`

A `tonic::service::Interceptor` on the client channel attaches the `Bearer <token>` header to every outgoing request if a token is configured.

### Server implementation

The gRPC server is constructed with `DistvirtClientServer::new(svc)` -- no interceptor. Adding server-side auth would mean wrapping this with `DistvirtClientServer::with_interceptor(svc, check_auth)`.

### Transport security

TLS (server-side) is **required** when exposed over the internet -- tokens are sent in the clear without it. In local mode (unix socket), TLS is optional.

---

## Service Definition

```protobuf
syntax = "proto3";
package distvirt.client.v1;

service DistvirtClient {
    // --- Namespace lifecycle ---
    rpc CreateNamespace(CreateNamespaceRequest) returns (CreateNamespaceResponse);
    rpc UpdateNamespace(UpdateNamespaceRequest) returns (UpdateNamespaceResponse);
    rpc DeleteNamespace(DeleteNamespaceRequest) returns (DeleteNamespaceResponse);
    rpc GetNamespaceStatus(GetNamespaceStatusRequest) returns (GetNamespaceStatusResponse);
    rpc ListNamespaces(ListNamespacesRequest) returns (ListNamespacesResponse);

    // --- Splice ---
    rpc Splice(SpliceRequest) returns (SpliceResponse);
    rpc Unsplice(UnspliceRequest) returns (UnspliceResponse);

    // --- Workload lifecycle hints ---
    rpc DeactivateWorkload(DeactivateWorkloadRequest) returns (DeactivateWorkloadResponse);

    // --- Cloning ---
    rpc CloneNamespace(CloneNamespaceRequest) returns (CloneNamespaceResponse);

    // --- Layer 2: Resource queries ---
    rpc ListWorkers(ListWorkersRequest) returns (ListWorkersResponse);
    rpc GetWorker(GetWorkerRequest) returns (GetWorkerResponse);
    rpc ListPods(ListPodsRequest) returns (ListPodsResponse);

    // --- Developer network access ---
    rpc ConnectNetwork(ConnectNetworkRequest) returns (ConnectNetworkResponse);
    rpc DisconnectNetwork(DisconnectNetworkRequest) returns (DisconnectNetworkResponse);

    // --- Workload I/O ---
    rpc AttachWorkload(stream AttachWorkloadInput) returns (stream AttachWorkloadOutput);

    // --- Streaming subscriptions ---
    rpc WatchNamespaceStatus(WatchNamespaceStatusRequest) returns (stream NamespaceStatusEvent);
    rpc StreamLogs(StreamLogsRequest) returns (stream LogChunk);
    rpc StreamEvents(StreamEventsRequest) returns (stream NamespaceEvent);
}
```

### Unary RPCs

Each unary RPC gets a single response. Errors use standard gRPC status codes (NOT_FOUND, ALREADY_EXISTS, INVALID_ARGUMENT, etc.) with descriptive messages in the status detail. Error mapping is done by inspecting the error message text for keywords like "not found", "already exists", "not spliced", "already spliced".

### Server-Streaming RPCs

> **Note:** `StreamLogs` and `StreamEvents` are implemented. `WatchNamespaceStatus` is defined in the proto but returns `UNIMPLEMENTED` -- it is a stub.

- **WatchNamespaceStatus**: Intended to push a `NamespaceStatusEvent` whenever any workload or service state changes within the namespace. **Not yet implemented** -- currently returns `Status::unimplemented`.
- **StreamLogs**: Pushes log chunks as they arrive. The client specifies an optional `workload_id` filter.
- **StreamEvents**: Pushes discrete, typed events describing state transitions -- activation triggers, pod launches, idle timeouts, suspend/resume, etc. Supports filtering by repeated `workload_ids` and `service_ids` arrays. This is what `dv events` displays.

All streaming RPCs are cancellable -- the client can cancel the stream at any time. The orchestrator detects cancellation and cleans up the subscription.

---

## Messages

### Namespace Spec

The `NamespaceSpec` is the declarative description of what should exist. Clients construct this (from compose files, k8s manifests, or directly) and send it to the orchestrator.

Workloads and services are both **top-level** in the spec. Both have network identity on the fabric (IP, MAC). A workload describes what to run; when scheduled, it becomes a pod (a microVM). A service is a network entity that NATs to its backing pod. Each service references a workload via `workload_id`. This binding is mutable -- changing a service's `workload_id` retargets it to a different workload's pod.

A common simple case (one service per workload, same name) is what compose frontends produce. But the model supports many-to-one: multiple services backed by the same workload, and retargeting.

```protobuf
message NamespaceSpec {
    NetworkConfig network = 1;
    map<string, WorkloadSpec> workloads = 2;
    map<string, ServiceSpec> services = 3;
}

message WorkloadSpec {
    PodNetworkConfig network = 1;
    repeated ContainerSpec containers = 2;
    bool suspend_on_idle = 3;           // enable suspend/resume instead of stop on idle
    ResourceRequirements resources = 4;  // resource requests and limits for the pod
}

message ResourceValues {
    uint64 memory_mb = 1;
    uint32 vcpus = 2;
}

message ResourceRequirements {
    ResourceValues requests = 1;   // minimum resources to schedule
    ResourceValues limits = 2;     // maximum resources allowed
}

message ContainerSpec {
    string name = 1;
    string image = 2;
    ContainerConfig config = 3;
}

message PodNetworkConfig {
    string ip = 1;       // assigned by orchestrator
    string mac = 2;      // assigned by orchestrator
}

message ServiceSpec {
    string workload_id = 1;             // which workload backs this service
    ServiceNetworkConfig network = 2;   // ip, mac -- assigned by orchestrator
    ActivationSpec activation = 3;      // absent = always-on
    repeated ExposeSpec expose = 4;
}

message ActivationSpec {
    ActivatorConfig activator = 1;
    ServicePolicy buffer_policy = 2;
    uint64 idle_timeout_ms = 3;
}

message ActivatorConfig {
    oneof activator {
        TcpActivator tcp = 1;
        Http2Activator http2 = 2;
    }
}

message TcpActivator {
    repeated uint32 ports = 1;
}

message Http2Activator {}

message ContainerConfig {
    repeated string entrypoint = 1;
    repeated string args = 2;
    map<string, string> env = 3;
    // Fields 4 (memory_mb) and 5 (vcpus) are reserved --
    // these moved to ResourceRequirements on WorkloadSpec.
    string working_dir = 6;
    string user = 7;
    string hostname = 8;
    bool tty = 9;              // allocate a PTY for the entrypoint at launch
}

message ServiceNetworkConfig {
    string ip = 1;       // assigned by orchestrator
    string mac = 2;      // assigned by orchestrator
}

message NetworkConfig {
    string subnet = 1;   // e.g. "10.0.0.0/24"
}

message ExposeSpec {
    uint32 container_port = 1;
    uint32 host_port = 2;
    ExposeProtocol protocol = 3;
}

enum ExposeProtocol {
    EXPOSE_PROTOCOL_UNSPECIFIED = 0;
    EXPOSE_PROTOCOL_TCP = 1;
    EXPOSE_PROTOCOL_UDP = 2;
}

message ServicePolicy {
    // Buffer policy configuration for activation.
}
```

### Namespace Lifecycle

```protobuf
message CreateNamespaceRequest {
    string namespace_id = 1;
    NamespaceSpec spec = 2;
}
message CreateNamespaceResponse {}

message UpdateNamespaceRequest {
    string namespace_id = 1;
    NamespaceSpec spec = 2;
}
message UpdateNamespaceResponse {}

message DeleteNamespaceRequest {
    string namespace_id = 1;
}
message DeleteNamespaceResponse {}

message GetNamespaceStatusRequest {
    string namespace_id = 1;
}
message GetNamespaceStatusResponse {
    NamespaceStatusReport status = 1;
}

message ListNamespacesRequest {}
message ListNamespacesResponse {
    repeated NamespaceStatusReport namespaces = 1;
}
```

### Status Reporting

Status mirrors the spec: workloads and services are both top-level. Each service status carries its `workload_id` so the CLI can group them for display. Workload state and service state are both strongly-typed oneofs.

```protobuf
message NamespaceStatusReport {
    string namespace_id = 1;
    NamespaceState state = 2;
    map<string, WorkloadStatusReport> workloads = 3;
    map<string, ServiceStatusReport> services = 4;
}

enum NamespaceState {
    NAMESPACE_STATE_UNSPECIFIED = 0;
    NAMESPACE_STATE_CREATING = 1;
    NAMESPACE_STATE_ACTIVE = 2;
    NAMESPACE_STATE_DESTROYING = 3;
}

message WorkloadStatusReport {
    WorkloadState state = 1;
    bool spliced = 2;
}

message WorkloadState {
    oneof state {
        WorkloadDormant dormant = 1;
        WorkloadWaitingForCapacity waiting_for_capacity = 2;
        WorkloadLaunching launching = 3;
        WorkloadRunning running = 4;
    }
}

message WorkloadDormant {}
message WorkloadWaitingForCapacity {}

message WorkloadLaunching {
    string pod_id = 1;
    string worker_id = 2;
}

message WorkloadRunning {
    string pod_id = 1;
    string worker_id = 2;
}

message ServiceStatusReport {
    string workload_id = 1;
    ServiceState state = 2;
    bool activation_enabled = 3;
    bool spliced = 4;
    string ip = 5;
    string mac = 6;
}

message ServiceState {
    oneof state {
        ServicePending pending = 1;
        ServiceIdle idle = 2;
        ServiceNeedBackend need_backend = 3;
        ServiceActive active = 4;
    }
}

message ServicePending {}
message ServiceIdle {}
message ServiceNeedBackend {}

message ServiceActive {
    string pod_id = 1;
    string worker_id = 2;
    BackendNeed backend_need = 3;
}

enum BackendNeed {
    BACKEND_NEED_UNSPECIFIED = 0;
    BACKEND_NEED_NONE = 1;
    BACKEND_NEED_TRAFFIC = 2;
    BACKEND_NEED_ACTIVE = 3;
}
```

The CLI groups services by `workload_id` to produce the workload-centric display:

```
WORKLOAD     STATE     POD        WORKER        SERVICES
api          running   pod-3a1f   worker-east   grpc(active) graphql(idle) health(active)
```

This is a presentation concern -- the protocol provides flat data, the CLI joins on `workload_id`.

### Splice

Splice operates at the **workload level**. Moving the workload's pod automatically affects all services currently targeting that workload. (Retargeting individual services to different workloads is a separate operation via `UpdateNamespace`.)

```protobuf
message SpliceRequest {
    string namespace_id = 1;
    string workload_id = 2;
    string local_worker_id = 3;
}
message SpliceResponse {}

message UnspliceRequest {
    string namespace_id = 1;
    string workload_id = 2;
}
message UnspliceResponse {}
```

### DeactivateWorkload

Explicitly deactivate a workload's pod. Returns whether deactivation occurred and a human-readable reason if it did not (e.g. the workload is already dormant, or it has active services that prevent deactivation).

```protobuf
message DeactivateWorkloadRequest {
    string namespace_id = 1;
    string workload_id = 2;
}

message DeactivateWorkloadResponse {
    bool deactivated = 1;       // true if workload was deactivated
    string reason = 2;          // human-readable explanation when not deactivated
}
```

### ConnectNetwork / DisconnectNetwork (Developer Network Access)

These RPCs allow a developer's CLI to join the namespace's network fabric via WireGuard. The client provides its X25519 public key; the server responds with the WireGuard endpoint details needed to configure a local tunnel.

```protobuf
message ConnectNetworkRequest {
    string namespace_id = 1;
    bytes client_public_key = 2;   // 32-byte X25519 public key from CLI
}

message ConnectNetworkResponse {
    bytes server_public_key = 1;   // 32-byte X25519 public key of worker's WG adapter
    string endpoint = 2;           // "host:port" (worker public IP + WG listen port)
    string client_ip = 3;          // IP assigned to client on namespace subnet
    string subnet = 4;             // Namespace subnet CIDR (becomes AllowedIPs)
}

message DisconnectNetworkRequest {
    string namespace_id = 1;
    bytes client_public_key = 2;
}
message DisconnectNetworkResponse {}
```

The server validates that `client_public_key` is exactly 32 bytes, returning `INVALID_ARGUMENT` otherwise.

### Cloning

```protobuf
message CloneNamespaceRequest {
    string source_namespace_id = 1;
    string target_namespace_id = 2;
    NamespaceOverrides overrides = 3;
}
message CloneNamespaceResponse {}

message NamespaceOverrides {
    // Per-workload overrides. Key is workload_id.
    // Only specified fields are overridden; absent entries
    // inherit the source spec unchanged.
    map<string, WorkloadOverrides> workloads = 1;
    // Per-service overrides. Key is service_id.
    map<string, ServiceOverrides> services = 2;
}

message WorkloadOverrides {
    optional string image = 1;
    map<string, string> env_overrides = 2;  // merged into source env
}

message ServiceOverrides {
    optional string workload_id = 1;  // retarget to a different workload
}
```

Note: the server implementation currently ignores `overrides` -- it clones the source namespace as-is.

### AttachWorkload (Bidirectional Streaming)

Attaches to the stdin/stdout/stderr of a running workload's entrypoint process. This is the only bidirectional streaming RPC in the protocol.

Whether the entrypoint runs with a PTY is determined at launch time by `ContainerConfig.tty` in the workload spec — not at attach time. The guest-init allocates (or doesn't) a PTY when starting the process, and attach simply connects to whatever streams already exist.

The first client message must be `AttachStart` to identify the target. Subsequent client messages carry stdin data or terminal resize events. Server messages carry stdout/stderr data or an exit notification when the process terminates.

```protobuf
message AttachWorkloadInput {
    oneof input {
        AttachStart start = 1;
        AttachStdin stdin = 2;
        AttachResize resize = 3;
    }
}

message AttachStart {
    string namespace_id = 1;
    string workload_id = 2;
}

message AttachStdin {
    bytes data = 1;
}

message AttachResize {
    uint32 cols = 1;
    uint32 rows = 2;
}

message AttachWorkloadOutput {
    oneof output {
        AttachStarted started = 1;
        AttachStdout stdout = 2;
        AttachStderr stderr = 3;
        AttachExited exited = 4;
    }
}

message AttachStarted {
    bool tty = 1;              // whether the entrypoint is running with a PTY
}

message AttachStdout {
    bytes data = 1;
}

message AttachStderr {
    bytes data = 1;
}

message AttachExited {
    int32 exit_code = 1;
}
```

**Stream lifecycle:**

1. Client sends `AttachStart` with namespace/workload.
2. Server responds with `AttachStarted`, which reports whether the session is a TTY. The CLI uses this to decide whether to enter raw mode.
3. Client sends `AttachStdin` messages; server sends `AttachStdout`/`AttachStderr` messages.
4. When the entrypoint process exits, server sends `AttachExited` with the exit code and closes the stream.
5. If the client cancels the stream (detach), the entrypoint process is **not** killed — it continues running.

**TTY vs non-TTY:** The PTY is allocated at process start based on `ContainerConfig.tty`. When the entrypoint has a PTY, stdout and stderr are merged (standard PTY behavior) — the server only sends `AttachStdout`. The client should send `AttachResize` when the local terminal size changes. When the entrypoint has no PTY, stdout and stderr are separate pipe-backed streams and `AttachResize` is ignored.

**Error cases:**
- Workload not found: `NOT_FOUND`
- Workload not running (dormant, launching, etc.): `FAILED_PRECONDITION`
- Workload is spliced: `FAILED_PRECONDITION` (the local worker owns the process, not the orchestrator)

### Layer 2: Resource Queries

These RPCs support the uniform `dv get <resource-type>` commands. They return the same data as the status RPCs but sliced by resource type for convenience and scriptability.

```protobuf
message ListWorkersRequest {}
message ListWorkersResponse {
    repeated WorkerInfo workers = 1;
}

message GetWorkerRequest {
    string worker_id = 1;
}
message GetWorkerResponse {
    WorkerInfo worker = 1;
}

message WorkerInfo {
    string worker_id = 1;
    uint32 max_pods = 2;
    uint64 available_memory_mb = 3;
    uint32 active_pods = 4;
}

message ListPodsRequest {
    string namespace_id = 1;  // required -- pods are namespace-scoped
}
message ListPodsResponse {
    repeated PodInfo pods = 1;
}

message PodInfo {
    string pod_id = 1;
    string workload_id = 2;
    string worker_id = 3;
    string ip = 4;
    string mac = 5;
    PodState state = 6;
}

enum PodState {
    POD_STATE_UNSPECIFIED = 0;
    POD_STATE_LAUNCHING = 1;
    POD_STATE_RUNNING = 2;
    POD_STATE_SUSPENDING = 3;
    POD_STATE_SUSPENDED = 4;
    POD_STATE_RESUMING = 5;
}
```

### Events

Events are discrete, typed records of state transitions. They carry semantic meaning beyond what can be derived from status diffs -- the *why*, not just the *what*.

The `StreamEventsRequest` uses repeated arrays for filtering: `workload_ids` and `service_ids`. Empty arrays mean "all workloads" or "all services" respectively.

```protobuf
message StreamEventsRequest {
    string namespace_id = 1;
    repeated string workload_ids = 2;   // empty = all workloads
    repeated string service_ids = 3;    // empty = all services
}

message NamespaceEvent {
    int64 timestamp_unix_ms = 1;
    oneof event {
        WorkloadEvent workload_event = 2;
        ServiceEvent service_event = 3;
    }
}

message WorkloadEvent {
    string workload_id = 1;
    oneof event {
        WorkloadDemandChanged demand_changed = 2;
        WorkloadPodLaunching pod_launching = 3;
        WorkloadPodRunning pod_running = 4;
        WorkloadPodStopped pod_stopped = 5;
        WorkloadPodFailed pod_failed = 6;
        WorkloadSpliced spliced = 7;
        WorkloadUnspliced unspliced = 8;
        WorkloadPodSuspending pod_suspending = 9;
        WorkloadPodSuspended pod_suspended = 10;
        WorkloadPodSuspendFailed pod_suspend_failed = 11;
        WorkloadPodResuming pod_resuming = 12;
    }
}

message WorkloadDemandChanged {
    uint32 demanding_services = 1;
}

message WorkloadPodLaunching {
    string pod_id = 1;
    string worker_id = 2;
}

message WorkloadPodRunning {
    string pod_id = 1;
    string worker_id = 2;
}

message WorkloadPodStopped {
    int32 exit_code = 1;
}

message WorkloadPodFailed {
    string reason = 1;
}

message WorkloadPodSuspending {
    string pod_id = 1;
    string worker_id = 2;
}

message WorkloadPodSuspended {
    string worker_id = 1;
    string snapshot_id = 2;
}

message WorkloadPodSuspendFailed {
    string reason = 1;
}

message WorkloadPodResuming {
    string pod_id = 1;
    string worker_id = 2;
}

message WorkloadSpliced {
    string worker_id = 1;
}

message WorkloadUnspliced {}

message ServiceEvent {
    string service_id = 1;
    string workload_id = 2;
    oneof event {
        ServiceActivated activated = 3;
        ServiceBackendReady backend_ready = 4;
        ServiceIdleTimerStarted idle_timer_started = 5;
        ServiceIdleTimerCancelled idle_timer_cancelled = 6;
        ServiceIdleTimeoutFired idle_timeout_fired = 7;
        ServiceDeactivated deactivated = 8;
    }
}

message ServiceActivated {
    ServiceActivationTrigger trigger = 1;
}

enum ServiceActivationTrigger {
    SERVICE_ACTIVATION_TRIGGER_UNSPECIFIED = 0;
    SERVICE_ACTIVATION_TRIGGER_TRAFFIC = 1;
}

message ServiceBackendReady {}

message ServiceIdleTimerStarted {
    uint64 timeout_ms = 1;
}

message ServiceIdleTimerCancelled {
    IdleTimerCancelReason reason = 1;
}

enum IdleTimerCancelReason {
    IDLE_TIMER_CANCEL_REASON_UNSPECIFIED = 0;
    IDLE_TIMER_CANCEL_REASON_NEW_TRAFFIC = 1;
}

message ServiceIdleTimeoutFired {}

message ServiceDeactivated {
    ServiceDeactivationReason reason = 1;
}

enum ServiceDeactivationReason {
    SERVICE_DEACTIVATION_REASON_UNSPECIFIED = 0;
    SERVICE_DEACTIVATION_REASON_IDLE_TIMEOUT = 1;
    SERVICE_DEACTIVATION_REASON_FORCE_DEACTIVATE = 2;
}
```

Events are what `dv events` renders:

```
12:03:41  service/graphql    activated (traffic)
12:03:41  workload/api       demand up (1 service)
12:03:42  workload/api       pod-3a1f launching on worker-east
12:03:43  workload/api       pod-3a1f running
12:03:43  service/graphql    backend ready
12:03:43  service/grpc       backend ready (shared workload)
12:08:55  service/graphql    idle timer started (5m)
12:13:55  service/graphql    idle timeout fired
12:13:55  service/graphql    deactivated (idle_timeout)
12:13:55  workload/api       pod stopped (exit_code=0)
```

### Streaming (Logs)

```protobuf
message StreamLogsRequest {
    string namespace_id = 1;
    optional string workload_id = 2;  // absent = all workloads
}

// Sent on every state change. First message is always a full snapshot.
message WatchNamespaceStatusRequest {
    string namespace_id = 1;
}

message NamespaceStatusEvent {
    NamespaceStatusReport status = 1;
}

message LogChunk {
    string workload_id = 1;
    bytes data = 2;
    // Timestamp of when the orchestrator received this chunk.
    // Not the originating timestamp from the guest.
    int64 timestamp_unix_ms = 3;
}
```

---

## Error Handling

Errors use standard gRPC status codes. No custom error enums in the response messages -- the response types are for success payloads only (with the exception of `DeactivateWorkloadResponse` which uses `deactivated`/`reason` fields for a soft failure case).

Error code mapping is done by string-matching the orchestrator's error message:

| Error message contains | gRPC Status Code |
|---|---|
| "not found" | `NOT_FOUND` |
| "already exists" | `ALREADY_EXISTS` |
| "not spliced" or "already spliced" | `FAILED_PRECONDITION` |
| Anything else | `INTERNAL` |

Additionally, input validation errors (missing spec, invalid public key length) use `INVALID_ARGUMENT`.

---

## Status Watch Design

`WatchNamespaceStatus` is the intended primary mechanism for CLI/UI to display live state. It is currently **not implemented** (returns `UNIMPLEMENTED`).

### Full snapshots on every change

The design uses full snapshots: every `NamespaceStatusEvent` would contain the complete `NamespaceStatusReport`. The client replaces its local state entirely on each message.

Given the small size of namespace status (a handful of workloads and services), full snapshots are the right starting point. A namespace with 20 workloads and 30 services produces a status report well under 2KB. Delta compression is not worth the complexity.

### Events vs Status

Status watches and event streams serve different purposes:

- **Status watch** (`WatchNamespaceStatus`): "What is the current state?" -- full snapshot, replace-on-update. Intended for `dv status` with live refresh.
- **Event stream** (`StreamEvents`): "What happened?" -- discrete transitions with semantic meaning. Used by `dv events`. Events carry the *reason* for a transition (e.g. "activated because traffic", "idle timeout fired"), which isn't in the status snapshot.

### Backpressure

gRPC server-streaming over HTTP/2 has built-in flow control. If the client is slow to read, the orchestrator's send buffer fills up and applies backpressure naturally. The orchestrator should coalesce pending status updates -- if multiple state changes happen while the client is behind, only the latest snapshot needs to be sent. Events are not coalesced (each event is meaningful).

---

## Implementation Notes

### Rust gRPC Stack

Uses **tonic** for the gRPC server. Proto definitions compile to Rust types via **prost**. The orchestrator's async shell implements the tonic service trait, translating between gRPC requests and orchestrator state machine inputs/outputs.

The implementation uses a `unary_command` helper that connects a temporary client, sends a command, and gets the response:

```rust
async fn unary_command(&self, command: ClientCommand) -> Result<ClientEvent, Status> {
    let client_id = self.handle.connect_client();
    let result = self.handle.send_command(client_id.clone(), command).await;
    self.handle.disconnect_client(client_id);
    result.map_err(|e| Status::internal(e.to_string()))
}
```

Each RPC handler sends the appropriate `ClientCommand` variant to the orchestrator state machine and pattern-matches on the resulting `ClientEvent` to build the gRPC response.

### Boundary Translation

The gRPC layer is a thin boundary that translates between protobuf types and internal orchestrator types. This translation layer:

- Validates inputs (returns `INVALID_ARGUMENT` for bad data)
- Converts proto types to internal Rust types via `convert_proto_spec`
- Converts internal types to proto types for responses via `convert_status_report` and `convert_sm_event_to_proto`
- Maps internal errors to gRPC status codes via string matching

The internal orchestrator state machine never sees protobuf types. This keeps the state machine pure and testable without gRPC dependencies.

---

## Open Questions

1. **Server-side auth**: Authentication is client-side only. Server-side validation needs to be implemented before any multi-tenant or internet-facing deployment.
2. **Reflection / health**: Should we enable gRPC server reflection and the standard health checking service? Useful for tooling (grpcurl, grpc-health-probe). Low cost to add.
3. **Log streaming granularity**: Should `StreamLogs` support streaming stderr/stdout separately? Or is a merged stream sufficient? (Note: `AttachWorkload` does separate stdout/stderr in non-TTY mode.)
4. **WatchNamespaceStatus**: Needs implementation -- currently a stub returning `UNIMPLEMENTED`.
5. **Event history**: Should `StreamEvents` support a lookback window (e.g. last N events or last T seconds)? Without it, events before the stream opens are lost. Could be a simple ring buffer in the orchestrator.
