# Client Protocol

## Overview

The client protocol defines the interface between **clients** (CLI, UI, automation tools) and the **orchestrator**. It uses **gRPC** over HTTP/2, providing strong typing via protobuf, natural request-response correlation, and built-in streaming for subscriptions.

This is a control plane protocol — all operations are management commands and status queries. There is no data plane traffic on this interface.

The protocol has two primary entities:

- **Workloads**: the spec for a schedulable unit. A workload describes what a pod should look like (image, containers, config). When scheduled, it becomes a **pod** — a microVM with its own IP/MAC on the fabric. A pod can host multiple containers.
- **Services**: network entities with their own IP/MAC on the fabric, with a programmable activation layer. Each service points at a workload via `workload_id`. For L3 traffic, the service NATs to the backing pod's IP. The activation mechanism is what enables scale-to-zero — the service entity exists on the fabric and can intercept traffic even when no pod is running.

Both workloads and services are top-level in the spec and status. At runtime, both pods (the workload's runtime instance) and services have their own IP/MAC on the fabric. The service→workload binding is mutable: retargeting a service from one workload to another is a valid operation (e.g. blue/green, canary, splice). The CLI reassembles the workload-grouped view (`dv status` shows services nested under workloads) from the flat data.

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
  |──── gRPC connect (TCP/UDS) ─────────>|
  |                                       |
  |  Unary RPCs (create, delete, etc.)   |
  |──── request ────────────────────────>|
  |<──── response ──────────────────────-|
  |                                       |
  |  Server-streaming RPCs (watch, logs) |
  |──── request ────────────────────────>|
  |<──── stream of responses ───────────-|
  |<──── stream of responses ───────────-|
  |<──── ... ───────────────────────────-|
  |                                       |
```

No client-streaming or bidirectional-streaming RPCs are needed. The protocol is either unary (one request, one response) or server-streaming (one request, many responses).

### Connection Lifecycle

- Clients connect on demand. There is no persistent session registration — each RPC is independent.
- Server-streaming RPCs (watch, logs, events) maintain a long-lived connection. The orchestrator tracks active subscriptions internally and cleans up when the stream is cancelled or the connection drops.
- Clients can open multiple concurrent RPCs on the same connection (HTTP/2 multiplexing).
- Authentication: API key tokens in gRPC metadata (see Authentication section below).

---

## Authentication

Clients authenticate by sending an API key as a bearer token in the `authorization` gRPC metadata header. The server validates the token via a tonic interceptor before the request reaches any service handler.

### Token model

- Tokens are **opaque API keys** — random strings stored and validated server-side.
- All tokens are **global scope** — a valid token grants access to all namespaces and operations. Namespace-scoped tokens are a future extension.
- Tokens are issued and revoked out-of-band (CLI command, config file, etc.). There is no token-management RPC in this protocol.

### Wire format

The client sets the `authorization` metadata key on every RPC (unary and streaming):

```
authorization: Bearer <token>
```

For streaming RPCs, the token is sent once at stream open. The server validates it at that point.

**Caveat — streaming auth expiry**: Once a streaming RPC is established, the token is not re-validated for the lifetime of the stream. If a token is revoked while a stream is open, the stream continues until the client disconnects or the server restarts. This is acceptable for now. If mid-stream revocation becomes necessary, options include: server-side periodic re-validation, short-lived tokens with forced reconnect, or server-initiated stream termination on revocation.

### Server implementation

Auth is enforced in a tonic interceptor, keeping it out of service handlers entirely:

```rust
fn check_auth(req: Request<()>) -> Result<Request<()>, Status> {
    let token = req.metadata().get("authorization")
        .ok_or_else(|| Status::unauthenticated("missing token"))?;
    // validate against token store
    Ok(req)
}

Server::builder()
    .add_service(DistvirtClientServer::with_interceptor(svc, check_auth))
```

### Client usage

The CLI stores the token in `~/.config/distvirt/credentials` and attaches it to every RPC. Example flow:

```
$ dv login --token <api-key>    # stores token locally
$ dv status my-namespace        # token sent automatically
```

### Transport security

TLS (server-side) is **required** when exposed over the internet — tokens are sent in the clear without it. In local mode (unix socket), TLS is optional.

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

    // --- Cloning ---
    rpc CloneNamespace(CloneNamespaceRequest) returns (CloneNamespaceResponse);

    // --- Layer 2: Resource queries ---
    rpc ListWorkers(ListWorkersRequest) returns (ListWorkersResponse);
    rpc GetWorker(GetWorkerRequest) returns (GetWorkerResponse);
    rpc ListPods(ListPodsRequest) returns (ListPodsResponse);

    // --- Streaming subscriptions ---
    rpc WatchNamespaceStatus(WatchNamespaceStatusRequest) returns (stream NamespaceStatusEvent);
    rpc StreamLogs(StreamLogsRequest) returns (stream LogChunk);
    rpc StreamEvents(StreamEventsRequest) returns (stream NamespaceEvent);
}
```

### Unary RPCs

Each unary RPC gets a single response. Errors use standard gRPC status codes (NOT_FOUND, ALREADY_EXISTS, INVALID_ARGUMENT, etc.) with descriptive messages in the status detail.

### Server-Streaming RPCs

> **Note:** `WatchNamespaceStatus`, `StreamLogs`, and `StreamEvents` are defined in the proto and have gRPC handler stubs, but currently return "not yet implemented" status in the server code.

- **WatchNamespaceStatus**: Pushes a `NamespaceStatusEvent` whenever any workload or service state changes within the namespace. The first message is always the current full status (so the client doesn't need a separate `GetNamespaceStatus` call). Subsequent messages are full snapshots (see Status Watch Design below).
- **StreamLogs**: Pushes log chunks as they arrive. The client specifies an optional `workload_id` filter.
- **StreamEvents**: Pushes discrete, typed events describing state transitions — activation triggers, pod launches, idle timeouts, etc. This is what `dv events` displays.

All streaming RPCs are cancellable — the client can cancel the stream at any time. The orchestrator detects cancellation and cleans up the subscription.

---

## Messages

### Namespace Spec

The `NamespaceSpec` is the declarative description of what should exist. Clients construct this (from compose files, k8s manifests, or directly) and send it to the orchestrator.

Workloads and services are both **top-level** in the spec. Both have network identity on the fabric (IP/MAC). A workload describes what to run; when scheduled, it becomes a pod (a microVM). A service is a network entity that NATs to its backing pod. Each service references a workload via `workload_id`. This binding is mutable — changing a service's `workload_id` retargets it to a different workload's pod.

A common simple case (one service per workload, same name) is what compose frontends produce. But the model supports many-to-one: multiple services backed by the same workload, and retargeting.

```protobuf
message NamespaceSpec {
    NetworkConfig network = 1;
    map<string, WorkloadSpec> workloads = 2;
    map<string, ServiceSpec> services = 3;
}

message WorkloadSpec {
    PodNetworkConfig network = 1;       // pod IP, MAC — assigned by orchestrator
    repeated ContainerSpec containers = 2;
}

message ContainerSpec {
    string name = 1;
    string image = 2;
    ContainerConfig config = 3;
}

message PodNetworkConfig {
    string ip = 1;      // assigned by orchestrator
    string mac = 2;     // assigned by orchestrator
}

message ServiceSpec {
    string workload_id = 1;             // which workload backs this service
    ServiceNetworkConfig network = 2;   // ip, mac — assigned by orchestrator
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
    uint64 memory_mb = 4;
    uint32 vcpus = 5;
}

message ServiceNetworkConfig {
    string ip = 1;      // assigned by orchestrator
    string mac = 2;     // assigned by orchestrator
}

message NetworkConfig {
    string subnet = 1;  // e.g. "10.0.0.0/24"
}

message ExposeSpec {
    uint32 container_port = 1;
    uint32 host_port = 2;
    string protocol = 3;  // "tcp" or "udp"
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
    bool spliced = 4;                // from workload hosting state
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

This is a presentation concern — the protocol provides flat data, the CLI joins on `workload_id`.

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
    // Future: location, labels, etc.
}

message ListPodsRequest {
    string namespace_id = 1;  // required — pods are namespace-scoped
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
}
```

### Events

Events are discrete, typed records of state transitions. They carry semantic meaning beyond what can be derived from status diffs — the *why*, not just the *what*.

```protobuf
message StreamEventsRequest {
    string namespace_id = 1;
    optional string workload_id = 2;    // absent = all workloads
    optional string service_id = 3;     // absent = all services
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
    }
}

message WorkloadDemandChanged {
    uint32 demanding_services = 1;  // new demand count
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
    string reason = 1;  // "demand_dropped", "worker_lost", etc.
}

message WorkloadPodFailed {
    string reason = 1;
}

message WorkloadSpliced {
    string worker_id = 1;  // splice target
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
    string trigger = 1;  // "first_traffic", "always_on", etc.
}

message ServiceBackendReady {}

message ServiceIdleTimerStarted {
    uint64 timeout_ms = 1;
}

message ServiceIdleTimerCancelled {
    string reason = 1;  // "traffic_resumed", "backend_need_active"
}

message ServiceIdleTimeoutFired {}

message ServiceDeactivated {
    string reason = 1;  // "idle_timeout", "workload_lost"
}
```

Events are what `dv events` renders:

```
12:03:41  service/graphql    activated (first_traffic)
12:03:41  workload/api       demand up (1 service)
12:03:42  workload/api       pod-3a1f launching on worker-east
12:03:43  workload/api       pod-3a1f running
12:03:43  service/graphql    backend ready
12:03:43  service/grpc       backend ready (shared workload)
12:08:55  service/graphql    idle timer started (5m)
12:13:55  service/graphql    idle timeout fired
12:13:55  workload/api       pod stopped (demand_dropped)
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

Errors use standard gRPC status codes. No custom error enums in the response messages — the response types are for success payloads only.

| Condition | gRPC Status Code |
|---|---|
| Namespace not found | `NOT_FOUND` |
| Namespace already exists (create) | `ALREADY_EXISTS` |
| Workload not found (splice, unsplice) | `NOT_FOUND` |
| Worker not found | `NOT_FOUND` |
| Invalid spec (missing required fields, bad subnet) | `INVALID_ARGUMENT` |
| Workload not spliced (unsplice) | `FAILED_PRECONDITION` |
| Workload already spliced (splice) | `FAILED_PRECONDITION` |
| Clone source not found | `NOT_FOUND` |
| Clone target already exists | `ALREADY_EXISTS` |
| Internal orchestrator error | `INTERNAL` |

The status detail message should be human-readable and describe the specific problem (e.g. `"workload 'api' is not spliced"` not just `"precondition failed"`).

---

## Status Watch Design

`WatchNamespaceStatus` is the primary mechanism for CLI/UI to display live state. Design choices:

### Full snapshots on every change

The simplest approach: every `NamespaceStatusEvent` contains the complete `NamespaceStatusReport`. The client replaces its local state entirely on each message.

Pros: Simple, no state synchronization bugs, client can reconnect and get full state immediately.
Cons: More bytes on the wire per update.

Given the small size of namespace status (a handful of workloads and services), full snapshots are the right starting point. A namespace with 20 workloads and 30 services produces a status report well under 2KB. Delta compression is not worth the complexity.

### Events vs Status

Status watches and event streams serve different purposes:

- **Status watch** (`WatchNamespaceStatus`): "What is the current state?" — full snapshot, replace-on-update. Used by `dv status` with live refresh.
- **Event stream** (`StreamEvents`): "What happened?" — discrete transitions with semantic meaning. Used by `dv events`. Events carry the *reason* for a transition (e.g. "activated because first traffic", "idle timeout fired"), which isn't in the status snapshot.

Both can be open simultaneously. The status watch is the source of truth for display; events provide the activity log.

### Backpressure

gRPC server-streaming over HTTP/2 has built-in flow control. If the client is slow to read, the orchestrator's send buffer fills up and applies backpressure naturally. The orchestrator should coalesce pending status updates — if multiple state changes happen while the client is behind, only the latest snapshot needs to be sent. Events are not coalesced (each event is meaningful).

---

## Implementation Notes

### Rust gRPC Stack

Use **tonic** for the gRPC server. Proto definitions compile to Rust types via **prost**. The orchestrator's async shell implements the tonic service trait, translating between gRPC requests and orchestrator state machine inputs/outputs.

```rust
#[tonic::async_trait]
impl DistvirtClient for OrchestratorGrpcService {
    async fn create_namespace(
        &self,
        request: Request<CreateNamespaceRequest>,
    ) -> Result<Response<CreateNamespaceResponse>, Status> {
        let req = request.into_inner();
        let spec = convert_spec(req.spec)?;
        let result = self.orch_handle
            .send_command(ClientCommand::CreateNamespace {
                namespace_id: req.namespace_id,
                spec,
            })
            .await;
        match result {
            Ok(()) => Ok(Response::new(CreateNamespaceResponse {})),
            Err(e) => Err(e.into_status()),
        }
    }

    type WatchNamespaceStatusStream = ReceiverStream<Result<NamespaceStatusEvent, Status>>;

    async fn watch_namespace_status(
        &self,
        request: Request<WatchNamespaceStatusRequest>,
    ) -> Result<Response<Self::WatchNamespaceStatusStream>, Status> {
        let req = request.into_inner();
        let (tx, rx) = mpsc::channel(32);
        self.orch_handle
            .subscribe_status(req.namespace_id, tx)
            .await?;
        Ok(Response::new(ReceiverStream::new(rx)))
    }

    type StreamEventsStream = ReceiverStream<Result<NamespaceEvent, Status>>;

    async fn stream_events(
        &self,
        request: Request<StreamEventsRequest>,
    ) -> Result<Response<Self::StreamEventsStream>, Status> {
        let req = request.into_inner();
        let (tx, rx) = mpsc::channel(64);
        self.orch_handle
            .subscribe_events(req.namespace_id, req.workload_id, req.service_id, tx)
            .await?;
        Ok(Response::new(ReceiverStream::new(rx)))
    }

    // ...
}
```

### Boundary Translation

The gRPC layer is a thin boundary that translates between protobuf types and internal orchestrator types. This translation layer:

- Validates inputs (returns `INVALID_ARGUMENT` for bad data)
- Converts proto types to internal Rust types (e.g. `proto::WorkloadSpec` -> internal `WorkloadSpec`)
- Converts internal types to proto types for responses
- Maps internal errors to gRPC status codes

The internal orchestrator state machine never sees protobuf types. This keeps the state machine pure and testable without gRPC dependencies.

### Event Generation

Events are generated by the orchestrator state machine as part of its output. Each sub-SM transition that the CLI cares about produces a typed event in `NamespaceOutput`:

```rust
struct NamespaceOutput {
    worker_commands: Vec<(WorkerId, WorkerCommand)>,
    client_events: Vec<(ClientId, ClientEvent)>,
    namespace_events: Vec<NamespaceEvent>,  // for StreamEvents subscribers
    timers_set: Vec<(TimerKey, Duration)>,
    timers_cancel: Vec<TimerKey>,
    pod_requests: Vec<PodRequest>,
}
```

The async shell dispatches `namespace_events` to all active `StreamEvents` subscribers for that namespace, applying workload/service filters.

---

## Open Questions

1. **Reflection / health**: Should we enable gRPC server reflection and the standard health checking service? Useful for tooling (grpcurl, grpc-health-probe). Low cost to add.
2. **Log streaming granularity**: Should `StreamLogs` support streaming stderr/stdout separately? Or is a merged stream sufficient?
3. **Status watch scope**: Should there be a `WatchAllNamespaces` variant that streams status changes across all namespaces? Useful for a dashboard UI.
4. **Spec validation**: How much spec validation happens at the protocol layer vs in the orchestrator state machine? The gRPC layer should catch structural issues (missing fields). Semantic validation (duplicate IPs, invalid subnets) belongs in the orchestrator.
5. **Event history**: Should `StreamEvents` support a lookback window (e.g. last N events or last T seconds)? Without it, events before the stream opens are lost. Could be a simple ring buffer in the orchestrator.
