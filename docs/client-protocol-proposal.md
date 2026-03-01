# Client Protocol

## Overview

The client protocol defines the interface between **clients** (CLI, UI, automation tools) and the **orchestrator**. It uses **gRPC** over HTTP/2, providing strong typing via protobuf, natural request-response correlation, and built-in streaming for subscriptions.

This is a control plane protocol — all operations are management commands and status queries. There is no data plane traffic on this interface.

### Why gRPC

- **Request-response for free**: Each RPC is its own HTTP/2 stream. No need for request ID correlation.
- **Streaming built-in**: Server-streaming RPCs for log tailing and status watches, with HTTP/2 flow control handling backpressure.
- **Strong typing**: Protobuf defines the exact shape of every message, including service state variants. Code generation produces idiomatic types in every target language.
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
- Server-streaming RPCs (watch, logs) maintain a long-lived connection. The orchestrator tracks active subscriptions internally and cleans up when the stream is cancelled or the connection drops.
- Clients can open multiple concurrent RPCs on the same connection (HTTP/2 multiplexing).
- Authentication is an extension point. Initially there is no auth. Future: mTLS client certificates or token-based auth via gRPC metadata.

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

    // --- Streaming subscriptions ---
    rpc WatchNamespaceStatus(WatchNamespaceStatusRequest) returns (stream NamespaceStatusEvent);
    rpc StreamLogs(StreamLogsRequest) returns (stream LogChunk);
}
```

### Unary RPCs

Each unary RPC gets a single response. Errors use standard gRPC status codes (NOT_FOUND, ALREADY_EXISTS, INVALID_ARGUMENT, etc.) with descriptive messages in the status detail.

### Server-Streaming RPCs

- **WatchNamespaceStatus**: Pushes a `NamespaceStatusEvent` whenever any service state changes. The first message is always the current full status (so the client doesn't need a separate `GetNamespaceStatus` call). Subsequent messages are deltas or full snapshots (see below).
- **StreamLogs**: Pushes log chunks as they arrive. The client specifies an optional `service_id` filter.

Both streaming RPCs are cancellable — the client can cancel the stream at any time. The orchestrator detects cancellation and cleans up the subscription.

---

## Messages

### Namespace Spec

The `NamespaceSpec` is the declarative description of what should exist. Clients construct this (from compose files, k8s manifests, or directly) and send it to the orchestrator.

```protobuf
message NamespaceSpec {
    NetworkConfig network = 1;
    map<string, ServiceSpec> services = 2;
}

message ServiceSpec {
    string image = 1;
    ContainerConfig container_config = 2;
    ServiceNetworkConfig network = 3;
    ActivationSpec activation = 4;  // absent = always-on
    repeated ExposeSpec expose = 5;
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

Service state is a strongly-typed `oneof`, not a string. Each variant carries only the fields relevant to that state.

```protobuf
message NamespaceStatusReport {
    string namespace_id = 1;
    NamespaceState state = 2;
    map<string, ServiceStatusReport> services = 3;
}

enum NamespaceState {
    NAMESPACE_STATE_UNSPECIFIED = 0;
    NAMESPACE_STATE_CREATING = 1;
    NAMESPACE_STATE_ACTIVE = 2;
    NAMESPACE_STATE_DESTROYING = 3;
}

message ServiceStatusReport {
    ServiceState state = 1;
    bool activation_enabled = 2;
    bool spliced = 3;
}

message ServiceState {
    oneof state {
        ServicePending pending = 1;
        ServiceIdle idle = 2;
        ServiceWaitingForCapacity waiting_for_capacity = 3;
        ServiceLaunching launching = 4;
        ServiceActive active = 5;
    }
}

message ServicePending {}

message ServiceIdle {}

message ServiceWaitingForCapacity {}

message ServiceLaunching {
    string pod_id = 1;
    string worker_id = 2;
}

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

### Splice

```protobuf
message SpliceRequest {
    string namespace_id = 1;
    string service_id = 2;
    string local_worker_id = 3;
}
message SpliceResponse {}

message UnspliceRequest {
    string namespace_id = 1;
    string service_id = 2;
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
    // Per-service overrides. Key is service_id.
    // Only specified fields are overridden; absent services
    // inherit the source spec unchanged.
    map<string, ServiceOverrides> services = 1;
}

message ServiceOverrides {
    optional string image = 1;
    map<string, string> env_overrides = 2;  // merged into source env
}
```

### Streaming

```protobuf
message WatchNamespaceStatusRequest {
    string namespace_id = 1;
}

// Sent on every state change. First message is always a full snapshot.
message NamespaceStatusEvent {
    NamespaceStatusReport status = 1;
}

message StreamLogsRequest {
    string namespace_id = 1;
    optional string service_id = 2;  // absent = all services
}

message LogChunk {
    string service_id = 1;
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
| Service not found (splice, unsplice) | `NOT_FOUND` |
| Invalid spec (missing required fields, bad subnet) | `INVALID_ARGUMENT` |
| Service not spliced (unsplice) | `FAILED_PRECONDITION` |
| Service already spliced (splice) | `FAILED_PRECONDITION` |
| Clone source not found | `NOT_FOUND` |
| Clone target already exists | `ALREADY_EXISTS` |
| Internal orchestrator error | `INTERNAL` |

The status detail message should be human-readable and describe the specific problem (e.g. `"service 'web' is not spliced"` not just `"precondition failed"`).

---

## Status Watch Design

`WatchNamespaceStatus` is the primary mechanism for CLI/UI to display live state. Design choices:

### Full snapshots on every change

The simplest approach: every `NamespaceStatusEvent` contains the complete `NamespaceStatusReport`. The client replaces its local state entirely on each message.

Pros: Simple, no state synchronization bugs, client can reconnect and get full state immediately.
Cons: More bytes on the wire per update.

Given the small size of namespace status (a handful of services), full snapshots are the right starting point. A namespace with 20 services produces a status report well under 1KB. Delta compression is not worth the complexity.

### Backpressure

gRPC server-streaming over HTTP/2 has built-in flow control. If the client is slow to read, the orchestrator's send buffer fills up and applies backpressure naturally. The orchestrator should coalesce pending updates — if multiple state changes happen while the client is behind, only the latest snapshot needs to be sent.

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
        // Send input to orchestrator state machine, await response
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
        // Register subscription in orchestrator
        self.orch_handle
            .subscribe_status(req.namespace_id, tx)
            .await?;
        Ok(Response::new(ReceiverStream::new(rx)))
    }

    // ...
}
```

### Boundary Translation

The gRPC layer is a thin boundary that translates between protobuf types and internal orchestrator types. This translation layer:

- Validates inputs (returns `INVALID_ARGUMENT` for bad data)
- Converts proto types to internal Rust types (e.g. `proto::ServiceSpec` -> internal `ServiceSpec`)
- Converts internal types to proto types for responses
- Maps internal errors to gRPC status codes

The internal orchestrator state machine never sees protobuf types. This keeps the state machine pure and testable without gRPC dependencies.

---

## Open Questions

1. **Reflection / health**: Should we enable gRPC server reflection and the standard health checking service? Useful for tooling (grpcurl, grpc-health-probe). Low cost to add.
2. **Log streaming granularity**: Should `StreamLogs` support streaming stderr/stdout separately? Or is a merged stream sufficient?
3. **Status watch scope**: Should there be a `WatchAllNamespaces` variant that streams status changes across all namespaces? Useful for a dashboard UI.
4. **Spec validation**: How much spec validation happens at the protocol layer vs in the orchestrator state machine? The gRPC layer should catch structural issues (missing fields). Semantic validation (duplicate IPs, invalid subnets) belongs in the orchestrator.
