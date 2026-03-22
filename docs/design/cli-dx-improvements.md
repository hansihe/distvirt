# CLI DX Improvements

Tracked issues and designs for improving the `dv` CLI experience.

## 1. Fix log source display

**Problem:** `dv logs` displays the numeric pod ID in the workload field. The gRPC
conversion in `orchestrator/src/grpc/conversions.rs` puts `chunk.pod_id.0.to_string()`
into the `workload_id` proto field. Users see `[3]` instead of `[my-app]`.

**Fix:** Resolve pod_id → workload name in the conversion. The log chunk already
carries `namespace_id` and `pod_id`; use the pod list or ID registry to look up
the workload name. Display format should be `[workload_name/container_id]`.

**Files:** `crates/orchestrator/src/grpc/conversions.rs`, `crates/cli/src/format.rs`

## 2. Log bus lifecycle and workload-scoped queries

**Problem:** Log topics are never cleaned up. `remove_namespace()` exists on
`LogBus` but is never called from production code. Topics for dead pods
accumulate in memory forever.

**Problem:** Querying logs for a workload should show logs from all pods for
that workload (including recently dead pods), merged in timestamp order.

**Design:**

- Add `workload_name` to the log bus topic metadata so the bus can answer
  "give me all topics for workload X, including retired ones."
- When a pod is removed, don't delete the topic. Mark it with a `retired_at`
  timestamp.
- Retired topics are evicted after a configurable retention period (e.g. 5 min)
  or under memory pressure, whichever comes first.
- Add `subscribe_by_workload(namespace_id, workload_name)` that returns a
  merged stream across all matching pod topics, ordered by timestamp.
- The gRPC handler switches from resolving workload → pod IDs via `list_pods()`
  to using the log bus's workload-aware query directly.
- Also call `log_bus.remove_namespace()` from `DestroyNamespace` in the shell
  (same for event_bus).

**Files:** `crates/orchestrator/src/log_bus.rs`, `crates/orchestrator/src/grpc/mod.rs`,
`crates/orchestrator/src/shell/async/mod.rs`

## 3. Port spec with progressive enhancement

**Problem:** The current service spec requires separate service objects (with
separate IPs) to get multiple activators on different ports. The spec format
is awkward for simple use cases. Activation config is service-level, so you
need separate services (with separate IPs) to use different activator types
on different ports.

### User-facing YAML spec

Replace `activation` + `expose` with a `ports` list. `ExposeSpec` is removed
entirely — `ports` subsumes it. Progressive enhancement — simple cases are
terse, complex cases add fields.

Applies to both top-level `SpecService` and inline `SpecInlineService`
identically.

```yaml
services:
  my-service:
    workload: my-app
    idle_timeout: 30s           # service-level, applies to all activators
    ports:
      - 80                      # short form: TCP activation with defaults
      - 8080:80                 # short form: port mapped (target port, L4 only)
      - port: 443               # extended form, TCP activation by default
        target: 8443
      - port: 443
        activator:
          type: tcp             # explicit TCP with custom config
          max_flows: 100
      - port: 8080
        activator:
          type: http2           # different activator type
```

Short forms desugar into the extended form with default TCP activator.

The `activator` field is an optional tagged union — `type` selects the variant,
remaining fields are that variant's config.

### Activation model

A service is either **activated** or **not activated** — this is mutually
exclusive. If any port has an activator, all ports must have activators (they
get TCP activation by default). A service with no activators on any port is
a pure passthrough service. Mixed activated/non-activated ports on the same
service are rejected at spec validation time. This constraint can be relaxed
in the future if needed.

`has_activation` on the endpoint is derived from whether any port has an
activator.

`idle_timeout` lives at the service level and applies uniformly to all
activators on the service.

`PassthroughActivator` is removed. A port with no `activator` field (or
`activator: none`) is a passthrough port. A service where all ports are
passthrough has `has_activation = false`.

No changes to the workload spec — workload-level demand control is orthogonal
and already exists.

### Target port (port mapping)

`target` specifies the backend port the container listens on. This is an
activator concern — the activator is what actually connects upstream.

- **L4 (http2):** StreamManager connects upstream on `target` port. Activator
  (wasm) receives the configured target port. Fully functional.
- **L3 (tcp) / passthrough:** Would require DNAT (rewrite dst_port in IP
  header + reverse NAT on reply). TODO — not implemented in this work. Spec
  validation rejects `target` on non-L4 ports until DNAT is implemented.
  Leave a clear TODO in the code for this.

### Wire format changes

**Client proto** (`client.proto`): Remove `ActivationSpec`, `ExposeSpec`,
`PassthroughActivator`. Replace with a `PortSpec` message on `ServiceSpec`:
- `port: uint32` — exposed port
- `target_port: uint32` — backend port (0 = same as port)
- `activator: oneof` — tcp / http2 (absent = no activator / passthrough)

`idle_timeout` stays as a top-level field on `ServiceSpec`.

**Worker protocol** (`worker-protocol/src/types.rs`): `ServicePolicy` carries
`Vec<PortConfig>` instead of single `Option<ActivatorConfig>`:
```rust
struct PortConfig {
    port: u16,
    target_port: u16,
    activator: Option<ActivatorConfig>,  // None = passthrough
}

struct ServicePolicy {
    ports: Vec<PortConfig>,
    buffer_frames: u32,
    timeout_ms: u32,
}
```

`buffer_frames` and `timeout_ms` stay at service level. They control
per-endpoint frame buffering in the hot path (`dispatch.rs::try_buffer_frame`)
which operates below port-level routing — the buffer is shared across all
ports on the endpoint.

### Orchestrator changes

**Internal types** (`types/specs.rs`): `ServicePolicy` carries per-port config.
`has_activation` derived from any port having an activator.

**Conversion** (`grpc/conversions.rs`): Convert list of proto `PortSpec` →
internal per-port config. Extract `idle_timeout` from service-level field.

**SM types** (`sm/mod.rs`): `ServiceSpec` and `EndpointConfig` pass through
per-port policy. `has_activation` stays a bool, derived at conversion time.

### Worker changes

**ServiceProcessor** restructured from an enum (Passthrough/L3/L4) to a struct
with per-port routing:

```rust
struct ServiceProcessor {
    port_routes: HashMap<u16, PortMode>,
    stream_manager: Option<StreamManager>,  // shared across all L4 ports
    flow_tracker: FlowTracker,              // shared for demand signaling
}

enum PortMode {
    Passthrough,
    L3 { activator: ActivatorInstance },
    L4,  // uses shared stream_manager
}
```

Incoming packet → extract dst_port → route to appropriate `PortMode`.
L4 ports share one `StreamManager` / smoltcp stack. L3 ports each get their
own activator instance. Passthrough ports skip to buffering/forwarding.

The endpoint table stays IP-indexed. Port dispatch happens inside
`ServiceProcessor`, not at the endpoint table level.

`build_processor` (`worker/namespace.rs`) constructs the processor from the
per-port config list, instantiating activators and configuring StreamManager
`listen_ports` from the L4 port set.

### Client changes

**Spec types** (`client/src/spec/types.rs`): Remove `SpecActivation`,
`SpecExpose`. Replace with `Vec<SpecPort>` on both `SpecService` and
`SpecInlineService`. `SpecPort` supports short forms (u16, "port:target"
string) and extended form (struct with optional activator).

**Spec conversion** (`client/src/spec/convert.rs`): Desugar short forms →
extended form → proto `PortSpec`. Validate mutual exclusivity constraint
(all ports activated or none). Reject `target` on non-L4 activator types.

### Files

- `crates/client-protocol/proto/distvirt/client/v1/client.proto`
- `crates/client/src/spec/types.rs`, `convert.rs`, `helpers.rs`
- `crates/orchestrator/src/types/specs.rs`
- `crates/orchestrator/src/grpc/conversions.rs`
- `crates/orchestrator/src/sm/mod.rs`, `service.rs`
- `crates/orchestrator/src/adapter/management/mod.rs`
- `crates/worker-protocol/src/types.rs`
- `crates/worker/src/fabric/endpoint/service_processor.rs`
- `crates/worker/src/worker/namespace.rs`

## 4. `dv delete` for workloads and services

**Problem:** `dv delete` only works for namespaces. Deleting a workload or
service requires going through `dv spec sync` with the item removed.

**Fix:** Wire `dv delete workload <name> --namespace <ns>` and
`dv delete service <name> --namespace <ns>` into calling the existing
`PatchNamespaceRequest` with `remove_workloads` / `remove_services` fields.

**Files:** `crates/cli/src/commands/resource.rs`, `crates/client/src/operations.rs`

## 5. Pod creation progress events

**Problem:** The worker protocol is binary — `LaunchPod` → `PodRunning` or
`PodFailed`. No intermediate progress (image pulling, container startup, etc.).
There's a 30-second launch timeout with no visibility into what's happening.

**Design:**

- Add a `PodProgress` event to the worker protocol with a phase enum
  (e.g. `pulling_image`, `starting_containers`, `configuring_network`) and
  an optional detail string.
- Worker emits these during launch. Orchestrator forwards them through the
  event bus.
- `dv events` already streams events and can display these with no changes
  to the streaming infrastructure.

**Files:** `crates/worker-protocol/src/lib.rs`, `crates/worker-protocol/src/types.rs`,
`crates/orchestrator/src/adapter/observability/mod.rs`,
`crates/client-protocol/proto/distvirt/client/v1/client.proto`
