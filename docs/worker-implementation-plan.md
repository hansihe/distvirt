# Worker Implementation Plan

## Current State vs. Worker Protocol Target

### What Already Exists (aligned with the spec)

- **Compose parser** (`distvirt-compose/`) — converts YAML to `Deployment`
- **Execution planning** (`deployment.rs`) — topological sort, IP/MAC assignment, `ServiceRegistry`
- **Fabric layer** (`fabric/`) — L2 switch, smoltcp gateway, TAP ports, TUN egress, DNS
- **Guest protocol** (`distvirt-guest-protocol/`) — vsock control + I/O channels, container lifecycle messages
- **VM management** (`orchestrate.rs`) — `ManagedVm` with connect, configure, add/start/wait container
- **Log streaming** (`io_session.rs`, `log_collector.rs`) — host-side I/O session + multi-service aggregation
- **Guest init** — handles AddContainer, StartContainer, ConfigureNetwork, I/O multiplexing
- **Worker abstraction** (`worker.rs`) — `Worker` struct with command/event interface, local orchestrator loop

### What's Missing: The Worker Abstraction

~~The critical gap is that there is no `Worker` struct.~~ **DONE** — The `Worker` struct now exists in `distvirt/src/worker.rs`.

### Concrete Gaps

| Area | Current State | Target State | Gap |
|------|--------------|-------------|-----|
| **Worker struct** | **DONE** (`worker.rs`) | Command/event driven executor | ✅ Implemented |
| **WorkerCommand/WorkerEvent enums** | **DONE** (`worker.rs`) | Typed enums matching protobuf spec | ✅ Implemented |
| **Namespace lifecycle** | **DONE** (`worker.rs`) | `CreateNamespace`/`DestroyNamespace` commands manage `NamespaceState` | ✅ Implemented |
| **Multi-pod per namespace** | **DONE** (`worker.rs`) | Worker manages N pods per namespace, each with TAP on shared fabric | ✅ Implemented |
| **DNS registry sync** | **DONE** (`fabric/dns.rs`, `gateway.rs`, `worker.rs`) | `RegistrySync`/`RegistryUpdate` push entries to gateway's DNS | ✅ Shared `DnsRegistry` wired to gateway DNS |
| **Fabric routing table** | No routing table concept | `FabricRouteSync`/`FabricRouteUpdate` with remote/placeholder destinations | Not needed for local mode but types should exist |
| **Pod output streaming** | **DONE** (`worker.rs`) | `PodOutput` events emitted by worker | ✅ Bridged IoSession into WorkerEvent stream |
| **Compose CLI integration** | Parser exists, no `compose up` command | CLI embeds orchestrator, drives worker via commands | Orchestrator loop done, CLI command still needed |

---

## Implementation Steps

### Step 1: Define WorkerCommand and WorkerEvent Rust types ✅ DONE

Implemented in `distvirt/src/worker.rs`. Types defined:

- `WorkerCommand` — `CreateNamespace`, `DestroyNamespace`, `RegistrySync`, `LaunchPod`, `StopPod`
- `WorkerEvent` — `NamespaceCreated`, `PodRunning`, `PodExited`, `PodFailed`, `PodOutput`
- Supporting types: `NetworkConfig`, `PodNetworkConfig`, `ContainerSpec`, `RegistryEntry`, `OutputStream`

Deferred from the spec (not needed for local mode):
- `RegistryUpdate` (incremental updates — RegistrySync is sufficient)
- `FabricRouteSync`/`FabricRouteUpdate` (all pods are local)
- `FabricRouteMiss` event

### Step 2: Build Worker struct ✅ DONE

Implemented in `distvirt/src/worker.rs`. The Worker manages:
- `HashMap<String, NamespaceState>` — each namespace owns its fabric, gateway, registry, and pods
- `event_tx: mpsc::Sender<WorkerEvent>` — events emitted to orchestrator
- `kernel_path` / `rootfs_image_path` — VM launch config

Key design decisions:
- Worker takes `vmm` and `image_provider` as parameters to `handle_command()` rather than owning them (avoids trait object boxing, keeps concrete types)
- `VmInstance` trait was updated to return `impl Future + Send` (from bare `async fn`) so spawned background tasks can hold `ManagedVm` instances
- Each pod gets a background `tokio::spawn` task for exit monitoring and log streaming

### Step 3: Refactor `run()` into Worker commands ✅ DONE

The inline logic from `orchestrate::run()` has been extracted into Worker command handlers:

- `handle_create_namespace()` — creates Fabric + FabricGateway, wires them together
- `handle_destroy_namespace()` — drops namespace state (aborts gateway task, drops fabric)
- `handle_registry_sync()` — replaces the namespace's ServiceRegistry
- `handle_launch_pod()` — prepares image, launches VM, takes TAP into fabric, configures network, adds/starts container, sets up log streaming, spawns exit monitor task
- `handle_stop_pod()` — aborts the pod's exit task, removes from namespace

The existing `ManagedVm`, `Fabric`, `FabricGateway` code is unchanged — Worker calls into them.

### Step 4: Wire DNS registry to gateway ✅ DONE

Implemented via a new `fabric/dns.rs` module and changes to `gateway.rs` and `worker.rs`:

- **`fabric/dns.rs`** (new) — `DnsRegistry` type alias (`Arc<RwLock<HashMap<String, Ipv4Addr>>>`), DNS wire format parsing (`parse_qname`), A-record response synthesis (`synthesize_a_response`), and convenience resolver (`try_resolve`). Includes 7 unit tests.
- **`fabric/gateway.rs`** — `FabricGateway` now holds a `DnsRegistry` (passed via `new()`). `process_dns_queries()` checks the local registry first; only falls back to upstream for unknown names.
- **`worker.rs`** — `NamespaceState.registry` changed from `ServiceRegistry` to `DnsRegistry`. `handle_create_namespace()` creates the shared registry and passes a clone to the gateway. `handle_registry_sync()` acquires a write lock, clears, and repopulates the map.
- **`orchestrate.rs`** — Updated legacy `FabricGateway::new()` call to pass an empty registry.

### Step 5: Build the local orchestrator loop ✅ DONE

Implemented as `worker::run_deployment()` in `distvirt/src/worker.rs`. The function:

1. Plans the deployment (`deployment::plan()`)
2. Creates an in-process Worker with an mpsc event channel
3. `CreateNamespace` with 172.16.0.0/24 subnet
4. `RegistrySync` with all service name→IP mappings from the plan
5. `LaunchPod` for each service in dependency order
6. Event loop that prints `PodOutput` with service-name prefixes, tracks `PodExited`/`PodFailed`
7. `DestroyNamespace` on completion

Still TODO:
- Wait for `PodRunning` events before launching dependents (currently launches sequentially which provides ordering but doesn't gate on readiness)
- Ctrl-C handling (`StopPod` for each pod → `DestroyNamespace`)

### Step 6: CLI `compose up` command

Thin wrapper that:
1. Parses compose file via `distvirt_compose::parse()`
2. Creates in-process worker (channels, not sockets)
3. Runs the orchestrator loop from step 5
4. Prints multiplexed logs with service name prefixes
5. Handles Ctrl-C gracefully

---

## What NOT to build yet

- Protobuf/gRPC wire format (channels are fine for local mode)
- Fabric routing table / `FabricRouteSync` (all pods are local in single-worker mode)
- Tunnel ports / multi-worker networking
- Suspend/resume, port forwarding, health checks
- `WorkerHello`/`WorkerAccepted` handshake (in-process worker doesn't need it)

---

## Changes to existing code

- **`vmm/mod.rs`**: `VmInstance` trait now uses `impl Future + Send` return types instead of `async fn`, and requires `'static` bound. This was necessary so `ManagedVm<I>` can be moved into `tokio::spawn` tasks for background exit monitoring.
- **`vmm/mod.rs`**: `NetConfig` now derives `Clone` (needed when building VmConfig for the worker).
- **`fabric/gateway.rs`**: `FabricGateway::new()` now takes a `DnsRegistry` parameter. DNS queries check local registry before upstream forwarding.
- **`fabric/mod.rs`**: Added `pub mod dns;`.
- **`orchestrate.rs`**: Updated `FabricGateway::new()` call to pass an empty registry.
