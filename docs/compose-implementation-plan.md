# Compose Implementation Plan — Milestone 1

## Progress

| Step | Task | Status |
|------|------|--------|
| 1 | Refactor orchestrate.rs → ManagedVm + core types | **Done** |
| 2 | Compose parser crate | **Done** |
| 3 | Planning (IP assignment, ordering) | **Done** |
| 4 | Worker (generic, owns Vmm + ImageProvider) | **Done** |
| 5 | smoltcp gateway (ARP, TUN egress, DNS forwarding) | **Done** |
| 5a | DNS service discovery (ServiceRegistry lookup) | **Done** |
| 6 | `/etc/resolv.conf` injection | **Done** |
| 7 | Port forwarding (host → container) | Not started |
| 8 | Stdout/stderr streaming | **Done** |
| 9 | CLI `compose up` command | **Done** |

### Design deviations from original plan

- **ManagedVm is generic (`ManagedVm<I: VmInstance>`)** rather than `Box<dyn VmInstance>`. VmInstance uses async-in-trait and isn't object-safe. Dynamic dispatch deferred until Worker needs local vs remote VMs.
- **FabricPort trait deferred.** Gateway integrates via mpsc channels, TAP ports use concrete `Port` struct. Trait needed when tunnel ports arrive.
- **`PlannedService` is simpler than originally planned.** No embedded `spec`, `port_forwards`, or `depends_on` — service spec is looked up from `Deployment` by name.
- **`LogCollector` exists but Worker inlines its own log streaming** via `WorkerEvent::PodOutput` sends. LogCollector may be useful for the CLI layer.
- **Orchestration logic lives in CLI, not Worker.** `run_deployment()` / `LocalOrchestratorConfig` removed from `worker.rs`. The Worker is a pure executor; orchestration (plan → command sequencing → event loop) is in `distvirt-cli/src/main.rs` `ComposeUp` handler. `Worker` is generic: `Worker<V: Vmm, P: ImageProvider>`, owning its vmm/image_provider.

---

## Goal

`distvirt compose up` launches a multi-service environment from a standard `compose.yaml`. Services run in individual Firecracker VMs, connected by the existing L2 fabric with DNS-based service discovery. No suspend/resume, no scale-to-zero — just "compose up starts everything, compose down stops everything."

---

## Architecture

### Separation of concerns

```
                    ┌─────────────────────────────────────────────┐
                    │              Declarations                    │
                    │  compose.yaml → distvirt-compose (parser)    │
                    │  future: API calls, CLI, custom configs      │
                    └──────────────────┬──────────────────────────┘
                                       │ Deployment spec
                    ┌──────────────────▼──────────────────────────┐
                    │             Orchestration                    │
                    │  distvirt core: Deployment, ServiceRegistry, │
                    │  planning (IP assignment, ordering)           │
                    └──────────────────┬──────────────────────────┘
                                       │ WorkerCommand
                    ┌──────────────────▼──────────────────────────┐
                    │              Execution                       │
                    │  Worker: VMM, local fabric, VMs              │
                    │  (local = in-process, distributed = remote)  │
                    └─────────────────────────────────────────────┘
```

### Crate layout

```
distvirt/                  CORE — orchestration primitives + fabric
  src/
    deployment.rs          ✅ Deployment, ServiceSpec, ServiceRegistry, plan()
    orchestrate.rs         ✅ ManagedVm<I>, ContainerConfig, run()/run_with_image()
    worker.rs              ✅ Worker<V,P>, WorkerCommand/WorkerEvent (pure executor)
    io_session.rs          ✅ IoSession (vsock I/O streaming)
    log_collector.rs       ✅ LogCollector (multi-service log aggregation)
    fabric/
      mod.rs               Fabric struct, port management
      port.rs              FramePort trait, Port (TAP-backed async L2)
      switch.rs            L2 switch (MacTable, frame parsing)
      gateway.rs           ✅ FabricGateway (smoltcp + TUN + DNS)
      dns.rs               ✅ DnsRegistry, DNS wire parsing, A-record synthesis
    vmm/                   Vmm/VmInstance traits, Firecracker impl
    image_provider/        ImageProvider trait, containerd + rootfs impls

distvirt-compose/          Compose file parser ONLY
  src/lib.rs               ✅ parse(path) → Deployment

distvirt-worker/           Worker binary (placeholder — prints hello world)
  src/main.rs              future: standalone binary connecting to orchestrator

distvirt-cli/              CLI binary + orchestration logic
  src/main.rs              ✅ build-image, run, run-image, compose-up
```

---

## Remaining Steps

### Step 7: Port forwarding (host → container)

Port forwarding uses the smoltcp gateway. Lives in core since it's a fabric capability, not compose-specific.

Flow per forwarded port:
1. Bind a real `TcpListener` on `host_ip:host_port` (Tokio, on the host's network stack)
2. On accept: open a TCP connection from the smoltcp gateway (172.16.0.1) to `target_ip:target_port` through the fabric
3. Bidirectionally copy bytes between the host TCP stream and the smoltcp TCP socket
4. The VM sees an incoming connection from 172.16.0.1 — entirely within the fabric

`PortMapping` is already parsed from compose files and stored on `ServiceSpec`, but nothing uses it at runtime yet.

**Validation:** compose file with `ports: ["8080:80"]`, curl localhost:8080 reaches the container.

### Step 9: CLI `compose up` command — **Done**

Implemented as `Commands::ComposeUp` in `distvirt-cli/src/main.rs`. Orchestration logic (previously `run_deployment()` in `worker.rs`) now lives here on the CLI side of the worker protocol boundary.

`compose-up` flow:
1. Parse compose file → `Deployment` (via `distvirt_compose::parse`)
2. `deployment::plan()` → `ExecutionPlan`
3. Create `Worker::new(kernel, rootfs, event_tx, vmm, image_provider)`
4. Send `CreateNamespace`, `RegistrySync`, `LaunchPod` commands
5. Event loop printing `{pod_id} | {line}`, waiting for all exits
6. Send `DestroyNamespace`

Helper: `build_container_config(spec: &ServiceSpec) -> ContainerConfig` extracts the entrypoint/args/env mapping logic.

Foreground-only for milestone 1. Detach mode is a follow-up.

---

## How this extends to distributed mode (future)

**What changes:**
- `distvirt-worker` binary becomes standalone, connects to remote orchestrator over TCP/TLS
- `FabricPort` gets a `TunnelPort` implementation for inter-worker traffic
- Orchestrator gains scheduling policy, fabric route management, autoscaling
- Workers connect to orchestrator (not the other way around)

**What stays the same:**
- `Worker` struct and `WorkerCommand`/`WorkerEvent` protocol
- `ManagedVm` and vsock guest protocol
- `Fabric` and port abstractions
- `Deployment` / `ServiceSpec` / `ExecutionPlan`
- smoltcp gateway, DNS, port forwarding
- Guest agent, protocol, image providers

**Splice mode:** A local worker joins a remote fabric via `TunnelPort`. Local VMs appear on the same network as remote ones.

---

## Out of scope (milestone 2+)

- `build:` — building images from Dockerfiles
- `volumes:` — named volumes, bind mounts
- `healthcheck` + `depends_on: condition: service_healthy`
- `restart` policies
- `configs` / `secrets`
- Resource limits (`mem_limit`, `cpus`)
- Detached mode / daemonization
- `compose exec` / `compose run`
- Multiple networks
- Scale-to-zero / suspend / resume
- Distributed mode / remote workers
- Splice mode
