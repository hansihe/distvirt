# Compose Implementation Plan — Milestone 1

## Progress

| Step | Task | Status | Notes |
|------|------|--------|-------|
| 1 | Refactor orchestrate.rs → ManagedVm + core types | **Done** | `ManagedVm<I>` extracted (generic over VmInstance), `Deployment`/`ServiceSpec`/`ServiceRegistry` in `deployment.rs`. `ContainerConfig` and `merge_config` made public. `run()`/`run_with_image()` reimplemented on ManagedVm. FabricPort trait deferred — current Port + channel-based gateway works; trait needed when tunnel ports arrive. |
| 2 | Compose parser crate | **Done** | `distvirt-compose` crate with `parse(path) → Deployment`. Uses `compose_spec` crate. Extracts image, command, entrypoint, environment, ports, depends_on, hostname, user, working_dir. Warns on unsupported fields (build, volumes, healthcheck, restart, configs, secrets, networks). 11 unit tests. |
| 3 | Planning (IP assignment, ordering) | **Done** | `plan(deployment) → ExecutionPlan` in `deployment.rs`. Topological sort via Kahn's algorithm (alphabetical within same level, cycle-tolerant). IPs from 172.16.0.2+ (gateway .1), deterministic MACs `06:00:AC:10:00:{octet}`. Max 253 services (/24 subnet). 11 unit tests. |
| 4 | Worker + compose up | Not started | |
| 5 | smoltcp gateway | **Done** | `FabricGateway` with TUN egress, DNS forwarding, ARP via smoltcp. Integrated into fabric via channels. |
| 5a | DNS service discovery | Partial | Gateway has UDP :53 socket and upstream forwarding, but no `ServiceRegistry` lookup yet. |
| 6 | `/etc/resolv.conf` injection | **Done** | `AddContainer` has `dns_servers` field, guest-init writes resolv.conf. |
| 7 | Port forwarding (in-fabric) | Not started | |
| 8 | Stdout/stderr streaming | **Done** | Multi-connection vsock transport: control port 1024 + I/O port 1025. Pipe-based capture in guest, binary-framed streaming, host-side IoSession + LogCollector. |
| 9 | CLI commands | Not started | |

### Design deviations from original plan

- **ManagedVm is generic (`ManagedVm<I: VmInstance>`)** rather than holding `Box<dyn VmInstance>`. The VmInstance trait uses native async-in-trait and isn't object-safe. Generic approach works for milestone 1; dynamic dispatch (ServiceHandle trait, `Box<dyn ServiceHandle>`) will be introduced when Worker/RemoteVm need it.
- **FabricPort trait deferred.** The gateway integrates via mpsc channels, TAP ports use the concrete `Port` struct. This works well and avoids premature abstraction. The trait becomes useful when tunnel ports arrive for distributed mode.
- **`merge_config` made public** so compose can resolve image configs into `ContainerConfig` from outside orchestrate.rs.
- **`PlannedService` is simpler than originally planned.** No embedded `spec`, `port_forwards`, or `depends_on` fields — the service spec is looked up from `Deployment` by name, and port forwards will be added when step 7 is implemented.

---

## Goal

`distvirt compose up` launches a multi-service environment from a standard `compose.yaml`. Services run in individual Firecracker VMs, connected by the existing L2 fabric with DNS-based service discovery. No suspend/resume, no scale-to-zero — just "compose up starts everything, compose down stops everything."

---

## Vision alignment

This milestone builds local compose support, but the architecture is designed to align with the longer-term distributed vision:

- **Local mode** (milestone 1): Run an entire cluster of services on one machine. The CLI starts a worker in-process.
- **Distributed mode** (future): A central orchestrator coordinates N worker processes on different machines. Workers spawn VMs and run local fabric segments. The fabric operates peer-to-peer between workers (tunnel ports). The orchestrator manages service placement, scaling policy, and the service registry.
- **Splice mode** (future): Run some services locally, leave the rest in a remote cluster. A local worker joins the distributed fabric via a tunnel port, making local VMs appear on the same network as remote ones.
- **Scale-to-zero** (future): When a packet enters the fabric for an inactive service, the worker notifies the orchestrator, which allocates the workload. The smoltcp gateway holds the TCP connection while the VM boots.

The key architectural principle: **separate "what to run" from "where to run it" from "how to declare it."** Compose is one way to declare services. The worker is one place to run them. The orchestrator decides placement. These are independent concerns.

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
                                       │ "start service X"
                    ┌──────────────────▼──────────────────────────┐
                    │              Execution                       │
                    │  distvirt-worker: VMM, local fabric, VMs     │
                    │  (local = in-process, distributed = remote)  │
                    └─────────────────────────────────────────────┘
```

### Why L2

The fabric operates with L2 semantics (identity-based forwarding, learning) but optimizes the wire format. Guest VMs and smoltcp speak Ethernet natively, so intra-worker traffic is real Ethernet frames (TAP ↔ switch ↔ smoltcp). However, MACs are deterministic from IPs (e.g., `06:00:AC:10:00:{octet}`), which means the Ethernet header is redundant information on the inter-worker path.

**Intra-worker:** Real Ethernet frames. TAP devices and smoltcp require them.

**Inter-worker:** Raw IP packets in a thin tunnel header (source worker, fabric ID, QoS). No Ethernet header — the receiving worker reconstructs it from the deterministic IP→MAC mapping. ARP never crosses tunnels; each worker's switch answers ARP locally from the mapping. This eliminates broadcast traffic across the mesh.

**Scale-to-zero integration:** When a guest ARPs for a suspended service, the local switch answers immediately (the MAC is deterministic, no resolution needed). The guest sends a TCP SYN, which reaches the smoltcp gateway. The gateway holds the SYN, asks the orchestrator to wake the service, and once the VM boots, proxies or replays the connection. From the guest's perspective, nothing special happens — it's just a slow connection. The "service IP → worker" forwarding table is the conceptual equivalent of a MAC table, just without the overhead.

The conceptual model is still L2 — identity-based forwarding, learning which worker owns which service — but the wire format between workers is minimal.

### Crate layout

```
distvirt/                  CORE — orchestration primitives + fabric
  src/
    deployment.rs          ✅ Deployment, ServiceSpec, ServiceRegistry
    orchestrate.rs         ✅ ManagedVm<I>, ContainerConfig, run()/run_with_image()
    io_session.rs          ✅ Host-side IoSession (connect, handshake, frame decoding)
    log_collector.rs       ✅ LogCollector (multi-service log aggregation)
    worker.rs              TODO — Worker struct (owns VMM, fabric, runs VMs)
    fabric/
      mod.rs               Fabric struct, port management, gateway via channels
      port.rs              Port (async TAP wrapper, concrete struct)
      switch.rs            L2 switch (MacTable, frame parsing)
      gateway.rs           ✅ FabricGateway (smoltcp + TUN + DNS forwarding)
    vmm/                   Vmm/VmInstance traits, Firecracker impl
    image_provider/        ImageProvider trait, containerd + rootfs impls
    vsock_client.rs        GuestConnection (length-prefixed JSON over vsock)

distvirt-compose/          NEW — compose file parser ONLY
  src/
    lib.rs                 public API: parse() → Deployment
    parse.rs               compose.yaml → Deployment (via compose_spec)

distvirt-worker/           EXISTING stub — worker binary
  src/
    main.rs                worker process entry point
                           milestone 1: started in-process by CLI
                           future: standalone binary connecting to orchestrator

distvirt-guest-protocol/   EXISTING — shared types
    ✅ IoSessionRequest/Response, IoMode, stream constants, capture_output

guest-image/guest-init/    EXISTING — guest agent
    ✅ pipe-based capture, IoSessionManager, multi-fd poll, binary framing

distvirt-cli/              EXISTING — CLI
    add: `compose up`, `compose down`, `compose logs` commands
    depends on distvirt-compose, distvirt (for Worker)
```

### What lives where — and why

**`distvirt` (core)** owns:
- `Deployment` / `ServiceSpec` — the runtime representation of "a set of named services with IPs, ports, dependencies." This is what compose parses *into*, and what a distributed orchestrator would also produce. It's the shared vocabulary.
- `ServiceRegistry` — name→IP mapping. The DNS server queries this. The orchestrator owns the authoritative copy; workers hold a projected copy kept in sync via the worker protocol.
- `Worker` — a command-driven executor that owns a VMM, local fabric segments (one per namespace), and an image provider. It receives commands (`LaunchPod`, `CreateNamespace`, etc.) and emits events (`PodRunning`, `PodExited`, etc.). In milestone 1, the CLI embeds a trivial orchestrator that drives a Worker in-process via channels. In the distributed case, `distvirt-worker` is a standalone binary that creates a Worker and connects to a remote orchestrator. See `docs/worker-protocol.md`.
- `ManagedVm` — a local VM with vsock connection. The Worker uses this internally to manage pod VMs.
- `Fabric` + `FabricPort` trait — the L2 switch with pluggable port types (TAP, smoltcp, future: tunnel).
- smoltcp gateway, DNS server, port forwarding — fabric-level capabilities, not compose-specific.

**`distvirt-compose`** owns:
- Parsing `compose.yaml` via `compose_spec` crate
- Converting compose types into `Deployment` / `ServiceSpec`
- Validating and warning on unsupported fields
- Nothing else. No orchestration, no VM management, no networking.

**`distvirt-worker`** owns:
- The worker binary entry point
- In milestone 1: minimal, just starts a Worker in-process
- Future: connects to orchestrator via RPC, receives service placement commands, reports status

**`distvirt-cli`** owns:
- `compose up/down/logs` commands
- Creates a Worker, feeds it a Deployment, manages the lifecycle
- Future: could instead connect to a remote orchestrator

### Key abstractions

```rust
// distvirt/src/deployment.rs — the "what" ✅ IMPLEMENTED

pub struct Deployment {
    pub name: String,
    pub services: HashMap<String, ServiceSpec>,
}

pub struct ServiceSpec {
    pub image: String,
    pub command: Option<Vec<String>>,
    pub entrypoint: Option<Vec<String>>,
    pub environment: HashMap<String, String>,
    pub ports: Vec<PortMapping>,
    pub depends_on: Vec<Dependency>,
    pub hostname: Option<String>,
    pub user: Option<String>,
    pub working_dir: Option<String>,
}

pub struct ServiceRegistry {
    services: HashMap<String, Ipv4Addr>,
}
// Methods: new(), register(), lookup(), iter()
```

```rust
// distvirt/src/orchestrate.rs — the "how" (local execution) ✅ IMPLEMENTED

/// A launched VM with an established vsock connection.
/// Generic over VmInstance (static dispatch for now).
pub struct ManagedVm<I> {
    instance: I,
    conn: GuestConnection,
}

impl<I: VmInstance> ManagedVm<I> {
    pub async fn connect(instance: I) -> Result<Self>;          // vsock + wait Ready
    pub async fn configure_network(&mut self, ...) -> Result<()>;
    pub async fn add_container(&mut self, ...) -> Result<()>;
    pub async fn start_container(&mut self, ...) -> Result<u32>;
    pub async fn wait_container_exit(&mut self) -> Result<(String, i32)>;
    pub async fn stream_logs(&self, container_id: &str) -> Result<IoSession>; // ✅ step 8
    pub async fn shutdown(mut self) -> Result<()>;
}

// Future: ServiceHandle trait + RemoteVm for distributed mode
// Introduced when Worker needs dynamic dispatch over local vs remote VMs.
```

```rust
// distvirt/src/fabric/ — L2 switch + gateway ✅ IMPLEMENTED

// Fabric manages ports (concrete Port struct) and gateway (mpsc channels).
// Gateway is FabricGateway (smoltcp + TUN + DNS forwarding).
// FabricPort trait deferred — current design works, trait needed for tunnel ports.

// Future: FabricPort trait, TunnelPort for distributed fabric
```

```rust
// distvirt/src/worker.rs — the "where" (local execution engine) TODO
// See docs/worker-protocol.md for the full protocol design.

pub struct Worker {
    vmm: Box<dyn Vmm>,
    image_provider: Box<dyn ImageProvider>,
    namespaces: HashMap<String, NamespaceState>,  // namespace_id → local state
}

struct NamespaceState {
    fabric: Fabric,
    gateway: FabricGateway,
    registry: ServiceRegistry,        // projected copy, orchestrator-owned
    pods: HashMap<String, ManagedVm>, // pod_id → running VM
}

// Worker is command-driven. It receives WorkerCommand, emits WorkerEvent.
// No planning, no scheduling — just executes what the orchestrator tells it.
impl Worker {
    pub async fn handle_command(&mut self, cmd: WorkerCommand) -> Result<()>;
    pub async fn recv_event(&mut self) -> WorkerEvent;
    pub async fn shutdown(self) -> Result<()>;
}
```

---

## Implementation Steps

### Step 1: Refactor `orchestrate.rs` + introduce core abstractions — ✅ DONE

Extracted the single-VM lifecycle into `ManagedVm<I>` (generic over VmInstance). Introduced `Deployment`, `ServiceSpec`, `ServiceRegistry` in `deployment.rs`. Made `ContainerConfig` and `merge_config` public. Reimplemented `run()`/`run_with_image()` on top of ManagedVm.

**Deferred:** FabricPort trait — the current concrete Port + channel-based gateway integration works well. The trait abstraction is needed when tunnel ports arrive for distributed mode, not before.

**Deferred:** ServiceHandle trait — ManagedVm is generic (static dispatch) rather than trait-object-based. VmInstance uses native async-in-trait which isn't object-safe. Dynamic dispatch will be introduced when Worker needs to handle both local and remote VMs.

**Validated:** builds clean, no warnings. Existing `run` and `run-image` CLI commands unchanged.

### Step 2: Add `distvirt-compose` crate — parser only — ✅ DONE

`distvirt-compose` crate implemented with `compose_spec = "0.3"` dependency. Single public API: `parse(path: &Path) -> Result<Deployment>`.

The `parse()` function:
1. Reads and deserializes the compose file via `compose_spec`
2. Derives deployment name from compose `name` field or parent directory
3. Warns on unsupported fields (build, volumes, healthcheck, restart, configs, secrets, networks)
4. Converts `compose_spec` types into core `Deployment` / `ServiceSpec`
5. Handles command (string and list), environment, ports (TCP/UDP), depends_on

**Validated:** 11 unit tests covering minimal parse, name derivation, command formats, environment, ports, dependencies, missing image validation, and full service extraction.

### Step 3: Planning — IP assignment and port mapping — ✅ DONE

Lives in core (`distvirt/src/deployment.rs`) since it's not compose-specific.

```rust
pub struct ExecutionPlan {
    pub services: Vec<PlannedService>,
}

pub struct PlannedService {
    pub name: String,
    pub ip: Ipv4Addr,
    pub mac: [u8; 6],
}

pub fn plan(deployment: &Deployment) -> Result<ExecutionPlan>;
```

The planner:
1. Orders services via Kahn's algorithm (topological sort, alphabetical within same dependency level, deterministic output)
2. Assigns IPs from 172.16.0.0/24 subnet (gateway .1, services .2, .3, ...)
3. Derives MAC addresses deterministically (`06:00:AC:10:00:{last_octet}`)
4. Validates max 253 services (per /24 subnet)
5. Cycles don't cause failure — remaining services appended alphabetically with warning
6. Unknown dependencies ignored gracefully

**Note:** `PlannedService` is lighter than originally planned — no `spec`, `port_forwards`, or `depends_on` fields. The service spec is looked up from the `Deployment` by name. Port forwards will be added when step 7 is implemented.

**Validated:** 11 unit tests covering single/multi-service IP assignment, dependency ordering, diamond dependencies, cycle detection, service limits, unknown dependencies.

### Step 4: Worker + `compose up` — multi-VM orchestration

**Updated:** The Worker now follows the worker protocol design (see `docs/worker-protocol.md`). The Worker is a dumb executor driven by commands from the orchestrator. For milestone 1, the CLI acts as a trivial in-process orchestrator that sends commands via channels.

The Worker receives `WorkerCommand` messages and emits `WorkerEvent` messages. It does NOT do planning, IP assignment, or dependency ordering — that's the orchestrator's job.

```rust
// distvirt/src/worker.rs

impl Worker {
    pub fn new(config: WorkerConfig) -> Self;

    /// Process a single command from the orchestrator.
    /// Returns events produced in response.
    pub async fn handle_command(&mut self, cmd: WorkerCommand) -> Result<()>;

    /// Receive the next event from the worker (pod started, exited, output, etc.)
    pub async fn recv_event(&mut self) -> WorkerEvent;

    pub async fn shutdown(self) -> Result<()>;
}
```

The CLI's `compose up` flow (CLI acts as orchestrator):
1. Parse compose file → `Deployment`
2. Plan → `ExecutionPlan` (IP/MAC assignment, dependency ordering)
3. Create a `Worker` in-process
4. Send `CreateNamespace` with network config
5. Send `RegistrySync` with all service name→IP mappings
6. For each pod (respecting dependency order):
   a. Send `LaunchPod { namespace, pod_id, ip, mac, containers }`
   b. Wait for `PodRunning` event before launching dependents
7. Stream `PodOutput` events to terminal
8. On Ctrl-C, send `StopPod` for each pod, then `DestroyNamespace`

**Validation:** launch a two-service compose file (e.g., alpine pinging alpine), verify both start, verify fabric connects them.

### Step 5: Userspace IP stack in the fabric (smoltcp) — ✅ DONE

`FabricGateway` implemented with smoltcp interface, TUN device for internet egress, DNS forwarding to upstream servers, and ARP handling. Integrated into fabric via mpsc channels (not FabricPort trait). Gateway runs as a spawned tokio task.

```rust
// distvirt/src/fabric/gateway.rs

pub struct FabricGateway {
    iface: smoltcp::iface::Interface,
    // smoltcp sockets: UDP for DNS, TCP for port forwards
}
```

The gateway is a smoltcp `Interface` that:
- Has IP 172.16.0.1 and the gateway MAC
- Is wired into the fabric via the `FabricPort` trait
- Hosts sockets: UDP :53 for DNS, TCP listeners for port forwards

The existing ARP responder in `switch.rs` gets replaced by smoltcp's built-in ARP handling.

#### Two roles at the gateway identity

The fabric's gateway IP (172.16.0.1) serves two distinct roles:

1. **smoltcp gateway** — handles traffic *for* the gateway itself: DNS queries (UDP :53), port forward connections (TCP to specific ports), ARP. Fully in-process.

2. **Uplink TAP** — handles traffic *through* the gateway: egress to the internet. A real TAP device with host kernel NAT.

The switch logic for frames destined to the gateway MAC:
- If it matches a smoltcp socket (DNS :53, a registered port forward) → deliver to smoltcp
- Otherwise → forward to the uplink TAP for kernel routing

This means VMs get full internet access (through uplink TAP + host NAT) while DNS and port forwarding stay fully in-process (through smoltcp).

#### DNS server

The DNS server queries the `ServiceRegistry` (which lives in core, not compose):

- Receive UDP packets on the smoltcp socket bound to :53
- Parse DNS query (use `simple-dns` crate, or hand-roll for A-record-only)
- Look up service name in `ServiceRegistry`
- Return A record or NXDOMAIN
- Upstream forwarding (external DNS) is out of scope for milestone 1

**Validation:** launch a VM, `nslookup service-name 172.16.0.1` returns correct IP.

### Step 6: `/etc/resolv.conf` injection — ✅ DONE

`AddContainer` message has `dns_servers: Vec<String>` field. Guest-init writes `/etc/resolv.conf` with the specified nameservers after mounting the container filesystem.

### Step 7: Port forwarding (in-fabric)

Port forwarding uses the smoltcp gateway from step 5. Lives in core since it's a fabric capability, not compose-specific.

Flow per forwarded port:
1. Bind a real `TcpListener` on `host_ip:host_port` (Tokio, on the host's network stack)
2. On accept: open a TCP connection from the smoltcp gateway (172.16.0.1) to `target_ip:target_port` through the fabric
3. Bidirectionally copy bytes between the host TCP stream and the smoltcp TCP socket
4. The VM sees an incoming connection from 172.16.0.1 — entirely within the fabric

Future benefit: for scale-to-zero, the host listener can hold incoming connections while the target VM boots.

**Validation:** compose file with `ports: ["8080:80"]`, curl localhost:8080 reaches the container.

### Step 8: Stdout/stderr streaming — ✅ DONE

Uses a **multi-connection vsock transport**: the existing control channel (port 1024) stays as-is for JSON control messages, and a new I/O port (1025) accepts per-container streaming sessions. This cleanly separates control from data and supports future PTY attach sessions.

**Protocol changes (`distvirt-guest-protocol`):**
- `VSOCK_CONTROL_PORT` (1024) and `VSOCK_IO_PORT` (1025) constants (`VSOCK_PORT` kept as compat alias)
- `capture_output: bool` field on `StartContainer` (serde default false)
- `IoSessionRequest`/`IoSessionResponse` for handshake (length-prefixed JSON)
- Binary I/O frame format after handshake: `[1 byte stream_id][2 bytes LE length][payload]`
  - stream_id: 0=EOF, 1=stdout, 2=stderr. Max payload 8192 bytes.

**Guest-init changes:**
- `container.rs`: When `capture_output=true`, creates `pipe()` pairs for stdout/stderr. Child dup2's write ends to fd 1/2 (stdin gets `/dev/null`). Parent stores non-blocking read ends. When `capture_output=false`, legacy `/dev/console` behavior preserved.
- `io_session.rs` (new): `IoSessionManager` owns VsockListener on port 1025. Accepts sessions, performs handshake, forwards pipe data as binary frames. Buffers up to 64KB per stream when no session connected (drops oldest on overflow). Sends EOF frame on container exit.
- `main.rs`: Refactored to dynamic multi-fd poll: control vsock + signalfd + I/O listener + per-container pipe fds + per-session fds. On child exit: drains pipes, sends EOF, closes fds.
- `vsock.rs`: Added `accept_nonblocking()`, `as_raw_fd()` on VsockListener, `write_raw()` on VsockStream.

**Host-side changes:**
- `io_session.rs` (new): `IoSession` with async connect + handshake + binary frame decoding. `IoEvent` enum: `Stdout(Vec<u8>)`, `Stderr(Vec<u8>)`, `Eof`.
- `orchestrate.rs`: `ManagedVm::stream_logs(container_id)` connects to I/O port and returns an `IoSession`. `ContainerConfig` has `capture_output` field (defaults false in existing config builders).
- `log_collector.rs` (new): `LogCollector` aggregates output from multiple containers via tokio mpsc. `collect()` spawns a task per IoSession forwarding events as `LogLine { service, stream, data }`.

**Design for future extension (not implemented):**
- PTY attach: `IoMode::Attach` — guest allocates PTY, bidirectional raw bytes
- Stdin: `stream_id=3` for host→guest stdin
- Exec: `ExecInContainer` control message + virtual container ID for I/O session

**Validated:** builds clean, no warnings.

### Step 9: CLI commands

```rust
#[derive(Subcommand)]
enum ComposeCommand {
    Up {
        #[arg(short, long, default_value = "compose.yaml")]
        file: PathBuf,
        #[arg(long)]
        kernel: PathBuf,
        #[arg(long)]
        rootfs_image: PathBuf,
    },
    Down { ... },
    Logs { ... },
}
```

`compose up` flow:
1. Parse compose file → `Deployment`
2. Plan → `ExecutionPlan`
3. Create `Worker` in-process
4. Start DNS, port forwards on the worker's fabric
5. Launch services via worker
6. Foreground: stream logs, wait for ctrl-c, then `worker.shutdown()`

Foreground-only for milestone 1. Detach mode is a follow-up.

---

## Dependency graph (implementation order)

```
Step 1: Refactor orchestrate.rs → ManagedVm + core types              ✅ DONE
  │
  ├── Step 2: Compose parser                                           ✅ DONE
  │     │
  │     └── Step 3: Planning / IP assignment (in core)                 ✅ DONE
  │
  ├── Step 5: smoltcp gateway                                          ✅ DONE
  │     │
  │     ├── Step 5a: DNS service discovery (wire ServiceRegistry in)   ← needs Step 4
  │     │
  │     └── Step 7: Port forwarding (fabric capability)
  │
  ├── Step 6: resolv.conf injection                                    ✅ DONE
  │
  └── Step 4: Worker + compose up (needs steps 2, 3)                   ← UNBLOCKED, NEXT
        │
        └── Step 8: Stdout/stderr streaming                            ✅ DONE
              │
              └── Step 9: CLI commands (integrates everything)
```

**Next up:** Step 4 (Worker + compose up) is now unblocked. Steps 2, 3, and 8 are complete.

---

## How this extends to distributed mode (future)

The architecture is designed so the distributed case is "put the orchestrator in a server, put workers on N machines, add RPC between them" — not a rewrite.

**What changes for distributed:**
- `distvirt-worker` binary becomes a standalone process that connects to a remote orchestrator over TCP/TLS instead of being driven in-process via channels
- `FabricPort` gets a `TunnelPort` implementation for inter-worker traffic
- The orchestrator gains scheduling policy (which worker runs which pod), fabric route management, and autoscaling
- Workers connect to the orchestrator (not the other way around) — see `docs/worker-protocol.md`

**What stays the same:**
- `Worker` struct and the `WorkerCommand`/`WorkerEvent` protocol — same messages whether local or remote
- `ManagedVm` and the vsock guest protocol
- `Fabric` and `FabricPort` trait
- `Deployment` / `ServiceSpec` / `ExecutionPlan` (orchestrator-side)
- smoltcp gateway, DNS, port forwarding
- Guest agent, protocol, image providers

**Splice mode:**
- A local worker joins a remote fabric by adding a `TunnelPort` to its fabric
- The local `ServiceRegistry` merges with the remote one
- Local VMs appear on the same network as remote VMs
- The CLI connects to the remote orchestrator to register its local worker

---

## Out of scope (milestone 2+)

- `build:` section — building images from Dockerfiles
- `volumes:` — named volumes, bind mounts between host and VM
- `healthcheck` + `depends_on: condition: service_healthy`
- `restart` policies (restart on crash)
- `configs` / `secrets`
- Resource limits (`mem_limit`, `cpus` → VM config)
- Detached mode / daemonization
- External DNS forwarding (resolve non-service hostnames)
- `compose exec` / `compose run` (exec into running container)
- Multiple networks (all services share one L2 segment for now)
- Scale-to-zero / suspend / resume
- Distributed mode / remote workers
- Splice mode

---

## Crate dependency graph

```
distvirt-guest-protocol    (serde only, no std ok)
       │
       ├──── guest-init         (libc, serde, protocol)
       │
       ├──── distvirt           (tokio, smoltcp, containerd, protocol)
       │         │                 Deployment, Worker, Fabric, ServiceHandle
       │         │
       │         ├──── distvirt-compose  (compose_spec → Deployment, thin parser)
       │         │
       │         └──── distvirt-worker   (binary, creates Worker, future: RPC to orchestrator)
       │                    │
       └────────────────────┴──── distvirt-cli  (clap, distvirt, distvirt-compose)
```
