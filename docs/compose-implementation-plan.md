# Compose Implementation Plan — Milestone 1

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
    deployment.rs          NEW — Deployment, ServiceSpec, ServiceRegistry
    orchestrate.rs         refactor: extract ManagedVm as trait impl
    worker.rs              NEW — Worker struct (owns VMM, fabric, runs VMs)
    fabric/
      mod.rs               refactor: FabricPort trait
      port.rs              TapPort implements FabricPort
      switch.rs            existing L2 switch (uses FabricPort trait)
      gateway.rs           NEW — smoltcp-based gateway (implements FabricPort)
    vmm/                   (no changes expected)
    image_provider/        (no changes expected)
    vsock_client.rs        (no changes expected)

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
    add: Output message for stdout/stderr streaming

guest-image/guest-init/    EXISTING — guest agent
    add: pipe stdout/stderr, send Output messages over vsock

distvirt-cli/              EXISTING — CLI
    add: `compose up`, `compose down`, `compose logs` commands
    depends on distvirt-compose, distvirt (for Worker)
```

### What lives where — and why

**`distvirt` (core)** owns:
- `Deployment` / `ServiceSpec` — the runtime representation of "a set of named services with IPs, ports, dependencies." This is what compose parses *into*, and what a distributed orchestrator would also produce. It's the shared vocabulary.
- `ServiceRegistry` — name→IP mapping. The DNS server queries this. Compose populates it, but so could a distributed orchestrator or a splice command.
- `Worker` — a struct that owns a VMM, a local fabric segment, and an image provider. It receives "start service X" commands and executes them locally. In milestone 1, the CLI creates a Worker in-process. In the distributed case, `distvirt-worker` is a standalone binary that creates a Worker and connects to a remote orchestrator.
- `ManagedVm` — a local VM with vsock connection. Implements a `ServiceHandle` trait so the orchestrator doesn't assume locality.
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
// distvirt/src/deployment.rs — the "what"

/// A set of services to run. Source-agnostic — could come from compose,
/// API calls, or a distributed orchestrator.
pub struct Deployment {
    pub name: String,
    pub services: IndexMap<String, ServiceSpec>,
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

/// Name→IP mapping, shared between DNS server, orchestrator, and workers.
pub struct ServiceRegistry {
    services: HashMap<String, Ipv4Addr>,
}
```

```rust
// distvirt/src/orchestrate.rs — the "how" (local execution)

/// Trait for interacting with a running service, regardless of location.
#[async_trait]
pub trait ServiceHandle {
    async fn configure_network(&mut self, cfg: NetConfig) -> Result<()>;
    async fn add_container(&mut self, id: &str, device: &str, dns: &[String]) -> Result<()>;
    async fn start_container(&mut self, cfg: ContainerConfig) -> Result<u32>;
    async fn shutdown(self: Box<Self>) -> Result<()>;
}

/// Local implementation — a VM on this machine with a vsock connection.
pub struct ManagedVm {
    instance: Box<dyn VmInstance>,
    vsock: GuestConnection,
}

impl ServiceHandle for ManagedVm { ... }

// Future: RemoteVm that talks to a worker over RPC
// pub struct RemoteVm { worker_client: WorkerClient, vm_id: VmId }
// impl ServiceHandle for RemoteVm { ... }
```

```rust
// distvirt/src/fabric/mod.rs — pluggable port abstraction

/// A source/sink of Ethernet frames on the fabric.
#[async_trait]
pub trait FabricPort: Send {
    async fn recv_frame(&mut self) -> Result<BytesMut>;
    async fn send_frame(&mut self, frame: &[u8]) -> Result<()>;
}

/// TAP device port (existing, refactored to implement trait).
pub struct TapPort { ... }
impl FabricPort for TapPort { ... }

/// smoltcp gateway port (new).
pub struct SmoltcpPort { ... }
impl FabricPort for SmoltcpPort { ... }

// Future: tunnel port for distributed fabric
// pub struct TunnelPort { ... }  // WireGuard/VXLAN to remote worker
// impl FabricPort for TunnelPort { ... }
```

```rust
// distvirt/src/worker.rs — the "where" (local execution engine)

/// A worker manages VMs and a local fabric segment on one machine.
pub struct Worker {
    vmm: Box<dyn Vmm>,
    image_provider: Box<dyn ImageProvider>,
    fabric: Fabric,
    registry: ServiceRegistry,
    // smoltcp gateway, DNS, port forwards
}

impl Worker {
    /// Start a service on this worker.
    pub async fn start_service(&mut self, name: &str, spec: &ServiceSpec) -> Result<Box<dyn ServiceHandle>>;

    /// Stop a service.
    pub async fn stop_service(&mut self, name: &str) -> Result<()>;

    /// Shut down all services and the fabric.
    pub async fn shutdown(self) -> Result<()>;
}
```

---

## Implementation Steps

### Step 1: Refactor `orchestrate.rs` + introduce core abstractions

Extract the single-VM lifecycle into `ManagedVm` implementing `ServiceHandle`. Introduce the `Deployment`, `ServiceSpec`, and `ServiceRegistry` types. Keep the existing `run()` working as-is by reimplementing it on top of the new primitives.

Also introduce the `FabricPort` trait and refactor the existing TAP port handling to implement it. The switch logic uses `FabricPort` instead of directly managing TAP file descriptors.

This is a refactor — no new functionality, no behavior change.

**Validation:** existing `distvirt run` and `distvirt run-image` CLI commands still work identically.

### Step 2: Add `distvirt-compose` crate — parser only

Add `compose_spec` dependency. The crate has one job: parse a compose file into a `Deployment`.

```rust
// distvirt-compose/src/lib.rs

pub fn parse(path: &Path) -> Result<Deployment>;
```

The `parse()` function:
1. Reads and deserializes the compose file via `compose_spec`
2. Validates that all referenced images exist / are pullable
3. Warns on unsupported fields (anything we don't extract)
4. Converts `compose_spec` types into core `Deployment` / `ServiceSpec`

**Validation:** unit tests parsing real compose files, round-trip checks.

### Step 3: Planning — IP assignment and port mapping

This lives in core (`distvirt`) since it's not compose-specific — any deployment source needs IP assignment.

```rust
// distvirt/src/deployment.rs (or a planning module)

pub struct ExecutionPlan {
    pub services: Vec<PlannedService>,
}

pub struct PlannedService {
    pub name: String,
    pub spec: ServiceSpec,
    pub ip: Ipv4Addr,
    pub mac: [u8; 6],
    pub port_forwards: Vec<PortForward>,
    pub depends_on: Vec<String>,
}

pub fn plan(deployment: &Deployment) -> Result<ExecutionPlan>;
```

The planner:
1. Orders services so dependencies come first (best-effort topological order, not strict DAG — cycles aren't fatal)
2. Assigns IPs from 172.16.0.0/24 subnet (gateway .1, services .2, .3, ...)
3. Derives MAC addresses deterministically
4. Resolves port mappings

**Validation:** unit tests for ordering, IP assignment.

### Step 4: Worker + `compose up` — multi-VM orchestration

Introduce the `Worker` struct and wire up `compose up`.

```rust
// distvirt/src/worker.rs

impl Worker {
    pub fn new(vmm: Box<dyn Vmm>, image_provider: Box<dyn ImageProvider>) -> Self;
    pub async fn start_service(&mut self, name: &str, spec: &ServiceSpec, plan: &PlannedService) -> Result<()>;
    pub async fn shutdown(self) -> Result<()>;
}
```

The CLI's `compose up` flow:
1. Parse compose file → `Deployment`
2. Plan → `ExecutionPlan`
3. Create a `Worker` (in-process for milestone 1)
4. For each service (respecting dependency order):
   a. `worker.start_service(name, spec, plan)` which internally:
      - `image_provider.prepare(image)`
      - `ManagedVm::start(vmm, config)`
      - Take TAP → add to fabric as `TapPort`
      - Register in `ServiceRegistry`
      - Configure network, add container, start container
5. Wait for ctrl-c, then `worker.shutdown()`

**Validation:** launch a two-service compose file (e.g., alpine pinging alpine), verify both start, verify fabric connects them.

### Step 5: Userspace IP stack in the fabric (smoltcp)

The fabric currently operates at L2 only. Add `smoltcp` as a userspace TCP/IP stack, attached to the fabric as a `SmoltcpPort` (implementing `FabricPort`) with the gateway identity (IP 172.16.0.1, MAC 02:00:00:00:00:01).

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

### Step 6: `/etc/resolv.conf` injection

Protocol extension — the host tells the guest what DNS server to use:

```
AddContainer { id, device, dns_servers: Vec<String> }
```

Guest agent writes `/containers/<id>/etc/resolv.conf` after mount:
```
nameserver 172.16.0.1
```

### Step 7: Port forwarding (in-fabric)

Port forwarding uses the smoltcp gateway from step 5. Lives in core since it's a fabric capability, not compose-specific.

Flow per forwarded port:
1. Bind a real `TcpListener` on `host_ip:host_port` (Tokio, on the host's network stack)
2. On accept: open a TCP connection from the smoltcp gateway (172.16.0.1) to `target_ip:target_port` through the fabric
3. Bidirectionally copy bytes between the host TCP stream and the smoltcp TCP socket
4. The VM sees an incoming connection from 172.16.0.1 — entirely within the fabric

Future benefit: for scale-to-zero, the host listener can hold incoming connections while the target VM boots.

**Validation:** compose file with `ports: ["8080:80"]`, curl localhost:8080 reaches the container.

### Step 8: Stdout/stderr streaming

**Protocol extension:**

```rust
// distvirt-guest-protocol
GuestMessage::Output { id: String, stream: u8, data: Vec<u8> }
// stream: 1 = stdout, 2 = stderr
```

**Guest agent changes:**
- Create pipes for stdout and stderr before `fork()`
- Child: dup2 pipe write ends to fd 1 and 2
- Parent: add pipe read ends to the `poll()` set
- On readable: read into buffer, send `Output` message over vsock

**Host-side:** The `ManagedVm` runs a background message-dispatching loop:
- `ContainerExited` → notify the worker
- `Output` → forward to log collector
- `Error` → forward to log collector

The log collector aggregates output from all VMs, prefixes with service name, writes to terminal.

**Validation:** `distvirt compose logs` shows interleaved output from multiple services.

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
Step 1: Refactor orchestrate.rs → ManagedVm/ServiceHandle + FabricPort trait
  │
  ├── Step 2: Compose parser (independent of step 1)
  │     │
  │     └── Step 3: Planning / IP assignment (in core)
  │
  ├── Step 5: smoltcp gateway as FabricPort (independent of compose)
  │     │
  │     ├── Step 5a: DNS server (queries ServiceRegistry)
  │     │
  │     └── Step 7: Port forwarding (fabric capability)
  │
  ├── Step 6: resolv.conf injection (protocol + guest agent)
  │
  └── Step 4: Worker + compose up (needs steps 1, 2, 3, 5)
        │
        └── Step 8: Stdout/stderr streaming (protocol + guest agent)
              │
              └── Step 9: CLI commands (integrates everything)
```

Steps 1, 2, 5, 6, 8 touch different parts of the codebase and can be developed concurrently. Step 5 (smoltcp gateway) is the most significant new infrastructure.

---

## How this extends to distributed mode (future)

The architecture is designed so the distributed case is "put the orchestrator in a server, put workers on N machines, add RPC between them" — not a rewrite.

**What changes for distributed:**
- `distvirt-worker` binary becomes a standalone process that connects to a remote orchestrator instead of being started in-process by the CLI
- `ServiceHandle` gets a `RemoteVm` implementation that talks to a worker over RPC
- `FabricPort` gets a `TunnelPort` implementation (WireGuard/VXLAN) for inter-worker traffic
- `ServiceRegistry` becomes distributed (orchestrator is the source of truth, workers cache)
- The orchestrator gains scheduling policy (which worker runs which service) and autoscaling

**What stays the same:**
- `Worker` struct and its local VM management
- `ManagedVm` and the vsock protocol
- `Fabric` and `FabricPort` trait
- `Deployment` / `ServiceSpec` / `ExecutionPlan`
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
