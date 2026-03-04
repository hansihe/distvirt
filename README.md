# distvirt

A VM-based container runtime built for transparent scale-to-zero. Runs containers inside Firecracker microVMs that can be suspended, resumed, and activated on demand — with protocol-aware network buffering so callers never know the target was asleep.

## Vision

VMs give you something containers can't: **suspend and resume**. Snapshot a running service to disk and bring it back later with its full in-memory state intact. Combined with a network fabric that buffers traffic while a service activates, this enables transparent scale-to-zero — existing applications work without modification, services spin up on demand when traffic arrives, and callers just see a slightly slow connection.

**Scale-to-zero staging environments** — deploy a full multi-service application where idle services automatically suspend. When a developer visits the environment, services activate on demand as traffic flows through. Clone an environment instantly as a control-plane operation; actual compute only happens when needed. A staging environment with 20 services costs nothing until someone visits it.

**Instant environment clones** — given an existing environment (or its latest snapshot), cloning is a control-plane-only operation. No workloads actually start — they spin up on demand as traffic hits them.

**Minimal boot overhead** — distvirt runs a ~200-line Rust guest agent as PID 1 that mounts the container rootfs and directly execs the entrypoint. No nested container runtime, no systemd, no unnecessary layers. Cold start from VM launch to container process is ~100-150ms; restore from snapshot is ~5-10ms.

## How It Works

```
  CLI / gRPC API
        │
   Orchestrator        ← state machine: planning, scheduling, lifecycle
        │
   Worker Protocol     ← Cap'n Proto over yamux (in-process, UDS, or TCP)
        │
   Worker              ← pure executor: launches VMs, manages fabric
        │
   Firecracker VM      ← microVM with minimal guest agent as PID 1
```

**Orchestrator** — the brain. Pure state machine with an async I/O shell. Owns the service registry, assigns IPs, orders dependencies, manages workload lifecycle and activation, coordinates workers. Structured as an outer layer (worker management, scheduling) and per-namespace sub-state machines (workload lifecycle, services, reconciliation). In local mode, the CLI embeds the orchestrator in-process. In distributed mode, it runs as a standalone gRPC server.

**Worker** — the muscle. Launches Firecracker microVMs, manages the per-namespace networking fabric, prepares container images (via containerd), and reports lifecycle events. Workers are intentionally stateless — they execute commands and report results.

**Guest Agent** — PID 1 inside the VM. A static musl binary that mounts the container rootfs, configures networking, execs the workload, streams output, and reaps zombies. Communicates with the host over virtio-vsock.

### Networking Fabric

Each namespace gets an isolated userspace L3 network. Pods see a normal network interface with an IP, a gateway, and working DNS.

- **IP fabric** — packet router connecting all pods via TAP devices, with a static IP-to-port table
- **smoltcp gateway** — userspace IP stack handling DNS (service discovery + upstream forwarding) and internet egress via TUN
- **Service entities** — virtual IPs on the fabric with buffering and readiness gating. Traffic to a service IP is held until the backing pod is ready, then flushed.

### Protocol-Aware Activation

The fabric understands protocols. When traffic arrives for a dormant service, activators inspect the traffic to make intelligent activation decisions:

- **TCP** — detect SYN packets, filter RSTs and stale keepalives, buffer connections during boot, replay to backend once ready. From the caller's perspective, it's just a slow connection.
- **PostgreSQL** — protocol-aware activation for Postgres connections
- **HTTP/2** — per-stream activation on multiplexed connections, maintaining the H2 session to the client while waking the backend only on actual requests

Activators run as WASM components on service entities, supporting both L3 (packet-level) and L4 (TCP stream-level) processing.

### Ingress

Ingress adapters bridge external traffic into namespace fabrics:

- **WireGuard** — userspace WireGuard endpoint (boringtun). `dv connect` creates an ephemeral tunnel into the namespace. Peer keys map to namespaces.
- **Reverse proxy** (planned) — L7 HTTP/TCP termination at the edge, shareable URLs with zero client setup
- **OS-level routing** (planned) — host routing/NAT into the fabric for infrastructure integration

## CLI

The `dv` CLI has two layers:

**Task-oriented** — `dv up` (deploy from compose file), `dv down`, `dv status` (smart overview), `dv logs` (stream output), `dv events` (activity stream), `dv connect`/`dv disconnect` (WireGuard tunnel into namespace), `dv clone`, `dv deactivate`, `dv splice` (take over a workload's identity for local dev).

**Resource-oriented** — `dv get <type>`, `dv describe <type> <name>`, `dv create`, `dv delete`. Resource types: service, workload, worker, pod, adapter. All support `-o json`.

## Status

### Working today

- **End-to-end local mode** — `dv up` parses a compose file, plans execution, launches Firecracker VMs with networking, DNS service discovery, and log streaming
- **Networking fabric** — L3 IP fabric, smoltcp gateway, service entities with readiness gating and packet buffering
- **Protocol activators** — TCP, HTTP/2, and PostgreSQL activators running as WASM components on service entities
- **WireGuard ingress** — `dv connect` tunnels into a namespace via embedded boringtun
- **Container image preparation** — OCI image pull/mount via containerd, ext4 rootfs generation
- **gRPC client protocol** — namespace CRUD, pod listing, workload deactivation, network connect/disconnect, log/event streaming
- **Orchestrator state machine** — workload lifecycle, service management, IP assignment, dependency ordering
- **Suspend/resume** — VMM snapshot/restore, orchestrator lifecycle management, guest protocol coordination
- **Guest agent** — container setup, output streaming, signal forwarding, network configuration

### In progress / planned

- **Distributed multi-worker mode** — TCP/TLS transport, tunnel ports connecting fabric segments across workers
- **Live migration** — suspend on source, transfer snapshot, resume on target, with fabric buffering for invisibility
- **Namespace snapshots** — full checkpoint to S3, enabling `dv clone` and disaster recovery
- **Autoscaling** — scale workers based on need, drain workers transparently by live migration 
- **Streaming RPCs** — `WatchNamespaceStatus`
- **Reverse proxy ingress** — L7 edge termination with shareable URLs

## Project Structure

| Crate | Role |
|-------|------|
| `distvirt-cli` | CLI binary (`dv` commands) |
| `distvirt-orchestrator` | Orchestrator state machine + async shell + gRPC server |
| `distvirt-worker` | Worker: VMM management, networking fabric, image provider, pod lifecycle |
| `distvirt-worker-protocol` | Orchestrator-worker protocol (Cap'n Proto over yamux) |
| `distvirt-client-protocol` | Client-orchestrator gRPC protocol (tonic/prost, `.proto` definitions) |
| `distvirt-guest-protocol` | Host-guest vsock protocol types |
| `distvirt-compose` | Docker Compose parsing and deployment planning |
| `distvirt-activator` | Protocol activator runtime (WASM component support) |
| `guest-image/guest-init` | Guest agent (PID 1 in the VM, static musl binary) |

The `activators/` directory contains standalone activator components: TCP, HTTP/2, PostgreSQL, and others.

## Usage

```sh
# Deploy a multi-service application from a compose file
dv up -f compose.yaml

# Check status
dv status my-namespace

# Stream logs
dv logs my-namespace/my-service

# Tunnel into the namespace network
dv connect my-namespace

# Run a single container
dv run-image docker.io/library/nginx:latest
```
