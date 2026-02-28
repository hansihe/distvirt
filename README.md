# distvirt

A VM-based container runtime built for transparent scale-to-zero. Runs containers inside Firecracker microVMs that can be suspended, resumed, and activated on demand — with protocol-aware network buffering so callers never know the target was asleep.

## Why VMs?

VMs give you something containers can't: **suspend and resume**. You can snapshot a running service to disk and bring it back later with its full in-memory state intact. Combined with a network fabric that buffers traffic while a service activates, this enables transparent scale-to-zero — existing applications work without modification, services spin up on demand when traffic arrives, and callers just see a slightly slow connection.

This is the foundation for features like **instant environment clones**: given an existing environment (or its latest snapshot), cloning is a control-plane-only operation. No workloads actually start — they spin up on demand as traffic hits them. A staging environment with 20 services costs nothing until someone visits it.

### Why not existing VM runtimes?

Existing VM-based container runtimes (Kata Containers, firecracker-containerd) run a full Linux userspace inside the VM — systemd, containerd, runc — adding hundreds of milliseconds to every cold start. When services activate on demand as traffic arrives, boot latency is directly felt by users.

distvirt runs a ~200-line Rust guest agent as PID 1 that mounts the container rootfs and directly execs the entrypoint. No nested container runtime, no systemd, no unnecessary layers. The VM boundary itself provides isolation.

## Architecture

```
  compose.yaml / API
        │
   Orchestrator          ← planning, scheduling, state ownership
        │
   Worker Protocol       ← transport-agnostic (channels, UDS, TCP)
        │
   Worker                ← pure executor: launches VMs, manages fabric
        │
   Firecracker VM        ← microVM with custom guest agent
```

**Orchestrator** — the brain. Owns the service registry, assigns IPs, orders dependencies, decides what runs where. In local mode, the CLI embeds a minimal orchestrator in-process. In distributed mode (future), it runs as a standalone server.

**Worker** — the muscle. Launches Firecracker VMs, manages the local network fabric, prepares container images, reports lifecycle events. Workers are intentionally dumb — they execute commands and report results, nothing more.

**Worker Protocol** — a bidirectional command/event stream between orchestrator and worker. The same protocol flows over in-process channels (local mode), Unix domain sockets, or TCP/TLS (distributed mode). Workers connect to the orchestrator, not the other way around.

## Network Fabric

Each deployment gets an isolated L2 network namespace with:

- **Software L2 switch** — MAC-learning frame router connecting all pods
- **smoltcp gateway** — userspace network stack acting as the namespace gateway, handling ARP, DNS resolution (service discovery + upstream forwarding), and host egress via TUN
- **Service registry** — name-to-IP mappings maintained by the orchestrator, projected to workers for local DNS resolution

Pods communicate over this fabric using standard networking — they see a normal network interface with an IP, a gateway, and working DNS. Service names resolve to IPs within the namespace. In distributed mode, fabric segments on different workers connect via tunnel ports, extending the L2 network across machines transparently.

### Protocol-Aware Activation

The fabric doesn't just forward frames — it understands protocols. When traffic arrives for a dormant service, the fabric can intelligently buffer the connection while the orchestrator activates the target:

- **TCP activation** — hold the SYN, buffer the connection during boot, deliver once the service is up. From the caller's perspective, it's just a slow connection.
- **HTTP/2 activation** (planned) — parse H2 framing on multiplexed connections, activate per-stream (per-request) rather than per-connection. The fabric maintains the H2 connection to the client (SETTINGS, PING, WINDOW_UPDATE) and only wakes the backend when an actual request arrives.

This is the key enabler for transparent scale-to-zero — the sending service doesn't know or care that the target was suspended. No retries, no service mesh, no application changes.

## Vision

**Scale-to-zero staging environments** — deploy a full multi-service application where idle services automatically suspend. When a developer visits the environment, services activate on demand as traffic flows through. Clone an environment instantly as a control-plane operation; actual compute only happens when needed.

**Durable execution for provisioning** (planned) — environment setup orchestrated by a durable execution engine, enabling rollback of partially-provisioned environments when workflow steps fail and need to be retried.

## Usage

```sh
# Run a single container from an OCI image (via containerd)
distvirt run-image docker.io/library/nginx:latest

# Run a multi-service deployment from a compose file
distvirt compose-up -f compose.yaml
```

## Project Structure

| Crate | Purpose |
|-------|---------|
| `distvirt-cli` | CLI binary with user-facing commands |
| `distvirt` | Core orchestration library (deployment planning, compose orchestration) |
| `distvirt-worker` | Worker execution engine (VMM, fabric, image preparation) |
| `distvirt-worker-protocol` | Orchestrator-worker protocol definitions |
| `distvirt-guest-protocol` | Host-guest vsock protocol types |
| `distvirt-compose` | Docker Compose file parser |
| `distvirt-types` | Shared type definitions |
| `guest-image/guest-init` | Minimal guest agent (PID 1 inside the VM) |

## Status

Local single-worker mode works end-to-end: `compose-up` parses a compose file, plans execution, launches VMs with networking and DNS service discovery, and streams output.

Not yet implemented: distributed multi-worker mode, suspend/resume, scale-to-zero activation, protocol inspectors, port forwarding, health checks, exec.
