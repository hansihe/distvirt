# Ingress Adapters

> **Status:** This document is a design proposal. No ingress adapter implementation exists yet in the codebase.

Ingress adapters provide external access into the networking fabric. The fabric is an isolated per-namespace L2 network — adapters bridge traffic from outside into it, allowing developers and external systems to reach services and pods within a namespace.

The primary use case is **developer access to staging environments**.

## Design Principles

- **Adapters are transparent**: An adapter just gets traffic into the fabric. Once inside, the existing resolution order handles it — local TAP → service entity → route table → flood. The adapter doesn't know about services vs pods.
- **Pluggable strategies**: Different access patterns need different adapters. WireGuard for developer VPN-style access, reverse proxy for shareable URLs, OS-level routing for CI/infrastructure integration.
- **Worker-level with per-namespace virtual ports**: Adapters own a single external-facing resource (e.g., one WireGuard UDP socket) at the worker level, and present virtual ports into each namespace's fabric instance.

---

## Architecture

```
┌──────────────────────────────────┐
│  Ingress Adapter (worker-level)  │
│  e.g. WireGuard server           │
│  Demultiplexes to namespaces     │
├──────────┬──────────┬────────────┤
│ vport ns1│ vport ns2│ vport ns3  │  ← virtual ports, one per namespace
└────┬─────┴────┬─────┴─────┬──────┘
     │          │           │
   Fabric    Fabric      Fabric
   (ns1)     (ns2)       (ns3)
```

Each virtual port is a `FramePort` from the fabric's perspective — the fabric doesn't know or care that it's backed by an ingress adapter. The adapter handles the external protocol (WireGuard, HTTP proxying, etc.) and translates to/from L2 frames.

---

## Configuration

Adapter configuration is delivered by the orchestrator during the worker handshake (see [worker-protocol.md](worker-protocol.md#messages-handshake)). Workers don't bake in adapter config — they advertise capabilities (`available_adapters` in `WorkerHello`), and the orchestrator responds with the specific adapter config and key material to use.

This means worker images are generic: an AMI or container image only needs an orchestrator address and an auth token. Which adapters to run, on which ports, with which keys — all of that comes from the orchestrator.

Key material (WireGuard private keys, TLS certs) derives from the cluster identity. All workers sharing the same adapter type get the same keys, so clients aren't affected when namespaces move between workers.

**Per-namespace binding**: When the orchestrator creates a namespace on a worker, it specifies which of that worker's active adapters should present virtual ports into the namespace's fabric. This allows mixed configurations — some namespaces routable via WireGuard while others are reverse-proxy-only.

---

## Adapter Strategies

### WireGuard (boringtun) — Primary

A userspace WireGuard endpoint running at the worker level. Developers connect with standard WireGuard tooling and get direct network access to their staging namespace.

**How it works**:
- Single UDP listener on the worker
- Peer key → namespace routing (each developer's key maps to a namespace)
- Decapsulated packets injected into the correct namespace's fabric as L2 frames
- Implemented via [boringtun](https://github.com/cloudflare/boringtun) (pure Rust, userspace)

**Developer experience**:
- Dev gets a WireGuard config file (or QR code)
- Connect, and all services in the staging namespace are reachable by IP or DNS name
- Feels like being "on the network" — no port mapping, no tunneling hassle

**Strengths**:
- Fully userspace, no extra root/capability escalation
- Encrypted by default, works across NATs (UDP-based)
- Clean trust boundary — each peer gets a keypair, access is explicit
- Works identically in dev and prod, single-machine and distributed
- boringtun is pure Rust, fits the stack

**Considerations**:
- Clients need WireGuard tooling configured (keys, endpoints)
- Doesn't naturally integrate with existing infrastructure load balancers

**Integration**: The adapter owns the UDP socket and WireGuard state (private key delivered by the orchestrator during handshake, derived from cluster identity). Per-namespace, it creates a virtual port on the fabric. Incoming packets from a peer are decapsulated, an Ethernet header is constructed (using the peer's assigned IP for ARP/MAC resolution), and the frame is injected into the fabric. Return traffic from the fabric is encapsulated and sent back through the WireGuard tunnel. Since all workers share the same WireGuard identity, namespace migration between workers is transparent to clients.

### Reverse Proxy — For Shareable Access

An L7 adapter that terminates HTTP/TCP at the edge and proxies into the fabric. Useful for sharing staging environments with non-technical stakeholders — "here's a URL for the staging frontend."

**How it works**:
- The adapter is a client *inside* the fabric — it gets its own IP on the namespace subnet, sends and receives frames like a pod would
- External HTTP requests are routed to the correct namespace and service based on hostname, path, or other L7 attributes
- The adapter resolves service names via the fabric's DNS, connects to service IPs, and proxies traffic

**Developer experience**:
- Share a URL like `https://my-staging.dev.example.com/`
- No VPN or client-side tooling required
- Works in browsers, curl, CI scripts

**Strengths**:
- Zero client-side setup
- Can do TLS termination, hostname-based routing
- Interacts naturally with service entities (buffering, activation)

**Considerations**:
- Not transparent — protocol-specific (HTTP, TCP, etc.)
- Adds a hop and protocol termination overhead
- Needs external DNS/routing to direct traffic to the worker

**Integration**: Unlike WireGuard (which injects raw frames), the reverse proxy adapter participates in the fabric as a network endpoint. It resolves service IPs via the DNS registry, sends traffic through the normal fabric path, and benefits from service entity features (readiness gating, activation) automatically.

### OS-Level Routing / NAT

Makes namespace subnets routable from the host network. The most transparent option for infrastructure integration.

**How it works**:
- Host routing table entries point namespace subnets to the fabric's TUN device
- Or: iptables DNAT rules map host ports to service IPs
- Traffic enters through the existing gateway/TUN path

**Strengths**:
- Zero client-side tooling — just connect to an IP:port
- Integrates with existing infrastructure (load balancers, DNS, firewalls, CI)
- Low overhead, no encapsulation

**Considerations**:
- Requires host-level privileges (iptables, routing table manipulation)
- Subnet conflicts — namespace subnets must be unique on the host network
- Weaker security boundary (host network access = fabric access)
- Harder in multi-worker distributed mode

**Variant — BGP**: Workers advertise their namespace subnets via BGP, with path lengths based on locality. Elegant and correct, but impractical for most real-world deployments due to BGP infrastructure requirements.

**Integration**: The TUN device and gateway already exist. This adapter is primarily about host-side configuration — adding routes and firewall rules. The actual traffic path reuses the existing gateway infrastructure.

---

## Future Considerations

- **Adapter status in CLI**: The orchestrator knows which adapters are active on each worker (from the handshake). The CLI can surface this for developer onboarding — e.g., "run `wg-quick up` with this config to reach your staging namespace."
- **Multi-worker WireGuard**: Since all workers share the same WireGuard identity (derived from cluster identity), a developer can connect to any worker and reach any namespace. The remaining question is routing — a dedicated "gateway worker" could handle all WireGuard traffic, or peers could be directed to the worker hosting their namespace.
- **Adapter composition**: A namespace might use multiple adapters simultaneously — WireGuard for developer access + reverse proxy for shareable URLs + OS routing for CI.
