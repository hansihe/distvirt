---
title: "Ingress Adapters"
---

> **Status:** The core adapter framework and WireGuard adapter are implemented. Reverse proxy and OS-level routing adapters are not yet implemented (protocol stubs exist).

Ingress adapters provide external access into the networking fabric. The fabric is an isolated per-namespace L3 IP network — adapters bridge traffic from outside into it, allowing developers and external systems to reach services and pods within a namespace.

The primary use case is **developer access to staging environments**.

## Design Principles

- **Adapters are transparent**: An adapter just gets traffic into the fabric. Once inside, the existing resolution order handles it — local port (IP table) → service entity → route table → drop. The adapter doesn't know about services vs pods.
- **Pluggable strategies**: Different access patterns need different adapters. WireGuard for developer VPN-style access, reverse proxy for shareable URLs, OS-level routing for CI/infrastructure integration.
- **Worker-level with per-namespace virtual ports**: Adapters own a single external-facing resource (e.g., one WireGuard UDP socket) at the worker level. When a namespace is created on a worker, `AdapterManager::create_namespace_ports` creates a virtual port for **every** configured adapter on that namespace's fabric. There is currently no per-namespace adapter selection — all adapters get ports on all namespaces.

> **Planned/future**: Per-namespace adapter binding (choosing which adapters present ports into which namespaces) is not yet implemented. The `IngressAdapter::create_port` interface accepts a `namespace_id`, so the plumbing exists, but the orchestrator does not currently send per-namespace adapter binding configuration. All configured adapters unconditionally create ports for every namespace created on the worker.

---

## Architecture

```
┌──────────────────────────────────┐
│  Ingress Adapter (worker-level)  │
│  e.g. WireGuard server           │
│  Ports for all namespaces        │
├──────────┬──────────┬────────────┤
│ vport ns1│ vport ns2│ vport ns3  │  ← virtual ports, one per namespace (all adapters)
└────┬─────┴────┬─────┴─────┬──────┘
     │          │           │
   Fabric    Fabric      Fabric
   (ns1)     (ns2)       (ns3)
```

Each virtual port is a `ChannelPort` from the fabric's perspective — the fabric doesn't know or care that it's backed by an ingress adapter. The adapter handles the external protocol (WireGuard, HTTP proxying, etc.) and translates to/from fabric packets (IP packets with a 3-byte fabric header).

The first adapter port created for a namespace is stored as `adapter_port_id` on the `NamespaceState`. This port ID is passed to the endpoint table during `EndpointSync` and is used to route traffic to locally-placed WireGuard peers (see below).

---

## Configuration

Adapter configuration is delivered by the orchestrator during the worker handshake (see [worker-protocol.md](worker-protocol.md#messages-handshake)). Workers don't bake in adapter config — they advertise capabilities (`available_adapters` in `WorkerHello`), and the orchestrator responds with the specific adapter config and key material to use.

This means worker images are generic: an AMI or container image only needs an orchestrator address and an auth token. Which adapters to run, on which ports, with which keys — all of that comes from the orchestrator.

Key material (WireGuard private keys, TLS certs) derives from the cluster identity. All workers sharing the same adapter type get the same keys, so clients aren't affected when namespaces move between workers.

---

## Adapter Strategies

### WireGuard (boringtun) — Primary [Implemented]

A userspace WireGuard endpoint running at the worker level. Developers connect with standard WireGuard tooling and get direct network access to their staging namespace.

**How it works**:
- Single UDP listener on the worker
- Peer key → namespace routing (each developer's key maps to a namespace)
- Decapsulated IP packets injected into the correct namespace's fabric
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

**Integration**: The adapter owns the UDP socket and WireGuard state (private key delivered by the orchestrator during handshake, derived from cluster identity). Per-namespace, it creates a virtual port on the fabric via `ChannelPort`. Incoming IP packets from a peer are decapsulated, wrapped with a fabric header, and injected into the fabric. Return traffic from the fabric is unwrapped and encapsulated back through the WireGuard tunnel. Since all workers share the same WireGuard identity, namespace migration between workers is transparent to clients.

#### WireGuard Peer Lifecycle

WireGuard peers are not static configuration — they are dynamically managed entities with a full lifecycle driven by the orchestrator.

**Peer management on the orchestrator** (`WireGuardPeerManager`):
- Each namespace has a `WireGuardPeerManager` that tracks connected peers and allocates IPs from the top of the namespace subnet downward.
- When a client connects (via `ClientCommand::Connect`), the orchestrator allocates a peer IP, records the peer's public key, and emits an `AddWireGuardPeer` worker command.
- When a client disconnects, the orchestrator emits a `RemoveWireGuardPeer` worker command to all active workers for that namespace.
- Connect is idempotent — reconnecting with the same public key returns the existing IP without re-adding the peer.

**Worker commands**:
- `WorkerCommand::AddWireGuardPeer { namespace_id, peer_public_key, peer_ip, preshared_key }` — Calls `WireGuardAdapter::add_peer` to register a boringtun `Tunn` for the peer, mapping it to the given namespace and IP.
- `WorkerCommand::RemoveWireGuardPeer { peer_public_key }` — Calls `WireGuardAdapter::remove_peer` to tear down the peer's tunnel state and remove it from address mappings.

**Peer state on the worker** (`PeerState`):
- Each peer has a boringtun `Tunn` instance, a namespace binding, an assigned IP, and a mutable endpoint (the peer's most recent UDP source address, used for roaming support).
- Peers are indexed by public key (`peers_by_key`) and by UDP source address (`peers_by_addr`) for fast packet dispatch.

#### WireGuard Peers as Endpoints

WireGuard peers participate in the fabric's endpoint system as `EndpointKind::WireGuardPeer`. This is critical for routing — the fabric needs to know where to send traffic destined for a peer's IP.

When the orchestrator adds/removes a peer, it also triggers an endpoint sync (`emit_endpoint_sync`) so all workers learn about the peer's route. Each worker then derives its local view:

| Peer placement | EndpointBackend | State |
|---|---|---|
| Local (peer's adapter is on this worker) | `LocalAdapter { port_id }` | Ready |
| Remote (peer's adapter is on another worker) | `RemoteSegment { worker_id }` | Ready |
| Unplaced | `UnplacedPod` (buffer policy) | Buffering |

- **Local peers**: Traffic destined for the peer IP is routed to the adapter's `ChannelPort` via the stored `adapter_port_id`. The adapter's egress loop then finds the peer by destination IP and namespace, encrypts the packet via boringtun, and sends it over UDP.
- **Remote peers**: Traffic is forwarded via the fabric tunnel to the worker that hosts the WireGuard adapter for that peer, where it is then delivered locally.
- **Unplaced peers**: Traffic is buffered (up to 64 frames, 30s timeout) until the peer is placed on a worker.

This means WireGuard peers are first-class citizens in the fabric's routing — they get IPs on the namespace subnet and are routable from any worker in the cluster, just like pods.

### Reverse Proxy — For Shareable Access [Not Implemented]

> Protocol stubs exist in the worker protocol schema (`AdapterConfig::ReverseProxy`), but no implementation yet.

An L7 adapter that terminates HTTP/TCP at the edge and proxies into the fabric. Useful for sharing staging environments with non-technical stakeholders — "here's a URL for the staging frontend."

**How it works**:
- The adapter is a client *inside* the fabric — it gets its own IP on the namespace subnet, sends and receives packets like a pod would
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

**Integration**: Unlike WireGuard (which injects raw IP packets), the reverse proxy adapter participates in the fabric as a network endpoint. It resolves service IPs via the DNS registry, sends traffic through the normal fabric path, and benefits from service entity features (readiness gating, activation) automatically.

### OS-Level Routing / NAT [Not Implemented]

> Protocol stubs exist in the worker protocol schema (`AdapterConfig::OsRouting`), but no implementation yet.

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

- **Per-namespace adapter binding**: The orchestrator could specify which adapters should present virtual ports into each namespace during namespace creation. This would allow mixed configurations — some namespaces routable via WireGuard while others are reverse-proxy-only. The worker-side `create_port(namespace_id)` interface already supports this, but the orchestrator currently does not send per-namespace binding configuration.
- **Adapter status in CLI**: The orchestrator knows which adapters are active on each worker (from the handshake). The CLI can surface this for developer onboarding — e.g., "run `wg-quick up` with this config to reach your staging namespace."
- **Multi-worker WireGuard**: Since all workers share the same WireGuard identity (derived from cluster identity), a developer can connect to any worker and reach any namespace. The remaining question is routing — a dedicated "gateway worker" could handle all WireGuard traffic, or peers could be directed to the worker hosting their namespace.
- **Adapter composition**: A namespace might use multiple adapters simultaneously — WireGuard for developer access + reverse proxy for shareable URLs + OS routing for CI.
