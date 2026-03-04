# Fabric Tunnels

Inter-worker tunneling for distributed namespaces. When a namespace spans multiple workers, each worker's fabric instance connects to its peers via `TunnelTransport` — a UDP tunnel multiplexing all shared segments over a single socket per peer.

## Architecture

```
Worker A                                    Worker B
┌──────────────────┐                       ┌──────────────────┐
│ Namespace "foo"  │                       │ Namespace "foo"  │
│  Fabric          │                       │  Fabric          │
│   └─ TunnelPort ─┼── segment_id=1 ──┐   │   └─ TunnelPort  │
│                  │                   │   │                  │
│ Namespace "bar"  │                   │   │ Namespace "bar"  │
│  Fabric          │                   │   │  Fabric          │
│   └─ TunnelPort ─┼── segment_id=2 ──┤   │   └─ TunnelPort  │
└──────────────────┘                   │   └──────────────────┘
                                       ▼
                              ┌─────────────────┐
                              │ TunnelTransport  │
                              │ Single UDP socket│
                              │ per worker pair  │
                              │                  │
                              │ Noise encryption │
                              │ segment_id demux │
                              └─────────────────┘
```

One `TunnelTransport` per remote worker peer, multiplexing all namespaces over a single encrypted UDP socket. The `segment_id` field in `FabricHeader` demultiplexes packets to the correct namespace's fabric instance.

---

## Control Plane: Worker Registry

The orchestrator does **not** manage individual tunnel lifecycle. Instead, it pushes a **worker registry** — a list of known worker peers and their segment memberships — to each worker. Workers autonomously establish and tear down tunnels based on segment overlap.

This follows the same pattern as `RegistrySync` for DNS: the orchestrator declares desired state, workers converge.

### Protocol

```
WorkerCommand::WorkerRegistrySync {
    workers: Vec<WorkerPeerInfo>,
}

WorkerPeerInfo {
    worker_id: String,
    endpoint: SocketAddr,
    public_key: [u8; 32],   // Noise static key
    segments: Vec<u16>,      // segment_ids this worker participates in
}
```

The registry is a **full replacement** on each sync (not incremental). The orchestrator sends it whenever the worker set or segment assignments change.

### Worker Behavior on Registry Update

1. **Diff** against current peer set
2. **New peers with overlapping segments** → create `TunnelTransport`, call `add_peer()`, initiate Noise handshake
3. **Removed peers** → tear down transport, clean up tunnel ports
4. **Changed segments** (peer exists but segment set changed) → add/remove `TunnelPort`s on existing transport, no reconnect needed
5. **No overlap** → skip peer (don't connect to workers that share no segments)

Tunnel setup is **preemptive**: as soon as a worker sees a peer with overlapping segments, it opens the tunnel. This ensures the first packet doesn't hit a cold path.

### Implicit Allowlist

Workers only accept inbound tunnel connections from peers present in their registry. The registry acts as an allowlist, and the expected Noise public key provides authentication. Connections from unknown peers are rejected at the handshake level.

### Error Reporting

Workers report tunnel status changes back to the orchestrator:

```
WorkerEvent::TunnelStatus {
    peer_worker_id: String,
    status: TunnelPeerStatus,
}

enum TunnelPeerStatus {
    Connected,
    Disconnected { error: String },
    HandshakeFailed { error: String },
}
```

These are informational — the orchestrator can log, alert, or reschedule pods off workers with persistent tunnel failures. Workers handle reconnection locally; the orchestrator doesn't need to intervene for transient errors.

---

## Segment ID Allocation

`segment_id` is a 16-bit field assigned **globally per namespace** by the orchestrator. Each namespace gets a unique segment ID at creation time, valid across all tunnels in the cluster. The same segment ID always refers to the same namespace regardless of which worker pair tunnel it traverses.

The orchestrator maintains a simple `u16` allocator (incrementing counter + set of active IDs). On namespace creation, allocate the next free ID; on destruction, return it to the pool. With 65536 values and typical namespace lifetimes, wraparound is infrequent — the allocator skips IDs still in use.

Workers receive the segment ID as part of namespace setup (via the `segment_id` field in `NetworkConfig`). No per-tunnel negotiation needed.

---

## Wire Format

Fabric frames pass through the tunnel as-is — the existing 3-byte fabric header is the tunnel multiplexing header:

```
UDP datagram (after Noise decrypt):
  [fabric_hdr (3 bytes)][IP packet]
   ├─ flags (u8):       NEEDS_CSUM etc.
   └─ segment_id (u16): namespace demux key
```

One UDP datagram = one fabric frame. No additional framing or length prefixes needed.

---

## Encryption: Noise Protocol via `snow`

The tunnel uses the [Noise protocol framework](https://noiseprotocol.org/) (Noise_IK pattern) for authenticated encryption, via the `snow` crate.

**Why not boringtun's noise module**: Boringtun's useful internals (`Session`, `Handshake`) are `pub(crate)` — inaccessible from outside the crate. The only public API is `Tunn`, which validates decrypted payloads as IP packets (`validate_decapsulated_packet` checks the IP version nibble). Fabric frames start with a `flags` byte (not an IP header), so decapsulation rejects them. Boringtun is a WireGuard tunnel implementation, not a reusable Noise library.

**Why `snow`**: Purpose-built Noise protocol framework that operates on arbitrary byte buffers with no payload validation. Uses `ring` for crypto (same as boringtun already pulls in), so no new crypto dependency. The `TransportState` provides exactly what we need — encrypt/decrypt with counter-based nonces and replay protection.

**Noise_IK pattern**: The initiator's static key is sent encrypted in the first message, and the responder's static key is known ahead of time (pre-distributed by the orchestrator via the worker registry). This provides mutual authentication and forward secrecy with a 1-RTT handshake.

**Key distribution**: Each worker generates a Noise static keypair at startup and reports its public key in `WorkerCapabilities`. The orchestrator includes peer public keys in `WorkerPeerInfo`. The worker registry is the sole distribution mechanism — no separate key exchange protocol.

**Overhead**: ~16 bytes (Poly1305 auth tag) + ~8 bytes (counter/header) ≈ 24 bytes per datagram, on top of the outer UDP/IP headers (28 bytes for IPv4). Total tunnel overhead ≈ 52 bytes — less than WireGuard's ~60 bytes.

---

## MTU Considerations

The inner MTU (guest network interface) must account for tunnel overhead to avoid IP-layer fragmentation on the outer path. With ~52 bytes of overhead, an inner MTU of 1420 (matching the WireGuard convention) provides comfortable margin on standard 1500-byte Ethernet links.

IP-layer fragmentation of outer UDP datagrams works as a fallback (the receiving kernel reassembles before the UDP socket sees the data), but should be avoided for performance — especially since PMTUD is often broken by middleboxes. The guest MTU is configured via the existing `ConfigureNetwork` command.

---

## Data Plane Components

### `tunnel.rs` — `TunnelTransport`

Worker-level, one per remote worker peer. **Already implemented** (plaintext UDP, no Noise yet).

- Owns a single UDP socket bound to the worker's tunnel listen port
- Manages peers via `add_peer()` / `remove_peer()`
- **Recv loop**: read UDP datagrams, parse `segment_id` from fabric header bytes `[1..3]`, dispatch to matching namespace channel via `mpsc::Sender`
- **Per-segment egress loop**: read frames from fabric `ChannelPort`, complete deferred checksums, stamp `segment_id`, `send_to` peer endpoint
- `create_namespace_port(worker_id, segment_id)` → returns `(ChannelPort, TunnelPortHandle)` — the `ChannelPort` plugs into the fabric, the handle provides RAII cleanup
- `TunnelPortHandle` on drop: removes segment channel, aborts egress task

### Fabric integration (`mod.rs`)

- `add_tunnel_port(worker_id, port)` / `remove_tunnel_port(worker_id)` — register/deregister tunnel ports
- `FabricContextInner::tunnel_ports`: `HashMap<String, PortId>` maps `worker_id → port_id`
- `dispatch_frame()` resolves `RouteAction::RemoteWorker { worker_id }` by looking up the tunnel port and forwarding

### Route table (`route.rs`)

- `FabricRouteEntry` with `RouteDestination::RemoteWorker { worker_id }` — orchestrator sends these via `FabricRouteSync`/`FabricRouteUpdate`
- Worker resolves `worker_id` to the corresponding `TunnelTransport` peer

---

## Worker-Side Tunnel Manager

**Not yet implemented.** The tunnel manager is a worker-level component that processes `WorkerRegistrySync` commands and manages `TunnelTransport` instances.

### Responsibilities

- Maintain a map of `worker_id → TunnelTransport`
- On registry sync: diff peers, create/destroy transports, add/remove namespace ports
- On namespace creation (with `segment_id`): find peers that share the segment, create tunnel ports on existing transports
- On namespace destruction: remove tunnel ports from all transports
- Emit `TunnelStatus` events on handshake success/failure, disconnect, reconnect

### Lifecycle

```
WorkerRegistrySync arrives
  → for each new peer with overlapping segments:
      1. TunnelTransport::new(listen_addr)
      2. transport.add_peer(worker_id, endpoint)
      3. Initiate Noise handshake
      4. For each shared segment_id:
         a. transport.create_namespace_port(worker_id, segment_id) → (ChannelPort, handle)
         b. fabric.add_tunnel_port(worker_id, channel_port)
      5. Emit TunnelStatus::Connected

  → for each removed peer:
      1. fabric.remove_tunnel_port(worker_id)
      2. Drop TunnelPortHandle (RAII cleanup)
      3. Drop TunnelTransport
      4. Emit TunnelStatus::Disconnected
```

---

## Existing Infrastructure

The following pieces are already in place:

- `segment_id` field in `FabricHeader` — reserved for tunnel demux, currently always 0
- `segment_id: Option<u16>` in `NetworkConfig` — sent in `CreateNamespace`, currently always `None`
- `RouteDestination::RemoteWorker { worker_id }` in route table entries
- `RouteAction::RemoteWorker { worker_id }` in `forwarding.rs` dispatch — forwards to tunnel port
- `Fabric::add_tunnel_port()` / `remove_tunnel_port()` — tunnel port registration
- `TunnelTransport` with plaintext UDP + segment demux — unit tested
- `ChannelPort` — proven virtual port integration pattern (used by WireGuard ingress adapter)
- `public_endpoint` in `WorkerCapabilities` — workers announce their reachable endpoint at connect time

---

## Implementation Plan

1. **Global `segment_id` allocator** in orchestrator, assigned per namespace at creation time
2. **`WorkerRegistrySync` command** — add to worker protocol schema + types
3. **`TunnelStatus` event** — add to worker protocol schema + types
4. **Worker tunnel manager** — processes registry syncs, manages `TunnelTransport` lifecycle
5. **Orchestrator integration** — build worker registry from connected workers, push on worker join/leave/namespace changes
6. **Noise encryption** via `snow` crate (Noise_IK, `ring` backend) — wrap existing plaintext UDP
7. **MTU configuration** — propagate tunnel overhead to guest `ConfigureNetwork`
8. **E2E test** — two workers, shared namespace, pod-to-pod traffic across tunnel
