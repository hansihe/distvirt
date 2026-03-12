---
title: "Fabric Tunnels"
---

Inter-worker tunneling for distributed namespaces. When a namespace spans multiple workers, each worker's fabric instance connects to its peers via `TunnelTransport` -- a UDP tunnel multiplexing all shared segments over a single socket per peer.

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

The orchestrator does **not** manage individual tunnel lifecycle. Instead, it pushes a **worker registry** -- a list of known worker peers and their segment memberships -- to each worker. Workers autonomously establish and tear down tunnels based on segment overlap.

This follows the same pattern as `RegistrySync` for DNS: the orchestrator declares desired state, workers converge.

### Protocol

Defined in `worker_protocol.capnp`:

```
WorkerCommand::WorkerRegistrySync {
    workers: Vec<WorkerPeerInfo>,
}

WorkerPeerInfo {
    worker_id: String,
    endpoint: String,         // "host:port"
    public_key: [u8; 32],    // Noise static key
    segments: Vec<u16>,       // segment_ids this worker participates in
}
```

The registry is a **full replacement** on each sync (not incremental). The orchestrator sends it whenever the worker set or segment assignments change.

### Worker Behavior on Registry Update

1. **Diff** against current peer set
2. **New peers with overlapping segments** -- create peer on `TunnelTransport`, initiate Noise handshake
3. **Removed peers** -- tear down transport, clean up tunnel ports
4. **Changed segments** (peer exists but segment set changed) -- remove and re-add the peer with the new segment set
5. **No overlap** -- skip peer (don't connect to workers that share no segments)

Tunnel setup is **preemptive**: as soon as a worker sees a peer with overlapping segments, it opens the tunnel. This ensures the first packet doesn't hit a cold path.

### Implicit Allowlist

Workers only accept inbound tunnel connections from peers present in their registry. The registry acts as an allowlist, and the expected Noise public key provides authentication. Connections from unknown peers are rejected at the handshake level.

### Error Reporting

The protocol defines `TunnelStatusEvt` for reporting tunnel status changes back to the orchestrator:

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

These are informational -- the orchestrator can log, alert, or reschedule pods off workers with persistent tunnel failures. Workers handle reconnection locally; the orchestrator doesn't need to intervene for transient errors.

**Status: not yet implemented.** The protocol schema and types exist (`TunnelStatusEvt` in Cap'n Proto, `tunnelStatus` variant in `WorkerEvent`), but the worker does not currently emit these events. The `TunnelManager` does not report handshake success/failure or disconnection back to the orchestrator.

---

## Segment ID Allocation

`segment_id` is a 16-bit field assigned **globally per namespace** by the orchestrator. Each namespace gets a unique segment ID at creation time, valid across all tunnels in the cluster. The same segment ID always refers to the same namespace regardless of which worker pair tunnel it traverses.

The orchestrator maintains a `u16` allocator (`alloc_segment_id` / `free_segment_id` on `Orchestrator`): an incrementing counter that skips zero and any IDs still in active use. With 65536 values and typical namespace lifetimes, wraparound is infrequent. The segment ID is stored in `NamespaceStateMachine::segment_id` and passed to workers in the `NetworkConfig` of `CreateNamespace` commands.

---

## Wire Format

Fabric frames pass through the tunnel as-is -- the existing 3-byte fabric header is the tunnel multiplexing header:

```
UDP datagram (after Noise decrypt):
  [fabric_hdr (3 bytes)][IP packet]
   ├─ flags (u8):       NEEDS_CSUM etc.
   └─ segment_id (u16): namespace demux key
```

One UDP datagram = one fabric frame. No additional framing or length prefixes needed.

---

## Encryption: Noise Protocol via `snow`

The tunnel uses the [Noise protocol framework](https://noiseprotocol.org/) (Noise_IK pattern) for authenticated encryption, via the `snow` crate. The pattern string is `Noise_IK_25519_ChaChaPoly_BLAKE2s`.

**Why not boringtun's noise module**: Boringtun's useful internals (`Session`, `Handshake`) are `pub(crate)` -- inaccessible from outside the crate. The only public API is `Tunn`, which validates decrypted payloads as IP packets (`validate_decapsulated_packet` checks the IP version nibble). Fabric frames start with a `flags` byte (not an IP header), so decapsulation rejects them. Boringtun is a WireGuard tunnel implementation, not a reusable Noise library.

**Why `snow`**: Purpose-built Noise protocol framework that operates on arbitrary byte buffers with no payload validation. Uses `ring` for crypto (same as boringtun already pulls in), so no new crypto dependency. The `TransportState` provides exactly what we need -- encrypt/decrypt with counter-based nonces and replay protection.

**Noise_IK pattern**: The initiator's static key is sent encrypted in the first message, and the responder's static key is known ahead of time (pre-distributed by the orchestrator via the worker registry). This provides mutual authentication and forward secrecy with a 1-RTT handshake.

**Initiator determination**: Rather than relying on out-of-band coordination, the peer with the lexicographically smaller Noise static public key always initiates. Both sides compute this independently from the keys distributed via the worker registry.

**Key distribution**: Each worker generates a Noise static keypair at `TunnelManager` initialization (after the handshake determines whether encryption is enabled via `WorkerAccepted::tunnel_encrypted`). The public key is reported back to the orchestrator in `WorkerReady`. The orchestrator includes peer public keys in `WorkerPeerInfo`. The worker registry is the sole distribution mechanism -- no separate key exchange protocol.

**Overhead**: ~16 bytes (Poly1305 auth tag) per datagram, on top of the outer UDP/IP headers (28 bytes for IPv4).

---

## MTU Considerations

The inner MTU (guest network interface) must account for tunnel overhead to avoid IP-layer fragmentation on the outer path. With tunnel overhead, an inner MTU of 1420 (matching the WireGuard convention) provides comfortable margin on standard 1500-byte Ethernet links.

IP-layer fragmentation of outer UDP datagrams works as a fallback (the receiving kernel reassembles before the UDP socket sees the data), but should be avoided for performance -- especially since PMTUD is often broken by middleboxes.

**Status: not implemented.** There is no MTU configuration in the protocol or the worker. The guest network interface MTU is not adjusted to account for tunnel overhead. This should eventually be propagated via `NetworkConfig`.

---

## Data Plane: `TunnelTransport`

Located at `distvirt-worker/src/fabric/tunnel.rs`. One instance per worker, owned by `TunnelManager`.

- Owns a single UDP socket bound to `0.0.0.0:0` (port reported to orchestrator in `WorkerReady`)
- Supports both plaintext and encrypted modes (selected at construction time)
- Manages peers via `add_peer()` / `remove_peer()`
- **Recv loop**: reads UDP datagrams, handles Noise handshake messages or decrypts transport data, parses `segment_id` from fabric header bytes `[1..3]`, dispatches to matching namespace channel via `mpsc::Sender`
- **Per-segment egress loop**: reads frames from fabric `ChannelPort`, completes deferred checksums, stamps `segment_id`, encrypts if enabled (waiting for handshake completion first), sends to peer endpoint
- `create_namespace_port(worker_id, segment_id)` returns `(ChannelPort, TunnelPortHandle)` -- the `ChannelPort` plugs into the fabric, the handle provides RAII cleanup
- `TunnelPortHandle` on drop: removes segment channel, aborts egress task
- Noise session per peer transitions through `Handshaking(HandshakeState)` then `Transport(TransportState)`
- Unit tested: plaintext round-trip, multi-segment demux, unknown segment drop, encrypted round-trip, encrypted multi-segment demux

### Fabric integration

- `add_tunnel_port(worker_id, port)` / `remove_tunnel_port(worker_id)` -- register/deregister tunnel ports on a fabric instance
- `FabricContextInner::tunnel_ports`: `HashMap<String, PortId>` maps `worker_id` to `port_id`
- `dispatch_frame()` resolves `RouteAction::RemoteWorker { worker_id }` by looking up the tunnel port and forwarding

### Route table

- `FabricRouteEntry` with `RouteDestination::RemoteWorker { worker_id }` -- orchestrator sends these via `FabricRouteSync`/`FabricRouteUpdate`
- Worker resolves `worker_id` to the corresponding tunnel port

---

## Worker-Side Tunnel Manager

Located at `distvirt-worker/src/worker/tunnel_manager.rs`. Fully implemented and integrated into the worker.

### Responsibilities

- Owns a single `TunnelTransport` (one UDP socket per worker)
- Maintains a map of `worker_id` to `PeerState` (endpoint, segments, public key, active namespace ports)
- Maintains a map of `segment_id` to `NamespaceInfo` (namespace ID, fabric reference)
- On `WorkerRegistrySync`: diffs peers, removes stale peers, adds/updates peers, creates tunnel ports for any overlapping namespaces
- On namespace creation (`on_namespace_created`): registers the namespace, creates tunnel ports on all peers that share the segment
- On namespace destruction (`on_namespace_destroyed`): removes the namespace, drops all tunnel ports for that segment (RAII cleanup)

### Lifecycle

The `TunnelManager` is initialized after the worker handshake, once the orchestrator communicates whether encryption is enabled (via `WorkerAccepted::tunnel_encrypted`). It binds to `0.0.0.0:0` and reports the listen port and public key in `WorkerReady`.

Tunnel ports can be created in either order -- namespace first then peer, or peer first then namespace. The manager handles both cases by checking for overlaps at both registration points.

```
WorkerRegistrySync arrives
  -> diff against current peers
  -> for each stale peer (not in new registry):
      1. Drop all TunnelPortHandles (RAII cleanup of segment channels + egress tasks)
      2. transport.remove_peer(worker_id)

  -> for each new/changed peer:
      1. Remove old state if exists
      2. Determine initiator by lexicographic public key comparison
      3. transport.add_peer(worker_id, endpoint, public_key, is_initiator)
      4. For each shared segment_id (peer.segments intersect namespaces):
         a. transport.create_namespace_port(worker_id, segment_id) -> (ChannelPort, handle)
         b. fabric.add_tunnel_port(worker_id, channel_port) -> (port_id, read_loop_task)
         c. Store handle + task in peer's namespace_ports map

Namespace created (on_namespace_created)
  -> Register segment_id -> namespace info
  -> For each peer that lists this segment_id:
      create tunnel port (same as step 4 above)

Namespace destroyed (on_namespace_destroyed)
  -> Remove segment from namespace map
  -> Drop all tunnel port entries for that segment across all peers
```

### What is NOT yet implemented

- **TunnelStatus event emission**: The manager does not report handshake success/failure or peer disconnection back to the orchestrator. The protocol types exist but are unused.

---

## Orchestrator Integration

Located at `distvirt-orchestrator/src/orchestrator/networking.rs`.

### Worker Registry Construction

`build_worker_registry()` iterates over all connected workers that have both a public endpoint and tunnel config (listen port + public key). For each, it builds a `WorkerPeerInfo` with the worker's segments derived from its assigned namespaces.

`push_worker_registry()` sends the full registry to every connected worker via `WorkerCommand::WorkerRegistrySync`. Called after worker join/leave and namespace assignment changes.

### Segment ID Allocation

`alloc_segment_id()` on `Orchestrator`: incrementing `u16` counter, skips zero and any IDs still in `active_segment_ids`. Each `NamespaceStateMachine` stores its `segment_id`, which is included in `NetworkConfig` when creating the namespace on a worker.

### Namespace Assignment

`assign_worker_to_namespace()` sends `CreateNamespace` with the network config (including `segment_id`). Callers are responsible for calling `push_worker_registry()` afterward to update all workers' peer registries.

---

## Implementation Status

| Component | Status |
|---|---|
| `segment_id` in `FabricHeader` | Done |
| `segment_id` in `NetworkConfig` | Done |
| Segment ID allocator in orchestrator | Done |
| `TunnelTransport` (plaintext UDP + segment demux) | Done |
| Noise_IK encryption via `snow` | Done |
| `TunnelManager` (registry sync, namespace lifecycle) | Done |
| `WorkerRegistrySync` protocol + types | Done |
| Orchestrator `build_worker_registry` / `push_worker_registry` | Done |
| Worker handshake (tunnel config negotiation) | Done |
| `RouteDestination::RemoteWorker` in route table | Done |
| Fabric `add_tunnel_port` / `remove_tunnel_port` | Done |
| `TunnelStatus` event emission from worker | Not implemented (protocol types exist, worker does not emit) |
| MTU configuration for tunnel overhead | Not implemented (no MTU field in protocol or guest config) |
| E2E test (two workers, cross-tunnel traffic) | Not implemented |
