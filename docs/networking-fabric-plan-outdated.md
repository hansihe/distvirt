# Networking Fabric Plan

## Current State

**Phase 1 is implemented.** The diagnostic loop in `orchestrate.rs` has been replaced with the fabric L2 switch. Guest ARP requests for the gateway (172.16.0.1) get synthetic replies. MAC learning and frame forwarding/flooding between ports works. The fabric runs on a dedicated tokio runtime in a background thread.

## Architecture

A userspace L2 switch that sits between VM TAP sockets and processes Ethernet frames. Layered to support protocol-aware activation.

```
┌─────────────────────────────────────────────────┐
│                  Fabric (top-level)              │
│  Owns ports, routes frames, manages activation   │
├─────────────────────────────────────────────────┤
│           Protocol Inspectors (pluggable)        │
│  TCP state tracking → HTTP/2 frame parsing →     │
│  activation decisions at the right granularity   │
├─────────────────────────────────────────────────┤
│              Port abstraction                    │
│  Each port = a TapDevice fd (async read/write)   │
│  Raw Ethernet frames via tokio AsyncFd           │
└─────────────────────────────────────────────────┘
```

The fabric is **decoupled from VMM and container code**. It only knows about ports (L2 frame sources/sinks) and activation policies. The orchestrator is the glue — it reacts to activation events by launching VMs, then hands TAP devices to the fabric.

## Modules (`src/fabric/`)

### `port.rs` — Async L2 port

Wraps a raw L2 fd (AF_PACKET socket from `TapDevice`) in tokio `AsyncFd`. Provides async read/write of Ethernet frames. This is the VMM-agnostic boundary — the fabric never sees Firecracker or any VMM, just ports.

### `switch.rs` — L2 switch

MAC address table, frame forwarding between ports. Handles ARP — responds to ARP requests for the gateway IP so guests can route traffic. Learns MAC→port mappings from source addresses of incoming frames.

### `connection.rs` — TCP connection tracking

Stateful tracking of TCP connections from L2 frames. Needed for protocol-aware activation — must see TCP payload to inspect higher-level protocols. Tracks connection state (SYN/SYN-ACK/established/etc.) and reassembles streams where needed for protocol inspection.

### `activation.rs` — Activation policies (trait-based)

Pluggable activation strategies:

- **L3 activation**: any IP packet to a dormant service triggers wake (simplest)
- **TCP activation**: SYN to a dormant service triggers wake, buffer packets during boot
- **HTTP/2 activation**: parse H2 framing on muxed connections, activate per-stream (per-request) rather than per-connection. Hold the H2 connection open (respond to SETTINGS/PING/WINDOW_UPDATE), only wake the backend when a HEADERS frame arrives on a new stream.

### `mod.rs` — `Fabric` struct

Ties everything together. Owns ports, runs the tokio event loop, delegates to activation policies.

## Interface with Orchestrator

```rust
pub struct Fabric { /* ... */ }

impl Fabric {
    /// Add a port for a running VM
    pub fn add_port(&mut self, tap: TapDevice, config: PortConfig) -> PortId;

    /// Remove a port (VM shutting down)
    pub fn remove_port(&mut self, id: PortId);

    /// Register a dormant service (not yet running, fabric buffers for it)
    pub fn register_dormant(&mut self, config: DormantConfig) -> DormantId;

    /// Subscribe to activation events
    pub fn activations(&self) -> Receiver<ActivationEvent>;
}
```

The orchestrator reacts to `ActivationEvent`s by spinning up VMs, then calls `add_port` once the TAP is ready. The fabric buffers packets in between. This keeps the fabric completely decoupled from VMM/container concerns.

## HTTP/2 Activation (Future)

The key use case: a single H2 connection multiplexes many requests. Activating on the TCP connection means waking the VM for every new connection even if the client is just opening a persistent connection. Activating per-stream means the VM only wakes when an actual request arrives.

The fabric would need to:
1. Track TCP state and reassemble the stream
2. Parse H2 frame headers (9 bytes each) — only needs HEADERS frame detection
3. Maintain the H2 connection to the client (respond to SETTINGS, PING, WINDOW_UPDATE)
4. On new stream (HEADERS frame): emit activation event, buffer the frame
5. Once VM is up: replay buffered frames or splice the connection through

This is additive — the layered design means TCP activation works first, H2 parsing is added on top later.

## Async Runtime

Introduce **tokio** at the fabric layer. The event loop polls multiple TAP fds concurrently using `AsyncFd`. This is a natural fit — the fabric is fundamentally an async I/O multiplexer over many sockets.

The existing synchronous VMM/vsock/orchestration code can coexist initially and migrate to async incrementally.

## Implementation Order

### Phase 1: Port + Basic Switch ✓ DONE
- Move diagnostic loop from `orchestrate.rs` into `fabric/`
- Wrap TapDevice socket in tokio `AsyncFd`
- Implement MAC table and frame forwarding
- ARP responder for gateway IP
- Result: two VMs on the same subnet can communicate

#### Implementation notes
- `Port` uses `dup()` on the AF_PACKET socket fd — one fd stays in `TapDevice` (which owns Drop cleanup for the TAP device), the dup'd fd goes into `AsyncFd`. Both are set `O_NONBLOCK`.
- `VmInstance` trait gained `take_tap(&mut self) -> Option<TapDevice>` so the orchestrator can transfer ownership to the fabric.
- Per-port architecture: each port spawns its own tokio task that reads frames and does MAC learning + forwarding inline (no central forwarding task). Shared state is `Arc<Mutex<MacTable>>` and `Arc<Mutex<HashMap<PortId, SharedPort>>>` — locks held only briefly for lookups.
- The orchestrator creates a separate `tokio::runtime::Runtime` for the fabric and runs it in a background thread via `rt.block_on(pending())`. The fabric and runtime are kept alive via `Arc`.
- Gateway ARP: responds to ARP requests for 172.16.0.1 with synthetic MAC `02:00:00:00:00:01`. ARP replies are sent back on the same port (no forwarding). Gateway doesn't actually route yet — that's Phase 2.
- Rust 2024 edition: closure patterns on `HashMap::iter()` can't use `(&id, _)` — the edition's implicit borrow rules require either explicit `&` on the outer pattern or avoiding destructuring. Used a `for` loop instead of iterator chains for clarity.

### Phase 2: External Connectivity
- Gateway port that provides NAT or bridge to host networking
- Containers can reach the internet
- IP address allocation (replace hardcoded `172.16.0.2`)

### Phase 3: TCP-Level Activation
- TCP connection tracking (SYN detection)
- Dormant service registry
- Activation events on SYN to dormant service
- Packet buffering during VM boot, delivery once port is added

### Phase 4: HTTP/2 Activation
- H2 frame parsing on tracked TCP connections
- Per-stream activation instead of per-connection
- H2 connection maintenance (SETTINGS/PING/WINDOW_UPDATE proxying)
- Buffered frame replay to woken VM
