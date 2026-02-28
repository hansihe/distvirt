# Protocol Activators

Protocol activators provide protocol-aware activation and traffic management for service entities on the networking fabric. They replace the default "buffer everything, activate on any frame" behavior with protocol-specific logic — detecting meaningful traffic (TCP SYN, HTTP/2 request), filtering noise (RSTs, keepalives), and optionally acting as a full protocol-aware proxy (H2 connection management, stream multiplexing).

## Motivation

The current service entity treats all frames equally. Any frame to a not-ready service triggers an activation event and gets buffered. This works but is coarse:

- **Wasted activations**: A TCP RST or stale keepalive probe triggers the same activation as a real client request.
- **No connection awareness**: Frames are buffered in a flat queue with no per-flow tracking. Replay works but the activator can't make protocol-informed decisions about what to buffer.
- **H2 multiplexing is invisible**: Every new TCP connection triggers activation. With HTTP/2, a single long-lived connection multiplexes many requests — per-stream activation would let the pod sleep between requests on the same connection.
- **No scale-to-zero signal**: The fabric has no way to know when all meaningful work is done. Session-aware protocols (H2) know exactly when the last active stream closes, but that information isn't surfaced.

For simple protocols (raw TCP services), the cost of coarse activation is low. For connection-oriented protocols with multiplexing (H2, gRPC, WebSocket), protocol awareness enables meaningfully better activation and scale-to-zero behavior.

## Design: WASM Components

Protocol activators are implemented as **WebAssembly components** loaded at runtime, rather than being compiled into the fabric.

### Why WASM

A TCP activator is simple — check TCP flags, track flows. This could live in Rust in the fabric. But an H2 activator requires H2 frame parsing, connection maintenance (SETTINGS exchange, PING/ACK, WINDOW_UPDATE), and proxying of active connections. That's proxy-level code. And beyond H2, there's gRPC (H2 + protobuf framing), WebSocket, MQTT, and whatever comes next. Each protocol is a self-contained proxy implementation.

Embedding all of this in the fabric is a massive overextension of what the fabric should be — a dumb L2 switch with minimal awareness of what's flowing through it. WASM components solve this:

- **Complexity containment**: Each activator is a self-contained component with its own development, testing, and compilation lifecycle. The H2 activator can be as complex as it needs to be without touching the fabric codebase.
- **Independent development**: Protocol activators can be written, tested, and released independently. A new gRPC activator doesn't require changes to the fabric or worker.
- **Language flexibility**: Activators can be written in any language that compiles to WASM components (Rust, Go, C, etc.), though Rust is the natural first choice.
- **Sandboxing**: WASM provides memory isolation. A buggy activator can't corrupt fabric state.

### Performance is acceptable

Activators are in the frame path for the lifetime of connections they handle. All traffic to a service with an activator flows through the activator — there is no bypass/splice path (see [Future: Stream Splicing](#future-stream-splicing)).

This system targets staging environments where network throughput is not the primary optimization target. The latency budget during "pod is booting" is measured in seconds, not microseconds. WASM overhead is negligible in that context.

## Architecture: Layered Transport

The fabric provides **layered transport abstractions** to activators, handling L3 (packet parsing) and L4 (TCP stream management) in native code. This avoids every activator reimplementing Ethernet/IP/TCP parsing and lets protocol-specific activators focus on their actual protocol logic.

### Transport Layers

| Layer | Fabric provides | Activator sees |
|-------|----------------|----------------|
| **L3 (packet)** | Parsed Ethernet/IP/TCP/UDP headers | Packet metadata + payload + raw frame, per-flow correlation |
| **L4 (stream)** | Full TCP connection management | Byte streams with flow lifecycle events |

The fabric uses [etherparse](https://github.com/JulianSchmid/etherparse) (no_std, zero-alloc) for L3 packet parsing on all incoming frames. For L4, the fabric manages TCP state (SYN/ACK handshake, sequence numbers, windowing, retransmits) using [smoltcp](https://github.com/smoltcp-rs/smoltcp) (no_std, full TCP stack), presenting activators with reassembled byte streams.

An activator declares its downstream mode (L3 or L4) at instantiation. The upstream side has no declared mode — the activator simply uses whichever actions it needs (packet replay for L3, `upstream-connect` for L4).

### Two-Sided Proxy Model

Activators are not just filters — they are **proxies** with two independent sides:

```
              Downstream (clients)          Upstream (service)
              ┌─────────────────┐           ┌─────────────────┐
              │  L3 or L4       │           │  L3 or L4       │
              │  (fabric-owned) │           │  (fabric-owned) │
              └────────┬────────┘           └────────┬────────┘
                       │                             │
                       ▼                             ▼
              ┌──────────────────────────────────────────────┐
              │              Activator (WASM)                │
              │                                              │
              │  - receives downstream events                │
              │  - produces downstream replies                │
              │  - requests upstream connections              │
              │  - sends/receives upstream data               │
              │  - signals backend need                       │
              └──────────────────────────────────────────────┘
```

Simple activators (TCP) use L3 downstream and replay buffered packets upstream — they're filters, not proxies. Complex activators (H2) use L4 on both sides — the fabric manages TCP connections to both clients and the backend, and the activator bridges them at the application protocol level.

This means the H2 activator never parses a TCP header or constructs an Ethernet frame. It receives H2 byte streams from clients, parses H2 frames, and sends H2 bytes upstream. All packet construction, TCP state, and connection management is handled by the fabric in native code.

## Signaling: Backend Need

The activator and fabric communicate backend availability through signals:

**Activator → Fabric** (trinary):

```wit
enum backend-need {
    /// No meaningful traffic. Backend may be released.
    none,
    /// Instantaneous pulse: saw meaningful traffic just now.
    /// Semantically equivalent to "active then immediately none."
    /// The fabric timestamps receipt and applies its own timeout policy.
    traffic,
    /// Active sessions require a backend.
    /// Backend must stay up as long as this is asserted.
    active,
}
```

**Fabric → Activator** (boolean): "a backend is available" / "no backend available."

### Signal semantics

`traffic` is a **pulse signal** — it communicates "something meaningful just happened" without the activator needing to track ongoing state. The fabric timestamps each `traffic` signal and applies a configurable timeout policy: if no further `traffic` (or transition to `active`) within the timeout window, the fabric may release the backend. The activator does not need to reset the signal back to `none`; `traffic` is inherently instantaneous.

`active` is a **level signal** — asserted for the duration of active sessions and cleared when the last session ends.

### How activators use the signals

**TCP activator** (not session-aware): SYN arrives → `traffic`. Each new connection attempt pulses `traffic`. The fabric's timeout policy governs when the backend is released. The TCP activator never asserts `active` because it doesn't track session lifecycle.

**H2 activator** (session-aware): HEADERS arrives → `active`. Last H2 stream closes → `none`. Never uses `traffic` — it always knows exactly whether work is active. This gives precise scale-to-zero: the backend can be released the moment the last stream closes, no timeout guessing needed.

```
TCP:   ──SYN──→ traffic(pulse) ──SYN──→ traffic(pulse)
                        fabric timeout governs release

H2:    ──HEADERS──→ active ──last stream closes──→ none
                           precise lifecycle
```

The `traffic` vs `active` distinction encodes whether the activator tracks session lifecycle. Session-aware activators use `none`/`active`. Non-session-aware activators use `none`/`traffic` and delegate the "when is it over" question to fabric timeout policy.

## Interface: Batched Event Processing

The WASM interface uses a **batched event/action model**. The fabric collects pending events and delivers them in a single call. The activator processes the batch and returns a list of actions. This amortizes WASM boundary crossing costs and makes the interaction pattern explicit — each `process-events` call is a synchronous exchange of events for actions.

### Type Definitions

```wit
/// L3 flow — fabric-tracked packet correlation by (src, dst, ports, protocol) tuple.
type packet-flow = u64;

/// L4 stream — fabric-managed TCP connection with byte-stream semantics.
/// Distinct type from packet-flow; L3 and L4 handles cannot be mixed.
type stream = u64;

enum ip-protocol {
    tcp,
    udp,
    other,
}

record packet-info {
    flow: packet-flow,
    src-addr: list<u8>,     // 4 (v4) or 16 (v6) bytes
    dst-addr: list<u8>,
    src-port: u16,
    dst-port: u16,
    protocol: ip-protocol,
    tcp-flags: option<u8>,  // present only for TCP
    payload: list<u8>,
    raw-frame: list<u8>,    // original frame bytes, for activator-owned buffering/replay
}

record stream-data-event {
    s: stream,
    data: list<u8>,
}

record upstream-connect-result-event {
    s: stream,
    result: connect-result,
}

enum packet-decision {
    /// Accept — activator takes ownership and may buffer for replay.
    buffered,
    /// Drop the packet.
    drop,
}

enum connect-result {
    ok,
    refused,
    timeout,
}

enum backend-need {
    none,
    traffic,
    active,
}

enum log-level {
    trace,
    debug,
    info,
    warn,
    error,
}

record log-action {
    level: log-level,
    message: string,
}
```

### Events (fabric → activator)

```wit
variant event {
    /// Backend availability changed.
    backend-available(bool),
    /// Periodic housekeeping tick.
    tick,

    // --- L3 (packet) downstream ---

    /// Incoming packet for the service.
    packet(packet-info),

    // --- L4 (stream) downstream ---

    /// New incoming TCP connection from a client.
    stream-open(stream),
    /// Data received from a client.
    stream-data(stream-data-event),
    /// Client closed/reset the connection.
    stream-close(stream),

    // --- L4 (stream) upstream ---

    /// Result of a prior upstream-connect action.
    upstream-connect-result(upstream-connect-result-event),
    /// Data received from the backend.
    upstream-data(stream-data-event),
    /// Backend closed/reset the connection.
    upstream-close(stream),
}
```

### Actions (activator → fabric)

```wit
variant action {
    /// Assert backend need level.
    set-backend-need(backend-need),
    /// Emit a log message.
    log(log-action),

    // --- L3 (packet) ---

    /// Decision for the most recently received packet event.
    packet-decision(packet-flow, packet-decision),
    /// Send a raw packet back toward a downstream packet-flow's source.
    packet-reply(packet-flow, list<u8>),
    /// Replay a raw frame toward the backend (L3 activators, after backend available).
    replay-packet(list<u8>),

    // --- L4 (stream) downstream ---

    /// Send bytes to a downstream client on an existing stream.
    downstream-send(stream, list<u8>),
    /// Close a downstream stream.
    downstream-close(stream),
    /// Pause delivery of stream-data events for this stream (backpressure).
    pause-downstream(stream),
    /// Resume delivery of stream-data events for this stream.
    resume-downstream(stream),

    // --- L4 (stream) upstream ---

    /// Request a new TCP connection to the backend. Returns a stream handle.
    upstream-connect,
    /// Send bytes on an upstream stream.
    upstream-send(stream, list<u8>),
    /// Close an upstream stream.
    upstream-close(stream),
    /// Pause delivery of upstream-data events for this stream (backpressure).
    pause-upstream(stream),
    /// Resume delivery of upstream-data events for this stream.
    resume-upstream(stream),
}
```

### Entry Point

```wit
/// Process a batch of events. Returns a list of actions for the fabric to execute.
process-events: func(events: list<event>) -> list<action>;
```

The fabric collects pending events (incoming packets, stream data, lifecycle transitions, ticks), calls `process-events` once with the full batch, and executes the returned actions. This replaces the traditional import/export split with a single synchronous exchange.

### Backpressure

Activators control data delivery rate through `pause-downstream` / `resume-downstream` and `pause-upstream` / `resume-upstream` actions. When a stream is paused, the fabric buffers incoming data at the TCP level (smoltcp applies TCP window pressure) and withholds `stream-data` / `upstream-data` events until the activator resumes. This lets the activator manage its own buffer pressure without unbounded memory growth.

### Integration with ServiceEntity

The `ServiceEntity` holds an optional activator handle (WASM instance). When present, incoming frames are parsed by the fabric (L3) and optionally fed through the fabric's TCP stack (L4) before being delivered to the activator. When absent, the existing passthrough behavior (buffer everything, activate on first frame) is preserved — no WASM overhead for services that don't declare a protocol.

```
ServiceEntity {
    // ... existing fields (service_id, ip, mac, policy, backend, ready) ...
    activator: Option<ActivatorInstance>,  // WASM component instance
}
```

## Multiple Instances

The fabric may instantiate **multiple WASM instances** of the same activator component for a single service, distributing load across instances.

**Routing**: The fabric routes events to instances based on the activator's downstream mode:
- **L3 activators**: Any instance can handle any packet — no stickiness needed. The fabric distributes freely.
- **L4 activators**: Streams are sticky to the instance that received the `stream-open` event. All subsequent events for that stream (data, close) go to the same instance. Upstream connections opened by an instance are also routed back to that instance.

This provides concurrency without requiring the activator itself to be thread-safe — each WASM instance is single-threaded and processes its own events independently.

## Activator Implementations

### Passthrough (default, native Rust)

The current behavior, staying in Rust. No WASM involved. Buffer all frames up to capacity, activate on first frame (debounced). This is the fallback for services with no protocol declaration.

### TCP Activator

**Mode**: L3 downstream. No upstream connection management — uses `replay-packet` actions to replay buffered frames.

**Behavior**:

| Packet type | Decision | Backend need? |
|---|---|---|
| TCP SYN (new flow) | `buffered` | `traffic` |
| TCP SYN (retransmit, known flow) | `buffered` | — |
| TCP non-SYN (known flow) | `buffered` | — |
| TCP non-SYN (unknown flow) | `buffered` | `traffic` (conservative — may have missed SYN) |
| TCP RST | `drop` | — |
| Non-TCP (if tcp_only policy) | `drop` | — |
| Non-TCP (if not tcp_only) | `buffered` | — |

**State**: Per-flow entries keyed by `(src_ip, src_port, dst_port)` with first-seen timestamps. No TCP sequence number tracking — just enough to distinguish "new connection attempt" from "ongoing traffic." Capped at `max_flows` (configurable, default 1024).

**Buffering**: The activator owns all buffering. For efficiency, the TCP activator only buffers one SYN per source flow — additional packets for the same flow are accepted (`buffered`) but not stored. The `raw-frame` field in `packet-info` provides the original frame bytes for storage without reconstruction.

**Replay**: On `backend-available(true)`, the activator emits `replay-packet` actions containing the buffered raw frames. The fabric replays them toward the backend with MAC rewriting. The backend's TCP stack sees the SYN, completes the handshake, and the connection proceeds normally. Client TCP retransmits cover timing gaps.

**No synthetic replies**: The TCP activator doesn't generate reply packets. It lets the client's TCP retry mechanism handle the delay.

### HTTP/2 Activator

**Mode**: L4 downstream, L4 upstream. Full proxy — the fabric manages TCP on both sides, the activator handles H2 framing.

**Downstream (client-facing)**:
- `stream-open`: New TCP connection. Start reading H2 connection preface.
- `stream-data`: Parse H2 frames from byte stream. Respond to SETTINGS with SETTINGS ACK, PING with PING ACK via `downstream-send`. Set `backend-need: active` on HEADERS (new H2 stream = new request).
- `stream-close`: Client disconnected. Clean up associated H2 state.

**Upstream (backend-facing)**:
- On `backend-available(true)`: Emit `upstream-connect` actions to open TCP connections to the backend. May open M connections for N client connections (N:M multiplexing).
- `upstream-connect-result(ok)`: Send H2 connection preface, forward buffered H2 streams.
- `upstream-data`: Forward backend H2 responses to appropriate downstream client streams via `downstream-send`.
- `upstream-close`: Backend connection lost. Handle reconnect or propagate error to clients.

**Signaling**: `none` → HEADERS → `active` → last H2 stream closes → `none`. Precise scale-to-zero — no timeout guessing.

**Complexity**: The H2 activator is a substantial component — H2 frame parsing, connection preface handling, stream multiplexing, connection maintenance. But it never touches TCP or packet construction — that's all handled by the fabric's native L4 layer. The activator is pure H2 logic operating on byte streams.

### Future: UDP / DNS Activator

**Mode**: L3 downstream. Inspects UDP packets, parses DNS queries from payload, activates on real queries, drops health-check probes. Pure L3, no streams. The interface supports UDP through `ip-protocol::udp` in `packet-info`.

## Runtime

### WASM runtime

[wasmtime](https://wasmtime.dev/) — Rust-native, mature component model support, pre-compilation for fast instantiation, instance pooling.

### Instance lifecycle

- **Service created with protocol config** → Load and instantiate the appropriate WASM component. Each service entity gets one or more WASM instances (own linear memory, own state).
- **Frames arrive while not ready** → Fabric parses L3 headers, optionally manages L4 TCP state, collects events into a batch, calls `process-events` on the appropriate instance.
- **Backend becomes available** → Fabric delivers `backend-available(true)` event. L3 activators respond with `replay-packet` actions. L4 activators respond with `upstream-connect` actions and start proxying.
- **Backend goes away** → Fabric delivers `backend-available(false)` event. Activator handles gracefully (buffer new traffic, GOAWAY existing connections, etc).
- **Service destroyed** → Drop all WASM instances.

### Resource limits

WASM instances are configured with **memory limits** via wasmtime to prevent a buggy activator from consuming unbounded memory. Default limits are per-activator-type and configurable through `ActivatorConfig`.

### Native transport infrastructure

The fabric provides L3 and L4 transport in native Rust code, shared across all activators:

- **L3**: [etherparse](https://github.com/JulianSchmid/etherparse) for packet parsing and construction. Zero-allocation, `no_std`. Every incoming frame is parsed before reaching an activator.
- **L4**: [smoltcp](https://github.com/smoltcp-rs/smoltcp) for TCP connection management. `no_std`, full TCP state machine. The fabric acts as a real TCP endpoint — handles SYN/ACK, windowing, retransmits — and presents byte streams to activators. Note: smoltcp targets embedded environments and may lack some TCP extensions (SACK, timestamps, ECN) that real-world clients use. Acceptable for staging environments; worth monitoring for edge cases.

This means L4 activators (H2) never see TCP headers or construct packets. The fabric handles all of that natively.

### Module loading

Compiled WASM components are **bundled with the worker binary** for standard protocols (TCP, H2). The worker loads them from a known path at startup. Future: service configs could reference components from a registry for custom protocols.

### Configuration

Protocol activator selection is part of `ServicePolicy`, passed through `CreateService`:

```rust
struct ServicePolicy {
    pub buffer_frames: u32,
    pub timeout_ms: u32,
    pub activator: Option<ActivatorConfig>,
}

enum ActivatorConfig {
    Tcp {
        /// Ports to apply SYN-based activation to (None = all ports)
        ports: Option<Vec<u16>>,
        /// Drop non-TCP frames
        tcp_only: bool,
        /// Max tracked flows
        max_flows: u32,
    },
    Http2 {
        // H2-specific configuration TBD
    },
    // Future protocols...
}
```

When `activator` is `None`, the service uses the native passthrough behavior.

## Future: Stream Splicing

A future optimization: `action::splice(downstream-stream, upstream-stream)` would tell the fabric to bypass the activator for a specific stream pair, forwarding data directly between downstream and upstream at the fabric level. Once spliced, no more events are delivered for those streams. The activator steps out of the data path entirely for spliced connections.

This is a natural extension of the batched action model — just another action variant. Not needed initially; all traffic flows through the activator for now.

## Implementation Phases

### Phase 1: Activator framework + TCP activator

1. Add wasmtime dependency to the worker.
2. Define the WIT interface (types, events, actions, `process-events` entry point).
3. Implement L3 packet parsing layer using etherparse.
4. Implement the WASM loading and instantiation infrastructure in the fabric.
5. Integrate the activator call path into `ServiceEntity` — event batching, `process-events` dispatch, action execution.
6. Build the TCP activator as a WASM component (Rust guest code).
7. Wire up `ActivatorConfig` through `ServicePolicy` and `CreateService`.

The TCP activator is simple enough to validate the entire framework end-to-end: WIT interface design, WASM call overhead, batched event/action flow, L3 parsing, signaling, replay.

### Phase 2: H2 activator

1. Implement L4 stream management layer using smoltcp — TCP connection handling, byte stream delivery to activators.
2. Implement upstream connection support — `upstream-connect` action handling, connection lifecycle.
3. Implement backpressure — pause/resume actions, TCP window integration.
4. Build H2 activator component with H2 frame parsing and connection maintenance.
5. Per-stream activation on HEADERS frames.
6. Connection proxying with N:M multiplexing between client and backend connections.
7. Test against real H2 clients (curl, browsers, gRPC clients).

### Phase 3: Ecosystem

- gRPC activator (extends H2 with protobuf-aware stream detection)
- WebSocket activator
- UDP/DNS activator
- Stream splicing optimization
- Multiple instances per service with routing
- Custom/user-provided activator components
- Component registry for dynamic loading
