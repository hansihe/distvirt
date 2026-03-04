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

### Signal propagation

`SetBackendNeed` actions are tracked per-instance (`ActivatorInstance.last_backend_need`). The fabric emits `FabricEvent::ServiceBackendNeed` which the worker's event bridge forwards to the orchestrator as `WorkerEvent::ServiceBackendNeed`.

## Interface: Batched Event Processing

The WASM interface uses a **batched event/action model**. The fabric collects pending events and delivers them in a single call. The activator processes the batch and returns a list of actions. This amortizes WASM boundary crossing costs and makes the interaction pattern explicit — each `process-events` call is a synchronous exchange of events for actions.

### WIT Interface

The full interface definition is in [`distvirt-activator/wit/activator.wit`](../distvirt-activator/wit/activator.wit). Key elements:

- **Types**: `packet-flow` (u64, L3 flow correlation), `stream-handle` (u64, L4 TCP connection), `packet-info` (parsed packet with metadata + raw frame), `backend-need` (none/traffic/active)
- **Events** (fabric → activator): `backend-available`, `tick`, `packet`, `stream-open`/`stream-data`/`stream-close`, `upstream-connect-result`/`upstream-data`/`upstream-close`
- **Actions** (activator → fabric): `set-backend-need`, `log`, `packet-decision`/`packet-reply`/`replay-packet`, `downstream-send`/`downstream-close`/`pause-downstream`/`resume-downstream`, `upstream-connect(port)`/`upstream-send`/`upstream-close`/`pause-upstream`/`resume-upstream`
- **Entry point**: `process-events: func(events: list<event>) -> list<action>` — single synchronous batch exchange

The fabric collects pending events (incoming packets, stream data, lifecycle transitions, ticks), calls `process-events` once with the full batch, and executes the returned actions.

### Backpressure

Activators control data delivery rate through `pause-downstream` / `resume-downstream` and `pause-upstream` / `resume-upstream` actions. When a stream is paused, the fabric buffers incoming data at the TCP level (smoltcp applies TCP window pressure) and withholds `stream-data` / `upstream-data` events until the activator resumes. This lets the activator manage its own buffer pressure without unbounded memory growth.

### Integration with ServiceEntity

The `ServiceEntity` holds a `ServiceProcessor` that determines how frames are handled:

```rust
enum ServiceProcessor {
    /// No activator — buffer all frames, activate on first frame.
    Passthrough,
    /// L3 packet-level processing via WASM activator.
    L3 {
        activator: ActivatorInstance,
        flow_tracker: FlowTracker,
    },
    /// L4 stream-level processing via smoltcp + optional WASM activator.
    L4 {
        activator: Option<ActivatorInstance>,
        stream_manager: StreamManager,
    },
}
```

When a `ServiceProcessor` is present (L3 or L4), incoming frames are parsed by the fabric and routed through the processor before any buffering/forwarding decision. When `Passthrough`, the existing behavior (buffer everything, activate on first frame) is preserved — no WASM overhead.

**L3 path**: `FlowTracker` assigns stable `packet-flow` IDs by 5-tuple `(src_ip, dst_ip, protocol, src_port, dst_port)`. The frame is parsed via etherparse, wrapped in a `PacketInfo`, and delivered as a `Packet` event. The activator's `PacketDecision` determines buffering.

**L4 path**: The frame (with vnet header stripped) is fed to `StreamManager`, which manages smoltcp's TCP stack. The stream manager generates events (StreamOpen, StreamData, etc.) which are batched and passed to the activator. A bounded event loop (4 rounds max) separates L4 actions (executed by the stream manager — sends, connects, pause/resume) from non-L4 actions (returned to the fabric — replay, backend need, log).

**Replay with NAT**: When an activator emits `ReplayPacket`, the fabric applies DNAT (rewrites dst IP from service IP to backend pod IP) and inserts a reverse NAT entry so return traffic is correctly SNATted back. This is handled transparently by `dispatch_action` in `forwarding.rs`.

## Multiple Instances

The fabric may instantiate **multiple WASM instances** of the same activator component for a single service, distributing load across instances.

**Routing**: The fabric routes events to instances based on the activator's downstream mode:
- **L3 activators**: Any instance can handle any packet — no stickiness needed. The fabric distributes freely.
- **L4 activators**: Streams are sticky to the instance that received the `stream-open` event. All subsequent events for that stream (data, close) go to the same instance. Upstream connections opened by an instance are also routed back to that instance.

This provides concurrency without requiring the activator itself to be thread-safe — each WASM instance is single-threaded and processes its own events independently.

## Activator Implementations

### Passthrough (default, native Rust)

The current behavior, staying in Rust. No WASM involved. Buffer all frames up to capacity, activate on first frame (debounced). This is the fallback for services with no protocol declaration.

### TCP Activator (`activators/tcp/`)

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

**Replay**: On `backend-available(true)`, the activator emits `replay-packet` actions containing the buffered raw frames. The fabric replays them toward the backend with DNAT (service IP → backend pod IP) and MAC rewriting. The backend's TCP stack sees the SYN, completes the handshake, and the connection proceeds normally. Client TCP retransmits cover timing gaps.

**No synthetic replies**: The TCP activator doesn't generate reply packets. It lets the client's TCP retry mechanism handle the delay.

### HTTP/2 Activator (`activators/http2/`)

**Mode**: L4 downstream, L4 upstream. Full proxy — the fabric manages TCP on both sides, the activator handles H2 framing.

**Implementation**: Full H2 connection state machine with frame parsing/serialization (`frame.rs`), per-downstream connection tracking (`connection.rs`), and multi-connection orchestration (`core.rs`).

**Downstream (client-facing)**:
- `stream-open`: New TCP connection. Connection enters `AwaitingPreface` phase.
- `stream-data`: Parse H2 frames. Handle connection preface, SETTINGS exchange (with ACK), PING (with PING ACK), WINDOW_UPDATE. Set `backend-need: active` on HEADERS (new H2 stream). Track per-stream end_stream flags for lifecycle.
- `stream-close`: Client disconnected. Clean up H2 connection state.

**Upstream (backend-facing)**:
- On `backend-available(true)` with buffered frames: Emit `upstream-connect` to open TCP connection to backend.
- `upstream-connect-result(ok)`: Send H2 connection preface, complete SETTINGS handshake, forward buffered H2 frames.
- `upstream-data`: Forward backend H2 responses to appropriate downstream client stream via `downstream-send`.
- `upstream-close`: Backend connection lost. Handle reconnect or propagate error to clients.

**Signaling**: `none` → HEADERS → `active` → last H2 stream closes → `none`. Global stream counting across all connections. Precise scale-to-zero — no timeout guessing.

**Connection lifecycle**: `AwaitingPreface` → `Handshaking` (SETTINGS exchange) → `Active` (frame forwarding) → `Closing`. Upstream has its own state machine: `None` → `Connecting` → `Handshaking` → `Ready` / `Failed`.

### PostgreSQL Activator (`activators/postgres/`) — Prototype

**Mode**: L4 downstream, L4 upstream. Full proxy with Postgres wire protocol awareness.

**Status**: Functional prototype. Not yet wired into `ActivatorConfig` — no worker-protocol variant exists yet.

**Implementation**: Postgres wire protocol parsing (`protocol.rs`), per-connection state machine (`connection.rs`), multi-connection orchestration (`core.rs`).

**Key features**:
- **Wire protocol parsing**: SSLRequest detection, StartupMessage (version 3.0), tagged messages (tag byte + length + payload). Tracks ReadyForQuery status (Idle/InTransaction/Failed).
- **Health-check interception**: When idle, intercepts `SELECT 1` queries and fabricates a complete response (RowDescription + DataRow + CommandComplete + ReadyForQuery) without involving the backend. This prevents health checks from triggering backend activation.
- **Smart backend need**: `none` when all connections are idle or in startup; `active` when any connection has buffered startup data or is in-transaction.
- **Startup buffering**: Buffers the initial StartupMessage until backend is available, then flushes on upstream connect.

**Signaling**: `none` (no connections / all idle) → startup received → `active` → auth complete + ReadyForQuery(Idle) → `none`. Re-enters `active` on queries, returns to `none` on ReadyForQuery(Idle).

### Future: UDP / DNS Activator

**Mode**: L3 downstream. Inspects UDP packets, parses DNS queries from payload, activates on real queries, drops health-check probes. Pure L3, no streams. The interface supports UDP through `ip-protocol::udp` in `packet-info`.

## Codebase Structure

### `distvirt-activator/` — Host-side framework

| File | Purpose |
|------|---------|
| `src/lib.rs` | Module exports, wasmtime component bindings generation |
| `src/types.rs` | Host-side `PacketInfo` and `Event` types with `std::net::IpAddr` fields; conversion to/from portable `activator_types` |
| `src/packet_parse.rs` | L3 parsing via etherparse + `FlowTracker` (5-tuple → stable u64 flow ID assignment) |
| `src/stream_manager.rs` | L4 `StreamManager`: smoltcp-backed TCP stack with `FabricDevice` (queue-based phy), listening socket pool, downstream/upstream stream tracking, pause/resume, ephemeral port allocation (49152-65535) |
| `src/runtime.rs` | `ActivatorRuntime`: scans component directory for `.wasm` files, pre-loads by filename (e.g. `tcp.wasm` → "tcp") |
| `src/instance.rs` | `ActivatorInstance`: per-service WASM instance with `Store<HostState>` (WasiCtx + ResourceTable), pending event queue, fuel budget (1,000,000 per `process_events` call) |
| `wit/activator.wit` | WIT interface definition |
| `tests/wasm_integration.rs` | 26 integration tests covering runtime loading, event/action roundtrips, fuel exhaustion |

### `activators/` — WASM component crates

| Directory | Purpose |
|-----------|---------|
| `activator-types/` | Portable types (no_std, WASM-compatible) shared between guest and host. `Event`, `Action`, `BackendNeed` enums, `Activator` trait for native testing, `test_helpers.rs` with packet/frame builders |
| `tcp/` | TCP activator: SYN-based flow tracking, RST dropping, frame buffering + replay, 1024-flow cap |
| `http2/` | H2 activator: full proxy with frame parsing, connection state machine, stream multiplexing |
| `postgres/` | PostgreSQL activator (prototype): wire protocol parsing, idle detection, `SELECT 1` health-check interception |
| `test-echo/` | Deterministic test fixture exercising all event/action variants |
| `spin/` | Infinite loop component for fuel exhaustion testing |
| `build.sh` | Build script: iterates activator dirs, runs `cargo component build --release`, copies `.wasm` to `target/components/` |

All activator crates are `cargo-component` crates targeting `wasm32-wasip1`, producing WASM components via the Component Model.

### Fabric-side integration (`distvirt-worker/src/fabric/`)

| File | Activator role |
|------|----------------|
| `service_activator.rs` | `ServiceProcessor` enum (Passthrough/L3/L4), frame processing, bounded L4 event loop |
| `service.rs` | `ServiceEntity` holds `ServiceProcessor`, delegates to it in `lookup_and_buffer` and `mark_ready` |
| `forwarding.rs` | `dispatch_action` executes `ReplayPacket` (with DNAT + NAT entry), `SetBackendNeed`, `Log` |
| `mod.rs` | `Fabric.dispatch_actions()` and `send_l4_frames()` entry points |

### Worker integration (`distvirt-worker/src/worker/`)

`Worker` holds `ActivatorRuntime` (optional). On `create_service`, the runtime instantiates the appropriate WASM component based on `ActivatorConfig` → `ServiceProcessor`. `ServiceBackendNeed` events are bridged from fabric → worker → orchestrator via the event bridge task.

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

WASM instances are configured with a **fuel budget** of 1,000,000 per `process-events` call via wasmtime. If an activator exhausts its fuel (e.g. infinite loop), the call returns an error and the fabric handles it gracefully. This prevents a buggy activator from blocking the fabric event loop.

### Native transport infrastructure

The fabric provides L3 and L4 transport in native Rust code, shared across all activators:

- **L3**: [etherparse](https://github.com/JulianSchmid/etherparse) for packet parsing and construction. Zero-allocation, `no_std`. Every incoming frame is parsed before reaching an activator. `FlowTracker` in `packet_parse.rs` assigns stable u64 flow IDs by 5-tuple for `packet-flow` handles.
- **L4**: [smoltcp](https://github.com/smoltcp-rs/smoltcp) for TCP connection management. `no_std`, full TCP state machine. The fabric acts as a real TCP endpoint — handles SYN/ACK, windowing, retransmits — and presents byte streams to activators. `StreamManager` uses a `FabricDevice` (queue-based phy device) and manages listening socket pools, upstream connections, and per-stream pause state. ARP resolution for backend IPs is handled via synthetic ARP reply injection into smoltcp's RX queue. Note: smoltcp targets embedded environments and may lack some TCP extensions (SACK, timestamps, ECN) that real-world clients use. Acceptable for staging environments; worth monitoring for edge cases.

This means L4 activators (H2) never see TCP headers or construct packets. The fabric handles all of that natively.

### Module loading

Compiled WASM components are **bundled with the worker binary** for standard protocols (TCP, H2). `ActivatorRuntime` scans a component directory at startup, loading `.wasm` files by filename (e.g. `tcp.wasm` is registered as component "tcp"). Build via `./activators/build.sh`. Future: service configs could reference components from a registry for custom protocols.

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

## Implementation Status

### Complete

- **WIT interface** — `distvirt-activator/wit/activator.wit`. Types, events, actions, `process-events` entry point.
- **L3 packet parsing** — `distvirt-activator/src/packet_parse.rs`. etherparse-based, `FlowTracker` for stable flow IDs.
- **WASM runtime** — `ActivatorRuntime` (component directory scanner) and `ActivatorInstance` (fuel-limited WASM execution) in `distvirt-activator/src/`.
- **L4 stream management** — `StreamManager` in `distvirt-activator/src/stream_manager.rs`. smoltcp-backed TCP stack with `FabricDevice`, listening socket pool, upstream connection support, backpressure via pause/resume.
- **Fabric integration** — `ServiceProcessor` (Passthrough/L3/L4) in `service_activator.rs`. L3/L4 branches in `lookup_and_buffer`, action dispatch in `forwarding.rs` (ReplayPacket with DNAT + NAT entry, SetBackendNeed, Log). `ServiceAction::L4Result` for stream manager output.
- **Worker integration** — `Worker` holds `ActivatorRuntime`, `handle_create_service` instantiates components based on `ActivatorConfig`, `ServiceBackendNeed` events bridged to orchestrator.
- **TCP activator** — `activators/tcp/`. SYN-based flow tracking, RST dropping, frame buffering + replay, 1024-flow cap.
- **HTTP/2 activator** — `activators/http2/`. Full H2 proxy with frame parsing, SETTINGS/PING handling, connection state machine, stream multiplexing, per-stream backend need signaling.
- **PostgreSQL activator** (prototype) — `activators/postgres/`. Wire protocol parsing, startup buffering, idle detection, `SELECT 1` health-check interception. Not yet wired into `ActivatorConfig`.
- **Test infrastructure** — `test-echo` (deterministic fixture), `spin` (fuel exhaustion test), 26 integration tests in `distvirt-activator/tests/wasm_integration.rs`. Build via `./activators/build.sh`.

### Remaining Work

- **PostgreSQL `ActivatorConfig` variant** — add `Postgres` to `ActivatorConfig` enum and worker-protocol schema so the postgres activator can be selected via `CreateService`
- **End-to-end testing** — test H2 and postgres activators against real clients (curl, browsers, gRPC clients, psql)
- **gRPC activator** — extends H2 with protobuf-aware stream detection
- **WebSocket activator**
- **UDP/DNS activator**
- **Stream splicing optimization** — `splice(downstream, upstream)` action to bypass activator for established connections
- **Multiple instances per service** — load distribution with L3 free routing, L4 stream stickiness
- **Component registry** — dynamic loading of custom/user-provided activator components
