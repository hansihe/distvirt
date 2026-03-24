---
title: "distvirt — System Overview"
sidebar:
  order: 0
---

distvirt is a VM-based container runtime in Rust optimized for ultra-fast cold start. Containers run inside Firecracker microVMs with the container process as the only significant guest process — no nested container runtime inside the VM. The driving use case is **scale-to-zero staging environments**: a virtualized network fabric where VMs are spun up on demand when traffic arrives for a dormant service.

---

## Crate Structure

Workspace members (from `Cargo.toml`):

| Crate | Role |
|-------|------|
| `distvirt-worker` | Worker process — VMM management, networking fabric, image provider, pod lifecycle |
| `distvirt-worker-protocol` | Orchestrator↔worker protocol types + yamux/Cap'n Proto transport |
| `distvirt-guest-protocol` | Shared host↔guest message types (serde, musl-compatible) |
| `distvirt-orchestrator` | Orchestrator state machine + async shell + gRPC server |
| `distvirt-client-protocol` | Client↔orchestrator gRPC protocol (tonic + prost, `.proto` definitions) |
| `distvirt-cli` | CLI binary (`dv` commands — compose-up, status, etc.) |
| `distvirt-activator` | Protocol activator runtime (WASM component support) |
| `distvirt-tests` | Orchestrator scenario tests (harness + 12 scenario modules) |
| `guest-image/guest-init` | Guest agent (PID 1 in the VM, static musl binary) |

The `activators/` directory (excluded from the workspace) contains standalone activator components: `tcp`, `http2`, `postgres`, `spin`, `test-echo`, and `activator-types`.

The `web/` directory contains the documentation site, built with [Astro Starlight](https://starlight.astro.build/).

---

## Architecture Layers

```
┌─────────────┐
│  CLI / UI   │  ── gRPC (tonic) ──►  ┌──────────────┐
└─────────────┘                        │ Orchestrator  │
                                       │ (state machine│
                                       │  + async shell)│
                                       └──────┬───────┘
                                              │ yamux + Cap'n Proto
                                              ▼
                                       ┌──────────────┐
                                       │   Worker      │
                                       │ (VMM, fabric, │
                                       │  images)      │
                                       └──────┬───────┘
                                              │ vsock + yamux + JSON
                                              ▼
                                       ┌──────────────┐
                                       │ Guest Agent   │
                                       │ (PID 1)       │
                                       └──────────────┘
```

1. **Guest agent** — PID 1 in the microVM. Mounts container disks, configures networking, forks+execs workloads, streams output, reaps zombies. Communicates with the host over virtio-vsock.
2. **Worker** — Manages local Firecracker VMs, the per-namespace networking fabric, container image preparation, and pod lifecycle. Reports events to the orchestrator.
3. **Orchestrator** — Pure state machine that owns all planning: IP assignment, dependency ordering, service lifecycle, activation, and worker coordination. An async shell handles I/O. Internally structured as an outer orchestrator (worker management, scheduling, client routing) and per-namespace sub-state machines (workload lifecycle, services, reconciliation, WireGuard peers).
4. **CLI** (`dv`) — Two-layer design. Layer 1: task-oriented commands (`dv up`, `dv status`, `dv logs`, `dv connect`, `dv deactivate`, `dv splice`) with smart defaults and summarization. Layer 2: uniform resource commands (`dv get`, `dv describe`, `dv create`, `dv delete`) for scripting and power users. Authenticates via API key tokens stored in `~/.config/distvirt/credentials.toml` with named contexts. See [cli-design.md](cli-design.md) for full details.

---

## Host↔Guest Protocol

The host connects to the guest agent over **virtio-vsock** (port 1024), multiplexed via **yamux**. One control stream carries commands/events; additional streams carry container output.

Wire format: 4-byte LE length prefix + **JSON** body (serde). Shared via the `distvirt-guest-protocol` crate.

Key messages:
- **Host → Guest**: `AddContainer`, `StartContainer`, `ConfigureNetwork`, `SignalContainer`, `SetClock`, `PrepareSuspend`, `Shutdown`
- **Guest → Host**: `Ready`, `ContainerAdded`, `ContainerStarted`, `ContainerExited`, `ContainerSignaled`, `NetworkConfigured`, `ClockSet`, `SuspendReady`, `Error`

Container output uses a separate yamux stream per container with framed `[stream_id: u8][length: u32 LE][payload]` chunks (1=stdout, 2=stderr).

---

## Worker↔Orchestrator Protocol

See [worker-protocol.md](worker-protocol.md) for full details.

Transport: **yamux** over any async byte stream (in-process `tokio::io::duplex` for local mode, TCP/TLS for future distributed mode). The control stream carries length-prefixed **Cap'n Proto** messages. Schema at `distvirt-worker-protocol/schema/worker_protocol.capnp`.

Three-step handshake: `WorkerHello` → `WorkerAccepted` → `WorkerReady`.

Key commands: `CreateNamespace`, `DestroyNamespace`, `LaunchPod`, `StopPod`, `SuspendPod`, `ResumePod`, `RegistrySync`, `RegistryUpdate`, `CreateService`, `UpdateServiceBackend`, `ServiceReady`, `DestroyService`, `FabricRouteSync`, `FabricRouteUpdate`, `AddWireGuardPeer`, `RemoveWireGuardPeer`, `DeleteSnapshot`, `Shutdown`.

Key events: `PodRunning`, `PodExited`, `PodFailed`, `PodSuspended`, `PodSuspendFailed`, `NamespaceCreated`, `NamespaceFailed`, `NamespaceDestroyed`, `ServiceActivation`, `ServiceBackendNeed`, `FabricRouteMiss`.

Log streams use separate yamux streams (out-of-band) to avoid head-of-line blocking.

---

## Client Protocol

See [client-protocol.md](client-protocol.md) for full details.

**gRPC** via tonic/prost. Proto definitions at `distvirt-client-protocol/proto/distvirt/client/v1/client.proto`.

Unary RPCs: `CreateNamespace`, `UpdateNamespace`, `DeleteNamespace`, `GetNamespaceStatus`, `ListNamespaces`, `Splice`, `Unsplice`, `CloneNamespace`, `ListWorkers`, `GetWorker`, `ListPods`, `DeactivateWorkload`, `ConnectNetwork`, `DisconnectNetwork`.

Server-streaming RPCs: `WatchNamespaceStatus`, `StreamLogs`, `StreamEvents`.

---

## Networking Fabric

See [networking-fabric.md](networking-fabric.md) for full details.

Per-namespace userspace **L3 IP router** with a smoltcp-based gateway. Each pod's TAP device is a port on the router.

- **IP fabric** — static IP-to-port table, IP-based packet forwarding (no MAC learning or flooding).
- **Gateway** (smoltcp, configurable per namespace, default 172.16.0.1) — DNS service discovery from local registry, internet egress via TUN device + NAT.
- **Services** — Virtual IP entities on the fabric with buffering policies and protocol activators for scale-to-zero activation.
- **Route table** — Pod-to-pod forwarding entries (remote worker or placeholder with buffer policy). Supports multi-worker fabric segments (future).

Traffic resolution order: local port (IP table) → service entity → route table → drop.

---

## Protocol Activators

See [protocol-activators.md](protocol-activators.md) for full details.

Activators are protocol-aware components that run on service entities in the fabric. They inspect traffic to make intelligent activation decisions (e.g., only activate on TCP SYN, not RST or stale keepalives).

Activator types:
- **TCP** — SYN-based activation, filters RSTs and stale keepalives, replays buffered SYNs to backend.
- **PostgreSQL** — Protocol-aware activation for Postgres connections.
- **HTTP/2** (future) — Full H2 proxy with per-stream activation.

Service processors support L3 (WASM-based flow tracking) and L4 (smoltcp-backed TCP stream management) modes.

Activators in the `activators/` directory are built as standalone components. The `distvirt-activator` crate provides the runtime.

---

## Ingress Adapters

See [ingress-adapters.md](ingress-adapters.md) for full details.

Ingress adapters bridge external traffic into the per-namespace fabric. Adapters are worker-level resources that present virtual ports (`FramePort`) into each namespace's fabric instance. Configuration and key material are delivered by the orchestrator during the worker handshake.

Adapter strategies:

- **WireGuard (boringtun)** — Primary. Implemented. Userspace WireGuard endpoint on the worker. Peer key maps to a namespace. Decapsulated IP packets are injected into the fabric. Worker protocol supports `AddWireGuardPeer`/`RemoveWireGuardPeer` commands. The CLI integrates via `dv connect` (embedded boringtun, ephemeral keypair per connection) and `dv disconnect`. Client protocol provides `ConnectNetwork`/`DisconnectNetwork` RPCs.
- **Reverse proxy** (future) — L7 adapter that terminates HTTP/TCP at the edge and proxies into the fabric as a network endpoint. Zero client-side setup — shareable URLs for non-technical stakeholders.
- **OS-level routing / NAT** (future) — Host routing table entries or iptables DNAT rules pointing namespace subnets to the fabric's TUN device. Most transparent for infrastructure integration, but requires host-level privileges.

---

## CLI

See [cli-design.md](cli-design.md) for full details.

The `dv` CLI has two layers:

- **Layer 1 — Task-oriented**: `dv up` (deploy from compose file), `dv down` (tear down namespace), `dv status` (smart overview that scales from namespace to workload detail), `dv logs` (stream workload output), `dv events` (activity stream showing activation cascades), `dv connect` / `dv disconnect` (WireGuard tunnel into namespace via embedded boringtun), `dv clone` (clone namespace with scale-to-zero), `dv deactivate` (hint to deactivate a workload), `dv splice` (take over workload identity for local dev).
- **Layer 2 — Uniform resource**: `dv get <type>`, `dv describe <type> <name>`, `dv create`, `dv delete`. Resource types: service, workload, worker, pod, adapter. All support `-o json`.
- **Auth commands**: `dv login` (save server + token), `dv context` (use/list/delete/show named contexts).

Addressing: `<namespace>`, `<namespace>/<workload>`, `<namespace>/<resource-type>/<name>`. Namespaces are always explicit. The CLI has platform-specific TUN/routing support for `dv connect` on both Linux and macOS (`distvirt-cli/src/platform/`).

Authentication: API key tokens as gRPC bearer tokens. Credentials in `~/.config/distvirt/credentials.toml` with named contexts (`dv login`, `dv context`). Resolution: CLI flags > env vars (`DV_SERVER`, `DV_TOKEN`) > active context.

---

## Master Guest Image

Built once via **Nix**, reused across all VMs. Contains a minimal Linux kernel and the guest agent (`guest-init`) as a static musl binary. Uses `devtmpfs` with `CONFIG_DEVTMPFS_MOUNT=y` for device nodes (avoids `mknod` in the Nix sandbox).

Kernel config at `guest-image/guest-kernel.config` — minimal virtio drivers (blk, net, vsock), ext4, serial console. Everything else disabled.

Output paths: `guest-image/result-kernel/bzImage` (kernel), `guest-image/result-rootfs` (root filesystem).

---

## Image Provider

Trait-based container image preparation. Two implementations:

- **ContainerdOverlayfsProvider** (primary) — Pulls OCI images via containerd gRPC API, mounts overlayfs snapshot, builds ext4 image from merged rootfs via `mkfs.ext4 -d`. Parses OCI config (Entrypoint, Cmd, env, working_dir, user).
- **RootfsDirProvider** (dev/test) — Builds ext4 from a host directory.

---

## VMM Abstraction

Two-trait design separating factory (`Vmm`) from instance (`VmInstance`), fully async. Vmm provides `launch()` and `restore()` (from snapshot). VmInstance provides `connect_vsock()`, `tap()`, `wait()`, `kill()`, `snapshot()`.

**Firecracker implementation**: Spawns `firecracker` process, configures via REST API over Unix socket (raw HTTP, no library). Sets up: boot source, rootfs drive (read-only), container drive (writable), virtio-net with vhost-net backend, vsock. Vsock connection via Firecracker's UDS-based proxy (`CONNECT <port>\n` handshake). Supports snapshot creation (pause vCPUs + snapshot state) and restore from `SnapshotArtifacts`.

---

## Boot Path

1. VMM startup (Firecracker: ~5-10ms)
2. Kernel boot (~50-125ms)
3. Guest init: mount essential filesystems
4. Guest init: vsock listen + yamux handshake
5. Host: vsock connect → `AddContainer` → `ConfigureNetwork` → `StartContainer`
6. Container process starts

---

## Testing

- **Unit tests** — In-crate `#[test]` modules across the workspace.
- **Integration tests** — `distvirt-tests/tests/integration.rs` exercises orchestrator integration logic.
- **Stateright model tests** — Model-checked state machine tests for the orchestrator.
- **Scenario tests** — `distvirt-tests/tests/scenarios/` contains 12 scenario modules (pod lifecycle, activation lifecycle, suspend/resume, drain, multi-worker, multi-service, fabric routing, spec reconciliation, retry/backoff, pressure, edge cases, known bugs) that run the full orchestrator with a test harness.
- **E2E tests** — `distvirt-worker/tests/e2e/` spins up real Firecracker VMs (requires root). Covers pod lifecycle, suspend/resume, cross-worker resume, artifact transfer, services, and WireGuard tunnels.
- **Simulation tests** — `distvirt-worker/tests/sim/` tests the worker with a simulated VMM backend (no real VMs). Covers pod lifecycle, suspend/resume, crash handling, and services.

---

## Resolved Decisions

- **PID 1**: Custom Rust PID 1, no systemd
- **Master image build**: Nix for reproducibility; devtmpfs eliminates mknod
- **Rootfs image format**: ext4 via `mkfs.ext4 -d` (no loopback mount)
- **VMM API**: Raw HTTP over Unix socket for Firecracker
- **Host↔guest wire format**: JSON (serde) over yamux/vsock
- **Host↔guest multiplexing**: yamux — separates control stream from output streams
- **Worker↔orchestrator wire format**: Cap'n Proto over yamux
- **Client↔orchestrator protocol**: gRPC via tonic
- **Networking**: Host TAP devices with userspace L3 IP fabric, smoltcp gateway, TUN for egress
- **OCI image handling**: containerd for pull/cache/snapshot
- **Orchestrator architecture**: Pure state machine + async shell; two-layer (outer + per-namespace sub-SMs)
- **CLI design**: Two-layer (`dv up`/`dv status` task layer + `dv get`/`dv describe` resource layer), workload-centric, explicit namespaces
- **CLI auth**: API key tokens as gRPC bearer tokens, named contexts in credentials file, env var overrides for CI
- **Ingress adapter architecture**: Worker-level resources with per-namespace virtual ports, config delivered by orchestrator during handshake

---

## Snapshots, Suspend/Resume & Live Migration

> **Status:** Suspend/resume is implemented end-to-end: VMM snapshot/restore, worker commands (`SuspendPod`, `ResumePod`, `DeleteSnapshot`), orchestrator workload states (`Suspending`, `Suspended`, `Resuming`), guest handshake (`PrepareSuspend`/`SuspendReady`), `ServiceActivation`-triggered demand-up, and `suspend_on_idle` policy. Live migration and namespace snapshots are not yet implemented. See [snapshots-migration.md](snapshots-migration.md) for full design.

Three capabilities built on Firecracker's native VM snapshot/restore:

- **Suspend/resume** — Scale-to-zero with fast restore (~5-10ms) instead of cold start (VM boot ~100ms+ plus application startup, which can take seconds to tens of seconds until ready). Orchestrator suspends idle workloads, stores snapshots to local storage, resumes from snapshot on traffic activation. Integrates with existing service entity buffering.
- **Live migration** (future) — Transparently move a running pod between workers (for draining, rebalancing). Suspend on source → transfer snapshot → resume on target. Fabric buffering makes migration invisible to the guest and its peers. Failure at any point safely falls back to source.
- **Namespace snapshots** (future) — Full namespace checkpoint to S3. All pods suspended at a consistent point, uploaded in parallel. Enables `dv clone` (restore under new namespace ID with same IP space) and disaster recovery.

Storage is modeled as **pools** (local, shared, remote) with capabilities (Boot, Snapshot, Transfer). Workers advertise pools at connect time; orchestrator manages all placement, eviction, and transfer decisions. V1: single local pool per worker + optional S3 remote pool.

---

## Open Questions / Future Work

- **libkrun backend** — Secondary VMM target for macOS support via Hypervisor.framework. Library-based, supports virtio-fs.
- **Multi-worker distribution** — TCP/TLS transport between orchestrator and remote workers, tunnel ports connecting fabric segments, cross-worker scheduling.
- **Multi-container pods** — Multiple virtio-blk devices per VM, hot-plugging, independent container lifecycle.
- **Config-from-file optimization** — Guest reads initial config from disk instead of waiting for vsock handshake, reducing boot latency.
- **Protocol extensions** — Exec support, capabilities (drop/add per OCI spec), read-only rootfs.
- **Incremental migration (pre-copy)** — Firecracker dirty page tracking for minimal pause windows during live migration. Protocol supports it; deferred past v1.
- **Snapshot pre-warming** — Speculatively distribute snapshots to workers where traffic is likely. Policy TBD.
