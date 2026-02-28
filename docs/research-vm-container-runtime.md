# Research: VM-Based Container Runtime

## Goal

A VM-based container runtime in Rust optimized for **ultra-fast cold start**. Containers run inside microVMs (Firecracker, libkrun, etc.) with the container process as the only significant guest process — no nested container runtime inside the VM.

The driving use case is **scale-to-zero staging environments**: a virtualized network fabric where VMs are spun up (or resumed) on demand when a packet arrives for a dormant service. Every millisecond of boot latency is user-facing latency, so startup speed is a first-class concern throughout the design.

---

## Prior Art

### Kata Containers (heavy approach)
- Boots a VM with a minimal guest OS + **kata-agent** (Rust, ~15MB)
- Agent listens on virtio-vsock, receives ttRPC commands from host
- Uses libcontainer internally to create **full namespaces and cgroups inside the VM**
- Container rootfs enters the VM via **virtio-fs** or **virtio-blk** (devmapper block device hot-plugged)
- Heavyweight: agent + nested namespaces + cgroups inside VM

### firecracker-containerd (Amazon)
- Firecracker-specific, block-device only (no virtio-fs support)
- Runs a full **containerd agent + runc** inside the VM
- Container image layers hot-plugged as virtio-blk devices

### libkrun / krun (Red Hat) — closest to our goal
- A dynamic library embedding a VMM (rust-vmm based)
- **Minimal init** (~200 lines of C) that mounts virtiofs rootfs, sets env, directly `exec()`s entrypoint
- No agent, no runc, no nested namespaces — the VM is the isolation boundary
- Integrates with crun (container runtime) — invoked as `krun` symlink
- Networking via TSI (transparent socket impersonation over vsock) or virtio-net
- **Supports macOS** via Hypervisor.framework — important for developer experience

---

## Architecture Overview

The VM boots from a **master guest image** that is built once and reused across all containers. It contains the kernel, the guest agent/init, and minimal supporting files. Container-specific rootfs images are mounted into the VM separately. A single VM hosts one pod (one or more containers sharing the VM's network).

```
                    HOST                              │       GUEST (microVM)
                                                      │
                                                      │
 ┌──────────────┐     ┌────────────┐                  │   ┌──────────────────────┐
 │  distvirt     │────▶│  VMM       │                  │   │  master image        │
 │  (host-side)  │     │ (firecracker│                 │   │  (boots from         │
 │               │     │  / libkrun)│                  │   │   virtio-blk 0)      │
 └──────┬───────┘     └─────┬──────┘                  │   │  ┌────────────────┐  │
        │                   │                          │   │  │ kernel         │  │
        │                   │  virtio-blk 0: master    │   │  │ guest-agent    │  │
        │                   │  virtio-blk 1..N:        │   │  │ (PID 1)        │  │
        │                   │    container rootfs(es)  │   │  │ devtmpfs auto  │  │
        │                   │  virtio-vsock: control   │   │  └────────────────┘  │
        │                   │  virtio-net: TAP on      │   │       │              │
        │                   │    host fabric           │   │       ▼              │
 ┌──────▼───────┐           │                          │   │  mount /dev/vdb at   │
 │ image        │──────────┘                          │   │  /containers/ctr-1   │
 │ provider     │  prepares container                  │   │       │              │
 │ (containerd) │  ext4 disk from OCI image            │   │       ▼              │
 └───────────────┘                                     │   │  fork+chroot per     │
                                                       │   │  container, exec     │
 ┌───────────────┐                                     │   │  entrypoints         │
 │ master image  │  built once (via Nix)               │   └──────────────────────┘
 │ builder       │  kernel + agent + base system       │
 └───────────────┘                                     │
```

**Components:**
1. **Master guest image** (built once via Nix) — kernel + guest agent + minimal rootfs. Boots as virtio-blk 0. Read-only, shared across all VMs. Uses `devtmpfs` for device nodes.
2. **Container rootfs image(s)** (built per container) — OCI image rootfs packed into ext4 disk images via containerd overlayfs snapshotter, mounted as virtio-blk 1..N.
3. **Guest agent** — PID 1 in the master image. Mounts container disks, configures networking, forks+execs workloads, streams output, reaps zombies.
4. **Host-side worker** — prepares container disk images, launches VMM, communicates with guest agent over yamux-multiplexed vsock, manages network fabric.

### Why two separate images?

| Concern | Single combined image | Master + container image (chosen) |
|---------|----------------------|-----------------------------------|
| **Build speed** | Must rebuild full image per container | Master is cached; only pack container rootfs |
| **Deduplication** | Kernel/agent duplicated per container | Kernel/agent shared across all VMs |
| **Mutability** | Container writes go to same image | Master is read-only; container image is the write layer |
| **Image size** | Larger per-container | Container image is just the rootfs |
| **Complexity** | Simpler (one disk) | Slightly more (two virtio-blk devices) |

---

## Current Implementation Status

Working end-to-end container runtime. Pods (single-container for now) launch in Firecracker VMs with full networking (inter-pod L2 fabric, DNS service discovery, internet egress via NAT). Docker Compose files can bring up multi-service environments. Container output is streamed to the host.

### Crate structure

```
distvirt-guest-protocol/     — shared host↔guest message types (serde, musl-compatible)
guest-image/guest-init/      — guest agent (PID 1, static musl binary)
distvirt-worker/             — worker process (VMM, fabric, image provider, pod lifecycle)
distvirt-worker-protocol/    — orchestrator↔worker protocol types + yamux transport
distvirt-compose/            — docker-compose parsing, deployment planning, orchestration
distvirt-cli/                — CLI binary (compose-up, run-image)
```

### Host↔guest protocol (vsock + yamux)

The host connects to the guest agent over virtio-vsock (port 1024). The connection is multiplexed via **yamux** — one control stream for commands/events, additional streams for container output.

Wire format on each yamux stream: 4-byte LE length prefix + JSON body. Shared via `distvirt-guest-protocol` crate.

```
Host → Guest (HostMessage):
  AddContainer { id, device, dns_servers }          — mount virtio-blk device as ext4
  StartContainer { id, entrypoint, args, env,       — fork+chroot+exec with full config
                   working_dir, uid, gid, hostname,
                   capture_output }
  ConfigureNetwork { interface, ip, netmask, gateway } — configure guest network interface
  Shutdown                                           — clean power off

Guest → Host (GuestMessage):
  Ready                                  — agent booted, yamux session established
  ContainerAdded { id }                  — device mounted, resolv.conf written
  ContainerStarted { id, pid }           — process running
  ContainerExited { id, code }           — process exited (128+signum if signaled)
  NetworkConfigured                      — interface up with IP and default route
  Error { message }                      — something went wrong
```

Container output streams use a separate yamux stream per container, with a `StreamHeader::ContainerOutput { container_id }` followed by framed output chunks: `[stream_id: u8][length: u32 LE][payload]` (stream_id 1=stdout, 2=stderr).

### Guest agent

Static Rust binary (~1300 lines), runs as PID 1 in the VM. Uses raw `libc` for syscalls, `async-io` + `futures-lite` + `async-executor` for async I/O, yamux for stream multiplexing.

**Boot sequence:**
1. Mount essential filesystems (proc, sysfs, tmpfs, devpts, /dev/shm)
2. Set up `signalfd` for SIGCHLD
3. Bind vsock listener on port 1024, accept connection
4. Establish yamux session (guest = server), accept control stream
5. Send `Ready` message

**Event loop:** Async multiplexing of four sources — signalfd (child reaping), control stream (host commands), container output pipes (stdout/stderr draining), and yamux lifecycle.

**Container management** (`container.rs`):
- `add()`: mount device as ext4 at `/containers/<id>`, write `/etc/resolv.conf`, `/etc/hostname`, `/etc/hosts`
- `start()`: `fork` → child does `setsid` + `sethostname` + `chroot` + `chdir(working_dir)` + mount /proc,/sys,/dev,/tmp + redirect stdout/stderr to pipes + `setgid`/`setuid` + `execv`
- Output capture: parent reads from pipe fds, encodes as output chunks, streams over dedicated yamux stream
- `reap_children()`: non-blocking `waitpid(-1, WNOHANG)` loop, matches PIDs to containers

**Networking** (`net.rs`): Configures guest interface via raw ioctls — SIOCSIFADDR, SIOCSIFNETMASK, SIOCSIFFLAGS (UP), SIOCADDRT (default route via gateway).

### VMM abstraction

Two-trait design (separates factory from instance), fully async:

```rust
trait Vmm: Send + Sync {
    type Instance: VmInstance;
    async fn launch(&self, config: &VmConfig) -> Result<Self::Instance>;
}

trait VmInstance {
    async fn connect_vsock(&self, port: u32) -> Result<UnixStream>;
    fn tap(&self) -> Option<&TapDevice>;
    fn take_tap(&mut self) -> Option<TapDevice>;
    async fn wait(&mut self) -> Result<()>;
    async fn kill(&mut self) -> Result<()>;
}
```

**VmConfig** includes: kernel path, rootfs image, container image, vCPU count, memory size, optional network config (TAP device), serial console toggle.

**Firecracker implementation:**
- Spawns `firecracker` process, communicates via REST API over Unix socket
- Raw HTTP/1.1 — no hyper/reqwest, just `PUT` with fresh `UnixStream` per request
- Configures: boot source, rootfs drive (read-only), container drive (writable), virtio-net with vhost-net backend, vsock, machine config
- Vsock connection via Firecracker's UDS-based vsock proxy (connect + `CONNECT <port>\n` handshake)
- Optional serial console output forwarded to host logs

### Image provider

Trait-based image preparation:

```rust
trait ImageProvider: Send + Sync {
    async fn prepare(&self, image_ref: &str) -> Result<PreparedArtifact>;
}

struct PreparedArtifact {
    pub image_path: PathBuf,           // ext4 image for Firecracker
    pub oci_config: Option<ImageConfig>, // parsed OCI image config
    _cleanup: Box<dyn Any + Send>,     // RAII cleanup (unmount overlay, etc.)
}
```

**ContainerdOverlayfsProvider** (primary): Pulls OCI images via containerd gRPC API, mounts overlayfs snapshot, builds ext4 image from merged rootfs via `mkfs.ext4 -d`, returns image path + parsed OCI config (entrypoint, env, working_dir, user). Overlay mount kept alive by RAII cleanup handle.

**RootfsDirProvider** (dev/test): Builds ext4 from a host directory.

### Worker↔orchestrator protocol

See `docs/worker-protocol.md` for full details. The worker is a dumb executor; all planning lives in the orchestrator.

Transport: yamux over any async byte stream (in-process `tokio::io::duplex` for local mode, TCP/TLS for future distributed mode). Control stream carries length-prefixed JSON commands/events. Log streams carry raw container output.

Key commands: `CreateNamespace`, `LaunchPod`, `StopPod`, `RegistrySync`, `Shutdown`.
Key events: `PodRunning`, `PodExited`, `PodFailed`, `FabricRouteMiss`.

### Networking fabric

See `docs/networking-fabric.md` for full details. Per-namespace userspace L2 switch with smoltcp-based gateway providing ARP, DNS service discovery, and internet egress via TUN+NAT.

### Compose orchestration

Docker Compose file parser + execution planner + orchestrator. Handles dependency ordering (topological sort), IP/MAC assignment, DNS registry sync, pod launch sequencing, and output streaming to terminal.

CLI: `distvirt compose-up -f docker-compose.yml` — spins up an in-process worker, creates namespace, launches pods, streams output.

---

## Master Guest Image

Built once via Nix, reused across all VMs.

### Contents

```
/
├── kernel          (vmlinux, separate file for Firecracker)
├── sbin/
│   └── init        (guest-agent, static musl binary)
├── dev/            (empty — populated at boot by devtmpfs)
├── proc/           (mountpoint)
├── sys/            (mountpoint)
├── tmp/            (mountpoint)
└── containers/     (mountpoint directory for container rootfs mounts)
```

`devtmpfs` with `CONFIG_DEVTMPFS_MOUNT=y` auto-populates `/dev` at boot. No `mknod` at build time — critical for Nix sandbox builds which lack `CAP_MKNOD`.

### Kernel config (minimal)

Key options:
- `CONFIG_VIRTIO`, `CONFIG_VIRTIO_PCI`, `CONFIG_VIRTIO_MMIO`
- `CONFIG_VIRTIO_BLK` (disk), `CONFIG_VIRTIO_NET` (networking), `CONFIG_VSOCKETS` + `CONFIG_VIRTIO_VSOCKETS` (control channel)
- `CONFIG_EXT4_FS`
- `CONFIG_DEVTMPFS`, `CONFIG_DEVTMPFS_MOUNT`
- `CONFIG_SERIAL_8250` (console)
- Disable everything else (no USB, no sound, no GPU, no legacy buses, etc.)

Config file tracked in repo at `guest-image/guest-kernel.config`.

---

## Networking: Virtualized Network Fabric

This is the core differentiator. The goal is a **virtualized network layer** that enables scale-to-zero distributed systems.

### Concept

A userspace network fabric that intercepts all traffic between VMs. When a packet arrives for a VM that isn't running, the fabric:
1. Buffers the packet (configurable policy: hold TCP SYN, buffer N frames, or drop)
2. Reports a route miss to the orchestrator
3. Orchestrator spins up (or resumes) the target VM
4. Delivers buffered packets once the VM is on the fabric

This enables **transparent scale-to-zero staging environments**: deploy a full distributed system, let idle VMs shut down, and revive them on demand.

### Current implementation

The fabric is implemented as a per-namespace userspace L2 Ethernet switch with a smoltcp-based IP gateway. Each pod's TAP device is a port on the switch. The fabric runs on the worker's tokio runtime.

**L2 switch** (`fabric/mod.rs`, `fabric/switch.rs`): MAC learning table, standard switch forwarding (known unicast → direct, unknown → flood, broadcast/multicast → flood + gateway). Per-port tokio tasks for concurrent frame processing. Frames include 10-byte vhost VNET header.

**Gateway** (`fabric/gateway.rs`): smoltcp IP stack at 172.16.0.1 providing:
- ARP responses (synthetic MAC `02:00:00:00:00:01`)
- DNS server (port 53) — resolves service names from local registry, forwards unknown queries upstream
- Internet egress via TUN device — strips Ethernet, writes IP to TUN, rebuilds Ethernet on return
- Virtio-net checksum offload handling

**DNS registry** (`fabric/dns.rs`): Name→IP mappings synced from orchestrator. Gateway answers A-record queries against this registry.

### Future: activation and scale-to-zero

The worker protocol already defines the primitives for activation:

- **Fabric routing table**: orchestrator pushes route entries per namespace. Entries are either `RemoteWorker` (forward through tunnel) or `Placeholder` (pod is dormant, apply buffer policy).
- **Buffer policies**: `hold_tcp_syn` (smoltcp gateway holds TCP connection during boot), `buffer_frames` (queue N frames for M ms), or drop.
- **Route miss events**: fabric reports `FabricRouteMiss` to orchestrator when a frame hits a placeholder or unknown destination.

The activation flow: frame arrives → placeholder route → buffer per policy → report miss → orchestrator launches pod → pod boots, TAP added to fabric → buffered frames delivered. From the sender's perspective, it's just a slow connection.

Protocol-aware activation (TCP SYN detection, HTTP/2 per-stream activation) is a future layer on top of this.

---

## Boot Speed

Boot latency is critical — it's the time between "packet arrives for dormant service" and "service processes the packet".

### Current boot path

1. VMM startup (Firecracker: ~5-10ms)
2. Kernel boot (~50-125ms depending on config)
3. Guest init: mount filesystems (~1-2ms)
4. Guest init: vsock listen + yamux handshake
5. Host: vsock connect + send AddContainer + ConfigureNetwork + StartContainer
6. Container process starts

### Optimization: config-from-file

The vsock handshake adds latency. The guest agent could instead:
- Read initial container config from a **file baked into the container rootfs** (or a dedicated small virtio-blk config device)
- Begin mounting + forking immediately on boot, in parallel with vsock setup
- Use vsock only for runtime control (signals, exec, status reporting)

### Snapshot resume

Firecracker supports **VM snapshots** — save full VM state (memory + device state) and restore it later:
- First boot: normal cold start, snapshot after container is running and idle
- Subsequent starts: restore from snapshot in ~5-10ms, VM is immediately ready
- Network fabric delivers buffered packets to the restored VM

---

## Toward a Full-Featured Runtime

### Multi-container pods

Current: one container per VM. For pod support:
- **Multiple virtio-blk devices** per VM (one per container)
- **Hot-plugging** container disks after boot (init containers → app containers)
- **Independent lifecycle** per container (start/stop/remove individually)
- **Shared pod resources** — volumes mounted once in VM, bind-mounted into each chroot

VMs make pod semantics natural: all containers share the VM's network and IPC for free. The protocol already supports multiple containers per pod.

### libkrun backend

Secondary VMM target after Firecracker:
- **macOS support** via Hypervisor.framework — important for developer experience
- Library-based VMM (links into our process, no separate daemon)
- virtio-fs support (skip image build entirely)

### Protocol extensions still needed

- **Signal forwarding**: `SignalContainer { id, signal }`
- **Exec support**: `ExecInContainer { id, exec_id, cmd, args, env }`
- **Capabilities**: drop/add per OCI spec (currently runs as full root)
- **Read-only rootfs**: mount ext4 as read-only when spec says so

### Multi-worker distribution

The worker protocol is designed for distributed mode from day one. Future work:
- TCP/TLS transport between orchestrator and remote workers
- Tunnel ports connecting fabric segments across workers
- Orchestrator-side scheduling across workers based on resource availability
- Worker failure detection and pod rescheduling

---

## Resolved Decisions

- **PID 1**: Custom Rust PID 1, no systemd.
- **Master image build**: Nix for reproducibility. devtmpfs eliminates mknod. Kernel config tracked in repo.
- **Rootfs image format**: ext4 via `mkfs.ext4 -d` (no loopback mount).
- **VMM API**: Raw HTTP over Unix socket for Firecracker. No HTTP library needed.
- **Vsock implementation**: Raw `libc` AF_VSOCK in guest. No external crate needed for static musl builds.
- **Guest agent deps**: libc + serde + async-io + futures-lite + yamux. Minimal, fully self-contained.
- **Host↔guest multiplexing**: yamux over vsock. Separates control stream from output streams.
- **Networking**: Host TAP devices with userspace L2 switch. No patched VMM needed.
- **Gateway**: smoltcp for ARP/DNS/NAT. TUN device for internet egress.
- **OCI image handling**: containerd for pull/cache/snapshot, parse OCI config for entrypoint/env/user/workdir.
- **Worker↔orchestrator protocol**: yamux over duplex channel (local) or TCP (distributed). Length-prefixed JSON.
- **Container output**: Streamed over dedicated yamux streams (guest→host→orchestrator).

## Open Questions

1. **Snapshot strategy**: Per-container snapshots? Per-pod? How to handle snapshot invalidation when container image changes?
2. **Hot-plug vs pre-attach**: For multi-container pods, attach all disks at boot vs hot-plug on demand?
3. **Config-from-file format**: What goes in the baked config vs what comes over vsock at runtime?
4. **Activation granularity**: When to implement TCP SYN hold vs simple frame buffering? HTTP/2 per-stream activation worth the complexity?
