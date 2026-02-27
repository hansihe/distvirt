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

The VM boots from a **master guest image** that is built once and reused across all containers. It contains the kernel, the guest agent/init, and minimal supporting files. Container-specific rootfs images are mounted into the VM separately. A single VM can host **multiple containers** (a pod), each with its own rootfs and process tree.

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
        │                   │                          │   │       │              │
 ┌──────▼───────┐           │                          │   │       ▼              │
 │ rootfs image  │──────────┘                          │   │  mount /dev/vdb at   │
 │ builder       │  builds container                   │   │  /containers/ctr-1   │
 │               │  disk image(s) from                 │   │       │              │
 │               │  OCI rootfs/                        │   │       ▼              │
 └───────────────┘                                     │   │  fork+chroot per     │
                                                       │   │  container, exec     │
 ┌───────────────┐                                     │   │  entrypoints         │
 │ master image  │  built once (via Nix)               │   └──────────────────────┘
 │ builder       │  kernel + agent + base system       │
 └───────────────┘                                     │
```

**Components:**
1. **Master guest image** (built once via Nix) — kernel + guest agent + minimal rootfs. Boots as virtio-blk 0. Read-only, shared across all VMs. Uses `devtmpfs` for device nodes.
2. **Container rootfs image(s)** (built per container) — OCI rootfs packed into ext4 disk images, mounted as virtio-blk 1..N.
3. **Guest agent** — PID 1 in the master image. Mounts container disks, forks+execs workloads, reaps zombies.
4. **Host-side runtime** — builds container disk images, launches VMM, communicates with guest agent over vsock.

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

Working end-to-end minimal container runtime. A single container can be launched in a Firecracker VM, execute a process, and report its exit code back to the host. The CLI is a dev/debug tool, not the final interface.

### Crate structure

```
distvirt-guest-protocol/     — shared protocol types (serde only, musl-compatible)
guest-image/guest-init/      — guest agent (PID 1, static musl binary)
distvirt/                    — host-side library (VMM, image builder, orchestration)
distvirt-cli/                — dev/debug CLI
```

### Vsock protocol

Wire format: 4-byte LE length prefix + JSON body. Shared via `distvirt-guest-protocol` crate (depends only on `serde`, compatible with static musl guest builds).

```
Host → Guest (HostMessage):
  AddContainer { id, device }            — mount virtio-blk device as ext4
  StartContainer { id, entrypoint, args } — fork+chroot+exec
  Shutdown                               — clean power off

Guest → Host (GuestMessage):
  Ready                                  — agent booted, vsock connected
  ContainerAdded { id }                  — device mounted successfully
  ContainerStarted { id, pid }           — process running
  ContainerExited { id, code }           — process exited (or 128+signum if killed)
  Error { message }                      — something went wrong
```

### Guest agent

Static Rust binary, runs as PID 1 in the VM. Uses raw `libc` throughout — no external vsock crate, no nix crate.

**Boot sequence:**
1. Mount essential filesystems (proc, sysfs, tmpfs, devpts, /dev/shm)
2. Set up `signalfd` for SIGCHLD (block SIGCHLD, read from signalfd)
3. Bind vsock listener on port 1024, accept connection
4. Send `Ready` message

**Event loop:** `poll()` on two fds — vsock stream and signalfd. Handles buffered data correctly (checks `has_buffered_data()` before choosing poll timeout).

**Container management** (`container.rs`):
- `add()`: create `/containers/<id>`, mount device as ext4
- `start()`: `fork` → child does `setsid` + `chroot` + mount /proc,/sys,/dev,/tmp inside chroot + `execv`
- `reap_children()`: non-blocking `waitpid(-1, WNOHANG)` loop, matches PIDs to containers, reports exit codes

### VMM abstraction

Two-trait design (separates factory from instance):

```rust
trait Vmm {
    type Instance: VmInstance;
    fn launch(&self, config: &VmConfig) -> Result<Self::Instance>;
}

trait VmInstance {
    fn connect_vsock(&self, port: u32) -> Result<UnixStream>;
    fn wait(&mut self) -> Result<()>;
    fn kill(&mut self) -> Result<()>;
}
```

**Firecracker implementation:**
- Spawns `firecracker` process, communicates via REST API over Unix socket
- Raw HTTP/1.1 — no hyper/reqwest, just `PUT` with fresh `UnixStream` per request
- Configures: boot source, rootfs drive, container drive (`/dev/vdb`), vsock, machine config
- Vsock connection via Firecracker's UDS-based vsock proxy (connect + `CONNECT <port>\n` handshake, 30s retry loop)
- Rootfs image copied to tmpdir (Firecracker needs writable image). TempDir cleaned up on drop.

### Container rootfs image builder

Uses `mkfs.ext4 -d` to populate the filesystem directly — no loopback mount needed. Requires `e2fsprogs` on the host.

Steps: `du -sb` (size) → `truncate` (allocate, size × 1.2 + 10MB) → `mkfs.ext4 -d <rootfs>` (create + populate) → `resize2fs -M` (shrink to minimum).

### Orchestration

`run_container()` drives the full lifecycle: build ext4 image → launch VM → connect vsock → wait for Ready → AddContainer(/dev/vdb) → StartContainer → wait for ContainerExited → Shutdown → wait for VM exit → return exit code.

### Dev CLI

`distvirt` binary with clap derive (for development/debugging only):
- `build-image --rootfs <dir> --output <path>`
- `run --kernel <path> --rootfs-image <path> --container-rootfs <dir> --entrypoint <cmd> [--args ...] [--firecracker <path>]`

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

## Rootfs Transport Options

| Method | Firecracker | libkrun | Startup overhead | Image build cost | Complexity |
|--------|-------------|---------|-----------------|-----------------|------------|
| **virtio-blk + ext4** (current) | Yes | Yes | Medium (mount ext4) | Medium (`mkfs.ext4 -d`) | Low |
| **virtio-blk + squashfs + overlay** | Yes | Yes | Medium (mount sqsh + overlay setup) | Low (`mksquashfs`, fast + small) | Medium |
| **virtio-fs** | No | Yes | **Lowest** (no image build, no mount overhead beyond FUSE) | **None** (share directory directly) | Low |
| **devmapper block device** | Yes | Yes | **Lowest** (direct block device, no build) | **None** (CoW snapshot from containerd) | High (host setup) |

Current implementation uses virtio-blk + ext4 for broadest VMM compatibility. `mkfs.ext4 -d` avoids loopback mounts.

For the scale-to-zero use case, container images are pre-built ahead of time, so **image build cost is not on the critical path** — only startup overhead matters. The ext4 mount itself is fast. The bigger win is eliminating vsock handshake latency (see boot speed section below).

---

## Networking: Virtualized Network Fabric

This is the core differentiator. The goal is a **virtualized network layer** implemented in Rust that enables scale-to-zero distributed systems.

### Concept

A userspace network fabric that intercepts all traffic between VMs. When a packet arrives for a VM that isn't running, the fabric:
1. Buffers the packet
2. Spins up (or resumes from snapshot) the target VM
3. Delivers the packet once the VM is ready
4. The VM processes the request as if it had been running all along

This enables **transparent scale-to-zero staging environments**: deploy a full distributed system (databases, services, queues, etc.), let idle VMs shut down, and revive them on demand. Developers interact with the staging environment normally — the on-demand startup is invisible (modulo a brief cold-start delay).

### Implementation approach

Rather than modifying guest kernels or using complex host networking stacks, the plan is to keep the host network path minimal:

- **Option A: Patched VMM** — patch Firecracker/libkrun to accept TAP-over-Unix-socket, so the distvirt process owns the raw Ethernet frames directly. No host kernel networking involvement.
- **Option B: Host TAP passthrough** — use standard TAP devices but with no routing/bridging on the host. Just round-trip through the kernel's TAP interface. Simpler but adds kernel overhead.

In both cases, the distvirt process acts as a virtual switch/router in userspace, with full control over packet delivery, buffering, and VM lifecycle decisions.

### Why this matters for boot latency

The network fabric controls when packets reach VMs. This means:
- VMs can be started **before** the first packet is delivered (buffered during boot)
- VM resume from snapshot can bring a "cold" service online in single-digit milliseconds
- The fabric can maintain TCP connections on behalf of dormant VMs (SYN → hold → wake VM → deliver)

---

## Boot Speed

Boot latency is critical — it's the time between "packet arrives for dormant service" and "service processes the packet". Every component on this path matters.

### Current boot path latency breakdown

1. VMM startup (Firecracker: ~5-10ms)
2. Kernel boot (~50-125ms depending on config)
3. Guest init: mount filesystems (~1-2ms)
4. Guest init: vsock listen + wait for host connection (**variable, 10s of ms**)
5. Host: vsock connect + handshake (~few ms)
6. Host: send AddContainer + StartContainer (~few ms)
7. Container process starts

### Optimization: config-from-file

The vsock handshake (steps 4-6) adds unnecessary latency. The guest agent could instead:
- Read initial container config from a **file baked into the container rootfs** (or a dedicated small virtio-blk config device)
- Begin mounting + forking immediately on boot, in parallel with vsock setup
- Use vsock only for runtime control (signals, exec, status reporting)

This eliminates the synchronous round-trip and lets the container process start as soon as the kernel is up.

### Snapshot resume

Firecracker supports **VM snapshots** — save full VM state (memory + device state) and restore it later. This is the ultimate fast path:
- First boot: normal cold start, snapshot after container is running and idle
- Subsequent starts: restore from snapshot in ~5-10ms, VM is immediately ready
- Network fabric delivers buffered packets to the restored VM

---

## Toward a Full-Featured Runtime

The current implementation is a minimal proof-of-concept. Key areas to build out:

### Multi-container pods

Current: one container per VM. For pod support:
- **Multiple virtio-blk devices** per VM (one per container)
- **Hot-plugging** container disks after boot (init containers → app containers)
- **Independent lifecycle** per container (start/stop/remove individually)
- **Shared pod resources** — volumes mounted once in VM, bind-mounted into each chroot

VMs make pod semantics natural: all containers share the VM's network and IPC for free.

### Protocol extensions

The vsock protocol needs to grow:
- **Container config**: env vars, cwd, uid/gid, mounts, capabilities
- **Signal forwarding**: `SignalContainer { id, signal }`
- **Exec support**: `ExecInContainer { id, exec_id, cmd, args, env }`
- **I/O streaming**: per-container stdout/stderr forwarded to host

### libkrun backend

Primary secondary VMM target after Firecracker. Key reasons:
- **macOS support** via Hypervisor.framework — important for developer experience
- Library-based VMM (links into our process, no separate daemon)
- virtio-fs support (skip image build entirely)

### Resource limits

- **Host-side**: cgroups on the VMM process (memory, CPU)
- **VM-level**: Firecracker/libkrun enforce vCPU count and memory limits directly

### Containerd integration

Two paths depending on snapshotter:
- **overlayfs** (default) — pack merged rootfs directory into ext4 (current path)
- **devmapper** — pass block device snapshot directly as virtio-blk, zero-copy (what Kata does)

OCI runtime interface compliance is not a primary goal, but containerd integration may be useful for image management (pull/cache/GC).

---

## OCI Image Spec Compliance

We want to accept a standard container image and run it correctly — honoring the image's config (env, entrypoint, working directory, uid/gid, mounts, capabilities, etc.) even though we're not implementing the OCI runtime CLI interface. The goal is "give us an image, we run it right."

### What we need to handle (currently missing)

The current implementation only passes entrypoint + args. A spec-compliant container also needs:
- **Environment variables** — from image config + runtime overrides
- **Working directory** — `chdir` before exec
- **User/group** — `setuid`/`setgid` + supplementary groups
- **OCI spec mounts** — tmpfs, bind mounts, procfs options per spec
- **Capabilities** — drop/add per spec (currently runs as full root)
- **Hostname** — `sethostname` in the guest
- **Read-only rootfs** — mount ext4 as read-only when spec says so

### Youki crates useful for this

- `oci-spec` — `Spec`, `Process`, `Mount`, `Linux` structs for parsing config.json / image config
- `libcontainer::capabilities` — OCI capability names → Linux constants
- `libcontainer::signal` — signal name/number mapping for forwarding
- `libcontainer::rootfs::utils::parse_mount()` — parse OCI mount specs into flags + data
- `libcgroups` — host-side VM resource limits (v1/v2/systemd cgroup managers)

### Not applicable (VM replaces these)

- `process/`, `namespaces/`, `syscall/` — namespace/fork dance replaced by VM boot
- `rootfs::mount`, `rootfs::device` — mount namespace / pivot_root specific
- `seccomp`, `apparmor`, `user_ns`, `network/` — VM provides isolation instead
- Device creation (`mknod`) — `devtmpfs` handles this

---

## Resolved Decisions

- **PID 1**: Custom Rust PID 1, no systemd. Boot time (<1ms vs 200-500ms), image size (~1MB vs ~50MB+), semantic fit all favor custom.
- **Master image build**: Nix for reproducibility. devtmpfs eliminates mknod. Kernel config tracked in repo.
- **Rootfs image format**: ext4 via `mkfs.ext4 -d` (no loopback mount). Works, simple, good enough.
- **VMM API**: Raw HTTP over Unix socket for Firecracker. No HTTP library needed.
- **Vsock implementation**: Raw `libc` AF_VSOCK. No external crate needed for static musl builds.
- **Guest agent deps**: Only libc + serde. Fully self-contained, minimal binary.

## Open Questions

1. **Virtualized networking implementation**: Patched VMM (TAP-over-Unix-socket) vs host TAP passthrough? Need to evaluate latency and complexity tradeoffs.
2. **Snapshot strategy**: Per-container snapshots? Per-pod? How to handle snapshot invalidation when container image changes?
3. **Hot-plug vs pre-attach**: For multi-container pods, attach all disks at boot vs hot-plug on demand?
4. **Config-from-file format**: What goes in the baked config vs what comes over vsock at runtime?
