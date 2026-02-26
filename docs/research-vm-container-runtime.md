# Research: VM-Based Container Runtime Using Youki Components

## Goal

Design a minimal VM-based container runtime in Rust that reuses youki components where possible. Containers run inside microVMs (Firecracker, Cloud Hypervisor, etc.) with the container process as the only significant guest process — no nested container runtime inside the VM.

---

## Prior Art

### Kata Containers (heavy approach)
- Boots a VM with a minimal guest OS + **kata-agent** (Rust, ~15MB)
- Agent listens on virtio-vsock, receives ttRPC commands from host
- Uses libcontainer internally to create **full namespaces and cgroups inside the VM**
- Container rootfs enters the VM via **virtio-fs** (host directory share) or **virtio-blk** (devmapper block device hot-plugged)
- Heavyweight: agent + nested namespaces + cgroups inside VM

### firecracker-containerd (Amazon)
- Firecracker-specific, block-device only (no virtio-fs support)
- VM rootfs is a squashfs + overlayfs assembled by a custom `overlay-init`
- Runs a full **containerd agent + runc** inside the VM
- Container image layers hot-plugged as virtio-blk devices

### libkrun / krun (Red Hat) — closest to our goal
- A dynamic library embedding a VMM (rust-vmm based)
- **Minimal init** (~200 lines of C) inside the VM that:
  - Mounts virtiofs-shared rootfs
  - Sets environment variables
  - Directly `exec()`s the container entrypoint
- No agent, no runc, no nested namespaces
- The VM itself is the isolation boundary
- Integrates with crun (container runtime) — invoked as `krun` symlink
- Networking via TSI (transparent socket impersonation over vsock) or virtio-net

### William Durand's proof-of-concept
- QEMU microvm + custom tiny kernel + custom init binary
- Entrypoint passed via kernel command-line parameters
- Rootfs shared via virtiofsd
- Demonstrates the absolute minimum needed

---

## Image Layers and Responsibility Boundary

**Youki (and OCI runtimes in general) do NOT handle image pull/cache/GC.** That is the job of the higher-level runtime:
- **containerd** — snapshotter plugins (overlayfs, devmapper, etc.), image pull, layer dedup, GC
- **podman/buildah** — image management via `containers/image` and `containers/storage`
- **CRI-O** — similar to containerd

Our runtime receives a **pre-unpacked OCI bundle** (rootfs directory + config.json) from the higher layer, same as youki does today. We do not need to implement image management.

---

## Architecture Overview

The VM boots from a **master guest image** that is built once and reused across all containers. It contains the kernel, the guest agent/init, and minimal supporting files. Container-specific rootfs images are mounted into the VM separately. A single VM can host **multiple containers** (a pod), each with its own rootfs and process tree.

```
                    HOST                              │       GUEST (microVM)
                                                      │
 ┌──────────────┐                                     │
 │  containerd   │  (pulls images, unpacks layers,    │
 │  / podman     │   provides OCI bundle)             │
 └──────┬───────┘                                     │
        │ OCI runtime interface                       │
        │ (bundle dir with rootfs/ + config.json)     │
 ┌──────▼───────┐     ┌────────────┐                  │   ┌──────────────────────┐
 │  our runtime  │────▶│  VMM       │                  │   │  master image        │
 │  (host-side)  │     │ (firecracker│                 │   │  (boots from         │
 │               │     │  / CH)     │                  │   │   virtio-blk 0)      │
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
 │               │  disk image(s) from                 │   │  mount /dev/vdc at   │
 │               │  OCI rootfs/                        │   │  /containers/ctr-2   │
 └───────────────┘                                     │   │       │              │
                                                       │   │       ▼              │
 ┌───────────────┐                                     │   │  fork+chroot per     │
 │ master image  │  built once (via Nix),              │   │  container, exec     │
 │ builder       │  kernel + agent + base system       │   │  entrypoints         │
 │               │                                     │   └──────────────────────┘
 └───────────────┘
```

**Components:**
1. **Master guest image** (built once via Nix) — kernel + guest agent + minimal rootfs. Boots as virtio-blk 0 (or virtio-pmem for faster boot). Read-only, shared across all VMs. Uses `devtmpfs` for device nodes (no baked-in `/dev` entries needed).
2. **Container rootfs image(s)** (built per container) — OCI rootfs packed into disk images, mounted as virtio-blk 1..N in the VM.
3. **Guest agent** — lives in the master image, runs as PID 1. Manages multiple containers per VM (pod support). Mounts container disks, receives config over vsock, forks+execs workloads, reaps zombies, forwards signals.
4. **Host-side runtime** — OCI-compatible shim. Builds container disk images, launches VMM, communicates with guest agent over vsock.

### Why two separate images?

| Concern | Single combined image | Master + container image (chosen) |
|---------|----------------------|-----------------------------------|
| **Build speed** | Must rebuild full image per container | Master is cached; only pack container rootfs |
| **Deduplication** | Kernel/agent duplicated per container | Kernel/agent shared across all VMs |
| **Mutability** | Container writes go to same image | Master is read-only; container image is the write layer |
| **Image size** | Larger per-container | Container image is just the rootfs |
| **Complexity** | Simpler (one disk) | Slightly more (two virtio-blk devices) |

---

## Youki Components: Reuse Assessment

### Directly Reusable

| Component | Crate | What it provides | How we use it |
|-----------|-------|-----------------|---------------|
| **OCI spec types** | `oci-spec` (dep) | `Spec`, `Process`, `Mount`, `Linux` structs | Parse container config.json, extract process args/env/cwd |
| **OCI CLI parsing** | `liboci-cli` | Standard + common OCI command parsing | Host-side runtime CLI (create/start/delete/state/kill) |
| ~~**Default device list**~~ | ~~`libcontainer::rootfs::utils::default_devices()`~~ | ~~List of `/dev/null`, etc.~~ | Not needed — using `devtmpfs` (kernel auto-populates `/dev`) |
| ~~**Device creation (mknod)**~~ | ~~`libcontainer::rootfs::device`~~ | ~~mknod() syscall~~ | Not needed — `devtmpfs` handles this |
| ~~**Symlink setup**~~ | ~~`libcontainer::rootfs::symlink`~~ | ~~`/dev/fd` → `/proc/self/fd`, etc.~~ | Not needed — `devtmpfs` creates standard symlinks |
| **Mount option parsing** | `libcontainer::rootfs::utils::parse_mount()` | Parses OCI mount spec into flags + data | Understand what mounts the container spec expects |
| **Container state** | `libcontainer::container::state` | Container state machine (Creating→Created→Running→Stopped) | Track container lifecycle on host side |
| **Cgroup management** | `libcgroups` | v1/v2/systemd cgroup managers | Apply resource limits on host side (limit VM process) |
| **Signal handling** | `libcontainer::signal` | Signal name/number mapping | Forward signals to VM |
| **Capabilities mapping** | `libcontainer::capabilities` | OCI capability names → Linux capability constants | Inform guest init which caps to set |

### Partially Reusable (need adaptation)

| Component | Why partial | What to extract |
|-----------|-------------|-----------------|
| **Container builder pattern** | Tightly coupled to namespace/fork flow | Reuse the builder API shape but replace internals with VMM launch |
| **Config persistence** | `YoukiConfig` is minimal (hooks + cgroup_path) | Extend for VM-specific config (kernel path, VMM type, memory/CPU) |
| **Hooks execution** | OCI lifecycle hooks (prestart, poststart, poststop) | Reuse hook runner, but trigger points differ (VM boot vs namespace creation) |
| **Process args/env setup** | Tightly coupled to exec() in container | Extract the spec→process-config logic, pass to guest init via different channel |

### Not Reusable (namespace/process-specific)

| Component | Why not applicable |
|-----------|-------------------|
| `rootfs::mount` — mount_into_container, mount_to_rootfs | Uses mount namespace, fd-based mount API, pivot_root — all runtime-only |
| `rootfs::device` — bind_dev() path | Bind-mounts host devices, requires mount namespace |
| `process/` — fork, intermediate process, init process | Entire multi-fork dance for namespace entry — replaced by VM boot |
| `namespaces/` | VM provides isolation instead |
| `syscall/` — pivot_rootfs, chroot, set_ns, unshare | All namespace/container-specific |
| `seccomp` | Syscall filtering inside container process — may use at VM level instead |
| `apparmor` | MAC for container process |
| `tty` | Console/PTY setup for container — guest init handles this differently |
| `user_ns` | UID/GID mapping in user namespace |
| `network/` — netlink setup | Container network namespace setup — VM uses virtio-net instead |

---

## Master Guest Image (built once via Nix)

The master image is a minimal bootable Linux system containing only what's needed to host container processes. Built reproducibly with Nix.

### Contents

```
/
├── kernel          (vmlinux or bzImage, ~5MB compressed — separate file for Firecracker)
├── sbin/
│   └── init        (our guest-agent, static Rust binary, ~1-2MB)
├── dev/            (empty — populated at boot by devtmpfs)
├── proc/           (mountpoint)
├── sys/            (mountpoint)
├── tmp/            (mountpoint)
└── containers/     (mountpoint directory for container rootfs mounts)
```

Device nodes (`/dev/null`, `/dev/zero`, `/dev/random`, etc.) and standard symlinks (`/dev/fd → /proc/self/fd`, etc.) are **not baked into the image**. The kernel's `devtmpfs` auto-populates `/dev` at boot when `CONFIG_DEVTMPFS_MOUNT=y` is set. This eliminates the need for `mknod` at build time (which would require root/`CAP_MKNOD`, problematic in Nix sandbox builds).

### Building with Nix

See the dedicated [Building the Guest Image with Nix](#building-the-guest-image-with-nix) section below.

### Kernel config (minimal)

Key options to enable:
- `CONFIG_VIRTIO`, `CONFIG_VIRTIO_PCI`, `CONFIG_VIRTIO_MMIO`
- `CONFIG_VIRTIO_BLK` (disk), `CONFIG_VIRTIO_NET` (networking), `CONFIG_VSOCKETS` + `CONFIG_VIRTIO_VSOCKETS` (control channel)
- `CONFIG_EXT4_FS` (or `CONFIG_SQUASHFS`), `CONFIG_OVERLAY_FS` (for squashfs + tmpfs overlay)
- `CONFIG_DEVTMPFS`, `CONFIG_DEVTMPFS_MOUNT` (auto-populate /dev — critical for Nix-built images)
- `CONFIG_SERIAL_8250` (console)
- Disable everything else (no USB, no sound, no GPU, no legacy buses, etc.)

---

## Container Rootfs Image (built per container create)

This is the fast path — takes an already-unpacked OCI rootfs and packs it into a mountable disk image.

### Input
- OCI bundle directory containing:
  - `rootfs/` — unpacked container filesystem (provided by containerd snapshotter)
  - `config.json` — OCI runtime spec

### Output
- `container-rootfs.ext4` — disk image attached as virtio-blk 1 in the VM

### Build steps

```
1. Parse config.json (reuse: oci-spec)
2. Calculate rootfs size (du -s rootfs/ + margin)
3. Create ext4 image: truncate + mkfs.ext4
4. Mount via loopback
5. Copy OCI rootfs/ contents into mounted image (cp -a or tar)
6. Create any additional mountpoint directories from spec mounts
7. Unmount and finalize
```

This is intentionally simple — device nodes, symlinks, and system mounts live in the master image, not here. The container image is purely the application filesystem.

### Performance considerations

- **Loopback mount + copy is slow for large images.** Mitigation options:
  - Use `mkfs.ext4 -d rootfs/` to populate directly without mounting (ext4 only, needs e2fsprogs)
  - Use squashfs (`mksquashfs rootfs/ container.sqsh`) — very fast, read-only, good compression
  - Skip image build entirely if using virtio-fs (Cloud Hypervisor/crosvm only)

### Alternative: virtio-fs (no disk image)

If targeting Cloud Hypervisor or crosvm (not Firecracker), skip image building entirely:
- Share the OCI rootfs directory directly via virtiofsd
- Guest agent mounts it as virtio-fs at /container
- Fastest "create" path — no copy, no image build
- Not available on Firecracker

### Alternative: squashfs (read-only) + tmpfs overlay

For most workloads (containers are typically ephemeral):
- `mksquashfs rootfs/ container.sqsh` — fast, compressed, read-only
- Guest agent mounts squashfs + overlayfs with tmpfs upper for writes
- Smaller images, faster to build than ext4
- Writes are ephemeral (lost on VM stop) — usually fine for containers

---

## Guest Agent Design

A minimal static Rust binary (~1-2MB stripped) that lives in the master image as `/sbin/init` (PID 1). Designed from the start for **multi-container pod support** — manages multiple independent container processes within a single VM.

### Why not systemd?

Systemd is a poor fit for this use case:

| Concern | Custom Rust PID 1 | systemd |
|---------|-------------------|---------|
| **Boot time** | Negligible (<1ms) | 200-500ms+ (doubles total boot time when Firecracker boots in ~125ms) |
| **Image size** | ~1-2MB static binary | ~50-100MB+ (systemd + dbus + util-linux + deps) |
| **Semantic fit** | Purpose-built for OCI container lifecycle | Unit/service model doesn't map to OCI semantics (lifecycle hooks, exec, exit code propagation) |
| **Cgroups** | No cgroups needed in guest (host manages VM limits) | systemd wants to own the cgroup hierarchy — imposes unwanted structure |
| **Complexity** | ~1000-1500 lines, fully understood | Massive surface area, configuration complexity, failure modes |

Kata Containers validates this approach: their agent is ~15MB of Rust, runs as PID 1 directly, handles multi-container pods without systemd.

### PID 1 responsibilities

| Responsibility | Details |
|---|---|
| **Reap zombies** | Essential for any PID 1 — `waitpid(-1, WNOHANG)` in event loop |
| **Mount filesystems** | Boot-time: proc, sys, devpts, shm, tmp. Per-container: rootfs mount + OCI spec mounts |
| **Multi-container lifecycle** | Independent start/stop per container, init container ordering |
| **Signal forwarding** | Route signals to correct container process by ID |
| **Exec support** | Per-container exec (spawn additional processes in a container's chroot) |
| **Exit status reporting** | Per-container exit code propagation to host via vsock |
| **Shared pod resources** | Pod-level volumes mounted once, bind-mounted into each container's rootfs |
| **I/O multiplexing** | Per-container stdout/stderr streams forwarded to host |

### Agent structure (pseudocode)

```rust
struct Agent {
    containers: HashMap<String, ContainerState>,
    vsock: VsockListener,
    signal_fd: SignalFd,  // signalfd for SIGCHLD
}

struct ContainerState {
    id: String,
    config: ContainerConfig,
    rootfs_mount: PathBuf,        // /containers/<id>
    device: PathBuf,              // /dev/vdX
    main_process: Option<Child>,
    execs: HashMap<String, Child>,
    status: ContainerStatus,      // Created, Running, Stopped
}

enum ContainerStatus { Created, Running, Stopped { exit_code: i32 } }

// PID 1 main loop
fn main() {
    // 1. Mount essential filesystems
    //    /dev is auto-populated by devtmpfs (CONFIG_DEVTMPFS_MOUNT=y)
    mount("proc", "/proc", "proc", 0, None);
    mount("sysfs", "/sys", "sysfs", 0, None);
    mount("devpts", "/dev/pts", "devpts", 0, None);
    mount("tmpfs", "/dev/shm", "tmpfs", 0, None);
    mount("tmpfs", "/tmp", "tmpfs", 0, None);

    // 2. Connect to host via vsock
    let listener = VsockListener::bind(CID_ANY, AGENT_PORT);
    let stream = listener.accept();
    send_status(stream, Ready);

    // 3. Event loop: handle commands + reap children
    let mut agent = Agent::new(stream);
    loop {
        select! {
            cmd = agent.vsock.recv() => {
                match cmd {
                    AddContainer { id, device, config } => {
                        // Mount container rootfs from virtio-blk device
                        let mount_path = format!("/containers/{}", id);
                        mkdir(&mount_path);
                        mount(&device, &mount_path, "ext4", 0, None);
                        // Apply OCI spec mounts
                        for m in &config.mounts {
                            mount(m.source, join(&mount_path, &m.dest), ...);
                        }
                        // Apply pod-level shared volume bind mounts
                        for v in &config.volumes {
                            bind_mount(&v.host_path, join(&mount_path, &v.container_path));
                        }
                        agent.containers.insert(id, ContainerState::created(config, mount_path));
                    }
                    StartContainer { id } => {
                        let ctr = agent.containers.get_mut(&id);
                        match fork() {
                            Child => {
                                chroot(&ctr.rootfs_mount);
                                chdir(&ctr.config.cwd);
                                setgid(ctr.config.gid);
                                setuid(ctr.config.uid);
                                set_env(&ctr.config.env);
                                exec(&ctr.config.entrypoint, &ctr.config.args);
                            }
                            Parent(pid) => {
                                ctr.main_process = Some(pid);
                                ctr.status = Running;
                                send_status(stream, ContainerStarted { id, pid });
                            }
                        }
                    }
                    StopContainer { id, signal, timeout } => { ... }
                    RemoveContainer { id } => {
                        // Unmount rootfs, clean up
                        umount(&ctr.rootfs_mount);
                        agent.containers.remove(&id);
                    }
                    ExecInContainer { id, exec_id, cmd } => {
                        let ctr = agent.containers.get_mut(&id);
                        // Fork into container's chroot, exec cmd
                        ...
                    }
                    SignalContainer { id, signal } => {
                        let ctr = agent.containers.get(&id);
                        kill(ctr.main_process, signal);
                    }
                    Shutdown => {
                        reboot(LINUX_REBOOT_CMD_POWER_OFF);
                    }
                }
            }
            sigchld = agent.signal_fd.read() => {
                // Reap all finished children, update container states
                loop {
                    match waitpid(-1, WNOHANG) {
                        Ok((pid, status)) => {
                            if let Some(ctr) = find_container_by_pid(pid) {
                                ctr.status = Stopped { exit_code: status };
                                send_status(stream, ContainerExited { id: ctr.id, code: status });
                            }
                        }
                        Err(ECHILD) => break,
                    }
                }
            }
        }
    }
}
```

This is ~1000-1500 lines of Rust — still minimal and purpose-built. No namespace setup, no cgroup management inside the VM. Just mount + fork + exec + reap, multiplied by N containers.

### vsock protocol (pod-aware)

```
Host → Guest:
  AddContainer { id, device, config: ContainerConfig }
  StartContainer { id }
  StopContainer { id, signal, grace_period }
  RemoveContainer { id }
  ExecInContainer { id, exec_id, cmd, args, env }
  SignalContainer { id, signal }
  Shutdown

ContainerConfig:
  { entrypoint, args, env, cwd, uid, gid, mounts, volumes }

Guest → Host:
  Ready                                    (agent booted, awaiting commands)
  ContainerStarted { id, pid }             (container process running)
  ContainerExited { id, code }             (container process exited)
  ExecStarted { id, exec_id, pid }         (exec process running)
  ExecExited { id, exec_id, code }         (exec process exited)
  Stdout { id, data }                      (per-container stdout)
  Stderr { id, data }                      (per-container stderr)
  Error { id, message }                    (per-container error)
```

---

## Host-Side Runtime Design

The host-side binary implements the OCI runtime interface so it can be used as a drop-in with containerd/podman.

### OCI commands (reuse `liboci-cli` for parsing)

| Command | What it does |
|---------|-------------|
| `create` | Build disk image from bundle, configure VMM, start VM (paused or waiting) |
| `start` | Signal VM to proceed (via vsock or resume paused VM) |
| `state` | Return container state JSON (reuse `libcontainer::container::state`) |
| `kill` | Send signal to VM process (or forward via vsock to guest) |
| `delete` | Stop VM, clean up disk image and state directory |

### Additional commands
| Command | What it does |
|---------|-------------|
| `exec` | Attach to running VM via vsock, request guest init to spawn additional process |
| `pause/resume` | Pause/resume VM via VMM API |

### VMM integration

Abstract the VMM behind a trait:

```rust
trait VmmDriver {
    fn create_vm(&self, config: &VmConfig) -> Result<VmHandle>;
    fn start_vm(&self, handle: &VmHandle) -> Result<()>;
    fn stop_vm(&self, handle: &VmHandle) -> Result<()>;
    fn pause_vm(&self, handle: &VmHandle) -> Result<()>;
    fn resume_vm(&self, handle: &VmHandle) -> Result<()>;
    fn attach_block_device(&self, handle: &VmHandle, path: &Path) -> Result<()>;
}
```

Implementations:
- **Firecracker** — REST API over Unix socket
- **Cloud Hypervisor** — REST API over Unix socket
- **QEMU microvm** — command-line + QMP socket

---

## Rootfs Transport Comparison

| Method | Firecracker | Cloud Hypervisor | QEMU | Performance | Complexity |
|--------|-------------|-----------------|------|-------------|------------|
| **virtio-blk + ext4** | Yes | Yes | Yes | Good | Medium (image build) |
| **virtio-blk + squashfs + overlay** | Yes | Yes | Yes | Good (reads) | Medium |
| **virtio-fs** | No | Yes | Yes | Good (DAX) | Low (no image build) |
| **virtio-pmem** | No | Yes | Yes | Best | High |

**Recommendation for broadest VMM support:** virtio-blk with ext4 image. Can add virtio-fs as a fast path for Cloud Hypervisor later.

---

## Dependency Map

```
our-runtime (host binary)
├── oci-spec            — parse config.json (already a youki dep)
├── liboci-cli          — CLI parsing (youki crate, use directly)
├── libcgroups          — resource limits on VMM process (youki crate)
├── libcontainer        — cherry-pick:
│   ├── signal          — signal name/number mapping
│   ├── capabilities    — OCI cap names → Linux constants
│   └── container::state — container state machine
├── container-image-builder — NEW: pack OCI rootfs into ext4/squashfs
├── vmm-driver              — NEW: trait + impls for Firecracker/CH/QEMU
└── (master image build is handled by Nix, not a Rust crate)

guest-agent (static binary, lives in master image, runs as PID 1)
├── nix / libc          — mount, chroot, fork, exec, setuid/gid, waitpid, vsock
├── serde / serde_json  — deserialize container config from vsock
└── (no youki deps — must be minimal and self-contained)

master guest image (built via Nix)
├── guest-agent binary  — cross-compiled for x86_64-unknown-linux-musl
├── minimal kernel      — custom .config via linuxManualConfig
└── ext4/squashfs image — via nixos/lib/make-ext4-fs.nix or make-squashfs.nix
```

---

## Containerd Integration: Snapshotters

Since we don't handle image pull/cache/GC, the containerd snapshotter determines how we receive the container filesystem. Two relevant options:

### overlayfs snapshotter (default)
- containerd unpacks image layers and assembles them into a merged rootfs directory via overlayfs
- We receive a path to this merged directory
- We must **copy it into a disk image** (ext4/squashfs) — this is the "container rootfs image" step
- Simple but involves a full copy on every container create

### devmapper snapshotter
- containerd uses thin-provisioned LVM block devices for image layers
- Each container gets a **block device snapshot** (thin clone of the image layers)
- We could potentially **pass this block device directly as virtio-blk** to the VM — no image build step needed
- This is exactly what Kata Containers + Firecracker does
- Advantages:
  - **No copy on create** — just create a thin snapshot (instant, CoW)
  - **Deduplication** — shared base layers are shared at the block level
  - **Write support** — the snapshot is read-write, writes are CoW
  - **GC handled by containerd** — snapshotter manages thin pool
- Disadvantages:
  - Requires devmapper/LVM setup on host (thin pool)
  - More complex host configuration
  - Less portable than a simple directory copy

### Recommendation

Support both paths:
1. **Directory-based** (overlayfs snapshotter) — pack into squashfs/ext4, works everywhere, simpler setup
2. **Block-device-based** (devmapper snapshotter) — pass block device directly, zero-copy create, better for production

The runtime detects which it receives (directory vs block device path) and acts accordingly.

---

## Multi-Container Pods

### Why pods matter

In Kubernetes, the **pod** is the smallest schedulable unit. A pod contains one or more containers that share:
- **Network** — all containers in a pod see the same IP, can communicate via localhost
- **IPC** — shared IPC namespace (System V IPC, POSIX message queues)
- **Storage** — shared volumes mounted into multiple containers
- **Init ordering** — init containers run to completion before app containers start

The CRI (Container Runtime Interface) is fundamentally pod-oriented. Without pod support, Kubernetes integration is not possible.

### Why VMs make this easier

In a traditional container runtime, pod semantics require careful namespace sharing between separate container processes. In a VM-based runtime, pod semantics are **natural**:

| Pod requirement | Traditional runtime | VM-based runtime |
|---|---|---|
| Shared network | Create network namespace, join all containers to it | All containers share the VM's network stack — free |
| Shared IPC | Create IPC namespace, join all containers | All containers share the VM's IPC — free |
| Shared volumes | Bind-mount into each container's mount namespace | Mount once in VM, bind-mount into each chroot |
| Process isolation | Separate PID namespaces per container | Separate chroots, separate process groups |

### Pod lifecycle

```
1. Host: CreatePod — launch VM with master image + N container disk images
                     (virtio-blk 1..N, or hot-plug devices after boot)
2. Host: AddContainer(init-1) → Guest: mount rootfs, record config
3. Host: StartContainer(init-1) → Guest: fork+exec, wait for exit
4. Host: AddContainer(app-1) → Guest: mount rootfs
5. Host: AddContainer(sidecar-1) → Guest: mount rootfs
6. Host: StartContainer(app-1), StartContainer(sidecar-1) → Guest: fork+exec both
7. ...containers run...
8. Host: StopContainer(app-1) → Guest: signal + wait
9. Host: RemoveContainer(app-1) → Guest: unmount rootfs
10. Host: Shutdown → Guest: stop all remaining, power off
```

### Hot-plugging container disks

For pods where containers are added after VM boot (init containers finishing, sidecars added later), the VMM must support **hot-plugging virtio-blk devices**:
- **Firecracker** — supports hot-plugging block devices via API
- **Cloud Hypervisor** — supports hot-plug via API
- **QEMU** — supports via QMP

The guest agent detects new block devices (via `AddContainer` command specifying the device path) and mounts them.

---

## Building the Guest Image with Nix

Nix is a natural fit for building the master guest image: reproducible, minimal, declarative, and avoids the privilege issues (mknod) that make traditional image building awkward.

### Why Nix?

- **Reproducibility** — byte-for-byte identical images across builds, important for security auditing and distribution
- **Minimal closure** — Nix computes the exact dependency closure, no extra packages leak in
- **Kernel building** — nixpkgs has solid infrastructure for custom kernel configs (`linuxManualConfig`)
- **Cross-compilation** — building the guest agent as a static musl binary is straightforward with `pkgsStatic`
- **Image building** — `nixos/lib/make-ext4-fs.nix` and `nixos/lib/make-squashfs.nix` are available
- **No privilege needed** — `devtmpfs` eliminates `mknod` at build time, so the entire build runs in the Nix sandbox

### Example Nix expression

```nix
{ pkgs, ... }:
let
  # Minimal kernel with only virtio + ext4 + devtmpfs
  guestKernel = pkgs.linuxManualConfig {
    src = pkgs.linux_latest.src;
    version = pkgs.linux_latest.version;
    configfile = ./guest-kernel.config;
    allowImportFromDerivation = true;
  };

  # Static musl guest agent
  guestAgent = pkgs.callPackage ./guest-agent {
    # Builds with --target x86_64-unknown-linux-musl
    # Produces a ~1-2MB stripped static binary
  };

  # Compose the rootfs contents (no device nodes needed — devtmpfs handles /dev)
  guestRootfs = pkgs.runCommand "guest-rootfs" {} ''
    mkdir -p $out/{sbin,dev,proc,sys,tmp,containers}
    cp ${guestAgent}/bin/guest-agent $out/sbin/init
  '';

  # Build the master image as ext4
  masterImage = pkgs.callPackage <nixpkgs/nixos/lib/make-ext4-fs.nix> {
    storePaths = [ guestRootfs ];
    populateImageCommands = ''
      cp -a ${guestRootfs}/* ./files/
    '';
    volumeLabel = "youki-guest";
  };
in {
  inherit guestKernel masterImage;
  # Firecracker needs separate kernel + rootfs files
  # Cloud Hypervisor can use a single combined image or separate files
}
```

### Key design decisions for Nix build

1. **devtmpfs eliminates mknod** — with `CONFIG_DEVTMPFS_MOUNT=y`, the kernel auto-populates `/dev` at boot. No device nodes need to be baked into the image. This is critical because `mknod` requires root/`CAP_MKNOD`, which is unavailable in the Nix sandbox.

2. **Kernel config as a tracked file** — the `.config` file lives in the repo, reviewed and versioned. Changes to kernel config produce a different image hash.

3. **Separate kernel + rootfs outputs** — Firecracker requires the kernel as a separate file (not embedded in the disk image). The Nix expression produces both as separate derivations.

4. **Image versioning** — the Nix store hash serves as the version. The host runtime can verify it's using the expected master image.

---

## Open Questions

1. **Networking:** virtio-net with TAP on host? Or vsock-based networking (like libkrun's TSI)? TAP is simpler but requires bridge/NAT setup on host.

2. **Storage:** For directory-based path: ext4 (read-write, simpler) vs squashfs + tmpfs overlay (smaller, faster build, ephemeral writes)?

3. ~~**Multi-container pods:** Kata solves this by running multiple containers in one VM. Do we need this?~~ **Resolved:** Yes, design for multi-container pods from day one. The CRI is pod-oriented; without it, no Kubernetes support. The complexity increase (~2-3x agent code) is manageable and avoids a painful retrofit.

4. ~~**PID 1 / systemd:**~~ **Resolved:** Custom Rust PID 1, no systemd. Boot time, image size, semantic mismatch, and cgroup ownership all argue against systemd. Kata validates this approach.

5. ~~**Master image build:**~~ **Resolved:** Build with Nix for reproducibility. Use devtmpfs to avoid mknod at build time. Kernel config tracked in repo. Separate kernel + rootfs outputs for Firecracker compatibility.

6. **devmapper integration depth:** Do we just accept a block device path from containerd, or do we need to interact with the thin pool ourselves? Kata delegates this entirely to the snapshotter.

7. **Hot-plug vs pre-attach:** For multi-container pods, attach all container disks at VM boot? Or hot-plug as containers are added? Hot-plug is more flexible (init containers) but adds complexity. May depend on VMM capabilities.

8. **Pod-level networking setup:** Who configures the VM's network? The host runtime (via VMM API) or a CNI plugin? Kata uses a dedicated network namespace on the host connected to the VM via TAP + tc redirect.
