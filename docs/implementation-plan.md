# Implementation Plan: First Container Launch

Goal: Boot a VM, mount an ext4 container rootfs, and exec a simple process inside it.

---

## Phase 1: Guest Init — Real PID 1

### 1.1 Mount essential filesystems
- [x] Mount `/proc` (proc)
- [x] Mount `/sys` (sysfs)
- [x] Mount `/tmp` (tmpfs)
- [x] Mount `/dev/pts` (devpts)
- [x] Mount `/dev/shm` (tmpfs)
- [x] `/dev` already handled by devtmpfs — verify this works

Notes: Implemented in `guest-image/guest-init/src/main.rs`. All mounts use correct flags (MS_NOSUID, MS_NODEV, MS_NOEXEC where appropriate). Currently the init just mounts and then calls `reboot(RB_POWER_OFF)` — needs to be replaced with vsock listener loop.


### 1.2 Vsock listener
- [x] Add vsock support (listen on a fixed port, e.g. 1024)
- [x] Define a simple message protocol (serde + JSON to start, can optimize later)
- [x] Send `Ready` message to host on boot
- [x] Implement request/response loop

Notes: Implemented using raw libc AF_VSOCK (no external vsock crate — safe for static musl builds). Protocol is length-prefixed (4-byte LE u32) JSON via serde. Modules: `vsock.rs` (listener/stream with send/recv), `protocol.rs` (HostMessage/GuestMessage enums). Message handlers for AddContainer/StartContainer are stubs returning Error — will be implemented in 1.3.


### 1.3 Container lifecycle (guest side)
- [x] `AddContainer`: mount virtio-blk device (`/dev/vdb`, etc.) as ext4 at `/containers/<id>`
- [x] `StartContainer`: fork + chroot + chdir + exec entrypoint
- [x] Reap children via SIGCHLD / waitpid(-1, WNOHANG)
- [x] Report exit status back over vsock
- [x] `Shutdown`: clean power off

Notes: Implemented in `container.rs` (ContainerManager with add/start/reap_children). Child setup: setsid + chroot + mount /proc,/sys,/dev,/tmp + execv. Event loop in main.rs uses poll() on vsock fd + signalfd(SIGCHLD) for non-blocking child reaping. Exit codes reported as ContainerExited messages. Signal-killed processes report 128+signum.


---

## Phase 2: Host Side — Build Container Image

### 2.0 Shared protocol crate
- [x] Extract `protocol.rs` into `distvirt-guest-protocol/` crate
- [x] Both `HostMessage` and `GuestMessage` now derive `Serialize` + `Deserialize`
- [x] `VSOCK_PORT` constant exported from shared crate
- [x] `guest-init` updated to depend on shared crate, `protocol.rs` deleted

Notes: `distvirt-guest-protocol/` depends only on `serde`, so it's compatible with the static musl guest-init build. Guest-init imports via `use distvirt_guest_protocol::{GuestMessage, HostMessage, VSOCK_PORT}`.

### 2.1 Ext4 image builder
- [x] Take a rootfs directory path as input
- [x] Calculate required size (du + margin)
- [x] Create ext4 image using `mkfs.ext4 -d <rootfs>` (no loopback mount needed)
- [x] Output path to the built image

Notes: Implemented in `distvirt/src/image.rs`. Shells out to `du -sb`, `truncate`, `mkfs.ext4 -d`, `resize2fs -M`. Requires `e2fsprogs` on the host. Image size = rootfs size * 1.2 + 10MB, then shrunk to minimum.


---

## Phase 3: Host Side — Launch VM with Container

### 3.1 VMM launcher
- [x] Programmatically launch Firecracker with:
  - virtio-blk 0: master image
  - virtio-blk 1: container ext4 image
  - vsock device enabled
- [x] Connect to guest agent over vsock
- [x] Wait for `Ready` message

Notes: Implemented with trait-based VMM abstraction in `distvirt/src/vmm/`. `Vmm` trait (launch) and `VmInstance` trait (connect_vsock, wait, kill). Firecracker implementation in `vmm/firecracker.rs` uses raw HTTP over Unix socket for the API (no hyper/reqwest). Each PUT uses a fresh UnixStream connection. Vsock connection uses retry loop with 30s timeout. Rootfs image copied to tmpdir (Firecracker needs writable). TempDir cleaned up on drop.

### 3.2 Host-side vsock client
- [x] Length-prefixed JSON client matching guest wire format

Notes: Implemented in `distvirt/src/vsock_client.rs`. `GuestConnection` wraps `UnixStream` with `BufReader`/`BufWriter`. `send()` and `recv()` use 4-byte LE length prefix + JSON, identical to guest's `vsock.rs`.

### 3.3 Container orchestration
- [x] Send `AddContainer` (device path, config)
- [x] Send `StartContainer`
- [x] Receive exit status
- [x] Send `Shutdown`

Notes: Implemented in `distvirt/src/orchestrate.rs`. `run_container()` function handles the full lifecycle: build ext4 image → launch VM → connect vsock → wait for Ready → AddContainer(/dev/vdb) → StartContainer → wait for ContainerExited → Shutdown → wait for VM exit. Returns the container's exit code.

### 3.4 CLI
- [x] `build-image --rootfs <dir> --output <path>`
- [x] `run --kernel <path> --rootfs-image <path> --container-rootfs <dir> --entrypoint <cmd> [--args ...]`

Notes: Implemented in `distvirt-cli/` with clap derive. Binary name is `distvirt`. The `run` subcommand also accepts `--firecracker <path>` (defaults to `firecracker` in PATH).


---

## Phase 4: End-to-End Test

- [ ] Obtain a simple rootfs (Alpine minirootfs or busybox)
- [ ] Build ext4 image from it
- [ ] Boot VM with both images attached
- [ ] Run `/bin/sh -c "echo hello from container"` inside container
- [ ] Verify exit code propagated to host
- [ ] VM shuts down cleanly

Notes: Use `distvirt-cli` to drive the full flow. Alpine minirootfs (~3MB) is a good first target.


---

## Decisions / Open Items

- **Vsock protocol format:** JSON to start, revisit if performance matters
- **Error handling in guest init:** panic vs log-and-continue — guest init must never crash (it's PID 1)
- ~~**Which VMM to target first:**~~ Firecracker for production. Trait abstraction for future VMMs (krun, etc).
- **Container config:** Start with minimal hardcoded config, expand to full OCI spec parsing later
- **Image builder abstraction:** Currently `build_ext4_image()` is a plain function. May need an `ImageBuilder` trait to support containerd devmapper as an alternative backend.
