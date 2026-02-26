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
- [ ] `AddContainer`: mount virtio-blk device (`/dev/vdb`, etc.) as ext4 at `/containers/<id>`
- [ ] `StartContainer`: fork + chroot + chdir + exec entrypoint
- [ ] Reap children via SIGCHLD / waitpid(-1, WNOHANG)
- [ ] Report exit status back over vsock
- [ ] `Shutdown`: clean power off

Notes:


---

## Phase 2: Host Side — Build Container Image

### 2.1 Ext4 image builder
- [ ] Take a rootfs directory path as input
- [ ] Calculate required size (du + margin)
- [ ] Create ext4 image using `mkfs.ext4 -d <rootfs>` (no loopback mount needed)
- [ ] Output path to the built image

Notes:


---

## Phase 3: Host Side — Launch VM with Container

### 3.1 VMM launcher
- [ ] Programmatically launch Firecracker (or QEMU) with:
  - virtio-blk 0: master image
  - virtio-blk 1: container ext4 image
  - vsock device enabled
- [ ] Connect to guest agent over vsock
- [ ] Wait for `Ready` message

Notes: Shell script prototypes exist for both Firecracker (`guest-image/scripts/run-firecracker.sh`) and QEMU (`guest-image/scripts/run-qemu.sh`). Nix flake builds kernel, guest-init (static musl binary), and rootfs image. No programmatic Rust VMM integration yet.


### 3.2 Container orchestration
- [ ] Send `AddContainer` (device path, config)
- [ ] Send `StartContainer`
- [ ] Receive exit status
- [ ] Send `Shutdown`

Notes:


---

## Phase 4: End-to-End Test

- [ ] Obtain a simple rootfs (Alpine minirootfs or busybox)
- [ ] Build ext4 image from it
- [ ] Boot VM with both images attached
- [ ] Run `/bin/sh -c "echo hello from container"` inside container
- [ ] Verify exit code propagated to host
- [ ] VM shuts down cleanly

Notes:


---

## Decisions / Open Items

- **Vsock protocol format:** JSON to start, revisit if performance matters
- **Error handling in guest init:** panic vs log-and-continue — guest init must never crash (it's PID 1)
- **Which VMM to target first:** Firecracker (already have a working script) or QEMU (more flexible for dev)?
- **Container config:** Start with minimal hardcoded config, expand to full OCI spec parsing later
