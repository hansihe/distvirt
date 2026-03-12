---
title: "Container Compatibility Fixes"
---

> **Status (March 2026):** ~85% complete. Most compatibility fixes landed; remaining items are low-priority edge cases.

Checklist of issues in `guest-image/guest-init/` compared to proper OCI
container runtime behaviour. Ordered by priority.

Context: guest-init runs inside a Firecracker microVM, so the VM boundary
already provides hardware-level isolation. Many of these are still important
for correct multi-container behaviour within a single VM/pod.

## High Priority

- [x] **Add KillContainer / signal support**
  `HostMessage::SignalContainer { id, signal }` sends an arbitrary signal to a
  container's PID. Guest-init replies `ContainerSignaled` on success.
  Worker-side `ManagedVm::signal_container()` stub exists but is not yet
  integrated into the pod lifecycle.

- [x] **Add mount namespace (`unshare(CLONE_NEWNS)`)**
  Containers share the mount namespace. Mounts done inside one container are
  visible to guest-init and other containers. `/proc` and `/sys` inside chroot
  also lack proper masking — a container can read sensitive info about other
  processes.

- [x] **Fix `sethostname()` affecting entire VM**
  `container.rs:337-340` — `sethostname()` is called without a UTS namespace,
  so every container start changes the hostname for all containers in the VM.
  Either add `unshare(CLONE_NEWUTS)` or remove the call.

## Medium Priority

- [ ] **Switch from `chroot` to `pivot_root`**
  `chroot()` is not a proper isolation boundary — a root process can escape via
  `fchdir()` on an open fd. `pivot_root()` (requires mount namespace) properly
  detaches the old root. Limited blast radius inside a VM but still allows
  container-to-container interference.

- [ ] **Add `close_range()` before exec**
  No FD cleanup before `execve()`. Pipes use `O_CLOEXEC` but any FDs without
  it could leak into the container. Should call
  `close_range(3, MAX, CLOSE_RANGE_CLOEXEC)` before exec.

- [ ] **Drop capabilities after uid/gid switch**
  After `setuid()`/`setgid()`, capabilities are not dropped. A container
  running as root (uid 0) has full capabilities — can `mount()`, `mknod()`,
  load kernel modules, etc. Should drop to a minimal capability set.

- [x] **Add PID namespace (`unshare(CLONE_NEWPID)`)**
  Without a PID namespace, containers can see and signal each other's
  processes via `/proc`. Important for multi-container pods.

- [x] **Add stdin forwarding**
  When `StartContainer { stdin: true }`, guest-init creates a pipe for the
  container's stdin and accepts inbound yamux streams with
  `StreamHeader::ContainerInput` to relay data into the pipe. Worker-side
  `GuestSession::open_input_stream()` stub exists but is not yet integrated
  into the pod lifecycle.

## Low Priority

- [ ] **Add resource limits (cgroups / rlimits)**
  No cgroups or rlimits. A container can consume all VM resources. Since the
  VM itself is resource-bounded this is less critical, but for multi-container
  pods one container could starve others.

- [ ] **Reopen `/dev/null` after pivot_root**
  Should reopen `/dev/null` from inside the container post-pivot to ensure it
  points to the container's device, not the host's.

## Working Correctly

- Environment variable isolation — `execve()` passes only explicitly provided
  env vars
- uid/gid switching — correct order (`setgid` before `setuid`)
- Session creation — `setsid()` before exec
- Output capture — stdout/stderr pipes with async forwarding over yamux
- Stdin forwarding — optional pipe-based stdin with yamux inbound stream relay
- Signal delivery — per-container signal support via `SignalContainer` message
- Child reaping — SIGCHLD via signalfd + `waitpid(-1, WNOHANG)`, no zombies
- FD hygiene — `O_CLOEXEC` on pipes/sockets, `OwnedFd` RAII
- Mount flags — `MS_NOSUID | MS_NODEV | MS_NOEXEC` on proc/sys
- DNS/hostname/hosts file setup
