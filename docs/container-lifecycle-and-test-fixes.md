# Container Lifecycle & Test Fixes

Issues identified while debugging hanging E2E service tests
(`test_service_backend_buffer_and_flush`, `test_service_backend_ready_forward`).

The tests hang because the backend's `nc -l -p 80` never receives TCP FIN,
so it never exits. This is caused by multiple interacting problems across
the guest-init shutdown path, the host-side managed VM lifecycle, and the
test commands themselves.

## Status of the hanging tests

After the fabric DNAT/SNAT forwarding fixes, the traffic path works
end-to-end:

1. Client SYN → activator captures (or buffer)
2. `mark_ready` → ReplayPacket / buffer flush delivers SYN to backend (DNAT)
3. SYN-ACK from backend → SNAT → client
4. TCP handshake completes, data ("hello-buffered") is delivered
5. Backend ACKs are SNAT'd correctly back to client

The hang occurs during connection teardown. The client's `nc -w 30` waits
~60s, then exits. The client VM immediately shuts down (`sync()` →
`reboot()`), destroying the TAP device before the kernel can transmit a
FIN. The backend's `nc -l -p 80` never gets EOF and waits forever.

---

## Fix 1: Graceful container shutdown in guest-init (high priority)

**Files:** `guest-image/guest-init/src/main.rs`, `guest-image/guest-init/src/container.rs`

### Current behavior

When `HostMessage::Shutdown` is received (`main.rs:197-200`):
```rust
HostMessage::Shutdown => {
    log::info!("shutdown requested");
    return Ok(true); // → sync() → reboot(RB_AUTOBOOT)
}
```

No SIGTERM is sent to running containers. They die when the VM reboots.
This means:
- Containers can't clean up (close sockets, flush buffers)
- TCP FIN packets are never sent — peers see the connection vanish
- Pipe output that hasn't been drained is lost

### Desired behavior

When `Shutdown` is received:
1. SIGTERM all running container processes
2. Wait for them to exit via SIGCHLD/waitpid (with ~5s timeout)
3. SIGKILL any still running
4. Drain remaining pipe output for each exited container
5. Brief delay (~200ms) for network I/O to flush through virtio-net
6. `sync()` → `reboot()`

### Implementation — DONE

Added to `container.rs`:
- `signal_all_running(signal)` — iterates all containers, calls `kill(pid, signal)` for each with `pid.is_some()`, logs errors
- `has_running_containers()` — returns true if any container has `pid.is_some()`

In `main.rs`, after the main loop breaks on `Shutdown`:
1. SIGTERM all running containers via `signal_all_running(SIGTERM)`
2. Poll signalfd + reap children in a loop with 5s `async_io::Timer` deadline
3. For each reaped child: drain pipes, send `ContainerExited`, remove container
4. If containers remain after timeout, SIGKILL + brief reap
5. 200ms sleep for virtio-net flush
6. Falls through to existing `sync()` → `reboot()`

---

## Fix 2: Host-side graceful shutdown escalation (medium priority)

**Files:** `distvirt-worker/src/managed_vm.rs`, `distvirt-worker/src/worker.rs`

### Current behavior

`ManagedVm::shutdown()` sends `HostMessage::Shutdown` and waits for the
VM process to exit. No per-container signal is sent first.

The `pod_monitor` shutdown path (both normal exit and cancellation) calls
`vm.shutdown()` directly.

### Desired behavior

`ManagedVm` should expose a `graceful_shutdown()` that:
1. Sends `SignalContainer { signal: SIGTERM }` for each known container
2. Waits for `ContainerExited` events (with timeout)
3. Sends `Shutdown` (guest-init handles any stragglers via Fix 1)

This gives the host visibility into container exit codes even during
shutdown, and allows the host to log/report them.

### Implementation — DONE

In `managed_vm.rs`:
- Added `started_containers: Vec<String>` field to `ManagedVm`
- `start_container` pushes the container ID to `started_containers`
- `wait_container_exit` removes the container from `started_containers` (via `retain`)
- Added `graceful_shutdown(timeout)` method:
  1. Sends `SignalContainer { signal: SIGTERM }` for each started container (best-effort)
  2. Drains `ContainerSignaled` acks (500ms timeout each)
  3. Waits for `ContainerExited` events until all accounted for or timeout
  4. Calls `shutdown()` to send `Shutdown` and wait for VM exit

In `worker.rs`:
- Both normal container exit and cancellation paths in `pod_monitor` now call
  `vm.graceful_shutdown(Duration::from_secs(8))` instead of `vm.shutdown()`
- The 8s timeout leaves headroom within the 10s `GRACEFUL_SHUTDOWN_TIMEOUT`

**Note:** Because `wait_container_exit` removes containers from the tracking
list, `graceful_shutdown` after a natural container exit correctly skips
already-exited containers (avoids signaling containers the guest has already
removed).

---

## Fix 3: TCP RST injection on fabric port removal (low priority, defense-in-depth)

**File:** `distvirt-worker/src/fabric/forwarding.rs`

### Problem

When a VM dies (crash, force-kill, or even graceful shutdown that races),
its fabric port is removed. Any established TCP connections from peers
will hang until TCP keepalive fires (2 hours by default on Linux).

### Desired behavior

When a port is removed from the fabric:
1. Scan the NAT table for entries involving the removed port's IP/MAC
2. For each entry, craft a TCP RST packet and send it to the peer port
3. Remove the NAT entries

This is defense-in-depth — with Fix 1, graceful shutdown should handle
most cases. But crashes and force-kills will always exist.

### Implementation notes

- The NAT table has `NatFlowKey` (5-tuple) and `NatEntry` (service_ip,
  backend_ip, service_mac)
- Need to identify entries by backend_ip or iterate all entries
- RST packet construction: needs correct seq/ack numbers, which the
  NAT table doesn't currently store
- Alternative: send RST with seq=0 and let the peer's TCP stack handle
  the challenge ACK → RST sequence (simpler but less clean)
- Could also just rely on shorter TCP keepalive timeouts in guests

---

## Fix 4: E2E test command fixes (high priority, quick win)

### 4a. Remove redundant `ip addr add` in backend commands

**Lines:** `e2e.rs:918`, `e2e.rs:1072`

Both service tests have the backend run:
```sh
ip addr add 10.0.0.99/32 dev eth0 && nc -l -p 80
```

With DNAT, the backend never sees packets addressed to 10.0.0.99 — the
fabric rewrites the destination to 10.0.0.2 before delivery. The
`ip addr add` is now dead code. Remove it:
```sh
nc -l -p 80
```

### 4b. Add idle timeout to backend `nc`

**Lines:** `e2e.rs:918`, `e2e.rs:1072`

Backend `nc -l -p 80` waits forever for the peer to close. Even with
graceful shutdown (Fix 1), the backend should have its own timeout as
defense-in-depth:
```sh
nc -l -w 5 -p 80
```

### 4c. Reduce client `nc` timeout

**Lines:** `e2e.rs:975`, `e2e.rs:1112`

The client commands use `-w 10` and `-w 30` respectively. These cause
unnecessarily long waits before the test can complete. Reduce to `-w 5`:

| Line | Current | Proposed |
|------|---------|----------|
| 918  | `ip addr add 10.0.0.99/32 dev eth0 && nc -l -p 80` | `nc -l -w 5 -p 80` |
| 975  | `echo hello-service \| nc -w 10 10.0.0.99 80` | `echo hello-service \| nc -w 5 10.0.0.99 80` |
| 1072 | `ip addr add 10.0.0.99/32 dev eth0 && nc -l -p 80` | `nc -l -w 5 -p 80` |
| 1112 | `echo hello-buffered \| nc -w 30 10.0.0.99 80` | `echo hello-buffered \| nc -w 5 10.0.0.99 80` |

### 4d. TCP activator test (`test_tcp_activator_activation`)

**Line:** `e2e.rs:829`
```sh
nc -w 1 10.0.0.99 80 || true
```
This one is fine — it only tests that the SYN triggers `BackendNeed`,
the `|| true` handles expected connection failure, and `-w 1` is short.

---

## Fix 5: `stdin: false` behavior in guest-init (informational)

**File:** `guest-image/guest-init/src/container.rs:415-439`

When `stdin: false`, the container's stdin is wired to `/dev/console`
(or `/dev/null` if console open fails). This means nc's stdin is not
a pipe that gets EOF — it's a tty/devnull that never closes.

This interacts with the nc timeout: busybox `nc` with a pipe stdin
sends FIN shortly after stdin EOF. With `/dev/null` as stdin, nc reads
EOF immediately and should close the write side. With `/dev/console`,
nc's stdin never gets EOF (it's a tty), so nc never initiates close.

For the **client** pods this works because the entrypoint is
`echo ... | nc ...` — the shell pipe provides stdin to nc, and echo
exits quickly. The `stdin: false` affects the shell's own stdin, not
nc's (nc's stdin is the pipe from echo).

For the **backend** pods, nc's stdin comes from the shell, which gets
its stdin from the container stdin setup. With `stdin: false`, this is
console/devnull. With `/dev/null`, `nc -l` would read EOF on stdin
immediately after accepting a connection and send FIN to the client.
With `/dev/console`, nc blocks reading the tty forever. The actual
behavior depends on which path guest-init takes, which depends on
whether `/dev/console` exists in the container rootfs.

No action needed here, but worth understanding when debugging nc
behavior in tests.

---

## Execution order

1. ~~**Fix 4** (test commands) — immediate unblock, 5 minutes~~
2. ~~**Fix 1** (guest-init graceful shutdown) — proper fix, needs guest
   image rebuild~~ **DONE**
3. ~~**Fix 2** (host-side escalation) — builds on Fix 1~~ **DONE**
4. **Fix 3** (RST injection) — nice-to-have, can defer
