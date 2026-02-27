# Container Output / Logs Not Visible - Debugging Summary

## Status

Containers start successfully inside Firecracker VMs, but no stdout/stderr output
is forwarded to the host, and no container exit events are received.

## What Was Fixed

### CONFIG_TMPFS missing from guest kernel

The guest kernel config was missing `CONFIG_TMPFS` (it only had `CONFIG_DEVTMPFS`).
These are different things:
- `CONFIG_DEVTMPFS` - special-purpose filesystem for `/dev` auto-populated by the kernel
- `CONFIG_TMPFS` - general-purpose in-memory filesystem (`mount -t tmpfs`)

The container child process (`child_exec_inner` in `guest-init/src/container.rs`)
mounts tmpfs on `/tmp` inside the chroot. Without `CONFIG_TMPFS`, this mount returned
`EINVAL`, which aborted the entire `child_exec_inner` before pipe redirection or
execve happened. The error message went to serial (not captured pipes) because
stdout/stderr hadn't been redirected yet.

**Fix applied:** Added `CONFIG_TMPFS=y` to `guest-image/guest-kernel.config` in the
Pseudo filesystems section. Depends on `CONFIG_SHMEM` which was already enabled.

After rebuilding the kernel, the tmpfs mount succeeds and containers start without errors.

## Current Problem: No Output or Exit Events

Both containers start, I/O sessions connect, but nothing comes through.

### Test setup (test-compose.yaml)
- **web**: nginx:latest, entrypoint `/docker-entrypoint.sh nginx -g "daemon off;"`
- **app**: alpine:latest, command `/bin/sh` (no args)

### What happens
1. VMs boot, guest-init starts, vsock control channel connects
2. Network configured, containers added, containers started (pid 35 in each VM)
3. I/O sessions connect to vsock port 1025 successfully
4. `pod 'web' is running` and `pod 'app' is running` logged
5. **No container output is ever printed** (expected nginx startup messages)
6. **No exit events** (expected `/bin/sh` to exit immediately with stdin=/dev/null)
7. System sits idle indefinitely

### Data path (traced, looks correct)

**Guest side** (`guest-image/guest-init/`):
1. Container forked, pipes created for stdout/stderr (`container.rs:132-148`)
2. Child: chroot, mount /proc /sys /dev /tmp, redirect stdin to /dev/null,
   dup2 stdout/stderr to pipe write ends, execve (`container.rs:311-423`)
3. Parent: stores pipe read ends, sets non-blocking (`container.rs:186-196`)
4. Event loop polls pipe fds, reads data, forwards to IoSessionManager (`main.rs:291-301`)
5. IoSessionManager writes frames to vsock session stream (`io_session.rs:180-193`)
6. VsockStream `write_raw` flushes after every write (`vsock.rs:150-154`)

**Host side** (`distvirt/src/`):
1. Connects to vsock port 1025, performs handshake (`io_session.rs:31-66`)
2. Spawned tokio task loops on `session.next_event()` (`worker.rs:290-322`)
3. Sends `WorkerEvent::PodOutput` to mpsc channel
4. Compose event loop receives events, prints with `println!("{} | {}", pod_id, line)`
   (`orchestrate_compose.rs:149-151`)

### Leading theory: /dev/null not available inside container chroot

In `child_exec_inner`, after chroot + devtmpfs mount on /dev, the code opens
`/dev/null` for stdin redirection:

```rust
let null_fd = unsafe { libc::open(devnull.as_ptr(), libc::O_RDONLY) };
if null_fd >= 0 {
    unsafe { libc::dup2(null_fd, 0); ... }
}
```

If `/dev/null` doesn't exist (or open fails), `null_fd < 0` and **stdin is NOT
redirected**. The child inherits init's stdin (serial console), which has no input.
Then `/bin/sh` blocks reading from serial forever, explaining why it never exits.

devtmpfs SHOULD have /dev/null (created by `drivers/char/mem.c` at boot), but this
hasn't been verified inside the chroot context.

**This doesn't fully explain the nginx case** - nginx's entrypoint script should
produce stdout output regardless of stdin. Unless the script itself is failing
silently or the pipe forwarding has a subtle bug.

### Suggested next debugging steps

1. **Add debug logging to guest init** to confirm:
   - Whether `/dev/null` opens successfully in `child_exec_inner`
   - Whether pipe fds receive any data (log in `read_pipe` or `forward_pipe_data`)
   - Whether the I/O session `write_raw` is called and succeeds

2. **Test with explicit output**: Change the alpine command to something like
   `/bin/sh -c "echo HELLO && sleep 10"` to verify the pipe capture path works
   at all.

3. **Check devtmpfs contents**: Add a debug step in `child_exec_inner` after the
   devtmpfs mount to list /dev and confirm /dev/null exists.

## Other Issue: Duplicate MAC Addresses

All VMs get the same hardcoded MAC `06:00:AC:10:00:02` (`vmm/firecracker.rs:177`).
This causes `IPv6 duplicate address` warnings and will break inter-VM networking
on the fabric. Each VM should get a unique MAC derived from its assigned IP address.
