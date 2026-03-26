# Firecracker Usage in distvirt-worker

## VM Process Lifecycle

- Firecracker spawned as a child process, one per VM
- Configured via REST-like HTTP/1.1 API over Unix domain socket (`firecracker.sock`)
- One fresh connection per API request
- All VM artifacts live in a per-VM tmpdir, cleaned up on drop

**Lifecycle:** Launch → Configure (API calls) → Start → Running → Suspend/Shutdown → Kill

## API Endpoints Used

| Endpoint | Method | Purpose |
|----------|--------|---------|
| `/boot-source` | PUT | Kernel image path + boot args |
| `/drives/{id}` | PUT | Block devices (rootfs, container, config, volumes) |
| `/machine-config` | PUT | vCPU count, memory size |
| `/balloon` | PUT | Virtio-balloon setup (optional) |
| `/balloon` | PATCH | Dynamic balloon resize |
| `/vsock` | PUT | Vsock device (CID 3, UDS path) |
| `/network-interfaces/eth0` | PUT | TAP-backed network interface + guest MAC |
| `/actions` | PUT | `InstanceStart` |
| `/vm` | PATCH | Pause vCPUs (`{"state": "Paused"}`) |
| `/snapshot/create` | PUT | Snapshot device state + memory to files |
| `/snapshot/load` | PUT | Restore from snapshot, optionally resume + override network |

## Devices

### Block Drives (virtio-blk)
- `/dev/vda` — **Rootfs**: ext4, read-write, fresh copy per VM
- `/dev/vdb` — **Container image**: ext4, read-write
- `/dev/vdc` — **Config drive** (optional): length-prefixed JSON payload, read-only. Used for pre-vsock commands
- `/dev/vdd`+ — **Volumes**: EmptyDir (sparse ext4) or ConfigData (ext4 with embedded files), read-only or read-write

All images are writable copies in tmpdir (Firecracker requires writable file handles).

### Network (virtio-net)
- Single TAP device created via `ioctl` on `/dev/net/tun` (`IFF_TAP | IFF_NO_PI`)
- Attached as `eth0` in guest with assigned MAC
- Host-side: `AF_PACKET` socket bound to TAP for L2 frame injection/capture (used by userspace network fabric)
- TAP device destroyed on drop

### Vsock (virtio-vsock)
- CID 3, UDS at `vsock.sock` in tmpdir
- Host connects to guest port 1024 via Firecracker's UDS protocol (`CONNECT <port>\n` → `OK <id>\n`)
- Primary guest-host communication channel

### Virtio-Balloon (optional)
- Configured at launch with initial size + `deflate_on_oom`
- Dynamically resized via `PATCH /balloon` for memory overcommit
- Guest reports memory pressure/OOM events back over vsock

### Serial Console
- `console=ttyS0` in boot args
- Firecracker stdout captured, line-read for debug logging
- Optional (can be disabled)

## Guest Communication

### Transport Stack
```
vsock UDS → Firecracker vsock → guest port 1024 → Yamux multiplexer
```

### Yamux Streams
- **Control stream**: Request-response JSON messages (length-prefixed)
- **Event stream**: Async guest→host events (container exits, OOM, balloon, memory pressure)
- **Output streams**: Per-container stdout/stderr capture

### Control Messages (Host→Guest)
AddContainer, StartContainer, ConfigureNetwork, MountVolume, SignalContainer, SetClock, PrepareSuspend, Shutdown

### Pre-Vsock Commands
Optional commands embedded in config drive (`/dev/vdc`), executed by guest-init before vsock is established. Responses returned in the `Ready` message.

## Snapshot/Restore

### Snapshot
1. Pause vCPUs via `/vm` PATCH
2. Create snapshot via `/snapshot/create` → `snapshot.bin` (device state) + `mem.bin` (memory)
3. Copy artifacts to snapshot directory: metadata.json, snapshot.bin, mem.bin, container.ext4, volume images

### Restore
1. Fresh tmpdir, copy rootfs from original source (fresh), container+volumes from snapshot
2. Spawn new Firecracker process
3. Load via `/snapshot/load` with `resume_vm: true`
4. Network device override required (new TAP device name)

## Boot Args
```
console=ttyS0 reboot=k panic=-1 pci=off init=/sbin/init
distvirt.balloon_mib=<N>          # optional
distvirt.config_device=/dev/vdc   # optional
```

## Abstraction Layer

There is a `Vmm` trait in `vmm/mod.rs` with Firecracker as one implementation. Key types:
- `VmConfig` — launch parameters (kernel, rootfs, container image, vcpus, mem, network, volumes, etc.)
- `SnapshotArtifacts` — paths to snapshot files + metadata
- `SnapshotMetadata` — serialized config needed for restore
