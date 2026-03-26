# Firecracker to Cloud Hypervisor Migration Guide

## Feature Mapping

### API & Process Lifecycle

| Firecracker | Cloud Hypervisor | Notes |
|-------------|-----------------|-------|
| HTTP/1.1 REST over UDS | HTTP REST over UDS (OpenAPI 3.0) | Same transport concept, different endpoint naming |
| One connection per request | Same pattern supported | CH also offers D-Bus API as alternative |
| Configure then start (sequential PUT calls) | `vm.create` (single JSON config) then `vm.boot` | CH takes full config as one JSON blob rather than per-resource PUT calls |
| `PUT /actions` `InstanceStart` | `PUT /vm.boot` | |
| `PATCH /vm` `{"state":"Paused"}` | `PUT /vm.pause` | Dedicated endpoint instead of PATCH |
| No live resize | `PUT /vm.resize` for CPU/memory hotplug | New capability |
| Process-per-VM | Process-per-VM | Same model |

**Key API difference:** Firecracker configures resources incrementally via individual PUT calls (`/boot-source`, `/drives/{id}`, `/machine-config`, etc.) before starting. Cloud Hypervisor takes a single `VmConfig` JSON document at `/vm.create` that describes everything at once. The `Vmm` trait implementation will need to build this config struct rather than making sequential HTTP calls.

### Boot Configuration

| Firecracker | Cloud Hypervisor | Notes |
|-------------|-----------------|-------|
| `PUT /boot-source` with `kernel_image_path`, `boot_args` | `payload.kernel`, `payload.cmdline` in VmConfig | |
| Supports vmlinux (uncompressed ELF) | Supports vmlinux (PVH), bzImage, and UEFI firmware boot | More boot options |
| `console=ttyS0` | `console=ttyS0` (serial) or `console=hvc0` (virtio-console) | See console section |
| `pci=off` in boot args | **Remove this** - CH uses PCI | Critical change |
| `reboot=k panic=-1` | Still applicable | |

### Block Devices (virtio-blk)

| Firecracker | Cloud Hypervisor | Notes |
|-------------|-----------------|-------|
| `PUT /drives/{id}` per drive | `disks` array in VmConfig | |
| `drive_id`, `path_on_host`, `is_root_device`, `is_read_only` | `path`, `readonly`, `id` | No `is_root_device`; root is specified via `root=/dev/vdaX` in cmdline |
| Always raw images | Supports Raw, Qcow2, FixedVhd, Vhdx | |
| Device naming: `/dev/vda`, `/dev/vdb`, etc. | Same: `/dev/vda`, `/dev/vdb`, etc. | PCI-based but same guest device names |
| Rate limiting per drive | Rate limiting per drive + shared rate limit groups | |

Device ordering in the guest should be stable as long as PCI slot assignment is consistent. Verify that the drive order in the `disks` array produces the expected `/dev/vdX` mapping.

### Network (virtio-net)

| Firecracker | Cloud Hypervisor | Notes |
|-------------|-----------------|-------|
| `PUT /network-interfaces/{id}` | `net` array in VmConfig | |
| TAP device name (`host_dev_name`) | `tap` field (named TAP) or auto-created | |
| `guest_mac` | `mac` | |
| Single queue | Multi-queue (default 2 queues) | Set `num_queues: 1` if single queue needed |
| No offload control | `offload_tso`, `offload_ufo`, `offload_csum` (all default true) | May want to disable offloads if using raw packet injection on host side |

**Important:** distvirt uses `AF_PACKET` socket bound to the TAP for L2 frame injection/capture (userspace network fabric). Cloud Hypervisor enables TCP/UDP offloads by default on the TAP device. If the host-side code is doing raw L2 injection, TSO/UFO offloads may cause issues with oversized frames. Consider setting `offload_tso: false`, `offload_ufo: false`, `offload_csum: false` for the network device, or ensure the host-side packet handling is offload-aware.

### Vsock (virtio-vsock)

| Firecracker | Cloud Hypervisor | Notes |
|-------------|-----------------|-------|
| `PUT /vsock` with `guest_cid`, `uds_path` | `vsock` in VmConfig with `cid`, `socket` | |
| CID 3 hardcoded | CID configurable (minimum 3) | |
| UDS protocol: `CONNECT <port>\n` -> `OK <id>\n` | Same UDS-based vsock proxy | CH's vsock is a copy of Firecracker's implementation |
| Vsock connections close on snapshot/restore | Same behavior expected | |

Good news: Cloud Hypervisor's vsock implementation is directly derived from Firecracker's. The host-side UDS protocol (`CONNECT <port>\n` handshake) should be identical. The Yamux multiplexer layer on top should work without changes.

### Virtio-Balloon

| Firecracker | Cloud Hypervisor | Notes |
|-------------|-----------------|-------|
| `PUT /balloon` with `amount_mib`, `deflate_on_oom` | `balloon` in VmConfig with `size` (bytes), `deflate_on_oom` | Size is in **bytes** not MiB |
| `PATCH /balloon` for resize | `PUT /vm.resize` with `balloon_size` | Different endpoint for dynamic resize |
| `stats_polling_interval_s` | `free_page_reporting` option available | Different stats mechanism |

### Serial Console

| Firecracker | Cloud Hypervisor | Notes |
|-------------|-----------------|-------|
| 8250 UART, `console=ttyS0` | 16550A UART (x86_64) with `--serial tty`, `console=ttyS0` | |
| stdout capture, line-read for logging | `--serial file=<path>` or `--serial tty` | Can also redirect to file directly |
| Enabled by default | **Disabled by default** - must explicitly enable with `--serial` | |

Cloud Hypervisor defaults to `virtio-console` (`console=hvc0`) instead of legacy serial. For compatibility with existing boot args and log capture, explicitly enable serial (`--serial tty --console off` or configure via API).

### CPU & Memory

| Firecracker | Cloud Hypervisor | Notes |
|-------------|-----------------|-------|
| `PUT /machine-config` with `vcpu_count`, `mem_size_mib` | `cpus.boot_vcpus`, `memory.size` in VmConfig | Memory size is a string like `"1G"` or `"512M"` in CLI, integer bytes in API |
| Static vCPU count | Supports CPU hotplug (`boot_vcpus` < `max_vcpus`) | New capability |
| Static memory (with balloon) | Supports memory hotplug (virtio-mem, ACPI) | New capability |

### Snapshot/Restore

| Firecracker | Cloud Hypervisor | Notes |
|-------------|-----------------|-------|
| `PATCH /vm` pause, `PUT /snapshot/create` | `PUT /vm.pause`, `PUT /vm.snapshot` | |
| `snapshot.bin` (state) + `mem.bin` (memory) | `state.json` + `memory-ranges` + `config.json` | Different file format/naming |
| `PUT /snapshot/load` on fresh process | `--restore source_url=file://...` or `PUT /vm.restore` | |
| Network override on restore (`tap` field in load config) | `net_fds` on restore for FD passing | Different mechanism for network re-attachment |
| `resume_vm: true` in load request | `resume=true` in restore config, or separate `PUT /vm.resume` | |
| Eager memory restore only | `copy` (eager) or `ondemand` (userfaultfd lazy) | New capability |
| Disk images managed by user, not in snapshot | Same - disk images managed by user | |

**Network on restore:** Firecracker allows overriding the TAP device name directly in the snapshot load request. Cloud Hypervisor's approach for restore with new network devices is via FD passing (`net_fds` parameter). This means the restore code will need to:
1. Create the TAP device
2. Get its file descriptor
3. Pass the FD to the restore call

Alternatively, if using named TAP devices and re-creating them with the same name before restore, this may work without FD passing. This needs testing.

**Snapshot compatibility:** Cloud Hypervisor snapshots are not cross-version compatible. You'll need to ensure snapshot/restore pairs use the same CH version.

## Migration Challenges

### 1. Virtio Transport: MMIO to PCI (Major)

This is the single biggest architectural difference. Firecracker uses **virtio-MMIO**; Cloud Hypervisor uses **virtio-PCI** exclusively.

**Impact:**
- Guest kernel must have PCI support enabled
- Boot args must **not** contain `pci=off`
- Device discovery changes from MMIO device tree to PCI enumeration
- Device naming in guest should remain the same (`/dev/vda`, `eth0`, etc.) but the underlying transport is different
- The `distvirt.config_device=/dev/vdc` boot arg mechanism should still work since it references the block device name, not the transport

### 2. API Restructuring (Medium)

The Firecracker `Vmm` trait implementation makes sequential PUT calls to configure individual resources. The Cloud Hypervisor implementation needs to:
- Build a complete `VmConfig` JSON document
- Send it in a single `vm.create` call
- Then call `vm.boot`

This is a structural change to the VMM abstraction layer but not conceptually difficult.

### 3. Network Device Restore (Medium)

Firecracker's snapshot restore accepts a `network_overrides` field to specify the new TAP device. Cloud Hypervisor uses FD passing for this. The restore codepath needs reworking to:
- Create the TAP device and obtain its FD
- Pass the FD via the `net_fds` parameter in the restore call

The existing TAP creation code (ioctl on `/dev/net/tun`) can be reused; the change is in how the FD is handed to the hypervisor.

### 4. Balloon Size Units (Low)

Firecracker uses MiB for balloon size. Cloud Hypervisor uses bytes. Conversion needed in the balloon setup and dynamic resize paths.

### 5. Console Configuration (Low)

Cloud Hypervisor defaults to virtio-console (`hvc0`). To maintain compatibility with existing `console=ttyS0` boot args and the serial log capture mechanism, explicitly configure `--serial tty --console off` (or equivalent API config). Alternatively, switch to virtio-console and update boot args to `console=hvc0`.

### 6. Snapshot File Format (Low-Medium)

Snapshot artifacts differ:
- Firecracker: `snapshot.bin`, `mem.bin`, metadata JSON (your own)
- Cloud Hypervisor: `state.json`, `memory-ranges`, `config.json`

The `SnapshotArtifacts` and `SnapshotMetadata` types need updating. If you have existing Firecracker snapshots in production, they cannot be migrated to Cloud Hypervisor - a fresh boot is required.

### 7. Config Drive / Pre-Vsock Commands (Low)

The config drive mechanism (`/dev/vdc` with length-prefixed JSON) is a guest-init feature, not a hypervisor feature. It should work identically since it's just another virtio-blk device. Just ensure the drive ordering in the `disks` array places it at the right index.

### 8. Live Migration (New Capability)

Cloud Hypervisor supports live migration (local and remote), which Firecracker does not. This requires `shared=on` or hugepages for memory. If you plan to use this feature, memory configuration needs `shared=on`:

```json
"memory": {"size": "1G", "shared": true}
```

### 9. Boot Time Regression

Firecracker is optimized for fast boot (~125ms to init). Cloud Hypervisor is also fast but uses PCI device enumeration which adds some overhead compared to MMIO. Expect slightly longer boot times. The PCI bus scan in the guest kernel adds a few milliseconds. Benchmark to confirm acceptability.

## Guest Kernel Configuration Changes

Yes, the guest kernel config will need changes. Here's what needs to change:

### Must Enable

| Config Option | Reason |
|--------------|--------|
| `CONFIG_PCI=y` | Cloud Hypervisor uses virtio-PCI transport |
| `CONFIG_PCI_MSI=y` | MSI/MSI-X interrupt delivery for virtio-PCI |
| `CONFIG_VIRTIO_PCI=y` | Virtio PCI transport driver |
| `CONFIG_PCI_HOST_GENERIC=y` | Generic PCI host controller |
| `CONFIG_PCI_HOST_COMMON=y` | Common PCI host code |
| `CONFIG_PCIEPORTBUS=y` | PCIe port bus support |

### Must Remove / Can Remove

| Config Option | Reason |
|--------------|--------|
| `CONFIG_VIRTIO_MMIO=y` | No longer needed (CH doesn't use MMIO) |
| `CONFIG_VIRTIO_MMIO_CMDLINE_DEVICES=y` | No longer needed |

### Keep As-Is

| Config Option | Reason |
|--------------|--------|
| `CONFIG_VIRTIO_BLK=y` | Still needed for block devices |
| `CONFIG_VIRTIO_NET=y` | Still needed for networking |
| `CONFIG_VIRTIO_VSOCKETS=y` | Still needed for vsock |
| `CONFIG_VIRTIO_BALLOON=y` | Still needed for balloon |
| `CONFIG_SERIAL_8250=y` / `CONFIG_SERIAL_8250_CONSOLE=y` | Still needed if using serial console |
| `CONFIG_KVM_GUEST=y` | Still needed for KVM clock (x86_64) |

### Recommended Additions

| Config Option | Reason |
|--------------|--------|
| `CONFIG_ACPI=y` | CH uses ACPI for device discovery and power management |
| `CONFIG_HW_RANDOM_VIRTIO=y` | CH always enables virtio-rng |
| `CONFIG_VIRTIO_CONSOLE=y` | If you want to use virtio-console in the future |

### Boot Args Changes

```diff
- console=ttyS0 reboot=k panic=-1 pci=off init=/sbin/init
+ console=ttyS0 reboot=k panic=-1 init=/sbin/init
```

The `pci=off` parameter **must be removed**. All other existing boot args should remain compatible.

## Summary of Risk Assessment

| Area | Risk | Effort |
|------|------|--------|
| Kernel reconfig (MMIO->PCI) | Low (well-understood change) | Low |
| API restructuring | Low (mechanical) | Medium |
| Vsock compatibility | Very Low (same implementation) | Low |
| Block device mapping | Low (verify ordering) | Low |
| Network TAP + offloads | Medium (test with packet fabric) | Medium |
| Snapshot/restore rework | Medium (different mechanism) | Medium |
| Boot time validation | Low (benchmark needed) | Low |
| Console setup | Low (config change) | Low |
