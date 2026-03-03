# E2E Fix Tech Debt

Issues identified while debugging the full-stack E2E WireGuard tunnel test.

## 1. `merge_config` ignored override args when image has only CMD

**File**: `distvirt-worker/src/managed_vm.rs` `merge_config()`

**Fixed**: Added a branch so that when no entrypoint is set (neither override nor image) but override args are provided (compose `command:`), those args are used as the full command line.

**Remaining concern**: The OCI entrypoint/cmd resolution logic is split across three places with slightly different semantics:
- `distvirt-cli/src/commands/namespace.rs` `deployment_to_spec()` — maps compose fields to proto
- `distvirt-orchestrator/src/grpc.rs` `convert_proto_container_spec()` — converts proto to worker protocol
- `distvirt-worker/src/managed_vm.rs` `merge_config()` — merges overrides with image config

The orchestrator's `convert_proto_container_spec` joins `repeated string entrypoint` with spaces (`config.entrypoint.join(" ")`), which loses the boundary between arguments. This should be consolidated into a single well-tested resolution function with clear Docker/OCI semantics.

## 2. `guest-init` uses `execve` without PATH resolution

**File**: `guest-image/guest-init/src/container.rs` `child_exec_inner()`

**Partially fixed**: Added `resolve_in_path()` to search PATH from the env list when the entrypoint is a bare name (no `/`). However, this fix is in the guest-init source which requires rebuilding the guest image to take effect. The current workaround is using absolute paths in compose files.

**Action**: Rebuild guest image and verify the PATH resolution works. Consider whether `execvp`-like behavior (with the custom envp) would be more robust.

## 3. Client TUN device doesn't call `TUNSETOFFLOAD`

**File**: `distvirt-cli/src/tun.rs`

The client-side TUN device is created without calling `TUNSETOFFLOAD`. By default, the kernel computes full checksums for packets sent through TUN (so this works today), but it means the kernel can't offload checksums to the TUN device, slightly reducing performance. The worker's gateway TUN (`distvirt-worker/src/gateway/tun.rs`) does call `TUNSETOFFLOAD` with `TUN_F_CSUM` and properly handles `IFF_VNET_HDR`. The client TUN should be reviewed for consistency.

## 4. Silent container exec failures

**File**: `guest-image/guest-init/src/container.rs`

When `execve` fails in the child process, the error is written to stderr (which goes to the capture pipe) and the child exits with code 127. However:
- The parent reports `ContainerStarted` before knowing if exec succeeded (inherent to fork+exec)
- The worker reports `PodRunning` before the container has been confirmed alive
- Container exit may not be reaped promptly, so the failure can be invisible for seconds

Consider adding a mechanism to detect early exec failures (e.g. a close-on-exec pipe where the parent waits briefly for the write end to close, confirming exec succeeded).

## 5. Diagnostic logging left in wireguard adapter

**File**: `distvirt-worker/src/adapter/wireguard.rs`

TCP flag diagnostic logging was added to the ingress and egress paths during debugging. This should be reviewed — keep if useful at `debug`/`trace` level, or remove if too noisy.
