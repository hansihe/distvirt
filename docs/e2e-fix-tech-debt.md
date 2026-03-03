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

## 6. `capture_output` hardcoded to `true` with no proto control

**Files**: `distvirt-orchestrator/src/grpc.rs` `convert_proto_container_spec()`, `distvirt-client-protocol/proto/.../client.proto`

`capture_output` is unconditionally set to `true` in the gRPC conversion. The proto `ContainerConfig` message has no field for this — it should be exposed so clients can opt in/out. Defaulting to `true` is fine for log streaming, but some workloads may want to disable it (e.g. high-throughput stdout producers where the capture overhead matters).

## 7. Log subscriber cleanup is lazy

**File**: `distvirt-orchestrator/src/shell.rs`

Log subscribers are only cleaned up when a `LogData` message is distributed and a `try_send` returns `Closed`. If a client disconnects while no log data is flowing, the `LogSubscriber` entry (with its dead `mpsc::Sender`) lingers in the `log_subscribers` vec indefinitely. Similarly, per-workload `log_buffers` entries are never removed when a namespace is deleted — they accumulate until the orchestrator restarts. Consider cleaning up buffers in the namespace deletion path and periodically pruning dead subscribers.

## 8. Log stream pod→workload resolution race

**File**: `distvirt-orchestrator/src/shell.rs`

When a worker opens a log stream, the shell's acceptor task sends `ShellMsg::ResolvePod` to look up the `workload_id` from `orchestrator.namespaces[ns].pods[pod_id]`. If the pod exits and is removed from the map before the `ResolvePod` message is processed, the resolution fails and the log stream is silently dropped. This is a benign race (the pod is gone anyway), but means the final output from a short-lived container may be lost. A more robust approach would carry `workload_id` in the worker protocol's `LogStreamHeader` directly, avoiding the round-trip lookup.

## 9. Guest-init `drain_container_pipes_final` blocks yamux driver

**File**: `guest-image/guest-init/src/main.rs`

`drain_container_pipes_final` is called from the `Ready::Signal` handler in the main event loop. It writes remaining pipe data to the yamux output stream via `write_all().await`. If the yamux stream's send buffer is full, this blocks, but the yamux connection isn't being driven (no `poll_next_inbound` call during this handler), so window updates from the peer can't be processed. This creates a potential deadlock for containers that produce large output bursts right before exiting. In practice the buffer is usually large enough, but the architecture is fragile.

## 10. Dead code: `ClientCommand::StreamLogs` / `NamespaceInput::StreamLogs`

**Files**: `distvirt-orchestrator/src/types.rs`, `distvirt-orchestrator/src/orchestrator.rs`

The original plan was to route `StreamLogs` through the orchestrator state machine. Log streaming is now handled entirely in the shell layer (data plane), bypassing the SM. The existing `ClientCommand::StreamLogs` and `NamespaceInput::StreamLogs` variants are dead code and should be removed.
