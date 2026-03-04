# E2E Fix Tech Debt

Issues identified while debugging the full-stack E2E WireGuard tunnel test.

High priority (correctness bugs):
  1. ~~#1 — entrypoint join(" ") — FIXED~~ — `convert_proto_container_spec` now splits `entrypoint[0]` as executable and `entrypoint[1..]` as args.
  2. ~~#7 — yamux deadlock in drain_container_pipes_final — FIXED~~ — Drain now runs concurrently with a yamux-driving `poll_fn` via `future::or`.

  Medium priority (robustness):
  3. #6 — pod→workload resolution race — Loses final output from short-lived containers. Annoying to debug when it happens.
  4. #8 — dead StreamLogs code — Quick win, just delete it. Reduces confusion.

  Low priority (minor/cosmetic):
  5. #5 — lazy log subscriber cleanup — Slow memory leak, only matters for long-running orchestrators.
  6. #3 — debug logging in wireguard — Already behind log::debug!, harmless unless noisy.
  7. #4 — capture_output hardcoded — Fine default, only matters when you have high-throughput stdout workloads.
  8. #2 — TUN TUNSETOFFLOAD — Works correctly today, just a minor perf optimization.

## 1. `merge_config` ignored override args when image has only CMD

**File**: `distvirt-worker/src/oci.rs` `merge_config()`

**Fixed**: OCI entrypoint/cmd resolution is now consolidated in a single `oci::merge_config()` function in `distvirt-worker/src/oci.rs`. The CLI and orchestrator pass through `Vec<String>` entrypoint/args without splitting — all resolution against image config happens in the worker. The proto was extended with `working_dir`, `user`, `hostname` fields so these pass through end-to-end. The old `ImageOverrides` intermediary struct was eliminated — `merge_config` takes `&ContainerConfig` directly.

## 2. Client TUN device doesn't call `TUNSETOFFLOAD`

**File**: `distvirt-cli/src/tun.rs`

The client-side TUN device is created without calling `TUNSETOFFLOAD`. By default, the kernel computes full checksums for packets sent through TUN (so this works today), but it means the kernel can't offload checksums to the TUN device, slightly reducing performance. The worker's gateway TUN (`distvirt-worker/src/gateway/tun.rs`) does call `TUNSETOFFLOAD` with `TUN_F_CSUM` and properly handles `IFF_VNET_HDR`. The client TUN should be reviewed for consistency.

## 3. Diagnostic logging left in wireguard adapter

**File**: `distvirt-worker/src/adapter/wireguard.rs`

TCP flag diagnostic logging was added to the ingress and egress paths during debugging. This should be reviewed — keep if useful at `debug`/`trace` level, or remove if too noisy.

## 4. `capture_output` hardcoded to `true` with no proto control

**Files**: `distvirt-orchestrator/src/grpc.rs` `convert_proto_container_spec()`, `distvirt-client-protocol/proto/.../client.proto`

`capture_output` is unconditionally set to `true` in the gRPC conversion. The proto `ContainerConfig` message has no field for this — it should be exposed so clients can opt in/out. Defaulting to `true` is fine for log streaming, but some workloads may want to disable it (e.g. high-throughput stdout producers where the capture overhead matters).

## 5. Log subscriber cleanup is lazy

**File**: `distvirt-orchestrator/src/shell.rs`

Log subscribers are only cleaned up when a `LogData` message is distributed and a `try_send` returns `Closed`. If a client disconnects while no log data is flowing, the `LogSubscriber` entry (with its dead `mpsc::Sender`) lingers in the `log_subscribers` vec indefinitely. Similarly, per-workload `log_buffers` entries are never removed when a namespace is deleted — they accumulate until the orchestrator restarts. Consider cleaning up buffers in the namespace deletion path and periodically pruning dead subscribers.

## 6. Log stream pod→workload resolution race

**File**: `distvirt-orchestrator/src/shell.rs`

When a worker opens a log stream, the shell's acceptor task sends `ShellMsg::ResolvePod` to look up the `workload_id` from `orchestrator.namespaces[ns].pods[pod_id]`. If the pod exits and is removed from the map before the `ResolvePod` message is processed, the resolution fails and the log stream is silently dropped. This is a benign race (the pod is gone anyway), but means the final output from a short-lived container may be lost. A more robust approach would carry `workload_id` in the worker protocol's `LogStreamHeader` directly, avoiding the round-trip lookup.

## 7. Guest-init `drain_container_pipes_final` blocks yamux driver

**File**: `guest-image/guest-init/src/main.rs`

**Fixed**: The drain call in the `Ready::Signal` handler is now wrapped with `future::or` alongside a `poll_fn` that drives `conn.poll_next_inbound()`. The yamux driver future never resolves (always `Pending`), so `future::or` completes when the drain finishes, but yamux gets polled on every wakeup to process window updates. Any inbound streams accepted during the drain are dropped — acceptable since we're in exit cleanup.

## 8. Dead code: `ClientCommand::StreamLogs` / `NamespaceInput::StreamLogs`

**Files**: `distvirt-orchestrator/src/types.rs`, `distvirt-orchestrator/src/orchestrator.rs`

The original plan was to route `StreamLogs` through the orchestrator state machine. Log streaming is now handled entirely in the shell layer (data plane), bypassing the SM. The existing `ClientCommand::StreamLogs` and `NamespaceInput::StreamLogs` variants are dead code and should be removed.
