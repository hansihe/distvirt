---
title: "E2E Fix Tech Debt"
---

Issues identified while debugging the full-stack E2E WireGuard tunnel test.

## Pending

### Medium priority (robustness)

**#6 — pod-to-workload resolution race** — When a worker opens a log stream, `ShellMsg::ResolvePod` looks up the `workload_id` from the namespace pod map. If the pod exits and is removed before the message is processed, the log stream is silently dropped and final output from short-lived containers is lost. Fix: carry `workload_id` in the worker protocol's `LogStreamHeader` directly.
- `distvirt-orchestrator/src/shell/mod.rs`

**#8 — dead `StreamLogs` code** — `ClientCommand::StreamLogs` and `NamespaceInput::StreamLogs` are dead code. Log streaming is handled entirely in the shell layer now. Quick cleanup.
- `distvirt-orchestrator/src/types/client.rs`
- `distvirt-orchestrator/src/types/namespace_io.rs`

### Low priority (minor/cosmetic)

**#5 — lazy log subscriber cleanup** — Log subscribers are only cleaned up when a `try_send` returns `Closed`. Dead `LogSubscriber` entries linger if no log data flows. Per-workload `log_buffers` entries are never removed on namespace deletion.
- `distvirt-orchestrator/src/shell/subscriptions.rs`

**#3 — debug logging in wireguard** — TCP flag diagnostic logging in ingress/egress paths is at `log::debug!` level. Should be demoted to `trace!` or removed to reduce noise when debug logging is enabled.
- `distvirt-worker/src/adapter/wireguard.rs`

**#4 — `capture_output` hardcoded** — Still unconditionally set to `true` in gRPC conversion with no proto field to control it. Fine as a default, only matters for high-throughput stdout workloads.
- `distvirt-orchestrator/src/grpc/conversions.rs`

**#2 — TUN `TUNSETOFFLOAD`** — Client-side TUN device does not call `TUNSETOFFLOAD`. Works correctly (kernel computes full checksums), but prevents checksum offload. Worker gateway TUN already does this properly.

## Resolved

- **#1 — command `join(" ")`** — OCI Entrypoint/Cmd resolution consolidated in `merge_config()`. CLI and orchestrator pass `Vec<String>` without splitting. Proto extended with `working_dir`, `user`, `hostname`. Old `ImageOverrides` eliminated.
- **#7 — yamux deadlock in `drain_container_pipes_final`** — Drain now runs concurrently with a yamux-driving `poll_fn` via `future::or`.
