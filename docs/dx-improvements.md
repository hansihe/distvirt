# DX Improvement Plan

Grouped by subsystem, roughly in dependency order.

## 1. Spec Application Model

Foundational changes to how specs are applied and managed.

### Orchestrator-side IP allocation
- **Current**: IP allocation is client-side using deterministic FNV-1a hashing (`client/src/spec/ip_alloc.rs`). Applying partial specs from different CLI invocations can produce collisions if the allocation context differs.
- **Goal**: Move allocation to the orchestrator so it has the full view of allocated IPs per namespace. Stable allocation regardless of which client or fragment is applying.
- **Scope**: Client spec resolution, orchestrator namespace management, client-protocol changes.
- **Prerequisite for**: reliable multi-client partial apply.

### Individual fragment apply / partial apply / labels
- **Current**: Fragments (`kind: WorkloadFragment`) exist and `PatchNamespace` RPC does upsert, but fragments are merged client-side. Labels infrastructure landed (see below), but CLI filtering not yet wired.
- **Goal**: Support `dv apply -l env=staging` to selectively apply subsets. Enable phased deployments.
- **Scope**: ~~Spec types (add labels to workloads/services)~~, CLI argument parsing, patch filtering logic.
- **Depends on**: orchestrator-side IP allocation for correctness with multi-client workflows.
- **Labels (done)**: `map<string, string> labels` added to `WorkloadSpec`, `ServiceSpec` (proto + internal types), and `WorkloadStatusReport`, `ServiceStatusReport`. YAML spec supports `labels:` on workloads, inline services, and top-level services. Inline services inherit parent workload labels with service-level overrides winning. Labels are stored on the orchestrator spec layer and surfaced in status reports. Remaining: CLI `-l` flag for apply/status filtering.

## 2. guest-init Process Model

Hardening the container setup path. Self-contained, can be done independently.

### musl target for guest-init
- **Current**: Compiled against glibc. Already has workarounds for glibc issues (raw syscalls for setuid/setgid because `nptl_setxid` breaks after clone3). `CLONE_INTO_CGROUP` constant manually defined since musl doesn't expose it.
- **Goal**: Switch to `x86_64-unknown-linux-musl`. Eliminates the class of glibc thread-sync issues. May also resolve exec-related problems.
- **Scope**: Build config, Cargo target, verify no dynamic linking dependencies.

### Reexec intermediate after fork
- **Current**: Direct clone3 -> child_exec with ~200 lines of setup in the forked child (`container.rs`). Inherits potentially stale state from the parent's async runtime.
- **Goal**: Fork, then immediately exec guest-init with `--container-setup` flag. Runs setup in a clean process image, avoids inherited file descriptors, signal handlers, thread-local storage.
- **Scope**: `container.rs`, new CLI mode for guest-init binary.
- **Complements**: musl target (both address process-state issues in forked child).

## 3. Observability Pipeline

Events generated at lower layers need to propagate up through worker -> orchestrator -> client event stream -> CLI. Building the propagation infrastructure once benefits all items here.

### Memory pressure / OOM event propagation
- **Current**: guest-init has sophisticated monitoring (PSI triggers at two levels, inotify on `memory.events` tracking oom/oom_kill/oom_group_kill, adaptive balloon deflation). But this data stays inside the VM. Only `GuestEvent::BalloonSet` reaches the host.
- **Goal**: Add `GuestEvent::MemoryPressure` / `GuestEvent::OomKill` events. Propagate through worker -> orchestrator -> client event stream. Surface in `dv status -w` and `dv events`.
- **Scope**: guest-init event emission (`memory/task.rs`), worker-protocol, orchestrator event routing, CLI display.

### Restart counter in `dv status` ✓
- **Done**: `restart_count` field on `WorkloadSm`, propagated through proto → gRPC → client model → CLI. Shown as `(N restarts)` in `dv status` when > 0. Resets on spec change or admin restart only (not on pod reaching Running or demand changes). Stateright model updated with normalization.
- **Remaining**: Event stream doesn't carry `restart_count` yet — watch mode has correct initial value but won't update live. Requires bundling the counter into the observability signal.

### Progress tracing on pod launch
- **Current**: Flat 120s `launch_timeout` in pod SM (`pod.rs`). No intermediate progress signals, no forward-progress detection.
- **Goal**: Worker reports intermediate status (image pulled, VM booting, init responding). Timer resets on progress. No false timeouts on slow but progressing launches.
- **Scope**: Worker-protocol (new progress message types), pod state machine, timer logic, CLI display.

### Logs dropped detection
- **Current**: Log bus uses `try_send` and silently drops on full channels. guest-init fill/drain tracks `bytes_dropped` internally but doesn't propagate.
- **Goal**: Counter in log bus subscriber that increments on `Full`. Periodic "X messages dropped" sentinels injected into stream. CLI renders visual indicator.
- **Scope**: `log_bus.rs` subscriber machinery, new `StreamLogsResponse` message type, CLI log rendering.

## 4. Logs Subsystem

All contained within `log_bus.rs` + CLI log rendering.

### Follow for new pods not working
- **Current**: `subscribe_by_workload()` in `log_bus.rs` registers standing subscriptions that should auto-attach to new topics. Mechanism exists (lines 143-166, 269-308) but reportedly broken.
- **Goal**: Fix subscription matching for newly created topics. Add test case for this scenario.
- **Scope**: `log_bus.rs` topic registration + standing subscription matching. Possibly a race condition.

### Logs formatting
- **Current**: Format is `[workload_name/pod_id/container_id] line`. Hard to scan across multiple workloads.
- **Goal**: Per-workload color coding (hash name to terminal color), or fixed-width left-margin gutter. Easier visual distinction.
- **Scope**: `format.rs` log line rendering.

## 5. CLI UX

Display and argument parsing improvements. Can happen anytime.

### `dv apply -w`
- **Current**: Apply and watch are separate commands. Watch TUI already exists on `status -w`.
- **Goal**: Add `-w` flag to `apply` that chains into `status -w` after apply completes. Optionally block until stable.
- **Scope**: `namespace.rs` apply command, reuse existing watch infrastructure.

### Service state in `status -w`
- **Current**: Watch mode subscribes to workload, pod, and endpoint events. Endpoint events carry `service_id` and `workload_id`. But not all service lifecycle transitions surface in the TUI.
- **Goal**: Render all service state changes in the watch display.
- **Scope**: `status_watch.rs` event handling and rendering.

### CLI entity format revamp
- **Current**: Inconsistent — some commands take `namespace_id`, others take `namespace/workload` via `parse_target()`.
- **Goal**: Uniform `namespace/type/name` or similar format across all commands.
- **Scope**: `namespace.rs` argument parsing, all CLI command definitions.

### Hostname resolution in Python client
- **Current**: Tonic's `Channel::from_shared()` handles DNS transparently. Auto-prepends `http://` for bare `host:port`. May need better error messages for edge cases.
- **Goal**: Document supported formats, improve error messages for malformed connection strings.
- **Scope**: `connection.rs` error handling, Python SDK docs.
