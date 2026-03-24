# DX Improvement Plan

Grouped by subsystem, roughly in dependency order.

## 1. Spec Application Model

Foundational changes to how specs are applied and managed.

### Orchestrator-side IP allocation ✓
- **Done**: IP allocation moved from client to orchestrator. `NamespaceIpAllocator` in `orchestrator/src/core/namespace/ip_alloc.rs` owns per-namespace state. Subnet split into auto zone (bottom half, sequential assignment) and manual zone (top half, user-specified explicit IPs), with WireGuard reserve at the top. Allocations are sticky (survive re-apply). Client-side `ip_alloc.rs`, `resolve.rs` (`${...}` IP expressions) deleted. Responses now return full `IpAllocResult` snapshot with auto/manual indicator per resource. Dedicated `apply_full_spec`/`apply_patch` methods on `NamespaceUnit` with explicit `Result<(NamespaceOutput, IpAllocResult), ClientError>` return type.
- **Prerequisite for**: reliable multi-client partial apply.

### Individual fragment apply / partial apply / labels ✓
- **Done**: Full label selector engine and CLI `-l` flag on `dv spec apply`.
- **Selector engine** (`client-protocol/src/selector.rs`): Kubernetes-style label selector syntax — `=`, `!=`, `in (...)`, `notin (...)`, existence (`key`), non-existence (`!key`). Comma-separated predicates with AND semantics. Closure-based `matches()` decouples the engine from storage representation; lives in `client-protocol` so it's usable from both client and orchestrator. Re-exported via `distvirt-client::selector`.
- **CLI**: `dv spec apply -l "env=staging"` parses the selector, filters the spec client-side (workloads by label match, services included if own labels match OR parent workload matched), then sends only the filtered subset via `PatchNamespace` (additive upsert, no removals). Reports matched counts before patching.
- **Labels (done)**: `map<string, string> labels` added to `WorkloadSpec`, `ServiceSpec` (proto + internal types), and `WorkloadStatusReport`, `ServiceStatusReport`. YAML spec supports `labels:` on workloads, inline services, and top-level services. Inline services inherit parent workload labels with service-level overrides winning. Labels are stored on the orchestrator spec layer and surfaced in status reports.
- **Remaining**: `-l` flag for `dv status` filtering. Orchestrator-side selector evaluation (engine is already in `client-protocol` for when this is needed).

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

### Logs dropped detection ✓
- **Done**: End-to-end drop detection via monotonic per-container sequence numbers. Guest-init fill task assigns a `seq: u64` to each output chunk (shared across stdout/stderr, increments even for dropped chunks so gaps are visible). Seq flows through: guest-init frame format (`[stream_id][seq][length][payload]`) → worker `IoSession` parses seq → worker re-frames with `send_log_frame` (new `[seq][length][payload]` codec in worker-protocol) → orchestrator `spawn_log_ingest` does frame-aware reads (replaces raw 8KB `stream.read`) → `LogChunk.seq` in LogBus → proto `LogChunk.seq` → CLI. CLI tracks expected next seq per (pod_id, container_id) and prints `*** N log chunk(s) dropped ***` on gaps. Detects drops at any stage: guest-init buffer overflow during final drain, LogBus subscriber backpressure, or network-level losses.

## 4. Logs Subsystem

All contained within `log_bus.rs` + CLI log rendering.

### Follow for new pods not working ✓
- **Done**: Two-part race condition fixed. (1) `spawn_log_ingest` resolved `workload_name` once on stream open; if id registry wasn't populated yet (`sync_dynamic_ids` runs after reconcile), name was `None` for the stream's lifetime. Fix: re-resolve on each chunk until successful. (2) `LogBusHandle::publish` only matched standing subscriptions on new topic creation; when a topic was created with `workload_name: None` and later backfilled, standing subscriptions were never attached. Fix: also match standing subscriptions on backfill. Test added for the late-backfill scenario.

### Logs formatting
- **Current**: Format is `[workload_name/pod_id/container_id] line`. Hard to scan across multiple workloads.
- **Goal**: Per-workload color coding (hash name to terminal color), or fixed-width left-margin gutter. Easier visual distinction.
- **Scope**: `format.rs` log line rendering.
- **Progress**: `[...]` prefix now rendered with dim styling (crossterm `Stylize`) so metadata visually recedes from log content. Further improvements (per-workload colors, fixed-width gutter) possible later.

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
