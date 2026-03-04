# Unwrap/Panic Cleanup Tracker

Incremental cleanup of unwrap/panic usage in non-test code. Goal: graceful error
handling and intentional failure semantics rather than scattered panics.

## Category 1: Mutex/RwLock Poisoning (architectural — deferred)

~30+ occurrences in `fabric/forwarding.rs`, `fabric/mod.rs`, `fabric/tunnel.rs`.

Poisoning is intentional — it signals corrupted state. The fix isn't to suppress
it, but to make lock call sites return `Result` so callers can tear down and
reinitialize the affected subsystem. This requires designing recovery boundaries
in the fabric and is deferred until we have a clear subsystem recovery strategy.

## Category 2: External ID Lookups → return Result

`namespace/commands.rs:416` — `launch_pod` unwraps `workloads.get_mut(workload_id)`
`namespace/commands.rs:505` — `resume_pod` unwraps `workloads.get_mut(workload_id)`

These take `workload_id` as a parameter from callers. A stale or invalid ID
should return an error, not panic.

- [ ] `launch_pod`: return error on missing workload_id
- [ ] `resume_pod`: return error on missing workload_id

## Category 3: PathBuf-to-str in Drop (process abort risk)

`image_provider/containerd.rs:38` — `.unwrap()` on `to_str()` inside `Drop` impl.
Panicking in `Drop` can abort the entire process (double panic).

- [ ] Use `if let Some(s) = path.to_str()` and log warning on failure

## Category 4: Other PathBuf-to-str Conversions

`vmm/firecracker.rs:93` — kernel_path.to_str().unwrap()
`image_provider/containerd.rs:340` — mount dir to_str().unwrap()
`cli/commands/legacy.rs:103,160` — containerd socket path to_str().unwrap()

- [ ] Return proper errors via `.to_str().ok_or_else(...)` with `?`

## Category 5: Logical Guard Unwraps → structural safety

Cases where `.unwrap()` is protected by an earlier `if .is_none() { return }`
guard. Logically safe but fragile to refactoring.

`fabric/service.rs:196,210` — `backend_ip.unwrap()` after is_none check
`fabric/service.rs:345` — same pattern in flush_ready_services
`fabric/gateway/dns.rs:75,130` — unwrap after is_none guard
`worker/vsock_client.rs:75` — control_opt.unwrap() after poll_fn guarantee
`worker-protocol/connection.rs:335` — same pattern

- [ ] Refactor to `let Some(x) = ... else { return }` or `if let` patterns

## Category 6: Internal Map Invariant Lookups → descriptive expects

HashMap lookups where the key is known to exist by construction. These are
effectively assertions and are acceptable, but should have descriptive messages.

`namespace/reconciliation.rs:38,50,53,55,62` — spec/service/workload lookups
`namespace/events.rs:245` — workload lookup after state match
`fabric/route.rs:120` — re-borrow after confirmed existence
`compose/deployment.rs:111` — topo_sort in_degree lookup

- [ ] Convert `.unwrap()` to `.expect("invariant: <description>")`

## General: Log context at error sites before propagating

When converting unwraps to `Result` propagation, consider whether the call site
has context that would be lost by the time the error reaches the top. Propagated
errors often end up as a generic message far from where the problem occurred.

If a call site has useful context (relevant IDs, state, what operation was
attempted), log it at warn/error level *before* propagating. This way even if
the error gets wrapped or flattened upstream, the debug info is in the logs.

Don't over-log — if the error message itself carries enough info, just propagate.
The goal is to capture context that would otherwise be lost.

## Category 7: No action needed

These are fine as-is:

- **Hard-coded literal parsing** (`worker/mod.rs`, `fabric/tunnel.rs`): infallible
- **Crypto/protocol expects** (`fabric/tunnel.rs`, `tunnel_manager.rs`): already have descriptive messages
- **unreachable!() in match arms** (`shell.rs`, `namespace/output.rs`, etc.): document routing assumptions
- **API contract assertions** (`task_handle.rs`): standard Rust patterns
