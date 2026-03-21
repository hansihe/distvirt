# distvirt Python SDK

Async Python SDK for the distvirt orchestrator. Designed for deployment sequencing — bringing up environments in stages, running migrations, waiting for readiness, and orchestrating multi-step workflows that the declarative YAML spec can't express alone.

## Why

The YAML spec format describes steady-state ("what should be running"), but real deployments need sequencing:

1. Bring up the database
2. Run migrations, wait for success
3. Deploy the application
4. Wait for it to be healthy

This SDK makes that a Python async program instead of a shell script.

## Usage

```python
import distvirt

async def deploy_staging():
    async with distvirt.connect("orchestrator:9090", token="...") as dv:
        # Phase 1: database
        ns = await dv.apply("staging", file="db-only.yaml")
        await ns.workload("db").wait_for(distvirt.running)

        # Phase 2: migrations (job workload, runs to completion)
        await dv.apply("staging", file="with-migrations.yaml")
        await ns.workload("migrate").wait_for(distvirt.completed, timeout=300)

        # Phase 3: full stack
        await dv.apply("staging", file="full-stack.yaml")
        await ns.workload("api").wait_for(distvirt.running)
```

## Architecture

The SDK has two layers:

### Rust core (`distvirt-sdk-core`)

PyO3 extension that wraps the `distvirt-spec` crate — the same spec parsing pipeline used by the CLI. Takes a YAML file path and returns serialized protobuf bytes. This ensures spec parsing behavior is identical between the CLI and SDK — no reimplementation, no drift.

Single function: `parse_spec(path, values) -> (namespace_id, proto_bytes)`

### Python client (`distvirt/`)

Pure async Python built on `grpcio`. Manages gRPC connections, provides typed handles for namespaces/workloads/services, and maintains a live object model from event streams.

## Core concepts

### Live namespace handle

When you call `dv.apply()` or `dv.namespace()`, the SDK opens an event stream to the orchestrator *before returning the handle*. This event stream feeds a background task that maintains an in-memory object model of the namespace state.

This eliminates a class of races: if you `apply()` then immediately `wait_for(running)`, and the workload transitions to running between those two calls, the event stream has already captured the transition. No events are missed.

### Object model

The `Namespace` handle maintains `WorkloadModel` and `ServiceModel` objects that reflect current state. These are updated from the event stream in real time.

- `ns.status()` reads from the local model — synchronous, no RPC.
- `ns.workload("api").status()` reads the workload's model entry.
- `ns.events()` and `ns.logs()` open separate streams for user consumption (independent of the internal model stream).

### State waiters

`wait_for()` checks the current model state. If it already matches, it returns immediately. If not, it registers a predicate that the background event loop checks after each model update. When the predicate matches, the future resolves.

```python
# Returns immediately if already running
await ns.workload("api").wait_for(distvirt.running)

# With timeout
await ns.workload("migrate").wait_for(distvirt.completed, timeout=300)
```

Available state matchers:

| Workload states | Service states |
|----------------|---------------|
| `distvirt.dormant` | `distvirt.idle` |
| `distvirt.launching` | `distvirt.active` |
| `distvirt.running` | |
| `distvirt.completed` | |
| `distvirt.failed` | |
| `distvirt.suspended` | |

### Apply is idempotent

`apply()` creates the namespace if it doesn't exist, or updates it if it does. The SDK handles the create-or-update logic internally.

### Spec files, not spec objects

The SDK parses YAML spec files (via the Rust core) and sends proto bytes to the orchestrator. There is no Python-side spec manipulation API — specs are authored as YAML, and the Python code sequences their application. This keeps the SDK surface small and avoids duplicating the spec model.

Variable substitution for fragment includes is supported:

```python
await dv.apply("staging", file="distvirt.yaml",
               values={"IMAGE": "myorg/api:v1.2.3"})
```

For advanced use cases, pre-serialized proto bytes can be passed directly:

```python
await dv.apply("staging", spec_bytes=my_proto_bytes)
```

## Development

Requires [uv](https://docs.astral.sh/uv/).

```bash
cd sdk/python

# Install dependencies and dev tools
uv sync

# Run tests
uv run pytest

# Build the Rust extension (requires Rust toolchain)
uv run maturin develop
```

## Checklist

What needs to happen before this is production-ready:

- [x] **Extract `distvirt-spec` crate** — Spec parsing now lives in `distvirt-spec`, shared by the CLI and SDK.
- [x] **Implement PyO3 `parse_spec`** — `parse_spec(path)` works end-to-end: parse → resolve includes → validate → serialize to proto bytes.
- [ ] **`parse_spec` `values` parameter** — The `values` parameter is accepted but ignored. Needs an API addition to `distvirt-spec` to accept caller-provided variable substitutions (currently values are only read from the YAML `include` entries).
- [x] **Proto codegen for Python** — Generate Python gRPC stubs from `client.proto`. Decide on build-time vs checked-in generation.
- [x] **Implement `connect()`** — gRPC channel creation with optional TLS and auth token interceptor.
- [x] **Implement `apply()`** — Create-or-update logic (try `CreateNamespace`, fall back to `UpdateNamespace` on `ALREADY_EXISTS`).
- [x] **Implement `Namespace` event loop** — Background task consuming `StreamEvents`, updating the `NamespaceModel`, notifying waiters.
- [x] **Implement `NamespaceModel.apply_event()`** — Map each proto event type to model state mutations. Requires documenting the event lifecycle guarantees in the proto.
- [x] **Implement `NamespaceModel.apply_status()`** — Bootstrap model from `GetNamespaceStatus` response.
- [x] **Add `exit_code` to `WorkloadCompleted` proto message** — Added to proto.
- [x] **Document event lifecycle guarantees in proto** — Delivery guarantees, state transitions, and client tracking documented in proto comments.
- [ ] **Implement remaining RPCs** — `logs()`, `attach()`, `deactivate()`.
- [x] **Error handling** — Typed exception hierarchy (`DistvirtError` → `SpecError`, `ConnectionError`, `ApiError`, `StreamEndedError`, `TimeoutError`). gRPC status codes mapped to `ApiError`. Event loop failures propagated to waiters. Contextual timeout messages.
- [ ] **Tests** — Unit tests for the waiter/model system, integration tests against a running orchestrator.
- [ ] **Package and publish** — CI pipeline for building the maturin wheel (manylinux + macOS).
