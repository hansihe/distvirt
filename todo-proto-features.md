# Proto & SDK Feature Gaps for Fresha Migration

## Protocol gaps

### ~~1. Restart policy on WorkloadSpec~~ ✓
**Done** — Added `RunPolicy` enum (`SERVICE = 0`, `JOB = 1`) and `run_policy` field on `WorkloadSpec` in the client proto. Conversion wired through to the existing orchestrator SM which already handles job completion/retry logic.

### ~~2. Exit code / reason on WorkloadCompleted / WorkloadFailed~~ ✓
**Done** — `WorkloadCompleted` now carries `int32 exit_code` and `WorkloadFailed` carries `optional int32 exit_code` + `string reason`. Exit codes and failure reasons propagate from pod-level events through the orchestrator SM to the client proto.

### 3. Readiness signal
**Priority: medium**

`WorkloadRunning` means the VM started, not that the service inside is accepting connections. The deploy script does `await ns.workload("postgres").wait_for(distvirt.running)` then immediately tries to bootstrap databases — that races against postgres init. Can be worked around with a TCP poll loop in the deploy script, but a readiness probe would be cleaner.

## SDK gaps (proto exists, SDK doesn't expose it)

### 4. `patch()` method on Client
**Priority: high**

The migration applies infra first, then incrementally adds app fragments. `apply()` does Create/UpdateNamespace (full spec replacement). `PatchNamespace` exists in the proto and does exactly what fragments need (upsert workloads/services), but `Client` doesn't expose it.

### 5. `connect_network()` / `disconnect_network()` in SDK
**Priority: high**

The deploy script calls `bootstrap_databases("postgres:5432", ...)` and `create_kafka_topics("tansu:9092", ...)` which need network access to the namespace fabric. `ConnectNetwork`/`DisconnectNetwork` exist in the proto but aren't in the SDK. Without this the script has to shell out to `dv connect`.

### ~~6. Exit code on WorkloadModel~~ ✓
**Done** — Resolved by item 2. Exit codes now flow through `PodStatus::Finished`/`Failed` → `WlStatus::Completed`/`Failed` → proto `WorkloadCompleted`/`WorkloadFailed`.

## Quality-of-life improvements

### 7. Compound state matchers
**Priority: medium**

`wait_for(distvirt.running)` hangs forever if the workload fails. For migrations you want something like `wait_for(distvirt.completed | distvirt.failed)` and then check which state was reached. Currently `WorkloadStateMatcher` only matches a single state.

### 8. Bulk wait
**Priority: low**

Waiting for all 4 infra workloads requires 4 separate `wait_for` calls. Something like `await ns.wait_for_all(["postgres", "valkey", "tansu", "amqp"], distvirt.running)` would be cleaner but isn't blocking.
