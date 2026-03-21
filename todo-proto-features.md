# Proto & SDK Feature Gaps for Fresha Migration

## Protocol gaps

### 1. Restart policy on WorkloadSpec
**Priority: critical**

No field on `WorkloadSpec` to distinguish run-to-completion workloads (DB migrations, seed jobs) from long-running services. The proto has `completed`/`failed` states, but nothing tells the orchestrator "don't restart this when it exits." Without this, migration jobs either restart in a loop or require implicit one-shot behavior.

### 2. Exit code / reason on WorkloadCompleted / WorkloadFailed
**Priority: high**

Both `WorkloadCompleted` and `WorkloadFailed` messages are empty. `PodStopped` has `exit_code` and `PodFailed` has `reason`, but this info isn't propagated to the workload level. The deploy script needs to know if a migration exited 0 vs crashed.

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

### 6. Exit code on WorkloadModel
**Priority: high**

`PodStopped` carries `exit_code` but `WorkloadModel` discards it. Migration jobs need to check success/failure after `wait_for(completed)`.

## Quality-of-life improvements

### 7. Compound state matchers
**Priority: medium**

`wait_for(distvirt.running)` hangs forever if the workload fails. For migrations you want something like `wait_for(distvirt.completed | distvirt.failed)` and then check which state was reached. Currently `WorkloadStateMatcher` only matches a single state.

### 8. Bulk wait
**Priority: low**

Waiting for all 4 infra workloads requires 4 separate `wait_for` calls. Something like `await ns.wait_for_all(["postgres", "valkey", "tansu", "amqp"], distvirt.running)` would be cleaner but isn't blocking.
