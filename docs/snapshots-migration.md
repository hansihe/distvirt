# Snapshots, Suspend/Resume & Live Migration

## Current State

**Implemented:**
- Pod suspend/resume lifecycle (Suspending → Suspended → Resuming → Running)
- Firecracker `snapshot()` and `restore()` VMM trait methods
- Worker commands: `SuspendPod`, `ResumePod`, `DeleteSnapshot`
- Worker events: `PodSuspended`, `PodSuspendFailed`, `PodRunning` (on resume)
- Orchestrator workload states: `Suspending`, `Suspended`, `Resuming` (with timeout timers)
- Guest-side suspend handshake (`PrepareSuspend` / `SuspendReady` via guest-init vsock)
- Fabric buffering infrastructure: service entity buffering, route table placeholder buffering, packet flush on resume
- `ServiceActivation` events triggering demand-up from suspended state
- `suspend_on_idle` workload policy for automatic scale-to-zero
- Snapshot artifacts stored as directory on worker local filesystem (snapshot.bin, mem.bin, container.ext4, metadata.json)

**Not yet implemented:**
- Storage pool abstraction (PoolId, pool types, capabilities, pool-aware commands)
- Worker `compatibility_hash` for snapshot validation
- Live migration (TransferSnapshot, Migrating state, MigrationPhase)
- Namespace-level snapshots (S3 upload/download, namespace manifest)
- Snapshot registry / eviction management
- TAP frame drain after VM pause (post-pause frames from TAP fd)

---

## Overview

Three related capabilities built on Firecracker's native VM snapshot/restore:

1. **Suspend/resume** — Scale-to-zero with fast restore (~5-10ms) instead of cold boot (~100ms+). Frequent operation, needs fast local storage.
2. **Live migration** — Transparently move a running pod between workers for pool management (draining, rebalancing). Must be invisible to the guest and its peers.
3. **Namespace snapshots** — Full namespace checkpoint to persistent storage (S3). Enables cloning, disaster recovery, and environment sharing.

---

## Snapshot Anatomy

### Pod Snapshot

A pod snapshot consists of:

| Component | Source | Size |
|-----------|--------|------|
| VM memory + device state | Firecracker `CreateSnapshot` API | ~mem_size_mib (compressible — zero pages, repeated patterns) |
| Writable disk state | Container drive (ext4 with guest writes) | Varies — small for stateless workloads |
| Pod metadata | Orchestrator | Negligible (pod config, network assignment, container config) |

Snapshot artifacts are stored as a directory:

```
<snapshot_dir>/
  metadata.json   # SnapshotMetadata (kernel_path, rootfs_source_path)
  snapshot.bin    # Firecracker device state
  mem.bin         # VM memory dump
  container.ext4  # Container drive with runtime writes
```

### Namespace Snapshot

A namespace snapshot bundles:
- All pod snapshots (one per running workload)
- Orchestrator namespace state: service definitions, DNS registry, IP allocations, service→workload bindings
- Fabric state: route table entries, service entity configs

Namespace snapshots are **consistent** — all pods are suspended at the same logical point before any snapshot data is captured.

---

## Storage

### Current Implementation

Snapshots are stored on the worker's local filesystem in a temporary directory. The orchestrator tracks which worker holds a given snapshot via `Suspended { worker_id, snapshot_id }`. There is no pool abstraction yet — resume must happen on the same worker that performed the suspend.

### Target: Storage Pool Model

Storage is modeled as **pools** — named storage locations with known capabilities, locality, and capacity. The orchestrator reasons about pools to plan snapshot placement and transfer paths.

```
Pool {
    pool_id: PoolId,
    pool_type: Local | Shared | Remote,
    capabilities: Set<Boot | Snapshot | Transfer>,
    locality: Set<WorkerId>,       // Which workers can access this pool
    capacity: u64,                 // Bytes available
}
```

**Pool types**:

| Type | Example | Locality | Typical capabilities | Latency |
|------|---------|----------|---------------------|---------|
| Local | Worker tmpfs, local SSD | Single worker | Boot, Snapshot | Sub-ms to low single-digit ms |
| Shared | EBS multi-attach, NFS | Multiple workers | Boot, Snapshot | Low ms (depends on backend) |
| Remote | S3-compatible object store | All workers | Transfer (not Boot) | Hundreds of ms to seconds |

**Transfer as pool-to-pool operations**: The orchestrator plans snapshot movement as transfers between pools. Examples:

- **Same-worker resume**: Snapshot already in local bootable pool — no transfer needed.
- **Migration with shared storage**: Local→Shared (fast, same volume), boot from Shared on target. Or Shared→Local on target, then boot.
- **Migration without shared storage**: Local→Local via worker-to-worker streaming.
- **Namespace snapshot to S3**: Local→Remote.
- **Restore from S3**: Remote→Local, then boot.

Workers advertise their pools at connect time. The orchestrator tracks pool inventory and makes all placement/eviction decisions. New storage backends become new pool types with different capabilities — no protocol changes needed.

### V1 Scope

V1 implements a single local pool per worker (and optionally one S3 remote pool). Transfer operations are worker-to-worker streaming or upload/download to S3. The pool abstraction exists in the protocol (workers report pools, commands reference pool IDs) but the orchestrator's "planning" is trivial with only these pool types.

---

## Suspend/Resume (Scale-to-Zero)

### Suspend Path

```
1. Orchestrator decides to suspend workload W (idle timeout via suspend_on_idle policy)
2. Orchestrator: UpdateServiceBackend(service_id, None)
   → Service entity enters buffering mode, new traffic buffered
3. Orchestrator: SuspendPod(namespace_id, pod_id, snapshot_id)
   → Worker receives command, starts suspend_timeout timer
4. Worker: Send PrepareSuspend to guest via vsock
5. Guest-init: Flush application state, respond with SuspendReady
6. Worker: Firecracker CreateSnapshot API (pauses vCPUs, serializes state)
7. Worker: Store snapshot to local storage (memory dump + device state + disk)
8. Worker: Tear down VM process, release resources (memory, fds, TAP)
9. Worker: PodSuspended { namespace_id, pod_id, snapshot_id, snapshot_size_bytes }
10. Orchestrator: Remove pod route entries, keep placeholder with buffer policy
11. Orchestrator: Workload transitions to Suspended { worker_id, snapshot_id }
```

**Timeout handling**: If the worker doesn't respond with `PodSuspended` within the suspend timeout, the orchestrator treats the suspend as failed and can retry or fall back. The `PodSuspendFailed` event covers explicit failures (e.g. Firecracker snapshot API error).

**TODO**: Drain remaining frames from TAP fd after vCPU pause (step between 6 and 7). Frames already in the TAP buffer after pause should be read and forwarded into the fabric to avoid frame loss.

### Resume Path (Traffic-Triggered)

```
1. Traffic arrives at service IP → ServiceActivation event (existing mechanism)
2. Orchestrator decides: restore from snapshot vs cold start
   - Is the workload in Suspended state with a valid snapshot on a known worker?
   - If yes → ResumeFromSnapshot path
   - If no → standard LaunchPod cold start
3. Orchestrator: ResumePod(namespace_id, pod_id, snapshot_id, network)
   → Worker receives command, starts resume_timeout timer
4. Worker: Firecracker LoadSnapshot API (~5-10ms)
5. Worker: Attach new TAP device, connect to fabric
6. Worker: PodRunning { namespace_id, pod_id }
7. Orchestrator: UpdateServiceBackend(service_id, new backend) + ServiceReady
8. Fabric flushes buffered packets → resumed guest
```

### Snapshot Registry

> **Not yet implemented.** Currently the orchestrator tracks snapshots implicitly via `Suspended { worker_id, snapshot_id }`. The registry described below is the target for when we need eviction, invalidation, and multi-pool awareness.

The orchestrator maintains a registry of available snapshots:

```
SnapshotEntry {
    snapshot_id: SnapshotId,
    pod_id: PodId,
    namespace_id: NamespaceId,
    pool_id: PoolId,              // Which storage pool holds this snapshot
    created_at: Timestamp,
    compatibility_hash: u64,      // Hash of kernel + Firecracker version + VM config
    image_digest: String,         // Container image digest for invalidation
    mem_size_bytes: u64,
    disk_size_bytes: u64,
}
```

**Invalidation rules**:
- Container image changes (different digest) → snapshot invalid
- Compatibility hash mismatch (kernel, Firecracker version, or VM config change) → snapshot invalid
- Pool eviction (LRU, space pressure) → snapshot removed, orchestrator notified

**Eviction**: Orchestrator-managed. Workers report pool capacity and current inventory. Orchestrator decides eviction policy (LRU, priority-based, etc.) and issues explicit eviction commands. This enables global rebalancing — e.g. keeping hot snapshots on workers with available capacity, migrating cold snapshots to shared/remote pools.

---

## Live Migration

> **Not yet implemented.** The suspend/resume primitives and fabric buffering infrastructure that migration builds on are in place. The missing pieces are snapshot transfer between workers, the `Migrating` workload state, and orchestrator migration coordination logic.

### Goals

- **Transparent**: The guest and its network peers should not observe the migration (beyond a brief latency spike during the pause window).
- **Minimal downtime**: Pause window should be as short as possible — ideally single-digit ms for the VM itself, plus network transfer time for dirty pages.
- **Safe**: If migration fails at any point, the source pod remains running.

### Basic Migration Flow

```
1. Orchestrator decides: migrate pod P from worker A → worker B
   (trigger: worker drain, rebalancing, capacity management)

2. PREPARE PHASE
   a. Orchestrator: UpdateServiceBackend(service_id, None) on worker A
      → Service entity enters buffering mode, new traffic to service IP buffered
   b. Orchestrator: Update route table — replace pod route with placeholder + buffer
      → Direct pod-to-pod traffic also buffered

3. SUSPEND PHASE
   a. Orchestrator: SuspendPod(namespace_id, pod_id, snapshot_id) on worker A
   b. Worker A: PrepareSuspend handshake with guest
   c. Worker A: Firecracker CreateSnapshot (vCPUs paused)
   d. Worker A: Drain remaining frames from TAP fd
   e. Worker A: PodSuspended event

4. TRANSFER PHASE
   a. Orchestrator: TransferSnapshot(snapshot_id, target_worker_id) on worker A
      → Worker A streams snapshot data to worker B
      → Or: Worker A uploads to shared storage, worker B pulls
   b. Worker B: TransferComplete event

5. RESUME PHASE
   a. Orchestrator: ResumePod(namespace_id, pod_id, snapshot_id, new_network) on worker B
   b. Worker B: LoadSnapshot, attach TAP, join fabric
   c. Worker B: PodRunning event

6. CUTOVER PHASE
   a. Orchestrator: Update route table — replace placeholder with worker B route
   b. Orchestrator: UpdateServiceBackend(service_id, new backend on worker B)
   c. Orchestrator: ServiceReady → flush buffered packets to resumed guest
   d. Orchestrator: Cleanup source snapshot on worker A (DeleteSnapshot)
```

### Failure Handling

| Failure point | Recovery |
|--------------|----------|
| Suspend fails on worker A | Abort migration, resume normal operation, restore routes |
| Transfer fails | Abort migration, resume pod on worker A, restore routes |
| Resume fails on worker B | Abort migration, resume pod on worker A (snapshot still valid), restore routes |
| Worker A dies during suspend | Pod is lost. Orchestrator cold-starts on worker B (or another). Same as any worker failure. |
| Worker B dies after resume | Standard pod failure handling. Orchestrator reschedules. |

### Network Transparency Details

The key insight is that **the fabric's existing buffering infrastructure handles migration transparency**:

- **Service entity buffering**: `UpdateServiceBackend(None)` puts the service into buffering mode. This is the same mechanism used for scale-to-zero activation. New traffic to the service IP is buffered until `ServiceReady` after resume.
- **Route table buffering**: Placeholder route entries with buffer policy capture direct pod-to-pod traffic during migration.
- **TAP drain**: After Firecracker pauses vCPUs, any frames already in the TAP buffer are read and forwarded into the fabric. This ensures no frames are lost that the guest already sent.
- **Packet flush on resume**: Buffered packets are delivered to the resumed guest after `ServiceReady` / route update, making the migration transparent to both the migrated pod and its peers.

**What the guest sees**: A brief pause (vCPUs frozen during snapshot), then execution continues. The guest's network interface may see a small gap in traffic followed by a burst of buffered packets. TCP handles this naturally via retransmission.

**What peers see**: A brief period where traffic is buffered (latency spike), then delivered. TCP connections survive. UDP traffic may experience brief packet loss if buffers fill.

### Future: Incremental Migration (Pre-Copy)

> **Not in v1** — the protocol supports it, but only full-snapshot migration is implemented initially.

Firecracker supports **dirty page tracking** for incremental snapshots. This enables a pre-copy approach to minimize pause time:

```
1. Start dirty page tracking on worker A
2. Take base snapshot, stream to worker B (VM still running)
3. Take incremental snapshot (only dirty pages since base)
4. If dirty set is small enough: pause → final incremental → resume on B
5. If dirty set still large: repeat step 3 (converge)
```

This reduces the pause window to the time needed to transfer the final dirty page set, which for most workloads is much smaller than the full memory.

---

## Namespace Snapshots (S3)

> **Not yet implemented.**

### Create Namespace Snapshot

```
1. Orchestrator: pause namespace (all services → buffering mode)
2. Orchestrator: SuspendPod for all running pods (parallel)
3. All workers: PodSuspended events
4. Orchestrator: UploadSnapshot commands to each worker
   → Workers upload pod snapshots to S3 (parallel)
5. Orchestrator: Write namespace manifest to S3:
   {
     namespace_id, created_at,
     pods: [{ pod_id, snapshot_key, workload_id, vm_config, network }],
     services: [{ service_id, ip, policy, workload_binding }],
     dns_entries: { name → ip },
     ip_allocations: { ... },
   }
6. Orchestrator: Resume all pods (or leave suspended if snapshot-and-stop)
```

### Restore Namespace Snapshot

```
1. Orchestrator: Create new namespace from manifest
2. Orchestrator: Assign workers, create fabric, create service entities
3. Orchestrator: Download pod snapshots to target workers
4. Orchestrator: ResumePod for each pod (with new network config if IPs differ)
5. Orchestrator: Wire up services, DNS, routes
6. Orchestrator: ServiceReady for all services
```

### Namespace Cloning

Clone = restore a namespace snapshot under a new namespace ID.

- Fresh namespace ID, but **same IP space** — namespaces are isolated IP networks so there is no overlap concern.
- Pod network config (IPs, gateway) stays the same within the cloned namespace.
- Service IPs stay the same; DNS entries resolve identically within the namespace.
- Clone is fully transparent to the guest — no network reconfiguration needed.

---

## Protocol

### Worker Handshake

Current handshake (`WorkerCapabilities`):

```rust
pub struct WorkerCapabilities {
    pub has_kvm: bool,
    pub has_containerd: bool,
    pub available_adapters: Vec<String>,
    pub max_pods: u32,
    pub available_memory_mb: u64,
    pub public_endpoint: String,
}
```

**Target extensions** (not yet implemented):

```rust
pub struct WorkerCapabilities {
    // ... existing fields ...
    pub compatibility_hash: u64,       // Hash of kernel + Firecracker version + VM config
    pub storage_pools: Vec<PoolInfo>,  // Available storage pools on this worker
}

pub struct PoolInfo {
    pub pool_id: PoolId,
    pub pool_type: PoolType,           // Local | Shared | Remote
    pub capabilities: HashSet<PoolCapability>, // Boot | Snapshot | Transfer
    pub total_bytes: u64,
    pub available_bytes: u64,
}
```

The orchestrator uses `compatibility_hash` to determine which workers can accept a given snapshot. Mismatches are rejected — no best-effort restore attempts.

### Worker Commands

Current snapshot-related commands:

```rust
SuspendPod {
    namespace_id: NamespaceId,
    pod_id: PodId,
    snapshot_id: SnapshotId,
}

ResumePod {
    namespace_id: NamespaceId,
    pod_id: PodId,
    snapshot_id: SnapshotId,
    network: PodNetworkConfig,       // May differ from original (migration)
}

DeleteSnapshot {
    snapshot_id: SnapshotId,
}
```

**Target extensions** (not yet implemented — needed for migration and pool-aware storage):

```rust
SuspendPod {
    // ... existing fields ...
    destination_pool_id: PoolId,     // Which pool to store the snapshot in
}

ResumePod {
    // ... existing fields ...
    source_pool_id: PoolId,          // Which pool to load the snapshot from
}

TransferSnapshot {
    snapshot_id: SnapshotId,
    source_pool_id: PoolId,
    destination_pool_id: PoolId,     // Could be local on another worker, shared, or remote
    target_worker_id: WorkerId,      // For worker-to-worker streaming transfers
}
```

### Worker Events

Current snapshot-related events:

```rust
PodSuspended {
    namespace_id: NamespaceId,
    pod_id: PodId,
    snapshot_id: SnapshotId,
    snapshot_size_bytes: u64,
}

PodSuspendFailed {
    namespace_id: NamespaceId,
    pod_id: PodId,
    error: String,
}
```

**Target extensions** (not yet implemented):

```rust
PodSuspended {
    // ... existing fields ...
    pool_id: PoolId,
}

SnapshotTransferred {
    snapshot_id: SnapshotId,
    destination_pool_id: PoolId,
}

SnapshotEvicted {
    snapshot_id: SnapshotId,
    pool_id: PoolId,
    reason: String,            // "orchestrator_eviction", "space_pressure", "invalidated"
}
```

### VMM Traits

```rust
pub trait Vmm: Send + Sync {
    type Instance: VmInstance;
    fn launch(&self, config: &VmConfig)
        -> impl Future<Output = anyhow::Result<Self::Instance>> + Send;
    fn restore(&self, snapshot: &SnapshotArtifacts, net: Option<&NetConfig>)
        -> impl Future<Output = anyhow::Result<Self::Instance>> + Send;
}

pub trait VmInstance: Send + 'static {
    fn connect_vsock(&self, port: u32) -> impl Future<Output = anyhow::Result<UnixStream>> + Send;
    fn tap(&self) -> Option<&TapDevice>;
    fn take_tap(&mut self) -> Option<TapDevice>;
    fn wait(&mut self) -> impl Future<Output = anyhow::Result<()>> + Send;
    fn kill(&mut self) -> impl Future<Output = anyhow::Result<()>> + Send;
    fn snapshot(&mut self, snapshot_dir: &Path)
        -> impl Future<Output = anyhow::Result<SnapshotArtifacts>> + Send;
}

pub struct SnapshotMetadata {
    pub kernel_path: PathBuf,        // Needed by Firecracker restore
    pub rootfs_source_path: PathBuf, // Re-copied into tmpdir on restore
}

pub struct SnapshotArtifacts {
    pub snapshot_dir: PathBuf,       // Directory containing all snapshot files
    pub metadata: SnapshotMetadata,
}
```

**Disk snapshot strategy (v1)**: Snapshot the full writable disk with compression. For stateless workloads the disk delta is typically small (ext4 journal + app writes), and compression handles this well.

**Future optimization**: Use an overlay filesystem inside the guest — base image mounted read-only, overlayfs on top for writes. Snapshots only need to capture the overlay, and the base image becomes shareable/cacheable across pods. This naturally separates "container image" (immutable, stored in pools) from "guest writes" (small, per-instance). This also opens the door to smarter base image distribution via the pool model.

### Orchestrator State

Current workload states:

```rust
pub enum WorkloadState {
    Dormant,
    WaitingForCapacity,
    Launching {
        pod_id: PodId,
        worker_id: WorkerId,
        launch_timeout: TimerKey,
    },
    Running {
        pod_id: PodId,
        worker_id: WorkerId,
    },
    Suspending {
        pod_id: PodId,
        worker_id: WorkerId,
        snapshot_id: SnapshotId,
        suspend_timeout: TimerKey,
    },
    Suspended {
        worker_id: WorkerId,
        snapshot_id: SnapshotId,
    },
    Resuming {
        pod_id: PodId,
        worker_id: WorkerId,
        snapshot_id: SnapshotId,
        resume_timeout: TimerKey,
    },
}
```

**Target extensions** (not yet implemented):

```rust
pub enum WorkloadState {
    // ... existing variants ...
    Migrating {
        source_worker: WorkerId,
        target_worker: WorkerId,
        snapshot_id: SnapshotId,
        phase: MigrationPhase,
    },
}

pub enum MigrationPhase {
    Suspending,
    Transferring,
    Resuming,
}
```

Note: the current `Suspended` variant tracks `worker_id` directly. When the pool abstraction is added, this will change to reference a `PoolId` / `SnapshotLocation` instead, decoupling snapshot storage from the worker that created it.

---

## Design Decisions

1. **Snapshot compatibility** — Workers will include a `compatibility_hash` (kernel + Firecracker version + VM config) in the handshake. The orchestrator rejects snapshot restore on workers with mismatched hashes. No best-effort attempts — mismatch falls back to cold start.

2. **Guest-side suspend handshake** — Before taking a snapshot, the worker sends `PrepareSuspend` to guest-init via vsock. The guest flushes application state and responds with `SuspendReady`. This ensures a clean snapshot point. The guest-init protocol is implemented.

3. **Cloning uses same IP space** — Namespaces are isolated IP networks, so cloned namespaces reuse the same IP space. Clone is fully transparent to the guest.

4. **V1 disk handling: full snapshot with compression** — Snapshot the entire writable disk. For stateless workloads the delta is small and compresses well. Future optimization: guest-internal overlayfs separating base image (read-only, shareable) from guest writes (small overlay).

5. **Storage is orchestrator-managed** — Workers report pool inventory and capacity. Orchestrator makes all placement, eviction, and transfer decisions. This enables global policy (rebalancing, cost-aware placement) and supports future pool types (shared storage, EBS multi-attach).

6. **Incremental migration deferred** — The protocol is designed to support it (transfer commands can express incremental operations), but v1 implements only full-snapshot migration. Pre-copy with dirty page tracking is a future optimization for reducing pause windows.

7. **Concurrent operations during namespace snapshot** — Wait for all pods to reach a stable VMM state (Firecracker process snapshotable) before beginning the namespace snapshot. "Stable" means the VMM is running, not the guest application — the guest could still be booting.

8. **Timeout-based failure detection** — All suspend/resume operations have timeout timers in the orchestrator. If a worker doesn't respond within the timeout, the orchestrator treats it as a failure and can retry or fall back to cold start.

## Open Questions

1. **Pre-warming** — Should the orchestrator speculatively distribute snapshots to workers where traffic is likely? This reduces resume latency but costs storage and bandwidth. Could be a policy knob. Likely to become clearer after other parts are implemented.

2. **Pool capability discovery** — How rich should the pool capability model be? V1 is simple (Boot, Snapshot, Transfer), but future backends may need finer-grained capabilities (e.g. "supports incremental writes", "supports concurrent readers"). Extend as needed.

3. **Shared pool semantics** — EBS multi-attach and similar shared storage has specific consistency semantics (e.g. no concurrent writers without coordination). How do we model this in the pool capability system? Defer until we have a concrete shared storage backend to target.

4. **TAP drain timing** — The post-pause TAP drain (reading frames from the TAP fd after vCPUs are frozen) is not yet implemented. Need to determine if this is a practical concern — the guest-side `PrepareSuspend` handshake may be sufficient to ensure the guest has quiesced network activity before the snapshot.
