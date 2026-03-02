# Snapshots, Suspend/Resume & Live Migration

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

### Namespace Snapshot

A namespace snapshot bundles:
- All pod snapshots (one per running workload)
- Orchestrator namespace state: service definitions, DNS registry, IP allocations, service→workload bindings
- Fabric state: route table entries, service entity configs

Namespace snapshots are **consistent** — all pods are suspended at the same logical point before any snapshot data is captured.

---

## Storage

### Storage Pool Model

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
1. Orchestrator decides to suspend workload W (idle timeout, scale-to-zero policy)
2. Orchestrator: UpdateServiceBackend(service_id, None)
   → Service entity enters buffering mode, new traffic buffered
3. Orchestrator: SuspendPod(namespace_id, pod_id, snapshot_id)
   → Worker receives command
4. Worker: Firecracker CreateSnapshot API (pauses vCPUs, serializes state)
5. Worker: Drain remaining frames from TAP fd (post-pause)
6. Worker: Store snapshot to local storage (memory dump + disk state)
7. Worker: Tear down VM process, release resources (memory, fds, TAP)
8. Worker: PodSuspended { namespace_id, pod_id, snapshot_id }
9. Orchestrator: Remove pod route entries, keep placeholder with buffer policy
10. Orchestrator: Workload transitions to Suspended state
```

### Resume Path (Traffic-Triggered)

```
1. Traffic arrives at service IP → ServiceActivation event (existing mechanism)
2. Orchestrator decides: restore from snapshot vs cold start
   - Check snapshot registry: is there a valid local snapshot on a suitable worker?
   - If yes → ResumeFromSnapshot path
   - If no → standard LaunchPod cold start
3. Orchestrator: ResumePod(namespace_id, pod_id, snapshot_id, network)
   → Worker receives command
4. Worker: Firecracker LoadSnapshot API (~5-10ms)
5. Worker: Attach new TAP device, connect to fabric
6. Worker: PodRunning { namespace_id, pod_id }
7. Orchestrator: UpdateServiceBackend(service_id, new backend) + ServiceReady
8. Fabric flushes buffered frames → resumed guest
```

### Snapshot Registry

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
   b. Worker A: Firecracker CreateSnapshot (vCPUs paused)
   c. Worker A: Drain remaining frames from TAP fd
   d. Worker A: PodSuspended event

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
   c. Orchestrator: ServiceReady → flush buffered frames to resumed guest
   d. Orchestrator: Cleanup source snapshot on worker A
```

### Failure Handling

| Failure point | Recovery |
|--------------|----------|
| Transfer fails | Abort migration, resume pod on worker A, restore routes |
| Resume fails on worker B | Abort migration, resume pod on worker A (snapshot still valid), restore routes |
| Worker A dies during suspend | Pod is lost. Orchestrator cold-starts on worker B (or another). Same as any worker failure. |
| Worker B dies after resume | Standard pod failure handling. Orchestrator reschedules. |

### Network Transparency Details

The key insight is that **the fabric's existing buffering infrastructure handles migration transparency**:

- **Service entity buffering**: `UpdateServiceBackend(None)` puts the service into buffering mode. This is the same mechanism used for scale-to-zero activation. New traffic to the service IP is buffered until `ServiceReady` after resume.
- **Route table buffering**: Placeholder route entries with buffer policy capture direct pod-to-pod traffic during migration.
- **TAP drain**: After Firecracker pauses vCPUs, any frames already in the TAP buffer are read and forwarded into the fabric. This ensures no frames are lost that the guest already sent.
- **Frame flush on resume**: Buffered frames are delivered to the resumed guest after `ServiceReady` / route update, making the migration transparent to both the migrated pod and its peers.

**What the guest sees**: A brief pause (vCPUs frozen during snapshot), then execution continues. The guest's network interface may see a small gap in traffic followed by a burst of buffered frames. TCP handles this naturally via retransmission.

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
     services: [{ service_id, ip, mac, policy, workload_binding }],
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

- Fresh namespace ID, but **same IP space** — namespaces are isolated L2 domains so there is no overlap concern.
- Pod network config (IPs, MACs, gateway) stays the same within the cloned namespace.
- Service IPs stay the same; DNS entries resolve identically within the namespace.
- Clone is fully transparent to the guest — no network reconfiguration needed.

---

## Protocol Changes

### Worker Handshake Extensions

Workers include snapshot-related information in the initial handshake:

```
WorkerHandshake {
    // ... existing fields ...
    compatibility_hash: u64,       // Hash of kernel + Firecracker version + VM config
    storage_pools: [PoolInfo],     // Available storage pools on this worker
}

PoolInfo {
    pool_id: PoolId,
    pool_type: Local | Shared | Remote,
    capabilities: Set<Boot | Snapshot | Transfer>,
    total_bytes: u64,
    available_bytes: u64,
}
```

The orchestrator uses `compatibility_hash` to determine which workers can accept a given snapshot. Mismatches are rejected — no best-effort restore attempts.

### New Worker Commands

```
SuspendPod {
    namespace_id: String,
    pod_id: u64,
    snapshot_id: String,
    destination_pool_id: PoolId,   // Which pool to store the snapshot in
}

ResumePod {
    namespace_id: String,
    pod_id: u64,
    snapshot_id: String,
    source_pool_id: PoolId,        // Which pool to load the snapshot from
    network: PodNetwork,           // May differ from original (migration)
}

TransferSnapshot {
    snapshot_id: String,
    source_pool_id: PoolId,
    destination_pool_id: PoolId,   // Could be local on another worker, shared, or remote
    target_worker_id: String,      // For worker-to-worker streaming transfers
}
```

### New Worker Events

```
PodSuspended {
    namespace_id: String,
    pod_id: u64,
    snapshot_id: String,
    pool_id: PoolId,
    snapshot_size_bytes: u64,
}

SnapshotTransferred {
    snapshot_id: String,
    destination_pool_id: PoolId,
}

SnapshotEvicted {
    snapshot_id: String,
    pool_id: PoolId,
    reason: String,            // "orchestrator_eviction", "space_pressure", "invalidated"
}
```

### VMM Trait Extensions

```rust
pub trait VmInstance: Send + 'static {
    // ... existing methods ...

    /// Create a snapshot of the VM (pauses vCPUs).
    /// Returns the path to the snapshot artifacts.
    fn snapshot(&mut self, snapshot_dir: &Path)
        -> impl Future<Output = anyhow::Result<SnapshotArtifacts>> + Send;
}

pub trait Vmm: Send + Sync {
    // ... existing methods ...

    /// Restore a VM from a snapshot.
    fn restore(&self, config: &VmConfig, snapshot: &SnapshotArtifacts)
        -> impl Future<Output = anyhow::Result<Self::Instance>> + Send;
}

pub struct SnapshotArtifacts {
    pub memory_file: PathBuf,    // VM memory dump
    pub snapshot_file: PathBuf,  // Device state (Firecracker snapshot file)
    pub disk_file: PathBuf,      // Writable container drive
    pub mem_size_bytes: u64,
    pub disk_size_bytes: u64,
}
```

**Disk snapshot strategy (v1)**: Snapshot the full writable disk with compression. For stateless workloads the disk delta is typically small (ext4 journal + app writes), and compression handles this well.

**Future optimization**: Use an overlay filesystem inside the guest — base image mounted read-only, overlayfs on top for writes. Snapshots only need to capture the overlay, and the base image becomes shareable/cacheable across pods. This naturally separates "container image" (immutable, stored in pools) from "guest writes" (small, per-instance). This also opens the door to smarter base image distribution via the pool model.

### Orchestrator State Extensions

New workload state:

```rust
pub enum WorkloadState {
    Dormant,
    WaitingForCapacity,
    Launching { .. },
    Running { .. },
    Suspending {                   // NEW
        pod_id: PodId,
        worker_id: WorkerId,
        snapshot_id: SnapshotId,
    },
    Suspended {                    // NEW
        snapshot_id: SnapshotId,
        snapshot_location: SnapshotLocation,
    },
    Migrating {                    // NEW
        source_worker: WorkerId,
        target_worker: WorkerId,
        snapshot_id: SnapshotId,
        phase: MigrationPhase,
    },
    Resuming {                     // NEW
        pod_id: PodId,
        worker_id: WorkerId,
        snapshot_id: SnapshotId,
    },
}

pub enum MigrationPhase {
    Suspending,
    Transferring,
    Resuming,
}

pub enum SnapshotLocation {
    Pool { pool_id: PoolId },
}
```

---

## Design Decisions

1. **Snapshot compatibility** — Workers include a `compatibility_hash` (kernel + Firecracker version + VM config) in the handshake. The orchestrator rejects snapshot restore on workers with mismatched hashes. No best-effort attempts — mismatch falls back to cold start.

2. **Cloning uses same IP space** — Namespaces are isolated L2 domains, so cloned namespaces reuse the same IP space. Clone is fully transparent to the guest.

3. **V1 disk handling: full snapshot with compression** — Snapshot the entire writable disk. For stateless workloads the delta is small and compresses well. Future optimization: guest-internal overlayfs separating base image (read-only, shareable) from guest writes (small overlay).

4. **Storage is orchestrator-managed** — Workers report pool inventory and capacity. Orchestrator makes all placement, eviction, and transfer decisions. This enables global policy (rebalancing, cost-aware placement) and supports future pool types (shared storage, EBS multi-attach).

5. **Incremental migration deferred** — The protocol is designed to support it (transfer commands can express incremental operations), but v1 implements only full-snapshot migration. Pre-copy with dirty page tracking is a future optimization for reducing pause windows.

6. **Concurrent operations during namespace snapshot** — Wait for all pods to reach a stable VMM state (Firecracker process snapshotable) before beginning the namespace snapshot. "Stable" means the VMM is running, not the guest application — the guest could still be booting.

## Open Questions

1. **Pre-warming** — Should the orchestrator speculatively distribute snapshots to workers where traffic is likely? This reduces resume latency but costs storage and bandwidth. Could be a policy knob. Likely to become clearer after other parts are implemented.

2. **Pool capability discovery** — How rich should the pool capability model be? V1 is simple (Boot, Snapshot, Transfer), but future backends may need finer-grained capabilities (e.g. "supports incremental writes", "supports concurrent readers"). Extend as needed.

3. **Shared pool semantics** — EBS multi-attach and similar shared storage has specific consistency semantics (e.g. no concurrent writers without coordination). How do we model this in the pool capability system? Defer until we have a concrete shared storage backend to target.
