# Storage Pools & Artifact Management

## Overview

Artifacts are addressable blobs of data (VM snapshots, container images, persistent volumes, etc.) stored in **pools** — named storage locations with known capabilities, locality, and capacity. The orchestrator tracks which artifacts exist in which pools and plans transfers, eviction, and placement.

This design separates the **artifact** (what is stored) from the **pool** (where it is stored), enabling the orchestrator to reason about storage generically regardless of the underlying backend.

---

## Core Types

### Artifacts

An artifact is a stored blob with metadata. Artifacts are type-tagged and carry type-specific metadata.

```rust
struct Artifact {
    artifact_id: ArtifactId,
    artifact_type: ArtifactType,
    access_mode: ArtifactAccessMode,
    size_bytes: u64,
    metadata: ArtifactMetadata,
}

enum ArtifactType {
    Snapshot,         // VM memory + device state + disk
    ContainerImage,   // Built/pulled container rootfs
    Volume,           // Persistent writable volume
    Kernel,           // VM kernel image
    // Future: overlay working copies, incremental snapshots, etc.
}
```

### Artifact Access Modes

Artifacts have an access mode that governs concurrency and mutability:

```rust
enum ArtifactAccessMode {
    Exclusive,    // Mutable, single consumer at a time
    SharedRO,     // Immutable, many concurrent consumers
    CopyOnUse,    // Immutable template — cloned/copied before use
}
```

**Exclusive**: Locked to one consumer (pod) at a time. The orchestrator enforces this. Examples: persistent volume attached to a pod, container rootfs in mutable mode. Must move with the pod during migration. On suspend, mutations are part of the artifact.

**SharedRO**: Many pods can reference the same artifact simultaneously. Can be replicated freely across pools for locality — copies are just caching. Eviction is safe as long as the artifact is re-fetchable. Examples: base container images, kernel images.

**CopyOnUse**: The artifact is a template. On pod start, a working copy is created (filesystem copy, overlay, or CoW clone). The template remains immutable and shareable. The working copy is a separate Exclusive artifact referencing its parent template. Example: container rootfs used with overlay — base image is SharedRO, overlay is an Exclusive working copy.

### Pools

A pool is a storage location that holds artifacts.

```rust
struct Pool {
    pool_id: PoolId,
    pool_type: PoolType,
    capabilities: HashSet<PoolCapability>,
    locality: HashSet<WorkerId>,    // Which workers can access this pool
    capacity: PoolCapacity,
}

enum PoolType {
    Local,    // Worker-local storage (tmpfs, local SSD)
    Shared,   // Multi-worker accessible (EBS multi-attach, NFS)
    Remote,   // Network storage (S3-compatible object store)
}

enum PoolCapability {
    Boot,       // Can boot a VM directly from artifacts in this pool
    Snapshot,   // Can write snapshots to this pool
    Transfer,   // Can transfer artifacts to/from this pool
}

struct PoolCapacity {
    total_bytes: u64,
    used_bytes: u64,
    soft_watermark: u64,    // Proactive eviction threshold
    hard_watermark: u64,    // Aggressive eviction threshold
}
```

**Pool type characteristics**:

| Type | Example | Locality | Typical capabilities | Latency |
|------|---------|----------|---------------------|---------|
| Local | Worker tmpfs, local SSD | Single worker | Boot, Snapshot | Sub-ms to low ms |
| Shared | EBS multi-attach, NFS | Multiple workers | Boot, Snapshot | Low ms |
| Remote | S3-compatible store | All workers | Transfer (not Boot) | Hundreds of ms |

### Pool Declaration

Pools are declared from three sources, all unified in the orchestrator's pool inventory:

**1. Worker-intrinsic pools** — Workers discover local storage on startup (tmpfs, local SSD, etc.) and report them in `WorkerHello`. These are always worker-local. The worker knows the path, capacity, and type. This is the baseline — every worker has at least one local pool.

**2. Orchestrator-pushed pool config** — Delivered in `WorkerAccepted` (or a subsequent config message). The orchestrator tells the worker about additional pools it should use — path, capabilities, capacity limits. Use cases:
- Generic worker AMI that the orchestrator configures based on deployment topology
- Shared pools (NFS, EBS multi-attach) where multiple workers need to reference the same named pool
- Overriding or supplementing worker-discovered pools

Workers initialize pushed pools alongside self-discovered ones. The orchestrator can reference them in commands immediately after the worker acks.

**3. Orchestrator-only pools** — S3 and other remote storage. Workers don't know about these at connect time. The orchestrator manages them directly and issues transfer commands referencing remote URLs. These exist only in the orchestrator's placement table.

The orchestrator is always the authoritative source of truth for the global pool inventory and artifact placement. Workers are executors that read/write to pool paths they've been told about (self-discovered or pushed).

### Pool Backend (V1)

V1 pools are **directory-backed**: each pool maps to a filesystem path on the worker, with capacity tracking. Artifacts are subdirectories within the pool path. This is sufficient for local storage (tmpfs, local SSD) and shared filesystems (NFS). S3 is handled separately via the remote transfer path, not as a local directory.

```
/pool/path/
    <artifact_id>/
        snapshot.bin
        mem.bin
        metadata.json
        ...
```

More sophisticated backends (btrfs CoW, ZFS, overlayfs) can be added later by extending the pool capabilities model — the orchestrator drives all decisions, so the worker-side backend is an implementation detail.

### Placement Table

The orchestrator maintains a placement table mapping artifacts to pools:

```rust
struct ArtifactPlacement {
    artifact_id: ArtifactId,
    pool_id: PoolId,
    locked_by: Option<PodId>,    // For Exclusive artifacts — which pod holds the lock
    ref_count: u32,              // For SharedRO — number of active consumers
    parent_artifact: Option<ArtifactId>,  // For CopyOnUse working copies — reference to template
}
```

This is the core data structure for planning transfers and eviction.

---

## S3: Two Modes

S3 storage serves two distinct purposes that should not be conflated:

### Orchestrator-Managed S3 Pool

A `Remote` pool in the orchestrator's pool model. Used as overflow, transfer staging, or durable snapshot storage. The orchestrator tracks contents, manages lifecycle, and does GC. Ephemeral — expectation is contents may go away when the orchestrator dies (or are cleaned up on startup).

This is just another pool with `Remote` type — no special handling needed beyond the pool abstraction.

### Manual Export/Import (External S3)

**Not a pool.** An import/export target for user-initiated operations:

- `ExportArtifact(artifact_id, s3_path)` — one-shot copy out
- `ImportArtifact(s3_path) -> artifact_id` — pull into a managed pool
- `ExportNamespace(namespace_id, s3_prefix)` — coordinated multi-artifact export with manifest
- `ImportNamespace(s3_prefix)` — restore from manifest

Use cases: checkpoint a namespace to durable storage, clone across clusters, disaster recovery, environment sharing. The orchestrator does not track or manage the external S3 contents — it's a source/sink.

**Self-describing layout** — artifacts in S3 should be self-describing so a different cluster can discover and import without out-of-band coordination:

```
s3://bucket/distvirt/artifacts/<artifact_id>/
    manifest.json    # type, size, compatibility_hash, created_at, components
    snapshot.bin
    mem.bin
    ...
```

A cross-cluster index (listing available artifacts in a shared S3 bucket) is deferred — the self-describing layout is sufficient for v1.

---

## Artifact Routing / Transfer

The orchestrator plans artifact transfers across pools. The transfer graph is:

```
Nodes: pools (each with locality and capabilities)
Edges: possible transfer paths
```

**Transfer paths**:

| Path | Mechanism | When |
|------|-----------|------|
| Same worker, pool to pool | Local copy | e.g. local SSD → local tmpfs |
| Cross-worker, shared pool | Both workers access same pool | EBS multi-attach, NFS |
| Cross-worker, direct | Fabric tunnel streaming | Worker-to-worker, reuses existing tunnel infra |
| Via remote pool | Upload then download | When no direct path, or for durability |

Transfer command to a worker:

```rust
TransferArtifact {
    artifact_id: ArtifactId,
    source: TransferSource,
    destination: TransferDest,
}

enum TransferSource {
    LocalPool(PoolId),
    RemoteUrl(String),
    WorkerStream(WorkerId),
}

enum TransferDest {
    LocalPool(PoolId),
    RemoteUrl(String),
    WorkerStream(WorkerId),
}
```

**Fabric tunnel reuse**: Worker-to-worker artifact streaming can reuse the existing fabric tunnel infrastructure. However, artifact transfers are bulk data (potentially GBs) — they should not starve real-time fabric traffic. Options: separate tunnel, priority/QoS on existing tunnels, or rate limiting.

---

## Eviction & GC

### Watermark-Based Pressure

Three tiers of eviction pressure per pool:

1. **Below soft watermark** — no eviction needed.
2. **Soft watermark exceeded** — orchestrator proactively migrates cold artifacts to cheaper pools (local → S3) or evicts artifacts that have copies elsewhere.
3. **Hard watermark exceeded** — aggressive eviction. Suspended pods whose sole snapshot copy is evicted lose their state (transition to `SnapshotLost`). They can still be cold-started.
4. **Critical** — worker running out of space for active operations. Emergency eviction, potentially killing suspended pods.

### Eviction Priority

Access mode informs eviction priority (from hardest to easiest to evict):

1. **Exclusive artifacts for running pods** — never evict without killing the pod.
2. **Exclusive artifacts for suspended pods** — evicting loses pod state. Pod transitions to `SnapshotLost`, can only cold-start. This is the "worst case kills a pod" scenario.
3. **CopyOnUse working copies for suspended pods** — same consequence as #2.
4. **SharedRO artifacts with active consumers** — can evict if a copy exists in another accessible pool, or if re-fetchable from source (registry, remote pool).
5. **SharedRO artifacts with no active consumers** — evict freely.
6. **CopyOnUse templates** — evict only when no working copies reference them locally.

### SnapshotLost State

When the orchestrator must evict the sole copy of a suspended pod's snapshot:

```rust
// The workload transitions to:
SnapshotLost {
    workload_id: WorkloadId,
    // Workload spec is preserved — cold start is still possible
}
```

The workload definition is not lost. The orchestrator can cold-start the workload when demand returns. Only the in-memory/disk state from the previous run is gone.

### Orchestrator-Driven Eviction

Workers report pool capacity (periodic or event-driven). All eviction decisions are made by the orchestrator — workers do not evict autonomously. This enables global policy: rebalancing across workers, cost-aware placement (prefer cheap storage for cold artifacts), priority-based retention.

---

## Practical Artifact Lifecycle Examples

### Suspend/Resume (Local Pool)

```
1. Pod running on worker A
2. Orchestrator: SuspendPod(pod_id, snapshot_id, destination_pool: worker_a_local)
3. Worker A: snapshot → local pool
4. Orchestrator placement table: artifact(snapshot_id) in pool(worker_a_local), locked_by: None
5. Traffic arrives → resume
6. Orchestrator: ResumePod(pod_id, snapshot_id, source_pool: worker_a_local)
7. Worker A: restore from local pool
```

### Live Migration (Direct Transfer)

```
1. Pod running on worker A, need to move to worker B
2. Suspend on worker A → snapshot in worker_a_local
3. TransferArtifact(snapshot_id, source: LocalPool(a_local), dest: WorkerStream(B))
   → Worker A streams to worker B via fabric tunnel
   → Worker B writes to worker_b_local
4. ResumePod on worker B from worker_b_local
5. Delete snapshot from worker_a_local
```

### Live Migration (Via Shared Pool)

```
1. Pod running on worker A, shared pool accessible by both A and B
2. Suspend on worker A → snapshot in worker_a_local
3. TransferArtifact(snapshot_id, source: LocalPool(a_local), dest: LocalPool(shared))
4. ResumePod on worker B from shared pool (or transfer shared → b_local first)
5. Cleanup
```

### Namespace Export to S3

```
1. Orchestrator: pause namespace, suspend all pods
2. Each worker: snapshot to local pool
3. ExportArtifact for each snapshot → external S3 path
4. Orchestrator: write namespace manifest to S3
5. Resume pods (or leave suspended)
```

### Container Image (CopyOnUse)

```
1. Container image pulled/built → SharedRO artifact in worker_a_local
2. Pod starts: working copy created (Exclusive, parent: image artifact)
3. Pod runs, mutates working copy
4. Pod suspended: working copy is part of snapshot
5. Image artifact can be replicated to other workers for locality
6. Image artifact evictable when no local working copies reference it
```

### Persistent Volume

```
1. Volume created → Exclusive artifact in worker_a_local
2. Pod starts: volume attached (locked_by: pod_id)
3. Pod runs, writes to volume
4. Pod suspended: volume state preserved in pool
5. Migration: volume must transfer with the pod (Exclusive, cannot be shared)
6. Volume eviction: data loss — higher priority than reproducible snapshots
```

---

## V1 Scope (Phased)

### Phase 1: Pool Abstraction on Worker

Foundation — replace ad-hoc snapshot tempdir with pool-aware storage. No orchestrator state machine changes yet.

1. [x] Define `PoolId` type in `distvirt-worker-protocol` — `PoolId` newtype via `define_id_newtype!` in `types.rs`
2. [x] Worker-side pool registry: `Worker` maintains `pools: HashMap<PoolId, PathBuf>`, initialized with a default `"local-default"` pool on startup. `pool_path()` helper resolves pool IDs to paths. Handlers error gracefully on unknown pool IDs.
3. [x] Worker discovers/creates a default local pool on startup, reports it in `WorkerHello`/`WorkerCapabilities` — `detect_capabilities()` iterates the pool registry, reports real `capacity_bytes` and `available_bytes` via `libc::statvfs`. Cap'n Proto schema has `WorkerCapabilities.pools @10 :List(PoolInfo)`
4. [x] `SuspendPod`, `ResumePod`, `DeleteSnapshot` commands gain `pool_id` fields in the wire protocol — done in both Rust types and capnp schema (`SuspendPodCmd.poolId @3`, `ResumePodCmd.poolId @4`, `DeleteSnapshotCmd.poolId @1`). `PodSuspendedEvt.poolId @4` also present
5. [x] Worker reads/writes snapshots to the specified pool path instead of hardcoded tempdir — command dispatch threads `pool_id` to all handlers, which resolve it via `pool_path()`. `SuspendRequest` carries `pool_id` through to `PodSuspended` event. `handle_resume_pod` emits `PodFailed` (not `FatalError`) on missing snapshot.
6. [x] Orchestrator passes pool IDs through (uses the single pool the worker reported) — `NamespaceWorkerState.primary_pool_id` set from `capabilities.pools.first()` (`networking.rs:112-116`), `SuspendRequest` resolves `pool_id` from it (`output.rs:160-164`)

**Phase 1 complete.** `ArtifactId` type is deferred to Phase 2.

### Phase 2: Orchestrator Placement Table

7. [ ] Orchestrator tracks `PlacementTable`: artifact → set of (pool, lock, ref_count)
8. [x] `WorkerState` gains pool info from handshake — `WorkerState.capabilities.pools` populated from `WorkerHello` at `shell.rs:279`
9. [~] `WorkloadState::Suspended` references `ArtifactId` + `PoolId` instead of `snapshot_id` + `worker_id` — currently stores `{ worker_id, snapshot_id, pool_id }`, no `ArtifactId` type yet
10. [ ] Capacity reporting from workers (periodic or event-driven) — fields exist on `PoolInfo` but hardcoded to 0, no reporting loop

### Phase 3: Orchestrator-Pushed Pool Config

11. [ ] Pool config in `WorkerAccepted` — orchestrator can push additional pools to workers
12. [ ] Shared pool support (same `PoolId` across multiple workers)

### Phase 4: Transfers & Eviction

13. [ ] `TransferArtifact` command, fabric tunnel streaming for worker-to-worker
14. [ ] Eviction logic based on watermarks, LRU policy
15. [ ] S3 as orchestrator-managed remote pool
16. [ ] Export/import to external S3 as separate operations (not a managed pool)

### All Phases

- Only `Exclusive` (snapshots) and `SharedRO` (base images) access modes exercised initially

---

## Future Considerations

- **S3 index for cross-cluster discovery**: A lightweight index (JSON manifest or minimal DB) listing available artifacts in a shared S3 bucket. Enables cluster-to-cluster artifact sharing without manual path coordination.
- **Pre-warming**: Speculatively distributing snapshots to workers where demand is expected. Policy knob — trades storage/bandwidth for resume latency.
- **Shared pool consistency**: EBS multi-attach and similar backends have specific consistency semantics (no concurrent writers without coordination). Model in pool capabilities when a concrete backend is targeted.
- **Incremental transfers**: Transfer only dirty pages/blocks between pools. Builds on Firecracker dirty page tracking.
- **CopyOnUse with CoW backends**: Some storage backends (btrfs, ZFS, overlayfs) support cheap clones. Pool capabilities could advertise CoW support, letting the orchestrator prefer these for CopyOnUse artifacts.
