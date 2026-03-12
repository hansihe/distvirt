---
title: "MicroVM Memory Management via Cgroup2 + Virtio-Balloon"
---

> **Status (March 2026):** ~50% implemented. Core balloon control works. Actual guest-init implementation has evolved beyond this doc with improved pending-deflation tracking — see `guest-image/guest-init/src/memory/` for current state.

## Problem

Running containers inside Firecracker microVMs for staging. We want to overcommit host memory (VMs sized smaller than workload limits) while avoiding OOM kills. We accept brief workload stalls in exchange for reduced host memory footprint.

## Core Mechanism

The system combines three Linux kernel primitives:

- **cgroup2 memory controller** — `memory.high` throttles allocations when exceeded, `memory.max` is the hard OOM boundary.
- **Virtio-balloon** — dynamically adjusts the guest's usable memory by inflating (reclaiming pages from guest) or deflating (returning pages to guest).
- **PSI (Pressure Stall Information)** — provides real-time signals when processes in a cgroup are stalled waiting for memory.
- **cgroup freezer** — pauses all processes in a cgroup with ~15–20µs latency, used as a circuit breaker while the balloon catches up.
- **zram swap** — a small compressed swap partition used as a safety buffer to absorb allocation bursts that outpace the balloon response cycle.

The VM starts with memory sized to the workload's *requests* (not limits) plus a kernel overhead buffer. The balloon is initially inflated to consume the gap between the VM size and the workload limit. When the workload needs more memory, the system deflates the balloon to provide it, expanding the effective cgroup limits in lockstep.

## Architecture: Two Independent Control Loops

The design decomposes into two loops with cleanly separated concerns. They interact only through observable system state, never directly.

The two things we need to monitor are (1) memory pressure on the container workload and (2) the state of memory outside the container — kernel overhead, PID1, etc. The container cgroup has its own PSI file. The VM as a whole has global PSI at `/proc/pressure/memory`. The question is what combination of signals best drives each loop.

### Signal Analysis: Global PSI vs Cgroup PSI vs meminfo

**Cgroup PSI** (`/sys/fs/cgroup/<name>/memory.pressure`) measures stall time for tasks within the container. This fires when the workload hits `memory.high` (throttling) or `memory.max` (direct reclaim / OOM). This is a clean, unambiguous signal for container pressure.

**Global PSI** (`/proc/pressure/memory`) measures stall time across all tasks in the VM. This is a superset — container stalls are counted in both cgroup and global PSI. A natural idea is to use the differential (global fires but cgroup doesn't) as a signal for kernel-side pressure. However, there are subtleties:

1. **PID1 is the only non-cgroup task, and it barely allocates.** When kernel overhead grows and free pages shrink, the tasks that actually stall during allocation are almost always container tasks hitting direct reclaim due to the VM being globally low on pages. This shows up in *both* cgroup and global PSI, not cleanly in "global minus cgroup."

2. **PSI is reactive, not predictive.** PSI fires after stalls have begun. Kernel overhead can silently consume the buffer — slab caches grow, network buffers accumulate — without any task stalling, because nothing is allocating at that moment. By the time PSI fires, the buffer is already gone and the next allocation is already stalling.

3. **Distinguishing stall sources is hard from PSI alone.** When a container task stalls, was it because it hit its cgroup limit, or because the VM is globally out of pages? Both look the same in the PSI metrics. You'd need to cross-reference `memory.current` vs `memory.high` at the moment of the stall to tell them apart.

**`/proc/meminfo`** gives `MemAvailable`, which directly answers "how much headroom does the VM have right now" regardless of who consumed it. This catches silent overhead growth before stalls begin, but requires polling rather than event-driven triggers.

### Options for the Monitoring Architecture

Given these tradeoffs, there are three viable architectures. They differ primarily in how Loop 1 (the kernel overhead loop) gets its signal.

#### Option A: Cgroup PSI + meminfo Polling

```
┌─────────────────────────────────────────────────────┐
│ MicroVM                                             │
│                                                     │
│  ┌─────────────────────────────────────────────┐    │
│  │ Container Cgroup2                           │    │
│  │                                             │    │
│  │   workload processes                        │    │
│  │                                             │    │
│  │   cgroup PSI ──────────────► Loop 2         │    │
│  └─────────────────────────────────────────────┘    │
│                                                     │
│  PID1                                               │
│    ├── Loop 1: /proc/meminfo poll ► cgroup limits   │
│    └── Loop 2: cgroup PSI ► balloon + freeze        │
│                                                     │
│  Kernel, balloon, zram swap, etc.                   │
└─────────────────────────────────────────────────────┘
```

Loop 1 polls `/proc/meminfo` every 1–2s and adjusts cgroup `memory.high` / `memory.max` to maintain a target buffer for kernel overhead. Loop 2 listens to cgroup PSI and manages the balloon + freeze cycle.

**Advantages:** Simple and predictable. meminfo catches silent overhead growth before stalls occur. Clear separation — one loop per signal source. Well-understood failure modes.

**Disadvantages:** Loop 1 is polling-based with inherent latency (up to 2s before detecting overhead growth). Needs tuning for poll interval, smoothing window, and hysteresis to avoid jitter. `MemAvailable` fluctuates as the kernel grows and shrinks slab caches, so a moving average or multi-sample threshold is needed.

#### Option B: Dual PSI (Fully Event-Driven)

```
┌─────────────────────────────────────────────────────┐
│ MicroVM                                             │
│                                                     │
│  ┌─────────────────────────────────────────────┐    │
│  │ Container Cgroup2                           │    │
│  │                                             │    │
│  │   cgroup PSI ──────────────► Loop 2         │    │
│  └─────────────────────────────────────────────┘    │
│                                                     │
│  /proc/pressure/memory ───────► Loop 1              │
│                                                     │
│  PID1                                               │
│    ├── Loop 1: global PSI ► cgroup limits           │
│    └── Loop 2: cgroup PSI ► balloon + freeze        │
│                                                     │
│  Kernel, balloon, zram swap, etc.                   │
└─────────────────────────────────────────────────────┘
```

Both loops are event-driven. Loop 1 registers a PSI trigger on `/proc/pressure/memory` and interprets "global PSI without cgroup PSI" as kernel overhead pressure. Loop 2 is unchanged.

**Advantages:** Fully event-driven — no polling, no tuning of intervals. Reacts instantly when pressure occurs. Elegant symmetry between the two loops.

**Disadvantages:** The "global without cgroup" signal is weak in practice because PID1 barely allocates and most stalls manifest on container tasks regardless of source. Cannot catch silent overhead growth — if kernel overhead eats the buffer without causing stalls, global PSI never fires. Requires a timing window to determine whether cgroup PSI is also firing (fragile). When both fire simultaneously, determining the source requires reading meminfo/cgroup stats anyway, which undermines the pure-PSI approach.

#### Option C: Dual PSI + meminfo Safety Net (Hybrid)

```
┌─────────────────────────────────────────────────────┐
│ MicroVM                                             │
│                                                     │
│  ┌─────────────────────────────────────────────┐    │
│  │ Container Cgroup2                           │    │
│  │                                             │    │
│  │   cgroup PSI ──────────────► Loop 2         │    │
│  └─────────────────────────────────────────────┘    │
│                                                     │
│  /proc/pressure/memory ──┐                          │
│  /proc/meminfo (poll) ───┤► Loop 1 decision logic   │
│  cgroup memory.current ──┘                          │
│                                                     │
│  PID1                                               │
│    ├── Loop 1: global PSI + meminfo ► cgroup limits │
│    └── Loop 2: cgroup PSI ► balloon + freeze        │
│                                                     │
│  Kernel, balloon, zram swap, etc.                   │
└─────────────────────────────────────────────────────┘
```

Loop 1 uses *both* global PSI as an event-driven trigger *and* low-frequency meminfo polling as a safety net. When either signal indicates an issue, it reads meminfo + cgroup stats to make an informed decision. Loop 2 is unchanged.

The decision function on any Loop 1 trigger:
1. Read `MemAvailable`, `MemTotal`, and `memory.current`.
2. Compute overhead: `MemTotal - MemAvailable - memory.current`.
3. If `MemAvailable` < buffer target → tighten cgroup limits to protect the buffer.
4. If `memory.current` is well below `memory.high` but `MemAvailable` is low → the pressure is from kernel overhead, not the container. Tighten limits or deflate balloon without raising limits.

**Advantages:** Primarily event-driven for fast response. meminfo poll (low frequency, e.g., every 5–10s) acts as a safety net to catch silent overhead growth that PSI misses. Decision logic uses precise data, not just "something stalled." Most robust against edge cases.

**Disadvantages:** Most complex. Two trigger sources for Loop 1 means more code paths to test. The global PSI trigger may rarely fire for the kernel-only case in practice, making it dead code that still needs maintenance.

### Recommendation

Option A (cgroup PSI + meminfo polling) is the simplest and most robust starting point. The main weakness — polling latency on Loop 1 — is acceptable because kernel overhead changes slowly (over seconds, not milliseconds). A 1–2s poll interval catches drift with plenty of margin before it becomes dangerous.

Option C is worth evolving toward if testing reveals cases where the polling interval is too slow or where global PSI provides a useful early warning. The upgrade path is straightforward — add a global PSI trigger alongside the existing meminfo poll.

Option B is elegant but has real gaps. The "global minus cgroup" signal is too unreliable in a VM with only one non-cgroup task, and the inability to catch silent overhead growth is a fundamental limitation.

### Loop 1: Kernel Overhead Regulator

**Signal:** `MemAvailable` from `/proc/meminfo` (Options A/C), optionally global PSI as additional trigger (Option C)

**Actuator:** cgroup2 `memory.high` and `memory.max`

**Goal:** Maintain the target free buffer for everything outside the container cgroup — kernel slab caches, page tables, network buffers, PID1 itself, and vmalloc.

**Logic:**

1. Periodically (every 1–2s) read `MemTotal`, `MemAvailable`, and the container's `memory.current`.
2. Compute the non-container overhead: `MemTotal - MemAvailable - memory.current`.
3. Derive the maximum safe cgroup limit: `MemTotal - overhead - buffer_target`.
4. Adjust `memory.max` toward this value. Set `memory.high` slightly below (e.g., max minus 10–20MB).

**Constraints:**

- Never lower `memory.max` below `memory.current` without first freezing the container and giving Loop 2 a chance to deflate the balloon. Dropping max below current triggers synchronous reclaim and risks OOM.
- Use a moving average or require multiple consecutive readings before tightening. `MemAvailable` fluctuates as the kernel grows and shrinks slab caches, and reacting to every dip causes jitter.
- Run at a slower frequency than Loop 2. This loop handles slow drift in kernel overhead, not fast workload spikes.

**Why this matters:** The container cgroup PSI only reflects pressure *inside* the cgroup. If the kernel consumes memory outside the cgroup (e.g., growing dentry cache due to workload I/O patterns), the cgroup sees no pressure, but the VM is running out of memory. Loop 1 catches this by monitoring the VM-global view.

### Loop 2: Balloon Pressure Regulator

**Signal:** PSI events on the container cgroup (`memory.pressure`)

**Actuator:** Virtio-balloon (inflate/deflate), cgroup freezer

**Goal:** Provide memory to the workload on demand by deflating the balloon, and reclaim memory when the workload no longer needs it by inflating.

#### Deflation (workload needs more memory)

1. PSI `FULL` event fires on the container cgroup — the workload is stalled on memory.
2. Freeze the container cgroup (~15–20µs).
3. Deflate the balloon by the current step size (starting at 32MB; see adaptive stepping below). The guest kernel reclaims pages from the balloon device.
4. Raise `memory.high` and `memory.max` by the same step.
5. Unfreeze the container.

The freeze prevents the workload from racing into the OOM boundary while the balloon deflation completes. Observed freeze durations are ~50ms total, which is invisible to most web services and batch jobs.

##### Adaptive Balloon Step Sizing

A fixed 32MB step is conservative and slow for large spikes. To converge faster, the deflation step size increases exponentially on consecutive deflations within a short window:

1. Initial step size: 32MB.
2. If another deflation is triggered within 2s of the previous one, double the step size (32 → 64 → 128MB), capped at 256MB or half the remaining balloon, whichever is smaller.
3. Reset the step size back to 32MB after 10s with no deflation trigger.

This converges quickly for large spikes (e.g., a 400MB spike in ~2 steps instead of 5) while remaining granular for small fluctuations. Each step is still individually safe — the freeze-deflate-raise-unfreeze sequence is atomic from the workload's perspective regardless of step size.

#### Inflation (reclaiming unused memory)

1. Periodically check `memory.current` relative to `memory.high`.
2. If usage has been below 75% of `memory.high` for multiple consecutive samples (e.g., 3–4 samples at 5s intervals, ~15–20s of sustained low usage), inflate the balloon by one step.
3. Lower `memory.high` and `memory.max` by the same step.

This is the steady-state path for reclaiming memory after a workload spike subsides. The hysteresis (requiring sustained low usage) prevents oscillation.

For workloads that are essentially idle (near-zero PSI, stable low usage), inflation can be more aggressive — larger steps, shorter wait.

Every inflation step is safely reversible: if the workload spikes again, the PSI-triggered deflation path kicks in automatically.

**Guarding against bursty workloads:** The 15–20s observation window may be too short for workloads with periodic spikes (e.g., a 30s cron cycle). To avoid inflating during a trough and immediately deflating on the next spike, the inflation decision should also consider recent deflation history: if any deflation occurred in the last 60s, suppress inflation. This prevents pointless inflate-deflate cycles that impose freeze costs with no net memory savings. For v2, workload-specific profiling (e.g., tracking RSS trend direction over longer windows) can further tune inflation aggressiveness.

#### Enhancement (v2): eBPF Closed-Loop Freeze

The fixed 50ms freeze duration is a conservative guess. An eBPF kprobe on the balloon driver (`leak_balloon` for deflation, `fill_balloon` for inflation in `drivers/virtio/virtio_balloon.c`) can provide real-time page counts, enabling a closed-loop freeze:

1. Freeze the container.
2. Initiate balloon deflation.
3. Watch via eBPF for the required number of pages to be freed.
4. Unfreeze as soon as headroom is confirmed.

This yields ~5ms freezes when the balloon responds quickly and longer holds only when genuinely needed.

**Caveat:** The eBPF probe sees pages *offered* by the guest, not yet *reclaimed* by the host. For the deflation case this is fine — we care that the guest has free pages available, not whether the host has processed them yet. The fixed-timeout path should be kept as a fallback for robustness.

**Deferral rationale:** The fixed 50ms timeout works well enough for v1. The eBPF optimization reduces freeze duration but doesn't change correctness. Prioritize getting the core loops stable and well-instrumented before adding this layer.

## zram Swap as a Safety Buffer

### The high-to-max Race

When the workload hits `memory.high`, PSI fires and Loop 2 begins the freeze-deflate-unfreeze cycle. But `memory.max` is only 10–20MB above `memory.high`. If the workload is doing coarse-grained allocations (a 50MB buffer, a JVM region), a single allocation can blow past both limits before the freeze lands. The cgroup controller intercepts at `memory.high`, but under severe pressure the kernel can push through to `memory.max` and OOM.

### Solution: zram Swap Partition

A small zram swap partition (64–128MB) absorbs allocation bursts that outpace the balloon response cycle. Instead of hitting OOM, workload pages compress into zram, buying time for Loop 2 to deflate the balloon and raise limits.

**Configuration:**
- Size: 64–128MB of compressed swap (effective capacity depends on compression ratio; typical workload pages compress ~2:1, yielding ~128–256MB effective).
- Priority: Set as the only swap device. The container cgroup's `memory.swap.max` should be set equal to the zram size to prevent unbounded swap usage.
- Interaction with loops: zram usage is visible via `/sys/block/zram0/mm_stat`. If zram usage is growing, it confirms that the workload is outpacing the balloon — Loop 2 should use larger step sizes. If zram usage persists after balloon deflation completes, it indicates pages were swapped under pressure and will fault back in naturally.

**Why zram specifically:** zram compresses into host memory that's already allocated to the VM, so it doesn't change the VM's memory footprint from the host's perspective. It's purely an internal buffer that converts an OOM race into a latency penalty (page compression/decompression), which is consistent with the design's core tradeoff of accepting stalls to avoid kills.

## Interaction Between Loops

The loops compose naturally through system state:

**Workload spike:**
Container hits pressure → cgroup PSI fires → Loop 2 deflates balloon → VM gains pages → `MemAvailable` rises → Loop 1 sees headroom and may raise cgroup max → container grows into it.

**Kernel overhead growth:**
Kernel consumes more memory → `MemAvailable` drops (caught by Loop 1 poll, or global PSI fires in Option C) → Loop 1 lowers cgroup max to protect buffer → container is squeezed → cgroup PSI fires → Loop 2 deflates balloon → `MemAvailable` recovers → equilibrium restored.

**Workload subsides:**
Container usage drops → Loop 2 inflates balloon (slow path) → `MemAvailable` drops → Loop 1 lowers cgroup max accordingly → system tightens.

Neither loop needs to know about the other's existence. They observe and actuate on their own signals, and the system converges.

### Cross-Loop Oscillation Risk

The kernel overhead growth scenario above has an implicit coupling that can oscillate under specific conditions. Consider: the container is using all its allocated memory (`memory.current` ≈ `memory.max`) and kernel overhead grows. Loop 1 cannot lower `memory.max` below `memory.current`, so it lowers `memory.high` below `memory.current`, triggering kernel throttling and cgroup PSI. Loop 2 deflates the balloon and raises limits. `MemAvailable` rises, so Loop 1 relaxes limits. If the workload remains at the same usage level, Loop 2 eventually inflates the balloon. But now kernel overhead hasn't shrunk, so `MemAvailable` drops again, and the cycle repeats.

The specific failure case: the container's memory is entirely anonymous pages (no file cache to drop). Lowering `memory.high` causes thrashing rather than reclaim. Loop 2 deflates the balloon, but freed pages go to the global pool, not the cgroup. `MemAvailable` rises, Loop 1 relaxes, Loop 2 inflates — oscillation.

**Mitigations:**

- The inflation hysteresis (15–20s sustained low usage + 60s deflation cooldown) already damps most oscillation by ensuring inflation doesn't occur immediately after deflation.
- Add a "steady-state floor" to Loop 1: once kernel overhead is observed to have stabilized at a higher level (e.g., 3+ minutes of consistent overhead readings), Loop 1 should accept the new overhead as the baseline rather than continuously trying to reclaim the buffer. The buffer target itself should be adjusted upward.
- Track the inflate/deflate ratio over a rolling window. If the ratio approaches 1:1 over a 5-minute window, the system is oscillating. Response: stop inflating and log a warning for operator investigation.
- zram absorbs the transient pressure during each oscillation cycle, preventing OOM even if the system takes several cycles to converge.

This scenario should be an explicit test case in the integration test suite.

## Host-Side Contention

The design assumes balloon deflation completes promptly, but the entire point of this system is host memory overcommit. When the host itself is under memory pressure — multiple VMs requesting deflation simultaneously, or the host reclaiming pages for its own needs — balloon deflation latency increases because the host is reluctant to return pages.

**Implications:**

- The freeze duration (currently ~50ms observed) may increase significantly under host contention. The freeze timeout must account for this. A fixed 50ms timeout that unfreezes the container before the balloon has actually deflated defeats the purpose of the freeze.
- The host may be inflating balloons across other VMs while this guest is trying to deflate — a direct conflict that the guest has no visibility into.

**Mitigations:**

- Use a generous freeze timeout ceiling (e.g., 500ms) for v1. The workload experiences a longer stall, but this is strictly better than OOM. The eBPF closed-loop freeze in v2 will tighten this to actual completion time.
- Monitor freeze durations as an operational metric. Sustained long freezes indicate host-level overcommit is too aggressive and should trigger alerts.
- zram provides a second layer of defense: if the balloon is slow to deflate due to host contention, the workload pages into zram rather than hitting OOM.
- Document the host-side contract: the host's balloon management must respect a minimum deflation rate per VM, or the guest-side system cannot maintain its latency guarantees.

## Initial Sizing

| Parameter | Value | Rationale |
|---|---|---|
| VM memory | requests + kernel overhead estimate + buffer target | Sized to initial steady-state needs |
| Balloon initial size | limits - requests | Consumes the gap; deflated on demand |
| `memory.high` | Slightly below requests | Triggers kernel throttling before hard limit |
| `memory.max` | Slightly above `memory.high` (~10–20MB) | Hard OOM boundary; zram absorbs bursts past this gap |
| Initial balloon step | 32MB | Granular enough to avoid waste; scales up adaptively on consecutive deflations |
| Max balloon step | 256MB or half remaining balloon | Caps exponential growth to prevent overshoot |
| Kernel buffer target | 64MB (provisional; see below) | Must exceed worst-case kernel overhead variance |
| zram swap size | 64–128MB | Absorbs allocation bursts that outpace balloon response |

### Buffer Target Sizing

The kernel buffer target is the single most critical parameter in the system. It must be large enough to absorb kernel overhead variance between Loop 1 poll cycles without hitting OOM, but small enough that it doesn't defeat the purpose of overcommit.

The primary sources of kernel overhead variance are:

- **TCP socket buffers:** Default `tcp_rmem`/`tcp_wmem` max is ~6MB per socket. A burst of 20 connections could add 50–100MB. This is the dominant risk.
- **Slab cache fluctuation:** Typically gradual (10s of MB over seconds), well within polling latency.
- **Page tables:** Proportional to virtual address space touched. Can spike with large sparse mappings (Java, Go heaps) but is generally stable for a given workload.

**Required prerequisite:** Cap `tcp_rmem` and `tcp_wmem` inside the VM before relying on a sub-100MB buffer. Without this, a connection burst can exceed the buffer in a single poll cycle. Recommended caps: `tcp_rmem = "4096 131072 2097152"` (2MB max), `tcp_wmem = "4096 131072 2097152"` (2MB max). This bounds per-socket buffers and makes the 64MB target viable.

**Sizing strategy for v1:** Start with 64MB. Instrument `MemAvailable` and kernel overhead across all staging workloads for the first two weeks of deployment. Collect p50/p95/p99 of overhead variance per poll cycle. Adjust the buffer target to p99 + 20% margin. Persist per-workload overhead profiles and use them as starting estimates on subsequent deploys.

If instrumentation is not yet available, start with 96MB as a more conservative default and tighten once data is collected.

## Kernel Overhead Considerations

Memory outside the container cgroup but inside the VM includes:

- **Slab caches** (dentry, inode) — grows with filesystem activity, but kernel reclaims readily under pressure. Typically gradual, not sudden.
- **Page tables** — proportional to virtual address space touched, not RSS. Large sparse mappings (Java, Go heaps) can consume significant page table memory.
- **Network buffers** — TCP socket buffers default to ~6MB max per socket. A burst of 20 connections could add 50–100MB. Historically inconsistent cgroup charging. **Must be capped as a prerequisite** (see Buffer Target Sizing above).
- **Kernel stacks** — ~16KB per thread, generally negligible.
- **PID1 and its allocations** — small, stable.
- **vmalloc, module memory** — small, stable.

Sudden large spikes are unlikely for typical staging workloads *once network buffers are capped*. The realistic risk is slow drift to a steady state that's higher than the initial estimate. Loop 1 handles this by continuously adapting.

**Additional mitigations for worst case:**
- Run Loop 1 at higher frequency during the first 30–60s after container start, when kernel overhead is ramping from near-zero to steady state.
- Persist observed kernel overhead per workload and use as the starting estimate on subsequent deploys.

## Strict Mode: Production-Fidelity Testing

The overcommit system intentionally diverges from production behavior: a memory leak that would OOM in production is gracefully accommodated by balloon deflation in staging. This masks a class of bugs that only surface in production.

**Strict mode** is a per-VM flag that disables the overcommit system and simulates production sizing:

- Balloon is pinned at its initial size (no deflation).
- `memory.high` and `memory.max` are set to the production-equivalent limits and not adjusted.
- Loop 1 continues running (kernel overhead protection is still useful) but Loop 2 is disabled.
- The workload experiences the same OOM behavior it would in production.

**Usage:** Strict mode should be available as a deployment flag. It is not intended for default use — the purpose of the overcommit system is to save host memory — but should be used for periodic validation runs and when debugging suspected memory leaks. CI pipelines that test memory behavior should run in strict mode.

## Observability

Both loops must emit metrics for tuning and anomaly detection. All metrics should be exposed via a local Prometheus endpoint or equivalent.

**Loop 1 metrics:**
- `memavailable_bytes` — current `MemAvailable` reading.
- `kernel_overhead_bytes` — computed `MemTotal - MemAvailable - memory.current`.
- `cgroup_max_bytes` / `cgroup_high_bytes` — current limit values.
- `loop1_adjustments_total` — counter of limit adjustments, labeled by direction (tighten/relax).

**Loop 2 metrics:**
- `balloon_size_bytes` — current balloon size.
- `balloon_deflations_total` / `balloon_inflations_total` — counters.
- `balloon_step_bytes` — current adaptive step size.
- `freeze_duration_seconds` — histogram of freeze durations.
- `freeze_total` — counter of freeze events.
- `psi_triggers_total` — counter of cgroup PSI events.
- `zram_used_bytes` — current zram swap usage.

**Derived alerts:**
- Freeze duration p99 > 200ms → host contention may be too high.
- Inflate/deflate ratio > 0.5 over 5min → possible oscillation.
- zram usage sustained > 50% for > 60s → balloon is not keeping up.
- Kernel overhead trend increasing over 10min → workload may need a larger VM or overhead estimate adjustment.
- Balloon fully deflated → workload has reached its limit ceiling; further pressure will OOM.

**Dashboard:** A single time-series view overlaying balloon size, cgroup limits, `memory.current`, `MemAvailable`, and freeze events provides a complete picture of system behavior for any given VM.

## Observed Performance

From testbench with a 400MB spike against a 250MB initial limit:

- Freeze latency: 15–20µs
- Total freeze duration per balloon step: ~50ms
- With fixed 32MB steps: 5 steps, ~3.5s to fully accommodate spike
- With adaptive stepping (32 → 64 → 128 → 128): 4 steps, ~2.0s estimated (to be validated)
- Workload survived without OOM

The ~50ms stalls per step are negligible for staging web services and batch jobs. With zram as a safety buffer, the workload would remain functional even if balloon response is delayed by host contention.

## Tradeoffs

**What we gain:**
- Host memory overcommit — VMs consume only what workloads actually need.
- OOM avoidance — the balloon and zram expand before the OOM killer fires.
- Graceful degradation — workloads slow down briefly rather than dying.

**What we give up:**
- Latency — brief stalls (~50ms per balloon step) during memory pressure, potentially longer under host contention.
- Behavioral fidelity — staging behavior differs from production where VM sizes are fixed. Mitigated by strict mode for targeted validation.
- Complexity — two control loops, zram configuration, cgroup management in PID1, and (in v2) eBPF probes.

For staging purposes, these tradeoffs are favorable.

## Implementation Phases

**v1:** Core system.
- Option A architecture (cgroup PSI + meminfo polling).
- Loop 1 and Loop 2 as described.
- Adaptive balloon stepping.
- zram swap partition.
- `tcp_rmem`/`tcp_wmem` caps.
- Strict mode flag.
- Full observability (metrics + dashboard).
- 64MB provisional buffer target.
- Fixed 50ms freeze timeout with 500ms ceiling.

**v2:** Optimizations and hardening.
- eBPF closed-loop freeze (replaces fixed timeout).
- Per-workload overhead profiling and persisted baselines.
- Evaluate upgrade to Option C if polling latency proves insufficient.
- Oscillation detection and auto-dampening.
- Buffer target tuning based on collected instrumentation data.
