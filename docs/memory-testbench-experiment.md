# Memory Testbench Experiment: Guest-Driven Balloon Control

## Goal

Implement an experimental version of the guest memory management system described in `guest-memory-management.md`, scoped to the memory testbench. Guest-driven (Option A from the design doc): the guest monitors PSI and meminfo, decides what balloon size it wants, and tells the host to apply it.

## Scope

**In scope (minimal experiment):**
- Loop 2: PSI-driven balloon deflation with cgroup freeze
- Dynamic `memory.high` / `memory.max` adjustment
- Adaptive balloon step sizing
- Guest→host balloon request protocol
- Host-side listener in testbench that executes balloon requests

**Deferred:**
- Loop 1 (kernel overhead regulator / meminfo polling)
- Balloon inflation (reclaiming unused memory)
- zram swap setup
- Observability / metrics
- Strict mode

## Architecture

```
┌─────────────────────────────────────────────────────┐
│ MicroVM (guest-init)                                │
│                                                     │
│  ┌─────────────────────────────────────────────┐    │
│  │ Container Cgroup2                           │    │
│  │   /sys/fs/cgroup/containers/<id>            │    │
│  │                                             │    │
│  │   memory.pressure (PSI fd)                  │    │
│  │   memory.high / memory.max (adjusted)       │    │
│  │   cgroup.freeze (freeze/unfreeze)           │    │
│  │                                             │    │
│  │   workload processes                        │    │
│  └─────────────────────────────────────────────┘    │
│                                                     │
│  PID1 (guest-init)                                  │
│    memory_manager module:                           │
│      on PSI trigger →                               │
│        1. freeze container cgroup                   │
│        2. raise memory.high & memory.max            │
│        3. send BalloonSet request to host via event │
│        4. unfreeze container cgroup                 │
│                                                     │
│  vsock event stream ──────────────────────────────► │
└─────────────────────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────┐
│ Host (testbench)                                    │
│                                                     │
│   event stream listener                             │
│     on BalloonSet { amount_mib } →                  │
│       vm.set_balloon(amount_mib)                    │
│       (Firecracker PATCH /balloon API)              │
└─────────────────────────────────────────────────────┘
```

## Changes Required

### 1. Protocol: `distvirt-guest-protocol/src/lib.rs`

Add a new event variant for balloon requests:

```rust
// In GuestEvent enum:
#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum GuestEvent {
    ContainerExited { id: String, code: i32 },
    /// Guest requests the host to set the balloon to this size.
    /// The host should call set_balloon(amount_mib) on the VMM.
    BalloonSet { amount_mib: u32 },
}
```

The event stream already exists and is used for `ContainerExited`. Adding `BalloonSet` here keeps it simple — no new streams or request/response protocol needed. The guest fires and forgets; the host applies it asynchronously. This is fine because the guest has already frozen the container and raised limits before sending the request — the balloon deflation just needs to happen eventually.

### 2. Guest-init: New `memory_manager` module

**File:** `guest-image/guest-init/src/memory_manager.rs`

This module encapsulates the balloon pressure regulator (Loop 2 from the design doc).

#### State

```rust
pub struct MemoryManager {
    /// Current balloon size in MiB (as far as the guest knows).
    balloon_amount_mib: u32,
    /// Current balloon step size in MiB (adaptive: 32 → 64 → 128 → 256).
    step_size_mib: u32,
    /// Timestamp of last deflation (for adaptive step reset).
    last_deflation: Option<std::time::Instant>,
    /// Total VM memory in MiB (from boot parameter or /proc/meminfo).
    vm_mem_mib: u32,
}
```

#### Constants

```rust
const INITIAL_STEP_MIB: u32 = 32;
const MAX_STEP_MIB: u32 = 256;
const STEP_DOUBLE_WINDOW: Duration = Duration::from_secs(2);
const STEP_RESET_TIMEOUT: Duration = Duration::from_secs(10);
```

#### Core method: `handle_pressure`

Called from the main event loop when `Ready::PsiTriggered` fires. Takes the container cgroup path and returns an optional `GuestEvent::BalloonSet` to send on the event stream.

```rust
impl MemoryManager {
    /// Handle a PSI pressure event. Returns a BalloonSet event if deflation occurred.
    pub fn handle_pressure(&mut self, cgroup_path: &str) -> Option<GuestEvent> {
        if self.balloon_amount_mib == 0 {
            log::warn!("balloon fully deflated, cannot release more memory");
            return None;
        }

        let now = Instant::now();

        // Adaptive step sizing: double if consecutive deflation within 2s.
        if let Some(last) = self.last_deflation {
            if now.duration_since(last) < STEP_DOUBLE_WINDOW {
                self.step_size_mib = (self.step_size_mib * 2)
                    .min(MAX_STEP_MIB)
                    .min(self.balloon_amount_mib);
            }
        }

        // Reset step size if no deflation for 10s.
        if let Some(last) = self.last_deflation {
            if now.duration_since(last) > STEP_RESET_TIMEOUT {
                self.step_size_mib = INITIAL_STEP_MIB;
            }
        }

        let step = self.step_size_mib.min(self.balloon_amount_mib);
        let new_balloon = self.balloon_amount_mib - step;

        // 1. Freeze the container cgroup.
        freeze_cgroup(cgroup_path);

        // 2. Raise memory.high and memory.max by step.
        raise_cgroup_limits(cgroup_path, step);

        // 3. Unfreeze the container cgroup.
        unfreeze_cgroup(cgroup_path);

        self.balloon_amount_mib = new_balloon;
        self.last_deflation = Some(now);

        log::info!(
            "deflated balloon: step={}MiB, new_balloon={}MiB, next_step={}MiB",
            step, new_balloon, self.step_size_mib
        );

        Some(GuestEvent::BalloonSet { amount_mib: new_balloon })
    }
}
```

#### Cgroup operations

```rust
/// Freeze all tasks in a cgroup (writes "1" to cgroup.freeze).
fn freeze_cgroup(cgroup_path: &str) {
    let path = format!("{}/cgroup.freeze", cgroup_path);
    if let Err(e) = std::fs::write(&path, "1") {
        log::error!("freeze cgroup {}: {}", path, e);
    }
}

/// Unfreeze all tasks in a cgroup (writes "0" to cgroup.freeze).
fn unfreeze_cgroup(cgroup_path: &str) {
    let path = format!("{}/cgroup.freeze", cgroup_path);
    if let Err(e) = std::fs::write(&path, "0") {
        log::error!("unfreeze cgroup {}: {}", path, e);
    }
}

/// Raise memory.high and memory.max by `step_mib` MiB.
fn raise_cgroup_limits(cgroup_path: &str, step_mib: u32) {
    let step_bytes = (step_mib as u64) * 1024 * 1024;

    // Read current memory.high, raise by step.
    let high_path = format!("{}/memory.high", cgroup_path);
    let max_path = format!("{}/memory.max", cgroup_path);

    let current_high = read_cgroup_bytes(&high_path).unwrap_or(u64::MAX);
    let current_max = read_cgroup_bytes(&max_path).unwrap_or(u64::MAX);

    // If limits are "max" (unlimited), don't try to raise them.
    if current_high == u64::MAX && current_max == u64::MAX {
        log::warn!("cgroup limits are 'max', nothing to raise");
        return;
    }

    let new_high = current_high.saturating_add(step_bytes);
    let new_max = current_max.saturating_add(step_bytes);

    if let Err(e) = std::fs::write(&max_path, new_max.to_string()) {
        log::error!("write {}: {}", max_path, e);
    }
    if let Err(e) = std::fs::write(&high_path, new_high.to_string()) {
        log::error!("write {}: {}", high_path, e);
    }

    log::info!(
        "raised cgroup limits: high={}→{} max={}→{} (step={}MiB)",
        current_high, new_high, current_max, new_max, step_mib
    );
}

/// Read a cgroup memory file that contains a byte count or "max".
fn read_cgroup_bytes(path: &str) -> Option<u64> {
    let content = std::fs::read_to_string(path).ok()?;
    let trimmed = content.trim();
    if trimmed == "max" {
        return Some(u64::MAX);
    }
    trimmed.parse::<u64>().ok()
}
```

### 3. Guest-init: `cgroup.rs` changes

#### Set initial memory limits on container cgroup

Add a function to set `memory.high` and `memory.max` when a container starts:

```rust
/// Set memory limits on a container cgroup.
/// `memory_high_bytes` is the throttling threshold.
/// `memory_max_bytes` is the hard OOM boundary.
pub fn set_memory_limits(cgroup_path: &str, high_bytes: u64, max_bytes: u64) -> anyhow::Result<()> {
    let high_path = format!("{}/memory.high", cgroup_path);
    let max_path = format!("{}/memory.max", cgroup_path);

    std::fs::write(&max_path, max_bytes.to_string())
        .with_context(|| format!("write {}", max_path))?;
    std::fs::write(&high_path, high_bytes.to_string())
        .with_context(|| format!("write {}", high_path))?;

    log::info!("set cgroup limits: high={} max={} on {}", high_bytes, max_bytes, cgroup_path);
    Ok(())
}
```

#### PSI trigger tuning

The current PSI trigger is `"some 100000 1000000"` (some stall for 100ms in 1s window). For the balloon use case, we want to detect pressure quickly. Consider using `"full 50000 1000000"` (all tasks stalled for 50ms in 1s window) to trigger only when the workload is actually blocked, not just slightly pressured.

For the experiment, keep the existing trigger but consider changing to `full` if `some` fires too eagerly.

### 4. Guest-init: `main.rs` integration

#### Initialization

After `ContainerManager::new()`, create the `MemoryManager`:

```rust
let mut memory_manager = MemoryManager::new(
    args.balloon_amount_mib,  // Need to receive this from host or from boot params
    args.mem_size_mib,
);
```

**Problem:** The guest doesn't currently know the balloon size or VM memory size. Options:
1. **Kernel command line:** Pass `mem_balloon=256` on the kernel cmdline, guest-init reads `/proc/cmdline`.
2. **Protocol message:** Host sends initial memory config after Ready.
3. **Read from `/proc/meminfo`:** Guest can read `MemTotal` for VM size. Balloon size can be inferred from `MemTotal` vs expected size, or passed via cmdline.

**Recommendation for experiment:** Use kernel cmdline for balloon size. Read `MemTotal` from `/proc/meminfo`. Add a boot parameter like `distvirt.balloon_mib=256` and parse it in guest-init.

#### Boot parameter parsing

```rust
/// Parse a key=value parameter from /proc/cmdline.
fn read_cmdline_param(key: &str) -> Option<String> {
    let cmdline = std::fs::read_to_string("/proc/cmdline").ok()?;
    for param in cmdline.split_whitespace() {
        if let Some(value) = param.strip_prefix(&format!("{}=", key)) {
            return Some(value.to_string());
        }
    }
    None
}
```

#### Setting initial cgroup limits

When a container starts (in `handle_message` for `StartContainer`), set initial memory limits based on effective guest memory (VM size minus balloon minus kernel buffer):

```rust
// After container is started and moved to cgroup:
let effective_mem = vm_mem_mib - balloon_amount_mib;
let kernel_buffer_mib: u32 = 64; // provisional
let container_max = effective_mem.saturating_sub(kernel_buffer_mib);
let container_high = container_max.saturating_sub(16); // 16MiB gap

cgroup::set_memory_limits(
    &cgroup_path,
    (container_high as u64) * 1024 * 1024,
    (container_max as u64) * 1024 * 1024,
)?;
```

#### Event loop: PSI handler

Replace the current PSI log-only handler with balloon logic:

```rust
Ready::PsiTriggered => {
    log::warn!("memory pressure detected");
    // Re-arm the PSI trigger.
    if let Some(psi) = containers.psi_fd() {
        let mut buf = [0u8; 256];
        unsafe {
            libc::read(psi.as_raw_fd(), buf.as_mut_ptr() as *mut _, buf.len());
        }
    }

    // Get the first running container's cgroup path.
    // (For the experiment, we have one container.)
    if let Some(cgroup_path) = containers.first_cgroup_path() {
        if let Some(event) = memory_manager.handle_pressure(&cgroup_path) {
            if let Err(e) = vsock::send_msg(&mut event_stream, &event).await {
                log::error!("failed to send BalloonSet event: {:#}", e);
            }
        }
    }
}
```

### 5. Guest-init: `container.rs` changes

Expose cgroup paths for running containers:

```rust
impl ContainerManager {
    /// Get the cgroup path of the first running container (for single-container experiment).
    pub fn first_cgroup_path(&self) -> Option<String> {
        self.containers.values()
            .find(|c| c.pid.is_some())
            .and_then(|c| c.cgroup_path.clone())
    }
}
```

### 6. Host-side: Testbench changes

#### `distvirt-worker/examples/memory_testbench.rs`

The testbench currently does a manual balloon adjustment after 3s. Replace with an event-driven loop that listens for `BalloonSet` events from the guest.

The event stream is already accepted via `vm.wait_container_exit()` which calls `session.recv_event()`. We need to change this to handle both `ContainerExited` and `BalloonSet` events.

**Key change:** Replace the manual balloon sleep+set with an event loop:

```rust
// Remove the manual balloon adjustment (lines 267-277).
// Instead, run a loop that handles events:

eprintln!("  [testbench] waiting for events (Ctrl+C to shut down)...");

loop {
    tokio::select! {
        result = vm.recv_event() => {
            match result {
                Ok(GuestEvent::ContainerExited { id, code }) => {
                    eprintln!("  [event] container {} exited (code={})", id, code);
                    container_exited = true;
                    break;
                }
                Ok(GuestEvent::BalloonSet { amount_mib }) => {
                    eprintln!("  [balloon] guest requests balloon={} MiB", amount_mib);
                    match vm.set_balloon(amount_mib).await {
                        Ok(()) => eprintln!("  [balloon] set to {} MiB", amount_mib),
                        Err(e) => eprintln!("  [balloon] failed: {:#}", e),
                    }
                }
                Err(e) => {
                    eprintln!("  [event] error: {:#}", e);
                    break;
                }
            }
        }
        _ = shutdown.cancelled() => {
            eprintln!("  [testbench] shutdown requested");
            break;
        }
    }
}
```

**Problem:** `ManagedVm::wait_container_exit()` currently only handles `ContainerExited` events. Need to expose the raw event stream or make `wait_container_exit` return all event types.

#### `distvirt-worker/src/managed_vm.rs` change

Add a method to receive raw events:

```rust
/// Receive the next event from the guest event stream.
/// Returns any GuestEvent variant (ContainerExited, BalloonSet, etc).
pub async fn recv_event(&mut self) -> anyhow::Result<GuestEvent> {
    self.session.recv_event().await
}
```

### 7. VMM: Pass balloon size on kernel cmdline

#### `distvirt-worker/src/vmm/firecracker.rs`

Append balloon size to the kernel boot args so the guest knows its initial balloon:

```rust
// In the launch method, when building boot_args:
if let Some(ref balloon) = config.balloon {
    boot_args.push_str(&format!(" distvirt.balloon_mib={}", balloon.amount_mib));
}
```

### 8. Initial cgroup limits: Who sets them?

The design doc says `memory.high` = slightly below requests, `memory.max` = slightly above `memory.high`. The guest needs to know the "requests" value.

For the experiment, derive it from what the guest can observe:
- `MemTotal` from `/proc/meminfo` = VM size
- `distvirt.balloon_mib` from cmdline = initial balloon
- Effective memory = `MemTotal - balloon_mib`
- Container max = effective memory - kernel buffer (64 MiB)
- Container high = container max - 16 MiB

This means the initial limits are set purely from boot parameters, no extra protocol messages needed.

## Sequence of Events

```
1. Host launches VM with balloon=256MiB, mem=512MiB
   Kernel cmdline includes: distvirt.balloon_mib=256
   Firecracker configures balloon device at 256MiB

2. Guest boots, guest-init starts
   Reads /proc/meminfo → MemTotal ≈ 512MiB
   Reads /proc/cmdline → balloon_mib=256
   Creates MemoryManager { balloon=256, vm_mem=512 }

3. Guest-init sets up container cgroup
   Effective = 512 - 256 = 256MiB
   Kernel buffer = 64MiB
   memory.max = 192MiB (= 256 - 64)
   memory.high = 176MiB (= 192 - 16)

4. Container starts, allocates memory
   At ~176MiB: hits memory.high → kernel throttles
   PSI trigger fires (some stall for 100ms in 1s)

5. Guest-init handles PSI:
   a. Freeze cgroup (write "1" to cgroup.freeze)
   b. Raise memory.high by 32MiB → 208MiB
   c. Raise memory.max by 32MiB → 224MiB
   d. Unfreeze cgroup (write "0" to cgroup.freeze)
   e. Send BalloonSet { amount_mib: 224 } on event stream
      (balloon was 256, step was 32, new balloon = 224)

6. Host testbench receives BalloonSet event:
   Calls vm.set_balloon(224)
   Firecracker PATCH /balloon → guest kernel releases 32MiB from balloon

7. Container continues allocating, hits new memory.high at 208MiB
   Repeat from step 5 with doubled step (64MiB) if within 2s

8. Container finishes or hits balloon=0 (fully deflated)
```

## Files to Modify

| File | Change |
|------|--------|
| `distvirt-guest-protocol/src/lib.rs` | Add `BalloonSet { amount_mib: u32 }` to `GuestEvent` |
| `guest-image/guest-init/src/memory_manager.rs` | **New file.** Balloon pressure regulator logic |
| `guest-image/guest-init/src/cgroup.rs` | Add `set_memory_limits()` function |
| `guest-image/guest-init/src/container.rs` | Expose `first_cgroup_path()` method |
| `guest-image/guest-init/src/main.rs` | Initialize `MemoryManager`, handle PSI with balloon logic, parse cmdline params |
| `distvirt-worker/src/managed_vm.rs` | Add `recv_event()` method |
| `distvirt-worker/src/vmm/firecracker.rs` | Append `distvirt.balloon_mib=` to kernel cmdline |
| `distvirt-worker/examples/memory_testbench.rs` | Replace manual balloon with event-driven loop |

## Open Questions / Risks

1. **PSI trigger sensitivity.** Current trigger is `some 100000 1000000`. May fire too eagerly or too late. The `full` variant fires only when all tasks are stalled, which is a stronger signal. May need to tune during experimentation.

2. **Freeze duration.** The design doc targets ~50ms total freeze. In this experiment the freeze is instantaneous (write "1", adjust limits, write "0") — the balloon deflation happens asynchronously via Firecracker API after unfreeze. The container may briefly stall again before the balloon pages are actually reclaimed. This is acceptable for the experiment.

3. **Race between PSI re-arm and next trigger.** After handling pressure and raising limits, the workload may still be pressured if it's allocating faster than the step size. The PSI trigger will fire again immediately. The adaptive stepping handles this by doubling the step size.

4. **`wait_container_exit` cancel safety.** The existing code notes that `recv_event` is not cancel-safe (two sequential `read_exact` calls). The testbench event loop must not drop the `recv_event` future mid-read. Using `tokio::select!` with it is technically unsafe. For the experiment, this is acceptable — a real implementation would need a spawn + channel pattern.

5. **Guest doesn't confirm balloon took effect.** The guest sends `BalloonSet` and assumes it worked. If the host is slow to apply it (host contention), the guest may send multiple deflation requests. The balloon amount tracking in `MemoryManager` stays consistent because it tracks the *requested* state, not the *actual* state. Worst case: guest requests balloon=0 before any deflation completes, then all deflations land at once.
