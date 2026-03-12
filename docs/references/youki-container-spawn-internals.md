---
title: "Youki Container Spawn Internals"
---

This document describes the low-level Linux behaviour of the youki container
runtime when launching, executing into, and managing containers.

## Table of Contents

- [Process Architecture](#process-architecture)
- [Container Creation Flow](#container-creation-flow)
- [The Three-Stage Fork](#the-three-stage-fork)
- [Namespace Setup](#namespace-setup)
- [Cgroup Setup](#cgroup-setup)
- [Rootfs and Pivot Root](#rootfs-and-pivot-root)
- [stdio and Terminal Handling](#stdio-and-terminal-handling)
- [Environment Variables and PATH](#environment-variables-and-path)
- [Process Environment Setup](#process-environment-setup)
- [Logging](#logging)
- [Inter-Process Communication](#inter-process-communication)
- [Container Lifecycle and State Machine](#container-lifecycle-and-state-machine)
- [Exec: Injecting into a Running Container](#exec-injecting-into-a-running-container)
- [Signal Handling](#signal-handling)
- [OCI Lifecycle Hooks](#oci-lifecycle-hooks)
- [CLI vs Spec Config Interaction](#cli-vs-spec-config-interaction)
- [Security Measures](#security-measures)

---

## Process Architecture

Youki uses a three-process model to spawn containers:

```
youki main process
  └─ intermediate process  (youki:[1:INTER])
       └─ init process     (youki:[2:INIT])  ← becomes the container payload
```

The intermediate process exists to handle namespace/cgroup setup that must
happen before the init process is forked. The init process becomes PID 1
inside the container's PID namespace and eventually `execvp()`s the
user-specified command.

Key files:
- `crates/libcontainer/src/process/container_main_process.rs` — main process logic
- `crates/libcontainer/src/process/container_intermediate_process.rs` — intermediate
- `crates/libcontainer/src/process/init/process.rs` — init process (`container_init_process()`)
- `crates/libcontainer/src/process/fork.rs` — clone/fork implementation

---

## Container Creation Flow

### Entry Points

- **`youki create`** (`crates/youki/src/commands/create.rs`) — creates a
  container in the "Created" state. The init process is spawned and waits for
  a start signal. Always runs detached.

- **`youki run`** (`crates/youki/src/commands/run.rs`) — equivalent to
  create + start. If not detached, the runtime waits for the container process
  and forwards signals.

Both use the builder pattern:

```
ContainerBuilder::new()
    .as_init(&bundle_path)
    .with_console_socket(...)
    .with_pid_file(...)
    .build()
```

### Builder Pipeline

1. **`ContainerBuilder`** (`crates/libcontainer/src/container/builder.rs`) —
   validates the container ID, stores stdio FDs, console socket, etc.

2. **`InitContainerBuilder::build()`** (`crates/libcontainer/src/container/init_builder.rs`) —
   loads `config.json` from the OCI bundle, creates the container state
   directory, sets up the console socket, and calls into `ContainerBuilderImpl`.

3. **`ContainerBuilderImpl::create()`** (`crates/libcontainer/src/container/builder_impl.rs`) —
   calls `run_container()` which:
   - Creates a `NotifyListener` (Unix domain socket for start signaling)
   - Sets OOM score adjustment on the parent
   - Sets the process as non-dumpable (`prctl(PR_SET_DUMPABLE, 0)`)
   - Assembles `ContainerArgs` with all configuration
   - Calls `container_main_process()`

---

## The Three-Stage Fork

### Clone Implementation

Youki uses Linux `clone3()` (with a `clone()` fallback) rather than `fork()`.
Two variants exist (`crates/libcontainer/src/process/fork.rs`):

- **`container_clone()`** — creates a normal child process.
- **`container_clone_sibling()`** — uses `CLONE_PARENT` so the new process
  shares the same parent as the caller. This makes the init process a sibling
  of the intermediate process (not a child), which means the main process can
  directly `waitpid()` on it.

The `clone3` path uses the syscall directly. The `clone` fallback allocates
an 8 MB stack via `mmap()` with a guard page.

### Stage 1: Main Process

(`container_main_process()` in `container_main_process.rs`)

1. Creates three IPC channels (uni-directional pipes):
   - `main_channel` — intermediate/init report status to main
   - `intermediate_channel` — main communicates with intermediate
   - `init_channel` — main communicates with init (hook coordination)
2. Clones the intermediate process
3. If user namespaces are requested: waits for intermediate to request UID/GID
   mapping, writes the mapping, sends acknowledgment
4. Waits for `IntermediateReady(init_pid)` message
5. Waits for init to report `InitReady`
6. Runs prestart/create-runtime hooks (in the runtime's namespaces)
7. Returns the init PID to the caller

### Stage 2: Intermediate Process

(`container_intermediate_process()` in `container_intermediate_process.rs`)

1. Creates the cgroup and adds itself to it (so the init process inherits it)
2. Enters the user namespace if requested (calls `unshare(CLONE_NEWUSER)`,
   requests mapping from main, waits for acknowledgment)
3. Sets resource limits (`setrlimit()` for each rlimit in the spec)
4. Enters the PID namespace (`unshare(CLONE_NEWPID)`) — this ensures the
   next fork produces PID 1 in the new namespace
5. Clones the init process as a **sibling** (`CLONE_PARENT`)
6. Sends the init PID to main via the channel
7. Exits

### Stage 3: Init Process

(`container_init_process()` in `process/init/process.rs`)

This is the most complex stage. The init process sets up the entire container
environment and eventually `execvp()`s the payload. See
[Process Environment Setup](#process-environment-setup) for the full ordered
sequence.

---

## Namespace Setup

Namespace types are applied in this order (`crates/libcontainer/src/namespaces.rs`):

```
User → PID → UTS → IPC → Network → Cgroup → Mount
```

Each namespace is either created (`unshare()`) or joined (`setns()` with
a path to an existing namespace file).

**Who applies what:**

| Namespace | Applied By | When |
|-----------|-----------|------|
| User | Intermediate process | Before PID namespace unshare |
| PID | Intermediate process | Before forking init (so init gets PID 1) |
| UTS, IPC, Network, Cgroup, Mount | Init process | Early in `container_init_process()` |

The split is necessary because:
- User namespace must be set up before PID namespace (for permission to create it)
- PID namespace must be unshared before the fork that creates PID 1
- Mount namespace must be set up in the init process (pivot_root happens there)

---

## Cgroup Setup

Cgroups are applied in the intermediate process **before** forking the init
process (`container_intermediate_process.rs`):

1. `cgroup_manager.add_task(intermediate_pid)` — adds intermediate to the cgroup
2. `cgroup_manager.apply(resources)` — applies resource limits

Because cgroup membership is inherited across `fork()`/`clone()`, the init
process automatically starts in the correct cgroup.

---

## Rootfs and Pivot Root

The rootfs setup happens in the init process (`process/init/process.rs`).

### Mount Preparation

`RootFS::prepare_rootfs()` (`crates/libcontainer/src/rootfs/`) sets up
all OCI-specified mounts (bind mounts, tmpfs, proc, sysfs, etc.) on
the new rootfs before switching to it.

### Root Switching

Three strategies, chosen based on configuration:

1. **`pivot_root()`** (default) — the standard mechanism. Changes the root
   mount and puts the old root on a separate mount point which is then
   unmounted.

2. **`move_root()`** (when `--no-pivot` is set) — uses `mount(MS_MOVE)` +
   `chroot()`. Sequence:
   - `chdir()` into new rootfs
   - Unmount/hide `/sys` and `/proc` from host
   - `mount(rootfs, "/", MS_MOVE)` — moves the entire mount tree
   - `chroot(".")` — change root
   - `chdir("/")`

3. **`chroot()`** — used when there is no mount namespace (joining host
   mount namespace).

### Post-Pivot

After pivot_root:
- `/dev/null` is reopened from inside the container to ensure it points to the
  container's device, not the host's
- Read-only paths and masked paths from the spec are applied
- Root may be remounted read-only if specified

---

## stdio and Terminal Handling

### Non-Interactive Mode (no console socket)

When `terminal` is false in the OCI spec (or no console socket is provided),
stdio is connected early in the init process **before** any namespace or
rootfs setup:

```rust
// process/init/process.rs, lines 59-73
if args.console_socket.is_none() {
    if let Some(stdin)  = args.stdin  { dup2(stdin, 0);  close(stdin);  }
    if let Some(stdout) = args.stdout { dup2(stdout, 1); close(stdout); }
    if let Some(stderr) = args.stderr { dup2(stderr, 2); close(stderr); }
}
```

The file descriptors come from the `ContainerBuilder`:
- `with_stdin(fd)`, `with_stdout(fd)`, `with_stderr(fd)` methods accept
  `OwnedFd` values
- These are typically pipes created by the container manager (containerd,
  Docker, etc.)
- If not provided, the container inherits the runtime's own stdio

### Interactive Mode (terminal: true, with console socket)

When a console socket is provided, a PTY is set up **after** pivot_root,
inside the container:

1. **PTY creation** — `openpty()` creates a master/slave pair from
   `/dev/pts/ptmx` in the container's devpts

2. **PTY verification** — both master and slave are verified to be real PTY
   devices (filesystem must be devpts, correct major/minor numbers). This
   mitigates CVE-2025-52565.

3. **Mount on `/dev/console`** — for init containers, the PTY slave is
   bind-mounted onto `/dev/console` using FD-based mounting
   (`open_tree()` + `move_mount()`) to avoid TOCTOU races

4. **Send master to runtime** — the PTY master FD is sent to the console
   socket using `sendmsg()` with `SCM_RIGHTS`:
   ```rust
   let cmsg = socket::ControlMessage::ScmRights(&[master.as_raw_fd()]);
   socket::sendmsg::<UnixAddr>(console_fd, &iov, &[cmsg], ...)?;
   ```
   The container manager receives this FD and uses it to read/write to the
   container's terminal.

5. **Set controlling terminal** — `ioctl(slave_fd, TIOCSCTTY)` makes the PTY
   slave the controlling terminal for the process session. This enables
   signal delivery (SIGINT on Ctrl-C, SIGWINCH on resize, etc.).

6. **Connect stdio to PTY** — `dup2()` maps the slave FD onto stdin (0),
   stdout (1), and stderr (2).

### Console Socket Path

The console socket is a Unix domain socket path. Because `sun_path` is limited
to 108 bytes, youki `chdir()`s into the container directory and creates a
symlink to keep the path short.

---

## Environment Variables and PATH

### Environment Setup

Environment variables are set via the executor's `setup_envs()` method
(`crates/libcontainer/src/workload/mod.rs`):

```rust
fn setup_envs(&self, envs: HashMap<String, String>) -> Result<()> {
    // Clear ALL host environment variables
    env::vars().for_each(|(key, _)| unsafe { env::remove_var(key) });
    // Set only the OCI spec's environment variables
    envs.iter().for_each(|(key, value)| unsafe { env::set_var(key, value) });
    Ok(())
}
```

The environment is completely replaced — no host variables leak into the
container. The env vars come from `process.env` in the OCI spec's
`config.json`.

### HOME Variable

If `HOME` is not explicitly set in the spec's environment, youki adds it
automatically by looking up the container user's home directory from the
system user database (via UID) (`process/init/process.rs`, lines 407-408,
and `utils.rs` lines 143-148).

### PATH Handling

Youki does **not** set a default PATH. The PATH must come from the OCI spec's
environment variables. The default executor validates that PATH exists and
searches it to resolve the executable (`crates/libcontainer/src/workload/default.rs`,
lines 58-65 and 115-127). If PATH is not set, executable resolution will fail.

### LISTEN_FDS (systemd socket activation)

For systemd socket activation, youki sets `LISTEN_FDS` and `LISTEN_PID`
environment variables if file descriptors for sockets are present
(`process/init/process.rs`, lines 276-310).

---

## Process Environment Setup

The init process (`container_init_process()` in `process/init/process.rs`)
sets up the container environment in this order:

| # | Action | Syscall/Mechanism |
|---|--------|-------------------|
| 1 | Create new session | `setsid()` |
| 2 | Set I/O priority & CPU scheduler | `ioprio_set()`, `sched_setattr()` |
| 3 | Connect stdio (non-terminal mode) | `dup2()` on fds 0, 1, 2 |
| 4 | Apply remaining namespaces | `unshare()` / `setns()` |
| 5 | Set no_new_privileges | `prctl(PR_SET_NO_NEW_PRIVS)` |
| 6 | Prepare rootfs mounts | `mount()` calls |
| 7 | Run create_runtime/prestart hooks | Via IPC to main process |
| 8 | Pivot root / chroot | `pivot_root()` or `chroot()` |
| 9 | Reopen /dev/null in container | `open("/dev/null")` |
| 10 | Setup console (terminal mode) | `openpty()`, `dup2()`, `ioctl(TIOCSCTTY)` |
| 11 | Set personality domain | `personality()` |
| 12 | Apply AppArmor profile | Write to `/proc/self/attr/apparmor/exec` |
| 13 | Set umask | `umask()` |
| 14 | Apply readonly/masked paths | `mount()` with `MS_RDONLY`, bind `/dev/null` |
| 15 | Remount root readonly (if specified) | `mount()` with `MS_REMOUNT|MS_RDONLY` |
| 16 | Set working directory | `chdir()` (before user switch for permission) |
| 17 | Set supplementary groups | `setgroups()` |
| 18 | Switch user/group | `setgid()`, `setuid()` |
| 19 | Handle LISTEN_FDS | Set env vars, manage FDs |
| 20 | Close leaked file descriptors | `close_range()` with `CLOEXEC` |
| 21 | Configure network devices | If specified in spec |
| 22 | Initialize seccomp filter | `seccomp()` syscall |
| 23 | Drop to spec capabilities | `capset()` |
| 24 | Retry chdir if needed | `chdir()` (after user switch) |
| 25 | Verify cwd is in container | Path validation |
| 26 | Set HOME env if missing | `env::set_var("HOME", ...)` |
| 27 | Clear host env, set spec env | `env::remove_var()` / `env::set_var()` |
| 28 | Validate executable | PATH lookup |
| 29 | Signal ready to main | IPC channel write |
| 30 | Wait for start signal | Block on notify socket |
| 31 | Run start_container hooks | In-process |
| 32 | Exec the payload | `execvp(args[0], args)` |

The `execvp()` call replaces the process image entirely. At this point all
namespaces, cgroups, capabilities, seccomp filters, and filesystem isolation
are in place.

---

## Logging

### Runtime Logging

Youki uses `tracing` + `tracing_subscriber` (`crates/youki/src/observability.rs`):

- **Default level**: `debug` in debug builds, `error` in release builds
- **Outputs**: stderr (default), file (`--log <path>`), or systemd journald
  (when `--systemd-log` is enabled)
- **Formats**: Text or JSON (`--log-format`)
- **Colors**: Disabled by default (`with_ansi(false)`)

CLI flags: `--log-level`, `--log`, `--log-format`, `--debug`

### Container Output

Container stdout/stderr is **not** captured by youki itself. The container's
stdio goes wherever the file descriptors point:
- In non-terminal mode: to the pipes/FDs provided by the container manager
- In terminal mode: through the PTY, with the master FD held by the container manager

Youki does not implement any log driver — that is the responsibility of the
higher-level container manager (Docker, containerd, etc.).

---

## Inter-Process Communication

### Channel System

Three uni-directional channels are used (`crates/libcontainer/src/process/channel.rs`),
implemented as Unix pipes with serialized messages:

```
Main ←──── main_channel ────── Intermediate / Init
Main ────── intermediate_channel ──→ Intermediate
Main ────── init_channel ──────────→ Init
```

Message types:

| Message | Direction | Purpose |
|---------|-----------|---------|
| `WriteMapping` | Intermediate → Main | Request UID/GID mapping write |
| `MappingWritten` | Main → Intermediate | Acknowledge mapping done |
| `IntermediateReady(pid)` | Intermediate → Main | Report init PID |
| `InitReady` | Init → Main | Init is configured and waiting |
| `HookRequest` | Init → Main | Request prestart hook execution |
| `HookDone` | Main → Init | Hooks completed |
| `SeccompNotify` | Init → Main | Send seccomp notify FD |
| `SeccompNotifyDone` | Main → Init | Seccomp handling complete |
| `ExecFailed(err)` | Init → Main | Report exec failure |

### Notify Socket (Start Signal)

A separate Unix domain socket (`notify.sock`) is used for the start signal:
- Created by main process as a `NotifyListener`
- Init process blocks on `wait_for_container_start()`
- When `youki start` is called, it connects to the socket and writes
  `"start container"`, unblocking the init process

This two-phase approach (create then start) is required by the OCI runtime
spec to allow the container manager to set up networking, etc. between
creation and start.

### Exec Notification

For `youki exec`, a simpler pipe-based barrier is used instead of the
notify socket:
- A pipe is created; the write end is passed to the exec process
- The write end is closed on successful setup (EOF signals success)
- If setup fails, error bytes are written to the pipe

---

## Container Lifecycle and State Machine

Container states:

```
Creating → Created → Running → Stopped
                               ↕
                             Paused
```

| State | Meaning |
|-------|---------|
| Creating | Container directory created, processes being spawned |
| Created | Init process spawned and waiting for start signal |
| Running | Start signal sent, init process executing payload |
| Stopped | Init process has exited |
| Paused | Container frozen via cgroup freezer |

### Exit Code Capture

The main process captures the exit code via `waitpid()`:

```rust
match waitpid(pid, None) {
    WaitStatus::Exited(_, status) => status,       // Normal exit
    WaitStatus::Signaled(_, sig, _) => sig as i32, // Killed by signal
}
```

---

## Exec: Injecting into a Running Container

`youki exec` (`crates/youki/src/commands/exec.rs`) uses a **tenant container**
model rather than an init container.

### Key Differences from Create

| Aspect | Create (Init) | Exec (Tenant) |
|--------|---------------|---------------|
| Namespaces | Creates new namespaces | Joins existing namespaces via `/proc/<pid>/ns/*` |
| Rootfs | Prepares mounts, does pivot_root | No pivot_root, rootfs already set up |
| Cgroup | Creates new cgroup | Joins existing cgroup |
| Hooks | Runs all lifecycle hooks | No hooks |
| Console | Sets up via console socket | Can set up its own console socket |
| PID | Becomes PID 1 in new PID namespace | Gets next available PID in existing namespace |

### Namespace Joining

The tenant builder reads the init container's namespace files from
`/proc/<init_pid>/ns/` and passes them as namespace paths in the spec.
The exec process then uses `setns()` to join each namespace.

### Spec Adaptation

`adapt_spec_for_tenant()` in `tenant_builder.rs` merges CLI arguments with
the init container's spec:
- `--env` vars override spec env, but original PATH is preserved if not
  explicitly overridden
- `--cwd` overrides spec working directory
- `--user`/`--group` override spec user
- `--cap-add`/`--cap-drop` are unioned with spec capabilities
- Command args from CLI replace spec args entirely

---

## Signal Handling

### Kill Single Process

`container_kill.rs` sends signals to the container's init process via
`kill(pid, signal)`:

```rust
signal::kill(pid, signal)
```

`ESRCH` (no such process) is treated as a non-error.

### Kill All Processes

To kill all processes in a container, youki uses the cgroup:

1. **Freeze** the cgroup (`cgroup_manager.freeze(Frozen)`) — prevents new
   processes from being created and existing processes from running
2. **Enumerate** all PIDs in the cgroup (`cgroup_manager.get_all_pids()`)
3. **Send signal** to each PID
4. **Thaw** the cgroup (`cgroup_manager.freeze(Thawed)`)

The freeze step is necessary because without it, processes could fork between
enumeration and signal delivery, escaping the kill.

### Signal Parsing

Signals can be specified as numeric (`9`), short (`KILL`), or long (`SIGKILL`)
forms (`crates/libcontainer/src/signal.rs`).

---

## OCI Lifecycle Hooks

Hooks are executed at specific points in the container lifecycle:

| Hook | Executor | When | Context |
|------|----------|------|---------|
| `createRuntime` | Main process | After namespace setup, before pivot_root | Runtime namespaces |
| `prestart` (deprecated) | Main process | Same as createRuntime | Runtime namespaces |
| `createContainer` | Init process | After namespace setup, before pivot_root | Container namespaces |
| `startContainer` | Init process | After pivot_root, after start signal | Fully jailed |
| `poststart` | Main process | After init signals ready | Runtime namespaces |
| `poststop` | Main process | After container exits | Runtime namespaces |

### Hook Execution

Each hook (`crates/libcontainer/src/hooks.rs`):
1. Spawns the hook command as a child process
2. Pipes the OCI container state as JSON to the hook's stdin
3. Waits for completion (with optional timeout)
4. On timeout, sends `SIGKILL` to the hook process
5. Non-zero exit code is treated as an error

### Hook IPC

Hooks that run in the main process but are triggered by the init process use
the channel system: init sends a `HookRequest`, main runs the hooks, main
sends `HookDone`.

---

## CLI vs Spec Config Interaction

### Create/Run

For `create` and `run`, almost all configuration comes from the OCI spec's
`config.json`. CLI arguments only control runtime behavior:

| CLI Argument | Effect |
|-------------|--------|
| `--bundle` | Path to OCI bundle containing `config.json` |
| `--pid-file` | Write container PID to this file |
| `--console-socket` | Path to console socket for terminal setup |
| `--preserve-fds` | Number of additional FDs to pass to container |
| `--no-pivot` | Use `chroot` + `MS_MOVE` instead of `pivot_root` |
| `--systemd-cgroup` | Use systemd cgroup driver |
| `--detach` / `-d` | Don't wait for container (run only) |

### Exec

For `exec`, CLI arguments can **override** the spec:

| CLI Argument | Overrides |
|-------------|-----------|
| `--env` | Merged with spec env (CLI wins on conflict) |
| `--cwd` | Replaces spec working directory |
| `--user` | Replaces spec UID |
| `--additional-gids` | Replaces spec supplementary groups |
| `--cap` | Unioned with spec capabilities |
| `--no-new-privs` | Overrides spec no_new_privileges |
| `--process` | JSON file replacing the entire process spec |
| `--detach` | Don't wait for exec process |

---

## Security Measures

### Binary Sealing (CVE-2019-5736)

On startup (`crates/youki/src/main.rs`), youki calls `pentacle::ensure_sealed()`:
- Copies itself into an anonymous `memfd`
- Seals the memfd (prevents modification)
- Re-executes from the sealed memfd
- Prevents a container process from overwriting the runtime binary via
  `/proc/self/exe`

### Non-Dumpable Flag

Before entering namespaces, the process is marked non-dumpable:
```rust
prctl::set_dumpable(false)
```
This prevents ptrace-based attacks and blocks access to `/proc/self/` of the
runtime process from within the container.

### PTY Verification (CVE-2025-52565)

When setting up a console, youki verifies that PTY devices are genuine:
- Master must be on devpts filesystem with inode 2, major:minor 5:2
- Slave must be on devpts filesystem with major 136
- Prevents container escape via device spoofing

### FD-Based Mounting

Console mounting uses `open_tree()` + `move_mount()` instead of path-based
`mount()` to prevent TOCTOU (time-of-check-time-of-use) attacks.

### File Descriptor Cleanup

Before exec, youki closes all file descriptors above stderr (except
explicitly preserved ones) using `close_range()` with `CLOEXEC`. This
prevents leaking runtime FDs into the container process.

### /dev/null Verification

When opening `/dev/null`, youki verifies it is a real character device with
major:minor 1:3, preventing device spoofing attacks.
