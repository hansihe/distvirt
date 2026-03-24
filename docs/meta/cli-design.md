---
title: "CLI Design"
---

## Overview

The `dv` CLI is the primary user interface for interacting with the distvirt orchestrator. It has two layers:

1. **Layer 1 — Task-oriented commands**: Opinionated, smart defaults, summarization. What you use 90% of the time.
2. **Layer 2 — Uniform resource commands**: Systematic, predictable, scriptable. The escape hatch for power users and scripting.

---

## Addressing

```
<namespace>                              # namespace
<namespace>/<workload>                   # workload within namespace
```

Workloads are the primary user-facing entity. Users think about workloads as "their things" — the api, the database, the background worker. Services (activation, routing) are visible *on* workloads in status views.

Namespaces are always explicit — no implicit namespace context. Convenience (directory-based defaults, etc.) can be added later.

---

## Configuration & Authentication

### Credentials

The CLI authenticates to the orchestrator using API key tokens sent as bearer tokens in gRPC metadata. Credentials are stored in `~/.config/distvirt/credentials.toml`, organized by named context:

```toml
current_context = "default"

[contexts.default]
server = "localhost:9090"
token = "dv_tok_abc123"

[contexts.prod]
server = "prod.example.com:9090"
token = "dv_tok_xyz789"
tls = true
```

### `dv login`

Login is non-interactive. Both `--server` and `--token` are required flags:

```
dv login --server <host:port> --token <api-key>
```

Creates or updates the current context in the credentials file. First login creates the `default` context.

### `dv context`

```
dv context                        # show current context
dv context use <name>             # switch active context
dv context list                   # list all contexts
dv context delete <name>          # remove a context
```

### Resolution precedence

1. `--server` / `--token` flags on the command (highest)
2. `DV_SERVER` / `DV_TOKEN` environment variables
3. Active context in `~/.config/distvirt/credentials.toml`
4. Default server `http://[::1]:9090` (no default token)

Environment variables are the primary mechanism for CI/automation — no `dv login` needed.

### Global flags

All commands accept these connection overrides:

```
--server <host:port>              # override server address
--token <api-key>                 # override auth token
--context <name>                  # use a specific named context
```

### Error messages

When auth fails, errors are specific and actionable:

```
$ dv status myapp
Error: authentication failed — token rejected by server

  Run `dv login` to authenticate, or set DV_TOKEN.
```

```
$ dv status myapp
Error: no credentials configured

  Run `dv login --server <host:port> --token <api-key>` to get started.
```

---

## Layer 1 — Task-Oriented Commands

### `dv up`

Deploy or update a namespace from a spec file. Supports native distvirt specs (`distvirt.yaml`, `distvirt.yml`) and Docker Compose files (`docker-compose.yml`). If no `-f` flag is given, looks for these files in the current directory in that order.

If the namespace already exists, the spec is updated rather than rejected.

```
dv up <namespace>                         # auto-detect spec file in cwd
dv up <namespace> -f docker-compose.yml   # explicit file
dv up -f distvirt.yaml                    # namespace ID from spec metadata.name
```

### `dv render`

Render a spec file to its resolved proto representation locally, without connecting to the server. Useful for debugging spec parsing.

```
dv render -f <spec-file>
```

### `dv down`

Tear down a namespace.

```
dv down <namespace>
```

### `dv status`

Smart overview that scales from namespace level down to individual workload detail.

**Namespace overview:**

```
$ dv status myapp
```

Shows workload and service summaries for the namespace.

**Workload detail:**

```
$ dv status myapp/api
```

Shows the workload's state, whether it is spliced, and lists all services attached to it.

### `dv logs`

Stream logs from a namespace. Takes the namespace ID as a positional argument, with an optional `--workload` flag to filter to a specific workload.

```
dv logs <namespace>
dv logs <namespace> --workload <name>
dv logs <namespace> --workload <name> -f        # follow
```

### `dv events`

Activity stream showing state transitions across the namespace.

```
dv events <namespace>
dv events <namespace> -f                        # follow (live stream)
```

Filters: `--workload <name>` (repeatable), `--service <name>` (repeatable).

```
dv events myapp --workload api --workload web -f
```

### `dv deactivate`

Hint the orchestrator to deactivate a workload immediately, skipping the idle timeout. Only takes effect if the workload has no active demand — if any service on the workload has recent activation signals, the hint is ignored and the workload stays running.

This is useful during development to test scale-to-zero behavior without waiting for idle timeouts to expire.

```
dv deactivate <namespace>/<workload>
```

```
$ dv deactivate myapp/api
Workload api deactivated — pod stopping, services returning to idle.

$ dv deactivate myapp/api
Workload api has active demand — not deactivating.
```

### `dv connect`

Connect your local machine to a namespace's network via WireGuard. Runs in the foreground, holds the tunnel open. After connecting, services in the namespace are reachable by IP.

```
dv connect <namespace>                # establish tunnel (foreground)
dv connect <namespace> --config       # emit wg-quick config file, don't connect
dv disconnect <namespace>             # tear down from another terminal
```

> **Planned:** `--qr` flag for generating QR codes for mobile WireGuard clients.

`dv connect` is read-only access — you can reach services in the namespace, but you don't take over any workload's identity. See `dv splice` for that.

**Implementation**: The CLI embeds a userspace WireGuard implementation ([boringtun](https://github.com/cloudflare/boringtun)) — no external WireGuard tooling required. The orchestrator provides key material and endpoint config via `ConnectNetwork` gRPC. The CLI handles TUN device creation, route configuration, and packet forwarding.

Active connections are tracked via state files in `~/.config/distvirt/connections/`, allowing `dv disconnect` to find and terminate tunnel processes from another terminal.

Requires elevated privileges for tunnel creation.

**Escape hatch**: `--config` outputs standard wg-quick config without touching the system. Useful for corporate MDM environments, VPN conflicts, or platforms where the built-in tunnel doesn't work.

### `dv splice`

Splice a workload to a local worker — takes over its identity in the namespace so other services route to your local process. Unlike `dv connect` (read-only access), splice makes you *be* the workload.

```
dv splice <namespace> <workload> <worker-id>
```

Runs in the foreground. Press Ctrl+C to unsplice.

### `dv attach`

Attach to the stdin/stdout/stderr of a running workload's command process. Runs in the foreground, forwarding I/O until detached.

```
dv attach <namespace>/<workload>
```

Detach with `Ctrl-P Ctrl-Q` (configurable via `--detach-keys`). Closing the terminal or pressing `Ctrl-C` sends the signal to the container process (same as Docker behavior).

Whether the session is a TTY depends on the workload's container spec (`tty: true`), not a CLI flag — the PTY is allocated at process launch. The CLI automatically enters raw mode and forwards terminal resizes when the server reports a TTY session.

```
dv attach myapp/api                     # interactive attach
dv attach myapp/api --detach-keys ctrl-] # custom detach sequence
```

Multiple clients can attach simultaneously — stdout/stderr are broadcast to all, stdin is delivered to one (last attach wins).

**Implementation**: The guest-init agent holds the command process's stdio file descriptors (PTY master FD or pipe FDs, depending on `tty` in the container spec) and multiplexes them over the host communication channel. The worker proxies streams between guest-init and the orchestrator. The orchestrator exposes a bidirectional gRPC streaming RPC (`AttachWorkload`) that the CLI connects to. The CLI sets the local terminal to raw mode and handles detach key detection.

### `dv clone`

Clone a namespace. Creates a copy of the spec with all services set to activation-enabled (scale-to-zero). Everything starts dormant and activates on demand.

```
dv clone <source> <target>
```

---

## Layer 2 — Uniform Resource Commands

Systematic access to all resource types. Predictable, scriptable. Every resource type works the same way.

### Resource types

The following resource types are implemented:

- `namespaces` (aliases: `namespace`, `ns`) — namespace lifecycle
- `workers` (alias: `worker`) — infrastructure nodes
- `pods` (alias: `pod`) — running pod instances (namespace-scoped)
- `services` (aliases: `service`, `svc`) — activation, routing, backend config (namespace-scoped)
- `workloads` (alias: `workload`) — recognized in normalization but not yet wired to list/describe

### Commands

```
dv get <resource-type> [-n <namespace>]
dv describe <resource-type> <name>
dv create <resource-type>             # not yet implemented, defers to `dv up`
dv delete <resource-type> <name>      # implemented for namespaces only
```

All `get` and `describe` commands support `-o json` for machine-readable output.

`describe` is currently implemented for `namespaces` and `workers`.

### Examples

```
dv get namespaces                     # list all namespaces
dv get workers                        # list all workers
dv get pods -n myapp                  # list pods (requires -n)
dv get services -n myapp              # list services (requires -n)
dv describe namespace myapp           # detailed namespace status
dv describe worker worker-east        # detailed worker info
dv get services -n myapp -o json      # JSON output
dv delete namespace myapp             # delete a namespace
```

---

## Design Principles

- **Workload-centric**: Users think about workloads. Services are visible on workloads, not the other way around.
- **Summarize at scale**: Hundreds of entities is normal. Default views show running/problematic items, summarize the rest.
- **Layer 1 for humans, layer 2 for scripts**: Task-oriented commands have opinionated formatting. Resource commands have `-o json`.
- **Explicit over implicit**: Namespace is always specified. No hidden state to get wrong.
- **Auth stays out of the way**: Login once, forget about it. Clean escape hatches for multi-environment (`dv context`) and CI (`DV_TOKEN`).

---

## Deferred / Planned

- `dv connect --qr` — QR code output for mobile WireGuard clients
- `dv create` for resource types beyond namespaces
- `dv describe` for pods, services, workloads
- `dv delete` for resource types beyond namespaces
- `dv exec <namespace>/<workload> -- <command>` — spawn a new process inside a running workload's container (requires guest-init process spawning support)
- Directory-based namespace context (`.distvirt` file written by `dv up`, future commands in that directory default to the namespace)
- `dv set` for inline spec mutations (`dv set myapp/api --image foo:latest`)
- Worker management commands beyond `dv get workers`
- DNS integration for `dv connect` (scoped resolvers for `<namespace>.dv` domains)
- TUI / live-updating watch mode
- Event type filtering (`--type activation,lifecycle,...`)
- Namespace-scoped tokens (protocol currently global-only)
- Token creation/rotation commands (tokens are managed out-of-band for now)
