# CLI Design

## Overview

The `dv` CLI is the primary user interface for interacting with the distvirt orchestrator. It has two layers:

1. **Layer 1 — Task-oriented commands**: Opinionated, smart defaults, summarization. What you use 90% of the time.
2. **Layer 2 — Uniform resource commands**: Systematic, predictable, scriptable. The escape hatch for power users and scripting.

---

## Addressing

```
<namespace>                              # namespace
<namespace>/<workload>                   # workload within namespace
<namespace>/<resource-type>/<name>       # layer 2, when disambiguation needed
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

```
dv login                          # prompt for server + token
dv login --token <api-key>        # non-interactive, current server
dv login --server <host:port>     # set server + prompt for token
dv login --server <host:port> --token <api-key>
```

Creates or updates a context in the credentials file. First login creates the `default` context.

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

  Run `dv login --server <host:port>` to get started.
```

---

## Layer 1 — Task-Oriented Commands

### `dv up`

Deploy or update a namespace from a compose file or spec.

```
dv up -f docker-compose.yml -n <namespace>
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
Namespace: myapp (active)  Workers: 3  Capacity: 72%
Ingress: wireguard (connected via dv connect)

Workloads: 189 total — 12 running, 177 dormant
Services: 247 total — 15 active, 232 idle

  Running workloads:
  WORKLOAD     STATE     POD        WORKER        SERVICES
  api          running   pod-3a1f   worker-east   grpc(active) graphql(idle) health(active)
  postgres     running   pod-7b2c   worker-east   pg(active)
  web          running   pod-9e4d   worker-west   http(active) ws(active)
  ... (9 more)

  ⚠ 0 workloads need attention
```

At scale (hundreds of entities), status summarizes totals and shows only running/problematic workloads by default. `--all` to see everything.

Capacity is surfaced here so users can see compute pressure without a separate command.

**Workload detail:**

```
$ dv status myapp/api
Workload: api (running)
  Pod: pod-3a1f on worker-east
  Image: myapp/api:v1.2
  Demand: 2 services
  Reachable: api.myapp.dv / 10.0.1.5

  Services:
    SERVICE   STATE    ACTIVATION   BACKEND NEED   IDLE
    grpc      active   tcp:9090     active         —
    graphql   idle     tcp:4000     —              idle 3m12s
    health    active   tcp:8080     traffic        —
```

Shows the workload's compute state and all services attached to it. The `Reachable` line appears when you have an active `dv connect` session to the namespace.

### `dv logs`

Stream logs from a workload's pod.

```
dv logs <namespace>/<workload>
dv logs <namespace>/<workload> -f        # follow
```

### `dv events`

Activity stream showing state transitions across the namespace. Shows how activation ripples through — traffic hits a service, demands the workload, workload launches, all services on that workload get backends, then idle cascading back down.

```
$ dv events myapp -f
12:03:41  service/graphql    activated (first traffic)
12:03:41  workload/api       demand up → waiting for capacity
12:03:42  workload/api       pod-3a1f launching on worker-east
12:03:43  workload/api       pod-3a1f running
12:03:43  service/graphql    backend ready → active
12:03:43  service/grpc       backend ready → active (shared workload)
12:08:55  service/graphql    backend_need=none, idle timer started (5m)
12:13:55  service/graphql    idle timeout fired → idle
12:13:55  workload/api       demand down → dormant, pod stopped
```

Without `-f`, shows recent history. With `-f`, live stream.

Filters: `--workload <name>`, `--service <name>`.

### `dv connect`

Connect your local machine to a namespace's network via WireGuard. Runs in the foreground, holds the tunnel open. After connecting, all services in the namespace are reachable by IP and DNS name.

```
$ dv connect myapp
Connecting to myapp via WireGuard...
  ✓ Obtained config from orchestrator
  ✓ WireGuard tunnel established
  ✓ Routes configured for 10.0.1.0/24
  ✓ DNS configured: *.myapp.dv

  Namespace subnet: 10.0.1.0/24
  Reachable services:
    api        10.0.1.5   api.myapp.dv
    postgres   10.0.1.8   postgres.myapp.dv
    web        10.0.1.12  web.myapp.dv

Connected. Ctrl+C to disconnect.
```

```
dv connect <namespace>                # auto-connect (default)
dv connect <namespace> --config       # emit wg-quick config file, don't connect
dv connect <namespace> --qr           # QR code for mobile WireGuard clients
dv disconnect <namespace>             # tear down from another terminal
```

`dv connect` is read-only access — you can reach services in the namespace, but you don't take over any workload's identity. See `dv splice` for that.

**Implementation**: The CLI embeds a userspace WireGuard implementation ([boringtun](https://github.com/cloudflare/boringtun)) — no external WireGuard tooling required. The orchestrator provides key material and endpoint config. The CLI handles tunnel device creation, route configuration, and DNS setup.

Requires elevated privileges for tunnel creation. The CLI prompts for sudo when needed.

**DNS**: Each connected namespace gets a DNS domain `<namespace>.dv`. Service names resolve through a lightweight DNS resolver embedded in the CLI process.

- macOS: scoped resolver via `/etc/resolver/<namespace>.dv` — only affects the namespace domain, no system-wide DNS changes.
- Linux: `systemd-resolved` or `/etc/resolver/` depending on distro.

Multiple simultaneous connections work — each gets a separate domain (`myapp.dv`, `staging.dv`) with no conflicts.

**Platform-specific surface**: The WireGuard protocol (boringtun) is cross-platform. Only three concerns need platform code: tunnel device creation (`utun` on macOS, `/dev/net/tun` on Linux), route manipulation (`route` vs `ip route`), and DNS resolver configuration.

**Escape hatches**: `--config` and `--qr` output standard WireGuard config without touching the system. Useful for corporate MDM environments, VPN conflicts, or platforms where the built-in tunnel doesn't work.

### `dv splice`

> **Status:** Deferred. Will build on the same tunnel infrastructure as `dv connect`. Design TBD.

Splice a workload to the local machine — take over its identity in the namespace so other services route to your local process. Unlike `dv connect` (read-only access), splice makes you *be* the workload.

### `dv clone`

Clone a namespace. Creates a copy of the spec with all services set to activation-enabled (scale-to-zero). Everything starts dormant and activates on demand.

```
dv clone <namespace> --as <name>
```

---

## Layer 2 — Uniform Resource Commands

Systematic access to all resource types. Predictable, scriptable. Every resource type works the same way.

### Resource types

- `service` — activation, routing, backend config
- `workload` — pod lifecycle, compute
- `worker` — infrastructure nodes
- `pod` — running pod instances
- `adapter` — ingress adapters (WireGuard, reverse proxy, etc.)

### Commands

```
dv get <resource-type> -n <namespace>
dv describe <resource-type> <namespace>/<name>
dv create <resource-type> -n <namespace> [flags or -f file]
dv delete <resource-type> <namespace>/<name>
```

All support `-o json` for machine-readable output.

### Examples

```
dv get services -n myapp
dv get workloads -n myapp
dv get workers
dv get pods -n myapp
dv get adapters
dv describe service myapp/grpc
dv describe workload myapp/api
dv describe adapter wireguard
dv get services -n myapp -o json
```

---

## Design Principles

- **Workload-centric**: Users think about workloads. Services are visible on workloads, not the other way around.
- **Summarize at scale**: Hundreds of entities is normal. Default views show running/problematic items, summarize the rest.
- **Capacity visible where relevant**: Compute pressure is shown in `dv status`, not hidden behind a separate command.
- **Layer 1 for humans, layer 2 for scripts**: Task-oriented commands have opinionated formatting. Resource commands have `-o json`.
- **Explicit over implicit**: Namespace is always specified. No hidden state to get wrong.
- **Auth stays out of the way**: Login once, forget about it. Clean escape hatches for multi-environment (`dv context`) and CI (`DV_TOKEN`).

---

## Deferred

- `dv splice` implementation — builds on `dv connect` tunnel infrastructure, design TBD
- Directory-based namespace context (`.distvirt` file written by `dv up`, future commands in that directory default to the namespace)
- `dv set` for inline spec mutations (`dv set myapp/api --image foo:latest`)
- Worker management commands beyond `dv get workers`
- TUI / live-updating watch mode
- Event type filtering (`--type activation,lifecycle,...`)
- Namespace-scoped tokens (protocol currently global-only)
- Token creation/rotation commands (tokens are managed out-of-band for now)
