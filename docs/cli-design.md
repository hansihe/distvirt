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

  Services:
    SERVICE   STATE    ACTIVATION   BACKEND NEED   IDLE
    grpc      active   tcp:9090     active         —
    graphql   idle     tcp:4000     —              idle 3m12s
    health    active   tcp:8080     traffic        —
```

Shows the workload's compute state and all services attached to it.

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

### `dv splice`

Splice a workload to the local machine. Runs in the foreground, holds the splice open. Sets up network plumbing so the local process can reach other services in the namespace and other services can reach it.

```
$ dv splice myapp/api
Splicing into myapp as api...
  ✓ Connected to orchestrator
  ✓ Fabric tunnel established
  ✓ Network configured: api is 10.0.1.5
    Other services reachable at their namespace IPs
    Traffic to api (from other services) routes to localhost

Run your service locally — it can reach other services
and other services can reach it.

  Namespace subnet: 10.0.1.0/24
  Your IP:          10.0.1.5

Ctrl+C to unsplice.
```

The local process is not containerized. Splice is about network access, not compute isolation.

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
dv describe service myapp/grpc
dv describe workload myapp/api
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

- Directory-based namespace context (`.distvirt` file written by `dv up`, future commands in that directory default to the namespace)
- `dv set` for inline spec mutations (`dv set myapp/api --image foo:latest`)
- Worker management commands beyond `dv get workers`
- TUI / live-updating watch mode
- Event type filtering (`--type activation,lifecycle,...`)
- Namespace-scoped tokens (protocol currently global-only)
- Token creation/rotation commands (tokens are managed out-of-band for now)
