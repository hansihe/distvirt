---
title: "distvirt Spec Format"
---

The distvirt spec format is the native declarative configuration for distvirt namespaces. It replaces Docker Compose as the primary input format, exposing distvirt's full capabilities (activation, suspend/resume, protocol activators) in a natural way.

Docker Compose files remain supported via automatic conversion at `dv up` time.

### Implementation status legend

Throughout this document, features are marked with their implementation status:

- **[Implemented]** — Parsed, sent over the client protocol, and acted upon by the orchestrator.
- **[Parsed, ignored]** — The YAML field is parsed without error, but the value is silently dropped (usually with a log warning). Listed so users know the field will not take effect.
- **[Planned]** — Not implemented. Described here for future direction only.

---

## Design Principles

1. **Workloads and services are separate concepts.** A workload is a schedulable VM. A service is a virtual IP that routes traffic to a workload. Multiple services can point to the same workload.
2. **Services can be declared inline on workloads.** The common case (1 workload = 1 service) should be concise.
3. **Activation is first-class.** Scale-to-zero is the driving use case, not an afterthought.
4. **Format: YAML.** Industry standard for container orchestration specs.

---

## Namespace Spec

A complete namespace spec declares the network, workloads, and optionally top-level services:

```yaml
apiVersion: v1
kind: Namespace

metadata:
  name: my-staging-env

network:
  subnet: 172.16.0.0/24

workloads:
  api:
    containers:
      - name: main
        image: docker.io/myorg/api:latest
        entrypoint: ["/app/server"]
        args: ["--port", "8080"]
        env:
          DATABASE_URL: "postgres://db:5432/myapp"
          LOG_LEVEL: "info"
        working_dir: /app
        user: "1000:1000"
        hostname: api
    resources:
      requests:
        memory_mb: 256
        vcpus: 1
      limits:
        memory_mb: 512
        vcpus: 2
    services:
      api:
        activation:
          activator:
            tcp: { ports: [8080] }
          idle_timeout: 5m

  database:
    containers:
      - name: main
        image: docker.io/library/postgres:16
        env:
          POSTGRES_PASSWORD: "dev"
    services:
      database:
        activation:
          idle_timeout: 10m

  frontend:
    containers:
      - name: main
        image: docker.io/myorg/frontend:latest
    services:
      frontend:
        # No activation -> always-on
```

---

## Workloads

**[Implemented]**

A workload is a schedulable unit -- one Firecracker VM with one or more containers.

```yaml
workloads:
  <workload-id>:
    ip: <ipv4>                  # Pod IP. Auto-assigned from subnet if omitted.
    suspend_on_idle: <bool>     # Default: true (from defaults). Snapshot instead of stop.
    containers:
      - name: <string>         # Container ID within pod. Default: "main".
        image: <oci-ref>       # Required. OCI image reference.
        entrypoint: [<args>]   # Override image entrypoint.
        args: [<args>]         # Override image CMD.
        env: {<map>}           # KEY: VALUE environment variables.
        working_dir: <path>    # Working directory.
        user: "<uid>[:<gid>]"  # Run as user/group.
        hostname: <string>     # Container hostname.
    resources:
      requests:                 # Scheduling weight. What the orchestrator uses for placement.
        memory_mb: <uint>
        vcpus: <uint>
      limits:                   # VM size. Hard ceiling enforced by Firecracker.
        memory_mb: <uint>
        vcpus: <uint>
    healthcheck: ...            # [Parsed, ignored] See Health Checks.
    services:                   # Inline service declarations (see Services).
      <service-id>: ...
```

### Minimal workload

```yaml
workloads:
  api:
    containers:
      - image: docker.io/myorg/api:latest
```

No explicit IP, no services, single unnamed container. The orchestrator auto-assigns an IP from the namespace subnet.

---

## Resources

**[Implemented]**

Resources have two levels, following the Kubernetes requests/limits model:

- **`requests`** -- Scheduling weight. The orchestrator uses these to decide placement: which worker has enough capacity for this workload. Does not enforce a hard ceiling.
- **`limits`** -- VM size. Firecracker enforces these as hard ceilings. The VM gets exactly this many vCPUs and this much memory.

```yaml
resources:
  requests:
    memory_mb: 256
    vcpus: 1
  limits:
    memory_mb: 512
    vcpus: 2
```

`requests` and `limits` are independent — if only one is specified, the other is left unset (zero values). If neither is specified, system defaults apply.

Resource defaults can be set at the namespace level via the `defaults` block (see Defaults).

---

## Health Checks

**[Parsed, ignored]** -- The `healthcheck` field is accepted in the YAML without parse errors (it is stored as raw YAML), but its value is not sent over the client protocol and has no effect. A log warning is emitted when a healthcheck is present.

The intended design is documented below for future implementation.

When implemented, health checks will gate service readiness. A workload's pod reports `PodRunning` when the VM boots and containers start, but the application inside may not be ready to serve traffic yet.

```yaml
healthcheck:
  # Probe type (exactly one):
  tcp: { port: 8080 }                          # TCP connect succeeds
  http: { port: 8080, path: /healthz }         # HTTP 2xx response
  exec: { command: ["pg_isready", "-U", "postgres"] }  # Exit code 0

  # Timing:
  interval: 5s          # Time between probes. Default: 5s.
  timeout: 3s           # Probe timeout. Default: 3s.
  retries: 3            # Consecutive failures before unhealthy. Default: 3.
  initial_delay: 0s     # Delay before first probe. Default: 0s.
```

Currently, `PodRunning` immediately triggers `ServiceReady` for all workloads regardless of whether a healthcheck is specified.

---

## Services

**[Implemented]**

A service is a virtual IP on the fabric that routes traffic to a workload. Services are the unit of activation and DNS resolution -- other workloads reach a service by its name.

### Inline declaration (common case)

Services declared under `workloads.<id>.services` are automatically linked to that workload:

```yaml
workloads:
  api:
    containers:
      - image: docker.io/myorg/api:latest
    services:
      api:
        activation:
          activator:
            tcp: { ports: [8080] }
          idle_timeout: 5m
```

The service IP is auto-assigned from the subnet. The workload linkage is implicit.

### Top-level declaration (advanced)

For cases where services don't map 1:1 to workloads, declare them at the top level:

```yaml
services:
  <service-id>:
    workload: <workload-id>     # Required at top level.
    ip: <ipv4>                  # Auto-assigned if omitted.
    activation: ...             # See Activation.
    expose: ...                 # See Port Exposure.
```

### Service fields

```yaml
services:
  <service-id>:
    workload: <workload-id>     # Implicit when inline, required at top level.
    ip: <ipv4>                  # Service virtual IP. Auto-assigned if omitted.

    activation:                 # If omitted, service is always-on.
      activator: ...            # Protocol activator (see Protocol Activators).
      idle_timeout: <duration>  # Time before deactivation. Default: 30s when activation is present.
      buffer:                   # [Parsed, ignored] See note below.
        frames: <uint>
        timeout: <duration>

    expose:
      - container_port: <uint>
        host_port: <uint>
        protocol: tcp | udp
```

> **Note on `buffer`:** The `buffer.frames` and `buffer.timeout` fields are parsed without error but silently ignored. The `ServicePolicy` proto message exists but is empty -- these fields have no effect. A log warning is emitted.

### Always-on vs activated

- **No `activation` block** -- always-on. Workload starts when namespace is created and stays running.
- **With `activation` block** -- scale-to-zero. Workload starts only when traffic arrives, suspends/stops after idle timeout.

### Activation without a protocol activator

```yaml
services:
  simple:
    activation:
      idle_timeout: 5m
```

Buffers all packets and activates on the first one (passthrough mode). No protocol awareness.

---

## Protocol Activators

Protocol activators inspect traffic to make intelligent activation decisions (e.g., activate on TCP SYN, not RST or stale keepalives).

### TCP

**[Implemented]**

L3 packet-level. Activates on new TCP connections (SYN). Filters RSTs and stale traffic.

```yaml
activator:
  tcp:
    ports: [8080, 8443]    # Ports to watch. Omit for all ports.
```

> **Note:** The fields `tcp_only` and `max_flows` that may appear in older documentation are not supported in the client protocol. They are not parsed and will cause a YAML deserialization error if present.

### HTTP/2

**[Implemented]**

L4 stream-level. Full H2 proxy with per-stream activation. Backend need is held active while streams are open -- precise scale-to-zero without timeout guessing.

```yaml
activator:
  http2: {}
```

### PostgreSQL

**[Parsed, ignored]** -- The `postgres: {}` activator is accepted in the YAML but is not supported in the client protocol (`ActivatorConfig` only has `tcp` and `http2` variants). When present, a log warning is emitted and the activator is dropped -- the service will behave as if no activator was specified (passthrough activation).

```yaml
activator:
  postgres: {}    # Parsed but ignored -- activator is dropped
```

---

## Duration Syntax

**[Implemented]**

Human-readable: `30s`, `5m`, `10m`, `1h`, `500ms`.

---

## IP Auto-Assignment

**[Implemented]**

When `ip` is omitted on a workload or service, the orchestrator assigns one from the namespace subnet using deterministic name-based hashing (FNV-1a). This means IPs are stable across spec updates -- adding or removing a workload does not change the IPs of other workloads.

Explicit IPs are reserved first, then auto-assigned names get a slot via hash-based probing.

Service IPs and pod IPs are drawn from the same subnet but are distinct -- a service IP is a virtual address on the fabric, a pod IP is the VM's actual network interface. The `.0` (network) and `.1` (gateway) addresses are reserved automatically.

---

## Network Configuration

**[Partially implemented]**

```yaml
network:
  subnet: 172.16.0.0/24       # [Implemented] Subnet CIDR for the namespace.
  gateway: 172.16.0.1          # [Parsed, ignored] Not in client protocol.
```

The `gateway` field is parsed but not sent to the orchestrator (the `NetworkConfig` proto only contains `subnet`). A log warning is emitted.

---

## Defaults

**[Implemented]**

Namespace-level defaults applied to all workloads/services unless overridden:

```yaml
defaults:
  suspend_on_idle: true               # All workloads suspend by default
  resources:                          # Default resource requests/limits
    requests:
      memory_mb: 128
      vcpus: 1
    limits:
      memory_mb: 512
      vcpus: 2
  activation:                         # Default activation for all services
    activator:
      tcp: { ports: [80] }
    idle_timeout: 5m
```

A workload or service can override any default by specifying the field explicitly. An inline service with an empty body (`service_name: {}`) inherits the default activation if one is set.

Note: when no `defaults.suspend_on_idle` is specified, the parser defaults to `true`.

---

## Mapping to Internal Types

```
YAML                              -> Proto Type                    Status
-----------------------------------------------------------------------
metadata.name                     -> namespace_id (string)         Implemented
network.subnet                    -> NetworkConfig.subnet          Implemented
network.gateway                   -> (dropped)                     Parsed, ignored
workloads.<id>                    -> WorkloadSpec                  Implemented
workloads.<id>.ip                 -> PodNetworkConfig.ip           Implemented
workloads.<id>.suspend_on_idle    -> WorkloadSpec.suspend_on_idle  Implemented
workloads.<id>.resources.requests -> ResourceRequirements.requests Implemented
workloads.<id>.resources.limits   -> ResourceRequirements.limits   Implemented
workloads.<id>.healthcheck        -> (dropped)                     Parsed, ignored
workloads.<id>.containers[]       -> ContainerSpec                 Implemented
services.<id>                     -> ServiceSpec                   Implemented
services.<id>.ip                  -> ServiceNetworkConfig.ip       Implemented
services.<id>.activation          -> ActivationSpec                Implemented
  .activator.tcp                  -> ActivatorConfig::Tcp          Implemented
  .activator.http2                -> ActivatorConfig::Http2        Implemented
  .activator.postgres             -> (dropped)                     Parsed, ignored
  .buffer.frames / .timeout       -> (dropped)                     Parsed, ignored
services.<id>.expose              -> ExposeSpec                    Implemented
```

---

## Comparison with Docker Compose

| Aspect | Docker Compose | distvirt spec |
|--------|---------------|---------------|
| Workload + service | Conflated as "service" | Separate concepts, inline shorthand |
| Activation | N/A | First-class per-service config |
| Suspend/resume | N/A | Per-workload `suspend_on_idle` |
| Protocol activators | N/A | TCP, HTTP/2 (PostgreSQL planned) |
| Resources | Limits only (deploy.resources) | Requests (scheduling) + limits (VM size) |
| Health checks | Basic (no readiness gating) | Planned (parsed but not yet functional) |
| Network | Implicit/multi-network | Explicit single subnet |
| Dependencies | `depends_on` ordering | Implicit via service activation |
| Multi-container | N/A | Multiple containers per workload |
| Namespace defaults | N/A | `defaults` block |

---

## CLI Integration

```bash
# Deploy from native spec
dv up staging -f distvirt.yaml

# Deploy from Docker Compose (auto-converted)
dv up staging -f docker-compose.yml
```

---

## Things Outside the Spec

These are infrastructure/runtime concerns, not application spec:

- **Ingress adapters** (WireGuard, reverse proxy) -- cluster-level config, managed via `dv connect` / cluster setup.
- **Worker placement** -- scheduler decides, not the spec.
- **Storage pools** -- infrastructure config.
- **Splice** -- runtime debugging operation (`dv splice`).
- **Inter-worker tunneling** -- transparent infrastructure.

---

## Planned: Multi-Repo Deployments (Fragments)

> **[Planned] -- Not implemented.** The `SpecFile` struct has no `include` field. There is no `WorkloadFragment` assembly logic. The parser recognizes `kind: WorkloadFragment` in the YAML probe (it won't reject the file), but no fragment merging, variable substitution, or include resolution exists. Everything below describes future design intent.

For organizations with many microservices across repos, distvirt will support a fragment-based workflow analogous to Helm values + ArgoCD.

### Per-repo: workload fragment

Each service repo would contain a `distvirt.yaml` declaring its workload:

```yaml
# api-service/distvirt.yaml
apiVersion: v1
kind: WorkloadFragment

workloads:
  api:
    containers:
      - image: ${IMAGE}
        args: ["--port", "8080"]
        env:
          LOG_LEVEL: "info"
    services:
      api:
        activation:
          activator:
            tcp: { ports: [8080] }
          idle_timeout: 5m
```

A fragment is a partial namespace spec. It declares one or more workloads with their inline services but has no `network` or `metadata` -- those belong to the namespace definition.

### Central: namespace assembly

A state repo (analogous to an Argo state repo) would have a namespace file that imports fragments:

```yaml
# state-repo/namespaces/staging.yaml
apiVersion: v1
kind: Namespace

metadata:
  name: staging

network:
  subnet: 172.16.0.0/16

include:
  - fragment: api-service
    values:
      IMAGE: docker.io/myorg/api:v1.2.3
    overrides:
      env:
        DATABASE_URL: "postgres://staging-db:5432/myapp"

  - fragment: frontend
    values:
      IMAGE: docker.io/myorg/frontend:v2.0.1
```

### Fragment resolution

Fragments would be referenced by name. Resolution is a tooling concern -- fragments could be:

- Files in the same repo (`fragments/api-service.yaml`)
- OCI artifacts pulled from a registry
- Git refs from other repos
- Downloaded from an internal artifact store

### CI workflow

1. Service CI builds image, pushes to registry.
2. Service CI updates the state repo -- bumps the image tag in the relevant `include` entry.
3. A reconciler watches the state repo, renders the full namespace spec by assembling fragments, and applies via `dv apply` or the gRPC API.
