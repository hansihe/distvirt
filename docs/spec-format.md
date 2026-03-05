# distvirt Spec Format

The distvirt spec format is the native declarative configuration for distvirt namespaces. It replaces Docker Compose as the primary input format, exposing distvirt's full capabilities (activation, suspend/resume, protocol activators) in a natural way.

Docker Compose files remain supported via automatic conversion at `dv up` time.

---

## Design Principles

1. **Workloads and services are separate concepts.** A workload is a schedulable VM. A service is a virtual IP that routes traffic to a workload. Multiple services can point to the same workload.
2. **Services can be declared inline on workloads.** The common case (1 workload = 1 service) should be concise.
3. **Activation is first-class.** Scale-to-zero is the driving use case, not an afterthought.
4. **Fragment files for multi-repo deployments.** Each service repo declares its own workload fragment. A central namespace file assembles them.
5. **Format: YAML.** Industry standard for container orchestration specs.

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
  gateway: 172.16.0.1       # Optional, defaults to .1 of subnet

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
    healthcheck:
      tcp: { port: 8080 }
      interval: 5s
      timeout: 3s
      retries: 3
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
          activator:
            postgres: {}
          idle_timeout: 10m

  frontend:
    containers:
      - name: main
        image: docker.io/myorg/frontend:latest
    services:
      frontend:
        # No activation → always-on
```

---

## Workloads

A workload is a schedulable unit — one Firecracker VM with one or more containers.

```yaml
workloads:
  <workload-id>:
    ip: <ipv4>                  # Pod IP. Auto-assigned from subnet if omitted.
    suspend_on_idle: <bool>     # Default: false. Snapshot instead of stop.
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
    healthcheck: ...            # Readiness probe (see Health Checks).
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

Resources have two levels, following the Kubernetes requests/limits model:

- **`requests`** — Scheduling weight. The orchestrator uses these to decide placement: which worker has enough capacity for this workload. Does not enforce a hard ceiling.
- **`limits`** — VM size. Firecracker enforces these as hard ceilings. The VM gets exactly this many vCPUs and this much memory.

```yaml
resources:
  requests:
    memory_mb: 256
    vcpus: 1
  limits:
    memory_mb: 512
    vcpus: 2
```

If only `limits` is specified, `requests` defaults to equal `limits` (the pod requests what it needs). If only `requests` is specified, `limits` defaults to equal `requests` (no overcommit). If neither is specified, system defaults apply.

This split matters for overcommit: a staging namespace might request 128MB per workload (for scheduling) but limit at 512MB (in case of spikes). The orchestrator can pack more workloads per worker than the physical memory would suggest, knowing most are idle or suspended.

---

## Health Checks

Health checks gate service readiness. A workload's pod reports `PodRunning` when the VM boots and containers start, but the application inside may not be ready to serve traffic yet. Health checks bridge this gap.

When a workload has a health check, the orchestrator waits for it to pass before marking associated services as ready (which flushes buffered traffic to the backend).

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

### Probe types

- **`tcp`** — Attempts a TCP connection to the pod IP on the given port. Succeeds if the connection is established. Lightweight, works for any TCP service.
- **`http`** — Sends an HTTP GET to `http://<pod-ip>:<port><path>`. Succeeds on any 2xx status. Good for services with dedicated health endpoints.
- **`exec`** — Runs a command inside the container via the guest agent. Succeeds on exit code 0. Most flexible, but heavier — requires guest agent support.

### Interaction with activation

For activated services (scale-to-zero), health checks determine when the service transitions from `NeedBackend` to `Active`:

1. Traffic arrives → activation fires → workload demand raised
2. Pod launches (or resumes from snapshot)
3. `PodRunning` received — VM is up
4. Health check probes begin
5. Health check passes → `ServiceReady` → buffered traffic flushed

Without a health check, `PodRunning` immediately triggers `ServiceReady`. This is fine for simple services but can cause connection failures if the application takes time to initialize.

### Interaction with suspend/resume

After resume from snapshot, the application is already initialized — health checks typically pass on the first probe. The `initial_delay` can be set to `0s` (the default) since the app was already warm. This keeps resume latency minimal (~5-10ms VM restore + first probe interval).

---

## Services

A service is a virtual IP on the fabric that routes traffic to a workload. Services are the unit of activation and DNS resolution — other workloads reach a service by its name.

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
      activator: ...            # Protocol activator. Optional even with activation.
      idle_timeout: <duration>  # Time before deactivation. Required with activation.
      buffer:
        frames: <uint>          # Max buffered packets. Default: 64.
        timeout: <duration>     # Buffer timeout. Default: 30s.

    expose:
      - container_port: <uint>
        host_port: <uint>
        protocol: tcp | udp
```

### Always-on vs activated

- **No `activation` block** → always-on. Workload starts when namespace is created and stays running.
- **With `activation` block** → scale-to-zero. Workload starts only when traffic arrives, suspends/stops after idle timeout.

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

L3 packet-level. Activates on new TCP connections (SYN). Filters RSTs and stale traffic.

```yaml
activator:
  tcp:
    ports: [8080, 8443]    # Ports to watch. Omit for all ports.
    tcp_only: true          # Drop non-TCP traffic. Default: false.
    max_flows: 1024         # Flow tracking limit. Default: 1024.
```

### HTTP/2

L4 stream-level. Full H2 proxy with per-stream activation. Backend need is held active while streams are open — precise scale-to-zero without timeout guessing.

```yaml
activator:
  http2: {}
```

### PostgreSQL

L4 stream-level. Intercepts Postgres wire protocol. Can respond to health checks without waking the backend.

```yaml
activator:
  postgres: {}
```

---

## Duration Syntax

Human-readable: `30s`, `5m`, `10m`, `1h`, `500ms`.

---

## IP Auto-Assignment

When `ip` is omitted on a workload or service, the orchestrator assigns one from the namespace subnet. Explicit IPs are always allowed for deterministic setups.

Service IPs and pod IPs are drawn from the same subnet but are distinct — a service IP is a virtual address on the fabric, a pod IP is the VM's actual network interface.

---

## Multi-Repo Deployments (Fragments)

For organizations with many microservices across repos, distvirt supports a fragment-based workflow analogous to Helm values + ArgoCD.

### Per-repo: workload fragment

Each service repo contains a `distvirt.yaml` declaring its workload:

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

A fragment is a partial namespace spec. It declares one or more workloads with their inline services but has no `network` or `metadata` — those belong to the namespace definition.

### Central: namespace assembly

A state repo (analogous to an Argo state repo) has a namespace file that imports fragments:

```yaml
# state-repo/namespaces/staging.yaml
apiVersion: v1
kind: Namespace

metadata:
  name: staging

network:
  subnet: 172.16.0.0/16

defaults:
  suspend_on_idle: true
  activation:
    idle_timeout: 5m
    buffer:
      frames: 64
      timeout: 30s

include:
  - fragment: api-service
    values:
      IMAGE: docker.io/myorg/api:v1.2.3
    overrides:                        # Per-fragment overrides
      env:
        DATABASE_URL: "postgres://staging-db:5432/myapp"

  - fragment: frontend
    values:
      IMAGE: docker.io/myorg/frontend:v2.0.1

  - fragment: database
    values:
      IMAGE: docker.io/library/postgres:16
    overrides:
      activation:
        idle_timeout: 15m             # DB stays warm longer
```

### CI workflow

1. Service CI builds image, pushes to registry.
2. Service CI updates the state repo — bumps the image tag in the relevant `include` entry (same as updating a Helm values file for Argo).
3. A reconciler (built into distvirt or external) watches the state repo, renders the full namespace spec by assembling fragments, and applies via `dv apply` or the gRPC API.

### `defaults` block

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
    idle_timeout: 5m
    buffer:
      frames: 64
      timeout: 30s
```

A workload or service can override any default by specifying the field explicitly. A service with no `activation` block and a namespace default still gets activation — use `activation: none` to explicitly opt out.

### Fragment resolution

Fragments are referenced by name. Resolution is a tooling concern — fragments could be:

- Files in the same repo (`fragments/api-service.yaml`)
- OCI artifacts pulled from a registry
- Git refs from other repos
- Downloaded from an internal artifact store

The spec defines the shape, not the distribution mechanism.

---

## Mapping to Internal Types

```
YAML                              → Internal Type
────────────────────────────────────────────────────────────
metadata.name                     → NamespaceId
network.subnet                    → NetworkConfig { subnet, prefix_len }
network.gateway                   → NetworkConfig { gateway }
workloads.<id>                    → WorkloadSpec
workloads.<id>.ip                 → PodNetworkConfig { ip }
workloads.<id>.suspend_on_idle    → WorkloadSpec { suspend_on_idle }
workloads.<id>.resources.requests → Scheduling weight (orchestrator placement)
workloads.<id>.resources.limits   → VM sizing (Firecracker vCPUs + memory)
workloads.<id>.healthcheck        → Readiness probe (gates ServiceReady)
workloads.<id>.containers[]       → ContainerSpec { container_id, image_ref, config }
services.<id>                     → ServiceSpec
services.<id>.ip                  → ServiceSpec { ip }
services.<id>.activation          → ActivationSpec + ServicePolicy
services.<id>.activation.activator → ActivatorConfig::Tcp | Http2
```

---

## Comparison with Docker Compose

| Aspect | Docker Compose | distvirt spec |
|--------|---------------|---------------|
| Workload + service | Conflated as "service" | Separate concepts, inline shorthand |
| Activation | N/A | First-class per-service config |
| Suspend/resume | N/A | Per-workload `suspend_on_idle` |
| Protocol activators | N/A | TCP, HTTP/2, PostgreSQL |
| Resources | Limits only (deploy.resources) | Requests (scheduling) + limits (VM size) |
| Health checks | Basic (no readiness gating) | Readiness-gated service activation |
| Network | Implicit/multi-network | Explicit single subnet |
| Dependencies | `depends_on` ordering | Implicit via service activation + health checks |
| Multi-container | N/A | Multiple containers per workload |
| Multi-repo | N/A | Fragment files + assembly |
| Namespace defaults | N/A | `defaults` block |

---

## CLI Integration

```bash
# Deploy from native spec
dv up staging -f distvirt.yaml

# Deploy from Docker Compose (auto-converted)
dv up staging -f docker-compose.yml

# Render assembled namespace (fragments → full spec)
dv render -f namespaces/staging.yaml

# Apply partial update (merge semantics)
dv apply staging -f distvirt.yaml
```

---

## Things Outside the Spec

These are infrastructure/runtime concerns, not application spec:

- **Ingress adapters** (WireGuard, reverse proxy) — cluster-level config, managed via `dv connect` / cluster setup.
- **Worker placement** — scheduler decides, not the spec.
- **Storage pools** — infrastructure config.
- **Splice** — runtime debugging operation (`dv splice`).
- **Inter-worker tunneling** — transparent infrastructure.
