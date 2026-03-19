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
3. **Always-on by default, activation is opt-in.** Workloads run by default. Scale-to-zero is available as an opt-in per-workload or per-service, with increasing levels of protocol awareness (passthrough → TCP → HTTP/2).
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
        volume_mounts:
          - name: cache
            mount_path: /var/cache/app
    resources:
      requests:
        memory_mb: 256
        vcpus: 1
      limits:
        memory_mb: 512
        vcpus: 2
    volumes:
      - name: cache
        empty_dir: {}
    services:
      api:
        activation:
          tcp:
            ports: [8080]
            idle_timeout: 5m

  database:
    containers:
      - name: main
        image: docker.io/library/postgres:16
        env:
          POSTGRES_PASSWORD: "dev"
    activation:                     # Workload-level passthrough activation
      passthrough:
        idle_timeout: 10m

  frontend:
    containers:
      - name: main
        image: docker.io/myorg/frontend:latest
    services:
      frontend: {}                # Always-on (no activation)
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
    activation:                  # Workload-level activation. If omitted, always-on.
      passthrough:               # Only passthrough is valid on workloads.
        idle_timeout: <duration>
    volumes:                    # Pod-scoped volume definitions. See Volumes.
      - name: <string>         # Volume name. Referenced by volume_mounts.
        empty_dir: {}          # Volume type (exactly one). See Volumes.
    containers:
      - name: <string>         # Container ID within pod. Default: "main".
        image: <oci-ref>       # Required. OCI image reference.
        entrypoint: [<args>]   # Override image entrypoint.
        args: [<args>]         # Override image CMD.
        env: {<map>}           # KEY: VALUE environment variables.
        working_dir: <path>    # Working directory.
        user: "<uid>[:<gid>]"  # Run as user/group.
        hostname: <string>     # Container hostname.
        tty: <bool>            # Allocate a TTY. Default: false.
        volume_mounts:          # Mount volumes into this container.
          - name: <string>     # References a volume from the workload's volumes list.
            mount_path: <path> # Absolute path inside the container.
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

## Volumes

Volumes provide storage to containers. They are defined at the workload level (pod-scoped) and mounted into individual containers via `volume_mounts`. This two-level design allows multiple containers in the same workload to share a volume.

Volumes survive suspend/resume — their backing storage is snapshotted alongside the VM.

### Volume types

Volume type is specified as a tagged union (exactly one key), following the same pattern as activators.

#### empty_dir

**[Implemented]**

An empty filesystem created when the workload starts and destroyed when the workload is removed. In Firecracker terms, this is a fresh ext4 block device image attached to the VM.

```yaml
workloads:
  api:
    volumes:
      - name: scratch
        empty_dir: {}
      - name: large-scratch
        empty_dir:
          size_mb: 1024         # Optional. Maximum size in MB. Default: 64.
    containers:
      - image: docker.io/myorg/api:latest
        volume_mounts:
          - name: scratch
            mount_path: /tmp/data
          - name: large-scratch
            mount_path: /var/cache
```

#### config_data

**[Implemented]**

Inline file content baked into a read-only filesystem image at spec render time. Useful for configuration files, certificates, and other small static data without needing a separate config management system.

```yaml
volumes:
  - name: config
    config_data:
      files:
        - path: nginx.conf
          content: |
            server {
                listen 80;
                location / { proxy_pass http://api:8080; }
            }
        - path: certs/ca.pem
          content: "..."
```

#### persistent_volume

**[Planned]**

A named volume backed by a storage pool that survives workload removal. Details TBD — will involve pool management, size provisioning, and migration concerns.

```yaml
volumes:
  - name: data
    persistent_volume:
      pool: fast-ssd
      size_mb: 10240
```

### Volume mounts

Each container declares which volumes to mount and where:

```yaml
containers:
  - image: docker.io/myorg/api:latest
    volume_mounts:
      - name: scratch           # Must reference a volume from the workload's volumes list.
        mount_path: /tmp/data   # Absolute path inside the container. Required.
```

A volume can be mounted into multiple containers within the same workload (shared storage between main container and a sidecar, for example).

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
          tcp:
            ports: [8080]
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
      passthrough: ...          # Activator type (exactly one). See Activators.
      tcp: ...                  #
      http2: ...                #
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

The default behavior is **always-on**: a workload starts when the namespace is created and stays running. This applies to all workloads, including those with no services.

Activation is opt-in. It can be configured at two levels:

- **Workload-level activation:** Add an `activation` block on the workload itself. The workload starts only when traffic arrives at its pod IP, and suspends or stops after idle. Only the `passthrough` activator is valid at this level.
- **Service-level activation:** Add an `activation` block on a service. The service (and its linked workload) becomes scale-to-zero. Any activator can be used (`passthrough`, `tcp`, `http2`).

There are three activator types, each with increasing protocol awareness:

#### 1. No activation (default) -- always-on

```yaml
workloads:
  api:
    containers:
      - image: docker.io/myorg/api:latest
    services:
      api: {}
```

Workload starts immediately and stays running. This is also the behavior for workloads with no services at all.

#### 2. Passthrough activation -- packet-level

```yaml
# On a workload (activates on traffic to pod IP):
workloads:
  database:
    activation:
      passthrough:
        idle_timeout: 10m
    containers:
      - image: docker.io/library/postgres:16

# On a service (activates on traffic to service VIP):
services:
  api:
    activation:
      passthrough:
        idle_timeout: 5m
```

Buffers all packets and activates on the first one. No protocol awareness. Suspends/stops after the idle timeout. `idle_timeout` is required for passthrough since it cannot detect when traffic has stopped.

#### 3. TCP activation -- connection-aware

```yaml
services:
  api:
    activation:
      tcp:
        ports: [8080]
        idle_timeout: 5m    # Optional safety net for stuck flows.
```

Activates on new TCP connections (SYN). Filters RSTs and stale traffic. More precise than passthrough -- avoids spurious wakeups from non-connection traffic. Tracks active TCP flows and can deactivate when all connections close. `idle_timeout` is optional (safety net for half-open or stuck connections).

#### 4. HTTP/2 activation -- stream-aware

```yaml
services:
  api:
    activation:
      http2: {}
```

Full H2 proxy with per-stream activation. The backend is held active while streams are open -- precise scale-to-zero without timeout guessing. No `idle_timeout` needed.

---

## Activators

Activators determine how activation detects traffic and decides when to deactivate. They are specified directly inside the `activation` block as a tagged union (exactly one key).

### Passthrough

**[Planned]**

Packet-level. Buffers all packets and activates on the first one. No protocol awareness. Valid on both workloads and services.

```yaml
activation:
  passthrough:
    idle_timeout: 5m          # Required. Time after last packet before deactivation.
```

`idle_timeout` is required because passthrough has no way to detect when traffic has stopped.

### TCP

**[Implemented]**

Connection-aware. Activates on new TCP connections (SYN). Filters RSTs and stale traffic. Tracks active TCP flows and can deactivate when all connections close.

```yaml
activation:
  tcp:
    ports: [8080, 8443]      # Ports to watch. Omit for all ports.
    idle_timeout: 10m         # Optional. Safety net for stuck/half-open connections.
```

`idle_timeout` is optional -- the activator tracks active flows and deactivates when all connections close. The timeout is a safety net for half-open or stuck connections.

> **Note:** The fields `tcp_only` and `max_flows` that may appear in older documentation are not supported in the client protocol. They are not parsed and will cause a YAML deserialization error if present.

### HTTP/2

**[Implemented]**

Stream-aware. Full H2 proxy with per-stream activation. Backend need is held active while streams are open -- precise scale-to-zero without timeout guessing.

```yaml
activation:
  http2: {}
```

No `idle_timeout` needed -- the activator tracks active streams precisely.

### PostgreSQL

**[Parsed, ignored]** -- The `postgres: {}` activator is accepted in the YAML but is not supported in the client protocol (`ActivatorConfig` only has `tcp` and `http2` variants). When present, a log warning is emitted and the activator is dropped.

```yaml
activation:
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
    tcp:
      ports: [80]
      idle_timeout: 5m
```

A workload or service can override any default by specifying the field explicitly. An inline service with an empty body (`service_name: {}`) inherits the default activation if one is set.

Note that setting `defaults.activation` changes services from always-on to scale-to-zero by default. Without it, all services (and their workloads) are always-on unless they individually declare an `activation` block.

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
workloads.<id>.activation         -> (workload activation)         Planned
  .passthrough                    -> (flow activation)             Planned
workloads.<id>.resources.requests -> ResourceRequirements.requests Implemented
workloads.<id>.resources.limits   -> ResourceRequirements.limits   Implemented
workloads.<id>.healthcheck        -> (dropped)                     Parsed, ignored
workloads.<id>.volumes[]          -> VolumeSpec                    Implemented
  .empty_dir                      -> VolumeType::EmptyDir          Implemented
  .config_data                    -> VolumeType::ConfigData        Implemented
  .persistent_volume              -> VolumeType::PersistentVolume  Planned
workloads.<id>.containers[]       -> ContainerSpec                 Implemented
  .volume_mounts[]                -> VolumeMountSpec               Implemented
services.<id>                     -> ServiceSpec                   Implemented
services.<id>.ip                  -> ServiceNetworkConfig.ip       Implemented
services.<id>.activation          -> ActivationSpec                Implemented
  .passthrough                    -> ActivatorConfig (none)        Planned
  .tcp                            -> ActivatorConfig::Tcp          Implemented
  .http2                          -> ActivatorConfig::Http2        Implemented
  .postgres                       -> (dropped)                     Parsed, ignored
  .buffer.frames / .timeout       -> (dropped)                     Parsed, ignored
services.<id>.expose              -> ExposeSpec                    Implemented
```

---

## Comparison with Docker Compose

| Aspect | Docker Compose | distvirt spec |
|--------|---------------|---------------|
| Workload + service | Conflated as "service" | Separate concepts, inline shorthand |
| Activation | N/A | Opt-in per-workload or per-service (passthrough → TCP → HTTP/2) |
| Suspend/resume | N/A | Per-workload `suspend_on_idle` |
| Activators | N/A | Passthrough, TCP, HTTP/2 (PostgreSQL planned) |
| Resources | Limits only (deploy.resources) | Requests (scheduling) + limits (VM size) |
| Health checks | Basic (no readiness gating) | Planned (parsed but not yet functional) |
| Network | Implicit/multi-network | Explicit single subnet |
| Dependencies | `depends_on` ordering | Implicit via service activation |
| Multi-container | N/A | Multiple containers per workload |
| Volumes | Named volumes, bind mounts | Pod-scoped volumes (empty_dir, config_data, persistent_volume) |
| Namespace defaults | N/A | `defaults` block |

---

## CLI Integration

```bash
# Validate a spec file (parse, resolve includes, check for errors)
dv spec validate -f distvirt.yaml

# Render a spec file to resolved proto JSON (no server needed)
dv spec render -f distvirt.yaml

# Deploy from native spec
dv up staging -f distvirt.yaml

# Deploy from Docker Compose (auto-converted)
dv up staging -f docker-compose.yml
```

Validation errors include source snippets pointing at the offending YAML:

```
error: workloads.api.ip — IP 10.0.0.5 is outside the subnet 172.16.0.0/24
 --> distvirt.yaml:8:9
  |
8 |     ip: 10.0.0.5
  |         ^^^^^^^^
  |
```

Unknown fields in the YAML are rejected at parse time, catching typos early.

---

## Things Outside the Spec

These are infrastructure/runtime concerns, not application spec:

- **Ingress adapters** (WireGuard, reverse proxy) -- cluster-level config, managed via `dv connect` / cluster setup.
- **Worker placement** -- scheduler decides, not the spec.
- **Storage pools** -- infrastructure config.
- **Splice** -- runtime debugging operation (`dv splice`).
- **Inter-worker tunneling** -- transparent infrastructure.

---

## Multi-Repo Deployments (Fragments)

**[Implemented]**

For organizations with many microservices across repos, distvirt supports a fragment-based workflow. A namespace spec can include workload fragments from other files, with variable substitution and environment overrides.

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
          tcp:
            ports: [8080]
            idle_timeout: 5m
```

A fragment is a partial namespace spec. It declares one or more workloads with their inline services and optionally top-level services. Fragments cannot have `network`, `metadata`, `defaults`, or `include` fields.

### Central: namespace assembly

A state repo has a namespace file that imports fragments:

```yaml
# state-repo/namespaces/staging.yaml
apiVersion: v1
kind: Namespace

metadata:
  name: staging

network:
  subnet: 172.16.0.0/16

include:
  - path: fragments/api-service.yaml
    values:
      IMAGE: docker.io/myorg/api:v1.2.3
    overrides:
      env:
        DATABASE_URL: "postgres://staging-db:5432/myapp"

  - path: fragments/frontend.yaml
    values:
      IMAGE: docker.io/myorg/frontend:v2.0.1
```

### Include fields

```yaml
include:
  - path: <relative-path>       # Required. Path to fragment file, relative to the namespace spec.
    values:                      # Variable substitution. Replaces ${VAR} in the fragment YAML.
      <key>: <value>
    overrides:                   # Optional overrides applied to all containers in the fragment.
      env:                       # Environment variables merged into every container.
        <key>: <value>
```

### Fragment rules

- Fragments must have `kind: WorkloadFragment` and `apiVersion: v1`.
- Fragments must contain at least one workload.
- Fragments cannot have `metadata`, `network`, `defaults`, or `include` (no recursive includes).
- Top-level services in fragments must reference workloads within the same fragment.
- Workload and service IDs must be unique across all fragments and the main spec.
- Variable substitution (`${VAR}`) is performed before YAML parsing. All variables must be defined in `values`.

### CI workflow

1. Service CI builds image, pushes to registry.
2. Service CI updates the state repo -- bumps the image tag in the relevant `include` entry.
3. A reconciler watches the state repo, renders the full namespace spec by assembling fragments, and applies via `dv up` or the gRPC API.
