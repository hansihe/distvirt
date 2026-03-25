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
        command: ["/app/server"]
        args: ["--port", "8080"]
        env:
          DATABASE_URL: "postgres://${services.database.ip}:5432/myapp"
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
        idle_timeout: 5m
        ports:
          - port: 8080
            activator:
              type: tcp

  database:
    containers:
      - name: main
        image: docker.io/library/postgres:16
        env:
          POSTGRES_PASSWORD: "dev"
    activation:                     # Workload-level activation
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
    run_policy: service | job   # Default: service. Jobs run to completion (exit 0 = done).
    activation:                  # Workload-level activation. If omitted, always-on.
      idle_timeout: <duration>   # Time after last traffic before deactivation.
    volumes:                    # Pod-scoped volume definitions. See Volumes.
      - name: <string>         # Volume name. Referenced by volume_mounts.
        empty_dir: {}          # Volume type (exactly one). See Volumes.
    containers:
      - name: <string>         # Container ID within pod. Default: "main".
        image: <oci-ref>       # Required. OCI image reference.
        command: [<args>]      # Override image entrypoint.
        args: [<args>]         # Override image CMD.
        env: {<map>}           # KEY: VALUE environment variables. Supports ${...} expressions.
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
      default_mode: "0644"          # Optional. Default POSIX mode for all files. Default: "0644".
      files:
        - path: nginx.conf
          content: |
            server {
                listen 80;
                location / { proxy_pass http://api:8080; }
            }
        - path: certs/ca.pem
          content: "..."
          mode: "0400"              # Optional. Per-file POSIX mode override.
        - path: scripts/init.sh
          content: "#!/bin/sh\necho hello"
          mode: "0755"
```

File permissions are specified as octal strings. `default_mode` sets the mode for all files that don't specify their own `mode`. If neither is set, files default to `0644`. The client expands `default_mode` before sending to the orchestrator — the wire protocol always carries an explicit mode on every file.

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
        idle_timeout: 5m
        ports:
          - port: 8080
            activator:
              type: tcp
```

The service IP is auto-assigned from the subnet. The workload linkage is implicit.

### Top-level declaration (advanced)

For cases where services don't map 1:1 to workloads, declare them at the top level:

```yaml
services:
  <service-id>:
    workload: <workload-id>     # Required at top level.
    ip: <ipv4>                  # Auto-assigned if omitted.
    ports: ...                  # See Ports.
    idle_timeout: <duration>    # See Activation.
```

### Service fields

```yaml
services:
  <service-id>:
    workload: <workload-id>     # Implicit when inline, required at top level.
    ip: <ipv4>                  # Service virtual IP. Auto-assigned if omitted.

    idle_timeout: <duration>    # Service-level idle timeout. Default: 30s when activated.
    buffer:                     # Buffer config for activation frame buffering.
      frames: <uint>            # Max frames to buffer. Default: 64.
      timeout: <duration>       # Buffer timeout. Default: 5s.

    ports:                      # Per-port configuration. See Ports.
      - port: <uint>            # Exposed port number.
        target: <uint>          # Backend port. Default: same as port.
        activator:              # Per-port activator. If omitted, port is passthrough.
          type: tcp | http2
          max_flows: <uint>     # TCP only. Default: 1024.
```

### Always-on vs activated

The default behavior is **always-on**: a workload starts when the namespace is created and stays running. This applies to all workloads, including those with no services.

Activation is opt-in. It can be configured at two levels:

- **Workload-level activation:** Add an `activation` block on the workload itself. The workload starts only when traffic arrives at its pod IP, and suspends or stops after idle.
- **Service-level activation:** Add `activator` fields to ports on a service. The service (and its linked workload) becomes scale-to-zero. Different ports can use different activator types.

### Activation model

A service is either **activated** or **not activated** — this is mutually exclusive. If any port has an activator, all ports must have activators (ports without an explicit activator get TCP activation by default). A service with no activators on any port is a pure passthrough service. Mixed activated/non-activated ports on the same service are rejected at spec validation time.

`idle_timeout` lives at the service level and applies uniformly to all activators on the service. Default is 30s when activation is present.

#### 1. No activation (default) -- always-on

```yaml
workloads:
  api:
    containers:
      - image: docker.io/myorg/api:latest
    services:
      api: {}
```

Workload starts immediately and stays running. This is also the behavior for workloads with no services at all. Ports without activators pass traffic through directly.

#### 2. Workload-level activation

```yaml
workloads:
  database:
    activation:
      idle_timeout: 10m
    containers:
      - image: docker.io/library/postgres:16
```

The workload starts only when traffic arrives at its pod IP, and suspends or stops after idle. The `idle_timeout` controls how long to wait after last traffic before deactivation.

#### 3. TCP activation -- connection-aware

```yaml
services:
  api:
    idle_timeout: 5m
    ports:
      - port: 8080
        activator:
          type: tcp
```

Per-port TCP activation. Activates on new TCP connections (SYN). Filters RSTs and stale traffic. Tracks active TCP flows and can deactivate when all connections close. `idle_timeout` is a service-level safety net for half-open or stuck connections.

#### 4. HTTP/2 activation -- stream-aware

```yaml
services:
  api:
    ports:
      - port: 443
        target: 8443
        activator:
          type: http2
```

Full H2 proxy with per-stream activation. The backend is held active while streams are open -- precise scale-to-zero without timeout guessing. `target` specifies the backend port the container listens on.

#### 5. Multiple ports with different activators

```yaml
services:
  api:
    idle_timeout: 30s
    ports:
      - port: 80
        activator:
          type: tcp
      - port: 443
        target: 8443
        activator:
          type: http2
      - port: 8080
        activator:
          type: tcp
          max_flows: 100
```

Each port can use a different activator type. The `idle_timeout` applies to all activators on the service.

---

## Port Activators

Activators determine how activation detects traffic and decides when to deactivate. They are specified per-port via the `activator` field, which is a tagged union selected by `type`.

A port with no `activator` field is a passthrough port — traffic is forwarded directly without protocol awareness.

### Passthrough (no activator)

A port without an `activator` field. Traffic is buffered and forwarded directly. On a service with activation (i.e. other ports have activators), passthrough ports still participate in the activation lifecycle but have no protocol-specific logic.

```yaml
ports:
  - port: 80                  # No activator = passthrough
```

### TCP

**[Implemented]**

Connection-aware. Activates on new TCP connections (SYN). Filters RSTs and stale traffic. Tracks active TCP flows and can deactivate when all connections close.

```yaml
ports:
  - port: 8080
    activator:
      type: tcp
      max_flows: 1024        # Optional. Max tracked flows. Default: 1024.
```

### HTTP/2

**[Implemented]**

Stream-aware. Full H2 proxy with per-stream activation. Backend need is held active while streams are open -- precise scale-to-zero without timeout guessing.

```yaml
ports:
  - port: 443
    target: 8443              # Backend port the container listens on.
    activator:
      type: http2
```

`target` specifies the backend port for L4 (HTTP/2) activators. The activator connects upstream on the target port. For non-L4 activators, `target` is accepted but not yet functional (future DNAT support).

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

Namespace-level defaults applied to all workloads unless overridden:

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
```

A workload can override any default by specifying the field explicitly.

Note: when no `defaults.suspend_on_idle` is specified, the parser defaults to `true`.

Service-level activation (ports, idle_timeout, buffer) is configured per-service — there is no default activation block. Each service explicitly declares its own port configuration.

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
workloads.<id>.activation         -> ActivationSpec                Implemented
  .idle_timeout                   -> ActivationSpec.idle_timeout   Implemented
workloads.<id>.resources.requests -> ResourceRequirements.requests Implemented
workloads.<id>.resources.limits   -> ResourceRequirements.limits   Implemented
workloads.<id>.run_policy         -> WorkloadSpec.run_policy       Implemented
workloads.<id>.healthcheck        -> (dropped)                     Parsed, ignored
workloads.<id>.volumes[]          -> VolumeSpec                    Implemented
  .empty_dir                      -> VolumeType::EmptyDir          Implemented
  .config_data                    -> VolumeType::ConfigData        Implemented
  .persistent_volume              -> VolumeType::PersistentVolume  Planned
workloads.<id>.containers[]       -> ContainerSpec                 Implemented
  .volume_mounts[]                -> VolumeMountSpec               Implemented
services.<id>                     -> ServiceSpec                   Implemented
services.<id>.ip                  -> ServiceNetworkConfig.ip       Implemented
services.<id>.idle_timeout        -> ServiceSpec.idle_timeout_ms   Implemented
services.<id>.buffer.frames       -> ServiceSpec.buffer_frames     Implemented
services.<id>.buffer.timeout      -> ServiceSpec.buffer_timeout_ms Implemented
services.<id>.ports[]             -> PortSpec                      Implemented
  .port                           -> PortSpec.port                 Implemented
  .target                         -> PortSpec.target_port          Implemented
  .activator.type=tcp             -> TcpPortActivator              Implemented
  .activator.type=http2           -> Http2PortActivator            Implemented
${self.ip}                        -> resolved to workload pod IP   Implemented
${workloads.<id>.ip}              -> resolved to workload pod IP   Implemented
${services.<id>.ip}               -> resolved to service VIP       Implemented
${values.<key>}                   -> resolved from include values  Implemented
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
| Variable substitution | `${VAR}` env vars only | `${...}` expressions in any string field (IPs, values) |
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

## Expressions

**[Implemented]**

distvirt supports `${...}` expressions in string fields for referencing dynamic values like IPs. All expression types use the same `${...}` syntax.

### Available expressions

| Expression | Resolves to | Available in |
|---|---|---|
| `${self.ip}` | Pod IP of the current workload | Workload string fields |
| `${workloads.<id>.ip}` | Pod IP of another workload | Any string field |
| `${services.<id>.ip}` | Virtual IP of a service | Any string field |
| `${values.<key>}` | Value from fragment `include.values` | Fragment files only |

### Reference expressions

Reference expressions resolve to IPs allocated from the namespace subnet. They can be used in any string field within a workload: `env` values, `image`, `args`, `command`, `working_dir`, `user`, `hostname`, and `config_data` file content.

```yaml
workloads:
  api:
    containers:
      - image: docker.io/myorg/api:latest
        env:
          MY_IP: "${self.ip}"
          DATABASE_URL: "postgres://${services.db.ip}:5432/myapp"
          CACHE_HOST: "${workloads.cache.ip}"
    volumes:
      - name: config
        config_data:
          files:
            - path: upstream.conf
              content: "server ${workloads.backend.ip}:8080;"

  cache:
    containers:
      - image: redis:7
    services:
      cache: {}

  backend:
    containers:
      - image: myorg/backend:latest
    services:
      db:
        ports:
          - port: 5432
```

Expressions can be embedded in larger strings (`"postgres://${services.db.ip}:5432"`) or used as the entire value (`"${self.ip}"`).

Since IPs are assigned deterministically (see IP Auto-Assignment), all expressions are resolved at spec render time -- no runtime templating is involved.

### Fragment value expressions

`${values.*}` expressions are used in workload fragments to accept parameters from the including namespace spec. They are resolved via text substitution before YAML parsing, which means they can appear in any YAML position -- including non-string fields like port numbers.

```yaml
# Fragment file
apiVersion: v1
kind: WorkloadFragment
workloads:
  app:
    containers:
      - image: ${values.IMAGE}
        env:
          LOG_LEVEL: ${values.LOG_LEVEL}
```

```yaml
# Namespace spec
include:
  - path: app.yaml
    values:
      IMAGE: myorg/app:v1.2.3
      LOG_LEVEL: info
```

See Multi-Repo Deployments (Fragments) for full details.

### Combining values and references

Fragment value expressions and reference expressions can be used together. Values are resolved first (during fragment loading), then references are resolved on the assembled spec:

```yaml
# Fragment file
apiVersion: v1
kind: WorkloadFragment
workloads:
  app:
    containers:
      - image: ${values.IMAGE}
        env:
          MY_IP: "${self.ip}"
          DB_HOST: "${services.db.ip}"
```

### Expression resolution order

1. **Fragment values** (`${values.*}`) -- resolved per-fragment during include loading, before YAML parsing.
2. **References** (`${self.*}`, `${workloads.*}`, `${services.*}`) -- resolved after all fragments are merged and IPs are allocated.
3. Any remaining `${...}` expressions after both phases produce an error.

### Reserved top-level names

The top-level namespace in expressions is reserved. Currently defined: `self`, `workloads`, `services`, `values`. Other top-level names are reserved for future use and will produce an error.

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
      - image: ${values.IMAGE}
        args: ["--port", "8080"]
        env:
          LOG_LEVEL: "info"
    services:
      api:
        idle_timeout: 5m
        ports:
          - port: 8080
            activator:
              type: tcp
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
        DATABASE_URL: "postgres://${services.db.ip}:5432/myapp"

  - path: fragments/frontend.yaml
    values:
      IMAGE: docker.io/myorg/frontend:v2.0.1
```

### Include fields

```yaml
include:
  - path: <relative-path>       # Required. Path to fragment file, relative to the namespace spec.
    values:                      # Variable substitution. Replaces ${values.KEY} in the fragment YAML.
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
- Fragment value substitution (`${values.KEY}`) is performed before YAML parsing. All referenced values must be defined in `include.values`.
- Reference expressions (`${self.ip}`, `${workloads.X.ip}`, `${services.X.ip}`) are resolved after fragment merging and can reference workloads/services from other fragments or the main spec.

### CI workflow

1. Service CI builds image, pushes to registry.
2. Service CI updates the state repo -- bumps the image tag in the relevant `include` entry.
3. A reconciler watches the state repo, renders the full namespace spec by assembling fragments, and applies via `dv up` or the gRPC API.
