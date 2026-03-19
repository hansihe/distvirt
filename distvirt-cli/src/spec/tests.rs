use super::convert::spec_to_namespace_spec;
use super::helpers::parse_duration_ms;
use super::includes::resolve_includes;
use super::ip_alloc::IpAllocator;
use super::parse::{try_parse, ParsedSpec};

use distvirt_client_protocol::*;

use std::io::Write;
use std::net::Ipv4Addr;
use std::path::Path;
use tempfile::{NamedTempFile, TempDir};

    fn parse_yaml(yaml: &str) -> ParsedSpec {
        let mut f = NamedTempFile::new().unwrap();
        f.write_all(yaml.as_bytes()).unwrap();
        try_parse(f.path()).unwrap().unwrap()
    }

    fn convert(yaml: &str) -> (Option<String>, NamespaceSpec) {
        let parsed = parse_yaml(yaml);
        spec_to_namespace_spec(&parsed).unwrap()
    }

    // --- (a) Full example parse + convert ---

    #[test]
    fn full_example_parse_and_convert() {
        let yaml = r#"
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
        working_dir: /app
        user: "1000:1000"
        hostname: api
    resources:
      limits:
        memory_mb: 512
        vcpus: 2
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
    services:
      database:
        activation:
          postgres: {}
  frontend:
    containers:
      - name: main
        image: docker.io/myorg/frontend:latest
    services:
      frontend: {}
"#;
        let (ns_id, proto) = convert(yaml);
        assert_eq!(ns_id.as_deref(), Some("my-staging-env"));
        assert_eq!(proto.workloads.len(), 3);
        assert_eq!(proto.services.len(), 3);

        // Check container fields on api workload
        let api = &proto.workloads["api"];
        assert_eq!(api.containers.len(), 1);
        let c = &api.containers[0];
        assert_eq!(c.name, "main");
        assert_eq!(c.image, "docker.io/myorg/api:latest");
        let cfg = c.config.as_ref().unwrap();
        assert_eq!(cfg.entrypoint, vec!["/app/server"]);
        assert_eq!(cfg.args, vec!["--port", "8080"]);
        assert_eq!(cfg.working_dir, "/app");
        assert_eq!(cfg.user, "1000:1000");
        assert_eq!(cfg.hostname, "api");
        // Resources are now on the workload level
        let res = api.resources.as_ref().unwrap();
        let limits = res.limits.as_ref().unwrap();
        assert_eq!(limits.memory_mb, 512);
        assert_eq!(limits.vcpus, 2);

        // Check activation on api service — idle_timeout is now inside the tcp activator
        let api_svc = &proto.services["api"];
        assert_eq!(api_svc.workload_id, "api");
        let act = api_svc.activation.as_ref().unwrap();
        let activator = act.activator.as_ref().unwrap().activator.as_ref().unwrap();
        match activator {
            activator_config::Activator::Tcp(tcp) => {
                assert_eq!(tcp.idle_timeout_ms, 300_000);
                assert_eq!(tcp.ports, vec![8080]);
            }
            _ => panic!("expected TCP activator"),
        }
    }

    // --- (b) IP stability on addition ---

    #[test]
    fn ip_stable_on_addition() {
        let yaml_base = r#"
apiVersion: v1
kind: Namespace
network:
  subnet: 172.16.0.0/24
workloads:
  api:
    containers:
      - image: img
  database:
    containers:
      - image: img
"#;
        let (_, proto1) = convert(yaml_base);
        let api_ip1 = &proto1.workloads["api"].network.as_ref().unwrap().ip;
        let db_ip1 = &proto1.workloads["database"].network.as_ref().unwrap().ip;

        let yaml_added = r#"
apiVersion: v1
kind: Namespace
network:
  subnet: 172.16.0.0/24
workloads:
  api:
    containers:
      - image: img
  database:
    containers:
      - image: img
  cache:
    containers:
      - image: img
"#;
        let (_, proto2) = convert(yaml_added);
        let api_ip2 = &proto2.workloads["api"].network.as_ref().unwrap().ip;
        let db_ip2 = &proto2.workloads["database"].network.as_ref().unwrap().ip;

        assert_eq!(
            api_ip1, api_ip2,
            "api IP should be stable after adding cache"
        );
        assert_eq!(
            db_ip1, db_ip2,
            "database IP should be stable after adding cache"
        );
    }

    // --- (c) IP stability on removal ---

    #[test]
    fn ip_stable_on_removal() {
        let yaml_full = r#"
apiVersion: v1
kind: Namespace
network:
  subnet: 172.16.0.0/24
workloads:
  api:
    containers:
      - image: img
  database:
    containers:
      - image: img
  frontend:
    containers:
      - image: img
"#;
        let (_, proto1) = convert(yaml_full);
        let api_ip1 = &proto1.workloads["api"].network.as_ref().unwrap().ip;
        let fe_ip1 = &proto1.workloads["frontend"].network.as_ref().unwrap().ip;

        let yaml_removed = r#"
apiVersion: v1
kind: Namespace
network:
  subnet: 172.16.0.0/24
workloads:
  api:
    containers:
      - image: img
  frontend:
    containers:
      - image: img
"#;
        let (_, proto2) = convert(yaml_removed);
        let api_ip2 = &proto2.workloads["api"].network.as_ref().unwrap().ip;
        let fe_ip2 = &proto2.workloads["frontend"].network.as_ref().unwrap().ip;

        assert_eq!(
            api_ip1, api_ip2,
            "api IP should be stable after removing database"
        );
        assert_eq!(
            fe_ip1, fe_ip2,
            "frontend IP should be stable after removing database"
        );
    }

    // --- (d) Explicit IP respected ---

    #[test]
    fn explicit_ip_respected() {
        let yaml = r#"
apiVersion: v1
kind: Namespace
network:
  subnet: 172.16.0.0/24
workloads:
  fixed:
    ip: 172.16.0.50
    containers:
      - image: img
  auto1:
    containers:
      - image: img
  auto2:
    containers:
      - image: img
"#;
        let (_, proto) = convert(yaml);
        let fixed_ip = &proto.workloads["fixed"].network.as_ref().unwrap().ip;
        let auto1_ip = &proto.workloads["auto1"].network.as_ref().unwrap().ip;
        let auto2_ip = &proto.workloads["auto2"].network.as_ref().unwrap().ip;

        assert_eq!(fixed_ip, "172.16.0.50");
        assert_ne!(
            auto1_ip, "172.16.0.50",
            "auto-assigned should not collide with explicit"
        );
        assert_ne!(
            auto2_ip, "172.16.0.50",
            "auto-assigned should not collide with explicit"
        );
        assert_ne!(auto1_ip, auto2_ip, "auto-assigned IPs should be distinct");
    }

    // --- (e) Defaults merging: suspend_on_idle ---

    #[test]
    fn defaults_suspend_on_idle() {
        let yaml = r#"
apiVersion: v1
kind: Namespace
network:
  subnet: 172.16.0.0/24
defaults:
  suspend_on_idle: true
workloads:
  inherits:
    containers:
      - image: img
  overrides:
    suspend_on_idle: false
    containers:
      - image: img
"#;
        let (_, proto) = convert(yaml);
        assert!(
            proto.workloads["inherits"].suspend_on_idle,
            "should inherit default true"
        );
        assert!(
            !proto.workloads["overrides"].suspend_on_idle,
            "should override to false"
        );
    }

    // --- (f) Defaults activation ---

    #[test]
    fn defaults_activation_inherited_and_overridden() {
        let yaml = r#"
apiVersion: v1
kind: Namespace
network:
  subnet: 172.16.0.0/24
defaults:
  activation:
    tcp:
      ports: [80]
      idle_timeout: 5m
workloads:
  app:
    containers:
      - image: img
    services:
      inherits: {}
      overrides:
        activation:
          tcp:
            ports: [9090]
            idle_timeout: 30s
"#;
        let (_, proto) = convert(yaml);

        // Service that inherits default activation — idle_timeout inside tcp activator
        let inherits_act = proto.services["inherits"].activation.as_ref().unwrap();
        let inherits_tcp = match inherits_act
            .activator
            .as_ref()
            .unwrap()
            .activator
            .as_ref()
            .unwrap()
        {
            activator_config::Activator::Tcp(tcp) => tcp,
            _ => panic!("expected TCP activator"),
        };
        assert_eq!(inherits_tcp.idle_timeout_ms, 300_000);

        // Service that overrides activation
        let overrides_act = proto.services["overrides"].activation.as_ref().unwrap();
        let overrides_tcp = match overrides_act
            .activator
            .as_ref()
            .unwrap()
            .activator
            .as_ref()
            .unwrap()
        {
            activator_config::Activator::Tcp(tcp) => tcp,
            _ => panic!("expected TCP activator"),
        };
        assert_eq!(overrides_tcp.idle_timeout_ms, 30_000);
    }

    // --- (g) Duration parsing ---

    #[test]
    fn duration_parsing() {
        assert_eq!(parse_duration_ms("5m").unwrap(), 300_000);
        assert_eq!(parse_duration_ms("30s").unwrap(), 30_000);
        assert_eq!(parse_duration_ms("500ms").unwrap(), 500);
        assert_eq!(parse_duration_ms("1h").unwrap(), 3_600_000);
        assert!(parse_duration_ms("invalid").is_err());
        assert!(parse_duration_ms("5x").is_err());
    }

    // --- (h) Compose fallback detection ---

    #[test]
    fn compose_yaml_returns_none() {
        let compose = r#"
version: "3"
services:
  web:
    image: nginx
    ports:
      - "80:80"
"#;
        let mut f = NamedTempFile::new().unwrap();
        f.write_all(compose.as_bytes()).unwrap();
        let result = try_parse(f.path()).unwrap();
        assert!(
            result.is_none(),
            "docker-compose YAML should not parse as native spec"
        );
    }

    // --- IpAllocator unit tests ---

    #[test]
    fn allocator_deterministic() {
        let mut a1 = IpAllocator::new("10.0.0.0/24").unwrap();
        let mut a2 = IpAllocator::new("10.0.0.0/24").unwrap();
        let ip1 = a1.assign("foo").unwrap();
        let ip2 = a2.assign("foo").unwrap();
        assert_eq!(ip1, ip2, "same name should always get same IP");
    }

    #[test]
    fn allocator_reserve_prevents_collision() {
        let mut alloc = IpAllocator::new("10.0.0.0/24").unwrap();
        let reserved: Ipv4Addr = "10.0.0.2".parse().unwrap();
        alloc.reserve(reserved).unwrap();

        // Assign many names, none should get the reserved IP
        for i in 0..50 {
            let ip = alloc.assign(&format!("name-{}", i)).unwrap();
            assert_ne!(ip, reserved, "should not assign reserved IP");
        }
    }

    #[test]
    fn allocator_reserve_duplicate_errors() {
        let mut alloc = IpAllocator::new("10.0.0.0/24").unwrap();
        alloc.reserve("10.0.0.5".parse().unwrap()).unwrap();
        assert!(alloc.reserve("10.0.0.5".parse().unwrap()).is_err());
    }

    #[test]
    fn allocator_exhaustion() {
        // /30 = 4 addresses, minus .0 and .1 = 2 usable
        let mut alloc = IpAllocator::new("10.0.0.0/30").unwrap();
        alloc.assign("a").unwrap();
        alloc.assign("b").unwrap();
        assert!(alloc.assign("c").is_err(), "should be exhausted");
    }

    // --- Validation tests ---

    /// Helper: parse YAML and attempt conversion, expecting an error.
    fn convert_err(yaml: &str) -> String {
        let parsed = parse_yaml(yaml);
        let err = spec_to_namespace_spec(&parsed).unwrap_err();
        err.to_string()
    }

    #[test]
    fn validation_empty_containers() {
        let err = convert_err(r#"
apiVersion: v1
kind: Namespace
workloads:
  api:
    containers: []
"#);
        assert!(err.contains("containers list is empty"), "got: {}", err);
        assert!(err.contains("workloads.api.containers"), "got: {}", err);
    }

    #[test]
    fn validation_empty_image() {
        let err = convert_err(r#"
apiVersion: v1
kind: Namespace
workloads:
  api:
    containers:
      - image: ""
"#);
        assert!(err.contains("image is empty"), "got: {}", err);
    }

    #[test]
    fn validation_bad_workload_ref() {
        let err = convert_err(r#"
apiVersion: v1
kind: Namespace
workloads:
  api:
    containers:
      - image: img
services:
  frontend:
    workload: nonexistent
"#);
        assert!(err.contains("workload 'nonexistent' does not exist"), "got: {}", err);
        assert!(err.contains("services.frontend"), "got: {}", err);
    }

    #[test]
    fn validation_duplicate_service_ids() {
        let err = convert_err(r#"
apiVersion: v1
kind: Namespace
workloads:
  api:
    containers:
      - image: img
    services:
      mysvc: {}
services:
  mysvc:
    workload: api
"#);
        assert!(err.contains("duplicate service ID 'mysvc'"), "got: {}", err);
    }

    #[test]
    fn validation_ip_outside_subnet() {
        let err = convert_err(r#"
apiVersion: v1
kind: Namespace
network:
  subnet: 172.16.0.0/24
workloads:
  api:
    ip: 10.0.0.5
    containers:
      - image: img
"#);
        assert!(err.contains("outside the subnet"), "got: {}", err);
        assert!(err.contains("workloads.api.ip"), "got: {}", err);
    }

    #[test]
    fn validation_duplicate_explicit_ips() {
        let err = convert_err(r#"
apiVersion: v1
kind: Namespace
network:
  subnet: 172.16.0.0/24
workloads:
  api:
    ip: 172.16.0.50
    containers:
      - image: img
  db:
    ip: 172.16.0.50
    containers:
      - image: img
"#);
        assert!(err.contains("duplicate IP '172.16.0.50'"), "got: {}", err);
    }

    #[test]
    fn validation_invalid_duration_is_error() {
        let err = convert_err(r#"
apiVersion: v1
kind: Namespace
workloads:
  api:
    activation:
      passthrough:
        idle_timeout: 5x
    containers:
      - image: img
"#);
        assert!(err.contains("invalid duration '5x'"), "got: {}", err);
        assert!(err.contains("workloads.api.activation.passthrough.idle_timeout"), "got: {}", err);
    }

    #[test]
    fn validation_workload_non_passthrough_is_error() {
        // SpecWorkloadActivation only has passthrough field, but if it's None
        // (e.g. empty activation block), that should be an error.
        let err = convert_err(r#"
apiVersion: v1
kind: Namespace
workloads:
  api:
    activation: {}
    containers:
      - image: img
"#);
        assert!(err.contains("only passthrough activator is valid on workloads"), "got: {}", err);
    }

    #[test]
    fn validation_multiple_errors_collected() {
        let err = convert_err(r#"
apiVersion: v1
kind: Namespace
network:
  subnet: 172.16.0.0/24
workloads:
  api:
    containers: []
  db:
    ip: 10.0.0.1
    activation:
      passthrough:
        idle_timeout: bad
    containers:
      - image: img
services:
  frontend:
    workload: ghost
"#);
        // Should contain multiple errors
        assert!(err.contains("containers list is empty"), "got: {}", err);
        assert!(err.contains("outside the subnet"), "got: {}", err);
        assert!(err.contains("invalid duration 'bad'"), "got: {}", err);
        assert!(err.contains("workload 'ghost' does not exist"), "got: {}", err);
    }

    #[test]
    fn validation_warnings_dont_block() {
        // postgres activator and gateway are warnings, not errors
        let yaml = r#"
apiVersion: v1
kind: Namespace
network:
  subnet: 172.16.0.0/24
  gateway: 172.16.0.1
workloads:
  db:
    containers:
      - image: img
    services:
      db:
        activation:
          postgres: {}
"#;
        let parsed = parse_yaml(yaml);
        let result = spec_to_namespace_spec(&parsed);
        assert!(result.is_ok(), "warnings should not block: {:?}", result.err());
        let (_, proto) = result.unwrap();
        assert_eq!(proto.workloads.len(), 1);
    }

    #[test]
    fn validation_tcp_port_out_of_range() {
        let err = convert_err(r#"
apiVersion: v1
kind: Namespace
workloads:
  api:
    containers:
      - image: img
    services:
      web:
        activation:
          tcp:
            ports: [0, 80, 99999]
"#);
        assert!(err.contains("invalid port number 0"), "got: {}", err);
        assert!(err.contains("invalid port number 99999"), "got: {}", err);
    }

    #[test]
    fn validation_resource_zero_values() {
        let err = convert_err(r#"
apiVersion: v1
kind: Namespace
workloads:
  api:
    containers:
      - image: img
    resources:
      limits:
        memory_mb: 0
        vcpus: 0
"#);
        assert!(err.contains("memory_mb must be > 0"), "got: {}", err);
        assert!(err.contains("vcpus must be > 0"), "got: {}", err);
    }

    #[test]
    fn validation_bad_api_version() {
        let err = convert_err(r#"
apiVersion: v99
kind: Namespace
workloads:
  api:
    containers:
      - image: img
"#);
        assert!(err.contains("unrecognized apiVersion 'v99'"), "got: {}", err);
    }

    // --- Snippet rendering sanity tests ---

    #[test]
    fn rendered_validation_error_has_source_snippet() {
        let err = convert_err(r#"
apiVersion: v1
kind: Namespace
network:
  subnet: 172.16.0.0/24
workloads:
  api:
    ip: 10.0.0.5
    containers:
      - image: img
"#);
        // Should have annotate-snippets style output with source pointer
        assert!(err.contains("-->"), "expected source location pointer, got:\n{}", err);
        assert!(err.contains("10.0.0.5"), "expected value in snippet, got:\n{}", err);
        assert!(err.contains("^^^^"), "expected underline, got:\n{}", err);
    }

    #[test]
    fn rendered_parse_error_has_source_snippet() {
        let mut f = NamedTempFile::new().unwrap();
        f.write_all(b"apiVersion: v1\nkind: Namespace\nworkloads:\n  api:\n    containers:\n      - image: valid\n        args: not_a_list\n").unwrap();
        let msg = match try_parse(f.path()) {
            Err(e) => e.to_string(),
            Ok(_) => panic!("should fail to parse"),
        };
        // serde-saphyr renders parse errors with source snippets
        assert!(msg.contains("-->"), "expected source location in parse error, got:\n{}", msg);
    }

    // --- Fragment tests ---

    fn write_file(dir: &Path, name: &str, content: &str) -> std::path::PathBuf {
        let p = dir.join(name);
        std::fs::write(&p, content).unwrap();
        p
    }

    fn parse_with_includes(dir: &TempDir, ns_yaml: &str) -> (Option<String>, NamespaceSpec) {
        let spec_path = write_file(dir.path(), "distvirt.yaml", ns_yaml);
        let mut parsed = try_parse(&spec_path).unwrap().unwrap();
        resolve_includes(&mut parsed, &spec_path).unwrap();
        spec_to_namespace_spec(&parsed).unwrap()
    }

    #[test]
    fn fragment_basic_merge() {
        let dir = TempDir::new().unwrap();
        write_file(dir.path(), "api.yaml", r#"
apiVersion: v1
kind: WorkloadFragment
workloads:
  api:
    containers:
      - image: myorg/api:latest
"#);
        let (_, proto) = parse_with_includes(&dir, r#"
apiVersion: v1
kind: Namespace
network:
  subnet: 172.16.0.0/24
workloads:
  database:
    containers:
      - image: postgres:16
include:
  - path: api.yaml
"#);
        assert_eq!(proto.workloads.len(), 2);
        assert!(proto.workloads.contains_key("api"));
        assert!(proto.workloads.contains_key("database"));
    }

    #[test]
    fn fragment_variable_substitution() {
        let dir = TempDir::new().unwrap();
        write_file(dir.path(), "app.yaml", r#"
apiVersion: v1
kind: WorkloadFragment
workloads:
  app:
    containers:
      - image: ${IMAGE}
"#);
        let (_, proto) = parse_with_includes(&dir, r#"
apiVersion: v1
kind: Namespace
network:
  subnet: 172.16.0.0/24
include:
  - path: app.yaml
    values:
      IMAGE: myorg/app:v2
"#);
        let app = &proto.workloads["app"];
        assert_eq!(app.containers[0].image, "myorg/app:v2");
    }

    #[test]
    fn fragment_undefined_variable_error() {
        let dir = TempDir::new().unwrap();
        write_file(dir.path(), "app.yaml", r#"
apiVersion: v1
kind: WorkloadFragment
workloads:
  app:
    containers:
      - image: ${FOO}
"#);
        let spec_path = write_file(dir.path(), "distvirt.yaml", r#"
apiVersion: v1
kind: Namespace
include:
  - path: app.yaml
"#);
        let mut parsed = try_parse(&spec_path).unwrap().unwrap();
        let err = resolve_includes(&mut parsed, &spec_path).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("undefined variable 'FOO'"), "got: {}", msg);
    }

    #[test]
    fn fragment_env_overrides() {
        let dir = TempDir::new().unwrap();
        write_file(dir.path(), "app.yaml", r#"
apiVersion: v1
kind: WorkloadFragment
workloads:
  app:
    containers:
      - image: myorg/app:latest
        env:
          EXISTING: keep
          OVERRIDE_ME: old
"#);
        let (_, proto) = parse_with_includes(&dir, r#"
apiVersion: v1
kind: Namespace
network:
  subnet: 172.16.0.0/24
include:
  - path: app.yaml
    overrides:
      env:
        OVERRIDE_ME: new
        ADDED: yes
"#);
        let env = &proto.workloads["app"].containers[0]
            .config
            .as_ref()
            .unwrap()
            .env;
        assert_eq!(env.get("EXISTING").unwrap(), "keep");
        assert_eq!(env.get("OVERRIDE_ME").unwrap(), "new");
        assert_eq!(env.get("ADDED").unwrap(), "yes");
    }

    #[test]
    fn fragment_duplicate_workload_error() {
        let dir = TempDir::new().unwrap();
        write_file(dir.path(), "frag1.yaml", r#"
apiVersion: v1
kind: WorkloadFragment
workloads:
  api:
    containers:
      - image: img1
"#);
        write_file(dir.path(), "frag2.yaml", r#"
apiVersion: v1
kind: WorkloadFragment
workloads:
  api:
    containers:
      - image: img2
"#);
        let spec_path = write_file(dir.path(), "distvirt.yaml", r#"
apiVersion: v1
kind: Namespace
include:
  - path: frag1.yaml
  - path: frag2.yaml
"#);
        let mut parsed = try_parse(&spec_path).unwrap().unwrap();
        let err = resolve_includes(&mut parsed, &spec_path).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("duplicate workload ID 'api'"), "got: {}", msg);
    }

    #[test]
    fn fragment_duplicate_service_across_fragments() {
        let dir = TempDir::new().unwrap();
        write_file(dir.path(), "frag1.yaml", r#"
apiVersion: v1
kind: WorkloadFragment
workloads:
  app1:
    containers:
      - image: img
services:
  mysvc:
    workload: app1
"#);
        write_file(dir.path(), "frag2.yaml", r#"
apiVersion: v1
kind: WorkloadFragment
workloads:
  app2:
    containers:
      - image: img
services:
  mysvc:
    workload: app2
"#);
        let spec_path = write_file(dir.path(), "distvirt.yaml", r#"
apiVersion: v1
kind: Namespace
include:
  - path: frag1.yaml
  - path: frag2.yaml
"#);
        let mut parsed = try_parse(&spec_path).unwrap().unwrap();
        let err = resolve_includes(&mut parsed, &spec_path).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("duplicate service ID 'mysvc'"), "got: {}", msg);
    }

    #[test]
    fn fragment_rejects_metadata() {
        let dir = TempDir::new().unwrap();
        write_file(dir.path(), "bad.yaml", r#"
apiVersion: v1
kind: WorkloadFragment
metadata:
  name: oops
workloads:
  app:
    containers:
      - image: img
"#);
        let spec_path = write_file(dir.path(), "distvirt.yaml", r#"
apiVersion: v1
kind: Namespace
include:
  - path: bad.yaml
"#);
        let mut parsed = try_parse(&spec_path).unwrap().unwrap();
        let err = resolve_includes(&mut parsed, &spec_path).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("fragments cannot have 'metadata'"), "got: {}", msg);
    }

    #[test]
    fn fragment_rejects_network() {
        let dir = TempDir::new().unwrap();
        write_file(dir.path(), "bad.yaml", r#"
apiVersion: v1
kind: WorkloadFragment
network:
  subnet: 10.0.0.0/24
workloads:
  app:
    containers:
      - image: img
"#);
        let spec_path = write_file(dir.path(), "distvirt.yaml", r#"
apiVersion: v1
kind: Namespace
include:
  - path: bad.yaml
"#);
        let mut parsed = try_parse(&spec_path).unwrap().unwrap();
        let err = resolve_includes(&mut parsed, &spec_path).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("fragments cannot have 'network'"), "got: {}", msg);
    }

    #[test]
    fn fragment_top_level_services() {
        let dir = TempDir::new().unwrap();
        write_file(dir.path(), "app.yaml", r#"
apiVersion: v1
kind: WorkloadFragment
workloads:
  app:
    containers:
      - image: myorg/app:latest
services:
  app-svc:
    workload: app
    activation:
      tcp:
        ports: [8080]
"#);
        let (_, proto) = parse_with_includes(&dir, r#"
apiVersion: v1
kind: Namespace
network:
  subnet: 172.16.0.0/24
include:
  - path: app.yaml
"#);
        assert!(proto.services.contains_key("app-svc"));
        assert_eq!(proto.services["app-svc"].workload_id, "app");
    }

    #[test]
    fn fragment_bad_workload_ref_in_fragment() {
        let dir = TempDir::new().unwrap();
        write_file(dir.path(), "bad.yaml", r#"
apiVersion: v1
kind: WorkloadFragment
workloads:
  app:
    containers:
      - image: img
services:
  svc:
    workload: nonexistent
"#);
        let spec_path = write_file(dir.path(), "distvirt.yaml", r#"
apiVersion: v1
kind: Namespace
include:
  - path: bad.yaml
"#);
        let mut parsed = try_parse(&spec_path).unwrap().unwrap();
        let err = resolve_includes(&mut parsed, &spec_path).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("workload 'nonexistent' does not exist in this fragment"), "got: {}", msg);
    }

    #[test]
    fn fragment_ip_stability() {
        let dir = TempDir::new().unwrap();

        // First: namespace with just database
        let yaml_base = r#"
apiVersion: v1
kind: Namespace
network:
  subnet: 172.16.0.0/24
workloads:
  database:
    containers:
      - image: postgres:16
"#;
        let spec_path = write_file(dir.path(), "distvirt.yaml", yaml_base);
        let parsed = try_parse(&spec_path).unwrap().unwrap();
        let (_, proto1) = spec_to_namespace_spec(&parsed).unwrap();
        let db_ip1 = proto1.workloads["database"].network.as_ref().unwrap().ip.clone();

        // Now add a fragment
        write_file(dir.path(), "api.yaml", r#"
apiVersion: v1
kind: WorkloadFragment
workloads:
  api:
    containers:
      - image: myorg/api:latest
"#);
        let yaml_with_fragment = r#"
apiVersion: v1
kind: Namespace
network:
  subnet: 172.16.0.0/24
workloads:
  database:
    containers:
      - image: postgres:16
include:
  - path: api.yaml
"#;
        let spec_path2 = write_file(dir.path(), "distvirt2.yaml", yaml_with_fragment);
        let mut parsed2 = try_parse(&spec_path2).unwrap().unwrap();
        resolve_includes(&mut parsed2, &spec_path2).unwrap();
        let (_, proto2) = spec_to_namespace_spec(&parsed2).unwrap();
        let db_ip2 = &proto2.workloads["database"].network.as_ref().unwrap().ip;

        assert_eq!(&db_ip1, db_ip2, "database IP should be stable after adding fragment");
    }

    #[test]
    fn fragment_path_relative_to_spec() {
        let dir = TempDir::new().unwrap();
        let subdir = dir.path().join("fragments");
        std::fs::create_dir(&subdir).unwrap();

        write_file(&subdir, "app.yaml", r#"
apiVersion: v1
kind: WorkloadFragment
workloads:
  app:
    containers:
      - image: myorg/app:latest
"#);
        let (_, proto) = parse_with_includes(&dir, r#"
apiVersion: v1
kind: Namespace
network:
  subnet: 172.16.0.0/24
include:
  - path: fragments/app.yaml
"#);
        assert!(proto.workloads.contains_key("app"));
    }
