use std::collections::HashMap;
use std::path::Path;

use anyhow::Context;
use compose_spec::Compose;
use compose_spec::service::Command;
use compose_spec::service::ports;
use distvirt_types::{Dependency, Deployment, PortMapping, PortProtocol, ServiceSpec};

/// Parse a Docker Compose file into a [`Deployment`].
pub fn parse(path: &Path) -> anyhow::Result<Deployment> {
    let contents = std::fs::read_to_string(path)
        .with_context(|| format!("failed to read {}", path.display()))?;

    let compose = Compose::options()
        .from_yaml_str(&contents)
        .context("failed to parse compose file")?;

    let name = compose
        .name
        .map(|n| n.to_string())
        .unwrap_or_else(|| {
            path.parent()
                .and_then(|p| p.file_name())
                .map(|n| n.to_string_lossy().into_owned())
                .unwrap_or_else(|| "unnamed".to_string())
        });

    let mut services = HashMap::new();

    for (ident, service) in compose.services {
        let svc_name = ident.to_string();

        // Warn about unsupported fields.
        if service.build.is_some() {
            log::warn!("{svc_name}: `build` is not supported, ignoring");
        }
        if !service.volumes.is_empty() {
            log::warn!("{svc_name}: `volumes` is not supported, ignoring");
        }
        if service.healthcheck.is_some() {
            log::warn!("{svc_name}: `healthcheck` is not supported, ignoring");
        }
        if service.restart.is_some() {
            log::warn!("{svc_name}: `restart` is not supported, ignoring");
        }
        if !service.configs.is_empty() {
            log::warn!("{svc_name}: `configs` is not supported, ignoring");
        }
        if !service.secrets.is_empty() {
            log::warn!("{svc_name}: `secrets` is not supported, ignoring");
        }
        if service.network_config.is_some() {
            log::warn!("{svc_name}: `networks` is not supported, ignoring");
        }

        let image = service
            .image
            .as_ref()
            .map(|i| i.to_string())
            .ok_or_else(|| anyhow::anyhow!("{svc_name}: `image` is required"))?;

        let command = service.command.map(convert_command);
        let entrypoint = service.entrypoint.map(convert_command);

        let environment = convert_environment(service.environment);

        let ports = convert_ports(service.ports);

        // DependsOn is ShortOrLong<IndexSet<Identifier>, IndexMap<Identifier, Dependency>>.
        // Convert to long form (IndexMap) then extract service names.
        let depends_on_map = service.depends_on.into_long();
        let depends_on = depends_on_map
            .into_keys()
            .map(|ident| Dependency {
                service: ident.to_string(),
            })
            .collect();

        let hostname = service.hostname.map(|h| h.to_string());
        let user = service.user.map(|u| u.to_string());
        let working_dir = service
            .working_dir
            .map(|w| w.as_path().to_string_lossy().into_owned());

        services.insert(
            svc_name,
            ServiceSpec {
                image,
                command,
                entrypoint,
                environment,
                ports,
                depends_on,
                hostname,
                user,
                working_dir,
            },
        );
    }

    Ok(Deployment { name, services })
}

fn convert_command(cmd: Command) -> Vec<String> {
    match cmd {
        Command::String(s) => vec!["/bin/sh".into(), "-c".into(), s],
        Command::List(v) => v,
    }
}

fn convert_environment(env: compose_spec::ListOrMap) -> HashMap<String, String> {
    match env.into_map() {
        Ok(map) => map
            .into_iter()
            .filter_map(|(k, v)| {
                let val = v?;
                Some((k.to_string(), val.to_string()))
            })
            .collect(),
        Err(_) => HashMap::new(),
    }
}

fn convert_ports(p: compose_spec::service::Ports) -> Vec<PortMapping> {
    ports::into_long_iter(p)
        .map(|port| {
            let host_port = port
                .published
                .as_ref()
                .map(|r| r.start())
                .unwrap_or(port.target);

            let protocol = match port.protocol {
                Some(ports::Protocol::Udp) => PortProtocol::Udp,
                _ => PortProtocol::Tcp,
            };

            PortMapping {
                host_port,
                container_port: port.target,
                protocol,
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::NamedTempFile;

    fn parse_yaml(yaml: &str) -> anyhow::Result<Deployment> {
        let mut f = NamedTempFile::new().unwrap();
        f.write_all(yaml.as_bytes()).unwrap();
        parse(f.path())
    }

    #[test]
    fn parse_minimal() {
        let d = parse_yaml(
            r#"
name: myapp
services:
  web:
    image: nginx:latest
"#,
        )
        .unwrap();
        assert_eq!(d.name, "myapp");
        assert_eq!(d.services.len(), 1);
        assert!(d.services.contains_key("web"));
        assert_eq!(d.services["web"].image, "nginx:latest");
    }

    #[test]
    fn parse_name_from_dir() {
        let dir = tempfile::tempdir().unwrap();
        let file_path = dir.path().join("docker-compose.yml");
        std::fs::write(
            &file_path,
            r#"
services:
  web:
    image: nginx
"#,
        )
        .unwrap();
        let d = parse(&file_path).unwrap();
        // Name should be derived from parent directory
        let expected = dir.path().file_name().unwrap().to_string_lossy().to_string();
        assert_eq!(d.name, expected);
    }

    #[test]
    fn parse_command_string() {
        let d = parse_yaml(
            r#"
name: test
services:
  app:
    image: alpine
    command: "echo hello"
"#,
        )
        .unwrap();
        assert_eq!(
            d.services["app"].command.as_ref().unwrap(),
            &vec!["/bin/sh".to_string(), "-c".to_string(), "echo hello".to_string()]
        );
    }

    #[test]
    fn parse_command_list() {
        let d = parse_yaml(
            r#"
name: test
services:
  app:
    image: alpine
    command: ["echo", "hello"]
"#,
        )
        .unwrap();
        assert_eq!(
            d.services["app"].command.as_ref().unwrap(),
            &vec!["echo".to_string(), "hello".to_string()]
        );
    }

    #[test]
    fn parse_environment_map() {
        let d = parse_yaml(
            r#"
name: test
services:
  app:
    image: alpine
    environment:
      FOO: bar
"#,
        )
        .unwrap();
        assert_eq!(d.services["app"].environment.get("FOO").unwrap(), "bar");
    }

    #[test]
    fn parse_ports() {
        let d = parse_yaml(
            r#"
name: test
services:
  app:
    image: nginx
    ports:
      - "8080:80"
"#,
        )
        .unwrap();
        let ports = &d.services["app"].ports;
        assert_eq!(ports.len(), 1);
        assert_eq!(ports[0].host_port, 8080);
        assert_eq!(ports[0].container_port, 80);
        assert!(matches!(ports[0].protocol, PortProtocol::Tcp));
    }

    #[test]
    fn parse_depends_on() {
        let d = parse_yaml(
            r#"
name: test
services:
  web:
    image: nginx
    depends_on:
      - db
  db:
    image: postgres
"#,
        )
        .unwrap();
        let deps = &d.services["web"].depends_on;
        assert_eq!(deps.len(), 1);
        assert_eq!(deps[0].service, "db");
    }

    #[test]
    fn parse_missing_image() {
        let result = parse_yaml(
            r#"
name: test
services:
  app:
    command: "echo hi"
"#,
        );
        assert!(result.is_err());
    }

    #[test]
    fn parse_full_service() {
        let d = parse_yaml(
            r#"
name: full
services:
  app:
    image: myapp:v1
    command: ["run", "--flag"]
    entrypoint: ["/entrypoint.sh"]
    environment:
      DB_HOST: localhost
      DB_PORT: "5432"
    ports:
      - "3000:3000"
      - "9090:9090"
    depends_on:
      - db
    hostname: myhost
    user: "1000"
    working_dir: /app
  db:
    image: postgres:15
"#,
        )
        .unwrap();

        assert_eq!(d.name, "full");
        let app = &d.services["app"];
        assert_eq!(app.image, "myapp:v1");
        assert_eq!(app.command.as_ref().unwrap(), &vec!["run".to_string(), "--flag".to_string()]);
        assert_eq!(app.entrypoint.as_ref().unwrap(), &vec!["/entrypoint.sh".to_string()]);
        assert_eq!(app.environment.len(), 2);
        assert_eq!(app.environment["DB_HOST"], "localhost");
        assert_eq!(app.environment["DB_PORT"], "5432");
        assert_eq!(app.ports.len(), 2);
        assert_eq!(app.depends_on.len(), 1);
        assert_eq!(app.depends_on[0].service, "db");
        assert_eq!(app.hostname.as_ref().unwrap(), "myhost");
        assert_eq!(app.user.as_ref().unwrap(), "1000");
        assert_eq!(app.working_dir.as_ref().unwrap(), "/app");
    }
}
