use std::collections::HashMap;
use std::path::Path;

use anyhow::Context;
use base64::prelude::*;
use serde::Deserialize;

#[derive(Deserialize)]
struct DockerConfig {
    auths: HashMap<String, AuthEntry>,
}

#[derive(Deserialize)]
struct AuthEntry {
    auth: String,
}

pub struct RegistryCredential {
    pub username: String,
    pub password: String,
}

/// Extract the registry host from an image reference.
///
/// Examples:
///   "514443763038.dkr.ecr.us-east-1.amazonaws.com/foo:latest" -> "514443763038.dkr.ecr.us-east-1.amazonaws.com"
///   "docker.io/library/nginx:latest" -> "docker.io"
///   "nginx:latest" -> "docker.io"  (implicit default)
fn registry_host(image_ref: &str) -> &str {
    // If there's no slash, or the part before the first slash doesn't look
    // like a hostname (contains a dot or colon), it's an implicit docker.io ref.
    match image_ref.split_once('/') {
        Some((host, _)) if host.contains('.') || host.contains(':') => host,
        _ => "docker.io",
    }
}

/// Read a Docker config.json and return credentials for the given image's registry, if any.
pub fn lookup_credentials(config_path: &Path, image_ref: &str) -> Option<RegistryCredential> {
    let contents = match std::fs::read_to_string(config_path) {
        Ok(c) => c,
        Err(e) => {
            log::warn!(
                "could not read docker config at {}: {}",
                config_path.display(),
                e
            );
            return None;
        }
    };

    let config: DockerConfig = match serde_json::from_str(&contents) {
        Ok(c) => c,
        Err(e) => {
            log::warn!(
                "could not parse docker config at {}: {}",
                config_path.display(),
                e
            );
            return None;
        }
    };

    let host = registry_host(image_ref);

    let entry = config.auths.get(host)?;

    match decode_auth(&entry.auth) {
        Ok(cred) => Some(cred),
        Err(e) => {
            log::warn!("could not decode auth for registry {}: {}", host, e);
            None
        }
    }
}

fn decode_auth(encoded: &str) -> anyhow::Result<RegistryCredential> {
    let decoded = BASE64_STANDARD.decode(encoded).context("base64 decode")?;
    let decoded = String::from_utf8(decoded).context("auth is not valid utf-8")?;
    let (username, password) = decoded
        .split_once(':')
        .context("auth does not contain ':'")?;
    Ok(RegistryCredential {
        username: username.to_string(),
        password: password.to_string(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_registry_host() {
        assert_eq!(
            registry_host("514443763038.dkr.ecr.us-east-1.amazonaws.com/repo:tag"),
            "514443763038.dkr.ecr.us-east-1.amazonaws.com"
        );
        assert_eq!(registry_host("docker.io/library/nginx:latest"), "docker.io");
        assert_eq!(registry_host("nginx:latest"), "docker.io");
        assert_eq!(registry_host("localhost:5000/myimage"), "localhost:5000");
        assert_eq!(registry_host("ghcr.io/owner/repo:v1"), "ghcr.io");
    }

    #[test]
    fn test_decode_auth() {
        let encoded = BASE64_STANDARD.encode("myuser:mypass");
        let cred = decode_auth(&encoded).unwrap();
        assert_eq!(cred.username, "myuser");
        assert_eq!(cred.password, "mypass");
    }

    #[test]
    fn test_decode_auth_with_colon_in_password() {
        let encoded = BASE64_STANDARD.encode("user:pass:with:colons");
        let cred = decode_auth(&encoded).unwrap();
        assert_eq!(cred.username, "user");
        assert_eq!(cred.password, "pass:with:colons");
    }
}
