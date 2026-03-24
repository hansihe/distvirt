//! OCI image config handling — parsing, merging, and entrypoint resolution.
//!
//! # Supported OCI image config fields
//!
//! - `Entrypoint` / `Cmd` — full OCI resolution rules
//! - `Env` — merged with overrides (image first, overrides appended)
//! - `WorkingDir` — override takes precedence, falls back to image default
//! - `User` — numeric uid:gid or username, resolved via image /etc/passwd
//! - `Hostname` — passed through to guest `sethostname()`
//!
//! # Missing OCI image config fields
//!
//! High value:
//! - `ExposedPorts` — not parsed; useful for compose/orchestrator port mapping
//! - `Volumes` — no volume/mount support (blocks stateful workloads, config injection)
//! - `Labels` — not parsed or exposed; useful for metadata-driven config
//! - `StopSignal` — not honored; container is hard-killed instead of receiving the
//!   image-specified signal (usually SIGTERM) with a grace period
//! - `Healthcheck` — HEALTHCHECK instructions from Dockerfile are ignored
//! - `Domainname` — only hostname is set
//!
//! Medium value:
//! - `AdditionalGids` — supplementary groups not supported
//! - `Umask` — not configurable
//!
//! # Missing guest filesystem setup (in guest-init)
//!
//! - `/dev/shm` — shared memory tmpfs expected by many apps
//! - `/dev/pts` — no devpts mount (pseudo-terminal support)
//! - `/dev/mqueue` — POSIX message queues
//! - `/etc/hosts` — not generated (hostname resolution won't work)
//! - `/etc/hostname` — not written despite sethostname() being called
//!
//! # Deprioritized (VM provides isolation)
//!
//! These matter less because the microVM already provides strong isolation:
//! - `Capabilities`, `NoNewPrivileges`, `Seccomp`, `AppArmor`/`SELinux`
//! - `OomScoreAdj` — single process per VM
//! - `MaskedPaths`/`ReadonlyPaths` — /proc and /sys mounted without restrictions
//! - `Sysctl` — no sysctl configuration in guest
//! - `Rlimits` — process resource limits not set

use anyhow::{Context, bail};

use distvirt_worker_protocol::ContainerConfig;

/// An entry from /etc/passwd.
#[derive(Debug, Clone)]
pub struct PasswdEntry {
    pub name: String,
    pub uid: u32,
    pub gid: u32,
}

/// An entry from /etc/group.
#[derive(Debug, Clone)]
pub struct GroupEntry {
    pub name: String,
    pub gid: u32,
}

/// Parsed OCI image configuration relevant for container execution.
#[derive(Clone)]
pub struct ImageConfig {
    pub entrypoint: Vec<String>,
    pub cmd: Vec<String>,
    pub env: Vec<String>,
    pub working_dir: Option<String>,
    pub user: Option<String>,
    pub passwd_entries: Vec<PasswdEntry>,
    pub group_entries: Vec<GroupEntry>,
}

/// Parse /etc/passwd content into entries.
///
/// Format: `name:password:uid:gid:gecos:home:shell`
/// Skips malformed lines.
pub fn parse_passwd(content: &str) -> Vec<PasswdEntry> {
    content
        .lines()
        .filter_map(|line| {
            let fields: Vec<&str> = line.split(':').collect();
            if fields.len() < 4 {
                return None;
            }
            let uid = fields[2].parse().ok()?;
            let gid = fields[3].parse().ok()?;
            Some(PasswdEntry {
                name: fields[0].to_string(),
                uid,
                gid,
            })
        })
        .collect()
}

/// Parse /etc/group content into entries.
///
/// Format: `name:password:gid:members`
/// Skips malformed lines.
pub fn parse_group(content: &str) -> Vec<GroupEntry> {
    content
        .lines()
        .filter_map(|line| {
            let fields: Vec<&str> = line.split(':').collect();
            if fields.len() < 3 {
                return None;
            }
            let gid = fields[2].parse().ok()?;
            Some(GroupEntry {
                name: fields[0].to_string(),
                gid,
            })
        })
        .collect()
}

/// Resolve a user string to (uid, optional gid).
///
/// Supports formats:
/// - `"1000"` — numeric uid
/// - `"1000:1000"` — numeric uid:gid
/// - `"postgres"` — username, looked up in passwd entries
/// - `"postgres:postgres"` — user:group, looked up in passwd/group entries
pub fn resolve_user(
    user: &str,
    passwd: &[PasswdEntry],
    groups: &[GroupEntry],
) -> anyhow::Result<(u32, Option<u32>)> {
    if let Some((user_part, group_part)) = user.split_once(':') {
        let uid = resolve_uid(user_part, passwd)?;
        let gid = resolve_gid(group_part, passwd, groups)?;
        Ok((uid, Some(gid)))
    } else {
        let uid = resolve_uid(user, passwd)?;
        // When only a user is specified, also use their primary gid from passwd.
        let gid = passwd.iter().find(|e| e.uid == uid).map(|e| e.gid);
        Ok((uid, gid))
    }
}

fn resolve_uid(user: &str, passwd: &[PasswdEntry]) -> anyhow::Result<u32> {
    if let Ok(uid) = user.parse::<u32>() {
        return Ok(uid);
    }
    passwd
        .iter()
        .find(|e| e.name == user)
        .map(|e| e.uid)
        .with_context(|| format!("user '{}' not found in image /etc/passwd", user))
}

fn resolve_gid(
    group: &str,
    passwd: &[PasswdEntry],
    groups: &[GroupEntry],
) -> anyhow::Result<u32> {
    if let Ok(gid) = group.parse::<u32>() {
        return Ok(gid);
    }
    // Try /etc/group first, then fall back to passwd entries (some images
    // use the username as group name with matching gid in passwd).
    if let Some(entry) = groups.iter().find(|e| e.name == group) {
        return Ok(entry.gid);
    }
    if let Some(entry) = passwd.iter().find(|e| e.name == group) {
        return Ok(entry.gid);
    }
    bail!("group '{}' not found in image /etc/group", group)
}

/// Merge OCI image config with overrides following OCI entrypoint/cmd resolution rules.
///
/// The `overrides` come from the orchestrator (via the worker protocol). Empty
/// `entrypoint` vec means "no override" — fall through to image defaults.
pub fn merge_config(
    image: &ImageConfig,
    overrides: &ContainerConfig,
) -> anyhow::Result<ContainerConfig> {
    let has_entrypoint_override = !overrides.entrypoint.is_empty();
    let has_args_override = !overrides.args.is_empty();

    let (entrypoint, args) = if has_entrypoint_override {
        // Override entrypoint provided — use it with override args (or image cmd as default args).
        let ep = overrides.entrypoint.clone();
        let args = if has_args_override {
            overrides.args.clone()
        } else {
            image.cmd.clone()
        };
        (ep, args)
    } else if !image.entrypoint.is_empty() {
        // Image has entrypoint, no override — use image entrypoint with override args or image cmd.
        let args = if has_args_override {
            overrides.args.clone()
        } else {
            image.cmd.clone()
        };
        (image.entrypoint.clone(), args)
    } else if has_args_override {
        // No entrypoint anywhere, but override args provided (compose `command:` replaces CMD).
        (overrides.args.clone(), vec![])
    } else if !image.cmd.is_empty() {
        // Image has CMD only.
        (image.cmd.clone(), vec![])
    } else {
        bail!("image has no entrypoint or cmd, and none was specified");
    };

    let mut env = image.env.clone();
    env.extend(overrides.env.iter().cloned());

    Ok(ContainerConfig {
        entrypoint,
        args,
        env,
        working_dir: overrides
            .working_dir
            .clone()
            .or_else(|| image.working_dir.clone()),
        user: overrides.user.clone().or_else(|| image.user.clone()),
        hostname: overrides.hostname.clone(),
        capture_output: overrides.capture_output,
        stdin: overrides.stdin,
        volume_mounts: overrides.volume_mounts.clone(),
    })
}
