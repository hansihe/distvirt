//! OCI image config handling — parsing, merging, and entrypoint resolution.
//!
//! # Supported OCI image config fields
//!
//! - `Entrypoint` / `Cmd` — full OCI resolution rules
//! - `Env` — merged with overrides (image first, overrides appended)
//! - `WorkingDir` — override takes precedence, falls back to image default
//! - `User` — numeric uid:gid only (no /etc/passwd lookup)
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
//! - `User` by name — no /etc/passwd lookup, so `USER nobody` style images fail
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

/// Parsed OCI image configuration relevant for container execution.
#[derive(Clone)]
pub struct ImageConfig {
    pub entrypoint: Vec<String>,
    pub cmd: Vec<String>,
    pub env: Vec<String>,
    pub working_dir: Option<String>,
    pub user: Option<String>,
}

/// Parse a numeric user string like "1000" or "1000:1000" into (uid, gid).
pub fn parse_user_numeric(user: &str) -> anyhow::Result<(Option<u32>, Option<u32>)> {
    if user.is_empty() {
        return Ok((None, None));
    }
    if let Some((uid_str, gid_str)) = user.split_once(':') {
        let uid: u32 = uid_str
            .parse()
            .with_context(|| format!("non-numeric uid: {}", uid_str))?;
        let gid: u32 = gid_str
            .parse()
            .with_context(|| format!("non-numeric gid: {}", gid_str))?;
        Ok((Some(uid), Some(gid)))
    } else {
        let uid: u32 = user
            .parse()
            .with_context(|| format!("non-numeric user: {}", user))?;
        Ok((Some(uid), None))
    }
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

    let (img_uid, img_gid) = image
        .user
        .as_deref()
        .map(parse_user_numeric)
        .transpose()?
        .unwrap_or((None, None));

    Ok(ContainerConfig {
        entrypoint,
        args,
        env,
        working_dir: overrides
            .working_dir
            .clone()
            .or_else(|| image.working_dir.clone()),
        uid: overrides.uid.or(img_uid),
        gid: overrides.gid.or(img_gid),
        hostname: overrides.hostname.clone(),
        capture_output: overrides.capture_output,
        stdin: overrides.stdin,
    })
}
