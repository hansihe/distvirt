//! Platform-specific privilege escalation for the TUN setup helper.
//!
//! This module is specific to the `dv` CLI binary — it assumes the current
//! executable supports the `internal setup-tun` subcommand.

use std::process::Command;

#[cfg(target_os = "macos")]
use std::os::unix::process::CommandExt;
use anyhow::Context;

/// Arguments passed to the privileged helper.
pub struct SetupTunArgs {
    pub socket_path: String,
    pub nonce: String,
    pub client_ip: String,
    pub prefix_len: u8,
    pub subnet: String,
    pub dns_domain: String,
    pub gateway_ip: String,
    pub log_level: Option<String>,
}

/// Launch the privileged helper, wait for it to finish, and return its exit status.
///
/// Re-executes the current binary (`dv internal setup-tun …`) with elevated
/// privileges. On macOS this shows the native authentication dialog via
/// `osascript`; on Linux it uses `sudo`.
pub fn launch_privileged_helper(args: &SetupTunArgs) -> anyhow::Result<std::process::ExitStatus> {
    let exe = std::env::current_exe().context("cannot determine own executable path")?;
    let exe_str = exe.to_string_lossy();

    let prefix_len_str = args.prefix_len.to_string();
    let mut helper_args: Vec<&str> = vec![
        "internal",
        "setup-tun",
        "--socket",
        &args.socket_path,
        "--nonce",
        &args.nonce,
        "--client-ip",
        &args.client_ip,
        "--prefix-len",
        &prefix_len_str,
        "--subnet",
        &args.subnet,
        "--dns-domain",
        &args.dns_domain,
        "--gateway-ip",
        &args.gateway_ip,
    ];

    if let Some(ref level) = args.log_level {
        helper_args.push("--log-level");
        helper_args.push(level);
    }

    launch_platform(&exe_str, &helper_args)
}

#[cfg(target_os = "macos")]
fn launch_platform(exe: &str, args: &[&str]) -> anyhow::Result<std::process::ExitStatus> {
    eprintln!("creating a network tunnel requires administrator privileges...");

    // Build a shell command string with proper escaping for osascript.
    let shell_cmd = build_shell_command(exe, args);

    // Escape for AppleScript string literal (backslash and double-quote).
    let escaped = shell_cmd.replace('\\', "\\\\").replace('"', "\\\"");
    let script = format!(
        "do shell script \"{}\" with administrator privileges",
        escaped
    );

    // Use .output() instead of .status() so osascript does not inherit our
    // terminal stdio.  The auth dialog is GUI-based and doesn't need the
    // terminal; keeping osascript detached from our tty prevents it from
    // disturbing terminal state (e.g. signal disposition, foreground pgrp)
    // which would break Ctrl-C after escalation completes.
    // We also place it in its own process group to avoid signal
    // cross-contamination (Ctrl-C during the dialog shouldn't kill us).
    let output = Command::new("osascript")
        .arg("-e")
        .arg(&script)
        .process_group(0)
        .output()
        .context("failed to launch osascript")?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        if !stderr.is_empty() {
            eprintln!("osascript: {}", stderr.trim());
        }
    }

    Ok(output.status)
}

#[cfg(target_os = "linux")]
fn launch_platform(exe: &str, args: &[&str]) -> anyhow::Result<std::process::ExitStatus> {
    eprintln!("creating a network tunnel requires root privileges, running setup with sudo...");

    let status = Command::new("sudo")
        .arg("--")
        .arg(exe)
        .args(args)
        .status()
        .context("failed to launch sudo")?;

    Ok(status)
}

/// Build a shell-safe command string from an executable path and arguments.
#[cfg(target_os = "macos")]
fn build_shell_command(exe: &str, args: &[&str]) -> String {
    let mut parts = Vec::with_capacity(1 + args.len());
    parts.push(shell_escape(exe));
    for arg in args {
        parts.push(shell_escape(arg));
    }
    parts.join(" ")
}

/// Escape a string for safe inclusion in a POSIX shell command.
#[cfg(target_os = "macos")]
fn shell_escape(s: &str) -> String {
    if s.is_empty() {
        return "''".to_string();
    }
    // If it contains no special characters, return as-is.
    if s.chars()
        .all(|c| c.is_ascii_alphanumeric() || matches!(c, '-' | '_' | '.' | '/' | ':' | '=' | '+'))
    {
        return s.to_string();
    }
    // Wrap in single quotes, escaping any single quotes.
    format!("'{}'", s.replace('\'', "'\\''"))
}
