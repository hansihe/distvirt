//! Platform-specific privilege escalation for the TUN setup helper.
//!
//! This module is specific to the `dv` CLI binary — it assumes the current
//! executable supports the `internal setup-tun` subcommand.

use std::process::Command;

use anyhow::Context;

/// Arguments passed to the privileged helper.
pub struct SetupTunArgs {
    pub socket_path: String,
    pub nonce: String,
    pub client_ip: String,
    pub prefix_len: u8,
    pub subnet: String,
}

/// Launch the privileged helper, wait for it to finish, and return its exit status.
///
/// Re-executes the current binary (`dv internal setup-tun …`) with elevated
/// privileges. On macOS this shows the native authentication dialog via
/// `osascript`; on Linux it uses `sudo`.
pub fn launch_privileged_helper(args: &SetupTunArgs) -> anyhow::Result<std::process::ExitStatus> {
    let exe = std::env::current_exe().context("cannot determine own executable path")?;
    let exe_str = exe.to_string_lossy();

    let helper_args = [
        "internal",
        "setup-tun",
        "--socket",
        &args.socket_path,
        "--nonce",
        &args.nonce,
        "--client-ip",
        &args.client_ip,
        "--prefix-len",
        &args.prefix_len.to_string(),
        "--subnet",
        &args.subnet,
    ];

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

    let status = Command::new("osascript")
        .arg("-e")
        .arg(&script)
        .status()
        .context("failed to launch osascript")?;

    Ok(status)
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
