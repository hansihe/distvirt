use std::path::PathBuf;

use anyhow::{Context, bail};
use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64;
use distvirt_client_protocol::*;

use distvirt_client::connect::{ProvisionedTunnel, wg_quick_config};
use distvirt_client::connection::{handle_grpc_error, Client, ConnectionParams};

/// Check whether an anyhow error chain contains a permission-denied I/O error.
fn is_permission_denied(err: &anyhow::Error) -> bool {
    for cause in err.chain() {
        if let Some(io_err) = cause.downcast_ref::<std::io::Error>() {
            if io_err.kind() == std::io::ErrorKind::PermissionDenied {
                return true;
            }
        }
    }
    false
}

/// Re-execute the current process with `sudo`, passing connection params explicitly
/// so that the root shell doesn't lose the user's context/config.
fn reexec_with_sudo(params: &ConnectionParams) -> anyhow::Result<()> {
    let exe = std::env::current_exe().context("cannot determine own executable path")?;
    let args: Vec<String> = std::env::args().collect();

    eprintln!("creating a network tunnel requires root privileges, re-running with sudo...");

    let mut cmd = std::process::Command::new("sudo");
    cmd.arg("--").arg(&exe);

    // Pass connection params explicitly so sudo's env doesn't matter.
    cmd.arg("--server").arg(&params.server);
    if let Some(ref token) = params.token {
        cmd.arg("--token").arg(token);
    }

    // Re-add the subcommand and its arguments from the original invocation.
    // Skip argv[0] and any global flags we already handled above.
    let mut skip_next = false;
    for arg in &args[1..] {
        if skip_next {
            skip_next = false;
            continue;
        }
        match arg.as_str() {
            "--server" | "--token" | "--context" => {
                skip_next = true;
                continue;
            }
            s if s.starts_with("--server=")
                || s.starts_with("--token=")
                || s.starts_with("--context=") =>
            {
                continue;
            }
            _ => {}
        }
        cmd.arg(arg);
    }

    let status = cmd
        .status()
        .context("failed to exec sudo (is it installed?)")?;

    std::process::exit(status.code().unwrap_or(1));
}

/// State file for tracking active connections.
#[derive(serde::Serialize, serde::Deserialize)]
struct ConnectionState {
    public_key: String,
    pid: u32,
}

fn connections_dir() -> anyhow::Result<PathBuf> {
    let config_dir = dirs::config_dir().context("cannot determine config directory")?;
    let dir = config_dir.join("distvirt").join("connections");
    std::fs::create_dir_all(&dir)?;
    Ok(dir)
}

fn state_file_path(namespace_id: &str) -> anyhow::Result<PathBuf> {
    Ok(connections_dir()?.join(format!("{}.json", namespace_id)))
}

/// `dv connect` — establish a WireGuard tunnel into a namespace.
pub async fn connect(
    mut client: Client,
    params: &ConnectionParams,
    namespace_id: &str,
    config_only: bool,
) -> anyhow::Result<()> {
    if config_only {
        let config = wg_quick_config(&mut client, namespace_id).await?;
        print!("{}", config);
        return Ok(());
    }

    let provisioned = ProvisionedTunnel::connect(&mut client, namespace_id).await?;

    // Materialize as kernel TUN tunnel (requires root).
    let tunnel = match provisioned.into_kernel().await {
        Ok(t) => t,
        Err(e) if is_permission_denied(&e) => {
            return reexec_with_sudo(params);
        }
        Err(e) => return Err(e),
    };

    // Write connection state file.
    let state = ConnectionState {
        public_key: BASE64.encode(tunnel.public_key()),
        pid: std::process::id(),
    };
    let state_path = state_file_path(namespace_id)?;
    std::fs::write(&state_path, serde_json::to_string(&state)?)?;

    let info = tunnel.info();
    eprintln!("connected to namespace '{}' via {}", namespace_id, tunnel.tun_name());
    eprintln!("  client IP: {}", info.client_ip);
    eprintln!("  subnet:    {}", info.subnet);
    eprintln!("  endpoint:  {}", info.endpoint);
    eprintln!("press Ctrl+C to disconnect");

    let result = tokio::select! {
        _ = tokio::signal::ctrl_c() => Ok(()),
        r = tunnel.run() => r,
    };

    eprintln!("\ndisconnecting...");

    // Remove state file.
    let _ = std::fs::remove_file(&state_path);

    tunnel.disconnect(&mut client, namespace_id).await?;

    eprintln!("disconnected");

    result
}

/// `dv disconnect` — tear down an existing connection from another terminal.
pub async fn disconnect(mut client: Client, namespace_id: &str) -> anyhow::Result<()> {
    let state_path = state_file_path(namespace_id)?;

    if !state_path.exists() {
        bail!(
            "no active connection found for namespace '{}'",
            namespace_id
        );
    }

    let contents = std::fs::read_to_string(&state_path)?;
    let state: ConnectionState = serde_json::from_str(&contents)?;

    // Decode public key.
    let pubkey_bytes = BASE64
        .decode(&state.public_key)
        .context("invalid base64 in state file")?;

    // Call DisconnectNetwork gRPC.
    client
        .disconnect_network(DisconnectNetworkRequest {
            namespace_id: namespace_id.to_string(),
            client_public_key: pubkey_bytes,
        })
        .await
        .map_err(handle_grpc_error)?;

    // Send SIGTERM to the connect process (if still running).
    let pid = state.pid as i32;
    if pid > 0 {
        let ret = unsafe { libc::kill(pid, 0) };
        if ret == 0 {
            unsafe { libc::kill(pid, libc::SIGTERM) };
        } else {
            eprintln!(
                "warning: connect process (pid {}) is no longer running",
                pid
            );
        }
    }

    // Remove state file.
    let _ = std::fs::remove_file(&state_path);

    eprintln!("disconnected from namespace '{}'", namespace_id);
    Ok(())
}
