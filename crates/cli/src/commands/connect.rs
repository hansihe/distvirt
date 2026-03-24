use std::path::PathBuf;

use anyhow::{Context, bail};
use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64;
use distvirt_client_protocol::*;

use distvirt_client::connect::fd_pass;
use distvirt_client::connect::platform::TunDevice;
use distvirt_client::connect::{ProvisionedTunnel, wg_quick_config};
use distvirt_client::connection::{handle_grpc_error, Client, ConnectionParams};

use super::escalate::{self, SetupTunArgs};

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

/// Launch a privileged helper to create a TUN device and receive the fd back
/// via `SCM_RIGHTS`. The helper also configures the interface and routes.
async fn escalate_tun(provisioned: &ProvisionedTunnel) -> anyhow::Result<TunDevice> {
    let (listener, sock_path, _tmp_dir) = fd_pass::setup_listener()?;

    let nonce: String = {
        let val: u64 = rand::random();
        format!("{:016x}", val)
    };

    let args = SetupTunArgs {
        socket_path: sock_path.to_string_lossy().into_owned(),
        nonce: nonce.clone(),
        client_ip: provisioned.client_ip().to_string(),
        prefix_len: provisioned.prefix_len(),
        subnet: provisioned.subnet().to_string(),
    };

    // Launch the privileged helper in a blocking thread so we don't
    // block the tokio runtime while it waits for the user to authenticate.
    // The helper will connect to our socket and send the fd, so we must
    // accept concurrently.
    let helper_handle =
        tokio::task::spawn_blocking(move || escalate::launch_privileged_helper(&args));

    // Accept the connection from the helper. This blocks until the helper
    // connects (after the user authenticates).
    let conn = tokio::task::spawn_blocking(move || fd_pass::accept(&listener))
        .await
        .context("accept task panicked")??;

    // Wait for the helper to finish.
    let status = helper_handle.await.context("helper task panicked")??;
    if !status.success() {
        bail!(
            "privileged helper exited with status {}",
            status.code().unwrap_or(-1)
        );
    }

    let mut buf = [0u8; 1024];
    let (n, maybe_fd) = fd_pass::recv_fd(&conn, &mut buf)?;

    let payload = std::str::from_utf8(&buf[..n])
        .context("helper sent non-UTF-8 payload")?;

    // Parse protocol: "OK:<nonce>:<device_name>" or "ERR:<nonce>:<message>"
    if let Some(rest) = payload.strip_prefix("ERR:") {
        let msg = rest
            .strip_prefix(nonce.as_str())
            .and_then(|r| r.strip_prefix(':'))
            .context("nonce mismatch in error response from helper")?;
        bail!("privileged helper error: {}", msg);
    }

    let rest = payload
        .strip_prefix("OK:")
        .context("unexpected helper response")?;
    let rest = rest
        .strip_prefix(nonce.as_str())
        .and_then(|r| r.strip_prefix(':'))
        .context("nonce mismatch from helper")?;
    let device_name = rest.to_string();

    let fd = maybe_fd.context("helper did not send a file descriptor")?;
    TunDevice::from_raw_fd(fd, device_name)
}

/// `dv connect` — establish a WireGuard tunnel into a namespace.
pub async fn connect(
    mut client: Client,
    _params: &ConnectionParams,
    namespace_id: &str,
    config_only: bool,
) -> anyhow::Result<()> {
    if config_only {
        let config = wg_quick_config(&mut client, namespace_id).await?;
        print!("{}", config);
        return Ok(());
    }

    let provisioned = ProvisionedTunnel::connect(&mut client, namespace_id).await?;

    // Try creating the kernel tunnel directly. If we lack privileges,
    // fall back to a privileged helper that creates the TUN device and
    // passes the fd back via SCM_RIGHTS.
    let tunnel = match TunDevice::create() {
        Ok(tun) => provisioned.into_kernel_with_tun(tun).await?,
        Err(e) if is_permission_denied(&e) => {
            let tun = escalate_tun(&provisioned).await?;
            provisioned.into_kernel_preconfigured(tun).await?
        }
        Err(e) => return Err(e.context("failed to create TUN device")),
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
