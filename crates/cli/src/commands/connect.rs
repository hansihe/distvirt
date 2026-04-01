use std::os::fd::OwnedFd;
use std::path::PathBuf;

use anyhow::{Context, bail};
use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64;
use distvirt_client_protocol::*;
use tempfile::TempDir;
use tokio::task::JoinHandle;

use distvirt_client::connect::kernel::KernelTunnel;
use distvirt_client::connect::platform::{
    TunDevice, add_route, configure_dns, configure_interface, remove_dns, remove_route,
};
use distvirt_client::connect::{ProvisionedTunnel, wg_quick_config};
use distvirt_client::connection::{handle_grpc_error, Client};

use super::escalate::{self, SetupTunArgs};
use super::{fd_pass, helper_protocol};
use super::helper_protocol::{HelperToParent, ParentToHelper};

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
    control_socket: Option<String>,
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

fn control_socket_path(namespace_id: &str) -> anyhow::Result<PathBuf> {
    Ok(connections_dir()?.join(format!("{}.sock", namespace_id)))
}

// ── Connection state helpers ──────────────────────────────────────────────

/// Write the connection state file and return its path.
fn write_connection_state(
    namespace_id: &str,
    public_key: &[u8; 32],
    pid: u32,
    control_socket_path: Option<&PathBuf>,
) -> anyhow::Result<PathBuf> {
    let state = ConnectionState {
        public_key: BASE64.encode(public_key),
        pid,
        control_socket: control_socket_path.map(|p| p.to_string_lossy().into_owned()),
    };
    let state_path = state_file_path(namespace_id)?;
    std::fs::write(&state_path, serde_json::to_string(&state)?)?;
    Ok(state_path)
}

/// Remove the connection state file (best-effort).
fn remove_connection_state(state_path: &PathBuf) {
    let _ = std::fs::remove_file(state_path);
}

// ── Teardown helper ───────────────────────────────────────────────────────

/// Perform the full teardown sequence: drop tunnel, clean up routes/DNS,
/// disconnect from server, remove state file and control socket.
async fn teardown_connection(
    tun_name: &str,
    subnet: &str,
    escalated_helper: Option<EscalatedHelper>,
    provisioned: ProvisionedTunnel,
    client: &mut Client,
    namespace_id: &str,
    state_path: &PathBuf,
    control_sock_path: &PathBuf,
) {
    // Remove state file and control socket.
    remove_connection_state(state_path);
    let _ = std::fs::remove_file(control_sock_path);

    // Cleanup: direct mode removes DNS/routes, escalated mode delegates to helper.
    if let Some(helper) = escalated_helper {
        helper.teardown().await;
    } else {
        let _ = remove_dns(tun_name);
        let _ = remove_route(subnet, tun_name);
    }

    // Always disconnect server-side.
    if let Err(e) = provisioned.disconnect(client, namespace_id).await {
        log::warn!("disconnect gRPC failed: {:#}", e);
    }
}

// ── Escalated helper ──────────────────────────────────────────────────────

/// Handle to a long-lived privileged helper process.
///
/// The helper stays alive after sending the TUN fd so it can perform
/// privileged teardown (DNS/route removal) when we send `Teardown`.
struct EscalatedHelper {
    /// Socket connection to the helper for sending `Teardown`.
    conn: OwnedFd,
    /// Handle to the blocking task running the helper process.
    helper_handle: JoinHandle<anyhow::Result<std::process::ExitStatus>>,
    /// Temp directory holding the Unix socket — must outlive the connection.
    _tmp_dir: TempDir,
}

impl EscalatedHelper {
    /// Send a `Teardown` message to the helper and wait for it to exit.
    async fn teardown(self) {
        // Send teardown; if the helper already died the send will fail — that's fine.
        if let Err(e) = helper_protocol::send_parent_msg(&self.conn, &ParentToHelper::Teardown) {
            log::warn!("failed to send Teardown to helper: {:#}", e);
        }
        // Drop the socket so the helper sees EOF if it hasn't already.
        drop(self.conn);

        // Wait for the helper to exit with a timeout.
        match tokio::time::timeout(
            std::time::Duration::from_secs(5),
            self.helper_handle,
        )
        .await
        {
            Ok(Ok(Ok(status))) => {
                if !status.success() {
                    log::warn!(
                        "helper exited with status {}",
                        status.code().unwrap_or(-1)
                    );
                }
            }
            Ok(Ok(Err(e))) => log::warn!("helper error: {:#}", e),
            Ok(Err(e)) => log::warn!("helper task panicked: {:#}", e),
            Err(_) => log::warn!("helper did not exit within 5s, abandoning"),
        }
    }
}

/// Launch a privileged helper to create a TUN device and receive the fd back
/// via `SCM_RIGHTS`. The helper also configures the interface and routes,
/// and stays alive to perform teardown later.
async fn escalate_tun(
    provisioned: &ProvisionedTunnel,
    namespace_id: &str,
) -> anyhow::Result<(TunDevice, EscalatedHelper)> {
    let (listener, sock_path, tmp_dir) = fd_pass::setup_listener()?;

    let nonce: String = {
        let val: u128 = rand::random();
        format!("{:032x}", val)
    };

    let dns_domain = format!("{}.dv.local", namespace_id);
    let log_level = Some(log::max_level().to_string());
    let args = SetupTunArgs {
        socket_path: sock_path.to_string_lossy().into_owned(),
        nonce: nonce.clone(),
        client_ip: provisioned.client_ip().to_string(),
        prefix_len: provisioned.prefix_len(),
        subnet: provisioned.subnet().to_string(),
        dns_domain,
        gateway_ip: provisioned.gateway_ip().to_string(),
        log_level,
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

    // Receive the setup result (don't wait for the helper to exit — it stays alive).
    let (msg, maybe_fd) = helper_protocol::recv_helper_msg(&conn)?;

    match msg {
        HelperToParent::Error { nonce: n, message } => {
            if n != nonce {
                bail!("nonce mismatch in error response from helper");
            }
            bail!("privileged helper error: {}", message);
        }
        HelperToParent::SetupResult {
            nonce: n,
            device_name,
            helper_nonce,
        } => {
            if n != nonce {
                bail!("nonce mismatch from helper");
            }
            let fd = maybe_fd.context("helper did not send a file descriptor")?;
            let tun = TunDevice::from_raw_fd(fd, device_name)?;

            // Send Ack with the helper's nonce to complete bidirectional validation.
            helper_protocol::send_parent_msg(
                &conn,
                &ParentToHelper::Ack { helper_nonce },
            ).context("send Ack to helper")?;

            let helper = EscalatedHelper {
                conn,
                helper_handle,
                _tmp_dir: tmp_dir,
            };
            Ok((tun, helper))
        }
    }
}

/// `dv connect` — establish a WireGuard tunnel into a namespace.
pub async fn connect(
    mut client: Client,
    namespace_id: &str,
    config_only: bool,
) -> anyhow::Result<()> {
    if config_only {
        let config = wg_quick_config(&mut client, namespace_id).await?;
        print!("{}", config);
        return Ok(());
    }

    let provisioned = ProvisionedTunnel::connect(&mut client, namespace_id).await?;
    let info = provisioned.info();
    let dns_domain = format!("{}.dv.local", namespace_id);

    // Try creating a TUN device directly. If we lack privileges,
    // fall back to a privileged helper that creates the TUN device and
    // passes the fd back via SCM_RIGHTS.
    let (tun, escalated_helper) = match TunDevice::create() {
        Ok(tun) => {
            configure_interface(&tun.name, &provisioned.client_ip().to_string(), &provisioned.gateway_ip().to_string(), provisioned.prefix_len())?;
            add_route(provisioned.subnet(), &tun.name)?;
            configure_dns(&tun.name, &provisioned.gateway_ip().to_string(), &dns_domain)?;
            (tun, None)
        }
        Err(e) if is_permission_denied(&e) => {
            let (tun, helper) = escalate_tun(&provisioned, namespace_id).await?;
            (tun, Some(helper))
        }
        Err(e) => return Err(e.context("failed to create TUN device")),
    };

    let (tunn, udp) = provisioned.create_wg_tunnel().await?;
    let tun_name = tun.name.clone();
    let subnet = provisioned.subnet().to_string();
    let tunnel = KernelTunnel::new(tun, tunn, udp, provisioned.endpoint());

    // Create control socket for graceful shutdown from `dv disconnect`.
    let ctrl_sock_path = control_socket_path(namespace_id)?;
    let _ = std::fs::remove_file(&ctrl_sock_path); // clean up stale socket
    let control_listener = tokio::net::UnixListener::bind(&ctrl_sock_path)?;

    // Write connection state.
    let state_path = write_connection_state(
        namespace_id,
        provisioned.public_key(),
        std::process::id(),
        Some(&ctrl_sock_path),
    )?;

    eprintln!(
        "connected to namespace '{}' via {}",
        namespace_id,
        tunnel.tun_name()
    );
    eprintln!("  client IP: {}", info.client_ip);
    eprintln!("  subnet:    {}", info.subnet);
    eprintln!("  endpoint:  {}", info.endpoint);
    eprintln!("press Ctrl+C to disconnect");

    let result = tokio::select! {
        _ = tokio::signal::ctrl_c() => Ok(()),
        r = tunnel.run() => r,
        _ = control_listener.accept() => {
            // Any connection to the control socket triggers graceful shutdown.
            Ok(())
        }
    };

    eprintln!("\ndisconnecting...");

    // Drop the tunnel before cleanup so the TUN device is released.
    drop(tunnel);

    teardown_connection(
        &tun_name,
        &subnet,
        escalated_helper,
        provisioned,
        &mut client,
        namespace_id,
        &state_path,
        &ctrl_sock_path,
    )
    .await;

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

    // Try graceful shutdown via control socket first.
    let mut signalled = false;
    if let Some(ref sock_path) = state.control_socket {
        match std::os::unix::net::UnixStream::connect(sock_path) {
            Ok(_stream) => {
                // Connection alone triggers shutdown; stream is dropped immediately.
                signalled = true;
            }
            Err(e) => {
                log::debug!("control socket connect failed ({}), falling back to SIGTERM", e);
            }
        }
    }

    // Fall back to SIGTERM if control socket was unavailable.
    if !signalled {
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
    }

    // Remove state file.
    let _ = std::fs::remove_file(&state_path);

    eprintln!("disconnected from namespace '{}'", namespace_id);
    Ok(())
}
