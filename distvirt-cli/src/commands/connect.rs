use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{Context, bail};
use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64;
use boringtun::noise::{Tunn, TunnResult};
use boringtun::x25519::{PublicKey, StaticSecret};
use distvirt_client_protocol::*;
use tokio::net::UdpSocket;
use tokio::sync::Mutex;

use rand::rngs::OsRng;

use crate::client::{self, Client};
use crate::connection::ConnectionParams;
use crate::platform::{TunDevice, add_route, configure_interface, remove_route};

const MAX_PACKET_SIZE: usize = 65536;

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
    // 1. Generate ephemeral X25519 keypair.
    let private_key = StaticSecret::random_from_rng(OsRng);
    let public_key = PublicKey::from(&private_key);

    // 2. Call ConnectNetwork gRPC.
    let resp = client
        .connect_network(ConnectNetworkRequest {
            namespace_id: namespace_id.to_string(),
            client_public_key: public_key.as_bytes().to_vec(),
        })
        .await
        .map_err(client::handle_grpc_error)?
        .into_inner();

    let server_public_key_bytes: [u8; 32] = resp
        .server_public_key
        .as_slice()
        .try_into()
        .context("server public key must be 32 bytes")?;
    let endpoint: SocketAddr = resp.endpoint.parse().context("invalid endpoint address")?;
    let client_ip = &resp.client_ip;
    let subnet = &resp.subnet;

    // Parse prefix length from subnet CIDR (e.g. "172.16.0.0/24" → 24).
    let prefix_len: u8 = subnet
        .split('/')
        .nth(1)
        .context("subnet missing /prefix_len")?
        .parse()
        .context("invalid prefix length in subnet")?;

    // 3. If --config: print wg-quick format and exit.
    if config_only {
        let private_key_b64 = BASE64.encode(private_key.to_bytes());
        let server_pub_b64 = BASE64.encode(server_public_key_bytes);
        println!("[Interface]");
        println!("PrivateKey = {}", private_key_b64);
        println!("Address = {}/{}", client_ip, prefix_len);
        println!();
        println!("[Peer]");
        println!("PublicKey = {}", server_pub_b64);
        println!("Endpoint = {}", endpoint);
        println!("AllowedIPs = {}", subnet);
        println!("PersistentKeepalive = 25");
        return Ok(());
    }

    // 4. Create TUN device, escalating to sudo on permission denied.
    let tun = match TunDevice::create() {
        Ok(tun) => tun,
        Err(e) if is_permission_denied(&e) => {
            return reexec_with_sudo(&params);
        }
        Err(e) => return Err(e.context("failed to create TUN device")),
    };
    let tun_name = tun.name.clone();

    // 5. Configure IP + routes.
    configure_interface(&tun_name, client_ip, prefix_len)?;
    add_route(subnet, &tun_name)?;

    // 6. Create boringtun tunnel.
    let server_public = PublicKey::from(server_public_key_bytes);
    let tunn = Tunn::new(
        private_key.clone(),
        server_public,
        None,
        Some(25), // persistent keepalive
        0,
        None,
    );
    let tunn = Arc::new(Mutex::new(tunn));

    // 7. Open UDP socket.
    let udp = Arc::new(UdpSocket::bind("0.0.0.0:0").await?);

    // Write connection state file.
    let state = ConnectionState {
        public_key: BASE64.encode(public_key.as_bytes()),
        pid: std::process::id(),
    };
    let state_path = state_file_path(namespace_id)?;
    std::fs::write(&state_path, serde_json::to_string(&state)?)?;

    // 8. Print connection info.
    eprintln!("connected to namespace '{}' via {}", namespace_id, tun_name);
    eprintln!("  client IP: {}", client_ip);
    eprintln!("  subnet:    {}", subnet);
    eprintln!("  endpoint:  {}", endpoint);
    eprintln!("press Ctrl+C to disconnect");

    // 9. Run packet forwarding loop.
    let result = run_tunnel(tun, Arc::clone(&tunn), Arc::clone(&udp), endpoint).await;

    // 10. Cleanup on exit.
    eprintln!("\ndisconnecting...");

    // Remove state file.
    let _ = std::fs::remove_file(&state_path);

    // Remove route (best-effort, interface going away will clean it too).
    let _ = remove_route(subnet, &tun_name);

    // Call DisconnectNetwork gRPC.
    let disconnect_result = client
        .disconnect_network(DisconnectNetworkRequest {
            namespace_id: namespace_id.to_string(),
            client_public_key: public_key.as_bytes().to_vec(),
        })
        .await;

    if let Err(e) = disconnect_result {
        log::warn!("disconnect gRPC failed: {}", e);
    }

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
        .map_err(client::handle_grpc_error)?;

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

/// Format a brief description of an IP packet for logging.
fn describe_ip_packet(pkt: &[u8]) -> String {
    if pkt.len() < 20 {
        return format!("{} bytes (runt)", pkt.len());
    }
    let proto = pkt[9];
    let src = std::net::Ipv4Addr::new(pkt[12], pkt[13], pkt[14], pkt[15]);
    let dst = std::net::Ipv4Addr::new(pkt[16], pkt[17], pkt[18], pkt[19]);
    let proto_name = match proto {
        1 => "ICMP",
        6 => "TCP",
        17 => "UDP",
        _ => "??",
    };
    let ihl = (pkt[0] & 0x0f) as usize * 4;
    let ports = if (proto == 6 || proto == 17) && pkt.len() >= ihl + 4 {
        let sp = u16::from_be_bytes([pkt[ihl], pkt[ihl + 1]]);
        let dp = u16::from_be_bytes([pkt[ihl + 2], pkt[ihl + 3]]);
        format!(" {}→{}", sp, dp)
    } else {
        String::new()
    };
    format!("{} {} → {}{} ({} bytes)", proto_name, src, dst, ports, pkt.len())
}

/// Main tunnel forwarding loop. Returns on Ctrl+C or error.
async fn run_tunnel(
    tun: TunDevice,
    tunn: Arc<Mutex<Tunn>>,
    udp: Arc<UdpSocket>,
    endpoint: SocketAddr,
) -> anyhow::Result<()> {
    let tun: Arc<TunDevice> = Arc::new(tun);

    // TUN → WireGuard → UDP
    let tun_to_udp = {
        let tun: Arc<TunDevice> = Arc::clone(&tun);
        let tunn = Arc::clone(&tunn);
        let udp = Arc::clone(&udp);
        async move {
            let mut tun_buf = vec![0u8; MAX_PACKET_SIZE];
            let mut enc_buf = vec![0u8; MAX_PACKET_SIZE];
            loop {
                let n = tun.read_packet(&mut tun_buf).await?;
                let ip_packet = &tun_buf[..n];
                eprintln!("  tun ▶ wg: {}", describe_ip_packet(ip_packet));

                let result = {
                    let mut t = tunn.lock().await;
                    t.encapsulate(ip_packet, &mut enc_buf)
                };

                match result {
                    TunnResult::WriteToNetwork(data) => {
                        eprintln!("  wg  ▶ udp: {} bytes encrypted → {}", data.len(), endpoint);
                        udp.send_to(data, endpoint).await?;
                    }
                    TunnResult::Err(e) => {
                        eprintln!("  wg  encapsulate error: {:?}", e);
                    }
                    other => {
                        eprintln!("  wg  encapsulate unexpected: {}", describe_tunn_result(&other));
                    }
                }
            }
            #[allow(unreachable_code)]
            Ok::<(), anyhow::Error>(())
        }
    };

    // UDP → WireGuard → TUN
    let udp_to_tun = {
        let tun: Arc<TunDevice> = Arc::clone(&tun);
        let tunn = Arc::clone(&tunn);
        let udp = Arc::clone(&udp);
        async move {
            let mut recv_buf = vec![0u8; MAX_PACKET_SIZE];
            let mut dec_buf = vec![0u8; MAX_PACKET_SIZE];
            loop {
                let (n, src) = udp.recv_from(&mut recv_buf).await?;
                let datagram = &recv_buf[..n];

                eprintln!("  udp ◀ {}: {} bytes", src, n);
                let result = {
                    let mut t = tunn.lock().await;
                    t.decapsulate(Some(src.ip()), datagram, &mut dec_buf)
                };

                match result {
                    TunnResult::Done => {
                        eprintln!("  wg  ◀ decapsulate: Done (no data)");
                    }
                    TunnResult::Err(e) => {
                        eprintln!("  wg  ◀ decapsulate error: {:?}", e);
                    }
                    TunnResult::WriteToNetwork(data) => {
                        eprintln!("  wg  ◀ decapsulate: handshake response, sending {} bytes", data.len());
                        let data = data.to_vec();
                        udp.send_to(&data, endpoint).await?;
                        // Handshake continuation loop.
                        let mut cont_buf = vec![0u8; MAX_PACKET_SIZE];
                        loop {
                            let cont = {
                                let mut t = tunn.lock().await;
                                t.decapsulate(None, &[], &mut cont_buf)
                            };
                            match cont {
                                TunnResult::Done => break,
                                TunnResult::WriteToNetwork(data) => {
                                    eprintln!("  wg  ◀ handshake continuation: sending {} bytes", data.len());
                                    let data = data.to_vec();
                                    udp.send_to(&data, endpoint).await?;
                                }
                                _ => break,
                            }
                        }
                    }
                    TunnResult::WriteToTunnelV4(ip_packet, _) => {
                        eprintln!("  wg  ◀ tun: {}", describe_ip_packet(ip_packet));
                        tun.write_packet(ip_packet).await?;
                    }
                    TunnResult::WriteToTunnelV6(_, _) => {
                        eprintln!("  wg  ◀ dropping IPv6 packet");
                    }
                }
            }
            #[allow(unreachable_code)]
            Ok::<(), anyhow::Error>(())
        }
    };

    // Timer task: update_timers every 250ms, also prints WireGuard status periodically
    let timer = {
        let tunn = Arc::clone(&tunn);
        let udp = Arc::clone(&udp);
        async move {
            let mut interval = tokio::time::interval(std::time::Duration::from_millis(250));
            let mut timer_buf = vec![0u8; MAX_PACKET_SIZE];
            let mut was_connected = false;
            let mut status_tick: u32 = 0;
            loop {
                interval.tick().await;
                let (result, stats) = {
                    let mut t = tunn.lock().await;
                    let r = t.update_timers(&mut timer_buf);
                    let s = t.stats();
                    (r, s)
                };
                match result {
                    TunnResult::Done => {}
                    TunnResult::Err(e) => {
                        eprintln!("  wg  timer error: {:?}", e);
                    }
                    TunnResult::WriteToNetwork(data) => {
                        eprintln!("  wg  timer: sending {} bytes (handshake init / keepalive)", data.len());
                        let data = data.to_vec();
                        udp.send_to(&data, endpoint).await?;
                    }
                    _ => {}
                }

                // Check handshake status.
                let (time_since_hs, tx_bytes, rx_bytes, loss, rtt) = stats;
                let is_connected = time_since_hs.is_some();
                if is_connected && !was_connected {
                    let rtt_str = rtt.map(|r| format!(" rtt={}ms", r)).unwrap_or_default();
                    eprintln!("  wg  handshake complete!{}", rtt_str);
                    was_connected = true;
                } else if !is_connected && was_connected {
                    eprintln!("  wg  session lost, re-handshaking...");
                    was_connected = false;
                }

                // Print periodic status every ~5s (20 ticks * 250ms).
                status_tick += 1;
                if status_tick % 20 == 0 {
                    let hs_str = match time_since_hs {
                        Some(d) => format!("{}s ago", d.as_secs()),
                        None => "no session".to_string(),
                    };
                    eprintln!(
                        "  wg  status: handshake={}, tx={}, rx={}, loss={:.1}%",
                        hs_str, tx_bytes, rx_bytes, loss * 100.0
                    );
                }
            }
            #[allow(unreachable_code)]
            Ok::<(), anyhow::Error>(())
        }
    };

    tokio::select! {
        _ = tokio::signal::ctrl_c() => {
            Ok(())
        }
        r = tun_to_udp => r,
        r = udp_to_tun => r,
        r = timer => r,
    }
}

fn describe_tunn_result(result: &TunnResult) -> String {
    match result {
        TunnResult::Done => "Done".to_string(),
        TunnResult::Err(e) => format!("Err({:?})", e),
        TunnResult::WriteToNetwork(d) => format!("WriteToNetwork({} bytes)", d.len()),
        TunnResult::WriteToTunnelV4(d, _) => format!("WriteToTunnelV4({} bytes)", d.len()),
        TunnResult::WriteToTunnelV6(d, _) => format!("WriteToTunnelV6({} bytes)", d.len()),
    }
}
