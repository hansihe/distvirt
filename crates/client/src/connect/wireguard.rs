use std::net::SocketAddr;
use std::sync::Arc;

use boringtun::noise::{Tunn, TunnResult};
use tokio::net::UdpSocket;
use tokio::sync::Mutex;

use super::platform::TunDevice;

const MAX_PACKET_SIZE: usize = 65536;

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
    format!(
        "{} {} → {}{} ({} bytes)",
        proto_name,
        src,
        dst,
        ports,
        pkt.len()
    )
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

/// Run the WireGuard packet forwarding loop.
///
/// This drives three concurrent tasks:
/// - TUN -> WireGuard -> UDP (outbound)
/// - UDP -> WireGuard -> TUN (inbound)
/// - Timer (keepalive / handshake)
///
/// Returns on error; never returns `Ok` naturally.
/// Caller is responsible for cancellation (e.g. `select!` with ctrl+c).
pub(crate) async fn run_tunnel(
    tun: Arc<TunDevice>,
    tunn: Arc<Mutex<Tunn>>,
    udp: Arc<UdpSocket>,
    endpoint: SocketAddr,
) -> anyhow::Result<()> {
    // TUN → WireGuard → UDP
    let tun_to_udp = {
        let tun = Arc::clone(&tun);
        let tunn = Arc::clone(&tunn);
        let udp = Arc::clone(&udp);
        async move {
            let mut tun_buf = vec![0u8; MAX_PACKET_SIZE];
            let mut enc_buf = vec![0u8; MAX_PACKET_SIZE];
            loop {
                let n = tun.read_packet(&mut tun_buf).await?;
                let ip_packet = &tun_buf[..n];
                log::trace!("tun ▶ wg: {}", describe_ip_packet(ip_packet));

                let result = {
                    let mut t = tunn.lock().await;
                    t.encapsulate(ip_packet, &mut enc_buf)
                };

                match result {
                    TunnResult::WriteToNetwork(data) => {
                        log::trace!("wg ▶ udp: {} bytes encrypted → {}", data.len(), endpoint);
                        udp.send_to(data, endpoint).await?;
                    }
                    TunnResult::Err(e) => {
                        log::warn!("wg encapsulate error: {:?}", e);
                    }
                    other => {
                        log::warn!(
                            "wg encapsulate unexpected: {}",
                            describe_tunn_result(&other)
                        );
                    }
                }
            }
            #[allow(unreachable_code)]
            Ok::<(), anyhow::Error>(())
        }
    };

    // UDP → WireGuard → TUN
    let udp_to_tun = {
        let tun = Arc::clone(&tun);
        let tunn = Arc::clone(&tunn);
        let udp = Arc::clone(&udp);
        async move {
            let mut recv_buf = vec![0u8; MAX_PACKET_SIZE];
            let mut dec_buf = vec![0u8; MAX_PACKET_SIZE];
            loop {
                let (n, src) = udp.recv_from(&mut recv_buf).await?;
                let datagram = &recv_buf[..n];

                log::trace!("udp ◀ {}: {} bytes", src, n);
                let result = {
                    let mut t = tunn.lock().await;
                    t.decapsulate(Some(src.ip()), datagram, &mut dec_buf)
                };

                match result {
                    TunnResult::Done => {
                        log::trace!("wg ◀ decapsulate: Done (no data)");
                    }
                    TunnResult::Err(e) => {
                        log::warn!("wg decapsulate error: {:?}", e);
                    }
                    TunnResult::WriteToNetwork(data) => {
                        log::debug!("wg ◀ handshake response, sending {} bytes", data.len());
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
                                    log::debug!(
                                        "wg ◀ handshake continuation: sending {} bytes",
                                        data.len()
                                    );
                                    let data = data.to_vec();
                                    udp.send_to(&data, endpoint).await?;
                                }
                                _ => break,
                            }
                        }
                    }
                    TunnResult::WriteToTunnelV4(ip_packet, _) => {
                        log::trace!("wg ◀ tun: {}", describe_ip_packet(ip_packet));
                        tun.write_packet(ip_packet).await?;
                    }
                    TunnResult::WriteToTunnelV6(_, _) => {
                        log::debug!("wg ◀ dropping IPv6 packet");
                    }
                }
            }
            #[allow(unreachable_code)]
            Ok::<(), anyhow::Error>(())
        }
    };

    // Timer task: update_timers every 250ms
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
                        log::warn!("wg timer error: {:?}", e);
                    }
                    TunnResult::WriteToNetwork(data) => {
                        log::debug!(
                            "wg timer: sending {} bytes (handshake init / keepalive)",
                            data.len()
                        );
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
                    eprintln!("wg handshake complete!{}", rtt_str);
                    was_connected = true;
                } else if !is_connected && was_connected {
                    eprintln!("wg session lost, re-handshaking...");
                    was_connected = false;
                }

                // Print periodic status every ~5s (20 ticks * 250ms).
                status_tick += 1;
                if status_tick % 20 == 0 {
                    let hs_str = match time_since_hs {
                        Some(d) => format!("{}s ago", d.as_secs()),
                        None => "no session".to_string(),
                    };
                    log::debug!(
                        "wg status: handshake={}, tx={}, rx={}, loss={:.1}%",
                        hs_str,
                        tx_bytes,
                        rx_bytes,
                        loss * 100.0
                    );
                }
            }
            #[allow(unreachable_code)]
            Ok::<(), anyhow::Error>(())
        }
    };

    tokio::select! {
        r = tun_to_udp => r,
        r = udp_to_tun => r,
        r = timer => r,
    }
}
