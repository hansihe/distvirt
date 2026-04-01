use std::net::SocketAddr;
use std::sync::Arc;

use boringtun::noise::Tunn;
use tokio::net::UdpSocket;
use tokio::sync::Mutex;

use super::platform::TunDevice;
use super::wg_ops::{self, WgDecapAction, WgEncapAction, describe_ip_packet};

const MAX_PACKET_SIZE: usize = 65536;

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
            let mut scratch = Vec::new();
            loop {
                let n = tun.read_packet(&mut tun_buf, &mut scratch).await?;
                let ip_packet = &tun_buf[..n];
                log::trace!("tun ▶ wg: {}", describe_ip_packet(ip_packet));

                let result = {
                    let mut t = tunn.lock().await;
                    wg_ops::encapsulate(&mut t, ip_packet, &mut enc_buf)
                };

                match result {
                    WgEncapAction::SendToNetwork(data) => {
                        log::trace!("wg ▶ udp: {} bytes encrypted → {}", data.len(), endpoint);
                        udp.send_to(data, endpoint).await?;
                    }
                    WgEncapAction::Error(e) => {
                        log::warn!("wg encapsulate error: {:?}", e);
                    }
                    WgEncapAction::Nothing => {
                        log::warn!("wg encapsulate: unexpected Nothing result");
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
            let mut cont_buf = vec![0u8; MAX_PACKET_SIZE];
            loop {
                let (n, src) = udp.recv_from(&mut recv_buf).await?;
                let datagram = &recv_buf[..n];

                log::trace!("udp ◀ {}: {} bytes", src, n);

                // Collect handshake continuation packets to send after releasing the lock.
                let mut to_send: Vec<Vec<u8>> = Vec::new();
                let result = {
                    let mut t = tunn.lock().await;
                    wg_ops::decapsulate(
                        &mut t,
                        Some(src.ip()),
                        datagram,
                        &mut dec_buf,
                        &mut cont_buf,
                        &mut |data| to_send.push(data.to_vec()),
                    )
                };

                // Send any handshake continuation packets.
                for data in &to_send {
                    log::debug!("wg ◀ handshake: sending {} bytes", data.len());
                    udp.send_to(data, endpoint).await?;
                }

                match result {
                    WgDecapAction::WriteToTunnel(ip_packet) => {
                        log::trace!("wg ◀ tun: {}", describe_ip_packet(ip_packet));
                        tun.write_packet(ip_packet).await?;
                    }
                    WgDecapAction::Nothing => {}
                    WgDecapAction::Error(e) => {
                        log::warn!("wg decapsulate error: {:?}", e);
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
                    let r = wg_ops::timer_tick(&mut t, &mut timer_buf);
                    let s = t.stats();
                    (r, s)
                };
                match result {
                    WgEncapAction::SendToNetwork(data) => {
                        log::debug!(
                            "wg timer: sending {} bytes (handshake init / keepalive)",
                            data.len()
                        );
                        let data = data.to_vec();
                        udp.send_to(&data, endpoint).await?;
                    }
                    WgEncapAction::Error(e) => {
                        log::warn!("wg timer error: {:?}", e);
                    }
                    WgEncapAction::Nothing => {}
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
