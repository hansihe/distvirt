//! Shared WireGuard encapsulation/decapsulation helpers.
//!
//! These operate on a `boringtun::noise::Tunn` and return action enums that
//! the caller dispatches to the appropriate I/O layer (TUN device, UDP socket,
//! or smoltcp channel device).

use boringtun::noise::{Tunn, TunnResult};

/// Result of encapsulating an IP packet for WireGuard transport.
pub enum WgEncapAction<'a> {
    /// Encrypted data ready to send on the UDP socket.
    SendToNetwork(&'a [u8]),
    /// Nothing to do (e.g. packet was buffered internally).
    Nothing,
    /// An error occurred during encapsulation.
    Error(boringtun::noise::errors::WireGuardError),
}

/// Result of decapsulating a WireGuard packet.
pub enum WgDecapAction<'a> {
    /// Decrypted IP packet ready to write to the tunnel device.
    WriteToTunnel(&'a [u8]),
    /// Nothing to do (e.g. keepalive with no payload).
    Nothing,
    /// An error occurred during decapsulation.
    Error(boringtun::noise::errors::WireGuardError),
}

/// Format a brief description of an IP packet for logging.
pub fn describe_ip_packet(pkt: &[u8]) -> String {
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
        proto_name, src, dst, ports, pkt.len()
    )
}

/// Encapsulate an IP packet into a WireGuard transport message.
pub fn encapsulate<'a>(tunn: &mut Tunn, pkt: &[u8], buf: &'a mut [u8]) -> WgEncapAction<'a> {
    match tunn.encapsulate(pkt, buf) {
        TunnResult::WriteToNetwork(data) => WgEncapAction::SendToNetwork(data),
        TunnResult::Err(e) => WgEncapAction::Error(e),
        _ => WgEncapAction::Nothing,
    }
}

/// Decapsulate a WireGuard transport message into an IP packet.
///
/// Handles the handshake continuation loop internally: any additional
/// handshake packets that need to be sent are dispatched via `send_fn`.
pub fn decapsulate<'a>(
    tunn: &mut Tunn,
    src_ip: Option<std::net::IpAddr>,
    pkt: &[u8],
    dec_buf: &'a mut [u8],
    cont_buf: &mut [u8],
    send_fn: &mut impl FnMut(&[u8]),
) -> WgDecapAction<'a> {
    match tunn.decapsulate(src_ip, pkt, dec_buf) {
        TunnResult::WriteToTunnelV4(ip_packet, _) => WgDecapAction::WriteToTunnel(ip_packet),
        TunnResult::WriteToNetwork(data) => {
            send_fn(data);
            // Handshake continuation loop.
            loop {
                match tunn.decapsulate(None, &[], cont_buf) {
                    TunnResult::Done => break,
                    TunnResult::WriteToNetwork(data) => send_fn(data),
                    _ => break,
                }
            }
            WgDecapAction::Nothing
        }
        TunnResult::Done => WgDecapAction::Nothing,
        TunnResult::Err(e) => WgDecapAction::Error(e),
        TunnResult::WriteToTunnelV6(_, _) => {
            log::debug!("wg: dropping IPv6 packet");
            WgDecapAction::Nothing
        }
    }
}

/// Run timer maintenance on the WireGuard tunnel.
pub fn timer_tick<'a>(tunn: &mut Tunn, buf: &'a mut [u8]) -> WgEncapAction<'a> {
    match tunn.update_timers(buf) {
        TunnResult::WriteToNetwork(data) => WgEncapAction::SendToNetwork(data),
        TunnResult::Err(e) => WgEncapAction::Error(e),
        _ => WgEncapAction::Nothing,
    }
}
