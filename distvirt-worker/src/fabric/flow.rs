use std::collections::HashMap;
use std::net::Ipv4Addr;
use std::time::{Duration, Instant};

/// 5-tuple flow key identifying a unique connection.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct FlowKey {
    pub src_ip: Ipv4Addr,
    pub dst_ip: Ipv4Addr,
    pub protocol: u8,
    pub src_port: u16,
    pub dst_port: u16,
}

/// TCP connection state tracked from packet flags.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TcpFlowState {
    /// SYN seen, connection establishing.
    /// Does NOT count as active demand — connection attempts are handled
    /// by the separate EndpointActivation (instantaneous demand) path.
    /// Short timeout (OPENING_TIMEOUT).
    Opening,
    /// Established (SYN+ACK seen or data flowing).
    /// Counts as active demand — keeps workload alive.
    /// Uses IDLE_TIMEOUT.
    Established,
    /// FIN seen from one side; connection winding down.
    /// Counts as active demand — connection still in progress.
    /// Uses HALF_CLOSED_TIMEOUT.
    HalfClosed,
    /// FIN seen from both sides or RST received.
    /// Does not count as active demand. Brief linger (CLOSED_LINGER)
    /// before removal to allow retransmits.
    Closed,
}

/// State for a tracked flow.
#[derive(Debug, Clone)]
pub struct FlowState {
    pub first_seen: Instant,
    pub last_seen: Instant,
    pub tcp_state: TcpFlowState,
}

/// Timeout for Opening (SYN-only) flows.
/// Short because these are connection attempts that haven't completed.
/// If the backend isn't up yet, the SYN will be retransmitted.
const OPENING_TIMEOUT: Duration = Duration::from_secs(10);

/// Hard idle timeout for established connections.
const IDLE_TIMEOUT: Duration = Duration::from_secs(300);

/// Timeout for HalfClosed flows.
/// Connection is winding down (FIN sent from one side).
const HALF_CLOSED_TIMEOUT: Duration = Duration::from_secs(30);

/// Brief linger after Closed state before removal (allow retransmits).
const CLOSED_LINGER: Duration = Duration::from_secs(5);

/// Lightweight TCP flow tracker.
///
/// Tracks TCP connections by observing SYN/FIN/RST flags. Provides
/// `has_active_flows()` as a demand signal to the orchestrator.
#[derive(Debug)]
pub struct FlowTracker {
    flows: HashMap<FlowKey, FlowState>,
}

impl FlowTracker {
    pub fn new() -> Self {
        FlowTracker {
            flows: HashMap::new(),
        }
    }

    /// Track a TCP packet. Call with the TCP flags byte from the packet header.
    ///
    /// Only tracks TCP (protocol 6). Non-TCP packets are ignored.
    pub fn track_packet(&mut self, key: FlowKey, tcp_flags: u8) {
        let now = Instant::now();
        let syn = tcp_flags & 0x02 != 0;
        let fin = tcp_flags & 0x01 != 0;
        let rst = tcp_flags & 0x04 != 0;
        let ack = tcp_flags & 0x10 != 0;

        if let Some(state) = self.flows.get_mut(&key) {
            state.last_seen = now;

            if rst {
                state.tcp_state = TcpFlowState::Closed;
            } else if fin {
                state.tcp_state = match state.tcp_state {
                    TcpFlowState::HalfClosed => TcpFlowState::Closed,
                    _ => TcpFlowState::HalfClosed,
                };
            } else if state.tcp_state == TcpFlowState::Opening && ack {
                state.tcp_state = TcpFlowState::Established;
            }
        } else if syn {
            // New connection: track on SYN.
            self.flows.insert(
                key,
                FlowState {
                    first_seen: now,
                    last_seen: now,
                    tcp_state: if ack {
                        TcpFlowState::Established
                    } else {
                        TcpFlowState::Opening
                    },
                },
            );
        }
    }

    /// Returns true if there are any established or half-closed flows.
    ///
    /// Opening (SYN-only) flows are NOT counted. They represent connection
    /// attempts, not established connections. The activation signal for
    /// connection attempts is handled separately by EndpointActivation
    /// (instantaneous demand). This method tracks sustained demand — keeping
    /// a workload alive while real connections are in progress.
    pub fn has_active_flows(&self) -> bool {
        self.flows.values().any(|s| {
            matches!(
                s.tcp_state,
                TcpFlowState::Established | TcpFlowState::HalfClosed
            )
        })
    }

    /// Remove expired and closed flows using per-state timeouts.
    pub fn gc(&mut self, now: Instant) {
        self.flows.retain(|_, state| {
            let elapsed = now.duration_since(state.last_seen);
            match state.tcp_state {
                TcpFlowState::Opening => elapsed < OPENING_TIMEOUT,
                TcpFlowState::Established => elapsed < IDLE_TIMEOUT,
                TcpFlowState::HalfClosed => elapsed < HALF_CLOSED_TIMEOUT,
                TcpFlowState::Closed => elapsed < CLOSED_LINGER,
            }
        });
    }

    /// Number of tracked flows (for diagnostics).
    pub fn flow_count(&self) -> usize {
        self.flows.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key() -> FlowKey {
        FlowKey {
            src_ip: Ipv4Addr::new(10, 0, 0, 1),
            dst_ip: Ipv4Addr::new(10, 0, 0, 2),
            protocol: 6,
            src_port: 12345,
            dst_port: 80,
        }
    }

    const SYN: u8 = 0x02;
    const SYN_ACK: u8 = 0x12;
    const ACK: u8 = 0x10;
    const FIN_ACK: u8 = 0x11;
    const RST: u8 = 0x04;

    #[test]
    fn syn_creates_opening_flow() {
        let mut ft = FlowTracker::new();
        assert!(!ft.has_active_flows());

        ft.track_packet(key(), SYN);
        // Opening flows do NOT count as active demand.
        assert!(!ft.has_active_flows());
        assert_eq!(ft.flow_count(), 1);
        assert_eq!(ft.flows[&key()].tcp_state, TcpFlowState::Opening);
    }

    #[test]
    fn established_flow_is_active() {
        let mut ft = FlowTracker::new();
        ft.track_packet(key(), SYN);
        ft.track_packet(key(), ACK);
        assert_eq!(ft.flows[&key()].tcp_state, TcpFlowState::Established);
        assert!(ft.has_active_flows());
    }

    #[test]
    fn half_closed_flow_is_active() {
        let mut ft = FlowTracker::new();
        ft.track_packet(key(), SYN);
        ft.track_packet(key(), ACK);
        ft.track_packet(key(), FIN_ACK);
        assert_eq!(ft.flows[&key()].tcp_state, TcpFlowState::HalfClosed);
        assert!(ft.has_active_flows());
    }

    #[test]
    fn syn_ack_creates_established_flow() {
        let mut ft = FlowTracker::new();
        ft.track_packet(key(), SYN_ACK);
        assert_eq!(ft.flows[&key()].tcp_state, TcpFlowState::Established);
        assert!(ft.has_active_flows());
    }

    #[test]
    fn ack_transitions_opening_to_established() {
        let mut ft = FlowTracker::new();
        ft.track_packet(key(), SYN);
        assert_eq!(ft.flows[&key()].tcp_state, TcpFlowState::Opening);

        ft.track_packet(key(), ACK);
        assert_eq!(ft.flows[&key()].tcp_state, TcpFlowState::Established);
    }

    #[test]
    fn fin_transitions_to_half_closed_then_closed() {
        let mut ft = FlowTracker::new();
        ft.track_packet(key(), SYN);
        ft.track_packet(key(), ACK);

        ft.track_packet(key(), FIN_ACK);
        assert_eq!(ft.flows[&key()].tcp_state, TcpFlowState::HalfClosed);
        assert!(ft.has_active_flows());

        ft.track_packet(key(), FIN_ACK);
        assert_eq!(ft.flows[&key()].tcp_state, TcpFlowState::Closed);
        assert!(!ft.has_active_flows());
    }

    #[test]
    fn rst_immediately_closes() {
        let mut ft = FlowTracker::new();
        ft.track_packet(key(), SYN);
        ft.track_packet(key(), RST);
        assert_eq!(ft.flows[&key()].tcp_state, TcpFlowState::Closed);
        assert!(!ft.has_active_flows());
    }

    #[test]
    fn non_syn_packet_does_not_create_flow() {
        let mut ft = FlowTracker::new();
        ft.track_packet(key(), ACK);
        assert_eq!(ft.flow_count(), 0);
        assert!(!ft.has_active_flows());
    }

    #[test]
    fn gc_removes_closed_flows_after_linger() {
        let mut ft = FlowTracker::new();
        ft.track_packet(key(), SYN);
        ft.track_packet(key(), RST);
        assert_eq!(ft.flow_count(), 1);

        // Before linger expires: retained.
        ft.gc(Instant::now());
        assert_eq!(ft.flow_count(), 1);

        // After linger expires: removed.
        let later = Instant::now() + CLOSED_LINGER + Duration::from_secs(1);
        ft.gc(later);
        assert_eq!(ft.flow_count(), 0);
    }

    #[test]
    fn gc_removes_idle_flows() {
        let mut ft = FlowTracker::new();
        ft.track_packet(key(), SYN);
        ft.track_packet(key(), ACK);
        assert_eq!(ft.flows[&key()].tcp_state, TcpFlowState::Established);

        // Before idle timeout: retained.
        ft.gc(Instant::now());
        assert_eq!(ft.flow_count(), 1);

        // After idle timeout: removed.
        let later = Instant::now() + IDLE_TIMEOUT + Duration::from_secs(1);
        ft.gc(later);
        assert_eq!(ft.flow_count(), 0);
    }

    #[test]
    fn gc_removes_opening_flows_quickly() {
        let mut ft = FlowTracker::new();
        ft.track_packet(key(), SYN);
        assert_eq!(ft.flows[&key()].tcp_state, TcpFlowState::Opening);

        // Before OPENING_TIMEOUT: retained.
        ft.gc(Instant::now());
        assert_eq!(ft.flow_count(), 1);

        // After OPENING_TIMEOUT: removed.
        let later = Instant::now() + OPENING_TIMEOUT + Duration::from_secs(1);
        ft.gc(later);
        assert_eq!(ft.flow_count(), 0);
    }

    #[test]
    fn gc_removes_half_closed_flows() {
        let mut ft = FlowTracker::new();
        ft.track_packet(key(), SYN);
        ft.track_packet(key(), ACK);
        ft.track_packet(key(), FIN_ACK);
        assert_eq!(ft.flows[&key()].tcp_state, TcpFlowState::HalfClosed);

        // Before HALF_CLOSED_TIMEOUT: retained.
        ft.gc(Instant::now());
        assert_eq!(ft.flow_count(), 1);

        // After HALF_CLOSED_TIMEOUT: removed.
        let later = Instant::now() + HALF_CLOSED_TIMEOUT + Duration::from_secs(1);
        ft.gc(later);
        assert_eq!(ft.flow_count(), 0);
    }
}
