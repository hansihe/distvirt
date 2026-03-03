use crate::protocol::*;

/// Phase of the client-side Postgres connection.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Phase {
    /// Waiting for the client's startup message (or SSLRequest).
    AwaitingStartup,
    /// Client sent SSLRequest, we responded `N`, waiting for real startup.
    SslNegotiation,
    /// Got StartupMessage, buffered it, waiting for backend.
    Startup,
    /// Backend connected, forwarding bytes bidirectionally.
    Proxying,
    /// Connection terminating.
    Closing,
}

/// Actions the connection wants the caller to perform.
pub enum ConnAction {
    /// Send data to the downstream (client) TCP connection.
    DownstreamSend(Vec<u8>),
    /// Send data to the upstream (backend) TCP connection.
    UpstreamSend(Vec<u8>),
    /// Close the downstream TCP connection.
    DownstreamClose,
    /// Close the upstream TCP connection.
    UpstreamClose,
    /// Log a message.
    Log(String),
}

pub struct Connection {
    phase: Phase,
    /// Buffer for incoming client bytes before we have a complete startup message.
    recv_buf: Vec<u8>,
    /// Buffer for incoming upstream bytes (for ReadyForQuery parsing).
    upstream_recv_buf: Vec<u8>,
    /// Data buffered while waiting for backend connection.
    buffered_data: Vec<u8>,
    /// Last observed transaction status from ReadyForQuery.
    last_txn_status: Option<TransactionStatus>,
    /// Whether we've seen at least one ReadyForQuery (auth complete).
    auth_complete: bool,
}

impl Connection {
    pub fn new() -> Self {
        Self {
            phase: Phase::AwaitingStartup,
            recv_buf: Vec::new(),
            upstream_recv_buf: Vec::new(),
            buffered_data: Vec::new(),
            last_txn_status: None,
            auth_complete: false,
        }
    }

    pub fn phase(&self) -> Phase {
        self.phase
    }

    /// Returns true if the connection is idle (last ReadyForQuery status was `I`
    /// and auth is complete).
    pub fn is_idle(&self) -> bool {
        self.auth_complete && self.last_txn_status == Some(TransactionStatus::Idle)
    }

    /// Returns true if the connection has a startup message buffered and is
    /// waiting for a backend connection.
    pub fn has_buffered_startup(&self) -> bool {
        self.phase == Phase::Startup && !self.buffered_data.is_empty()
    }

    /// Process data received from the client (downstream).
    pub fn on_client_data(&mut self, data: &[u8], actions: &mut Vec<ConnAction>) {
        match self.phase {
            Phase::AwaitingStartup | Phase::SslNegotiation => {
                self.recv_buf.extend_from_slice(data);
                self.try_parse_startup(actions);
            }
            Phase::Startup => {
                // Buffer additional data until upstream is ready
                self.buffered_data.extend_from_slice(data);
            }
            Phase::Proxying => {
                if self.is_idle() {
                    // When idle, check for interceptable health-check queries
                    // before forwarding to upstream.
                    self.recv_buf.extend_from_slice(data);
                    self.drain_client_proxying(actions);
                } else {
                    // Forward directly to upstream
                    actions.push(ConnAction::UpstreamSend(data.to_vec()));
                }
            }
            Phase::Closing => {}
        }
    }

    /// Called when the upstream TCP connection is established.
    pub fn on_upstream_connected(&mut self, actions: &mut Vec<ConnAction>) {
        if self.phase != Phase::Startup {
            return;
        }
        self.phase = Phase::Proxying;
        // Flush buffered startup message + any queued data
        if !self.buffered_data.is_empty() {
            let data = std::mem::take(&mut self.buffered_data);
            actions.push(ConnAction::UpstreamSend(data));
        }
    }

    /// Process data received from the upstream (backend).
    pub fn on_upstream_data(&mut self, data: &[u8], actions: &mut Vec<ConnAction>) {
        if self.phase == Phase::Closing {
            return;
        }
        // Forward to client
        actions.push(ConnAction::DownstreamSend(data.to_vec()));
        // Parse for ReadyForQuery to track transaction state
        self.scan_upstream_for_ready(data);
    }

    /// Handle upstream connection closed.
    pub fn on_upstream_closed(&mut self, actions: &mut Vec<ConnAction>) {
        if self.phase != Phase::Closing {
            actions.push(ConnAction::DownstreamClose);
            self.phase = Phase::Closing;
        }
    }

    /// Handle upstream connection failed.
    pub fn on_upstream_failed(&mut self, actions: &mut Vec<ConnAction>) {
        if self.phase != Phase::Closing {
            actions.push(ConnAction::DownstreamClose);
            self.phase = Phase::Closing;
        }
    }

    /// Drain client data while in Proxying phase.
    /// Tries to intercept health-check queries when idle; forwards everything
    /// else to upstream. Once a non-interceptable message is seen, flushes
    /// remaining bytes directly.
    fn drain_client_proxying(&mut self, actions: &mut Vec<ConnAction>) {
        loop {
            if self.recv_buf.is_empty() {
                break;
            }

            // Try to parse a complete message
            let Some((tag, total)) = parse_tagged_message(&self.recv_buf) else {
                // Incomplete message — leave in buffer for next call
                break;
            };

            // Only intercept simple Query messages when idle
            if self.is_idle() && tag == MSG_QUERY {
                if let Some(query) = extract_simple_query(&self.recv_buf[..total]) {
                    if is_health_check_query(query) {
                        self.recv_buf.drain(..total);
                        actions.push(ConnAction::DownstreamSend(build_select_1_response()));
                        // State stays idle — we synthesized ReadyForQuery(I)
                        continue;
                    }
                }
            }

            // Not interceptable — forward this message and flush remaining bytes
            // directly (we're no longer idle after forwarding a real query).
            let remaining = std::mem::take(&mut self.recv_buf);
            actions.push(ConnAction::UpstreamSend(remaining));
            break;
        }
    }

    /// Try to parse startup messages from recv_buf.
    fn try_parse_startup(&mut self, actions: &mut Vec<ConnAction>) {
        loop {
            let Some((msg, consumed)) = parse_startup_message(&self.recv_buf) else {
                break;
            };

            match msg {
                StartupMessage::SslRequest => {
                    self.recv_buf.drain(..consumed);
                    // Respond 'N' — no SSL support
                    actions.push(ConnAction::DownstreamSend(vec![b'N']));
                    self.phase = Phase::SslNegotiation;
                }
                StartupMessage::Startup { raw } => {
                    self.recv_buf.drain(..consumed);
                    // Buffer the startup message for when upstream connects
                    self.buffered_data.extend_from_slice(&raw);
                    // Any remaining bytes in recv_buf are early-bird data
                    if !self.recv_buf.is_empty() {
                        let remaining = std::mem::take(&mut self.recv_buf);
                        self.buffered_data.extend_from_slice(&remaining);
                    }
                    self.phase = Phase::Startup;
                    break;
                }
            }
        }
    }

    /// Scan upstream data for ReadyForQuery messages to track transaction state.
    fn scan_upstream_for_ready(&mut self, data: &[u8]) {
        self.upstream_recv_buf.extend_from_slice(data);

        loop {
            let Some((tag, total)) = parse_tagged_message(&self.upstream_recv_buf) else {
                break;
            };

            if tag == MSG_READY_FOR_QUERY {
                if let Some(status) =
                    parse_ready_for_query_status(&self.upstream_recv_buf[..total])
                {
                    self.last_txn_status = Some(status);
                    if !self.auth_complete {
                        self.auth_complete = true;
                    }
                }
            }

            self.upstream_recv_buf.drain(..total);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_startup_message(user: &str) -> Vec<u8> {
        let params = format!("user\0{}\0\0", user);
        let length = (4 + 4 + params.len()) as u32;
        let mut buf = Vec::new();
        buf.extend_from_slice(&length.to_be_bytes());
        buf.extend_from_slice(&PROTOCOL_VERSION_3_0.to_be_bytes());
        buf.extend_from_slice(params.as_bytes());
        buf
    }

    fn make_ssl_request() -> Vec<u8> {
        let mut buf = Vec::new();
        buf.extend_from_slice(&8u32.to_be_bytes());
        buf.extend_from_slice(&SSL_REQUEST_CODE.to_be_bytes());
        buf
    }

    fn make_ready_for_query(status: u8) -> Vec<u8> {
        vec![b'Z', 0, 0, 0, 5, status]
    }

    fn make_tagged_message(tag: u8, payload: &[u8]) -> Vec<u8> {
        let length = (4 + payload.len()) as u32;
        let mut buf = Vec::new();
        buf.push(tag);
        buf.extend_from_slice(&length.to_be_bytes());
        buf.extend_from_slice(payload);
        buf
    }

    #[test]
    fn test_startup_flow() {
        let mut conn = Connection::new();
        assert_eq!(conn.phase(), Phase::AwaitingStartup);

        let startup = make_startup_message("test");
        let mut actions = Vec::new();
        conn.on_client_data(&startup, &mut actions);

        assert_eq!(conn.phase(), Phase::Startup);
        assert!(conn.has_buffered_startup());
        assert!(actions.is_empty());
    }

    #[test]
    fn test_ssl_then_startup() {
        let mut conn = Connection::new();
        let mut actions = Vec::new();

        // Send SSLRequest
        conn.on_client_data(&make_ssl_request(), &mut actions);
        assert_eq!(conn.phase(), Phase::SslNegotiation);
        assert_eq!(actions.len(), 1);
        match &actions[0] {
            ConnAction::DownstreamSend(data) => assert_eq!(data, &[b'N']),
            _ => panic!("expected DownstreamSend"),
        }

        // Send real startup
        actions.clear();
        conn.on_client_data(&make_startup_message("test"), &mut actions);
        assert_eq!(conn.phase(), Phase::Startup);
        assert!(conn.has_buffered_startup());
    }

    #[test]
    fn test_upstream_connected_flushes_buffer() {
        let mut conn = Connection::new();
        let mut actions = Vec::new();

        let startup = make_startup_message("test");
        conn.on_client_data(&startup, &mut actions);
        assert_eq!(conn.phase(), Phase::Startup);

        // Simulate upstream connect
        actions.clear();
        conn.on_upstream_connected(&mut actions);
        assert_eq!(conn.phase(), Phase::Proxying);
        assert_eq!(actions.len(), 1);
        match &actions[0] {
            ConnAction::UpstreamSend(data) => assert_eq!(data, &startup),
            _ => panic!("expected UpstreamSend"),
        }
    }

    #[test]
    fn test_proxying_forwards_client_data() {
        let mut conn = Connection::new();
        let mut actions = Vec::new();

        conn.on_client_data(&make_startup_message("test"), &mut actions);
        conn.on_upstream_connected(&mut actions);
        actions.clear();

        let query = make_tagged_message(b'Q', b"SELECT 1\0");
        conn.on_client_data(&query, &mut actions);
        assert_eq!(actions.len(), 1);
        match &actions[0] {
            ConnAction::UpstreamSend(data) => assert_eq!(data, &query),
            _ => panic!("expected UpstreamSend"),
        }
    }

    #[test]
    fn test_upstream_data_forwarded_and_ready_tracked() {
        let mut conn = Connection::new();
        let mut actions = Vec::new();

        conn.on_client_data(&make_startup_message("test"), &mut actions);
        conn.on_upstream_connected(&mut actions);
        actions.clear();

        assert!(!conn.is_idle());

        // Send auth OK + ReadyForQuery(Idle)
        let mut upstream_data = Vec::new();
        upstream_data.extend_from_slice(&make_tagged_message(b'R', &[0, 0, 0, 0])); // AuthOk
        upstream_data.extend_from_slice(&make_ready_for_query(b'I'));

        conn.on_upstream_data(&upstream_data, &mut actions);

        // Should have forwarded to downstream
        assert_eq!(actions.len(), 1);
        match &actions[0] {
            ConnAction::DownstreamSend(data) => assert_eq!(data, &upstream_data),
            _ => panic!("expected DownstreamSend"),
        }

        // Should now be idle
        assert!(conn.is_idle());
    }

    #[test]
    fn test_transaction_tracking() {
        let mut conn = Connection::new();
        let mut actions = Vec::new();

        conn.on_client_data(&make_startup_message("test"), &mut actions);
        conn.on_upstream_connected(&mut actions);
        actions.clear();

        // ReadyForQuery(Idle) — auth complete
        conn.on_upstream_data(&make_ready_for_query(b'I'), &mut actions);
        assert!(conn.is_idle());

        // ReadyForQuery(InTransaction)
        actions.clear();
        conn.on_upstream_data(&make_ready_for_query(b'T'), &mut actions);
        assert!(!conn.is_idle());

        // ReadyForQuery(Failed)
        actions.clear();
        conn.on_upstream_data(&make_ready_for_query(b'E'), &mut actions);
        assert!(!conn.is_idle());

        // ReadyForQuery(Idle) again
        actions.clear();
        conn.on_upstream_data(&make_ready_for_query(b'I'), &mut actions);
        assert!(conn.is_idle());
    }

    #[test]
    fn test_upstream_closed_closes_downstream() {
        let mut conn = Connection::new();
        let mut actions = Vec::new();

        conn.on_client_data(&make_startup_message("test"), &mut actions);
        conn.on_upstream_connected(&mut actions);
        actions.clear();

        conn.on_upstream_closed(&mut actions);
        assert_eq!(conn.phase(), Phase::Closing);
        assert!(actions.iter().any(|a| matches!(a, ConnAction::DownstreamClose)));
    }

    #[test]
    fn test_upstream_failed_closes_downstream() {
        let mut conn = Connection::new();
        let mut actions = Vec::new();

        conn.on_client_data(&make_startup_message("test"), &mut actions);
        actions.clear();

        conn.on_upstream_failed(&mut actions);
        assert_eq!(conn.phase(), Phase::Closing);
        assert!(actions.iter().any(|a| matches!(a, ConnAction::DownstreamClose)));
    }

    #[test]
    fn test_buffering_during_startup_phase() {
        let mut conn = Connection::new();
        let mut actions = Vec::new();

        let startup = make_startup_message("test");
        conn.on_client_data(&startup, &mut actions);
        assert_eq!(conn.phase(), Phase::Startup);

        // Client sends additional data before upstream is ready
        let extra = make_tagged_message(b'Q', b"SELECT 1\0");
        conn.on_client_data(&extra, &mut actions);

        // Should still be in Startup phase, data buffered
        assert_eq!(conn.phase(), Phase::Startup);

        // Now connect upstream — should flush startup + extra data
        actions.clear();
        conn.on_upstream_connected(&mut actions);
        assert_eq!(actions.len(), 1);
        match &actions[0] {
            ConnAction::UpstreamSend(data) => {
                let mut expected = startup.clone();
                expected.extend_from_slice(&extra);
                assert_eq!(data, &expected);
            }
            _ => panic!("expected UpstreamSend"),
        }
    }

    #[test]
    fn test_ready_for_query_across_chunks() {
        let mut conn = Connection::new();
        let mut actions = Vec::new();

        conn.on_client_data(&make_startup_message("test"), &mut actions);
        conn.on_upstream_connected(&mut actions);
        actions.clear();

        // Send ReadyForQuery split across two chunks
        let rfq = make_ready_for_query(b'I');
        conn.on_upstream_data(&rfq[..3], &mut actions);
        assert!(!conn.is_idle());

        conn.on_upstream_data(&rfq[3..], &mut actions);
        assert!(conn.is_idle());
    }

    fn make_query_message(query: &str) -> Vec<u8> {
        let payload_len = query.len() + 1;
        let length = (4 + payload_len) as u32;
        let mut buf = Vec::new();
        buf.push(b'Q');
        buf.extend_from_slice(&length.to_be_bytes());
        buf.extend_from_slice(query.as_bytes());
        buf.push(0);
        buf
    }

    #[test]
    fn test_intercept_select_1_while_idle() {
        let mut conn = Connection::new();
        let mut actions = Vec::new();

        conn.on_client_data(&make_startup_message("test"), &mut actions);
        conn.on_upstream_connected(&mut actions);
        actions.clear();

        // Make connection idle
        conn.on_upstream_data(&make_ready_for_query(b'I'), &mut actions);
        assert!(conn.is_idle());
        actions.clear();

        // Send SELECT 1
        let query = make_query_message("SELECT 1");
        conn.on_client_data(&query, &mut actions);

        // Should be intercepted — response sent to downstream, nothing to upstream
        assert_eq!(actions.len(), 1);
        match &actions[0] {
            ConnAction::DownstreamSend(data) => {
                // Verify response contains ReadyForQuery at the end
                let rfq_start = data.len() - 6;
                assert_eq!(
                    parse_ready_for_query_status(&data[rfq_start..]),
                    Some(TransactionStatus::Idle)
                );
            }
            _ => panic!("expected DownstreamSend, got upstream send or other"),
        }

        // Should still be idle
        assert!(conn.is_idle());
    }

    #[test]
    fn test_intercept_select_1_case_insensitive() {
        let mut conn = Connection::new();
        let mut actions = Vec::new();

        conn.on_client_data(&make_startup_message("test"), &mut actions);
        conn.on_upstream_connected(&mut actions);
        conn.on_upstream_data(&make_ready_for_query(b'I'), &mut actions);
        actions.clear();

        conn.on_client_data(&make_query_message("select 1;"), &mut actions);
        assert_eq!(actions.len(), 1);
        assert!(matches!(&actions[0], ConnAction::DownstreamSend(_)));
    }

    #[test]
    fn test_no_intercept_when_not_idle() {
        let mut conn = Connection::new();
        let mut actions = Vec::new();

        conn.on_client_data(&make_startup_message("test"), &mut actions);
        conn.on_upstream_connected(&mut actions);
        actions.clear();

        // Connection is NOT idle (no ReadyForQuery seen yet)
        assert!(!conn.is_idle());

        let query = make_query_message("SELECT 1");
        conn.on_client_data(&query, &mut actions);

        // Should be forwarded to upstream
        assert_eq!(actions.len(), 1);
        assert!(matches!(&actions[0], ConnAction::UpstreamSend(_)));
    }

    #[test]
    fn test_no_intercept_for_real_query() {
        let mut conn = Connection::new();
        let mut actions = Vec::new();

        conn.on_client_data(&make_startup_message("test"), &mut actions);
        conn.on_upstream_connected(&mut actions);
        conn.on_upstream_data(&make_ready_for_query(b'I'), &mut actions);
        actions.clear();

        // A real query should NOT be intercepted
        let query = make_query_message("SELECT * FROM users");
        conn.on_client_data(&query, &mut actions);

        assert_eq!(actions.len(), 1);
        assert!(matches!(&actions[0], ConnAction::UpstreamSend(_)));
    }

    #[test]
    fn test_no_intercept_in_transaction() {
        let mut conn = Connection::new();
        let mut actions = Vec::new();

        conn.on_client_data(&make_startup_message("test"), &mut actions);
        conn.on_upstream_connected(&mut actions);
        // Idle first, then in transaction
        conn.on_upstream_data(&make_ready_for_query(b'I'), &mut actions);
        conn.on_upstream_data(&make_ready_for_query(b'T'), &mut actions);
        actions.clear();

        assert!(!conn.is_idle());
        let query = make_query_message("SELECT 1");
        conn.on_client_data(&query, &mut actions);

        // Should forward to upstream
        assert_eq!(actions.len(), 1);
        assert!(matches!(&actions[0], ConnAction::UpstreamSend(_)));
    }

    #[test]
    fn test_intercept_multiple_select_1() {
        let mut conn = Connection::new();
        let mut actions = Vec::new();

        conn.on_client_data(&make_startup_message("test"), &mut actions);
        conn.on_upstream_connected(&mut actions);
        conn.on_upstream_data(&make_ready_for_query(b'I'), &mut actions);
        actions.clear();

        // First SELECT 1
        conn.on_client_data(&make_query_message("SELECT 1"), &mut actions);
        assert_eq!(actions.len(), 1);
        assert!(matches!(&actions[0], ConnAction::DownstreamSend(_)));
        assert!(conn.is_idle());

        // Second SELECT 1
        actions.clear();
        conn.on_client_data(&make_query_message("SELECT 1"), &mut actions);
        assert_eq!(actions.len(), 1);
        assert!(matches!(&actions[0], ConnAction::DownstreamSend(_)));
        assert!(conn.is_idle());
    }

    #[test]
    fn test_intercept_then_real_query() {
        let mut conn = Connection::new();
        let mut actions = Vec::new();

        conn.on_client_data(&make_startup_message("test"), &mut actions);
        conn.on_upstream_connected(&mut actions);
        conn.on_upstream_data(&make_ready_for_query(b'I'), &mut actions);
        actions.clear();

        // Intercepted
        conn.on_client_data(&make_query_message("SELECT 1"), &mut actions);
        assert!(matches!(&actions[0], ConnAction::DownstreamSend(_)));
        actions.clear();

        // Real query should go to upstream
        conn.on_client_data(&make_query_message("SELECT * FROM users"), &mut actions);
        assert_eq!(actions.len(), 1);
        assert!(matches!(&actions[0], ConnAction::UpstreamSend(_)));
    }

    #[test]
    fn test_multiple_messages_in_upstream_data() {
        let mut conn = Connection::new();
        let mut actions = Vec::new();

        conn.on_client_data(&make_startup_message("test"), &mut actions);
        conn.on_upstream_connected(&mut actions);
        actions.clear();

        // Multiple backend messages in one chunk
        let mut data = Vec::new();
        data.extend_from_slice(&make_tagged_message(b'C', b"SELECT 1\0")); // CommandComplete
        data.extend_from_slice(&make_ready_for_query(b'I'));

        conn.on_upstream_data(&data, &mut actions);
        assert!(conn.is_idle());
    }
}
