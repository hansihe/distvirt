use std::collections::{HashMap, VecDeque};

use activator_types::*;

use crate::connection::{ConnAction, Connection};

pub struct PostgresActivator {
    /// Per-downstream-connection state.
    connections: HashMap<u64, Connection>,
    /// Maps upstream stream handle -> downstream stream handle.
    upstream_to_downstream: HashMap<u64, u64>,
    /// FIFO queue: downstream handles waiting for upstream-connect-result.
    pending_connects: VecDeque<u64>,
    /// Whether the backend is currently available.
    backend_available: bool,
}

impl PostgresActivator {
    pub fn new() -> Self {
        Self {
            connections: HashMap::new(),
            upstream_to_downstream: HashMap::new(),
            pending_connects: VecDeque::new(),
            backend_available: false,
        }
    }

    /// Recalculate and emit backend need if it changed.
    fn update_backend_need(&self, old_need: BackendNeed, actions: &mut Vec<Action>) {
        let new_need = self.compute_backend_need();
        if old_need != new_need {
            actions.push(Action::SetBackendNeed(new_need));
        }
    }

    fn compute_backend_need(&self) -> BackendNeed {
        if self.connections.is_empty() {
            return BackendNeed::None;
        }

        // Connections that haven't sent a startup message yet don't need a backend.
        let meaningful_connections: Vec<_> = self
            .connections
            .values()
            .filter(|c| {
                !matches!(
                    c.phase(),
                    crate::connection::Phase::AwaitingStartup
                        | crate::connection::Phase::SslNegotiation
                )
            })
            .collect();

        if meaningful_connections.is_empty() {
            return BackendNeed::None;
        }

        // If all meaningful connections are idle, no backend needed.
        if meaningful_connections.iter().all(|c| c.is_idle()) {
            return BackendNeed::None;
        }

        // At least one connection needs the backend (startup buffered, in auth, in transaction, etc.)
        BackendNeed::Active
    }
}

impl Activator for PostgresActivator {
    fn process_events(&mut self, events: Vec<Event>) -> Vec<Action> {
        let mut actions = Vec::new();

        for event in events {
            match event {
                Event::StreamOpen(handle) => {
                    self.handle_stream_open(handle, &mut actions);
                }
                Event::StreamData { stream, data } => {
                    self.handle_stream_data(stream, &data, &mut actions);
                }
                Event::StreamClose(handle) => {
                    self.handle_stream_close(handle, &mut actions);
                }
                Event::BackendAvailable(available) => {
                    self.handle_backend_available(available, &mut actions);
                }
                Event::UpstreamConnectResult { stream, ok } => {
                    self.handle_upstream_connect_result(stream, ok, &mut actions);
                }
                Event::UpstreamData { stream, data } => {
                    self.handle_upstream_data(stream, &data, &mut actions);
                }
                Event::UpstreamClose(handle) => {
                    self.handle_upstream_close(handle, &mut actions);
                }
                Event::Tick => {}
                Event::Packet(_) => {}
            }
        }

        actions
    }
}

impl PostgresActivator {
    fn handle_stream_open(&mut self, handle: u64, _actions: &mut Vec<Action>) {
        self.connections.insert(handle, Connection::new());
    }

    fn handle_stream_data(&mut self, handle: u64, data: &[u8], actions: &mut Vec<Action>) {
        let old_need = self.compute_backend_need();

        let Some(conn) = self.connections.get_mut(&handle) else {
            return;
        };

        let mut conn_actions = Vec::new();
        conn.on_client_data(data, &mut conn_actions);
        self.process_conn_actions(handle, conn_actions, actions);

        // If backend is available and connection has buffered startup, connect
        if self.backend_available {
            if let Some(conn) = self.connections.get(&handle) {
                if conn.has_buffered_startup() {
                    self.pending_connects.push_back(handle);
                    actions.push(Action::UpstreamConnect { port: 5432 });
                }
            }
        }

        self.update_backend_need(old_need, actions);
    }

    fn handle_stream_close(&mut self, handle: u64, actions: &mut Vec<Action>) {
        let old_need = self.compute_backend_need();

        if let Some(_conn) = self.connections.remove(&handle) {
            // Close associated upstream if any
            let upstream_handle: Option<u64> = self
                .upstream_to_downstream
                .iter()
                .find(|&(_, &v)| v == handle)
                .map(|(&k, _)| k);

            if let Some(uh) = upstream_handle {
                self.upstream_to_downstream.remove(&uh);
                actions.push(Action::UpstreamClose(uh));
            }

            // Remove from pending connects
            self.pending_connects.retain(|&h| h != handle);
        }

        self.update_backend_need(old_need, actions);
    }

    fn handle_backend_available(&mut self, available: bool, actions: &mut Vec<Action>) {
        let old_need = self.compute_backend_need();
        self.backend_available = available;

        if available {
            // For each connection with buffered startup, initiate connect
            let handles: Vec<u64> = self
                .connections
                .iter()
                .filter(|(_, conn)| conn.has_buffered_startup())
                .map(|(&h, _)| h)
                .collect();

            for handle in handles {
                self.pending_connects.push_back(handle);
                actions.push(Action::UpstreamConnect { port: 5432 });
            }
        } else {
            // Backend went away — close all connections
            let handles: Vec<u64> = self.connections.keys().copied().collect();
            for handle in handles {
                if let Some(conn) = self.connections.get_mut(&handle) {
                    let mut conn_actions = Vec::new();
                    conn.on_upstream_closed(&mut conn_actions);
                    self.process_conn_actions(handle, conn_actions, actions);
                }
            }
            // Close all upstreams
            let upstream_handles: Vec<u64> =
                self.upstream_to_downstream.keys().copied().collect();
            for uh in upstream_handles {
                self.upstream_to_downstream.remove(&uh);
                actions.push(Action::UpstreamClose(uh));
            }
        }

        self.update_backend_need(old_need, actions);
    }

    fn handle_upstream_connect_result(
        &mut self,
        upstream_handle: u64,
        ok: bool,
        actions: &mut Vec<Action>,
    ) {
        let old_need = self.compute_backend_need();

        let Some(downstream_handle) = self.pending_connects.pop_front() else {
            actions.push(Action::UpstreamClose(upstream_handle));
            return;
        };

        if ok {
            self.upstream_to_downstream
                .insert(upstream_handle, downstream_handle);

            if let Some(conn) = self.connections.get_mut(&downstream_handle) {
                let mut conn_actions = Vec::new();
                conn.on_upstream_connected(&mut conn_actions);
                self.process_conn_actions(downstream_handle, conn_actions, actions);
            }
        } else {
            if let Some(conn) = self.connections.get_mut(&downstream_handle) {
                let mut conn_actions = Vec::new();
                conn.on_upstream_failed(&mut conn_actions);
                self.process_conn_actions(downstream_handle, conn_actions, actions);
            }
        }

        self.update_backend_need(old_need, actions);
    }

    fn handle_upstream_data(
        &mut self,
        upstream_handle: u64,
        data: &[u8],
        actions: &mut Vec<Action>,
    ) {
        let old_need = self.compute_backend_need();

        let Some(&downstream_handle) = self.upstream_to_downstream.get(&upstream_handle) else {
            return;
        };

        let Some(conn) = self.connections.get_mut(&downstream_handle) else {
            return;
        };

        let mut conn_actions = Vec::new();
        conn.on_upstream_data(data, &mut conn_actions);
        self.process_conn_actions(downstream_handle, conn_actions, actions);

        self.update_backend_need(old_need, actions);
    }

    fn handle_upstream_close(&mut self, upstream_handle: u64, actions: &mut Vec<Action>) {
        let old_need = self.compute_backend_need();

        let Some(downstream_handle) = self.upstream_to_downstream.remove(&upstream_handle) else {
            return;
        };

        let Some(conn) = self.connections.get_mut(&downstream_handle) else {
            return;
        };

        let mut conn_actions = Vec::new();
        conn.on_upstream_closed(&mut conn_actions);
        self.process_conn_actions(downstream_handle, conn_actions, actions);

        self.update_backend_need(old_need, actions);
    }

    /// Translate ConnActions into shared Actions.
    fn process_conn_actions(
        &mut self,
        downstream_handle: u64,
        conn_actions: Vec<ConnAction>,
        actions: &mut Vec<Action>,
    ) {
        for ca in conn_actions {
            match ca {
                ConnAction::DownstreamSend(data) => {
                    actions.push(Action::DownstreamSend {
                        stream: downstream_handle,
                        data,
                    });
                }
                ConnAction::UpstreamSend(data) => {
                    if let Some((&uh, _)) = self
                        .upstream_to_downstream
                        .iter()
                        .find(|&(_, &v)| v == downstream_handle)
                    {
                        actions.push(Action::UpstreamSend {
                            stream: uh,
                            data,
                        });
                    }
                }
                ConnAction::DownstreamClose => {
                    actions.push(Action::DownstreamClose(downstream_handle));
                }
                ConnAction::UpstreamClose => {
                    if let Some((&uh, _)) = self
                        .upstream_to_downstream
                        .iter()
                        .find(|&(_, &v)| v == downstream_handle)
                    {
                        actions.push(Action::UpstreamClose(uh));
                    }
                }
                ConnAction::Log(msg) => {
                    actions.push(Action::Log(LogAction {
                        level: LogLevel::Info,
                        message: msg,
                    }));
                }
            }
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
        buf.extend_from_slice(&crate::protocol::PROTOCOL_VERSION_3_0.to_be_bytes());
        buf.extend_from_slice(params.as_bytes());
        buf
    }

    fn make_ready_for_query(status: u8) -> Vec<u8> {
        vec![b'Z', 0, 0, 0, 5, status]
    }

    fn make_auth_ok() -> Vec<u8> {
        // Authentication OK: tag='R', length=8, auth_type=0
        vec![b'R', 0, 0, 0, 8, 0, 0, 0, 0]
    }

    fn has_action<F>(actions: &[Action], pred: F) -> bool
    where
        F: Fn(&Action) -> bool,
    {
        actions.iter().any(pred)
    }

    fn get_backend_need(actions: &[Action]) -> Option<BackendNeed> {
        actions
            .iter()
            .filter_map(|a| match a {
                Action::SetBackendNeed(n) => Some(*n),
                _ => None,
            })
            .last()
    }

    #[test]
    fn test_stream_open_and_startup_triggers_backend_need() {
        let mut act = PostgresActivator::new();

        // Open stream
        let actions = act.process_events(vec![Event::StreamOpen(1)]);
        assert!(actions.is_empty()); // No need yet — no startup message

        // Send startup message → should trigger Active backend need
        let actions = act.process_events(vec![Event::StreamData {
            stream: 1,
            data: make_startup_message("test"),
        }]);
        assert_eq!(get_backend_need(&actions), Some(BackendNeed::Active));
    }

    #[test]
    fn test_backend_available_triggers_upstream_connect() {
        let mut act = PostgresActivator::new();

        act.process_events(vec![Event::StreamOpen(1)]);
        act.process_events(vec![Event::StreamData {
            stream: 1,
            data: make_startup_message("test"),
        }]);

        // Backend becomes available
        let actions = act.process_events(vec![Event::BackendAvailable(true)]);
        assert!(has_action(&actions, |a| matches!(
            a,
            Action::UpstreamConnect { port: 5432 }
        )));
    }

    #[test]
    fn test_upstream_connect_flushes_startup() {
        let mut act = PostgresActivator::new();
        let startup = make_startup_message("test");

        act.process_events(vec![Event::StreamOpen(1)]);
        act.process_events(vec![Event::StreamData {
            stream: 1,
            data: startup.clone(),
        }]);
        act.process_events(vec![Event::BackendAvailable(true)]);

        // Upstream connect result
        let actions = act.process_events(vec![Event::UpstreamConnectResult {
            stream: 100,
            ok: true,
        }]);

        // Should have flushed the startup message to upstream
        assert!(has_action(&actions, |a| matches!(
            a,
            Action::UpstreamSend { stream: 100, .. }
        )));
    }

    #[test]
    fn test_idle_connection_signals_no_backend_need() {
        let mut act = PostgresActivator::new();
        let startup = make_startup_message("test");

        act.process_events(vec![Event::StreamOpen(1)]);
        act.process_events(vec![Event::StreamData {
            stream: 1,
            data: startup,
        }]);
        act.process_events(vec![Event::BackendAvailable(true)]);
        act.process_events(vec![Event::UpstreamConnectResult {
            stream: 100,
            ok: true,
        }]);

        // Send auth OK + ReadyForQuery(Idle)
        let mut backend_data = make_auth_ok();
        backend_data.extend_from_slice(&make_ready_for_query(b'I'));

        let actions = act.process_events(vec![Event::UpstreamData {
            stream: 100,
            data: backend_data,
        }]);

        assert_eq!(get_backend_need(&actions), Some(BackendNeed::None));
    }

    #[test]
    fn test_stream_close_cleans_up() {
        let mut act = PostgresActivator::new();

        act.process_events(vec![Event::StreamOpen(1)]);
        act.process_events(vec![Event::StreamData {
            stream: 1,
            data: make_startup_message("test"),
        }]);
        act.process_events(vec![Event::BackendAvailable(true)]);
        act.process_events(vec![Event::UpstreamConnectResult {
            stream: 100,
            ok: true,
        }]);

        // Close the stream
        let actions = act.process_events(vec![Event::StreamClose(1)]);

        // Should close upstream
        assert!(has_action(&actions, |a| matches!(
            a,
            Action::UpstreamClose(100)
        )));
        // Should signal no backend need
        assert_eq!(get_backend_need(&actions), Some(BackendNeed::None));
    }

    #[test]
    fn test_multiple_connections_backend_need() {
        let mut act = PostgresActivator::new();

        // Two connections
        act.process_events(vec![Event::StreamOpen(1), Event::StreamOpen(2)]);

        act.process_events(vec![Event::StreamData {
            stream: 1,
            data: make_startup_message("user1"),
        }]);
        act.process_events(vec![Event::StreamData {
            stream: 2,
            data: make_startup_message("user2"),
        }]);

        act.process_events(vec![Event::BackendAvailable(true)]);
        act.process_events(vec![Event::UpstreamConnectResult {
            stream: 100,
            ok: true,
        }]);
        act.process_events(vec![Event::UpstreamConnectResult {
            stream: 101,
            ok: true,
        }]);

        // First connection goes idle
        let mut data1 = make_auth_ok();
        data1.extend_from_slice(&make_ready_for_query(b'I'));
        let actions = act.process_events(vec![Event::UpstreamData {
            stream: 100,
            data: data1,
        }]);
        // Still not all idle — conn 2 is still in auth
        assert_ne!(get_backend_need(&actions), Some(BackendNeed::None));

        // Second connection goes idle
        let mut data2 = make_auth_ok();
        data2.extend_from_slice(&make_ready_for_query(b'I'));
        let actions = act.process_events(vec![Event::UpstreamData {
            stream: 101,
            data: data2,
        }]);
        assert_eq!(get_backend_need(&actions), Some(BackendNeed::None));
    }

    #[test]
    fn test_upstream_connect_failure() {
        let mut act = PostgresActivator::new();

        act.process_events(vec![Event::StreamOpen(1)]);
        act.process_events(vec![Event::StreamData {
            stream: 1,
            data: make_startup_message("test"),
        }]);
        act.process_events(vec![Event::BackendAvailable(true)]);

        let actions = act.process_events(vec![Event::UpstreamConnectResult {
            stream: 100,
            ok: false,
        }]);

        // Should close downstream
        assert!(has_action(&actions, |a| matches!(
            a,
            Action::DownstreamClose(1)
        )));
    }

    #[test]
    fn test_backend_unavailable_closes_connections() {
        let mut act = PostgresActivator::new();

        act.process_events(vec![Event::StreamOpen(1)]);
        act.process_events(vec![Event::StreamData {
            stream: 1,
            data: make_startup_message("test"),
        }]);
        act.process_events(vec![Event::BackendAvailable(true)]);
        act.process_events(vec![Event::UpstreamConnectResult {
            stream: 100,
            ok: true,
        }]);

        // Backend goes away
        let actions = act.process_events(vec![Event::BackendAvailable(false)]);

        assert!(has_action(&actions, |a| matches!(
            a,
            Action::DownstreamClose(1)
        )));
        assert!(has_action(&actions, |a| matches!(
            a,
            Action::UpstreamClose(100)
        )));
    }

    #[test]
    fn test_backend_available_before_startup() {
        let mut act = PostgresActivator::new();

        // Backend available before any connections
        let actions = act.process_events(vec![Event::BackendAvailable(true)]);
        assert!(actions.is_empty());

        // Now open connection and send startup — should immediately connect
        act.process_events(vec![Event::StreamOpen(1)]);
        let actions = act.process_events(vec![Event::StreamData {
            stream: 1,
            data: make_startup_message("test"),
        }]);

        assert!(has_action(&actions, |a| matches!(
            a,
            Action::UpstreamConnect { port: 5432 }
        )));
    }
}
