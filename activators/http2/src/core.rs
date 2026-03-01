use std::collections::{HashMap, VecDeque};

use activator_types::*;

use crate::connection::{ConnAction, Connection, UpstreamState};

pub struct Http2Activator {
    /// Per-downstream-connection state.
    connections: HashMap<u64, Connection>,
    /// Maps upstream stream handle -> downstream stream handle.
    upstream_to_downstream: HashMap<u64, u64>,
    /// FIFO queue: downstream handles waiting for upstream-connect-result.
    pending_connects: VecDeque<u64>,
    /// Whether the backend is currently available.
    backend_available: bool,
    /// Total number of active H2 streams across all connections.
    total_h2_streams: usize,
}

impl Http2Activator {
    pub fn new() -> Self {
        Self {
            connections: HashMap::new(),
            upstream_to_downstream: HashMap::new(),
            pending_connects: VecDeque::new(),
            backend_available: false,
            total_h2_streams: 0,
        }
    }
}

impl Activator for Http2Activator {
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

impl Http2Activator {
    fn handle_stream_open(&mut self, handle: u64, _actions: &mut Vec<Action>) {
        self.connections.insert(handle, Connection::new());
    }

    fn handle_stream_data(&mut self, handle: u64, data: &[u8], actions: &mut Vec<Action>) {
        let Some(conn) = self.connections.get_mut(&handle) else {
            return;
        };

        let old_streams = self.total_h2_streams;
        let mut conn_actions = Vec::new();
        conn.on_client_data(data, &mut conn_actions);

        self.process_conn_actions(handle, conn_actions, actions);

        // If backend is available and connection has buffered frames and no upstream yet, connect
        if self.backend_available {
            if let Some(conn) = self.connections.get_mut(&handle) {
                if conn.upstream_state() == UpstreamState::None && conn.has_buffered_frames() {
                    conn.set_upstream_state(UpstreamState::Connecting);
                    self.pending_connects.push_back(handle);
                    actions.push(Action::UpstreamConnect { port: 80 });
                }
            }
        }

        check_backend_need_transition(old_streams, self.total_h2_streams, actions);
    }

    fn handle_stream_close(&mut self, handle: u64, actions: &mut Vec<Action>) {
        let old_streams = self.total_h2_streams;

        if let Some(conn) = self.connections.remove(&handle) {
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

            // Count remaining streams as closed
            let remaining = conn.stream_count();
            self.total_h2_streams = self.total_h2_streams.saturating_sub(remaining);
        }

        check_backend_need_transition(old_streams, self.total_h2_streams, actions);
    }

    fn handle_backend_available(&mut self, available: bool, actions: &mut Vec<Action>) {
        self.backend_available = available;

        if available {
            // For each connection that has buffered frames and no upstream, initiate connect
            let handles: Vec<u64> = self
                .connections
                .iter()
                .filter(|(_, conn)| {
                    conn.upstream_state() == UpstreamState::None && conn.has_buffered_frames()
                })
                .map(|(&h, _)| h)
                .collect();

            for handle in handles {
                if let Some(conn) = self.connections.get_mut(&handle) {
                    conn.set_upstream_state(UpstreamState::Connecting);
                    self.pending_connects.push_back(handle);
                    actions.push(Action::UpstreamConnect { port: 80 });
                }
            }
        } else {
            // Backend went away — send GOAWAY to all clients
            let old_streams = self.total_h2_streams;
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
            check_backend_need_transition(old_streams, self.total_h2_streams, actions);
        }
    }

    fn handle_upstream_connect_result(
        &mut self,
        upstream_handle: u64,
        ok: bool,
        actions: &mut Vec<Action>,
    ) {
        let Some(downstream_handle) = self.pending_connects.pop_front() else {
            // No pending connect — close unexpected upstream
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
            let old_streams = self.total_h2_streams;
            if let Some(conn) = self.connections.get_mut(&downstream_handle) {
                let mut conn_actions = Vec::new();
                conn.on_upstream_failed(&mut conn_actions);
                self.process_conn_actions(downstream_handle, conn_actions, actions);
            }
            check_backend_need_transition(old_streams, self.total_h2_streams, actions);
        }
    }

    fn handle_upstream_data(
        &mut self,
        upstream_handle: u64,
        data: &[u8],
        actions: &mut Vec<Action>,
    ) {
        let Some(&downstream_handle) = self.upstream_to_downstream.get(&upstream_handle) else {
            return;
        };

        let Some(conn) = self.connections.get_mut(&downstream_handle) else {
            return;
        };

        let old_streams = self.total_h2_streams;
        let mut conn_actions = Vec::new();
        conn.on_upstream_data(data, &mut conn_actions);
        self.process_conn_actions(downstream_handle, conn_actions, actions);
        check_backend_need_transition(old_streams, self.total_h2_streams, actions);
    }

    fn handle_upstream_close(&mut self, upstream_handle: u64, actions: &mut Vec<Action>) {
        let Some(downstream_handle) = self.upstream_to_downstream.remove(&upstream_handle) else {
            return;
        };

        let Some(conn) = self.connections.get_mut(&downstream_handle) else {
            return;
        };

        let old_streams = self.total_h2_streams;
        let mut conn_actions = Vec::new();
        conn.on_upstream_closed(&mut conn_actions);
        self.process_conn_actions(downstream_handle, conn_actions, actions);
        check_backend_need_transition(old_streams, self.total_h2_streams, actions);
    }

    /// Translate ConnActions into shared Actions, updating global stream count.
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
                    // Find the upstream handle for this downstream
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
                ConnAction::StreamOpened => {
                    self.total_h2_streams += 1;
                }
                ConnAction::StreamClosed => {
                    self.total_h2_streams = self.total_h2_streams.saturating_sub(1);
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

/// Emit SetBackendNeed when total_h2_streams crosses 0<->N boundary.
fn check_backend_need_transition(old: usize, new: usize, actions: &mut Vec<Action>) {
    if old == 0 && new > 0 {
        actions.push(Action::SetBackendNeed(BackendNeed::Active));
    } else if old > 0 && new == 0 {
        actions.push(Action::SetBackendNeed(BackendNeed::None));
    }
}
