//! ActivatorInstance: per-service WASM instance with event queue.

use anyhow::Result;
use wasmtime::Store;
use wasmtime::component::{Component, Linker};
use wasmtime::error::Context;
use wasmtime_wasi::{ResourceTable, WasiCtx, WasiCtxBuilder, WasiCtxView, WasiView};

use crate::bindings;
use crate::types::{self, Action, BackendNeed, Event};

/// Fuel granted per `process_events` call to bound execution time.
const FUEL_PER_CALL: u64 = 1_000_000;

/// Host state stored in the wasmtime Store.
struct HostState {
    wasi: WasiCtx,
    table: ResourceTable,
}

impl WasiView for HostState {
    fn ctx(&mut self) -> WasiCtxView<'_> {
        WasiCtxView {
            ctx: &mut self.wasi,
            table: &mut self.table,
        }
    }
}

/// A per-service activator WASM instance.
///
/// Owns the wasmtime Store and generated bindings. Accumulates events
/// between `process_events` calls.
pub struct ActivatorInstance {
    store: Store<HostState>,
    bindings: bindings::Activator,
    pending_events: Vec<Event>,
    last_backend_need: BackendNeed,
}

impl ActivatorInstance {
    /// Create a new instance from an engine and pre-compiled component.
    pub fn new(engine: &wasmtime::Engine, component: &Component) -> Result<Self> {
        let wasi = WasiCtxBuilder::new().build();
        let table = ResourceTable::new();
        let mut store = Store::new(engine, HostState { wasi, table });
        store
            .set_fuel(FUEL_PER_CALL)
            .context("setting initial fuel")?;

        let mut linker = Linker::new(engine);
        wasmtime_wasi::p2::add_to_linker_sync(&mut linker).context("adding WASI to linker")?;
        let bindings = bindings::Activator::instantiate(&mut store, component, &linker)
            .context("instantiating activator component")?;

        Ok(ActivatorInstance {
            store,
            bindings,
            pending_events: Vec::new(),
            last_backend_need: BackendNeed::None,
        })
    }

    /// Queue an event for the next `process_events` call.
    pub fn push_event(&mut self, event: Event) {
        self.pending_events.push(event);
    }

    /// Call the WASM `process-events` function with all pending events.
    ///
    /// Drains the event queue, converts to WIT types, calls the component,
    /// converts returned actions back to Rust types.
    pub fn process_events(&mut self) -> Result<Vec<Action>> {
        if self.pending_events.is_empty() {
            return Ok(Vec::new());
        }

        // Reset fuel for this call (not additive — prevents accumulation).
        self.store.set_fuel(FUEL_PER_CALL)?;

        let events: Vec<Event> = self.pending_events.drain(..).collect();
        let wit_events: Vec<bindings::Event> = events.iter().map(event_to_wit).collect();

        let wit_actions = self
            .bindings
            .call_process_events(&mut self.store, &wit_events)
            .context("calling process-events")?;

        let mut actions = Vec::with_capacity(wit_actions.len());
        for wit_action in wit_actions {
            let action = action_from_wit(wit_action);
            // Track backend need changes.
            if let Action::SetBackendNeed(need) = &action {
                self.last_backend_need = *need;
            }
            actions.push(action);
        }

        Ok(actions)
    }

    /// The last backend need signaled by this activator.
    pub fn backend_need(&self) -> BackendNeed {
        self.last_backend_need
    }

    /// Whether there are pending events waiting to be processed.
    pub fn has_pending_events(&self) -> bool {
        !self.pending_events.is_empty()
    }
}

// --- Conversion helpers: Rust types <-> WIT types ---

fn event_to_wit(event: &Event) -> bindings::Event {
    match event {
        Event::BackendAvailable(available) => bindings::Event::BackendAvailable(*available),
        Event::Tick => bindings::Event::Tick,
        Event::Packet(info) => {
            let src_addr = match info.src_addr {
                std::net::IpAddr::V4(v4) => v4.octets().to_vec(),
                std::net::IpAddr::V6(v6) => v6.octets().to_vec(),
            };
            let dst_addr = match info.dst_addr {
                std::net::IpAddr::V4(v4) => v4.octets().to_vec(),
                std::net::IpAddr::V6(v6) => v6.octets().to_vec(),
            };
            bindings::Event::Packet(bindings::PacketInfo {
                flow: info.flow,
                src_addr,
                dst_addr,
                src_port: info.src_port,
                dst_port: info.dst_port,
                protocol: match info.protocol {
                    types::IpProtocol::Tcp => bindings::IpProtocol::Tcp,
                    types::IpProtocol::Udp => bindings::IpProtocol::Udp,
                    types::IpProtocol::Other => bindings::IpProtocol::Other,
                },
                tcp_flags: info.tcp_flags,
                payload: Vec::new(), // Payload not copied for efficiency; activator uses raw_frame.
                raw_frame: info.raw_frame.clone(),
            })
        }
        Event::StreamOpen(s) => bindings::Event::StreamOpen(*s),
        Event::StreamData { stream, data } => {
            bindings::Event::StreamData(bindings::StreamDataEvent {
                s: *stream,
                data: data.clone(),
            })
        }
        Event::StreamClose(s) => bindings::Event::StreamClose(*s),
        Event::UpstreamConnectResult { stream, ok } => {
            let outcome = if *ok {
                bindings::ConnectResult::Ok
            } else {
                bindings::ConnectResult::Refused
            };
            bindings::Event::UpstreamConnectResult(bindings::UpstreamConnectResultEvent {
                s: *stream,
                outcome,
            })
        }
        Event::UpstreamData { stream, data } => {
            bindings::Event::UpstreamData(bindings::StreamDataEvent {
                s: *stream,
                data: data.clone(),
            })
        }
        Event::UpstreamClose(s) => bindings::Event::UpstreamClose(*s),
    }
}

fn action_from_wit(action: bindings::Action) -> Action {
    match action {
        bindings::Action::SetBackendNeed(need) => Action::SetBackendNeed(match need {
            bindings::BackendNeed::None => BackendNeed::None,
            bindings::BackendNeed::Traffic => BackendNeed::Traffic,
            bindings::BackendNeed::Active => BackendNeed::Active,
        }),
        bindings::Action::Log(log) => Action::Log(types::LogAction {
            level: match log.level {
                bindings::LogLevel::Trace => types::LogLevel::Trace,
                bindings::LogLevel::Debug => types::LogLevel::Debug,
                bindings::LogLevel::Info => types::LogLevel::Info,
                bindings::LogLevel::Warn => types::LogLevel::Warn,
                bindings::LogLevel::Error => types::LogLevel::Error,
            },
            message: log.message,
        }),
        bindings::Action::PacketDecision((flow, decision)) => Action::PacketDecision {
            flow,
            decision: match decision {
                bindings::PacketDecision::Buffered => types::PacketDecision::Buffered,
                bindings::PacketDecision::Drop => types::PacketDecision::Drop,
            },
        },
        bindings::Action::PacketReply((flow, data)) => Action::PacketReply { flow, data },
        bindings::Action::ReplayPacket(data) => Action::ReplayPacket(data),
        bindings::Action::DownstreamSend((stream, data)) => Action::DownstreamSend { stream, data },
        bindings::Action::DownstreamClose(s) => Action::DownstreamClose(s),
        bindings::Action::PauseDownstream(s) => Action::PauseDownstream(s),
        bindings::Action::ResumeDownstream(s) => Action::ResumeDownstream(s),
        bindings::Action::UpstreamConnect(port) => Action::UpstreamConnect { port },
        bindings::Action::UpstreamSend((stream, data)) => Action::UpstreamSend { stream, data },
        bindings::Action::UpstreamClose(s) => Action::UpstreamClose(s),
        bindings::Action::PauseUpstream(s) => Action::PauseUpstream(s),
        bindings::Action::ResumeUpstream(s) => Action::ResumeUpstream(s),
    }
}
