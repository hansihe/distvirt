//! Worker protocol for communication between orchestrator and worker processes.
//!
//! This crate defines the wire protocol used between the **orchestrator** (scheduling brain)
//! and **workers** (execution muscle) in distvirt. Workers are dumb executors — they launch
//! pods, manage fabric segments, and report events. All planning, scheduling, and state
//! ownership lives in the orchestrator.
//!
//! # Transport
//!
//! The protocol is transport-agnostic. The same message types flow over:
//! - **In-process** — `tokio::io::duplex` byte pipe (local/CLI mode)
//! - **TCP/TLS** — remote workers connecting to a central orchestrator (distributed mode)
//!
//! The transport is a **yamux**-multiplexed bidirectional connection. The primary control
//! stream carries length-prefixed Cap'n Proto messages ([`WorkerCommand`] and
//! [`WorkerEvent`]). Additional yamux streams carry out-of-band data like container log
//! output.
//!
//! # Connection Roles
//!
//! The **orchestrator** is the yamux Client — it opens the control stream and sends
//! commands. Use [`OrchestratorConnection::connect`] to establish a connection.
//!
//! The **worker** is the yamux Server — it accepts the control stream and opens log
//! streams back toward the orchestrator. Use [`WorkerConnection::accept`] to accept.
//!
//! ```text
//! Worker                              Orchestrator
//!   |                                      |
//!   |──── Connect (TCP/UDS/duplex) ───────>|
//!   |──── Establish yamux session ────────>|
//!   |                                      |
//!   |   control stream (commands/events)   |
//!   |<──── WorkerCommand ─────────────────|
//!   |────── WorkerEvent ─────────────────>|
//!   |                                      |
//!   |   log streams (worker-initiated)     |
//!   |────── LogStreamHeader ─────────────>|
//!   |────── raw output bytes ────────────>|
//! ```
//!
//! # Command/Event Flow
//!
//! The orchestrator drives all state changes by sending [`WorkerCommand`]s. The worker
//! reacts to commands and reports lifecycle transitions as [`WorkerEvent`]s. The worker
//! never makes scheduling decisions — it only executes what it's told.
//!
//! ## Launching a Pod (Basic Flow)
//!
//! ```text
//! Orchestrator                           Worker
//!   |                                      |
//!   |── CreateNamespace ─────────────────>|  // set up fabric segment
//!   |<─────────────────── NamespaceCreated |
//!   |── RegistrySync ───────────────────>|  // seed DNS entries
//!   |── LaunchPod ──────────────────────>|  // start VM + containers
//!   |<──────────────────────── PodRunning |  // all containers started
//!   |        ...pod runs...                |
//!   |<───────────────────────── PodExited |  // main container exited
//! ```
//!
//! ## Endpoint-Based Communication
//!
//! Endpoints are stable network identities (virtual IP) on the fabric. They
//! decouple pod lifecycle from addressability and enable features like buffering,
//! activation, and readiness gating. The orchestrator manages endpoints via
//! [`WorkerCommand::EndpointSync`] and [`WorkerCommand::EndpointUpdate`].
//!
//! ```text
//! Orchestrator                           Worker
//!   |                                      |
//!   |── CreateNamespace ─────────────────>|
//!   |<─────────────────── NamespaceCreated |
//!   |── RegistrySync ───────────────────>|  // DNS: "api" -> service IP
//!   |── EndpointSync ────────────────────>|  // set up service endpoints
//!   |── LaunchPod ──────────────────────>|  // start the backing pod
//!   |<──────────────────────── PodRunning |
//!   |── EndpointUpdate ──────────────────>|  // assign pod as backend
//! ```
//!
//! ## Scale-to-Zero with Endpoint Activation
//!
//! When a service endpoint has no backend, traffic is buffered and the worker
//! fires a [`WorkerEvent::EndpointDemandTraffic`] so the orchestrator can schedule
//! a pod on demand.
//!
//! ```text
//! Orchestrator                           Worker
//!   |                                      |
//!   |  (endpoint exists, no backend)       |
//!   |                                      |  // client pod sends traffic
//!   |                                      |  // to service IP
//!   |<──────────── EndpointDemandTraffic     |  // "someone wants this endpoint"
//!   |                                      |
//!   |── LaunchPod ──────────────────────>|  // orchestrator reacts
//!   |<──────────────────────── PodRunning |
//!   |── EndpointUpdate ──────────────────>|  // assign backend, mark ready
//! ```
//!
//! With a **protocol activator** (e.g., TCP), the activation is smarter — only
//! meaningful traffic (TCP SYN) triggers activation, and RSTs/noise are filtered.
//! The activator also signals [`WorkerEvent::EndpointDemandActive`] so the
//! orchestrator knows when all sessions have ended and it can release the backend.
//!
//! ```text
//! Orchestrator                           Worker
//!   |                                      |
//!   |── EndpointSync(activator: Tcp) ────>|  // TCP-aware service endpoint
//!   |                                      |
//!   |                                      |  // TCP SYN arrives
//!   |<───────────── EndpointDemandTraffic    |  // pulse: traffic detected
//!   |                                      |  // (RSTs are dropped silently)
//!   |── LaunchPod ──────────────────────>|
//!   |<──────────────────────── PodRunning |
//!   |── EndpointUpdate ──────────────────>|  // assign backend
//!   |        ...traffic flows...           |
//!   |                                      |  // no new SYNs for a while
//!   |<───────────── EndpointDemandActive        |  // active=false
//!   |── EndpointUpdate(backend: None) ───>|  // release backend
//!   |── StopPod ────────────────────────>|  // scale to zero
//! ```
//!
//! ## Shutdown
//!
//! ```text
//! Orchestrator                           Worker
//!   |── StopPod (for each pod) ─────────>|  // graceful stop
//!   |<───────────────────────── PodExited |
//!   |── DestroyNamespace ───────────────>|
//!   |── Shutdown ───────────────────────>|
//!   |<──────────────────── ShuttingDown   |
//! ```
//!
//! # Log Streams
//!
//! Container output (stdout/stderr) is delivered over separate yamux streams, not
//! on the control stream. This avoids head-of-line blocking — heavy log output
//! from one pod doesn't delay command/event processing.
//!
//! The worker opens a new yamux stream, sends a [`LogStreamHeader`], then writes
//! raw output bytes. On the orchestrator side, use
//! [`OrchestratorConnection::accept_log_stream`] or
//! [`OrchestratorConnection::take_log_stream_receiver`] to consume them.

pub mod codec;
pub mod connection;
pub mod convert;
pub mod types;

pub mod worker_protocol_capnp {
    include!(concat!(env!("OUT_DIR"), "/worker_protocol_capnp.rs"));
}

pub use connection::{
    DriverHandle, LogStreamOpener, OrchestratorConnection, OrchestratorReader, OrchestratorWriter,
    WorkerConnection, WorkerReader, WorkerWriter,
};
pub use types::*;
