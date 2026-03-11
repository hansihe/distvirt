//! Sim integration tests for distvirt-worker.
//!
//! These tests use `TestVmm` + `StubImageProvider` + channel-based gateway,
//! so they run without root, without real VMs, and without special env vars.
//!
//! Run with: `cargo test -p distvirt-worker --test sim`

#[path = "sim/common.rs"]
mod common;
#[path = "sim/pod_lifecycle.rs"]
mod pod_lifecycle;
#[path = "sim/services.rs"]
mod services;
#[path = "sim/suspend_resume.rs"]
mod suspend_resume;
#[path = "sim/crash.rs"]
mod crash;
