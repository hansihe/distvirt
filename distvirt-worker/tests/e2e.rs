//! End-to-end integration tests for distvirt-worker.
//!
//! These tests launch real Firecracker VMs and require:
//! - Root privileges
//! - `firecracker` binary (or `FIRECRACKER_BIN` env var)
//! - Running containerd (or `CONTAINERD_SOCKET` env var)
//! - Built kernel at `../guest-image/result-kernel/bzImage`
//! - Built rootfs at `../guest-image/result-rootfs`
//!
//! Gate with: `DISTVIRT_E2E=1 cargo test --package distvirt-worker --test e2e`

#[path = "e2e/common.rs"]
mod common;
#[path = "e2e/pod_lifecycle.rs"]
mod pod_lifecycle;
#[path = "e2e/services.rs"]
mod services;
#[path = "e2e/suspend_resume.rs"]
mod suspend_resume;
#[path = "e2e/tunnel.rs"]
mod tunnel;
#[path = "e2e/cross_worker_resume.rs"]
mod cross_worker_resume;
