use std::path::PathBuf;

use anyhow::{Context, bail};

use super::fd_pass;
use super::helper_protocol::{
    self, HelperToParent, ParentToHelper,
};
use distvirt_client::connect::platform::{
    TunDevice, add_route, configure_dns, configure_interface, remove_dns, remove_route,
};

/// `dv internal setup-tun` — privileged helper that creates a TUN device,
/// configures networking, passes the fd back to the unprivileged parent,
/// then stays alive until it receives a `Teardown` message (or the socket
/// closes) so it can perform privileged cleanup (DNS/routes).
pub fn setup_tun(
    socket_path: PathBuf,
    nonce: String,
    client_ip: String,
    prefix_len: u8,
    subnet: String,
    dns_domain: String,
    gateway_ip: String,
) -> anyhow::Result<()> {
    let send_error = |msg: &str| -> anyhow::Result<()> {
        if let Ok(conn) = fd_pass::connect(&socket_path) {
            let err_msg = HelperToParent::Error {
                nonce: nonce.clone(),
                message: msg.to_string(),
            };
            let _ = helper_protocol::send_helper_msg(&conn, None, &err_msg);
        }
        Ok(())
    };

    // Create TUN device.
    let tun = match TunDevice::create() {
        Ok(t) => t,
        Err(e) => {
            let msg = format!("failed to create TUN device: {}", e);
            send_error(&msg)?;
            anyhow::bail!("{}", msg);
        }
    };

    // Configure interface.
    if let Err(e) = configure_interface(&tun.name, &client_ip, &gateway_ip, prefix_len) {
        let msg = format!("failed to configure interface: {}", e);
        send_error(&msg)?;
        anyhow::bail!("{}", msg);
    }

    // Add route.
    if let Err(e) = add_route(&subnet, &tun.name) {
        let msg = format!("failed to add route: {}", e);
        send_error(&msg)?;
        anyhow::bail!("{}", msg);
    }

    // Configure DNS.
    let has_dns = !dns_domain.is_empty();
    if has_dns {
        if let Err(e) = configure_dns(&tun.name, &gateway_ip, &dns_domain) {
            let msg = format!("failed to configure DNS: {}", e);
            send_error(&msg)?;
            anyhow::bail!("{}", msg);
        }
    }

    // Save the device name before into_raw_fd() consumes the TunDevice.
    let device_name = tun.name.clone();

    // Generate a helper nonce for bidirectional validation.
    let helper_nonce: String = {
        let val: u128 = rand::random();
        format!("{:032x}", val)
    };

    // Connect to the parent's Unix socket and send the fd with the setup result.
    let conn = fd_pass::connect(&socket_path).context("connect to parent socket")?;

    let msg = HelperToParent::SetupResult {
        nonce,
        device_name: device_name.clone(),
        helper_nonce: helper_nonce.clone(),
    };
    helper_protocol::send_helper_msg(&conn, Some(tun.into_raw_fd()), &msg)
        .context("send TUN fd to parent")?;

    log::info!("helper: TUN device {} sent to parent, waiting for ack", device_name);

    // Wait for the parent to acknowledge our nonce before entering teardown-wait mode.
    match helper_protocol::recv_parent_msg(&conn) {
        Ok(Some(ParentToHelper::Ack { helper_nonce: ack_nonce })) => {
            if ack_nonce != helper_nonce {
                bail!("helper: helper_nonce mismatch in Ack (expected {}, got {})", helper_nonce, ack_nonce);
            }
            log::info!("helper: received valid Ack, entering teardown-wait mode");
        }
        Ok(Some(ParentToHelper::Teardown)) => {
            // Parent sent teardown immediately (shouldn't happen, but handle gracefully).
            log::info!("helper: received Teardown before Ack, cleaning up");
            do_teardown(has_dns, &device_name, &subnet);
            return Ok(());
        }
        Ok(None) => {
            log::info!("helper: parent socket closed before Ack, cleaning up");
            do_teardown(has_dns, &device_name, &subnet);
            return Ok(());
        }
        Err(e) => {
            log::warn!("helper: error waiting for Ack: {:#}, cleaning up", e);
            do_teardown(has_dns, &device_name, &subnet);
            return Ok(());
        }
    }

    // Stay alive — wait for a Teardown message or socket close.
    // The helper intentionally keeps its copy of the TUN fd open as a safety
    // net: if the parent crashes, the device stays alive until the helper
    // performs cleanup, preventing the kernel from tearing down the interface
    // (and its routes/DNS) before we get a chance to clean up properly.
    match helper_protocol::recv_parent_msg(&conn) {
        Ok(Some(ParentToHelper::Teardown)) => {
            log::info!("helper: received Teardown, cleaning up");
        }
        Ok(Some(ParentToHelper::Ack { .. })) => {
            log::warn!("helper: received unexpected second Ack, cleaning up");
        }
        Ok(None) => {
            log::info!("helper: parent socket closed, cleaning up");
        }
        Err(e) => {
            log::warn!("helper: error waiting for parent message: {:#}, cleaning up", e);
        }
    }

    do_teardown(has_dns, &device_name, &subnet);

    log::info!("helper: cleanup complete, exiting");
    Ok(())
}

/// Perform privileged teardown (remove DNS and routes).
fn do_teardown(has_dns: bool, device_name: &str, subnet: &str) {
    if has_dns {
        if let Err(e) = remove_dns(device_name) {
            log::warn!("helper: failed to remove DNS: {:#}", e);
        }
    }
    if let Err(e) = remove_route(subnet, device_name) {
        log::warn!("helper: failed to remove route: {:#}", e);
    }
}
