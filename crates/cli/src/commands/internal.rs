use std::path::PathBuf;

use anyhow::Context;

use distvirt_client::connect::fd_pass;
use distvirt_client::connect::platform::{TunDevice, add_route, configure_interface};

/// `dv internal setup-tun` — privileged helper that creates a TUN device,
/// configures networking, and passes the fd back to the unprivileged parent.
pub fn setup_tun(
    socket_path: PathBuf,
    nonce: String,
    client_ip: String,
    prefix_len: u8,
    subnet: String,
) -> anyhow::Result<()> {
    let send_error = |msg: &str| -> anyhow::Result<()> {
        if let Ok(conn) = fd_pass::connect(&socket_path) {
            let payload = format!("ERR:{}:{}", nonce, msg);
            let _ = fd_pass::send_msg(&conn, None, payload.as_bytes());
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
    if let Err(e) = configure_interface(&tun.name, &client_ip, prefix_len) {
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

    // Connect to the parent's Unix socket and send the fd.
    let conn = fd_pass::connect(&socket_path)
        .context("connect to parent socket")?;

    let payload = format!("OK:{}:{}", nonce, tun.name);
    fd_pass::send_msg(&conn, Some(tun.into_raw_fd()), payload.as_bytes())
        .context("send TUN fd to parent")?;

    Ok(())
}
