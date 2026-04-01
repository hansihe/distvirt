use super::super::run_cmd;

/// Assign IP address to interface and bring it up.
pub fn configure_interface(
    device_name: &str,
    client_ip: &str,
    _gateway_ip: &str,
    prefix_len: u8,
) -> anyhow::Result<()> {
    run_cmd(
        "ip",
        &[
            "addr",
            "add",
            &format!("{}/{}", client_ip, prefix_len),
            "dev",
            device_name,
        ],
    )?;
    run_cmd("ip", &["link", "set", device_name, "up"])?;
    Ok(())
}

/// Add route for subnet through device.
pub fn add_route(subnet: &str, device_name: &str) -> anyhow::Result<()> {
    run_cmd("ip", &["route", "replace", subnet, "dev", device_name])
}

/// Remove route (best-effort).
pub fn remove_route(subnet: &str, device_name: &str) -> anyhow::Result<()> {
    run_cmd("ip", &["route", "del", subnet, "dev", device_name])
}

/// Configure split-DNS resolver. Not yet implemented on Linux.
pub fn configure_dns(_service_id: &str, _dns_server: &str, _domain: &str) -> anyhow::Result<()> {
    Ok(())
}

/// Remove split-DNS resolver. Not yet implemented on Linux.
pub fn remove_dns(_service_id: &str) -> anyhow::Result<()> {
    Ok(())
}
