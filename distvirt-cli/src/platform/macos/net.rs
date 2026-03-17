use super::super::run_cmd;

/// Assign IP address to interface and bring it up.
pub fn configure_interface(
    device_name: &str,
    client_ip: &str,
    prefix_len: u8,
) -> anyhow::Result<()> {
    let netmask = prefix_len_to_netmask_str(prefix_len);
    run_cmd(
        "ifconfig",
        &[device_name, client_ip, client_ip, "netmask", &netmask, "up"],
    )
}

/// Convert a prefix length (0–32) to a dotted-decimal netmask string.
fn prefix_len_to_netmask_str(prefix_len: u8) -> String {
    let mask: u32 = if prefix_len == 0 {
        0
    } else {
        !0u32 << (32 - prefix_len)
    };
    let bytes = mask.to_be_bytes();
    format!("{}.{}.{}.{}", bytes[0], bytes[1], bytes[2], bytes[3])
}

/// Add route for subnet through device.
pub fn add_route(subnet: &str, device_name: &str) -> anyhow::Result<()> {
    run_cmd("route", &["add", "-net", subnet, "-interface", device_name])
}

/// Remove route (best-effort).
pub fn remove_route(subnet: &str, device_name: &str) -> anyhow::Result<()> {
    run_cmd(
        "route",
        &["delete", "-net", subnet, "-interface", device_name],
    )
}
