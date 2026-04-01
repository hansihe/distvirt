use super::super::{run_cmd, run_cmd_stdin};

/// Assign IP address to interface and bring it up.
pub fn configure_interface(
    device_name: &str,
    client_ip: &str,
    gateway_ip: &str,
    prefix_len: u8,
) -> anyhow::Result<()> {
    let netmask = prefix_len_to_netmask_str(prefix_len);
    run_cmd(
        "ifconfig",
        &[device_name, "inet", client_ip, gateway_ip, "netmask", &netmask, "up"],
    )
}

/// Convert a prefix length (0-32) to a dotted-decimal netmask string.
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

/// Register a split-DNS resolver via scutil so that queries for `domain`
/// are sent to `dns_server`. `service_id` should be a stable unique key
/// (e.g. the utun device name).
pub fn configure_dns(service_id: &str, dns_server: &str, domain: &str) -> anyhow::Result<()> {
    let commands = format!(
        "d.init\n\
         d.add ServerAddresses * {dns_server}\n\
         d.add SupplementalMatchDomains * {domain}\n\
         set State:/Network/Service/{service_id}/DNS\n"
    );
    run_cmd_stdin("scutil", &[], &commands)
}

/// Remove the split-DNS resolver previously registered via `configure_dns`.
pub fn remove_dns(service_id: &str) -> anyhow::Result<()> {
    let commands = format!("remove State:/Network/Service/{service_id}/DNS\n");
    run_cmd_stdin("scutil", &[], &commands)
}
