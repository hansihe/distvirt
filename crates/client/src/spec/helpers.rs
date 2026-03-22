use std::net::Ipv4Addr;

use distvirt_client_protocol::*;

use crate::errors::SpecError;
use super::types::*;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

pub(crate) fn resolve_resources(
    workload_res: &Option<SpecResources>,
    defaults: &Option<SpecDefaults>,
) -> (Option<SpecResourceValues>, Option<SpecResourceValues>) {
    let default_res = defaults.as_ref().and_then(|d| d.resources.as_ref());

    let limits = workload_res
        .as_ref()
        .and_then(|r| r.limits.clone())
        .or_else(|| default_res.and_then(|r| r.limits.clone()));

    let requests = workload_res
        .as_ref()
        .and_then(|r| r.requests.clone())
        .or_else(|| default_res.and_then(|r| r.requests.clone()));

    (requests, limits)
}

pub(crate) fn convert_ports(ports: &Option<Vec<SpecPort>>) -> Vec<PortSpec> {
    ports
        .as_ref()
        .map(|specs| {
            specs
                .iter()
                .map(|p| {
                    let activator = p.activator.as_ref().map(|a| match a {
                        SpecPortActivator::Tcp { max_flows } => {
                            port_spec::Activator::Tcp(TcpPortActivator {
                                max_flows: max_flows.unwrap_or(0),
                            })
                        }
                        SpecPortActivator::Http2 => {
                            port_spec::Activator::Http2(Http2PortActivator {})
                        }
                    });
                    PortSpec {
                        port: p.port,
                        target_port: p.target.unwrap_or(0),
                        activator,
                    }
                })
                .collect()
        })
        .unwrap_or_default()
}

/// Convert optional buffer spec to (buffer_frames, buffer_timeout_ms).
/// Returns (0, 0) if unset — server applies defaults.
pub(crate) fn convert_buffer(buffer: &Option<SpecBuffer>) -> (u32, u32) {
    match buffer {
        Some(buf) => {
            let frames = buf.frames.unwrap_or(0);
            let timeout_ms = buf
                .timeout
                .as_ref()
                .map(|s| parse_duration_ms(s).expect("validated earlier") as u32)
                .unwrap_or(0);
            (frames, timeout_ms)
        }
        None => (0, 0),
    }
}

pub(crate) fn parse_cidr(cidr: &str) -> Result<(Ipv4Addr, u8), SpecError> {
    let (ip_str, prefix_str) = cidr
        .split_once('/')
        .ok_or_else(|| SpecError::Validation {
            message: format!("invalid CIDR: {}", cidr),
        })?;
    let ip: Ipv4Addr = ip_str
        .parse()
        .map_err(|e| SpecError::Validation {
            message: format!("invalid IP in CIDR: {e}"),
        })?;
    let prefix: u8 = prefix_str
        .parse()
        .map_err(|e| SpecError::Validation {
            message: format!("invalid prefix in CIDR: {e}"),
        })?;
    Ok((ip, prefix))
}

pub(crate) fn ip_to_u32(ip: Ipv4Addr) -> u32 {
    u32::from(ip)
}

pub(crate) fn u32_to_ip(val: u32) -> Ipv4Addr {
    Ipv4Addr::from(val)
}

pub(crate) fn ip_to_mac(ip: &str) -> String {
    // Generate a deterministic MAC from IP: 02:00:xx:xx:xx:xx
    let addr: Ipv4Addr = ip.parse().unwrap_or(Ipv4Addr::new(0, 0, 0, 0));
    let octets = addr.octets();
    format!(
        "02:00:{:02X}:{:02X}:{:02X}:{:02X}",
        octets[0], octets[1], octets[2], octets[3]
    )
}

/// Parse a human-readable duration string into milliseconds.
/// Supports: "30s", "5m", "1h", "500ms"
pub(crate) fn parse_duration_ms(s: &str) -> Result<u64, SpecError> {
    let s = s.trim();
    let parse_val = |val: &str| -> Result<u64, SpecError> {
        val.trim()
            .parse::<u64>()
            .map_err(|e| SpecError::Validation {
                message: format!("invalid duration value: {e}"),
            })
    };
    if let Some(val) = s.strip_suffix("ms") {
        return Ok(parse_val(val)?);
    }
    if let Some(val) = s.strip_suffix('s') {
        return Ok(parse_val(val)? * 1_000);
    }
    if let Some(val) = s.strip_suffix('m') {
        return Ok(parse_val(val)? * 60_000);
    }
    if let Some(val) = s.strip_suffix('h') {
        return Ok(parse_val(val)? * 3_600_000);
    }
    Err(SpecError::Validation {
        message: format!("unsupported duration format: '{}' (use ms/s/m/h suffix)", s),
    })
}
