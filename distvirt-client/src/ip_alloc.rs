use std::collections::HashSet;
use std::net::Ipv4Addr;

use anyhow::bail;

use crate::helpers::{ip_to_u32, parse_cidr, u32_to_ip};

// ---------------------------------------------------------------------------
// IpAllocator — stable, hash-based IP assignment
// ---------------------------------------------------------------------------

/// Allocates IPs from a subnet using deterministic name-based hashing.
///
/// Explicit IPs are reserved first, then auto-assigned names get a slot via
/// `hash(name) % available_slots` with linear probing on collision.
pub struct IpAllocator {
    base: u32,
    pub(crate) num_hosts: u32,
    occupied: HashSet<u32>,
}

impl IpAllocator {
    /// Create an allocator for the given CIDR subnet.
    /// Reserves .0 (network) and .1 (gateway) automatically.
    pub fn new(cidr: &str) -> anyhow::Result<Self> {
        let (base_ip, prefix) = parse_cidr(cidr)?;
        let base = ip_to_u32(base_ip);
        let host_bits = 32 - prefix as u32;
        let total_addrs = 1u32
            .checked_shl(host_bits)
            .ok_or_else(|| anyhow::anyhow!("invalid prefix length: {}", prefix))?;
        let num_hosts = total_addrs.saturating_sub(2);
        if num_hosts == 0 {
            bail!("subnet {} has no usable host addresses", cidr);
        }
        Ok(Self {
            base,
            num_hosts,
            occupied: HashSet::new(),
        })
    }

    /// Reserve an explicit IP address.
    pub fn reserve(&mut self, ip: Ipv4Addr) -> anyhow::Result<()> {
        let ip_u32 = ip_to_u32(ip);
        let first_host = self.base + 2;
        if ip_u32 < first_host || ip_u32 >= first_host + self.num_hosts {
            bail!("IP {} is outside the allocatable range of the subnet", ip);
        }
        let offset = ip_u32 - first_host;
        if !self.occupied.insert(offset) {
            bail!("IP {} is already allocated", ip);
        }
        Ok(())
    }

    /// Auto-assign an IP for the given name using deterministic hashing.
    pub fn assign(&mut self, name: &str) -> anyhow::Result<Ipv4Addr> {
        if self.occupied.len() as u32 >= self.num_hosts {
            bail!("no more IPs available in subnet");
        }
        let hash = fnv1a_hash(name);
        let start = hash % self.num_hosts;
        for i in 0..self.num_hosts {
            let offset = (start + i) % self.num_hosts;
            if !self.occupied.contains(&offset) {
                self.occupied.insert(offset);
                let ip = u32_to_ip(self.base + 2 + offset);
                return Ok(ip);
            }
        }
        bail!("no more IPs available in subnet")
    }
}

/// FNV-1a hash — deterministic, not seeded like HashMap's hasher.
fn fnv1a_hash(s: &str) -> u32 {
    let mut hash: u32 = 0x811c_9dc5;
    for byte in s.as_bytes() {
        hash ^= *byte as u32;
        hash = hash.wrapping_mul(0x0100_0193);
    }
    hash
}
