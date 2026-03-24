//! Per-namespace IP allocation.
//!
//! The allocator splits the subnet into three zones:
//!
//! ```text
//! |--- auto zone (N/2) ---|--- manual zone ---|-- WG (reserve) --|
//! base+2              midpoint            top-reserve          top
//! ```
//!
//! - **Auto zone**: orchestrator assigns IPs sequentially from the bottom.
//! - **Manual zone**: user specifies explicit IPs in this range.
//! - **WireGuard zone**: reserved for WireGuard peer allocation (top-down,
//!   managed by `WireGuardPeerManager`).
//!
//! Auto and manual zones never overlap, preventing collisions by construction.

use std::collections::{BTreeMap, HashSet};
use std::net::Ipv4Addr;

use crate::types::{
    IpAllocKind, IpAllocResult, IpAllocation, IpResourceKey,
};

// =============================================================================
// Error type
// =============================================================================

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IpAllocError {
    /// The auto zone is exhausted — no more automatic IPs available.
    AutoZoneExhausted,
    /// The manual zone is exhausted.
    ManualZoneExhausted,
    /// The specified manual IP is outside the manual zone.
    IpOutsideManualZone { ip: Ipv4Addr },
    /// The specified manual IP is already in use by another resource.
    ManualIpCollision { ip: Ipv4Addr, existing_key: IpResourceKey },
    /// Attempted to change allocation kind (auto → manual or vice versa).
    KindMigration {
        key: IpResourceKey,
        existing: IpAllocKind,
        requested: IpAllocKind,
    },
}

impl std::fmt::Display for IpAllocError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::AutoZoneExhausted => write!(f, "no more automatic IPs available in subnet"),
            Self::ManualZoneExhausted => write!(f, "no more manual IPs available in subnet"),
            Self::IpOutsideManualZone { ip } => {
                write!(f, "IP {ip} is outside the manual allocation zone")
            }
            Self::ManualIpCollision { ip, existing_key } => {
                write!(f, "IP {ip} is already in use by {existing_key:?}")
            }
            Self::KindMigration {
                key,
                existing,
                requested,
            } => {
                write!(
                    f,
                    "cannot change allocation kind for {key:?} from {existing:?} to {requested:?} \
                     (delete and re-create the resource to change)"
                )
            }
        }
    }
}

impl std::error::Error for IpAllocError {}

// =============================================================================
// Allocator
// =============================================================================

pub struct NamespaceIpAllocator {
    /// Base address of the subnet (e.g. 172.16.0.0). Kept for diagnostics.
    #[allow(dead_code)]
    subnet_base: u32,
    /// First usable host = subnet_base + 2 (skips .0 network and .1 gateway).
    first_host: u32,

    // Zone sizes (in number of addresses).
    auto_zone_size: u32,
    manual_zone_start: u32, // offset from first_host
    manual_zone_end: u32,   // exclusive, offset from first_host

    // All allocations keyed by resource.
    allocations: BTreeMap<IpResourceKey, (u32, IpAllocKind)>, // key → (offset, kind)
    /// All occupied offsets for O(1) collision detection.
    occupied_offsets: HashSet<u32>,

    // Auto zone cursor and free list.
    next_auto_offset: u32,
    auto_free_list: Vec<u32>,
}

impl NamespaceIpAllocator {
    /// Create a new allocator for the given subnet.
    ///
    /// `wg_reserve` addresses are reserved at the top of the subnet for
    /// WireGuard peer allocation (managed externally by `WireGuardPeerManager`).
    pub fn new(subnet: Ipv4Addr, prefix_len: u8, wg_reserve: u32) -> Self {
        let subnet_base = u32::from(subnet);
        let total_addrs = 1u32.checked_shl(32 - prefix_len as u32).unwrap_or(0);
        // Usable: total - network (.0) - gateway (.1)
        let usable = total_addrs.saturating_sub(2);
        // WG zone at the top
        let non_wg = usable.saturating_sub(wg_reserve);
        // Split remaining in half: auto (bottom) / manual (top)
        let auto_zone_size = non_wg / 2;
        let manual_zone_start = auto_zone_size;
        let manual_zone_end = non_wg;

        NamespaceIpAllocator {
            subnet_base,
            first_host: subnet_base + 2,
            auto_zone_size,
            manual_zone_start,
            manual_zone_end,
            allocations: BTreeMap::new(),
            occupied_offsets: HashSet::new(),
            next_auto_offset: 0,
            auto_free_list: Vec::new(),
        }
    }

    /// Allocate an IP for the given resource key.
    ///
    /// - If the key already has an allocation, returns it (sticky).
    /// - If `explicit_ip` is `Some`, allocates in the manual zone.
    /// - If `explicit_ip` is `None`, allocates in the auto zone.
    pub fn allocate(
        &mut self,
        key: IpResourceKey,
        explicit_ip: Option<Ipv4Addr>,
    ) -> Result<IpAllocation, IpAllocError> {
        let requested_kind = if explicit_ip.is_some() {
            IpAllocKind::Manual
        } else {
            IpAllocKind::Auto
        };

        // Sticky: if already allocated, return existing (or error on kind change).
        if let Some(&(offset, existing_kind)) = self.allocations.get(&key) {
            if existing_kind != requested_kind {
                return Err(IpAllocError::KindMigration {
                    key,
                    existing: existing_kind,
                    requested: requested_kind,
                });
            }
            return Ok(IpAllocation {
                ip: self.offset_to_ip(offset),
                kind: existing_kind,
            });
        }

        match explicit_ip {
            None => self.allocate_auto(key),
            Some(ip) => self.allocate_manual(key, ip),
        }
    }

    /// Release an allocation, freeing the IP for reuse.
    pub fn release(&mut self, key: &IpResourceKey) {
        if let Some((offset, kind)) = self.allocations.remove(key) {
            self.occupied_offsets.remove(&offset);
            if kind == IpAllocKind::Auto {
                self.auto_free_list.push(offset);
            }
        }
    }

    /// Return a full snapshot of all current allocations.
    pub fn full_snapshot(&self) -> IpAllocResult {
        let mut result = IpAllocResult::default();
        for (key, &(offset, kind)) in &self.allocations {
            let alloc = IpAllocation {
                ip: self.offset_to_ip(offset),
                kind,
            };
            match key {
                IpResourceKey::Workload(name) => {
                    result.workload_ips.insert(name.clone(), alloc);
                }
                IpResourceKey::Service(name) => {
                    result.service_ips.insert(name.clone(), alloc);
                }
            }
        }
        result
    }

    /// Convert an offset (from first_host) to an IP address.
    fn offset_to_ip(&self, offset: u32) -> Ipv4Addr {
        Ipv4Addr::from(self.first_host + offset)
    }

    /// Convert an IP to an offset from first_host, or None if outside the subnet.
    fn ip_to_offset(&self, ip: Ipv4Addr) -> Option<u32> {
        let ip_u32 = u32::from(ip);
        ip_u32.checked_sub(self.first_host)
    }

    fn allocate_auto(&mut self, key: IpResourceKey) -> Result<IpAllocation, IpAllocError> {
        // Try the free list first.
        if let Some(offset) = self.auto_free_list.pop() {
            self.occupied_offsets.insert(offset);
            self.allocations
                .insert(key, (offset, IpAllocKind::Auto));
            return Ok(IpAllocation {
                ip: self.offset_to_ip(offset),
                kind: IpAllocKind::Auto,
            });
        }

        // Sequential scan from cursor.
        let start = self.next_auto_offset;
        if start >= self.auto_zone_size {
            return Err(IpAllocError::AutoZoneExhausted);
        }

        // The auto zone is exclusively ours, so no collision check needed
        // against manual allocations. But we do skip offsets that are somehow
        // already occupied (defensive).
        for candidate in start..self.auto_zone_size {
            if !self.occupied_offsets.contains(&candidate) {
                self.next_auto_offset = candidate + 1;
                self.occupied_offsets.insert(candidate);
                self.allocations
                    .insert(key, (candidate, IpAllocKind::Auto));
                return Ok(IpAllocation {
                    ip: self.offset_to_ip(candidate),
                    kind: IpAllocKind::Auto,
                });
            }
        }

        Err(IpAllocError::AutoZoneExhausted)
    }

    fn allocate_manual(
        &mut self,
        key: IpResourceKey,
        ip: Ipv4Addr,
    ) -> Result<IpAllocation, IpAllocError> {
        let offset = self
            .ip_to_offset(ip)
            .ok_or(IpAllocError::IpOutsideManualZone { ip })?;

        // Validate the IP is in the manual zone.
        if offset < self.manual_zone_start || offset >= self.manual_zone_end {
            return Err(IpAllocError::IpOutsideManualZone { ip });
        }

        // Check collision.
        if self.occupied_offsets.contains(&offset) {
            // Find who owns it.
            let existing_key = self
                .allocations
                .iter()
                .find(|&(_, &(o, _))| o == offset)
                .map(|(k, _)| k.clone())
                .unwrap_or(key.clone());
            return Err(IpAllocError::ManualIpCollision {
                ip,
                existing_key,
            });
        }

        self.occupied_offsets.insert(offset);
        self.allocations
            .insert(key, (offset, IpAllocKind::Manual));
        Ok(IpAllocation {
            ip,
            kind: IpAllocKind::Manual,
        })
    }

    /// Auto zone size (for testing/diagnostics).
    #[cfg(test)]
    pub fn auto_zone_size(&self) -> u32 {
        self.auto_zone_size
    }

    /// Manual zone size (for testing/diagnostics).
    #[cfg(test)]
    pub fn manual_zone_size(&self) -> u32 {
        self.manual_zone_end - self.manual_zone_start
    }
}

/// Generate a locally-administered unicast MAC from an IPv4 address.
///
/// Format: `02:00:a:b:c:d` where a.b.c.d are the IP octets.
pub fn ip_to_mac(ip: Ipv4Addr) -> [u8; 6] {
    let o = ip.octets();
    [0x02, 0x00, o[0], o[1], o[2], o[3]]
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::WorkloadName;

    fn make_allocator() -> NamespaceIpAllocator {
        // 172.16.0.0/24: 254 usable (.2-.255)
        // wg_reserve=10 → 244 non-WG usable
        // auto: 122, manual: 122
        NamespaceIpAllocator::new(Ipv4Addr::new(172, 16, 0, 0), 24, 10)
    }

    fn wl(name: &str) -> IpResourceKey {
        IpResourceKey::Workload(WorkloadName(name.to_string()))
    }

    fn svc(name: &str) -> IpResourceKey {
        IpResourceKey::Service(name.to_string())
    }

    #[test]
    fn zone_sizes() {
        let a = make_allocator();
        // 254 usable, minus 10 WG = 244, split in half = 122 each
        assert_eq!(a.auto_zone_size(), 122);
        assert_eq!(a.manual_zone_size(), 122);
    }

    #[test]
    fn auto_assign_sequential() {
        let mut a = make_allocator();
        let r1 = a.allocate(wl("a"), None).unwrap();
        let r2 = a.allocate(wl("b"), None).unwrap();
        let r3 = a.allocate(svc("s1"), None).unwrap();

        assert_eq!(r1.kind, IpAllocKind::Auto);
        assert_eq!(r1.ip, Ipv4Addr::new(172, 16, 0, 2)); // first_host + 0
        assert_eq!(r2.ip, Ipv4Addr::new(172, 16, 0, 3)); // first_host + 1
        assert_eq!(r3.ip, Ipv4Addr::new(172, 16, 0, 4)); // first_host + 2
    }

    #[test]
    fn sticky_allocation() {
        let mut a = make_allocator();
        let r1 = a.allocate(wl("a"), None).unwrap();
        let r2 = a.allocate(wl("a"), None).unwrap();
        assert_eq!(r1.ip, r2.ip);
    }

    #[test]
    fn manual_allocation() {
        let mut a = make_allocator();
        // Manual zone starts at offset 122 → 172.16.0.124 (base+2+122)
        let ip = Ipv4Addr::new(172, 16, 0, 130);
        let r = a.allocate(wl("m"), Some(ip)).unwrap();
        assert_eq!(r.ip, ip);
        assert_eq!(r.kind, IpAllocKind::Manual);
    }

    #[test]
    fn manual_outside_zone_rejected() {
        let mut a = make_allocator();
        // 172.16.0.5 is in the auto zone
        let ip = Ipv4Addr::new(172, 16, 0, 5);
        let err = a.allocate(wl("m"), Some(ip)).unwrap_err();
        assert!(matches!(err, IpAllocError::IpOutsideManualZone { .. }));
    }

    #[test]
    fn manual_collision_detected() {
        let mut a = make_allocator();
        let ip = Ipv4Addr::new(172, 16, 0, 130);
        a.allocate(wl("a"), Some(ip)).unwrap();
        let err = a.allocate(wl("b"), Some(ip)).unwrap_err();
        assert!(matches!(err, IpAllocError::ManualIpCollision { .. }));
    }

    #[test]
    fn kind_migration_rejected() {
        let mut a = make_allocator();
        // Auto-assign first
        a.allocate(wl("a"), None).unwrap();
        // Try to switch to manual
        let ip = Ipv4Addr::new(172, 16, 0, 130);
        let err = a.allocate(wl("a"), Some(ip)).unwrap_err();
        assert!(matches!(err, IpAllocError::KindMigration { .. }));
    }

    #[test]
    fn release_and_reuse() {
        let mut a = make_allocator();
        let r1 = a.allocate(wl("a"), None).unwrap();
        let ip1 = r1.ip;

        a.release(&wl("a"));

        // Allocate a new workload — should get the released IP via free list
        let r2 = a.allocate(wl("b"), None).unwrap();
        assert_eq!(r2.ip, ip1);
    }

    #[test]
    fn auto_zone_exhaustion() {
        // Tiny subnet: /28 = 14 usable, wg_reserve=2 → 12 non-WG, auto=6
        let mut a = NamespaceIpAllocator::new(Ipv4Addr::new(10, 0, 0, 0), 28, 2);
        assert_eq!(a.auto_zone_size(), 6);

        for i in 0..6 {
            a.allocate(wl(&format!("w{i}")), None).unwrap();
        }
        let err = a.allocate(wl("overflow"), None).unwrap_err();
        assert!(matches!(err, IpAllocError::AutoZoneExhausted));
    }

    #[test]
    fn full_snapshot() {
        let mut a = make_allocator();
        a.allocate(wl("api"), None).unwrap();
        a.allocate(svc("db"), None).unwrap();
        let ip = Ipv4Addr::new(172, 16, 0, 130);
        a.allocate(wl("manual"), Some(ip)).unwrap();

        let snap = a.full_snapshot();
        assert_eq!(snap.workload_ips.len(), 2);
        assert_eq!(snap.service_ips.len(), 1);
        assert_eq!(
            snap.workload_ips[&WorkloadName("manual".into())].kind,
            IpAllocKind::Manual
        );
        assert_eq!(
            snap.workload_ips[&WorkloadName("api".into())].kind,
            IpAllocKind::Auto
        );
    }

    #[test]
    fn workload_and_service_same_name_distinct() {
        let mut a = make_allocator();
        let r1 = a.allocate(wl("api"), None).unwrap();
        let r2 = a.allocate(svc("api"), None).unwrap();
        // Different keys, different IPs
        assert_ne!(r1.ip, r2.ip);
    }

    #[test]
    fn ip_to_mac_generation() {
        let mac = ip_to_mac(Ipv4Addr::new(172, 16, 0, 50));
        assert_eq!(mac, [0x02, 0x00, 172, 16, 0, 50]);
    }

    #[test]
    fn manual_in_wg_zone_rejected() {
        let mut a = make_allocator();
        // WG zone is the last 10 addresses: 172.16.0.246-255 (offsets 244-253)
        // manual_zone_end = 244, so offset 244+ is WG zone
        let ip = Ipv4Addr::new(172, 16, 0, 250); // offset = 248, in WG zone
        let err = a.allocate(wl("wg-collision"), Some(ip)).unwrap_err();
        assert!(matches!(err, IpAllocError::IpOutsideManualZone { .. }));
    }

    #[test]
    fn sticky_manual_allocation() {
        let mut a = make_allocator();
        let ip = Ipv4Addr::new(172, 16, 0, 130);
        let r1 = a.allocate(wl("m"), Some(ip)).unwrap();
        let r2 = a.allocate(wl("m"), Some(ip)).unwrap();
        assert_eq!(r1.ip, r2.ip);
        assert_eq!(r2.kind, IpAllocKind::Manual);
    }
}
