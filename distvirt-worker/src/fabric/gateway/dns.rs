use std::collections::HashMap;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

use smoltcp::socket::udp;
use smoltcp::wire::IpEndpoint;
use tokio::net::UdpSocket as TokioUdpSocket;

/// Shared DNS registry: service name → IPv4 address.
pub type DnsRegistry = Arc<RwLock<HashMap<String, Ipv4Addr>>>;

/// Extract the QNAME from a DNS wire-format query.
///
/// DNS queries start with a 12-byte header, followed by the question section.
/// The QNAME is a sequence of length-prefixed labels terminated by a zero byte.
/// Returns the name as a lowercase dotted string (e.g. "db", "web.local").
pub fn parse_qname(query: &[u8]) -> Option<String> {
    if query.len() < 13 {
        return None; // too short for header + at least 1 byte of question
    }

    let mut pos = 12; // skip DNS header
    let mut name = String::new();

    loop {
        if pos >= query.len() {
            return None;
        }

        let label_len = query[pos] as usize;
        pos += 1;

        if label_len == 0 {
            break; // end of QNAME
        }

        // Reject compression pointers in the question section
        if label_len & 0xC0 != 0 {
            return None;
        }

        if pos + label_len > query.len() {
            return None;
        }

        if !name.is_empty() {
            name.push('.');
        }

        let label = std::str::from_utf8(&query[pos..pos + label_len]).ok()?;
        name.push_str(&label.to_ascii_lowercase());
        pos += label_len;
    }

    if name.is_empty() {
        None
    } else {
        Some(name)
    }
}

/// Build a minimal DNS A-record response from a query and an IPv4 address.
///
/// Copies the query header and question section, then sets:
/// - QR bit (response) and AA bit (authoritative) in flags
/// - ANCOUNT = 1
/// - Appends one answer RR: NAME (pointer to offset 12), TYPE=A, CLASS=IN, TTL=60, RDATA=ip
pub fn synthesize_a_response(query: &[u8], ip: Ipv4Addr) -> Vec<u8> {
    // Find end of question section: skip QNAME + QTYPE(2) + QCLASS(2)
    let qname_end = find_qname_end(query, 12);
    if qname_end.is_none() {
        return Vec::new();
    }
    let question_end = qname_end.unwrap() + 4; // +2 QTYPE +2 QCLASS
    if question_end > query.len() {
        return Vec::new();
    }

    let mut resp = Vec::with_capacity(question_end + 16);

    // Copy header + question
    resp.extend_from_slice(&query[..question_end]);

    // Set QR (bit 15) and AA (bit 10) in flags (bytes 2-3)
    resp[2] |= 0x84; // QR=1 (0x80) + AA=1 (0x04)
    resp[3] &= 0x00; // clear RCODE etc.

    // QDCOUNT stays as-is (should be 1)
    // Set ANCOUNT = 1 (bytes 6-7)
    resp[6] = 0;
    resp[7] = 1;
    // NSCOUNT = 0, ARCOUNT = 0
    resp[8] = 0;
    resp[9] = 0;
    resp[10] = 0;
    resp[11] = 0;

    // Answer RR:
    // NAME: pointer to offset 12 (0xC00C)
    resp.push(0xC0);
    resp.push(0x0C);
    // TYPE: A (1)
    resp.push(0x00);
    resp.push(0x01);
    // CLASS: IN (1)
    resp.push(0x00);
    resp.push(0x01);
    // TTL: 60 seconds
    resp.push(0x00);
    resp.push(0x00);
    resp.push(0x00);
    resp.push(0x3C);
    // RDLENGTH: 4
    resp.push(0x00);
    resp.push(0x04);
    // RDATA: IPv4 address
    let octets = ip.octets();
    resp.extend_from_slice(&octets);

    resp
}

/// Convenience: parse the QNAME from a query, look it up in the registry,
/// and return a synthesized A-record response if found.
pub fn try_resolve(registry: &DnsRegistry, query: &[u8]) -> Option<Vec<u8>> {
    let name = parse_qname(query)?;
    let map = registry.read().ok()?;
    let ip = map.get(&name)?;
    Some(synthesize_a_response(query, *ip))
}

/// Find the byte offset just past the QNAME (i.e. the zero terminator + 1).
fn find_qname_end(data: &[u8], start: usize) -> Option<usize> {
    let mut pos = start;
    loop {
        if pos >= data.len() {
            return None;
        }
        let label_len = data[pos] as usize;
        pos += 1;
        if label_len == 0 {
            return Some(pos);
        }
        if label_len & 0xC0 != 0 {
            return None; // compression pointer not expected
        }
        pos += label_len;
    }
}

/// DNS forwarding component: resolves local names from the registry and
/// forwards unresolved queries to upstream nameservers.
pub(crate) struct DnsForwarder {
    registry: DnsRegistry,
    upstream_socket: TokioUdpSocket,
    upstream_servers: Vec<SocketAddr>,
    pending: HashMap<u16, (IpEndpoint, Instant)>,
    timeout: Duration,
}

impl DnsForwarder {
    /// Create a new DNS forwarder: parse /etc/resolv.conf and bind an upstream socket.
    pub fn new(registry: DnsRegistry) -> anyhow::Result<Self> {
        let upstream_servers = parse_resolv_conf();
        log::info!("gateway: upstream DNS servers: {:?}", upstream_servers);

        let upstream_socket = {
            let std_sock = std::net::UdpSocket::bind("0.0.0.0:0")?;
            std_sock.set_nonblocking(true)?;
            TokioUdpSocket::from_std(std_sock)?
        };

        Ok(DnsForwarder {
            registry,
            upstream_socket,
            upstream_servers,
            pending: HashMap::new(),
            timeout: Duration::from_secs(10),
        })
    }

    /// Process DNS queries from the smoltcp UDP socket.
    ///
    /// For local hits, writes the response directly into the socket.
    /// For misses, forwards to upstream. The caller must call `poll_and_drain()`
    /// after this returns if it wrote to the socket (returns `true`).
    pub async fn process_queries(&mut self, sock: &mut udp::Socket<'_>) -> bool {
        let mut wrote_socket = false;
        loop {
            let (query, endpoint) = match sock.recv() {
                Ok((data, meta)) => (data.to_vec(), meta.endpoint),
                Err(_) => break,
            };

            if query.len() < 2 {
                continue;
            }

            let query_id = u16::from_be_bytes([query[0], query[1]]);
            log::info!("gateway: DNS query id={} from {}", query_id, endpoint);

            // Try local registry first.
            if let Some(response) = try_resolve(&self.registry, &query) {
                log::info!("gateway: DNS query id={} resolved locally", query_id);
                if let Err(e) = sock.send_slice(&response, endpoint) {
                    log::warn!("gateway: DNS local response send: {:?}", e);
                }
                wrote_socket = true;
                continue;
            }

            // Fall back to upstream.
            self.pending.insert(query_id, (endpoint, Instant::now()));

            if let Some(upstream) = self.upstream_servers.first() {
                if let Err(e) = self.upstream_socket.send_to(&query, upstream).await {
                    log::warn!("gateway: DNS upstream send error: {}", e);
                }
            }
        }
        wrote_socket
    }

    /// Handle a DNS response from upstream: write it to the smoltcp DNS socket
    /// targeting the original client. Returns `true` if the socket was written to
    /// (caller must call `poll_and_drain()`).
    pub fn handle_response(&mut self, response: &[u8], sock: &mut udp::Socket<'_>) -> bool {
        if response.len() < 2 {
            return false;
        }

        let query_id = u16::from_be_bytes([response[0], response[1]]);
        if let Some((endpoint, _)) = self.pending.remove(&query_id) {
            log::info!("gateway: DNS response id={} -> {}", query_id, endpoint);
            if let Err(e) = sock.send_slice(response, endpoint) {
                log::warn!("gateway: DNS response send to smoltcp: {:?}", e);
            }
            true
        } else {
            log::info!(
                "gateway: DNS response id={} has no pending query",
                query_id
            );
            false
        }
    }

    /// Expose the upstream socket for use in `select!`.
    pub fn upstream_socket(&self) -> &TokioUdpSocket {
        &self.upstream_socket
    }

    /// Remove stale pending DNS entries that have exceeded the timeout.
    pub fn sweep_stale(&mut self) {
        let now = Instant::now();
        let before = self.pending.len();
        self.pending.retain(|id, (_, inserted)| {
            if now.duration_since(*inserted) > self.timeout {
                log::warn!("gateway: DNS query id={} timed out, removing", id);
                false
            } else {
                true
            }
        });
        let expired = before - self.pending.len();
        if expired > 0 {
            log::info!("gateway: swept {} stale DNS entries", expired);
        }
    }
}

/// Parse /etc/resolv.conf for upstream nameservers. Falls back to 8.8.8.8.
fn parse_resolv_conf() -> Vec<SocketAddr> {
    let content = match std::fs::read_to_string("/etc/resolv.conf") {
        Ok(c) => c,
        Err(_) => return parse_resolv_conf_content(""),
    };
    parse_resolv_conf_content(&content)
}

/// Parse the content of a resolv.conf file for nameserver entries.
/// Falls back to 8.8.8.8:53 if no valid nameservers are found.
pub(super) fn parse_resolv_conf_content(content: &str) -> Vec<SocketAddr> {
    let mut servers = Vec::new();
    for line in content.lines() {
        let line = line.trim();
        if let Some(rest) = line.strip_prefix("nameserver") {
            let addr_str = rest.trim();
            if let Ok(ip) = addr_str.parse::<IpAddr>() {
                servers.push(SocketAddr::new(ip, 53));
            }
        }
    }

    if servers.is_empty() {
        servers.push(SocketAddr::new(
            IpAddr::V4(Ipv4Addr::new(8, 8, 8, 8)),
            53,
        ));
    }

    servers
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Build a minimal DNS query for a given name.
    /// Header (12 bytes) + question (QNAME + QTYPE + QCLASS).
    fn make_dns_query(id: u16, name: &str) -> Vec<u8> {
        let mut buf = Vec::new();

        // Header: ID
        buf.extend_from_slice(&id.to_be_bytes());
        // Flags: standard query (0x0100 = RD set)
        buf.push(0x01);
        buf.push(0x00);
        // QDCOUNT = 1
        buf.push(0x00);
        buf.push(0x01);
        // ANCOUNT, NSCOUNT, ARCOUNT = 0
        buf.extend_from_slice(&[0u8; 6]);

        // QNAME: length-prefixed labels
        for label in name.split('.') {
            buf.push(label.len() as u8);
            buf.extend_from_slice(label.as_bytes());
        }
        buf.push(0); // terminator

        // QTYPE = A (1)
        buf.push(0x00);
        buf.push(0x01);
        // QCLASS = IN (1)
        buf.push(0x00);
        buf.push(0x01);

        buf
    }

    #[test]
    fn parse_qname_single_label() {
        let query = make_dns_query(1, "db");
        let name = parse_qname(&query).unwrap();
        assert_eq!(name, "db");
    }

    #[test]
    fn parse_qname_multiple_labels() {
        let query = make_dns_query(2, "web.service.local");
        let name = parse_qname(&query).unwrap();
        assert_eq!(name, "web.service.local");
    }

    #[test]
    fn parse_qname_case_insensitive() {
        let query = make_dns_query(3, "MyService");
        let name = parse_qname(&query).unwrap();
        assert_eq!(name, "myservice");
    }

    #[test]
    fn parse_qname_too_short() {
        assert!(parse_qname(&[0u8; 12]).is_none());
        assert!(parse_qname(&[0u8; 5]).is_none());
    }

    #[test]
    fn synthesize_a_response_valid() {
        let query = make_dns_query(0x1234, "db");
        let ip = Ipv4Addr::new(172, 16, 0, 3);
        let resp = synthesize_a_response(&query, ip);

        // Check ID preserved
        assert_eq!(resp[0], 0x12);
        assert_eq!(resp[1], 0x34);

        // Check QR + AA flags set
        assert_ne!(resp[2] & 0x80, 0, "QR bit should be set");
        assert_ne!(resp[2] & 0x04, 0, "AA bit should be set");

        // ANCOUNT = 1
        assert_eq!(resp[6], 0);
        assert_eq!(resp[7], 1);

        // Answer section: last 4 bytes should be the IP
        let len = resp.len();
        assert_eq!(&resp[len - 4..], &[172, 16, 0, 3]);

        // RDLENGTH = 4
        assert_eq!(resp[len - 6], 0x00);
        assert_eq!(resp[len - 5], 0x04);
    }

    #[test]
    fn try_resolve_found() {
        let registry: DnsRegistry = Arc::new(RwLock::new(HashMap::new()));
        {
            let mut map = registry.write().unwrap();
            map.insert("db".to_string(), Ipv4Addr::new(172, 16, 0, 3));
        }

        let query = make_dns_query(0xABCD, "db");
        let resp = try_resolve(&registry, &query);
        assert!(resp.is_some());

        let resp = resp.unwrap();
        let len = resp.len();
        assert_eq!(&resp[len - 4..], &[172, 16, 0, 3]);
    }

    #[test]
    fn try_resolve_not_found() {
        let registry: DnsRegistry = Arc::new(RwLock::new(HashMap::new()));
        let query = make_dns_query(0x0001, "unknown");
        assert!(try_resolve(&registry, &query).is_none());
    }

    #[test]
    fn try_resolve_empty_registry() {
        let registry: DnsRegistry = Arc::new(RwLock::new(HashMap::new()));
        let query = make_dns_query(0x0001, "db");
        assert!(try_resolve(&registry, &query).is_none());
    }
}
