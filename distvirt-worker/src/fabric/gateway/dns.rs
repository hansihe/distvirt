use std::collections::HashMap;
use std::net::Ipv4Addr;
use std::sync::{Arc, RwLock};

use hickory_resolver::TokioResolver;
use smoltcp::socket::udp;
use smoltcp::wire::IpEndpoint;
use tokio::sync::mpsc;

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

    if name.is_empty() { None } else { Some(name) }
}

/// Build a minimal DNS A-record response from a query and an IPv4 address.
///
/// Copies the query header and question section, then sets:
/// - QR bit (response) and AA bit (authoritative) in flags
/// - ANCOUNT = 1
/// - Appends one answer RR: NAME (pointer to offset 12), TYPE=A, CLASS=IN, TTL=60, RDATA=ip
pub fn synthesize_a_response(query: &[u8], ip: Ipv4Addr) -> Vec<u8> {
    // Find end of question section: skip QNAME + QTYPE(2) + QCLASS(2)
    let Some(qname_end) = find_qname_end(query, 12) else {
        return Vec::new();
    };
    let question_end = qname_end + 4; // +2 QTYPE +2 QCLASS
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

/// Build a minimal NXDOMAIN response from a query.
fn synthesize_nxdomain_response(query: &[u8]) -> Vec<u8> {
    let Some(qname_end) = find_qname_end(query, 12) else {
        return Vec::new();
    };
    let question_end = qname_end + 4;
    if question_end > query.len() {
        return Vec::new();
    }

    let mut resp = Vec::with_capacity(question_end);
    resp.extend_from_slice(&query[..question_end]);

    // Set QR=1 (0x80), AA=1 (0x04) in byte 2
    resp[2] |= 0x84;
    // Set RCODE=3 (NXDOMAIN) in byte 3
    resp[3] = (resp[3] & 0xF0) | 0x03;

    // ANCOUNT = 0, NSCOUNT = 0, ARCOUNT = 0
    resp[6] = 0;
    resp[7] = 0;
    resp[8] = 0;
    resp[9] = 0;
    resp[10] = 0;
    resp[11] = 0;

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

/// Result of an async DNS lookup via hickory-resolver.
pub(crate) struct DnsLookupResult {
    query: Vec<u8>,
    endpoint: IpEndpoint,
    result: Option<Ipv4Addr>,
}

/// DNS forwarding component: resolves local names from the registry and
/// forwards unresolved queries to upstream nameservers via hickory-resolver.
pub(crate) struct DnsForwarder {
    registry: DnsRegistry,
    resolver: TokioResolver,
    result_tx: mpsc::Sender<DnsLookupResult>,
    result_rx: mpsc::Receiver<DnsLookupResult>,
}

impl DnsForwarder {
    /// Create a new DNS forwarder using system-configured resolvers.
    pub fn new(registry: DnsRegistry) -> anyhow::Result<Self> {
        let resolver = TokioResolver::builder_tokio()?.build();
        let (result_tx, result_rx) = mpsc::channel(64);

        Ok(DnsForwarder {
            registry,
            resolver,
            result_tx,
            result_rx,
        })
    }

    /// Process DNS queries from the smoltcp UDP socket.
    ///
    /// For local hits, writes the response directly into the socket.
    /// For misses, spawns an async lookup task. Results arrive on `result_rx`.
    /// Returns `true` if the socket was written to (caller must call `poll_and_drain()`).
    pub fn process_queries(&self, sock: &mut udp::Socket<'_>) -> bool {
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

            // Forward to upstream via hickory-resolver.
            let qname = match parse_qname(&query) {
                Some(n) => n,
                None => continue,
            };

            let resolver = self.resolver.clone();
            let tx = self.result_tx.clone();
            tokio::spawn(async move {
                let ip = match resolver.lookup_ip(&qname).await {
                    Ok(lookup) => lookup.iter().find_map(|addr| match addr {
                        std::net::IpAddr::V4(v4) => Some(v4),
                        _ => None,
                    }),
                    Err(e) => {
                        log::warn!("gateway: DNS upstream resolve for '{}': {}", qname, e);
                        None
                    }
                };

                let _ = tx
                    .send(DnsLookupResult {
                        query,
                        endpoint,
                        result: ip,
                    })
                    .await;
            });
        }
        wrote_socket
    }

    /// Returns the receiver for completed DNS lookup results.
    pub fn result_rx(&mut self) -> &mut mpsc::Receiver<DnsLookupResult> {
        &mut self.result_rx
    }

    /// Write a resolved DNS result back to the smoltcp socket.
    /// Returns `true` if the socket was written to.
    pub fn write_result(&self, result: DnsLookupResult, sock: &mut udp::Socket<'_>) -> bool {
        let response = match result.result {
            Some(ip) => synthesize_a_response(&result.query, ip),
            None => synthesize_nxdomain_response(&result.query),
        };

        if response.is_empty() {
            return false;
        }

        let query_id = u16::from_be_bytes([result.query[0], result.query[1]]);
        log::info!(
            "gateway: DNS response id={} -> {} ({})",
            query_id,
            result.endpoint,
            result
                .result
                .map(|ip| ip.to_string())
                .unwrap_or_else(|| "NXDOMAIN".to_string())
        );

        if let Err(e) = sock.send_slice(&response, result.endpoint) {
            log::warn!("gateway: DNS response send to smoltcp: {:?}", e);
        }
        true
    }
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
            let mut map = registry.write().expect("poisoned");
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
