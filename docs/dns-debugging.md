# Container Networking Issues

## Issue 1: DNS queries not reaching smoltcp (FIXED)

### Problem

DNS resolution failed — no `gateway: DNS query` log lines appeared when the container attempted resolution. smoltcp silently dropped all UDP packets.

### Root Cause

Firecracker's virtio-net negotiates checksum offloading with the guest kernel. The guest sends TCP/UDP packets with partial checksums (pseudo-header only), expecting the "NIC" to complete them. The AF_PACKET socket on the TAP captures frames before checksum completion, so smoltcp received packets with invalid UDP checksums and silently dropped them.

ARP worked (no IP/UDP checksums). Ping to outside IPs worked (routed through TUN, not smoltcp).

### Fix

Set `ChecksumCapabilities::ignored()` in `ChannelDevice::capabilities()` so smoltcp skips checksum verification on incoming frames. Also opened the TUN device with `IFF_VNET_HDR` + `TUNSETOFFLOAD(TUN_F_CSUM)` so the kernel completes partial checksums on packets written to the TUN for internet egress.

## Issue 2: TCP connections fail after DNS resolves (INVESTIGATING)

### Problem

DNS now resolves successfully (query and response logs appear), but HTTPS connections to fetch Alpine packages time out with "temporary error (try again later)".

```
/ # apk add curl
fetch https://dl-cdn.alpinelinux.org/alpine/v3.20/main/x86_64/APKINDEX.tar.gz
gateway: DNS query id=16137 from 172.16.0.2:45860
gateway: DNS response id=16417 -> 172.16.0.2:45860
WARNING: updating and opening https://dl-cdn.alpinelinux.org/alpine/v3.20/main: temporary error (try again later)
```

### What Works

- DNS resolution (queries forwarded upstream, responses delivered to container)
- ICMP ping to outside IPs
- ARP between container and gateway

### What Doesn't Work

- TCP connections from the container to the internet (HTTPS to Alpine CDN)
- The container resolves the hostname but cannot establish TCP connections

### Likely Failure Points to Investigate

1. **DNS response delivery to container**: DNS responses go back through smoltcp → Ethernet frame → fabric → container. With checksums ignored, smoltcp won't compute outgoing UDP checksums. Verify the guest kernel accepts UDP packets with zero/invalid checksums (UDP checksum 0 means "no checksum" in IPv4, so this should be fine).

2. **TUN egress path for TCP**: TCP SYN packets from the container go through the TUN. With `IFF_VNET_HDR`, verify the vnet header is correctly constructed (csum_start, csum_offset) and that the kernel actually completes the checksum before forwarding.

3. **NAT/iptables MASQUERADE**: The TUN device routes packets from 172.16.0.2 to the internet. Even with `ip_forward=1`, packets need source NAT (MASQUERADE) so responses can route back. The code does not set up iptables rules. Check if MASQUERADE is configured on the host for the TUN interface.

4. **TUN ingress path**: When internet responses arrive at the TUN, the gateway reads them (now with VNET_HDR_SZ prefix), strips the vnet header, wraps in Ethernet, and sends to the fabric. Verify the vnet header stripping is correct and frames reach the container.

## Architecture

```
Container (172.16.0.2, MAC 06:00:AC:10:00:02)
  -> /etc/resolv.conf points to 172.16.0.1
  -> DNS query UDP to 172.16.0.1:53
  -> TCP to internet via default route 172.16.0.1

Fabric Switch (distvirt/src/fabric/switch.rs)
  -> Routes frames by destination MAC
  -> Frames destined for GATEWAY_MAC go to gateway channel

Gateway (distvirt/src/fabric/gateway.rs)
  -> Receives frames on egress_rx channel
  -> If ARP or IPv4 destined for GATEWAY_IP: feed to smoltcp
  -> smoltcp UDP socket on port 53 handles DNS forwarding
  -> Other IPv4 (internet-bound): strip Ethernet, write to TUN with vnet header
  -> TUN ingress: strip vnet header, wrap in Ethernet, send to fabric
```

## Key Files

| File | Role |
|------|------|
| `distvirt/src/fabric/gateway.rs` | Gateway: ARP, DNS forwarding, TUN egress |
| `distvirt/src/fabric/switch.rs` | Ethernet frame switching between VMs and gateway |
| `distvirt/src/fabric/mod.rs` | Fabric setup, spawns switch and gateway |
| `guest-image/guest-init/src/container.rs` | Writes resolv.conf, hosts, hostname |
| `distvirt/src/orchestrate.rs` | Sends AddContainer with dns_servers=["172.16.0.1"] |
