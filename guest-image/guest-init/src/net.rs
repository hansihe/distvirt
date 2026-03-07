use std::ffi::CString;
use std::net::Ipv4Addr;
use std::os::unix::io::{AsRawFd, FromRawFd, OwnedFd};

use anyhow::{bail, Context};

/// Configure a network interface with IP, netmask, bring it up, and add a default route.
pub fn configure_network(
    interface: &str,
    ip: &str,
    netmask: &str,
    gateway: &str,
) -> anyhow::Result<()> {
    let sock = unsafe { libc::socket(libc::AF_INET, libc::SOCK_DGRAM | libc::SOCK_CLOEXEC, 0) };
    if sock < 0 {
        bail!("socket: {}", std::io::Error::last_os_error());
    }
    let sock = unsafe { OwnedFd::from_raw_fd(sock) };

    configure_network_inner(sock.as_raw_fd(), interface, ip, netmask, gateway)
}

fn configure_network_inner(
    sock: i32,
    interface: &str,
    ip: &str,
    netmask: &str,
    gateway: &str,
) -> anyhow::Result<()> {
    let ifname = CString::new(interface)?;

    // Set IP address (SIOCSIFADDR).
    set_if_addr(sock, &ifname, ip, libc::SIOCSIFADDR)
        .with_context(|| format!("set IP {} on {}", ip, interface))?;

    // Set netmask (SIOCSIFNETMASK).
    set_if_addr(sock, &ifname, netmask, libc::SIOCSIFNETMASK)
        .with_context(|| format!("set netmask {} on {}", netmask, interface))?;

    // Bring interface up (SIOCSIFFLAGS with IFF_UP | IFF_RUNNING).
    bring_if_up(sock, &ifname)
        .with_context(|| format!("bring up {}", interface))?;

    // Add default route via gateway (SIOCADDRT).
    add_default_route(sock, gateway)
        .with_context(|| format!("add default route via {}", gateway))?;

    log::info!(
        "configured {}: ip={}, netmask={}, gateway={}",
        interface, ip, netmask, gateway
    );
    Ok(())
}

/// Build a sockaddr_in from an IPv4 address string.
fn make_sockaddr_in(addr: &str) -> anyhow::Result<libc::sockaddr_in> {
    let ip: Ipv4Addr = addr.parse().with_context(|| format!("parse IP: {}", addr))?;
    let octets = ip.octets();
    let s_addr = u32::from_ne_bytes(octets);
    Ok(libc::sockaddr_in {
        sin_family: libc::AF_INET as libc::sa_family_t,
        sin_port: 0,
        sin_addr: libc::in_addr { s_addr },
        sin_zero: [0; 8],
    })
}

/// Set an interface address using the given ioctl (SIOCSIFADDR or SIOCSIFNETMASK).
fn set_if_addr(sock: i32, ifname: &CString, addr: &str, ioctl_num: libc::c_ulong) -> anyhow::Result<()> {
    let sockaddr = make_sockaddr_in(addr)?;

    #[repr(C)]
    struct Ifreq {
        ifr_name: [u8; libc::IFNAMSIZ],
        ifr_addr: libc::sockaddr_in,
    }

    let mut ifr: Ifreq = unsafe { std::mem::zeroed() };
    let name_bytes = ifname.as_bytes();
    let copy_len = name_bytes.len().min(libc::IFNAMSIZ - 1);
    ifr.ifr_name[..copy_len].copy_from_slice(&name_bytes[..copy_len]);
    ifr.ifr_addr = sockaddr;

    let ret = unsafe { libc::ioctl(sock, ioctl_num as _, &ifr as *const Ifreq) };
    if ret < 0 {
        bail!("ioctl: {}", std::io::Error::last_os_error());
    }
    Ok(())
}

/// Bring an interface up by setting IFF_UP | IFF_RUNNING flags.
fn bring_if_up(sock: i32, ifname: &CString) -> anyhow::Result<()> {
    #[repr(C)]
    struct IfreqFlags {
        ifr_name: [u8; libc::IFNAMSIZ],
        ifr_flags: libc::c_short,
        _pad: [u8; 22],
    }

    let mut ifr: IfreqFlags = unsafe { std::mem::zeroed() };
    let name_bytes = ifname.as_bytes();
    let copy_len = name_bytes.len().min(libc::IFNAMSIZ - 1);
    ifr.ifr_name[..copy_len].copy_from_slice(&name_bytes[..copy_len]);

    // Get current flags first.
    let ret = unsafe { libc::ioctl(sock, libc::SIOCGIFFLAGS as _, &mut ifr as *mut IfreqFlags) };
    if ret < 0 {
        bail!("SIOCGIFFLAGS: {}", std::io::Error::last_os_error());
    }

    ifr.ifr_flags |= (libc::IFF_UP | libc::IFF_RUNNING) as libc::c_short;

    let ret = unsafe { libc::ioctl(sock, libc::SIOCSIFFLAGS as _, &ifr as *const IfreqFlags) };
    if ret < 0 {
        bail!("SIOCSIFFLAGS: {}", std::io::Error::last_os_error());
    }
    Ok(())
}

/// Add a default route (0.0.0.0/0) via the given gateway.
fn add_default_route(sock: i32, gateway: &str) -> anyhow::Result<()> {
    let gw_addr = make_sockaddr_in(gateway)?;
    let zero_addr = make_sockaddr_in("0.0.0.0")?;

    // struct rtentry for SIOCADDRT
    let mut rt: libc::rtentry = unsafe { std::mem::zeroed() };

    // Destination = 0.0.0.0
    unsafe {
        std::ptr::copy_nonoverlapping(
            &zero_addr as *const libc::sockaddr_in as *const libc::sockaddr,
            &mut rt.rt_dst,
            1,
        );
    }

    // Gateway
    unsafe {
        std::ptr::copy_nonoverlapping(
            &gw_addr as *const libc::sockaddr_in as *const libc::sockaddr,
            &mut rt.rt_gateway,
            1,
        );
    }

    // Netmask = 0.0.0.0 (default route)
    unsafe {
        std::ptr::copy_nonoverlapping(
            &zero_addr as *const libc::sockaddr_in as *const libc::sockaddr,
            &mut rt.rt_genmask,
            1,
        );
    }

    rt.rt_flags = (libc::RTF_UP | libc::RTF_GATEWAY) as libc::c_ushort;

    let ret = unsafe { libc::ioctl(sock, libc::SIOCADDRT as _, &rt as *const libc::rtentry) };
    if ret < 0 {
        bail!("SIOCADDRT: {}", std::io::Error::last_os_error());
    }
    Ok(())
}

// ── tc qdisc plug/unplug via netlink ────────────────────────────────────

const RTM_NEWQDISC: u16 = 36;
const RTM_DELQDISC: u16 = 37;
const NLM_F_REQUEST: u16 = 1;
const NLM_F_ACK: u16 = 4;
const NLM_F_CREATE: u16 = 0x400;
const NLM_F_EXCL: u16 = 0x200;
const NLMSG_ERROR: u16 = 2;
const NLMSG_HDRLEN: usize = 16;
const TC_H_ROOT: u32 = 0xFFFFFFFF;
const TCA_KIND: u16 = 1;
const TCA_OPTIONS: u16 = 2;
const TCQ_PLUG_RELEASE_INDEFINITE: i32 = 2;

#[repr(C)]
struct Nlmsghdr {
    nlmsg_len: u32,
    nlmsg_type: u16,
    nlmsg_flags: u16,
    nlmsg_seq: u32,
    nlmsg_pid: u32,
}

#[repr(C)]
struct Tcmsg {
    tcm_family: u8,
    _pad1: u8,
    _pad2: u16,
    tcm_ifindex: i32,
    tcm_handle: u32,
    tcm_parent: u32,
    tcm_info: u32,
}

#[repr(C)]
struct TcPlugQopt {
    action: i32,
    limit: u32,
}

/// Reinterpret a `#[repr(C)]` struct as a byte slice.
unsafe fn as_bytes<T: Sized>(val: &T) -> &[u8] {
    unsafe { std::slice::from_raw_parts(val as *const T as *const u8, std::mem::size_of::<T>()) }
}

/// Append a netlink attribute to a buffer with 4-byte alignment padding.
fn nla_put(buf: &mut Vec<u8>, nla_type: u16, payload: &[u8]) {
    let nla_len = 4u16 + payload.len() as u16;
    buf.extend_from_slice(&nla_len.to_ne_bytes());
    buf.extend_from_slice(&nla_type.to_ne_bytes());
    buf.extend_from_slice(payload);
    let pad = (4 - (payload.len() % 4)) % 4;
    for _ in 0..pad {
        buf.push(0);
    }
}

fn get_ifindex(interface: &str) -> anyhow::Result<u32> {
    let ifname = CString::new(interface)?;
    let idx = unsafe { libc::if_nametoindex(ifname.as_ptr()) };
    if idx == 0 {
        bail!("if_nametoindex({}): {}", interface, std::io::Error::last_os_error());
    }
    Ok(idx)
}

fn netlink_open() -> anyhow::Result<OwnedFd> {
    let fd = unsafe {
        libc::socket(libc::AF_NETLINK, libc::SOCK_DGRAM | libc::SOCK_CLOEXEC, libc::NETLINK_ROUTE)
    };
    if fd < 0 {
        bail!("netlink socket: {}", std::io::Error::last_os_error());
    }
    Ok(unsafe { OwnedFd::from_raw_fd(fd) })
}

/// Send a netlink message and wait for the ACK.
fn netlink_request(sock: &OwnedFd, msg: &[u8]) -> anyhow::Result<()> {
    let fd = sock.as_raw_fd();

    let sent = unsafe { libc::send(fd, msg.as_ptr() as *const libc::c_void, msg.len(), 0) };
    if sent < 0 {
        bail!("netlink send: {}", std::io::Error::last_os_error());
    }

    let mut resp = [0u8; 1024];
    let n = unsafe { libc::recv(fd, resp.as_mut_ptr() as *mut libc::c_void, resp.len(), 0) };
    if n < 0 {
        bail!("netlink recv: {}", std::io::Error::last_os_error());
    }
    let n = n as usize;
    if n < NLMSG_HDRLEN {
        bail!("netlink response too short: {} bytes", n);
    }

    let nlmsg_type = u16::from_ne_bytes(resp[4..6].try_into().unwrap());
    if nlmsg_type == NLMSG_ERROR {
        if n < NLMSG_HDRLEN + 4 {
            bail!("NLMSG_ERROR response too short");
        }
        let errno = i32::from_ne_bytes(resp[NLMSG_HDRLEN..NLMSG_HDRLEN + 4].try_into().unwrap());
        if errno < 0 {
            bail!("{}", std::io::Error::from_raw_os_error(-errno));
        }
    }

    Ok(())
}

/// Build a netlink qdisc message.
fn build_qdisc_msg(
    msg_type: u16,
    flags: u16,
    ifindex: u32,
    kind: Option<&[u8]>,
    options: Option<&[u8]>,
) -> Vec<u8> {
    let mut buf = Vec::with_capacity(128);

    // Placeholder for nlmsghdr.
    buf.extend_from_slice(&[0u8; NLMSG_HDRLEN]);

    let tcmsg = Tcmsg {
        tcm_family: 0,
        _pad1: 0,
        _pad2: 0,
        tcm_ifindex: ifindex as i32,
        tcm_handle: 0,
        tcm_parent: TC_H_ROOT,
        tcm_info: 0,
    };
    buf.extend_from_slice(unsafe { as_bytes(&tcmsg) });

    if let Some(kind) = kind {
        nla_put(&mut buf, TCA_KIND, kind);
    }
    if let Some(opts) = options {
        nla_put(&mut buf, TCA_OPTIONS, opts);
    }

    // Fill in nlmsghdr.
    let hdr = Nlmsghdr {
        nlmsg_len: buf.len() as u32,
        nlmsg_type: msg_type,
        nlmsg_flags: flags,
        nlmsg_seq: 1,
        nlmsg_pid: 0,
    };
    buf[..NLMSG_HDRLEN].copy_from_slice(unsafe { as_bytes(&hdr) });

    buf
}

pub const DEFAULT_INTERFACE: &str = "eth0";

/// Suspend network traffic by installing a plug qdisc on the default interface.
pub fn suspend() -> anyhow::Result<()> {
    plug_qdisc(DEFAULT_INTERFACE)
}

/// Resume network traffic by removing the plug qdisc from the default interface.
pub fn resume() -> anyhow::Result<()> {
    unplug_qdisc(DEFAULT_INTERFACE)
}

/// Install a `plug` qdisc on the given interface to buffer all outbound packets.
///
/// Equivalent to: `tc qdisc add dev <interface> root plug`
pub fn plug_qdisc(interface: &str) -> anyhow::Result<()> {
    let ifindex = get_ifindex(interface)?;
    let sock = netlink_open()?;

    let msg = build_qdisc_msg(
        RTM_NEWQDISC,
        NLM_F_REQUEST | NLM_F_ACK | NLM_F_CREATE | NLM_F_EXCL,
        ifindex,
        Some(b"plug\0"),
        None,
    );
    netlink_request(&sock, &msg).context("add plug qdisc")?;

    log::info!("plug qdisc installed on {}", interface);
    Ok(())
}

/// Release buffered packets and remove the `plug` qdisc.
///
/// Equivalent to:
///   `tc qdisc change dev <interface> root plug release_indefinite`
///   `tc qdisc del dev <interface> root`
pub fn unplug_qdisc(interface: &str) -> anyhow::Result<()> {
    let ifindex = get_ifindex(interface)?;
    let sock = netlink_open()?;

    // Release buffered packets first — destroying the qdisc would drop them.
    let opt = TcPlugQopt {
        action: TCQ_PLUG_RELEASE_INDEFINITE,
        limit: 0,
    };
    let msg = build_qdisc_msg(
        RTM_NEWQDISC,
        NLM_F_REQUEST | NLM_F_ACK,
        ifindex,
        Some(b"plug\0"),
        Some(unsafe { as_bytes(&opt) }),
    );
    netlink_request(&sock, &msg).context("release plug qdisc")?;

    // Delete the qdisc.
    let msg = build_qdisc_msg(
        RTM_DELQDISC,
        NLM_F_REQUEST | NLM_F_ACK,
        ifindex,
        Some(b"plug\0"),
        None,
    );
    netlink_request(&sock, &msg).context("delete plug qdisc")?;

    log::info!("plug qdisc removed from {}", interface);
    Ok(())
}
