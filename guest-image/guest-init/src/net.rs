use std::ffi::CString;
use std::net::Ipv4Addr;

use anyhow::{bail, Context};

/// Configure a network interface with IP, netmask, bring it up, and add a default route.
pub fn configure_network(
    interface: &str,
    ip: &str,
    netmask: &str,
    gateway: &str,
) -> anyhow::Result<()> {
    let sock = unsafe { libc::socket(libc::AF_INET, libc::SOCK_DGRAM, 0) };
    if sock < 0 {
        bail!("socket: {}", std::io::Error::last_os_error());
    }

    let result = configure_network_inner(sock, interface, ip, netmask, gateway);

    unsafe { libc::close(sock) };
    result
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
