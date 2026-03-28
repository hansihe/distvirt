use std::ffi::CString;
use std::os::unix::io::{AsRawFd, FromRawFd, OwnedFd};

use anyhow::{bail, Context};

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
        bail!(
            "if_nametoindex({}): {}",
            interface,
            std::io::Error::last_os_error()
        );
    }
    Ok(idx)
}

fn netlink_open() -> anyhow::Result<OwnedFd> {
    let fd = unsafe {
        libc::socket(
            libc::AF_NETLINK,
            libc::SOCK_DGRAM | libc::SOCK_CLOEXEC,
            libc::NETLINK_ROUTE,
        )
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

const DEFAULT_INTERFACE: &str = "eth0";

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
