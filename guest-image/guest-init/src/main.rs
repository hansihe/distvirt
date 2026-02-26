mod protocol;
mod vsock;

use std::ffi::CString;
use std::ptr;

use anyhow::{bail, Context};

use protocol::{GuestMessage, HostMessage};

const VSOCK_PORT: u32 = 1024;

fn mount(source: &str, target: &str, fstype: &str, flags: libc::c_ulong, data: Option<&str>) -> anyhow::Result<()> {
    let source_c = CString::new(source).unwrap();
    let target_c = CString::new(target).unwrap();
    let fstype_c = CString::new(fstype).unwrap();
    let data_c = data.map(|d| CString::new(d).unwrap());

    std::fs::create_dir_all(target)
        .with_context(|| format!("creating mount point {}", target))?;

    let ret = unsafe {
        libc::mount(
            source_c.as_ptr(),
            target_c.as_ptr(),
            fstype_c.as_ptr(),
            flags,
            data_c
                .as_ref()
                .map(|d| d.as_ptr() as *const libc::c_void)
                .unwrap_or(ptr::null()),
        )
    };
    if ret != 0 {
        bail!(
            "mount {} on {} ({}): {}",
            source,
            target,
            fstype,
            std::io::Error::last_os_error(),
        );
    }
    Ok(())
}

fn mount_essential_filesystems() {
    let mounts: &[(&str, &str, &str, libc::c_ulong, Option<&str>)] = &[
        ("proc", "/proc", "proc", libc::MS_NOSUID | libc::MS_NODEV | libc::MS_NOEXEC, None),
        ("sysfs", "/sys", "sysfs", libc::MS_NOSUID | libc::MS_NODEV | libc::MS_NOEXEC, None),
        ("tmpfs", "/tmp", "tmpfs", libc::MS_NOSUID | libc::MS_NODEV, None),
        ("devpts", "/dev/pts", "devpts", libc::MS_NOSUID | libc::MS_NOEXEC, Some("gid=5,mode=620")),
        ("tmpfs", "/dev/shm", "tmpfs", libc::MS_NOSUID | libc::MS_NODEV, None),
    ];

    for &(source, target, fstype, flags, data) in mounts {
        if let Err(err) = mount(source, target, fstype, flags, data) {
            log::warn!("{:#}", err);
        }
    }
}

fn handle_message(msg: HostMessage, stream: &mut vsock::VsockStream) -> anyhow::Result<bool> {
    match msg {
        HostMessage::AddContainer { id, device } => {
            log::info!("AddContainer: id={}, device={} (not yet implemented)", id, device);
            stream.send(&GuestMessage::Error {
                message: "AddContainer not yet implemented".into(),
            })?;
        }
        HostMessage::StartContainer { id, .. } => {
            log::info!("StartContainer: id={} (not yet implemented)", id);
            stream.send(&GuestMessage::Error {
                message: "StartContainer not yet implemented".into(),
            })?;
        }
        HostMessage::Shutdown => {
            log::info!("shutdown requested");
            return Ok(true);
        }
    }
    Ok(false)
}

fn run() -> anyhow::Result<()> {
    mount_essential_filesystems();

    log::info!("starting vsock listener on port {}", VSOCK_PORT);
    let listener = vsock::VsockListener::bind(VSOCK_PORT)
        .context("bind vsock listener")?;

    log::info!("waiting for host connection");
    let mut stream = listener.accept().context("accept vsock connection")?;

    log::info!("host connected, sending Ready");
    stream.send(&GuestMessage::Ready)?;

    loop {
        let msg: HostMessage = stream.recv().context("receive host message")?;
        log::info!("received: {:?}", msg);

        match handle_message(msg, &mut stream) {
            Ok(true) => break,
            Ok(false) => {}
            Err(e) => {
                log::error!("error handling message: {:#}", e);
                let _ = stream.send(&GuestMessage::Error {
                    message: format!("{:#}", e),
                });
            }
        }
    }

    Ok(())
}

fn main() {
    env_logger::Builder::new()
        .filter_level(log::LevelFilter::Info)
        .parse_default_env()
        .init();

    log::info!("guest-init started");

    if let Err(e) = run() {
        log::error!("fatal: {:#}", e);
    }

    log::info!("powering off");
    unsafe { libc::sync(); }
    unsafe { libc::reboot(libc::RB_POWER_OFF); }
    loop {
        unsafe { libc::pause(); }
    }
}
