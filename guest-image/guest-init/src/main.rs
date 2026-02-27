mod container;
mod io_session;
mod net;
mod vsock;

use std::ffi::CString;
use std::os::unix::io::RawFd;
use std::ptr;

use anyhow::{bail, Context};

use container::ContainerManager;
use distvirt_guest_protocol::{GuestMessage, HostMessage, VSOCK_CONTROL_PORT, STREAM_STDOUT, STREAM_STDERR};
use io_session::IoSessionManager;

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

/// Block SIGCHLD and return a signalfd that fires when children exit.
fn setup_signalfd() -> anyhow::Result<i32> {
    unsafe {
        let mut mask: libc::sigset_t = std::mem::zeroed();
        libc::sigemptyset(&mut mask);
        libc::sigaddset(&mut mask, libc::SIGCHLD);

        // Block SIGCHLD so it's delivered via signalfd instead of the default handler.
        if libc::sigprocmask(libc::SIG_BLOCK, &mask, ptr::null_mut()) != 0 {
            bail!("sigprocmask: {}", std::io::Error::last_os_error());
        }

        let fd = libc::signalfd(-1, &mask, libc::SFD_CLOEXEC | libc::SFD_NONBLOCK);
        if fd < 0 {
            bail!("signalfd: {}", std::io::Error::last_os_error());
        }
        Ok(fd)
    }
}

/// Drain all pending signals from the signalfd.
fn drain_signalfd(fd: i32) {
    let mut buf = [0u8; std::mem::size_of::<libc::signalfd_siginfo>()];
    loop {
        let n = unsafe { libc::read(fd, buf.as_mut_ptr() as *mut libc::c_void, buf.len()) };
        if n <= 0 {
            break;
        }
    }
}

fn handle_message(
    msg: HostMessage,
    stream: &mut vsock::VsockStream,
    containers: &mut ContainerManager,
) -> anyhow::Result<bool> {
    match msg {
        HostMessage::AddContainer {
            id,
            device,
            dns_servers,
        } => {
            log::info!("AddContainer: id={}, device={}", id, device);
            match containers.add(id.clone(), device, &dns_servers) {
                Ok(()) => {
                    stream.send(&GuestMessage::ContainerAdded { id })?;
                }
                Err(e) => {
                    log::error!("AddContainer failed: {:#}", e);
                    stream.send(&GuestMessage::Error {
                        message: format!("{:#}", e),
                    })?;
                }
            }
        }
        HostMessage::StartContainer { id, entrypoint, args, env, working_dir, uid, gid, hostname, capture_output } => {
            log::info!("StartContainer: id={}, entrypoint={}, capture_output={}", id, entrypoint, capture_output);
            match containers.start(&id, &entrypoint, &args, &env, working_dir.as_deref(), uid, gid, hostname.as_deref(), capture_output) {
                Ok(pid) => {
                    stream.send(&GuestMessage::ContainerStarted { id, pid })?;
                }
                Err(e) => {
                    log::error!("StartContainer failed: {:#}", e);
                    stream.send(&GuestMessage::Error {
                        message: format!("{:#}", e),
                    })?;
                }
            }
        }
        HostMessage::ConfigureNetwork { interface, ip, netmask, gateway } => {
            log::info!("ConfigureNetwork: {}={}, netmask={}, gw={}", interface, ip, netmask, gateway);
            match net::configure_network(&interface, &ip, &netmask, &gateway) {
                Ok(()) => {
                    stream.send(&GuestMessage::NetworkConfigured)?;
                }
                Err(e) => {
                    log::error!("ConfigureNetwork failed: {:#}", e);
                    stream.send(&GuestMessage::Error {
                        message: format!("{:#}", e),
                    })?;
                }
            }
        }
        HostMessage::Shutdown => {
            log::info!("shutdown requested");
            return Ok(true);
        }
    }
    Ok(false)
}

/// Read available data from a non-blocking pipe fd.
/// Returns the data read, or empty vec if EAGAIN/no data.
fn read_pipe(fd: RawFd) -> Vec<u8> {
    let mut buf = [0u8; 8192];
    let n = unsafe { libc::read(fd, buf.as_mut_ptr() as *mut libc::c_void, buf.len()) };
    if n > 0 {
        buf[..n as usize].to_vec()
    } else {
        Vec::new()
    }
}

fn run() -> anyhow::Result<()> {
    mount_essential_filesystems();

    let sigfd = setup_signalfd().context("setup signalfd")?;

    log::info!("starting vsock control listener on port {}", VSOCK_CONTROL_PORT);
    let listener = vsock::VsockListener::bind(VSOCK_CONTROL_PORT)
        .context("bind vsock control listener")?;

    let mut io_manager = IoSessionManager::new()
        .context("create I/O session manager")?;

    log::info!("waiting for host connection");
    let mut stream = listener.accept().context("accept vsock connection")?;

    log::info!("host connected, sending Ready");
    stream.send(&GuestMessage::Ready)?;

    let mut containers = ContainerManager::new();

    loop {
        // Build dynamic poll fd set.
        let mut poll_fds: Vec<libc::pollfd> = Vec::new();

        // [0] = control vsock fd
        poll_fds.push(libc::pollfd {
            fd: stream.as_raw_fd(),
            events: libc::POLLIN,
            revents: 0,
        });
        // [1] = signalfd
        poll_fds.push(libc::pollfd {
            fd: sigfd,
            events: libc::POLLIN,
            revents: 0,
        });
        // [2] = I/O listener fd
        poll_fds.push(libc::pollfd {
            fd: io_manager.listener_fd(),
            events: libc::POLLIN,
            revents: 0,
        });

        // Collect pipe fds from containers with captured output.
        let captured_ids = containers.captured_container_ids();
        let mut pipe_entries: Vec<(String, u8, RawFd)> = Vec::new(); // (container_id, stream_id, fd)
        for id in &captured_ids {
            if let Some(fd) = containers.stdout_fd(id) {
                poll_fds.push(libc::pollfd {
                    fd,
                    events: libc::POLLIN,
                    revents: 0,
                });
                pipe_entries.push((id.clone(), STREAM_STDOUT, fd));
            }
            if let Some(fd) = containers.stderr_fd(id) {
                poll_fds.push(libc::pollfd {
                    fd,
                    events: libc::POLLIN,
                    revents: 0,
                });
                pipe_entries.push((id.clone(), STREAM_STDERR, fd));
            }
        }

        // Collect session fds (to detect disconnects).
        let session_entries = io_manager.session_fds();
        let session_fds_start = poll_fds.len();
        for &(fd, _) in &session_entries {
            poll_fds.push(libc::pollfd {
                fd,
                events: libc::POLLIN,
                revents: 0,
            });
        }

        // If the BufReader has buffered data, don't block on poll.
        let timeout = if stream.has_buffered_data() { 0 } else { -1 };

        let ret = unsafe { libc::poll(poll_fds.as_mut_ptr(), poll_fds.len() as libc::nfds_t, timeout) };
        if ret < 0 {
            let err = std::io::Error::last_os_error();
            if err.raw_os_error() == Some(libc::EINTR) {
                continue;
            }
            bail!("poll: {}", err);
        }

        // Handle child exits (signalfd).
        if poll_fds[1].revents & libc::POLLIN != 0 {
            drain_signalfd(sigfd);
        }
        // Always try to reap, even without signalfd event (handles races).
        for exit in containers.reap_children() {
            // Drain remaining pipe data before sending EOF.
            if let Some(fd) = containers.stdout_fd(&exit.id) {
                loop {
                    let data = read_pipe(fd);
                    if data.is_empty() { break; }
                    io_manager.forward_pipe_data(&exit.id, STREAM_STDOUT, &data);
                }
            }
            if let Some(fd) = containers.stderr_fd(&exit.id) {
                loop {
                    let data = read_pipe(fd);
                    if data.is_empty() { break; }
                    io_manager.forward_pipe_data(&exit.id, STREAM_STDERR, &data);
                }
            }
            // Send EOF and clean up session.
            io_manager.container_exited(&exit.id);
            // Close pipe fds.
            containers.close_pipes(&exit.id);

            let _ = stream.send(&GuestMessage::ContainerExited {
                id: exit.id,
                code: exit.code,
            });
        }

        // Handle I/O listener (new session connections).
        if poll_fds[2].revents & libc::POLLIN != 0 {
            while io_manager.try_accept()? {}
        }

        // Handle pipe data (container stdout/stderr).
        let pipe_fds_start = 3;
        for (i, (container_id, stream_id, _fd)) in pipe_entries.iter().enumerate() {
            let poll_idx = pipe_fds_start + i;
            if poll_idx < poll_fds.len() && poll_fds[poll_idx].revents & libc::POLLIN != 0 {
                let data = read_pipe(poll_fds[poll_idx].fd);
                if !data.is_empty() {
                    io_manager.forward_pipe_data(container_id, *stream_id, &data);
                }
            }
        }

        // Handle session disconnects.
        let mut readable_session_fds = Vec::new();
        for (i, &(fd, _)) in session_entries.iter().enumerate() {
            let poll_idx = session_fds_start + i;
            if poll_idx < poll_fds.len() && poll_fds[poll_idx].revents & libc::POLLIN != 0 {
                readable_session_fds.push(fd);
            }
        }
        if !readable_session_fds.is_empty() {
            io_manager.check_disconnects(&readable_session_fds);
        }

        // Handle control vsock messages.
        if poll_fds[0].revents & libc::POLLIN != 0 || stream.has_buffered_data() {
            let msg: HostMessage = stream.recv().context("receive host message")?;
            log::info!("received: {:?}", msg);

            match handle_message(msg, &mut stream, &mut containers) {
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
    unsafe { libc::reboot(libc::RB_AUTOBOOT); }
    loop {
        unsafe { libc::pause(); }
    }
}
