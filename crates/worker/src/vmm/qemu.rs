use std::path::PathBuf;
use std::process::{ExitStatus, Stdio};
use std::time::Duration;

use anyhow::{Context, bail};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::UnixStream;
use tokio::sync::watch;

use super::{
    VmConfig, VmInstance, Vmm, copy_file_writable, spawn_exit_monitor, spawn_serial_task,
    wait_for_file,
};
use crate::fabric::FabricPort;
use crate::task_handle::TaskHandle;

/// QEMU VMM implementation.
///
/// Uses QEMU in TCG (software emulation) mode — no KVM and no root required.
/// Host↔guest communication uses virtio-serial exposed as a unix socket,
/// so no kernel modules are needed beyond what QEMU itself provides.
pub struct Qemu {
    pub qemu_bin: PathBuf,
}

impl Qemu {
    pub fn new(qemu_bin: impl Into<PathBuf>) -> Self {
        Qemu {
            qemu_bin: qemu_bin.into(),
        }
    }
}

impl Vmm for Qemu {
    type Instance = QemuInstance;

    async fn launch(&self, config: &VmConfig) -> anyhow::Result<QemuInstance> {
        let tmpdir = tempfile::tempdir().context("create tmpdir")?;

        // Copy disk images into tmpdir (writable copies).
        copy_file_writable(
            &config.rootfs_image_path,
            &tmpdir.path().join("rootfs.ext4"),
        )
        .await?;
        copy_file_writable(
            &config.container_image_path,
            &tmpdir.path().join("container.ext4"),
        )
        .await?;

        for drive in &config.additional_drives {
            let filename = drive
                .image_path
                .file_name()
                .context("additional drive has no filename")?
                .to_str()
                .context("additional drive filename not valid UTF-8")?;
            copy_file_writable(&drive.image_path, &tmpdir.path().join(filename)).await?;
        }

        let qmp_socket_path = tmpdir.path().join("qmp.sock");
        let transport_socket_path = tmpdir.path().join("transport.sock");

        // Build QEMU command line.
        let mut cmd = tokio::process::Command::new(&self.qemu_bin);
        cmd.current_dir(tmpdir.path());

        // TCG mode — software emulation, no KVM.
        cmd.args(["-machine", "q35,accel=tcg"]);
        cmd.args(["-cpu", "max"]);
        cmd.args(["-m", &format!("{}M", config.mem_size_mib)]);
        cmd.args(["-smp", &config.vcpu_count.to_string()]);
        cmd.args(["-display", "none"]);
        cmd.args(["-no-reboot"]);

        // QMP control socket.
        cmd.args([
            "-qmp",
            &format!("unix:{},server,wait=off", qmp_socket_path.display()),
        ]);

        // Kernel direct boot.
        // root=/dev/vda is needed because QEMU doesn't have a separate API to
        // designate the root device (unlike Firecracker).
        let mut boot_args =
            "console=ttyS0 reboot=k panic=-1 root=/dev/vda init=/sbin/init distvirt.transport=virtio-serial"
                .to_string();
        if let Some(ref balloon) = config.balloon {
            boot_args.push_str(&format!(" distvirt.balloon_mib={}", balloon.amount_mib));
        }

        // Config drive.
        if !config.initial_commands.is_empty() {
            let config_img_path = tmpdir.path().join("config.img");
            let json_payload = serde_json::to_vec(&config.initial_commands)
                .context("serialize initial_commands")?;
            let mut img_data = Vec::with_capacity(4 + json_payload.len());
            img_data.extend_from_slice(&(json_payload.len() as u32).to_le_bytes());
            img_data.extend_from_slice(&json_payload);
            tokio::fs::write(&config_img_path, &img_data)
                .await
                .context("write config.img")?;
            // TODO: verify device naming under QEMU virtio-blk matches Firecracker
            boot_args.push_str(" distvirt.config_device=/dev/vdc");
        }

        cmd.args([
            "-kernel",
            config
                .kernel_path
                .to_str()
                .ok_or_else(|| anyhow::anyhow!("kernel_path not valid UTF-8"))?,
        ]);
        cmd.args(["-append", &boot_args]);

        // Block devices as virtio-blk.
        // index=0 → vda (rootfs), index=1 → vdb (container), etc.
        cmd.args([
            "-drive",
            "file=./rootfs.ext4,format=raw,if=virtio,index=0",
        ]);
        cmd.args([
            "-drive",
            "file=./container.ext4,format=raw,if=virtio,index=1",
        ]);

        if !config.initial_commands.is_empty() {
            cmd.args([
                "-drive",
                "file=./config.img,format=raw,if=virtio,index=2,readonly=on",
            ]);
        }

        let mut drive_index: u32 = if config.initial_commands.is_empty() {
            2
        } else {
            3
        };
        for drive in &config.additional_drives {
            let filename = drive
                .image_path
                .file_name()
                .unwrap()
                .to_str()
                .unwrap();
            let ro = if drive.read_only { ",readonly=on" } else { "" };
            cmd.args([
                "-drive",
                &format!(
                    "file=./{},format=raw,if=virtio,index={}{}",
                    filename, drive_index, ro
                ),
            ]);
            drive_index += 1;
        }

        // Virtio-serial transport: exposes a unix socket on the host and a
        // /dev/virtio-ports/transport device in the guest.
        cmd.args(["-device", "virtio-serial-pci"]);
        cmd.args([
            "-chardev",
            &format!(
                "socket,id=transport,path={},server=on,wait=off",
                transport_socket_path.display()
            ),
        ]);
        cmd.args([
            "-device",
            "virtserialport,chardev=transport,name=transport",
        ]);

        // Serial console — QEMU needs `-serial stdio` to pipe serial
        // output through stdout (unlike Firecracker which does this by default).
        if config.serial_console {
            cmd.args(["-serial", "stdio"]);
            cmd.stdout(Stdio::piped());
        } else {
            cmd.args(["-serial", "none"]);
            cmd.stdout(Stdio::null());
        }
        cmd.stderr(Stdio::null());

        // No networking for now.
        cmd.args(["-nic", "none"]);

        let mut child = cmd.spawn().context("spawn qemu")?;
        let serial_stdout = if config.serial_console {
            child.stdout.take()
        } else {
            None
        };

        // Wait for QMP socket and complete handshake.
        wait_for_file(&qmp_socket_path, Duration::from_secs(10))
            .await
            .context("waiting for QMP socket")?;
        let qmp = QmpConnection::connect(&qmp_socket_path)
            .await
            .context("QMP handshake")?;

        let (exit_rx, _exit_monitor) = spawn_exit_monitor(&child);
        let _serial_task = serial_stdout.map(spawn_serial_task);

        log::info!(
            "QEMU launched (TCG, pid={})",
            child.id().unwrap_or(0)
        );

        Ok(QemuInstance {
            child,
            _qmp: qmp,
            transport_socket_path,
            _serial_task,
            exit_rx,
            _exit_monitor,
            _tmpdir: tmpdir,
        })
    }
}

pub struct QemuInstance {
    child: tokio::process::Child,
    _qmp: QmpConnection,
    transport_socket_path: PathBuf,
    _serial_task: Option<TaskHandle<()>>,
    exit_rx: watch::Receiver<Option<ExitStatus>>,
    _exit_monitor: TaskHandle<()>,
    /// Tmpdir holding disk images and sockets. Dropped after child is killed
    /// (field order matters — child is declared first).
    _tmpdir: tempfile::TempDir,
}

impl Drop for QemuInstance {
    fn drop(&mut self) {
        let _ = self.child.start_kill();
    }
}

impl VmInstance for QemuInstance {
    async fn connect_vsock(&self, _port: u32) -> anyhow::Result<UnixStream> {
        let sock_path = &self.transport_socket_path;
        log::info!(
            "connecting to guest via virtio-serial at {}",
            sock_path.display()
        );

        // Retry loop — the QEMU chardev socket exists immediately, but the
        // guest needs time to boot and open the virtio-serial port. If the
        // guest port isn't open yet, QEMU accepts the connection but closes
        // it immediately (EOF). We detect this with a non-blocking read:
        // EOF means the guest isn't ready, WouldBlock means it is.
        let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
        loop {
            match UnixStream::connect(sock_path).await {
                Ok(stream) => {
                    // If the guest port isn't open, QEMU closes the socket
                    // almost immediately. If the port IS open, the socket
                    // stays idle (guest waits for us to send first).
                    // So: readable() returning quickly → likely EOF (bad).
                    //     readable() timing out → connection alive (good).
                    match tokio::time::timeout(
                        Duration::from_millis(100),
                        stream.readable(),
                    )
                    .await
                    {
                        // Timeout — socket stayed open with no EOF. Good.
                        Err(_) => {
                            log::info!("virtio-serial transport connected");
                            return Ok(stream);
                        }
                        // Became readable quickly — check if it's EOF.
                        Ok(Ok(())) => {
                            let mut probe = [0u8; 1];
                            match stream.try_read(&mut probe) {
                                Ok(0) => {
                                    log::debug!("virtio-serial: got EOF, guest not ready yet");
                                }
                                Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                                    log::info!("virtio-serial transport connected");
                                    return Ok(stream);
                                }
                                Ok(_) => {
                                    log::debug!("virtio-serial: unexpected data before handshake");
                                }
                                Err(e) => {
                                    log::debug!("virtio-serial: probe error: {}, retrying", e);
                                }
                            }
                        }
                        Ok(Err(e)) => {
                            log::debug!("virtio-serial: readable error: {}, retrying", e);
                        }
                    }
                }
                Err(e) => {
                    log::debug!("virtio-serial: connect error: {}, retrying", e);
                }
            }

            if tokio::time::Instant::now() >= deadline {
                bail!(
                    "timeout connecting to guest via virtio-serial at {}",
                    sock_path.display()
                );
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    fn take_fabric_port(&mut self) -> Option<FabricPort> {
        None // No fabric support yet.
    }

    async fn wait(&mut self) -> anyhow::Result<ExitStatus> {
        self.exit_rx
            .wait_for(|s| s.is_some())
            .await
            .map_err(|_| anyhow::anyhow!("exit monitor task dropped"))?;
        let status = self.child.wait().await.context("wait for qemu")?;
        Ok(status)
    }

    async fn kill(&mut self) -> anyhow::Result<()> {
        self.child.kill().await.context("kill qemu")?;
        Ok(())
    }

    fn take_exit_signal(&mut self) -> Option<watch::Receiver<Option<ExitStatus>>> {
        Some(self.exit_rx.clone())
    }
}

// ---------------------------------------------------------------------------
// QMP (QEMU Machine Protocol) connection
// ---------------------------------------------------------------------------

/// A connection to QEMU's QMP control socket.
///
/// QMP uses newline-delimited JSON over a unix socket. After connecting,
/// the server sends a greeting and waits for `qmp_capabilities` before
/// accepting commands.
struct QmpConnection {
    stream: BufReader<UnixStream>,
}

impl QmpConnection {
    async fn connect(socket_path: &std::path::Path) -> anyhow::Result<Self> {
        let stream = UnixStream::connect(socket_path)
            .await
            .context("connect to QMP socket")?;
        let mut reader = BufReader::new(stream);

        // Read the QMP greeting.
        let mut line = String::new();
        reader
            .read_line(&mut line)
            .await
            .context("read QMP greeting")?;
        if !line.contains("QMP") {
            bail!("unexpected QMP greeting: {}", line.trim());
        }

        // Enter command mode.
        let caps_cmd = b"{\"execute\":\"qmp_capabilities\"}\n";
        reader.get_mut().write_all(caps_cmd).await?;
        reader.get_mut().flush().await?;

        // Read response (skip any events).
        loop {
            let mut resp = String::new();
            reader
                .read_line(&mut resp)
                .await
                .context("read qmp_capabilities response")?;
            let parsed: serde_json::Value =
                serde_json::from_str(&resp).context("parse QMP response")?;
            if parsed.get("event").is_some() {
                continue;
            }
            if parsed.get("return").is_some() {
                break;
            }
            bail!("qmp_capabilities failed: {}", resp.trim());
        }

        Ok(QmpConnection { stream: reader })
    }

    /// Send a QMP command and wait for the result.
    #[allow(dead_code)] // Will be used for snapshot/balloon support.
    async fn execute(
        &mut self,
        command: &str,
        arguments: Option<serde_json::Value>,
    ) -> anyhow::Result<serde_json::Value> {
        let mut cmd = serde_json::json!({"execute": command});
        if let Some(args) = arguments {
            cmd["arguments"] = args;
        }
        let mut cmd_bytes = serde_json::to_vec(&cmd)?;
        cmd_bytes.push(b'\n');
        self.stream.get_mut().write_all(&cmd_bytes).await?;
        self.stream.get_mut().flush().await?;

        // Read lines until we get a return or error (skip async events).
        loop {
            let mut line = String::new();
            self.stream
                .read_line(&mut line)
                .await
                .with_context(|| format!("read QMP response for '{}'", command))?;
            let parsed: serde_json::Value = serde_json::from_str(&line)
                .with_context(|| format!("parse QMP response: {}", line.trim()))?;

            if parsed.get("event").is_some() {
                continue;
            }
            if let Some(err) = parsed.get("error") {
                bail!("QMP command '{}' failed: {}", command, err);
            }
            if let Some(ret) = parsed.get("return") {
                return Ok(ret.clone());
            }
            bail!(
                "unexpected QMP response for '{}': {}",
                command,
                line.trim()
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::vmm::{VmConfig, Vmm};
    use distvirt_guest_protocol::{GuestMessage, HostMessage, VSOCK_CONTROL_PORT};

    /// Returns true if we have the QEMU binary and guest images available.
    fn should_run() -> bool {
        if std::env::var("DISTVIRT_QEMU").is_err() {
            eprintln!("DISTVIRT_QEMU not set, skipping QEMU test");
            return false;
        }
        true
    }

    fn qemu_bin() -> PathBuf {
        std::env::var("QEMU_BIN")
            .unwrap_or_else(|_| "qemu-system-x86_64".into())
            .into()
    }

    fn guest_paths() -> (PathBuf, PathBuf) {
        let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        let kernel = manifest_dir.join("../guest-image/result-kernel/bzImage");
        let rootfs = manifest_dir.join("../guest-image/result-rootfs");
        assert!(kernel.exists(), "kernel not found at {}", kernel.display());
        assert!(rootfs.exists(), "rootfs not found at {}", rootfs.display());
        (kernel, rootfs)
    }

    /// Create a minimal empty ext4 container image.
    async fn create_empty_container_image(path: &std::path::Path) -> anyhow::Result<()> {
        // Create a 4MB sparse file and format as ext4.
        let f = std::fs::File::create(path)?;
        f.set_len(4 * 1024 * 1024)?;
        drop(f);
        let status = tokio::process::Command::new("mkfs.ext4")
            .args(["-q", "-F"])
            .arg(path)
            .status()
            .await?;
        anyhow::ensure!(status.success(), "mkfs.ext4 failed");
        Ok(())
    }

    /// Basic smoke test: launch a QEMU VM, connect via virtio-serial,
    /// complete the yamux handshake, send Shutdown, and wait for exit.
    #[tokio::test]
    async fn test_qemu_launch_and_handshake() {
        if !should_run() {
            return;
        }
        let _ = env_logger::try_init();

        let (kernel, rootfs) = guest_paths();
        let qemu = Qemu::new(qemu_bin());

        // Create a temporary empty container image.
        let tmpdir = tempfile::tempdir().unwrap();
        let container_img = tmpdir.path().join("container.ext4");
        create_empty_container_image(&container_img).await.unwrap();

        let config = VmConfig {
            kernel_path: kernel,
            rootfs_image_path: rootfs,
            container_image_path: container_img,
            vcpu_count: 1,
            mem_size_mib: 128,
            net: None,
            serial_console: true,
            balloon: None,
            initial_commands: vec![],
            additional_drives: vec![],
        };

        let mut instance = qemu.launch(&config).await.expect("QEMU launch failed");

        // Connect to guest-init via virtio-serial transport.
        let socket = instance
            .connect_vsock(VSOCK_CONTROL_PORT)
            .await
            .expect("connect_vsock failed");

        // Set up yamux session (host is client).
        let (mut session, _yamux_driver, _exit_signal) =
            crate::vsock_client::GuestSession::new(socket)
                .await
                .expect("GuestSession setup failed");

        // Receive Ready from guest-init.
        let ready: GuestMessage = session.recv().await.expect("recv Ready failed");
        match ready {
            GuestMessage::Ready {
                running_containers,
                pre_config_responses,
            } => {
                eprintln!(
                    "guest Ready: running={:?}, pre_config={:?}",
                    running_containers, pre_config_responses
                );
                assert!(running_containers.is_empty(), "expected no running containers on fresh boot");
            }
            other => panic!("expected GuestMessage::Ready, got {:?}", other),
        }

        // Accept the event stream (guest opens it before sending Ready).
        session
            .accept_event_stream()
            .await
            .expect("accept_event_stream failed");

        // Send Shutdown and wait for VM to exit.
        session
            .send(&HostMessage::Shutdown)
            .await
            .expect("send Shutdown failed");

        let status = tokio::time::timeout(Duration::from_secs(30), instance.wait())
            .await
            .expect("VM did not exit within 30s")
            .expect("wait failed");

        eprintln!("QEMU exited with status: {:?}", status);
    }
}
