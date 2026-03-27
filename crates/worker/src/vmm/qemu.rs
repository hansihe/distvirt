use std::path::PathBuf;
use std::process::{ExitStatus, Stdio};
use std::time::Duration;

use anyhow::{Context, bail};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::UnixStream;
use tokio::sync::watch;

use super::{
    BaseVmConfig, GuestDevice, MountRequest, MountRestoreInfo, PlannedMount, ProvidedAccess,
    ResolvedEntry, ResolvedMounts, VmBuilder, VmInstance, VmMountSource, Vmm,
    copy_file_writable, spawn_exit_monitor, spawn_serial_task, wait_for_file,
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

/// Deferred mount for the QEMU builder. QEMU only supports block devices.
struct DeferredQemuMount {
    tag: String,
    source_path: PathBuf,
    read_only: bool,
}

/// Deferred scratch device for the QEMU builder.
struct DeferredQemuScratch {
    tag: String,
    size_mib: u32,
}

/// Builder for configuring and launching a QEMU VM.
pub struct QemuBuilder {
    base: BaseVmConfig,
    qemu_bin: PathBuf,
    tmpdir: tempfile::TempDir,
    mounts: Vec<DeferredQemuMount>,
    scratches: Vec<DeferredQemuScratch>,
    mount_restore_info: Vec<MountRestoreInfo>,
}

impl VmBuilder for QemuBuilder {
    type Instance = QemuInstance;

    fn add_mount(&mut self, request: MountRequest) -> anyhow::Result<PlannedMount> {
        match request.source {
            VmMountSource::BlockImage { path, read_only } => {
                self.mounts.push(DeferredQemuMount {
                    tag: request.tag.clone(),
                    source_path: path,
                    read_only,
                });
                Ok(PlannedMount {
                    tag: request.tag,
                    provided: ProvidedAccess::BlockDevice { read_only },
                })
            }
            VmMountSource::Directory { .. } => {
                bail!("QEMU VMM does not support directory mounts (no virtiofs support)")
            }
            VmMountSource::ContainerdImage { .. } => {
                bail!("QEMU VMM does not support containerd images directly")
            }
        }
    }

    fn add_scratch_device(&mut self, tag: &str, size_mib: u32) -> anyhow::Result<()> {
        self.scratches.push(DeferredQemuScratch {
            tag: tag.to_string(),
            size_mib,
        });
        Ok(())
    }

    fn set_snapshot_context(&mut self, mount_restore_info: Vec<MountRestoreInfo>) {
        self.mount_restore_info = mount_restore_info;
    }

    async fn launch(self) -> anyhow::Result<(QemuInstance, ResolvedMounts)> {
        let base = self.base;
        let tmpdir = self.tmpdir;

        // Create scratch devices (e.g., overlay).
        // These get sequential block device indices starting at /dev/vdb.
        let mut resolved = Vec::new();
        let mut drive_index: u32 = 1; // vda=rootfs, vdb onwards
        let mut drive_args: Vec<String> = Vec::new();

        for scratch in &self.scratches {
            let filename = format!("scratch-{}.ext4", scratch.tag);
            let path = tmpdir.path().join(&filename);
            crate::volume::create_overlay_image(&path, scratch.size_mib as u64)
                .await
                .context("create scratch device")?;
            let device = format!("/dev/vd{}", (b'a' + drive_index as u8) as char);
            drive_args.push(format!(
                "file=./{},format=raw,if=virtio,index={}",
                filename, drive_index
            ));
            resolved.push(ResolvedEntry {
                tag: scratch.tag.clone(),
                guest: GuestDevice::Device { path: device },
            });
            drive_index += 1;
        }

        // Process block mounts.
        for mount in &self.mounts {
            let filename = mount
                .source_path
                .file_name()
                .context("block image has no filename")?
                .to_str()
                .context("block image filename not valid UTF-8")?
                .to_string();
            copy_file_writable(&mount.source_path, &tmpdir.path().join(&filename)).await?;
            let ro = if mount.read_only {
                ",readonly=on"
            } else {
                ""
            };
            let device = format!("/dev/vd{}", (b'a' + drive_index as u8) as char);
            drive_args.push(format!(
                "file=./{},format=raw,if=virtio,index={}{}",
                filename, drive_index, ro
            ));
            resolved.push(ResolvedEntry {
                tag: mount.tag.clone(),
                guest: GuestDevice::Device { path: device },
            });
            drive_index += 1;
        }

        let qmp_socket_path = tmpdir.path().join("qmp.sock");
        let transport_socket_path = tmpdir.path().join("transport.sock");

        // Build QEMU command line.
        let mut cmd = tokio::process::Command::new(&self.qemu_bin);
        cmd.current_dir(tmpdir.path());

        // TCG mode — software emulation, no KVM.
        cmd.args(["-machine", "q35,accel=tcg"]);
        cmd.args(["-cpu", "max"]);
        cmd.args(["-m", &format!("{}M", base.mem_size_mib)]);
        cmd.args(["-smp", &base.vcpu_count.to_string()]);
        cmd.args(["-display", "none"]);
        cmd.args(["-no-reboot"]);

        // QMP control socket.
        cmd.args([
            "-qmp",
            &format!("unix:{},server,wait=off", qmp_socket_path.display()),
        ]);

        // Kernel direct boot.
        let console_dev = if cfg!(target_arch = "aarch64") {
            "ttyAMA0"
        } else {
            "ttyS0"
        };
        let boot_args = {
            let mut args = format!(
                "console={console_dev} reboot=k panic=-1 root=/dev/vda init=/sbin/init distvirt.transport=virtio-serial"
            );
            if let Some(ref balloon) = base.balloon {
                args.push_str(&format!(" distvirt.balloon_mib={}", balloon.amount_mib));
            }
            args
        };

        cmd.args([
            "-kernel",
            base.kernel_path
                .to_str()
                .ok_or_else(|| anyhow::anyhow!("kernel_path not valid UTF-8"))?,
        ]);
        cmd.args(["-append", &boot_args]);

        // Block devices as virtio-blk.
        // index=0 → vda (rootfs), then scratch + mount drives.
        cmd.args([
            "-drive",
            "file=./rootfs.ext4,format=raw,if=virtio,index=0",
        ]);
        for drive_arg in &drive_args {
            cmd.args(["-drive", drive_arg]);
        }

        // Virtio-serial transport.
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

        // Serial console.
        if base.serial_console {
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
        let serial_stdout = if base.serial_console {
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

        log::info!("QEMU launched (TCG, pid={})", child.id().unwrap_or(0));

        let instance = QemuInstance {
            child,
            _qmp: qmp,
            transport_socket_path,
            _serial_task,
            exit_rx,
            _exit_monitor,
            _tmpdir: tmpdir,
        };

        Ok((instance, ResolvedMounts { entries: resolved }))
    }
}

impl Vmm for Qemu {
    type Builder = QemuBuilder;
    type Instance = QemuInstance;

    fn builder(&self, base: BaseVmConfig) -> anyhow::Result<QemuBuilder> {
        let tmpdir = tempfile::tempdir().context("create tmpdir")?;

        // Copy rootfs image into tmpdir (writable copy).
        // This is synchronous file copy — block_on is fine here since
        // builder() is called from an async context and we need the file ready.
        let rootfs_dest = tmpdir.path().join("rootfs.ext4");
        std::fs::copy(&base.rootfs_image_path, &rootfs_dest)
            .context("copy rootfs image to tmpdir")?;
        // Make writable.
        let mut perms = std::fs::metadata(&rootfs_dest)?.permissions();
        perms.set_readonly(false);
        std::fs::set_permissions(&rootfs_dest, perms)?;

        Ok(QemuBuilder {
            base,
            qemu_bin: self.qemu_bin.clone(),
            tmpdir,
            mounts: Vec::new(),
            scratches: Vec::new(),
            mount_restore_info: Vec::new(),
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
    use crate::vmm::Vmm;
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

        let base = BaseVmConfig {
            kernel_path: kernel,
            rootfs_image_path: rootfs,
            vcpu_count: 1,
            mem_size_mib: 128,
            net: None,
            serial_console: true,
            balloon: None,
        };

        let mut builder = qemu.builder(base).expect("builder creation failed");

        // Add a scratch device for overlay (replaces the old container_image block).
        builder
            .add_scratch_device("overlay", 256)
            .expect("add_scratch_device failed");

        let (mut instance, _resolved) = builder.launch().await.expect("QEMU launch failed");

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
                pre_config_responses: _,
            } => {
                eprintln!("guest Ready: running={:?}", running_containers);
                assert!(
                    running_containers.is_empty(),
                    "expected no running containers on fresh boot"
                );
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
