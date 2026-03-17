use async_executor::LocalExecutor;
use distvirt_guest_protocol::{GuestMessage, HostMessage};

use crate::container::ContainerManager;
use crate::memory::init::read_cmdline_param;
use crate::session::{self, CommandResult};

/// Read and execute config drive commands, if a config device is specified on the kernel cmdline.
///
/// The config device contains a 4-byte LE length prefix followed by JSON-encoded `Vec<HostMessage>`.
/// Returns the corresponding `GuestMessage` responses for each successfully executed command.
/// Errors are logged and treated as non-fatal — the guest will still boot and connect via vsock.
pub fn execute_pre_config(
    containers: &mut ContainerManager,
    ex: &LocalExecutor<'_>,
) -> Vec<GuestMessage> {
    let mut responses = Vec::new();

    let device = match read_cmdline_param("distvirt.config_device") {
        Some(dev) => dev,
        None => {
            log::info!("no distvirt.config_device on cmdline, skipping config drive");
            return responses;
        }
    };

    log::info!("reading config drive from {}", device);

    let commands = match read_config_device(&device) {
        Ok(cmds) => cmds,
        Err(e) => {
            log::warn!("failed to read config drive {}: {:#}", device, e);
            return responses;
        }
    };

    log::info!("config drive: {} command(s) to execute", commands.len());

    for cmd in commands {
        match session::execute_command(cmd, containers, ex) {
            CommandResult::Response(msg) => {
                responses.push(msg);
            }
            CommandResult::PrepareSuspend | CommandResult::Shutdown => {
                log::warn!("config drive: skipping lifecycle command");
            }
        }
    }

    responses
}

/// Read length-prefixed JSON from a raw block device.
fn read_config_device(device: &str) -> anyhow::Result<Vec<HostMessage>> {
    use std::io::Read;

    let mut file =
        std::fs::File::open(device).map_err(|e| anyhow::anyhow!("open {}: {}", device, e))?;

    let mut len_buf = [0u8; 4];
    file.read_exact(&mut len_buf)
        .map_err(|e| anyhow::anyhow!("read length prefix: {}", e))?;

    let len = u32::from_le_bytes(len_buf) as usize;
    if len > 1024 * 1024 {
        anyhow::bail!("config drive payload too large: {} bytes", len);
    }

    let mut payload = vec![0u8; len];
    file.read_exact(&mut payload)
        .map_err(|e| anyhow::anyhow!("read payload ({} bytes): {}", len, e))?;

    let commands: Vec<HostMessage> = serde_json::from_slice(&payload)
        .map_err(|e| anyhow::anyhow!("deserialize config: {}", e))?;

    Ok(commands)
}
