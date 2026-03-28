use std::path::Path;

use distvirt_guest_protocol::{GuestMessage, HostMessage};

use crate::container::{ContainerBackend, ContainerManager};
use crate::spawner::LocalSpawner;
use crate::platform::Platform;
use crate::session::{self, CommandResult};

/// Read and execute config drive commands, if a config device path is provided.
///
/// The config device contains a 4-byte LE length prefix followed by JSON-encoded `Vec<HostMessage>`.
/// Returns the corresponding `GuestMessage` responses for each successfully executed command.
/// Errors are logged and treated as non-fatal — the guest will still boot and connect via vsock.
pub fn execute_pre_config<B: ContainerBackend, S: LocalSpawner>(
    config_device: Option<&Path>,
    containers: &mut ContainerManager<B>,
    platform: &impl Platform,
    spawner: &S,
) -> Vec<GuestMessage> {
    let mut responses = Vec::new();

    let device = match config_device {
        Some(dev) => dev,
        None => {
            log::info!("no config device configured, skipping config drive");
            return responses;
        }
    };

    let device_str = device.display().to_string();
    log::info!("reading config drive from {}", device_str);

    let commands = match read_config_device(&device_str) {
        Ok(cmds) => cmds,
        Err(e) => {
            log::warn!("failed to read config drive {}: {:#}", device_str, e);
            return responses;
        }
    };

    log::info!("config drive: {} command(s) to execute", commands.len());

    for cmd in commands {
        match session::execute_command(cmd, containers, platform, spawner) {
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
