// TODO: artifact_transfer uses tokio::fs and spawn_blocking directly.
// Once sim tests exercise artifact transfers, these should go through the Fs trait too.
//! Artifact transfer: stream artifacts between pools (local or cross-worker via TCP).
//!
//! Wire format for TCP transfers:
//! ```text
//! [TransferHeader (24 bytes)]
//! [source_artifact_id bytes]
//! [source_pool_id bytes]
//! [dest_artifact_id bytes]
//! [dest_pool_id bytes]
//! [tar stream of artifact directory contents]
//! ```

use super::supervisor::dir_size;
use std::collections::HashMap;
use std::io;
use std::path::{Path, PathBuf};

use distvirt_worker_protocol::{ArtifactId, PoolId, WorkerEvent};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;
use zerocopy::{FromBytes, IntoBytes, KnownLayout, Immutable, Unaligned, network_endian::{U16, U64}};

use super::supervisor::send_event;

/// Magic bytes identifying a distvirt artifact transfer stream.
const TRANSFER_MAGIC: [u8; 4] = *b"DVXF";

/// Current transfer protocol version.
const TRANSFER_VERSION: u8 = 1;

/// Fixed-size header at the start of a TCP transfer stream.
#[derive(FromBytes, IntoBytes, KnownLayout, Immutable, Unaligned, Clone, Copy)]
#[repr(C)]
pub struct TransferHeader {
    pub magic: [u8; 4],
    pub version: u8,
    pub _reserved: u8,
    pub transfer_id: U64,
    pub source_artifact_id_len: U16,
    pub source_pool_id_len: U16,
    pub dest_artifact_id_len: U16,
    pub dest_pool_id_len: U16,
}

/// Bind a TCP listener for incoming artifact transfers.
pub async fn start_transfer_listener(bind_addr: &str) -> io::Result<(TcpListener, u16)> {
    let listener = TcpListener::bind(bind_addr).await?;
    let port = listener.local_addr()?.port();
    Ok((listener, port))
}

/// Accept loop: runs until the listener errors or is dropped.
/// Spawns `handle_incoming_transfer` per connection.
pub async fn transfer_accept_loop(
    listener: TcpListener,
    pools: HashMap<PoolId, PathBuf>,
    bg_event_tx: mpsc::Sender<WorkerEvent>,
) {
    loop {
        match listener.accept().await {
            Ok((stream, addr)) => {
                log::info!("artifact transfer: accepted connection from {}", addr);
                let pools = pools.clone();
                let tx = bg_event_tx.clone();
                tokio::spawn(async move {
                    if let Err(e) = handle_incoming_transfer(stream, &pools, &tx).await {
                        log::error!("artifact transfer: incoming transfer failed: {:#}", e);
                    }
                });
            }
            Err(e) => {
                log::error!("artifact transfer: accept error: {}, stopping listener", e);
                break;
            }
        }
    }
}

/// Destination side: read header, unpack tar stream into pool.
async fn handle_incoming_transfer(
    mut stream: TcpStream,
    pools: &HashMap<PoolId, PathBuf>,
    bg_event_tx: &mpsc::Sender<WorkerEvent>,
) -> anyhow::Result<()> {
    // Read fixed header.
    let mut hdr_buf = [0u8; size_of::<TransferHeader>()];
    stream.read_exact(&mut hdr_buf).await?;
    let hdr = TransferHeader::read_from_bytes(&hdr_buf)
        .map_err(|_| anyhow::anyhow!("invalid transfer header"))?;

    if hdr.magic != TRANSFER_MAGIC {
        anyhow::bail!("invalid transfer magic: {:?}", hdr.magic);
    }
    if hdr.version != TRANSFER_VERSION {
        anyhow::bail!("unsupported transfer version: {}", hdr.version);
    }

    let transfer_id = hdr.transfer_id.get();
    let total_str_len = hdr.source_artifact_id_len.get() as usize
        + hdr.source_pool_id_len.get() as usize
        + hdr.dest_artifact_id_len.get() as usize
        + hdr.dest_pool_id_len.get() as usize;

    // Read variable-length string data.
    let mut str_buf = vec![0u8; total_str_len];
    stream.read_exact(&mut str_buf).await?;

    let mut offset = 0;
    let source_artifact_id = ArtifactId::from(
        std::str::from_utf8(&str_buf[offset..offset + hdr.source_artifact_id_len.get() as usize])?,
    );
    offset += hdr.source_artifact_id_len.get() as usize;
    let source_pool_id = PoolId::from(
        std::str::from_utf8(&str_buf[offset..offset + hdr.source_pool_id_len.get() as usize])?,
    );
    offset += hdr.source_pool_id_len.get() as usize;
    let dest_artifact_id = ArtifactId::from(
        std::str::from_utf8(&str_buf[offset..offset + hdr.dest_artifact_id_len.get() as usize])?,
    );
    offset += hdr.dest_artifact_id_len.get() as usize;
    let dest_pool_id = PoolId::from(
        std::str::from_utf8(&str_buf[offset..offset + hdr.dest_pool_id_len.get() as usize])?,
    );

    log::info!(
        "artifact transfer: receiving transfer_id={} {}:{} -> {}:{}",
        transfer_id,
        source_pool_id,
        source_artifact_id,
        dest_pool_id,
        dest_artifact_id,
    );

    // Resolve dest pool.
    let pool_path = pools
        .get(&dest_pool_id)
        .ok_or_else(|| anyhow::anyhow!("unknown dest pool '{}'", dest_pool_id))?
        .clone();

    let dest_dir = pool_path.join(dest_artifact_id.as_ref());
    let temp_dir = pool_path.join(format!(".{}.partial", dest_artifact_id.as_ref()));

    // Create temp dir.
    tokio::fs::create_dir_all(&temp_dir).await?;

    // Unpack tar from remaining TCP stream using spawn_blocking + SyncIoBridge.
    let temp_dir_clone = temp_dir.clone();
    let unpack_result = {
        let stream = tokio_util::io::SyncIoBridge::new(stream);
        tokio::task::spawn_blocking(move || -> anyhow::Result<()> {
            let mut archive = tar::Archive::new(stream);
            archive.unpack(&temp_dir_clone)?;
            Ok(())
        })
        .await?
    };

    if let Err(e) = unpack_result {
        // Clean up temp dir on error.
        let _ = tokio::fs::remove_dir_all(&temp_dir).await;
        anyhow::bail!("tar unpack failed: {:#}", e);
    }

    // Atomic rename.
    tokio::fs::rename(&temp_dir, &dest_dir).await?;

    // Calculate size.
    let size_bytes = match dir_size(&dest_dir).await {
        Ok(size) => size,
        Err(e) => {
            log::warn!("artifact transfer: failed to calculate size for {}: {:#}", dest_artifact_id, e);
            0
        }
    };

    log::info!(
        "artifact transfer: received transfer_id={} dest={}:{} size={}",
        transfer_id,
        dest_pool_id,
        dest_artifact_id,
        size_bytes,
    );

    // Emit ArtifactTransferReceived event.
    send_event(
        bg_event_tx,
        WorkerEvent::ArtifactTransferReceived {
            transfer_id,
            source_artifact_id,
            source_pool_id,
            dest_artifact_id,
            dest_pool_id,
            size_bytes,
        },
    )
    .await;

    Ok(())
}

/// Source side: connect to dest, stream artifact as tar.
pub async fn send_artifact(
    dest_endpoint: &str,
    transfer_id: u64,
    source_artifact_id: &ArtifactId,
    source_pool_id: &PoolId,
    dest_artifact_id: &ArtifactId,
    dest_pool_id: &PoolId,
    source_dir: &Path,
) -> anyhow::Result<()> {
    let mut stream = TcpStream::connect(dest_endpoint).await?;

    // Build and write header.
    let hdr = TransferHeader {
        magic: TRANSFER_MAGIC,
        version: TRANSFER_VERSION,
        _reserved: 0,
        transfer_id: U64::new(transfer_id),
        source_artifact_id_len: U16::new(source_artifact_id.as_ref().len() as u16),
        source_pool_id_len: U16::new(source_pool_id.as_ref().len() as u16),
        dest_artifact_id_len: U16::new(dest_artifact_id.as_ref().len() as u16),
        dest_pool_id_len: U16::new(dest_pool_id.as_ref().len() as u16),
    };

    stream.write_all(hdr.as_bytes()).await?;

    // Write variable-length strings.
    stream.write_all(source_artifact_id.as_ref().as_bytes()).await?;
    stream.write_all(source_pool_id.as_ref().as_bytes()).await?;
    stream.write_all(dest_artifact_id.as_ref().as_bytes()).await?;
    stream.write_all(dest_pool_id.as_ref().as_bytes()).await?;

    // Stream tar archive using spawn_blocking + SyncIoBridge.
    let source_dir = source_dir.to_path_buf();
    let bridge = tokio_util::io::SyncIoBridge::new(stream);
    let bridge = tokio::task::spawn_blocking(move || -> anyhow::Result<tokio_util::io::SyncIoBridge<TcpStream>> {
        let mut builder = tar::Builder::new(bridge);
        builder.append_dir_all(".", &source_dir)?;
        let bridge = builder.into_inner()?;
        Ok(bridge)
    })
    .await??;

    // Recover the TcpStream and shut down the write half.
    let mut stream = bridge.into_inner();
    stream.shutdown().await?;

    Ok(())
}

/// Local pool-to-pool copy (no network).
/// Creates a temp dir, copies all files, renames to final location.
/// Returns total bytes copied.
pub async fn local_pool_copy(source_dir: &Path, dest_dir: &Path) -> anyhow::Result<u64> {
    let temp_dir = dest_dir.with_file_name(format!(
        ".{}.partial",
        dest_dir.file_name().unwrap_or_default().to_string_lossy()
    ));

    tokio::fs::create_dir_all(&temp_dir).await?;

    let mut total = 0u64;
    let mut stack = vec![(source_dir.to_path_buf(), temp_dir.clone())];
    while let Some((src, dst)) = stack.pop() {
        let mut entries = tokio::fs::read_dir(&src).await?;
        while let Some(entry) = entries.next_entry().await? {
            let meta = entry.metadata().await?;
            let dest_path = dst.join(entry.file_name());
            if meta.is_file() {
                tokio::fs::copy(entry.path(), &dest_path).await?;
                total += meta.len();
            } else if meta.is_dir() {
                tokio::fs::create_dir_all(&dest_path).await?;
                stack.push((entry.path(), dest_path));
            }
        }
    }

    tokio::fs::rename(&temp_dir, dest_dir).await?;

    Ok(total)
}

use std::mem::size_of;
