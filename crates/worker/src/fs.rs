use std::future::Future;
use std::io;
use std::path::Path;

/// Filesystem abstraction for worker I/O operations.
///
/// Production code uses `TokioFs` (delegates to `tokio::fs`), while sim tests
/// use `SyncFs` (uses `std::fs` inline) to avoid `spawn_blocking` interactions
/// with fake time and `current_thread` runtimes.
///
/// Methods are static — implementations are zero-sized type markers.
pub trait Fs: Send + Sync + 'static {
    fn read(path: &Path) -> impl Future<Output = io::Result<Vec<u8>>> + Send;
    fn remove_dir_all(path: &Path) -> impl Future<Output = io::Result<()>> + Send;
    fn dir_size(path: &Path) -> impl Future<Output = io::Result<u64>> + Send;
}

/// Production implementation: delegates to `tokio::fs`.
pub struct TokioFs;

impl Fs for TokioFs {
    async fn read(path: &Path) -> io::Result<Vec<u8>> {
        tokio::fs::read(path).await
    }

    async fn remove_dir_all(path: &Path) -> io::Result<()> {
        tokio::fs::remove_dir_all(path).await
    }

    async fn dir_size(path: &Path) -> io::Result<u64> {
        let mut total = 0u64;
        let mut stack = vec![path.to_path_buf()];
        while let Some(dir) = stack.pop() {
            let mut entries = tokio::fs::read_dir(&dir).await?;
            while let Some(entry) = entries.next_entry().await? {
                let meta = entry.metadata().await?;
                if meta.is_file() {
                    total += meta.len();
                } else if meta.is_dir() {
                    stack.push(entry.path());
                }
            }
        }
        Ok(total)
    }
}

/// Test implementation: uses `std::fs` synchronously (no blocking pool).
pub struct SyncFs;

impl Fs for SyncFs {
    async fn read(path: &Path) -> io::Result<Vec<u8>> {
        std::fs::read(path)
    }

    async fn remove_dir_all(path: &Path) -> io::Result<()> {
        std::fs::remove_dir_all(path)
    }

    async fn dir_size(path: &Path) -> io::Result<u64> {
        let mut total = 0u64;
        let mut stack = vec![path.to_path_buf()];
        while let Some(dir) = stack.pop() {
            for entry in std::fs::read_dir(&dir)? {
                let entry = entry?;
                let meta = entry.metadata()?;
                if meta.is_file() {
                    total += meta.len();
                } else if meta.is_dir() {
                    stack.push(entry.path());
                }
            }
        }
        Ok(total)
    }
}
