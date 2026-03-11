//! Filesystem helpers.

use std::io;
use std::path::Path;

/// Filesystem usage statistics.
pub struct DiskStats {
    pub capacity_bytes: u64,
    pub available_bytes: u64,
}

/// Query filesystem statistics for a path using `statvfs`.
pub fn disk_stats(path: &Path) -> io::Result<DiskStats> {
    use std::ffi::CString;
    use std::os::unix::ffi::OsStrExt;

    let c_path = CString::new(path.as_os_str().as_bytes())
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?;

    unsafe {
        let mut stat: libc::statvfs = std::mem::zeroed();
        if libc::statvfs(c_path.as_ptr(), &mut stat) == 0 {
            Ok(DiskStats {
                capacity_bytes: stat.f_blocks as u64 * stat.f_frsize as u64,
                available_bytes: stat.f_bavail as u64 * stat.f_frsize as u64,
            })
        } else {
            Err(io::Error::last_os_error())
        }
    }
}
