//! Filesystem mount/unmount helpers.

use std::ffi::CString;
use std::io;
use std::path::Path;

use anyhow::{bail, Context};

/// Mount a filesystem.
///
/// Thin safe wrapper around `libc::mount`. The `flags` parameter uses
/// `libc::MS_*` constants.
pub fn mount(
    source: &str,
    target: &Path,
    fstype: &str,
    flags: libc::c_ulong,
    options: &str,
) -> anyhow::Result<()> {
    let source_c = CString::new(source).context("mount source")?;
    let target_str = target
        .to_str()
        .context("mount target path not valid UTF-8")?;
    let target_c = CString::new(target_str).context("mount target")?;
    let fstype_c = CString::new(fstype).context("mount fstype")?;
    let options_c = CString::new(options).context("mount options")?;

    let ret = unsafe {
        libc::mount(
            source_c.as_ptr(),
            target_c.as_ptr(),
            fstype_c.as_ptr(),
            flags,
            options_c.as_ptr() as *const libc::c_void,
        )
    };
    if ret != 0 {
        let err = io::Error::last_os_error();
        bail!(
            "mount at {:?}: {} (source={:?}, type={:?}, options={:?})",
            target,
            err,
            source,
            fstype,
            options
        );
    }
    Ok(())
}

/// Unmount a filesystem with `MNT_DETACH`.
///
/// Returns `Ok(())` on success, `Err` with the OS error on failure.
pub fn umount_detach(target: &Path) -> io::Result<()> {
    let target_str = target
        .to_str()
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "path not UTF-8"))?;
    let target_c = CString::new(target_str)
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "path contains null byte"))?;

    let ret = unsafe { libc::umount2(target_c.as_ptr(), libc::MNT_DETACH) };
    if ret != 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(())
}
