use crate::util;

pub fn mount_essential_filesystems() {
    let mounts: &[(&str, &str, &str, libc::c_ulong, Option<&str>)] = &[
        (
            "proc",
            "/proc",
            "proc",
            libc::MS_NOSUID | libc::MS_NODEV | libc::MS_NOEXEC,
            None,
        ),
        (
            "sysfs",
            "/sys",
            "sysfs",
            libc::MS_NOSUID | libc::MS_NODEV | libc::MS_NOEXEC,
            None,
        ),
        (
            "tmpfs",
            "/tmp",
            "tmpfs",
            libc::MS_NOSUID | libc::MS_NODEV,
            None,
        ),
        (
            "devpts",
            "/dev/pts",
            "devpts",
            libc::MS_NOSUID | libc::MS_NOEXEC,
            Some("gid=5,mode=620"),
        ),
        (
            "tmpfs",
            "/dev/shm",
            "tmpfs",
            libc::MS_NOSUID | libc::MS_NODEV,
            None,
        ),
        (
            "cgroup2",
            "/sys/fs/cgroup",
            "cgroup2",
            libc::MS_NOSUID | libc::MS_NODEV | libc::MS_NOEXEC,
            None,
        ),
    ];

    for &(source, target, fstype, flags, data) in mounts {
        if let Err(err) = util::mount(source, target, fstype, flags, data) {
            log::warn!("{:#}", err);
        }
    }
}
