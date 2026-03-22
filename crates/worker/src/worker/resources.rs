use distvirt_worker_protocol::PsiMetrics;

/// Query filesystem stats for a pool path, returning (capacity_bytes, available_bytes).
/// Creates the directory if needed. Returns (0, 0) on any error.
pub(crate) fn pool_disk_stats(path: &std::path::Path) -> (u64, u64) {
    let _ = std::fs::create_dir_all(path);
    crate::linux::fs::disk_stats(path)
        .map(|s| (s.capacity_bytes, s.available_bytes))
        .unwrap_or((0, 0))
}

/// Detect total host memory in MB by reading `/proc/meminfo`.
/// Returns 1024 as fallback if detection fails.
pub(crate) fn detect_host_memory_mb() -> u64 {
    #[cfg(target_os = "linux")]
    {
        if let Ok(contents) = std::fs::read_to_string("/proc/meminfo") {
            for line in contents.lines() {
                if let Some(rest) = line.strip_prefix("MemTotal:") {
                    // Format: "MemTotal:       16384000 kB"
                    let rest = rest.trim();
                    if let Some(kb_str) =
                        rest.strip_suffix("kB").or_else(|| rest.strip_suffix("KB"))
                    {
                        if let Ok(kb) = kb_str.trim().parse::<u64>() {
                            return kb / 1024;
                        }
                    }
                }
            }
        }
        1024
    }
    #[cfg(not(target_os = "linux"))]
    {
        1024
    }
}

/// Parse PSI content string into `PsiMetrics`.
///
/// Format: `some avg10=X.XX avg60=X.XX avg300=X.XX total=N\nfull avg10=...`
/// The `cpu` file only has a `some` line (no `full`).
pub(crate) fn parse_psi(content: &str) -> PsiMetrics {
    let mut some_avg10 = 0.0;
    let mut some_avg60 = 0.0;
    let mut full_avg10 = 0.0;
    let mut full_avg60 = 0.0;

    for line in content.lines() {
        let is_some = line.starts_with("some ");
        let is_full = line.starts_with("full ");
        if !is_some && !is_full {
            continue;
        }
        for part in line.split_whitespace() {
            if let Some(val) = part.strip_prefix("avg10=") {
                if let Ok(v) = val.parse::<f64>() {
                    if is_some {
                        some_avg10 = v;
                    } else {
                        full_avg10 = v;
                    }
                }
            } else if let Some(val) = part.strip_prefix("avg60=") {
                if let Ok(v) = val.parse::<f64>() {
                    if is_some {
                        some_avg60 = v;
                    } else {
                        full_avg60 = v;
                    }
                }
            }
        }
    }

    PsiMetrics {
        some_avg10,
        some_avg60,
        full_avg10,
        full_avg60,
    }
}

/// Read and parse a single `/proc/pressure/{cpu,memory,io}` file.
pub(crate) fn read_psi_file(path: &str) -> Option<PsiMetrics> {
    let content = std::fs::read_to_string(path).ok()?;
    Some(parse_psi(&content))
}

/// Read PSI metrics for all three resource dimensions.
/// Returns `None` on non-Linux or if `/proc/pressure/` is unavailable.
pub(crate) fn read_all_psi() -> Option<(PsiMetrics, PsiMetrics, PsiMetrics)> {
    let cpu = read_psi_file("/proc/pressure/cpu")?;
    let memory = read_psi_file("/proc/pressure/memory")?;
    let io = read_psi_file("/proc/pressure/io")?;
    Some((cpu, memory, io))
}

/// Check if any avg10 value changed by more than 1 percentage point.
pub(crate) fn psi_changed_significantly(
    old: &(PsiMetrics, PsiMetrics, PsiMetrics),
    new: &(PsiMetrics, PsiMetrics, PsiMetrics),
) -> bool {
    fn delta(a: f64, b: f64) -> bool {
        (a - b).abs() > 1.0
    }
    delta(old.0.some_avg10, new.0.some_avg10)
        || delta(old.1.some_avg10, new.1.some_avg10)
        || delta(old.2.some_avg10, new.2.some_avg10)
        || delta(old.0.full_avg10, new.0.full_avg10)
        || delta(old.1.full_avg10, new.1.full_avg10)
        || delta(old.2.full_avg10, new.2.full_avg10)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_psi_memory() {
        let content = "some avg10=1.23 avg60=4.56 avg300=7.89 total=12345\n\
                        full avg10=0.11 avg60=0.22 avg300=0.33 total=6789\n";
        let m = parse_psi(content);
        assert!((m.some_avg10 - 1.23).abs() < 0.001);
        assert!((m.some_avg60 - 4.56).abs() < 0.001);
        assert!((m.full_avg10 - 0.11).abs() < 0.001);
        assert!((m.full_avg60 - 0.22).abs() < 0.001);
    }

    #[test]
    fn test_parse_psi_cpu_no_full_line() {
        // CPU PSI only has a "some" line on most kernels.
        let content = "some avg10=5.00 avg60=3.00 avg300=1.50 total=99999\n";
        let m = parse_psi(content);
        assert!((m.some_avg10 - 5.0).abs() < 0.001);
        assert!((m.some_avg60 - 3.0).abs() < 0.001);
        assert_eq!(m.full_avg10, 0.0);
        assert_eq!(m.full_avg60, 0.0);
    }

    #[test]
    fn test_psi_changed_significantly_small_delta() {
        let a = (
            PsiMetrics {
                some_avg10: 1.0,
                some_avg60: 0.0,
                full_avg10: 0.0,
                full_avg60: 0.0,
            },
            PsiMetrics::default(),
            PsiMetrics::default(),
        );
        let b = (
            PsiMetrics {
                some_avg10: 1.5,
                some_avg60: 0.0,
                full_avg10: 0.0,
                full_avg60: 0.0,
            },
            PsiMetrics::default(),
            PsiMetrics::default(),
        );
        assert!(!psi_changed_significantly(&a, &b));
    }

    #[test]
    fn test_psi_changed_significantly_large_delta() {
        let a = (
            PsiMetrics {
                some_avg10: 1.0,
                some_avg60: 0.0,
                full_avg10: 0.0,
                full_avg60: 0.0,
            },
            PsiMetrics::default(),
            PsiMetrics::default(),
        );
        let b = (
            PsiMetrics {
                some_avg10: 5.0,
                some_avg60: 0.0,
                full_avg10: 0.0,
                full_avg60: 0.0,
            },
            PsiMetrics::default(),
            PsiMetrics::default(),
        );
        assert!(psi_changed_significantly(&a, &b));
    }
}
