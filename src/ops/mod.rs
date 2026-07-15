pub mod diagnostics;
#[cfg(feature = "native-runtime")]
pub mod metrics;
#[cfg(feature = "native-runtime")]
pub mod slo;

/// Write `data` to `path` atomically: write to a uniquely-named sibling temp
/// file, fsync, rename, then fsync the parent directory (a rename is only
/// durable across power loss once the directory entry is journaled). Every
/// failure — including the directory fsync — is surfaced to the caller: the
/// raft storage builds its persist-before-ack safety argument on it. The
/// unique suffix (pid + counter) avoids temp file contention when multiple
/// callers persist concurrently.
///
/// Only compiled with `native-runtime`: every caller (HTTP handlers and the
/// equivocation persist task) lives behind that feature, and ungated it
/// would trip `dead_code` on `--no-default-features` (wasm) builds.
#[cfg(feature = "native-runtime")]
pub(crate) fn write_atomic(path: &std::path::Path, data: &[u8]) -> Result<(), String> {
    use std::io::Write;
    use std::sync::atomic::{AtomicU64, Ordering};

    static COUNTER: AtomicU64 = AtomicU64::new(0);

    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).map_err(|e| e.to_string())?;
    }
    let seq = COUNTER.fetch_add(1, Ordering::Relaxed);
    let tmp_name = format!(
        ".tmp.{}.{}.{}",
        path.file_name().unwrap_or_default().to_string_lossy(),
        std::process::id(),
        seq,
    );
    let tmp_path = path.with_file_name(tmp_name);
    let mut file = std::fs::File::create(&tmp_path).map_err(|e| e.to_string())?;
    file.write_all(data).map_err(|e| e.to_string())?;
    file.sync_all().map_err(|e| e.to_string())?;
    drop(file);
    if let Err(e) = std::fs::rename(&tmp_path, path) {
        // Clean up stranded temp file on rename failure.
        let _ = std::fs::remove_file(&tmp_path);
        return Err(e.to_string());
    }
    // Fsync the parent directory so the rename is durable. Failures MUST
    // surface: safety-critical callers (raft `save_hard_state` / `save_log`)
    // treat an `Ok` as "durable before the response is sent" — a swallowed
    // dir-fsync error would let a granted vote / term bump evaporate on
    // power loss (double-vote enabler). Best-effort callers already log
    // and tolerate the `Err`.
    let parent = match path.parent() {
        Some(p) if !p.as_os_str().is_empty() => p,
        _ => std::path::Path::new("."),
    };
    let dir = std::fs::File::open(parent)
        .map_err(|e| format!("open dir {} for fsync: {e}", parent.display()))?;
    dir.sync_all()
        .map_err(|e| format!("fsync dir {}: {e}", parent.display()))?;
    Ok(())
}
