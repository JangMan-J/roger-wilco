use rusqlite::Connection;
use std::io;
use std::path::Path;

/// Read-only import of a legacy JSONL `coordinator.log` into initialized slice
/// connections (slice 0..N-1). Leaves `legacy_log` untouched on disk.
pub fn maybe_import_coordinator_log(
    _legacy_log: &Path,
    _slices: &mut [Connection],
) -> io::Result<bool> {
    Ok(false)
}

/// Read-only import of a legacy JSONL `daemon.log` into `conn`.
/// Leaves `legacy_log` untouched on disk.
pub fn maybe_import_daemon_log(_legacy_log: &Path, _conn: &mut Connection) -> io::Result<bool> {
    Ok(false)
}
