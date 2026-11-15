use super::*;
use std::io;
use std::path::Path;

pub struct DaemonStore;

impl DaemonStore {
    pub fn open(_data: &Path) -> io::Result<(Self, DaemonStartup)> {
        Err(io::Error::other("daemon stub"))
    }
    pub fn insert_deliver(&self, _row: &DaemonDeliverRow) -> io::Result<bool> {
        Err(io::Error::other("daemon stub"))
    }
    pub fn pending_for_session(&self, _session: &str) -> io::Result<Vec<DaemonDeliverRow>> {
        Err(io::Error::other("daemon stub"))
    }
    pub fn settle_ack(
        &self,
        _mail_id: &str,
        _session: &str,
        _record_id: &str,
    ) -> io::Result<bool> {
        Err(io::Error::other("daemon stub"))
    }
    pub fn confirm_receipt(&self, _mail_id: &str, _session: Option<&str>) -> io::Result<()> {
        Err(io::Error::other("daemon stub"))
    }
}
