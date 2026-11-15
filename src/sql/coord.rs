use super::*;
use std::io;

pub struct CoordinatorStore;

impl CoordinatorStore {
    pub fn open(_cfg: CoordOpenConfig) -> io::Result<(Self, CoordStartup)> {
        Err(io::Error::other("coord stub"))
    }
    pub fn commit_timeline(&self, _events: Vec<TimelineEvent>, _now_ms: i64) -> io::Result<()> {
        Err(io::Error::other("coord stub"))
    }
    pub fn send_directed(&self, _req: SendDirectedReq) -> io::Result<SendStatus> {
        Err(io::Error::other("coord stub"))
    }
    pub fn send_broadcast(&self, _req: SendBroadcastReq) -> io::Result<SendStatus> {
        Err(io::Error::other("coord stub"))
    }
    pub fn send_group(&self, _req: SendGroupReq) -> io::Result<SendStatus> {
        Err(io::Error::other("coord stub"))
    }
    pub fn record_ack(
        &self,
        _mail_id: &str,
        _session: &str,
        _record_id: &str,
        _now_ms: i64,
    ) -> io::Result<AckResult> {
        Err(io::Error::other("coord stub"))
    }
    pub fn pending_for_session(&self, _session: &str) -> io::Result<Vec<PendingDelivery>> {
        Err(io::Error::other("coord stub"))
    }
    pub fn pending_for_host(&self, _sessions: &[String]) -> io::Result<Vec<PendingDelivery>> {
        Err(io::Error::other("coord stub"))
    }
    pub fn query_receipt(
        &self,
        _mail_id: &str,
        _reader: Option<&str>,
    ) -> io::Result<Option<ReceiptInfo>> {
        Err(io::Error::other("coord stub"))
    }
    pub fn subscribe_group(
        &self,
        _session: &str,
        _group: &str,
        _after_article: Option<i64>,
        _from_latest: bool,
        _now_ms: i64,
    ) -> io::Result<(i64, i64, Vec<PendingDelivery>)> {
        Err(io::Error::other("coord stub"))
    }
    pub fn unsubscribe_group(
        &self,
        _session: &str,
        _group: &str,
        _now_ms: i64,
    ) -> io::Result<i64> {
        Err(io::Error::other("coord stub"))
    }
    pub fn list_groups(&self, _session: Option<&str>) -> io::Result<Vec<GroupSummary>> {
        Err(io::Error::other("coord stub"))
    }
    pub fn flush_cold_sweeps_for_test(&self) -> io::Result<()> {
        Ok(())
    }
}
