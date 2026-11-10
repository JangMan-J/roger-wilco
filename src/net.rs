//! Line-delimited JSON over any byte stream. One object per line.

use crate::json::{self, obj, s, Value};
use std::collections::VecDeque;
use std::io::{self, BufRead, BufReader, Read, Write};
use std::sync::{Arc, Condvar, Mutex};

/// Where one peer's lines go.
///
/// `Direct` writes on the caller's thread: a process's own stdout, or the one
/// connection of a client that has nothing else to serve.
///
/// `Queued` is for a process that serves many peers (the coordinator, a host
/// daemon). `send` only appends to the peer's queue and one thread per peer
/// does the writing, so a peer that stops reading holds up its own lines and
/// nothing else: no thread that serves another peer ever waits on it.
pub enum Out {
    Direct(Mutex<Box<dyn Write + Send>>),
    Queued(Queue),
}

pub type Writer = Arc<Out>;

/// How often a host daemon pings a coordinator that answers pings.
const PING_EVERY: std::time::Duration = std::time::Duration::from_secs(10);

/// Test hook (feature `test-hooks` only): a shorter pace for the ping, so a
/// test sees a silent link given up without waiting half a minute.
#[cfg(feature = "test-hooks")]
pub static LINK_PACE: std::sync::OnceLock<std::time::Duration> = std::sync::OnceLock::new();

pub fn ping_every() -> std::time::Duration {
    #[cfg(feature = "test-hooks")]
    if let Some(pace) = LINK_PACE.get() {
        return *pace;
    }
    PING_EVERY
}

/// How long a pinged link may stay silent before the side reading it gives
/// the peer up: three pings (30 s), longer than a host daemon waits for one
/// answer, so a peer that is only slow is not dropped.
pub fn link_silence() -> std::time::Duration {
    ping_every() * 3
}

/// A read that ran into `link_silence` says so, as what was expected and what
/// was seen; any other error passes through.
pub fn silence(e: io::Error) -> io::Error {
    match e.kind() {
        io::ErrorKind::WouldBlock | io::ErrorKind::TimedOut => io::Error::new(
            io::ErrorKind::TimedOut,
            format!(
                "expected a line from the peer within {} ms, saw none",
                link_silence().as_millis()
            ),
        ),
        _ => e,
    }
}

/// One peer's outgoing lines, in order. It has no bound: what it holds for a
/// peer that is not reading is Mail this process already keeps for that peer
/// until its Ack, plus the answers to that peer's own requests. A bound would
/// refuse a Register whose pending Mail is larger than it, every time.
pub struct Queue {
    shared: Arc<Shared>,
}

struct Shared {
    pending: Mutex<Pending>,
    more: Condvar,
    /// Ends the connection under the writing thread: a write that is waiting
    /// on the peer fails, and the peer reads end of stream.
    hangup: Box<dyn Fn() + Send + Sync>,
}

#[derive(Default)]
struct Pending {
    lines: VecDeque<String>,
    /// No line is accepted any more; the writing thread stops once `lines` is empty.
    closed: bool,
}

/// Dropping the last handle ends the writing thread once it has written what
/// was queued, so a connection nobody closed does not keep its thread.
impl Drop for Queue {
    fn drop(&mut self) {
        self.shared.pending.lock().unwrap().closed = true;
        self.shared.more.notify_all();
    }
}

/// The one shape a host daemon uses to forward a receipt upstream, both on
/// the ack itself and when replaying receipts after a reconnect.
pub fn ack_msg(mail: &str, session: &str, record: &str) -> Value {
    obj(vec![
        ("type", s("ack")),
        ("mail", s(mail)),
        ("session", s(session)),
        ("record", s(record)),
    ])
}

/// The Kind of a Mail sent to one Address.
pub const DIRECTED: &str = "directed";
/// The Kind of a copy of a Broadcast.
pub const BROADCAST: &str = "broadcast";

/// The one shape both the coordinator and a host daemon use to hand a Mail
/// to the next hop: its Kind and the sender's headers, carried as given.
pub fn deliver_msg(
    mail: &str,
    from: &str,
    to: &str,
    body: &str,
    kind: &str,
    headers: &Value,
) -> Value {
    obj(vec![
        ("type", s("deliver")),
        ("mail", s(mail)),
        ("from", s(from)),
        ("to", s(to)),
        ("body", s(body)),
        ("kind", s(kind)),
        ("headers", headers.clone()),
    ])
}

/// A delivery's Kind. A line written before Kind existed says it in the
/// older field: false marks a Broadcast copy, anything else is directed.
pub fn kind_of(v: &Value) -> String {
    if let Some(kind) = v.get("kind").and_then(|x| x.as_str()) {
        return kind.to_string();
    }
    let older = v.get("wake"); // legacy-wire: read a log or peer from before Kind
    match older {
        Some(Value::Bool(false)) => BROADCAST.to_string(),
        _ => DIRECTED.to_string(),
    }
}

/// A delivery's headers, as the sender set them; the empty set when the line
/// has none (written before headers existed, or sent without any).
pub fn headers_of(v: &Value) -> Value {
    v.get("headers").cloned().unwrap_or(Value::Obj(Vec::new()))
}

/// A Register's declared text: opaque text a session states about itself,
/// stored and returned verbatim and never read by the core.
pub fn declared_of(v: &Value) -> Option<String> {
    v.get("declared")
        .and_then(|x| x.as_str())
        .map(str::to_string)
}

pub fn writer(w: Box<dyn Write + Send>) -> Writer {
    Arc::new(Out::Direct(Mutex::new(w)))
}

/// A queued writer on `w`, with the thread that writes for it. `hangup` must
/// end the connection `w` writes to (a socket's `shutdown`).
pub fn queued(mut w: Box<dyn Write + Send>, hangup: Box<dyn Fn() + Send + Sync>) -> Writer {
    let shared = Arc::new(Shared {
        pending: Mutex::new(Pending::default()),
        more: Condvar::new(),
        hangup,
    });
    let work = Arc::clone(&shared);
    std::thread::spawn(move || {
        loop {
            let line = {
                let mut pending = work.pending.lock().unwrap();
                loop {
                    if let Some(line) = pending.lines.pop_front() {
                        break Some(line);
                    }
                    if pending.closed {
                        break None;
                    }
                    pending = work.more.wait(pending).unwrap();
                }
            };
            let Some(line) = line else { break };
            if w.write_all(line.as_bytes())
                .and_then(|()| w.flush())
                .is_err()
            {
                let mut pending = work.pending.lock().unwrap();
                pending.closed = true;
                pending.lines.clear();
                break;
            }
        }
        // Whichever way it ended, the peer reads end of stream now, even
        // while another handle on the same connection is still held.
        (work.hangup)();
    });
    Arc::new(Out::Queued(Queue { shared }))
}

/// End a queued writer; a direct one is left alone, since it ends with its
/// stream. With `drain` the lines already queued are written first (the peer
/// closed its side and may still be reading its answers). Without it they are
/// dropped and the connection ends at once, even under a write that is
/// waiting on the peer.
pub fn close(w: &Writer, drain: bool) {
    let Out::Queued(q) = &**w else { return };
    {
        let mut pending = q.shared.pending.lock().unwrap();
        pending.closed = true;
        if !drain {
            pending.lines.clear();
        }
    }
    q.shared.more.notify_all();
    if !drain {
        (q.shared.hangup)();
    }
}

/// One object as one line, handed over in a single write so the line and its
/// newline never travel as two segments.
pub fn send(w: &Writer, v: &Value) -> io::Result<()> {
    let mut line = json::stringify(v);
    line.push('\n');
    match &**w {
        Out::Direct(w) => {
            let mut guard = w.lock().unwrap();
            guard.write_all(line.as_bytes())?;
            guard.flush()
        }
        Out::Queued(q) => {
            let mut pending = q.shared.pending.lock().unwrap();
            if pending.closed {
                return Err(io::Error::new(
                    io::ErrorKind::BrokenPipe,
                    "expected an open connection, saw one already closed",
                ));
            }
            pending.lines.push_back(line);
            drop(pending);
            q.shared.more.notify_one();
            Ok(())
        }
    }
}

pub struct Lines {
    inner: BufReader<Box<dyn Read + Send>>,
    /// The longest line accepted, in bytes, newline excluded.
    cap: usize,
}

pub fn lines(r: Box<dyn Read + Send>) -> Lines {
    Lines {
        inner: BufReader::new(r),
        cap: usize::MAX,
    }
}

impl Lines {
    /// Accept no line longer than `bytes` from here on. A longer one is an
    /// `InvalidData` error as soon as it passes the bound, before the rest of
    /// it is read.
    pub fn set_cap(&mut self, bytes: usize) {
        self.cap = bytes;
    }

    /// The next line without its newline; `None` at end of stream. A last
    /// line that ends without a newline is returned as it stands.
    fn line(&mut self) -> io::Result<Option<String>> {
        let mut line: Vec<u8> = Vec::new();
        loop {
            let available = match self.inner.fill_buf() {
                Ok(bytes) => bytes,
                Err(e) if e.kind() == io::ErrorKind::Interrupted => continue,
                Err(e) => return Err(e),
            };
            if available.is_empty() {
                if line.is_empty() {
                    return Ok(None);
                }
                break;
            }
            let (take, ended) = match available.iter().position(|&b| b == b'\n') {
                Some(i) => (i, true),
                None => (available.len(), false),
            };
            if line.len() + take > self.cap {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!(
                        "expected a line of at most {} bytes, saw a longer one",
                        self.cap
                    ),
                ));
            }
            line.extend_from_slice(&available[..take]);
            self.inner.consume(take + usize::from(ended));
            if ended {
                break;
            }
        }
        if line.last() == Some(&b'\r') {
            line.pop();
        }
        String::from_utf8(line).map(Some).map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "expected a line of UTF-8, saw other bytes",
            )
        })
    }
}

impl Iterator for Lines {
    type Item = io::Result<Value>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            match self.line() {
                Ok(None) => return None,
                Ok(Some(line)) if line.trim().is_empty() => continue,
                Ok(Some(line)) => {
                    return Some(
                        json::parse(&line)
                            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e)),
                    )
                }
                Err(e) => return Some(Err(e)),
            }
        }
    }
}

pub fn field<'a>(v: &'a Value, key: &str) -> io::Result<&'a str> {
    v.get(key)
        .and_then(|x| x.as_str())
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, format!("missing {key}")))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::mpsc;

    fn read(bytes: &[u8]) -> Lines {
        lines(Box::new(io::Cursor::new(bytes.to_vec())))
    }

    #[test]
    fn lines_split_on_newlines_skip_blanks_and_keep_an_unterminated_last_line() {
        let got: Vec<Value> = read(b"{\"a\":1}\r\n\n  \n{\"b\":2}\n{\"c\":3}")
            .map(|v| v.unwrap())
            .collect();
        assert_eq!(
            got,
            vec![
                obj(vec![("a", Value::Int(1))]),
                obj(vec![("b", Value::Int(2))]),
                obj(vec![("c", Value::Int(3))]),
            ]
        );
    }

    #[test]
    fn a_line_past_the_cap_is_refused_and_one_at_the_cap_is_read() {
        let at = format!("{{\"k\":\"{}\"}}", "x".repeat(92)); // 100 bytes
        assert_eq!(at.len(), 100);
        let mut ok = read(format!("{at}\n").as_bytes());
        ok.set_cap(100);
        assert!(ok.next().unwrap().is_ok());
        let mut long = read(format!("x{at}\n").as_bytes());
        long.set_cap(100);
        let e = long.next().unwrap().unwrap_err();
        assert_eq!(e.kind(), io::ErrorKind::InvalidData);
        assert!(
            e.to_string()
                .contains("expected a line of at most 100 bytes, saw a longer one"),
            "{e}"
        );
        // A stream with no newline at all is refused at the bound too.
        let mut endless = read(&[b'['; 100_000]);
        endless.set_cap(100);
        assert!(endless.next().unwrap().is_err());
    }

    /// A stream whose every write waits until the test lets it through.
    struct Gated {
        open: mpsc::Receiver<()>,
        wrote: mpsc::Sender<String>,
    }

    impl Write for Gated {
        fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
            self.open
                .recv()
                .map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "gate gone"))?;
            let _ = self.wrote.send(String::from_utf8_lossy(buf).to_string());
            Ok(buf.len())
        }
        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    fn gated() -> (
        Writer,
        mpsc::Sender<()>,
        mpsc::Receiver<String>,
        mpsc::Receiver<()>,
    ) {
        let (open, gate) = mpsc::channel();
        let (wrote, seen) = mpsc::channel();
        let (hung, hangups) = mpsc::channel();
        let hung = Mutex::new(hung);
        let w = queued(
            Box::new(Gated { open: gate, wrote }),
            Box::new(move || {
                let _ = hung.lock().unwrap().send(());
            }),
        );
        (w, open, seen, hangups)
    }

    #[test]
    fn a_queued_send_returns_while_the_peer_takes_nothing_and_lines_keep_their_order() {
        let (w, open, seen, _hangups) = gated();
        // Nothing is let through yet: every send still returns at once.
        for n in 0..1000 {
            send(&w, &obj(vec![("n", Value::Int(n))])).unwrap();
        }
        for n in 0..1000 {
            open.send(()).unwrap();
            // One write per line, newline included.
            assert_eq!(seen.recv().unwrap(), format!("{{\"n\":{n}}}\n"));
        }
    }

    #[test]
    fn close_with_drain_writes_what_was_queued_then_hangs_up() {
        let (w, open, seen, hangups) = gated();
        send(&w, &obj(vec![("n", Value::Int(1))])).unwrap();
        send(&w, &obj(vec![("n", Value::Int(2))])).unwrap();
        close(&w, true);
        assert!(send(&w, &obj(vec![("n", Value::Int(3))])).is_err());
        open.send(()).unwrap();
        open.send(()).unwrap();
        assert_eq!(seen.recv().unwrap(), "{\"n\":1}\n");
        assert_eq!(seen.recv().unwrap(), "{\"n\":2}\n");
        hangups.recv().unwrap();
        assert!(seen.recv().is_err(), "nothing follows the drained lines");
    }

    #[test]
    fn close_without_drain_hangs_up_at_once_and_drops_the_queue() {
        let (w, open, seen, hangups) = gated();
        for n in 0..10 {
            send(&w, &obj(vec![("n", Value::Int(n))])).unwrap();
        }
        close(&w, false);
        // The hangup comes from the closing call itself, under the waiting write.
        hangups.recv().unwrap();
        assert!(send(&w, &obj(vec![("n", Value::Int(11))])).is_err());
        // At most the one line already handed to the stream gets out.
        drop(open);
        let written: Vec<String> = seen.iter().collect();
        assert!(written.len() <= 1, "{written:?}");
    }

    #[test]
    fn dropping_the_last_handle_ends_the_writing_thread() {
        let (w, open, seen, hangups) = gated();
        send(&w, &obj(vec![("n", Value::Int(1))])).unwrap();
        drop(w);
        open.send(()).unwrap();
        assert_eq!(seen.recv().unwrap(), "{\"n\":1}\n");
        hangups.recv().unwrap();
    }
}
