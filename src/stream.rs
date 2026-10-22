//! Optional SSE follower of new chat records.
//!
//! The JSONL dump is written by the caller and is not read or rewritten here.
//! This module appends a separate spool and serves it only when configured.

use anyhow::{bail, Context, Result};
use chrono::{DateTime, Datelike, Timelike, Utc};
use chrono_tz::America::New_York;
use eq_network::chat::ChannelName;
use eq_network::client::{Record, RecordEvent};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::fs::{self, File, OpenOptions};
use std::io::{self, BufRead, BufReader, Read, Seek, SeekFrom, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::mpsc::{self, Receiver, RecvTimeoutError, SyncSender};
use std::sync::{Arc, Mutex, Weak};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};
use zeroize::Zeroizing;

const MAX_LINE_BYTES: usize = 8192;
const HEADER_LIMIT: usize = 8192;
const PING_INTERVAL: Duration = Duration::from_secs(15);
const MAX_CLIENTS: usize = 32;
const MAX_ROW_BYTES: u64 = 64 * 1024;

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
struct SpoolEvent {
    seq: u64,
    file: String,
    character: String,
    line: String,
}

struct Inner {
    spool_path: PathBuf,
    spool: File,
    file_name: String,
    next_seq: u64,
    first_seq: Option<u64>,
    subscribers: BTreeMap<u64, SyncSender<String>>,
    next_subscriber: u64,
}

/// Persisted sequence plus the fan-out used by live HTTP clients.
pub struct StreamHub {
    inner: Mutex<Inner>,
}

pub struct ServerHandle {
    stop: Arc<AtomicBool>,
    join: Option<JoinHandle<()>>,
    inflight: Arc<AtomicUsize>,
}

enum Resume {
    /// No `Last-Event-ID`: follow only lines appended after this connection.
    Live,
    /// Replay `seq` greater than this value, then follow.
    After(u64),
    /// The cursor cannot be used. Send `reset`, then follow live.
    Reset,
}

struct Follow {
    reset: Option<String>,
    backlog: Replay,
    live: Receiver<String>,
    _subscription: Subscription,
}

/// Remove idle/disconnected registrations even when no chat is arriving.
struct Subscription {
    hub: Weak<StreamHub>,
    id: u64,
}

impl Drop for Subscription {
    fn drop(&mut self) {
        if let Some(hub) = self.hub.upgrade() {
            if let Ok(mut inner) = hub.inner.lock() {
                inner.subscribers.remove(&self.id);
            }
        }
    }
}

/// A fixed file prefix, read outside the writer lock with bounded memory.
#[derive(Default)]
struct Replay {
    reader: Option<BufReader<io::Take<File>>>,
    cursor: u64,
}

impl Replay {
    fn next(&mut self, stop: &AtomicBool) -> Result<Option<String>> {
        let Some(reader) = self.reader.as_mut() else {
            return Ok(None);
        };
        while !stop.load(Ordering::Relaxed) {
            let Some(event) = read_row(reader)? else {
                return Ok(None);
            };
            if event.seq > self.cursor {
                return Ok(Some(line_event(&event)));
            }
        }
        Ok(None)
    }
}

impl StreamHub {
    /// Open or create `spool`. Existing lines set the next sequence number.
    ///
    /// # Errors
    ///
    /// Returns an error when the spool cannot be read, repaired, or appended.
    pub fn open(spool: &Path, file_name: &str) -> Result<Arc<Self>> {
        if let Some(parent) = spool.parent() {
            if !parent.as_os_str().is_empty() && !parent.exists() {
                bail!("spool directory does not exist: {}", parent.display());
            }
        }
        repair_trailing_partial(spool)?;
        let (next_seq, first_seq) = load_spool(spool)?;
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(spool)
            .with_context(|| format!("open stream spool {}", spool.display()))?;
        Ok(Arc::new(Self {
            inner: Mutex::new(Inner {
                spool_path: spool.to_path_buf(),
                spool: file,
                file_name: file_name.to_owned(),
                next_seq,
                first_seq,
                subscribers: BTreeMap::new(),
                next_subscriber: 0,
            }),
        }))
    }

    /// Append one synthesized log line and flush it before assigning `seq`.
    ///
    /// # Errors
    ///
    /// Returns an error when the spool write or flush fails. The sequence does
    /// not advance in that case.
    pub fn append(&self, character: &str, line: &str) -> Result<u64> {
        let mut inner = self.inner.lock().expect("stream spool lock");
        let seq = inner.next_seq;
        let next_seq = seq.checked_add(1).context("stream sequence exhausted")?;
        let event = SpoolEvent {
            seq,
            file: inner.file_name.clone(),
            character: character.to_owned(),
            line: line.to_owned(),
        };
        let mut bytes = serde_json::to_vec(&event)?;
        bytes.push(b'\n');
        anyhow::ensure!(bytes.len() as u64 <= MAX_ROW_BYTES, "stream row too large");
        let length = inner.spool.metadata()?.len();
        if let Err(error) = inner
            .spool
            .write_all(&bytes)
            .and_then(|()| inner.spool.flush())
        {
            inner
                .spool
                .set_len(length)
                .context("rollback partial stream row")?;
            return Err(error).context("write stream spool");
        }
        inner.next_seq = next_seq;
        if inner.first_seq.is_none() {
            inner.first_seq = Some(seq);
        }
        let sse = line_event(&event);
        inner
            .subscribers
            .retain(|_, sender| sender.try_send(sse.clone()).is_ok());
        Ok(seq)
    }

    fn follow(self: &Arc<Self>, resume: Resume) -> Result<Follow> {
        let mut inner = self.inner.lock().expect("stream spool lock");
        let last = inner.next_seq.saturating_sub(1);
        let first = inner.first_seq;
        let path = inner.spool_path.clone();
        let (reset, backlog) = match resume {
            Resume::Live => (None, Replay::default()),
            Resume::Reset => (Some(reset_event(last, "unknown_cursor")), Replay::default()),
            Resume::After(cursor) => {
                if cursor > last {
                    (Some(reset_event(last, "unknown_cursor")), Replay::default())
                } else if first.is_some_and(|oldest| cursor + 1 < oldest) {
                    let oldest = first.expect("oldest seq");
                    let from = oldest.saturating_sub(1);
                    (Some(reset_event(from, "expired")), read_after(&path, from)?)
                } else {
                    (None, read_after(&path, cursor)?)
                }
            }
        };
        let id = inner.next_subscriber;
        inner.next_subscriber = id.checked_add(1).context("stream subscription exhausted")?;
        let (tx, rx) = mpsc::sync_channel(64);
        inner.subscribers.insert(id, tx);
        Ok(Follow {
            reset,
            backlog,
            live: rx,
            _subscription: Subscription {
                hub: Arc::downgrade(self),
                id,
            },
        })
    }
}

impl ServerHandle {
    /// Stop accepting clients and wait for in-flight streams to notice.
    pub fn shutdown(mut self) {
        self.stop.store(true, Ordering::Relaxed);
        if let Some(join) = self.join.take() {
            let _ = join.join();
        }
        let deadline = Instant::now() + Duration::from_secs(3);
        while self.inflight.load(Ordering::Relaxed) > 0 && Instant::now() < deadline {
            thread::sleep(Duration::from_millis(20));
        }
    }
}

impl Drop for ServerHandle {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
        if let Some(join) = self.join.take() {
            let _ = join.join();
        }
    }
}

/// Check the optional listener settings before opening a socket.
///
/// # Errors
///
/// Returns an error when the bind address, stream id, token, or spool path is unusable.
pub fn validate(listen: &str, stream_id: &str, token: &str, spool: &Path) -> Result<()> {
    listen
        .parse::<SocketAddr>()
        .context("parse stream listen address")?;
    if !valid_stream_id(stream_id) {
        bail!("stream_id must be 1-64 characters of letters, digits, '.', '_' or '-'");
    }
    if token.is_empty() || token.len() > 1024 || !token.bytes().all(|b| b.is_ascii_graphic()) {
        bail!("stream token must be 1-1024 visible ASCII characters without spaces");
    }
    if spool.as_os_str().is_empty() {
        bail!("stream spool path must not be empty");
    }
    if spool
        .file_name()
        .and_then(|name| name.to_str())
        .is_none_or(|name| name.contains(':') || name.contains('/') || name.contains('\\'))
    {
        bail!("stream spool file name must not contain ':', '/' or '\\'");
    }
    Ok(())
}

/// Reject output/health aliases before spool recovery can change either file.
pub fn require_distinct(spool: &Path, other: &Path) -> Result<()> {
    fn resolved(path: &Path) -> Result<PathBuf> {
        if path.exists() {
            return Ok(fs::canonicalize(path)?);
        }
        anyhow::ensure!(
            !path.is_symlink(),
            "cannot resolve missing stream/output symlink target"
        );
        let parent = path
            .parent()
            .filter(|p| !p.as_os_str().is_empty())
            .unwrap_or(Path::new("."));
        Ok(fs::canonicalize(parent)?.join(path.file_name().context("missing file name")?))
    }
    let same = resolved(spool)? == resolved(other)?
        || (spool.exists() && other.exists() && same_file::is_same_file(spool, other)?);
    anyhow::ensure!(
        !same,
        "stream spool must differ from output and health files"
    );
    Ok(())
}

/// Bind the listener and serve `GET /v1/streams/{stream_id}` until `shutdown`.
///
/// # Errors
///
/// Returns an error when the address cannot be bound.
pub fn serve(
    listen: &str,
    stream_id: String,
    token: String,
    hub: Arc<StreamHub>,
) -> Result<(SocketAddr, ServerHandle)> {
    let listener =
        TcpListener::bind(listen).with_context(|| format!("bind log stream {listen}"))?;
    listener.set_nonblocking(true)?;
    let addr = listener
        .local_addr()
        .context("read stream listen address")?;
    let stop = Arc::new(AtomicBool::new(false));
    let inflight = Arc::new(AtomicUsize::new(0));
    let stop_flag = Arc::clone(&stop);
    let inflight_flag = Arc::clone(&inflight);
    let join = thread::Builder::new()
        .name("log-stream".into())
        .spawn(move || {
            accept_loop(
                listener,
                stream_id,
                Arc::new(Zeroizing::new(token)),
                hub,
                stop_flag,
                inflight_flag,
            )
        })
        .context("spawn log stream")?;
    eprintln!("log stream listening on {addr}");
    Ok((
        addr,
        ServerHandle {
            stop,
            join: Some(join),
            inflight,
        },
    ))
}

/// Build the EQ-style line for one decoded record.
///
/// Guild chat uses the form the reader looks for. Other channels are still
/// streamed, but they do not use that form. Decode failures and empty or
/// oversized text return `None` so they stay in the JSONL dump only.
pub fn eq_line(record: &Record) -> Option<String> {
    let RecordEvent::Chat(chat) = &record.event else {
        return None;
    };
    let text = one_line(chat.message.as_ref()?.text.as_str());
    if text.is_empty() {
        return None;
    }
    let stamp = eastern_stamp(&record.timestamp)?;
    let sender = one_line(chat.sender.as_deref().unwrap_or(""));
    let line = if chat.channel_name == ChannelName::Guild {
        if sender.is_empty() {
            format!("[{stamp}] You say to your guild, '{text}'")
        } else {
            format!("[{stamp}] {sender} tells the guild, '{text}'")
        }
    } else if sender.is_empty() {
        format!("[{stamp}] You say, '{text}'")
    } else {
        format!("[{stamp}] {sender} says, '{text}'")
    };
    if line.len() > MAX_LINE_BYTES {
        return None;
    }
    Some(line)
}

/// Render a UTC RFC3339 timestamp as an EQ log stamp in US Eastern.
fn eastern_stamp(timestamp: &str) -> Option<String> {
    let utc: DateTime<Utc> = DateTime::parse_from_rfc3339(timestamp)
        .ok()?
        .with_timezone(&Utc);
    let local = utc.with_timezone(&New_York);
    let weekday = match local.weekday().num_days_from_monday() {
        0 => "Mon",
        1 => "Tue",
        2 => "Wed",
        3 => "Thu",
        4 => "Fri",
        5 => "Sat",
        _ => "Sun",
    };
    let month = match local.month() {
        1 => "Jan",
        2 => "Feb",
        3 => "Mar",
        4 => "Apr",
        5 => "May",
        6 => "Jun",
        7 => "Jul",
        8 => "Aug",
        9 => "Sep",
        10 => "Oct",
        11 => "Nov",
        _ => "Dec",
    };
    Some(format!(
        "{weekday} {month} {:02} {:02}:{:02}:{:02} {}",
        local.day(),
        local.hour(),
        local.minute(),
        local.second(),
        local.year()
    ))
}

fn one_line(value: &str) -> String {
    value
        .chars()
        .map(|ch| if ch.is_control() { ' ' } else { ch })
        .collect()
}

fn valid_stream_id(id: &str) -> bool {
    !id.is_empty()
        && id.len() <= 64
        && id
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-'))
}

fn line_event(event: &SpoolEvent) -> String {
    let data = serde_json::to_string(event).unwrap_or_else(|_| "{}".to_owned());
    format!("id: {}\nevent: line\ndata: {data}\n\n", event.seq)
}

fn reset_event(seq: u64, reason: &str) -> String {
    let data = serde_json::json!({ "seq": seq, "reason": reason });
    format!("id: {seq}\nevent: reset\ndata: {data}\n\n")
}

fn repair_trailing_partial(path: &Path) -> Result<()> {
    if !path.exists() {
        return Ok(());
    }
    let mut reader = File::open(path).context("read stream spool tail")?;
    let length = reader.metadata()?.len();
    if length == 0 {
        return Ok(());
    }
    let start = length.saturating_sub(MAX_ROW_BYTES);
    reader.seek(SeekFrom::Start(start))?;
    let mut bytes = Vec::new();
    reader.take(MAX_ROW_BYTES).read_to_end(&mut bytes)?;
    if bytes.last() == Some(&b'\n') {
        return Ok(());
    }
    let end = match bytes.iter().rposition(|byte| *byte == b'\n') {
        Some(index) => start + index as u64 + 1,
        None if start == 0 => 0,
        None => bail!("stream spool partial row exceeds size limit"),
    };
    let file = OpenOptions::new()
        .write(true)
        .open(path)
        .with_context(|| format!("repair stream spool {}", path.display()))?;
    file.set_len(end)
        .context("truncate partial stream spool line")?;
    Ok(())
}

fn load_spool(path: &Path) -> Result<(u64, Option<u64>)> {
    if !path.exists() {
        return Ok((1, None));
    }
    let file = File::open(path).with_context(|| format!("read stream spool {}", path.display()))?;
    let mut next = 1_u64;
    let mut first = None;
    let mut reader = BufReader::new(file);
    while let Some(event) = read_row(&mut reader)? {
        if event.seq == 0 || event.seq != next {
            bail!(
                "stream spool sequence must increase by 1 starting at {next}, found {}",
                event.seq
            );
        }
        if first.is_none() {
            first = Some(event.seq);
        }
        next = event
            .seq
            .checked_add(1)
            .context("stream sequence exhausted")?;
    }
    Ok((next, first))
}

fn read_after(path: &Path, cursor: u64) -> Result<Replay> {
    let file = File::open(path).context("replay stream spool")?;
    let length = file.metadata()?.len();
    Ok(Replay {
        reader: Some(BufReader::new(file.take(length))),
        cursor,
    })
}

fn read_row(reader: &mut impl BufRead) -> Result<Option<SpoolEvent>> {
    let mut bytes = Vec::new();
    let count = reader
        .take(MAX_ROW_BYTES + 1)
        .read_until(b'\n', &mut bytes)?;
    if count == 0 {
        return Ok(None);
    }
    anyhow::ensure!(
        count as u64 <= MAX_ROW_BYTES && bytes.last() == Some(&b'\n'),
        "invalid stream spool row length"
    );
    Ok(Some(
        serde_json::from_slice(&bytes).context("parse stream spool row")?,
    ))
}

fn accept_loop(
    listener: TcpListener,
    stream_id: String,
    token: Arc<Zeroizing<String>>,
    hub: Arc<StreamHub>,
    stop: Arc<AtomicBool>,
    inflight: Arc<AtomicUsize>,
) {
    while !stop.load(Ordering::Relaxed) {
        match listener.accept() {
            Ok((socket, _)) => {
                // Bound unauthenticated requests too; a partial header must not
                // create unlimited threads or prevent the game session running.
                if inflight.load(Ordering::Relaxed) >= MAX_CLIENTS {
                    drop(socket);
                    continue;
                }
                let stream_id = stream_id.clone();
                let token = token.clone();
                let hub = Arc::clone(&hub);
                let stop = Arc::clone(&stop);
                let inflight = Arc::clone(&inflight);
                inflight.fetch_add(1, Ordering::Relaxed);
                let active = Arc::clone(&inflight);
                let spawned = thread::Builder::new()
                    .name("log-stream-client".into())
                    .spawn(move || {
                        if let Err(error) = handle_client(socket, &stream_id, &token, &hub, &stop) {
                            if !normal_disconnect(&error) {
                                eprintln!("log stream client ended: {error}");
                            }
                        }
                        inflight.fetch_sub(1, Ordering::Relaxed);
                    });
                if let Err(error) = spawned {
                    active.fetch_sub(1, Ordering::Relaxed);
                    eprintln!("log stream client could not start: {error}");
                }
            }
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                thread::sleep(Duration::from_millis(50));
            }
            Err(error) => {
                eprintln!("log stream accept failed: {error}");
                break;
            }
        }
    }
}

/// Followers may close at any point; keep real spool/protocol failures visible.
fn normal_disconnect(error: &io::Error) -> bool {
    matches!(
        error.kind(),
        io::ErrorKind::BrokenPipe
            | io::ErrorKind::ConnectionReset
            | io::ErrorKind::ConnectionAborted
            | io::ErrorKind::UnexpectedEof
    )
}

fn handle_client(
    mut socket: TcpStream,
    stream_id: &str,
    token: &str,
    hub: &Arc<StreamHub>,
    stop: &AtomicBool,
) -> io::Result<()> {
    socket.set_nodelay(true)?;
    let raw = read_headers(&mut socket)?;
    let request = parse_request(&raw);
    if request.method != "GET" || request.path != format!("/v1/streams/{stream_id}") {
        return write_plain(&mut socket, "404 Not Found", "not found\n");
    }
    if !tokens_match(request.authorization.as_deref().unwrap_or(""), token) {
        return write_plain(&mut socket, "401 Unauthorized", "unauthorized\n");
    }
    let resume = match request.last_event_id.as_deref() {
        None => Resume::Live,
        Some(value) => match value.parse::<u64>() {
            Ok(cursor) => Resume::After(cursor),
            Err(_) => Resume::Reset,
        },
    };
    let mut follow = hub.follow(resume).map_err(io::Error::other)?;
    socket.set_write_timeout(Some(Duration::from_secs(5)))?;
    write!(
        socket,
        "HTTP/1.1 200 OK\r\nContent-Type: text/event-stream; charset=utf-8\r\nCache-Control: no-cache, no-transform\r\nX-Accel-Buffering: no\r\nConnection: keep-alive\r\nTransfer-Encoding: chunked\r\n\r\n"
    )?;
    if let Some(reset) = &follow.reset {
        write_chunk(&mut socket, reset)?;
    }
    while let Some(event) = follow.backlog.next(stop).map_err(io::Error::other)? {
        write_chunk(&mut socket, &event)?;
    }
    let mut since_ping = Instant::now();
    loop {
        if stop.load(Ordering::Relaxed) {
            break;
        }
        match follow.live.recv_timeout(Duration::from_secs(1)) {
            Ok(event) => {
                write_chunk(&mut socket, &event)?;
                since_ping = Instant::now();
            }
            Err(RecvTimeoutError::Timeout) => {
                if since_ping.elapsed() >= PING_INTERVAL {
                    write_chunk(&mut socket, ": ping\n\n")?;
                    since_ping = Instant::now();
                }
            }
            Err(RecvTimeoutError::Disconnected) => break,
        }
    }
    let _ = write!(socket, "0\r\n\r\n");
    let _ = socket.flush();
    Ok(())
}

struct Request {
    method: String,
    path: String,
    authorization: Option<String>,
    last_event_id: Option<String>,
}

fn read_headers(socket: &mut TcpStream) -> io::Result<String> {
    let deadline = Instant::now() + Duration::from_secs(5);
    let mut buf = Vec::new();
    let mut tmp = [0_u8; 512];
    while !buf.windows(4).any(|window| window == b"\r\n\r\n") {
        if buf.len() >= HEADER_LIMIT {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "request headers too large",
            ));
        }
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return Err(io::Error::new(
                io::ErrorKind::TimedOut,
                "request header deadline",
            ));
        }
        socket.set_read_timeout(Some(remaining))?;
        let limit = tmp.len().min(HEADER_LIMIT - buf.len());
        let read = socket.read(&mut tmp[..limit])?;
        if read == 0 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "incomplete request headers",
            ));
        }
        buf.extend_from_slice(&tmp[..read]);
    }
    String::from_utf8(buf).map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))
}

fn parse_request(raw: &str) -> Request {
    let mut lines = raw.split("\r\n\r\n").next().unwrap_or("").split("\r\n");
    let mut request = Request {
        method: String::new(),
        path: String::new(),
        authorization: None,
        last_event_id: None,
    };
    if let Some(start) = lines.next() {
        let mut parts = start.split_whitespace();
        request.method = parts.next().unwrap_or("").to_owned();
        request.path = parts.next().unwrap_or("").to_owned();
    }
    for line in lines {
        let Some((name, value)) = line.split_once(':') else {
            continue;
        };
        let value = value.trim();
        if name.eq_ignore_ascii_case("authorization") {
            request.authorization = value.strip_prefix("Bearer ").map(str::to_owned);
        } else if name.eq_ignore_ascii_case("last-event-id") {
            request.last_event_id = Some(value.to_owned());
        }
    }
    request
}

fn tokens_match(presented: &str, expected: &str) -> bool {
    let presented = presented.as_bytes();
    let expected = expected.as_bytes();
    let mut diff = presented.len() ^ expected.len();
    for index in 0..presented.len().max(expected.len()) {
        let left = presented.get(index).copied().unwrap_or(0);
        let right = expected.get(index).copied().unwrap_or(0);
        diff |= usize::from(left ^ right);
    }
    diff == 0
}

fn write_plain(socket: &mut TcpStream, status: &str, body: &str) -> io::Result<()> {
    write!(
        socket,
        "HTTP/1.1 {status}\r\nContent-Type: text/plain; charset=utf-8\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    )?;
    socket.flush()
}

fn write_chunk(socket: &mut TcpStream, data: &str) -> io::Result<()> {
    write!(socket, "{:x}\r\n{data}\r\n", data.len())?;
    socket.flush()
}

#[cfg(test)]
mod tests;
