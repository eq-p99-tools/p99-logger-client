use super::*;
use eq_network::chat;
use eq_network::client::RecordEvent;
use std::time::Duration;

#[test]
fn expected_disconnects_are_quiet_but_stream_failures_remain_visible() {
    for kind in [
        io::ErrorKind::BrokenPipe,
        io::ErrorKind::ConnectionReset,
        io::ErrorKind::ConnectionAborted,
        io::ErrorKind::UnexpectedEof,
    ] {
        assert!(normal_disconnect(&io::Error::from(kind)));
    }
    for kind in [
        io::ErrorKind::Other,
        io::ErrorKind::InvalidData,
        io::ErrorKind::PermissionDenied,
    ] {
        assert!(!normal_disconnect(&io::Error::from(kind)));
    }
}

struct TestDir(PathBuf);

impl TestDir {
    fn new() -> Self {
        static NEXT: AtomicUsize = AtomicUsize::new(0);
        let path = std::env::temp_dir().join(format!(
            "logger-stream-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir(&path).unwrap();
        Self(path)
    }
}

impl Drop for TestDir {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

#[test]
fn spool_must_not_alias_existing_or_future_output() {
    let dir = TestDir::new();
    let output = dir.0.join("chat.jsonl");
    let alias = dir.0.join(".").join("chat.jsonl");
    assert!(require_distinct(&output, &alias).is_err());
    fs::write(&output, b"original output\npartial").unwrap();
    let link = dir.0.join("alias.jsonl");
    fs::hard_link(&output, &link).unwrap();
    assert!(require_distinct(&output, &link).is_err());
    assert_eq!(fs::read(&output).unwrap(), b"original output\npartial");
    assert!(require_distinct(&dir.0.join("spool.jsonl"), &output).is_ok());
}

#[cfg(unix)]
#[test]
fn broken_symlink_cannot_alias_a_future_output_file() {
    let dir = TestDir::new();
    let spool = dir.0.join("stream.jsonl");
    let output = dir.0.join("future.jsonl");
    std::os::unix::fs::symlink("future.jsonl", &spool).unwrap();
    assert!(require_distinct(&spool, &output).is_err());
    assert!(!output.exists());
}

#[test]
fn replay_snapshot_and_live_queue_do_not_duplicate_or_block_each_other() {
    let dir = TestDir::new();
    let hub = StreamHub::open(&dir.0.join("stream.jsonl"), "chat.jsonl").unwrap();
    hub.append("ExamplePlayer", "one").unwrap();
    let mut follower = hub.follow(Resume::After(0)).unwrap();
    hub.append("ExamplePlayer", "two").unwrap();
    let stop = AtomicBool::new(false);
    assert!(follower
        .backlog
        .next(&stop)
        .unwrap()
        .unwrap()
        .contains("\"seq\":1"));
    assert!(follower.backlog.next(&stop).unwrap().is_none());
    assert!(follower.live.try_recv().unwrap().contains("\"seq\":2"));
    assert_eq!(hub.inner.lock().unwrap().subscribers.len(), 1);
    drop(follower);
    assert!(hub.inner.lock().unwrap().subscribers.is_empty());
}

#[test]
fn slow_subscriber_disconnects_and_can_resume_every_missed_row() {
    let dir = TestDir::new();
    let hub = StreamHub::open(&dir.0.join("stream.jsonl"), "chat.jsonl").unwrap();
    let slow = hub.follow(Resume::Live).unwrap();
    for _ in 0..65 {
        hub.append("ExamplePlayer", "line").unwrap();
    }
    for seq in 1..=64 {
        assert!(slow
            .live
            .try_recv()
            .unwrap()
            .contains(&format!("id: {seq}\n")));
    }
    assert!(matches!(
        slow.live.try_recv(),
        Err(mpsc::TryRecvError::Disconnected)
    ));
    let mut resumed = hub.follow(Resume::After(64)).unwrap();
    assert!(resumed
        .backlog
        .next(&AtomicBool::new(false))
        .unwrap()
        .unwrap()
        .contains("id: 65\n"));
}

#[test]
fn restart_repairs_only_a_partial_tail_and_preserves_sequence() {
    let dir = TestDir::new();
    let path = dir.0.join("stream.jsonl");
    let hub = StreamHub::open(&path, "chat.jsonl").unwrap();
    hub.append("ExamplePlayer", "one").unwrap();
    drop(hub);
    OpenOptions::new()
        .append(true)
        .open(&path)
        .unwrap()
        .write_all(b"{partial")
        .unwrap();
    let hub = StreamHub::open(&path, "chat.jsonl").unwrap();
    assert_eq!(hub.append("ExamplePlayer", "two").unwrap(), 2);
    drop(hub);
    let original = fs::read(&path).unwrap();
    fs::write(&path, b"invalid complete row\n").unwrap();
    assert!(StreamHub::open(&path, "chat.jsonl").is_err());
    assert_eq!(fs::read(&path).unwrap(), b"invalid complete row\n");
    fs::write(&path, original).unwrap();
}

#[test]
fn invalid_and_future_cursors_reset_without_replaying_old_lines() {
    let dir = TestDir::new();
    let hub = StreamHub::open(&dir.0.join("stream.jsonl"), "chat.jsonl").unwrap();
    hub.append("ExamplePlayer", "one").unwrap();
    for cursor in [Resume::Reset, Resume::After(99)] {
        let mut follower = hub.follow(cursor).unwrap();
        assert!(follower.reset.as_ref().unwrap().contains("id: 1\n"));
        assert!(follower
            .backlog
            .next(&AtomicBool::new(false))
            .unwrap()
            .is_none());
    }
}

#[test]
fn sequence_exhaustion_and_oversize_rows_leave_spool_unchanged() {
    let dir = TestDir::new();
    let path = dir.0.join("stream.jsonl");
    let hub = StreamHub::open(&path, "chat.jsonl").unwrap();
    assert!(hub
        .append("ExamplePlayer", &"x".repeat(MAX_ROW_BYTES as usize))
        .is_err());
    hub.inner.lock().unwrap().next_seq = u64::MAX;
    assert!(hub.append("ExamplePlayer", "line").is_err());
    assert_eq!(fs::metadata(&path).unwrap().len(), 0);
}

#[test]
fn config_and_header_parser_reject_unusable_authentication() {
    for token in ["", "two words", "token\nheader"] {
        assert!(validate("127.0.0.1:0", "example", token, Path::new("stream.jsonl")).is_err());
    }
    let request = parse_request("GET /v1/streams/example HTTP/1.1\r\nAuthorization: Bearer wrong\r\n\r\nAuthorization: Bearer secret\r\n");
    assert_eq!(request.authorization.as_deref(), Some("wrong"));
}

#[test]
fn unauthenticated_connections_are_bounded_and_shutdown_releases_them() {
    let dir = TestDir::new();
    let hub = StreamHub::open(&dir.0.join("stream.jsonl"), "chat.jsonl").unwrap();
    let (addr, server) = serve("127.0.0.1:0", "myguild".into(), "secret".into(), hub).unwrap();
    let sockets: Vec<_> = (0..MAX_CLIENTS)
        .map(|_| TcpStream::connect(addr).unwrap())
        .collect();
    let deadline = Instant::now() + Duration::from_secs(2);
    while server.inflight.load(Ordering::Relaxed) < MAX_CLIENTS && Instant::now() < deadline {
        thread::sleep(Duration::from_millis(10));
    }
    assert_eq!(server.inflight.load(Ordering::Relaxed), MAX_CLIENTS);
    let mut extra = TcpStream::connect(addr).unwrap();
    extra
        .set_read_timeout(Some(Duration::from_secs(2)))
        .unwrap();
    let ended = extra.read(&mut [0]);
    assert!(
        matches!(ended, Ok(0)) || ended.is_err_and(|e| e.kind() == io::ErrorKind::ConnectionReset)
    );
    drop(sockets);
    let inflight = Arc::clone(&server.inflight);
    server.shutdown();
    assert_eq!(inflight.load(Ordering::Relaxed), 0);
}

#[test]
fn controls_are_sanitized_and_long_messages_are_not_streamed() {
    let record = guild_record("ExamplePlayer", "hello\n\rworld", 0, "2026-03-06T15:13:03Z");
    assert!(!eq_line(&record).unwrap().contains(['\n', '\r']));
    let record = guild_record(
        "ExamplePlayer",
        &"x".repeat(MAX_LINE_BYTES),
        0,
        "2026-03-06T15:13:03Z",
    );
    assert!(eq_line(&record).is_none());
}

fn guild_record(sender: &str, text: &str, channel: u32, timestamp: &str) -> Record {
    let mut body = vec![0_u8; 148];
    body[64..64 + sender.len()].copy_from_slice(sender.as_bytes());
    body[132..136].copy_from_slice(&channel.to_le_bytes());
    body.extend_from_slice(text.as_bytes());
    body.push(0);
    let event = chat::parse(0x1004, &body, false).unwrap().unwrap();
    Record::new(
        "Green",
        "ExamplePlayer",
        "ecommons",
        "sess",
        1,
        RecordEvent::Chat(event),
    )
    .with_timestamp(timestamp)
}

#[test]
fn guild_line_uses_eastern_stamp_and_other_channels_do_not_match_it() {
    let record = guild_record(
        "ExamplePlayer",
        "!tod Lord Nagafen",
        0,
        "2026-03-06T15:13:03Z",
    );
    let line = eq_line(&record).unwrap();
    assert_eq!(
        line,
        "[Fri Mar 06 10:13:03 2026] ExamplePlayer tells the guild, '!tod Lord Nagafen'"
    );

    let summer = guild_record("ExamplePlayer", "!bp naggy", 0, "2026-07-16T16:00:00Z");
    assert_eq!(
        eq_line(&summer).unwrap(),
        "[Thu Jul 16 12:00:00 2026] ExamplePlayer tells the guild, '!bp naggy'"
    );

    let say = guild_record("ExamplePlayer", "hello", 8, "2026-03-06T15:13:03Z");
    let say_line = eq_line(&say).unwrap();
    assert!(!say_line.contains("tells the guild"));
    assert!(say_line.contains("says, 'hello'"));
}

#[test]
fn live_stream_does_not_replay_and_resume_does() {
    let dir = std::env::temp_dir().join(format!("p99-stream-{}", std::process::id()));
    let _ = fs::remove_dir_all(&dir);
    fs::create_dir_all(&dir).unwrap();
    let spool = dir.join("stream.jsonl");
    let hub = StreamHub::open(&spool, "chat.jsonl").unwrap();
    let record = guild_record(
        "ExamplePlayer",
        "!tod Lord Nagafen",
        0,
        "2026-03-06T15:13:03Z",
    );
    let line = eq_line(&record).unwrap();
    hub.append("ExamplePlayer", &line).unwrap();

    let (addr, server) = serve(
        "127.0.0.1:0",
        "myguild".into(),
        "secret".into(),
        Arc::clone(&hub),
    )
    .unwrap();

    let mut live = request(addr, "secret", None);
    let live_headers = read_for(&mut live, Duration::from_secs(2));
    assert!(live_headers.contains("200 OK"));
    hub.append("ExamplePlayer", &line).unwrap();
    let live_body = read_for(&mut live, Duration::from_secs(2));
    assert!(!live_body.contains("\"seq\":1"));
    assert!(live_body.contains("\"seq\":2"));
    assert!(live_body.contains("event: line"));

    let mut resume = request(addr, "secret", Some("1"));
    let resume_body = read_for(&mut resume, Duration::from_secs(2));
    assert!(resume_body.contains("\"seq\":2"));
    assert!(!resume_body.contains("\"seq\":1"));

    let mut denied = request(addr, "nope", None);
    let denied_body = read_for(&mut denied, Duration::from_secs(1));
    assert!(denied_body.contains("401"));
    assert!(denied_body.contains("unauthorized"));
    assert!(!denied_body.contains("event: line"));

    server.shutdown();
    let _ = fs::remove_dir_all(&dir);
}

fn request(addr: SocketAddr, token: &str, last_event_id: Option<&str>) -> TcpStream {
    let mut socket = TcpStream::connect(addr).unwrap();
    socket
        .set_read_timeout(Some(Duration::from_secs(2)))
        .unwrap();
    let cursor = last_event_id
        .map(|id| format!("Last-Event-ID: {id}\r\n"))
        .unwrap_or_default();
    write!(
        socket,
        "GET /v1/streams/myguild HTTP/1.1\r\nHost: localhost\r\nAuthorization: Bearer {token}\r\n{cursor}Accept: text/event-stream\r\n\r\n"
    )
    .unwrap();
    socket
}

fn read_for(socket: &mut TcpStream, budget: Duration) -> String {
    let deadline = Instant::now() + budget;
    let mut buf = Vec::new();
    let mut tmp = [0_u8; 1024];
    while Instant::now() < deadline {
        match socket.read(&mut tmp) {
            Ok(0) => break,
            Ok(n) => buf.extend_from_slice(&tmp[..n]),
            Err(error)
                if error.kind() == io::ErrorKind::WouldBlock
                    || error.kind() == io::ErrorKind::TimedOut =>
            {
                if buf.windows(10).any(|window| window == b"event: line")
                    || buf.windows(3).any(|window| window == b"401")
                {
                    break;
                }
                thread::sleep(Duration::from_millis(20));
            }
            Err(_) => break,
        }
    }
    String::from_utf8_lossy(&buf).into_owned()
}
