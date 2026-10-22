mod stream;

use anyhow::{ensure, Context, Result};
use eq_network::{
    assets::Assets,
    chat,
    client::{
        CancellationToken, Client, ClientConfig, ClientEvent, ClientIdentity, ConnectionState,
        Record, RunOptions, ServerProtocol, SessionStatus,
    },
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{
    collections::HashSet,
    fs,
    io::{self, Write},
    path::{Path, PathBuf},
    sync::{atomic::AtomicBool, Arc},
    time::Duration,
};
use zeroize::Zeroizing;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Config {
    #[serde(default)]
    protocol: ServerProtocol,
    #[serde(default)]
    host: Option<String>,
    #[serde(default)]
    port: Option<u16>,
    user: Zeroizing<String>,
    pass: Zeroizing<String>,
    server: String,
    character: String,
    #[serde(default)]
    assets: Option<PathBuf>,
    #[serde(default)]
    output: Option<PathBuf>,
    #[serde(default)]
    health: Option<PathBuf>,
    #[serde(default)]
    include_raw: bool,
    #[serde(default)]
    channels: Option<HashSet<chat::ChannelName>>,
    #[serde(default = "default_reconnect")]
    reconnect_seconds: u64,
    /// Optional SSE follower. Absent means the process only writes the JSONL dump.
    #[serde(default)]
    stream: Option<StreamConfig>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct StreamConfig {
    listen: String,
    stream_id: String,
    token: Zeroizing<String>,
    spool: PathBuf,
}

impl Config {
    /// Translate the existing flat JSON settings into the embeddable engine config.
    fn client_config(&self) -> ClientConfig {
        let mut config = ClientConfig::for_protocol(
            self.protocol,
            self.user.as_str(),
            self.pass.as_str(),
            &self.server,
            &self.character,
        );
        if let Some(host) = &self.host {
            config.host.clone_from(host);
        }
        if let Some(port) = self.port {
            config.port = port;
        }
        config.include_raw = self.include_raw;
        config.channels.clone_from(&self.channels);
        config.reconnect_delay = Duration::from_secs(self.reconnect_seconds);
        config
    }
}
const fn default_reconnect() -> u64 {
    30
}

/// Load a caller-supplied checksum inventory or the one compiled into the client.
fn load_assets(path: Option<&Path>) -> Result<Assets> {
    match path {
        Some(path) => serde_json::from_slice(&fs::read(path).context("read asset inventory")?)
            .context("parse asset inventory"),
        None => Assets::bundled(),
    }
}

#[cfg(test)]
mod config_tests {
    use super::*;

    #[test]
    fn connection_defaults_require_only_account_server_and_character() {
        let config: Config = serde_json::from_str(
            r#"{
                "user": "EXAMPLE_LOGIN_ACCOUNT",
                "pass": "EXAMPLE_PASSWORD",
                "server": "Project 1999: Green (Velious, PvE)",
                "character": "ExampleCharacter"
            }"#,
        )
        .unwrap();

        assert_eq!(config.protocol, ServerProtocol::Project1999);
        assert_eq!(config.client_config().host, "login.eqemulator.net");
        assert_eq!(config.client_config().port, 5998);
        assert_eq!(config.assets, None);
        assert_eq!(config.reconnect_seconds, 30);
        assert_eq!(config.output, None);
        assert_eq!(config.health, None);
        assert!(config.stream.is_none());
        assert!(!config.include_raw);
        assert_eq!(config.channels, None);
    }

    #[test]
    fn quarm_selects_the_takp_login_endpoint_unless_overridden() {
        let config: Config = serde_json::from_str(
            r#"{
                "protocol": "quarm",
                "user": "EXAMPLE_LOGIN_ACCOUNT",
                "pass": "EXAMPLE_PASSWORD",
                "server": "The Project Quarm Server",
                "character": "ExampleCharacter"
            }"#,
        )
        .unwrap();
        let client = config.client_config();
        assert_eq!(client.protocol, ServerProtocol::Quarm);
        assert_eq!(client.host, "loginserver.takproject.net");
        assert_eq!(client.port, 6000);
    }

    #[test]
    fn built_in_inventory_covers_multiple_zones_and_an_asset_path_overrides_it() {
        let assets = load_assets(None).unwrap();
        assert!(assets.spells().is_ok());
        for zone in ["ecommons", "qeynos", "freportw", "sebilis", "velketor"] {
            assert!(assets.files[&format!("{zone}.s3d")].is_some());
        }
        assert!(assets.files.values().all(Option::is_some));

        let config: Config = serde_json::from_str(
            r#"{
                "user": "EXAMPLE_LOGIN_ACCOUNT",
                "pass": "EXAMPLE_PASSWORD",
                "server": "Project 1999: Green (Velious, PvE)",
                "character": "ExampleCharacter",
                "assets": "/custom/assets.json"
            }"#,
        )
        .unwrap();
        assert_eq!(config.assets, Some(PathBuf::from("/custom/assets.json")));
    }

    #[test]
    fn raw_packet_logging_is_opt_in() {
        let config: Config = serde_json::from_str(
            r#"{
                "user": "EXAMPLE_LOGIN_ACCOUNT",
                "pass": "EXAMPLE_PASSWORD",
                "server": "Project 1999: Blue (Velious, PvE)",
                "character": "ExampleCharacter",
                "include_raw": true
            }"#,
        )
        .unwrap();

        assert!(config.include_raw);
    }

    #[test]
    fn channel_filter_accepts_canonical_names_and_chat_aliases() {
        let config: Config = serde_json::from_str(
            r#"{
                "user": "EXAMPLE_LOGIN_ACCOUNT",
                "pass": "EXAMPLE_PASSWORD",
                "server": "Project 1999: Blue (Velious, PvE)",
                "character": "ExampleCharacter",
                "channels": ["auc", "ooc", "gu"]
            }"#,
        )
        .unwrap();
        let channels = config.channels.as_ref().unwrap();

        assert_eq!(channels.len(), 3);
        assert!(channels.contains(&chat::ChannelName::Auction));
        assert!(channels.contains(&chat::ChannelName::Ooc));
        assert!(channels.contains(&chat::ChannelName::Guild));

        let event = |channel| {
            let mut body = vec![0; 148];
            body[132..136].copy_from_slice(&u32::to_le_bytes(channel));
            body.push(0);
            chat::parse(0x1004, &body, false).unwrap().unwrap()
        };
        assert!(config.client_config().logs(&event(4)));
        assert!(!config.client_config().logs(&event(8)));
    }

    #[test]
    fn jsonl_dump_bytes_stay_the_same_when_streaming() {
        use eq_network::chat;
        use eq_network::client::RecordEvent;

        let dir = std::env::temp_dir().join(format!("p99-jsonl-{}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        let output = dir.join("chat.jsonl");
        let spool = dir.join("stream.jsonl");
        let config: Config = serde_json::from_str(&format!(
            r#"{{
                "user": "EXAMPLE_LOGIN_ACCOUNT",
                "pass": "EXAMPLE_PASSWORD",
                "server": "Project 1999: Green (Velious, PvE)",
                "character": "ExamplePlayer",
                "output": "{}",
                "stream": {{
                    "listen": "127.0.0.1:0",
                    "stream_id": "myguild",
                    "token": "secret",
                    "spool": "{}"
                }}
            }}"#,
            output.display().to_string().replace('\\', "\\\\"),
            spool.display().to_string().replace('\\', "\\\\")
        ))
        .unwrap();
        let hub = stream::StreamHub::open(&spool, "chat.jsonl").unwrap();
        let mut log = ChatLog::new(&config, Some(hub)).unwrap();
        let mut body = vec![0_u8; 148];
        body[64..77].copy_from_slice(b"ExamplePlayer");
        body.extend_from_slice(b"!tod Lord Nagafen\0");
        let event = chat::parse(0x1004, &body, false).unwrap().unwrap();
        let record = Record::new(
            "Green",
            "ExamplePlayer",
            "ecommons",
            "sess",
            1,
            RecordEvent::Chat(event),
        )
        .with_timestamp("2026-03-06T15:13:03Z");
        log.emit(&record).unwrap();

        let dumped = fs::read(&output).unwrap();
        let mut expected = serde_json::to_vec(&record).unwrap();
        expected.push(b'\n');
        assert_eq!(dumped, expected);
        assert!(!String::from_utf8_lossy(&dumped).contains("\"seq\""));
        assert!(fs::read_to_string(&spool).unwrap().contains("\"seq\":1"));
        let _ = fs::remove_dir_all(&dir);
    }
}

/// The CLI adapter writes the library's flat records to stdout and an optional file.
///
/// `stream` is a separate spool. It is not consulted while the JSONL bytes are produced.
struct ChatLog {
    file: Option<fs::File>,
    stream: Option<Arc<stream::StreamHub>>,
}

impl ChatLog {
    fn new(config: &Config, hub: Option<Arc<stream::StreamHub>>) -> Result<Self> {
        let file = config
            .output
            .as_ref()
            .map(|path| fs::OpenOptions::new().create(true).append(true).open(path))
            .transpose()
            .context("open JSONL output")?;
        Ok(Self { file, stream: hub })
    }

    fn emit(&mut self, record: &Record) -> Result<()> {
        let mut bytes = serde_json::to_vec(record)?;
        bytes.push(b'\n');
        if let Some(file) = &mut self.file {
            file.write_all(&bytes)?;
        }
        io::stdout().lock().write_all(&bytes)?;
        if let Some(hub) = &self.stream {
            if let Some(line) = stream::eq_line(record) {
                hub.append(&record.character, &line)?;
            }
        }
        Ok(())
    }
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct HealthStatus {
    state: ConnectionState,
    timestamp: i64,
    messages: u64,
    packets: u64,
    last_received_seconds: Option<u64>,
}

/// Atomically replace the CLI health file from a library status event.
fn health(path: &Path, status: &SessionStatus) -> Result<()> {
    let value = HealthStatus {
        state: status.state,
        timestamp: status.timestamp,
        messages: status.messages,
        packets: status.packets,
        last_received_seconds: status.last_received_seconds,
    };
    let mut temporary = path.as_os_str().to_owned();
    temporary.push(".tmp");
    let temporary = PathBuf::from(temporary);
    fs::write(&temporary, serde_json::to_vec(&value)?)?;
    fs::rename(temporary, path)?;
    Ok(())
}

#[derive(Deserialize)]
struct CapturedPacket {
    direction: String,
    opcode: u16,
    payload_hex: String,
    time: Value,
}

#[derive(Serialize)]
struct TimestampedEvent<'a, T> {
    timestamp: &'a Value,
    #[serde(flatten)]
    event: &'a T,
}

fn main() -> Result<()> {
    let args: Vec<String> = std::env::args().collect();
    match args.get(1).map(String::as_str) {
        Some("health") => {
            let path = args.get(2).context("usage: health STATUS_JSON")?;
            let status: HealthStatus = serde_json::from_slice(&fs::read(path)?)?;
            let age = chrono::Utc::now().timestamp() - status.timestamp;
            ensure!(
                status.state == ConnectionState::Connected
                    && (0..=60).contains(&age)
                    && status.last_received_seconds.is_some_and(|seconds| {
                        seconds
                            .checked_add(age.cast_unsigned())
                            .is_some_and(|total| total < 60)
                    }),
                "collector is not connected"
            );
            return Ok(());
        }
        Some("scan-assets") => {
            ensure!(
                args.len() == 4 || args.len() == 5,
                "usage: scan-assets INSTALL OUTPUT [FILE_LIST]"
            );
            let directory = Path::new(&args[2]);
            let assets = if let Some(path) = args.get(4) {
                let names: Vec<_> = fs::read_to_string(path)?
                    .lines()
                    .map(str::trim)
                    .filter(|name| !name.is_empty())
                    .map(str::to_owned)
                    .collect();
                Assets::scan(directory, &names)?
            } else {
                Assets::scan_all(directory)?
            };
            fs::write(&args[3], serde_json::to_vec_pretty(&assets)?)?;
            eprintln!("Inventoried {} files", assets.files.len());
            return Ok(());
        }
        Some("decode-events") => {
            ensure!(
                args.len() == 3 || args.len() == 4,
                "usage: decode-events APPLICATION_PACKETS_JSON [p99|quarm]"
            );
            let protocol: ServerProtocol = args
                .get(3)
                .map(|value| value.parse())
                .transpose()?
                .unwrap_or_default();
            let records: Vec<CapturedPacket> = serde_json::from_slice(&fs::read(&args[2])?)?;
            let mut output = io::BufWriter::new(io::stdout().lock());
            for record in records {
                if record.direction != "server" {
                    continue;
                }
                let body = hex::decode(&record.payload_hex)?;
                if let Some(event) = chat::parse_for(protocol.into(), record.opcode, &body, true)? {
                    serde_json::to_writer(
                        &mut output,
                        &TimestampedEvent {
                            timestamp: &record.time,
                            event: &event,
                        },
                    )?;
                    writeln!(output)?;
                }
            }
            return Ok(());
        }
        _ => (),
    }
    ensure!(
        args.len() >= 2,
        "usage: p99-logger-client CONFIG [--world-only]"
    );
    let config: Config = serde_json::from_slice(&fs::read(&args[1])?)?;
    let identity = ClientIdentity::new(
        fs::read_to_string("/etc/hostname")
            .context("read container hostname")?
            .trim()
            .to_owned(),
        std::env::var("USER").unwrap_or_else(|_| "nobody".into()),
    );
    let mut client = Client::new(config.client_config(), identity)?;
    if config.assets.is_some() {
        client = client.with_assets(load_assets(config.assets.as_deref())?)?;
    }
    let stop = Arc::new(AtomicBool::new(false));
    signal_hook::flag::register(signal_hook::consts::SIGINT, Arc::clone(&stop))?;
    signal_hook::flag::register(signal_hook::consts::SIGTERM, Arc::clone(&stop))?;
    let zone_duration = args
        .windows(2)
        .find(|args| args[0] == "--duration")
        .map(|args| args[1].parse::<u64>())
        .transpose()?
        .filter(|seconds| *seconds > 0)
        .map(Duration::from_secs);
    let world_only = args.iter().any(|arg| arg == "--world-only");
    let mut options = RunOptions::default();
    options.reconnect = zone_duration.is_none() && !world_only;
    options.world_only = world_only;
    options.zone_duration = zone_duration;
    let stream_server = match &config.stream {
        Some(stream_config) => {
            stream::validate(
                &stream_config.listen,
                &stream_config.stream_id,
                stream_config.token.as_str(),
                &stream_config.spool,
            )?;
            for path in config.output.iter().chain(config.health.iter()) {
                stream::require_distinct(&stream_config.spool, path)?;
            }
            stream::require_distinct(&stream_config.spool, Path::new(&args[1]))?;
            let file_name = config
                .output
                .as_ref()
                .and_then(|path| path.file_name())
                .and_then(|name| name.to_str())
                .unwrap_or("stdout");
            let hub = stream::StreamHub::open(&stream_config.spool, file_name)?;
            let (_addr, handle) = stream::serve(
                &stream_config.listen,
                stream_config.stream_id.clone(),
                stream_config.token.as_str().to_owned(),
                Arc::clone(&hub),
            )?;
            Some((hub, handle))
        }
        None => None,
    };
    let mut log = ChatLog::new(
        &config,
        stream_server.as_ref().map(|(hub, _)| Arc::clone(hub)),
    )?;
    let run_result = client.run(
        &CancellationToken::from(stop),
        options,
        |event| match event {
            ClientEvent::Record(record) => log.emit(&record),
            ClientEvent::Progress(_) => Ok(()),
            ClientEvent::Status(status) => {
                if let Some(path) = &config.health {
                    health(path, &status)?;
                }
                Ok(())
            }
            ClientEvent::Diagnostic(message) => {
                eprintln!("{message}");
                Ok(())
            }
            ClientEvent::Reconnecting {
                error,
                delay_seconds,
            } => {
                eprintln!("Connection ended: {error}. Reconnecting in {delay_seconds} seconds");
                Ok(())
            }
            _ => Ok(()),
        },
    );
    if let Some((_, handle)) = stream_server {
        handle.shutdown();
    }
    run_result
}
