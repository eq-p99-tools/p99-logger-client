# p99-logger-client

A native Rust client that logs into an EverQuest server, enters an existing
character, keeps the character at its saved position, and writes received
communications as JSONL. It runs headlessly without Wine or the EverQuest
executable.

The client supports the Titanium/P99 V62 protocol and Project Quarm's
Windows TAKP/EQMac protocol. P99
Green has live coverage for auction and OOC messages, including complete item
links. Quarm login, zone entry, received chat, and outbound tells have been
exercised through the Android client. A live retest also confirmed that the DLL
version announcement removes the outdated-client warning. See
[the Quarm protocol notes](docs/quarm-protocol.md) for the packet flow and
validation limits.

## Configuration

Copy [config.example.json](config.example.json) to a private location and set
the login account, password, exact server-list name, and existing character.
Keep the configuration and output private. The included Compose setup uses
`.local/`, which Git and Docker both exclude.

`protocol` selects the client family and its default login endpoint. It may be
omitted for existing P99 configurations.

| `protocol` | Default login endpoint | Status |
| --- | --- | --- |
| `project1999` | `login.eqemulator.net:5998` | Live tested |
| `quarm` | `loginserver.takproject.net:6000` | Live login, chat, and version announcement |

The public P99 server-list names are:

| Server | `server` value |
| --- | --- |
| Green | `Project 1999: Green (Velious, PvE)` |
| Blue | `Project 1999: Blue (Velious, PvE)` |

The only required values are the login account, password, server, and
character. A fictional Blue configuration is:

```json
{
  "user": "EXAMPLE_LOGIN_ACCOUNT",
  "pass": "EXAMPLE_PASSWORD",
  "server": "Project 1999: Blue (Velious, PvE)",
  "character": "ExampleCharacter"
}
```

The account and character must already exist; this client does not create or
modify them. The `server` value is matched case-insensitively against the name
returned by the live server list, so retain its punctuation and spacing.

A fictional Quarm configuration is:

```json
{
  "protocol": "quarm",
  "user": "EXAMPLE_TAKP_LOGIN_ACCOUNT",
  "pass": "EXAMPLE_PASSWORD",
  "server": "The Project Quarm Server Server",
  "character": "ExampleCharacter"
}
```

The repeated `Server` in Quarm's name is intentional: the TAKP login service
appends ` Server` to the registered world name. Use the complete server-list name.

The optional fields are `protocol` (default `project1999`), `host`, `port`,
`assets`, `output`, `health`, `channels`, `include_raw` (default `false`),
`reconnect_seconds` (default `30`), and `stream`. `host` and `port` override the
selected protocol's endpoint. With no `output`, JSONL is written only to
standard output. Omitting `stream` leaves the process exactly as a JSONL logger:
nothing listens, and no second file is written.

For P99, omitting `assets` uses the inventory compiled into the binary; set it
to a readable path to override that inventory. Quarm's implemented handshake
does not use this inventory. The example supplies the fixed `output` and
`health` paths used by Compose, so P99 users still edit only the four required
values.

Omit `channels` to log every decoded communication category. Set it to a list
to keep only selected categories. These examples are equivalent; the short
names match the familiar EverQuest chat commands:

```json
"channels": ["auction", "ooc"]
```

```json
"channels": ["auc", "ooc"]
```

To collect only guild chat, use `"channels": ["gu"]`. The canonical names are
`guild`, `group`, `shout`, `auction`, `ooc`, `broadcast`, `tell`, `say`,
`gm_say`, `raid`, `emote`, `motd`, `system`, `guild_motd`, and `unknown`.
Filtering applies to decoded chat records; malformed recognized packets remain
visible as `decode_error` records.

For P99, the repository and published image include checksums for the installed zone
and model archives, along with the world validation files. The server supplies
a manifest of filenames during world login and zone handoff; the client looks
up and sends only the requested checksums. No zone selection is needed in the
configuration.

To refresh the inventory after a P99 patch or use a different installation,
generate it from the current game files; the files are not needed afterward:

```sh
mkdir -p .local/collector/config .local/collector/data
cp config.example.json .local/collector/config/config.json
chmod 600 .local/collector/config/config.json

docker run --rm --network none --user "$(id -u):$(id -g)" \
  --mount "type=bind,source=/absolute/path/to/EverQuest,target=/eq,readonly" \
  --mount "type=bind,source=$(pwd)/.local/collector/config,target=/out" \
  ghcr.io/rm-you/p99-logger-client:latest \
  scan-assets /eq /out/assets.json
```

Then add `"assets": "/config/assets.json"` to the private configuration. The
Compose configuration already mounts that directory read-only at `/config`.

By default, `scan-assets` inventories the known validation files, all installed
`.s3d` and `.eqg` archives, and zone `_chr.txt` and `_assets.txt` lists. Account
settings, character settings, and chat logs are not scanned. Only filenames,
sizes, and CRC32 values for measured files are stored; absent files are omitted.

To scan an exact set of files instead, pass a text file containing their names
as the final `scan-assets` argument. For an unrecognized filename in a server
manifest, the client emits a warning and sends checksum `0`, letting the server
decide whether to accept it. Older inventories containing explicit `null`
entries remain supported and also send `0`. If the server requires the real
checksum, refresh the inventory; retrying alone cannot fix it. Malformed
manifests still fail validation. The expanded inventory covers installed
assets, but live zone entry has only been verified in East Commonlands.

## Run with Compose

```sh
P99_UID=$(id -u) P99_GID=$(id -g) docker compose up -d
tail -f .local/collector/data/chat.jsonl
docker compose ps
docker compose logs --tail 30 collector
```

The container runs as the configured unprivileged UID/GID, drops all Linux
capabilities, uses a read-only root filesystem and configuration mount, and
appends JSONL to `.local/collector/data/chat.jsonl`. Its health check requires
completed zone entry and recent valid server traffic.

On connection loss, the process reports the failure and repeats the complete
login flow after `reconnect_seconds`, deriving fresh keys for every session.
SIGTERM closes the active session. Docker's `unless-stopped` policy restarts a
process that exits. The JSONL file is append-only and needs an external
retention policy.

## Optional log stream

Set `stream` to also serve new records to external consumers over server-sent events.
The JSONL file named by `output` is still written first, with the same bytes as
before. The stream does not read or rewrite that file. It appends a separate
spool and only starts when this object is present.

```json
"stream": {
  "listen": "0.0.0.0:8090",
  "stream_id": "myguild",
  "token": "choose-a-long-secret",
  "spool": "/data/stream.jsonl"
}
```

`GET /v1/streams/myguild` with `Authorization: Bearer <token>` returns
`text/event-stream`. No `Last-Event-ID` follows only records that arrive after
the connection. A numeric `Last-Event-ID` replays later spool rows, then
follows. The token is never written to the JSONL dump or logged.

Each `event: line` has an SSE `id` and JSON data containing `seq`, `file`
(the output basename, or `stdout`), `character`, and the synthesized `line`.
Malformed or future cursors receive `event: reset` with `seq` and `reason`,
then follow live. Save the last processed ID and send it on reconnect. A slow
consumer is disconnected when its 64-record queue fills and can resume from
the spool. Idle connections receive a comment heartbeat every 15 seconds.
There are at most 32 connected clients, including requests awaiting authentication.
Replay reads a fixed file snapshot outside the chat-writer lock with bounded
memory, then continues with queued live records.

Guild chat is sent as an EQ log line whose clock is the record's UTC receive
time converted to US Eastern, for example
`[Fri Mar 06 10:13:03 2026] ExamplePlayer tells the guild, '!tod Lord Nagafen'`. Other
channels are sent too, but not in that guild form. `decode_error` rows stay in
the JSONL dump and are left out of the stream.

Publish the listen port yourself when another host must connect. Compose does
not publish it unless you add that mapping. A read-only container can listen;
the spool path has to be on the writable data volume, next to `chat.jsonl`.
Keep the spool as long as you keep the dump. Do not truncate it: sequence
numbers are not reused.

Use a random, unguessable token (for example 32 random bytes encoded as hex).
Tokens must be visible ASCII without spaces, at most 1024 bytes. The endpoint
uses plain HTTP: keep it on a private container network or bind the published
port to `127.0.0.1`; use a TLS reverse proxy for access across an untrusted network.
The spool must be a different file from the JSONL output, health file and config,
including symlink and hard-link aliases. An incomplete final spool row is removed
on startup; malformed complete rows fail startup. Writes are flushed, not fsynced:
this is restart replay, not a power-loss durability guarantee. Spool write errors
are reported through the same failure path as output errors.

EverQuest may temporarily deny a new world entry when the previous session did
not exit cleanly. The collector treats that response as a connection failure
and keeps retrying; it does not reuse the rejected session's keys.

## JSONL records

Each record includes a UTC timestamp, server, character, zone, local session
ID, message ID, numeric channel, channel name, sender, nonempty target, and
original opcode. Successfully decoded records contain readable text without
duplicating the original packet and message bytes. Set `include_raw` to `true`
when collecting reverse-engineering data to add `payload_hex`, `message`, and
`message_hex`.

Item links retain their complete protocol body (45 characters on P99 and 7 on
Quarm), label, item ID, wire byte offsets, and decoded-text byte offsets. Empty
item-link arrays are omitted.
MOTD, guild MOTD, emotes, special messages, and string-table messages are also
logged. Unknown channel IDs are preserved rather than dropped. Malformed recognized communication
packets always produce `decode_error` records with their original bytes.

On P99, auction and OOC have live native coverage. Guild, group, shout, tell, say,
raid, broadcast, GM-say, emote, and unknown channel IDs share the tested
channel decoder but have not all been exercised live. Quarm channel, MOTD,
guild-MOTD, emote, special-message, and formatted-message layouts have offline
tests against structures from the Quarm server source. Live Quarm testing has
exercised chat, tells, and system messages, but not every communication category.

### Item-link positions

Each item link includes `text_start` and `text_end`: the inclusive start and
exclusive end in UTF-8 bytes of its label in the decoded `text` string. These
ranges remain correct when non-UTF-8 wire bytes become replacement characters.
JavaScript consumers must convert UTF-8 byte positions before slicing strings.
For formatted game messages, ranges refer to the individual argument's `text`.

The existing `start` and `end` fields retain their original wire-byte meaning,
including link metadata and delimiters. Use the new fields to display inline
links; do not apply wire offsets to decoded text. All item-link fields remain
available when raw packet logging is disabled.

## Shared networking library

The login, world, zone, transport, chat, command, and validation code lives in
[`eq-network`](https://github.com/eq-p99-tools/eq-network). This repository is a
thin application adapter for JSON configuration, JSONL output, process signals,
health files, and the container image.

Native applications should depend on `eq-network` directly. Its public API
provides `Client`, `ClientConfig`, typed events and commands, structured chat and
item links, cooperative cancellation, and the bundled P99 checksum inventory.
Protocol tools can depend on its smaller transport, login, or game-codec crates.
The shared repository documents how movement, zoning, and later client actions
fit into the command and session architecture.

## Build and release

```sh
docker build -t p99-logger-client .
```

The Docker build checks formatting, runs Rust tests and Clippy, and creates a
stripped static binary in a `scratch` image.
The GitHub Actions workflow builds `linux/amd64`. Pull requests build without
publishing. Merges to `main`, version tags, and manual runs publish
provenance/SBOM-enabled images to
`ghcr.io/rm-you/p99-logger-client`, tagged by commit; the default branch also
publishes `latest`.

The application pins the reviewed `eq-network` commit until the initial crates.io
release is published.

### Login failures

A confirmed account/password rejection stops the client immediately, even with
reconnection enabled. Correct the credentials before starting another session.
Transient connection failures retain the normal retry behavior. Native callers
can downcast the error from `Client::run` to `client::LoginError::InvalidCredentials`;
this uses the bad-password response detector in the shared SSO login crate.

## License

Licensed under the [MIT License](LICENSE).
