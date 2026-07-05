# Tutorial — your first replication

By the end you'll have file-replicator watching a directory, and you'll drop a file into it and watch it be
copied to a destination directory, verified, and the source archived — plus see the granular event stream
and query live status on demand. No cloud account required: this uses the built-in `local` destination.

## 1. Prerequisites

- A Rust toolchain (stable) — build from the repo root with `cargo`.
- A local MQTT broker on `localhost:1883` for the event/command plane
  (`docker run -d -p 1883:1883 emqx/emqx`). Replication itself is filesystem work and would run without a
  broker, but the broker is what lets you *watch* events and call `get-status` in steps 5–6.

The default build features are `standalone` + `dest-s3`; the `local` destination is always compiled in, so
a plain `cargo run` is all you need for this walkthrough (no destination feature flags).

## 2. Create the spool directories

The bundled `test-configs/config.json` defines one instance, `spool-to-archive`, with relative paths under
`./_spool`. Create them from the repo root:

```bash
mkdir -p _spool/in _spool/out _spool/archive
```

That instance watches `_spool/in` recursively, replicates each ready file to the `local` destination
`_spool/out` (with `fsync`), verifies by checksum, and on success **archives** the source into
`_spool/archive`.

## 3. Run the component

```bash
# HOST platform, MQTT transport (local broker), FILE config source, ThingName "my-thing":
cargo run -- \
  --platform HOST --transport MQTT ./test-configs/standalone-messaging.json \
  -c FILE ./test-configs/config.json \
  -t my-thing
```

You should see it parse the instance (`spool-to-archive`), log `OS file watch active` for `_spool/in` (the
low-latency discovery path), and settle into watching. It is now idle only because `_spool/in` is empty.

## 4. Drop a file and watch it replicate

In another shell, from the repo root, write a file into the watched directory:

```bash
echo "hello file-replicator" > _spool/in/report.txt
```

Within a second or two (the default `stability` readiness strategy waits `quietSecs: 5` of no
size/mtime change to be sure the file is fully written), the file is judged **ready**, replicated, and
verified. Confirm:

```bash
ls _spool/out          # report.txt now here (the replicated copy)
ls _spool/in           # empty — the source was moved out on success
ls _spool/archive      # report.txt here (the archived source)
```

That is the whole engine end to end: **discover → ready → replicate → verify → complete** (here,
`onSuccess: archive`). Try a subtree — `mkdir _spool/in/sub && echo x > _spool/in/sub/a.txt` — and note the
`sub/` path is preserved under `_spool/out` because `ingress.recursive` is `true`.

## 5. Watch the event stream

Every lifecycle step is published on the UNS `evt` class. Subscribe with any MQTT client — one wildcard
covers every instance of every device:

```bash
mosquitto_sub -t 'ecv1/+/FileReplicator/+/evt/#' -v
```

Drop another file and you'll see a sequence like:

```
ecv1/my-thing/FileReplicator/spool-to-archive/evt/info/file-ready              {"severity":"info","type":"file-ready","context":{"path":"report2.txt","size":22}, ...}
ecv1/my-thing/FileReplicator/spool-to-archive/evt/info/replication-started     { ... "context":{"path":"report2.txt","destination":"local","attempt":1, ...} }
ecv1/my-thing/FileReplicator/spool-to-archive/evt/info/replication-completed   { ... "context":{"path":"report2.txt","destination":"local","bytes":22, ...} }
ecv1/my-thing/FileReplicator/spool-to-archive/evt/info/file-archived           { ... "context":{"path":"report2.txt","archivePath":"..."} }
```

The channel (`evt/{severity}/{type}`) is derived from the event body, so the topic and body can never
disagree. The full catalog — every `type`, its severity, and its `context` fields — is in the
[messaging interface reference](reference/messaging-interface.md) and
[data-types reference](reference/data-types.md).

## 6. Ask for status on demand

There is no retained state snapshot; the current picture is a request/reply command. Publish to the `main`
command inbox with a `reply_to` you subscribe:

```bash
# subscribe to your reply topic first, then publish the request:
mosquitto_pub -t 'ecv1/my-thing/FileReplicator/main/cmd/get-status' \
  -m '{"header":{"name":"get-status","reply_to":"app/r","correlation_id":"1"},"body":{}}'
```

The reply on `app/r` is `{"ok":true,"result": …}` where `result` is the component roster + summary. Add
`"body":{"instance":"spool-to-archive"}` to get that one instance's document
(`awaiting`/`inProgress`/`replicated`/`failed` tallies). Both shapes are specified in the
[data-types reference](reference/data-types.md).

## 7. Where to next

- Change the destination to S3, SFTP, HTTP, Azure, or GCS — [How-to guides](how-to-guides.md) and
  [Sample configurations](sample-configurations.md).
- Gate replication to a nightly window, cap bandwidth, or quarantine failures — [How-to guides](how-to-guides.md).
- Understand *why* discovery, readiness, durability, and scheduling behave as they do —
  [Explanation](explanation.md).
- Look up every field — [Reference › Configuration](reference/configuration.md).
