# Reference — messaging interface (UNS)

All command/event topics ride the edgecommons **Unified Namespace** core (`gg.commands()` / `gg.events()` /
the automatic `state`/`cfg` keepalive) — minted by the library, not a hand-rolled topic builder. Normal
EdgeCommons messages on these topics are protobuf-encoded `EdgeCommonsMessage` payload bytes; the JSON
objects below are the decoded body/diagnostic shapes that component handlers and client APIs work with.
Full rationale in [`DESIGN.md`](https://github.com/edgecommons/file-replicator/blob/main/DESIGN.md); this
page and the `crate::events`/`crate::control` module docs are the source of truth for the message contract.

```
ecv1/{device}/{component}[/{instance}]/{class}[/{channel…}]
```

- `{device}` — the resolved ThingName (`-t`/`--thing`).
- `{component}` — the resolved UNS token. The supplied configuration sets `component.token` to
  `file-replicator`; the examples below use that token. Match the resolved token in your deployment
  if you supply a different configuration.
- `{instance}` — OPTIONAL. Present (a `component.instances[].id`) for instance-scoped traffic — a
  replication instance's own events (`file-ready`, `replication-*`, …) ride `gg.instance(id).events()`,
  and a command addressed at one instance rides that instance's own command inbox.
  **Absent** for component-scope traffic: a command addressed at the whole component, `ComponentReady`,
  the scope-`"all"` `trigger`/`get-status` events, and the library's own `state`/`cfg`/`metric`
  keepalives.
- `{class}` ∈ `cmd` (inbound commands, request/reply) · `evt` (event stream) · the **reserved**,
  library-owned `state`/`cfg`/`metric`/`log` (this component never publishes to them directly).

There is no configurable topic prefix and no legacy alias — the UNS grammar above is fixed, with no
`component.global.topics.prefix`/`legacyConfigTopic` knobs.

## Commands (`cmd`, request/reply via `reply_to`)

Two inboxes carry commands: the component one, `ecv1/{device}/file-replicator/cmd/#`, and one per
replication instance, `ecv1/{device}/file-replicator/{instance}/cmd/#`. Publish commands with the
edgecommons client APIs (`MessageBuilder` + `MessagingService` request/reply, or an equivalent protobuf
producer), not by sending JSON text to MQTT. Every decoded reply body is
`{"ok": true, "result": <value>}` or `{"ok": false, "error": {"code", "message"}}` (the edgecommons
command-inbox contract — the request's `header.name` MUST equal the verb).

| Verb | Scope | Body | Result / error codes |
|---|---|---|---|
| `get-status` | both | `{ "instance"?: string }` | no instance named → component-wide roster+summary; one named → that instance's document; `UNKNOWN_INSTANCE` |
| `trigger` | both | `{ "instance"?: string, "ignoreWindow"?: bool }` | accepted + counts; `UNKNOWN_INSTANCE` |
| `set-activation` | instance | `{ "instance": string, "active"?: bool, "persist"?: bool, "reset"?: bool }` | new state; `INSTANCE_REQUIRED` (no "all" form), `UNKNOWN_INSTANCE`, `INVALID_REQUEST` (neither `active` nor `reset`), `ACTIVATION_FAILED` |

### Addressing an instance

Each verb declares an addressing **scope**, which the library enforces before the verb runs:

- **`both`** (`get-status`, `trigger`) — either inbox is meaningful. Address the component
  (`…/file-replicator/cmd/{verb}`) for the fleet-wide answer, or one instance
  (`…/file-replicator/{instance}/cmd/{verb}`) for that instance alone.
- **`instance`** (`set-activation`) — this verb has no "all" form. It answers `INSTANCE_REQUIRED`
  when neither the topic nor the body names an instance.

The `instance` body field remains the way to target one instance over the component inbox. When the
topic names an instance it wins; a request that names one instance in the topic and a different one
in `body.instance` is rejected with `BAD_ARGS` before the verb runs.

The library's own built-in verbs are also available on both inboxes: `ping` (liveness),
`reload-config` (re-fetch + re-apply), and **`get-configuration`** — returns the **redacted**
effective config (`{"config": <redacted>}`, with secrets replaced rather than left as unresolved
`$secret` refs).

## Events (`evt`, `evt/{severity}/{type}`)

Published through the `events()` facade (`gg.instance(id).events()` for a replication instance,
`gg.events()` for component-level events), which derives the channel from the body's own
severity + type — the topic and the body can never disagree. Body:
`{"severity", "type", "message"?, "timestamp", "context"?, "alarm"?, "active"?}`.

| Wire `type` | Severity | `alarm` | `context` fields |
|---|---|---|---|
| `file-discovered` | debug | — | `path`, `size` — not emitted |
| `file-ready` | info | — | `path`, `size` |
| `replication-started` | info | — | `path`, `size`, `destination`, `attempt` |
| `replication-progress` | info | — | `path`, `size`, `bytesDone`, `percent`, `destination`, `attempt` (throttled) |
| `replication-completed` | info | — | `path`, `size`, `destination`, `bytes` |
| `replication-failed` | warning | — | `path`, `destination`, `attempt`, `willRetry: true`, `nextAttemptAt`? (`message` carries the error) |
| `retries-exhausted` | critical | — | `path`, `destination`, `attempts` (`message` carries the last error) |
| `file-archived` | info | — | `path`, `archivePath`? |
| `file-deleted` | info | — | `path` |
| `file-cleanup-failed` | critical | — | `path`, `action` (`archive`\|`delete`), `attempts` (`message` carries the last error) |
| `file-quarantined` | critical | — | `path`, `attempts`, `quarantinePath`? (`message` carries the last error) |
| `scan-complete` | info | — | `discovered`, `awaiting` |
| `instance-activated` / `instance-deactivated` | info | — | `source` |
| `component-ready` | info | — | `instances`, `version` |
| `schedule-triggered` | info | — | `scope` (`"all"` or an instance id) |
| `window-opened` / `window-closed` | info | — | `window` (the schedule's human-readable label) |
| `schedule-complete` | info | — | `mode` (`"cron"` \| `"window"`) |
| `disconnected` | critical | `raise_alarm` | `link` — not emitted (there is no destination circuit-breaker) |
| `permission-denied` | critical | — | `path`, `role` (`ingress`\|`egress`\|`archive`\|`failed`) (`message` carries the error) |

`file-archived` / `file-deleted` — published only for a source completion action that verifiably happened:
the archive target exists and matches the source, or the deleted source is gone. `archivePath` is the path
the file actually landed at, which the `suffix` collision policy can rename.

`file-cleanup-failed` — the file replicated and verified on every destination, but its source could not be
released and the completion retry budget is spent. The file is **not** counted in `replicated`, its source
stays in the watch directory, and it appears in `get-status` under `failed.items[]` with
`state: "cleanup_failed"`. Fix what `message` reports, then send `trigger` to re-drive it. See
**Completion is proven, not assumed** in `explanation.md`.

`permission-denied` — a directory/target the instance depends on is unreadable/unwritable, at startup or
at runtime. ALWAYS emitted, but **deduplicated** so it is not repeated on every rescan or every file: for
`ingress`/`archive`/`failed` the dedup key (carried in `context.path`) is the directory, and for `egress`
it is the **destination** (so a broken-permission destination emits once, not once per file —
`context.path` then names the destination). A recovery re-arms the dedup, so a later re-break emits
again. See **Permission handling** in `explanation.md`. Governed by `onPermissionError`
(`component.global` / per-instance).

## State / heartbeat — library-owned

The UNS `state` class is **reserved** (library-owned; an app-level publish to it is rejected) and carries
only the library's own `RUNNING`/`STOPPED` keepalive (`ecv1/{device}/file-replicator/state`, on by
default, 5 s, best-effort `STOPPED` on shutdown). The component publishes no `state` snapshot of its own,
and none is retained (there are no retained MQTT messages; a timestamped app-layer cache on the consumer
side is the substitute). The "current state on demand" path is **`get-status`** (a `cmd` verb, above),
which returns the full document — per instance (`awaiting`/`inProgress`/`replicated`/`failed`) or the whole
component roster + summary. An instance disabled at startup by `onPermissionError: disableInstance` still
answers `get-status` — `active: false`, `disabled: true`, `disabledReason` — instead of `UNKNOWN_INSTANCE`.

## Metrics — library-owned

Metrics are emitted through `gg.metrics()` on the reserved UNS `metric` class, using the configured
`metricEmission` target. The compatibility `fileReplicator` group and the richer
`FileReplicator*` groups publish under `ecv1/{device}/file-replicator/metric/{metricName}`.
For every metric's dimensions, measures, units, and diagnostic purpose, see
[Reference - Metrics](metrics.md).

## Envelope

The standard edgecommons `Message`: `header` (`name`/`version`/`timestamp`/`correlation_id`/`reply_to`?),
top-level `identity` (`hier`/`path`/`component`/`instance`, stamped automatically), optional `tags`
(config `tags`, metadata only), `body`. The Rust component never hand-emits this envelope as JSON; it
builds messages through the core facades and the core messaging service serializes/deserializes protobuf
bytes while preserving `identity` and `tags`.
