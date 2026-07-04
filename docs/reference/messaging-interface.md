# Reference — messaging interface (UNS)

All command/event topics ride the ggcommons **Unified Namespace** core (`gg.commands()` / `gg.events()` /
the automatic `state`/`cfg` keepalive) — minted by the library, not a hand-rolled topic builder. Full
rationale in [`DESIGN.md`](https://github.com/edgecommons/file-replicator/blob/main/DESIGN.md) §15-§17
(some of that section's wording predates this migration — this page + `crate::events`/`crate::control`'s
module docs are the current source of truth).

```
ecv1/{device}/{component}/{instance}/{class}[/{channel…}]
```

- `{device}` — the resolved ThingName (`-t`/`--thing`).
- `{component}` — the SHORT UNS token ggcommons derives from the full name (the segment after the last
  `.` — `com.mbreissi.edgecommons.FileReplicator` → **`FileReplicator`**), not the `file-replicator`
  registry slug.
- `{instance}` — a `component.instances[].id`, or `main` for component-level traffic (the built-in
  command verbs, `ComponentReady`, the scope-`"all"` `trigger`/`get-status` events, and the library's own
  `state`/`cfg`/`metric` keepalives).
- `{class}` ∈ `cmd` (inbound commands, request/reply) · `evt` (event stream) · the **reserved**,
  library-owned `state`/`cfg`/`metric`/`log` (this component never publishes to them directly).

There is no configurable topic prefix and no legacy core-`GetConfiguration` alias — both were retired in
this migration (the fixed UNS grammar replaces the old `{thing}/file-replicator/…` scheme and its
`component.global.topics.prefix`/`legacyConfigTopic` config knobs outright).

## Commands (`cmd`, request/reply via `reply_to`)

Registered on the single `main`-instance command inbox (`ecv1/{device}/FileReplicator/main/cmd/#`) —
scoping an instance is a request-body field, not a topic segment (mirroring how opcua-adapter/
modbus-adapter address their multi-instance `sb/*` verbs and telemetry-processor's `route`/`pause`/
`resume`). Every reply is `{"ok": true, "result": <value>}` or `{"ok": false, "error": {"code",
"message"}}` (the ggcommons command-inbox contract — the request's `header.name` MUST equal the verb).

| Verb | Topic | Body | Result / error codes |
|---|---|---|---|
| `get-status` | `…/cmd/get-status` | `{ "instance"?: string }` | omitted `instance` → component-wide roster+summary; present → that instance's document; `UNKNOWN_INSTANCE` |
| `trigger` | `…/cmd/trigger` | `{ "instance"?: string, "ignoreWindow"?: bool }` | accepted + counts; `UNKNOWN_INSTANCE` |
| `set-activation` | `…/cmd/set-activation` | `{ "instance": string, "active"?: bool, "persist"?: bool, "reset"?: bool }` | new state; `INSTANCE_REQUIRED` (no "all" form), `UNKNOWN_INSTANCE`, `INVALID_REQUEST` (neither `active` nor `reset`), `ACTIVATION_FAILED` |

The library's own built-in verbs answer everything the old custom `get-config` verb (and its
`legacyConfigTopic` alias) used to: `ping` (liveness), `reload-config` (re-fetch + re-apply), and
**`get-configuration`** — returns the **redacted** effective config (`{"config": <redacted>}`, secrets
replaced, not just left as unresolved `$secret` refs — strictly safer than the old verb).

## Events (`evt`, `evt/{severity}/{type}`)

Published through the `events()` facade (`gg.instance(id).events()` for a replication instance,
`gg.events()` for component-level events), which derives the channel from the body's own
severity + type — the topic and the body can never disagree (DESIGN-class-facades §2.2). Body:
`{"severity", "type", "message"?, "timestamp", "context"?, "alarm"?, "active"?}`.

| Wire `type` | Severity | `alarm` | `context` fields |
|---|---|---|---|
| `file-discovered` | debug | — | `path`, `size` (**deferred** — not yet emitted, see `CLAUDE.md`) |
| `file-ready` | info | — | `path`, `size` |
| `replication-started` | info | — | `path`, `size`, `destination`, `attempt` |
| `replication-progress` | info | — | `path`, `size`, `bytesDone`, `percent`, `destination`, `attempt` (throttled) |
| `replication-completed` | info | — | `path`, `size`, `destination`, `bytes` |
| `replication-failed` | warning | — | `path`, `destination`, `attempt`, `willRetry: true`, `nextAttemptAt`? (`message` carries the error) |
| `retries-exhausted` | critical | — | `path`, `destination`, `attempts` (`message` carries the last error) |
| `file-archived` | info | — | `path`, `archivePath`? |
| `file-deleted` | info | — | `path` |
| `file-quarantined` | critical | — | `path`, `attempts`, `quarantinePath`? (`message` carries the last error) |
| `scan-complete` | info | — | `discovered`, `awaiting` |
| `instance-activated` / `instance-deactivated` | info | — | `source` |
| `component-ready` | info | — | `instances`, `version` |
| `schedule-triggered` | info | — | `scope` (`"all"` or an instance id) |
| `window-opened` / `window-closed` | info | — | `window` (the schedule's human-readable label) |
| `schedule-complete` | info | — | `mode` (`"cron"` \| `"window"`) |
| `disconnected` | critical | `raise_alarm` | `link` — **deferred** (the P4 destination circuit-breaker; `Reconnected` `clear_alarm`s the SAME `disconnected` channel once built) |
| `permission-denied` | critical | — | `path`, `role` (`ingress`\|`egress`\|`archive`\|`failed`) (`message` carries the error) |

`permission-denied` — a directory/target the instance depends on is unreadable/unwritable, at startup or
at runtime. ALWAYS emitted, but **deduplicated** so it is not repeated on every rescan or every file: for
`ingress`/`archive`/`failed` the dedup key (carried in `context.path`) is the directory, and for `egress`
it is the **destination** (so a broken-permission destination emits once, not once per file —
`context.path` then names the destination). A recovery re-arms the dedup, so a later re-break emits
again. See **Permission handling** in `explanation.md`. Governed by `onPermissionError`
(`component.global` / per-instance).

## State / heartbeat — now library-owned, not a component publish

The old per-instance/component `state/…` snapshot publish is **gone, not migrated**: the new UNS `state`
class is **reserved** (library-owned; an app-level publish to it is rejected), and carries only the
library's own `RUNNING`/`STOPPED` keepalive (`ecv1/{device}/FileReplicator/main/state`, on by default,
5 s, best-effort `STOPPED` on shutdown) — there is no seam left for a component-specific rich snapshot,
and there never will be a retained one (the platform dropped MQTT retain fleet-wide; a timestamped
app-layer cache on the consumer side is the substitute). The reliable "current state on demand" path is
unchanged: **`get-status`** (a `cmd` verb, above) returns the exact same document a live subscriber used
to also get pushed to it — per instance (`awaiting`/`inProgress`/`replicated`/`failed`) or the whole
component roster + summary. An instance disabled at startup by `onPermissionError: disableInstance` still
answers `get-status` — `active: false`, `disabled: true`, `disabledReason` — instead of `UNKNOWN_INSTANCE`.

## Envelope

The standard ggcommons `Message`: `header` (`name`/`version`/`timestamp`/`correlation_id`/`reply_to`?),
top-level `identity` (`hier`/`path`/`component`/`instance` — stamped automatically, no `tags.thing`
anymore), optional `tags` (config `tags`, metadata only), `body`. See DESIGN §16/§17 for the pre-migration
payload rationale (topic/body SHAPES there are superseded by this page).
