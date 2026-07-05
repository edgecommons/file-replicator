# Reference — data types

The two structured payloads a client parses: the **`get-status` status document** (the reply to the
`get-status` command) and the **event `context`** object carried by every `evt` message. Both are validated
against the source — the status document against `src/control.rs`
(`instance_status_json` / `disabled_status_json` / `component_status_json`), the event context against
`src/events.rs` (`Event::fields` / `Event::plan`). For the topic grammar, command envelope, and the event
catalog (each `type` + severity), see [messaging-interface.md](messaging-interface.md).

All numeric fields are JSON numbers; byte counts and sizes are unsigned 64-bit (a JavaScript consumer may
lose precision above 2^53). Timestamps are RFC3339 UTC strings.

---

## The `get-status` document

`get-status` (`…/main/cmd/get-status`) returns different shapes depending on the request body:

- **`{}`** (no `instance`) → a **component-wide** document: a roster of every instance plus a summary.
- **`{ "instance": "<id>" }`** → that one instance's **per-instance** document (or its **disabled** document
  if it was disabled at startup). An unknown id is the error `UNKNOWN_INSTANCE`.

The reply is always wrapped by the command contract: `{ "ok": true, "result": <document> }`.

### Component-wide document

```json
{
  "component": "com.mbreissi.edgecommons.FileReplicator",
  "thing": "gw-01",
  "instances": [ /* per-instance and/or disabled documents */ ],
  "summary": { "instances": 3, "active": 2, "disabled": 1 }
}
```

| Field | Type | Notes |
|---|---|---|
| `component` | string | The component's full name. |
| `thing` | string | The resolved ThingName (`-t`). |
| `instances` | array | One entry per configured instance — a per-instance document for a live instance, a disabled document for one disabled at startup. |
| `summary.instances` | int | Total configured instances (live **plus** disabled). |
| `summary.active` | int | How many are currently active (never counts disabled ones). |
| `summary.disabled` | int | How many were disabled at startup (`onPermissionError: disableInstance`). |

### Per-instance document

```json
{
  "instance": "spool-to-archive",
  "active": true,
  "configuredEnabled": true,
  "schedule": { "mode": "immediate" },
  "awaiting":   { "count": 1, "bytes": 40, "files": [ { "path": "a.csv", "size": 40, "ageSecs": 100 } ] },
  "inProgress": [ { "path": "big.bin", "size": 100, "bytesDone": 30, "percent": 30.0, "destination": "s3", "attempt": 1 } ],
  "replicated": { "count": 4310, "bytes": 90 },
  "failed":     { "count": 2, "items": [ { "path": "x.csv", "attempts": 3, "lastError": "…", "state": "failed" } ] }
}
```

| Field | Type | Notes |
|---|---|---|
| `instance` | string | The instance id. |
| `active` | bool | Effective activation (persisted runtime state wins over config `enabled`). |
| `configuredEnabled` | bool | The config `enabled` value (what a `set-activation` `reset` reverts to). |
| `schedule.mode` | string | The **configured** mode, verbatim: `immediate` \| `cron` \| `window`. |
| `awaiting.count` | int | Files ready and queued but not yet started. |
| `awaiting.bytes` | int | Total bytes of the awaiting set. |
| `awaiting.files[]` | array | Up to **100** entries (`count`/`bytes` reflect the full set): `path` (source-relative), `size` (int bytes), `ageSecs` (int — whole seconds queued). |
| `inProgress[]` | array | Up to 100 in-flight transfers: `path`, `size`, `bytesDone` (int), `percent` (float 0.0–100.0, one decimal), `destination` (backend label, e.g. `local`/`s3`), `attempt` (int, 1-based). |
| `replicated.count` | int | Lifetime successfully-replicated count (running stat). |
| `replicated.bytes` | int | Lifetime replicated bytes. |
| `failed.count` | int | Current failed/exhausted/quarantined item count. |
| `failed.items[]` | array | Up to 100 entries: `path`, `attempts` (int), `lastError` (string), `state`, and `quarantinedAt` **only** when `state == "quarantined"`. |

`failed.items[].state` is one of `failed` (retrying), `exhausted` (retry budget spent, retained in place),
or `quarantined` (moved to `failedDir`). `quarantinedAt` is an RFC3339 UTC timestamp.

> There is no `link` (destination-connectivity) field — file-replicator has no destination circuit-breaker
> (see [explanation › Resilience](../explanation.md#resilience-across-long-outages)). Do not depend on it.

### Disabled-instance document

An instance disabled at startup by `onPermissionError: disableInstance` (the default) still answers
`get-status` — with its reason — instead of `UNKNOWN_INSTANCE`. It shares the same top-level keys as a live
instance (all tallies zeroed) plus the `disabled*` fields, so a consumer needs no separate schema:

```json
{
  "instance": "plant-in",
  "active": false,
  "configuredEnabled": false,
  "disabled": true,
  "disabledReason": "permission denied: /data/in",
  "disabledRole": "ingress",
  "disabledPath": "/data/in",
  "schedule": { "mode": "disabled" },
  "awaiting":   { "count": 0, "bytes": 0, "files": [] },
  "inProgress": [],
  "replicated": { "count": 0, "bytes": 0 },
  "failed":     { "count": 0, "items": [] }
}
```

| Field | Type | Notes |
|---|---|---|
| `disabled` | bool | Always `true` on this document. |
| `disabledReason` | string | Human-readable summary of the startup violation. |
| `disabledRole` | string | The offending directory role: `ingress` \| `egress` \| `archive` \| `failed`. |
| `disabledPath` | string | The path that failed the startup readable/writable check. |
| `schedule.mode` | string | The literal `"disabled"`. |

---

## Event `context` field types

Every `evt` message body is `{ "severity", "type", "message"?, "timestamp", "context"?, "alarm"?, "active"? }`
(see [messaging-interface.md › Events](messaging-interface.md#events-evt-evtseveritytype)). The `context`
object carries the event-specific data; its fields, across all event types, have these types:

| Context field | Type | Appears on | Meaning |
|---|---|---|---|
| `path` | string | most file events, `permission-denied` | Source-relative file path (or, for `permission-denied` egress, the destination). |
| `size` | int | `file-ready`, `replication-*` | File size in bytes. |
| `destination` | string | `replication-*` | Backend label of the target (`local`/`s3`/…). |
| `attempt` | int | `replication-started`/`-progress`/`-failed` | Current attempt, 1-based. |
| `attempts` | int | `retries-exhausted`, `file-quarantined` | Total attempts made. |
| `bytesDone` | int | `replication-progress` | Bytes transferred so far. |
| `percent` | float | `replication-progress` | Percent complete, 0.0–100.0. |
| `bytes` | int | `replication-completed` | Bytes transferred (total). |
| `willRetry` | bool | `replication-failed` | Always `true` — the engine will retry (contrast `retries-exhausted`). |
| `nextAttemptAt` | string (RFC3339) | `replication-failed` | Next scheduled attempt time; present only when scheduled. |
| `archivePath` | string | `file-archived` | Where the source was archived; omitted when unknown. |
| `quarantinePath` | string | `file-quarantined` | Where the source was quarantined; omitted when unknown. |
| `discovered` | int | `scan-complete` | Files newly enqueued this scan tick. |
| `awaiting` | int | `scan-complete` | Total files ready after the tick. |
| `source` | string | `instance-activated`/`-deactivated` | Who toggled activation (e.g. `control`). |
| `instances` | int | `component-ready` | Number of instances that started. |
| `version` | string | `component-ready` | Component version. |
| `scope` | string | `schedule-triggered` | `"all"` or an instance id. |
| `window` | string | `window-opened`/`-closed` | The schedule's human-readable label (e.g. `0 22 * * * -> 0 6 * * *`). |
| `mode` | string | `schedule-complete` | `"cron"` \| `"window"`. |
| `role` | string | `permission-denied` | `ingress` \| `egress` \| `archive` \| `failed`. |
| `link` | string | `disconnected` — not emitted | Destination link label (there is no destination circuit-breaker). |

**`message` vs `context`.** For the events with a natural error string — `replication-failed`,
`retries-exhausted`, `file-quarantined`, `permission-denied` — the error is promoted to the top-level
`message` field and **removed from `context`** (never duplicated on the wire). So a consumer reads the human
error from `message`, and the machine fields (`path`, `attempt`, `willRetry`, `role`, …) from `context`.
