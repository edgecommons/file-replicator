# How-to guides

Task-focused recipes. Each assumes the component builds and runs (see the [tutorial](tutorial.md)). For the
model and the *why*, see [explanation.md](explanation.md); for every field, [reference/](reference/). Each
instance is a `component.instances[]` entry; complete configs are in
[sample-configurations.md](sample-configurations.md).

---

## Replicate a directory to S3

Point an instance's `egress` at an `s3` destination (compiled in by default — `dest-s3` is in the default
feature set):

```jsonc
"egress": [ {
  "type": "s3",
  "bucket": "acme-plant-telemetry",
  "prefix": "site42/csv/",        // key = prefix + source-relative path (subtree preserved)
  "region": "us-east-1",
  "accelerate": true,             // S3 Transfer Acceleration endpoint
  "checksumAlgorithm": "CRC32C",  // flexible/trailing checksum (default CRC32C; or SHA256)
  "multipart": { "thresholdBytes": 16777216, "partSizeBytes": 16777216, "maxConcurrentParts": 4 }
} ]
```

- **Credentials — ambient by default.** Omit `credentials` and the S3 backend inherits the platform's
  provider chain: Greengrass TokenExchangeService device role, Kubernetes IRSA / env / node role, or HOST
  env / shared profile / instance role. Scope IAM to `bucket/prefix/*` with `PutObject` + the multipart
  actions (add KMS if you set `sse: "aws:kms"`).
- **Explicit credentials** come from the vault, never inline: `"credentials": {"$secret": "s3-uploader"}`.
  The resolved secret is never logged.
- Files above `multipart.thresholdBytes` upload as parallel multipart (parts persisted for resume);
  smaller files use a single `PutObject`. `unsignedPayload: true` skips the payload-signing pass on those
  simple PUTs over TLS. Use `endpointUrl` for MinIO / S3-compatible / GovCloud.

Full field table: [reference › destinations › S3](reference/destinations.md#s3).

---

## Schedule a nightly upload window

By default replication is `immediate` (files go as they become ready). To hold work for an overnight
window, set `schedule.mode: "window"`. `open`/`close` accept either standard cron **or** an English sugar
phrase, and are timezone/DST-aware:

```jsonc
"schedule": {
  "mode": "window",
  "open": "0 22 * * *",      // 22:00 — cron, or the phrase "between 10pm and 6am" on `open` alone
  "close": "0 6 * * *",      // 06:00 next day — an overnight span is inferred from the two times
  "timezone": "America/Chicago",
  "onWindowClose": "pauseResume"   // pauseResume (default) | finishCurrent
}
```

- **Overnight** needs no special flag — a `close` time-of-day earlier than `open` is derived as spanning
  midnight from the two crons. The whole span can also be one phrase on `open`:
  `"open": "between 10pm and 6am"` (no `close`).
- **`onWindowClose`** governs a transfer still running when the window closes: `pauseResume` pauses it and
  resumes next window (if the destination supports resume, else it finishes the current file);
  `finishCurrent` lets in-flight files finish but admits no new ones until the next open.
- **DST** is handled by evaluating the cron in `timezone` (falls back to
  `component.global.defaults.timezone`, then UTC). Use `durationMins` instead of `close` for a fixed-length
  window (e.g. `"durationMins": 480`); setting **both** `close` and `durationMins` is ambiguous and skips
  the instance.
- For a point trigger instead of a span, use `"mode": "cron"` with an `expression` (e.g. `"daily at 2am"`),
  which releases all ready work at each fire.

English phrases (see `src/schedule/sugar.rs`): `hourly`, `daily`, `every day at 2am`, `every 15 minutes`,
`every 15 minutes on weekdays`, `every weekday at 8:00`, `every monday at 7:15`, `weekly on friday at
17:30`, `between 9am and 5pm`.

---

## Cap upload bandwidth

There are two independent byte-rate caps; both accept a human rate string like `"20MB/s"` or `"5MB/s"`:

```jsonc
// per-instance:
"limits": { "maxBandwidth": "5MB/s", "maxConcurrentFiles": 4 }

// process-wide aggregate (component.global) — a token bucket EVERY transfer also passes:
"component": { "global": { "limits": { "maxBandwidth": "50MB/s", "maxConcurrentFiles": 64 } } }
```

- A transfer is gated by **both** its instance cap and the global bucket, so the global cap bounds total
  egress no matter how many instances run.
- `maxConcurrentFiles` (per-instance and the `component.global` default of 64) bounds in-flight *files*.
  When more instances contend for the global slot pool than there are slots, each instance's `priority`
  (default `100`, **lower = admitted first**, FIFO among equals) decides admission order — see
  [explanation › Cross-instance priority](explanation.md#cross-instance-priority). `priority` does **not**
  weight the byte-rate; give a class its own `maxBandwidth` if it needs more of the budget.

---

## Choose a readiness strategy

A newly-seen file may still be mid-write. `ingress.readiness.strategy` picks how "done" is decided:

| Strategy | When ready | Config |
|---|---|---|
| `stability` (default) | size + mtime unchanged for `quietSecs` | `{ "strategy": "stability", "quietSecs": 5 }` |
| `marker` | a companion marker file appears (e.g. `FILE.done` for `FILE`) | `{ "strategy": "marker", "suffix": ".done" }` |
| `rename` | the file is renamed/moved into the watch dir (atomic-publish producers) | `{ "strategy": "rename" }` |
| `glob` | anything not matched by a temp/exclude glob is ready immediately | `{ "strategy": "glob", "ready": ["**/*.csv"] }` |

Use `stability` for unknown producers, `marker`/`rename` for cooperative ones (no quiet-period wait), and
`glob` when files land complete. Only *ready* files enter the durable queue. See
[explanation › Readiness](explanation.md#readiness--knowing-a-file-is-done).

---

## Quarantine files that exhaust their retry budget

By default an exhausted file is left in place (`onExhausted: "retainInPlace"`, re-tried on the next
`trigger`). To move failures aside instead, set `quarantine` with a `failedDir`:

```jsonc
"completion": {
  "onSuccess": "delete",
  "onExhausted": "quarantine",
  "failedDir": "/data/failed"      // required for quarantine
}
```

On exhaustion the source is moved to `failedDir` alongside an `.error.json` sidecar describing the failure,
and a `file-quarantined` event (severity `critical`, `context.quarantinePath`) is emitted. Quarantined
items also show up in `get-status` under `failed.items[]` with `state: "quarantined"` and a
`quarantinedAt`. A retry budget is exhausted per `retry.giveUpAfter` (time) or an optional
`retry.maxAttempts` (count).

---

## Survive a multi-day disconnection

Two shipped mechanisms tolerate an endpoint being down for hours to days — configure the time budget and
lean on resume:

```jsonc
"retry": { "baseDelayMs": 1000, "maxDelayMs": 900000, "giveUpAfter": "7d" }
```

- **`giveUpAfter`** (default `7d`) is a **time** budget, not an attempt cap — the file keeps retrying on the
  `baseDelayMs`→`maxDelayMs` backoff across the whole outage and is only given up when the budget elapses.
- **Resume** is automatic: interrupted transfers resume from a persisted checkpoint (S3 multipart parts,
  ranged-PUT / append / session / staged blocks per backend), so a reconnect after a long gap continues
  rather than restarting the file.

> **Not available (deferred):** the DESIGN §13.4 destination **circuit-breaker** (staggered reconnects,
> `Disconnected`/`Reconnected` alarm events, the `get-status` `link` field) is **not implemented** — the
> event variants in `src/events.rs` are marked `Deferred` and never emitted. Do not build alerting on a
> `disconnected` event or a `link` status field; instead watch `replication-failed` events (each carries
> `willRetry: true` and `nextAttemptAt`) and the `failed`/`inProgress` tallies from `get-status`. See
> [explanation › Resilience](explanation.md#resilience-across-long-outages).

---

## Activate / deactivate an instance from the control plane

Pause or resume one instance at runtime without a redeploy, via the `set-activation` command (it has **no**
"all" form — `instance` is required):

```
publish   ecv1/<device>/FileReplicator/main/cmd/set-activation
          { "header": { "name": "set-activation", "reply_to": "app/r", "correlation_id": "9" },
            "body": { "instance": "plant-csv-to-s3", "active": false, "persist": true } }
subscribe app/r   → { "ok": true, "result": { "instance": "plant-csv-to-s3", "active": false, "persisted": true } }
```

- `persist: true` (the default) writes the override to durable state so it **survives restart** (runtime
  state wins over config `enabled`); `persist: false` is a runtime-only flip.
- `reset: true` (instead of `active`) clears the persisted override, reverting to config `enabled`.
- A deactivated instance's forced `trigger` is a no-op and its scheduler admits nothing; it still answers
  `get-status`. Each transition emits an `instance-activated` / `instance-deactivated` event.

---

## Build a realtime UI on the event stream

There is **no** retained state snapshot to hydrate from (the UNS `state` class is reserved to the library's
RUNNING/STOPPED keepalive). Build a live view by combining two sources:

1. **Subscribe to the event stream** for a device (or the whole fleet):
   `ecv1/+/FileReplicator/+/evt/#`. Apply each `type` (`file-ready`, `replication-started`,
   `replication-progress`, `replication-completed`, `replication-failed`, `retries-exhausted`,
   `file-archived`/`file-deleted`/`file-quarantined`, …) to your in-memory model. `replication-progress`
   carries `percent`/`bytesDone` (throttled).
2. **Prime and re-sync with `get-status`** — call it on connect (and periodically) to get the exact
   `awaiting`/`inProgress`/`replicated`/`failed` document a late subscriber would otherwise have missed.
   Keep your own timestamped app-layer cache as the retain substitute.

Both the event `context` shapes and the `get-status` document schema are in the
[data-types reference](reference/data-types.md).

---

## Bridge status to the cloud for fleet-wide visibility

Every device publishes under the same fixed UNS grammar
`ecv1/{device}/FileReplicator/{instance}/{class}[/…]`, so a single cloud-side consumer sees every device
with one subscription:

```
ecv1/+/FileReplicator/+/evt/#         # every event, every instance, every device
ecv1/+/FileReplicator/+/state         # the library RUNNING/STOPPED keepalive per device
```

`{device}` is the ThingName (`-t`), `{component}` is the short UNS token `FileReplicator`. There is no
configurable prefix and no legacy alias. To fold status into a fleet dashboard, bridge these topics
northbound and fan `get-status` requests to each device's `main` inbox as needed. Envelope `tags` (e.g.
`enterprise`/`site`) travel in the message body for grouping — they are **not** topic segments. See the
[messaging interface reference](reference/messaging-interface.md).
