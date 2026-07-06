# Reference — configuration

The canonical configuration reference. There is no per-component JSON schema in the ecosystem; **this
document is the source of truth**, and it is validated against the parser in `src/config.rs` (not against
other docs). It covers the full field surface: the component config, all seven `egress` backends,
scheduling, completion, retry, limits, and permission policy. Per-backend
destination fields (all of them) are tabulated in [Reference › Destinations](destinations.md); the
`get-status` and event payloads in [Reference › Data types](data-types.md).

The component owns the `component` section (`component.global` + `component.instances[]`). Sibling sections
(`messaging`, `credentials`, `logging`, `heartbeat`, `metricEmission`, `health`, `tags`) are parsed by the
`edgecommons` library — see the library docs.

## `component.global`
| Key | Type | Default | Notes |
|---|---|---|---|
| `defaults.retry` | object | — | Instance retry defaults (overridden per instance). See **retry**. |
| `defaults.timezone` | string | UTC | Default schedule timezone. |
| `limits.maxConcurrentFiles` | int | 64 | Global in-flight cap across all instances. |
| `limits.maxBandwidth` | string | — | Global aggregate byte-rate cap, e.g. `"50MB/s"`. |
| `onPermissionError` | `"disableInstance"` \| `"fatal"` \| `"retain"` | `"disableInstance"` | Component-wide default for what happens when an instance's ingress/egress/archive/failed directory fails a startup readable/writable check. An instance's own `onPermissionError` (below) overrides this. See **Permission handling** in `explanation.md`. |

> There is no `topics.prefix`/`legacyConfigTopic` config anymore — topics are minted by the edgecommons UNS
> core (`ecv1/{device}/FileReplicator/{instance}/{class}…`, fixed grammar, no override). See
> [Reference › Messaging interface](messaging-interface.md).

## `component.instances[]` — one per watched directory
| Key | Type | Default | Notes |
|---|---|---|---|
| `id` | string | *(required)* | Stable instance id. |
| `enabled` | bool | `true` | Initial activation; persisted runtime state may override. |
| `ingress` | object | *(required)* | Source + readiness. See below. |
| `egress` | array | *(required)* | Destination list; `N >= 1` entries fan out independently and complete once every entry verifies. See **egress**. |
| `schedule` | object | `{ "mode": "immediate" }` | See **schedule**. |
| `completion` | object | defaults below | See **completion**. |
| `retry` | object | global default | See **retry**. |
| `limits` | object | — | `maxConcurrentFiles`, `maxBandwidth` (per-instance). |
| `onPermissionError` | `"disableInstance"` \| `"fatal"` \| `"retain"` | inherits `component.global.onPermissionError` | Per-instance override of the component-wide permission-error policy. `disableInstance` skips just this instance (its siblings keep running); `fatal` aborts the whole component; `retain` starts the instance anyway, leaning on runtime dedup-logging + the `PermissionDenied` event for ongoing diagnostics. If **every** instance ends up disabled (or the set is empty), the component still fails fast (the "zero instances started" rule). |
| `priority` | int | `100` | Cross-instance **global** concurrency-admission priority: under contention for the shared `component.global.limits.maxConcurrentFiles` cap, a **lower** number is admitted first (the same 0-255-ish convention as elsewhere; ties broken FIFO by enqueue order). Governs ONLY the global admission decision — it does not affect this instance's own `limits.maxConcurrentFiles` cap, and there is **no bandwidth weighting** by priority. Demand-adaptive: an instance not currently contending for a global slot reserves nothing, regardless of its priority. See **Cross-instance priority** in `explanation.md`. |

### `ingress`
| Key | Type | Default | Notes |
|---|---|---|---|
| `path` | string | *(required)* | Source directory. |
| `recursive` | bool | `false` | Watch the whole tree; preserve subtree at destination. |
| `include` / `exclude` | string[] | `[]` | Globs, matched against the source-relative path. |
| `rescanSecs` | int | `30` | Reconciliation-rescan interval — the **fallback** discovery path (see note). |
| `readiness` | object | `{ "strategy": "stability", "quietSecs": 5 }` | `stability` \| `marker` \| `rename` \| `glob`. |

> **Discovery latency & degraded mode.** Files are discovered by an OS file watch (low-latency) *and* the
> periodic `rescanSecs` rescan (fallback). Normally the watch drives discovery and ready latency is
> ≈ `quietSecs` (a second or two), regardless of `rescanSecs`. If the watch can't be established or misses an
> event — common on **network filesystems (NFS/SMB), container bind-mounts/overlay volumes, and FUSE** — the
> instance logs a warning and falls back to the rescan: no file is lost, but discovery can take up to
> `rescanSecs`. On such filesystems, **lower `rescanSecs`** to bound worst-case latency (the rescan is a cheap
> directory walk); raise it only on very large/slow spools. The startup log says which mode you're in
> (`OS file watch active` vs `… using periodic rescan only`). See
> [Explanation › Discovery](../explanation.md).

### `egress` (item)
`egress` is an ordered list of `N >= 1` destination items; each fans out independently and a file completes
only once **every** item verifies. `type` selects the backend:

| `type` | Feature (default?) | Required fields | Full field table |
|---|---|---|---|
| `local` | built-in (always) | `path` | [destinations › local](destinations.md#local) |
| `s3` | `dest-s3` (**on**) | `bucket` | [destinations › s3](destinations.md#s3) |
| `sftp` | `dest-sftp` (off) | `host` | [destinations › sftp](destinations.md#sftp) |
| `ftps` | `dest-ftps` (off) | `host` | [destinations › ftps](destinations.md#ftps) |
| `http` | `dest-http` (off) | `url` | [destinations › http](destinations.md#http-httphttps) |
| `azure` | `dest-azure` (off) | `account`, `container` | [destinations › azure](destinations.md#azure-azure-blob) |
| `gcs` | `dest-gcs` (off) | `bucket` | [destinations › gcs](destinations.md#gcs-google-cloud-storage) |

All backends are implemented; the off-by-default ones join the build only when their `dest-*` cargo feature
is enabled. Every full field table (all fields, types, defaults) lives in
[Reference › Destinations](destinations.md). Quick summary of the two default-on backends: `local`: `path`,
`fsync`. `s3`: `bucket`, `prefix`, `region`, `endpointUrl`, `credentials` (`$secret`; optional — ambient by
default), `storageClass`, `sse`/`kmsKeyId`, `accelerate`, `unsignedPayload`, `checksumAlgorithm`,
`multipart.{thresholdBytes,partSizeBytes,maxConcurrentParts}`. Across every non-`local` backend,
`checksumAlgorithm` defaults to `CRC32C` (alt `SHA256`) and `credentials` is an optional `{"$secret":"…"}`
vault ref; ambient plaintext secrets are supported but redacted in logs/`Debug`.

### `schedule`
`mode`: `immediate` (default) \| `cron` (`expression`, `timezone`) \| `window` (`open`, `close` **or**
`durationMins`, `timezone`, `onWindowClose`: `pauseResume` (default) \| `finishCurrent`). Cron is standard
cron, tz/DST-aware.

### `completion`
| Key | Type | Default | Notes |
|---|---|---|---|
| `onSuccess` | enum | `archive` | `archive` (needs `archiveDir`) \| `delete`. |
| `archiveDir` | string | — | Required when `onSuccess=archive`. |
| `onExhausted` | enum | `retainInPlace` | `retainInPlace` \| `quarantine` (needs `failedDir`). |
| `failedDir` | string | — | Quarantine dir + `.error.json` sidecar. |
| `onCollision` | enum | `suffix` | `suffix` \| `overwrite` \| `fail`. |
| `verify` | enum | `checksum` | `checksum` \| `size` \| `none`. |

### `retry`
| Key | Type | Default | Notes |
|---|---|---|---|
| `baseDelayMs` | int | 1000 | Backoff base. |
| `maxDelayMs` | int | 900000 | Backoff cap (15 min). |
| `giveUpAfter` | string | `7d` | Time budget; governs long-outage tolerance. |
| `maxAttempts` | int | — | Optional hard cap (default: none — time-governed). |
