# Reference — configuration

The canonical configuration reference. There is no per-component JSON schema in the ecosystem; **this
document is the source of truth**, and it is validated against the parser in `src/config.rs` (not against
other docs). Fields are added here as each phase implements them; the shape below matches the P0 model.

The component owns the `component` section (`component.global` + `component.instances[]`). Sibling sections
(`messaging`, `credentials`, `logging`, `heartbeat`, `metricEmission`, `health`, `tags`) are parsed by the
`ggcommons` library — see the library docs.

## `component.global`
| Key | Type | Default | Notes |
|---|---|---|---|
| `defaults.retry` | object | — | Instance retry defaults (overridden per instance). See **retry**. |
| `defaults.timezone` | string | UTC | Default schedule timezone. |
| `limits.maxConcurrentFiles` | int | 8 | Global in-flight cap across all instances. |
| `limits.maxBandwidth` | string | — | Global aggregate byte-rate cap, e.g. `"50MB/s"` (P1). |
| `topics.prefix` | string | `{ThingName}/file-replicator` | UNS prefix template (DESIGN §15). |

## `component.instances[]` — one per watched directory
| Key | Type | Default | Notes |
|---|---|---|---|
| `id` | string | *(required)* | Stable instance id. |
| `enabled` | bool | `true` | Initial activation; persisted runtime state may override (DESIGN §7.5). |
| `ingress` | object | *(required)* | Source + readiness. See below. |
| `egress` | array | *(required)* | Destination list; v1 requires exactly one. See **egress**. |
| `schedule` | object | `{ "mode": "immediate" }` | See **schedule**. |
| `completion` | object | defaults below | See **completion**. |
| `retry` | object | global default | See **retry**. |
| `limits` | object | — | `maxConcurrentFiles`, `maxBandwidth` (per-instance). |
| `topics` | object | global prefix | `{ "prefix": "…" }` override. |

### `ingress`
| Key | Type | Default | Notes |
|---|---|---|---|
| `path` | string | *(required)* | Source directory. |
| `recursive` | bool | `false` | Watch the whole tree; preserve subtree at destination. |
| `include` / `exclude` | string[] | `[]` | Globs, matched against the source-relative path. |
| `rescanSecs` | int | — | Reconciliation rescan interval. |
| `readiness` | object | `{ "strategy": "stability", "quietSecs": 5 }` | `stability` \| `marker` \| `rename` \| `glob`. |

### `egress` (item)
`type` selects the backend: `local` \| `s3` (modeled) \| `sftp` \| `ftps` \| `http` \| `azure` \| `gcs`
(phased). See [Reference › Destinations](destinations.md) for per-backend fields. `local`: `path`,
`fsync`. `s3`: `bucket`, `prefix`, `region`, `endpointUrl`, `credentials` (`$secret`; optional — ambient
by default), `storageClass`, `sse`/`kmsKeyId`, `accelerate`, `unsignedPayload`, `checksumAlgorithm`,
`multipart.{thresholdBytes,partSizeBytes,maxConcurrentParts}`.

### `schedule`
`mode`: `immediate` (default) \| `cron` (`expression`, `timezone`) \| `window` (`open`, `close` **or**
`durationMins`, `timezone`, `onWindowClose`: `pauseResume` (default) \| `finishCurrent`). Cron is standard
cron, tz/DST-aware (DESIGN §12).

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
| `giveUpAfter` | string | `7d` | Time budget; governs long-outage tolerance (DESIGN §13.4). |
| `maxAttempts` | int | — | Optional hard cap (default: none — time-governed). |
