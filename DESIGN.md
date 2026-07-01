# file-replicator — Requirements & Design

> **Status:** DRAFT for review · **Version:** 0.1 (design) · **Date:** 2026-07-01
> **Component:** `file-replicator` · **Full name:** `com.mbreissi.greengrass.FileReplicator`
> **Category:** `sink` (northbound delivery) · **Language:** Rust · **Library:** `ggcommons`
> **Platforms:** HOST · GREENGRASS · KUBERNETES

This document is the single review artifact. It captures **what** the component does (requirements)
and **how** it is built (design), grounded in the existing EdgeCommons conventions (`telemetry-processor`
as the Rust template, the `ggcommons` library API, and the org registry/CI/docs plumbing). Nothing here
is implemented yet — this is for your review before we scaffold code.

Open questions you raised are answered inline and collected in **§18**. Your review decisions there drive
the scaffold.

---

## Table of contents

1. [Overview & positioning](#1-overview--positioning)
2. [Language decision (Rust)](#2-language-decision-rust)
3. [Concepts & terminology](#3-concepts--terminology)
4. [Functional requirements](#4-functional-requirements)
5. [Non-functional requirements](#5-non-functional-requirements)
6. [Architecture](#6-architecture)
7. [Configuration model](#7-configuration-model)
8. [The replication engine](#8-the-replication-engine)
9. [Readiness detection](#9-readiness-detection)
10. [Destinations](#10-destinations)
11. [S3 destination — high-performance design](#11-s3-destination--high-performance-design)
12. [Scheduling & windows](#12-scheduling--windows)
13. [Completion, integrity & reliability](#13-completion-integrity--reliability)
14. [Durable state](#14-durable-state)
15. [Control-message suite](#15-control-message-suite)
16. [Status/event publishing (realtime UI)](#16-statusevent-publishing-realtime-ui)
17. [Observability, platform & packaging](#17-observability-platform--packaging)
18. [Open decisions & recommendations](#18-open-decisions--recommendations)
19. [Phased implementation plan](#19-phased-implementation-plan)
20. [Appendix — registry entry, deps, repo scaffold](#20-appendix--registry-entry-deps-repo-scaffold)

---

## 1. Overview & positioning

`file-replicator` watches one or more local source directories and **replicates files** (copy-then-remove,
i.e. "move") to one or more destinations — another local directory, S3, and a pluggable set of remote
backends — either **as files arrive** (default) or **on a plain-English schedule/window**. It handles
"on-complete" source lifecycle (delete or archive), retries on failure (resuming partial uploads where the
backend supports it), exposes a full **control-message** surface (get-config / get-status+statistics /
trigger-now), and publishes **granular status events** so a realtime UI can be built on top.

**Where it sits.** It is a **sink / northbound-delivery** component in the EdgeCommons taxonomy
(`adapter | processor | sink`). The registry already reserves the `sink` category and the org profile/docs
landing pages have a "Sinks — northbound delivery" section waiting for the first one — this is it. Unlike
the southbound adapters, it does **not** emit `SouthboundSignalUpdate`; unlike `telemetry-processor`, its
data plane is **files on disk**, not the MQTT message bus. It still speaks the standard ggcommons control
plane (message envelope, config schema, health, metrics, credentials vault).

**Sink vs processor.** You noted it could be either. It is best modelled as a **sink**: its *input* is the
filesystem (not a ggcommons topic) and its *output* is a durable external store. It performs no payload
transformation. If we later add content-level transforms (e.g. compress, encrypt-at-rest, format-convert
on the way out), those are *egress filters*, not a reclassification — it stays a sink.

**Design ethos.** Per the EdgeCommons "depth over simplest-case" principle, this design covers the full
capability surface (arrays not scalars, multi-destination-ready, schedules *and* windows, resumable
uploads, a complete control+event surface) rather than the happy path. Where that adds risk, we phase the
*implementation* (§19) without narrowing the *design*.

---

## 2. Language decision (Rust)

**Recommendation: Rust.** Your instinct is right and it is also the ecosystem-consistent choice
(`telemetry-processor` is Rust; the pinned-git-rev `ggcommons` Rust lib is mature and used in production).
Concretely, this component is a near-ideal fit for Rust:

| Driver | Why Rust wins here |
|---|---|
| **Resource-constrained edge** | Small static binary, no GC pauses, low RSS — matters on Greengrass gateways. |
| **Concurrency** | `tokio` async + bounded worker pools give clean parallel multipart uploads and per-instance isolation without a thread-per-file explosion. |
| **I/O throughput** | Zero-copy streaming reads, backpressure, and precise control over part sizing/buffering for high-throughput S3. |
| **Correctness / data safety** | Move-then-delete with integrity verification is data-loss-sensitive; Rust's error handling + strong typing reduce the class of bugs that silently drop files. |
| **Ecosystem parity** | Reuses `ggcommons` Rust lib directly (messaging, config, credentials vault, health, metrics) — no FFI, one CI lane. |

**Caveats we accept and mitigate:**

- **Some backend SDKs are less mature in Rust.** S3 is first-class (`aws-sdk-s3`). Azure Blob and GCS have
  usable-but-younger crates; SFTP and HTTP are well-served. We isolate every backend behind a
  `Destination` trait (§10) so crate maturity is contained and swappable, and we ship them behind cargo
  features so a build only pulls what it needs.
- **Pure-Rust bias for the toolchain.** The org notes Windows has no C compiler (Lua/librdkafka only build
  in WSL). We deliberately choose **pure-Rust dependencies** for the new subsystems (durable state,
  scheduling) so `cargo build`/`test` work on Windows *and* everywhere else. See §14.

No other language was competitive: Python (`modbus-adapter`) and Java (`opcua-adapter`) are the adapter
languages and would regress on binary size, concurrency ergonomics, and edge footprint for a
high-throughput file mover.

---

## 3. Concepts & terminology

| Term | Meaning |
|---|---|
| **Instance** | One watched-directory specification = one `component.instances[]` entry (the standard ggcommons instance idiom, exactly as `telemetry-processor` uses routes). An instance is the unit of config, isolation, statistics, and control. |
| **Ingress** | The source: a local directory, optional recursion, readiness policy, include/exclude globs. |
| **Egress** | The destination(s): an ordered **list** of delivery targets. v1 enforces exactly one; the schema and engine are multi-destination-ready (§18-B). |
| **Schedule** | *When* replication runs: `immediate` (on arrival, default) or a plain-English recurrence, optionally bounded by a **window**. |
| **Window** | A recurring time span (e.g. *Wednesday 02:00–04:00*) during which uploads are permitted; work outside the window waits for the next window. |
| **Completion** | The source-file lifecycle action after **all** egress targets succeed: `delete` or `archive` (move to a processed dir). |
| **Readiness** | The policy that decides a newly-seen file is fully written and safe to move (default: stability window). |
| **Work item** | A single file tracked through its lifecycle: `Discovered → Ready → Queued → InProgress → Verified → Completed`, or `Failed → (retry)`. Persisted durably. |
| **Signal / tags** | Unchanged ggcommons vocabulary. This component does not produce `signal` telemetry; it uses the standard message **envelope** (`header`/`tags`/`body`) for control + events only. |

---

## 4. Functional requirements

IDs follow the ecosystem `FR-<AREA>-<n>` convention (cf. `FR-CRED-*`, `FR-MSG-*`). "MUST/SHOULD/MAY" per
RFC 2119.

### 4.1 Ingress / watching (ING)

- **FR-ING-1** — MUST watch a configured local **source directory** for files.
- **FR-ING-2** — MUST support **non-recursive** (top-level only, default) and **recursive** (whole tree) watching, per instance.
- **FR-ING-3** — When recursive, MUST **preserve the relative subtree** at the destination and **auto-create destination subdirectories** as needed (for local/SFTP; encode as key prefix for object stores).
- **FR-ING-4** — MUST support **include/exclude glob patterns** (e.g. include `*.csv`, exclude `*.tmp`, `.*`), evaluated against the path relative to the source root.
- **FR-ING-5** — MUST detect files that **already exist** at startup (pre-existing spool), not only files that arrive after start.
- **FR-ING-6** — MUST use OS filesystem notifications for low-latency arrival detection **and** a periodic **reconciliation rescan** to catch missed events (network FS, inotify overflow, files present before start). Rescan interval configurable.
- **FR-ING-7** — MUST decide **readiness** before queueing (§9); default = stability window.
- **FR-ING-8** — MUST NOT act on its own **archive** directory or any configured destination that happens to live under the watch root (loop prevention).

### 4.2 Egress / destinations (EGR)

- **FR-EGR-1** — MUST support destination types: **local directory** and **S3** (v1); **SFTP/FTPS**, **HTTP(S) POST/webhook**, **Azure Blob**, **GCS** (designed now, phased in — §18-A).
- **FR-EGR-2** — MUST model egress as an **ordered list** per instance. **v1 validates exactly one** element; multi-destination fan-out is a defined future capability (§18-B) with semantics fully specified here.
- **FR-EGR-3** (multi, future) — With N destinations, a file is **Completed only when ALL destinations succeed**; each destination retries **independently**; completion action fires once, after the last success.
- **FR-EGR-4** — Each destination MUST support a configurable **path/prefix mapping** and preserve the recursive subtree (FR-ING-3).
- **FR-EGR-5** — Each destination MUST report **per-file progress** (bytes transferred / total) to drive events and statistics.
- **FR-EGR-6** — Destinations that support it MUST **resume a partially-completed transfer** rather than restarting from zero (FR-REL-3).

### 4.3 Scheduling (SCH)

- **FR-SCH-1** — MUST support **immediate** mode (replicate as files become ready) — the default.
- **FR-SCH-2** — MUST support **plain-English schedules** (NOT cron in config): e.g. `"Every day at 5am"`, `"Every Wednesday at 2am"`, `"Every 15 minutes"`, `"Every hour"`.
- **FR-SCH-3** — MUST support **windows**: e.g. `"Every Wednesday from 2am to 4am"`. Files ready outside the window **wait** and are attempted when the window next opens.
- **FR-SCH-4** — MUST support **timezone** selection per instance (default: a configurable component-wide TZ; fallback UTC). Windows/recurrences evaluate in that TZ, DST-aware.
- **FR-SCH-5** — An in-flight transfer that does not finish before a window closes MUST **pause/resume at the next window** (leaning on resumable transfers, FR-REL-3) rather than being lost; partial progress is retained.
- **FR-SCH-6** — MUST publish **schedule-triggered** / **window-opened** / **window-closed** / **schedule-complete** events (§16).

### 4.4 Completion & lifecycle (CMP)

- **FR-CMP-1** — MUST support two on-complete behaviors: **delete** the source file, or **move to a user-defined archive/processed directory**.
- **FR-CMP-2** — Archive MUST preserve the relative subtree under the archive root; on name collision MUST apply a configurable policy (default: suffix with a monotonic counter/timestamp; never silently overwrite).
- **FR-CMP-3** — The completion action MUST fire **only after successful integrity verification** (§13) of **all** egress targets.
- **FR-CMP-4** — Completion MUST be **crash-safe**: a crash between "destination succeeded" and "source removed" MUST NOT lose or silently double-deliver the file (idempotent re-verify on restart; §14).
- **FR-CMP-5** — The `completion` policy is a **separate instance section**, not part of `egress` (rationale in §18-C).

### 4.5 Reliability / retry / resume (REL)

- **FR-REL-1** — On any error, the file MUST **remain in the queue** and be retried (never dropped).
- **FR-REL-2** — Retries MUST use **bounded exponential backoff with jitter**, configurable (`maxAttempts`, base/max delay). Exhausted items move to a `Failed` state (visible via statistics), retained for manual/triggered retry — **not** deleted.
- **FR-REL-3** — Where the backend supports it, a retry MUST **resume from the last successful part/offset** (S3 multipart parts; SFTP `APPE`/offset; HTTP range where supported), not restart. Resume state is persisted (§14).
- **FR-REL-4** — MUST be safe against **duplicate delivery**: re-delivering after a crash MUST be idempotent (stable object keys, overwrite-same-key for object stores; verify-before-complete for filesystems).
- **FR-REL-5** — MUST apply **backpressure**: a bounded number of concurrent in-flight files per instance and globally, so a large spool cannot exhaust memory or file handles.

### 4.6 Configuration (CFG)

- **FR-CFG-1** — Config MUST live under `component.global` / `component.instances[]` per the ggcommons schema; the component parses its own subtree (no change to the canonical schema). See §7.
- **FR-CFG-2** — MUST support `component.global.defaults` overlaid per instance (instance wins), mirroring `telemetry-processor`.
- **FR-CFG-3** — MUST tolerate **Greengrass numeric doubles** (lenient int-or-float deserialization) for all numeric fields.
- **FR-CFG-4** — A malformed **instance** MUST be logged and skipped; startup fails only if **zero** instances build (mirrors `telemetry-processor`).
- **FR-CFG-5** — MUST resolve secrets via **`$secret` refs** from the credentials vault (§13) so credentials never appear in the logged config snapshot.
- **FR-CFG-6** — Config hot-reload (via the ggcommons config watcher) SHOULD apply changes to instances without dropping in-flight transfers where possible; at minimum it MUST be safe (drain-and-restart per changed instance).

### 4.7 Control surface (CTL) — see §15

- **FR-CTL-1** — MUST answer **GetConfiguration** (reuse the core `GetConfiguration` v1.0 contract; return the effective config).
- **FR-CTL-2** — MUST answer **GetStatus** returning per-instance statistics: files awaiting replication (name/size/age), files replicated (count + recent), files failed (count + last error), files in-progress (name + **% complete**), current schedule/window state.
- **FR-CTL-3** — MUST accept **TriggerReplication** to force a scan+replication now (all instances or a named instance/window override).
- **FR-CTL-4** — Control replies MUST use the standard request/reply pattern (`reply_to` / `messaging.reply`).

### 4.8 Events (EVT) — see §16

- **FR-EVT-1** — MUST publish lifecycle events on configurable topics with intelligent defaults: file-discovered, file-ready, upload-started, upload-progress (throttled), upload-completed, upload-failed, file-archived/deleted, schedule-triggered, window-opened/closed, scan-complete.
- **FR-EVT-2** — Progress events MUST be **throttled** (by percent delta and/or time) to avoid flooding the bus.
- **FR-EVT-3** — Events MUST carry enough context (instance id, relative path, size, bytes-done, destination, attempt) to drive a UI without extra lookups.

---

## 5. Non-functional requirements

- **NFR-1 (Platforms)** — MUST run standalone (**HOST**), on **GREENGRASS** (IPC), and on **KUBERNETES** (ConfigMap), using the ggcommons platform/transport resolver — **no platform branching in component code** (mirror `telemetry-processor`).
- **NFR-2 (Coverage)** — MUST meet the org **90% line-coverage gate** on the CI-testable surface (`cargo llvm-cov --fail-under-lines 90`); live-infra paths (real S3/GG IPC/network backends) excluded via trait seams + fakes.
- **NFR-3 (Footprint)** — Idle RSS target < 25 MiB; bounded memory under a 100k-file spool (streaming reads, no whole-file buffering; state on disk not in RAM).
- **NFR-4 (Throughput)** — Saturate available uplink for large files via parallel multipart; handle high-count small-file spools via bounded concurrency.
- **NFR-5 (Durability)** — No acknowledged-ready file is lost across process crash, host reboot, or window boundary.
- **NFR-6 (Portability)** — All *new* subsystem dependencies pure-Rust (build on Windows without a C toolchain). Native-toolchain features (if any) held OFF by default, matching the `greengrass`/`streaming-kafka` pattern.
- **NFR-7 (Security)** — Credentials only via the vault / `$secret`; TLS on by default for network backends; least-privilege IAM guidance in docs.
- **NFR-8 (Observability)** — Metrics via `gg.metrics()`; health via the library HTTP endpoints on k8s; structured JSON logs on k8s.

---

## 6. Architecture

### 6.1 Runtime shape (mirrors `telemetry-processor`)

`main.rs` is ~40 lines: build the runtime, hand off to the app, await shutdown.

```rust
use ggcommons::prelude::*;
const COMPONENT_NAME: &str = "com.mbreissi.greengrass.FileReplicator";

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let gg = GgCommonsBuilder::new(COMPONENT_NAME)
        .args(std::env::args_os())   // -c/--platform/--transport/-t
        .build().await?;             // platform+transport resolve, config load+validate, logging/metrics/health
    let app = app::ReplicatorApp::start(&gg).await?;
    app.run(&gg).await?;             // runs until gg.shutdown_signal()
    Ok(())
}
```

The library owns platform detection, config source, logging, metrics, heartbeat, health server, SIGTERM,
and hot-reload. Teardown is RAII on `GgCommons` drop.

### 6.2 Module layout

```
src/
  main.rs            entry point (canonical bootstrap)
  app.rs             ReplicatorApp: builds one Instance per component.instances[]; wires control + shutdown
  config.rs          serde config model (Ingress/Egress/Schedule/Completion/Retry) + lenient numbers + parsing
  instance/
    mod.rs           Instance runtime: owns watcher + scheduler + queue + workers for one watched dir
    watcher.rs       notify-based watcher + periodic reconciliation rescan + readiness gate
    queue.rs         durable work-queue (redb-backed) + state machine + backpressure
    worker.rs        per-file transfer driver: read → egress fan-out → verify → complete
  schedule/
    mod.rs           Schedule + Window model; next-fire / window-open computation (chrono + chrono-tz)
    parse.rs         plain-English grammar parser ("Every Wednesday from 2am to 4am")
  dest/
    mod.rs           Destination trait + factory (dispatch by egress type)
    local.rs         local-directory destination
    s3.rs            S3 destination (size-adaptive: PutObject vs resumable multipart)   [feature: dest-s3]
    sftp.rs          SFTP/FTPS destination                                              [feature: dest-sftp]
    http.rs          HTTP(S) POST/PUT/webhook destination                               [feature: dest-http]
    azure.rs         Azure Blob destination                                             [feature: dest-azure]
    gcs.rs           GCS destination                                                    [feature: dest-gcs]
  control.rs         control-message dispatcher (GetConfiguration / GetStatus / TriggerReplication)
  events.rs          event publisher (envelope builder + topic templating + progress throttle)
  integrity.rs       hashing + verification helpers (streaming CRC32C/SHA-256)
  state.rs           durable-state store wrapper (redb): work items, resume checkpoints, offsets
  metricsx.rs        metric definitions + emission helpers
```

### 6.3 Concurrency model

- **One `Instance` task per watched directory.** Instances are independent (own watcher, scheduler, queue,
  worker pool, statistics) so a slow/failed destination on one instance never stalls another.
- **Bounded worker pool per instance** (`maxConcurrentFiles`, default e.g. 4) + a **global** semaphore
  (`component.global.maxConcurrentFiles`) so total in-flight is capped (FR-REL-5).
- **Within a file**, S3 multipart uses its own bounded part-concurrency (`maxConcurrentParts`).
- The watcher feeds the durable queue; workers pull `Ready` items when the schedule/window permits.
- Shutdown: `gg.shutdown_signal()` → stop accepting new work, checkpoint in-flight resume state, drain or
  cleanly pause workers, flush events/metrics, drop.

### 6.4 Data-flow (single file, immediate mode)

```
notify/rescan ─▶ readiness gate ─▶ [Ready] enqueue(durable) ─▶ worker acquires slot
   ─▶ open source (streaming) ─▶ egress.upload(stream, progress_cb)   (resumable)
   ─▶ integrity verify (checksum vs destination)
   ─▶ mark all-destinations-complete (durable)
   ─▶ completion action (delete | archive)  ◀── crash-safe ordering (§14)
   ─▶ emit Completed event + metrics
```

---

## 7. Configuration model

Config is one JSON document from the platform's config source (FILE on HOST, CONFIGMAP on k8s, GG_CONFIG
on Greengrass). The component owns `component.*`; all sibling sections (`messaging`, `credentials`,
`logging`, `heartbeat`, `metricEmission`, `health`, `tags`) are standard ggcommons sections.

### 7.1 Instance sections

Per your suggested structure, an instance has **`ingress` / `egress` / `schedule` / `completion`**, plus a
small **`retry`** section. Rationale for `completion` being its own section (not inside `egress`) is in
§18-C. Readiness lives under `ingress` (it's a source-side concern); integrity policy lives under
`completion` (it gates the source lifecycle).

### 7.2 Annotated example — S3, immediate, archive-on-complete

```jsonc
{
  "component": {
    "global": {
      "defaults": {
        "retry": { "maxAttempts": 10, "baseDelayMs": 1000, "maxDelayMs": 300000 },
        "timezone": "America/Chicago"
      },
      "maxConcurrentFiles": 8          // global cap across all instances
    },
    "instances": [
      {
        "id": "plant-csv-to-s3",

        "ingress": {
          "path": "/data/outbound/csv",
          "recursive": true,
          "include": ["**/*.csv"],
          "exclude": ["**/*.tmp", "**/.*"],
          "rescanSecs": 30,            // reconciliation scan interval (belt-and-suspenders)
          "readiness": {
            "strategy": "stability",   // stability | marker | rename | glob   (default: stability)
            "quietSecs": 5             // size+mtime unchanged for 5s => ready
          }
        },

        "egress": [                    // LIST — v1 requires exactly one element
          {
            "type": "s3",
            "bucket": "acme-plant-telemetry",
            "prefix": "site42/csv/",   // subtree preserved beneath this prefix
            "region": "us-east-1",
            "storageClass": "INTELLIGENT_TIERING",
            "sse": "aws:kms",
            "credentials": { "$secret": "aws/plant-uploader" },  // typed AwsCredentials view
            "multipart": {
              "thresholdBytes": 16777216,   // >16 MiB => multipart, else PutObject
              "partSizeBytes": 16777216,    // auto-scales up to stay <10000 parts
              "maxConcurrentParts": 4
            },
            "checksumAlgorithm": "CRC32C"    // end-to-end integrity (S3-native)
          }
        ],

        "schedule": { "mode": "immediate" },

        "completion": {
          "onSuccess": "archive",      // archive | delete
          "archiveDir": "/data/processed/csv",
          "onCollision": "suffix",     // suffix | overwrite | fail
          "verify": "checksum"         // checksum | size | none   (default: checksum)
        },

        "maxConcurrentFiles": 4,       // per-instance worker cap
        "events": { "enabled": true }  // topics defaulted; see §16
      }
    ]
  },

  "credentials": { "vault": { "path": "/data/vault.json" }, "keyProvider": { "type": "file" } },
  "messaging":   { /* platform-appropriate broker block */ },
  "logging":     { "level": "INFO" },
  "metricEmission": { "namespace": "filereplicator" }
}
```

### 7.3 Annotated example — scheduled window to another edge/local dir

```jsonc
{
  "id": "nightly-images-to-nas",
  "ingress": { "path": "/var/spool/images", "recursive": false,
               "readiness": { "strategy": "rename" } },
  "egress":  [ { "type": "local", "path": "/mnt/nas/ingest/images", "fsync": true } ],
  "schedule": {
    "mode": "scheduled",
    "expression": "Every Wednesday from 2am to 4am",   // recurrence + window (plain English)
    "timezone": "America/Chicago",
    "onWindowClose": "pause"        // pause | finishCurrent  (default: pause & resume next window)
  },
  "completion": { "onSuccess": "delete", "verify": "checksum" },
  "retry": { "maxAttempts": 20, "baseDelayMs": 2000, "maxDelayMs": 600000 }
}
```

### 7.4 Config structs (sketch)

All `#[serde(rename_all = "camelCase")]`, numeric fields via a lenient int-or-float deserializer (GG
doubles). Parsed per-instance with skip-on-error; startup fails only if zero instances build.

```rust
struct GlobalDefaults { retry: Option<RetryCfg>, timezone: Option<String>, /* ... */ }
struct InstanceCfg {
    id: String,
    ingress: IngressCfg,
    egress: Vec<EgressCfg>,          // v1: validated len == 1
    schedule: ScheduleCfg,           // default { mode: Immediate }
    completion: CompletionCfg,
    retry: Option<RetryCfg>,
    max_concurrent_files: Option<usize>,
    events: Option<EventsCfg>,
}
struct IngressCfg { path: PathBuf, recursive: bool, include: Vec<String>, exclude: Vec<String>,
                    rescan_secs: Option<u64>, readiness: ReadinessCfg }
enum ReadinessCfg { Stability{quiet_secs:u64}, Marker{suffix:String}, Rename, Glob{ready:Vec<String>} }
enum EgressCfg { Local(LocalCfg), S3(S3Cfg), Sftp(SftpCfg), Http(HttpCfg), Azure(AzureCfg), Gcs(GcsCfg) } // serde tag = "type"
enum ScheduleCfg { Immediate, Scheduled{ expression:String, timezone:Option<String>, on_window_close:WindowClose } }
struct CompletionCfg { on_success: OnSuccess, archive_dir: Option<PathBuf>, on_collision: Collision, verify: Verify }
```

---

## 8. The replication engine

### 8.1 Work-item state machine

```
Discovered ──readiness ok──▶ Ready ──schedule/window open + slot──▶ InProgress
     │                                                                  │
     │                                            per-destination upload + progress
     ▼                                                                  ▼
  (ignored: excluded / not ready yet)                        all destinations ok?
                                                             │              │
                                                        yes  ▼         no   ▼
                                                        Verified        Failed(attempt++)
                                                             │              │
                                                     completion action   backoff, requeue
                                                     (delete|archive)     (or exhausted→Failed-terminal)
                                                             ▼
                                                        Completed  ──▶ emit event, drop from queue
```

Every transition is written to the durable store **before** the side effect it authorizes (write-ahead),
so restart re-derives the exact position (§14).

### 8.2 Per-instance loop

1. **Watcher** (`notify`) + **reconciliation rescan** (every `rescanSecs`) discover candidate paths.
2. **Readiness gate** (§9) promotes `Discovered → Ready`.
3. **Scheduler** (§12) gates whether `Ready` work may start now (immediate = always; scheduled = only in
   window). It also wakes workers at window open and issues `ScheduleTriggered`.
4. **Workers** (bounded) pull `Ready` items, acquire a global slot, run the transfer, verify, complete.
5. **Retry manager** requeues `Failed` items with backoff until `maxAttempts`.

### 8.3 Backpressure & fairness

- Global + per-instance concurrency semaphores (FR-REL-5).
- Fair scan ordering: oldest-ready-first by default (configurable `order: fifo|lifo|smallest-first` to let
  small files drain quickly ahead of a huge one if desired).
- The queue is disk-backed, so a 100k-file spool costs disk, not RAM (NFR-3).

---

## 9. Readiness detection

A newly-observed file may still be mid-write. **Default strategy: stability window** (your choice). All
strategies are config-selectable and composable:

| Strategy | Behavior | Producer cooperation |
|---|---|---|
| **`stability`** (default) | Ready when `size`+`mtime` unchanged for `quietSecs` (default 5s). | none |
| `marker` | Ready when a companion marker appears (e.g. `FILE.done`); marker removed on completion. | producer writes marker |
| `rename` | Only react to files **renamed/moved into** the watch dir; ignore in-progress temp names. | producer write-then-rename |
| `glob` | Treat everything not matching `exclude`/temp globs as ready immediately (e.g. ignore `*.part`). | naming convention |

Notes:
- `stability` also guards against open write handles by re-stat polling; on Linux we *may* additionally
  check for writers via `/proc` when available (best-effort, not required).
- `rename` is the most robust for cooperative producers; docs will recommend it where the producer can be
  changed, with `stability` as the zero-cooperation default.
- Readiness is evaluated lazily and cheaply; only `Ready` items enter the durable queue.

---

## 10. Destinations

### 10.1 The `Destination` trait

Every backend implements one seam; the engine is backend-agnostic. This is the single most important
abstraction — it contains SDK-maturity risk, enables the 90% coverage gate (fakes in tests), and makes
multi-destination fan-out uniform.

```rust
#[async_trait]
trait Destination: Send + Sync {
    /// Human id for logs/events/stats.
    fn kind(&self) -> &'static str;

    /// Begin or RESUME delivering `item` (relative path, size, source reader factory).
    /// Reports progress via `progress`. Returns a completion token carrying the
    /// destination-side checksum/etag for verification.
    async fn deliver(&self, item: &WorkItem, resume: Option<ResumeState>,
                     progress: &ProgressSink) -> Result<Delivered>;

    /// Verify the delivered object matches the source (checksum/size), per policy.
    async fn verify(&self, item: &WorkItem, delivered: &Delivered, policy: Verify) -> Result<()>;

    /// Optional: clean up abandoned partial state (e.g. AbortMultipartUpload) on give-up.
    async fn abort(&self, item: &WorkItem, resume: &ResumeState) -> Result<()>;
}
```

`ResumeState` is persisted (§14) so `deliver` can pick up mid-transfer after a crash/window close.

### 10.2 Backend matrix

| Type | v1? | Resume support | Parallelism | Checksum/verify | Crate (candidate) | Feature |
|---|---|---|---|---|---|---|
| **local** | ✅ ship | temp-file + atomic rename; offset-append optional | n/a (single stream) | re-hash file | std/`tokio::fs` | always on |
| **s3** | ✅ ship | multipart parts persisted; resume remaining | parallel parts | S3-native CRC32C/SHA-256 + ETag | `aws-sdk-s3` | `dest-s3` (default) |
| **sftp / ftps** | design→phase | `APPE`/offset resume where server allows | single stream | size + optional hash | `russh`/`russh-sftp` (pure-Rust) | `dest-sftp` |
| **http(s)** | design→phase | `Content-Range`/resumable-PUT where server allows; else restart | single (or presigned-multipart) | server 2xx + optional `Content-MD5` | `reqwest` | `dest-http` |
| **azure blob** | design→phase | staged blocks persisted; resume uncommitted blocks | parallel blocks | MD5/CRC64 | `azure_storage_blobs` | `dest-azure` |
| **gcs** | design→phase | resumable-upload session URI persisted; resume by offset | single session (chunked) | CRC32C/MD5 | `google-cloud-storage` | `dest-gcs` |

Backends are cargo-feature-gated (batteries-included default = `local` + `dest-s3`), matching the
`telemetry-processor` feature philosophy. Immature crates stay off the default build.

### 10.3 Additional destination suggestions

Beyond what you selected, worth noting as future trait impls (cheap once the seam exists):

- **ggcommons durable stream / northbound MQTT** — hand the *file* (or a manifest/pointer) to the existing
  `gg.streams()` sink or publish an "arrived" notification northbound. Useful for "notify, don't move."
- **Another edge node** — an SFTP/HTTP peer is the pragmatic edge-to-edge hop; a dedicated
  `file-replicator`-to-`file-replicator` protocol is possible but SFTP/HTTP covers it.
- **NFS/SMB mount** — handled by the `local` destination pointing at a mounted share (no new code).
- **MinIO / S3-compatible** — the `s3` destination with a custom `endpointUrl` (no new code).

---

## 11. S3 destination — high-performance design

Directly addresses your S3 requirements (bucket+prefix, best-practice performance, vary by size, parallel,
resumable).

### 11.1 Size-adaptive upload strategy

- **Small files** (`size ≤ multipart.thresholdBytes`, default 16 MiB): single **`PutObject`** with a
  trailing checksum (`ChecksumAlgorithm=CRC32C`). One request, lowest overhead.
- **Large files** (`size > threshold`): **multipart upload** with **parallel part uploads**:
  - `partSizeBytes` default 16 MiB, **auto-scaled up** so `ceil(size/partSize) ≤ 10000` (S3 hard limit);
    respects the 5 MiB min part size.
  - `maxConcurrentParts` (default 4) parts in flight, each streamed from the source (no whole-file buffer).
  - Per-part checksums + a full-object checksum → S3 verifies integrity server-side; we compare on
    `CompleteMultipartUpload`.

### 11.2 Resumable multipart (FR-REL-3, FR-SCH-5)

- On `CreateMultipartUpload`, persist `{uploadId, key, partSize, completedParts:[{n, etag, checksum}]}` in
  the durable store keyed by the work item.
- On resume (retry or next window), **skip already-completed parts**, upload only the missing ones, then
  `CompleteMultipartUpload`. No re-transfer of good parts.
- On give-up (exhausted attempts) or config removal, `AbortMultipartUpload` to avoid orphaned part storage
  (and document an S3 lifecycle rule to sweep abandoned MPUs as defense-in-depth).

### 11.3 Integrity (matches "checksum verify always")

- Compute the checksum **on read** (streaming, single pass) and let S3 compute/verify the same algorithm;
  compare the returned checksum/ETag before marking the destination complete.
- Only after verification does the completion action (delete/archive) run (FR-CMP-3). This gives
  end-to-end, native, cheap integrity without a second read of the object.

### 11.4 Credentials & config

- Region, `endpointUrl` (for MinIO/S3-compat), `storageClass`, `sse` (`AES256`/`aws:kms` + `kmsKeyId`),
  optional object `tagging`/`metadata`.
- Credentials resolution order: explicit `{"$secret": ...}` (vault → `AwsCredentials` typed view) →
  Greengrass **TokenExchangeService** device role (on GG) → default provider chain / IRSA (on k8s/EC2).
  Reuses `aws-config` already in the dependency graph.
- **New direct dependency:** `aws-sdk-s3` (the ecosystem currently pulls `aws-sdk-kinesis`/`secretsmanager`
  /`kms`/`ssm` but **not** s3). Flagged in §20; it shares the pinned AWS SDK `1.x` line already in the lock.

---

## 12. Scheduling & windows

There is **no cron/calendar precedent** in the ecosystem, so this is net-new. We deliberately do **not**
expose cron and do **not** use a general NLP library. Instead we define a **small, closed, testable
grammar** and parse it into an internal schedule model.

### 12.1 Grammar (v1)

```
schedule   := "Every" spec ("at" time | "from" time "to" time)?
spec       := "day" | weekday | "hour" | "minute" | number ("minutes"|"hours"|"seconds")
weekday    := Monday..Sunday
time       := "2am" | "2:30am" | "14:00" | "5 AM" ...
```

Examples that MUST parse:
`"Every day at 5am"`, `"Every Wednesday at 2am"`, `"Every Wednesday from 2am to 4am"`,
`"Every hour"`, `"Every 15 minutes"`, `"Every day from 22:00 to 06:00"` (overnight window).

### 12.2 Internal model & evaluation

```rust
enum Recurrence { EveryDay, EveryWeekday(Weekday), EveryHour, EveryMinute, EveryN(Duration) }
struct Schedule { recur: Recurrence, at: Option<TimeOfDay>, window: Option<(TimeOfDay, TimeOfDay)>, tz: Tz }
impl Schedule {
    fn next_fire(&self, now: DateTime<Tz>) -> DateTime<Tz>;      // point trigger
    fn window_state(&self, now: DateTime<Tz>) -> WindowState;    // Open{until} | Closed{opens_at}
}
```

- **Timezone/DST-aware** via `chrono` + `chrono-tz` (pure-Rust). Overnight windows (start > end) handled.
- **Immediate mode** bypasses all of this (`ScheduleCfg::Immediate`).
- **Window semantics (FR-SCH-3/5):** while closed, `Ready` items wait; on open, the scheduler releases work
  and emits `WindowOpened`; if the window closes mid-transfer, `onWindowClose` decides `pause` (default —
  checkpoint resume state, resume next window) or `finishCurrent` (let the active file complete, admit no
  new files). Emits `WindowClosed` + `ScheduleComplete`.
- We reuse the **`SyncEngine` interval pattern** from ggcommons credentials (sleep in ≤1s steps so shutdown
  is honored promptly) for the scheduler tick rather than a long single sleep.

### 12.3 Why not cron-under-the-hood

We could translate English→cron→evaluator, but a purpose-built parser over a closed grammar is (a) exactly
the surface we advertise (no surprising cron semantics leak), (b) trivially unit-testable to the 90% gate,
(c) able to express **windows** (cron can't natively), and (d) dependency-light. If we ever need arbitrary
expressions, we can add an escape-hatch `"cron": "..."` field later — not now.

---

## 13. Completion, integrity & reliability

### 13.1 Integrity (default: checksum-verify-always — your choice)

- Single-pass streaming hash on read (CRC32C for object stores that support it natively; SHA-256 where a
  strong hash is wanted). Compared against the destination's reported checksum/ETag (S3), server response
  (HTTP), or a re-hash (local/SFTP) **before** completion.
- `completion.verify`: `checksum` (default) | `size` | `none`. Docs will strongly steer toward `checksum`
  given move+delete semantics.

### 13.2 Crash-safe completion (FR-CMP-4)

Ordering with write-ahead state (§14):

```
1. destination.deliver() succeeds
2. destination.verify() succeeds
3. persist item state = Verified(dest checksums)     ← write-ahead
4. completion action (delete source | move to archive)
5. persist item state = Completed; remove from active set
```

Recovery on restart:
- `InProgress` with resume state → resume the transfer.
- `Verified` but not `Completed` → **re-verify idempotently** (object already there & matches) then run the
  completion action. Never re-upload; never lose the source.
- Object stores use **stable keys** (deterministic from relative path + prefix) so re-delivery overwrites
  identically (idempotent), avoiding duplicates (FR-REL-4).

### 13.3 Retry (FR-REL-1/2)

- Bounded exponential backoff + jitter; `maxAttempts` then terminal `Failed` (retained, visible in
  statistics, retriable via `TriggerReplication`).
- Partial-success resume (FR-REL-3) means a retry continues, not restarts, wherever the backend supports it.

---

## 14. Durable state

The queue and resume checkpoints **must** survive crashes/reboots (NFR-5). Design:

- **Embedded, pure-Rust store: `redb`** (ACID, single-file, no C toolchain → builds on Windows and
  everywhere; aligns with NFR-6). Alternative considered: SQLite via `rusqlite` (bundled) — rejected as
  default because it needs a C compiler, which the org notes Windows lacks. (`sled` also considered; `redb`
  chosen for maturity + crash-safety guarantees.)
- **Per-instance state** under a component data dir (`/data/...` on k8s PVC; component work dir on
  HOST/GG). Tables:
  - `work_items`: key = `(instance, relpath)`, value = `{state, size, discovered_at, attempts, last_error}`.
  - `resume`: key = work item, value = destination `ResumeState` (S3 uploadId+completed parts, GCS session
    URI, SFTP offset, ...).
  - `stats`: per-instance counters (replicated, failed, bytes) for fast `GetStatus`.
- **Write-ahead discipline:** state transition persisted before the authorized side effect (§13.2).
- Mirrors the *spirit* of the ggstreamlog durable buffer (disk-backed, crash-safe, fsync policy) but is
  file-oriented rather than record-oriented, so it's a purpose-built store, not a reuse of ggstreamlog.

---

## 15. Control-message suite

Uses the ggcommons request/reply primitive (`MessagingService::request`/`reply`, `reply_to` header). There
is **no generic control framework in core today** — each component wires its own `subscribe` handlers — so
we build a small local **control dispatcher** (message-name → handler), designed to be liftable into core.

### 15.1 Commands

| Command (`header.name`) | Topic (default) | Body | Reply |
|---|---|---|---|
| **`GetConfiguration`** v1.0 | `ggcommons/{ThingName}/config/get/{ComponentName}` (core contract) | `{}` | effective config document |
| **`GetStatus`** v1.0 | `edgecommons/filereplicator/{ThingName}/control` | `{ "instance": "?" }` (optional filter) | per-instance statistics (below) |
| **`TriggerReplication`** v1.0 | same control topic | `{ "instance": "?", "ignoreWindow": bool }` | accepted + counts scheduled |

`GetStatus` reply body (per instance):

```jsonc
{
  "instance": "plant-csv-to-s3",
  "schedule": { "mode": "scheduled", "window": "Open", "windowClosesAt": "..." },
  "awaiting":  { "count": 12, "bytes": 48210233,
                 "files": [ { "path": "a/b.csv", "size": 40211, "ageSecs": 91 } ] },
  "inProgress":[ { "path": "big.parquet", "size": 734003200, "bytesDone": 220200960, "percent": 30.0,
                   "destination": "s3", "attempt": 1 } ],
  "replicated":{ "count": 4310, "bytes": 90230411223, "last": { "path": "...", "at": "..." } },
  "failed":    { "count": 2, "items": [ { "path": "x.csv", "attempts": 10, "lastError": "..." } ] }
}
```

### 15.2 "Promote GetConfiguration to core?" — recommendation

**Short answer: partially, and not as a blocker.**

- Core **already defines** the `GetConfiguration` v1.0 *requester* (it's a config *source* — topic + message
  name are fixed and cross-language). It does **not** ship a **responder** or any generic control framework.
- **Recommendation (this component):** implement the **responder** side locally now, answering the existing
  `GetConfiguration` contract — zero new core surface, immediately useful, and consistent with the standard
  topic/name.
- **Recommendation (ggcommons core, separate proposal):** promote a small, opt-in **control-surface helper**
  into core with:
  1. a built-in `GetConfiguration` responder (every component benefits — returns the effective config), and
  2. a `GetStatus`/health responder returning a standard liveness/summary, and
  3. a message-name→handler **registry** components extend with their own commands (like our
     `TriggerReplication`).

  This is genuinely reusable (it's not file-replicator-specific), but it's **net-new infrastructure that all
  four languages must mirror** (the four-way-parity rule). So: build it in `file-replicator` first as the
  proving ground, extract to core once the shape is validated. I've left `control.rs` structured so the
  dispatcher + `GetConfiguration`/`GetStatus` handlers are the extractable part.

I'll file this as a ggcommons issue proposal for your sign-off rather than changing core under this repo.

---

## 16. Status/event publishing (realtime UI)

One-way events on user-configurable topics with intelligent defaults, so a UI can subscribe and render
live. Envelope = standard ggcommons `Message`; `header.name = "FileReplicatorEvent"`, `version = "1.0"`;
`body.event` discriminates.

### 16.1 Event types

`FileDiscovered`, `FileReady`, `ReplicationStarted`, `ReplicationProgress`, `ReplicationCompleted`,
`ReplicationFailed`, `FileArchived`, `FileDeleted`, `ScheduleTriggered`, `WindowOpened`, `WindowClosed`,
`ScanComplete`.

### 16.2 Topics (defaults, all overridable)

```
edgecommons/filereplicator/{ThingName}/{instance}/events        (all events, default)
edgecommons/filereplicator/{ThingName}/{instance}/progress      (progress only, higher volume)
```

Config allows either a single events topic or per-event-type topic templates (`{ThingName}`, `{instance}`,
`{event}` tokens resolved via the ggcommons template resolver).

### 16.3 Event body (example — progress)

```jsonc
{ "event": "ReplicationProgress", "instance": "plant-csv-to-s3",
  "path": "big.parquet", "size": 734003200, "bytesDone": 220200960, "percent": 30.0,
  "destination": "s3", "attempt": 1, "ts": "2026-07-01T09:15:22Z" }
```

- **Throttling (FR-EVT-2):** progress emitted on ≥`progressPercentStep` (default 10%) **or** every
  `progressIntervalSecs` (default 5s), whichever first; `0%`/`100%` always emitted.
- QoS default `AtMostOnce` for progress (lossy-ok), `AtLeastOnce` for lifecycle transitions.

---

## 17. Observability, platform & packaging

### 17.1 Metrics (`gg.metrics()`)

`files_discovered`, `files_replicated`, `files_failed`, `bytes_replicated`, `in_progress`, `queue_depth`,
`retry_count`, `upload_duration_ms` (histogram-ish via measures), `window_open` (gauge). Target resolves per
platform (prometheus on k8s, log elsewhere) — no component-side branching.

### 17.2 Platform support

- Platform/transport entirely via the ggcommons resolver (HOST→FILE/MQTT, GG→GG_CONFIG/IPC,
  K8S→CONFIGMAP/MQTT). No `#[cfg(platform)]` in engine code; only cargo features gate backends + the
  Linux-only `greengrass` (IPC) feature (held OFF by default like `telemetry-processor`).
- Health (`/livez` `/readyz` `/startupz`) and structured JSON logging come from the library on k8s.
- `gg.set_ready(false)` until each instance's watcher+queue is initialized; `true` when replicating.
- SIGTERM → `gg.shutdown_signal()` → checkpoint resume state, pause/drain, flush, drop.

### 17.3 Deploy artifacts (three, kept in sync on the full name)

- **`recipe.yaml`** + **`build.sh`** + **`gdk-config.json`** — Greengrass (declares TokenExchangeService for
  device-role S3 creds; pubsub/mqttproxy access control; `Run: ... --platform GREENGRASS -c GG_CONFIG`).
- **`Dockerfile`** + **`k8s/{configmap,deployment}.yaml`** — Kubernetes (needs a **PVC** for the durable
  state + archive/spool; ConfigMap mounted as a whole volume for hot-reload; Downward-API identity;
  health/metrics ports).
- **`test-configs/`** — HOST samples (config + dual-MQTT messaging block).

### 17.4 CI

One caller workflow → `edgecommons/.github/.github/workflows/component-ci.yml@main` with
`language: RUST`, `rust-features: "dest-s3,dest-sftp,dest-http"` (compile+test the shippable backends),
`secrets: inherit` (private-git PAT). We **add the 90% coverage gate** in-repo
(`cargo llvm-cov --fail-under-lines 90`) since the reusable workflow doesn't enforce it.

### 17.5 Docs (post-approval, Diátaxis, no frontmatter — synced to Starlight)

```
docs/README.md · tutorial.md · how-to-guides.md · sample-configurations.md · explanation.md
docs/reference/configuration.md · reference/messaging-interface.md · reference/destinations.md
```

`configuration.md` becomes the canonical config spec (there is no per-component JSON schema in the
ecosystem; the reference doc is the source of truth, matching `telemetry-processor`).

---

## 18. Open decisions & recommendations

Your explicit questions, answered. **Items A–D reflect your selections; E–G are my recommendations for
your sign-off.**

- **A. Additional destinations — DECIDED (you):** design for **SFTP/FTPS, HTTP(S), Azure Blob, GCS** (plus
  local + S3). All behind the `Destination` trait + cargo features; **ship `local` + `s3` first**, others
  phased (§19). ✔
- **B. Multiple destinations (fan-out) — DECIDED (you):** **design multi, ship single first.** `egress` is a
  list; v1 validates `len == 1`; fan-out semantics fully specified (FR-EGR-3: done-when-all-succeed,
  independent per-destination retry, single completion). ✔
- **C. On-complete placement — RECOMMEND: separate `completion` section (not inside `egress`).** Rationale:
  with fan-out, completion is a **source-file lifecycle** decision spanning *all* destinations, so it can't
  belong to a single egress entry; it's conceptually distinct (source cleanup vs delivery) and keeps the
  schema clean. Adopted in §7. ✔ (your call to confirm)
- **D. Readiness default — DECIDED (you):** **stability window** default; `marker`/`rename`/`glob` selectable
  (§9). Integrity: **checksum-verify-always** default (§13). ✔
- **E. GetConfiguration → core — RECOMMEND: implement responder locally now; propose a core control-surface
  helper separately** (§15.2). Not a blocker; core change is a distinct four-language proposal.
- **F. Durable state engine — RECOMMEND: `redb` (pure-Rust)** over SQLite/sled for Windows-buildability and
  crash-safety (§14). Confirm if you'd prefer SQLite (then Windows dev builds need WSL, like Lua).
- **G. Category — RECOMMEND: `sink`** in the registry (§1, §20). Confirm vs `processor`.

**Additional decisions I made with defaults (flag if you disagree):**
- Idempotent re-delivery via **stable, deterministic object keys** (FR-REL-4).
- Progress throttle defaults: **10% or 5s**.
- Global + per-instance **concurrency caps** (defaults 8 / 4).
- Scheduling via a **closed English grammar**, not cron-under-the-hood, with a possible future `cron:`
  escape hatch (§12.3).

---

## 19. Phased implementation plan

Design stays whole; implementation lands in reviewable slices, each green on the 90% gate + all 3 platforms.

| Phase | Scope | Exit criteria |
|---|---|---|
| **P0 — Scaffold** | Repo skeleton from the Rust template, config model, module stubs, CI caller, docs shell, registry entry PR. | `cargo build/test/clippy` green; empty engine runs on HOST. |
| **P1 — Core engine (local dest)** | Watcher + readiness(stability) + durable queue(redb) + worker + completion(delete/archive) + integrity(checksum) + retry, **local destination**, immediate mode. | Move files local→local, crash-safe, verified, on all 3 platforms; ≥90% cov. |
| **P2 — S3 destination** | Size-adaptive PutObject/multipart, parallel parts, resumable, S3-native checksums, vault/`$secret` + GG TES + IRSA creds. | Validated vs floci (local AWS emulator) + a real bucket; resume-after-kill proven. |
| **P3 — Control + events** | Control dispatcher (GetConfiguration/GetStatus/TriggerReplication) + event publisher (all event types, throttling) + metrics. | Live status via control msg; UI-ready event stream verified. |
| **P4 — Scheduling & windows** | English grammar parser + schedule/window engine + window-boundary pause/resume. | Windowed uploads validated incl. overnight + DST; mid-window resume proven. |
| **P5 — Additional destinations** | SFTP/FTPS, HTTP(S), then Azure/GCS (feature-gated). | Each backend validated + resume where supported; features off default until stable. |
| **P6 — Multi-destination fan-out** | Lift the `len==1` egress constraint; parallel fan-out + aggregate completion. | N-destination delivery, per-dest retry, single completion, verified. |
| **P7 — Core promotion (optional)** | Extract control-surface helper → ggcommons (4-lang), if approved. | Parity + gates in all four langs. |

Validation uses the standard matrix: HOST→Windows + EMQX/floci; GREENGRASS→lab-5950x; k8s→kind + lab-k3s;
Rust `greengrass`/native builds→WSL.

---

## 20. Appendix — registry entry, deps, repo scaffold

### 20.1 Proposed `registry/components.json` entry (PR to `edgecommons/registry`)

```json
{
  "name": "file-replicator",
  "repo": "edgecommons/file-replicator",
  "language": "RUST",
  "category": "sink",
  "description": "Rust reference sink: watches directories and replicates files to S3, local, SFTP/FTPS, HTTP, Azure Blob and GCS — on arrival or on plain-English schedules/windows — with resumable uploads, integrity verification, and a full control/event surface.",
  "status": "experimental",
  "platforms": ["GREENGRASS", "HOST", "KUBERNETES"],
  "library": "ggcommons",
  "topics": ["edgecommons", "edgecommons-sink", "aws-iot-greengrass", "iiot", "file-replication", "s3"]
}
```

GitHub repo topics mirror `topics`. (Note: the registry has a strict JSON schema — validate with
`check-jsonschema` before the PR.)

### 20.2 New / notable dependencies (all pure-Rust unless noted)

| Crate | Purpose | Notes |
|---|---|---|
| `ggcommons` | the library | pinned by git rev; local sibling via `.cargo/config.toml` (gitignored) |
| `tokio` | async runtime | features `rt-multi-thread,macros,signal,time,sync,fs` |
| `notify` | filesystem watch | already used by ggcommons config watcher (proven) |
| `redb` | durable state | pure-Rust, ACID, single-file |
| `chrono` + `chrono-tz` | schedule/window TZ+DST | pure-Rust |
| `globset` | include/exclude globs | pure-Rust |
| `crc32c` / `sha2` | integrity hashing | pure-Rust |
| `aws-sdk-s3` + `aws-config` | S3 | **new** to the ecosystem (kinesis/sm/kms/ssm exist; s3 does not); same `1.x` line |
| `reqwest` | HTTP dest | rustls TLS |
| `russh` / `russh-sftp` | SFTP/FTPS | pure-Rust; `dest-sftp` (off default) |
| `azure_storage_blobs` / `google-cloud-storage` | Azure/GCS | younger crates; features off default |

Cargo features (batteries-included default, native/immature held off — the `telemetry-processor` pattern):

```toml
default    = ["standalone", "dest-s3"]
standalone = ["ggcommons/standalone"]
greengrass = ["ggcommons/greengrass"]         # Linux-only IPC, OFF by default
cloudwatch = ["ggcommons/cloudwatch"]
dest-s3    = ["dep:aws-sdk-s3", "dep:aws-config"]
dest-sftp  = ["dep:russh", "dep:russh-sftp"]
dest-http  = ["dep:reqwest"]
dest-azure = ["dep:azure_storage_blobs"]      # OFF by default (crate maturity)
dest-gcs   = ["dep:google-cloud-storage"]     # OFF by default (crate maturity)
```

### 20.3 Repo scaffold (created after design approval)

```
file-replicator/
  DESIGN.md            ← this document
  README.md            (created)
  LICENSE              Apache-2.0 (created — fills the org-wide LICENSE gap)
  CLAUDE.md            component notes (created)
  .gitignore           (created)
  Cargo.toml Cargo.lock
  src/…                (§6.2)
  recipe.yaml build.sh gdk-config.json
  Dockerfile k8s/{configmap,deployment}.yaml
  test-configs/{config.json,standalone-messaging.json}
  docs/…               (§17.5, Diátaxis)
  .github/workflows/ci.yml   (calls the org reusable CI + coverage gate)
```

---

### Review checklist for you

1. **§18-C** — confirm `completion` as its own section (vs folding into `egress`).
2. **§18-E** — confirm approach to `GetConfiguration` (local responder now, core proposal later).
3. **§18-F** — confirm `redb` for durable state (vs SQLite).
4. **§18-G** — confirm registry `category: "sink"`.
5. **§12.1** — confirm the schedule grammar covers your real phrasings (add examples if not).
6. **§10.3 / §16.2 / §15.1** — confirm default topics + whether the ggcommons-stream "notify" destination is wanted.
7. **§19** — confirm the phase ordering / MVP cut (P1+P2 = usable local+S3 mover).

On your sign-off I'll scaffold P0 (repo from the Rust template, config model, CI, docs shell) and open the
registry PR.
