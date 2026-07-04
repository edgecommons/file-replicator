# file-replicator — component notes

EdgeCommons **sink** component (Rust). Full name `com.mbreissi.edgecommons.FileReplicator`. Depends on the
`ggcommons` Rust library. Read the org umbrella `../CLAUDE.md` first (platform matrix, validation infra,
local-dev sibling override).

## What it is
Watches local dirs and replicates files → destinations (local, S3 default-on; SFTP/FTPS, HTTP(S), Azure Blob,
GCS implemented behind off-by-default cargo features, DESIGN §10), on-arrival or on plain-English
schedules/windows; delete/archive on complete; resumable uploads; full control-message + event surface. Runs
HOST / GREENGRASS / KUBERNETES via ggcommons (no platform branching).

## Current status
**Implemented through P6 of the DESIGN §21 phase plan** (P0 scaffold → P1 local engine → P2 S3 → P3
control+events+UNS → P4 cron/window scheduling → P5 SFTP/FTPS/HTTP/Azure/GCS → P6 multi-destination
fan-out — see `git log --oneline` for the per-phase commits), **plus the UNS-core migration** (onto
`ggcommons` branch `feat/unified-namespace`): `evt`/`cmd` now ride the library's `events()` facade +
`commands()` inbox (`ecv1/{device}/FileReplicator/{instance}/{class}…`), replacing the old hand-rolled
`{thing}/file-replicator/…` topic builder (`src/uns.rs`, deleted) — see
`docs/reference/messaging-interface.md` for the current wire contract (topics/verbs/event catalog) and
`src/events.rs`/`src/control.rs` module docs for the migration rationale. CI (`.github/workflows/ci.yml`)
builds with every destination feature enabled and enforces the 90%-line coverage gate across that full
surface. Only **P7 (optional core promotion of the UNS/control-surface helper into `ggcommons`)** has not
been started — largely moot now the UNS core itself IS that promotion.

**Migration-driven feature drops (not regressions — see the design rationale in `src/events.rs`/
`src/control.rs`):** the retained `state/…` snapshot publish is gone outright (`state` is now a reserved,
library-owned UNS class carrying only the RUNNING/STOPPED keepalive; `get-status` is the current-state-
on-demand path, as it always was for a late subscriber) — this closes the old "no `publish_retained`"
gap by removing the need for it, not by adding it. The custom `get-config` verb + `legacyConfigTopic`
alias are retired (the library's built-in `get-configuration` verb already answers this, more safely —
redacted, not just unresolved `$secret` refs). `component.global.topics.prefix` /
`InstanceCfg.topics` config are removed (no configurable prefix in the fixed UNS grammar).

Still-deliberately-deferred gaps (unrelated to the UNS migration, see DESIGN.md's own inline notes): the
destination disconnection circuit-breaker + `get-status` `link`/window sub-fields (§13.4/§16 — event
types exist in `events.rs` but are marked `Deferred`; `Disconnected`/`Reconnected` now share ONE evt wire
type, `disconnected`, so the eventual raise/clear pair rides one channel), per-schedule bandwidth, and
`keyShardBits` S3 prefix sharding (neither has any implementation). `DESIGN.md` remains the authoritative
spec for rationale; treat any of its wording that still reads "nothing is implemented", "before
scaffolding", or describes the pre-migration topic scheme as stale relative to the code — check `src/`
first.

## Template & conventions (mirror `../telemetry-processor`)
- `main.rs` = `GgCommonsBuilder::new(NAME).args(env::args_os()).build().await?` → `App::start`/`app.run`.
- Config: own subtree under `component.global`/`component.instances[]`; standard ggcommons sibling sections;
  `#[serde(rename_all="camelCase")]`; lenient int-or-float numbers (GG doubles); skip-bad-instance,
  fail-only-if-zero.
- Instance sections: `ingress` / `egress` (ordered list, `N >= 1` — multi-destination fan-out shipped P6,
  DESIGN §20-B) / `schedule` / `completion` / `retry`.
- Three deploy artifacts kept in sync on the full name: `recipe.yaml`(+`build.sh`,`gdk-config.json`),
  `Dockerfile`+`k8s/`, `test-configs/`.
- CI: one caller → `edgecommons/.github/.github/workflows/component-ci.yml@main` (`language: RUST`,
  `rust-features`, `secrets: inherit`) + in-repo 90% gate (`cargo llvm-cov --fail-under-lines 90`).
- Docs: Diátaxis `.md` (+`.mdx` only where Starlight `<Tabs>` needed), **no frontmatter**; synced to the site.

## Key design choices (see DESIGN.md v0.2 for rationale)
- **Durable state = SQLite** (`rusqlite` bundled, WAL) — DECIDED (user confirmed) and SHIPPED as the sole
  `StateStore` impl (`SqliteStore`, `src/state.rs`). Chosen over redb now the Windows C toolchain exists
  ([[windows-msvc-build-toolchain]]): crash-safe AND better fit for the get-status/stats query surface +
  operational introspection. A `redb`-backed `state-redb` fallback behind the same `state.rs` trait was
  proposed but **never implemented** — no `redb` dependency or `state-redb` feature exists in `Cargo.toml`;
  treat DESIGN.md mentions of it as an unbuilt future option, not shipped behavior.
- **New ecosystem dep:** `aws-sdk-s3` (kinesis/sm/kms/ssm exist in ggcommons; s3 did not).
- Backends behind a `Destination` trait + cargo features; default = `local` + `dest-s3`; immature crates
  (azure/gcs) OFF by default (the `greengrass`/`streaming-kafka` pattern).
- **Scheduling = cron-first** (`croner`, tz/DST-aware); plain-English is optional sugar → cron. Windows =
  `open`+`close` (or `open`+`durationMins`) crons; `onWindowClose` = pauseResume (resume if dest supports,
  else finishCurrent) | finishCurrent.
- **Unified namespace, ggcommons-core-owned**: `ecv1/{device}/FileReplicator/{instance}/{cmd|evt}/…`,
  minted by the library's `commands()`/`events()` facades — no hand-rolled topic builder, no
  configurable prefix, no legacy alias (all retired in the UNS-core migration; see
  `docs/reference/messaging-interface.md`). `state`/`cfg`/`metric` are reserved, library-owned classes.
- Durable **write-ahead** state → crash-safe move+delete with checksum-verify-before-complete.
- **Long-outage tolerant** (hours–~2d): time-based `giveUpAfter` (default 7d, not attempt caps, shipped) and
  resume in-flight (shipped). The **disconnection circuit-breaker** (§13.4) is still **not implemented** —
  `Event::Disconnected`/`Reconnected` exist as enum variants in `src/events.rs` marked `Deferred`, and
  `get-status`'s `link` field stays omitted (tested in `src/control.rs`) until it lands.
- **Bandwidth caps** (per-instance + global token bucket); **per-instance activation** persisted across
  restarts (runtime state wins over config `enabled`), toggled via control message; **Failed folder**
  quarantine (opt-in) with `.error.json` sidecar.
- **S3 creds ambient by default** (GG TES / k8s IRSA / HOST env), `$secret` optional; S3 perf =
  Transfer Acceleration + trailing checksums + unsigned-PUT + prefix parallelism, size-adaptive multipart.
- **Docs**: extensive annotated config samples + deep teaching explanation, validated against `config.rs`
  (not other docs) — NOT a syntax rehash. Mermaid diagrams globally.

## Registry
Add to `../registry/components.json` as `category: "sink"` (entry drafted in DESIGN §20.1); validate with
`check-jsonschema` before the PR. Mirror `topics` to GitHub repo topics.
