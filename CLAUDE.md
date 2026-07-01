# file-replicator — component notes

EdgeCommons **sink** component (Rust). Full name `com.mbreissi.greengrass.FileReplicator`. Depends on the
`ggcommons` Rust library. Read the org umbrella `../CLAUDE.md` first (platform matrix, validation infra,
local-dev sibling override).

## What it is
Watches local dirs and replicates files → destinations (local, S3; designed: SFTP/FTPS, HTTP, Azure, GCS),
on-arrival or on plain-English schedules/windows; delete/archive on complete; resumable uploads; full
control-message + event surface. Runs HOST / GREENGRASS / KUBERNETES via ggcommons (no platform branching).

## Current status
**Design phase.** The authoritative spec is [`DESIGN.md`](DESIGN.md). No code scaffolded yet — implementation
follows the phase plan (DESIGN §19) after sign-off. Do not write engine code before the design is approved.

## Template & conventions (mirror `../telemetry-processor`)
- `main.rs` = `GgCommonsBuilder::new(NAME).args(env::args_os()).build().await?` → `App::start`/`app.run`.
- Config: own subtree under `component.global`/`component.instances[]`; standard ggcommons sibling sections;
  `#[serde(rename_all="camelCase")]`; lenient int-or-float numbers (GG doubles); skip-bad-instance,
  fail-only-if-zero.
- Instance sections: `ingress` / `egress` (list, v1 len==1) / `schedule` / `completion` / `retry`.
- Three deploy artifacts kept in sync on the full name: `recipe.yaml`(+`build.sh`,`gdk-config.json`),
  `Dockerfile`+`k8s/`, `test-configs/`.
- CI: one caller → `edgecommons/.github/.github/workflows/component-ci.yml@main` (`language: RUST`,
  `rust-features`, `secrets: inherit`) + in-repo 90% gate (`cargo llvm-cov --fail-under-lines 90`).
- Docs: Diátaxis `.md` (+`.mdx` only where Starlight `<Tabs>` needed), **no frontmatter**; synced to the site.

## Key design choices (see DESIGN.md v0.2 for rationale)
- **Durable state = SQLite** (`rusqlite` bundled, WAL) — DECIDED (user confirmed). Chosen over redb now the
  Windows C toolchain exists ([[windows-msvc-build-toolchain]]): crash-safe AND better fit for the
  get-status/stats query surface + operational introspection. `redb` kept only as a `state-redb` fallback
  feature behind the same `state.rs` trait.
- **New ecosystem dep:** `aws-sdk-s3` (kinesis/sm/kms/ssm exist in ggcommons; s3 did not).
- Backends behind a `Destination` trait + cargo features; default = `local` + `dest-s3`; immature crates
  (azure/gcs) OFF by default (the `greengrass`/`streaming-kafka` pattern).
- **Scheduling = cron-first** (`croner`, tz/DST-aware); plain-English is optional sugar → cron. Windows =
  `open`+`close` (or `open`+`durationMins`) crons; `onWindowClose` = pauseResume (resume if dest supports,
  else finishCurrent) | finishCurrent.
- **Unified namespace** for all cmd/evt/state: **`{thing}/file-replicator/{cmd|evt|state}/{resource…}`** —
  rooted on the IoT-Core-globally-unique ThingName; NO vendor root/version/site/enterprise in the path
  (version is in the envelope; site/enterprise are unreliable tags → carried in the envelope for
  consumer-side filtering). Fits IoT Core's 256-byte / 7-slash limits. Retained `state/…` snapshots.
  Configurable prefix (default `{ThingName}/file-replicator`). Core `ggcommons/…` topics = separate
  migration proposal + optional legacy `GetConfiguration` alias.
- Durable **write-ahead** state → crash-safe move+delete with checksum-verify-before-complete.
- **Long-outage tolerant** (hours–~2d): time-based `giveUpAfter` (default 7d, not attempt caps), resume
  in-flight, disconnection circuit-breaker.
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
