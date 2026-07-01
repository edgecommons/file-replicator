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

## Key design choices (see DESIGN.md for rationale)
- **Pure-Rust deps** for new subsystems so it builds on Windows (no C toolchain): `redb` (durable state),
  `chrono`+`chrono-tz` (schedule), `globset`, `notify`, `crc32c`/`sha2`.
- **New ecosystem dep:** `aws-sdk-s3` (kinesis/sm/kms/ssm exist in ggcommons; s3 did not).
- Backends behind a `Destination` trait + cargo features; batteries-included default = `local` + `dest-s3`;
  immature crates (azure/gcs) OFF by default (the `greengrass`/`streaming-kafka` pattern).
- Scheduling is a **closed English grammar**, not cron.
- Durable, write-ahead state → crash-safe move+delete with checksum-verify-before-complete.

## Registry
Add to `../registry/components.json` as `category: "sink"` (entry drafted in DESIGN §20.1); validate with
`check-jsonschema` before the PR. Mirror `topics` to GitHub repo topics.
