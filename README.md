# file-replicator

> EdgeCommons **sink** component (Rust) · full name `com.mbreissi.edgecommons.FileReplicator`

Watches local directories and **replicates files** to one or more destinations — another local directory,
**S3** (on by default), and SFTP/FTPS, HTTP(S), Azure Blob, and GCS (implemented, off by default behind
cargo features pending crate-maturity confidence) — either **as files arrive** (default) or on a
**plain-English schedule/window** (e.g. *"Every Wednesday from 2am to 4am"*).

Handles source lifecycle on completion (**delete** or **archive**), retries with **resumable** partial
uploads, and exposes a full **control-message** surface (get-config / get-status+statistics /
trigger-now) plus granular **status events** and CloudWatch-friendly metric groups for building a
realtime UI. Supports multi-destination fan-out (an ordered `egress` list, `N >= 1`).

Runs standalone (**HOST**), on **Greengrass** (IPC), and on **Kubernetes** (ConfigMap) via the
`edgecommons` library — no platform-specific code.

## Status

**Implemented through P6** of the phase plan in **[DESIGN.md](DESIGN.md) §21** (scaffold → local engine →
S3 → control/events/UNS → cron/window scheduling → the additional destinations above → multi-destination
fan-out); CI enforces a 90%-line coverage gate across every destination feature. Only **P7** (optional
promotion of the UNS/control-surface helper into `edgecommons`) remains undone. DESIGN.md is still the
authoritative spec for rationale — but treat any framing there that reads "nothing is implemented yet"
as stale; check `src/` and `git log` for actual status.

## Docs

User docs live under [`docs/`](docs/) (Diátaxis) and sync into the EdgeCommons docs site — they are
authored per phase and, as of P6, lag the implementation in places (each page notes its own coverage).
The canonical configuration reference is `docs/reference/configuration.md`.

## License

Apache-2.0 — see [LICENSE](LICENSE).
