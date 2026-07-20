# file-replicator

> EdgeCommons **sink** component (Rust) · full name `com.mbreissi.edgecommons.FileReplicator`

Watches local directories and **replicates files** to one or more destinations — another local directory,
**S3** (on by default), and SFTP/FTPS, HTTP(S), Azure Blob, and GCS (each behind its own cargo feature, off
by default pending crate-maturity confidence) — either **as files arrive** (default) or on a
**plain-English schedule/window** (e.g. *"Every Wednesday from 2am to 4am"*).

Handles source lifecycle on completion (**delete** or **archive**), retries with **resumable** partial
uploads, and exposes a full **control-message** surface (get-config / get-status+statistics /
trigger-now) plus granular **status events** and CloudWatch-friendly metric groups for building a
realtime UI. Supports multi-destination fan-out (an ordered `egress` list, `N >= 1`).

Runs standalone (**HOST**), on **Greengrass** (IPC), and on **Kubernetes** (ConfigMap) via the
`edgecommons` library — no platform-specific code.

## Docs

User docs live under [`docs/`](docs/) (Diátaxis) and sync into the EdgeCommons docs site. The canonical
configuration reference is [`docs/reference/configuration.md`](docs/reference/configuration.md); the full
per-backend field tables are in [`docs/reference/destinations.md`](docs/reference/destinations.md).
`DESIGN.md` carries the deeper design rationale and the decision register.

## License

Business Source License 1.1 — see [LICENSE](LICENSE).
