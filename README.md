# file-replicator

> EdgeCommons **sink** component (Rust) · full name `com.mbreissi.edgecommons.FileReplicator`

Watches local directories and **replicates files** to one or more destinations — another local directory,
**S3**, and (designed, phased) SFTP/FTPS, HTTP(S), Azure Blob, and GCS — either **as files arrive**
(default) or on a **plain-English schedule/window** (e.g. *"Every Wednesday from 2am to 4am"*).

Handles source lifecycle on completion (**delete** or **archive**), retries with **resumable** partial
uploads, and exposes a full **control-message** surface (get-config / get-status+statistics /
trigger-now) plus granular **status events** for building a realtime UI.

Runs standalone (**HOST**), on **Greengrass** (IPC), and on **Kubernetes** (ConfigMap) via the
`ggcommons` library — no platform-specific code.

## Status

🚧 **Design phase.** No code yet. The full requirements & design are in **[DESIGN.md](DESIGN.md)** —
please review that first. Implementation is scaffolded only after design sign-off (see the phase plan in
DESIGN §19).

## Docs

Once implemented, user docs live under [`docs/`](docs/) (Diátaxis) and sync into the EdgeCommons docs
site. The canonical configuration reference will be `docs/reference/configuration.md`.

## License

Apache-2.0 — see [LICENSE](LICENSE).
