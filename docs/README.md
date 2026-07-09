# file-replicator

EdgeCommons **sink** component (Rust) that watches local directories and **replicates files** to one or
more destinations — another local directory, **S3**, and — behind optional build features — SFTP/FTPS,
HTTP(S), Azure Blob and GCS — either **as files arrive** (default) or on a **cron schedule/window**. It
handles source lifecycle on
completion (delete / archive / quarantine), retries with resumable partial uploads, throttles bandwidth,
can be activated/deactivated per instance from the control plane, and publishes granular status events on a
unified namespace for realtime UIs. Runs standalone (**HOST**), on **Greengrass** (IPC), and on
**Kubernetes** (ConfigMap) via the `edgecommons` library.

> These docs are validated against `src/`. The deep design rationale lives in
> [`DESIGN.md`](https://github.com/edgecommons/file-replicator/blob/main/DESIGN.md) in the repo root.

## Where to go

| If you want to… | Read |
|---|---|
| Learn by doing (first replication) | [Tutorial](tutorial.md) |
| Accomplish a specific task | [How-to guides](how-to-guides.md) |
| Copy a complete, annotated config | [Sample configurations](sample-configurations.md) |
| Understand the design & concepts | [Explanation](explanation.md) |
| Look up every config field | [Reference › Configuration](reference/configuration.md) |
| Integrate with control/events | [Reference › Messaging interface](reference/messaging-interface.md) |
| Interpret emitted metrics | [Reference › Metrics](reference/metrics.md) |
| Compare destination backends + every field | [Reference › Destinations](reference/destinations.md) |
| Parse `get-status` + event `context` payloads | [Reference › Data types](reference/data-types.md) |
