# file-replicator

EdgeCommons **sink** component (Rust) that watches local directories and **replicates files** to one or
more destinations — another local directory, **S3**, and (phased) SFTP/FTPS, HTTP(S), Azure Blob and GCS —
either **as files arrive** (default) or on a **cron schedule/window**. It handles source lifecycle on
completion (delete / archive / quarantine), retries with resumable partial uploads, throttles bandwidth,
can be activated/deactivated per instance from the control plane, and publishes granular status events on a
unified namespace for realtime UIs. Runs standalone (**HOST**), on **Greengrass** (IPC), and on
**Kubernetes** (ConfigMap) via the `ggcommons` library.

> **Status: design + scaffold (P0).** The authoritative specification is
> [`DESIGN.md`](https://github.com/edgecommons/file-replicator/blob/main/DESIGN.md) in the repo root. These
> docs are authored per implementation phase (see DESIGN §21) and validated against the source — start with
> the design doc for anything not yet covered here.

## Where to go

| If you want to… | Read |
|---|---|
| Learn by doing (first replication) | [Tutorial](tutorial.md) |
| Accomplish a specific task | [How-to guides](how-to-guides.md) |
| Copy a complete, annotated config | [Sample configurations](sample-configurations.md) |
| Understand the design & concepts | [Explanation](explanation.md) |
| Look up every config field | [Reference › Configuration](reference/configuration.md) |
| Integrate with control/events | [Reference › Messaging interface](reference/messaging-interface.md) |
| Compare destination backends | [Reference › Destinations](reference/destinations.md) |
