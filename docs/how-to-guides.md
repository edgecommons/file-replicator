# How-to guides

> **Status: authored per phase** as each capability lands (DESIGN §21). Each guide is task-focused and
> grounded in the shipped behaviour. Planned guides:

- **Replicate a directory to S3** (P2) — bucket/prefix, ambient vs `$secret` credentials, acceleration.
- **Schedule a nightly upload window** (P4) — cron `open`/`close`, overnight windows, DST.
- **Cap upload bandwidth during operating hours** (P1) — per-instance and global `maxBandwidth`.
- **Choose a readiness strategy** (P1) — stability window vs marker vs rename vs glob.
- **Quarantine files that exhaust retries** (P1) — the Failed folder + error sidecar.
- **Survive a multi-day disconnection** (P2) — `giveUpAfter`, resumable multipart, the circuit-breaker.
- **Activate/deactivate an instance from the control plane** (P3) — the `set-activation` command.
- **Build a realtime UI on the event stream** (P3) — subscribing the UNS `evt/…` + retained `state/…`.
- **Bridge status to the cloud for fleet-wide visibility** (P3) — the `{thing}/…` namespace + wildcards.

Until a guide is written, the corresponding design is in
[`DESIGN.md`](https://github.com/edgecommons/file-replicator/blob/main/DESIGN.md) (section cross-refs above).
