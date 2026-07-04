# How-to guides

> **Status: not yet authored.** The underlying capability for every guide below has already shipped (the
> component is implemented through P6 of DESIGN §21 — see `DESIGN.md` and `git log`), so the phase tags are
> historical (which phase added the capability), not a "wait for it" marker. The guides themselves are
> still just this title list — writing task-focused, grounded-in-the-shipped-behaviour content for each is
> outstanding docs work, not blocked on implementation.

- **Replicate a directory to S3** (P2) — bucket/prefix, ambient vs `$secret` credentials, acceleration.
- **Schedule a nightly upload window** (P4) — cron `open`/`close`, overnight windows, DST.
- **Cap upload bandwidth during operating hours** (P1) — per-instance and global `maxBandwidth`.
- **Choose a readiness strategy** (P1) — stability window vs marker vs rename vs glob.
- **Quarantine files that exhaust retries** (P1) — the Failed folder + error sidecar.
- **Survive a multi-day disconnection** (P2) — `giveUpAfter`, resumable multipart, the circuit-breaker.
- **Activate/deactivate an instance from the control plane** (P3) — the `set-activation` command.
- **Build a realtime UI on the event stream** (P3) — subscribing `ecv1/+/FileReplicator/+/evt/#` and
  polling `get-status` for current state (there is no retained snapshot — see `messaging-interface.md`).
- **Bridge status to the cloud for fleet-wide visibility** (P3) — the `ecv1/{device}/…` UNS namespace +
  the fleet-wide six-wildcard consumer pattern.

Until a guide is written, the corresponding design is in
[`DESIGN.md`](https://github.com/edgecommons/file-replicator/blob/main/DESIGN.md) (section cross-refs above).
