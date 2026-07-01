# Tutorial — your first replication

> **Status: authored in P1** (the local-directory engine). This page will walk through, end to end:
> installing the component, configuring one watched directory, dropping a file, and watching it replicate
> to a local destination and get archived — then repeating against S3 (P2).

Until then, the runnable path today is the P0 scaffold, which loads and validates configuration and runs an
empty engine:

```bash
# HOST platform, MQTT transport (against a local broker), FILE config source:
cargo run -- \
  --platform HOST --transport MQTT ./test-configs/standalone-messaging.json \
  -c FILE ./test-configs/config.json \
  -t my-thing
```

You should see the component start, log the parsed instances (e.g. `spool-to-archive`), and idle until
`Ctrl-C`. The replication behaviour it *will* perform is specified in
[`DESIGN.md`](https://github.com/edgecommons/file-replicator/blob/main/DESIGN.md) §8.

See also: [Sample configurations](sample-configurations.md) · [Reference › Configuration](reference/configuration.md).
