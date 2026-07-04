# Tutorial — your first replication

> **Status: the engine has shipped through P6** (local + S3 + SFTP/FTPS/HTTP/Azure/GCS + scheduling +
> fan-out — see `DESIGN.md` §21 and `git log`), but this walkthrough page itself has not yet been rewritten
> to match — it still documents the P0 scaffold's config-load-and-idle behavior below. Treat the
> step-by-step "drop a file and watch it replicate" narrative as **not yet authored**; until it is, see
> [Sample configurations](sample-configurations.md) for real, parseable instance configs and
> `docs/explanation.md` for how the engine actually behaves end to end.

Runnable today (and every phase since P0): loads and validates configuration and runs the engine —
watching, replicating, retrying, scheduling, etc. per the configured instances:

```bash
# HOST platform, MQTT transport (against a local broker), FILE config source:
cargo run -- \
  --platform HOST --transport MQTT ./test-configs/standalone-messaging.json \
  -c FILE ./test-configs/config.json \
  -t my-thing
```

You should see the component start, log the parsed instances (e.g. `spool-to-archive`), and then actually
replicate files dropped into a configured watched directory per that instance's `egress`/`schedule`/
`completion` config — not just idle. The full behaviour is specified in
[`DESIGN.md`](https://github.com/edgecommons/file-replicator/blob/main/DESIGN.md) §8.

See also: [Sample configurations](sample-configurations.md) · [Reference › Configuration](reference/configuration.md).
