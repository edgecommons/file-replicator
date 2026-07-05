# Sample configurations

Complete, annotated configs for real scenarios. Each is validated against the config parser
(`src/config.rs`) — not against other docs. The `component` subtree is **identical across platforms** (the
component never branches on platform); only how config/messaging/identity are *sourced* differs, covered in
[§9 Per-platform](#9-per-platform-host--greengrass--kubernetes).

Every instance lives under `component.instances[]`; the standard ggcommons sections (`messaging`,
`credentials`, `logging`, `heartbeat`, `metricEmission`, `tags`) sit alongside `component`. Full field
reference: [Reference › Configuration](reference/configuration.md); every destination field:
[Reference › Destinations](reference/destinations.md). The off-by-default backends (`sftp`/`ftps`/`http`/
`azure`/`gcs`) require their `dest-*` cargo feature to be compiled in.

## 1. Watch a tree, replicate to a local directory, archive on success

```jsonc
{
  "component": {
    "global": { "limits": { "maxConcurrentFiles": 8 } },
    "instances": [
      {
        "id": "spool-to-archive",
        "ingress": {
          "path": "/data/outbound",
          "recursive": true,                    // preserve subtree at the destination
          "include": ["**/*"],
          "exclude": ["**/*.tmp", "**/.*"],
          "readiness": { "strategy": "stability", "quietSecs": 5 }
        },
        "egress": [ { "type": "local", "path": "/mnt/nas/ingest", "fsync": true } ],
        "schedule": { "mode": "immediate" },    // replicate as files become ready
        "completion": {
          "onSuccess": "archive", "archiveDir": "/data/processed",
          "onExhausted": "retainInPlace", "verify": "checksum"
        },
        "limits": { "maxConcurrentFiles": 4, "maxBandwidth": "20MB/s" }
      }
    ]
  }
}
```

## 2. Nightly bandwidth-limited window to S3 (ambient credentials), quarantine failures

```jsonc
{
  "tags": { "enterprise": "acme", "site": "site42" },   // in the envelope; NOT in topics
  "component": {
    "global": { "defaults": { "retry": { "baseDelayMs": 1000, "maxDelayMs": 900000, "giveUpAfter": "7d" } } },
    "instances": [
      {
        "id": "plant-csv-to-s3",
        "ingress": { "path": "/data/csv", "recursive": true, "include": ["**/*.csv"],
                     "readiness": { "strategy": "rename" } },
        "egress": [ {
          "type": "s3", "bucket": "acme-plant-telemetry", "prefix": "site42/csv/", "region": "us-east-1",
          // no "credentials" → inherit the platform's ambient chain (GG device role / IRSA / env)
          "accelerate": true, "unsignedPayload": true, "checksumAlgorithm": "CRC32C",
          "storageClass": "INTELLIGENT_TIERING", "sse": "aws:kms",
          "multipart": { "thresholdBytes": 16777216, "partSizeBytes": 16777216, "maxConcurrentParts": 4 }
        } ],
        "schedule": {
          "mode": "window", "open": "0 2 * * *", "close": "0 4 * * *",
          "timezone": "America/Chicago", "onWindowClose": "pauseResume"
        },
        "completion": {
          "onSuccess": "delete", "onExhausted": "quarantine", "failedDir": "/data/failed", "verify": "checksum"
        },
        "limits": { "maxBandwidth": "5MB/s" }
      }
    ]
  }
}
```

## 3. SFTP to a partner (SSH key from the vault, host-key pinned), marker readiness

Requires the `dest-sftp` feature. `marker` readiness waits for a companion `FILE.done` before sending
`FILE`, so a cooperative producer needs no quiet-period guess.

```jsonc
{
  "component": {
    "instances": [
      {
        "id": "reports-to-sftp",
        "ingress": {
          "path": "/data/reports", "recursive": true, "include": ["**/*.pdf"],
          "readiness": { "strategy": "marker", "suffix": ".done" }   // send report.pdf when report.pdf.done appears
        },
        "egress": [ {
          "type": "sftp", "host": "sftp.partner.example", "port": 22, "baseDir": "/incoming/acme",
          "username": "acme",
          "credentials": { "$secret": "partner-sftp" },   // resolves {username,privateKey,passphrase} or {username,password}
          // pinned server host key — WITHOUT this (and without insecureAcceptAnyHostKey) the handshake is refused:
          "hostKey": "ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIL0...comment",
          "fsync": true, "checksumAlgorithm": "SHA256"
        } ],
        "completion": { "onSuccess": "archive", "archiveDir": "/data/reports/_sent", "verify": "checksum" }
      }
    ]
  }
}
```

## 4. HTTP(S) resumable PUT to an ingest endpoint, rename readiness

Requires `dest-http`. `rename` readiness suits an atomic-publish producer (write to a temp name, then
rename into the watch dir).

```jsonc
{
  "component": {
    "instances": [
      {
        "id": "events-to-webhook",
        "ingress": { "path": "/var/spool/events", "readiness": { "strategy": "rename" } },
        "egress": [ {
          "type": "http", "url": "https://ingest.example.com/upload", "method": "PUT",
          "headers": { "X-Tenant": "acme" },                 // static, non-secret headers
          "credentials": { "$secret": "ingest-bearer" },      // { "bearerToken": "..." }  (or basic {username,password})
          "resumable": true, "chunkBytes": 8388608,           // 8 MiB ranged-PUT segments, checkpointed
          "checksumAlgorithm": "CRC32C"
        } ],
        "completion": { "onSuccess": "delete", "onExhausted": "quarantine", "failedDir": "/var/spool/events/_failed" }
      }
    ]
  }
}
```

## 5. Azure Blob (staged blocks), glob readiness

Requires `dest-azure`. `glob` readiness treats anything matching `ready` as immediately done.

```jsonc
{
  "component": {
    "instances": [
      {
        "id": "telemetry-to-azure",
        "ingress": {
          "path": "/data/parquet", "recursive": true,
          "readiness": { "strategy": "glob", "ready": ["**/*.parquet"] }
        },
        "egress": [ {
          "type": "azure", "account": "acmeacct", "container": "telemetry", "prefix": "site42/",
          // "endpointUrl": "http://127.0.0.1:10000/devstoreaccount1",   // Azurite / sovereign cloud
          "credentials": { "$secret": "azure-blob" },   // { "accountKey": "..." } or { "connectionString": "..." }
          "blocks": { "thresholdBytes": 33554432, "blockSizeBytes": 8388608 },
          "checksumAlgorithm": "CRC32C"
        } ],
        "schedule": { "mode": "cron", "expression": "every 15 minutes" },
        "completion": { "onSuccess": "delete", "verify": "checksum" }
      }
    ]
  }
}
```

## 6. Google Cloud Storage (resumable), token from the vault

Requires `dest-gcs`. Set `anonymous: true` (and drop `credentials`) for a fake-gcs-server or an
`allUsers`-public bucket.

```jsonc
{
  "component": {
    "instances": [
      {
        "id": "archive-to-gcs",
        "ingress": { "path": "/data/archive", "recursive": true },
        "egress": [ {
          "type": "gcs", "bucket": "acme-cold-archive", "prefix": "site42/",
          // "endpointUrl": "http://127.0.0.1:4443",   // fake-gcs-server
          "credentials": { "$secret": "gcs-token" },   // { "accessToken": "..." }  (bearer token only)
          "chunkBytes": 8388608,                        // rounded down to 256 KiB alignment
          "checksumAlgorithm": "CRC32C"                 // also compared against the object's crc32c metadata on verify
        } ],
        "completion": { "onSuccess": "archive", "archiveDir": "/data/archive/_done" }
      }
    ]
  }
}
```

## 7. Several readiness strategies side by side

One component, four instances, each demonstrating a different `readiness.strategy` — instances run
independently, so a slow/failing one never stalls the others.

```jsonc
{
  "component": {
    "instances": [
      { "id": "unknown-producer", "ingress": { "path": "/spool/a",
          "readiness": { "strategy": "stability", "quietSecs": 10 } },   // wait for size+mtime to settle
        "egress": [ { "type": "local", "path": "/out/a" } ] },
      { "id": "cooperative-marker", "ingress": { "path": "/spool/b",
          "readiness": { "strategy": "marker", "suffix": ".ok" } },       // send FILE when FILE.ok appears
        "egress": [ { "type": "local", "path": "/out/b" } ] },
      { "id": "atomic-rename", "ingress": { "path": "/spool/c",
          "readiness": { "strategy": "rename" } },                        // only react to renamed-in files
        "egress": [ { "type": "local", "path": "/out/c" } ] },
      { "id": "lands-complete", "ingress": { "path": "/spool/d",
          "readiness": { "strategy": "glob", "ready": ["**/*.csv"] } },   // ready immediately if it matches
        "egress": [ { "type": "local", "path": "/out/d" } ] }
    ]
  }
}
```

## 8. Multi-destination fan-out (one source → local + S3 + SFTP)

The `egress` list holds `N >= 1` destinations. Each is delivered and verified independently, and the source
is completed (here, archived) only once **every** destination has succeeded.

```jsonc
{
  "component": {
    "instances": [
      {
        "id": "fanout-critical",
        "ingress": { "path": "/data/critical", "recursive": true,
                     "readiness": { "strategy": "stability", "quietSecs": 5 } },
        "egress": [
          { "type": "local", "path": "/mnt/nas/mirror", "fsync": true },
          { "type": "s3", "bucket": "acme-dr", "prefix": "critical/", "region": "us-west-2" },
          { "type": "sftp", "host": "dr.partner.example", "baseDir": "/incoming",
            "credentials": { "$secret": "dr-sftp" },
            "hostKey": "ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAA..." }
        ],
        "completion": { "onSuccess": "archive", "archiveDir": "/data/critical/_done", "verify": "checksum" },
        "priority": 10   // jump the global concurrency queue ahead of bulk feeds under contention
      }
    ]
  }
}
```

## 9. Per-platform (HOST / GREENGRASS / KUBERNETES)

The `component` config above is unchanged across all three platforms — only the launch and the
config/messaging/identity *source* differ.

**HOST** — config from a local file, messaging to a local/remote broker, identity from `-t`:

```bash
file-replicator --platform HOST --transport MQTT ./messaging.json \
  -c FILE ./config.json -t gw-01
```

`messaging.json` (the transport descriptor) is separate from the component `config.json`:

```jsonc
{ "messaging": { "local": { "host": "localhost", "port": 1883, "clientId": "file-replicator-local" } } }
```

**GREENGRASS** — packaged as a v2 component (`gdk-config.json` / `recipe.yaml`); the `component` config is
supplied as the deployment's merged component configuration, transport is IPC, and identity comes from the
IPC environment. IAM for an `s3` destination comes from the device's TokenExchangeService role (ambient — no
`credentials` needed). The recipe launches it exactly as:

```bash
file-replicator --platform GREENGRASS -c GG_CONFIG
```

**KUBERNETES** — build the image (`Dockerfile`) and apply the manifests in `k8s/` (`kubectl apply -f k8s/`).
**No CLI args are needed:** with `--platform auto` the library detects KUBERNETES from the projected
ServiceAccount token, the config source defaults to `CONFIGMAP` (the ConfigMap is mounted as a whole
directory at `/etc/ggcommons`), transport defaults to MQTT with broker config from that ConfigMap, and
identity comes from the Downward API; `s3` credentials come from IRSA (ambient). The `component` config in
the ConfigMap is unchanged from the examples above — mount your watched source directory and any
local-destination/archive/failed dirs into the pod per deployment (durable SQLite state lives on the `/data`
PVC, so `replicas: 1` / `Recreate`).

See [`DESIGN.md`](https://github.com/edgecommons/file-replicator/blob/main/DESIGN.md) for additional
annotated examples.
