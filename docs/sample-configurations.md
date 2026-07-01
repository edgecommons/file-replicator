# Sample configurations

Complete, annotated configs for real scenarios. This library grows per phase and each sample is validated
against the config parser (`src/config.rs`) — not against other docs. The two below are parseable today
(they are covered by the `config.rs` unit tests); scheduling/S3-runtime behaviour lands in P2/P4.

Every instance lives under `component.instances[]`; the standard ggcommons sections (`messaging`,
`credentials`, `logging`, `heartbeat`, `metricEmission`, `tags`) sit alongside `component`. Full field
reference: [Reference › Configuration](reference/configuration.md).

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
  "tags": { "enterprise": "acme", "site": "site42" },   // in the envelope; NOT in topics (DESIGN §15)
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

More samples (SFTP, HTTP, Azure/GCS, multi-readiness, per-platform HOST/GG/k8s variants) are added as those
capabilities ship. See [`DESIGN.md`](https://github.com/edgecommons/file-replicator/blob/main/DESIGN.md)
§7 for additional annotated examples.
