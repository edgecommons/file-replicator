# Reference - Metrics

The file-replicator emits metrics through the EdgeCommons metric service. With
`metricEmission.target: messaging`, metric messages are published on the reserved UNS `metric` class:

```text
ecv1/{device}/FileReplicator/main/metric/{metricName}
```

The component does not publish directly to `metric` topics. It defines and emits metrics through
`gg.metrics()`, so the same metric names, measures, units, and dimensions are used for messaging,
CloudWatch, and Prometheus targets.

## Dimension model

Dimensions are intentionally bounded for CloudWatch. The file-replicator uses only `instance`,
`readinessStrategy`, `destinationType`, `result`, and `mode`, plus the dimensions injected by the
EdgeCommons runtime such as component identity and category.

Metric dimensions never include local paths, filenames, bucket names, object keys, endpoint URLs, or
raw error text. Use events and logs for those high-cardinality details.

## Emission model

Most operational measures are emitted as interval values at the metric reporting cadence. Queue and
schedule state measures are gauges: they describe the current state at the time of emission. The
compatibility `fileReplicator` group carries durable lifetime totals and interval deltas for existing
dashboards.

## `fileReplicator`

Compatibility metric group for existing dashboards and alarms.

Dimensions: runtime-injected component dimensions only.

| Measure | Unit | Purpose |
|---|---:|---|
| `filesReplicated` | Count | Lifetime files successfully replicated. Helps confirm aggregate progress after restarts. |
| `bytesReplicated` | Bytes | Lifetime bytes successfully replicated. Helps compare file count with actual data volume. |
| `filesFailed` | Count | Lifetime files that failed terminally. Helps detect unrecovered replication failures. |
| `filesReplicatedInterval` | Count | Files completed in the current reporting interval. Helps build rate charts and CloudWatch `Sum` alarms. |
| `bytesReplicatedInterval` | Bytes | Bytes completed in the current reporting interval. Helps build throughput charts. |
| `filesFailedInterval` | Count | Terminal failures in the current reporting interval. Helps alert on active failure bursts. |

## `FileReplicatorDiscovery`

Discovery and readiness evaluation for a source directory.

Dimensions: `instance`, `readinessStrategy`.

| Measure | Unit | Purpose |
|---|---:|---|
| `scanCount` | Count | Source scans performed. Helps determine whether discovery is running. |
| `scanDurationMs` | Milliseconds | Time spent scanning. Helps detect slow filesystems or expensive readiness checks. |
| `filesDiscovered` | Count | Files seen by discovery. Helps compare source activity with queued work. |
| `filesReady` | Count | Files admitted as ready for replication. Helps determine whether readiness checks are releasing work. |
| `filesIgnored` | Count | Files ignored by filters or readiness rules. Helps diagnose unexpected missing transfers. |
| `scanErrors` | Count | Discovery errors. Helps identify source read or traversal problems. |
| `permissionDenied` | Count | Permission/access failures during discovery. Helps distinguish auth/path problems from empty sources. |

## `FileReplicatorQueue`

Durable work queue state for one replication instance.

Dimensions: `instance`.

| Measure | Unit | Purpose |
|---|---:|---|
| `queueDepthReady` | Count | Files ready to be replicated. Helps identify backlog growth. |
| `queueDepthInProgress` | Count | Files currently claimed by workers. Helps verify active processing. |
| `queueDepthFailed` | Count | Files failed but still retryable. Helps identify transient destination/source instability. |
| `queueDepthExhausted` | Count | Files that exhausted retry policy. Helps identify work needing operator action. |
| `oldestQueuedAgeMs` | Milliseconds | Age of the oldest queued item. Helps alert on stuck or slow replication. |
| `bytesQueued` | Bytes | Bytes waiting in the queue. Helps estimate drain time and storage pressure. |
| `retryBacklog` | Count | Items waiting for retry. Helps spot retry storms or long outages. |
| `activeWorkers` | Count | Current worker count. Helps confirm concurrency and throttling behavior. |

## `FileReplicatorTransfer`

Per-destination transfer outcomes and throughput.

Dimensions: `instance`, `destinationType`, `result`.

`destinationType` is a bounded backend token such as `local`, `s3`, `sftp`, `ftps`, `http`, `azure`, `gcs`,
or `multi`. `result` is a bounded outcome category.

| Measure | Unit | Purpose |
|---|---:|---|
| `filesStarted` | Count | Files whose transfer began. Helps compare admitted work with completed work. |
| `filesReplicated` | Count | Files completed successfully. Helps measure successful drain rate by destination type. |
| `filesFailed` | Count | Files whose transfer failed. Helps detect active destination or source errors. |
| `filesQuarantined` | Count | Failed files moved to quarantine. Helps track data requiring manual review. |
| `filesRetained` | Count | Exhausted files retained in place. Helps distinguish retained failures from quarantined failures. |
| `bytesReplicated` | Bytes | Bytes completed successfully. Helps measure payload throughput by backend. |
| `transferDurationMs` | Milliseconds | Accumulated transfer time. Helps estimate latency and throughput degradation. |
| `throughputBytesPerSec` | Bytes/Second | Observed transfer throughput. Helps compare destination performance and throttling impact. |
| `retryAttempts` | Count | Retry attempts used during transfer. Helps detect unstable backends before terminal failure. |
| `verificationFailures` | Count | Checksum or validation failures. Helps identify data integrity problems. |
| `resumeRecoveries` | Count | Transfers resumed from a checkpoint. Helps confirm long-outage and partial-upload recovery. |

## `FileReplicatorDestination`

Destination link and write behavior.

Dimensions: `instance`, `destinationType`.

| Measure | Unit | Purpose |
|---|---:|---|
| `linkConnected` | Count | `1` when the destination link is considered usable, `0` otherwise. Helps distinguish queue backlog from link outage. |
| `connectFailures` | Count | Destination connection failures. Helps diagnose unavailable endpoints or network paths. |
| `authFailures` | Count | Destination authentication failures. Helps distinguish bad credentials from transport errors. |
| `writeFailures` | Count | Destination write failures. Helps detect permission, capacity, or API errors. |
| `throttleDelayMs` | Milliseconds | Time spent waiting on bandwidth throttles. Helps explain intentional slowdowns. |
| `bandwidthLimitBytesPerSec` | Bytes/Second | Effective bandwidth cap. Helps correlate transfer rate with configured throttling. |

## `FileReplicatorSchedule`

Scheduling and admission state for a replication instance.

Dimensions: `instance`, `mode`.

| Measure | Unit | Purpose |
|---|---:|---|
| `instanceActive` | Count | `1` when the instance is active, `0` when disabled. Helps explain why discovered work is not moving. |
| `windowOpen` | Count | `1` when a schedule window is open, `0` when closed. Helps diagnose scheduled pauses. |
| `scheduleTriggers` | Count | Schedule trigger firings. Helps confirm cron/window evaluation. |
| `scheduleSkipped` | Count | Schedule opportunities skipped. Helps identify disabled or blocked execution. |
| `admissionBlocked` | Count | Ready files blocked by the schedule gate. Helps separate backlog from destination failure. |
| `filesReleased` | Count | Files released from schedule gating into transfer. Helps measure scheduled batch size. |
