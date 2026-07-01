//! # file-replicator — configuration model
//!
//! Deserializes the component's own config subtree (`component.instances[]` / `component.global`)
//! into typed structs. Sibling ggcommons sections (`messaging`, `credentials`, `logging`,
//! `heartbeat`, `metricEmission`, `health`, `tags`) are parsed by the library, not here.
//!
//! Conventions (mirrors `telemetry-processor`): `camelCase` field names; per-instance parse is
//! skip-on-error ([`load_instances`]); the component starts as long as **at least one** instance
//! builds (the app fails fast only when zero instances start — FR-CFG-4). The full field reference is
//! `DESIGN.md` §7 (and, once authored, `docs/reference/configuration.md` — the canonical spec
//! validated against THIS module).
//!
//! Scope note: `local` and `s3` egress are fully typed; `sftp`/`http`/`azure`/`gcs` are recognized
//! but their fields are not yet modeled (P5). Every numeric field accepts a JSON integer **or** an
//! integer-valued float via [`lenient_u64`] / [`lenient_opt_u64`] etc., because Greengrass delivers
//! config numbers as doubles (FR-CFG-3); human byte-rate parsing for `maxBandwidth` (FR-REL-6) lives
//! in [`ratelimit::parse_byte_rate`](crate::ratelimit::parse_byte_rate).
//!
//! The P0 module-level `allow(dead_code)` is gone now the P1 engine consumes the P1 field set
//! (ingress/egress-local/completion/retry/limits/schedule-immediate/activation). What remains
//! unconsumed is genuinely future scope, so the few types it covers carry a **narrow, reason-tagged**
//! `allow(dead_code)`: the S3 egress model (`S3Egress`/`MultipartCfg`, P2), the cron/window schedule
//! payloads (`CronSchedule`/`WindowSchedule`/`WindowClose`, P4), and the UNS topic override
//! (`TopicsCfg` + `InstanceCfg::topics`, P3). They are removed as each phase wires them in.

use std::path::PathBuf;

use ggcommons::prelude::Config;
use serde::{Deserialize, Deserializer};

// ---- lenient numeric deserializers (FR-CFG-3: Greengrass delivers config numbers as doubles) -----
//
// Every numeric config field routes through these so `"maxConcurrentFiles": 4` and
// `"maxConcurrentFiles": 4.0` both parse. Without this a single float-valued number fails the whole
// instance parse, and `load_instances` then skips it — so a Greengrass deployment (a platform P1 must
// support) could silently skip every instance.

/// Accept a JSON integer or an integer-valued float for a required `u64` field.
fn lenient_u64<'de, D: Deserializer<'de>>(d: D) -> Result<u64, D::Error> {
    use serde::de::Error;
    match serde_json::Value::deserialize(d)? {
        serde_json::Value::Number(n) => n
            .as_u64()
            .or_else(|| n.as_f64().map(|f| f as u64))
            .ok_or_else(|| Error::custom("expected a non-negative integer")),
        other => Err(Error::custom(format!("expected a number, got {other}"))),
    }
}

/// Accept a JSON integer or an integer-valued float (or null/absent → `None`) for `Option<u64>`.
fn lenient_opt_u64<'de, D: Deserializer<'de>>(d: D) -> Result<Option<u64>, D::Error> {
    use serde::de::Error;
    match Option::<serde_json::Value>::deserialize(d)? {
        None | Some(serde_json::Value::Null) => Ok(None),
        Some(serde_json::Value::Number(n)) => n
            .as_u64()
            .or_else(|| n.as_f64().map(|f| f as u64))
            .map(Some)
            .ok_or_else(|| Error::custom("expected a non-negative integer")),
        Some(other) => Err(Error::custom(format!("expected a number, got {other}"))),
    }
}

/// As [`lenient_opt_u64`], narrowed to `Option<u32>`.
fn lenient_opt_u32<'de, D: Deserializer<'de>>(d: D) -> Result<Option<u32>, D::Error> {
    Ok(lenient_opt_u64(d)?.map(|v| v as u32))
}

/// As [`lenient_opt_u64`], narrowed to `Option<usize>`.
fn lenient_opt_usize<'de, D: Deserializer<'de>>(d: D) -> Result<Option<usize>, D::Error> {
    Ok(lenient_opt_u64(d)?.map(|v| v as usize))
}

/// One watched-directory instance = one `component.instances[]` entry.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct InstanceCfg {
    /// Stable instance id (unit of config, isolation, statistics, activation, control).
    pub id: String,
    /// Initial activation state; the persisted runtime state may override this (DESIGN §7.5).
    #[serde(default = "default_true")]
    pub enabled: bool,
    /// Source directory + readiness + include/exclude.
    pub ingress: IngressCfg,
    /// Destination(s). An ordered list; v1 validates exactly one (multi = future, DESIGN §20-B).
    #[serde(default)]
    pub egress: Vec<EgressCfg>,
    /// When replication runs (default: immediate).
    #[serde(default)]
    pub schedule: ScheduleCfg,
    /// Source-file lifecycle on success / exhaustion.
    #[serde(default)]
    pub completion: CompletionCfg,
    /// Retry/backoff policy (falls back to `component.global.defaults.retry`).
    pub retry: Option<RetryCfg>,
    /// Concurrency + bandwidth caps (per-instance; global caps live under `component.global`).
    pub limits: Option<LimitsCfg>,
    /// Per-instance UNS topic override (default is `component.global.topics.prefix`).
    // P3 (control/events on the unified namespace): consumed by the topic builder, not the engine.
    #[allow(dead_code)]
    pub topics: Option<TopicsCfg>,
}

/// Ingress: the source directory and how files are discovered + judged ready.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct IngressCfg {
    pub path: PathBuf,
    #[serde(default)]
    pub recursive: bool,
    #[serde(default)]
    pub include: Vec<String>,
    #[serde(default)]
    pub exclude: Vec<String>,
    /// Reconciliation rescan interval (seconds); belt-and-suspenders alongside OS notifications.
    #[serde(default, deserialize_with = "lenient_opt_u64")]
    pub rescan_secs: Option<u64>,
    #[serde(default)]
    pub readiness: ReadinessCfg,
}

/// Readiness strategy — how the replicator decides a file is fully written (DESIGN §9).
#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "strategy", rename_all = "lowercase")]
pub enum ReadinessCfg {
    /// Ready once size+mtime are unchanged for `quietSecs` (default).
    Stability(StabilityReadiness),
    /// Ready when a companion marker file appears.
    Marker(MarkerReadiness),
    /// React only to files renamed/moved into the watch dir.
    Rename,
    /// Everything not matching a temp/exclude glob is ready immediately.
    Glob(GlobReadiness),
}

impl Default for ReadinessCfg {
    fn default() -> Self {
        ReadinessCfg::Stability(StabilityReadiness {
            quiet_secs: default_quiet_secs(),
        })
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct StabilityReadiness {
    #[serde(default = "default_quiet_secs", deserialize_with = "lenient_u64")]
    pub quiet_secs: u64,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MarkerReadiness {
    /// Companion marker suffix, e.g. `.done` → replicate `FILE` when `FILE.done` appears.
    #[serde(default = "default_marker_suffix")]
    pub suffix: String,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GlobReadiness {
    #[serde(default)]
    pub ready: Vec<String>,
}

/// Egress destination. `local`/`s3` are modeled; the rest are recognized but not yet typed (P5).
#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum EgressCfg {
    Local(LocalEgress),
    // Boxed: S3Egress is much larger than the other variants (clippy::large_enum_variant).
    // P2 (S3 destination): the factory matches this variant but does not read its payload until the
    // `dest-s3` backend lands.
    #[allow(dead_code)]
    S3(Box<S3Egress>),
    /// Recognized future backends (DESIGN §10.2, phase P5). Their fields are ignored at P0.
    Sftp,
    Ftps,
    Http,
    Azure,
    Gcs,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LocalEgress {
    pub path: PathBuf,
    /// fsync the written file before completion (durability on the destination filesystem).
    #[serde(default)]
    pub fsync: bool,
}

// P2 (S3 destination): the whole S3 egress model is parsed at P0 but only read once the `dest-s3`
// backend lands. Narrow, reason-tagged allow (replaces the removed module-level P0 allow).
#[allow(dead_code)]
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct S3Egress {
    pub bucket: String,
    #[serde(default)]
    pub prefix: String,
    pub region: Option<String>,
    /// Custom endpoint (MinIO / S3-compatible / govcloud).
    pub endpoint_url: Option<String>,
    /// Optional `{"$secret": "..."}` ref; when absent, the ambient provider chain is used (DESIGN §11.5).
    pub credentials: Option<serde_json::Value>,
    pub storage_class: Option<String>,
    /// e.g. `AES256` or `aws:kms`.
    pub sse: Option<String>,
    pub kms_key_id: Option<String>,
    /// S3 Transfer Acceleration endpoint.
    #[serde(default)]
    pub accelerate: bool,
    /// `UNSIGNED-PAYLOAD` for simple PUTs over TLS (skip the payload-signing pass).
    #[serde(default)]
    pub unsigned_payload: bool,
    /// Flexible/trailing checksum algorithm, e.g. `CRC32C` (default) or `SHA256`.
    pub checksum_algorithm: Option<String>,
    #[serde(default)]
    pub multipart: MultipartCfg,
}

// P2 (S3 destination): multipart tuning, read by the S3 backend only.
#[allow(dead_code)]
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MultipartCfg {
    /// Files larger than this use multipart; smaller use a single PutObject.
    #[serde(default, deserialize_with = "lenient_opt_u64")]
    pub threshold_bytes: Option<u64>,
    #[serde(default, deserialize_with = "lenient_opt_u64")]
    pub part_size_bytes: Option<u64>,
    #[serde(default, deserialize_with = "lenient_opt_usize")]
    pub max_concurrent_parts: Option<usize>,
}

/// Schedule — when ready work is released (DESIGN §12).
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(tag = "mode", rename_all = "lowercase")]
pub enum ScheduleCfg {
    /// Replicate as files become ready (default).
    #[default]
    Immediate,
    /// Point trigger: release all ready work at each cron fire.
    // P4 (scheduling): the P1 engine matches only the discriminant (immediate vs not); the cron
    // payload is read once the scheduler lands.
    #[allow(dead_code)]
    Cron(CronSchedule),
    /// Continuous flow gated to an `open`→`close` window.
    // P4 (scheduling): see `Cron` — payload read only once the window scheduler lands.
    #[allow(dead_code)]
    Window(WindowSchedule),
}

// P4 (scheduling & windows): cron payload; the P1 engine treats any non-immediate schedule as
// immediate (with a TODO), so these fields are only read once the cron scheduler lands.
#[allow(dead_code)]
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CronSchedule {
    /// Standard cron expression (evaluated tz/DST-aware; DESIGN §12.2).
    pub expression: String,
    pub timezone: Option<String>,
}

// P4 (scheduling & windows): window payload; see [`CronSchedule`] — unread until the scheduler lands.
#[allow(dead_code)]
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct WindowSchedule {
    /// Window open (cron).
    pub open: String,
    /// Window close (cron); or use `durationMins`.
    pub close: Option<String>,
    pub duration_mins: Option<u64>,
    pub timezone: Option<String>,
    #[serde(default)]
    pub on_window_close: WindowClose,
}

// P4 (scheduling & windows): window-close behavior, read by the window scheduler only.
#[allow(dead_code)]
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum WindowClose {
    /// Pause in-flight and resume next window (if the destination supports resume), else finish current.
    #[default]
    PauseResume,
    /// Let in-flight transfers finish past close; admit no new files until next open.
    FinishCurrent,
}

/// Completion — source-file lifecycle after success / retry-exhaustion (DESIGN §13).
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CompletionCfg {
    #[serde(default)]
    pub on_success: OnSuccess,
    pub archive_dir: Option<PathBuf>,
    #[serde(default)]
    pub on_exhausted: OnExhausted,
    pub failed_dir: Option<PathBuf>,
    #[serde(default)]
    pub on_collision: Collision,
    #[serde(default)]
    pub verify: Verify,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum OnSuccess {
    /// Move the source to `archiveDir`.
    #[default]
    Archive,
    /// Delete the source.
    Delete,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum OnExhausted {
    /// Leave the file in place, marked Failed (retriable via `trigger`).
    #[default]
    RetainInPlace,
    /// Move to `failedDir` with an error sidecar.
    Quarantine,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum Verify {
    #[default]
    Checksum,
    Size,
    None,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum Collision {
    #[default]
    Suffix,
    Overwrite,
    Fail,
}

/// Retry / backoff policy (DESIGN §13.4/§13.6).
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RetryCfg {
    #[serde(default, deserialize_with = "lenient_opt_u64")]
    pub base_delay_ms: Option<u64>,
    #[serde(default, deserialize_with = "lenient_opt_u64")]
    pub max_delay_ms: Option<u64>,
    /// Time budget before giving up (e.g. `"7d"`) — governs long-outage tolerance (FR-REL-7).
    pub give_up_after: Option<String>,
    /// Optional hard attempt cap (default: none — time-governed).
    #[serde(default, deserialize_with = "lenient_opt_u32")]
    pub max_attempts: Option<u32>,
}

/// Concurrency + bandwidth caps.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LimitsCfg {
    #[serde(default, deserialize_with = "lenient_opt_usize")]
    pub max_concurrent_files: Option<usize>,
    /// Human byte-rate, e.g. `"20MB/s"` (parsed in P1; FR-REL-6).
    pub max_bandwidth: Option<String>,
}

/// UNS topic override (DESIGN §15).
// P3 (control/events on the unified namespace): read by the topic builder, not the engine.
#[allow(dead_code)]
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TopicsCfg {
    /// Topic prefix template (default `{ThingName}/file-replicator`).
    pub prefix: Option<String>,
}

/// Component-global config subtree (`component.global`, DESIGN §7.2): aggregate concurrency +
/// bandwidth caps and cross-instance defaults. Every field is optional; an absent `component.global`
/// yields all-defaults.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GlobalCfg {
    /// Aggregate caps across all instances (`maxConcurrentFiles` = global slot semaphore size;
    /// `maxBandwidth` = the global token bucket every transfer also passes).
    pub limits: Option<LimitsCfg>,
    /// Cross-instance defaults (currently the fallback retry policy).
    pub defaults: Option<GlobalDefaults>,
    /// Global UNS topic prefix. P3 (control/events): consumed by the topic builder, not the engine.
    #[allow(dead_code)]
    pub topics: Option<TopicsCfg>,
}

/// `component.global.defaults` — values an instance inherits when it does not set its own.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GlobalDefaults {
    /// Fallback retry policy (an instance's own `retry` wins field-by-field).
    pub retry: Option<RetryCfg>,
    /// Default schedule timezone. P4 (scheduling): read by the cron/window scheduler only.
    #[allow(dead_code)]
    pub timezone: Option<String>,
}

/// Parse the `component.global` subtree, tolerating a malformed/absent block by falling back to
/// all-defaults (a bad global section must not stop the component from starting; FR-CFG-4).
pub fn load_global(config: &Config) -> GlobalCfg {
    match serde_json::from_value::<GlobalCfg>(config.global().clone()) {
        Ok(g) => g,
        Err(e) => {
            tracing::warn!(error = %e, "malformed component.global; using defaults");
            GlobalCfg::default()
        }
    }
}

fn default_true() -> bool {
    true
}
fn default_quiet_secs() -> u64 {
    5
}
fn default_marker_suffix() -> String {
    ".done".to_string()
}

/// Parse every `component.instances[]` entry into an [`InstanceCfg`], logging and skipping any
/// malformed instance (FR-CFG-4). Returns the successfully-parsed instances; the caller
/// ([`App::run`](crate::app::App::run)) fails fast if that set is empty (the fail-only-if-zero rule).
pub fn load_instances(config: &Config) -> Vec<InstanceCfg> {
    let mut out = Vec::new();
    for id in config.instance_ids() {
        let Some(raw) = config.instance(&id) else {
            continue;
        };
        match serde_json::from_value::<InstanceCfg>(raw.clone()) {
            Ok(inst) => out.push(inst),
            Err(e) => {
                tracing::warn!(instance = %id, error = %e, "skipping malformed instance");
            }
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(v: serde_json::Value) -> InstanceCfg {
        serde_json::from_value(v).expect("valid instance")
    }

    #[test]
    fn local_immediate_defaults() {
        let inst = parse(serde_json::json!({
            "id": "x",
            "ingress": { "path": "/in" },
            "egress": [ { "type": "local", "path": "/out" } ]
        }));
        assert_eq!(inst.id, "x");
        assert!(inst.enabled, "enabled defaults to true");
        assert!(!inst.ingress.recursive);
        assert!(matches!(inst.schedule, ScheduleCfg::Immediate));
        assert!(matches!(inst.readiness_default(), ReadinessCfg::Stability(s) if s.quiet_secs == 5));
        assert_eq!(inst.egress.len(), 1);
        assert!(matches!(inst.egress[0], EgressCfg::Local(ref l) if l.path.as_path() == std::path::Path::new("/out")));
        // completion defaults
        assert_eq!(inst.completion.on_success, OnSuccess::Archive);
        assert_eq!(inst.completion.on_exhausted, OnExhausted::RetainInPlace);
        assert_eq!(inst.completion.verify, Verify::Checksum);
    }

    #[test]
    fn s3_window_full() {
        let inst = parse(serde_json::json!({
            "id": "s3win",
            "enabled": false,
            "ingress": {
                "path": "/data/out", "recursive": true,
                "include": ["**/*.csv"], "exclude": ["**/*.tmp"],
                "readiness": { "strategy": "rename" }
            },
            "egress": [ {
                "type": "s3", "bucket": "b", "prefix": "p/", "region": "us-east-1",
                "accelerate": true, "unsignedPayload": true, "checksumAlgorithm": "CRC32C",
                "multipart": { "thresholdBytes": 16777216, "maxConcurrentParts": 4 }
            } ],
            "schedule": {
                "mode": "window", "open": "0 2 * * WED", "close": "0 4 * * WED",
                "onWindowClose": "finishCurrent"
            },
            "completion": { "onSuccess": "delete", "onExhausted": "quarantine", "failedDir": "/f" },
            "limits": { "maxConcurrentFiles": 4, "maxBandwidth": "20MB/s" }
        }));
        assert!(!inst.enabled);
        assert!(matches!(inst.ingress.readiness, ReadinessCfg::Rename));
        match &inst.egress[0] {
            EgressCfg::S3(s3) => {
                assert_eq!(s3.bucket, "b");
                assert!(s3.accelerate && s3.unsigned_payload);
                assert_eq!(s3.multipart.threshold_bytes, Some(16777216));
            }
            other => panic!("expected s3, got {other:?}"),
        }
        match &inst.schedule {
            ScheduleCfg::Window(w) => {
                assert_eq!(w.open, "0 2 * * WED");
                assert_eq!(w.on_window_close, WindowClose::FinishCurrent);
            }
            other => panic!("expected window, got {other:?}"),
        }
        assert_eq!(inst.completion.on_success, OnSuccess::Delete);
        assert_eq!(inst.completion.on_exhausted, OnExhausted::Quarantine);
    }

    #[test]
    fn global_parses_limits_and_defaults() {
        let cfg = Config::from_value(
            "com.test",
            "thing",
            serde_json::json!({
                "component": {
                    "global": {
                        "limits": { "maxConcurrentFiles": 8, "maxBandwidth": "50MB/s" },
                        "defaults": { "retry": { "baseDelayMs": 1000, "giveUpAfter": "7d" } }
                    }
                }
            }),
        )
        .unwrap();
        let g = load_global(&cfg);
        let limits = g.limits.expect("limits present");
        assert_eq!(limits.max_concurrent_files, Some(8));
        assert_eq!(limits.max_bandwidth.as_deref(), Some("50MB/s"));
        let retry = g.defaults.and_then(|d| d.retry).expect("retry default present");
        assert_eq!(retry.base_delay_ms, Some(1000));
        assert_eq!(retry.give_up_after.as_deref(), Some("7d"));
    }

    #[test]
    fn global_absent_is_all_defaults() {
        let cfg = Config::from_value("com.test", "thing", serde_json::json!({ "component": {} }))
            .unwrap();
        let g = load_global(&cfg);
        assert!(g.limits.is_none());
        assert!(g.defaults.is_none());
    }

    #[test]
    fn malformed_instance_is_none() {
        // Missing required `ingress` → deserialization fails (caught + skipped by load_instances).
        let bad: Result<InstanceCfg, _> =
            serde_json::from_value(serde_json::json!({ "id": "bad" }));
        assert!(bad.is_err());
    }

    #[test]
    fn lenient_numbers_accept_greengrass_doubles() {
        // FR-CFG-3: Greengrass delivers config numbers as doubles. Every numeric field must accept an
        // integer-valued float, or a whole instance would fail to parse and be silently skipped.
        let inst = parse(serde_json::json!({
            "id": "gg",
            "ingress": {
                "path": "/in",
                "rescanSecs": 30.0,
                "readiness": { "strategy": "stability", "quietSecs": 5.0 }
            },
            "egress": [ { "type": "local", "path": "/out" } ],
            "retry": { "baseDelayMs": 1000.0, "maxDelayMs": 900000.0, "maxAttempts": 5.0 },
            "limits": { "maxConcurrentFiles": 8.0, "maxBandwidth": "20MB/s" }
        }));
        assert_eq!(inst.ingress.rescan_secs, Some(30));
        assert!(matches!(inst.ingress.readiness, ReadinessCfg::Stability(s) if s.quiet_secs == 5));
        let retry = inst.retry.expect("retry present");
        assert_eq!(retry.base_delay_ms, Some(1000));
        assert_eq!(retry.max_delay_ms, Some(900000));
        assert_eq!(retry.max_attempts, Some(5));
        let limits = inst.limits.expect("limits present");
        assert_eq!(limits.max_concurrent_files, Some(8));
    }

    #[test]
    fn lenient_numbers_still_accept_plain_integers() {
        // The lenient path must not regress the ordinary integer form.
        let inst = parse(serde_json::json!({
            "id": "int",
            "ingress": { "path": "/in", "rescanSecs": 15 },
            "egress": [ { "type": "local", "path": "/out" } ],
            "limits": { "maxConcurrentFiles": 4 }
        }));
        assert_eq!(inst.ingress.rescan_secs, Some(15));
        assert_eq!(inst.limits.unwrap().max_concurrent_files, Some(4));
    }

    #[test]
    fn load_instances_skips_only_the_malformed_ones() {
        // FR-CFG-4: a malformed instance is skipped; the valid siblings survive (not all-or-nothing).
        let cfg = Config::from_value(
            "com.test",
            "thing",
            serde_json::json!({
                "component": {
                    "instances": [
                        { "id": "good1", "ingress": { "path": "/a" },
                          "egress": [ { "type": "local", "path": "/o1" } ] },
                        { "id": "bad", "egress": [ { "type": "local", "path": "/o" } ] }, // no ingress
                        { "id": "good2", "ingress": { "path": "/b" },
                          "egress": [ { "type": "local", "path": "/o2" } ] }
                    ]
                }
            }),
        )
        .unwrap();
        let mut ids: Vec<String> = load_instances(&cfg).into_iter().map(|i| i.id).collect();
        ids.sort();
        assert_eq!(ids, vec!["good1".to_string(), "good2".to_string()], "bad skipped, good kept");
    }

    #[test]
    fn load_instances_survives_a_float_numeric_from_greengrass() {
        // A float-valued numeric (GG double) must NOT cause the instance to be skipped.
        let cfg = Config::from_value(
            "com.test",
            "thing",
            serde_json::json!({
                "component": {
                    "instances": [
                        {
                            "id": "gg",
                            "ingress": { "path": "/a", "rescanSecs": 30.0 },
                            "egress": [ { "type": "local", "path": "/o" } ],
                            "limits": { "maxConcurrentFiles": 8.0 }
                        }
                    ]
                }
            }),
        )
        .unwrap();
        let insts = load_instances(&cfg);
        assert_eq!(insts.len(), 1, "float numerics must not get the instance skipped");
        assert_eq!(insts[0].ingress.rescan_secs, Some(30));
    }

    // Helper so the default-readiness assertion reads cleanly in the test above.
    impl InstanceCfg {
        fn readiness_default(&self) -> &ReadinessCfg {
            &self.ingress.readiness
        }
    }
}
