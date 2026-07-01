//! # file-replicator — configuration model
//!
//! Deserializes the component's own config subtree (`component.instances[]` / `component.global`)
//! into typed structs. Sibling ggcommons sections (`messaging`, `credentials`, `logging`,
//! `heartbeat`, `metricEmission`, `health`, `tags`) are parsed by the library, not here.
//!
//! Conventions (mirrors `telemetry-processor`): `camelCase` field names; per-instance parse is
//! skip-on-error ([`load_instances`]); startup tolerates zero instances at P0. The full field
//! reference is `DESIGN.md` §7 (and, once authored, `docs/reference/configuration.md` — the
//! canonical spec validated against THIS module).
//!
//! Scope note: this is the P0 model. `local` and `s3` egress are fully typed; `sftp`/`http`/`azure`/
//! `gcs` are recognized but their fields are not yet modeled (P5). A lenient int-or-float
//! deserializer for Greengrass doubles (FR-CFG-3) and human byte-rate parsing for `maxBandwidth`
//! (FR-REL-6) land in P1 — for now numeric fields take JSON integers and `maxBandwidth` is a string.

// P0 scaffold: many config fields parse now but are only consumed once the engine lands (P1+). Allow
// dead_code for this module until then, rather than sprinkling per-field attributes that we'd remove.
#![allow(dead_code)]

use std::path::PathBuf;

use ggcommons::prelude::Config;
use serde::Deserialize;

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
    #[serde(default = "default_quiet_secs")]
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

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MultipartCfg {
    /// Files larger than this use multipart; smaller use a single PutObject.
    pub threshold_bytes: Option<u64>,
    pub part_size_bytes: Option<u64>,
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
    Cron(CronSchedule),
    /// Continuous flow gated to an `open`→`close` window.
    Window(WindowSchedule),
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CronSchedule {
    /// Standard cron expression (evaluated tz/DST-aware; DESIGN §12.2).
    pub expression: String,
    pub timezone: Option<String>,
}

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
    pub base_delay_ms: Option<u64>,
    pub max_delay_ms: Option<u64>,
    /// Time budget before giving up (e.g. `"7d"`) — governs long-outage tolerance (FR-REL-7).
    pub give_up_after: Option<String>,
    /// Optional hard attempt cap (default: none — time-governed).
    pub max_attempts: Option<u32>,
}

/// Concurrency + bandwidth caps.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LimitsCfg {
    pub max_concurrent_files: Option<usize>,
    /// Human byte-rate, e.g. `"20MB/s"` (parsed in P1; FR-REL-6).
    pub max_bandwidth: Option<String>,
}

/// UNS topic override (DESIGN §15).
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TopicsCfg {
    /// Topic prefix template (default `{ThingName}/file-replicator`).
    pub prefix: Option<String>,
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
/// malformed instance (FR-CFG-4). Returns the successfully-parsed instances (possibly empty at P0).
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
    fn malformed_instance_is_none() {
        // Missing required `ingress` → deserialization fails (caught + skipped by load_instances).
        let bad: Result<InstanceCfg, _> =
            serde_json::from_value(serde_json::json!({ "id": "bad" }));
        assert!(bad.is_err());
    }

    // Helper so the default-readiness assertion reads cleanly in the test above.
    impl InstanceCfg {
        fn readiness_default(&self) -> &ReadinessCfg {
            &self.ingress.readiness
        }
    }
}
