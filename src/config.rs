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
//! Scope note: `local`/`s3`/`sftp`/`ftps`/`http`/`azure`/`gcs` are all fully typed (P5 complete);
//! each backend's fields are only *read* once its `dest-*` feature is compiled in. Every numeric field
//! accepts a JSON integer **or** an
//! integer-valued float via [`lenient_u64`] / [`lenient_opt_u64`] etc., because Greengrass delivers
//! config numbers as doubles (FR-CFG-3); human byte-rate parsing for `maxBandwidth` (FR-REL-6) lives
//! in [`ratelimit::parse_byte_rate`](crate::ratelimit::parse_byte_rate).
//!
//! The P0 module-level `allow(dead_code)` is gone now the P1 engine consumes the P1 field set
//! (ingress/egress-local/completion/retry/limits/schedule-immediate/activation), and P4
//! (`file_replicator::schedule`) consumes the cron/window schedule payloads
//! (`CronSchedule`/`WindowSchedule`/`WindowClose`). What remains unconsumed is genuinely future
//! scope, so the few types it covers carry a **narrow, reason-tagged** `allow(dead_code)`: the S3
//! egress model (`S3Egress`/`MultipartCfg`, P2). They are removed as each phase wires them in.
//!
//! **UNS migration note**: the old `TopicsCfg` (`component.global.topics.prefix` /
//! `InstanceCfg.topics` / `legacyConfigTopic`) is REMOVED, not deprecated — topics are now minted by
//! the ggcommons UNS grammar (`ecv1/{device}/{component}/{instance}/{class}`), which has no
//! configurable prefix and no legacy-alias concept (see `crate::control`/`crate::events` module
//! docs).

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

/// As [`lenient_opt_u64`], narrowed to `Option<u16>` (TCP port fields).
fn lenient_opt_u16<'de, D: Deserializer<'de>>(d: D) -> Result<Option<u16>, D::Error> {
    Ok(lenient_opt_u64(d)?.map(|v| v as u16))
}

/// As [`lenient_opt_u64`], narrowed to `Option<usize>`.
fn lenient_opt_usize<'de, D: Deserializer<'de>>(d: D) -> Result<Option<usize>, D::Error> {
    Ok(lenient_opt_u64(d)?.map(|v| v as usize))
}

/// Accept a JSON integer or an integer-valued float for a required (signed) `i32` field — the same
/// FR-CFG-3 leniency as [`lenient_u64`], but signed: `priority` (Feature B) is a signed 0-255-ish
/// convention (lower = higher priority) and a Greengrass-delivered negative "boost" value must parse
/// too.
fn lenient_i32<'de, D: Deserializer<'de>>(d: D) -> Result<i32, D::Error> {
    use serde::de::Error;
    match serde_json::Value::deserialize(d)? {
        serde_json::Value::Number(n) => n
            .as_i64()
            .or_else(|| n.as_f64().map(|f| f as i64))
            .map(|v| v as i32)
            .ok_or_else(|| Error::custom("expected an integer")),
        other => Err(Error::custom(format!("expected a number, got {other}"))),
    }
}

/// Map a JSON value to a known [`PermissionPolicy`], or `None` (with a `warn`) for anything
/// unrecognized. Shared by the global (non-optional) and per-instance (optional) lenient parsers below.
fn permission_policy_from_value(v: &serde_json::Value) -> Option<PermissionPolicy> {
    match v.as_str() {
        Some("disableInstance") => Some(PermissionPolicy::DisableInstance),
        Some("fatal") => Some(PermissionPolicy::Fatal),
        Some("retain") => Some(PermissionPolicy::Retain),
        other => {
            tracing::warn!(
                value = ?other,
                "unrecognized onPermissionError (expected disableInstance|fatal|retain); ignoring this field"
            );
            None
        }
    }
}

/// Lenient parse for `component.global.onPermissionError`: a typo'd value (e.g. `"fatel"`) must NOT
/// reject the whole `GlobalCfg`. [`load_global`] falls back coarsely — a serde error on ANY field drops
/// the ENTIRE `component.global` block back to defaults — so without this, one misspelled policy string
/// would silently discard every sibling global setting (a configured `maxConcurrentFiles`, bandwidth
/// cap, topics, retry defaults). Instead: warn and fall back to just the default policy, leaving the
/// siblings intact. (`#[serde(default)]` still handles the absent-field case; this runs only when the
/// field is present.)
fn lenient_permission_policy_global<'de, D: Deserializer<'de>>(
    d: D,
) -> Result<PermissionPolicy, D::Error> {
    let v = serde_json::Value::deserialize(d)?;
    Ok(permission_policy_from_value(&v).unwrap_or_default())
}

/// As [`lenient_permission_policy_global`] but for the per-instance `Option` override: a null or
/// unrecognized value yields `None` (inherit the component default) rather than dropping the whole
/// instance from [`load_instances`] over a typo in one optional field.
fn lenient_permission_policy_opt<'de, D: Deserializer<'de>>(
    d: D,
) -> Result<Option<PermissionPolicy>, D::Error> {
    let v = serde_json::Value::deserialize(d)?;
    if v.is_null() {
        return Ok(None);
    }
    Ok(permission_policy_from_value(&v))
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
    /// Destination(s). An ordered list; `N >= 1` entries fan out independently — a file completes
    /// (source deleted/archived) only once EVERY entry has delivered and been verified (P6, DESIGN
    /// §20-B). Cardinality itself isn't validated here; [`crate::instance::Instance::build`] rejects an
    /// empty list.
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
    /// Per-instance override of `component.global.onPermissionError` (`None` inherits the component
    /// default — see [`PermissionPolicy`] and [`resolve_permission_policy`]). Parsed leniently: a typo'd
    /// value warns and falls back to `None` (inherit) rather than dropping the whole instance.
    #[serde(default, deserialize_with = "lenient_permission_policy_opt")]
    pub on_permission_error: Option<PermissionPolicy>,
    /// Cross-instance GLOBAL concurrency-admission priority (Feature B, `src/admission.rs`): under
    /// contention for the shared `component.global.limits.maxConcurrentFiles` cap, a lower `priority`
    /// number is admitted first (matching the file-replicator-wide 0-255 convention — lower = higher
    /// priority), FIFO among equal priorities. Governs the GLOBAL admission decision ONLY — it does not
    /// affect this instance's own `limits.maxConcurrentFiles` cap, and there is no bandwidth weighting
    /// by priority. Demand-adaptive: an instance that isn't currently contending for a global slot
    /// reserves nothing, regardless of its priority. Default 100 (a neutral mid value).
    #[serde(default = "default_priority", deserialize_with = "lenient_i32")]
    pub priority: i32,
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
    /// P5 (SFTP destination): the factory matches this variant but only reads its payload when the
    /// `dest-sftp` backend is compiled in.
    Sftp(Box<SftpEgress>),
    /// P5 (FTPS destination): read once the `dest-ftps` backend lands.
    Ftps(Box<FtpsEgress>),
    /// P5 (HTTP(S) destination): the factory matches this variant but only reads its payload when the
    /// `dest-http` backend is compiled in.
    Http(Box<HttpEgress>),
    /// P5 (Azure Blob destination): the factory matches this variant but only reads its payload when
    /// the `dest-azure` backend is compiled in.
    Azure(Box<AzureEgress>),
    /// P5 (GCS destination): the factory matches this variant but only reads its payload when the
    /// `dest-gcs` backend is compiled in.
    Gcs(Box<GcsEgress>),
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

// P5 (SFTP destination): the whole SFTP egress model is parsed at P0 but only read once the
// `dest-sftp` backend lands. Narrow, reason-tagged allow (removed once `dest/sftp` consumes it).
// `Debug` is hand-written (below) to redact the ambient secret fields — FR-CFG-5: a `?egress`/panic/
// config-dump must never surface the raw `password`/`passphrase` (mirrors `SftpAuth`'s redacted Debug).
#[allow(dead_code)]
#[derive(Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SftpEgress {
    pub host: String,
    /// Default 22.
    #[serde(default, deserialize_with = "lenient_opt_u16")]
    pub port: Option<u16>,
    /// Remote root; the remote path is `pathmap(base_dir, item.relpath)` (DESIGN §10.2 subtree
    /// preservation, same traversal-safe join as the S3 `prefix`/key mapping).
    #[serde(default)]
    pub base_dir: String,
    pub username: Option<String>,
    /// Ambient password (discouraged in favor of `credentials.$secret`).
    pub password: Option<String>,
    /// Ambient path to a private key file on local disk.
    pub private_key: Option<String>,
    pub passphrase: Option<String>,
    /// Optional `{"$secret": "..."}` ref resolving to `{username,password}` or
    /// `{username,privateKey,passphrase}` JSON (DESIGN §11.5-equivalent).
    pub credentials: Option<serde_json::Value>,
    /// Pinned host public key (OpenSSH `algo base64[ comment]` line). Absent = no pinning; then the
    /// connection is REJECTED (fail-closed) unless `insecure_accept_any_host_key` is set (NFR-7).
    pub host_key: Option<String>,
    /// Accept ANY server host key when no `host_key` is pinned (fail-OPEN). Default `false`
    /// (fail-closed): with no pinned `host_key` and this unset, the SSH handshake is refused with an
    /// actionable error. Set `true` only as an explicit, documented escape hatch — it disables
    /// man-in-the-middle protection (an attacker presenting its own host key would be accepted, and
    /// under password auth could capture the credential). NFR-7.
    #[serde(default)]
    pub insecure_accept_any_host_key: bool,
    /// Upload-in-progress suffix appended to a hidden temp filename before the publish rename.
    /// Default `.part`.
    pub temp_suffix: Option<String>,
    /// Best-effort `fsync@openssh.com` before the publish rename, when the server supports it.
    #[serde(default)]
    pub fsync: bool,
    /// Local re-hash checksum algorithm (SFTP has no native content-checksum echo); default CRC32C.
    pub checksum_algorithm: Option<String>,
}

/// Redaction marker shown in a config struct's `Debug` in place of a present secret (absent → `None`),
/// so a diagnostic dump reveals *whether* a secret is set without ever printing it (FR-CFG-5).
fn redacted(present: bool) -> Option<&'static str> {
    present.then_some("***redacted***")
}

impl std::fmt::Debug for SftpEgress {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SftpEgress")
            .field("host", &self.host)
            .field("port", &self.port)
            .field("base_dir", &self.base_dir)
            .field("username", &self.username)
            .field("password", &redacted(self.password.is_some()))
            // `private_key` is a filesystem PATH (not key material — the inline PEM only ever lives in
            // the `$secret` vault), so it stays visible for diagnostics like `SftpKeySource::Path` does.
            .field("private_key", &self.private_key)
            .field("passphrase", &redacted(self.passphrase.is_some()))
            .field("credentials", &self.credentials)
            .field("host_key", &self.host_key)
            .field("insecure_accept_any_host_key", &self.insecure_accept_any_host_key)
            .field("temp_suffix", &self.temp_suffix)
            .field("fsync", &self.fsync)
            .field("checksum_algorithm", &self.checksum_algorithm)
            .finish()
    }
}

// P5 (FTPS destination): parsed at P0, read once the `dest-ftps` backend lands. `Debug` hand-written
// (below) to redact `password` (FR-CFG-5).
#[allow(dead_code)]
#[derive(Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FtpsEgress {
    pub host: String,
    /// Default 21 (explicit AUTH TLS) — implicit FTPS conventionally uses 990.
    #[serde(default, deserialize_with = "lenient_opt_u16")]
    pub port: Option<u16>,
    #[serde(default)]
    pub base_dir: String,
    pub username: Option<String>,
    pub password: Option<String>,
    /// Optional `{"$secret": "..."}` ref resolving to `{username,password}` JSON.
    pub credentials: Option<serde_json::Value>,
    /// `true` (default) = explicit AUTH TLS on the plain control port; `false` = implicit TLS.
    pub explicit_tls: Option<bool>,
    /// PASV (default `true`) vs active mode.
    pub passive: Option<bool>,
    pub temp_suffix: Option<String>,
    pub checksum_algorithm: Option<String>,
}

impl std::fmt::Debug for FtpsEgress {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FtpsEgress")
            .field("host", &self.host)
            .field("port", &self.port)
            .field("base_dir", &self.base_dir)
            .field("username", &self.username)
            .field("password", &redacted(self.password.is_some()))
            .field("credentials", &self.credentials)
            .field("explicit_tls", &self.explicit_tls)
            .field("passive", &self.passive)
            .field("temp_suffix", &self.temp_suffix)
            .field("checksum_algorithm", &self.checksum_algorithm)
            .finish()
    }
}

// P5 (HTTP(S) destination): the whole HTTP egress model is parsed at P0 but only read once the
// `dest-http` backend lands. Narrow, reason-tagged allow (removed once `dest/http` consumes it).
// `Debug` hand-written (below) to redact `bearer_token`/`password` (FR-CFG-5).
#[allow(dead_code)]
#[derive(Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct HttpEgress {
    /// Base URL the file is PUT/POSTed to; `item.relpath` is joined onto its path (DESIGN §10.2
    /// subtree preservation), e.g. `https://host/upload` + `a/b.csv` → `https://host/upload/a/b.csv`.
    pub url: String,
    /// `PUT` (default, resumable) or `POST` (single-shot; resume is forced off — DESIGN §10.2).
    pub method: Option<String>,
    /// Static headers sent on every request (e.g. a fixed API key header).
    #[serde(default)]
    pub headers: std::collections::BTreeMap<String, String>,
    /// Ambient bearer token (discouraged in favor of `credentials.$secret`).
    pub bearer_token: Option<String>,
    /// Ambient HTTP Basic auth username/password (discouraged in favor of `credentials.$secret`).
    pub username: Option<String>,
    pub password: Option<String>,
    /// Optional `{"$secret": "..."}` ref resolving to `{"bearerToken":"..."}` or
    /// `{"username":"...","password":"..."}` JSON (DESIGN §11.5-equivalent).
    pub credentials: Option<serde_json::Value>,
    /// `true` (default) = resumable ranged `PUT` in `chunkBytes`-sized segments with a persisted
    /// checkpoint; forced `false` for `method: POST` (DESIGN §10.2/§10.3).
    pub resumable: Option<bool>,
    /// Ranged-`PUT` chunk size; default 8 MiB. A smaller value makes resume tests/fixtures cheap.
    #[serde(default, deserialize_with = "lenient_opt_u64")]
    pub chunk_bytes: Option<u64>,
    /// Local re-hash checksum algorithm (generic HTTP has no universal content-checksum echo);
    /// default CRC32C.
    pub checksum_algorithm: Option<String>,
}

impl std::fmt::Debug for HttpEgress {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HttpEgress")
            .field("url", &self.url)
            .field("method", &self.method)
            .field("headers", &self.headers)
            .field("bearer_token", &redacted(self.bearer_token.is_some()))
            .field("username", &self.username)
            .field("password", &redacted(self.password.is_some()))
            .field("credentials", &self.credentials)
            .field("resumable", &self.resumable)
            .field("chunk_bytes", &self.chunk_bytes)
            .field("checksum_algorithm", &self.checksum_algorithm)
            .finish()
    }
}

// P5 (Azure Blob destination): the whole Azure egress model is parsed at P0 but only read once the
// `dest-azure` backend lands. Narrow, reason-tagged allow (removed once `dest/azure` consumes it).
// `Debug` hand-written (below) to redact `account_key`/`connection_string` (the connection string
// embeds `AccountKey=…`) — FR-CFG-5.
#[allow(dead_code)]
#[derive(Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AzureEgress {
    /// Storage account name (also the default `CloudLocation` — overridden by `endpointUrl`).
    pub account: String,
    pub container: String,
    #[serde(default)]
    pub prefix: String,
    /// Custom blob-service endpoint (Azurite / a sovereign cloud), e.g.
    /// `http://127.0.0.1:10000/devstoreaccount1`. Absent = the public Azure cloud for `account`.
    pub endpoint_url: Option<String>,
    /// Ambient storage account key (discouraged in favor of `credentials.$secret`).
    pub account_key: Option<String>,
    /// Ambient connection string (only `AccountKey` is extracted; `account`/`endpointUrl` above stay
    /// authoritative for the account/endpoint, discouraged in favor of `credentials.$secret`).
    pub connection_string: Option<String>,
    /// Optional `{"$secret": "..."}` ref resolving to `{"accountKey":"..."}` or
    /// `{"connectionString":"..."}` JSON (DESIGN §11.5-equivalent).
    pub credentials: Option<serde_json::Value>,
    #[serde(default)]
    pub blocks: AzureBlockCfg,
    /// Local re-hash checksum algorithm (Azure's per-block MD5/CRC64 is not a whole-object echo
    /// comparable to S3's flexible checksum); default CRC32C.
    pub checksum_algorithm: Option<String>,
}

impl std::fmt::Debug for AzureEgress {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AzureEgress")
            .field("account", &self.account)
            .field("container", &self.container)
            .field("prefix", &self.prefix)
            .field("endpoint_url", &self.endpoint_url)
            .field("account_key", &redacted(self.account_key.is_some()))
            .field("connection_string", &redacted(self.connection_string.is_some()))
            .field("credentials", &self.credentials)
            .field("blocks", &self.blocks)
            .field("checksum_algorithm", &self.checksum_algorithm)
            .finish()
    }
}

// P5 (Azure Blob destination): staged-block tuning, read by the Azure backend only.
#[allow(dead_code)]
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AzureBlockCfg {
    /// Files larger than this use staged blocks; smaller use a single `Put Blob`.
    #[serde(default, deserialize_with = "lenient_opt_u64")]
    pub threshold_bytes: Option<u64>,
    #[serde(default, deserialize_with = "lenient_opt_u64")]
    pub block_size_bytes: Option<u64>,
}

// P5 (GCS destination): the whole GCS egress model is parsed at P0 but only read once the `dest-gcs`
// backend lands. Narrow, reason-tagged allow (removed once `dest/gcs` consumes it). `Debug`
// hand-written (below) to redact `access_token` (FR-CFG-5).
#[allow(dead_code)]
#[derive(Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GcsEgress {
    pub bucket: String,
    #[serde(default)]
    pub prefix: String,
    /// Custom JSON/upload API endpoint (fake-gcs-server), e.g. `http://127.0.0.1:4443`. Absent = the
    /// public GCS JSON API (`https://storage.googleapis.com`).
    pub endpoint_url: Option<String>,
    /// Ambient OAuth2 access token (discouraged in favor of `credentials.$secret`).
    pub access_token: Option<String>,
    /// No `Authorization` header at all — the fake-gcs-server, or an `allUsers`-public bucket.
    #[serde(default)]
    pub anonymous: bool,
    /// Optional `{"$secret": "..."}` ref resolving to `{"accessToken":"..."}` JSON (DESIGN
    /// §11.5-equivalent). Full service-account JWT signing / Application Default Credentials
    /// discovery is a documented follow-up (see `dest/gcs/mod.rs`'s module doc) — this backend
    /// authenticates via a bearer access token only.
    pub credentials: Option<serde_json::Value>,
    /// Resumable-upload chunk size; default 8 MiB, rounded down to the nearest 256 KiB (GCS's
    /// required alignment for every chunk but the final one).
    #[serde(default, deserialize_with = "lenient_opt_u64")]
    pub chunk_bytes: Option<u64>,
    /// Local re-hash checksum algorithm; default CRC32C, also compared directly against the object's
    /// `crc32c` metadata field on `verify` (no re-download needed for that algorithm — DESIGN §10.2).
    pub checksum_algorithm: Option<String>,
}

impl std::fmt::Debug for GcsEgress {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GcsEgress")
            .field("bucket", &self.bucket)
            .field("prefix", &self.prefix)
            .field("endpoint_url", &self.endpoint_url)
            .field("access_token", &redacted(self.access_token.is_some()))
            .field("anonymous", &self.anonymous)
            .field("credentials", &self.credentials)
            .field("chunk_bytes", &self.chunk_bytes)
            .field("checksum_algorithm", &self.checksum_algorithm)
            .finish()
    }
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

/// Cron payload for [`ScheduleCfg::Cron`]; consumed by [`crate::schedule::Schedule::from_cfg`].
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CronSchedule {
    /// Standard cron expression, or an English sugar phrase (evaluated tz/DST-aware; DESIGN §12.2).
    pub expression: String,
    pub timezone: Option<String>,
}

/// Window payload for [`ScheduleCfg::Window`]; see [`CronSchedule`].
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

/// Window-close behavior (DESIGN §12.4); consumed by [`crate::schedule`].
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

/// Startup-validation / runtime permission-error policy (the [`crate::permission`] module). Governs
/// what happens when an instance's ingress/egress/archive/failed directory is unreadable/unwritable —
/// at startup validation and, for `retain`, for the runtime dedup-logging path too. Instance-scoped by
/// default (NOT a whole-component-fatal policy): a bad instance is disabled, its siblings keep running.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum PermissionPolicy {
    /// Skip (don't start) just the offending instance; every other instance is unaffected (FR-CFG-4's
    /// skip-bad-instance path). The default — matches the existing skip-on-malformed-config behavior.
    #[default]
    DisableInstance,
    /// Abort the whole component (returns an error from `App::run`). An explicit opt-in for
    /// deployments that would rather fail loudly than run degraded.
    Fatal,
    /// Start the instance anyway. Runtime dedup-logging (log once, not per rescan) plus the
    /// `PermissionDenied` event carry the ongoing diagnostic signal.
    Retain,
}

/// Resolve the effective [`PermissionPolicy`] for an instance: its own `onPermissionError` override
/// wins; otherwise it inherits `component.global.onPermissionError`.
pub fn resolve_permission_policy(inst: &InstanceCfg, global: &GlobalCfg) -> PermissionPolicy {
    inst.on_permission_error.unwrap_or(global.on_permission_error)
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
    /// Component-wide default startup-validation / runtime permission-error policy (an instance's own
    /// `onPermissionError` wins — see [`resolve_permission_policy`]). Defaults to `disableInstance`.
    /// Parsed leniently so a typo'd value falls back to the default WITHOUT dropping the rest of
    /// `component.global` (see [`lenient_permission_policy_global`]).
    #[serde(default, deserialize_with = "lenient_permission_policy_global")]
    pub on_permission_error: PermissionPolicy,
}

/// `component.global.defaults` — values an instance inherits when it does not set its own.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GlobalDefaults {
    /// Fallback retry policy (an instance's own `retry` wins field-by-field).
    pub retry: Option<RetryCfg>,
    /// Default schedule timezone, consumed by [`crate::instance::Instance::build_with_dest`] as the
    /// `Schedule::from_cfg` fallback when a schedule sets none of its own (DESIGN §12).
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
/// Default GLOBAL concurrency-admission priority (Feature B) when an instance sets none — a neutral
/// mid value on the 0-255-ish (lower = higher priority) convention.
fn default_priority() -> i32 {
    100
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

    // ---- permission policy (Feature A) ------------------------------------------------------------

    #[test]
    fn on_permission_error_defaults_to_disable_instance() {
        let inst = parse(serde_json::json!({
            "id": "x",
            "ingress": { "path": "/in" },
            "egress": [ { "type": "local", "path": "/out" } ]
        }));
        assert_eq!(inst.on_permission_error, None, "absent → inherit the component default");

        let cfg = Config::from_value("com.test", "thing", serde_json::json!({ "component": {} }))
            .unwrap();
        let g = load_global(&cfg);
        assert_eq!(g.on_permission_error, PermissionPolicy::DisableInstance);
    }

    #[test]
    fn on_permission_error_parses_fatal_and_retain() {
        let cfg = Config::from_value(
            "com.test",
            "thing",
            serde_json::json!({ "component": { "global": { "onPermissionError": "fatal" } } }),
        )
        .unwrap();
        assert_eq!(load_global(&cfg).on_permission_error, PermissionPolicy::Fatal);

        let inst = parse(serde_json::json!({
            "id": "x",
            "ingress": { "path": "/in" },
            "egress": [ { "type": "local", "path": "/out" } ],
            "onPermissionError": "retain"
        }));
        assert_eq!(inst.on_permission_error, Some(PermissionPolicy::Retain));
    }

    #[test]
    fn bad_global_on_permission_error_falls_back_but_keeps_sibling_limits() {
        // A typo'd onPermissionError must NOT nuke the rest of component.global (the coarse load_global
        // whole-object fallback would otherwise silently drop a configured concurrency cap). The bad
        // field falls back to the default policy; the sibling limits survive.
        let cfg = Config::from_value(
            "com.test",
            "thing",
            serde_json::json!({
                "component": {
                    "global": {
                        "onPermissionError": "fatel", // typo
                        "limits": { "maxConcurrentFiles": 32 }
                    }
                }
            }),
        )
        .unwrap();
        let g = load_global(&cfg);
        assert_eq!(
            g.on_permission_error,
            PermissionPolicy::DisableInstance,
            "typo'd policy falls back to the default"
        );
        assert_eq!(
            g.limits.and_then(|l| l.max_concurrent_files),
            Some(32),
            "the sibling limits value is preserved, not discarded"
        );
    }

    #[test]
    fn bad_instance_on_permission_error_inherits_and_is_not_dropped() {
        // A typo'd per-instance override falls back to None (inherit global) instead of dropping the
        // whole instance from load_instances.
        let cfg = Config::from_value(
            "com.test",
            "thing",
            serde_json::json!({
                "component": {
                    "instances": [
                        { "id": "ok", "ingress": { "path": "/a" },
                          "egress": [ { "type": "local", "path": "/o" } ],
                          "onPermissionError": "disabel" } // typo
                    ]
                }
            }),
        )
        .unwrap();
        let insts = load_instances(&cfg);
        assert_eq!(insts.len(), 1, "the instance is kept, not dropped over a typo'd optional field");
        assert_eq!(insts[0].on_permission_error, None, "unrecognized → inherit the component default");
        assert_eq!(
            resolve_permission_policy(&insts[0], &GlobalCfg::default()),
            PermissionPolicy::DisableInstance
        );
    }

    #[test]
    fn non_numeric_priority_skips_just_that_instance() {
        // FR-CFG-4 + lenient_i32's error path: a non-numeric `priority` is a hard parse error for that
        // field (it is a required i32, not an optional string-tolerant one), so the offending instance is
        // skipped while its valid siblings survive.
        let cfg = Config::from_value(
            "com.test",
            "thing",
            serde_json::json!({
                "component": {
                    "instances": [
                        { "id": "good", "ingress": { "path": "/a" },
                          "egress": [ { "type": "local", "path": "/o1" } ] },
                        { "id": "badprio", "ingress": { "path": "/b" },
                          "egress": [ { "type": "local", "path": "/o2" } ],
                          "priority": "high" } // non-numeric → skipped
                    ]
                }
            }),
        )
        .unwrap();
        let ids: Vec<String> = load_instances(&cfg).into_iter().map(|i| i.id).collect();
        assert_eq!(ids, vec!["good".to_string()], "only the non-numeric-priority instance is skipped");
    }

    #[test]
    fn resolve_permission_policy_instance_override_wins_else_inherits_global() {
        let mut global = GlobalCfg {
            on_permission_error: PermissionPolicy::Fatal,
            ..Default::default()
        };
        let mut inst = parse(serde_json::json!({
            "id": "x",
            "ingress": { "path": "/in" },
            "egress": [ { "type": "local", "path": "/out" } ]
        }));
        // No override → inherits the global default.
        assert_eq!(resolve_permission_policy(&inst, &global), PermissionPolicy::Fatal);

        // An instance override wins regardless of the global value.
        inst.on_permission_error = Some(PermissionPolicy::Retain);
        assert_eq!(resolve_permission_policy(&inst, &global), PermissionPolicy::Retain);

        global.on_permission_error = PermissionPolicy::DisableInstance;
        assert_eq!(
            resolve_permission_policy(&inst, &global),
            PermissionPolicy::Retain,
            "instance override still wins"
        );
    }

    // ---- priority (Feature B) ---------------------------------------------------------------------

    #[test]
    fn priority_defaults_to_100_when_absent() {
        let inst = parse(serde_json::json!({
            "id": "x",
            "ingress": { "path": "/in" },
            "egress": [ { "type": "local", "path": "/out" } ]
        }));
        assert_eq!(inst.priority, 100);
    }

    #[test]
    fn priority_is_lenient_and_accepts_negatives() {
        // Plain integer.
        let inst = parse(serde_json::json!({
            "id": "x",
            "ingress": { "path": "/in" },
            "egress": [ { "type": "local", "path": "/out" } ],
            "priority": 5
        }));
        assert_eq!(inst.priority, 5);

        // FR-CFG-3: a Greengrass-delivered integer-valued double must parse identically.
        let inst = parse(serde_json::json!({
            "id": "x",
            "ingress": { "path": "/in" },
            "egress": [ { "type": "local", "path": "/out" } ],
            "priority": 5.0
        }));
        assert_eq!(inst.priority, 5);

        // Negative = "boost past the highest normal priority" — must parse (signed field).
        let inst = parse(serde_json::json!({
            "id": "x",
            "ingress": { "path": "/in" },
            "egress": [ { "type": "local", "path": "/out" } ],
            "priority": -10
        }));
        assert_eq!(inst.priority, -10);
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

    // ---- Debug redaction of ambient secrets in the P5 egress structs (FR-CFG-5) -----------------

    #[test]
    fn p5_egress_debug_redacts_ambient_secrets() {
        // Every ambient plaintext secret carried in these config structs must be redacted in `Debug`
        // so a `?egress`, panic message, or config dump can never leak it (mirrors the resolved-auth
        // types' redaction). Non-secret fields (host, url, username, paths) stay visible for diagnostics.

        let sftp = SftpEgress {
            host: "h".into(),
            port: Some(2222),
            base_dir: "/upload".into(),
            username: Some("alice".into()),
            password: Some("PW_SECRET".into()),
            private_key: Some("/home/a/.ssh/id_ed25519".into()),
            passphrase: Some("PP_SECRET".into()),
            credentials: None,
            host_key: None,
            insecure_accept_any_host_key: false,
            temp_suffix: None,
            fsync: false,
            checksum_algorithm: None,
        };
        let dbg = format!("{sftp:?}");
        assert!(!dbg.contains("PW_SECRET"), "sftp password leaked: {dbg}");
        assert!(!dbg.contains("PP_SECRET"), "sftp passphrase leaked: {dbg}");
        assert!(dbg.contains("alice") && dbg.contains("id_ed25519"), "non-secrets visible: {dbg}");
        assert!(dbg.contains("***redacted***"));

        let ftps = FtpsEgress {
            host: "h".into(),
            port: None,
            base_dir: "/".into(),
            username: Some("bob".into()),
            password: Some("FTPS_SECRET".into()),
            credentials: None,
            explicit_tls: None,
            passive: None,
            temp_suffix: None,
            checksum_algorithm: None,
        };
        let dbg = format!("{ftps:?}");
        assert!(!dbg.contains("FTPS_SECRET"), "ftps password leaked: {dbg}");
        assert!(dbg.contains("bob") && dbg.contains("***redacted***"));

        let http = HttpEgress {
            url: "https://h/upload".into(),
            method: None,
            headers: Default::default(),
            bearer_token: Some("BEARER_SECRET".into()),
            username: Some("carol".into()),
            password: Some("HTTP_PW_SECRET".into()),
            credentials: None,
            resumable: None,
            chunk_bytes: None,
            checksum_algorithm: None,
        };
        let dbg = format!("{http:?}");
        assert!(!dbg.contains("BEARER_SECRET"), "http bearer leaked: {dbg}");
        assert!(!dbg.contains("HTTP_PW_SECRET"), "http password leaked: {dbg}");
        assert!(dbg.contains("carol") && dbg.contains("https://h/upload"));

        let azure = AzureEgress {
            account: "acct".into(),
            container: "c".into(),
            prefix: String::new(),
            endpoint_url: None,
            account_key: Some("ACCT_KEY_SECRET".into()),
            connection_string: Some("AccountName=a;AccountKey=CS_SECRET;".into()),
            credentials: None,
            blocks: AzureBlockCfg::default(),
            checksum_algorithm: None,
        };
        let dbg = format!("{azure:?}");
        assert!(!dbg.contains("ACCT_KEY_SECRET"), "azure account key leaked: {dbg}");
        assert!(!dbg.contains("CS_SECRET"), "azure connection string leaked: {dbg}");
        assert!(dbg.contains("acct") && dbg.contains("***redacted***"));

        let gcs = GcsEgress {
            bucket: "b".into(),
            prefix: String::new(),
            endpoint_url: None,
            access_token: Some("TOKEN_SECRET".into()),
            anonymous: false,
            credentials: None,
            chunk_bytes: None,
            checksum_algorithm: None,
        };
        let dbg = format!("{gcs:?}");
        assert!(!dbg.contains("TOKEN_SECRET"), "gcs access token leaked: {dbg}");
        assert!(dbg.contains("b") && dbg.contains("***redacted***"));

        // The `redacted` marker helper: present → marker, absent → None.
        assert_eq!(redacted(true), Some("***redacted***"));
        assert_eq!(redacted(false), None);
    }
}
