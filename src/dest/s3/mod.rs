//! # file-replicator — S3 destination (DESIGN §10.2, §11; P2)
//!
//! The high-performance object-store backend: size-adaptive `PutObject` vs resumable multipart with
//! parallel parts, flexible/trailing CRC32C (default) or SHA-256 checksums, ambient credentials by
//! default with an optional `{"$secret":"…"}` vault override, and endpoint/region/path-style/storage
//! -class/SSE/KMS configuration (MinIO / floci / S3-compatible / govcloud all via `endpointUrl`).
//!
//! ## Where the code lives (coverage seam, NFR-2)
//! Everything in **this file** is either pure decision logic (part planning, key mapping, checksum
//! codecs, resume bookkeeping, credential selection, client-param derivation, error classification)
//! or the streaming file read — all unit-tested with no network. Every line that makes an actual AWS
//! SDK round-trip lives in [`client`], which is excluded from the coverage gate (it is unreachable in
//! CI/unit runs; the floci integration tests in `tests/s3_floci.rs` self-skip when floci is down, so
//! they never gate). The lazy client builder and the whole `impl Destination for S3Dest` delegation
//! live in [`client`] too — on the excluded side of the seam — so this module's counted coverage
//! reflects only the pure/decision logic it actually tests.
//!
//! ## Resume (DESIGN §11.3/§13.4)
//! A multipart upload persists `{uploadId, key, partSize, totalSize, mtimeMs, completed:[{n, etag,
//! checksum}]}` into the shared [`StateStore`] resume row (keyed by `(instance, relpath, "s3")`) —
//! write-ahead on `CreateMultipartUpload` and after each completed part — so a crash resumes only the
//! missing parts then `CompleteMultipartUpload`. A token is reused only when key/partSize **and** the
//! current source size + mtime still match ([`S3ResumeToken::matches`]); otherwise the parts are stale
//! (a resized or in-place-rewritten source) and a fresh MPU is started. If a persisted `uploadId` has
//! been swept/aborted (a `NoSuchUpload` on `UploadPart`/`Complete`), the backend re-initiates a fresh
//! upload rather than failing permanently — unless the object is already live at the key (a prior
//! `Complete` whose ACK was lost), which is treated as delivered. On give-up the worker calls
//! [`Destination::abort`](super::Destination::abort) → `AbortMultipartUpload`.

pub mod client;

use std::sync::Arc;

use serde::{Deserialize, Serialize};

use crate::config::{MultipartCfg, S3Egress};
use crate::domain::{Checksum, ResumeState};
use crate::error::{ReplError, Result};
use crate::integrity::{Algorithm, Hasher};
use crate::ratelimit::Bandwidth;
use crate::state::StateStore;

use super::DestDeps;

// ---- constants (DESIGN §11.1) -------------------------------------------------------------------

const MIB: u64 = 1024 * 1024;
/// Files at or below this use a single `PutObject`; larger use multipart.
const DEFAULT_THRESHOLD: u64 = 16 * MIB;
/// Default multipart part size before auto-scaling.
const DEFAULT_PART_SIZE: u64 = 16 * MIB;
/// Default parallel-part concurrency.
const DEFAULT_MAX_CONCURRENT: usize = 4;
/// S3 minimum size for every non-final part.
const S3_MIN_PART: u64 = 5 * MIB;
/// S3 maximum part count per multipart upload.
const S3_MAX_PARTS: u64 = 10_000;
/// S3 maximum single part size (5 GiB).
const S3_MAX_PART: u64 = 5 * 1024 * MIB;
/// Streaming read window (matches [`crate::integrity::hash_reader`] / `local.rs`).
const READ_CHUNK: usize = 64 * 1024;

// ---- multipart tuning ---------------------------------------------------------------------------

/// Resolved multipart parameters (config ▸ defaults, clamped to S3 limits).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MultipartParams {
    pub threshold: u64,
    pub part_size: u64,
    pub max_concurrent: usize,
}

impl MultipartParams {
    /// Resolve from the (already lenient-parsed) [`MultipartCfg`], applying defaults and clamping the
    /// part size into `[5 MiB, 5 GiB]` and concurrency to `≥ 1`.
    pub fn resolve(cfg: &MultipartCfg) -> Self {
        MultipartParams {
            threshold: cfg.threshold_bytes.unwrap_or(DEFAULT_THRESHOLD),
            part_size: cfg
                .part_size_bytes
                .unwrap_or(DEFAULT_PART_SIZE)
                .clamp(S3_MIN_PART, S3_MAX_PART),
            max_concurrent: cfg.max_concurrent_parts.unwrap_or(DEFAULT_MAX_CONCURRENT).max(1),
        }
    }
}

/// The size-adaptive upload decision (DESIGN §11.1).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UploadPlan {
    /// Single `PutObject` (small file, or a 0-byte object).
    Single,
    /// Multipart with `num_parts` parts of `part_size` (last part = remainder).
    Multipart { part_size: u64, num_parts: u32 },
}

/// Decide single vs multipart and, for multipart, the effective part size auto-scaled so
/// `num_parts ≤ 10 000` while respecting the 5 MiB floor and 5 GiB ceiling (DESIGN §11.1).
pub fn plan_upload(size: u64, p: &MultipartParams) -> UploadPlan {
    if size <= p.threshold {
        return UploadPlan::Single;
    }
    // Grow the part size until ceil(size/part) ≤ 10 000, honoring the 5 MiB floor / 5 GiB ceiling.
    let min_by_count = size.div_ceil(S3_MAX_PARTS);
    let effective = p.part_size.max(min_by_count).clamp(S3_MIN_PART, S3_MAX_PART);
    let num_parts = size.div_ceil(effective) as u32;
    UploadPlan::Multipart {
        part_size: effective,
        num_parts,
    }
}

/// Byte length of `part_number` (1-based) given the plan: `part_size` for every part but the last,
/// which carries the remainder.
pub fn part_len(part_number: u32, part_size: u64, num_parts: u32, total: u64) -> u64 {
    if part_number >= num_parts {
        total - (num_parts as u64 - 1) * part_size
    } else {
        part_size
    }
}

// ---- key mapping (DESIGN §10.2 subtree preservation) --------------------------------------------

/// The deterministic object key for `relpath` under `prefix`: forward-slash join with traversal /
/// empty segments dropped (mirrors `local.rs::final_path`), so the recursive source subtree is
/// preserved and the key can never escape the prefix. A trailing/leading `/` on `prefix` is
/// normalized away; an empty prefix yields the bare relpath.
pub fn object_key(prefix: &str, relpath: &str) -> String {
    let rel: Vec<&str> = relpath
        .split('/')
        .filter(|s| !s.is_empty() && *s != "." && *s != "..")
        .collect();
    let joined = rel.join("/");
    let p = prefix.trim_matches('/');
    if p.is_empty() {
        joined
    } else if joined.is_empty() {
        p.to_string()
    } else {
        format!("{p}/{joined}")
    }
}

// ---- checksum codecs (DESIGN §11.4) -------------------------------------------------------------

use base64::Engine as _;

/// Base64 of the 4-byte big-endian CRC32C — the S3 flexible-checksum header value.
pub fn crc32c_to_b64(v: u32) -> String {
    base64::engine::general_purpose::STANDARD.encode(v.to_be_bytes())
}

/// Base64 of a 32-byte SHA-256 digest — the S3 flexible-checksum header value.
pub fn sha256_to_b64(b: &[u8; 32]) -> String {
    base64::engine::general_purpose::STANDARD.encode(b)
}

/// The S3 header value for a [`Checksum`], or `None` when no checksum was computed.
pub fn checksum_to_b64(c: &Checksum) -> Option<String> {
    match c {
        Checksum::Crc32c(v) => Some(crc32c_to_b64(*v)),
        Checksum::Sha256(b) => Some(sha256_to_b64(b)),
        Checksum::None => None,
    }
}

/// Inverse of [`crc32c_to_b64`] (round-trip / test helper): decode a base64 4-byte-BE CRC32C.
pub fn b64_to_crc32c(s: &str) -> Option<u32> {
    let bytes = base64::engine::general_purpose::STANDARD.decode(s).ok()?;
    let arr: [u8; 4] = bytes.try_into().ok()?;
    Some(u32::from_be_bytes(arr))
}

/// The S3 `x-amz-checksum-*` / `ChecksumAlgorithm` token for an [`Algorithm`].
pub fn algo_name(algo: Algorithm) -> &'static str {
    match algo {
        Algorithm::Crc32c => "CRC32C",
        Algorithm::Sha256 => "SHA256",
    }
}

/// Compare the object-level checksum S3 returned against the one we captured on delivery.
pub fn compare_object_checksum(expected: &str, actual: &str) -> Result<()> {
    if expected == actual {
        Ok(())
    } else {
        Err(ReplError::Integrity(format!(
            "s3 object checksum mismatch: expected {expected}, got {actual}"
        )))
    }
}

// ---- resume token (DESIGN §11.3 / §14.2) --------------------------------------------------------

/// One uploaded part recorded in the resume token.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct S3CompletedPart {
    pub part_number: u32,
    pub e_tag: String,
    #[serde(default)]
    pub checksum: Option<String>,
}

/// The multipart resume state, serialized under the `"s3"` key of [`ResumeState::token`].
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct S3ResumeToken {
    pub upload_id: String,
    pub key: String,
    pub part_size: u64,
    /// Total object size at `CreateMultipartUpload` time. A resume whose *current* source size
    /// differs must NOT reuse the persisted parts — a grown/shrunk source would splice a stale part
    /// into the object (corrupt), send a too-short non-final part (`EntityTooSmall`), or reference a
    /// part that no longer exists (`InvalidPart`). A legacy row (`0`) never matches a real upload, so
    /// it safely forces a fresh MPU (DESIGN §11.3).
    #[serde(default)]
    pub total_size: u64,
    /// Source mtime (unix ms) captured at `CreateMultipartUpload`. Guards the *same-length* in-place
    /// mutation the `total_size` check cannot see: reusing old parts against new content yields an
    /// object of `[old parts | new parts]` that would still pass the (composite) multipart checksum.
    /// A changed mtime forces a fresh MPU (DESIGN §11.3/§13.2).
    #[serde(default)]
    pub mtime_ms: i64,
    #[serde(default)]
    pub completed: Vec<S3CompletedPart>,
}

impl S3ResumeToken {
    /// Extract a multipart token from a [`ResumeState`], if one is present and well-formed.
    pub fn from_resume(r: &ResumeState) -> Option<S3ResumeToken> {
        r.token
            .get("s3")
            .and_then(|v| serde_json::from_value(v.clone()).ok())
    }

    /// Wrap this token into a [`ResumeState`] with `bytes_committed` derived from the completed parts.
    pub fn to_resume(&self, num_parts: u32, total: u64) -> ResumeState {
        let committed = self.bytes_committed(num_parts, total);
        ResumeState {
            bytes_committed: committed,
            token: serde_json::json!({
                "s3": serde_json::to_value(self).unwrap_or(serde_json::Value::Null)
            }),
        }
    }

    /// Sum of the byte lengths of the parts recorded as completed.
    pub fn bytes_committed(&self, num_parts: u32, total: u64) -> u64 {
        self.completed
            .iter()
            .map(|p| part_len(p.part_number, self.part_size, num_parts, total))
            .sum()
    }

    /// Whether this resume token still matches the current plan AND the current source, so its
    /// already-uploaded parts can be reused. All four must agree:
    /// - `key` + `part_size` — a reconfigured `multipart.partSizeBytes` (or a different object)
    ///   invalidates every persisted part boundary;
    /// - `total_size` — the source grew/shrank between attempts (stale-part splice / `EntityTooSmall`
    ///   / `InvalidPart`);
    /// - `mtime_ms` — a same-length in-place rewrite (checksum-invisible for a composite MPU).
    ///
    /// Any mismatch means the token cannot be reused and the transfer must restart from a fresh
    /// `CreateMultipartUpload` (DESIGN §11.3/§13.2).
    pub fn matches(&self, key: &str, part_size: u64, total_size: u64, mtime_ms: i64) -> bool {
        self.key == key
            && self.part_size == part_size
            && self.total_size == total_size
            && self.mtime_ms == mtime_ms
    }
}

/// The 1-based part numbers from `1..=num_parts` that are **not** yet in `completed`.
pub fn missing_parts(num_parts: u32, completed: &[S3CompletedPart]) -> Vec<u32> {
    let done: std::collections::HashSet<u32> = completed.iter().map(|p| p.part_number).collect();
    (1..=num_parts).filter(|n| !done.contains(n)).collect()
}

// ---- delivery handle (carried through verify + recovery) ----------------------------------------

/// Build the opaque [`Delivered::handle`] for an S3 object: its key, the S3 object-level checksum
/// string (single = plain base64, multipart = `base64-N` composite), the algorithm token, and
/// whether it was a multipart upload.
pub fn build_handle(key: &str, s3_checksum: Option<&str>, algo: Algorithm, multipart: bool) -> serde_json::Value {
    serde_json::json!({
        "key": key,
        "s3Checksum": s3_checksum,
        "algo": algo_name(algo),
        "multipart": multipart,
    })
}

/// The object key recorded in a delivery handle.
pub fn handle_key(handle: &serde_json::Value) -> Option<String> {
    handle.get("key").and_then(|v| v.as_str()).map(String::from)
}

/// The S3 object-level checksum recorded in a delivery handle.
pub fn handle_s3_checksum(handle: &serde_json::Value) -> Option<String> {
    handle.get("s3Checksum").and_then(|v| v.as_str()).map(String::from)
}

// ---- credentials (DESIGN §11.5) -----------------------------------------------------------------

use ggcommons::credentials::CredentialService;

/// How the S3 client authenticates.
///
/// `Debug` is implemented **by hand** (not derived) so the resolved secret key + session token are
/// never emitted through `{:?}` — a future `tracing::debug!(?dest)` or an error wrapping this value
/// must not leak the credentials the `$secret`/vault indirection deliberately keeps out of the logged
/// config (FR-CFG-5, DESIGN §11.5). The access key id is not secret (it identifies, like the AWS
/// console shows it) so it stays visible for diagnostics.
#[derive(Clone, PartialEq, Eq)]
pub enum CredMode {
    /// The aws-config default provider chain (GG TES device role / k8s IRSA / HOST env-profile-role).
    Ambient,
    /// Static keys resolved from the ggcommons vault via a `{"$secret":"…"}` egress reference.
    Static {
        access_key_id: String,
        secret_access_key: String,
        session_token: Option<String>,
    },
}

impl std::fmt::Debug for CredMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CredMode::Ambient => f.write_str("Ambient"),
            CredMode::Static {
                access_key_id,
                session_token,
                ..
            } => f
                .debug_struct("Static")
                .field("access_key_id", access_key_id)
                .field("secret_access_key", &"***redacted***")
                .field(
                    "session_token",
                    &session_token.as_ref().map(|_| "***redacted***"),
                )
                .finish(),
        }
    }
}

/// Resolve the credential mode from the egress `credentials` config and the (optional) credential
/// service (DESIGN §11.5):
///
/// - absent → [`CredMode::Ambient`] (the default; no secret to manage);
/// - `{"$secret":"name"}` → look the secret up as [`AwsCredentials`](ggcommons::credentials) and use
///   static keys — a missing service, missing secret, or malformed secret is a **permanent** config
///   error (the secret value never enters logs);
/// - any other JSON shape → permanent (unsupported credentials form).
pub fn resolve_credentials(
    cred_cfg: &Option<serde_json::Value>,
    creds: Option<&dyn CredentialService>,
) -> Result<CredMode> {
    let Some(v) = cred_cfg else {
        return Ok(CredMode::Ambient);
    };
    let name = v
        .get("$secret")
        .and_then(|s| s.as_str())
        .ok_or_else(|| {
            ReplError::Permanent(
                "egress.credentials must be absent (ambient) or {\"$secret\":\"name\"}".to_string(),
            )
        })?;
    let svc = creds.ok_or_else(|| {
        ReplError::Permanent(format!(
            "egress.credentials references secret '{name}' but no credentials subsystem is configured"
        ))
    })?;
    let aws = svc
        .get_aws_credentials(name)
        .map_err(|e| ReplError::Permanent(format!("resolve secret '{name}': {e}")))?
        .ok_or_else(|| ReplError::Permanent(format!("secret '{name}' not found in the vault")))?;
    Ok(CredMode::Static {
        access_key_id: aws.access_key_id,
        secret_access_key: aws.secret_access_key,
        session_token: aws.session_token,
    })
}

// ---- client params (DESIGN §11.2/§4) ------------------------------------------------------------

/// Derived S3 client configuration (pure, from [`S3Egress`]).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct S3ClientParams {
    pub region: Option<String>,
    pub endpoint_url: Option<String>,
    pub force_path_style: bool,
    pub accelerate: bool,
}

/// Map an [`S3Egress`] to the client params (DESIGN §11.2):
///
/// - `force_path_style` iff `endpointUrl` is set (required for floci/MinIO; real AWS uses
///   virtual-hosted addressing);
/// - `accelerate` only when there is **no** `endpointUrl` (Transfer Acceleration is real-AWS-only —
///   an `accelerate` + `endpointUrl` combination is forced off with a warning);
/// - region passthrough, defaulting to `us-east-1` for endpoint deployments so the SDK has a region.
pub fn s3_client_params(cfg: &S3Egress) -> S3ClientParams {
    let has_endpoint = cfg.endpoint_url.is_some();
    if cfg.accelerate && has_endpoint {
        tracing::warn!(
            "egress.accelerate is ignored when endpointUrl is set (Transfer Acceleration is real-AWS-only)"
        );
    }
    let region = cfg
        .region
        .clone()
        .or_else(|| has_endpoint.then(|| "us-east-1".to_string()));
    S3ClientParams {
        region,
        endpoint_url: cfg.endpoint_url.clone(),
        force_path_style: has_endpoint,
        accelerate: cfg.accelerate && !has_endpoint,
    }
}

// ---- error classification (DESIGN §13.4) --------------------------------------------------------

/// True for S3 error codes that can never succeed by retrying (fail fast to `Exhausted`).
fn is_permanent_code(code: &str) -> bool {
    matches!(
        code,
        "NoSuchBucket"
            | "NoSuchKey"
            | "AccessDenied"
            | "AccessDeniedException"
            | "AllAccessDisabled"
            | "InvalidAccessKeyId"
            | "SignatureDoesNotMatch"
            | "InvalidRequest"
            | "InvalidArgument"
            | "InvalidBucketName"
            | "EntityTooLarge"
            | "EntityTooSmall"
            | "MethodNotAllowed"
            | "PreconditionFailed"
            | "KMS.DisabledException"
            | "KMSInvalidStateException"
    )
}

/// True for S3 error codes worth retrying (throttling / capacity / transient server faults).
fn is_transient_code(code: &str) -> bool {
    matches!(
        code,
        "SlowDown"
            | "RequestTimeout"
            | "RequestTimeTooSkewed"
            | "Throttling"
            | "ThrottlingException"
            | "RequestLimitExceeded"
            | "TooManyRequests"
            | "ServiceUnavailable"
            | "InternalError"
    )
}

/// Classify an S3 failure into the retry engine's [`ReplError`] taxonomy (DESIGN §13.4). `code` is
/// the service error code (when the error reached the service), `status` the HTTP status (when a
/// response came back), and `transport` true for connect/timeout/dispatch failures. The thin call
/// wrappers in [`client`] extract these off `SdkError`; the decision logic lives here so it is
/// unit-tested off the excluded lines.
pub fn classify_s3(code: Option<&str>, status: Option<u16>, transport: bool) -> ReplError {
    if transport {
        return ReplError::Transient(format!(
            "s3 transport failure{}",
            code.map(|c| format!(" ({c})")).unwrap_or_default()
        ));
    }
    if let Some(c) = code {
        if is_permanent_code(c) {
            return ReplError::Permanent(format!("s3 error: {c}"));
        }
        if is_transient_code(c) {
            return ReplError::Transient(format!("s3 error: {c}"));
        }
    }
    if let Some(s) = status {
        if s >= 500 || s == 429 {
            return ReplError::Transient(format!(
                "s3 http {s}{}",
                code.map(|c| format!(" ({c})")).unwrap_or_default()
            ));
        }
        if s >= 400 {
            return ReplError::Permanent(format!(
                "s3 http {s}{}",
                code.map(|c| format!(" ({c})")).unwrap_or_default()
            ));
        }
    }
    // A service error with an unknown code (and no helpful status) means the server rejected the
    // request → treat as permanent; a bare failure with nothing to go on → retry defensively.
    match code {
        Some(c) => ReplError::Permanent(format!("s3 error: {c}")),
        None => ReplError::Transient("s3 error (unclassified)".to_string()),
    }
}

// ---- streaming read (pure, no network) ----------------------------------------------------------

/// Read `len` bytes from `path` starting at `offset` into memory, throttling every chunk through the
/// bandwidth governor, folding them into a single-pass [`Checksum`] (DESIGN §13.1), and reporting the
/// **delta** of each chunk via `on_progress` (the caller turns deltas into the cumulative total the
/// [`ProgressSink`] wants — one accumulator for a single PUT, a shared one across concurrent parts).
///
/// This is the whole streaming/integrity path for both `PutObject` (offset 0, whole file) and each
/// multipart part; keeping it here (not in [`client`]) makes it unit-testable with a real file and no
/// network. The in-memory buffer is bounded by `len` (≤ the multipart part size, DESIGN NFR-3).
pub async fn read_range(
    path: &std::path::Path,
    offset: u64,
    len: u64,
    bw: &Bandwidth,
    algo: Algorithm,
    on_progress: &(dyn Fn(u64) + Send + Sync),
) -> Result<(Vec<u8>, Checksum)> {
    use tokio::io::{AsyncReadExt, AsyncSeekExt};

    let mut file = tokio::fs::File::open(path).await.map_err(ReplError::classify_io)?;
    if offset > 0 {
        file.seek(std::io::SeekFrom::Start(offset))
            .await
            .map_err(ReplError::classify_io)?;
    }
    let mut buf = Vec::with_capacity(len.min(64 * MIB) as usize);
    let mut hasher = Hasher::new(algo);
    let mut remaining = len;
    let mut chunk = vec![0u8; READ_CHUNK];
    while remaining > 0 {
        let want = remaining.min(READ_CHUNK as u64) as usize;
        let n = file
            .read(&mut chunk[..want])
            .await
            .map_err(ReplError::classify_io)?;
        if n == 0 {
            // EOF before `len`: the source shrank/was truncated since `deliver` read its metadata
            // (or a producer is still rewriting it). Delivering the short buffer would ship a
            // truncated object — and under `verify:none` the worker would then delete the source
            // (data loss). Fail as Transient so the next attempt re-reads metadata and re-plans
            // against the current size (self-healing) rather than committing a partial object.
            return Err(ReplError::Transient(format!(
                "source truncated: read {} of {len} bytes at offset {offset} from {}",
                len - remaining,
                path.display()
            )));
        }
        bw.throttle(n as u64).await;
        hasher.update(&chunk[..n]);
        buf.extend_from_slice(&chunk[..n]);
        remaining -= n as u64;
        on_progress(n as u64);
    }
    Ok((buf, hasher.finish()))
}

// ---- the destination ----------------------------------------------------------------------------

/// The S3 backend. Holds the resolved, immutable transfer parameters; the AWS [`Client`] is built
/// lazily on first use ([`OnceCell`]) so construction stays synchronous and offline-tolerant (a bad
/// region/credential surfaces on the first transfer as a classified error the retry engine handles,
/// not a startup crash). Cloneable field access for [`client`] is `pub(crate)`.
///
/// [`Client`]: aws_sdk_s3::Client
/// [`OnceCell`]: tokio::sync::OnceCell
pub struct S3Dest {
    pub(crate) bucket: String,
    pub(crate) prefix: String,
    pub(crate) algo: Algorithm,
    pub(crate) storage_class: Option<String>,
    pub(crate) sse: Option<String>,
    pub(crate) kms_key_id: Option<String>,
    pub(crate) unsigned_payload: bool,
    pub(crate) multipart: MultipartParams,
    pub(crate) client_params: S3ClientParams,
    pub(crate) creds: CredMode,
    /// Shared durable store, so the multipart transfer can persist its resume checkpoint mid-flight
    /// (the [`Destination`] trait has no store handle). `None` in tests that inject no store.
    pub(crate) store: Option<Arc<dyn StateStore>>,
    pub(crate) client: tokio::sync::OnceCell<aws_sdk_s3::Client>,
}

impl S3Dest {
    /// Build the backend from its typed egress config and the shared [`DestDeps`] (credentials +
    /// store). Resolves the checksum algorithm, credential mode, multipart params, and client params
    /// eagerly (all pure/synchronous); the network client is deferred to first transfer.
    pub fn build(cfg: &S3Egress, deps: &DestDeps) -> Result<Self> {
        let algo = match &cfg.checksum_algorithm {
            Some(name) => Algorithm::from_name(name).ok_or_else(|| {
                ReplError::Permanent(format!("unknown egress.checksumAlgorithm '{name}'"))
            })?,
            None => Algorithm::Crc32c,
        };
        let creds = resolve_credentials(&cfg.credentials, deps.credentials.as_deref())?;
        Ok(S3Dest {
            bucket: cfg.bucket.clone(),
            prefix: cfg.prefix.clone(),
            algo,
            storage_class: cfg.storage_class.clone(),
            sse: cfg.sse.clone(),
            kms_key_id: cfg.kms_key_id.clone(),
            unsigned_payload: cfg.unsigned_payload,
            multipart: MultipartParams::resolve(&cfg.multipart),
            client_params: s3_client_params(cfg),
            creds,
            store: deps.store.clone(),
            client: tokio::sync::OnceCell::new(),
        })
    }
}

// The lazy client builder (`s3()`) and the `impl Destination for S3Dest` delegation glue live in
// [`client`] — every line makes (or immediately gates) an AWS SDK round-trip, so they belong on the
// coverage-excluded side of the NFR-2 seam rather than eroding this module's counted coverage.

#[cfg(test)]
mod tests {
    use super::*;
    // The `Destination` trait impl lives in `client`; bring the trait into scope so the
    // construction tests can call `kind()`/`supports_resume()`.
    use crate::dest::Destination;

    // ---- MultipartParams / plan_upload ----------------------------------------------------------

    fn params(threshold: u64, part_size: u64, max_concurrent: usize) -> MultipartParams {
        MultipartParams {
            threshold,
            part_size,
            max_concurrent,
        }
    }

    #[test]
    fn multipart_params_defaults_and_clamps() {
        let d = MultipartParams::resolve(&MultipartCfg::default());
        assert_eq!(d.threshold, 16 * MIB);
        assert_eq!(d.part_size, 16 * MIB);
        assert_eq!(d.max_concurrent, 4);

        // A too-small part size is clamped up to the 5 MiB floor; concurrency floored to 1.
        let c = MultipartParams::resolve(&MultipartCfg {
            threshold_bytes: Some(1024),
            part_size_bytes: Some(1024),
            max_concurrent_parts: Some(0),
        });
        assert_eq!(c.threshold, 1024);
        assert_eq!(c.part_size, S3_MIN_PART);
        assert_eq!(c.max_concurrent, 1);

        // A huge part size is clamped down to the 5 GiB ceiling.
        let big = MultipartParams::resolve(&MultipartCfg {
            threshold_bytes: None,
            part_size_bytes: Some(50 * 1024 * MIB),
            max_concurrent_parts: Some(8),
        });
        assert_eq!(big.part_size, S3_MAX_PART);
        assert_eq!(big.max_concurrent, 8);
    }

    #[test]
    fn plan_single_at_and_below_threshold() {
        let p = params(16 * MIB, 8 * MIB, 4);
        assert_eq!(plan_upload(0, &p), UploadPlan::Single, "0-byte → single");
        assert_eq!(plan_upload(1, &p), UploadPlan::Single);
        assert_eq!(plan_upload(16 * MIB, &p), UploadPlan::Single, "== threshold → single");
    }

    #[test]
    fn plan_multipart_just_over_threshold() {
        let p = params(16 * MIB, 8 * MIB, 4);
        // 16 MiB + 1 → multipart with 8 MiB parts → 3 parts (8, 8, ~0).
        match plan_upload(16 * MIB + 1, &p) {
            UploadPlan::Multipart { part_size, num_parts } => {
                assert_eq!(part_size, 8 * MIB);
                assert_eq!(num_parts, 3);
            }
            other => panic!("expected multipart, got {other:?}"),
        }
    }

    #[test]
    fn plan_honors_five_mib_floor() {
        // A configured 1 MiB part size is impossible for S3; resolve() clamps to 5 MiB, and the plan
        // uses that floor.
        let p = MultipartParams::resolve(&MultipartCfg {
            threshold_bytes: Some(5 * MIB),
            part_size_bytes: Some(MIB),
            max_concurrent_parts: Some(2),
        });
        match plan_upload(12 * MIB, &p) {
            UploadPlan::Multipart { part_size, num_parts } => {
                assert_eq!(part_size, S3_MIN_PART);
                assert_eq!(num_parts, 3); // ceil(12/5)
            }
            other => panic!("expected multipart, got {other:?}"),
        }
    }

    #[test]
    fn plan_autoscales_part_size_to_stay_under_10000_parts() {
        // 1 TiB with a 16 MiB configured part size would need 65 536 parts (> 10 000). The planner
        // must grow the part size so num_parts ≤ 10 000.
        let p = params(16 * MIB, 16 * MIB, 4);
        let size = 1024 * 1024 * MIB; // 1 TiB
        match plan_upload(size, &p) {
            UploadPlan::Multipart { part_size, num_parts } => {
                assert!(num_parts as u64 <= S3_MAX_PARTS, "num_parts {num_parts} must be ≤ 10 000");
                assert!(part_size >= size.div_ceil(S3_MAX_PARTS));
                // And every part covers the object.
                assert!((num_parts as u64) * part_size >= size);
            }
            other => panic!("expected multipart, got {other:?}"),
        }
    }

    #[test]
    fn plan_keeps_configured_part_size_when_it_satisfies_the_cap() {
        // 100 MiB with a 20 MiB part size → 5 parts, well under the cap → keep 20 MiB exactly.
        let p = params(16 * MIB, 20 * MIB, 4);
        match plan_upload(100 * MIB, &p) {
            UploadPlan::Multipart { part_size, num_parts } => {
                assert_eq!(part_size, 20 * MIB);
                assert_eq!(num_parts, 5);
            }
            other => panic!("expected multipart, got {other:?}"),
        }
    }

    #[test]
    fn part_len_remainder_math() {
        // 12 MiB in 5 MiB parts → [5, 5, 2].
        assert_eq!(part_len(1, 5 * MIB, 3, 12 * MIB), 5 * MIB);
        assert_eq!(part_len(2, 5 * MIB, 3, 12 * MIB), 5 * MIB);
        assert_eq!(part_len(3, 5 * MIB, 3, 12 * MIB), 2 * MIB);
        // Exact multiple: last part is a full part.
        assert_eq!(part_len(2, 5 * MIB, 2, 10 * MIB), 5 * MIB);
    }

    // ---- object_key -----------------------------------------------------------------------------

    #[test]
    fn object_key_joins_prefix_and_relpath() {
        assert_eq!(object_key("p", "a/b.txt"), "p/a/b.txt");
        assert_eq!(object_key("", "a/b.txt"), "a/b.txt");
        assert_eq!(object_key("data/out", "deep/nested/r.csv"), "data/out/deep/nested/r.csv");
    }

    #[test]
    fn object_key_normalizes_slashes_and_strips_traversal() {
        assert_eq!(object_key("/p/", "a/b.txt"), "p/a/b.txt", "leading+trailing slash trimmed");
        // `.`/`..`/empty segments are dropped (so the key can never escape the prefix); like
        // `local.rs`, a `..` drops only itself, it does not pop the preceding segment.
        assert_eq!(object_key("p", "a/../b/./c.txt"), "p/a/b/c.txt", "traversal + . segments dropped");
        assert!(!object_key("p", "../../etc/passwd").contains(".."), "no traversal escapes the prefix");
        assert_eq!(object_key("p", "//a//b//"), "p/a/b", "empty segments dropped");
        assert_eq!(object_key("p", ""), "p", "empty relpath → prefix only");
    }

    // ---- checksum codecs ------------------------------------------------------------------------

    #[test]
    fn crc32c_b64_round_trip_and_known_vector() {
        // CRC32C("123456789") = 0xE3069283 (Castagnoli check value, matches integrity.rs).
        let b64 = crc32c_to_b64(0xE306_9283);
        assert_eq!(b64, "4waSgw==");
        assert_eq!(b64_to_crc32c(&b64), Some(0xE306_9283));
        for v in [0u32, 1, 0xFFFF_FFFF, 0x1234_5678] {
            assert_eq!(b64_to_crc32c(&crc32c_to_b64(v)), Some(v));
        }
        assert_eq!(b64_to_crc32c("not-4-bytes"), None);
    }

    #[test]
    fn sha256_b64_encodes_32_bytes() {
        let digest = [0u8; 32];
        let b64 = sha256_to_b64(&digest);
        // 32 zero bytes → base64 of 32 zero bytes.
        assert_eq!(base64::engine::general_purpose::STANDARD.decode(&b64).unwrap().len(), 32);
        assert_eq!(checksum_to_b64(&Checksum::Sha256(digest)), Some(b64));
    }

    #[test]
    fn checksum_to_b64_variants() {
        assert_eq!(checksum_to_b64(&Checksum::Crc32c(0xE306_9283)), Some("4waSgw==".to_string()));
        assert!(checksum_to_b64(&Checksum::Sha256([1u8; 32])).is_some());
        assert_eq!(checksum_to_b64(&Checksum::None), None);
    }

    #[test]
    fn compare_object_checksum_ok_and_mismatch() {
        assert!(compare_object_checksum("abc", "abc").is_ok());
        assert!(matches!(
            compare_object_checksum("abc", "xyz").unwrap_err(),
            ReplError::Integrity(_)
        ));
    }

    #[test]
    fn algo_name_tokens() {
        assert_eq!(algo_name(Algorithm::Crc32c), "CRC32C");
        assert_eq!(algo_name(Algorithm::Sha256), "SHA256");
    }

    // ---- resume bookkeeping ---------------------------------------------------------------------

    fn parts(nums: &[u32]) -> Vec<S3CompletedPart> {
        nums.iter()
            .map(|n| S3CompletedPart {
                part_number: *n,
                e_tag: format!("etag{n}"),
                checksum: Some(format!("ck{n}")),
            })
            .collect()
    }

    #[test]
    fn missing_parts_computes_the_gap() {
        assert_eq!(missing_parts(4, &parts(&[1, 3])), vec![2, 4]);
        assert_eq!(missing_parts(3, &[]), vec![1, 2, 3]);
        assert_eq!(missing_parts(3, &parts(&[1, 2, 3])), Vec::<u32>::new());
    }

    #[test]
    fn s3_resume_token_serde_round_trip_via_resume_state() {
        let tok = S3ResumeToken {
            upload_id: "UP-123".into(),
            key: "p/a.bin".into(),
            part_size: 5 * MIB,
            total_size: 12 * MIB,
            mtime_ms: 1_700_000,
            completed: parts(&[1, 2]),
        };
        let rs = tok.to_resume(3, 12 * MIB);
        // bytes_committed = two full 5 MiB parts.
        assert_eq!(rs.bytes_committed, 10 * MIB);
        // Round-trips through the ResumeState JSON blob (as state.rs stores it).
        let json = serde_json::to_string(&rs).unwrap();
        let back: ResumeState = serde_json::from_str(&json).unwrap();
        let tok2 = S3ResumeToken::from_resume(&back).expect("token present");
        assert_eq!(tok2, tok);
    }

    #[test]
    fn s3_resume_token_absent_or_wrong_shape_is_none() {
        let empty = ResumeState::default();
        assert!(S3ResumeToken::from_resume(&empty).is_none());
        let local_shape = ResumeState {
            bytes_committed: 1,
            token: serde_json::json!({ "temp": "x.part" }),
        };
        assert!(S3ResumeToken::from_resume(&local_shape).is_none());
    }

    #[test]
    fn s3_resume_token_matches_guards_key_partsize_size_and_mtime() {
        let tok = S3ResumeToken {
            upload_id: "U".into(),
            key: "p/a.bin".into(),
            part_size: 5 * MIB,
            total_size: 12 * MIB,
            mtime_ms: 42,
            completed: vec![],
        };
        assert!(tok.matches("p/a.bin", 5 * MIB, 12 * MIB, 42));
        assert!(!tok.matches("p/a.bin", 8 * MIB, 12 * MIB, 42), "part-size change → discard resume");
        assert!(!tok.matches("p/other.bin", 5 * MIB, 12 * MIB, 42), "key change → discard resume");
        // A grown/shrunk source (size change) must not reuse the persisted parts.
        assert!(!tok.matches("p/a.bin", 5 * MIB, 20 * MIB, 42), "size change → discard resume");
        // A same-length in-place rewrite (mtime change) must not reuse the persisted parts.
        assert!(!tok.matches("p/a.bin", 5 * MIB, 12 * MIB, 99), "mtime change → discard resume");
        // A legacy row (total_size/mtime defaulted to 0) never matches a real upload → fresh MPU.
        let legacy = S3ResumeToken {
            total_size: 0,
            mtime_ms: 0,
            ..tok.clone()
        };
        assert!(!legacy.matches("p/a.bin", 5 * MIB, 12 * MIB, 42), "legacy row → discard resume");
    }

    #[test]
    fn bytes_committed_counts_remainder_last_part() {
        let tok = S3ResumeToken {
            upload_id: "U".into(),
            key: "k".into(),
            part_size: 5 * MIB,
            total_size: 12 * MIB,
            mtime_ms: 0,
            completed: parts(&[3]), // only the last (2 MiB) part done
        };
        assert_eq!(tok.bytes_committed(3, 12 * MIB), 2 * MIB);
    }

    // ---- delivery handle ------------------------------------------------------------------------

    #[test]
    fn handle_round_trips_key_and_checksum() {
        let h = build_handle("p/a.bin", Some("Zm9v"), Algorithm::Crc32c, true);
        assert_eq!(handle_key(&h).as_deref(), Some("p/a.bin"));
        assert_eq!(handle_s3_checksum(&h).as_deref(), Some("Zm9v"));
        assert_eq!(h.get("multipart").and_then(|v| v.as_bool()), Some(true));
        assert_eq!(h.get("algo").and_then(|v| v.as_str()), Some("CRC32C"));

        let none = build_handle("k", None, Algorithm::Sha256, false);
        assert_eq!(handle_s3_checksum(&none), None);
        assert_eq!(handle_key(&none).as_deref(), Some("k"));
    }

    // ---- credentials ----------------------------------------------------------------------------

    /// A fake credential service holding one secret's JSON bytes. It overrides the `get_aws_credentials`
    /// typed view directly (the opaque `Secret` has no public constructor), which is exactly what
    /// [`resolve_credentials`] calls.
    struct FakeCreds {
        name: String,
        json: Option<Vec<u8>>,
    }
    impl CredentialService for FakeCreds {
        fn get(&self, _name: &str) -> ggcommons::Result<Option<ggcommons::credentials::Secret>> {
            Ok(None)
        }
        fn get_version(
            &self,
            _n: &str,
            _v: &str,
        ) -> ggcommons::Result<Option<ggcommons::credentials::Secret>> {
            Ok(None)
        }
        fn exists(&self, name: &str) -> ggcommons::Result<bool> {
            Ok(name == self.name && self.json.is_some())
        }
        fn list(
            &self,
            _prefix: &str,
        ) -> ggcommons::Result<Vec<ggcommons::credentials::SecretMeta>> {
            Ok(vec![])
        }
        fn versions(&self, _n: &str) -> ggcommons::Result<Vec<String>> {
            Ok(vec![])
        }
        fn put(
            &self,
            _n: &str,
            _v: &[u8],
            _o: ggcommons::credentials::PutOptions,
        ) -> ggcommons::Result<String> {
            Ok("v1".to_string())
        }
        fn delete(&self, _n: &str) -> ggcommons::Result<bool> {
            Ok(false)
        }
        fn get_aws_credentials(
            &self,
            name: &str,
        ) -> ggcommons::Result<Option<ggcommons::credentials::AwsCredentials>> {
            if name == self.name {
                if let Some(bytes) = &self.json {
                    let aws = serde_json::from_slice(bytes)
                        .map_err(|e| ggcommons::GgError::Credentials(format!("bad AWS secret: {e}")))?;
                    return Ok(Some(aws));
                }
            }
            Ok(None)
        }
    }

    fn aws_secret_json() -> Vec<u8> {
        serde_json::to_vec(&serde_json::json!({
            "accessKeyId": "AKIA_TEST",
            "secretAccessKey": "secret123",
            "sessionToken": "tok456"
        }))
        .unwrap()
    }

    #[test]
    fn resolve_credentials_absent_is_ambient() {
        assert_eq!(resolve_credentials(&None, None).unwrap(), CredMode::Ambient);
    }

    #[test]
    fn resolve_credentials_secret_ref_becomes_static() {
        let creds = FakeCreds {
            name: "s3-creds".into(),
            json: Some(aws_secret_json()),
        };
        let cfg = Some(serde_json::json!({ "$secret": "s3-creds" }));
        let mode = resolve_credentials(&cfg, Some(&creds)).unwrap();
        assert_eq!(
            mode,
            CredMode::Static {
                access_key_id: "AKIA_TEST".into(),
                secret_access_key: "secret123".into(),
                session_token: Some("tok456".into()),
            }
        );
    }

    #[test]
    fn resolve_credentials_secret_ref_without_service_is_permanent() {
        let cfg = Some(serde_json::json!({ "$secret": "s3-creds" }));
        assert!(resolve_credentials(&cfg, None).unwrap_err().is_permanent());
    }

    #[test]
    fn resolve_credentials_unknown_secret_is_permanent() {
        let creds = FakeCreds {
            name: "other".into(),
            json: Some(aws_secret_json()),
        };
        let cfg = Some(serde_json::json!({ "$secret": "missing" }));
        assert!(resolve_credentials(&cfg, Some(&creds)).unwrap_err().is_permanent());
    }

    #[test]
    fn resolve_credentials_malformed_secret_is_permanent() {
        let creds = FakeCreds {
            name: "s3-creds".into(),
            json: Some(b"not json".to_vec()),
        };
        let cfg = Some(serde_json::json!({ "$secret": "s3-creds" }));
        assert!(resolve_credentials(&cfg, Some(&creds)).unwrap_err().is_permanent());
    }

    #[test]
    fn resolve_credentials_non_secret_json_is_permanent() {
        let cfg = Some(serde_json::json!({ "accessKeyId": "inline" }));
        assert!(resolve_credentials(&cfg, None).unwrap_err().is_permanent());
    }

    #[test]
    fn cred_mode_debug_redacts_the_secret_and_session_token() {
        // FR-CFG-5 / DESIGN §11.5: a stray `{:?}` (e.g. tracing::debug!(?dest) or an error wrapper)
        // must never surface the resolved secret key or session token.
        let mode = CredMode::Static {
            access_key_id: "AKIA_VISIBLE".into(),
            secret_access_key: "TOP_SECRET_VALUE".into(),
            session_token: Some("SESSION_SECRET_VALUE".into()),
        };
        let dbg = format!("{mode:?}");
        assert!(!dbg.contains("TOP_SECRET_VALUE"), "secret key leaked: {dbg}");
        assert!(!dbg.contains("SESSION_SECRET_VALUE"), "session token leaked: {dbg}");
        assert!(dbg.contains("***redacted***"), "expected redaction marker: {dbg}");
        assert!(dbg.contains("AKIA_VISIBLE"), "access key id (not secret) stays visible: {dbg}");
        // Ambient carries no secret.
        assert_eq!(format!("{:?}", CredMode::Ambient), "Ambient");
    }

    // ---- client params --------------------------------------------------------------------------

    fn s3egress() -> S3Egress {
        S3Egress {
            bucket: "b".into(),
            prefix: String::new(),
            region: None,
            endpoint_url: None,
            credentials: None,
            storage_class: None,
            sse: None,
            kms_key_id: None,
            accelerate: false,
            unsigned_payload: false,
            checksum_algorithm: None,
            multipart: MultipartCfg::default(),
        }
    }

    #[test]
    fn client_params_endpoint_forces_path_style_and_disables_accelerate() {
        let mut cfg = s3egress();
        cfg.endpoint_url = Some("http://localhost:4566".into());
        cfg.accelerate = true; // must be forced off with an endpoint
        let p = s3_client_params(&cfg);
        assert!(p.force_path_style);
        assert!(!p.accelerate, "accelerate is real-AWS-only");
        assert_eq!(p.region.as_deref(), Some("us-east-1"), "endpoint deploys default us-east-1");
        assert_eq!(p.endpoint_url.as_deref(), Some("http://localhost:4566"));
    }

    #[test]
    fn client_params_real_aws_accelerate_and_virtual_hosted() {
        let mut cfg = s3egress();
        cfg.accelerate = true;
        cfg.region = Some("eu-west-1".into());
        let p = s3_client_params(&cfg);
        assert!(!p.force_path_style, "real AWS uses virtual-hosted addressing");
        assert!(p.accelerate, "accelerate on when no endpoint");
        assert_eq!(p.region.as_deref(), Some("eu-west-1"));
    }

    #[test]
    fn client_params_no_region_no_endpoint_leaves_region_to_chain() {
        let p = s3_client_params(&s3egress());
        assert!(p.region.is_none());
        assert!(!p.force_path_style);
        assert!(!p.accelerate);
    }

    // ---- error classification -------------------------------------------------------------------

    #[test]
    fn classify_transport_is_transient() {
        assert!(classify_s3(None, None, true).is_transient());
        assert!(classify_s3(Some("Whatever"), None, true).is_transient());
    }

    #[test]
    fn classify_permanent_codes() {
        for c in ["NoSuchBucket", "AccessDenied", "InvalidRequest", "SignatureDoesNotMatch", "PreconditionFailed"] {
            assert!(classify_s3(Some(c), None, false).is_permanent(), "{c} should be permanent");
        }
    }

    #[test]
    fn classify_transient_codes() {
        for c in ["SlowDown", "ThrottlingException", "ServiceUnavailable", "InternalError", "RequestTimeout"] {
            assert!(classify_s3(Some(c), None, false).is_transient(), "{c} should be transient");
        }
    }

    #[test]
    fn classify_by_status_when_code_unknown() {
        assert!(classify_s3(None, Some(503), false).is_transient());
        assert!(classify_s3(None, Some(500), false).is_transient());
        assert!(classify_s3(None, Some(429), false).is_transient());
        assert!(classify_s3(None, Some(403), false).is_permanent());
        assert!(classify_s3(None, Some(404), false).is_permanent());
        assert!(classify_s3(None, Some(400), false).is_permanent());
    }

    #[test]
    fn classify_unknown_service_code_is_permanent_bare_failure_is_transient() {
        // A service error with an unrecognized code (server rejected) → permanent.
        assert!(classify_s3(Some("SomethingWeird"), None, false).is_permanent());
        // Nothing to go on → retry defensively.
        assert!(classify_s3(None, None, false).is_transient());
        // Known transient code wins even if a 4xx status is present.
        assert!(classify_s3(Some("SlowDown"), Some(400), false).is_transient());
    }

    // ---- read_range (streaming, no network) -----------------------------------------------------

    #[tokio::test]
    async fn read_range_reads_whole_file_hashes_and_reports_progress() {
        use std::sync::atomic::{AtomicU64, Ordering};
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("f.bin");
        let data = vec![7u8; READ_CHUNK * 2 + 100];
        std::fs::write(&path, &data).unwrap();

        let seen = Arc::new(AtomicU64::new(0));
        let s = seen.clone();
        let (buf, ck) = read_range(
            &path,
            0,
            data.len() as u64,
            &Bandwidth::unlimited(),
            Algorithm::Crc32c,
            &move |d| {
                s.fetch_add(d, Ordering::SeqCst);
            },
        )
        .await
        .unwrap();
        assert_eq!(buf, data, "whole file read");
        assert_eq!(seen.load(Ordering::SeqCst), data.len() as u64, "progress deltas sum to len");
        // Checksum equals the one-pass integrity hash of the same bytes.
        let mut r = data.as_slice();
        let (_, expect) = crate::integrity::hash_reader(&mut r, Algorithm::Crc32c).unwrap();
        assert_eq!(ck, expect);
    }

    #[tokio::test]
    async fn read_range_reads_an_offset_slice() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("f.bin");
        let data: Vec<u8> = (0..30_000u32).map(|i| (i % 256) as u8).collect();
        std::fs::write(&path, &data).unwrap();

        let (buf, ck) = read_range(
            &path,
            10_000,
            5_000,
            &Bandwidth::unlimited(),
            Algorithm::Crc32c,
            &|_d| {},
        )
        .await
        .unwrap();
        assert_eq!(buf, &data[10_000..15_000], "reads exactly the requested range");
        let mut slice = &data[10_000..15_000];
        let (_, expect) = crate::integrity::hash_reader(&mut slice, Algorithm::Crc32c).unwrap();
        assert_eq!(ck, expect, "per-part checksum is over the part bytes only");
    }

    #[tokio::test]
    async fn read_range_short_read_is_transient_not_a_silent_truncation() {
        // If the source shrank since `deliver` planned the transfer, read_range must FAIL (so no
        // truncated object is delivered + the source deleted under verify:none), not return a short
        // buffer. Transient so the next attempt re-plans against the smaller size (self-healing).
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("short.bin");
        std::fs::write(&path, vec![9u8; 1000]).unwrap();
        let err = read_range(
            &path,
            0,
            5000, // ask for more than the file holds
            &Bandwidth::unlimited(),
            Algorithm::Crc32c,
            &|_d| {},
        )
        .await
        .unwrap_err();
        assert!(err.is_transient(), "short read must be transient (re-plan), got {err:?}");
        assert!(err.to_string().contains("truncated"), "diagnostic mentions truncation: {err}");
    }

    #[tokio::test]
    async fn read_range_missing_file_is_permanent() {
        let err = read_range(
            std::path::Path::new("/no/such/file.bin"),
            0,
            10,
            &Bandwidth::unlimited(),
            Algorithm::Crc32c,
            &|_d| {},
        )
        .await
        .unwrap_err();
        assert!(err.is_permanent(), "NotFound source classifies permanent: {err:?}");
    }

    // ---- S3Dest::build --------------------------------------------------------------------------

    #[test]
    fn build_defaults_to_crc32c_and_ambient() {
        let dest = S3Dest::build(&s3egress(), &DestDeps::default()).unwrap();
        assert_eq!(dest.kind(), "s3");
        assert!(dest.supports_resume());
        assert_eq!(dest.algo, Algorithm::Crc32c);
        assert_eq!(dest.creds, CredMode::Ambient);
        assert_eq!(dest.multipart.threshold, 16 * MIB);
    }

    #[test]
    fn build_rejects_unknown_checksum_algorithm() {
        let mut cfg = s3egress();
        cfg.checksum_algorithm = Some("MD5".into());
        assert!(S3Dest::build(&cfg, &DestDeps::default())
            .err()
            .expect("unknown algorithm rejected")
            .is_permanent());
    }

    #[test]
    fn build_reads_sha256_and_storage_options() {
        let mut cfg = s3egress();
        cfg.checksum_algorithm = Some("SHA-256".into());
        cfg.storage_class = Some("STANDARD_IA".into());
        cfg.sse = Some("aws:kms".into());
        cfg.kms_key_id = Some("key-arn".into());
        let dest = S3Dest::build(&cfg, &DestDeps::default()).unwrap();
        assert_eq!(dest.algo, Algorithm::Sha256);
        assert_eq!(dest.storage_class.as_deref(), Some("STANDARD_IA"));
        assert_eq!(dest.sse.as_deref(), Some("aws:kms"));
        assert_eq!(dest.kms_key_id.as_deref(), Some("key-arn"));
    }
}
