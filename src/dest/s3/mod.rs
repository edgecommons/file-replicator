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
//! or the streaming, throttled/progress-reported egress body ([`RangeBody`]) — all unit-tested with no
//! network, and bounded to one [`READ_CHUNK`] at a time (no whole-part/whole-file buffer). The object's
//! flexible checksum is a **single-pass trailing checksum**: [`client`] sets `.checksum_algorithm(..)`
//! and the streaming body only, so the aws-sdk-s3 flexible-checksum machinery (`ChecksumBody`) computes
//! the digest over exactly the bytes [`RangeBody`] streams and emits it as an aws-chunked trailer — one
//! disk read per range, not two. Every line that makes an actual AWS
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

use std::path::Path;
use std::pin::Pin;
use std::sync::{Arc, Mutex as StdMutex};
use std::task::{Context, Poll};

use bytes::Bytes;
use futures_core::Stream;
use serde::{Deserialize, Serialize};

use crate::config::{MultipartCfg, S3Egress};
use crate::domain::{Checksum, ResumeState};
use crate::error::{ReplError, Result};
use crate::integrity::Algorithm;
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
//
// NOTE (single-pass conversion): these encoders are no longer on the S3 *delivery* path. The old
// two-pass upload precomputed the checksum locally and sent it as an `x-amz-checksum-*` header (that
// header value came from [`checksum_to_b64`]); the single-pass trailing checksum instead lets the SDK
// compute the digest over the streamed body and reads the resulting object/part checksum back off the
// response as an opaque base64 string (see [`client::response_checksum`]), so nothing in
// deliver/verify calls these anymore. They are retained deliberately as small, unit-tested pure codecs
// and are exercised end-to-end by the real-S3 integration test (`tests/s3_real.rs`), which independently
// recomputes the expected base64 CRC32C of the source and asserts it equals the checksum stored in the
// delivery handle — the assertion that guards against the response silently ceasing to echo a checksum.

use base64::Engine as _;

/// Base64 of the 4-byte big-endian CRC32C — the S3 flexible-checksum value for a CRC32C digest.
pub fn crc32c_to_b64(v: u32) -> String {
    base64::engine::general_purpose::STANDARD.encode(v.to_be_bytes())
}

/// Base64 of a 32-byte SHA-256 digest — the S3 flexible-checksum value for a SHA-256 digest.
pub fn sha256_to_b64(b: &[u8; 32]) -> String {
    base64::engine::general_purpose::STANDARD.encode(b)
}

/// The S3 flexible-checksum base64 value for a [`Checksum`], or `None` when no checksum was computed.
/// Retained as a tested pure codec (see the section note): the delivery path no longer sends a
/// precomputed checksum header, but the real-S3 test uses this to derive the expected value it compares
/// against the response-derived object checksum.
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

use edgecommons::credentials::CredentialService;

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
    /// Static keys resolved from the edgecommons vault via a `{"$secret":"…"}` egress reference.
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
/// - `{"$secret":"name"}` → look the secret up as [`AwsCredentials`](edgecommons::credentials) and use
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

/// True for S3 error codes worth retrying (throttling / capacity / transient server faults) — plus the
/// flexible-checksum / digest **mismatch** codes. With the single-pass trailing checksum (the digest is
/// computed over exactly the bytes [`RangeBody`] streams, by the same pass that sends them) a mismatch
/// can no longer be caused by *our own* precompute-vs-stream desync — that race is structurally
/// eliminated. `BadDigest` / `XAmzContentChecksumMismatch` / `XAmzContentSHA256Mismatch` are kept
/// transient anyway as a defensible "retry a genuine in-transit corruption" (a bit-flip on the wire is
/// rare but real, and one extra retry is cheap); they are no longer load-bearing for our integrity
/// story. `InvalidDigest` — a *malformed* digest value — is deliberately NOT here: that is a
/// client-side bug, not a content race, so it stays permanent (via the 4xx path).
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
            | "BadDigest"
            | "XAmzContentChecksumMismatch"
            | "XAmzContentSHA256Mismatch"
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

/// The truncation message for [`RangeBody`]'s short-read guard (`io::Error`, surfaced through the
/// SDK's dispatch-failure → [`client::map_sdk`] transport classification).
fn truncated_msg(path: &Path, offset: u64, len: u64, read: u64) -> String {
    format!("source truncated: read {read} of {len} bytes at offset {offset} from {}", path.display())
}

/// Whether the source changed under an in-flight upload: its size or mtime differs from what the
/// upload path captured before it started sending. The single-pass **trailing** checksum is computed
/// over exactly the bytes streamed, so it can never itself desync from the sent body — but a source
/// rewritten *mid-stream* still produces a self-consistent-but-**torn** snapshot (early chunks from the
/// old content, later chunks from the new) whose checksum S3 happily accepts: the bytes shipped are
/// simply not a coherent point-in-time copy of the file. The single-PUT and multipart paths re-stat the
/// source after the send and, if this returns `true`, fail `Transient` so the next attempt re-reads
/// metadata and re-streams the now-stable bytes (self-healing). This guard is orthogonal to checksum
/// integrity and remains necessary even though the trailing checksum eliminated the old two-pass
/// precompute-vs-stream race (DESIGN §11.4/§13.2).
///
/// Residual window (unchanged by the single-pass conversion, but now the *only* torn-read detector on
/// every endpoint): the old two-pass code hashed the *old* content in the precompute pass while the
/// stream sent the *new* content after an in-place rewrite, so on a checksum-validating endpoint (real
/// S3) that specific race also surfaced as a `BadDigest` regardless of mtime. The single-pass trailing
/// checksum is always self-consistent with the exact bytes streamed, so a torn snapshot now carries a
/// checksum S3 accepts on every endpoint and size+mtime is the sole detector. The one case it cannot
/// see is a *same-length in-place rewrite whose mtime is unchanged across the whole upload* — possible
/// only with coarse filesystem mtime granularity plus a writer that preserves/resets mtime. Realistic
/// throttled uploads span far more than mtime granularity, so a mid-stream content change virtually
/// always bumps mtime and is caught; this matches the pre-existing floci/MinIO size-only gap (DESIGN
/// §11.3/§11.4). A stronger guarantee would need an opt-in post-send read-back verify (not implemented).
pub fn source_changed(orig_size: u64, orig_mtime_ms: i64, now_size: u64, now_mtime_ms: i64) -> bool {
    orig_size != now_size || orig_mtime_ms != now_mtime_ms
}

/// A `Send + Sync` streaming egress body over one byte range of `path`: reads bounded [`READ_CHUNK`]
/// windows straight off disk, throttling every chunk through the bandwidth governor and reporting its
/// length via `on_progress` **as it is handed to the SDK** (not pre-read into a buffer) — the
/// memory-bounded replacement for the old whole-part `Vec<u8>` body. Implements [`http_body::Body`] so
/// [`client`] can wrap it into a retryable `SdkBody` (`SdkBody::from_body_0_4`) for `PutObject` /
/// `UploadPart` without ever materializing more than one chunk of the range at a time (peak transfer
/// memory ≈ `max_concurrent × READ_CHUNK`, independent of part/file size).
///
/// This is the **only** disk read per range: [`client`] hands this body straight to the SDK's flexible
/// -checksum machinery, which wraps it to compute the object/part's trailing checksum over exactly the
/// bytes read here as they are streamed — a single pass, not a precompute-then-resend.
///
/// A short read before the range is exhausted yields an `io::Error`; the SDK surfaces that as a
/// dispatch failure, which [`client::map_sdk`] classifies as `Transient` — self-healing re-plan.
pub struct RangeBody {
    // A boxed `dyn Stream` is `Send` but not (in general) `Sync`, and `http_body::Body` requires
    // `Sync`. `poll_data` is handed `Pin<&mut Self>` (this struct is `Unpin`, so `get_mut()` recovers
    // `&mut Self`), i.e. every call already has exclusive access — the `Mutex` exists purely to make
    // the type `Sync`; `get_mut()` (not `lock()`) bypasses any runtime locking.
    stream: StdMutex<Pin<Box<dyn Stream<Item = std::io::Result<Bytes>> + Send>>>,
    len: u64,
}

impl RangeBody {
    /// Build the streaming body for `[offset, offset+len)` of `path`. `on_progress` receives each
    /// chunk's length (a delta) as it streams.
    pub fn new(
        path: Arc<Path>,
        offset: u64,
        len: u64,
        bw: Bandwidth,
        on_progress: Arc<dyn Fn(u64) + Send + Sync>,
    ) -> Self {
        let stream = async_stream::try_stream! {
            use tokio::io::{AsyncReadExt, AsyncSeekExt};

            let mut file = tokio::fs::File::open(&*path).await?;
            if offset > 0 {
                file.seek(std::io::SeekFrom::Start(offset)).await?;
            }
            let mut remaining = len;
            let mut chunk = vec![0u8; READ_CHUNK];
            while remaining > 0 {
                let want = remaining.min(READ_CHUNK as u64) as usize;
                let n = file.read(&mut chunk[..want]).await?;
                if n == 0 {
                    Err(std::io::Error::new(
                        std::io::ErrorKind::UnexpectedEof,
                        truncated_msg(&path, offset, len, len - remaining),
                    ))?;
                }
                bw.throttle(n as u64).await;
                on_progress(n as u64);
                remaining -= n as u64;
                yield Bytes::copy_from_slice(&chunk[..n]);
            }
        };
        RangeBody {
            stream: StdMutex::new(Box::pin(stream)),
            len,
        }
    }
}

impl http_body::Body for RangeBody {
    type Data = Bytes;
    type Error = std::io::Error;

    fn poll_data(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<std::result::Result<Bytes, std::io::Error>>> {
        let this = self.get_mut();
        this.stream.get_mut().expect("range body mutex").as_mut().poll_next(cx)
    }

    fn poll_trailers(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
    ) -> Poll<std::result::Result<Option<http::HeaderMap>, std::io::Error>> {
        Poll::Ready(Ok(None))
    }

    fn size_hint(&self) -> http_body::SizeHint {
        // Exact (lower == upper) = the *decoded* content length of this range. On the trailing-checksum
        // path the request IS `aws-chunked` content-encoded (`STREAMING-…-TRAILER`), and the SDK's
        // aws-chunked/trailing-checksum interceptor consumes this exact hint to derive both the framed
        // `Content-Length` (decoded length + the chunk/trailer framing overhead) and
        // `x-amz-decoded-content-length`. A non-exact hint would leave it unable to size the framed body
        // (S3 `PutObject`/`UploadPart` require a length up front — the body is not chunked
        // transfer-encoded at the HTTP layer).
        http_body::SizeHint::with_exact(self.len)
    }
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
    use crate::integrity::Hasher;
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
        fn get(&self, _name: &str) -> edgecommons::Result<Option<edgecommons::credentials::Secret>> {
            Ok(None)
        }
        fn get_version(
            &self,
            _n: &str,
            _v: &str,
        ) -> edgecommons::Result<Option<edgecommons::credentials::Secret>> {
            Ok(None)
        }
        fn exists(&self, name: &str) -> edgecommons::Result<bool> {
            Ok(name == self.name && self.json.is_some())
        }
        fn list(
            &self,
            _prefix: &str,
        ) -> edgecommons::Result<Vec<edgecommons::credentials::SecretMeta>> {
            Ok(vec![])
        }
        fn versions(&self, _n: &str) -> edgecommons::Result<Vec<String>> {
            Ok(vec![])
        }
        fn put(
            &self,
            _n: &str,
            _v: &[u8],
            _o: edgecommons::credentials::PutOptions,
        ) -> edgecommons::Result<String> {
            Ok("v1".to_string())
        }
        fn delete(&self, _n: &str) -> edgecommons::Result<bool> {
            Ok(false)
        }
        fn get_aws_credentials(
            &self,
            name: &str,
        ) -> edgecommons::Result<Option<edgecommons::credentials::AwsCredentials>> {
            if name == self.name {
                if let Some(bytes) = &self.json {
                    let aws = serde_json::from_slice(bytes)
                        .map_err(|e| edgecommons::EdgeCommonsError::Credentials(format!("bad AWS secret: {e}")))?;
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
    fn classify_checksum_mismatch_is_transient_for_self_heal() {
        // No longer load-bearing for our own precompute-vs-stream desync (the trailing checksum is
        // computed over exactly the bytes sent, eliminating that race structurally); kept transient as
        // a defensible retry of a genuine in-transit corruption (DESIGN §11.4).
        for c in ["BadDigest", "XAmzContentChecksumMismatch", "XAmzContentSHA256Mismatch"] {
            assert!(
                classify_s3(Some(c), Some(400), false).is_transient(),
                "{c} should self-heal (transient)"
            );
        }
        // A malformed digest value is a client bug, not a content race → stays permanent (4xx path).
        assert!(classify_s3(Some("InvalidDigest"), Some(400), false).is_permanent());
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

    // ---- source_changed (mid-upload stability guard) --------------------------------------------

    #[test]
    fn source_changed_detects_size_or_mtime_drift() {
        assert!(!source_changed(100, 42, 100, 42), "stable source (size+mtime unchanged)");
        assert!(source_changed(100, 42, 101, 42), "grew");
        assert!(source_changed(100, 42, 99, 42), "shrank");
        assert!(source_changed(100, 42, 100, 43), "same-length in-place rewrite (mtime bumped)");
        assert!(source_changed(100, 42, 101, 43), "both changed");
    }

    // ---- range_byte_stream / RangeBody (streaming egress body, no network) -----------------------

    /// Drain a [`RangeBody`] via its `http_body::Body` impl, folding a [`Hasher`] over the yielded
    /// chunks (mirroring how the real send path would consume it) and returning `(bytes, checksum)`.
    async fn drain_body(mut body: RangeBody, algo: Algorithm) -> std::io::Result<(Vec<u8>, Checksum)> {
        use http_body::Body as _;
        let mut out = Vec::new();
        let mut hasher = Hasher::new(algo);
        let mut pinned = Pin::new(&mut body);
        while let Some(chunk) = std::future::poll_fn(|cx| pinned.as_mut().poll_data(cx)).await {
            let bytes = chunk?;
            hasher.update(&bytes);
            out.extend_from_slice(&bytes);
        }
        Ok((out, hasher.finish()))
    }

    #[tokio::test]
    async fn range_byte_stream_reads_whole_file_throttles_and_reports_progress() {
        use std::sync::atomic::{AtomicU64, Ordering};
        let dir = tempfile::tempdir().unwrap();
        let path: Arc<Path> = dir.path().join("f.bin").into();
        let data = vec![7u8; READ_CHUNK * 2 + 100];
        std::fs::write(&path, &data).unwrap();

        let seen = Arc::new(AtomicU64::new(0));
        let s = seen.clone();
        let body = RangeBody::new(
            path.clone(),
            0,
            data.len() as u64,
            Bandwidth::unlimited(),
            Arc::new(move |d| {
                s.fetch_add(d, Ordering::SeqCst);
            }),
        );
        // Exact size hint drives the SDK's Content-Length.
        assert_eq!(
            http_body::Body::size_hint(&body).exact(),
            Some(data.len() as u64)
        );
        let (buf, ck) = drain_body(body, Algorithm::Crc32c).await.unwrap();
        assert_eq!(buf, data, "whole file streamed");
        assert_eq!(seen.load(Ordering::SeqCst), data.len() as u64, "progress deltas sum to len");
        let mut r = data.as_slice();
        let (_, expect) = crate::integrity::hash_reader(&mut r, Algorithm::Crc32c).unwrap();
        assert_eq!(ck, expect);
    }

    #[tokio::test]
    async fn range_byte_stream_streams_an_offset_slice() {
        let dir = tempfile::tempdir().unwrap();
        let path: Arc<Path> = dir.path().join("f.bin").into();
        let data: Vec<u8> = (0..30_000u32).map(|i| (i % 256) as u8).collect();
        std::fs::write(&path, &data).unwrap();

        let body = RangeBody::new(path, 10_000, 5_000, Bandwidth::unlimited(), Arc::new(|_d| {}));
        let (buf, ck) = drain_body(body, Algorithm::Crc32c).await.unwrap();
        assert_eq!(buf, &data[10_000..15_000], "streams exactly the requested range");
        let mut slice = &data[10_000..15_000];
        let (_, expect) = crate::integrity::hash_reader(&mut slice, Algorithm::Crc32c).unwrap();
        assert_eq!(ck, expect, "per-part checksum is over the part bytes only");
    }

    #[tokio::test]
    async fn range_byte_stream_never_buffers_more_than_one_chunk() {
        // The defect this refactor fixes: no whole-range `Vec<u8>` buffer. Assert every yielded chunk
        // is at most READ_CHUNK bytes, for a range much larger than one chunk.
        let dir = tempfile::tempdir().unwrap();
        let path: Arc<Path> = dir.path().join("big.bin").into();
        let data = vec![3u8; READ_CHUNK * 5 + 1234];
        std::fs::write(&path, &data).unwrap();

        let body = RangeBody::new(
            path,
            0,
            data.len() as u64,
            Bandwidth::unlimited(),
            Arc::new(|_d| {}),
        );
        use http_body::Body as _;
        let mut body = Box::pin(body);
        let mut total = 0usize;
        let mut max_chunk = 0usize;
        while let Some(chunk) = std::future::poll_fn(|cx| body.as_mut().poll_data(cx)).await {
            let bytes = chunk.unwrap();
            max_chunk = max_chunk.max(bytes.len());
            total += bytes.len();
        }
        assert_eq!(total, data.len());
        assert!(max_chunk <= READ_CHUNK, "chunk {max_chunk} exceeds READ_CHUNK {READ_CHUNK}");
    }

    #[tokio::test]
    async fn range_byte_stream_throttles_through_the_bandwidth_governor() {
        // The governor must gate the streamed bytes (not a pre-read): draining the body with a slow
        // per-instance bucket must take real wait time proportional to the data, under paused tokio
        // time so the assertion is deterministic (mirrors ratelimit.rs's paused-time pattern).
        let dir = tempfile::tempdir().unwrap();
        let path: Arc<Path> = dir.path().join("f.bin").into();
        let data = vec![1u8; 2000];
        std::fs::write(&path, &data).unwrap();

        tokio::time::pause();
        let clock = Arc::new(crate::ratelimit::ManualClock::new());
        // 1000 B/s starts with a 1000-byte burst; the 2000-byte file drains it then owes ~1s.
        let bucket = Arc::new(crate::ratelimit::TokenBucket::new(1000, clock));
        let bw = Bandwidth::new(bucket, Arc::new(crate::ratelimit::TokenBucket::unlimited()));
        let body = RangeBody::new(path, 0, data.len() as u64, bw, Arc::new(|_d| {}));
        let start = tokio::time::Instant::now();
        let (buf, _) = drain_body(body, Algorithm::Crc32c).await.unwrap();
        assert_eq!(buf.len(), data.len());
        assert!(
            start.elapsed() >= std::time::Duration::from_millis(900),
            "expected the governor to gate streamed bytes, elapsed {:?}",
            start.elapsed()
        );
    }

    #[tokio::test]
    async fn range_byte_stream_short_read_is_transient_after_sdk_mapping() {
        // A short read surfaces as an io::Error from the body; `client::map_sdk` classifies the
        // resulting SDK dispatch failure as Transient (unit-tested directly here on the io::Error
        // path — this is now the *only* short-read guard on the upload path).
        let dir = tempfile::tempdir().unwrap();
        let path: Arc<Path> = dir.path().join("short.bin").into();
        std::fs::write(&path, vec![9u8; 1000]).unwrap();

        let body = RangeBody::new(path, 0, 5000, Bandwidth::unlimited(), Arc::new(|_d| {}));
        let err = drain_body(body, Algorithm::Crc32c).await.unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::UnexpectedEof);
        assert!(err.to_string().contains("truncated"), "diagnostic mentions truncation: {err}");
    }

    #[tokio::test]
    async fn range_byte_stream_missing_file_yields_an_error() {
        let path: Arc<Path> = std::path::Path::new("/no/such/file.bin").into();
        let body = RangeBody::new(path, 0, 10, Bandwidth::unlimited(), Arc::new(|_d| {}));
        drain_body(body, Algorithm::Crc32c).await.unwrap_err();
    }

    #[tokio::test]
    async fn range_byte_stream_poll_trailers_is_always_empty() {
        // `poll_trailers` is on the coverage-counted side; exercise it so it is not dead-to-tests.
        // RangeBody itself never owns trailer bytes — the SDK's `ChecksumBody` wrapper computes and
        // emits the trailing checksum around this body — so `RangeBody::poll_trailers` must resolve
        // immediately to `Ok(None)`.
        use http_body::Body as _;
        let dir = tempfile::tempdir().unwrap();
        let path: Arc<Path> = dir.path().join("f.bin").into();
        std::fs::write(&path, vec![1u8; 10]).unwrap();

        let mut body = RangeBody::new(path, 0, 10, Bandwidth::unlimited(), Arc::new(|_d| {}));
        let mut pinned = Pin::new(&mut body);
        let trailers = std::future::poll_fn(|cx| pinned.as_mut().poll_trailers(cx))
            .await
            .unwrap();
        assert!(trailers.is_none(), "RangeBody yields no trailers");
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
