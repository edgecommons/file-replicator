//! # file-replicator — GCS destination (DESIGN §10.2/§10.3; P5)
//!
//! Google Cloud Storage ingest via the JSON API's **resumable upload** protocol: initiate a session
//! (`POST .../upload/storage/v1/b/{bucket}/o?uploadType=resumable`, which returns a session URI in the
//! `Location` header), then stream the file as a sequence of `PUT`s to that URI, each carrying
//! `Content-Range: bytes {start}-{end}/{total}` — GCS's chunked/multipart-equivalent (mirrors
//! `dest/http`'s ranged `PUT`s; `dest/azure`'s staged blocks). A 0-byte file uses a one-shot
//! `uploadType=media` `PUT` instead (no session needed, mirrors the other backends' 0-byte special
//! case).
//!
//! ## Why `reqwest` directly, not `google-cloud-storage`/`object_store` (DESIGN §22.2 crate-maturity note)
//! GCS's resumable-upload wire protocol is plain HTTP with no GCS-specific framing, so this backend
//! drives it directly over the same `reqwest` (rustls) client `dest-http` already uses, rather than
//! pulling in a second immature SDK (`google-cloud-storage`) with its own auth/dependency surface on
//! top of `dest-azure`'s `azure_storage_blobs`. Every network call still lives behind [`client`]
//! (excluded from the coverage gate, NFR-2), so the crate-maturity containment DESIGN §22.2 calls for
//! is unaffected by this choice — it just avoids a second large, young dependency tree for a protocol
//! this crate can speak natively with what it already has.
//!
//! ## Resume (DESIGN §10.2 "resumable session URI")
//! After every successful chunk, `{bucket, object, sessionUri, size, mtimeMs, bytesCommitted}` is
//! persisted into the shared [`StateStore`] resume row (keyed by `(instance, relpath, "gcs")`) —
//! write-ahead so a crash resumes only the missing tail. The persisted **session URI** is GCS's own
//! resumable handle — real GCS also supports reconciling it against the server's authoritative byte
//! count via a status-query `PUT` (`Content-Range: bytes */{total}`, empty body); this backend does
//! **not** do that extra round-trip, though, and instead resumes straight from the locally-persisted
//! `bytesCommitted`, the same way `dest/azure`/`dest/http` do (a mismatched chunk send fails the
//! transfer — retried, not silently corrupted — rather than being masked by a query). That is a
//! deliberate simplification, not an oversight: `fake-gcs-server` (this backend's only exercised
//! target, DESIGN §10.3) does not correctly emulate the status-query idiom — probing it live shows it
//! prematurely finalizes the object at whatever was already staged instead of reporting `308` — so
//! depending on it here would trade a well-tested code path for one this backend cannot actually
//! validate against its own integration test. A production deployment against real GCS wanting
//! server-reconciled resume can add the query as a follow-up (mirrors `dest-ftps`'s documented missing
//! live-test follow-up, DESIGN §10.2 table note). A token is reused only when `bucket`/`object`/`size`/
//! `mtime` still match ([`GcsResumeToken::matches`]); [`client::send_chunk`] does defensively compare
//! each `308` response's echoed `Range` header against the expected committed count and **warns** (not
//! fails) on a mismatch. As with `azure`/`http`, the already-committed prefix is re-hashed **locally**
//! (no network — [`hash_local_prefix`]) so the returned whole-file checksum stays correct across a
//! resume.
//!
//! ## Verify (DESIGN §10.2 "CRC32C/MD5")
//! For the default `Algorithm::Crc32c`, `verify` fetches the object's JSON metadata (no re-download)
//! and compares its `crc32c` field directly against the checksum computed while streaming — the same
//! flexible-checksum-echo strategy `dest/s3` uses, avoiding SFTP/FTPS/HTTP/Azure's re-download re-hash.
//! `Algorithm::Sha256` has no GCS metadata equivalent, so it falls back to downloading and re-hashing
//! (mirrors `dest/http::verify`'s `Checksum` path).
//!
//! ## Credentials (DESIGN §11.5-equivalent)
//! This backend authenticates with a bearer **OAuth2 access token** (ambient `egress.accessToken`, a
//! `credentials.$secret` reference resolving to `{"accessToken":"..."}`, or `egress.anonymous: true`
//! for the fake-gcs-server / a public bucket). Full service-account JWT signing and Application
//! Default Credentials discovery (the usual production GCS auth path) are **not implemented** — that
//! needs a JWT-signing dependency this crate does not otherwise carry — and are a documented follow-up,
//! the same way `dest-ftps`'s live integration test is a documented follow-up (DESIGN §10.2 table
//! note). A deployment that needs it can mint a short-lived access token out of band (e.g. `gcloud auth
//! print-access-token` in a sidecar, or a token-vending secret rotated into the vault) and supply it
//! via `credentials.$secret`.
//!
//! ## Where the code lives (coverage seam, NFR-2)
//! Everything in **this file** is pure decision logic (object-path mapping, chunk-size/range math,
//! credential selection, resume bookkeeping, error classification, range-header parsing) plus the
//! streaming *local* file read (no network — mirrors `dest/s3::RangeBody`/`dest/azure`'s equivalent) —
//! all unit-tested with no network. Every line that opens an HTTP connection to GCS or the fake-gcs-
//! server lives in [`client`], excluded from the coverage gate (mirrors `dest/{s3,sftp,ftps,http,azure}/
//! client.rs`). The self-skipping integration test (`tests/gcs_fakegcs.rs`) exercises it live against
//! fake-gcs-server when available.

pub mod client;

use std::path::Path;
use std::sync::Arc;

use serde::{Deserialize, Serialize};

use crate::config::GcsEgress;
use crate::domain::ResumeState;
use crate::error::{ReplError, Result};
use crate::integrity::{Algorithm, Hasher};
use crate::state::StateStore;

use super::DestDeps;

use ggcommons::credentials::CredentialService;

const MIB: u64 = 1024 * 1024;
/// Default resumable-upload chunk size.
const DEFAULT_CHUNK_BYTES: u64 = 8 * MIB;
/// GCS requires every chunk but the last to be a multiple of 256 KiB.
const CHUNK_ALIGN: u64 = 256 * 1024;
/// Streaming read window (matches `dest/s3::READ_CHUNK` / `dest/http::READ_CHUNK`).
const READ_CHUNK: usize = 64 * 1024;
/// The public GCS JSON/upload API host, used when `egress.endpointUrl` is absent.
pub const DEFAULT_ENDPOINT: &str = "https://storage.googleapis.com";

// ---- object-path mapping (DESIGN §10.2 subtree preservation) -------------------------------------

/// The deterministic object name for `relpath` under `prefix`: forward-slash join with traversal/empty
/// segments dropped (identical algorithm to `s3::object_key`/`azure::blob_path`/`sftp::remote_path`,
/// duplicated here so this backend stays self-contained under its own feature gate).
pub fn object_path(prefix: &str, relpath: &str) -> String {
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

// ---- chunk planning (DESIGN §11.1-equivalent) -----------------------------------------------------

/// Resolve the configured chunk size (default 8 MiB), rounded **down** to the nearest 256 KiB — GCS's
/// required alignment for every chunk but the final one — with a floor of one alignment unit.
pub fn resolve_chunk_bytes(cfg: Option<u64>) -> u64 {
    let raw = cfg.unwrap_or(DEFAULT_CHUNK_BYTES).max(CHUNK_ALIGN);
    (raw / CHUNK_ALIGN) * CHUNK_ALIGN
}

/// The `[start, end]` inclusive byte range of the next chunk to send, given `committed` bytes already
/// confirmed and `total` object size (identical semantics to `http::chunk_bounds`/`azure::block_bounds`).
/// Panics if `committed >= total` (callers only invoke this while there is work left).
pub fn chunk_bounds(committed: u64, chunk_bytes: u64, total: u64) -> (u64, u64) {
    assert!(committed < total, "chunk_bounds called with no remaining bytes");
    let end = (committed + chunk_bytes.max(1)).min(total) - 1;
    (committed, end)
}

/// The `Content-Range: bytes {start}-{end}/{total}` header value for a chunk `PUT`.
pub fn content_range_header(start: u64, end: u64, total: u64) -> String {
    format!("bytes {start}-{end}/{total}")
}

/// Parse the committed byte count out of a `308 Resume Incomplete` response's `Range: bytes=0-{end}`
/// header (the upper bound is inclusive, so committed = end + 1). Returns `None` for any value that
/// doesn't match GCS's documented shape (a missing/malformed header, or a non-zero-based range — GCS
/// always anchors resumable ranges at 0); [`client::send_chunk`] treats that as "nothing to compare",
/// not as a hard failure (see the module doc's "Resume" section).
pub fn committed_from_range_header(v: &str) -> Option<u64> {
    let rest = v.strip_prefix("bytes=")?;
    let (start, end) = rest.split_once('-')?;
    if start.trim() != "0" {
        return None;
    }
    let end: u64 = end.trim().parse().ok()?;
    Some(end + 1)
}

// ---- checksum codec (DESIGN §11.4-equivalent) -----------------------------------------------------

use base64::Engine as _;

/// Base64 of the 4-byte big-endian CRC32C — the shape GCS's object-metadata `crc32c` field uses.
pub fn crc32c_to_b64(v: u32) -> String {
    base64::engine::general_purpose::STANDARD.encode(v.to_be_bytes())
}

/// Inverse of [`crc32c_to_b64`]: decode a base64 4-byte-BE CRC32C, or `None` if malformed.
pub fn b64_to_crc32c(s: &str) -> Option<u32> {
    let bytes = base64::engine::general_purpose::STANDARD.decode(s).ok()?;
    let arr: [u8; 4] = bytes.try_into().ok()?;
    Some(u32::from_be_bytes(arr))
}

// ---- credentials (DESIGN §11.5-equivalent) --------------------------------------------------------

/// How the GCS client authenticates. `Debug` is hand-written (not derived) so a stray `{:?}` never
/// leaks the access token (mirrors S3's `CredMode` / Azure's `AzureCredMode` / HTTP's `HttpAuth`).
#[derive(Clone, PartialEq, Eq)]
pub enum GcsCredMode {
    /// No `Authorization` header at all (the fake-gcs-server, or an `allUsers`-public bucket).
    Anonymous,
    /// A bearer OAuth2 access token.
    AccessToken(String),
}

impl std::fmt::Debug for GcsCredMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            GcsCredMode::Anonymous => f.write_str("Anonymous"),
            GcsCredMode::AccessToken(_) => f.write_str("AccessToken(***redacted***)"),
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct GcsSecretJson {
    access_token: Option<String>,
}

/// Resolve credentials from the egress config and the (optional) credential service (DESIGN
/// §11.5-equivalent):
///
/// - `credentials: {"$secret":"name"}` → resolve as `{"accessToken":"..."}` JSON — a missing
///   service/secret/shape is **permanent**;
/// - else `egress.accessToken` set → ambient bearer token;
/// - else `egress.anonymous: true` → no `Authorization` header;
/// - else → permanent (no credentials configured; see the module doc for the ADC/service-account
///   follow-up).
pub fn resolve_gcs_credentials(
    cfg: &GcsEgress,
    creds: Option<&dyn CredentialService>,
) -> Result<GcsCredMode> {
    if let Some(v) = &cfg.credentials {
        let name = v.get("$secret").and_then(|s| s.as_str()).ok_or_else(|| {
            ReplError::Permanent(
                "egress.credentials must be {\"$secret\":\"name\"} for the gcs backend".to_string(),
            )
        })?;
        let svc = creds.ok_or_else(|| {
            ReplError::Permanent(format!(
                "egress.credentials references secret '{name}' but no credentials subsystem is configured"
            ))
        })?;
        let json = svc
            .get_json(name)
            .map_err(|e| ReplError::Permanent(format!("resolve secret '{name}': {e}")))?
            .ok_or_else(|| ReplError::Permanent(format!("secret '{name}' not found in the vault")))?;
        let parsed: GcsSecretJson = serde_json::from_value(json).map_err(|e| {
            ReplError::Permanent(format!("secret '{name}' is not a valid gcs credentials JSON: {e}"))
        })?;
        return match parsed.access_token {
            Some(token) => Ok(GcsCredMode::AccessToken(token)),
            None => Err(ReplError::Permanent(format!(
                "secret '{name}' must contain 'accessToken'"
            ))),
        };
    }

    if let Some(token) = &cfg.access_token {
        return Ok(GcsCredMode::AccessToken(token.clone()));
    }
    if cfg.anonymous {
        return Ok(GcsCredMode::Anonymous);
    }

    Err(ReplError::Permanent(
        "gcs egress has no credentials: set accessToken, anonymous:true, or credentials.$secret \
         (service-account/ADC signing is not implemented — see dest/gcs/mod.rs's module doc)"
            .to_string(),
    ))
}

// ---- resume token (DESIGN §10.2) ------------------------------------------------------------------

/// The resumable-session resume checkpoint, serialized under the `"gcs"` key of [`ResumeState::token`].
/// Unlike `azure`/`http`'s tokens, this also carries the **session URI** itself — GCS's server-side
/// resume handle — alongside the locally-tracked committed-byte count that resume trusts directly (see
/// the module doc's "Resume" section for why this backend does not query the server to reconcile it).
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GcsResumeToken {
    pub bucket: String,
    pub object: String,
    pub session_uri: String,
    pub size: u64,
    pub mtime_ms: i64,
    #[serde(default)]
    pub bytes_committed: u64,
}

impl GcsResumeToken {
    pub fn from_resume(r: &ResumeState) -> Option<GcsResumeToken> {
        r.token
            .get("gcs")
            .and_then(|v| serde_json::from_value(v.clone()).ok())
    }

    pub fn to_resume(&self) -> ResumeState {
        ResumeState {
            bytes_committed: self.bytes_committed,
            token: serde_json::json!({
                "gcs": serde_json::to_value(self).unwrap_or(serde_json::Value::Null)
            }),
        }
    }

    /// Whether this resume token still matches the current bucket/object AND the current source —
    /// `size` + `mtime_ms` (a grown/shrunk or same-length in-place-rewritten source invalidates the
    /// session), mirroring `azure::AzureResumeToken::matches`/`http::HttpResumeToken::matches`.
    pub fn matches(&self, bucket: &str, object: &str, size: u64, mtime_ms: i64) -> bool {
        self.bucket == bucket && self.object == object && self.size == size && self.mtime_ms == mtime_ms
    }
}

// ---- delivery handle --------------------------------------------------------------------------------

fn algo_name(algo: Algorithm) -> &'static str {
    match algo {
        Algorithm::Crc32c => "CRC32C",
        Algorithm::Sha256 => "SHA256",
    }
}

pub fn build_handle(bucket: &str, object: &str, algo: Algorithm) -> serde_json::Value {
    serde_json::json!({
        "bucket": bucket,
        "object": object,
        "algo": algo_name(algo),
    })
}

/// Extract `(bucket, object)` from a delivery handle, or `None` if either field is absent.
pub fn handle_object(handle: &serde_json::Value) -> Option<(String, String)> {
    let bucket = handle.get("bucket").and_then(|v| v.as_str())?.to_string();
    let object = handle.get("object").and_then(|v| v.as_str())?.to_string();
    Some((bucket, object))
}

// ---- error classification (DESIGN §13.4) -----------------------------------------------------------

/// Classify a GCS JSON-API failure into the retry engine's [`ReplError`] taxonomy. `status` is the
/// HTTP status code when a response came back; `transport` is true for connect/TLS/timeout/dispatch
/// failures (no response at all). Mirrors `http::classify_http`/`azure::classify_azure`'s status-band
/// fallback: `5xx`/`429`/`408` retried, other `4xx` fail fast, anything unrecognized is retried
/// defensively.
pub fn classify_gcs(status: Option<u16>, transport: bool) -> ReplError {
    if transport {
        return ReplError::Transient(format!(
            "gcs transport failure{}",
            status.map(|s| format!(" ({s})")).unwrap_or_default()
        ));
    }
    match status {
        Some(s) if s >= 500 || s == 429 || s == 408 => ReplError::Transient(format!("gcs status {s}")),
        Some(s) if (400..500).contains(&s) => ReplError::Permanent(format!("gcs status {s}")),
        Some(s) => ReplError::Transient(format!("gcs status {s} (unclassified)")),
        None => ReplError::Transient("gcs error (unclassified)".to_string()),
    }
}

// ---- local source reads (pure, no network) ---------------------------------------------------------

/// Read `len` bytes from `path` starting at `offset` into memory, throttling every chunk through the
/// bandwidth governor and reporting the **delta** of each chunk via `on_progress` (mirrors
/// `dest/s3::RangeBody`/`dest/azure::read_source_range`). The in-memory buffer is bounded by `len`
/// (≤ the resolved chunk size).
pub async fn read_source_range(
    path: &Path,
    offset: u64,
    len: u64,
    bw: &crate::ratelimit::Bandwidth,
    on_progress: &(dyn Fn(u64) + Send + Sync),
) -> Result<Vec<u8>> {
    use tokio::io::{AsyncReadExt, AsyncSeekExt};

    let mut file = tokio::fs::File::open(path).await.map_err(ReplError::classify_io)?;
    if offset > 0 {
        file.seek(std::io::SeekFrom::Start(offset))
            .await
            .map_err(ReplError::classify_io)?;
    }
    let mut buf = Vec::with_capacity(len.min(64 * MIB) as usize);
    let mut remaining = len;
    let mut chunk = vec![0u8; READ_CHUNK];
    while remaining > 0 {
        let want = remaining.min(READ_CHUNK as u64) as usize;
        let n = file
            .read(&mut chunk[..want])
            .await
            .map_err(ReplError::classify_io)?;
        if n == 0 {
            // EOF before `len`: the source shrank/was truncated since `deliver` planned the transfer.
            // Fail Transient so the next attempt re-reads metadata and re-plans against the current
            // size, rather than shipping (and letting the worker complete) a truncated object.
            return Err(ReplError::Transient(format!(
                "source truncated: read {} of {len} bytes at offset {offset} from {}",
                len - remaining,
                path.display()
            )));
        }
        bw.throttle(n as u64).await;
        buf.extend_from_slice(&chunk[..n]);
        remaining -= n as u64;
        on_progress(n as u64);
    }
    Ok(buf)
}

/// Fold the already-committed `[0, len)` prefix of the local source into `hasher` — a plain local
/// re-read, no network and no bandwidth throttle (those bytes are not being re-sent), so a resumed
/// transfer's returned [`Checksum`](crate::domain::Checksum) still covers the whole file (mirrors
/// `dest/azure::hash_local_prefix`/`dest/http`'s equivalent).
pub async fn hash_local_prefix(path: &Path, len: u64, hasher: &mut Hasher) -> Result<()> {
    use tokio::io::AsyncReadExt;

    let mut f = tokio::fs::File::open(path).await.map_err(ReplError::classify_io)?;
    let mut remaining = len;
    let mut buf = vec![0u8; READ_CHUNK];
    while remaining > 0 {
        let want = remaining.min(READ_CHUNK as u64) as usize;
        let n = f
            .read(&mut buf[..want])
            .await
            .map_err(ReplError::classify_io)?;
        if n == 0 {
            break;
        }
        hasher.update(&buf[..n]);
        remaining -= n as u64;
    }
    Ok(())
}

// ---- the destination ----------------------------------------------------------------------------

/// The GCS backend. Holds the resolved, immutable transfer parameters; the `reqwest::Client` is built
/// lazily on first use ([`OnceCell`]) so construction stays synchronous (mirrors `HttpDest`/`AzureDest`).
///
/// [`OnceCell`]: tokio::sync::OnceCell
pub struct GcsDest {
    pub(crate) bucket: String,
    pub(crate) prefix: String,
    pub(crate) endpoint: String,
    pub(crate) creds: GcsCredMode,
    pub(crate) chunk_bytes: u64,
    pub(crate) algo: Algorithm,
    pub(crate) store: Option<Arc<dyn StateStore>>,
    pub(crate) client: tokio::sync::OnceCell<reqwest::Client>,
}

impl GcsDest {
    /// Build the backend from its typed egress config and the shared [`DestDeps`] (credentials +
    /// store). Resolves the checksum algorithm, credential mode, chunk size, and endpoint eagerly (all
    /// pure/synchronous, including a fail-fast endpoint-shape check); the network client is deferred to
    /// first transfer.
    pub fn build(cfg: &GcsEgress, deps: &DestDeps) -> Result<Self> {
        let algo = match &cfg.checksum_algorithm {
            Some(name) => Algorithm::from_name(name).ok_or_else(|| {
                ReplError::Permanent(format!("unknown egress.checksumAlgorithm '{name}'"))
            })?,
            None => Algorithm::Crc32c,
        };
        let creds = resolve_gcs_credentials(cfg, deps.credentials.as_deref())?;
        let endpoint = cfg
            .endpoint_url
            .clone()
            .unwrap_or_else(|| DEFAULT_ENDPOINT.to_string());
        // Fail fast on a malformed endpoint rather than surfacing it on first transfer.
        let parsed_endpoint = reqwest::Url::parse(&endpoint)
            .map_err(|e| ReplError::Permanent(format!("invalid egress.endpointUrl '{endpoint}': {e}")))?;
        // NFR-7 (TLS by default): an OAuth2 access token sent to a plaintext `http://` endpoint travels
        // UNENCRYPTED. Warn (rather than reject — the fake-gcs-server simulator legitimately uses http
        // with `anonymous`, which stays silent) so a misconfigured production endpoint is visible.
        if matches!(creds, GcsCredMode::AccessToken(_)) && parsed_endpoint.scheme() == "http" {
            tracing::warn!(
                endpoint = %endpoint,
                "egress.endpointUrl uses plaintext http:// with an access token — the token will be \
                 sent UNENCRYPTED; use https:// (NFR-7)"
            );
        }
        Ok(GcsDest {
            bucket: cfg.bucket.clone(),
            prefix: cfg.prefix.clone(),
            endpoint,
            creds,
            chunk_bytes: resolve_chunk_bytes(cfg.chunk_bytes),
            algo,
            store: deps.store.clone(),
            client: tokio::sync::OnceCell::new(),
        })
    }
}

// The lazy client builder and the `impl Destination for GcsDest` delegation glue live in [`client`]
// — every line makes (or immediately gates) a network round-trip, so they belong on the coverage-
// excluded side of the NFR-2 seam rather than eroding this module's counted coverage.

#[cfg(test)]
mod tests {
    use super::*;
    // The `Destination` trait impl lives in `client`; bring the trait into scope so the construction
    // tests can call `kind()`/`supports_resume()`.
    use crate::dest::Destination;

    // ---- object_path -----------------------------------------------------------------------------

    #[test]
    fn object_path_joins_prefix_and_relpath() {
        assert_eq!(object_path("p", "a/b.txt"), "p/a/b.txt");
        assert_eq!(object_path("", "a/b.txt"), "a/b.txt");
        assert_eq!(object_path("data/out", "deep/nested/r.csv"), "data/out/deep/nested/r.csv");
    }

    #[test]
    fn object_path_normalizes_slashes_and_strips_traversal() {
        assert_eq!(object_path("/p/", "a/b.txt"), "p/a/b.txt", "leading+trailing slash trimmed");
        assert_eq!(object_path("p", "a/../b/./c.txt"), "p/a/b/c.txt", "traversal + . segments dropped");
        assert!(!object_path("p", "../../etc/passwd").contains(".."), "no traversal escapes the prefix");
        assert_eq!(object_path("p", "//a//b//"), "p/a/b", "empty segments dropped");
        assert_eq!(object_path("p", ""), "p", "empty relpath → prefix only");
    }

    // ---- chunk planning ---------------------------------------------------------------------------

    #[test]
    fn resolve_chunk_bytes_default() {
        assert_eq!(resolve_chunk_bytes(None), 8 * MIB);
    }

    #[test]
    fn resolve_chunk_bytes_rounds_down_to_256kib_alignment() {
        assert_eq!(resolve_chunk_bytes(Some(1_000_000)), 3 * CHUNK_ALIGN); // 786432
        assert_eq!(resolve_chunk_bytes(Some(CHUNK_ALIGN)), CHUNK_ALIGN);
        assert_eq!(resolve_chunk_bytes(Some(CHUNK_ALIGN + 1)), CHUNK_ALIGN);
    }

    #[test]
    fn resolve_chunk_bytes_floors_at_one_alignment_unit() {
        assert_eq!(resolve_chunk_bytes(Some(0)), CHUNK_ALIGN);
        assert_eq!(resolve_chunk_bytes(Some(1)), CHUNK_ALIGN);
    }

    #[test]
    fn chunk_bounds_middle_and_last_chunk() {
        assert_eq!(chunk_bounds(0, 10, 25), (0, 9));
        assert_eq!(chunk_bounds(10, 10, 25), (10, 19));
        assert_eq!(chunk_bounds(20, 10, 25), (20, 24), "last chunk carries the remainder");
    }

    #[test]
    #[should_panic(expected = "no remaining bytes")]
    fn chunk_bounds_panics_when_nothing_left() {
        chunk_bounds(25, 10, 25);
    }

    #[test]
    fn content_range_header_format() {
        assert_eq!(content_range_header(0, 9, 100), "bytes 0-9/100");
    }

    // ---- Range-header parsing ----------------------------------------------------------------------

    #[test]
    fn committed_from_range_header_parses_upper_bound_inclusive() {
        assert_eq!(committed_from_range_header("bytes=0-1234"), Some(1235));
        assert_eq!(committed_from_range_header("bytes=0-0"), Some(1));
    }

    #[test]
    fn committed_from_range_header_rejects_malformed_or_non_zero_start() {
        assert_eq!(committed_from_range_header(""), None);
        assert_eq!(committed_from_range_header("bytes=100-200"), None, "must be zero-based");
        assert_eq!(committed_from_range_header("bytes=0-abc"), None);
        assert_eq!(committed_from_range_header("nonsense"), None);
    }

    // ---- checksum codec -----------------------------------------------------------------------------

    #[test]
    fn crc32c_b64_round_trips() {
        let v = 0xE306_9283u32;
        let b64 = crc32c_to_b64(v);
        assert_eq!(b64_to_crc32c(&b64), Some(v));
    }

    #[test]
    fn b64_to_crc32c_rejects_malformed() {
        assert_eq!(b64_to_crc32c("not base64!!"), None);
        assert_eq!(b64_to_crc32c(""), None);
        // Valid base64 but wrong decoded length (not 4 bytes).
        assert_eq!(b64_to_crc32c("QQ=="), None);
    }

    // ---- credentials ----------------------------------------------------------------------------

    /// A fake credential service returning canned JSON for one secret name (mirrors S3's/HTTP's/
    /// Azure's fakes — the opaque `Secret` type has no public constructor outside the ggcommons
    /// credentials crate).
    struct FakeCreds {
        name: String,
        json: Option<serde_json::Value>,
    }
    impl CredentialService for FakeCreds {
        fn get(&self, _n: &str) -> ggcommons::Result<Option<ggcommons::credentials::Secret>> {
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
        fn list(&self, _p: &str) -> ggcommons::Result<Vec<ggcommons::credentials::SecretMeta>> {
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
            Ok("v1".into())
        }
        fn delete(&self, _n: &str) -> ggcommons::Result<bool> {
            Ok(false)
        }
        fn get_json(&self, name: &str) -> ggcommons::Result<Option<serde_json::Value>> {
            if name == self.name {
                Ok(self.json.clone())
            } else {
                Ok(None)
            }
        }
    }

    fn base_cfg() -> GcsEgress {
        GcsEgress {
            bucket: "b".into(),
            prefix: String::new(),
            endpoint_url: None,
            access_token: None,
            anonymous: false,
            credentials: None,
            chunk_bytes: None,
            checksum_algorithm: None,
        }
    }

    #[test]
    fn resolve_credentials_ambient_access_token() {
        let mut cfg = base_cfg();
        cfg.access_token = Some("tok".into());
        assert_eq!(
            resolve_gcs_credentials(&cfg, None).unwrap(),
            GcsCredMode::AccessToken("tok".into())
        );
    }

    #[test]
    fn resolve_credentials_anonymous() {
        let mut cfg = base_cfg();
        cfg.anonymous = true;
        assert_eq!(resolve_gcs_credentials(&cfg, None).unwrap(), GcsCredMode::Anonymous);
    }

    #[test]
    fn resolve_credentials_access_token_takes_precedence_over_anonymous() {
        let mut cfg = base_cfg();
        cfg.access_token = Some("tok".into());
        cfg.anonymous = true;
        assert_eq!(
            resolve_gcs_credentials(&cfg, None).unwrap(),
            GcsCredMode::AccessToken("tok".into())
        );
    }

    #[test]
    fn resolve_credentials_none_configured_is_permanent() {
        assert!(resolve_gcs_credentials(&base_cfg(), None).unwrap_err().is_permanent());
    }

    #[test]
    fn resolve_credentials_secret_access_token() {
        let creds = FakeCreds {
            name: "gcs-creds".into(),
            json: Some(serde_json::json!({ "accessToken": "vault-tok" })),
        };
        let mut cfg = base_cfg();
        cfg.credentials = Some(serde_json::json!({ "$secret": "gcs-creds" }));
        assert_eq!(
            resolve_gcs_credentials(&cfg, Some(&creds)).unwrap(),
            GcsCredMode::AccessToken("vault-tok".into())
        );
    }

    #[test]
    fn resolve_credentials_secret_without_service_is_permanent() {
        let mut cfg = base_cfg();
        cfg.credentials = Some(serde_json::json!({ "$secret": "gcs-creds" }));
        assert!(resolve_gcs_credentials(&cfg, None).unwrap_err().is_permanent());
    }

    #[test]
    fn resolve_credentials_unknown_secret_is_permanent() {
        let creds = FakeCreds {
            name: "other".into(),
            json: Some(serde_json::json!({ "accessToken": "x" })),
        };
        let mut cfg = base_cfg();
        cfg.credentials = Some(serde_json::json!({ "$secret": "missing" }));
        assert!(resolve_gcs_credentials(&cfg, Some(&creds)).unwrap_err().is_permanent());
    }

    #[test]
    fn resolve_credentials_secret_missing_field_is_permanent() {
        let creds = FakeCreds {
            name: "gcs-creds".into(),
            json: Some(serde_json::json!({ "somethingElse": true })),
        };
        let mut cfg = base_cfg();
        cfg.credentials = Some(serde_json::json!({ "$secret": "gcs-creds" }));
        assert!(resolve_gcs_credentials(&cfg, Some(&creds)).unwrap_err().is_permanent());
    }

    #[test]
    fn resolve_credentials_secret_malformed_shape_is_permanent() {
        let mut cfg = base_cfg();
        cfg.credentials = Some(serde_json::json!({ "accessToken": 5 })); // wrong type
        assert!(resolve_gcs_credentials(&cfg, None).unwrap_err().is_permanent());
    }

    #[test]
    fn gcs_cred_mode_debug_redacts_the_token() {
        let mode = GcsCredMode::AccessToken("TOP_SECRET_TOKEN".into());
        let dbg = format!("{mode:?}");
        assert!(!dbg.contains("TOP_SECRET_TOKEN"), "access token leaked: {dbg}");
        assert!(dbg.contains("***redacted***"));
        assert_eq!(format!("{:?}", GcsCredMode::Anonymous), "Anonymous");
    }

    // ---- resume token --------------------------------------------------------------------------

    #[test]
    fn gcs_resume_token_round_trips_via_resume_state() {
        let tok = GcsResumeToken {
            bucket: "b".into(),
            object: "in/a.bin".into(),
            session_uri: "http://host/upload/session/abc".into(),
            size: 999,
            mtime_ms: 12345,
            bytes_committed: 500,
        };
        let rs = tok.to_resume();
        assert_eq!(rs.bytes_committed, 500);
        let json = serde_json::to_string(&rs).unwrap();
        let back: ResumeState = serde_json::from_str(&json).unwrap();
        let tok2 = GcsResumeToken::from_resume(&back).expect("token present");
        assert_eq!(tok2, tok);
    }

    #[test]
    fn gcs_resume_token_absent_or_wrong_shape_is_none() {
        assert!(GcsResumeToken::from_resume(&ResumeState::default()).is_none());
        let azure_shape = ResumeState {
            bytes_committed: 1,
            token: serde_json::json!({ "azure": { "blob": "x" } }),
        };
        assert!(GcsResumeToken::from_resume(&azure_shape).is_none());
    }

    #[test]
    fn gcs_resume_token_matches_guards_bucket_object_size_and_mtime() {
        let tok = GcsResumeToken {
            bucket: "b".into(),
            object: "in/a.bin".into(),
            session_uri: "http://host/s".into(),
            size: 100,
            mtime_ms: 42,
            bytes_committed: 50,
        };
        assert!(tok.matches("b", "in/a.bin", 100, 42));
        assert!(!tok.matches("other", "in/a.bin", 100, 42), "bucket change → discard resume");
        assert!(!tok.matches("b", "in/other.bin", 100, 42), "object change → discard resume");
        assert!(!tok.matches("b", "in/a.bin", 200, 42), "size change → discard resume");
        assert!(!tok.matches("b", "in/a.bin", 100, 99), "mtime change → discard resume");
    }

    // ---- delivery handle ------------------------------------------------------------------------

    #[test]
    fn handle_round_trips_bucket_and_object() {
        let h = build_handle("b", "in/a.bin", Algorithm::Crc32c);
        assert_eq!(handle_object(&h), Some(("b".to_string(), "in/a.bin".to_string())));
        assert_eq!(h.get("algo").and_then(|v| v.as_str()), Some("CRC32C"));
    }

    #[test]
    fn handle_object_absent_is_none() {
        assert_eq!(handle_object(&serde_json::json!({})), None);
        assert_eq!(handle_object(&serde_json::json!({ "bucket": "b" })), None);
    }

    // ---- error classification -------------------------------------------------------------------

    #[test]
    fn classify_transport_is_transient() {
        assert!(classify_gcs(None, true).is_transient());
        assert!(classify_gcs(Some(401), true).is_transient(), "transport wins over a 4xx code");
    }

    #[test]
    fn classify_5xx_and_throttle_codes_are_transient() {
        for c in [500, 502, 503, 504, 429, 408] {
            assert!(classify_gcs(Some(c), false).is_transient(), "{c} should be transient");
        }
    }

    #[test]
    fn classify_other_4xx_is_permanent() {
        for c in [400, 401, 403, 404, 409, 412] {
            assert!(classify_gcs(Some(c), false).is_permanent(), "{c} should be permanent");
        }
    }

    #[test]
    fn classify_unclassified_status_and_bare_failure_are_transient() {
        assert!(classify_gcs(Some(302), false).is_transient());
        assert!(classify_gcs(None, false).is_transient());
    }

    // ---- read_source_range / hash_local_prefix (local I/O only, no network) ----------------------

    #[tokio::test]
    async fn read_source_range_reads_whole_file_and_reports_progress() {
        use std::sync::atomic::{AtomicU64, Ordering};
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("f.bin");
        let data = vec![7u8; READ_CHUNK * 2 + 100];
        std::fs::write(&path, &data).unwrap();

        let seen = Arc::new(AtomicU64::new(0));
        let s = seen.clone();
        let buf = read_source_range(
            &path,
            0,
            data.len() as u64,
            &crate::ratelimit::Bandwidth::unlimited(),
            &move |d| {
                s.fetch_add(d, Ordering::SeqCst);
            },
        )
        .await
        .unwrap();
        assert_eq!(buf, data);
        assert_eq!(seen.load(Ordering::SeqCst), data.len() as u64);
    }

    #[tokio::test]
    async fn read_source_range_reads_an_offset_slice() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("f.bin");
        let data: Vec<u8> = (0..30_000u32).map(|i| (i % 256) as u8).collect();
        std::fs::write(&path, &data).unwrap();

        let buf = read_source_range(
            &path,
            10_000,
            5_000,
            &crate::ratelimit::Bandwidth::unlimited(),
            &|_d| {},
        )
        .await
        .unwrap();
        assert_eq!(buf, &data[10_000..15_000]);
    }

    #[tokio::test]
    async fn read_source_range_short_read_is_transient() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("short.bin");
        std::fs::write(&path, vec![9u8; 1000]).unwrap();
        let err = read_source_range(
            &path,
            0,
            5000,
            &crate::ratelimit::Bandwidth::unlimited(),
            &|_d| {},
        )
        .await
        .unwrap_err();
        assert!(err.is_transient());
        assert!(err.to_string().contains("truncated"));
    }

    #[tokio::test]
    async fn read_source_range_missing_file_is_permanent() {
        let err = read_source_range(
            Path::new("/no/such/file.bin"),
            0,
            10,
            &crate::ratelimit::Bandwidth::unlimited(),
            &|_d| {},
        )
        .await
        .unwrap_err();
        assert!(err.is_permanent());
    }

    #[tokio::test]
    async fn hash_local_prefix_matches_hashing_the_prefix_directly() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("f.bin");
        let data: Vec<u8> = (0..50_000u32).map(|i| (i % 251) as u8).collect();
        std::fs::write(&path, &data).unwrap();

        let mut hasher = Hasher::new(Algorithm::Crc32c);
        hash_local_prefix(&path, 12_345, &mut hasher).await.unwrap();
        let got = hasher.finish();

        let mut direct = &data[..12_345];
        let (_, expect) = crate::integrity::hash_reader(&mut direct, Algorithm::Crc32c).unwrap();
        assert_eq!(got, expect);
    }

    // ---- GcsDest::build --------------------------------------------------------------------------

    fn deps_default() -> DestDeps {
        DestDeps::default()
    }

    #[test]
    fn build_defaults_to_crc32c_and_default_endpoint() {
        let mut cfg = base_cfg();
        cfg.anonymous = true;
        let dest = GcsDest::build(&cfg, &deps_default()).unwrap();
        assert_eq!(dest.kind(), "gcs");
        assert!(dest.supports_resume());
        assert_eq!(dest.algo, Algorithm::Crc32c);
        assert_eq!(dest.endpoint, DEFAULT_ENDPOINT);
        assert_eq!(dest.chunk_bytes, 8 * MIB);
    }

    #[test]
    fn build_reads_sha256_endpoint_and_chunk_overrides() {
        let mut cfg = base_cfg();
        cfg.anonymous = true;
        cfg.checksum_algorithm = Some("SHA-256".into());
        cfg.endpoint_url = Some("http://127.0.0.1:4443".into());
        cfg.chunk_bytes = Some(512 * 1024);
        let dest = GcsDest::build(&cfg, &deps_default()).unwrap();
        assert_eq!(dest.algo, Algorithm::Sha256);
        assert_eq!(dest.endpoint, "http://127.0.0.1:4443");
        assert_eq!(dest.chunk_bytes, 512 * 1024);
    }

    #[test]
    fn build_rejects_unknown_checksum_algorithm() {
        let mut cfg = base_cfg();
        cfg.anonymous = true;
        cfg.checksum_algorithm = Some("MD5".into());
        assert!(GcsDest::build(&cfg, &deps_default())
            .err()
            .expect("unknown algorithm rejected")
            .is_permanent());
    }

    #[test]
    fn build_rejects_malformed_endpoint() {
        let mut cfg = base_cfg();
        cfg.anonymous = true;
        cfg.endpoint_url = Some("not a url".into());
        assert!(GcsDest::build(&cfg, &deps_default())
            .err()
            .expect("malformed endpoint rejected")
            .is_permanent());
    }

    #[test]
    fn build_allows_but_warns_on_plaintext_http_endpoint_with_access_token() {
        // NFR-7: an access token over a plaintext http:// endpoint is a misconfiguration — build still
        // succeeds (warn only). anonymous+http (the fake-gcs-server case) stays silent.
        let mut cfg = base_cfg();
        cfg.access_token = Some("tok".into());
        cfg.endpoint_url = Some("http://127.0.0.1:4443".into());
        let dest = GcsDest::build(&cfg, &deps_default()).expect("http+token still builds");
        assert_eq!(dest.creds, GcsCredMode::AccessToken("tok".into()));
        // https:// endpoint with a token is correct — still builds, no warn path.
        let mut cfg2 = base_cfg();
        cfg2.access_token = Some("tok".into());
        cfg2.endpoint_url = Some("https://storage.example.com".into());
        GcsDest::build(&cfg2, &deps_default()).expect("https+token builds");
    }

    #[test]
    fn build_propagates_credential_resolution_failure() {
        // No accessToken/anonymous/$secret configured → permanent.
        assert!(GcsDest::build(&base_cfg(), &deps_default())
            .err()
            .expect("missing credentials rejected")
            .is_permanent());
    }
}
