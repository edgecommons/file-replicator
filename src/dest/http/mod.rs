//! # file-replicator — HTTP(S) destination (DESIGN §10.2/§10.3; P5)
//!
//! Generic HTTP(S) ingest: `PUT` (default, resumable) or `POST` (single-shot) to `egress.url` with
//! `item.relpath` joined onto its path, static + auth headers, and a local re-hash checksum (generic
//! HTTP has no universal content-checksum echo comparable to S3's flexible checksums).
//!
//! ## Resume (DESIGN §10.2 "resume via `Content-Range`/resumable PUT where supported")
//! Unlike S3 (a stateful multipart-upload id) or SFTP/FTPS (offset seek/append on one connection),
//! plain HTTP has no universal "append" verb. This backend implements resume as a sequence of
//! **ranged `PUT`s**: the source is split into `egress.chunkBytes`-sized segments (default 8 MiB) and
//! each is sent as its own request carrying `Content-Range: bytes {start}-{end}/{total}`, so a
//! receiving endpoint that honors `Content-Range` (writing each segment at its byte offset) ends up
//! with the same object regardless of how the segments were spread across attempts. After each
//! segment succeeds, `{url, size, mtimeMs, bytesCommitted}` is persisted into the shared
//! [`StateStore`] resume row (keyed by `(instance, relpath, "http")`) — write-ahead so a crash resumes
//! only the missing tail. A token is reused only when `url`/`size`/`mtime` still match
//! ([`HttpResumeToken::matches`]); otherwise the source changed and a fresh sequence starts at byte 0.
//! The whole-file checksum is kept correct across a resume the same way SFTP/FTPS do it: the
//! already-committed prefix is **re-read from the local source** (no network) and folded into the
//! hasher before streaming the remaining chunks ([`hash_local_prefix`]).
//!
//! On resume the persisted `bytesCommitted` is **reconciled against the server** before it is trusted:
//! `deliver` issues a `HEAD` and clamps `committed` to the object's actual current length (a server
//! that retained fewer bytes than the checkpoint claims), or restarts from byte 0 when the object is
//! gone (`404`) — the same self-correction SFTP/FTPS get by clamping to the remote temp file's size.
//!
//! **No atomic publish.** Unlike every other backend (SFTP/FTPS temp-file + rename; S3/Azure/GCS
//! commit-at-end), plain HTTP has no rename/commit verb, so the ranged `PUT`s write straight to the
//! final URL: an interrupted transfer leaves a *partial* object live at the stable URL until the resume
//! completes it. Combined with the server reconciliation above this is crash-safe under the default
//! `verify: checksum` (and `verify: size`), which re-checks the published object before the source side
//! effect; a deployment therefore **must not** use `verify: none` with this backend (a partial/
//! head-truncated object could otherwise be accepted and the source deleted).
//!
//! `method: POST` has no natural "append to an existing partial resource" semantics, so resume is
//! forced off for it (a fresh single-shot request every attempt) — see [`HttpDest::build`].
//!
//! ## Where the code lives (coverage seam, NFR-2)
//! Everything in **this file** is pure decision logic (URL/path mapping, method/auth/resume
//! bookkeeping, chunk-range math, error classification) plus the streaming *local* file read (no
//! network — mirrors `dest/s3::RangeBody`) — all unit-tested with no network. Every line that opens
//! an HTTP connection or sends a request lives in [`client`], excluded from the coverage gate (mirrors
//! `dest/{s3,sftp,ftps}/{mod.rs,client.rs}`). The in-process integration test (`tests/http_inprocess.rs`)
//! always runs (no simulator container to skip), exercising `client.rs` live end to end.

pub mod client;

use std::path::Path;
use std::sync::Arc;

use serde::{Deserialize, Serialize};

use crate::config::HttpEgress;
use crate::domain::ResumeState;
use crate::error::{ReplError, Result};
use crate::integrity::{Algorithm, Hasher};
use crate::state::StateStore;

use super::DestDeps;

use edgecommons::credentials::CredentialService;

const DEFAULT_CHUNK_BYTES: u64 = 8 * 1024 * 1024;
/// Streaming read window for local source reads (matches `dest/s3::READ_CHUNK` / `integrity.rs`).
const READ_CHUNK: usize = 64 * 1024;

// ---- URL mapping (DESIGN §10.2 subtree preservation) ---------------------------------------------

/// The deterministic target URL for `relpath` joined onto `base`'s path (traversal/empty segments
/// dropped, same algorithm as `s3::object_key`/`sftp::remote_path`/`ftps::remote_path`, duplicated
/// here so this backend stays self-contained under its own feature gate). `base` must be an absolute
/// URL with a scheme+host (`cannot-be-a-base` URLs like `mailto:` are rejected).
pub fn build_url(base: &str, relpath: &str) -> Result<reqwest::Url> {
    let mut url = reqwest::Url::parse(base)
        .map_err(|e| ReplError::Permanent(format!("invalid egress.url '{base}': {e}")))?;
    let rel: Vec<&str> = relpath
        .split('/')
        .filter(|s| !s.is_empty() && *s != "." && *s != "..")
        .collect();
    {
        let mut segs = url.path_segments_mut().map_err(|_| {
            ReplError::Permanent(format!(
                "egress.url '{base}' cannot be a base for a relative path (missing scheme/host?)"
            ))
        })?;
        // Drop a trailing empty segment (the `.../upload/` case) before pushing, so the join never
        // produces a doubled slash regardless of whether `base` ends with `/`.
        segs.pop_if_empty();
        for part in &rel {
            segs.push(part);
        }
    }
    Ok(url)
}

// ---- method ----------------------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HttpMethod {
    Put,
    Post,
}

impl HttpMethod {
    /// Parse `egress.method` (case-insensitive): absent → `PUT` (the resumable default); `PUT`/`POST`;
    /// anything else is a permanent config error.
    pub fn parse(name: Option<&str>) -> Result<Self> {
        match name {
            None => Ok(HttpMethod::Put),
            Some(s) if s.eq_ignore_ascii_case("PUT") => Ok(HttpMethod::Put),
            Some(s) if s.eq_ignore_ascii_case("POST") => Ok(HttpMethod::Post),
            Some(other) => Err(ReplError::Permanent(format!(
                "unsupported egress.method '{other}' (expected PUT or POST)"
            ))),
        }
    }
}

// ---- credentials (DESIGN §11.5-equivalent) --------------------------------------------------------

/// HTTP authentication. `Debug` is hand-written (not derived) so a stray `{:?}` never leaks the
/// bearer token / password (mirrors S3's `CredMode` / SFTP's `SftpAuth` / FTPS's `FtpsAuth`).
#[derive(Clone, PartialEq, Eq)]
pub enum HttpAuth {
    /// No `Authorization` header (a plain/unauthenticated endpoint, or auth baked into `egress.url`
    /// itself — e.g. a presigned/pre-tokenized URL — or into `egress.headers`).
    None,
    Bearer(String),
    Basic {
        username: String,
        password: String,
    },
}

impl std::fmt::Debug for HttpAuth {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            HttpAuth::None => f.write_str("None"),
            HttpAuth::Bearer(_) => f.write_str("Bearer(***redacted***)"),
            HttpAuth::Basic { username, .. } => f
                .debug_struct("Basic")
                .field("username", username)
                .field("password", &"***redacted***")
                .finish(),
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct HttpSecretJson {
    bearer_token: Option<String>,
    username: Option<String>,
    password: Option<String>,
}

/// Resolve auth from the egress config and the (optional) credential service (DESIGN §11.5-equivalent):
///
/// - `credentials: {"$secret":"name"}` → resolve as `{bearerToken}` or `{username?,password}` JSON
///   (`username` falls back to `egress.username` when absent) — a missing service/secret/shape, or a
///   secret with neither field, is **permanent**;
/// - else `egress.bearerToken` set → `Bearer`;
/// - else `egress.password` set → `Basic` (`egress.username` required);
/// - else → `None` (many HTTP ingest endpoints need no `Authorization` header at all).
pub fn resolve_http_auth(
    cfg: &HttpEgress,
    creds: Option<&dyn CredentialService>,
) -> Result<HttpAuth> {
    if let Some(v) = &cfg.credentials {
        let name = v.get("$secret").and_then(|s| s.as_str()).ok_or_else(|| {
            ReplError::Permanent(
                "egress.credentials must be {\"$secret\":\"name\"} for the http backend"
                    .to_string(),
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
            .ok_or_else(|| {
                ReplError::Permanent(format!("secret '{name}' not found in the vault"))
            })?;
        let parsed: HttpSecretJson = serde_json::from_value(json).map_err(|e| {
            ReplError::Permanent(format!(
                "secret '{name}' is not a valid http credentials JSON: {e}"
            ))
        })?;
        if let Some(token) = parsed.bearer_token {
            return Ok(HttpAuth::Bearer(token));
        }
        if let Some(password) = parsed.password {
            let username = parsed
                .username
                .or_else(|| cfg.username.clone())
                .ok_or_else(|| {
                    ReplError::Permanent(format!(
                        "secret '{name}' has no 'username' and egress.username is not set"
                    ))
                })?;
            return Ok(HttpAuth::Basic { username, password });
        }
        return Err(ReplError::Permanent(format!(
            "secret '{name}' must contain 'bearerToken' or 'username'+'password'"
        )));
    }

    if let Some(token) = &cfg.bearer_token {
        return Ok(HttpAuth::Bearer(token.clone()));
    }

    if let Some(password) = &cfg.password {
        let username = cfg.username.clone().ok_or_else(|| {
            ReplError::Permanent(
                "egress.username is required when egress.password is set".to_string(),
            )
        })?;
        return Ok(HttpAuth::Basic {
            username,
            password: password.clone(),
        });
    }

    Ok(HttpAuth::None)
}

// ---- chunk-range math (DESIGN §10.2 resume) ---------------------------------------------------

/// The `[start, end]` inclusive byte range (1-based-exclusive-end style S3/HTTP ranges use) of the
/// next chunk to send, given `committed` bytes already confirmed and `total` object size. Panics if
/// `committed >= total` (callers only invoke this while there is work left — DESIGN §10.2).
pub fn chunk_bounds(committed: u64, chunk_bytes: u64, total: u64) -> (u64, u64) {
    assert!(
        committed < total,
        "chunk_bounds called with no remaining bytes"
    );
    let end = (committed + chunk_bytes.max(1)).min(total) - 1;
    (committed, end)
}

/// The `Content-Range: bytes {start}-{end}/{total}` header value for a ranged `PUT` chunk.
pub fn content_range_header(start: u64, end: u64, total: u64) -> String {
    format!("bytes {start}-{end}/{total}")
}

// ---- resume token (DESIGN §10.2) ----------------------------------------------------------------

/// The ranged-PUT resume checkpoint, serialized under the `"http"` key of [`ResumeState::token`] —
/// same shape/semantics as `sftp::SftpResumeToken`/`ftps::FtpsResumeToken` (offset instead of a
/// seek/append position), keyed by target URL instead of a remote path.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct HttpResumeToken {
    pub url: String,
    pub size: u64,
    pub mtime_ms: i64,
    #[serde(default)]
    pub bytes_committed: u64,
}

impl HttpResumeToken {
    pub fn from_resume(r: &ResumeState) -> Option<HttpResumeToken> {
        r.token
            .get("http")
            .and_then(|v| serde_json::from_value(v.clone()).ok())
    }

    pub fn to_resume(&self) -> ResumeState {
        ResumeState {
            bytes_committed: self.bytes_committed,
            token: serde_json::json!({
                "http": serde_json::to_value(self).unwrap_or(serde_json::Value::Null)
            }),
        }
    }

    pub fn matches(&self, url: &str, size: u64, mtime_ms: i64) -> bool {
        self.url == url && self.size == size && self.mtime_ms == mtime_ms
    }
}

// ---- delivery handle --------------------------------------------------------------------------------

fn algo_name(algo: Algorithm) -> &'static str {
    match algo {
        Algorithm::Crc32c => "CRC32C",
        Algorithm::Sha256 => "SHA256",
    }
}

pub fn build_handle(url: &str, algo: Algorithm) -> serde_json::Value {
    serde_json::json!({
        "url": url,
        "algo": algo_name(algo),
    })
}

pub fn handle_url(handle: &serde_json::Value) -> Option<String> {
    handle.get("url").and_then(|v| v.as_str()).map(String::from)
}

// ---- error classification (DESIGN §13.4) -----------------------------------------------------------

/// Classify an HTTP failure into the retry engine's [`ReplError`] taxonomy (DESIGN §13.4). `status` is
/// the HTTP status code when a response came back; `transport` is true for connect/TLS/timeout/
/// dispatch failures (no response at all). Mirrors `s3::classify_s3`'s status-band fallback: `5xx`/
/// `429`/`408` are retried, other `4xx` fail fast, anything unrecognized is retried defensively.
pub fn classify_http(status: Option<u16>, transport: bool) -> ReplError {
    if transport {
        return ReplError::Transient(format!(
            "http transport failure{}",
            status.map(|s| format!(" ({s})")).unwrap_or_default()
        ));
    }
    match status {
        Some(s) if s >= 500 || s == 429 || s == 408 => {
            ReplError::Transient(format!("http status {s}"))
        }
        Some(s) if (400..500).contains(&s) => ReplError::Permanent(format!("http status {s}")),
        Some(s) => ReplError::Transient(format!("http status {s} (unclassified)")),
        None => ReplError::Transient("http error (unclassified)".to_string()),
    }
}

// ---- local source reads (pure, no network) ---------------------------------------------------------

/// Read `len` bytes from `path` starting at `offset` into memory, throttling every chunk through the
/// bandwidth governor and reporting the **delta** of each chunk via `on_progress`. This is the local
/// half of a chunk send (no network — mirrors `dest/s3::RangeBody`, which likewise streams bytes
/// without hashing, since the caller folds the bytes into a whole-file [`Hasher`] that must also see the resumed prefix, see
/// [`hash_local_prefix`]). The in-memory buffer is bounded by `len` (≤ `egress.chunkBytes`).
pub async fn read_source_range(
    path: &Path,
    offset: u64,
    len: u64,
    bw: &crate::ratelimit::Bandwidth,
    on_progress: &(dyn Fn(u64) + Send + Sync),
) -> Result<Vec<u8>> {
    use tokio::io::{AsyncReadExt, AsyncSeekExt};

    let mut file = tokio::fs::File::open(path)
        .await
        .map_err(ReplError::classify_io)?;
    if offset > 0 {
        file.seek(std::io::SeekFrom::Start(offset))
            .await
            .map_err(ReplError::classify_io)?;
    }
    let mut buf = Vec::with_capacity(len.min(64 * 1024 * 1024) as usize);
    let mut remaining = len;
    let mut chunk = vec![0u8; READ_CHUNK];
    while remaining > 0 {
        let want = remaining.min(READ_CHUNK as u64) as usize;
        let n = file
            .read(&mut chunk[..want])
            .await
            .map_err(ReplError::classify_io)?;
        if n == 0 {
            // EOF before `len`: the source shrank/was truncated since `deliver` read its metadata.
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
/// re-read, no network and no bandwidth throttle (those bytes are not being re-transferred), so a
/// resumed transfer's returned [`Checksum`](crate::domain::Checksum) still covers the whole file
/// (mirrors `ftps::hash_prefix_sync`/the SFTP equivalent).
pub async fn hash_local_prefix(path: &Path, len: u64, hasher: &mut Hasher) -> Result<()> {
    use tokio::io::AsyncReadExt;

    let mut f = tokio::fs::File::open(path)
        .await
        .map_err(ReplError::classify_io)?;
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

/// The HTTP(S) backend. Holds the resolved, immutable transfer parameters; the `reqwest::Client` is
/// built lazily on first use ([`OnceCell`]) so construction stays synchronous (mirrors `S3Dest`).
///
/// [`OnceCell`]: tokio::sync::OnceCell
pub struct HttpDest {
    pub(crate) base_url: String,
    pub(crate) method: HttpMethod,
    pub(crate) headers: std::collections::BTreeMap<String, String>,
    pub(crate) auth: HttpAuth,
    /// Whether a `deliver` uses resumable ranged `PUT`s (chunked, checkpointed) vs one whole-body
    /// request. Always `false` when `method` is `Post` (see [`HttpDest::build`]).
    pub(crate) resumable: bool,
    pub(crate) chunk_bytes: u64,
    pub(crate) algo: Algorithm,
    pub(crate) store: Option<Arc<dyn StateStore>>,
    pub(crate) client: tokio::sync::OnceCell<reqwest::Client>,
}

impl HttpDest {
    /// Build the backend from its typed egress config and the shared [`DestDeps`] (credentials +
    /// store). Resolves the checksum algorithm, method, auth mode, and chunk size eagerly (all
    /// pure/synchronous, including a fail-fast URL-shape check); the network client is deferred to
    /// first transfer.
    pub fn build(cfg: &HttpEgress, deps: &DestDeps) -> Result<Self> {
        let algo = match &cfg.checksum_algorithm {
            Some(name) => Algorithm::from_name(name).ok_or_else(|| {
                ReplError::Permanent(format!("unknown egress.checksumAlgorithm '{name}'"))
            })?,
            None => Algorithm::Crc32c,
        };
        let method = HttpMethod::parse(cfg.method.as_deref())?;
        let mut resumable = cfg.resumable.unwrap_or(true);
        if method == HttpMethod::Post && resumable {
            tracing::warn!(
                "egress.resumable is ignored for egress.method \"POST\" (single-shot upload only)"
            );
            resumable = false;
        }
        let chunk_bytes = cfg.chunk_bytes.unwrap_or(DEFAULT_CHUNK_BYTES).max(1);
        let auth = resolve_http_auth(cfg, deps.credentials.as_deref())?;
        // NFR-7 (TLS by default): attaching a Bearer/Basic credential to a plaintext `http://` URL sends
        // the token/password UNENCRYPTED. Warn loudly at build (rather than reject — a trusted
        // localhost/mesh endpoint may legitimately use `http`) so a misconfigured production URL is
        // visible. `http://` with no auth (or auth baked into the URL/headers, e.g. a presigned URL) is
        // fine and stays silent.
        if !matches!(auth, HttpAuth::None)
            && reqwest::Url::parse(&cfg.url)
                .map(|u| u.scheme() == "http")
                .unwrap_or(false)
        {
            tracing::warn!(
                url = %cfg.url,
                "egress.url uses plaintext http:// with credentials attached — the bearer token / \
                 password will be sent UNENCRYPTED; use https:// (NFR-7)"
            );
        }
        // Fail fast on a malformed/non-joinable base URL rather than surfacing it on first transfer.
        build_url(&cfg.url, "")?;
        Ok(HttpDest {
            base_url: cfg.url.clone(),
            method,
            headers: cfg.headers.clone(),
            auth,
            resumable,
            chunk_bytes,
            algo,
            store: deps.store.clone(),
            client: tokio::sync::OnceCell::new(),
        })
    }
}

// The lazy client builder and the `impl Destination for HttpDest` delegation glue live in [`client`]
// — every line makes (or immediately gates) a network round-trip, so they belong on the coverage-
// excluded side of the NFR-2 seam rather than eroding this module's counted coverage.

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dest::DestDeps;

    // ---- build_url --------------------------------------------------------------------------------

    #[test]
    fn build_url_joins_base_and_relpath() {
        assert_eq!(
            build_url("http://host/upload", "a/b.txt").unwrap().as_str(),
            "http://host/upload/a/b.txt"
        );
    }

    #[test]
    fn build_url_handles_trailing_slash_without_doubling() {
        assert_eq!(
            build_url("http://host/upload/", "a/b.txt")
                .unwrap()
                .as_str(),
            "http://host/upload/a/b.txt"
        );
    }

    #[test]
    fn build_url_root_base() {
        assert_eq!(
            build_url("http://host", "a.txt").unwrap().as_str(),
            "http://host/a.txt"
        );
    }

    #[test]
    fn build_url_strips_traversal_and_empty_segments() {
        // `.`/`..`/empty segments are dropped (so the URL can never escape the base path); like
        // `s3::object_key`/`local.rs`, a `..` drops only itself, it does not pop the preceding segment.
        let u = build_url("http://host/p", "a/../b/./c.txt").unwrap();
        assert_eq!(u.as_str(), "http://host/p/a/b/c.txt");
        assert!(!build_url("http://host/p", "../../etc/passwd")
            .unwrap()
            .as_str()
            .contains(".."));
        assert_eq!(
            build_url("http://host/p", "").unwrap().as_str(),
            "http://host/p"
        );
    }

    #[test]
    fn build_url_rejects_malformed_base() {
        assert!(build_url("not a url", "a.txt").unwrap_err().is_permanent());
    }

    #[test]
    fn build_url_rejects_cannot_be_a_base() {
        // `mailto:` URLs have no host/path segments to append to.
        assert!(build_url("mailto:a@b.com", "a.txt")
            .unwrap_err()
            .is_permanent());
    }

    // ---- HttpMethod::parse --------------------------------------------------------------------------

    #[test]
    fn method_parse_defaults_to_put() {
        assert_eq!(HttpMethod::parse(None).unwrap(), HttpMethod::Put);
    }

    #[test]
    fn method_parse_case_insensitive() {
        assert_eq!(HttpMethod::parse(Some("put")).unwrap(), HttpMethod::Put);
        assert_eq!(HttpMethod::parse(Some("Post")).unwrap(), HttpMethod::Post);
    }

    #[test]
    fn method_parse_rejects_unknown() {
        assert!(HttpMethod::parse(Some("PATCH")).unwrap_err().is_permanent());
    }

    // ---- chunk_bounds / content_range_header ------------------------------------------------------

    #[test]
    fn chunk_bounds_middle_chunk() {
        assert_eq!(chunk_bounds(0, 10, 25), (0, 9));
        assert_eq!(chunk_bounds(10, 10, 25), (10, 19));
        assert_eq!(
            chunk_bounds(20, 10, 25),
            (20, 24),
            "last chunk carries the remainder"
        );
    }

    #[test]
    fn chunk_bounds_single_chunk_covers_whole_file() {
        assert_eq!(chunk_bounds(0, 100, 7), (0, 6));
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

    // ---- resume token --------------------------------------------------------------------------

    #[test]
    fn http_resume_token_round_trips_via_resume_state() {
        let tok = HttpResumeToken {
            url: "http://host/upload/a.bin".into(),
            size: 999,
            mtime_ms: 12345,
            bytes_committed: 500,
        };
        let rs = tok.to_resume();
        assert_eq!(rs.bytes_committed, 500);
        let json = serde_json::to_string(&rs).unwrap();
        let back: ResumeState = serde_json::from_str(&json).unwrap();
        let tok2 = HttpResumeToken::from_resume(&back).expect("token present");
        assert_eq!(tok2, tok);
    }

    #[test]
    fn http_resume_token_absent_or_wrong_shape_is_none() {
        assert!(HttpResumeToken::from_resume(&ResumeState::default()).is_none());
        let ftps_shape = ResumeState {
            bytes_committed: 1,
            token: serde_json::json!({ "ftps": { "tempPath": "x" } }),
        };
        assert!(HttpResumeToken::from_resume(&ftps_shape).is_none());
    }

    #[test]
    fn http_resume_token_matches_guards_url_size_and_mtime() {
        let tok = HttpResumeToken {
            url: "http://host/a.bin".into(),
            size: 100,
            mtime_ms: 42,
            bytes_committed: 50,
        };
        assert!(tok.matches("http://host/a.bin", 100, 42));
        assert!(!tok.matches("http://host/other.bin", 100, 42));
        assert!(!tok.matches("http://host/a.bin", 200, 42));
        assert!(!tok.matches("http://host/a.bin", 100, 99));
    }

    // ---- delivery handle ------------------------------------------------------------------------

    #[test]
    fn handle_round_trips_url() {
        let h = build_handle("http://host/upload/a.bin", Algorithm::Crc32c);
        assert_eq!(handle_url(&h).as_deref(), Some("http://host/upload/a.bin"));
        assert_eq!(h.get("algo").and_then(|v| v.as_str()), Some("CRC32C"));
    }

    #[test]
    fn handle_url_absent_is_none() {
        assert_eq!(handle_url(&serde_json::json!({})), None);
    }

    // ---- error classification -------------------------------------------------------------------

    #[test]
    fn classify_transport_is_transient() {
        assert!(classify_http(None, true).is_transient());
        assert!(
            classify_http(Some(401), true).is_transient(),
            "transport wins over a 4xx code"
        );
    }

    #[test]
    fn classify_5xx_and_throttle_codes_are_transient() {
        for c in [500, 502, 503, 504, 429, 408] {
            assert!(
                classify_http(Some(c), false).is_transient(),
                "{c} should be transient"
            );
        }
    }

    #[test]
    fn classify_other_4xx_is_permanent() {
        for c in [400, 401, 403, 404, 409, 410, 422] {
            assert!(
                classify_http(Some(c), false).is_permanent(),
                "{c} should be permanent"
            );
        }
    }

    #[test]
    fn classify_unclassified_status_and_bare_failure_are_transient() {
        assert!(classify_http(Some(302), false).is_transient());
        assert!(classify_http(None, false).is_transient());
    }

    // ---- credentials ----------------------------------------------------------------------------

    /// A fake credential service returning canned JSON for one secret name. Overrides `get_json`
    /// directly (a provided/default trait method) — the opaque `Secret` type has no public
    /// constructor outside the edgecommons credentials crate (same pattern as S3's/SFTP's/FTPS's fakes).
    struct FakeCreds {
        name: String,
        json: Option<serde_json::Value>,
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
        fn get_json(&self, name: &str) -> edgecommons::Result<Option<serde_json::Value>> {
            if name == self.name {
                Ok(self.json.clone())
            } else {
                Ok(None)
            }
        }
    }

    fn base_cfg() -> HttpEgress {
        HttpEgress {
            url: "http://localhost/upload".into(),
            method: None,
            headers: Default::default(),
            bearer_token: None,
            username: None,
            password: None,
            credentials: None,
            resumable: None,
            chunk_bytes: None,
            checksum_algorithm: None,
        }
    }

    #[test]
    fn resolve_auth_none_when_unconfigured() {
        assert_eq!(
            resolve_http_auth(&base_cfg(), None).unwrap(),
            HttpAuth::None
        );
    }

    #[test]
    fn resolve_auth_ambient_bearer() {
        let mut cfg = base_cfg();
        cfg.bearer_token = Some("tok123".into());
        assert_eq!(
            resolve_http_auth(&cfg, None).unwrap(),
            HttpAuth::Bearer("tok123".into())
        );
    }

    #[test]
    fn resolve_auth_ambient_basic() {
        let mut cfg = base_cfg();
        cfg.username = Some("alice".into());
        cfg.password = Some("s3cr3t".into());
        assert_eq!(
            resolve_http_auth(&cfg, None).unwrap(),
            HttpAuth::Basic {
                username: "alice".into(),
                password: "s3cr3t".into(),
            }
        );
    }

    #[test]
    fn resolve_auth_ambient_password_without_username_is_permanent() {
        let mut cfg = base_cfg();
        cfg.password = Some("s3cr3t".into());
        assert!(resolve_http_auth(&cfg, None).unwrap_err().is_permanent());
    }

    #[test]
    fn resolve_auth_secret_bearer() {
        let creds = FakeCreds {
            name: "http-creds".into(),
            json: Some(serde_json::json!({ "bearerToken": "vault-tok" })),
        };
        let mut cfg = base_cfg();
        cfg.credentials = Some(serde_json::json!({ "$secret": "http-creds" }));
        assert_eq!(
            resolve_http_auth(&cfg, Some(&creds)).unwrap(),
            HttpAuth::Bearer("vault-tok".into())
        );
    }

    #[test]
    fn resolve_auth_secret_basic_falls_back_to_egress_username() {
        let creds = FakeCreds {
            name: "http-creds".into(),
            json: Some(serde_json::json!({ "password": "vault-pass" })),
        };
        let mut cfg = base_cfg();
        cfg.username = Some("fallback-user".into());
        cfg.credentials = Some(serde_json::json!({ "$secret": "http-creds" }));
        let auth = resolve_http_auth(&cfg, Some(&creds)).unwrap();
        assert_eq!(
            auth,
            HttpAuth::Basic {
                username: "fallback-user".into(),
                password: "vault-pass".into(),
            }
        );
    }

    #[test]
    fn resolve_auth_secret_without_service_is_permanent() {
        let mut cfg = base_cfg();
        cfg.credentials = Some(serde_json::json!({ "$secret": "http-creds" }));
        assert!(resolve_http_auth(&cfg, None).unwrap_err().is_permanent());
    }

    #[test]
    fn resolve_auth_unknown_secret_is_permanent() {
        let creds = FakeCreds {
            name: "other".into(),
            json: Some(serde_json::json!({ "bearerToken": "x" })),
        };
        let mut cfg = base_cfg();
        cfg.credentials = Some(serde_json::json!({ "$secret": "missing" }));
        assert!(resolve_http_auth(&cfg, Some(&creds))
            .unwrap_err()
            .is_permanent());
    }

    #[test]
    fn resolve_auth_secret_missing_username_is_permanent() {
        let creds = FakeCreds {
            name: "http-creds".into(),
            json: Some(serde_json::json!({ "password": "vault-pass" })),
        };
        let mut cfg = base_cfg();
        cfg.credentials = Some(serde_json::json!({ "$secret": "http-creds" }));
        assert!(resolve_http_auth(&cfg, Some(&creds))
            .unwrap_err()
            .is_permanent());
    }

    #[test]
    fn resolve_auth_secret_neither_field_is_permanent() {
        let creds = FakeCreds {
            name: "http-creds".into(),
            json: Some(serde_json::json!({ "somethingElse": true })),
        };
        let mut cfg = base_cfg();
        cfg.credentials = Some(serde_json::json!({ "$secret": "http-creds" }));
        assert!(resolve_http_auth(&cfg, Some(&creds))
            .unwrap_err()
            .is_permanent());
    }

    #[test]
    fn resolve_auth_non_secret_json_shape_is_permanent() {
        let mut cfg = base_cfg();
        cfg.credentials = Some(serde_json::json!({ "bearerToken": "inline" }));
        assert!(resolve_http_auth(&cfg, None).unwrap_err().is_permanent());
    }

    // ---- Debug redaction ------------------------------------------------------------------------

    #[test]
    fn http_auth_debug_redacts_secrets() {
        assert_eq!(format!("{:?}", HttpAuth::None), "None");
        assert_eq!(
            format!("{:?}", HttpAuth::Bearer("TOP_SECRET".into())),
            "Bearer(***redacted***)"
        );
        let basic = HttpAuth::Basic {
            username: "alice".into(),
            password: "TOP_SECRET".into(),
        };
        let dbg = format!("{basic:?}");
        assert!(!dbg.contains("TOP_SECRET"));
        assert!(dbg.contains("alice"));
        assert!(dbg.contains("***redacted***"));
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

    // ---- HttpDest::build --------------------------------------------------------------------------

    #[test]
    fn build_defaults_to_put_resumable_crc32c() {
        let dest = HttpDest::build(&base_cfg(), &DestDeps::default()).unwrap();
        assert_eq!(dest.method, HttpMethod::Put);
        assert!(dest.resumable);
        assert_eq!(dest.algo, Algorithm::Crc32c);
        assert_eq!(dest.chunk_bytes, DEFAULT_CHUNK_BYTES);
        assert_eq!(dest.auth, HttpAuth::None);
    }

    #[test]
    fn build_post_forces_resumable_off() {
        let mut cfg = base_cfg();
        cfg.method = Some("POST".into());
        cfg.resumable = Some(true);
        let dest = HttpDest::build(&cfg, &DestDeps::default()).unwrap();
        assert_eq!(dest.method, HttpMethod::Post);
        assert!(!dest.resumable, "resume is forced off for POST");
    }

    #[test]
    fn build_reads_overrides() {
        let mut cfg = base_cfg();
        cfg.checksum_algorithm = Some("SHA-256".into());
        cfg.chunk_bytes = Some(1024);
        cfg.resumable = Some(false);
        let dest = HttpDest::build(&cfg, &DestDeps::default()).unwrap();
        assert_eq!(dest.algo, Algorithm::Sha256);
        assert_eq!(dest.chunk_bytes, 1024);
        assert!(!dest.resumable);
    }

    #[test]
    fn build_rejects_unknown_checksum_algorithm() {
        let mut cfg = base_cfg();
        cfg.checksum_algorithm = Some("MD5".into());
        assert!(HttpDest::build(&cfg, &DestDeps::default())
            .err()
            .expect("unknown algorithm rejected")
            .is_permanent());
    }

    #[test]
    fn build_rejects_malformed_url() {
        let mut cfg = base_cfg();
        cfg.url = "not a url".into();
        assert!(HttpDest::build(&cfg, &DestDeps::default())
            .err()
            .expect("malformed url rejected")
            .is_permanent());
    }

    #[test]
    fn build_allows_but_warns_on_plaintext_http_with_credentials() {
        // NFR-7: credentials over plaintext http:// are a misconfiguration — build still succeeds
        // (a trusted localhost/mesh endpoint may use http) but a warn is emitted. Bearer over http://:
        let mut cfg = base_cfg(); // url is http://localhost/upload
        cfg.bearer_token = Some("tok".into());
        let dest = HttpDest::build(&cfg, &DestDeps::default()).expect("http+creds still builds");
        assert_eq!(dest.auth, HttpAuth::Bearer("tok".into()));
        // Basic over http:// exercises the same guard.
        let mut cfg2 = base_cfg();
        cfg2.username = Some("alice".into());
        cfg2.password = Some("s3cr3t".into());
        HttpDest::build(&cfg2, &DestDeps::default()).expect("basic http builds");
        // https:// with credentials is the correct configuration — no warn path, still builds.
        let mut cfg3 = base_cfg();
        cfg3.url = "https://host/upload".into();
        cfg3.bearer_token = Some("tok".into());
        HttpDest::build(&cfg3, &DestDeps::default()).expect("https+creds builds");
    }

    #[test]
    fn build_propagates_auth_resolution_failure() {
        let mut cfg = base_cfg();
        cfg.password = Some("s3cr3t".into()); // no username → permanent
        assert!(HttpDest::build(&cfg, &DestDeps::default())
            .err()
            .expect("bad auth config rejected")
            .is_permanent());
    }
}
