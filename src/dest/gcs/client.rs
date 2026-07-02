//! # file-replicator — GCS network call seam (coverage-excluded, NFR-2)
//!
//! Every function here builds the `reqwest::Client` or sends a real HTTP request against the GCS JSON/
//! upload API (session initiate, chunk `PUT`, metadata/media `GET`, session `DELETE`). Those lines are
//! unreachable in CI/unit runs, so this file is **excluded from the coverage gate** via
//! `--ignore-filename-regex '(testutil|dest.(s3|sftp|ftps|http|azure|gcs).client)\.rs'` (mirrors
//! `dest/{s3,sftp,ftps,http,azure}/client.rs`). It is exercised live by `tests/gcs_fakegcs.rs`, which
//! self-skips when fake-gcs-server is not reachable — all *decision* logic it relies on lives in
//! [`super`] and is unit-tested there.

use std::sync::Arc;

use async_trait::async_trait;
use reqwest::header::{CONTENT_RANGE, CONTENT_TYPE, LOCATION, RANGE};

use crate::config::Verify;
use crate::dest::Destination;
use crate::domain::{Checksum, Delivered, ProgressSink, ResumeState, WorkItem};
use crate::error::{ReplError, Result};
use crate::integrity::{Algorithm, Hasher};
use crate::ratelimit::Bandwidth;
use crate::state::StateStore;

use super::{
    b64_to_crc32c, build_handle, chunk_bounds, classify_gcs, committed_from_range_header,
    content_range_header, handle_object, hash_local_prefix, object_path, read_source_range,
    GcsCredMode, GcsDest, GcsResumeToken,
};

// ---- lazy client + trait glue -------------------------------------------------------------------

impl GcsDest {
    /// Lazily build (once) and return the `reqwest::Client`. Construction itself does no network I/O
    /// (connections are made lazily per-request by reqwest), but it is kept on this side of the seam
    /// for symmetry with `HttpDest::http`/`S3Dest::s3`.
    async fn http(&self) -> Result<&reqwest::Client> {
        self.client
            .get_or_try_init(|| async {
                reqwest::Client::builder()
                    .build()
                    .map_err(|e| ReplError::Permanent(format!("build gcs client: {e}")))
            })
            .await
    }
}

#[async_trait]
impl Destination for GcsDest {
    fn kind(&self) -> &'static str {
        "gcs"
    }

    fn supports_resume(&self) -> bool {
        true
    }

    async fn deliver(
        &self,
        item: &WorkItem,
        resume: Option<ResumeState>,
        progress: &ProgressSink,
        bw: &Bandwidth,
    ) -> Result<Delivered> {
        deliver(self, item, resume, progress, bw).await
    }

    async fn verify(&self, item: &WorkItem, delivered: &Delivered, policy: Verify) -> Result<()> {
        verify(self, item, delivered, policy).await
    }

    async fn abort(&self, item: &WorkItem, resume: &ResumeState) -> Result<()> {
        abort(self, item, resume).await
    }
}

// ---- URL builders ------------------------------------------------------------------------------

/// The `/upload/storage/v1/b/{bucket}/o` base URL (both `uploadType=resumable` session-initiate and
/// `uploadType=media` one-shot requests hang query params off this).
fn upload_base_url(dest: &GcsDest) -> Result<reqwest::Url> {
    let mut url = reqwest::Url::parse(&dest.endpoint)
        .map_err(|e| ReplError::Permanent(format!("invalid gcs endpoint '{}': {e}", dest.endpoint)))?;
    {
        let mut segs = url.path_segments_mut().map_err(|_| {
            ReplError::Permanent(format!("gcs endpoint '{}' cannot be a base", dest.endpoint))
        })?;
        segs.pop_if_empty();
        segs.extend(["upload", "storage", "v1", "b", dest.bucket.as_str(), "o"]);
    }
    Ok(url)
}

/// The `/storage/v1/b/{bucket}/o/{object}` metadata/media URL. `object` is pushed as a single path
/// segment, so `url`'s percent-encoding escapes any `/` within it as `%2F` (the JSON API's convention
/// for an object name embedded in the path — GCS decodes it back to the literal name).
fn object_url(dest: &GcsDest, bucket: &str, object: &str) -> Result<reqwest::Url> {
    let mut url = reqwest::Url::parse(&dest.endpoint)
        .map_err(|e| ReplError::Permanent(format!("invalid gcs endpoint '{}': {e}", dest.endpoint)))?;
    {
        let mut segs = url.path_segments_mut().map_err(|_| {
            ReplError::Permanent(format!("gcs endpoint '{}' cannot be a base", dest.endpoint))
        })?;
        segs.pop_if_empty();
        segs.extend(["storage", "v1", "b", bucket, "o"]);
        segs.push(object);
    }
    Ok(url)
}

fn apply_auth(req: reqwest::RequestBuilder, creds: &GcsCredMode) -> reqwest::RequestBuilder {
    match creds {
        GcsCredMode::Anonymous => req,
        GcsCredMode::AccessToken(token) => req.bearer_auth(token),
    }
}

fn file_mtime_ms(meta: &std::fs::Metadata) -> i64 {
    meta.modified()
        .ok()
        .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

// ---- session lifecycle ---------------------------------------------------------------------------

/// Initiate a new resumable-upload session (`POST .../o?uploadType=resumable&name=...`), returning the
/// session URI from the `Location` response header.
async fn init_session(client: &reqwest::Client, dest: &GcsDest, object: &str) -> Result<String> {
    let mut url = upload_base_url(dest)?;
    url.query_pairs_mut()
        .append_pair("uploadType", "resumable")
        .append_pair("name", object);
    let mut req = client
        .post(url)
        .header(CONTENT_TYPE, "application/json; charset=UTF-8");
    req = apply_auth(req, &dest.creds);
    let resp = req
        .json(&serde_json::json!({ "name": object }))
        .send()
        .await
        .map_err(|_e| classify_gcs(None, true))?;
    let status = resp.status();
    if !status.is_success() {
        return Err(classify_gcs(Some(status.as_u16()), false));
    }
    resp.headers()
        .get(LOCATION)
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string())
        .ok_or_else(|| {
            ReplError::Transient("gcs resumable session response missing Location header".to_string())
        })
}

/// A failure sending a chunk to a resumable session.
enum SessionErr {
    /// The session URI is gone (`404`/`410`) — expired or forgotten by GCS. The caller drops the stale
    /// session and re-initiates a fresh one (mirrors `dest/s3`'s `MpuError::Gone`), rather than letting
    /// `classify_gcs`'s 4xx→Permanent mapping quarantine an otherwise-deliverable file.
    Gone,
    /// A real transfer error to surface to the retry engine.
    Fatal(ReplError),
}

/// Send one chunk to the session URI, carrying `Content-Range: bytes {start}-{end}/{total}`. A `308
/// Resume Incomplete` response's `Range` header is compared against `expected_committed` (= `end + 1`)
/// purely as a defensive sanity check — logged, not fatal, on mismatch. This backend does **not** issue
/// a separate server-side status-query round-trip before resuming (the documented `Content-Range: bytes
/// */{total}` idiom — see the module doc's "Resume" section for why): it trusts the locally-persisted
/// checkpoint directly, the same way `dest/azure`/`dest/http` do. A `404`/`410` (the session GCS no
/// longer knows about) maps to [`SessionErr::Gone`] so the caller can restart with a fresh session.
async fn send_chunk(
    client: &reqwest::Client,
    session_uri: &str,
    range: &str,
    expected_committed: u64,
    body: Vec<u8>,
) -> std::result::Result<(), SessionErr> {
    let resp = client
        .put(session_uri)
        .header(CONTENT_RANGE, range)
        .body(body)
        .send()
        .await
        .map_err(|_e| SessionErr::Fatal(classify_gcs(None, true)))?;
    let status = resp.status();
    let code = status.as_u16();
    if code == 308 {
        if let Some(reported) = resp
            .headers()
            .get(RANGE)
            .and_then(|v| v.to_str().ok())
            .and_then(committed_from_range_header)
        {
            if reported != expected_committed {
                tracing::warn!(
                    expected = expected_committed,
                    reported,
                    "gcs chunk PUT committed-byte count mismatch"
                );
            }
        }
        Ok(())
    } else if status.is_success() {
        Ok(())
    } else if code == 404 || code == 410 {
        Err(SessionErr::Gone)
    } else {
        Err(SessionErr::Fatal(classify_gcs(Some(code), false)))
    }
}

// ---- checkpoint persistence (mirrors dest/azure, dest/http) ---------------------------------------

#[allow(clippy::too_many_arguments)]
fn persist_checkpoint(
    store: &Option<Arc<dyn StateStore>>,
    instance: &str,
    relpath: &str,
    bucket: &str,
    object: &str,
    session_uri: &str,
    size: u64,
    mtime_ms: i64,
    committed: u64,
) {
    if let Some(st) = store {
        let tok = GcsResumeToken {
            bucket: bucket.to_string(),
            object: object.to_string(),
            session_uri: session_uri.to_string(),
            size,
            mtime_ms,
            bytes_committed: committed,
        };
        if let Err(e) = st.save_resume(instance, relpath, "gcs", &tok.to_resume()) {
            tracing::warn!(relpath = %relpath, error = %e, "persist gcs resume checkpoint failed");
        }
    }
}

fn clear_checkpoint(store: &Option<Arc<dyn StateStore>>, instance: &str, relpath: &str) {
    if let Some(st) = store {
        if let Err(e) = st.clear_resume(instance, relpath, "gcs") {
            tracing::warn!(relpath = %relpath, error = %e, "clear gcs resume checkpoint failed");
        }
    }
}

// ---- deliver ---------------------------------------------------------------------------------------

async fn deliver(
    dest: &GcsDest,
    item: &WorkItem,
    resume: Option<ResumeState>,
    progress: &ProgressSink,
    bw: &Bandwidth,
) -> Result<Delivered> {
    let meta = tokio::fs::metadata(&item.abs_source)
        .await
        .map_err(ReplError::classify_io)?;
    let size = meta.len();
    let mtime_ms = file_mtime_ms(&meta);
    let object = object_path(&dest.prefix, &item.relpath);
    let client = dest.http().await?;

    if size == 0 {
        // One-shot `uploadType=media` PUT: no session needed for an empty object (mirrors the other
        // backends' 0-byte special case).
        let mut url = upload_base_url(dest)?;
        url.query_pairs_mut()
            .append_pair("uploadType", "media")
            .append_pair("name", &object);
        let mut req = client.post(url);
        req = apply_auth(req, &dest.creds);
        let resp = req
            .body(Vec::new())
            .send()
            .await
            .map_err(|_e| classify_gcs(None, true))?;
        let status = resp.status();
        if !status.is_success() {
            return Err(classify_gcs(Some(status.as_u16()), false));
        }
        clear_checkpoint(&dest.store, &item.instance, &item.relpath);
        return Ok(Delivered {
            bytes: 0,
            checksum: Hasher::new(dest.algo).finish(),
            handle: build_handle(&dest.bucket, &object, dest.algo),
        });
    }

    // Resume trusts the locally-persisted checkpoint directly (mirrors `dest/azure`/`dest/http`) rather
    // than issuing a server-side status-query round-trip first — see the module doc's "Resume" section.
    let reusable = resume
        .as_ref()
        .and_then(GcsResumeToken::from_resume)
        .filter(|t| t.matches(&dest.bucket, &object, size, mtime_ms));
    let resuming = reusable.is_some();

    let (session_uri, committed) = match reusable {
        Some(t) => (t.session_uri, t.bytes_committed.min(size)),
        None => (init_session(client, dest, &object).await?, 0),
    };

    match run_session(client, dest, item, &object, size, mtime_ms, &session_uri, committed, progress, bw)
        .await
    {
        Ok(d) => Ok(d),
        Err(SessionErr::Fatal(e)) => Err(e),
        Err(SessionErr::Gone) if resuming => {
            // The persisted resumable session expired (~1 week) or was forgotten (a `404`/`410` on the
            // chunk PUT). Rather than fail permanently, drop the stale session, initiate a fresh one,
            // and restart from byte 0 — mirrors `dest/s3`'s `MpuError::Gone` → fresh
            // `CreateMultipartUpload` restart (DESIGN §13.4).
            tracing::info!(
                object = %object,
                "gcs resumable session gone; re-initiating a fresh session and restarting from byte 0"
            );
            clear_checkpoint(&dest.store, &item.instance, &item.relpath);
            let fresh = init_session(client, dest, &object).await?;
            match run_session(client, dest, item, &object, size, mtime_ms, &fresh, 0, progress, bw).await
            {
                Ok(d) => Ok(d),
                Err(SessionErr::Fatal(e)) => Err(e),
                // A brand-new session disappearing mid-flight is unexpected; retry defensively.
                Err(SessionErr::Gone) => Err(ReplError::Transient(
                    "gcs resumable session disappeared during a fresh upload".into(),
                )),
            }
        }
        // A brand-new session (no resume in play) disappearing mid-flight is unexpected; retry.
        Err(SessionErr::Gone) => Err(ReplError::Transient(
            "gcs resumable session disappeared during a fresh upload".into(),
        )),
    }
}

/// Stream `[committed, size)` to an established resumable `session_uri`, keeping the whole-file checksum
/// correct across a resume by re-hashing the `[0, committed)` prefix locally first (no network). Returns
/// [`SessionErr::Gone`] if the session URI is rejected mid-flight (`404`/`410`), so [`deliver`] can
/// re-initiate a fresh session and restart.
#[allow(clippy::too_many_arguments)]
async fn run_session(
    client: &reqwest::Client,
    dest: &GcsDest,
    item: &WorkItem,
    object: &str,
    size: u64,
    mtime_ms: i64,
    session_uri: &str,
    mut committed: u64,
    progress: &ProgressSink,
    bw: &Bandwidth,
) -> std::result::Result<Delivered, SessionErr> {
    let mut hasher = Hasher::new(dest.algo);
    if committed > 0 {
        hash_local_prefix(&item.abs_source, committed, &mut hasher)
            .await
            .map_err(SessionErr::Fatal)?;
    }

    persist_checkpoint(
        &dest.store,
        &item.instance,
        &item.relpath,
        &dest.bucket,
        object,
        session_uri,
        size,
        mtime_ms,
        committed,
    );

    // `read_source_range` reports the **delta** of each internal read, not a running total (see its
    // doc comment) — fold deltas into a cumulative total (seeded at the resumed `committed` offset)
    // across the whole multi-chunk transfer, mirroring `dest/azure`/`dest/http`'s progress accounting.
    let total = std::sync::atomic::AtomicU64::new(committed);

    while committed < size {
        let (start, end) = chunk_bounds(committed, dest.chunk_bytes, size);
        let len = end - start + 1;
        let buf = read_source_range(&item.abs_source, start, len, bw, &|n| {
            progress.report(total.fetch_add(n, std::sync::atomic::Ordering::Relaxed) + n)
        })
        .await
        .map_err(SessionErr::Fatal)?;
        hasher.update(&buf);
        let range = content_range_header(start, end, size);
        send_chunk(client, session_uri, &range, end + 1, buf).await?;
        committed = end + 1;
        persist_checkpoint(
            &dest.store,
            &item.instance,
            &item.relpath,
            &dest.bucket,
            object,
            session_uri,
            size,
            mtime_ms,
            committed,
        );
    }

    clear_checkpoint(&dest.store, &item.instance, &item.relpath);
    Ok(Delivered {
        bytes: size,
        checksum: hasher.finish(),
        handle: build_handle(&dest.bucket, object, dest.algo),
    })
}

// ---- verify (before the source side effect, DESIGN §13.1) ------------------------------------------

#[derive(serde::Deserialize)]
struct GcsObjectMetadata {
    size: String,
    crc32c: Option<String>,
}

async fn fetch_metadata(
    client: &reqwest::Client,
    dest: &GcsDest,
    bucket: &str,
    object: &str,
) -> Result<GcsObjectMetadata> {
    let url = object_url(dest, bucket, object)?;
    let mut req = client.get(url);
    req = apply_auth(req, &dest.creds);
    let resp = req.send().await.map_err(|_e| classify_gcs(None, true))?;
    let status = resp.status();
    if !status.is_success() {
        return Err(classify_gcs(Some(status.as_u16()), false));
    }
    resp.json::<GcsObjectMetadata>()
        .await
        .map_err(|_e| ReplError::Transient("gcs metadata response was not valid JSON".to_string()))
}

/// Download the object media and fold it through `hasher` in bounded chunks, returning the byte count.
/// Streams via `resp.chunk()` (NFR-3) rather than `resp.bytes()` — buffering the whole object would make
/// peak RSS scale with maxConcurrentFiles × file size (mirrors SFTP's streaming verify / `dest/http`).
async fn download_and_hash(
    client: &reqwest::Client,
    dest: &GcsDest,
    bucket: &str,
    object: &str,
    hasher: &mut Hasher,
) -> Result<u64> {
    let mut url = object_url(dest, bucket, object)?;
    url.query_pairs_mut().append_pair("alt", "media");
    let mut req = client.get(url);
    req = apply_auth(req, &dest.creds);
    let mut resp = req.send().await.map_err(|_e| classify_gcs(None, true))?;
    let status = resp.status();
    if !status.is_success() {
        return Err(classify_gcs(Some(status.as_u16()), false));
    }
    let mut downloaded: u64 = 0;
    while let Some(chunk) = resp.chunk().await.map_err(|_e| classify_gcs(None, true))? {
        downloaded += chunk.len() as u64;
        hasher.update(&chunk);
    }
    Ok(downloaded)
}

async fn verify(dest: &GcsDest, item: &WorkItem, delivered: &Delivered, policy: Verify) -> Result<()> {
    if matches!(policy, Verify::None) {
        return Ok(());
    }
    let (bucket, object) = handle_object(&delivered.handle)
        .unwrap_or_else(|| (dest.bucket.clone(), object_path(&dest.prefix, &item.relpath)));
    let client = dest.http().await?;

    match policy {
        Verify::None => unreachable!("handled above"),
        Verify::Size => {
            let meta = fetch_metadata(client, dest, &bucket, &object).await?;
            let size: u64 = meta
                .size
                .parse()
                .map_err(|_| ReplError::Transient("gcs metadata 'size' was not numeric".to_string()))?;
            crate::integrity::verify_size(delivered.bytes, size)
        }
        Verify::Checksum => match dest.algo {
            Algorithm::Crc32c => {
                let meta = fetch_metadata(client, dest, &bucket, &object).await?;
                let size: u64 = meta.size.parse().map_err(|_| {
                    ReplError::Transient("gcs metadata 'size' was not numeric".to_string())
                })?;
                crate::integrity::verify_size(delivered.bytes, size)?;
                let actual = meta
                    .crc32c
                    .as_deref()
                    .and_then(b64_to_crc32c)
                    .map(Checksum::Crc32c)
                    .ok_or_else(|| {
                        ReplError::Transient("gcs metadata missing a valid 'crc32c' field".to_string())
                    })?;
                crate::integrity::verify_checksum(&delivered.checksum, &actual)
            }
            Algorithm::Sha256 => {
                // GCS's JSON API exposes no SHA-256 metadata field — download + re-hash (mirrors
                // `dest/http::verify`'s `Checksum` path), streamed in bounded chunks (NFR-3).
                let mut hasher = Hasher::new(dest.algo);
                let downloaded = download_and_hash(client, dest, &bucket, &object, &mut hasher).await?;
                crate::integrity::verify_size(delivered.bytes, downloaded)?;
                crate::integrity::verify_checksum(&delivered.checksum, &hasher.finish())
            }
        },
    }
}

// ---- abort (give-up cleanup) -------------------------------------------------------------------

/// Cancel an in-progress resumable session (`DELETE` the session URI — GCS's documented response for a
/// successful cancel is `499 Client Closed Request`, an unusual but real status this endpoint returns).
/// A missing/never-started token, or a session GCS has already forgotten (404/410), is treated as
/// already-clean (idempotent, mirrors `dest/http::abort`).
async fn abort(dest: &GcsDest, _item: &WorkItem, resume: &ResumeState) -> Result<()> {
    let Some(token) = GcsResumeToken::from_resume(resume) else {
        return Ok(());
    };
    if token.bytes_committed == 0 && token.session_uri.is_empty() {
        return Ok(());
    }
    let client = dest.http().await?;
    let mut req = client.delete(&token.session_uri);
    req = apply_auth(req, &dest.creds);
    let resp = req.send().await.map_err(|_e| classify_gcs(None, true))?;
    let status = resp.status().as_u16();
    if status == 499 || (200..300).contains(&status) || status == 404 || status == 410 {
        Ok(())
    } else {
        Err(classify_gcs(Some(status), false))
    }
}
