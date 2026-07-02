//! # file-replicator — HTTP(S) network call seam (coverage-excluded, NFR-2)
//!
//! Every function here builds the `reqwest::Client` or sends a real HTTP request (`PUT`/`POST` chunk
//! upload, `HEAD`/`GET` verify, `DELETE` abort). Those lines are unreachable in CI/unit runs, so this
//! file is **excluded from the coverage gate** via
//! `--ignore-filename-regex '(testutil|dest.(s3|sftp|ftps|http).client)\.rs'` (mirrors
//! `dest/{s3,sftp,ftps}/client.rs`). It is exercised live by `tests/http_inprocess.rs`, which always
//! runs (an in-process server, not a container to skip) — all *decision* logic it relies on lives in
//! [`super`] and is unit-tested there.

use std::sync::Arc;

use async_trait::async_trait;

use crate::config::Verify;
use crate::dest::Destination;
use crate::domain::{Checksum, Delivered, ProgressSink, ResumeState, WorkItem};
use crate::error::{ReplError, Result};
use crate::integrity::Hasher;
use crate::ratelimit::Bandwidth;
use crate::state::StateStore;

use super::{
    build_handle, build_url, chunk_bounds, classify_http, content_range_header, handle_url,
    hash_local_prefix, read_source_range, HttpAuth, HttpDest, HttpMethod, HttpResumeToken,
};

// ---- lazy client + trait glue -------------------------------------------------------------------

impl HttpDest {
    /// Lazily build (once) and return the `reqwest::Client`. Construction itself does no network I/O
    /// (connections are made lazily per-request by reqwest), but it is kept on this side of the seam
    /// for symmetry with `S3Dest::s3`/the SFTP/FTPS connection builders.
    async fn http(&self) -> Result<&reqwest::Client> {
        self.client
            .get_or_try_init(|| async {
                reqwest::Client::builder()
                    .build()
                    .map_err(|e| ReplError::Permanent(format!("build http client: {e}")))
            })
            .await
    }
}

#[async_trait]
impl Destination for HttpDest {
    fn kind(&self) -> &'static str {
        "http"
    }

    fn supports_resume(&self) -> bool {
        self.resumable
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

// ---- deliver ---------------------------------------------------------------------------------------

async fn deliver(
    dest: &HttpDest,
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
    let url = build_url(&dest.base_url, &item.relpath)?;
    let url_str = url.to_string();
    let client = dest.http().await?;

    if !dest.resumable {
        // Single-shot: one whole-body request, no Content-Range, no incremental checkpoint (mirrors
        // `HttpDest::build`'s forcing of `resumable: false` for `method: POST`). `read_source_range`
        // reports the **delta** of each internal read, not a running total (see its doc comment), so
        // fold deltas into a cumulative total here before reporting (mirrors `dest/s3::put_single`).
        let total = std::sync::atomic::AtomicU64::new(0);
        let buf = read_source_range(&item.abs_source, 0, size, bw, &|n| {
            progress.report(total.fetch_add(n, std::sync::atomic::Ordering::Relaxed) + n)
        })
        .await?;
        let mut hasher = Hasher::new(dest.algo);
        hasher.update(&buf);
        send_chunk(client, dest, &url, None, buf).await?;
        clear_checkpoint(&dest.store, &item.instance, &item.relpath);
        return Ok(Delivered {
            bytes: size,
            checksum: hasher.finish(),
            handle: build_handle(&url_str, dest.algo),
        });
    }

    let reusable = resume
        .as_ref()
        .and_then(HttpResumeToken::from_resume)
        .filter(|t| t.matches(&url_str, size, mtime_ms));
    let mut committed = reusable.map(|t| t.bytes_committed.min(size)).unwrap_or(0);

    // Reconcile the locally-persisted checkpoint against the server's ACTUAL object length before
    // trusting it — SFTP/FTPS do the same (they clamp `committed` to the remote temp file's size).
    // Without this: a server that retained fewer bytes than the checkpoint claims would get a
    // head-truncated object (the ranged PUTs would resume past where the real data ends), and a server
    // that discarded the partial entirely needs a full restart. Plain HTTP has NO atomic publish (the
    // ranged PUTs write straight to the final URL, no temp+rename/commit), so this reconciliation is
    // how a resume stays correct; a deployment MUST use verify=Checksum/Size (never verify=None) with
    // this backend (see the module doc). A HEAD that the server can't answer is treated as "trust the
    // local checkpoint" (no reconciliation) rather than failing the transfer.
    if committed > 0 {
        match reconcile_remote_len(client, dest, &url).await? {
            RemoteLen::Len(remote) => committed = committed.min(remote),
            RemoteLen::Gone => committed = 0, // server discarded the partial → restart from byte 0
            RemoteLen::Unknown => {}
        }
    }

    let mut hasher = Hasher::new(dest.algo);
    if committed > 0 {
        hash_local_prefix(&item.abs_source, committed, &mut hasher).await?;
    }

    if size == 0 {
        // A 0-byte object: one empty ranged request rather than an unrepresentable "0 chunks" loop
        // (mirrors S3's 0-byte → single `PutObject`).
        send_chunk(
            client,
            dest,
            &url,
            Some(content_range_header(0, 0, 0)),
            Vec::new(),
        )
        .await?;
        clear_checkpoint(&dest.store, &item.instance, &item.relpath);
        return Ok(Delivered {
            bytes: 0,
            checksum: hasher.finish(),
            handle: build_handle(&url_str, dest.algo),
        });
    }

    persist_checkpoint(
        &dest.store,
        &item.instance,
        &item.relpath,
        &url_str,
        size,
        mtime_ms,
        committed,
    );

    // `read_source_range` reports the **delta** of each internal (sub-chunk) read, not a running
    // total (see its doc comment) — fold deltas into a cumulative total (seeded at the resumed
    // `committed` offset) across the whole multi-chunk transfer, mirroring `dest/s3`'s multipart
    // progress accounting.
    let total = std::sync::atomic::AtomicU64::new(committed);

    while committed < size {
        let (start, end) = chunk_bounds(committed, dest.chunk_bytes, size);
        let len = end - start + 1;
        let buf = read_source_range(&item.abs_source, start, len, bw, &|n| {
            progress.report(total.fetch_add(n, std::sync::atomic::Ordering::Relaxed) + n)
        })
        .await?;
        hasher.update(&buf);
        let range = content_range_header(start, end, size);
        send_chunk(client, dest, &url, Some(range), buf).await?;
        committed = end + 1;
        persist_checkpoint(
            &dest.store,
            &item.instance,
            &item.relpath,
            &url_str,
            size,
            mtime_ms,
            committed,
        );
    }

    clear_checkpoint(&dest.store, &item.instance, &item.relpath);
    Ok(Delivered {
        bytes: size,
        checksum: hasher.finish(),
        handle: build_handle(&url_str, dest.algo),
    })
}

/// The server's view of the resumed object's current length, used to reconcile a resume checkpoint.
enum RemoteLen {
    /// The object exists and the server reported this length via `Content-Length`.
    Len(u64),
    /// The object is gone (`404`) — the server discarded the partial, so restart from byte 0.
    Gone,
    /// The server could not be asked (`HEAD` unsupported / no `Content-Length`) — trust the checkpoint.
    Unknown,
}

/// `HEAD` the target URL to learn the server's current object length (for resume reconciliation). A
/// transport failure propagates (Transient — the following PUTs would fail too); a `404` maps to
/// [`RemoteLen::Gone`]; any other non-2xx, or a missing/unparseable `Content-Length`, maps to
/// [`RemoteLen::Unknown`] (fall back to trusting the local checkpoint rather than failing the transfer).
async fn reconcile_remote_len(
    client: &reqwest::Client,
    dest: &HttpDest,
    url: &reqwest::Url,
) -> Result<RemoteLen> {
    let mut req = client.head(url.clone());
    req = apply_auth(req, &dest.auth);
    let resp = req.send().await.map_err(|_e| classify_http(None, true))?;
    let status = resp.status();
    if status == reqwest::StatusCode::NOT_FOUND {
        return Ok(RemoteLen::Gone);
    }
    if !status.is_success() {
        return Ok(RemoteLen::Unknown);
    }
    // A `HEAD` response's `Content-Length` describes the resource a `GET` would return (RFC 9110
    // §9.3.2), not the (absent) HEAD body — read the header directly.
    let len = resp
        .headers()
        .get(reqwest::header::CONTENT_LENGTH)
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.parse::<u64>().ok());
    Ok(len.map(RemoteLen::Len).unwrap_or(RemoteLen::Unknown))
}

/// Send one chunk (or the whole body, for a single-shot request) and classify a non-2xx / transport
/// failure via [`classify_http`].
async fn send_chunk(
    client: &reqwest::Client,
    dest: &HttpDest,
    url: &reqwest::Url,
    content_range: Option<String>,
    body: Vec<u8>,
) -> Result<()> {
    let method = match dest.method {
        HttpMethod::Put => reqwest::Method::PUT,
        HttpMethod::Post => reqwest::Method::POST,
    };
    let mut req = client.request(method, url.clone());
    for (k, v) in &dest.headers {
        req = req.header(k.as_str(), v.as_str());
    }
    if let Some(range) = &content_range {
        req = req.header(reqwest::header::CONTENT_RANGE, range.as_str());
    }
    req = apply_auth(req, &dest.auth);
    let resp = req
        .body(body)
        .send()
        .await
        .map_err(|_e| classify_http(None, true))?;
    let status = resp.status();
    if status.is_success() {
        Ok(())
    } else {
        Err(classify_http(Some(status.as_u16()), false))
    }
}

fn apply_auth(req: reqwest::RequestBuilder, auth: &HttpAuth) -> reqwest::RequestBuilder {
    match auth {
        HttpAuth::None => req,
        HttpAuth::Bearer(token) => req.bearer_auth(token),
        HttpAuth::Basic { username, password } => req.basic_auth(username, Some(password)),
    }
}

fn file_mtime_ms(meta: &std::fs::Metadata) -> i64 {
    meta.modified()
        .ok()
        .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

#[allow(clippy::too_many_arguments)]
fn persist_checkpoint(
    store: &Option<Arc<dyn StateStore>>,
    instance: &str,
    relpath: &str,
    url: &str,
    size: u64,
    mtime_ms: i64,
    committed: u64,
) {
    if let Some(st) = store {
        let tok = HttpResumeToken {
            url: url.to_string(),
            size,
            mtime_ms,
            bytes_committed: committed,
        };
        if let Err(e) = st.save_resume(instance, relpath, "http", &tok.to_resume()) {
            tracing::warn!(relpath = %relpath, error = %e, "persist http resume checkpoint failed");
        }
    }
}

fn clear_checkpoint(store: &Option<Arc<dyn StateStore>>, instance: &str, relpath: &str) {
    if let Some(st) = store {
        if let Err(e) = st.clear_resume(instance, relpath, "http") {
            tracing::warn!(relpath = %relpath, error = %e, "clear http resume checkpoint failed");
        }
    }
}

// ---- verify (before the source side effect, DESIGN §13.1) ------------------------------------------

async fn verify(
    dest: &HttpDest,
    item: &WorkItem,
    delivered: &Delivered,
    policy: Verify,
) -> Result<()> {
    if matches!(policy, Verify::None) {
        return Ok(());
    }
    let url_str = handle_url(&delivered.handle)
        .or_else(|| {
            build_url(&dest.base_url, &item.relpath)
                .ok()
                .map(|u| u.to_string())
        })
        .ok_or_else(|| {
            ReplError::Permanent("no url recorded on the delivered handle".to_string())
        })?;
    let url = reqwest::Url::parse(&url_str)
        .map_err(|e| ReplError::Permanent(format!("invalid delivered url '{url_str}': {e}")))?;
    let client = dest.http().await?;

    match policy {
        Verify::None => unreachable!("handled above"),
        Verify::Size => {
            let mut req = client.head(url);
            req = apply_auth(req, &dest.auth);
            let resp = req.send().await.map_err(|_e| classify_http(None, true))?;
            let status = resp.status();
            if !status.is_success() {
                return Err(classify_http(Some(status.as_u16()), false));
            }
            // `Response::content_length()` reflects the (absent) body of a `HEAD` response, not the
            // `Content-Length` header describing what a `GET` would return — read the header directly
            // (RFC 9110 §9.3.2: a `HEAD` response's `Content-Length` still describes the resource).
            let len: u64 = resp
                .headers()
                .get(reqwest::header::CONTENT_LENGTH)
                .and_then(|v| v.to_str().ok())
                .and_then(|s| s.parse().ok())
                .ok_or_else(|| {
                    ReplError::Transient("http HEAD response missing Content-Length".to_string())
                })?;
            crate::integrity::verify_size(delivered.bytes, len)
        }
        Verify::Checksum => {
            let mut req = client.get(url);
            req = apply_auth(req, &dest.auth);
            let mut resp = req.send().await.map_err(|_e| classify_http(None, true))?;
            let status = resp.status();
            if !status.is_success() {
                return Err(classify_http(Some(status.as_u16()), false));
            }
            // Stream the download through the hasher in bounded chunks (NFR-3) instead of buffering the
            // whole object in memory — SFTP's verify does the same. `resp.bytes()` would materialize the
            // entire object, so peak RSS would scale with maxConcurrentFiles × file size; `chunk()`
            // keeps it at ~one network buffer.
            let mut hasher = Hasher::new(dest.algo);
            let mut downloaded: u64 = 0;
            while let Some(chunk) = resp.chunk().await.map_err(|_e| classify_http(None, true))? {
                downloaded += chunk.len() as u64;
                hasher.update(&chunk);
            }
            crate::integrity::verify_size(delivered.bytes, downloaded)?;
            let actual: Checksum = hasher.finish();
            crate::integrity::verify_checksum(&delivered.checksum, &actual)
        }
    }
}

// ---- abort (give-up cleanup) -------------------------------------------------------------------

async fn abort(dest: &HttpDest, _item: &WorkItem, resume: &ResumeState) -> Result<()> {
    let Some(token) = HttpResumeToken::from_resume(resume) else {
        return Ok(());
    };
    if token.bytes_committed == 0 {
        // Nothing was ever sent for this attempt — no partial object to clean up.
        return Ok(());
    }
    let url = reqwest::Url::parse(&token.url)
        .map_err(|e| ReplError::Permanent(format!("invalid resume url '{}': {e}", token.url)))?;
    let client = dest.http().await?;
    let mut req = client.delete(url);
    req = apply_auth(req, &dest.auth);
    let resp = req.send().await.map_err(|_e| classify_http(None, true))?;
    let status = resp.status();
    if status.is_success()
        || status == reqwest::StatusCode::NOT_FOUND
        || status == reqwest::StatusCode::METHOD_NOT_ALLOWED
    {
        Ok(())
    } else {
        Err(classify_http(Some(status.as_u16()), false))
    }
}
