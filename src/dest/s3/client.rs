//! # file-replicator — S3 SDK call seam (coverage-excluded, NFR-2)
//!
//! Every function here makes a real AWS S3 network round-trip (client construction, `PutObject`,
//! `CreateMultipartUpload`/`UploadPart`/`CompleteMultipartUpload`/`AbortMultipartUpload`,
//! `HeadObject`). Those lines are unreachable in CI/unit runs, so this file is **excluded from the
//! coverage gate** via `--ignore-filename-regex '(testutil|dest.s3.client)\.rs'` — the same
//! "trait seams + fakes, exclude live-infra" convention ggcommons uses. It is exercised live by the
//! self-skipping floci integration tests (`tests/s3_floci.rs`) and the org validation matrix
//! (DESIGN §21); all *decision* logic it relies on lives in [`super`] and is unit-tested.
//!
//! The orchestration (size-adaptive dispatch, parallel resumable parts, resume checkpointing,
//! restart-on-`NoSuchUpload`), the lazy client builder ([`S3Dest::s3`]), and the
//! `impl Destination for S3Dest` delegation glue all live here so [`super::S3Dest`] stays on the
//! coverage-counted side with only pure/decision logic. Helpers this file calls (`plan_upload`,
//! `object_key`, `hash_range`, `RangeBody`, `checksum_to_b64`, `S3ResumeToken`, `classify_s3`,
//! `compare_object_checksum`, `build_handle`) are the unit-tested pure functions in [`super`].

use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use aws_config::BehaviorVersion;
use aws_sdk_s3::config::{Credentials, Region};
use aws_sdk_s3::error::{ProvideErrorMetadata, SdkError};
use aws_sdk_s3::primitives::{ByteStream, SdkBody};
use aws_sdk_s3::types::{
    ChecksumAlgorithm, ChecksumMode, CompletedMultipartUpload, CompletedPart, ServerSideEncryption,
    StorageClass,
};
use aws_sdk_s3::Client;
use tokio::task::JoinSet;

use crate::config::Verify;
use crate::dest::Destination;
use crate::domain::{Checksum, Delivered, ProgressSink, ResumeState, WorkItem};
use crate::error::{ReplError, Result};
use crate::integrity::Algorithm;
use crate::ratelimit::Bandwidth;

use super::{
    build_handle, checksum_to_b64, classify_s3, compare_object_checksum, hash_range, handle_key,
    handle_s3_checksum, missing_parts, object_key, part_len, plan_upload, source_changed, RangeBody,
    S3ClientParams, S3CompletedPart, S3Dest, S3ResumeToken, UploadPlan,
};

/// Wrap a range of `path` into a retryable, streamed `ByteStream` for `PutObject`/`UploadPart`: a
/// fresh [`RangeBody`] (re-opens + re-seeks the file) is built each time the SDK needs to (re)send the
/// body — the first send, or an SDK-internal retry of a transient failure. Bounded chunk memory (no
/// whole-part `Vec`, DESIGN NFR-3); throttle + progress run once per actual byte send. An SDK-internal
/// retry rebuilds the body and re-throttles/re-reports the resent bytes: re-throttling is correct
/// (those bytes really are sent again), and re-reporting cannot overshoot because the caller's
/// `on_progress` clamps the running total to the object/part size (so progress is bounded to 100%
/// even across retries). This retry-rebuild path is not unit-testable here (it needs a live SDK
/// transient), but the clamp makes its only observable effect a bounded no-op.
fn streaming_body(
    path: Arc<Path>,
    offset: u64,
    len: u64,
    bw: Bandwidth,
    on_progress: Arc<dyn Fn(u64) + Send + Sync>,
) -> ByteStream {
    ByteStream::new(SdkBody::retryable(move || {
        SdkBody::from_body_0_4(RangeBody::new(
            path.clone(),
            offset,
            len,
            bw.clone(),
            on_progress.clone(),
        ))
    }))
}

// ---- lazy client + trait glue -------------------------------------------------------------------

impl S3Dest {
    /// Lazily build (once) and return the AWS S3 client. The build itself is [`build_client`] (network
    /// SDK config); this is the only orchestration glue and lives on the excluded side of the seam.
    async fn s3(&self) -> Result<&Client> {
        self.client
            .get_or_try_init(|| build_client(&self.client_params, &self.creds))
            .await
    }
}

#[async_trait]
impl Destination for S3Dest {
    fn kind(&self) -> &'static str {
        "s3"
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
        let client = self.s3().await?;
        deliver(self, client, item, resume, progress, bw).await
    }

    async fn verify(&self, item: &WorkItem, delivered: &Delivered, policy: Verify) -> Result<()> {
        let client = self.s3().await?;
        verify(self, client, item, delivered, policy).await
    }

    async fn abort(&self, item: &WorkItem, resume: &ResumeState) -> Result<()> {
        let client = self.s3().await?;
        abort(self, client, item, resume).await
    }
}

// ---- client construction ------------------------------------------------------------------------

/// Build the AWS S3 client from the derived params + credential mode. `load_defaults`/`load` resolve
/// credentials lazily (no network here in the ambient case); the S3-specific `force_path_style` /
/// `accelerate` come off the S3 config builder (they are not aws-config loader options).
pub(super) async fn build_client(
    params: &S3ClientParams,
    creds: &super::CredMode,
) -> Result<Client> {
    let mut loader = aws_config::defaults(BehaviorVersion::latest());
    if let Some(r) = &params.region {
        loader = loader.region(Region::new(r.clone()));
    }
    if let Some(url) = &params.endpoint_url {
        loader = loader.endpoint_url(url.clone());
    }
    if let super::CredMode::Static {
        access_key_id,
        secret_access_key,
        session_token,
    } = creds
    {
        loader = loader.credentials_provider(Credentials::new(
            access_key_id,
            secret_access_key,
            session_token.clone(),
            None,
            "file-replicator-static",
        ));
    }
    let sdk = loader.load().await;
    let mut b = aws_sdk_s3::config::Builder::from(&sdk);
    if params.force_path_style {
        b = b.force_path_style(true);
    }
    if params.accelerate {
        b = b.accelerate(true);
    }
    Ok(Client::from_conf(b.build()))
}

// ---- deliver ------------------------------------------------------------------------------------

/// Size-adaptive delivery: `PutObject` for small files, resumable multipart for large ones.
pub(super) async fn deliver(
    dest: &S3Dest,
    client: &Client,
    item: &WorkItem,
    resume: Option<ResumeState>,
    progress: &ProgressSink,
    bw: &Bandwidth,
) -> Result<Delivered> {
    // Trust the on-disk metadata, not the (possibly stale) discovery size, for the plan; also capture
    // the mtime so a multipart resume can reject a same-length in-place rewrite (§11.3).
    let meta = tokio::fs::metadata(&item.abs_source)
        .await
        .map_err(ReplError::classify_io)?;
    let size = meta.len();
    let mtime_ms = file_mtime_ms(&meta);
    let key = object_key(&dest.prefix, &item.relpath);

    match plan_upload(size, &dest.multipart) {
        UploadPlan::Single => {
            put_single(dest, client, item, &key, size, mtime_ms, progress, bw).await
        }
        UploadPlan::Multipart {
            part_size,
            num_parts,
        } => {
            put_multipart(
                dest, client, item, &key, size, part_size, num_parts, mtime_ms, resume, progress, bw,
            )
            .await
        }
    }
}

/// Source last-modified time in unix ms (0 when the platform/file does not expose it — a `0` token
/// then never matches a real upload, forcing a safe fresh MPU on resume).
fn file_mtime_ms(meta: &std::fs::Metadata) -> i64 {
    meta.modified()
        .ok()
        .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

/// Single `PutObject` with a precomputed flexible checksum. Two disk passes over the same range: pass 1
/// ([`hash_range`]) computes the flexible-checksum header that must precede the body; pass 2 (a streamed
/// [`RangeBody`]) is the throttled/progress-reported egress. This doubles the source read versus the old
/// whole-range `Vec` buffer — the deliberate memory-for-I/O tradeoff that bounds peak transfer RSS to a
/// single [`READ_CHUNK`](super) regardless of file size (DESIGN §11.4/NFR-3). Because the two passes are
/// independent reads, a post-send size+mtime re-check ([`source_changed`]) rejects a source mutated
/// mid-upload (`Transient` → re-plan) so the stored object can never silently desync from the sent
/// checksum.
#[allow(clippy::too_many_arguments)]
async fn put_single(
    dest: &S3Dest,
    client: &Client,
    item: &WorkItem,
    key: &str,
    size: u64,
    mtime_ms: i64,
    progress: &ProgressSink,
    bw: &Bandwidth,
) -> Result<Delivered> {
    // Pass 1 (local disk, no throttle/no progress): precompute the flexible-checksum header that must
    // precede the body. Pass 2 (below) is the throttled/progress-reported streamed send.
    let checksum = hash_range(&item.abs_source, 0, size, dest.algo).await?;
    let ck_b64 = checksum_to_b64(&checksum);

    let path: Arc<Path> = item.abs_source.clone().into();
    let total = Arc::new(AtomicU64::new(0));
    let p = progress.clone();
    let body = streaming_body(
        path,
        0,
        size,
        bw.clone(),
        Arc::new(move |d| {
            // Clamp to `size`: an SDK-internal retry rebuilds the body and re-reports its bytes, which
            // would otherwise push the reported running total past 100% (see `streaming_body`).
            p.report((total.fetch_add(d, Ordering::Relaxed) + d).min(size));
        }),
    );
    let mut req = client.put_object().bucket(&dest.bucket).key(key).body(body);
    if let Some(b64) = &ck_b64 {
        req = match dest.algo {
            Algorithm::Crc32c => req.checksum_crc32_c(b64),
            Algorithm::Sha256 => req.checksum_sha256(b64),
        };
    }
    if let Some(sc) = &dest.storage_class {
        req = req.storage_class(StorageClass::from(sc.as_str()));
    }
    if let Some(sse) = &dest.sse {
        req = req.server_side_encryption(ServerSideEncryption::from(sse.as_str()));
    }
    if let Some(k) = &dest.kms_key_id {
        req = req.ssekms_key_id(k);
    }

    // UNSIGNED-PAYLOAD (DESIGN §11.2): skip the SigV4 payload-hash pass for simple PUTs. Integrity is
    // still guaranteed by TLS plus the precomputed flexible checksum sent alongside. This is the SDK's
    // first-class `disable_payload_signing` knob (not a no-op). Multipart parts are already streamed,
    // so it applies to the single-PUT path only, matching §11.2 ("off for multipart").
    let resp = if dest.unsigned_payload {
        req.customize()
            .disable_payload_signing()
            .send()
            .await
            .map_err(map_sdk)?
    } else {
        req.send().await.map_err(map_sdk)?
    };

    let obj_ck = response_checksum(dest.algo, resp.checksum_crc32_c(), resp.checksum_sha256());
    // Server-side echo must match what we computed (defence in depth over TLS).
    if let (Some(local), Some(remote)) = (&ck_b64, &obj_ck) {
        compare_object_checksum(local, remote)?;
    }
    // The object is stored. Because the checksum (pass 1) and the streamed body (pass 2) are two
    // independent disk reads, re-stat the source: a same-length OR resized in-place rewrite during the
    // (throttled) send desyncs the stored bytes from the sent checksum. A checksum-validating endpoint
    // already rejected such a send (BadDigest → transient); this additionally covers non-validating
    // S3-compatible endpoints (MinIO/floci) whose verify downgrades to size-only. Transient → the next
    // attempt re-hashes the now-stable bytes and overwrites the object (self-heals, DESIGN §11.4/§13.2).
    let after = tokio::fs::metadata(&item.abs_source)
        .await
        .map_err(ReplError::classify_io)?;
    if source_changed(size, mtime_ms, after.len(), file_mtime_ms(&after)) {
        return Err(ReplError::Transient(format!(
            "s3 source changed during upload of {key}; re-planning against the current bytes"
        )));
    }
    let handle = build_handle(key, obj_ck.as_deref().or(ck_b64.as_deref()), dest.algo, false);
    Ok(Delivered {
        bytes: size,
        checksum,
        handle,
    })
}

// ---- multipart ----------------------------------------------------------------------------------

/// Whether a single multipart attempt found its `uploadId` still valid.
enum MpuError {
    /// The `uploadId` no longer exists (`NoSuchUpload` at `UploadPart` or `CompleteMultipartUpload`).
    /// The caller decides: the object may already be complete (a prior complete whose ACK we lost),
    /// otherwise the MPU was swept/aborted and we restart from a fresh `CreateMultipartUpload`
    /// (DESIGN §13.4 — "the replicator re-initiates the upload").
    Gone,
    /// A real transfer error to surface to the retry engine.
    Fatal(ReplError),
}

impl From<ReplError> for MpuError {
    fn from(e: ReplError) -> Self {
        MpuError::Fatal(e)
    }
}

/// Resumable multipart with bounded-concurrency parallel parts (DESIGN §11.1/§11.3/§13.4).
#[allow(clippy::too_many_arguments)]
async fn put_multipart(
    dest: &S3Dest,
    client: &Client,
    item: &WorkItem,
    key: &str,
    size: u64,
    part_size: u64,
    num_parts: u32,
    mtime_ms: i64,
    resume: Option<ResumeState>,
    progress: &ProgressSink,
    bw: &Bandwidth,
) -> Result<Delivered> {
    // 1. Reuse a persisted token only when it targets this exact object + layout + source
    //    (key/partSize/size/mtime). If it does, try to finish it.
    let reusable = resume
        .as_ref()
        .and_then(S3ResumeToken::from_resume)
        .filter(|t| t.matches(key, part_size, size, mtime_ms));

    if let Some(token) = reusable {
        match run_multipart_attempt(
            dest, client, item, key, size, part_size, num_parts, mtime_ms, token, progress, bw,
        )
        .await
        {
            Ok(d) => return Ok(d),
            Err(MpuError::Fatal(e)) => return Err(e),
            Err(MpuError::Gone) => {
                // The persisted uploadId vanished: swept by the bucket's incomplete-MPU lifecycle
                // during a long outage, aborted out-of-band, OR already consumed by a prior
                // CompleteMultipartUpload whose response we never saw. If the object is already live
                // at the right size, that earlier complete succeeded → treat as delivered (recover
                // the object checksum for verify). Otherwise drop the stale token and re-initiate a
                // fresh upload (DESIGN §13.4).
                if let Some(d) = recover_completed_object(dest, client, key, size).await? {
                    return Ok(d);
                }
                clear_token(dest, item);
                // fall through to a fresh MPU
            }
        }
    }

    // 2. Fresh multipart upload: new uploadId, upload every part, complete.
    let token = create_mpu(dest, client, item, key, part_size, num_parts, size, mtime_ms).await?;
    match run_multipart_attempt(
        dest, client, item, key, size, part_size, num_parts, mtime_ms, token, progress, bw,
    )
    .await
    {
        Ok(d) => Ok(d),
        Err(MpuError::Fatal(e)) => Err(e),
        // A brand-new uploadId disappearing mid-flight is unexpected; retry defensively.
        Err(MpuError::Gone) => Err(ReplError::Transient(
            "s3 multipart uploadId disappeared during a fresh upload".into(),
        )),
    }
}

/// Start a fresh multipart upload and write-ahead its token (so a crash after create can still
/// abort/resume it — DESIGN §11.3).
#[allow(clippy::too_many_arguments)]
async fn create_mpu(
    dest: &S3Dest,
    client: &Client,
    item: &WorkItem,
    key: &str,
    part_size: u64,
    num_parts: u32,
    size: u64,
    mtime_ms: i64,
) -> Result<S3ResumeToken> {
    let mut req = client
        .create_multipart_upload()
        .bucket(&dest.bucket)
        .key(key)
        .checksum_algorithm(checksum_algorithm(dest.algo));
    if let Some(sc) = &dest.storage_class {
        req = req.storage_class(StorageClass::from(sc.as_str()));
    }
    if let Some(sse) = &dest.sse {
        req = req.server_side_encryption(ServerSideEncryption::from(sse.as_str()));
    }
    if let Some(k) = &dest.kms_key_id {
        req = req.ssekms_key_id(k);
    }
    let resp = req.send().await.map_err(map_sdk)?;
    let upload_id = resp
        .upload_id()
        .ok_or_else(|| ReplError::Transient("s3 CreateMultipartUpload: no uploadId".into()))?
        .to_string();
    let token = S3ResumeToken {
        upload_id,
        key: key.to_string(),
        part_size,
        total_size: size,
        mtime_ms,
        completed: Vec::new(),
    };
    // Write-ahead the fresh uploadId so a crash after create can still abort/resume it.
    save_token(dest, item, &token, num_parts, size);
    Ok(token)
}

/// Upload `token`'s still-missing parts (bounded concurrency) then `CompleteMultipartUpload`.
/// `Err(MpuError::Gone)` means the uploadId was rejected (`NoSuchUpload`) at `UploadPart` or complete.
#[allow(clippy::too_many_arguments)]
async fn run_multipart_attempt(
    dest: &S3Dest,
    client: &Client,
    item: &WorkItem,
    key: &str,
    size: u64,
    part_size: u64,
    num_parts: u32,
    mtime_ms: i64,
    token: S3ResumeToken,
    progress: &ProgressSink,
    bw: &Bandwidth,
) -> std::result::Result<Delivered, MpuError> {
    let missing = missing_parts(num_parts, &token.completed);
    let committed = token.bytes_committed(num_parts, size);
    let total = Arc::new(AtomicU64::new(committed));
    let completed = Arc::new(Mutex::new(token.completed.clone()));
    let max = dest.multipart.max_concurrent.max(1);

    // Bounded spawning: keep at most `max` part tasks resident, spawning the next only as one drains.
    // Bounds both the in-flight read buffers AND the task/allocation count even for a 10 000-part
    // multi-TiB object (DESIGN NFR-3) — the JoinSet size is the concurrency governor.
    let mut set: JoinSet<std::result::Result<(), MpuError>> = JoinSet::new();
    let mut pending = missing.into_iter();

    loop {
        while set.len() < max {
            let Some(part_number) = pending.next() else {
                break;
            };
            let offset = (part_number as u64 - 1) * part_size;
            let len = part_len(part_number, part_size, num_parts, size);

            let client = client.clone();
            let bucket = dest.bucket.clone();
            let key = key.to_string();
            let upload_id = token.upload_id.clone();
            let path: Arc<Path> = item.abs_source.clone().into();
            let algo = dest.algo;
            let bw = bw.clone();
            let progress = progress.clone();
            let total = total.clone();
            let completed = completed.clone();
            let store = dest.store.clone();
            let instance = item.instance.clone();
            let relpath = item.relpath.clone();

            set.spawn(async move {
                // Pass 1 (local disk, no throttle/no progress): precompute the part's checksum header.
                let part_ck = hash_range(&path, offset, len, algo)
                    .await
                    .map_err(MpuError::Fatal)?;
                let b64 = checksum_to_b64(&part_ck);
                // Pass 2: the throttled/progress-reported streamed send.
                let t = total.clone();
                let p = progress.clone();
                let body = streaming_body(
                    path.clone(),
                    offset,
                    len,
                    bw.clone(),
                    Arc::new(move |d| {
                        // Clamp to the object size (the shared accumulator is committed + streamed
                        // across all concurrent parts): an SDK-internal retry re-reports a part's bytes,
                        // which would otherwise push the reported total past 100% (see `streaming_body`).
                        p.report((t.fetch_add(d, Ordering::Relaxed) + d).min(size));
                    }),
                );
                let mut req = client
                    .upload_part()
                    .bucket(&bucket)
                    .key(&key)
                    .upload_id(&upload_id)
                    .part_number(part_number as i32)
                    .body(body);
                if let Some(b) = &b64 {
                    req = match algo {
                        Algorithm::Crc32c => req.checksum_crc32_c(b),
                        Algorithm::Sha256 => req.checksum_sha256(b),
                    };
                }
                let resp = match req.send().await {
                    Ok(r) => r,
                    Err(e) if is_no_such_upload(&e) => return Err(MpuError::Gone),
                    Err(e) => return Err(MpuError::Fatal(map_sdk(e))),
                };
                // A missing/empty ETag would poison CompleteMultipartUpload (InvalidPart) if recorded;
                // treat it as transient (like the missing-uploadId guard on create) rather than
                // persisting "" into both the checkpoint and the completed-part list.
                let etag = match resp.e_tag() {
                    Some(t) if !t.is_empty() => t.to_string(),
                    _ => {
                        return Err(MpuError::Fatal(ReplError::Transient(format!(
                            "s3 UploadPart {part_number}: response had no ETag"
                        ))))
                    }
                };
                let cp = S3CompletedPart {
                    part_number,
                    e_tag: etag,
                    checksum: response_checksum(algo, resp.checksum_crc32_c(), resp.checksum_sha256())
                        .or(b64),
                };
                // Record the part AND persist the durable checkpoint UNDER the same lock, so a slower
                // task's older snapshot can never overwrite a newer one (out-of-order clobber). The
                // save is synchronous (no `.await` under the guard), so this only briefly serializes
                // the concurrent part tasks' checkpoint writes.
                let mut guard = completed.lock().expect("completed parts mutex");
                guard.push(cp);
                guard.sort_by_key(|p| p.part_number);
                if let Some(st) = &store {
                    let tok = S3ResumeToken {
                        upload_id: upload_id.clone(),
                        key: key.clone(),
                        part_size,
                        total_size: size,
                        mtime_ms,
                        completed: guard.clone(),
                    };
                    if let Err(e) =
                        st.save_resume(&instance, &relpath, "s3", &tok.to_resume(num_parts, size))
                    {
                        tracing::warn!(relpath = %relpath, error = %e, "persist s3 multipart checkpoint failed");
                    }
                }
                drop(guard);
                Ok(())
            });
        }

        match set.join_next().await {
            None => break, // all parts done
            Some(Ok(Ok(()))) => {}
            Some(Ok(Err(e))) => {
                set.abort_all();
                return Err(e);
            }
            Some(Err(join_err)) => {
                set.abort_all();
                return Err(MpuError::Fatal(ReplError::Transient(format!(
                    "s3 part task join: {join_err}"
                ))));
            }
        }
    }

    // Complete with the ordered part list (part_number → etag [+ checksum]).
    let parts = completed.lock().expect("completed parts mutex").clone();
    let completed_mp = CompletedMultipartUpload::builder()
        .set_parts(Some(
            parts
                .iter()
                .map(|p| {
                    let mut b = CompletedPart::builder()
                        .part_number(p.part_number as i32)
                        .e_tag(&p.e_tag);
                    if let Some(c) = &p.checksum {
                        b = match dest.algo {
                            Algorithm::Crc32c => b.checksum_crc32_c(c),
                            Algorithm::Sha256 => b.checksum_sha256(c),
                        };
                    }
                    b.build()
                })
                .collect(),
        ))
        .build();
    let resp = match client
        .complete_multipart_upload()
        .bucket(&dest.bucket)
        .key(key)
        .upload_id(&token.upload_id)
        .multipart_upload(completed_mp)
        .send()
        .await
    {
        Ok(r) => r,
        Err(e) if is_no_such_upload(&e) => return Err(MpuError::Gone),
        Err(e) => return Err(MpuError::Fatal(map_sdk(e))),
    };

    // Same source-stability re-check as the single-PUT path: each part's checksum header was computed
    // in a pass separate from that part's streamed body, so a source rewritten mid-upload leaves the
    // assembled object desynced from those checksums (invisible to a size-only verify on a non-validating
    // endpoint). Transient → the next attempt sees a changed mtime, discards the resume token, and
    // re-uploads a fresh, consistent object (DESIGN §11.4/§13.2).
    let after = tokio::fs::metadata(&item.abs_source)
        .await
        .map_err(|e| MpuError::Fatal(ReplError::classify_io(e)))?;
    if source_changed(size, mtime_ms, after.len(), file_mtime_ms(&after)) {
        return Err(MpuError::Fatal(ReplError::Transient(format!(
            "s3 source changed during multipart upload of {key}; re-planning"
        ))));
    }

    let obj_ck = response_checksum(dest.algo, resp.checksum_crc32_c(), resp.checksum_sha256());
    let handle = build_handle(key, obj_ck.as_deref(), dest.algo, true);
    // A multipart object's content CRC differs from S3's composite `base64-N`, so the whole-file
    // Checksum enum is not comparable — verification uses the stored `s3Checksum` string (the handle).
    Ok(Delivered {
        bytes: size,
        checksum: Checksum::None,
        handle,
    })
}

/// After a uploadId turned out to be gone, check whether the object is already live at `key` with the
/// expected size — meaning a prior `CompleteMultipartUpload` actually succeeded (its ACK was lost). If
/// so, return it as delivered (with the object checksum for verify); otherwise `None` (restart fresh).
async fn recover_completed_object(
    dest: &S3Dest,
    client: &Client,
    key: &str,
    size: u64,
) -> Result<Option<Delivered>> {
    match client
        .head_object()
        .bucket(&dest.bucket)
        .key(key)
        .checksum_mode(ChecksumMode::Enabled)
        .send()
        .await
    {
        Ok(resp) => {
            let cl = resp.content_length().unwrap_or(-1);
            if cl < 0 || cl as u64 != size {
                // A different or partial object sits at the key — not our completed upload.
                return Ok(None);
            }
            let obj_ck =
                response_checksum(dest.algo, resp.checksum_crc32_c(), resp.checksum_sha256());
            tracing::info!(
                key = %key,
                "s3 uploadId gone but object already present + right size; treating a prior CompleteMultipartUpload as delivered (lost ACK)"
            );
            Ok(Some(Delivered {
                bytes: size,
                checksum: Checksum::None,
                handle: build_handle(key, obj_ck.as_deref(), dest.algo, true),
            }))
        }
        Err(e) if is_not_found(&e) => Ok(None),
        Err(e) => Err(map_sdk(e)),
    }
}

/// Drop a stale S3 resume checkpoint (best-effort) so the fresh upload starts clean.
fn clear_token(dest: &S3Dest, item: &WorkItem) {
    if let Some(st) = &dest.store {
        if let Err(e) = st.clear_resume(&item.instance, &item.relpath, "s3") {
            tracing::warn!(relpath = %item.relpath, error = %e, "clear stale s3 resume token failed");
        }
    }
}

/// Persist the current resume token (best-effort; a store fault only loses cross-crash resume, not
/// the in-flight transfer).
fn save_token(dest: &S3Dest, item: &WorkItem, token: &S3ResumeToken, num_parts: u32, size: u64) {
    if let Some(st) = &dest.store {
        if let Err(e) = st.save_resume(
            &item.instance,
            &item.relpath,
            "s3",
            &token.to_resume(num_parts, size),
        ) {
            tracing::warn!(relpath = %item.relpath, error = %e, "persist s3 resume token failed");
        }
    }
}

// ---- verify (before the source side effect, DESIGN §13.1) ---------------------------------------

/// Verify the delivered object per `policy`. `Checksum` compares S3's object-level checksum to the one
/// captured on delivery (downgrading to size+existence if S3 does not echo a checksum, e.g. a floci
/// limitation); `Size` compares `content_length`; `None` confirms existence.
pub(super) async fn verify(
    dest: &S3Dest,
    client: &Client,
    item: &WorkItem,
    delivered: &Delivered,
    policy: Verify,
) -> Result<()> {
    let key = handle_key(&delivered.handle).unwrap_or_else(|| object_key(&dest.prefix, &item.relpath));

    let want_checksum = matches!(policy, Verify::Checksum);
    let mut req = client.head_object().bucket(&dest.bucket).key(&key);
    if want_checksum {
        req = req.checksum_mode(ChecksumMode::Enabled);
    }
    let resp = req.send().await.map_err(map_sdk)?;

    match policy {
        Verify::None => Ok(()),
        Verify::Size => {
            let cl = resp.content_length().unwrap_or(-1);
            crate::integrity::verify_size(delivered.bytes, cl.max(0) as u64)
        }
        Verify::Checksum => {
            // Size is always cheap to check alongside the checksum.
            if let Some(cl) = resp.content_length() {
                crate::integrity::verify_size(delivered.bytes, cl.max(0) as u64)?;
            }
            match handle_s3_checksum(&delivered.handle) {
                Some(expected) => {
                    match response_checksum(dest.algo, resp.checksum_crc32_c(), resp.checksum_sha256())
                    {
                        Some(actual) => compare_object_checksum(&expected, &actual),
                        None => {
                            tracing::warn!(
                                key = %key,
                                "S3 returned no object checksum; downgraded to size+existence (endpoint limitation)"
                            );
                            Ok(())
                        }
                    }
                }
                // No stored object checksum (legacy recovery row, or multipart without one) → the
                // successful HeadObject already proved existence + size.
                None => Ok(()),
            }
        }
    }
}

// ---- abort (give-up cleanup, DESIGN §11.3) ------------------------------------------------------

/// Abort the in-flight multipart upload recorded in `resume` (idempotent: a `NoSuchUpload` — already
/// swept or completed — is treated as success). A resume without an S3 token is a no-op.
pub(super) async fn abort(
    dest: &S3Dest,
    client: &Client,
    _item: &WorkItem,
    resume: &ResumeState,
) -> Result<()> {
    let Some(token) = S3ResumeToken::from_resume(resume) else {
        return Ok(());
    };
    match client
        .abort_multipart_upload()
        .bucket(&dest.bucket)
        .key(&token.key)
        .upload_id(&token.upload_id)
        .send()
        .await
    {
        Ok(_) => Ok(()),
        Err(e) if is_no_such_upload(&e) => Ok(()),
        Err(e) => Err(map_sdk(e)),
    }
}

// ---- SDK helpers --------------------------------------------------------------------------------

/// The `ChecksumAlgorithm` request enum for an [`Algorithm`].
fn checksum_algorithm(algo: Algorithm) -> ChecksumAlgorithm {
    match algo {
        Algorithm::Crc32c => ChecksumAlgorithm::Crc32C,
        Algorithm::Sha256 => ChecksumAlgorithm::Sha256,
    }
}

/// Pick the response checksum field matching `algo`.
fn response_checksum(algo: Algorithm, crc32c: Option<&str>, sha256: Option<&str>) -> Option<String> {
    match algo {
        Algorithm::Crc32c => crc32c.map(String::from),
        Algorithm::Sha256 => sha256.map(String::from),
    }
}

/// Map an `SdkError` to the retry engine's [`ReplError`] via the unit-tested [`classify_s3`], pulling
/// the service code + HTTP status off the error and flagging transport (timeout/dispatch) failures.
fn map_sdk<E: ProvideErrorMetadata>(e: SdkError<E>) -> ReplError {
    let transport = matches!(
        e,
        SdkError::TimeoutError(_) | SdkError::DispatchFailure(_) | SdkError::ConstructionFailure(_)
    );
    let code = e.as_service_error().and_then(|se| se.code()).map(String::from);
    let status = e.raw_response().map(|r| r.status().as_u16());
    classify_s3(code.as_deref(), status, transport)
}

/// Whether an error means the multipart upload no longer exists (idempotent abort / restart trigger).
fn is_no_such_upload<E: ProvideErrorMetadata>(e: &SdkError<E>) -> bool {
    e.as_service_error().and_then(|se| se.code()) == Some("NoSuchUpload")
}

/// Whether an error means the object/key does not exist (a `HeadObject` 404 carries no service code).
fn is_not_found<E: ProvideErrorMetadata>(e: &SdkError<E>) -> bool {
    matches!(
        e.as_service_error().and_then(|se| se.code()),
        Some("NoSuchKey") | Some("NotFound")
    ) || e.raw_response().map(|r| r.status().as_u16()) == Some(404)
}
