//! # file-replicator — Azure Blob network call seam (coverage-excluded, NFR-2)
//!
//! Every function here builds the `ContainerClient`/`BlobClient` or sends a real Azure Blob REST
//! request (`Put Blob`, `Put Block`, `Put Block List`, `Get Properties`, `Get Blob`). Those lines are
//! unreachable in CI/unit runs, so this file is **excluded from the coverage gate** via
//! `--ignore-filename-regex '(testutil|dest.(s3|sftp|ftps|http|azure).client)\.rs'` (mirrors
//! `dest/{s3,sftp,ftps,http}/client.rs`). It is exercised live by the self-skipping
//! `tests/azure_azurite.rs` — all *decision* logic it relies on lives in [`super`] and is unit-tested.

use std::sync::Arc;

use async_trait::async_trait;
use azure_core::auth::Secret;
use azure_storage::{CloudLocation, StorageCredentials};
use azure_storage_blobs::prelude::{BlobBlockType, BlockList, ClientBuilder, ContainerClient};

use crate::config::Verify;
use crate::dest::Destination;
use crate::domain::{Delivered, ProgressSink, ResumeState, WorkItem};
use crate::error::{ReplError, Result};
use crate::integrity::Hasher;
use crate::ratelimit::Bandwidth;
use crate::state::StateStore;

use super::{
    block_bounds, block_id_bytes, blob_path, build_handle, classify_azure, handle_blob_path,
    hash_local_prefix, plan_upload, read_source_range, AzureCredMode, AzureDest, AzureResumeToken,
    UploadPlan,
};

// ---- lazy client + trait glue -------------------------------------------------------------------

impl AzureDest {
    /// Lazily build (once) and return the `ContainerClient` (bound to `account`+`container`; a blob's
    /// `BlobClient` is derived per-call from it — cheap, no network). Construction itself does no
    /// network I/O (connections are made lazily per-request), but it is kept on this side of the seam
    /// for symmetry with `S3Dest::s3`/`HttpDest::http`.
    async fn container(&self) -> Result<&ContainerClient> {
        self.client
            .get_or_try_init(|| async { build_container_client(self) })
            .await
    }
}

#[async_trait]
impl Destination for AzureDest {
    fn kind(&self) -> &'static str {
        "azure"
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
        let container = self.container().await?;
        deliver(self, container, item, resume, progress, bw).await
    }

    async fn verify(&self, item: &WorkItem, delivered: &Delivered, policy: Verify) -> Result<()> {
        let container = self.container().await?;
        verify(self, container, item, delivered, policy).await
    }

    async fn abort(&self, item: &WorkItem, resume: &ResumeState) -> Result<()> {
        abort(item, resume)
    }
}

// ---- client construction ------------------------------------------------------------------------

/// Build the `ContainerClient` from the resolved account/container/endpoint/credential mode. No
/// network I/O — the SDK resolves connections lazily per-request.
fn build_container_client(dest: &AzureDest) -> Result<ContainerClient> {
    let AzureCredMode::AccountKey(key) = &dest.creds;
    let storage_creds = StorageCredentials::access_key(dest.account.clone(), Secret::new(key.clone()));
    let builder = match &dest.endpoint_url {
        Some(url) => ClientBuilder::with_location(
            CloudLocation::Custom {
                account: dest.account.clone(),
                uri: url.clone(),
            },
            storage_creds,
        ),
        None => ClientBuilder::new(dest.account.clone(), storage_creds),
    };
    Ok(builder.container_client(dest.container.clone()))
}

// ---- deliver ------------------------------------------------------------------------------------

async fn deliver(
    dest: &AzureDest,
    container: &ContainerClient,
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
    let blob = blob_path(&dest.prefix, &item.relpath);
    let blob_client = container.blob_client(blob.clone());

    match plan_upload(size, &dest.blocks) {
        UploadPlan::Single => {
            let total = std::sync::atomic::AtomicU64::new(0);
            let buf = read_source_range(&item.abs_source, 0, size, bw, &|n| {
                progress.report(total.fetch_add(n, std::sync::atomic::Ordering::Relaxed) + n)
            })
            .await?;
            let mut hasher = Hasher::new(dest.algo);
            hasher.update(&buf);
            blob_client.put_block_blob(buf).await.map_err(|e| map_azure(&e))?;
            clear_checkpoint(&dest.store, &item.instance, &item.relpath);
            Ok(Delivered {
                bytes: size,
                checksum: hasher.finish(),
                handle: build_handle(&blob, dest.algo),
            })
        }
        UploadPlan::Blocks { block_size, num_blocks } => {
            deliver_blocks(
                dest, &blob_client, item, &blob, size, block_size, num_blocks, mtime_ms, resume, progress,
                bw,
            )
            .await
        }
    }
}

/// Stage `blob`'s blocks sequentially (resuming from `resume` when it still matches), then commit
/// with one `Put Block List` (DESIGN §10.2 "resume of uncommitted blocks" — see the module doc for why
/// sequential staging keeps the committed set a contiguous prefix, letting the whole-file checksum be
/// kept correct with a local-only re-read of the already-staged bytes, no re-send).
#[allow(clippy::too_many_arguments)]
async fn deliver_blocks(
    dest: &AzureDest,
    blob_client: &azure_storage_blobs::prelude::BlobClient,
    item: &WorkItem,
    blob: &str,
    size: u64,
    block_size: u64,
    num_blocks: u32,
    mtime_ms: i64,
    resume: Option<ResumeState>,
    progress: &ProgressSink,
    bw: &Bandwidth,
) -> Result<Delivered> {
    let reusable = resume
        .as_ref()
        .and_then(AzureResumeToken::from_resume)
        .filter(|t| t.matches(blob, block_size, size, mtime_ms));
    let mut committed = reusable.map(|t| t.bytes_committed.min(size)).unwrap_or(0);

    let mut hasher = Hasher::new(dest.algo);
    if committed > 0 {
        hash_local_prefix(&item.abs_source, committed, &mut hasher).await?;
    }

    persist_checkpoint(&dest.store, &item.instance, &item.relpath, blob, block_size, size, mtime_ms, committed);

    let total = std::sync::atomic::AtomicU64::new(committed);
    while committed < size {
        let (start, end) = block_bounds(committed, block_size, size);
        let len = end - start + 1;
        let buf = read_source_range(&item.abs_source, start, len, bw, &|n| {
            progress.report(total.fetch_add(n, std::sync::atomic::Ordering::Relaxed) + n)
        })
        .await?;
        hasher.update(&buf);
        let block_number = (start / block_size) as u32 + 1;
        blob_client
            .put_block(block_id_bytes(block_number), buf)
            .await
            .map_err(|e| map_azure(&e))?;
        committed = end + 1;
        persist_checkpoint(&dest.store, &item.instance, &item.relpath, blob, block_size, size, mtime_ms, committed);
    }

    let mut list = BlockList::default();
    for n in 1..=num_blocks {
        list.blocks.push(BlobBlockType::new_uncommitted(block_id_bytes(n)));
    }
    if let Err(e) = blob_client.put_block_list(list).await {
        // A crash between a PRIOR `Put Block List` success and the checkpoint clear leaves those block
        // ids in the blob's COMMITTED list; re-committing them as `new_uncommitted` refs then fails
        // (400 InvalidBlockList) — and `classify_azure` would map that 4xx to Permanent, quarantining
        // an already-delivered blob. Mirror `dest/s3`'s `recover_completed_object`: if the blob is
        // already live at the expected size, that earlier commit succeeded (its checkpoint clear was
        // lost) → treat as delivered rather than fail. Otherwise surface the real error.
        if blob_committed_at_size(blob_client, size).await? {
            tracing::info!(
                blob = %blob,
                "azure Put Block List failed but blob already present at the right size; treating a \
                 prior commit as delivered (lost checkpoint clear)"
            );
            clear_checkpoint(&dest.store, &item.instance, &item.relpath);
            return Ok(Delivered {
                bytes: size,
                checksum: hasher.finish(),
                handle: build_handle(blob, dest.algo),
            });
        }
        return Err(map_azure(&e));
    }

    clear_checkpoint(&dest.store, &item.instance, &item.relpath);
    Ok(Delivered {
        bytes: size,
        checksum: hasher.finish(),
        handle: build_handle(blob, dest.algo),
    })
}

/// Whether the blob is already live at exactly `size` bytes (a committed block list). Used to recover
/// from a re-commit after a lost checkpoint clear (see [`deliver_blocks`]); a 404 (`BlobNotFound`)
/// means it was never committed → `false`, any other error propagates.
async fn blob_committed_at_size(
    blob_client: &azure_storage_blobs::prelude::BlobClient,
    size: u64,
) -> Result<bool> {
    match blob_client.get_properties().await {
        Ok(resp) => Ok(resp.blob.properties.content_length == size),
        Err(e) if is_azure_not_found(&e) => Ok(false),
        Err(e) => Err(map_azure(&e)),
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
    blob: &str,
    block_size: u64,
    size: u64,
    mtime_ms: i64,
    committed: u64,
) {
    if let Some(st) = store {
        let tok = AzureResumeToken {
            blob: blob.to_string(),
            block_size,
            size,
            mtime_ms,
            bytes_committed: committed,
        };
        if let Err(e) = st.save_resume(instance, relpath, "azure", &tok.to_resume()) {
            tracing::warn!(relpath = %relpath, error = %e, "persist azure resume checkpoint failed");
        }
    }
}

fn clear_checkpoint(store: &Option<Arc<dyn StateStore>>, instance: &str, relpath: &str) {
    if let Some(st) = store {
        if let Err(e) = st.clear_resume(instance, relpath, "azure") {
            tracing::warn!(relpath = %relpath, error = %e, "clear azure resume checkpoint failed");
        }
    }
}

// ---- verify (before the source side effect, DESIGN §13.1) ------------------------------------------

async fn verify(
    dest: &AzureDest,
    container: &ContainerClient,
    item: &WorkItem,
    delivered: &Delivered,
    policy: Verify,
) -> Result<()> {
    if matches!(policy, Verify::None) {
        return Ok(());
    }
    let blob = handle_blob_path(&delivered.handle).unwrap_or_else(|| blob_path(&dest.prefix, &item.relpath));
    let blob_client = container.blob_client(blob);

    match policy {
        Verify::None => unreachable!("handled above"),
        Verify::Size => {
            let resp = blob_client.get_properties().await.map_err(|e| map_azure(&e))?;
            crate::integrity::verify_size(delivered.bytes, resp.blob.properties.content_length)
        }
        Verify::Checksum => {
            // Stream the download range-by-range through the hasher (NFR-3) rather than `get_content()`,
            // which buffers the WHOLE blob in memory (peak RSS would scale with maxConcurrentFiles ×
            // file size). `get().into_stream()` pages the blob in bounded (default 16 MiB) ranges; we
            // hash + drop each range's bytes, so peak memory is one range, not the whole object (mirrors
            // SFTP's chunked streaming verify).
            use futures::StreamExt as _;
            let mut hasher = Hasher::new(dest.algo);
            let mut downloaded: u64 = 0;
            let mut stream = blob_client.get().into_stream();
            while let Some(value) = stream.next().await {
                let response = value.map_err(|e| map_azure(&e))?;
                let range = response.data.collect().await.map_err(|e| map_azure(&e))?;
                downloaded += range.len() as u64;
                hasher.update(&range);
            }
            crate::integrity::verify_size(delivered.bytes, downloaded)?;
            crate::integrity::verify_checksum(&delivered.checksum, &hasher.finish())
        }
    }
}

// ---- abort (give-up cleanup) -------------------------------------------------------------------

/// Azure has no API to drop staged-but-uncommitted blocks (unlike S3's `AbortMultipartUpload`) — a
/// blob's never-committed blocks are simply garbage-collected ~7 days after their last `Put Block`.
/// A give-up is therefore a documented no-op: nothing was ever committed at the stable blob path
/// (`Put Block List` runs only after every block is staged), so there is nothing live to clean up.
fn abort(_item: &WorkItem, _resume: &ResumeState) -> Result<()> {
    Ok(())
}

// ---- error mapping -------------------------------------------------------------------------------

/// Map an `azure_core::Error` to the retry engine's [`ReplError`] via the unit-tested
/// [`classify_azure`], pulling the HTTP status off an `HttpResponse` error kind and flagging every
/// other kind (`Io`, `Credential`, `DataConversion`, `MockFramework`, `Other`) as a transport-ish
/// failure with no status to classify by.
fn map_azure(e: &azure_core::Error) -> ReplError {
    match e.kind() {
        azure_core::error::ErrorKind::HttpResponse { status, .. } => {
            classify_azure(Some(*status as u16), false)
        }
        _ => classify_azure(None, true),
    }
}

/// Whether an Azure error is a `404 Not Found` (the blob/resource does not exist).
fn is_azure_not_found(e: &azure_core::Error) -> bool {
    matches!(
        e.kind(),
        azure_core::error::ErrorKind::HttpResponse { status, .. } if *status as u16 == 404
    )
}
