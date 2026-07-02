//! # file-replicator — Azure Blob destination (DESIGN §10.2/§10.3; P5)
//!
//! Block-blob ingest via staged blocks: small files (`≤ blocks.thresholdBytes`, default 16 MiB) use a
//! single `Put Blob`; larger files are split into `blocks.blockSizeBytes`-sized (default 16 MiB)
//! segments, each staged with **Put Block**, then committed atomically with one **Put Block List** —
//! Azure's multipart-equivalent (mirrors `dest/s3`'s `PutObject`/multipart split, DESIGN §11.1).
//!
//! ## Resume (DESIGN §10.2 "resume of uncommitted blocks")
//! Blocks are uploaded **sequentially** (block 1, 2, … in order) rather than in parallel, which keeps
//! the already-staged set always a contiguous prefix of the blob — exactly like `dest/http`'s ranged
//! `PUT`s. After each block succeeds, `{blob, blockSize, size, mtimeMs, bytesCommitted}` is persisted
//! into the shared [`StateStore`] resume row (keyed by `(instance, relpath, "azure")`) — write-ahead so
//! a crash resumes only the missing tail. A token is reused only when `blob`/`blockSize`/`size`/`mtime`
//! still match ([`AzureResumeToken::matches`]); otherwise the source changed and a fresh block sequence
//! starts at block 1. Because the committed prefix is contiguous, the whole-file checksum stays correct
//! across a resume the same way `dest/http` does it: the already-staged bytes are **re-read from the
//! local source** (no network — [`hash_local_prefix`]) and folded into the hasher before staging the
//! remaining blocks. Put Block List is idempotent against the same still-uncommitted block set, so a
//! crash after the last block staged but before the commit call simply re-commits on the next attempt.
//!
//! Azure has **no API to drop staged-but-uncommitted blocks** (unlike S3's `AbortMultipartUpload`) —
//! they are simply garbage-collected ~7 days after their last Put Block if never committed. A give-up
//! [`Destination::abort`](super::Destination::abort) is therefore a documented no-op (DESIGN §10.2).
//!
//! ## Concurrency note (follow-up)
//! The backend matrix (DESIGN §10.2) lists "parallel blocks" for Azure (mirroring S3's parallel
//! parts); this implementation ships **sequential** staging, matching `dest/http`/`dest/sftp`/
//! `dest/ftps`. Sequential staging keeps the resume checksum bookkeeping simple (a contiguous
//! prefix, re-hashed locally with no network — see above) at the cost of not saturating bandwidth
//! on a single huge file the way S3's bounded-`JoinSet` parallel parts do. A follow-up could lift
//! this the same way `dest/s3::put_multipart` does, at the cost of the per-block-range local rehash
//! `dest/s3` sidesteps entirely (S3 verifies via the server-echoed object checksum instead of a local
//! rehash — see `dest/s3` module doc).
//!
//! ## Where the code lives (coverage seam, NFR-2)
//! Everything in **this file** is pure decision logic (blob-path mapping, block-size/count planning,
//! block-id derivation, credential selection, connection-string parsing, resume bookkeeping, error
//! classification) plus the streaming *local* file read (no network — mirrors `dest/s3::read_range` /
//! `dest/http::read_source_range`) — all unit-tested with no network. Every line that opens an Azure
//! Storage connection or sends a request lives in [`client`], excluded from the coverage gate (mirrors
//! `dest/{s3,sftp,ftps,http}/client.rs`). The self-skipping integration test (`tests/azure_azurite.rs`)
//! exercises it live against Azurite when available.

pub mod client;

use std::path::Path;
use std::sync::Arc;

use serde::{Deserialize, Serialize};

use crate::config::{AzureBlockCfg, AzureEgress};
use crate::domain::ResumeState;
use crate::error::{ReplError, Result};
use crate::integrity::{Algorithm, Hasher};
use crate::state::StateStore;

use super::DestDeps;

use ggcommons::credentials::CredentialService;

const MIB: u64 = 1024 * 1024;
/// Files at or below this use a single `Put Blob`; larger use staged blocks.
const DEFAULT_THRESHOLD: u64 = 16 * MIB;
/// Default block size before auto-scaling.
const DEFAULT_BLOCK_SIZE: u64 = 16 * MIB;
/// Azure's per-block maximum (REST API version 2019-12-12+): 4000 MiB.
const AZURE_MAX_BLOCK: u64 = 4000 * MIB;
/// Azure's maximum block count per blob.
const AZURE_MAX_BLOCKS: u64 = 50_000;
/// Streaming read window (matches `dest/s3::READ_CHUNK` / `dest/http::READ_CHUNK`).
const READ_CHUNK: usize = 64 * 1024;

// ---- blob-path mapping (DESIGN §10.2 subtree preservation) --------------------------------------

/// The deterministic blob path for `relpath` under `prefix`: forward-slash join with traversal/empty
/// segments dropped (identical algorithm to `s3::object_key`/`sftp::remote_path`/`ftps::remote_path`,
/// duplicated here so this backend stays self-contained under its own feature gate).
pub fn blob_path(prefix: &str, relpath: &str) -> String {
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

// ---- block planning (DESIGN §11.1-equivalent) -----------------------------------------------------

/// Resolved staged-block parameters (config ▸ defaults, clamped to Azure's per-block ceiling).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AzureBlockParams {
    pub threshold: u64,
    pub block_size: u64,
}

impl AzureBlockParams {
    /// Resolve from the (already lenient-parsed) [`AzureBlockCfg`], applying defaults and clamping the
    /// block size into `[1, 4000 MiB]` (Azure has no minimum block size, unlike S3's 5 MiB floor).
    pub fn resolve(cfg: &AzureBlockCfg) -> Self {
        AzureBlockParams {
            threshold: cfg.threshold_bytes.unwrap_or(DEFAULT_THRESHOLD),
            block_size: cfg
                .block_size_bytes
                .unwrap_or(DEFAULT_BLOCK_SIZE)
                .clamp(1, AZURE_MAX_BLOCK),
        }
    }
}

/// The size-adaptive upload decision (mirrors `s3::UploadPlan`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UploadPlan {
    /// Single `Put Blob` (small file, or a 0-byte blob).
    Single,
    /// Staged blocks with `num_blocks` blocks of `block_size` (last block = remainder).
    Blocks { block_size: u64, num_blocks: u32 },
}

/// Decide single vs staged-blocks and, for staged blocks, the effective block size auto-scaled so
/// `num_blocks ≤ 50 000` (Azure's per-blob block-count ceiling) while respecting the 4000 MiB cap.
pub fn plan_upload(size: u64, p: &AzureBlockParams) -> UploadPlan {
    if size <= p.threshold {
        return UploadPlan::Single;
    }
    let min_by_count = size.div_ceil(AZURE_MAX_BLOCKS);
    let effective = p.block_size.max(min_by_count).clamp(1, AZURE_MAX_BLOCK);
    let num_blocks = size.div_ceil(effective) as u32;
    UploadPlan::Blocks {
        block_size: effective,
        num_blocks,
    }
}

/// Byte length of `block_number` (1-based) given the plan: `block_size` for every block but the last,
/// which carries the remainder (identical shape to `s3::part_len`).
pub fn block_len(block_number: u32, block_size: u64, num_blocks: u32, total: u64) -> u64 {
    if block_number >= num_blocks {
        total - (num_blocks as u64 - 1) * block_size
    } else {
        block_size
    }
}

/// The `[start, end]` inclusive byte range of the next block to stage, given `committed` bytes already
/// confirmed and `total` blob size (identical semantics to `http::chunk_bounds`). Panics if
/// `committed >= total` (callers only invoke this while there is work left).
pub fn block_bounds(committed: u64, block_size: u64, total: u64) -> (u64, u64) {
    assert!(committed < total, "block_bounds called with no remaining bytes");
    let end = (committed + block_size.max(1)).min(total) - 1;
    (committed, end)
}

/// The deterministic, fixed-length (16 bytes, independent of `n`) block id for block `n` (1-based).
/// All block ids in a blob's block list must be the same length before base64 encoding (the SDK
/// base64-encodes these bytes consistently for both `Put Block`'s query param and `Put Block List`'s
/// XML body — see `dest/azure/client.rs`), so a fixed-width zero-padded decimal satisfies that for
/// every `n` up to Azure's 50 000-block ceiling.
pub fn block_id_bytes(n: u32) -> Vec<u8> {
    format!("block-{n:010}").into_bytes()
}

// ---- credentials (DESIGN §11.5-equivalent) --------------------------------------------------------

/// How the Azure client authenticates. `Debug` is hand-written (not derived) so a stray `{:?}` never
/// leaks the account key (mirrors S3's `CredMode` / SFTP's `SftpAuth` / FTPS's `FtpsAuth` / HTTP's
/// `HttpAuth`).
#[derive(Clone, PartialEq, Eq)]
pub enum AzureCredMode {
    /// A shared-key credential: the storage account's primary/secondary access key.
    AccountKey(String),
}

impl std::fmt::Debug for AzureCredMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            AzureCredMode::AccountKey(_) => f.write_str("AccountKey(***redacted***)"),
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct AzureSecretJson {
    account_key: Option<String>,
    connection_string: Option<String>,
}

/// Extract `AccountKey=...` from a standard Azure `key=value;key=value;...` connection string
/// (`account`/`endpointUrl` stay authoritative from the egress config — only the key is pulled out of
/// the connection string, DESIGN §11.5-equivalent). Permanent (config error) if no `AccountKey` entry
/// is present.
pub fn account_key_from_connection_string(cs: &str) -> Result<String> {
    for pair in cs.split(';') {
        let pair = pair.trim();
        if let Some(v) = pair.strip_prefix("AccountKey=") {
            if !v.is_empty() {
                return Ok(v.to_string());
            }
        }
    }
    Err(ReplError::Permanent(
        "connection string has no 'AccountKey=' entry".to_string(),
    ))
}

/// Resolve credentials from the egress config and the (optional) credential service (DESIGN
/// §11.5-equivalent):
///
/// - `credentials: {"$secret":"name"}` → resolve as `{"accountKey":"..."}` or
///   `{"connectionString":"..."}` JSON — a missing service/secret/shape is **permanent**;
/// - else `egress.accountKey` set → ambient;
/// - else `egress.connectionString` set → ambient, `AccountKey` extracted;
/// - else → permanent (no credentials configured).
pub fn resolve_azure_credentials(
    cfg: &AzureEgress,
    creds: Option<&dyn CredentialService>,
) -> Result<AzureCredMode> {
    if let Some(v) = &cfg.credentials {
        let name = v.get("$secret").and_then(|s| s.as_str()).ok_or_else(|| {
            ReplError::Permanent(
                "egress.credentials must be {\"$secret\":\"name\"} for the azure backend".to_string(),
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
        let parsed: AzureSecretJson = serde_json::from_value(json).map_err(|e| {
            ReplError::Permanent(format!("secret '{name}' is not a valid azure credentials JSON: {e}"))
        })?;
        if let Some(key) = parsed.account_key {
            return Ok(AzureCredMode::AccountKey(key));
        }
        if let Some(cs) = parsed.connection_string {
            return Ok(AzureCredMode::AccountKey(account_key_from_connection_string(&cs)?));
        }
        return Err(ReplError::Permanent(format!(
            "secret '{name}' must contain 'accountKey' or 'connectionString'"
        )));
    }

    if let Some(key) = &cfg.account_key {
        return Ok(AzureCredMode::AccountKey(key.clone()));
    }
    if let Some(cs) = &cfg.connection_string {
        return Ok(AzureCredMode::AccountKey(account_key_from_connection_string(cs)?));
    }

    Err(ReplError::Permanent(
        "azure egress has no credentials: set accountKey, connectionString, or credentials.$secret"
            .to_string(),
    ))
}

// ---- resume token (DESIGN §10.2) ------------------------------------------------------------------

/// The staged-block resume checkpoint, serialized under the `"azure"` key of [`ResumeState::token`] —
/// same shape/semantics as `http::HttpResumeToken` (blocks are staged sequentially, so the committed
/// set is always a contiguous byte prefix — no per-block id list to persist, unlike S3's per-part
/// `S3ResumeToken` whose parallel parts can complete out of order).
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AzureResumeToken {
    pub blob: String,
    pub block_size: u64,
    pub size: u64,
    pub mtime_ms: i64,
    #[serde(default)]
    pub bytes_committed: u64,
}

impl AzureResumeToken {
    pub fn from_resume(r: &ResumeState) -> Option<AzureResumeToken> {
        r.token
            .get("azure")
            .and_then(|v| serde_json::from_value(v.clone()).ok())
    }

    pub fn to_resume(&self) -> ResumeState {
        ResumeState {
            bytes_committed: self.bytes_committed,
            token: serde_json::json!({
                "azure": serde_json::to_value(self).unwrap_or(serde_json::Value::Null)
            }),
        }
    }

    /// Whether this resume token still matches the current plan AND the current source — `blob` +
    /// `block_size` (a reconfigured `blocks.blockSizeBytes` or a different blob invalidates every
    /// persisted block boundary) + `size` + `mtime_ms` (a grown/shrunk or same-length in-place-
    /// rewritten source), mirroring `s3::S3ResumeToken::matches`/`http::HttpResumeToken::matches`.
    pub fn matches(&self, blob: &str, block_size: u64, size: u64, mtime_ms: i64) -> bool {
        self.blob == blob && self.block_size == block_size && self.size == size && self.mtime_ms == mtime_ms
    }
}

// ---- delivery handle --------------------------------------------------------------------------------

fn algo_name(algo: Algorithm) -> &'static str {
    match algo {
        Algorithm::Crc32c => "CRC32C",
        Algorithm::Sha256 => "SHA256",
    }
}

pub fn build_handle(blob: &str, algo: Algorithm) -> serde_json::Value {
    serde_json::json!({
        "blob": blob,
        "algo": algo_name(algo),
    })
}

pub fn handle_blob_path(handle: &serde_json::Value) -> Option<String> {
    handle.get("blob").and_then(|v| v.as_str()).map(String::from)
}

// ---- error classification (DESIGN §13.4) -----------------------------------------------------------

/// Classify an Azure Blob REST failure into the retry engine's [`ReplError`] taxonomy. `status` is the
/// HTTP status code when a response came back; `transport` is true for connect/TLS/timeout/dispatch
/// failures (no response at all). Mirrors `http::classify_http`'s status-band fallback (Azure Blob REST
/// uses standard HTTP status semantics: `5xx`/`429`/`408` retried, other `4xx` fail fast).
pub fn classify_azure(status: Option<u16>, transport: bool) -> ReplError {
    if transport {
        return ReplError::Transient(format!(
            "azure transport failure{}",
            status.map(|s| format!(" ({s})")).unwrap_or_default()
        ));
    }
    match status {
        Some(s) if s >= 500 || s == 429 || s == 408 => ReplError::Transient(format!("azure status {s}")),
        Some(s) if (400..500).contains(&s) => ReplError::Permanent(format!("azure status {s}")),
        Some(s) => ReplError::Transient(format!("azure status {s} (unclassified)")),
        None => ReplError::Transient("azure error (unclassified)".to_string()),
    }
}

// ---- local source reads (pure, no network) ---------------------------------------------------------

/// Read `len` bytes from `path` starting at `offset` into memory, throttling every chunk through the
/// bandwidth governor and reporting the **delta** of each chunk via `on_progress` (mirrors
/// `dest/s3::read_range`/`dest/http::read_source_range`). The in-memory buffer is bounded by `len`
/// (≤ `blocks.blockSizeBytes`).
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
            // size, rather than shipping (and letting the worker complete) a truncated blob.
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
/// re-read, no network and no bandwidth throttle (those bytes are not being re-staged), so a resumed
/// transfer's returned [`Checksum`](crate::domain::Checksum) still covers the whole file (mirrors
/// `dest/http::hash_local_prefix`/`dest/sftp`'s equivalent).
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

/// The Azure Blob backend. Holds the resolved, immutable transfer parameters; the Azure
/// `ContainerClient` (bound to `account`+`container`; the blob name varies per item and is derived
/// cheaply per-call) is built lazily on first use ([`OnceCell`]) so construction stays synchronous
/// (mirrors `S3Dest`/`HttpDest`).
///
/// [`OnceCell`]: tokio::sync::OnceCell
pub struct AzureDest {
    pub(crate) account: String,
    pub(crate) container: String,
    pub(crate) prefix: String,
    pub(crate) endpoint_url: Option<String>,
    pub(crate) creds: AzureCredMode,
    pub(crate) blocks: AzureBlockParams,
    pub(crate) algo: Algorithm,
    pub(crate) store: Option<Arc<dyn StateStore>>,
    pub(crate) client: tokio::sync::OnceCell<azure_storage_blobs::prelude::ContainerClient>,
}

impl AzureDest {
    /// Build the backend from its typed egress config and the shared [`DestDeps`] (credentials +
    /// store). Resolves the checksum algorithm, credential mode, and block params eagerly (all
    /// pure/synchronous); the network client is deferred to first transfer.
    pub fn build(cfg: &AzureEgress, deps: &DestDeps) -> Result<Self> {
        let algo = match &cfg.checksum_algorithm {
            Some(name) => Algorithm::from_name(name).ok_or_else(|| {
                ReplError::Permanent(format!("unknown egress.checksumAlgorithm '{name}'"))
            })?,
            None => Algorithm::Crc32c,
        };
        let creds = resolve_azure_credentials(cfg, deps.credentials.as_deref())?;
        Ok(AzureDest {
            account: cfg.account.clone(),
            container: cfg.container.clone(),
            prefix: cfg.prefix.clone(),
            endpoint_url: cfg.endpoint_url.clone(),
            creds,
            blocks: AzureBlockParams::resolve(&cfg.blocks),
            algo,
            store: deps.store.clone(),
            client: tokio::sync::OnceCell::new(),
        })
    }
}

// The lazy client builder and the `impl Destination for AzureDest` delegation glue live in [`client`]
// — every line makes (or immediately gates) a network round-trip, so they belong on the coverage-
// excluded side of the NFR-2 seam rather than eroding this module's counted coverage.

#[cfg(test)]
mod tests {
    use super::*;
    // The `Destination` trait impl lives in `client`; bring the trait into scope so the construction
    // tests can call `kind()`/`supports_resume()`.
    use crate::dest::Destination;

    // ---- blob_path -------------------------------------------------------------------------------

    #[test]
    fn blob_path_joins_prefix_and_relpath() {
        assert_eq!(blob_path("p", "a/b.txt"), "p/a/b.txt");
        assert_eq!(blob_path("", "a/b.txt"), "a/b.txt");
        assert_eq!(blob_path("data/out", "deep/nested/r.csv"), "data/out/deep/nested/r.csv");
    }

    #[test]
    fn blob_path_normalizes_slashes_and_strips_traversal() {
        assert_eq!(blob_path("/p/", "a/b.txt"), "p/a/b.txt", "leading+trailing slash trimmed");
        assert_eq!(blob_path("p", "a/../b/./c.txt"), "p/a/b/c.txt", "traversal + . segments dropped");
        assert!(!blob_path("p", "../../etc/passwd").contains(".."), "no traversal escapes the prefix");
        assert_eq!(blob_path("p", "//a//b//"), "p/a/b", "empty segments dropped");
        assert_eq!(blob_path("p", ""), "p", "empty relpath → prefix only");
    }

    // ---- block planning ----------------------------------------------------------------------------

    fn params(threshold: u64, block_size: u64) -> AzureBlockParams {
        AzureBlockParams { threshold, block_size }
    }

    #[test]
    fn block_params_defaults_and_clamps() {
        let d = AzureBlockParams::resolve(&AzureBlockCfg::default());
        assert_eq!(d.threshold, 16 * MIB);
        assert_eq!(d.block_size, 16 * MIB);

        // A huge block size is clamped down to the 4000 MiB ceiling.
        let big = AzureBlockParams::resolve(&AzureBlockCfg {
            threshold_bytes: None,
            block_size_bytes: Some(9000 * MIB),
        });
        assert_eq!(big.block_size, AZURE_MAX_BLOCK);

        // Unlike S3, Azure has no minimum block size — a tiny configured size is honored, not clamped
        // up.
        let small = AzureBlockParams::resolve(&AzureBlockCfg {
            threshold_bytes: Some(1024),
            block_size_bytes: Some(1024),
        });
        assert_eq!(small.block_size, 1024);
    }

    #[test]
    fn plan_single_at_and_below_threshold() {
        let p = params(16 * MIB, 8 * MIB);
        assert_eq!(plan_upload(0, &p), UploadPlan::Single, "0-byte → single");
        assert_eq!(plan_upload(1, &p), UploadPlan::Single);
        assert_eq!(plan_upload(16 * MIB, &p), UploadPlan::Single, "== threshold → single");
    }

    #[test]
    fn plan_blocks_just_over_threshold() {
        let p = params(16 * MIB, 8 * MIB);
        match plan_upload(16 * MIB + 1, &p) {
            UploadPlan::Blocks { block_size, num_blocks } => {
                assert_eq!(block_size, 8 * MIB);
                assert_eq!(num_blocks, 3); // 8, 8, ~0
            }
            other => panic!("expected blocks, got {other:?}"),
        }
    }

    #[test]
    fn plan_autoscales_block_size_to_stay_under_50000_blocks() {
        let p = params(16 * MIB, 16 * MIB);
        let size = 1024 * 1024 * MIB; // 1 TiB
        match plan_upload(size, &p) {
            UploadPlan::Blocks { block_size, num_blocks } => {
                assert!(num_blocks as u64 <= AZURE_MAX_BLOCKS, "num_blocks {num_blocks} must be ≤ 50 000");
                assert!(block_size >= size.div_ceil(AZURE_MAX_BLOCKS));
                assert!((num_blocks as u64) * block_size >= size);
            }
            other => panic!("expected blocks, got {other:?}"),
        }
    }

    #[test]
    fn plan_keeps_configured_block_size_when_it_satisfies_the_cap() {
        let p = params(16 * MIB, 20 * MIB);
        match plan_upload(100 * MIB, &p) {
            UploadPlan::Blocks { block_size, num_blocks } => {
                assert_eq!(block_size, 20 * MIB);
                assert_eq!(num_blocks, 5);
            }
            other => panic!("expected blocks, got {other:?}"),
        }
    }

    #[test]
    fn block_len_remainder_math() {
        assert_eq!(block_len(1, 5 * MIB, 3, 12 * MIB), 5 * MIB);
        assert_eq!(block_len(2, 5 * MIB, 3, 12 * MIB), 5 * MIB);
        assert_eq!(block_len(3, 5 * MIB, 3, 12 * MIB), 2 * MIB);
        assert_eq!(block_len(2, 5 * MIB, 2, 10 * MIB), 5 * MIB);
    }

    #[test]
    fn block_bounds_middle_and_last_block() {
        assert_eq!(block_bounds(0, 10, 25), (0, 9));
        assert_eq!(block_bounds(10, 10, 25), (10, 19));
        assert_eq!(block_bounds(20, 10, 25), (20, 24), "last block carries the remainder");
    }

    #[test]
    #[should_panic(expected = "no remaining bytes")]
    fn block_bounds_panics_when_nothing_left() {
        block_bounds(25, 10, 25);
    }

    #[test]
    fn block_id_bytes_are_fixed_length_and_deterministic() {
        let a = block_id_bytes(1);
        let b = block_id_bytes(49_999);
        assert_eq!(a.len(), b.len(), "block ids must be a fixed length before base64 encoding");
        assert_eq!(block_id_bytes(7), block_id_bytes(7), "deterministic");
        assert_ne!(block_id_bytes(1), block_id_bytes(2));
    }

    // ---- connection string parsing -------------------------------------------------------------

    #[test]
    fn account_key_from_connection_string_extracts_the_key() {
        let cs = "DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://127.0.0.1:10000/devstoreaccount1;";
        assert_eq!(
            account_key_from_connection_string(cs).unwrap(),
            "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw=="
        );
    }

    #[test]
    fn account_key_from_connection_string_missing_key_is_permanent() {
        let cs = "AccountName=devstoreaccount1;BlobEndpoint=http://127.0.0.1:10000/devstoreaccount1;";
        assert!(account_key_from_connection_string(cs).unwrap_err().is_permanent());
    }

    #[test]
    fn account_key_from_connection_string_empty_value_is_permanent() {
        assert!(account_key_from_connection_string("AccountKey=;").unwrap_err().is_permanent());
    }

    // ---- credentials ----------------------------------------------------------------------------

    /// A fake credential service returning canned JSON for one secret name (mirrors S3's/HTTP's fakes
    /// — the opaque `Secret` type has no public constructor outside the ggcommons credentials crate).
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

    fn base_cfg() -> AzureEgress {
        AzureEgress {
            account: "devstoreaccount1".into(),
            container: "c".into(),
            prefix: String::new(),
            endpoint_url: None,
            account_key: None,
            connection_string: None,
            credentials: None,
            blocks: AzureBlockCfg::default(),
            checksum_algorithm: None,
        }
    }

    #[test]
    fn resolve_credentials_ambient_account_key() {
        let mut cfg = base_cfg();
        cfg.account_key = Some("k".into());
        assert_eq!(
            resolve_azure_credentials(&cfg, None).unwrap(),
            AzureCredMode::AccountKey("k".into())
        );
    }

    #[test]
    fn resolve_credentials_ambient_connection_string() {
        let mut cfg = base_cfg();
        cfg.connection_string = Some("AccountName=a;AccountKey=fromcs;".into());
        assert_eq!(
            resolve_azure_credentials(&cfg, None).unwrap(),
            AzureCredMode::AccountKey("fromcs".into())
        );
    }

    #[test]
    fn resolve_credentials_none_configured_is_permanent() {
        assert!(resolve_azure_credentials(&base_cfg(), None).unwrap_err().is_permanent());
    }

    #[test]
    fn resolve_credentials_secret_account_key() {
        let creds = FakeCreds {
            name: "azure-creds".into(),
            json: Some(serde_json::json!({ "accountKey": "vault-key" })),
        };
        let mut cfg = base_cfg();
        cfg.credentials = Some(serde_json::json!({ "$secret": "azure-creds" }));
        assert_eq!(
            resolve_azure_credentials(&cfg, Some(&creds)).unwrap(),
            AzureCredMode::AccountKey("vault-key".into())
        );
    }

    #[test]
    fn resolve_credentials_secret_connection_string() {
        let creds = FakeCreds {
            name: "azure-creds".into(),
            json: Some(serde_json::json!({ "connectionString": "AccountName=a;AccountKey=vault-cs;" })),
        };
        let mut cfg = base_cfg();
        cfg.credentials = Some(serde_json::json!({ "$secret": "azure-creds" }));
        assert_eq!(
            resolve_azure_credentials(&cfg, Some(&creds)).unwrap(),
            AzureCredMode::AccountKey("vault-cs".into())
        );
    }

    #[test]
    fn resolve_credentials_secret_without_service_is_permanent() {
        let mut cfg = base_cfg();
        cfg.credentials = Some(serde_json::json!({ "$secret": "azure-creds" }));
        assert!(resolve_azure_credentials(&cfg, None).unwrap_err().is_permanent());
    }

    #[test]
    fn resolve_credentials_unknown_secret_is_permanent() {
        let creds = FakeCreds {
            name: "other".into(),
            json: Some(serde_json::json!({ "accountKey": "x" })),
        };
        let mut cfg = base_cfg();
        cfg.credentials = Some(serde_json::json!({ "$secret": "missing" }));
        assert!(resolve_azure_credentials(&cfg, Some(&creds)).unwrap_err().is_permanent());
    }

    #[test]
    fn resolve_credentials_secret_neither_field_is_permanent() {
        let creds = FakeCreds {
            name: "azure-creds".into(),
            json: Some(serde_json::json!({ "somethingElse": true })),
        };
        let mut cfg = base_cfg();
        cfg.credentials = Some(serde_json::json!({ "$secret": "azure-creds" }));
        assert!(resolve_azure_credentials(&cfg, Some(&creds)).unwrap_err().is_permanent());
    }

    #[test]
    fn resolve_credentials_secret_non_secret_json_shape_is_permanent() {
        let mut cfg = base_cfg();
        cfg.credentials = Some(serde_json::json!({ "accountKey": "inline" }));
        assert!(resolve_azure_credentials(&cfg, None).unwrap_err().is_permanent());
    }

    #[test]
    fn azure_cred_mode_debug_redacts_the_key() {
        let mode = AzureCredMode::AccountKey("TOP_SECRET_KEY".into());
        let dbg = format!("{mode:?}");
        assert!(!dbg.contains("TOP_SECRET_KEY"), "account key leaked: {dbg}");
        assert!(dbg.contains("***redacted***"));
    }

    // ---- resume token --------------------------------------------------------------------------

    #[test]
    fn azure_resume_token_round_trips_via_resume_state() {
        let tok = AzureResumeToken {
            blob: "in/a.bin".into(),
            block_size: 5 * MIB,
            size: 999,
            mtime_ms: 12345,
            bytes_committed: 500,
        };
        let rs = tok.to_resume();
        assert_eq!(rs.bytes_committed, 500);
        let json = serde_json::to_string(&rs).unwrap();
        let back: ResumeState = serde_json::from_str(&json).unwrap();
        let tok2 = AzureResumeToken::from_resume(&back).expect("token present");
        assert_eq!(tok2, tok);
    }

    #[test]
    fn azure_resume_token_absent_or_wrong_shape_is_none() {
        assert!(AzureResumeToken::from_resume(&ResumeState::default()).is_none());
        let s3_shape = ResumeState {
            bytes_committed: 1,
            token: serde_json::json!({ "s3": { "uploadId": "x" } }),
        };
        assert!(AzureResumeToken::from_resume(&s3_shape).is_none());
    }

    #[test]
    fn azure_resume_token_matches_guards_blob_blocksize_size_and_mtime() {
        let tok = AzureResumeToken {
            blob: "in/a.bin".into(),
            block_size: 5 * MIB,
            size: 100,
            mtime_ms: 42,
            bytes_committed: 50,
        };
        assert!(tok.matches("in/a.bin", 5 * MIB, 100, 42));
        assert!(!tok.matches("in/other.bin", 5 * MIB, 100, 42), "blob change → discard resume");
        assert!(!tok.matches("in/a.bin", 8 * MIB, 100, 42), "block-size change → discard resume");
        assert!(!tok.matches("in/a.bin", 5 * MIB, 200, 42), "size change → discard resume");
        assert!(!tok.matches("in/a.bin", 5 * MIB, 100, 99), "mtime change → discard resume");
    }

    // ---- delivery handle ------------------------------------------------------------------------

    #[test]
    fn handle_round_trips_blob_path() {
        let h = build_handle("in/a.bin", Algorithm::Crc32c);
        assert_eq!(handle_blob_path(&h).as_deref(), Some("in/a.bin"));
        assert_eq!(h.get("algo").and_then(|v| v.as_str()), Some("CRC32C"));
    }

    #[test]
    fn handle_blob_path_absent_is_none() {
        assert_eq!(handle_blob_path(&serde_json::json!({})), None);
    }

    // ---- error classification -------------------------------------------------------------------

    #[test]
    fn classify_transport_is_transient() {
        assert!(classify_azure(None, true).is_transient());
        assert!(classify_azure(Some(401), true).is_transient(), "transport wins over a 4xx code");
    }

    #[test]
    fn classify_5xx_and_throttle_codes_are_transient() {
        for c in [500, 502, 503, 504, 429, 408] {
            assert!(classify_azure(Some(c), false).is_transient(), "{c} should be transient");
        }
    }

    #[test]
    fn classify_other_4xx_is_permanent() {
        for c in [400, 401, 403, 404, 409, 412] {
            assert!(classify_azure(Some(c), false).is_permanent(), "{c} should be permanent");
        }
    }

    #[test]
    fn classify_unclassified_status_and_bare_failure_are_transient() {
        assert!(classify_azure(Some(302), false).is_transient());
        assert!(classify_azure(None, false).is_transient());
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

    // ---- AzureDest::build --------------------------------------------------------------------------

    fn deps_with_key() -> DestDeps {
        DestDeps::default()
    }

    #[test]
    fn build_defaults_to_crc32c() {
        let mut cfg = base_cfg();
        cfg.account_key = Some("k".into());
        let dest = AzureDest::build(&cfg, &deps_with_key()).unwrap();
        assert_eq!(dest.kind(), "azure");
        assert!(dest.supports_resume());
        assert_eq!(dest.algo, Algorithm::Crc32c);
        assert_eq!(dest.blocks.threshold, 16 * MIB);
    }

    #[test]
    fn build_reads_sha256_and_block_overrides() {
        let mut cfg = base_cfg();
        cfg.account_key = Some("k".into());
        cfg.checksum_algorithm = Some("SHA-256".into());
        cfg.blocks = AzureBlockCfg {
            threshold_bytes: Some(1024),
            block_size_bytes: Some(2048),
        };
        let dest = AzureDest::build(&cfg, &deps_with_key()).unwrap();
        assert_eq!(dest.algo, Algorithm::Sha256);
        assert_eq!(dest.blocks.threshold, 1024);
        assert_eq!(dest.blocks.block_size, 2048);
    }

    #[test]
    fn build_rejects_unknown_checksum_algorithm() {
        let mut cfg = base_cfg();
        cfg.account_key = Some("k".into());
        cfg.checksum_algorithm = Some("MD5".into());
        assert!(AzureDest::build(&cfg, &deps_with_key())
            .err()
            .expect("unknown algorithm rejected")
            .is_permanent());
    }

    #[test]
    fn build_propagates_credential_resolution_failure() {
        // No accountKey/connectionString/$secret configured → permanent.
        assert!(AzureDest::build(&base_cfg(), &deps_with_key())
            .err()
            .expect("missing credentials rejected")
            .is_permanent());
    }
}
