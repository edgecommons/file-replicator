//! # file-replicator — FTPS control/data-connection seam (coverage-excluded, NFR-2)
//!
//! Every function here opens a real TCP/TLS connection or performs an FTP command round-trip
//! (`suppaftp`'s synchronous `RustlsFtpStream`: connect, `AUTH TLS`, `USER`/`PASS`, `MKD`, `REST`,
//! `STOR`, `RNFR`/`RNTO`-equivalent rename, `DELE`, `SIZE`). These lines are unreachable in CI/unit
//! runs, so this file is **excluded from the coverage gate** via
//! `--ignore-filename-regex '(testutil|dest.(s3|sftp|ftps).client)\.rs'`. There is no live
//! integration test for this backend (module scope note in [`super`]); all *decision* logic lives in
//! [`super`] and is unit-tested there.
//!
//! ## Blocking bridge
//! `suppaftp`'s synchronous API takes `&mut self` for every operation and is not internally
//! synchronized, so each [`Destination`] call opens (and closes) its own connection inside
//! `tokio::task::spawn_blocking` — see the [`super::FtpsDest`] doc comment for the tradeoff. The
//! bandwidth governor is `async`; [`tokio::runtime::Handle::block_on`] bridges a per-chunk
//! `bw.throttle(..)` wait back into the blocking thread (that thread is off the async reactor, so this
//! does not stall other tasks).

use std::io::{Read, Seek, SeekFrom, Write};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use async_trait::async_trait;
use suppaftp::{FtpError, Mode, RustlsConnector, RustlsFtpStream, Status};

use crate::config::Verify;
use crate::dest::Destination;
use crate::domain::{Checksum, Delivered, ProgressSink, ResumeState, WorkItem};
use crate::error::{ReplError, Result};
use crate::integrity::Hasher;
use crate::ratelimit::Bandwidth;
use crate::state::StateStore;

use super::{
    build_handle, classify_ftps, handle_remote_path, parent_dir_chain, remote_path, temp_path,
    FtpsAuth, FtpsDest, FtpsResumeToken,
};

const CHUNK: usize = 64 * 1024;
const CHECKPOINT_EVERY: u64 = 1024 * 1024;

/// Owned, `'static` connection parameters — cloned out of [`FtpsDest`] so they can move into
/// `spawn_blocking`.
#[derive(Clone)]
struct ConnParams {
    host: String,
    port: u16,
    auth: FtpsAuth,
    passive: bool,
}

impl From<&FtpsDest> for ConnParams {
    fn from(d: &FtpsDest) -> Self {
        ConnParams {
            host: d.host.clone(),
            port: d.port,
            auth: d.auth.clone(),
            passive: d.passive,
        }
    }
}

#[async_trait]
impl Destination for FtpsDest {
    fn kind(&self) -> &'static str {
        "ftps"
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

// ---- connection ------------------------------------------------------------------------------------

/// Wrap the safe-default rustls [`ClientConfig`](rustls::ClientConfig) (built + documented in
/// [`super::build_rustls_client_config`]) in suppaftp's `RustlsConnector`. The config uses the default
/// WebPKI verifier (cert chain + hostname), never a `dangerous()`/accept-invalid path — see that fn's
/// SECURITY note.
fn build_rustls_connector() -> Result<RustlsConnector> {
    let config = super::build_rustls_client_config()?;
    Ok(RustlsConnector::from(Arc::new(config)))
}

/// Connect, upgrade to explicit `AUTH TLS`, log in, and select passive/active mode + binary transfer
/// type.
fn connect_and_login(p: &ConnParams) -> Result<RustlsFtpStream> {
    let addr = format!("{}:{}", p.host, p.port);
    let mut stream = RustlsFtpStream::connect(&addr).map_err(|e| map_ftp(&e, true))?;
    if !p.passive {
        stream.set_mode(Mode::Active);
    }
    let connector = build_rustls_connector()?;
    // SECURITY (NFR-7): pass the CONFIGURED host as the TLS `ServerName`, so the default WebPKI
    // verifier checks the server certificate's identity against `egress.host` (hostname verification),
    // in addition to validating the chain against the system trust roots.
    let mut stream = stream
        .into_secure(connector, &p.host)
        .map_err(|e| map_ftp(&e, false))?;
    stream
        .login(&p.auth.username, &p.auth.password)
        .map_err(|e| map_ftp(&e, false))?;
    stream
        .transfer_type(suppaftp::types::FileType::Binary)
        .map_err(|e| map_ftp(&e, false))?;
    Ok(stream)
}

// ---- deliver ---------------------------------------------------------------------------------------

async fn deliver(
    dest: &FtpsDest,
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
    let remote = remote_path(&dest.base_dir, &item.relpath);
    let temp = temp_path(&remote, &dest.temp_suffix);

    let reusable = resume
        .as_ref()
        .and_then(FtpsResumeToken::from_resume)
        .filter(|t| t.matches(&temp, size, mtime_ms));

    let params = ConnParams::from(dest);
    let src_path = item.abs_source.clone();
    let algo = dest.algo;
    let bw = bw.clone();
    let progress = progress.clone();
    let rt_handle = tokio::runtime::Handle::current();
    let store = dest.store.clone();
    let instance = item.instance.clone();
    let relpath = item.relpath.clone();
    let temp_c = temp.clone();
    let remote_c = remote.clone();

    // Cooperative cancellation for the (otherwise non-cancellable) `spawn_blocking` upload. A
    // window-close `pauseResume` / shutdown aborts the worker future, which only *detaches* the blocking
    // task (spawn_blocking work can't be aborted) — it would otherwise keep streaming AND could still
    // call `publish` + `clear_resume` AFTER `pause_revert` re-armed the item, wiping the checkpoint it
    // just preserved (the next window would then restart from zero). This guard is a local of the outer
    // async fn: when that future is dropped on abort, `Drop` flips the flag, and the blocking loop bails
    // at its next chunk boundary BEFORE finalize/publish/clear_resume, so the paused checkpoint survives.
    let cancel = Arc::new(AtomicBool::new(false));
    struct CancelOnDrop(Arc<AtomicBool>);
    impl Drop for CancelOnDrop {
        fn drop(&mut self) {
            self.0.store(true, Ordering::SeqCst);
        }
    }
    let _cancel_guard = CancelOnDrop(cancel.clone());

    let (checksum, total) = tokio::task::spawn_blocking(move || -> Result<(Checksum, u64)> {
        let mut stream = connect_and_login(&params)?;

        for dir in parent_dir_chain(&remote_c) {
            let _ = stream.mkdir(&dir);
        }

        let mut committed: u64 = 0;
        if let Some(tok) = &reusable {
            if let Ok(sz) = stream.size(&temp_c) {
                committed = (sz as u64).min(tok.bytes_committed).min(size);
            }
        }

        let mut src = std::fs::File::open(&src_path).map_err(ReplError::classify_io)?;
        let mut hasher = Hasher::new(algo);
        if committed > 0 {
            hash_prefix_sync(&src_path, committed, &mut hasher)?;
            src.seek(SeekFrom::Start(committed))
                .map_err(ReplError::classify_io)?;
            stream
                .resume_transfer(committed as usize)
                .map_err(|e| map_ftp(&e, false))?;
        }

        persist_checkpoint(&store, &instance, &relpath, &temp_c, size, mtime_ms, committed);

        let mut data_stream = stream.put_with_stream(&temp_c).map_err(|e| map_ftp(&e, false))?;
        let mut total = committed;
        let mut since_checkpoint: u64 = 0;
        let mut buf = vec![0u8; CHUNK];
        loop {
            if cancel.load(Ordering::SeqCst) {
                // The worker future was dropped (window-close pause / shutdown). Bail cooperatively
                // BEFORE finalize/publish/clear_resume so the persisted checkpoint (which `pause_revert`
                // relies on) is left intact and the next window resumes from it, not from zero.
                return Err(ReplError::Transient(
                    "ftps upload cancelled mid-transfer (window close / shutdown)".to_string(),
                ));
            }
            let n = src.read(&mut buf).map_err(ReplError::classify_io)?;
            if n == 0 {
                break;
            }
            rt_handle.block_on(bw.throttle(n as u64));
            data_stream
                .write_all(&buf[..n])
                .map_err(ReplError::classify_io)?;
            hasher.update(&buf[..n]);
            total += n as u64;
            since_checkpoint += n as u64;
            progress.report(total);
            if since_checkpoint >= CHECKPOINT_EVERY {
                since_checkpoint = 0;
                persist_checkpoint(&store, &instance, &relpath, &temp_c, size, mtime_ms, total);
            }
        }
        stream
            .finalize_put_stream(data_stream)
            .map_err(|e| map_ftp(&e, false))?;

        if total != size {
            return Err(ReplError::Transient(format!(
                "ftps source size changed during transfer: expected {size}, streamed {total}"
            )));
        }

        publish(&mut stream, &temp_c, &remote_c)?;
        if let Some(st) = &store {
            if let Err(e) = st.clear_resume(&instance, &relpath, "ftps") {
                tracing::warn!(relpath = %relpath, error = %e, "clear ftps resume checkpoint failed");
            }
        }

        Ok((hasher.finish(), total))
    })
    .await
    .map_err(|e| ReplError::Transient(format!("ftps blocking task join: {e}")))??;

    let handle = build_handle(&remote, dest.algo);
    Ok(Delivered {
        bytes: total,
        checksum,
        handle,
    })
}

/// Hash (without uploading) the first `len` bytes of `path` — the already-committed prefix on a
/// resume — so the whole-file checksum stays correct without re-sending committed bytes.
fn hash_prefix_sync(path: &std::path::Path, len: u64, hasher: &mut Hasher) -> Result<()> {
    let mut f = std::fs::File::open(path).map_err(ReplError::classify_io)?;
    let mut remaining = len;
    let mut buf = vec![0u8; CHUNK];
    while remaining > 0 {
        let want = remaining.min(CHUNK as u64) as usize;
        let n = f.read(&mut buf[..want]).map_err(ReplError::classify_io)?;
        if n == 0 {
            break;
        }
        hasher.update(&buf[..n]);
        remaining -= n as u64;
    }
    Ok(())
}

/// Publish the temp file at its final `remote` path. Plain `RNFR`/`RNTO` (like SFTP's
/// `SSH_FXP_RENAME`) fails on most servers when the destination already exists, so an idempotent
/// re-delivery (FR-REL-4-equivalent) removes the stale target first and retries once.
fn publish(stream: &mut RustlsFtpStream, temp: &str, remote: &str) -> Result<()> {
    match stream.rename(temp, remote) {
        Ok(()) => Ok(()),
        Err(_) => {
            let _ = stream.rm(remote);
            stream.rename(temp, remote).map_err(|e| map_ftp(&e, false))
        }
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
    temp: &str,
    size: u64,
    mtime_ms: i64,
    committed: u64,
) {
    if let Some(st) = store {
        let tok = FtpsResumeToken {
            temp_path: temp.to_string(),
            size,
            mtime_ms,
            bytes_committed: committed,
        };
        if let Err(e) = st.save_resume(instance, relpath, "ftps", &tok.to_resume()) {
            tracing::warn!(relpath = %relpath, error = %e, "persist ftps resume checkpoint failed");
        }
    }
}

// ---- verify (before the source side effect, DESIGN §13.1) ------------------------------------------

async fn verify(dest: &FtpsDest, item: &WorkItem, delivered: &Delivered, policy: Verify) -> Result<()> {
    let remote = handle_remote_path(&delivered.handle)
        .unwrap_or_else(|| remote_path(&dest.base_dir, &item.relpath));
    let params = ConnParams::from(dest);
    let algo = dest.algo;
    let bytes = delivered.bytes;
    let checksum = delivered.checksum.clone();

    tokio::task::spawn_blocking(move || -> Result<()> {
        let mut stream = connect_and_login(&params)?;
        let actual_size = stream.size(&remote).map_err(|e| map_ftp(&e, false))? as u64;
        match policy {
            Verify::None => Ok(()),
            Verify::Size => crate::integrity::verify_size(bytes, actual_size),
            Verify::Checksum => {
                crate::integrity::verify_size(bytes, actual_size)?;
                // Stream the download through the hasher in bounded chunks (NFR-3) rather than
                // `retr_as_buffer`, which reads the whole remote file into a `Vec` before hashing (peak
                // RSS would scale with maxConcurrentFiles × file size). `retr` hands us the data
                // connection as a `&mut dyn Read` we fold into the hasher a chunk at a time.
                let mut hasher = Hasher::new(algo);
                stream
                    .retr(&remote, |reader| {
                        let mut chunk = vec![0u8; CHUNK];
                        loop {
                            let n = reader.read(&mut chunk).map_err(FtpError::ConnectionError)?;
                            if n == 0 {
                                break;
                            }
                            hasher.update(&chunk[..n]);
                        }
                        Ok(())
                    })
                    .map_err(|e| map_ftp(&e, false))?;
                crate::integrity::verify_checksum(&checksum, &hasher.finish())
            }
        }
    })
    .await
    .map_err(|e| ReplError::Transient(format!("ftps blocking task join: {e}")))?
}

// ---- abort (give-up cleanup) -------------------------------------------------------------------

async fn abort(dest: &FtpsDest, _item: &WorkItem, resume: &ResumeState) -> Result<()> {
    let Some(token) = FtpsResumeToken::from_resume(resume) else {
        return Ok(());
    };
    let params = ConnParams::from(dest);
    tokio::task::spawn_blocking(move || -> Result<()> {
        let mut stream = connect_and_login(&params)?;
        match stream.rm(&token.temp_path) {
            Ok(()) => Ok(()),
            Err(e) if is_not_found(&e) => Ok(()),
            Err(e) => Err(map_ftp(&e, false)),
        }
    })
    .await
    .map_err(|e| ReplError::Transient(format!("ftps blocking task join: {e}")))?
}

// ---- error mapping -------------------------------------------------------------------------------

/// Map an `FtpError` to the retry engine's [`ReplError`] via the unit-tested [`classify_ftps`].
/// `transport` is true for a bare TCP connect failure (no FTP reply to classify by).
fn map_ftp(e: &FtpError, transport: bool) -> ReplError {
    classify_ftps(ftp_reply_code(e), transport || matches!(e, FtpError::ConnectionError(_)))
}

fn ftp_reply_code(e: &FtpError) -> Option<u32> {
    match e {
        FtpError::UnexpectedResponse(r) => Some(r.status.code()),
        _ => None,
    }
}

fn is_not_found(e: &FtpError) -> bool {
    ftp_reply_code(e) == Some(Status::FileUnavailable.code())
}
