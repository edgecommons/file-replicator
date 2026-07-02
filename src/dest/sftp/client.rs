//! # file-replicator — SFTP SSH/protocol seam (coverage-excluded, NFR-2)
//!
//! Every function here opens a real TCP/SSH connection or performs an SFTP protocol round-trip
//! (`russh` handshake + auth, `russh-sftp` `open`/`write`/`rename`/`mkdir`/`metadata`). These lines
//! are unreachable in CI/unit runs, so this file is **excluded from the coverage gate** via
//! `--ignore-filename-regex '(testutil|dest.(s3|sftp).client)\.rs'` — the same convention
//! `dest/s3/client.rs` uses. It is exercised live by the self-skipping `tests/sftp_atmoz.rs`
//! integration test; all *decision* logic it relies on lives in [`super`] and is unit-tested there.

use std::path::Path;
use std::sync::Arc;

use async_trait::async_trait;
use russh::client::{self, Handle};
use russh::keys::{decode_secret_key, load_secret_key, PrivateKeyWithHashAlg};
use russh_sftp::client::error::Error as SftpError;
use russh_sftp::client::SftpSession;
use russh_sftp::protocol::{OpenFlags, StatusCode};
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};

use crate::config::Verify;
use crate::dest::Destination;
use crate::domain::{Delivered, ProgressSink, ResumeState, WorkItem};
use crate::error::{ReplError, Result};
use crate::integrity::Hasher;
use crate::ratelimit::Bandwidth;

use super::{
    build_handle, check_host_key, classify_sftp, handle_remote_path, parent_dir_chain, remote_path,
    temp_path, HostKeyCheck, SftpAuth, SftpDest, SftpKeySource, SftpResumeToken,
};

/// Streaming read/write chunk size (matches `s3::READ_CHUNK` / `local.rs::COPY_CHUNK`).
const CHUNK: usize = 64 * 1024;
/// Persist the resume checkpoint at most this often, bounding durable-store write pressure on a large
/// file (mirrors S3's per-completed-part checkpoint, just time/byte- rather than part-boundary-gated).
const CHECKPOINT_EVERY: u64 = 1024 * 1024;

// ---- lazy connection + trait glue -------------------------------------------------------------------

/// An established SSH session + its SFTP subsystem channel. The `russh` handle is kept alive for the
/// SFTP session's whole lifetime (dropping it would close the transport); it is never read again after
/// [`connect`] hands the connection off.
pub struct SftpConn {
    #[allow(
        dead_code,
        reason = "kept alive only to hold the SSH transport open for `sftp`'s lifetime"
    )]
    ssh: Handle<ClientHandler>,
    sftp: SftpSession,
}

impl SftpDest {
    /// Return a live SFTP session, establishing (and caching) one if none is current.
    ///
    /// Unlike a plain `OnceCell` (which caches the FIRST session forever and can never recover once the
    /// transport dies — the FR-REL-7 long-outage failure the review flagged: after a server restart /
    /// NAT-idle / multi-hour outage every subsequent `russh_sftp` call returns `connection_lost`, which
    /// classifies Transient, so the retry engine keeps retrying but each retry gets the SAME dead
    /// session, forever), the session lives behind a `Mutex<Option<Arc<..>>>` so a failed session can
    /// be invalidated ([`reset_conn`]) and the next attempt re-establishes it, resuming from the
    /// persisted offset as designed. The returned `Arc` lets the caller drive the transfer without
    /// holding the mutex.
    async fn conn(&self) -> Result<Arc<SftpConn>> {
        let mut guard = self.conn.lock().await;
        if let Some(c) = guard.as_ref() {
            return Ok(c.clone());
        }
        let fresh = Arc::new(connect(self).await?);
        *guard = Some(fresh.clone());
        Ok(fresh)
    }

    /// Drop the cached session **iff** it is still the one that just failed (pointer identity), so a
    /// concurrent task that already reconnected keeps its fresh session (no reconnect churn). The next
    /// [`conn`](Self::conn) then re-establishes the transport.
    async fn reset_conn(&self, failed: &Arc<SftpConn>) {
        let mut guard = self.conn.lock().await;
        if guard.as_ref().is_some_and(|c| Arc::ptr_eq(c, failed)) {
            *guard = None;
        }
    }

    /// A transient error is (or may be) a dead transport, so drop the cached session — the retry the
    /// engine schedules then reconnects instead of hitting the same corpse. A permanent error keeps the
    /// session (the transport is fine; only this operation is doomed).
    async fn invalidate_on_transient<T>(&self, conn: &Arc<SftpConn>, r: &Result<T>) {
        if matches!(r, Err(e) if e.is_transient()) {
            self.reset_conn(conn).await;
        }
    }
}

#[async_trait]
impl Destination for SftpDest {
    fn kind(&self) -> &'static str {
        "sftp"
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
        let conn = self.conn().await?;
        let r = deliver(self, &conn.sftp, item, resume, progress, bw).await;
        self.invalidate_on_transient(&conn, &r).await;
        r
    }

    async fn verify(&self, item: &WorkItem, delivered: &Delivered, policy: Verify) -> Result<()> {
        let conn = self.conn().await?;
        let r = verify(self, &conn.sftp, item, delivered, policy).await;
        self.invalidate_on_transient(&conn, &r).await;
        r
    }

    async fn abort(&self, item: &WorkItem, resume: &ResumeState) -> Result<()> {
        let conn = self.conn().await?;
        let r = abort(&conn.sftp, item, resume).await;
        self.invalidate_on_transient(&conn, &r).await;
        r
    }
}

// ---- host-key checking ----------------------------------------------------------------------------

struct ClientHandler {
    pinned: Option<String>,
    /// The `egress.insecureAcceptAnyHostKey` opt-in: accept any key when none is pinned (fail-open).
    insecure_accept_any: bool,
    host: String,
}

impl client::Handler for ClientHandler {
    type Error = russh::Error;

    /// Host-key verification (SECURITY, NFR-7). The accept/reject **decision** lives in the
    /// unit-tested [`check_host_key`]; this handler only renders the presented key to its OpenSSH form
    /// and maps the outcome onto russh's boolean (returning `Ok(false)` aborts the handshake with
    /// `russh::Error::UnknownKey`, which [`classify_sftp`] tags `host_key_mismatch` → **PERMANENT**, so
    /// the retry engine fails fast instead of looping on a doomed handshake).
    ///
    /// Fail-closed default: with no pinned `egress.hostKey` and no `egress.insecureAcceptAnyHostKey`
    /// opt-in, the connection is REFUSED with an actionable error rather than trusting any key.
    async fn check_server_key(
        &mut self,
        server_public_key: &russh::keys::PublicKey,
    ) -> std::result::Result<bool, Self::Error> {
        let presented = server_public_key
            .to_openssh()
            .map_err(|e| russh::Error::IO(std::io::Error::other(e.to_string())))?;
        match check_host_key(self.pinned.as_deref(), &presented, self.insecure_accept_any) {
            HostKeyCheck::Accept => {
                if self.pinned.is_none() {
                    // Reached only via the explicit insecureAcceptAnyHostKey opt-in — announce loudly.
                    tracing::warn!(
                        host = %self.host,
                        "sftp host key NOT verified — egress.insecureAcceptAnyHostKey is set, so the \
                         server's key is accepted ON TRUST. This disables man-in-the-middle protection \
                         (an active attacker could intercept this session and, under password auth, \
                         capture the credential). Pin egress.hostKey to enable verification."
                    );
                }
                Ok(true)
            }
            HostKeyCheck::RejectUnpinned => {
                tracing::error!(
                    host = %self.host,
                    "sftp connection REFUSED (fail-closed): the server's host key is not verified and \
                     no egress.hostKey is pinned. Pin the server's public key via egress.hostKey (an \
                     OpenSSH 'algo base64' known-hosts line), or — to accept any key on trust (INSECURE, \
                     disables MITM protection) — set egress.insecureAcceptAnyHostKey: true."
                );
                Ok(false)
            }
            HostKeyCheck::RejectMismatch => {
                tracing::error!(
                    host = %self.host,
                    "sftp host key MISMATCH — the server presented a key that does not match the pinned \
                     egress.hostKey; refusing to connect (possible man-in-the-middle)."
                );
                Ok(false)
            }
        }
    }
}

// ---- connection ------------------------------------------------------------------------------------

/// Handshake the SSH transport, authenticate, open the `sftp` subsystem channel, and start the SFTP
/// session.
async fn connect(dest: &SftpDest) -> Result<SftpConn> {
    let cfg = Arc::new(client::Config::default());
    let handler = ClientHandler {
        pinned: dest.host_key.clone(),
        insecure_accept_any: dest.insecure_accept_any_host_key,
        host: dest.host.clone(),
    };
    let mut handle = client::connect(cfg, (dest.host.as_str(), dest.port), handler)
        .await
        .map_err(|e| map_ssh(&e))?;

    let auth = match &dest.auth {
        SftpAuth::Password { username, password } => handle
            .authenticate_password(username, password)
            .await
            .map_err(|e| map_ssh(&e))?,
        SftpAuth::PrivateKey {
            username,
            key,
            passphrase,
        } => {
            let private = load_private_key(key, passphrase.as_deref())?;
            let with_hash = PrivateKeyWithHashAlg::new(Arc::new(private), None);
            handle
                .authenticate_publickey(username, with_hash)
                .await
                .map_err(|e| map_ssh(&e))?
        }
    };
    if !auth.success() {
        return Err(ReplError::Permanent("sftp authentication failed".to_string()));
    }

    let channel = handle.channel_open_session().await.map_err(|e| map_ssh(&e))?;
    channel
        .request_subsystem(true, "sftp")
        .await
        .map_err(|e| map_ssh(&e))?;
    let stream = channel.into_stream();
    let sftp = SftpSession::new(stream)
        .await
        .map_err(|e| map_sftp(&e, false))?;

    Ok(SftpConn { ssh: handle, sftp })
}

fn load_private_key(src: &SftpKeySource, passphrase: Option<&str>) -> Result<russh::keys::PrivateKey> {
    match src {
        SftpKeySource::Path(p) => load_secret_key(p, passphrase)
            .map_err(|e| ReplError::Permanent(format!("sftp private key '{p}': {e}"))),
        SftpKeySource::Inline(pem) => decode_secret_key(pem, passphrase)
            .map_err(|e| ReplError::Permanent(format!("sftp private key (inline): {e}"))),
    }
}

// ---- deliver ---------------------------------------------------------------------------------------

/// Stream the source into a hidden temp file (resuming from a persisted offset when the temp file and
/// checkpoint agree), then publish with a `rename` (best-effort overwrite fallback).
async fn deliver(
    dest: &SftpDest,
    sess: &SftpSession,
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

    for dir in parent_dir_chain(&remote) {
        // Best-effort `mkdir -p`: "already exists" is the expected steady-state after the first file
        // in a directory, and any real failure (e.g. permission denied) surfaces below at `open`.
        let _ = sess.create_dir(dir.as_str()).await;
    }

    let reusable = resume
        .as_ref()
        .and_then(SftpResumeToken::from_resume)
        .filter(|t| t.matches(&temp, size, mtime_ms));

    let mut committed: u64 = 0;
    if let Some(tok) = &reusable {
        if let Ok(remote_meta) = sess.metadata(temp.as_str()).await {
            committed = remote_meta.size.unwrap_or(0).min(tok.bytes_committed).min(size);
        }
    }

    let mut flags = OpenFlags::CREATE | OpenFlags::WRITE;
    if committed == 0 {
        flags |= OpenFlags::TRUNCATE;
    }
    let mut file = sess
        .open_with_flags(temp.as_str(), flags)
        .await
        .map_err(|e| map_sftp(&e, false))?;

    // Write-ahead the checkpoint before streaming, so a crash right after open still resumes.
    persist_checkpoint(dest, item, &temp, size, mtime_ms, committed);

    let mut hasher = Hasher::new(dest.algo);

    // Re-hash the already-committed prefix locally (no re-upload) to keep the whole-file checksum
    // honest for `Verify::Checksum` (DESIGN §13.1 single-pass integrity, extended across a resume).
    if committed > 0 {
        hash_prefix(&item.abs_source, committed, &mut hasher).await?;
        file.seek(std::io::SeekFrom::Start(committed))
            .await
            .map_err(ReplError::classify_io)?;
    }

    let mut src = tokio::fs::File::open(&item.abs_source)
        .await
        .map_err(ReplError::classify_io)?;
    if committed > 0 {
        src.seek(std::io::SeekFrom::Start(committed))
            .await
            .map_err(ReplError::classify_io)?;
    }

    let mut total = committed;
    let mut since_checkpoint: u64 = 0;
    let mut buf = vec![0u8; CHUNK];
    loop {
        let n = src.read(&mut buf).await.map_err(ReplError::classify_io)?;
        if n == 0 {
            break;
        }
        bw.throttle(n as u64).await;
        file.write_all(&buf[..n]).await.map_err(ReplError::classify_io)?;
        hasher.update(&buf[..n]);
        total += n as u64;
        since_checkpoint += n as u64;
        progress.report(total);
        if since_checkpoint >= CHECKPOINT_EVERY {
            since_checkpoint = 0;
            persist_checkpoint(dest, item, &temp, size, mtime_ms, total);
        }
    }

    if total != size {
        // The source shrank/grew mid-transfer (metadata read at the start vs. what was actually
        // streamed). Same anti-truncation guard as `s3::read_range`: fail transient so the next
        // attempt re-plans against the current size rather than publishing a short/long object.
        return Err(ReplError::Transient(format!(
            "sftp source size changed during transfer: expected {size}, streamed {total}"
        )));
    }

    file.flush().await.map_err(ReplError::classify_io)?;
    if dest.fsync {
        // Best-effort: `File::sync_all` no-ops when the server does not advertise `fsync@openssh.com`.
        file.sync_all()
            .await
            .map_err(|e| ReplError::Transient(format!("sftp fsync: {e}")))?;
    }
    file.shutdown().await.map_err(ReplError::classify_io)?;
    drop(file);

    publish(sess, &temp, &remote).await?;
    clear_checkpoint(dest, item);

    let checksum = hasher.finish();
    let handle = build_handle(&remote, dest.algo);
    Ok(Delivered {
        bytes: size,
        checksum,
        handle,
    })
}

/// Hash (without uploading) the first `len` bytes of `path` — the already-committed prefix on a
/// resume — so the whole-file checksum stays correct without re-sending committed bytes.
async fn hash_prefix(path: &Path, len: u64, hasher: &mut Hasher) -> Result<()> {
    let mut f = tokio::fs::File::open(path).await.map_err(ReplError::classify_io)?;
    let mut remaining = len;
    let mut buf = vec![0u8; CHUNK];
    while remaining > 0 {
        let want = remaining.min(CHUNK as u64) as usize;
        let n = f.read(&mut buf[..want]).await.map_err(ReplError::classify_io)?;
        if n == 0 {
            break; // source shorter than expected; the outer size check catches this
        }
        hasher.update(&buf[..n]);
        remaining -= n as u64;
    }
    Ok(())
}

/// Publish the temp file at its final `remote` path. Plain `SSH_FXP_RENAME` (unlike POSIX `rename(2)`)
/// fails on most servers when the destination already exists, so an idempotent re-delivery
/// (FR-REL-4-equivalent) removes the stale target first and retries once.
async fn publish(sess: &SftpSession, temp: &str, remote: &str) -> Result<()> {
    match sess.rename(temp, remote).await {
        Ok(()) => Ok(()),
        Err(_) => {
            let _ = sess.remove_file(remote).await;
            sess.rename(temp, remote).await.map_err(|e| map_sftp(&e, false))
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

fn persist_checkpoint(dest: &SftpDest, item: &WorkItem, temp: &str, size: u64, mtime_ms: i64, committed: u64) {
    if let Some(st) = &dest.store {
        let tok = SftpResumeToken {
            temp_path: temp.to_string(),
            size,
            mtime_ms,
            bytes_committed: committed,
        };
        if let Err(e) = st.save_resume(&item.instance, &item.relpath, "sftp", &tok.to_resume()) {
            tracing::warn!(relpath = %item.relpath, error = %e, "persist sftp resume checkpoint failed");
        }
    }
}

fn clear_checkpoint(dest: &SftpDest, item: &WorkItem) {
    if let Some(st) = &dest.store {
        if let Err(e) = st.clear_resume(&item.instance, &item.relpath, "sftp") {
            tracing::warn!(relpath = %item.relpath, error = %e, "clear sftp resume checkpoint failed");
        }
    }
}

// ---- verify (before the source side effect, DESIGN §13.1) ------------------------------------------

/// Verify the delivered object per `policy`. SFTP has no native content-checksum echo, so
/// `Verify::Checksum` re-hashes the remote file over the wire (downloads it) and compares to the
/// checksum captured on delivery; `Verify::Size` compares the remote `SIZE`; `Verify::None` confirms
/// existence.
async fn verify(
    dest: &SftpDest,
    sess: &SftpSession,
    item: &WorkItem,
    delivered: &Delivered,
    policy: Verify,
) -> Result<()> {
    let remote = handle_remote_path(&delivered.handle)
        .unwrap_or_else(|| remote_path(&dest.base_dir, &item.relpath));

    let meta = sess.metadata(remote.as_str()).await.map_err(|e| map_sftp(&e, false))?;
    match policy {
        Verify::None => Ok(()),
        Verify::Size => {
            let actual = meta.size.unwrap_or(0);
            crate::integrity::verify_size(delivered.bytes, actual)
        }
        Verify::Checksum => {
            crate::integrity::verify_size(delivered.bytes, meta.size.unwrap_or(0))?;
            let mut file = sess
                .open(remote.as_str())
                .await
                .map_err(|e| map_sftp(&e, false))?;
            let mut hasher = Hasher::new(dest.algo);
            let mut buf = vec![0u8; CHUNK];
            loop {
                let n = file.read(&mut buf).await.map_err(ReplError::classify_io)?;
                if n == 0 {
                    break;
                }
                hasher.update(&buf[..n]);
            }
            crate::integrity::verify_checksum(&delivered.checksum, &hasher.finish())
        }
    }
}

// ---- abort (give-up cleanup) -------------------------------------------------------------------

/// Delete the orphan temp file recorded in `resume` (idempotent — a missing file is fine). A resume
/// without an SFTP token is a no-op.
async fn abort(sess: &SftpSession, _item: &WorkItem, resume: &ResumeState) -> Result<()> {
    let Some(token) = SftpResumeToken::from_resume(resume) else {
        return Ok(());
    };
    match sess.remove_file(token.temp_path.as_str()).await {
        Ok(()) => Ok(()),
        Err(e) if is_not_found(&e) => Ok(()),
        Err(e) => Err(map_sftp(&e, false)),
    }
}

// ---- error mapping -------------------------------------------------------------------------------

/// Map an `SftpError` (from a `russh_sftp::client::SftpSession` call) to the retry engine's
/// [`ReplError`] via the unit-tested [`classify_sftp`].
fn map_sftp(e: &SftpError, transport: bool) -> ReplError {
    classify_sftp(sftp_error_kind(e), transport)
}

fn sftp_error_kind(e: &SftpError) -> &'static str {
    match e {
        SftpError::Status(s) => match s.status_code {
            StatusCode::PermissionDenied => "permission_denied",
            StatusCode::NoSuchFile => "no_such_file",
            StatusCode::BadMessage => "bad_message",
            StatusCode::OpUnsupported => "op_unsupported",
            StatusCode::NoConnection => "no_connection",
            StatusCode::ConnectionLost => "connection_lost",
            StatusCode::Ok | StatusCode::Eof | StatusCode::Failure => "failure",
        },
        SftpError::Timeout => "timeout",
        SftpError::IO(_) => "connection_lost",
        SftpError::Limited(_) | SftpError::UnexpectedPacket | SftpError::UnexpectedBehavior(_) => {
            "failure"
        }
    }
}

fn is_not_found(e: &SftpError) -> bool {
    matches!(
        e,
        SftpError::Status(s) if s.status_code == StatusCode::NoSuchFile
    )
}

/// Map an SSH-level `russh::Error` (connect/auth/channel setup) to the retry engine's [`ReplError`].
fn map_ssh(e: &russh::Error) -> ReplError {
    classify_sftp(ssh_error_kind(e), is_ssh_transport(e))
}

fn ssh_error_kind(e: &russh::Error) -> &'static str {
    match e {
        russh::Error::UnknownKey | russh::Error::KeyChanged { .. } => "host_key_mismatch",
        russh::Error::NotAuthenticated
        | russh::Error::UnsupportedAuthMethod
        | russh::Error::NoAuthMethod
        | russh::Error::WrongServerSig => "auth",
        russh::Error::HUP
        | russh::Error::Disconnect
        | russh::Error::IO(_)
        | russh::Error::ConnectionTimeout
        | russh::Error::KeepaliveTimeout
        | russh::Error::InactivityTimeout => "connection_lost",
        _ => "failure",
    }
}

fn is_ssh_transport(e: &russh::Error) -> bool {
    matches!(
        e,
        russh::Error::HUP
            | russh::Error::Disconnect
            | russh::Error::IO(_)
            | russh::Error::ConnectionTimeout
            | russh::Error::KeepaliveTimeout
            | russh::Error::InactivityTimeout
    )
}
