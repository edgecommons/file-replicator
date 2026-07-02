//! # file-replicator — SFTP destination (DESIGN §10.3; P5)
//!
//! Pure-Rust SFTP over SSH (`russh` + `russh-sftp` — no C toolchain, builds clean on Windows/MSVC):
//! stream the source into a hidden temp file under the deterministic remote directory, then publish
//! with an SFTP `rename` (best-effort overwrite fallback, since plain `SSH_FXP_RENAME` — unlike POSIX
//! `rename(2)` — fails when the destination already exists on most servers). Resume is offset
//! append/seek: a crash mid-transfer leaves the temp file at some length on the server; on retry the
//! backend seeks to the persisted `bytes_committed`, re-hashes that already-uploaded prefix locally
//! (no re-upload — keeps the whole-file checksum honest for `Verify::Checksum`), and streams only the
//! remainder.
//!
//! ## Where the code lives (coverage seam, NFR-2)
//! Everything in **this file** is pure decision logic (remote-path/temp-name mapping, credential
//! selection, resume-token bookkeeping, error classification) — unit-tested with no network. Every
//! line that opens an SSH/SFTP connection or performs a protocol round-trip lives in [`client`],
//! excluded from the coverage gate (mirrors `dest/s3/{mod.rs,client.rs}`).

pub mod client;

use serde::{Deserialize, Serialize};

use crate::config::SftpEgress;
use crate::domain::ResumeState;
use crate::error::{ReplError, Result};
use crate::integrity::Algorithm;
use crate::state::StateStore;

use ggcommons::credentials::CredentialService;

use std::sync::Arc;

/// Default upload-in-progress temp suffix (mirrors `local.rs`'s `.part`).
const DEFAULT_TEMP_SUFFIX: &str = ".part";

// ---- path mapping (DESIGN §10.2 subtree preservation) --------------------------------------------

/// The deterministic remote path for `relpath` under `base_dir`: forward-slash join with traversal /
/// empty segments dropped (identical algorithm to `s3::object_key`, duplicated here so this backend
/// stays self-contained under its own feature gate).
pub fn remote_path(base_dir: &str, relpath: &str) -> String {
    let rel: Vec<&str> = relpath
        .split('/')
        .filter(|s| !s.is_empty() && *s != "." && *s != "..")
        .collect();
    let joined = rel.join("/");
    let b = base_dir.trim_matches('/');
    if b.is_empty() {
        joined
    } else if joined.is_empty() {
        b.to_string()
    } else {
        format!("{b}/{joined}")
    }
}

/// The hidden upload-in-progress temp path for `remote` in the same directory (so the publish
/// `rename` is same-directory): `.{filename}{suffix}`.
pub fn temp_path(remote: &str, suffix: &str) -> String {
    let (dir, filename) = match remote.rfind('/') {
        Some(i) => (&remote[..i], &remote[i + 1..]),
        None => ("", remote),
    };
    let name = format!(".{filename}{suffix}");
    if dir.is_empty() {
        name
    } else {
        format!("{dir}/{name}")
    }
}

/// The chain of ancestor directories of `remote`'s directory portion, shallowest-first (e.g.
/// `"a/b/c/file.csv"` → `["a", "a/b", "a/b/c"]`), so the caller can `mkdir` each in order
/// (`mkdir -p` equivalent; a "directory already exists" error at any step is expected and ignored by
/// the caller).
pub fn parent_dir_chain(remote: &str) -> Vec<String> {
    let mut segments: Vec<&str> = remote.split('/').collect();
    segments.pop(); // drop the filename
    let mut out = Vec::new();
    let mut acc = String::new();
    for seg in segments {
        if seg.is_empty() {
            continue;
        }
        acc = if acc.is_empty() {
            seg.to_string()
        } else {
            format!("{acc}/{seg}")
        };
        out.push(acc.clone());
    }
    out
}

// ---- credentials ----------------------------------------------------------------------------------

/// Where a private key's bytes come from: an ambient filesystem path (config), or inline PEM material
/// resolved from the vault (`$secret`).
#[derive(Clone, PartialEq, Eq)]
pub enum SftpKeySource {
    /// A path to a key file on local disk — not secret material itself, stays visible in `Debug`.
    Path(String),
    /// Inline PEM key bytes resolved from the vault — secret material, redacted in `Debug`.
    Inline(String),
}

impl std::fmt::Debug for SftpKeySource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SftpKeySource::Path(p) => f.debug_tuple("Path").field(p).finish(),
            SftpKeySource::Inline(_) => f.debug_tuple("Inline").field(&"***redacted***").finish(),
        }
    }
}

/// How the SFTP session authenticates. `Debug` is hand-written (not derived) so a stray `{:?}` never
/// leaks the password/passphrase/inline-key material (FR-CFG-5-equivalent, mirrors S3's `CredMode`).
#[derive(Clone, PartialEq, Eq)]
pub enum SftpAuth {
    Password {
        username: String,
        password: String,
    },
    PrivateKey {
        username: String,
        key: SftpKeySource,
        passphrase: Option<String>,
    },
}

impl std::fmt::Debug for SftpAuth {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SftpAuth::Password { username, .. } => f
                .debug_struct("Password")
                .field("username", username)
                .field("password", &"***redacted***")
                .finish(),
            SftpAuth::PrivateKey {
                username,
                key,
                passphrase,
            } => f
                .debug_struct("PrivateKey")
                .field("username", username)
                .field("key", key)
                .field("passphrase", &passphrase.as_ref().map(|_| "***redacted***"))
                .finish(),
        }
    }
}

/// A generic (non-AWS) secret JSON shape for SFTP credentials: `{username?, password?, privateKey?,
/// passphrase?}`. `username` is optional in the secret (falls back to `egress.username` when absent).
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct SftpSecretJson {
    username: Option<String>,
    password: Option<String>,
    private_key: Option<String>,
    passphrase: Option<String>,
}

/// Resolve the authentication mode from the egress config and the (optional) credential service:
///
/// - `credentials: {"$secret":"name"}` → resolve the secret as generic JSON
///   (`{username?,password?,privateKey?,passphrase?}`); a missing service, missing secret, or a shape
///   with neither `password` nor `privateKey` is a **permanent** config error;
/// - else `egress.password` set → ambient password auth (`egress.username` required);
/// - else `egress.privateKey` set → ambient key-file auth (`egress.username` required);
/// - else → permanent (no credentials configured).
pub fn resolve_sftp_credentials(
    cfg: &SftpEgress,
    creds: Option<&dyn CredentialService>,
) -> Result<SftpAuth> {
    if let Some(v) = &cfg.credentials {
        let name = v.get("$secret").and_then(|s| s.as_str()).ok_or_else(|| {
            ReplError::Permanent(
                "egress.credentials must be {\"$secret\":\"name\"} for the sftp backend".to_string(),
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
        let parsed: SftpSecretJson = serde_json::from_value(json).map_err(|e| {
            ReplError::Permanent(format!("secret '{name}' is not a valid sftp credentials JSON: {e}"))
        })?;
        let username = parsed.username.or_else(|| cfg.username.clone()).ok_or_else(|| {
            ReplError::Permanent(format!(
                "secret '{name}' has no 'username' and egress.username is not set"
            ))
        })?;
        if let Some(password) = parsed.password {
            return Ok(SftpAuth::Password { username, password });
        }
        if let Some(key) = parsed.private_key {
            return Ok(SftpAuth::PrivateKey {
                username,
                key: SftpKeySource::Inline(key),
                passphrase: parsed.passphrase,
            });
        }
        return Err(ReplError::Permanent(format!(
            "secret '{name}' has neither 'password' nor 'privateKey'"
        )));
    }

    if let Some(password) = &cfg.password {
        let username = cfg.username.clone().ok_or_else(|| {
            ReplError::Permanent("egress.username is required when egress.password is set".to_string())
        })?;
        return Ok(SftpAuth::Password {
            username,
            password: password.clone(),
        });
    }

    if let Some(path) = &cfg.private_key {
        let username = cfg.username.clone().ok_or_else(|| {
            ReplError::Permanent(
                "egress.username is required when egress.privateKey is set".to_string(),
            )
        })?;
        return Ok(SftpAuth::PrivateKey {
            username,
            key: SftpKeySource::Path(path.clone()),
            passphrase: cfg.passphrase.clone(),
        });
    }

    Err(ReplError::Permanent(
        "sftp egress has no credentials: set password, privateKey, or credentials.$secret".to_string(),
    ))
}

/// Compare a pinned OpenSSH host-key line (`"algo base64[ comment]"`) against the key the server
/// actually presented, ignoring surrounding whitespace and any trailing comment (only the algorithm
/// + key blob must match).
pub fn host_key_matches(pinned: &str, actual_openssh_line: &str) -> bool {
    fn fields(s: &str) -> Option<(&str, &str)> {
        let mut it = s.split_whitespace();
        let algo = it.next()?;
        let blob = it.next()?;
        Some((algo, blob))
    }
    match (fields(pinned.trim()), fields(actual_openssh_line.trim())) {
        (Some(a), Some(b)) => a == b,
        _ => false,
    }
}

/// The outcome of the SSH host-key check — the pure decision behind
/// [`client::ClientHandler::check_server_key`], kept on the unit-tested side of the NFR-2 seam (the
/// [`client`] handler only extracts the presented key's OpenSSH form and maps this outcome onto
/// russh's accept/reject boolean + a log line, exactly as [`classify_sftp`] splits decision from
/// extraction).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HostKeyCheck {
    /// Accept the presented key: it matches the pinned `egress.hostKey`, OR no key is pinned but the
    /// explicit `egress.insecureAcceptAnyHostKey` opt-in is set.
    Accept,
    /// **Fail-closed default:** no `egress.hostKey` is pinned and the insecure opt-in is not set, so
    /// the handshake is refused. The operator must pin `egress.hostKey` or set
    /// `egress.insecureAcceptAnyHostKey: true`.
    RejectUnpinned,
    /// A key IS pinned but the server presented a different one — refuse (possible man-in-the-middle).
    RejectMismatch,
}

/// Decide whether to accept the server's host key (SECURITY, NFR-7). `pinned` is the configured
/// `egress.hostKey` (an OpenSSH `algo base64[ comment]` line) if any; `presented` is the key the
/// server actually presented in the same OpenSSH form; `insecure_accept_any` is the
/// `egress.insecureAcceptAnyHostKey` opt-in.
///
/// The policy is **fail-closed**: with no pinned key and no explicit opt-in the connection is
/// REJECTED ([`HostKeyCheck::RejectUnpinned`]) rather than trusting whatever key appears — closing the
/// MITM hole where an attacker's substituted host key (and, under password auth, the credential) would
/// otherwise be accepted silently.
pub fn check_host_key(
    pinned: Option<&str>,
    presented: &str,
    insecure_accept_any: bool,
) -> HostKeyCheck {
    match pinned {
        Some(want) if host_key_matches(want, presented) => HostKeyCheck::Accept,
        Some(_) => HostKeyCheck::RejectMismatch,
        None if insecure_accept_any => HostKeyCheck::Accept,
        None => HostKeyCheck::RejectUnpinned,
    }
}

// ---- resume token (DESIGN §13.4/§14.2) -------------------------------------------------------------

/// The offset-resume checkpoint, serialized under the `"sftp"` key of [`ResumeState::token`]: the
/// hidden temp path being written, the source size + mtime at the time the transfer started (so a
/// resized/rewritten source discards the token and restarts — same staleness guard as
/// `S3ResumeToken::matches`), and the confirmed-committed byte offset.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SftpResumeToken {
    pub temp_path: String,
    pub size: u64,
    pub mtime_ms: i64,
    #[serde(default)]
    pub bytes_committed: u64,
}

impl SftpResumeToken {
    /// Extract an SFTP token from a [`ResumeState`], if one is present and well-formed.
    pub fn from_resume(r: &ResumeState) -> Option<SftpResumeToken> {
        r.token
            .get("sftp")
            .and_then(|v| serde_json::from_value(v.clone()).ok())
    }

    /// Wrap this token into a [`ResumeState`].
    pub fn to_resume(&self) -> ResumeState {
        ResumeState {
            bytes_committed: self.bytes_committed,
            token: serde_json::json!({
                "sftp": serde_json::to_value(self).unwrap_or(serde_json::Value::Null)
            }),
        }
    }

    /// Whether this token still targets the same temp path AND the current source (size + mtime), so
    /// its `bytes_committed` offset can be trusted (a grown/shrunk/rewritten source discards it).
    pub fn matches(&self, temp_path: &str, size: u64, mtime_ms: i64) -> bool {
        self.temp_path == temp_path && self.size == size && self.mtime_ms == mtime_ms
    }
}

// ---- delivery handle --------------------------------------------------------------------------------

/// The algorithm token recorded in a delivery handle / used for diagnostics.
fn algo_name(algo: Algorithm) -> &'static str {
    match algo {
        Algorithm::Crc32c => "CRC32C",
        Algorithm::Sha256 => "SHA256",
    }
}

/// Build the opaque [`crate::domain::Delivered::handle`] for an SFTP object: its remote path + the
/// local checksum algorithm used (SFTP has no native content-checksum echo to cross-check against).
pub fn build_handle(remote: &str, algo: Algorithm) -> serde_json::Value {
    serde_json::json!({
        "remotePath": remote,
        "algo": algo_name(algo),
    })
}

/// The remote path recorded in a delivery handle.
pub fn handle_remote_path(handle: &serde_json::Value) -> Option<String> {
    handle.get("remotePath").and_then(|v| v.as_str()).map(String::from)
}

// ---- error classification (DESIGN §13.4) -----------------------------------------------------------

/// True for SFTP/SSH failure kinds that can never succeed by retrying.
fn is_permanent_kind(kind: &str) -> bool {
    matches!(
        kind,
        "auth" | "permission_denied" | "host_key_mismatch" | "no_such_file" | "bad_message" | "op_unsupported"
    )
}

/// True for SFTP/SSH failure kinds worth retrying (transient server/connection faults).
fn is_transient_kind(kind: &str) -> bool {
    matches!(kind, "no_connection" | "connection_lost" | "timeout" | "failure")
}

/// Classify an SFTP/SSH failure into the retry engine's [`ReplError`] taxonomy (DESIGN §13.4). `kind`
/// is a stable tag [`client`] derives off the underlying `russh`/`russh-sftp` error (an SSH-level
/// failure or an `SSH_FXP_STATUS` code); `transport` is true for connect/timeout/dispatch failures.
/// The decision logic lives here (unit-tested); [`client`] only extracts the tag.
pub fn classify_sftp(kind: &str, transport: bool) -> ReplError {
    if transport {
        return ReplError::Transient(format!("sftp transport failure ({kind})"));
    }
    // A `permission_denied` SFTP status is permanent AND must drive the Feature A egress permission
    // dedup-log + `PermissionDenied` event (`src/permission.rs`) — so it gets the dedicated variant the
    // worker keys off, not plain `Permanent` (which would only ever surface as `RetriesExhausted`).
    if kind == "permission_denied" {
        return ReplError::PermissionDenied(format!("sftp error: {kind}"));
    }
    if is_permanent_kind(kind) {
        return ReplError::Permanent(format!("sftp error: {kind}"));
    }
    if is_transient_kind(kind) {
        return ReplError::Transient(format!("sftp error: {kind}"));
    }
    // An unrecognized kind: retry defensively rather than quarantining on an unknown failure mode.
    ReplError::Transient(format!("sftp error (unclassified): {kind}"))
}

// ---- the destination ----------------------------------------------------------------------------

/// The SFTP backend. Holds the resolved, immutable transfer parameters; the SSH/SFTP session is
/// established lazily on first use ([`OnceCell`], see [`client`]) so construction stays synchronous
/// and offline-tolerant.
///
/// [`OnceCell`]: tokio::sync::OnceCell
pub struct SftpDest {
    pub(crate) host: String,
    pub(crate) port: u16,
    pub(crate) base_dir: String,
    pub(crate) auth: SftpAuth,
    pub(crate) host_key: Option<String>,
    /// Fail-open escape hatch: when no `host_key` is pinned, accept ANY server key instead of
    /// refusing the connection (fail-closed default). See [`check_host_key`] / NFR-7.
    pub(crate) insecure_accept_any_host_key: bool,
    pub(crate) temp_suffix: String,
    pub(crate) fsync: bool,
    pub(crate) algo: Algorithm,
    /// Shared durable store, so a resumed transfer can persist its offset checkpoint mid-flight.
    pub(crate) store: Option<Arc<dyn StateStore>>,
    /// The live SSH/SFTP session, established lazily. Held as `Mutex<Option<Arc<..>>>` rather than a
    /// `OnceCell` so a dead transport can be **invalidated and re-established** on the next attempt — a
    /// `OnceCell` caches the first session forever and, once its transport drops (server restart, NAT/
    /// idle timeout, or an hours-to-days outage), leaves the instance permanently unable to reconnect,
    /// defeating FR-REL-7 long-outage resilience (see [`client::SftpDest::conn`]).
    pub(crate) conn: tokio::sync::Mutex<Option<Arc<client::SftpConn>>>,
}

impl SftpDest {
    /// Build the backend from its typed egress config and the shared [`super::DestDeps`] (credentials
    /// and store). Resolves the checksum algorithm and the auth mode eagerly (pure/synchronous); the
    /// network session is deferred to first transfer.
    pub fn build(cfg: &SftpEgress, deps: &super::DestDeps) -> Result<Self> {
        let algo = match &cfg.checksum_algorithm {
            Some(name) => Algorithm::from_name(name).ok_or_else(|| {
                ReplError::Permanent(format!("unknown egress.checksumAlgorithm '{name}'"))
            })?,
            None => Algorithm::Crc32c,
        };
        let auth = resolve_sftp_credentials(cfg, deps.credentials.as_deref())?;
        Ok(SftpDest {
            host: cfg.host.clone(),
            port: cfg.port.unwrap_or(22),
            base_dir: cfg.base_dir.clone(),
            auth,
            host_key: cfg.host_key.clone(),
            insecure_accept_any_host_key: cfg.insecure_accept_any_host_key,
            temp_suffix: cfg
                .temp_suffix
                .clone()
                .unwrap_or_else(|| DEFAULT_TEMP_SUFFIX.to_string()),
            fsync: cfg.fsync,
            algo,
            store: deps.store.clone(),
            conn: tokio::sync::Mutex::new(None),
        })
    }
}

// The lazy connection builder and the `impl Destination for SftpDest` delegation glue live in
// [`client`] — every line opens a socket or performs an SFTP protocol round-trip, so they belong on
// the coverage-excluded side of the NFR-2 seam rather than eroding this module's counted coverage.

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dest::{DestDeps, Destination};

    // ---- remote_path / temp_path / parent_dir_chain ----------------------------------------------

    #[test]
    fn remote_path_joins_base_and_relpath() {
        assert_eq!(remote_path("upload", "a/b.txt"), "upload/a/b.txt");
        assert_eq!(remote_path("", "a/b.txt"), "a/b.txt");
        assert_eq!(remote_path("/upload/", "deep/nested/r.csv"), "upload/deep/nested/r.csv");
    }

    #[test]
    fn remote_path_normalizes_slashes_and_strips_traversal() {
        assert_eq!(remote_path("/p/", "a/b.txt"), "p/a/b.txt");
        assert_eq!(remote_path("p", "a/../b/./c.txt"), "p/a/b/c.txt");
        assert!(!remote_path("p", "../../etc/passwd").contains(".."));
        assert_eq!(remote_path("p", "//a//b//"), "p/a/b");
        assert_eq!(remote_path("p", ""), "p");
    }

    #[test]
    fn temp_path_hides_and_suffixes_the_filename() {
        assert_eq!(temp_path("upload/a/b.txt", ".part"), "upload/a/.b.txt.part");
        assert_eq!(temp_path("b.txt", ".part"), ".b.txt.part");
        assert_eq!(temp_path("a/b/c.bin", ".tmp"), "a/b/.c.bin.tmp");
    }

    #[test]
    fn parent_dir_chain_builds_incremental_paths() {
        assert_eq!(
            parent_dir_chain("a/b/c/file.csv"),
            vec!["a".to_string(), "a/b".to_string(), "a/b/c".to_string()]
        );
        assert!(parent_dir_chain("file.csv").is_empty());
        assert_eq!(parent_dir_chain("a/file.csv"), vec!["a".to_string()]);
    }

    // ---- host_key_matches --------------------------------------------------------------------------

    #[test]
    fn host_key_matches_ignores_comment_and_whitespace() {
        assert!(host_key_matches(
            "ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAI comment-here",
            "  ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAI  "
        ));
        assert!(!host_key_matches(
            "ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAI",
            "ssh-rsa AAAAC3NzaC1lZDI1NTE5AAAAI"
        ));
        assert!(!host_key_matches("ssh-ed25519 AAAA", "ssh-ed25519 BBBB"));
        assert!(!host_key_matches("", "ssh-ed25519 AAAA"));
    }

    // ---- check_host_key (FIX 1 — fail-closed host-key policy, NFR-7) --------------------------------

    const KEY_A: &str = "ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIAAA";
    const KEY_B: &str = "ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIBBB";

    #[test]
    fn check_host_key_unpinned_without_optin_is_rejected() {
        // The fail-closed default: no pinned key, no opt-in → refuse the connection.
        assert_eq!(check_host_key(None, KEY_A, false), HostKeyCheck::RejectUnpinned);
    }

    #[test]
    fn check_host_key_unpinned_with_optin_is_accepted() {
        // The explicit insecure escape hatch: no pinned key + opt-in → accept on trust.
        assert_eq!(check_host_key(None, KEY_A, true), HostKeyCheck::Accept);
    }

    #[test]
    fn check_host_key_pinned_match_is_accepted() {
        // A pinned key that matches → accept (the opt-in is irrelevant once a key is pinned).
        assert_eq!(check_host_key(Some(KEY_A), KEY_A, false), HostKeyCheck::Accept);
        assert_eq!(check_host_key(Some(KEY_A), KEY_A, true), HostKeyCheck::Accept);
    }

    #[test]
    fn check_host_key_pinned_mismatch_is_rejected() {
        // A pinned key that does NOT match → reject, even with the insecure opt-in set: an explicit
        // pin is authoritative, so a mismatch is always a possible MITM and must fail.
        assert_eq!(check_host_key(Some(KEY_A), KEY_B, false), HostKeyCheck::RejectMismatch);
        assert_eq!(check_host_key(Some(KEY_A), KEY_B, true), HostKeyCheck::RejectMismatch);
    }

    // ---- resume token --------------------------------------------------------------------------

    #[test]
    fn sftp_resume_token_round_trips_via_resume_state() {
        let tok = SftpResumeToken {
            temp_path: "upload/.a.txt.part".into(),
            size: 12345,
            mtime_ms: 1_700_000,
            bytes_committed: 4096,
        };
        let rs = tok.to_resume();
        assert_eq!(rs.bytes_committed, 4096);
        let json = serde_json::to_string(&rs).unwrap();
        let back: ResumeState = serde_json::from_str(&json).unwrap();
        let tok2 = SftpResumeToken::from_resume(&back).expect("token present");
        assert_eq!(tok2, tok);
    }

    #[test]
    fn sftp_resume_token_absent_or_wrong_shape_is_none() {
        assert!(SftpResumeToken::from_resume(&ResumeState::default()).is_none());
        let s3_shape = ResumeState {
            bytes_committed: 1,
            token: serde_json::json!({ "s3": { "uploadId": "x" } }),
        };
        assert!(SftpResumeToken::from_resume(&s3_shape).is_none());
    }

    #[test]
    fn sftp_resume_token_matches_guards_temp_path_size_and_mtime() {
        let tok = SftpResumeToken {
            temp_path: "d/.a.txt.part".into(),
            size: 100,
            mtime_ms: 42,
            bytes_committed: 50,
        };
        assert!(tok.matches("d/.a.txt.part", 100, 42));
        assert!(!tok.matches("d/.other.txt.part", 100, 42), "temp path change → discard");
        assert!(!tok.matches("d/.a.txt.part", 200, 42), "size change → discard");
        assert!(!tok.matches("d/.a.txt.part", 100, 99), "mtime change → discard");
    }

    // ---- delivery handle ------------------------------------------------------------------------

    #[test]
    fn handle_round_trips_remote_path() {
        let h = build_handle("upload/a.bin", Algorithm::Crc32c);
        assert_eq!(handle_remote_path(&h).as_deref(), Some("upload/a.bin"));
        assert_eq!(h.get("algo").and_then(|v| v.as_str()), Some("CRC32C"));
        let h2 = build_handle("upload/b.bin", Algorithm::Sha256);
        assert_eq!(h2.get("algo").and_then(|v| v.as_str()), Some("SHA256"));
    }

    #[test]
    fn handle_remote_path_absent_is_none() {
        assert_eq!(handle_remote_path(&serde_json::json!({})), None);
    }

    // ---- error classification -------------------------------------------------------------------

    #[test]
    fn classify_transport_is_transient() {
        assert!(classify_sftp("connection_lost", true).is_transient());
        assert!(classify_sftp("auth", true).is_transient(), "transport wins over an auth-shaped tag");
    }

    #[test]
    fn classify_permanent_kinds() {
        for k in ["auth", "permission_denied", "host_key_mismatch", "no_such_file", "bad_message", "op_unsupported"] {
            assert!(classify_sftp(k, false).is_permanent(), "{k} should be permanent");
        }
    }

    #[test]
    fn classify_permission_denied_is_the_permission_denied_variant() {
        // A `permission_denied` status must be detectable as a permission denial (Feature A egress
        // event), not just "some permanent error" — distinct from `auth`/`no_such_file`, which stay
        // plain `Permanent`.
        let e = classify_sftp("permission_denied", false);
        assert!(e.is_permission_denied(), "drives the egress PermissionDenied event");
        assert!(e.is_permanent());
        assert!(!classify_sftp("auth", false).is_permission_denied());
        assert!(!classify_sftp("no_such_file", false).is_permission_denied());
    }

    #[test]
    fn classify_transient_kinds() {
        for k in ["no_connection", "connection_lost", "timeout", "failure"] {
            assert!(classify_sftp(k, false).is_transient(), "{k} should be transient");
        }
    }

    #[test]
    fn classify_unknown_kind_is_transient() {
        assert!(classify_sftp("something-new", false).is_transient());
    }

    // ---- credentials ----------------------------------------------------------------------------

    /// A fake credential service returning canned JSON for one secret name. Overrides `get_json`
    /// directly (a provided/default trait method) rather than `get`, since the opaque `Secret` type
    /// has no public constructor outside the ggcommons credentials crate — exactly the pattern the S3
    /// backend's tests use for `get_aws_credentials`.
    struct FakeCreds {
        name: String,
        json: Option<serde_json::Value>,
    }
    impl CredentialService for FakeCreds {
        fn get(&self, _name: &str) -> ggcommons::Result<Option<ggcommons::credentials::Secret>> {
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
        fn list(&self, _prefix: &str) -> ggcommons::Result<Vec<ggcommons::credentials::SecretMeta>> {
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
            Ok("v1".to_string())
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

    fn base_cfg() -> SftpEgress {
        SftpEgress {
            host: "localhost".into(),
            port: None,
            base_dir: "/upload".into(),
            username: None,
            password: None,
            private_key: None,
            passphrase: None,
            credentials: None,
            host_key: None,
            insecure_accept_any_host_key: false,
            temp_suffix: None,
            fsync: false,
            checksum_algorithm: None,
        }
    }

    #[test]
    fn resolve_credentials_ambient_password() {
        let mut cfg = base_cfg();
        cfg.username = Some("alice".into());
        cfg.password = Some("s3cr3t".into());
        let auth = resolve_sftp_credentials(&cfg, None).unwrap();
        assert_eq!(
            auth,
            SftpAuth::Password {
                username: "alice".into(),
                password: "s3cr3t".into(),
            }
        );
    }

    #[test]
    fn resolve_credentials_ambient_password_without_username_is_permanent() {
        let mut cfg = base_cfg();
        cfg.password = Some("s3cr3t".into());
        assert!(resolve_sftp_credentials(&cfg, None).unwrap_err().is_permanent());
    }

    #[test]
    fn resolve_credentials_ambient_private_key() {
        let mut cfg = base_cfg();
        cfg.username = Some("bob".into());
        cfg.private_key = Some("/home/bob/.ssh/id_ed25519".into());
        cfg.passphrase = Some("p4ss".into());
        let auth = resolve_sftp_credentials(&cfg, None).unwrap();
        assert_eq!(
            auth,
            SftpAuth::PrivateKey {
                username: "bob".into(),
                key: SftpKeySource::Path("/home/bob/.ssh/id_ed25519".into()),
                passphrase: Some("p4ss".into()),
            }
        );
    }

    #[test]
    fn resolve_credentials_no_credentials_is_permanent() {
        let cfg = base_cfg();
        assert!(resolve_sftp_credentials(&cfg, None).unwrap_err().is_permanent());
    }

    #[test]
    fn resolve_credentials_secret_password_mode() {
        let creds = FakeCreds {
            name: "sftp-creds".into(),
            json: Some(serde_json::json!({ "username": "svc", "password": "vault-pass" })),
        };
        let mut cfg = base_cfg();
        cfg.credentials = Some(serde_json::json!({ "$secret": "sftp-creds" }));
        let auth = resolve_sftp_credentials(&cfg, Some(&creds)).unwrap();
        assert_eq!(
            auth,
            SftpAuth::Password {
                username: "svc".into(),
                password: "vault-pass".into(),
            }
        );
    }

    #[test]
    fn resolve_credentials_secret_key_mode_falls_back_to_egress_username() {
        let creds = FakeCreds {
            name: "sftp-creds".into(),
            json: Some(serde_json::json!({ "privateKey": "-----BEGIN KEY-----", "passphrase": "pp" })),
        };
        let mut cfg = base_cfg();
        cfg.username = Some("fallback-user".into());
        cfg.credentials = Some(serde_json::json!({ "$secret": "sftp-creds" }));
        let auth = resolve_sftp_credentials(&cfg, Some(&creds)).unwrap();
        assert_eq!(
            auth,
            SftpAuth::PrivateKey {
                username: "fallback-user".into(),
                key: SftpKeySource::Inline("-----BEGIN KEY-----".into()),
                passphrase: Some("pp".into()),
            }
        );
    }

    #[test]
    fn resolve_credentials_secret_without_service_is_permanent() {
        let mut cfg = base_cfg();
        cfg.credentials = Some(serde_json::json!({ "$secret": "sftp-creds" }));
        assert!(resolve_sftp_credentials(&cfg, None).unwrap_err().is_permanent());
    }

    #[test]
    fn resolve_credentials_unknown_secret_is_permanent() {
        let creds = FakeCreds {
            name: "other".into(),
            json: Some(serde_json::json!({ "username": "x", "password": "y" })),
        };
        let mut cfg = base_cfg();
        cfg.credentials = Some(serde_json::json!({ "$secret": "missing" }));
        assert!(resolve_sftp_credentials(&cfg, Some(&creds)).unwrap_err().is_permanent());
    }

    #[test]
    fn resolve_credentials_secret_missing_username_is_permanent() {
        let creds = FakeCreds {
            name: "sftp-creds".into(),
            json: Some(serde_json::json!({ "password": "vault-pass" })),
        };
        let mut cfg = base_cfg();
        cfg.credentials = Some(serde_json::json!({ "$secret": "sftp-creds" }));
        assert!(resolve_sftp_credentials(&cfg, Some(&creds)).unwrap_err().is_permanent());
    }

    #[test]
    fn resolve_credentials_secret_missing_password_and_key_is_permanent() {
        let creds = FakeCreds {
            name: "sftp-creds".into(),
            json: Some(serde_json::json!({ "username": "svc" })),
        };
        let mut cfg = base_cfg();
        cfg.credentials = Some(serde_json::json!({ "$secret": "sftp-creds" }));
        assert!(resolve_sftp_credentials(&cfg, Some(&creds)).unwrap_err().is_permanent());
    }

    #[test]
    fn resolve_credentials_malformed_secret_json_is_permanent() {
        let creds = FakeCreds {
            name: "sftp-creds".into(),
            json: Some(serde_json::json!(["not", "an", "object"])),
        };
        let mut cfg = base_cfg();
        cfg.credentials = Some(serde_json::json!({ "$secret": "sftp-creds" }));
        assert!(resolve_sftp_credentials(&cfg, Some(&creds)).unwrap_err().is_permanent());
    }

    #[test]
    fn resolve_credentials_non_secret_json_shape_is_permanent() {
        let mut cfg = base_cfg();
        cfg.credentials = Some(serde_json::json!({ "username": "inline" }));
        assert!(resolve_sftp_credentials(&cfg, None).unwrap_err().is_permanent());
    }

    // ---- Debug redaction ------------------------------------------------------------------------

    #[test]
    fn sftp_auth_debug_redacts_password_and_passphrase() {
        let pw = SftpAuth::Password {
            username: "alice".into(),
            password: "TOP_SECRET".into(),
        };
        let dbg = format!("{pw:?}");
        assert!(!dbg.contains("TOP_SECRET"));
        assert!(dbg.contains("alice"));
        assert!(dbg.contains("***redacted***"));

        let key = SftpAuth::PrivateKey {
            username: "bob".into(),
            key: SftpKeySource::Inline("SECRET_PEM_BYTES".into()),
            passphrase: Some("SECRET_PASSPHRASE".into()),
        };
        let dbg = format!("{key:?}");
        assert!(!dbg.contains("SECRET_PEM_BYTES"));
        assert!(!dbg.contains("SECRET_PASSPHRASE"));
        assert!(dbg.contains("bob"));

        // A path is not secret material — stays visible.
        let key_path = SftpKeySource::Path("/home/bob/.ssh/id_ed25519".into());
        assert!(format!("{key_path:?}").contains("id_ed25519"));
    }

    // ---- SftpDest::build --------------------------------------------------------------------------

    #[test]
    fn build_defaults_port_and_algo_and_temp_suffix() {
        let mut cfg = base_cfg();
        cfg.username = Some("alice".into());
        cfg.password = Some("s3cr3t".into());
        let dest = SftpDest::build(&cfg, &DestDeps::default()).unwrap();
        assert_eq!(dest.kind(), "sftp");
        assert!(dest.supports_resume());
        assert_eq!(dest.port, 22);
        assert_eq!(dest.algo, Algorithm::Crc32c);
        assert_eq!(dest.temp_suffix, ".part");
    }

    #[test]
    fn build_reads_explicit_port_algo_and_temp_suffix() {
        let mut cfg = base_cfg();
        cfg.username = Some("alice".into());
        cfg.password = Some("s3cr3t".into());
        cfg.port = Some(2222);
        cfg.checksum_algorithm = Some("SHA-256".into());
        cfg.temp_suffix = Some(".uploading".into());
        let dest = SftpDest::build(&cfg, &DestDeps::default()).unwrap();
        assert_eq!(dest.port, 2222);
        assert_eq!(dest.algo, Algorithm::Sha256);
        assert_eq!(dest.temp_suffix, ".uploading");
    }

    #[test]
    fn build_rejects_unknown_checksum_algorithm() {
        let mut cfg = base_cfg();
        cfg.username = Some("alice".into());
        cfg.password = Some("s3cr3t".into());
        cfg.checksum_algorithm = Some("MD5".into());
        assert!(SftpDest::build(&cfg, &DestDeps::default())
            .err()
            .expect("unknown algorithm rejected")
            .is_permanent());
    }

    #[test]
    fn build_propagates_credential_resolution_failure() {
        let cfg = base_cfg(); // no credentials at all
        assert!(SftpDest::build(&cfg, &DestDeps::default())
            .err()
            .expect("no credentials rejected")
            .is_permanent());
    }
}
