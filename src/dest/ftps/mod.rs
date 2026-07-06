//! # file-replicator — FTPS destination (DESIGN §10.3; P5)
//!
//! FTPS (FTP over explicit `AUTH TLS`) via `suppaftp`'s **synchronous** client (its `async` feature
//! runs on `async-std`, not `tokio` — pulling a second executor into the process is the wrong tradeoff
//! for one backend; the network calls in [`client`] run inside `tokio::task::spawn_blocking`).
//!
//! ## Scope note (DESIGN §22.2, follow-up)
//! SFTP is the priority backend of the "sftp/ftps" family (offset-append/seek resume over a mature
//! pure-Rust SSH stack). FTPS lands here fully wired through the same [`super::Destination`] seam and
//! is covered by unit tests, but there is **no live integration test**: no clean containerized FTPS
//! simulator was available in this environment (unlike `atmoz/sftp` for SFTP). A follow-up candidate
//! is `fauria/vsftpd` or `garethflowers/ftp-server` with TLS + a pinned passive-port range, added as a
//! self-skipping `tests/ftps_*.rs` in the same shape as `tests/sftp_atmoz.rs`. Only **explicit** TLS
//! (`AUTH TLS` on the plaintext control port) is implemented; `egress.explicitTls: false` (implicit
//! TLS, conventionally port 990) is rejected as a permanent config error rather than silently
//! downgrading to plaintext or half-implementing a deprecated mode.
//!
//! ## Where the code lives (coverage seam, NFR-2)
//! Everything in **this file** is pure decision logic (remote-path/temp-name mapping, credential
//! selection, resume-token bookkeeping, FTP reply-code classification) — unit-tested with no network.
//! Every line that opens a control/data connection or performs an FTP command round-trip lives in
//! [`client`], excluded from the coverage gate (mirrors `dest/s3/{mod.rs,client.rs}` and
//! `dest/sftp/{mod.rs,client.rs}`).

pub mod client;

use serde::{Deserialize, Serialize};

use crate::config::FtpsEgress;
use crate::domain::ResumeState;
use crate::error::{ReplError, Result};
use crate::integrity::Algorithm;
use crate::state::StateStore;

use edgecommons::credentials::CredentialService;

use std::sync::Arc;

const DEFAULT_TEMP_SUFFIX: &str = ".part";
const DEFAULT_PORT: u16 = 21;

// ---- path mapping (DESIGN §10.2 subtree preservation) --------------------------------------------

/// The deterministic remote path for `relpath` under `base_dir` (identical algorithm to
/// `sftp::remote_path`/`s3::object_key`, duplicated here so this backend stays self-contained under
/// its own feature gate).
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

/// The hidden upload-in-progress temp path for `remote` in the same directory: `.{filename}{suffix}`.
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

/// The chain of ancestor directories of `remote`'s directory portion, shallowest-first — see
/// `sftp::parent_dir_chain` (identical algorithm).
pub fn parent_dir_chain(remote: &str) -> Vec<String> {
    let mut segments: Vec<&str> = remote.split('/').collect();
    segments.pop();
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

/// FTPS authentication. `Debug` is hand-written (not derived) so a stray `{:?}` never leaks the
/// password (mirrors S3's `CredMode` / SFTP's `SftpAuth`).
#[derive(Clone, PartialEq, Eq)]
pub struct FtpsAuth {
    pub username: String,
    pub password: String,
}

impl std::fmt::Debug for FtpsAuth {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FtpsAuth")
            .field("username", &self.username)
            .field("password", &"***redacted***")
            .finish()
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct FtpsSecretJson {
    username: Option<String>,
    password: String,
}

/// Resolve credentials from the egress config and the (optional) credential service:
///
/// - `credentials: {"$secret":"name"}` → resolve as `{username?,password}` JSON (`username` falls
///   back to `egress.username` when absent in the secret); a missing service/secret/shape is
///   **permanent**;
/// - else `egress.password` set → ambient (`egress.username` required);
/// - else → permanent (no credentials configured).
pub fn resolve_ftps_credentials(
    cfg: &FtpsEgress,
    creds: Option<&dyn CredentialService>,
) -> Result<FtpsAuth> {
    if let Some(v) = &cfg.credentials {
        let name = v.get("$secret").and_then(|s| s.as_str()).ok_or_else(|| {
            ReplError::Permanent(
                "egress.credentials must be {\"$secret\":\"name\"} for the ftps backend".to_string(),
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
        let parsed: FtpsSecretJson = serde_json::from_value(json).map_err(|e| {
            ReplError::Permanent(format!("secret '{name}' is not a valid ftps credentials JSON: {e}"))
        })?;
        let username = parsed.username.or_else(|| cfg.username.clone()).ok_or_else(|| {
            ReplError::Permanent(format!(
                "secret '{name}' has no 'username' and egress.username is not set"
            ))
        })?;
        return Ok(FtpsAuth {
            username,
            password: parsed.password,
        });
    }

    if let Some(password) = &cfg.password {
        let username = cfg.username.clone().ok_or_else(|| {
            ReplError::Permanent("egress.username is required when egress.password is set".to_string())
        })?;
        return Ok(FtpsAuth {
            username,
            password: password.clone(),
        });
    }

    Err(ReplError::Permanent(
        "ftps egress has no credentials: set password or credentials.$secret".to_string(),
    ))
}

// ---- resume token ------------------------------------------------------------------------------

/// The offset-resume checkpoint, serialized under the `"ftps"` key of [`ResumeState::token`] — same
/// shape/semantics as `sftp::SftpResumeToken` (REST offset instead of an SFTP seek).
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FtpsResumeToken {
    pub temp_path: String,
    pub size: u64,
    pub mtime_ms: i64,
    #[serde(default)]
    pub bytes_committed: u64,
}

impl FtpsResumeToken {
    pub fn from_resume(r: &ResumeState) -> Option<FtpsResumeToken> {
        r.token
            .get("ftps")
            .and_then(|v| serde_json::from_value(v.clone()).ok())
    }

    pub fn to_resume(&self) -> ResumeState {
        ResumeState {
            bytes_committed: self.bytes_committed,
            token: serde_json::json!({
                "ftps": serde_json::to_value(self).unwrap_or(serde_json::Value::Null)
            }),
        }
    }

    pub fn matches(&self, temp_path: &str, size: u64, mtime_ms: i64) -> bool {
        self.temp_path == temp_path && self.size == size && self.mtime_ms == mtime_ms
    }
}

// ---- delivery handle --------------------------------------------------------------------------------

fn algo_name(algo: Algorithm) -> &'static str {
    match algo {
        Algorithm::Crc32c => "CRC32C",
        Algorithm::Sha256 => "SHA256",
    }
}

pub fn build_handle(remote: &str, algo: Algorithm) -> serde_json::Value {
    serde_json::json!({
        "remotePath": remote,
        "algo": algo_name(algo),
    })
}

pub fn handle_remote_path(handle: &serde_json::Value) -> Option<String> {
    handle.get("remotePath").and_then(|v| v.as_str()).map(String::from)
}

// ---- TLS client config (SECURITY, NFR-7) -----------------------------------------------------------

/// Build the rustls [`ClientConfig`](rustls::ClientConfig) for the FTPS `AUTH TLS` upgrade.
///
/// **SECURITY (NFR-7, DESIGN §10.2):** this uses rustls's **default** server-certificate verifier —
/// `ClientConfig::builder().with_root_certificates(roots)` installs the WebPKI verifier, which
/// validates BOTH the certificate chain (against the OS trust roots loaded via `rustls-native-certs`)
/// AND the certificate's identity against the TLS `ServerName`. [`client::connect_and_login`] passes
/// the **configured `egress.host`** as that `ServerName` (`into_secure(connector, &host)`), so a
/// certificate issued for a different host is rejected. There is deliberately NO
/// `dangerous()` / `with_custom_certificate_verifier` / accept-invalid-certs path anywhere — TLS
/// certificate + hostname verification is always in effect.
///
/// Kept here (pure/CPU + local trust-store read, no network) rather than in the coverage-excluded
/// [`client`] seam so the safe-default construction is unit-testable — the same
/// decision-logic-here / round-trips-in-`client` split the rest of the module follows.
pub(crate) fn build_rustls_client_config() -> Result<rustls::ClientConfig> {
    if rustls::crypto::CryptoProvider::get_default().is_none() {
        // Best-effort: ignore "already installed" (another part of the process won the race).
        let _ = rustls::crypto::ring::default_provider().install_default();
    }
    let mut roots = rustls::RootCertStore::empty();
    let found = rustls_native_certs::load_native_certs();
    for cert in found.certs {
        let _ = roots.add(cert);
    }
    // `with_root_certificates` = the safe default WebPKI verifier (chain + hostname). Never
    // `.dangerous()`.
    Ok(rustls::ClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth())
}

// ---- error classification (DESIGN §13.4) -----------------------------------------------------------

/// True for FTP reply codes that can never succeed by retrying: `530` not logged in, `532` need
/// account, `550`/`551`/`552`/`553` file-action-not-taken family (no such file/dir, permission,
/// storage allocation exceeded, bad filename).
fn is_permanent_ftp_code(code: u32) -> bool {
    matches!(code, 530 | 532 | 550 | 551 | 552 | 553)
}

/// True for FTP reply codes worth retrying: `421` service unavailable, `425`/`426` data-connection
/// faults, `450`/`451`/`452` transient file/local-resource errors.
fn is_transient_ftp_code(code: u32) -> bool {
    matches!(code, 421 | 425 | 426 | 450 | 451 | 452)
}

/// Classify an FTP failure into the retry engine's [`ReplError`] taxonomy. `code` is the numeric FTP
/// reply code when a reply was received; `transport` is true for connect/TLS/timeout failures. The
/// decision logic lives here (unit-tested); [`client`] only extracts the tag.
pub fn classify_ftps(code: Option<u32>, transport: bool) -> ReplError {
    if transport {
        return ReplError::Transient(format!(
            "ftps transport failure{}",
            code.map(|c| format!(" ({c})")).unwrap_or_default()
        ));
    }
    if let Some(c) = code {
        if is_permanent_ftp_code(c) {
            return ReplError::Permanent(format!("ftps error: {c}"));
        }
        if is_transient_ftp_code(c) {
            return ReplError::Transient(format!("ftps error: {c}"));
        }
        // An unrecognized 4xx/5xx-shaped or unmapped code: retry defensively.
        return ReplError::Transient(format!("ftps error (unclassified): {c}"));
    }
    ReplError::Transient("ftps error (unclassified)".to_string())
}

// ---- the destination ----------------------------------------------------------------------------

/// The FTPS backend. Holds the resolved, immutable transfer parameters. Unlike S3/SFTP, no connection
/// is cached across calls (see the module doc scope note): each `deliver`/`verify`/`abort` opens and
/// closes its own control connection inside [`client`], trading one extra TLS handshake per file for a
/// simpler, `Send`-safe implementation over `suppaftp`'s `&mut self`-only synchronous API.
pub struct FtpsDest {
    pub(crate) host: String,
    pub(crate) port: u16,
    pub(crate) base_dir: String,
    pub(crate) auth: FtpsAuth,
    /// Always `true` post-`build()` (implicit TLS is rejected there); kept as a field — not just a
    /// local — so it is visible for diagnostics/tests and `client.rs` doesn't have to re-derive it.
    #[allow(dead_code, reason = "diagnostic/test-only until a diagnostics surface reads it")]
    pub(crate) explicit_tls: bool,
    pub(crate) passive: bool,
    pub(crate) temp_suffix: String,
    pub(crate) algo: Algorithm,
    pub(crate) store: Option<Arc<dyn StateStore>>,
}

impl FtpsDest {
    /// Build the backend from its typed egress config and the shared [`super::DestDeps`] (credentials
    /// and store). Resolves the checksum algorithm and the auth mode eagerly (pure/synchronous);
    /// rejects implicit TLS (`explicitTls: false`) as a permanent config error (module scope note).
    pub fn build(cfg: &FtpsEgress, deps: &super::DestDeps) -> Result<Self> {
        let algo = match &cfg.checksum_algorithm {
            Some(name) => Algorithm::from_name(name).ok_or_else(|| {
                ReplError::Permanent(format!("unknown egress.checksumAlgorithm '{name}'"))
            })?,
            None => Algorithm::Crc32c,
        };
        let explicit_tls = cfg.explicit_tls.unwrap_or(true);
        if !explicit_tls {
            return Err(ReplError::Permanent(
                "ftps implicit TLS (egress.explicitTls: false) is not supported in this build; \
                 use explicit AUTH TLS (the default)"
                    .to_string(),
            ));
        }
        let auth = resolve_ftps_credentials(cfg, deps.credentials.as_deref())?;
        Ok(FtpsDest {
            host: cfg.host.clone(),
            port: cfg.port.unwrap_or(DEFAULT_PORT),
            base_dir: cfg.base_dir.clone(),
            auth,
            explicit_tls,
            passive: cfg.passive.unwrap_or(true),
            temp_suffix: cfg
                .temp_suffix
                .clone()
                .unwrap_or_else(|| DEFAULT_TEMP_SUFFIX.to_string()),
            algo,
            store: deps.store.clone(),
        })
    }
}

// The connection builder and the `impl Destination for FtpsDest` delegation glue live in [`client`] —
// every line opens a socket or performs an FTP command round-trip, so they belong on the
// coverage-excluded side of the NFR-2 seam rather than eroding this module's counted coverage.

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dest::DestDeps;

    // ---- remote_path / temp_path / parent_dir_chain ------------------------------------------------

    #[test]
    fn remote_path_joins_base_and_relpath() {
        assert_eq!(remote_path("upload", "a/b.txt"), "upload/a/b.txt");
        assert_eq!(remote_path("", "a/b.txt"), "a/b.txt");
    }

    #[test]
    fn remote_path_normalizes_slashes_and_strips_traversal() {
        assert_eq!(remote_path("/p/", "a/b.txt"), "p/a/b.txt");
        assert_eq!(remote_path("p", "a/../b/./c.txt"), "p/a/b/c.txt");
        assert!(!remote_path("p", "../../etc/passwd").contains(".."));
        assert_eq!(remote_path("p", ""), "p");
    }

    #[test]
    fn temp_path_hides_and_suffixes_the_filename() {
        assert_eq!(temp_path("upload/a/b.txt", ".part"), "upload/a/.b.txt.part");
        assert_eq!(temp_path("b.txt", ".part"), ".b.txt.part");
    }

    #[test]
    fn parent_dir_chain_builds_incremental_paths() {
        assert_eq!(
            parent_dir_chain("a/b/c/file.csv"),
            vec!["a".to_string(), "a/b".to_string(), "a/b/c".to_string()]
        );
        assert!(parent_dir_chain("file.csv").is_empty());
    }

    // ---- resume token --------------------------------------------------------------------------

    #[test]
    fn ftps_resume_token_round_trips_via_resume_state() {
        let tok = FtpsResumeToken {
            temp_path: "upload/.a.txt.part".into(),
            size: 999,
            mtime_ms: 12345,
            bytes_committed: 500,
        };
        let rs = tok.to_resume();
        assert_eq!(rs.bytes_committed, 500);
        let json = serde_json::to_string(&rs).unwrap();
        let back: ResumeState = serde_json::from_str(&json).unwrap();
        let tok2 = FtpsResumeToken::from_resume(&back).expect("token present");
        assert_eq!(tok2, tok);
    }

    #[test]
    fn ftps_resume_token_absent_or_wrong_shape_is_none() {
        assert!(FtpsResumeToken::from_resume(&ResumeState::default()).is_none());
        let sftp_shape = ResumeState {
            bytes_committed: 1,
            token: serde_json::json!({ "sftp": { "tempPath": "x" } }),
        };
        assert!(FtpsResumeToken::from_resume(&sftp_shape).is_none());
    }

    #[test]
    fn ftps_resume_token_matches_guards_temp_path_size_and_mtime() {
        let tok = FtpsResumeToken {
            temp_path: "d/.a.txt.part".into(),
            size: 100,
            mtime_ms: 42,
            bytes_committed: 50,
        };
        assert!(tok.matches("d/.a.txt.part", 100, 42));
        assert!(!tok.matches("d/.other.txt.part", 100, 42));
        assert!(!tok.matches("d/.a.txt.part", 200, 42));
        assert!(!tok.matches("d/.a.txt.part", 100, 99));
    }

    // ---- delivery handle ------------------------------------------------------------------------

    #[test]
    fn handle_round_trips_remote_path() {
        let h = build_handle("upload/a.bin", Algorithm::Crc32c);
        assert_eq!(handle_remote_path(&h).as_deref(), Some("upload/a.bin"));
        assert_eq!(h.get("algo").and_then(|v| v.as_str()), Some("CRC32C"));
    }

    #[test]
    fn handle_remote_path_absent_is_none() {
        assert_eq!(handle_remote_path(&serde_json::json!({})), None);
    }

    // ---- TLS client config (FIX 2 — hostname/cert verification in effect, NFR-7) -------------------

    #[test]
    fn rustls_client_config_uses_safe_default_verifier() {
        // SECURITY: the FTPS TLS config must use rustls's DEFAULT WebPKI server-certificate verifier
        // (validates the cert chain AND the hostname against the ServerName) with NO
        // `dangerous()`/custom accept-invalid verifier. rustls does not expose the (private) verifier
        // for introspection, but `with_root_certificates(..).with_no_client_auth()` is the only
        // safe-default construction path, and SNI — the client-sent server name that the cert is
        // checked against, passed as the configured host in `connect_and_login` — must stay enabled.
        // (A full wrong-hostname-cert test needs a live FTPS TLS server, which is a documented
        // follow-up; DESIGN §10.2.)
        let config = build_rustls_client_config().expect("client config builds");
        assert!(config.enable_sni, "SNI must be enabled so the server cert hostname is verified");
    }

    // ---- error classification -------------------------------------------------------------------

    #[test]
    fn classify_transport_is_transient() {
        assert!(classify_ftps(None, true).is_transient());
        assert!(classify_ftps(Some(530), true).is_transient(), "transport wins over a 5xx code");
    }

    #[test]
    fn classify_permanent_codes() {
        for c in [530, 532, 550, 551, 552, 553] {
            assert!(classify_ftps(Some(c), false).is_permanent(), "{c} should be permanent");
        }
    }

    #[test]
    fn classify_transient_codes() {
        for c in [421, 425, 426, 450, 451, 452] {
            assert!(classify_ftps(Some(c), false).is_transient(), "{c} should be transient");
        }
    }

    #[test]
    fn classify_unclassified_code_and_bare_failure_are_transient() {
        assert!(classify_ftps(Some(999), false).is_transient());
        assert!(classify_ftps(None, false).is_transient());
    }

    // ---- credentials ----------------------------------------------------------------------------

    /// A fake credential service returning canned JSON for one secret name. Overrides `get_json`
    /// directly (a provided/default trait method) — the opaque `Secret` type has no public
    /// constructor outside the edgecommons credentials crate (same pattern as S3's/SFTP's fakes).
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
        fn list(&self, _prefix: &str) -> edgecommons::Result<Vec<edgecommons::credentials::SecretMeta>> {
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

    fn base_cfg() -> FtpsEgress {
        FtpsEgress {
            host: "localhost".into(),
            port: None,
            base_dir: "/".into(),
            username: None,
            password: None,
            credentials: None,
            explicit_tls: None,
            passive: None,
            temp_suffix: None,
            checksum_algorithm: None,
        }
    }

    #[test]
    fn resolve_credentials_ambient_password() {
        let mut cfg = base_cfg();
        cfg.username = Some("alice".into());
        cfg.password = Some("s3cr3t".into());
        let auth = resolve_ftps_credentials(&cfg, None).unwrap();
        assert_eq!(
            auth,
            FtpsAuth {
                username: "alice".into(),
                password: "s3cr3t".into(),
            }
        );
    }

    #[test]
    fn resolve_credentials_ambient_password_without_username_is_permanent() {
        let mut cfg = base_cfg();
        cfg.password = Some("s3cr3t".into());
        assert!(resolve_ftps_credentials(&cfg, None).unwrap_err().is_permanent());
    }

    #[test]
    fn resolve_credentials_no_credentials_is_permanent() {
        assert!(resolve_ftps_credentials(&base_cfg(), None).unwrap_err().is_permanent());
    }

    #[test]
    fn resolve_credentials_secret_mode() {
        let creds = FakeCreds {
            name: "ftps-creds".into(),
            json: Some(serde_json::json!({ "username": "svc", "password": "vault-pass" })),
        };
        let mut cfg = base_cfg();
        cfg.credentials = Some(serde_json::json!({ "$secret": "ftps-creds" }));
        let auth = resolve_ftps_credentials(&cfg, Some(&creds)).unwrap();
        assert_eq!(
            auth,
            FtpsAuth {
                username: "svc".into(),
                password: "vault-pass".into(),
            }
        );
    }

    #[test]
    fn resolve_credentials_secret_falls_back_to_egress_username() {
        let creds = FakeCreds {
            name: "ftps-creds".into(),
            json: Some(serde_json::json!({ "password": "vault-pass" })),
        };
        let mut cfg = base_cfg();
        cfg.username = Some("fallback-user".into());
        cfg.credentials = Some(serde_json::json!({ "$secret": "ftps-creds" }));
        let auth = resolve_ftps_credentials(&cfg, Some(&creds)).unwrap();
        assert_eq!(auth.username, "fallback-user");
    }

    #[test]
    fn resolve_credentials_secret_without_service_is_permanent() {
        let mut cfg = base_cfg();
        cfg.credentials = Some(serde_json::json!({ "$secret": "ftps-creds" }));
        assert!(resolve_ftps_credentials(&cfg, None).unwrap_err().is_permanent());
    }

    #[test]
    fn resolve_credentials_unknown_secret_is_permanent() {
        let creds = FakeCreds {
            name: "other".into(),
            json: Some(serde_json::json!({ "username": "x", "password": "y" })),
        };
        let mut cfg = base_cfg();
        cfg.credentials = Some(serde_json::json!({ "$secret": "missing" }));
        assert!(resolve_ftps_credentials(&cfg, Some(&creds)).unwrap_err().is_permanent());
    }

    #[test]
    fn resolve_credentials_secret_missing_username_is_permanent() {
        let creds = FakeCreds {
            name: "ftps-creds".into(),
            json: Some(serde_json::json!({ "password": "vault-pass" })),
        };
        let mut cfg = base_cfg();
        cfg.credentials = Some(serde_json::json!({ "$secret": "ftps-creds" }));
        assert!(resolve_ftps_credentials(&cfg, Some(&creds)).unwrap_err().is_permanent());
    }

    #[test]
    fn resolve_credentials_malformed_secret_json_is_permanent() {
        let creds = FakeCreds {
            name: "ftps-creds".into(),
            json: Some(serde_json::json!({ "username": "svc" })), // missing required password
        };
        let mut cfg = base_cfg();
        cfg.credentials = Some(serde_json::json!({ "$secret": "ftps-creds" }));
        assert!(resolve_ftps_credentials(&cfg, Some(&creds)).unwrap_err().is_permanent());
    }

    #[test]
    fn resolve_credentials_non_secret_json_shape_is_permanent() {
        let mut cfg = base_cfg();
        cfg.credentials = Some(serde_json::json!({ "username": "inline" }));
        assert!(resolve_ftps_credentials(&cfg, None).unwrap_err().is_permanent());
    }

    // ---- Debug redaction ------------------------------------------------------------------------

    #[test]
    fn ftps_auth_debug_redacts_password() {
        let auth = FtpsAuth {
            username: "alice".into(),
            password: "TOP_SECRET".into(),
        };
        let dbg = format!("{auth:?}");
        assert!(!dbg.contains("TOP_SECRET"));
        assert!(dbg.contains("alice"));
        assert!(dbg.contains("***redacted***"));
    }

    // ---- FtpsDest::build --------------------------------------------------------------------------

    fn creds_cfg() -> FtpsEgress {
        let mut cfg = base_cfg();
        cfg.username = Some("alice".into());
        cfg.password = Some("s3cr3t".into());
        cfg
    }

    #[test]
    fn build_defaults_port_algo_passive_and_temp_suffix() {
        let dest = FtpsDest::build(&creds_cfg(), &DestDeps::default()).unwrap();
        assert_eq!(dest.port, 21);
        assert_eq!(dest.algo, Algorithm::Crc32c);
        assert!(dest.passive);
        assert!(dest.explicit_tls);
        assert_eq!(dest.temp_suffix, ".part");
    }

    #[test]
    fn build_reads_explicit_overrides() {
        let mut cfg = creds_cfg();
        cfg.port = Some(2121);
        cfg.passive = Some(false);
        cfg.checksum_algorithm = Some("SHA-256".into());
        cfg.temp_suffix = Some(".uploading".into());
        let dest = FtpsDest::build(&cfg, &DestDeps::default()).unwrap();
        assert_eq!(dest.port, 2121);
        assert!(!dest.passive);
        assert_eq!(dest.algo, Algorithm::Sha256);
        assert_eq!(dest.temp_suffix, ".uploading");
    }

    #[test]
    fn build_rejects_implicit_tls() {
        let mut cfg = creds_cfg();
        cfg.explicit_tls = Some(false);
        assert!(FtpsDest::build(&cfg, &DestDeps::default())
            .err()
            .expect("implicit tls rejected")
            .is_permanent());
    }

    #[test]
    fn build_rejects_unknown_checksum_algorithm() {
        let mut cfg = creds_cfg();
        cfg.checksum_algorithm = Some("MD5".into());
        assert!(FtpsDest::build(&cfg, &DestDeps::default())
            .err()
            .expect("unknown algorithm rejected")
            .is_permanent());
    }

    #[test]
    fn build_propagates_credential_resolution_failure() {
        assert!(FtpsDest::build(&base_cfg(), &DestDeps::default())
            .err()
            .expect("no credentials rejected")
            .is_permanent());
    }
}
