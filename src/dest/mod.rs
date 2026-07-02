//! # file-replicator — destination abstraction (DESIGN §10.1)
//!
//! The [`Destination`] trait is the seam every backend implements and the worker drives uniformly.
//! It is frozen here (foundation slice) so `state.rs`, the worker, and the fake used by the
//! coverage-gate tests all compile against one shape; the concrete P1 backend (`dest/local.rs`) and
//! the [`build_destination`] factory land in the destination slice.
//!
//! Shape rationale vs the 3-method sketch in DESIGN §10.1: `deliver` performs the atomic
//! temp-write + fsync + rename, so the object is **live at its stable key before `verify`** — matching
//! the §13.2 crash-safe sequence exactly (persist `Verified` only after a passing re-hash, before the
//! *source* side effect). `supports_resume` is added so the scheduler can later choose
//! pause-resume vs finish-current without changing the trait (P4).

use std::sync::Arc;

use async_trait::async_trait;

use crate::config::{EgressCfg, Verify};
use crate::domain::{Delivered, ProgressSink, ResumeState, WorkItem};
use crate::error::Result;
use crate::integrity::Algorithm;
use crate::ratelimit::Bandwidth;
use crate::state::StateStore;

pub mod local;
#[cfg(feature = "dest-s3")]
pub mod s3;
#[cfg(feature = "dest-sftp")]
pub mod sftp;
#[cfg(feature = "dest-ftps")]
pub mod ftps;
#[cfg(feature = "dest-http")]
pub mod http;
#[cfg(feature = "dest-azure")]
pub mod azure;
#[cfg(feature = "dest-gcs")]
pub mod gcs;

pub use local::LocalDest;
#[cfg(feature = "dest-s3")]
pub use s3::S3Dest;
#[cfg(feature = "dest-sftp")]
pub use sftp::SftpDest;
#[cfg(feature = "dest-ftps")]
pub use ftps::FtpsDest;
#[cfg(feature = "dest-http")]
pub use http::HttpDest;
#[cfg(feature = "dest-azure")]
pub use azure::AzureDest;
#[cfg(feature = "dest-gcs")]
pub use gcs::GcsDest;

/// Process-wide handles a backend may need beyond its own egress config (DESIGN §11.5):
/// the ggcommons credential service (for a `{"$secret":"…"}` egress reference) and the shared durable
/// [`StateStore`] (so the S3 backend can persist its multipart resume checkpoint — the [`Destination`]
/// trait carries no store handle). Both are optional; the `local` backend ignores them. Feature-gated
/// so a `--no-default-features` build without `dest-s3` neither references the credentials type nor
/// pulls the subsystem.
#[derive(Clone, Default)]
pub struct DestDeps {
    /// Credential service for resolving `{"$secret":"…"}` egress credentials.
    #[cfg(any(
        feature = "dest-s3",
        feature = "dest-sftp",
        feature = "dest-ftps",
        feature = "dest-http",
        feature = "dest-azure",
        feature = "dest-gcs"
    ))]
    pub credentials: Option<Arc<dyn ggcommons::credentials::CredentialService>>,
    /// Shared durable store, threaded to backends that checkpoint resume state mid-transfer (S3, SFTP).
    pub store: Option<Arc<dyn StateStore>>,
}

impl DestDeps {
    /// Attach the shared durable store.
    pub fn with_store(mut self, store: Arc<dyn StateStore>) -> Self {
        self.store = Some(store);
        self
    }

    /// Attach the credential service (`$secret` resolution).
    #[cfg(any(
        feature = "dest-s3",
        feature = "dest-sftp",
        feature = "dest-ftps",
        feature = "dest-http",
        feature = "dest-azure",
        feature = "dest-gcs"
    ))]
    pub fn with_credentials(
        mut self,
        credentials: Option<Arc<dyn ggcommons::credentials::CredentialService>>,
    ) -> Self {
        self.credentials = credentials;
        self
    }
}

/// A replication target. `Send + Sync` so the worker pool can share one `Arc<dyn Destination>`.
#[async_trait]
pub trait Destination: Send + Sync {
    /// Stable backend tag (`"local"`; later `"s3"`). Also the `dest` key in the resume table.
    fn kind(&self) -> &'static str;

    /// Whether a killed transfer resumes from a persisted [`ResumeState`] (local: `true`,
    /// offset-append) rather than restarting from zero.
    fn supports_resume(&self) -> bool;

    /// Stream the source to its **deterministic** key (dest root/prefix + `item.relpath`), resuming
    /// from `resume` if present. Streams through `bw` (per-instance ∩ global cap) and reports byte
    /// progress via `progress`. On success the object is LIVE at its stable key (local: temp write +
    /// fsync + atomic rename), so this call *is* the delivery commit. MUST be idempotent: same
    /// `relpath` → same key → overwrite-identical (FR-REL-4). Returns the byte count + single-pass
    /// checksum for verification.
    async fn deliver(
        &self,
        item: &WorkItem,
        resume: Option<ResumeState>,
        progress: &ProgressSink,
        bw: &Bandwidth,
    ) -> Result<Delivered>;

    /// Verify the delivered object matches per `policy` (re-hash | size | none) BEFORE the source
    /// side effect. Idempotent — safe to re-run on crash recovery (DESIGN §13.2).
    async fn verify(&self, item: &WorkItem, delivered: &Delivered, policy: Verify) -> Result<()>;

    /// Clean up a partial transfer on give-up/removal (local: delete the temp file). Idempotent.
    async fn abort(&self, item: &WorkItem, resume: &ResumeState) -> Result<()>;
}

/// A shared, dynamically-dispatched destination handle.
pub type SharedDestination = Arc<dyn Destination>;

/// Build the concrete [`Destination`] for an egress config entry (DESIGN §10.2).
///
/// Ships the `local` (P1) and `s3` (P2, feature `dest-s3`) backends; `sftp`/`ftps`/`http`/`azure`/`gcs`
/// (P5, features `dest-sftp`/`dest-ftps`/`dest-http`/`dest-azure`/`dest-gcs`) too.
/// A not-compiled-in backend is a **permanent**
/// configuration error (it will never succeed by retrying), so the retry engine fails the instance
/// fast rather than burning the `giveUpAfter` budget. `deps` supplies cross-cutting handles
/// (credentials + store); the `local` backend ignores it.
pub fn build_destination(cfg: &EgressCfg, deps: &DestDeps) -> Result<SharedDestination> {
    let _ = deps; // used only by feature-gated backends
    // `egress_kind` is otherwise only reachable from the not-compiled-in error arms below, and those
    // are feature-gated (stripped when the corresponding `dest-*` feature IS on) — so call it here,
    // unconditionally, to keep it live (and usefully logged) across every feature combination.
    tracing::debug!(kind = egress_kind(cfg), "building destination");
    match cfg {
        // Local re-hash verification uses CRC32C (the default transfer-integrity hash, §13.1); the
        // local egress config carries no algorithm selector (that is an S3 flexible-checksum concern).
        EgressCfg::Local(local) => Ok(Arc::new(LocalDest::new(local, Algorithm::Crc32c))),
        #[cfg(feature = "dest-s3")]
        EgressCfg::S3(s3cfg) => Ok(Arc::new(S3Dest::build(s3cfg, deps)?)),
        #[cfg(not(feature = "dest-s3"))]
        EgressCfg::S3(_) => Err(crate::error::ReplError::Permanent(
            "s3 destination is not available in this build (enable feature dest-s3)".to_string(),
        )),
        #[cfg(feature = "dest-sftp")]
        EgressCfg::Sftp(cfg) => Ok(Arc::new(SftpDest::build(cfg, deps)?)),
        #[cfg(not(feature = "dest-sftp"))]
        EgressCfg::Sftp(_) => Err(crate::error::ReplError::Permanent(
            "sftp destination is not available in this build (enable feature dest-sftp)".to_string(),
        )),
        #[cfg(feature = "dest-ftps")]
        EgressCfg::Ftps(cfg) => Ok(Arc::new(FtpsDest::build(cfg, deps)?)),
        #[cfg(not(feature = "dest-ftps"))]
        EgressCfg::Ftps(_) => Err(crate::error::ReplError::Permanent(
            "ftps destination is not available in this build (enable feature dest-ftps)".to_string(),
        )),
        #[cfg(feature = "dest-http")]
        EgressCfg::Http(cfg) => Ok(Arc::new(HttpDest::build(cfg, deps)?)),
        #[cfg(not(feature = "dest-http"))]
        EgressCfg::Http(_) => Err(crate::error::ReplError::Permanent(
            "http destination is not available in this build (enable feature dest-http)".to_string(),
        )),
        #[cfg(feature = "dest-azure")]
        EgressCfg::Azure(cfg) => Ok(Arc::new(AzureDest::build(cfg, deps)?)),
        #[cfg(not(feature = "dest-azure"))]
        EgressCfg::Azure(_) => Err(crate::error::ReplError::Permanent(
            "azure destination is not available in this build (enable feature dest-azure)".to_string(),
        )),
        #[cfg(feature = "dest-gcs")]
        EgressCfg::Gcs(gcscfg) => Ok(Arc::new(GcsDest::build(gcscfg, deps)?)),
        #[cfg(not(feature = "dest-gcs"))]
        EgressCfg::Gcs(_) => Err(crate::error::ReplError::Permanent(
            "gcs destination is not available in this build (enable feature dest-gcs)".to_string(),
        )),
    }
}

/// Stable backend tag for an egress config variant (for diagnostics / the resume table `dest` key).
fn egress_kind(cfg: &EgressCfg) -> &'static str {
    match cfg {
        EgressCfg::Local(_) => "local",
        EgressCfg::S3(_) => "s3",
        EgressCfg::Sftp(_) => "sftp",
        EgressCfg::Ftps(_) => "ftps",
        EgressCfg::Http(_) => "http",
        EgressCfg::Azure(_) => "azure",
        EgressCfg::Gcs(_) => "gcs",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::LocalEgress;
    use std::path::PathBuf;

    #[test]
    fn factory_builds_local() {
        let cfg = EgressCfg::Local(LocalEgress {
            path: PathBuf::from("/out"),
            fsync: false,
        });
        let d = build_destination(&cfg, &DestDeps::default()).expect("local builds");
        assert_eq!(d.kind(), "local");
        assert!(d.supports_resume());
    }

    fn gcs_egress() -> crate::config::GcsEgress {
        crate::config::GcsEgress {
            bucket: "b".into(),
            prefix: String::new(),
            endpoint_url: None,
            access_token: None,
            anonymous: true,
            credentials: None,
            chunk_bytes: None,
            checksum_algorithm: None,
        }
    }

    #[cfg(not(feature = "dest-gcs"))]
    #[test]
    fn factory_rejects_gcs_permanently_when_feature_off() {
        let cfg = EgressCfg::Gcs(Box::new(gcs_egress()));
        let err = build_destination(&cfg, &DestDeps::default()).err().expect("must reject");
        assert!(err.is_permanent());
    }

    #[cfg(feature = "dest-gcs")]
    #[test]
    fn factory_builds_gcs() {
        let cfg = EgressCfg::Gcs(Box::new(gcs_egress()));
        let d = build_destination(&cfg, &DestDeps::default()).expect("gcs builds");
        assert_eq!(d.kind(), "gcs");
        assert!(d.supports_resume());
    }

    fn sftp_egress() -> crate::config::SftpEgress {
        crate::config::SftpEgress {
            host: "localhost".into(),
            port: None,
            base_dir: "/upload".into(),
            username: Some("testuser".into()),
            password: Some("testpass".into()),
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

    fn ftps_egress() -> crate::config::FtpsEgress {
        crate::config::FtpsEgress {
            host: "localhost".into(),
            port: None,
            base_dir: "/".into(),
            username: Some("testuser".into()),
            password: Some("testpass".into()),
            credentials: None,
            explicit_tls: None,
            passive: None,
            temp_suffix: None,
            checksum_algorithm: None,
        }
    }

    fn http_egress() -> crate::config::HttpEgress {
        crate::config::HttpEgress {
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

    #[cfg(not(feature = "dest-sftp"))]
    #[test]
    fn factory_rejects_sftp_permanently_when_feature_off() {
        let cfg = EgressCfg::Sftp(Box::new(sftp_egress()));
        let err = build_destination(&cfg, &DestDeps::default()).err().expect("must reject");
        assert!(err.is_permanent());
    }

    #[cfg(not(feature = "dest-ftps"))]
    #[test]
    fn factory_rejects_ftps_permanently_when_feature_off() {
        let cfg = EgressCfg::Ftps(Box::new(ftps_egress()));
        let err = build_destination(&cfg, &DestDeps::default()).err().expect("must reject");
        assert!(err.is_permanent());
    }

    #[cfg(not(feature = "dest-http"))]
    #[test]
    fn factory_rejects_http_permanently_when_feature_off() {
        let cfg = EgressCfg::Http(Box::new(http_egress()));
        let err = build_destination(&cfg, &DestDeps::default()).err().expect("must reject");
        assert!(err.is_permanent());
    }

    fn azure_egress() -> crate::config::AzureEgress {
        crate::config::AzureEgress {
            account: "devstoreaccount1".into(),
            container: "c".into(),
            prefix: String::new(),
            endpoint_url: None,
            account_key: Some("k".into()),
            connection_string: None,
            credentials: None,
            blocks: Default::default(),
            checksum_algorithm: None,
        }
    }

    #[cfg(not(feature = "dest-azure"))]
    #[test]
    fn factory_rejects_azure_permanently_when_feature_off() {
        let cfg = EgressCfg::Azure(Box::new(azure_egress()));
        let err = build_destination(&cfg, &DestDeps::default()).err().expect("must reject");
        assert!(err.is_permanent());
    }

    #[cfg(feature = "dest-azure")]
    #[test]
    fn factory_builds_azure() {
        let cfg = EgressCfg::Azure(Box::new(azure_egress()));
        let d = build_destination(&cfg, &DestDeps::default()).expect("azure builds");
        assert_eq!(d.kind(), "azure");
        assert!(d.supports_resume());
    }

    #[cfg(feature = "dest-sftp")]
    #[test]
    fn factory_builds_sftp() {
        let cfg = EgressCfg::Sftp(Box::new(sftp_egress()));
        let d = build_destination(&cfg, &DestDeps::default()).expect("sftp builds");
        assert_eq!(d.kind(), "sftp");
        assert!(d.supports_resume());
    }

    #[cfg(feature = "dest-ftps")]
    #[test]
    fn factory_builds_ftps() {
        let cfg = EgressCfg::Ftps(Box::new(ftps_egress()));
        let d = build_destination(&cfg, &DestDeps::default()).expect("ftps builds");
        assert_eq!(d.kind(), "ftps");
    }

    #[cfg(feature = "dest-http")]
    #[test]
    fn factory_builds_http() {
        let cfg = EgressCfg::Http(Box::new(http_egress()));
        let d = build_destination(&cfg, &DestDeps::default()).expect("http builds");
        assert_eq!(d.kind(), "http");
        assert!(d.supports_resume());
    }

    #[cfg(feature = "dest-s3")]
    #[test]
    fn factory_builds_s3() {
        let cfg = EgressCfg::S3(Box::new(crate::config::S3Egress {
            bucket: "b".into(),
            prefix: "p/".into(),
            region: Some("us-east-1".into()),
            endpoint_url: None,
            credentials: None,
            storage_class: None,
            sse: None,
            kms_key_id: None,
            accelerate: false,
            unsigned_payload: false,
            checksum_algorithm: None,
            multipart: Default::default(),
        }));
        let d = build_destination(&cfg, &DestDeps::default()).expect("s3 builds");
        assert_eq!(d.kind(), "s3");
        assert!(d.supports_resume());
    }

    #[test]
    fn egress_kind_tags() {
        assert_eq!(egress_kind(&EgressCfg::Sftp(Box::new(sftp_egress()))), "sftp");
        assert_eq!(egress_kind(&EgressCfg::Ftps(Box::new(ftps_egress()))), "ftps");
        assert_eq!(egress_kind(&EgressCfg::Http(Box::new(http_egress()))), "http");
        assert_eq!(egress_kind(&EgressCfg::Azure(Box::new(azure_egress()))), "azure");
        assert_eq!(egress_kind(&EgressCfg::Gcs(Box::new(gcs_egress()))), "gcs");
        assert_eq!(
            egress_kind(&EgressCfg::Local(LocalEgress {
                path: PathBuf::from("/x"),
                fsync: false
            })),
            "local"
        );
    }
}
