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
use crate::error::{ReplError, Result};
use crate::integrity::Algorithm;
use crate::ratelimit::Bandwidth;

pub mod local;

pub use local::LocalDest;

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
/// P1 ships the `local` backend only. `s3` is P2; `sftp`/`ftps`/`http`/`azure`/`gcs` are P5 (§22.2).
/// Any not-yet-implemented backend is a **permanent** configuration error (it will never succeed by
/// retrying), so the retry engine fails the instance fast rather than burning the `giveUpAfter` budget.
pub fn build_destination(cfg: &EgressCfg) -> Result<SharedDestination> {
    match cfg {
        // Local re-hash verification uses CRC32C (the default transfer-integrity hash, §13.1); the
        // local egress config carries no algorithm selector (that is an S3 flexible-checksum concern).
        EgressCfg::Local(local) => Ok(Arc::new(LocalDest::new(local, Algorithm::Crc32c))),
        EgressCfg::S3(_) => Err(ReplError::Permanent(
            "s3 destination is not available in this build (P2; feature dest-s3)".to_string(),
        )),
        EgressCfg::Sftp | EgressCfg::Ftps | EgressCfg::Http | EgressCfg::Azure | EgressCfg::Gcs => {
            Err(ReplError::Permanent(format!(
                "{} destination is not implemented (P5)",
                egress_kind(cfg)
            )))
        }
    }
}

/// Stable backend tag for an egress config variant (for diagnostics / the resume table `dest` key).
fn egress_kind(cfg: &EgressCfg) -> &'static str {
    match cfg {
        EgressCfg::Local(_) => "local",
        EgressCfg::S3(_) => "s3",
        EgressCfg::Sftp => "sftp",
        EgressCfg::Ftps => "ftps",
        EgressCfg::Http => "http",
        EgressCfg::Azure => "azure",
        EgressCfg::Gcs => "gcs",
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
        let d = build_destination(&cfg).expect("local builds");
        assert_eq!(d.kind(), "local");
        assert!(d.supports_resume());
    }

    #[test]
    fn factory_rejects_unimplemented_backends_permanently() {
        for cfg in [
            EgressCfg::Sftp,
            EgressCfg::Ftps,
            EgressCfg::Http,
            EgressCfg::Azure,
            EgressCfg::Gcs,
        ] {
            let err = build_destination(&cfg).err().expect("must reject");
            assert!(err.is_permanent(), "{cfg:?} → {err:?}");
        }
    }

    #[test]
    fn factory_rejects_s3_until_p2() {
        let cfg = EgressCfg::S3(Box::new(crate::config::S3Egress {
            bucket: "b".into(),
            prefix: String::new(),
            region: None,
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
        assert!(build_destination(&cfg)
            .err()
            .expect("s3 rejected")
            .is_permanent());
    }

    #[test]
    fn egress_kind_tags() {
        assert_eq!(egress_kind(&EgressCfg::Sftp), "sftp");
        assert_eq!(egress_kind(&EgressCfg::Http), "http");
        assert_eq!(
            egress_kind(&EgressCfg::Local(LocalEgress {
                path: PathBuf::from("/x"),
                fsync: false
            })),
            "local"
        );
    }
}
