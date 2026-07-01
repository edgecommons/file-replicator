//! # file-replicator — frozen shared domain types
//!
//! The leaf every other module compiles against: the tracked-file lifecycle ([`ItemState`]), the
//! unit of work ([`WorkItem`]), the opaque per-`(item, dest)` resume checkpoint ([`ResumeState`]),
//! the delivery result ([`Delivered`] + [`Checksum`]), and the streaming byte-progress callback
//! ([`ProgressSink`]). No I/O and no dependency on the engine, so `state.rs` / `dest/*` / the worker
//! all share one vocabulary (DESIGN §8/§13/§14).

use std::path::PathBuf;
use std::sync::Arc;

use serde::{Deserialize, Serialize};

/// Lifecycle position of a tracked file (DESIGN §8). Persisted as TEXT via
/// [`as_str`](Self::as_str) / [`from_str`](Self::from_str).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ItemState {
    /// Readiness passed → durably queued. Only `Ready` items enter the store (DESIGN §9).
    Ready,
    /// A worker claimed it; delivery is underway (write-ahead of the transfer).
    InProgress,
    /// Delivered + integrity-verified; the WRITE-AHEAD point *before* the source side effect (§13.2).
    Verified,
    /// Terminal success — the source has been deleted or archived.
    Completed,
    /// Last attempt errored; awaiting backoff (attempts recorded, `next_attempt_at` set).
    Failed,
    /// `giveUpAfter` / `maxAttempts` hit; awaiting quarantine or retain.
    Exhausted,
    /// Terminal — moved to `failedDir` with an error sidecar (§13.3).
    Quarantined,
    /// Left in place, marked failed; retriable via a trigger command (P3).
    Retained,
}

impl ItemState {
    /// Stable lowercase token used as the persisted TEXT value.
    pub fn as_str(&self) -> &'static str {
        match self {
            ItemState::Ready => "ready",
            ItemState::InProgress => "in_progress",
            ItemState::Verified => "verified",
            ItemState::Completed => "completed",
            ItemState::Failed => "failed",
            ItemState::Exhausted => "exhausted",
            ItemState::Quarantined => "quarantined",
            ItemState::Retained => "retained",
        }
    }

    /// Inverse of [`as_str`](Self::as_str); `None` for an unrecognized token.
    // Deliberately an inherent, `Option`-returning parser (the exact inverse of `as_str`), not the
    // `FromStr` trait — persisted tokens are a closed set and an unknown value is simply "no match",
    // so a `Result`/`Err` type would add nothing. Narrow, reason-tagged (surfaced once the module
    // became a public library API).
    #[allow(clippy::should_implement_trait)]
    pub fn from_str(s: &str) -> Option<Self> {
        Some(match s {
            "ready" => ItemState::Ready,
            "in_progress" => ItemState::InProgress,
            "verified" => ItemState::Verified,
            "completed" => ItemState::Completed,
            "failed" => ItemState::Failed,
            "exhausted" => ItemState::Exhausted,
            "quarantined" => ItemState::Quarantined,
            "retained" => ItemState::Retained,
            _ => return None,
        })
    }

    /// True for the terminal states no worker will re-claim: `Completed`, `Quarantined`, `Retained`.
    pub fn is_terminal(&self) -> bool {
        matches!(
            self,
            ItemState::Completed | ItemState::Quarantined | ItemState::Retained
        )
    }
}

/// A single tracked file. Identity = `(instance, relpath)`.
///
/// `relpath` is source-root-relative and forward-slash normalized — the stable key for both the
/// destination object AND idempotent crash recovery (DESIGN §13.2, FR-REL-4). `abs_source` is
/// derived (`ingress.path.join(relpath)`), not a DB column.
#[derive(Debug, Clone)]
pub struct WorkItem {
    pub instance: String,
    pub relpath: String,
    /// Derived: `ingress.path.join(relpath)` — not persisted.
    pub abs_source: PathBuf,
    pub state: ItemState,
    pub size: u64,
    /// Unix ms — oldest-ready-first ordering.
    pub discovered_at: i64,
    pub attempts: u32,
    /// Unix ms — backoff gate for re-claim (P1 addition to the §14.2 schema).
    pub next_attempt_at: i64,
    pub last_error: Option<String>,
    pub bytes_done: u64,
    pub updated_at: i64,
}

/// Opaque per-`(item, dest)` resume checkpoint. For the local destination the `token` holds the
/// temp-file name and `bytes_committed` the append offset; stored as a JSON blob so the S3 backend
/// (P2) can reuse the same column for `{uploadId, completedParts}` with no schema change.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ResumeState {
    pub bytes_committed: u64,
    pub token: serde_json::Value,
}

/// Result of a successful [`Destination::deliver`](crate::dest::Destination::deliver): the byte
/// count, the single-pass [`Checksum`], and a backend finalize handle (local: the final path;
/// S3: object key + object-level checksum) carried opaquely for `verify` + completion accounting.
///
/// `Serialize`/`Deserialize` let the worker persist the whole `Delivered` as the `Verified`
/// write-ahead checkpoint (DESIGN §13.2), so crash recovery re-verifies against the exact expected
/// result (full checksum re-hash, not just size) before touching the source.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Delivered {
    pub bytes: u64,
    pub checksum: Checksum,
    pub handle: serde_json::Value,
}

/// A content checksum computed in the single streaming pass (DESIGN §13.1).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Checksum {
    Crc32c(u32),
    Sha256([u8; 32]),
    None,
}

/// Progress callback threaded from a worker into [`Destination::deliver`](crate::dest::Destination::deliver).
/// The destination calls [`report`](Self::report) with the running byte total as it streams; the
/// worker's closure throttles the update and persists `bytes_done`. Cheap, cloneable, `Send + Sync`.
#[derive(Clone)]
pub struct ProgressSink(Arc<dyn Fn(u64) + Send + Sync>);

impl ProgressSink {
    /// Wrap a byte-progress closure.
    pub fn new(f: impl Fn(u64) + Send + Sync + 'static) -> Self {
        ProgressSink(Arc::new(f))
    }

    /// Report the running total of bytes transferred so far.
    pub fn report(&self, bytes_done_total: u64) {
        (self.0)(bytes_done_total)
    }

    /// A sink that discards progress (tests, or destinations that don't stream).
    pub fn noop() -> Self {
        ProgressSink(Arc::new(|_| {}))
    }
}

impl std::fmt::Debug for ProgressSink {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("ProgressSink(..)")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};

    #[test]
    fn item_state_round_trips() {
        for s in [
            ItemState::Ready,
            ItemState::InProgress,
            ItemState::Verified,
            ItemState::Completed,
            ItemState::Failed,
            ItemState::Exhausted,
            ItemState::Quarantined,
            ItemState::Retained,
        ] {
            assert_eq!(ItemState::from_str(s.as_str()), Some(s));
        }
    }

    #[test]
    fn item_state_unknown_is_none() {
        assert_eq!(ItemState::from_str("bogus"), None);
        assert_eq!(ItemState::from_str(""), None);
    }

    #[test]
    fn terminal_states() {
        assert!(ItemState::Completed.is_terminal());
        assert!(ItemState::Quarantined.is_terminal());
        assert!(ItemState::Retained.is_terminal());
        for s in [
            ItemState::Ready,
            ItemState::InProgress,
            ItemState::Verified,
            ItemState::Failed,
            ItemState::Exhausted,
        ] {
            assert!(!s.is_terminal(), "{s:?} must not be terminal");
        }
    }

    #[test]
    fn progress_sink_invokes_closure() {
        let seen = Arc::new(AtomicU64::new(0));
        let s = seen.clone();
        let sink = ProgressSink::new(move |n| s.store(n, Ordering::SeqCst));
        sink.report(42);
        assert_eq!(seen.load(Ordering::SeqCst), 42);
    }

    #[test]
    fn progress_sink_noop_is_safe() {
        ProgressSink::noop().report(99); // must not panic
    }

    #[test]
    fn resume_state_json_round_trip() {
        let r = ResumeState {
            bytes_committed: 1024,
            token: serde_json::json!({"temp": "abc.part"}),
        };
        let s = serde_json::to_string(&r).unwrap();
        let back: ResumeState = serde_json::from_str(&s).unwrap();
        assert_eq!(back.bytes_committed, 1024);
        assert_eq!(back.token, serde_json::json!({"temp": "abc.part"}));
    }

    #[test]
    fn checksum_equality() {
        assert_eq!(Checksum::Crc32c(7), Checksum::Crc32c(7));
        assert_ne!(Checksum::Crc32c(7), Checksum::Crc32c(8));
        assert_ne!(Checksum::Crc32c(7), Checksum::None);
        assert_eq!(Checksum::Sha256([0u8; 32]), Checksum::Sha256([0u8; 32]));
    }

    #[test]
    fn progress_sink_debug() {
        assert_eq!(format!("{:?}", ProgressSink::noop()), "ProgressSink(..)");
    }
}
