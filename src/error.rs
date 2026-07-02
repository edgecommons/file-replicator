//! # file-replicator — component-internal error type
//!
//! The component uses `anyhow` at the app boundary (mirroring `telemetry-processor`) but its **own**
//! [`ReplError`] internally so the retry/exhaustion engine can classify a failure as *transient*
//! (back off and keep retrying within `giveUpAfter`, DESIGN §13.4) or *permanent* (fail fast to
//! `Exhausted`). Integrity (checksum/size) mismatches are retriable but counted separately, and
//! durable-store failures get their own variant. Raw [`std::io::Error`] is classified at the
//! boundary via [`ReplError::classify_io`] / the `From` impl.

use std::io;

/// The component-internal error type. Every fallible engine path returns [`Result<T>`].
#[derive(Debug, thiserror::Error)]
pub enum ReplError {
    /// Connectivity / timeout / 5xx / throttle — back off and keep retrying (DESIGN §13.4).
    #[error("transient: {0}")]
    Transient(String),
    /// Auth denied / no-such-target / precondition / config — fail fast to `Exhausted` (DESIGN §13.4).
    #[error("permanent: {0}")]
    Permanent(String),
    /// Permission/access denied — a *permanent* failure (fail fast, exactly like [`Permanent`](Self::Permanent))
    /// that ALSO drives the Feature A egress permission dedup-log + `PermissionDenied` event
    /// (`src/permission.rs`). Split out from `Permanent` so the worker can detect a permission denial by
    /// **classification** (see [`is_permission_denied`](Self::is_permission_denied)) rather than by
    /// inspecting a raw [`Io`](Self::Io) kind: every shipping backend funnels its `io::Error`s through
    /// [`classify_io`](Self::classify_io) / `classify_sftp`, which collapse the `io::ErrorKind` into a
    /// string, so a raw `Io(PermissionDenied)` never actually reaches the worker in production.
    #[error("permission denied: {0}")]
    PermissionDenied(String),
    /// Checksum / size mismatch — retriable, but counted separately for diagnostics.
    #[error("integrity: {0}")]
    Integrity(String),
    /// Durable-store (SQLite) failure.
    #[error("state: {0}")]
    State(String),
    /// Raw I/O; classify at the boundary with [`ReplError::classify_io`] to keep the retry engine
    /// from burning the whole `giveUpAfter` budget on a `NotFound`/`PermissionDenied`.
    #[error(transparent)]
    Io(#[from] io::Error),
}

/// A durable-store (SQLite) failure is an engine-level fault, not a per-item retry decision
/// (see [`ReplError::is_transient`]/[`is_permanent`](ReplError::is_permanent)), so every rusqlite
/// error maps to [`ReplError::State`]. Lets [`SqliteStore`](crate::state::SqliteStore) use `?`.
impl From<rusqlite::Error> for ReplError {
    fn from(e: rusqlite::Error) -> Self {
        ReplError::State(e.to_string())
    }
}

impl ReplError {
    /// True for failures worth retrying: [`Transient`](Self::Transient),
    /// [`Integrity`](Self::Integrity), and transient-kind [`Io`](Self::Io).
    pub fn is_transient(&self) -> bool {
        match self {
            ReplError::Transient(_) | ReplError::Integrity(_) => true,
            ReplError::Io(e) => !Self::io_kind_is_permanent(e.kind()),
            ReplError::Permanent(_) | ReplError::PermissionDenied(_) | ReplError::State(_) => false,
        }
    }

    /// True for failures that should fail fast to `Exhausted`: [`Permanent`](Self::Permanent) and
    /// permanent-kind [`Io`](Self::Io) (`NotFound`, `PermissionDenied`).
    ///
    /// Note [`State`](Self::State) is neither transient nor permanent here: a durable-store failure
    /// is an engine-level fault handled out of band, not a per-item retry/exhaust decision.
    pub fn is_permanent(&self) -> bool {
        match self {
            ReplError::Permanent(_) | ReplError::PermissionDenied(_) => true,
            ReplError::Io(e) => Self::io_kind_is_permanent(e.kind()),
            ReplError::Transient(_) | ReplError::Integrity(_) | ReplError::State(_) => false,
        }
    }

    /// True when this error represents a **permission/access denial**, whether it was classified into the
    /// dedicated [`PermissionDenied`](Self::PermissionDenied) variant (the production path — every
    /// shipping backend routes its io errors through [`classify_io`](Self::classify_io) / `classify_sftp`)
    /// or is still a raw [`Io`](Self::Io) whose kind is `PermissionDenied` (the injection-seam path a
    /// fake destination uses in tests). The worker keys the Feature A egress dedup-log + `PermissionDenied`
    /// event off THIS, not off the raw `Io` variant — which no real destination ever surfaces, so matching
    /// on it directly (as the first cut did) was dead code in production.
    pub fn is_permission_denied(&self) -> bool {
        match self {
            ReplError::PermissionDenied(_) => true,
            ReplError::Io(e) => e.kind() == io::ErrorKind::PermissionDenied,
            _ => false,
        }
    }

    /// Map a raw [`io::Error`] to a classified [`ReplError`]: `PermissionDenied` becomes the dedicated
    /// [`PermissionDenied`](Self::PermissionDenied) variant (so the worker's Feature A egress dedup-log +
    /// event actually fire — it is still *permanent* for the retry engine), `NotFound` becomes
    /// [`Permanent`](Self::Permanent), and everything else is treated as [`Transient`](Self::Transient).
    pub fn classify_io(e: io::Error) -> Self {
        match e.kind() {
            io::ErrorKind::PermissionDenied => {
                ReplError::PermissionDenied(format!("io: {e} ({:?})", e.kind()))
            }
            io::ErrorKind::NotFound => ReplError::Permanent(format!("io: {e} ({:?})", e.kind())),
            _ => ReplError::Transient(format!("io: {e} ({:?})", e.kind())),
        }
    }

    fn io_kind_is_permanent(kind: io::ErrorKind) -> bool {
        matches!(
            kind,
            io::ErrorKind::NotFound | io::ErrorKind::PermissionDenied
        )
    }
}

/// Convenience alias for `Result<T, ReplError>`.
pub type Result<T> = std::result::Result<T, ReplError>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn transient_variants() {
        assert!(ReplError::Transient("x".into()).is_transient());
        assert!(!ReplError::Transient("x".into()).is_permanent());
        assert!(ReplError::Integrity("x".into()).is_transient());
        assert!(!ReplError::Integrity("x".into()).is_permanent());
    }

    #[test]
    fn permanent_variant() {
        assert!(ReplError::Permanent("x".into()).is_permanent());
        assert!(!ReplError::Permanent("x".into()).is_transient());
    }

    #[test]
    fn state_is_neither() {
        let e = ReplError::State("db locked".into());
        assert!(!e.is_transient());
        assert!(!e.is_permanent());
    }

    #[test]
    fn classify_io_not_found_is_permanent() {
        let e = ReplError::classify_io(io::Error::new(io::ErrorKind::NotFound, "gone"));
        assert!(e.is_permanent());
        assert!(!e.is_transient());
    }

    #[test]
    fn classify_io_permission_denied_is_permanent() {
        let e = ReplError::classify_io(io::Error::new(io::ErrorKind::PermissionDenied, "no"));
        assert!(e.is_permanent());
    }

    #[test]
    fn classify_io_permission_denied_maps_to_the_permission_denied_variant() {
        // The production path: a real backend's permission error is routed through `classify_io`, which
        // must yield the dedicated `PermissionDenied` variant so the worker's Feature A egress dedup-log
        // + event fire (matching on the raw `Io` variant, as the first cut did, was dead code — no
        // shipping destination returns `Io`).
        let e = ReplError::classify_io(io::Error::new(io::ErrorKind::PermissionDenied, "denied"));
        assert!(matches!(e, ReplError::PermissionDenied(_)), "got {e:?}");
        assert!(e.is_permission_denied());
        assert!(e.is_permanent(), "still fail-fast for the retry engine");
        assert!(!e.is_transient());
    }

    #[test]
    fn is_permission_denied_covers_the_raw_io_injection_seam_and_rejects_others() {
        // A fake destination (tests) injects the raw `Io(PermissionDenied)` variant — still detected.
        let raw: ReplError = io::Error::new(io::ErrorKind::PermissionDenied, "no").into();
        assert!(raw.is_permission_denied());
        // Everything that is NOT a permission denial is rejected — including a `NotFound` (which
        // `classify_io` maps to plain `Permanent`) and an unrelated permanent/transient error.
        assert!(!ReplError::classify_io(io::Error::new(io::ErrorKind::NotFound, "gone")).is_permission_denied());
        assert!(!ReplError::Permanent("nope".into()).is_permission_denied());
        assert!(!ReplError::Transient("later".into()).is_permission_denied());
    }

    #[test]
    fn classify_io_timeout_is_transient() {
        let e = ReplError::classify_io(io::Error::new(io::ErrorKind::TimedOut, "slow"));
        assert!(e.is_transient());
        assert!(!e.is_permanent());
    }

    #[test]
    fn from_io_preserves_kind_classification() {
        // The `#[from]` path keeps the raw Io variant; is_transient/is_permanent inspect the kind.
        let e: ReplError = io::Error::new(io::ErrorKind::NotFound, "gone").into();
        assert!(matches!(e, ReplError::Io(_)));
        assert!(e.is_permanent());

        let e: ReplError = io::Error::new(io::ErrorKind::ConnectionReset, "reset").into();
        assert!(e.is_transient());
    }

    #[test]
    fn rusqlite_error_maps_to_state() {
        let e: ReplError = rusqlite::Error::QueryReturnedNoRows.into();
        assert!(matches!(e, ReplError::State(_)));
        assert!(!e.is_transient());
        assert!(!e.is_permanent());
    }

    #[test]
    fn display_has_prefix() {
        assert_eq!(ReplError::Transient("boom".into()).to_string(), "transient: boom");
        assert_eq!(ReplError::Permanent("nope".into()).to_string(), "permanent: nope");
        assert_eq!(ReplError::PermissionDenied("no".into()).to_string(), "permission denied: no");
        assert_eq!(ReplError::Integrity("bad".into()).to_string(), "integrity: bad");
        assert_eq!(ReplError::State("db".into()).to_string(), "state: db");
    }
}
