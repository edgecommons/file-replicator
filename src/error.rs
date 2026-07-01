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
            ReplError::Permanent(_) | ReplError::State(_) => false,
        }
    }

    /// True for failures that should fail fast to `Exhausted`: [`Permanent`](Self::Permanent) and
    /// permanent-kind [`Io`](Self::Io) (`NotFound`, `PermissionDenied`).
    ///
    /// Note [`State`](Self::State) is neither transient nor permanent here: a durable-store failure
    /// is an engine-level fault handled out of band, not a per-item retry/exhaust decision.
    pub fn is_permanent(&self) -> bool {
        match self {
            ReplError::Permanent(_) => true,
            ReplError::Io(e) => Self::io_kind_is_permanent(e.kind()),
            ReplError::Transient(_) | ReplError::Integrity(_) | ReplError::State(_) => false,
        }
    }

    /// Map a raw [`io::Error`] to a classified [`ReplError`]: `NotFound` / `PermissionDenied`
    /// become [`Permanent`](Self::Permanent); everything else is treated as [`Transient`](Self::Transient).
    pub fn classify_io(e: io::Error) -> Self {
        if Self::io_kind_is_permanent(e.kind()) {
            ReplError::Permanent(format!("io: {e} ({:?})", e.kind()))
        } else {
            ReplError::Transient(format!("io: {e} ({:?})", e.kind()))
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
        assert_eq!(ReplError::Integrity("bad".into()).to_string(), "integrity: bad");
        assert_eq!(ReplError::State("db".into()).to_string(), "state: db");
    }
}
