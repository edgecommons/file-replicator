//! # file-replicator — permission handling (Feature A)
//!
//! Two halves:
//!
//! - **Startup validation** ([`validate_instance`]): before an instance starts, probe its ingress dir
//!   (readable), every `local` egress dir (writable), and `completion.archiveDir`/`failedDir`
//!   (creatable-or-writable). A violation is resolved against the instance's effective
//!   [`PermissionPolicy`](crate::config::PermissionPolicy) via [`apply_policy`] — `disableInstance`
//!   (default) skips just that instance (the existing FR-CFG-4 skip-bad-instance path, so a bad
//!   instance never stops its siblings); `fatal` aborts the whole component; `retain` starts it anyway,
//!   leaning on the runtime dedup-logging below for the ongoing diagnostic signal.
//! - **Dedup log-once** ([`PermissionLog`]): a permission/access error must ALWAYS be logged and
//!   surfaced as a `PermissionDenied` event, but never per-rescan (the footgun this fixes — see
//!   `src/instance/watcher.rs`'s module docs). [`PermissionLog::should_log`] is true on first sighting
//!   (a genuine state change) or after a long re-log interval; false otherwise.
//!   [`PermissionLog::recovered`] evicts entries for keys no longer erroring, bounding memory and
//!   re-arming logging if the same key breaks again later.
//!
//!   An instance keeps **two independent** `PermissionLog`s, NOT one shared log (see
//!   [`crate::instance::Instance`]): the *ingress* log is keyed by watched-directory path and pruned
//!   every tick via [`recovered`](PermissionLog::recovered) against that scan's still-erroring set; the
//!   *egress* log (the instance's `Worker`) is keyed by destination **label** so a broken destination
//!   logs once (bounded by the fixed destination count, no per-file growth). They must not
//!   be shared: the ingress recovery pass only knows ingress paths, so a shared log would evict every
//!   egress entry each tick and re-arm the very spam this module prevents.

use std::collections::{HashMap, HashSet};
use std::io;
use std::path::PathBuf;
use std::sync::Mutex;

use crate::config::{EgressCfg, InstanceCfg, PermissionPolicy};

/// Re-log interval for a persistently-failing path (ms) — long enough that a stuck instance doesn't
/// spam logs, short enough that a long-lived outage still gets periodic visibility. One hour.
pub const RELOG_INTERVAL_MS: i64 = 60 * 60 * 1000;

/// Which directory role a [`Violation`] (or a runtime [`PermissionLog`] entry) concerns — carried into
/// the `PermissionDenied` event body and the disabled-instance status doc.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Role {
    Ingress,
    Egress,
    Archive,
    Failed,
}

impl Role {
    pub fn as_str(&self) -> &'static str {
        match self {
            Role::Ingress => "ingress",
            Role::Egress => "egress",
            Role::Archive => "archive",
            Role::Failed => "failed",
        }
    }
}

/// One startup-validation failure: a directory that could not be probed as required by its [`Role`].
#[derive(Debug, Clone)]
pub struct Violation {
    pub path: PathBuf,
    pub role: Role,
    pub kind: io::ErrorKind,
    pub msg: String,
}

/// Try to create `dir` (recursively) if it doesn't exist, then prove it's writable by creating and
/// removing a throwaway probe file in it. Returns the classified I/O error on any failure.
fn probe_writable(dir: &std::path::Path) -> Result<(), io::Error> {
    std::fs::create_dir_all(dir)?;
    let probe = dir.join(".file-replicator-write-probe");
    std::fs::write(&probe, b"")?;
    let _ = std::fs::remove_file(&probe);
    Ok(())
}

/// Probe every directory an instance depends on: the ingress source (readable), each `local` egress
/// destination (writable — off-filesystem egress kinds like S3 are skipped, they have no local dir to
/// probe), and `completion.archiveDir`/`failedDir` when configured (creatable-or-writable). Returns one
/// [`Violation`] per failing directory; an empty result means the instance is fully viable.
pub fn validate_instance(cfg: &InstanceCfg) -> Vec<Violation> {
    let mut violations = Vec::new();

    if let Err(e) = std::fs::read_dir(&cfg.ingress.path) {
        violations.push(Violation {
            path: cfg.ingress.path.clone(),
            role: Role::Ingress,
            kind: e.kind(),
            msg: e.to_string(),
        });
    }

    for e in &cfg.egress {
        if let EgressCfg::Local(local) = e {
            if let Err(err) = probe_writable(&local.path) {
                violations.push(Violation {
                    path: local.path.clone(),
                    role: Role::Egress,
                    kind: err.kind(),
                    msg: err.to_string(),
                });
            }
        }
    }

    if let Some(dir) = &cfg.completion.archive_dir {
        if let Err(err) = probe_writable(dir) {
            violations.push(Violation {
                path: dir.clone(),
                role: Role::Archive,
                kind: err.kind(),
                msg: err.to_string(),
            });
        }
    }
    if let Some(dir) = &cfg.completion.failed_dir {
        if let Err(err) = probe_writable(dir) {
            violations.push(Violation {
                path: dir.clone(),
                role: Role::Failed,
                kind: err.kind(),
                msg: err.to_string(),
            });
        }
    }

    violations
}

/// What [`apply_policy`] says to do with an instance given its resolved policy + startup violations.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PolicyOutcome {
    /// No violations, or the policy is `retain`: build + start the instance normally.
    Start,
    /// `disableInstance` with at least one violation: skip just this instance (siblings unaffected).
    Skip,
    /// `fatal` with at least one violation: abort the whole component.
    Fatal,
}

/// Pure decision function (directly unit-testable, no I/O): fold a resolved
/// [`PermissionPolicy`](crate::config::PermissionPolicy) + a startup violation set into a
/// [`PolicyOutcome`]. No violations always starts, regardless of policy.
pub fn apply_policy(policy: PermissionPolicy, violations: &[Violation]) -> PolicyOutcome {
    if violations.is_empty() {
        return PolicyOutcome::Start;
    }
    match policy {
        PermissionPolicy::DisableInstance => PolicyOutcome::Skip,
        PermissionPolicy::Fatal => PolicyOutcome::Fatal,
        PermissionPolicy::Retain => PolicyOutcome::Start,
    }
}

/// One dedup entry: when this (path, error-kind) pair was last logged.
struct Entry {
    last_logged_ms: i64,
}

/// Per-instance dedup state for permission/access-error logging: turns "log every rescan" into "log
/// once, then again only on a state change (recovery) or after [`RELOG_INTERVAL_MS`]". An instance
/// holds TWO of these (see the module docs) — one for ingress (keyed by directory path), one for egress
/// (keyed by destination label) — deliberately NOT shared, because their key spaces and eviction
/// lifecycles differ. Bounded: for the ingress log an entry only exists for a path that is *currently*
/// failing ([`recovered`](Self::recovered) evicts it once the path is accessible again); the egress log
/// is bounded instead by the fixed number of configured destinations.
pub struct PermissionLog {
    entries: Mutex<HashMap<(String, io::ErrorKind), Entry>>,
}

impl PermissionLog {
    pub fn new() -> Self {
        Self {
            entries: Mutex::new(HashMap::new()),
        }
    }

    /// Whether a permission/access error at `path` (of `kind`) should be logged now, at `now_ms`. True
    /// on first sighting (a state change — nothing was tracked for this path/kind before) or once
    /// [`RELOG_INTERVAL_MS`] has elapsed since the last log; false otherwise (already logged, still
    /// within the quiet window — this is what stops per-rescan spam). A `true` result updates the
    /// tracked `last_logged_ms`.
    pub fn should_log(&self, path: &str, kind: io::ErrorKind, now_ms: i64) -> bool {
        let mut entries = self.entries.lock().expect("permission log mutex");
        match entries.get_mut(&(path.to_string(), kind)) {
            Some(entry) => {
                if now_ms - entry.last_logged_ms >= RELOG_INTERVAL_MS {
                    entry.last_logged_ms = now_ms;
                    true
                } else {
                    false
                }
            }
            None => {
                entries.insert((path.to_string(), kind), Entry { last_logged_ms: now_ms });
                true
            }
        }
    }

    /// Evict every tracked entry whose path is not in `still_erroring` (bounded memory — an instance
    /// with a high-churn spool never accumulates dedup state for paths that recovered), returning the
    /// distinct paths that recovered so the caller can log a one-line INFO "access restored" (itself a
    /// state change worth announcing). Call after each discovery pass / worker error report, and once
    /// more at instance stop (with an empty `still_erroring`) to drain.
    pub fn recovered(&self, still_erroring: &HashSet<String>) -> Vec<String> {
        let mut entries = self.entries.lock().expect("permission log mutex");
        let gone: Vec<(String, io::ErrorKind)> = entries
            .keys()
            .filter(|(path, _)| !still_erroring.contains(path))
            .cloned()
            .collect();
        let mut recovered_paths: Vec<String> = Vec::new();
        for key in gone {
            entries.remove(&key);
            if !recovered_paths.contains(&key.0) {
                recovered_paths.push(key.0);
            }
        }
        recovered_paths
    }

    /// Current number of tracked (path, kind) entries — diagnostics / bounded-memory assertions.
    #[cfg(test)]
    fn len(&self) -> usize {
        self.entries.lock().expect("permission log mutex").len()
    }
}

impl Default for PermissionLog {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{CompletionCfg, EgressCfg, IngressCfg, LocalEgress, ReadinessCfg, ScheduleCfg};

    fn cfg(ingress: PathBuf, egress: Vec<EgressCfg>, completion: CompletionCfg) -> InstanceCfg {
        InstanceCfg {
            id: "t".to_string(),
            enabled: true,
            ingress: IngressCfg {
                path: ingress,
                recursive: false,
                include: vec![],
                exclude: vec![],
                rescan_secs: None,
                readiness: ReadinessCfg::default(),
            },
            egress,
            schedule: ScheduleCfg::Immediate,
            completion,
            retry: None,
            limits: None,
            topics: None,
            on_permission_error: None,
            priority: 100,
        }
    }

    // ---- validate_instance -----------------------------------------------------------------------

    #[test]
    fn validate_instance_clean_dirs_no_violations() {
        let src = tempfile::tempdir().unwrap();
        let dst = tempfile::tempdir().unwrap();
        let c = cfg(
            src.path().to_path_buf(),
            vec![EgressCfg::Local(LocalEgress { path: dst.path().to_path_buf(), fsync: false })],
            CompletionCfg::default(),
        );
        assert!(validate_instance(&c).is_empty());
    }

    #[test]
    fn validate_instance_reports_ingress_violation() {
        // A FILE where a directory is expected makes `read_dir` fail deterministically cross-platform.
        let dir = tempfile::tempdir().unwrap();
        let not_a_dir = dir.path().join("f.txt");
        std::fs::write(&not_a_dir, b"x").unwrap();
        let dst = tempfile::tempdir().unwrap();
        let c = cfg(
            not_a_dir.clone(),
            vec![EgressCfg::Local(LocalEgress { path: dst.path().to_path_buf(), fsync: false })],
            CompletionCfg::default(),
        );
        let violations = validate_instance(&c);
        assert_eq!(violations.len(), 1);
        assert_eq!(violations[0].role, Role::Ingress);
        assert_eq!(violations[0].path, not_a_dir);
    }

    #[test]
    fn validate_instance_reports_egress_and_archive_violations() {
        let src = tempfile::tempdir().unwrap();
        let bad_egress = src.path().join("out/f.txt"); // a file masquerading as the egress dir
        std::fs::create_dir_all(src.path().join("out")).unwrap();
        std::fs::write(&bad_egress, b"x").unwrap();
        let bad_archive = bad_egress.join("nested"); // create_dir_all under a file also fails
        let c = cfg(
            src.path().to_path_buf(),
            vec![EgressCfg::Local(LocalEgress { path: bad_egress.clone(), fsync: false })],
            CompletionCfg {
                archive_dir: Some(bad_archive.clone()),
                ..Default::default()
            },
        );
        let violations = validate_instance(&c);
        let roles: Vec<Role> = violations.iter().map(|v| v.role).collect();
        assert!(roles.contains(&Role::Egress), "{roles:?}");
        assert!(roles.contains(&Role::Archive), "{roles:?}");
    }

    #[test]
    fn validate_instance_skips_off_filesystem_egress() {
        // No local egress at all → nothing to probe there; only ingress is checked.
        let src = tempfile::tempdir().unwrap();
        let c = cfg(src.path().to_path_buf(), vec![], CompletionCfg::default());
        assert!(validate_instance(&c).is_empty());
    }

    // ---- apply_policy ------------------------------------------------------------------------------

    fn one_violation() -> Vec<Violation> {
        vec![Violation {
            path: "/bad".into(),
            role: Role::Ingress,
            kind: io::ErrorKind::PermissionDenied,
            msg: "denied".into(),
        }]
    }

    #[test]
    fn apply_policy_matrix() {
        let v = one_violation();
        assert_eq!(apply_policy(PermissionPolicy::DisableInstance, &v), PolicyOutcome::Skip);
        assert_eq!(apply_policy(PermissionPolicy::Fatal, &v), PolicyOutcome::Fatal);
        assert_eq!(apply_policy(PermissionPolicy::Retain, &v), PolicyOutcome::Start);
    }

    #[test]
    fn apply_policy_no_violations_always_starts() {
        for policy in [
            PermissionPolicy::DisableInstance,
            PermissionPolicy::Fatal,
            PermissionPolicy::Retain,
        ] {
            assert_eq!(apply_policy(policy, &[]), PolicyOutcome::Start);
        }
    }

    // ---- PermissionLog -------------------------------------------------------------------------

    #[test]
    fn permission_log_logs_once_not_per_rescan() {
        let log = PermissionLog::new();
        let kind = io::ErrorKind::PermissionDenied;
        assert!(log.should_log("/a", kind, 1_000), "first sighting logs");
        assert!(!log.should_log("/a", kind, 1_001), "immediate re-check does not");
        assert!(!log.should_log("/a", kind, 60_000), "still within the quiet window");
    }

    #[test]
    fn permission_log_relogs_after_interval() {
        let log = PermissionLog::new();
        let kind = io::ErrorKind::PermissionDenied;
        assert!(log.should_log("/a", kind, 0));
        assert!(!log.should_log("/a", kind, RELOG_INTERVAL_MS - 1));
        assert!(log.should_log("/a", kind, RELOG_INTERVAL_MS), "interval elapsed");
    }

    #[test]
    fn permission_log_recovery_evicts_and_allows_relog() {
        let log = PermissionLog::new();
        let kind = io::ErrorKind::PermissionDenied;
        assert!(log.should_log("/a", kind, 0));
        assert_eq!(log.len(), 1);

        // /a no longer in the still-erroring set → evicted, reported as recovered.
        let recovered = log.recovered(&HashSet::new());
        assert_eq!(recovered, vec!["/a".to_string()]);
        assert_eq!(log.len(), 0, "bounded: nothing tracked once recovered");

        // The same path breaking again immediately logs again (the entry was evicted, so this reads as
        // a fresh state change, not a suppressed repeat).
        assert!(log.should_log("/a", kind, 1), "re-armed after recovery");
    }

    #[test]
    fn permission_log_retain_is_bounded_to_still_erroring_set() {
        let log = PermissionLog::new();
        let kind = io::ErrorKind::PermissionDenied;
        log.should_log("/a", kind, 0);
        log.should_log("/b", kind, 0);
        log.should_log("/c", kind, 0);
        assert_eq!(log.len(), 3);

        let mut still: HashSet<String> = HashSet::new();
        still.insert("/b".to_string());
        let mut recovered = log.recovered(&still);
        recovered.sort(); // HashMap iteration order is unspecified
        assert_eq!(recovered, vec!["/a".to_string(), "/c".to_string()]);
        assert_eq!(log.len(), 1, "only the still-erroring path remains tracked");
    }

    #[test]
    fn permission_log_distinguishes_by_kind() {
        // Two different error kinds at the same path are independent dedup entries.
        let log = PermissionLog::new();
        assert!(log.should_log("/a", io::ErrorKind::PermissionDenied, 0));
        assert!(log.should_log("/a", io::ErrorKind::NotFound, 0), "different kind, independent entry");
        assert_eq!(log.len(), 2);
    }

    #[test]
    fn role_as_str_matches_the_wire_vocabulary() {
        assert_eq!(Role::Ingress.as_str(), "ingress");
        assert_eq!(Role::Egress.as_str(), "egress");
        assert_eq!(Role::Archive.as_str(), "archive");
        assert_eq!(Role::Failed.as_str(), "failed");
    }
}
