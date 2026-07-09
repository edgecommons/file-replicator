//! # file-replicator — discovery watcher (DESIGN §6.3/§8.2/§9)
//!
//! Two discovery paths that feed the same [`Watcher::discover`] readiness gate:
//!
//! - **Deterministic [`scan`]** — a full recursive directory walk applying the include/exclude glob
//!   filter ([`select`](crate::readiness::select)). No dependency on OS-event timing, so the engine's
//!   periodic *reconciliation rescan* and every unit test drive discovery the same way.
//! - **Live notify watch** — [`Instance::run`](crate::instance::Instance::run) attaches an OS file
//!   watcher (the `notify` crate) that simply *nudges* a tick; the tick still calls [`Watcher::discover`],
//!   so correctness never depends on which OS events arrived, only on what the scan currently sees.
//!
//! [`Watcher::discover`] promotes each scanned candidate through the configured
//! [`ReadyStrategy`](crate::readiness::ReadyStrategy) (default: a stability window) and returns only
//! the files judged **ready** — the exact set the engine durably enqueues. Because the stability
//! window compares successive observations, the engine calls `discover` repeatedly (rescan + notify
//! nudges); tests reproduce that deterministically by advancing a
//! [`ManualClock`](crate::ratelimit::ManualClock) between calls.

use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::UNIX_EPOCH;

use crate::config::IngressCfg;
use crate::ratelimit::Clock;
use crate::readiness::{select, GlobMatcher, ReadinessProbe, ReadyStrategy};

/// Extract a file's last-modified time as Unix milliseconds (`0` if the platform/entry can't
/// report it), so it can serve as the [`Candidate`] change signature.
fn mtime_ms_of(md: &std::fs::Metadata) -> i64 {
    md.modified()
        .ok()
        .and_then(|t| t.duration_since(UNIX_EPOCH).ok())
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

/// A directory that could not be walked/read during a [`scan`] (Feature A, `src/permission.rs`):
/// carried up so the caller can dedup-log it and emit a `PermissionDenied` event instead of the old
/// silent per-rescan `debug!` (the log-spam footgun this fixes — every entry here would otherwise
/// re-surface on every reconciliation rescan).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScanIssue {
    pub path: PathBuf,
    pub kind: std::io::ErrorKind,
}

/// A discovery pass with ready files plus aggregate counters for metric emission.
pub struct ScanReport {
    pub ready: Vec<Candidate>,
    pub issues: Vec<ScanIssue>,
    pub files_discovered: u64,
    pub files_ignored: u64,
}

/// A file discovered by [`scan`]: its source-root-relative, forward-slash key plus the absolute path,
/// size, and last-modified time captured from the directory entry.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Candidate {
    /// Source-root-relative, forward-slash-normalized key (the durable-store + destination identity).
    pub relpath: String,
    /// Absolute source path (`root` joined with `relpath`).
    pub abs: PathBuf,
    /// Size at scan time (bytes).
    pub size: u64,
    /// Last-modified time at scan time (Unix ms, `0` if unavailable). With `size` this is the change
    /// signature that lets the durable store tell a re-written file from a re-discovery of the same
    /// one (see [`StateStore::upsert_ready`](crate::state::StateStore::upsert_ready)).
    pub mtime_ms: i64,
}

/// Recursively (or shallowly) walk `root`, returning the files that pass the include/exclude filter.
///
/// Directories under any path in `skip` (e.g. an in-tree archive/failed folder) are pruned so
/// completed/quarantined files are never re-discovered. The result is sorted by `relpath` for
/// deterministic ordering. Unreadable directories/entries are never fatal to the scan — the walk keeps
/// going — but every failure is recorded in `issues` (Feature A) so the caller can dedup-log it and emit
/// a `PermissionDenied` event instead of silently re-attempting (and re-`debug!`-logging) it forever.
pub fn scan(
    root: &Path,
    recursive: bool,
    include: &GlobMatcher,
    exclude: &GlobMatcher,
    skip: &[PathBuf],
    issues: &mut Vec<ScanIssue>,
) -> Vec<Candidate> {
    let mut out = Vec::new();
    let mut stack = vec![root.to_path_buf()];
    while let Some(dir) = stack.pop() {
        let entries = match std::fs::read_dir(&dir) {
            Ok(e) => e,
            Err(e) => {
                issues.push(ScanIssue {
                    path: dir.clone(),
                    kind: e.kind(),
                });
                tracing::debug!(dir = %dir.display(), error = %e, "read_dir failed; skipping");
                continue;
            }
        };
        for entry in entries {
            // A per-`DirEntry` iteration error (a single entry that can't be read/stat'd while
            // enumerating an otherwise-readable directory — e.g. a per-entry permission failure) is
            // recorded as a [`ScanIssue`] instead of being silently dropped by `.flatten()`, keeping
            // Feature A's "every walk failure is recorded (dedup-logged), never a silent skip" contract.
            let entry = match entry {
                Ok(e) => e,
                Err(e) => {
                    issues.push(ScanIssue {
                        path: dir.clone(),
                        kind: e.kind(),
                    });
                    continue;
                }
            };
            let path = entry.path();
            if skip.iter().any(|s| path.starts_with(s)) {
                continue;
            }
            let ft = match entry.file_type() {
                Ok(t) => t,
                Err(e) => {
                    issues.push(ScanIssue {
                        path: path.clone(),
                        kind: e.kind(),
                    });
                    continue;
                }
            };
            if ft.is_dir() {
                if recursive {
                    stack.push(path);
                }
                continue;
            }
            if !ft.is_file() {
                continue; // symlinks/others: not replicated in P1
            }
            let Ok(rel) = path.strip_prefix(root) else {
                continue;
            };
            let relpath = rel
                .components()
                .filter_map(|c| c.as_os_str().to_str())
                .collect::<Vec<_>>()
                .join("/");
            if relpath.is_empty() {
                continue;
            }
            if !select(Path::new(&relpath), include, exclude) {
                continue;
            }
            let (size, mtime_ms) = entry
                .metadata()
                .map(|m| (m.len(), mtime_ms_of(&m)))
                .unwrap_or((0, 0));
            out.push(Candidate {
                relpath,
                abs: path,
                size,
                mtime_ms,
            });
        }
    }
    out.sort_by(|a, b| a.relpath.cmp(&b.relpath));
    out
}

/// The per-instance discovery watcher: the compiled glob filters, the readiness strategy, and the
/// pruned in-tree directories. Holds mutable readiness state (the stability tracker), so
/// [`discover`](Self::discover) takes `&mut self`.
pub struct Watcher {
    root: PathBuf,
    recursive: bool,
    include: GlobMatcher,
    exclude: GlobMatcher,
    strategy: ReadyStrategy,
    skip: Vec<PathBuf>,
}

impl Watcher {
    /// Build a watcher from an instance's `ingress` config. `clock` backs the stability window; `skip`
    /// lists directories (e.g. archive/failed) to prune from the walk. Fails only on a bad glob.
    pub fn from_cfg(
        ingress: &IngressCfg,
        clock: Arc<dyn Clock>,
        skip: Vec<PathBuf>,
    ) -> Result<Self, globset::Error> {
        Ok(Watcher {
            root: ingress.path.clone(),
            recursive: ingress.recursive,
            include: GlobMatcher::new(&ingress.include)?,
            exclude: GlobMatcher::new(&ingress.exclude)?,
            strategy: ReadyStrategy::from_cfg(&ingress.readiness, clock)?,
            skip,
        })
    }

    /// The watched source root.
    pub fn root(&self) -> &Path {
        &self.root
    }

    /// Whether the walk recurses into subdirectories.
    pub fn recursive(&self) -> bool {
        self.recursive
    }

    /// Delay until a file currently inside its stability window is due for a follow-up observation,
    /// or `None` when nothing is pending. The run loop self-schedules a near-term re-check off this so
    /// OS-watch-discovered files don't wait out the full reconciliation rescan (see
    /// [`crate::readiness::StabilityTracker::pending_recheck`]).
    pub fn pending_recheck(&self) -> Option<std::time::Duration> {
        self.strategy.pending_recheck()
    }

    /// Scan the tree and return the candidates judged **ready** by the configured strategy, plus any
    /// [`ScanIssue`]s hit while walking (Feature A — the caller dedup-logs + emits `PermissionDenied`
    /// for these instead of the old silent per-rescan skip). A candidate whose readiness probe fails
    /// with a *permission* denial is likewise recorded as a [`ScanIssue`] (so a file-level permission
    /// error gets the same bounded, once-only signal); any other probe error (e.g. the file vanished
    /// between scan and probe) is a transient, self-correcting condition and is quietly skipped.
    pub fn discover(&mut self, probe: &dyn ReadinessProbe) -> (Vec<Candidate>, Vec<ScanIssue>) {
        let report = self.discover_report(probe);
        (report.ready, report.issues)
    }

    /// As [`discover`](Self::discover), plus aggregate counters for metric emission.
    pub fn discover_report(&mut self, probe: &dyn ReadinessProbe) -> ScanReport {
        let mut issues = Vec::new();
        let candidates = scan(
            &self.root,
            self.recursive,
            &self.include,
            &self.exclude,
            &self.skip,
            &mut issues,
        );
        // Snapshot the paths present this scan so per-file readiness bookkeeping (the stability
        // tracker) can be evicted for files that no longer exist — otherwise it would grow without
        // bound over the process lifetime on a high-churn spool (NFR-3 bounded memory).
        let files_discovered = candidates.len() as u64;
        let mut files_ignored = 0;
        let present: HashSet<PathBuf> = candidates.iter().map(|c| c.abs.clone()).collect();
        let mut ready = Vec::with_capacity(candidates.len());
        for c in candidates {
            match self.strategy.is_ready(&c.abs, probe, &self.exclude) {
                Ok(true) => ready.push(c),
                Ok(false) => {
                    files_ignored += 1;
                }
                Err(e) if e.kind() == std::io::ErrorKind::PermissionDenied => {
                    // A file the scan could list but cannot open/stat because of a *permission* denial
                    // (Feature A): route it through the same dedup+`PermissionDenied` event path as an
                    // unreadable directory, rather than the silent per-rescan `debug!` a file-level
                    // permission error would otherwise get (which would re-log every rescan, forever).
                    issues.push(ScanIssue {
                        path: c.abs,
                        kind: e.kind(),
                    });
                }
                Err(e) => {
                    // Any other probe error (e.g. the file vanished between scan and probe) is a
                    // transient, self-correcting condition — a quiet debug line, not a scan issue.
                    files_ignored += 1;
                    tracing::debug!(path = %c.abs.display(), error = %e, "readiness probe failed");
                }
            }
        }
        self.strategy.retain_tracked(&present);
        ScanReport {
            ready,
            issues,
            files_discovered,
            files_ignored,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{ReadinessCfg, StabilityReadiness};
    use crate::ratelimit::ManualClock;
    use crate::readiness::RealProbe;
    use std::fs;
    use std::time::Duration;

    fn write(path: &Path, bytes: &[u8]) {
        if let Some(p) = path.parent() {
            fs::create_dir_all(p).unwrap();
        }
        fs::write(path, bytes).unwrap();
    }

    fn ingress(root: &Path, recursive: bool, include: &[&str], exclude: &[&str]) -> IngressCfg {
        IngressCfg {
            path: root.to_path_buf(),
            recursive,
            include: include.iter().map(|s| s.to_string()).collect(),
            exclude: exclude.iter().map(|s| s.to_string()).collect(),
            rescan_secs: None,
            // glob strategy = ready immediately (decouples scan tests from the stability timer).
            readiness: ReadinessCfg::Glob(crate::config::GlobReadiness { ready: vec![] }),
        }
    }

    #[test]
    fn scan_filters_include_exclude_and_normalizes_relpath() {
        let dir = tempfile::tempdir().unwrap();
        write(&dir.path().join("a.csv"), b"1");
        write(&dir.path().join("skip.tmp"), b"2");
        write(&dir.path().join("nested/b.csv"), b"33");
        let inc = GlobMatcher::new(&["**/*.csv".into()]).unwrap();
        let exc = GlobMatcher::new(&["**/*.tmp".into()]).unwrap();
        let mut issues = Vec::new();
        let got = scan(dir.path(), true, &inc, &exc, &[], &mut issues);
        let rels: Vec<_> = got.iter().map(|c| c.relpath.as_str()).collect();
        assert_eq!(rels, vec!["a.csv", "nested/b.csv"]); // forward-slash, sorted, .tmp excluded
        assert_eq!(
            got.iter()
                .find(|c| c.relpath == "nested/b.csv")
                .unwrap()
                .size,
            2
        );
        assert!(issues.is_empty(), "a clean tree has no scan issues");
    }

    #[test]
    fn scan_non_recursive_ignores_subdirs() {
        let dir = tempfile::tempdir().unwrap();
        write(&dir.path().join("top.csv"), b"1");
        write(&dir.path().join("sub/deep.csv"), b"2");
        let all = GlobMatcher::empty();
        let mut issues = Vec::new();
        let got = scan(
            dir.path(),
            false,
            &all,
            &GlobMatcher::empty(),
            &[],
            &mut issues,
        );
        let rels: Vec<_> = got.iter().map(|c| c.relpath.as_str()).collect();
        assert_eq!(rels, vec!["top.csv"]);
    }

    #[test]
    fn scan_prunes_skip_dirs() {
        let dir = tempfile::tempdir().unwrap();
        write(&dir.path().join("keep.csv"), b"1");
        write(&dir.path().join("archive/done.csv"), b"2");
        let all = GlobMatcher::empty();
        let skip = vec![dir.path().join("archive")];
        let mut issues = Vec::new();
        let got = scan(
            dir.path(),
            true,
            &all,
            &GlobMatcher::empty(),
            &skip,
            &mut issues,
        );
        let rels: Vec<_> = got.iter().map(|c| c.relpath.as_str()).collect();
        assert_eq!(rels, vec!["keep.csv"], "in-tree archive dir must be pruned");
    }

    #[test]
    fn scan_reports_an_unreadable_directory_as_an_issue() {
        // A FILE where a subdirectory-to-walk is expected makes `read_dir` fail deterministically
        // cross-platform, standing in for a permission-denied directory (Feature A).
        let dir = tempfile::tempdir().unwrap();
        write(&dir.path().join("keep.csv"), b"1");
        let not_a_dir = dir.path().join("looks-like-a-dir");
        write(&not_a_dir, b"actually a file");
        // Force it onto the walk stack as if it were a directory by scanning IT directly as root.
        let all = GlobMatcher::empty();
        let mut issues = Vec::new();
        let got = scan(
            &not_a_dir,
            true,
            &all,
            &GlobMatcher::empty(),
            &[],
            &mut issues,
        );
        assert!(got.is_empty());
        assert_eq!(issues.len(), 1);
        assert_eq!(issues[0].path, not_a_dir);
    }

    /// A readiness probe that always fails with a chosen [`io::ErrorKind`] — drives the `discover`
    /// probe-error branches (Feature A) deterministically, without needing real un-openable files.
    struct FailProbe(std::io::ErrorKind);
    impl ReadinessProbe for FailProbe {
        fn observe(&self, _path: &Path) -> std::io::Result<crate::readiness::Observation> {
            Err(std::io::Error::new(self.0, "probe failed"))
        }
        fn marker_exists(&self, _marker: &Path) -> bool {
            false
        }
    }

    #[test]
    fn discover_records_a_permission_denied_readiness_probe_as_a_scan_issue() {
        // A file the scan lists but the readiness probe can't open/stat due to a permission denial is
        // surfaced as a ScanIssue (→ dedup + PermissionDenied event), not a silent per-rescan skip.
        let dir = tempfile::tempdir().unwrap();
        write(&dir.path().join("f.bin"), b"hello");
        let clock: Arc<dyn Clock> = Arc::new(ManualClock::new());
        let mut ing = ingress(dir.path(), false, &[], &[]);
        // The stability strategy calls `probe.observe`, so a failing probe reaches the error branch.
        ing.readiness = ReadinessCfg::Stability(StabilityReadiness { quiet_secs: 5 });
        let mut w = Watcher::from_cfg(&ing, clock, vec![]).unwrap();

        let (ready, issues) = w.discover(&FailProbe(std::io::ErrorKind::PermissionDenied));
        assert!(ready.is_empty());
        assert_eq!(issues.len(), 1);
        assert_eq!(issues[0].kind, std::io::ErrorKind::PermissionDenied);
        assert_eq!(issues[0].path, dir.path().join("f.bin"));
    }

    #[test]
    fn discover_skips_a_non_permission_readiness_probe_error_without_an_issue() {
        // A non-permission probe error (e.g. the file vanished between scan and probe) is transient and
        // self-correcting: quietly skipped, NOT recorded as a scan issue.
        let dir = tempfile::tempdir().unwrap();
        write(&dir.path().join("f.bin"), b"hello");
        let clock: Arc<dyn Clock> = Arc::new(ManualClock::new());
        let mut ing = ingress(dir.path(), false, &[], &[]);
        ing.readiness = ReadinessCfg::Stability(StabilityReadiness { quiet_secs: 5 });
        let mut w = Watcher::from_cfg(&ing, clock, vec![]).unwrap();

        let (ready, issues) = w.discover(&FailProbe(std::io::ErrorKind::NotFound));
        assert!(ready.is_empty());
        assert!(
            issues.is_empty(),
            "a non-permission probe error is not a scan issue"
        );
    }

    #[test]
    fn discover_glob_strategy_is_immediate() {
        let dir = tempfile::tempdir().unwrap();
        write(&dir.path().join("a.csv"), b"data");
        let clock: Arc<dyn Clock> = Arc::new(ManualClock::new());
        let mut w = Watcher::from_cfg(
            &ingress(dir.path(), true, &["**/*.csv"], &[]),
            clock,
            vec![],
        )
        .unwrap();
        let (ready, issues) = w.discover(&RealProbe);
        assert_eq!(ready.len(), 1);
        assert_eq!(ready[0].relpath, "a.csv");
        assert_eq!(w.root(), dir.path());
        assert!(w.recursive());
        assert!(issues.is_empty());
    }

    #[test]
    fn discover_stability_waits_for_quiet_window() {
        let dir = tempfile::tempdir().unwrap();
        write(&dir.path().join("f.bin"), b"hello");
        let clock = Arc::new(ManualClock::new());
        let dyn_clock: Arc<dyn Clock> = clock.clone();
        let mut ing = ingress(dir.path(), false, &[], &[]);
        ing.readiness = ReadinessCfg::Stability(StabilityReadiness { quiet_secs: 5 });
        let mut w = Watcher::from_cfg(&ing, dyn_clock, vec![]).unwrap();
        // First observation: not yet stable.
        assert!(w.discover(&RealProbe).0.is_empty());
        // The file is untouched; after the quiet window it is promoted to ready.
        clock.advance(Duration::from_secs(5));
        let (ready, _issues) = w.discover(&RealProbe);
        assert_eq!(ready.len(), 1);
        assert_eq!(ready[0].relpath, "f.bin");
    }

    #[test]
    fn from_cfg_rejects_bad_glob() {
        let dir = tempfile::tempdir().unwrap();
        let clock: Arc<dyn Clock> = Arc::new(ManualClock::new());
        let ing = ingress(dir.path(), false, &["["], &[]);
        assert!(Watcher::from_cfg(&ing, clock, vec![]).is_err());
    }
}
