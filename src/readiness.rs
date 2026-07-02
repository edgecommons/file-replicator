//! # file-replicator — readiness detection (DESIGN §9)
//!
//! Decides when a discovered file is *fully written* and may enter the durable queue. All four
//! strategies are here as small, independently testable predicates so the engine's watcher can drive
//! them from either the live OS-event path or the deterministic `scan()` path without depending on
//! event timing:
//!
//! | Strategy | Predicate |
//! |---|---|
//! | `stability` (default) | [`StabilityTracker`] — size+mtime unchanged for `quietSecs` |
//! | `marker` | [`marker_path`] companion exists |
//! | `rename` | [`rename_ready`] — a file renamed into the dir is atomically complete |
//! | `glob` | [`glob_ready`] — matches the ready globs and not the excludes |
//!
//! [`GlobMatcher`] + [`select`] also back include/exclude discovery filtering (FR-ING-3). Time is
//! injected via [`Clock`](crate::ratelimit::Clock) so stability is deterministic in tests.

use std::collections::{HashMap, HashSet};
use std::ffi::OsString;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, UNIX_EPOCH};

use globset::{Glob, GlobSet, GlobSetBuilder};

use crate::config::ReadinessCfg;
use crate::ratelimit::Clock;

/// A point-in-time observation of a candidate file — the inputs the stability strategy compares.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Observation {
    pub size: u64,
    pub mtime_ms: i64,
}

/// How the engine reads a candidate file's stats and marker existence. Injected so readiness can be
/// unit-tested against an in-memory fake instead of the real filesystem.
pub trait ReadinessProbe {
    /// Current size + mtime for `path`.
    fn observe(&self, path: &Path) -> io::Result<Observation>;
    /// Whether a companion marker file exists.
    fn marker_exists(&self, marker: &Path) -> bool;
}

/// The production probe backed by [`std::fs`].
pub struct RealProbe;

impl ReadinessProbe for RealProbe {
    fn observe(&self, path: &Path) -> io::Result<Observation> {
        let md = std::fs::metadata(path)?;
        let mtime_ms = md
            .modified()
            .ok()
            .and_then(|t| t.duration_since(UNIX_EPOCH).ok())
            .map(|d| d.as_millis() as i64)
            .unwrap_or(0);
        Ok(Observation {
            size: md.len(),
            mtime_ms,
        })
    }

    fn marker_exists(&self, marker: &Path) -> bool {
        marker.exists()
    }
}

/// Tracks per-file stability across successive observations (DESIGN §9 `stability`): a file is ready
/// once its `size`+`mtime` have been unchanged for at least `quiet`.
pub struct StabilityTracker {
    quiet: Duration,
    clock: Arc<dyn Clock>,
    /// path → (last observation, unchanged-since instant, whether already reported ready).
    seen: HashMap<PathBuf, Entry>,
}

struct Entry {
    last: Observation,
    since: std::time::Instant,
}

impl StabilityTracker {
    pub fn new(quiet: Duration, clock: Arc<dyn Clock>) -> Self {
        StabilityTracker {
            quiet,
            clock,
            seen: HashMap::new(),
        }
    }

    /// Feed a fresh observation for `path`; returns `true` once it has been stable for `quiet`.
    /// A changed observation resets the timer.
    pub fn observe(&mut self, path: &Path, obs: Observation) -> bool {
        let now = self.clock.now();
        match self.seen.get_mut(path) {
            Some(entry) => {
                if entry.last != obs {
                    entry.last = obs;
                    entry.since = now;
                    false
                } else {
                    now.saturating_duration_since(entry.since) >= self.quiet
                }
            }
            None => {
                self.seen.insert(
                    path.to_path_buf(),
                    Entry {
                        last: obs,
                        since: now,
                    },
                );
                // First sighting is never immediately ready (unless quiet == 0).
                self.quiet.is_zero()
            }
        }
    }

    /// Drop tracking state for a path (call on completion/removal so it can be re-evaluated fresh).
    pub fn forget(&mut self, path: &Path) {
        self.seen.remove(path);
    }

    /// Evict tracking state for every path not in `present` — the bounded-memory sweep the watcher
    /// runs after each scan so entries for deleted/completed files don't accumulate (NFR-3).
    pub fn retain(&mut self, present: &HashSet<PathBuf>) {
        self.seen.retain(|p, _| present.contains(p));
    }

    /// Number of tracked paths (diagnostics / tests).
    pub fn tracked(&self) -> usize {
        self.seen.len()
    }

    /// The shortest time until any tracked-but-not-yet-stable file will satisfy the quiet window —
    /// i.e. when a follow-up observation should be taken to (potentially) promote it to ready.
    /// `None` when nothing is mid-window. The engine self-schedules a near-term re-check off this
    /// after an OS-watch nudge: a finished file emits no further watch events, so its
    /// stability-confirming observation would otherwise only land on the next periodic rescan
    /// (making the OS watch useless for the default strategy — discovery latency ≈ `rescanSecs`).
    pub fn pending_recheck(&self) -> Option<Duration> {
        let now = self.clock.now();
        self.seen
            .values()
            .filter_map(|e| {
                let ready_at = e.since.checked_add(self.quiet)?;
                (ready_at > now).then(|| ready_at.saturating_duration_since(now))
            })
            .min()
    }
}

/// The companion marker path for `file` given a `suffix` (e.g. `a.csv` + `.done` → `a.csv.done`).
pub fn marker_path(file: &Path, suffix: &str) -> PathBuf {
    let mut s: OsString = file.as_os_str().to_owned();
    s.push(suffix);
    PathBuf::from(s)
}

/// A file arriving via an atomic rename into the watch dir is complete by construction — always ready.
/// The watcher only feeds rename-*in* events to this predicate; provenance can't be recovered from a
/// bare `scan()`, so on reconciliation existing files are treated as already-renamed-in.
pub fn rename_ready() -> bool {
    true
}

/// A compiled set of globs. An empty set matches nothing (see [`is_empty`](Self::is_empty)).
#[derive(Clone)]
pub struct GlobMatcher {
    set: GlobSet,
    count: usize,
}

impl GlobMatcher {
    /// Compile `patterns` (e.g. `["**/*.csv"]`). An empty slice yields an always-non-matching set.
    pub fn new(patterns: &[String]) -> Result<Self, globset::Error> {
        let mut b = GlobSetBuilder::new();
        for p in patterns {
            b.add(Glob::new(p)?);
        }
        Ok(GlobMatcher {
            set: b.build()?,
            count: patterns.len(),
        })
    }

    /// An empty matcher (matches nothing).
    pub fn empty() -> Self {
        GlobMatcher {
            set: GlobSet::empty(),
            count: 0,
        }
    }

    /// Whether `path` matches any pattern.
    pub fn is_match(&self, path: &Path) -> bool {
        self.set.is_match(path)
    }

    /// Whether no patterns were configured.
    pub fn is_empty(&self) -> bool {
        self.count == 0
    }
}

/// Discovery filter (FR-ING-3): `true` if `path` passes include/exclude. An empty `include` means
/// "include everything"; `exclude` always wins.
pub fn select(path: &Path, include: &GlobMatcher, exclude: &GlobMatcher) -> bool {
    if exclude.is_match(path) {
        return false;
    }
    include.is_empty() || include.is_match(path)
}

/// The `glob` readiness strategy: ready if `path` matches the ready globs (or the ready set is empty,
/// meaning "everything") and is not excluded (temp/partial patterns).
pub fn glob_ready(path: &Path, ready: &GlobMatcher, exclude: &GlobMatcher) -> bool {
    if exclude.is_match(path) {
        return false;
    }
    ready.is_empty() || ready.is_match(path)
}

/// A configured readiness strategy, built from [`ReadinessCfg`], that the engine drives per candidate.
pub enum ReadyStrategy {
    Stability(StabilityTracker),
    Marker { suffix: String },
    Rename,
    Glob { ready: GlobMatcher },
}

impl ReadyStrategy {
    /// Build the runtime strategy from config. `clock` backs the stability timer.
    pub fn from_cfg(cfg: &ReadinessCfg, clock: Arc<dyn Clock>) -> Result<Self, globset::Error> {
        Ok(match cfg {
            ReadinessCfg::Stability(s) => ReadyStrategy::Stability(StabilityTracker::new(
                Duration::from_secs(s.quiet_secs),
                clock,
            )),
            ReadinessCfg::Marker(m) => ReadyStrategy::Marker {
                suffix: m.suffix.clone(),
            },
            ReadinessCfg::Rename => ReadyStrategy::Rename,
            ReadinessCfg::Glob(g) => ReadyStrategy::Glob {
                ready: GlobMatcher::new(&g.ready)?,
            },
        })
    }

    /// Evict per-file readiness bookkeeping for paths not in `present` (bounded memory, NFR-3). Only
    /// the stability strategy holds per-file state; the others are stateless, so this is a no-op.
    pub fn retain_tracked(&mut self, present: &HashSet<PathBuf>) {
        if let ReadyStrategy::Stability(t) = self {
            t.retain(present);
        }
    }

    /// Delay until a mid-stability-window file should be re-observed (see
    /// [`StabilityTracker::pending_recheck`]). Only the time-based `stability` strategy needs this:
    /// `marker`/`rename`/`glob` all become ready on a filesystem event that itself nudges the watcher.
    pub fn pending_recheck(&self) -> Option<Duration> {
        match self {
            ReadyStrategy::Stability(t) => t.pending_recheck(),
            _ => None,
        }
    }

    /// Evaluate readiness for `path` using `probe`. `exclude` gates the glob strategy (temp files).
    pub fn is_ready(
        &mut self,
        path: &Path,
        probe: &dyn ReadinessProbe,
        exclude: &GlobMatcher,
    ) -> io::Result<bool> {
        Ok(match self {
            ReadyStrategy::Stability(tracker) => {
                let obs = probe.observe(path)?;
                tracker.observe(path, obs)
            }
            ReadyStrategy::Marker { suffix } => {
                probe.marker_exists(&marker_path(path, suffix))
            }
            ReadyStrategy::Rename => rename_ready(),
            ReadyStrategy::Glob { ready } => glob_ready(path, ready, exclude),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{GlobReadiness, MarkerReadiness, StabilityReadiness};
    use crate::ratelimit::ManualClock;
    use std::collections::HashSet;

    // ---- in-memory fake probe -------------------------------------------------------------------

    struct FakeProbe {
        stats: HashMap<PathBuf, Observation>,
        markers: HashSet<PathBuf>,
    }
    impl FakeProbe {
        fn new() -> Self {
            FakeProbe {
                stats: HashMap::new(),
                markers: HashSet::new(),
            }
        }
    }
    impl ReadinessProbe for FakeProbe {
        fn observe(&self, path: &Path) -> io::Result<Observation> {
            self.stats
                .get(path)
                .copied()
                .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "no stat"))
        }
        fn marker_exists(&self, marker: &Path) -> bool {
            self.markers.contains(marker)
        }
    }

    // ---- stability ------------------------------------------------------------------------------

    #[test]
    fn stability_not_ready_until_quiet_elapses() {
        let clock = Arc::new(ManualClock::new());
        let mut t = StabilityTracker::new(Duration::from_secs(5), clock.clone());
        let p = Path::new("a.bin");
        let obs = Observation {
            size: 10,
            mtime_ms: 1,
        };
        assert!(!t.observe(p, obs), "first sighting is not ready");
        clock.advance(Duration::from_secs(4));
        assert!(!t.observe(p, obs), "4s < 5s quiet window");
        clock.advance(Duration::from_secs(1));
        assert!(t.observe(p, obs), "5s >= quiet → ready");
    }

    #[test]
    fn stability_change_resets_timer() {
        let clock = Arc::new(ManualClock::new());
        let mut t = StabilityTracker::new(Duration::from_secs(5), clock.clone());
        let p = Path::new("a.bin");
        assert!(!t.observe(p, Observation { size: 10, mtime_ms: 1 }));
        clock.advance(Duration::from_secs(4));
        // Size grew → still being written → resets.
        assert!(!t.observe(p, Observation { size: 20, mtime_ms: 2 }));
        clock.advance(Duration::from_secs(4));
        assert!(!t.observe(p, Observation { size: 20, mtime_ms: 2 }), "only 4s since reset");
        clock.advance(Duration::from_secs(1));
        assert!(t.observe(p, Observation { size: 20, mtime_ms: 2 }));
    }

    #[test]
    fn pending_recheck_tracks_the_stability_window() {
        let clock = Arc::new(ManualClock::new());
        let mut t = StabilityTracker::new(Duration::from_secs(5), clock.clone());
        // Nothing tracked yet → no re-check scheduled.
        assert_eq!(t.pending_recheck(), None);
        // First sighting → pending; a follow-up observation is due in the full quiet window.
        let p = Path::new("a.bin");
        let obs = Observation {
            size: 10,
            mtime_ms: 1,
        };
        assert!(!t.observe(p, obs));
        assert_eq!(t.pending_recheck(), Some(Duration::from_secs(5)));
        // Part-way through the window → recheck due for the remaining time only.
        clock.advance(Duration::from_secs(2));
        assert_eq!(t.pending_recheck(), Some(Duration::from_secs(3)));
        // Once the window elapses and the file is confirmed ready, it is no longer pending — so the
        // engine stops self-scheduling re-checks (no busy loop after a file settles).
        clock.advance(Duration::from_secs(3));
        assert!(t.observe(p, obs));
        assert_eq!(t.pending_recheck(), None);
    }

    #[test]
    fn stability_zero_quiet_ready_immediately() {
        let clock = Arc::new(ManualClock::new());
        let mut t = StabilityTracker::new(Duration::ZERO, clock);
        assert!(t.observe(Path::new("a.bin"), Observation { size: 1, mtime_ms: 1 }));
    }

    #[test]
    fn stability_forget_clears_state() {
        let clock = Arc::new(ManualClock::new());
        let mut t = StabilityTracker::new(Duration::from_secs(5), clock.clone());
        let p = Path::new("a.bin");
        t.observe(p, Observation { size: 1, mtime_ms: 1 });
        assert_eq!(t.tracked(), 1);
        t.forget(p);
        assert_eq!(t.tracked(), 0);
    }

    #[test]
    fn stability_retain_evicts_absent_paths() {
        // The bounded-memory sweep: entries for files no longer present are dropped so the tracker
        // cannot grow without bound over the process lifetime (NFR-3).
        let clock = Arc::new(ManualClock::new());
        let mut t = StabilityTracker::new(Duration::from_secs(5), clock.clone());
        t.observe(Path::new("a.bin"), Observation { size: 1, mtime_ms: 1 });
        t.observe(Path::new("b.bin"), Observation { size: 2, mtime_ms: 2 });
        t.observe(Path::new("c.bin"), Observation { size: 3, mtime_ms: 3 });
        assert_eq!(t.tracked(), 3);
        // Only b.bin still exists in the current scan → a.bin/c.bin are evicted.
        let present: HashSet<PathBuf> = [PathBuf::from("b.bin")].into_iter().collect();
        t.retain(&present);
        assert_eq!(t.tracked(), 1);
        // The surviving entry keeps its progress (still within the quiet window, not reset).
        clock.advance(Duration::from_secs(5));
        assert!(t.observe(Path::new("b.bin"), Observation { size: 2, mtime_ms: 2 }));
    }

    #[test]
    fn ready_strategy_retain_is_noop_for_stateless_strategies() {
        // Non-stability strategies hold no per-file state, so retain_tracked must not panic/mutate.
        let empty: HashSet<PathBuf> = HashSet::new();
        ReadyStrategy::Rename.retain_tracked(&empty);
        ReadyStrategy::Marker {
            suffix: ".done".into(),
        }
        .retain_tracked(&empty);
        ReadyStrategy::Glob {
            ready: GlobMatcher::empty(),
        }
        .retain_tracked(&empty);
    }

    // ---- marker ---------------------------------------------------------------------------------

    #[test]
    fn marker_path_appends_suffix() {
        assert_eq!(
            marker_path(Path::new("/in/a.csv"), ".done"),
            PathBuf::from("/in/a.csv.done")
        );
    }

    #[test]
    fn marker_ready_when_companion_present() {
        let mut probe = FakeProbe::new();
        let file = Path::new("/in/a.csv");
        let mut strat = ReadyStrategy::Marker {
            suffix: ".done".to_string(),
        };
        let excl = GlobMatcher::empty();
        assert!(!strat.is_ready(file, &probe, &excl).unwrap());
        probe.markers.insert(PathBuf::from("/in/a.csv.done"));
        assert!(strat.is_ready(file, &probe, &excl).unwrap());
    }

    // ---- rename ---------------------------------------------------------------------------------

    #[test]
    fn rename_is_always_ready() {
        assert!(rename_ready());
        let mut strat = ReadyStrategy::Rename;
        let probe = FakeProbe::new();
        assert!(strat
            .is_ready(Path::new("x"), &probe, &GlobMatcher::empty())
            .unwrap());
    }

    // ---- glob + select --------------------------------------------------------------------------

    #[test]
    fn glob_matcher_basics() {
        let m = GlobMatcher::new(&["**/*.csv".to_string()]).unwrap();
        assert!(m.is_match(Path::new("a/b/c.csv")));
        assert!(!m.is_match(Path::new("a/b/c.txt")));
        assert!(!m.is_empty());
        assert!(GlobMatcher::empty().is_empty());
        assert!(!GlobMatcher::empty().is_match(Path::new("anything")));
    }

    #[test]
    fn glob_bad_pattern_errors() {
        assert!(GlobMatcher::new(&["[".to_string()]).is_err());
    }

    #[test]
    fn select_include_exclude() {
        let inc = GlobMatcher::new(&["**/*.csv".to_string()]).unwrap();
        let exc = GlobMatcher::new(&["**/*.tmp".to_string()]).unwrap();
        assert!(select(Path::new("d/x.csv"), &inc, &exc));
        assert!(!select(Path::new("d/x.txt"), &inc, &exc)); // not included
        assert!(!select(Path::new("d/x.tmp"), &inc, &exc)); // excluded
        // empty include → everything except excludes.
        let all = GlobMatcher::empty();
        assert!(select(Path::new("d/x.txt"), &all, &exc));
        assert!(!select(Path::new("d/x.tmp"), &all, &exc));
    }

    #[test]
    fn glob_ready_predicate() {
        let ready = GlobMatcher::new(&["*.csv".to_string()]).unwrap();
        let excl = GlobMatcher::new(&["*.part".to_string()]).unwrap();
        assert!(glob_ready(Path::new("a.csv"), &ready, &excl));
        assert!(!glob_ready(Path::new("a.txt"), &ready, &excl)); // not in ready set
        assert!(!glob_ready(Path::new("a.part"), &ready, &excl)); // excluded
        // empty ready set → everything not excluded.
        let any = GlobMatcher::empty();
        assert!(glob_ready(Path::new("a.txt"), &any, &excl));
        assert!(!glob_ready(Path::new("a.part"), &any, &excl));
    }

    // ---- ReadyStrategy::from_cfg ----------------------------------------------------------------

    #[test]
    fn from_cfg_builds_each_strategy() {
        let clock: Arc<dyn Clock> = Arc::new(ManualClock::new());
        assert!(matches!(
            ReadyStrategy::from_cfg(
                &ReadinessCfg::Stability(StabilityReadiness { quiet_secs: 3 }),
                clock.clone()
            )
            .unwrap(),
            ReadyStrategy::Stability(_)
        ));
        assert!(matches!(
            ReadyStrategy::from_cfg(
                &ReadinessCfg::Marker(MarkerReadiness {
                    suffix: ".ok".into()
                }),
                clock.clone()
            )
            .unwrap(),
            ReadyStrategy::Marker { .. }
        ));
        assert!(matches!(
            ReadyStrategy::from_cfg(&ReadinessCfg::Rename, clock.clone()).unwrap(),
            ReadyStrategy::Rename
        ));
        assert!(matches!(
            ReadyStrategy::from_cfg(
                &ReadinessCfg::Glob(GlobReadiness {
                    ready: vec!["*.csv".into()]
                }),
                clock
            )
            .unwrap(),
            ReadyStrategy::Glob { .. }
        ));
    }

    #[test]
    fn from_cfg_stability_end_to_end() {
        let clock = Arc::new(ManualClock::new());
        let dyn_clock: Arc<dyn Clock> = clock.clone();
        let mut strat = ReadyStrategy::from_cfg(
            &ReadinessCfg::Stability(StabilityReadiness { quiet_secs: 2 }),
            dyn_clock,
        )
        .unwrap();
        let mut probe = FakeProbe::new();
        let p = PathBuf::from("f.bin");
        probe.stats.insert(p.clone(), Observation { size: 5, mtime_ms: 1 });
        let excl = GlobMatcher::empty();
        assert!(!strat.is_ready(&p, &probe, &excl).unwrap());
        clock.advance(Duration::from_secs(2));
        assert!(strat.is_ready(&p, &probe, &excl).unwrap());
    }
}
