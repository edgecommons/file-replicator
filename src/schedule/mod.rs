//! # file-replicator — scheduling model (DESIGN §12)
//!
//! `immediate` (default, unchanged P1 behavior) | `cron` (point trigger — release all `Ready` work at
//! each fire) | `window` (continuous flow gated to an `open..close` span). Cron expressions are
//! evaluated tz/DST-aware via [`croner`] + [`chrono_tz`] ([`cron`]); a window is `open` (cron) + either
//! `close` (cron) or `durationMins` ([`window`]). Plain-English phrases are optional sugar compiled to
//! the same cron model ([`sugar`]) — cron is authoritative, sugar is a documented convenience for a
//! handful of common forms.
//!
//! Every decision here is `now`-parameterized (mirrors [`crate::instance::Instance::tick`]'s
//! deterministic seam), so the whole model — cron fires, window open/close, English sugar — is
//! unit-testable with zero real sleeps. [`WallClock`] exists only for the process's scheduler wake
//! timer, which does need a real (or manually-driven) source of "now".

pub mod cron;
pub mod sugar;
pub mod window;

use std::str::FromStr;
use std::sync::Mutex;

use chrono::{DateTime, Utc};
use chrono_tz::Tz;

pub use cron::CronExpr;
pub use window::{Close, WindowSched, WindowState};

use crate::config::{CronSchedule, ScheduleCfg, WindowClose, WindowSchedule};

/// Injectable wall-clock time source — analogous to [`crate::ratelimit::Clock`], but wall time:
/// cron/window evaluation needs a real calendar instant (for tz/DST conversion), not a monotonic one.
pub trait WallClock: Send + Sync {
    fn now_utc(&self) -> DateTime<Utc>;
}

/// Production wall clock backed by [`Utc::now`].
pub struct SystemWallClock;
impl WallClock for SystemWallClock {
    fn now_utc(&self) -> DateTime<Utc> {
        Utc::now()
    }
}

/// Test wall clock, set/advanced by hand — mirrors [`crate::ratelimit::ManualClock`]'s pattern.
pub struct ManualWallClock(Mutex<DateTime<Utc>>);
impl ManualWallClock {
    pub fn new(t: DateTime<Utc>) -> Self {
        ManualWallClock(Mutex::new(t))
    }
    pub fn set(&self, t: DateTime<Utc>) {
        *self.0.lock().expect("wall clock mutex") = t;
    }
    pub fn advance(&self, d: chrono::Duration) {
        let mut t = self.0.lock().expect("wall clock mutex");
        *t += d;
    }
}
impl WallClock for ManualWallClock {
    fn now_utc(&self) -> DateTime<Utc> {
        *self.0.lock().expect("wall clock mutex")
    }
}

/// A malformed schedule (bad cron expression, unknown timezone, unrecognized English phrase, or an
/// invalid window) — the instance fails to build (skip-bad-instance, FR-CFG-4), like every other
/// per-instance config error.
#[derive(Debug, thiserror::Error)]
pub enum ScheduleError {
    #[error("invalid cron expression {expr:?}: {source}")]
    InvalidCron {
        expr: String,
        #[source]
        source: croner::errors::CronError,
    },
    #[error("unknown schedule timezone {0:?}")]
    InvalidTimezone(String),
    #[error("unrecognized schedule phrase {0:?} (not valid cron and matches no supported English form)")]
    UnrecognizedPhrase(String),
    #[error("window schedule must set exactly one of `close` / `durationMins`")]
    AmbiguousWindowClose,
    #[error("window `durationMins` must be greater than 0")]
    InvalidDuration,
    #[error("cron expression {0:?} never fires (no valid occurrence — e.g. Feb 30 / Apr 31)")]
    NeverFires(String),
}

/// The schedule model driving admission of `Ready` work (DESIGN §12).
// Boxed: `CronExpr`/`WindowSched` are much larger than the unit `Immediate` variant
// (clippy::large_enum_variant).
#[derive(Debug)]
pub enum Schedule {
    /// Replicate as files become ready (default) — every tick releases whatever is `Ready`.
    Immediate,
    /// Point trigger: release all `Ready` work at each cron fire.
    Cron(Box<CronExpr>),
    /// Continuous flow gated to an `open`→`close` window.
    Window(Box<WindowSched>),
}

/// What a scheduling evaluation at a given instant admits.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Admission {
    /// Release ready work. `drain: true` means keep claiming until the backlog is empty (a cron
    /// fire releases *all* ready work at once); `drain: false` is the immediate-mode single bounded
    /// batch per tick (unchanged P1 behavior).
    All { drain: bool },
    /// Gate closed — admit nothing (cron between fires; window closed). Discovery/enqueue still runs
    /// elsewhere; newly-ready items simply accumulate in the durable `Ready` backlog until admitted.
    None,
    /// A window is open. New work may be admitted, but a transfer still in flight when `close_at`
    /// arrives must obey `on_close` (DESIGN §12.4).
    Windowed {
        close_at: DateTime<Utc>,
        on_close: WindowClose,
    },
}

impl Schedule {
    /// Build a [`Schedule`] from the parsed config. `default_tz` is
    /// `component.global.defaults.timezone`, used when the schedule itself sets none; absent both,
    /// `"UTC"`.
    pub fn from_cfg(cfg: &ScheduleCfg, default_tz: Option<&str>) -> Result<Self, ScheduleError> {
        match cfg {
            ScheduleCfg::Immediate => Ok(Schedule::Immediate),
            ScheduleCfg::Cron(c) => Ok(Schedule::Cron(Box::new(cron_from_cfg(c, default_tz)?))),
            ScheduleCfg::Window(w) => Ok(Schedule::Window(Box::new(window_from_cfg(w, default_tz)?))),
        }
    }

    /// The pure admission decision at `now`. `last_fire` is the caller-held in-memory cron watermark
    /// (cron does not persist across restarts — the first call after (re)start establishes the
    /// baseline without treating it as a fire, so historical fires are never replayed; a fire that
    /// lands between two consecutive calls is detected and drains the backlog once).
    pub fn admission(&self, now: DateTime<Utc>, last_fire: &mut Option<DateTime<Utc>>) -> Admission {
        match self {
            Schedule::Immediate => Admission::All { drain: false },
            Schedule::Cron(expr) => {
                let baseline = last_fire.unwrap_or(now);
                let fired = expr.prev_fire(now).is_some_and(|f| f > baseline);
                *last_fire = Some(now);
                if fired {
                    Admission::All { drain: true }
                } else {
                    Admission::None
                }
            }
            Schedule::Window(w) => match w.window_state(now) {
                WindowState::Open { until } => Admission::Windowed {
                    close_at: until,
                    on_close: w.on_close,
                },
                WindowState::Closed { .. } => Admission::None,
            },
        }
    }

    /// The next instant `run()` should wake to re-evaluate the schedule (a cron fire, or a window
    /// open/close edge). `None` means there is nothing to wake for — rely on rescan/file-watch only
    /// (immediate mode).
    pub fn next_wake(&self, now: DateTime<Utc>) -> Option<DateTime<Utc>> {
        match self {
            Schedule::Immediate => None,
            Schedule::Cron(expr) => expr.next_fire(now),
            Schedule::Window(w) => Some(match w.window_state(now) {
                WindowState::Open { until } => until,
                WindowState::Closed { opens_at } => opens_at,
            }),
        }
    }
}

/// Resolve a schedule's effective timezone: its own `timezone` ▸ the global default ▸ `"UTC"`.
fn resolve_tz(explicit: Option<&str>, default_tz: Option<&str>) -> Result<Tz, ScheduleError> {
    let raw = explicit.or(default_tz).unwrap_or("UTC");
    Tz::from_str(raw).map_err(|_| ScheduleError::InvalidTimezone(raw.to_string()))
}

/// Parse a schedule string as raw cron first; on failure, try the English sugar and parse the
/// resulting cron string. Both failing yields the sugar's (more actionable) error.
fn parse_cron_or_sugar(expr: &str, tz: Tz) -> Result<CronExpr, ScheduleError> {
    if let Ok(c) = CronExpr::parse(expr, tz) {
        return Ok(c);
    }
    let translated = sugar::to_cron(expr)?;
    CronExpr::parse(&translated, tz)
}

/// Reject a cron that syntactically parses but has **no future occurrence** (e.g. `0 0 30 2 *` — Feb 30
/// never exists, `0 0 31 4 *` — Apr 31), so the bad instance is skipped at build time per FR-CFG-4
/// rather than silently spinning at runtime. A window whose `open` never fires would otherwise resolve
/// forever to `Closed { opens_at }` with `opens_at == now` (croner returns `None`, and
/// [`WindowSched::next_open_at_or_after`]'s fallback yields `now`), which drives
/// [`crate::instance::Instance::run`]'s scheduler-wake at its 1ms floor into a tick-storm; a never-firing
/// cron in `cron` mode is likewise a misconfiguration that would never release any work.
///
/// A genuinely-never cron returns `None` from croner's forward search from *any* reference instant
/// (its search horizon is exhausted); a valid periodic — including leap-day (`0 0 29 2 *`) — cron always
/// finds a next occurrence, so the real-clock reference is immaterial to the outcome.
fn ensure_fires(expr: &CronExpr) -> Result<(), ScheduleError> {
    if expr.next_fire(Utc::now()).is_none() {
        return Err(ScheduleError::NeverFires(expr.as_str().to_string()));
    }
    Ok(())
}

fn cron_from_cfg(cfg: &CronSchedule, default_tz: Option<&str>) -> Result<CronExpr, ScheduleError> {
    let tz = resolve_tz(cfg.timezone.as_deref(), default_tz)?;
    let expr = parse_cron_or_sugar(&cfg.expression, tz)?;
    ensure_fires(&expr)?;
    Ok(expr)
}

fn window_from_cfg(cfg: &WindowSchedule, default_tz: Option<&str>) -> Result<WindowSched, ScheduleError> {
    let tz = resolve_tz(cfg.timezone.as_deref(), default_tz)?;

    if cfg.close.is_some() && cfg.duration_mins.is_some() {
        return Err(ScheduleError::AmbiguousWindowClose);
    }

    if let Some(close_str) = &cfg.close {
        let open = parse_cron_or_sugar(&cfg.open, tz)?;
        ensure_fires(&open)?;
        let close = parse_cron_or_sugar(close_str, tz)?;
        let label = format!("{} -> {}", cfg.open, close_str);
        return Ok(WindowSched::new(open, Close::Cron(Box::new(close)), cfg.on_window_close, label));
    }

    if let Some(mins) = cfg.duration_mins {
        if mins == 0 {
            return Err(ScheduleError::InvalidDuration);
        }
        let open = parse_cron_or_sugar(&cfg.open, tz)?;
        ensure_fires(&open)?;
        let label = format!("{} for {mins}m", cfg.open);
        let dur = chrono::Duration::minutes(mins as i64);
        return Ok(WindowSched::new(open, Close::Duration(dur), cfg.on_window_close, label));
    }

    // Neither `close` nor `durationMins` set: the whole window may be a single English phrase on
    // `open` (e.g. `"between 22:00 and 06:00"` — DESIGN §12.2 window sugar).
    if let Some((open_cron, close_cron)) = sugar::parse_window_between(&cfg.open) {
        let open = CronExpr::parse(&open_cron, tz)?;
        ensure_fires(&open)?;
        let close = CronExpr::parse(&close_cron, tz)?;
        let label = cfg.open.clone();
        return Ok(WindowSched::new(open, Close::Cron(Box::new(close)), cfg.on_window_close, label));
    }

    Err(ScheduleError::AmbiguousWindowClose)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{CronSchedule, WindowClose, WindowSchedule};
    use chrono::TimeZone;

    fn utc(y: i32, mo: u32, d: u32, h: u32, mi: u32, s: u32) -> DateTime<Utc> {
        Utc.with_ymd_and_hms(y, mo, d, h, mi, s).single().unwrap()
    }

    // --- WallClock -------------------------------------------------------------------------------

    #[test]
    fn system_wall_clock_tracks_real_utc_now() {
        let before = Utc::now();
        let got = SystemWallClock.now_utc();
        let after = Utc::now();
        assert!(before <= got && got <= after);
    }

    #[test]
    fn manual_wall_clock_set_and_advance_round_trip() {
        let start = utc(2026, 1, 1, 0, 0, 0);
        let clock = ManualWallClock::new(start);
        assert_eq!(clock.now_utc(), start);

        let later = utc(2026, 6, 15, 12, 30, 0);
        clock.set(later);
        assert_eq!(clock.now_utc(), later);

        clock.advance(chrono::Duration::hours(1));
        assert_eq!(clock.now_utc(), later + chrono::Duration::hours(1));

        // Exercised through the trait object too, matching how the pattern is meant to be reused.
        let dyn_clock: &dyn WallClock = &clock;
        assert_eq!(dyn_clock.now_utc(), later + chrono::Duration::hours(1));
    }

    // --- from_cfg ------------------------------------------------------------------------------

    #[test]
    fn immediate_from_cfg() {
        assert!(matches!(
            Schedule::from_cfg(&ScheduleCfg::Immediate, None).unwrap(),
            Schedule::Immediate
        ));
    }

    #[test]
    fn cron_from_cfg_raw_expression() {
        let cfg = ScheduleCfg::Cron(CronSchedule {
            expression: "0 2 * * *".to_string(),
            timezone: None,
        });
        let sched = Schedule::from_cfg(&cfg, None).unwrap();
        match sched {
            Schedule::Cron(expr) => assert_eq!(expr.as_str(), "0 2 * * *"),
            other => panic!("expected Cron, got {other:?}"),
        }
    }

    #[test]
    fn cron_from_cfg_english_sugar() {
        let cfg = ScheduleCfg::Cron(CronSchedule {
            expression: "every 15 minutes".to_string(),
            timezone: None,
        });
        let sched = Schedule::from_cfg(&cfg, None).unwrap();
        match sched {
            Schedule::Cron(expr) => assert_eq!(expr.as_str(), "*/15 * * * *"),
            other => panic!("expected Cron, got {other:?}"),
        }
    }

    #[test]
    fn cron_from_cfg_bad_expression_is_instance_skip_error() {
        let cfg = ScheduleCfg::Cron(CronSchedule {
            expression: "not a schedule at all".to_string(),
            timezone: None,
        });
        assert!(Schedule::from_cfg(&cfg, None).is_err());
    }

    #[test]
    fn cron_from_cfg_unknown_timezone_is_an_error() {
        let cfg = ScheduleCfg::Cron(CronSchedule {
            expression: "0 2 * * *".to_string(),
            timezone: Some("Not/AZone".to_string()),
        });
        assert!(matches!(
            Schedule::from_cfg(&cfg, None),
            Err(ScheduleError::InvalidTimezone(_))
        ));
    }

    #[test]
    fn cron_from_cfg_falls_back_to_global_default_timezone() {
        let cfg = ScheduleCfg::Cron(CronSchedule {
            expression: "0 9 * * *".to_string(),
            timezone: None,
        });
        let sched = Schedule::from_cfg(&cfg, Some("America/New_York")).unwrap();
        match sched {
            Schedule::Cron(expr) => assert_eq!(expr.timezone(), "America/New_York".parse().unwrap()),
            other => panic!("expected Cron, got {other:?}"),
        }
    }

    #[test]
    fn window_from_cfg_close_cron() {
        let cfg = ScheduleCfg::Window(WindowSchedule {
            open: "0 22 * * *".to_string(),
            close: Some("0 6 * * *".to_string()),
            duration_mins: None,
            timezone: None,
            on_window_close: WindowClose::FinishCurrent,
        });
        let sched = Schedule::from_cfg(&cfg, None).unwrap();
        match sched {
            Schedule::Window(w) => {
                assert_eq!(w.on_close, WindowClose::FinishCurrent);
                assert_eq!(
                    w.window_state(utc(2026, 1, 5, 23, 0, 0)),
                    WindowState::Open {
                        until: utc(2026, 1, 6, 6, 0, 0)
                    }
                );
            }
            other => panic!("expected Window, got {other:?}"),
        }
    }

    #[test]
    fn window_from_cfg_duration() {
        let cfg = ScheduleCfg::Window(WindowSchedule {
            open: "0 2 * * *".to_string(),
            close: None,
            duration_mins: Some(90),
            timezone: None,
            on_window_close: WindowClose::PauseResume,
        });
        let sched = Schedule::from_cfg(&cfg, None).unwrap();
        match sched {
            Schedule::Window(w) => assert_eq!(
                w.window_state(utc(2026, 1, 5, 2, 30, 0)),
                WindowState::Open {
                    until: utc(2026, 1, 5, 3, 30, 0)
                }
            ),
            other => panic!("expected Window, got {other:?}"),
        }
    }

    #[test]
    fn window_from_cfg_both_close_and_duration_is_ambiguous() {
        let cfg = ScheduleCfg::Window(WindowSchedule {
            open: "0 22 * * *".to_string(),
            close: Some("0 6 * * *".to_string()),
            duration_mins: Some(90),
            timezone: None,
            on_window_close: WindowClose::PauseResume,
        });
        assert!(matches!(
            Schedule::from_cfg(&cfg, None),
            Err(ScheduleError::AmbiguousWindowClose)
        ));
    }

    #[test]
    fn window_from_cfg_neither_close_nor_duration_is_ambiguous() {
        let cfg = ScheduleCfg::Window(WindowSchedule {
            open: "0 22 * * *".to_string(),
            close: None,
            duration_mins: None,
            timezone: None,
            on_window_close: WindowClose::PauseResume,
        });
        assert!(matches!(
            Schedule::from_cfg(&cfg, None),
            Err(ScheduleError::AmbiguousWindowClose)
        ));
    }

    #[test]
    fn window_open_that_never_fires_is_rejected_at_build() {
        // Regression: a syntactically-valid `open` cron with no real occurrence (Feb 30) must be a
        // skip-bad-instance error (FR-CFG-4), not a runtime `Closed { opens_at == now }` tick-storm.
        let cfg = ScheduleCfg::Window(WindowSchedule {
            open: "0 0 30 2 *".to_string(),
            close: Some("0 6 * * *".to_string()),
            duration_mins: None,
            timezone: None,
            on_window_close: WindowClose::PauseResume,
        });
        assert!(matches!(
            Schedule::from_cfg(&cfg, None),
            Err(ScheduleError::NeverFires(_))
        ));

        // Same guard on the duration-window and whole-phrase-sugar branches.
        let dur = ScheduleCfg::Window(WindowSchedule {
            open: "0 0 31 4 *".to_string(), // Apr 31 never exists
            close: None,
            duration_mins: Some(60),
            timezone: None,
            on_window_close: WindowClose::PauseResume,
        });
        assert!(matches!(
            Schedule::from_cfg(&dur, None),
            Err(ScheduleError::NeverFires(_))
        ));
    }

    #[test]
    fn cron_that_never_fires_is_rejected_at_build() {
        let cfg = ScheduleCfg::Cron(CronSchedule {
            expression: "0 0 30 2 *".to_string(),
            timezone: None,
        });
        assert!(matches!(
            Schedule::from_cfg(&cfg, None),
            Err(ScheduleError::NeverFires(_))
        ));
    }

    #[test]
    fn leap_day_cron_is_accepted() {
        // A leap-day schedule DOES fire (next Feb 29 within croner's horizon), so it must build.
        let cfg = ScheduleCfg::Cron(CronSchedule {
            expression: "0 0 29 2 *".to_string(),
            timezone: None,
        });
        assert!(matches!(Schedule::from_cfg(&cfg, None), Ok(Schedule::Cron(_))));
    }

    #[test]
    fn window_from_cfg_zero_duration_is_invalid() {
        let cfg = ScheduleCfg::Window(WindowSchedule {
            open: "0 2 * * *".to_string(),
            close: None,
            duration_mins: Some(0),
            timezone: None,
            on_window_close: WindowClose::PauseResume,
        });
        assert!(matches!(
            Schedule::from_cfg(&cfg, None),
            Err(ScheduleError::InvalidDuration)
        ));
    }

    #[test]
    fn window_from_cfg_whole_phrase_sugar() {
        let cfg = ScheduleCfg::Window(WindowSchedule {
            open: "between 10pm and 6am".to_string(),
            close: None,
            duration_mins: None,
            timezone: None,
            on_window_close: WindowClose::PauseResume,
        });
        let sched = Schedule::from_cfg(&cfg, None).unwrap();
        match sched {
            Schedule::Window(w) => assert_eq!(
                w.window_state(utc(2026, 1, 5, 23, 0, 0)),
                WindowState::Open {
                    until: utc(2026, 1, 6, 6, 0, 0)
                }
            ),
            other => panic!("expected Window, got {other:?}"),
        }
    }

    // --- admission -------------------------------------------------------------------------------

    #[test]
    fn immediate_always_admits_without_draining() {
        let sched = Schedule::Immediate;
        let mut last_fire = None;
        assert_eq!(
            sched.admission(utc(2026, 1, 1, 0, 0, 0), &mut last_fire),
            Admission::All { drain: false }
        );
    }

    #[test]
    fn cron_admits_only_on_a_fresh_fire_then_gates_closed() {
        let expr = CronExpr::parse("0 2 * * *", chrono_tz::UTC).unwrap();
        let sched = Schedule::Cron(Box::new(expr));
        let mut last_fire = None;

        // First call establishes the baseline; even if `now` happens to be past a fire, the very
        // first evaluation never replays history.
        assert_eq!(
            sched.admission(utc(2026, 1, 5, 1, 0, 0), &mut last_fire),
            Admission::None
        );
        // No fire yet between the baseline and this tick.
        assert_eq!(
            sched.admission(utc(2026, 1, 5, 1, 59, 0), &mut last_fire),
            Admission::None
        );
        // The 02:00 fire has now happened since the last tick -> drain.
        assert_eq!(
            sched.admission(utc(2026, 1, 5, 2, 0, 30), &mut last_fire),
            Admission::All { drain: true }
        );
        // Immediately after, no NEW fire has happened -> gated closed again.
        assert_eq!(
            sched.admission(utc(2026, 1, 5, 2, 0, 31), &mut last_fire),
            Admission::None
        );
        // Next day's fire.
        assert_eq!(
            sched.admission(utc(2026, 1, 6, 2, 5, 0), &mut last_fire),
            Admission::All { drain: true }
        );
    }

    #[test]
    fn window_admission_open_and_closed() {
        let w = WindowSched::new(
            CronExpr::parse("0 9 * * *", chrono_tz::UTC).unwrap(),
            Close::Cron(Box::new(CronExpr::parse("0 17 * * *", chrono_tz::UTC).unwrap())),
            WindowClose::FinishCurrent,
            "0 9 * * * -> 0 17 * * *".to_string(),
        );
        let sched = Schedule::Window(Box::new(w));
        let mut last_fire = None; // unused by the Window arm, but the signature is uniform.

        assert_eq!(
            sched.admission(utc(2026, 1, 5, 8, 0, 0), &mut last_fire),
            Admission::None
        );
        assert_eq!(
            sched.admission(utc(2026, 1, 5, 12, 0, 0), &mut last_fire),
            Admission::Windowed {
                close_at: utc(2026, 1, 5, 17, 0, 0),
                on_close: WindowClose::FinishCurrent,
            }
        );
    }

    // --- next_wake -------------------------------------------------------------------------------

    #[test]
    fn next_wake_immediate_is_none() {
        assert_eq!(Schedule::Immediate.next_wake(utc(2026, 1, 1, 0, 0, 0)), None);
    }

    #[test]
    fn next_wake_cron_is_the_next_fire() {
        let sched = Schedule::Cron(Box::new(CronExpr::parse("0 2 * * *", chrono_tz::UTC).unwrap()));
        assert_eq!(
            sched.next_wake(utc(2026, 1, 5, 1, 0, 0)),
            Some(utc(2026, 1, 5, 2, 0, 0))
        );
    }

    #[test]
    fn next_wake_window_is_the_nearer_of_open_or_close() {
        let w = WindowSched::new(
            CronExpr::parse("0 9 * * *", chrono_tz::UTC).unwrap(),
            Close::Cron(Box::new(CronExpr::parse("0 17 * * *", chrono_tz::UTC).unwrap())),
            WindowClose::FinishCurrent,
            "0 9 * * * -> 0 17 * * *".to_string(),
        );
        let sched = Schedule::Window(Box::new(w));
        // Closed -> wake at the next open.
        assert_eq!(
            sched.next_wake(utc(2026, 1, 5, 8, 0, 0)),
            Some(utc(2026, 1, 5, 9, 0, 0))
        );
        // Open -> wake at the close.
        assert_eq!(
            sched.next_wake(utc(2026, 1, 5, 12, 0, 0)),
            Some(utc(2026, 1, 5, 17, 0, 0))
        );
    }
}
