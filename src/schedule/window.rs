//! Window state — `open` (cron) + `close` (cron) or `durationMins` (DESIGN §12.3).
//!
//! A window is the half-open span **`[open, close)`**: the close instant itself is already Closed
//! (no new admits exactly at the boundary — `onWindowClose` governs whatever is still in flight, not
//! new admission). `window_state(now)` finds the most recent `open` fire at-or-before `now`
//! ([`CronExpr::prev_fire`]) and the close paired with it; this makes overnight windows (open later in
//! the clock than close, e.g. `22:00`→`06:00`) fall out correctly with no special-casing, because
//! "most recent open" and "its paired close" are always evaluated relative to each other rather than
//! to the calendar day.

use chrono::{DateTime, Duration, Utc};

use super::cron::CronExpr;
use crate::config::WindowClose;

/// How a window's close is expressed.
// Boxed: `CronExpr` (wrapping `croner::Cron`) is much larger than `Duration` (clippy::large_enum_variant).
#[derive(Debug)]
pub enum Close {
    /// A second cron expression (independent cadence from `open`; DESIGN §12.3).
    Cron(Box<CronExpr>),
    /// A fixed length from the `open` fire (`durationMins` sugar for `close = open + duration`).
    Duration(Duration),
}

/// A parsed, ready-to-evaluate window schedule.
#[derive(Debug)]
pub struct WindowSched {
    open: CronExpr,
    close: Close,
    pub on_close: WindowClose,
    /// Human-readable label for events/logs (e.g. `"0 22 * * * -> 0 6 * * *"`).
    label: String,
}

/// The window's state at a given instant.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WindowState {
    /// Open until `until` (the close instant paired with the most recent open).
    Open { until: DateTime<Utc> },
    /// Closed; the next open is at `opens_at`.
    Closed { opens_at: DateTime<Utc> },
}

impl WindowSched {
    pub fn new(open: CronExpr, close: Close, on_close: WindowClose, label: String) -> Self {
        WindowSched {
            open,
            close,
            on_close,
            label,
        }
    }

    pub fn label(&self) -> &str {
        &self.label
    }

    /// Evaluate window state at `now` (DESIGN §12.3; overnight- and DST-correct — DST correctness
    /// comes from `CronExpr` computing fires in-tz via `chrono-tz`, and duration windows adding the
    /// fixed length in Utc so a window's *length* is never distorted by a DST shift inside it).
    pub fn window_state(&self, now: DateTime<Utc>) -> WindowState {
        match &self.close {
            Close::Cron(close) => self.state_for(now, |last_open| close.next_fire_inclusive(last_open)),
            Close::Duration(dur) => self.state_for(now, |last_open| Some(last_open + *dur)),
        }
    }

    /// Shared evaluation: find the most recent `open` at-or-before `now`; ask `close_after` for the
    /// close instant paired with that open; open iff `now` is strictly before that close.
    fn state_for(
        &self,
        now: DateTime<Utc>,
        close_after: impl Fn(DateTime<Utc>) -> Option<DateTime<Utc>>,
    ) -> WindowState {
        let last_open = self.open.prev_fire(now);
        let open_until = last_open.and_then(&close_after).filter(|&close| now < close);
        match open_until {
            Some(until) => WindowState::Open { until },
            None => WindowState::Closed {
                opens_at: self.next_open_at_or_after(now),
            },
        }
    }

    /// The next open at-or-after `now`. Safe to search inclusively here: if `now` itself were an open
    /// instant with a close still ahead, `state_for` would already have returned `Open` above, so
    /// reaching this path means either there's no open in effect, or degenerately `open == close`
    /// exactly at `now` (a zero-length window), which we treat as Closed.
    fn next_open_at_or_after(&self, now: DateTime<Utc>) -> DateTime<Utc> {
        self.open.next_fire_inclusive(now).unwrap_or(now)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::WindowClose;
    use chrono::TimeZone;

    fn utc(y: i32, mo: u32, d: u32, h: u32, mi: u32, s: u32) -> DateTime<Utc> {
        Utc.with_ymd_and_hms(y, mo, d, h, mi, s).single().unwrap()
    }

    fn cron(expr: &str) -> CronExpr {
        CronExpr::parse(expr, chrono_tz::UTC).unwrap()
    }

    fn day_window() -> WindowSched {
        WindowSched::new(
            cron("0 9 * * *"),
            Close::Cron(Box::new(cron("0 17 * * *"))),
            WindowClose::PauseResume,
            "0 9 * * * -> 0 17 * * *".to_string(),
        )
    }

    fn overnight_window() -> WindowSched {
        WindowSched::new(
            cron("0 22 * * *"),
            Close::Cron(Box::new(cron("0 6 * * *"))),
            WindowClose::PauseResume,
            "0 22 * * * -> 0 6 * * *".to_string(),
        )
    }

    fn duration_window(mins: i64) -> WindowSched {
        WindowSched::new(
            cron("0 2 * * *"),
            Close::Duration(Duration::minutes(mins)),
            WindowClose::FinishCurrent,
            "0 2 * * * for 90m".to_string(),
        )
    }

    #[test]
    fn day_window_closed_before_open() {
        let w = day_window();
        assert_eq!(
            w.window_state(utc(2026, 1, 5, 8, 59, 59)),
            WindowState::Closed {
                opens_at: utc(2026, 1, 5, 9, 0, 0)
            }
        );
    }

    #[test]
    fn day_window_open_at_open_instant() {
        let w = day_window();
        assert_eq!(
            w.window_state(utc(2026, 1, 5, 9, 0, 0)),
            WindowState::Open {
                until: utc(2026, 1, 5, 17, 0, 0)
            }
        );
    }

    #[test]
    fn day_window_open_mid_span() {
        let w = day_window();
        assert_eq!(
            w.window_state(utc(2026, 1, 5, 12, 0, 0)),
            WindowState::Open {
                until: utc(2026, 1, 5, 17, 0, 0)
            }
        );
    }

    #[test]
    fn day_window_closed_exactly_at_close() {
        let w = day_window();
        // Half-open [open, close): the close instant itself is already Closed.
        assert_eq!(
            w.window_state(utc(2026, 1, 5, 17, 0, 0)),
            WindowState::Closed {
                opens_at: utc(2026, 1, 6, 9, 0, 0)
            }
        );
    }

    #[test]
    fn overnight_window_open_late_evening() {
        let w = overnight_window();
        assert_eq!(
            w.window_state(utc(2026, 1, 5, 23, 0, 0)),
            WindowState::Open {
                until: utc(2026, 1, 6, 6, 0, 0)
            }
        );
    }

    #[test]
    fn overnight_window_open_after_midnight() {
        let w = overnight_window();
        assert_eq!(
            w.window_state(utc(2026, 1, 6, 2, 0, 0)),
            WindowState::Open {
                until: utc(2026, 1, 6, 6, 0, 0)
            }
        );
    }

    #[test]
    fn overnight_window_closed_midday() {
        let w = overnight_window();
        assert_eq!(
            w.window_state(utc(2026, 1, 5, 12, 0, 0)),
            WindowState::Closed {
                opens_at: utc(2026, 1, 5, 22, 0, 0)
            }
        );
    }

    #[test]
    fn overnight_window_open_exactly_at_open() {
        let w = overnight_window();
        assert_eq!(
            w.window_state(utc(2026, 1, 5, 22, 0, 0)),
            WindowState::Open {
                until: utc(2026, 1, 6, 6, 0, 0)
            }
        );
    }

    #[test]
    fn overnight_window_closed_exactly_at_close() {
        let w = overnight_window();
        assert_eq!(
            w.window_state(utc(2026, 1, 6, 6, 0, 0)),
            WindowState::Closed {
                opens_at: utc(2026, 1, 6, 22, 0, 0)
            }
        );
    }

    #[test]
    fn duration_window_open_within_span() {
        let w = duration_window(90);
        assert_eq!(
            w.window_state(utc(2026, 1, 5, 2, 30, 0)),
            WindowState::Open {
                until: utc(2026, 1, 5, 3, 30, 0)
            }
        );
    }

    #[test]
    fn duration_window_closed_after_span() {
        let w = duration_window(90);
        assert_eq!(
            w.window_state(utc(2026, 1, 5, 4, 0, 0)),
            WindowState::Closed {
                opens_at: utc(2026, 1, 6, 2, 0, 0)
            }
        );
    }

    #[test]
    fn duration_window_closed_before_open() {
        let w = duration_window(90);
        assert_eq!(
            w.window_state(utc(2026, 1, 5, 0, 0, 0)),
            WindowState::Closed {
                opens_at: utc(2026, 1, 5, 2, 0, 0)
            }
        );
    }

    #[test]
    fn label_accessor() {
        assert_eq!(day_window().label(), "0 9 * * * -> 0 17 * * *");
    }

    // --- DST: America/New_York, 2026 spring-forward (Mar 8, 02:00 -> 03:00) and fall-back
    // (Nov 1, 02:00 repeats) transitions. -----------------------------------------------------

    fn ny_window(open: &str, close: &str) -> WindowSched {
        let ny: chrono_tz::Tz = "America/New_York".parse().unwrap();
        WindowSched::new(
            CronExpr::parse(open, ny).unwrap(),
            Close::Cron(Box::new(CronExpr::parse(close, ny).unwrap())),
            WindowClose::PauseResume,
            format!("{open} -> {close}"),
        )
    }

    #[test]
    fn dst_spring_forward_window_stays_valid() {
        // 2026-03-08: America/New_York clocks skip 02:00 -> 03:00. A 01:00-04:00 local window must
        // still resolve to a valid, sane Open/Closed span across the gap (no panic, no nonsense
        // instant), proving the tz-aware cron search — not naive Utc arithmetic — governs the edges.
        let w = ny_window("0 1 * * *", "0 4 * * *");
        // 01:30 local = 06:30 UTC (still EST, UTC-5) -> inside the window.
        let mid_before_gap = utc(2026, 3, 8, 6, 30, 0);
        match w.window_state(mid_before_gap) {
            WindowState::Open { until } => {
                // 04:00 local on the transition day is 08:00 UTC (now EDT, UTC-4).
                assert_eq!(until, utc(2026, 3, 8, 8, 0, 0));
            }
            other => panic!("expected Open, got {other:?}"),
        }
        // 03:30 local (EDT, after the gap) = 07:30 UTC -> still inside the same window.
        let mid_after_gap = utc(2026, 3, 8, 7, 30, 0);
        match w.window_state(mid_after_gap) {
            WindowState::Open { until } => assert_eq!(until, utc(2026, 3, 8, 8, 0, 0)),
            other => panic!("expected Open, got {other:?}"),
        }
    }

    #[test]
    fn dst_spring_forward_open_inside_gap_snaps_forward() {
        // An `open` fire whose wall-clock instant (02:00) falls inside the skipped hour must resolve
        // to a real instant rather than erroring or resolving to a nonexistent time. Croner resolves
        // the fixed 02:00 open on the gap day to the last valid instant just before the skip
        // (01:59:59 EST = 06:59:59 UTC), so at the gap boundary the window is definitively **Open**,
        // paired with the 04:00 EDT close (= 08:00 UTC). Pin the concrete resolution (not a
        // two-outcome match) so a future croner behavior change here is caught, not silently absorbed —
        // mirroring the deterministic fall-back assertion below.
        let w = ny_window("0 2 * * *", "0 4 * * *");
        // Local 03:00 = 07:00 UTC, the first valid instant after the skip.
        let at_gap_end = utc(2026, 3, 8, 7, 0, 0);
        assert_eq!(
            w.window_state(at_gap_end),
            WindowState::Open {
                until: utc(2026, 3, 8, 8, 0, 0)
            }
        );
    }

    #[test]
    fn dst_fall_back_duration_window_not_double_counted() {
        // 2026-11-01: America/New_York clocks repeat 01:00 -> 02:00 (fall back, EDT -> EST). A
        // duration window opening at local 01:30 for 90 minutes must resolve to exactly one open
        // span (not two, and not a length distorted by the repeated hour) because the length is
        // added in Utc off the single resolved `open` instant.
        let ny: chrono_tz::Tz = "America/New_York".parse().unwrap();
        let w = WindowSched::new(
            CronExpr::parse("30 1 * * *", ny).unwrap(),
            Close::Duration(Duration::minutes(90)),
            WindowClose::PauseResume,
            "30 1 * * * for 90m".to_string(),
        );
        // The ambiguous local 01:30 occurs twice: first at 05:30 UTC (EDT, UTC-4), again at 06:30 UTC
        // (EST, UTC-5). Whichever instant croner resolves as the fire, `until` must be exactly 90
        // real minutes later, and the two neighbouring instants must not both independently open a
        // fresh window (the state is a function of the single most-recent `open` fire).
        let open_fire = CronExpr::parse("30 1 * * *", ny)
            .unwrap()
            .prev_fire(utc(2026, 11, 1, 7, 0, 0))
            .expect("resolves to some fire");
        // croner resolves an ambiguous fixed-time fire to its EARLIEST occurrence — the EDT (UTC-4)
        // instant, not the repeated EST one an hour later.
        assert_eq!(open_fire, utc(2026, 11, 1, 5, 30, 0));
        match w.window_state(open_fire + Duration::minutes(10)) {
            WindowState::Open { until } => assert_eq!(until, open_fire + Duration::minutes(90)),
            other => panic!("expected Open shortly after the fall-back open, got {other:?}"),
        }
        // Well past the 90-minute span (regardless of which of the two ambiguous instants was
        // chosen), the window must be Closed.
        assert_eq!(
            w.window_state(open_fire + Duration::minutes(200)),
            WindowState::Closed {
                opens_at: utc(2026, 11, 2, 6, 30, 0)
            }
        );
    }
}
