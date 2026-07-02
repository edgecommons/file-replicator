//! Parsed, tz/DST-aware cron expression (DESIGN §12.2).
//!
//! Wraps [`croner::Cron`], which is generic over [`chrono::TimeZone`] — every fire is computed **in
//! the configured [`chrono_tz::Tz`]** (so DST gaps/overlaps are resolved the way `chrono-tz` resolves
//! them for that zone) and mapped back to [`chrono::Utc`] for the engine, which only ever deals in
//! `Utc` instants (mirrors `Instance::tick`'s `now: i64` Unix-ms-in-Utc convention). Standard 5-field
//! cron (`min hour dom mon dow`) or 6-field with a leading seconds field are both accepted — croner's
//! default parser treats seconds as optional (5 tokens ⇒ seconds default to `0`; 6 ⇒ explicit).
//!
//! ## DST edge behavior (wall-clock semantics)
//! Because fires are evaluated against **local wall-clock time** (DESIGN §12.2), the two annual DST
//! transitions shift *interval* schedules by one repeated/skipped hour, once a year, in the configured
//! zone:
//! - **Spring-forward** (gap): a fixed-time job whose instant lands in the skipped hour snaps to a real
//!   instant at the gap edge (see [`super::window`]'s DST tests); an interval job simply has no fire in
//!   the skipped hour.
//! - **Fall-back** (repeat): the ambiguous hour's wall-clock slots are emitted **once** (the earlier,
//!   pre-transition offset). So `*/30 * * * *` in a fall-back zone has a ~90-minute real gap across the
//!   transition instead of 30 min, and `hourly` skips one repeated `01:00` — an "every N minutes/hours"
//!   replication schedule pauses for ~1 extra hour that one night. Admission never *double*-fires (the
//!   watermark advances monotonically); this is a spacing artifact, not a correctness bug. Use a
//!   **UTC** timezone for interval schedules that need strictly uniform spacing.

use std::str::FromStr;

use chrono::{DateTime, Utc};
use chrono_tz::Tz;
use croner::Cron;

use super::ScheduleError;

/// A cron expression bound to a timezone.
#[derive(Debug, Clone)]
pub struct CronExpr {
    cron: Cron,
    tz: Tz,
    src: String,
}

impl CronExpr {
    /// Parse a raw cron expression bound to `tz`. This does **not** attempt the English sugar
    /// fallback — see [`super::sugar::to_cron`] and [`super::parse_cron_or_sugar`] for that.
    pub fn parse(expr: &str, tz: Tz) -> Result<Self, ScheduleError> {
        let cron = Cron::from_str(expr).map_err(|source| ScheduleError::InvalidCron {
            expr: expr.to_string(),
            source,
        })?;
        Ok(CronExpr {
            cron,
            tz,
            src: expr.trim().to_string(),
        })
    }

    /// The original expression text (for labels/events).
    pub fn as_str(&self) -> &str {
        &self.src
    }

    /// The bound timezone.
    pub fn timezone(&self) -> Tz {
        self.tz
    }

    /// The next occurrence strictly after `after`.
    pub fn next_fire(&self, after: DateTime<Utc>) -> Option<DateTime<Utc>> {
        self.occurrence(after, false, true)
    }

    /// The next occurrence at-or-after `at` (`at` itself counts as a fire if it matches).
    pub fn next_fire_inclusive(&self, at: DateTime<Utc>) -> Option<DateTime<Utc>> {
        self.occurrence(at, true, true)
    }

    /// The most recent occurrence at-or-before `at` (`at` itself counts as a fire if it matches).
    pub fn prev_fire(&self, at_or_before: DateTime<Utc>) -> Option<DateTime<Utc>> {
        self.occurrence(at_or_before, true, false)
    }

    /// Shared search: convert to the bound tz, ask croner (forward or backward, inclusive or not),
    /// map the result back to Utc. `croner::CronError::TimeSearchLimitExceeded` (an unsatisfiable
    /// pattern) collapses to `None` — callers treat "never fires" the same as "not yet found".
    fn occurrence(&self, at: DateTime<Utc>, inclusive: bool, forward: bool) -> Option<DateTime<Utc>> {
        let local = at.with_timezone(&self.tz);
        let found = if forward {
            self.cron.find_next_occurrence(&local, inclusive)
        } else {
            self.cron.find_previous_occurrence(&local, inclusive)
        };
        found.ok().map(|dt| dt.with_timezone(&Utc))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    fn utc(y: i32, mo: u32, d: u32, h: u32, mi: u32, s: u32) -> DateTime<Utc> {
        Utc.with_ymd_and_hms(y, mo, d, h, mi, s).single().unwrap()
    }

    #[test]
    fn invalid_expression_is_an_error() {
        assert!(CronExpr::parse("not a cron", chrono_tz::UTC).is_err());
        assert!(CronExpr::parse("", chrono_tz::UTC).is_err());
    }

    #[test]
    fn next_fire_daily_utc() {
        let expr = CronExpr::parse("0 2 * * *", chrono_tz::UTC).unwrap();
        // Just before the fire today.
        assert_eq!(
            expr.next_fire(utc(2026, 1, 5, 1, 0, 0)),
            Some(utc(2026, 1, 5, 2, 0, 0))
        );
        // Exactly at the fire: `next_fire` is exclusive, so it rolls to tomorrow.
        assert_eq!(
            expr.next_fire(utc(2026, 1, 5, 2, 0, 0)),
            Some(utc(2026, 1, 6, 2, 0, 0))
        );
        // `next_fire_inclusive` at the exact instant returns the instant itself.
        assert_eq!(
            expr.next_fire_inclusive(utc(2026, 1, 5, 2, 0, 0)),
            Some(utc(2026, 1, 5, 2, 0, 0))
        );
    }

    #[test]
    fn prev_fire_is_inclusive_at_or_before() {
        let expr = CronExpr::parse("0 2 * * *", chrono_tz::UTC).unwrap();
        assert_eq!(
            expr.prev_fire(utc(2026, 1, 5, 2, 0, 0)),
            Some(utc(2026, 1, 5, 2, 0, 0)),
            "exactly at the fire counts"
        );
        assert_eq!(
            expr.prev_fire(utc(2026, 1, 5, 1, 59, 59)),
            Some(utc(2026, 1, 4, 2, 0, 0)),
            "just before rolls back to yesterday's fire"
        );
    }

    #[test]
    fn seconds_field_is_supported() {
        // 6-field: seconds min hour dom mon dow — fire every 10s.
        let expr = CronExpr::parse("*/10 * * * * *", chrono_tz::UTC).unwrap();
        assert_eq!(
            expr.next_fire(utc(2026, 1, 5, 0, 0, 3)),
            Some(utc(2026, 1, 5, 0, 0, 10))
        );
    }

    #[test]
    fn timezone_shifts_the_fire_relative_to_utc() {
        // "0 9 * * *" in America/New_York (UTC-5 in January, standard time) fires at 14:00 UTC.
        let ny: Tz = "America/New_York".parse().unwrap();
        let expr = CronExpr::parse("0 9 * * *", ny).unwrap();
        assert_eq!(
            expr.next_fire(utc(2026, 1, 5, 0, 0, 0)),
            Some(utc(2026, 1, 5, 14, 0, 0))
        );
    }

    #[test]
    fn as_str_and_timezone_accessors() {
        let expr = CronExpr::parse(" 0 2 * * * ", chrono_tz::UTC).unwrap();
        assert_eq!(expr.as_str(), "0 2 * * *");
        assert_eq!(expr.timezone(), chrono_tz::UTC);
    }
}
