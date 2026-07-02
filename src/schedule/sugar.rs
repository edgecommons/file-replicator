//! English → cron sugar (DESIGN §12.2).
//!
//! Thin, secondary, and documented as convenience: cron is the primary/authoritative expression
//! (`schedule/cron.rs`). A phrase that fails to parse as raw cron is tried here; if it matches one of
//! the grammar forms below it is compiled to a standard cron string and handed back through
//! [`super::cron::CronExpr::parse`] — so every downstream consumer only ever deals in cron. An
//! unrecognized phrase is a config error (`ScheduleError::UnrecognizedPhrase`), which skips the bad
//! instance (FR-CFG-4) exactly like a malformed raw cron expression.
//!
//! Grammar (case-insensitive, whitespace-tolerant):
//!
//! | Phrase | Cron |
//! |---|---|
//! | `every N seconds` | `*/N * * * * *` |
//! | `every N minutes` | `*/N * * * *` |
//! | `every N hours` | `0 */N * * *` |
//! | `every N minutes on weekdays` | `*/N * * * 1-5` |
//! | `hourly` | `0 * * * *` |
//! | `daily` / `every day` | `0 0 * * *` |
//! | `daily at H[:MM][am\|pm]` / `every day at …` | `MM H * * *` |
//! | `every weekday at H:MM` | `MM H * * 1-5` |
//! | `every <weekday> at H:MM` | `MM H * * <0-6>` |
//! | `weekly on <weekday> at H:MM` | `MM H * * <0-6>` |
//!
//! Window-only sugar (a whole `open..close` span from one phrase — see
//! [`parse_window_between`]): `between HH:MM and HH:MM` (overnight spans, e.g. `between 10pm and
//! 6am`, work with no special-casing — `WindowSched` derives overnight-ness from the two crons, not
//! from the phrase).

use super::ScheduleError;

/// Compile an English schedule phrase to a standard cron string. Returns
/// [`ScheduleError::UnrecognizedPhrase`] if `phrase` matches none of the documented forms.
pub fn to_cron(phrase: &str) -> Result<String, ScheduleError> {
    let norm = normalize(phrase);
    let words: Vec<&str> = norm.split_whitespace().collect();

    let cron = match words.as_slice() {
        ["hourly"] => Some("0 * * * *".to_string()),
        ["daily"] | ["every", "day"] => Some("0 0 * * *".to_string()),

        ["every", n, "seconds"] => parse_count(n).map(|n| format!("*/{n} * * * * *")),
        ["every", n, "minutes"] => parse_count(n).map(|n| format!("*/{n} * * * *")),
        ["every", n, "hours"] => parse_count(n).map(|n| format!("0 */{n} * * *")),
        ["every", n, "minutes", "on", "weekdays"] => {
            parse_count(n).map(|n| format!("*/{n} * * * 1-5"))
        }

        ["daily", "at", rest @ ..] | ["every", "day", "at", rest @ ..] => {
            parse_time(&rest.join(" ")).map(|(h, m)| format!("{m} {h} * * *"))
        }
        ["every", "weekday", "at", rest @ ..] => {
            parse_time(&rest.join(" ")).map(|(h, m)| format!("{m} {h} * * 1-5"))
        }
        ["every", day, "at", rest @ ..] if weekday_num(day).is_some() => {
            let dow = weekday_num(day).expect("checked by guard");
            parse_time(&rest.join(" ")).map(|(h, m)| format!("{m} {h} * * {dow}"))
        }
        ["weekly", "on", day, "at", rest @ ..] if weekday_num(day).is_some() => {
            let dow = weekday_num(day).expect("checked by guard");
            parse_time(&rest.join(" ")).map(|(h, m)| format!("{m} {h} * * {dow}"))
        }

        _ => None,
    };

    cron.ok_or_else(|| ScheduleError::UnrecognizedPhrase(phrase.to_string()))
}

/// Window-only sugar: `between HH:MM and HH:MM` → `(open_cron, close_cron)`. Overnight spans (the
/// close time-of-day earlier than the open) need no special handling here — [`super::window::WindowSched`]
/// derives openness purely from the two crons' relative fire times, not from any "overnight" flag.
pub fn parse_window_between(phrase: &str) -> Option<(String, String)> {
    let norm = normalize(phrase);
    let rest = norm.strip_prefix("between ")?;
    let (open_s, close_s) = rest.split_once(" and ")?;
    let (oh, om) = parse_time(open_s.trim())?;
    let (ch, cm) = parse_time(close_s.trim())?;
    Some((format!("{om} {oh} * * *"), format!("{cm} {ch} * * *")))
}

/// Lowercase, collapse whitespace, trim.
fn normalize(phrase: &str) -> String {
    phrase.trim().to_ascii_lowercase().split_whitespace().collect::<Vec<_>>().join(" ")
}

/// A positive integer count (`N` in `every N …`); `0` is rejected (not a valid cron step).
fn parse_count(s: &str) -> Option<u32> {
    let n: u32 = s.parse().ok()?;
    if n == 0 {
        None
    } else {
        Some(n)
    }
}

/// Parse a time-of-day: `H`, `H:MM`, `HH:MM` (24h), or any of those with a trailing `am`/`pm`
/// (optionally space-separated, e.g. `"9am"`, `"9 am"`, `"9:30pm"`). Returns `(hour_0_23, minute)`.
fn parse_time(s: &str) -> Option<(u32, u32)> {
    let s = s.trim();
    if s.is_empty() {
        return None;
    }
    let (body, meridiem) = if let Some(b) = s.strip_suffix("am") {
        (b.trim(), Some(false))
    } else if let Some(b) = s.strip_suffix("pm") {
        (b.trim(), Some(true))
    } else {
        (s, None)
    };
    let (h_str, m_str) = body.split_once(':').unwrap_or((body, "0"));
    let mut hour: u32 = h_str.trim().parse().ok()?;
    let minute: u32 = m_str.trim().parse().ok()?;
    if minute > 59 {
        return None;
    }
    match meridiem {
        Some(is_pm) => {
            if !(1..=12).contains(&hour) {
                return None;
            }
            if is_pm && hour != 12 {
                hour += 12;
            } else if !is_pm && hour == 12 {
                hour = 0;
            }
        }
        None => {
            if hour > 23 {
                return None;
            }
        }
    }
    Some((hour, minute))
}

/// `sun`/`sunday` = 0 … `sat`/`saturday` = 6 (standard cron day-of-week numbering; croner also
/// accepts `7` for Sunday, but we always emit `0`).
fn weekday_num(s: &str) -> Option<u8> {
    match s {
        "sun" | "sunday" => Some(0),
        "mon" | "monday" => Some(1),
        "tue" | "tues" | "tuesday" => Some(2),
        "wed" | "wednesday" => Some(3),
        "thu" | "thur" | "thurs" | "thursday" => Some(4),
        "fri" | "friday" => Some(5),
        "sat" | "saturday" => Some(6),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schedule::cron::CronExpr;

    #[test]
    fn hourly_and_daily() {
        assert_eq!(to_cron("hourly").unwrap(), "0 * * * *");
        assert_eq!(to_cron("Hourly").unwrap(), "0 * * * *");
        assert_eq!(to_cron("daily").unwrap(), "0 0 * * *");
        assert_eq!(to_cron("every day").unwrap(), "0 0 * * *");
        assert_eq!(to_cron("  every   day  ").unwrap(), "0 0 * * *");
    }

    #[test]
    fn every_n_units() {
        assert_eq!(to_cron("every 30 seconds").unwrap(), "*/30 * * * * *");
        assert_eq!(to_cron("every 15 minutes").unwrap(), "*/15 * * * *");
        assert_eq!(to_cron("every 2 hours").unwrap(), "0 */2 * * *");
        assert_eq!(
            to_cron("every 15 minutes on weekdays").unwrap(),
            "*/15 * * * 1-5"
        );
    }

    #[test]
    fn every_n_zero_is_rejected() {
        assert!(to_cron("every 0 minutes").is_err());
    }

    #[test]
    fn daily_at_various_time_formats() {
        assert_eq!(to_cron("daily at 9am").unwrap(), "0 9 * * *");
        assert_eq!(to_cron("daily at 9 am").unwrap(), "0 9 * * *");
        assert_eq!(to_cron("every day at 9:30am").unwrap(), "30 9 * * *");
        assert_eq!(to_cron("daily at 21:00").unwrap(), "0 21 * * *");
        assert_eq!(to_cron("daily at 2pm").unwrap(), "0 14 * * *");
        assert_eq!(to_cron("daily at 12am").unwrap(), "0 0 * * *");
        assert_eq!(to_cron("daily at 12pm").unwrap(), "0 12 * * *");
    }

    #[test]
    fn weekday_and_weekly_forms() {
        assert_eq!(to_cron("every weekday at 8:00").unwrap(), "0 8 * * 1-5");
        assert_eq!(to_cron("every monday at 7:15").unwrap(), "15 7 * * 1");
        assert_eq!(to_cron("weekly on friday at 17:30").unwrap(), "30 17 * * 5");
        assert_eq!(to_cron("every sun at 0:00").unwrap(), "0 0 * * 0");
        assert_eq!(to_cron("every sat at 23:59").unwrap(), "59 23 * * 6");
    }

    #[test]
    fn unrecognized_phrase_is_an_error() {
        let err = to_cron("whenever the mood strikes").unwrap_err();
        assert!(matches!(err, ScheduleError::UnrecognizedPhrase(p) if p == "whenever the mood strikes"));
        assert!(to_cron("").is_err());
        assert!(to_cron("daily at 25:00").is_err(), "invalid hour");
        assert!(to_cron("daily at 9:99").is_err(), "invalid minute");
        assert!(to_cron("daily at 13pm").is_err(), "invalid 12h hour");
    }

    #[test]
    fn every_generated_cron_string_is_itself_valid() {
        for phrase in [
            "hourly",
            "daily",
            "every day",
            "every 30 seconds",
            "every 15 minutes",
            "every 2 hours",
            "every 15 minutes on weekdays",
            "daily at 9:30am",
            "every weekday at 8:00",
            "every monday at 7:15",
            "weekly on friday at 17:30",
        ] {
            let cron = to_cron(phrase).unwrap();
            CronExpr::parse(&cron, chrono_tz::UTC)
                .unwrap_or_else(|e| panic!("{phrase:?} -> {cron:?} did not parse as cron: {e}"));
        }
    }

    #[test]
    fn window_between_simple() {
        let (open, close) = parse_window_between("between 9am and 5pm").unwrap();
        assert_eq!(open, "0 9 * * *");
        assert_eq!(close, "0 17 * * *");
    }

    #[test]
    fn window_between_overnight_needs_no_special_case() {
        let (open, close) = parse_window_between("between 10pm and 6am").unwrap();
        assert_eq!(open, "0 22 * * *");
        assert_eq!(close, "0 6 * * *");
    }

    #[test]
    fn window_between_with_minutes() {
        let (open, close) = parse_window_between("between 9:15am and 5:45pm").unwrap();
        assert_eq!(open, "15 9 * * *");
        assert_eq!(close, "45 17 * * *");
    }

    #[test]
    fn window_between_rejects_other_phrases() {
        assert!(parse_window_between("daily at 9am").is_none());
        assert!(parse_window_between("between 9am").is_none());
    }
}
