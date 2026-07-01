//! Schedule parsing + next-fire computation.
//!
//! Action `schedule:` block supports three kinds:
//!
//! | kind | spec | example | semantics |
//! |------|------|---------|-----------|
//! | `interval` | human duration | `30m`, `2h`, `90s` | fire every N from the last run's `started_at` |
//! | `cron` | 5-field cron | `*/30 * * * *` | fire at the next instant matching the expression after the last run |
//! | `manual` | (omitted) | - | never auto-fires; only `claude-run trigger` |
//!
//! `parse_schedule` validates the action's stored fields; `next_fire` decides
//! whether an action is due now.

use std::str::FromStr;
use std::time::Duration;

use chrono::{DateTime, Utc};

use crate::action::{Schedule, ScheduleKind};
use crate::error::Result;

#[derive(Debug, Clone)]
pub enum ParsedSchedule {
    Interval(Duration),
    // Boxed because cron::Schedule is ~1KB while the other variants are tiny;
    // clippy::large_enum_variant otherwise.
    Cron(Box<cron::Schedule>),
    Manual,
}

pub fn parse_schedule(s: &Schedule) -> Result<ParsedSchedule> {
    match s.kind {
        ScheduleKind::Manual => Ok(ParsedSchedule::Manual),
        ScheduleKind::Interval => {
            let spec = s
                .spec
                .as_deref()
                .ok_or_else(|| anyhow::anyhow!("schedule.kind=interval requires schedule.spec"))?;
            let dur = humantime::parse_duration(spec)
                .map_err(|e| anyhow::anyhow!("interval spec `{spec}`: {e}"))?;
            Ok(ParsedSchedule::Interval(dur))
        }
        ScheduleKind::Cron => {
            let spec = s
                .spec
                .as_deref()
                .ok_or_else(|| anyhow::anyhow!("schedule.kind=cron requires schedule.spec"))?;
            // cron::Schedule wants a 6-field (seconds-precision) expression.
            // Accept the common 5-field form by prefixing "0 " (fire on the
            // 0th second of the matching minute).
            let normalized = if spec.split_whitespace().count() == 5 {
                format!("0 {spec}")
            } else {
                spec.to_string()
            };
            let sched = cron::Schedule::from_str(&normalized)
                .map_err(|e| anyhow::anyhow!("cron spec `{spec}`: {e}"))?;
            Ok(ParsedSchedule::Cron(Box::new(sched)))
        }
    }
}

/// Decide whether an action is due to fire now, given when it last started.
pub fn next_fire_due(
    schedule: &ParsedSchedule,
    last_run_started: Option<DateTime<Utc>>,
    now: DateTime<Utc>,
) -> Option<DateTime<Utc>> {
    match schedule {
        ParsedSchedule::Manual => None,
        ParsedSchedule::Interval(d) => {
            let next = match last_run_started {
                Some(prev) => prev + chrono::Duration::from_std(*d).unwrap_or_default(),
                None => now, // never run -> fire on the first tick
            };
            if next <= now {
                Some(next)
            } else {
                None
            }
        }
        ParsedSchedule::Cron(sched) => {
            let after = last_run_started.unwrap_or(now - chrono::Duration::seconds(1));
            sched.after(&after).next().filter(|t| *t <= now)
        }
    }
}

/// CLAUDE-156: the scheduler's per-tick fire decision, factored out so the
/// in-flight guard is unit-testable.
///
/// An action already running is never re-fired on subsequent ticks. During the
/// window between `dispatch` and `run_begin` writing the fresh `runs.started_at`
/// row (which for the native YouTrack handler is only after the gather phase),
/// `next_fire_due` alone keeps returning "due" every 5s tick because the DB
/// still reports the previous cycle's (already interval-old) `started_at` or
/// `None`. That produced a "scheduler firing" storm in the logs and a wasted
/// `dispatch` call every tick, each short-circuited by the in-flight guard. The
/// `in_flight` check here suppresses both. It applies uniformly to the native,
/// scripted, and native-routing-bypass paths, since all of them are dispatched
/// through the same in-flight set.
pub fn should_fire(
    schedule: &ParsedSchedule,
    last_run_started: Option<DateTime<Utc>>,
    now: DateTime<Utc>,
    in_flight: bool,
) -> bool {
    !in_flight && next_fire_due(schedule, last_run_started, now).is_some()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ts(s: &str) -> DateTime<Utc> {
        DateTime::parse_from_rfc3339(s).unwrap().with_timezone(&Utc)
    }

    #[test]
    fn interval_first_run_fires_immediately() {
        let s = ParsedSchedule::Interval(Duration::from_secs(60));
        let due = next_fire_due(&s, None, ts("2026-01-01T00:00:00Z"));
        assert!(due.is_some());
    }

    #[test]
    fn interval_waits_for_period_after_last_run() {
        let s = ParsedSchedule::Interval(Duration::from_secs(60));
        let last = ts("2026-01-01T00:00:00Z");
        // 30s after last run: not due
        assert!(next_fire_due(&s, Some(last), ts("2026-01-01T00:00:30Z")).is_none());
        // 60s after: due
        assert!(next_fire_due(&s, Some(last), ts("2026-01-01T00:01:00Z")).is_some());
    }

    #[test]
    fn manual_never_fires() {
        let s = ParsedSchedule::Manual;
        assert!(next_fire_due(&s, None, ts("2026-01-01T00:00:00Z")).is_none());
        assert!(next_fire_due(
            &s,
            Some(ts("2025-01-01T00:00:00Z")),
            ts("2030-01-01T00:00:00Z")
        )
        .is_none());
    }

    #[test]
    fn cron_5_field_form_accepted() {
        let s = Schedule {
            kind: ScheduleKind::Cron,
            spec: Some("*/30 * * * *".into()),
        };
        let p = parse_schedule(&s).unwrap();
        assert!(matches!(p, ParsedSchedule::Cron(_)));
    }

    #[test]
    fn human_duration_parses() {
        let s = Schedule {
            kind: ScheduleKind::Interval,
            spec: Some("30m".into()),
        };
        let p = parse_schedule(&s).unwrap();
        match p {
            ParsedSchedule::Interval(d) => assert_eq!(d.as_secs(), 1800),
            _ => panic!("wrong variant"),
        }
    }

    // CLAUDE-156: the reported case - `"5m"` means 5 minutes (300s), so the
    // parser is not the bug. Guards against a regression that would mis-parse
    // the minutes suffix.
    #[test]
    fn interval_5m_parses_to_300s() {
        let s = Schedule {
            kind: ScheduleKind::Interval,
            spec: Some("5m".into()),
        };
        match parse_schedule(&s).unwrap() {
            ParsedSchedule::Interval(d) => assert_eq!(d.as_secs(), 300),
            _ => panic!("wrong variant"),
        }
    }

    // CLAUDE-156: an in-flight action is never re-fired, even across the many
    // 5s ticks that a single run outlasts while its fresh `runs.started_at` row
    // does not yet exist (`last_run_started == None`). This is the firing-storm
    // regression guard: `next_fire_due` reports the action "due" on every tick,
    // but `should_fire` suppresses the fire while the run is in flight.
    #[test]
    fn in_flight_action_not_refired_across_ticks() {
        let s = ParsedSchedule::Interval(Duration::from_secs(300));
        let start = ts("2026-07-01T15:04:47Z");
        for i in 0..60 {
            let now = start + chrono::Duration::seconds(i * 5);
            // The DB has no fresh started_at yet, so next_fire_due says "due"...
            assert!(next_fire_due(&s, None, now).is_some());
            // ...but should_fire refuses to fire while the action is in flight.
            assert!(!should_fire(&s, None, now, true));
        }
    }

    // CLAUDE-156: once the run finishes (no longer in flight) and its
    // `started_at` is recorded, the 5m interval is honored: not due before 5m,
    // due at 5m. So the action fires at most once per interval.
    #[test]
    fn interval_5m_fires_at_most_once_per_period() {
        let s = ParsedSchedule::Interval(Duration::from_secs(300));
        let started = ts("2026-07-01T15:04:47Z");
        assert!(!should_fire(
            &s,
            Some(started),
            started + chrono::Duration::seconds(299),
            false
        ));
        assert!(should_fire(
            &s,
            Some(started),
            started + chrono::Duration::seconds(300),
            false
        ));
    }
}
