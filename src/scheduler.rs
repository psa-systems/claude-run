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

/// CLAUDE-162: the continuous-pickup cadence check. Replaces the CLAUDE-156
/// `should_fire` in-flight suppressor: overlapping runs are now allowed, so the
/// scheduler no longer refuses to fire while a prior run is in flight. Instead an
/// action is due for an overlapping pickup run when at least `interval`
/// (`regather_interval`) has elapsed since it last FIRED.
///
/// The cadence is tracked in-memory (the dispatcher's `last_fire` map), decoupled
/// from the `runs.started_at` row on purpose: a re-gather that finds only
/// already-in-flight work creates no run row (the admission check), so keying the
/// cadence off run rows would re-fire every 5s tick (the CLAUDE-156 firing storm).
/// A never-fired action (`None`) is due immediately.
pub fn regather_due(
    last_fire: Option<DateTime<Utc>>,
    now: DateTime<Utc>,
    interval: Duration,
) -> bool {
    match last_fire {
        None => true,
        Some(prev) => {
            now.signed_duration_since(prev)
                >= chrono::Duration::from_std(interval).unwrap_or_else(|_| chrono::Duration::zero())
        }
    }
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

    // CLAUDE-162: a never-fired action is due immediately for its first pickup
    // run; after firing, it is not due again until the regather interval elapses.
    #[test]
    fn regather_first_fire_is_immediate_then_paced() {
        let interval = Duration::from_secs(60);
        let now = ts("2026-07-01T15:04:47Z");
        // Never fired -> due now.
        assert!(regather_due(None, now, interval));
        // Just fired -> not due until 60s pass.
        assert!(!regather_due(Some(now), now, interval));
        assert!(!regather_due(
            Some(now),
            now + chrono::Duration::seconds(59),
            interval
        ));
        assert!(regather_due(
            Some(now),
            now + chrono::Duration::seconds(60),
            interval
        ));
    }

    // CLAUDE-162: the cadence is governed by the last FIRE time, not by run rows,
    // so an action that has been firing every interval keeps a bounded cadence and
    // does NOT re-fire on every 5s tick (the CLAUDE-156 firing-storm invariant,
    // now enforced via the in-memory last-fire stamp rather than an in-flight
    // guard). Across 12 5s ticks after a fire, it is due exactly once (at 60s).
    #[test]
    fn regather_does_not_fire_every_tick() {
        let interval = Duration::from_secs(60);
        let fired = ts("2026-07-01T15:04:47Z");
        let mut due_count = 0;
        for i in 1..=12 {
            let now = fired + chrono::Duration::seconds(i * 5);
            if regather_due(Some(fired), now, interval) {
                due_count += 1;
            }
        }
        // Only the tick at +60s is due; the eleven before it are suppressed.
        assert_eq!(due_count, 1);
    }
}
