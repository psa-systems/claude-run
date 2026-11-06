//! SPEC-3.14: step timing. An attempt is stored as a sequence of timed
//! `attempt_steps` rows (`migrations/{sqlite,postgres}/00{30,11}_attempt_steps.sql`)
//! instead of being summarized by one `job_outcomes.duration_ms`.
//!
//! [`StepRecorder`] times the runner-owned steps directly; [`derive_claude_steps`]
//! and [`classify_bash_command`] derive the `coding` / `check` / `build` steps
//! from a `claude` invocation's stream log after the attempt (SPEC-3.14 item 4).
//! Every gap the recorder never saw a step for is written back as its own
//! `kind = "other", label = "unaccounted"` row (SPEC-3.14 item 6), so
//! `SUM(ended_at - started_at)` over an attempt's rows always equals its wall
//! time; the remainder is never a separate, driftable column.

use chrono::{DateTime, SecondsFormat, Utc};
use tracing::error;

use crate::db::DbPool;
use crate::error::Result;

/// SPEC-3.14 item 2: the closed set of step kinds.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StepKind {
    Clone,
    HookInstall,
    PostgresStart,
    PostgresHealthy,
    Coding,
    Check,
    Build,
    Checkpoint,
    Push,
    PrCreate,
    Review,
    CiWait,
    ConflictResolution,
    Gate,
    Other,
}

impl StepKind {
    pub fn as_str(self) -> &'static str {
        match self {
            StepKind::Clone => "clone",
            StepKind::HookInstall => "hook_install",
            StepKind::PostgresStart => "postgres_start",
            StepKind::PostgresHealthy => "postgres_healthy",
            StepKind::Coding => "coding",
            StepKind::Check => "check",
            StepKind::Build => "build",
            StepKind::Checkpoint => "checkpoint",
            StepKind::Push => "push",
            StepKind::PrCreate => "pr_create",
            StepKind::Review => "review",
            StepKind::CiWait => "ci_wait",
            StepKind::ConflictResolution => "conflict_resolution",
            StepKind::Gate => "gate",
            StepKind::Other => "other",
        }
    }
}

/// SPEC-3.14 item 1: the stage an attempt belongs to, from its 1-based attempt
/// number. `ci-remediation`, `conflict-resolution`, `review` and `final` are
/// set by their own paths, never derived from a count.
pub fn stage_for_attempt(attempt: u32) -> &'static str {
    match attempt {
        0 | 1 => "initial",
        2 => "retry-2",
        _ => "retry-3",
    }
}

/// SPEC-3.14 item 7: the closed set of attempt outcomes stored in
/// `job_outcomes.outcome`, each a row of the SPEC-3.12 table folded to what a
/// funnel needs. `Queued` is the only non-terminal value: the issue goes back
/// for another attempt.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Outcome {
    /// A non-tracker job (scripted action, comment or triage run) succeeded.
    Success,
    /// The PR is open for review (SPEC-3.12 "Code Review").
    CodeReview,
    /// The `.claude-run-no-code-change` marker.
    NoCodeChange,
    /// Terminal, a person must act: the `Needs Review` rows (marker, timeout,
    /// review at the cap, delivery blocked).
    NeedsReview,
    /// Not terminal: re-queued for another attempt.
    Queued,
    /// Terminal hard failure.
    Failed,
}

impl Outcome {
    pub fn as_str(self) -> &'static str {
        match self {
            Outcome::Success => "success",
            Outcome::CodeReview => "code_review",
            Outcome::NoCodeChange => "no_code_change",
            Outcome::NeedsReview => "needs_review",
            Outcome::Queued => "queued",
            Outcome::Failed => "failed",
        }
    }

    /// The outcome of a job that has no tracker state machine.
    pub fn from_ok(ok: bool) -> Self {
        if ok {
            Outcome::Success
        } else {
            Outcome::Failed
        }
    }
}

/// One derived step from [`derive_claude_steps`]: kind, label, start, end.
pub type ClaudeStep = (StepKind, &'static str, DateTime<Utc>, DateTime<Utc>);

fn fmt_ts(ts: DateTime<Utc>) -> String {
    ts.to_rfc3339_opts(SecondsFormat::Millis, true)
}

/// Records one attempt's steps as they happen, then fills the remainder.
/// SPEC-3.14.
pub struct StepRecorder<'a> {
    pool: &'a DbPool,
    run_id: i64,
    action_name: String,
    job_key: String,
    attempt: i64,
    seq: i64,
    spans: Vec<(DateTime<Utc>, DateTime<Utc>)>,
    /// Set once any step write fails. SPEC-3.14 item 8.
    incomplete: bool,
}

impl<'a> StepRecorder<'a> {
    pub fn new(
        pool: &'a DbPool,
        run_id: i64,
        action_name: &str,
        job_key: &str,
        attempt: u32,
    ) -> Self {
        Self {
            pool,
            run_id,
            action_name: action_name.to_string(),
            job_key: job_key.to_string(),
            attempt: attempt as i64,
            seq: 0,
            spans: Vec::new(),
            incomplete: false,
        }
    }

    /// Records one step. A write failure is logged at `error` with the issue
    /// key and the step, and marks the attempt "step data incomplete"
    /// (SPEC-3.14 item 8) rather than aborting the attempt.
    #[allow(clippy::too_many_arguments)]
    pub async fn record(
        &mut self,
        stage: &str,
        kind: StepKind,
        label: &str,
        started: DateTime<Utc>,
        ended: DateTime<Utc>,
        ok: bool,
        detail: Option<&str>,
    ) {
        if ended <= started {
            return;
        }
        self.seq += 1;
        self.spans.push((started, ended));
        if let Err(e) = insert_step(
            self.pool,
            self.run_id,
            &self.action_name,
            &self.job_key,
            self.attempt,
            self.seq,
            stage,
            kind.as_str(),
            label,
            &fmt_ts(started),
            &fmt_ts(ended),
            ok,
            detail,
        )
        .await
        {
            error!(
                action = %self.action_name,
                key = %self.job_key,
                attempt = self.attempt,
                step = kind.as_str(),
                error = %e,
                "writing attempt_steps row failed; attempt marked step data incomplete"
            );
            self.incomplete = true;
        }
    }

    /// SPEC-3.14 item 6: fills every gap between recorded spans (and before
    /// the first / after the last) with its own `other`/`unaccounted` row, so
    /// the attempt's steps plus this remainder always equal its wall time.
    /// Call once, after the last `record`. Returns whether any step write
    /// failed (SPEC-3.14 item 8), so the caller can mark the attempt "step
    /// data incomplete".
    pub async fn finish(
        &mut self,
        stage: &str,
        attempt_start: DateTime<Utc>,
        attempt_end: DateTime<Utc>,
    ) -> bool {
        let mut spans = std::mem::take(&mut self.spans);
        spans.sort_by_key(|s| s.0);
        let mut cursor = attempt_start;
        let mut gaps = Vec::new();
        for (start, end) in &spans {
            let start = (*start).clamp(attempt_start, attempt_end);
            let end = (*end).clamp(attempt_start, attempt_end);
            if start > cursor {
                gaps.push((cursor, start));
            }
            if end > cursor {
                cursor = end;
            }
        }
        if attempt_end > cursor {
            gaps.push((cursor, attempt_end));
        }
        for (start, end) in gaps {
            self.record(
                stage,
                StepKind::Other,
                "unaccounted",
                start,
                end,
                true,
                None,
            )
            .await;
        }
        self.incomplete
    }
}

#[allow(clippy::too_many_arguments)]
async fn insert_step(
    pool: &DbPool,
    run_id: i64,
    action_name: &str,
    job_key: &str,
    attempt: i64,
    seq: i64,
    stage: &str,
    kind: &str,
    label: &str,
    started_at: &str,
    ended_at: &str,
    ok: bool,
    detail: Option<&str>,
) -> Result<()> {
    sqlx::query(sqlx::AssertSqlSafe(
        crate::db::q(
            pool,
            r#"
        INSERT INTO attempt_steps (
            run_id, action_name, job_key, attempt, seq, stage, kind, label,
            started_at, ended_at, ok, detail
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        "#,
        )
        .into_owned(),
    ))
    .bind(run_id)
    .bind(action_name)
    .bind(job_key)
    .bind(attempt)
    .bind(seq)
    .bind(stage)
    .bind(kind)
    .bind(label)
    .bind(started_at)
    .bind(ended_at)
    .bind(ok as i64)
    .bind(detail)
    .execute(pool)
    .await?;
    Ok(())
}

/// SPEC-3.14 item 4: classify a Bash tool call by its command text alone.
/// Word-exact matching (not substring) so `git checkout` never matches
/// `check` and `unchecked_foo.sh` never matches `check`. A repo's own
/// pre-commit / pre-push / check / verify recipe wins over a generic build or
/// ad-hoc test/lint command; anything unmatched is `other`, never silently
/// `coding` (SPEC-3.14 item 4: "A command that matches no rule is `other`").
pub fn classify_bash_command(cmd: &str) -> (StepKind, &'static str) {
    let lower = cmd.to_lowercase();
    let words: Vec<&str> = lower
        .split(|c: char| c.is_whitespace() || matches!(c, '&' | '|' | ';' | '(' | ')'))
        .filter(|w| !w.is_empty())
        .collect();
    let has = |w: &str| words.contains(&w);

    if has("pre-commit") {
        return (StepKind::Check, "pre-commit");
    }
    if has("pre-push") {
        return (StepKind::Check, "pre-push");
    }
    if has("verify") {
        return (StepKind::Check, "verify");
    }
    if has("check") {
        return (StepKind::Check, "check");
    }
    if has("build") {
        return (StepKind::Build, "build");
    }
    const AD_HOC: &[&str] = &[
        "test", "tests", "pytest", "jest", "vitest", "rspec", "lint", "clippy", "eslint", "flake8",
        "mypy", "tox",
    ];
    if words.iter().any(|w| AD_HOC.contains(w)) {
        return (StepKind::Check, "ad-hoc");
    }
    (StepKind::Other, "other")
}

/// SPEC-3.14 item 4: derive the `coding` / `check` / `build` / `other` steps
/// for one `claude.invoke` call from its stream log. The time between
/// recognized Bash tool calls is `coding`; a Bash call's own span runs from
/// the `ts` of the `tool_use` line to the `ts` of the next stream-log line
/// (the stamped arrival time of its result), clamped to
/// `[invoke_start, invoke_end]`. The returned steps cover the whole
/// `[invoke_start, invoke_end]` span with no gaps, so nothing here falls into
/// the recorder's `unaccounted` remainder.
pub fn derive_claude_steps(
    stream_log: &std::path::Path,
    invoke_start: DateTime<Utc>,
    invoke_end: DateTime<Utc>,
) -> std::io::Result<Vec<ClaudeStep>> {
    let raw = std::fs::read_to_string(stream_log)?;
    struct Event {
        ts: DateTime<Utc>,
        bash_cmd: Option<String>,
    }
    let mut events = Vec::new();
    for line in raw.lines() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        let Ok(v) = serde_json::from_str::<serde_json::Value>(line) else {
            continue;
        };
        let Some(ts_str) = v.get("ts").and_then(|t| t.as_str()) else {
            continue;
        };
        let Ok(ts) = DateTime::parse_from_rfc3339(ts_str) else {
            continue;
        };
        let ts = ts.with_timezone(&Utc);
        let mut bash_cmd = None;
        if v.get("type").and_then(|t| t.as_str()) == Some("assistant") {
            if let Some(blocks) = v
                .get("message")
                .and_then(|m| m.get("content"))
                .and_then(|c| c.as_array())
            {
                for b in blocks {
                    if b.get("type").and_then(|t| t.as_str()) == Some("tool_use")
                        && b.get("name").and_then(|n| n.as_str()) == Some("Bash")
                    {
                        if let Some(cmd) = b
                            .get("input")
                            .and_then(|i| i.get("command"))
                            .and_then(|c| c.as_str())
                        {
                            bash_cmd = Some(cmd.to_string());
                        }
                    }
                }
            }
        }
        events.push(Event { ts, bash_cmd });
    }

    let mut steps: Vec<ClaudeStep> = Vec::new();
    let mut cursor = invoke_start;
    for i in 0..events.len() {
        let Some(cmd) = events[i].bash_cmd.as_ref() else {
            continue;
        };
        let start = events[i].ts.clamp(invoke_start, invoke_end);
        if start > cursor {
            steps.push((StepKind::Coding, "coding", cursor, start));
        }
        let end = events
            .get(i + 1)
            .map(|n| n.ts)
            .unwrap_or(invoke_end)
            .clamp(start, invoke_end);
        let (kind, label) = classify_bash_command(cmd);
        if end > start {
            steps.push((kind, label, start, end));
        }
        cursor = end;
    }
    if invoke_end > cursor {
        steps.push((StepKind::Coding, "coding", cursor, invoke_end));
    }
    Ok(steps)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn classifies_recipe_names_as_check() {
        assert_eq!(
            classify_bash_command("just pre-commit"),
            (StepKind::Check, "pre-commit")
        );
        assert_eq!(
            classify_bash_command("just pre-push"),
            (StepKind::Check, "pre-push")
        );
        assert_eq!(
            classify_bash_command("just check"),
            (StepKind::Check, "check")
        );
        assert_eq!(
            classify_bash_command("npm run verify"),
            (StepKind::Check, "verify")
        );
    }

    #[test]
    fn classifies_build_commands() {
        assert_eq!(
            classify_bash_command("cargo build --release"),
            (StepKind::Build, "build")
        );
        assert_eq!(
            classify_bash_command("npm run build"),
            (StepKind::Build, "build")
        );
        assert_eq!(
            classify_bash_command("just build"),
            (StepKind::Build, "build")
        );
    }

    #[test]
    fn classifies_ad_hoc_test_and_lint_commands() {
        assert_eq!(
            classify_bash_command("cargo test --lib"),
            (StepKind::Check, "ad-hoc")
        );
        assert_eq!(
            classify_bash_command("pytest -k foo"),
            (StepKind::Check, "ad-hoc")
        );
        assert_eq!(
            classify_bash_command("cargo clippy --all-targets"),
            (StepKind::Check, "ad-hoc")
        );
    }

    #[test]
    fn unmatched_commands_are_other_never_coding() {
        assert_eq!(
            classify_bash_command("git checkout main"),
            (StepKind::Other, "other")
        );
        assert_eq!(classify_bash_command("ls -la"), (StepKind::Other, "other"));
        assert_eq!(
            classify_bash_command("echo unchecked"),
            (StepKind::Other, "other")
        );
    }

    /// A recorded-shaped stream log: an init event, two assistant turns each
    /// with a Bash `tool_use` (one `just pre-push`, one an unmatched `ls`),
    /// a user tool_result after each, then a terminal result, each line
    /// stamped with an increasing `ts` the way `crate::claude::stamp_line`
    /// stamps every real line. Exercises the classifier end to end against a
    /// log shaped like a real recording rather than one line in isolation.
    #[test]
    fn derives_steps_from_a_recorded_shaped_log() {
        let dir = tempfile::tempdir().unwrap();
        let log = dir.path().join("stream.jsonl");
        let base = DateTime::parse_from_rfc3339("2026-01-01T00:00:00Z")
            .unwrap()
            .with_timezone(&Utc);
        let line = |offset_secs: i64, json: &str| -> String {
            let ts = base + chrono::Duration::seconds(offset_secs);
            let mut v: serde_json::Value = serde_json::from_str(json).unwrap();
            v.as_object_mut()
                .unwrap()
                .insert("ts".to_string(), serde_json::Value::String(fmt_ts(ts)));
            serde_json::to_string(&v).unwrap()
        };
        let lines = [
            line(0, r#"{"type":"system","subtype":"init"}"#),
            line(
                5,
                r#"{"type":"assistant","message":{"content":[{"type":"tool_use","name":"Bash","input":{"command":"just pre-push"}}]}}"#,
            ),
            line(65, r#"{"type":"user","message":{"content":[]}}"#),
            line(
                70,
                r#"{"type":"assistant","message":{"content":[{"type":"tool_use","name":"Bash","input":{"command":"ls -la"}}]}}"#,
            ),
            line(71, r#"{"type":"user","message":{"content":[]}}"#),
            line(
                80,
                r#"{"type":"result","subtype":"success","is_error":false,"result":"done"}"#,
            ),
        ];
        std::fs::write(&log, lines.join("\n") + "\n").unwrap();

        let invoke_start = base;
        let invoke_end = base + chrono::Duration::seconds(90);
        let steps = derive_claude_steps(&log, invoke_start, invoke_end).unwrap();

        // coding(0..5), check/pre-push(5..65), coding(65..70), other(70..71), coding(71..90)
        assert_eq!(steps.len(), 5, "{steps:?}");
        assert_eq!(steps[0].0, StepKind::Coding);
        assert_eq!(
            steps[1],
            (
                StepKind::Check,
                "pre-push",
                base + chrono::Duration::seconds(5),
                base + chrono::Duration::seconds(65)
            )
        );
        assert_eq!(steps[2].0, StepKind::Coding);
        assert_eq!(steps[3].0, StepKind::Other);
        assert_eq!(steps[4].0, StepKind::Coding);

        let total: chrono::Duration = steps.iter().map(|s| s.3 - s.2).sum();
        assert_eq!(
            total,
            invoke_end - invoke_start,
            "steps cover the whole invocation span with no gaps"
        );
    }

    /// SPEC-3.14 item 6: an attempt's recorded steps plus the filled
    /// remainder always equal its wall time, on whichever backend
    /// `CLAUDE_RUN_TEST_DATABASE_URL` selects (SQLite when unset, Postgres
    /// when set; CI runs this module's tests both ways).
    #[tokio::test]
    async fn steps_plus_remainder_equal_wall_time() {
        let pool = crate::db::test_pool().await;
        sqlx::query(sqlx::AssertSqlSafe(crate::db::q(
            &pool,
            "INSERT INTO runs (action_name, started_at) VALUES ('test-action', '2026-01-01T00:00:00Z')",
        ).into_owned()))
        .execute(&pool)
        .await
        .unwrap();
        let run_id: i64 = sqlx::query_scalar(sqlx::AssertSqlSafe(
            crate::db::q(
                &pool,
                "SELECT id FROM runs WHERE action_name = 'test-action'",
            )
            .into_owned(),
        ))
        .fetch_one(&pool)
        .await
        .unwrap();

        let attempt_start = DateTime::parse_from_rfc3339("2026-01-01T00:00:00Z")
            .unwrap()
            .with_timezone(&Utc);
        let attempt_end = attempt_start + chrono::Duration::seconds(100);

        let mut recorder = StepRecorder::new(&pool, run_id, "test-action", "TEST-1", 1);
        recorder
            .record(
                "initial",
                StepKind::Clone,
                "clone",
                attempt_start,
                attempt_start + chrono::Duration::seconds(10),
                true,
                None,
            )
            .await;
        recorder
            .record(
                "initial",
                StepKind::Check,
                "pre-push",
                attempt_start + chrono::Duration::seconds(40),
                attempt_start + chrono::Duration::seconds(60),
                true,
                None,
            )
            .await;
        let incomplete = recorder.finish("initial", attempt_start, attempt_end).await;
        assert!(!incomplete);

        let rows: Vec<(String, String)> = sqlx::query_as(sqlx::AssertSqlSafe(crate::db::q(
            &pool,
            "SELECT started_at, ended_at FROM attempt_steps WHERE run_id = ? AND job_key = ? AND attempt = ?",
        ).into_owned()))
        .bind(run_id)
        .bind("TEST-1")
        .bind(1_i64)
        .fetch_all(&pool)
        .await
        .unwrap();
        assert!(
            rows.len() >= 3,
            "clone + check + at least one unaccounted gap: {rows:?}"
        );

        let total: chrono::Duration = rows
            .iter()
            .map(|(s, e)| {
                let s = DateTime::parse_from_rfc3339(s).unwrap().with_timezone(&Utc);
                let e = DateTime::parse_from_rfc3339(e).unwrap().with_timezone(&Utc);
                e - s
            })
            .sum();
        assert_eq!(total, attempt_end - attempt_start, "rows: {rows:?}");
    }
}
