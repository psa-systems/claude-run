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

/// One derived step from [`derive_claude_steps`]. `detail` is `Some` when the
/// step carries a note for its `attempt_steps.detail` column, e.g.
/// [`SUBSTEPS_UNKNOWN`] on a check whose output printed no timed stage boundary.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClaudeStep {
    pub kind: StepKind,
    pub label: &'static str,
    pub start: DateTime<Utc>,
    pub end: DateTime<Utc>,
    pub detail: Option<&'static str>,
}

/// SPEC-3.14 item 5: the `detail` of a check row whose recipe printed no
/// observable, timed stage boundary, so Reports lists its sub-steps as unknown.
pub const SUBSTEPS_UNKNOWN: &str = "sub-steps unknown";

/// SPEC-3.14 item 5: classify one output line as a recipe stage banner. The
/// shared recipes print `[<recipe>] <command>` before each stage (for example
/// `[pre-commit] cargo clippy --all-targets`); the stage is named by the
/// cargo subcommand. `Some(None)` is a banner that is not a timed stage (such
/// as `[pre-commit] check-tree-ownership`), which still ends the previous
/// stage. `None` is not a banner.
pub fn classify_banner(line: &str) -> Option<Option<&'static str>> {
    let rest = line.trim_start().strip_prefix('[')?;
    let (_, command) = rest.split_once(']')?;
    let command = command.trim().to_lowercase();
    let has = |needle: &str| command.contains(needle);
    let stage = if has("cargo fmt") {
        Some("fmt")
    } else if has("cargo clippy") {
        Some("clippy")
    } else if has("cargo test") || has("cargo nextest") {
        Some("test")
    } else if has("cargo build") || has("cargo check") {
        Some("build")
    } else {
        None
    };
    Some(stage)
}

/// One timed output line: its arrival time when the output carries one, and
/// its text.
pub type TimedLine = (Option<DateTime<Utc>>, String);

/// Split a tool result's text into [`TimedLine`]s. A line that starts with an
/// RFC 3339 timestamp token (the forge log format, or a `ts`-prefixed recipe)
/// is timed; any other line is untimed.
pub fn timed_lines(text: &str) -> Vec<TimedLine> {
    text.lines()
        .map(|l| {
            let t = l.trim_start();
            match t.split_once(' ') {
                Some((first, rest)) => match DateTime::parse_from_rfc3339(first) {
                    Ok(ts) => (Some(ts.with_timezone(&Utc)), rest.to_string()),
                    Err(_) => (None, l.to_string()),
                },
                None => (None, l.to_string()),
            }
        })
        .collect()
}

/// A timed recipe stage: the stage label and its span.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StageSpan {
    pub label: &'static str,
    pub start: DateTime<Utc>,
    pub end: DateTime<Utc>,
}

/// SPEC-3.14 item 5: the timed stage spans in `lines`. A stage runs from its
/// banner to the next banner (stage or not), or to `end` for the last one.
/// Only a banner with an arrival time is a boundary: an untimed banner proves
/// the stage exists but not how long it took, so it yields no span.
pub fn stage_spans(lines: &[TimedLine], end: DateTime<Utc>) -> Vec<StageSpan> {
    let banners: Vec<(DateTime<Utc>, Option<&'static str>)> = lines
        .iter()
        .filter_map(|(ts, line)| Some(((*ts)?, classify_banner(line)?)))
        .collect();
    let mut out = Vec::new();
    for (i, (start, stage)) in banners.iter().enumerate() {
        let Some(label) = stage else { continue };
        let stop = banners.get(i + 1).map(|b| b.0).unwrap_or(end).min(end);
        if stop > *start {
            out.push(StageSpan {
                label,
                start: *start,
                end: stop,
            });
        }
    }
    out
}

/// SPEC-3.14 items 5 and 6: tile the check span `[start, end]` with its stage
/// spans, filling every gap (the prologue, a non-stage banner, the tail) with a
/// row labelled `parent`, so the rows still sum to the check's own duration.
/// With no stage spans the result is the single `parent` row, which the caller
/// flags [`SUBSTEPS_UNKNOWN`].
pub fn tile_check(
    parent: &'static str,
    start: DateTime<Utc>,
    end: DateTime<Utc>,
    stages: &[StageSpan],
) -> Vec<(&'static str, DateTime<Utc>, DateTime<Utc>)> {
    let mut rows = Vec::new();
    let mut cursor = start;
    for st in stages {
        let s = st.start.clamp(start, end);
        let e = st.end.clamp(start, end);
        if s > cursor {
            rows.push((parent, cursor, s));
        }
        if e > s.max(cursor) {
            rows.push((st.label, s.max(cursor), e));
            cursor = e;
        }
    }
    if end > cursor {
        rows.push((parent, cursor, end));
    }
    rows
}

/// A verification-gate run's timing (SPEC-3.3.2, SPEC-3.14), stashed by the
/// synchronous `finalize` for the async caller that owns the recorder.
#[derive(Debug, Clone)]
pub struct GateTiming {
    pub command: &'static str,
    pub start: DateTime<Utc>,
    pub end: DateTime<Utc>,
    pub ok: bool,
    pub stages: Vec<StageSpan>,
}

type GateRegistry = std::sync::Mutex<std::collections::HashMap<String, Vec<GateTiming>>>;

fn gate_registry() -> &'static GateRegistry {
    static R: std::sync::OnceLock<GateRegistry> = std::sync::OnceLock::new();
    R.get_or_init(Default::default)
}

/// Stash one gate run's timing under the job key. `finalize` is synchronous
/// with no recorder or pool (its signature and ~40 test call sites are fixed),
/// so the timing is handed to the async caller through this keyed registry
/// and drained by [`take_gate_timings`] once `finalize` returns.
pub fn stash_gate_timing(key: &str, timing: GateTiming) {
    gate_registry()
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .entry(key.to_string())
        .or_default()
        .push(timing);
}

/// Drain the gate timings stashed for `key`.
pub fn take_gate_timings(key: &str) -> Vec<GateTiming> {
    gate_registry()
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .remove(key)
        .unwrap_or_default()
}

impl<'a> StepRecorder<'a> {
    /// Record one verification-gate run (SPEC-3.14 items 3 and 5): the whole
    /// span as a `gate` row when no stage boundary was timed (flagged
    /// [`SUBSTEPS_UNKNOWN`]), else `check` rows per stage with `gate` rows for
    /// the gaps.
    pub async fn record_gate(&mut self, stage: &str, g: &GateTiming) {
        if g.stages.is_empty() {
            self.record(
                stage,
                StepKind::Gate,
                g.command,
                g.start,
                g.end,
                g.ok,
                Some(SUBSTEPS_UNKNOWN),
            )
            .await;
            return;
        }
        for (label, start, end) in tile_check(g.command, g.start, g.end, &g.stages) {
            let kind = if label == g.command {
                StepKind::Gate
            } else {
                StepKind::Check
            };
            self.record(stage, kind, label, start, end, g.ok, None)
                .await;
        }
    }
}

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

    /// Delete every row stored for this recorder's attempt number. Called when
    /// the attempt starts (rows left by an earlier attempt that never counted
    /// and so reuses this number) and when it ends uncounted, because step-end
    /// writes (CLAUDE-603) land before the attempt is known to count. A failure
    /// is logged at `error` with the key and marks the attempt incomplete.
    pub async fn clear_attempt(&mut self) {
        let res = sqlx::query(sqlx::AssertSqlSafe(
            crate::db::q(
                self.pool,
                "DELETE FROM attempt_steps WHERE action_name = ? AND job_key = ? AND attempt = ?",
            )
            .into_owned(),
        ))
        .bind(&self.action_name)
        .bind(&self.job_key)
        .bind(self.attempt)
        .execute(self.pool)
        .await;
        self.seq = 0;
        self.spans.clear();
        if let Err(e) = res {
            error!(
                action = %self.action_name,
                key = %self.job_key,
                attempt = self.attempt,
                error = %e,
                "clearing the attempt's stale attempt_steps rows failed; attempt marked step data incomplete"
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

/// SPEC-3.14 item 6: remove `holes` (steps recorded while `claude` ran, such as
/// a checkpoint push) from derived steps so no instant is counted twice. A
/// step a hole splits keeps both pieces; a step a hole covers is dropped.
pub fn carve_steps(
    steps: Vec<ClaudeStep>,
    holes: &[(DateTime<Utc>, DateTime<Utc>)],
) -> Vec<ClaudeStep> {
    let mut out = Vec::new();
    for step in steps {
        let mut pieces = vec![(step.start, step.end)];
        for (hs, he) in holes {
            pieces = pieces
                .into_iter()
                .flat_map(|(s, e)| {
                    if *he <= s || *hs >= e {
                        return vec![(s, e)];
                    }
                    let mut kept = Vec::new();
                    if *hs > s {
                        kept.push((s, *hs));
                    }
                    if *he < e {
                        kept.push((*he, e));
                    }
                    kept
                })
                .collect();
        }
        for (start, end) in pieces {
            out.push(ClaudeStep {
                start,
                end,
                ..step.clone()
            });
        }
    }
    out
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
        result_text: Option<String>,
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
        let mut result_text = None;
        if v.get("type").and_then(|t| t.as_str()) == Some("user") {
            if let Some(blocks) = v
                .get("message")
                .and_then(|m| m.get("content"))
                .and_then(|c| c.as_array())
            {
                let mut text = String::new();
                for b in blocks {
                    if b.get("type").and_then(|t| t.as_str()) != Some("tool_result") {
                        continue;
                    }
                    match b.get("content") {
                        Some(serde_json::Value::String(s)) => {
                            text.push_str(s);
                            text.push('\n');
                        }
                        Some(serde_json::Value::Array(parts)) => {
                            for p in parts {
                                if let Some(t) = p.get("text").and_then(|t| t.as_str()) {
                                    text.push_str(t);
                                    text.push('\n');
                                }
                            }
                        }
                        _ => {}
                    }
                }
                if !text.is_empty() {
                    result_text = Some(text);
                }
            }
        }
        events.push(Event {
            ts,
            bash_cmd,
            result_text,
        });
    }

    let mut steps: Vec<ClaudeStep> = Vec::new();
    let mut cursor = invoke_start;
    for i in 0..events.len() {
        let Some(cmd) = events[i].bash_cmd.as_ref() else {
            continue;
        };
        let start = events[i].ts.clamp(invoke_start, invoke_end);
        if start > cursor {
            steps.push(ClaudeStep {
                kind: StepKind::Coding,
                label: "coding",
                start: cursor,
                end: start,
                detail: None,
            });
        }
        let end = events
            .get(i + 1)
            .map(|n| n.ts)
            .unwrap_or(invoke_end)
            .clamp(start, invoke_end);
        let (kind, label) = classify_bash_command(cmd);
        if end > start {
            if kind == StepKind::Check {
                // SPEC-3.14 item 5: the result arrives on the next line; its
                // text names the recipe's stages and, when timed, their spans.
                let lines = events
                    .get(i + 1)
                    .and_then(|n| n.result_text.as_deref())
                    .map(timed_lines)
                    .unwrap_or_default();
                let spans = stage_spans(&lines, end);
                let unknown = spans.is_empty();
                for (row_label, s, e) in tile_check(label, start, end, &spans) {
                    steps.push(ClaudeStep {
                        kind,
                        label: row_label,
                        start: s,
                        end: e,
                        detail: unknown.then_some(SUBSTEPS_UNKNOWN),
                    });
                }
            } else {
                steps.push(ClaudeStep {
                    kind,
                    label,
                    start,
                    end,
                    detail: None,
                });
            }
        }
        cursor = end;
    }
    if invoke_end > cursor {
        steps.push(ClaudeStep {
            kind: StepKind::Coding,
            label: "coding",
            start: cursor,
            end: invoke_end,
            detail: None,
        });
    }
    Ok(steps)
}

#[cfg(test)]
mod tests {
    /// CLAUDE-603: a checkpoint span inside a derived step splits it, so no
    /// instant is counted by both rows.
    #[test]
    fn carve_steps_removes_hole_spans_from_derived_steps() {
        let at = |s: i64| {
            DateTime::parse_from_rfc3339("2026-01-01T00:00:00Z")
                .unwrap()
                .with_timezone(&Utc)
                + chrono::Duration::seconds(s)
        };
        let step = |kind, start, end| ClaudeStep {
            kind,
            label: "x",
            start: at(start),
            end: at(end),
            detail: None,
        };
        let out = carve_steps(
            vec![
                step(StepKind::Coding, 0, 100),
                step(StepKind::Check, 100, 120),
            ],
            &[(at(40), at(50)), (at(100), at(120))],
        );
        let spans: Vec<_> = out.iter().map(|s| (s.kind, s.start, s.end)).collect();
        assert_eq!(
            spans,
            vec![
                (StepKind::Coding, at(0), at(40)),
                (StepKind::Coding, at(50), at(100)),
            ]
        );
    }

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
        assert_eq!(steps[0].kind, StepKind::Coding);
        assert_eq!(
            steps[1],
            ClaudeStep {
                kind: StepKind::Check,
                label: "pre-push",
                start: base + chrono::Duration::seconds(5),
                end: base + chrono::Duration::seconds(65),
                detail: Some(SUBSTEPS_UNKNOWN),
            }
        );
        assert_eq!(steps[2].kind, StepKind::Coding);
        assert_eq!(steps[3].kind, StepKind::Other);
        assert_eq!(steps[4].kind, StepKind::Coding);

        let total: chrono::Duration = steps.iter().map(|s| s.end - s.start).sum();
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

    fn at(secs: i64) -> DateTime<Utc> {
        DateTime::parse_from_rfc3339("2026-01-01T00:00:00Z")
            .unwrap()
            .with_timezone(&Utc)
            + chrono::Duration::seconds(secs)
    }

    #[test]
    fn banners_name_the_recipe_stage() {
        assert_eq!(
            classify_banner("[pre-commit] cargo fmt --check"),
            Some(Some("fmt"))
        );
        assert_eq!(
            classify_banner("[pre-commit] cargo clippy --all-targets -- -D warnings"),
            Some(Some("clippy"))
        );
        assert_eq!(
            classify_banner("[pre-commit] cargo build --all-targets"),
            Some(Some("build"))
        );
        assert_eq!(
            classify_banner("[pre-commit] cargo test --lib"),
            Some(Some("test"))
        );
        // A banner that is not a timed stage still ends the previous one.
        assert_eq!(
            classify_banner("[pre-commit] check-tree-ownership"),
            Some(None)
        );
        assert_eq!(classify_banner("Compiling foo v0.1.0"), None);
    }

    /// A recorded-shaped gate output: each line stamped on arrival.
    #[test]
    fn stage_rows_tile_a_timed_check_with_no_gap() {
        let lines = timed_lines(
            "2026-01-01T00:00:02Z [pre-push] cargo fmt --check\n\
             2026-01-01T00:00:12Z [pre-push] cargo clippy --all-targets\n\
             2026-01-01T00:01:12Z [pre-push] cargo test\n\
             2026-01-01T00:03:12Z [pre-push] check-tree-ownership\n\
             2026-01-01T00:03:15Z all checks passed\n",
        );
        let spans = stage_spans(&lines, at(200));
        let labels: Vec<_> = spans.iter().map(|s| s.label).collect();
        assert_eq!(labels, ["fmt", "clippy", "test"]);
        let rows = tile_check("pre-push", at(0), at(200), &spans);
        assert_eq!(
            rows,
            vec![
                ("pre-push", at(0), at(2)),
                ("fmt", at(2), at(12)),
                ("clippy", at(12), at(72)),
                ("test", at(72), at(192)),
                ("pre-push", at(192), at(200)),
            ]
        );
        let total: chrono::Duration = rows.iter().map(|r| r.2 - r.1).sum();
        assert_eq!(
            total,
            at(200) - at(0),
            "sub-steps sum to the check's duration"
        );
    }

    #[test]
    fn untimed_banners_yield_no_stage_rows() {
        let lines = timed_lines("[pre-push] cargo fmt --check\n[pre-push] cargo test\n");
        assert!(stage_spans(&lines, at(100)).is_empty());
        assert_eq!(
            tile_check("pre-push", at(0), at(100), &[]),
            vec![("pre-push", at(0), at(100))],
            "no boundary: one row, never zero-length sub-steps"
        );
    }

    /// An agent-run check whose tool result carries timed stage banners.
    #[test]
    fn agent_check_log_yields_stage_rows_and_a_no_boundary_check_stays_one_flagged_row() {
        let dir = tempfile::tempdir().unwrap();
        let log = dir.path().join("stream.jsonl");
        let line = |offset: i64, json: serde_json::Value| -> String {
            let mut v = json;
            v.as_object_mut().unwrap().insert(
                "ts".to_string(),
                serde_json::Value::String(fmt_ts(at(offset))),
            );
            serde_json::to_string(&v).unwrap()
        };
        let bash = |cmd: &str| {
            serde_json::json!({"type":"assistant","message":{"content":[
                {"type":"tool_use","name":"Bash","input":{"command":cmd}}]}})
        };
        let result = |text: &str| {
            serde_json::json!({"type":"user","message":{"content":[
                {"type":"tool_result","content":text}]}})
        };
        let lines = [
            line(0, serde_json::json!({"type":"system","subtype":"init"})),
            line(10, bash("just pre-push")),
            line(
                100,
                result(
                    "2026-01-01T00:00:10Z [pre-push] cargo fmt --check\n\
                     2026-01-01T00:00:20Z [pre-push] cargo test\n",
                ),
            ),
            line(110, bash("just check")),
            line(150, result("everything passed")),
        ];
        std::fs::write(&log, lines.join("\n") + "\n").unwrap();
        let steps = derive_claude_steps(&log, at(0), at(160)).unwrap();
        let checks: Vec<_> = steps
            .iter()
            .filter(|s| s.kind == StepKind::Check)
            .map(|s| (s.label, (s.end - s.start).num_seconds(), s.detail))
            .collect();
        assert_eq!(
            checks,
            vec![
                ("fmt", 10, None),
                ("test", 80, None),
                ("check", 40, Some(SUBSTEPS_UNKNOWN)),
            ]
        );
    }

    #[tokio::test]
    async fn gate_with_stage_spans_records_check_rows_and_without_records_one_flagged_gate_row() {
        let pool = crate::db::test_pool().await;
        let run_id = crate::state::run_begin(&pool, "youtrack", "cli")
            .await
            .unwrap();
        let mut recorder = StepRecorder::new(&pool, run_id, "youtrack", "T-1", 1);
        recorder
            .record_gate(
                "initial",
                &GateTiming {
                    command: "pre-push",
                    start: at(0),
                    end: at(100),
                    ok: true,
                    stages: vec![StageSpan {
                        label: "test",
                        start: at(20),
                        end: at(90),
                    }],
                },
            )
            .await;
        recorder
            .record_gate(
                "initial",
                &GateTiming {
                    command: "check",
                    start: at(100),
                    end: at(130),
                    ok: true,
                    stages: vec![],
                },
            )
            .await;
        let rows: Vec<(String, String, Option<String>)> = sqlx::query_as(sqlx::AssertSqlSafe(
            crate::db::q(
                &pool,
                "SELECT kind, label, detail FROM attempt_steps WHERE run_id = ? ORDER BY seq",
            )
            .into_owned(),
        ))
        .bind(run_id)
        .fetch_all(&pool)
        .await
        .unwrap();
        assert_eq!(
            rows,
            vec![
                ("gate".into(), "pre-push".into(), None),
                ("check".into(), "test".into(), None),
                ("gate".into(), "pre-push".into(), None),
                ("gate".into(), "check".into(), Some(SUBSTEPS_UNKNOWN.into())),
            ]
        );
    }
}
