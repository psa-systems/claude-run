//! SPEC-3.15: the step table on runner lifecycle comments.
//!
//! The table is rendered from the stored `attempt_steps` rows (SPEC-3.14), not
//! from comment text, so it survives a restart. Rows are folded into listed
//! milestones ([`milestones`]); the current comment's own row is marked
//! `<-- This comment`, earlier rows carry their times, and the rest of the
//! expected happy path is listed with no times. Steps are ASCII only.

use chrono::{DateTime, Utc};

use crate::db::DbPool;
use crate::error::{Error, Result};

/// SPEC-3.15 item 4: the happy path, in order.
pub const HAPPY_PATH: &[&str] = &[
    "Initial commit",
    "Checkpoint 1",
    "Checks 1",
    "Checkpoint 2",
    "Checks 2",
    "Verdict",
    "Final commit",
    "PR ready",
];

/// One stored `attempt_steps` row, as far as the list needs it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StepRow {
    pub attempt: i64,
    pub kind: String,
    /// The `attempt_steps.label` the row was stored under: a check/gate row's
    /// recipe or stage name, otherwise the same as `kind` for most rows.
    pub label: String,
    /// The SPEC-3.14 item 1 stage the row was stored under.
    pub stage: String,
    pub started_at: DateTime<Utc>,
    pub ended_at: DateTime<Utc>,
    pub ok: bool,
}

/// A listed step that has happened: its label, the check type(s) it folds
/// (SPEC-3.15 item 2, empty for a non-check step), its own duration and the
/// elapsed time at its end, both in whole seconds.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Milestone {
    pub label: String,
    pub check_type: Option<String>,
    pub step_secs: i64,
    pub total_secs: i64,
}

/// SPEC-3.15 item 1: `m:ss`, or `h:mm:ss` from one hour.
pub fn format_secs(secs: i64) -> String {
    let secs = secs.max(0);
    let (h, m, s) = (secs / 3600, (secs % 3600) / 60, secs % 60);
    if h > 0 {
        format!("{h}:{m:02}:{s:02}")
    } else {
        format!("{m}:{s:02}")
    }
}

/// SPEC-3.15 item 7: a step with no recorded time because its data is
/// incomplete.
pub const UNKNOWN_TIME: &str = "?:??";

fn is_check_kind(kind: &str) -> bool {
    kind == "check" || kind == "gate"
}

/// SPEC-3.15 item 2: the check type `label` names for a `check`/`gate` row, or
/// `None` for a per-stage sub-step row (`fmt`, `clippy`, `test`, `build`) that
/// folds into its parent's type instead of adding its own. A `gate` row's
/// label is always its gate command; a `check` row's label is a type only when
/// it is one of the repo-recipe or ad-hoc names `classify_bash_command` emits.
fn check_type<'a>(kind: &str, label: &'a str) -> Option<&'a str> {
    match kind {
        "gate" => Some(label),
        "check"
            if matches!(
                label,
                "pre-commit" | "pre-push" | "check" | "verify" | "ad-hoc"
            ) =>
        {
            Some(label)
        }
        _ => None,
    }
}

/// The label a stored kind lists under, or `None` for a kind that only adds
/// to the elapsed time (clone, coding, build, Postgres, ...). `n` counts the
/// earlier steps of the same family in this list.
fn listed_label(
    kind: &str,
    stage: &str,
    seen_milestones_in_attempt: usize,
    counts: &mut Counts,
) -> Option<String> {
    match kind {
        "checkpoint" => {
            counts.checkpoint += 1;
            Some(format!("Checkpoint {}", counts.checkpoint))
        }
        "push" if seen_milestones_in_attempt == 0 => Some("Initial commit".to_string()),
        "push" => Some("Final commit".to_string()),
        "review" => Some("Verdict".to_string()),
        "pr_create" => Some("PR ready".to_string()),
        "conflict_resolution" => Some("Conflict resolution".to_string()),
        // A `ci_wait` outside the `ci-remediation` stage is the finalize-time
        // status read (CLAUDE-549), not a remediation step: it only adds time.
        "ci_wait" if stage == "ci-remediation" => {
            counts.ci_remediation += 1;
            Some(format!("CI remediation {}", counts.ci_remediation))
        }
        _ => None,
    }
}

#[derive(Default)]
struct Counts {
    checkpoint: usize,
    checks: usize,
    ci_remediation: usize,
}

/// SPEC-3.15 items 1, 5 and 6: fold `rows` (all attempts of one job, ordered by
/// attempt then sequence) into the milestones that have happened. A later
/// attempt opens with its `Retry N` line, so a resumed run continues the
/// numbering of the branch's earlier attempts. Adjacent `check`/`gate` rows are
/// one `Checks N` step, `(failed)` when any of them failed; a review that did
/// not pass is `Review (no verdict)`.
pub fn milestones(rows: &[StepRow]) -> Vec<Milestone> {
    let Some(first) = rows.first() else {
        return Vec::new();
    };
    let origin = first.started_at;
    let mut out: Vec<Milestone> = Vec::new();
    let mut counts = Counts::default();
    let mut current_attempt = first.attempt;
    let mut in_attempt = 0usize;
    let mut open_checks: Option<(DateTime<Utc>, DateTime<Utc>, bool, Vec<String>)> = None;
    let secs = |a: DateTime<Utc>, b: DateTime<Utc>| (b - a).num_seconds().max(0);

    let flush_checks = |open: &mut Option<(DateTime<Utc>, DateTime<Utc>, bool, Vec<String>)>,
                        counts: &mut Counts,
                        in_attempt: &mut usize,
                        out: &mut Vec<Milestone>| {
        if let Some((start, end, ok, types)) = open.take() {
            counts.checks += 1;
            let label = if ok {
                format!("Checks {}", counts.checks)
            } else {
                format!("Checks {} (failed)", counts.checks)
            };
            *in_attempt += 1;
            out.push(Milestone {
                label,
                check_type: (!types.is_empty()).then(|| types.join(", ")),
                step_secs: secs(start, end),
                total_secs: secs(origin, end),
            });
        }
    };

    for row in rows {
        if row.attempt != current_attempt {
            flush_checks(&mut open_checks, &mut counts, &mut in_attempt, &mut out);
            current_attempt = row.attempt;
            in_attempt = 0;
            out.push(Milestone {
                label: format!("Retry {}", row.attempt),
                check_type: None,
                step_secs: 0,
                total_secs: secs(origin, row.started_at),
            });
        }
        if is_check_kind(&row.kind) {
            let entry =
                open_checks.get_or_insert_with(|| (row.started_at, row.ended_at, true, Vec::new()));
            entry.1 = row.ended_at;
            entry.2 = entry.2 && row.ok;
            if let Some(t) = check_type(&row.kind, &row.label) {
                if !entry.3.iter().any(|x| x == t) {
                    entry.3.push(t.to_string());
                }
            }
            continue;
        }
        flush_checks(&mut open_checks, &mut counts, &mut in_attempt, &mut out);
        let Some(mut label) = listed_label(&row.kind, &row.stage, in_attempt, &mut counts) else {
            // Unlisted time also breaks adjacency between two check groups.
            continue;
        };
        if row.kind == "review" && !row.ok {
            label = "Review (no verdict)".to_string();
        }
        in_attempt += 1;
        out.push(Milestone {
            label,
            check_type: None,
            step_secs: secs(row.started_at, row.ended_at),
            total_secs: secs(origin, row.ended_at),
        });
    }
    flush_checks(&mut open_checks, &mut counts, &mut in_attempt, &mut out);
    out
}

/// SPEC-3.15 item 2: what the `Type` column's check-type names mean.
const LEGEND: &str = "`pre-commit`, `pre-push`, `check` and `verify` are the repo's own recipes run by the agent, `ad-hoc` is any other test or lint command the agent chose to run, and a gate command is run by the runner.";

/// SPEC-3.15 items 1 to 4: render the step table. `done` are the earlier
/// steps, `current` is the row this comment reports (marked
/// `<-- This comment` in every time cell), and the rest of [`HAPPY_PATH`]
/// after `current` is listed with empty cells. When `current` is not a
/// happy-path label (`Failed`, `Needs Review`, `Retry 2`, ...) nothing follows
/// it, so a terminal row ends the table. `incomplete` renders every time cell
/// as `?:??` (SPEC-3.15 item 7, SPEC-3.14 item 8).
pub fn render(done: &[Milestone], current: &str, incomplete: bool) -> String {
    const MARK: &str = "<-- This comment";
    let mut lines: Vec<String> = vec![
        "| # | Step | Type | Step time | Since previous | Total |".to_string(),
        "| --- | --- | --- | --- | --- | --- |".to_string(),
    ];
    let mut prev_total = 0i64;
    for (i, m) in done.iter().enumerate() {
        let check_type = m.check_type.as_deref().unwrap_or("");
        let (step_time, since_previous, total) = if incomplete {
            (
                UNKNOWN_TIME.to_string(),
                UNKNOWN_TIME.to_string(),
                UNKNOWN_TIME.to_string(),
            )
        } else {
            (
                format_secs(m.step_secs),
                format_secs(m.total_secs - prev_total),
                format_secs(m.total_secs),
            )
        };
        prev_total = m.total_secs;
        lines.push(format!(
            "| {} | {} | {check_type} | {step_time} | {since_previous} | {total} |",
            i + 1,
            m.label
        ));
    }
    lines.push(format!(
        "| {} | {current} |  | {MARK} | {MARK} | {MARK} |",
        done.len() + 1
    ));
    if let Some(pos) = HAPPY_PATH.iter().position(|l| *l == current) {
        for (offset, label) in HAPPY_PATH[pos + 1..].iter().enumerate() {
            lines.push(format!(
                "| {} | {label} |  |  |  |  |",
                done.len() + 2 + offset
            ));
        }
    }
    lines.push(String::new());
    lines.push(LEGEND.to_string());
    lines.join("\n")
}

/// Convenience for a caller holding raw rows.
pub fn render_rows(rows: &[StepRow], current: &str, incomplete: bool) -> String {
    render(&milestones(rows), current, incomplete)
}

/// Load every stored step of one job across its attempts, oldest first. A row
/// whose timestamps do not parse is an error naming the row, never skipped.
pub async fn load_rows(pool: &DbPool, action: &str, key: &str) -> Result<Vec<StepRow>> {
    load_rows_where(pool, Some(action), key).await
}

/// [`load_rows`] for a caller that knows the job key but not which action
/// stored it (a webhook handler). `None` matches every action.
async fn load_rows_where(pool: &DbPool, action: Option<&str>, key: &str) -> Result<Vec<StepRow>> {
    // SPEC-3.14: step-end writes (a checkpoint, the clone) interleave with the
    // attempt-end rows, so chronological order is `started_at`, not `seq`.
    let sql = if action.is_some() {
        "SELECT attempt, kind, label, stage, started_at, ended_at, ok FROM attempt_steps \
         WHERE action_name = ? AND job_key = ? ORDER BY attempt, started_at, seq"
    } else {
        "SELECT attempt, kind, label, stage, started_at, ended_at, ok FROM attempt_steps \
         WHERE job_key = ? ORDER BY attempt, started_at, seq"
    };
    let mut query = sqlx::query_as::<_, (i64, String, String, String, String, String, i64)>(
        sqlx::AssertSqlSafe(crate::db::q(pool, sql).into_owned()),
    );
    if let Some(action) = action {
        query = query.bind(action);
    }
    let raw = query.bind(key).fetch_all(pool).await?;
    raw.into_iter()
        .map(|(attempt, kind, label, stage, started, ended, ok)| {
            let parse = |what: &str, v: &str| {
                DateTime::parse_from_rfc3339(v)
                    .map(|t| t.with_timezone(&Utc))
                    .map_err(|e| {
                        Error::Other(anyhow::anyhow!(
                            "attempt_steps row for {key} attempt {attempt}: bad {what} {v:?}: {e}"
                        ))
                    })
            };
            Ok(StepRow {
                attempt,
                kind,
                label,
                stage,
                started_at: parse("started_at", &started)?,
                ended_at: parse("ended_at", &ended)?,
                ok: ok != 0,
            })
        })
        .collect()
}

/// SPEC-3.15 item 6: the list for `current` rendered from the stored rows of
/// `key`. A failed read is logged at `error` with the key and renders an
/// all-unknown list (`?:??`), so the comment never shows a made-up time.
pub async fn render_for(pool: &DbPool, action: &str, key: &str, current: &str) -> String {
    match load_rows(pool, action, key).await {
        Ok(rows) => render_rows(&rows, current, false),
        Err(e) => {
            tracing::error!(action = %action, key = %key, error = %e, "reading attempt_steps for the step list failed; rendering unknown times");
            render(&[], current, true)
        }
    }
}

/// SPEC-3.15 item 6: the stored rows of `key` as a [`ListContext`]. A failed
/// read is logged at `error` with the key and marks the context incomplete, so
/// every time renders `?:??` instead of a made-up one. `action` is `None` for
/// a caller with no action name (a webhook handler).
pub async fn context_for(pool: &DbPool, action: Option<&str>, key: &str) -> ListContext {
    match load_rows_where(pool, action, key).await {
        Ok(rows) => ListContext {
            done: milestones(&rows),
            incomplete: false,
        },
        Err(e) => {
            tracing::error!(key = %key, error = %e, "reading attempt_steps for the push comment step list failed; rendering unknown times");
            ListContext {
                done: Vec::new(),
                incomplete: true,
            }
        }
    }
}

/// SPEC-3.15 + SPEC-8.8: the line a push comment of `event` reports, given the
/// steps already listed. A checkpoint push is the next `Checkpoint N`.
pub fn push_step_label(event: crate::push_comment::PushEvent, done: &[Milestone]) -> String {
    use crate::push_comment::PushEvent;
    match event {
        PushEvent::CheckpointPush => {
            let n = done
                .iter()
                .filter(|m| m.label.starts_with("Checkpoint "))
                .count();
            format!("Checkpoint {}", n + 1)
        }
        PushEvent::InitialPush
        | PushEvent::ResumedRunPush
        | PushEvent::MergedRemoteBranchBeforePush => "Final commit".to_string(),
        PushEvent::TimeoutSalvagePush => "Timed Out".to_string(),
        PushEvent::ConflictResolvePush => "Conflict resolution".to_string(),
        PushEvent::CommentWebhookFixPush => "Fix push".to_string(),
        PushEvent::BranchUpdatedFromBase => "Branch update".to_string(),
    }
}

/// SPEC-3.15 item 6 + SPEC-8.8: the list for a push comment of `event`,
/// rendered from the context stashed for `key` (see [`stash_context`]). No
/// stashed context is logged at `info` with the key and renders the current
/// line alone: a push outside any run has no earlier steps to list.
pub fn render_push(key: &str, event: crate::push_comment::PushEvent) -> String {
    let ctx = context_registry()
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .get(key)
        .cloned();
    let Some(ctx) = ctx else {
        tracing::info!(key = %key, event = event.label(), "no stored steps for this push comment; listing its own line only");
        return render(&[], &push_step_label(event, &[]), false);
    };
    render_push_with(&ctx, event)
}

/// [`render_push`] from a context the caller already holds.
pub fn render_push_with(ctx: &ListContext, event: crate::push_comment::PushEvent) -> String {
    render(
        &ctx.done,
        &push_step_label(event, &ctx.done),
        ctx.incomplete,
    )
}

/// The earlier steps a lifecycle comment for one job lists: stored rows of
/// earlier attempts plus the current attempt's steps derived so far.
#[derive(Debug, Clone, Default)]
pub struct ListContext {
    pub done: Vec<Milestone>,
    /// The stored rows could not be read; every time renders `?:??`.
    pub incomplete: bool,
}

type ContextRegistry = std::sync::Mutex<std::collections::HashMap<String, ListContext>>;

fn context_registry() -> &'static ContextRegistry {
    static R: std::sync::OnceLock<ContextRegistry> = std::sync::OnceLock::new();
    R.get_or_init(Default::default)
}

/// Hand the synchronous `finalize` (no pool, fixed signature) the steps its
/// comments list, keyed by job key. Cleared by [`clear_context`] when
/// `finalize` returns.
pub fn stash_context(key: &str, ctx: ListContext) {
    context_registry()
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .insert(key.to_string(), ctx);
}

/// Drop the stashed context for `key`.
pub fn clear_context(key: &str) {
    context_registry()
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .remove(key);
}

/// SPEC-3.15: the list for a comment whose own line is `current`, from the
/// context stashed for `key` (earlier steps only when none was stashed).
pub fn render_with_context(key: &str, current: &str) -> String {
    let ctx = context_registry()
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .get(key)
        .cloned()
        .unwrap_or_default();
    render(&ctx.done, current, ctx.incomplete)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn at(secs: i64) -> DateTime<Utc> {
        DateTime::parse_from_rfc3339("2026-01-01T00:00:00Z")
            .unwrap()
            .with_timezone(&Utc)
            + chrono::Duration::seconds(secs)
    }

    fn row(attempt: i64, kind: &str, start: i64, end: i64, ok: bool) -> StepRow {
        StepRow {
            attempt,
            kind: kind.to_string(),
            label: kind.to_string(),
            stage: "initial".to_string(),
            started_at: at(start),
            ended_at: at(end),
            ok,
        }
    }

    /// A `check`-kind row naming its recipe or ad-hoc type explicitly.
    fn check_row(attempt: i64, label: &str, start: i64, end: i64, ok: bool) -> StepRow {
        StepRow {
            label: label.to_string(),
            ..row(attempt, "check", start, end, ok)
        }
    }

    /// A `gate`-kind row naming its gate command.
    fn gate_row(attempt: i64, command: &str, start: i64, end: i64, ok: bool) -> StepRow {
        StepRow {
            label: command.to_string(),
            ..row(attempt, "gate", start, end, ok)
        }
    }

    /// The SPEC-3.15 item 4 happy-path example, as a table: headings, a check
    /// type on the folded `Checks N` row, a `Since previous` column that
    /// reconciles with `Total`, the current-comment marker and the
    /// not-yet-reached rows with empty cells.
    #[test]
    fn renders_the_spec_happy_path_example() {
        let rows = [
            row(1, "clone", 0, 1, true),
            row(1, "push", 1, 2, true),
            row(1, "checkpoint", 2, 88, true),
            row(1, "check", 88, 1040, true),
        ];
        let list = render_rows(&rows, "Checkpoint 2", false);
        assert_eq!(
            list,
            format!(
                "| # | Step | Type | Step time | Since previous | Total |\n\
                 | --- | --- | --- | --- | --- | --- |\n\
                 | 1 | Initial commit |  | 0:01 | 0:02 | 0:02 |\n\
                 | 2 | Checkpoint 1 |  | 1:26 | 1:26 | 1:28 |\n\
                 | 3 | Checks 1 | check | 15:52 | 15:52 | 17:20 |\n\
                 | 4 | Checkpoint 2 |  | <-- This comment | <-- This comment | <-- This comment |\n\
                 | 5 | Checks 2 |  |  |  |  |\n\
                 | 6 | Verdict |  |  |  |  |\n\
                 | 7 | Final commit |  |  |  |  |\n\
                 | 8 | PR ready |  |  |  |  |\n\n{LEGEND}"
            )
        );
    }

    /// SPEC-3.15 item 2: two distinct check types folded into one `Checks N`
    /// row join in order, separated by `, `.
    #[test]
    fn a_mixed_type_check_fold_joins_distinct_types_in_order() {
        let rows = [
            check_row(1, "pre-commit", 0, 10, true),
            check_row(1, "ad-hoc", 10, 20, true),
        ];
        let list = render_rows(&rows, "Checks 2", false);
        assert!(
            list.contains("| 1 | Checks 1 | pre-commit, ad-hoc | 0:20 | 0:20 | 0:20 |"),
            "{list}"
        );
    }

    /// SPEC-3.15 item 2: a `gate` row's type is the gate command itself.
    #[test]
    fn a_gate_rows_type_is_its_gate_command() {
        let rows = [gate_row(1, "just check-docker", 0, 15, true)];
        let list = render_rows(&rows, "Checks 2", false);
        assert!(
            list.contains("| 1 | Checks 1 | just check-docker | 0:15 | 0:15 | 0:15 |"),
            "{list}"
        );
    }

    /// SPEC-3.15 item 2: a recipe's own per-stage sub-step rows (`fmt`,
    /// `clippy`, ...) do not add their own type.
    #[test]
    fn a_per_stage_sub_step_does_not_add_its_own_type() {
        let rows = [
            check_row(1, "pre-push", 0, 5, true),
            check_row(1, "fmt", 5, 10, true),
            check_row(1, "clippy", 10, 20, true),
        ];
        let list = render_rows(&rows, "Checks 2", false);
        assert!(
            list.contains("| 1 | Checks 1 | pre-push | 0:20 | 0:20 | 0:20 |"),
            "{list}"
        );
    }

    /// SPEC-3.15 acceptance example: the gap between `Checkpoint 2` (total
    /// 7:16) and `Verdict` (total 12:08) shows as a 4:52 `Since previous`,
    /// where the old shape hid it between two step times that summed to far
    /// less than the elapsed time.
    #[test]
    fn since_previous_shows_the_unlisted_gap_between_checkpoint_2_and_review() {
        let rows = [
            row(1, "checkpoint", 0, 1, true),
            row(1, "checkpoint", 1, 436, true),
            row(1, "review", 708, 728, true),
        ];
        let list = render_rows(&rows, "PR ready", false);
        assert!(
            list.contains("| 2 | Checkpoint 2 |  | 7:15 | 7:15 | 7:16 |"),
            "{list}"
        );
        assert!(
            list.contains("| 3 | Verdict |  | 0:20 | 4:52 | 12:08 |"),
            "{list}"
        );
    }

    /// CLAUDE-603 / SPEC-3.15 item 6, SPEC-8.8: a push comment renders the
    /// stashed rows with its own line marked, and the next checkpoint is
    /// numbered after the stored ones.
    #[test]
    fn a_push_comment_lists_the_stashed_rows_and_marks_its_own_line() {
        let rows = [
            row(1, "clone", 0, 1, true),
            row(1, "checkpoint", 2, 88, true),
        ];
        stash_context(
            "PUSH-1",
            ListContext {
                done: milestones(&rows),
                incomplete: false,
            },
        );
        let list = render_push("PUSH-1", crate::push_comment::PushEvent::CheckpointPush);
        clear_context("PUSH-1");
        assert!(
            list.contains("| 1 | Checkpoint 1 |  | 1:26 | 1:26 | 1:28 |"),
            "{list}"
        );
        assert!(
            list.contains(
                "| 2 | Checkpoint 2 |  | <-- This comment | <-- This comment | <-- This comment |"
            ),
            "{list}"
        );
    }

    /// A push with no stashed steps still lists its own line, never a made-up time.
    #[test]
    fn a_push_comment_without_stored_steps_lists_its_own_line_only() {
        let list = render_push(
            "PUSH-NONE",
            crate::push_comment::PushEvent::BranchUpdatedFromBase,
        );
        assert_eq!(
            list,
            format!(
                "| # | Step | Type | Step time | Since previous | Total |\n\
                 | --- | --- | --- | --- | --- | --- |\n\
                 | 1 | Branch update |  | <-- This comment | <-- This comment | <-- This comment |\n\n{LEGEND}"
            )
        );
    }

    /// `context_for` reads the key's stored rows from the database.
    #[tokio::test]
    async fn context_for_reads_the_stored_rows_in_time_order() {
        let pool = crate::db::test_pool().await;
        let run_id: i64 = sqlx::query_scalar(sqlx::AssertSqlSafe(
            crate::db::q(
                &pool,
                "INSERT INTO runs (action_name, started_at) VALUES ('act', '2026-01-01T00:00:00Z') RETURNING id",
            )
            .into_owned(),
        ))
        .fetch_one(&pool)
        .await
        .unwrap();
        // seq order differs from time order: the checkpoint was written first.
        for (seq, kind, start, end) in [
            (
                1,
                "checkpoint",
                "2026-01-01T00:00:10.000Z",
                "2026-01-01T00:00:12.000Z",
            ),
            (
                2,
                "clone",
                "2026-01-01T00:00:00.000Z",
                "2026-01-01T00:00:03.000Z",
            ),
        ] {
            sqlx::query(sqlx::AssertSqlSafe(
                crate::db::q(
                    &pool,
                    "INSERT INTO attempt_steps (run_id, action_name, job_key, attempt, seq, stage, kind, label, started_at, ended_at, ok) VALUES (?, 'act', 'CTX-1', 1, ?, 'initial', ?, ?, ?, ?, 1)",
                )
                .into_owned(),
            ))
            .bind(run_id)
            .bind(seq as i64)
            .bind(kind)
            .bind(kind)
            .bind(start)
            .bind(end)
            .execute(&pool)
            .await
            .unwrap();
        }
        let ctx = context_for(&pool, Some("act"), "CTX-1").await;
        assert!(!ctx.incomplete);
        assert_eq!(ctx.done.len(), 1, "{:?}", ctx.done);
        assert_eq!(ctx.done[0].label, "Checkpoint 1");
        assert_eq!(
            ctx.done[0].total_secs, 12,
            "total runs from the clone start"
        );
        let any_action = context_for(&pool, None, "CTX-1").await;
        assert_eq!(any_action.done.len(), 1);
    }

    #[test]
    fn a_step_of_an_hour_or_more_uses_h_mm_ss() {
        assert_eq!(format_secs(59), "0:59");
        assert_eq!(format_secs(3599), "59:59");
        assert_eq!(format_secs(3600), "1:00:00");
        assert_eq!(format_secs(3725), "1:02:05");
        let rows = [row(1, "checkpoint", 0, 3725, true)];
        let list = render_rows(&rows, "Checks 1", false);
        assert!(
            list.contains("| 1 | Checkpoint 1 |  | 1:02:05 | 1:02:05 | 1:02:05 |"),
            "{list}"
        );
    }

    #[test]
    fn incomplete_data_renders_unknown_not_zero() {
        let rows = [row(1, "checkpoint", 0, 60, true)];
        let list = render_rows(&rows, "Checks 1", true);
        assert!(
            list.contains("| 1 | Checkpoint 1 |  | ?:?? | ?:?? | ?:?? |"),
            "{list}"
        );
        assert!(!list.contains("0:00"), "{list}");
    }

    /// A `(failed)` check and a `Retry N` row, with `Since previous`
    /// reconciling against `Total` across the retry boundary.
    #[test]
    fn a_resumed_run_continues_the_earlier_numbering() {
        let rows = [
            row(1, "checkpoint", 0, 60, true),
            row(1, "check", 60, 120, false),
            row(2, "checkpoint", 200, 260, true),
        ];
        let list = render_rows(&rows, "Failed", false);
        assert_eq!(
            list,
            format!(
                "| # | Step | Type | Step time | Since previous | Total |\n\
                 | --- | --- | --- | --- | --- | --- |\n\
                 | 1 | Checkpoint 1 |  | 1:00 | 1:00 | 1:00 |\n\
                 | 2 | Checks 1 (failed) | check | 1:00 | 1:00 | 2:00 |\n\
                 | 3 | Retry 2 |  | 0:00 | 1:20 | 3:20 |\n\
                 | 4 | Checkpoint 2 |  | 1:00 | 1:00 | 4:20 |\n\
                 | 5 | Failed |  | <-- This comment | <-- This comment | <-- This comment |\n\n{LEGEND}"
            )
        );
    }

    #[test]
    fn a_review_without_a_pass_is_listed_as_no_verdict() {
        let rows = [
            row(1, "check", 0, 10, true),
            row(1, "review", 10, 30, false),
        ];
        let list = render_rows(&rows, "Needs Review", false);
        assert!(
            list.contains("| 2 | Review (no verdict) |  | 0:20 | 0:20 | 0:30 |"),
            "{list}"
        );
        assert!(
            list.contains(
                "| 3 | Needs Review |  | <-- This comment | <-- This comment | <-- This comment |"
            ),
            "{list}"
        );
        assert!(list.ends_with(LEGEND), "{list}");
    }

    #[test]
    fn output_is_ascii_only() {
        let rows = [row(1, "checkpoint", 0, 60, true)];
        assert!(render_rows(&rows, "Checks 1", false).is_ascii());
    }

    #[test]
    fn a_stashed_context_supplies_the_earlier_steps() {
        let rows = [row(1, "check", 0, 90, true)];
        stash_context(
            "CTX-1",
            ListContext {
                done: milestones(&rows),
                incomplete: false,
            },
        );
        let list = render_with_context("CTX-1", "Failed");
        clear_context("CTX-1");
        assert_eq!(
            list,
            format!(
                "| # | Step | Type | Step time | Since previous | Total |\n\
                 | --- | --- | --- | --- | --- | --- |\n\
                 | 1 | Checks 1 | check | 1:30 | 1:30 | 1:30 |\n\
                 | 2 | Failed |  | <-- This comment | <-- This comment | <-- This comment |\n\n{LEGEND}"
            )
        );
        assert_eq!(
            render_with_context("CTX-1", "Failed"),
            format!(
                "| # | Step | Type | Step time | Since previous | Total |\n\
                 | --- | --- | --- | --- | --- | --- |\n\
                 | 1 | Failed |  | <-- This comment | <-- This comment | <-- This comment |\n\n{LEGEND}"
            ),
            "a cleared context lists only the current line"
        );
    }

    #[tokio::test]
    async fn the_list_is_rendered_from_stored_rows() {
        let pool = crate::db::test_pool().await;
        let run_id = crate::state::run_begin(&pool, "youtrack", "cli")
            .await
            .unwrap();
        let mut rec = crate::attempt_steps::StepRecorder::new(&pool, run_id, "youtrack", "T-9", 1);
        rec.record(
            "initial",
            crate::attempt_steps::StepKind::Check,
            "pre-push",
            at(0),
            at(90),
            true,
            None,
        )
        .await;
        let list = render_for(&pool, "youtrack", "T-9", "Verdict").await;
        assert!(
            list.contains("| 1 | Checks 1 | pre-push | 1:30 | 1:30 | 1:30 |"),
            "{list}"
        );
        assert!(
            list.contains(
                "| 2 | Verdict |  | <-- This comment | <-- This comment | <-- This comment |"
            ),
            "{list}"
        );
        assert!(list.contains("| 3 | Final commit |  |  |  |  |"), "{list}");
    }

    /// CLAUDE-597: a `ci_wait` row lists as `CI remediation N` only in the
    /// `ci-remediation` stage; the finalize-time status read adds time only.
    #[test]
    fn ci_wait_lists_only_in_the_ci_remediation_stage() {
        let mut finalize_read = row(1, "ci_wait", 0, 5, true);
        assert!(milestones(&[finalize_read.clone()]).is_empty());
        finalize_read.stage = "ci-remediation".to_string();
        let done = milestones(&[finalize_read]);
        assert_eq!(done.len(), 1);
        assert_eq!(done[0].label, "CI remediation 1");
    }
}
