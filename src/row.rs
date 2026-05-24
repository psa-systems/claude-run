//! Per-row pipeline. Mirrors `row-process` from `scripts/claude-run.nu`:
//!
//! ```text
//! filter -> dir-ensure workdir
//!         -> pre[]
//!         -> prompt -> claude  (if pre ok and prompt non-empty)
//!         -> post[]
//!         -> outcome record
//!         -> cleanup workdir
//! ```
//!
//! Key contracts preserved from the nu version:
//! - Post runs on every code path that reached the workdir, including pre /
//!   prompt failure. `$claude_result` is synthesized on those paths.
//! - Transient claude errors (rate limit) skip post AND skip attempt counting
//!   so the row retries cleanly on the next firing.
//! - Per-key attempt count is suppressed when post writes the
//!   `$workdir/.claude-run-skip-attempt` marker (blocker / parked rows).
//! - Daily Claude-invocation count + cost ledger advance only when claude
//!   actually ran (real budget was spent).

use std::path::Path;
use std::time::Duration;

use serde_json::Value;
use sqlx::SqlitePool;
use tracing::{info, warn};

use crate::action::LoadedAction;
use crate::claude::{ClaudeInvoker, ClaudeResult};
use crate::config::Paths;
use crate::error::{Error, Result};
use crate::snippet::{snippet_dir_for, Bindings, SnippetRunner};
use crate::state;
use crate::workdir;

#[derive(Debug, Clone)]
pub struct RunnerDefaults {
    pub max_attempts_per_key: u32,
    pub max_runs_per_day: u32,
    pub claude_timeout_seconds: u32,
    pub daily_budget_usd: f64,
}

impl Default for RunnerDefaults {
    fn default() -> Self {
        Self {
            max_attempts_per_key: 3,
            max_runs_per_day: 20,
            claude_timeout_seconds: 600,
            daily_budget_usd: 5.00,
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct EffectiveLimits {
    pub max_attempts_per_key: u32,
    pub max_runs_per_day: u32,
    pub claude_timeout: Duration,
    pub daily_budget_usd: f64,
}

pub fn effective_limits(defaults: &RunnerDefaults, action: &LoadedAction) -> EffectiveLimits {
    let l = &action.action.limits;
    EffectiveLimits {
        max_attempts_per_key: l
            .max_attempts_per_key
            .unwrap_or(defaults.max_attempts_per_key),
        max_runs_per_day: l.max_runs_per_day.unwrap_or(defaults.max_runs_per_day),
        claude_timeout: Duration::from_secs(
            l.claude_timeout_seconds
                .unwrap_or(defaults.claude_timeout_seconds) as u64,
        ),
        daily_budget_usd: l.daily_budget_usd.unwrap_or(defaults.daily_budget_usd),
    }
}

pub struct RowContext<'a> {
    pub action: &'a LoadedAction,
    pub limits: EffectiveLimits,
    pub paths: &'a Paths,
    pub snippet: &'a SnippetRunner,
    pub claude: &'a ClaudeInvoker,
    pub pool: &'a SqlitePool,
    pub run_id: i64,
}

/// Compute the row key for a row. If `dedup_key` is set on the action, look
/// it up; otherwise use a positional fallback.
pub fn row_key(action: &LoadedAction, row: &Value, idx: usize) -> String {
    let Some(field) = action.action.dedup_key.as_deref() else {
        return format!("row-{idx}");
    };
    match row.get(field) {
        Some(Value::String(s)) => s.clone(),
        Some(other) => other.to_string().trim_matches('"').to_string(),
        None => format!("row-{idx}"),
    }
}

pub async fn gather_rows(
    action: &LoadedAction,
    ctx_snippet: &SnippetRunner,
    paths: &Paths,
) -> Result<Vec<Value>> {
    let snippet_dir = paths
        .state_dir
        .join("snippets")
        .join(&action.action.name)
        .join("_input");
    let outcome = ctx_snippet.run(
        &action.action.input.command,
        &Bindings::new(),
        &snippet_dir,
        "input",
    )?;
    if !outcome.ok {
        return Err(Error::SnippetFailed {
            phase: "input.command".into(),
            exit_code: outcome.exit_code,
            stderr: outcome.stderr,
        });
    }
    let parsed: Value = serde_json::from_str(&outcome.stdout)
        .map_err(|e| Error::InputNotJsonArray(format!("{e}: stdout={}", outcome.stdout)))?;
    match parsed {
        Value::Array(arr) => Ok(arr),
        other => Err(Error::InputNotJsonArray(format!(
            "expected JSON array, got {}",
            other
        ))),
    }
}

/// Process a single row. Returns the per-row outcome record that the runner
/// will both write to the per-action JSONL and insert into `row_outcomes`.
pub async fn process_row(ctx: &RowContext<'_>, row: &Value, idx: usize) -> Result<ProcessedRow> {
    let action = &ctx.action.action;
    let key = row_key(ctx.action, row, idx);
    let attempts = state::attempts_get(ctx.pool, &action.name, &key).await?;

    if attempts >= ctx.limits.max_attempts_per_key && action.dedup_key.is_some() {
        info!(action = %action.name, key = %key, attempts, "skipping row, attempt cap reached");
        return Ok(ProcessedRow::CapHit { key });
    }

    info!(action = %action.name, key = %key, attempt = attempts + 1, "processing row");

    let wb_base = workdir::resolve_workdir_base(action.workdir_base.as_deref(), ctx.paths);
    let workdir_path = workdir::row_workdir(&wb_base, &action.name, &key);
    let snippet_dir = snippet_dir_for(&ctx.paths.state_dir, &action.name, &key);

    let mut bindings = Bindings::new();
    bindings.insert("row", row.clone());
    bindings.insert("workdir", Value::String(workdir_path.display().to_string()));

    // ---- filter ----
    if let Some(filter) = action.filter.as_deref() {
        let r = ctx.snippet.run(filter, &bindings, &snippet_dir, "filter")?;
        if !r.ok {
            info!(action = %action.name, key = %key, stderr = %r.stderr, "row filtered out");
            return Ok(ProcessedRow::Filtered { key });
        }
    }

    std::fs::create_dir_all(&workdir_path)?;

    // ---- pre steps ----
    let mut pre_failed: Option<(usize, i32, String)> = None;
    for (i, step) in action.pre.iter().enumerate() {
        let r = ctx
            .snippet
            .run(step, &bindings, &snippet_dir, &format!("pre-{i}"))?;
        if !r.ok {
            warn!(action = %action.name, key = %key, step = i, exit_code = r.exit_code, stderr = %r.stderr, "pre step failed (post still runs)");
            pre_failed = Some((i, r.exit_code, r.stderr));
            break;
        }
    }

    // ---- prompt + claude (skipped on pre failure; synthesize claude_result) ----
    let (claude_result, claude_ran) = if let Some((step_idx, exit_code, stderr)) = pre_failed {
        (
            ClaudeResult::synthesized(format!("pre step {step_idx} exited {exit_code}"), stderr),
            false,
        )
    } else {
        let prompt_r = ctx
            .snippet
            .run(&action.prompt, &bindings, &snippet_dir, "prompt")?;
        if !prompt_r.ok {
            warn!(action = %action.name, key = %key, exit_code = prompt_r.exit_code, "prompt snippet failed (post still runs)");
            (
                ClaudeResult::synthesized(
                    format!("prompt snippet exited {}", prompt_r.exit_code),
                    prompt_r.stderr,
                ),
                false,
            )
        } else {
            let prompt = prompt_r.stdout.trim().to_string();
            if prompt.is_empty() {
                warn!(action = %action.name, key = %key, "prompt snippet produced empty output (post still runs)");
                (
                    ClaudeResult::synthesized(
                        "empty prompt".to_string(),
                        "prompt snippet produced empty output",
                    ),
                    false,
                )
            } else {
                let stream_log = ClaudeInvoker::stream_log_path(
                    &ctx.paths.logs_dir(),
                    &action.name,
                    &key,
                    attempts + 1,
                );
                info!(action = %action.name, key = %key, stream_log = %stream_log.display(), "invoking claude");
                let r = ctx.claude.invoke(
                    &prompt,
                    &action.claude,
                    ctx.limits.claude_timeout,
                    &workdir_path,
                    &stream_log,
                )?;
                if !r.transient {
                    if r.ok {
                        info!(action = %action.name, key = %key, cost = r.cost, "claude completed");
                    } else {
                        warn!(action = %action.name, key = %key, error = %r.error, "claude run failed (post still runs)");
                    }
                }
                (r, true)
            }
        }
    };

    // Transient claude error: no post, no attempt counted, no daily count++.
    if claude_result.transient {
        info!(action = %action.name, key = %key, error = %claude_result.error, "transient claude error, not counting attempt");
        cleanup_workdir(&workdir_path);
        return Ok(ProcessedRow::Transient { key });
    }

    if claude_ran {
        state::daily_cost_add(ctx.pool, &action.name, claude_result.cost).await?;
    }

    // ---- post steps ----
    let mut post_bindings = bindings.clone();
    post_bindings.insert("claude_result", serde_json::to_value(&claude_result)?);

    let mut post_failed = false;
    for (i, step) in action.post.iter().enumerate() {
        let r = ctx
            .snippet
            .run(step, &post_bindings, &snippet_dir, &format!("post-{i}"))?;
        if !r.ok {
            warn!(action = %action.name, key = %key, step = i, exit_code = r.exit_code, stderr = %r.stderr, "post step failed");
            post_failed = true;
            break;
        }
    }

    // ---- skip-attempt marker (parked / blocker outcomes) ----
    let skip_marker = workdir_path.join(".claude-run-skip-attempt");
    let skip_attempt = skip_marker.exists();
    if skip_attempt {
        info!(action = %action.name, key = %key, "skip-attempt marker present, not counting attempt");
    }

    let ok = claude_result.ok && !post_failed;
    let stream_log_path = if claude_ran {
        Some(
            ClaudeInvoker::stream_log_path(&ctx.paths.logs_dir(), &action.name, &key, attempts + 1)
                .display()
                .to_string(),
        )
    } else {
        None
    };

    // ---- state updates ----
    if !skip_attempt {
        state::attempts_increment(ctx.pool, &action.name, &key).await?;
    }
    if claude_ran {
        state::daily_count_increment(ctx.pool, &action.name).await?;
    }

    cleanup_workdir(&workdir_path);

    Ok(ProcessedRow::Done {
        key,
        attempt: attempts + 1,
        ok,
        claude_ran,
        claude_ok: claude_result.ok,
        claude_cost: claude_result.cost,
        attempt_counted: !skip_attempt,
        summary: claude_result.summary,
        stream_log_path,
    })
}

fn cleanup_workdir(workdir: &Path) {
    if workdir.exists() {
        if let Err(e) = std::fs::remove_dir_all(workdir) {
            warn!(workdir = %workdir.display(), error = %e, "workdir cleanup failed (continuing)");
        }
    }
}

/// Outcome of `process_row`, kept simple enough to fold into both the JSONL
/// `output.file` and the `row_outcomes` DB table.
#[derive(Debug)]
pub enum ProcessedRow {
    /// Filter snippet rejected the row before any state-mutating work.
    Filtered { key: String },
    /// Attempt cap reached for this row; row was skipped without invocation.
    CapHit { key: String },
    /// Transient claude error - row will be retried next firing.
    Transient { key: String },
    /// Row reached at least pre steps; outcome record is meaningful.
    Done {
        key: String,
        attempt: u32,
        ok: bool,
        claude_ran: bool,
        claude_ok: bool,
        claude_cost: f64,
        attempt_counted: bool,
        summary: String,
        stream_log_path: Option<String>,
    },
}

impl ProcessedRow {
    pub fn counted_in_daily(&self) -> bool {
        matches!(
            self,
            ProcessedRow::Done {
                claude_ran: true,
                ..
            }
        )
    }
}

/// Append a row-outcome record to the action's per-action JSONL log (the
/// `output.file` field on the action). Best-effort; logs but does not fail.
pub fn append_output_record(paths: &Paths, action: &LoadedAction, record: &Value) {
    let Some(rel) = action.action.output.file.as_deref() else {
        return;
    };
    let dir = paths.logs_dir().join(&action.action.name);
    if let Err(e) = std::fs::create_dir_all(&dir) {
        warn!(error = %e, "output.file dir create failed");
        return;
    }
    let path = dir.join(rel);
    let mut line =
        serde_json::to_string(record).unwrap_or_else(|e| format!("{{\"error\":\"{e}\"}}"));
    line.push('\n');
    if let Err(e) = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&path)
        .and_then(|mut f| {
            use std::io::Write;
            f.write_all(line.as_bytes())
        })
    {
        warn!(path = %path.display(), error = %e, "output.file write failed");
    }
}

/// Convert a `ProcessedRow::Done` into the JSONL "done" record the nu
/// implementation has been writing.
pub fn done_record(d: &ProcessedRow) -> Option<Value> {
    if let ProcessedRow::Done {
        key,
        attempt,
        ok,
        claude_ran,
        claude_ok,
        claude_cost,
        attempt_counted,
        summary,
        ..
    } = d
    {
        Some(serde_json::json!({
            "ts": state::now_iso(),
            "key": key,
            "attempt": attempt,
            "phase": "done",
            "ok": ok,
            "claude_ran": claude_ran,
            "claude_cost": claude_cost,
            "attempt_counted": attempt_counted,
            "claude_ok": claude_ok,
            "summary": summary,
        }))
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn row_key_from_dedup_field() {
        let action = mk_action(Some("idReadable".to_string()));
        let r = row_key(&action, &json!({"idReadable": "MK-1"}), 0);
        assert_eq!(r, "MK-1");
    }

    #[test]
    fn row_key_fallback_to_index() {
        let action = mk_action(None);
        let r = row_key(&action, &json!({"foo": "bar"}), 7);
        assert_eq!(r, "row-7");
    }

    fn mk_action(dedup_key: Option<String>) -> LoadedAction {
        LoadedAction {
            action: crate::action::Action {
                name: "t".into(),
                workdir_base: None,
                input: crate::action::InputBlock {
                    command: "x".into(),
                },
                dedup_key,
                filter: None,
                pre: vec![],
                prompt: "y".into(),
                claude: Default::default(),
                post: vec![],
                output: Default::default(),
                limits: Default::default(),
                schedule: None,
                link_template: None,
                enabled: true,
            },
            path: std::path::PathBuf::from("/dev/null"),
            yaml_sha256: "".into(),
        }
    }
}
