//! Read-only troubleshooting MCP server (`claude-run mcp stdio`, CLAUDE-437).
//!
//! Runs inside the container as `docker compose exec --no-TTY --user claudebot
//! <service> claude-run mcp stdio`. The tool set is limited to status, runs,
//! attempts, stream logs and CI failures; no tool writes anything. See
//! `docs/conventions/mcp.md`.
//!
//! Over this transport stdout carries nothing but MCP frames: `main()` routes
//! tracing to stderr for this subcommand only (see `resolve_log_target` in
//! `src/bin/claude-run.rs`), and every tool error logs to stderr via
//! [`tool_error`] / [`tool_not_found`] rather than printing.

use rmcp::handler::server::router::tool::ToolRouter;
use rmcp::handler::server::wrapper::Parameters;
use rmcp::model::{CallToolResult, Content, Implementation, ServerCapabilities, ServerInfo};
use rmcp::{tool, tool_handler, tool_router, ServerHandler};
use schemars::JsonSchema;
use serde::Deserialize;

use crate::config as cfg;
use crate::db::DbPool;
use crate::{ci_failure, state};

/// `list_runs` / `list_ci_failures` `limit` default.
const DEFAULT_LIST_LIMIT: i64 = 20;
/// `list_runs` / `list_ci_failures` `limit` ceiling; above this is an error.
const MAX_LIST_LIMIT: i64 = 100;
/// `get_stream_log` `tail_lines` default.
const DEFAULT_TAIL_LINES: u32 = 200;
/// `get_stream_log` `tail_lines` ceiling; above this is an error.
const MAX_TAIL_LINES: u32 = 2000;
/// `get_stream_log` whole-response cap.
const STREAM_LOG_RESPONSE_CAP_BYTES: usize = 256 * 1024;

fn json_result(value: serde_json::Value) -> CallToolResult {
    CallToolResult::success(vec![Content::text(value.to_string())])
}

/// A failed read or an unreadable file: logged at `warn` (the query or file
/// itself is the problem), reported to the caller with the cause.
fn tool_error(cause: impl std::fmt::Display) -> CallToolResult {
    tracing::warn!(error = %cause, "mcp: tool call failed");
    CallToolResult::error(vec![Content::text(cause.to_string())])
}

/// An unknown id / key / action: logged at `info` (an expected "no such
/// thing" rather than a failure), reported to the caller with the cause.
fn tool_not_found(cause: impl std::fmt::Display) -> CallToolResult {
    tracing::info!(message = %cause, "mcp: tool call found no match");
    CallToolResult::error(vec![Content::text(cause.to_string())])
}

/// Validate `limit`: `None` becomes `default`, a value over `max` is an
/// error naming the parameter and the value rather than being clamped.
fn validate_limit(limit: Option<i64>, default: i64, max: i64) -> Result<i64, CallToolResult> {
    match limit {
        None => Ok(default),
        Some(v) if v > max => Err(tool_error(format!("limit must not exceed {max} (got {v})"))),
        Some(v) if v < 1 => Err(tool_error(format!("limit must be at least 1 (got {v})"))),
        Some(v) => Ok(v),
    }
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct ListRunsArgs {
    /// Restrict to this action name. Omit to list runs for every action.
    #[serde(default)]
    pub action: Option<String>,
    /// Keep only runs with at least one job whose latest attempt hard-failed.
    #[serde(default)]
    pub failed_only: Option<bool>,
    /// Maximum rows to return, newest first. Defaults to 20, capped at 100;
    /// a higher value is an error, not a clamp.
    #[serde(default)]
    pub limit: Option<i64>,
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct RunIdArgs {
    /// The `runs.id` to inspect.
    pub run_id: i64,
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct GetAttemptsArgs {
    /// The action name the attempt counter is keyed under.
    pub action: String,
    /// The job key (dedup key) the attempt counter is keyed under.
    pub key: String,
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct GetStreamLogArgs {
    /// The `runs.id` the job ran under.
    pub run_id: i64,
    /// The job key whose stream log to fetch.
    pub job_key: String,
    /// Which attempt to fetch. Defaults to that job's latest attempt in this run.
    #[serde(default)]
    pub attempt: Option<u32>,
    /// How many trailing lines to return. Defaults to 200, capped at 2000; a
    /// higher value is an error, not a clamp.
    #[serde(default)]
    pub tail_lines: Option<u32>,
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct ListCiFailuresArgs {
    /// Restrict to this `ci_failures.state` (`open` / `resolved` / `pr_only`).
    /// Omit to list every state.
    #[serde(default)]
    pub state: Option<String>,
    /// Maximum rows to return, newest-updated first. Defaults to 20, capped
    /// at 100; a higher value is an error, not a clamp.
    #[serde(default)]
    pub limit: Option<i64>,
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct CiFailureIdArgs {
    /// The `ci_failures.id` to inspect.
    pub id: i64,
}

/// The `claude-run` read-only troubleshooting MCP server. Holds the DB pool
/// (opened once, migrated by [`crate::db::open`] like every other DB-direct
/// subcommand) and the resolved [`cfg::Paths`] (needed to confine
/// `get_stream_log` to the logs directory).
pub struct McpServer {
    pool: DbPool,
    paths: cfg::Paths,
    // Read by the `#[tool_handler]`-generated dispatcher, not by our code.
    #[allow(dead_code)]
    tool_router: ToolRouter<Self>,
}

impl McpServer {
    pub fn new(pool: DbPool, paths: cfg::Paths) -> Self {
        Self {
            pool,
            paths,
            tool_router: Self::tool_router(),
        }
    }
}

#[tool_router]
impl McpServer {
    #[tool(
        description = "Global pause state, every action with its enabled/schedule/pause/last-run \
                        info, open runs, stale dependency gates, and keys parked at their \
                        attempt cap. Takes no arguments. Read directly off the state store: the \
                        result makes no claim about whether the daemon process itself is \
                        running.",
        annotations(read_only_hint = true, destructive_hint = false)
    )]
    async fn get_status(&self) -> CallToolResult {
        match state::status_blocks(&self.pool).await {
            Ok(b) => json_result(serde_json::json!({
                "source": "database",
                "global_paused": b.global_paused.map(|(at, reason)| serde_json::json!({
                    "paused_at": at,
                    "reason": reason,
                })),
                "activity": {
                    "quiet": b.active_runs.is_empty(),
                    "active_runs": b.active_runs,
                },
                "stale_gates": b.stale_gates,
                "attempt_cap_hits": b.attempt_cap_hits,
                "actions": b.actions,
            })),
            Err(e) => tool_error(e),
        }
    }

    #[tool(
        description = "List runs newest first: id, action, started/ended, exit code, jobs \
                        processed, failed job count, triggered-by, current job key, and stage. \
                        Optional action name and failed_only filter. limit defaults to 20, capped \
                        at 100; a higher value is an error, not a clamp.",
        annotations(read_only_hint = true, destructive_hint = false)
    )]
    async fn list_runs(&self, Parameters(args): Parameters<ListRunsArgs>) -> CallToolResult {
        let limit = match validate_limit(args.limit, DEFAULT_LIST_LIMIT, MAX_LIST_LIMIT) {
            Ok(l) => l,
            Err(e) => return e,
        };
        match state::runs_list(
            &self.pool,
            args.action.as_deref(),
            args.failed_only.unwrap_or(false),
            limit,
        )
        .await
        {
            Ok(rows) => json_result(serde_json::to_value(rows).expect("RunRecord serializes")),
            Err(e) => tool_error(e),
        }
    }

    #[tool(
        description = "One run's row, its job outcomes (key, attempt, ok, claude ran/ok, cost, \
                        timed out, duration, summary, whether a stream log is recorded), and its \
                        run_events (warn/error diagnostics first). Unknown run_id is an error.",
        annotations(read_only_hint = true, destructive_hint = false)
    )]
    async fn get_run(&self, Parameters(args): Parameters<RunIdArgs>) -> CallToolResult {
        let run = match state::run_get(&self.pool, args.run_id).await {
            Ok(Some(r)) => r,
            Ok(None) => return tool_not_found(format!("no run with id {}", args.run_id)),
            Err(e) => return tool_error(e),
        };
        let outcomes = match state::job_outcomes_for_run(&self.pool, args.run_id).await {
            Ok(o) => o,
            Err(e) => return tool_error(e),
        };
        let events = match state::run_events_for_run(&self.pool, args.run_id).await {
            Ok(e) => e,
            Err(e) => return tool_error(e),
        };
        json_result(serde_json::json!({
            "run": run,
            "job_outcomes": outcomes,
            "run_events": events,
        }))
    }

    #[tool(
        description = "The attempt counter for one (action, key) pair, plus the action's \
                        effective attempt cap.",
        annotations(read_only_hint = true, destructive_hint = false)
    )]
    async fn get_attempts(&self, Parameters(args): Parameters<GetAttemptsArgs>) -> CallToolResult {
        let count = match state::attempts_get(&self.pool, &args.action, &args.key).await {
            Ok(c) => c,
            Err(e) => return tool_error(e),
        };
        let actions = match state::action_index_list(&self.pool).await {
            Ok(a) => a,
            Err(e) => return tool_error(e),
        };
        let defaults = crate::job::RunnerDefaults::default();
        let caps = crate::job::attempt_caps(&actions, &defaults);
        let cap = caps
            .get(&args.action)
            .copied()
            .unwrap_or(defaults.max_attempts_per_key);
        json_result(serde_json::json!({
            "action": args.action,
            "key": args.key,
            "count": count,
            "cap": cap,
        }))
    }

    #[tool(
        description = "Tail one job attempt's recorded stream log. run_id and job_key are \
                        required; attempt defaults to that job's latest attempt in the run. \
                        tail_lines defaults to 200, capped at 2000; the whole response is also \
                        capped at 256 KiB. The result always states total_lines, returned_lines \
                        and truncated. Stream logs are raw agent output, returned unredacted: the \
                        caller already has full container access.",
        annotations(read_only_hint = true, destructive_hint = false)
    )]
    async fn get_stream_log(
        &self,
        Parameters(args): Parameters<GetStreamLogArgs>,
    ) -> CallToolResult {
        let tail_lines = match args.tail_lines {
            None => DEFAULT_TAIL_LINES,
            Some(v) if v > MAX_TAIL_LINES => {
                return tool_error(format!(
                    "tail_lines must not exceed {MAX_TAIL_LINES} (got {v})"
                ))
            }
            Some(0) => return tool_error("tail_lines must be at least 1 (got 0)"),
            Some(v) => v,
        };

        let recorded = match state::job_outcome_stream_log(
            &self.pool,
            args.run_id,
            &args.job_key,
            args.attempt,
        )
        .await
        {
            Ok(r) => r,
            Err(e) => return tool_error(e),
        };
        let (attempt, path) = match recorded {
            None => {
                return tool_not_found(format!(
                    "no job outcome recorded for run {} job {:?} attempt {:?}",
                    args.run_id, args.job_key, args.attempt
                ))
            }
            Some((attempt, None)) => {
                return tool_error(format!(
                    "no stream log recorded for run {} job {:?} attempt {attempt}",
                    args.run_id, args.job_key
                ))
            }
            Some((attempt, Some(path))) => (attempt, path),
        };

        let logs_dir = self.paths.logs_dir();
        let canon_logs_dir = match std::fs::canonicalize(&logs_dir) {
            Ok(d) => d,
            Err(e) => return tool_error(format!("logs directory {}: {e}", logs_dir.display())),
        };
        let canon_path = match std::fs::canonicalize(&path) {
            Ok(p) => p,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                return tool_error(format!(
                    "stream log file not found at {path}; the age sweep may have removed it"
                ))
            }
            Err(e) => return tool_error(format!("{path}: {e}")),
        };
        if !canon_path.starts_with(&canon_logs_dir) {
            return tool_error(format!(
                "recorded stream log path {} resolves outside the logs directory",
                canon_path.display()
            ));
        }

        let content = match std::fs::read_to_string(&canon_path) {
            Ok(c) => c,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                return tool_error(format!(
                    "stream log file not found at {path}; the age sweep may have removed it"
                ))
            }
            Err(e) => return tool_error(e.to_string()),
        };

        let (total_lines, returned_lines, truncated, tail) =
            tail_text(&content, tail_lines as usize, STREAM_LOG_RESPONSE_CAP_BYTES);

        json_result(serde_json::json!({
            "run_id": args.run_id,
            "job_key": args.job_key,
            "attempt": attempt,
            "path": path,
            "total_lines": total_lines,
            "returned_lines": returned_lines,
            "truncated": truncated,
            "content": tail,
        }))
    }

    #[tool(
        description = "List CI failures newest-updated first: id, repo, workflow id, job name, \
                        infra kind, issue key, state, created/updated. Optional state filter \
                        (open/resolved/pr_only). limit defaults to 20, capped at 100; a higher \
                        value is an error, not a clamp.",
        annotations(read_only_hint = true, destructive_hint = false)
    )]
    async fn list_ci_failures(
        &self,
        Parameters(args): Parameters<ListCiFailuresArgs>,
    ) -> CallToolResult {
        let limit = match validate_limit(args.limit, DEFAULT_LIST_LIMIT, MAX_LIST_LIMIT) {
            Ok(l) => l,
            Err(e) => return e,
        };
        match ci_failure::store::list_failures(&self.pool, args.state.as_deref(), limit).await {
            Ok(rows) => json_result(serde_json::to_value(rows).expect("Failure serializes")),
            Err(e) => tool_error(e),
        }
    }

    #[tool(
        description = "One CI failure row, its distinct sighted branches, and the distinct PR \
                        numbers it has been seen on. Unknown id is an error.",
        annotations(read_only_hint = true, destructive_hint = false)
    )]
    async fn get_ci_failure(
        &self,
        Parameters(args): Parameters<CiFailureIdArgs>,
    ) -> CallToolResult {
        let failure = match ci_failure::store::get_failure(&self.pool, args.id).await {
            Ok(Some(f)) => f,
            Ok(None) => return tool_not_found(format!("no CI failure with id {}", args.id)),
            Err(e) => return tool_error(e),
        };
        let branches = match ci_failure::store::distinct_branches(&self.pool, args.id).await {
            Ok(b) => b,
            Err(e) => return tool_error(e),
        };
        let pr_numbers = match ci_failure::store::pr_numbers_for_failure(&self.pool, args.id).await
        {
            Ok(p) => p,
            Err(e) => return tool_error(e),
        };
        json_result(serde_json::json!({
            "failure": failure,
            "distinct_branches": branches,
            "pr_numbers": pr_numbers,
        }))
    }
}

#[tool_handler]
impl ServerHandler for McpServer {
    fn get_info(&self) -> ServerInfo {
        ServerInfo::new(ServerCapabilities::builder().enable_tools().build())
            .with_server_info(Implementation::new("claude-run", crate::VERSION))
    }
}

/// Split `content` into lines, keep the last `tail_lines`, then trim from the
/// front (on a char boundary) so the returned text fits `cap_bytes`. Returns
/// `(total_lines, returned_lines, truncated, text)`; `truncated` is true
/// whenever either the line-count tail or the byte cap dropped something.
fn tail_text(content: &str, tail_lines: usize, cap_bytes: usize) -> (i64, i64, bool, String) {
    let lines: Vec<&str> = content.lines().collect();
    let total_lines = lines.len();
    let start = total_lines.saturating_sub(tail_lines);
    let mut truncated = start > 0;
    let mut tail = lines[start..].join("\n");
    if tail.len() > cap_bytes {
        let mut cut = tail.len() - cap_bytes;
        while cut < tail.len() && !tail.is_char_boundary(cut) {
            cut += 1;
        }
        tail = tail[cut..].to_string();
        truncated = true;
    }
    let returned_lines = if tail.is_empty() && total_lines == 0 {
        0
    } else {
        tail.lines().count()
    };
    (total_lines as i64, returned_lines as i64, truncated, tail)
}

/// Start the MCP server over stdio: open the DB pool, migrate it (same as
/// every other DB-direct subcommand), serve until the client disconnects.
pub async fn serve_stdio(
    db_target: &cfg::DatabaseTarget,
    paths: cfg::Paths,
) -> crate::error::Result<()> {
    use rmcp::ServiceExt;
    let pool = crate::db::open(db_target).await?;
    tracing::info!(
        version = crate::VERSION,
        "starting claude-run mcp server on stdio"
    );
    let server = McpServer::new(pool, paths);
    let service = server
        .serve(rmcp::transport::stdio())
        .await
        .map_err(|e| anyhow::anyhow!("mcp stdio: {e}"))?;
    service
        .waiting()
        .await
        .map_err(|e| anyhow::anyhow!("mcp stdio: {e}"))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ci_failure::store as ci_store;

    fn result_text(r: &CallToolResult) -> String {
        r.content
            .first()
            .and_then(|c| c.as_text())
            .map(|t| t.text.clone())
            .unwrap_or_default()
    }

    async fn test_server() -> (McpServer, tempfile::TempDir) {
        let pool = crate::db::test_pool().await;
        let dir = tempfile::tempdir().expect("tempdir");
        let paths = cfg::Paths {
            config_dir: dir.path().join("config"),
            data_dir: dir.path().join("data"),
            state_dir: dir.path().join("state"),
            runtime_dir: dir.path().join("runtime"),
        };
        std::fs::create_dir_all(paths.logs_dir()).expect("create logs dir");
        (McpServer::new(pool, paths), dir)
    }

    #[tokio::test]
    async fn tool_router_registers_every_tool() {
        let (server, _dir) = test_server().await;
        for name in [
            "get_status",
            "list_runs",
            "get_run",
            "get_attempts",
            "get_stream_log",
            "list_ci_failures",
            "get_ci_failure",
        ] {
            assert!(
                server.tool_router.has_route(name),
                "missing tool route: {name}"
            );
        }
    }

    #[tokio::test]
    async fn get_status_reports_source_database_and_no_daemon_keys() {
        let (server, _dir) = test_server().await;
        let result = server.get_status().await;
        assert_ne!(result.is_error, Some(true));
        let value: serde_json::Value = serde_json::from_str(&result_text(&result)).unwrap();
        assert_eq!(value["source"], "database");
        assert!(value.get("daemon").is_none());
        assert!(value.get("socket_expected_at").is_none());
    }

    #[tokio::test]
    async fn list_runs_rejects_limit_above_max_instead_of_clamping() {
        let (server, _dir) = test_server().await;
        let result = server
            .list_runs(Parameters(ListRunsArgs {
                action: None,
                failed_only: None,
                limit: Some(MAX_LIST_LIMIT + 1),
            }))
            .await;
        assert_eq!(result.is_error, Some(true));
        assert!(result_text(&result).contains("limit"));
    }

    #[tokio::test]
    async fn get_run_errors_on_unknown_run_id() {
        let (server, _dir) = test_server().await;
        let result = server
            .get_run(Parameters(RunIdArgs { run_id: 999_999 }))
            .await;
        assert_eq!(result.is_error, Some(true));
        assert!(result_text(&result).contains("999999"));
    }

    #[tokio::test]
    async fn get_run_returns_outcomes_for_a_seeded_run() {
        let (server, _dir) = test_server().await;
        let run_id = state::run_begin(&server_pool(&server), "youtrack", "cli")
            .await
            .unwrap();
        state::job_outcome_insert(
            &server_pool(&server),
            &state::JobOutcome {
                run_id,
                action_name: "youtrack",
                job_key: "CLAUDE-1",
                attempt: 1,
                ok: true,
                claude_ran: true,
                claude_ok: true,
                claude_cost_usd: 0.5,
                attempt_counted: true,
                summary: Some("did a thing"),
                stream_log_path: None,
                duration_ms: 1000,
                timed_out: false,
            },
        )
        .await
        .unwrap();

        let result = server.get_run(Parameters(RunIdArgs { run_id })).await;
        assert_ne!(result.is_error, Some(true));
        let value: serde_json::Value = serde_json::from_str(&result_text(&result)).unwrap();
        assert_eq!(value["run"]["id"], run_id);
        assert_eq!(value["job_outcomes"][0]["job_key"], "CLAUDE-1");
    }

    #[tokio::test]
    async fn get_ci_failure_errors_on_unknown_id() {
        let (server, _dir) = test_server().await;
        let result = server
            .get_ci_failure(Parameters(CiFailureIdArgs { id: 424_242 }))
            .await;
        assert_eq!(result.is_error, Some(true));
        assert!(result_text(&result).contains("424242"));
    }

    #[tokio::test]
    async fn get_ci_failure_returns_a_seeded_failure() {
        let (server, _dir) = test_server().await;
        let pool = server_pool(&server);
        let id = ci_store::record_failure(
            &pool,
            "psa-systems/claude-run",
            "check",
            "test",
            None,
            None,
            "open",
            "2026-01-01T00:00:00Z",
        )
        .await
        .unwrap();

        let result = server
            .get_ci_failure(Parameters(CiFailureIdArgs { id }))
            .await;
        assert_ne!(result.is_error, Some(true));
        let value: serde_json::Value = serde_json::from_str(&result_text(&result)).unwrap();
        assert_eq!(value["failure"]["id"], id);
    }

    #[tokio::test]
    async fn get_stream_log_errors_when_no_outcome_is_recorded() {
        let (server, _dir) = test_server().await;
        let result = server
            .get_stream_log(Parameters(GetStreamLogArgs {
                run_id: 1,
                job_key: "no-such-job".to_string(),
                attempt: None,
                tail_lines: None,
            }))
            .await;
        assert_eq!(result.is_error, Some(true));
    }

    #[tokio::test]
    async fn get_stream_log_errors_on_null_recorded_path() {
        let (server, _dir) = test_server().await;
        let pool = server_pool(&server);
        let run_id = state::run_begin(&pool, "youtrack", "cli").await.unwrap();
        state::job_outcome_insert(
            &pool,
            &state::JobOutcome {
                run_id,
                action_name: "youtrack",
                job_key: "CLAUDE-2",
                attempt: 1,
                ok: true,
                claude_ran: true,
                claude_ok: true,
                claude_cost_usd: 0.0,
                attempt_counted: true,
                summary: None,
                stream_log_path: None,
                duration_ms: 1,
                timed_out: false,
            },
        )
        .await
        .unwrap();

        let result = server
            .get_stream_log(Parameters(GetStreamLogArgs {
                run_id,
                job_key: "CLAUDE-2".to_string(),
                attempt: None,
                tail_lines: None,
            }))
            .await;
        assert_eq!(result.is_error, Some(true));
        assert!(result_text(&result).contains("no stream log recorded"));
    }

    #[tokio::test]
    async fn get_stream_log_errors_when_the_file_is_missing() {
        let (server, dir) = test_server().await;
        let pool = server_pool(&server);
        let run_id = state::run_begin(&pool, "youtrack", "cli").await.unwrap();
        let missing = dir.path().join("state/logs/youtrack/streams/gone.jsonl");
        state::job_outcome_insert(
            &pool,
            &state::JobOutcome {
                run_id,
                action_name: "youtrack",
                job_key: "CLAUDE-3",
                attempt: 1,
                ok: true,
                claude_ran: true,
                claude_ok: true,
                claude_cost_usd: 0.0,
                attempt_counted: true,
                summary: None,
                stream_log_path: Some(missing.to_str().unwrap()),
                duration_ms: 1,
                timed_out: false,
            },
        )
        .await
        .unwrap();

        let result = server
            .get_stream_log(Parameters(GetStreamLogArgs {
                run_id,
                job_key: "CLAUDE-3".to_string(),
                attempt: None,
                tail_lines: None,
            }))
            .await;
        assert_eq!(result.is_error, Some(true));
        assert!(result_text(&result).contains("not found"));
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn get_stream_log_rejects_a_symlink_escape_outside_the_logs_dir() {
        let (server, dir) = test_server().await;
        let pool = server_pool(&server);
        let run_id = state::run_begin(&pool, "youtrack", "cli").await.unwrap();

        let outside = dir.path().join("outside-secret.jsonl");
        std::fs::write(&outside, "top secret\n").unwrap();
        let streams_dir = dir.path().join("state/logs/youtrack/streams");
        std::fs::create_dir_all(&streams_dir).unwrap();
        let link = streams_dir.join("escape-attempt-1.jsonl");
        std::os::unix::fs::symlink(&outside, &link).unwrap();

        state::job_outcome_insert(
            &pool,
            &state::JobOutcome {
                run_id,
                action_name: "youtrack",
                job_key: "escape",
                attempt: 1,
                ok: true,
                claude_ran: true,
                claude_ok: true,
                claude_cost_usd: 0.0,
                attempt_counted: true,
                summary: None,
                stream_log_path: Some(link.to_str().unwrap()),
                duration_ms: 1,
                timed_out: false,
            },
        )
        .await
        .unwrap();

        let result = server
            .get_stream_log(Parameters(GetStreamLogArgs {
                run_id,
                job_key: "escape".to_string(),
                attempt: None,
                tail_lines: None,
            }))
            .await;
        assert_eq!(result.is_error, Some(true));
        assert!(result_text(&result).contains("outside the logs directory"));
    }

    #[tokio::test]
    async fn get_stream_log_reports_truncation_for_a_long_log() {
        let (server, dir) = test_server().await;
        let pool = server_pool(&server);
        let run_id = state::run_begin(&pool, "youtrack", "cli").await.unwrap();

        let streams_dir = dir.path().join("state/logs/youtrack/streams");
        std::fs::create_dir_all(&streams_dir).unwrap();
        let log_path = streams_dir.join("tailme-attempt-1.jsonl");
        let lines: Vec<String> = (1..=500).map(|n| format!("line {n}")).collect();
        std::fs::write(&log_path, lines.join("\n")).unwrap();

        state::job_outcome_insert(
            &pool,
            &state::JobOutcome {
                run_id,
                action_name: "youtrack",
                job_key: "tailme",
                attempt: 1,
                ok: true,
                claude_ran: true,
                claude_ok: true,
                claude_cost_usd: 0.0,
                attempt_counted: true,
                summary: None,
                stream_log_path: Some(log_path.to_str().unwrap()),
                duration_ms: 1,
                timed_out: false,
            },
        )
        .await
        .unwrap();

        let result = server
            .get_stream_log(Parameters(GetStreamLogArgs {
                run_id,
                job_key: "tailme".to_string(),
                attempt: None,
                tail_lines: Some(100),
            }))
            .await;
        assert_ne!(result.is_error, Some(true));
        let value: serde_json::Value = serde_json::from_str(&result_text(&result)).unwrap();
        assert_eq!(value["total_lines"], 500);
        assert_eq!(value["returned_lines"], 100);
        assert_eq!(value["truncated"], true);
        assert!(value["content"].as_str().unwrap().starts_with("line 401"));
    }

    fn server_pool(server: &McpServer) -> DbPool {
        server.pool.clone()
    }

    #[test]
    fn tail_text_reports_full_content_when_under_the_limit() {
        let (total, returned, truncated, text) = tail_text("a\nb\nc", 200, 256 * 1024);
        assert_eq!(total, 3);
        assert_eq!(returned, 3);
        assert!(!truncated);
        assert_eq!(text, "a\nb\nc");
    }

    #[test]
    fn tail_text_truncates_by_line_count() {
        let content = (1..=10)
            .map(|n| n.to_string())
            .collect::<Vec<_>>()
            .join("\n");
        let (total, returned, truncated, text) = tail_text(&content, 3, 256 * 1024);
        assert_eq!(total, 10);
        assert_eq!(returned, 3);
        assert!(truncated);
        assert_eq!(text, "8\n9\n10");
    }

    #[test]
    fn tail_text_truncates_by_byte_cap_on_a_char_boundary() {
        let content = "x".repeat(100);
        let (total, returned, truncated, text) = tail_text(&content, 200, 40);
        assert_eq!(total, 1);
        assert_eq!(returned, 1);
        assert!(truncated);
        assert_eq!(text.len(), 40);
    }

    #[test]
    fn validate_limit_errors_above_the_max_instead_of_clamping() {
        assert!(validate_limit(Some(101), 20, 100).is_err());
        assert_eq!(validate_limit(Some(50), 20, 100).unwrap(), 50);
        assert_eq!(validate_limit(None, 20, 100).unwrap(), 20);
    }
}
