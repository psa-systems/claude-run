//! Native cross-project closed-issue reporter (CLAUDE-218).
//!
//! Uses the automation as a reporting tool rather than an issue worker: count
//! the issues closed in a rolling window across a named set of projects, write
//! the summary, and file it as a new issue hanging off a nominated parent
//! (typically the milestone being reported against).
//!
//! Pipeline (typed Rust, mirroring [`crate::native::ci_failure_reconcile`]):
//!
//! - targets -> the union of `parents` (nominated by id) and the issues
//!   `query` selects (skipped entirely when `query` is empty).
//! - gather  -> one `yt list --query="project: <P> resolved date: <start> ..
//!   <end>"` per configured project. Fully paginated, so the counts are exact;
//!   only the rendered LISTING is capped, and a capped project says so.
//! - render  -> [`render_report`], a pure function over the gathered rows:
//!   per-project counts, a total, and a per-project listing.
//! - file    -> `yt issue create` in `report_project` (defaulting to the
//!   parent's own project), then `yt issue link <report> <parent> --type
//!   <link_type>`, then the marker comment on the parent.
//!
//! The run touches no repository: it clones nothing, starts no agent session,
//! and opens no pull request, so there is no forge call and no git call on any
//! path. That is the contract, not an accident of the current code, and
//! `report_run_makes_no_code_change_and_opens_no_pr` holds it.
//!
//! Idempotency: the marker comment carries a hidden `Report-Window:` line. A
//! parent already carrying this run's window marker is skipped, so a poll
//! interval shorter than the window files one report per window rather than one
//! per poll.

use std::collections::HashSet;

use chrono::{Duration, Local, NaiveDate};
use tracing::{info, warn};

use super::{HandlerFuture, NativeContext, NativeHandler};
use crate::action::{ClosedIssueSummaryConfig, NativeConfig};
use crate::cancel::{CancellationToken, RegistryGuard};
use crate::error::{Error, Result};
use crate::provider::{self, Backend};
use crate::state;

/// Hidden HTML comment fencing the machine-read block of a report body and of
/// the marker comment posted on the parent.
const MARKER_FENCE: &str = "<!-- claude-run closed-issue-summary; do not edit -->";

pub struct ClosedIssueSummaryHandler;

impl NativeHandler for ClosedIssueSummaryHandler {
    fn run<'a>(&self, ctx: NativeContext<'a>) -> HandlerFuture<'a> {
        Box::pin(async move { run_closed_issue_summary(ctx).await })
    }
}

/// The reporting window: an inclusive `[start, end]` date range, `days` long.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Window {
    pub start: NaiveDate,
    pub end: NaiveDate,
    pub days: u32,
}

impl Window {
    /// The `days`-long window ending on `end` inclusive, so `days = 7` covers
    /// `end` plus the six days before it. `days = 0` is normalized to 1: a
    /// zero-length window would report on nothing at all.
    pub fn ending(end: NaiveDate, days: u32) -> Self {
        let days = days.max(1);
        Window {
            start: end - Duration::days(i64::from(days) - 1),
            end,
            days,
        }
    }

    /// The hidden idempotency marker identifying this exact window.
    fn marker(&self) -> String {
        format!("Report-Window: {}..{}", self.start, self.end)
    }
}

/// One closed issue, flattened from the row `yt list --json` emits.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClosedIssue {
    pub key: String,
    pub summary: String,
    pub issue_type: String,
    pub state: String,
    pub assignee: String,
}

impl ClosedIssue {
    fn from_row(row: &serde_json::Value) -> Option<Self> {
        let text = |k: &str| {
            row.get(k)
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .trim()
                .to_string()
        };
        let key = text("idReadable");
        if key.is_empty() {
            return None;
        }
        Some(ClosedIssue {
            key,
            summary: text("summary"),
            issue_type: text("Type"),
            state: text("State"),
            assignee: text("Assignee"),
        })
    }
}

/// What one parent's pass produced.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Filing {
    /// The report was created and linked; carries its new issue id.
    Filed(String),
    /// The parent already carries this window's marker, so nothing was written.
    AlreadyFiled,
}

async fn run_closed_issue_summary(ctx: NativeContext<'_>) -> Result<()> {
    let action = &ctx.action.action;
    let pool = ctx.pool;

    let cfg = match &action.native {
        Some(NativeConfig::ClosedIssueSummary(cfg)) => (**cfg).clone(),
        _ => {
            return Err(Error::NativeUnimplemented {
                kind: action.kind.as_str().to_string(),
            })
        }
    };

    if !cfg.enabled {
        info!(action = %action.name, "closed-issue-summary is disabled; skipping run");
        return Ok(());
    }
    if cfg.projects.is_empty() {
        warn!(action = %action.name, "no projects configured; nothing to report on");
        return Ok(());
    }

    let backend = provider::resolve(provider::DEFAULT_PRIORITY)?;

    // Resolve targets BEFORE run_begin so a gather failure is a no-op run.
    let (targets, from_query) = match resolve_targets(backend.as_ref(), &cfg) {
        Ok(t) => t,
        Err(e) => {
            warn!(action = %action.name, error = %e, "selecting report parents failed; skipping run");
            return Ok(());
        }
    };
    if targets.is_empty() {
        info!(action = %action.name, "no parent issue to report against");
        return Ok(());
    }

    let window = Window::ending(Local::now().date_naive(), cfg.window_days);

    let run_id = state::run_begin(pool, &action.name, ctx.triggered_by).await?;
    let cancel = match ctx.registry {
        Some(r) => r.register(run_id),
        None => CancellationToken::new(),
    };
    let _registry_guard = RegistryGuard {
        registry: ctx.registry,
        run_id,
    };
    if let Err(e) = state::run_set_job_keys(pool, run_id, &targets).await {
        warn!(run_id, error = %e, "recording planned keys failed (continuing)");
    }

    // CLAUDE-223: run-scoped global-pause gate, checked alongside cancel at the
    // per-parent pickup so a mid-run pause stops the next report.
    let run_pause = ctx.pause.for_action_run(&action.name);
    let mut filed: u32 = 0;
    for parent in &targets {
        if cancel.is_cancelled() {
            break;
        }
        if run_pause.stop().await {
            break;
        }
        match file_report(backend.as_ref(), &cfg, parent, window) {
            Ok(Filing::Filed(key)) => {
                info!(action = %action.name, parent, report = %key, "filed the closed-issue summary");
                filed += 1;
                advance(backend.as_ref(), &cfg, &from_query, parent, &cfg.done_label);
            }
            Ok(Filing::AlreadyFiled) => {
                info!(action = %action.name, parent, window = %window.marker(), "a report for this window is already filed; skipping");
            }
            Err(e) => {
                warn!(action = %action.name, parent, error = %e, "filing the closed-issue summary failed (continuing)");
                advance(
                    backend.as_ref(),
                    &cfg,
                    &from_query,
                    parent,
                    &cfg.failed_label,
                );
            }
        }
    }

    let exit_code = run_pause.exit_code(cancel.is_cancelled());
    state::run_finish(pool, run_id, exit_code, filed).await?;
    Ok(())
}

/// The parents to report against: the configured `parents` (in order) unioned
/// with the issues `query` selects, plus the subset that came from the query.
/// Only that subset is ever field-written; a parent nominated by id is left
/// alone entirely.
fn resolve_targets(
    backend: &dyn Backend,
    cfg: &ClosedIssueSummaryConfig,
) -> Result<(Vec<String>, HashSet<String>)> {
    let mut targets: Vec<String> = Vec::new();
    for p in &cfg.parents {
        let p = p.trim();
        if !p.is_empty() && !targets.iter().any(|t| t == p) {
            targets.push(p.to_string());
        }
    }
    let mut from_query: HashSet<String> = HashSet::new();
    if !cfg.query.trim().is_empty() {
        for row in backend.list_queued(&cfg.query)? {
            let Some(key) = row.get("idReadable").and_then(|v| v.as_str()) else {
                continue;
            };
            from_query.insert(key.to_string());
            if !targets.iter().any(|t| t == key) {
                targets.push(key.to_string());
            }
        }
    }
    Ok((targets, from_query))
}

/// Advance a query-selected parent's custom field to `label`. A parent
/// nominated by id never reaches the write: it has no reporting state machine,
/// and its idempotency comes from the window marker instead.
fn advance(
    backend: &dyn Backend,
    cfg: &ClosedIssueSummaryConfig,
    from_query: &HashSet<String>,
    parent: &str,
    label: &str,
) {
    if !from_query.contains(parent) {
        return;
    }
    if let Err(e) = backend.set_field(parent, &cfg.custom_field, label) {
        warn!(parent, field = %cfg.custom_field, %label, error = %e, "advancing the parent's field failed");
    }
}

/// Produce and file one parent's report: gather, render, create, link, mark.
///
/// The marker comment is posted LAST, so a failure anywhere earlier leaves the
/// window unmarked and the next run retries rather than recording a report that
/// was never linked.
fn file_report(
    backend: &dyn Backend,
    cfg: &ClosedIssueSummaryConfig,
    parent: &str,
    window: Window,
) -> Result<Filing> {
    if already_filed(backend, parent, window) {
        return Ok(Filing::AlreadyFiled);
    }

    let mut gathered: Vec<(String, Vec<ClosedIssue>)> = Vec::new();
    for project in &cfg.projects {
        gathered.push((project.clone(), gather_closed(backend, project, window)?));
    }
    let total: usize = gathered.iter().map(|(_, v)| v.len()).sum();

    let summary = render_summary_line(&cfg.summary_template, window, total, parent);
    let body = render_report(window, &gathered, cfg.max_listed_per_project);
    let project = match cfg.report_project.as_deref().map(str::trim) {
        Some(p) if !p.is_empty() => p.to_string(),
        _ => project_key(parent),
    };

    let report = backend.create_issue(&project, &summary, &body, None)?;
    backend.link_issue(&report, &cfg.link_type, parent)?;
    backend.comment(
        parent,
        &parent_comment(&report, window, total, &cfg.projects),
    )?;
    Ok(Filing::Filed(report))
}

/// True when the parent's description or any of its comments already carries
/// this window's marker. An unreadable parent errs toward filing: a duplicate
/// report is visible and fixable, a silently skipped window is not.
fn already_filed(backend: &dyn Backend, parent: &str, window: Window) -> bool {
    let marker = window.marker();
    match backend.inspect_issue(parent) {
        Ok(i) => i.description.contains(&marker) || i.comments.iter().any(|c| c.contains(&marker)),
        Err(e) => {
            warn!(parent, error = %e, "reading the parent for the idempotency marker failed; filing anyway");
            false
        }
    }
}

/// Every issue in `project` resolved inside `window`. The query is the tracker's
/// own `resolved date:` range, so the window is applied server-side and the row
/// set is exact rather than filtered from a truncated page.
fn gather_closed(backend: &dyn Backend, project: &str, window: Window) -> Result<Vec<ClosedIssue>> {
    let query = format!(
        "project: {} resolved date: {} .. {}",
        project.trim(),
        window.start,
        window.end
    );
    let rows = backend.list_queued(&query)?;
    Ok(rows.iter().filter_map(ClosedIssue::from_row).collect())
}

/// The report issue's summary line, with `{start}` / `{end}` / `{days}` /
/// `{total}` / `{parent}` substituted.
pub fn render_summary_line(template: &str, window: Window, total: usize, parent: &str) -> String {
    template
        .replace("{start}", &window.start.to_string())
        .replace("{end}", &window.end.to_string())
        .replace("{days}", &window.days.to_string())
        .replace("{total}", &total.to_string())
        .replace("{parent}", parent)
}

/// The report body: a counts table over every configured project (zero-count
/// projects included, so a quiet project reads as quiet rather than as missing),
/// then one section per project listing its closed issues.
///
/// `max_listed` caps the LISTING only; every count is the true total, and a
/// capped section states how many rows it did not print.
pub fn render_report(
    window: Window,
    gathered: &[(String, Vec<ClosedIssue>)],
    max_listed: usize,
) -> String {
    let total: usize = gathered.iter().map(|(_, v)| v.len()).sum();
    let mut out = String::new();

    out.push_str(&format!(
        "Issues closed between {} and {} ({} days), across {} project(s).\n",
        window.start,
        window.end,
        window.days,
        gathered.len()
    ));

    out.push_str("\n## Counts\n\n| Project | Closed |\n| --- | ---: |\n");
    for (project, issues) in gathered {
        out.push_str(&format!("| {} | {} |\n", project, issues.len()));
    }
    out.push_str(&format!("| **Total** | **{total}** |\n"));

    for (project, issues) in gathered {
        out.push_str(&format!("\n## {} ({})\n\n", project, issues.len()));
        if issues.is_empty() {
            out.push_str("No issues were closed in this window.\n");
            continue;
        }
        for issue in issues.iter().take(max_listed) {
            out.push_str(&format!(
                "- {} - {} - {}\n",
                issue.key,
                issue.summary,
                facets(issue)
            ));
        }
        if issues.len() > max_listed {
            out.push_str(&format!(
                "- ... and {} more (listing capped at {}).\n",
                issues.len() - max_listed,
                max_listed
            ));
        }
    }

    out.push_str(&format!("\n{MARKER_FENCE}\n{}\n", window.marker()));
    out
}

/// The trailing `Type / State / Assignee` facets of one listed issue, with the
/// empty ones dropped so a project without an Assignee field does not render a
/// row of separators.
fn facets(issue: &ClosedIssue) -> String {
    let parts: Vec<&str> = [
        issue.issue_type.as_str(),
        issue.state.as_str(),
        issue.assignee.as_str(),
    ]
    .into_iter()
    .filter(|s| !s.is_empty())
    .collect();
    if parts.is_empty() {
        "closed".to_string()
    } else {
        parts.join(" / ")
    }
}

/// The marker comment posted on the parent: what was filed, plus the hidden
/// window marker the next run reads for idempotency.
fn parent_comment(report: &str, window: Window, total: usize, projects: &[String]) -> String {
    format!(
        "Filed the closed-issue summary for {} .. {} as {}: {} issue(s) closed across {}.\n\n{}\n{}\n",
        window.start,
        window.end,
        report,
        total,
        projects.join(", "),
        MARKER_FENCE,
        window.marker()
    )
}

/// The project short name of an `idReadable`, e.g. `CLAUDE-218` -> `CLAUDE`.
fn project_key(idreadable: &str) -> String {
    idreadable
        .rsplit_once('-')
        .map(|(p, _)| p.to_string())
        .unwrap_or_else(|| idreadable.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::provider::cli::testutil::StubRunner;
    use crate::provider::CliBackend;
    use std::sync::Arc;

    fn day(s: &str) -> NaiveDate {
        s.parse().unwrap()
    }

    fn window() -> Window {
        Window::ending(day("2026-08-07"), 7)
    }

    fn issue(key: &str, summary: &str, assignee: &str) -> ClosedIssue {
        ClosedIssue {
            key: key.to_string(),
            summary: summary.to_string(),
            issue_type: "Task".to_string(),
            state: "Done".to_string(),
            assignee: assignee.to_string(),
        }
    }

    fn cfg() -> ClosedIssueSummaryConfig {
        ClosedIssueSummaryConfig {
            projects: vec!["CLAUDE".to_string(), "LC".to_string()],
            ..ClosedIssueSummaryConfig::default()
        }
    }

    /// `yt list --json` rows for one project, in the shape the CLI emits.
    fn rows(pairs: &[(&str, &str)]) -> String {
        let items: Vec<String> = pairs
            .iter()
            .map(|(key, summary)| {
                format!(
                    r#"{{"idReadable":"{key}","summary":"{summary}","Type":"Task","State":"Done","Assignee":"Claude Run"}}"#
                )
            })
            .collect();
        format!("[{}]", items.join(","))
    }

    fn list_args(project: &str) -> String {
        format!("--query=project: {project} resolved date: 2026-08-01 .. 2026-08-07")
    }

    // ---- AC 1: counts + a written summary over the configured projects -----

    /// The window is `days` long and ends today inclusive, so the default 7
    /// covers today plus the previous six days.
    #[test]
    fn window_ending_is_inclusive_of_both_ends() {
        let w = Window::ending(day("2026-08-07"), 7);
        assert_eq!(w.start, day("2026-08-01"));
        assert_eq!(w.end, day("2026-08-07"));
        assert_eq!(w.days, 7);
        // A zero-length window would report on nothing; it is normalized to one day.
        let z = Window::ending(day("2026-08-07"), 0);
        assert_eq!(
            (z.start, z.end, z.days),
            (day("2026-08-07"), day("2026-08-07"), 1)
        );
    }

    /// AC 1: a fixture set of closed issues renders exact per-project counts, a
    /// total, and a per-project listing. A project that closed nothing still
    /// appears, with a zero count and an explicit line.
    #[test]
    fn render_report_counts_and_lists_every_configured_project() {
        let gathered = vec![
            (
                "CLAUDE".to_string(),
                vec![
                    issue(
                        "CLAUDE-214",
                        "Allow the agent to author issues",
                        "Claude Run",
                    ),
                    issue("CLAUDE-215", "Partition the audit query", ""),
                ],
            ),
            (
                "LC".to_string(),
                vec![issue("LC-9", "Fix the thing", "dev")],
            ),
            ("YT".to_string(), Vec::new()),
        ];
        let body = render_report(window(), &gathered, 100);

        assert!(
            body.contains(
                "Issues closed between 2026-08-01 and 2026-08-07 (7 days), across 3 project(s)."
            ),
            "{body}"
        );
        assert!(body.contains("| CLAUDE | 2 |"), "{body}");
        assert!(body.contains("| LC | 1 |"), "{body}");
        assert!(body.contains("| YT | 0 |"), "{body}");
        assert!(body.contains("| **Total** | **3** |"), "{body}");

        assert!(body.contains("## CLAUDE (2)"), "{body}");
        assert!(
            body.contains(
                "- CLAUDE-214 - Allow the agent to author issues - Task / Done / Claude Run"
            ),
            "{body}"
        );
        // An empty Assignee drops out rather than rendering a bare separator.
        assert!(
            body.contains("- CLAUDE-215 - Partition the audit query - Task / Done\n"),
            "{body}"
        );
        assert!(body.contains("## YT (0)"), "{body}");
        assert!(
            body.contains("No issues were closed in this window."),
            "{body}"
        );
        assert!(
            body.contains("Report-Window: 2026-08-01..2026-08-07"),
            "{body}"
        );
    }

    /// The listing cap never falsifies a count: the count stays exact and the
    /// dropped rows are named, so a capped section is auditable (no silent
    /// truncation).
    #[test]
    fn render_report_caps_the_listing_but_never_the_count() {
        let many: Vec<ClosedIssue> = (1..=5)
            .map(|n| issue(&format!("CLAUDE-{n}"), "x", "dev"))
            .collect();
        let body = render_report(window(), &[("CLAUDE".to_string(), many)], 2);
        assert!(body.contains("| CLAUDE | 5 |"), "{body}");
        assert!(body.contains("## CLAUDE (5)"), "{body}");
        assert!(
            body.contains("- ... and 3 more (listing capped at 2)."),
            "{body}"
        );
        assert!(!body.contains("CLAUDE-4"), "row 4 is past the cap: {body}");
    }

    #[test]
    fn render_summary_line_substitutes_every_placeholder() {
        let line = render_summary_line(
            "{parent}: {total} closed, {start} .. {end} ({days}d)",
            window(),
            12,
            "CLAUDE-100",
        );
        assert_eq!(line, "CLAUDE-100: 12 closed, 2026-08-01 .. 2026-08-07 (7d)");
    }

    #[test]
    fn closed_issue_rows_without_an_id_are_dropped() {
        let rows: Vec<serde_json::Value> =
            serde_json::from_str(r#"[{"summary":"no id"},{"idReadable":"LC-1","summary":"ok"}]"#)
                .unwrap();
        let parsed: Vec<ClosedIssue> = rows.iter().filter_map(ClosedIssue::from_row).collect();
        assert_eq!(parsed.len(), 1);
        assert_eq!(parsed[0].key, "LC-1");
    }

    #[test]
    fn project_key_splits_idreadable() {
        assert_eq!(project_key("CLAUDE-218"), "CLAUDE");
        assert_eq!(project_key("A-B-7"), "A-B");
        assert_eq!(project_key("nodash"), "nodash");
    }

    // ---- AC 2: the report is created and linked as a subtask ---------------

    /// The whole filing pass, with `yt` stubbed call-for-call: one
    /// window-scoped query per configured project, `issue create` in the
    /// parent's project carrying the rendered body, `issue link <report>
    /// <parent> --type "subtask of"`, then the marker comment on the parent.
    /// Every call is registered by its exact argv, so an unexpected shape is a
    /// missing-canned-response failure rather than a passing test.
    #[test]
    fn file_report_creates_the_issue_and_links_it_as_a_subtask() {
        let gathered = vec![
            (
                "CLAUDE".to_string(),
                vec![
                    issue("CLAUDE-214", "Author issues", "Claude Run"),
                    issue("CLAUDE-215", "Partition", "Claude Run"),
                ],
            ),
            ("LC".to_string(), Vec::new()),
        ];
        let body = render_report(window(), &gathered, 100);
        let comment = parent_comment("CLAUDE-230", window(), 2, &cfg().projects);
        let runner = Arc::new(
            StubRunner::new()
                .ok(
                    "yt",
                    &["issue", "inspect", "CLAUDE-100", "--json"],
                    r#"{"issue":{"description":"milestone"},"comments":[]}"#,
                )
                .ok(
                    "yt",
                    &["list", &list_args("CLAUDE"), "--json"],
                    &rows(&[("CLAUDE-214", "Author issues"), ("CLAUDE-215", "Partition")]),
                )
                .ok("yt", &["list", &list_args("LC"), "--json"], "[]")
                .ok(
                    "yt",
                    &[
                        "issue",
                        "create",
                        "--project",
                        "CLAUDE",
                        "--summary",
                        "Closed-issue summary: 2026-08-01 .. 2026-08-07",
                        "--description",
                        &body,
                        "--json",
                    ],
                    r#"{"idReadable":"CLAUDE-230"}"#,
                )
                .ok(
                    "yt",
                    &[
                        "issue",
                        "link",
                        "CLAUDE-230",
                        "CLAUDE-100",
                        "--type",
                        "subtask of",
                    ],
                    "",
                )
                .ok("yt", &["issue", "comment", "CLAUDE-100", &comment], ""),
        );
        let backend = CliBackend::new(runner.clone());

        let filed = file_report(&backend, &cfg(), "CLAUDE-100", window()).unwrap();
        assert_eq!(filed, Filing::Filed("CLAUDE-230".to_string()));

        let calls = runner.calls.lock().unwrap().clone();
        assert!(
            calls
                .iter()
                .any(|c| c == "yt issue link CLAUDE-230 CLAUDE-100 --type subtask of"),
            "the report must hang off the nominated parent: {calls:?}"
        );
        assert!(comment.contains("as CLAUDE-230"), "{comment}");
        assert!(
            comment.contains("Report-Window: 2026-08-01..2026-08-07"),
            "{comment}"
        );
        // The report is created in the parent's own project when none is configured.
        assert!(
            calls
                .iter()
                .any(|c| c.contains("issue create --project CLAUDE ")),
            "{calls:?}"
        );
    }

    /// Idempotency: a parent already carrying this window's marker is skipped
    /// with no create, no link and no comment, so a poll interval shorter than
    /// the window files one report per window.
    #[test]
    fn file_report_skips_a_parent_already_marked_for_this_window() {
        let inspect = format!(
            r#"{{"issue":{{"description":"milestone"}},"comments":[{{"text":"filed earlier {}"}}]}}"#,
            window().marker()
        );
        let runner = Arc::new(StubRunner::new().ok(
            "yt",
            &["issue", "inspect", "CLAUDE-100", "--json"],
            &inspect,
        ));
        let backend = CliBackend::new(runner.clone());

        let filed = file_report(&backend, &cfg(), "CLAUDE-100", window()).unwrap();
        assert_eq!(filed, Filing::AlreadyFiled);

        let calls = runner.calls.lock().unwrap();
        assert!(
            !calls.iter().any(|c| c.contains("issue create")),
            "no second report for the same window: {calls:?}"
        );
        assert!(!calls.iter().any(|c| c.contains("issue link")), "{calls:?}");
        assert!(
            !calls.iter().any(|c| c.starts_with("yt issue comment")),
            "{calls:?}"
        );
    }

    /// A parent whose window is unmarked but whose project list is different
    /// still files: the marker is per window, not per parent.
    #[test]
    fn file_report_files_when_the_marked_window_is_a_different_one() {
        let other = Window::ending(day("2026-07-31"), 7);
        let inspect = format!(
            r#"{{"issue":{{"description":"{}"}},"comments":[]}}"#,
            other.marker()
        );
        let runner = Arc::new(
            StubRunner::new()
                .ok(
                    "yt",
                    &["issue", "inspect", "CLAUDE-100", "--json"],
                    &inspect,
                )
                .ok("yt", &["list", &list_args("CLAUDE"), "--json"], "[]")
                .ok("yt", &["list", &list_args("LC"), "--json"], "[]"),
        );
        let backend = CliBackend::new(runner.clone());
        // The create call is unregistered, so this errors AFTER deciding to file.
        // Deciding to file is the assertion; the create shape is covered above.
        assert!(file_report(&backend, &cfg(), "CLAUDE-100", window()).is_err());
        let calls = runner.calls.lock().unwrap();
        assert!(
            calls.iter().any(|c| c.contains("issue create")),
            "a different window's marker must not suppress this window: {calls:?}"
        );
    }

    // ---- AC 3 (negative): no code change, no PR ----------------------------

    /// The reporting run touches no repository. Neither `fj` nor `git` is ever
    /// invoked, so no branch is created, no commit is made, and no pull request
    /// is opened; the only writes are the tracker's.
    #[test]
    fn report_run_makes_no_code_change_and_opens_no_pr() {
        let gathered = vec![
            (
                "CLAUDE".to_string(),
                vec![issue("CLAUDE-214", "Author issues", "Claude Run")],
            ),
            ("LC".to_string(), Vec::new()),
        ];
        let body = render_report(window(), &gathered, 100);
        let comment = parent_comment("CLAUDE-230", window(), 1, &cfg().projects);
        let runner = Arc::new(
            StubRunner::new()
                .ok(
                    "yt",
                    &["issue", "inspect", "CLAUDE-100", "--json"],
                    r#"{"issue":{"description":""},"comments":[]}"#,
                )
                .ok(
                    "yt",
                    &["list", &list_args("CLAUDE"), "--json"],
                    &rows(&[("CLAUDE-214", "Author issues")]),
                )
                .ok("yt", &["list", &list_args("LC"), "--json"], "[]")
                .ok(
                    "yt",
                    &[
                        "issue",
                        "create",
                        "--project",
                        "CLAUDE",
                        "--summary",
                        "Closed-issue summary: 2026-08-01 .. 2026-08-07",
                        "--description",
                        &body,
                        "--json",
                    ],
                    r#"{"idReadable":"CLAUDE-230"}"#,
                )
                .ok(
                    "yt",
                    &[
                        "issue",
                        "link",
                        "CLAUDE-230",
                        "CLAUDE-100",
                        "--type",
                        "subtask of",
                    ],
                    "",
                )
                .ok("yt", &["issue", "comment", "CLAUDE-100", &comment], ""),
        );
        let backend = CliBackend::new(runner.clone());
        file_report(&backend, &cfg(), "CLAUDE-100", window()).unwrap();

        let calls = runner.calls.lock().unwrap();
        assert!(
            calls.iter().all(|c| c.starts_with("yt ")),
            "a report run shells out to the tracker only: {calls:?}"
        );
        assert!(
            !calls.iter().any(|c| c.starts_with("fj ")),
            "no pull request may be opened by a report run: {calls:?}"
        );
        assert!(
            !calls.iter().any(|c| c.starts_with("git ")),
            "no clone, branch or commit may happen in a report run: {calls:?}"
        );
        assert!(
            !calls.iter().any(|c| c.starts_with("claude ")),
            "a report run starts no agent session: {calls:?}"
        );
    }

    /// The module itself carries no code-work call sites, so the "no code
    /// change, no PR" contract cannot regress by someone adding one later.
    #[test]
    fn the_handler_source_has_no_forge_or_git_call_sites() {
        let src = include_str!("closed_issue_summary.rs");
        // Only the non-test half: the assertions below name the forbidden
        // symbols, which would otherwise match themselves.
        let prod = src.split("#[cfg(test)]").next().unwrap();
        for forbidden in ["create_pr", "clone_repo", "ClaudeInvoker", "Command::new"] {
            assert!(
                !prod.contains(forbidden),
                "a report run must never reach `{forbidden}`"
            );
        }
    }

    // ---- target resolution -------------------------------------------------

    #[test]
    fn resolve_targets_unions_nominated_parents_with_the_query() {
        let c = ClosedIssueSummaryConfig {
            parents: vec!["CLAUDE-100".to_string(), " ".to_string()],
            query: "-Resolved AI Agent: Reporting".to_string(),
            ..cfg()
        };
        let runner = Arc::new(StubRunner::new().ok(
            "yt",
            &["list", "--query=-Resolved AI Agent: Reporting", "--json"],
            r#"[{"idReadable":"LC-3"},{"idReadable":"CLAUDE-100"}]"#,
        ));
        let backend = CliBackend::new(runner);
        let (targets, from_query) = resolve_targets(&backend, &c).unwrap();
        assert_eq!(targets, vec!["CLAUDE-100".to_string(), "LC-3".to_string()]);
        assert!(from_query.contains("LC-3"));
        assert!(from_query.contains("CLAUDE-100"));
    }

    /// An empty `query` runs no tracker query at all, so the default config
    /// cannot collide with the worker action's `AI Agent: Queued` selector.
    #[test]
    fn resolve_targets_runs_no_query_when_it_is_empty() {
        let c = ClosedIssueSummaryConfig {
            parents: vec!["CLAUDE-100".to_string()],
            ..cfg()
        };
        let runner = Arc::new(StubRunner::new());
        let backend = CliBackend::new(runner.clone());
        let (targets, from_query) = resolve_targets(&backend, &c).unwrap();
        assert_eq!(targets, vec!["CLAUDE-100".to_string()]);
        assert!(from_query.is_empty());
        assert!(runner.calls.lock().unwrap().is_empty(), "no query is run");
    }

    /// Only a query-selected parent gets its field advanced. A parent nominated
    /// by id has no reporting state machine and is never written to.
    #[test]
    fn advance_writes_only_for_query_selected_parents() {
        let runner = Arc::new(StubRunner::new().ok(
            "yt",
            &["issue", "set-field", "LC-3", "AI Agent", "Done"],
            "",
        ));
        let backend = CliBackend::new(runner.clone());
        let c = cfg();
        let mut from_query = HashSet::new();
        from_query.insert("LC-3".to_string());

        advance(&backend, &c, &from_query, "LC-3", &c.done_label);
        advance(&backend, &c, &from_query, "CLAUDE-100", &c.done_label);

        let calls = runner.calls.lock().unwrap();
        assert_eq!(
            *calls,
            vec!["yt issue set-field LC-3 AI Agent Done".to_string()],
            "the nominated parent must not be field-written"
        );
    }
}
