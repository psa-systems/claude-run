//! Implements SPEC-3.9.
//!
//! The footer every runner-posted YouTrack comment carries (CLAUDE-446): the
//! run's start time, end time, duration, and an absolute link back to it, so
//! a reader goes from the issue to the run in one click.
//!
//! `public_url` comes from [`crate::config::public_url`], resolved
//! independently of email/SMTP configuration so a footer link works even
//! when SMTP is unset or its own configuration is broken. `run_id` is `None`
//! only when the comment is posted outside any run's lifecycle (for example
//! a daemon-restart reconcile) or the run row itself failed to open; the
//! `Run:` line then says so explicitly rather than being omitted.

use chrono::{DateTime, Utc};
use std::time::Duration;

/// SPEC-3.9: "a comment about one job's attempt" links the attempt page.
///
/// The job-attempt-specific page to link to, when the comment is about one
/// job's attempt rather than the run as a whole.
pub struct Attempt<'a> {
    pub key: &'a str,
    pub attempt: u32,
}

/// What caused the run that produced a comment (CLAUDE-556), printed as the
/// footer's `Trigger:` line right after `Source:`.
///
/// Variants carry the human-readable origin so a reader sees why the work
/// started without opening the run page. A comment whose run has no known
/// trigger passes `None` and the line is omitted.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Trigger {
    /// The scheduler's periodic tick.
    Scheduled,
    /// A webhook delivery; `source` is the readable origin, e.g. "CI failure on PR 42".
    WebhookEvent { source: String },
    /// A `/claude` command; `login` is the asker.
    UserCommand { login: String },
    /// An operator-initiated run (`claude-run trigger` or `run`); `by` is the channel.
    Manual { by: String },
    /// The `youtrack` action picking up an issue whose `AI Agent` field is `Queued`.
    AiAgentPickup,
}

impl Trigger {
    /// The one-line description printed after `Trigger: `.
    pub fn render(&self) -> String {
        match self {
            Trigger::Scheduled => "scheduled poll".to_string(),
            Trigger::WebhookEvent { source } => format!("webhook event ({source})"),
            Trigger::UserCommand { login } => format!("/claude command from {login}"),
            Trigger::Manual { by } => format!("manual run ({by})"),
            Trigger::AiAgentPickup => "AI Agent pickup".to_string(),
        }
    }

    /// Map a run's `triggered_by` origin to a trigger: the scheduler's periodic
    /// pickup becomes `scheduled`, any other origin is an operator-initiated run.
    pub fn from_run_origin(triggered_by: &str, scheduled: Trigger) -> Trigger {
        if triggered_by == "scheduler" {
            scheduled
        } else {
            Trigger::Manual {
                by: triggered_by.to_string(),
            }
        }
    }
}

/// Which run and attempt a job-lifecycle comment belongs to, and when that
/// attempt started: the context a finalize-path comment needs to build its
/// footer, threaded in by the caller that already holds `run_id` and
/// `Instant::now()`.
///
/// `source` (CLAUDE-555) names the action that produced the comment, so a
/// reader scanning an issue knows which path (e.g. `youtrack`,
/// `closed-issue-summary`, a webhook listener) wrote it without opening the
/// run page. The field is non-defaulting on purpose: a new call site that
/// forgets to set it fails to compile.
///
/// `trigger` (CLAUDE-556) is optional: `None` omits the `Trigger:` line.
/// It borrows so the struct stays `Copy`.
#[derive(Clone, Copy)]
pub struct CommentRun<'a> {
    pub run_id: i64,
    pub attempt: u32,
    pub started: DateTime<Utc>,
    pub source: &'static str,
    pub trigger: Option<&'a Trigger>,
}

/// SPEC-3.9: "`public_url` is resolved independently of SMTP configuration."
///
/// `crate::config::public_url()`, defaulting to `None` on a malformed
/// `CLAUDE_RUN_PUBLIC_URL` rather than letting a comment-posting path fail on
/// a problem the daemon already logged once at startup.
pub fn resolved_public_url() -> Option<String> {
    crate::config::public_url().unwrap_or_default()
}

/// SPEC-3.9: the attempt-page `Run:` line for a job-lifecycle comment.
///
/// [`render`] appended for a job-lifecycle comment: links the job's own attempt page,
/// with "now" as the finish time. The `source` for the `Source:` line is
/// taken from the `CommentRun` itself (CLAUDE-555).
pub fn append_job(body: &str, public_url: Option<&str>, run: CommentRun<'_>, key: &str) -> String {
    let footer = render(
        public_url,
        Some(run.run_id),
        Some(Attempt {
            key,
            attempt: run.attempt,
        }),
        run.started,
        Utc::now(),
        run.source,
        run.trigger,
    );
    format!("{body}{footer}")
}

/// SPEC-3.9: the run-page `Run:` line for a comment about a run as a whole.
///
/// [`render`] appended for a comment about a run as a whole rather than one job's
/// attempt (webhook runs, the closed-issue summary, the dependency gate, the
/// attempt-cap comment): links `/runs/<id>`, with "now" as both the start
/// and finish time when no better start time is available. `source`
/// (CLAUDE-555) names the action that produced the comment.
pub fn append_run(
    body: &str,
    public_url: Option<&str>,
    run_id: Option<i64>,
    started: Option<DateTime<Utc>>,
    source: &'static str,
    trigger: Option<&Trigger>,
) -> String {
    let finished = Utc::now();
    let footer = render(
        public_url,
        run_id,
        None,
        started.unwrap_or(finished),
        finished,
        source,
        trigger,
    );
    format!("{body}{footer}")
}

/// SPEC-3.9: full footer text, all three link/no-link/no-run-id cases, the
/// `Started:` / `Finished:` / `Duration:` lines, and the `Source:` line
/// (CLAUDE-555) naming the action that produced the comment.
///
/// Render the footer text (leading blank line included) to append to a
/// comment body.
pub fn render(
    public_url: Option<&str>,
    run_id: Option<i64>,
    attempt: Option<Attempt<'_>>,
    started: DateTime<Utc>,
    finished: DateTime<Utc>,
    source: &'static str,
    trigger: Option<&Trigger>,
) -> String {
    let run_line = match (public_url, run_id) {
        (Some(base), Some(id)) => {
            let base = base.trim_end_matches('/');
            match attempt {
                Some(a) => format!("Run: {base}/runs/{id}/jobs/{}/attempt/{}", a.key, a.attempt),
                None => format!("Run: {base}/runs/{id}"),
            }
        }
        (None, Some(id)) => match attempt {
            Some(a) => format!(
                "Run: link unavailable (CLAUDE_RUN_PUBLIC_URL is not set); run {id}, attempt {}",
                a.attempt
            ),
            None => {
                format!("Run: link unavailable (CLAUDE_RUN_PUBLIC_URL is not set); run {id}")
            }
        },
        (_, None) => "Run: no run id is available for this comment".to_string(),
    };

    let trigger_line = trigger
        .map(|t| format!("\nTrigger: {}", t.render()))
        .unwrap_or_default();

    format!(
        "\n\n{run_line}\nStarted: {}\nFinished: {}\nDuration: {}\nSource: {source}{trigger_line}",
        started.to_rfc3339_opts(chrono::SecondsFormat::Secs, true),
        finished.to_rfc3339_opts(chrono::SecondsFormat::Secs, true),
        humanize(finished - started),
    )
}

/// SPEC-3.16.3: "The cause is one line from a closed set." One variant per
/// member of that set; an exhaustive match with no wildcard arm means a new
/// variant fails to compile here until [`FailureCause::render`] names it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum FailureCause {
    /// The run hit its wall-clock session limit. `turns` is the number of
    /// agent turns completed before the kill.
    TimedOut { limit: Duration, turns: u32 },
    /// A push was rejected, typically by a pre-push hook; `error` is the
    /// one-line git error.
    PushRejected { error: String },
    /// The verification gate or a hook command itself failed; `command`
    /// names it.
    CommandFailed { command: String },
    /// The run was cancelled (an operator `claude-run cancel`, or a kill
    /// delivered before any other terminal state).
    Cancelled,
    /// Any other agent-side failure; `message` is a one-line, already
    /// redacted summary (never the raw stream-log path).
    AgentError { message: String },
}

impl FailureCause {
    /// SPEC-3.16.3: the one-line cause text, first bullet of
    /// [`failure_comment`].
    fn render(&self) -> String {
        match self {
            FailureCause::TimedOut { limit, turns } => {
                format!("timed out (limit {}s, {turns} turn(s))", limit.as_secs())
            }
            FailureCause::PushRejected { error } => format!("push rejected: {error}"),
            FailureCause::CommandFailed { command } => format!("`{command}` failed"),
            FailureCause::Cancelled => "cancelled".to_string(),
            FailureCause::AgentError { message } => format!("agent error: {message}"),
        }
    }
}

/// SPEC-3.16.3: a `FAILED` comment body, at most 3 bullets: the cause first,
/// then what was or was not pushed, then the run link. The run link is the
/// third bullet SPEC-3.16.3 names; every caller already appends the SPEC-3.9
/// footer (which carries its own `Run:` line), so this body holds only the
/// two bullets it owns and relies on that footer for the third. The raw
/// stream-log path never appears here, since the run link reaches it.
pub fn failure_comment(cause: &FailureCause, pushed: &str) -> String {
    format!(
        "**Claude AI agent run FAILED.**\n\n- {}\n- {pushed}",
        cause.render()
    )
}

fn humanize(d: chrono::Duration) -> String {
    let total_secs = d.num_seconds().max(0);
    let mins = total_secs / 60;
    let secs = total_secs % 60;
    if mins > 0 {
        format!("{mins}m {secs}s")
    } else {
        format!("{secs}s")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ts(s: &str) -> DateTime<Utc> {
        DateTime::parse_from_rfc3339(s).unwrap().with_timezone(&Utc)
    }

    #[test]
    fn footer_with_url_run_and_attempt_links_the_attempt_page() {
        let started = ts("2026-10-12T18:41:03Z");
        let finished = ts("2026-10-12T18:52:05Z");
        let footer = render(
            Some("https://claude-run.example.com/"),
            Some(42),
            Some(Attempt {
                key: "CLAUDE-446",
                attempt: 1,
            }),
            started,
            finished,
            "youtrack",
            None,
        );
        assert!(footer
            .contains("Run: https://claude-run.example.com/runs/42/jobs/CLAUDE-446/attempt/1"));
        assert!(footer.contains("Started: 2026-10-12T18:41:03Z"));
        assert!(footer.contains("Finished: 2026-10-12T18:52:05Z"));
        assert!(footer.contains("Duration: 11m 2s"));
        assert!(footer.contains("Source: youtrack"));
    }

    #[test]
    fn footer_with_url_run_and_no_attempt_links_the_run_page() {
        let started = ts("2026-10-12T18:41:03Z");
        let finished = ts("2026-10-12T18:41:33Z");
        let footer = render(
            Some("https://claude-run.example.com"),
            Some(7),
            None,
            started,
            finished,
            "closed-issue-summary",
            None,
        );
        assert!(footer.contains("Run: https://claude-run.example.com/runs/7"));
        assert!(!footer.contains("/jobs/"));
        assert!(footer.contains("Duration: 30s"));
        assert!(footer.contains("Source: closed-issue-summary"));
    }

    #[test]
    fn footer_with_no_public_url_names_the_missing_variable() {
        let started = ts("2026-10-12T18:41:03Z");
        let finished = ts("2026-10-12T18:41:33Z");
        let footer = render(
            None,
            Some(7),
            Some(Attempt {
                key: "CLAUDE-446",
                attempt: 2,
            }),
            started,
            finished,
            "youtrack",
            None,
        );
        assert!(footer
            .contains("link unavailable (CLAUDE_RUN_PUBLIC_URL is not set); run 7, attempt 2"));
    }

    #[test]
    fn footer_with_no_run_id_says_so_explicitly() {
        let started = ts("2026-10-12T18:41:03Z");
        let finished = ts("2026-10-12T18:41:33Z");
        let footer = render(
            Some("https://claude-run.example.com"),
            None,
            None,
            started,
            finished,
            "youtrack",
            None,
        );
        assert!(footer.contains("Run: no run id is available for this comment"));
    }

    /// CLAUDE-555: every footer carries a `Source:` line naming the action
    /// that produced the comment, so a reader scanning an issue can tell
    /// which code path wrote it without opening the run page.
    #[test]
    fn footer_names_source_action() {
        let started = ts("2026-10-12T18:41:03Z");
        let finished = ts("2026-10-12T18:41:33Z");
        for source in [
            "youtrack",
            "closed-issue-summary",
            "ci-failure-webhook",
            "comment-webhook",
            "automerge-reconciler",
            "push-comment",
        ] {
            let footer = render(
                Some("https://claude-run.example.com"),
                Some(1),
                None,
                started,
                finished,
                source,
                None,
            );
            assert!(
                footer.contains(&format!("Source: {source}")),
                "{source}: footer must name the source action:\n{footer}"
            );
        }
    }

    #[test]
    fn append_puts_the_footer_after_the_body() {
        let started = ts("2026-10-12T18:41:03Z");
        let body = append_run(
            "Opened a PR.",
            Some("https://claude-run.example.com"),
            Some(1),
            Some(started),
            "youtrack",
            None,
        );
        assert!(body.starts_with("Opened a PR.\n\n"));
        assert!(body.contains("Run: https://claude-run.example.com/runs/1"));
        assert!(body.contains("Source: youtrack"));
    }

    #[test]
    fn append_job_links_the_job_attempt_page() {
        let run = CommentRun {
            run_id: 9,
            attempt: 3,
            started: ts("2026-10-12T18:41:03Z"),
            source: "youtrack",
            trigger: None,
        };
        let body = append_job(
            "Opened a PR.",
            Some("https://claude-run.example.com"),
            run,
            "CLAUDE-446",
        );
        assert!(
            body.contains("Run: https://claude-run.example.com/runs/9/jobs/CLAUDE-446/attempt/3")
        );
        assert!(body.contains("Source: youtrack"));
    }

    #[test]
    fn append_run_links_the_run_page_with_no_job_attempt() {
        let body = append_run(
            "Filed a report.",
            Some("https://claude-run.example.com"),
            Some(5),
            Some(ts("2026-10-12T18:41:03Z")),
            "closed-issue-summary",
            None,
        );
        assert!(body.contains("Run: https://claude-run.example.com/runs/5"));
        assert!(!body.contains("/jobs/"));
        assert!(body.contains("Source: closed-issue-summary"));
    }

    #[test]
    fn append_run_with_no_run_id_says_so_explicitly() {
        let body = append_run(
            "Filed a report.",
            Some("https://claude-run.example.com"),
            None,
            None,
            "ci-failure-webhook",
            None,
        );
        assert!(body.contains("Run: no run id is available for this comment"));
        assert!(body.contains("Source: ci-failure-webhook"));
    }

    fn footer_with(trigger: Option<&Trigger>) -> String {
        render(
            Some("https://claude-run.example.com"),
            Some(1),
            None,
            ts("2026-10-12T18:41:03Z"),
            ts("2026-10-12T18:41:33Z"),
            "youtrack",
            trigger,
        )
    }

    /// CLAUDE-556: each trigger variant prints its own `Trigger:` line right after `Source:`.
    #[test]
    fn footer_renders_each_trigger_variant_after_source() {
        let cases = [
            (Trigger::Scheduled, "Trigger: scheduled poll"),
            (
                Trigger::WebhookEvent {
                    source: "CI failure on PR 42".to_string(),
                },
                "Trigger: webhook event (CI failure on PR 42)",
            ),
            (
                Trigger::UserCommand {
                    login: "alice".to_string(),
                },
                "Trigger: /claude command from alice",
            ),
            (
                Trigger::Manual {
                    by: "trigger".to_string(),
                },
                "Trigger: manual run (trigger)",
            ),
            (Trigger::AiAgentPickup, "Trigger: AI Agent pickup"),
        ];
        for (trigger, line) in cases {
            let footer = footer_with(Some(&trigger));
            assert!(
                footer.ends_with(&format!("Source: youtrack\n{line}")),
                "{trigger:?}: Trigger line must follow Source:\n{footer}"
            );
        }
    }

    #[test]
    fn footer_without_a_trigger_omits_the_line() {
        let footer = footer_with(None);
        assert!(!footer.contains("Trigger:"), "{footer}");
        assert!(footer.ends_with("Source: youtrack"), "{footer}");
    }

    #[test]
    fn append_job_prints_the_run_trigger() {
        let trigger = Trigger::AiAgentPickup;
        let run = CommentRun {
            run_id: 9,
            attempt: 1,
            started: ts("2026-10-12T18:41:03Z"),
            source: "youtrack",
            trigger: Some(&trigger),
        };
        let body = append_job("Opened a PR.", None, run, "CLAUDE-556");
        assert!(body.contains("\nTrigger: AI Agent pickup"), "{body}");
    }

    #[test]
    fn append_run_prints_the_trigger() {
        let trigger = Trigger::Scheduled;
        let body = append_run(
            "Filed a report.",
            None,
            Some(5),
            None,
            "closed-issue-summary",
            Some(&trigger),
        );
        assert!(body.contains("\nTrigger: scheduled poll"), "{body}");
    }

    /// SPEC-3.16.3: one case per [`FailureCause`] variant, asserting the
    /// rendered body is at most 3 bullets (2 here plus the footer's run
    /// link), the cause is the first bullet, and no stream-log path ever
    /// appears.
    #[test]
    fn failure_comment_per_cause_is_capped_and_leads_with_the_cause() {
        let cases: &[(FailureCause, &str)] = &[
            (
                FailureCause::TimedOut {
                    limit: Duration::from_secs(1800),
                    turns: 42,
                },
                "- timed out (limit 1800s, 42 turn(s))",
            ),
            (
                FailureCause::PushRejected {
                    error: "! [remote rejected] (pre-push hook declined)".to_string(),
                },
                "- push rejected: ! [remote rejected] (pre-push hook declined)",
            ),
            (
                FailureCause::CommandFailed {
                    command: "pre".to_string(),
                },
                "- `pre` failed",
            ),
            (FailureCause::Cancelled, "- cancelled"),
            (
                FailureCause::AgentError {
                    message: "API Error: 529 Overloaded.".to_string(),
                },
                "- agent error: API Error: 529 Overloaded.",
            ),
        ];
        for (cause, first_bullet) in cases {
            let body = failure_comment(cause, "Nothing was pushed.");
            let bullets: Vec<&str> = body.lines().filter(|l| l.starts_with("- ")).collect();
            assert!(bullets.len() <= 3, "{cause:?}: at most 3 bullets:\n{body}");
            assert_eq!(
                bullets.first().copied(),
                Some(*first_bullet),
                "{cause:?}: the cause must be the first bullet:\n{body}"
            );
            assert!(
                !body.contains(".jsonl") && !body.contains("/streams/"),
                "{cause:?}: no raw stream-log path:\n{body}"
            );
            assert!(
                body.starts_with("**Claude AI agent run FAILED.**"),
                "{body}"
            );
        }
    }

    #[test]
    fn from_run_origin_maps_scheduler_to_the_given_trigger_and_others_to_manual() {
        assert_eq!(
            Trigger::from_run_origin("scheduler", Trigger::AiAgentPickup),
            Trigger::AiAgentPickup
        );
        assert_eq!(
            Trigger::from_run_origin("trigger", Trigger::Scheduled),
            Trigger::Manual {
                by: "trigger".to_string()
            }
        );
    }
}
