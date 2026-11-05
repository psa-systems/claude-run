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

/// SPEC-3.9: "a comment about one job's attempt" links the attempt page.
///
/// The job-attempt-specific page to link to, when the comment is about one
/// job's attempt rather than the run as a whole.
pub struct Attempt<'a> {
    pub key: &'a str,
    pub attempt: u32,
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
#[derive(Clone, Copy)]
pub struct CommentRun {
    pub run_id: i64,
    pub attempt: u32,
    pub started: DateTime<Utc>,
    pub source: &'static str,
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
/// [`append`] for a job-lifecycle comment: links the job's own attempt page,
/// with "now" as the finish time. The `source` for the `Source:` line is
/// taken from the `CommentRun` itself (CLAUDE-555).
pub fn append_job(body: &str, public_url: Option<&str>, run: CommentRun, key: &str) -> String {
    append(
        body,
        public_url,
        Some(run.run_id),
        Some(Attempt {
            key,
            attempt: run.attempt,
        }),
        run.started,
        Utc::now(),
        run.source,
    )
}

/// SPEC-3.9: the run-page `Run:` line for a comment about a run as a whole.
///
/// [`append`] for a comment about a run as a whole rather than one job's
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
) -> String {
    let finished = Utc::now();
    append(
        body,
        public_url,
        run_id,
        None,
        started.unwrap_or(finished),
        finished,
        source,
    )
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

    format!(
        "\n\n{run_line}\nStarted: {}\nFinished: {}\nDuration: {}\nSource: {source}",
        started.to_rfc3339_opts(chrono::SecondsFormat::Secs, true),
        finished.to_rfc3339_opts(chrono::SecondsFormat::Secs, true),
        humanize(finished - started),
    )
}

/// Append [`render`]'s footer to a comment body.
pub fn append(
    body: &str,
    public_url: Option<&str>,
    run_id: Option<i64>,
    attempt: Option<Attempt<'_>>,
    started: DateTime<Utc>,
    finished: DateTime<Utc>,
    source: &'static str,
) -> String {
    format!(
        "{body}{}",
        render(public_url, run_id, attempt, started, finished, source)
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
        let finished = ts("2026-10-12T18:41:33Z");
        let body = append(
            "Opened a PR.",
            Some("https://claude-run.example.com"),
            Some(1),
            None,
            started,
            finished,
            "youtrack",
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
        );
        assert!(body.contains("Run: no run id is available for this comment"));
        assert!(body.contains("Source: ci-failure-webhook"));
    }
}
