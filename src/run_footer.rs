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

/// The job-attempt-specific page to link to, when the comment is about one
/// job's attempt rather than the run as a whole.
pub struct Attempt<'a> {
    pub key: &'a str,
    pub attempt: u32,
}

/// Render the footer text (leading blank line included) to append to a
/// comment body.
pub fn render(
    public_url: Option<&str>,
    run_id: Option<i64>,
    attempt: Option<Attempt<'_>>,
    started: DateTime<Utc>,
    finished: DateTime<Utc>,
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
        "\n\n{run_line}\nStarted: {}\nFinished: {}\nDuration: {}",
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
) -> String {
    format!(
        "{body}{}",
        render(public_url, run_id, attempt, started, finished)
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
        );
        assert!(footer
            .contains("Run: https://claude-run.example.com/runs/42/jobs/CLAUDE-446/attempt/1"));
        assert!(footer.contains("Started: 2026-10-12T18:41:03Z"));
        assert!(footer.contains("Finished: 2026-10-12T18:52:05Z"));
        assert!(footer.contains("Duration: 11m 2s"));
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
        );
        assert!(footer.contains("Run: https://claude-run.example.com/runs/7"));
        assert!(!footer.contains("/jobs/"));
        assert!(footer.contains("Duration: 30s"));
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
        );
        assert!(footer.contains("Run: no run id is available for this comment"));
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
        );
        assert!(body.starts_with("Opened a PR.\n\n"));
        assert!(body.contains("Run: https://claude-run.example.com/runs/1"));
    }
}
