//! The PR comment posted after every runner push or branch update to a PR
//! branch (CLAUDE-447), so the PR's own timeline explains how the branch
//! reached its current state, not just the title/body `refresh_pr_text`
//! rewrites on a resumed run.
//!
//! The body reuses the CLAUDE-446 run footer ([`crate::run_footer`]). A
//! failed [`crate::provider::Forge::pr_comment`] call never fails, retries,
//! or rolls back the push it is reporting on (writes are never retried,
//! CLAUDE-366): [`post`] logs it at `error` and returns a human-readable
//! cause for the caller to fold into its own run outcome comment, naming the
//! event (`PR comment for <event> failed: <cause>`).

use std::path::Path;

use crate::provider::Forge;

/// The cap on commit subjects listed in a push comment body (proposed
/// approach step 1): beyond this, the remaining count is named instead of
/// listed.
pub const MAX_COMMIT_LINES: usize = 20;

/// Which runner-owned write to a PR branch this comment reports on
/// (CLAUDE-447's enumerated table, plus CLAUDE-448's `CheckpointPush`: one of
/// the periodic WIP pushes a checkpoint watcher makes while the agent is
/// still running, hooks disabled, CLAUDE-448 step 1/2).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PushEvent {
    InitialPush,
    ResumedRunPush,
    TimeoutSalvagePush,
    CommentWebhookFixPush,
    MergedRemoteBranchBeforePush,
    BranchUpdatedFromBase,
    CheckpointPush,
}

impl PushEvent {
    pub fn label(&self) -> &'static str {
        match self {
            PushEvent::InitialPush => "initial push (PR opened)",
            PushEvent::ResumedRunPush => "resumed-run push",
            PushEvent::TimeoutSalvagePush => "timeout salvage push",
            PushEvent::CommentWebhookFixPush => "comment-webhook fix push",
            PushEvent::MergedRemoteBranchBeforePush => "merged remote branch before push",
            PushEvent::BranchUpdatedFromBase => "branch updated from base",
            PushEvent::CheckpointPush => "checkpoint push",
        }
    }
}

/// The commit subjects a push comment lists, newest first. `Unavailable`
/// covers a server-side forge operation with no local clone to run `git log`
/// against (the Forgejo/GitHub "update branch from base" write).
pub enum CommitSubjects {
    Listed(Vec<String>),
    Unavailable,
}

fn format_subjects(subjects: &CommitSubjects) -> String {
    match subjects {
        CommitSubjects::Unavailable => {
            "(commit subjects unavailable: no local clone for this operation)".to_string()
        }
        CommitSubjects::Listed(subjects) if subjects.is_empty() => "(no new commits)".to_string(),
        CommitSubjects::Listed(subjects) => {
            let shown = subjects
                .iter()
                .take(MAX_COMMIT_LINES)
                .map(|s| format!("- {s}"))
                .collect::<Vec<_>>()
                .join("\n");
            if subjects.len() > MAX_COMMIT_LINES {
                format!(
                    "{shown}\n... and {} more",
                    subjects.len() - MAX_COMMIT_LINES
                )
            } else {
                shown
            }
        }
    }
}

fn short(sha: &str) -> &str {
    &sha[..sha.len().min(12)]
}

/// Everything [`build_body`] needs to render one push comment.
pub struct PushCommentContext<'a> {
    pub event: PushEvent,
    pub issue_key: &'a str,
    /// The PR's resulting state: `true` for draft, `false` for ready.
    pub draft: bool,
    /// `None` for a brand-new branch (the proposed approach's `<old-sha> is
    /// `none` for a new branch`).
    pub old_sha: Option<&'a str>,
    /// `None` only when the forge operation gives no new head SHA to report
    /// (a best-effort "branch updated" whose post-update lookup also
    /// failed); rendered as `unknown`.
    pub new_sha: Option<&'a str>,
    pub subjects: CommitSubjects,
    pub public_url: Option<&'a str>,
    pub run_id: Option<i64>,
    pub started: Option<chrono::DateTime<chrono::Utc>>,
}

/// Render the push comment body (event, issue key, PR state, commit range,
/// capped commit subjects, then the CLAUDE-446 run footer).
pub fn build_body(ctx: &PushCommentContext<'_>) -> String {
    let state = if ctx.draft { "draft" } else { "ready" };
    let old = ctx.old_sha.map(short).unwrap_or("none");
    let new = ctx.new_sha.map(short).unwrap_or("unknown");
    let body = format!(
        "{}\n\nIssue: {}\nPR state: {state}\nRange: {old}..{new}\n\n{}",
        ctx.event.label(),
        ctx.issue_key,
        format_subjects(&ctx.subjects),
    );
    crate::run_footer::append_run(&body, ctx.public_url, ctx.run_id, ctx.started)
}

/// Post the push comment through `forge.pr_comment`, body written to a
/// scratch file under `scratch_dir` (the caller's choice of
/// [`crate::provider::cli::CliBackend::git_dir_scratch`]-style git-directory
/// scratch, or a temp directory when no workdir exists) and removed
/// afterward. Returns `None` on success, or the failure line the caller
/// folds into its own outcome comment on error; either way the push itself is
/// already done and is never undone here.
pub fn post(
    forge: &dyn Forge,
    host: &str,
    repo: &str,
    pr_number: u64,
    scratch_dir: &Path,
    ctx: &PushCommentContext<'_>,
) -> Option<String> {
    let body = build_body(ctx);
    let scratch = scratch_dir.join(format!(
        "claude-run-push-comment-{pr_number}-{}.md",
        ctx.event.label().replace([' ', '(', ')'], "_")
    ));
    if let Err(e) = std::fs::write(&scratch, &body) {
        tracing::error!(pr_number, event = ctx.event.label(), error = %e, "writing the push comment scratch file failed");
        return Some(format!(
            "PR comment for {} failed: writing the comment body failed: {e}",
            ctx.event.label()
        ));
    }
    let result = forge.pr_comment(host, repo, pr_number, &scratch);
    if let Err(e) = std::fs::remove_file(&scratch) {
        if e.kind() != std::io::ErrorKind::NotFound {
            tracing::error!(path = %scratch.display(), error = %e, "removing the push comment scratch file failed");
        }
    }
    match result {
        Ok(()) => None,
        Err(e) => {
            tracing::error!(pr_number, event = ctx.event.label(), error = %e, "posting the push comment failed");
            Some(format!("PR comment for {} failed: {e}", ctx.event.label()))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ctx<'a>(event: PushEvent, subjects: CommitSubjects) -> PushCommentContext<'a> {
        PushCommentContext {
            event,
            issue_key: "CLAUDE-447",
            draft: false,
            old_sha: Some("0123456789abcdef"),
            new_sha: Some("fedcba9876543210"),
            subjects,
            public_url: Some("https://claude-run.example.com"),
            run_id: Some(42),
            started: Some(
                chrono::DateTime::parse_from_rfc3339("2026-10-12T18:41:03Z")
                    .unwrap()
                    .with_timezone(&chrono::Utc),
            ),
        }
    }

    #[test]
    fn every_event_label_appears_verbatim_in_its_own_body() {
        let events = [
            PushEvent::InitialPush,
            PushEvent::ResumedRunPush,
            PushEvent::TimeoutSalvagePush,
            PushEvent::CommentWebhookFixPush,
            PushEvent::MergedRemoteBranchBeforePush,
            PushEvent::BranchUpdatedFromBase,
            PushEvent::CheckpointPush,
        ];
        for event in events {
            let body = build_body(&ctx(event, CommitSubjects::Listed(vec!["one".into()])));
            assert!(
                body.starts_with(event.label()),
                "body for {event:?} did not open with its label: {body}"
            );
        }
    }

    #[test]
    fn body_names_issue_key_range_subjects_and_footer() {
        let body = build_body(&ctx(
            PushEvent::InitialPush,
            CommitSubjects::Listed(vec!["Add thing".into(), "Fix other thing".into()]),
        ));
        assert!(body.contains("Issue: CLAUDE-447"));
        assert!(body.contains("PR state: ready"));
        assert!(body.contains("Range: 0123456789ab..fedcba987654"));
        assert!(body.contains("- Add thing"));
        assert!(body.contains("- Fix other thing"));
        assert!(body.contains("Run: https://claude-run.example.com/runs/42"));
        assert!(body.contains("Started: 2026-10-12T18:41:03Z"));
    }

    #[test]
    fn draft_state_is_named() {
        let mut c = ctx(PushEvent::InitialPush, CommitSubjects::Listed(vec![]));
        c.draft = true;
        let body = build_body(&c);
        assert!(body.contains("PR state: draft"));
    }

    #[test]
    fn new_branch_has_no_old_sha() {
        let mut c = ctx(PushEvent::InitialPush, CommitSubjects::Listed(vec![]));
        c.old_sha = None;
        let body = build_body(&c);
        assert!(body.contains("Range: none..fedcba987654"));
    }

    #[test]
    fn no_new_commits_says_so() {
        let body = build_body(&ctx(
            PushEvent::ResumedRunPush,
            CommitSubjects::Listed(vec![]),
        ));
        assert!(body.contains("(no new commits)"));
    }

    #[test]
    fn unavailable_subjects_say_so_without_a_local_clone() {
        let body = build_body(&ctx(
            PushEvent::BranchUpdatedFromBase,
            CommitSubjects::Unavailable,
        ));
        assert!(body.contains("(commit subjects unavailable: no local clone for this operation)"));
    }

    #[test]
    fn more_than_twenty_commit_subjects_cap_with_a_count() {
        let subjects: Vec<String> = (1..=25).map(|i| format!("commit {i}")).collect();
        let body = build_body(&ctx(
            PushEvent::ResumedRunPush,
            CommitSubjects::Listed(subjects),
        ));
        let lines: Vec<&str> = body.lines().filter(|l| l.starts_with("- commit")).collect();
        assert_eq!(lines.len(), MAX_COMMIT_LINES);
        assert!(body.contains("- commit 1"));
        assert!(body.contains("- commit 20"));
        assert!(!body.contains("- commit 21"));
        assert!(body.contains("... and 5 more"));
    }

    #[test]
    fn exactly_twenty_commit_subjects_has_no_more_tail() {
        let subjects: Vec<String> = (1..=20).map(|i| format!("commit {i}")).collect();
        let body = build_body(&ctx(
            PushEvent::ResumedRunPush,
            CommitSubjects::Listed(subjects),
        ));
        assert!(!body.contains("more"));
    }

    struct FailingForge;
    impl Forge for FailingForge {
        fn create_pr(
            &self,
            _workdir: &Path,
            _host: &str,
            _strategy: crate::action::PrStrategy,
        ) -> crate::error::Result<crate::provider::PullRequest> {
            unreachable!()
        }
        fn clone_url(&self, _host: &str, _repo: &str) -> crate::error::Result<String> {
            unreachable!()
        }
        fn repo_access(&self, _host: &str, _repo: &str) -> crate::error::Result<()> {
            unreachable!()
        }
        fn list_open_prs(
            &self,
            _host: &str,
            _repo: &str,
        ) -> crate::error::Result<Vec<crate::provider::ForgePullRequest>> {
            unreachable!()
        }
        fn pr_ci_state(
            &self,
            _host: &str,
            _repo: &str,
            _pr_number: u64,
        ) -> crate::error::Result<String> {
            unreachable!()
        }
        fn issue_comment(
            &self,
            _host: &str,
            _repo: &str,
            _issue_number: u64,
            _body_file: &Path,
        ) -> crate::error::Result<()> {
            unreachable!()
        }
        fn pr_comment(
            &self,
            _host: &str,
            _repo: &str,
            _pr_number: u64,
            _body_file: &Path,
        ) -> crate::error::Result<()> {
            Err(crate::error::Error::Other(anyhow::anyhow!(
                "pr comment boom"
            )))
        }
    }

    #[test]
    fn a_failed_pr_comment_is_reported_but_never_panics() {
        let dir = tempfile::tempdir().unwrap();
        let forge = FailingForge;
        let failure = post(
            &forge,
            "dev.a8n.run",
            "psa-systems/claude-run",
            7,
            dir.path(),
            &ctx(PushEvent::ResumedRunPush, CommitSubjects::Listed(vec![])),
        );
        let failure = failure.expect("a failing pr_comment call must be reported");
        assert!(failure.starts_with("PR comment for resumed-run push failed:"));
        assert!(failure.contains("pr comment boom"));
    }
}
