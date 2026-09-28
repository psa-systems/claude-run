//! The per-run bound and the audit trail on the YouTrack issue writes a run
//! performs.
//!
//! Two guardrails, both at the provider boundary rather than in a prompt (a
//! prompt is guidance; these have to hold when the model ignores guidance,
//! which is the case they exist for):
//!
//! - **A per-run issue-creation cap.** [`IssueWriteAudit`] carries the run's
//!   ceiling and its spend. Reaching it refuses further creates with
//!   [`Error::IssueCapReached`], logs at `warn`, and records a notice the caller
//!   persists as a `run_event`, so a truncated filing run never reads as clean.
//! - **Write attribution.** Every create and every update is logged with the
//!   originating run id and the issue key it wrote, so an unexpected issue
//!   resolves to a run in one query.
//!
//! [`AuditedTracker`] is the enforcement point: it wraps any [`IssueTracker`]
//! (the `yt` [`super::CliBackend`] in production, a stub in tests) and applies
//! both. Wrapping per run is what makes the ledger correct even for a
//! daemon-lifetime backend shared by every webhook delivery: each run gets its
//! own [`IssueWriteAudit`], never one shared counter.
//!
//! Deletes stay denied by omission: [`IssueTracker`] exposes no delete, so no
//! path through this module can issue one. `no_delete_on_the_tracker_surface`
//! below is the guard that fails if one reappears.

use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex};

use tracing::{info, warn};

use super::{IssueInspect, IssueLink, IssueTracker, VcsMapEntry, VcsRepo};
use crate::error::{Error, Result};

/// Issues one run may create when no action overrides it. Above the audit
/// prompts' instructed 20-per-target figure, so the enforced ceiling backstops
/// the instructed one instead of fighting it.
pub const DEFAULT_MAX_ISSUES_PER_RUN: u32 = 25;

/// One run's issue-write ledger: the run its writes are attributed to, the
/// ceiling on creates, what the run has spent, and the operator-visible notices
/// the ceiling produced.
///
/// Held behind an `Arc` so several backends serving ONE run (the youtrack action
/// builds one per job) count against a single ceiling.
#[derive(Debug)]
pub struct IssueWriteAudit {
    /// Run the writes belong to. `None` outside a run (a CLI probe), where the
    /// log line says so rather than inventing an id.
    run_id: Option<i64>,
    max_issues_per_run: u32,
    created: AtomicU32,
    refused: AtomicU32,
    notices: Mutex<Vec<String>>,
}

impl IssueWriteAudit {
    /// Ledger for `run_id` (`None` outside a run), capped at `max_issues_per_run`.
    pub fn new(run_id: Option<i64>, max_issues_per_run: u32) -> Arc<Self> {
        Arc::new(Self {
            run_id,
            max_issues_per_run,
            created: AtomicU32::new(0),
            refused: AtomicU32::new(0),
            notices: Mutex::new(Vec::new()),
        })
    }

    /// The run these writes are attributed to.
    pub fn run_id(&self) -> Option<i64> {
        self.run_id
    }

    /// The run's issue-creation ceiling.
    pub fn max_issues_per_run(&self) -> u32 {
        self.max_issues_per_run
    }

    /// Issues this run has created through the audited tracker.
    pub fn created(&self) -> u32 {
        self.created.load(Ordering::SeqCst)
    }

    /// Creates this run refused because the ceiling was reached.
    pub fn refused(&self) -> u32 {
        self.refused.load(Ordering::SeqCst)
    }

    /// Drain the operator-visible notices the ceiling produced. The caller
    /// persists them as `run_events` so a refusal shows on the run, not only in
    /// the daemon journal.
    pub fn take_notices(&self) -> Vec<String> {
        std::mem::take(&mut *self.notices.lock().unwrap())
    }

    /// Render the run for a log field / an error message.
    fn run_label(&self) -> String {
        match self.run_id {
            Some(id) => id.to_string(),
            None => "none".to_string(),
        }
    }

    /// Claim one create slot, returning this create's 1-based ordinal within the
    /// run. Errors [`Error::IssueCapReached`] when the run is already at its
    /// ceiling; the claim is taken BEFORE the CLI is invoked, so a refusal
    /// creates nothing.
    ///
    /// A claim is never released. A `yt issue create` that fails may still have
    /// created the issue, so a spent slot stays spent: the cap errs toward
    /// creating fewer issues than the ceiling, never more.
    fn claim_create(&self, project: &str) -> Result<u32> {
        let claimed = self
            .created
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |n| {
                (n < self.max_issues_per_run).then_some(n + 1)
            });
        match claimed {
            Ok(prev) => Ok(prev + 1),
            Err(spent) => {
                self.refused.fetch_add(1, Ordering::SeqCst);
                let run = self.run_label();
                warn!(
                    run_id = %run,
                    project,
                    created = spent,
                    cap = self.max_issues_per_run,
                    "per-run issue-creation cap reached; refusing to create another issue"
                );
                self.notices.lock().unwrap().push(format!(
                    "per-run issue-creation cap reached: {spent} issues created, cap {}. Refused \
                     to create another issue in `{project}`. Raise `limits.max_issues_per_run` if \
                     this run legitimately needs more.",
                    self.max_issues_per_run
                ));
                Err(Error::IssueCapReached {
                    run,
                    cap: self.max_issues_per_run,
                    project: project.to_string(),
                })
            }
        }
    }

    /// The audit line for one write: which operation, which issue, which run,
    /// and whether the write landed. A failed write is recorded too (it was
    /// still attempted), marked `ok = false` so it never reads as a change.
    fn record(&self, op: &str, key: &str, ok: bool) {
        info!(
            run_id = %self.run_label(),
            issue = %key,
            op,
            ok,
            "youtrack issue write"
        );
    }
}

/// An [`IssueTracker`] bounded and attributed by an [`IssueWriteAudit`].
/// Reads pass straight through; creates are capped and logged,
/// updates are logged.
pub struct AuditedTracker<'a> {
    inner: &'a dyn IssueTracker,
    audit: Arc<IssueWriteAudit>,
}

impl<'a> AuditedTracker<'a> {
    /// Wrap `inner` for one run.
    pub fn new(inner: &'a dyn IssueTracker, audit: Arc<IssueWriteAudit>) -> Self {
        Self { inner, audit }
    }

    /// The run's ledger, for the caller's notice drain and cap reporting.
    pub fn audit(&self) -> &Arc<IssueWriteAudit> {
        &self.audit
    }
}

impl IssueTracker for AuditedTracker<'_> {
    fn list_queued(&self, query: &str) -> Result<Vec<serde_json::Value>> {
        self.inner.list_queued(query)
    }

    fn get_field(&self, key: &str, field: &str) -> Result<String> {
        self.inner.get_field(key, field)
    }

    fn set_field(&self, key: &str, field: &str, value: &str) -> Result<()> {
        let out = self.inner.set_field(key, field, value);
        self.audit.record("set_field", key, out.is_ok());
        out
    }

    fn set_state(&self, key: &str, state: &str) -> Result<()> {
        let out = self.inner.set_state(key, state);
        self.audit.record("set_state", key, out.is_ok());
        out
    }

    fn set_assignee(&self, key: &str, login: &str) -> Result<()> {
        let out = self.inner.set_assignee(key, login);
        self.audit.record("set_assignee", key, out.is_ok());
        out
    }

    fn comment(&self, key: &str, body: &str) -> Result<()> {
        let out = self.inner.comment(key, body);
        self.audit.record("comment", key, out.is_ok());
        out
    }

    fn issue_links(&self, key: &str) -> Result<Vec<IssueLink>> {
        self.inner.issue_links(key)
    }

    fn inspect_issue(&self, key: &str) -> Result<IssueInspect> {
        self.inner.inspect_issue(key)
    }

    fn resolve_vcs(&self, project_key: &str) -> Result<VcsRepo> {
        self.inner.resolve_vcs(project_key)
    }

    fn vcs_repo_map(&self) -> Result<Vec<VcsMapEntry>> {
        self.inner.vcs_repo_map()
    }

    fn create_issue(
        &self,
        project: &str,
        summary: &str,
        description: &str,
        assignee: Option<&str>,
    ) -> Result<String> {
        // Claim first: a refusal must never reach `yt issue create`.
        self.audit.claim_create(project)?;
        let key = self
            .inner
            .create_issue(project, summary, description, assignee)?;
        self.audit.record("create_issue", &key, true);
        Ok(key)
    }

    fn attach_file(&self, key: &str, file: &std::path::Path) -> Result<()> {
        let out = self.inner.attach_file(key, file);
        self.audit.record("attach_file", key, out.is_ok());
        out
    }

    fn link_issue(&self, key: &str, link_type: &str, target: &str) -> Result<()> {
        let out = self.inner.link_issue(key, link_type, target);
        self.audit.record("link_issue", key, out.is_ok());
        out
    }

    fn auth_verify(&self) -> Result<String> {
        self.inner.auth_verify()
    }

    fn list_query_top(&self, query: &str, top: u32) -> Result<Vec<serde_json::Value>> {
        self.inner.list_query_top(query, top)
    }

    fn project_show(&self, project_key: &str) -> Result<String> {
        self.inner.project_show(project_key)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex as StdMutex;

    /// Records the writes it is asked to perform; every read is unused here.
    #[derive(Default)]
    struct StubTracker {
        calls: StdMutex<Vec<String>>,
        next: AtomicU32,
    }

    impl StubTracker {
        fn calls(&self) -> Vec<String> {
            self.calls.lock().unwrap().clone()
        }
    }

    impl IssueTracker for StubTracker {
        fn list_queued(&self, _query: &str) -> Result<Vec<serde_json::Value>> {
            Ok(Vec::new())
        }
        fn get_field(&self, _key: &str, _field: &str) -> Result<String> {
            Ok(String::new())
        }
        fn set_field(&self, key: &str, field: &str, value: &str) -> Result<()> {
            self.calls
                .lock()
                .unwrap()
                .push(format!("set_field {key} {field}={value}"));
            Ok(())
        }
        fn set_state(&self, key: &str, state: &str) -> Result<()> {
            self.calls
                .lock()
                .unwrap()
                .push(format!("set_state {key} {state}"));
            Ok(())
        }
        fn set_assignee(&self, _key: &str, _login: &str) -> Result<()> {
            Ok(())
        }
        fn comment(&self, key: &str, _body: &str) -> Result<()> {
            self.calls.lock().unwrap().push(format!("comment {key}"));
            Ok(())
        }
        fn issue_links(&self, _key: &str) -> Result<Vec<IssueLink>> {
            Ok(Vec::new())
        }
        fn inspect_issue(&self, _key: &str) -> Result<IssueInspect> {
            Ok(IssueInspect::default())
        }
        fn resolve_vcs(&self, _project_key: &str) -> Result<VcsRepo> {
            Err(Error::NoVcsIntegration("stub".to_string()))
        }
        fn vcs_repo_map(&self) -> Result<Vec<VcsMapEntry>> {
            Ok(Vec::new())
        }
        fn create_issue(
            &self,
            project: &str,
            _summary: &str,
            _description: &str,
            _assignee: Option<&str>,
        ) -> Result<String> {
            let n = self.next.fetch_add(1, Ordering::SeqCst) + 1;
            let key = format!("{project}-{n}");
            self.calls.lock().unwrap().push(format!("create {key}"));
            Ok(key)
        }
        fn attach_file(&self, _key: &str, _file: &std::path::Path) -> Result<()> {
            Ok(())
        }
    }

    #[test]
    fn last_allowed_create_succeeds_and_the_next_is_refused() {
        // The cap boundary: with a ceiling of 2, creates 1 and 2 land and the
        // third is refused BEFORE it reaches the tracker.
        let stub = StubTracker::default();
        let audit = IssueWriteAudit::new(Some(7), 2);
        let tracker = AuditedTracker::new(&stub, audit.clone());

        assert_eq!(
            tracker.create_issue("CLAUDE", "a", "", None).unwrap(),
            "CLAUDE-1"
        );
        assert_eq!(
            tracker.create_issue("CLAUDE", "b", "", None).unwrap(),
            "CLAUDE-2"
        );
        let err = tracker.create_issue("CLAUDE", "c", "", None).unwrap_err();

        assert!(
            matches!(&err, Error::IssueCapReached { run, cap, project }
                if run == "7" && *cap == 2 && project == "CLAUDE"),
            "got {err:?}"
        );
        assert_eq!(
            stub.calls(),
            vec!["create CLAUDE-1", "create CLAUDE-2"],
            "the refused create must not reach the tracker"
        );
        assert_eq!(audit.created(), 2);
    }

    #[test]
    fn a_refused_create_is_recorded_not_swallowed() {
        // The refusal is counted AND leaves a notice the caller persists as a
        // run event, so a truncated filing run cannot read as a clean one.
        let stub = StubTracker::default();
        let audit = IssueWriteAudit::new(Some(9), 1);
        let tracker = AuditedTracker::new(&stub, audit.clone());

        tracker.create_issue("CLAUDE", "a", "", None).unwrap();
        tracker.create_issue("CLAUDE", "b", "", None).unwrap_err();

        assert_eq!(audit.refused(), 1);
        let notices = audit.take_notices();
        assert_eq!(notices.len(), 1, "{notices:?}");
        assert!(notices[0].contains("cap reached"), "{}", notices[0]);
        assert!(notices[0].contains("CLAUDE"), "{}", notices[0]);
        assert!(
            notices[0].contains("max_issues_per_run"),
            "the notice names the knob that raises the cap: {}",
            notices[0]
        );
        assert!(audit.take_notices().is_empty(), "notices drain once");
    }

    #[test]
    fn the_cap_is_per_run_not_per_backend() {
        // Two trackers sharing ONE run's ledger (the youtrack action builds a
        // backend per job) spend one ceiling between them.
        let a = StubTracker::default();
        let b = StubTracker::default();
        let audit = IssueWriteAudit::new(Some(1), 2);
        let first = AuditedTracker::new(&a, audit.clone());
        let second = AuditedTracker::new(&b, audit.clone());

        first.create_issue("CLAUDE", "a", "", None).unwrap();
        second.create_issue("CLAUDE", "b", "", None).unwrap();
        second.create_issue("CLAUDE", "c", "", None).unwrap_err();

        assert_eq!(audit.created(), 2);
        assert_eq!(b.calls().len(), 1);
    }

    #[test]
    fn updates_pass_through_and_are_attributed() {
        // Updates are never capped, only logged; they must reach the tracker
        // unchanged.
        let stub = StubTracker::default();
        let audit = IssueWriteAudit::new(Some(3), 0);
        let tracker = AuditedTracker::new(&stub, audit.clone());

        tracker.set_state("CLAUDE-1", "In Progress").unwrap();
        tracker.set_field("CLAUDE-1", "AI Agent", "Queued").unwrap();
        tracker.comment("CLAUDE-1", "hi").unwrap();

        assert_eq!(
            stub.calls(),
            vec![
                "set_state CLAUDE-1 In Progress",
                "set_field CLAUDE-1 AI Agent=Queued",
                "comment CLAUDE-1",
            ]
        );
        assert_eq!(audit.run_id(), Some(3));
    }

    #[test]
    fn a_zero_cap_refuses_the_first_create() {
        // `max_issues_per_run: 0` is a kill switch for issue creation, not an
        // "unlimited" sentinel.
        let stub = StubTracker::default();
        let audit = IssueWriteAudit::new(None, 0);
        let tracker = AuditedTracker::new(&stub, audit.clone());

        let err = tracker.create_issue("CLAUDE", "a", "", None).unwrap_err();
        assert!(
            matches!(&err, Error::IssueCapReached { run, .. } if run == "none"),
            "got {err:?}"
        );
        assert!(stub.calls().is_empty());
    }

    #[test]
    fn the_cap_stops_the_yt_cli_itself() {
        // The enforcement is at the provider boundary, not in a prompt: with the
        // real CLI backend behind the ledger, the refused create never reaches
        // `yt issue create`.
        use crate::provider::cli::testutil::StubRunner;
        use crate::provider::CliBackend;

        let runner = Arc::new(StubRunner::new().ok(
            "yt",
            &[
                "issue",
                "create",
                "--project",
                "CLAUDE",
                "--summary",
                "s",
                "--description",
                "d",
                "--json",
            ],
            r#"{"idReadable":"CLAUDE-1"}"#,
        ));
        let backend = CliBackend::new(runner.clone());
        let audit = IssueWriteAudit::new(Some(42), 1);
        let tracker = AuditedTracker::new(&backend, audit.clone());

        assert_eq!(
            tracker.create_issue("CLAUDE", "s", "d", None).unwrap(),
            "CLAUDE-1"
        );
        assert!(tracker.create_issue("CLAUDE", "s", "d", None).is_err());

        let calls = runner.calls.lock().unwrap().clone();
        assert_eq!(
            calls.iter().filter(|c| c.contains("issue create")).count(),
            1,
            "the refused create must not reach the CLI: {calls:?}"
        );
        assert_eq!(audit.refused(), 1);
    }

    #[test]
    fn no_delete_on_the_tracker_surface() {
        // Deletes remain denied. The tracker trait exposes no delete and the
        // CLI backend invokes no `yt issue delete`, so the guard is a scan of
        // the two files that own that surface. It fails if either reappears.
        for (name, src) in [
            ("provider/mod.rs", include_str!("mod.rs")),
            ("provider/cli.rs", include_str!("cli.rs")),
        ] {
            for (i, line) in src.lines().enumerate() {
                let line = line.trim();
                // Skip doc/comment lines: this test's own prose names the verb.
                if line.starts_with("//") {
                    continue;
                }
                assert!(
                    !line.contains("\"delete\""),
                    "{name}:{}: a CLI argv naming the `delete` verb: {line}",
                    i + 1
                );
                assert!(
                    !line.contains("fn delete"),
                    "{name}:{}: a delete method on the tracker surface: {line}",
                    i + 1
                );
            }
        }
    }
}
