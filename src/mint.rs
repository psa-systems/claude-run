//! Triage-driven YouTrack issue mint (CLAUDE-46, part of the webhook fast-path
//! epic CLAUDE-36).
//!
//! Turns a triaged failure ([`crate::triage::Triage`]) plus its
//! `action_run_failure` metadata and redacted job log into exactly one YouTrack
//! task issue, routed to the repo's own YouTrack project (with `CLAUDE` as the
//! fallback per the 2026-05-31 decision). The pipeline never duplicates and never
//! reopens a resolved issue:
//!
//! ```text
//! route   -> bucket 3 (transient) mints nothing.
//! guard   -> loop safety: a failing run triggered by a bot login (the CI-fix
//!            bot's own fix PR) mints nothing; per-PR attempt cap and daily cap
//!            (the DEV-335 caps, reused not re-derived) gate the rest.
//! dedupe  -> a resolved issue for the key is never reopened; an open issue for
//!            the key is left alone (no second issue).
//! mint    -> create in the mapped project, State=To do, attach the redacted log,
//!            then route the bucket:
//!              bucket 1 (code_fix)    -> AI Agent=Queued (forgejo-ci-fix CLAUDE-29
//!                                        picks it up and opens the fix PR)
//!              bucket 2 (human_action)-> assign a human (no AI Agent, no PR)
//! ```
//!
//! The minting logic is pure / trait-driven so it is unit-tested against a stub
//! [`IssueTracker`] with no real `yt` calls. Wiring the chain (log fetch ->
//! triage -> mint) into [`crate::web::webhooks::consume_events`] is the daemon
//! integration step: this module delivers the engine and its types.

use std::path::Path;

use sha2::{Digest, Sha256};
use tracing::warn;

use crate::config::MintConfig;
use crate::error::Result;
use crate::provider::IssueTracker;
use crate::triage::{Triage, TriageBucket};

/// Everything the mint needs about one triaged failure: the
/// `action_run_failure` metadata, the triage verdict, and the redacted log path.
#[derive(Debug, Clone)]
pub struct MintRequest<'a> {
    /// `owner/repo`.
    pub repo: &'a str,
    /// The run's per-repo index (one half of the primary dedupe key).
    pub index_in_repo: i64,
    /// Failing commit SHA (part of the secondary dedupe key).
    pub commit_sha: &'a str,
    /// Workflow filename, e.g. `ci.yml` (part of the secondary dedupe key).
    pub workflow_id: &'a str,
    /// Run URL, surfaced in the issue body.
    pub html_url: &'a str,
    /// Login that triggered the run; matched against the loop-safety bot list.
    pub trigger_user: &'a str,
    /// The triage verdict (bucket + analysis).
    pub triage: &'a Triage,
    /// Redacted job-log artifact to attach; `None` when no log was available.
    pub log_path: Option<&'a Path>,
}

/// The DEV-335 caps, reused (not re-derived) from the runner's existing
/// per-key attempt counter and daily run ledger ([`crate::state`]). The caller
/// reads these from the DB and passes them in so the mint decision stays pure.
#[derive(Debug, Clone, Copy)]
pub struct MintCaps {
    /// Attempts already recorded for this failure's dedupe key.
    pub attempts: u32,
    /// Per-key attempt cap (`EffectiveLimits::max_attempts_per_key`).
    pub max_attempts: u32,
    /// Runs already counted today for the minting action.
    pub daily: u32,
    /// Daily run cap (`EffectiveLimits::max_runs_per_day`).
    pub max_runs_per_day: u32,
}

/// The outcome of a mint attempt. Every non-`Created` variant names why nothing
/// was minted, so the caller can log the decision.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MintOutcome {
    /// A new issue was created (and the log attached / fields set).
    Created {
        key: String,
        project: String,
        bucket: TriageBucket,
    },
    /// Bucket 3 (transient/infra): mint nothing.
    SkippedTransient,
    /// Loop safety: the failing run was triggered by a bot login (a fix-PR run).
    SkippedBotLoop,
    /// The per-key attempt cap was already reached.
    SkippedAttemptCap,
    /// The daily run cap was already reached.
    SkippedDailyCap,
    /// An open issue already exists for this key: do not duplicate.
    SkippedDuplicateOpen { existing: String },
    /// A resolved issue exists for this key: never reopen.
    SkippedResolved { existing: String },
}

/// How a (non-transient) bucket routes once an issue is minted.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Route {
    /// Set `AI Agent = Queued` so forgejo-ci-fix (CLAUDE-29) opens the fix PR.
    queue_for_fix: bool,
    /// Assign the configured human (bucket 2): no AI Agent, no PR.
    assign_human: bool,
}

/// Route a triage bucket. `None` for [`TriageBucket::Transient`] (mint nothing);
/// `Some` for the two actionable buckets.
fn route(bucket: TriageBucket) -> Option<Route> {
    match bucket {
        TriageBucket::CodeFix => Some(Route {
            queue_for_fix: true,
            assign_human: false,
        }),
        TriageBucket::HumanAction => Some(Route {
            queue_for_fix: false,
            assign_human: true,
        }),
        TriageBucket::Transient => None,
    }
}

/// True when `trigger_user` is one of the loop-safety bot logins: a failing run
/// the CI-fix bot itself triggered (its own fix PR) must never mint a new task,
/// or a failing fix recurses.
pub fn is_bot_loop(trigger_user: &str, cfg: &MintConfig) -> bool {
    cfg.bot_logins.iter().any(|b| b == trigger_user)
}

/// Resolve the YouTrack project short name for `repo`: the per-repo mapping when
/// present, else the configured fallback (CLAUDE).
pub fn resolve_project(repo: &str, cfg: &MintConfig) -> String {
    cfg.repo_project_map
        .get(repo)
        .cloned()
        .unwrap_or_else(|| cfg.fallback_project.clone())
}

/// A stable, single-token full-text dedupe marker over `parts`. Hex-only so
/// YouTrack indexes it as one searchable term (punctuation would fragment it).
fn marker(parts: &[&str]) -> String {
    let mut h = Sha256::new();
    for p in parts {
        h.update(p.as_bytes());
        h.update([0x1f]);
    }
    let digest = h.finalize();
    format!("crdedupe{}", hex::encode(&digest[..8]))
}

/// Primary dedupe marker: `(repo, index_in_repo)`.
fn primary_marker(req: &MintRequest) -> String {
    marker(&[req.repo, &req.index_in_repo.to_string()])
}

/// Secondary dedupe marker: `(repo, commit_sha, workflow_id)`.
fn secondary_marker(req: &MintRequest) -> String {
    marker(&[req.repo, req.commit_sha, req.workflow_id])
}

/// Mint (or decline to mint) a YouTrack issue from a triaged failure. Pure
/// decision flow over the [`IssueTracker`] trait; see the module docs for the
/// ordering (route -> loop safety -> caps -> dedupe -> create).
pub fn mint(
    tracker: &dyn IssueTracker,
    cfg: &MintConfig,
    req: &MintRequest,
    caps: &MintCaps,
) -> Result<MintOutcome> {
    // Bucket 3: transient/infra. Mint nothing.
    let Some(route) = route(req.triage.bucket) else {
        return Ok(MintOutcome::SkippedTransient);
    };

    // Loop safety: a fix-PR run the bot triggered must not mint a new task.
    if is_bot_loop(req.trigger_user, cfg) {
        return Ok(MintOutcome::SkippedBotLoop);
    }

    // DEV-335 caps, reused: per-key attempt cap, then daily run cap.
    if caps.attempts >= caps.max_attempts {
        return Ok(MintOutcome::SkippedAttemptCap);
    }
    if caps.daily >= caps.max_runs_per_day {
        return Ok(MintOutcome::SkippedDailyCap);
    }

    // Dedupe. A resolved issue for either key is never reopened; an open issue
    // for either key means we already have the task.
    let primary = primary_marker(req);
    let secondary = secondary_marker(req);
    for m in [&primary, &secondary] {
        if let Some(existing) = find_first(tracker, &format!("{m} #Resolved"))? {
            return Ok(MintOutcome::SkippedResolved { existing });
        }
    }
    for m in [&primary, &secondary] {
        if let Some(existing) = find_first(tracker, &format!("{m} #Unresolved"))? {
            return Ok(MintOutcome::SkippedDuplicateOpen { existing });
        }
    }

    // Create the issue in the mapped project, bucket 2 pre-assigned to a human.
    let project = resolve_project(req.repo, cfg);
    let summary = build_summary(req);
    let description = build_description(req, &primary, &secondary);
    let assignee = if route.assign_human {
        cfg.human_assignee.as_deref()
    } else {
        None
    };
    let key = tracker.create_issue(&project, &summary, &description, assignee)?;

    // Attach the redacted log (best-effort: the issue already exists, so a
    // failed attachment must not strand the mint).
    if let Some(log) = req.log_path {
        if let Err(e) = tracker.attach_file(&key, log) {
            warn!(key = %key, error = %e, "attaching redacted log failed (continuing)");
        }
    }

    // State first, then the authoritative AI Agent write last (bucket 1 only):
    // forgejo-ci-fix only picks the issue up once it is fully formed.
    tracker.set_state(&key, &cfg.target_state)?;
    if route.queue_for_fix {
        tracker.set_field(&key, &cfg.ai_agent_field, &cfg.queued_value)?;
    }

    Ok(MintOutcome::Created {
        key,
        project,
        bucket: req.triage.bucket,
    })
}

/// First matching issue's `idReadable` for `query`, or `None` when the query
/// matches nothing. Reuses the tracker's `yt list --query=<q> --json` surface.
fn find_first(tracker: &dyn IssueTracker, query: &str) -> Result<Option<String>> {
    let issues = tracker.list_queued(query)?;
    Ok(issues.into_iter().find_map(|v| {
        v.get("idReadable")
            .and_then(|i| i.as_str().map(String::from))
    }))
}

/// Short issue summary. The bare run metadata; the body carries the analysis.
fn build_summary(req: &MintRequest) -> String {
    let short_sha = req.commit_sha.chars().take(8).collect::<String>();
    format!(
        "CI failure: {} on {} (run #{}, {})",
        req.workflow_id, req.repo, req.index_in_repo, short_sha
    )
}

/// Markdown issue body: the triage analysis, the run metadata, and the two
/// hidden dedupe markers a later mint searches for.
fn build_description(req: &MintRequest, primary: &str, secondary: &str) -> String {
    let attached = if req.log_path.is_some() {
        "The redacted failing-job log is attached to this issue."
    } else {
        "No job log was available (transient miss); none attached."
    };
    [
        "## Triage".to_string(),
        format!(
            "Bucket: `{}` (classified by {:?}).",
            req.triage.bucket, req.triage.classified_by
        ),
        String::new(),
        req.triage.analysis.clone(),
        String::new(),
        "## Failure".to_string(),
        format!("- Repo: `{}`", req.repo),
        format!("- Workflow: `{}`", req.workflow_id),
        format!("- Commit: `{}`", req.commit_sha),
        format!("- Run: {}", req.html_url),
        String::new(),
        attached.to_string(),
        String::new(),
        "<!-- claude-run dedupe markers; do not edit -->".to_string(),
        format!("Dedupe-Key: {primary} {secondary}"),
    ]
    .join("\n")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::provider::VcsRepo;
    use crate::triage::{ClassifiedBy, Triage};
    use serde_json::Value;
    use std::collections::HashMap;
    use std::sync::Mutex;

    /// Stub [`IssueTracker`] recording every mutation and returning canned
    /// `list_queued` responses keyed by query. Only the methods the mint calls
    /// are meaningful; the rest are unreachable in these tests.
    struct StubTracker {
        /// query -> JSON array string the dedupe search should return.
        searches: HashMap<String, String>,
        /// idReadable handed back by `create_issue`.
        new_key: String,
        calls: Mutex<Vec<String>>,
    }

    impl StubTracker {
        fn new() -> Self {
            Self {
                searches: HashMap::new(),
                new_key: "CLAUDE-99".to_string(),
                calls: Mutex::new(Vec::new()),
            }
        }
        fn with_search(mut self, query: &str, json_array: &str) -> Self {
            self.searches
                .insert(query.to_string(), json_array.to_string());
            self
        }
        fn calls(&self) -> Vec<String> {
            self.calls.lock().unwrap().clone()
        }
    }

    impl IssueTracker for StubTracker {
        fn list_queued(&self, query: &str) -> Result<Vec<Value>> {
            self.calls.lock().unwrap().push(format!("list {query}"));
            let raw = self.searches.get(query).map(String::as_str).unwrap_or("[]");
            Ok(serde_json::from_str(raw).unwrap())
        }
        fn get_field(&self, _key: &str, _field: &str) -> Result<String> {
            unreachable!("mint does not read fields")
        }
        fn set_field(&self, key: &str, field: &str, value: &str) -> Result<()> {
            self.calls
                .lock()
                .unwrap()
                .push(format!("set_field {key} {field} {value}"));
            Ok(())
        }
        fn set_state(&self, key: &str, state: &str) -> Result<()> {
            self.calls
                .lock()
                .unwrap()
                .push(format!("set_state {key} {state}"));
            Ok(())
        }
        fn comment(&self, _key: &str, _body: &str) -> Result<()> {
            unreachable!("mint does not comment")
        }
        fn resolve_vcs(&self, _project_key: &str) -> Result<VcsRepo> {
            unreachable!("mint does not resolve vcs")
        }
        fn create_issue(
            &self,
            project: &str,
            summary: &str,
            _description: &str,
            assignee: Option<&str>,
        ) -> Result<String> {
            self.calls.lock().unwrap().push(format!(
                "create {project} | {summary} | assignee={}",
                assignee.unwrap_or("-")
            ));
            Ok(self.new_key.clone())
        }
        fn attach_file(&self, key: &str, file: &Path) -> Result<()> {
            self.calls
                .lock()
                .unwrap()
                .push(format!("attach {key} {}", file.display()));
            Ok(())
        }
    }

    fn cfg() -> MintConfig {
        MintConfig {
            repo_project_map: [("fabrikam/forgejo-cli".to_string(), "FJ".to_string())]
                .into_iter()
                .collect(),
            human_assignee: Some("alice".to_string()),
            ..MintConfig::default()
        }
    }

    fn caps() -> MintCaps {
        MintCaps {
            attempts: 0,
            max_attempts: 3,
            daily: 0,
            max_runs_per_day: 20,
        }
    }

    fn triage(bucket: TriageBucket) -> Triage {
        Triage {
            bucket,
            analysis: "root cause and next step".to_string(),
            classified_by: ClassifiedBy::Llm,
        }
    }

    fn req<'a>(repo: &'a str, t: &'a Triage, user: &'a str) -> MintRequest<'a> {
        MintRequest {
            repo,
            index_in_repo: 7,
            commit_sha: "abc123def456",
            workflow_id: "ci.yml",
            html_url: "https://dev.a8n.run/owner/repo/actions/runs/7",
            trigger_user: user,
            triage: t,
            log_path: None,
        }
    }

    // ---- routing helpers --------------------------------------------------

    #[test]
    fn route_buckets_correctly() {
        assert_eq!(
            route(TriageBucket::CodeFix),
            Some(Route {
                queue_for_fix: true,
                assign_human: false
            })
        );
        assert_eq!(
            route(TriageBucket::HumanAction),
            Some(Route {
                queue_for_fix: false,
                assign_human: true
            })
        );
        assert_eq!(route(TriageBucket::Transient), None);
    }

    #[test]
    fn resolve_project_uses_map_then_fallback() {
        let c = cfg();
        assert_eq!(resolve_project("fabrikam/forgejo-cli", &c), "FJ");
        assert_eq!(resolve_project("contoso/claude-run", &c), "CLAUDE");
    }

    #[test]
    fn markers_are_stable_single_tokens_and_distinct() {
        let t = triage(TriageBucket::CodeFix);
        let r = req("owner/repo", &t, "alice");
        let p = primary_marker(&r);
        let s = secondary_marker(&r);
        assert!(p.starts_with("crdedupe"));
        assert!(p.chars().all(|c| c.is_ascii_alphanumeric()));
        assert_ne!(p, s);
        // Stable across calls (same inputs -> same marker).
        assert_eq!(p, primary_marker(&r));
    }

    // ---- AC 5: bucket 3 mints nothing -------------------------------------

    #[test]
    fn transient_bucket_mints_nothing() {
        let t = triage(TriageBucket::Transient);
        let tracker = StubTracker::new();
        let out = mint(&tracker, &cfg(), &req("owner/repo", &t, "alice"), &caps()).unwrap();
        assert_eq!(out, MintOutcome::SkippedTransient);
        assert!(tracker.calls().is_empty(), "no yt calls for a transient");
    }

    // ---- AC 1 + AC 5: bucket 1 creates one issue and queues it -------------

    #[test]
    fn code_fix_creates_issue_in_mapped_project_and_queues() {
        let t = triage(TriageBucket::CodeFix);
        let tracker = StubTracker::new();
        let out = mint(
            &tracker,
            &cfg(),
            &req("fabrikam/forgejo-cli", &t, "alice"),
            &caps(),
        )
        .unwrap();
        assert_eq!(
            out,
            MintOutcome::Created {
                key: "CLAUDE-99".to_string(),
                project: "FJ".to_string(),
                bucket: TriageBucket::CodeFix,
            }
        );
        let calls = tracker.calls();
        assert!(
            calls.iter().any(|c| c.starts_with("create FJ |")),
            "{calls:?}"
        );
        assert!(
            calls.iter().any(|c| c == "set_state CLAUDE-99 To do"),
            "{calls:?}"
        );
        assert!(
            calls
                .iter()
                .any(|c| c == "set_field CLAUDE-99 AI Agent Queued"),
            "{calls:?}"
        );
        // No human assignee on a code-fix issue.
        assert!(calls.iter().any(|c| c.contains("assignee=-")), "{calls:?}");
    }

    // ---- AC 1 + AC 5: bucket 2 assigns a human, no Queued ------------------

    #[test]
    fn human_action_assigns_human_without_queueing() {
        let t = triage(TriageBucket::HumanAction);
        let tracker = StubTracker::new();
        let out = mint(
            &tracker,
            &cfg(),
            &req("contoso/claude-run", &t, "alice"),
            &caps(),
        )
        .unwrap();
        assert_eq!(
            out,
            MintOutcome::Created {
                key: "CLAUDE-99".to_string(),
                project: "CLAUDE".to_string(),
                bucket: TriageBucket::HumanAction,
            }
        );
        let calls = tracker.calls();
        assert!(
            calls.iter().any(|c| c.contains("assignee=alice")),
            "{calls:?}"
        );
        assert!(
            calls.iter().any(|c| c == "set_state CLAUDE-99 To do"),
            "{calls:?}"
        );
        // Bucket 2 never queues for the fix bot.
        assert!(!calls.iter().any(|c| c.contains("AI Agent")), "{calls:?}");
    }

    // ---- AC 1: the redacted log is attached when present -------------------

    #[test]
    fn created_issue_attaches_the_redacted_log() {
        let t = triage(TriageBucket::CodeFix);
        let tracker = StubTracker::new();
        let mut r = req("contoso/claude-run", &t, "alice");
        let log = Path::new("/tmp/redacted.log");
        r.log_path = Some(log);
        mint(&tracker, &cfg(), &r, &caps()).unwrap();
        assert!(
            tracker
                .calls()
                .iter()
                .any(|c| c == "attach CLAUDE-99 /tmp/redacted.log"),
            "{:?}",
            tracker.calls()
        );
    }

    // ---- AC 3: loop safety -------------------------------------------------

    #[test]
    fn bot_authored_run_mints_nothing() {
        // Default bot login is "claude-run"; a failing fix-PR run it triggered
        // must not recurse into a new task.
        let t = triage(TriageBucket::CodeFix);
        let tracker = StubTracker::new();
        let out = mint(
            &tracker,
            &cfg(),
            &req("contoso/claude-run", &t, "claude-run"),
            &caps(),
        )
        .unwrap();
        assert_eq!(out, MintOutcome::SkippedBotLoop);
        assert!(tracker.calls().is_empty());
    }

    #[test]
    fn attempt_cap_blocks_minting() {
        let t = triage(TriageBucket::CodeFix);
        let tracker = StubTracker::new();
        let capped = MintCaps {
            attempts: 3,
            ..caps()
        };
        let out = mint(&tracker, &cfg(), &req("owner/repo", &t, "alice"), &capped).unwrap();
        assert_eq!(out, MintOutcome::SkippedAttemptCap);
        assert!(tracker.calls().is_empty());
    }

    // ---- AC 4: DEV-335 daily cap is enforced -------------------------------

    #[test]
    fn daily_cap_blocks_minting() {
        let t = triage(TriageBucket::CodeFix);
        let tracker = StubTracker::new();
        let capped = MintCaps {
            daily: 20,
            ..caps()
        };
        let out = mint(&tracker, &cfg(), &req("owner/repo", &t, "alice"), &capped).unwrap();
        assert_eq!(out, MintOutcome::SkippedDailyCap);
        assert!(tracker.calls().is_empty());
    }

    // ---- AC 2: dedupe -----------------------------------------------------

    #[test]
    fn open_issue_for_key_is_not_duplicated() {
        let t = triage(TriageBucket::CodeFix);
        let r = req("owner/repo", &t, "alice");
        let primary = primary_marker(&r);
        let tracker = StubTracker::new().with_search(
            &format!("{primary} #Unresolved"),
            r#"[{"idReadable":"CLAUDE-50","summary":"already open"}]"#,
        );
        let out = mint(&tracker, &cfg(), &r, &caps()).unwrap();
        assert_eq!(
            out,
            MintOutcome::SkippedDuplicateOpen {
                existing: "CLAUDE-50".to_string()
            }
        );
        // Searched, but never created.
        assert!(!tracker.calls().iter().any(|c| c.starts_with("create")));
    }

    #[test]
    fn resolved_issue_for_key_is_never_reopened() {
        let t = triage(TriageBucket::CodeFix);
        let r = req("owner/repo", &t, "alice");
        let primary = primary_marker(&r);
        let tracker = StubTracker::new().with_search(
            &format!("{primary} #Resolved"),
            r#"[{"idReadable":"CLAUDE-40","summary":"done last week"}]"#,
        );
        let out = mint(&tracker, &cfg(), &r, &caps()).unwrap();
        assert_eq!(
            out,
            MintOutcome::SkippedResolved {
                existing: "CLAUDE-40".to_string()
            }
        );
        assert!(!tracker.calls().iter().any(|c| c.starts_with("create")));
    }

    #[test]
    fn secondary_key_also_dedupes_against_an_open_issue() {
        // The primary (repo,index) key misses, but the secondary
        // (repo,commit,workflow) key matches an open issue -> still no duplicate.
        let t = triage(TriageBucket::CodeFix);
        let r = req("owner/repo", &t, "alice");
        let secondary = secondary_marker(&r);
        let tracker = StubTracker::new().with_search(
            &format!("{secondary} #Unresolved"),
            r#"[{"idReadable":"CLAUDE-51"}]"#,
        );
        let out = mint(&tracker, &cfg(), &r, &caps()).unwrap();
        assert_eq!(
            out,
            MintOutcome::SkippedDuplicateOpen {
                existing: "CLAUDE-51".to_string()
            }
        );
    }
}
