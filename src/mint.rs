//! Triage-driven YouTrack issue mint (CLAUDE-46, part of the webhook
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
//!            the key is left alone (no second issue). CLAUDE-264: a failure the
//!            log shows is a RUNNER condition collapses on a branch-agnostic
//!            infra key instead, and that key alone is exempt from the
//!            never-reopen check, because a host condition recurs.
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
use std::sync::Mutex;
use std::time::{Duration, Instant};

use sha2::{Digest, Sha256};
use tracing::warn;

use crate::config::MintConfig;
use crate::error::Result;
use crate::provider::{IssueTracker, VcsMapEntry};
use crate::triage::{Triage, TriageBucket};

/// Default time-to-live for the cached inverse VCS routing map (CLAUDE-86): a
/// just-added VCS integration is picked up within this window without a daemon
/// restart, while steady-state mints reuse the cache and issue no extra `yt`
/// calls. Refresh-on-miss shortens this for a genuinely new repo.
pub const DEFAULT_VCS_MAP_TTL: Duration = Duration::from_secs(15 * 60);

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
    /// CLAUDE-190/199: the failing run's branch, resolved by the consumer from
    /// the log's environment dump (`GITHUB_HEAD_REF` on a PR run, else the
    /// `refs/heads/` short name), else the run detail. Empty when unresolvable
    /// (a logless failure). Part of the root-cause dedupe key.
    pub branch: &'a str,
    /// CLAUDE-190: the failing job's name, resolved from the jobs endpoint.
    /// Empty when unresolvable. Part of the root-cause dedupe key.
    pub job_name: &'a str,
    /// CLAUDE-190: a deterministic, pure-string signature of the first
    /// diagnostic line in the redacted log (paths / numbers stripped), so the
    /// same root cause across commits yields the same key. `None` when no
    /// diagnostic line is derivable: the log stops before one was emitted,
    /// carries none, or the failure produced no log at all. The key then falls
    /// back to `(repo, branch, workflow, job)`.
    pub failure_signature: Option<&'a str>,
    /// CLAUDE-264: the stable [`crate::infra_failure`] kind id when the full
    /// redacted log shows the failure is a property of the RUNNER (a full disk,
    /// an OOM kill, an unreachable registry) rather than of the commit. `Some`
    /// switches the cross-run collapse onto the branch-agnostic
    /// [`infra_marker`]; `None` (a code failure, the common case) leaves every
    /// key and every decision exactly as it was.
    pub infra_kind: Option<&'a str>,
    /// Run URL, surfaced in the issue body.
    pub html_url: &'a str,
    /// Login that triggered the run; matched against the loop-safety bot list.
    pub trigger_user: &'a str,
    /// The triage verdict (bucket + analysis).
    pub triage: &'a Triage,
    /// Redacted job-log artifact to attach; `None` when no log was available.
    pub log_path: Option<&'a Path>,
    /// CLAUDE-92 `repository.private`, from the payload. CLAUDE-190: no longer
    /// gates dedupe (the root-cause key collapses repeats for public and private
    /// repos alike); retained as context on the request.
    pub private: bool,
    /// CLAUDE-149: whether the failing run has an open PR for its branch. When
    /// set, the auto-fix (queue-for-fix) route is suppressed so the PR author
    /// owns the fix; a human-action failure still mints.
    pub has_open_pr: bool,
    /// CLAUDE-153: the webhook-scoped auto-fix toggle (`[webhook].auto_fix`).
    /// When `false`, a queue-for-fix route (`code_fix` / `auto_fix`) is
    /// suppressed so no corrective fixup PR is queued and the failure stays
    /// reported on the original PR; a human-action failure still mints.
    pub auto_fix_enabled: bool,
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
        /// How `project` was chosen (override / vcs / fallback), for the journal.
        source: RouteSource,
    },
    /// Bucket 3 (transient/infra): mint nothing.
    SkippedTransient,
    /// Loop safety: the failing run was triggered by a bot login (a fix-PR run).
    SkippedBotLoop,
    /// CLAUDE-149: the failing run has an open PR, so the auto-fix (queued)
    /// route is suppressed: a failing open PR is the author's to fix, not the
    /// bot's. Only the queue-for-fix buckets are suppressed; a human-action
    /// failure on an open PR still mints.
    SkippedOpenPr,
    /// CLAUDE-153: the webhook-scoped auto-fix toggle (`[webhook].auto_fix`) is
    /// off, so a queue-for-fix bucket (`code_fix` / `auto_fix`) mints nothing
    /// and queues no fixup PR; the failure stays on the original PR. Only the
    /// queue-for-fix buckets are suppressed; a human-action failure still mints.
    SkippedAutoFixDisabled,
    /// The per-key attempt cap was already reached.
    SkippedAttemptCap,
    /// The daily run cap was already reached.
    SkippedDailyCap,
    /// An open issue already exists for this key: do not duplicate.
    SkippedDuplicateOpen { existing: String },
    /// A resolved issue exists for this key: never reopen.
    SkippedResolved { existing: String },
    /// CLAUDE-100: a private-repo repeat of the same `(repo, workflow_id)`
    /// commented onto the existing OPEN issue instead of minting a sibling.
    AssociatedWithOpen { existing: String },
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
        // CLAUDE-104: an auto-fix routes exactly like a code-fix - queue for the
        // runner, no human assignee - but reaches this point even with no open
        // PR and on a push to main.
        TriageBucket::CodeFix | TriageBucket::AutoFix => Some(Route {
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

/// How a mint's destination project was chosen, surfaced in the journal so a
/// wrong destination is diagnosable (CLAUDE-86).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RouteSource {
    /// An explicit `repo_project_map` entry (operator override).
    Override,
    /// A project's enabled VCS integration claimed the repo.
    Vcs,
    /// No override and no VCS claim: `fallback_project`.
    Fallback,
}

impl std::fmt::Display for RouteSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            RouteSource::Override => "override",
            RouteSource::Vcs => "vcs",
            RouteSource::Fallback => "fallback",
        };
        f.write_str(s)
    }
}

/// Normalize a forge host or processor `serverUrl` for comparison: drop any
/// scheme, trailing slash, and case. `https://dev.a8n.run/` and `dev.a8n.run`
/// both become `dev.a8n.run`.
fn norm_host(s: &str) -> String {
    let after = s.split_once("://").map(|(_, rest)| rest).unwrap_or(s);
    after.trim_end_matches('/').to_ascii_lowercase()
}

/// The forge host (no scheme) of a run's `html_url`, e.g.
/// `https://dev.a8n.run/owner/repo/...` -> `dev.a8n.run`. Empty when the URL has
/// no host segment.
fn forge_host(url: &str) -> String {
    norm_host(url).split('/').next().unwrap_or("").to_string()
}

/// The inverse VCS routing map: forge `(host, repo path)` -> project short name,
/// built from every non-archived project's enabled VCS processors. Lookups are
/// case-insensitive on both host and path and ignore trailing slashes on the
/// host (CLAUDE-86).
#[derive(Debug, Clone, Default)]
pub struct VcsRouteMap {
    /// `(host_normalized, path_lowercased, project_key)`.
    entries: Vec<(String, String, String)>,
}

impl VcsRouteMap {
    /// Build a route map from the flattened `(server_url, path, project)` rows
    /// the [`IssueTracker`] returns, normalizing host and path once up front.
    pub fn from_entries(rows: Vec<VcsMapEntry>) -> Self {
        let entries = rows
            .into_iter()
            .map(|e| {
                (
                    norm_host(&e.server_url),
                    e.path.to_ascii_lowercase(),
                    e.project_key,
                )
            })
            .collect();
        Self { entries }
    }

    /// The project that claims `repo` on `host`, or `None` when no enabled VCS
    /// integration matches. When several projects claim the same repo, the match
    /// is deterministic (first by project short name, ascending) and all
    /// candidates are logged (CLAUDE-86 ambiguity rule).
    pub fn lookup(&self, host: &str, repo: &str) -> Option<String> {
        let host = norm_host(host);
        let repo = repo.to_ascii_lowercase();
        let mut claimants: Vec<&str> = self
            .entries
            .iter()
            .filter(|(h, p, _)| *h == host && *p == repo)
            .map(|(_, _, project)| project.as_str())
            .collect();
        claimants.sort_unstable();
        claimants.dedup();
        match claimants.as_slice() {
            [] => None,
            [only] => Some((*only).to_string()),
            many => {
                warn!(
                    host = %host,
                    repo = %repo,
                    candidates = %many.join(", "),
                    chosen = %many[0],
                    "multiple projects claim this repo via VCS integration; routing to the first by short name"
                );
                Some(many[0].to_string())
            }
        }
    }
}

/// Lazily-built, TTL-cached inverse VCS routing map shared across mints
/// (CLAUDE-86). The map is built on first use and rebuilt when older than the
/// TTL; a lookup miss triggers one immediate refresh before falling back, so a
/// just-added VCS integration routes without waiting out the TTL. A rebuild
/// failure keeps any prior map and degrades to the fallback rather than erroring.
pub struct VcsRouteCache {
    ttl: Duration,
    inner: Mutex<Option<CachedMap>>,
}

struct CachedMap {
    map: VcsRouteMap,
    built_at: Instant,
}

impl VcsRouteCache {
    /// Cache with the given TTL.
    pub fn new(ttl: Duration) -> Self {
        Self {
            ttl,
            inner: Mutex::new(None),
        }
    }

    /// Resolve the YouTrack project for `repo` on forge `host`, full order:
    /// explicit `repo_project_map` override, then the cached VCS map (rebuilt if
    /// stale, refreshed once on a miss), then `fallback_project`. Returns the
    /// project short name and which rule chose it.
    pub fn resolve_project(
        &self,
        tracker: &dyn IssueTracker,
        cfg: &MintConfig,
        repo: &str,
        host: &str,
    ) -> (String, RouteSource) {
        // 1. Explicit override always wins and needs no VCS map.
        if let Some(project) = cfg.repo_project_map.get(repo) {
            return (project.clone(), RouteSource::Override);
        }

        // 2. VCS integration map.
        let mut guard = self.inner.lock().unwrap();
        let built_now = self.ensure(tracker, &mut guard, false);
        if let Some(project) = guard.as_ref().and_then(|c| c.map.lookup(host, repo)) {
            return (project, RouteSource::Vcs);
        }
        // Miss. Refresh once before falling back, unless the map was just built
        // (a fresh build that already missed will not change on a re-fetch).
        if !built_now {
            self.ensure(tracker, &mut guard, true);
            if let Some(project) = guard.as_ref().and_then(|c| c.map.lookup(host, repo)) {
                return (project, RouteSource::Vcs);
            }
        }

        // 3. Fallback for an unclaimed repo.
        (cfg.fallback_project.clone(), RouteSource::Fallback)
    }

    /// Ensure the cached map is fresh enough, rebuilding it via `tracker` when it
    /// is missing, older than the TTL, or `force`d. Returns whether this call
    /// performed a (successful) rebuild. A rebuild error is logged and leaves any
    /// prior map in place.
    fn ensure(
        &self,
        tracker: &dyn IssueTracker,
        guard: &mut Option<CachedMap>,
        force: bool,
    ) -> bool {
        let stale = match guard.as_ref() {
            None => true,
            Some(c) => force || c.built_at.elapsed() >= self.ttl,
        };
        if !stale {
            return false;
        }
        match tracker.vcs_repo_map() {
            Ok(rows) => {
                *guard = Some(CachedMap {
                    map: VcsRouteMap::from_entries(rows),
                    built_at: Instant::now(),
                });
                true
            }
            Err(e) => {
                warn!(error = %e, "rebuilding the VCS routing map failed; routing via the prior map / fallback");
                false
            }
        }
    }
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

/// Primary dedupe marker: `(repo, index_in_repo)`. Per-run: it collapses only a
/// webhook redelivery of the identical run (idempotency), never across commits.
fn primary_marker(req: &MintRequest) -> String {
    marker(&[req.repo, &req.index_in_repo.to_string()])
}

/// Secondary dedupe marker: `(repo, commit_sha, workflow_id)`. Commit-scoped, so
/// retained only for back-compat with issues minted before the root-cause key
/// (a resolved such issue must still not be reopened); the cross-run collapse is
/// [`root_cause_marker`], not this.
fn secondary_marker(req: &MintRequest) -> String {
    marker(&[req.repo, req.commit_sha, req.workflow_id])
}

/// CLAUDE-190 root-cause dedupe marker: the cross-run collapsing key. Keyed on
/// `(repo, branch, workflow_id, job_name, failure_signature)` so N commits that
/// each fail the same job on the same branch with the same signature collapse
/// onto ONE open issue, while a different signature (a different root cause)
/// stays a separate issue. When no signature is derivable (the log stops before
/// a diagnostic was emitted, carries none, or the failure produced no log at
/// all) the key falls back to
/// `(repo, branch, workflow_id, job_name)`: this still collapses the
/// "branch is broken" storm onto one issue, intentionally grouping two
/// indistinguishable logless failures (the CLAUDE-189 analysis can later split a
/// mis-grouped issue).
fn root_cause_marker(req: &MintRequest) -> String {
    match req.failure_signature {
        Some(sig) => marker(&[req.repo, req.branch, req.workflow_id, req.job_name, sig]),
        None => marker(&[req.repo, req.branch, req.workflow_id, req.job_name]),
    }
}

/// CLAUDE-264 infrastructure dedupe marker: the branch-agnostic collapsing key,
/// `(repo, workflow_id, job_name, infra_kind)`. `None` unless the consumer
/// detected an infrastructure condition. It deliberately contains neither
/// `branch` nor `commit_sha`, because a host condition (a full disk, an OOM
/// kill, an unreachable registry) is identical on every branch and is not
/// fixable on any of them: one condition on one `(repo, workflow, job)` must
/// collapse onto ONE open issue however many PR branches hit it. The stable
/// `kind` id is hashed rather than the matched line, so the same condition keys
/// identically however the message is worded or however many bytes free it
/// reports. A code failure keeps the branch-scoped [`root_cause_marker`]: two
/// unrelated bugs failing the same job on two branches must NOT merge, and the
/// auto-fix route needs a single branch to deliver onto (CLAUDE-190).
fn infra_marker(req: &MintRequest) -> Option<String> {
    req.infra_kind
        .map(|kind| marker(&[req.repo, req.workflow_id, req.job_name, kind]))
}

/// CLAUDE-190: extract a deterministic failure signature from a redacted log,
/// or `None` when the log carries no recognizable diagnostic line. Pure string
/// work (no LLM, no I/O): the first diagnostic line, normalized so the same root
/// cause across commits yields the same signature.
pub fn failure_signature(log: &str) -> Option<String> {
    log.lines()
        .find(|l| is_diagnostic_line(l))
        .map(normalize_signature)
}

/// True for a line that looks like a compiler / runtime / test diagnostic: the
/// first such line in a log is the signature source. Ordered by how the tools
/// print them (a panic / assertion precedes the run summary), so the first hit
/// is the specific cause, not the generic "test result: FAILED" tally.
fn is_diagnostic_line(line: &str) -> bool {
    let t = line.trim();
    let lower = t.to_ascii_lowercase();
    t.starts_with("error[")
        || lower.starts_with("error:")
        || lower.contains("panicked at")
        || lower.contains("assertion failed")
        || lower.contains("assertion `")
        || (lower.starts_with("thread '") && lower.contains("panicked"))
        || lower.starts_with("test result: failed")
}

/// Normalize a diagnostic line so the same root cause yields the same signature:
/// lowercase, replace each path-like token with its basename (drop `/…`
/// prefixes), replace runs of digits with `#` (strip line/column numbers,
/// timestamps, hashes), and collapse whitespace.
fn normalize_signature(line: &str) -> String {
    let debased: Vec<String> = line
        .to_ascii_lowercase()
        .split_whitespace()
        .map(|tok| {
            if tok.contains('/') {
                tok.rsplit('/').next().unwrap_or(tok).to_string()
            } else {
                tok.to_string()
            }
        })
        .collect();
    mask_digit_runs(&debased.join(" "))
}

/// Replace every maximal run of ASCII digits with a single `#`.
fn mask_digit_runs(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    let mut in_digits = false;
    for c in s.chars() {
        if c.is_ascii_digit() {
            if !in_digits {
                out.push('#');
                in_digits = true;
            }
        } else {
            out.push(c);
            in_digits = false;
        }
    }
    out
}

/// Mint (or decline to mint) a YouTrack issue from a triaged failure. Pure
/// decision flow over the [`IssueTracker`] trait; see the module docs for the
/// ordering (route -> loop safety -> caps -> dedupe -> create).
pub fn mint(
    tracker: &dyn IssueTracker,
    cfg: &MintConfig,
    route_cache: &VcsRouteCache,
    req: &MintRequest,
    caps: &MintCaps,
) -> Result<MintOutcome> {
    // Bucket 3: transient/infra. Mint nothing.
    let Some(route) = route(req.triage.bucket) else {
        return Ok(MintOutcome::SkippedTransient);
    };

    // CLAUDE-153: the webhook-scoped auto-fix toggle. When off, a queue-for-fix
    // route mints nothing and queues no corrective fixup PR, leaving the failure
    // reported on the original PR for its author (the CLAUDE-152 behavior). Only
    // the queue-for-fix buckets are gated; a human-action failure still mints.
    if !req.auto_fix_enabled && route.queue_for_fix {
        return Ok(MintOutcome::SkippedAutoFixDisabled);
    }

    // CLAUDE-149: a failing open PR is the author's to fix. Suppress the auto-fix
    // (queue-for-fix) route for it so no corrective PR is queued; a human-action
    // failure on an open PR still mints (it needs a human, not the bot).
    if req.has_open_pr && route.queue_for_fix {
        return Ok(MintOutcome::SkippedOpenPr);
    }

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

    // Dedupe (CLAUDE-190). Three keys: the per-run `primary` (webhook-redelivery
    // idempotency), the commit-scoped `secondary` (back-compat only), and the
    // cross-run `root_cause` (the collapse). A resolved issue for ANY key is
    // never reopened.
    let primary = primary_marker(req);
    let secondary = secondary_marker(req);
    let root_cause = root_cause_marker(req);
    for m in [&primary, &secondary, &root_cause] {
        if let Some(existing) = find_first(tracker, &format!("{m} #Resolved"))? {
            return Ok(MintOutcome::SkippedResolved { existing });
        }
    }

    // CLAUDE-264: for an infrastructure failure the branch-agnostic infra marker
    // REPLACES the root-cause key as the cross-run collapse key (and as the
    // `Root-Cause-Key:` the CLAUDE-198 duplicate-collapse groups on). It does
    // NOT join the resolved loop above: a host condition recurs, and each
    // recurrence needs a new task, so suppressing a mint because a months-old
    // disk-full issue was resolved would silently stop reporting the condition.
    // The other three keys keep their existing never-reopen behaviour.
    let infra = infra_marker(req);
    let collapse = infra.as_deref().unwrap_or(root_cause.as_str());

    // Per-run redelivery of the identical run: idempotency, no second issue.
    if let Some(existing) = find_first(tracker, &format!("{primary} #Unresolved"))? {
        return Ok(MintOutcome::SkippedDuplicateOpen { existing });
    }

    // Cross-run collapse: an OPEN issue for the collapse key means the same
    // failing job / branch / signature already has a task (a later commit that
    // fails the same way), or, for an infra failure, the same host condition on
    // the same job on ANY branch. Append this run to it and mint nothing; a
    // different signature (or a different infra kind) yields a different key and
    // mints a sibling. The comment carries
    // this run's own per-run Dedupe-Key so a redelivery of THIS run full-text
    // matches and hits the per-run `SkippedDuplicateOpen` path above (no second
    // comment). No issue is created, so the caps ledger does not advance.
    if let Some(existing) = find_first(tracker, &format!("{collapse} #Unresolved"))? {
        let body = build_association_comment(req, &primary, collapse);
        tracker.comment(&existing, &body)?;
        return Ok(MintOutcome::AssociatedWithOpen { existing });
    }

    // Create the issue in the mapped project, bucket 2 pre-assigned to a human.
    // Routing order: explicit override, the project's VCS integration, fallback.
    let (project, source) =
        route_cache.resolve_project(tracker, cfg, req.repo, &forge_host(req.html_url));
    let summary = build_summary(req);
    let description = build_description(req, &primary, &secondary, collapse);
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
        source,
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
/// CLAUDE-104: an auto-fix failure gets a directive summary naming the concrete
/// problem (`Auto-fix: cargo fmt on ...`) instead of the generic `CI failure`.
fn build_summary(req: &MintRequest) -> String {
    let short_sha = short_sha(req.commit_sha);
    match &req.triage.auto_fix {
        Some(detail) => {
            let label = autofix_label(detail, req.workflow_id);
            format!(
                "Auto-fix: {} on {} (run #{}, {})",
                label, req.repo, req.index_in_repo, short_sha
            )
        }
        None => format!(
            "CI failure: {} on {} (run #{}, {})",
            req.workflow_id, req.repo, req.index_in_repo, short_sha
        ),
    }
}

/// First 8 chars of a commit SHA, the short form used throughout the issue body.
fn short_sha(sha: &str) -> String {
    sha.chars().take(8).collect()
}

/// Human label for an auto-fix in the summary: the precise `cargo fmt` for the
/// cargo-fmt detector, else the failing workflow filename.
fn autofix_label<'a>(detail: &'a crate::autofix::AutoFix, workflow_id: &'a str) -> &'a str {
    if detail.kind == "cargo-fmt" {
        "cargo fmt"
    } else {
        workflow_id
    }
}

/// Markdown issue body: the triage analysis, the run metadata, and the hidden
/// dedupe / reconciler markers a later mint (and the CLAUDE-191 auto-close
/// reconciler) searches for. `collapse` is the cross-run collapse key: the
/// CLAUDE-190 root-cause key, or the CLAUDE-264 branch-agnostic infra key when
/// an infrastructure condition was detected. It rides the `Dedupe-Key:` line (so
/// a later run's `{collapse} #Unresolved` full-text search matches this issue)
/// and is repeated as an explicit `Root-Cause-Key:` marker alongside `Branch:` /
/// `Job:`.
fn build_description(req: &MintRequest, primary: &str, secondary: &str, collapse: &str) -> String {
    // CLAUDE-104: an auto-fix failure gets an implementation-spec body (concrete
    // problem, fix command, new-branch + PR-vs-main delivery instruction) with
    // no "human must commit directly to main" wording.
    if let Some(detail) = &req.triage.auto_fix {
        return build_auto_fix_description(req, detail, primary, secondary, collapse);
    }
    let attached = if req.log_path.is_some() {
        "The redacted failing-job log is attached to this issue."
    } else {
        "No job log was available (transient miss); none attached."
    };
    let mut lines = vec![
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
        format!("- Branch: `{}`", req.branch),
        format!("- Commit: `{}`", req.commit_sha),
        format!("- Run: {}", req.html_url),
        String::new(),
        attached.to_string(),
        String::new(),
        "<!-- claude-run dedupe markers; do not edit -->".to_string(),
        format!("Dedupe-Key: {primary} {secondary} {collapse}"),
    ];
    push_reconciler_markers(&mut lines, req, collapse);
    lines.join("\n")
}

/// CLAUDE-190: append the hidden reconciler markers under the dedupe-marker
/// comment block. `Root-Cause-Key:` / `Branch:` / `Job:` let the auto-close
/// reconciler (CLAUDE-191) find every issue for a `(repo, branch, workflow,
/// job)` by full-text search, and the CLAUDE-198 duplicate-collapse groups on
/// `Root-Cause-Key:`. CLAUDE-264: that marker carries whichever key the mint
/// collapsed on, so an infra-keyed issue groups with its own siblings; the
/// `Infra-Kind:` line (present only for an infra failure) makes such an issue
/// identifiable without re-parsing the log. `Branch:` and `Job:` are unchanged,
/// because the reconciler still reads them to auto-close.
fn push_reconciler_markers(lines: &mut Vec<String>, req: &MintRequest, collapse: &str) {
    lines.push(format!("Root-Cause-Key: {collapse}"));
    lines.push(format!("Branch: {}", req.branch));
    lines.push(format!("Job: {}", req.job_name));
    if let Some(kind) = req.infra_kind {
        lines.push(format!("Infra-Kind: {kind}"));
    }
}

/// CLAUDE-104: the implementation-spec body for an auto-fixable failure. Names
/// the concrete problem and the fix command(s), instructs the runner to deliver
/// the fix on a fresh `chore/<kind>-<short-sha>` branch via a PR against `main`
/// (never a direct push to main), and carries the same dedupe markers as the
/// generic body so repeats still group.
fn build_auto_fix_description(
    req: &MintRequest,
    detail: &crate::autofix::AutoFix,
    primary: &str,
    secondary: &str,
    collapse: &str,
) -> String {
    let short = short_sha(req.commit_sha);
    let branch = format!("chore/{}-{}", detail.kind, short);
    let attached = if req.log_path.is_some() {
        "The redacted failing-job log is attached to this issue."
    } else {
        "No job log was attached."
    };
    let dedupe_keys = format!("Dedupe-Key: {primary} {secondary} {collapse}");

    // Fix step: the concrete command(s) when the detector knows them, else a
    // reproduce-and-fix instruction for the generic checks-workflow case.
    let fix_lines: Vec<String> = if detail.fix_commands.is_empty() {
        vec![format!(
            "- Reproduce the `{}` checks locally and fix every issue it reports.",
            req.workflow_id
        )]
    } else {
        detail
            .fix_commands
            .iter()
            .map(|c| format!("- Run `{c}`."))
            .collect()
    };

    let mut lines = vec![
        "## Auto-fix".to_string(),
        format!(
            "This is a mechanically-fixable verification failure (detector `{}`); it is auto-worked into a corrective PR rather than routed to a human.",
            detail.kind
        ),
        String::new(),
        "## Problem".to_string(),
        detail.problem.clone(),
    ];
    if let Some(scope) = &detail.scope {
        lines.push(format!("Failing step: `{scope}`."));
    }
    lines.push(String::new());
    lines.push("## Fix".to_string());
    lines.extend(fix_lines);
    lines.push(String::new());
    lines.push("## Delivery".to_string());
    lines.push(format!(
        "Commit the fix on a fresh branch named `{branch}` and open a pull request against `main` using the configured PR strategy. Do not push to `main` directly."
    ));
    lines.push(String::new());
    lines.push("## Failure".to_string());
    lines.push(format!("- Repo: `{}`", req.repo));
    lines.push(format!("- Workflow: `{}`", req.workflow_id));
    lines.push(format!("- Branch: `{}`", req.branch));
    lines.push(format!("- Commit: `{}`", req.commit_sha));
    lines.push(format!("- Run: {}", req.html_url));
    lines.push(String::new());
    lines.push(attached.to_string());
    lines.push(String::new());
    lines.push("<!-- claude-run dedupe markers; do not edit -->".to_string());
    lines.push(dedupe_keys);
    push_reconciler_markers(&mut lines, req, collapse);
    lines.join("\n")
}

/// CLAUDE-190: the comment posted onto an existing open issue when the same
/// root cause fails again on a later commit. Carries the new run's number, short
/// SHA, and run link, plus this run's own per-run `Dedupe-Key:` line (the
/// per-run `primary` and the `collapse` key) so a redelivery of THIS run
/// full-text matches and hits the per-run duplicate path (no second comment).
fn build_association_comment(req: &MintRequest, primary: &str, collapse: &str) -> String {
    let short_sha = req.commit_sha.chars().take(8).collect::<String>();
    [
        format!(
            "`{}` failed again on `{}` (run #{}, {}).",
            req.workflow_id, req.repo, req.index_in_repo, short_sha
        ),
        format!("- Run: {}", req.html_url),
        String::new(),
        "<!-- claude-run dedupe markers; do not edit -->".to_string(),
        format!("Dedupe-Key: {primary} {collapse}"),
    ]
    .join("\n")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::provider::{VcsMapEntry, VcsRepo};
    use crate::triage::{ClassifiedBy, Triage};
    use serde_json::Value;
    use std::collections::{HashMap, VecDeque};
    use std::sync::Mutex;

    /// Stub [`IssueTracker`] recording every mutation and returning canned
    /// `list_queued` / `vcs_repo_map` responses. Only the methods the mint calls
    /// are meaningful; the rest are unreachable in these tests.
    struct StubTracker {
        /// query -> JSON array string the dedupe search should return.
        searches: HashMap<String, String>,
        /// idReadable handed back by `create_issue`.
        new_key: String,
        /// Successive `vcs_repo_map` responses; the last is reused once drained.
        vcs_responses: Mutex<VecDeque<Vec<VcsMapEntry>>>,
        calls: Mutex<Vec<String>>,
    }

    impl StubTracker {
        fn new() -> Self {
            Self {
                searches: HashMap::new(),
                new_key: "CLAUDE-99".to_string(),
                vcs_responses: Mutex::new(VecDeque::new()),
                calls: Mutex::new(Vec::new()),
            }
        }
        fn with_search(mut self, query: &str, json_array: &str) -> Self {
            self.searches
                .insert(query.to_string(), json_array.to_string());
            self
        }
        /// One canned `vcs_repo_map` result, reused for every call.
        fn with_vcs(self, entries: Vec<VcsMapEntry>) -> Self {
            self.vcs_responses.lock().unwrap().push_back(entries);
            self
        }
        /// A sequence of `vcs_repo_map` results, one per successive call (the last
        /// repeats once the queue drains). Drives the miss-then-refresh path.
        fn with_vcs_sequence(self, seq: Vec<Vec<VcsMapEntry>>) -> Self {
            *self.vcs_responses.lock().unwrap() = seq.into();
            self
        }
        fn calls(&self) -> Vec<String> {
            self.calls.lock().unwrap().clone()
        }
        fn vcs_calls(&self) -> usize {
            self.calls
                .lock()
                .unwrap()
                .iter()
                .filter(|c| c.as_str() == "vcs_repo_map")
                .count()
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
        fn set_assignee(&self, key: &str, login: &str) -> Result<()> {
            self.calls
                .lock()
                .unwrap()
                .push(format!("set_assignee {key} {login}"));
            Ok(())
        }
        fn comment(&self, key: &str, body: &str) -> Result<()> {
            self.calls
                .lock()
                .unwrap()
                .push(format!("comment {key} | {body}"));
            Ok(())
        }
        fn issue_links(&self, _key: &str) -> Result<Vec<crate::provider::IssueLink>> {
            unreachable!("mint does not read issue links")
        }
        fn inspect_issue(&self, _key: &str) -> Result<crate::provider::IssueInspect> {
            unreachable!("mint does not inspect issues")
        }
        fn resolve_vcs(&self, _project_key: &str) -> Result<VcsRepo> {
            unreachable!("mint does not resolve vcs")
        }
        fn vcs_repo_map(&self) -> Result<Vec<VcsMapEntry>> {
            self.calls.lock().unwrap().push("vcs_repo_map".to_string());
            let mut q = self.vcs_responses.lock().unwrap();
            Ok(if q.len() > 1 {
                q.pop_front().unwrap()
            } else {
                q.front().cloned().unwrap_or_default()
            })
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

    /// A route cache with a long TTL (refresh-on-miss is exercised explicitly).
    fn route_cache() -> VcsRouteCache {
        VcsRouteCache::new(Duration::from_secs(900))
    }

    fn entry(server_url: &str, path: &str, project: &str) -> VcsMapEntry {
        VcsMapEntry {
            server_url: server_url.to_string(),
            path: path.to_string(),
            project_key: project.to_string(),
        }
    }

    fn triage(bucket: TriageBucket) -> Triage {
        Triage {
            bucket,
            analysis: "root cause and next step".to_string(),
            classified_by: ClassifiedBy::Llm,
            auto_fix: None,
        }
    }

    /// CLAUDE-104: an auto-fix triage verdict carrying detector detail.
    fn auto_fix_triage(detail: crate::autofix::AutoFix) -> Triage {
        Triage {
            bucket: TriageBucket::AutoFix,
            analysis: detail.problem.clone(),
            classified_by: ClassifiedBy::Rules,
            auto_fix: Some(detail),
        }
    }

    fn cargo_fmt_detail() -> crate::autofix::AutoFix {
        crate::autofix::AutoFix {
            kind: "cargo-fmt".to_string(),
            problem: "rustfmt found unformatted code: `cargo fmt --check` reported diffs."
                .to_string(),
            fix_commands: vec!["cargo fmt --all".to_string()],
            scope: None,
        }
    }

    fn req<'a>(repo: &'a str, t: &'a Triage, user: &'a str) -> MintRequest<'a> {
        MintRequest {
            repo,
            index_in_repo: 7,
            commit_sha: "abc123def456",
            workflow_id: "ci.yml",
            branch: "main",
            job_name: "check",
            failure_signature: None,
            infra_kind: None,
            html_url: "https://dev.a8n.run/owner/repo/actions/runs/7",
            trigger_user: user,
            triage: t,
            log_path: None,
            private: false,
            has_open_pr: false,
            auto_fix_enabled: true,
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
        // CLAUDE-104: an auto-fix routes like a code-fix (queued, no human).
        assert_eq!(
            route(TriageBucket::AutoFix),
            Some(Route {
                queue_for_fix: true,
                assign_human: false
            })
        );
        assert_eq!(route(TriageBucket::Transient), None);
    }

    // ---- CLAUDE-86: VCS-integration routing -------------------------------

    #[test]
    fn route_order_override_beats_vcs_beats_fallback() {
        let c = cfg(); // override: fabrikam/forgejo-cli -> FJ
        let tracker = StubTracker::new().with_vcs(vec![entry(
            "https://dev.a8n.run",
            "fabrikam/forgejo-cli",
            "WRONG",
        )]);
        let r = route_cache();
        // Override wins even when a VCS entry also claims the repo.
        assert_eq!(
            r.resolve_project(&tracker, &c, "fabrikam/forgejo-cli", "dev.a8n.run"),
            ("FJ".to_string(), RouteSource::Override)
        );
        // No override -> VCS claim wins over the fallback.
        let tracker = StubTracker::new().with_vcs(vec![entry(
            "https://dev.a8n.run",
            "fabrikam/gizmo",
            "GIZMO",
        )]);
        assert_eq!(
            route_cache().resolve_project(&tracker, &c, "fabrikam/gizmo", "dev.a8n.run"),
            ("GIZMO".to_string(), RouteSource::Vcs)
        );
        // No override, no VCS claim -> fallback.
        let tracker = StubTracker::new().with_vcs(vec![]);
        assert_eq!(
            route_cache().resolve_project(&tracker, &c, "who/knows", "dev.a8n.run"),
            ("CLAUDE".to_string(), RouteSource::Fallback)
        );
    }

    #[test]
    fn vcs_lookup_is_case_and_trailing_slash_insensitive() {
        // Processor path is lowercased and serverUrl carries a trailing slash;
        // the payload carries case-preserved values: matching must ignore both.
        let map = VcsRouteMap::from_entries(vec![entry(
            "https://dev.a8n.run/",
            "fabrikam/youtrack-mcp",
            "YTMCP",
        )]);
        assert_eq!(
            map.lookup("DEV.A8N.RUN", "Fabrikam/YouTrack-MCP"),
            Some("YTMCP".to_string())
        );
        assert_eq!(
            map.lookup("https://dev.a8n.run", "fabrikam/youtrack-mcp"),
            Some("YTMCP".to_string())
        );
        // A different host with the same path does not match.
        assert_eq!(
            map.lookup("other.example", "fabrikam/youtrack-mcp"),
            None
        );
    }

    #[test]
    fn ambiguous_claim_is_deterministic_first_by_short_name() {
        // Two projects claim the same repo: pick the first short name ascending.
        let map = VcsRouteMap::from_entries(vec![
            entry("https://dev.a8n.run", "owner/repo", "ZED"),
            entry("https://dev.a8n.run", "owner/repo", "ALPHA"),
        ]);
        assert_eq!(
            map.lookup("dev.a8n.run", "owner/repo"),
            Some("ALPHA".to_string())
        );
    }

    #[test]
    fn cached_map_serves_steady_state_without_extra_calls() {
        let c = cfg();
        let tracker = StubTracker::new().with_vcs(vec![entry(
            "https://dev.a8n.run",
            "fabrikam/gizmo",
            "GIZMO",
        )]);
        let r = route_cache();
        // First resolve builds the map; the second reuses the cache.
        assert_eq!(
            r.resolve_project(&tracker, &c, "fabrikam/gizmo", "dev.a8n.run")
                .0,
            "GIZMO"
        );
        assert_eq!(
            r.resolve_project(&tracker, &c, "fabrikam/gizmo", "dev.a8n.run")
                .0,
            "GIZMO"
        );
        assert_eq!(
            tracker.vcs_calls(),
            1,
            "steady-state issues no extra rebuild"
        );
    }

    #[test]
    fn miss_refreshes_once_before_falling_back() {
        let c = cfg();
        // First build returns an empty map (repo not yet integrated); the
        // refresh-on-miss rebuild returns the now-present claim.
        let tracker = StubTracker::new().with_vcs_sequence(vec![
            vec![],
            vec![entry("https://dev.a8n.run", "fabrikam/gizmo", "GIZMO")],
        ]);
        // Pre-warm the cache so the lookup is served from cache (not a fresh
        // build), which is what arms the refresh-on-miss.
        let r = route_cache();
        let _ = r.resolve_project(&tracker, &c, "fabrikam/gizmo", "dev.a8n.run");
        let (project, source) =
            r.resolve_project(&tracker, &c, "fabrikam/gizmo", "dev.a8n.run");
        assert_eq!((project.as_str(), source), ("GIZMO", RouteSource::Vcs));
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

    // ---- CLAUDE-204: no Run-Global-Id marker ------------------------------

    #[test]
    fn description_emits_no_run_global_id_marker() {
        // CLAUDE-204: `fj actions logs` addresses a run by its per-repo number,
        // so nothing reads a global id and the marker is gone from every body.
        let t = triage(TriageBucket::CodeFix);
        let r = req("owner/repo", &t, "alice");
        let p = primary_marker(&r);
        let s = secondary_marker(&r);
        let rc = root_cause_marker(&r);
        assert!(!build_description(&r, &p, &s, &rc).contains("Run-Global-Id"));

        // The auto-fix body carries none either.
        let af = auto_fix_triage(cargo_fmt_detail());
        let r = req("owner/repo", &af, "alice");
        assert!(!build_description(&r, &p, &s, &rc).contains("Run-Global-Id"));
    }

    // ---- AC 5: bucket 3 mints nothing -------------------------------------

    #[test]
    fn transient_bucket_mints_nothing() {
        let t = triage(TriageBucket::Transient);
        let tracker = StubTracker::new();
        let out = mint(
            &tracker,
            &cfg(),
            &route_cache(),
            &req("owner/repo", &t, "alice"),
            &caps(),
        )
        .unwrap();
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
            &route_cache(),
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
                source: RouteSource::Override,
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
            &route_cache(),
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
                source: RouteSource::Fallback,
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

    // ---- CLAUDE-149: a failing open PR is left to its author --------------

    #[test]
    fn open_pr_suppresses_the_auto_fix_mint() {
        // A code-fix failure on a run WITH an open PR is the author's to fix:
        // the queue-for-fix route is suppressed and nothing is minted.
        let t = triage(TriageBucket::CodeFix);
        let tracker = StubTracker::new();
        let mut r = req("fabrikam/forgejo-cli", &t, "alice");
        r.has_open_pr = true;
        let out = mint(&tracker, &cfg(), &route_cache(), &r, &caps()).unwrap();
        assert_eq!(out, MintOutcome::SkippedOpenPr);
        assert!(
            tracker.calls().is_empty(),
            "no yt calls for an open-PR failure: {:?}",
            tracker.calls()
        );
    }

    #[test]
    fn open_pr_does_not_suppress_a_human_action_mint() {
        // Only the queue-for-fix (auto-fix) route is suppressed for an open PR.
        // A human-action failure still needs a human, so it mints and assigns.
        let t = triage(TriageBucket::HumanAction);
        let tracker = StubTracker::new();
        let mut r = req("contoso/claude-run", &t, "alice");
        r.has_open_pr = true;
        let out = mint(&tracker, &cfg(), &route_cache(), &r, &caps()).unwrap();
        assert!(
            matches!(out, MintOutcome::Created { .. }),
            "human-action on an open PR still mints: {out:?}"
        );
        assert!(
            tracker.calls().iter().any(|c| c.contains("assignee=alice")),
            "{:?}",
            tracker.calls()
        );
    }

    #[test]
    fn no_open_pr_keeps_the_auto_fix_mint() {
        // The suppression is gated on the open PR: with no open PR (the canonical
        // push-to-main / branch case) a code-fix still queues as before.
        let t = triage(TriageBucket::CodeFix);
        let tracker = StubTracker::new();
        let r = req("fabrikam/forgejo-cli", &t, "alice"); // has_open_pr: false
        let out = mint(&tracker, &cfg(), &route_cache(), &r, &caps()).unwrap();
        assert!(matches!(out, MintOutcome::Created { .. }), "{out:?}");
        assert!(
            tracker
                .calls()
                .iter()
                .any(|c| c == "set_field CLAUDE-99 AI Agent Queued"),
            "{:?}",
            tracker.calls()
        );
    }

    // ---- CLAUDE-153: the webhook-scoped auto-fix toggle -------------------

    #[test]
    fn auto_fix_off_suppresses_the_queue_for_fix_mint() {
        // With the webhook auto-fix toggle off, a code-fix failure mints nothing
        // and queues no fixup PR (CLAUDE-152): the failure stays on the PR.
        let t = triage(TriageBucket::CodeFix);
        let tracker = StubTracker::new();
        let mut r = req("fabrikam/forgejo-cli", &t, "alice");
        r.auto_fix_enabled = false;
        let out = mint(&tracker, &cfg(), &route_cache(), &r, &caps()).unwrap();
        assert_eq!(out, MintOutcome::SkippedAutoFixDisabled);
        assert!(
            tracker.calls().is_empty(),
            "no yt calls when auto-fix is off: {:?}",
            tracker.calls()
        );
    }

    #[test]
    fn auto_fix_off_also_suppresses_the_auto_fix_bucket() {
        // The `auto_fix` bucket routes like a code-fix (queue-for-fix), so the
        // toggle gates it too: no mint, no queued fixup.
        let t = auto_fix_triage(cargo_fmt_detail());
        let tracker = StubTracker::new();
        let mut r = req("fabrikam/forgejo-cli", &t, "alice");
        r.auto_fix_enabled = false;
        let out = mint(&tracker, &cfg(), &route_cache(), &r, &caps()).unwrap();
        assert_eq!(out, MintOutcome::SkippedAutoFixDisabled);
        assert!(tracker.calls().is_empty(), "{:?}", tracker.calls());
    }

    #[test]
    fn auto_fix_off_still_mints_a_human_action() {
        // The toggle gates only the queue-for-fix buckets. A human-action failure
        // needs a human either way, so it still mints and assigns.
        let t = triage(TriageBucket::HumanAction);
        let tracker = StubTracker::new();
        let mut r = req("contoso/claude-run", &t, "alice");
        r.auto_fix_enabled = false;
        let out = mint(&tracker, &cfg(), &route_cache(), &r, &caps()).unwrap();
        assert!(
            matches!(out, MintOutcome::Created { .. }),
            "human-action mints even with auto-fix off: {out:?}"
        );
        assert!(
            tracker.calls().iter().any(|c| c.contains("assignee=alice")),
            "{:?}",
            tracker.calls()
        );
    }

    #[test]
    fn auto_fix_on_keeps_the_queue_for_fix_mint() {
        // With the toggle on, a code-fix queues for the fix bot as before.
        let t = triage(TriageBucket::CodeFix);
        let tracker = StubTracker::new();
        let mut r = req("fabrikam/forgejo-cli", &t, "alice");
        r.auto_fix_enabled = true;
        let out = mint(&tracker, &cfg(), &route_cache(), &r, &caps()).unwrap();
        assert!(matches!(out, MintOutcome::Created { .. }), "{out:?}");
        assert!(
            tracker
                .calls()
                .iter()
                .any(|c| c == "set_field CLAUDE-99 AI Agent Queued"),
            "{:?}",
            tracker.calls()
        );
    }

    // ---- AC 1: the redacted log is attached when present -------------------

    #[test]
    fn created_issue_attaches_the_redacted_log() {
        let t = triage(TriageBucket::CodeFix);
        let tracker = StubTracker::new();
        let mut r = req("contoso/claude-run", &t, "alice");
        let log = Path::new("/tmp/redacted.log");
        r.log_path = Some(log);
        mint(&tracker, &cfg(), &route_cache(), &r, &caps()).unwrap();
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
            &route_cache(),
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
        let out = mint(
            &tracker,
            &cfg(),
            &route_cache(),
            &req("owner/repo", &t, "alice"),
            &capped,
        )
        .unwrap();
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
        let out = mint(
            &tracker,
            &cfg(),
            &route_cache(),
            &req("owner/repo", &t, "alice"),
            &capped,
        )
        .unwrap();
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
        let out = mint(&tracker, &cfg(), &route_cache(), &r, &caps()).unwrap();
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
        let out = mint(&tracker, &cfg(), &route_cache(), &r, &caps()).unwrap();
        assert_eq!(
            out,
            MintOutcome::SkippedResolved {
                existing: "CLAUDE-40".to_string()
            }
        );
        assert!(!tracker.calls().iter().any(|c| c.starts_with("create")));
    }

    // ---- CLAUDE-190: root-cause cross-run collapse ------------------------

    /// A request carrying a derived failure signature (the common case).
    fn req_with_sig<'a>(
        repo: &'a str,
        t: &'a Triage,
        user: &'a str,
        sig: &'a str,
    ) -> MintRequest<'a> {
        let mut r = req(repo, t, user);
        r.failure_signature = Some(sig);
        r
    }

    #[test]
    fn root_cause_collapses_repeats_across_commits() {
        // AC 4: a later commit that fails the same job on the same branch with
        // the same signature finds the OPEN root-cause issue, appends a
        // run-association comment, and mints nothing (regardless of commit SHA).
        let t = triage(TriageBucket::CodeFix);
        let r = req_with_sig(
            "owner/repo",
            &t,
            "alice",
            "assertion failed: expected # got #",
        );
        let root_cause = root_cause_marker(&r);
        let tracker = StubTracker::new().with_search(
            &format!("{root_cause} #Unresolved"),
            r#"[{"idReadable":"CLAUDE-60","summary":"CI failure: ci.yml"}]"#,
        );
        let out = mint(&tracker, &cfg(), &route_cache(), &r, &caps()).unwrap();
        assert_eq!(
            out,
            MintOutcome::AssociatedWithOpen {
                existing: "CLAUDE-60".to_string()
            }
        );
        let calls = tracker.calls();
        assert!(
            !calls.iter().any(|c| c.starts_with("create")),
            "no second issue minted: {calls:?}"
        );
        // The comment carries this run's per-run primary and the root-cause key.
        let primary = primary_marker(&r);
        assert!(
            calls.iter().any(|c| c.starts_with("comment CLAUDE-60 |")
                && c.contains(&primary)
                && c.contains(&root_cause)),
            "comment carries per-run + root-cause keys: {calls:?}"
        );
    }

    #[test]
    fn different_signatures_on_same_job_mint_two_issues() {
        // AC 5: same branch / workflow / job but a DIFFERENT signature is a
        // different root cause -> a distinct key -> a second issue is minted.
        let t = triage(TriageBucket::CodeFix);
        // The open issue is keyed on signature A.
        let a = req_with_sig("owner/repo", &t, "alice", "error[e#]: mismatched types");
        let key_a = root_cause_marker(&a);
        // The new failure carries signature B, so its root-cause search misses.
        let b = req_with_sig("owner/repo", &t, "alice", "error[e#]: cannot find value");
        let tracker = StubTracker::new().with_search(
            &format!("{key_a} #Unresolved"),
            r#"[{"idReadable":"CLAUDE-60"}]"#,
        );
        let out = mint(&tracker, &cfg(), &route_cache(), &b, &caps()).unwrap();
        assert!(
            matches!(out, MintOutcome::Created { .. }),
            "a different signature mints a sibling: {out:?}"
        );
        assert_ne!(key_a, root_cause_marker(&b), "distinct keys per signature");
    }

    #[test]
    fn resolved_root_cause_key_is_never_reopened() {
        // AC 6: a RESOLVED issue for the root-cause key is not reopened.
        let t = triage(TriageBucket::CodeFix);
        let r = req_with_sig("owner/repo", &t, "alice", "panicked at boom");
        let root_cause = root_cause_marker(&r);
        let tracker = StubTracker::new().with_search(
            &format!("{root_cause} #Resolved"),
            r#"[{"idReadable":"CLAUDE-40"}]"#,
        );
        let out = mint(&tracker, &cfg(), &route_cache(), &r, &caps()).unwrap();
        assert_eq!(
            out,
            MintOutcome::SkippedResolved {
                existing: "CLAUDE-40".to_string()
            }
        );
        assert!(!tracker.calls().iter().any(|c| c.starts_with("create")));
    }

    #[test]
    fn no_signature_fallback_key_still_collapses_the_storm() {
        // AC 3 fallback: with no derivable signature the key degrades to
        // (repo, branch, workflow, job), which still collapses a logless
        // "branch is broken" storm onto one open issue.
        let t = triage(TriageBucket::HumanAction);
        let r = req("owner/repo", &t, "alice"); // failure_signature: None
        let root_cause = root_cause_marker(&r);
        // The fallback key is job-scoped, not commit-scoped.
        assert_eq!(
            root_cause,
            marker(&["owner/repo", "main", "ci.yml", "check"])
        );
        let tracker = StubTracker::new().with_search(
            &format!("{root_cause} #Unresolved"),
            r#"[{"idReadable":"CLAUDE-60"}]"#,
        );
        let out = mint(&tracker, &cfg(), &route_cache(), &r, &caps()).unwrap();
        assert_eq!(
            out,
            MintOutcome::AssociatedWithOpen {
                existing: "CLAUDE-60".to_string()
            }
        );
    }

    #[test]
    fn per_run_redelivery_takes_precedence_over_root_cause_association() {
        // Ordering: an open per-run match short-circuits before the root-cause
        // search, so an exact-run redelivery never comments a second time.
        let t = triage(TriageBucket::CodeFix);
        let r = req_with_sig("owner/repo", &t, "alice", "boom");
        let primary = primary_marker(&r);
        let root_cause = root_cause_marker(&r);
        let tracker = StubTracker::new()
            .with_search(
                &format!("{primary} #Unresolved"),
                r#"[{"idReadable":"CLAUDE-50"}]"#,
            )
            .with_search(
                &format!("{root_cause} #Unresolved"),
                r#"[{"idReadable":"CLAUDE-60"}]"#,
            );
        let out = mint(&tracker, &cfg(), &route_cache(), &r, &caps()).unwrap();
        assert_eq!(
            out,
            MintOutcome::SkippedDuplicateOpen {
                existing: "CLAUDE-50".to_string()
            }
        );
        let calls = tracker.calls();
        assert!(!calls.iter().any(|c| c.starts_with("comment")), "{calls:?}");
        assert!(!calls.iter().any(|c| c.starts_with("create")), "{calls:?}");
    }

    // ---- CLAUDE-190: signature extraction + normalization -----------------

    #[test]
    fn signature_is_stable_across_commits_and_splits_on_cause() {
        // The same root cause on two different commits (paths / line numbers
        // differ) yields the SAME signature; a different diagnostic differs.
        let a = failure_signature(
            "   Compiling foo\nerror[E0308]: mismatched types\n  --> src/a.rs:12:9\n",
        )
        .unwrap();
        let b = failure_signature(
            "   Compiling foo\nerror[E0308]: mismatched types\n  --> src/other/b.rs:99:1\n",
        )
        .unwrap();
        assert_eq!(a, b, "same cause across commits -> same signature");
        let c = failure_signature("error[E0425]: cannot find value `x`\n").unwrap();
        assert_ne!(a, c, "different cause -> different signature");
    }

    #[test]
    fn signature_normalizes_paths_and_numbers() {
        let s =
            failure_signature("thread 'main' panicked at /home/ci/work/src/lib.rs:42:5\n").unwrap();
        // Path basename kept, digits masked to '#', lowercased.
        assert!(s.contains("lib.rs"), "basename kept: {s}");
        assert!(!s.contains("/home/ci"), "path prefix dropped: {s}");
        assert!(!s.chars().any(|c| c.is_ascii_digit()), "digits masked: {s}");
        assert_eq!(s, s.to_ascii_lowercase(), "lowercased: {s}");
    }

    #[test]
    fn signature_none_when_no_diagnostic_line() {
        // A log carrying no diagnostic line yields no signature (fallback key).
        assert!(failure_signature("Compiling...\nBuilding...\nDone.\n").is_none());
        assert!(failure_signature("").is_none());
    }

    #[test]
    fn description_embeds_root_cause_branch_and_job_markers() {
        // AC 7: the body carries hidden Root-Cause-Key / Branch / Job markers and
        // the root-cause key rides the Dedupe-Key line (searchable).
        let t = triage(TriageBucket::CodeFix);
        let r = req_with_sig("owner/repo", &t, "alice", "boom");
        let p = primary_marker(&r);
        let s = secondary_marker(&r);
        let rc = root_cause_marker(&r);
        let body = build_description(&r, &p, &s, &rc);
        assert!(
            body.contains(&format!("Dedupe-Key: {p} {s} {rc}")),
            "{body}"
        );
        assert!(body.contains(&format!("Root-Cause-Key: {rc}")), "{body}");
        assert!(body.contains("Branch: main"), "{body}");
        assert!(body.contains("Job: check"), "{body}");
    }

    // ---- CLAUDE-264: branch-agnostic infrastructure collapse --------------

    /// A request carrying a detected infrastructure kind, on a named branch and
    /// commit (the two fields the infra key must ignore).
    fn req_infra<'a>(
        t: &'a Triage,
        kind: &'a str,
        branch: &'a str,
        commit_sha: &'a str,
    ) -> MintRequest<'a> {
        let mut r = req("owner/repo", t, "alice");
        r.infra_kind = Some(kind);
        r.branch = branch;
        r.commit_sha = commit_sha;
        r
    }

    #[test]
    fn infra_marker_ignores_branch_and_commit() {
        // The whole point: two requests differing ONLY in branch and commit key
        // identically, so one host condition on one job is one issue.
        let t = triage(TriageBucket::HumanAction);
        let a = req_infra(
            &t,
            "disk-full",
            "fix/PMS-1041-invoice-module-gate",
            "aaaa1111",
        );
        let b = req_infra(&t, "disk-full", "feat/PMS-1126-kb-provenance", "bbbb2222");
        assert_eq!(infra_marker(&a), infra_marker(&b));
        assert_eq!(
            infra_marker(&a),
            Some(marker(&["owner/repo", "ci.yml", "check", "disk-full"]))
        );
        // The branch-scoped root-cause key still differs, which is exactly why
        // these two never collapsed before.
        assert_ne!(root_cause_marker(&a), root_cause_marker(&b));
        // No infra kind means no infra marker at all.
        assert_eq!(infra_marker(&req("owner/repo", &t, "alice")), None);
    }

    #[test]
    fn two_branches_with_the_same_infra_kind_mint_one_issue() {
        // The live PMS case: run on branch A minted the issue; the run on
        // branch B finds it through the branch-agnostic key, comments, and
        // creates nothing.
        let t = triage(TriageBucket::HumanAction);
        let first = req_infra(
            &t,
            "disk-full",
            "fix/PMS-1041-invoice-module-gate",
            "aaaa1111",
        );
        let infra = infra_marker(&first).unwrap();
        let second = req_infra(&t, "disk-full", "feat/PMS-1126-kb-provenance", "bbbb2222");
        let tracker = StubTracker::new().with_search(
            &format!("{infra} #Unresolved"),
            r#"[{"idReadable":"PMS-1099"}]"#,
        );
        let out = mint(&tracker, &cfg(), &route_cache(), &second, &caps()).unwrap();
        assert_eq!(
            out,
            MintOutcome::AssociatedWithOpen {
                existing: "PMS-1099".to_string()
            }
        );
        let calls = tracker.calls();
        assert!(
            !calls.iter().any(|c| c.starts_with("create")),
            "no second issue minted: {calls:?}"
        );
        assert!(
            calls
                .iter()
                .any(|c| c.starts_with("comment PMS-1099 |") && c.contains(&infra)),
            "run-association comment carries the infra key: {calls:?}"
        );
    }

    #[test]
    fn different_infra_kinds_on_the_same_job_mint_two_issues() {
        // A full disk and an OOM kill are separate conditions needing separate
        // tasks, even on the same job and the same branch.
        let t = triage(TriageBucket::HumanAction);
        let disk = req_infra(&t, "disk-full", "main", "aaaa1111");
        let oom = req_infra(&t, "out-of-memory", "main", "aaaa1111");
        assert_ne!(infra_marker(&disk), infra_marker(&oom));
        let tracker = StubTracker::new().with_search(
            &format!("{} #Unresolved", infra_marker(&disk).unwrap()),
            r#"[{"idReadable":"PMS-1099"}]"#,
        );
        let out = mint(&tracker, &cfg(), &route_cache(), &oom, &caps()).unwrap();
        assert!(
            matches!(out, MintOutcome::Created { .. }),
            "a different infra kind mints a sibling: {out:?}"
        );
    }

    #[test]
    fn a_resolved_infra_issue_does_not_suppress_a_later_mint() {
        // Deliberate exception to the never-reopen rule: a host condition
        // recurs, and each recurrence needs its own task. Suppressing on a
        // months-old resolved disk-full issue would silently stop reporting it.
        let t = triage(TriageBucket::HumanAction);
        let r = req_infra(&t, "disk-full", "main", "aaaa1111");
        let infra = infra_marker(&r).unwrap();
        let tracker = StubTracker::new().with_search(
            &format!("{infra} #Resolved"),
            r#"[{"idReadable":"PMS-741"}]"#,
        );
        let out = mint(&tracker, &cfg(), &route_cache(), &r, &caps()).unwrap();
        assert!(
            matches!(out, MintOutcome::Created { .. }),
            "a resolved infra issue never suppresses the next occurrence: {out:?}"
        );
        // The infra key is never searched with #Resolved at all.
        assert!(
            !tracker
                .calls()
                .iter()
                .any(|c| c == &format!("list {infra} #Resolved")),
            "{:?}",
            tracker.calls()
        );
    }

    #[test]
    fn infra_body_carries_the_infra_key_and_kind_markers() {
        // The Root-Cause-Key marker holds the INFRA key (so the CLAUDE-198
        // duplicate-collapse groups on it), plus an Infra-Kind marker; Branch
        // and Job are untouched, because the reconciler reads them to close.
        let t = triage(TriageBucket::HumanAction);
        let r = req_infra(&t, "disk-full", "feat/x", "aaaa1111");
        let infra = infra_marker(&r).unwrap();
        let body = build_description(&r, &primary_marker(&r), &secondary_marker(&r), &infra);
        assert!(body.contains(&format!("Root-Cause-Key: {infra}")), "{body}");
        assert!(body.contains("Infra-Kind: disk-full"), "{body}");
        assert!(body.contains("Branch: feat/x"), "{body}");
        assert!(body.contains("Job: check"), "{body}");
        assert!(
            !body.contains(&root_cause_marker(&r)),
            "the branch-scoped key is replaced, not added: {body}"
        );
    }

    #[test]
    fn no_infra_kind_leaves_the_body_and_the_keys_untouched() {
        // The byte-identical guarantee for a code failure: no Infra-Kind line,
        // and the collapse key is still the branch-scoped root-cause key.
        let t = triage(TriageBucket::CodeFix);
        let r = req_with_sig("owner/repo", &t, "alice", "boom");
        let rc = root_cause_marker(&r);
        let body = build_description(&r, &primary_marker(&r), &secondary_marker(&r), &rc);
        assert!(!body.contains("Infra-Kind:"), "{body}");
        assert!(body.contains(&format!("Root-Cause-Key: {rc}")), "{body}");
    }

    // ---- CLAUDE-104: auto-fix minting -------------------------------------

    #[test]
    fn auto_fix_creates_a_queued_issue_with_no_human_assignee() {
        // The auto-fix bucket mints and queues for the runner exactly like a
        // code-fix, even though the canonical case has no open PR / is a push to
        // main; no human is assigned.
        let t = auto_fix_triage(cargo_fmt_detail());
        let tracker = StubTracker::new();
        let out = mint(
            &tracker,
            &cfg(),
            &route_cache(),
            &req("fabrikam/forgejo-cli", &t, "alice"),
            &caps(),
        )
        .unwrap();
        assert_eq!(
            out,
            MintOutcome::Created {
                key: "CLAUDE-99".to_string(),
                project: "FJ".to_string(),
                bucket: TriageBucket::AutoFix,
                source: RouteSource::Override,
            }
        );
        let calls = tracker.calls();
        assert!(calls.iter().any(|c| c.contains("assignee=-")), "{calls:?}");
        assert!(
            calls
                .iter()
                .any(|c| c == "set_field CLAUDE-99 AI Agent Queued"),
            "{calls:?}"
        );
    }

    #[test]
    fn auto_fix_summary_names_the_concrete_problem() {
        // AC 4: an `Auto-fix: cargo fmt on ...` summary, not the generic
        // `CI failure` template.
        let t = auto_fix_triage(cargo_fmt_detail());
        let r = req("contoso/claude-run", &t, "alice");
        let summary = build_summary(&r);
        assert_eq!(
            summary,
            "Auto-fix: cargo fmt on contoso/claude-run (run #7, abc123de)"
        );
        // A non-auto-fix bucket keeps the generic summary.
        let plain = triage(TriageBucket::CodeFix);
        let pr = req("contoso/claude-run", &plain, "alice");
        assert!(build_summary(&pr).starts_with("CI failure: ci.yml"));
    }

    #[test]
    fn auto_fix_description_states_fix_command_and_pr_delivery() {
        // AC 4: the body names the fix command and the open-a-PR-vs-main
        // delivery, on a `chore/cargo-fmt-<short-sha>` branch, with no
        // "human must commit directly to main" wording.
        let t = auto_fix_triage(cargo_fmt_detail());
        let r = req("contoso/claude-run", &t, "alice");
        let primary = primary_marker(&r);
        let secondary = secondary_marker(&r);
        let root_cause = root_cause_marker(&r);
        let body = build_description(&r, &primary, &secondary, &root_cause);
        assert!(body.contains("cargo fmt --all"), "fix command: {body}");
        assert!(
            body.contains("chore/cargo-fmt-abc123de"),
            "branch name: {body}"
        );
        assert!(
            body.contains("open a pull request against `main`"),
            "PR delivery: {body}"
        );
        assert!(
            body.contains("Do not push to `main` directly."),
            "no-direct-push: {body}"
        );
        assert!(
            !body.to_lowercase().contains("human must commit"),
            "no human-must-commit wording: {body}"
        );
        // The dedupe markers still ride the body so repeats group (AC 7).
        assert!(body.contains(&primary) && body.contains(&secondary));
        assert!(body.contains(&root_cause), "root-cause key in body: {body}");
    }

    #[test]
    fn auto_fix_description_handles_a_checks_workflow_without_fix_commands() {
        // The generic checks-workflow detector carries no fix command, so the
        // body instructs a reproduce-and-fix and uses the workflow-kind branch.
        let detail = crate::autofix::AutoFix {
            kind: "checks-workflow".to_string(),
            problem: "the `check.yml` checks workflow failed at step `Clippy`; ...".to_string(),
            fix_commands: Vec::new(),
            scope: Some("Clippy".to_string()),
        };
        let t = auto_fix_triage(detail);
        let r = req("contoso/claude-run", &t, "alice");
        let primary = primary_marker(&r);
        let secondary = secondary_marker(&r);
        let root_cause = root_cause_marker(&r);
        let body = build_description(&r, &primary, &secondary, &root_cause);
        assert!(body.contains("Reproduce the `ci.yml` checks"), "{body}");
        assert!(body.contains("chore/checks-workflow-abc123de"), "{body}");
        assert!(body.contains("Failing step: `Clippy`."), "{body}");
    }
}
