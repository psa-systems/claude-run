//! Action YAML loader.
//!
//! Schema mirrors `actions/SCHEMA.md`. Each script-bearing field holds opaque
//! code plus an optional interpreter; the runner shells out to that interpreter
//! (default `nu`) to execute it (see `crate::snippet`).

use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::error::{Error, Result};

/// A script-bearing field (`input.command`, `filter`, each `pre`, `prompt`,
/// each `post`). Accepted in two forms:
///
/// - a plain YAML string (implicit `shell: nu`) for back-compat, or
/// - a `{run: <code>, shell: <interpreter>}` mapping, following Docker
///   Compose's `run` / `shell` convention.
///
/// `shell` defaults to `nu` when omitted. The interpreter is a bare binary
/// name resolved on `PATH` (no arguments).
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(untagged)]
pub enum Script {
    /// Plain string form: code with the default `nu` interpreter.
    Plain(String),
    /// Explicit mapping form: code plus an optional interpreter.
    Mapping {
        run: String,
        #[serde(default)]
        shell: Option<String>,
    },
}

impl Script {
    /// The code to execute.
    pub fn code(&self) -> &str {
        match self {
            Script::Plain(s) => s,
            Script::Mapping { run, .. } => run,
        }
    }

    /// The declared interpreter, or `None` to use the runner's default (`nu`).
    pub fn shell(&self) -> Option<&str> {
        match self {
            Script::Plain(_) => None,
            Script::Mapping { shell, .. } => shell.as_deref(),
        }
    }
}

impl From<String> for Script {
    fn from(s: String) -> Self {
        Script::Plain(s)
    }
}

impl From<&str> for Script {
    fn from(s: &str) -> Self {
        Script::Plain(s.to_string())
    }
}

/// How an action is executed (CLAUDE-30 dispatch discriminator).
///
/// `scripted` (the default, kept for back-compat) runs the generic nu-snippet
/// pipeline `input.command` -> `filter` -> `pre` -> `prompt` -> `claude` ->
/// `post`. It is **deprecated**: new actions should prefer a native kind once
/// one exists for their source. Native kinds (currently only `youtrack`) route
/// to a built-in Rust handler and supply a typed [`NativeConfig`] under
/// `native` instead of the script-bearing fields.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ActionKind {
    /// Generic nu-snippet pipeline (deprecated). Default for back-compat.
    #[default]
    Scripted,
    /// Built-in YouTrack handler. Config lives in `native` as
    /// [`NativeConfig::Youtrack`]; the real handler lands in CLAUDE-33.
    Youtrack,
    /// Built-in Forgejo CI-fix handler (CLAUDE-29, supersedes DEV-330). Config
    /// lives in `native` as [`NativeConfig::ForgejoCiFix`].
    #[serde(rename = "forgejo-ci-fix")]
    ForgejoCiFix,
    /// Built-in CI-failure-analysis handler (CLAUDE-189): enriches each open "CI
    /// failure" / "Auto-fix" issue with a deep, log-grounded root-cause comment.
    /// Config lives in `native` as [`NativeConfig::CiFailureAnalysis`].
    #[serde(rename = "ci-failure-analysis")]
    CiFailureAnalysis,
    /// Built-in CI-failure auto-close reconciler (CLAUDE-191): polls Forgejo and
    /// closes each open CI-failure issue whose failing `(repo, branch, workflow,
    /// job)` is now green on its latest run. Config lives in `native` as
    /// [`NativeConfig::CiFailureReconcile`].
    #[serde(rename = "ci-failure-reconcile")]
    CiFailureReconcile,
    /// Built-in cross-project closed-issue reporter (CLAUDE-218): counts and
    /// lists every issue closed in a rolling window across a configured project
    /// list, files the summary as a new issue, and hangs it off a nominated
    /// parent as a subtask. Config lives in `native` as
    /// [`NativeConfig::ClosedIssueSummary`].
    #[serde(rename = "closed-issue-summary")]
    ClosedIssueSummary,
}

impl ActionKind {
    /// Every kind, in the order the web UI's new-action picker lists them
    /// (CLAUDE-211). `scripted` last: it is deprecated and raw-YAML only.
    pub const ALL: [ActionKind; 6] = [
        ActionKind::Youtrack,
        ActionKind::ForgejoCiFix,
        ActionKind::CiFailureAnalysis,
        ActionKind::CiFailureReconcile,
        ActionKind::ClosedIssueSummary,
        ActionKind::Scripted,
    ];

    /// True for any kind routed to a built-in Rust handler (everything but
    /// `scripted`).
    pub fn is_native(self) -> bool {
        !matches!(self, ActionKind::Scripted)
    }

    /// Lowercase wire name, matching the serde `snake_case` rename.
    pub fn as_str(self) -> &'static str {
        match self {
            ActionKind::Scripted => "scripted",
            ActionKind::Youtrack => "youtrack",
            ActionKind::ForgejoCiFix => "forgejo-ci-fix",
            ActionKind::CiFailureAnalysis => "ci-failure-analysis",
            ActionKind::CiFailureReconcile => "ci-failure-reconcile",
            ActionKind::ClosedIssueSummary => "closed-issue-summary",
        }
    }
}

/// Typed configuration for a native (non-scripted) action: one variant per
/// native [`ActionKind`], supplied under the action's `native:` key. Required
/// when `kind` names a native handler, and its variant must match `kind`.
///
/// `#[serde(untagged)]`: the action's top-level `kind` is the discriminator, so
/// `native:` holds the variant's fields directly (e.g. the `youtrack` blocks
/// under `native:`) rather than re-tagging the variant in YAML.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(untagged)]
pub enum NativeConfig {
    /// YouTrack built-in handler config. Boxed (CLAUDE-125): adding the optional
    /// `prompt` override grew this variant past the other, so the box keeps the
    /// enum's variants size-balanced (`clippy::large_enum_variant`). `Box<T>` is
    /// serde- and `PartialEq`-transparent, and field access auto-derefs.
    Youtrack(Box<YoutrackConfig>),
    /// Forgejo CI-fix built-in handler config (CLAUDE-29). Boxed (CLAUDE-149):
    /// the auto-fix / Needs Review fields grew it past the boxed Youtrack
    /// variant, so the box keeps the enum size-balanced
    /// (`clippy::large_enum_variant`). `Box<T>` is serde- and
    /// `PartialEq`-transparent, and field access auto-derefs.
    ForgejoCiFix(Box<ForgejoCiFixConfig>),
    /// CI-failure-analysis built-in handler config (CLAUDE-189). Boxed to keep
    /// the enum's variants size-balanced (`clippy::large_enum_variant`), matching
    /// the other native variants.
    CiFailureAnalysis(Box<CiFailureAnalysisConfig>),
    /// CI-failure auto-close reconciler config (CLAUDE-191). Boxed to keep the
    /// enum's variants size-balanced (`clippy::large_enum_variant`), matching the
    /// other native variants.
    CiFailureReconcile(Box<CiFailureReconcileConfig>),
    /// Cross-project closed-issue reporter config (CLAUDE-218). Listed LAST, so
    /// the untagged probe reaches it only after the four older variants have
    /// rejected the block; its `projects` / `window_days` keys are unknown to
    /// every one of them, so a reporting config can never be mis-parsed as one.
    /// Boxed to keep the enum's variants size-balanced
    /// (`clippy::large_enum_variant`), matching the other native variants.
    ClosedIssueSummary(Box<ClosedIssueSummaryConfig>),
}

impl NativeConfig {
    /// The [`ActionKind`] this config variant corresponds to.
    pub fn kind(&self) -> ActionKind {
        match self {
            NativeConfig::Youtrack(_) => ActionKind::Youtrack,
            NativeConfig::ForgejoCiFix(_) => ActionKind::ForgejoCiFix,
            NativeConfig::CiFailureAnalysis(_) => ActionKind::CiFailureAnalysis,
            NativeConfig::CiFailureReconcile(_) => ActionKind::CiFailureReconcile,
            NativeConfig::ClosedIssueSummary(_) => ActionKind::ClosedIssueSummary,
        }
    }
}

/// Config for the native YouTrack handler (CLAUDE-33, Phase 4 of the
/// native-action epic CLAUDE-25). Supplied under the action's `native:` key when
/// `kind: youtrack`.
///
/// The generic blocks (`workdir_base`, `link_template`, `limits`, `schedule`,
/// `claude`, `output`) stay on [`Action`]; only the YouTrack- and Forge-specific
/// settings live here. Every field defaults, so a bare `native: {}` validates
/// and resolves to the same behavior the scripted action had.
///
/// Credentials are referenced by name (`token_ref`), not embedded: see
/// [`crate::secrets`] (CLAUDE-26). When neither block sets a `token_ref` the
/// CLIs use host auth, exactly as the scripted action did.
#[derive(Debug, Clone, Default, PartialEq, Eq, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct YoutrackConfig {
    /// YouTrack source: where jobs come from and how their state is tracked.
    pub youtrack: YoutrackSource,
    /// Forgejo target: where the runner opens PRs from the agent's commits.
    pub forgejo: ForgejoTarget,
    /// GitHub target (CLAUDE-50): where the runner opens PRs when a job's repo
    /// resolves to a GitHub host. All fields default, so existing actions are
    /// unchanged; routing is automatic by the resolved `repo_host`. Skipped on
    /// serialize when fully default (the default `mcp_endpoint` is a non-empty
    /// string, so unlike `forgejo` it would not otherwise prune away) so saving
    /// a Forgejo-only action through the web form writes no `github` block.
    #[serde(skip_serializing_if = "GithubTarget::is_default")]
    pub github: GithubTarget,
}

/// YouTrack-side settings for the native handler.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct YoutrackSource {
    /// YouTrack base URL. `None` uses the host-configured `yt` CLI default; it
    /// is required when `token_ref` is set, since the injected `yt` config needs
    /// a `url` (CLAUDE-26).
    pub base_url: Option<String>,
    /// Name of the encrypted secret holding the YouTrack API token (CLAUDE-26).
    /// `None` uses host `yt` auth. The raw token is never stored here; it is
    /// resolved from the [`crate::secrets`] store and injected at run time.
    pub token_ref: Option<String>,
    /// Issue query selecting the jobs this action picks up.
    pub query: String,
    /// Custom field holding the AI Agent state machine.
    pub custom_field: String,
    /// Labels of the AI Agent / State machine the handler drives.
    pub states: YoutrackStates,
    /// Full-replacement prompt override (CLAUDE-125). `None` uses the built-in
    /// `DEFAULT_YOUTRACK_PROMPT` from [`crate::native::youtrack`]. When set, the
    /// text fully replaces the default and is rendered with the documented
    /// `{{...}}` placeholders (`{{key}}`, `{{summary}}`, `{{clone_url}}`,
    /// `{{workdir}}`, `{{needs_review_marker}}`, `{{skip_attempt_marker}}`,
    /// `{{no_code_marker}}`, `{{write_projects}}`).
    pub prompt: Option<String>,
    /// Projects the agent may create issues in (CLAUDE-214), rendered into the
    /// prompt as `{{write_projects}}`. Empty (the default) resolves to the
    /// triggering issue's own project, so filing a deferred item needs no
    /// site-specific configuration and none is baked into the binary.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub write_projects: Vec<String>,
    /// Optional CI-gated automerge of the opened PR (CLAUDE-126). Disabled by
    /// default, so finalize stops at Code Review exactly as before; when enabled
    /// the runner waits for the PR's checks to go green and merges it.
    #[serde(skip_serializing_if = "Automerge::is_default")]
    pub automerge: Automerge,
    /// Optional Assignee to set on the issue during pickup (CLAUDE-131). A literal
    /// `yt` assignee argument (a login, `me`, or `unassigned`). `None` (the
    /// default) leaves the Assignee untouched, exactly as before.
    pub assignee: Option<String>,
    /// Pre-clone dependency gate (CLAUDE-161): skip a queued issue whose blockers
    /// are still open, leaving it Queued so it auto-resumes when they clear.
    /// Enabled by default; `enabled: false` makes the gate a no-op.
    #[serde(skip_serializing_if = "Dependencies::is_default")]
    pub dependencies: Dependencies,
}

impl Default for YoutrackSource {
    fn default() -> Self {
        Self {
            base_url: None,
            token_ref: None,
            query: default_query(),
            custom_field: default_custom_field(),
            states: YoutrackStates::default(),
            prompt: None,
            write_projects: Vec::new(),
            automerge: Automerge::default(),
            assignee: None,
            dependencies: Dependencies::default(),
        }
    }
}

/// Pre-clone dependency gate settings (CLAUDE-161). Before the clone and the
/// `claude --print` invocation, a queued issue with any unmet dependency link is
/// left Queued (no transition to Working), consumes no attempt, and posts a
/// blocker comment (idempotently). Blocking is by direction-resolved link `verb`
/// (YT-40): the defaults block `depends on` (Depend INWARD, an open blocker) and
/// `parent for` (Subtask OUTWARD, an open child), never `is required for` /
/// `subtask of`.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct Dependencies {
    /// Master switch. `true` (the default) runs the gate; `false` makes it a
    /// no-op and pickup proceeds exactly as before.
    pub enabled: bool,
    /// Backoff (humantime, e.g. `5m`) before a blocked issue is re-evaluated.
    /// While the window is open the issue stays Queued with no `yt` read and no
    /// comment, so repeated polls do not spam the issue.
    pub recheck_timeout: String,
    /// Direction-resolved link verbs that block. A queued issue is blocked when
    /// it has any link whose `verb` is listed here and whose target is
    /// unresolved.
    pub block_on: Vec<String>,
}

impl Default for Dependencies {
    fn default() -> Self {
        Self {
            enabled: true,
            recheck_timeout: default_recheck_timeout(),
            block_on: default_block_on(),
        }
    }
}

impl Dependencies {
    /// True when every field is at its default. Used to skip serializing an
    /// untouched `dependencies` block so existing action YAML stays unchanged.
    fn is_default(&self) -> bool {
        *self == Self::default()
    }
}

fn default_recheck_timeout() -> String {
    "5m".to_string()
}

fn default_block_on() -> Vec<String> {
    vec!["depends on".to_string(), "parent for".to_string()]
}

/// Opt-in, CI-gated automerge of the PR the runner opens after a successful
/// native YouTrack run (CLAUDE-126). When `enabled`, finalize polls the PR's
/// combined CI status every `check_interval_seconds` up to `wait_seconds`, and
/// on green (with an up-to-date branch) merges the PR as a merge-commit and
/// advances the issue to Done. Disabled by default, leaving the existing
/// stop-at-Code-Review behavior unchanged.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct Automerge {
    /// Master switch. `false` (the default) keeps finalize stopping at Code
    /// Review with no CI wait and no merge.
    pub enabled: bool,
    /// Maximum total seconds to wait for the PR's checks to go green (and the
    /// branch to be up to date) before giving up and leaving the PR open.
    pub wait_seconds: u64,
    /// Seconds between CI status polls.
    pub check_interval_seconds: u64,
}

impl Default for Automerge {
    fn default() -> Self {
        Self {
            enabled: false,
            wait_seconds: 1800,
            check_interval_seconds: 30,
        }
    }
}

impl Automerge {
    /// True when every field is at its default. Used to skip serializing an
    /// untouched `automerge` block so existing action YAML stays unchanged.
    fn is_default(&self) -> bool {
        *self == Self::default()
    }
}

fn default_query() -> String {
    "-Resolved AI Agent: Queued".to_string()
}

fn default_custom_field() -> String {
    "AI Agent".to_string()
}

/// The AI Agent (and standard `State`) values the handler reads and writes.
/// Defaults match the merged scripted action's hardcoded labels.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct YoutrackStates {
    /// AI Agent value an issue must hold to be picked up.
    pub queued: String,
    /// AI Agent value set while claude runs.
    pub working: String,
    /// AI Agent value set after a PR opens (awaiting human review).
    pub code_review: String,
    /// AI Agent value set when the agent parks the job pending human input.
    pub needs_review: String,
    /// AI Agent value set on a hard failure.
    pub failed: String,
    /// AI Agent value set after the runner automerges the PR (CLAUDE-126).
    pub done: String,
    /// AI Agent value set when a successful run deliberately produced no commits
    /// and left the `.claude-run-no-code-change` marker (CLAUDE-214). `None` (the
    /// default) resolves to `done`: there is no PR for a human to review. Read
    /// through [`YoutrackStates::no_code_label`], never directly.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub no_code: Option<String>,
    /// Standard `State` field value set in the pre step.
    pub in_progress: String,
    /// Standard `State` field value set after the runner automerges the PR
    /// (CLAUDE-126).
    pub state_done: String,
}

impl Default for YoutrackStates {
    fn default() -> Self {
        Self {
            queued: "Queued".to_string(),
            working: "Working".to_string(),
            code_review: "Code Review".to_string(),
            needs_review: "Needs Review".to_string(),
            failed: "Failed".to_string(),
            done: "Done".to_string(),
            no_code: None,
            in_progress: "In Progress".to_string(),
            state_done: "Done".to_string(),
        }
    }
}

impl YoutrackStates {
    /// AI Agent label for a deliberate no-code run (CLAUDE-214): the configured
    /// `no_code` when set, otherwise the configured `done` label.
    pub fn no_code_label(&self) -> &str {
        self.no_code.as_deref().unwrap_or(&self.done)
    }
}

/// How the runner opens the PR for a worked Forgejo issue (CLAUDE-103).
///
/// `Branch` (the default) pushes a real server branch with the git CLI
/// (`git push --set-upstream origin <branch>:<branch>`, where `<branch>` is
/// resolved by name, not from the ambient `HEAD`; CLAUDE-166) and opens the PR
/// over the API with an explicit `--head`, so Forgejo can always offer "Update
/// Branch" and the PR stays mergeable.
/// `Agit` runs `fj pr create -aA`, which creates a branchless PR whose head
/// lives only at `refs/pull/<N>/head`; such a PR loses its merge button the
/// moment it falls behind a base that requires an up-to-date branch. AGit is
/// kept as an explicit opt-in, never a silent fallback (the selected strategy
/// is authoritative).
///
/// CLAUDE-212: both variants are gated by the same ahead check before PR
/// creation (exactly one local branch ahead of the base), so neither can open a
/// PR for a run that produced no commits.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum PrStrategy {
    /// Push a real server branch, open the PR over the API. Always mergeable
    /// and updatable. The safe default.
    #[default]
    Branch,
    /// `fj pr create -aA`: branchless AGit PR. Opt-in; see the type docs for
    /// the merge-button footgun.
    Agit,
}

impl PrStrategy {
    /// The kebab-case wire / web-form value (`"branch"` / `"agit"`), matching
    /// the serde `rename_all` encoding.
    pub fn as_str(&self) -> &'static str {
        match self {
            PrStrategy::Branch => "branch",
            PrStrategy::Agit => "agit",
        }
    }

    /// Parse a web-form value into a strategy. An empty or unrecognized value
    /// falls back to the safe default ([`PrStrategy::Branch`]), so a malformed
    /// POST can never silently select the AGit footgun.
    pub fn from_form_value(s: &str) -> Self {
        match s {
            "agit" => PrStrategy::Agit,
            _ => PrStrategy::Branch,
        }
    }
}

/// Forgejo-side settings for the native handler.
#[derive(Debug, Clone, Default, PartialEq, Eq, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct ForgejoTarget {
    /// Forgejo base URL. `None` derives the host from each issue's resolved VCS
    /// integration (the current behavior).
    pub base_url: Option<String>,
    /// Name of the encrypted secret holding the Forgejo API token (CLAUDE-26).
    /// `None` uses host `fj` auth. The raw token is never stored here; it is
    /// resolved from the [`crate::secrets`] store and injected at run time.
    pub token_ref: Option<String>,
    /// PR-creation strategy (CLAUDE-103). Omitted deserializes to
    /// [`PrStrategy::Branch`], so existing action files get the safe,
    /// always-mergeable behavior with no migration.
    pub pr_strategy: PrStrategy,
}

/// GitHub-side settings for the native handler (CLAUDE-50). A job whose
/// resolved `repo_host` is GitHub (`github.com` plus any host in `hosts`) opens
/// its PR through the GitHub `Forge` backend instead of `fj`; every other host
/// keeps using `fj` unchanged. Auto-routing means no per-action forge selector:
/// every field is optional and defaults so existing Forgejo actions validate
/// and behave identically.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct GithubTarget {
    /// Extra GitHub host(s) beyond `github.com` to treat as GitHub, e.g. GitHub
    /// Enterprise hosts. Bare hosts (no scheme). Empty by default.
    pub hosts: Vec<String>,
    /// Override for the remote GitHub MCP endpoint. Defaults to GitHub's hosted
    /// MCP server, which needs no local install.
    pub mcp_endpoint: String,
    /// Name of the encrypted secret (CLAUDE-26) holding the GitHub PAT, used for
    /// the MCP call, the authenticated push, and `gh`. `None` falls back to host
    /// `gh` auth / `GH_TOKEN`. The raw token is never stored here; it is resolved
    /// from the [`crate::secrets`] store and injected at run time. The PAT needs
    /// `repo` + `pull_request` scope.
    pub token_ref: Option<String>,
}

impl Default for GithubTarget {
    fn default() -> Self {
        Self {
            hosts: Vec::new(),
            mcp_endpoint: default_github_mcp_endpoint(),
            token_ref: None,
        }
    }
}

impl GithubTarget {
    /// True when every field is at its default. Used to skip serializing an
    /// untouched `github` block (see [`YoutrackConfig`]).
    fn is_default(&self) -> bool {
        *self == Self::default()
    }
}

/// GitHub's hosted remote MCP endpoint (no local server to install).
pub fn default_github_mcp_endpoint() -> String {
    "https://api.githubcopilot.com/mcp/".to_string()
}

/// Config for the native Forgejo CI-fix handler (CLAUDE-29, supersedes
/// DEV-330). Supplied under the action's `native:` key when
/// `kind: forgejo-ci-fix`.
///
/// The handler scans `repos` on `host` for open PRs whose head-commit combined
/// CI status is `failure` and that were updated within `window_minutes`, keeps
/// only PRs authored by `bot_author` (CLAUDE-149), clones the PR branch,
/// runs Claude to fix the failing CI, applies a diff-size decision gate
/// (`no_changes` / `too_big` against `max_files` + `max_lines` / `push`),
/// commits as the bot identity with the `skip_marker`, pushes, and comments on
/// the PR. The dedup / attempt key is the PR head SHA (per-commit attempt cap).
///
/// Every field defaults, so a bare config validates; `repos` defaults to empty
/// (a no-op scan) so a freshly imported action does nothing until the allow-list
/// is filled in. Reads and writes go through the `fj` CLI (no raw REST), which
/// must be a build carrying FJ-1 (`pr search --json`) and FJ-2
/// (`pr status --json`).
///
/// The generic blocks (`workdir_base`, `claude`, `limits`, `schedule`,
/// `output`) stay on [`Action`]; only the Forgejo-specific settings live here.
/// `token` is inline plaintext for now (CLAUDE-26 encrypts it); the CLI backend
/// authenticates clone / push from `fj`'s own credential store today.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct ForgejoCiFixConfig {
    /// CLAUDE-149: master enable toggle. Defaults to `true`. The handler only
    /// auto-fixes the bot's OWN failing PRs (those authored by `bot_author`); a
    /// human's failing PR is left to its author. Set `false` to turn the handler
    /// off entirely.
    pub enabled: bool,
    /// Forgejo host, e.g. `dev.a8n.run` (bare host, no scheme), passed to
    /// `fj --host`.
    pub host: String,
    /// Allow-list of `owner/name` repos to scan for failing PRs. Empty means
    /// scan nothing.
    pub repos: Vec<String>,
    /// Only consider PRs updated within this many minutes of the run.
    pub window_minutes: u64,
    /// Decision gate: a diff touching more than this many files is `too_big`.
    pub max_files: u64,
    /// Decision gate: a diff changing more than this many lines is `too_big`.
    pub max_lines: u64,
    /// CLAUDE-149: the bot's own login. ONLY PRs authored by this login are
    /// auto-fixed (the bot iterates on its own failing PR); every other author's
    /// PR is left alone. Also the git author name used for the fix commit.
    pub bot_author: String,
    /// Git author email used for the fix commit.
    pub bot_email: String,
    /// Marker appended to the fix commit message so the bot's own push does not
    /// re-trigger this action.
    pub skip_marker: String,
    /// CLAUDE-149: per-PR fix-push ceiling. The bot keeps pushing corrective
    /// commits to its own PR until CI passes; after this many distinct fix pushes
    /// on one PR it stops and flags the PR Needs Review instead. Separate from
    /// the per-commit attempt cap (which resets on every new head SHA). Default 3.
    pub max_pr_attempts: u64,
    /// CLAUDE-149: Forgejo label added to a PR when the per-PR ceiling is reached
    /// (Needs Review). Default `needs-review`.
    pub needs_review_label: String,
    /// CLAUDE-149: YouTrack State set on the PR's source issue when the per-PR
    /// ceiling is reached, best-effort (only when the PR body references an
    /// issue). Default `Needs Review`.
    pub needs_review_state: String,
    /// CLAUDE-149: YouTrack login assigned to the PR's source issue when the
    /// per-PR ceiling is reached, best-effort. `None` (default) skips the
    /// assignee step.
    pub review_assignee: Option<String>,
    /// Forgejo API token (inline plaintext for now; CLAUDE-26 encrypts it). The
    /// CLI backend ignores it and uses `fj`'s credential store today.
    pub token: Option<String>,
}

impl Default for ForgejoCiFixConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            host: "dev.a8n.run".to_string(),
            repos: Vec::new(),
            window_minutes: 45,
            max_files: 10,
            max_lines: 300,
            bot_author: "claude-run".to_string(),
            bot_email: "claude-run@a8n.run".to_string(),
            skip_marker: "[skip claude]".to_string(),
            max_pr_attempts: 3,
            needs_review_label: "needs-review".to_string(),
            needs_review_state: "Needs Review".to_string(),
            review_assignee: None,
            token: None,
        }
    }
}

/// CLAUDE-200: how a native CI-failure action picks the Forgejo log token for the
/// repo an issue names. Both actions select issues across a global `#Unresolved`
/// query, so their candidate set spans every org and one token cannot serve them
/// all.
///
/// Two YAML forms:
///
/// ```yaml
/// log_token_ref: forgejo-logs-contoso    # one token for every repo
/// ```
///
/// ```yaml
/// log_token_ref:                          # first matching entry wins
///   - repos: ["contoso/*"]
///     token_ref: forgejo-logs-contoso
///   - repos: ["initech/*"]
///     token_ref: forgejo-logs-initech
/// ```
///
/// The scalar form behaves exactly like a single `*` entry, so a pre-CLAUDE-200
/// config keeps working. A repo matching no entry resolves to `None`, and the
/// handler skips that repo (or issue) with a warning rather than attempting it
/// with another org's token.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(untagged)]
pub enum LogTokenRef {
    /// One secret for every repo (the pre-CLAUDE-200 scalar form).
    Global(String),
    /// Ordered per-repo entries; the first whose pattern matches wins.
    PerRepo(Vec<LogTokenEntry>),
}

/// One entry of a [`LogTokenRef::PerRepo`] mapping.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct LogTokenEntry {
    /// Repo patterns this entry serves, in the webhook allow-list forms: exact
    /// `owner/repo`, per-owner `owner/*`, or the global `*`.
    pub repos: Vec<String>,
    /// Name of the encrypted secret (CLAUDE-26) holding the Forgejo PAT used for
    /// a repo this entry matches.
    pub token_ref: String,
}

impl LogTokenRef {
    /// The secret name to use for `repo` (an `owner/repo` full name), or `None`
    /// when no entry matches (the caller skips the repo instead of guessing).
    pub fn resolve(&self, repo: &str) -> Option<&str> {
        match self {
            LogTokenRef::Global(name) => Some(name.as_str()),
            LogTokenRef::PerRepo(entries) => entries
                .iter()
                .find(|e| crate::config::repo_matches(&e.repos, repo))
                .map(|e| e.token_ref.as_str()),
        }
    }

    /// The mapping derived from the webhook sources, used when an action
    /// configures none: one entry per source that carries a `log_token_ref`,
    /// serving that source's `allowed_repos`, in source-name order. Keeps a token
    /// rotation in one place (the Webhook settings section) instead of duplicated
    /// per action.
    pub fn from_webhook_sources(webhook: &crate::config::WebhookConfig) -> LogTokenRef {
        LogTokenRef::PerRepo(
            webhook
                .sources
                .values()
                .filter_map(|src| {
                    src.log_token_ref.as_ref().map(|token_ref| LogTokenEntry {
                        repos: src.allowed_repos.clone(),
                        token_ref: token_ref.clone(),
                    })
                })
                .collect(),
        )
    }
}

/// Config for the native CI-failure-analysis handler (CLAUDE-189). Supplied under
/// the action's `native:` key when `kind: ci-failure-analysis`.
///
/// The handler scans the tracker for open, unresolved issues whose summary starts
/// with one of `summary_prefixes` and that are not yet marked analyzed, then for
/// each: resolves the run coordinates from the issue's `## Failure` block, fetches
/// the complete redacted run log (`fj actions logs`, PAT from `log_token_ref`)
/// plus the failing workflow YAML, runs one agentic `claude` session over both,
/// and posts a deep root-cause comment before marking the issue analyzed
/// (idempotent). Selection is by summary prefix + analyzed-marker ONLY; it never
/// selects by `AI Agent: Queued` (that field belongs to the code-fix runner).
///
/// The generic blocks (`workdir_base`, `claude`, `limits`, `schedule`, `output`)
/// stay at the top level of the action, not inside `native`. Every field
/// defaults; `query` and `summary_prefixes` are the distinctive fields that
/// disambiguate this variant from the others.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct CiFailureAnalysisConfig {
    /// Master enable toggle. `true` (the default) runs the handler; `false` makes
    /// a run a no-op.
    pub enabled: bool,
    /// YouTrack query selecting the candidate issues to scan. It MUST NOT filter
    /// by `AI Agent` (analysis is read-only enrichment and must not collide with
    /// the code-fix runner's `AI Agent: Queued` selector); the summary-prefix and
    /// analyzed-marker filtering happens in the handler. Default `#Unresolved`.
    pub query: String,
    /// Summary prefixes an issue must start with to be analyzed. Default
    /// `["CI failure:", "Auto-fix:"]`, matching the mint summaries.
    pub summary_prefixes: Vec<String>,
    /// Forgejo PAT (`read:repository` scope) used to fetch the run log through
    /// `fj actions logs` and to read the workflow YAML. Either
    /// one secret name for every repo or a per-repo mapping (CLAUDE-200). When
    /// unset the mapping is derived from `config.webhook.sources`; an issue whose
    /// repo resolves to no token is skipped with a warning.
    pub log_token_ref: Option<LogTokenRef>,
    /// Cap (bytes) on the posted analysis comment, so a runaway agent reply cannot
    /// blow the comment. Default 24 KiB.
    pub max_analysis_bytes: usize,
}

impl Default for CiFailureAnalysisConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            query: "#Unresolved".to_string(),
            summary_prefixes: vec!["CI failure:".to_string(), "Auto-fix:".to_string()],
            log_token_ref: None,
            max_analysis_bytes: 24 * 1024,
        }
    }
}

/// Config for the native CI-failure auto-close reconciler (CLAUDE-191). Supplied
/// under the action's `native:` key when `kind: ci-failure-reconcile`.
///
/// On a schedule the handler scans the tracker for open CI-failure issues (whose
/// summary starts with one of `summary_prefixes`), reads each issue's hidden
/// `Branch:` / `Job:` markers (or, for a pre-rekey issue, the `## Failure` block,
/// skipping when branch/job are not parseable), then for each allow-listed repo
/// pages the Forgejo runs list, reduces it to the LATEST run per
/// `(branch, workflow_id)`, and reads that run's per-job status from the jobs
/// endpoint. An open issue whose `(repo, branch, workflow, job)` is success on its
/// latest run is closed: State set to `resolved_state` and a run-linked comment
/// posted. The close is idempotent (an already-closed issue is skipped). All
/// Forgejo reads go through [`crate::forge_log`] (Rust), never a raw REST call.
///
/// CLAUDE-198: before the Forgejo scan, open issues sharing a hidden
/// `Root-Cause-Key:` marker are collapsed onto the oldest of the group and the
/// rest closed as duplicates (`close_duplicates`).
///
/// `repos` and `resolved_state` are the distinctive fields that disambiguate this
/// variant from the other native configs under the untagged `native:` key, so a
/// shipped action MUST set at least one of them.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct CiFailureReconcileConfig {
    /// Master enable toggle. `true` (the default) runs the handler; `false` makes
    /// a run a no-op.
    pub enabled: bool,
    /// YouTrack query selecting the candidate issues to scan. MUST NOT filter by
    /// `AI Agent` (reconcile is independent of the code-fix runner's
    /// `AI Agent: Queued` selector); the summary-prefix + marker filtering happens
    /// in the handler. Default `#Unresolved`.
    pub query: String,
    /// Summary prefixes an issue must start with to be reconciled. Default
    /// `["CI failure:", "Auto-fix:"]`, matching the mint summaries.
    pub summary_prefixes: Vec<String>,
    /// Allow-list of `owner/repo` full names whose runs the reconciler scans. Each
    /// entry is an exact `owner/repo`, a per-owner `owner/*`, or the global `*`
    /// (the same three literal forms the webhook allow-list accepts). Empty (the
    /// default) scans nothing, so the reconciler must be given the repos to poll.
    pub repos: Vec<String>,
    /// Forgejo PAT (repository read scope) used to page the runs list, read the
    /// jobs endpoint, and (when `head_branch` is null) fetch a run's log to
    /// resolve its branch. Either one secret name for every repo or a per-repo
    /// mapping (CLAUDE-200). When unset the mapping is derived from
    /// `config.webhook.sources`; a repo that resolves to no token is skipped with
    /// a warning.
    pub log_token_ref: Option<LogTokenRef>,
    /// The State value set on a closed issue. Default `Done` (the CLAUDE project's
    /// only resolved State).
    pub resolved_state: String,
    /// Template for the run-linked auto-close comment. The placeholders
    /// `{run_index}`, `{branch}`, `{run_url}`, and `{job}` are substituted with the
    /// now-green run's per-repo index, branch, web URL, and job name. A hidden
    /// auto-closed marker is always appended (not part of the template).
    pub close_comment: String,
    /// Cap on the number of runs whose branch is resolved from the log per repo
    /// per run (bounds log fetches when the runs list returns `head_branch` null).
    /// Reached-cap is logged, never silent. Default 40.
    pub max_branch_resolves: usize,
    /// CLAUDE-198: collapse duplicate open issues that carry the same hidden
    /// `Root-Cause-Key:` marker before the Forgejo scan. Mint's cross-run collapse
    /// is a full-text search, so a lagging tracker index lets two failures of the
    /// same job mint two issues; this closes all but the oldest of each group.
    /// `true` by default; `false` leaves duplicates open.
    pub close_duplicates: bool,
    /// Template for the duplicate close comment. `{canonical}` is the surviving
    /// issue id and `{root_cause_key}` the shared marker. The hidden auto-closed
    /// marker is always appended (not part of the template).
    pub duplicate_comment: String,
    /// The branch a code (non-infra) issue's close scope is measured against
    /// (CLAUDE-265): closing such an issue requires a green run of its
    /// `(repo, workflow_id, job_name)` on THIS branch, never any branch, so a
    /// merged or deleted PR branch no longer strands the issue open forever. An
    /// infra-keyed issue (carrying the CLAUDE-264 `Infra-Kind:` marker) ignores
    /// this and closes on ANY branch going green. Default `"main"`.
    pub default_branch: String,
}

impl Default for CiFailureReconcileConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            query: "#Unresolved".to_string(),
            summary_prefixes: vec!["CI failure:".to_string(), "Auto-fix:".to_string()],
            repos: Vec::new(),
            log_token_ref: None,
            resolved_state: "Done".to_string(),
            close_comment: "Auto-closed: run #{run_index} on `{branch}` succeeded ({run_url}); the failing `{job}` is green, root cause resolved.".to_string(),
            max_branch_resolves: 40,
            close_duplicates: true,
            duplicate_comment: "Auto-closed as a duplicate of {canonical}: both issues track the same failing job (root-cause key `{root_cause_key}`). Follow {canonical} for the fix.".to_string(),
            default_branch: "main".to_string(),
        }
    }
}

/// Config for the native cross-project closed-issue reporter (CLAUDE-218).
/// Supplied under the action's `native:` key when `kind: closed-issue-summary`.
///
/// The run is a REPORT, not a code job: it clones no repository, starts no agent
/// session, and opens no pull request. For each nominated parent it queries
/// `projects` for the issues resolved inside a rolling `window_days` window,
/// renders counts plus a per-project listing, creates that report as a new issue
/// in `report_project` (defaulting to the parent's own project), and links it to
/// the parent with `link_type`. A hidden window marker on the parent makes the
/// whole thing idempotent, so a poll interval shorter than the window files one
/// report per window rather than one per poll.
///
/// Parents come from two independent sources, unioned: `parents` (nominated by
/// id, the mode that needs no tracker configuration) and `query` (a tracker
/// query, empty by default). `query` MUST select a `custom_field` value the
/// worker action does not, or the two race for the same issue: `actions/
/// youtrack.yml` owns `AI Agent: Queued`, so a reporting query needs its own
/// distinct value. Only query-selected parents get `custom_field` advanced to
/// `done_label` / `failed_label`; a parent nominated by id is never field-written.
///
/// `projects` and `window_days` are the distinctive keys that disambiguate this
/// variant from the other native configs under the untagged `native:` block, so
/// a shipped action MUST set at least one of them.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct ClosedIssueSummaryConfig {
    /// Master enable toggle. `true` (the default) runs the handler; `false` makes
    /// a run a no-op.
    pub enabled: bool,
    /// Issues to hang each report off, nominated by id (e.g. `["CLAUDE-100"]`),
    /// typically the milestone being reported against. Reported for on every run,
    /// subject to the idempotency marker. Empty by default.
    pub parents: Vec<String>,
    /// Optional tracker query selecting further parents dynamically. Empty (the
    /// default) means no query is run at all. See the type docs for the
    /// partitioning rule this query must obey.
    pub query: String,
    /// Custom field advanced on a query-selected parent once its report is filed.
    pub custom_field: String,
    /// Value written to `custom_field` after a query-selected parent's report is
    /// filed, which is what drops it back out of `query`.
    pub done_label: String,
    /// Value written to `custom_field` when a query-selected parent's report
    /// could not be filed.
    pub failed_label: String,
    /// Project short names the report counts closed issues across, e.g.
    /// `["CLAUDE", "DELTA"]`. Empty reports nothing, so the action must be given the
    /// projects to read.
    pub projects: Vec<String>,
    /// Length of the rolling reporting window in days, counting back from today
    /// inclusive (7 covers today plus the previous six days).
    pub window_days: u32,
    /// Project short name the report issue is created in. `None` (the default)
    /// creates it in the parent's own project, so no site-specific project has to
    /// be configured.
    pub report_project: Option<String>,
    /// Link type used to attach the report to its parent. Default `subtask of`,
    /// applied as `<report> subtask of <parent>`.
    pub link_type: String,
    /// Summary line of the report issue. `{start}`, `{end}`, `{days}`, `{total}`
    /// and `{parent}` are substituted.
    pub summary_template: String,
    /// Cap on how many issues are LISTED per project in the report body. Counts
    /// are always exact (the query is fully paginated); a project over the cap
    /// renders the cap plus an explicit "and N more" line, never a silent
    /// truncation.
    pub max_listed_per_project: usize,
}

impl Default for ClosedIssueSummaryConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            parents: Vec::new(),
            query: String::new(),
            custom_field: default_custom_field(),
            done_label: "Done".to_string(),
            failed_label: "Failed".to_string(),
            projects: Vec::new(),
            window_days: 7,
            report_project: None,
            link_type: "subtask of".to_string(),
            summary_template: "Closed-issue summary: {start} .. {end}".to_string(),
            max_listed_per_project: 100,
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Action {
    pub name: String,

    /// Dispatch discriminator (CLAUDE-30). Defaults to `scripted`.
    #[serde(default)]
    pub kind: ActionKind,

    #[serde(default)]
    pub workdir_base: Option<String>,

    /// Required for `scripted` actions; optional (and ignored) for native
    /// kinds, which produce their jobs in the Rust handler instead.
    #[serde(default)]
    pub input: Option<InputBlock>,

    #[serde(default)]
    pub dedup_key: Option<String>,

    #[serde(default)]
    pub filter: Option<Script>,

    #[serde(default)]
    pub pre: Vec<Script>,

    /// Required for `scripted` actions; optional (and ignored) for native
    /// kinds.
    #[serde(default)]
    pub prompt: Option<Script>,

    /// Typed config for native kinds; required when `kind` is native, absent
    /// for `scripted`.
    #[serde(default)]
    pub native: Option<NativeConfig>,

    #[serde(default)]
    pub claude: ClaudeBlock,

    #[serde(default)]
    pub post: Vec<Script>,

    #[serde(default)]
    pub output: OutputBlock,

    #[serde(default)]
    pub limits: Limits,

    /// CLAUDE-28: keyed parallel execution. Defaults to strictly sequential
    /// (`max_parallel: 1`) so existing actions are unchanged.
    #[serde(default)]
    pub concurrency: Concurrency,

    #[serde(default)]
    pub schedule: Option<Schedule>,

    /// Optional URL template for rendering job keys as clickable links in the
    /// web UI. `{key}` is substituted with the job key. Example:
    /// `https://example.myjetbrains.com/issue/{key}`.
    #[serde(default)]
    pub link_template: Option<String>,

    #[serde(default = "default_enabled")]
    pub enabled: bool,
}

/// Substitute `{key}` in a template string. Other `{...}` tokens pass through
/// unchanged so templates can include literal braces if they need to.
pub fn render_link(template: &str, key: &str) -> String {
    template.replace("{key}", key)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn action_kind_all_lists_every_variant() {
        // CLAUDE-211: the web UI's new-action picker iterates `ALL`, so a kind
        // missing from it would be uncreatable from the UI. The match below is
        // exhaustive, so adding a variant breaks this test until `ALL` grows.
        for kind in ActionKind::ALL {
            match kind {
                ActionKind::Scripted
                | ActionKind::Youtrack
                | ActionKind::ForgejoCiFix
                | ActionKind::CiFailureAnalysis
                | ActionKind::CiFailureReconcile
                | ActionKind::ClosedIssueSummary => {}
            }
        }
        let mut names: Vec<&str> = ActionKind::ALL.iter().map(|k| k.as_str()).collect();
        names.sort_unstable();
        names.dedup();
        assert_eq!(names.len(), ActionKind::ALL.len(), "duplicate kind in ALL");
        assert_eq!(names.len(), 6, "a new ActionKind must be added to ALL");
    }

    #[test]
    fn render_link_substitutes_key() {
        assert_eq!(
            render_link("https://example.myjetbrains.com/issue/{key}", "MK-16"),
            "https://example.myjetbrains.com/issue/MK-16"
        );
    }

    #[test]
    fn render_link_leaves_unknown_tokens_alone() {
        assert_eq!(render_link("/{repo}/{key}", "PR-42"), "/{repo}/PR-42");
    }

    #[test]
    fn script_field_accepts_string_and_mapping_forms() {
        let yaml = r#"
name: t
input:
  command: "echo []"
filter:
  run: "exit 0"
  shell: bash
pre:
  - "print pre"
prompt:
  run: "print hi"
post:
  - run: "echo done"
    shell: sh
"#;
        let action: Action = serde_yaml::from_str(yaml).unwrap();

        // Default kind is scripted.
        assert_eq!(action.kind, ActionKind::Scripted);

        // Plain string -> default interpreter (nu).
        let input = action.input.as_ref().unwrap();
        assert_eq!(input.command.code(), "echo []");
        assert_eq!(input.command.shell(), None);

        // Mapping with explicit shell.
        let filter = action.filter.as_ref().unwrap();
        assert_eq!(filter.code(), "exit 0");
        assert_eq!(filter.shell(), Some("bash"));

        // List of plain strings.
        assert_eq!(action.pre[0].code(), "print pre");
        assert_eq!(action.pre[0].shell(), None);

        // Mapping with omitted shell -> default (nu).
        let prompt = action.prompt.as_ref().unwrap();
        assert_eq!(prompt.code(), "print hi");
        assert_eq!(prompt.shell(), None);

        // List of mappings.
        assert_eq!(action.post[0].code(), "echo done");
        assert_eq!(action.post[0].shell(), Some("sh"));
    }

    const MINIMAL: &str = "name: demo\ninput:\n  command: echo '[]'\nprompt: do the thing\n";

    #[test]
    fn validate_str_accepts_minimal_action() {
        let loaded = validate_str(MINIMAL, "<candidate>").expect("valid");
        assert_eq!(loaded.action.name, "demo");
        assert_eq!(loaded.yaml_sha256.len(), 64);
    }

    #[test]
    fn validate_str_rejects_missing_prompt() {
        let raw = "name: demo\ninput:\n  command: echo '[]'\nprompt: \"\"\n";
        let err = validate_str(raw, "<candidate>").unwrap_err();
        assert!(matches!(err, Error::ActionMissingField { field, .. } if field == "prompt"));
    }

    #[test]
    fn validate_str_rejects_unknown_field() {
        let raw = format!("{MINIMAL}bogus: nope\n");
        assert!(matches!(
            validate_str(&raw, "<candidate>").unwrap_err(),
            Error::Yaml(_)
        ));
    }

    const NATIVE: &str = "name: yt\nkind: youtrack\nnative: {}\n";

    #[test]
    fn validate_str_accepts_native_action_without_script_fields() {
        let loaded = validate_str(NATIVE, "<candidate>").expect("valid native");
        assert_eq!(loaded.action.kind, ActionKind::Youtrack);
        assert!(loaded.action.kind.is_native());
        assert!(loaded.action.input.is_none());
        assert!(loaded.action.prompt.is_none());
        // A bare `native: {}` resolves every field to its default.
        assert_eq!(
            loaded.action.native,
            Some(NativeConfig::Youtrack(Box::default()))
        );
    }

    #[test]
    fn native_config_defaults_match_the_scripted_action() {
        let raw = "name: yt\nkind: youtrack\nnative: {}\n";
        let loaded = validate_str(raw, "<candidate>").unwrap();
        let Some(NativeConfig::Youtrack(cfg)) = loaded.action.native else {
            panic!("expected youtrack native config");
        };
        assert_eq!(cfg.youtrack.query, "-Resolved AI Agent: Queued");
        assert_eq!(cfg.youtrack.custom_field, "AI Agent");
        assert_eq!(cfg.youtrack.states.queued, "Queued");
        assert_eq!(cfg.youtrack.states.working, "Working");
        assert_eq!(cfg.youtrack.states.code_review, "Code Review");
        assert_eq!(cfg.youtrack.states.needs_review, "Needs Review");
        assert_eq!(cfg.youtrack.states.failed, "Failed");
        assert_eq!(cfg.youtrack.states.in_progress, "In Progress");
        // CLAUDE-125: no prompt override by default; the built-in prompt is used.
        assert!(cfg.youtrack.prompt.is_none());
        assert!(cfg.youtrack.base_url.is_none());
        assert!(cfg.youtrack.token_ref.is_none());
        assert!(cfg.forgejo.base_url.is_none());
        assert!(cfg.forgejo.token_ref.is_none());
        // CLAUDE-103: an omitted pr_strategy deserializes to the safe,
        // always-mergeable branch-backed strategy, so no migration is needed.
        assert_eq!(cfg.forgejo.pr_strategy, PrStrategy::Branch);
        // CLAUDE-50: the github block defaults to the hosted MCP endpoint, no
        // extra hosts, and host gh auth (no token_ref).
        assert!(cfg.github.hosts.is_empty());
        assert_eq!(
            cfg.github.mcp_endpoint,
            "https://api.githubcopilot.com/mcp/"
        );
        assert!(cfg.github.token_ref.is_none());
    }

    /// CLAUDE-161: the dependency gate defaults to on, a 5m re-check backoff, and
    /// blocking on `depends on` / `parent for`, matching the issue spec. A bare
    /// `native: {}` resolves them.
    #[test]
    fn native_config_dependency_gate_defaults() {
        let loaded = validate_str(NATIVE, "<candidate>").unwrap();
        let Some(NativeConfig::Youtrack(cfg)) = loaded.action.native else {
            panic!("expected youtrack native config");
        };
        let deps = &cfg.youtrack.dependencies;
        assert!(deps.enabled);
        assert_eq!(deps.recheck_timeout, "5m");
        assert_eq!(deps.block_on, vec!["depends on", "parent for"]);
    }

    /// CLAUDE-161: an untouched `dependencies` block round-trips out of the
    /// serialized YAML (`skip_serializing_if`), so saving an action through the
    /// web form does not write a redundant block.
    #[test]
    fn native_config_dependency_gate_skips_serializing_default() {
        let cfg = YoutrackConfig::default();
        let yaml = serde_yaml::to_string(&cfg).unwrap();
        assert!(
            !yaml.contains("dependencies"),
            "default dependencies must not serialize: {yaml}"
        );
    }

    /// CLAUDE-161: a non-humantime `recheck_timeout` is rejected at load with a
    /// typed `ActionInvalidField`, so a bad value fails fast rather than silently
    /// falling back at gate time.
    #[test]
    fn validate_str_rejects_bad_recheck_timeout() {
        let raw = "name: yt\nkind: youtrack\nnative:\n  youtrack:\n    dependencies:\n      recheck_timeout: \"not-a-duration\"\n";
        let err = validate_str(raw, "<candidate>").unwrap_err();
        assert!(
            matches!(&err, Error::ActionInvalidField { field, .. }
                if field == "native.youtrack.dependencies.recheck_timeout"),
            "got {err:?}"
        );
    }

    /// CLAUDE-161: a valid humantime `recheck_timeout` and an `enabled: false`
    /// override parse cleanly.
    #[test]
    fn native_config_parses_dependency_overrides() {
        let raw = "name: yt\nkind: youtrack\nnative:\n  youtrack:\n    dependencies:\n      enabled: false\n      recheck_timeout: 30s\n      block_on:\n        - depends on\n";
        let loaded = validate_str(raw, "<candidate>").unwrap();
        let Some(NativeConfig::Youtrack(cfg)) = loaded.action.native else {
            panic!("expected youtrack native config");
        };
        let deps = &cfg.youtrack.dependencies;
        assert!(!deps.enabled);
        assert_eq!(deps.recheck_timeout, "30s");
        assert_eq!(deps.block_on, vec!["depends on"]);
    }

    #[test]
    fn native_config_parses_forgejo_pr_strategy() {
        // CLAUDE-103: an explicit `pr_strategy: agit` selects the AGit strategy;
        // the kebab-case value matches the serde encoding, and an unknown value
        // is rejected by `deny_unknown_fields` semantics on the enum.
        let raw = "\
name: yt
kind: youtrack
native:
  forgejo:
    pr_strategy: agit
";
        let loaded = validate_str(raw, "<candidate>").unwrap();
        let Some(NativeConfig::Youtrack(cfg)) = loaded.action.native else {
            panic!("expected youtrack native config");
        };
        assert_eq!(cfg.forgejo.pr_strategy, PrStrategy::Agit);

        let bogus = raw.replace("agit", "rebase-magic");
        assert!(matches!(
            validate_str(&bogus, "<candidate>").unwrap_err(),
            Error::Yaml(_)
        ));
    }

    #[test]
    fn native_config_parses_youtrack_prompt_override() {
        // CLAUDE-125: an explicit `prompt` is a full-replacement override that
        // round-trips through the loader (block scalar keeps its trailing
        // newline).
        let raw = "\
name: yt
kind: youtrack
native:
  youtrack:
    prompt: |
      Work {{key}}: {{summary}}
";
        let loaded = validate_str(raw, "<candidate>").unwrap();
        let Some(NativeConfig::Youtrack(cfg)) = loaded.action.native else {
            panic!("expected youtrack native config");
        };
        assert_eq!(
            cfg.youtrack.prompt.as_deref(),
            Some("Work {{key}}: {{summary}}\n")
        );
    }

    #[test]
    fn youtrack_automerge_defaults_to_disabled_when_absent() {
        // CLAUDE-126: a youtrack config with no `automerge` block deserializes to
        // the disabled defaults, so the existing finalize behavior is unchanged.
        let raw = "name: yt\nkind: youtrack\nnative:\n  youtrack: {}\n";
        let loaded = validate_str(raw, "<candidate>").unwrap();
        let Some(NativeConfig::Youtrack(cfg)) = loaded.action.native else {
            panic!("expected youtrack native config");
        };
        assert_eq!(cfg.youtrack.automerge, Automerge::default());
        assert!(!cfg.youtrack.automerge.enabled);
        assert_eq!(cfg.youtrack.automerge.wait_seconds, 1800);
        assert_eq!(cfg.youtrack.automerge.check_interval_seconds, 30);
    }

    #[test]
    fn youtrack_automerge_round_trips_through_yaml() {
        // CLAUDE-126: an explicit `automerge` block parses its fields and survives
        // a serialize -> deserialize round-trip unchanged.
        let raw = "\
name: yt
kind: youtrack
native:
  youtrack:
    automerge:
      enabled: true
      wait_seconds: 600
      check_interval_seconds: 15
";
        let loaded = validate_str(raw, "<candidate>").unwrap();
        let Some(NativeConfig::Youtrack(cfg)) = loaded.action.native else {
            panic!("expected youtrack native config");
        };
        let am = &cfg.youtrack.automerge;
        assert!(am.enabled);
        assert_eq!(am.wait_seconds, 600);
        assert_eq!(am.check_interval_seconds, 15);

        // Round-trip: serializing then re-parsing yields the same struct.
        let yaml = serde_yaml::to_string(am).unwrap();
        let back: Automerge = serde_yaml::from_str(&yaml).unwrap();
        assert_eq!(*am, back);

        // An unknown key inside the block is rejected (deny_unknown_fields).
        let bogus = raw.replace("wait_seconds: 600", "wait_secondz: 600");
        assert!(matches!(
            validate_str(&bogus, "<candidate>").unwrap_err(),
            Error::Yaml(_)
        ));
    }

    #[test]
    fn native_config_parses_github_block() {
        // CLAUDE-50: the optional github block parses its hosts / mcp_endpoint /
        // token_ref; unknown keys inside it are still rejected.
        let raw = "\
name: yt
kind: youtrack
native:
  github:
    hosts:
      - ghe.example.com
    mcp_endpoint: https://ghe.example.com/api/mcp/
    token_ref: gh-prod
";
        let loaded = validate_str(raw, "<candidate>").unwrap();
        let Some(NativeConfig::Youtrack(cfg)) = loaded.action.native else {
            panic!("expected youtrack native config");
        };
        assert_eq!(cfg.github.hosts, vec!["ghe.example.com".to_string()]);
        assert_eq!(cfg.github.mcp_endpoint, "https://ghe.example.com/api/mcp/");
        assert_eq!(cfg.github.token_ref.as_deref(), Some("gh-prod"));

        let bogus = format!("{raw}    bogus: nope\n");
        assert!(matches!(
            validate_str(&bogus, "<candidate>").unwrap_err(),
            Error::Yaml(_)
        ));
    }

    #[test]
    fn native_config_parses_overrides_and_rejects_unknown_keys() {
        let raw = "\
name: yt
kind: youtrack
native:
  youtrack:
    base_url: https://yt.example
    token_ref: yt-prod
    query: \"-Resolved AI Agent: Ready\"
    custom_field: Bot State
    states:
      queued: Ready
      working: Busy
  forgejo:
    base_url: https://forge.example
    token_ref: fj-prod
";
        let loaded = validate_str(raw, "<candidate>").unwrap();
        let Some(NativeConfig::Youtrack(cfg)) = loaded.action.native else {
            panic!("expected youtrack native config");
        };
        assert_eq!(cfg.youtrack.base_url.as_deref(), Some("https://yt.example"));
        assert_eq!(cfg.youtrack.token_ref.as_deref(), Some("yt-prod"));
        assert_eq!(cfg.youtrack.query, "-Resolved AI Agent: Ready");
        assert_eq!(cfg.youtrack.custom_field, "Bot State");
        // Overridden labels apply; the rest keep their defaults.
        assert_eq!(cfg.youtrack.states.queued, "Ready");
        assert_eq!(cfg.youtrack.states.working, "Busy");
        assert_eq!(cfg.youtrack.states.code_review, "Code Review");
        assert_eq!(
            cfg.forgejo.base_url.as_deref(),
            Some("https://forge.example")
        );
        assert_eq!(cfg.forgejo.token_ref.as_deref(), Some("fj-prod"));

        let bogus = format!("{raw}    bogus: nope\n");
        assert!(matches!(
            validate_str(&bogus, "<candidate>").unwrap_err(),
            Error::Yaml(_)
        ));
    }

    #[test]
    fn validate_str_rejects_native_action_without_native_config() {
        let raw = "name: yt\nkind: youtrack\n";
        let err = validate_str(raw, "<candidate>").unwrap_err();
        assert!(matches!(err, Error::ActionMissingField { field, .. } if field == "native"));
    }

    /// The shipped `actions/youtrack.yml` migrated to the native form: it loads,
    /// is recognized as a native youtrack action with no script-bearing fields,
    /// and round-trips through serialize -> parse unchanged (AC2 + AC4).
    #[test]
    fn shipped_youtrack_action_is_native_and_round_trips() {
        const SHIPPED: &str = include_str!("../actions/youtrack.yml");
        let loaded = validate_str(SHIPPED, "actions/youtrack.yml").expect("shipped action valid");
        let action = loaded.action;

        assert_eq!(action.name, "youtrack");
        assert_eq!(action.kind, ActionKind::Youtrack);
        // No script-bearing fields remain on the migrated file.
        assert!(action.input.is_none());
        assert!(action.filter.is_none());
        assert!(action.pre.is_empty());
        assert!(action.prompt.is_none());
        assert!(action.post.is_empty());

        let Some(NativeConfig::Youtrack(cfg)) = &action.native else {
            panic!("expected youtrack native config");
        };
        // CLAUDE-215: the shipped query excludes the audit project, which
        // `audit-run.yml` owns. It therefore differs from `default_query()`,
        // which stays site-agnostic.
        assert_eq!(
            cfg.youtrack.query,
            "-Resolved AI Agent: Queued project: -AUDIT"
        );
        assert_eq!(cfg.youtrack.custom_field, "AI Agent");
        // Generic blocks stay on the Action, not in the native config.
        assert_eq!(action.workdir_base.as_deref(), Some("~/workspace"));
        assert_eq!(action.limits.max_attempts_per_key, Some(3));

        // Serialize -> parse round-trip yields an identical native config.
        let reserialized = serde_yaml::to_string(&action).unwrap();
        let reloaded = validate_str(&reserialized, "<roundtrip>").unwrap();
        assert_eq!(reloaded.action.native, action.native);
        assert_eq!(reloaded.action.kind, action.kind);
    }

    /// The shipped `actions/audit-run.yml` (CLAUDE-214): a second youtrack-kind
    /// action whose only structural difference is the `AUDIT` query and the
    /// full-replacement prompt that permits the three audit tracker writes.
    #[test]
    fn shipped_audit_run_action_is_native_and_round_trips() {
        const SHIPPED: &str = include_str!("../actions/audit-run.yml");
        let loaded = validate_str(SHIPPED, "actions/audit-run.yml").expect("shipped action valid");
        let action = loaded.action;

        assert_eq!(action.name, "audit-run");
        assert_eq!(action.kind, ActionKind::Youtrack);
        // Config only: no script-bearing fields on a native action.
        assert!(action.input.is_none());
        assert!(action.filter.is_none());
        assert!(action.pre.is_empty());
        assert!(action.prompt.is_none());
        assert!(action.post.is_empty());

        let Some(NativeConfig::Youtrack(cfg)) = &action.native else {
            panic!("expected youtrack native config");
        };
        assert_eq!(
            cfg.youtrack.query,
            "-Resolved AI Agent: Queued project: AUDIT"
        );
        // The prompt override is the reason this action exists (CLAUDE-125).
        let prompt = cfg
            .youtrack
            .prompt
            .as_deref()
            .expect("audit action carries a prompt override");
        assert!(prompt.contains("{{key}}"));
        assert!(prompt.contains("{{workdir}}"));
        assert!(prompt.contains("{{needs_review_marker}}"));
        assert!(prompt.contains("{{skip_attempt_marker}}"));
        // Exactly three tracker writes are permitted; creation stays banned.
        assert!(prompt.contains("yt issue add-comment"));
        assert!(prompt.contains("no `yt issue create`"));
        assert!(prompt.contains("no `yt issue set-field`"));
        assert!(prompt.contains("no `yt issue set-state`"));
        // The parent gate is what makes the daily summary run last, so it must
        // not be disabled here.
        assert!(cfg.youtrack.dependencies.enabled);
        // One audit at a time: the cost-control property.
        assert_eq!(action.concurrency.max_parallel, 1);

        let reserialized = serde_yaml::to_string(&action).unwrap();
        let reloaded = validate_str(&reserialized, "<roundtrip>").unwrap();
        assert_eq!(reloaded.action.native, action.native);
        assert_eq!(reloaded.action.kind, action.kind);
    }

    /// CLAUDE-214 addendum: relaxing the built-in prompt must not widen the
    /// audit action underneath it. `audit-run.yml` keeps its own override, and
    /// that override stays the tighter policy: it bans issue creation, which the
    /// built-in prompt now permits, and it names its three permitted writes.
    #[test]
    fn audit_prompt_override_stays_narrower_than_the_builtin() {
        const SHIPPED: &str = include_str!("../actions/audit-run.yml");
        let loaded = validate_str(SHIPPED, "actions/audit-run.yml").expect("shipped action valid");
        let Some(NativeConfig::Youtrack(cfg)) = &loaded.action.native else {
            panic!("expected youtrack native config");
        };
        let audit = cfg
            .youtrack
            .prompt
            .as_deref()
            .expect("audit action carries a prompt override");
        let builtin = crate::native::youtrack::default_youtrack_prompt();

        assert_ne!(audit, builtin, "the override must not become the default");
        // The built-in permits creation; the audit override forbids it.
        assert!(
            builtin.contains("Create issues with 'yt issue create'"),
            "built-in must permit creation: {builtin}"
        );
        assert!(
            audit.contains("no `yt issue create`"),
            "audit override must still ban creation: {audit}"
        );
        // Both keep `AI Agent` and `State` the runner's alone.
        for p in [audit, builtin.as_str()] {
            assert!(p.contains("set-field"), "{p}");
            assert!(p.contains("set-state"), "{p}");
        }
    }

    /// CLAUDE-215: the two youtrack-kind actions partition the queued issues
    /// rather than racing for them. Both poll on the same interval, so an issue
    /// matching both would be cloned by whichever action polled first, and for
    /// an audit issue that means the wrong repo and a pull request against it.
    ///
    /// The queries execute server-side, so this asserts the static half: the
    /// default action excludes `AUDIT` and the audit action selects only `AUDIT`.
    /// Note the syntax - YouTrack rejects `-project: AUDIT` as a parse error, so
    /// the negation has to sit on the value (`project: -AUDIT`).
    #[test]
    fn shipped_youtrack_actions_partition_audit_issues() {
        fn query_of(raw: &str, path: &str) -> String {
            let loaded = validate_str(raw, path).expect("shipped action valid");
            let Some(NativeConfig::Youtrack(cfg)) = loaded.action.native else {
                panic!("expected youtrack native config in {path}");
            };
            cfg.youtrack.query
        }

        let default_query = query_of(
            include_str!("../actions/youtrack.yml"),
            "actions/youtrack.yml",
        );
        let audit_query = query_of(
            include_str!("../actions/audit-run.yml"),
            "actions/audit-run.yml",
        );

        assert!(
            default_query.contains("project: -AUDIT"),
            "default action must exclude the audit project, got {default_query:?}"
        );
        assert!(
            !default_query.contains("-project:"),
            "`-project:` is a YouTrack parse error; negate the value instead, got {default_query:?}"
        );
        assert!(
            audit_query.contains("project: AUDIT") && !audit_query.contains("project: -AUDIT"),
            "audit action must select only the audit project, got {audit_query:?}"
        );
        assert_ne!(default_query, audit_query);

        // Both queries name the project by its real short name. `AUD` is a
        // legacy alias YouTrack still resolves but lists nowhere.
        fn project_value(q: &str) -> &str {
            q.rsplit("project: ")
                .next()
                .expect("query names a project")
                .split_whitespace()
                .next()
                .expect("project clause carries a value")
                .trim_start_matches('-')
        }
        for q in [&default_query, &audit_query] {
            assert_eq!(
                project_value(q),
                "AUDIT",
                "name the audit project by its short name, not a legacy alias: {q:?}"
            );
        }
    }

    #[test]
    fn forgejo_ci_fix_native_parses_and_defaults() {
        let raw = "\
name: forgejo-ci-fix
kind: forgejo-ci-fix
native:
  host: dev.a8n.run
  repos:
    - contoso/claude-run
";
        let loaded = validate_str(raw, "<candidate>").unwrap();
        assert_eq!(loaded.action.kind, ActionKind::ForgejoCiFix);
        assert!(loaded.action.kind.is_native());
        let Some(NativeConfig::ForgejoCiFix(cfg)) = loaded.action.native else {
            panic!("expected forgejo-ci-fix native config");
        };
        assert_eq!(cfg.host, "dev.a8n.run");
        assert_eq!(cfg.repos, vec!["contoso/claude-run".to_string()]);
        // Unset fields fall back to the documented defaults.
        assert_eq!(cfg.window_minutes, 45);
        assert_eq!(cfg.max_files, 10);
        assert_eq!(cfg.max_lines, 300);
        assert_eq!(cfg.bot_author, "claude-run");
        assert_eq!(cfg.bot_email, "claude-run@a8n.run");
        assert_eq!(cfg.skip_marker, "[skip claude]");
        assert!(cfg.token.is_none());
    }

    #[test]
    fn forgejo_ci_fix_native_overrides_and_rejects_unknown_keys() {
        let raw = "\
name: ci
kind: forgejo-ci-fix
native:
  host: forge.example
  repos: [a/b, c/d]
  window_minutes: 90
  max_files: 5
  max_lines: 120
  bot_author: ci-bot
  bot_email: ci-bot@example.test
  skip_marker: \"[skip ci]\"
  token: secret-fj
";
        let loaded = validate_str(raw, "<candidate>").unwrap();
        let Some(NativeConfig::ForgejoCiFix(cfg)) = loaded.action.native else {
            panic!("expected forgejo-ci-fix native config");
        };
        assert_eq!(cfg.host, "forge.example");
        assert_eq!(cfg.repos, vec!["a/b".to_string(), "c/d".to_string()]);
        assert_eq!(cfg.window_minutes, 90);
        assert_eq!(cfg.max_files, 5);
        assert_eq!(cfg.max_lines, 120);
        assert_eq!(cfg.bot_author, "ci-bot");
        assert_eq!(cfg.bot_email, "ci-bot@example.test");
        assert_eq!(cfg.skip_marker, "[skip ci]");
        assert_eq!(cfg.token.as_deref(), Some("secret-fj"));

        let bogus = format!("{raw}  bogus: nope\n");
        assert!(matches!(
            validate_str(&bogus, "<candidate>").unwrap_err(),
            Error::Yaml(_)
        ));
    }

    /// The shipped `actions/forgejo-ci-fix.yml`, migrated to the native form:
    /// loads, is recognized as a native forgejo-ci-fix action with no
    /// script-bearing fields, and round-trips through serialize -> parse
    /// unchanged (AC1 + AC3).
    #[test]
    fn shipped_forgejo_ci_fix_action_is_native_and_round_trips() {
        const SHIPPED: &str = include_str!("../actions/forgejo-ci-fix.yml");
        let loaded =
            validate_str(SHIPPED, "actions/forgejo-ci-fix.yml").expect("shipped action valid");
        let action = loaded.action;

        assert_eq!(action.name, "forgejo-ci-fix");
        assert_eq!(action.kind, ActionKind::ForgejoCiFix);
        // No script-bearing fields remain on the migrated file.
        assert!(action.input.is_none());
        assert!(action.filter.is_none());
        assert!(action.pre.is_empty());
        assert!(action.prompt.is_none());
        assert!(action.post.is_empty());

        let Some(NativeConfig::ForgejoCiFix(_)) = &action.native else {
            panic!("expected forgejo-ci-fix native config");
        };

        // Serialize -> parse round-trip yields an identical native config.
        let reserialized = serde_yaml::to_string(&action).unwrap();
        let reloaded = validate_str(&reserialized, "<roundtrip>").unwrap();
        assert_eq!(reloaded.action.native, action.native);
        assert_eq!(reloaded.action.kind, action.kind);
    }

    // ---- CLAUDE-189: ci-failure-analysis native kind ---------------------

    #[test]
    fn ci_failure_analysis_native_parses_and_defaults() {
        let raw = "\
name: ci-failure-analysis
kind: ci-failure-analysis
native:
  query: \"#Unresolved\"
  log_token_ref: forgejo-logs-contoso
";
        let loaded = validate_str(raw, "<candidate>").unwrap();
        assert_eq!(loaded.action.kind, ActionKind::CiFailureAnalysis);
        assert!(loaded.action.kind.is_native());
        let Some(NativeConfig::CiFailureAnalysis(cfg)) = loaded.action.native else {
            panic!("expected ci-failure-analysis native config");
        };
        assert!(cfg.enabled);
        assert_eq!(cfg.query, "#Unresolved");
        // Scalar form: one token for every repo (CLAUDE-200 back-compat).
        assert_eq!(
            cfg.log_token_ref,
            Some(LogTokenRef::Global("forgejo-logs-contoso".to_string()))
        );
        // Unset fields fall back to the documented defaults.
        assert_eq!(
            cfg.summary_prefixes,
            vec!["CI failure:".to_string(), "Auto-fix:".to_string()]
        );
        assert_eq!(cfg.max_analysis_bytes, 24 * 1024);
    }

    #[test]
    fn ci_failure_analysis_native_overrides_and_rejects_unknown_keys() {
        let raw = "\
name: cfa
kind: ci-failure-analysis
native:
  enabled: false
  query: \"project: CLAUDE #Unresolved\"
  summary_prefixes: [\"CI failure:\"]
  log_token_ref: pat
  max_analysis_bytes: 4096
";
        let loaded = validate_str(raw, "<candidate>").unwrap();
        let Some(NativeConfig::CiFailureAnalysis(cfg)) = loaded.action.native else {
            panic!("expected ci-failure-analysis native config");
        };
        assert!(!cfg.enabled);
        assert_eq!(cfg.query, "project: CLAUDE #Unresolved");
        assert_eq!(cfg.summary_prefixes, vec!["CI failure:".to_string()]);
        assert_eq!(cfg.max_analysis_bytes, 4096);

        let bogus = format!("{raw}  bogus: nope\n");
        assert!(matches!(
            validate_str(&bogus, "<candidate>").unwrap_err(),
            Error::Yaml(_)
        ));
    }

    /// The default query must NEVER select by `AI Agent`: analysis is read-only
    /// enrichment and must not collide with the code-fix runner's
    /// `AI Agent: Queued` selector.
    #[test]
    fn ci_failure_analysis_default_query_never_selects_by_ai_agent() {
        let cfg = CiFailureAnalysisConfig::default();
        assert!(!cfg.query.contains("AI Agent"), "query: {}", cfg.query);
        assert!(!cfg.query.contains("Queued"), "query: {}", cfg.query);
    }

    /// The shipped `actions/ci-failure-analysis.yml` loads, is recognized as a
    /// native ci-failure-analysis action with no script-bearing fields, and
    /// round-trips through serialize -> parse unchanged.
    #[test]
    fn shipped_ci_failure_analysis_action_is_native_and_round_trips() {
        const SHIPPED: &str = include_str!("../actions/ci-failure-analysis.yml");
        let loaded =
            validate_str(SHIPPED, "actions/ci-failure-analysis.yml").expect("shipped action valid");
        let action = loaded.action;
        assert_eq!(action.name, "ci-failure-analysis");
        assert_eq!(action.kind, ActionKind::CiFailureAnalysis);
        assert!(action.input.is_none());
        assert!(action.filter.is_none());
        assert!(action.pre.is_empty());
        assert!(action.prompt.is_none());
        assert!(action.post.is_empty());
        let Some(NativeConfig::CiFailureAnalysis(_)) = &action.native else {
            panic!("expected ci-failure-analysis native config");
        };
        // CLAUDE-222: job keys are YouTrack issue ids, so the run screens link
        // them out through the action's link template.
        let link = action.link_template.as_deref().expect("link_template");
        assert!(link.contains("{key}"), "link_template: {link}");
        let reserialized = serde_yaml::to_string(&action).unwrap();
        let reloaded = validate_str(&reserialized, "<roundtrip>").unwrap();
        assert_eq!(reloaded.action.native, action.native);
        assert_eq!(reloaded.action.kind, action.kind);
    }

    // ---- CLAUDE-191: ci-failure-reconcile native config -------------------

    #[test]
    fn ci_failure_reconcile_native_parses_and_defaults() {
        let raw = "\
name: ci-failure-reconcile
kind: ci-failure-reconcile
native:
  repos:
    - contoso/claude-run
  log_token_ref: forgejo-logs-contoso
";
        let loaded = validate_str(raw, "<candidate>").unwrap();
        assert_eq!(loaded.action.kind, ActionKind::CiFailureReconcile);
        assert!(loaded.action.kind.is_native());
        let Some(NativeConfig::CiFailureReconcile(cfg)) = loaded.action.native else {
            panic!("expected ci-failure-reconcile native config");
        };
        assert!(cfg.enabled);
        assert_eq!(cfg.repos, vec!["contoso/claude-run".to_string()]);
        // Scalar form: one token for every repo (CLAUDE-200 back-compat).
        assert_eq!(
            cfg.log_token_ref,
            Some(LogTokenRef::Global("forgejo-logs-contoso".to_string()))
        );
        // Unset fields fall back to the documented defaults.
        assert_eq!(cfg.query, "#Unresolved");
        assert_eq!(
            cfg.summary_prefixes,
            vec!["CI failure:".to_string(), "Auto-fix:".to_string()]
        );
        assert_eq!(cfg.resolved_state, "Done");
        assert_eq!(cfg.max_branch_resolves, 40);
        // CLAUDE-198: duplicate collapse is on unless the action turns it off.
        assert!(cfg.close_duplicates);
        assert!(cfg.duplicate_comment.contains("{canonical}"));
        // CLAUDE-265: the default close-scope branch for a code issue.
        assert_eq!(cfg.default_branch, "main");
    }

    #[test]
    fn ci_failure_reconcile_native_overrides_and_rejects_unknown_keys() {
        let raw = "\
name: cfr
kind: ci-failure-reconcile
native:
  enabled: false
  query: \"project: CLAUDE #Unresolved\"
  summary_prefixes: [\"CI failure:\"]
  repos: [\"contoso/*\", \"fabrikam/forgejo-cli\"]
  log_token_ref: pat
  resolved_state: Closed
  close_comment: \"Fixed by run {run_url}\"
  max_branch_resolves: 5
  close_duplicates: false
  duplicate_comment: \"Dup of {canonical}\"
  default_branch: develop
";
        let loaded = validate_str(raw, "<candidate>").unwrap();
        let Some(NativeConfig::CiFailureReconcile(cfg)) = loaded.action.native else {
            panic!("expected ci-failure-reconcile native config");
        };
        assert!(!cfg.enabled);
        assert_eq!(cfg.query, "project: CLAUDE #Unresolved");
        assert_eq!(cfg.summary_prefixes, vec!["CI failure:".to_string()]);
        assert_eq!(
            cfg.repos,
            vec![
                "contoso/*".to_string(),
                "fabrikam/forgejo-cli".to_string()
            ]
        );
        assert_eq!(cfg.resolved_state, "Closed");
        assert_eq!(cfg.close_comment, "Fixed by run {run_url}");
        assert_eq!(cfg.max_branch_resolves, 5);
        assert!(!cfg.close_duplicates);
        assert_eq!(cfg.duplicate_comment, "Dup of {canonical}");
        assert_eq!(cfg.default_branch, "develop");

        let bogus = format!("{raw}  bogus: nope\n");
        assert!(matches!(
            validate_str(&bogus, "<candidate>").unwrap_err(),
            Error::Yaml(_)
        ));
    }

    // ---- CLAUDE-200: per-repo log-token mapping ---------------------------

    fn per_repo_tokens() -> LogTokenRef {
        LogTokenRef::PerRepo(vec![
            LogTokenEntry {
                repos: vec!["contoso/claude-run".to_string()],
                token_ref: "forgejo-logs-claude-run".to_string(),
            },
            LogTokenEntry {
                repos: vec!["contoso/*".to_string()],
                token_ref: "forgejo-logs-contoso".to_string(),
            },
            LogTokenEntry {
                repos: vec!["initech/*".to_string(), "globex/*".to_string()],
                token_ref: "forgejo-logs-initech".to_string(),
            },
        ])
    }

    /// Exact, per-owner and multi-pattern entries each resolve their own token.
    #[test]
    fn per_repo_mapping_matches_exact_and_owner_globs() {
        let t = per_repo_tokens();
        assert_eq!(
            t.resolve("contoso/claude-run"),
            Some("forgejo-logs-claude-run")
        );
        assert_eq!(
            t.resolve("contoso/governance"),
            Some("forgejo-logs-contoso")
        );
        assert_eq!(
            t.resolve("initech/chat"),
            Some("forgejo-logs-initech")
        );
        assert_eq!(
            t.resolve("globex/runner"),
            Some("forgejo-logs-initech")
        );
    }

    /// First match wins: the exact entry precedes the owner glob that also covers
    /// the repo, so ordering (not specificity) decides.
    #[test]
    fn per_repo_mapping_takes_the_first_matching_entry() {
        assert_eq!(
            per_repo_tokens().resolve("contoso/claude-run"),
            Some("forgejo-logs-claude-run")
        );
        // Reversed order: the owner glob now wins for the same repo.
        let reversed = LogTokenRef::PerRepo(vec![
            LogTokenEntry {
                repos: vec!["contoso/*".to_string()],
                token_ref: "forgejo-logs-contoso".to_string(),
            },
            LogTokenEntry {
                repos: vec!["contoso/claude-run".to_string()],
                token_ref: "forgejo-logs-claude-run".to_string(),
            },
        ]);
        assert_eq!(
            reversed.resolve("contoso/claude-run"),
            Some("forgejo-logs-contoso")
        );
    }

    /// A repo no entry covers resolves to `None`, so the handler skips it instead
    /// of reading Forgejo with another org's token.
    #[test]
    fn per_repo_mapping_returns_none_for_an_unmatched_repo() {
        assert!(per_repo_tokens().resolve("someone-else/repo").is_none());
        assert!(LogTokenRef::PerRepo(Vec::new())
            .resolve("contoso/claude-run")
            .is_none());
    }

    /// The scalar form and an explicit `*` entry both serve every repo.
    #[test]
    fn scalar_and_global_entry_serve_every_repo() {
        let scalar = LogTokenRef::Global("forgejo-logs-contoso".to_string());
        assert_eq!(
            scalar.resolve("contoso/claude-run"),
            Some("forgejo-logs-contoso")
        );
        assert_eq!(
            scalar.resolve("whoever/whatever"),
            Some("forgejo-logs-contoso")
        );
        let star = LogTokenRef::PerRepo(vec![LogTokenEntry {
            repos: vec!["*".to_string()],
            token_ref: "forgejo-logs-contoso".to_string(),
        }]);
        assert_eq!(
            star.resolve("whoever/whatever"),
            Some("forgejo-logs-contoso")
        );
    }

    /// Both YAML forms parse into the same type, so an existing action keeps
    /// working while a new one can map per repo.
    #[test]
    fn both_log_token_yaml_forms_parse() {
        let raw = "\
name: cfa
kind: ci-failure-analysis
native:
  query: \"#Unresolved\"
  log_token_ref:
    - repos: [\"contoso/*\"]
      token_ref: forgejo-logs-contoso
    - repos: [\"initech/*\", \"fabrikam/*\"]
      token_ref: forgejo-logs-initech
";
        let loaded = validate_str(raw, "<candidate>").unwrap();
        let Some(NativeConfig::CiFailureAnalysis(cfg)) = loaded.action.native else {
            panic!("expected ci-failure-analysis native config");
        };
        let tokens = cfg.log_token_ref.expect("mapping parsed");
        assert_eq!(
            tokens.resolve("contoso/claude-run"),
            Some("forgejo-logs-contoso")
        );
        assert_eq!(
            tokens.resolve("fabrikam/forgejo-cli"),
            Some("forgejo-logs-initech")
        );
        assert!(tokens.resolve("elsewhere/repo").is_none());

        // An unknown key inside an entry is rejected, not silently ignored.
        let bogus = raw.replace("token_ref: forgejo-logs-contoso", "tokenref: typo");
        assert!(matches!(
            validate_str(&bogus, "<candidate>").unwrap_err(),
            Error::Yaml(_)
        ));
    }

    /// With no mapping on the action, the webhook sources supply one: each source
    /// serves its own `allowed_repos` with its own `log_token_ref`, so ONE
    /// deployed action copy covers every org (a source with no token is skipped).
    #[test]
    fn webhook_sources_supply_the_default_mapping() {
        use crate::config::{WebhookConfig, WebhookSource};
        let mut webhook = WebhookConfig::default();
        webhook.sources.insert(
            "contoso".to_string(),
            WebhookSource {
                allowed_repos: vec!["contoso/*".to_string()],
                log_token_ref: Some("forgejo-logs-contoso".to_string()),
                ..WebhookSource::default()
            },
        );
        webhook.sources.insert(
            "initech".to_string(),
            WebhookSource {
                allowed_repos: vec!["initech/*".to_string()],
                log_token_ref: Some("forgejo-logs-initech".to_string()),
                ..WebhookSource::default()
            },
        );
        webhook.sources.insert(
            "tokenless".to_string(),
            WebhookSource {
                allowed_repos: vec!["someone-else/*".to_string()],
                log_token_ref: None,
                ..WebhookSource::default()
            },
        );

        let tokens = LogTokenRef::from_webhook_sources(&webhook);
        // Issues from two different orgs, one mapping.
        assert_eq!(
            tokens.resolve("contoso/claude-run"),
            Some("forgejo-logs-contoso")
        );
        assert_eq!(
            tokens.resolve("initech/chat"),
            Some("forgejo-logs-initech")
        );
        // A source carrying no log token contributes no entry, so its repos are
        // skipped rather than attempted with another org's token.
        assert!(tokens.resolve("someone-else/repo").is_none());
        assert!(tokens.resolve("unmapped/repo").is_none());
    }

    /// CLAUDE-200: `forgejo-log-pat` never existed in the deployed secrets store,
    /// so neither shipped action nor the schema doc may name it again.
    #[test]
    fn shipped_ci_failure_config_does_not_name_the_nonexistent_log_secret() {
        for (path, src) in [
            (
                "actions/ci-failure-analysis.yml",
                include_str!("../actions/ci-failure-analysis.yml"),
            ),
            (
                "actions/ci-failure-reconcile.yml",
                include_str!("../actions/ci-failure-reconcile.yml"),
            ),
            ("actions/SCHEMA.md", include_str!("../actions/SCHEMA.md")),
        ] {
            assert!(
                !src.contains("forgejo-log-pat"),
                "{path} still names the nonexistent secret forgejo-log-pat"
            );
        }
    }

    /// CLAUDE-207: the repo-access preflight now probes both scope categories the
    /// PR path uses, so the caveat that it tested repository read only (prose that
    /// stood in for the fix) must not come back in either the schema doc or the
    /// gather doc comment.
    #[test]
    fn preflight_docs_do_not_claim_the_probe_is_weaker_than_the_pr_path() {
        for (path, src) in [
            ("actions/SCHEMA.md", include_str!("../actions/SCHEMA.md")),
            ("src/native/youtrack.rs", include_str!("native/youtrack.rs")),
        ] {
            for needle in ["repository read only", "than the PR path needs"] {
                assert!(
                    !src.contains(needle),
                    "{path} still says the preflight is weaker than the PR path: {needle}"
                );
            }
        }
    }

    /// The default query must NEVER select by `AI Agent`: reconcile is independent
    /// of the code-fix runner's `AI Agent: Queued` selector.
    #[test]
    fn ci_failure_reconcile_default_query_never_selects_by_ai_agent() {
        let cfg = CiFailureReconcileConfig::default();
        assert!(!cfg.query.contains("AI Agent"), "query: {}", cfg.query);
        assert!(!cfg.query.contains("Queued"), "query: {}", cfg.query);
    }

    /// The shipped `actions/ci-failure-reconcile.yml` loads, is recognized as a
    /// native ci-failure-reconcile action with no script-bearing fields, and
    /// round-trips through serialize -> parse unchanged.
    #[test]
    fn shipped_ci_failure_reconcile_action_is_native_and_round_trips() {
        const SHIPPED: &str = include_str!("../actions/ci-failure-reconcile.yml");
        let loaded = validate_str(SHIPPED, "actions/ci-failure-reconcile.yml")
            .expect("shipped action valid");
        let action = loaded.action;
        assert_eq!(action.name, "ci-failure-reconcile");
        assert_eq!(action.kind, ActionKind::CiFailureReconcile);
        assert!(action.input.is_none());
        assert!(action.filter.is_none());
        assert!(action.pre.is_empty());
        assert!(action.prompt.is_none());
        assert!(action.post.is_empty());
        let Some(NativeConfig::CiFailureReconcile(_)) = &action.native else {
            panic!("expected ci-failure-reconcile native config");
        };
        // CLAUDE-222: job keys are YouTrack issue ids, so the run screens link
        // them out through the action's link template.
        let link = action.link_template.as_deref().expect("link_template");
        assert!(link.contains("{key}"), "link_template: {link}");
        let reserialized = serde_yaml::to_string(&action).unwrap();
        let reloaded = validate_str(&reserialized, "<roundtrip>").unwrap();
        assert_eq!(reloaded.action.native, action.native);
        assert_eq!(reloaded.action.kind, action.kind);
    }

    // ---- CLAUDE-218: closed-issue-summary native config -------------------

    #[test]
    fn closed_issue_summary_native_parses_and_defaults() {
        let raw = "\
name: closed-issue-summary
kind: closed-issue-summary
native:
  projects: [CLAUDE, DELTA]
  window_days: 7
";
        let loaded = validate_str(raw, "<candidate>").unwrap();
        assert_eq!(loaded.action.kind, ActionKind::ClosedIssueSummary);
        let Some(NativeConfig::ClosedIssueSummary(cfg)) = loaded.action.native else {
            panic!("expected closed-issue-summary native config");
        };
        assert!(cfg.enabled);
        assert_eq!(cfg.projects, vec!["CLAUDE".to_string(), "DELTA".to_string()]);
        assert_eq!(cfg.window_days, 7);
        // Defaults: no parents, no query, subtask linkage, the parent's project.
        assert!(cfg.parents.is_empty());
        assert!(cfg.query.is_empty());
        assert_eq!(cfg.link_type, "subtask of");
        assert_eq!(cfg.custom_field, "AI Agent");
        assert_eq!(cfg.report_project, None);
        assert_eq!(cfg.max_listed_per_project, 100);
    }

    #[test]
    fn closed_issue_summary_native_overrides_and_rejects_unknown_keys() {
        let raw = "\
name: report
kind: closed-issue-summary
native:
  parents: [CLAUDE-100]
  projects: [CLAUDE]
  window_days: 14
  report_project: RPT
  link_type: relates
  max_listed_per_project: 5
";
        let loaded = validate_str(raw, "<candidate>").unwrap();
        let Some(NativeConfig::ClosedIssueSummary(cfg)) = loaded.action.native else {
            panic!("expected closed-issue-summary native config");
        };
        assert_eq!(cfg.parents, vec!["CLAUDE-100".to_string()]);
        assert_eq!(cfg.window_days, 14);
        assert_eq!(cfg.report_project.as_deref(), Some("RPT"));
        assert_eq!(cfg.link_type, "relates");
        assert_eq!(cfg.max_listed_per_project, 5);

        let bad = "name: r\nkind: closed-issue-summary\nnative:\n  projects: [CLAUDE]\n  nope: 1\n";
        assert!(validate_str(bad, "<candidate>").is_err());
    }

    /// CLAUDE-239: an action may pin a model in its `claude:` block, and the
    /// value round-trips unvalidated (any string the `claude` CLI accepts).
    #[test]
    fn claude_block_accepts_a_model() {
        let raw =
            "name: demo\ninput:\n  command: echo '[]'\nprompt: |\n  'hi'\nclaude:\n  model: opus\n";
        let loaded = validate_str(raw, "<candidate>").unwrap();
        assert_eq!(loaded.action.claude.model.as_deref(), Some("opus"));

        // Absent stays absent, so the runner passes no `--model` flag.
        let raw = "name: demo\ninput:\n  command: echo '[]'\nprompt: |\n  'hi'\n";
        assert!(validate_str(raw, "<candidate>")
            .unwrap()
            .action
            .claude
            .model
            .is_none());
    }

    /// CLAUDE-259: an action may pin an effort level the same way, and any
    /// string parses (a level added after this release must not fail the
    /// `deny_unknown_fields` loader).
    #[test]
    fn claude_block_accepts_an_effort() {
        let raw =
            "name: demo\ninput:\n  command: echo '[]'\nprompt: |\n  'hi'\nclaude:\n  effort: high\n";
        let loaded = validate_str(raw, "<candidate>").unwrap();
        assert_eq!(loaded.action.claude.effort.as_deref(), Some("high"));

        let raw = "name: demo\ninput:\n  command: echo '[]'\nprompt: |\n  'hi'\nclaude:\n  effort: colossal\n";
        let loaded = validate_str(raw, "<candidate>").unwrap();
        assert_eq!(loaded.action.claude.effort.as_deref(), Some("colossal"));

        // Absent stays absent, so the runner passes no `--effort` flag.
        let raw = "name: demo\ninput:\n  command: echo '[]'\nprompt: |\n  'hi'\n";
        assert!(validate_str(raw, "<candidate>")
            .unwrap()
            .action
            .claude
            .effort
            .is_none());
    }

    /// The shipped `actions/closed-issue-summary.yml` loads, is recognized as a
    /// native closed-issue-summary action with no script-bearing fields, and
    /// round-trips through serialize -> parse unchanged.
    #[test]
    fn shipped_closed_issue_summary_action_is_native_and_round_trips() {
        const SHIPPED: &str = include_str!("../actions/closed-issue-summary.yml");
        let loaded = validate_str(SHIPPED, "actions/closed-issue-summary.yml")
            .expect("shipped action valid");
        let action = loaded.action;
        assert_eq!(action.name, "closed-issue-summary");
        assert_eq!(action.kind, ActionKind::ClosedIssueSummary);
        assert!(action.input.is_none());
        assert!(action.filter.is_none());
        assert!(action.pre.is_empty());
        assert!(action.prompt.is_none());
        assert!(action.post.is_empty());
        // A report run starts no agent session, so it configures no claude block.
        assert!(action.claude.allowed_tools.is_none());
        assert!(action.claude.permission_mode.is_none());
        let Some(NativeConfig::ClosedIssueSummary(cfg)) = &action.native else {
            panic!("expected closed-issue-summary native config");
        };
        assert!(!cfg.projects.is_empty(), "the reporter needs projects");
        let reserialized = serde_yaml::to_string(&action).unwrap();
        let reloaded = validate_str(&reserialized, "<roundtrip>").unwrap();
        assert_eq!(reloaded.action.native, action.native);
        assert_eq!(reloaded.action.kind, action.kind);
    }

    /// CLAUDE-218 query partition: the reporting action and the worker action
    /// must never gather the same issue. The worker pins the single `AI Agent`
    /// value `Queued`; the reporter ships with no query at all, and its
    /// `parents` list is nominated by id, so no issue can match both. This is
    /// the mechanical guard `actions/youtrack.yml` points at.
    #[test]
    fn shipped_action_queries_are_disjoint() {
        let worker = validate_str(
            include_str!("../actions/youtrack.yml"),
            "actions/youtrack.yml",
        )
        .unwrap()
        .action;
        let reporter = validate_str(
            include_str!("../actions/closed-issue-summary.yml"),
            "actions/closed-issue-summary.yml",
        )
        .unwrap()
        .action;

        let Some(NativeConfig::Youtrack(w)) = &worker.native else {
            panic!("expected youtrack native config");
        };
        let Some(NativeConfig::ClosedIssueSummary(r)) = &reporter.native else {
            panic!("expected closed-issue-summary native config");
        };

        assert!(
            w.youtrack.query.contains("AI Agent: Queued"),
            "the worker owns the Queued value: {}",
            w.youtrack.query
        );
        assert!(
            r.query.trim().is_empty() || !r.query.contains("AI Agent: Queued"),
            "the reporting query must not select the worker's value: {}",
            r.query
        );
    }

    #[test]
    fn validate_str_rejects_unknown_kind() {
        let raw = "name: yt\nkind: bogus\nnative: {}\n";
        assert!(matches!(
            validate_str(raw, "<candidate>").unwrap_err(),
            Error::Yaml(_)
        ));
    }

    #[test]
    fn scripted_kind_still_requires_prompt() {
        // An explicit `kind: scripted` validates its script fields as before.
        let raw = "name: demo\nkind: scripted\ninput:\n  command: echo '[]'\n";
        let err = validate_str(raw, "<candidate>").unwrap_err();
        assert!(matches!(err, Error::ActionMissingField { field, .. } if field == "prompt"));
    }
}

fn default_enabled() -> bool {
    true
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct InputBlock {
    pub command: Script,
}

#[derive(Debug, Clone, Default, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct ClaudeBlock {
    pub cwd: Option<String>,
    pub allowed_tools: Option<String>,
    pub permission_mode: Option<String>,
    /// CLAUDE-239: model this action pins, passed through to `--model`
    /// untouched (no validation here; the `claude` CLI owns that). Unset falls
    /// back to the global `config.claude.model`, then to the CLI's own default.
    pub model: Option<String>,
    /// CLAUDE-259: effort level this action pins, passed through to `--effort`
    /// untouched. Unset falls back to the global `config.claude.effort`, then to
    /// the CLI's own default. Deliberately a string, not an enum: a level
    /// Anthropic adds later must parse here without a runner release. The runner
    /// warns about a value outside the documented set but never rejects it.
    pub effort: Option<String>,
}

#[derive(Debug, Clone, Default, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct OutputBlock {
    pub file: Option<String>,
}

#[derive(Debug, Clone, Default, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct Limits {
    pub max_attempts_per_key: Option<u32>,
    pub max_runs_per_day: Option<u32>,
    pub claude_timeout_seconds: Option<u32>,
    pub daily_budget_usd: Option<f64>,
    /// CLAUDE-234: issues ONE run may create in the tracker. `max_runs_per_day`
    /// bounds how often an action fires; this bounds what a single firing can
    /// author, so a misfiring run opens a handful of issues rather than a
    /// hundred. Unset falls back to
    /// [`crate::provider::DEFAULT_MAX_ISSUES_PER_RUN`]; `0` creates nothing.
    pub max_issues_per_run: Option<u32>,
}

/// CLAUDE-28: per-action keyed parallel execution.
///
/// Within a run, jobs are partitioned by a *concurrency key* resolved from each
/// job's fields ([`Concurrency::key`]). At most one job per resolved key runs at
/// a time (same key serializes); distinct keys run in parallel up to
/// [`Concurrency::max_parallel`]. Per-key concurrency is fixed at 1 - two jobs
/// against the same repo would collide on the clone dir, branch, and PR - so
/// only `max_parallel` is tunable.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct Concurrency {
    /// Maximum distinct-key jobs in flight at once within a run. `1` (the
    /// default) reproduces the original strictly-sequential behavior.
    pub max_parallel: u32,
    /// Job fields whose values compose the concurrency key, in order. Defaults
    /// to the project / repo / workflow tuple. Configurable, not hard-coded, so
    /// the serialization scope can be widened (e.g. a whole project) or narrowed
    /// without a code change. `workflow` is forward-looking (reserved for a
    /// future Forgejo-failed-workflow action) and unset on YouTrack jobs today.
    pub key: Vec<String>,
}

impl Default for Concurrency {
    fn default() -> Self {
        Self {
            max_parallel: 1,
            key: default_concurrency_key(),
        }
    }
}

fn default_concurrency_key() -> Vec<String> {
    vec![
        "project".to_string(),
        "repo".to_string(),
        "workflow".to_string(),
    ]
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields, rename_all = "snake_case")]
pub struct Schedule {
    pub kind: ScheduleKind,
    #[serde(default)]
    pub spec: Option<String>,
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ScheduleKind {
    Interval,
    Cron,
    Manual,
}

/// Action + provenance: original file path and SHA-256 of the on-disk YAML.
/// The DB indexes the hash so the daemon can detect on-disk edits.
#[derive(Debug, Clone)]
pub struct LoadedAction {
    pub action: Action,
    pub path: PathBuf,
    pub yaml_sha256: String,
}

/// Load a YAML file as an [`Action`]. Path resolution rules:
///
/// - If `arg` exists as a file, use it.
/// - Else, look for `<actions_dir>/<arg>.yml` and `.yaml`.
pub fn resolve_action_path(arg: &str, actions_dir: &Path) -> Option<PathBuf> {
    let direct = PathBuf::from(arg);
    if direct.is_file() {
        return Some(direct);
    }
    for ext in ["yml", "yaml"] {
        let p = actions_dir.join(format!("{}.{}", arg, ext));
        if p.is_file() {
            return Some(p);
        }
    }
    None
}

pub fn load_from_path(path: &Path) -> Result<LoadedAction> {
    let raw = std::fs::read_to_string(path)?;
    let mut loaded = validate_str(&raw, &path.display().to_string())?;
    loaded.path = path.to_path_buf();
    Ok(loaded)
}

/// Validate raw YAML as an [`Action`] without touching disk, computing the
/// same sha256 provenance [`load_from_path`] does. `label` names the candidate
/// source in error messages (e.g. the would-be file path); the returned
/// `path` is set to it but callers that persist should overwrite it with the
/// real destination. Used by the web UI to reject broken YAML before it ever
/// reaches `actions/<name>.yml`.
pub fn validate_str(raw: &str, label: &str) -> Result<LoadedAction> {
    let action: Action = serde_yaml::from_str(raw)?;

    // Required-field validation that goes beyond what serde catches with
    // `#[serde(deny_unknown_fields)]` (which already catches typos).
    if action.name.trim().is_empty() {
        return Err(Error::ActionMissingField {
            name: "(unnamed)".into(),
            field: "name".into(),
            path: label.to_string(),
        });
    }

    // CLAUDE-30: `kind` selects which fields are required. Scripted actions
    // validate their script-bearing fields exactly as before; native kinds
    // ignore those and require a matching typed `NativeConfig` instead.
    if action.kind.is_native() {
        validate_native(&action, label)?;
    } else {
        validate_scripted(&action, label)?;
    }

    let mut hasher = Sha256::new();
    hasher.update(raw.as_bytes());
    let yaml_sha256 = hex::encode(hasher.finalize());

    Ok(LoadedAction {
        action,
        path: PathBuf::from(label),
        yaml_sha256,
    })
}

/// Validate the script-bearing fields a `scripted` action requires:
/// a non-empty `input.command` and a non-empty `prompt`.
fn validate_scripted(action: &Action, label: &str) -> Result<()> {
    let missing = |field: &str| Error::ActionMissingField {
        name: action.name.clone(),
        field: field.to_string(),
        path: label.to_string(),
    };
    let input = action
        .input
        .as_ref()
        .ok_or_else(|| missing("input.command"))?;
    if input.command.code().trim().is_empty() {
        return Err(missing("input.command"));
    }
    let prompt = action.prompt.as_ref().ok_or_else(|| missing("prompt"))?;
    if prompt.code().trim().is_empty() {
        return Err(missing("prompt"));
    }
    Ok(())
}

/// Validate a native action: it must carry a `native` config whose variant
/// matches `kind`. The script-bearing fields are not required.
fn validate_native(action: &Action, label: &str) -> Result<()> {
    let native = action
        .native
        .as_ref()
        .ok_or_else(|| Error::ActionMissingField {
            name: action.name.clone(),
            field: "native".to_string(),
            path: label.to_string(),
        })?;
    if native.kind() != action.kind {
        return Err(Error::ActionKindMismatch {
            name: action.name.clone(),
            kind: action.kind.as_str().to_string(),
            native: native.kind().as_str().to_string(),
            path: label.to_string(),
        });
    }
    // CLAUDE-161: reject a `dependencies.recheck_timeout` that is not a valid
    // humantime duration at load, so a bad value fails fast instead of at gate
    // time (where it would silently fall back to the default).
    if let NativeConfig::Youtrack(cfg) = native {
        let raw = &cfg.youtrack.dependencies.recheck_timeout;
        if let Err(e) = humantime::parse_duration(raw) {
            return Err(Error::ActionInvalidField {
                name: action.name.clone(),
                field: "native.youtrack.dependencies.recheck_timeout".to_string(),
                reason: format!("`{raw}` is not a valid duration: {e}"),
                path: label.to_string(),
            });
        }
    }
    Ok(())
}

/// Resolve `arg` to a path and load. Convenience for the CLI.
pub fn load(arg: &str, actions_dir: &Path) -> Result<LoadedAction> {
    let path = resolve_action_path(arg, actions_dir).ok_or_else(|| Error::ActionNotFound {
        name: arg.to_string(),
        searched: actions_dir.display().to_string(),
    })?;
    load_from_path(&path)
}
