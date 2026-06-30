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
}

impl ActionKind {
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
}

impl NativeConfig {
    /// The [`ActionKind`] this config variant corresponds to.
    pub fn kind(&self) -> ActionKind {
        match self {
            NativeConfig::Youtrack(_) => ActionKind::Youtrack,
            NativeConfig::ForgejoCiFix(_) => ActionKind::ForgejoCiFix,
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
    /// `{{workdir}}`, `{{needs_review_marker}}`, `{{skip_attempt_marker}}`).
    pub prompt: Option<String>,
    /// Optional CI-gated automerge of the opened PR (CLAUDE-126). Disabled by
    /// default, so finalize stops at Code Review exactly as before; when enabled
    /// the runner waits for the PR's checks to go green and merges it.
    #[serde(skip_serializing_if = "Automerge::is_default")]
    pub automerge: Automerge,
    /// Optional Assignee to set on the issue during pickup (CLAUDE-131). A literal
    /// `yt` assignee argument (a login, `me`, or `unassigned`). `None` (the
    /// default) leaves the Assignee untouched, exactly as before.
    pub assignee: Option<String>,
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
            automerge: Automerge::default(),
            assignee: None,
        }
    }
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
            in_progress: "In Progress".to_string(),
            state_done: "Done".to_string(),
        }
    }
}

/// How the runner opens the PR for a worked Forgejo issue (CLAUDE-103).
///
/// `Branch` (the default) pushes a real server branch with the git CLI
/// (`git push --set-upstream origin HEAD`) and opens the PR over the API, so
/// Forgejo can always offer "Update Branch" and the PR stays mergeable.
/// `Agit` runs `fj pr create -aA`, which creates a branchless PR whose head
/// lives only at `refs/pull/<N>/head`; such a PR loses its merge button the
/// moment it falls behind a base that requires an up-to-date branch. AGit is
/// kept as an explicit opt-in, never a silent fallback (the selected strategy
/// is authoritative).
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
/// CI status is `failure` and that were updated within `window_minutes`, skips
/// PRs authored by `bot_author` (anti-feedback-loop), clones the PR branch,
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
        assert_eq!(cfg.youtrack.query, "-Resolved AI Agent: Queued");
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
