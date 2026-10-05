//! XDG-aware path resolution + on-disk config.
//!
//! Layout (defaults; each can be overridden by the corresponding env var):
//!
//! | Purpose | Default | Env override |
//! |---|---|---|
//! | Config (actions, config.toml) | `$XDG_CONFIG_HOME/claude-run/` | `CLAUDE_RUN_CONFIG_DIR` |
//! | Data (state.db, workdirs) | `$XDG_DATA_HOME/claude-run/` | `CLAUDE_RUN_DATA_DIR` |
//! | State (logs) | `$XDG_STATE_HOME/claude-run/` | `CLAUDE_RUN_STATE_DIR` |
//! | Runtime (pid, socket) | `$XDG_RUNTIME_DIR/claude-run/` | `CLAUDE_RUN_RUNTIME_DIR` |
//!
//! `state.db` is the DEFAULT state store, not the only one. A
//! `[database] url` (or `CLAUDE_RUN_DATABASE_URL`, or the file named by
//! `CLAUDE_RUN_DATABASE_URL_FILE`) points the daemon at PostgreSQL instead, in
//! which case nothing but `workdirs/` lives under the data dir. See
//! [`resolve_database`].
//!
//! `~`, `$VAR`, and `${VAR}` are expanded in all values (the env override and
//! the `workdir_base` field on actions both go through `expand_path`).

use std::collections::BTreeMap;
use std::path::PathBuf;

use serde::{Deserialize, Serialize};

use crate::error::Result;
use crate::web::webhooks::WebhookProvider;

/// The per-run usage record, directly under `logs/`. Shared with
/// `crate::gc` so the writer's path and the sweep's target cannot drift apart.
pub const USAGE_LOG: &str = "usage.jsonl";

/// Resolved on-disk locations. Computed once at startup.
#[derive(Debug, Clone)]
pub struct Paths {
    pub config_dir: PathBuf,
    pub data_dir: PathBuf,
    pub state_dir: PathBuf,
    pub runtime_dir: PathBuf,
}

impl Paths {
    pub fn resolve() -> Result<Self> {
        Ok(Self {
            config_dir: resolve_dir("CLAUDE_RUN_CONFIG_DIR", dirs::config_dir(), "claude-run")?,
            data_dir: resolve_dir("CLAUDE_RUN_DATA_DIR", dirs::data_dir(), "claude-run")?,
            state_dir: resolve_dir("CLAUDE_RUN_STATE_DIR", dirs::state_dir(), "claude-run")?,
            runtime_dir: resolve_dir(
                "CLAUDE_RUN_RUNTIME_DIR",
                dirs::runtime_dir()
                    .or_else(|| Some(std::env::temp_dir().join("claude-run-runtime"))),
                "claude-run",
            )?,
        })
    }

    pub fn actions_dir(&self) -> PathBuf {
        self.config_dir.join("actions")
    }

    pub fn config_file(&self) -> PathBuf {
        self.config_dir.join("config.toml")
    }

    pub fn state_db(&self) -> PathBuf {
        self.data_dir.join("state.db")
    }

    pub fn logs_dir(&self) -> PathBuf {
        self.state_dir.join("logs")
    }

    pub fn workdirs_dir(&self) -> PathBuf {
        self.data_dir.join("workdirs")
    }

    pub fn snippets_dir(&self) -> PathBuf {
        self.state_dir.join("snippets")
    }

    pub fn webhook_logs_dir(&self) -> PathBuf {
        self.state_dir.join("webhook-logs")
    }

    /// The newline-delimited per-run usage record, one line per run,
    /// so runs are comparable over time without a provider console.
    pub fn usage_log(&self) -> PathBuf {
        self.logs_dir().join(USAGE_LOG)
    }
}

fn resolve_dir(env_var: &str, xdg_default: Option<PathBuf>, subdir: &str) -> Result<PathBuf> {
    if let Ok(raw) = std::env::var(env_var) {
        return Ok(PathBuf::from(expand_path(&raw)));
    }
    let base =
        xdg_default.ok_or_else(|| anyhow::anyhow!("could not resolve XDG dir for {}", env_var))?;
    Ok(base.join(subdir))
}

/// Expand `~`, `$VAR`, and `${VAR}` in a path string. No nu evaluation.
/// Used for the `workdir_base` action field and the env-override values.
pub fn expand_path(raw: &str) -> String {
    shellexpand::full(raw)
        .map(|cow| cow.into_owned())
        .unwrap_or_else(|_| raw.to_string())
}

/// `config.toml` schema. All fields optional; absence means "use built-in default".
/// Runtime-tunable settings live in the DB (`settings` table); this file is
/// for things that have to be known before the DB is opened.
#[derive(Debug, Clone, Default, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct OnDiskConfig {
    pub web: WebConfig,
    pub daemon: DaemonConfig,
    /// Forgejo webhook listener settings (HMAC secret reference,
    /// repo allow-list, replay window, rate limit).
    pub webhook: WebhookConfig,
    /// Triage-driven issue-mint settings (repo-to-project map,
    /// fallback project, loop-safety bot logins, bucket-2 assignee).
    pub mint: MintConfig,
    /// `version` self-update settings (release/latest train default
    /// and the name of the secret holding the registry token).
    pub update: UpdateConfig,
    /// Which database backend holds the runtime state. Bootstrap,
    /// not a DB-backed setting: it has to be known before the DB is open.
    pub database: DatabaseConfig,
}

/// Backend selection for the runtime state store.
#[derive(Debug, Clone, Default, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct DatabaseConfig {
    /// Connection URL for the state store. Unset (the default) means SQLite at
    /// [`Paths::state_db`], which is what every install had before this key
    /// existed. `postgres://` or `postgresql://` selects PostgreSQL;
    /// `sqlite://` selects SQLite explicitly. Any other scheme is a startup
    /// error naming the scheme, never a silent fallback.
    ///
    /// Precedence, highest first: the contents of the file named by
    /// `CLAUDE_RUN_DATABASE_URL_FILE`, then `CLAUDE_RUN_DATABASE_URL`, then
    /// this key. The file form exists because the URL carries the password and
    /// an environment value is readable through `docker inspect` and `/proc`,
    /// the same reasoning that puts the age key on a compose secret.
    pub url: Option<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct WebConfig {
    pub bind: String,
    pub port: u16,
    /// Name of the stored secret holding the
    /// bearer token that gates the web UI and `/api/*` when the bind is
    /// non-loopback. Unset is fine for a loopback bind; required (else the TCP
    /// listener fails closed) for any exposed bind.
    pub auth_token_ref: Option<String>,
    /// Require the bearer token even on a loopback bind.
    /// Off by default so local development stays unauthenticated; set true to
    /// exercise the gate locally.
    pub require_auth_on_loopback: bool,
    /// Username for the browser login form. When set (and auth is required), an
    /// unauthenticated browser request is redirected to `/login`, where this
    /// username plus the `auth_token_ref` secret value (as the password) mint a
    /// session cookie. Lets a password manager (e.g. Bitwarden) sign in without
    /// crafting an `Authorization: Bearer` header. Unset disables the form;
    /// `Authorization: Bearer <token>` still works for API clients.
    pub auth_username: Option<String>,
}

impl Default for WebConfig {
    fn default() -> Self {
        Self {
            bind: "127.0.0.1".to_string(),
            port: 7321,
            auth_token_ref: None,
            require_auth_on_loopback: false,
            auth_username: None,
        }
    }
}

/// Forgejo webhook listener config. Lives in `config.toml`
/// because the daemon needs it before it binds the TCP listener.
///
/// The listener is multi-source: each `[webhook.sources.<name>]`
/// table is one webhook endpoint at `POST /webhooks/forgejo/<name>` with its own
/// HMAC secret, allow-list, and rate-limit bucket. The replay window is shared
/// across sources (`X-Forgejo-Delivery` ids are globally unique). An empty
/// `sources` map disables the listener entirely (the old "no `secret_ref`"
/// behaviour). The old top-level `secret_ref` / `allowed_repos` /
/// `rate_limit_per_min` keys are gone; `deny_unknown_fields` makes a config still
/// carrying them fail to parse at serve start, naming the offending key, which is
/// the intended migration signal.
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(default, deny_unknown_fields)]
pub struct WebhookConfig {
    /// CLAUDE-360: master on/off switch for the listener, defaulting `false`.
    /// While `false`, `/webhooks/forgejo/<source>` 404s every request
    /// regardless of `sources`, with its own log line distinct from an
    /// unknown-source rejection. A save that sets this `true` is rejected
    /// (`src/settings.rs::validate_webhook`) unless `sources` is non-empty and
    /// every entry has `secret_ref` set, so "enabled" can never mean "accepting
    /// requests with no real authentication configured behind it." An upgrade
    /// from a version with no such flag deserializes a stored document with no
    /// `enabled` key to this default, so a pre-existing, fully-configured
    /// listener goes quiet on upgrade; `serve` start warns once when `sources`
    /// is non-empty but this is `false`, naming the section.
    pub enabled: bool,
    /// Replay-protection window in seconds, shared across all sources. A delivery
    /// whose `X-Forgejo-Delivery` id was already seen inside this window is
    /// rejected with 409 (a duplicate delivered to two sources is caught too).
    pub replay_window_secs: u64,
    /// Per-source webhook endpoints, keyed by source name. The key is used
    /// verbatim as the `<name>` URL path segment in `/webhooks/forgejo/<name>`,
    /// so a lowercase `[a-z0-9_-]` name needing no URL escaping is expected;
    /// names with other characters are warned about at load. An empty map
    /// disables the listener (no endpoints, fail-closed like an unset secret).
    pub sources: BTreeMap<String, WebhookSource>,
    /// Webhook-scoped auto-fix switch. It no longer
    /// decides whether an issue is minted at all (every code failure is filed,
    /// so `No orphan notes` holds even with this off): `false` (the default,
    /// opt-in) still creates a `code_fix` / `auto_fix` issue but leaves it with
    /// no `AI Agent` write, so no corrective fixup PR is queued and a red PR
    /// check stays reported on the original PR for its author to fix in place;
    /// `true` sets `AI Agent = Queued` on that issue
    /// as before. A `human_action` failure is unaffected either way. This gates
    /// only the webhook fixup queuing; it is independent of the framework-wide
    /// `[mint].auto_fix.enabled` detector switch and of the open-PR
    /// suppression. Being on `WebhookConfig` it is DB-backed (`config.webhook`)
    /// and edited live on the Settings page / via `PUT /api/settings/webhook`
    /// with no restart.
    pub auto_fix: bool,
    /// Timeout (seconds) for the at-ingest [`crate::triage::AnalysisClassifier`]
    /// session, which runs `Read,Grep,Bash` over the complete log and the
    /// failing workflow YAML. Default 600 (the retired scheduled analysis
    /// action's timeout), well past the old tool-less classifier's 120s.
    pub analysis_timeout_secs: u64,
    /// How many events [`crate::web::webhooks::consume_events`]
    /// analyzes concurrently, each behind its own `analysis_timeout_secs`
    /// session. Must be at least 1 (validated in `src/settings.rs`); default 2.
    pub analysis_concurrency: usize,
    /// How many CI failures on the agent's own open PR are routed
    /// back to its issue (comment + `AI Agent = Queued`) before the issue is
    /// flagged `Needs Review` instead. Matched against
    /// `agent_pull_requests.fix_attempts`. Default 3.
    pub agent_pr_fix_attempts: u32,
    /// Daily run cap of the synthetic `webhook-forgejo` action's
    /// ledger, read per event so a Settings save applies to the next one.
    /// Must be at least 1 (validated in `src/settings.rs`); default 20.
    pub max_runs_per_day: u32,
}

impl Default for WebhookConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            replay_window_secs: 300,
            sources: BTreeMap::new(),
            analysis_timeout_secs: 600,
            analysis_concurrency: 2,
            auto_fix: false,
            agent_pr_fix_attempts: 3,
            max_runs_per_day: 20,
        }
    }
}

/// True when `repo` (an `owner/repo` full name) matches one of the allow-list
/// patterns: exact `owner/repo`, per-owner `owner/*`, or the global
/// `*`. Only these three literal forms - no general globbing - so a typo cannot
/// silently widen what a consumer acts on. Matching is case-sensitive; an empty
/// list matches nothing (fail closed). Shared by the webhook allow-list, the
/// reconciler's `repos`, and the per-repo log-token mapping so all
/// three read a pattern the same way.
pub fn repo_matches(patterns: &[String], repo: &str) -> bool {
    patterns.iter().any(|pat| match pat.as_str() {
        "*" => true,
        p => match p.strip_suffix("/*") {
            Some(owner) => repo.split_once('/').is_some_and(|(o, _)| o == owner),
            None => p == repo,
        },
    })
}

/// True when `name` is a clean source name (lowercase `[a-z0-9_-]`, non-empty)
/// usable verbatim as a URL path segment with no escaping. Source names outside
/// this set still work as map keys but produce an ambiguous / escaped URL, so
/// [`WebhookConfig::warn_unusual_source_names`] flags them at load.
fn source_name_is_clean(name: &str) -> bool {
    !name.is_empty()
        && name
            .chars()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_' || c == '-')
}

impl WebhookConfig {
    /// Warn at load for any source name that is not lowercase
    /// `[a-z0-9_-]`. The name is the `/webhooks/forgejo/<name>` path segment, so
    /// an unusual name needs URL escaping in the Forgejo webhook URL and is a
    /// likely typo; the source still loads (the map key is honored) but the
    /// operator is told to expect an escaped URL.
    pub fn warn_unusual_source_names(&self) {
        for name in self.sources.keys() {
            if !source_name_is_clean(name) {
                tracing::warn!(
                    source = %name,
                    "webhook source name is not lowercase [a-z0-9_-]; the URL path segment will need escaping"
                );
            }
        }
    }
}

/// One webhook source. Each source is an independent endpoint with its
/// own HMAC secret and allow-list, so a leaked org secret can authenticate only
/// that org's allow-listed payloads and one org's failure storm rate-starves only
/// its own bucket. The HMAC secret itself is never inlined here; `secret_ref`
/// names a credential in the `secrets` table, resolved (and decrypted)
/// at request time.
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(default, deny_unknown_fields)]
pub struct WebhookSource {
    /// Name of the secret (in the `secrets` table) holding the webhook HMAC
    /// secret for this source's configured [`WebhookSource::provider`]. When
    /// unset, this source's endpoint fails closed and rejects every request
    /// with 401.
    pub secret_ref: Option<String>,
    /// Allow-list of repo patterns for this source. Only `action_run_failure`
    /// events from a matching repo produce a verified-failure event; every other
    /// authenticated request is acked (2xx) and dropped so the forge does not
    /// retry. Three pattern forms: exact `owner/repo`, per-owner
    /// `owner/*`, and the global `*`. Matching is case-sensitive; an empty list
    /// denies all (fail closed).
    pub allowed_repos: Vec<String>,
    /// Fixed-window rate limit: max accepted requests per minute for this source.
    /// Excess requests get 429 before any crypto work. Per-source, so one source
    /// exhausting its budget never 429s another.
    pub rate_limit_per_min: u32,
    /// Name of the secret (in the `secrets` table) holding a
    /// read-scoped PAT that authorizes this source's `fj actions logs` reads
    /// (Forgejo's `read:repository` scope). It reaches the `fj` child in a temp
    /// single-host `keys.json` carrying that one secret and nothing else. Unset,
    /// the fetch runs unauthenticated, which serves a public repo; a private
    /// failure then mints a human-action issue with no log attached.
    pub log_token_ref: Option<String>,
    /// Name of the secret (in the `secrets` table) holding a
    /// PAT that authorizes commenting on this source's pull requests
    /// (routing a CI failure on an open PR back to its issue, or onto the
    /// human author's PR). Reaches `fj` the same way `log_token_ref` does: a
    /// temp single-host `keys.json` carrying only this one secret. CLAUDE-347:
    /// required on every source of an `enabled` listener, same as
    /// `secret_ref` - unset fails `/settings` save and `serve` startup,
    /// naming this source, rather than only surfacing when the comment
    /// backend needs it and cannot find it.
    pub write_token_ref: Option<String>,
    /// The forge sending this source's deliveries, and the `:provider` URL
    /// path segment (`/webhooks/<provider>/triage/<source>`) the request
    /// must arrive on. `#[serde(default)]` so every source stored before this
    /// field existed deserializes as [`WebhookProvider::Forgejo`], which was
    /// the only behavior before CLAUDE-361 split the route on provider.
    #[serde(default)]
    pub provider: WebhookProvider,
}

impl Default for WebhookSource {
    fn default() -> Self {
        Self {
            secret_ref: None,
            allowed_repos: Vec::new(),
            rate_limit_per_min: 60,
            log_token_ref: None,
            write_token_ref: None,
            provider: WebhookProvider::Forgejo,
        }
    }
}

/// `/claude <instruction>` pull-request comment commands. A SECOND,
/// independent listener from [`WebhookConfig`]: its token pushes branches, while a
/// CI-failure source's tokens only read logs and comment, so a push-capable token
/// never shares a config block with them.
///
/// DB-only from day one (settings key `config.webhook_comment`, edited on the web
/// Settings page / via `PUT /api/settings/webhook_comment`). It is deliberately
/// absent from [`OnDiskConfig`], so a `config.toml` declaring `[webhook_comment]`
/// fails to parse as an unknown key, and there is no file-to-DB import path.
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(default, deny_unknown_fields)]
pub struct CommentWebhookConfig {
    /// CLAUDE-360: master on/off switch for the listener, mirroring
    /// [`WebhookConfig::enabled`] (same default, same save-time secret-presence
    /// requirement in `src/settings.rs::validate_comment_webhook`, same startup
    /// warning when `sources` is non-empty but this is `false`).
    pub enabled: bool,
    /// Command prefix the first line of a comment must start with, e.g.
    /// `/claude fix the clippy lint`. Configurable so an operator can rename the
    /// command without a code change.
    pub trigger: String,
    /// Maximum accepted instruction length in characters. A longer body is
    /// refused with a reply naming the limit rather than handed to the agent.
    pub max_instruction_chars: usize,
    /// Per-source comment endpoints, keyed by source name. The key is the
    /// `<name>` path segment of `/webhooks/forgejo-comment/<name>`, read the same
    /// way [`WebhookConfig::sources`] keys are. An empty map disables the
    /// listener (no endpoints, fail closed).
    pub sources: BTreeMap<String, CommentWebhookSource>,
    /// Daily run cap of the synthetic `webhook-forgejo-comment`
    /// action's ledger, read per command so a Settings save applies to the next
    /// one. Must be at least 1 (validated in `src/settings.rs`); default 20.
    pub max_runs_per_day: u32,
    /// Per-run `claude` session timeout in seconds for the `/claude` comment
    /// listener, read live per command (CLAUDE-364, mirroring
    /// `WebhookConfig::analysis_timeout_secs`) instead of the daemon-wide
    /// `RunnerDefaults::claude_timeout_seconds`. Must be at least 1 (validated
    /// in `src/settings.rs`); default 600 matches the prior hard-coded value.
    pub claude_timeout_seconds: u32,
}

impl Default for CommentWebhookConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            trigger: "/claude".to_string(),
            max_instruction_chars: 4000,
            sources: BTreeMap::new(),
            max_runs_per_day: 20,
            claude_timeout_seconds: 600,
        }
    }
}

/// Default git commit identity used wherever the runner authors a commit and
/// nothing more specific is configured: the comment-webhook's own corrective
/// commits ([`CommentWebhookSource::bot_author`] / `bot_email`) and
/// [`AuthConfig::git_author_name`] / `git_author_email` when unset.
pub const DEFAULT_GIT_AUTHOR_NAME: &str = "claude-run";
/// See [`DEFAULT_GIT_AUTHOR_NAME`].
pub const DEFAULT_GIT_AUTHOR_EMAIL: &str = "claude-run@a8n.run";

impl CommentWebhookConfig {
    /// Warn at load for any source name needing URL escaping, exactly as
    /// [`WebhookConfig::warn_unusual_source_names`] does for the CI-failure
    /// sources (same predicate, same reasoning).
    pub fn warn_unusual_source_names(&self) {
        for name in self.sources.keys() {
            if !source_name_is_clean(name) {
                tracing::warn!(
                    source = %name,
                    "comment webhook source name is not lowercase [a-z0-9_-]; the URL path segment will need escaping"
                );
            }
        }
    }
}

/// One comment-webhook source. Same isolation model as
/// [`WebhookSource`] (own HMAC secret, own allow-list, own rate bucket) plus the
/// write-scoped PAT and the authorization / decision-gate knobs the comment
/// pipeline needs.
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(default, deny_unknown_fields)]
pub struct CommentWebhookSource {
    /// Name of the secret holding this source's webhook HMAC secret for its
    /// configured [`CommentWebhookSource::provider`]. Unset means the endpoint
    /// fails closed and 401s every request.
    pub secret_ref: Option<String>,
    /// Allow-list of repo patterns, read by [`repo_matches`] (the same three
    /// forms as the CI-failure sources; empty denies all).
    pub allowed_repos: Vec<String>,
    /// Fixed-window rate limit: max accepted requests per minute for this source.
    pub rate_limit_per_min: u32,
    /// Name of the secret holding a PAT with WRITE scope. It authorizes
    /// the commenter's permission read, the PR replies, and the branch push, and
    /// reaches `fj` in a temp single-host `keys.json` carrying that one secret.
    /// CLAUDE-347: required on every source of an `enabled` listener, same as
    /// `secret_ref` - unset fails `/settings` save and `serve` startup,
    /// naming this source, rather than discovering it when a delivery is
    /// refused.
    pub write_token_ref: Option<String>,
    /// Repository permissions that may run a command. A Forgejo access level (read, write,
    /// admin, owner) also admits every higher level; any other value matches exactly.
    pub allowed_permissions: Vec<String>,
    /// Decision-gate cap: a working-tree diff touching more files than this is
    /// commented on rather than pushed.
    pub max_files: u64,
    /// Decision-gate cap: a working-tree diff changing more lines than this is
    /// commented on rather than pushed.
    pub max_lines: u64,
    /// Git identity the corrective commit is authored as.
    pub bot_author: String,
    /// Git email the corrective commit is authored as.
    pub bot_email: String,
    /// Marker appended to the commit message so the bot's own push does not
    /// re-trigger the CI-fix action.
    pub skip_marker: String,
    /// The forge sending this source's deliveries, and the `:provider` URL
    /// path segment (`/webhooks/<provider>/comment/<source>`) the request
    /// must arrive on. See [`WebhookSource::provider`] for the same
    /// `#[serde(default)]` upgrade reasoning.
    #[serde(default)]
    pub provider: WebhookProvider,
}

impl Default for CommentWebhookSource {
    fn default() -> Self {
        Self {
            secret_ref: None,
            allowed_repos: Vec::new(),
            rate_limit_per_min: 60,
            write_token_ref: None,
            allowed_permissions: vec!["write".to_string(), "admin".to_string()],
            max_files: 10,
            max_lines: 300,
            bot_author: DEFAULT_GIT_AUTHOR_NAME.to_string(),
            bot_email: DEFAULT_GIT_AUTHOR_EMAIL.to_string(),
            skip_marker: "[skip claude]".to_string(),
            provider: WebhookProvider::Forgejo,
        }
    }
}

/// Settings for minting (or updating) a YouTrack issue from a triaged
/// failure. Lives in `config.toml` because the daemon's webhook consumer needs it
/// at startup. Routing sends an issue to the project whose enabled VCS
/// integration claims the failing repo; `repo_project_map` is an optional override
/// consulted first, and `fallback_project` catches any unclaimed repo.
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(default, deny_unknown_fields)]
pub struct MintConfig {
    /// Optional override map of `owner/repo` full name to the YouTrack project
    /// short name the minted issue should land in, e.g.
    /// `fabrikam/forgejo-cli` -> `FJ`. Consulted *before* the VCS-integration
    /// lookup; leave empty to let each project's VCS settings drive
    /// routing. A repo claimed by neither falls back to
    /// [`MintConfig::fallback_project`].
    pub repo_project_map: BTreeMap<String, String>,
    /// Project short name used for any repo no override and no VCS integration
    /// claims.
    pub fallback_project: String,
    /// Loop-safety allow-list of trigger-user logins whose failing runs never
    /// mint a task. The CI-fix bot pushes fixes as itself, so a failing fix-PR
    /// run is triggered by one of these logins; minting from it would recurse.
    pub bot_logins: Vec<String>,
    /// Bucket-2 (human-action) assignee login. `None` leaves the minted issue
    /// unassigned: the project is watched, so a human is still notified.
    pub human_assignee: Option<String>,
    /// `State` value set on a freshly minted issue. Default `To do`.
    pub target_state: String,
    /// The `State` value a YouTrack workflow (see
    /// `docs/yt-workflow-resolve-on-pr-merge.js`) sets on an issue when its own
    /// PR merges, resolving the issue. The runner never writes this value: it
    /// only READS it at match time, at CI-failure dispatch, to tell an issue
    /// already resolved (a regression) from one still open. Default `Done`.
    /// Separate from [`MintConfig::target_state`], which is what a fresh mint
    /// (or a regression reopen) sets.
    pub resolved_state: String,
    /// `AI Agent` custom-field name driven on the minted issue.
    pub ai_agent_field: String,
    /// `AI Agent` value set on a bucket-1 (code-fix) issue so the `youtrack`
    /// action's pickup logic opens the fix PR. Default `Queued`.
    pub queued_value: String,
    /// Auto-fix framework settings. A nested `[mint]` subsection (so
    /// it rides the existing DB-backed mint section rather than
    /// adding a whole new settings section). When enabled, mechanically-fixable
    /// CI failures (cargo fmt, a failing checks workflow) are routed
    /// deterministically to the auto-fix bucket (queued for a corrective PR)
    /// instead of to a human.
    pub auto_fix: AutoFixConfig,
}

impl Default for MintConfig {
    fn default() -> Self {
        Self {
            repo_project_map: BTreeMap::new(),
            fallback_project: "CLAUDE".to_string(),
            bot_logins: vec!["claude-run".to_string()],
            human_assignee: None,
            target_state: "To do".to_string(),
            resolved_state: "Done".to_string(),
            ai_agent_field: "AI Agent".to_string(),
            queued_value: "Queued".to_string(),
            auto_fix: AutoFixConfig::default(),
        }
    }
}

/// Auto-fix framework configuration, nested under [`MintConfig`].
///
/// Detectors in [`crate::autofix`] turn a verification-only CI failure into a
/// deterministic auto-fix verdict (no LLM call). The `cargo-fmt` detector is
/// signature-driven and always on while the framework is `enabled`; the
/// `checks-workflow` detector matches the run's `workflow_id` against
/// [`AutoFixConfig::checks_workflows`].
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(default, deny_unknown_fields)]
pub struct AutoFixConfig {
    /// Master enable toggle. When `false`, no detector runs and every failure
    /// follows the prior transient / code_fix / human_action triage path.
    pub enabled: bool,
    /// Workflow filenames whose failures are verification-only (they check code
    /// rather than produce it) and are therefore mechanically auto-fixable.
    /// Matched case-insensitively against the run's `workflow_id`. Default
    /// `["check.yml"]`.
    pub checks_workflows: Vec<String>,
}

impl Default for AutoFixConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            checks_workflows: vec!["check.yml".to_string()],
        }
    }
}

/// Settings for the `version` self-update command. Lives in
/// `config.toml` so an operator can pin a default channel (and reference a
/// registry token) without passing flags every time. The token itself is never
/// inlined here; `token_ref` names a credential in the `secrets`
/// table, resolved (and decrypted) at run time and used for the registry list /
/// asset requests when private packages require auth. The `train` string is
/// validated into [`crate::update::UpdateTrain`] when the command runs.
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(default, deny_unknown_fields)]
pub struct UpdateConfig {
    /// Default update channel: `release` (newest semver `vX.Y.Z`) or `latest`
    /// (the moving `latest` main build). Overridable per invocation by
    /// `version {check,update} --train <release|latest>`.
    pub train: String,
    /// Name of the secret (in the `secrets` table) holding the registry token.
    /// When unset, the updater stays anonymous-first and falls back to the `fj`
    /// token only on a 401/403 with `--use-fj-token`.
    pub token_ref: Option<String>,
}

impl Default for UpdateConfig {
    fn default() -> Self {
        Self {
            train: "release".to_string(),
            token_ref: None,
        }
    }
}

/// Time-based GC sweeper for preserved / aside workdirs. A
/// DB-backed runtime setting (settings key `config.workdir_gc`, editable on the
/// web Settings page) per the bootstrap-vs-runtime split, so it is
/// NOT a `config.toml` key. The sweeper ([`crate::gc`]) runs on `serve` start
/// and on a periodic tick, deleting workdirs under `$CLAUDE_RUN_DATA_DIR/
/// workdirs/<source>/<key>` (including `.kept-<n>` aside dirs) older than
/// `retention_days`. A workdir that still holds unpushed commits is respected
/// until the longer `unpushed_hard_cap_days`, so genuine un-saved work is not
/// discarded the moment the window passes.
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(default, deny_unknown_fields)]
pub struct WorkdirGcConfig {
    /// Master enable toggle. When `false`, no sweep deletes anything (the walk
    /// short-circuits), so an operator can park the feature without losing the
    /// configured windows.
    pub enabled: bool,
    /// Age, in days, past which a preserved / aside workdir is deleted. Measured
    /// from the directory's last-modified time. Conservative default of 14 days
    /// (revise if operational data shows otherwise).
    pub retention_days: u64,
    /// Longer age, in days, past which even a workdir that still holds
    /// local-only work (unpushed commits, a dirty tree, or a stash) is
    /// deleted. Between `retention_days` and this cap such a
    /// workdir is kept; beyond it the disk cost wins. Assumed 60 days; revise
    /// if validation shows otherwise. Must exceed `retention_days` to have
    /// any effect (a smaller value just deletes such dirs at the normal
    /// window).
    pub unpushed_hard_cap_days: u64,
}

impl Default for WorkdirGcConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            retention_days: 14,
            unpushed_hard_cap_days: 60,
        }
    }
}

/// Retention and size ceilings for everything the runner writes under
/// `$CLAUDE_RUN_STATE_DIR/logs/`. A DB-backed runtime setting (settings key
/// `config.stream_log_gc`, editable on the web Settings page) per the
/// bootstrap-vs-runtime split, so it is NOT a `config.toml` key. The sweeps
/// ([`crate::gc::sweep_stream_logs`] and [`crate::gc::sweep_oversized_logs`])
/// run on `serve` start and on a periodic tick.
///
/// This section originally covered only the per-attempt stream logs under
/// `logs/<action>/streams/`: nothing deleted those before, the directory had
/// only ever grown, and filling the log partition is what truncated the stream
/// logs in a past production incident. It was later extended to the two
/// writers under `logs/` it left unbounded, `daemon.log` and the per-action
/// `output.file`. The settings key keeps its original `stream_log_gc` name (and
/// the `Stream log GC` card title) rather than being renamed to match the wider
/// scope: the key is the primary key of the stored document, so renaming it
/// would silently reset an operator's configured window back to the default.
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(default, deny_unknown_fields)]
pub struct StreamLogGcConfig {
    /// Master enable toggle. When `false`, nothing under `logs/` is swept or
    /// bounded, so an operator can park the feature without losing the
    /// configured windows.
    pub enabled: bool,
    /// Age, in days, past which a stream log is deleted, measured from its
    /// last-modified time. Default 30 days: long enough that a failure reported
    /// weeks after the fact still has its transcript, and at the ~800 MB/month observed on host-a
    /// it bounds the directory near 1 GB instead of the 2.8 GB it reached. A log
    /// whose run is still in flight is kept regardless of age.
    pub retention_days: u64,
    /// Size ceiling, in bytes, for `logs/daemon.log`. An age window
    /// does not apply to it: `serve --detach` opens ONE file in append mode and
    /// keeps every tracing line the daemon ever emits in it, so what bounds it
    /// is size. Past the ceiling the file is rolled to `daemon.log.1` and
    /// truncated in place back to its newest half (in place because the daemon
    /// holds the fd open; a rename alone would leave it writing into the
    /// predecessor). `0` disables the ceiling and leaves the file unbounded.
    pub daemon_log_max_bytes: u64,
    /// Size ceiling, in bytes, for each `logs/<action>/<output.file>`
    /// (the opt-in per-job JSONL sink an action's `output.file` field turns on).
    ///
    /// Decision: this file is bounded by SIZE and never deleted by age, unlike
    /// the stream logs beside it. An operator sets `output.file` deliberately to
    /// produce a data sink, often consumed by something outside the runner, and
    /// age-sweeping it would silently destroy the whole record. A size ceiling
    /// keeps the newest records in place, hands the previous generation to
    /// `<output.file>.1`, and still leaves nothing under the state dir growing
    /// without bound. `0` disables the ceiling for operators who want the sink
    /// kept whole and rotate it themselves.
    pub output_file_max_bytes: u64,
    /// Size ceiling, in bytes, for `logs/usage.jsonl` (the
    /// per-run token and cost record, one line per run).
    ///
    /// Bounded by SIZE like the two files above rather than by age: it is one
    /// append-forever file, not a per-unit-of-work artifact, so there is no
    /// dated entry an age sweep could expire. The ceiling is smaller than
    /// theirs because a record is one short JSON object rather than a log
    /// line, so this still holds far more history than anyone reads back. `0`
    /// disables the ceiling and leaves the file unbounded.
    pub usage_log_max_bytes: u64,
    /// Age, in days, past which a `snippets/<action>/<job_key>/`
    /// directory is deleted, measured from its last-modified time. One per job
    /// per scripted step (`filter` / `pre` / `prompt` / `post`), holding the
    /// rendered snippet plus one `binding-<name>.json` per `$`-binding
    /// (`binding-job.json` carries the whole job payload), so a scripted action
    /// polling every few minutes writes one indefinitely without this window. A
    /// directory whose job is still in flight is kept regardless of age, the
    /// same guard [`crate::gc::sweep_stream_logs`] applies to a live run's
    /// stream log.
    pub snippet_retention_days: u64,
    /// Age, in days, past which a file directly under
    /// `webhook-logs/` (the redacted, concatenated CI job logs
    /// [`crate::forge_log::fetch_run_log`] writes per triaged run) is deleted,
    /// measured from its last-modified time. The `webhook-logs/triage/` working
    /// directory the mint classifier reuses across triages is not a per-run
    /// artifact and is left alone by this sweep (it walks files directly under
    /// `webhook-logs/`, not its subdirectories).
    pub webhook_log_retention_days: u64,
}

impl Default for StreamLogGcConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            retention_days: 30,
            // 50 MiB each: large enough that a rotation is rare on a busy
            // daemon, small enough that both generations of both files stay a
            // rounding error next to the stream logs' ~1 GB window.
            daemon_log_max_bytes: 50 * 1024 * 1024,
            output_file_max_bytes: 50 * 1024 * 1024,
            // 8 MiB. At the ~300 bytes a usage record runs to that
            // is on the order of 25,000 runs of history, far past what is read
            // back, and a fraction of the two 50 MiB ceilings above.
            usage_log_max_bytes: 8 * 1024 * 1024,
            // Same 30-day default as the stream logs: both are per-unit-of-work
            // artifacts with the same "recent failure still has its evidence"
            // shape, and there is no stated reason yet to diverge the windows.
            snippet_retention_days: 30,
            webhook_log_retention_days: 30,
        }
    }
}

/// DB-backed retention for the unbounded history tables (settings
/// key `config.db_retention`, editable on the web Settings page), Phase 6 of the
/// roadmap. NOT a `config.toml` key. The daemon sweep ([`crate::retention`]) runs
/// on `serve` start and on a periodic tick, deleting rows from every table in
/// [`crate::retention::RETENTION_TABLES`] whose `ts` is older than
/// `retention_days`. `job_outcomes` is the driver; `run_events`
/// shares the same config, helper, and loop rather than a divergent
/// implementation.
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(default, deny_unknown_fields)]
pub struct DbRetentionConfig {
    /// Master enable toggle. When `false`, no sweep deletes any row, so an
    /// operator can park the feature without losing the configured window.
    pub enabled: bool,
    /// Age, in days, past which a history row is deleted, measured from its
    /// stored `ts`. Default 180 days. This must stay `>=` the longest reporting
    /// range the web UI exposes (90 days, [`crate::report::Range::Last90Days`]),
    /// so the bounded reports are never silently truncated; the all-time report
    /// (`Range::All`) is inherently bounded by this window once a prune runs.
    pub retention_days: u64,
}

impl Default for DbRetentionConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            retention_days: 180,
        }
    }
}

/// Global display preferences. A single-operator daemon has no
/// `users` table, so this is one global section (settings key
/// `config.preferences`, editable on the web Settings page) rather than
/// per-user rows. It drives how every stored RFC3339 UTC timestamp is rendered
/// in the web UI: convert to `timezone`, then format with `datetime_format`
/// (a chrono strftime string). Defaults reproduce today's UTC ISO-with-seconds
/// surface, so behavior is unchanged until an operator configures it.
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(default, deny_unknown_fields)]
pub struct PreferencesConfig {
    /// BCP-47 locale (e.g. `en-US`). Mapped to a [`chrono::Locale`]
    /// (hyphen normalized to underscore) and passed to `format_localized`, so
    /// name-emitting specifiers (`%A`, `%a`, `%B`, `%b`, `%p`) render in this
    /// locale. Validated on save; an unknown locale is rejected.
    pub locale: String,
    /// IANA timezone name (e.g. `America/New_York`), parsed as a
    /// [`chrono_tz::Tz`]. Displayed timestamps are converted into it. Default
    /// `UTC` keeps today's values.
    pub timezone: String,
    /// chrono strftime pattern applied after the timezone conversion. Default
    /// `%Y-%m-%d %H:%M:%S %Z` renders e.g. `2026-07-01 00:00:00 UTC`. The
    /// Settings page offers presets plus free-form editing; the stored value is
    /// always the resolved strftime string.
    pub datetime_format: String,
}

impl Default for PreferencesConfig {
    fn default() -> Self {
        Self {
            locale: "en-US".to_string(),
            timezone: "UTC".to_string(),
            datetime_format: "%Y-%m-%d %H:%M:%S %Z".to_string(),
        }
    }
}

/// Global defaults for the `claude` CLI invocation. A DB-backed
/// runtime section (settings key `config.claude`, editable on the web Settings
/// page and via `GET`/`PUT /api/settings/claude`) per the
/// bootstrap-vs-runtime split, so it is NOT a `config.toml` key (a brand-new
/// section, so there is no file fallback: the DB value or the default).
#[derive(Debug, Clone, Default, Deserialize, Serialize, PartialEq, Eq)]
#[serde(default, deny_unknown_fields)]
pub struct ClaudeConfig {
    /// Instance-wide default model, passed through to `--model` untouched. An
    /// action's own `claude.model` wins over it; when neither is set no
    /// `--model` flag is passed and the `claude` CLI's own default applies. The
    /// runner never validates the value: the CLI is the source of truth for
    /// which models exist.
    pub model: Option<String>,
    /// Instance-wide default effort level, passed through to
    /// `--effort` untouched. An action's own `claude.effort` wins over it; when
    /// neither is set no `--effort` flag is passed and the `claude` CLI's own
    /// default applies. A value outside
    /// [`crate::claude::EFFORT_LEVELS`] is warned about and then passed through:
    /// the set is advisory, so a level Anthropic adds later works with no runner
    /// release.
    pub effort: Option<String>,
    /// Instance-wide default compaction window, passed through to
    /// `--autocompact` untouched (`auto`, or 100k-1M tokens). An action's own
    /// `claude.autocompact` wins over it; when neither is set the runner passes
    /// [`crate::claude::DEFAULT_AUTOCOMPACT`], because the CLI's own `auto`
    /// scales to the whole context window and so lets a tool-heavy session
    /// resend every completed operation's payloads on every turn.
    pub autocompact: Option<String>,
}

/// Live handle to the global [`ClaudeConfig`], shared between
/// [`crate::settings::LiveSettings`] (which swaps it on save) and
/// [`crate::claude::ClaudeInvoker`] (which reads it per invocation). A
/// `std::sync::RwLock` rather than a tokio one because the read happens inside
/// the synchronous `invoke`, which has no `await`.
pub type ClaudeConfigHandle = std::sync::Arc<std::sync::RwLock<ClaudeConfig>>;

/// Daemon-wide concurrency + continuous-pickup settings. A DB-backed
/// runtime section (settings key `config.concurrency`, editable on the web
/// Settings page and via `GET`/`PUT /api/settings/concurrency`) per the
/// bootstrap-vs-runtime split, so it is NOT a `config.toml` key (a
/// brand-new section, so there is no file fallback: the DB value or the default).
///
/// `max_parallel_global` caps total concurrent jobs across ALL actions; the
/// daemon acquires a global permit before each per-action permit (`max_parallel`)
/// so overlapping runs cannot spawn unbounded concurrent `claude` processes.
/// `regather_interval_secs` is the cadence at which the scheduler starts
/// overlapping pickup runs, decoupled from (and typically faster than) an
/// action's `schedule.spec`, so an issue queued mid-run is picked up within one
/// interval rather than waiting for the in-flight run to drain.
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(default, deny_unknown_fields)]
pub struct ConcurrencyConfig {
    /// Global cap on concurrent jobs across every action. `0` means auto: the
    /// daemon defaults it to the maximum `concurrency.max_parallel` among enabled
    /// actions, so a single action still reaches its full per-action cap out of
    /// the box (matching "global 5 = per-action 5"). Raise it to allow more
    /// cross-action concurrency, lower it to throttle host `claude` load.
    pub max_parallel_global: u32,
    /// Cadence, in seconds, at which the scheduler starts an overlapping pickup
    /// run for each enabled non-manual action. Default 60s. The 5s scheduler tick
    /// is unchanged; this only governs the fire decision. Must be >= 1.
    pub regather_interval_secs: u64,
}

impl Default for ConcurrencyConfig {
    fn default() -> Self {
        Self {
            max_parallel_global: 0,
            regather_interval_secs: 60,
        }
    }
}

/// Names the service-auth secrets are stored under. Fixed rather than
/// operator-chosen: the Settings page owns these three credentials end to end,
/// and a stable name keeps a rotation an overwrite instead of an orphan.
pub const ANTHROPIC_KEY_SECRET: &str = "auth-anthropic-api-key";
/// See [`ANTHROPIC_KEY_SECRET`].
pub const YOUTRACK_TOKEN_SECRET: &str = "auth-youtrack-token";
/// See [`ANTHROPIC_KEY_SECRET`].
pub const FORGEJO_TOKEN_SECRET: &str = "auth-forgejo-token";
/// See [`ANTHROPIC_KEY_SECRET`].
pub const GITHUB_TOKEN_SECRET: &str = "auth-github-token";

/// The `config.auth` settings section: the daemon's own logins to Claude,
/// YouTrack, Forgejo and GitHub, entered on the Settings page.
///
/// Only non-secret fields and secret NAMES live here. The tokens themselves go
/// into the encrypted secrets store under the constants above, and
/// [`crate::service_auth`] materializes them into the credential files the `yt`,
/// `fj` and `gh` CLIs read. An action's own `token_ref` still wins for its runs;
/// this is the default the daemon's own provider calls use, which before had
/// nothing but whatever the host operator had logged in.
#[derive(Debug, Clone, Default, Deserialize, Serialize, PartialEq, Eq)]
#[serde(default, deny_unknown_fields)]
pub struct AuthConfig {
    /// Set when an Anthropic API key is stored; carries the secret name so the
    /// document says where the value lives.
    pub anthropic_key_ref: Option<String>,
    /// YouTrack base URL, e.g. `https://yt.example.com`.
    pub youtrack_base_url: Option<String>,
    /// Secret name holding the YouTrack token.
    pub youtrack_token_ref: Option<String>,
    /// Forgejo host, e.g. `dev.example.com` (no scheme).
    pub forgejo_host: Option<String>,
    /// Secret name holding the Forgejo token.
    pub forgejo_token_ref: Option<String>,
    /// GitHub host, e.g. `github.com` (no scheme).
    pub github_host: Option<String>,
    /// Secret name holding the GitHub token.
    pub github_token_ref: Option<String>,
    /// Git commit author name for commits the agent makes inside a job's clone.
    /// Not a secret (a name is not a credential), so no `_ref` field; unset
    /// falls back to the `"claude-run"` bot-identity default at materialize
    /// time.
    pub git_author_name: Option<String>,
    /// Git commit author email, alongside [`AuthConfig::git_author_name`]. Unset
    /// falls back to the `"claude-run@a8n.run"` bot-identity default.
    pub git_author_email: Option<String>,
}

/// Name the SMTP password secret is stored under. See [`ANTHROPIC_KEY_SECRET`]
/// for why this is fixed rather than operator-chosen.
pub const SMTP_PASSWORD_SECRET: &str = "email-smtp-password";

/// How the SMTP connection is secured. `Starttls` is the default: plaintext
/// connect, then upgrade. `Tls` connects already encrypted (the old "implicit
/// TLS" port 465 style). `None` sends in the clear, for a local relay only.
#[derive(Debug, Clone, Copy, Default, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum SmtpEncryption {
    #[default]
    Starttls,
    Tls,
    None,
}

/// The `config.email` settings section: the SMTP transport invite-by-email
/// sends through, entered on the Settings page's "Email (SMTP)" card.
///
/// Only non-secret fields and the password secret's NAME live here; the
/// password itself goes into the encrypted secrets store under
/// [`SMTP_PASSWORD_SECRET`], resolved fresh on every send by
/// [`crate::mailer::Mailer`]. Email is "configured" only once `smtp_host`,
/// `from`, and `public_url` are all set; see [`EmailConfig::configured`].
#[derive(Debug, Clone, Default, Deserialize, Serialize, PartialEq, Eq)]
#[serde(default, deny_unknown_fields)]
pub struct EmailConfig {
    /// SMTP server host, e.g. `smtp.example.com`.
    pub smtp_host: Option<String>,
    /// SMTP server port. Unset means the encryption's own default: 587 for
    /// STARTTLS, 465 for implicit TLS, 25 for none.
    pub smtp_port: Option<u16>,
    /// How the connection is secured. Defaults to STARTTLS.
    pub smtp_encryption: SmtpEncryption,
    /// SMTP username, when the server requires auth.
    pub smtp_username: Option<String>,
    /// Secret name holding the SMTP password. A username with no password is
    /// refused at save time rather than silently sending anonymously.
    pub smtp_password_ref: Option<String>,
    /// The From mailbox, e.g. `claude-run <claude-run@example.com>`.
    pub from: Option<String>,
    /// The instance's externally reachable base URL, e.g.
    /// `https://claude-run.example.com`, used to build the absolute invite
    /// link. Never built from a request's `Host` header: that would put a
    /// client-controlled host into an outbound email.
    pub public_url: Option<String>,
}

impl EmailConfig {
    /// Whether enough is set to send mail: host, From, and the public URL the
    /// invite link is built from.
    pub fn configured(&self) -> bool {
        self.smtp_host.is_some() && self.from.is_some() && self.public_url.is_some()
    }
}

#[derive(Debug, Clone, Default, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct DaemonConfig {
    /// Optional override for the nu binary. Default: "nu" (looked up on PATH).
    pub nu_binary: Option<String>,
    /// Optional override for the claude binary. Default: "claude".
    pub claude_binary: Option<String>,
    /// Where the spawned `claude` agent reads its user-level config
    /// (`CLAUDE.md`, `.credentials.json`, `settings.json`) from, via the
    /// `CLAUDE_CONFIG_DIR` env var. A path the daemon needs before it can spawn
    /// the agent, so it is a bootstrap `config.toml` key (not a DB-backed runtime
    /// setting). When unset it defaults to `<CLAUDE_RUN_CONFIG_DIR>/claude-agent-home`,
    /// self-contained under claude-run's own config tree. At serve start the
    /// daemon seeds this dir with the slim bot `CLAUDE.md` and links the host
    /// login, so the agent never loads the operator's global `~/.claude/CLAUDE.md`.
    pub claude_config_dir: Option<String>,
    /// Persistent log filter, a tracing `EnvFilter` directive string
    /// (e.g. `info,claude_run=debug,sqlx=warn`). Applied after config load via a
    /// reload layer. Precedence: `CLAUDE_RUN_LOG` > this field > the built-in
    /// default (`info,claude_run=info,sqlx=warn`); set `CLAUDE_RUN_LOG` for
    /// one-shot debugging without editing the file. Directive syntax:
    /// <https://docs.rs/tracing-subscriber/latest/tracing_subscriber/filter/struct.EnvFilter.html#directives>.
    /// Known limitation: the few startup lines emitted before config load (e.g.
    /// the config-source line) are governed by the initial filter, not
    /// this one; use `CLAUDE_RUN_LOG` when those specific lines are needed at debug.
    pub log_filter: Option<String>,
}

/// Precedence decision for the post-config-load log-filter swap,
/// extracted as a pure function so the `CLAUDE_RUN_LOG` > `[daemon].log_filter`
/// > default ordering is unit-testable without a live tracing subscriber.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LogFilterChoice {
    /// `CLAUDE_RUN_LOG` is set; it already seeded the initial filter and wins.
    /// Leave the reload layer untouched (today's behavior, byte-for-byte).
    EnvWins,
    /// No env var and no `[daemon].log_filter`; the built-in default stands.
    KeepDefault,
    /// No env var, `[daemon].log_filter` present; swap this directive in.
    ApplyFile(String),
}

/// Pure precedence logic. `env_set` is whether `CLAUDE_RUN_LOG` is
/// present in the environment; `file_filter` is `[daemon].log_filter`.
pub fn log_filter_choice(env_set: bool, file_filter: Option<&str>) -> LogFilterChoice {
    if env_set {
        LogFilterChoice::EnvWins
    } else {
        match file_filter {
            Some(f) => LogFilterChoice::ApplyFile(f.to_string()),
            None => LogFilterChoice::KeepDefault,
        }
    }
}

/// Where the effective [`OnDiskConfig`] came from, so `serve` can
/// log one startup line answering "was my config actually read?". The silent
/// default-fallback under systemd (a different resolved config dir than the
/// shell, so the file "does not exist" there and defaults win) is exactly the
/// case this disambiguates.
#[derive(Debug, Clone)]
pub enum ConfigSource {
    /// `config.toml` existed at this path and was parsed.
    Loaded(PathBuf),
    /// No file at this path; built-in defaults are in use.
    Defaulted(PathBuf),
}

impl ConfigSource {
    /// Emit the single config-source startup line. `info` when a file backed the
    /// config, `warn` when defaults silently won: a missing file under systemd
    /// while the shell shows one is exactly the silent failure this guards against.
    pub fn log(&self) {
        match self {
            ConfigSource::Loaded(path) => {
                tracing::info!(path = %path.display(), "config loaded");
            }
            ConfigSource::Defaulted(path) => {
                tracing::warn!(
                    path = %path.display(),
                    "config.toml not found; built-in defaults in use (web bind 127.0.0.1:7321)"
                );
            }
        }
    }
}

impl OnDiskConfig {
    /// Load `config.toml`, returning the parsed config alongside a
    /// [`ConfigSource`] recording whether the file existed. A missing file is
    /// not an error (built-in defaults apply); a present-but-invalid file aborts
    /// loudly so it is never the silent case.
    pub fn load(paths: &Paths) -> Result<(Self, ConfigSource)> {
        let f = paths.config_file();
        if !f.exists() {
            return Ok((Self::default(), ConfigSource::Defaulted(f)));
        }
        let raw = std::fs::read_to_string(&f)?;
        let cfg: Self =
            toml::from_str(&raw).map_err(|e| anyhow::anyhow!("parsing {}: {}", f.display(), e))?;
        Ok((cfg, ConfigSource::Loaded(f)))
    }
}

/// The resolved state-store backend, produced once at startup and
/// handed to `db::open`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DatabaseTarget {
    /// The SQLite file at [`Paths::state_db`], or at the path a `sqlite://`
    /// URL named.
    Sqlite(PathBuf),
    /// A PostgreSQL server, carrying the URL verbatim (password included, so
    /// this value is never logged).
    Postgres(String),
}

/// The URL configured for the state store, or `None` for the default SQLite
/// file. Highest precedence first: `CLAUDE_RUN_DATABASE_URL_FILE`, then
/// `CLAUDE_RUN_DATABASE_URL`, then `[database] url` in `config.toml`.
pub fn database_url(cfg: &DatabaseConfig) -> Result<Option<String>> {
    if let Ok(path) = std::env::var("CLAUDE_RUN_DATABASE_URL_FILE") {
        let path = expand_path(&path);
        let raw = std::fs::read_to_string(&path)
            .map_err(|e| anyhow::anyhow!("reading CLAUDE_RUN_DATABASE_URL_FILE ({path}): {e}"))?;
        return Ok(Some(raw.trim().to_string()));
    }
    if let Ok(url) = std::env::var("CLAUDE_RUN_DATABASE_URL") {
        return Ok(Some(url.trim().to_string()));
    }
    Ok(cfg.url.as_ref().map(|u| u.trim().to_string()))
}

/// Resolve [`database_url`] into the backend `db::open` should open. An
/// unrecognised scheme is an error naming it, so a typo stops the daemon
/// instead of quietly opening a second, empty SQLite store.
pub fn resolve_database(paths: &Paths, cfg: &DatabaseConfig) -> Result<DatabaseTarget> {
    let Some(url) = database_url(cfg)? else {
        return Ok(DatabaseTarget::Sqlite(paths.state_db()));
    };
    if url.is_empty() {
        return Ok(DatabaseTarget::Sqlite(paths.state_db()));
    }
    let scheme = url.split(':').next().unwrap_or_default();
    match scheme {
        "sqlite" => {
            let rest = url
                .strip_prefix("sqlite://")
                .or_else(|| url.strip_prefix("sqlite:"))
                .unwrap_or_default();
            let file = rest.split('?').next().unwrap_or_default();
            if file.is_empty() {
                return Err(anyhow::anyhow!(
                    "database URL `{url}` names no SQLite file; leave the setting unset to use the default"
                )
                .into());
            }
            Ok(DatabaseTarget::Sqlite(PathBuf::from(file)))
        }
        "postgres" | "postgresql" => Ok(DatabaseTarget::Postgres(url)),
        other => Err(anyhow::anyhow!(
            "unsupported database URL scheme `{other}`: expected sqlite, postgres or postgresql"
        )
        .into()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn expand_handles_tilde_and_env() {
        std::env::set_var("CLAUDE_RUN_TEST_FOO", "bar");
        let out = expand_path("$CLAUDE_RUN_TEST_FOO/x");
        assert_eq!(out, "bar/x");
        let out = expand_path("${CLAUDE_RUN_TEST_FOO}/x");
        assert_eq!(out, "bar/x");
        let out = expand_path("~/x");
        assert!(out.starts_with('/'));
    }

    /// A `config.auth` document stored before `github_host` /
    /// `github_token_ref` existed must still deserialize, with both new fields
    /// defaulting to `None`.
    #[test]
    fn auth_config_without_github_fields_still_deserializes() {
        let json = serde_json::json!({
            "anthropic_key_ref": "auth-anthropic-api-key",
            "youtrack_base_url": "https://yt.example.com",
            "youtrack_token_ref": "auth-youtrack-token",
            "forgejo_host": "dev.example.com",
            "forgejo_token_ref": "auth-forgejo-token",
        });
        let cfg: AuthConfig = serde_json::from_value(json).unwrap();
        assert_eq!(
            cfg.youtrack_base_url.as_deref(),
            Some("https://yt.example.com")
        );
        assert!(cfg.github_host.is_none());
        assert!(cfg.github_token_ref.is_none());
    }

    #[test]
    fn update_config_defaults_to_release_train_no_token() {
        let cfg = UpdateConfig::default();
        assert_eq!(cfg.train, "release");
        assert!(cfg.token_ref.is_none());
        // An empty config still yields the release default.
        let parsed: OnDiskConfig = toml::from_str("").unwrap();
        assert_eq!(parsed.update, UpdateConfig::default());
    }

    #[test]
    fn update_block_parses_train_and_token_ref() {
        let parsed: OnDiskConfig = toml::from_str(
            r#"
            [update]
            train = "latest"
            token_ref = "registry-token"
            "#,
        )
        .unwrap();
        assert_eq!(parsed.update.train, "latest");
        assert_eq!(parsed.update.token_ref.as_deref(), Some("registry-token"));
    }

    // The new `[daemon].log_filter` parses when present and is `None`
    // when absent (the field must stay optional so existing files keep loading).
    #[test]
    fn daemon_log_filter_parses_and_defaults_to_none() {
        // Absent: the whole `[daemon]` block can be missing.
        let parsed: OnDiskConfig = toml::from_str("").unwrap();
        assert!(parsed.daemon.log_filter.is_none());

        // Present: the directive string round-trips.
        let parsed: OnDiskConfig = toml::from_str(
            r#"
            [daemon]
            log_filter = "info,claude_run=debug,sqlx=warn"
            "#,
        )
        .unwrap();
        assert_eq!(
            parsed.daemon.log_filter.as_deref(),
            Some("info,claude_run=debug,sqlx=warn")
        );
    }

    // The precedence function covers all four input combinations of
    // (env set?) x (file set?). Env always wins; otherwise the file applies when
    // present, else the default is kept.
    #[test]
    fn log_filter_choice_precedence() {
        // Env set wins regardless of the file.
        assert_eq!(log_filter_choice(true, None), LogFilterChoice::EnvWins);
        assert_eq!(
            log_filter_choice(true, Some("info,claude_run=debug")),
            LogFilterChoice::EnvWins
        );
        // No env, file present -> apply the file.
        assert_eq!(
            log_filter_choice(false, Some("info,claude_run=debug")),
            LogFilterChoice::ApplyFile("info,claude_run=debug".to_string())
        );
        // Neither -> keep the built-in default.
        assert_eq!(log_filter_choice(false, None), LogFilterChoice::KeepDefault);
    }

    // An invalid directive must be rejected at parse time so the apply
    // path can keep the active filter instead of aborting the daemon.
    #[test]
    fn invalid_log_filter_directive_fails_to_parse() {
        use tracing_subscriber::EnvFilter;
        // A bare `=` with no level/target is not a valid directive.
        assert!(EnvFilter::try_new("claude_run=not_a_level").is_err());
        // A valid one parses, confirming the negative test is meaningful.
        assert!(EnvFilter::try_new("info,claude_run=debug,sqlx=warn").is_ok());
    }

    // ---- Multi-source webhook config ---------------------------

    #[test]
    fn webhook_sources_map_parses() {
        let parsed: OnDiskConfig = toml::from_str(
            r#"
            [webhook]
            replay_window_secs = 600

            [webhook.sources.contoso]
            secret_ref = "forgejo-hmac-contoso"
            allowed_repos = ["Contoso/*"]

            [webhook.sources.fabrikam]
            secret_ref = "forgejo-hmac-fabrikam"
            allowed_repos = ["fabrikam/*"]
            rate_limit_per_min = 120
            "#,
        )
        .unwrap();
        assert_eq!(parsed.webhook.replay_window_secs, 600);
        assert_eq!(parsed.webhook.sources.len(), 2);
        let ng = &parsed.webhook.sources["contoso"];
        assert_eq!(ng.secret_ref.as_deref(), Some("forgejo-hmac-contoso"));
        assert_eq!(ng.allowed_repos, vec!["Contoso/*".to_string()]);
        // Unset rate_limit_per_min falls back to the per-source default.
        assert_eq!(ng.rate_limit_per_min, 60);
        let pb = &parsed.webhook.sources["fabrikam"];
        assert_eq!(pb.rate_limit_per_min, 120);
    }

    #[test]
    fn empty_sources_map_disables_the_listener() {
        // No `[webhook.sources.*]` table: the map is empty and the daemon treats
        // the listener as disabled, exactly like the old unset `secret_ref`.
        let parsed: OnDiskConfig = toml::from_str("").unwrap();
        assert!(parsed.webhook.sources.is_empty());
        assert_eq!(parsed.webhook, WebhookConfig::default());

        // A bare `[webhook]` block with only the shared window is also empty.
        let parsed: OnDiskConfig = toml::from_str(
            r#"
            [webhook]
            replay_window_secs = 300
            "#,
        )
        .unwrap();
        assert!(parsed.webhook.sources.is_empty());
    }

    #[test]
    fn old_top_level_webhook_keys_fail_to_parse() {
        // This was a BREAKING config change: the old top-level keys
        // are gone, and `deny_unknown_fields` makes a config still carrying them
        // fail to parse, naming the offending key as the migration signal.
        for key in ["secret_ref", "allowed_repos", "rate_limit_per_min"] {
            let toml = match key {
                "allowed_repos" => "[webhook]\nallowed_repos = [\"*\"]\n".to_string(),
                "rate_limit_per_min" => "[webhook]\nrate_limit_per_min = 60\n".to_string(),
                _ => "[webhook]\nsecret_ref = \"x\"\n".to_string(),
            };
            let err = toml::from_str::<OnDiskConfig>(&toml).unwrap_err();
            let msg = err.to_string();
            assert!(msg.contains(key), "error names `{key}`: {msg}");
        }
    }

    #[test]
    fn unknown_source_field_fails_to_parse() {
        // `deny_unknown_fields` on the source table catches a typo'd field.
        let err = toml::from_str::<OnDiskConfig>(
            r#"
            [webhook.sources.contoso]
            secret = "oops-should-be-secret_ref"
            "#,
        )
        .unwrap_err();
        assert!(err.to_string().contains("secret"));
    }

    /// CLAUDE-347: a source whose `provider` names a forge we do not support
    /// is an "unknown source type" from the webhook config's point of view -
    /// closed-enum deserialization catches it at config load, naming the
    /// unrecognized value, rather than letting it default to a provider the
    /// operator did not configure.
    #[test]
    fn unknown_provider_value_fails_to_parse() {
        let err = toml::from_str::<OnDiskConfig>(
            r#"
            [webhook.sources.contoso]
            secret_ref = "x"
            provider = "bitbucket"
            "#,
        )
        .unwrap_err();
        assert!(err.to_string().contains("bitbucket"), "{err}");
    }

    // ---- Comment webhook section is DB-only --------------------

    #[test]
    fn comment_webhook_section_cannot_be_declared_in_config_toml() {
        // The section is deliberately absent from `OnDiskConfig`, so
        // `deny_unknown_fields` rejects it and names the offending key. There is
        // no file-to-DB import path: the Settings page is the only source.
        let err = toml::from_str::<OnDiskConfig>(
            r#"
            [webhook_comment]
            trigger = "/claude"
            "#,
        )
        .unwrap_err();
        assert!(
            err.to_string().contains("webhook_comment"),
            "error names the section: {err}"
        );
    }

    #[test]
    fn comment_webhook_defaults_are_the_documented_ones() {
        let d = CommentWebhookConfig::default();
        assert_eq!(d.trigger, "/claude");
        assert_eq!(d.max_instruction_chars, 4000);
        assert_eq!(d.claude_timeout_seconds, 600);
        assert!(d.sources.is_empty());
        let s = CommentWebhookSource::default();
        assert_eq!(s.allowed_permissions, vec!["write", "admin"]);
        assert_eq!(s.rate_limit_per_min, 60);
        assert!(s.secret_ref.is_none());
        assert!(s.write_token_ref.is_none());
        assert!(s.allowed_repos.is_empty(), "empty allow-list denies all");
    }

    #[test]
    fn comment_webhook_section_round_trips_as_json() {
        // The section is stored as a JSON settings document, so the serde shape
        // (not TOML) is what has to round-trip.
        let raw = r#"{"trigger":"/bot","max_instruction_chars":10,
            "sources":{"ng":{"secret_ref":"s","allowed_repos":["Contoso/*"],
            "write_token_ref":"w","allowed_permissions":["admin"]}}}"#;
        let cfg: CommentWebhookConfig = serde_json::from_str(raw).unwrap();
        assert_eq!(cfg.trigger, "/bot");
        assert_eq!(cfg.max_instruction_chars, 10);
        let src = &cfg.sources["ng"];
        assert_eq!(src.write_token_ref.as_deref(), Some("w"));
        assert_eq!(src.allowed_permissions, vec!["admin"]);
        // Unset per-source fields fall back to the defaults.
        assert_eq!(src.rate_limit_per_min, 60);
        assert_eq!(src.max_files, 10);
        // An unknown field is rejected (deny_unknown_fields), like every section.
        assert!(serde_json::from_str::<CommentWebhookConfig>(r#"{"bogus":1}"#).is_err());
    }

    #[test]
    fn source_name_cleanliness() {
        assert!(source_name_is_clean("contoso"));
        assert!(source_name_is_clean("fabrikam"));
        assert!(source_name_is_clean("globex_tools"));
        assert!(source_name_is_clean("initech-1"));
        // Uppercase, spaces, slashes, and emptiness all need escaping / are typos.
        assert!(!source_name_is_clean("Contoso"));
        assert!(!source_name_is_clean("a b"));
        assert!(!source_name_is_clean("a/b"));
        assert!(!source_name_is_clean(""));
    }

    // One test rather than four, because the three inputs are
    // process-wide environment variables and `cargo test` runs in parallel.
    #[test]
    fn database_url_precedence_is_file_then_env_then_config() {
        let tmp = tempfile::tempdir().unwrap();
        let paths = Paths {
            config_dir: tmp.path().to_path_buf(),
            data_dir: tmp.path().to_path_buf(),
            state_dir: tmp.path().to_path_buf(),
            runtime_dir: tmp.path().to_path_buf(),
        };
        std::env::remove_var("CLAUDE_RUN_DATABASE_URL");
        std::env::remove_var("CLAUDE_RUN_DATABASE_URL_FILE");

        // Nothing set anywhere: the SQLite file every install had before.
        let empty = DatabaseConfig::default();
        assert_eq!(database_url(&empty).unwrap(), None);
        assert_eq!(
            resolve_database(&paths, &empty).unwrap(),
            DatabaseTarget::Sqlite(paths.state_db())
        );

        // The config key alone.
        let from_file = DatabaseConfig {
            url: Some("postgres://cfg/db".to_string()),
        };
        assert_eq!(
            database_url(&from_file).unwrap().as_deref(),
            Some("postgres://cfg/db")
        );

        // The env var beats the config key.
        std::env::set_var("CLAUDE_RUN_DATABASE_URL", "postgres://env/db");
        assert_eq!(
            database_url(&from_file).unwrap().as_deref(),
            Some("postgres://env/db")
        );

        // The file beats both, and its trailing newline is trimmed.
        let secret = tmp.path().join("db-url");
        std::fs::write(&secret, "postgres://secret/db\n").unwrap();
        std::env::set_var("CLAUDE_RUN_DATABASE_URL_FILE", &secret);
        assert_eq!(
            database_url(&from_file).unwrap().as_deref(),
            Some("postgres://secret/db")
        );
        assert_eq!(
            resolve_database(&paths, &from_file).unwrap(),
            DatabaseTarget::Postgres("postgres://secret/db".to_string())
        );

        // An unreadable file named by the variable is an error, never a silent
        // fall-through to the next input.
        std::env::set_var("CLAUDE_RUN_DATABASE_URL_FILE", tmp.path().join("absent"));
        let err = database_url(&from_file).unwrap_err().to_string();
        assert!(err.contains("CLAUDE_RUN_DATABASE_URL_FILE"), "{err}");
        std::env::remove_var("CLAUDE_RUN_DATABASE_URL_FILE");

        // An explicit sqlite URL selects the file it names.
        std::env::set_var("CLAUDE_RUN_DATABASE_URL", "sqlite:///tmp/other.db");
        assert_eq!(
            resolve_database(&paths, &empty).unwrap(),
            DatabaseTarget::Sqlite(PathBuf::from("/tmp/other.db"))
        );

        // Any other scheme stops startup and names the scheme.
        std::env::set_var("CLAUDE_RUN_DATABASE_URL", "mysql://host/db");
        let err = resolve_database(&paths, &empty).unwrap_err().to_string();
        assert!(err.contains("mysql"), "{err}");

        std::env::remove_var("CLAUDE_RUN_DATABASE_URL");
    }

    // `load` reports whether `config.toml` was present so `serve` can
    // log the source. A missing file falls back to defaults (the silent
    // case this guards against); a present file is parsed.
    #[test]
    fn load_reports_config_source() {
        let tmp = tempfile::tempdir().unwrap();
        let paths = Paths {
            config_dir: tmp.path().to_path_buf(),
            data_dir: tmp.path().to_path_buf(),
            state_dir: tmp.path().to_path_buf(),
            runtime_dir: tmp.path().to_path_buf(),
        };

        // Missing file -> Defaulted, defaults applied (web bind 127.0.0.1).
        let (cfg, src) = OnDiskConfig::load(&paths).unwrap();
        assert!(matches!(src, ConfigSource::Defaulted(_)));
        assert_eq!(cfg.web.bind, "127.0.0.1");

        // Present file -> Loaded, parsed value wins.
        std::fs::write(paths.config_file(), "[web]\nbind = \"198.51.100.11\"\n").unwrap();
        let (cfg, src) = OnDiskConfig::load(&paths).unwrap();
        assert!(matches!(src, ConfigSource::Loaded(_)));
        assert_eq!(cfg.web.bind, "198.51.100.11");
    }
}
