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
//! `~`, `$VAR`, and `${VAR}` are expanded in all values (the env override and
//! the `workdir_base` field on actions both go through `expand_path`).

use std::collections::BTreeMap;
use std::path::PathBuf;

use serde::{Deserialize, Serialize};

use crate::error::Result;

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
    /// CLAUDE-43: Forgejo webhook listener settings (HMAC secret reference,
    /// repo allow-list, replay window, rate limit).
    pub webhook: WebhookConfig,
    /// CLAUDE-46: triage-driven issue-mint settings (repo-to-project map,
    /// fallback project, loop-safety bot logins, bucket-2 assignee).
    pub mint: MintConfig,
    /// CLAUDE-55: `version` self-update settings (release/latest train default
    /// and the name of the secret holding the registry token).
    pub update: UpdateConfig,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct WebConfig {
    pub bind: String,
    pub port: u16,
    /// Phase 5 (CLAUDE-48): name of the stored secret (CLAUDE-26) holding the
    /// bearer token that gates the web UI and `/api/*` when the bind is
    /// non-loopback. Unset is fine for a loopback bind; required (else the TCP
    /// listener fails closed) for any exposed bind.
    pub auth_token_ref: Option<String>,
    /// Phase 5 (CLAUDE-48): require the bearer token even on a loopback bind.
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

/// CLAUDE-43 / CLAUDE-87: Forgejo webhook listener config. Lives in `config.toml`
/// because the daemon needs it before it binds the TCP listener.
///
/// CLAUDE-87 makes the listener multi-source: each `[webhook.sources.<name>]`
/// table is one webhook endpoint at `POST /webhooks/forgejo/<name>` with its own
/// HMAC secret, allow-list, and rate-limit bucket. The replay window is shared
/// across sources (`X-Forgejo-Delivery` ids are globally unique). An empty
/// `sources` map disables the listener entirely (the old "no `secret_ref`"
/// behaviour). The pre-CLAUDE-87 top-level `secret_ref` / `allowed_repos` /
/// `rate_limit_per_min` keys are gone; `deny_unknown_fields` makes a config still
/// carrying them fail to parse at serve start, naming the offending key, which is
/// the intended migration signal.
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(default, deny_unknown_fields)]
pub struct WebhookConfig {
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
    /// CLAUDE-153: webhook-scoped auto-fix switch. When `false` (the default,
    /// opt-in), a failure the webhook triage routes to a queue-for-fix bucket
    /// (`code_fix` / `auto_fix`) mints nothing and queues no corrective fixup
    /// PR, so a red PR check stays reported on the original PR for its author to
    /// fix in place (the CLAUDE-152 behavior). A `human_action` failure still
    /// mints. This gates only the webhook fixup routing; it is independent of the
    /// framework-wide `[mint].auto_fix.enabled` detector switch and of the
    /// CLAUDE-149 open-PR suppression. Being on `WebhookConfig` it is DB-backed
    /// (`config.webhook`) and edited live on the Settings page / via
    /// `PUT /api/settings/webhook` with no restart.
    pub auto_fix: bool,
}

impl Default for WebhookConfig {
    fn default() -> Self {
        Self {
            replay_window_secs: 300,
            sources: BTreeMap::new(),
            auto_fix: false,
        }
    }
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
    /// CLAUDE-87: warn at load for any source name that is not lowercase
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

/// CLAUDE-87: one webhook source. Each source is an independent endpoint with its
/// own HMAC secret and allow-list, so a leaked org secret can authenticate only
/// that org's allow-listed payloads and one org's failure storm rate-starves only
/// its own bucket. The HMAC secret itself is never inlined here; `secret_ref`
/// names a credential in the CLAUDE-26 `secrets` table, resolved (and decrypted)
/// at request time.
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(default, deny_unknown_fields)]
pub struct WebhookSource {
    /// Name of the secret (in the `secrets` table) holding this source's Forgejo
    /// webhook HMAC secret. When unset, this source's endpoint fails closed and
    /// rejects every request with 401.
    pub secret_ref: Option<String>,
    /// Allow-list of repo patterns for this source. Only `action_run_failure`
    /// events from a matching repo produce a verified-failure event; every other
    /// authenticated request is acked (2xx) and dropped so Forgejo does not
    /// retry. Three pattern forms (CLAUDE-79): exact `owner/repo`, per-owner
    /// `owner/*`, and the global `*`. Matching is case-sensitive; an empty list
    /// denies all (fail closed).
    pub allowed_repos: Vec<String>,
    /// Fixed-window rate limit: max accepted requests per minute for this source.
    /// Excess requests get 429 before any crypto work. Per-source, so one source
    /// exhausting its budget never 429s another.
    pub rate_limit_per_min: u32,
    /// CLAUDE-96: name of the secret (in the `secrets` table) holding a Forgejo
    /// PAT (repository read scope) used to fetch a PRIVATE repo's action logs
    /// through the v16 REST endpoint
    /// (`GET /api/v1/repos/{owner}/{repo}/actions/runs/{run_id}/logs`, merged
    /// upstream in Forgejo PR 12666). Unset keeps the CLAUDE-92 behavior: a
    /// private failure mints a human-action issue with no log attached. On a
    /// pre-v16 instance the endpoint 404s and the pipeline degrades to the same
    /// outcome after exactly one request.
    pub log_token_ref: Option<String>,
}

impl Default for WebhookSource {
    fn default() -> Self {
        Self {
            secret_ref: None,
            allowed_repos: Vec::new(),
            rate_limit_per_min: 60,
            log_token_ref: None,
        }
    }
}

/// CLAUDE-46: settings for minting (or updating) a YouTrack issue from a triaged
/// failure. Lives in `config.toml` because the daemon's webhook consumer needs it
/// at startup. Routing (CLAUDE-86) sends an issue to the project whose enabled VCS
/// integration claims the failing repo; `repo_project_map` is an optional override
/// consulted first, and `fallback_project` (CLAUDE) catches any unclaimed repo.
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(default, deny_unknown_fields)]
pub struct MintConfig {
    /// Optional override map of `owner/repo` full name to the YouTrack project
    /// short name the minted issue should land in, e.g.
    /// `fabrikam/forgejo-cli` -> `FJ`. Consulted *before* the VCS-integration
    /// lookup (CLAUDE-86); leave empty to let each project's VCS settings drive
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
    /// `AI Agent` custom-field name driven on the minted issue.
    pub ai_agent_field: String,
    /// `AI Agent` value set on a bucket-1 (code-fix) issue so the existing
    /// forgejo-ci-fix pickup logic (CLAUDE-29) opens the fix PR. Default `Queued`.
    pub queued_value: String,
    /// CLAUDE-104: auto-fix framework settings. A nested `[mint]` subsection (so
    /// it rides the existing DB-backed mint section per CLAUDE-101 rather than
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
            ai_agent_field: "AI Agent".to_string(),
            queued_value: "Queued".to_string(),
            auto_fix: AutoFixConfig::default(),
        }
    }
}

/// CLAUDE-104: auto-fix framework configuration, nested under [`MintConfig`].
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

/// CLAUDE-55: settings for the `version` self-update command. Lives in
/// `config.toml` so an operator can pin a default channel (and reference a
/// registry token) without passing flags every time. The token itself is never
/// inlined here; `token_ref` names a credential in the CLAUDE-26 `secrets`
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

/// CLAUDE-106: time-based GC sweeper for preserved / aside workdirs. A
/// DB-backed runtime setting (settings key `config.workdir_gc`, editable on the
/// web Settings page) per the CLAUDE-101 bootstrap-vs-runtime split, so it is
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
    /// (revise if operational data shows otherwise, per the issue).
    pub retention_days: u64,
    /// Longer age, in days, past which even a workdir that still holds unpushed
    /// commits is deleted. Between `retention_days` and this cap an unpushed
    /// workdir is kept; beyond it the disk cost wins. Assumed 60 days; revise if
    /// validation shows otherwise. Must exceed `retention_days` to have any
    /// effect (a smaller value just deletes unpushed dirs at the normal window).
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

/// CLAUDE-154: global display preferences. A single-operator daemon has no
/// `users` table, so this is one global section (settings key
/// `config.preferences`, editable on the web Settings page) rather than
/// per-user rows. It drives how every stored RFC3339 UTC timestamp is rendered
/// in the web UI: convert to `timezone`, then format with `datetime_format`
/// (a chrono strftime string). Defaults reproduce today's UTC ISO-with-seconds
/// surface, so behavior is unchanged until an operator configures it.
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(default, deny_unknown_fields)]
pub struct PreferencesConfig {
    /// BCP-47 locale (e.g. `en-US`). ADVISORY / RESERVED for now: it is stored
    /// and round-trips through the API but does NOT localize month / weekday
    /// names in this issue (that needs chrono's `unstable-locales` feature).
    /// Wiring it live is tracked in CLAUDE-155.
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

#[derive(Debug, Clone, Default, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct DaemonConfig {
    /// Optional override for the nu binary. Default: "nu" (looked up on PATH).
    pub nu_binary: Option<String>,
    /// Optional override for the claude binary. Default: "claude".
    pub claude_binary: Option<String>,
    /// CLAUDE-62: persistent log filter, a tracing `EnvFilter` directive string
    /// (e.g. `info,claude_run=debug,sqlx=warn`). Applied after config load via a
    /// reload layer. Precedence: `CLAUDE_RUN_LOG` > this field > the built-in
    /// default (`info,claude_run=info,sqlx=warn`); set `CLAUDE_RUN_LOG` for
    /// one-shot debugging without editing the file. Directive syntax:
    /// <https://docs.rs/tracing-subscriber/latest/tracing_subscriber/filter/struct.EnvFilter.html#directives>.
    /// Known limitation: the few startup lines emitted before config load (e.g.
    /// the CLAUDE-57 config-source line) are governed by the initial filter, not
    /// this one; use `CLAUDE_RUN_LOG` when those specific lines are needed at debug.
    pub log_filter: Option<String>,
}

/// CLAUDE-62: precedence decision for the post-config-load log-filter swap,
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

/// Pure precedence logic for CLAUDE-62. `env_set` is whether `CLAUDE_RUN_LOG` is
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

/// CLAUDE-57: where the effective [`OnDiskConfig`] came from, so `serve` can
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
    /// while the shell shows one is the silent failure CLAUDE-57 is about.
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

    // CLAUDE-62: the new `[daemon].log_filter` parses when present and is `None`
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

    // CLAUDE-62: the precedence function covers all four input combinations of
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

    // CLAUDE-62: an invalid directive must be rejected at parse time so the apply
    // path can keep the active filter instead of aborting the daemon.
    #[test]
    fn invalid_log_filter_directive_fails_to_parse() {
        use tracing_subscriber::EnvFilter;
        // A bare `=` with no level/target is not a valid directive.
        assert!(EnvFilter::try_new("claude_run=not_a_level").is_err());
        // A valid one parses, confirming the negative test is meaningful.
        assert!(EnvFilter::try_new("info,claude_run=debug,sqlx=warn").is_ok());
    }

    // ---- CLAUDE-87: multi-source webhook config ---------------------------

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
        // CLAUDE-87 is a BREAKING config change: the pre-CLAUDE-87 top-level keys
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

    // CLAUDE-57: `load` reports whether `config.toml` was present so `serve` can
    // log the source. A missing file falls back to defaults (the silent case
    // this issue surfaces); a present file is parsed.
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
