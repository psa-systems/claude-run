//! Structured editors for the native action kinds other than `youtrack`
//! (CLAUDE-211): `forgejo-ci-fix`, `ci-failure-analysis`, `ci-failure-reconcile`,
//! `closed-issue-summary`.
//!
//! Same shape as the YouTrack editor in the parent module: a route pair
//! (`/actions/new/<kind>` to create, `/actions/:name/form` to edit), one
//! template per kind, and a `<kind>_form_to_yaml` builder that emits `kind:`
//! plus the `native:` block. The shared widgets (secret dropdown, schedule /
//! limits / output / claude blocks) live in `templates/form_macros.html` and are
//! imported by every editor template, including the YouTrack one.
//!
//! Every save runs the generated YAML through `crate::action::validate_str` and
//! the same write + re-index path the raw editor uses, so a form can never write
//! a file the raw editor would reject.
//!
//! Bodies are read with `RawForm` and decoded into ordered key/value pairs
//! instead of `Form<T>`: the `log_token_ref` per-repo rows post repeated keys,
//! which a struct extractor collapses.

use askama::Template;
use axum::extract::{RawForm, State};
use axum::http::StatusCode;
use axum::response::{Html, IntoResponse, Redirect, Response};

use super::{
    indexed_action_path, prune_yaml, safe_action_stem, set_or_remove, set_scalar_or_remove,
    WebError, WebState,
};
use crate::action::{
    Action, CiFailureAnalysisConfig, CiFailureReconcileConfig, ClaudeBlock,
    ClosedIssueSummaryConfig, ForgejoCiFixConfig, Limits, LogTokenEntry, LogTokenRef, NativeConfig,
    OutputBlock,
};
use crate::state;

/// Spare blank `log_token_ref` rows rendered below the saved ones, so rows can
/// be added without client-side scripting. Blank rows are dropped on save; save
/// and reopen the form to add more.
const SPARE_LOG_TOKEN_ROWS: usize = 3;

// ---- form body decoding ---------------------------------------------------

/// A decoded `application/x-www-form-urlencoded` body as ordered pairs, keeping
/// repeated keys (the per-repo `log_token_ref` rows post one pair per row).
struct FormPairs(Vec<(String, String)>);

impl FormPairs {
    fn parse(body: &[u8]) -> Result<Self, String> {
        serde_urlencoded::from_bytes::<Vec<(String, String)>>(body)
            .map(FormPairs)
            .map_err(|e| format!("malformed form submission: {e}"))
    }

    /// The first value posted for `key`, or `""` when the field is absent.
    fn get(&self, key: &str) -> String {
        self.0
            .iter()
            .find(|(k, _)| k == key)
            .map(|(_, v)| v.clone())
            .unwrap_or_default()
    }

    /// Every value posted for `key`, in document order.
    fn all(&self, key: &str) -> Vec<String> {
        self.0
            .iter()
            .filter(|(k, _)| k == key)
            .map(|(_, v)| v.clone())
            .collect()
    }

    /// Whether a checkbox was ticked (the browser omits an unchecked box).
    fn checked(&self, key: &str) -> bool {
        self.0.iter().any(|(k, _)| k == key)
    }
}

// ---- shared field helpers -------------------------------------------------

/// Trimmed value, or `None` when the box was left empty.
fn opt(raw: &str) -> Option<String> {
    let t = raw.trim();
    (!t.is_empty()).then(|| t.to_string())
}

/// Trimmed value, falling back to `default` when the box was left empty, so a
/// cleared required field never becomes an empty string.
fn or_default(raw: &str, default: String) -> String {
    opt(raw).unwrap_or(default)
}

/// One entry per non-blank line, for the list fields (repos, prefixes).
fn lines(raw: &str) -> Vec<String> {
    raw.lines()
        .map(str::trim)
        .filter(|l| !l.is_empty())
        .map(str::to_string)
        .collect()
}

/// Render a list field back into its one-per-line textarea value.
fn join_lines(items: &[String]) -> String {
    items.join("\n")
}

fn parse_u64(label: &str, raw: &str, default: u64) -> Result<u64, String> {
    match raw.trim() {
        "" => Ok(default),
        t => t
            .parse::<u64>()
            .map_err(|_| format!("{label} must be a whole number (got `{t}`)")),
    }
}

fn parse_usize(label: &str, raw: &str, default: usize) -> Result<usize, String> {
    match raw.trim() {
        "" => Ok(default),
        t => t
            .parse::<usize>()
            .map_err(|_| format!("{label} must be a whole number (got `{t}`)")),
    }
}

fn parse_opt_u32(label: &str, raw: &str) -> Result<Option<u32>, String> {
    match raw.trim() {
        "" => Ok(None),
        t => t
            .parse::<u32>()
            .map(Some)
            .map_err(|_| format!("{label} must be a whole number (got `{t}`)")),
    }
}

/// DEV-705: the dollar / fraction analogue of [`parse_opt_u32`].
fn parse_opt_f64(label: &str, raw: &str) -> Result<Option<f64>, String> {
    match raw.trim() {
        "" => Ok(None),
        t => t
            .parse::<f64>()
            .map(Some)
            .map_err(|_| format!("{label} must be a number (got `{t}`)")),
    }
}

// ---- blocks shared by every kind ------------------------------------------

/// The generic `Action` blocks every structured editor surfaces: identity,
/// `workdir_base`, `link_template`, `schedule`, `claude`, `output`, and
/// `limits`. Kinds that run
/// no agent session (`ci-failure-reconcile`) render no claude fields and pass
/// `with_claude: false` so a hand-written block is left untouched.
#[derive(Debug, Clone, Default)]
pub struct CommonForm {
    pub name: String,
    pub enabled: bool,
    pub workdir_base: String,
    pub link_template: String,
    pub schedule_kind: String,
    pub schedule_spec: String,
    pub output_file: String,
    pub claude_cwd: String,
    pub claude_allowed_tools: String,
    pub claude_permission_mode: String,
    /// CLAUDE-239: per-action `--model`; blank inherits the global
    /// `config.claude.model`.
    pub claude_model: String,
    /// CLAUDE-259: per-action `--effort`; blank inherits the global
    /// `config.claude.effort`.
    pub claude_effort: String,
    /// DEV-706: per-action `--autocompact`; blank inherits the global
    /// `config.claude.autocompact`, then the runner's bounded default.
    pub claude_autocompact: String,
    pub limit_max_attempts_per_key: String,
    pub limit_max_runs_per_day: String,
    pub limit_claude_timeout_seconds: String,
    pub limit_daily_budget_usd: String,
    /// DEV-705: dollars ONE run may spend; blank inherits the daily budget.
    pub limit_run_budget_usd: String,
    /// DEV-705: fraction of the per-run budget at which the run warns once.
    pub limit_budget_warn_fraction: String,
    /// CLAUDE-234: issues one run may create in the tracker.
    pub limit_max_issues_per_run: String,
}

impl CommonForm {
    fn from_pairs(p: &FormPairs) -> Self {
        Self {
            name: p.get("name"),
            enabled: p.checked("enabled"),
            workdir_base: p.get("workdir_base"),
            link_template: p.get("link_template"),
            schedule_kind: p.get("schedule_kind"),
            schedule_spec: p.get("schedule_spec"),
            output_file: p.get("output_file"),
            claude_cwd: p.get("claude_cwd"),
            claude_allowed_tools: p.get("claude_allowed_tools"),
            claude_permission_mode: p.get("claude_permission_mode"),
            claude_model: p.get("claude_model"),
            claude_effort: p.get("claude_effort"),
            claude_autocompact: p.get("claude_autocompact"),
            limit_max_attempts_per_key: p.get("limit_max_attempts_per_key"),
            limit_max_runs_per_day: p.get("limit_max_runs_per_day"),
            limit_claude_timeout_seconds: p.get("limit_claude_timeout_seconds"),
            limit_daily_budget_usd: p.get("limit_daily_budget_usd"),
            limit_run_budget_usd: p.get("limit_run_budget_usd"),
            limit_budget_warn_fraction: p.get("limit_budget_warn_fraction"),
            limit_max_issues_per_run: p.get("limit_max_issues_per_run"),
        }
    }

    fn from_action(a: &Action) -> Self {
        let s = |o: &Option<String>| o.clone().unwrap_or_default();
        let n = |o: Option<u32>| o.map(|v| v.to_string()).unwrap_or_default();
        let f = |o: Option<f64>| o.map(|v| v.to_string()).unwrap_or_default();
        Self {
            name: a.name.clone(),
            enabled: a.enabled,
            workdir_base: s(&a.workdir_base),
            link_template: s(&a.link_template),
            schedule_kind: a
                .schedule
                .as_ref()
                .map(|sc| {
                    match sc.kind {
                        crate::action::ScheduleKind::Interval => "interval",
                        crate::action::ScheduleKind::Cron => "cron",
                        crate::action::ScheduleKind::Manual => "manual",
                    }
                    .to_string()
                })
                .unwrap_or_default(),
            schedule_spec: a
                .schedule
                .as_ref()
                .and_then(|sc| sc.spec.clone())
                .unwrap_or_default(),
            output_file: s(&a.output.file),
            claude_cwd: s(&a.claude.cwd),
            claude_allowed_tools: s(&a.claude.allowed_tools),
            claude_permission_mode: s(&a.claude.permission_mode),
            claude_model: s(&a.claude.model),
            claude_effort: s(&a.claude.effort),
            claude_autocompact: s(&a.claude.autocompact),
            limit_max_attempts_per_key: n(a.limits.max_attempts_per_key),
            limit_max_runs_per_day: n(a.limits.max_runs_per_day),
            limit_claude_timeout_seconds: n(a.limits.claude_timeout_seconds),
            limit_daily_budget_usd: a
                .limits
                .daily_budget_usd
                .map(|v| v.to_string())
                .unwrap_or_default(),
            limit_run_budget_usd: f(a.limits.run_budget_usd),
            limit_budget_warn_fraction: f(a.limits.budget_warn_fraction),
            limit_max_issues_per_run: n(a.limits.max_issues_per_run),
        }
    }
}

/// Overlay the generic blocks onto the action mapping. Keys the form owns are
/// written (or removed when cleared); every other key in the file is untouched.
fn apply_common(
    map: &mut serde_yaml::Mapping,
    c: &CommonForm,
    with_claude: bool,
) -> Result<(), String> {
    use serde_yaml::Value;

    map.insert(Value::from("name"), Value::from(c.name.trim()));
    set_scalar_or_remove(map, "workdir_base", opt(&c.workdir_base));
    set_scalar_or_remove(map, "link_template", opt(&c.link_template));
    map.insert(Value::from("enabled"), Value::from(c.enabled));

    match c.schedule_kind.trim() {
        "" | "none" => {
            map.remove("schedule");
        }
        kind @ ("interval" | "cron" | "manual") => {
            let mut sm = serde_yaml::Mapping::new();
            sm.insert(Value::from("kind"), Value::from(kind));
            if let Some(spec) = opt(&c.schedule_spec) {
                sm.insert(Value::from("spec"), Value::from(spec));
            }
            map.insert(Value::from("schedule"), Value::Mapping(sm));
        }
        other => return Err(format!("unknown schedule kind `{other}`")),
    }

    let output = OutputBlock {
        file: opt(&c.output_file),
    };
    set_or_remove(
        map,
        "output",
        prune_yaml(serde_yaml::to_value(&output).map_err(|e| e.to_string())?),
    );

    if with_claude {
        let claude = ClaudeBlock {
            cwd: opt(&c.claude_cwd),
            allowed_tools: opt(&c.claude_allowed_tools),
            permission_mode: opt(&c.claude_permission_mode),
            model: opt(&c.claude_model),
            effort: opt(&c.claude_effort),
            autocompact: opt(&c.claude_autocompact),
        };
        set_or_remove(
            map,
            "claude",
            prune_yaml(serde_yaml::to_value(&claude).map_err(|e| e.to_string())?),
        );
    }

    let limits = Limits {
        max_attempts_per_key: parse_opt_u32("Max attempts per key", &c.limit_max_attempts_per_key)?,
        max_runs_per_day: parse_opt_u32("Max runs per day", &c.limit_max_runs_per_day)?,
        claude_timeout_seconds: parse_opt_u32(
            "Claude timeout (seconds)",
            &c.limit_claude_timeout_seconds,
        )?,
        daily_budget_usd: match c.limit_daily_budget_usd.trim() {
            "" => None,
            t => Some(
                t.parse::<f64>()
                    .map_err(|_| format!("Daily budget (USD) must be a number (got `{t}`)"))?,
            ),
        },
        run_budget_usd: parse_opt_f64("Per-run budget (USD)", &c.limit_run_budget_usd)?,
        budget_warn_fraction: parse_opt_f64(
            "Budget warning fraction",
            &c.limit_budget_warn_fraction,
        )?,
        max_issues_per_run: parse_opt_u32("Max issues per run", &c.limit_max_issues_per_run)?,
    };
    set_or_remove(
        map,
        "limits",
        prune_yaml(serde_yaml::to_value(&limits).map_err(|e| e.to_string())?),
    );
    Ok(())
}

// ---- log_token_ref (CLAUDE-200: scalar or per-repo rows) -------------------

/// One `{repos, token_ref}` row of the per-repo mapping. `repos` is the
/// textarea's raw one-pattern-per-line value.
#[derive(Debug, Clone, Default)]
pub struct LogTokenRow {
    pub repos: String,
    pub token_ref: String,
}

/// `log_token_ref` in every accepted shape: unset (derived from
/// `config.webhook.sources`), one secret for every repo, or ordered per-repo
/// rows.
#[derive(Debug, Clone, Default)]
pub struct LogTokenForm {
    /// `"derive"`, `"global"`, or `"per_repo"`.
    pub mode: String,
    pub global: String,
    pub rows: Vec<LogTokenRow>,
}

impl LogTokenForm {
    fn from_pairs(p: &FormPairs) -> Self {
        let repos = p.all("log_token_repos");
        let refs = p.all("log_token_ref");
        Self {
            mode: p.get("log_token_mode"),
            global: p.get("log_token_global"),
            rows: repos
                .into_iter()
                .zip(refs)
                .map(|(repos, token_ref)| LogTokenRow { repos, token_ref })
                .collect(),
        }
    }

    fn from_config(cfg: Option<&LogTokenRef>) -> Self {
        let (mode, global, mut rows) = match cfg {
            None => ("derive", String::new(), Vec::new()),
            Some(LogTokenRef::Global(name)) => ("global", name.clone(), Vec::new()),
            Some(LogTokenRef::PerRepo(entries)) => (
                "per_repo",
                String::new(),
                entries
                    .iter()
                    .map(|e| LogTokenRow {
                        repos: join_lines(&e.repos),
                        token_ref: e.token_ref.clone(),
                    })
                    .collect(),
            ),
        };
        rows.extend(std::iter::repeat_with(LogTokenRow::default).take(SPARE_LOG_TOKEN_ROWS));
        Self {
            mode: mode.to_string(),
            global,
            rows,
        }
    }

    /// The typed config, or `None` for the derive-from-webhook-sources default.
    fn to_config(&self) -> Result<Option<LogTokenRef>, String> {
        match self.mode.trim() {
            "global" => {
                let name = opt(&self.global).ok_or_else(|| {
                    "Forgejo log token: pick a secret, or select \"Derive from \
                     config.webhook.sources\""
                        .to_string()
                })?;
                Ok(Some(LogTokenRef::Global(name)))
            }
            "per_repo" => {
                let mut entries = Vec::new();
                for row in &self.rows {
                    let repos = lines(&row.repos);
                    let token_ref = opt(&row.token_ref);
                    match (repos.is_empty(), token_ref) {
                        (true, None) => continue,
                        (false, Some(token_ref)) => {
                            entries.push(LogTokenEntry { repos, token_ref })
                        }
                        _ => {
                            return Err("Forgejo log token: every per-repo row needs both a repo \
                                        pattern and a secret"
                                .to_string())
                        }
                    }
                }
                if entries.is_empty() {
                    return Err(
                        "Forgejo log token: add at least one per-repo row, or select \
                                \"Derive from config.webhook.sources\""
                            .to_string(),
                    );
                }
                Ok(Some(LogTokenRef::PerRepo(entries)))
            }
            _ => Ok(None),
        }
    }

    /// Secret names this form references, so an orphaned reference still shows
    /// as selected in the dropdowns.
    fn referenced(&self) -> Vec<String> {
        let mut out: Vec<String> = self.rows.iter().map(|r| r.token_ref.clone()).collect();
        out.push(self.global.clone());
        out
    }
}

// ---- per-kind forms -------------------------------------------------------

/// Backing form for `kind: forgejo-ci-fix`.
#[derive(Debug, Clone, Default)]
pub struct ForgejoCiFixForm {
    pub common: CommonForm,
    pub native_enabled: bool,
    pub host: String,
    pub repos: String,
    pub window_minutes: String,
    pub max_files: String,
    pub max_lines: String,
    pub bot_author: String,
    pub bot_email: String,
    pub skip_marker: String,
    pub max_pr_attempts: String,
    pub needs_review_label: String,
    pub needs_review_state: String,
    pub review_assignee: String,
    /// A new inline token, or empty to keep the stored one. Never prefilled.
    pub token: String,
    /// Tick to drop the stored inline token.
    pub token_clear: bool,
    /// Whether the file already carries an inline token (drives the hint only).
    pub token_set: bool,
}

impl ForgejoCiFixForm {
    /// Blank identity with every config field seeded from the type's defaults.
    fn defaults() -> Self {
        let d = ForgejoCiFixConfig::default();
        Self {
            common: CommonForm {
                enabled: true,
                ..CommonForm::default()
            },
            native_enabled: d.enabled,
            host: d.host,
            repos: String::new(),
            window_minutes: d.window_minutes.to_string(),
            max_files: d.max_files.to_string(),
            max_lines: d.max_lines.to_string(),
            bot_author: d.bot_author,
            bot_email: d.bot_email,
            skip_marker: d.skip_marker,
            max_pr_attempts: d.max_pr_attempts.to_string(),
            needs_review_label: d.needs_review_label,
            needs_review_state: d.needs_review_state,
            ..Self::default()
        }
    }

    fn from_pairs(p: &FormPairs) -> Self {
        Self {
            common: CommonForm::from_pairs(p),
            native_enabled: p.checked("native_enabled"),
            host: p.get("host"),
            repos: p.get("repos"),
            window_minutes: p.get("window_minutes"),
            max_files: p.get("max_files"),
            max_lines: p.get("max_lines"),
            bot_author: p.get("bot_author"),
            bot_email: p.get("bot_email"),
            skip_marker: p.get("skip_marker"),
            max_pr_attempts: p.get("max_pr_attempts"),
            needs_review_label: p.get("needs_review_label"),
            needs_review_state: p.get("needs_review_state"),
            review_assignee: p.get("review_assignee"),
            token: p.get("token"),
            token_clear: p.checked("token_clear"),
            token_set: false,
        }
    }
}

/// Prefill from a loaded action, or `None` when it is not a `forgejo-ci-fix`.
pub fn action_to_forgejo_ci_fix_form(a: &Action) -> Option<ForgejoCiFixForm> {
    let NativeConfig::ForgejoCiFix(c) = a.native.as_ref()? else {
        return None;
    };
    Some(ForgejoCiFixForm {
        common: CommonForm::from_action(a),
        native_enabled: c.enabled,
        host: c.host.clone(),
        repos: join_lines(&c.repos),
        window_minutes: c.window_minutes.to_string(),
        max_files: c.max_files.to_string(),
        max_lines: c.max_lines.to_string(),
        bot_author: c.bot_author.clone(),
        bot_email: c.bot_email.clone(),
        skip_marker: c.skip_marker.clone(),
        max_pr_attempts: c.max_pr_attempts.to_string(),
        needs_review_label: c.needs_review_label.clone(),
        needs_review_state: c.needs_review_state.clone(),
        review_assignee: c.review_assignee.clone().unwrap_or_default(),
        token: String::new(),
        token_clear: false,
        token_set: c.token.is_some(),
    })
}

/// Map the form into a full action YAML overlaid onto `base` (the parsed
/// existing file for an edit, an empty mapping for a create).
pub fn forgejo_ci_fix_form_to_yaml(
    mut base: serde_yaml::Value,
    form: &ForgejoCiFixForm,
) -> Result<String, String> {
    use serde_yaml::Value;

    let map = base
        .as_mapping_mut()
        .ok_or_else(|| "the action file is not a YAML mapping".to_string())?;
    let d = ForgejoCiFixConfig::default();
    // The inline token is never rendered, so keep the stored value unless the
    // form supplies a new one or asks for it to be cleared.
    let stored_token = map
        .get("native")
        .and_then(|n| n.get("token"))
        .and_then(|t| t.as_str())
        .map(str::to_string);
    let token = match (form.token_clear, opt(&form.token)) {
        (true, _) => None,
        (false, Some(new)) => Some(new),
        (false, None) => stored_token,
    };
    let native = ForgejoCiFixConfig {
        enabled: form.native_enabled,
        host: or_default(&form.host, d.host),
        repos: lines(&form.repos),
        window_minutes: parse_u64("Window (minutes)", &form.window_minutes, d.window_minutes)?,
        max_files: parse_u64("Max files", &form.max_files, d.max_files)?,
        max_lines: parse_u64("Max lines", &form.max_lines, d.max_lines)?,
        bot_author: or_default(&form.bot_author, d.bot_author),
        bot_email: or_default(&form.bot_email, d.bot_email),
        skip_marker: or_default(&form.skip_marker, d.skip_marker),
        max_pr_attempts: parse_u64("Max PR attempts", &form.max_pr_attempts, d.max_pr_attempts)?,
        needs_review_label: or_default(&form.needs_review_label, d.needs_review_label),
        needs_review_state: or_default(&form.needs_review_state, d.needs_review_state),
        review_assignee: opt(&form.review_assignee),
        token,
    };
    map.insert(Value::from("kind"), Value::from("forgejo-ci-fix"));
    map.insert(Value::from("native"), native_value(&native)?);
    apply_common(map, &form.common, true)?;
    serde_yaml::to_string(&base).map_err(|e| e.to_string())
}

/// Backing form for `kind: ci-failure-analysis`.
#[derive(Debug, Clone, Default)]
pub struct CiFailureAnalysisForm {
    pub common: CommonForm,
    pub native_enabled: bool,
    pub query: String,
    pub summary_prefixes: String,
    pub log_token: LogTokenForm,
    pub max_analysis_bytes: String,
}

impl CiFailureAnalysisForm {
    fn defaults() -> Self {
        let d = CiFailureAnalysisConfig::default();
        Self {
            common: CommonForm {
                enabled: true,
                ..CommonForm::default()
            },
            native_enabled: d.enabled,
            query: d.query,
            summary_prefixes: join_lines(&d.summary_prefixes),
            log_token: LogTokenForm::from_config(None),
            max_analysis_bytes: d.max_analysis_bytes.to_string(),
        }
    }

    fn from_pairs(p: &FormPairs) -> Self {
        Self {
            common: CommonForm::from_pairs(p),
            native_enabled: p.checked("native_enabled"),
            query: p.get("query"),
            summary_prefixes: p.get("summary_prefixes"),
            log_token: LogTokenForm::from_pairs(p),
            max_analysis_bytes: p.get("max_analysis_bytes"),
        }
    }
}

/// Prefill from a loaded action, or `None` when it is not a
/// `ci-failure-analysis`.
pub fn action_to_ci_failure_analysis_form(a: &Action) -> Option<CiFailureAnalysisForm> {
    let NativeConfig::CiFailureAnalysis(c) = a.native.as_ref()? else {
        return None;
    };
    Some(CiFailureAnalysisForm {
        common: CommonForm::from_action(a),
        native_enabled: c.enabled,
        query: c.query.clone(),
        summary_prefixes: join_lines(&c.summary_prefixes),
        log_token: LogTokenForm::from_config(c.log_token_ref.as_ref()),
        max_analysis_bytes: c.max_analysis_bytes.to_string(),
    })
}

pub fn ci_failure_analysis_form_to_yaml(
    mut base: serde_yaml::Value,
    form: &CiFailureAnalysisForm,
) -> Result<String, String> {
    use serde_yaml::Value;

    let map = base
        .as_mapping_mut()
        .ok_or_else(|| "the action file is not a YAML mapping".to_string())?;
    let d = CiFailureAnalysisConfig::default();
    let prefixes = lines(&form.summary_prefixes);
    let native = CiFailureAnalysisConfig {
        enabled: form.native_enabled,
        query: or_default(&form.query, d.query),
        summary_prefixes: if prefixes.is_empty() {
            d.summary_prefixes
        } else {
            prefixes
        },
        log_token_ref: form.log_token.to_config()?,
        max_analysis_bytes: parse_usize(
            "Max analysis bytes",
            &form.max_analysis_bytes,
            d.max_analysis_bytes,
        )?,
    };
    map.insert(Value::from("kind"), Value::from("ci-failure-analysis"));
    map.insert(Value::from("native"), native_value(&native)?);
    apply_common(map, &form.common, true)?;
    serde_yaml::to_string(&base).map_err(|e| e.to_string())
}

/// Backing form for `kind: ci-failure-reconcile`.
#[derive(Debug, Clone, Default)]
pub struct CiFailureReconcileForm {
    pub common: CommonForm,
    pub native_enabled: bool,
    pub query: String,
    pub summary_prefixes: String,
    pub repos: String,
    pub log_token: LogTokenForm,
    pub resolved_state: String,
    pub close_comment: String,
    pub max_branch_resolves: String,
    pub close_duplicates: bool,
    pub duplicate_comment: String,
    pub default_branch: String,
}

impl CiFailureReconcileForm {
    fn defaults() -> Self {
        let d = CiFailureReconcileConfig::default();
        Self {
            common: CommonForm {
                enabled: true,
                ..CommonForm::default()
            },
            native_enabled: d.enabled,
            query: d.query,
            summary_prefixes: join_lines(&d.summary_prefixes),
            repos: String::new(),
            log_token: LogTokenForm::from_config(None),
            resolved_state: d.resolved_state,
            close_comment: d.close_comment,
            max_branch_resolves: d.max_branch_resolves.to_string(),
            close_duplicates: d.close_duplicates,
            duplicate_comment: d.duplicate_comment,
            default_branch: d.default_branch,
        }
    }

    fn from_pairs(p: &FormPairs) -> Self {
        Self {
            common: CommonForm::from_pairs(p),
            native_enabled: p.checked("native_enabled"),
            query: p.get("query"),
            summary_prefixes: p.get("summary_prefixes"),
            repos: p.get("repos"),
            log_token: LogTokenForm::from_pairs(p),
            resolved_state: p.get("resolved_state"),
            close_comment: p.get("close_comment"),
            max_branch_resolves: p.get("max_branch_resolves"),
            close_duplicates: p.checked("close_duplicates"),
            duplicate_comment: p.get("duplicate_comment"),
            default_branch: p.get("default_branch"),
        }
    }
}

/// Prefill from a loaded action, or `None` when it is not a
/// `ci-failure-reconcile`.
pub fn action_to_ci_failure_reconcile_form(a: &Action) -> Option<CiFailureReconcileForm> {
    let NativeConfig::CiFailureReconcile(c) = a.native.as_ref()? else {
        return None;
    };
    Some(CiFailureReconcileForm {
        common: CommonForm::from_action(a),
        native_enabled: c.enabled,
        query: c.query.clone(),
        summary_prefixes: join_lines(&c.summary_prefixes),
        repos: join_lines(&c.repos),
        log_token: LogTokenForm::from_config(c.log_token_ref.as_ref()),
        resolved_state: c.resolved_state.clone(),
        close_comment: c.close_comment.clone(),
        max_branch_resolves: c.max_branch_resolves.to_string(),
        close_duplicates: c.close_duplicates,
        duplicate_comment: c.duplicate_comment.clone(),
        default_branch: c.default_branch.clone(),
    })
}

pub fn ci_failure_reconcile_form_to_yaml(
    mut base: serde_yaml::Value,
    form: &CiFailureReconcileForm,
) -> Result<String, String> {
    use serde_yaml::Value;

    let map = base
        .as_mapping_mut()
        .ok_or_else(|| "the action file is not a YAML mapping".to_string())?;
    let d = CiFailureReconcileConfig::default();
    let prefixes = lines(&form.summary_prefixes);
    let native = CiFailureReconcileConfig {
        enabled: form.native_enabled,
        query: or_default(&form.query, d.query),
        summary_prefixes: if prefixes.is_empty() {
            d.summary_prefixes
        } else {
            prefixes
        },
        repos: lines(&form.repos),
        log_token_ref: form.log_token.to_config()?,
        // Always written: `resolved_state` is what disambiguates this variant
        // from `ci-failure-analysis` under the untagged `native:` key.
        resolved_state: or_default(&form.resolved_state, d.resolved_state),
        close_comment: or_default(&form.close_comment, d.close_comment),
        max_branch_resolves: parse_usize(
            "Max branch resolves",
            &form.max_branch_resolves,
            d.max_branch_resolves,
        )?,
        close_duplicates: form.close_duplicates,
        duplicate_comment: or_default(&form.duplicate_comment, d.duplicate_comment),
        default_branch: or_default(&form.default_branch, d.default_branch),
    };
    map.insert(Value::from("kind"), Value::from("ci-failure-reconcile"));
    map.insert(Value::from("native"), native_value(&native)?);
    // `ci-failure-reconcile` runs no agent session, so the form has no claude
    // fields and a hand-written block is left alone.
    apply_common(map, &form.common, false)?;
    serde_yaml::to_string(&base).map_err(|e| e.to_string())
}

/// Backing form for `kind: closed-issue-summary` (CLAUDE-218).
#[derive(Debug, Clone, Default)]
pub struct ClosedIssueSummaryForm {
    pub common: CommonForm,
    pub native_enabled: bool,
    pub parents: String,
    pub query: String,
    pub custom_field: String,
    pub done_label: String,
    pub failed_label: String,
    pub projects: String,
    pub window_days: String,
    pub report_project: String,
    pub link_type: String,
    pub summary_template: String,
    pub max_listed_per_project: String,
}

impl ClosedIssueSummaryForm {
    fn defaults() -> Self {
        let d = ClosedIssueSummaryConfig::default();
        Self {
            common: CommonForm {
                enabled: true,
                ..CommonForm::default()
            },
            native_enabled: d.enabled,
            parents: String::new(),
            query: d.query,
            custom_field: d.custom_field,
            done_label: d.done_label,
            failed_label: d.failed_label,
            projects: String::new(),
            window_days: d.window_days.to_string(),
            report_project: String::new(),
            link_type: d.link_type,
            summary_template: d.summary_template,
            max_listed_per_project: d.max_listed_per_project.to_string(),
        }
    }

    fn from_pairs(p: &FormPairs) -> Self {
        Self {
            common: CommonForm::from_pairs(p),
            native_enabled: p.checked("native_enabled"),
            parents: p.get("parents"),
            query: p.get("query"),
            custom_field: p.get("custom_field"),
            done_label: p.get("done_label"),
            failed_label: p.get("failed_label"),
            projects: p.get("projects"),
            window_days: p.get("window_days"),
            report_project: p.get("report_project"),
            link_type: p.get("link_type"),
            summary_template: p.get("summary_template"),
            max_listed_per_project: p.get("max_listed_per_project"),
        }
    }
}

/// Prefill from a loaded action, or `None` when it is not a
/// `closed-issue-summary`.
pub fn action_to_closed_issue_summary_form(a: &Action) -> Option<ClosedIssueSummaryForm> {
    let NativeConfig::ClosedIssueSummary(c) = a.native.as_ref()? else {
        return None;
    };
    Some(ClosedIssueSummaryForm {
        common: CommonForm::from_action(a),
        native_enabled: c.enabled,
        parents: join_lines(&c.parents),
        query: c.query.clone(),
        custom_field: c.custom_field.clone(),
        done_label: c.done_label.clone(),
        failed_label: c.failed_label.clone(),
        projects: join_lines(&c.projects),
        window_days: c.window_days.to_string(),
        report_project: c.report_project.clone().unwrap_or_default(),
        link_type: c.link_type.clone(),
        summary_template: c.summary_template.clone(),
        max_listed_per_project: c.max_listed_per_project.to_string(),
    })
}

pub fn closed_issue_summary_form_to_yaml(
    mut base: serde_yaml::Value,
    form: &ClosedIssueSummaryForm,
) -> Result<String, String> {
    use serde_yaml::Value;

    let map = base
        .as_mapping_mut()
        .ok_or_else(|| "the action file is not a YAML mapping".to_string())?;
    let d = ClosedIssueSummaryConfig::default();
    let native = ClosedIssueSummaryConfig {
        enabled: form.native_enabled,
        parents: lines(&form.parents),
        query: form.query.trim().to_string(),
        custom_field: or_default(&form.custom_field, d.custom_field),
        done_label: or_default(&form.done_label, d.done_label),
        failed_label: or_default(&form.failed_label, d.failed_label),
        projects: lines(&form.projects),
        // Always written (a number never prunes away): `window_days` is what
        // disambiguates this variant from the other native configs under the
        // untagged `native:` key.
        window_days: u32::try_from(parse_usize(
            "Window days",
            &form.window_days,
            d.window_days as usize,
        )?)
        .map_err(|_| "Window days is too large".to_string())?,
        report_project: opt(&form.report_project),
        link_type: or_default(&form.link_type, d.link_type),
        summary_template: or_default(&form.summary_template, d.summary_template),
        max_listed_per_project: parse_usize(
            "Max listed per project",
            &form.max_listed_per_project,
            d.max_listed_per_project,
        )?,
    };
    map.insert(Value::from("kind"), Value::from("closed-issue-summary"));
    map.insert(Value::from("native"), native_value(&native)?);
    // `closed-issue-summary` runs no agent session, so the form has no claude
    // fields and a hand-written block is left alone.
    apply_common(map, &form.common, false)?;
    serde_yaml::to_string(&base).map_err(|e| e.to_string())
}

/// Serialize a native config and drop its unset keys.
fn native_value<T: serde::Serialize>(cfg: &T) -> Result<serde_yaml::Value, String> {
    let val = serde_yaml::to_value(cfg).map_err(|e| e.to_string())?;
    Ok(prune_yaml(val).unwrap_or_else(|| serde_yaml::Value::Mapping(serde_yaml::Mapping::new())))
}

// ---- templates ------------------------------------------------------------

#[derive(Template)]
#[template(path = "forgejo_ci_fix_editor.html")]
struct ForgejoCiFixTpl {
    version: &'static str,
    heading: String,
    post_url: String,
    raw_url: String,
    form: ForgejoCiFixForm,
    error: Option<String>,
    name: Option<String>,
}

#[derive(Template)]
#[template(path = "ci_failure_analysis_editor.html")]
struct CiFailureAnalysisTpl {
    version: &'static str,
    heading: String,
    post_url: String,
    raw_url: String,
    form: CiFailureAnalysisForm,
    secrets: Vec<String>,
    error: Option<String>,
    name: Option<String>,
}

#[derive(Template)]
#[template(path = "closed_issue_summary_editor.html")]
struct ClosedIssueSummaryTpl {
    version: &'static str,
    heading: String,
    post_url: String,
    raw_url: String,
    form: ClosedIssueSummaryForm,
    error: Option<String>,
    name: Option<String>,
}

#[derive(Template)]
#[template(path = "ci_failure_reconcile_editor.html")]
struct CiFailureReconcileTpl {
    version: &'static str,
    heading: String,
    post_url: String,
    raw_url: String,
    form: CiFailureReconcileForm,
    secrets: Vec<String>,
    error: Option<String>,
    name: Option<String>,
}

/// Stored secret names plus any name the action already references that no
/// longer exists, so an orphaned reference still shows as selected.
async fn secret_options(s: &WebState, referenced: &[String]) -> Result<Vec<String>, WebError> {
    let mut names: Vec<String> = state::secret_list(&s.api.pool)
        .await?
        .into_iter()
        .map(|m| m.name)
        .collect();
    for r in referenced {
        let r = r.trim();
        if !r.is_empty() && !names.iter().any(|n| n == r) {
            names.push(r.to_string());
        }
    }
    names.sort();
    names.dedup();
    Ok(names)
}

/// Heading / POST target / raw-editor link for a create (`None`) or an edit.
fn urls(name: Option<&String>, kind: &str) -> (String, String, String) {
    match name {
        Some(n) => (
            format!("Edit {n}"),
            format!("/actions/{n}/form"),
            format!("/actions/{n}/edit"),
        ),
        None => (
            format!("New {kind} action"),
            format!("/actions/new/{kind}"),
            "/actions/new/raw".to_string(),
        ),
    }
}

// ---- write path (shared with the raw editor) ------------------------------

/// Outcome of a form submission: either the redirect to the saved action, or an
/// inline message to re-render the form with.
enum Saved {
    Redirect(Response),
    Rejected(String),
}

/// Validate `yaml` and write it over an existing action's file, re-indexing so
/// the redirect target is current. Same checks and write path as
/// `action_edit_save`.
async fn write_existing(s: &WebState, path: &str, yaml: &str) -> Result<Saved, WebError> {
    let loaded = match crate::action::validate_str(yaml, path) {
        Ok(l) => l,
        Err(e) => return Ok(Saved::Rejected(e.to_string())),
    };
    std::fs::write(path, yaml.as_bytes()).map_err(WebError::from)?;
    crate::daemon::scan_actions(&s.api.pool, &s.paths.actions_dir())
        .await
        .map_err(WebError::from)?;
    Ok(Saved::Redirect(
        Redirect::to(&format!("/actions/{}", loaded.action.name)).into_response(),
    ))
}

/// Validate `yaml` and write it as a new `actions/<name>.yml`. Same checks,
/// duplicate rejection, and write path as `action_create`.
async fn write_new(s: &WebState, yaml: &str) -> Result<Saved, WebError> {
    let loaded = match crate::action::validate_str(yaml, "<import>") {
        Ok(l) => l,
        Err(e) => return Ok(Saved::Rejected(e.to_string())),
    };
    let Some(stem) = safe_action_stem(&loaded.action.name) else {
        return Ok(Saved::Rejected(format!(
            "action name `{}` is not a safe filename (allowed: letters, digits, '-', '_', '.')",
            loaded.action.name
        )));
    };
    let actions_dir = s.paths.actions_dir();
    let dest = actions_dir.join(format!("{stem}.yml"));
    if dest.exists() {
        return Ok(Saved::Rejected(format!(
            "`{stem}.yml` already exists; edit it instead of importing a duplicate"
        )));
    }
    std::fs::create_dir_all(&actions_dir).map_err(WebError::from)?;
    std::fs::write(&dest, yaml.as_bytes()).map_err(WebError::from)?;
    crate::daemon::scan_actions(&s.api.pool, &actions_dir)
        .await
        .map_err(WebError::from)?;
    Ok(Saved::Redirect(
        Redirect::to(&format!("/actions/{}", loaded.action.name)).into_response(),
    ))
}

/// The parsed on-disk YAML of an existing action, used as the overlay base so
/// keys no form covers survive the save. An unparsable file starts from empty.
fn base_value(path: &str) -> serde_yaml::Value {
    std::fs::read_to_string(path)
        .ok()
        .and_then(|raw| serde_yaml::from_str(&raw).ok())
        .unwrap_or_else(|| serde_yaml::Value::Mapping(serde_yaml::Mapping::new()))
}

// ---- forgejo-ci-fix handlers ----------------------------------------------

async fn render_forgejo_ci_fix(
    s: &WebState,
    status: StatusCode,
    name: Option<String>,
    form: ForgejoCiFixForm,
    error: Option<String>,
) -> Result<Response, WebError> {
    // No secret dropdown: this kind references no stored secret (its `token` is
    // an inline plaintext placeholder the CLI backend ignores).
    let (heading, post_url, raw_url) = urls(name.as_ref(), "forgejo-ci-fix");
    let tpl = ForgejoCiFixTpl {
        version: s.api.version,
        heading,
        post_url,
        raw_url,
        form,
        error,
        name,
    };
    Ok((status, Html(tpl.render().map_err(WebError::from)?)).into_response())
}

pub(super) async fn forgejo_ci_fix_new_page(
    State(s): State<WebState>,
) -> Result<Response, WebError> {
    render_forgejo_ci_fix(&s, StatusCode::OK, None, ForgejoCiFixForm::defaults(), None).await
}

pub(super) async fn forgejo_ci_fix_create(
    State(s): State<WebState>,
    RawForm(body): RawForm,
) -> Result<Response, WebError> {
    let pairs = match FormPairs::parse(&body) {
        Ok(p) => p,
        Err(msg) => {
            return render_forgejo_ci_fix(
                &s,
                StatusCode::UNPROCESSABLE_ENTITY,
                None,
                ForgejoCiFixForm::defaults(),
                Some(msg),
            )
            .await
        }
    };
    let form = ForgejoCiFixForm::from_pairs(&pairs);
    let empty = serde_yaml::Value::Mapping(serde_yaml::Mapping::new());
    let outcome = match forgejo_ci_fix_form_to_yaml(empty, &form) {
        Ok(yaml) => write_new(&s, &yaml).await?,
        Err(msg) => Saved::Rejected(msg),
    };
    match outcome {
        Saved::Redirect(r) => Ok(r),
        Saved::Rejected(msg) => {
            render_forgejo_ci_fix(&s, StatusCode::UNPROCESSABLE_ENTITY, None, form, Some(msg)).await
        }
    }
}

pub(super) async fn forgejo_ci_fix_edit_page(
    s: &WebState,
    name: String,
    action: &Action,
) -> Result<Response, WebError> {
    let Some(form) = action_to_forgejo_ci_fix_form(action) else {
        return Ok(Redirect::to(&format!("/actions/{name}/edit")).into_response());
    };
    render_forgejo_ci_fix(s, StatusCode::OK, Some(name), form, None).await
}

pub(super) async fn forgejo_ci_fix_edit_save(
    s: &WebState,
    name: String,
    path: &str,
    body: &[u8],
) -> Result<Response, WebError> {
    let pairs = match FormPairs::parse(body) {
        Ok(p) => p,
        Err(msg) => {
            return render_forgejo_ci_fix(
                s,
                StatusCode::UNPROCESSABLE_ENTITY,
                Some(name),
                ForgejoCiFixForm::defaults(),
                Some(msg),
            )
            .await
        }
    };
    let mut form = ForgejoCiFixForm::from_pairs(&pairs);
    let outcome = match forgejo_ci_fix_form_to_yaml(base_value(path), &form) {
        Ok(yaml) => write_existing(s, path, &yaml).await?,
        Err(msg) => Saved::Rejected(msg),
    };
    match outcome {
        Saved::Redirect(r) => Ok(r),
        Saved::Rejected(msg) => {
            // Re-render without echoing a submitted token back to the browser.
            form.token = String::new();
            render_forgejo_ci_fix(
                s,
                StatusCode::UNPROCESSABLE_ENTITY,
                Some(name),
                form,
                Some(msg),
            )
            .await
        }
    }
}

// ---- ci-failure-analysis handlers -----------------------------------------

async fn render_ci_failure_analysis(
    s: &WebState,
    status: StatusCode,
    name: Option<String>,
    form: CiFailureAnalysisForm,
    error: Option<String>,
) -> Result<Response, WebError> {
    let secrets = secret_options(s, &form.log_token.referenced()).await?;
    let (heading, post_url, raw_url) = urls(name.as_ref(), "ci-failure-analysis");
    let tpl = CiFailureAnalysisTpl {
        version: s.api.version,
        heading,
        post_url,
        raw_url,
        form,
        secrets,
        error,
        name,
    };
    Ok((status, Html(tpl.render().map_err(WebError::from)?)).into_response())
}

pub(super) async fn ci_failure_analysis_new_page(
    State(s): State<WebState>,
) -> Result<Response, WebError> {
    render_ci_failure_analysis(
        &s,
        StatusCode::OK,
        None,
        CiFailureAnalysisForm::defaults(),
        None,
    )
    .await
}

pub(super) async fn ci_failure_analysis_create(
    State(s): State<WebState>,
    RawForm(body): RawForm,
) -> Result<Response, WebError> {
    let pairs = match FormPairs::parse(&body) {
        Ok(p) => p,
        Err(msg) => {
            return render_ci_failure_analysis(
                &s,
                StatusCode::UNPROCESSABLE_ENTITY,
                None,
                CiFailureAnalysisForm::defaults(),
                Some(msg),
            )
            .await
        }
    };
    let form = CiFailureAnalysisForm::from_pairs(&pairs);
    let empty = serde_yaml::Value::Mapping(serde_yaml::Mapping::new());
    let outcome = match ci_failure_analysis_form_to_yaml(empty, &form) {
        Ok(yaml) => write_new(&s, &yaml).await?,
        Err(msg) => Saved::Rejected(msg),
    };
    match outcome {
        Saved::Redirect(r) => Ok(r),
        Saved::Rejected(msg) => {
            render_ci_failure_analysis(&s, StatusCode::UNPROCESSABLE_ENTITY, None, form, Some(msg))
                .await
        }
    }
}

pub(super) async fn ci_failure_analysis_edit_page(
    s: &WebState,
    name: String,
    action: &Action,
) -> Result<Response, WebError> {
    let Some(form) = action_to_ci_failure_analysis_form(action) else {
        return Ok(Redirect::to(&format!("/actions/{name}/edit")).into_response());
    };
    render_ci_failure_analysis(s, StatusCode::OK, Some(name), form, None).await
}

pub(super) async fn ci_failure_analysis_edit_save(
    s: &WebState,
    name: String,
    path: &str,
    body: &[u8],
) -> Result<Response, WebError> {
    let pairs = match FormPairs::parse(body) {
        Ok(p) => p,
        Err(msg) => {
            return render_ci_failure_analysis(
                s,
                StatusCode::UNPROCESSABLE_ENTITY,
                Some(name),
                CiFailureAnalysisForm::defaults(),
                Some(msg),
            )
            .await
        }
    };
    let form = CiFailureAnalysisForm::from_pairs(&pairs);
    let outcome = match ci_failure_analysis_form_to_yaml(base_value(path), &form) {
        Ok(yaml) => write_existing(s, path, &yaml).await?,
        Err(msg) => Saved::Rejected(msg),
    };
    match outcome {
        Saved::Redirect(r) => Ok(r),
        Saved::Rejected(msg) => {
            render_ci_failure_analysis(
                s,
                StatusCode::UNPROCESSABLE_ENTITY,
                Some(name),
                form,
                Some(msg),
            )
            .await
        }
    }
}

// ---- ci-failure-reconcile handlers ----------------------------------------

async fn render_ci_failure_reconcile(
    s: &WebState,
    status: StatusCode,
    name: Option<String>,
    form: CiFailureReconcileForm,
    error: Option<String>,
) -> Result<Response, WebError> {
    let secrets = secret_options(s, &form.log_token.referenced()).await?;
    let (heading, post_url, raw_url) = urls(name.as_ref(), "ci-failure-reconcile");
    let tpl = CiFailureReconcileTpl {
        version: s.api.version,
        heading,
        post_url,
        raw_url,
        form,
        secrets,
        error,
        name,
    };
    Ok((status, Html(tpl.render().map_err(WebError::from)?)).into_response())
}

pub(super) async fn ci_failure_reconcile_new_page(
    State(s): State<WebState>,
) -> Result<Response, WebError> {
    render_ci_failure_reconcile(
        &s,
        StatusCode::OK,
        None,
        CiFailureReconcileForm::defaults(),
        None,
    )
    .await
}

pub(super) async fn ci_failure_reconcile_create(
    State(s): State<WebState>,
    RawForm(body): RawForm,
) -> Result<Response, WebError> {
    let pairs = match FormPairs::parse(&body) {
        Ok(p) => p,
        Err(msg) => {
            return render_ci_failure_reconcile(
                &s,
                StatusCode::UNPROCESSABLE_ENTITY,
                None,
                CiFailureReconcileForm::defaults(),
                Some(msg),
            )
            .await
        }
    };
    let form = CiFailureReconcileForm::from_pairs(&pairs);
    let empty = serde_yaml::Value::Mapping(serde_yaml::Mapping::new());
    let outcome = match ci_failure_reconcile_form_to_yaml(empty, &form) {
        Ok(yaml) => write_new(&s, &yaml).await?,
        Err(msg) => Saved::Rejected(msg),
    };
    match outcome {
        Saved::Redirect(r) => Ok(r),
        Saved::Rejected(msg) => {
            render_ci_failure_reconcile(&s, StatusCode::UNPROCESSABLE_ENTITY, None, form, Some(msg))
                .await
        }
    }
}

pub(super) async fn ci_failure_reconcile_edit_page(
    s: &WebState,
    name: String,
    action: &Action,
) -> Result<Response, WebError> {
    let Some(form) = action_to_ci_failure_reconcile_form(action) else {
        return Ok(Redirect::to(&format!("/actions/{name}/edit")).into_response());
    };
    render_ci_failure_reconcile(s, StatusCode::OK, Some(name), form, None).await
}

pub(super) async fn ci_failure_reconcile_edit_save(
    s: &WebState,
    name: String,
    path: &str,
    body: &[u8],
) -> Result<Response, WebError> {
    let pairs = match FormPairs::parse(body) {
        Ok(p) => p,
        Err(msg) => {
            return render_ci_failure_reconcile(
                s,
                StatusCode::UNPROCESSABLE_ENTITY,
                Some(name),
                CiFailureReconcileForm::defaults(),
                Some(msg),
            )
            .await
        }
    };
    let form = CiFailureReconcileForm::from_pairs(&pairs);
    let outcome = match ci_failure_reconcile_form_to_yaml(base_value(path), &form) {
        Ok(yaml) => write_existing(s, path, &yaml).await?,
        Err(msg) => Saved::Rejected(msg),
    };
    match outcome {
        Saved::Redirect(r) => Ok(r),
        Saved::Rejected(msg) => {
            render_ci_failure_reconcile(
                s,
                StatusCode::UNPROCESSABLE_ENTITY,
                Some(name),
                form,
                Some(msg),
            )
            .await
        }
    }
}

// ---- closed-issue-summary handlers ----------------------------------------

async fn render_closed_issue_summary(
    s: &WebState,
    status: StatusCode,
    name: Option<String>,
    form: ClosedIssueSummaryForm,
    error: Option<String>,
) -> Result<Response, WebError> {
    let (heading, post_url, raw_url) = urls(name.as_ref(), "closed-issue-summary");
    let tpl = ClosedIssueSummaryTpl {
        version: s.api.version,
        heading,
        post_url,
        raw_url,
        form,
        error,
        name,
    };
    Ok((status, Html(tpl.render().map_err(WebError::from)?)).into_response())
}

pub(super) async fn closed_issue_summary_new_page(
    State(s): State<WebState>,
) -> Result<Response, WebError> {
    render_closed_issue_summary(
        &s,
        StatusCode::OK,
        None,
        ClosedIssueSummaryForm::defaults(),
        None,
    )
    .await
}

pub(super) async fn closed_issue_summary_create(
    State(s): State<WebState>,
    RawForm(body): RawForm,
) -> Result<Response, WebError> {
    let pairs = match FormPairs::parse(&body) {
        Ok(p) => p,
        Err(msg) => {
            return render_closed_issue_summary(
                &s,
                StatusCode::UNPROCESSABLE_ENTITY,
                None,
                ClosedIssueSummaryForm::defaults(),
                Some(msg),
            )
            .await
        }
    };
    let form = ClosedIssueSummaryForm::from_pairs(&pairs);
    let empty = serde_yaml::Value::Mapping(serde_yaml::Mapping::new());
    let outcome = match closed_issue_summary_form_to_yaml(empty, &form) {
        Ok(yaml) => write_new(&s, &yaml).await?,
        Err(msg) => Saved::Rejected(msg),
    };
    match outcome {
        Saved::Redirect(r) => Ok(r),
        Saved::Rejected(msg) => {
            render_closed_issue_summary(&s, StatusCode::UNPROCESSABLE_ENTITY, None, form, Some(msg))
                .await
        }
    }
}

pub(super) async fn closed_issue_summary_edit_page(
    s: &WebState,
    name: String,
    action: &Action,
) -> Result<Response, WebError> {
    let Some(form) = action_to_closed_issue_summary_form(action) else {
        return Ok(Redirect::to(&format!("/actions/{name}/edit")).into_response());
    };
    render_closed_issue_summary(s, StatusCode::OK, Some(name), form, None).await
}

pub(super) async fn closed_issue_summary_edit_save(
    s: &WebState,
    name: String,
    path: &str,
    body: &[u8],
) -> Result<Response, WebError> {
    let pairs = match FormPairs::parse(body) {
        Ok(p) => p,
        Err(msg) => {
            return render_closed_issue_summary(
                s,
                StatusCode::UNPROCESSABLE_ENTITY,
                Some(name),
                ClosedIssueSummaryForm::defaults(),
                Some(msg),
            )
            .await
        }
    };
    let form = ClosedIssueSummaryForm::from_pairs(&pairs);
    let outcome = match closed_issue_summary_form_to_yaml(base_value(path), &form) {
        Ok(yaml) => write_existing(s, path, &yaml).await?,
        Err(msg) => Saved::Rejected(msg),
    };
    match outcome {
        Saved::Redirect(r) => Ok(r),
        Saved::Rejected(msg) => {
            render_closed_issue_summary(
                s,
                StatusCode::UNPROCESSABLE_ENTITY,
                Some(name),
                form,
                Some(msg),
            )
            .await
        }
    }
}

// ---- edit dispatch --------------------------------------------------------

/// Render the structured editor matching an action's native kind, or redirect to
/// the raw editor when the kind has none (`scripted`).
pub(super) async fn edit_page_for_kind(
    s: &WebState,
    name: String,
    action: &Action,
) -> Result<Response, WebError> {
    match action.native {
        Some(NativeConfig::ForgejoCiFix(_)) => forgejo_ci_fix_edit_page(s, name, action).await,
        Some(NativeConfig::CiFailureAnalysis(_)) => {
            ci_failure_analysis_edit_page(s, name, action).await
        }
        Some(NativeConfig::CiFailureReconcile(_)) => {
            ci_failure_reconcile_edit_page(s, name, action).await
        }
        Some(NativeConfig::ClosedIssueSummary(_)) => {
            closed_issue_summary_edit_page(s, name, action).await
        }
        _ => Ok(Redirect::to(&format!("/actions/{name}/edit")).into_response()),
    }
}

/// Save a submission from the structured editor matching an action's native
/// kind.
pub(super) async fn edit_save_for_kind(
    s: &WebState,
    name: String,
    path: &str,
    action: &Action,
    body: &[u8],
) -> Result<Response, WebError> {
    match action.native {
        Some(NativeConfig::ForgejoCiFix(_)) => forgejo_ci_fix_edit_save(s, name, path, body).await,
        Some(NativeConfig::CiFailureAnalysis(_)) => {
            ci_failure_analysis_edit_save(s, name, path, body).await
        }
        Some(NativeConfig::CiFailureReconcile(_)) => {
            ci_failure_reconcile_edit_save(s, name, path, body).await
        }
        Some(NativeConfig::ClosedIssueSummary(_)) => {
            closed_issue_summary_edit_save(s, name, path, body).await
        }
        _ => Ok(Redirect::to(&format!("/actions/{name}/edit")).into_response()),
    }
}

/// Resolve an indexed action to its file path and parsed form. A name the
/// daemon has not indexed, or a file that no longer parses, has no structured
/// editor.
pub(super) async fn indexed_action(
    s: &WebState,
    name: &str,
) -> Result<Option<(String, Action)>, WebError> {
    let Some(path) = indexed_action_path(&s.api.pool, name).await? else {
        return Ok(None);
    };
    let Ok(raw) = std::fs::read_to_string(&path) else {
        return Ok(None);
    };
    match crate::action::validate_str(&raw, &path) {
        Ok(loaded) => Ok(Some((path, loaded.action))),
        Err(_) => Ok(None),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::action::ActionKind;

    /// The shipped action files. Compiled in, so deleting or renaming one breaks
    /// the build rather than silently skipping its round-trip test.
    const FORGEJO_CI_FIX_YML: &str = include_str!("../../actions/forgejo-ci-fix.yml");
    const ANALYSIS_YML: &str = include_str!("../../actions/ci-failure-analysis.yml");
    const RECONCILE_YML: &str = include_str!("../../actions/ci-failure-reconcile.yml");
    const SUMMARY_YML: &str = include_str!("../../actions/closed-issue-summary.yml");

    fn parsed(raw: &str) -> serde_yaml::Value {
        serde_yaml::from_str(raw).unwrap()
    }

    fn empty_base() -> serde_yaml::Value {
        serde_yaml::Value::Mapping(serde_yaml::Mapping::new())
    }

    /// Every field of the loaded action survives a form round trip: same kind,
    /// same native config, same generic blocks.
    fn assert_same_action(before: &Action, after: &Action) {
        assert_eq!(after.kind, before.kind);
        assert_eq!(after.native, before.native);
        assert_eq!(
            serde_yaml::to_value(after).unwrap(),
            serde_yaml::to_value(before).unwrap()
        );
    }

    #[test]
    fn forgejo_ci_fix_round_trips_shipped_action() {
        let before = crate::action::validate_str(FORGEJO_CI_FIX_YML, "<test>").unwrap();
        let form = action_to_forgejo_ci_fix_form(&before.action).expect("forgejo-ci-fix form");
        let yaml = forgejo_ci_fix_form_to_yaml(parsed(FORGEJO_CI_FIX_YML), &form).unwrap();
        let after = crate::action::validate_str(&yaml, "<test>").unwrap();
        assert_eq!(after.action.kind, ActionKind::ForgejoCiFix);
        assert_same_action(&before.action, &after.action);
    }

    #[test]
    fn ci_failure_analysis_round_trips_shipped_action() {
        let before = crate::action::validate_str(ANALYSIS_YML, "<test>").unwrap();
        let form = action_to_ci_failure_analysis_form(&before.action).expect("analysis form");
        let yaml = ci_failure_analysis_form_to_yaml(parsed(ANALYSIS_YML), &form).unwrap();
        let after = crate::action::validate_str(&yaml, "<test>").unwrap();
        assert_eq!(after.action.kind, ActionKind::CiFailureAnalysis);
        assert_same_action(&before.action, &after.action);
    }

    #[test]
    fn ci_failure_reconcile_round_trips_shipped_action() {
        let before = crate::action::validate_str(RECONCILE_YML, "<test>").unwrap();
        let form = action_to_ci_failure_reconcile_form(&before.action).expect("reconcile form");
        let yaml = ci_failure_reconcile_form_to_yaml(parsed(RECONCILE_YML), &form).unwrap();
        let after = crate::action::validate_str(&yaml, "<test>").unwrap();
        assert_eq!(after.action.kind, ActionKind::CiFailureReconcile);
        assert_same_action(&before.action, &after.action);
    }

    #[test]
    fn closed_issue_summary_round_trips_shipped_action() {
        let before = crate::action::validate_str(SUMMARY_YML, "<test>").unwrap();
        let form = action_to_closed_issue_summary_form(&before.action).expect("summary form");
        let yaml = closed_issue_summary_form_to_yaml(parsed(SUMMARY_YML), &form).unwrap();
        let after = crate::action::validate_str(&yaml, "<test>").unwrap();
        assert_eq!(after.action.kind, ActionKind::ClosedIssueSummary);
        assert_same_action(&before.action, &after.action);
    }

    /// Every field of each native config struct is exposed by its form: an
    /// action with every field set to a non-default value survives a prefill ->
    /// save round trip unchanged. A field the form forgot would fall back to its
    /// default here and fail the comparison.
    #[test]
    fn every_forgejo_ci_fix_field_survives_a_round_trip() {
        let raw = "\
name: fix
kind: forgejo-ci-fix
enabled: false
native:
  enabled: false
  host: forge.example.com
  repos: [a/b, c/d]
  window_minutes: 7
  max_files: 3
  max_lines: 11
  bot_author: bot
  bot_email: bot@example.com
  skip_marker: \"[skip]\"
  max_pr_attempts: 9
  needs_review_label: review
  needs_review_state: Blocked
  review_assignee: nate
  token: tok
workdir_base: /tmp/wd
claude:
  cwd: /tmp/c
  allowed_tools: Read
  permission_mode: acceptEdits
output:
  file: o.jsonl
schedule:
  kind: cron
  spec: \"0 * * * *\"
limits:
  max_attempts_per_key: 2
  max_runs_per_day: 3
  claude_timeout_seconds: 4
  daily_budget_usd: 1.5
";
        let before = crate::action::validate_str(raw, "<test>").unwrap();
        let form = action_to_forgejo_ci_fix_form(&before.action).unwrap();
        let yaml = forgejo_ci_fix_form_to_yaml(parsed(raw), &form).unwrap();
        let after = crate::action::validate_str(&yaml, "<test>").unwrap();
        assert_same_action(&before.action, &after.action);
    }

    #[test]
    fn every_ci_failure_analysis_field_survives_a_round_trip() {
        let raw = "\
name: analysis
kind: ci-failure-analysis
enabled: false
native:
  enabled: false
  query: \"#Unresolved project: CLAUDE\"
  summary_prefixes: [\"Boom:\"]
  log_token_ref:
    - repos: [\"contoso/*\"]
      token_ref: logs-a
    - repos: [\"*\"]
      token_ref: logs-b
  max_analysis_bytes: 4096
workdir_base: /tmp/wd
claude:
  cwd: /tmp/c
  allowed_tools: Read
  permission_mode: acceptEdits
output:
  file: o.jsonl
schedule:
  kind: interval
  spec: 15m
limits:
  max_attempts_per_key: 2
  max_runs_per_day: 3
  claude_timeout_seconds: 4
  daily_budget_usd: 1.5
";
        let before = crate::action::validate_str(raw, "<test>").unwrap();
        let form = action_to_ci_failure_analysis_form(&before.action).unwrap();
        let yaml = ci_failure_analysis_form_to_yaml(parsed(raw), &form).unwrap();
        let after = crate::action::validate_str(&yaml, "<test>").unwrap();
        assert_same_action(&before.action, &after.action);
    }

    #[test]
    fn every_ci_failure_reconcile_field_survives_a_round_trip() {
        let raw = "\
name: reconcile
kind: ci-failure-reconcile
enabled: false
native:
  enabled: false
  query: \"#Unresolved project: CLAUDE\"
  summary_prefixes: [\"Boom:\"]
  repos: [\"a/b\", \"c/*\"]
  log_token_ref: logs-global
  resolved_state: Fixed
  close_comment: \"green again: {run_url}\"
  max_branch_resolves: 5
  close_duplicates: false
  duplicate_comment: \"dup of {canonical}\"
workdir_base: /tmp/wd
output:
  file: o.jsonl
schedule:
  kind: interval
  spec: 15m
limits:
  max_runs_per_day: 3
";
        let before = crate::action::validate_str(raw, "<test>").unwrap();
        let form = action_to_ci_failure_reconcile_form(&before.action).unwrap();
        let yaml = ci_failure_reconcile_form_to_yaml(parsed(raw), &form).unwrap();
        let after = crate::action::validate_str(&yaml, "<test>").unwrap();
        assert_same_action(&before.action, &after.action);
    }

    #[test]
    fn every_closed_issue_summary_field_survives_a_round_trip() {
        let raw = "\
name: report
kind: closed-issue-summary
enabled: false
native:
  enabled: false
  parents: [CLAUDE-100, DELTA-4]
  query: \"-Resolved AI Agent: Reporting\"
  custom_field: Reporter
  done_label: Filed
  failed_label: Broken
  projects: [CLAUDE, DELTA]
  window_days: 30
  report_project: RPT
  link_type: relates
  summary_template: \"weekly {start}\"
  max_listed_per_project: 7
workdir_base: /tmp/wd
output:
  file: o.jsonl
schedule:
  kind: interval
  spec: 6h
limits:
  max_runs_per_day: 3
";
        let before = crate::action::validate_str(raw, "<test>").unwrap();
        let form = action_to_closed_issue_summary_form(&before.action).unwrap();
        let yaml = closed_issue_summary_form_to_yaml(parsed(raw), &form).unwrap();
        let after = crate::action::validate_str(&yaml, "<test>").unwrap();
        assert_same_action(&before.action, &after.action);
    }

    #[test]
    fn prefill_rejects_a_mismatched_kind() {
        // Each prefill only handles its own kind; the dispatcher sends the rest
        // to the right editor (or the raw one).
        let yt = crate::action::validate_str("name: yt\nkind: youtrack\nnative: {}\n", "<test>")
            .unwrap()
            .action;
        assert!(action_to_forgejo_ci_fix_form(&yt).is_none());
        assert!(action_to_ci_failure_analysis_form(&yt).is_none());
        assert!(action_to_ci_failure_reconcile_form(&yt).is_none());
        assert!(action_to_closed_issue_summary_form(&yt).is_none());
    }

    #[test]
    fn defaults_create_a_valid_action_of_each_kind() {
        // The blank new-action forms only need a name to save.
        let mut f = ForgejoCiFixForm::defaults();
        f.common.name = "fix".to_string();
        let yaml = forgejo_ci_fix_form_to_yaml(empty_base(), &f).unwrap();
        let loaded = crate::action::validate_str(&yaml, "<test>").unwrap();
        assert_eq!(loaded.action.kind, ActionKind::ForgejoCiFix);

        let mut f = CiFailureAnalysisForm::defaults();
        f.common.name = "analysis".to_string();
        let yaml = ci_failure_analysis_form_to_yaml(empty_base(), &f).unwrap();
        let loaded = crate::action::validate_str(&yaml, "<test>").unwrap();
        assert_eq!(loaded.action.kind, ActionKind::CiFailureAnalysis);

        let mut f = CiFailureReconcileForm::defaults();
        f.common.name = "reconcile".to_string();
        let yaml = ci_failure_reconcile_form_to_yaml(empty_base(), &f).unwrap();
        let loaded = crate::action::validate_str(&yaml, "<test>").unwrap();
        // `resolved_state` is what disambiguates this variant from
        // `ci-failure-analysis` under the untagged `native:` key, so an
        // otherwise-empty reconcile config must still resolve to this kind.
        assert_eq!(loaded.action.kind, ActionKind::CiFailureReconcile);

        let mut f = ClosedIssueSummaryForm::defaults();
        f.common.name = "report".to_string();
        let yaml = closed_issue_summary_form_to_yaml(empty_base(), &f).unwrap();
        let loaded = crate::action::validate_str(&yaml, "<test>").unwrap();
        // `window_days` is a number, so it never prunes away: it is what
        // disambiguates this variant under the untagged `native:` key, and an
        // otherwise-empty reporting config must still resolve to this kind.
        assert_eq!(loaded.action.kind, ActionKind::ClosedIssueSummary);
    }

    #[test]
    fn log_token_derive_writes_no_key() {
        let mut f = CiFailureAnalysisForm::defaults();
        f.common.name = "analysis".to_string();
        f.log_token.mode = "derive".to_string();
        let yaml = ci_failure_analysis_form_to_yaml(empty_base(), &f).unwrap();
        assert!(!yaml.contains("log_token_ref"), "{yaml}");
        let loaded = crate::action::validate_str(&yaml, "<test>").unwrap();
        let Some(NativeConfig::CiFailureAnalysis(cfg)) = loaded.action.native else {
            panic!("expected an analysis config");
        };
        assert!(cfg.log_token_ref.is_none());
    }

    #[test]
    fn log_token_global_saves_the_scalar_shape() {
        let mut f = CiFailureAnalysisForm::defaults();
        f.common.name = "analysis".to_string();
        f.log_token.mode = "global".to_string();
        f.log_token.global = "forgejo-logs".to_string();
        let yaml = ci_failure_analysis_form_to_yaml(empty_base(), &f).unwrap();
        let loaded = crate::action::validate_str(&yaml, "<test>").unwrap();
        let Some(NativeConfig::CiFailureAnalysis(cfg)) = loaded.action.native else {
            panic!("expected an analysis config");
        };
        assert_eq!(
            cfg.log_token_ref,
            Some(LogTokenRef::Global("forgejo-logs".to_string()))
        );
    }

    #[test]
    fn log_token_per_repo_saves_rows_and_drops_blank_ones() {
        let mut f = CiFailureReconcileForm::defaults();
        f.common.name = "reconcile".to_string();
        f.log_token.mode = "per_repo".to_string();
        f.log_token.rows = vec![
            LogTokenRow {
                repos: "contoso/*\ninitech/*".to_string(),
                token_ref: "logs-a".to_string(),
            },
            LogTokenRow::default(),
            LogTokenRow {
                repos: "*".to_string(),
                token_ref: "logs-b".to_string(),
            },
        ];
        let yaml = ci_failure_reconcile_form_to_yaml(empty_base(), &f).unwrap();
        let loaded = crate::action::validate_str(&yaml, "<test>").unwrap();
        let Some(NativeConfig::CiFailureReconcile(cfg)) = loaded.action.native else {
            panic!("expected a reconcile config");
        };
        assert_eq!(
            cfg.log_token_ref,
            Some(LogTokenRef::PerRepo(vec![
                LogTokenEntry {
                    repos: vec!["contoso/*".to_string(), "initech/*".to_string()],
                    token_ref: "logs-a".to_string(),
                },
                LogTokenEntry {
                    repos: vec!["*".to_string()],
                    token_ref: "logs-b".to_string(),
                },
            ]))
        );
    }

    #[test]
    fn log_token_per_repo_prefills_and_offers_spare_rows() {
        let cfg = LogTokenRef::PerRepo(vec![LogTokenEntry {
            repos: vec!["contoso/*".to_string()],
            token_ref: "logs".to_string(),
        }]);
        let form = LogTokenForm::from_config(Some(&cfg));
        assert_eq!(form.mode, "per_repo");
        assert_eq!(form.rows.len(), 1 + SPARE_LOG_TOKEN_ROWS);
        assert_eq!(form.rows[0].repos, "contoso/*");
        assert_eq!(form.rows[0].token_ref, "logs");
        // The saved row survives; the spares are dropped.
        assert_eq!(
            form.to_config().unwrap(),
            Some(LogTokenRef::PerRepo(vec![LogTokenEntry {
                repos: vec!["contoso/*".to_string()],
                token_ref: "logs".to_string(),
            }]))
        );
    }

    #[test]
    fn log_token_half_filled_row_is_rejected() {
        let mut f = CiFailureAnalysisForm::defaults();
        f.common.name = "analysis".to_string();
        f.log_token.mode = "per_repo".to_string();
        f.log_token.rows = vec![LogTokenRow {
            repos: "contoso/*".to_string(),
            token_ref: String::new(),
        }];
        let err = ci_failure_analysis_form_to_yaml(empty_base(), &f).unwrap_err();
        assert!(err.contains("per-repo row"), "{err}");
    }

    #[test]
    fn log_token_modes_without_a_value_are_rejected() {
        let mut f = CiFailureAnalysisForm::defaults();
        f.common.name = "analysis".to_string();
        f.log_token.mode = "global".to_string();
        let err = ci_failure_analysis_form_to_yaml(empty_base(), &f).unwrap_err();
        assert!(err.contains("pick a secret"), "{err}");

        f.log_token.mode = "per_repo".to_string();
        f.log_token.rows = vec![LogTokenRow::default()];
        let err = ci_failure_analysis_form_to_yaml(empty_base(), &f).unwrap_err();
        assert!(err.contains("at least one per-repo row"), "{err}");
    }

    #[test]
    fn bad_number_is_reported_inline() {
        let mut f = CiFailureReconcileForm::defaults();
        f.common.name = "reconcile".to_string();
        f.max_branch_resolves = "lots".to_string();
        let err = ci_failure_reconcile_form_to_yaml(empty_base(), &f).unwrap_err();
        assert!(err.contains("Max branch resolves"), "{err}");

        let mut f = ForgejoCiFixForm::defaults();
        f.common.name = "fix".to_string();
        f.common.limit_daily_budget_usd = "cheap".to_string();
        let err = forgejo_ci_fix_form_to_yaml(empty_base(), &f).unwrap_err();
        assert!(err.contains("Daily budget"), "{err}");
    }

    #[test]
    fn cleared_required_fields_fall_back_to_defaults() {
        let d = ForgejoCiFixConfig::default();
        let mut f = ForgejoCiFixForm::defaults();
        f.common.name = "fix".to_string();
        f.host = String::new();
        f.bot_author = String::new();
        f.window_minutes = String::new();
        let yaml = forgejo_ci_fix_form_to_yaml(empty_base(), &f).unwrap();
        let loaded = crate::action::validate_str(&yaml, "<test>").unwrap();
        let Some(NativeConfig::ForgejoCiFix(cfg)) = loaded.action.native else {
            panic!("expected a forgejo-ci-fix config");
        };
        assert_eq!(cfg.host, d.host);
        assert_eq!(cfg.bot_author, d.bot_author);
        assert_eq!(cfg.window_minutes, d.window_minutes);
    }

    #[test]
    fn inline_token_is_kept_unless_replaced_or_cleared() {
        let existing = "name: fix\nkind: forgejo-ci-fix\nnative:\n  host: dev.a8n.run\n  token: secret-value\n";
        let loaded = crate::action::validate_str(existing, "<test>").unwrap();
        let form = action_to_forgejo_ci_fix_form(&loaded.action).unwrap();
        // The stored token is never rendered back into the form.
        assert!(form.token.is_empty());
        assert!(form.token_set);

        let token_of = |yaml: &str| {
            let l = crate::action::validate_str(yaml, "<test>").unwrap();
            let Some(NativeConfig::ForgejoCiFix(cfg)) = l.action.native else {
                panic!("expected a forgejo-ci-fix config");
            };
            cfg.token
        };
        // Blank box: the stored value survives the save.
        let yaml = forgejo_ci_fix_form_to_yaml(parsed(existing), &form).unwrap();
        assert_eq!(token_of(&yaml), Some("secret-value".to_string()));
        // A typed value replaces it.
        let mut replaced = form.clone();
        replaced.token = "new-value".to_string();
        let yaml = forgejo_ci_fix_form_to_yaml(parsed(existing), &replaced).unwrap();
        assert_eq!(token_of(&yaml), Some("new-value".to_string()));
        // The clear box removes the key.
        let mut cleared = form.clone();
        cleared.token_clear = true;
        let yaml = forgejo_ci_fix_form_to_yaml(parsed(existing), &cleared).unwrap();
        assert_eq!(token_of(&yaml), None);
        assert!(!yaml.contains("token"), "{yaml}");
    }

    #[test]
    fn keys_no_form_covers_are_preserved() {
        // `dedup_key` and `concurrency` have no form field; an edit must leave
        // them untouched rather than dropping them.
        let existing = "name: reconcile\nkind: ci-failure-reconcile\nnative:\n  resolved_state: Done\ndedup_key: key\nconcurrency:\n  max_parallel: 4\nclaude:\n  allowed_tools: Read\n";
        let loaded = crate::action::validate_str(existing, "<test>").unwrap();
        let form = action_to_ci_failure_reconcile_form(&loaded.action).unwrap();
        let yaml = ci_failure_reconcile_form_to_yaml(parsed(existing), &form).unwrap();
        let after = crate::action::validate_str(&yaml, "<test>").unwrap();
        assert_eq!(after.action.dedup_key, Some("key".to_string()));
        assert_eq!(after.action.concurrency.max_parallel, 4);
        // This kind runs no agent session, so its form has no claude fields and
        // a hand-written block survives.
        assert_eq!(after.action.claude.allowed_tools, Some("Read".to_string()));
    }

    #[test]
    fn form_pairs_keep_repeated_keys_in_order() {
        let p = FormPairs::parse(
            b"log_token_repos=a&log_token_ref=x&log_token_repos=b&log_token_ref=y&enabled=on",
        )
        .unwrap();
        assert_eq!(p.get("log_token_repos"), "a");
        assert_eq!(p.all("log_token_repos"), vec!["a", "b"]);
        assert_eq!(p.all("log_token_ref"), vec!["x", "y"]);
        assert!(p.checked("enabled"));
        assert!(!p.checked("native_enabled"));
        let form = LogTokenForm::from_pairs(&p);
        assert_eq!(form.rows.len(), 2);
        assert_eq!(form.rows[1].repos, "b");
        assert_eq!(form.rows[1].token_ref, "y");
    }

    #[test]
    fn unchecked_boxes_turn_the_toggles_off() {
        // A browser omits an unchecked box entirely, so an absent key must read
        // as false rather than falling back to the config default (true).
        let p = FormPairs::parse(b"name=reconcile&resolved_state=Done").unwrap();
        let f = CiFailureReconcileForm::from_pairs(&p);
        assert!(!f.common.enabled);
        assert!(!f.native_enabled);
        assert!(!f.close_duplicates);
        let yaml = ci_failure_reconcile_form_to_yaml(empty_base(), &f).unwrap();
        let loaded = crate::action::validate_str(&yaml, "<test>").unwrap();
        assert!(!loaded.action.enabled);
        let Some(NativeConfig::CiFailureReconcile(cfg)) = loaded.action.native else {
            panic!("expected a reconcile config");
        };
        assert!(!cfg.enabled);
        assert!(!cfg.close_duplicates);
    }

    /// CLAUDE-222: every native structured editor renders the shared identity
    /// block, so all four expose the `link_template` field with its saved value.
    #[test]
    fn every_native_editor_renders_the_link_template_field() {
        let common = CommonForm {
            link_template: "https://youtrack.example.com/issue/{key}".to_string(),
            ..CommonForm::default()
        };
        let mut rendered: Vec<String> = Vec::new();
        rendered.push(
            ForgejoCiFixTpl {
                version: "test",
                heading: "h".to_string(),
                post_url: "/p".to_string(),
                raw_url: "/r".to_string(),
                form: ForgejoCiFixForm {
                    common: common.clone(),
                    ..ForgejoCiFixForm::defaults()
                },
                error: None,
                name: None,
            }
            .render()
            .unwrap(),
        );
        rendered.push(
            CiFailureAnalysisTpl {
                version: "test",
                heading: "h".to_string(),
                post_url: "/p".to_string(),
                raw_url: "/r".to_string(),
                form: CiFailureAnalysisForm {
                    common: common.clone(),
                    ..CiFailureAnalysisForm::defaults()
                },
                secrets: Vec::new(),
                error: None,
                name: None,
            }
            .render()
            .unwrap(),
        );
        rendered.push(
            CiFailureReconcileTpl {
                version: "test",
                heading: "h".to_string(),
                post_url: "/p".to_string(),
                raw_url: "/r".to_string(),
                form: CiFailureReconcileForm {
                    common: common.clone(),
                    ..CiFailureReconcileForm::defaults()
                },
                secrets: Vec::new(),
                error: None,
                name: None,
            }
            .render()
            .unwrap(),
        );
        rendered.push(
            ClosedIssueSummaryTpl {
                version: "test",
                heading: "h".to_string(),
                post_url: "/p".to_string(),
                raw_url: "/r".to_string(),
                form: ClosedIssueSummaryForm {
                    common,
                    ..ClosedIssueSummaryForm::defaults()
                },
                error: None,
                name: None,
            }
            .render()
            .unwrap(),
        );
        for html in rendered {
            assert!(html.contains("name=\"link_template\""), "{html}");
            assert!(
                html.contains("https://youtrack.example.com/issue/{key}"),
                "{html}"
            );
        }
    }

    /// CLAUDE-222: `link_template` is a `CommonForm` field, so a native editor
    /// can both set it and clear it (a blank box removes the key).
    #[test]
    fn link_template_round_trips_through_the_common_block() {
        let existing = "name: analysis\nkind: ci-failure-analysis\nnative:\n  query: \"#Unresolved\"\nlink_template: \"https://youtrack.example.com/issue/{key}\"\n";
        let loaded = crate::action::validate_str(existing, "<test>").unwrap();
        let form = action_to_ci_failure_analysis_form(&loaded.action).unwrap();
        assert_eq!(
            form.common.link_template,
            "https://youtrack.example.com/issue/{key}"
        );

        // Set: the prefilled value survives the save.
        let yaml = ci_failure_analysis_form_to_yaml(parsed(existing), &form).unwrap();
        let after = crate::action::validate_str(&yaml, "<test>").unwrap();
        assert_eq!(
            after.action.link_template.as_deref(),
            Some("https://youtrack.example.com/issue/{key}")
        );

        // Cleared: the key is removed rather than written as an empty string.
        let mut cleared = form.clone();
        cleared.common.link_template = String::new();
        let yaml = ci_failure_analysis_form_to_yaml(parsed(existing), &cleared).unwrap();
        assert!(!yaml.contains("link_template"), "{yaml}");
        let after = crate::action::validate_str(&yaml, "<test>").unwrap();
        assert_eq!(after.action.link_template, None);
    }

    /// CLAUDE-259: the editor rebuilds the whole `claude:` block on save, so a
    /// pinned `effort:` survives a prefill -> save round trip (a missing form
    /// field would silently delete a hand-written key), including a level
    /// outside the documented set.
    #[test]
    fn claude_effort_round_trips_through_the_common_block() {
        let existing = "name: analysis\nkind: ci-failure-analysis\nnative:\n  query: \"#Unresolved\"\nclaude:\n  model: opus\n  effort: xhigh\n";
        let loaded = crate::action::validate_str(existing, "<test>").unwrap();
        let form = action_to_ci_failure_analysis_form(&loaded.action).unwrap();
        assert_eq!(form.common.claude_effort, "xhigh");

        let yaml = ci_failure_analysis_form_to_yaml(parsed(existing), &form).unwrap();
        let after = crate::action::validate_str(&yaml, "<test>").unwrap();
        assert_eq!(after.action.claude.effort.as_deref(), Some("xhigh"));
        assert_eq!(after.action.claude.model.as_deref(), Some("opus"));

        // An undocumented level is preserved verbatim, never dropped or rewritten.
        let mut odd = form.clone();
        odd.common.claude_effort = "colossal".to_string();
        let yaml = ci_failure_analysis_form_to_yaml(parsed(existing), &odd).unwrap();
        let after = crate::action::validate_str(&yaml, "<test>").unwrap();
        assert_eq!(after.action.claude.effort.as_deref(), Some("colossal"));

        // Cleared: the key is removed rather than written as an empty string.
        let mut cleared = form.clone();
        cleared.common.claude_effort = String::new();
        let yaml = ci_failure_analysis_form_to_yaml(parsed(existing), &cleared).unwrap();
        assert!(!yaml.contains("effort"), "{yaml}");
        let after = crate::action::validate_str(&yaml, "<test>").unwrap();
        assert_eq!(after.action.claude.effort, None);
    }

    #[test]
    fn schedule_and_output_round_trip_through_the_common_block() {
        let p = FormPairs::parse(
            b"name=analysis&enabled=on&schedule_kind=interval&schedule_spec=30m&output_file=a.jsonl\
              &limit_max_runs_per_day=50&workdir_base=%7E%2Fworkspace",
        )
        .unwrap();
        let f = CiFailureAnalysisForm::from_pairs(&p);
        let yaml = ci_failure_analysis_form_to_yaml(empty_base(), &f).unwrap();
        let a = crate::action::validate_str(&yaml, "<test>").unwrap().action;
        let sc = a.schedule.expect("schedule");
        assert_eq!(sc.kind, crate::action::ScheduleKind::Interval);
        assert_eq!(sc.spec, Some("30m".to_string()));
        assert_eq!(a.output.file, Some("a.jsonl".to_string()));
        assert_eq!(a.limits.max_runs_per_day, Some(50));
        assert_eq!(a.workdir_base, Some("~/workspace".to_string()));
    }
}
