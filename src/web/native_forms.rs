//! Structured editors for the native action kinds other than `youtrack`
//! (CLAUDE-211): `closed-issue-summary`.
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
//! instead of `Form<T>`.

use askama::Template;
use axum::extract::{RawForm, State};
use axum::http::StatusCode;
use axum::response::{Html, IntoResponse, Redirect, Response};

use super::{
    indexed_action_path, prune_yaml, safe_action_stem, set_or_remove, set_scalar_or_remove,
    WebError, WebState,
};
use crate::action::{
    Action, ClaudeBlock, ClosedIssueSummaryConfig, Limits, NativeConfig, OutputBlock,
};

// ---- form body decoding ---------------------------------------------------

/// A decoded `application/x-www-form-urlencoded` body as ordered pairs, keeping
/// repeated keys.
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
/// `limits`. A kind that runs no agent session (`closed-issue-summary`) renders
/// no claude fields and passes `with_claude: false` so a hand-written block is
/// left untouched.
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

    /// The shipped action file. Compiled in, so deleting or renaming it breaks
    /// the build rather than silently skipping its round-trip test.
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
    fn every_closed_issue_summary_field_survives_a_round_trip() {
        let raw = "\
name: report
kind: closed-issue-summary
enabled: false
native:
  enabled: false
  parents: [CLAUDE-100, LC-4]
  query: \"-Resolved AI Agent: Reporting\"
  custom_field: Reporter
  done_label: Filed
  failed_label: Broken
  projects: [CLAUDE, LC]
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
        assert!(action_to_closed_issue_summary_form(&yt).is_none());
    }

    #[test]
    fn defaults_create_a_valid_action_of_each_kind() {
        // The blank new-action forms only need a name to save.
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
    fn bad_number_is_reported_inline() {
        let mut f = ClosedIssueSummaryForm::defaults();
        f.common.name = "report".to_string();
        f.window_days = "lots".to_string();
        let err = closed_issue_summary_form_to_yaml(empty_base(), &f).unwrap_err();
        assert!(err.contains("Window days"), "{err}");
    }

    #[test]
    fn cleared_required_fields_fall_back_to_defaults() {
        let d = ClosedIssueSummaryConfig::default();
        let mut f = ClosedIssueSummaryForm::defaults();
        f.common.name = "report".to_string();
        f.custom_field = String::new();
        f.link_type = String::new();
        let yaml = closed_issue_summary_form_to_yaml(empty_base(), &f).unwrap();
        let loaded = crate::action::validate_str(&yaml, "<test>").unwrap();
        let Some(NativeConfig::ClosedIssueSummary(cfg)) = loaded.action.native else {
            panic!("expected a closed-issue-summary config");
        };
        assert_eq!(cfg.custom_field, d.custom_field);
        assert_eq!(cfg.link_type, d.link_type);
    }

    #[test]
    fn keys_no_form_covers_are_preserved() {
        // `dedup_key` and `concurrency` have no form field; an edit must leave
        // them untouched rather than dropping them.
        let existing = "name: report\nkind: closed-issue-summary\nnative:\n  projects: [CLAUDE]\ndedup_key: key\nconcurrency:\n  max_parallel: 4\nclaude:\n  allowed_tools: Read\n";
        let loaded = crate::action::validate_str(existing, "<test>").unwrap();
        let form = action_to_closed_issue_summary_form(&loaded.action).unwrap();
        let yaml = closed_issue_summary_form_to_yaml(parsed(existing), &form).unwrap();
        let after = crate::action::validate_str(&yaml, "<test>").unwrap();
        assert_eq!(after.action.dedup_key, Some("key".to_string()));
        assert_eq!(after.action.concurrency.max_parallel, 4);
        // This kind runs no agent session, so its form has no claude fields and
        // a hand-written block survives.
        assert_eq!(after.action.claude.allowed_tools, Some("Read".to_string()));
    }

    #[test]
    fn form_pairs_keep_repeated_keys_in_order() {
        let p = FormPairs::parse(b"parents=a&parents=b&enabled=on").unwrap();
        // `get` returns the FIRST posted value for a repeated key.
        assert_eq!(p.get("parents"), "a");
        assert!(p.checked("enabled"));
        assert!(!p.checked("native_enabled"));
    }

    #[test]
    fn unchecked_boxes_turn_the_toggles_off() {
        // A browser omits an unchecked box entirely, so an absent key must read
        // as false rather than falling back to the config default (true).
        let p = FormPairs::parse(b"name=report&projects=CLAUDE").unwrap();
        let f = ClosedIssueSummaryForm::from_pairs(&p);
        assert!(!f.common.enabled);
        assert!(!f.native_enabled);
        let yaml = closed_issue_summary_form_to_yaml(empty_base(), &f).unwrap();
        let loaded = crate::action::validate_str(&yaml, "<test>").unwrap();
        assert!(!loaded.action.enabled);
        let Some(NativeConfig::ClosedIssueSummary(cfg)) = loaded.action.native else {
            panic!("expected a closed-issue-summary config");
        };
        assert!(!cfg.enabled);
    }

    /// CLAUDE-222: the structured editor renders the shared identity block, so
    /// it exposes the `link_template` field with its saved value.
    #[test]
    fn native_editor_renders_the_link_template_field() {
        let common = CommonForm {
            link_template: "https://youtrack.example.com/issue/{key}".to_string(),
            ..CommonForm::default()
        };
        let html = ClosedIssueSummaryTpl {
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
        .unwrap();
        assert!(html.contains("name=\"link_template\""), "{html}");
        assert!(
            html.contains("https://youtrack.example.com/issue/{key}"),
            "{html}"
        );
    }

    /// CLAUDE-222: `link_template` is a `CommonForm` field, so a native editor
    /// can both set it and clear it (a blank box removes the key).
    #[test]
    fn link_template_round_trips_through_the_common_block() {
        let existing = "name: report\nkind: closed-issue-summary\nnative:\n  projects: [CLAUDE]\nlink_template: \"https://youtrack.example.com/issue/{key}\"\n";
        let loaded = crate::action::validate_str(existing, "<test>").unwrap();
        let form = action_to_closed_issue_summary_form(&loaded.action).unwrap();
        assert_eq!(
            form.common.link_template,
            "https://youtrack.example.com/issue/{key}"
        );

        // Set: the prefilled value survives the save.
        let yaml = closed_issue_summary_form_to_yaml(parsed(existing), &form).unwrap();
        let after = crate::action::validate_str(&yaml, "<test>").unwrap();
        assert_eq!(
            after.action.link_template.as_deref(),
            Some("https://youtrack.example.com/issue/{key}")
        );

        // Cleared: the key is removed rather than written as an empty string.
        let mut cleared = form.clone();
        cleared.common.link_template = String::new();
        let yaml = closed_issue_summary_form_to_yaml(parsed(existing), &cleared).unwrap();
        assert!(!yaml.contains("link_template"), "{yaml}");
        let after = crate::action::validate_str(&yaml, "<test>").unwrap();
        assert_eq!(after.action.link_template, None);
    }

    /// CLAUDE-259: [`apply_common`] writes the whole `claude:` block when asked,
    /// so a pinned `effort:` (including a level outside the documented set)
    /// survives verbatim. No shipped kind in this module sets `with_claude:
    /// true` today (`closed-issue-summary` runs no agent session), so this
    /// exercises the shared helper directly rather than through a form.
    #[test]
    fn apply_common_writes_the_claude_block_when_requested() {
        let common = CommonForm {
            claude_model: "opus".to_string(),
            claude_effort: "colossal".to_string(),
            ..CommonForm::default()
        };
        let mut map = serde_yaml::Mapping::new();
        apply_common(&mut map, &common, true).unwrap();
        let claude = map
            .get(serde_yaml::Value::from("claude"))
            .expect("claude block written");
        assert_eq!(
            claude.get("effort").and_then(|v| v.as_str()),
            Some("colossal")
        );
        assert_eq!(claude.get("model").and_then(|v| v.as_str()), Some("opus"));
    }

    #[test]
    fn schedule_and_output_round_trip_through_the_common_block() {
        let p = FormPairs::parse(
            b"name=report&enabled=on&schedule_kind=interval&schedule_spec=30m&output_file=a.jsonl\
              &limit_max_runs_per_day=50&workdir_base=%7E%2Fworkspace&projects=CLAUDE",
        )
        .unwrap();
        let f = ClosedIssueSummaryForm::from_pairs(&p);
        let yaml = closed_issue_summary_form_to_yaml(empty_base(), &f).unwrap();
        let a = crate::action::validate_str(&yaml, "<test>").unwrap().action;
        let sc = a.schedule.expect("schedule");
        assert_eq!(sc.kind, crate::action::ScheduleKind::Interval);
        assert_eq!(sc.spec, Some("30m".to_string()));
        assert_eq!(a.output.file, Some("a.jsonl".to_string()));
        assert_eq!(a.limits.max_runs_per_day, Some(50));
        assert_eq!(a.workdir_base, Some("~/workspace".to_string()));
    }
}
