//! CLAUDE-101: runtime-tunable configuration stored in the DB instead of
//! `config.toml`.
//!
//! Every runtime-tunable section lives as one JSON document in the existing
//! `settings` key/value table (`migrations/sqlite/0001_init.sql`), keyed
//! `config.<section>`; [`Section::ALL`] is the list. The serde structs in
//! [`crate::config`] are the schema, so saving a section is exactly the
//! file-parser's validation (`deny_unknown_fields`), and the web Settings page
//! and the `/api/settings/<section>` JSON endpoints share this one path.
//!
//! Four of them were MOVED out of `config.toml` (`[mint]`, `[update]`,
//! `[webhook]`, `[daemon].log_filter`); the rest are DB-only from birth and have
//! no import path at all. `config.toml` keeps only bootstrap keys (`[web]`,
//! `[daemon]` binary overrides). On serve start [`migrate_config_file`] imports
//! any MOVED section still present in the file (once), then a later start that
//! finds one in BOTH the file and the DB refuses to run, naming the section -
//! the CLAUDE-87-style breaking-migration signal that prevents two sources of
//! truth.
//!
//! [`LiveSettings`] holds the runtime copy behind `RwLock`s shared with the
//! webhook listener, the webhook consumer, and the mint context, so a save
//! applies live: the log filter swaps through the tracing reload handle, the
//! webhook source map + rate limiters are rebuilt, and mint/update reads see the
//! new value on the next event / invocation. `replay_window_secs` applies on
//! restart only (the replay store is built once at start).

use std::collections::HashMap;
use std::sync::Arc;

use crate::db::DbPool;
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use tokio::sync::{Mutex, RwLock};

use crate::config::{
    AuthConfig, ClaudeConfig, ClaudeConfigHandle, CommentWebhookConfig, ConcurrencyConfig,
    DbRetentionConfig, MintConfig, OnDiskConfig, Paths, PreferencesConfig, StreamLogGcConfig,
    UpdateConfig, WebhookConfig, WorkdirGcConfig,
};
use crate::error::Result;
use crate::state::now_iso;
use crate::web::webhooks::{build_rate_map, rate_map_from, RateLimiter};

/// One DB-backed runtime-tunable config section.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Section {
    Mint,
    Update,
    Webhook,
    LogFilter,
    /// CLAUDE-106: preserved / aside workdir GC retention windows.
    WorkdirGc,
    /// CLAUDE-245: per-attempt stream-log retention window.
    StreamLogGc,
    /// CLAUDE-154: global display preferences (locale, timezone, date format).
    Preferences,
    /// CLAUDE-162: daemon-wide concurrency caps + continuous-pickup cadence.
    Concurrency,
    /// CLAUDE-175: DB history-table retention window (Phase 6).
    DbRetention,
    /// CLAUDE-220: `/claude` pull-request comment listener. DB-only from day one
    /// (never a `config.toml` section, so `migrate_config_file` never touches it).
    WebhookComment,
    /// CLAUDE-239: global defaults for the `claude` CLI invocation (the default
    /// model an action inherits when it pins none).
    Claude,
    /// The daemon's own service logins (Claude API key, YouTrack, Forgejo).
    /// Edited through the Settings page's Auth card rather than the generic JSON
    /// editor, because the values are secrets typed into password fields; the
    /// document stored here holds only URLs and secret NAMES.
    Auth,
}

impl Section {
    /// Every section, in display order (used by the Settings page).
    pub const ALL: [Section; 12] = [
        Section::Mint,
        Section::Update,
        Section::Webhook,
        Section::WebhookComment,
        Section::LogFilter,
        Section::WorkdirGc,
        Section::StreamLogGc,
        Section::Preferences,
        Section::Concurrency,
        Section::DbRetention,
        Section::Claude,
        Section::Auth,
    ];

    /// `settings` table primary key for this section's JSON document.
    pub fn key(self) -> &'static str {
        match self {
            Section::Mint => "config.mint",
            Section::Update => "config.update",
            Section::Webhook => "config.webhook",
            Section::LogFilter => "config.log_filter",
            Section::WorkdirGc => "config.workdir_gc",
            Section::StreamLogGc => "config.stream_log_gc",
            Section::Preferences => "config.preferences",
            Section::Concurrency => "config.concurrency",
            Section::DbRetention => "config.db_retention",
            Section::WebhookComment => "config.webhook_comment",
            Section::Claude => "config.claude",
            Section::Auth => "config.auth",
        }
    }

    /// URL path segment for the web / API routes (`/settings/<seg>`).
    pub fn as_path(self) -> &'static str {
        match self {
            Section::Mint => "mint",
            Section::Update => "update",
            Section::Webhook => "webhook",
            Section::LogFilter => "log_filter",
            Section::WorkdirGc => "workdir_gc",
            Section::StreamLogGc => "stream_log_gc",
            Section::Preferences => "preferences",
            Section::Concurrency => "concurrency",
            Section::DbRetention => "db_retention",
            Section::WebhookComment => "webhook_comment",
            Section::Claude => "claude",
            Section::Auth => "auth",
        }
    }

    /// Parse the URL path segment back into a section; `None` for anything else.
    pub fn from_path(s: &str) -> Option<Section> {
        Section::ALL.into_iter().find(|sec| sec.as_path() == s)
    }

    /// Human title for the Settings page card.
    pub fn title(self) -> &'static str {
        match self {
            Section::Mint => "Issue minting",
            Section::Update => "Update train",
            Section::Webhook => "Webhook sources",
            Section::LogFilter => "Log filter",
            Section::WorkdirGc => "Workdir GC",
            Section::StreamLogGc => "Stream log GC",
            Section::Preferences => "Display preferences",
            Section::Concurrency => "Concurrency",
            Section::DbRetention => "DB retention",
            Section::WebhookComment => "Comment webhook",
            Section::Claude => "Claude invocation",
            Section::Auth => "Service auth",
        }
    }

    /// Apply semantics badge shown next to the section heading.
    pub fn apply_badge(self) -> &'static str {
        match self {
            Section::Mint | Section::Update => "next event",
            Section::Webhook => "live (replay window: restart)",
            Section::LogFilter => "live",
            Section::WorkdirGc | Section::StreamLogGc => "next sweep",
            Section::Preferences => "live",
            Section::Concurrency => "next tick",
            Section::DbRetention => "next sweep",
            Section::WebhookComment => "live",
            Section::Claude => "next invocation",
            Section::Auth => "live",
        }
    }
}

/// JSON shape of the `config.log_filter` section. A single optional directive so
/// the stored document is an object (`{"filter": "info,..."}`) like the others
/// rather than a bare string, leaving room for future log knobs.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(default, deny_unknown_fields)]
pub struct LogFilterDoc {
    /// Tracing `EnvFilter` directive (see `[daemon].log_filter` docs). `None`
    /// keeps the built-in default; `CLAUDE_RUN_LOG` still wins at runtime.
    pub filter: Option<String>,
}

// ---- CLAUDE-154: preferences snapshot + date formatting -------------------

/// Process-global snapshot of the live [`PreferencesConfig`]. Askama filters are
/// free functions with no request state, so the `datetime` filter reads the
/// current preferences from here. Kept current by [`LiveSettings`] (set on build
/// and on every save of the Preferences section). Defaults to UTC / ISO, so an
/// unset snapshot renders exactly today's values.
fn preferences_cell() -> &'static std::sync::RwLock<PreferencesConfig> {
    static CELL: std::sync::OnceLock<std::sync::RwLock<PreferencesConfig>> =
        std::sync::OnceLock::new();
    CELL.get_or_init(|| std::sync::RwLock::new(PreferencesConfig::default()))
}

/// Replace the process-global preferences snapshot (called by [`LiveSettings`]).
pub fn set_preferences_snapshot(prefs: PreferencesConfig) {
    *preferences_cell()
        .write()
        .expect("preferences lock poisoned") = prefs;
}

/// Clone the current process-global preferences snapshot.
pub fn current_preferences() -> PreferencesConfig {
    preferences_cell()
        .read()
        .expect("preferences lock poisoned")
        .clone()
}

/// CLAUDE-155: map the preferences BCP-47 `locale` string (e.g. `en-US`) to a
/// [`chrono::Locale`]. chrono's locale identifiers use an underscore separator
/// (`en_US`), so the hyphen is normalized before parsing. Returns `Err` with the
/// offending value for an unknown locale so callers can reject it on save.
pub fn parse_locale(locale: &str) -> std::result::Result<chrono::Locale, String> {
    chrono::Locale::try_from(locale.replace('-', "_").as_str())
        .map_err(|_| format!("unknown locale `{locale}`"))
}

/// Validate a [`PreferencesConfig`] before it is persisted: the `locale` must map
/// to a known [`chrono::Locale`], the `timezone` must be a known IANA name, and
/// the `datetime_format` must be a parseable strftime pattern. Returns a human
/// message on the first failure, `Ok(())` otherwise.
pub fn validate_preferences(prefs: &PreferencesConfig) -> std::result::Result<(), String> {
    parse_locale(&prefs.locale)?;
    prefs
        .timezone
        .parse::<chrono_tz::Tz>()
        .map_err(|_| format!("unknown IANA timezone `{}`", prefs.timezone))?;
    if strftime_has_error(&prefs.datetime_format) {
        return Err(format!(
            "invalid strftime datetime_format `{}`",
            prefs.datetime_format
        ));
    }
    Ok(())
}

/// True when the strftime pattern contains an unparseable specifier. chrono
/// surfaces a bad specifier as an [`Item::Error`] in the parsed item stream and
/// would otherwise panic when the format is written, so we detect it up front.
fn strftime_has_error(fmt: &str) -> bool {
    use chrono::format::{Item, StrftimeItems};
    StrftimeItems::new(fmt).any(|item| matches!(item, Item::Error))
}

/// Render one stored RFC3339 UTC timestamp string for display: parse it, convert
/// to the preferences' `timezone`, and format with the `datetime_format`
/// strftime pattern in the preferences' `locale`. Name-emitting specifiers
/// (`%A`, `%a`, `%B`, `%b`, `%p`) render in that locale via chrono's
/// `format_localized` (CLAUDE-155). On ANY failure (unparseable input, bad
/// timezone, bad locale, bad format) it returns the input unchanged, so
/// sentinels like `running` / `never` / empty pass through and a malformed value
/// is never blanked or panicked on.
pub fn format_datetime(rfc3339: &str, prefs: &PreferencesConfig) -> String {
    let (Ok(tz), Ok(dt), Ok(locale)) = (
        prefs.timezone.parse::<chrono_tz::Tz>(),
        chrono::DateTime::parse_from_rfc3339(rfc3339),
        parse_locale(&prefs.locale),
    ) else {
        return rfc3339.to_string();
    };
    if strftime_has_error(&prefs.datetime_format) {
        return rfc3339.to_string();
    }
    dt.with_timezone(&tz)
        .format_localized(&prefs.datetime_format, locale)
        .to_string()
}

// ---- raw + typed store ----------------------------------------------------

/// Read a raw settings value by key. `None` when the key is absent.
pub async fn get_raw(pool: &DbPool, key: &str) -> Result<Option<String>> {
    let row: Option<(String,)> = sqlx::query_as(&crate::db::q(
        pool,
        "SELECT value FROM settings WHERE key = ?",
    ))
    .bind(key)
    .fetch_optional(pool)
    .await?;
    Ok(row.map(|r| r.0))
}

/// Upsert a raw settings value.
pub async fn set_raw(pool: &DbPool, key: &str, value: &str) -> Result<()> {
    sqlx::query(&crate::db::q(
        pool,
        "INSERT INTO settings (key, value, updated_at) VALUES (?, ?, ?)
         ON CONFLICT(key) DO UPDATE SET value = excluded.value, updated_at = excluded.updated_at",
    ))
    .bind(key)
    .bind(value)
    .bind(now_iso())
    .execute(pool)
    .await?;
    Ok(())
}

/// CLAUDE-171: `settings` key holding the wall-clock start of the CURRENT serve.
/// Not a `config.*` section: it is daemon lifetime state, not a tunable, so it
/// never appears on the Settings page.
const SERVE_STARTED_AT_KEY: &str = "daemon.started_at";

/// CLAUDE-171: stamp the start of this serve. Written once per `serve` start
/// (overwriting the previous lifetime's value), in the same
/// [`crate::state::now_iso`] RFC3339-UTC-seconds format every `run_events.ts` is
/// written in, so the web layer can bound events to this lifetime with a
/// lexicographic `ts >= started_at`. Returns the stamp it wrote.
pub async fn record_serve_start(pool: &DbPool) -> Result<String> {
    let now = now_iso();
    set_raw(pool, SERVE_STARTED_AT_KEY, &now).await?;
    Ok(now)
}

/// CLAUDE-171: the current serve's start stamp, or `None` when no serve has
/// recorded one yet (a CLI invocation against a never-served DB). Callers that
/// filter by it treat `None` as "no lower bound" rather than "no rows".
pub async fn serve_started_at(pool: &DbPool) -> Result<Option<String>> {
    get_raw(pool, SERVE_STARTED_AT_KEY).await
}

/// Read and deserialize a JSON settings document. `None` when the key is absent.
pub async fn get_json<T: DeserializeOwned>(pool: &DbPool, key: &str) -> Result<Option<T>> {
    match get_raw(pool, key).await? {
        Some(raw) => Ok(Some(serde_json::from_str(&raw)?)),
        None => Ok(None),
    }
}

/// Serialize and upsert a JSON settings document.
pub async fn set_json<T: Serialize>(pool: &DbPool, key: &str, value: &T) -> Result<()> {
    let raw = serde_json::to_string(value)?;
    set_raw(pool, key, &raw).await
}

// ---- effective runtime config --------------------------------------------

/// The runtime-tunable sections resolved for this process: the DB value when
/// present, else the (bootstrap) file value, else the built-in default. After
/// [`migrate_config_file`] runs the DB is authoritative; the file fallback only
/// matters for a CLI invocation (e.g. `version`) before any serve has migrated.
#[derive(Debug, Clone, Default)]
pub struct EffectiveRuntime {
    pub mint: MintConfig,
    pub update: UpdateConfig,
    pub webhook: WebhookConfig,
    pub log_filter: Option<String>,
    /// CLAUDE-106: preserved / aside workdir GC windows. Brand-new section (never
    /// in `config.toml`), so there is no file fallback: the DB value or the
    /// built-in default.
    pub workdir_gc: WorkdirGcConfig,
    /// CLAUDE-245: stream-log retention window. Brand-new section (never in
    /// `config.toml`), so there is no file fallback: the DB value or the
    /// built-in default (30 days).
    pub stream_log_gc: StreamLogGcConfig,
    /// CLAUDE-154: global display preferences. Brand-new section (never in
    /// `config.toml`), so the DB value or the built-in default (UTC / ISO).
    pub preferences: PreferencesConfig,
    /// CLAUDE-162: daemon-wide concurrency caps + continuous-pickup cadence.
    /// Brand-new section (never in `config.toml`), so the DB value or the default.
    pub concurrency: ConcurrencyConfig,
    /// CLAUDE-175: DB history-table retention window. Brand-new section (never in
    /// `config.toml`), so the DB value or the built-in default (180 days).
    pub db_retention: DbRetentionConfig,
    /// CLAUDE-220: `/claude` comment-command listener. DB-only from day one, so
    /// there is no file fallback: the DB value or the built-in default.
    pub webhook_comment: CommentWebhookConfig,
    /// CLAUDE-239: global `claude` invocation defaults. DB-only from day one, so
    /// there is no file fallback: the DB value or the built-in default (no
    /// model, i.e. the CLI's own default).
    pub claude: ClaudeConfig,
    /// The daemon's own service logins. DB-only from day one (never a
    /// `config.toml` section), so the DB value or the built-in default (nothing
    /// configured, i.e. host auth as before).
    pub auth: AuthConfig,
}

/// Resolve [`EffectiveRuntime`] from the DB, falling back to `file` (the parsed
/// `config.toml`) for any section the DB does not hold.
pub async fn effective(pool: &DbPool, file: &OnDiskConfig) -> Result<EffectiveRuntime> {
    let mint = get_json::<MintConfig>(pool, Section::Mint.key())
        .await?
        .unwrap_or_else(|| file.mint.clone());
    let update = get_json::<UpdateConfig>(pool, Section::Update.key())
        .await?
        .unwrap_or_else(|| file.update.clone());
    let webhook = get_json::<WebhookConfig>(pool, Section::Webhook.key())
        .await?
        .unwrap_or_else(|| file.webhook.clone());
    let log_filter = match get_json::<LogFilterDoc>(pool, Section::LogFilter.key()).await? {
        Some(doc) => doc.filter,
        None => file.daemon.log_filter.clone(),
    };
    let workdir_gc = get_json::<WorkdirGcConfig>(pool, Section::WorkdirGc.key())
        .await?
        .unwrap_or_default();
    let stream_log_gc = get_json::<StreamLogGcConfig>(pool, Section::StreamLogGc.key())
        .await?
        .unwrap_or_default();
    let preferences = get_json::<PreferencesConfig>(pool, Section::Preferences.key())
        .await?
        .unwrap_or_default();
    let concurrency = get_json::<ConcurrencyConfig>(pool, Section::Concurrency.key())
        .await?
        .unwrap_or_default();
    let db_retention = get_json::<DbRetentionConfig>(pool, Section::DbRetention.key())
        .await?
        .unwrap_or_default();
    let webhook_comment = get_json::<CommentWebhookConfig>(pool, Section::WebhookComment.key())
        .await?
        .unwrap_or_default();
    let claude = get_json::<ClaudeConfig>(pool, Section::Claude.key())
        .await?
        .unwrap_or_default();
    let auth = get_json::<AuthConfig>(pool, Section::Auth.key())
        .await?
        .unwrap_or_default();
    Ok(EffectiveRuntime {
        mint,
        update,
        webhook,
        log_filter,
        workdir_gc,
        stream_log_gc,
        preferences,
        concurrency,
        db_retention,
        webhook_comment,
        claude,
        auth,
    })
}

/// CLAUDE-162: validate a [`ConcurrencyConfig`] before it is persisted. The
/// cadence must be at least one second (a zero interval would fire an overlapping
/// run every tick). `max_parallel_global` needs no bound: `0` means auto and any
/// positive value is a valid explicit cap. Returns a human message on failure.
pub fn validate_concurrency(cfg: &ConcurrencyConfig) -> std::result::Result<(), String> {
    if cfg.regather_interval_secs == 0 {
        return Err("regather_interval_secs must be >= 1".to_string());
    }
    Ok(())
}

/// Longest reporting range the web UI exposes, in days
/// ([`crate::report::Range::Last90Days`]). A retention window shorter than this
/// would silently truncate the bounded reports, so a save is rejected below it.
pub const MIN_DB_RETENTION_DAYS: u64 = 90;

/// CLAUDE-175: validate a [`DbRetentionConfig`] before it is persisted. The
/// window must stay `>=` the longest reporting range so a prune never silently
/// truncates the 30d / 90d reports. Returns a human message on failure.
pub fn validate_db_retention(cfg: &DbRetentionConfig) -> std::result::Result<(), String> {
    if cfg.enabled && cfg.retention_days < MIN_DB_RETENTION_DAYS {
        return Err(format!(
            "retention_days must be >= {MIN_DB_RETENTION_DAYS} (the longest report range) so reports are not truncated"
        ));
    }
    Ok(())
}

/// Resolve the effective [`UpdateConfig`] for a CLI `version` invocation: the DB
/// value when present, else the file value (`[update]`), else the default.
pub async fn effective_update(pool: &DbPool, file: &OnDiskConfig) -> Result<UpdateConfig> {
    Ok(get_json::<UpdateConfig>(pool, Section::Update.key())
        .await?
        .unwrap_or_else(|| file.update.clone()))
}

/// CLAUDE-200: resolve the effective [`WebhookConfig`] for a consumer that holds
/// no parsed `config.toml` (the native CI-failure handlers, which reach the
/// webhook sources only for their per-repo log-token fallback). Same order as
/// [`effective`]: the DB document, else the on-disk file, else the default.
pub async fn effective_webhook(pool: &DbPool, paths: &Paths) -> Result<WebhookConfig> {
    match get_json::<WebhookConfig>(pool, Section::Webhook.key()).await? {
        Some(webhook) => Ok(webhook),
        None => Ok(OnDiskConfig::load(paths)?.0.webhook),
    }
}

/// CLAUDE-239: resolve the effective [`ClaudeConfig`] for a caller that holds no
/// [`LiveSettings`] (the daemonless `claude-run run`). DB-only from birth, so
/// there is no file fallback: the stored document or the built-in default.
pub async fn effective_claude(pool: &DbPool) -> Result<ClaudeConfig> {
    Ok(get_json::<ClaudeConfig>(pool, Section::Claude.key())
        .await?
        .unwrap_or_default())
}

// ---- one-time import + breaking migration --------------------------------

/// CLAUDE-101 migration. Parse `config.toml` raw and, for every moved section
/// still present in the file, either import it into the DB (when the DB does not
/// yet hold it) or refuse to start (when the DB already holds it). A missing file
/// or a file with no moved sections is a clean no-op. Bootstrap keys (`[web]`,
/// `[daemon]` binary overrides) are ignored here.
pub async fn migrate_config_file(pool: &DbPool, paths: &Paths) -> Result<()> {
    let f = paths.config_file();
    if !f.exists() {
        return Ok(());
    }
    let raw = std::fs::read_to_string(&f)?;
    let doc: toml::Value =
        toml::from_str(&raw).map_err(|e| anyhow::anyhow!("parsing {}: {}", f.display(), e))?;
    let path = f.display().to_string();

    if let Some(v) = doc.get("mint") {
        import_or_error::<MintConfig>(pool, Section::Mint, "[mint]", v.clone(), &path).await?;
    }
    if let Some(v) = doc.get("update") {
        import_or_error::<UpdateConfig>(pool, Section::Update, "[update]", v.clone(), &path)
            .await?;
    }
    if let Some(v) = doc.get("webhook") {
        import_or_error::<WebhookConfig>(pool, Section::Webhook, "[webhook]", v.clone(), &path)
            .await?;
    }
    if let Some(filter) = doc
        .get("daemon")
        .and_then(|d| d.get("log_filter"))
        .and_then(|v| v.as_str())
    {
        let doc = LogFilterDoc {
            filter: Some(filter.to_string()),
        };
        import_doc_or_error(pool, Section::LogFilter, "[daemon].log_filter", doc, &path).await?;
    }
    Ok(())
}

/// Import one TOML-subtree section into the DB, or error if the DB already holds
/// it (both-sources-present case).
async fn import_or_error<T>(
    pool: &DbPool,
    section: Section,
    label: &str,
    value: toml::Value,
    path: &str,
) -> Result<()>
where
    T: DeserializeOwned + Serialize,
{
    if get_raw(pool, section.key()).await?.is_some() {
        return Err(stale_section_error(section, label, path));
    }
    let parsed: T = value
        .try_into()
        .map_err(|e| anyhow::anyhow!("importing {label} from {path}: {e}"))?;
    set_json(pool, section.key(), &parsed).await?;
    tracing::warn!(
        section = label,
        key = section.key(),
        path = %path,
        "imported config section into the DB; remove it from config.toml (a future start with both will refuse to run)"
    );
    Ok(())
}

/// Import an already-built section document (used for `[daemon].log_filter`,
/// which is a sub-key rather than a TOML table), or error if the DB holds it.
async fn import_doc_or_error<T>(
    pool: &DbPool,
    section: Section,
    label: &str,
    doc: T,
    path: &str,
) -> Result<()>
where
    T: Serialize,
{
    if get_raw(pool, section.key()).await?.is_some() {
        return Err(stale_section_error(section, label, path));
    }
    set_json(pool, section.key(), &doc).await?;
    tracing::warn!(
        section = label,
        key = section.key(),
        path = %path,
        "imported config section into the DB; remove it from config.toml (a future start with both will refuse to run)"
    );
    Ok(())
}

fn stale_section_error(section: Section, label: &str, path: &str) -> crate::error::Error {
    anyhow::anyhow!(
        "config.toml ({path}) still defines {label}, but it has already been migrated to the \
         database (settings key `{key}`); remove {label} from config.toml to start the daemon \
         (the Settings page is now the source of truth for it)",
        key = section.key(),
    )
    .into()
}

// ---- live runtime settings -----------------------------------------------

/// Closure that swaps the live tracing `EnvFilter` (the CLAUDE-62 reload handle),
/// returning the directive error string on a bad filter. `None` when
/// `CLAUDE_RUN_LOG` is set (the env var wins, so a saved filter persists but is
/// not applied) or when no reload handle is available.
pub type LogFilterApply =
    Arc<dyn Fn(&str) -> std::result::Result<(), String> + Send + Sync + 'static>;

/// Result of a save attempt. [`SaveError::Invalid`] carries the serde message so
/// the web page can re-render it inline and the API can 400 with it; nothing is
/// persisted on an invalid payload.
#[derive(Debug)]
pub enum SaveError {
    Invalid(String),
    Db(crate::error::Error),
}

impl std::fmt::Display for SaveError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SaveError::Invalid(m) => write!(f, "{m}"),
            SaveError::Db(e) => write!(f, "{e}"),
        }
    }
}

impl From<crate::error::Error> for SaveError {
    fn from(e: crate::error::Error) -> Self {
        SaveError::Db(e)
    }
}

/// Runtime copy of the tunable settings, shared (via `Arc<RwLock<..>>`) with the
/// webhook listener, the webhook consumer, and the mint context so a save applies
/// live without a restart.
pub struct LiveSettings {
    pool: DbPool,
    mint: Arc<RwLock<MintConfig>>,
    update: Arc<RwLock<UpdateConfig>>,
    webhook: Arc<RwLock<WebhookConfig>>,
    webhook_rate: Arc<RwLock<HashMap<String, Mutex<RateLimiter>>>>,
    log_filter: Arc<RwLock<Option<String>>>,
    log_apply: Option<LogFilterApply>,
    workdir_gc: Arc<RwLock<WorkdirGcConfig>>,
    stream_log_gc: Arc<RwLock<StreamLogGcConfig>>,
    preferences: Arc<RwLock<PreferencesConfig>>,
    concurrency: Arc<RwLock<ConcurrencyConfig>>,
    db_retention: Arc<RwLock<DbRetentionConfig>>,
    /// CLAUDE-220: live comment-webhook config + its own per-source limiter map,
    /// rebuilt in lock-step on save exactly like the `webhook` section.
    webhook_comment: Arc<RwLock<CommentWebhookConfig>>,
    webhook_comment_rate: Arc<RwLock<HashMap<String, Mutex<RateLimiter>>>>,
    /// CLAUDE-239: global `claude` invocation defaults. A `std::sync::RwLock`
    /// (not the tokio one every other section uses) because the reader is
    /// `ClaudeInvoker::invoke`, which is synchronous and cannot await.
    claude: ClaudeConfigHandle,
    /// The daemon's own service logins. Read by [`crate::service_auth`] when it
    /// re-materializes the `yt` / `fj` credential files and by the Settings
    /// page's Auth card.
    auth: Arc<RwLock<AuthConfig>>,
}

impl LiveSettings {
    /// Build from the effective runtime config. Applies the effective log filter
    /// immediately (so a DB value wins over the file value the bootstrap path
    /// already applied), unless `log_apply` is `None` (env wins / no handle).
    pub fn new(pool: DbPool, eff: EffectiveRuntime, log_apply: Option<LogFilterApply>) -> Self {
        let webhook_rate = build_rate_map(&eff.webhook);
        let webhook_comment_rate = comment_rate_map(&eff.webhook_comment);
        // CLAUDE-154: seed the process-global snapshot the Askama `datetime`
        // filter reads, so display formatting is live from startup.
        set_preferences_snapshot(eff.preferences.clone());
        let s = Self {
            pool,
            mint: Arc::new(RwLock::new(eff.mint)),
            update: Arc::new(RwLock::new(eff.update)),
            webhook: Arc::new(RwLock::new(eff.webhook)),
            webhook_rate: Arc::new(RwLock::new(webhook_rate)),
            log_filter: Arc::new(RwLock::new(eff.log_filter.clone())),
            log_apply,
            workdir_gc: Arc::new(RwLock::new(eff.workdir_gc)),
            stream_log_gc: Arc::new(RwLock::new(eff.stream_log_gc)),
            preferences: Arc::new(RwLock::new(eff.preferences)),
            concurrency: Arc::new(RwLock::new(eff.concurrency)),
            db_retention: Arc::new(RwLock::new(eff.db_retention)),
            webhook_comment: Arc::new(RwLock::new(eff.webhook_comment)),
            webhook_comment_rate: Arc::new(RwLock::new(webhook_comment_rate)),
            claude: Arc::new(std::sync::RwLock::new(eff.claude)),
            auth: Arc::new(RwLock::new(eff.auth)),
        };
        if let (Some(apply), Some(filter)) = (s.log_apply.as_ref(), eff.log_filter.as_ref()) {
            match apply(filter) {
                Ok(()) => tracing::info!(filter = %filter, "log filter applied from settings"),
                Err(e) => {
                    tracing::warn!(value = %filter, error = %e, "invalid stored log filter; keeping current filter")
                }
            }
        }
        s
    }

    /// Standalone instance backed by `pool` with built-in defaults and no live
    /// log-filter hook. For tests and any context without a daemon.
    pub fn detached(pool: DbPool) -> Self {
        Self::new(pool, EffectiveRuntime::default(), None)
    }

    /// Shared handle to the live mint config (read per event by the consumer).
    pub fn mint_handle(&self) -> Arc<RwLock<MintConfig>> {
        self.mint.clone()
    }

    /// Shared handle to the live webhook config (read per request / per event).
    pub fn webhook_handle(&self) -> Arc<RwLock<WebhookConfig>> {
        self.webhook.clone()
    }

    /// Shared handle to the live per-source rate-limiter map.
    pub fn webhook_rate_handle(&self) -> Arc<RwLock<HashMap<String, Mutex<RateLimiter>>>> {
        self.webhook_rate.clone()
    }

    /// CLAUDE-220: shared handle to the live comment-webhook config, read per
    /// request by the comment listener so a Settings save routes immediately.
    pub fn webhook_comment_handle(&self) -> Arc<RwLock<CommentWebhookConfig>> {
        self.webhook_comment.clone()
    }

    /// CLAUDE-220: shared handle to the live comment per-source limiter map.
    pub fn webhook_comment_rate_handle(&self) -> Arc<RwLock<HashMap<String, Mutex<RateLimiter>>>> {
        self.webhook_comment_rate.clone()
    }

    /// CLAUDE-106: shared handle to the live workdir-GC config, read by the
    /// sweeper on each tick so a Settings save applies on the next sweep.
    pub fn workdir_gc_handle(&self) -> Arc<RwLock<WorkdirGcConfig>> {
        self.workdir_gc.clone()
    }

    /// CLAUDE-245: shared handle to the live stream-log GC config, read by the
    /// sweeper on each tick so a Settings save applies on the next sweep.
    pub fn stream_log_gc_handle(&self) -> Arc<RwLock<StreamLogGcConfig>> {
        self.stream_log_gc.clone()
    }

    /// CLAUDE-162: shared handle to the live concurrency config, read by the
    /// scheduler each tick (global cap + regather cadence) so a Settings save
    /// applies on the next tick.
    pub fn concurrency_handle(&self) -> Arc<RwLock<ConcurrencyConfig>> {
        self.concurrency.clone()
    }

    /// CLAUDE-175: shared handle to the live DB-retention config, read by the
    /// retention sweep on each tick so a Settings save applies on the next sweep.
    pub fn db_retention_handle(&self) -> Arc<RwLock<DbRetentionConfig>> {
        self.db_retention.clone()
    }

    /// CLAUDE-239: shared handle to the live `claude` invocation defaults, read
    /// by [`crate::claude::ClaudeInvoker`] on each invocation so a Settings save
    /// applies to the next one without a daemon restart.
    /// Live handle to the daemon's own service logins.
    pub fn auth_handle(&self) -> Arc<RwLock<AuthConfig>> {
        Arc::clone(&self.auth)
    }

    /// Snapshot of the daemon's own service logins (names and URLs only).
    pub async fn auth_config(&self) -> AuthConfig {
        self.auth.read().await.clone()
    }

    pub fn claude_handle(&self) -> ClaudeConfigHandle {
        self.claude.clone()
    }

    /// CLAUDE-129: a snapshot of the live update config (train + token_ref), read
    /// by the About page handlers to drive the on-demand check / apply.
    pub async fn update_config(&self) -> UpdateConfig {
        self.update.read().await.clone()
    }

    /// Current value of `section` as a pretty JSON document (secrets appear as
    /// their `*_ref` names only - no secret material is stored or rendered).
    pub async fn section_json(&self, section: Section) -> String {
        match section {
            Section::Mint => to_pretty(&*self.mint.read().await),
            Section::Update => to_pretty(&*self.update.read().await),
            Section::Webhook => to_pretty(&*self.webhook.read().await),
            Section::LogFilter => to_pretty(&LogFilterDoc {
                filter: self.log_filter.read().await.clone(),
            }),
            Section::WorkdirGc => to_pretty(&*self.workdir_gc.read().await),
            Section::StreamLogGc => to_pretty(&*self.stream_log_gc.read().await),
            Section::Preferences => to_pretty(&*self.preferences.read().await),
            Section::Concurrency => to_pretty(&*self.concurrency.read().await),
            Section::DbRetention => to_pretty(&*self.db_retention.read().await),
            Section::WebhookComment => to_pretty(&*self.webhook_comment.read().await),
            Section::Claude => to_pretty(&*read_claude(&self.claude)),
            Section::Auth => to_pretty(&*self.auth.read().await),
        }
    }

    /// Validate `json` against the section's struct, persist it, and apply it
    /// live. On an invalid payload nothing is persisted and the serde error is
    /// returned in [`SaveError::Invalid`].
    pub async fn save_section(
        &self,
        section: Section,
        json: &str,
    ) -> std::result::Result<(), SaveError> {
        match section {
            Section::Mint => {
                let v: MintConfig = parse(json)?;
                set_json(&self.pool, section.key(), &v).await?;
                *self.mint.write().await = v;
            }
            Section::Update => {
                let v: UpdateConfig = parse(json)?;
                set_json(&self.pool, section.key(), &v).await?;
                *self.update.write().await = v;
            }
            Section::Webhook => {
                let v: WebhookConfig = parse(json)?;
                v.warn_unusual_source_names();
                set_json(&self.pool, section.key(), &v).await?;
                // Rebuild the per-source rate limiters from the new map, then
                // swap the config the handler routes against. A new/edited source
                // is routable immediately; `replay_window_secs` is restart-only.
                *self.webhook_rate.write().await = build_rate_map(&v);
                *self.webhook.write().await = v;
            }
            Section::LogFilter => {
                let v: LogFilterDoc = parse(json)?;
                set_json(&self.pool, section.key(), &v).await?;
                *self.log_filter.write().await = v.filter.clone();
                if let Some(filter) = v.filter.as_deref() {
                    match self.log_apply.as_ref() {
                        Some(apply) => apply(filter)
                            .map_err(|e| SaveError::Invalid(format!("invalid log filter: {e}")))?,
                        None => tracing::info!(
                            "log filter saved; CLAUDE_RUN_LOG is set, so it is not applied this run"
                        ),
                    }
                }
            }
            Section::WorkdirGc => {
                let v: WorkdirGcConfig = parse(json)?;
                set_json(&self.pool, section.key(), &v).await?;
                *self.workdir_gc.write().await = v;
            }
            Section::StreamLogGc => {
                let v: StreamLogGcConfig = parse(json)?;
                set_json(&self.pool, section.key(), &v).await?;
                *self.stream_log_gc.write().await = v;
            }
            Section::Preferences => {
                let v: PreferencesConfig = parse(json)?;
                // Reject an unknown timezone or an unparseable strftime format
                // before persisting: nothing is stored on an invalid payload.
                validate_preferences(&v).map_err(SaveError::Invalid)?;
                set_json(&self.pool, section.key(), &v).await?;
                set_preferences_snapshot(v.clone());
                *self.preferences.write().await = v;
            }
            Section::Concurrency => {
                let v: ConcurrencyConfig = parse(json)?;
                // Reject a zero cadence before persisting: nothing is stored on an
                // invalid payload.
                validate_concurrency(&v).map_err(SaveError::Invalid)?;
                set_json(&self.pool, section.key(), &v).await?;
                *self.concurrency.write().await = v;
            }
            Section::DbRetention => {
                let v: DbRetentionConfig = parse(json)?;
                // Reject a window shorter than the longest report range before
                // persisting: nothing is stored on an invalid payload.
                validate_db_retention(&v).map_err(SaveError::Invalid)?;
                set_json(&self.pool, section.key(), &v).await?;
                *self.db_retention.write().await = v;
            }
            Section::WebhookComment => {
                let v: CommentWebhookConfig = parse(json)?;
                validate_comment_webhook(&v).map_err(SaveError::Invalid)?;
                v.warn_unusual_source_names();
                set_json(&self.pool, section.key(), &v).await?;
                // Same live-apply as the `webhook` section: rebuild the limiters
                // from the new map, then swap the config the handler routes on.
                *self.webhook_comment_rate.write().await = comment_rate_map(&v);
                *self.webhook_comment.write().await = v;
            }
            Section::Claude => {
                let v: ClaudeConfig = parse(json)?;
                // No model-name validation: the `claude` CLI is the source of
                // truth for which models exist (CLAUDE-239). No effort-level
                // validation either (CLAUDE-259): an unknown level is warned
                // about at invocation time, never rejected on save, so a level
                // added after this release is still settable.
                set_json(&self.pool, section.key(), &v).await?;
                *write_claude(&self.claude) = v;
            }
            Section::Auth => {
                let v: AuthConfig = parse(json)?;
                set_json(&self.pool, section.key(), &v).await?;
                *self.auth.write().await = v;
            }
        }
        Ok(())
    }
}

/// CLAUDE-239: read the shared [`ClaudeConfig`] handle, recovering the value a
/// panicking writer poisoned rather than propagating the panic: a stale-but-real
/// default is better than killing the run that is reading it. The poisoning is
/// logged so the panic that caused it is never silent.
pub fn read_claude(handle: &ClaudeConfigHandle) -> std::sync::RwLockReadGuard<'_, ClaudeConfig> {
    handle.read().unwrap_or_else(|poisoned| {
        tracing::error!("claude settings lock poisoned; reading the last stored value");
        poisoned.into_inner()
    })
}

/// Write half of [`read_claude`], with the same poison recovery.
fn write_claude(handle: &ClaudeConfigHandle) -> std::sync::RwLockWriteGuard<'_, ClaudeConfig> {
    handle.write().unwrap_or_else(|poisoned| {
        tracing::error!("claude settings lock poisoned; overwriting the last stored value");
        poisoned.into_inner()
    })
}

/// CLAUDE-220: per-source limiter map for the comment listener, built through the
/// SHARED [`rate_map_from`] so both listeners keep identical bucket semantics.
fn comment_rate_map(cfg: &CommentWebhookConfig) -> HashMap<String, Mutex<RateLimiter>> {
    rate_map_from(cfg.sources.iter().map(|(n, s)| (n, s.rate_limit_per_min)))
}

/// CLAUDE-220: validate a [`CommentWebhookConfig`] before it is persisted. An
/// empty trigger would match every comment and a zero instruction cap would
/// refuse every command, so both are rejected rather than stored.
pub fn validate_comment_webhook(cfg: &CommentWebhookConfig) -> std::result::Result<(), String> {
    if cfg.trigger.trim().is_empty() {
        return Err("trigger must not be empty".to_string());
    }
    if cfg.max_instruction_chars == 0 {
        return Err("max_instruction_chars must be >= 1".to_string());
    }
    for (name, src) in &cfg.sources {
        if src.allowed_permissions.is_empty() {
            return Err(format!(
                "source `{name}`: allowed_permissions must not be empty (it would deny everyone)"
            ));
        }
    }
    Ok(())
}

fn parse<T: DeserializeOwned>(json: &str) -> std::result::Result<T, SaveError> {
    serde_json::from_str(json).map_err(|e| SaveError::Invalid(e.to_string()))
}

fn to_pretty<T: Serialize>(v: &T) -> String {
    serde_json::to_string_pretty(v).unwrap_or_else(|e| format!("{{\"error\":\"{e}\"}}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn pool() -> DbPool {
        crate::db::test_pool().await
    }

    fn eff_default() -> EffectiveRuntime {
        EffectiveRuntime {
            mint: MintConfig::default(),
            update: UpdateConfig::default(),
            webhook: WebhookConfig::default(),
            log_filter: None,
            workdir_gc: WorkdirGcConfig::default(),
            stream_log_gc: StreamLogGcConfig::default(),
            preferences: PreferencesConfig::default(),
            concurrency: ConcurrencyConfig::default(),
            db_retention: DbRetentionConfig::default(),
            webhook_comment: CommentWebhookConfig::default(),
            claude: ClaudeConfig::default(),
            auth: AuthConfig::default(),
        }
    }

    #[tokio::test]
    async fn json_store_round_trips() {
        let pool = pool().await;
        assert!(get_json::<UpdateConfig>(&pool, Section::Update.key())
            .await
            .unwrap()
            .is_none());
        let u = UpdateConfig {
            train: "latest".to_string(),
            ..Default::default()
        };
        set_json(&pool, Section::Update.key(), &u).await.unwrap();
        let got: UpdateConfig = get_json(&pool, Section::Update.key())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(got.train, "latest");
    }

    #[tokio::test]
    async fn save_section_validates_and_persists() {
        let pool = pool().await;
        let live = LiveSettings::new(pool.clone(), eff_default(), None);

        // Valid payload persists and is visible in the snapshot.
        live.save_section(Section::Update, r#"{"train":"latest"}"#)
            .await
            .unwrap();
        let stored: UpdateConfig = get_json(&pool, Section::Update.key())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(stored.train, "latest");
        assert_eq!(live.update.read().await.train, "latest");

        // Invalid payload (unknown field, deny_unknown_fields) is rejected with
        // the serde message and persists nothing new.
        let err = live
            .save_section(Section::Update, r#"{"train":"latest","bogus":1}"#)
            .await
            .expect_err("unknown field must reject");
        match err {
            SaveError::Invalid(m) => assert!(m.contains("bogus") || m.contains("unknown")),
            other => panic!("expected Invalid, got {other}"),
        }
        let stored: UpdateConfig = get_json(&pool, Section::Update.key())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(stored.train, "latest", "rejected save must not overwrite");
    }

    #[tokio::test]
    async fn mint_save_is_visible_on_the_shared_handle() {
        // The consumer reads mint per event off this exact handle, so a save
        // between events is honored without a restart.
        let pool = pool().await;
        let live = LiveSettings::new(pool.clone(), eff_default(), None);
        let handle = live.mint_handle();
        assert_eq!(handle.read().await.fallback_project, "CLAUDE");
        live.save_section(Section::Mint, r#"{"fallback_project":"FJ"}"#)
            .await
            .unwrap();
        assert_eq!(handle.read().await.fallback_project, "FJ");
    }

    #[tokio::test]
    async fn webhook_save_swaps_config_and_rebuilds_limiters() {
        let pool = pool().await;
        let live = LiveSettings::new(pool.clone(), eff_default(), None);
        let cfg = live.webhook_handle();
        let rate = live.webhook_rate_handle();
        assert!(cfg.read().await.sources.is_empty());
        assert!(rate.read().await.is_empty());

        live.save_section(
            Section::Webhook,
            r#"{"replay_window_secs":300,"sources":{"ng":{"allowed_repos":["Contoso/*"],"rate_limit_per_min":120}}}"#,
        )
        .await
        .unwrap();

        // The handler routes against this exact map, so the new source is live.
        assert!(cfg.read().await.sources.contains_key("ng"));
        assert!(rate.read().await.contains_key("ng"));
    }

    // ---- CLAUDE-220: comment webhook section ------------------------------

    #[tokio::test]
    async fn comment_webhook_save_swaps_config_and_rebuilds_limiters() {
        // The listener routes against these exact handles, so a Settings save is
        // live with no daemon restart (same contract as the `webhook` section).
        let pool = pool().await;
        let live = LiveSettings::new(pool.clone(), eff_default(), None);
        let cfg = live.webhook_comment_handle();
        let rate = live.webhook_comment_rate_handle();
        assert!(cfg.read().await.sources.is_empty());
        assert!(rate.read().await.is_empty());

        live.save_section(
            Section::WebhookComment,
            r#"{"trigger":"/claude","max_instruction_chars":4000,
               "sources":{"ng":{"secret_ref":"hmac","allowed_repos":["Contoso/*"],
               "rate_limit_per_min":120,"write_token_ref":"write-pat"}}}"#,
        )
        .await
        .unwrap();

        assert!(cfg.read().await.sources.contains_key("ng"));
        assert!(rate.read().await.contains_key("ng"));
        // Persisted under the documented DB key.
        let stored: crate::config::CommentWebhookConfig =
            get_json(&pool, Section::WebhookComment.key())
                .await
                .unwrap()
                .unwrap();
        assert_eq!(stored.sources["ng"].rate_limit_per_min, 120);
        assert_eq!(Section::WebhookComment.key(), "config.webhook_comment");
        assert_eq!(Section::WebhookComment.as_path(), "webhook_comment");
        assert_eq!(
            Section::from_path("webhook_comment"),
            Some(Section::WebhookComment)
        );
    }

    #[tokio::test]
    async fn comment_webhook_save_rejects_degenerate_config() {
        let pool = pool().await;
        let live = LiveSettings::detached(pool.clone());
        for (json, needle) in [
            (r#"{"trigger":"  "}"#, "trigger"),
            (r#"{"max_instruction_chars":0}"#, "max_instruction_chars"),
            (
                r#"{"sources":{"ng":{"allowed_permissions":[]}}}"#,
                "allowed_permissions",
            ),
            (r#"{"bogus":1}"#, "bogus"),
        ] {
            let err = live
                .save_section(Section::WebhookComment, json)
                .await
                .expect_err("must reject");
            match err {
                SaveError::Invalid(m) => assert!(m.contains(needle), "{m}"),
                other => panic!("expected Invalid, got {other}"),
            }
        }
        // Nothing was persisted by any rejected save.
        assert!(get_raw(&pool, Section::WebhookComment.key())
            .await
            .unwrap()
            .is_none());
    }

    #[tokio::test]
    async fn comment_webhook_is_never_imported_from_config_toml() {
        // DB-only from day one: the migration has no arm for it, so a file that
        // somehow carries the table leaves the DB key absent (and the file itself
        // fails `OnDiskConfig` parsing, covered in `config.rs`).
        let pool = pool().await;
        let tmp = tempfile::tempdir().unwrap();
        let paths = paths_in(tmp.path());
        std::fs::write(
            paths.config_file(),
            "[web]\nbind = \"127.0.0.1\"\n\n[webhook_comment]\ntrigger = \"/claude\"\n",
        )
        .unwrap();
        migrate_config_file(&pool, &paths).await.unwrap();
        assert!(get_raw(&pool, Section::WebhookComment.key())
            .await
            .unwrap()
            .is_none());
    }

    #[tokio::test]
    async fn log_filter_save_calls_the_reload_hook() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        let pool = pool().await;
        let calls = Arc::new(AtomicUsize::new(0));
        let c = calls.clone();
        let apply: LogFilterApply = Arc::new(move |_d: &str| {
            c.fetch_add(1, Ordering::SeqCst);
            Ok(())
        });
        let live = LiveSettings::new(pool.clone(), eff_default(), Some(apply));
        // Effective log filter was None, so no startup apply.
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        live.save_section(Section::LogFilter, r#"{"filter":"info,claude_run=debug"}"#)
            .await
            .unwrap();
        assert_eq!(calls.load(Ordering::SeqCst), 1, "save applies live");
        let stored: LogFilterDoc = get_json(&pool, Section::LogFilter.key())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(stored.filter.as_deref(), Some("info,claude_run=debug"));
    }

    fn paths_in(dir: &std::path::Path) -> Paths {
        Paths {
            config_dir: dir.to_path_buf(),
            data_dir: dir.to_path_buf(),
            state_dir: dir.to_path_buf(),
            runtime_dir: dir.to_path_buf(),
        }
    }

    #[tokio::test]
    async fn migration_imports_absent_then_errors_when_both_present() {
        let pool = pool().await;
        let tmp = tempfile::tempdir().unwrap();
        let paths = paths_in(tmp.path());
        std::fs::write(
            paths.config_file(),
            "[web]\nbind = \"127.0.0.1\"\n\n[update]\ntrain = \"latest\"\n",
        )
        .unwrap();

        // First start: DB empty + file section present -> import.
        migrate_config_file(&pool, &paths).await.unwrap();
        let stored: UpdateConfig = get_json(&pool, Section::Update.key())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(stored.train, "latest");

        // Second start: DB has it AND the file still defines it -> refuse.
        let err = migrate_config_file(&pool, &paths)
            .await
            .expect_err("both sources present must error");
        assert!(err.to_string().contains("[update]"), "{err}");
    }

    #[tokio::test]
    async fn migration_clean_file_is_a_noop() {
        let pool = pool().await;
        let tmp = tempfile::tempdir().unwrap();
        let paths = paths_in(tmp.path());
        // Bootstrap-only file: [web] + [daemon] binary overrides, no moved keys.
        std::fs::write(
            paths.config_file(),
            "[web]\nbind = \"127.0.0.1\"\nport = 7321\n\n[daemon]\nnu_binary = \"nu\"\n",
        )
        .unwrap();
        migrate_config_file(&pool, &paths).await.unwrap();
        assert!(get_raw(&pool, Section::Update.key())
            .await
            .unwrap()
            .is_none());
        assert!(get_raw(&pool, Section::Mint.key()).await.unwrap().is_none());
    }

    // ---- CLAUDE-175: DB-retention section save + window validation ---------

    #[tokio::test]
    async fn db_retention_saves_and_rejects_too_short_window() {
        let pool = pool().await;
        let live = LiveSettings::detached(pool.clone());

        // A window >= the longest report range persists and applies live.
        live.save_section(
            Section::DbRetention,
            r#"{"enabled":true,"retention_days":120}"#,
        )
        .await
        .expect("a >= 90-day window is valid");
        let stored: DbRetentionConfig = get_json(&pool, Section::DbRetention.key())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(stored.retention_days, 120);

        // A window shorter than the 90-day report range is rejected, and nothing
        // is persisted over the previously stored value.
        let err = live
            .save_section(
                Section::DbRetention,
                r#"{"enabled":true,"retention_days":30}"#,
            )
            .await
            .expect_err("a < 90-day window must be rejected");
        assert!(matches!(err, SaveError::Invalid(_)), "{err}");
        let unchanged: DbRetentionConfig = get_json(&pool, Section::DbRetention.key())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            unchanged.retention_days, 120,
            "a rejected save persists nothing"
        );

        // A disabled config may carry any window (the prune never runs).
        live.save_section(
            Section::DbRetention,
            r#"{"enabled":false,"retention_days":1}"#,
        )
        .await
        .expect("a disabled config bypasses the window floor");
    }

    // ---- CLAUDE-245: stream-log GC section ---------------------------------

    #[tokio::test]
    async fn stream_log_gc_section_round_trips() {
        let pool = pool().await;
        let live = LiveSettings::new(pool.clone(), eff_default(), None);

        // Routing surface: the section is addressable as `stream_log_gc` on
        // `/settings` and `GET` / `PUT /api/settings/<section>`.
        assert_eq!(Section::StreamLogGc.key(), "config.stream_log_gc");
        assert_eq!(Section::StreamLogGc.as_path(), "stream_log_gc");
        assert_eq!(
            Section::from_path("stream_log_gc"),
            Some(Section::StreamLogGc)
        );
        assert!(Section::ALL.contains(&Section::StreamLogGc));

        // The default is served before anything is stored.
        let default: StreamLogGcConfig =
            serde_json::from_str(&live.section_json(Section::StreamLogGc).await).unwrap();
        assert_eq!(default, StreamLogGcConfig::default());

        // A save persists and applies live on the handle the sweeper reads.
        live.save_section(
            Section::StreamLogGc,
            r#"{"enabled":true,"retention_days":7}"#,
        )
        .await
        .unwrap();
        let stored: StreamLogGcConfig = get_json(&pool, Section::StreamLogGc.key())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(stored.retention_days, 7);
        assert_eq!(live.stream_log_gc_handle().read().await.retention_days, 7);

        // CLAUDE-257: the two size ceilings ride the same document, so they
        // round-trip through `GET` / `PUT /api/settings/stream_log_gc` and apply
        // on the handle the daemon's log GC pass reads.
        live.save_section(
            Section::StreamLogGc,
            r#"{"enabled":true,"retention_days":7,"daemon_log_max_bytes":1024,"output_file_max_bytes":0}"#,
        )
        .await
        .unwrap();
        let stored: StreamLogGcConfig = get_json(&pool, Section::StreamLogGc.key())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(stored.daemon_log_max_bytes, 1024);
        assert_eq!(stored.output_file_max_bytes, 0);
        let handle = live.stream_log_gc_handle();
        let applied = handle.read().await.clone();
        assert_eq!(applied.daemon_log_max_bytes, 1024);
        assert_eq!(applied.output_file_max_bytes, 0);
        // A document written before those fields existed still loads: they take
        // their defaults rather than failing the whole section.
        let legacy: StreamLogGcConfig =
            serde_json::from_str(r#"{"enabled":true,"retention_days":7}"#).unwrap();
        assert_eq!(
            legacy.daemon_log_max_bytes,
            StreamLogGcConfig::default().daemon_log_max_bytes
        );

        // An unknown field is rejected and nothing is persisted over it.
        let err = live
            .save_section(Section::StreamLogGc, r#"{"retention_dayz":7}"#)
            .await
            .expect_err("deny_unknown_fields must reject a typo");
        assert!(matches!(err, SaveError::Invalid(_)), "{err}");
        let unchanged: StreamLogGcConfig = get_json(&pool, Section::StreamLogGc.key())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(unchanged.retention_days, 7);
    }

    // ---- CLAUDE-239: global claude invocation defaults ---------------------

    #[tokio::test]
    async fn claude_section_round_trips_on_the_shared_handle() {
        let pool = pool().await;
        let live = LiveSettings::new(pool.clone(), eff_default(), None);

        // Default is no model, i.e. the claude CLI's own default.
        let handle = live.claude_handle();
        assert_eq!(read_claude(&handle).model, None);
        assert_eq!(Section::Claude.key(), "config.claude");
        assert_eq!(Section::Claude.as_path(), "claude");
        assert_eq!(Section::from_path("claude"), Some(Section::Claude));

        // A saved model persists and is live on the handle the invoker reads.
        live.save_section(Section::Claude, r#"{"model":"opus"}"#)
            .await
            .unwrap();
        let stored: ClaudeConfig = get_json(&pool, Section::Claude.key())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(stored.model.as_deref(), Some("opus"));
        assert_eq!(read_claude(&handle).model.as_deref(), Some("opus"));

        // Any model string is accepted: the CLI, not the runner, validates it.
        live.save_section(Section::Claude, r#"{"model":"claude-sonnet-4-5-20250929"}"#)
            .await
            .unwrap();
        assert_eq!(
            read_claude(&handle).model.as_deref(),
            Some("claude-sonnet-4-5-20250929")
        );

        // CLAUDE-259: effort lives in the same document and round-trips beside
        // the model, including a level outside the documented set (the runner
        // warns about that and passes it through; nothing rejects it here).
        assert_eq!(read_claude(&handle).effort, None);
        live.save_section(Section::Claude, r#"{"model":"opus","effort":"xhigh"}"#)
            .await
            .unwrap();
        let stored: ClaudeConfig = get_json(&pool, Section::Claude.key())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(stored.effort.as_deref(), Some("xhigh"));
        assert_eq!(read_claude(&handle).effort.as_deref(), Some("xhigh"));
        live.save_section(Section::Claude, r#"{"model":"opus","effort":"colossal"}"#)
            .await
            .unwrap();
        assert_eq!(read_claude(&handle).effort.as_deref(), Some("colossal"));

        // Clearing either key falls back to the CLI default again.
        live.save_section(Section::Claude, r#"{"model":"opus","effort":null}"#)
            .await
            .unwrap();
        assert_eq!(read_claude(&handle).effort, None);
        assert_eq!(read_claude(&handle).model.as_deref(), Some("opus"));
        live.save_section(Section::Claude, r#"{"model":null}"#)
            .await
            .unwrap();
        assert_eq!(read_claude(&handle).model, None);

        // An unknown field is rejected (deny_unknown_fields) and persists nothing.
        let err = live
            .save_section(Section::Claude, r#"{"bogus":"x"}"#)
            .await
            .expect_err("unknown field must reject");
        assert!(matches!(err, SaveError::Invalid(_)), "{err}");
    }

    #[tokio::test]
    async fn effective_claude_reads_the_db_else_the_default() {
        let pool = pool().await;
        assert_eq!(effective_claude(&pool).await.unwrap().model, None);
        set_json(
            &pool,
            Section::Claude.key(),
            &ClaudeConfig {
                model: Some("sonnet".to_string()),
                effort: Some("high".to_string()),
                autocompact: Some("auto".to_string()),
            },
        )
        .await
        .unwrap();
        let eff = effective_claude(&pool).await.unwrap();
        assert_eq!(eff.model.as_deref(), Some("sonnet"));
        assert_eq!(eff.effort.as_deref(), Some("high"));
    }

    #[tokio::test]
    async fn claude_is_never_imported_from_config_toml() {
        // DB-only from day one: the migration has no arm for it.
        let pool = pool().await;
        let tmp = tempfile::tempdir().unwrap();
        let paths = paths_in(tmp.path());
        std::fs::write(paths.config_file(), "[web]\nbind = \"127.0.0.1\"\n").unwrap();
        migrate_config_file(&pool, &paths).await.unwrap();
        assert!(get_raw(&pool, Section::Claude.key())
            .await
            .unwrap()
            .is_none());
    }

    // ---- CLAUDE-154: preferences formatting + validation -------------------

    #[test]
    fn format_datetime_defaults_reproduce_utc_iso() {
        // Default prefs (UTC, %Y-%m-%d %H:%M:%S %Z) render the same wall-clock
        // as the stored value, only reformatted.
        let prefs = PreferencesConfig::default();
        assert_eq!(
            format_datetime("2026-07-01T00:00:00Z", &prefs),
            "2026-07-01 00:00:00 UTC"
        );
    }

    #[test]
    fn format_datetime_converts_timezone() {
        let prefs = PreferencesConfig {
            timezone: "America/New_York".to_string(),
            datetime_format: "%Y-%m-%d %H:%M:%S %Z".to_string(),
            ..Default::default()
        };
        // 2026-07-01T00:00:00Z is EDT (UTC-4) -> 2026-06-30 20:00:00.
        assert_eq!(
            format_datetime("2026-07-01T00:00:00Z", &prefs),
            "2026-06-30 20:00:00 EDT"
        );
    }

    #[test]
    fn format_datetime_honors_custom_format() {
        let prefs = PreferencesConfig {
            timezone: "UTC".to_string(),
            datetime_format: "%m/%d/%Y %I:%M:%S %p".to_string(),
            ..Default::default()
        };
        assert_eq!(
            format_datetime("2026-07-01T13:05:09Z", &prefs),
            "07/01/2026 01:05:09 PM"
        );
    }

    #[test]
    fn format_datetime_localizes_names() {
        // CLAUDE-155: name-emitting specifiers render in the configured locale.
        // 2026-07-01 is a Wednesday in July at 13:05 (PM).
        let fr = PreferencesConfig {
            locale: "fr-FR".to_string(),
            timezone: "UTC".to_string(),
            datetime_format: "%A %d %B %Y %p".to_string(),
        };
        // French: mercredi (weekday), juillet (month). chrono renders French %p
        // empty, so the trailing separator collapses; assert the localized words.
        let out = format_datetime("2026-07-01T13:05:09Z", &fr);
        assert!(out.contains("mercredi"), "weekday not localized: {out}");
        assert!(out.contains("juillet"), "month not localized: {out}");

        // Same instant in English keeps the English names, proving locale drives it.
        let en = PreferencesConfig {
            locale: "en-US".to_string(),
            ..fr.clone()
        };
        assert_eq!(
            format_datetime("2026-07-01T13:05:09Z", &en),
            "Wednesday 01 July 2026 PM"
        );
    }

    #[test]
    fn parse_locale_maps_and_rejects() {
        // BCP-47 hyphen form normalizes to chrono's underscore identifier.
        assert!(parse_locale("fr-FR").is_ok());
        assert!(parse_locale("en-US").is_ok());
        // Unknown locale is rejected with the offending value surfaced.
        let err = parse_locale("zz-ZZ").expect_err("unknown locale must reject");
        assert!(err.contains("zz-ZZ"), "{err}");
    }

    #[test]
    fn format_datetime_falls_back_on_bad_locale() {
        // A stored-but-invalid locale must not panic or blank; input passes through.
        let bad_locale = PreferencesConfig {
            locale: "zz-ZZ".to_string(),
            ..Default::default()
        };
        assert_eq!(
            format_datetime("2026-07-01T00:00:00Z", &bad_locale),
            "2026-07-01T00:00:00Z"
        );
    }

    #[test]
    fn format_datetime_falls_back_on_malformed_input() {
        let prefs = PreferencesConfig::default();
        // Sentinels and non-timestamps pass through unchanged, never blanked.
        assert_eq!(format_datetime("running", &prefs), "running");
        assert_eq!(format_datetime("never", &prefs), "never");
        assert_eq!(format_datetime("", &prefs), "");
        assert_eq!(format_datetime("not-a-date", &prefs), "not-a-date");
    }

    #[test]
    fn format_datetime_falls_back_on_bad_prefs() {
        // A stored-but-somehow-invalid tz / format must not panic or blank.
        let bad_tz = PreferencesConfig {
            timezone: "Mars/Olympus".to_string(),
            ..Default::default()
        };
        assert_eq!(
            format_datetime("2026-07-01T00:00:00Z", &bad_tz),
            "2026-07-01T00:00:00Z"
        );
        let bad_fmt = PreferencesConfig {
            datetime_format: "%Q".to_string(),
            ..Default::default()
        };
        assert_eq!(
            format_datetime("2026-07-01T00:00:00Z", &bad_fmt),
            "2026-07-01T00:00:00Z"
        );
    }

    #[tokio::test]
    async fn preferences_save_validates_timezone_and_format() {
        let pool = pool().await;
        let live = LiveSettings::new(pool.clone(), eff_default(), None);

        // Valid payload persists and updates the global snapshot.
        live.save_section(
            Section::Preferences,
            r#"{"locale":"fr-FR","timezone":"Europe/Paris","datetime_format":"%d.%m.%Y %H:%M:%S"}"#,
        )
        .await
        .unwrap();
        let stored: PreferencesConfig = get_json(&pool, Section::Preferences.key())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(stored.timezone, "Europe/Paris");
        assert_eq!(stored.locale, "fr-FR");
        // Assert on this instance's own live copy, not the process-global
        // `current_preferences()` snapshot: that global is single-writer in
        // production but is rewritten by every parallel test's
        // `LiveSettings::new`, so reading it here races (CLAUDE-154).
        assert_eq!(live.preferences.read().await.timezone, "Europe/Paris");

        // Invalid timezone is rejected, nothing overwritten.
        let err = live
            .save_section(
                Section::Preferences,
                r#"{"locale":"en-US","timezone":"Nowhere/Void","datetime_format":"%Y-%m-%d"}"#,
            )
            .await
            .expect_err("bad timezone must reject");
        match err {
            SaveError::Invalid(m) => assert!(m.contains("timezone"), "{m}"),
            other => panic!("expected Invalid, got {other}"),
        }

        // Invalid strftime format is rejected too.
        let err = live
            .save_section(
                Section::Preferences,
                r#"{"locale":"en-US","timezone":"UTC","datetime_format":"%Q"}"#,
            )
            .await
            .expect_err("bad format must reject");
        match err {
            SaveError::Invalid(m) => assert!(m.contains("datetime_format"), "{m}"),
            other => panic!("expected Invalid, got {other}"),
        }

        // CLAUDE-155: an unknown locale is rejected on save with the value named.
        let err = live
            .save_section(
                Section::Preferences,
                r#"{"locale":"zz-ZZ","timezone":"UTC","datetime_format":"%Y-%m-%d"}"#,
            )
            .await
            .expect_err("bad locale must reject");
        match err {
            SaveError::Invalid(m) => assert!(m.contains("locale") && m.contains("zz-ZZ"), "{m}"),
            other => panic!("expected Invalid, got {other}"),
        }

        // The earlier valid value survived both rejected saves.
        let stored: PreferencesConfig = get_json(&pool, Section::Preferences.key())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(stored.timezone, "Europe/Paris");
    }

    // CLAUDE-162: the concurrency section round-trips, defaults to auto global
    // (0) + 60s cadence, and rejects a zero cadence without overwriting.
    #[tokio::test]
    async fn concurrency_section_round_trips_and_validates() {
        let pool = pool().await;
        let live = LiveSettings::new(pool.clone(), eff_default(), None);

        // Default is auto (0) + 60s, and is exposed on the shared handle.
        let handle = live.concurrency_handle();
        assert_eq!(handle.read().await.max_parallel_global, 0);
        assert_eq!(handle.read().await.regather_interval_secs, 60);

        // Valid payload persists and is visible live.
        live.save_section(
            Section::Concurrency,
            r#"{"max_parallel_global":8,"regather_interval_secs":30}"#,
        )
        .await
        .unwrap();
        let stored: ConcurrencyConfig = get_json(&pool, Section::Concurrency.key())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(stored.max_parallel_global, 8);
        assert_eq!(stored.regather_interval_secs, 30);
        assert_eq!(handle.read().await.max_parallel_global, 8);

        // A zero cadence is rejected and does not overwrite the stored value.
        let err = live
            .save_section(
                Section::Concurrency,
                r#"{"max_parallel_global":2,"regather_interval_secs":0}"#,
            )
            .await
            .expect_err("zero cadence must reject");
        match err {
            SaveError::Invalid(m) => assert!(m.contains("regather_interval_secs"), "{m}"),
            other => panic!("expected Invalid, got {other}"),
        }
        let stored: ConcurrencyConfig = get_json(&pool, Section::Concurrency.key())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            stored.regather_interval_secs, 30,
            "rejected save must not overwrite"
        );

        // An unknown field is rejected (deny_unknown_fields).
        let err = live
            .save_section(
                Section::Concurrency,
                r#"{"max_parallel_global":1,"bogus":true}"#,
            )
            .await
            .expect_err("unknown field must reject");
        assert!(matches!(err, SaveError::Invalid(_)));
    }

    #[tokio::test]
    async fn migration_imports_log_filter_subkey() {
        let pool = pool().await;
        let tmp = tempfile::tempdir().unwrap();
        let paths = paths_in(tmp.path());
        std::fs::write(
            paths.config_file(),
            "[daemon]\nnu_binary = \"nu\"\nlog_filter = \"info,claude_run=debug\"\n",
        )
        .unwrap();
        migrate_config_file(&pool, &paths).await.unwrap();
        let doc: LogFilterDoc = get_json(&pool, Section::LogFilter.key())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(doc.filter.as_deref(), Some("info,claude_run=debug"));

        // File still carries [daemon].log_filter on the next start -> refuse.
        let err = migrate_config_file(&pool, &paths)
            .await
            .expect_err("stale log_filter must error");
        assert!(err.to_string().contains("log_filter"), "{err}");
    }
}
