//! CLAUDE-101: runtime-tunable configuration stored in the DB instead of
//! `config.toml`.
//!
//! The four sections that do NOT have to be known before the daemon binds -
//! `[mint]`, `[update]`, `[webhook]`, and `[daemon].log_filter` - live as one
//! JSON document each in the existing `settings` key/value table
//! (`migrations/0001_init.sql`), keyed `config.mint` / `config.update` /
//! `config.webhook` / `config.log_filter`. The serde structs in
//! [`crate::config`] are the schema, so saving a section is exactly the
//! file-parser's validation (`deny_unknown_fields`), and the web Settings page
//! and the `/api/settings/<section>` JSON endpoints share this one path.
//!
//! `config.toml` keeps only bootstrap keys (`[web]`, `[daemon]` binary
//! overrides). On serve start [`migrate_config_file`] imports any moved section
//! still present in the file (once), then a later start that finds the section in
//! BOTH the file and the DB refuses to run, naming the section - the
//! CLAUDE-87-style breaking-migration signal that prevents two sources of truth.
//!
//! [`LiveSettings`] holds the runtime copy behind `RwLock`s shared with the
//! webhook listener, the webhook consumer, and the mint context, so a save
//! applies live: the log filter swaps through the tracing reload handle, the
//! webhook source map + rate limiters are rebuilt, and mint/update reads see the
//! new value on the next event / invocation. `replay_window_secs` applies on
//! restart only (the replay store is built once at start).

use std::collections::HashMap;
use std::sync::Arc;

use serde::{de::DeserializeOwned, Deserialize, Serialize};
use sqlx::SqlitePool;
use tokio::sync::{Mutex, RwLock};

use crate::config::{
    MintConfig, OnDiskConfig, Paths, PreferencesConfig, UpdateConfig, WebhookConfig,
    WorkdirGcConfig,
};
use crate::error::Result;
use crate::state::now_iso;
use crate::web::webhooks::{build_rate_map, RateLimiter};

/// One of the four runtime-tunable config sections moved out of `config.toml`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Section {
    Mint,
    Update,
    Webhook,
    LogFilter,
    /// CLAUDE-106: preserved / aside workdir GC retention windows.
    WorkdirGc,
    /// CLAUDE-154: global display preferences (locale, timezone, date format).
    Preferences,
}

impl Section {
    /// Every section, in display order (used by the Settings page).
    pub const ALL: [Section; 6] = [
        Section::Mint,
        Section::Update,
        Section::Webhook,
        Section::LogFilter,
        Section::WorkdirGc,
        Section::Preferences,
    ];

    /// `settings` table primary key for this section's JSON document.
    pub fn key(self) -> &'static str {
        match self {
            Section::Mint => "config.mint",
            Section::Update => "config.update",
            Section::Webhook => "config.webhook",
            Section::LogFilter => "config.log_filter",
            Section::WorkdirGc => "config.workdir_gc",
            Section::Preferences => "config.preferences",
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
            Section::Preferences => "preferences",
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
            Section::Preferences => "Display preferences",
        }
    }

    /// Apply semantics badge shown next to the section heading.
    pub fn apply_badge(self) -> &'static str {
        match self {
            Section::Mint | Section::Update => "next event",
            Section::Webhook => "live (replay window: restart)",
            Section::LogFilter => "live",
            Section::WorkdirGc => "next sweep",
            Section::Preferences => "live",
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
pub async fn get_raw(pool: &SqlitePool, key: &str) -> Result<Option<String>> {
    let row: Option<(String,)> = sqlx::query_as("SELECT value FROM settings WHERE key = ?")
        .bind(key)
        .fetch_optional(pool)
        .await?;
    Ok(row.map(|r| r.0))
}

/// Upsert a raw settings value.
pub async fn set_raw(pool: &SqlitePool, key: &str, value: &str) -> Result<()> {
    sqlx::query(
        "INSERT INTO settings (key, value, updated_at) VALUES (?, ?, ?)
         ON CONFLICT(key) DO UPDATE SET value = excluded.value, updated_at = excluded.updated_at",
    )
    .bind(key)
    .bind(value)
    .bind(now_iso())
    .execute(pool)
    .await?;
    Ok(())
}

/// Read and deserialize a JSON settings document. `None` when the key is absent.
pub async fn get_json<T: DeserializeOwned>(pool: &SqlitePool, key: &str) -> Result<Option<T>> {
    match get_raw(pool, key).await? {
        Some(raw) => Ok(Some(serde_json::from_str(&raw)?)),
        None => Ok(None),
    }
}

/// Serialize and upsert a JSON settings document.
pub async fn set_json<T: Serialize>(pool: &SqlitePool, key: &str, value: &T) -> Result<()> {
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
    /// CLAUDE-154: global display preferences. Brand-new section (never in
    /// `config.toml`), so the DB value or the built-in default (UTC / ISO).
    pub preferences: PreferencesConfig,
}

/// Resolve [`EffectiveRuntime`] from the DB, falling back to `file` (the parsed
/// `config.toml`) for any section the DB does not hold.
pub async fn effective(pool: &SqlitePool, file: &OnDiskConfig) -> Result<EffectiveRuntime> {
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
    let preferences = get_json::<PreferencesConfig>(pool, Section::Preferences.key())
        .await?
        .unwrap_or_default();
    Ok(EffectiveRuntime {
        mint,
        update,
        webhook,
        log_filter,
        workdir_gc,
        preferences,
    })
}

/// Resolve the effective [`UpdateConfig`] for a CLI `version` invocation: the DB
/// value when present, else the file value (`[update]`), else the default.
pub async fn effective_update(pool: &SqlitePool, file: &OnDiskConfig) -> Result<UpdateConfig> {
    Ok(get_json::<UpdateConfig>(pool, Section::Update.key())
        .await?
        .unwrap_or_else(|| file.update.clone()))
}

// ---- one-time import + breaking migration --------------------------------

/// CLAUDE-101 migration. Parse `config.toml` raw and, for every moved section
/// still present in the file, either import it into the DB (when the DB does not
/// yet hold it) or refuse to start (when the DB already holds it). A missing file
/// or a file with no moved sections is a clean no-op. Bootstrap keys (`[web]`,
/// `[daemon]` binary overrides) are ignored here.
pub async fn migrate_config_file(pool: &SqlitePool, paths: &Paths) -> Result<()> {
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
    pool: &SqlitePool,
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
    pool: &SqlitePool,
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
    pool: SqlitePool,
    mint: Arc<RwLock<MintConfig>>,
    update: Arc<RwLock<UpdateConfig>>,
    webhook: Arc<RwLock<WebhookConfig>>,
    webhook_rate: Arc<RwLock<HashMap<String, Mutex<RateLimiter>>>>,
    log_filter: Arc<RwLock<Option<String>>>,
    log_apply: Option<LogFilterApply>,
    workdir_gc: Arc<RwLock<WorkdirGcConfig>>,
    preferences: Arc<RwLock<PreferencesConfig>>,
}

impl LiveSettings {
    /// Build from the effective runtime config. Applies the effective log filter
    /// immediately (so a DB value wins over the file value the bootstrap path
    /// already applied), unless `log_apply` is `None` (env wins / no handle).
    pub fn new(pool: SqlitePool, eff: EffectiveRuntime, log_apply: Option<LogFilterApply>) -> Self {
        let webhook_rate = build_rate_map(&eff.webhook);
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
            preferences: Arc::new(RwLock::new(eff.preferences)),
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
    pub fn detached(pool: SqlitePool) -> Self {
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

    /// CLAUDE-106: shared handle to the live workdir-GC config, read by the
    /// sweeper on each tick so a Settings save applies on the next sweep.
    pub fn workdir_gc_handle(&self) -> Arc<RwLock<WorkdirGcConfig>> {
        self.workdir_gc.clone()
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
            Section::Preferences => to_pretty(&*self.preferences.read().await),
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
            Section::Preferences => {
                let v: PreferencesConfig = parse(json)?;
                // Reject an unknown timezone or an unparseable strftime format
                // before persisting: nothing is stored on an invalid payload.
                validate_preferences(&v).map_err(SaveError::Invalid)?;
                set_json(&self.pool, section.key(), &v).await?;
                set_preferences_snapshot(v.clone());
                *self.preferences.write().await = v;
            }
        }
        Ok(())
    }
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
    use crate::db::MIGRATOR;
    use sqlx::sqlite::SqlitePoolOptions;

    async fn pool() -> SqlitePool {
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect("sqlite::memory:")
            .await
            .unwrap();
        MIGRATOR.run(&pool).await.unwrap();
        pool
    }

    fn eff_default() -> EffectiveRuntime {
        EffectiveRuntime {
            mint: MintConfig::default(),
            update: UpdateConfig::default(),
            webhook: WebhookConfig::default(),
            log_filter: None,
            workdir_gc: WorkdirGcConfig::default(),
            preferences: PreferencesConfig::default(),
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
        assert_eq!(current_preferences().timezone, "Europe/Paris");

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
