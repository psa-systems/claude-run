//! Database connection, dialect chokepoint, and migrations.
//!
//! Two backends, one code path. SQLite is the default and is what every host
//! install and the container image use unless the operator sets a URL; a
//! `postgres://` URL points the same daemon at a PostgreSQL server instead.
//! Both run through sqlx's `Any` driver, so the ~130 statements
//! across `src/` are written once and every module threads a single
//! [`DbPool`].
//!
//! `Any` does NOT translate SQL: it forwards the query string to the backend
//! unchanged. Everything dialect-specific therefore has to be handled here or
//! spelled portably at the call site:
//!
//! - Handled here, by [`q`], which every query string in the tree goes through:
//!   `?` / `?N` placeholders become `$n` for Postgres, and the two functions
//!   with no common spelling are written neutrally at the call site and
//!   emitted per dialect here. `strpos(haystack, needle)` becomes SQLite's
//!   `instr(...)`, and `elapsed_seconds(from, to)` becomes SQLite's
//!   `julianday()` difference or Postgres's `extract(epoch from ...)`. Neither
//!   backend's private spelling appears in a call site.
//! - Spelled portably at the call site: `COALESCE` instead of `IFNULL`,
//!   `ON CONFLICT DO NOTHING` instead of `INSERT OR IGNORE`, and an explicit
//!   `CAST(... AS BIGINT)` around each `SUM()` over an integer column (Postgres
//!   sums integers into `numeric`, which the `Any` driver cannot decode).

use std::borrow::Cow;
use std::path::Path;
use std::str::FromStr;
use std::time::{Duration, Instant};

use sqlx::any::{AnyConnectOptions, AnyPoolOptions};
use sqlx::Executor;
use tracing::{error, warn};

use crate::config::DatabaseTarget;
use crate::error::{Error, Result};

/// The pool every module threads. One type, whichever backend is configured.
/// SPEC-5.1.
pub type DbPool = sqlx::AnyPool;

/// The SQLite chain, twenty files, unchanged since before CLAUDE-250 so an
/// existing `state.db` matches the versions and checksums already in its
/// `_sqlx_migrations` table and re-runs nothing. SPEC-5.4.
pub static SQLITE_MIGRATOR: sqlx::migrate::Migrator = sqlx::migrate!("./migrations/sqlite");

/// The Postgres schema, squashed into one file. A Postgres database is new by
/// definition here, so it needs the end state rather than the history.
/// SPEC-5.4.
pub static POSTGRES_MIGRATOR: sqlx::migrate::Migrator = sqlx::migrate!("./migrations/postgres");

/// How long [`open`] keeps retrying an unreachable Postgres before giving up.
/// Compose can start the daemon before the database accepts connections, so an
/// instant abort would crash-loop a healthy stack; an unbounded wait would hide
/// a genuinely wrong URL forever.
const CONNECT_RETRY_DEADLINE: Duration = Duration::from_secs(60);
const CONNECT_RETRY_INTERVAL: Duration = Duration::from_secs(2);

/// Which SQL dialect the open pool speaks. SPEC-5.1.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Dialect {
    Sqlite,
    Postgres,
}

impl Dialect {
    /// The dialect a connection URL scheme selects. `sqlite`, `postgres` and
    /// `postgresql` are the whole accepted set; anything else is a startup
    /// error naming the scheme rather than a silent fallback to SQLite.
    /// SPEC-5.1.
    pub fn from_scheme(scheme: &str) -> Result<Self> {
        match scheme {
            "sqlite" => Ok(Dialect::Sqlite),
            "postgres" | "postgresql" => Ok(Dialect::Postgres),
            other => Err(crate::error::Error::Other(anyhow::anyhow!(
                "unsupported database URL scheme `{other}`: expected sqlite, postgres or postgresql"
            ))),
        }
    }
}

/// The dialect an open pool speaks, read back off the URL it was built from.
/// Cheap enough to call per statement: an `Arc` clone plus a scheme slice.
/// SPEC-5.1.
pub fn dialect(pool: &DbPool) -> Dialect {
    let opts = pool.connect_options();
    // The pool cannot exist unless `open` already accepted this scheme.
    Dialect::from_scheme(opts.database_url.scheme()).unwrap_or(Dialect::Sqlite)
}

/// Read an integer 0/1 flag column as a `bool`.
///
/// Every boolean-ish column in the schema is an integer on BOTH backends (see
/// `migrations/postgres/0001_init.sql` for why), and the `Any` driver reports
/// integers and booleans as different kinds with no coercion between them, so
/// deriving `FromRow` onto a `bool` field would fail to decode. Row structs
/// with flag columns implement `FromRow` by hand through this. SPEC-5.1.
pub fn flag(row: &sqlx::any::AnyRow, column: &str) -> std::result::Result<bool, sqlx::Error> {
    use sqlx::Row;
    Ok(row.try_get::<i64, _>(column)? != 0)
}

/// Position of `needle` in `haystack`, 1-based, 0 when absent. Written this way
/// in every query that needs it; [`q`] emits SQLite's `instr` or leaves the
/// Postgres spelling alone.
const NEUTRAL_STRPOS: &str = "strpos";

/// Seconds from the RFC 3339 timestamp in the first argument to the one in the
/// second. Written this way in every query that needs it; [`q`] emits the
/// SQLite or the Postgres spelling.
const NEUTRAL_ELAPSED: &str = "elapsed_seconds";

/// The one chokepoint every query string passes through on its way to sqlx.
/// Borrows unchanged in the common case: a SQLite statement with neither
/// neutral function in it is already exactly what the backend wants.
/// SPEC-5.1.
pub fn q<'a>(pool: &DbPool, sql: &'a str) -> Cow<'a, str> {
    rewrite(dialect(pool), sql)
}

/// [`q`] with the dialect supplied directly, so the rewrites are testable with
/// no database of either kind. SPEC-5.1.
pub fn rewrite(dialect: Dialect, sql: &str) -> Cow<'_, str> {
    let needs_functions = sql.contains(NEUTRAL_STRPOS) || sql.contains(NEUTRAL_ELAPSED);
    match dialect {
        Dialect::Sqlite if !needs_functions => Cow::Borrowed(sql),
        _ => Cow::Owned(to_dialect(dialect, sql, &mut 0)),
    }
}

/// Rewrite one statement into `dialect`.
///
/// Scans rather than regexes so a `?` or a function name inside a string
/// literal, a quoted identifier or a comment is left exactly as written.
/// `next_placeholder` is threaded rather than local so a rewritten function
/// argument keeps numbering in step with the rest of the statement.
fn to_dialect(dialect: Dialect, sql: &str, next_placeholder: &mut usize) -> String {
    let bytes = sql.as_bytes();
    let mut out = String::with_capacity(sql.len() + 16);
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'\'' | b'"' => {
                let quote = bytes[i];
                let end = skip_quoted(bytes, i, quote);
                out.push_str(&sql[i..end]);
                i = end;
            }
            b'-' if bytes.get(i + 1) == Some(&b'-') => {
                let end = skip_line_comment(bytes, i);
                out.push_str(&sql[i..end]);
                i = end;
            }
            b'/' if bytes.get(i + 1) == Some(&b'*') => {
                let end = skip_block_comment(bytes, i);
                out.push_str(&sql[i..end]);
                i = end;
            }
            b'?' if dialect == Dialect::Postgres => {
                let digits_end = take_digits(bytes, i + 1);
                if digits_end > i + 1 {
                    // `?N` is SQLite's explicitly numbered placeholder and maps
                    // straight onto `$N`; sqlx binds by position either way.
                    out.push('$');
                    out.push_str(&sql[i + 1..digits_end]);
                    i = digits_end;
                } else {
                    *next_placeholder += 1;
                    out.push_str(&format!("${next_placeholder}"));
                    i += 1;
                }
            }
            c if is_ident_start(c) => {
                let end = take_ident(bytes, i);
                let word = &sql[i..end];
                let neutral = word.eq_ignore_ascii_case(NEUTRAL_STRPOS)
                    || word.eq_ignore_ascii_case(NEUTRAL_ELAPSED);
                let args = if neutral { call_args(bytes, end) } else { None };
                let Some(args) = args else {
                    out.push_str(word);
                    i = end;
                    continue;
                };
                if word.eq_ignore_ascii_case(NEUTRAL_STRPOS) {
                    let inner = to_dialect(
                        dialect,
                        &sql[args.inner_start..args.inner_end],
                        next_placeholder,
                    );
                    // Same argument order and the same 1-based result in both,
                    // so the caller's `> 0` test holds either way.
                    let name = match dialect {
                        Dialect::Sqlite => "instr",
                        Dialect::Postgres => "strpos",
                    };
                    out.push_str(&format!("{name}({inner})"));
                } else {
                    let (from, to) = split_two_args(&sql[args.inner_start..args.inner_end]);
                    let from = to_dialect(dialect, from, next_placeholder);
                    let to = to_dialect(dialect, to, next_placeholder);
                    out.push_str(&match dialect {
                        // julianday is in days; scaling to seconds makes the
                        // two backends agree on the value, not just its order.
                        Dialect::Sqlite => {
                            format!("((julianday({to}) - julianday({from})) * 86400.0)")
                        }
                        Dialect::Postgres => format!(
                            "extract(epoch from (({to})::timestamptz - ({from})::timestamptz))"
                        ),
                    });
                }
                i = args.end;
            }
            _ => {
                let ch = sql[i..].chars().next().expect("index is a char boundary");
                out.push(ch);
                i += ch.len_utf8();
            }
        }
    }
    out
}

/// Split a two-argument list at its top-level comma. A malformed call is left
/// as one argument and an empty second, which the backend then rejects as the
/// syntax error it is.
fn split_two_args(args: &str) -> (&str, &str) {
    let bytes = args.as_bytes();
    let mut depth = 0usize;
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'\'' | b'"' => {
                i = skip_quoted(bytes, i, bytes[i]);
                continue;
            }
            b'(' => depth += 1,
            b')' => depth = depth.saturating_sub(1),
            b',' if depth == 0 => return (args[..i].trim(), args[i + 1..].trim()),
            _ => {}
        }
        i += 1;
    }
    (args.trim(), "")
}

/// Byte offsets of a parenthesised argument list: the whole `(...)` and the
/// span between the parentheses.
struct CallArgs {
    inner_start: usize,
    inner_end: usize,
    end: usize,
}

/// The argument list of a function call whose name ends at `from`, or `None`
/// when the identifier is not a call (or the parentheses never close).
fn call_args(bytes: &[u8], from: usize) -> Option<CallArgs> {
    let mut i = from;
    while i < bytes.len() && bytes[i].is_ascii_whitespace() {
        i += 1;
    }
    if bytes.get(i) != Some(&b'(') {
        return None;
    }
    let inner_start = i + 1;
    let mut depth = 0usize;
    while i < bytes.len() {
        match bytes[i] {
            b'\'' | b'"' => {
                i = skip_quoted(bytes, i, bytes[i]);
                continue;
            }
            b'(' => depth += 1,
            b')' => {
                depth -= 1;
                if depth == 0 {
                    return Some(CallArgs {
                        inner_start,
                        inner_end: i,
                        end: i + 1,
                    });
                }
            }
            _ => {}
        }
        i += 1;
    }
    None
}

/// End offset of the quoted run starting at `start` (the closing quote
/// included), treating a doubled quote as an escape. An unterminated literal
/// runs to end of input, which the backend then rejects as the syntax error it
/// is.
fn skip_quoted(bytes: &[u8], start: usize, quote: u8) -> usize {
    let mut i = start + 1;
    while i < bytes.len() {
        if bytes[i] == quote {
            if bytes.get(i + 1) == Some(&quote) {
                i += 2;
                continue;
            }
            return i + 1;
        }
        i += 1;
    }
    bytes.len()
}

fn skip_line_comment(bytes: &[u8], start: usize) -> usize {
    let mut i = start + 2;
    while i < bytes.len() && bytes[i] != b'\n' {
        i += 1;
    }
    i
}

fn skip_block_comment(bytes: &[u8], start: usize) -> usize {
    let mut i = start + 2;
    while i + 1 < bytes.len() {
        if bytes[i] == b'*' && bytes[i + 1] == b'/' {
            return i + 2;
        }
        i += 1;
    }
    bytes.len()
}

fn take_digits(bytes: &[u8], start: usize) -> usize {
    let mut i = start;
    while i < bytes.len() && bytes[i].is_ascii_digit() {
        i += 1;
    }
    i
}

fn take_ident(bytes: &[u8], start: usize) -> usize {
    let mut i = start;
    while i < bytes.len() && (bytes[i].is_ascii_alphanumeric() || bytes[i] == b'_') {
        i += 1;
    }
    i
}

fn is_ident_start(c: u8) -> bool {
    c.is_ascii_alphabetic() || c == b'_'
}

/// Open the pool for `target` and run the matching migration set. SPEC-5.4.
pub async fn open(target: &DatabaseTarget) -> Result<DbPool> {
    // Registers the sqlite and postgres backends behind the `Any` driver. Idempotent.
    sqlx::any::install_default_drivers();
    match target {
        DatabaseTarget::Sqlite(path) => open_sqlite(path).await,
        DatabaseTarget::Postgres(url) => open_postgres(url).await,
    }
}

/// Convenience for the many tests and CLI paths that just want the SQLite
/// file. SPEC-5.4.
pub async fn open_sqlite(db_path: &Path) -> Result<DbPool> {
    sqlx::any::install_default_drivers();
    if let Some(parent) = db_path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let url = format!("sqlite://{}?mode=rwc", db_path.display());
    let started = Instant::now();
    let pool = pool_options(Dialect::Sqlite)
        .connect_with(AnyConnectOptions::from_str(&url)?)
        .await
        .map_err(|e| {
            map_connect_error(
                "sqlite",
                POOL_MAX_CONNECTIONS,
                pool_acquire_timeout(),
                started,
                e,
            )
        })?;
    let started = Instant::now();
    SQLITE_MIGRATOR.run(&pool).await.map_err(|e| {
        if migrate_pool_timeout(&e) {
            map_live_pool_error(&pool, "sqlite", started, sqlx::Error::PoolTimedOut)
        } else {
            e.into()
        }
    })?;
    Ok(pool)
}

/// True when a migration failed because the acquire that fetches its
/// connection timed out, rather than for any other reason (a bad statement, a
/// constraint violation, ...).
fn migrate_pool_timeout(e: &sqlx::migrate::MigrateError) -> bool {
    matches!(
        e,
        sqlx::migrate::MigrateError::Execute(sqlx::Error::PoolTimedOut)
            | sqlx::migrate::MigrateError::ExecuteMigration(sqlx::Error::PoolTimedOut, _)
    )
}

async fn open_postgres(url: &str) -> Result<DbPool> {
    let opts = AnyConnectOptions::from_str(url)?;
    // Never the password: this string is logged on every retry.
    let host = opts.database_url.host_str().unwrap_or("?").to_string();
    let deadline = std::time::Instant::now() + CONNECT_RETRY_DEADLINE;
    let mut attempt = 0u32;
    let pool = loop {
        attempt += 1;
        let attempt_started = Instant::now();
        match pool_options(Dialect::Postgres)
            .connect_with(opts.clone())
            .await
        {
            Ok(pool) => break pool,
            Err(e) if std::time::Instant::now() < deadline => {
                warn!(
                    host = %host,
                    attempt,
                    error = %e,
                    "postgres not reachable yet, retrying"
                );
                tokio::time::sleep(CONNECT_RETRY_INTERVAL).await;
            }
            // A pool-acquire timeout on the final attempt gets the dedicated
            // variant (and its own `error!` log) instead of the generic
            // unreachable-host message below, so it is reported like every
            // other pool acquire timeout rather than losing its pool state.
            Err(e) if matches!(e, sqlx::Error::PoolTimedOut) => {
                return Err(map_connect_error(
                    "postgres",
                    POOL_MAX_CONNECTIONS,
                    pool_acquire_timeout(),
                    attempt_started,
                    e,
                ));
            }
            Err(e) => {
                // Loud and terminal. Falling back to SQLite here would start a
                // second, empty state store while the operator believes the
                // daemon is on their server.
                error!(
                    host = %host,
                    attempts = attempt,
                    deadline_secs = CONNECT_RETRY_DEADLINE.as_secs(),
                    error = %e,
                    "postgres unreachable, giving up"
                );
                // The log line above is for the journal; the returned error is
                // what the operator sees on their terminal, so it carries the
                // host too rather than a bare driver message.
                return Err(anyhow::anyhow!(
                    "postgres at {host} unreachable after {attempt} attempts over {}s: {e}",
                    CONNECT_RETRY_DEADLINE.as_secs()
                )
                .into());
            }
        }
    };
    let started = Instant::now();
    POSTGRES_MIGRATOR.run(&pool).await.map_err(|e| {
        if migrate_pool_timeout(&e) {
            map_live_pool_error(&pool, "postgres", started, sqlx::Error::PoolTimedOut)
        } else {
            e.into()
        }
    })?;
    Ok(pool)
}

/// The production pool's capacity and acquire timeout, named so the pool
/// acquire timeout error (below) can report them without a live pool to read
/// them off.
const POOL_MAX_CONNECTIONS: u32 = 16;
const POOL_ACQUIRE_TIMEOUT: Duration = Duration::from_secs(30);

/// The acquire timeout the production pools (`open_sqlite`, `open_postgres`)
/// are built with. In production it is [`POOL_ACQUIRE_TIMEOUT`] (30s): an
/// acquire that waits that long is a real stuck pool and should fail fast.
/// Under `cfg(test)` it is raised to 120s, matching the test-fixture pools'
/// own rationale (CLAUDE-524): on a contended CI host a connection can take
/// far longer than 30s to free purely from scheduler/IO starvation, not a
/// stuck pool (SPEC-10.1.4 slow-IO class), which was tripping the file-backed
/// SQLite pool these tests open at 30s (CLAUDE-542). The production binary is
/// not built with `cfg(test)`, so it keeps the 30s fail-fast behaviour.
fn pool_acquire_timeout() -> Duration {
    if cfg!(test) {
        Duration::from_secs(120)
    } else {
        POOL_ACQUIRE_TIMEOUT
    }
}

/// Pool settings, applied to both backends.
fn pool_options(dialect: Dialect) -> AnyPoolOptions {
    let opts = AnyPoolOptions::new()
        // 8 left no headroom once the daemon runs several overlapping
        // runs plus the web UI's polling.
        .max_connections(POOL_MAX_CONNECTIONS)
        // sqlx's default, stated so it is a decision and not an
        // inherited value. An acquire that waits this long is a real fault
        // (raised under cfg(test); see pool_acquire_timeout).
        .acquire_timeout(pool_acquire_timeout())
        // Raised from the 2s default. With blocking work off the
        // async runtime an acquire is sub-millisecond, so 5s flags a genuine
        // regression instead of logging routine contention.
        .acquire_slow_threshold(Duration::from_secs(5));
    match dialect {
        // WAL + NORMAL + a 5s busy timeout so one daemon and the occasional CLI
        // invocation cooperate on the same file. These arrive as PRAGMAs rather
        // than builder calls because `AnyConnectOptions` is only a URL, and
        // sqlx's SQLite URL parser rejects any query parameter outside
        // mode / cache / immutable / vfs.
        Dialect::Sqlite => opts.after_connect(|conn, _meta| {
            Box::pin(async move {
                conn.execute("PRAGMA journal_mode = WAL;").await?;
                conn.execute("PRAGMA synchronous = NORMAL;").await?;
                conn.execute("PRAGMA busy_timeout = 5000;").await?;
                Ok(())
            })
        }),
        Dialect::Postgres => opts,
    }
}

/// Builds [`Error::PoolAcquireTimeout`] for `pool_name` and logs it once at
/// `error` with the same fields, so a pool acquire that exceeds
/// `acquire_timeout` is reported with the operation's wait time and the
/// pool's state instead of a bare `Sqlx(PoolTimedOut)`. SPEC-10.1.4.
fn pool_acquire_timeout_error(
    pool_name: &str,
    timeout: Duration,
    size: u32,
    idle: u32,
    waited: Duration,
) -> Error {
    error!(
        pool = pool_name,
        waited_ms = waited.as_millis() as u64,
        timeout_secs = timeout.as_secs(),
        size,
        idle,
        "pool acquire timed out"
    );
    Error::PoolAcquireTimeout {
        pool: pool_name.to_string(),
        waited,
        timeout,
        size,
        idle,
    }
}

/// Maps `err` into [`pool_acquire_timeout_error`] when it is a
/// `sqlx::Error::PoolTimedOut` surfaced while a `pool` already exists (a
/// query or migration acquiring a connection), reading the pool's live state;
/// any other error passes through unchanged.
fn map_live_pool_error(
    pool: &DbPool,
    pool_name: &str,
    started: Instant,
    err: sqlx::Error,
) -> Error {
    match err {
        sqlx::Error::PoolTimedOut => pool_acquire_timeout_error(
            pool_name,
            pool.options().get_acquire_timeout(),
            pool.size(),
            pool.num_idle() as u32,
            started.elapsed(),
        ),
        other => other.into(),
    }
}

/// Maps `err` into [`pool_acquire_timeout_error`] when it is a
/// `sqlx::Error::PoolTimedOut` surfaced while establishing `pool_name`
/// itself: no pool exists yet on this path, so the idle count is always 0 and
/// the size is the configured `max_connections`; any other error passes
/// through unchanged.
fn map_connect_error(
    pool_name: &str,
    max_connections: u32,
    timeout: Duration,
    started: Instant,
    err: sqlx::Error,
) -> Error {
    match err {
        sqlx::Error::PoolTimedOut => {
            pool_acquire_timeout_error(pool_name, timeout, max_connections, 0, started.elapsed())
        }
        other => other.into(),
    }
}

/// The pool every test builds its fixture in: an in-memory SQLite database by
/// default, or a private schema on the server named by
/// `CLAUDE_RUN_TEST_DATABASE_URL`. Setting that variable therefore re-runs the
/// WHOLE suite against Postgres, which is what makes the ~130 statements in the
/// tree actually exercised on both backends rather than only on the default one.
#[cfg(test)]
pub async fn test_pool() -> DbPool {
    match std::env::var("CLAUDE_RUN_TEST_DATABASE_URL") {
        Ok(url) => test_pg_pool_at(&url, 4).await,
        Err(_) => test_sqlite_pool().await,
    }
}

/// An in-memory SQLite pool with the schema applied. One connection, because a
/// second connection to `sqlite::memory:` is a second, empty database. A test
/// that drops a transaction on this pool releases the connection only once
/// sqlx's background rollback finishes, and under the CPU contention of a
/// full-suite run that rollback can be scheduled well behind the 30s default
/// (CLAUDE-524); the queries themselves are in-memory and instant, so a
/// longer acquire timeout only tolerates scheduler starvation, not a real
/// stuck pool.
#[cfg(test)]
pub async fn test_sqlite_pool() -> DbPool {
    sqlx::any::install_default_drivers();
    let max_connections = 1;
    let acquire_timeout = Duration::from_secs(120);
    let started = Instant::now();
    let pool = AnyPoolOptions::new()
        .max_connections(max_connections)
        .acquire_timeout(acquire_timeout)
        .connect("sqlite::memory:")
        .await
        .map_err(|e| {
            map_connect_error(
                "sqlite test fixture",
                max_connections,
                acquire_timeout,
                started,
                e,
            )
        })
        .unwrap_or_else(|e: Error| panic!("{e}"));
    let started = Instant::now();
    SQLITE_MIGRATOR
        .run(&pool)
        .await
        .map_err(|e| {
            if migrate_pool_timeout(&e) {
                map_live_pool_error(
                    &pool,
                    "sqlite test fixture",
                    started,
                    sqlx::Error::PoolTimedOut,
                )
            } else {
                e.into()
            }
        })
        .unwrap_or_else(|e: Error| panic!("{e}"));
    pool
}

/// The Postgres pool the backend-specific tests run against, or `None` when
/// `CLAUDE_RUN_TEST_DATABASE_URL` is unset.
#[cfg(test)]
pub async fn test_pg_pool() -> Option<DbPool> {
    let url = std::env::var("CLAUDE_RUN_TEST_DATABASE_URL").ok()?;
    Some(test_pg_pool_at(&url, 4).await)
}

/// A single-connection variant of [`test_pg_pool`], for a test that needs
/// every statement to land on the same pooled connection (for example, one
/// proving a cached prepared statement's parameter types are not reused
/// across a NULL and a non-NULL bind). `None` when
/// `CLAUDE_RUN_TEST_DATABASE_URL` is unset.
#[cfg(test)]
pub async fn test_pg_pool_single_connection() -> Option<DbPool> {
    let url = std::env::var("CLAUDE_RUN_TEST_DATABASE_URL").ok()?;
    Some(test_pg_pool_at(&url, 1).await)
}

/// Fails when `schema` already holds `_sqlx_migrations`: that means the
/// target is a database someone has already migrated (the live runner
/// database, or a stale leftover), not a disposable schema a test fixture
/// just created. Schema-qualified so the connection's `search_path` does not
/// matter.
#[cfg(test)]
async fn refuse_live_database(conn: &mut sqlx::AnyConnection, schema: &str) -> anyhow::Result<()> {
    let regclass = format!("{schema}._sqlx_migrations");
    let already_migrated: Option<String> = sqlx::query_scalar("SELECT to_regclass($1)::text")
        .bind(&regclass)
        .fetch_one(&mut *conn)
        .await?;
    if already_migrated.is_some() {
        let database: String = sqlx::query_scalar("SELECT current_database()::text")
            .fetch_one(&mut *conn)
            .await?;
        anyhow::bail!(
            "{database} already has `_sqlx_migrations`: this is a live runner database, not a \
             disposable test server. Point CLAUDE_RUN_TEST_DATABASE_URL at a disposable Postgres \
             instead."
        );
    }
    Ok(())
}

/// The Postgres migration set as one script, built once per test process.
/// A fixture runs it in a single transaction instead of replaying the
/// migrator per test. SPEC-5.6.2.
#[cfg(test)]
fn fixture_script() -> &'static str {
    static SCRIPT: std::sync::OnceLock<String> = std::sync::OnceLock::new();
    SCRIPT.get_or_init(|| {
        let mut script = String::new();
        for migration in POSTGRES_MIGRATOR.iter() {
            // A no-transaction migration cannot join the single transaction.
            assert!(
                !migration.no_tx,
                "postgres migration {} is no-transaction and cannot be part of the fixture script",
                migration.version
            );
            script.push_str(migration.sql.as_str());
            script.push('\n');
        }
        script
    })
}

/// A database name no other fixture in this process or on this server uses.
/// SPEC-5.6.1.
#[cfg(test)]
fn next_fixture_database() -> String {
    use std::sync::atomic::{AtomicU64, Ordering};
    static SEQ: AtomicU64 = AtomicU64::new(0);
    format!(
        "cr_test_{}_{}",
        std::process::id(),
        SEQ.fetch_add(1, Ordering::Relaxed)
    )
}

/// `url` with its database path replaced by `database`, so a fixture pool
/// connects to a disposable database of its own on the same server, port and
/// credentials as `CLAUDE_RUN_TEST_DATABASE_URL`. SPEC-5.6.1.
#[cfg(test)]
fn url_with_database(url: &str, database: &str) -> anyhow::Result<String> {
    let mut parsed = url::Url::parse(url)?;
    parsed.set_path(database);
    Ok(parsed.into())
}

/// Opens a short-lived connection to the maintenance database (the one `url`
/// names) on the current runtime, for one `CREATE DATABASE` / `DROP DATABASE`.
/// Every such statement gets its own fresh connection rather than a shared
/// pool: each `#[tokio::test]` runs on a runtime of its own, and an sqlx pool
/// (or connection) cached in a process-wide static breaks when reused from a
/// later test, because its connections are bound to the runtime that opened
/// them. A clone is a cheap catalog operation, so a connect per clone is
/// negligible. SPEC-5.6.1.
#[cfg(test)]
async fn connect_maintenance(url: &str) -> sqlx::AnyConnection {
    use sqlx::Connection;
    sqlx::any::install_default_drivers();
    sqlx::AnyConnection::connect(url)
        .await
        .unwrap_or_else(|e| panic!("connect to the test maintenance database: {e}"))
}

/// Builds the per-process template database once and returns its name: a
/// database migrated to the full end schema that every fixture then clones
/// with `CREATE DATABASE ... TEMPLATE`, a block copy far cheaper than
/// replaying the migration set per test (CLAUDE-540). Refuses a live runner
/// database here, once per process, before creating anything, by checking the
/// maintenance database's own `public._sqlx_migrations` (SPEC-5.6.6). The
/// `OnceCell` holds only the template's name (not a live connection), so the
/// name is safe to reuse from every test's own runtime. SPEC-5.6.2.
#[cfg(test)]
async fn ensure_template(url: &str) -> &'static str {
    use sqlx::Connection;
    static TEMPLATE: tokio::sync::OnceCell<String> = tokio::sync::OnceCell::const_new();
    TEMPLATE
        .get_or_init(|| async {
            let mut maint = connect_maintenance(url).await;
            refuse_live_database(&mut maint, "public")
                .await
                .expect("refuse a live runner database");
            let template = format!("cr_test_template_{}", std::process::id());
            // A leftover template from an earlier process on a reused server
            // would otherwise fail the CREATE, so drop it first. CREATE
            // DATABASE cannot run in a transaction, so these go one statement
            // at a time.
            for sql in [
                format!("DROP DATABASE IF EXISTS {template}"),
                format!("CREATE DATABASE {template}"),
            ] {
                maint
                    .execute(sqlx::AssertSqlSafe(sql))
                    .await
                    .unwrap_or_else(|e| panic!("prepare the test template database: {e}"));
            }
            maint
                .close()
                .await
                .unwrap_or_else(|e| panic!("close the maintenance connection: {e}"));
            // Migrate the template over a connection of its own, then close
            // it: CREATE DATABASE ... TEMPLATE refuses a template that still
            // has a session connected to it.
            let template_url = url_with_database(url, template.as_str())
                .unwrap_or_else(|e| panic!("derive the template database url: {e}"));
            let mut conn = sqlx::AnyConnection::connect(&template_url)
                .await
                .unwrap_or_else(|e| panic!("connect to the test template database: {e}"));
            conn.execute(sqlx::AssertSqlSafe(fixture_script().to_owned()))
                .await
                .unwrap_or_else(|e| panic!("migrate the test template database: {e}"));
            conn.close()
                .await
                .unwrap_or_else(|e| panic!("close the test template connection: {e}"));
            template
        })
        .await
        .as_str()
}

/// A pool connected to `database`, a disposable database of its own on the
/// test server (its default `public` schema holds the cloned tables, so no
/// `search_path` is needed). The database itself is not created here.
///
/// Carries the same raised `acquire_timeout` as [`test_sqlite_pool`]: a
/// capacity-constrained pool here (most acutely `max_connections(1)` via
/// [`test_pg_pool_single_connection`]) is exposed to the identical race, a
/// dropped transaction's background rollback losing to full-suite CPU
/// contention, just on the Postgres leg of CI instead of the SQLite one
/// (CLAUDE-524). A connect failure panics with the server's state attached
/// (SPEC-5.6.12), since it carries the same "what was the server doing"
/// question as a clone failure. SPEC-5.6.1.
#[cfg(test)]
async fn pool_on_database(url: &str, database: &str, max_connections: u32) -> DbPool {
    sqlx::any::install_default_drivers();
    let database_url = url_with_database(url, database)
        .unwrap_or_else(|e| panic!("derive the fixture database url: {e}"));
    let acquire_timeout = Duration::from_secs(120);
    let started = Instant::now();
    match AnyPoolOptions::new()
        .max_connections(max_connections)
        .acquire_timeout(acquire_timeout)
        .connect(&database_url)
        .await
    {
        Ok(pool) => pool,
        Err(e) => {
            let mapped = map_connect_error(
                "postgres test fixture",
                max_connections,
                acquire_timeout,
                started,
                e,
            );
            let ctx = FixtureStallContext {
                waited_for_clone_slot: Duration::ZERO,
                step: started.elapsed(),
            };
            let snapshot = fetch_server_snapshot(url).await;
            panic!(
                "{}",
                fixture_failure_message(&mapped.to_string(), ctx, snapshot)
            );
        }
    }
}

/// Whether `e` is Postgres reporting SQLSTATE `55006` (`object_in_use`): a
/// `CREATE DATABASE ... TEMPLATE` whose template is momentarily "being accessed
/// by other users" (concurrent clones from one template race this check), or a
/// `DROP DATABASE` losing the race with the pool's own connection teardown (the
/// pool fires its `CloseEvent` as it closes, and the server can still be
/// tearing the last backend down for a moment after). The message is matched
/// too, for when the `Any` driver does not surface the code.
#[cfg(test)]
fn is_database_in_use(e: &sqlx::Error) -> bool {
    if let sqlx::Error::Database(db) = e {
        if db.code().as_deref() == Some("55006") {
            return true;
        }
    }
    e.to_string().contains("being accessed by other users")
}

/// Runs `sql` (a `CREATE` or `DROP DATABASE`, neither of which can be
/// transactional) over `conn`, retrying every 25ms while the error is
/// [`is_database_in_use`] until `deadline` elapses. That `55006` is transient:
/// the conflicting clone or teardown finishes and the retry succeeds. No
/// `WITH (FORCE)`: an error that outlasts the deadline is a real problem worth
/// surfacing, not something to force past.
#[cfg(test)]
async fn run_database_statement_with_retry(
    conn: &mut sqlx::AnyConnection,
    sql: String,
    deadline: Duration,
) -> std::result::Result<(), sqlx::Error> {
    let started = Instant::now();
    loop {
        match conn.execute(sqlx::AssertSqlSafe(sql.clone())).await {
            Ok(_) => return Ok(()),
            Err(e) if is_database_in_use(&e) && started.elapsed() < deadline => {
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
            Err(e) => return Err(e),
        }
    }
}

/// Drops `database` once `pool` is dropped or closed. One worker thread per
/// process does all the dropping over a single connection to the maintenance
/// database, so cleanup adds one server connection however many tests run in
/// parallel, and runs on a runtime of its own because the test's runtime is
/// gone by then. `DROP DATABASE` names one database and cannot run in a
/// transaction, so a backlog is drained and dropped one statement at a time
/// (unlike the one combined `DROP SCHEMA` this replaced). Before each drop it
/// terminates any backend still attached to the database (a loaded durable
/// server can lag reaping the pool's own closed connections, which would make
/// the drop hit `55006`). A failure goes to stderr naming the database, which
/// is then leaked (harmless: the whole server is a throwaway container
/// discarded on teardown) without stranding the rest of the batch.
/// SPEC-5.6.4, SPEC-5.6.5.
#[cfg(test)]
fn drop_database_when_closed(pool: &DbPool, url: &str, database: &str) {
    use futures_util::stream::{FuturesUnordered, StreamExt};
    use futures_util::FutureExt;
    use sqlx::pool::CloseEvent;
    use sqlx::Connection;
    use std::sync::OnceLock;
    use tokio::sync::mpsc::{unbounded_channel, UnboundedSender};

    type Job = (CloseEvent, String, String);
    static WORKER: OnceLock<UnboundedSender<Job>> = OnceLock::new();

    let sender = WORKER.get_or_init(|| {
        let (sender, mut jobs) = unbounded_channel::<Job>();
        std::thread::Builder::new()
            .name("test-db-cleanup".to_owned())
            .spawn(move || {
                let runtime = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .expect("build the test database cleanup runtime");
                runtime.block_on(async move {
                    let mut waiting = FuturesUnordered::new();
                    let mut conn: Option<sqlx::AnyConnection> = None;
                    loop {
                        tokio::select! {
                            Some((closed, url, database)) = jobs.recv() => {
                                waiting.push(async move {
                                    closed.await;
                                    (url, database)
                                });
                            }
                            Some((url, database)) = waiting.next(), if !waiting.is_empty() => {
                                // Drain every pool already closed and waiting, so a backlog
                                // is cleared in one wake rather than one select turn each.
                                let mut databases = vec![database];
                                while databases.len() < 16 {
                                    match waiting.next().now_or_never() {
                                        Some(Some((_, more))) => databases.push(more),
                                        _ => break,
                                    }
                                }
                                if conn.is_none() {
                                    match sqlx::AnyConnection::connect(&url).await {
                                        Ok(c) => conn = Some(c),
                                        Err(e) => {
                                            eprintln!("test databases {databases:?} not dropped: {e}");
                                            continue;
                                        }
                                    }
                                }
                                let mut reconnect_after = false;
                                {
                                    let conn = conn.as_mut().expect("connected above");
                                    for database in &databases {
                                        // Terminate any backend still attached to this
                                        // disposable database before dropping it. The pool
                                        // fired close_event, but on a loaded durable server
                                        // the server can lag reaping the last backends, which
                                        // makes DROP DATABASE hit 55006 ("being accessed by
                                        // other users") and burn the retry window per drop,
                                        // starving the single-connection worker (CLAUDE-540,
                                        // run #751/#752). pg_terminate_backend is the
                                        // sanctioned way to drop a test database promptly; no
                                        // DROP DATABASE ... (FORCE). Best effort: ignore its
                                        // result, the retry below still covers a residual race.
                                        let terminate = format!(
                                            "SELECT pg_terminate_backend(pid) FROM pg_stat_activity \
                                             WHERE datname = '{database}' AND pid <> pg_backend_pid()"
                                        );
                                        let _ = conn.execute(sqlx::AssertSqlSafe(terminate)).await;
                                        let sql = format!("DROP DATABASE IF EXISTS {database}");
                                        match run_database_statement_with_retry(
                                            conn,
                                            sql,
                                            Duration::from_secs(10),
                                        )
                                        .await
                                        {
                                            Ok(()) => {}
                                            // Leave this one leaked (harmless on a throwaway
                                            // server) but keep draining the rest of the batch,
                                            // so one bad drop never strands its siblings. A
                                            // broken connection is replaced after the batch.
                                            Err(e) => {
                                                eprintln!("test database {database} not dropped: {e}");
                                                reconnect_after = true;
                                            }
                                        }
                                    }
                                }
                                if reconnect_after {
                                    conn = None;
                                }
                            }
                        }
                    }
                });
            })
            .expect("spawn the test database cleanup thread");
        sender
    });
    sender
        .send((pool.close_event(), url.to_owned(), database.to_owned()))
        .expect("the test database cleanup worker is running");
}

/// Guards the `CREATE DATABASE ... TEMPLATE` step in [`test_pg_pool_at`] so
/// only one clone runs at a time in this process. Postgres already refuses a
/// second concurrent clone from the same template with `55006`
/// ("being accessed by other users"), so retrying through that refusal (what
/// [`run_database_statement_with_retry`] still does for a cross-process
/// collision) is racing a lock this process could just take. The lock is held
/// only for the clone statement itself and released before
/// [`pool_on_database`] connects. SPEC-5.6.11.
#[cfg(test)]
fn clone_lock() -> &'static tokio::sync::Mutex<()> {
    static LOCK: std::sync::OnceLock<tokio::sync::Mutex<()>> = std::sync::OnceLock::new();
    LOCK.get_or_init(|| tokio::sync::Mutex::new(()))
}

/// How long a fixture waited for [`clone_lock`] and how long the step that
/// then failed actually ran, carried into [`fixture_failure_message`] so a
/// stall panic says which part of the sequence was slow. SPEC-5.6.12.
#[cfg(test)]
#[derive(Debug, Clone, Copy)]
struct FixtureStallContext {
    waited_for_clone_slot: Duration,
    step: Duration,
}

/// One row of `pg_stat_activity`'s backend count, grouped by database and
/// state. Either can be `NULL` on the server (a backend not yet attached to a
/// database, or between states), hence `Option`.
#[cfg(test)]
type BackendCount = (Option<String>, Option<String>, i64);

/// A snapshot of what the Postgres server was doing, taken over a fresh
/// maintenance connection when a fixture fails to clone or connect, so the
/// panic says what the server was doing instead of just that the fixture
/// timed out. SPEC-5.6.12.
#[cfg(test)]
struct ServerSnapshot {
    max_connections: String,
    backends: Vec<BackendCount>,
}

#[cfg(test)]
impl ServerSnapshot {
    fn render(&self) -> String {
        let mut rendered = format!("max_connections={}", self.max_connections);
        for (datname, state, count) in &self.backends {
            let datname = datname.as_deref().unwrap_or("<none>");
            let state = state.as_deref().unwrap_or("<none>");
            rendered.push_str(&format!("; {datname}/{state}={count}"));
        }
        rendered
    }
}

/// Fetches [`ServerSnapshot`] over a fresh connection to the maintenance
/// database named by `url`. Never panics: a failure here is appended to the
/// original fixture failure by [`fixture_failure_message`] rather than
/// replacing it, so a snapshot query that itself fails to connect or query
/// still surfaces as text in the panic message. SPEC-5.6.12.
#[cfg(test)]
async fn fetch_server_snapshot(url: &str) -> std::result::Result<ServerSnapshot, String> {
    use sqlx::Connection;
    let mut conn = sqlx::AnyConnection::connect(url)
        .await
        .map_err(|e| format!("connect for the server snapshot: {e}"))?;
    let max_connections: String =
        sqlx::query_scalar("SELECT setting FROM pg_settings WHERE name = 'max_connections'")
            .fetch_one(&mut conn)
            .await
            .map_err(|e| format!("read max_connections for the server snapshot: {e}"))?;
    let backends: Vec<BackendCount> = sqlx::query_as(
        "SELECT datname, state, COUNT(*) FROM pg_stat_activity GROUP BY datname, state \
         ORDER BY datname, state",
    )
    .fetch_all(&mut conn)
    .await
    .map_err(|e| format!("read pg_stat_activity for the server snapshot: {e}"))?;
    let _ = conn.close().await;
    Ok(ServerSnapshot {
        max_connections,
        backends,
    })
}

/// Builds the panic message for a fixture clone or connect failure:
/// `original_error` (never dropped, only ever appended to) plus how long the
/// fixture waited for the clone slot, how long the failed step ran, and the
/// server snapshot, or the snapshot query's own error when that failed too.
/// SPEC-5.6.12.
#[cfg(test)]
fn fixture_failure_message(
    original_error: &str,
    ctx: FixtureStallContext,
    snapshot: std::result::Result<ServerSnapshot, String>,
) -> String {
    let snapshot = match snapshot {
        Ok(s) => s.render(),
        Err(e) => format!("server snapshot failed: {e}"),
    };
    format!(
        "{original_error} (waited {:?} for the clone slot, failed step ran {:?}); server state: {snapshot}",
        ctx.waited_for_clone_slot, ctx.step
    )
}

/// A Postgres pool backed by a disposable database of its own, cloned from the
/// per-process template (SPEC-5.6.2) so `cargo test`'s parallel tests neither
/// share tables nor each replay the migration set. The database is created
/// with `CREATE DATABASE ... TEMPLATE` (a block copy on one maintenance
/// connection, which cannot run in a transaction) and dropped when the pool
/// is. The clone itself runs under [`clone_lock`] so this process's own
/// clones never race each other for the template (SPEC-5.6.11); a failure
/// panics with the server's state attached (SPEC-5.6.12). SPEC-5.6.1,
/// SPEC-5.6.3.
#[cfg(test)]
async fn test_pg_pool_at(url: &str, max_connections: u32) -> DbPool {
    use sqlx::Connection;
    let template = ensure_template(url).await;
    let database = next_fixture_database();
    let mut maint = connect_maintenance(url).await;

    let wait_started = Instant::now();
    let guard = clone_lock().lock().await;
    let waited_for_clone_slot = wait_started.elapsed();
    let step_started = Instant::now();
    let clone_result = run_database_statement_with_retry(
        &mut maint,
        format!("CREATE DATABASE {database} TEMPLATE {template}"),
        Duration::from_secs(60),
    )
    .await;
    // Released before pool_on_database connects: the lock only needs to cover
    // the clone statement itself. SPEC-5.6.11.
    drop(guard);

    if let Err(e) = clone_result {
        let ctx = FixtureStallContext {
            waited_for_clone_slot,
            step: step_started.elapsed(),
        };
        let snapshot = fetch_server_snapshot(url).await;
        panic!(
            "{}",
            fixture_failure_message(
                &format!("clone the test fixture database {database}: {e}"),
                ctx,
                snapshot
            )
        );
    }

    maint
        .close()
        .await
        .unwrap_or_else(|e| panic!("close the maintenance connection: {e}"));
    let pool = pool_on_database(url, &database, max_connections).await;
    drop_database_when_closed(&pool, url, &database);
    pool
}

/// Print-and-skip for a Postgres test with no server configured, so a skipped
/// test says so instead of passing vacuously.
#[cfg(test)]
#[macro_export]
macro_rules! skip_without_postgres {
    () => {
        match $crate::db::test_pg_pool().await {
            Some(pool) => pool,
            None => {
                eprintln!(
                    "SKIP {}: CLAUDE_RUN_TEST_DATABASE_URL is unset, no postgres to test against",
                    concat!(module_path!(), "::", line!())
                );
                return;
            }
        }
    };
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pg(sql: &str) -> String {
        rewrite(Dialect::Postgres, sql).into_owned()
    }

    fn lite(sql: &str) -> String {
        rewrite(Dialect::Sqlite, sql).into_owned()
    }

    #[test]
    fn sqlite_is_passed_through_untouched() {
        let sql = "SELECT lower(a) FROM t WHERE b = ?1 AND c = ?";
        assert!(matches!(rewrite(Dialect::Sqlite, sql), Cow::Borrowed(_)));
        assert_eq!(rewrite(Dialect::Sqlite, sql), sql);
    }

    #[test]
    fn positional_placeholders_are_numbered_in_order() {
        assert_eq!(
            pg("INSERT INTO t (a, b, c) VALUES (?, ?, ?)"),
            "INSERT INTO t (a, b, c) VALUES ($1, $2, $3)"
        );
    }

    #[test]
    fn numbered_placeholders_keep_their_number() {
        assert_eq!(
            pg("SELECT a FROM t WHERE x = ?1 AND y = ?2 AND z = ?2 LIMIT ?3"),
            "SELECT a FROM t WHERE x = $1 AND y = $2 AND z = $2 LIMIT $3"
        );
    }

    #[test]
    fn a_question_mark_inside_a_string_literal_is_left_alone() {
        assert_eq!(
            pg("SELECT ? FROM t WHERE msg = 'why? because' AND b = ?"),
            "SELECT $1 FROM t WHERE msg = 'why? because' AND b = $2"
        );
    }

    #[test]
    fn a_doubled_quote_does_not_end_the_literal() {
        assert_eq!(
            pg("SELECT ? WHERE m = 'it''s ? here' AND n = ?"),
            "SELECT $1 WHERE m = 'it''s ? here' AND n = $2"
        );
    }

    #[test]
    fn a_question_mark_inside_a_quoted_identifier_is_left_alone() {
        assert_eq!(
            pg(r#"SELECT "we?ird" FROM t WHERE a = ?"#),
            r#"SELECT "we?ird" FROM t WHERE a = $1"#
        );
    }

    #[test]
    fn a_question_mark_inside_a_comment_is_left_alone() {
        assert_eq!(
            pg("SELECT a -- why? not\nFROM t WHERE b = ?"),
            "SELECT a -- why? not\nFROM t WHERE b = $1"
        );
        assert_eq!(
            pg("SELECT a /* why? not */ FROM t WHERE b = ?"),
            "SELECT a /* why? not */ FROM t WHERE b = $1"
        );
    }

    #[test]
    fn strpos_keeps_its_spelling_on_postgres_and_becomes_instr_on_sqlite() {
        assert_eq!(
            pg("WHERE strpos(lower(summary), ?) > 0"),
            "WHERE strpos(lower(summary), $1) > 0"
        );
        assert_eq!(
            lite("WHERE strpos(lower(summary), ?) > 0"),
            "WHERE instr(lower(summary), ?) > 0"
        );
    }

    #[test]
    fn strpos_inside_a_literal_is_not_a_call() {
        assert_eq!(
            pg("SELECT 'strpos(x, y)' FROM t WHERE a = ?"),
            "SELECT 'strpos(x, y)' FROM t WHERE a = $1"
        );
        assert_eq!(
            lite("SELECT 'strpos(x, y)' FROM t WHERE a = ?"),
            "SELECT 'strpos(x, y)' FROM t WHERE a = ?"
        );
    }

    #[test]
    fn an_identifier_merely_containing_a_neutral_name_is_left_alone() {
        assert_eq!(
            lite("SELECT strpos_count FROM t"),
            "SELECT strpos_count FROM t"
        );
        assert_eq!(
            pg("SELECT strpos_count FROM t"),
            "SELECT strpos_count FROM t"
        );
    }

    #[test]
    fn elapsed_seconds_becomes_each_backends_own_span() {
        assert_eq!(
            lite("ORDER BY elapsed_seconds(started_at, ended_at) ASC"),
            "ORDER BY ((julianday(ended_at) - julianday(started_at)) * 86400.0) ASC"
        );
        assert_eq!(
            pg("ORDER BY elapsed_seconds(started_at, ended_at) ASC"),
            "ORDER BY extract(epoch from ((ended_at)::timestamptz \
             - (started_at)::timestamptz)) ASC"
        );
    }

    #[test]
    fn a_placeholder_inside_a_rewritten_call_keeps_the_statements_numbering() {
        assert_eq!(
            pg("SELECT a FROM t WHERE b = ? AND strpos(lower(c), ?) > 0 AND d = ?"),
            "SELECT a FROM t WHERE b = $1 AND strpos(lower(c), $2) > 0 AND d = $3"
        );
    }

    #[test]
    fn scheme_selects_the_dialect_and_anything_else_is_an_error() {
        assert_eq!(Dialect::from_scheme("sqlite").unwrap(), Dialect::Sqlite);
        assert_eq!(Dialect::from_scheme("postgres").unwrap(), Dialect::Postgres);
        assert_eq!(
            Dialect::from_scheme("postgresql").unwrap(),
            Dialect::Postgres
        );
        let err = Dialect::from_scheme("mysql").unwrap_err().to_string();
        assert!(err.contains("mysql"), "{err}");
    }

    #[tokio::test]
    async fn an_open_sqlite_pool_reports_the_sqlite_dialect() {
        let pool = test_sqlite_pool().await;
        assert_eq!(dialect(&pool), Dialect::Sqlite);
        assert_eq!(q(&pool, "SELECT ?"), "SELECT ?");
    }

    #[tokio::test]
    async fn an_open_postgres_pool_reports_the_postgres_dialect() {
        let pool = skip_without_postgres!();
        assert_eq!(dialect(&pool), Dialect::Postgres);
        assert_eq!(
            q(&pool, "SELECT a FROM t WHERE b = ?"),
            "SELECT a FROM t WHERE b = $1"
        );
    }

    /// The Postgres migration is a hand-written squash of the SQLite chain, so
    /// nothing but a comparison against the chain's end state keeps the two
    /// from drifting apart.
    ///
    /// The four catalog reads below are the only statements in the tree that do
    /// NOT go through [`q`], deliberately: reading a schema is the one thing the
    /// two backends have no common spelling for, and they carry no parameters.
    #[tokio::test]
    async fn the_two_migration_sets_end_on_the_same_schema() {
        let pg = skip_without_postgres!();
        let lite = test_sqlite_pool().await;

        let lite_columns: Vec<(String, String)> = sqlx::query_as(
            "SELECT m.name, p.name FROM sqlite_master m JOIN pragma_table_info(m.name) p \
             WHERE m.type = 'table' AND m.name NOT LIKE 'sqlite_%' AND m.name <> '_sqlx_migrations' \
             ORDER BY 1, 2",
        )
        .fetch_all(&lite)
        .await
        .unwrap();
        let pg_columns: Vec<(String, String)> = sqlx::query_as(
            // Cast off `information_schema`'s `name` domain: the `Any` driver
            // maps only the plain scalar types.
            "SELECT table_name::text, column_name::text FROM information_schema.columns \
             WHERE table_schema = current_schema() AND table_name <> '_sqlx_migrations' \
             ORDER BY 1, 2",
        )
        .fetch_all(&pg)
        .await
        .unwrap();
        assert_eq!(pg_columns, lite_columns);

        let lite_indexes: Vec<String> = sqlx::query_scalar(
            "SELECT name FROM sqlite_master WHERE type = 'index' AND name LIKE 'idx_%' ORDER BY 1",
        )
        .fetch_all(&lite)
        .await
        .unwrap();
        // Only the explicitly named indexes: Postgres also materialises a
        // `<table>_pkey` per primary key, which SQLite keeps out of the catalog.
        let pg_indexes: Vec<String> = sqlx::query_scalar(
            "SELECT indexname::text FROM pg_indexes \
             WHERE schemaname = current_schema() AND indexname LIKE 'idx_%' ORDER BY 1",
        )
        .fetch_all(&pg)
        .await
        .unwrap();
        assert_eq!(pg_indexes, lite_indexes);
    }

    /// SPEC-5.6.3, SPEC-5.6.4: a fixture database holds the tables but no
    /// migration bookkeeping (the template was built by the joined script, not
    /// the migrator), and the drop path removes it. The automatic
    /// drop-on-pool-close is the background cleanup worker's job, which every
    /// other fixture in the suite exercises; here the drop is driven directly
    /// (the worker's own terminate-then-drop sequence) so the assertion is
    /// deterministic rather than racing that single-connection worker under
    /// full-suite load on a contended runner (CLAUDE-540).
    #[tokio::test]
    async fn a_fixture_database_has_no_bookkeeping_and_its_drop_path_removes_it() {
        use sqlx::Connection;
        let Ok(url) = std::env::var("CLAUDE_RUN_TEST_DATABASE_URL") else {
            eprintln!(
                "SKIP {}: CLAUDE_RUN_TEST_DATABASE_URL is unset, no postgres to test against",
                concat!(
                    module_path!(),
                    "::a_fixture_database_has_no_bookkeeping_and_its_drop_path_removes_it"
                )
            );
            return;
        };

        // Clone a fixture database of our own from the template, but do not
        // register the background drop worker: this test owns the drop so it
        // can prove the path deterministically.
        let template = ensure_template(&url).await;
        let database = next_fixture_database();
        let mut maint = connect_maintenance(&url).await;
        run_database_statement_with_retry(
            &mut maint,
            format!("CREATE DATABASE {database} TEMPLATE {template}"),
            Duration::from_secs(60),
        )
        .await
        .unwrap();

        let pool = pool_on_database(&url, &database, 2).await;
        let tables: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM information_schema.tables \
             WHERE table_schema = current_schema() AND table_name = 'settings'",
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(
            tables, 1,
            "the template clone carried the schema into {database}"
        );
        let bookkeeping: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM information_schema.tables \
             WHERE table_schema = current_schema() AND table_name = '_sqlx_migrations'",
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(bookkeeping, 0, "no bookkeeping rows in a fixture database");

        // Fully close the pool (await all its connections closed), then drive
        // the drop exactly as the worker does: terminate any residual backend
        // on the database, then DROP DATABASE over the maintenance connection.
        pool.close().await;
        let terminate = format!(
            "SELECT pg_terminate_backend(pid) FROM pg_stat_activity \
             WHERE datname = '{database}' AND pid <> pg_backend_pid()"
        );
        let _ = maint.execute(sqlx::AssertSqlSafe(terminate)).await;
        run_database_statement_with_retry(
            &mut maint,
            format!("DROP DATABASE IF EXISTS {database}"),
            Duration::from_secs(60),
        )
        .await
        .unwrap();

        let remaining: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM pg_database WHERE datname = $1")
                .bind(&database)
                .fetch_one(&mut maint)
                .await
                .unwrap();
        assert_eq!(remaining, 0, "the drop path did not remove {database}");
        maint.close().await.unwrap();
    }

    /// SPEC-5.6.7: fixtures clone a template built from the joined script, so
    /// this keeps the production migrator path (and the checksums it records)
    /// exercised, in a fresh database of its own.
    #[tokio::test]
    async fn the_production_migrator_applies_every_postgres_migration() {
        use sqlx::Connection;
        let _ = skip_without_postgres!();
        let url = std::env::var("CLAUDE_RUN_TEST_DATABASE_URL").unwrap();
        let database = next_fixture_database();
        let mut maint = connect_maintenance(&url).await;
        maint
            .execute(sqlx::AssertSqlSafe(format!("CREATE DATABASE {database}")))
            .await
            .unwrap();
        maint.close().await.unwrap();
        let pool = pool_on_database(&url, &database, 2).await;
        drop_database_when_closed(&pool, &url, &database);
        POSTGRES_MIGRATOR.run(&pool).await.unwrap();
        let applied: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM _sqlx_migrations")
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(applied as usize, POSTGRES_MIGRATOR.iter().count());
    }

    #[tokio::test]
    async fn refuse_live_database_flags_a_schema_that_already_has_sqlx_migrations() {
        let pool = skip_without_postgres!();
        let migrated_schema = format!("cr_test_guard_migrated_{}", std::process::id());
        pool.execute(sqlx::AssertSqlSafe(format!(
            "CREATE SCHEMA {migrated_schema}"
        )))
        .await
        .unwrap();
        pool.execute(sqlx::AssertSqlSafe(format!(
            "CREATE TABLE {migrated_schema}._sqlx_migrations (version bigint)"
        )))
        .await
        .unwrap();

        let mut conn = pool.acquire().await.unwrap();
        let err = refuse_live_database(&mut conn, &migrated_schema)
            .await
            .expect_err("a schema with _sqlx_migrations must be refused");
        let database: String = sqlx::query_scalar("SELECT current_database()::text")
            .fetch_one(&pool)
            .await
            .unwrap();
        assert!(err.to_string().contains(&database), "{err}");
        pool.execute(sqlx::AssertSqlSafe(format!(
            "DROP SCHEMA {migrated_schema} CASCADE"
        )))
        .await
        .unwrap();

        let empty_schema = format!("cr_test_guard_empty_{}", std::process::id());
        pool.execute(sqlx::AssertSqlSafe(format!("CREATE SCHEMA {empty_schema}")))
            .await
            .unwrap();
        refuse_live_database(&mut conn, &empty_schema)
            .await
            .expect("a schema without _sqlx_migrations is not refused");
        pool.execute(sqlx::AssertSqlSafe(format!(
            "DROP SCHEMA {empty_schema} CASCADE"
        )))
        .await
        .unwrap();
    }

    /// Every `.rs` under `src/` that uses a Postgres fixture entrypoint, as a
    /// `::`-terminated module path (`web/webhooks.rs` -> `web::webhooks::`).
    #[cfg(test)]
    fn db_touching_module_prefixes() -> Vec<String> {
        let src = Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
        let needles = [
            "test_pool(",
            "test_pg_pool(",
            "test_pg_pool_single_connection(",
            "skip_without_postgres!",
        ];
        let mut stack = vec![src.clone()];
        let mut prefixes = Vec::new();
        while let Some(dir) = stack.pop() {
            for entry in std::fs::read_dir(&dir).unwrap() {
                let path = entry.unwrap().path();
                if path.is_dir() {
                    stack.push(path);
                    continue;
                }
                if path.extension().and_then(|e| e.to_str()) != Some("rs") {
                    continue;
                }
                let text = std::fs::read_to_string(&path).unwrap();
                if !needles.iter().any(|n| text.contains(n)) {
                    continue;
                }
                let rel = path.strip_prefix(&src).unwrap();
                let mut parts: Vec<String> = rel
                    .components()
                    .map(|c| c.as_os_str().to_string_lossy().into_owned())
                    .collect();
                let last = parts.pop().unwrap();
                let stem = last.trim_end_matches(".rs");
                if !matches!(stem, "mod" | "lib" | "main") {
                    parts.push(stem.to_string());
                }
                if !parts.is_empty() {
                    prefixes.push(format!("{}::", parts.join("::")));
                }
            }
        }
        prefixes
    }

    /// CLAUDE-540: the Postgres CI leg re-runs only the test files that touch a
    /// DB pool (`scripts/postgres-test-filters.txt`), so pure-logic tests are
    /// not re-run on Postgres. This fails if a source file gains a DB test (a
    /// call to one of the fixture entrypoints) that no filter in that file
    /// covers, which would silently drop it from the both-backends guarantee
    /// (SPEC-5.6).
    #[test]
    fn every_db_touching_file_is_in_the_postgres_test_filter() {
        let path = Path::new(env!("CARGO_MANIFEST_DIR")).join("scripts/postgres-test-filters.txt");
        let text = std::fs::read_to_string(&path).expect("read scripts/postgres-test-filters.txt");
        let filters: Vec<&str> = text
            .lines()
            .map(str::trim)
            .filter(|l| !l.is_empty() && !l.starts_with('#'))
            .collect();

        let uncovered: Vec<String> = db_touching_module_prefixes()
            .into_iter()
            .filter(|module| !filters.iter().any(|f| module.starts_with(f)))
            .collect();
        assert!(
            uncovered.is_empty(),
            "these modules have Postgres-backed tests but no matching line in \
             scripts/postgres-test-filters.txt, so the Postgres CI leg would skip them: {uncovered:?}"
        );

        // Every filter must still match a real DB module, so a file that stops
        // touching the DB does not leave a dead over-broad filter behind.
        let modules = db_touching_module_prefixes();
        let unused: Vec<&str> = filters
            .iter()
            .copied()
            .filter(|f| !modules.iter().any(|m| m.starts_with(f)))
            .collect();
        assert!(
            unused.is_empty(),
            "these lines in scripts/postgres-test-filters.txt match no DB-touching module: {unused:?}"
        );
    }

    /// CLAUDE-436 regression: a NULL utilization bind declares the `Any`
    /// driver's cached-statement parameter type `float4`; a later non-NULL
    /// bind on the SAME connection must not reuse that cached Parse, or
    /// Postgres rejects the 8-byte `float8` value with 22P03. A
    /// `max_connections(1)` pool forces both inserts through the one
    /// connection, so this fails on the unpatched `CAST(? AS DOUBLE
    /// PRECISION)` statement (confirmed: 22P03) and passes once the
    /// production insert in `usage::try_record_invocation` uses
    /// `.persistent(false)` (confirmed: both rows persist).
    #[tokio::test]
    async fn claude_invocations_null_then_valued_utilization_same_connection() {
        let pool = match test_pg_pool_single_connection().await {
            Some(pool) => pool,
            None => {
                eprintln!(
                    "SKIP {}: CLAUDE_RUN_TEST_DATABASE_URL is unset, no postgres to test against",
                    concat!(module_path!(), "::", line!())
                );
                return;
            }
        };
        let stream_log = Path::new("/logs/act/streams/K-attempt-1.jsonl");

        let null_result = crate::claude::ClaudeResult::synthesized("", "no rate-limit reading");
        crate::usage::try_record_invocation(
            &pool,
            "youtrack",
            None,
            None,
            stream_log,
            &null_result,
        )
        .await
        .expect("NULL-utilization insert");

        let mut valued_result =
            crate::claude::ClaudeResult::synthesized("", "carried a rate-limit reading");
        valued_result.usage.rate_limit = Some(crate::usage::RateLimitSnapshot {
            status: Some("allowed".to_string()),
            rate_limit_type: Some("five_hour".to_string()),
            five_hour_utilization: Some(0.42),
            seven_day_utilization: Some(0.10),
            max_five_hour_utilization: Some(0.42),
            max_seven_day_utilization: Some(0.10),
            ..Default::default()
        });
        crate::usage::try_record_invocation(
            &pool,
            "youtrack",
            None,
            None,
            stream_log,
            &valued_result,
        )
        .await
        .expect("valued-utilization insert on the same connection must not hit 22P03");

        let count: i64 = sqlx::query_scalar(sqlx::AssertSqlSafe(
            q(&pool, "SELECT COUNT(*) FROM claude_invocations").into_owned(),
        ))
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(count, 2);
    }

    /// CLAUDE-529: forces a real `sqlx::Error::PoolTimedOut` out of a
    /// one-connection pool (hold its only connection, then acquire again
    /// under a short timeout) and asserts the mapped message names the pool,
    /// the configured timeout, and the pool's live size and idle count,
    /// instead of the bare `Sqlx(PoolTimedOut)` this fails on before the fix.
    #[tokio::test]
    async fn pool_acquire_timeout_is_reported_with_pool_state_and_wait_time_on_sqlite() {
        sqlx::any::install_default_drivers();
        let acquire_timeout = Duration::from_millis(50);
        let pool = AnyPoolOptions::new()
            .max_connections(1)
            .acquire_timeout(acquire_timeout)
            .connect("sqlite::memory:")
            .await
            .expect("open in-memory sqlite");
        let held = pool.acquire().await.expect("acquire the only connection");

        let started = Instant::now();
        let err = pool
            .acquire()
            .await
            .expect_err("a second acquire on a one-connection pool must time out");
        assert!(matches!(err, sqlx::Error::PoolTimedOut), "{err}");
        let reported = map_live_pool_error(&pool, "sqlite test", started, err);

        let message = reported.to_string();
        assert!(message.contains("pool `sqlite test`"), "{message}");
        assert!(message.contains("size 1"), "{message}");
        assert!(message.contains("idle 0"), "{message}");
        assert!(
            matches!(reported, Error::PoolAcquireTimeout { .. }),
            "{message}"
        );

        drop(held);
    }

    /// Same as the SQLite test above, run against Postgres so the fix is
    /// proven on both backends per the acceptance criteria. Unlike the
    /// SQLite variant, `connect()` here opens a real network connection, so
    /// the pool itself is built with the same 120s `acquire_timeout` as
    /// `test_sqlite_pool` and `pool_on_database` (CLAUDE-524): under
    /// full-suite CPU/connection contention that connect can take well
    /// over the 50ms this test needs to deliberately force the *second*
    /// acquire to time out, which was flaking CI (connect itself tripping
    /// `PoolTimedOut` before the test even got to the race it means to
    /// assert on). The short deadline is applied only to that second
    /// acquire, via `tokio::time::timeout`, independent of the pool's own
    /// configured timeout.
    #[tokio::test]
    async fn pool_acquire_timeout_is_reported_with_pool_state_and_wait_time_on_postgres() {
        let Ok(url) = std::env::var("CLAUDE_RUN_TEST_DATABASE_URL") else {
            eprintln!(
                "SKIP {}: CLAUDE_RUN_TEST_DATABASE_URL is unset, no postgres to test against",
                concat!(
                    module_path!(),
                    "::pool_acquire_timeout_is_reported_with_pool_state_and_wait_time_on_postgres"
                )
            );
            return;
        };
        sqlx::any::install_default_drivers();
        let acquire_timeout = Duration::from_millis(50);
        let pool = AnyPoolOptions::new()
            .max_connections(1)
            .acquire_timeout(Duration::from_secs(120))
            .connect(&url)
            .await
            .expect("connect to CLAUDE_RUN_TEST_DATABASE_URL");
        let held = pool.acquire().await.expect("acquire the only connection");

        let started = Instant::now();
        let err = match tokio::time::timeout(acquire_timeout, pool.acquire()).await {
            Ok(result) => {
                result.expect_err("a second acquire on a one-connection pool must time out")
            }
            Err(_) => sqlx::Error::PoolTimedOut,
        };
        assert!(matches!(err, sqlx::Error::PoolTimedOut), "{err}");
        let reported = map_live_pool_error(&pool, "postgres test", started, err);

        let message = reported.to_string();
        assert!(message.contains("pool `postgres test`"), "{message}");
        assert!(message.contains("size 1"), "{message}");
        assert!(message.contains("idle 0"), "{message}");
        assert!(
            matches!(reported, Error::PoolAcquireTimeout { .. }),
            "{message}"
        );

        drop(held);
    }

    /// `map_connect_error` has no live pool to read on a connect failure, so
    /// it reports the configured capacity and an idle count of 0 instead.
    #[test]
    fn connect_timeout_is_reported_with_configured_capacity() {
        let reported = map_connect_error(
            "sqlite",
            16,
            Duration::from_secs(30),
            Instant::now(),
            sqlx::Error::PoolTimedOut,
        );
        let message = reported.to_string();
        assert!(message.contains("pool `sqlite`"), "{message}");
        assert!(message.contains("size 16"), "{message}");
        assert!(message.contains("idle 0"), "{message}");
    }

    /// A non-timeout error must pass through unchanged rather than being
    /// misreported as a pool acquire timeout.
    #[test]
    fn a_non_timeout_sqlx_error_is_not_reported_as_a_pool_timeout() {
        let reported = map_connect_error(
            "sqlite",
            16,
            Duration::from_secs(30),
            Instant::now(),
            sqlx::Error::RowNotFound,
        );
        assert!(!matches!(reported, Error::PoolAcquireTimeout { .. }));
    }

    /// SPEC-5.6.11: two concurrent acquirers of [`clone_lock`] must never both
    /// hold it at once. Each task increments a shared counter on acquire and
    /// decrements it on release; a `Mutex::lock` guarantees mutual exclusion,
    /// so the counter must never be observed above 1. Against code that skips
    /// the lock (both tasks running the critical section unguarded) this same
    /// assertion fails under load, which is how this test would have caught
    /// an unserialized clone step.
    #[tokio::test]
    async fn clone_lock_never_lets_two_holders_overlap() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        use std::sync::Arc;

        let holders = Arc::new(AtomicUsize::new(0));
        let max_observed = Arc::new(AtomicUsize::new(0));

        let mut tasks = Vec::new();
        for _ in 0..8 {
            let holders = Arc::clone(&holders);
            let max_observed = Arc::clone(&max_observed);
            tasks.push(tokio::spawn(async move {
                for _ in 0..20 {
                    let _guard = clone_lock().lock().await;
                    let now_holding = holders.fetch_add(1, Ordering::SeqCst) + 1;
                    max_observed.fetch_max(now_holding, Ordering::SeqCst);
                    tokio::task::yield_now().await;
                    holders.fetch_sub(1, Ordering::SeqCst);
                }
            }));
        }
        for task in tasks {
            task.await.expect("clone lock task panicked");
        }

        assert_eq!(
            max_observed.load(Ordering::SeqCst),
            1,
            "two tasks held clone_lock at the same time"
        );
    }

    /// SPEC-5.6.12: the fixture failure message must carry the server's
    /// `max_connections`, the `pg_stat_activity` backend counts by database
    /// and state, the wait for the clone slot, and the duration of the failed
    /// step, built here from fixed rows rather than a live server. Before the
    /// snapshot was wired in, the panic message was just the bare clone/connect
    /// error with none of these fields, so this assertion set fails against
    /// that message and passes once the fields are appended.
    #[test]
    fn fixture_failure_message_includes_every_snapshot_field() {
        let ctx = FixtureStallContext {
            waited_for_clone_slot: Duration::from_millis(1234),
            step: Duration::from_secs(60),
        };
        let snapshot = ServerSnapshot {
            max_connections: "100".to_string(),
            backends: vec![
                (
                    Some("cr_test_template_42".to_string()),
                    Some("active".to_string()),
                    3,
                ),
                (None, None, 1),
            ],
        };
        let message = fixture_failure_message(
            "clone the test fixture database cr_test_42_7: boom",
            ctx,
            Ok(snapshot),
        );

        assert!(message.contains("boom"), "{message}");
        assert!(message.contains("max_connections=100"), "{message}");
        assert!(
            message.contains("cr_test_template_42/active=3"),
            "{message}"
        );
        assert!(message.contains("<none>/<none>=1"), "{message}");
        assert!(message.contains("1.234"), "{message}");
        assert!(message.contains("60"), "{message}");
    }

    /// SPEC-5.6.12: a failing snapshot query must be appended to the original
    /// error, never replace it.
    #[test]
    fn fixture_failure_message_appends_a_failed_snapshot_without_losing_the_original_error() {
        let ctx = FixtureStallContext {
            waited_for_clone_slot: Duration::ZERO,
            step: Duration::from_secs(5),
        };
        let message = fixture_failure_message(
            "clone the test fixture database cr_test_42_7: boom",
            ctx,
            Err("connect for the server snapshot: refused".to_string()),
        );

        assert!(message.contains("boom"), "{message}");
        assert!(message.contains("server snapshot failed"), "{message}");
        assert!(message.contains("refused"), "{message}");
    }
}
