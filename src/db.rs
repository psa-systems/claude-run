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
use std::time::Duration;

use sqlx::any::{AnyConnectOptions, AnyPoolOptions};
use sqlx::Executor;
use tracing::{error, warn};

use crate::config::DatabaseTarget;
use crate::error::Result;

/// The pool every module threads. One type, whichever backend is configured.
pub type DbPool = sqlx::AnyPool;

/// The SQLite chain, twenty files, unchanged since before CLAUDE-250 so an
/// existing `state.db` matches the versions and checksums already in its
/// `_sqlx_migrations` table and re-runs nothing.
pub static SQLITE_MIGRATOR: sqlx::migrate::Migrator = sqlx::migrate!("./migrations/sqlite");

/// The Postgres schema, squashed into one file. A Postgres database is new by
/// definition here, so it needs the end state rather than the history.
pub static POSTGRES_MIGRATOR: sqlx::migrate::Migrator = sqlx::migrate!("./migrations/postgres");

/// How long [`open`] keeps retrying an unreachable Postgres before giving up.
/// Compose can start the daemon before the database accepts connections, so an
/// instant abort would crash-loop a healthy stack; an unbounded wait would hide
/// a genuinely wrong URL forever.
const CONNECT_RETRY_DEADLINE: Duration = Duration::from_secs(60);
const CONNECT_RETRY_INTERVAL: Duration = Duration::from_secs(2);

/// Which SQL dialect the open pool speaks.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Dialect {
    Sqlite,
    Postgres,
}

impl Dialect {
    /// The dialect a connection URL scheme selects. `sqlite`, `postgres` and
    /// `postgresql` are the whole accepted set; anything else is a startup
    /// error naming the scheme rather than a silent fallback to SQLite.
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
/// with flag columns implement `FromRow` by hand through this.
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
pub fn q<'a>(pool: &DbPool, sql: &'a str) -> Cow<'a, str> {
    rewrite(dialect(pool), sql)
}

/// [`q`] with the dialect supplied directly, so the rewrites are testable with
/// no database of either kind.
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

/// Open the pool for `target` and run the matching migration set.
pub async fn open(target: &DatabaseTarget) -> Result<DbPool> {
    // Registers the sqlite and postgres backends behind the `Any` driver. Idempotent.
    sqlx::any::install_default_drivers();
    match target {
        DatabaseTarget::Sqlite(path) => open_sqlite(path).await,
        DatabaseTarget::Postgres(url) => open_postgres(url).await,
    }
}

/// Convenience for the many tests and CLI paths that just want the SQLite file.
pub async fn open_sqlite(db_path: &Path) -> Result<DbPool> {
    sqlx::any::install_default_drivers();
    if let Some(parent) = db_path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let url = format!("sqlite://{}?mode=rwc", db_path.display());
    let pool = pool_options(Dialect::Sqlite)
        .connect_with(AnyConnectOptions::from_str(&url)?)
        .await?;
    SQLITE_MIGRATOR.run(&pool).await?;
    Ok(pool)
}

async fn open_postgres(url: &str) -> Result<DbPool> {
    let opts = AnyConnectOptions::from_str(url)?;
    // Never the password: this string is logged on every retry.
    let host = opts.database_url.host_str().unwrap_or("?").to_string();
    let deadline = std::time::Instant::now() + CONNECT_RETRY_DEADLINE;
    let mut attempt = 0u32;
    let pool = loop {
        attempt += 1;
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
    POSTGRES_MIGRATOR.run(&pool).await?;
    Ok(pool)
}

/// Pool settings, applied to both backends.
fn pool_options(dialect: Dialect) -> AnyPoolOptions {
    let opts = AnyPoolOptions::new()
        // 8 left no headroom once the daemon runs several overlapping
        // runs plus the web UI's polling.
        .max_connections(16)
        // sqlx's default, stated so it is a decision and not an
        // inherited value. An acquire that waits this long is a real fault.
        .acquire_timeout(Duration::from_secs(30))
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

/// The pool every test builds its fixture in: an in-memory SQLite database by
/// default, or a private schema on the server named by
/// `CLAUDE_RUN_TEST_DATABASE_URL`. Setting that variable therefore re-runs the
/// WHOLE suite against Postgres, which is what makes the ~130 statements in the
/// tree actually exercised on both backends rather than only on the default one.
#[cfg(test)]
pub async fn test_pool() -> DbPool {
    match std::env::var("CLAUDE_RUN_TEST_DATABASE_URL") {
        Ok(url) => test_pg_pool_at(&url).await,
        Err(_) => test_sqlite_pool().await,
    }
}

/// An in-memory SQLite pool with the schema applied. One connection, because a
/// second connection to `sqlite::memory:` is a second, empty database.
#[cfg(test)]
pub async fn test_sqlite_pool() -> DbPool {
    sqlx::any::install_default_drivers();
    let pool = AnyPoolOptions::new()
        .max_connections(1)
        .connect("sqlite::memory:")
        .await
        .expect("open in-memory sqlite");
    SQLITE_MIGRATOR.run(&pool).await.expect("run migrations");
    pool
}

/// The Postgres pool the backend-specific tests run against, or `None` when
/// `CLAUDE_RUN_TEST_DATABASE_URL` is unset.
#[cfg(test)]
pub async fn test_pg_pool() -> Option<DbPool> {
    let url = std::env::var("CLAUDE_RUN_TEST_DATABASE_URL").ok()?;
    Some(test_pg_pool_at(&url).await)
}

/// A Postgres pool whose every connection sees only a schema of its own, so
/// `cargo test`'s parallel tests do not share tables. Nothing is dropped: the
/// server this points at is a disposable CI service container.
#[cfg(test)]
async fn test_pg_pool_at(url: &str) -> DbPool {
    use std::sync::atomic::{AtomicU64, Ordering};
    static SEQ: AtomicU64 = AtomicU64::new(0);
    let schema = format!(
        "cr_test_{}_{}",
        std::process::id(),
        SEQ.fetch_add(1, Ordering::Relaxed)
    );
    sqlx::any::install_default_drivers();
    let set_path = format!("SET search_path TO {schema}");
    let pool = AnyPoolOptions::new()
        .max_connections(4)
        .after_connect(move |conn, _meta| {
            let set_path = set_path.clone();
            Box::pin(async move {
                conn.execute(&*set_path).await?;
                Ok(())
            })
        })
        .connect(url)
        .await
        .expect("connect to CLAUDE_RUN_TEST_DATABASE_URL");
    pool.execute(&*format!("CREATE SCHEMA IF NOT EXISTS {schema}"))
        .await
        .expect("create the test schema");
    POSTGRES_MIGRATOR
        .run(&pool)
        .await
        .expect("run postgres migrations");
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
}
