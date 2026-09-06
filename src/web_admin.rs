//! The web UI's own administrator credential: username plus an argon2id
//! password hash in the `web_admin` table, written by the first-run setup page.
//!
//! This exists so a fresh instance is reachable with nothing but a browser. The
//! older login path (`web.auth_username` plus the plaintext of the
//! `web.auth_token_ref` secret) cannot bootstrap a container: the secret lives
//! age-encrypted in `secrets`, so it needs a key file on disk AND the
//! `claude-run secret set` CLI, while the daemon refuses to expose the TCP
//! surface until that token resolves. The operator ends up locked out of the UI
//! that would have configured it. Both paths now coexist: a resolvable
//! `auth_token_ref` still authenticates `Authorization: Bearer` for API clients,
//! and this credential authenticates the browser.
//!
//! There is exactly one row (`id = 1`, enforced by the schema). This is the
//! instance's admin, not a user table.

use argon2::password_hash::{phc::PasswordHash, PasswordHasher, PasswordVerifier};
use argon2::Argon2;
use sqlx::SqlitePool;

use crate::error::{Error, Result};

/// Shortest password the setup page accepts. Long enough that an exposed setup
/// page is not trivially brute-forced, short enough not to push operators into
/// reusing something memorable.
pub const MIN_PASSWORD_LEN: usize = 12;

/// Bytes of entropy in the session-signing key. The key never leaves the DB and
/// only ever feeds an HMAC, so 32 bytes is plenty.
const SESSION_KEY_BYTES: usize = 32;

/// The single administrator credential.
#[derive(Debug, Clone)]
pub struct AdminCredential {
    pub username: String,
    /// argon2id PHC string (`$argon2id$v=19$...`), never the password.
    password_hash: String,
    /// Hex-encoded HMAC key for login session cookies. Rotated with the
    /// password, so a password change signs out every existing session.
    session_key: String,
}

impl AdminCredential {
    /// The session-cookie signing key.
    pub fn session_key(&self) -> &str {
        &self.session_key
    }

    /// Whether `password` matches the stored hash. A malformed stored hash is an
    /// error rather than a silent `false`: that is a corrupt row, not a wrong
    /// password, and the two must not look the same to the operator.
    pub fn verify_password(&self, password: &str) -> Result<bool> {
        let parsed = PasswordHash::new(&self.password_hash).map_err(|e| {
            Error::Secret(format!(
                "stored admin password hash is not a valid PHC string: {e}"
            ))
        })?;
        Ok(Argon2::default()
            .verify_password(password.as_bytes(), &parsed)
            .is_ok())
    }
}

/// Load the admin credential, or `None` when the instance has not been set up.
pub async fn load(pool: &SqlitePool) -> Result<Option<AdminCredential>> {
    let row: Option<(String, String, String)> =
        sqlx::query_as("SELECT username, password_hash, session_key FROM web_admin WHERE id = 1")
            .fetch_optional(pool)
            .await?;
    Ok(
        row.map(|(username, password_hash, session_key)| AdminCredential {
            username,
            password_hash,
            session_key,
        }),
    )
}

/// Whether an admin already exists. Cheaper than [`load`] for the "is this
/// instance configured" question the setup page asks.
pub async fn exists(pool: &SqlitePool) -> Result<bool> {
    let n: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM web_admin WHERE id = 1")
        .fetch_one(pool)
        .await?;
    Ok(n > 0)
}

/// Create the admin. Fails when one already exists (the `id = 1` primary key is
/// what makes two concurrent setup submissions safe: the second insert loses).
pub async fn create(pool: &SqlitePool, username: &str, password: &str) -> Result<AdminCredential> {
    let username = username.trim();
    validate(username, password)?;
    let cred = AdminCredential {
        username: username.to_string(),
        password_hash: hash_password(password)?,
        session_key: new_session_key()?,
    };
    let now = chrono::Utc::now().to_rfc3339();
    let inserted = sqlx::query(
        "INSERT OR IGNORE INTO web_admin (id, username, password_hash, session_key, created_at, updated_at) \
         VALUES (1, ?, ?, ?, ?, ?)",
    )
    .bind(&cred.username)
    .bind(&cred.password_hash)
    .bind(&cred.session_key)
    .bind(&now)
    .bind(&now)
    .execute(pool)
    .await?
    .rows_affected();
    if inserted == 0 {
        return Err(Error::Secret(
            "an administrator already exists; setup runs once".to_string(),
        ));
    }
    Ok(cred)
}

/// Replace the password (and rotate the session key with it, signing out every
/// browser). Errors when no admin exists yet: that is a setup, not a change.
pub async fn set_password(pool: &SqlitePool, password: &str) -> Result<AdminCredential> {
    let Some(current) = load(pool).await? else {
        return Err(Error::Secret(
            "no administrator to update; run first-run setup instead".to_string(),
        ));
    };
    validate(&current.username, password)?;
    let cred = AdminCredential {
        username: current.username,
        password_hash: hash_password(password)?,
        session_key: new_session_key()?,
    };
    sqlx::query(
        "UPDATE web_admin SET password_hash = ?, session_key = ?, updated_at = ? WHERE id = 1",
    )
    .bind(&cred.password_hash)
    .bind(&cred.session_key)
    .bind(chrono::Utc::now().to_rfc3339())
    .execute(pool)
    .await?;
    Ok(cred)
}

/// Reject the credentials the setup form must not accept. Returned as an error
/// the page renders verbatim, so the operator is told which rule they broke.
pub fn validate(username: &str, password: &str) -> Result<()> {
    if username.trim().is_empty() {
        return Err(Error::Secret("username must not be empty".to_string()));
    }
    if password.chars().count() < MIN_PASSWORD_LEN {
        return Err(Error::Secret(format!(
            "password must be at least {MIN_PASSWORD_LEN} characters"
        )));
    }
    Ok(())
}

fn hash_password(password: &str) -> Result<String> {
    Ok(Argon2::default()
        .hash_password(password.as_bytes())
        .map_err(|e| Error::Secret(format!("hashing the admin password failed: {e}")))?
        .to_string())
}

fn new_session_key() -> Result<String> {
    let mut buf = [0u8; SESSION_KEY_BYTES];
    getrandom::getrandom(&mut buf)
        .map_err(|e| Error::Secret(format!("reading random bytes for the session key: {e}")))?;
    Ok(hex::encode(buf))
}

#[cfg(test)]
mod tests {
    use super::*;
    use sqlx::sqlite::SqlitePoolOptions;

    async fn mem_pool() -> SqlitePool {
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect("sqlite::memory:")
            .await
            .unwrap();
        crate::db::MIGRATOR.run(&pool).await.unwrap();
        pool
    }

    #[tokio::test]
    async fn a_fresh_instance_has_no_admin() {
        let pool = mem_pool().await;
        assert!(!exists(&pool).await.unwrap());
        assert!(load(&pool).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn create_then_verify_round_trips() {
        let pool = mem_pool().await;
        let created = create(&pool, "admin", "correct horse battery")
            .await
            .unwrap();
        assert_eq!(created.username, "admin");
        assert!(exists(&pool).await.unwrap());

        let loaded = load(&pool).await.unwrap().expect("admin row");
        assert_eq!(loaded.username, "admin");
        assert!(loaded.verify_password("correct horse battery").unwrap());
        assert!(!loaded.verify_password("wrong horse battery").unwrap());
        assert_eq!(
            loaded.session_key(),
            created.session_key(),
            "the stored session key must survive the round trip, or every restart signs everyone out"
        );
    }

    #[tokio::test]
    async fn the_password_is_never_stored_in_the_clear() {
        let pool = mem_pool().await;
        create(&pool, "admin", "correct horse battery")
            .await
            .unwrap();
        let stored: String = sqlx::query_scalar("SELECT password_hash FROM web_admin WHERE id = 1")
            .fetch_one(&pool)
            .await
            .unwrap();
        assert!(stored.starts_with("$argon2id$"), "got {stored}");
        assert!(!stored.contains("correct horse battery"));
    }

    #[tokio::test]
    async fn setup_runs_once() {
        let pool = mem_pool().await;
        create(&pool, "admin", "correct horse battery")
            .await
            .unwrap();
        let err = create(&pool, "intruder", "another long password")
            .await
            .unwrap_err();
        assert!(
            err.to_string().contains("already exists"),
            "a second setup must be refused, not silently ignored: {err}"
        );
        // The original credential is untouched by the refused attempt.
        let loaded = load(&pool).await.unwrap().unwrap();
        assert_eq!(loaded.username, "admin");
        assert!(loaded.verify_password("correct horse battery").unwrap());
    }

    #[tokio::test]
    async fn short_passwords_and_empty_usernames_are_refused() {
        let pool = mem_pool().await;
        let err = create(&pool, "admin", "short").await.unwrap_err();
        assert!(err.to_string().contains("at least"), "{err}");
        let err = create(&pool, "   ", "correct horse battery")
            .await
            .unwrap_err();
        assert!(err.to_string().contains("username"), "{err}");
        assert!(
            !exists(&pool).await.unwrap(),
            "nothing is written on refusal"
        );
    }

    #[tokio::test]
    async fn changing_the_password_rotates_the_session_key() {
        let pool = mem_pool().await;
        let first = create(&pool, "admin", "correct horse battery")
            .await
            .unwrap();
        let second = set_password(&pool, "a different long password")
            .await
            .unwrap();
        assert_eq!(second.username, "admin");
        assert!(second.verify_password("a different long password").unwrap());
        assert!(!second.verify_password("correct horse battery").unwrap());
        assert_ne!(
            first.session_key(),
            second.session_key(),
            "a password change must invalidate outstanding sessions"
        );
    }

    #[tokio::test]
    async fn two_admins_get_distinct_session_keys() {
        let pool_a = mem_pool().await;
        let pool_b = mem_pool().await;
        let a = create(&pool_a, "admin", "correct horse battery")
            .await
            .unwrap();
        let b = create(&pool_b, "admin", "correct horse battery")
            .await
            .unwrap();
        assert_ne!(
            a.session_key(),
            b.session_key(),
            "session keys must be random per instance, not derived from the password"
        );
    }
}
