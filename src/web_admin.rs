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

use crate::db::DbPool;
use argon2::password_hash::{phc::PasswordHash, PasswordHasher, PasswordVerifier};
use argon2::Argon2;
use totp_rs::{Builder, Secret, Totp};
use tracing::warn;

use crate::error::{Error, Result};
use crate::secrets::SecretsKey;

/// Shortest password the setup page accepts. Long enough that an exposed setup
/// page is not trivially brute-forced, short enough not to push operators into
/// reusing something memorable.
pub const MIN_PASSWORD_LEN: usize = 12;

/// Bytes of entropy in the session-signing key. The key never leaves the DB and
/// only ever feeds an HMAC, so 32 bytes is plenty.
const SESSION_KEY_BYTES: usize = 32;

/// Issuer label baked into every `otpauth://` URI, so an authenticator app
/// groups every claude-run instance's entry under the same name.
const TOTP_ISSUER: &str = "claude-run";

/// How many single-use recovery codes are minted at enrollment confirmation.
pub const RECOVERY_CODE_COUNT: usize = 10;

/// The single administrator credential.
#[derive(Debug, Clone)]
pub struct AdminCredential {
    pub username: String,
    /// argon2id PHC string (`$argon2id$v=19$...`), never the password.
    password_hash: String,
    /// Hex-encoded HMAC key for login session cookies. Rotated with the
    /// password, so a password change signs out every existing session.
    session_key: String,
    /// The RFC 6238 secret (base32), age-encrypted under the same key that
    /// protects the `secrets` table. Set as soon as enrollment starts, before
    /// it is confirmed: `totp_enrolled_at` is the actual enrollment flag.
    totp_secret: Option<Vec<u8>>,
    /// Set only once enrollment is confirmed with a valid code.
    totp_enrolled_at: Option<String>,
    /// The last accepted RFC 6238 time step, so a captured code cannot be
    /// replayed within its own 30s window.
    totp_last_step: Option<i64>,
}

/// The `otpauth://` URI and its rendered QR code for a pending enrollment.
pub struct PendingEnrollment {
    pub otpauth_url: String,
    /// Inline SVG markup (no `<?xml ... ?>` prologue), safe to embed directly
    /// in a template.
    pub qr_svg: String,
}

impl AdminCredential {
    /// The session-cookie signing key.
    pub fn session_key(&self) -> &str {
        &self.session_key
    }

    /// Whether TOTP enrollment has been confirmed. `false` for a fresh account
    /// and for one mid-enrollment (a secret generated but not yet confirmed).
    pub fn totp_enrolled(&self) -> bool {
        self.totp_enrolled_at.is_some()
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

type AdminRow = (
    String,
    String,
    String,
    Option<Vec<u8>>,
    Option<String>,
    Option<i64>,
);

/// Load the admin credential, or `None` when the instance has not been set up.
pub async fn load(pool: &DbPool) -> Result<Option<AdminCredential>> {
    let row: Option<AdminRow> = sqlx::query_as(&crate::db::q(
        pool,
        "SELECT username, password_hash, session_key, totp_secret, totp_enrolled_at, \
             totp_last_step FROM web_admin WHERE id = 1",
    ))
    .fetch_optional(pool)
    .await?;
    Ok(row.map(
        |(username, password_hash, session_key, totp_secret, totp_enrolled_at, totp_last_step)| {
            AdminCredential {
                username,
                password_hash,
                session_key,
                totp_secret,
                totp_enrolled_at,
                totp_last_step,
            }
        },
    ))
}

/// Whether an admin already exists. Cheaper than [`load`] for the "is this
/// instance configured" question the setup page asks.
pub async fn exists(pool: &DbPool) -> Result<bool> {
    let n: i64 = sqlx::query_scalar(&crate::db::q(
        pool,
        "SELECT COUNT(*) FROM web_admin WHERE id = 1",
    ))
    .fetch_one(pool)
    .await?;
    Ok(n > 0)
}

/// Create the admin. Fails when one already exists (the `id = 1` primary key is
/// what makes two concurrent setup submissions safe: the second insert loses).
pub async fn create(pool: &DbPool, username: &str, password: &str) -> Result<AdminCredential> {
    let username = username.trim();
    validate(username, password)?;
    let cred = AdminCredential {
        username: username.to_string(),
        password_hash: hash_password(password)?,
        session_key: new_session_key()?,
        totp_secret: None,
        totp_enrolled_at: None,
        totp_last_step: None,
    };
    let now = chrono::Utc::now().to_rfc3339();
    let inserted = sqlx::query(&crate::db::q(
        pool,
        "INSERT INTO web_admin (id, username, password_hash, session_key, created_at, updated_at) \
         VALUES (1, ?, ?, ?, ?, ?) ON CONFLICT DO NOTHING",
    ))
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
pub async fn set_password(pool: &DbPool, password: &str) -> Result<AdminCredential> {
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
        totp_secret: current.totp_secret,
        totp_enrolled_at: current.totp_enrolled_at,
        totp_last_step: current.totp_last_step,
    };
    sqlx::query(&crate::db::q(
        pool,
        "UPDATE web_admin SET password_hash = ?, session_key = ?, updated_at = ? WHERE id = 1",
    ))
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

fn build_totp(username: &str, secret: Secret) -> Result<Totp> {
    Builder::new()
        .with_issuer(Some(TOTP_ISSUER))
        .with_account_name(username.to_string())
        .with_secret(secret)
        .build()
        .map_err(|e| Error::Secret(format!("building the TOTP generator: {e}")))
}

fn decode_stored_secret(key: &SecretsKey, ciphertext: &[u8]) -> Result<Secret> {
    let base32 = key
        .decrypt(ciphertext)
        .map_err(|e| Error::Secret(format!("decrypting the stored TOTP secret: {e}")))?;
    Secret::try_from_base32(&base32)
        .map_err(|e| Error::Secret(format!("the stored TOTP secret is not valid base32: {e:?}")))
}

/// Begin (or restart) TOTP enrollment: generate a fresh secret, store it
/// encrypted but NOT yet enrolled, and return the `otpauth://` URI plus its
/// rendered QR code. Re-running this before confirmation simply replaces the
/// pending secret, so a reload during setup is harmless.
pub async fn begin_totp_enrollment(
    pool: &DbPool,
    key: &SecretsKey,
    username: &str,
) -> Result<PendingEnrollment> {
    let secret = Secret::generate();
    let base32 = secret.to_base32();
    let totp = build_totp(username, secret)?;
    let otpauth_url = totp
        .to_url()
        .map_err(|e| Error::Secret(format!("building the otpauth URL: {e}")))?;
    let ciphertext = key
        .encrypt(&base32)
        .map_err(|e| Error::Secret(format!("encrypting the TOTP secret: {e}")))?;
    sqlx::query(&crate::db::q(
        pool,
        "UPDATE web_admin SET totp_secret = ?, totp_enrolled_at = NULL, totp_last_step = NULL, \
         updated_at = ? WHERE id = 1",
    ))
    .bind(&ciphertext)
    .bind(chrono::Utc::now().to_rfc3339())
    .execute(pool)
    .await?;
    let qr_svg = qr_svg(&otpauth_url)?;
    Ok(PendingEnrollment {
        otpauth_url,
        qr_svg,
    })
}

/// Confirm a pending enrollment with a valid current code. On success, marks
/// the account enrolled and mints the one-time set of recovery codes.
pub async fn confirm_totp_enrollment(
    pool: &DbPool,
    key: &SecretsKey,
    username: &str,
    code: &str,
) -> Result<Vec<String>> {
    let admin = load(pool)
        .await?
        .ok_or_else(|| Error::Secret("no administrator to enroll".to_string()))?;
    let ciphertext = admin
        .totp_secret
        .ok_or_else(|| Error::Secret("no pending TOTP enrollment".to_string()))?;
    let secret = decode_stored_secret(key, &ciphertext)?;
    let totp = build_totp(username, secret)?;
    let now = chrono::Utc::now();
    let Some(matched_step) = totp.check_current(code) else {
        return Err(Error::Secret("that code is not valid".to_string()));
    };
    let codes = generate_recovery_codes()?;
    let now_str = now.to_rfc3339();
    sqlx::query(&crate::db::q(
        pool,
        "UPDATE web_admin SET totp_enrolled_at = ?, totp_last_step = ?, updated_at = ? WHERE id = 1",
    ))
    .bind(&now_str)
    .bind(matched_step as i64)
    .bind(&now_str)
    .execute(pool)
    .await?;
    sqlx::query(&crate::db::q(pool, "DELETE FROM web_admin_recovery_codes"))
        .execute(pool)
        .await?;
    for plain in &codes {
        let hash = hash_password(plain)?;
        sqlx::query(&crate::db::q(
            pool,
            "INSERT INTO web_admin_recovery_codes (code_hash, used_at, created_at) VALUES (?, NULL, ?)",
        ))
        .bind(&hash)
        .bind(&now_str)
        .execute(pool)
        .await?;
    }
    Ok(codes)
}

/// Verify a login-time code: either the current/adjacent-window TOTP code, or
/// an unused recovery code (consumed on success). Replay of the same TOTP
/// step is refused. A corrupt stored secret is a propagated error, never a
/// silent "not 2FA'd".
pub async fn verify_totp_or_recovery(
    pool: &DbPool,
    key: &SecretsKey,
    admin: &AdminCredential,
    code: &str,
) -> Result<bool> {
    let code = code.trim();
    if code.is_empty() {
        return Ok(false);
    }
    if let Some(ciphertext) = &admin.totp_secret {
        let secret = decode_stored_secret(key, ciphertext)?;
        let totp = build_totp(&admin.username, secret)?;
        if let Some(matched_step) = totp.check_current(code) {
            let replay = admin
                .totp_last_step
                .is_some_and(|last| last >= matched_step as i64);
            if !replay {
                sqlx::query(&crate::db::q(
                    pool,
                    "UPDATE web_admin SET totp_last_step = ? WHERE id = 1",
                ))
                .bind(matched_step as i64)
                .execute(pool)
                .await?;
                return Ok(true);
            }
            warn!("rejected a replayed TOTP code for the web admin login");
            return Ok(false);
        }
    }
    try_consume_recovery_code(pool, code).await
}

async fn try_consume_recovery_code(pool: &DbPool, code: &str) -> Result<bool> {
    let rows: Vec<(i64, String)> = sqlx::query_as(&crate::db::q(
        pool,
        "SELECT id, code_hash FROM web_admin_recovery_codes WHERE used_at IS NULL",
    ))
    .fetch_all(pool)
    .await?;
    for (id, hash) in rows {
        let parsed = PasswordHash::new(&hash)
            .map_err(|e| Error::Secret(format!("stored recovery code hash is corrupt: {e}")))?;
        if Argon2::default()
            .verify_password(code.as_bytes(), &parsed)
            .is_ok()
        {
            sqlx::query(&crate::db::q(
                pool,
                "UPDATE web_admin_recovery_codes SET used_at = ? WHERE id = ?",
            ))
            .bind(chrono::Utc::now().to_rfc3339())
            .bind(id)
            .execute(pool)
            .await?;
            return Ok(true);
        }
    }
    Ok(false)
}

fn generate_recovery_codes() -> Result<Vec<String>> {
    let mut codes = Vec::with_capacity(RECOVERY_CODE_COUNT);
    for _ in 0..RECOVERY_CODE_COUNT {
        let mut buf = [0u8; 5];
        getrandom::getrandom(&mut buf)
            .map_err(|e| Error::Secret(format!("reading random bytes for a recovery code: {e}")))?;
        let hex = hex::encode(buf).to_uppercase();
        codes.push(format!("{}-{}", &hex[0..5], &hex[5..10]));
    }
    Ok(codes)
}

/// Clear TOTP enrollment and every recovery code, forcing re-enrollment at the
/// next login. Rotates the session key too, so an in-progress session cannot
/// keep riding an old grant past the reset.
pub async fn reset_totp(pool: &DbPool) -> Result<AdminCredential> {
    let Some(current) = load(pool).await? else {
        return Err(Error::Secret(
            "no administrator to reset 2FA for".to_string(),
        ));
    };
    let session_key = new_session_key()?;
    sqlx::query(&crate::db::q(
        pool,
        "UPDATE web_admin SET totp_secret = NULL, totp_enrolled_at = NULL, totp_last_step = NULL, \
         session_key = ?, updated_at = ? WHERE id = 1",
    ))
    .bind(&session_key)
    .bind(chrono::Utc::now().to_rfc3339())
    .execute(pool)
    .await?;
    sqlx::query(&crate::db::q(pool, "DELETE FROM web_admin_recovery_codes"))
        .execute(pool)
        .await?;
    Ok(AdminCredential {
        session_key,
        totp_secret: None,
        totp_enrolled_at: None,
        totp_last_step: None,
        ..current
    })
}

/// Render `text` (an `otpauth://` URI) as an inline SVG QR code, with no XML
/// prologue, ready to embed directly in a template.
fn qr_svg(text: &str) -> Result<String> {
    let qr = qrcodegen::QrCode::encode_text(text, qrcodegen::QrCodeEcc::Medium)
        .map_err(|e| Error::Secret(format!("rendering the enrollment QR code: {e}")))?;
    Ok(render_qr_svg(&qr))
}

fn render_qr_svg(qr: &qrcodegen::QrCode) -> String {
    let size = qr.size();
    let border = 4;
    let mut path = String::new();
    for y in 0..size {
        for x in 0..size {
            if qr.get_module(x, y) {
                path.push_str(&format!("M{},{}h1v1h-1z", x + border, y + border));
            }
        }
    }
    let dim = size + border * 2;
    format!(
        "<svg xmlns=\"http://www.w3.org/2000/svg\" viewBox=\"0 0 {dim} {dim}\" width=\"220\" height=\"220\" shape-rendering=\"crispEdges\">\
<rect width=\"{dim}\" height=\"{dim}\" fill=\"#ffffff\"/>\
<path d=\"{path}\" fill=\"#000000\"/>\
</svg>"
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use age::secrecy::ExposeSecret;

    async fn mem_pool() -> DbPool {
        crate::db::test_pool().await
    }

    fn test_key() -> SecretsKey {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("key.txt");
        let identity = age::x25519::Identity::generate();
        std::fs::write(&path, identity.to_string().expose_secret()).unwrap();
        SecretsKey::load_from_file(&path).unwrap()
    }

    fn current_totp_code(admin: &AdminCredential, key: &SecretsKey) -> String {
        let ciphertext = admin.totp_secret.clone().expect("pending secret");
        let secret = decode_stored_secret(key, &ciphertext).unwrap();
        let totp = build_totp(&admin.username, secret).unwrap();
        totp.generate_current().to_string()
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
        let stored: String = sqlx::query_scalar(&crate::db::q(
            &pool,
            "SELECT password_hash FROM web_admin WHERE id = 1",
        ))
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

    #[tokio::test]
    async fn a_fresh_admin_is_not_totp_enrolled() {
        let pool = mem_pool().await;
        let admin = create(&pool, "admin", "correct horse battery")
            .await
            .unwrap();
        assert!(!admin.totp_enrolled());
    }

    #[tokio::test]
    async fn enrollment_round_trips_and_issues_recovery_codes() {
        let pool = mem_pool().await;
        let key = test_key();
        create(&pool, "admin", "correct horse battery")
            .await
            .unwrap();

        let pending = begin_totp_enrollment(&pool, &key, "admin").await.unwrap();
        assert!(pending.otpauth_url.starts_with("otpauth://totp/"));
        assert!(pending.qr_svg.starts_with("<svg"));

        let admin = load(&pool).await.unwrap().unwrap();
        assert!(
            !admin.totp_enrolled(),
            "a pending secret is not yet a confirmed enrollment"
        );
        let code = current_totp_code(&admin, &key);

        let codes = confirm_totp_enrollment(&pool, &key, "admin", &code)
            .await
            .unwrap();
        assert_eq!(codes.len(), RECOVERY_CODE_COUNT);
        assert_eq!(
            codes.iter().collect::<std::collections::HashSet<_>>().len(),
            RECOVERY_CODE_COUNT,
            "recovery codes must be distinct"
        );

        let admin = load(&pool).await.unwrap().unwrap();
        assert!(admin.totp_enrolled());
    }

    #[tokio::test]
    async fn confirming_with_a_wrong_code_leaves_enrollment_pending() {
        let pool = mem_pool().await;
        let key = test_key();
        create(&pool, "admin", "correct horse battery")
            .await
            .unwrap();
        begin_totp_enrollment(&pool, &key, "admin").await.unwrap();

        let err = confirm_totp_enrollment(&pool, &key, "admin", "000000")
            .await
            .unwrap_err();
        assert!(err.to_string().contains("not valid"), "{err}");

        let admin = load(&pool).await.unwrap().unwrap();
        assert!(!admin.totp_enrolled());
    }

    #[tokio::test]
    async fn a_totp_code_cannot_be_replayed() {
        let pool = mem_pool().await;
        let key = test_key();
        create(&pool, "admin", "correct horse battery")
            .await
            .unwrap();
        begin_totp_enrollment(&pool, &key, "admin").await.unwrap();
        let admin = load(&pool).await.unwrap().unwrap();
        let enroll_code = current_totp_code(&admin, &key);
        confirm_totp_enrollment(&pool, &key, "admin", &enroll_code)
            .await
            .unwrap();

        // The code that confirmed enrollment is itself now spent (it set
        // `totp_last_step`), so a login attempt reusing it must be refused.
        let admin = load(&pool).await.unwrap().unwrap();
        assert!(
            !verify_totp_or_recovery(&pool, &key, &admin, &enroll_code)
                .await
                .unwrap(),
            "the enrollment-confirming code must not double as a login code"
        );

        // A fresh code (the next time step) verifies once, then is refused on
        // an exact replay within the same window.
        let ciphertext = admin.totp_secret.clone().unwrap();
        let secret = decode_stored_secret(&key, &ciphertext).unwrap();
        let totp = build_totp(&admin.username, secret).unwrap();
        let next_code = totp
            .generate(admin.totp_last_step.unwrap() as u64 * 30 + 30)
            .to_string();

        let admin = load(&pool).await.unwrap().unwrap();
        assert!(
            verify_totp_or_recovery(&pool, &key, &admin, &next_code)
                .await
                .unwrap(),
            "a fresh code must verify a login"
        );
        let admin = load(&pool).await.unwrap().unwrap();
        assert!(
            !verify_totp_or_recovery(&pool, &key, &admin, &next_code)
                .await
                .unwrap(),
            "the exact same code must be refused a second time"
        );
    }

    #[tokio::test]
    async fn a_recovery_code_works_exactly_once() {
        let pool = mem_pool().await;
        let key = test_key();
        create(&pool, "admin", "correct horse battery")
            .await
            .unwrap();
        begin_totp_enrollment(&pool, &key, "admin").await.unwrap();
        let admin = load(&pool).await.unwrap().unwrap();
        let code = current_totp_code(&admin, &key);
        let codes = confirm_totp_enrollment(&pool, &key, "admin", &code)
            .await
            .unwrap();
        let recovery = &codes[0];

        let admin = load(&pool).await.unwrap().unwrap();
        assert!(verify_totp_or_recovery(&pool, &key, &admin, recovery)
            .await
            .unwrap());
        let admin = load(&pool).await.unwrap().unwrap();
        assert!(
            !verify_totp_or_recovery(&pool, &key, &admin, recovery)
                .await
                .unwrap(),
            "a recovery code must not work twice"
        );
    }

    #[tokio::test]
    async fn a_corrupt_stored_secret_is_a_propagated_error() {
        let pool = mem_pool().await;
        let key = test_key();
        create(&pool, "admin", "correct horse battery")
            .await
            .unwrap();
        sqlx::query(&crate::db::q(
            &pool,
            "UPDATE web_admin SET totp_secret = ?, totp_enrolled_at = ? WHERE id = 1",
        ))
        .bind(b"not a real ciphertext".to_vec())
        .bind(chrono::Utc::now().to_rfc3339())
        .execute(&pool)
        .await
        .unwrap();

        let admin = load(&pool).await.unwrap().unwrap();
        assert!(admin.totp_enrolled());
        let err = verify_totp_or_recovery(&pool, &key, &admin, "123456")
            .await
            .unwrap_err();
        assert!(err.to_string().contains("decrypting"), "{err}");
    }

    #[tokio::test]
    async fn reset_totp_clears_enrollment_and_rotates_the_session_key() {
        let pool = mem_pool().await;
        let key = test_key();
        create(&pool, "admin", "correct horse battery")
            .await
            .unwrap();
        begin_totp_enrollment(&pool, &key, "admin").await.unwrap();
        let admin = load(&pool).await.unwrap().unwrap();
        let code = current_totp_code(&admin, &key);
        confirm_totp_enrollment(&pool, &key, "admin", &code)
            .await
            .unwrap();
        let before = load(&pool).await.unwrap().unwrap();
        assert!(before.totp_enrolled());

        let reset = reset_totp(&pool).await.unwrap();
        assert!(!reset.totp_enrolled());
        assert_ne!(
            reset.session_key(),
            before.session_key(),
            "reset must sign out every existing session"
        );

        let after = load(&pool).await.unwrap().unwrap();
        assert!(!after.totp_enrolled());
        let codes: i64 = sqlx::query_scalar(&crate::db::q(
            &pool,
            "SELECT COUNT(*) FROM web_admin_recovery_codes",
        ))
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(codes, 0, "recovery codes must be cleared on reset");
    }
}
