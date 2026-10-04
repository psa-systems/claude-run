//! The web UI's accounts (CLAUDE-369): any number of credentials in
//! `web_accounts`, each with its own argon2id password hash, session-signing
//! key, and TOTP enrollment. The first account is created from the first-run
//! setup page (`/setup`); every account after that is created by accepting an
//! invite (`web_invites`) minted by any signed-in account from
//! `/settings/accounts`.
//!
//! This exists so a fresh instance is reachable with nothing but a browser.
//! The older login path (`web.auth_username` plus the plaintext of the
//! `web.auth_token_ref` secret) cannot bootstrap a container: the secret lives
//! age-encrypted in `secrets`, so it needs a key file on disk AND the
//! `claude-run secret set` CLI, while the daemon refuses to expose the TCP
//! surface until that token resolves. The operator ends up locked out of the UI
//! that would have configured it. Both paths now coexist: a resolvable
//! `auth_token_ref` still authenticates `Authorization: Bearer` for API
//! clients, and an account authenticates the browser.
//!
//! There are no roles yet: every account has full admin rights. `created_by`
//! and the plain shape of `web_accounts` leave room for a role column later
//! without a further migration.

use crate::db::DbPool;
use argon2::password_hash::{phc::PasswordHash, PasswordHasher, PasswordVerifier};
use argon2::Argon2;
use totp_rs::{Builder, Secret, Totp};
use tracing::{info, warn};

use crate::error::{Error, Result};
use crate::secrets::SecretsKey;

/// Shortest password the setup and invite-accept pages accept. Long enough
/// that an exposed form is not trivially brute-forced, short enough not to
/// push operators into reusing something memorable.
pub const MIN_PASSWORD_LEN: usize = 12;

/// Bytes of entropy in the session-signing key. The key never leaves the DB
/// and only ever feeds an HMAC, so 32 bytes is plenty.
const SESSION_KEY_BYTES: usize = 32;

/// Bytes of entropy in an invite token. 32 random bytes before hex-encoding.
const INVITE_TOKEN_BYTES: usize = 32;

/// How long a freshly minted invite stays valid before it can no longer be
/// accepted.
pub const INVITE_TTL_HOURS: i64 = 72;

/// Issuer label baked into every `otpauth://` URI, so an authenticator app
/// groups every claude-run instance's entry under the same name.
const TOTP_ISSUER: &str = "claude-run";

/// How many single-use recovery codes are minted at enrollment confirmation.
pub const RECOVERY_CODE_COUNT: usize = 10;

/// One web account.
#[derive(Debug, Clone)]
pub struct Account {
    pub id: i64,
    pub username: String,
    /// argon2id PHC string (`$argon2id$v=19$...`), never the password.
    password_hash: String,
    /// Hex-encoded HMAC key for login session cookies. Rotated with the
    /// password, so a password change (or an account removal) signs out every
    /// existing session for that account immediately.
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

/// A row for the accounts-management list: no secret material.
pub struct AccountSummary {
    pub id: i64,
    pub username: String,
    pub created_at: String,
    pub totp_enrolled: bool,
    pub created_by: Option<String>,
}

/// An outstanding invite, as shown to the inviter or checked on acceptance.
pub struct Invite {
    pub id: i64,
    pub created_by: i64,
    pub created_at: String,
    pub expires_at: String,
    pub used_at: Option<String>,
}

/// A row for the invites-management list.
pub struct InviteSummary {
    pub id: i64,
    pub created_by_username: String,
    pub created_at: String,
    pub expires_at: String,
}

/// The `otpauth://` URI and its rendered QR code for a pending enrollment.
pub struct PendingEnrollment {
    pub otpauth_url: String,
    /// Inline SVG markup (no `<?xml ... ?>` prologue), safe to embed directly
    /// in a template.
    pub qr_svg: String,
}

impl Account {
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
                "stored account password hash is not a valid PHC string: {e}"
            ))
        })?;
        Ok(Argon2::default()
            .verify_password(password.as_bytes(), &parsed)
            .is_ok())
    }
}

type AccountRow = (
    i64,
    String,
    String,
    String,
    Option<Vec<u8>>,
    Option<String>,
    Option<i64>,
);

const SELECT_COLUMNS: &str =
    "id, username, password_hash, session_key, totp_secret, totp_enrolled_at, totp_last_step";

fn row_to_account(row: AccountRow) -> Account {
    let (id, username, password_hash, session_key, totp_secret, totp_enrolled_at, totp_last_step) =
        row;
    Account {
        id,
        username,
        password_hash,
        session_key,
        totp_secret,
        totp_enrolled_at,
        totp_last_step,
    }
}

/// Load an account by id, or `None` when it does not exist (removed, or never
/// created).
pub async fn load_by_id(pool: &DbPool, id: i64) -> Result<Option<Account>> {
    let row: Option<AccountRow> = sqlx::query_as(&crate::db::q(
        pool,
        &format!("SELECT {SELECT_COLUMNS} FROM web_accounts WHERE id = ?"),
    ))
    .bind(id)
    .fetch_optional(pool)
    .await?;
    Ok(row.map(row_to_account))
}

/// Load an account by username, case-insensitively.
pub async fn load_by_username(pool: &DbPool, username: &str) -> Result<Option<Account>> {
    let row: Option<AccountRow> = sqlx::query_as(&crate::db::q(
        pool,
        &format!("SELECT {SELECT_COLUMNS} FROM web_accounts WHERE username_key = ?"),
    ))
    .bind(username.trim().to_lowercase())
    .fetch_optional(pool)
    .await?;
    Ok(row.map(row_to_account))
}

/// Whether any account already exists. Cheaper than loading one, and what the
/// setup page and the gate ask to decide whether `/setup` is still live.
pub async fn exists(pool: &DbPool) -> Result<bool> {
    let n: i64 = sqlx::query_scalar(&crate::db::q(pool, "SELECT COUNT(*) FROM web_accounts"))
        .fetch_one(pool)
        .await?;
    Ok(n > 0)
}

/// List every account for the accounts-management page, newest first.
pub async fn list(pool: &DbPool) -> Result<Vec<AccountSummary>> {
    let rows: Vec<(i64, String, String, bool, Option<String>)> = sqlx::query_as(&crate::db::q(
        pool,
        "SELECT a.id, a.username, a.created_at, a.totp_enrolled_at IS NOT NULL, b.username \
         FROM web_accounts a LEFT JOIN web_accounts b ON b.id = a.created_by \
         ORDER BY a.id ASC",
    ))
    .fetch_all(pool)
    .await?;
    Ok(rows
        .into_iter()
        .map(
            |(id, username, created_at, totp_enrolled, created_by)| AccountSummary {
                id,
                username,
                created_at,
                totp_enrolled,
                created_by,
            },
        )
        .collect())
}

/// Create the very first account from `/setup`. Fails when any account
/// already exists.
pub async fn create_first(pool: &DbPool, username: &str, password: &str) -> Result<Account> {
    if exists(pool).await? {
        return Err(Error::Secret(
            "an administrator already exists; setup runs once".to_string(),
        ));
    }
    create_account(pool, username, password, None).await
}

/// Create an account from an accepted invite, attributing it to `created_by`.
pub async fn create_invited(
    pool: &DbPool,
    username: &str,
    password: &str,
    created_by: i64,
) -> Result<Account> {
    create_account(pool, username, password, Some(created_by)).await
}

async fn create_account(
    pool: &DbPool,
    username: &str,
    password: &str,
    created_by: Option<i64>,
) -> Result<Account> {
    let username = username.trim();
    validate(username, password)?;
    let username_key = username.to_lowercase();
    if load_by_username(pool, &username_key).await?.is_some() {
        return Err(Error::Secret(format!(
            "an account named '{username}' already exists"
        )));
    }
    let password_hash = hash_password(password)?;
    let session_key = new_session_key()?;
    let now = chrono::Utc::now().to_rfc3339();
    let id: i64 = sqlx::query_scalar(&crate::db::q(
        pool,
        "INSERT INTO web_accounts (username, username_key, password_hash, session_key, \
         created_at, updated_at, created_by) VALUES (?, ?, ?, ?, ?, ?, ?) RETURNING id",
    ))
    .bind(username)
    .bind(&username_key)
    .bind(&password_hash)
    .bind(&session_key)
    .bind(&now)
    .bind(&now)
    .bind(created_by)
    .fetch_one(pool)
    .await?;
    info!(username, created_by, "web account created");
    Ok(Account {
        id,
        username: username.to_string(),
        password_hash,
        session_key,
        totp_secret: None,
        totp_enrolled_at: None,
        totp_last_step: None,
    })
}

/// Remove an account. Refused when it is the last remaining account, or when
/// `actor_id` is removing themselves: both must be visible refusals, not a
/// silent no-op.
pub async fn remove(pool: &DbPool, actor_id: i64, target_id: i64) -> Result<()> {
    if actor_id == target_id {
        return Err(Error::Secret(
            "you cannot remove your own account".to_string(),
        ));
    }
    let count: i64 = sqlx::query_scalar(&crate::db::q(pool, "SELECT COUNT(*) FROM web_accounts"))
        .fetch_one(pool)
        .await?;
    if count <= 1 {
        return Err(Error::Secret(
            "the last remaining account cannot be removed".to_string(),
        ));
    }
    let target = load_by_id(pool, target_id)
        .await?
        .ok_or_else(|| Error::Secret("no such account".to_string()))?;
    sqlx::query(&crate::db::q(
        pool,
        "DELETE FROM web_account_recovery_codes WHERE account_id = ?",
    ))
    .bind(target_id)
    .execute(pool)
    .await?;
    sqlx::query(&crate::db::q(pool, "DELETE FROM web_accounts WHERE id = ?"))
        .bind(target_id)
        .execute(pool)
        .await?;
    info!(
        username = target.username,
        account_id = target_id,
        "web account removed"
    );
    Ok(())
}

/// Replace the password (and rotate the session key with it, signing out every
/// browser for this account).
pub async fn set_password(pool: &DbPool, id: i64, password: &str) -> Result<Account> {
    let Some(current) = load_by_id(pool, id).await? else {
        return Err(Error::Secret("no such account".to_string()));
    };
    validate(&current.username, password)?;
    let password_hash = hash_password(password)?;
    let session_key = new_session_key()?;
    sqlx::query(&crate::db::q(
        pool,
        "UPDATE web_accounts SET password_hash = ?, session_key = ?, updated_at = ? WHERE id = ?",
    ))
    .bind(&password_hash)
    .bind(&session_key)
    .bind(chrono::Utc::now().to_rfc3339())
    .bind(id)
    .execute(pool)
    .await?;
    info!(username = current.username, "web account password changed");
    Ok(Account {
        password_hash,
        session_key,
        ..current
    })
}

/// Reject the credentials the setup and invite-accept forms must not accept.
/// Returned as an error the page renders verbatim, so the operator is told
/// which rule they broke.
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
        .map_err(|e| Error::Secret(format!("hashing the account password failed: {e}")))?
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

/// Begin (or restart) TOTP enrollment for `id`: generate a fresh secret, store
/// it encrypted but NOT yet enrolled, and return the `otpauth://` URI plus its
/// rendered QR code. Re-running this before confirmation simply replaces the
/// pending secret, so a reload during setup is harmless.
pub async fn begin_totp_enrollment(
    pool: &DbPool,
    key: &SecretsKey,
    id: i64,
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
        "UPDATE web_accounts SET totp_secret = ?, totp_enrolled_at = NULL, totp_last_step = NULL, \
         updated_at = ? WHERE id = ?",
    ))
    .bind(&ciphertext)
    .bind(chrono::Utc::now().to_rfc3339())
    .bind(id)
    .execute(pool)
    .await?;
    let qr_svg = qr_svg(&otpauth_url)?;
    Ok(PendingEnrollment {
        otpauth_url,
        qr_svg,
    })
}

/// Confirm a pending enrollment for `id` with a valid current code. On
/// success, marks the account enrolled and mints the one-time set of recovery
/// codes.
pub async fn confirm_totp_enrollment(
    pool: &DbPool,
    key: &SecretsKey,
    id: i64,
    code: &str,
) -> Result<Vec<String>> {
    let account = load_by_id(pool, id)
        .await?
        .ok_or_else(|| Error::Secret("no such account".to_string()))?;
    let ciphertext = account
        .totp_secret
        .ok_or_else(|| Error::Secret("no pending TOTP enrollment".to_string()))?;
    let secret = decode_stored_secret(key, &ciphertext)?;
    let totp = build_totp(&account.username, secret)?;
    let now = chrono::Utc::now();
    let Some(matched_step) = totp.check_current(code) else {
        return Err(Error::Secret("that code is not valid".to_string()));
    };
    let codes = generate_recovery_codes()?;
    let now_str = now.to_rfc3339();
    sqlx::query(&crate::db::q(
        pool,
        "UPDATE web_accounts SET totp_enrolled_at = ?, totp_last_step = ?, updated_at = ? \
         WHERE id = ?",
    ))
    .bind(&now_str)
    .bind(matched_step as i64)
    .bind(&now_str)
    .bind(id)
    .execute(pool)
    .await?;
    sqlx::query(&crate::db::q(
        pool,
        "DELETE FROM web_account_recovery_codes WHERE account_id = ?",
    ))
    .bind(id)
    .execute(pool)
    .await?;
    for plain in &codes {
        let hash = hash_password(plain)?;
        sqlx::query(&crate::db::q(
            pool,
            "INSERT INTO web_account_recovery_codes (account_id, code_hash, used_at, created_at) \
             VALUES (?, ?, NULL, ?)",
        ))
        .bind(id)
        .bind(&hash)
        .bind(&now_str)
        .execute(pool)
        .await?;
    }
    Ok(codes)
}

/// Verify a login-time code for `account`: either the current/adjacent-window
/// TOTP code, or an unused recovery code (consumed on success). Replay of the
/// same TOTP step is refused. A corrupt stored secret is a propagated error,
/// never a silent "not 2FA'd".
pub async fn verify_totp_or_recovery(
    pool: &DbPool,
    key: &SecretsKey,
    account: &Account,
    code: &str,
) -> Result<bool> {
    let code = code.trim();
    if code.is_empty() {
        return Ok(false);
    }
    if let Some(ciphertext) = &account.totp_secret {
        let secret = decode_stored_secret(key, ciphertext)?;
        let totp = build_totp(&account.username, secret)?;
        if let Some(matched_step) = totp.check_current(code) {
            let replay = account
                .totp_last_step
                .is_some_and(|last| last >= matched_step as i64);
            if !replay {
                sqlx::query(&crate::db::q(
                    pool,
                    "UPDATE web_accounts SET totp_last_step = ? WHERE id = ?",
                ))
                .bind(matched_step as i64)
                .bind(account.id)
                .execute(pool)
                .await?;
                return Ok(true);
            }
            warn!("rejected a replayed TOTP code for a web account login");
            return Ok(false);
        }
    }
    try_consume_recovery_code(pool, account.id, code).await
}

async fn try_consume_recovery_code(pool: &DbPool, account_id: i64, code: &str) -> Result<bool> {
    let rows: Vec<(i64, String)> = sqlx::query_as(&crate::db::q(
        pool,
        "SELECT id, code_hash FROM web_account_recovery_codes \
         WHERE account_id = ? AND used_at IS NULL",
    ))
    .bind(account_id)
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
                "UPDATE web_account_recovery_codes SET used_at = ? WHERE id = ?",
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

/// Clear TOTP enrollment and every recovery code for `id`, forcing
/// re-enrollment at the next login. Rotates the session key too, so an
/// in-progress session cannot keep riding an old grant past the reset.
pub async fn reset_totp(pool: &DbPool, id: i64) -> Result<Account> {
    let Some(current) = load_by_id(pool, id).await? else {
        return Err(Error::Secret("no such account".to_string()));
    };
    let session_key = new_session_key()?;
    sqlx::query(&crate::db::q(
        pool,
        "UPDATE web_accounts SET totp_secret = NULL, totp_enrolled_at = NULL, \
         totp_last_step = NULL, session_key = ?, updated_at = ? WHERE id = ?",
    ))
    .bind(&session_key)
    .bind(chrono::Utc::now().to_rfc3339())
    .bind(id)
    .execute(pool)
    .await?;
    sqlx::query(&crate::db::q(
        pool,
        "DELETE FROM web_account_recovery_codes WHERE account_id = ?",
    ))
    .bind(id)
    .execute(pool)
    .await?;
    Ok(Account {
        session_key,
        totp_secret: None,
        totp_enrolled_at: None,
        totp_last_step: None,
        ..current
    })
}

/// Mint a new single-use invite created by `created_by`, returning the plain
/// token (shown to the inviter exactly once) and the row id. Only the hash of
/// the token is stored.
pub async fn create_invite(pool: &DbPool, created_by: i64) -> Result<(String, i64)> {
    let mut buf = [0u8; INVITE_TOKEN_BYTES];
    getrandom::getrandom(&mut buf)
        .map_err(|e| Error::Secret(format!("reading random bytes for an invite token: {e}")))?;
    let token = hex::encode(buf);
    let token_hash = hash_password(&token)?;
    let now = chrono::Utc::now();
    let expires_at = now + chrono::Duration::hours(INVITE_TTL_HOURS);
    let id: i64 = sqlx::query_scalar(&crate::db::q(
        pool,
        "INSERT INTO web_invites (token_hash, created_by, created_at, expires_at, used_at) \
         VALUES (?, ?, ?, ?, NULL) RETURNING id",
    ))
    .bind(&token_hash)
    .bind(created_by)
    .bind(now.to_rfc3339())
    .bind(expires_at.to_rfc3339())
    .fetch_one(pool)
    .await?;
    Ok((token, id))
}

/// List outstanding (unused, unexpired) invites for the management page.
pub async fn list_invites(pool: &DbPool) -> Result<Vec<InviteSummary>> {
    let now = chrono::Utc::now().to_rfc3339();
    let rows: Vec<(i64, String, String, String)> = sqlx::query_as(&crate::db::q(
        pool,
        "SELECT i.id, a.username, i.created_at, i.expires_at FROM web_invites i \
         JOIN web_accounts a ON a.id = i.created_by \
         WHERE i.used_at IS NULL AND i.expires_at > ? ORDER BY i.id DESC",
    ))
    .bind(&now)
    .fetch_all(pool)
    .await?;
    Ok(rows
        .into_iter()
        .map(
            |(id, created_by_username, created_at, expires_at)| InviteSummary {
                id,
                created_by_username,
                created_at,
                expires_at,
            },
        )
        .collect())
}

/// Revoke an unused invite by id. A no-op (not an error) when it is already
/// used or gone, since the caller's intent (this invite must not work) is
/// already satisfied.
pub async fn revoke_invite(pool: &DbPool, id: i64) -> Result<()> {
    sqlx::query(&crate::db::q(
        pool,
        "DELETE FROM web_invites WHERE id = ? AND used_at IS NULL",
    ))
    .bind(id)
    .execute(pool)
    .await?;
    Ok(())
}

/// Look up a live (unused, unexpired) invite by its plaintext token, checking
/// the hash of every candidate row rather than looking it up by a derived key,
/// since the hash is a salted argon2id PHC string, not a lookup index.
pub async fn find_live_invite_by_token(pool: &DbPool, token: &str) -> Result<Option<Invite>> {
    let now = chrono::Utc::now().to_rfc3339();
    let rows: Vec<(i64, String, i64, String, String, Option<String>)> =
        sqlx::query_as(&crate::db::q(
            pool,
            "SELECT id, token_hash, created_by, created_at, expires_at, used_at \
             FROM web_invites WHERE used_at IS NULL AND expires_at > ?",
        ))
        .bind(&now)
        .fetch_all(pool)
        .await?;
    for (id, token_hash, created_by, created_at, expires_at, used_at) in rows {
        let parsed = PasswordHash::new(&token_hash)
            .map_err(|e| Error::Secret(format!("stored invite token hash is corrupt: {e}")))?;
        if Argon2::default()
            .verify_password(token.as_bytes(), &parsed)
            .is_ok()
        {
            return Ok(Some(Invite {
                id,
                created_by,
                created_at,
                expires_at,
                used_at,
            }));
        }
    }
    Ok(None)
}

/// Accept `invite`: create the account and mark the invite used in one
/// transaction, so a race between two acceptances of the same token can only
/// ever create one account.
pub async fn accept_invite(
    pool: &DbPool,
    invite: &Invite,
    username: &str,
    password: &str,
) -> Result<Account> {
    validate(username.trim(), password)?;
    let username_key = username.trim().to_lowercase();
    if load_by_username(pool, &username_key).await?.is_some() {
        return Err(Error::Secret(format!(
            "an account named '{}' already exists",
            username.trim()
        )));
    }
    let password_hash = hash_password(password)?;
    let session_key = new_session_key()?;
    let now = chrono::Utc::now().to_rfc3339();
    let mut tx = pool.begin().await?;
    let used: i64 = sqlx::query_scalar(&crate::db::q(
        pool,
        "SELECT COUNT(*) FROM web_invites WHERE id = ? AND used_at IS NULL",
    ))
    .bind(invite.id)
    .fetch_one(&mut *tx)
    .await?;
    if used == 0 {
        return Err(Error::Secret(
            "this invite has already been used".to_string(),
        ));
    }
    let id: i64 = sqlx::query_scalar(&crate::db::q(
        pool,
        "INSERT INTO web_accounts (username, username_key, password_hash, session_key, \
         created_at, updated_at, created_by) VALUES (?, ?, ?, ?, ?, ?, ?) RETURNING id",
    ))
    .bind(username.trim())
    .bind(&username_key)
    .bind(&password_hash)
    .bind(&session_key)
    .bind(&now)
    .bind(&now)
    .bind(invite.created_by)
    .fetch_one(&mut *tx)
    .await?;
    sqlx::query(&crate::db::q(
        pool,
        "UPDATE web_invites SET used_at = ? WHERE id = ?",
    ))
    .bind(&now)
    .bind(invite.id)
    .execute(&mut *tx)
    .await?;
    tx.commit().await?;
    info!(
        username = username.trim(),
        invited_by = invite.created_by,
        "web account created from an accepted invite"
    );
    Ok(Account {
        id,
        username: username.trim().to_string(),
        password_hash,
        session_key,
        totp_secret: None,
        totp_enrolled_at: None,
        totp_last_step: None,
    })
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

    fn current_totp_code(account: &Account, key: &SecretsKey) -> String {
        let ciphertext = account.totp_secret.clone().expect("pending secret");
        let secret = decode_stored_secret(key, &ciphertext).unwrap();
        let totp = build_totp(&account.username, secret).unwrap();
        totp.generate_current().to_string()
    }

    #[tokio::test]
    async fn a_fresh_instance_has_no_account() {
        let pool = mem_pool().await;
        assert!(!exists(&pool).await.unwrap());
        assert!(load_by_username(&pool, "admin").await.unwrap().is_none());
    }

    #[tokio::test]
    async fn create_then_verify_round_trips() {
        let pool = mem_pool().await;
        let created = create_first(&pool, "admin", "correct horse battery")
            .await
            .unwrap();
        assert_eq!(created.username, "admin");
        assert!(exists(&pool).await.unwrap());

        let loaded = load_by_username(&pool, "ADMIN")
            .await
            .unwrap()
            .expect("username lookup is case-insensitive");
        assert_eq!(loaded.username, "admin");
        assert!(loaded.verify_password("correct horse battery").unwrap());
        assert!(!loaded.verify_password("wrong horse battery").unwrap());
        assert_eq!(loaded.session_key(), created.session_key());
    }

    #[tokio::test]
    async fn the_password_is_never_stored_in_the_clear() {
        let pool = mem_pool().await;
        create_first(&pool, "admin", "correct horse battery")
            .await
            .unwrap();
        let stored: String = sqlx::query_scalar(&crate::db::q(
            &pool,
            "SELECT password_hash FROM web_accounts WHERE username_key = 'admin'",
        ))
        .fetch_one(&pool)
        .await
        .unwrap();
        assert!(stored.starts_with("$argon2id$"), "got {stored}");
        assert!(!stored.contains("correct horse battery"));
    }

    #[tokio::test]
    async fn only_the_first_account_can_use_create_first() {
        let pool = mem_pool().await;
        create_first(&pool, "admin", "correct horse battery")
            .await
            .unwrap();
        let err = create_first(&pool, "intruder", "another long password")
            .await
            .unwrap_err();
        assert!(err.to_string().contains("already exists"), "{err}");
        let loaded = load_by_username(&pool, "admin").await.unwrap().unwrap();
        assert!(loaded.verify_password("correct horse battery").unwrap());
    }

    #[tokio::test]
    async fn short_passwords_and_empty_usernames_are_refused() {
        let pool = mem_pool().await;
        let err = create_first(&pool, "admin", "short").await.unwrap_err();
        assert!(err.to_string().contains("at least"), "{err}");
        let err = create_first(&pool, "   ", "correct horse battery")
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
        let first = create_first(&pool, "admin", "correct horse battery")
            .await
            .unwrap();
        let second = set_password(&pool, first.id, "a different long password")
            .await
            .unwrap();
        assert!(second.verify_password("a different long password").unwrap());
        assert_ne!(first.session_key(), second.session_key());
    }

    #[tokio::test]
    async fn invite_accept_creates_a_distinct_account() {
        let pool = mem_pool().await;
        let admin = create_first(&pool, "admin", "correct horse battery")
            .await
            .unwrap();
        let (token, _id) = create_invite(&pool, admin.id).await.unwrap();
        let invite = find_live_invite_by_token(&pool, &token)
            .await
            .unwrap()
            .expect("the freshly minted invite is live");
        let second = accept_invite(&pool, &invite, "second", "another long password")
            .await
            .unwrap();
        assert_ne!(second.id, admin.id);
        assert_ne!(second.session_key(), admin.session_key());

        // Single-use: the same token no longer resolves to a live invite.
        assert!(find_live_invite_by_token(&pool, &token)
            .await
            .unwrap()
            .is_none());
    }

    #[tokio::test]
    async fn an_unknown_or_expired_invite_token_is_refused() {
        let pool = mem_pool().await;
        assert!(find_live_invite_by_token(&pool, "not-a-real-token")
            .await
            .unwrap()
            .is_none());
    }

    #[tokio::test]
    async fn a_used_invite_cannot_be_accepted_twice() {
        let pool = mem_pool().await;
        let admin = create_first(&pool, "admin", "correct horse battery")
            .await
            .unwrap();
        let (token, _id) = create_invite(&pool, admin.id).await.unwrap();
        let invite = find_live_invite_by_token(&pool, &token)
            .await
            .unwrap()
            .unwrap();
        accept_invite(&pool, &invite, "second", "another long password")
            .await
            .unwrap();
        // Re-using the same already-used invite row is refused.
        let err = accept_invite(&pool, &invite, "third", "yet another password")
            .await
            .unwrap_err();
        assert!(err.to_string().contains("already been used"), "{err}");
    }

    #[tokio::test]
    async fn revoking_an_invite_makes_it_unresolvable() {
        let pool = mem_pool().await;
        let admin = create_first(&pool, "admin", "correct horse battery")
            .await
            .unwrap();
        let (token, id) = create_invite(&pool, admin.id).await.unwrap();
        revoke_invite(&pool, id).await.unwrap();
        assert!(find_live_invite_by_token(&pool, &token)
            .await
            .unwrap()
            .is_none());
    }

    #[tokio::test]
    async fn removing_the_last_account_is_refused() {
        let pool = mem_pool().await;
        let admin = create_first(&pool, "admin", "correct horse battery")
            .await
            .unwrap();
        let err = remove(&pool, admin.id, admin.id).await.unwrap_err();
        // Self-removal is checked first, but the last-account rule holds too:
        // use a distinct "actor" id (not a real account) to reach that check.
        assert!(err.to_string().contains("own account"), "{err}");
        let err = remove(&pool, 999, admin.id).await.unwrap_err();
        assert!(err.to_string().contains("last remaining"), "{err}");
    }

    #[tokio::test]
    async fn removing_yourself_is_refused() {
        let pool = mem_pool().await;
        let admin = create_first(&pool, "admin", "correct horse battery")
            .await
            .unwrap();
        let (token, _id) = create_invite(&pool, admin.id).await.unwrap();
        let invite = find_live_invite_by_token(&pool, &token)
            .await
            .unwrap()
            .unwrap();
        let second = accept_invite(&pool, &invite, "second", "another long password")
            .await
            .unwrap();
        let err = remove(&pool, second.id, second.id).await.unwrap_err();
        assert!(err.to_string().contains("own account"), "{err}");
    }

    #[tokio::test]
    async fn removing_an_account_deletes_it() {
        let pool = mem_pool().await;
        let admin = create_first(&pool, "admin", "correct horse battery")
            .await
            .unwrap();
        let (token, _id) = create_invite(&pool, admin.id).await.unwrap();
        let invite = find_live_invite_by_token(&pool, &token)
            .await
            .unwrap()
            .unwrap();
        let second = accept_invite(&pool, &invite, "second", "another long password")
            .await
            .unwrap();
        remove(&pool, admin.id, second.id).await.unwrap();
        assert!(load_by_id(&pool, second.id).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn a_fresh_account_is_not_totp_enrolled() {
        let pool = mem_pool().await;
        let account = create_first(&pool, "admin", "correct horse battery")
            .await
            .unwrap();
        assert!(!account.totp_enrolled());
    }

    #[tokio::test]
    async fn enrollment_round_trips_and_issues_recovery_codes() {
        let pool = mem_pool().await;
        let key = test_key();
        let account = create_first(&pool, "admin", "correct horse battery")
            .await
            .unwrap();

        let pending = begin_totp_enrollment(&pool, &key, account.id, "admin")
            .await
            .unwrap();
        assert!(pending.otpauth_url.starts_with("otpauth://totp/"));
        assert!(pending.qr_svg.starts_with("<svg"));

        let account = load_by_id(&pool, account.id).await.unwrap().unwrap();
        assert!(!account.totp_enrolled());
        let code = current_totp_code(&account, &key);

        let codes = confirm_totp_enrollment(&pool, &key, account.id, &code)
            .await
            .unwrap();
        assert_eq!(codes.len(), RECOVERY_CODE_COUNT);
        assert_eq!(
            codes.iter().collect::<std::collections::HashSet<_>>().len(),
            RECOVERY_CODE_COUNT
        );

        let account = load_by_id(&pool, account.id).await.unwrap().unwrap();
        assert!(account.totp_enrolled());
    }

    #[tokio::test]
    async fn a_totp_code_cannot_be_replayed() {
        let pool = mem_pool().await;
        let key = test_key();
        let account = create_first(&pool, "admin", "correct horse battery")
            .await
            .unwrap();
        begin_totp_enrollment(&pool, &key, account.id, "admin")
            .await
            .unwrap();
        let account = load_by_id(&pool, account.id).await.unwrap().unwrap();
        let enroll_code = current_totp_code(&account, &key);
        confirm_totp_enrollment(&pool, &key, account.id, &enroll_code)
            .await
            .unwrap();

        let account = load_by_id(&pool, account.id).await.unwrap().unwrap();
        assert!(
            !verify_totp_or_recovery(&pool, &key, &account, &enroll_code)
                .await
                .unwrap()
        );

        let ciphertext = account.totp_secret.clone().unwrap();
        let secret = decode_stored_secret(&key, &ciphertext).unwrap();
        let totp = build_totp(&account.username, secret).unwrap();
        let next_code = totp
            .generate(account.totp_last_step.unwrap() as u64 * 30 + 30)
            .to_string();

        let account = load_by_id(&pool, account.id).await.unwrap().unwrap();
        assert!(verify_totp_or_recovery(&pool, &key, &account, &next_code)
            .await
            .unwrap());
        let account = load_by_id(&pool, account.id).await.unwrap().unwrap();
        assert!(!verify_totp_or_recovery(&pool, &key, &account, &next_code)
            .await
            .unwrap());
    }

    #[tokio::test]
    async fn a_recovery_code_works_exactly_once() {
        let pool = mem_pool().await;
        let key = test_key();
        let account = create_first(&pool, "admin", "correct horse battery")
            .await
            .unwrap();
        begin_totp_enrollment(&pool, &key, account.id, "admin")
            .await
            .unwrap();
        let account = load_by_id(&pool, account.id).await.unwrap().unwrap();
        let code = current_totp_code(&account, &key);
        let codes = confirm_totp_enrollment(&pool, &key, account.id, &code)
            .await
            .unwrap();
        let recovery = &codes[0];

        let account = load_by_id(&pool, account.id).await.unwrap().unwrap();
        assert!(verify_totp_or_recovery(&pool, &key, &account, recovery)
            .await
            .unwrap());
        let account = load_by_id(&pool, account.id).await.unwrap().unwrap();
        assert!(!verify_totp_or_recovery(&pool, &key, &account, recovery)
            .await
            .unwrap());
    }

    #[tokio::test]
    async fn two_accounts_get_distinct_totp_enrollment() {
        let pool = mem_pool().await;
        let key = test_key();
        let admin = create_first(&pool, "admin", "correct horse battery")
            .await
            .unwrap();
        let (token, _id) = create_invite(&pool, admin.id).await.unwrap();
        let invite = find_live_invite_by_token(&pool, &token)
            .await
            .unwrap()
            .unwrap();
        let second = accept_invite(&pool, &invite, "second", "another long password")
            .await
            .unwrap();

        begin_totp_enrollment(&pool, &key, admin.id, "admin")
            .await
            .unwrap();
        let reloaded_second = load_by_id(&pool, second.id).await.unwrap().unwrap();
        assert!(
            !reloaded_second.totp_enrolled(),
            "enrolling one account must not affect another"
        );
    }
}
