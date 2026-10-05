//! Phase 5: bearer-token auth for the daemon's TCP-exposed HTTP
//! surface.
//!
//! The daemon binds its axum router to loopback by default. The external
//! reverse proxy exposes that surface so a `/webhooks/forgejo`
//! route is reachable, which also exposes the web UI and `/api/*`. This module
//! gates that surface behind a bearer token whenever the effective bind is
//! non-loopback (or auth is explicitly required on loopback for testing).
//!
//! Exemptions: the `/webhooks/` prefix is never gated. Forgejo cannot send a
//! bearer token, and the webhook carries its own HMAC verification.
//!
//! Two credentials answer that gate, and they serve different callers:
//!
//! - `Authorization: Bearer <token>`, where the token is an encrypted secret
//!   named by `web.auth_token_ref`, resolved once at daemon startup and never
//!   read from the config file in plaintext. This is the API clients' path.
//! - The browser login, which since the first-run setup page is one of any
//!   number of [`crate::web_admin`] accounts in the DB: a username plus an
//!   argon2id hash, created from `/setup` (the first account) or from an
//!   accepted invite (every account after). Where no account exists at all,
//!   `web.auth_username` plus the token plaintext still works as the login,
//!   so existing host deployments are unchanged.
//!
//! When auth is required and NEITHER credential exists, the instance is
//! unconfigured: every request is steered to `/setup` (browsers) or answered
//! `503` (everything else) until an account is created. That replaces the
//! older behaviour of refusing to bind the TCP listener at all, which was
//! unrecoverable in a container: the operator could not reach the UI that would
//! have configured the credential the daemon was demanding. The surface is never
//! served unauthenticated either way.
//!
//! The session cookie carries the account id as a plaintext claim alongside the
//! expiry (`"<account id>.<expiry>.<hmac>"`), signed with that account's own
//! rotating `session_key`; a sentinel id of `0` is the legacy
//! `auth_username`/token pair, signed with the bearer token instead. Carrying
//! the id lets `require_token` look the account up fresh on every request
//! rather than caching a single administrator, which is what makes multiple
//! concurrently valid accounts (and multiple accounts mid-2FA at once) work.

use std::net::IpAddr;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::db::DbPool;
use crate::web_admin::Account;
use askama::Template;
use axum::{
    extract::{Extension, Form, Path, Request, State},
    http::{header, HeaderMap, HeaderValue, Method, StatusCode},
    middleware::Next,
    response::{Html, IntoResponse, Redirect, Response},
    routing::{get, post},
    Router,
};
use hmac::{Hmac, Mac};
use serde::Deserialize;
use sha2::Sha256;

use tracing::{error, warn};

use crate::config::WebConfig;
use crate::error::Result;
use crate::secrets::SecretsKey;

/// Session cookie name set on a successful form login and accepted by the gate
/// in lieu of a bearer header.
const SESSION_COOKIE: &str = "claude_run_session";
/// How long a login session stays valid before the browser must sign in again.
const SESSION_TTL_SECS: u64 = 12 * 3600;

/// Cookie proving a password check has already passed and a TOTP (or recovery)
/// code is still owed before a session is granted.
const PENDING_2FA_COOKIE: &str = "claude_run_2fa_pending";
/// How long the operator has to complete the second factor after a correct
/// password before having to sign in again from scratch.
const PENDING_2FA_TTL_SECS: u64 = 5 * 60;
/// Wrong-code attempts allowed against one pending cookie before it is
/// discarded and the operator is sent back to `/login`.
const PENDING_2FA_MAX_ATTEMPTS: u32 = 5;

/// Sentinel session account id for the legacy `web.auth_username` plus
/// `auth_token_ref` login, which has no row in `web_accounts`.
const LEGACY_ACCOUNT_ID: i64 = 0;

/// Holds whatever credentials this run has: the bearer token (when
/// `auth_token_ref` resolved), the legacy form-login username, and the pool
/// every account lookup reads through. Cheaply cloned into the middleware.
#[derive(Clone)]
pub struct TokenGate {
    /// `None` when no `auth_token_ref` is configured or it did not resolve.
    /// Bearer auth is then simply unavailable; the browser login still works.
    token: Option<Arc<String>>,
    /// `Some` when `web.auth_username` is set: the legacy login username, used
    /// only while no DB account exists.
    username: Option<Arc<String>>,
    /// Pool every account lookup (login, session verification, setup, invite
    /// acceptance, management) reads and writes through.
    pool: Option<DbPool>,
    /// The key that encrypts/decrypts a stored TOTP secret. `None` when it
    /// could not be loaded, which makes enrollment and 2FA login unavailable
    /// (surfaced as an error, never silently skipped).
    secrets_key: Option<Arc<SecretsKey>>,
}

impl TokenGate {
    /// Bearer-only gate (no form login, no DB accounts). Used by tests and any
    /// caller that does not configure a username.
    pub fn new(token: String) -> Self {
        Self {
            token: Some(Arc::new(token)),
            username: None,
            pool: None,
            secrets_key: None,
        }
    }

    /// Gate with the legacy browser login form enabled for `username`.
    pub fn with_login(token: String, username: String) -> Self {
        Self {
            username: Some(Arc::new(username)),
            ..Self::new(token)
        }
    }

    /// Attach the pool every account operation reads and writes through.
    pub fn with_accounts(mut self, pool: DbPool) -> Self {
        self.pool = Some(pool);
        self
    }

    /// Attach the secrets key TOTP enrollment and login encrypt/decrypt through.
    pub fn with_secrets_key(mut self, key: Arc<SecretsKey>) -> Self {
        self.secrets_key = Some(key);
        self
    }

    /// A gate with no credential at all: the instance is unconfigured and every
    /// request is steered to `/setup`.
    pub fn unconfigured(pool: DbPool) -> Self {
        Self {
            token: None,
            username: None,
            pool: Some(pool),
            secrets_key: None,
        }
    }

    /// Whether any credential can authenticate a request. `false` means the
    /// instance still needs first-run setup.
    async fn configured(&self) -> bool {
        if self.token.is_some() {
            return true;
        }
        match &self.pool {
            Some(pool) => crate::web_admin::exists(pool).await.unwrap_or(false),
            None => false,
        }
    }

    /// Whether a browser should be sent to a form rather than a bare 401. True
    /// once there is any form to send them to: a configured legacy username, a
    /// pool that can hold accounts, or an unconfigured instance needing setup.
    async fn has_browser_login(&self) -> bool {
        self.username.is_some() || self.pool.is_some() || !self.configured().await
    }
}

fn now_unix() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

/// Sign a session for `account_id` and `expiry` (unix seconds) under `key`,
/// returning the cookie value `"<account_id>.<expiry>.<hex-hmac>"`. The HMAC
/// keys on the account's own `session_key` (or the bearer token for the legacy
/// sentinel account id), so rotating either invalidates every outstanding
/// session signed with it.
fn sign_session(key: &str, account_id: i64, expiry: u64) -> String {
    let payload = format!("{account_id}.{expiry}");
    let mut mac =
        Hmac::<Sha256>::new_from_slice(key.as_bytes()).expect("HMAC accepts any key length");
    mac.update(payload.as_bytes());
    format!("{payload}.{}", hex::encode(mac.finalize().into_bytes()))
}

/// Parse the plaintext `(account_id, expiry)` claims out of a session cookie
/// value, with no signature check: like a JWT's claims, these are read before
/// the matching key is known, then verified by recomputing the signature.
fn parse_session_claims(cookie_val: &str) -> Option<(i64, u64)> {
    let mut parts = cookie_val.splitn(3, '.');
    let account_id: i64 = parts.next()?.parse().ok()?;
    let expiry: u64 = parts.next()?.parse().ok()?;
    parts.next()?;
    Some((account_id, expiry))
}

/// True when `cookie_val` is a non-expired session for `account_id` signed by
/// `key`.
fn verify_session(key: &str, account_id: i64, expiry: u64, cookie_val: &str) -> bool {
    if now_unix() > expiry {
        return false;
    }
    constant_time_eq(
        sign_session(key, account_id, expiry).as_bytes(),
        cookie_val.as_bytes(),
    )
}

/// The `Set-Cookie` value for a fresh session: HttpOnly + Secure + SameSite=Lax,
/// scoped to the whole site, expiring with the session TTL.
fn session_set_cookie(key: &str, account_id: i64) -> String {
    let value = sign_session(key, account_id, now_unix() + SESSION_TTL_SECS);
    format!(
        "{SESSION_COOKIE}={value}; HttpOnly; Secure; SameSite=Lax; Path=/; Max-Age={SESSION_TTL_SECS}"
    )
}

/// Read a named cookie's value out of a `Cookie` header, if present.
fn cookie_value(headers: &HeaderMap, name: &str) -> Option<String> {
    let raw = headers.get(header::COOKIE)?.to_str().ok()?;
    raw.split(';').map(str::trim).find_map(|kv| {
        let (k, v) = kv.split_once('=')?;
        (k == name).then(|| v.to_string())
    })
}

/// Read the session cookie value from a request's `Cookie` header, if present.
fn session_cookie(request: &Request) -> Option<String> {
    cookie_value(request.headers(), SESSION_COOKIE)
}

/// Sign a pending-2FA cookie: password already checked for `account_id`, a
/// second factor is still owed. `attempts` is the number of wrong codes
/// already tried against it, so a cap can be enforced without a DB round trip.
fn sign_pending_2fa(key: &str, account_id: i64, expiry: u64, attempts: u32) -> String {
    let payload = format!("{account_id}.{expiry}.{attempts}");
    let mut mac =
        Hmac::<Sha256>::new_from_slice(key.as_bytes()).expect("HMAC accepts any key length");
    mac.update(payload.as_bytes());
    format!("{payload}.{}", hex::encode(mac.finalize().into_bytes()))
}

/// Parse the plaintext `(account_id, expiry, attempts)` claims out of a
/// pending-2FA cookie, with no signature check.
fn parse_pending_claims(cookie_val: &str) -> Option<(i64, u64, u32)> {
    let mut parts = cookie_val.splitn(4, '.');
    let account_id: i64 = parts.next()?.parse().ok()?;
    let expiry: u64 = parts.next()?.parse().ok()?;
    let attempts: u32 = parts.next()?.parse().ok()?;
    parts.next()?;
    Some((account_id, expiry, attempts))
}

/// Verify a pending-2FA cookie for `account_id` signed by `key`, given its
/// already-parsed `expiry` and `attempts` claims.
fn verify_pending_2fa(
    key: &str,
    account_id: i64,
    expiry: u64,
    attempts: u32,
    cookie_val: &str,
) -> bool {
    if now_unix() > expiry || attempts >= PENDING_2FA_MAX_ATTEMPTS {
        return false;
    }
    constant_time_eq(
        sign_pending_2fa(key, account_id, expiry, attempts).as_bytes(),
        cookie_val.as_bytes(),
    )
}

/// The `Set-Cookie` value for a fresh (or re-issued, incremented) pending-2FA
/// cookie.
fn pending_2fa_set_cookie(key: &str, account_id: i64, attempts: u32) -> String {
    let value = sign_pending_2fa(key, account_id, now_unix() + PENDING_2FA_TTL_SECS, attempts);
    format!(
        "{PENDING_2FA_COOKIE}={value}; HttpOnly; Secure; SameSite=Lax; Path=/; \
         Max-Age={PENDING_2FA_TTL_SECS}"
    )
}

/// The `Set-Cookie` value that clears a pending-2FA cookie.
fn pending_2fa_clear_cookie() -> String {
    format!("{PENDING_2FA_COOKIE}=; HttpOnly; Secure; SameSite=Lax; Path=/; Max-Age=0")
}

/// Whether the request looks like a browser navigation that should be redirected
/// to the login page rather than answered with a bare 401.
fn wants_html(request: &Request) -> bool {
    request
        .headers()
        .get(header::ACCEPT)
        .and_then(|v| v.to_str().ok())
        .is_some_and(|a| a.contains("text/html"))
}

/// True when the TCP surface must require a token: any non-loopback bind, or an
/// explicit `require_auth_on_loopback` for local testing of the gate.
pub fn auth_required(web: &WebConfig) -> bool {
    web.require_auth_on_loopback || !is_loopback_bind(&web.bind)
}

/// Whether `bind` names a loopback-only address. `127.0.0.0/8`, `::1`, and the
/// literal `localhost` are loopback; `0.0.0.0`, `::`, and any routable address
/// are not. An unparseable, non-`localhost` value is treated as non-loopback so
/// the gate fails safe (require auth) rather than open.
pub fn is_loopback_bind(bind: &str) -> bool {
    let b = bind.trim();
    if b.eq_ignore_ascii_case("localhost") {
        return true;
    }
    match b.parse::<IpAddr>() {
        Ok(ip) => ip.is_loopback(),
        Err(_) => false,
    }
}

/// Resolve the gate for this daemon run from the web config, the secrets store
/// and the DB accounts.
///
/// - `Ok(None)`: auth not required (loopback bind); serve the TCP surface
///   unauthenticated, as before.
/// - `Ok(Some(gate))`: auth required. The gate carries whichever credentials
///   exist; when it carries none, it serves first-run setup and authenticates
///   nothing else.
/// - `Err(_)`: the DB could not be read at all, which is not a credential
///   problem and is left to the caller.
///
/// A configured `auth_token_ref` that fails to resolve (no key file, unknown
/// secret, empty value) is a `warn`, not a hard failure: bearer auth is then
/// unavailable, and the browser login or first-run setup still gets the operator
/// in. Refusing to bind at all was the older behaviour and it locked operators
/// out of containers, where the CLI that would fix the secret is not the
/// interface they have.
pub async fn resolve_gate(web: &WebConfig, pool: &DbPool) -> Result<Option<TokenGate>> {
    if !auth_required(web) {
        return Ok(None);
    }
    let has_account = crate::web_admin::exists(pool).await?;
    let token = resolve_token(web, pool).await;
    let secrets_key = match crate::secrets::SecretsKey::load() {
        Ok(k) => Some(Arc::new(k)),
        Err(e) => {
            warn!(
                error = %e,
                "the secrets key could not be loaded; TOTP enrollment and 2FA login are \
                 unavailable this run"
            );
            None
        }
    };

    let mut gate = match token {
        Some(token) => {
            let gate = match web.auth_username.as_deref() {
                Some(u) if !u.trim().is_empty() => {
                    TokenGate::with_login(token, u.trim().to_string())
                }
                _ => TokenGate::new(token),
            };
            gate.with_accounts(pool.clone())
        }
        None if has_account => TokenGate::unconfigured(pool.clone()),
        None => {
            warn!(
                "no web credential configured; serving first-run setup at /setup until an \
                 account is created. Every other request is refused until then."
            );
            TokenGate::unconfigured(pool.clone())
        }
    };
    if let Some(key) = secrets_key {
        gate = gate.with_secrets_key(key);
    }
    Ok(Some(gate))
}

/// Resolve the bearer token from `auth_token_ref`, or `None` with the reason
/// logged. Never fatal: see [`resolve_gate`].
async fn resolve_token(web: &WebConfig, pool: &DbPool) -> Option<String> {
    let name = web.auth_token_ref.as_deref()?;
    let key = match crate::secrets::SecretsKey::load() {
        Ok(k) => k,
        Err(e) => {
            warn!(
                secret = name,
                error = %e,
                "web.auth_token_ref is set but the secrets key could not be loaded; bearer auth is \
                 unavailable this run (the browser login is unaffected)"
            );
            return None;
        }
    };
    match crate::secrets::resolve_secret(pool, &key, name).await {
        Ok(t) if t.is_empty() => {
            warn!(
                secret = name,
                "web.auth_token_ref names an empty secret; refusing to gate with it, so bearer \
                 auth is unavailable this run"
            );
            None
        }
        Ok(t) => Some(t),
        Err(e) => {
            warn!(
                secret = name,
                error = %e,
                "web.auth_token_ref could not be resolved; bearer auth is unavailable this run"
            );
            None
        }
    }
}

/// Wrap `router` so every request must carry either `Authorization: Bearer
/// <token>` or a valid session cookie, except requests under the `/webhooks/`
/// prefix, the login routes, and invite acceptance. When the gate has a
/// configured username, the `/login` (+ `/logout`) form routes are mounted so a
/// browser / password manager can sign in.
pub fn protect(router: Router, gate: TokenGate) -> Router {
    // `/login`, `/logout`, `/setup` and `/invite/*` are always mounted and
    // always reachable: which of them is useful depends on state the gate
    // reads per request (an account can appear at runtime), so deciding at
    // mount time would require a restart to pick that up.
    let auth_routes = Router::new()
        .route("/login", get(login_page).post(login_submit))
        .route("/login/2fa", get(login_totp_page).post(login_totp_submit))
        .route("/logout", get(logout))
        .route("/setup", get(setup_page).post(setup_submit))
        .route(
            "/account/2fa/setup",
            get(totp_setup_page).post(totp_setup_submit),
        )
        .route("/invite/:token", get(invite_page).post(invite_submit))
        .route("/settings/accounts", get(accounts_page))
        .route("/settings/accounts/invite", post(create_invite_submit))
        .route("/settings/accounts/remove", post(remove_account_submit))
        .route("/settings/accounts/revoke", post(revoke_invite_submit))
        .with_state(gate.clone());
    router
        .merge(auth_routes)
        .layer(axum::middleware::from_fn_with_state(gate, require_token))
}

/// Axum middleware enforcing a bearer token or a session cookie. Exempts the
/// webhook prefix, the auth routes, and invite acceptance. An unauthenticated
/// browser navigation (GET accepting HTML) is redirected to the form it needs
/// (`/setup` before the instance is configured, `/login` after); everything
/// else gets a 401, or a 503 while unconfigured because there is no credential
/// to present yet. On a valid session the resolved [`Account`] is attached to
/// the request as an extension for downstream handlers; the legacy sentinel
/// session carries none.
async fn require_token(State(gate): State<TokenGate>, request: Request, next: Next) -> Response {
    // The webhook route authenticates itself via HMAC and Forgejo cannot send a
    // bearer token, so it is never gated here. The auth routes (and invite
    // acceptance) must be reachable unauthenticated or there is no way in.
    let path = request.uri().path();
    if path.starts_with("/webhooks/")
        || path == "/login"
        || path == "/login/2fa"
        || path == "/logout"
        || path == "/setup"
        || path.starts_with("/invite/")
    {
        return next.run(request).await;
    }

    // Unconfigured: nothing can authenticate, so say that plainly instead of
    // rejecting a credential the operator has no way to hold yet.
    if !gate.configured().await {
        if request.method() == Method::GET && wants_html(&request) {
            return Redirect::to("/setup").into_response();
        }
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            "claude-run has no account yet; open /setup in a browser to create one",
        )
            .into_response();
    }

    if let Some(expected) = gate.token.as_ref() {
        if let Some(presented) = bearer_token(&request) {
            if constant_time_eq(presented.as_bytes(), expected.as_bytes()) {
                return next.run(request).await;
            }
        }
    }
    if let Some(cookie) = session_cookie(&request) {
        if let Some((account_id, expiry)) = parse_session_claims(&cookie) {
            if account_id == LEGACY_ACCOUNT_ID {
                if let Some(token) = gate.token.as_ref() {
                    if verify_session(token, account_id, expiry, &cookie) {
                        return next.run(request).await;
                    }
                }
            } else if let Some(pool) = gate.pool.as_ref() {
                if let Ok(Some(account)) = crate::web_admin::load_by_id(pool, account_id).await {
                    if verify_session(account.session_key(), account_id, expiry, &cookie) {
                        // A session belonging to an account who has not yet
                        // completed TOTP enrollment may load nothing else:
                        // every other page (and every non-GET/non-HTML
                        // request) is refused until enrollment is confirmed.
                        // `/account/2fa/setup` and its own static assets are
                        // the only exceptions, or there would be no way to
                        // reach the page that fixes it.
                        if !account.totp_enrolled() && !totp_setup_exempt(path) {
                            if request.method() == Method::GET && wants_html(&request) {
                                return Redirect::to("/account/2fa/setup").into_response();
                            }
                            return (
                                StatusCode::FORBIDDEN,
                                "TOTP enrollment is required before this instance can be used",
                            )
                                .into_response();
                        }
                        let mut request = request;
                        request.extensions_mut().insert(account);
                        return next.run(request).await;
                    }
                }
            }
        }
    }
    // Send browsers to the login form (when there is one) instead of a bare 401,
    // so a password manager can complete the sign-in. API clients still get 401.
    if gate.has_browser_login().await && request.method() == Method::GET && wants_html(&request) {
        return Redirect::to("/login").into_response();
    }
    (
        StatusCode::UNAUTHORIZED,
        [(header::WWW_AUTHENTICATE, "Bearer")],
        "missing or invalid bearer token",
    )
        .into_response()
}

/// Paths an unenrolled but authenticated session may still reach: the
/// enrollment page itself and the static assets it renders with.
fn totp_setup_exempt(path: &str) -> bool {
    path == "/account/2fa/setup" || path.starts_with("/static/")
}

#[derive(Template)]
#[template(path = "login.html")]
struct LoginTpl {
    error: Option<String>,
}

#[derive(Template)]
#[template(path = "login_2fa.html")]
struct LoginTotpTpl {
    error: Option<String>,
}

#[derive(Template)]
#[template(path = "totp_setup.html")]
struct TotpSetupTpl {
    error: Option<String>,
    otpauth_url: String,
    qr_svg: String,
}

#[derive(Template)]
#[template(path = "totp_recovery_codes.html")]
struct TotpRecoveryCodesTpl {
    codes: Vec<String>,
}

#[derive(Template)]
#[template(path = "setup.html")]
struct SetupTpl {
    error: Option<String>,
    min_password_len: usize,
}

#[derive(Template)]
#[template(path = "invite_accept.html")]
struct InviteAcceptTpl {
    token: String,
    error: Option<String>,
    min_password_len: usize,
}

#[derive(Template)]
#[template(path = "accounts.html")]
struct AccountsTpl {
    version: &'static str,
    accounts: Vec<crate::web_admin::AccountSummary>,
    invites: Vec<crate::web_admin::InviteSummary>,
    current_id: i64,
    error: Option<String>,
    new_invite_link: Option<String>,
    invite_sent_to: Option<String>,
    email_configured: bool,
}

#[derive(Deserialize)]
struct LoginForm {
    username: String,
    password: String,
}

#[derive(Deserialize)]
struct LoginTotpForm {
    code: String,
}

#[derive(Deserialize)]
struct TotpConfirmForm {
    code: String,
}

#[derive(Deserialize)]
struct SetupForm {
    username: String,
    password: String,
    confirm: String,
}

#[derive(Deserialize)]
struct InviteAcceptForm {
    username: String,
    password: String,
    confirm: String,
}

#[derive(Deserialize)]
struct RemoveAccountForm {
    id: i64,
}

#[derive(Deserialize)]
struct RevokeInviteForm {
    id: i64,
}

/// GET `/login`: render the sign-in form, or bounce to setup when the instance
/// has no credential to sign in with.
async fn login_page(State(gate): State<TokenGate>) -> Response {
    if !gate.configured().await {
        return Redirect::to("/setup").into_response();
    }
    render_login(None, StatusCode::OK)
}

/// POST `/login`: verify the credentials, mint a session cookie, and redirect to
/// the dashboard. A DB account matching the username wins when any account
/// exists; otherwise the pre-setup pair (`web.auth_username` plus the
/// `auth_token_ref` plaintext) still authenticates, so existing host
/// deployments keep working. On mismatch, re-render with a 401 and a generic
/// error naming neither field.
async fn login_submit(State(gate): State<TokenGate>, Form(form): Form<LoginForm>) -> Response {
    if !gate.configured().await {
        return Redirect::to("/setup").into_response();
    }
    let mut matched: Option<Account> = None;
    let mut ok = false;
    let any_accounts = match gate.pool.as_ref() {
        Some(pool) => crate::web_admin::exists(pool).await.unwrap_or(false),
        None => false,
    };
    if any_accounts {
        if let Some(pool) = gate.pool.as_ref() {
            if let Ok(Some(account)) =
                crate::web_admin::load_by_username(pool, &form.username).await
            {
                match account.verify_password(&form.password) {
                    Ok(true) => {
                        ok = true;
                        matched = Some(account);
                    }
                    Ok(false) => {}
                    Err(e) => {
                        // A corrupt stored hash is an operator problem, not a
                        // wrong password: log the cause rather than let it
                        // read as a typo.
                        warn!(error = %e, "account password hash could not be parsed; login refused");
                    }
                }
            }
        }
    } else if let (Some(username), Some(token)) = (gate.username.as_ref(), gate.token.as_ref()) {
        ok = constant_time_eq(form.username.as_bytes(), username.as_bytes())
            && constant_time_eq(form.password.as_bytes(), token.as_bytes());
    }
    if !ok {
        return render_login(
            Some("Invalid username or password.".to_string()),
            StatusCode::UNAUTHORIZED,
        );
    }
    // An enrolled account owes a second factor before a session is granted; an
    // unenrolled one goes straight to a session, and the enrolled-check in
    // `require_token` is what then forces enrollment before anything else
    // loads.
    if let Some(account) = matched {
        if account.totp_enrolled() {
            let mut resp = Redirect::to("/login/2fa").into_response();
            if let Ok(v) = HeaderValue::from_str(&pending_2fa_set_cookie(
                account.session_key(),
                account.id,
                0,
            )) {
                resp.headers_mut().insert(header::SET_COOKIE, v);
            }
            return resp;
        }
        return redirect_with_session("/", account.session_key(), account.id);
    }
    let Some(token) = gate.token.as_ref() else {
        return render_login(
            Some("No session key is available; check the daemon log.".to_string()),
            StatusCode::INTERNAL_SERVER_ERROR,
        );
    };
    redirect_with_session("/", token, LEGACY_ACCOUNT_ID)
}

/// GET `/login/2fa`: render the second-factor form, or bounce to `/login` when
/// there is no live pending-2FA cookie to redeem (a direct visit, an expired
/// cookie, or an account that is not enrolled).
async fn login_totp_page(State(gate): State<TokenGate>, request: Request) -> Response {
    if !gate.configured().await {
        return Redirect::to("/setup").into_response();
    }
    let Some(account) = pending_2fa_account(&gate, request.headers()).await else {
        return Redirect::to("/login").into_response();
    };
    if !account.totp_enrolled() {
        return Redirect::to("/login").into_response();
    }
    render_login_totp(None, StatusCode::OK)
}

/// Resolve the account named by a live pending-2FA cookie, verifying its
/// signature against that account's own key.
async fn pending_2fa_account(gate: &TokenGate, headers: &HeaderMap) -> Option<Account> {
    let cookie = cookie_value(headers, PENDING_2FA_COOKIE)?;
    let (account_id, expiry, attempts) = parse_pending_claims(&cookie)?;
    let pool = gate.pool.as_ref()?;
    let account = crate::web_admin::load_by_id(pool, account_id)
        .await
        .ok()??;
    verify_pending_2fa(account.session_key(), account_id, expiry, attempts, &cookie)
        .then_some(account)
}

/// POST `/login/2fa`: redeem the pending cookie against a TOTP or recovery
/// code. A wrong code re-renders with the attempt count bumped in the
/// re-issued cookie; hitting the cap or a missing/expired cookie sends the
/// operator back to `/login` to start over. A corrupt stored secret is
/// reported, not treated as "not 2FA'd".
async fn login_totp_submit(
    State(gate): State<TokenGate>,
    headers: HeaderMap,
    Form(form): Form<LoginTotpForm>,
) -> Response {
    if !gate.configured().await {
        return Redirect::to("/setup").into_response();
    }
    let Some(account) = pending_2fa_account(&gate, &headers).await else {
        return Redirect::to("/login").into_response();
    };
    if !account.totp_enrolled() {
        return Redirect::to("/login").into_response();
    }
    // The cookie was already verified in `pending_2fa_account`; re-read its
    // attempt count here so a wrong code can bump it.
    let Some(cookie) = cookie_value(&headers, PENDING_2FA_COOKIE) else {
        return Redirect::to("/login").into_response();
    };
    let Some((_, _, attempts)) = parse_pending_claims(&cookie) else {
        return Redirect::to("/login").into_response();
    };
    let Some(pool) = gate.pool.clone() else {
        return render_login_totp(
            Some("This instance cannot verify a second factor (no database handle).".to_string()),
            StatusCode::INTERNAL_SERVER_ERROR,
        );
    };
    let Some(key) = gate.secrets_key.clone() else {
        error!("no secrets key available; cannot verify a TOTP or recovery code");
        return render_login_totp(
            Some("2FA verification is unavailable; check the daemon log.".to_string()),
            StatusCode::INTERNAL_SERVER_ERROR,
        );
    };
    match crate::web_admin::verify_totp_or_recovery(&pool, &key, &account, &form.code).await {
        Ok(true) => {
            let mut resp = redirect_with_session("/", account.session_key(), account.id);
            if let Ok(v) = HeaderValue::from_str(&pending_2fa_clear_cookie()) {
                resp.headers_mut().append(header::SET_COOKIE, v);
            }
            resp
        }
        Ok(false) => {
            let attempts = attempts + 1;
            let mut resp = render_login_totp(
                Some("That code is not valid.".to_string()),
                StatusCode::UNAUTHORIZED,
            );
            let cookie = if attempts >= PENDING_2FA_MAX_ATTEMPTS {
                pending_2fa_clear_cookie()
            } else {
                pending_2fa_set_cookie(account.session_key(), account.id, attempts)
            };
            if let Ok(v) = HeaderValue::from_str(&cookie) {
                resp.headers_mut().insert(header::SET_COOKIE, v);
            }
            resp
        }
        Err(e) => {
            error!(error = %e, "TOTP/recovery-code verification failed");
            render_login_totp(
                Some(format!("2FA verification failed: {e}")),
                StatusCode::INTERNAL_SERVER_ERROR,
            )
        }
    }
}

/// GET `/account/2fa/setup`: begin (or resume) enrollment, showing the
/// `otpauth://` URI, its QR code, and a confirmation form. Reachable only by
/// an authenticated session (the `require_token` gate lets an unenrolled
/// session through to exactly this page), so the current account comes from
/// the request extension it attached.
async fn totp_setup_page(
    State(gate): State<TokenGate>,
    Extension(account): Extension<Account>,
) -> Response {
    if account.totp_enrolled() {
        return Redirect::to("/").into_response();
    }
    let (Some(pool), Some(key)) = (gate.pool.clone(), gate.secrets_key.clone()) else {
        error!("no database handle or secrets key available; cannot begin TOTP enrollment");
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            "TOTP enrollment is unavailable; check the daemon log.",
        )
            .into_response();
    };
    match crate::web_admin::begin_totp_enrollment(&pool, &key, account.id, &account.username).await
    {
        Ok(pending) => render_totp_setup(None, pending, StatusCode::OK),
        Err(e) => {
            error!(error = %e, "starting TOTP enrollment failed");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("could not start TOTP enrollment: {e}"),
            )
                .into_response()
        }
    }
}

/// POST `/account/2fa/setup`: confirm enrollment with a valid current code.
/// On success, renders the one-time recovery codes directly rather than
/// persisting them anywhere they could be re-displayed.
async fn totp_setup_submit(
    State(gate): State<TokenGate>,
    Extension(account): Extension<Account>,
    Form(form): Form<TotpConfirmForm>,
) -> Response {
    if account.totp_enrolled() {
        return Redirect::to("/").into_response();
    }
    let (Some(pool), Some(key)) = (gate.pool.clone(), gate.secrets_key.clone()) else {
        error!("no database handle or secrets key available; cannot confirm TOTP enrollment");
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            "TOTP enrollment is unavailable; check the daemon log.",
        )
            .into_response();
    };
    match crate::web_admin::confirm_totp_enrollment(&pool, &key, account.id, &form.code).await {
        Ok(codes) => match (TotpRecoveryCodesTpl { codes }).render() {
            Ok(html) => (StatusCode::OK, Html(html)).into_response(),
            Err(_) => (
                StatusCode::INTERNAL_SERVER_ERROR,
                "recovery codes template error",
            )
                .into_response(),
        },
        Err(e) => {
            warn!(error = %e, "TOTP enrollment confirmation refused");
            match crate::web_admin::begin_totp_enrollment(
                &pool,
                &key,
                account.id,
                &account.username,
            )
            .await
            {
                Ok(pending) => {
                    render_totp_setup(Some(e.to_string()), pending, StatusCode::BAD_REQUEST)
                }
                Err(e2) => {
                    error!(error = %e2, "restarting TOTP enrollment after a refused code failed");
                    (
                        StatusCode::INTERNAL_SERVER_ERROR,
                        format!("could not restart TOTP enrollment: {e2}"),
                    )
                        .into_response()
                }
            }
        }
    }
}

/// GET `/logout`: clear the session cookie and bounce back to the login form.
async fn logout() -> Response {
    let mut resp = Redirect::to("/login").into_response();
    let clear = format!("{SESSION_COOKIE}=; HttpOnly; Secure; SameSite=Lax; Path=/; Max-Age=0");
    if let Ok(v) = HeaderValue::from_str(&clear) {
        resp.headers_mut().insert(header::SET_COOKIE, v);
    }
    resp
}

/// GET `/setup`: the first-run account form. Once any account exists this is a
/// dead end by design, so it redirects to the login form: setup runs exactly
/// once and cannot be used to mint a second account (invites do that).
async fn setup_page(State(gate): State<TokenGate>) -> Response {
    if gate.configured().await {
        return Redirect::to("/login").into_response();
    }
    render_setup(None, StatusCode::OK)
}

/// POST `/setup`: create the first account and sign the operator in.
async fn setup_submit(State(gate): State<TokenGate>, Form(form): Form<SetupForm>) -> Response {
    if gate.configured().await {
        return Redirect::to("/login").into_response();
    }
    let Some(pool) = gate.pool.clone() else {
        return render_setup(
            Some("This instance cannot run setup (no database handle).".to_string()),
            StatusCode::INTERNAL_SERVER_ERROR,
        );
    };
    if form.password != form.confirm {
        return render_setup(
            Some("The two passwords do not match.".to_string()),
            StatusCode::BAD_REQUEST,
        );
    }
    let account = match crate::web_admin::create_first(&pool, &form.username, &form.password).await
    {
        Ok(a) => a,
        Err(e) => {
            // Surface the rule that was broken (too short, empty username,
            // already set up) rather than a generic failure.
            warn!(error = %e, "first-run setup refused");
            return render_setup(Some(e.to_string()), StatusCode::BAD_REQUEST);
        }
    };
    tracing::info!(
        username = account.username,
        "account created from first-run setup"
    );
    // A fresh account has no TOTP enrolled yet: send them straight to
    // enrollment rather than the dashboard, which `require_token` would
    // bounce them out of anyway. Going there directly avoids a pointless
    // redirect round trip.
    redirect_with_session("/account/2fa/setup", account.session_key(), account.id)
}

/// GET `/invite/<token>`: render the invite-acceptance form, or refuse once the
/// token is unknown, expired, or already used.
async fn invite_page(State(gate): State<TokenGate>, Path(token): Path<String>) -> Response {
    let Some(pool) = gate.pool.clone() else {
        return (StatusCode::NOT_FOUND, "invites are unavailable").into_response();
    };
    match crate::web_admin::find_live_invite_by_token(&pool, &token).await {
        Ok(Some(_)) => render_invite_accept(token, None, StatusCode::OK),
        Ok(None) => (
            StatusCode::NOT_FOUND,
            "this invite link is invalid or has expired",
        )
            .into_response(),
        Err(e) => {
            error!(error = %e, "invite lookup failed");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                "could not check this invite",
            )
                .into_response()
        }
    }
}

/// POST `/invite/<token>`: accept the invite, create the account, and send the
/// new account straight to forced TOTP enrollment with a fresh session, the
/// same as first-run setup.
async fn invite_submit(
    State(gate): State<TokenGate>,
    Path(token): Path<String>,
    Form(form): Form<InviteAcceptForm>,
) -> Response {
    let Some(pool) = gate.pool.clone() else {
        return (StatusCode::NOT_FOUND, "invites are unavailable").into_response();
    };
    if form.password != form.confirm {
        return render_invite_accept(
            token,
            Some("The two passwords do not match.".to_string()),
            StatusCode::BAD_REQUEST,
        );
    }
    let invite = match crate::web_admin::find_live_invite_by_token(&pool, &token).await {
        Ok(Some(i)) => i,
        Ok(None) => {
            return (
                StatusCode::NOT_FOUND,
                "this invite link is invalid or has expired",
            )
                .into_response()
        }
        Err(e) => {
            error!(error = %e, "invite lookup failed");
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                "could not check this invite",
            )
                .into_response();
        }
    };
    let account =
        match crate::web_admin::accept_invite(&pool, &invite, &form.username, &form.password).await
        {
            Ok(a) => a,
            Err(e) => {
                warn!(error = %e, "invite acceptance refused");
                return render_invite_accept(token, Some(e.to_string()), StatusCode::BAD_REQUEST);
            }
        };
    tracing::info!(
        username = account.username,
        "account created from an accepted invite"
    );
    redirect_with_session("/account/2fa/setup", account.session_key(), account.id)
}

/// GET `/settings/accounts`: list every account and outstanding invite.
async fn accounts_page(
    State(gate): State<TokenGate>,
    Extension(account): Extension<Account>,
) -> Response {
    render_accounts(&gate, account.id, None, None, StatusCode::OK).await
}

#[derive(Deserialize)]
struct CreateInviteForm {
    /// Blank: today's link-only flow. Non-blank: emailed when SMTP is
    /// configured, refused with `400` otherwise. See [`create_invite_submit`].
    #[serde(default)]
    email: String,
}

/// POST `/settings/accounts/invite`: mint a single-use invite. With `email`
/// blank, shows its link once (unchanged). With `email` set, emails the link
/// instead and does not display it, unless the send fails, in which case the
/// link is shown once as a fallback. See CLAUDE-394.
async fn create_invite_submit(
    State(gate): State<TokenGate>,
    Extension(account): Extension<Account>,
    Form(form): Form<CreateInviteForm>,
) -> Response {
    let Some(pool) = gate.pool.clone() else {
        return render_accounts(
            &gate,
            account.id,
            Some("no database handle".to_string()),
            None,
            StatusCode::INTERNAL_SERVER_ERROR,
        )
        .await;
    };
    let email = form.email.trim();
    if email.is_empty() {
        return match crate::web_admin::create_invite(&pool, account.id, None).await {
            Ok((token, _id)) => {
                tracing::info!(created_by = account.username, "invite created");
                let link = format!("/invite/{token}");
                render_accounts(&gate, account.id, None, Some(link), StatusCode::OK).await
            }
            Err(e) => {
                render_accounts(
                    &gate,
                    account.id,
                    Some(e.to_string()),
                    None,
                    StatusCode::INTERNAL_SERVER_ERROR,
                )
                .await
            }
        };
    }

    if email.parse::<lettre::message::Mailbox>().is_err() {
        return render_accounts(
            &gate,
            account.id,
            Some(format!("`{email}` is not a valid email address")),
            None,
            StatusCode::BAD_REQUEST,
        )
        .await;
    }

    let email_cfg = load_email_config(&pool).await;
    if !email_cfg.configured() {
        return render_accounts(
            &gate,
            account.id,
            Some(
                "SMTP is not configured; set it up on the Email (SMTP) card in Settings, or \
                 leave the address blank to get a one-time link instead"
                    .to_string(),
            ),
            None,
            StatusCode::BAD_REQUEST,
        )
        .await;
    }

    let (token, _id) = match crate::web_admin::create_invite(&pool, account.id, Some(email)).await {
        Ok(v) => v,
        Err(e) => {
            return render_accounts(
                &gate,
                account.id,
                Some(e.to_string()),
                None,
                StatusCode::INTERNAL_SERVER_ERROR,
            )
            .await
        }
    };
    tracing::info!(
        created_by = account.username,
        recipient = email,
        "invite created"
    );

    let password = match &email_cfg.smtp_password_ref {
        Some(name) => match crate::secrets::SecretsKey::load() {
            Ok(key) => match crate::secrets::resolve_secret(&pool, &key, name).await {
                Ok(p) => Some(p),
                Err(_) => None,
            },
            Err(_) => None,
        },
        None => None,
    };
    let public_url = email_cfg
        .public_url
        .as_deref()
        .unwrap_or_default()
        .trim_end_matches('/');
    let link = format!("/invite/{token}");
    let body = format!(
        "{inviter} has invited you to claude-run.\n\n\
         Accept the invite: {public_url}{link}\n\n\
         This link is single-use and expires in 72 hours.",
        inviter = account.username
    );
    let send_result = match crate::mailer::Mailer::new(&email_cfg, password.as_deref()) {
        Ok(mailer) => {
            mailer
                .send(email, "You have been invited to claude-run", &body)
                .await
        }
        Err(e) => Err(e),
    };
    match send_result {
        Ok(()) => {
            tracing::info!(
                inviter = account.username,
                recipient = email,
                "invite emailed"
            );
            render_accounts(
                &gate,
                account.id,
                None,
                Some(format!("sent:{email}")),
                StatusCode::OK,
            )
            .await
        }
        Err(e) => {
            tracing::error!(
                recipient = email,
                error = %e,
                "invite email send failed; keeping the invite and showing the link"
            );
            render_accounts(
                &gate,
                account.id,
                Some(format!("could not email the invite to {email}: {e}")),
                Some(link),
                StatusCode::BAD_GATEWAY,
            )
            .await
        }
    }
}

/// Load the `config.email` document directly from the DB. `render_accounts`
/// and `create_invite_submit` only have a [`TokenGate`] (its own `pool`), not
/// the daemon's `LiveSettings`, so this reads the stored document fresh
/// rather than through a live handle; a saved settings change still applies
/// on the very next invite.
async fn load_email_config(pool: &DbPool) -> crate::config::EmailConfig {
    crate::settings::get_json::<crate::config::EmailConfig>(
        pool,
        crate::settings::Section::Email.key(),
    )
    .await
    .unwrap_or_default()
    .unwrap_or_default()
}

/// POST `/settings/accounts/remove`: remove an account, refusing the last
/// account or self-removal (surfaced visibly, never a silent no-op).
async fn remove_account_submit(
    State(gate): State<TokenGate>,
    Extension(account): Extension<Account>,
    Form(form): Form<RemoveAccountForm>,
) -> Response {
    let Some(pool) = gate.pool.clone() else {
        return render_accounts(
            &gate,
            account.id,
            Some("no database handle".to_string()),
            None,
            StatusCode::INTERNAL_SERVER_ERROR,
        )
        .await;
    };
    match crate::web_admin::remove(&pool, account.id, form.id).await {
        Ok(()) => render_accounts(&gate, account.id, None, None, StatusCode::OK).await,
        Err(e) => {
            render_accounts(
                &gate,
                account.id,
                Some(e.to_string()),
                None,
                StatusCode::BAD_REQUEST,
            )
            .await
        }
    }
}

/// POST `/settings/accounts/revoke`: revoke an outstanding invite.
async fn revoke_invite_submit(
    State(gate): State<TokenGate>,
    Extension(account): Extension<Account>,
    Form(form): Form<RevokeInviteForm>,
) -> Response {
    let Some(pool) = gate.pool.clone() else {
        return render_accounts(
            &gate,
            account.id,
            Some("no database handle".to_string()),
            None,
            StatusCode::INTERNAL_SERVER_ERROR,
        )
        .await;
    };
    match crate::web_admin::revoke_invite(&pool, form.id).await {
        Ok(()) => render_accounts(&gate, account.id, None, None, StatusCode::OK).await,
        Err(e) => {
            render_accounts(
                &gate,
                account.id,
                Some(e.to_string()),
                None,
                StatusCode::INTERNAL_SERVER_ERROR,
            )
            .await
        }
    }
}

/// Render the Accounts page. `error`/`status` travel together: `status` is
/// `200` exactly when `error` is `None` for every caller above, so a refused
/// request (bad address, SMTP unconfigured, last-account/self removal) or a
/// send failure always answers non-2xx. `new_invite_link` carries either a
/// bare `/invite/<token>` link to show once, or (as a sentinel distinct from a
/// real link) `"sent:<address>"` to show the "Invite sent to <address>"
/// confirmation with no link at all.
async fn render_accounts(
    gate: &TokenGate,
    current_id: i64,
    error: Option<String>,
    new_invite_link: Option<String>,
    status: StatusCode,
) -> Response {
    let Some(pool) = gate.pool.as_ref() else {
        return (StatusCode::INTERNAL_SERVER_ERROR, "no database handle").into_response();
    };
    let accounts = match crate::web_admin::list(pool).await {
        Ok(a) => a,
        Err(e) => return (StatusCode::INTERNAL_SERVER_ERROR, format!("{e}")).into_response(),
    };
    let invites = match crate::web_admin::list_invites(pool).await {
        Ok(i) => i,
        Err(e) => return (StatusCode::INTERNAL_SERVER_ERROR, format!("{e}")).into_response(),
    };
    let email_configured = load_email_config(pool).await.configured();
    let (new_invite_link, invite_sent_to) = match new_invite_link {
        Some(sent) if sent.starts_with("sent:") => {
            (None, Some(sent.trim_start_matches("sent:").to_string()))
        }
        other => (other, None),
    };
    let tpl = AccountsTpl {
        version: crate::VERSION,
        accounts,
        invites,
        current_id,
        error,
        new_invite_link,
        invite_sent_to,
        email_configured,
    };
    match tpl.render() {
        Ok(html) => (status, Html(html)).into_response(),
        Err(e) => {
            tracing::error!(error = %e, "accounts template failed to render");
            (StatusCode::INTERNAL_SERVER_ERROR, "accounts template error").into_response()
        }
    }
}

/// Redirect to `to`, carrying a fresh session cookie for `account_id` signed by
/// `key`.
fn redirect_with_session(to: &str, key: &str, account_id: i64) -> Response {
    let mut resp = Redirect::to(to).into_response();
    if let Ok(v) = HeaderValue::from_str(&session_set_cookie(key, account_id)) {
        resp.headers_mut().insert(header::SET_COOKIE, v);
    }
    resp
}

fn render_setup(error: Option<String>, status: StatusCode) -> Response {
    let tpl = SetupTpl {
        error,
        min_password_len: crate::web_admin::MIN_PASSWORD_LEN,
    };
    match tpl.render() {
        Ok(html) => (status, Html(html)).into_response(),
        Err(_) => (StatusCode::INTERNAL_SERVER_ERROR, "setup template error").into_response(),
    }
}

fn render_invite_accept(token: String, error: Option<String>, status: StatusCode) -> Response {
    let tpl = InviteAcceptTpl {
        token,
        error,
        min_password_len: crate::web_admin::MIN_PASSWORD_LEN,
    };
    match tpl.render() {
        Ok(html) => (status, Html(html)).into_response(),
        Err(_) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            "invite accept template error",
        )
            .into_response(),
    }
}

fn render_login(error: Option<String>, status: StatusCode) -> Response {
    match (LoginTpl { error }).render() {
        Ok(html) => (status, Html(html)).into_response(),
        Err(_) => (StatusCode::INTERNAL_SERVER_ERROR, "login template error").into_response(),
    }
}

fn render_login_totp(error: Option<String>, status: StatusCode) -> Response {
    match (LoginTotpTpl { error }).render() {
        Ok(html) => (status, Html(html)).into_response(),
        Err(_) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            "login 2fa template error",
        )
            .into_response(),
    }
}

fn render_totp_setup(
    error: Option<String>,
    pending: crate::web_admin::PendingEnrollment,
    status: StatusCode,
) -> Response {
    let tpl = TotpSetupTpl {
        error,
        otpauth_url: pending.otpauth_url,
        qr_svg: pending.qr_svg,
    };
    match tpl.render() {
        Ok(html) => (status, Html(html)).into_response(),
        Err(_) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            "totp setup template error",
        )
            .into_response(),
    }
}

/// Extract the token from an `Authorization: Bearer <token>` header, if present.
/// The scheme match is case-insensitive per RFC 7235; the token is returned
/// verbatim (trimmed of surrounding whitespace).
fn bearer_token(request: &Request) -> Option<String> {
    let raw = request
        .headers()
        .get(header::AUTHORIZATION)?
        .to_str()
        .ok()?;
    let rest = raw.strip_prefix("Bearer ").or_else(|| {
        // Case-insensitive scheme: fall back to a manual split when the prefix
        // is e.g. `bearer ` rather than `Bearer `.
        let (scheme, rest) = raw.split_once(' ')?;
        scheme.eq_ignore_ascii_case("bearer").then_some(rest)
    })?;
    let t = rest.trim();
    (!t.is_empty()).then(|| t.to_string())
}

/// Length-checked constant-time byte comparison. Avoids a timing oracle on the
/// token by folding every byte before deciding. The length comparison is not
/// constant-time, which is the standard, accepted leak for a bearer token.
fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut diff: u8 = 0;
    for (x, y) in a.iter().zip(b.iter()) {
        diff |= x ^ y;
    }
    diff == 0
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{body::Body, routing::get};
    use tower::ServiceExt; // for `oneshot`

    #[test]
    fn loopback_detection() {
        assert!(is_loopback_bind("127.0.0.1"));
        assert!(is_loopback_bind("127.0.0.53"));
        assert!(is_loopback_bind("::1"));
        assert!(is_loopback_bind("localhost"));
        assert!(is_loopback_bind("  localhost  "));
        assert!(!is_loopback_bind("0.0.0.0"));
        assert!(!is_loopback_bind("::"));
        assert!(!is_loopback_bind("192.168.1.10"));
        // Unparseable, non-localhost -> fail safe (treated as non-loopback).
        assert!(!is_loopback_bind("not-an-address"));
    }

    #[test]
    fn auth_required_follows_bind_and_flag() {
        let loopback = WebConfig {
            bind: "127.0.0.1".into(),
            ..Default::default()
        };
        assert!(!auth_required(&loopback));

        let loopback_forced = WebConfig {
            bind: "127.0.0.1".into(),
            require_auth_on_loopback: true,
            ..Default::default()
        };
        assert!(auth_required(&loopback_forced));

        let exposed = WebConfig {
            bind: "0.0.0.0".into(),
            ..Default::default()
        };
        assert!(auth_required(&exposed));
    }

    #[test]
    fn constant_time_eq_matches_only_equal_slices() {
        assert!(constant_time_eq(b"token", b"token"));
        assert!(!constant_time_eq(b"token", b"tokeN"));
        assert!(!constant_time_eq(b"token", b"token-longer"));
        assert!(!constant_time_eq(b"", b"x"));
        assert!(constant_time_eq(b"", b""));
    }

    /// Build a tiny router gated by `secret`, with a webhook route mounted to
    /// prove the exemption.
    fn gated_router(secret: &str) -> Router {
        let app = Router::new()
            .route("/api/status", get(|| async { "ok" }))
            .route("/webhooks/forgejo/contoso", get(|| async { "hook" }));
        protect(app, TokenGate::new(secret.to_string()))
    }

    async fn status_of(router: Router, req: Request) -> StatusCode {
        router.oneshot(req).await.unwrap().status()
    }

    async fn setup_pool() -> DbPool {
        crate::db::test_pool().await
    }

    /// A router gated by an instance with no credential at all: the state a
    /// fresh container comes up in.
    async fn unconfigured_router(pool: &DbPool) -> Router {
        let app = Router::new()
            .route("/api/status", get(|| async { "ok" }))
            .route("/", get(|| async { "dashboard" }))
            .route("/webhooks/forgejo/contoso", get(|| async { "hook" }));
        protect(app, TokenGate::unconfigured(pool.clone()))
    }

    fn html_get(uri: &str) -> Request {
        Request::builder()
            .uri(uri)
            .header(header::ACCEPT, "text/html")
            .body(Body::empty())
            .unwrap()
    }

    /// An unconfigured instance must send a browser to `/setup` rather than
    /// answer 401 for a credential the operator cannot hold yet, and must not
    /// serve the app itself.
    #[tokio::test]
    async fn unconfigured_redirects_browsers_to_setup() {
        let pool = setup_pool().await;
        let resp = unconfigured_router(&pool)
            .await
            .oneshot(html_get("/"))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::SEE_OTHER);
        assert_eq!(
            resp.headers().get(header::LOCATION).unwrap(),
            "/setup",
            "the browser is steered to first-run setup"
        );
    }

    /// Non-browser callers get a 503 naming the state, never a 200 and never a
    /// bare 401 that suggests a token would help.
    #[tokio::test]
    async fn unconfigured_answers_api_clients_503() {
        let pool = setup_pool().await;
        let req = Request::builder()
            .uri("/api/status")
            .body(Body::empty())
            .unwrap();
        let resp = unconfigured_router(&pool).await.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::SERVICE_UNAVAILABLE);
    }

    /// The webhook prefix keeps its HMAC-only exemption while unconfigured.
    #[tokio::test]
    async fn unconfigured_still_exempts_webhooks() {
        let pool = setup_pool().await;
        let req = Request::builder()
            .uri("/webhooks/forgejo/contoso")
            .body(Body::empty())
            .unwrap();
        let resp = unconfigured_router(&pool).await.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
    }

    /// The setup page itself is reachable with no credential, or there is no way
    /// to create one.
    #[tokio::test]
    async fn setup_page_is_reachable_unauthenticated() {
        let pool = setup_pool().await;
        let resp = unconfigured_router(&pool)
            .await
            .oneshot(html_get("/setup"))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
    }

    /// Posting the setup form creates the account and signs the operator in
    /// with a session cookie that works on the very next request.
    #[tokio::test]
    async fn setup_creates_the_account_and_signs_in() {
        let pool = setup_pool().await;
        let app = Router::new().route("/", get(|| async { "dashboard" }));
        let gate = TokenGate::unconfigured(pool.clone());
        let router = protect(app, gate);

        let body = "username=admin&password=correct+horse+battery&confirm=correct+horse+battery";
        let req = Request::builder()
            .method(Method::POST)
            .uri("/setup")
            .header(header::CONTENT_TYPE, "application/x-www-form-urlencoded")
            .body(Body::from(body))
            .unwrap();
        let resp = router.clone().oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::SEE_OTHER);
        assert_eq!(
            resp.headers().get(header::LOCATION).unwrap(),
            "/account/2fa/setup",
            "a fresh account is sent straight to TOTP enrollment, not the dashboard"
        );
        let cookie = resp
            .headers()
            .get(header::SET_COOKIE)
            .expect("setup mints a session")
            .to_str()
            .unwrap()
            .to_string();

        assert!(
            crate::web_admin::exists(&pool).await.unwrap(),
            "the account is persisted, not just held in memory"
        );

        // That cookie authenticates, but the dashboard itself is still gated
        // until enrollment completes.
        let session = cookie.split(';').next().unwrap().to_string();
        let req = Request::builder()
            .uri("/")
            .header(header::COOKIE, session.clone())
            .body(Body::empty())
            .unwrap();
        assert_eq!(
            router.clone().oneshot(req).await.unwrap().status(),
            StatusCode::FORBIDDEN,
            "an API-shaped request to an unenrolled session's gated page gets a plain refusal"
        );
        let req = Request::builder()
            .uri("/")
            .header(header::COOKIE, session)
            .header(header::ACCEPT, "text/html")
            .body(Body::empty())
            .unwrap();
        let resp = router.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::SEE_OTHER);
        assert_eq!(
            resp.headers().get(header::LOCATION).unwrap(),
            "/account/2fa/setup"
        );
    }

    /// Setup runs once. A second POST cannot mint another account, whether or
    /// not the caller is signed in.
    #[tokio::test]
    async fn setup_is_refused_once_an_account_exists() {
        let pool = setup_pool().await;
        crate::web_admin::create_first(&pool, "admin", "correct horse battery")
            .await
            .unwrap();
        let router = protect(
            Router::new().route("/", get(|| async { "dashboard" })),
            TokenGate::unconfigured(pool.clone()),
        );
        let body = "username=intruder&password=another+long+password&confirm=another+long+password";
        let req = Request::builder()
            .method(Method::POST)
            .uri("/setup")
            .header(header::CONTENT_TYPE, "application/x-www-form-urlencoded")
            .body(Body::from(body))
            .unwrap();
        let resp = router.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::SEE_OTHER);
        assert_eq!(
            resp.headers().get(header::LOCATION).unwrap(),
            "/login",
            "a configured instance sends setup traffic to the login form"
        );
        let still = crate::web_admin::load_by_username(&pool, "admin")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(still.username, "admin", "the original account is untouched");
    }

    /// The DB account authenticates the login form, and its password is
    /// checked against the hash rather than any configured token.
    #[tokio::test]
    async fn account_credentials_sign_in_through_the_login_form() {
        let pool = setup_pool().await;
        crate::web_admin::create_first(&pool, "admin", "correct horse battery")
            .await
            .unwrap();
        let gate = TokenGate::unconfigured(pool.clone());
        let router = protect(
            Router::new().route("/", get(|| async { "dashboard" })),
            gate,
        );

        let bad = Request::builder()
            .method(Method::POST)
            .uri("/login")
            .header(header::CONTENT_TYPE, "application/x-www-form-urlencoded")
            .body(Body::from("username=admin&password=wrong"))
            .unwrap();
        assert_eq!(
            router.clone().oneshot(bad).await.unwrap().status(),
            StatusCode::UNAUTHORIZED
        );

        let good = Request::builder()
            .method(Method::POST)
            .uri("/login")
            .header(header::CONTENT_TYPE, "application/x-www-form-urlencoded")
            .body(Body::from("username=admin&password=correct+horse+battery"))
            .unwrap();
        let resp = router.oneshot(good).await.unwrap();
        assert_eq!(resp.status(), StatusCode::SEE_OTHER);
        assert!(resp.headers().get(header::SET_COOKIE).is_some());
    }

    /// A short password is refused with the reason shown, and nothing is written.
    #[tokio::test]
    async fn setup_refuses_a_short_password_and_says_why() {
        let pool = setup_pool().await;
        let router = unconfigured_router(&pool).await;
        let req = Request::builder()
            .method(Method::POST)
            .uri("/setup")
            .header(header::CONTENT_TYPE, "application/x-www-form-urlencoded")
            .body(Body::from("username=admin&password=short&confirm=short"))
            .unwrap();
        let resp = router.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
        let body = axum::body::to_bytes(resp.into_body(), 64 * 1024)
            .await
            .unwrap();
        let html = String::from_utf8_lossy(&body);
        assert!(html.contains("at least"), "the rule is named: {html}");
        assert!(!crate::web_admin::exists(&pool).await.unwrap());
    }

    /// Mismatched confirmation is caught before anything is hashed or written.
    #[tokio::test]
    async fn setup_refuses_mismatched_confirmation() {
        let pool = setup_pool().await;
        let router = unconfigured_router(&pool).await;
        let req = Request::builder()
            .method(Method::POST)
            .uri("/setup")
            .header(header::CONTENT_TYPE, "application/x-www-form-urlencoded")
            .body(Body::from(
                "username=admin&password=correct+horse+battery&confirm=different+long+password",
            ))
            .unwrap();
        let resp = router.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
        assert!(!crate::web_admin::exists(&pool).await.unwrap());
    }

    #[tokio::test]
    async fn gate_rejects_missing_token() {
        let req = Request::builder()
            .uri("/api/status")
            .body(Body::empty())
            .unwrap();
        assert_eq!(
            status_of(gated_router("s3cret"), req).await,
            StatusCode::UNAUTHORIZED
        );
    }

    #[tokio::test]
    async fn gate_rejects_wrong_token() {
        let req = Request::builder()
            .uri("/api/status")
            .header(header::AUTHORIZATION, "Bearer wrong")
            .body(Body::empty())
            .unwrap();
        assert_eq!(
            status_of(gated_router("s3cret"), req).await,
            StatusCode::UNAUTHORIZED
        );
    }

    #[tokio::test]
    async fn gate_accepts_correct_token() {
        let req = Request::builder()
            .uri("/api/status")
            .header(header::AUTHORIZATION, "Bearer s3cret")
            .body(Body::empty())
            .unwrap();
        assert_eq!(status_of(gated_router("s3cret"), req).await, StatusCode::OK);
    }

    #[tokio::test]
    async fn gate_accepts_case_insensitive_scheme() {
        let req = Request::builder()
            .uri("/api/status")
            .header(header::AUTHORIZATION, "bearer s3cret")
            .body(Body::empty())
            .unwrap();
        assert_eq!(status_of(gated_router("s3cret"), req).await, StatusCode::OK);
    }

    #[tokio::test]
    async fn webhook_route_is_exempt_from_the_gate() {
        let req = Request::builder()
            .uri("/webhooks/forgejo/contoso")
            .body(Body::empty())
            .unwrap();
        // No Authorization header, yet the webhook passes through.
        assert_eq!(status_of(gated_router("s3cret"), req).await, StatusCode::OK);
    }

    #[test]
    fn session_round_trips_and_rejects_tampering() {
        let cookie = sign_session("s3cret", 42, now_unix() + 60);
        assert!(verify_session("s3cret", 42, now_unix() + 60, &cookie));
        // Wrong key, wrong id, garbage, and a non-numeric expiry all fail.
        assert!(!verify_session("other", 42, now_unix() + 60, &cookie));
        let (id, expiry) = parse_session_claims(&cookie).unwrap();
        assert!(!verify_session("s3cret", id + 1, expiry, &cookie));
        assert!(parse_session_claims("not-a-session").is_none());
        assert!(parse_session_claims("abc.deadbeef").is_none());
        // A flipped signature byte fails.
        let mut bad = cookie.clone();
        bad.pop();
        bad.push(if cookie.ends_with('0') { '1' } else { '0' });
        assert!(!verify_session("s3cret", 42, now_unix() + 60, &bad));
    }

    #[test]
    fn expired_session_is_rejected() {
        let cookie = sign_session("s3cret", 42, now_unix() - 1);
        assert!(!verify_session("s3cret", 42, now_unix() - 1, &cookie));
    }

    /// A router protected with form login enabled.
    fn login_router(token: &str, user: &str) -> Router {
        let app = Router::new().route("/", get(|| async { "home" }));
        protect(
            app,
            TokenGate::with_login(token.to_string(), user.to_string()),
        )
    }

    #[tokio::test]
    async fn browser_request_without_auth_redirects_to_login() {
        let req = Request::builder()
            .uri("/")
            .header(header::ACCEPT, "text/html")
            .body(Body::empty())
            .unwrap();
        assert_eq!(
            status_of(login_router("s3cret", "admin"), req).await,
            StatusCode::SEE_OTHER
        );
    }

    #[tokio::test]
    async fn login_form_route_is_reachable_unauthenticated() {
        let req = Request::builder()
            .uri("/login")
            .header(header::ACCEPT, "text/html")
            .body(Body::empty())
            .unwrap();
        assert_eq!(
            status_of(login_router("s3cret", "admin"), req).await,
            StatusCode::OK
        );
    }

    #[tokio::test]
    async fn login_submit_sets_cookie_and_cookie_unlocks_the_gate() {
        // Correct credentials -> redirect + Set-Cookie.
        let body = "username=admin&password=s3cret";
        let req = Request::builder()
            .method(Method::POST)
            .uri("/login")
            .header(header::CONTENT_TYPE, "application/x-www-form-urlencoded")
            .body(Body::from(body))
            .unwrap();
        let resp = login_router("s3cret", "admin").oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::SEE_OTHER);
        let cookie = resp
            .headers()
            .get(header::SET_COOKIE)
            .unwrap()
            .to_str()
            .unwrap()
            .to_string();
        assert!(cookie.contains("HttpOnly") && cookie.contains("Secure"));
        // The minted cookie then authenticates a normal request.
        let cookie_val = cookie.split(';').next().unwrap().to_string();
        let req2 = Request::builder()
            .uri("/")
            .header(header::COOKIE, cookie_val)
            .body(Body::empty())
            .unwrap();
        assert_eq!(
            status_of(login_router("s3cret", "admin"), req2).await,
            StatusCode::OK
        );
    }

    #[tokio::test]
    async fn login_submit_rejects_wrong_password() {
        let body = "username=admin&password=wrong";
        let req = Request::builder()
            .method(Method::POST)
            .uri("/login")
            .header(header::CONTENT_TYPE, "application/x-www-form-urlencoded")
            .body(Body::from(body))
            .unwrap();
        let resp = login_router("s3cret", "admin").oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
        assert!(resp.headers().get(header::SET_COOKIE).is_none());
    }

    fn test_secrets_key() -> Arc<SecretsKey> {
        use age::secrecy::ExposeSecret;
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("key.txt");
        let identity = age::x25519::Identity::generate();
        std::fs::write(&path, identity.to_string().expose_secret()).unwrap();
        // Leak the tempdir so the key file outlives the test; these are
        // short-lived process-local test fixtures, not a resource that
        // accumulates across a long-running binary.
        std::mem::forget(tmp);
        Arc::new(SecretsKey::load_from_file(&path).unwrap())
    }

    /// A fully enrolled account, its gate, and the router built from it, for
    /// exercising the full login -> 2FA -> dashboard flow.
    async fn enrolled_router() -> (Router, DbPool, Arc<SecretsKey>, String) {
        let pool = setup_pool().await;
        let key = test_secrets_key();
        let account = crate::web_admin::create_first(&pool, "admin", "correct horse battery")
            .await
            .unwrap();
        let pending = crate::web_admin::begin_totp_enrollment(&pool, &key, account.id, "admin")
            .await
            .unwrap();
        // Recover the base32 secret from the otpauth URL rather than reaching
        // into `Account`'s private stored-ciphertext field, so this test only
        // depends on `web_admin`'s public surface.
        let base32 = pending
            .otpauth_url
            .split("secret=")
            .nth(1)
            .and_then(|rest| rest.split('&').next())
            .expect("otpauth URL carries the secret")
            .to_string();
        let secret = totp_rs::Secret::try_from_base32(&base32).unwrap();
        let totp = totp_rs::Builder::new()
            .with_issuer(Some("claude-run"))
            .with_account_name("admin".to_string())
            .with_secret(secret)
            .build()
            .unwrap();
        let enroll_code = totp.generate_current().to_string();
        crate::web_admin::confirm_totp_enrollment(&pool, &key, account.id, &enroll_code)
            .await
            .unwrap();
        // The code that confirmed enrollment is itself now spent (replay
        // protection), so callers exercising a *login* code need a fresh one
        // from the next time step.
        let next_code = totp.generate(now_unix() + 30).to_string();

        let gate = TokenGate::unconfigured(pool.clone()).with_secrets_key(key.clone());
        let app = Router::new().route("/", get(|| async { "dashboard" }));
        (protect(app, gate), pool, key, next_code)
    }

    /// An enrolled account's login with the right password never yields a
    /// session cookie until the second factor is also presented.
    #[tokio::test]
    async fn login_with_correct_password_and_no_second_factor_never_sessions() {
        let (router, ..) = enrolled_router().await;
        let req = Request::builder()
            .method(Method::POST)
            .uri("/login")
            .header(header::CONTENT_TYPE, "application/x-www-form-urlencoded")
            .body(Body::from("username=admin&password=correct+horse+battery"))
            .unwrap();
        let resp = router.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::SEE_OTHER);
        assert_eq!(resp.headers().get(header::LOCATION).unwrap(), "/login/2fa");
        assert!(
            resp.headers()
                .get(header::SET_COOKIE)
                .unwrap()
                .to_str()
                .unwrap()
                .starts_with(PENDING_2FA_COOKIE),
            "only a pending-2fa cookie is issued, never a session"
        );
    }

    /// A wrong second factor also never yields a session cookie.
    #[tokio::test]
    async fn login_with_wrong_second_factor_never_sessions() {
        let (router, pool, admin_key, _code) = enrolled_router().await;
        let account = crate::web_admin::load_by_username(&pool, "admin")
            .await
            .unwrap()
            .unwrap();
        let pending = pending_2fa_set_cookie(account.session_key(), account.id, 0);
        let pending_val = pending.split(';').next().unwrap().to_string();
        let _ = admin_key;

        let req = Request::builder()
            .method(Method::POST)
            .uri("/login/2fa")
            .header(header::CONTENT_TYPE, "application/x-www-form-urlencoded")
            .header(header::COOKIE, pending_val)
            .body(Body::from("code=000000"))
            .unwrap();
        let resp = router.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
        assert!(
            !resp
                .headers()
                .get(header::SET_COOKIE)
                .map(|v| v.to_str().unwrap().starts_with(SESSION_COOKIE))
                .unwrap_or(false),
            "a wrong code must never mint a session cookie"
        );
    }

    /// The full flow: password, then a valid TOTP code, yields a working
    /// session; the pending-2fa cookie is cleared on success.
    #[tokio::test]
    async fn login_then_valid_totp_code_yields_a_session() {
        let (router, pool, key, code) = enrolled_router().await;
        let _ = &key;
        let account = crate::web_admin::load_by_username(&pool, "admin")
            .await
            .unwrap()
            .unwrap();
        let pending = pending_2fa_set_cookie(account.session_key(), account.id, 0);
        let pending_val = pending.split(';').next().unwrap().to_string();

        let req = Request::builder()
            .method(Method::POST)
            .uri("/login/2fa")
            .header(header::CONTENT_TYPE, "application/x-www-form-urlencoded")
            .header(header::COOKIE, pending_val)
            .body(Body::from(format!("code={code}")))
            .unwrap();
        let resp = router.clone().oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::SEE_OTHER);
        let cookies: Vec<String> = resp
            .headers()
            .get_all(header::SET_COOKIE)
            .iter()
            .map(|v| v.to_str().unwrap().to_string())
            .collect();
        assert!(
            cookies.iter().any(|c| c.starts_with(SESSION_COOKIE)),
            "a valid code mints a session cookie"
        );
        let session = cookies
            .iter()
            .find(|c| c.starts_with(SESSION_COOKIE))
            .unwrap()
            .split(';')
            .next()
            .unwrap()
            .to_string();

        let req = Request::builder()
            .uri("/")
            .header(header::COOKIE, session)
            .body(Body::empty())
            .unwrap();
        assert_eq!(router.oneshot(req).await.unwrap().status(), StatusCode::OK);
    }

    /// An existing account with no TOTP enrolled is redirected to enrollment on
    /// every browser GET except the setup page itself, and refused outright
    /// for anything else, so no other page loads until enrollment completes.
    #[tokio::test]
    async fn unenrolled_account_is_redirected_to_totp_setup_on_every_page() {
        let pool = setup_pool().await;
        let account = crate::web_admin::create_first(&pool, "admin", "correct horse battery")
            .await
            .unwrap();
        let gate = TokenGate::unconfigured(pool.clone());
        let app = Router::new()
            .route("/", get(|| async { "dashboard" }))
            .route("/other", get(|| async { "other" }));
        let router = protect(app, gate);

        let session = sign_session(account.session_key(), account.id, now_unix() + 60);
        let cookie = format!("{SESSION_COOKIE}={session}");

        let req = Request::builder()
            .uri("/other")
            .header(header::COOKIE, cookie.clone())
            .header(header::ACCEPT, "text/html")
            .body(Body::empty())
            .unwrap();
        let resp = router.clone().oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::SEE_OTHER);
        assert_eq!(
            resp.headers().get(header::LOCATION).unwrap(),
            "/account/2fa/setup"
        );

        let req = Request::builder()
            .uri("/other")
            .header(header::COOKIE, cookie)
            .body(Body::empty())
            .unwrap();
        let resp = router.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::FORBIDDEN);
    }

    /// A corrupt stored TOTP secret refuses login and reports the reason
    /// rather than silently treating the account as unenrolled or as a wrong
    /// code.
    #[tokio::test]
    async fn corrupt_stored_secret_refuses_login_with_a_visible_message() {
        let pool = setup_pool().await;
        let key = test_secrets_key();
        let account = crate::web_admin::create_first(&pool, "admin", "correct horse battery")
            .await
            .unwrap();
        sqlx::query(&crate::db::q(
            &pool,
            "UPDATE web_accounts SET totp_secret = ?, totp_enrolled_at = ? WHERE id = ?",
        ))
        .bind(b"not a real ciphertext".to_vec())
        .bind(chrono::Utc::now().to_rfc3339())
        .bind(account.id)
        .execute(&pool)
        .await
        .unwrap();

        let gate = TokenGate::unconfigured(pool.clone()).with_secrets_key(key);
        let app = Router::new().route("/", get(|| async { "dashboard" }));
        let router = protect(app, gate);

        let account = crate::web_admin::load_by_username(&pool, "admin")
            .await
            .unwrap()
            .unwrap();
        let pending = pending_2fa_set_cookie(account.session_key(), account.id, 0);
        let pending_val = pending.split(';').next().unwrap().to_string();

        let req = Request::builder()
            .method(Method::POST)
            .uri("/login/2fa")
            .header(header::CONTENT_TYPE, "application/x-www-form-urlencoded")
            .header(header::COOKIE, pending_val)
            .body(Body::from("code=123456"))
            .unwrap();
        let resp = router.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::INTERNAL_SERVER_ERROR);
        assert!(resp.headers().get(header::SET_COOKIE).is_none());
        let body = axum::body::to_bytes(resp.into_body(), 64 * 1024)
            .await
            .unwrap();
        let html = String::from_utf8_lossy(&body);
        assert!(
            html.contains("verification failed"),
            "the failure must be visible, not silent: {html}"
        );
    }

    /// Accepting an invite creates a second account that can sign in on its
    /// own session, independent of the inviter's.
    #[tokio::test]
    async fn accepting_an_invite_signs_in_a_second_account() {
        let pool = setup_pool().await;
        let admin = crate::web_admin::create_first(&pool, "admin", "correct horse battery")
            .await
            .unwrap();
        let (token, _id) = crate::web_admin::create_invite(&pool, admin.id, None)
            .await
            .unwrap();
        let gate = TokenGate::unconfigured(pool.clone());
        let app = Router::new().route("/", get(|| async { "dashboard" }));
        let router = protect(app, gate);

        let req = Request::builder()
            .uri(format!("/invite/{token}"))
            .body(Body::empty())
            .unwrap();
        assert_eq!(
            router.clone().oneshot(req).await.unwrap().status(),
            StatusCode::OK,
            "a live invite's page is reachable unauthenticated"
        );

        let body = "username=second&password=another+long+password&confirm=another+long+password";
        let req = Request::builder()
            .method(Method::POST)
            .uri(format!("/invite/{token}"))
            .header(header::CONTENT_TYPE, "application/x-www-form-urlencoded")
            .body(Body::from(body))
            .unwrap();
        let resp = router.clone().oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::SEE_OTHER);
        assert_eq!(
            resp.headers().get(header::LOCATION).unwrap(),
            "/account/2fa/setup"
        );
        assert!(
            crate::web_admin::load_by_username(&pool, "second")
                .await
                .unwrap()
                .is_some(),
            "the invited account is persisted"
        );

        // A second acceptance of the same (now used) token is refused.
        let req = Request::builder()
            .uri(format!("/invite/{token}"))
            .body(Body::empty())
            .unwrap();
        assert_eq!(
            router.oneshot(req).await.unwrap().status(),
            StatusCode::NOT_FOUND,
            "a used invite link no longer resolves"
        );
    }
}
