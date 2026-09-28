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
//! - The browser login, which since the first-run setup page is the
//!   [`crate::web_admin`] credential in the DB: a username plus an argon2id
//!   hash, created from `/setup` with no CLI and no secrets key file. Where no
//!   admin row exists, `web.auth_username` plus the token plaintext still works
//!   as the login, so existing host deployments are unchanged.
//!
//! When auth is required and NEITHER credential exists, the instance is
//! unconfigured: every request is steered to `/setup` (browsers) or answered
//! `503` (everything else) until an administrator is created. That replaces the
//! older behaviour of refusing to bind the TCP listener at all, which was
//! unrecoverable in a container: the operator could not reach the UI that would
//! have configured the credential the daemon was demanding. The surface is never
//! served unauthenticated either way.

use std::net::IpAddr;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::db::DbPool;
use askama::Template;
use axum::{
    extract::{Form, Request, State},
    http::{header, HeaderValue, Method, StatusCode},
    middleware::Next,
    response::{Html, IntoResponse, Redirect, Response},
    routing::get,
    Router,
};
use hmac::{Hmac, Mac};
use serde::Deserialize;
use sha2::Sha256;

use tracing::warn;

use crate::config::WebConfig;
use crate::error::Result;
use crate::web_admin::AdminCredential;

/// Session cookie name set on a successful form login and accepted by the gate
/// in lieu of a bearer header.
const SESSION_COOKIE: &str = "claude_run_session";
/// How long a login session stays valid before the browser must sign in again.
const SESSION_TTL_SECS: u64 = 12 * 3600;

/// The live administrator credential, shared between the gate and the setup
/// handler so a first-run setup takes effect on the next request instead of at
/// the next restart.
pub type AdminHandle = Arc<tokio::sync::RwLock<Option<Arc<AdminCredential>>>>;

/// Holds whatever credentials this run has: the bearer token (when
/// `auth_token_ref` resolved), the legacy form-login username, and a live handle
/// to the DB administrator. Cheaply cloned into the middleware.
#[derive(Clone)]
pub struct TokenGate {
    /// `None` when no `auth_token_ref` is configured or it did not resolve.
    /// Bearer auth is then simply unavailable; the browser login still works.
    token: Option<Arc<String>>,
    /// `Some` when `web.auth_username` is set: the legacy login username, used
    /// only while no DB administrator exists.
    username: Option<Arc<String>>,
    /// The DB administrator, or `None` on an instance that has not run setup.
    admin: AdminHandle,
    /// Pool the setup handler writes the new administrator through.
    pool: Option<DbPool>,
}

impl TokenGate {
    /// Bearer-only gate (no form login, no DB administrator). Used by tests and
    /// any caller that does not configure a username.
    pub fn new(token: String) -> Self {
        Self {
            token: Some(Arc::new(token)),
            username: None,
            admin: Arc::new(tokio::sync::RwLock::new(None)),
            pool: None,
        }
    }

    /// Gate with the legacy browser login form enabled for `username`.
    pub fn with_login(token: String, username: String) -> Self {
        Self {
            username: Some(Arc::new(username)),
            ..Self::new(token)
        }
    }

    /// Attach the live administrator handle and the pool `/setup` writes through.
    pub fn with_admin(mut self, admin: AdminHandle, pool: DbPool) -> Self {
        self.admin = admin;
        self.pool = Some(pool);
        self
    }

    /// A gate with no credential at all: the instance is unconfigured and every
    /// request is steered to `/setup`.
    pub fn unconfigured(admin: AdminHandle, pool: DbPool) -> Self {
        Self {
            token: None,
            username: None,
            admin,
            pool: Some(pool),
        }
    }

    /// The current administrator, if setup has run.
    async fn admin(&self) -> Option<Arc<AdminCredential>> {
        self.admin.read().await.clone()
    }

    /// Whether any credential can authenticate a request. `false` means the
    /// instance still needs first-run setup.
    async fn configured(&self) -> bool {
        self.token.is_some() || self.admin().await.is_some()
    }

    /// The key that signs and verifies login session cookies: the
    /// administrator's rotating session key when one exists, else the bearer
    /// token (the pre-setup behaviour). Returns `None` on an unconfigured
    /// instance, where no session can be minted at all.
    async fn session_key(&self) -> Option<String> {
        if let Some(admin) = self.admin().await {
            return Some(admin.session_key().to_string());
        }
        self.token.as_ref().map(|t| t.to_string())
    }

    /// Whether a browser should be sent to a form rather than a bare 401. True
    /// once there is any form to send them to: a DB administrator, a configured
    /// legacy username, or an unconfigured instance needing setup.
    async fn has_browser_login(&self) -> bool {
        self.username.is_some() || self.admin().await.is_some() || !self.configured().await
    }
}

fn now_unix() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

/// Sign a session for `expiry` (unix seconds) under `token`, returning the
/// cookie value `"<expiry>.<hex-hmac>"`. The HMAC keys on the token, so rotating
/// the secret invalidates every outstanding session.
fn sign_session(token: &str, expiry: u64) -> String {
    let mut mac =
        Hmac::<Sha256>::new_from_slice(token.as_bytes()).expect("HMAC accepts any key length");
    mac.update(expiry.to_string().as_bytes());
    format!("{expiry}.{}", hex::encode(mac.finalize().into_bytes()))
}

/// True when `cookie_val` is a non-expired session signed by `token`.
fn verify_session(token: &str, cookie_val: &str) -> bool {
    let Some((exp_str, _)) = cookie_val.split_once('.') else {
        return false;
    };
    let Ok(expiry) = exp_str.parse::<u64>() else {
        return false;
    };
    if now_unix() > expiry {
        return false;
    }
    constant_time_eq(
        sign_session(token, expiry).as_bytes(),
        cookie_val.as_bytes(),
    )
}

/// The `Set-Cookie` value for a fresh session: HttpOnly + Secure + SameSite=Lax,
/// scoped to the whole site, expiring with the session TTL.
fn session_set_cookie(token: &str) -> String {
    let value = sign_session(token, now_unix() + SESSION_TTL_SECS);
    format!(
        "{SESSION_COOKIE}={value}; HttpOnly; Secure; SameSite=Lax; Path=/; Max-Age={SESSION_TTL_SECS}"
    )
}

/// Read the session cookie value from a request's `Cookie` header, if present.
fn session_cookie(request: &Request) -> Option<String> {
    let raw = request.headers().get(header::COOKIE)?.to_str().ok()?;
    raw.split(';').map(str::trim).find_map(|kv| {
        let (k, v) = kv.split_once('=')?;
        (k == SESSION_COOKIE).then(|| v.to_string())
    })
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
/// and the DB administrator.
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
    let admin = crate::web_admin::load(pool).await?.map(Arc::new);
    let handle: AdminHandle = Arc::new(tokio::sync::RwLock::new(admin.clone()));
    let token = resolve_token(web, pool).await;

    let gate = match token {
        Some(token) => {
            let mut gate = match web.auth_username.as_deref() {
                Some(u) if !u.trim().is_empty() => {
                    TokenGate::with_login(token, u.trim().to_string())
                }
                _ => TokenGate::new(token),
            };
            gate = gate.with_admin(handle, pool.clone());
            gate
        }
        None if admin.is_some() => TokenGate {
            token: None,
            username: None,
            admin: handle,
            pool: Some(pool.clone()),
        },
        None => {
            warn!(
                "no web credential configured; serving first-run setup at /setup until an \
                 administrator is created. Every other request is refused until then."
            );
            TokenGate::unconfigured(handle, pool.clone())
        }
    };
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
/// prefix and the login routes. When the gate has a configured username, the
/// `/login` (+ `/logout`) form routes are mounted so a browser / password
/// manager can sign in.
pub fn protect(router: Router, gate: TokenGate) -> Router {
    // `/login`, `/logout` and `/setup` are always mounted and always reachable:
    // which of them is useful depends on state the gate reads per request (a DB
    // administrator can appear at runtime), so deciding at mount time would
    // require a restart to pick setup up.
    let auth_routes = Router::new()
        .route("/login", get(login_page).post(login_submit))
        .route("/logout", get(logout))
        .route("/setup", get(setup_page).post(setup_submit))
        .with_state(gate.clone());
    router
        .merge(auth_routes)
        .layer(axum::middleware::from_fn_with_state(gate, require_token))
}

/// Axum middleware enforcing a bearer token or a session cookie. Exempts the
/// webhook prefix and the auth routes. An unauthenticated browser navigation
/// (GET accepting HTML) is redirected to the form it needs (`/setup` before the
/// instance is configured, `/login` after); everything else gets a 401, or a 503
/// while unconfigured because there is no credential to present yet.
async fn require_token(State(gate): State<TokenGate>, request: Request, next: Next) -> Response {
    // The webhook route authenticates itself via HMAC and Forgejo cannot send a
    // bearer token, so it is never gated here. The auth routes must be reachable
    // unauthenticated or there is no way in.
    let path = request.uri().path();
    if path.starts_with("/webhooks/") || path == "/login" || path == "/logout" || path == "/setup" {
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
            "claude-run has no administrator yet; open /setup in a browser to create one",
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
        if let Some(key) = gate.session_key().await {
            if verify_session(&key, &cookie) {
                return next.run(request).await;
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

#[derive(Template)]
#[template(path = "login.html")]
struct LoginTpl {
    error: Option<String>,
}

#[derive(Template)]
#[template(path = "setup.html")]
struct SetupTpl {
    error: Option<String>,
    min_password_len: usize,
}

#[derive(Deserialize)]
struct LoginForm {
    username: String,
    password: String,
}

#[derive(Deserialize)]
struct SetupForm {
    username: String,
    password: String,
    confirm: String,
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
/// the dashboard. The DB administrator wins when one exists; otherwise the
/// pre-setup pair (`web.auth_username` plus the `auth_token_ref` plaintext)
/// still authenticates, so existing host deployments keep working. On mismatch,
/// re-render with a 401 and a generic error naming neither field.
async fn login_submit(State(gate): State<TokenGate>, Form(form): Form<LoginForm>) -> Response {
    if !gate.configured().await {
        return Redirect::to("/setup").into_response();
    }
    let ok = match gate.admin().await {
        Some(admin) => {
            let user_ok = constant_time_eq(form.username.as_bytes(), admin.username.as_bytes());
            match admin.verify_password(&form.password) {
                Ok(pass_ok) => user_ok && pass_ok,
                Err(e) => {
                    // A corrupt stored hash is an operator problem, not a wrong
                    // password: log the cause rather than let it read as a typo.
                    warn!(error = %e, "admin password hash could not be parsed; login refused");
                    false
                }
            }
        }
        None => match (gate.username.as_ref(), gate.token.as_ref()) {
            (Some(username), Some(token)) => {
                constant_time_eq(form.username.as_bytes(), username.as_bytes())
                    && constant_time_eq(form.password.as_bytes(), token.as_bytes())
            }
            _ => false,
        },
    };
    if !ok {
        return render_login(
            Some("Invalid username or password.".to_string()),
            StatusCode::UNAUTHORIZED,
        );
    }
    let Some(key) = gate.session_key().await else {
        return render_login(
            Some("No session key is available; check the daemon log.".to_string()),
            StatusCode::INTERNAL_SERVER_ERROR,
        );
    };
    redirect_with_session("/", &key)
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

/// GET `/setup`: the first-run administrator form. Once an administrator exists
/// this is a dead end by design, so it redirects to the login form: setup runs
/// exactly once and cannot be used to mint a second admin.
async fn setup_page(State(gate): State<TokenGate>) -> Response {
    if gate.configured().await {
        return Redirect::to("/login").into_response();
    }
    render_setup(None, StatusCode::OK)
}

/// POST `/setup`: create the administrator, adopt it for this run without a
/// restart, and sign the operator in.
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
    let cred = match crate::web_admin::create(&pool, &form.username, &form.password).await {
        Ok(c) => c,
        Err(e) => {
            // Surface the rule that was broken (too short, empty username,
            // already set up) rather than a generic failure.
            warn!(error = %e, "first-run setup refused");
            return render_setup(Some(e.to_string()), StatusCode::BAD_REQUEST);
        }
    };
    let key = cred.session_key().to_string();
    let username = cred.username.clone();
    // Adopt it for the live gate so the very next request is authenticated
    // against it; without this the operator would have to restart the daemon.
    *gate.admin.write().await = Some(Arc::new(cred));
    tracing::info!(username, "administrator created from first-run setup");
    redirect_with_session("/", &key)
}

/// Redirect to `to`, carrying a fresh session cookie signed by `key`.
fn redirect_with_session(to: &str, key: &str) -> Response {
    let mut resp = Redirect::to(to).into_response();
    if let Ok(v) = HeaderValue::from_str(&session_set_cookie(key)) {
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

fn render_login(error: Option<String>, status: StatusCode) -> Response {
    match (LoginTpl { error }).render() {
        Ok(html) => (status, Html(html)).into_response(),
        Err(_) => (StatusCode::INTERNAL_SERVER_ERROR, "login template error").into_response(),
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
        let handle: AdminHandle = Arc::new(tokio::sync::RwLock::new(None));
        protect(app, TokenGate::unconfigured(handle, pool.clone()))
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

    /// Posting the setup form creates the administrator, signs the operator in
    /// with a session cookie, and takes effect for the very next request with no
    /// restart.
    #[tokio::test]
    async fn setup_creates_the_admin_and_signs_in() {
        let pool = setup_pool().await;
        let app = Router::new().route("/", get(|| async { "dashboard" }));
        let handle: AdminHandle = Arc::new(tokio::sync::RwLock::new(None));
        let gate = TokenGate::unconfigured(handle.clone(), pool.clone());
        let router = protect(app, gate.clone());

        let body = "username=admin&password=correct+horse+battery&confirm=correct+horse+battery";
        let req = Request::builder()
            .method(Method::POST)
            .uri("/setup")
            .header(header::CONTENT_TYPE, "application/x-www-form-urlencoded")
            .body(Body::from(body))
            .unwrap();
        let resp = router.clone().oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::SEE_OTHER);
        assert_eq!(resp.headers().get(header::LOCATION).unwrap(), "/");
        let cookie = resp
            .headers()
            .get(header::SET_COOKIE)
            .expect("setup mints a session")
            .to_str()
            .unwrap()
            .to_string();

        assert!(
            crate::web_admin::exists(&pool).await.unwrap(),
            "the administrator is persisted, not just held in memory"
        );
        assert!(
            gate.configured().await,
            "the live gate adopts the new administrator without a restart"
        );

        // That cookie now opens the app through the same router instance.
        let session = cookie.split(';').next().unwrap().to_string();
        let req = Request::builder()
            .uri("/")
            .header(header::COOKIE, session)
            .body(Body::empty())
            .unwrap();
        assert_eq!(router.oneshot(req).await.unwrap().status(), StatusCode::OK);
    }

    /// Setup runs once. A second POST cannot mint another administrator, whether
    /// or not the caller is signed in.
    #[tokio::test]
    async fn setup_is_refused_once_an_admin_exists() {
        let pool = setup_pool().await;
        crate::web_admin::create(&pool, "admin", "correct horse battery")
            .await
            .unwrap();
        let admin = crate::web_admin::load(&pool).await.unwrap().map(Arc::new);
        let handle: AdminHandle = Arc::new(tokio::sync::RwLock::new(admin));
        let router = protect(
            Router::new().route("/", get(|| async { "dashboard" })),
            TokenGate::unconfigured(handle, pool.clone()).with_admin(
                Arc::new(tokio::sync::RwLock::new(
                    crate::web_admin::load(&pool).await.unwrap().map(Arc::new),
                )),
                pool.clone(),
            ),
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
        let still = crate::web_admin::load(&pool).await.unwrap().unwrap();
        assert_eq!(still.username, "admin", "the original admin is untouched");
    }

    /// The DB administrator authenticates the login form, and its password is
    /// checked against the hash rather than any configured token.
    #[tokio::test]
    async fn admin_credentials_sign_in_through_the_login_form() {
        let pool = setup_pool().await;
        crate::web_admin::create(&pool, "admin", "correct horse battery")
            .await
            .unwrap();
        let handle: AdminHandle = Arc::new(tokio::sync::RwLock::new(
            crate::web_admin::load(&pool).await.unwrap().map(Arc::new),
        ));
        let gate = TokenGate::unconfigured(handle, pool.clone());
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
        let cookie = sign_session("s3cret", now_unix() + 60);
        assert!(verify_session("s3cret", &cookie));
        // Wrong token, garbage, and a non-numeric expiry all fail.
        assert!(!verify_session("other", &cookie));
        assert!(!verify_session("s3cret", "not-a-session"));
        assert!(!verify_session("s3cret", "abc.deadbeef"));
        // A flipped signature byte fails.
        let mut bad = cookie.clone();
        bad.pop();
        bad.push(if cookie.ends_with('0') { '1' } else { '0' });
        assert!(!verify_session("s3cret", &bad));
    }

    #[test]
    fn expired_session_is_rejected() {
        let cookie = sign_session("s3cret", now_unix() - 1);
        assert!(!verify_session("s3cret", &cookie));
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
}
