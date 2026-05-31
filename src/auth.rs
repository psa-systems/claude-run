//! Phase 5 (CLAUDE-48): bearer-token auth for the daemon's TCP-exposed HTTP
//! surface.
//!
//! The daemon binds its axum router to loopback by default. CLAUDE-36 exposes
//! that surface through the external reverse proxy so a `/webhooks/forgejo`
//! route is reachable, which also exposes the web UI and `/api/*`. This module
//! gates that surface behind a bearer token whenever the effective bind is
//! non-loopback (or auth is explicitly required on loopback for testing).
//!
//! Exemptions: the `/webhooks/` prefix is never gated. Forgejo cannot send a
//! bearer token, and the webhook carries its own HMAC verification.
//!
//! The token is a CLAUDE-26 secret named by `web.auth_token_ref`; it is
//! resolved once at daemon startup (the middleware needs it synchronously per
//! request) and never read from the config file in plaintext. When auth is
//! required but no token resolves, the daemon refuses to bring up the TCP
//! listener at all (fail closed) rather than expose an unauthenticated surface.

use std::net::IpAddr;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

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
use sqlx::SqlitePool;

use crate::config::WebConfig;
use crate::error::{Error, Result};

/// Session cookie name set on a successful form login and accepted by the gate
/// in lieu of a bearer header.
const SESSION_COOKIE: &str = "claude_run_session";
/// How long a login session stays valid before the browser must sign in again.
const SESSION_TTL_SECS: u64 = 12 * 3600;

/// Holds the expected bearer token (and, when form login is enabled, the
/// configured username), shared (cheaply cloned) into the middleware.
#[derive(Clone)]
pub struct TokenGate {
    token: Arc<String>,
    /// `Some` when `web.auth_username` is set: enables the `/login` form and the
    /// redirect-to-login behavior for unauthenticated browser requests.
    username: Option<Arc<String>>,
}

impl TokenGate {
    /// Bearer-only gate (no form login). Used by tests and any caller that does
    /// not configure a username.
    pub fn new(token: String) -> Self {
        Self {
            token: Arc::new(token),
            username: None,
        }
    }

    /// Gate with the browser login form enabled for `username`.
    pub fn with_login(token: String, username: String) -> Self {
        Self {
            token: Arc::new(token),
            username: Some(Arc::new(username)),
        }
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

/// Resolve the token gate for this daemon run from the web config + secrets.
///
/// - `Ok(None)`: auth not required (loopback bind); serve the TCP surface
///   unauthenticated, as before.
/// - `Ok(Some(gate))`: auth required and the token resolved; gate the surface.
/// - `Err(_)`: auth required but the token could not be resolved (no
///   `auth_token_ref`, missing key file, unknown secret). The caller fails
///   closed: do not expose the surface.
pub async fn resolve_gate(web: &WebConfig, pool: &SqlitePool) -> Result<Option<TokenGate>> {
    if !auth_required(web) {
        return Ok(None);
    }
    let name = web.auth_token_ref.as_deref().ok_or_else(|| {
        Error::Secret(
            "web.bind is non-loopback but web.auth_token_ref is unset; set it to the name of a \
             stored secret holding the bearer token (see CLAUDE-26 secrets)"
                .to_string(),
        )
    })?;
    let key = crate::secrets::SecretsKey::load()?;
    let token = crate::secrets::resolve_secret(pool, &key, name).await?;
    if token.is_empty() {
        return Err(Error::Secret(format!(
            "secret `{name}` (web.auth_token_ref) is empty; refusing to gate with an empty token"
        )));
    }
    let gate = match web.auth_username.as_deref() {
        Some(u) if !u.trim().is_empty() => TokenGate::with_login(token, u.trim().to_string()),
        _ => TokenGate::new(token),
    };
    Ok(Some(gate))
}

/// Wrap `router` so every request must carry either `Authorization: Bearer
/// <token>` or a valid session cookie, except requests under the `/webhooks/`
/// prefix and the login routes. When the gate has a configured username, the
/// `/login` (+ `/logout`) form routes are mounted so a browser / password
/// manager can sign in.
pub fn protect(router: Router, gate: TokenGate) -> Router {
    let mut router = router;
    if let Some(username) = gate.username.clone() {
        let login = Router::new()
            .route("/login", get(login_page).post(login_submit))
            .route("/logout", get(logout))
            .with_state(LoginState {
                token: gate.token.clone(),
                username,
            });
        router = router.merge(login);
    }
    router.layer(axum::middleware::from_fn_with_state(gate, require_token))
}

/// Axum middleware enforcing the bearer token or session cookie. Exempts the
/// webhook prefix and the login routes. An unauthenticated browser navigation
/// (GET accepting HTML) is redirected to `/login` when the form is enabled;
/// everything else gets a 401.
async fn require_token(State(gate): State<TokenGate>, request: Request, next: Next) -> Response {
    // The webhook route authenticates itself via HMAC and Forgejo cannot send a
    // bearer token, so it is never gated here. The login routes must be reachable
    // unauthenticated or there is no way in.
    let path = request.uri().path();
    if path.starts_with("/webhooks/") || path == "/login" || path == "/logout" {
        return next.run(request).await;
    }
    if let Some(presented) = bearer_token(&request) {
        if constant_time_eq(presented.as_bytes(), gate.token.as_bytes()) {
            return next.run(request).await;
        }
    }
    if let Some(cookie) = session_cookie(&request) {
        if verify_session(&gate.token, &cookie) {
            return next.run(request).await;
        }
    }
    // Send browsers to the login form (when enabled) instead of a bare 401, so a
    // password manager can complete the sign-in. API clients still get 401.
    if gate.username.is_some() && request.method() == Method::GET && wants_html(&request) {
        return Redirect::to("/login").into_response();
    }
    (
        StatusCode::UNAUTHORIZED,
        [(header::WWW_AUTHENTICATE, "Bearer")],
        "missing or invalid bearer token",
    )
        .into_response()
}

/// State for the login routes: the expected password (the resolved bearer
/// token) and the configured username.
#[derive(Clone)]
struct LoginState {
    token: Arc<String>,
    username: Arc<String>,
}

#[derive(Template)]
#[template(path = "login.html")]
struct LoginTpl {
    error: Option<String>,
}

#[derive(Deserialize)]
struct LoginForm {
    username: String,
    password: String,
}

/// GET `/login`: render the sign-in form.
async fn login_page() -> Response {
    render_login(None, StatusCode::OK)
}

/// POST `/login`: verify username + password (the `auth_token_ref` secret),
/// mint a session cookie, and redirect to the dashboard. On mismatch, re-render
/// the form with a 401 and a generic error (no hint which field was wrong).
async fn login_submit(State(st): State<LoginState>, Form(form): Form<LoginForm>) -> Response {
    let user_ok = constant_time_eq(form.username.as_bytes(), st.username.as_bytes());
    let pass_ok = constant_time_eq(form.password.as_bytes(), st.token.as_bytes());
    if !(user_ok && pass_ok) {
        return render_login(
            Some("Invalid username or password.".to_string()),
            StatusCode::UNAUTHORIZED,
        );
    }
    let mut resp = Redirect::to("/").into_response();
    if let Ok(v) = HeaderValue::from_str(&session_set_cookie(&st.token)) {
        resp.headers_mut().insert(header::SET_COOKIE, v);
    }
    resp
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
            .route("/webhooks/forgejo", get(|| async { "hook" }));
        protect(app, TokenGate::new(secret.to_string()))
    }

    async fn status_of(router: Router, req: Request) -> StatusCode {
        router.oneshot(req).await.unwrap().status()
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
            .uri("/webhooks/forgejo")
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
