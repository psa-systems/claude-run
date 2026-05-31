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

use axum::{
    extract::{Request, State},
    http::{header, StatusCode},
    middleware::Next,
    response::{IntoResponse, Response},
    Router,
};
use sqlx::SqlitePool;

use crate::config::WebConfig;
use crate::error::{Error, Result};

/// Holds the expected bearer token, shared (cheaply cloned) into the middleware.
#[derive(Clone)]
pub struct TokenGate {
    token: Arc<String>,
}

impl TokenGate {
    pub fn new(token: String) -> Self {
        Self {
            token: Arc::new(token),
        }
    }
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
    Ok(Some(TokenGate::new(token)))
}

/// Wrap `router` so every request must carry `Authorization: Bearer <token>`
/// matching the gate, except requests under the `/webhooks/` prefix.
pub fn protect(router: Router, gate: TokenGate) -> Router {
    router.layer(axum::middleware::from_fn_with_state(gate, require_token))
}

/// Axum middleware enforcing the bearer token. Exempts the webhook prefix; all
/// other paths get a 401 unless a matching token is presented.
async fn require_token(State(gate): State<TokenGate>, request: Request, next: Next) -> Response {
    // The webhook route authenticates itself via HMAC and Forgejo cannot send a
    // bearer token, so it is never gated here.
    if request.uri().path().starts_with("/webhooks/") {
        return next.run(request).await;
    }
    if let Some(presented) = bearer_token(&request) {
        if constant_time_eq(presented.as_bytes(), gate.token.as_bytes()) {
            return next.run(request).await;
        }
    }
    (
        StatusCode::UNAUTHORIZED,
        [(header::WWW_AUTHENTICATE, "Bearer")],
        "missing or invalid bearer token",
    )
        .into_response()
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
}
