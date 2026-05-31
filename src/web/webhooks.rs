//! Forgejo webhook listener (CLAUDE-43).
//!
//! A single `POST /webhooks/forgejo` route on the daemon's TCP app receives
//! Forgejo Action-run webhook deliveries. The pipeline, in order:
//!
//! 1. Rate limit (fixed window, before any crypto) -> 429 when exceeded.
//! 2. Resolve the configured HMAC secret (CLAUDE-26 `secrets` table); a missing
//!    `secret_ref` or unresolvable secret fails closed -> 401.
//! 3. Verify the HMAC-SHA256 over the raw body against `X-Forgejo-Signature`
//!    with a constant-time compare -> 401 on mismatch / missing signature.
//! 4. Replay protection: reject a delivery whose `X-Forgejo-Delivery` id was
//!    already seen inside the configured window -> 409. A missing id is a
//!    malformed request -> 400.
//! 5. Parse the `ActionPayload`. Keep only `action_run_failure` (payload
//!    `action == "failure"`) for an allow-listed repo and emit a typed
//!    [`VerifiedFailureEvent`] to the next stage (log fetch / triage, a sibling
//!    subtask) -> 202. Every other authenticated event is acked -> 200 and
//!    dropped so Forgejo does not retry.
//!
//! TLS / reverse proxy and service authentication are out of scope here per the
//! epic decision (handled by the external proxy / the linked auth issue).

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

use axum::body::Bytes;
use axum::extract::State;
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::post;
use axum::Router;
use hmac::{Hmac, Mac};
use serde::{Deserialize, Serialize};
use sha2::Sha256;
use sqlx::SqlitePool;
use tokio::sync::{mpsc, Mutex};
use tracing::{info, warn};

use crate::config::WebhookConfig;
use crate::secrets::{self, SecretsKey};

/// Hex-encoded HMAC-SHA256 of the raw body, keyed by the webhook secret.
pub const SIGNATURE_HEADER: &str = "X-Forgejo-Signature";
/// Per-delivery unique id, used for replay protection.
pub const DELIVERY_HEADER: &str = "X-Forgejo-Delivery";

/// Subset of the Forgejo Action-run webhook payload we consume. Extra fields in
/// the delivery are ignored (no `deny_unknown_fields`) so a Forgejo upgrade that
/// adds keys does not break ingestion.
#[derive(Debug, Clone, Deserialize)]
pub struct ActionPayload {
    /// One of `failure`, `success`, `recover`. Only `failure` is kept.
    pub action: String,
    pub run: ActionRun,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ActionRun {
    pub index_in_repo: i64,
    pub commit_sha: String,
    pub workflow_id: String,
    pub status: String,
    pub trigger_user: ActionUser,
    pub html_url: String,
    pub repository: Repository,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ActionUser {
    /// Forgejo user objects carry both `login` and `username`; accept either.
    #[serde(alias = "username")]
    pub login: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct Repository {
    /// `owner/repo`, matched against the configured allow-list.
    pub full_name: String,
}

/// The typed event emitted downstream for a verified, allow-listed failure.
/// Carries exactly the fields the next stage (log fetch / triage) needs.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct VerifiedFailureEvent {
    pub repo: String,
    pub index_in_repo: i64,
    pub commit_sha: String,
    pub workflow_id: String,
    pub trigger_user: String,
    pub html_url: String,
}

/// State injected into the webhook handler. Cheap to clone (`SqlitePool` and
/// `Sender` are `Arc`-shaped; the config is `Arc`-wrapped).
#[derive(Clone)]
pub struct WebhookState {
    pool: SqlitePool,
    config: Arc<WebhookConfig>,
    events: mpsc::Sender<VerifiedFailureEvent>,
    seen: Arc<Mutex<SeenDeliveries>>,
    rate: Arc<Mutex<RateLimiter>>,
}

impl WebhookState {
    pub fn new(
        pool: SqlitePool,
        config: WebhookConfig,
        events: mpsc::Sender<VerifiedFailureEvent>,
    ) -> Self {
        let seen = SeenDeliveries::new(Duration::from_secs(config.replay_window_secs));
        let rate = RateLimiter::new(config.rate_limit_per_min, Duration::from_secs(60));
        Self {
            pool,
            config: Arc::new(config),
            events,
            seen: Arc::new(Mutex::new(seen)),
            rate: Arc::new(Mutex::new(rate)),
        }
    }
}

/// Router carrying the single webhook route, merged into the daemon's TCP app.
pub fn router(state: WebhookState) -> Router {
    Router::new()
        .route("/webhooks/forgejo", post(forgejo_webhook))
        .with_state(state)
}

/// Drain emitted events. For each verified failure this fetches the failing
/// job's log via the internal route, redacts secrets, and writes a redacted
/// artifact (CLAUDE-44). Triage (turning the redacted log into an enriched task)
/// is a sibling subtask; this slice logs the fetch outcome. The fetch is
/// best-effort: a not-ready / pruned log is a transient miss, not a fault, and
/// never strands the consumer.
pub async fn consume_events(
    mut rx: mpsc::Receiver<VerifiedFailureEvent>,
    transport: Arc<dyn crate::forge_log::LogTransport>,
    out_dir: PathBuf,
) {
    let cfg = crate::forge_log::LogFetchConfig::default();
    while let Some(ev) = rx.recv().await {
        info!(
            repo = %ev.repo,
            run = ev.index_in_repo,
            commit = %ev.commit_sha,
            workflow = %ev.workflow_id,
            trigger_user = %ev.trigger_user,
            html_url = %ev.html_url,
            "verified action_run_failure received"
        );

        let Some(target) =
            crate::forge_log::LogTarget::for_run(&ev.html_url, &ev.repo, ev.index_in_repo)
        else {
            warn!(repo = %ev.repo, html_url = %ev.html_url, "could not derive forge origin from html_url; skipping log fetch");
            continue;
        };

        // The fetch is blocking (reqwest on a thread + backoff sleeps); run it
        // off the async runtime. No service-injected secrets reach the CI run,
        // so `known_secrets` is empty here and pattern redaction does the work.
        let transport = transport.clone();
        let cfg = cfg.clone();
        let out_dir = out_dir.clone();
        let outcome = tokio::task::spawn_blocking(move || {
            crate::forge_log::fetch_failing_job_log(
                transport.as_ref(),
                &cfg,
                &target,
                &out_dir,
                &[],
                std::thread::sleep,
            )
        })
        .await;

        match outcome {
            Ok(Ok(crate::forge_log::LogOutcome::Fetched(log))) => info!(
                repo = %ev.repo,
                run = ev.index_in_repo,
                bytes = log.text.len(),
                artifact = %log.artifact.display(),
                "fetched and redacted failing job log"
            ),
            Ok(Ok(crate::forge_log::LogOutcome::TransientMiss { reason })) => warn!(
                repo = %ev.repo,
                run = ev.index_in_repo,
                %reason,
                "failing job log not available (transient miss)"
            ),
            Ok(Err(e)) => {
                warn!(repo = %ev.repo, run = ev.index_in_repo, error = %e, "log fetch failed")
            }
            Err(e) => {
                warn!(repo = %ev.repo, run = ev.index_in_repo, error = %e, "log fetch task panicked")
            }
        }
    }
}

async fn forgejo_webhook(
    State(st): State<WebhookState>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let now = Instant::now();

    // Rate limit first: cheapest check, shields the HMAC + secret-decrypt work
    // from a request flood.
    if !st.rate.lock().await.allow(now) {
        warn!("forgejo webhook rate limited");
        return StatusCode::TOO_MANY_REQUESTS.into_response();
    }

    // Resolve the HMAC secret. Fail closed: an unset ref or an unresolvable
    // secret means we cannot authenticate, so reject rather than accept.
    let secret = match resolve_secret(&st).await {
        Ok(s) => s,
        Err(e) => {
            warn!(error = %e, "forgejo webhook secret unavailable; rejecting");
            return StatusCode::UNAUTHORIZED.into_response();
        }
    };

    // Verify the signature over the RAW body before any parsing.
    let signature = headers.get(SIGNATURE_HEADER).and_then(|v| v.to_str().ok());
    let Some(signature) = signature else {
        return StatusCode::UNAUTHORIZED.into_response();
    };
    if !verify_signature(secret.as_bytes(), &body, signature) {
        return StatusCode::UNAUTHORIZED.into_response();
    }

    // Replay protection on authenticated requests only. A missing delivery id
    // is malformed; a previously-seen id inside the window is a replay.
    let delivery = headers
        .get(DELIVERY_HEADER)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    if delivery.is_empty() {
        return StatusCode::BAD_REQUEST.into_response();
    }
    if !st.seen.lock().await.check_and_record(delivery, now) {
        warn!(delivery, "forgejo webhook replay rejected");
        return StatusCode::CONFLICT.into_response();
    }

    // Parse only after the request is authenticated and de-duplicated.
    let payload: ActionPayload = match serde_json::from_slice(&body) {
        Ok(p) => p,
        Err(e) => {
            warn!(error = %e, "forgejo webhook payload parse failed");
            return StatusCode::BAD_REQUEST.into_response();
        }
    };

    match verified_failure_event(&payload, &st.config.allowed_repos) {
        Some(event) => {
            if st.events.send(event).await.is_err() {
                warn!("forgejo webhook event channel closed; dropping event");
            }
            StatusCode::ACCEPTED.into_response()
        }
        // Authenticated but not an allow-listed failure: ack + drop so Forgejo
        // marks the delivery delivered and does not retry.
        None => StatusCode::OK.into_response(),
    }
}

/// Resolve the configured webhook secret to plaintext, loading the age key from
/// `CLAUDE_RUN_SECRETS_KEY_FILE`. Errors when no `secret_ref` is configured or
/// the secret cannot be decrypted.
async fn resolve_secret(st: &WebhookState) -> crate::error::Result<String> {
    let name = st.config.secret_ref.as_deref().ok_or_else(|| {
        crate::error::Error::Secret("webhook.secret_ref is not configured".to_string())
    })?;
    let key = SecretsKey::load()?;
    secrets::resolve_secret(&st.pool, &key, name).await
}

/// Constant-time HMAC-SHA256 verification of `body` against the hex-encoded
/// `signature` under `secret`. Returns false on any decode/key/length error so
/// malformed input is indistinguishable from a bad signature.
pub fn verify_signature(secret: &[u8], body: &[u8], signature: &str) -> bool {
    let expected = match hex::decode(signature.trim()) {
        Ok(bytes) => bytes,
        Err(_) => return false,
    };
    let mut mac = match Hmac::<Sha256>::new_from_slice(secret) {
        Ok(mac) => mac,
        Err(_) => return false,
    };
    mac.update(body);
    mac.verify_slice(&expected).is_ok()
}

/// Build a [`VerifiedFailureEvent`] when the payload is an `action_run_failure`
/// for an allow-listed repo; `None` otherwise (caller acks and drops).
pub fn verified_failure_event(
    payload: &ActionPayload,
    allowed_repos: &[String],
) -> Option<VerifiedFailureEvent> {
    if payload.action != "failure" {
        return None;
    }
    let repo = &payload.run.repository.full_name;
    if !allowed_repos.iter().any(|r| r == repo) {
        return None;
    }
    Some(VerifiedFailureEvent {
        repo: repo.clone(),
        index_in_repo: payload.run.index_in_repo,
        commit_sha: payload.run.commit_sha.clone(),
        workflow_id: payload.run.workflow_id.clone(),
        trigger_user: payload.run.trigger_user.login.clone(),
        html_url: payload.run.html_url.clone(),
    })
}

/// Time-bounded set of delivery ids for replay protection. Entries older than
/// the window are evicted lazily on each check, so the map stays bounded by the
/// delivery rate within one window.
pub struct SeenDeliveries {
    window: Duration,
    seen: HashMap<String, Instant>,
}

impl SeenDeliveries {
    pub fn new(window: Duration) -> Self {
        Self {
            window,
            seen: HashMap::new(),
        }
    }

    /// Record `id` as seen at `now`. Returns true if it is fresh (first sighting
    /// within the window), false if it is a replay.
    pub fn check_and_record(&mut self, id: &str, now: Instant) -> bool {
        self.seen
            .retain(|_, seen_at| now.duration_since(*seen_at) < self.window);
        if self.seen.contains_key(id) {
            return false;
        }
        self.seen.insert(id.to_string(), now);
        true
    }
}

/// Fixed-window rate limiter: at most `max` allowances per `window`.
pub struct RateLimiter {
    max: u32,
    window: Duration,
    window_start: Instant,
    count: u32,
}

impl RateLimiter {
    pub fn new(max: u32, window: Duration) -> Self {
        Self {
            max,
            window,
            // Start the first window in the past so `allow` resets it on the
            // first call regardless of when the limiter was constructed.
            window_start: Instant::now() - window,
            count: 0,
        }
    }

    /// True if a request at `now` is within the current window's budget.
    pub fn allow(&mut self, now: Instant) -> bool {
        if now.duration_since(self.window_start) >= self.window {
            self.window_start = now;
            self.count = 0;
        }
        if self.count >= self.max {
            return false;
        }
        self.count += 1;
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const SECRET: &[u8] = b"super-secret-webhook-key";

    fn sign(secret: &[u8], body: &[u8]) -> String {
        let mut mac = Hmac::<Sha256>::new_from_slice(secret).unwrap();
        mac.update(body);
        hex::encode(mac.finalize().into_bytes())
    }

    fn failure_payload(repo: &str) -> String {
        format!(
            r#"{{"action":"failure","run":{{"index_in_repo":7,"commit_sha":"abc123",
            "workflow_id":"ci.yml","status":"failure","trigger_user":{{"login":"alice"}},
            "html_url":"https://dev.a8n.run/{repo}/actions/runs/7",
            "repository":{{"full_name":"{repo}"}}}}}}"#
        )
    }

    #[test]
    fn valid_signature_verifies() {
        let body = failure_payload("contoso/claude-run");
        let sig = sign(SECRET, body.as_bytes());
        assert!(verify_signature(SECRET, body.as_bytes(), &sig));
    }

    #[test]
    fn wrong_secret_or_tampered_body_fails() {
        let body = failure_payload("contoso/claude-run");
        let sig = sign(SECRET, body.as_bytes());
        assert!(!verify_signature(b"other-key", body.as_bytes(), &sig));
        assert!(!verify_signature(SECRET, b"tampered", &sig));
    }

    #[test]
    fn malformed_signature_fails_not_panics() {
        let body = failure_payload("contoso/claude-run");
        assert!(!verify_signature(SECRET, body.as_bytes(), "not-hex-zz"));
        assert!(!verify_signature(SECRET, body.as_bytes(), ""));
    }

    #[test]
    fn allow_listed_failure_emits_event() {
        let payload: ActionPayload =
            serde_json::from_str(&failure_payload("contoso/claude-run")).unwrap();
        let allowed = vec!["contoso/claude-run".to_string()];
        let event = verified_failure_event(&payload, &allowed).expect("emits event");
        assert_eq!(
            event,
            VerifiedFailureEvent {
                repo: "contoso/claude-run".to_string(),
                index_in_repo: 7,
                commit_sha: "abc123".to_string(),
                workflow_id: "ci.yml".to_string(),
                trigger_user: "alice".to_string(),
                html_url: "https://dev.a8n.run/contoso/claude-run/actions/runs/7".to_string(),
            }
        );
    }

    #[test]
    fn failure_from_unlisted_repo_is_dropped() {
        let payload: ActionPayload =
            serde_json::from_str(&failure_payload("someone/else")).unwrap();
        let allowed = vec!["contoso/claude-run".to_string()];
        assert!(verified_failure_event(&payload, &allowed).is_none());
    }

    #[test]
    fn non_failure_action_is_dropped_even_when_listed() {
        let body = r#"{"action":"success","run":{"index_in_repo":1,"commit_sha":"x",
            "workflow_id":"ci.yml","status":"success","trigger_user":{"login":"bob"},
            "html_url":"https://dev.a8n.run/contoso/claude-run/actions/runs/1",
            "repository":{"full_name":"contoso/claude-run"}}}"#;
        let payload: ActionPayload = serde_json::from_str(body).unwrap();
        let allowed = vec!["contoso/claude-run".to_string()];
        assert!(verified_failure_event(&payload, &allowed).is_none());
    }

    #[test]
    fn trigger_user_accepts_username_alias() {
        let body = r#"{"action":"failure","run":{"index_in_repo":1,"commit_sha":"x",
            "workflow_id":"ci.yml","status":"failure","trigger_user":{"username":"carol"},
            "html_url":"https://dev.a8n.run/contoso/claude-run/actions/runs/1",
            "repository":{"full_name":"contoso/claude-run"}}}"#;
        let payload: ActionPayload = serde_json::from_str(body).unwrap();
        let allowed = vec!["contoso/claude-run".to_string()];
        let event = verified_failure_event(&payload, &allowed).unwrap();
        assert_eq!(event.trigger_user, "carol");
    }

    #[test]
    fn replay_of_same_delivery_is_rejected() {
        let mut seen = SeenDeliveries::new(Duration::from_secs(300));
        let t0 = Instant::now();
        assert!(seen.check_and_record("delivery-1", t0), "first is fresh");
        assert!(
            !seen.check_and_record("delivery-1", t0),
            "second is a replay"
        );
        assert!(
            seen.check_and_record("delivery-2", t0),
            "a different id is fresh"
        );
    }

    #[test]
    fn delivery_id_is_fresh_again_after_window() {
        let window = Duration::from_secs(300);
        let mut seen = SeenDeliveries::new(window);
        let t0 = Instant::now();
        assert!(seen.check_and_record("delivery-1", t0));
        // Past the window the old entry is evicted, so the id is fresh again.
        let later = t0 + window + Duration::from_secs(1);
        assert!(seen.check_and_record("delivery-1", later));
    }

    #[test]
    fn rate_limiter_caps_per_window_and_resets() {
        let window = Duration::from_secs(60);
        let mut rl = RateLimiter::new(2, window);
        let t0 = Instant::now();
        assert!(rl.allow(t0));
        assert!(rl.allow(t0));
        assert!(!rl.allow(t0), "third in the same window is blocked");
        // Next window resets the budget.
        let next = t0 + window + Duration::from_secs(1);
        assert!(rl.allow(next));
    }
}
