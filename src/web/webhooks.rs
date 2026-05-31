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
//!    [`VerifiedFailureEvent`] to [`consume_events`] -> 202. Every other
//!    authenticated event is acked -> 200 and dropped so Forgejo does not retry.
//!
//! CLAUDE-47 wires the fast-path end-to-end: [`consume_events`] composes the
//! sibling subtasks - log fetch + redaction (CLAUDE-44), the hybrid triage
//! engine (CLAUDE-45), and the triage-driven issue mint + dedupe + loop safety
//! (CLAUDE-46) - so a verified failure turns into a minted issue (bucket 1 ->
//! `AI Agent=Queued` for forgejo-ci-fix, bucket 2 -> a human-assigned issue) or
//! nothing (bucket 3, transient). The scheduled `youtrack` / `forgejo-ci-fix`
//! actions stay as the backstop and dedupe against webhook-minted issues through
//! the same [`crate::mint`] markers.
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

use crate::config::{MintConfig, WebhookConfig};
use crate::forge_log::LogOutcome;
use crate::job::RunnerDefaults;
use crate::mint::{self, MintCaps, MintOutcome, MintRequest};
use crate::provider::{Backend, ForgePullRequest};
use crate::secrets::{self, SecretsKey};
use crate::state;
use crate::triage::{self, FailureClassifier, TriageInput};

/// Synthetic action name the webhook fast-path records its DEV-335 caps under
/// (per-key attempt counter + daily run ledger in [`crate::state`]), so the
/// event-driven mint throttles on the same accounting shape as the scheduled
/// actions instead of re-deriving its own.
pub const WEBHOOK_MINT_ACTION: &str = "webhook-fast-path";

/// Wall-clock cap for one triage classification (`claude --print`). The pass is
/// a single bounded-log JSON reply, so it needs far less than a full code run.
pub const TRIAGE_TIMEOUT_SECS: u64 = 120;

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

/// Everything [`consume_events`] needs to triage a verified failure and mint
/// (or decline to mint) a YouTrack issue from it. Built once at daemon start
/// from the resolved provider backend (CLAUDE-46 `IssueTracker` + `Forge`), the
/// on-disk mint config, and the shared `claude` invoker behind a
/// [`FailureClassifier`]. When the daemon cannot build one (no usable `yt`/`fj`
/// backend) it passes `None` and the consumer falls back to log-only behaviour.
pub struct MintContext {
    /// State DB, for the reused DEV-335 caps ledger.
    pub pool: SqlitePool,
    /// The LLM pass for the residue the deterministic rules cannot label.
    pub classifier: Arc<dyn FailureClassifier>,
    /// Provider backend: the `IssueTracker` the mint writes through and the
    /// `Forge` the `has_open_pr` signal is read from.
    pub backend: Arc<dyn Backend>,
    /// Repo-to-project routing, loop-safety bot logins, bucket-2 assignee, etc.
    pub mint_cfg: MintConfig,
    /// Caps applied to the event-driven mint (per-key attempts + daily runs).
    pub defaults: RunnerDefaults,
}

/// Drain emitted events. For each verified failure this fetches the failing
/// job's log via the internal route, redacts secrets, and writes a redacted
/// artifact (CLAUDE-44); then, when a [`MintContext`] is configured, triages the
/// failure (CLAUDE-45) and mints / declines a YouTrack issue (CLAUDE-46). The
/// fetch is best-effort: a not-ready / pruned log is a transient miss, not a
/// fault. A miss does not strand the consumer - it feeds triage as a bucket-3
/// signal (the rules pass routes an unavailable log to transient).
pub async fn consume_events(
    mut rx: mpsc::Receiver<VerifiedFailureEvent>,
    transport: Arc<dyn crate::forge_log::LogTransport>,
    out_dir: PathBuf,
    mint: Option<MintContext>,
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
        let fetch_cfg = cfg.clone();
        let out_dir = out_dir.clone();
        let outcome = tokio::task::spawn_blocking(move || {
            crate::forge_log::fetch_failing_job_log(
                transport.as_ref(),
                &fetch_cfg,
                &target,
                &out_dir,
                &[],
                std::thread::sleep,
            )
        })
        .await;

        // Normalise the fetch result into the triage inputs. A `TransientMiss`,
        // a fetch error, and a panicked task all collapse to "no log" - which is
        // itself a bucket-3 signal - so a flaky fetch never blocks the pipeline.
        let (log_text, log_available, log_artifact) = match outcome {
            Ok(Ok(LogOutcome::Fetched(log))) => {
                info!(
                    repo = %ev.repo,
                    run = ev.index_in_repo,
                    bytes = log.text.len(),
                    artifact = %log.artifact.display(),
                    "fetched and redacted failing job log"
                );
                (log.text, true, Some(log.artifact))
            }
            Ok(Ok(LogOutcome::TransientMiss { reason })) => {
                warn!(repo = %ev.repo, run = ev.index_in_repo, %reason, "failing job log not available (transient miss)");
                (String::new(), false, None)
            }
            Ok(Err(e)) => {
                warn!(repo = %ev.repo, run = ev.index_in_repo, error = %e, "log fetch failed");
                (String::new(), false, None)
            }
            Err(e) => {
                warn!(repo = %ev.repo, run = ev.index_in_repo, error = %e, "log fetch task panicked");
                (String::new(), false, None)
            }
        };

        let Some(ctx) = mint.as_ref() else {
            // No mint configured (no usable backend): log-only, as before.
            continue;
        };
        triage_and_mint(ctx, &ev, log_text, log_available, log_artifact).await;
    }
}

/// Triage a verified failure and mint (or decline to mint) a YouTrack issue.
/// Reads the reused DEV-335 caps from the DB, resolves the `has_open_pr` signal
/// from the forge, runs the hybrid triage + mint on a blocking thread (every
/// step shells out), and advances the caps ledger only when an issue was
/// actually created. Logs the decision; never propagates an error to the loop.
async fn triage_and_mint(
    ctx: &MintContext,
    ev: &VerifiedFailureEvent,
    log_text: String,
    log_available: bool,
    log_artifact: Option<PathBuf>,
) {
    // DEV-335 caps, reused (not re-derived): the per-key counter is keyed by the
    // run's (repo, index); the daily ledger is per synthetic action. The
    // read-then-increment below is race-free because `consume_events` drains the
    // channel serially - one event is fully processed (this `.await` completes)
    // before the next is pulled - so no two mints read the same stale counters.
    let job_key = format!("{}#{}", ev.repo, ev.index_in_repo);
    let attempts = state::attempts_get(&ctx.pool, WEBHOOK_MINT_ACTION, &job_key)
        .await
        .unwrap_or(0);
    let daily = state::daily_count_get(&ctx.pool, WEBHOOK_MINT_ACTION)
        .await
        .unwrap_or(0);
    let caps = MintCaps {
        attempts,
        max_attempts: ctx.defaults.max_attempts_per_key,
        daily,
        max_runs_per_day: ctx.defaults.max_runs_per_day,
    };

    let backend = ctx.backend.clone();
    let classifier = ctx.classifier.clone();
    let mint_cfg = ctx.mint_cfg.clone();
    let event = ev.clone();
    let outcome = tokio::task::spawn_blocking(move || {
        let has_open_pr = resolve_has_open_pr(backend.as_ref(), &event);
        decide_mint(
            classifier.as_ref(),
            backend.as_ref(),
            &mint_cfg,
            &caps,
            &event,
            log_text,
            log_available,
            log_artifact.as_deref(),
            has_open_pr,
        )
    })
    .await;

    let outcome = match outcome {
        Ok(Ok(o)) => o,
        Ok(Err(e)) => {
            warn!(repo = %ev.repo, run = ev.index_in_repo, error = %e, "triage/mint failed");
            return;
        }
        Err(e) => {
            warn!(repo = %ev.repo, run = ev.index_in_repo, error = %e, "triage/mint task panicked");
            return;
        }
    };

    // Advance the caps ledger only on a real mint, so repeated failures for the
    // same key (and a busy day) throttle exactly like the scheduled path. Skips
    // (transient, dedupe, cap-hit, loop) create no issue and must not count.
    if matches!(outcome, MintOutcome::Created { .. }) {
        if let Err(e) = state::attempts_increment(&ctx.pool, WEBHOOK_MINT_ACTION, &job_key).await {
            warn!(error = %e, "advancing webhook attempt counter failed");
        }
        if let Err(e) = state::daily_count_increment(&ctx.pool, WEBHOOK_MINT_ACTION).await {
            warn!(error = %e, "advancing webhook daily ledger failed");
        }
    }

    log_mint_outcome(ev, &outcome);
}

/// Pure triage + mint composition over the two traits, with `has_open_pr`
/// already resolved. Factored out so the bucket routing and dedupe are unit
/// tested against stub a classifier / tracker with no forge, DB, or process
/// spawn. Builds the [`TriageInput`], runs the hybrid [`triage::triage`], then
/// [`mint::mint`] on the result.
#[allow(clippy::too_many_arguments)]
fn decide_mint(
    classifier: &dyn FailureClassifier,
    tracker: &dyn crate::provider::IssueTracker,
    mint_cfg: &MintConfig,
    caps: &MintCaps,
    ev: &VerifiedFailureEvent,
    log_text: String,
    log_available: bool,
    log_artifact: Option<&std::path::Path>,
    has_open_pr: bool,
) -> crate::error::Result<MintOutcome> {
    let input = TriageInput {
        repo: ev.repo.clone(),
        workflow_id: ev.workflow_id.clone(),
        commit_sha: ev.commit_sha.clone(),
        html_url: ev.html_url.clone(),
        log: log_text,
        log_available,
        has_open_pr,
    };
    let verdict = triage::triage(&input, classifier);
    let req = MintRequest {
        repo: &ev.repo,
        index_in_repo: ev.index_in_repo,
        commit_sha: &ev.commit_sha,
        workflow_id: &ev.workflow_id,
        html_url: &ev.html_url,
        trigger_user: &ev.trigger_user,
        triage: &verdict,
        log_path: log_artifact,
    };
    mint::mint(tracker, mint_cfg, &req, caps)
}

/// Best-effort `has_open_pr` signal for the triage classifier: list the repo's
/// open PRs and check whether the failing commit is the head of one. A forge
/// error or an underivable host degrades to `false` (treated as "no PR"), never
/// an error - the signal is advisory context for bucket 1 vs 2.
fn resolve_has_open_pr(backend: &dyn Backend, ev: &VerifiedFailureEvent) -> bool {
    let Some(host) = forge_host_from_url(&ev.html_url) else {
        warn!(repo = %ev.repo, html_url = %ev.html_url, "could not derive forge host for has_open_pr; assuming none");
        return false;
    };
    match backend.list_open_prs(&host, &ev.repo) {
        Ok(prs) => pr_matches_commit(&prs, &ev.commit_sha),
        Err(e) => {
            warn!(repo = %ev.repo, error = %e, "listing open PRs for has_open_pr failed; assuming none");
            false
        }
    }
}

/// True when some open PR's head commit matches `commit_sha`. Tolerates short /
/// long SHA mismatches by accepting a prefix relationship in either direction.
fn pr_matches_commit(prs: &[ForgePullRequest], commit_sha: &str) -> bool {
    prs.iter().any(|p| {
        !p.head_sha.is_empty()
            && (p.head_sha == commit_sha
                || commit_sha.starts_with(&p.head_sha)
                || p.head_sha.starts_with(commit_sha))
    })
}

/// Authority (`host[:port]`) of a forge `html_url`, the bare host the `fj`
/// forge backend addresses repos by (e.g. `dev.a8n.run`). `None` when the URL
/// has no host segment.
fn forge_host_from_url(url: &str) -> Option<String> {
    let after = url.split_once("://").map(|(_, rest)| rest).unwrap_or(url);
    let host = after.split('/').next().unwrap_or("");
    (!host.is_empty()).then(|| host.to_string())
}

/// Structured log line for one mint decision, one branch per [`MintOutcome`].
fn log_mint_outcome(ev: &VerifiedFailureEvent, outcome: &MintOutcome) {
    match outcome {
        MintOutcome::Created {
            key,
            project,
            bucket,
        } => {
            info!(repo = %ev.repo, run = ev.index_in_repo, %key, %project, bucket = %bucket, "minted YouTrack issue from verified failure")
        }
        MintOutcome::SkippedTransient => {
            info!(repo = %ev.repo, run = ev.index_in_repo, "transient failure; minted nothing")
        }
        MintOutcome::SkippedBotLoop => {
            info!(repo = %ev.repo, run = ev.index_in_repo, trigger_user = %ev.trigger_user, "bot-loop guard; minted nothing")
        }
        MintOutcome::SkippedAttemptCap => {
            warn!(repo = %ev.repo, run = ev.index_in_repo, "per-key attempt cap reached; minted nothing")
        }
        MintOutcome::SkippedDailyCap => {
            warn!(repo = %ev.repo, run = ev.index_in_repo, "daily run cap reached; minted nothing")
        }
        MintOutcome::SkippedDuplicateOpen { existing } => {
            info!(repo = %ev.repo, run = ev.index_in_repo, %existing, "open issue already exists; minted nothing")
        }
        MintOutcome::SkippedResolved { existing } => {
            info!(repo = %ev.repo, run = ev.index_in_repo, %existing, "resolved issue exists; not reopening")
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

    // ---- CLAUDE-47: triage + mint composition -----------------------------

    use crate::triage::{ClassifiedBy, Triage, TriageBucket};
    use std::path::Path;
    use std::sync::Mutex as StdMutex;

    /// Canned [`FailureClassifier`]: returns a fixed bucket, recording that it
    /// ran so a test can assert the deterministic rules short-circuited it.
    struct StubClassifier {
        bucket: TriageBucket,
        calls: StdMutex<u32>,
    }

    impl StubClassifier {
        fn new(bucket: TriageBucket) -> Self {
            Self {
                bucket,
                calls: StdMutex::new(0),
            }
        }
    }

    impl FailureClassifier for StubClassifier {
        fn classify(&self, _input: &TriageInput) -> crate::error::Result<Triage> {
            *self.calls.lock().unwrap() += 1;
            Ok(Triage {
                bucket: self.bucket,
                analysis: "stub analysis".to_string(),
                classified_by: ClassifiedBy::Llm,
            })
        }
    }

    /// Minimal [`IssueTracker`] recording the mint's mutations; dedupe searches
    /// always miss (no existing issue).
    struct StubTracker {
        calls: StdMutex<Vec<String>>,
    }

    impl StubTracker {
        fn new() -> Self {
            Self {
                calls: StdMutex::new(Vec::new()),
            }
        }
        fn calls(&self) -> Vec<String> {
            self.calls.lock().unwrap().clone()
        }
    }

    impl crate::provider::IssueTracker for StubTracker {
        fn list_queued(&self, query: &str) -> crate::error::Result<Vec<serde_json::Value>> {
            self.calls.lock().unwrap().push(format!("list {query}"));
            Ok(Vec::new())
        }
        fn get_field(&self, _key: &str, _field: &str) -> crate::error::Result<String> {
            unreachable!()
        }
        fn set_field(&self, key: &str, field: &str, value: &str) -> crate::error::Result<()> {
            self.calls
                .lock()
                .unwrap()
                .push(format!("set_field {key} {field} {value}"));
            Ok(())
        }
        fn set_state(&self, key: &str, state: &str) -> crate::error::Result<()> {
            self.calls
                .lock()
                .unwrap()
                .push(format!("set_state {key} {state}"));
            Ok(())
        }
        fn comment(&self, _key: &str, _body: &str) -> crate::error::Result<()> {
            unreachable!()
        }
        fn resolve_vcs(
            &self,
            _project_key: &str,
        ) -> crate::error::Result<crate::provider::VcsRepo> {
            unreachable!()
        }
        fn create_issue(
            &self,
            project: &str,
            _summary: &str,
            _description: &str,
            assignee: Option<&str>,
        ) -> crate::error::Result<String> {
            self.calls.lock().unwrap().push(format!(
                "create {project} assignee={}",
                assignee.unwrap_or("-")
            ));
            Ok("CLAUDE-99".to_string())
        }
        fn attach_file(&self, _key: &str, _file: &Path) -> crate::error::Result<()> {
            Ok(())
        }
    }

    fn event() -> VerifiedFailureEvent {
        VerifiedFailureEvent {
            repo: "fabrikam/forgejo-cli".to_string(),
            index_in_repo: 7,
            commit_sha: "abc123def456".to_string(),
            workflow_id: "ci.yml".to_string(),
            trigger_user: "alice".to_string(),
            html_url: "https://dev.a8n.run/fabrikam/forgejo-cli/actions/runs/7".to_string(),
        }
    }

    fn mint_cfg() -> MintConfig {
        MintConfig {
            repo_project_map: [("fabrikam/forgejo-cli".to_string(), "FJ".to_string())]
                .into_iter()
                .collect(),
            human_assignee: Some("bob".to_string()),
            ..MintConfig::default()
        }
    }

    fn caps() -> MintCaps {
        MintCaps {
            attempts: 0,
            max_attempts: 3,
            daily: 0,
            max_runs_per_day: 20,
        }
    }

    #[test]
    fn unavailable_log_short_circuits_to_transient_without_the_classifier() {
        // log_available = false -> the deterministic rules route to bucket 3, so
        // the LLM classifier must never run and nothing is minted.
        let classifier = StubClassifier::new(TriageBucket::CodeFix);
        let tracker = StubTracker::new();
        let out = decide_mint(
            &classifier,
            &tracker,
            &mint_cfg(),
            &caps(),
            &event(),
            String::new(),
            false,
            None,
            false,
        )
        .unwrap();
        assert_eq!(out, MintOutcome::SkippedTransient);
        assert_eq!(*classifier.calls.lock().unwrap(), 0, "rules short-circuit");
        assert!(tracker.calls().is_empty());
    }

    #[test]
    fn transient_signature_in_log_mints_nothing() {
        // A clear infra signature is bucket 3 by the rules pass, even with a log.
        let classifier = StubClassifier::new(TriageBucket::CodeFix);
        let tracker = StubTracker::new();
        let out = decide_mint(
            &classifier,
            &tracker,
            &mint_cfg(),
            &caps(),
            &event(),
            "error: no runner available for this job".to_string(),
            true,
            None,
            true,
        )
        .unwrap();
        assert_eq!(out, MintOutcome::SkippedTransient);
        assert_eq!(*classifier.calls.lock().unwrap(), 0);
    }

    #[test]
    fn code_fix_classification_mints_and_queues() {
        // A non-transient log + a code_fix verdict -> issue in the mapped project,
        // queued for the fix bot, no human assignee.
        let classifier = StubClassifier::new(TriageBucket::CodeFix);
        let tracker = StubTracker::new();
        let out = decide_mint(
            &classifier,
            &tracker,
            &mint_cfg(),
            &caps(),
            &event(),
            "assertion failed: expected 1 got 2".to_string(),
            true,
            None,
            true,
        )
        .unwrap();
        assert_eq!(
            out,
            MintOutcome::Created {
                key: "CLAUDE-99".to_string(),
                project: "FJ".to_string(),
                bucket: TriageBucket::CodeFix,
            }
        );
        assert_eq!(*classifier.calls.lock().unwrap(), 1, "classifier ran once");
        let calls = tracker.calls();
        assert!(
            calls.iter().any(|c| c == "create FJ assignee=-"),
            "{calls:?}"
        );
        assert!(
            calls
                .iter()
                .any(|c| c == "set_field CLAUDE-99 AI Agent Queued"),
            "{calls:?}"
        );
    }

    #[test]
    fn human_action_classification_assigns_a_human() {
        let classifier = StubClassifier::new(TriageBucket::HumanAction);
        let tracker = StubTracker::new();
        let out = decide_mint(
            &classifier,
            &tracker,
            &mint_cfg(),
            &caps(),
            &event(),
            "the release tag push to main failed".to_string(),
            true,
            None,
            false,
        )
        .unwrap();
        assert!(matches!(
            out,
            MintOutcome::Created {
                bucket: TriageBucket::HumanAction,
                ..
            }
        ));
        let calls = tracker.calls();
        assert!(
            calls.iter().any(|c| c == "create FJ assignee=bob"),
            "{calls:?}"
        );
        assert!(!calls.iter().any(|c| c.contains("AI Agent")), "{calls:?}");
    }

    #[test]
    fn pr_matches_commit_handles_exact_and_prefix() {
        let pr = |sha: &str| ForgePullRequest {
            number: 1,
            title: String::new(),
            head_sha: sha.to_string(),
            head_ref: String::new(),
            author: String::new(),
            html_url: String::new(),
            updated_at: String::new(),
        };
        assert!(pr_matches_commit(&[pr("abc123")], "abc123"));
        // Full failing SHA, short PR head SHA -> prefix match still counts.
        assert!(pr_matches_commit(&[pr("abc123")], "abc123def456"));
        assert!(!pr_matches_commit(&[pr("def999")], "abc123def456"));
        // An empty head_sha never matches.
        assert!(!pr_matches_commit(&[pr("")], "abc123"));
        assert!(!pr_matches_commit(&[], "abc123"));
    }

    #[test]
    fn forge_host_from_url_extracts_authority() {
        assert_eq!(
            forge_host_from_url("https://dev.a8n.run/owner/repo/actions/runs/7").as_deref(),
            Some("dev.a8n.run")
        );
        assert_eq!(
            forge_host_from_url("http://localhost:3000/o/r").as_deref(),
            Some("localhost:3000")
        );
        assert_eq!(forge_host_from_url("").as_deref(), None);
    }
}
