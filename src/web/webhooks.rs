//! CI-failure triage webhook listener.
//!
//! `POST /webhooks/{provider}/triage/{source}` routes on the daemon's TCP app
//! receive Action-run webhook deliveries. `{provider}` names the sending forge
//! ([`WebhookProvider`]; `forgejo` and `github` are both implemented,
//! CLAUDE-362 adding `github`). Each `{source}`
//! names a configured [`crate::config::WebhookSource`] with its own HMAC
//! secret, allow-list, and rate-limit bucket; the replay store is shared
//! across sources (delivery ids are globally unique). The per-request
//! pipeline, in order:
//!
//! 0. Resolve `{provider}` to a [`WebhookProvider`]; unrecognized -> 404.
//!    Resolve `{source}` from the config sources map; an unknown source, or a
//!    source whose own configured `provider` does not match `{provider}`,
//!    -> 404.
//! 1. Per-source rate limit (fixed window, before any crypto) -> 429 when
//!    exceeded. One source's storm never starves another's bucket.
//! 2. Resolve that source's HMAC secret (`secrets` table); a missing
//!    `secret_ref` or unresolvable secret fails closed -> 401.
//! 3. Verify the signature over the raw body against the provider's own
//!    signature header (Forgejo: `X-Forgejo-Signature`, HMAC-SHA256, constant-time
//!    compare) -> 401 on mismatch / missing signature. The secret is the
//!    source's own, so a payload signed for one source's secret 401s on a
//!    different source's path.
//! 4. Replay protection (shared store): reject a delivery whose delivery-id
//!    header (Forgejo: `X-Forgejo-Delivery`) was already seen inside the
//!    configured window -> 409. A missing id is a malformed request -> 400.
//! 5. Parse the `ActionPayload`. Keep only `action_run_failure` (payload
//!    `action == "failure"`) for a repo in that source's allow-list and emit a
//!    typed [`VerifiedFailureEvent`] to [`consume_events`] -> 202. Every other
//!    authenticated event is acked -> 200 and dropped so the forge does not retry.
//!
//! Every per-request log line carries the `source` field so a rejection or ack
//! is always attributable to its endpoint.
//!
//! [`consume_events`] composes the sibling subtasks (log fetch + redaction, the
//! hybrid triage engine, and the triage-driven issue mint + dedupe + loop safety)
//! so a verified failure turns into a minted issue (bucket 1 ->
//! `AI Agent=Queued`, picked up by the `youtrack` action, bucket 2 -> a
//! human-assigned issue) or nothing (bucket 3, transient). Nothing polls for
//! CI failures, so a delivery this module drops is not recovered.
//!
//! TLS / reverse proxy and service authentication are out of scope here per the
//! epic decision (handled by the external proxy / the linked auth issue).

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::db::DbPool;
use axum::body::Bytes;
use axum::extract::{Path, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::post;
use axum::Router;
use hmac::{Hmac, Mac};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tokio::sync::{mpsc, Mutex, RwLock};
use tracing::{debug, error, info, warn};

use crate::ci_failure::{fingerprint, store as ci_failure_store};
use crate::config::{MintConfig, Paths, WebhookConfig, WebhookSource};
use crate::forge_log::LogOutcome;
use crate::job::RunnerDefaults;
use crate::mint::{self, MintCaps, MintOutcome, MintRequest, VcsRouteCache};
use crate::provider::{
    is_github_host, Backend, CliBackend, CommandRunner, Forge, ForgePullRequest, GithubForge,
};
use crate::secrets::{self, SecretsKey};
use crate::state;
use crate::triage::{self, FailureClassifier, TriageInput};

/// Synthetic action name the CI-failure triage webhook records its caps under
/// (per-key attempt counter + daily run ledger in [`crate::state`]), so the
/// event-driven mint throttles on the same accounting shape as the scheduled
/// actions instead of re-deriving its own. Renamed from the former
/// `webhook-fast-path`, then from `webhook-forgejo` (CLAUDE-361, when the
/// listener's route and name stopped being Forgejo-specific); a startup data
/// migration (migration `0013` plus the log-dir rename in [`crate::daemon`])
/// carries existing history across each rename.
pub const WEBHOOK_MINT_ACTION: &str = "webhook-triage";

/// The pause gate both webhook listeners take, once
/// signature / replay / rate-limit have passed. True means "accept the delivery
/// and drop it": return 2xx without parsing, minting, or emitting anything.
///
/// Three ways it returns true, all logged:
///
/// - the GLOBAL pause is set, which until now only the scheduler
///   and dispatcher honoured, so a webhook could still start work on a paused
///   daemon;
/// - the listener's own synthetic action (`action`) is paused;
/// - either read failed, in which case we fail safe and drop rather than start
///   work against a pause we could not read.
///
/// 2xx (not 4xx/5xx) because a retry cannot help: the delivery id is already in
/// the replay store, so a same-id retry would 409. Nothing recovers a dropped delivery.
pub(crate) async fn paused_accept_and_drop(
    pool: &DbPool,
    action: &str,
    source: &str,
    delivery: &str,
) -> bool {
    match state::global_pause_status(pool).await {
        Ok(Some((paused_at, _))) => {
            info!(
                source = %source,
                delivery,
                action,
                paused_at = %paused_at,
                "globally paused; delivery accepted and dropped"
            );
            return true;
        }
        Ok(None) => {}
        Err(e) => {
            error!(source = %source, delivery, action, error = %e, "global pause read failed; delivery accepted and dropped, and will not be recovered");
            return true;
        }
    }
    match state::action_pause_status(pool, action).await {
        Ok(Some(_)) => {
            info!(
                source = %source,
                delivery,
                action,
                "action is paused; delivery accepted and dropped"
            );
            true
        }
        Ok(None) => false,
        Err(e) => {
            error!(source = %source, delivery, action, error = %e, "pause-status read failed; delivery accepted and dropped, and will not be recovered");
            true
        }
    }
}

/// True when a timer- or queue-driven path of the
/// synthetic action `action` must not act: the daemon is globally paused,
/// `action` is paused, or a
/// pause flag could not be read (fail safe: a writer must never act against a
/// pause it could not check, unlike the cached `PauseGate` read).
pub(crate) async fn webhook_action_paused(pool: &DbPool, action: &str, what: &str) -> bool {
    match state::global_pause_status(pool).await {
        Ok(Some((paused_at, _))) => {
            info!(what, paused_at = %paused_at, "globally paused; skipping");
            return true;
        }
        Ok(None) => {}
        Err(e) => {
            error!(what, error = %e, "global pause read failed; skipping so a pause that could not be read is never acted against");
            return true;
        }
    }
    match state::action_pause_status(pool, action).await {
        Ok(Some(_)) => {
            info!(what, action, "action is paused; skipping");
            true
        }
        Ok(None) => false,
        Err(e) => {
            error!(what, action, error = %e, "action pause read failed; skipping so a pause that could not be read is never acted against");
            true
        }
    }
}

/// Right after the pause check, accept-and-drop a delivery while
/// the Claude login needs renewing. 2xx for the same reason as a pause: a
/// retry cannot help (the delivery id is already in the replay store), and
/// nothing recovers the delivery once signed in again. Unlike a pause, this ALSO records a
/// `run_events` row (`state::run_events_record`), because a login-required
/// drop is unusual enough that "on screen, not only in the log" earns it a
/// dedicated row rather than riding the pause's log-only treatment.
async fn login_required_accept_and_drop(st: &WebhookState, source: &str, delivery: &str) -> bool {
    let Some(gate) = &st.readiness else {
        return false;
    };
    let check = gate.check();
    if check.kind != Some(crate::readiness::NotReadyKind::LoginRequired) {
        return false;
    }
    let reason = check
        .reason()
        .unwrap_or_else(|| "Claude login required".to_string());
    warn!(source = %source, delivery, "{reason}; delivery accepted and dropped");
    let now = state::now_iso();
    let event = state::InputEvent {
        level: "warn".to_string(),
        context: Some(source.to_string()),
        message: format!("{reason}; delivery accepted and dropped"),
    };
    if let Err(e) = state::run_events_record(&st.pool, WEBHOOK_MINT_ACTION, &now, &[event]).await {
        warn!(source = %source, delivery, error = %e, "recording the login-required drop as a run_events row failed (continuing)");
    }
    true
}

/// `runs.triggered_by` value for the event-driven webhook, alongside
/// the existing `scheduler` / `trigger` / `cli` sources. Each
/// consumed verified failure becomes one run recorded under this source so the
/// dashboard / action-detail surfaces show webhook activity like any action.
pub const WEBHOOK_TRIGGERED_BY: &str = "webhook";

/// Hex-encoded HMAC-SHA256 of the raw body, keyed by the webhook secret.
pub const SIGNATURE_HEADER: &str = "X-Forgejo-Signature";
/// Per-delivery unique id, used for replay protection.
pub const DELIVERY_HEADER: &str = "X-Forgejo-Delivery";
/// The delivery's event type, mirroring Gitea's `X-Gitea-Event`
/// (Forgejo forked Gitea's webhook notifier and kept its header naming,
/// rebranded to `X-Forgejo-*` the same way it renamed the signature and
/// delivery headers above). Its value for an Action-run delivery is
/// `action_run_failure` / `action_run_success` / `action_run_recover`
/// (`services/webhook/notifier.go` `ActionRunNowDone`, the same notifier
/// [`VerifiedFailureEvent::status`]'s doc comment cites); for a pull-request
/// delivery it is [`PULL_REQUEST_EVENT_TYPE`]. Operators must confirm this
/// against their deployed Forgejo's actual delivery before relying on it (see
/// the operator-setup note in docs/webhooks.md): a missing or
/// unrecognized header still falls back to the pre-existing Action-run parse
/// path, so an Action-run-only deployment that never sends this header is
/// unaffected.
pub const EVENT_HEADER: &str = "X-Forgejo-Event";
/// [`EVENT_HEADER`] value for a pull-request delivery.
pub const PULL_REQUEST_EVENT_TYPE: &str = "pull_request";
/// [`EVENT_HEADER`] value prefix for every Action-run delivery kind.
const ACTION_RUN_EVENT_PREFIX: &str = "action_run_";
/// `pull_request.action` value for a PR closed (merged or not) delivery. Every
/// other action (`opened`, `edited`, `synchronized`, ...) is acked and dropped:
/// nothing to reconcile until the PR is terminal.
const PR_ACTION_CLOSED: &str = "closed";

/// GitHub's signature header (`Webhooks / Validating webhook deliveries`,
/// docs.github.com): hex-encoded HMAC-SHA256 of the raw body, prefixed
/// `sha256=`. GitHub still sends the deprecated `X-Hub-Signature` (SHA1) too;
/// this listener only ever checks the SHA256 one.
pub const GITHUB_SIGNATURE_HEADER: &str = "X-Hub-Signature-256";
/// GitHub's per-delivery unique id header, used for replay protection exactly
/// like [`DELIVERY_HEADER`].
pub const GITHUB_DELIVERY_HEADER: &str = "X-GitHub-Delivery";
/// GitHub's event-type header. Unlike Forgejo, GitHub folds no outcome into
/// this value (`workflow_run` covers requested/in_progress/completed alike;
/// the outcome is `workflow_run.conclusion` in the body); dispatch on outcome
/// happens after parsing, in [`github_workflow_run_to_action_payload`].
pub const GITHUB_EVENT_HEADER: &str = "X-GitHub-Event";
/// [`GITHUB_EVENT_HEADER`] value for a workflow-run delivery, GitHub's
/// analogue of Forgejo's `action_run_*` family.
pub const GITHUB_WORKFLOW_RUN_EVENT_TYPE: &str = "workflow_run";
/// Prefix GitHub puts on [`GITHUB_SIGNATURE_HEADER`]'s value ahead of the
/// hex-encoded HMAC, naming the algorithm.
const GITHUB_SIGNATURE_PREFIX: &str = "sha256=";
/// [`GITHUB_EVENT_HEADER`] value for an issue/PR comment delivery, the only
/// event the comment listener's GitHub arm acts on; every other event
/// (including `ping`) is acked and dropped.
pub const GITHUB_ISSUE_COMMENT_EVENT_TYPE: &str = "issue_comment";
/// [`GITHUB_EVENT_HEADER`] value GitHub sends when a webhook is created or
/// redelivered via "Redeliver" on the setup check; never carries a real
/// comment, so it gets its own ack log message distinct from other
/// unhandled events.
pub const GITHUB_PING_EVENT_TYPE: &str = "ping";

/// The forge sending a webhook delivery, named by the `:provider` URL path
/// segment (`/webhooks/:provider/triage/:source`,
/// `/webhooks/:provider/comment/:source`). This is deliberately a thin
/// data-carrying type (its own header names, its own signature check,
/// parse/serialize to the path segment / config value) rather than a trait:
/// every listener still shares one delivery pipeline (auth, replay,
/// rate-limit, pause, allow-list), only the header names, the signature
/// framing, and the event-type mapping vary by forge.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum WebhookProvider {
    #[default]
    Forgejo,
    Github,
}

impl WebhookProvider {
    /// Parse the `:provider` URL path segment. `None` for anything
    /// unrecognized, so the caller can 404 it the same way an unconfigured
    /// `:source` 404s.
    pub fn parse(segment: &str) -> Option<Self> {
        match segment {
            "forgejo" => Some(WebhookProvider::Forgejo),
            "github" => Some(WebhookProvider::Github),
            _ => None,
        }
    }

    /// The `:provider` URL path segment / config `provider` value this
    /// variant round-trips as.
    pub fn as_str(&self) -> &'static str {
        match self {
            WebhookProvider::Forgejo => "forgejo",
            WebhookProvider::Github => "github",
        }
    }

    /// Header carrying the hex-encoded HMAC-SHA256 of the raw body.
    pub fn signature_header(&self) -> &'static str {
        match self {
            WebhookProvider::Forgejo => SIGNATURE_HEADER,
            WebhookProvider::Github => GITHUB_SIGNATURE_HEADER,
        }
    }

    /// Header carrying the delivery's unique id, used for replay protection.
    pub fn delivery_header(&self) -> &'static str {
        match self {
            WebhookProvider::Forgejo => DELIVERY_HEADER,
            WebhookProvider::Github => GITHUB_DELIVERY_HEADER,
        }
    }

    /// Header carrying the delivery's event type.
    pub fn event_header(&self) -> &'static str {
        match self {
            WebhookProvider::Forgejo => EVENT_HEADER,
            WebhookProvider::Github => GITHUB_EVENT_HEADER,
        }
    }

    /// Constant-time signature verification of `body` against the
    /// `signature` header value under `secret`, in this provider's own
    /// scheme. Forgejo's is bare hex HMAC-SHA256 ([`verify_signature`]);
    /// GitHub's is the same HMAC-SHA256-hex scheme under a `sha256=` prefix
    /// naming the algorithm, so a missing/wrong prefix (including GitHub's
    /// own deprecated SHA1 `X-Hub-Signature` value, which this never reads)
    /// fails closed rather than falling through to a algorithm mismatch.
    pub fn verify_signature(&self, secret: &[u8], body: &[u8], signature: &str) -> bool {
        match self {
            WebhookProvider::Forgejo => verify_signature(secret, body, signature),
            WebhookProvider::Github => match signature.strip_prefix(GITHUB_SIGNATURE_PREFIX) {
                Some(hex_sig) => verify_signature(secret, body, hex_sig),
                None => false,
            },
        }
    }
}

/// A GitHub webhook created with GitHub's default Content type delivers its
/// body as `payload=<url-encoded JSON>` instead of raw JSON. Returns the JSON
/// bytes to parse: the decoded `payload` form field when `provider` is
/// [`WebhookProvider::Github`] and `content_type`'s media type (parameters
/// ignored, case-insensitive) is `application/x-www-form-urlencoded`, the
/// body unchanged otherwise (Forgejo always sends JSON; a GitHub JSON
/// delivery needs no decoding). Call only after the signature has been
/// verified over the raw body: that check must never see the decoded form.
pub(crate) fn github_json_payload(
    provider: WebhookProvider,
    content_type: Option<&str>,
    body: &Bytes,
) -> Result<Bytes, String> {
    if provider != WebhookProvider::Github {
        return Ok(body.clone());
    }
    let is_form_encoded = content_type
        .and_then(|ct| ct.split(';').next())
        .is_some_and(|media| {
            media
                .trim()
                .eq_ignore_ascii_case("application/x-www-form-urlencoded")
        });
    if !is_form_encoded {
        return Ok(body.clone());
    }
    #[derive(Deserialize)]
    struct FormPayload {
        payload: String,
    }
    let form: FormPayload =
        serde_urlencoded::from_bytes(body).map_err(|e| format!("form decode failed: {e}"))?;
    Ok(Bytes::from(form.payload.into_bytes()))
}

impl std::fmt::Display for WebhookProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// The subset of Forgejo's `pull_request` webhook payload the
/// merge-close path consumes. Extra fields are ignored (no
/// `deny_unknown_fields`), matching [`ActionPayload`]'s tolerance of a Forgejo
/// upgrade that adds keys.
#[derive(Debug, Clone, Deserialize)]
pub struct PullRequestPayload {
    /// `opened` | `edited` | `closed` | `synchronized` | ... . Only
    /// [`PR_ACTION_CLOSED`] is acted on.
    pub action: String,
    pub number: i64,
    pub pull_request: PullRequestInfo,
    pub repository: Repository,
}

#[derive(Debug, Clone, Deserialize)]
pub struct PullRequestInfo {
    /// True when the PR was merged rather than closed unmerged. Only
    /// meaningful when `action == "closed"`.
    #[serde(default)]
    pub merged: bool,
    pub head: PullRequestHead,
    pub html_url: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct PullRequestHead {
    #[serde(rename = "ref")]
    pub git_ref: String,
    pub sha: String,
}

/// The typed event emitted downstream for a verified, allow-listed, terminal
/// (`closed`) pull-request delivery.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VerifiedPullRequestEvent {
    pub repo: String,
    pub number: i64,
    pub merged: bool,
    pub head_sha: String,
    pub head_ref: String,
    pub html_url: String,
    /// The webhook source this event arrived through, so the
    /// consumer can resolve the source's `log_token_ref`.
    pub source: String,
}

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
    /// The run's per-repo index, which is the number `fj actions logs` takes and
    /// the one the `/actions/runs/<N>` web URL uses. The run's global
    /// id is no longer read - `fj` resolves the run itself - so it is not parsed.
    pub index_in_repo: i64,
    pub commit_sha: String,
    pub workflow_id: String,
    pub status: String,
    pub trigger_user: ActionUser,
    pub html_url: String,
    pub repository: Repository,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(try_from = "ActionUserRaw")]
pub struct ActionUser {
    /// Forgejo user objects carry both `login` and `username` (often with the
    /// same value). Both keys are accepted independently and reconciled in
    /// `TryFrom<ActionUserRaw>`, preferring `login`; at least one must be present.
    pub login: String,
}

#[derive(Debug, Clone, Deserialize)]
struct ActionUserRaw {
    login: Option<String>,
    username: Option<String>,
}

impl TryFrom<ActionUserRaw> for ActionUser {
    type Error = String;
    fn try_from(raw: ActionUserRaw) -> Result<Self, Self::Error> {
        raw.login
            .or(raw.username)
            .map(|login| ActionUser { login })
            .ok_or_else(|| "user object has neither `login` nor `username`".to_string())
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct Repository {
    /// `owner/repo`, matched against the configured allow-list.
    pub full_name: String,
    /// Visibility from the payload (`modules/structs/repo.go:57`).
    /// No longer gates the log fetch, which is one `fj actions logs`
    /// path for both visibilities; retained as context carried onto the mint.
    /// Serde default keeps older/foreign payloads on the public reading.
    #[serde(default)]
    pub private: bool,
}

/// Subset of GitHub's `workflow_run` webhook payload we consume
/// (`Webhook events and payloads / workflow_run`, docs.github.com). Extra
/// fields are ignored, matching [`ActionPayload`]'s tolerance of upstream
/// additions.
#[derive(Debug, Clone, Deserialize)]
pub struct GithubWorkflowRunPayload {
    /// `requested` | `in_progress` | `completed`. Only `completed` carries a
    /// terminal `conclusion`; the other two are acked and dropped.
    pub action: String,
    pub workflow_run: GithubWorkflowRun,
    pub repository: Repository,
}

#[derive(Debug, Clone, Deserialize)]
pub struct GithubWorkflowRun {
    /// The run's per-repo sequential number - GitHub's analogue of
    /// Forgejo's [`ActionRun::index_in_repo`]. Deliberately not the payload's
    /// `id` (a global, cross-repo identifier that plays no role anywhere
    /// downstream, which keys everything by the per-repo index).
    pub run_number: i64,
    pub head_sha: String,
    /// The workflow's file path, e.g. `.github/workflows/ci.yml`; reduced to
    /// its filename in [`github_workflow_run_to_action_payload`] to match
    /// Forgejo's `workflow_id` convention (a bare filename).
    pub path: String,
    pub html_url: String,
    /// Set only once `action == "completed"`.
    #[serde(default)]
    pub conclusion: Option<String>,
    #[serde(default)]
    pub actor: Option<GithubActor>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct GithubActor {
    pub login: String,
}

/// GitHub `workflow_run.conclusion` value meaning the run finished with
/// nothing to fix. Every other conclusion (`failure`, `cancelled`,
/// `timed_out`, `action_required`, `stale`, `startup_failure`, ...) is
/// mapped onto [`RUN_STATUS_FAILURE`], mirroring Forgejo's `action_run_failure`
/// notifier firing for every non-success done status (see
/// [`VerifiedFailureEvent::status`]'s doc comment).
const GITHUB_CONCLUSION_SUCCESS: &str = "success";

/// Map a completed GitHub `workflow_run` delivery onto the same
/// [`ActionPayload`] shape [`verified_failure_event`] and
/// [`handle_action_success`] already consume, so neither needs a
/// GitHub-specific type. `None` for a non-`completed` action, or a
/// `completed` run with no `conclusion` (should not happen per GitHub's own
/// docs, but the field is nullable in its schema, so this is defensive
/// rather than assumed).
fn github_workflow_run_to_action_payload(
    payload: &GithubWorkflowRunPayload,
) -> Option<ActionPayload> {
    if payload.action != "completed" {
        return None;
    }
    let conclusion = payload.workflow_run.conclusion.clone()?;
    let action = if conclusion == GITHUB_CONCLUSION_SUCCESS {
        "success".to_string()
    } else {
        "failure".to_string()
    };
    let workflow_id = payload
        .workflow_run
        .path
        .rsplit('/')
        .next()
        .unwrap_or(&payload.workflow_run.path)
        .to_string();
    let trigger_login = payload
        .workflow_run
        .actor
        .as_ref()
        .map(|a| a.login.clone())
        .unwrap_or_default();
    Some(ActionPayload {
        action,
        run: ActionRun {
            index_in_repo: payload.workflow_run.run_number,
            commit_sha: payload.workflow_run.head_sha.clone(),
            workflow_id,
            status: conclusion,
            trigger_user: ActionUser {
                login: trigger_login,
            },
            html_url: payload.workflow_run.html_url.clone(),
            repository: payload.repository.clone(),
        },
    })
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
    /// The run's own status from the payload. Forgejo's notifier
    /// (`services/webhook/notifier.go` `ActionRunNowDone`, v14.0.2) fires
    /// `action_run_failure` with `action: "failure"` for EVERY non-success done
    /// status - `failure`, `cancelled`, and `skipped` (`DoneStatuses()` in
    /// `models/actions/status.go`) - so this field, not the `action`, says
    /// whether anything actually failed. The consumer processes only
    /// [`RUN_STATUS_FAILURE`] and acks-and-skips the rest.
    pub status: String,
    /// `repository.private` from the payload; gates the log fetch
    /// (see [`Repository::private`]).
    pub private: bool,
    /// The webhook source (config block name) this event arrived
    /// through, so the consumer can resolve the source's `log_token_ref`.
    pub source: String,
}

/// The typed event emitted downstream for a verified, allow-listed
/// `success` or `recover` Action-run delivery - an agent PR's checks may have
/// just gone green. Deliberately thin: which PR (if any) this run belongs to,
/// whether its action has automerge enabled, and its CI/mergeable state are
/// all resolved downstream in `process_action_success_event`, so a slow forge
/// call never blocks the HTTP response.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VerifiedActionSuccessEvent {
    pub repo: String,
    pub commit_sha: String,
    pub html_url: String,
    /// The webhook source this event arrived through.
    pub source: String,
}

/// The one `run.status` the pipeline acts on. Forgejo's status enum
/// (`models/actions/status.go`) is `unknown, success, failure, cancelled,
/// skipped, waiting, running, blocked`; of the done statuses that reach the
/// `action_run_failure` event (`failure`, `cancelled`, `skipped`), only a real
/// `failure` has something to fix. A `cancelled` run was stopped deliberately
/// and a `skipped` run never ran (and has no log, so fetching would burn the
/// full 60s retry budget); both finish in ~0s with no fetch, triage, or mint.
pub const RUN_STATUS_FAILURE: &str = "failure";

/// State injected into the webhook handler. Cheap to clone (`DbPool` and
/// `Sender` are `Arc`-shaped; the config and the rate-limiter map are
/// `Arc`-wrapped). One [`RateLimiter`] per source (built once at
/// startup, keyed by source name) and a single shared [`SeenDeliveries`] replay
/// store across all sources.
#[derive(Clone)]
pub struct WebhookState {
    pool: DbPool,
    /// Live webhook config, shared with [`crate::settings::LiveSettings`]
    /// so a Settings save (new/edited source, allow-list, rate limit, secret refs)
    /// is routable on the next request without a restart.
    config: Arc<RwLock<WebhookConfig>>,
    events: mpsc::Sender<VerifiedFailureEvent>,
    seen: Arc<Mutex<SeenDeliveries>>,
    /// Live per-source limiter map, rebuilt by `LiveSettings` on save.
    rate: Arc<RwLock<HashMap<String, Mutex<RateLimiter>>>>,
    /// `None` outside the daemon (tests, standalone callers).
    /// When set, a login-required delivery is accepted and dropped exactly
    /// like a pause, and the drop is recorded as a `run_events` row.
    readiness: Option<crate::readiness::ReadinessGate>,
    /// `None` outside the daemon and in every pre-existing test
    /// (a separate `with_pr_events` builder, not a constructor parameter, so
    /// `WebhookState::new`'s call sites - including
    /// `comment_webhooks`'s cross-listener replay-store test - stay
    /// source-compatible). A `pull_request` delivery with no sender configured
    /// is acked and dropped with a log, exactly like a delivery arriving while
    /// the daemon has no usable provider backend.
    pr_events: Option<mpsc::Sender<VerifiedPullRequestEvent>>,
    /// Same source-compatibility shape as `pr_events`, for a
    /// verified `success`/`recover` Action-run delivery. `None` (no daemon
    /// wiring, every pre-existing test) acks and drops with a log.
    success_events: Option<mpsc::Sender<VerifiedActionSuccessEvent>>,
}

/// Build one fixed-window rate limiter per source, so exhausting
/// one source's budget never 429s another. Shared by [`WebhookState`] startup and
/// the live-apply rebuild in [`crate::settings::LiveSettings`], keeping the
/// limiter map and the routed source map in lock-step.
pub fn build_rate_map(config: &WebhookConfig) -> HashMap<String, Mutex<RateLimiter>> {
    rate_map_from(
        config
            .sources
            .iter()
            .map(|(n, s)| (n, s.rate_limit_per_min)),
    )
}

/// The source-name-to-limiter build itself, over any `(name, limit)`
/// pairs. The comment listener ([`super::comment_webhooks`]) has its own source
/// map with the same per-source bucket semantics, so it shares this builder (and
/// the [`RateLimiter`] type) rather than forking a parallel implementation.
pub fn rate_map_from<'a>(
    sources: impl Iterator<Item = (&'a String, u32)>,
) -> HashMap<String, Mutex<RateLimiter>> {
    sources
        .map(|(name, limit)| {
            (
                name.clone(),
                Mutex::new(RateLimiter::new(limit, Duration::from_secs(60))),
            )
        })
        .collect()
}

impl WebhookState {
    /// Standalone constructor (tests, single-source callers): owns fresh shared
    /// handles built from `config`.
    pub fn new(
        pool: DbPool,
        config: WebhookConfig,
        events: mpsc::Sender<VerifiedFailureEvent>,
    ) -> Self {
        let seen = SeenDeliveries::new(Duration::from_secs(config.replay_window_secs));
        let rate = build_rate_map(&config);
        Self {
            pool,
            config: Arc::new(RwLock::new(config)),
            events,
            seen: Arc::new(Mutex::new(seen)),
            rate: Arc::new(RwLock::new(rate)),
            readiness: None,
            pr_events: None,
            success_events: None,
        }
    }

    /// Build from handles shared with [`crate::settings::LiveSettings`]
    /// so a save swaps the routed config + limiters live. The replay store is
    /// built once here from `replay_window_secs` (restart-only by design).
    pub fn from_shared(
        pool: DbPool,
        config: Arc<RwLock<WebhookConfig>>,
        rate: Arc<RwLock<HashMap<String, Mutex<RateLimiter>>>>,
        replay_window_secs: u64,
        events: mpsc::Sender<VerifiedFailureEvent>,
    ) -> Self {
        let seen = SeenDeliveries::new(Duration::from_secs(replay_window_secs));
        Self {
            pool,
            config,
            events,
            seen: Arc::new(Mutex::new(seen)),
            rate,
            readiness: None,
            pr_events: None,
            success_events: None,
        }
    }

    /// Attach the daemon's readiness gate, so a login-required
    /// delivery is accepted and dropped like a pause. A builder so every
    /// existing constructor call site stays source-compatible.
    pub fn with_readiness(mut self, gate: crate::readiness::ReadinessGate) -> Self {
        self.readiness = Some(gate);
        self
    }

    /// Attach the channel a verified `pull_request` delivery is
    /// dispatched onto. A builder for the same source-compatibility reason as
    /// [`Self::with_readiness`].
    pub fn with_pr_events(mut self, events: mpsc::Sender<VerifiedPullRequestEvent>) -> Self {
        self.pr_events = Some(events);
        self
    }

    /// Attach the channel a verified `success`/`recover`
    /// Action-run delivery is dispatched onto. A builder for the same
    /// source-compatibility reason as [`Self::with_readiness`].
    pub fn with_success_events(mut self, events: mpsc::Sender<VerifiedActionSuccessEvent>) -> Self {
        self.success_events = Some(events);
        self
    }

    /// The SHARED replay store handle. The comment listener records
    /// its deliveries in this same instance, so one `X-Forgejo-Delivery` id
    /// fanned out to both endpoints is still caught as a replay.
    pub fn seen_handle(&self) -> Arc<Mutex<SeenDeliveries>> {
        self.seen.clone()
    }
}

/// Router carrying the per-source webhook route, merged into the daemon's TCP
/// app. `{provider}` names the sending forge ([`WebhookProvider`]); `{source}`
/// is an axum path param resolved against the configured sources map.
pub fn router(state: WebhookState) -> Router {
    Router::new()
        .route("/webhooks/:provider/triage/:source", post(forgejo_webhook))
        .with_state(state)
}

/// Everything [`consume_events`] needs to triage a verified failure and mint
/// (or decline to mint) a YouTrack issue from it. Built once at daemon start
/// from the resolved provider backend (`IssueTracker` + `Forge`), the
/// on-disk mint config, and the shared `claude` invoker behind a
/// [`FailureClassifier`]. When the daemon cannot build one (no usable `yt`/`fj`
/// backend) it passes `None` and the consumer falls back to log-only behaviour.
/// `Clone` so the daemon can hand the same context to both
/// [`consume_events`] and [`consume_pr_events`] - every field is already
/// `Arc`-shaped or cheap to clone.
#[derive(Clone)]
pub struct MintContext {
    /// State DB, for the reused caps ledger.
    pub pool: DbPool,
    /// The LLM pass for the residue the deterministic rules cannot label.
    pub classifier: Arc<dyn FailureClassifier>,
    /// Provider backend: the `IssueTracker` the mint writes through and the
    /// `Forge` the `has_open_pr` signal is read from.
    pub backend: Arc<dyn Backend>,
    /// Repo-to-project routing, loop-safety bot logins, bucket-2 assignee, etc.
    /// A live handle shared with [`crate::settings::LiveSettings`],
    /// read per event so a Settings save is honored by the next mint without a
    /// restart.
    pub mint_cfg: Arc<RwLock<MintConfig>>,
    /// Caps applied to the event-driven mint (per-key attempts + daily runs).
    pub defaults: RunnerDefaults,
    /// TTL-cached inverse VCS routing map (repo -> project), built
    /// from YouTrack project VCS integrations and shared across mints.
    pub route_cache: Arc<VcsRouteCache>,
    /// Resolved on-disk locations, so a PR comment's write-scoped
    /// backend can materialize its temp single-host `keys.json` under the
    /// daemon's own runtime dir (the same mechanism
    /// `comment_webhooks::build_env` uses).
    pub paths: Paths,
    /// The [`CommandRunner`] the PR-comment backend's `fj` child
    /// runs through. Real [`ProcessRunner`] in production; injectable so a test
    /// can exercise a PR-comment routing decision against a canned `fj`
    /// response with no real subprocess or network call.
    pub pr_comment_runner: Arc<dyn CommandRunner>,
    /// Dispatch handle for the automerge conflict auto-resolve (CLAUDE-380):
    /// the comment listener's command channel plus its live config, used to
    /// resolve which source covers `repo` and send a synthetic
    /// `/claude`-equivalent command on a conflict `update_pr_branch` failure.
    /// `None` outside the daemon (every test, and a daemon run with the
    /// comment listener unconfigured) keeps today's "needs a human" comment
    /// behavior exactly.
    pub comment_dispatch: Option<CommentDispatch>,
}

/// See [`MintContext::comment_dispatch`].
#[derive(Clone)]
pub struct CommentDispatch {
    pub tx: mpsc::Sender<crate::web::comment_webhooks::VerifiedCommentCommand>,
    pub cfg: Arc<RwLock<crate::config::CommentWebhookConfig>>,
}

/// Drain emitted events. For each verified failure this fetches the run's logs
/// through `fj actions logs`, redacts secrets, and writes a redacted artifact;
/// then, when a [`MintContext`] is configured, triages
/// the failure and mints / declines a YouTrack issue.
/// The fetch is best-effort: a not-ready / pruned log is a transient miss, not a
/// fault. A miss does not strand the consumer - it feeds triage as a bucket-3
/// signal (the rules pass routes an unavailable log to transient).
pub async fn consume_events(
    mut rx: mpsc::Receiver<VerifiedFailureEvent>,
    pool: DbPool,
    logs: Arc<dyn crate::forge_log::ActionLogClient>,
    out_dir: PathBuf,
    mint: Option<MintContext>,
    webhook_cfg: Arc<RwLock<WebhookConfig>>,
    keygate: crate::concurrency::KeyGate,
) {
    // Bound the number of events processed concurrently, so a
    // burst of failures does not start one 600s analysis session per event at
    // once (`config.webhook.analysis_concurrency`, default 2). The snapshot is
    // taken once at start: resizing the pool live is not required by this
    // issue and the DB-backed setting is otherwise honored per event as
    // before.
    let concurrency = webhook_cfg.read().await.analysis_concurrency.max(1);
    let semaphore = Arc::new(tokio::sync::Semaphore::new(concurrency));
    // Built once so every spawned event shares one classifier/backend/route
    // cache, exactly as the old inline loop did.
    let mint = mint.map(Arc::new);
    // Joined once the channel closes, so a caller awaiting
    // `consume_events` (every existing test, and the daemon's own shutdown)
    // observes every spawned event's completed side effects, not just that
    // they were scheduled.
    let mut tasks = Vec::new();
    while let Some(ev) = rx.recv().await {
        // Snapshot the live webhook config for this event so a
        // Settings save (e.g. a source's `log_token_ref`) is picked up by the
        // next event without a restart.
        let webhook_cfg_snapshot = webhook_cfg.read().await.clone();
        // Acquire a permit before spawning, so the loop itself
        // (not the spawned task) is what bounds how many events are ever
        // in flight; a saturated pool simply leaves later events queued in
        // the channel until one finishes.
        let permit = semaphore
            .clone()
            .acquire_owned()
            .await
            .expect("semaphore is never closed");
        let pool = pool.clone();
        let logs = logs.clone();
        let out_dir = out_dir.clone();
        let mint = mint.clone();
        let keygate = keygate.clone();
        tasks.push(tokio::spawn(async move {
            process_event(ev, pool, logs, out_dir, mint, webhook_cfg_snapshot, keygate).await;
            drop(permit);
        }));
        // Bound the join set's own growth: a task that already finished is
        // dropped from the list immediately rather than accumulating for the
        // whole (potentially unbounded) lifetime of the consumer.
        tasks.retain(|t| !t.is_finished());
    }
    for t in tasks {
        if let Err(e) = t.await {
            warn!(error = %e, "a webhook event task panicked");
        }
    }
}

/// One verified failure's whole pipeline: fetch the run's logs, redact, and
/// (when a [`MintContext`] is configured) triage and mint. Spawned as its own task by
/// [`consume_events`]'s worker pool so a slow analysis for one event never
/// delays another's.
async fn process_event(
    ev: VerifiedFailureEvent,
    pool: DbPool,
    logs: Arc<dyn crate::forge_log::ActionLogClient>,
    out_dir: PathBuf,
    mint: Option<Arc<MintContext>>,
    webhook_cfg: WebhookConfig,
    keygate: crate::concurrency::KeyGate,
) {
    // An event accepted before a pause was set must not act after it.
    if webhook_action_paused(&pool, WEBHOOK_MINT_ACTION, "action-run failure event").await {
        return;
    }
    let cfg = crate::forge_log::LogFetchConfig::default();
    let mint = mint.as_deref();
    {
        info!(
            repo = %ev.repo,
            run = ev.index_in_repo,
            commit = %ev.commit_sha,
            workflow = %ev.workflow_id,
            trigger_user = %ev.trigger_user,
            html_url = %ev.html_url,
            "verified action_run_failure received"
        );

        // Open a run row for every consumed event (user decision:
        // full observability including skips) so the web UI surfaces webhook
        // activity. `run` is `None` only if `run_begin` itself failed; the
        // pipeline still runs, it just goes unrecorded. `run_id` is split out
        // as its own `Copy` value for the bookkeeping calls below; `run` itself
        // (carrying the `RunGuard`) is threaded to whichever `finish_webhook_run`
        // call below actually closes the row, so a panic between here and there
        // still closes it as `exit::UNEXPECTED` on drop (CLAUDE-402).
        let job_key = format!("{}#{}", ev.repo, ev.index_in_repo);
        let run = begin_webhook_run(&pool, WEBHOOK_MINT_ACTION, &job_key, &ev.html_url).await;
        let run_id = run.as_ref().map(|(id, _)| *id);

        // Forgejo fires `action_run_failure` for every non-success
        // done status (`failure`, `cancelled`, `skipped`; bare `else` in
        // `webhookNotifier.ActionRunNowDone`), so the run status, not the event
        // type, says whether anything failed. Only a real failure gets the
        // pipeline: a cancelled run was stopped deliberately, and a server-side
        // skipped run (workflow condition false, no runner task) has no log at
        // all - fetching would burn the full 60s retry budget on `resource
        // does not exist`. Everything else finishes in ~0s, no fetch, no
        // triage, no mint.
        if ev.status != RUN_STATUS_FAILURE {
            info!(
                repo = %ev.repo,
                run = ev.index_in_repo,
                status = %ev.status,
                "run status is not 'failure'; log fetch and triage skipped"
            );
            let summary = format!(
                "run status is '{}', not 'failure'; log fetch and triage skipped; no issue",
                ev.status
            );
            finish_webhook_run(&pool, run, &job_key, &summary, true, None, None).await;
            return;
        }

        // ONE fetch path for public and private repos. `fj actions
        // logs` takes the per-repo run number the payload already carries, and
        // the source's `log_token_ref` (when configured) authorizes a private
        // repo's read; a public repo reads without one. A miss still surfaces:
        // the no-log triage rule routes it to a human (bucket 2) rather than
        // silently dropping it as transient.
        let token = source_log_token(&pool, &webhook_cfg, &ev.source).await;
        // Kept for the analysis classifier's own workflow-YAML fetch
        // (it authorizes the log fetch below, then travels via `TriageInput`).
        let log_token = token.clone();
        let Some(target) =
            crate::forge_log::LogTarget::for_run(&ev.html_url, &ev.repo, ev.index_in_repo, token)
        else {
            warn!(repo = %ev.repo, html_url = %ev.html_url, "could not derive forge host from html_url; skipping log fetch");
            finish_webhook_run(
                &pool,
                run,
                &job_key,
                "could not derive forge host from html_url; log fetch skipped",
                false,
                None,
                None,
            )
            .await;
            return;
        };
        if let Some(id) = run_id {
            if let Err(e) = state::run_stage_set(&pool, id, Some(STAGE_FETCHING_LOGS)).await {
                warn!(error = %e, "setting webhook run stage failed");
            }
        }

        // The fetch is blocking (a child process + backoff sleeps); run it off
        // the async runtime. No service-injected secrets reach the CI run, so
        // `known_secrets` is empty here and pattern redaction does the work.
        let outcome = {
            let logs = logs.clone();
            let fetch_cfg = cfg.clone();
            let out_dir = out_dir.clone();
            tokio::task::spawn_blocking(move || {
                crate::forge_log::fetch_run_log(
                    logs.as_ref(),
                    &fetch_cfg,
                    &target,
                    &out_dir,
                    &[],
                    std::thread::sleep,
                )
            })
            .await
        };

        // Normalise the fetch result into the triage inputs. A `TransientMiss`,
        // an auth denial, a fetch error, and a panicked task all collapse to
        // "no log" with the reason preserved; the no-log triage rule
        // routes those to a human rather than dropping them.
        let (log_text, log_available, log_artifact, log_unavailable_reason, jobs) = match outcome {
            Ok(Ok(LogOutcome::Fetched(log))) => {
                if let Some(reason) = crate::forge_log::logs_permanently_missing(&log.jobs) {
                    warn!(
                        repo = %ev.repo,
                        run = ev.index_in_repo,
                        %reason,
                        "run log permanently missing (FJ-79)"
                    );
                    (
                        String::new(),
                        false,
                        Some(log.artifact),
                        Some(reason),
                        log.jobs,
                    )
                } else {
                    info!(
                        repo = %ev.repo,
                        run = ev.index_in_repo,
                        bytes = log.text.len(),
                        jobs = log.jobs.len(),
                        artifact = %log.artifact.display(),
                        "fetched and redacted run log"
                    );
                    (log.text, true, Some(log.artifact), None, log.jobs)
                }
            }
            Ok(Ok(LogOutcome::TransientMiss { reason })) => {
                warn!(repo = %ev.repo, run = ev.index_in_repo, %reason, "run log not available (transient miss)");
                (
                    String::new(),
                    false,
                    None,
                    Some(format!("log not available after retries: {reason}")),
                    Vec::new(),
                )
            }
            Ok(Ok(LogOutcome::AuthDenied { reason })) => {
                warn!(repo = %ev.repo, run = ev.index_in_repo, %reason, "log token rejected");
                if let Some(id) = run_id {
                    if let Err(e) =
                        state::run_stage_set(&pool, id, Some(STAGE_LOG_UNAUTHORIZED)).await
                    {
                        warn!(error = %e, "setting webhook run stage failed");
                    }
                }
                (
                    String::new(),
                    false,
                    None,
                    Some(format!("log token rejected ({reason})")),
                    Vec::new(),
                )
            }
            Ok(Ok(LogOutcome::Failed { reason })) => {
                warn!(repo = %ev.repo, run = ev.index_in_repo, %reason, "log fetch failed");
                (
                    String::new(),
                    false,
                    None,
                    Some(format!("log fetch failed: {reason}")),
                    Vec::new(),
                )
            }
            Ok(Err(e)) => {
                warn!(repo = %ev.repo, run = ev.index_in_repo, error = %e, "log fetch failed");
                (
                    String::new(),
                    false,
                    None,
                    Some(format!("log fetch failed: {e}")),
                    Vec::new(),
                )
            }
            Err(e) => {
                warn!(repo = %ev.repo, run = ev.index_in_repo, error = %e, "log fetch task panicked");
                (
                    String::new(),
                    false,
                    None,
                    Some(format!("log fetch task panicked: {e}")),
                    Vec::new(),
                )
            }
        };

        // The guard above reads the RUN-level status, which is
        // `failure` even when every job was merely cancelled (a newer push
        // superseded the run). Nothing failed, so triage would classify a log
        // with no failure in it and mint noise. Skip only the all-`{cancelled,
        // skipped}` case; any other status set still mints and logs what was
        // observed, and an unresolved job list mints as before so a log-token
        // 401 fails open. The jobs ride along with the log since the two
        // fetches were collapsed into one `fj actions logs` call,
        // so the saving here is the triage `claude` call and the issue.
        let jobs_verdict =
            crate::forge_log::JobsVerdict::resolve(log_unavailable_reason.as_deref(), &jobs);
        if let crate::forge_log::JobsVerdict::NoFailure { .. } = &jobs_verdict {
            let statuses = jobs_verdict.statuses_display();
            if jobs_verdict.stopped() {
                info!(
                    repo = %ev.repo,
                    run = ev.index_in_repo,
                    job_statuses = %statuses,
                    "no job failed (run stopped, not broken); triage and mint skipped"
                );
                let summary = format!(
                    "no job failed; job statuses [{statuses}]; run stopped, not broken; triage and mint skipped; no issue"
                );
                finish_webhook_run(
                    &pool,
                    run,
                    &job_key,
                    &summary,
                    true,
                    log_artifact.as_deref(),
                    None,
                )
                .await;
                return;
            }
            info!(
                repo = %ev.repo,
                run = ev.index_in_repo,
                job_statuses = %statuses,
                "run failed with no failing job; not a stopped run, continuing to triage"
            );
        }

        let Some(ctx) = mint else {
            // No mint configured (no usable backend): log-only, as before. The
            // run is still recorded so the operator sees the event landed.
            let summary = if log_available {
                "log fetched and redacted; no provider backend configured (log-only)"
            } else {
                "log unavailable; no provider backend configured (log-only)"
            };
            finish_webhook_run(
                &pool,
                run,
                &job_key,
                summary,
                true,
                log_artifact.as_deref(),
                None,
            )
            .await;
            return;
        };
        let artifact = log_artifact.clone();
        // One ledger for this delivery, so the triage
        // classification's cost lands on this run's outcome row and daily
        // ledger instead of only `logs/usage.jsonl` with no run id.
        let spend = crate::usage::RunSpend::new(
            WEBHOOK_MINT_ACTION,
            crate::usage::RunBudget::new(
                ctx.defaults.daily_budget_usd,
                ctx.defaults.budget_warn_fraction,
            ),
            Some(ctx.paths.usage_log()),
        );
        // The failing job's name comes from the jobs `fj` already
        // returned with the log, so the root-cause dedupe key is job-scoped with
        // no extra request. A logless failure degrades to a job-agnostic key,
        // which still collapses the storm. The branch and the failure signature
        // are derived from the log inside `decide_mint`.
        let job_name = crate::forge_log::resolve_failing_job(&jobs)
            .map(|j| j.name.clone())
            .unwrap_or_default();
        let result = triage_and_mint(
            ctx,
            run_id,
            &ev,
            log_text,
            log_available,
            log_artifact,
            log_unavailable_reason,
            webhook_cfg.auto_fix,
            job_name,
            log_token,
            &keygate,
            &webhook_cfg,
            &spend,
        )
        .await;
        if spend.cost_usd() > 0.0 {
            if let Err(e) =
                state::daily_cost_add(&pool, WEBHOOK_MINT_ACTION, spend.cost_usd()).await
            {
                warn!(error = %e, "recording the webhook mint run cost failed");
            }
        }
        finish_webhook_run(
            &pool,
            run,
            &job_key,
            &result.text,
            result.ok,
            artifact.as_deref(),
            Some(&spend),
        )
        .await;
    }
}

/// Live stage labels for the webhook pipeline, surfaced on
/// the run-detail page (Stage row + the running job's LIVE column) via
/// `runs.stage`. `run_finish` clears the column, so no label outlives its run.
pub const STAGE_FETCHING_LOGS: &str = "fetching action logs";
/// Shown when the log token was rejected, so the run-detail page
/// names the actionable cause; the lasting record is the triage analysis and
/// journal line, since `run_finish` clears the stage.
pub const STAGE_LOG_UNAUTHORIZED: &str = "log fetch: unauthorized";
pub const STAGE_TRIAGING: &str = "triaging";
pub const STAGE_CLASSIFYING: &str = "classifying with claude";
pub const STAGE_MINTING: &str = "minting issue";

/// Decrypt the named log token from the secrets store (same age-key
/// mechanism as the HMAC refs).
async fn resolve_log_token(pool: &DbPool, name: &str) -> crate::error::Result<String> {
    let key = SecretsKey::load()?;
    // Trim the decrypted token. A stored trailing space or CR
    // survives as a valid header character and yields a spurious HTTP 401.
    let token = secrets::resolve_secret(pool, &key, name).await?;
    Ok(token.trim().to_string())
}

/// The source's `log_token_ref` decrypted, or `None` when the
/// source has no token configured or it cannot be resolved. Authorizes the
/// `fj actions logs` read; a public repo's run reads without one.
async fn source_log_token(
    pool: &DbPool,
    webhook_cfg: &WebhookConfig,
    source: &str,
) -> Option<String> {
    let token_ref = webhook_cfg
        .sources
        .get(source)
        .and_then(|s| s.log_token_ref.clone())?;
    match resolve_log_token(pool, &token_ref).await {
        Ok(t) => Some(t),
        Err(e) => {
            warn!(source = %source, token_ref = %token_ref, error = %e, "resolving log token for job resolution failed");
            None
        }
    }
}

/// Open the `runs` row for one webhook event and stamp its
/// single planned job key. Also registers the job in-flight (so the
/// run-detail badge reads `running`, not `queued`, for the whole pipeline) and
/// sets the initial stage. Stores the event's `html_url` as the
/// job's outbound link so the run-detail tables link the key straight to the
/// Forgejo Actions run. Returns the run id together with its [`state::RunGuard`],
/// or `None` when `run_begin` failed (the pipeline still runs, just
/// unrecorded). Failures are logged, not propagated: a DB hiccup must never
/// strand the event consumer. The guard must be finished (or simply dropped)
/// by the caller; a panic or early return between here and that point still
/// closes the row as [`crate::exit::UNEXPECTED`] instead of leaving it open
/// until the next daemon start (CLAUDE-402).
///
/// `action` is a parameter so the comment listener records its runs
/// through this same accounting rather than a forked copy.
pub(crate) async fn begin_webhook_run(
    pool: &DbPool,
    action: &str,
    job_key: &str,
    html_url: &str,
) -> Option<(i64, state::RunGuard)> {
    match state::run_begin(pool, action, WEBHOOK_TRIGGERED_BY).await {
        Ok(id) => {
            let guard = state::RunGuard::new(pool, id);
            if let Err(e) =
                state::run_set_job_keys(pool, id, std::slice::from_ref(&job_key.to_string())).await
            {
                warn!(error = %e, "recording webhook run job key failed");
            }
            let links =
                std::collections::BTreeMap::from([(job_key.to_string(), html_url.to_string())]);
            if let Err(e) = state::run_set_job_links(pool, id, &links).await {
                warn!(error = %e, "recording webhook run job link failed");
            }
            // One job per webhook event, so the concurrency key
            // equals the job key here.
            if let Err(e) = state::run_inflight_add(pool, id, job_key, job_key).await {
                warn!(error = %e, "registering webhook job in flight failed");
            }
            if let Err(e) = state::run_stage_set(pool, id, Some(STAGE_FETCHING_LOGS)).await {
                warn!(error = %e, "setting webhook run stage failed");
            }
            Some((id, guard))
        }
        Err(e) => {
            warn!(error = %e, "opening webhook run row failed; event runs unrecorded");
            None
        }
    }
}

/// Close a webhook run. Writes one `job_outcomes` row
/// carrying the human-readable `summary` (triage bucket + mint result, plus the
/// redacted-log artifact path when present) and finalizes the run through its
/// [`state::RunGuard`], so a failure here still leaves the row closed by the
/// guard's `Drop` retry (CLAUDE-402) rather than open forever. `spend` is
/// this delivery's own ledger: `None` for a path that never reached
/// triage (nothing to bill), `Some` for every path that did, so `claude_ran` /
/// `claude_ok` / `claude_cost_usd` reflect what the classification actually
/// spent instead of always reading false/zero. `attempt_counted = false`: the
/// caps ledger is advanced separately inside [`triage_and_mint`] and
/// must not be double counted here. A no-op when `run` is `None`, except
/// that a real `spend` must still be finished so its usage record is not lost.
async fn finish_webhook_run(
    pool: &DbPool,
    run: Option<(i64, state::RunGuard)>,
    job_key: &str,
    summary: &str,
    ok: bool,
    artifact: Option<&std::path::Path>,
    spend: Option<&crate::usage::RunSpend>,
) {
    let Some((run_id, guard)) = run else {
        if let Some(spend) = spend {
            spend.finish(None, crate::exit::UNEXPECTED);
        }
        return;
    };
    let full = match artifact {
        Some(p) => format!("{summary}; log={}", p.display()),
        None => summary.to_string(),
    };
    let outcome = state::JobOutcome {
        run_id,
        action_name: WEBHOOK_MINT_ACTION,
        job_key,
        attempt: 1,
        ok,
        // The triage classification's own cost, read back
        // off the caller's per-delivery ledger.
        claude_ran: spend.is_some_and(|s| s.claude_ran()),
        claude_ok: spend.is_some_and(|s| s.claude_ok()),
        claude_cost_usd: spend.map(|s| s.cost_usd()).unwrap_or(0.0),
        attempt_counted: false,
        summary: Some(&full),
        stream_log_path: None,
        duration_ms: 0,
        timed_out: false,
    };
    if let Err(e) = state::job_outcome_insert(pool, &outcome).await {
        warn!(error = %e, "recording webhook run outcome failed");
    }
    let base_exit_code = if ok {
        crate::exit::OK
    } else {
        crate::exit::UNEXPECTED
    };
    // An over-budget stop overrides only an otherwise-clean outcome.
    let exit_code = spend.map_or(base_exit_code, |s| s.exit_code(base_exit_code));
    if let Err(e) = guard.finish(exit_code, 1).await {
        error!(run_id, error = %e, "finalizing webhook run failed; the guard retries closing it as unexpected on drop");
    }
    if let Some(spend) = spend {
        spend.finish(Some(run_id), exit_code);
    }
}

/// The run-record summary for one webhook event. `text` is
/// the human-readable outcome (triage bucket + mint result, or an error), and
/// `ok` is whether the pipeline completed: a decided outcome (mint or any skip)
/// is `ok`, a triage/mint fault is not.
struct WebhookRunSummary {
    text: String,
    ok: bool,
}

impl WebhookRunSummary {
    fn decided(bucket: crate::triage::TriageBucket, outcome: &MintOutcome) -> Self {
        Self {
            text: format!("triage={bucket}; {}", mint_summary(outcome)),
            ok: true,
        }
    }

    /// A decision the `ci_failure` store made without ever calling
    /// `mint::mint` (an attach, a reopen, or a legacy adoption).
    fn store_decided(outcome: &StoreOutcome) -> Self {
        Self {
            text: store_outcome_summary(outcome),
            ok: true,
        }
    }

    fn error(message: String) -> Self {
        Self {
            text: format!("triage/mint error: {message}"),
            ok: false,
        }
    }
}

/// The outcomes the `ci_failure` store drives directly, with no
/// `mint::mint` call (and, for [`StoreOutcome::Attached`], no `claude` call
/// either).
enum StoreOutcome {
    /// The fingerprint matched an `open` `ci_failures` row: attached, no
    /// triage, no mint.
    Attached { key: String },
    /// The fingerprint matched a `resolved` row: reopened, triaged, and the
    /// analysis posted as a comment.
    Reopened { key: String },
    /// No store row matched, but a legacy YouTrack full-text search (the
    /// pre-store mechanism) found an issue minted before the store existed.
    Adopted { key: String },
}

/// What [`decide_mint`] decided, inside the blocking task, before
/// any of its async store bookkeeping runs.
enum DecideOutcome {
    /// No store row, but the legacy YouTrack marker search adopted an issue
    /// minted before the store existed.
    LegacyAdopted { key: String },
    /// The ordinary triage + mint path ran to a decision.
    Mint(triage::TriageBucket, MintOutcome),
}

/// One-line description of a [`MintOutcome`] for the run summary: the minted
/// issue key on success, or the skip reason otherwise.
fn mint_summary(outcome: &MintOutcome) -> String {
    match outcome {
        MintOutcome::Created { key, project, .. } => format!("minted {key} in {project}"),
        MintOutcome::SkippedTransient => "no issue (transient)".to_string(),
        MintOutcome::SkippedAttemptCap => "no issue (per-key attempt cap)".to_string(),
        MintOutcome::SkippedDailyCap => "no issue (daily run cap)".to_string(),
    }
}

/// One-line description of the store-driven outcomes that never
/// reach [`mint::mint`] at all: an attach onto an already-tracked failure, or a
/// reopen of a resolved one.
fn store_outcome_summary(outcome: &StoreOutcome) -> String {
    match outcome {
        StoreOutcome::Attached { key } => format!("attached to {key}"),
        StoreOutcome::Reopened { key } => format!("regression; reopened {key}"),
        StoreOutcome::Adopted { key } => format!("adopted legacy issue {key}"),
    }
}

/// Fingerprint the log and look it up in the `ci_failure` store
/// BEFORE triage. A store read failure fails the run rather than falling
/// through to a mint, which would recreate the duplicate this issue removes.
/// `open` attaches with no triage and no `claude` call; `resolved` reopens and
/// triages for the analysis comment and the `AI Agent` write; `none` and
/// `pr_only` (the PR-routing issue owns `pr_only`) fall through to the
/// ordinary classify + mint path, itself preceded by one legacy adoption
/// lookup for an issue minted before the store existed.
#[allow(clippy::too_many_arguments)]
async fn triage_and_mint(
    ctx: &MintContext,
    run_id: Option<i64>,
    ev: &VerifiedFailureEvent,
    log_text: String,
    log_available: bool,
    log_artifact: Option<PathBuf>,
    log_unavailable_reason: Option<String>,
    auto_fix_enabled: bool,
    job_name: String,
    log_token: Option<String>,
    keygate: &crate::concurrency::KeyGate,
    webhook_cfg: &WebhookConfig,
    spend: &crate::usage::RunSpend,
) -> WebhookRunSummary {
    // The store lookup, analysis and mint for one event run under a
    // key gate on (repo, workflow_id, job_name), so two concurrent failures of
    // the same job never both run an analysis: the second waits here and then
    // finds the first one's store row, attaching instead of re-analyzing.
    let key = format!("{}#{}#{}", ev.repo, ev.workflow_id, job_name);
    let _key_guard = keygate.acquire(&key).await;

    // Derive the deterministic failure signature and the branch
    // from the redacted log up front (pure string work); the fingerprint
    // is the failing-test overlap key the store matches on.
    let failure_signature = crate::mint::failure_signature(&log_text);
    let branch = crate::forge_log::resolve_branch(None, &log_text).unwrap_or_default();
    let infra = crate::infra_failure::detect(&log_text);
    if let Some(f) = &infra {
        info!(
            repo = %ev.repo,
            run = ev.index_in_repo,
            kind = %f.kind,
            problem = %f.problem,
            "infrastructure failure detected"
        );
    }
    let infra_kind = infra.as_ref().map(|f| f.kind.clone());
    let test_ids = fingerprint::fingerprint(&log_text);

    let store_match = if log_available {
        match ci_failure_store::find_match(
            &ctx.pool,
            &ev.repo,
            &ev.workflow_id,
            &job_name,
            &test_ids,
        )
        .await
        {
            Ok(m) => m,
            Err(e) => {
                error!(repo = %ev.repo, run = ev.index_in_repo, error = %e, "ci-failure store read failed; not minting");
                return WebhookRunSummary::error(format!("ci-failure store error: {e}"));
            }
        }
    } else {
        None
    };

    // Resolve whether the failing commit itself is on an open PR
    // BEFORE the store-match dispatch, not as a fallback from it: an agent's
    // own PR must intercept before the ordinary open/resolved handling runs,
    // and a human's PR always needs a PR comment, including one that shares a
    // failing test with an already-`open` failure. A lookup error is never
    // read as "no PR" (see [`resolve_open_pr`]).
    let forge = forge_for_event(ctx, ev);
    let event = ev.clone();
    let pr_result =
        tokio::task::spawn_blocking(move || resolve_open_pr(forge.as_ref(), &event)).await;
    let pr = match pr_result {
        Ok(Ok(pr)) => pr,
        Ok(Err(e)) => {
            error!(repo = %ev.repo, run = ev.index_in_repo, error = %e, "open-PR lookup failed; not routing");
            record_run_event(
                &ctx.pool,
                run_id,
                WEBHOOK_MINT_ACTION,
                "error",
                format!("open-PR lookup failed: {e}"),
            )
            .await;
            return WebhookRunSummary::error(format!("open-PR lookup failed: {e}"));
        }
        Err(e) => {
            error!(repo = %ev.repo, run = ev.index_in_repo, error = %e, "open-PR lookup task panicked");
            return WebhookRunSummary::error(format!("task panicked: {e}"));
        }
    };

    let Some(pr) = pr else {
        // No open PR for this commit: the store dispatch, run
        // exactly as before.
        if let Some(m) = store_match {
            if m.state == "open" {
                if let Some(key) = m.issue_key.clone() {
                    let resolved_state = ctx.mint_cfg.read().await.resolved_state.clone();
                    match issue_state_is_resolved(ctx, &key, &resolved_state).await {
                        Ok(true) => {
                            return reopen_resolved_failure(
                                ctx,
                                run_id,
                                ev,
                                log_text,
                                log_available,
                                log_unavailable_reason,
                                auto_fix_enabled,
                                branch,
                                test_ids,
                                log_token,
                                m,
                                spend,
                            )
                            .await;
                        }
                        Ok(false) => {
                            return attach_to_open_failure(ctx, ev, &branch, &test_ids, m).await;
                        }
                        Err(e) => {
                            error!(repo = %ev.repo, run = ev.index_in_repo, %key, error = %e, "youtrack state read failed; not attaching");
                            record_run_event(
                                &ctx.pool,
                                run_id,
                                WEBHOOK_MINT_ACTION,
                                "error",
                                format!("youtrack state read failed: {e}"),
                            )
                            .await;
                            return WebhookRunSummary::error(format!(
                                "youtrack state read failed: {e}"
                            ));
                        }
                    }
                }
                return attach_to_open_failure(ctx, ev, &branch, &test_ids, m).await;
            }
            if m.state == "resolved" {
                return reopen_resolved_failure(
                    ctx,
                    run_id,
                    ev,
                    log_text,
                    log_available,
                    log_unavailable_reason,
                    auto_fix_enabled,
                    branch,
                    test_ids,
                    log_token,
                    m,
                    spend,
                )
                .await;
            }
        }
        // `None` and `pr_only` both fall through to the ordinary classify +
        // mint path, as they did before this issue.
        return classify_and_mint(
            ctx,
            run_id,
            ev,
            log_text,
            log_available,
            log_artifact,
            log_unavailable_reason,
            auto_fix_enabled,
            job_name,
            branch,
            failure_signature,
            infra_kind,
            test_ids,
            log_token,
            webhook_cfg.max_runs_per_day,
            spend,
        )
        .await;
    };

    let Some(host) = forge_host_from_url(&ev.html_url) else {
        // Unreachable in practice: resolve_open_pr above already needed this
        // same host to find the PR it just returned.
        error!(repo = %ev.repo, run = ev.index_in_repo, "could not derive forge host after resolving an open PR");
        return WebhookRunSummary::error("could not derive forge host".to_string());
    };

    match ci_failure_store::agent_pr(&ctx.pool, &host, &ev.repo, pr.number as i64).await {
        Ok(Some(record)) if record.state == "open" => {
            route_agent_pr_failure(
                ctx,
                ev,
                log_text,
                log_available,
                log_unavailable_reason,
                log_token,
                &job_name,
                &branch,
                infra_kind.as_deref(),
                &test_ids,
                record,
                webhook_cfg,
                spend,
            )
            .await
        }
        Ok(_) => {
            route_human_pr_failure(
                ctx,
                run_id,
                ev,
                log_text,
                log_available,
                log_unavailable_reason,
                auto_fix_enabled,
                &job_name,
                &branch,
                failure_signature.as_deref(),
                infra_kind.as_deref(),
                &test_ids,
                log_token,
                store_match,
                &pr,
                &host,
                webhook_cfg,
                spend,
            )
            .await
        }
        Err(e) => {
            error!(repo = %ev.repo, run = ev.index_in_repo, error = %e, "ci-failure store read failed");
            WebhookRunSummary::error(format!("ci-failure store error: {e}"))
        }
    }
}

/// The comment posted when a failure attaches to an already-open
/// `ci_failures` row. No triage ran, so this names the run and the
/// overlapping failing tests rather than an analysis.
fn attach_comment_body(ev: &VerifiedFailureEvent, branch: &str, test_ids: &[String]) -> String {
    let short_sha: String = ev.commit_sha.chars().take(8).collect();
    let tests = if test_ids.is_empty() {
        "(no test ids parsed)".to_string()
    } else {
        test_ids.join(", ")
    };
    format!(
        "`{}` failed again on `{}` (run #{}, {}, branch `{}`).\n- Run: {}\n- Failing tests: {tests}",
        ev.workflow_id, ev.repo, ev.index_in_repo, short_sha, branch, ev.html_url
    )
}

/// Read `key`'s live YouTrack `State` and report whether it
/// already equals `resolved_state` (case-insensitive). The store's own
/// `open`/`resolved` column no longer changes on merge (the runner writes no
/// `State` there itself: `docs/yt-workflow-resolve-on-pr-merge.js` does), so
/// an `open` store row can still name an issue the workflow already
/// resolved; this is the match-time check that tells the two apart. A read
/// failure is never read as "still open": the caller reports it as an error
/// rather than risking a false attach.
async fn issue_state_is_resolved(
    ctx: &MintContext,
    key: &str,
    resolved_state: &str,
) -> crate::error::Result<bool> {
    let backend = ctx.backend.clone();
    let key = key.to_string();
    match tokio::task::spawn_blocking(move || backend.get_field(&key, "State")).await {
        Ok(Ok(state)) => Ok(state.eq_ignore_ascii_case(resolved_state)),
        Ok(Err(e)) => Err(e),
        Err(e) => Err(crate::error::Error::Other(anyhow::anyhow!(
            "state-read task panicked: {e}"
        ))),
    }
}

/// Attach `ev` to the already-tracked, still-`open` failure `m`
/// matched. No triage, no mint, no `claude` call: the failure is already
/// filed and this run is just another sighting of it.
async fn attach_to_open_failure(
    ctx: &MintContext,
    ev: &VerifiedFailureEvent,
    branch: &str,
    test_ids: &[String],
    m: ci_failure_store::FailureMatch,
) -> WebhookRunSummary {
    let Some(key) = m.issue_key.clone() else {
        error!(repo = %ev.repo, run = ev.index_in_repo, failure_id = m.id, "open ci_failures row has no issue_key");
        return WebhookRunSummary::error("ci-failure store row has no issue_key".to_string());
    };
    let now = state::now_iso();
    if let Err(e) = ci_failure_store::add_tests(&ctx.pool, m.id, test_ids).await {
        error!(repo = %ev.repo, run = ev.index_in_repo, error = %e, "recording failing tests failed");
        return WebhookRunSummary::error(format!("ci-failure store error: {e}"));
    }
    if let Err(e) = ci_failure_store::attach_run(
        &ctx.pool,
        m.id,
        ev.index_in_repo,
        &ev.commit_sha,
        branch,
        None,
        &now,
    )
    .await
    {
        error!(repo = %ev.repo, run = ev.index_in_repo, error = %e, "recording run attachment failed");
        return WebhookRunSummary::error(format!("ci-failure store error: {e}"));
    }

    let backend = ctx.backend.clone();
    let body = attach_comment_body(ev, branch, test_ids);
    let key_for_comment = key.clone();
    let result =
        tokio::task::spawn_blocking(move || backend.comment(&key_for_comment, &body)).await;
    match result {
        Ok(Ok(())) => {}
        Ok(Err(e)) => {
            warn!(repo = %ev.repo, run = ev.index_in_repo, %key, error = %e, "commenting on attached issue failed");
        }
        Err(e) => {
            warn!(repo = %ev.repo, run = ev.index_in_repo, %key, error = %e, "attach comment task panicked");
        }
    }
    WebhookRunSummary::store_decided(&StoreOutcome::Attached { key })
}

/// The comment posted immediately on reopening a resolved
/// `ci_failures` row, before the triage analysis is posted separately.
fn regression_comment_body(ev: &VerifiedFailureEvent, branch: &str, test_ids: &[String]) -> String {
    let tests = if test_ids.is_empty() {
        "(no test ids parsed)".to_string()
    } else {
        test_ids.join(", ")
    };
    format!(
        "Regression: run {} on `{branch}` fails {tests} again; reopened.",
        ev.html_url
    )
}

/// Reopen the resolved failure `m` matched: `set_state` back to
/// `target_state`, comment the regression, mark the store row `open` and
/// attach this run, then triage (for the analysis comment) and set `AI Agent`
/// exactly as a freshly created issue of that bucket would be.
#[allow(clippy::too_many_arguments)]
async fn reopen_resolved_failure(
    ctx: &MintContext,
    run_id: Option<i64>,
    ev: &VerifiedFailureEvent,
    log_text: String,
    log_available: bool,
    log_unavailable_reason: Option<String>,
    auto_fix_enabled: bool,
    branch: String,
    test_ids: Vec<String>,
    log_token: Option<String>,
    m: ci_failure_store::FailureMatch,
    spend: &crate::usage::RunSpend,
) -> WebhookRunSummary {
    let Some(key) = m.issue_key.clone() else {
        error!(repo = %ev.repo, run = ev.index_in_repo, failure_id = m.id, "resolved ci_failures row has no issue_key");
        return WebhookRunSummary::error("ci-failure store row has no issue_key".to_string());
    };
    let now = state::now_iso();
    if let Err(e) = ci_failure_store::set_failure_state(&ctx.pool, m.id, "open", &now).await {
        error!(repo = %ev.repo, run = ev.index_in_repo, error = %e, "reopening ci-failure store row failed");
        return WebhookRunSummary::error(format!("ci-failure store error: {e}"));
    }
    if let Err(e) = ci_failure_store::add_tests(&ctx.pool, m.id, &test_ids).await {
        error!(repo = %ev.repo, run = ev.index_in_repo, error = %e, "recording failing tests failed");
        return WebhookRunSummary::error(format!("ci-failure store error: {e}"));
    }
    if let Err(e) = ci_failure_store::attach_run(
        &ctx.pool,
        m.id,
        ev.index_in_repo,
        &ev.commit_sha,
        &branch,
        None,
        &now,
    )
    .await
    {
        error!(repo = %ev.repo, run = ev.index_in_repo, error = %e, "recording run attachment failed");
        return WebhookRunSummary::error(format!("ci-failure store error: {e}"));
    }

    let backend = ctx.backend.clone();
    let forge = forge_for_event(ctx, ev);
    let classifier = ctx.classifier.clone();
    let mint_cfg = ctx.mint_cfg.read().await.clone();
    let event = ev.clone();
    let key2 = key.clone();
    let regression_body = regression_comment_body(ev, &branch, &test_ids);
    let spend = spend.clone();
    let bucket =
        tokio::task::spawn_blocking(move || -> crate::error::Result<triage::TriageBucket> {
            backend.set_state(&key2, &mint_cfg.target_state)?;
            backend.comment(&key2, &regression_body)?;
            let has_open_pr = resolve_has_open_pr(forge.as_ref(), &event);
            let input = TriageInput {
                repo: event.repo.clone(),
                workflow_id: event.workflow_id.clone(),
                commit_sha: event.commit_sha.clone(),
                html_url: event.html_url.clone(),
                log: log_text,
                log_available,
                log_unavailable_reason,
                has_open_pr,
                log_token,
            };
            let verdict = triage::triage(&input, classifier.as_ref(), &mint_cfg.auto_fix, &spend);
            backend.comment(&key2, &verdict.analysis)?;
            if mint::queues_for_fix(verdict.bucket) && auto_fix_enabled {
                backend.set_field(&key2, &mint_cfg.ai_agent_field, &mint_cfg.queued_value)?;
            }
            Ok(verdict.bucket)
        })
        .await;

    let bucket = match bucket {
        Ok(Ok(b)) => b,
        Ok(Err(e)) => {
            warn!(repo = %ev.repo, run = ev.index_in_repo, error = %e, "reopen triage failed");
            return WebhookRunSummary::error(e.to_string());
        }
        Err(e) => {
            warn!(repo = %ev.repo, run = ev.index_in_repo, error = %e, "reopen triage task panicked");
            return WebhookRunSummary::error(format!("task panicked: {e}"));
        }
    };
    let _ = run_id;
    info!(repo = %ev.repo, run = ev.index_in_repo, %key, bucket = %bucket, "regression reopened a resolved ci-failure issue");
    WebhookRunSummary::store_decided(&StoreOutcome::Reopened { key })
}

/// Record a brand-new `ci_failures` row for `issue_key` (either
/// freshly minted, or a legacy issue this delivery adopted), and the tests /
/// run this delivery already carries.
async fn record_new_store_row(
    pool: &DbPool,
    ev: &VerifiedFailureEvent,
    job_name: &str,
    infra_kind: Option<&str>,
    issue_key: &str,
    branch: &str,
    test_ids: &[String],
) -> crate::error::Result<()> {
    let now = state::now_iso();
    let id = ci_failure_store::record_failure(
        pool,
        &ev.repo,
        &ev.workflow_id,
        job_name,
        infra_kind,
        Some(issue_key),
        "open",
        &now,
    )
    .await?;
    ci_failure_store::add_tests(pool, id, test_ids).await?;
    ci_failure_store::attach_run(
        pool,
        id,
        ev.index_in_repo,
        &ev.commit_sha,
        branch,
        None,
        &now,
    )
    .await?;
    Ok(())
}

/// Best-effort record a `run_events` row for something that went
/// wrong outside the ordinary per-job accounting (an open-PR lookup failure, a
/// missing `write_token_ref`): attaches to the run when one exists, else a
/// standalone row (never backfilled, but still visible on the action's event
/// log) like [`login_required_accept_and_drop`] writes.
async fn record_run_event(
    pool: &DbPool,
    run_id: Option<i64>,
    action: &str,
    level: &str,
    message: String,
) {
    let event = state::InputEvent {
        level: level.to_string(),
        context: Some(action.to_string()),
        message,
    };
    let result = match run_id {
        Some(id) => state::run_events_record_for_run(pool, id, action, &[event]).await,
        None => state::run_events_record(pool, action, &state::now_iso(), &[event]).await,
    };
    if let Err(e) = result {
        warn!(error = %e, "recording a run_events row failed (continuing)");
    }
}

/// True when some open PR's head commit matches `commit_sha`, and the PR
/// itself (tolerant of a short / long SHA mismatch, accepting a prefix
/// relationship in either direction).
fn pr_matching_commit<'a>(
    prs: &'a [ForgePullRequest],
    commit_sha: &str,
) -> Option<&'a ForgePullRequest> {
    prs.iter().find(|p| {
        !p.head_sha.is_empty()
            && (p.head_sha == commit_sha
                || commit_sha.starts_with(&p.head_sha)
                || p.head_sha.starts_with(commit_sha))
    })
}

/// Find the open PR (if any) whose head matches the failing
/// commit. Replaces the old `resolve_has_open_pr` for the routing
/// decision: that helper degraded a forge error or an underivable host to
/// `false` ("no PR"), which is safe only as advisory context for the triage
/// classifier. For routing it is not safe - it would send the agent's own
/// failing PR through the create-a-new-issue path instead of re-queuing it,
/// and would drop a human's failure with no comment. A lookup error (or an
/// underivable host) is therefore returned as an error, never folded into
/// `Ok(None)`.
fn resolve_open_pr(
    forge: &dyn Forge,
    ev: &VerifiedFailureEvent,
) -> crate::error::Result<Option<ForgePullRequest>> {
    let Some(host) = forge_host_from_url(&ev.html_url) else {
        return Err(crate::error::Error::Other(anyhow::anyhow!(
            "could not derive forge host from html_url `{}`",
            ev.html_url
        )));
    };
    let prs = forge.list_open_prs(&host, &ev.repo)?;
    Ok(pr_matching_commit(&prs, &ev.commit_sha).cloned())
}

/// The [`Forge`] a failing run's own open-PR lookups go through:
/// [`GithubForge`] for a `github.com` `html_url` (the same `github.com`-only
/// default [`resolve_forge`]'s other callers use - no action is known yet at
/// this point in the pipeline, so there is no per-action `github.hosts` GHE
/// list to consult), `mint.backend` (`fj`) otherwise, including when the host
/// cannot be derived at all: [`resolve_open_pr`] and [`resolve_has_open_pr`]
/// each re-derive the host themselves and fail/degrade on that same
/// derivation, so the fallback here never reaches a `list_open_prs` call.
fn forge_for_event(ctx: &MintContext, ev: &VerifiedFailureEvent) -> Arc<dyn Forge> {
    match forge_host_from_url(&ev.html_url) {
        Some(host) => resolve_forge(
            ctx,
            &host,
            &[],
            &crate::action::default_github_mcp_endpoint(),
        ),
        None => ctx.backend.clone() as Arc<dyn Forge>,
    }
}

/// On-disk root for a PR-comment backend's temp single-host
/// `keys.json`, alongside `comment_webhooks::creds_root`'s own `comment-creds`
/// sibling.
fn pr_creds_root(paths: &Paths) -> PathBuf {
    paths.runtime_dir.join("webhook-pr-creds")
}

/// A forge backend scoped to one webhook source's `write_token_ref`. Held
/// until every `pr_comment` call for this routing decision has run, then
/// dropped: `_creds`'s `Drop` scrubs its temp `keys.json` directory.
struct PrCommentEnv {
    backend: Box<dyn Backend>,
    _creds: secrets::JobCredentials,
}

/// Where a PR comment's body is staged before posting: a
/// deterministic path (not the ephemeral, randomly-suffixed credential
/// directory `PrCommentEnv` holds), so a repeated comment on the same PR
/// simply overwrites its own scratch file rather than accumulating one per
/// routing decision.
fn pr_comment_body_path(paths: &Paths, repo: &str, pr_number: u64) -> PathBuf {
    paths.state_dir.join(format!(
        "webhook-pr-comment-{}-{pr_number}.md",
        repo.replace('/', "_")
    ))
}

/// Resolve `source`'s `write_token_ref` and build a forge backend
/// carrying it, so a PR-comment token never appears in YAML, env, a process
/// argument, or a settings response. A source with no `write_token_ref`
/// configured is a hard failure: it must never be silent about being unable
/// to post. A GitHub-provider source (`source`'s own configured
/// [`WebhookProvider`], not a host guess - the config already says which forge
/// this delivery came from) routes through [`GithubForge`] instead of the
/// `fj`-based [`CliBackend`], mirroring `native::youtrack`'s per-job Forge
/// pick (`provider::is_github_host` keyed off `gh.hosts`) and
/// `comment_webhooks::build_env`'s identical branch.
async fn build_pr_comment_backend(
    ctx: &MintContext,
    webhook_cfg: &WebhookConfig,
    source: &str,
    host: &str,
) -> crate::error::Result<PrCommentEnv> {
    let src_cfg = webhook_cfg.sources.get(source).ok_or_else(|| {
        crate::error::Error::Secret(format!("webhook source `{source}` is not configured"))
    })?;
    let token_ref = src_cfg.write_token_ref.as_deref().ok_or_else(|| {
        crate::error::Error::Secret(format!(
            "webhook source `{source}` has no write_token_ref; it cannot comment on pull requests"
        ))
    })?;
    let key = SecretsKey::load()?;
    let token = secrets::resolve_secret(&ctx.pool, &key, token_ref)
        .await?
        .trim()
        .to_string();
    let dir = pr_creds_root(&ctx.paths).join(secrets::unique_suffix());
    if src_cfg.provider == WebhookProvider::Github {
        let creds = secrets::JobCredentials::materialize(
            &dir,
            &secrets::ResolvedCredentials {
                github_token: Some(token),
                ..Default::default()
            },
            None,
        )?;
        let forge = GithubForge::new(
            ctx.pr_comment_runner.clone(),
            crate::action::default_github_mcp_endpoint(),
            creds.github_token.clone(),
        );
        return Ok(PrCommentEnv {
            backend: Box::new(forge),
            _creds: creds,
        });
    }
    let creds = secrets::JobCredentials::materialize(
        &dir,
        &secrets::ResolvedCredentials {
            fj_token: Some(token),
            ..Default::default()
        },
        Some(host),
    )?;
    let mut backend =
        CliBackend::new(ctx.pr_comment_runner.clone()).with_fj_env(creds.fj_env.clone());
    if let Some(keys) = &creds.fj_keys_path {
        backend = backend.with_keys_path(keys.clone());
    }
    Ok(PrCommentEnv {
        backend: Box::new(backend),
        _creds: creds,
    })
}

/// [`build_pr_comment_backend`], with the missing-token failure
/// already logged and recorded as a `run_events` row - the shape every PR-
/// commenting caller in this module needs.
async fn require_pr_comment_backend(
    ctx: &MintContext,
    run_id: Option<i64>,
    ev: &VerifiedFailureEvent,
    host: &str,
    webhook_cfg: &WebhookConfig,
) -> Result<PrCommentEnv, WebhookRunSummary> {
    match build_pr_comment_backend(ctx, webhook_cfg, &ev.source, host).await {
        Ok(env) => Ok(env),
        Err(e) => {
            error!(repo = %ev.repo, run = ev.index_in_repo, source = %ev.source, error = %e, "no write-scoped forge backend for this webhook source; cannot comment on the PR");
            record_run_event(
                &ctx.pool,
                run_id,
                WEBHOOK_MINT_ACTION,
                "error",
                format!("PR comment failed: {e}"),
            )
            .await;
            Err(WebhookRunSummary::error(format!(
                "PR comment backend error: {e}"
            )))
        }
    }
}

/// Write `body` to `body_file` and post it on PR `pr_number`.
fn write_and_post_pr_comment(
    backend: &dyn Backend,
    host: &str,
    repo: &str,
    pr_number: u64,
    body_file: &std::path::Path,
    body: &str,
) -> crate::error::Result<()> {
    std::fs::write(body_file, body)?;
    backend.pr_comment(host, repo, pr_number, body_file)
}

/// Run triage (for its analysis text) and post that text as a PR comment
/// through `env`'s write-scoped backend. Returns the analysis text.
#[allow(clippy::too_many_arguments)]
async fn analyze_and_comment_pr(
    ctx: &MintContext,
    ev: &VerifiedFailureEvent,
    log_text: String,
    log_available: bool,
    log_unavailable_reason: Option<String>,
    log_token: Option<String>,
    pr_number: u64,
    host: &str,
    env: PrCommentEnv,
    spend: &crate::usage::RunSpend,
) -> crate::error::Result<String> {
    let classifier = ctx.classifier.clone();
    let mint_cfg = ctx.mint_cfg.read().await.clone();
    let event = ev.clone();
    let host_owned = host.to_string();
    let body_file = pr_comment_body_path(&ctx.paths, &ev.repo, pr_number);
    let spend = spend.clone();
    let result = tokio::task::spawn_blocking(move || -> crate::error::Result<String> {
        let input = TriageInput {
            repo: event.repo.clone(),
            workflow_id: event.workflow_id.clone(),
            commit_sha: event.commit_sha.clone(),
            html_url: event.html_url.clone(),
            log: log_text,
            log_available,
            log_unavailable_reason,
            has_open_pr: true,
            log_token,
        };
        let verdict = triage::triage(&input, classifier.as_ref(), &mint_cfg.auto_fix, &spend);
        write_and_post_pr_comment(
            env.backend.as_ref(),
            &host_owned,
            &event.repo,
            pr_number,
            &body_file,
            &verdict.analysis,
        )?;
        Ok(verdict.analysis)
    })
    .await;
    match result {
        Ok(inner) => inner,
        Err(e) => Err(crate::error::Error::Other(anyhow::anyhow!(
            "PR analysis task panicked: {e}"
        ))),
    }
}

/// `AI Agent` value flagging an issue for a human to look at, mirroring
/// `youtrack.states.needs_review`'s default (`src/action.rs`); this path has
/// no dependency on the `youtrack` action's own config, so the value is
/// spelled directly rather than threaded through another config section.
const NEEDS_REVIEW_VALUE: &str = "Needs Review";

/// `ev` failed on an open PR the agent itself opened (an `open`
/// `agent_pull_requests` row). Below `webhook_cfg.agent_pr_fix_attempts`,
/// comment the recorded issue with the run, the failing tests and the
/// analysis, bump the attempt counter, and re-queue it (`AI Agent = Queued`);
/// at the ceiling, flag the issue `Needs Review` instead, listing every
/// attempt's run. Either way: no PR comment, no new issue - the failure
/// belongs to an issue that already exists.
#[allow(clippy::too_many_arguments)]
async fn route_agent_pr_failure(
    ctx: &MintContext,
    ev: &VerifiedFailureEvent,
    log_text: String,
    log_available: bool,
    log_unavailable_reason: Option<String>,
    log_token: Option<String>,
    job_name: &str,
    branch: &str,
    infra_kind: Option<&str>,
    test_ids: &[String],
    record: ci_failure_store::AgentPullRequest,
    webhook_cfg: &WebhookConfig,
    spend: &crate::usage::RunSpend,
) -> WebhookRunSummary {
    let now = state::now_iso();
    let failure_id = match ci_failure_store::failure_for_issue(&ctx.pool, &record.issue_key).await {
        Ok(Some(f)) => f.id,
        Ok(None) => {
            match ci_failure_store::record_failure(
                &ctx.pool,
                &ev.repo,
                &ev.workflow_id,
                job_name,
                infra_kind,
                Some(&record.issue_key),
                "open",
                &now,
            )
            .await
            {
                Ok(id) => id,
                Err(e) => {
                    error!(repo = %ev.repo, run = ev.index_in_repo, error = %e, "creating the ci-failure store row for the agent's PR failed");
                    return WebhookRunSummary::error(format!("ci-failure store error: {e}"));
                }
            }
        }
        Err(e) => {
            error!(repo = %ev.repo, run = ev.index_in_repo, error = %e, "ci-failure store read failed");
            return WebhookRunSummary::error(format!("ci-failure store error: {e}"));
        }
    };
    if let Err(e) = ci_failure_store::add_tests(&ctx.pool, failure_id, test_ids).await {
        error!(repo = %ev.repo, run = ev.index_in_repo, error = %e, "recording failing tests failed");
        return WebhookRunSummary::error(format!("ci-failure store error: {e}"));
    }
    let run_id = match ci_failure_store::attach_run(
        &ctx.pool,
        failure_id,
        ev.index_in_repo,
        &ev.commit_sha,
        branch,
        Some(record.pr_number),
        &now,
    )
    .await
    {
        Ok(id) => id,
        Err(e) => {
            error!(repo = %ev.repo, run = ev.index_in_repo, error = %e, "recording run attachment failed");
            return WebhookRunSummary::error(format!("ci-failure store error: {e}"));
        }
    };

    // CLAUDE-371: compare this attempt's fingerprint with the immediately
    // previous attempt's on the same PR before deciding whether to re-queue.
    // An empty fingerprint never counts as "same as before" (a genuinely
    // unparseable log would otherwise look identical to every other
    // unparseable log and falsely stop the loop), so it falls through to the
    // plain attempt-cap behavior below.
    let previous = match ci_failure_store::previous_run_test_ids(
        &ctx.pool,
        failure_id,
        record.pr_number,
        run_id,
    )
    .await
    {
        Ok(previous) => previous,
        Err(e) => {
            error!(repo = %ev.repo, run = ev.index_in_repo, error = %e, "reading the previous attempt's fingerprint failed");
            return WebhookRunSummary::error(format!("ci-failure store error: {e}"));
        }
    };
    if let Err(e) = ci_failure_store::set_run_test_ids(&ctx.pool, run_id, test_ids).await {
        error!(repo = %ev.repo, run = ev.index_in_repo, error = %e, "recording this attempt's fingerprint failed");
        return WebhookRunSummary::error(format!("ci-failure store error: {e}"));
    }
    let repeats_previous = !test_ids.is_empty()
        && previous.as_ref().is_some_and(|(_, prev_ids)| {
            let prev_set: std::collections::BTreeSet<&str> =
                prev_ids.iter().map(String::as_str).collect();
            let cur_set: std::collections::BTreeSet<&str> =
                test_ids.iter().map(String::as_str).collect();
            prev_set == cur_set
        });

    if repeats_previous {
        let previous_index = previous.map(|(idx, _)| idx).unwrap_or_default();
        let base = ev.html_url.rsplit_once("/actions/runs/").map(|(b, _)| b);
        let previous_link = match base {
            Some(b) => format!("{b}/actions/runs/{previous_index}"),
            None => format!("run #{previous_index}"),
        };
        let tests_display = test_ids.join(", ");
        let body = format!(
            "CI failed the same way as the previous attempt on this PR (branch `{branch}`); stopping the fix-attempt loop instead of re-queueing.\n- Repeated failing tests: {tests_display}\n- This run: {}\n- Previous run: {previous_link}",
            ev.html_url
        );
        let backend = ctx.backend.clone();
        let mint_cfg = ctx.mint_cfg.read().await.clone();
        let key = record.issue_key.clone();
        let result = tokio::task::spawn_blocking(move || -> crate::error::Result<()> {
            backend.comment(&key, &body)?;
            backend.set_field(&key, &mint_cfg.ai_agent_field, NEEDS_REVIEW_VALUE)?;
            Ok(())
        })
        .await;
        match result {
            Ok(Ok(())) => {}
            Ok(Err(e)) => {
                warn!(repo = %ev.repo, run = ev.index_in_repo, key = %record.issue_key, error = %e, "flagging the agent's cyclical PR Needs Review failed");
                return WebhookRunSummary::error(e.to_string());
            }
            Err(e) => {
                warn!(repo = %ev.repo, run = ev.index_in_repo, key = %record.issue_key, error = %e, "flag task panicked");
                return WebhookRunSummary::error(format!("task panicked: {e}"));
            }
        }
        info!(repo = %ev.repo, run = ev.index_in_repo, key = %record.issue_key, "CI failure repeats the previous attempt unchanged; stopped the fix-attempt loop");
        return WebhookRunSummary {
            text: format!(
                "{} flagged Needs Review (repeated CI failure)",
                record.issue_key
            ),
            ok: true,
        };
    }

    if (record.fix_attempts as u32) < webhook_cfg.agent_pr_fix_attempts {
        let backend = ctx.backend.clone();
        let classifier = ctx.classifier.clone();
        let mint_cfg = ctx.mint_cfg.read().await.clone();
        let event = ev.clone();
        let key = record.issue_key.clone();
        let tests = test_ids.to_vec();
        let branch_owned = branch.to_string();
        let spend = spend.clone();
        let result = tokio::task::spawn_blocking(move || -> crate::error::Result<()> {
            let input = TriageInput {
                repo: event.repo.clone(),
                workflow_id: event.workflow_id.clone(),
                commit_sha: event.commit_sha.clone(),
                html_url: event.html_url.clone(),
                log: log_text,
                log_available,
                log_unavailable_reason,
                has_open_pr: true,
                log_token,
            };
            let verdict = triage::triage(&input, classifier.as_ref(), &mint_cfg.auto_fix, &spend);
            let tests_display = if tests.is_empty() {
                "(no test ids parsed)".to_string()
            } else {
                tests.join(", ")
            };
            let body = format!(
                "CI failed again on this PR (run #{}, branch `{branch_owned}`).\n- Run: {}\n- Failing tests: {tests_display}\n\n{}",
                event.index_in_repo, event.html_url, verdict.analysis
            );
            backend.comment(&key, &body)?;
            backend.set_field(&key, &mint_cfg.ai_agent_field, &mint_cfg.queued_value)?;
            Ok(())
        })
        .await;
        match result {
            Ok(Ok(())) => {}
            Ok(Err(e)) => {
                warn!(repo = %ev.repo, run = ev.index_in_repo, key = %record.issue_key, error = %e, "re-queuing the agent's own PR failed");
                return WebhookRunSummary::error(e.to_string());
            }
            Err(e) => {
                warn!(repo = %ev.repo, run = ev.index_in_repo, key = %record.issue_key, error = %e, "re-queue task panicked");
                return WebhookRunSummary::error(format!("task panicked: {e}"));
            }
        }
        if let Err(e) = ci_failure_store::bump_fix_attempts(&ctx.pool, record.id, &now).await {
            warn!(error = %e, "bumping the agent PR's fix-attempt counter failed (continuing)");
        }
        info!(repo = %ev.repo, run = ev.index_in_repo, key = %record.issue_key, "CI failure on the agent's own open PR; re-queued its issue");
        WebhookRunSummary {
            text: format!("re-queued {} (agent PR fix attempt)", record.issue_key),
            ok: true,
        }
    } else {
        let runs =
            ci_failure_store::run_indexes_for_failure_pr(&ctx.pool, failure_id, record.pr_number)
                .await
                .unwrap_or_default();
        let base = ev.html_url.rsplit_once("/actions/runs/").map(|(b, _)| b);
        let links = runs
            .iter()
            .map(|idx| match base {
                Some(b) => format!("- {b}/actions/runs/{idx}"),
                None => format!("- run #{idx}"),
            })
            .collect::<Vec<_>>()
            .join("\n");
        let body = format!(
            "Fix-attempt ceiling ({}) reached on this PR; flagging Needs Review.\nAttempts:\n{links}",
            webhook_cfg.agent_pr_fix_attempts
        );
        let backend = ctx.backend.clone();
        let mint_cfg = ctx.mint_cfg.read().await.clone();
        let key = record.issue_key.clone();
        let result = tokio::task::spawn_blocking(move || -> crate::error::Result<()> {
            backend.comment(&key, &body)?;
            backend.set_field(&key, &mint_cfg.ai_agent_field, NEEDS_REVIEW_VALUE)?;
            Ok(())
        })
        .await;
        match result {
            Ok(Ok(())) => {}
            Ok(Err(e)) => {
                warn!(repo = %ev.repo, run = ev.index_in_repo, key = %record.issue_key, error = %e, "flagging the agent's PR Needs Review failed");
                return WebhookRunSummary::error(e.to_string());
            }
            Err(e) => {
                warn!(repo = %ev.repo, run = ev.index_in_repo, key = %record.issue_key, error = %e, "flag task panicked");
                return WebhookRunSummary::error(format!("task panicked: {e}"));
            }
        }
        info!(repo = %ev.repo, run = ev.index_in_repo, key = %record.issue_key, "agent PR fix-attempt ceiling reached; flagged Needs Review");
        WebhookRunSummary {
            text: format!(
                "{} flagged Needs Review (agent PR fix ceiling)",
                record.issue_key
            ),
            ok: true,
        }
    }
}

/// `ev` failed on an open PR that is NOT the agent's own. Routes by
/// whether its failing tests overlap an already-tracked failure.
#[allow(clippy::too_many_arguments)]
async fn route_human_pr_failure(
    ctx: &MintContext,
    run_id: Option<i64>,
    ev: &VerifiedFailureEvent,
    log_text: String,
    log_available: bool,
    log_unavailable_reason: Option<String>,
    auto_fix_enabled: bool,
    job_name: &str,
    branch: &str,
    failure_signature: Option<&str>,
    infra_kind: Option<&str>,
    test_ids: &[String],
    log_token: Option<String>,
    store_match: Option<ci_failure_store::FailureMatch>,
    pr: &ForgePullRequest,
    host: &str,
    webhook_cfg: &WebhookConfig,
    spend: &crate::usage::RunSpend,
) -> WebhookRunSummary {
    match store_match {
        Some(m) if m.state == "open" => {
            if let Some(key) = m.issue_key.clone() {
                let resolved_state = ctx.mint_cfg.read().await.resolved_state.clone();
                match issue_state_is_resolved(ctx, &key, &resolved_state).await {
                    Ok(true) => {
                        return reopen_resolved_failure(
                            ctx,
                            run_id,
                            ev,
                            log_text,
                            log_available,
                            log_unavailable_reason,
                            auto_fix_enabled,
                            branch.to_string(),
                            test_ids.to_vec(),
                            log_token,
                            m,
                            spend,
                        )
                        .await;
                    }
                    Ok(false) => {}
                    Err(e) => {
                        error!(repo = %ev.repo, run = ev.index_in_repo, %key, error = %e, "youtrack state read failed; not attaching");
                        record_run_event(
                            &ctx.pool,
                            run_id,
                            WEBHOOK_MINT_ACTION,
                            "error",
                            format!("youtrack state read failed: {e}"),
                        )
                        .await;
                        return WebhookRunSummary::error(format!(
                            "youtrack state read failed: {e}"
                        ));
                    }
                }
            }
            route_pr_matches_open_failure(
                ctx,
                run_id,
                ev,
                branch,
                test_ids,
                &m,
                pr,
                host,
                webhook_cfg,
            )
            .await
        }
        // A human PR matching a `resolved` failure is a regression exactly as
        // it would be off a PR: reopen and triage for the analysis comment on
        // the issue, unchanged by this issue (it is not one of the three
        // human-PR cases the issue enumerates).
        Some(m) if m.state == "resolved" => {
            reopen_resolved_failure(
                ctx,
                run_id,
                ev,
                log_text,
                log_available,
                log_unavailable_reason,
                auto_fix_enabled,
                branch.to_string(),
                test_ids.to_vec(),
                log_token,
                m,
                spend,
            )
            .await
        }
        Some(m) if m.state == "pr_only" => {
            route_pr_matches_pr_only_failure(
                ctx,
                run_id,
                ev,
                log_text,
                log_available,
                log_unavailable_reason,
                auto_fix_enabled,
                job_name,
                branch,
                failure_signature,
                infra_kind,
                test_ids,
                log_token,
                &m,
                pr,
                host,
                webhook_cfg,
                spend,
            )
            .await
        }
        _ => {
            route_new_pr_only_failure(
                ctx,
                run_id,
                ev,
                log_text,
                log_available,
                log_unavailable_reason,
                job_name,
                branch,
                infra_kind,
                test_ids,
                log_token,
                pr,
                host,
                webhook_cfg,
                spend,
            )
            .await
        }
    }
}

/// `ev`'s failing tests overlap an `open` `ci_failures` row (that
/// row may or may not itself be on a PR). Attach the run, comment the PR
/// naming the issue, and run no analysis: the failure is already filed.
#[allow(clippy::too_many_arguments)]
async fn route_pr_matches_open_failure(
    ctx: &MintContext,
    run_id: Option<i64>,
    ev: &VerifiedFailureEvent,
    branch: &str,
    test_ids: &[String],
    m: &ci_failure_store::FailureMatch,
    pr: &ForgePullRequest,
    host: &str,
    webhook_cfg: &WebhookConfig,
) -> WebhookRunSummary {
    let Some(key) = m.issue_key.clone() else {
        error!(repo = %ev.repo, run = ev.index_in_repo, failure_id = m.id, "open ci_failures row has no issue_key");
        return WebhookRunSummary::error("ci-failure store row has no issue_key".to_string());
    };
    let now = state::now_iso();
    if let Err(e) = ci_failure_store::add_tests(&ctx.pool, m.id, test_ids).await {
        error!(repo = %ev.repo, run = ev.index_in_repo, error = %e, "recording failing tests failed");
        return WebhookRunSummary::error(format!("ci-failure store error: {e}"));
    }
    if let Err(e) = ci_failure_store::attach_run(
        &ctx.pool,
        m.id,
        ev.index_in_repo,
        &ev.commit_sha,
        branch,
        Some(pr.number as i64),
        &now,
    )
    .await
    {
        error!(repo = %ev.repo, run = ev.index_in_repo, error = %e, "recording run attachment failed");
        return WebhookRunSummary::error(format!("ci-failure store error: {e}"));
    }

    let env = match require_pr_comment_backend(ctx, run_id, ev, host, webhook_cfg).await {
        Ok(env) => env,
        Err(summary) => return summary,
    };
    let body = format!("This failure is already tracked in {key}; it is not caused by this PR.");
    let pr_number = pr.number;
    let repo = ev.repo.clone();
    let host_owned = host.to_string();
    let body_file = pr_comment_body_path(&ctx.paths, &ev.repo, pr_number);
    let result = tokio::task::spawn_blocking(move || {
        write_and_post_pr_comment(
            env.backend.as_ref(),
            &host_owned,
            &repo,
            pr_number,
            &body_file,
            &body,
        )
    })
    .await;
    match result {
        Ok(Ok(())) => {}
        Ok(Err(e)) => {
            warn!(repo = %ev.repo, run = ev.index_in_repo, %key, error = %e, "commenting on the human PR failed (continuing)");
        }
        Err(e) => {
            warn!(repo = %ev.repo, run = ev.index_in_repo, %key, error = %e, "PR comment task panicked (continuing)");
        }
    }
    WebhookRunSummary::store_decided(&StoreOutcome::Attached { key })
}

/// `ev`'s failing tests overlap a `pr_only` row: a prior sighting
/// on some open PR (possibly this same one). If the attach brings the row's
/// distinct branches to 2 or more, the failure belongs to `main`: mint a real
/// issue and comment its link onto every PR it was sighted on. Otherwise this
/// is another sighting of the same still-single-branch failure: analyze again
/// and comment the PR, same as a fresh sighting.
#[allow(clippy::too_many_arguments)]
async fn route_pr_matches_pr_only_failure(
    ctx: &MintContext,
    run_id: Option<i64>,
    ev: &VerifiedFailureEvent,
    log_text: String,
    log_available: bool,
    log_unavailable_reason: Option<String>,
    auto_fix_enabled: bool,
    job_name: &str,
    branch: &str,
    failure_signature: Option<&str>,
    infra_kind: Option<&str>,
    test_ids: &[String],
    log_token: Option<String>,
    m: &ci_failure_store::FailureMatch,
    pr: &ForgePullRequest,
    host: &str,
    webhook_cfg: &WebhookConfig,
    spend: &crate::usage::RunSpend,
) -> WebhookRunSummary {
    let now = state::now_iso();
    if let Err(e) = ci_failure_store::add_tests(&ctx.pool, m.id, test_ids).await {
        error!(repo = %ev.repo, run = ev.index_in_repo, error = %e, "recording failing tests failed");
        return WebhookRunSummary::error(format!("ci-failure store error: {e}"));
    }
    if let Err(e) = ci_failure_store::attach_run(
        &ctx.pool,
        m.id,
        ev.index_in_repo,
        &ev.commit_sha,
        branch,
        Some(pr.number as i64),
        &now,
    )
    .await
    {
        error!(repo = %ev.repo, run = ev.index_in_repo, error = %e, "recording run attachment failed");
        return WebhookRunSummary::error(format!("ci-failure store error: {e}"));
    }

    let branches = match ci_failure_store::distinct_branches(&ctx.pool, m.id).await {
        Ok(b) => b,
        Err(e) => {
            error!(repo = %ev.repo, run = ev.index_in_repo, error = %e, "ci-failure store read failed");
            return WebhookRunSummary::error(format!("ci-failure store error: {e}"));
        }
    };

    if branches.len() >= 2 {
        return mint_for_two_branch_pr_only_failure(
            ctx,
            ev,
            log_text,
            log_available,
            log_unavailable_reason,
            auto_fix_enabled,
            job_name,
            branch,
            failure_signature,
            infra_kind,
            log_token,
            m.id,
            host,
            webhook_cfg,
            spend,
        )
        .await;
    }

    let env = match require_pr_comment_backend(ctx, run_id, ev, host, webhook_cfg).await {
        Ok(env) => env,
        Err(summary) => return summary,
    };
    match analyze_and_comment_pr(
        ctx,
        ev,
        log_text,
        log_available,
        log_unavailable_reason,
        log_token,
        pr.number,
        host,
        env,
        spend,
    )
    .await
    {
        Ok(_) => WebhookRunSummary {
            text: format!(
                "commented analysis on PR #{} (pr_only, still one branch)",
                pr.number
            ),
            ok: true,
        },
        Err(e) => {
            warn!(repo = %ev.repo, run = ev.index_in_repo, error = %e, "PR analysis/comment failed");
            WebhookRunSummary::error(e.to_string())
        }
    }
}

/// A `pr_only` failure's tests just appeared on a second distinct
/// branch: promote it to a real issue (triage + mint, caps applied and
/// advanced exactly like a fresh mint), point the existing store row at it
/// instead of inserting a duplicate, and comment the issue link onto every
/// open PR the failure was sighted on.
#[allow(clippy::too_many_arguments)]
async fn mint_for_two_branch_pr_only_failure(
    ctx: &MintContext,
    ev: &VerifiedFailureEvent,
    log_text: String,
    log_available: bool,
    log_unavailable_reason: Option<String>,
    auto_fix_enabled: bool,
    job_name: &str,
    branch: &str,
    failure_signature: Option<&str>,
    infra_kind: Option<&str>,
    log_token: Option<String>,
    failure_id: i64,
    host: &str,
    webhook_cfg: &WebhookConfig,
    spend: &crate::usage::RunSpend,
) -> WebhookRunSummary {
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
        max_runs_per_day: webhook_cfg.max_runs_per_day,
    };

    let backend = ctx.backend.clone();
    let classifier = ctx.classifier.clone();
    let audit = crate::provider::IssueWriteAudit::new(None, ctx.defaults.max_issues_per_run);
    let mint_cfg = ctx.mint_cfg.read().await.clone();
    let route_cache = ctx.route_cache.clone();
    let event = ev.clone();
    let job_name_owned = job_name.to_string();
    let branch_owned = branch.to_string();
    let sig_owned = failure_signature.map(str::to_string);
    let infra_owned = infra_kind.map(str::to_string);
    let spend = spend.clone();

    let decision = tokio::task::spawn_blocking(
        move || -> crate::error::Result<(triage::TriageBucket, MintOutcome)> {
            let tracker = crate::provider::AuditedTracker::new(backend.as_ref(), audit);
            let input = TriageInput {
                repo: event.repo.clone(),
                workflow_id: event.workflow_id.clone(),
                commit_sha: event.commit_sha.clone(),
                html_url: event.html_url.clone(),
                log: log_text,
                log_available,
                log_unavailable_reason,
                has_open_pr: true,
                log_token,
            };
            let verdict = triage::triage(&input, classifier.as_ref(), &mint_cfg.auto_fix, &spend);
            let req = MintRequest {
                repo: &event.repo,
                index_in_repo: event.index_in_repo,
                commit_sha: &event.commit_sha,
                workflow_id: &event.workflow_id,
                branch: &branch_owned,
                job_name: &job_name_owned,
                failure_signature: sig_owned.as_deref(),
                infra_kind: infra_owned.as_deref(),
                html_url: &event.html_url,
                trigger_user: &event.trigger_user,
                triage: &verdict,
                log_path: None,
                private: event.private,
                has_open_pr: true,
                auto_fix_enabled,
            };
            let outcome = mint::mint(&tracker, &mint_cfg, route_cache.as_ref(), &req, &caps)?;
            Ok((verdict.bucket, outcome))
        },
    )
    .await;

    let (bucket, outcome) = match decision {
        Ok(Ok(v)) => v,
        Ok(Err(e)) => {
            warn!(repo = %ev.repo, run = ev.index_in_repo, error = %e, "two-branch mint failed");
            return WebhookRunSummary::error(e.to_string());
        }
        Err(e) => {
            warn!(repo = %ev.repo, run = ev.index_in_repo, error = %e, "two-branch mint task panicked");
            return WebhookRunSummary::error(format!("task panicked: {e}"));
        }
    };

    let key = match &outcome {
        MintOutcome::Created { key, .. } => key.clone(),
        other => {
            log_mint_outcome(ev, other);
            return WebhookRunSummary::decided(bucket, other);
        }
    };

    if let Err(e) = state::attempts_increment(&ctx.pool, WEBHOOK_MINT_ACTION, &job_key).await {
        warn!(error = %e, "advancing webhook attempt counter failed");
    }
    if let Err(e) = state::daily_count_increment(&ctx.pool, WEBHOOK_MINT_ACTION).await {
        warn!(error = %e, "advancing webhook daily ledger failed");
    }

    let now = state::now_iso();
    if let Err(e) = ci_failure_store::set_issue(&ctx.pool, failure_id, Some(&key), &now).await {
        error!(repo = %ev.repo, run = ev.index_in_repo, error = %e, "ci-failure store write failed");
        return WebhookRunSummary::error(format!("ci-failure store error: {e}"));
    }
    if let Err(e) = ci_failure_store::set_failure_state(&ctx.pool, failure_id, "open", &now).await {
        error!(repo = %ev.repo, run = ev.index_in_repo, error = %e, "ci-failure store write failed");
        return WebhookRunSummary::error(format!("ci-failure store error: {e}"));
    }

    let pr_numbers = ci_failure_store::pr_numbers_for_failure(&ctx.pool, failure_id)
        .await
        .unwrap_or_default();
    if !pr_numbers.is_empty() {
        match require_pr_comment_backend(ctx, None, ev, host, webhook_cfg).await {
            Ok(env) => {
                let body =
                    format!("Filed as {key}; the same failure was seen on another branch's PR.");
                let repo = ev.repo.clone();
                let host_owned = host.to_string();
                let backend = env.backend;
                let paths = ctx.paths.clone();
                let result = tokio::task::spawn_blocking(move || -> crate::error::Result<()> {
                    for pr_number in pr_numbers {
                        let body_file = pr_comment_body_path(&paths, &repo, pr_number as u64);
                        write_and_post_pr_comment(
                            backend.as_ref(),
                            &host_owned,
                            &repo,
                            pr_number as u64,
                            &body_file,
                            &body,
                        )?;
                    }
                    Ok(())
                })
                .await;
                match result {
                    Ok(Ok(())) => {}
                    Ok(Err(e)) => {
                        warn!(repo = %ev.repo, run = ev.index_in_repo, %key, error = %e, "commenting the issue link onto a PR failed (continuing)");
                    }
                    Err(e) => {
                        warn!(repo = %ev.repo, run = ev.index_in_repo, %key, error = %e, "commenting the issue link onto every PR task panicked (continuing)");
                    }
                }
            }
            Err(_) => {
                // Already logged and recorded by require_pr_comment_backend; the
                // issue itself is already filed, so this is best-effort only.
            }
        }
    }

    log_mint_outcome(ev, &outcome);
    WebhookRunSummary::decided(bucket, &outcome)
}

/// `ev`'s failing tests matched no store row at all, on an open PR
/// that is not the agent's own: the first sighting of this failure. Run the
/// analysis, comment it on the PR (always), and record a `pr_only` row - no
/// issue yet.
#[allow(clippy::too_many_arguments)]
async fn route_new_pr_only_failure(
    ctx: &MintContext,
    run_id: Option<i64>,
    ev: &VerifiedFailureEvent,
    log_text: String,
    log_available: bool,
    log_unavailable_reason: Option<String>,
    job_name: &str,
    branch: &str,
    infra_kind: Option<&str>,
    test_ids: &[String],
    log_token: Option<String>,
    pr: &ForgePullRequest,
    host: &str,
    webhook_cfg: &WebhookConfig,
    spend: &crate::usage::RunSpend,
) -> WebhookRunSummary {
    let env = match require_pr_comment_backend(ctx, run_id, ev, host, webhook_cfg).await {
        Ok(env) => env,
        Err(summary) => return summary,
    };
    if let Err(e) = analyze_and_comment_pr(
        ctx,
        ev,
        log_text,
        log_available,
        log_unavailable_reason,
        log_token,
        pr.number,
        host,
        env,
        spend,
    )
    .await
    {
        warn!(repo = %ev.repo, run = ev.index_in_repo, error = %e, "PR analysis/comment failed");
        return WebhookRunSummary::error(e.to_string());
    }

    let now = state::now_iso();
    let id = match ci_failure_store::record_failure(
        &ctx.pool,
        &ev.repo,
        &ev.workflow_id,
        job_name,
        infra_kind,
        None,
        "pr_only",
        &now,
    )
    .await
    {
        Ok(id) => id,
        Err(e) => {
            error!(repo = %ev.repo, run = ev.index_in_repo, error = %e, "ci-failure store write failed");
            return WebhookRunSummary::error(format!("ci-failure store error: {e}"));
        }
    };
    if let Err(e) = ci_failure_store::add_tests(&ctx.pool, id, test_ids).await {
        error!(repo = %ev.repo, run = ev.index_in_repo, error = %e, "recording failing tests failed");
        return WebhookRunSummary::error(format!("ci-failure store error: {e}"));
    }
    if let Err(e) = ci_failure_store::attach_run(
        &ctx.pool,
        id,
        ev.index_in_repo,
        &ev.commit_sha,
        branch,
        Some(pr.number as i64),
        &now,
    )
    .await
    {
        error!(repo = %ev.repo, run = ev.index_in_repo, error = %e, "recording run attachment failed");
        return WebhookRunSummary::error(format!("ci-failure store error: {e}"));
    }

    WebhookRunSummary {
        text: format!(
            "commented analysis on PR #{}; no issue (pr_only, first sighting)",
            pr.number
        ),
        ok: true,
    }
}

/// The classify + mint path for a failure the `ci_failure` store
/// does not yet track (`none`) or that is on an open PR (`pr_only`, treated as
/// `none` here; the PR-routing issue owns that case). One legacy adoption
/// lookup runs first (an issue minted before the store existed); on a miss,
/// this is the pre-existing triage + mint pipeline, and a `Created` outcome
/// (or a legacy hit) is recorded into the store so the next sighting matches.
#[allow(clippy::too_many_arguments)]
async fn classify_and_mint(
    ctx: &MintContext,
    run_id: Option<i64>,
    ev: &VerifiedFailureEvent,
    log_text: String,
    log_available: bool,
    log_artifact: Option<PathBuf>,
    log_unavailable_reason: Option<String>,
    auto_fix_enabled: bool,
    job_name: String,
    branch: String,
    failure_signature: Option<String>,
    infra_kind: Option<String>,
    test_ids: Vec<String>,
    log_token: Option<String>,
    max_runs_per_day: u32,
    spend: &crate::usage::RunSpend,
) -> WebhookRunSummary {
    // Caps, reused (not re-derived): the per-key counter is keyed by the
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
        max_runs_per_day,
    };

    // Surface what the blocking pipeline is doing. The blocking
    // thread cannot await the pool, so stage labels travel over an unbounded
    // channel to a drainer task that performs the `runs.stage` UPDATEs. The
    // sender drops with the closure when the blocking task ends, closing the
    // channel; the drainer is awaited below so no stale stage write can land
    // after `finish_webhook_run` clears the column.
    let (stage_tx, mut stage_rx) = mpsc::unbounded_channel::<&'static str>();
    let stage_drainer = {
        let pool = ctx.pool.clone();
        tokio::spawn(async move {
            while let Some(stage) = stage_rx.recv().await {
                let Some(id) = run_id else { continue };
                if let Err(e) = state::run_stage_set(&pool, id, Some(stage)).await {
                    warn!(error = %e, stage, "updating webhook run stage failed");
                }
            }
        })
    };

    let backend = ctx.backend.clone();
    let forge = forge_for_event(ctx, ev);
    let classifier = ctx.classifier.clone();
    // This delivery's own issue-write ledger. The backend is
    // daemon-lifetime and shared by every delivery, so the ledger is built per
    // run here rather than held on the backend: that is what keeps the ceiling
    // and the `run_id` on each write line belonging to THIS run.
    let audit = crate::provider::IssueWriteAudit::new(run_id, ctx.defaults.max_issues_per_run);
    // Snapshot the live mint config for this event; the next event
    // re-reads, so a Settings save in between is honored without a restart.
    let mint_cfg = ctx.mint_cfg.read().await.clone();
    let route_cache = ctx.route_cache.clone();
    let event = ev.clone();
    let branch_for_blocking = branch.clone();
    let job_name_for_blocking = job_name.clone();
    let sig_for_blocking = failure_signature.clone();
    let infra_for_blocking = infra_kind.clone();
    let spend = spend.clone();
    let decision = tokio::task::spawn_blocking(move || {
        let report_stage = move |stage: &'static str| {
            let _ = stage_tx.send(stage);
        };
        report_stage(STAGE_TRIAGING);
        let has_open_pr = resolve_has_open_pr(forge.as_ref(), &event);
        let tracker = crate::provider::AuditedTracker::new(backend.as_ref(), audit);
        decide_with_legacy_adoption(
            classifier.as_ref(),
            &tracker,
            &mint_cfg,
            route_cache.as_ref(),
            &caps,
            &event,
            log_text,
            log_available,
            log_artifact.as_deref(),
            log_unavailable_reason,
            has_open_pr,
            auto_fix_enabled,
            &job_name_for_blocking,
            &branch_for_blocking,
            sig_for_blocking.as_deref(),
            infra_for_blocking.as_deref(),
            log_token,
            &report_stage,
            &spend,
        )
    })
    .await;
    let _ = stage_drainer.await;

    let decision = match decision {
        Ok(Ok(d)) => d,
        Ok(Err(e)) => {
            warn!(repo = %ev.repo, run = ev.index_in_repo, error = %e, "triage/mint failed");
            return WebhookRunSummary::error(e.to_string());
        }
        Err(e) => {
            warn!(repo = %ev.repo, run = ev.index_in_repo, error = %e, "triage/mint task panicked");
            return WebhookRunSummary::error(format!("task panicked: {e}"));
        }
    };

    match decision {
        DecideOutcome::LegacyAdopted { key } => {
            if let Err(e) = record_new_store_row(
                &ctx.pool,
                ev,
                &job_name,
                infra_kind.as_deref(),
                &key,
                &branch,
                &test_ids,
            )
            .await
            {
                error!(repo = %ev.repo, run = ev.index_in_repo, error = %e, "ci-failure store write failed");
                return WebhookRunSummary::error(format!("ci-failure store error: {e}"));
            }
            WebhookRunSummary::store_decided(&StoreOutcome::Adopted { key })
        }
        DecideOutcome::Mint(bucket, outcome) => {
            // Advance the caps ledger only on a real mint, so repeated failures
            // for the same key (and a busy day) throttle exactly like the
            // scheduled path. Skips (transient, cap-hit, loop, open-PR) create
            // no issue and must not count.
            if matches!(outcome, MintOutcome::Created { .. }) {
                if let Err(e) =
                    state::attempts_increment(&ctx.pool, WEBHOOK_MINT_ACTION, &job_key).await
                {
                    warn!(error = %e, "advancing webhook attempt counter failed");
                }
                if let Err(e) = state::daily_count_increment(&ctx.pool, WEBHOOK_MINT_ACTION).await {
                    warn!(error = %e, "advancing webhook daily ledger failed");
                }
            }
            if let MintOutcome::Created { key, .. } = &outcome {
                if let Err(e) = record_new_store_row(
                    &ctx.pool,
                    ev,
                    &job_name,
                    infra_kind.as_deref(),
                    key,
                    &branch,
                    &test_ids,
                )
                .await
                {
                    error!(repo = %ev.repo, run = ev.index_in_repo, error = %e, "ci-failure store write failed");
                    return WebhookRunSummary::error(format!("ci-failure store error: {e}"));
                }
            }
            log_mint_outcome(ev, &outcome);
            WebhookRunSummary::decided(bucket, &outcome)
        }
    }
}

/// One legacy adoption lookup, then the ordinary triage + mint
/// pipeline. Factored out from [`classify_and_mint`] so the blocking work (the
/// legacy search, triage, and mint all shell out) stays inside one
/// `spawn_blocking` task. Returns [`DecideOutcome::LegacyAdopted`] on a legacy
/// hit (no triage, no mint) or [`DecideOutcome::Mint`] otherwise.
#[allow(clippy::too_many_arguments)]
fn decide_with_legacy_adoption(
    classifier: &dyn FailureClassifier,
    tracker: &dyn crate::provider::IssueTracker,
    mint_cfg: &MintConfig,
    route_cache: &VcsRouteCache,
    caps: &MintCaps,
    ev: &VerifiedFailureEvent,
    log_text: String,
    log_available: bool,
    log_artifact: Option<&std::path::Path>,
    log_unavailable_reason: Option<String>,
    has_open_pr: bool,
    auto_fix_enabled: bool,
    job_name: &str,
    branch: &str,
    failure_signature: Option<&str>,
    infra_kind: Option<&str>,
    log_token: Option<String>,
    report_stage: &(dyn Fn(&'static str) + Send + Sync),
    spend: &crate::usage::RunSpend,
) -> crate::error::Result<DecideOutcome> {
    // Before spending a `claude` classification, check whether this
    // exact collapse key already has an issue minted before the `ci_failure`
    // store existed (the caller already established this is a store miss).
    let collapse = mint::legacy_collapse_key(
        &ev.repo,
        branch,
        &ev.workflow_id,
        job_name,
        failure_signature,
        infra_kind,
    );
    if let Some(existing) = mint::find_first(tracker, &format!("{collapse} #Unresolved"))? {
        return Ok(DecideOutcome::LegacyAdopted { key: existing });
    }
    let (bucket, outcome) = decide_mint(
        classifier,
        tracker,
        mint_cfg,
        route_cache,
        caps,
        ev,
        log_text,
        log_available,
        log_artifact,
        log_unavailable_reason,
        has_open_pr,
        auto_fix_enabled,
        job_name,
        log_token,
        report_stage,
        spend,
    )?;
    Ok(DecideOutcome::Mint(bucket, outcome))
}

/// Pure triage + mint composition over the two traits, with `has_open_pr`
/// already resolved. Factored out so the bucket routing is unit tested against
/// a stub classifier / tracker with no forge, DB, or process spawn. Builds the
/// [`TriageInput`], runs the hybrid [`triage::triage`], then [`mint::mint`] on
/// the result. Dedupe by failing-test overlap is decided by the
/// caller against the `ci_failure` store before this is ever reached, so this
/// function itself issues no dedupe search.
#[allow(clippy::too_many_arguments)]
fn decide_mint(
    classifier: &dyn FailureClassifier,
    tracker: &dyn crate::provider::IssueTracker,
    mint_cfg: &MintConfig,
    route_cache: &VcsRouteCache,
    caps: &MintCaps,
    ev: &VerifiedFailureEvent,
    log_text: String,
    log_available: bool,
    log_artifact: Option<&std::path::Path>,
    log_unavailable_reason: Option<String>,
    has_open_pr: bool,
    auto_fix_enabled: bool,
    job_name: &str,
    log_token: Option<String>,
    report_stage: &(dyn Fn(&'static str) + Send + Sync),
    spend: &crate::usage::RunSpend,
) -> crate::error::Result<(triage::TriageBucket, MintOutcome)> {
    // Derive the deterministic failure signature and the branch from
    // the redacted log before it is moved into the triage input. The branch's
    // run-detail source is `None` here: the deployed Forgejo returns
    // `head_branch: null`, so the log's environment dump is the reliable source
    // (`resolve_branch` keeps the run-detail arm for instances that populate it).
    let failure_signature = crate::mint::failure_signature(&log_text);
    let branch = crate::forge_log::resolve_branch(None, &log_text).unwrap_or_default();
    // The infrastructure detector reads the SAME full redacted log,
    // not the bounded copy the classifier prompt gets, because a capacity abort
    // is post-steps and is exactly what the bound cuts.
    let infra = crate::infra_failure::detect(&log_text);
    if let Some(f) = &infra {
        info!(
            repo = %ev.repo,
            run = ev.index_in_repo,
            kind = %f.kind,
            problem = %f.problem,
            "infrastructure failure detected; collapsing on the branch-agnostic dedupe key"
        );
    }
    let input = TriageInput {
        repo: ev.repo.clone(),
        workflow_id: ev.workflow_id.clone(),
        commit_sha: ev.commit_sha.clone(),
        html_url: ev.html_url.clone(),
        log: log_text,
        log_available,
        log_unavailable_reason,
        has_open_pr,
        log_token,
    };
    // The classify stage only exists when the deterministic rules
    // abstain, so the label is reported from inside the classifier call rather
    // than unconditionally - a rules short-circuit never shows it.
    let reporting = ReportingClassifier {
        inner: classifier,
        report: report_stage,
    };
    let verdict = triage::triage(&input, &reporting, &mint_cfg.auto_fix, spend);
    report_stage(STAGE_MINTING);
    let req = MintRequest {
        repo: &ev.repo,
        index_in_repo: ev.index_in_repo,
        commit_sha: &ev.commit_sha,
        workflow_id: &ev.workflow_id,
        branch: &branch,
        job_name,
        failure_signature: failure_signature.as_deref(),
        infra_kind: infra.as_ref().map(|f| f.kind.as_str()),
        html_url: &ev.html_url,
        trigger_user: &ev.trigger_user,
        triage: &verdict,
        log_path: log_artifact,
        private: ev.private,
        has_open_pr,
        auto_fix_enabled,
    };
    let outcome = mint::mint(tracker, mint_cfg, route_cache, &req, caps)?;
    Ok((verdict.bucket, outcome))
}

/// Classifier decorator that reports the "classifying with claude"
/// stage right before delegating, so the label appears exactly when (and only
/// when) the LLM pass actually runs - the deterministic rules short-circuit
/// never consults the classifier and therefore never shows it.
struct ReportingClassifier<'a> {
    inner: &'a dyn FailureClassifier,
    report: &'a (dyn Fn(&'static str) + Send + Sync),
}

impl FailureClassifier for ReportingClassifier<'_> {
    fn classify(
        &self,
        input: &TriageInput,
        spend: &crate::usage::RunSpend,
    ) -> crate::error::Result<crate::triage::Triage> {
        (self.report)(STAGE_CLASSIFYING);
        self.inner.classify(input, spend)
    }
}

/// Best-effort `has_open_pr` signal for the triage classifier: list the repo's
/// open PRs and check whether the failing commit is the head of one. A forge
/// error or an underivable host degrades to `false` (treated as "no PR"), never
/// an error - the signal is advisory context for bucket 1 vs 2.
fn resolve_has_open_pr(forge: &dyn Forge, ev: &VerifiedFailureEvent) -> bool {
    let Some(host) = forge_host_from_url(&ev.html_url) else {
        warn!(repo = %ev.repo, html_url = %ev.html_url, "could not derive forge host for has_open_pr; assuming none");
        return false;
    };
    match forge.list_open_prs(&host, &ev.repo) {
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
            source,
        } => {
            info!(repo = %ev.repo, run = ev.index_in_repo, %key, %project, bucket = %bucket, routing = %source, "minted YouTrack issue from verified failure")
        }
        MintOutcome::SkippedTransient => {
            info!(repo = %ev.repo, run = ev.index_in_repo, "transient failure; minted nothing")
        }
        MintOutcome::SkippedAttemptCap => {
            warn!(repo = %ev.repo, run = ev.index_in_repo, "per-key attempt cap reached; minted nothing")
        }
        MintOutcome::SkippedDailyCap => {
            warn!(repo = %ev.repo, run = ev.index_in_repo, "daily run cap reached; minted nothing")
        }
    }
}

async fn forgejo_webhook(
    State(st): State<WebhookState>,
    Path((provider, source)): Path<(String, String)>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let now = Instant::now();

    // Resolve the provider segment first: unrecognized -> 404, same treatment
    // as an unconfigured source, before anything reads the config.
    let Some(provider) = WebhookProvider::parse(&provider) else {
        warn!(provider = %provider, source = %source, "webhook rejected: unrecognized provider");
        return StatusCode::NOT_FOUND.into_response();
    };

    // Resolve the source from the live config. An unknown source, or one whose
    // own configured provider does not match the URL's `:provider` segment,
    // 404s (it is not a configured endpoint for this provider), logged so a
    // misdirected webhook is diagnosable. Clone the source out so no read
    // guard is held across the secret-decrypt / channel-send awaits below.
    let src_cfg = {
        let cfg = st.config.read().await;
        if !cfg.enabled {
            warn!(source = %source, "webhook rejected: disabled");
            return StatusCode::NOT_FOUND.into_response();
        }
        match cfg.sources.get(&source) {
            Some(s) if s.provider == provider => s.clone(),
            Some(s) => {
                warn!(
                    source = %source,
                    provider = %provider,
                    configured_provider = %s.provider,
                    "webhook rejected: source is configured for a different provider"
                );
                return StatusCode::NOT_FOUND.into_response();
            }
            None => {
                warn!(source = %source, "webhook rejected: unknown source");
                return StatusCode::NOT_FOUND.into_response();
            }
        }
    };

    // Per-source rate limit first: cheapest check, shields the HMAC +
    // secret-decrypt work from a request flood, and one source's storm never
    // starves another's bucket. The limiter map is rebuilt in lock-step with the
    // sources map, so a resolved source always has a limiter; a
    // missing one is a bug.
    {
        let rate = st.rate.read().await;
        let Some(limiter) = rate.get(&source) else {
            warn!(source = %source, "webhook: no rate limiter for source (bug)");
            return StatusCode::INTERNAL_SERVER_ERROR.into_response();
        };
        if !limiter.lock().await.allow(now) {
            warn!(source = %source, "webhook rate limited");
            return StatusCode::TOO_MANY_REQUESTS.into_response();
        }
    }

    // Resolve this source's HMAC secret. Fail closed: an unset ref or an
    // unresolvable secret means we cannot authenticate, so reject rather than
    // accept. The secret is hard-bound to the source, so a payload signed for a
    // different source's secret cannot authenticate here.
    let secret = match resolve_secret(&st, &src_cfg).await {
        Ok(s) => s,
        Err(e) => {
            warn!(source = %source, error = %e, "webhook secret unavailable; rejecting");
            return StatusCode::UNAUTHORIZED.into_response();
        }
    };

    // Read the delivery id up front so the rejection logs below can name it. The
    // value is untrusted until the signature verifies: logging it before auth is
    // fine, acting on it is not, so it is only recorded in the replay store after
    // authentication succeeds.
    let delivery = headers
        .get(provider.delivery_header())
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    let delivery_label = if delivery.is_empty() { "-" } else { delivery };

    // Verify the signature over the RAW body before any parsing. Both rejection
    // branches log the reason (never the signature value, the computed MAC, or
    // the body) so a silent 401 is always explained by the journal.
    let signature = headers
        .get(provider.signature_header())
        .and_then(|v| v.to_str().ok());
    let Some(signature) = signature else {
        warn!(
            source = %source,
            delivery = delivery_label,
            header = provider.signature_header(),
            "webhook rejected: missing signature header (is the webhook Secret set on the forge side?)"
        );
        return StatusCode::UNAUTHORIZED.into_response();
    };
    if !provider.verify_signature(secret.as_bytes(), &body, signature) {
        warn!(
            source = %source,
            delivery = delivery_label,
            body_bytes = body.len(),
            "webhook rejected: signature mismatch (stored secret and the forge's webhook Secret differ?)"
        );
        return StatusCode::UNAUTHORIZED.into_response();
    }

    // Replay protection on authenticated requests only, against the shared store.
    // A missing delivery id is malformed; a previously-seen id inside the window
    // is a replay (including the same delivery fanned out to two sources).
    if delivery.is_empty() {
        warn!(source = %source, header = provider.delivery_header(), "webhook rejected: missing delivery header");
        return StatusCode::BAD_REQUEST.into_response();
    }
    if !st.seen.lock().await.check_and_record(delivery, now) {
        warn!(source = %source, delivery, "webhook replay rejected");
        return StatusCode::CONFLICT.into_response();
    }

    // Authenticated and de-duplicated: trace the accepted delivery (id + body
    // length only) before parsing so the happy path is followable at debug.
    debug!(
        source = %source,
        delivery,
        body_bytes = body.len(),
        "webhook authenticated"
    );

    if paused_accept_and_drop(&st.pool, WEBHOOK_MINT_ACTION, &source, delivery).await {
        return StatusCode::OK.into_response();
    }
    if login_required_accept_and_drop(&st, &source, delivery).await {
        return StatusCode::OK.into_response();
    }

    // Decode a GitHub form-urlencoded delivery to the JSON bytes it wraps
    // (Forgejo and a GitHub JSON delivery pass through unchanged). The
    // signature above was verified over the raw `body`; everything from here
    // on parses `payload_body` instead.
    let content_type = headers
        .get(axum::http::header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok());
    let payload_body = match github_json_payload(provider, content_type, &body) {
        Ok(b) => b,
        Err(e) => {
            warn!(
                source = %source,
                delivery,
                error = %e,
                content_type = content_type.unwrap_or("-"),
                body_bytes = body.len(),
                "webhook payload form-decode failed"
            );
            return StatusCode::BAD_REQUEST.into_response();
        }
    };

    // Dispatch on the event-type header before parsing. A pull-request
    // delivery (same header value and payload shape on both providers) takes
    // its own path; everything else is parsed per-provider below into one
    // shared `ActionPayload` shape, so nothing past this point is
    // provider-specific.
    let event_type = headers
        .get(provider.event_header())
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
        .to_string();
    if event_type == PULL_REQUEST_EVENT_TYPE {
        return handle_pull_request_delivery(
            &st,
            &src_cfg,
            &source,
            delivery,
            content_type,
            &payload_body,
        )
        .await;
    }

    // Parse only after the request is authenticated and de-duplicated.
    let payload: ActionPayload = match provider {
        WebhookProvider::Forgejo => {
            if !event_type.is_empty() && !event_type.starts_with(ACTION_RUN_EVENT_PREFIX) {
                info!(
                    source = %source,
                    delivery,
                    event = %event_type,
                    "webhook delivery of an unhandled event type acked and dropped"
                );
                return StatusCode::OK.into_response();
            }
            match serde_json::from_slice(&payload_body) {
                Ok(p) => p,
                Err(e) => {
                    warn!(source = %source, error = %e, "webhook payload parse failed");
                    return StatusCode::BAD_REQUEST.into_response();
                }
            }
        }
        WebhookProvider::Github => {
            if event_type != GITHUB_WORKFLOW_RUN_EVENT_TYPE {
                info!(
                    source = %source,
                    delivery,
                    event = %event_type,
                    "webhook delivery of an unhandled event type acked and dropped"
                );
                return StatusCode::OK.into_response();
            }
            let gh_payload: GithubWorkflowRunPayload = match serde_json::from_slice(&payload_body) {
                Ok(p) => p,
                Err(e) => {
                    warn!(
                        source = %source,
                        error = %e,
                        content_type = content_type.unwrap_or("-"),
                        body_bytes = payload_body.len(),
                        "webhook payload parse failed"
                    );
                    return StatusCode::BAD_REQUEST.into_response();
                }
            };
            match github_workflow_run_to_action_payload(&gh_payload) {
                Some(p) => p,
                None => {
                    info!(
                        source = %source,
                        delivery,
                        action = %gh_payload.action,
                        "webhook event dropped: non-terminal or unmappable workflow_run action"
                    );
                    return StatusCode::OK.into_response();
                }
            }
        }
    };

    // A `success` or `recover` action is routed to the automerge
    // success-event pipeline instead of `verified_failure_event`, which only
    // ever keeps `failure`.
    if payload.action == "success" || payload.action == "recover" {
        return handle_action_success(&st, &src_cfg, &source, delivery, &payload).await;
    }

    match verified_failure_event(&payload, &src_cfg.allowed_repos, &source) {
        Some(event) => {
            if st.events.send(event).await.is_err() {
                warn!(source = %source, "webhook event channel closed; dropping event");
            }
            StatusCode::ACCEPTED.into_response()
        }
        // Authenticated but not an allow-listed failure: ack + drop so the
        // forge marks the delivery delivered and does not retry. Log which of
        // the two drop conditions fired so a 200-ack drop is never silent (the
        // rest of the handler logs every other branch).
        None => {
            let reason = if payload.action != "failure" {
                "non-failure action"
            } else {
                "repo not in allowed_repos"
            };
            info!(
                source = %source,
                delivery,
                action = %payload.action,
                repo = %payload.run.repository.full_name,
                reason,
                "webhook event dropped"
            );
            StatusCode::OK.into_response()
        }
    }
}

/// Parse and route an authenticated, de-duplicated `success` or
/// `recover` Action-run delivery: an agent PR's checks may have just gone
/// green. Repo-allow-listed only; which PR (if any) this belongs to, and
/// whether automerge is even enabled for it, are resolved downstream in
/// `process_action_success_event` so a slow forge call never blocks the HTTP
/// response.
async fn handle_action_success(
    st: &WebhookState,
    src_cfg: &WebhookSource,
    source: &str,
    delivery: &str,
    payload: &ActionPayload,
) -> Response {
    let repo = &payload.run.repository.full_name;
    if !repo_allowed(&src_cfg.allowed_repos, repo) {
        info!(
            source = %source,
            delivery,
            action = %payload.action,
            repo = %repo,
            "webhook event dropped: repo not in allowed_repos"
        );
        return StatusCode::OK.into_response();
    }
    let event = VerifiedActionSuccessEvent {
        repo: repo.clone(),
        commit_sha: payload.run.commit_sha.clone(),
        html_url: payload.run.html_url.clone(),
        source: source.to_string(),
    };
    let Some(sender) = &st.success_events else {
        info!(
            source = %source,
            delivery,
            repo = %event.repo,
            "action-run success event accepted but no success_events channel is configured; dropping"
        );
        return StatusCode::OK.into_response();
    };
    if sender.send(event).await.is_err() {
        warn!(source = %source, delivery, "action-run success event channel closed; dropping event");
    }
    StatusCode::ACCEPTED.into_response()
}

/// Parse and route an authenticated, de-duplicated `pull_request`
/// delivery. Only a terminal `closed` action does anything; every other
/// action (`opened`, `edited`, `synchronized`, ...) and a repo outside the
/// source's allow-list is acked and dropped, exactly like the Action-run
/// path's own ack-drop branch.
async fn handle_pull_request_delivery(
    st: &WebhookState,
    src_cfg: &WebhookSource,
    source: &str,
    delivery: &str,
    content_type: Option<&str>,
    body: &Bytes,
) -> Response {
    let payload: PullRequestPayload = match serde_json::from_slice(body) {
        Ok(p) => p,
        Err(e) => {
            warn!(
                source = %source,
                delivery,
                error = %e,
                content_type = content_type.unwrap_or("-"),
                body_bytes = body.len(),
                "webhook pull_request payload parse failed"
            );
            return StatusCode::BAD_REQUEST.into_response();
        }
    };

    if payload.action != PR_ACTION_CLOSED {
        info!(
            source = %source,
            delivery,
            action = %payload.action,
            repo = %payload.repository.full_name,
            "pull_request event dropped: non-terminal action"
        );
        return StatusCode::OK.into_response();
    }
    if !repo_allowed(&src_cfg.allowed_repos, &payload.repository.full_name) {
        info!(
            source = %source,
            delivery,
            repo = %payload.repository.full_name,
            "pull_request event dropped: repo not in allowed_repos"
        );
        return StatusCode::OK.into_response();
    }

    let event = VerifiedPullRequestEvent {
        repo: payload.repository.full_name.clone(),
        number: payload.number,
        merged: payload.pull_request.merged,
        head_sha: payload.pull_request.head.sha.clone(),
        head_ref: payload.pull_request.head.git_ref.clone(),
        html_url: payload.pull_request.html_url.clone(),
        source: source.to_string(),
    };

    let Some(sender) = &st.pr_events else {
        warn!(
            source = %source,
            delivery,
            repo = %event.repo,
            number = event.number,
            "pull_request event accepted but no pr_events channel is configured; dropping"
        );
        return StatusCode::OK.into_response();
    };
    if sender.send(event).await.is_err() {
        warn!(source = %source, delivery, "pull-request event channel closed; dropping event");
    }
    StatusCode::ACCEPTED.into_response()
}

/// Drain verified `pull_request` deliveries for the lifetime of
/// the daemon, mirroring [`consume_events`]'s shape. `mint` carries the
/// backend + store access every close/left-open/mark-merged decision needs;
/// `None` (no usable provider backend) drops every event with a log, the same
/// degrade-to-log-only behaviour the Action-run path takes.
pub async fn consume_pr_events(
    mut rx: mpsc::Receiver<VerifiedPullRequestEvent>,
    pool: DbPool,
    mint: Option<MintContext>,
    keygate: crate::concurrency::KeyGate,
) {
    let mint = mint.map(Arc::new);
    let mut tasks = Vec::new();
    while let Some(ev) = rx.recv().await {
        let pool = pool.clone();
        let mint = mint.clone();
        let keygate = keygate.clone();
        tasks.push(tokio::spawn(async move {
            process_pr_event(ev, pool, mint, keygate).await;
        }));
        tasks.retain(|t| !t.is_finished());
    }
    for t in tasks {
        if let Err(e) = t.await {
            warn!(error = %e, "a pull-request webhook event task panicked");
        }
    }
}

/// One merged-or-closed PR's whole handling: mark its
/// `agent_pull_requests` row (merged / closed), and on a merge, close or leave
/// open every open store failure the PR references.
async fn process_pr_event(
    ev: VerifiedPullRequestEvent,
    pool: DbPool,
    mint: Option<Arc<MintContext>>,
    keygate: crate::concurrency::KeyGate,
) {
    // An event accepted before a pause was set must not act after it.
    if webhook_action_paused(&pool, WEBHOOK_MINT_ACTION, "pull-request event").await {
        return;
    }
    if mint.is_none() {
        info!(
            repo = %ev.repo,
            number = ev.number,
            "pull-request event dropped: no usable provider backend (mint disabled)"
        );
        return;
    }

    // One key per (repo, PR), so two deliveries of the same
    // PR (a retried delivery, or closed-then-reopened-then-merged in quick
    // succession) never race each other's store/issue writes.
    let key = format!("{}#{}", ev.repo, ev.number);
    let _key_guard = keygate.acquire(&key).await;

    let Some(host) = forge_host_from_url(&ev.html_url) else {
        warn!(repo = %ev.repo, number = ev.number, html_url = %ev.html_url, "could not derive forge host from pull_request html_url");
        return;
    };

    let job_key = format!("pr-{}-{}", ev.repo.replace('/', "_"), ev.number);
    let run = begin_webhook_run(&pool, WEBHOOK_MINT_ACTION, &job_key, &ev.html_url).await;

    let result = if ev.merged {
        handle_merged_pr(&pool, &host, &ev).await
    } else {
        handle_closed_unmerged_pr(&pool, &host, &ev).await
    };

    let (ok, summary) = match &result {
        Ok(s) => (true, s.clone()),
        Err(e) => {
            warn!(repo = %ev.repo, number = ev.number, error = %e, "pull-request event handling failed");
            (false, e.clone())
        }
    };
    finish_webhook_run(&pool, run, &job_key, &summary, ok, None, None).await;
}

/// A PR closed WITHOUT merging. The only action is marking the
/// agent's own PR record `closed` when one exists; a human's PR (no record)
/// changes nothing.
async fn handle_closed_unmerged_pr(
    pool: &DbPool,
    host: &str,
    ev: &VerifiedPullRequestEvent,
) -> Result<String, String> {
    let row = ci_failure_store::agent_pr(pool, host, &ev.repo, ev.number)
        .await
        .map_err(|e| format!("agent_pull_requests lookup failed: {e}"))?;
    let Some(row) = row else {
        return Ok(format!(
            "PR #{} closed without merging; no agent_pull_requests row",
            ev.number
        ));
    };
    let now = state::now_iso();
    ci_failure_store::set_agent_pr_state(pool, row.id, "closed", &now)
        .await
        .map_err(|e| format!("marking agent_pull_requests closed failed: {e}"))?;
    Ok(format!(
        "agent PR #{} closed without merging; marked closed",
        ev.number
    ))
}

/// A merged PR. Marks the agent's own PR record
/// `merged` when one exists; makes no tracker call and no forge run-list
/// call. A YouTrack workflow (`docs/yt-workflow-resolve-on-pr-merge.js`)
/// resolves whichever issue the merged PR claims, for any issue, not only one
/// the runner tracks, so this handler does not need to find or close it.
async fn handle_merged_pr(
    pool: &DbPool,
    host: &str,
    ev: &VerifiedPullRequestEvent,
) -> Result<String, String> {
    let now = state::now_iso();

    let agent_row = ci_failure_store::agent_pr(pool, host, &ev.repo, ev.number)
        .await
        .map_err(|e| format!("agent_pull_requests lookup failed: {e}"))?;
    let Some(row) = agent_row else {
        return Ok(format!(
            "PR #{} merged; no agent_pull_requests row",
            ev.number
        ));
    };
    ci_failure_store::set_agent_pr_state(pool, row.id, "merged", &now)
        .await
        .map_err(|e| format!("marking agent_pull_requests merged failed: {e}"))?;
    Ok(format!(
        "agent PR #{} merged; marked merged (issue {})",
        ev.number, row.issue_key
    ))
}

/// Drain verified `success`/`recover` Action-run deliveries for the
/// lifetime of the daemon, mirroring [`consume_pr_events`]'s shape. `mint`
/// carries the backend every routing decision needs; `None` (no usable
/// provider backend) drops every event with a log, the same degrade-to-log-only
/// behaviour the other two Action-run consumers take.
pub async fn consume_action_success_events(
    mut rx: mpsc::Receiver<VerifiedActionSuccessEvent>,
    pool: DbPool,
    mint: Option<MintContext>,
    keygate: crate::concurrency::KeyGate,
) {
    let mint = mint.map(Arc::new);
    let mut tasks = Vec::new();
    while let Some(ev) = rx.recv().await {
        let pool = pool.clone();
        let mint = mint.clone();
        let keygate = keygate.clone();
        tasks.push(tokio::spawn(async move {
            process_action_success_event(ev, pool, mint, keygate).await;
        }));
        tasks.retain(|t| !t.is_finished());
    }
    for t in tasks {
        if let Err(e) = t.await {
            warn!(error = %e, "an action-run success webhook event task panicked");
        }
    }
}

/// One success event's whole handling. Finds the open PR matching
/// the run's commit, looks it up in `agent_pull_requests`, and (only when it
/// is a recorded, still-open agent PR whose action has automerge enabled)
/// reads the PR's CI status and merges it, comments that it needs a branch
/// update, or does nothing while checks are still pending - `finalize` no
/// longer polls, so this event is the only thing that ever looks again.
async fn process_action_success_event(
    ev: VerifiedActionSuccessEvent,
    pool: DbPool,
    mint: Option<Arc<MintContext>>,
    keygate: crate::concurrency::KeyGate,
) {
    let Some(mint) = mint else {
        info!(
            repo = %ev.repo,
            commit = %ev.commit_sha,
            "action-run success event dropped: no usable provider backend (mint disabled)"
        );
        return;
    };

    // An event accepted before a pause was set must not act after it.
    if webhook_action_paused(&pool, WEBHOOK_MINT_ACTION, "action-run success event").await {
        return;
    }

    // Neither of the next two steps knows yet which (if any)
    // recorded agent PR this event concerns - that is only resolved once the
    // commit is matched to a PR below - so a failure here cannot itself name
    // an issue. Rather than dropping it as pure noise, `notify_agent_prs_of_repo_fault`
    // surfaces it on every OPEN agent PR already recorded for this repo with
    // automerge enabled (any of them could be the one that just went green);
    // a repo with none recorded stays silent.
    let Some(host) = forge_host_from_url(&ev.html_url) else {
        let msg = "could not derive forge host from action-run html_url";
        warn!(repo = %ev.repo, html_url = %ev.html_url, "{msg}");
        notify_agent_prs_of_repo_fault(&mint, &pool, &ev.repo, "action-run success", msg).await;
        return;
    };

    // No recorded agent PR (and so no action / `github.hosts`) is
    // known yet at this point - the PR is only found below, by matching the
    // commit against this very listing. `is_github_host(host, &[])` (i.e.
    // `host.eq_ignore_ascii_case("github.com")`) is a deliberate, narrower
    // default: it correctly routes the common `github.com` case, and a GHE
    // host configured only via an action's `github.hosts` is a known,
    // narrower gap for a follow-up rather than an oversight here.
    let forge = resolve_forge(
        &mint,
        &host,
        &[],
        &crate::action::default_github_mcp_endpoint(),
    );
    let host_for_list = host.clone();
    let repo_for_list = ev.repo.clone();
    let prs = match tokio::task::spawn_blocking(move || {
        forge.list_open_prs(&host_for_list, &repo_for_list)
    })
    .await
    {
        Ok(Ok(prs)) => prs,
        Ok(Err(e)) => {
            let msg = format!("list_open_prs failed: {e}");
            warn!(repo = %ev.repo, error = %e, "action-run success: {msg}");
            notify_agent_prs_of_repo_fault(&mint, &pool, &ev.repo, "action-run success", &msg)
                .await;
            return;
        }
        Err(e) => {
            let msg = format!("list_open_prs task panicked: {e}");
            warn!(repo = %ev.repo, error = %e, "action-run success: {msg}");
            notify_agent_prs_of_repo_fault(&mint, &pool, &ev.repo, "action-run success", &msg)
                .await;
            return;
        }
    };
    let Some(pr) = pr_matching_commit(&prs, &ev.commit_sha) else {
        info!(repo = %ev.repo, commit = %ev.commit_sha, "action-run success: no open PR matches the commit; dropping");
        return;
    };
    let pr_number = pr.number;
    let pr_url = pr.html_url.clone();

    // One key per (repo, PR), so two success deliveries for
    // the same PR never race each other's merge / comment.
    let key = format!("{}#{}", ev.repo, pr_number);
    let _key_guard = keygate.acquire(&key).await;

    let job_key = format!("automerge-{}-{}", ev.repo.replace('/', "_"), pr_number);
    let run = begin_webhook_run(&pool, WEBHOOK_MINT_ACTION, &job_key, &ev.html_url).await;

    let result =
        handle_action_success_for_pr(&mint, &pool, &host, &ev.repo, pr_number, &pr_url).await;

    let (ok, summary) = match &result {
        Ok(s) => (true, s.clone()),
        Err(e) => (false, e.clone()),
    };
    if !ok {
        warn!(repo = %ev.repo, pr = pr_number, error = %summary, "action-run success handling failed");
    }
    finish_webhook_run(&pool, run, &job_key, &summary, ok, None, None).await;
}

/// A fault that happened before the failing commit could be
/// matched to a specific PR (host derivation, `list_open_prs`) still concerns
/// this repo, and any currently-open, automerge-enabled agent PR recorded for
/// it could be the one that just went green and now silently will not
/// automerge. Rather than drop the fault as pure noise (nothing identifiable
/// to comment on) or guess at a single PR, this notifies every such row: an
/// `error` log and a `run_events` row unconditionally, plus a best-effort
/// issue comment per matching row. A repo with no open agent PR recorded logs
/// nothing beyond the caller's own warning, since there is nothing an
/// operator could act on.
async fn notify_agent_prs_of_repo_fault(
    mint: &MintContext,
    pool: &DbPool,
    repo: &str,
    context: &str,
    error_msg: &str,
) {
    let open_rows = match ci_failure_store::open_agent_prs(pool).await {
        Ok(rows) => rows,
        Err(e) => {
            error!(repo = %repo, error = %e, "{context}: {error_msg} (and listing open agent PRs to notify also failed: {e})");
            record_run_event(
                pool,
                None,
                WEBHOOK_MINT_ACTION,
                "error",
                format!("{context}: {error_msg} (listing open agent PRs to notify failed: {e})"),
            )
            .await;
            return;
        }
    };

    for row in open_rows.iter().filter(|r| r.repo == repo) {
        let actions_dir = mint.paths.actions_dir();
        let action_name = row.action_name.clone();
        let loaded =
            tokio::task::spawn_blocking(move || crate::action::load(&action_name, &actions_dir))
                .await;
        let automerge_enabled = match loaded {
            Ok(Ok(loaded)) => matches!(
                loaded.action.native,
                Some(crate::action::NativeConfig::Youtrack(cfg)) if cfg.youtrack.automerge.enabled
            ),
            Ok(Err(e)) => {
                warn!(repo = %repo, pr = row.pr_number, action = %row.action_name, error = %e, "{context}: could not load action to check automerge while notifying of repo fault");
                false
            }
            Err(e) => {
                warn!(repo = %repo, pr = row.pr_number, action = %row.action_name, error = %e, "{context}: action-load task panicked while notifying of repo fault");
                false
            }
        };
        if !automerge_enabled {
            continue;
        }

        let msg = format!("{context}: {error_msg}");
        error!(repo = %repo, pr = row.pr_number, issue = %row.issue_key, "{msg}");
        record_run_event(pool, None, WEBHOOK_MINT_ACTION, "error", msg.clone()).await;
        let pr_url = agent_pr_url(&row.host, repo, row.pr_number);
        let body = format!(
            "The runner could not check this PR's automerge status: {msg}.\n\nPR: {pr_url}\n\nA later Action Run event or the periodic reconciler will retry automatically."
        );
        post_issue_comment(
            mint,
            &row.issue_key,
            body,
            repo,
            row.pr_number as u64,
            "action-run success: repo-fault comment failed",
        )
        .await;
    }
}

/// Build a PR's web URL from its recorded host/repo/number, without an extra
/// forge call: every caller already has these three fields from the
/// `agent_pull_requests` row.
fn agent_pr_url(host: &str, repo: &str, pr_number: i64) -> String {
    format!("https://{host}/{repo}/pulls/{pr_number}")
}

/// The per-PR decision once a success event's PR is known. Looks
/// up the `agent_pull_requests` row and, once found and open, delegates the
/// rest of the decision to [`apply_automerge_decision`] - the same function
/// [`reconcile_automerge`] calls for a PR it already has the row
/// for, so the two entry points never diverge on the behind_base / mergeable /
/// `complete_automerge` logic, which merges the PR and comments; it makes no
/// `State` or `AI Agent` write, since the YouTrack workflow
/// resolves the issue once it observes the merge.
async fn handle_action_success_for_pr(
    mint: &MintContext,
    pool: &DbPool,
    host: &str,
    repo: &str,
    pr_number: u64,
    pr_url: &str,
) -> Result<String, String> {
    let row = match ci_failure_store::agent_pr(pool, host, repo, pr_number as i64).await {
        Ok(row) => row,
        Err(e) => {
            let msg = format!("agent_pull_requests lookup failed: {e}");
            error!(repo = %repo, pr = pr_number, error = %e, "action-run success: {msg}");
            record_run_event(pool, None, WEBHOOK_MINT_ACTION, "error", msg.clone()).await;
            return Err(msg);
        }
    };
    let Some(row) = row else {
        info!(repo = %repo, pr = pr_number, "action-run success: PR is not a recorded agent PR; no action");
        return Ok("not a recorded agent PR; no action".to_string());
    };
    if row.state != "open" {
        info!(repo = %repo, pr = pr_number, state = %row.state, "action-run success: recorded PR is no longer open; no action");
        return Ok(format!(
            "agent PR #{pr_number} already {}; no action",
            row.state
        ));
    }

    apply_automerge_decision(mint, pool, host, repo, pr_number, pr_url, &row)
        .await
        .map(AutomergeOutcome::into_summary)
}

/// Route a forge operation through [`GithubForge`] when `host` is
/// a GitHub host, mirroring the per-job dispatch at
/// `src/native/youtrack.rs:421-435`; every other host reuses `mint.backend`
/// (`fj`) unchanged. Token is always `None`: `GithubForge` falls back to the
/// daemon's stored `gh` login when constructed with no token,
/// matching the per-job path's own behavior when an action sets no
/// `github.token_ref`. Shared by the automerge webhook/reconciler and, since
/// CLAUDE-406, the CI-failure triage path's own open-PR lookups
/// ([`forge_for_event`]): every `list_open_prs` caller in this module needs
/// the same `github.com` vs `fj` routing, so this stays the one helper rather
/// than growing a second copy.
fn resolve_forge(
    mint: &MintContext,
    host: &str,
    hosts: &[String],
    mcp_endpoint: &str,
) -> Arc<dyn Forge> {
    if is_github_host(host, hosts) {
        Arc::new(GithubForge::new(
            mint.pr_comment_runner.clone(),
            mcp_endpoint.to_string(),
            None,
        ))
    } else {
        mint.backend.clone() as Arc<dyn Forge>
    }
}

/// Record this tick's automerge outcome (or fault) for `repo#pr_number` as a
/// `run_events` row, keyed with the same `PR {repo}#{pr_number}: ` prefix
/// [CLAUDE-333] used for the `pr_status` failure, so the previous tick's
/// outcome for this PR can be found regardless of which of the five sites
/// produced it (`pr_status` failure, behind_base missing, branch-update
/// failure, not-mergeable, merge failure), and post `body` to the issue only
/// when `outcome` differs from that previous message: an unchanged outcome
/// across reconciler ticks is recorded every pass but commented once, while
/// a changed outcome (a different error text, or a different outcome
/// entirely) posts again.
/// The PR and issue a [`record_automerge_outcome_and_comment_once`] call
/// concerns; bundled to keep that function's argument count down.
struct AutomergePr<'a> {
    repo: &'a str,
    pr_number: u64,
    issue_key: &'a str,
}

async fn record_automerge_outcome_and_comment_once(
    mint: &MintContext,
    pool: &DbPool,
    pr: AutomergePr<'_>,
    level: &str,
    outcome: String,
    body: String,
    comment_context: &str,
) {
    let prefix = format!("PR {}#{}: ", pr.repo, pr.pr_number);
    let msg = format!("{prefix}{outcome}");
    let previous = state::run_events_last_message_containing(pool, WEBHOOK_MINT_ACTION, &prefix)
        .await
        .unwrap_or_else(|read_err| {
            warn!(error = %read_err, "reading the previous automerge outcome failed; commenting anyway");
            None
        });
    record_run_event(pool, None, WEBHOOK_MINT_ACTION, level, msg.clone()).await;
    if previous.as_deref() != Some(msg.as_str()) {
        post_issue_comment(
            mint,
            pr.issue_key,
            body,
            pr.repo,
            pr.pr_number,
            comment_context,
        )
        .await;
    }
}

/// `hash(repo, pr_number, head_sha)`, forced negative so it never collides
/// with a real (positive) Forgejo comment id in the dedupe / attempt key
/// [`crate::web::comment_webhooks::command_key`] both routes share. Clearing
/// the digest's top bit before negating keeps the result in `i64`'s range
/// without risking the `i64::MIN` overflow a naive negation of an arbitrary
/// `u64` could hit.
fn synthetic_conflict_comment_id(repo: &str, pr_number: u64, head_sha: &str) -> i64 {
    let mut h = Sha256::new();
    h.update(b"automerge-conflict-resolve\0");
    h.update(repo.as_bytes());
    h.update(b"\0");
    h.update(pr_number.to_string().as_bytes());
    h.update(b"\0");
    h.update(head_sha.as_bytes());
    let digest = h.finalize();
    let mut bytes = [0u8; 8];
    bytes.copy_from_slice(&digest[..8]);
    let magnitude = (u64::from_be_bytes(bytes) & 0x7fff_ffff_ffff_ffff) as i64;
    -(1 + magnitude)
}

/// On a conflict `update_pr_branch` failure, dispatch the same
/// conflict-resolution run a human's `/claude merge from main and resolve any
/// merge conflicts` PR comment would start (CLAUDE-380), at most once per
/// `(repo, pr_number, head_sha)`. Returns `true` when a synthetic command was
/// just sent (the caller posts the "resolving" comment instead of "a human
/// needs to resolve this"); `false` for every case that keeps today's
/// behavior: a non-conflict error, an unknown `head_sha`, no sender wired, no
/// comment-webhook source covering `repo`, an attempt already made for this
/// head commit, or a failed send (logged at `error` with a `run_events` row,
/// never swallowed).
#[allow(clippy::too_many_arguments)]
async fn try_dispatch_conflict_auto_resolve(
    mint: &MintContext,
    pool: &DbPool,
    host: &str,
    repo: &str,
    pr_number: u64,
    pr_url: &str,
    issue_key: &str,
    error_msg: &str,
    head_sha: Option<&str>,
) -> bool {
    if !error_msg.to_ascii_lowercase().contains("conflict") {
        return false;
    }
    let Some(head_sha) = head_sha else {
        return false;
    };
    let Some(dispatch) = &mint.comment_dispatch else {
        return false;
    };
    let source = {
        let cfg = dispatch.cfg.read().await;
        cfg.sources
            .iter()
            .find(|(_, src)| repo_allowed(&src.allowed_repos, repo))
            .map(|(name, _)| name.clone())
    };
    let Some(source) = source else {
        error!(repo = %repo, pr = pr_number, "automerge conflict auto-resolve: no comment webhook source covers this repo; falling back to the human comment");
        record_run_event(
            pool,
            None,
            WEBHOOK_MINT_ACTION,
            "error",
            format!("PR {repo}#{pr_number}: conflict auto-resolve dispatch skipped (no comment webhook source covers this repo)"),
        )
        .await;
        return false;
    };
    let comment_id = synthetic_conflict_comment_id(repo, pr_number, head_sha);
    let key = crate::web::comment_webhooks::command_key(repo, pr_number, comment_id);
    let attempts = state::attempts_get(
        pool,
        crate::web::comment_webhooks::WEBHOOK_COMMENT_ACTION,
        &key,
    )
    .await
    .unwrap_or(0);
    if attempts > 0 {
        info!(repo = %repo, pr = pr_number, head_sha, "automerge conflict auto-resolve: already attempted for this head commit; falling back to the human comment");
        return false;
    }
    let author = mint
        .mint_cfg
        .read()
        .await
        .bot_logins
        .first()
        .cloned()
        .unwrap_or_else(|| "claude-run".to_string());
    let cmd = crate::web::comment_webhooks::VerifiedCommentCommand {
        source,
        host: host.to_string(),
        repo: repo.to_string(),
        number: pr_number,
        is_pull_request: true,
        comment_id,
        author,
        kind: crate::web::comment_webhooks::CommandKind::Run {
            instruction:
                "merge the base branch into this PR's branch and resolve any merge conflicts"
                    .to_string(),
        },
        html_url: pr_url.to_string(),
        title: String::new(),
        synthetic: true,
    };
    match dispatch.tx.send(cmd).await {
        Ok(()) => {
            info!(repo = %repo, pr = pr_number, issue = %issue_key, head_sha, "automerge conflict auto-resolve: dispatched");
            record_run_event(
                pool,
                None,
                WEBHOOK_MINT_ACTION,
                "info",
                format!("PR {repo}#{pr_number}: conflict auto-resolve dispatched"),
            )
            .await;
            true
        }
        Err(e) => {
            error!(repo = %repo, pr = pr_number, issue = %issue_key, error = %e, "automerge conflict auto-resolve: dispatch send failed; falling back to the human comment");
            record_run_event(
                pool,
                None,
                WEBHOOK_MINT_ACTION,
                "error",
                format!("PR {repo}#{pr_number}: conflict auto-resolve dispatch send failed: {e}"),
            )
            .await;
            false
        }
    }
}

/// The outcome of one [`apply_automerge_decision`] call, typed so neither
/// caller has to match on the summary text to tell a no-op pass from one that
/// changed something (CLAUDE-397): [`reconcile_automerge`] opens a `runs` row
/// only for [`AutomergeOutcome::Acted`] and `Err`, never for
/// [`AutomergeOutcome::NoAction`], while [`handle_action_success_for_pr`] (the
/// webhook delivery path) still records a row for both, since a delivery is a
/// consumed event regardless of the decision it led to.
#[derive(Debug)]
enum AutomergeOutcome {
    /// Action isn't a youtrack action; automerge disabled; checks not green
    /// yet; or not mergeable (already commented once via
    /// [`record_automerge_outcome_and_comment_once`]).
    NoAction(String),
    /// Branch updated from base; PR merged by `complete_automerge`; or PR
    /// found already merged and marked merged.
    Acted(String),
}

impl AutomergeOutcome {
    fn into_summary(self) -> String {
        match self {
            AutomergeOutcome::NoAction(s) | AutomergeOutcome::Acted(s) => s,
        }
    }
}

/// The shared decision body both [`handle_action_success_for_pr`]
/// (the webhook's low-latency trigger) and [`reconcile_automerge`] (the
/// tick-driven backstop for a missed webhook) call once a recorded, open
/// `agent_pull_requests` row is already in hand: load the row's action to
/// check `automerge.enabled`, read the PR's live CI status, and on
/// green+mergeable call [`crate::native::youtrack::complete_automerge`] - the
/// same merge + comment sequence the old poll loop made, with no
/// `State` or `AI Agent` write, since the YouTrack workflow
/// resolves the issue once it observes the merge. An action-load failure and
/// a `pr_status` failure both concern this
/// already-known, recorded, automerge-eligible PR, so unlike the caller's own
/// pre-resolution faults, both are made loud here: an `error` log, a
/// `run_events` row, and a best-effort issue comment.
async fn apply_automerge_decision(
    mint: &MintContext,
    pool: &DbPool,
    host: &str,
    repo: &str,
    pr_number: u64,
    pr_url: &str,
    row: &ci_failure_store::AgentPullRequest,
) -> Result<AutomergeOutcome, String> {
    let actions_dir = mint.paths.actions_dir();
    let action_name = row.action_name.clone();
    let loaded =
        tokio::task::spawn_blocking(move || crate::action::load(&action_name, &actions_dir)).await;
    let loaded = match loaded {
        Ok(Ok(loaded)) => loaded,
        Ok(Err(e)) => {
            let msg = format!("loading action `{}` failed: {e}", row.action_name);
            error!(repo = %repo, pr = pr_number, issue = %row.issue_key, error = %e, "action-run success: {msg}");
            record_run_event(pool, None, WEBHOOK_MINT_ACTION, "error", msg.clone()).await;
            let body = format!(
                "The runner could not check this PR's automerge status: {msg}.\n\nPR: {pr_url}\n\nA later Action Run event or the periodic reconciler will retry automatically."
            );
            post_issue_comment(
                mint,
                &row.issue_key,
                body,
                repo,
                pr_number,
                "action-run success: action-load-failed comment failed",
            )
            .await;
            return Err(msg);
        }
        Err(e) => {
            let msg = format!("loading action `{}` task panicked: {e}", row.action_name);
            error!(repo = %repo, pr = pr_number, issue = %row.issue_key, error = %e, "action-run success: {msg}");
            record_run_event(pool, None, WEBHOOK_MINT_ACTION, "error", msg.clone()).await;
            return Err(msg);
        }
    };
    let (youtrack_cfg, github_cfg) = match loaded.action.native {
        Some(crate::action::NativeConfig::Youtrack(cfg)) => (cfg.youtrack, cfg.github),
        _ => {
            info!(repo = %repo, pr = pr_number, action = %row.action_name, "action-run success: recorded action is not a youtrack action; no action");
            return Ok(AutomergeOutcome::NoAction(format!(
                "action `{}` is not a youtrack action; no action",
                row.action_name
            )));
        }
    };
    if !youtrack_cfg.automerge.enabled {
        info!(repo = %repo, pr = pr_number, action = %row.action_name, "action-run success: automerge disabled for this action; no action");
        return Ok(AutomergeOutcome::NoAction(
            "automerge disabled for this action; no action".to_string(),
        ));
    }
    // A GitHub-hosted PR must route through `GithubForge`, not
    // `mint.backend` (always `fj`, which 410s against GitHub's API).
    let forge = resolve_forge(mint, host, &github_cfg.hosts, &github_cfg.mcp_endpoint);

    let forge_for_status = forge.clone();
    let host_owned = host.to_string();
    let repo_owned = repo.to_string();
    let status = tokio::task::spawn_blocking(move || {
        forge_for_status.pr_status(&host_owned, &repo_owned, pr_number)
    })
    .await;
    let status = match status {
        Ok(Ok(status)) => status,
        Ok(Err(e)) => {
            let prefix = format!("PR {repo}#{pr_number}: ");
            let outcome = format!("pr_status failed: {e}");
            let msg = format!("{prefix}{outcome}");
            error!(repo = %repo, pr = pr_number, issue = %row.issue_key, error = %e, "action-run success: {msg}");
            let body = format!(
                "The runner could not check this PR's automerge status: {msg}.\n\nPR: {pr_url}\n\nA later Action Run event or the periodic reconciler will retry automatically."
            );
            record_automerge_outcome_and_comment_once(
                mint,
                pool,
                AutomergePr {
                    repo,
                    pr_number,
                    issue_key: &row.issue_key,
                },
                "error",
                outcome,
                body,
                "action-run success: pr-status-failed comment failed",
            )
            .await;
            return Err(msg);
        }
        Err(e) => {
            let msg = format!("pr_status task panicked: {e}");
            error!(repo = %repo, pr = pr_number, issue = %row.issue_key, error = %e, "action-run success: {msg}");
            record_run_event(pool, None, WEBHOOK_MINT_ACTION, "error", msg.clone()).await;
            return Err(msg);
        }
    };

    match status.state.trim() {
        "success" => match status.behind_base {
            // `mergeable` alone cannot tell whether the branch is
            // behind base (Forgejo's `mergeable` ignores that entirely), so an
            // `fj` that does not report `behind_base` at all (predates FJ-99)
            // takes no merge action rather than guessing.
            None => {
                let msg = format!(
                    "PR #{pr_number}: fj pr status did not report behind_base; the runtime's fj is too old to support this check"
                );
                warn!(repo = %repo, pr = pr_number, "action-run success: {msg}");
                let body = crate::native::youtrack::automerge_behind_base_missing_comment(pr_url);
                record_automerge_outcome_and_comment_once(
                    mint,
                    pool,
                    AutomergePr {
                        repo,
                        pr_number,
                        issue_key: &row.issue_key,
                    },
                    "warn",
                    "behind_base missing".to_string(),
                    body,
                    "action-run success: behind_base-missing comment failed",
                )
                .await;
                Err(msg)
            }
            Some(true) => {
                let forge_for_update = forge.clone();
                let host_owned = host.to_string();
                let repo_owned = repo.to_string();
                let update_result = tokio::task::spawn_blocking(move || {
                    forge_for_update.update_pr_branch(&host_owned, &repo_owned, pr_number)
                })
                .await
                .map_err(|e| format!("update_pr_branch task panicked: {e}"))?;
                match update_result {
                    Ok(()) => {
                        let body =
                            crate::native::youtrack::automerge_branch_updated_comment(pr_url);
                        post_issue_comment(
                            mint,
                            &row.issue_key,
                            body,
                            repo,
                            pr_number,
                            "action-run success: branch-updated comment failed",
                        )
                        .await;
                        info!(repo = %repo, pr = pr_number, "action-run success: branch behind base; updated, left at Code Review");
                        Ok(AutomergeOutcome::Acted(format!(
                            "PR #{pr_number} branch updated from base; will automerge on next green run"
                        )))
                    }
                    Err(e) => {
                        let msg = format!("update_pr_branch failed: {e}");
                        let dispatched = try_dispatch_conflict_auto_resolve(
                            mint,
                            pool,
                            host,
                            repo,
                            pr_number,
                            pr_url,
                            &row.issue_key,
                            &msg,
                            status.head_sha.as_deref(),
                        )
                        .await;
                        // The dedupe key below is the comment-once guard's
                        // identity for "this outcome"; folding `dispatched`
                        // into it (rather than just `msg`) means the fallback
                        // comment still posts the first time the conflict
                        // auto-resolve attempt has already run and failed,
                        // even though the underlying forge error text is
                        // unchanged from the first, dispatching, failure.
                        let (body, outcome) = if dispatched {
                            (
                                crate::native::youtrack::automerge_conflict_resolving_comment(
                                    pr_url,
                                ),
                                format!("{msg} (conflict auto-resolve dispatched)"),
                            )
                        } else {
                            (
                                crate::native::youtrack::automerge_update_failed_comment(
                                    pr_url, &msg,
                                ),
                                msg.clone(),
                            )
                        };
                        record_automerge_outcome_and_comment_once(
                            mint,
                            pool,
                            AutomergePr {
                                repo,
                                pr_number,
                                issue_key: &row.issue_key,
                            },
                            "warn",
                            outcome,
                            body,
                            "action-run success: update-failed comment failed",
                        )
                        .await;
                        Err(msg)
                    }
                }
            }
            Some(false) if status.mergeable != Some(true) => {
                let body = crate::native::youtrack::automerge_not_mergeable_comment(pr_url);
                record_automerge_outcome_and_comment_once(
                    mint,
                    pool,
                    AutomergePr {
                        repo,
                        pr_number,
                        issue_key: &row.issue_key,
                    },
                    "info",
                    "not mergeable".to_string(),
                    body,
                    "action-run success: not-mergeable comment failed",
                )
                .await;
                info!(repo = %repo, pr = pr_number, "action-run success: not mergeable; commented, left at Code Review");
                Ok(AutomergeOutcome::NoAction(format!(
                    "PR #{pr_number} not mergeable; commented, left at Code Review"
                )))
            }
            Some(false) => {
                let backend = mint.backend.clone();
                let forge_for_merge = forge.clone();
                let host_owned = host.to_string();
                let repo_owned = repo.to_string();
                let issue_key = row.issue_key.clone();
                let pr_url_owned = pr_url.to_string();
                let merge_result = tokio::task::spawn_blocking(move || {
                    crate::native::youtrack::complete_automerge(
                        backend.as_ref(),
                        forge_for_merge.as_ref(),
                        &issue_key,
                        &host_owned,
                        &repo_owned,
                        pr_number,
                        &pr_url_owned,
                    )
                })
                .await
                .map_err(|e| format!("complete_automerge task panicked: {e}"))?;
                match merge_result {
                    Ok(()) => {
                        info!(repo = %repo, pr = pr_number, issue = %row.issue_key, "action-run success: merged PR; the YouTrack workflow will resolve the issue");
                        mark_agent_pr_merged(pool, row, repo, pr_number).await?;
                        Ok(AutomergeOutcome::Acted(format!(
                            "merged PR #{pr_number}; issue {} left for the YouTrack workflow to resolve",
                            row.issue_key
                        )))
                    }
                    Err(e) => {
                        let msg = format!("complete_automerge failed: {e}");
                        let body =
                            crate::native::youtrack::automerge_merge_failed_comment(pr_url, &msg);
                        record_automerge_outcome_and_comment_once(
                            mint,
                            pool,
                            AutomergePr {
                                repo,
                                pr_number,
                                issue_key: &row.issue_key,
                            },
                            "error",
                            msg.clone(),
                            body,
                            "action-run success: merge-failed comment failed",
                        )
                        .await;
                        Err(msg)
                    }
                }
            }
        },
        "merged" => {
            mark_agent_pr_merged(pool, row, repo, pr_number).await?;
            info!(repo = %repo, pr = pr_number, issue = %row.issue_key, "action-run success: PR already merged; marked merged");
            Ok(AutomergeOutcome::Acted(format!(
                "agent PR #{pr_number} already merged; marked merged"
            )))
        }
        other => {
            info!(repo = %repo, pr = pr_number, state = %other, "action-run success: checks not successful; no action");
            Ok(AutomergeOutcome::NoAction(format!(
                "PR #{pr_number} checks not successful ({other}); no action"
            )))
        }
    }
}

/// Record a recorded agent PR as merged so neither the webhook nor
/// the reconciler walks it again. A store failure is logged at `error`, written
/// as a `run_events` row, and returned: an unrecorded merge would be re-walked
/// on every tick.
async fn mark_agent_pr_merged(
    pool: &DbPool,
    row: &ci_failure_store::AgentPullRequest,
    repo: &str,
    pr_number: u64,
) -> Result<(), String> {
    match ci_failure_store::set_agent_pr_state(pool, row.id, "merged", &state::now_iso()).await {
        Ok(()) => Ok(()),
        Err(e) => {
            let msg = format!("marking agent PR #{pr_number} merged failed: {e}");
            error!(repo = %repo, pr = pr_number, issue = %row.issue_key, error = %e, "action-run success: {msg}");
            record_run_event(pool, None, WEBHOOK_MINT_ACTION, "error", msg.clone()).await;
            Err(msg)
        }
    }
}

/// The reconciler backstop for a missed, dropped, or too-early
/// Action Run success webhook. Runs on its own tick (`daemon::automerge_reconciler_loop`),
/// independent of any webhook delivery: lists every `agent_pull_requests` row
/// still `open` and, for each, calls the exact same [`apply_automerge_decision`]
/// the webhook path calls, so a green+mergeable PR is merged on the next tick
/// even if no event for it ever arrives. Idempotent: a PR already merged or
/// closed since it was listed is caught by the row's own `agent_pr` state on
/// the webhook path, and here by `pr_status` simply reporting a state that
/// still routes to "no action" (or, for a PR that raced closed between the
/// list and the status call, by the forge call itself failing, which is
/// surfaced the same way any other `apply_automerge_decision` fault is).
///
/// `listing_failures` tracks, per `(host, repo)`, how many consecutive
/// passes `list_open_pr_numbers` has failed for that pair. It is owned by the
/// caller (`daemon::automerge_reconciler_loop`) and lives only in memory: a
/// daemon restart starts empty, so a restart never produces a false
/// "recovered", and a failure that predates a restart simply never gets a
/// recovery line. The first later pass on which the listing succeeds removes
/// the entry and writes a `recovered` `run_events` row naming the repo and
/// how many passes failed.
pub(crate) async fn reconcile_automerge(
    pool: &DbPool,
    mint: &MintContext,
    keygate: &crate::concurrency::KeyGate,
    listing_failures: &mut std::collections::HashMap<(String, String), u32>,
) {
    // The reconciler writes to the forge and tracker, so it honours
    // the same pauses the webhook ingest does.
    if webhook_action_paused(pool, WEBHOOK_MINT_ACTION, "automerge reconciler").await {
        return;
    }
    let rows = match ci_failure_store::open_agent_prs(pool).await {
        Ok(rows) => rows,
        Err(e) => {
            let msg = format!("automerge reconciler: listing open agent PRs failed: {e}");
            error!(error = %e, "{msg}");
            record_run_event(pool, None, WEBHOOK_MINT_ACTION, "error", msg).await;
            return;
        }
    };

    // One open-PR listing per (host, repo). A row whose PR is not
    // listed is retired instead of asking `pr_status` about it, which fails
    // for a PR closed without merge (`fj`: "no commits in pr").
    let mut open_by_repo: std::collections::HashMap<(String, String), Option<Vec<u64>>> =
        std::collections::HashMap::new();
    for row in rows {
        let repo_key = (row.host.clone(), row.repo.clone());
        if !open_by_repo.contains_key(&repo_key) {
            let listed = list_open_pr_numbers(pool, mint, &row.host, &row.repo).await;
            match &listed {
                Some(_) => {
                    if let Some(failed_passes) = listing_failures.remove(&repo_key) {
                        let msg = format!(
                            "automerge reconciler: list_open_prs for {} recovered after {} failed pass(es)",
                            row.repo, failed_passes
                        );
                        info!(repo = %row.repo, failed_passes, "{msg}");
                        record_run_event(pool, None, WEBHOOK_MINT_ACTION, "info", msg).await;
                    }
                }
                None => {
                    *listing_failures.entry(repo_key.clone()).or_insert(0) += 1;
                }
            }
            open_by_repo.insert(repo_key.clone(), listed);
        }
        match &open_by_repo[&repo_key] {
            None => continue,
            Some(open) if !open.contains(&(row.pr_number as u64)) => {
                let now = state::now_iso();
                match ci_failure_store::set_agent_pr_state(pool, row.id, "closed", &now).await {
                    Ok(()) => {
                        info!(repo = %row.repo, pr = row.pr_number, issue = %row.issue_key, "automerge reconciler: PR is no longer open on the forge; marked closed")
                    }
                    Err(e) => {
                        let msg = format!(
                            "automerge reconciler: marking agent PR {}#{} closed failed: {e}",
                            row.repo, row.pr_number
                        );
                        error!(repo = %row.repo, pr = row.pr_number, error = %e, "{msg}");
                        record_run_event(pool, None, WEBHOOK_MINT_ACTION, "error", msg).await;
                    }
                }
                continue;
            }
            Some(_) => {}
        }
        let key = format!("{}#{}", row.repo, row.pr_number);
        let _key_guard = keygate.acquire(&key).await;
        let pr_url = agent_pr_url(&row.host, &row.repo, row.pr_number);
        let job_key = format!(
            "automerge-reconcile-{}-{}",
            row.repo.replace('/', "_"),
            row.pr_number
        );
        let result = apply_automerge_decision(
            mint,
            pool,
            &row.host,
            &row.repo,
            row.pr_number as u64,
            &pr_url,
            &row,
        )
        .await;
        let (ok, summary) = match result {
            Ok(AutomergeOutcome::NoAction(_)) => continue,
            Ok(AutomergeOutcome::Acted(s)) => (true, s),
            Err(e) => {
                warn!(repo = %row.repo, pr = row.pr_number, error = %e, "automerge reconciler: decision failed");
                (false, e)
            }
        };
        let run = begin_webhook_run(pool, WEBHOOK_MINT_ACTION, &job_key, &pr_url).await;
        finish_webhook_run(pool, run, &job_key, &summary, ok, None, None).await;
    }
}

/// The numbers of the PRs the forge lists as open for `repo`, or
/// `None` when the listing failed (logged at `error` and written to
/// `run_events`), so the caller leaves that repo's rows alone rather than
/// reading an unknown as "closed".
async fn list_open_pr_numbers(
    pool: &DbPool,
    mint: &MintContext,
    host: &str,
    repo: &str,
) -> Option<Vec<u64>> {
    // Called per `(host, repo)`, grouped across rows, before any
    // row's action is loaded - so no `github.hosts` is available here.
    // `is_github_host(host, &[])` (i.e. `host.eq_ignore_ascii_case("github.com")`)
    // is a deliberate, narrower default: it correctly routes the reported
    // `github.com` case, and a GHE host configured only via an action's
    // `github.hosts` remains a known, narrower gap for a follow-up rather
    // than an oversight.
    let forge = resolve_forge(
        mint,
        host,
        &[],
        &crate::action::default_github_mcp_endpoint(),
    );
    let (host_owned, repo_owned) = (host.to_string(), repo.to_string());
    let outcome =
        tokio::task::spawn_blocking(move || forge.list_open_prs(&host_owned, &repo_owned)).await;
    let msg = match outcome {
        Ok(Ok(prs)) => return Some(prs.into_iter().map(|p| p.number).collect()),
        Ok(Err(e)) => format!("automerge reconciler: list_open_prs for {repo} failed: {e}"),
        Err(e) => format!("automerge reconciler: list_open_prs task for {repo} panicked: {e}"),
    };
    error!(repo = %repo, "{msg}");
    record_run_event(pool, None, WEBHOOK_MINT_ACTION, "error", msg).await;
    None
}

/// Post a best-effort issue comment on an automerge decision path. A
/// failed comment (write error, or the blocking task itself panicking) logs at
/// `error` naming the issue and the cause; it never replaces the caller's own
/// `Result`, since the underlying decision (branch updated, merge refused, ...)
/// is the outcome that matters even when the tracker write announcing it fails.
async fn post_issue_comment(
    mint: &MintContext,
    issue_key: &str,
    body: String,
    repo: &str,
    pr_number: u64,
    context: &str,
) {
    let backend = mint.backend.clone();
    let key = issue_key.to_string();
    match tokio::task::spawn_blocking(move || backend.comment(&key, &body)).await {
        Ok(Ok(())) => {}
        Ok(Err(e)) => {
            error!(repo = %repo, pr = pr_number, issue = %issue_key, error = %e, "{context}");
        }
        Err(e) => {
            error!(repo = %repo, pr = pr_number, issue = %issue_key, error = %e, "{context} (task panicked)");
        }
    }
}

/// Resolve a source's configured webhook secret to plaintext, loading the age key
/// from `CLAUDE_RUN_SECRETS_KEY_FILE`. Errors when the source has no `secret_ref`
/// or the secret cannot be decrypted.
async fn resolve_secret(st: &WebhookState, src: &WebhookSource) -> crate::error::Result<String> {
    let name = src.secret_ref.as_deref().ok_or_else(|| {
        crate::error::Error::Secret("webhook source secret_ref is not configured".to_string())
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

/// True when `repo` (an `owner/repo` full name) matches one of the allow-list
/// patterns. The three literal forms and the case-sensitive,
/// empty-denies-all semantics live in [`crate::config::repo_matches`], shared with
/// the reconciler's `repos` and the per-repo log-token mapping.
pub(crate) fn repo_allowed(allowed: &[String], repo: &str) -> bool {
    crate::config::repo_matches(allowed, repo)
}

/// Build a [`VerifiedFailureEvent`] when the payload is an `action_run_failure`
/// for an allow-listed repo; `None` otherwise (caller acks and drops).
pub fn verified_failure_event(
    payload: &ActionPayload,
    allowed_repos: &[String],
    source: &str,
) -> Option<VerifiedFailureEvent> {
    if payload.action != "failure" {
        return None;
    }
    let repo = &payload.run.repository.full_name;
    if !repo_allowed(allowed_repos, repo) {
        return None;
    }
    Some(VerifiedFailureEvent {
        repo: repo.clone(),
        index_in_repo: payload.run.index_in_repo,
        commit_sha: payload.run.commit_sha.clone(),
        workflow_id: payload.run.workflow_id.clone(),
        private: payload.run.repository.private,
        source: source.to_string(),
        trigger_user: payload.run.trigger_user.login.clone(),
        html_url: payload.run.html_url.clone(),
        status: payload.run.status.clone(),
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

    /// A fresh, unbounded per-test ledger; no assertion in this module reads
    /// it unless it says otherwise, so its budget never matters.
    fn test_spend() -> crate::usage::RunSpend {
        crate::usage::RunSpend::new(
            WEBHOOK_MINT_ACTION,
            crate::usage::RunBudget::new(1000.0, 0.8),
            None,
        )
    }

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
        let event = verified_failure_event(&payload, &allowed, "test-source").expect("emits event");
        assert_eq!(
            event,
            VerifiedFailureEvent {
                repo: "contoso/claude-run".to_string(),
                index_in_repo: 7,
                commit_sha: "abc123".to_string(),
                workflow_id: "ci.yml".to_string(),
                private: false,
                source: "test-source".to_string(),
                trigger_user: "alice".to_string(),
                html_url: "https://dev.a8n.run/contoso/claude-run/actions/runs/7".to_string(),
                status: "failure".to_string(),
            }
        );
    }

    #[test]
    fn failure_from_unlisted_repo_is_dropped() {
        let payload: ActionPayload =
            serde_json::from_str(&failure_payload("someone/else")).unwrap();
        let allowed = vec!["contoso/claude-run".to_string()];
        assert!(verified_failure_event(&payload, &allowed, "test-source").is_none());
    }

    // ---- Allow-list patterns (`*`, `owner/*`, exact) ------------

    #[test]
    fn repo_allowed_star_matches_every_repo() {
        let allowed = vec!["*".to_string()];
        assert!(repo_allowed(&allowed, "contoso/claude-run"));
        assert!(repo_allowed(&allowed, "fabrikam/gizmo"));
    }

    #[test]
    fn repo_allowed_owner_wildcard_matches_only_that_owner() {
        let allowed = vec!["fabrikam/*".to_string()];
        assert!(repo_allowed(&allowed, "fabrikam/forgejo-cli"));
        assert!(repo_allowed(&allowed, "fabrikam/gizmo"));
        assert!(!repo_allowed(&allowed, "contoso/claude-run"));
        // The bare owner (no slash) and a prefixed owner must not match.
        assert!(!repo_allowed(&allowed, "fabrikam"));
        assert!(!repo_allowed(&allowed, "xfabrikam/repo"));
    }

    #[test]
    fn repo_allowed_exact_and_empty_behave_as_before() {
        let exact = vec!["contoso/claude-run".to_string()];
        assert!(repo_allowed(&exact, "contoso/claude-run"));
        assert!(!repo_allowed(&exact, "contoso/governance"));
        // Case-sensitive, like the original exact match.
        assert!(!repo_allowed(&exact, "Contoso/claude-run"));
        // Empty list stays deny-all.
        assert!(!repo_allowed(&[], "contoso/claude-run"));
    }

    #[test]
    fn repo_allowed_mixed_list_takes_any_match() {
        let allowed = vec!["contoso/governance".to_string(), "fabrikam/*".to_string()];
        assert!(repo_allowed(&allowed, "contoso/governance"));
        assert!(repo_allowed(&allowed, "fabrikam/gizmo"));
        assert!(!repo_allowed(&allowed, "contoso/claude-run"));
    }

    #[test]
    fn failure_event_emitted_for_owner_wildcard() {
        let payload: ActionPayload =
            serde_json::from_str(&failure_payload("fabrikam/gizmo")).unwrap();
        let allowed = vec!["fabrikam/*".to_string()];
        let event =
            verified_failure_event(&payload, &allowed, "test-source").expect("wildcard allows");
        assert_eq!(event.repo, "fabrikam/gizmo");
    }

    #[test]
    fn non_failure_action_is_dropped_even_when_listed() {
        let body = r#"{"action":"success","run":{"index_in_repo":1,"commit_sha":"x",
            "workflow_id":"ci.yml","status":"success","trigger_user":{"login":"bob"},
            "html_url":"https://dev.a8n.run/contoso/claude-run/actions/runs/1",
            "repository":{"full_name":"contoso/claude-run"}}}"#;
        let payload: ActionPayload = serde_json::from_str(body).unwrap();
        let allowed = vec!["contoso/claude-run".to_string()];
        assert!(verified_failure_event(&payload, &allowed, "test-source").is_none());
    }

    #[test]
    fn trigger_user_accepts_username_alias() {
        let body = r#"{"action":"failure","run":{"index_in_repo":1,"commit_sha":"x",
            "workflow_id":"ci.yml","status":"failure","trigger_user":{"username":"carol"},
            "html_url":"https://dev.a8n.run/contoso/claude-run/actions/runs/1",
            "repository":{"full_name":"contoso/claude-run"}}}"#;
        let payload: ActionPayload = serde_json::from_str(body).unwrap();
        let allowed = vec!["contoso/claude-run".to_string()];
        let event = verified_failure_event(&payload, &allowed, "test-source").unwrap();
        assert_eq!(event.trigger_user, "carol");
    }

    #[test]
    fn trigger_user_accepts_both_login_and_username() {
        // Live Forgejo 14.0.2 payloads carry both keys in one user object; the
        // old `#[serde(alias = "username")]` rejected this with `duplicate field
        // `login``. Both present must parse and yield the `login` value.
        let body = r#"{"action":"failure","run":{"index_in_repo":1,"commit_sha":"x",
            "workflow_id":"ci.yml","status":"failure",
            "trigger_user":{"login":"dave","username":"dave"},
            "html_url":"https://dev.a8n.run/contoso/claude-run/actions/runs/1",
            "repository":{"full_name":"contoso/claude-run"}}}"#;
        let payload: ActionPayload = serde_json::from_str(body).unwrap();
        let allowed = vec!["contoso/claude-run".to_string()];
        let event = verified_failure_event(&payload, &allowed, "test-source").unwrap();
        assert_eq!(event.trigger_user, "dave");
    }

    #[test]
    fn trigger_user_with_neither_key_fails_parse() {
        let body = r#"{"action":"failure","run":{"index_in_repo":1,"commit_sha":"x",
            "workflow_id":"ci.yml","status":"failure","trigger_user":{},
            "html_url":"https://dev.a8n.run/contoso/claude-run/actions/runs/1",
            "repository":{"full_name":"contoso/claude-run"}}}"#;
        let err = serde_json::from_str::<ActionPayload>(body).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("login"), "error names `login`: {msg}");
        assert!(msg.contains("username"), "error names `username`: {msg}");
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

    // ---- Triage + mint composition -----------------------------

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
        fn classify(
            &self,
            _input: &TriageInput,
            _spend: &crate::usage::RunSpend,
        ) -> crate::error::Result<Triage> {
            *self.calls.lock().unwrap() += 1;
            Ok(Triage {
                bucket: self.bucket,
                analysis: "stub analysis".to_string(),
                classified_by: ClassifiedBy::Llm,
                auto_fix: None,
            })
        }
    }

    /// A classifier that records a real invocation cost onto the
    /// caller's ledger, the way `AnalysisClassifier` does, so a test can assert
    /// the cost reaches the webhook run's outcome row and daily ledger.
    struct CostingClassifier {
        bucket: TriageBucket,
        cost: f64,
    }

    impl CostingClassifier {
        fn new(bucket: TriageBucket, cost: f64) -> Self {
            Self { bucket, cost }
        }
    }

    impl FailureClassifier for CostingClassifier {
        fn classify(
            &self,
            _input: &TriageInput,
            spend: &crate::usage::RunSpend,
        ) -> crate::error::Result<Triage> {
            spend.record(&crate::claude::ClaudeResult {
                ok: true,
                summary: "classified".to_string(),
                cost: self.cost,
                usage: Default::default(),
                raw: serde_json::Value::Null,
                error: String::new(),
                transient: false,
                timed_out: false,
                login_required: false,
                killed: false,
                exit_code: Some(0),
            });
            Ok(Triage {
                bucket: self.bucket,
                analysis: "costing analysis".to_string(),
                classified_by: ClassifiedBy::Llm,
                auto_fix: None,
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
        fn set_assignee(&self, key: &str, login: &str) -> crate::error::Result<()> {
            self.calls
                .lock()
                .unwrap()
                .push(format!("set_assignee {key} {login}"));
            Ok(())
        }
        fn comment(&self, _key: &str, _body: &str) -> crate::error::Result<()> {
            unreachable!()
        }
        fn issue_links(&self, _key: &str) -> crate::error::Result<Vec<crate::provider::IssueLink>> {
            unreachable!()
        }
        fn inspect_issue(&self, _key: &str) -> crate::error::Result<crate::provider::IssueInspect> {
            unreachable!()
        }
        fn resolve_vcs(
            &self,
            _project_key: &str,
        ) -> crate::error::Result<crate::provider::VcsRepo> {
            unreachable!()
        }
        fn vcs_repo_map(&self) -> crate::error::Result<Vec<crate::provider::VcsMapEntry>> {
            // The webhook tests route via the explicit override, so the map is
            // never consulted; return empty to keep the stub total.
            Ok(Vec::new())
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

    // ---- `ci_failure` store dedupe through the full pipeline ---

    /// A [`Backend`] (IssueTracker + Forge) driving `consume_events` end to
    /// end for the store-dedupe tests: `create_issue` hands out incrementing
    /// keys, every mutation is recorded, and there is never an open PR (the
    /// Forge surface the `has_open_pr` signal reads).
    struct StoreBackend {
        next_key: StdMutex<u32>,
        calls: StdMutex<Vec<String>>,
        /// When set, `list_queued` returns this issue for any query: the
        /// legacy-adoption lookup's canned hit.
        legacy_match: StdMutex<Option<String>>,
        /// Canned `list_open_prs` result, so a test can put the
        /// failing commit on an open PR.
        open_prs: StdMutex<Vec<ForgePullRequest>>,
        /// When set, `list_open_prs` fails with this message.
        open_prs_error: StdMutex<Option<String>>,
        /// Canned `pr_status` result for the automerge success-event
        /// tests. `None` (the default) errors, matching every pre-existing test
        /// that never reaches the automerge path.
        pr_status: StdMutex<Option<crate::provider::PrStatus>>,
        /// When set, `update_pr_branch` fails with this message.
        update_pr_branch_error: StdMutex<Option<String>>,
        /// When set, `merge_pr` fails with this message.
        merge_pr_error: StdMutex<Option<String>>,
        /// Canned `get_field` result for the match-time YouTrack
        /// `State` read. Defaults to a value that never equals a test's
        /// `resolved_state` ("Done"), so every pre-existing test that reaches
        /// an `open`-match dispatch keeps taking the non-regression path with
        /// no change.
        state_field: StdMutex<Option<String>>,
        /// When set, `get_field` fails with this message.
        state_field_error: StdMutex<Option<String>>,
    }

    impl StoreBackend {
        fn new() -> Self {
            Self {
                next_key: StdMutex::new(100),
                calls: StdMutex::new(Vec::new()),
                legacy_match: StdMutex::new(None),
                open_prs: StdMutex::new(Vec::new()),
                open_prs_error: StdMutex::new(None),
                pr_status: StdMutex::new(None),
                update_pr_branch_error: StdMutex::new(None),
                merge_pr_error: StdMutex::new(None),
                state_field: StdMutex::new(None),
                state_field_error: StdMutex::new(None),
            }
        }
        fn with_legacy_match(self, key: &str) -> Self {
            *self.legacy_match.lock().unwrap() = Some(key.to_string());
            self
        }
        fn with_open_prs(self, prs: Vec<ForgePullRequest>) -> Self {
            *self.open_prs.lock().unwrap() = prs;
            self
        }
        fn with_open_prs_error(self, message: &str) -> Self {
            *self.open_prs_error.lock().unwrap() = Some(message.to_string());
            self
        }
        /// `behind_base` defaults to `Some(false)` (not behind), so
        /// every pre-existing call site keeps its original meaning without
        /// having to name a value it never tested.
        fn with_pr_status(self, state: &str, mergeable: bool) -> Self {
            self.with_pr_status_behind(state, mergeable, Some(false))
        }
        fn with_pr_status_behind(
            self,
            state: &str,
            mergeable: bool,
            behind_base: Option<bool>,
        ) -> Self {
            self.with_pr_status_full(state, Some(mergeable), behind_base, None)
        }
        /// Full control over every [`crate::provider::PrStatus`] field,
        /// including `head_sha` (CLAUDE-380's conflict-dispatch dedupe key).
        fn with_pr_status_full(
            self,
            state: &str,
            mergeable: Option<bool>,
            behind_base: Option<bool>,
            head_sha: Option<&str>,
        ) -> Self {
            *self.pr_status.lock().unwrap() = Some(crate::provider::PrStatus {
                state: state.to_string(),
                mergeable,
                behind_base,
                head_sha: head_sha.map(str::to_string),
            });
            self
        }
        fn with_update_pr_branch_error(self, message: &str) -> Self {
            *self.update_pr_branch_error.lock().unwrap() = Some(message.to_string());
            self
        }
        fn with_merge_pr_error(self, message: &str) -> Self {
            *self.merge_pr_error.lock().unwrap() = Some(message.to_string());
            self
        }
        /// Make the match-time `State` read return `state`.
        fn with_state_field(self, state: &str) -> Self {
            *self.state_field.lock().unwrap() = Some(state.to_string());
            self
        }
        /// Make the match-time `State` read fail with `message`.
        fn with_state_field_error(self, message: &str) -> Self {
            *self.state_field_error.lock().unwrap() = Some(message.to_string());
            self
        }
        fn calls(&self) -> Vec<String> {
            self.calls.lock().unwrap().clone()
        }
    }

    impl crate::provider::IssueTracker for StoreBackend {
        fn list_queued(&self, query: &str) -> crate::error::Result<Vec<serde_json::Value>> {
            self.calls.lock().unwrap().push(format!("list {query}"));
            match self.legacy_match.lock().unwrap().as_ref() {
                Some(key) => Ok(vec![serde_json::json!({"idReadable": key})]),
                None => Ok(Vec::new()),
            }
        }
        fn get_field(&self, key: &str, field: &str) -> crate::error::Result<String> {
            self.calls
                .lock()
                .unwrap()
                .push(format!("get_field {key} {field}"));
            if let Some(message) = self.state_field_error.lock().unwrap().clone() {
                return Err(crate::error::Error::Other(anyhow::anyhow!(message)));
            }
            Ok(self
                .state_field
                .lock()
                .unwrap()
                .clone()
                .unwrap_or_else(|| "Open".to_string()))
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
        fn set_assignee(&self, key: &str, login: &str) -> crate::error::Result<()> {
            self.calls
                .lock()
                .unwrap()
                .push(format!("set_assignee {key} {login}"));
            Ok(())
        }
        fn comment(&self, key: &str, body: &str) -> crate::error::Result<()> {
            self.calls
                .lock()
                .unwrap()
                .push(format!("comment {key} | {body}"));
            Ok(())
        }
        fn issue_links(&self, _key: &str) -> crate::error::Result<Vec<crate::provider::IssueLink>> {
            unreachable!()
        }
        fn inspect_issue(&self, _key: &str) -> crate::error::Result<crate::provider::IssueInspect> {
            unreachable!()
        }
        fn resolve_vcs(
            &self,
            _project_key: &str,
        ) -> crate::error::Result<crate::provider::VcsRepo> {
            unreachable!()
        }
        fn vcs_repo_map(&self) -> crate::error::Result<Vec<crate::provider::VcsMapEntry>> {
            Ok(Vec::new())
        }
        fn create_issue(
            &self,
            project: &str,
            summary: &str,
            _description: &str,
            assignee: Option<&str>,
        ) -> crate::error::Result<String> {
            let mut next = self.next_key.lock().unwrap();
            let key = format!("CLAUDE-{next}");
            *next += 1;
            self.calls.lock().unwrap().push(format!(
                "create {project} {key} | {summary} | assignee={}",
                assignee.unwrap_or("-")
            ));
            Ok(key)
        }
        fn attach_file(&self, _key: &str, _file: &Path) -> crate::error::Result<()> {
            Ok(())
        }
    }

    impl crate::provider::Forge for StoreBackend {
        fn create_pr(
            &self,
            _workdir: &Path,
            _host: &str,
            _strategy: crate::action::PrStrategy,
        ) -> crate::error::Result<crate::provider::PullRequest> {
            unreachable!()
        }
        fn clone_url(&self, _host: &str, _repo: &str) -> crate::error::Result<String> {
            unreachable!()
        }
        fn repo_access(&self, _host: &str, _repo: &str) -> crate::error::Result<()> {
            unreachable!()
        }
        fn list_open_prs(
            &self,
            host: &str,
            repo: &str,
        ) -> crate::error::Result<Vec<ForgePullRequest>> {
            self.calls
                .lock()
                .unwrap()
                .push(format!("list_open_prs {host} {repo}"));
            if let Some(message) = self.open_prs_error.lock().unwrap().clone() {
                return Err(crate::error::Error::Other(anyhow::anyhow!(message)));
            }
            Ok(self.open_prs.lock().unwrap().clone())
        }
        fn pr_ci_state(
            &self,
            _host: &str,
            _repo: &str,
            _pr_number: u64,
        ) -> crate::error::Result<String> {
            unreachable!()
        }
        fn pr_comment(
            &self,
            _host: &str,
            _repo: &str,
            _pr_number: u64,
            _body_file: &Path,
        ) -> crate::error::Result<()> {
            unreachable!()
        }
        fn issue_comment(
            &self,
            _host: &str,
            _repo: &str,
            _issue_number: u64,
            _body_file: &Path,
        ) -> crate::error::Result<()> {
            unreachable!()
        }
        fn pr_status(
            &self,
            _host: &str,
            _repo: &str,
            pr_number: u64,
        ) -> crate::error::Result<crate::provider::PrStatus> {
            self.calls
                .lock()
                .unwrap()
                .push(format!("pr_status {pr_number}"));
            self.pr_status
                .lock()
                .unwrap()
                .clone()
                .ok_or_else(|| crate::error::Error::Other(anyhow::anyhow!("no canned pr_status")))
        }
        fn merge_pr(&self, _host: &str, _repo: &str, pr_number: u64) -> crate::error::Result<()> {
            self.calls
                .lock()
                .unwrap()
                .push(format!("merge_pr {pr_number}"));
            if let Some(message) = self.merge_pr_error.lock().unwrap().clone() {
                return Err(crate::error::Error::Other(anyhow::anyhow!(message)));
            }
            Ok(())
        }
        fn update_pr_branch(
            &self,
            _host: &str,
            _repo: &str,
            pr_number: u64,
        ) -> crate::error::Result<()> {
            self.calls
                .lock()
                .unwrap()
                .push(format!("update_pr_branch {pr_number}"));
            if let Some(message) = self.update_pr_branch_error.lock().unwrap().clone() {
                return Err(crate::error::Error::Other(anyhow::anyhow!(message)));
            }
            Ok(())
        }
    }

    impl Backend for StoreBackend {
        fn name(&self) -> &'static str {
            "store-backend-stub"
        }
    }

    /// A [`FailureClassifier`] that always returns `code_fix`, recording how
    /// many times it ran so a test can assert the store dedupe skipped it.
    struct CountingClassifier {
        calls: StdMutex<u32>,
    }

    impl CountingClassifier {
        fn new() -> Self {
            Self {
                calls: StdMutex::new(0),
            }
        }
        fn call_count(&self) -> u32 {
            *self.calls.lock().unwrap()
        }
    }

    impl FailureClassifier for CountingClassifier {
        fn classify(
            &self,
            _input: &TriageInput,
            _spend: &crate::usage::RunSpend,
        ) -> crate::error::Result<Triage> {
            *self.calls.lock().unwrap() += 1;
            Ok(Triage {
                bucket: TriageBucket::CodeFix,
                analysis: "root cause and next step".to_string(),
                classified_by: ClassifiedBy::Llm,
                auto_fix: None,
            })
        }
    }

    /// A log carrying `test_id` as its one failing test (cargo-test-shaped, so
    /// [`fingerprint::fingerprint`] parses it), on `branch`, resolvable to a
    /// non-transient signature.
    fn failing_test_log(target: &str, test_id: &str, branch: &str) -> String {
        format!(
            "GITHUB_HEAD_REF={branch}\n     Running tests/{target}.rs (target/debug/deps/{target}-abc123)\ntest {test_id} ... FAILED\n"
        )
    }

    /// Like [`failing_test_log`], carrying every id in `test_ids` as a failing
    /// test on the same target (CLAUDE-371: used to exercise the
    /// shrinking-set / different-set fingerprint comparisons, which need more
    /// than one failing test per attempt).
    fn failing_tests_log(target: &str, test_ids: &[&str], branch: &str) -> String {
        let mut log = format!(
            "GITHUB_HEAD_REF={branch}\n     Running tests/{target}.rs (target/debug/deps/{target}-abc123)\n"
        );
        for test_id in test_ids {
            log.push_str(&format!("test {test_id} ... FAILED\n"));
        }
        log
    }

    /// A log on `branch` with no diagnostic line at all, so
    /// [`fingerprint::fingerprint`] parses an empty id set from it
    /// (CLAUDE-371: the empty/unparseable-fingerprint fallback case).
    fn unparseable_log(branch: &str) -> String {
        format!("GITHUB_HEAD_REF={branch}\nnothing recognizable here\n")
    }

    fn store_mint_cfg() -> MintConfig {
        MintConfig {
            repo_project_map: [("acme/widgets".to_string(), "CLAUDE".to_string())]
                .into_iter()
                .collect(),
            ..MintConfig::default()
        }
    }

    fn test_paths(dir: &std::path::Path) -> Paths {
        Paths {
            config_dir: dir.to_path_buf(),
            data_dir: dir.to_path_buf(),
            state_dir: dir.to_path_buf(),
            runtime_dir: dir.to_path_buf(),
        }
    }

    fn store_mint_context(
        pool: DbPool,
        backend: Arc<StoreBackend>,
        classifier: Arc<dyn FailureClassifier>,
    ) -> MintContext {
        store_mint_context_in(
            pool,
            backend,
            classifier,
            std::env::temp_dir().as_path(),
            Arc::new(crate::provider::cli::testutil::StubRunner::new()),
        )
    }

    fn store_mint_context_in(
        pool: DbPool,
        backend: Arc<StoreBackend>,
        classifier: Arc<dyn FailureClassifier>,
        paths_dir: &std::path::Path,
        pr_comment_runner: Arc<dyn CommandRunner>,
    ) -> MintContext {
        MintContext {
            pool,
            classifier,
            backend,
            mint_cfg: Arc::new(RwLock::new(store_mint_cfg())),
            defaults: RunnerDefaults::default(),
            route_cache: Arc::new(route_cache()),
            paths: test_paths(paths_dir),
            pr_comment_runner,
            comment_dispatch: None,
        }
    }

    /// One `action_run_failure` event for `acme/widgets`, run `index_in_repo`,
    /// with a distinct commit per run. The branch itself travels in the job
    /// log's `GITHUB_HEAD_REF` line, not on the event.
    fn store_event(index_in_repo: i64) -> VerifiedFailureEvent {
        VerifiedFailureEvent {
            repo: "acme/widgets".to_string(),
            index_in_repo,
            commit_sha: format!("commit-{index_in_repo}"),
            workflow_id: "ci.yml".to_string(),
            private: false,
            source: "test-source".to_string(),
            trigger_user: "alice".to_string(),
            html_url: format!("https://dev.a8n.run/acme/widgets/actions/runs/{index_in_repo}"),
            status: "failure".to_string(),
        }
    }

    /// A [`store_event`]-shaped event on `github.com`, so a test can
    /// drive the triage path through [`forge_for_event`]'s GitHub branch.
    fn github_store_event(index_in_repo: i64, repo: &str) -> VerifiedFailureEvent {
        VerifiedFailureEvent {
            repo: repo.to_string(),
            index_in_repo,
            commit_sha: format!("commit-{index_in_repo}"),
            workflow_id: "ci.yml".to_string(),
            private: false,
            source: "test-source".to_string(),
            trigger_user: "alice".to_string(),
            html_url: format!("https://github.com/{repo}/actions/runs/{index_in_repo}"),
            status: "failure".to_string(),
        }
    }

    /// [`ActionLogClient`] returning one canned failing job's log per repo/run
    /// number, keyed the way the worked-example test needs it.
    struct ScriptedFailingLogs {
        /// `(run_number, job_log)`.
        logs: HashMap<i64, String>,
    }

    impl crate::forge_log::ActionLogClient for ScriptedFailingLogs {
        fn run_logs(
            &self,
            _host: &str,
            _repo: &str,
            run_number: i64,
            _token: Option<&str>,
        ) -> crate::forge_log::RunLogsOutcome {
            let log = self.logs.get(&run_number).cloned().unwrap_or_default();
            crate::forge_log::RunLogsOutcome::Fetched(vec![crate::forge_log::JobLog {
                name: "test".to_string(),
                status: "failure".to_string(),
                log,
                ..Default::default()
            }])
        }
    }

    #[tokio::test]
    async fn worked_example_dedupes_by_failing_test_overlap_and_classifies_once() {
        // Runs 5845, 5850, 5859 and 5871 on
        // four branches, each failing `invitation_team_id::create_refuses_a_
        // foreign_tenant_team` plus other tests, produce one `Created` and
        // three attachments; the classifier runs exactly once (for the first,
        // creating run only - the three repeats attach with no triage at all).
        let pool = crate::db::test_pool().await;
        let backend = Arc::new(StoreBackend::new());
        let counting = Arc::new(CountingClassifier::new());
        let classifier: Arc<dyn FailureClassifier> = counting.clone();

        let branches = [
            "fix/ALPHA-1041-invoice-module-gate",
            "feat/ALPHA-1126-kb-provenance",
            "chore/ALPHA-1130-cleanup",
            "fix/ALPHA-1140-retry-logic",
        ];
        let runs = [5845, 5850, 5859, 5871];
        let mut logs = HashMap::new();
        for (run, branch) in runs.iter().zip(branches.iter()) {
            let mut log = failing_test_log(
                "invitation_team_id",
                "create_refuses_a_foreign_tenant_team",
                branch,
            );
            log.push_str(&format!(
                "     Running tests/other_{run}.rs (target/debug/deps/other_{run}-abc)\ntest some_other_test ... FAILED\n"
            ));
            logs.insert(*run, log);
        }

        for run in runs.iter() {
            let (tx, rx) = mpsc::channel(4);
            tx.send(store_event(*run)).await.unwrap();
            drop(tx);
            let out_dir = tempfile::tempdir().unwrap();
            consume_events(
                rx,
                pool.clone(),
                Arc::new(ScriptedFailingLogs { logs: logs.clone() }),
                out_dir.path().to_path_buf(),
                Some(store_mint_context(
                    pool.clone(),
                    backend.clone(),
                    classifier.clone(),
                )),
                Arc::new(RwLock::new(WebhookConfig::default())),
                crate::concurrency::KeyGate::new(),
            )
            .await;
        }
        let calls = backend.calls();
        let creates: Vec<_> = calls.iter().filter(|c| c.starts_with("create")).collect();
        assert_eq!(creates.len(), 1, "exactly one issue created: {calls:?}");
        let comments: Vec<_> = calls.iter().filter(|c| c.starts_with("comment")).collect();
        assert_eq!(
            comments.len(),
            3,
            "the three repeats each attach with one comment: {calls:?}"
        );
        assert_eq!(counting.call_count(), 1, "the classifier ran exactly once");
    }

    /// A mint run's triage classification cost is not thrown away.
    /// It lands on the run's own `job_outcomes` row, advances the
    /// `webhook-forgejo` daily ledger, and the `logs/usage.jsonl` record for the
    /// delivery carries the run's own id rather than `null`.
    #[tokio::test]
    async fn classification_cost_reaches_the_outcome_row_the_daily_ledger_and_the_usage_log() {
        let pool = crate::db::test_pool().await;
        let backend = Arc::new(StoreBackend::new());
        let classifier: Arc<dyn FailureClassifier> =
            Arc::new(CostingClassifier::new(TriageBucket::CodeFix, 0.42));
        let out_dir = tempfile::tempdir().unwrap();
        let paths_dir = tempfile::tempdir().unwrap();

        let mut logs = HashMap::new();
        logs.insert(1, failing_test_log("mod_a", "test_one", "main"));
        let (tx, rx) = mpsc::channel(4);
        tx.send(store_event(1)).await.unwrap();
        drop(tx);

        consume_events(
            rx,
            pool.clone(),
            Arc::new(ScriptedFailingLogs { logs }),
            out_dir.path().to_path_buf(),
            Some(store_mint_context_in(
                pool.clone(),
                backend,
                classifier,
                paths_dir.path(),
                Arc::new(crate::provider::cli::testutil::StubRunner::new()),
            )),
            Arc::new(RwLock::new(WebhookConfig::default())),
            crate::concurrency::KeyGate::new(),
        )
        .await;

        let (run_id,): (i64,) = sqlx::query_as(&crate::db::q(&pool, "SELECT id FROM runs"))
            .fetch_one(&pool)
            .await
            .unwrap();

        let (claude_ran, claude_ok, claude_cost_usd): (i64, i64, f64) =
            sqlx::query_as(&crate::db::q(
                &pool,
                "SELECT claude_ran, claude_ok, claude_cost_usd FROM job_outcomes",
            ))
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(claude_ran, 1, "claude_ran must reflect the classification");
        assert_eq!(claude_ok, 1, "claude_ok must reflect the classification");
        assert_eq!(
            claude_cost_usd, 0.42,
            "the outcome must carry the real cost"
        );

        let daily = state::daily_cost_get(&pool, WEBHOOK_MINT_ACTION)
            .await
            .unwrap();
        assert_eq!(
            daily, 0.42,
            "the daily ledger must advance by the run's cost"
        );

        let usage_path = test_paths(paths_dir.path()).usage_log();
        let usage_contents = std::fs::read_to_string(&usage_path).unwrap();
        let record: serde_json::Value =
            serde_json::from_str(usage_contents.lines().next().unwrap()).unwrap();
        assert_eq!(
            record["run_id"].as_i64(),
            Some(run_id),
            "the usage record must carry the mint run's own id, not null: {record}"
        );
    }

    /// The deterministic rules pass (no classifier call at all)
    /// still records zero cost rather than leaving the outcome row stale.
    #[tokio::test]
    async fn deterministic_autofix_short_circuit_records_zero_cost() {
        let pool = crate::db::test_pool().await;
        let backend = Arc::new(StoreBackend::new());
        let counting = Arc::new(CountingClassifier::new());
        let classifier: Arc<dyn FailureClassifier> = counting.clone();
        let out_dir = tempfile::tempdir().unwrap();

        let mut logs = HashMap::new();
        // An infra signature routes to bucket 3 by the deterministic rules pass
        // alone; the classifier never runs, so no cost is ever recorded.
        logs.insert(1, "error: no runner available for this job".to_string());
        let (tx, rx) = mpsc::channel(4);
        tx.send(store_event(1)).await.unwrap();
        drop(tx);

        consume_events(
            rx,
            pool.clone(),
            Arc::new(ScriptedFailingLogs { logs }),
            out_dir.path().to_path_buf(),
            Some(store_mint_context(pool.clone(), backend, classifier)),
            Arc::new(RwLock::new(WebhookConfig::default())),
            crate::concurrency::KeyGate::new(),
        )
        .await;

        assert_eq!(counting.call_count(), 0, "the classifier never ran");
        let (claude_ran, claude_cost_usd): (i64, f64) = sqlx::query_as(&crate::db::q(
            &pool,
            "SELECT claude_ran, claude_cost_usd FROM job_outcomes",
        ))
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(claude_ran, 0, "no invocation happened");
        assert_eq!(claude_cost_usd, 0.0, "no invocation means no cost");
        let daily = state::daily_cost_get(&pool, WEBHOOK_MINT_ACTION)
            .await
            .unwrap();
        assert_eq!(daily, 0.0, "the daily ledger must not move with no spend");
    }

    /// A mint run whose classification alone exceeds the action's
    /// daily budget stops with `exit::OVER_BUDGET`, not the generic
    /// `exit::UNEXPECTED` every other failure used to share.
    #[tokio::test]
    async fn a_classification_that_exceeds_the_budget_stops_with_over_budget() {
        let pool = crate::db::test_pool().await;
        let backend = Arc::new(StoreBackend::new());
        let classifier: Arc<dyn FailureClassifier> =
            Arc::new(CostingClassifier::new(TriageBucket::CodeFix, 5.00));
        let out_dir = tempfile::tempdir().unwrap();
        let paths_dir = tempfile::tempdir().unwrap();

        let mut logs = HashMap::new();
        logs.insert(1, failing_test_log("mod_a", "test_one", "main"));
        let (tx, rx) = mpsc::channel(4);
        tx.send(store_event(1)).await.unwrap();
        drop(tx);

        let mut ctx = store_mint_context_in(
            pool.clone(),
            backend,
            classifier,
            paths_dir.path(),
            Arc::new(crate::provider::cli::testutil::StubRunner::new()),
        );
        ctx.defaults = RunnerDefaults {
            daily_budget_usd: 1.00,
            ..RunnerDefaults::default()
        };

        consume_events(
            rx,
            pool.clone(),
            Arc::new(ScriptedFailingLogs { logs }),
            out_dir.path().to_path_buf(),
            Some(ctx),
            Arc::new(RwLock::new(WebhookConfig::default())),
            crate::concurrency::KeyGate::new(),
        )
        .await;

        let (exit,): (Option<i64>,) =
            sqlx::query_as(&crate::db::q(&pool, "SELECT exit_code FROM runs"))
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(
            exit,
            Some(crate::exit::OVER_BUDGET as i64),
            "a spend-ceiling stop must not read as exit::UNEXPECTED"
        );
    }

    /// A classifier that sleeps `delay` while classifying, tracking how many
    /// calls overlap (`peak`) so a test can observe the worker pool's actual
    /// concurrency rather than inferring it from timing.
    struct SlowClassifier {
        delay: Duration,
        live: std::sync::atomic::AtomicUsize,
        peak: std::sync::atomic::AtomicUsize,
        calls: std::sync::atomic::AtomicUsize,
    }

    impl SlowClassifier {
        fn new(delay: Duration) -> Self {
            Self {
                delay,
                live: std::sync::atomic::AtomicUsize::new(0),
                peak: std::sync::atomic::AtomicUsize::new(0),
                calls: std::sync::atomic::AtomicUsize::new(0),
            }
        }
        fn peak(&self) -> usize {
            self.peak.load(std::sync::atomic::Ordering::SeqCst)
        }
        fn calls(&self) -> usize {
            self.calls.load(std::sync::atomic::Ordering::SeqCst)
        }
    }

    impl FailureClassifier for SlowClassifier {
        fn classify(
            &self,
            _input: &TriageInput,
            _spend: &crate::usage::RunSpend,
        ) -> crate::error::Result<Triage> {
            use std::sync::atomic::Ordering::SeqCst;
            self.calls.fetch_add(1, SeqCst);
            let now = self.live.fetch_add(1, SeqCst) + 1;
            self.peak.fetch_max(now, SeqCst);
            std::thread::sleep(self.delay);
            self.live.fetch_sub(1, SeqCst);
            Ok(Triage {
                bucket: TriageBucket::CodeFix,
                analysis: "slow analysis".to_string(),
                classified_by: ClassifiedBy::Llm,
                auto_fix: None,
            })
        }
    }

    /// Two events for two DIFFERENT jobs (distinct `workflow_id`, so the
    /// key gate never serializes them against each other), sent
    /// through one `consume_events` call. Returns the peak observed
    /// concurrent classification count for the given `analysis_concurrency`.
    async fn run_two_distinct_jobs_at_concurrency(analysis_concurrency: usize) -> usize {
        let pool = crate::db::test_pool().await;
        let backend = Arc::new(StoreBackend::new());
        let slow = Arc::new(SlowClassifier::new(Duration::from_millis(150)));
        let classifier: Arc<dyn FailureClassifier> = slow.clone();

        let mut logs = HashMap::new();
        logs.insert(1, failing_test_log("mod_a", "test_one", "main"));
        logs.insert(2, failing_test_log("mod_b", "test_two", "main"));

        let mut ev1 = store_event(1);
        ev1.workflow_id = "ci-one.yml".to_string();
        let mut ev2 = store_event(2);
        ev2.workflow_id = "ci-two.yml".to_string();

        let (tx, rx) = mpsc::channel(4);
        tx.send(ev1).await.unwrap();
        tx.send(ev2).await.unwrap();
        drop(tx);

        let cfg = WebhookConfig {
            analysis_concurrency,
            ..WebhookConfig::default()
        };
        consume_events(
            rx,
            pool.clone(),
            Arc::new(ScriptedFailingLogs { logs }),
            tempfile::tempdir().unwrap().path().to_path_buf(),
            Some(store_mint_context(pool, backend, classifier)),
            Arc::new(RwLock::new(cfg)),
            crate::concurrency::KeyGate::new(),
        )
        .await;
        slow.peak()
    }

    /// A queued failure event does no fetch, triage, or tracker
    /// call while globally paused or while `webhook-forgejo` is paused, and is
    /// processed again once neither is set.
    #[tokio::test]
    async fn queued_failure_event_is_skipped_while_paused() {
        for scope in [None, Some(WEBHOOK_MINT_ACTION), Some("webhook-comment")] {
            for paused in [true, false] {
                let pool = crate::db::test_pool().await;
                if paused {
                    match scope {
                        None => state::global_pause_set(&pool, Some("t")).await.unwrap(),
                        Some(a) => state::action_pause_set(&pool, a, Some("t")).await.unwrap(),
                    }
                }
                // Only the global pause and this listener's own action stop it.
                let expect_skip = paused && scope != Some("webhook-comment");
                let backend = Arc::new(StoreBackend::new());
                let counting = Arc::new(CountingClassifier::new());
                let classifier: Arc<dyn FailureClassifier> = counting.clone();
                let mut logs = HashMap::new();
                logs.insert(1, failing_test_log("mod_a", "test_one", "main"));
                let (tx, rx) = mpsc::channel(4);
                tx.send(store_event(1)).await.unwrap();
                drop(tx);
                consume_events(
                    rx,
                    pool.clone(),
                    Arc::new(ScriptedFailingLogs { logs }),
                    tempfile::tempdir().unwrap().path().to_path_buf(),
                    Some(store_mint_context(
                        pool.clone(),
                        backend.clone(),
                        classifier,
                    )),
                    Arc::new(RwLock::new(WebhookConfig::default())),
                    crate::concurrency::KeyGate::new(),
                )
                .await;
                if expect_skip {
                    assert_eq!(counting.call_count(), 0, "scope {scope:?}");
                    assert!(backend.calls().is_empty(), "{:?}", backend.calls());
                } else {
                    assert!(counting.call_count() > 0, "scope {scope:?} paused {paused}");
                }
            }
        }
    }

    /// AC 5: with `analysis_concurrency = 2`, a slow stub shows a second event
    /// starting before the first finishes.
    #[tokio::test]
    async fn analysis_concurrency_two_overlaps_two_distinct_jobs() {
        assert_eq!(
            run_two_distinct_jobs_at_concurrency(2).await,
            2,
            "both distinct-job events analyze concurrently under a pool of 2"
        );
    }

    /// AC 5: with `analysis_concurrency = 1`, the second waits for the first.
    #[tokio::test]
    async fn analysis_concurrency_one_serializes_distinct_jobs() {
        assert_eq!(
            run_two_distinct_jobs_at_concurrency(1).await,
            1,
            "a pool of 1 never runs two analyses at once"
        );
    }

    /// AC 5 (racing duplicates): two concurrent failures of the SAME job
    /// (same repo/workflow_id/job_name) with an overlapping failing test,
    /// submitted together into one `consume_events` call with a slow
    /// classifier so they would genuinely overlap without the key gate.
    /// Exactly one analysis runs and exactly one issue is created; the second
    /// event's store lookup (after it waits its turn on the gate) finds the
    /// first's row and attaches instead of re-analyzing.
    #[tokio::test]
    async fn concurrent_failures_of_the_same_job_produce_one_issue_and_one_analysis() {
        let pool = crate::db::test_pool().await;
        let backend = Arc::new(StoreBackend::new());
        let slow = Arc::new(SlowClassifier::new(Duration::from_millis(150)));
        let classifier: Arc<dyn FailureClassifier> = slow.clone();

        let mut logs = HashMap::new();
        logs.insert(10, failing_test_log("shared_mod", "shared_test", "fix/a"));
        logs.insert(11, failing_test_log("shared_mod", "shared_test", "fix/b"));

        let (tx, rx) = mpsc::channel(4);
        tx.send(store_event(10)).await.unwrap();
        tx.send(store_event(11)).await.unwrap();
        drop(tx);

        let cfg = WebhookConfig {
            analysis_concurrency: 2,
            max_runs_per_day: 20,
            ..WebhookConfig::default()
        };
        consume_events(
            rx,
            pool.clone(),
            Arc::new(ScriptedFailingLogs { logs }),
            tempfile::tempdir().unwrap().path().to_path_buf(),
            Some(store_mint_context(pool, backend.clone(), classifier)),
            Arc::new(RwLock::new(cfg)),
            crate::concurrency::KeyGate::new(),
        )
        .await;

        assert_eq!(slow.calls(), 1, "exactly one analysis runs");
        let calls = backend.calls();
        let creates: Vec<_> = calls.iter().filter(|c| c.starts_with("create")).collect();
        assert_eq!(creates.len(), 1, "exactly one issue created: {calls:?}");
        let comments: Vec<_> = calls.iter().filter(|c| c.starts_with("comment")).collect();
        assert_eq!(comments.len(), 1, "the second attaches: {calls:?}");
    }

    #[tokio::test]
    async fn a_failure_sharing_a_test_with_a_resolved_issue_reopens_it() {
        // A shared test with a RESOLVED
        // issue reopens it (a state write plus a comment) and creates none.
        let pool = crate::db::test_pool().await;
        let now = "2026-09-01T00:00:00Z";
        let failure_id = ci_failure_store::record_failure(
            &pool,
            "acme/widgets",
            "ci.yml",
            "test",
            None,
            Some("CLAUDE-40"),
            "resolved",
            now,
        )
        .await
        .unwrap();
        ci_failure_store::add_tests(
            &pool,
            failure_id,
            &["invitation_team_id::create_refuses_a_foreign_tenant_team".to_string()],
        )
        .await
        .unwrap();

        let backend = Arc::new(StoreBackend::new());
        let classifier: Arc<dyn FailureClassifier> = Arc::new(CountingClassifier::new());
        let ctx = store_mint_context(pool.clone(), backend.clone(), classifier);
        let ev = store_event(6001);
        let log = failing_test_log(
            "invitation_team_id",
            "create_refuses_a_foreign_tenant_team",
            "main",
        );
        let result = triage_and_mint(
            &ctx,
            None,
            &ev,
            log,
            true,
            None,
            None,
            true,
            "test".to_string(),
            None,
            &crate::concurrency::KeyGate::new(),
            &WebhookConfig::default(),
            &test_spend(),
        )
        .await;
        assert!(result.ok, "{}", result.text);
        assert!(result.text.contains("CLAUDE-40"), "{}", result.text);

        let calls = backend.calls();
        assert!(
            !calls.iter().any(|c| c.starts_with("create")),
            "no new issue: {calls:?}"
        );
        assert!(
            calls.iter().any(|c| c == "set_state CLAUDE-40 To do"),
            "{calls:?}"
        );
        assert!(
            calls
                .iter()
                .any(|c| c.starts_with("comment CLAUDE-40 | Regression")),
            "{calls:?}"
        );
        assert!(
            calls
                .iter()
                .any(|c| c == "set_field CLAUDE-40 AI Agent Queued"),
            "AI Agent set exactly as a fresh code_fix issue would be: {calls:?}"
        );

        let failure = ci_failure_store::get_failure(&pool, failure_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(failure.state, "open");
    }

    /// An `open` store row whose issue's live
    /// YouTrack `State` is NOT the workflow's resolved state attaches exactly
    /// as it did before.
    #[tokio::test]
    async fn open_match_still_at_an_unresolved_state_attaches() {
        let pool = crate::db::test_pool().await;
        let now = "2026-09-01T00:00:00Z";
        let failure_id = ci_failure_store::record_failure(
            &pool,
            "acme/widgets",
            "ci.yml",
            "test",
            None,
            Some("CLAUDE-41"),
            "open",
            now,
        )
        .await
        .unwrap();
        ci_failure_store::add_tests(&pool, failure_id, &["mod_a::test_one".to_string()])
            .await
            .unwrap();

        let backend = Arc::new(StoreBackend::new().with_state_field("In Progress"));
        let classifier: Arc<dyn FailureClassifier> = Arc::new(CountingClassifier::new());
        let ctx = store_mint_context(pool.clone(), backend.clone(), classifier);
        let ev = store_event(6100);
        let log = failing_test_log("mod_a", "test_one", "main");
        let result = triage_and_mint(
            &ctx,
            None,
            &ev,
            log,
            true,
            None,
            None,
            true,
            "test".to_string(),
            None,
            &crate::concurrency::KeyGate::new(),
            &WebhookConfig::default(),
            &test_spend(),
        )
        .await;
        assert!(result.ok, "{}", result.text);
        let calls = backend.calls();
        assert!(
            calls.iter().any(|c| c == "get_field CLAUDE-41 State"),
            "{calls:?}"
        );
        assert!(
            calls.iter().any(|c| c.starts_with("comment CLAUDE-41 |")),
            "attached with a comment: {calls:?}"
        );
        assert!(
            !calls.iter().any(|c| c.starts_with("set_state")),
            "no regression reopen: {calls:?}"
        );

        let failure = ci_failure_store::get_failure(&pool, failure_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(failure.state, "open");
    }

    /// An `open` store row whose issue's live
    /// YouTrack `State` already equals the workflow's resolved state (the
    /// `docs/yt-workflow-resolve-on-pr-merge.js` workflow already closed it on
    /// merge) is a regression: reopen instead of attaching.
    #[tokio::test]
    async fn open_match_already_resolved_in_youtrack_reopens_as_a_regression() {
        let pool = crate::db::test_pool().await;
        let now = "2026-09-01T00:00:00Z";
        let failure_id = ci_failure_store::record_failure(
            &pool,
            "acme/widgets",
            "ci.yml",
            "test",
            None,
            Some("CLAUDE-42"),
            "open",
            now,
        )
        .await
        .unwrap();
        ci_failure_store::add_tests(&pool, failure_id, &["mod_a::test_one".to_string()])
            .await
            .unwrap();

        let backend = Arc::new(StoreBackend::new().with_state_field("Done"));
        let classifier: Arc<dyn FailureClassifier> = Arc::new(CountingClassifier::new());
        let ctx = store_mint_context(pool.clone(), backend.clone(), classifier);
        let ev = store_event(6101);
        let log = failing_test_log("mod_a", "test_one", "main");
        let result = triage_and_mint(
            &ctx,
            None,
            &ev,
            log,
            true,
            None,
            None,
            true,
            "test".to_string(),
            None,
            &crate::concurrency::KeyGate::new(),
            &WebhookConfig::default(),
            &test_spend(),
        )
        .await;
        assert!(result.ok, "{}", result.text);
        let calls = backend.calls();
        assert!(
            calls.iter().any(|c| c == "get_field CLAUDE-42 State"),
            "{calls:?}"
        );
        assert!(
            calls
                .iter()
                .any(|c| c.starts_with("comment CLAUDE-42 | Regression")),
            "{calls:?}"
        );
        assert!(
            calls.iter().any(|c| c.starts_with("set_state CLAUDE-42")),
            "regression reopen writes State: {calls:?}"
        );

        let failure = ci_failure_store::get_failure(&pool, failure_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(failure.state, "open");
    }

    /// A failed live-`State` read never
    /// silently attaches; it fails the run and leaves no comment behind.
    #[tokio::test]
    async fn open_match_state_read_failure_fails_the_run_with_no_comment() {
        let pool = crate::db::test_pool().await;
        let now = "2026-09-01T00:00:00Z";
        let failure_id = ci_failure_store::record_failure(
            &pool,
            "acme/widgets",
            "ci.yml",
            "test",
            None,
            Some("CLAUDE-43"),
            "open",
            now,
        )
        .await
        .unwrap();
        ci_failure_store::add_tests(&pool, failure_id, &["mod_a::test_one".to_string()])
            .await
            .unwrap();

        let backend = Arc::new(StoreBackend::new().with_state_field_error("yt: not found"));
        let classifier: Arc<dyn FailureClassifier> = Arc::new(CountingClassifier::new());
        let ctx = store_mint_context(pool.clone(), backend.clone(), classifier);
        let ev = store_event(6102);
        let log = failing_test_log("mod_a", "test_one", "main");
        let result = triage_and_mint(
            &ctx,
            None,
            &ev,
            log,
            true,
            None,
            None,
            true,
            "test".to_string(),
            None,
            &crate::concurrency::KeyGate::new(),
            &WebhookConfig::default(),
            &test_spend(),
        )
        .await;
        assert!(!result.ok, "a failed state read must fail the run");
        let calls = backend.calls();
        assert!(
            !calls.iter().any(|c| c.starts_with("comment")),
            "no comment on a failed read: {calls:?}"
        );
        assert!(
            !calls.iter().any(|c| c.starts_with("set_state")),
            "no reopen on a failed read: {calls:?}"
        );

        let failure = ci_failure_store::get_failure(&pool, failure_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            failure.state, "open",
            "store row untouched on a failed read"
        );
    }

    #[tokio::test]
    async fn a_failure_with_no_shared_test_creates_a_second_issue() {
        // A failure on the same job with no
        // shared test creates a second issue.
        let pool = crate::db::test_pool().await;
        let now = "2026-09-01T00:00:00Z";
        let failure_id = ci_failure_store::record_failure(
            &pool,
            "acme/widgets",
            "ci.yml",
            "test",
            None,
            Some("CLAUDE-50"),
            "open",
            now,
        )
        .await
        .unwrap();
        ci_failure_store::add_tests(&pool, failure_id, &["other::already_tracked".to_string()])
            .await
            .unwrap();

        let backend = Arc::new(StoreBackend::new());
        let classifier: Arc<dyn FailureClassifier> = Arc::new(CountingClassifier::new());
        let ctx = store_mint_context(pool.clone(), backend.clone(), classifier);
        let ev = store_event(6002);
        let log = failing_test_log(
            "invitation_team_id",
            "create_refuses_a_foreign_tenant_team",
            "main",
        );
        let result = triage_and_mint(
            &ctx,
            None,
            &ev,
            log,
            true,
            None,
            None,
            true,
            "test".to_string(),
            None,
            &crate::concurrency::KeyGate::new(),
            &WebhookConfig::default(),
            &test_spend(),
        )
        .await;
        assert!(result.ok, "{}", result.text);
        let calls = backend.calls();
        assert!(
            calls.iter().any(|c| c.starts_with("create")),
            "a distinct failing test mints a second issue: {calls:?}"
        );
    }

    #[tokio::test]
    async fn a_legacy_issue_found_by_marker_search_is_adopted_not_duplicated() {
        // An open legacy issue found by the
        // marker search, with no store row, is adopted and not duplicated.
        let pool = crate::db::test_pool().await;
        let backend = Arc::new(StoreBackend::new().with_legacy_match("CLAUDE-70"));
        let counting = Arc::new(CountingClassifier::new());
        let classifier: Arc<dyn FailureClassifier> = counting.clone();
        let ctx = store_mint_context(pool.clone(), backend.clone(), classifier);
        let ev = store_event(6003);
        let log = failing_test_log(
            "invitation_team_id",
            "create_refuses_a_foreign_tenant_team",
            "main",
        );
        let result = triage_and_mint(
            &ctx,
            None,
            &ev,
            log,
            true,
            None,
            None,
            true,
            "test".to_string(),
            None,
            &crate::concurrency::KeyGate::new(),
            &WebhookConfig::default(),
            &test_spend(),
        )
        .await;
        assert!(result.ok, "{}", result.text);
        assert!(result.text.contains("CLAUDE-70"), "{}", result.text);
        let calls = backend.calls();
        assert!(
            !calls.iter().any(|c| c.starts_with("create")),
            "adopted, not duplicated: {calls:?}"
        );
        assert_eq!(counting.call_count(), 0, "no triage for a legacy adoption");

        // The adoption is now recorded, so the next sighting of this test
        // attaches instead of searching YouTrack again.
        let m = ci_failure_store::find_match(
            &pool,
            "acme/widgets",
            "ci.yml",
            "test",
            &["invitation_team_id::create_refuses_a_foreign_tenant_team".to_string()],
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(m.issue_key, Some("CLAUDE-70".to_string()));
        assert_eq!(m.state, "open");
    }

    #[tokio::test]
    async fn a_ci_failure_store_read_error_fails_the_run_and_mints_nothing() {
        // A store read failure logs at error and fails the run rather than
        // falling through to a mint, which would recreate a duplicate.
        let pool = crate::db::test_pool().await;
        pool.close().await;
        let backend = Arc::new(StoreBackend::new());
        let classifier: Arc<dyn FailureClassifier> = Arc::new(CountingClassifier::new());
        let ctx = store_mint_context(pool.clone(), backend.clone(), classifier);
        let ev = store_event(9001);
        let log = failing_test_log(
            "invitation_team_id",
            "create_refuses_a_foreign_tenant_team",
            "main",
        );
        let result = triage_and_mint(
            &ctx,
            None,
            &ev,
            log,
            true,
            None,
            None,
            true,
            "test".to_string(),
            None,
            &crate::concurrency::KeyGate::new(),
            &WebhookConfig::default(),
            &test_spend(),
        )
        .await;
        assert!(!result.ok, "a store read failure must fail the run");
        assert!(
            result.text.contains("ci-failure store error"),
            "{}",
            result.text
        );
        assert!(
            backend.calls().is_empty(),
            "nothing minted on a store error: {:?}",
            backend.calls()
        );
    }

    fn event() -> VerifiedFailureEvent {
        VerifiedFailureEvent {
            repo: "fabrikam/forgejo-cli".to_string(),
            index_in_repo: 7,
            commit_sha: "abc123def456".to_string(),
            workflow_id: "ci.yml".to_string(),
            private: false,
            source: "test-source".to_string(),
            trigger_user: "alice".to_string(),
            html_url: "https://dev.a8n.run/fabrikam/forgejo-cli/actions/runs/7".to_string(),
            status: "failure".to_string(),
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

    fn route_cache() -> VcsRouteCache {
        VcsRouteCache::new(std::time::Duration::from_secs(900))
    }

    #[test]
    fn unavailable_log_mints_a_human_action_issue_without_the_classifier() {
        // log_available = false -> the deterministic rules route to
        // bucket 2 (a real failure must surface even without a log), the LLM
        // classifier never runs, and an issue IS minted with the reason in the
        // analysis.
        let classifier = StubClassifier::new(TriageBucket::CodeFix);
        let tracker = StubTracker::new();
        let (bucket, out) = decide_mint(
            &classifier,
            &tracker,
            &mint_cfg(),
            &route_cache(),
            &caps(),
            &event(),
            String::new(),
            false,
            None,
            Some("private repo: unauthorized".to_string()),
            false,
            true, // auto_fix_enabled
            "",
            None,
            &|_| {},
            &test_spend(),
        )
        .unwrap();
        assert_eq!(bucket, TriageBucket::HumanAction);
        assert!(
            matches!(
                out,
                MintOutcome::Created {
                    bucket: TriageBucket::HumanAction,
                    ..
                }
            ),
            "{out:?}"
        );
        assert_eq!(*classifier.calls.lock().unwrap(), 0, "rules short-circuit");
        assert!(
            tracker.calls().iter().any(|c| c.starts_with("create")),
            "{:?}",
            tracker.calls()
        );
    }

    #[test]
    fn transient_signature_in_log_mints_nothing() {
        // A clear infra signature is bucket 3 by the rules pass, even with a log.
        let classifier = StubClassifier::new(TriageBucket::CodeFix);
        let tracker = StubTracker::new();
        let (bucket, out) = decide_mint(
            &classifier,
            &tracker,
            &mint_cfg(),
            &route_cache(),
            &caps(),
            &event(),
            "error: no runner available for this job".to_string(),
            true,
            None,
            None,
            true,
            true, // auto_fix_enabled
            "",
            None,
            &|_| {},
            &test_spend(),
        )
        .unwrap();
        assert_eq!(bucket, TriageBucket::Transient);
        assert_eq!(out, MintOutcome::SkippedTransient);
        assert_eq!(*classifier.calls.lock().unwrap(), 0);
    }

    #[test]
    fn code_fix_classification_mints_and_queues() {
        // A non-transient log + a code_fix verdict with NO open PR -> issue in the
        // mapped project, queued for the fix bot, no human assignee. (An open PR
        // no longer suppresses this route inside `decide_mint`; see
        // `decide_mint_applies_no_open_pr_suppression_of_its_own`. Open-PR
        // routing is decided by the caller before `decide_mint` is reached.)
        let classifier = StubClassifier::new(TriageBucket::CodeFix);
        let tracker = StubTracker::new();
        let (bucket, out) = decide_mint(
            &classifier,
            &tracker,
            &mint_cfg(),
            &route_cache(),
            &caps(),
            &event(),
            "assertion failed: expected 1 got 2".to_string(),
            true,
            None,
            None,
            false, // has_open_pr: no PR, so the code_fix route still queues
            true,  // auto_fix_enabled
            "",
            None,
            &|_| {},
            &test_spend(),
        )
        .unwrap();
        assert_eq!(bucket, TriageBucket::CodeFix);
        assert_eq!(
            out,
            MintOutcome::Created {
                key: "CLAUDE-99".to_string(),
                project: "FJ".to_string(),
                bucket: TriageBucket::CodeFix,
                source: crate::mint::RouteSource::Override,
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
        let (bucket, out) = decide_mint(
            &classifier,
            &tracker,
            &mint_cfg(),
            &route_cache(),
            &caps(),
            &event(),
            "the release tag push to main failed".to_string(),
            true,
            None,
            None,
            false,
            true, // auto_fix_enabled
            "",
            None,
            &|_| {},
            &test_spend(),
        )
        .unwrap();
        assert_eq!(bucket, TriageBucket::HumanAction);
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
    fn cargo_fmt_push_to_main_routes_to_auto_fix_and_queues_without_the_llm() {
        // An end-to-end test through decide_mint: a check.yml run with NO open
        // PR (push to main) failing on `cargo fmt --check` is routed
        // deterministically to the auto-fix bucket - queued for the runner, no
        // human assignee - and the LLM classifier (which would say human_action)
        // is never consulted.
        let classifier = StubClassifier::new(TriageBucket::HumanAction);
        let tracker = StubTracker::new();
        let mut ev = event();
        ev.workflow_id = "check.yml".to_string();
        let (bucket, out) = decide_mint(
            &classifier,
            &tracker,
            &mint_cfg(),
            &route_cache(),
            &caps(),
            &ev,
            "Run cargo fmt --check\nDiff in /build/src/main.rs at line 1:\n".to_string(),
            true,
            None,
            None,
            false, // has_open_pr: push to main, no PR
            true,  // auto_fix_enabled
            "",
            None,
            &|_| {},
            &test_spend(),
        )
        .unwrap();
        assert_eq!(bucket, TriageBucket::AutoFix);
        assert!(
            matches!(
                out,
                MintOutcome::Created {
                    bucket: TriageBucket::AutoFix,
                    ..
                }
            ),
            "{out:?}"
        );
        assert_eq!(
            *classifier.calls.lock().unwrap(),
            0,
            "auto-fix short-circuits the LLM"
        );
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
    fn decide_mint_applies_no_open_pr_suppression_of_its_own() {
        // The open-PR routing decision (agent PR re-queue / human PR
        // comment) is made by the caller BEFORE `decide_mint` is ever
        // reached; `decide_mint` (and `mint::mint` underneath it) no longer
        // suppresses a code-fix mint for `has_open_pr: true` the way the old
        // `SkippedOpenPr` used to.
        let classifier = StubClassifier::new(TriageBucket::CodeFix);
        let tracker = StubTracker::new();
        let ev = event();
        let (bucket, out) = decide_mint(
            &classifier,
            &tracker,
            &mint_cfg(),
            &route_cache(),
            &caps(),
            &ev,
            "error[E0308]: mismatched types".to_string(),
            true,
            None,
            None,
            true, // has_open_pr: context only, no longer a suppression signal
            true, // auto_fix_enabled
            "",
            None,
            &|_| {},
            &test_spend(),
        )
        .unwrap();
        assert_eq!(bucket, TriageBucket::CodeFix);
        assert!(matches!(out, MintOutcome::Created { .. }), "{out:?}");
        let calls = tracker.calls();
        assert!(calls.iter().any(|c| c.starts_with("create")), "{calls:?}");
        assert!(
            calls
                .iter()
                .any(|c| c == "set_field CLAUDE-99 AI Agent Queued"),
            "{calls:?}"
        );
    }

    /// The collapse key the legacy adoption lookup
    /// searches, i.e. the `#Unresolved` full-text query
    /// `decide_with_legacy_adoption` issues before falling through to triage.
    fn collapse_query(log: &str) -> String {
        let classifier = StubClassifier::new(TriageBucket::HumanAction);
        let tracker = StubTracker::new();
        let failure_signature = crate::mint::failure_signature(log);
        let branch = crate::forge_log::resolve_branch(None, log).unwrap_or_default();
        let infra_kind = crate::infra_failure::detect(log).map(|f| f.kind);
        let _ = decide_with_legacy_adoption(
            &classifier,
            &tracker,
            &mint_cfg(),
            &route_cache(),
            &caps(),
            &event(),
            log.to_string(),
            true,
            None,
            None,
            false,
            true,
            "integration tests",
            &branch,
            failure_signature.as_deref(),
            infra_kind.as_deref(),
            None,
            &|_| {},
            &test_spend(),
        );
        tracker
            .calls()
            .into_iter()
            .rfind(|c| c.starts_with("list ") && c.ends_with("#Unresolved"))
            .expect("the legacy lookup issued a collapse search")
    }

    #[test]
    fn an_infra_failure_collapses_across_branches_but_a_code_failure_does_not() {
        // `infra_kind` is resolved from the same full
        // redacted log as `failure_signature`, and switches the legacy
        // adoption lookup's collapse search onto the branch-agnostic key. Two
        // runs of one job on two different PR branches: with a host condition
        // in the log they search the SAME key, and without one they search
        // different keys, exactly as before.
        let a = "GITHUB_HEAD_REF=fix/ALPHA-1041-invoice-module-gate\n";
        let b = "GITHUB_HEAD_REF=feat/ALPHA-1126-kb-provenance\n";
        let guard = "Insufficient free space: the suite needs ~25G to link test binaries.\n";
        assert_eq!(
            collapse_query(&format!("{a}{guard}")),
            collapse_query(&format!("{b}{guard}")),
            "one host condition on one job is one key, whatever the branch"
        );
        // The control: the same two branches with an ordinary code failure keep
        // their per-branch identity (and so their per-branch auto-fix routing).
        let bug = "error[E0308]: mismatched types\n";
        assert_ne!(
            collapse_query(&format!("{a}{bug}")),
            collapse_query(&format!("{b}{bug}")),
            "a code failure still keys per branch"
        );
    }

    #[test]
    fn auto_fix_off_code_fix_still_creates_but_leaves_it_on_the_pr() {
        // An end-to-end test through decide_mint: with the webhook
        // auto-fix toggle OFF, a code_fix failure (even with no open PR) still
        // creates an issue, but queues no fixup PR (no `AI Agent` write), so the
        // red check stays reported on the PR.
        let classifier = StubClassifier::new(TriageBucket::CodeFix);
        let tracker = StubTracker::new();
        let (bucket, out) = decide_mint(
            &classifier,
            &tracker,
            &mint_cfg(),
            &route_cache(),
            &caps(),
            &event(),
            "error[E0308]: mismatched types".to_string(),
            true,
            None,
            None,
            false, // has_open_pr: no PR; the toggle, not open-PR, does the gating
            false, // auto_fix_enabled: OFF
            "",
            None,
            &|_| {},
            &test_spend(),
        )
        .unwrap();
        assert_eq!(bucket, TriageBucket::CodeFix);
        assert!(matches!(out, MintOutcome::Created { .. }), "{out:?}");
        let calls = tracker.calls();
        assert!(
            calls.iter().any(|c| c.starts_with("create")),
            "issue still minted when auto-fix is off: {calls:?}"
        );
        assert!(
            !calls.iter().any(|c| c.contains("AI Agent")),
            "fix bot never queued when auto-fix is off: {calls:?}"
        );
    }

    #[test]
    fn pr_matches_commit_handles_exact_and_prefix() {
        let pr = |sha: &str| ForgePullRequest {
            number: 1,
            title: String::new(),
            head_sha: sha.to_string(),
            head_ref: String::new(),
            base_ref: String::new(),
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

    // ---- Webhook runs are recorded ----------------------------

    /// Canned [`ActionLogClient`] returning one failing job, so the fetch is
    /// `Fetched` on the first attempt and the backoff loop never sleeps.
    struct ReadyLogs;
    impl crate::forge_log::ActionLogClient for ReadyLogs {
        fn run_logs(
            &self,
            _host: &str,
            _repo: &str,
            _run_number: i64,
            _token: Option<&str>,
        ) -> crate::forge_log::RunLogsOutcome {
            crate::forge_log::RunLogsOutcome::Fetched(vec![crate::forge_log::JobLog {
                name: "check".to_string(),
                status: "failure".to_string(),
                log: "assertion failed: expected 1 got 2".to_string(),
                ..Default::default()
            }])
        }
    }

    async fn memory_pool() -> DbPool {
        crate::db::test_pool().await
    }

    /// A client that fails the test on any call, proving the
    /// skipped-run short-circuit never attempts a log fetch.
    struct PanicLogs;
    impl crate::forge_log::ActionLogClient for PanicLogs {
        fn run_logs(
            &self,
            _host: &str,
            repo: &str,
            run_number: i64,
            _token: Option<&str>,
        ) -> crate::forge_log::RunLogsOutcome {
            panic!("log fetch attempted for a non-failure run: {repo}#{run_number}");
        }
    }

    #[tokio::test]
    async fn non_failure_statuses_finish_without_a_log_fetch() {
        // Forgejo fires action_run_failure for every non-success
        // done status (bare `else` in ActionRunNowDone), so `skipped` (server-
        // side skip, no log will ever exist) and `cancelled` (deliberately
        // stopped) must both bypass the fetch + triage instead of burning the
        // 60s retry budget or minting an issue.
        for status in ["skipped", "cancelled"] {
            let pool = memory_pool().await;
            let out_dir = tempfile::tempdir().unwrap();
            let (tx, rx) = mpsc::channel(4);
            let mut ev = event();
            ev.status = status.to_string();
            tx.send(ev).await.unwrap();
            drop(tx);

            consume_events(
                rx,
                pool.clone(),
                Arc::new(PanicLogs),
                out_dir.path().to_path_buf(),
                None,
                Arc::new(RwLock::new(WebhookConfig::default())),
                crate::concurrency::KeyGate::new(),
            )
            .await;

            let (jobs, exit): (i64, Option<i64>) = sqlx::query_as(&crate::db::q(
                &pool,
                "SELECT jobs_processed, exit_code FROM runs",
            ))
            .fetch_one(&pool)
            .await
            .unwrap();
            assert_eq!(jobs, 1, "{status}");
            assert_eq!(exit, Some(crate::exit::OK as i64), "{status}");

            // `ok` is an integer 0/1 column on both backends (see `crate::db::flag`).
            let (ok, summary): (i64, Option<String>) =
                sqlx::query_as(&crate::db::q(&pool, "SELECT ok, summary FROM job_outcomes"))
                    .fetch_one(&pool)
                    .await
                    .unwrap();
            let ok = ok != 0;
            assert!(ok, "{status}");
            let summary = summary.expect("summary recorded");
            assert!(summary.contains(status), "{status}: {summary}");
            assert!(summary.contains("no issue"), "{status}: {summary}");
        }
    }

    #[test]
    fn verified_failure_event_carries_the_run_status() {
        // A delivery with action=failure but a non-failure run
        // status (cancelled / skipped flavors) surfaces that status on the
        // event so the consumer can route on it.
        let body = failure_payload("contoso/claude-run")
            .replace(r#""status":"failure""#, r#""status":"skipped""#);
        let payload: ActionPayload = serde_json::from_str(&body).unwrap();
        let allowed = vec!["contoso/claude-run".to_string()];
        let event = verified_failure_event(&payload, &allowed, "test-source").expect("emits event");
        assert_eq!(event.status, "skipped");
    }

    #[test]
    fn verified_failure_event_carries_repo_visibility() {
        // `repository.private` rides the payload onto the event; a
        // payload without the field (older/foreign forge) defaults to public.
        let body = failure_payload("contoso/claude-run").replace(
            r#""repository":{"full_name":"contoso/claude-run"}"#,
            r#""repository":{"full_name":"contoso/claude-run","private":true}"#,
        );
        let payload: ActionPayload = serde_json::from_str(&body).unwrap();
        let allowed = vec!["contoso/claude-run".to_string()];
        let event = verified_failure_event(&payload, &allowed, "test-source").expect("emits event");
        assert!(event.private, "private flag carried");

        let payload: ActionPayload =
            serde_json::from_str(&failure_payload("contoso/claude-run")).unwrap();
        let event = verified_failure_event(&payload, &allowed, "test-source").expect("emits event");
        assert!(!event.private, "absent field defaults to public");
    }

    /// One recorded `run_logs` call: `(host, repo, run_number, token)`.
    type LoggedCall = (String, String, i64, Option<String>);

    /// A scripted `fj actions logs` client recording every call's
    /// arguments, so the tests assert the run number, the token scoping, and the
    /// call count without spawning a process.
    struct ScriptedLogs {
        outcome: crate::forge_log::RunLogsOutcome,
        calls: std::sync::Mutex<Vec<LoggedCall>>,
    }

    impl ScriptedLogs {
        fn new(outcome: crate::forge_log::RunLogsOutcome) -> Self {
            Self {
                outcome,
                calls: std::sync::Mutex::new(Vec::new()),
            }
        }
    }

    impl crate::forge_log::ActionLogClient for ScriptedLogs {
        fn run_logs(
            &self,
            host: &str,
            repo: &str,
            run_number: i64,
            token: Option<&str>,
        ) -> crate::forge_log::RunLogsOutcome {
            self.calls.lock().unwrap().push((
                host.to_string(),
                repo.to_string(),
                run_number,
                token.map(str::to_string),
            ));
            self.outcome.clone()
        }
    }

    fn one_failing_job() -> crate::forge_log::RunLogsOutcome {
        crate::forge_log::RunLogsOutcome::Fetched(vec![crate::forge_log::JobLog {
            name: "check".to_string(),
            status: "failure".to_string(),
            log: "assertion failed: expected 1 got 2\n".to_string(),
            ..Default::default()
        }])
    }

    /// Shared scaffolding for the log-token consume tests: age key file, stored
    /// `log-token` secret, and a webhook config whose `test-source` (the
    /// `event()` fixture's source) carries the `log_token_ref`.
    async fn log_token_setup(tmp: &std::path::Path) -> (DbPool, Arc<RwLock<WebhookConfig>>) {
        use age::secrecy::ExposeSecret;
        let key_path = tmp.join("key.txt");
        let identity = age::x25519::Identity::generate();
        std::fs::write(&key_path, identity.to_string().expose_secret()).unwrap();
        std::env::set_var(crate::secrets::KEY_FILE_ENV, &key_path);

        let pool = memory_pool().await;
        let key = SecretsKey::load().unwrap();
        let ciphertext = key.encrypt("test-pat").unwrap();
        state::secret_create(&pool, "log-token", &ciphertext)
            .await
            .unwrap();

        let mut cfg = WebhookConfig::default();
        cfg.sources.insert(
            "test-source".to_string(),
            WebhookSource {
                log_token_ref: Some("log-token".to_string()),
                ..Default::default()
            },
        );
        (pool, Arc::new(RwLock::new(cfg)))
    }

    #[tokio::test]
    async fn public_and_private_repos_share_one_fetch_path() {
        // The public/private split is gone. Both visibilities reach
        // `fj actions logs` with the same arguments, addressed by the per-repo
        // run number, and the source's `log_token_ref` scopes both.
        for private in [false, true] {
            let _env = crate::secrets::key_file_env_lock().await;
            let tmp = tempfile::tempdir().unwrap();
            let (pool, cfg) = log_token_setup(tmp.path()).await;
            let logs = Arc::new(ScriptedLogs::new(one_failing_job()));

            let (tx, rx) = mpsc::channel(4);
            let mut ev = event();
            ev.private = private;
            tx.send(ev).await.unwrap();
            drop(tx);

            consume_events(
                rx,
                pool.clone(),
                logs.clone(),
                tmp.path().to_path_buf(),
                None,
                cfg,
                crate::concurrency::KeyGate::new(),
            )
            .await;

            let calls = logs.calls.lock().unwrap().clone();
            assert_eq!(calls.len(), 1, "exactly one fetch (private={private})");
            assert_eq!(
                calls[0],
                (
                    "dev.a8n.run".to_string(),
                    "fabrikam/forgejo-cli".to_string(),
                    7,
                    Some("test-pat".to_string())
                ),
                "private={private}"
            );

            let summary: Option<String> =
                sqlx::query_scalar(&crate::db::q(&pool, "SELECT summary FROM job_outcomes"))
                    .fetch_one(&pool)
                    .await
                    .unwrap();
            let summary = summary.expect("summary recorded");
            assert!(
                summary.contains("log fetched and redacted"),
                "private={private}: {summary}"
            );
            assert!(summary.contains("log="), "artifact referenced: {summary}");
        }
    }

    #[tokio::test]
    async fn a_rejected_log_token_degrades_to_a_no_log_outcome() {
        // An auth denial is terminal after ONE call (no retry burn)
        // and the failure still surfaces through the no-log path.
        let _env = crate::secrets::key_file_env_lock().await;
        let tmp = tempfile::tempdir().unwrap();
        let (pool, cfg) = log_token_setup(tmp.path()).await;
        let logs = Arc::new(ScriptedLogs::new(
            crate::forge_log::RunLogsOutcome::AuthDenied("HTTP 403: no scope".to_string()),
        ));

        let (tx, rx) = mpsc::channel(4);
        let mut ev = event();
        ev.private = true;
        tx.send(ev).await.unwrap();
        drop(tx);

        consume_events(
            rx,
            pool.clone(),
            logs.clone(),
            tmp.path().to_path_buf(),
            None,
            cfg,
            crate::concurrency::KeyGate::new(),
        )
        .await;

        assert_eq!(logs.calls.lock().unwrap().len(), 1, "exactly one call");
        let summary: Option<String> =
            sqlx::query_scalar(&crate::db::q(&pool, "SELECT summary FROM job_outcomes"))
                .fetch_one(&pool)
                .await
                .unwrap();
        let summary = summary.expect("summary recorded");
        assert!(summary.contains("log unavailable"), "{summary}");
    }

    /// A `Fetched` outcome whose jobs carry `log_state` in run order.
    fn jobs_with_log_states(states: &[&str]) -> crate::forge_log::RunLogsOutcome {
        crate::forge_log::RunLogsOutcome::Fetched(
            states
                .iter()
                .enumerate()
                .map(|(i, state)| crate::forge_log::JobLog {
                    name: format!("job-{i}"),
                    status: "failure".to_string(),
                    log: if *state == crate::forge_log::LOG_STATE_MISSING {
                        String::new()
                    } else {
                        "cargo build\n".to_string()
                    },
                    log_state: (*state).to_string(),
                    log_reason: if *state == crate::forge_log::LOG_STATE_MISSING {
                        Some("open logs for task 43177: no such file or directory".to_string())
                    } else {
                        None
                    },
                })
                .collect(),
        )
    }

    #[tokio::test]
    async fn a_permanently_missing_log_degrades_to_a_no_log_outcome() {
        // An every-job `log_state: "missing"` must be read the
        // same way an unfetchable log is, so the human-action route
        // still fires instead of triaging an empty-but-"available" log.
        let pool = memory_pool().await;
        let out_dir = tempfile::tempdir().unwrap();
        let logs = Arc::new(ScriptedLogs::new(jobs_with_log_states(&[
            crate::forge_log::LOG_STATE_MISSING,
            crate::forge_log::LOG_STATE_MISSING,
        ])));

        let (tx, rx) = mpsc::channel(4);
        tx.send(event()).await.unwrap();
        drop(tx);

        consume_events(
            rx,
            pool.clone(),
            logs,
            out_dir.path().to_path_buf(),
            None,
            Arc::new(RwLock::new(WebhookConfig::default())),
            crate::concurrency::KeyGate::new(),
        )
        .await;

        let summary: Option<String> =
            sqlx::query_scalar(&crate::db::q(&pool, "SELECT summary FROM job_outcomes"))
                .fetch_one(&pool)
                .await
                .unwrap();
        let summary = summary.expect("summary recorded");
        assert!(summary.contains("log unavailable"), "{summary}");
    }

    #[tokio::test]
    async fn a_partially_readable_log_still_mints_as_available() {
        // At least one job carrying a readable log must be
        // unaffected by the missing-log predicate, even alongside a missing one.
        let pool = memory_pool().await;
        let out_dir = tempfile::tempdir().unwrap();
        let logs = Arc::new(ScriptedLogs::new(jobs_with_log_states(&[
            crate::forge_log::LOG_STATE_MISSING,
            "available",
        ])));

        let (tx, rx) = mpsc::channel(4);
        tx.send(event()).await.unwrap();
        drop(tx);

        consume_events(
            rx,
            pool.clone(),
            logs,
            out_dir.path().to_path_buf(),
            None,
            Arc::new(RwLock::new(WebhookConfig::default())),
            crate::concurrency::KeyGate::new(),
        )
        .await;

        let summary: Option<String> =
            sqlx::query_scalar(&crate::db::q(&pool, "SELECT summary FROM job_outcomes"))
                .fetch_one(&pool)
                .await
                .unwrap();
        let summary = summary.expect("summary recorded");
        assert!(summary.contains("log fetched and redacted"), "{summary}");
    }

    #[tokio::test]
    async fn a_pre_fj79_log_with_no_log_state_still_mints_as_available() {
        // An `fj` predating the log-state field emits no `log_state` field at
        // all, which deserializes to `""` on every job, never `"missing"`; that
        // absence must never be read as "gone".
        let pool = memory_pool().await;
        let out_dir = tempfile::tempdir().unwrap();
        let logs = Arc::new(ScriptedLogs::new(one_failing_job()));

        let (tx, rx) = mpsc::channel(4);
        tx.send(event()).await.unwrap();
        drop(tx);

        consume_events(
            rx,
            pool.clone(),
            logs,
            out_dir.path().to_path_buf(),
            None,
            Arc::new(RwLock::new(WebhookConfig::default())),
            crate::concurrency::KeyGate::new(),
        )
        .await;

        let summary: Option<String> =
            sqlx::query_scalar(&crate::db::q(&pool, "SELECT summary FROM job_outcomes"))
                .fetch_one(&pool)
                .await
                .unwrap();
        let summary = summary.expect("summary recorded");
        assert!(summary.contains("log fetched and redacted"), "{summary}");
    }

    /// A `Fetched` outcome whose jobs carry `statuses` in run order.
    fn jobs_with_statuses(statuses: &[&str]) -> crate::forge_log::RunLogsOutcome {
        crate::forge_log::RunLogsOutcome::Fetched(
            statuses
                .iter()
                .enumerate()
                .map(|(i, s)| crate::forge_log::JobLog {
                    name: format!("job-{i}"),
                    status: (*s).to_string(),
                    log: "cargo build\n".to_string(),
                    ..Default::default()
                })
                .collect(),
        )
    }

    #[tokio::test]
    async fn a_stopped_run_finishes_without_triage_or_mint() {
        // Run-level status is `failure` (so the earlier status guard lets
        // it through) but every job was cancelled or skipped, which is what a
        // superseding push produces. Nothing failed, so the consumer closes the
        // run naming the statuses and never reaches triage or mint.
        for statuses in [
            &["cancelled"][..],
            &["cancelled", "cancelled"][..],
            &["skipped"][..],
            &["cancelled", "skipped"][..],
        ] {
            let pool = memory_pool().await;
            let out_dir = tempfile::tempdir().unwrap();
            let logs = Arc::new(ScriptedLogs::new(jobs_with_statuses(statuses)));
            let (tx, rx) = mpsc::channel(4);
            tx.send(event()).await.unwrap();
            drop(tx);

            consume_events(
                rx,
                pool.clone(),
                logs.clone(),
                out_dir.path().to_path_buf(),
                None,
                Arc::new(RwLock::new(WebhookConfig::default())),
                crate::concurrency::KeyGate::new(),
            )
            .await;

            // `ok` is an integer 0/1 column on both backends (see `crate::db::flag`).
            let (ok, summary): (i64, Option<String>) =
                sqlx::query_as(&crate::db::q(&pool, "SELECT ok, summary FROM job_outcomes"))
                    .fetch_one(&pool)
                    .await
                    .unwrap();
            let ok = ok != 0;
            assert!(ok, "{statuses:?}");
            let summary = summary.expect("summary recorded");
            assert!(summary.contains("no job failed"), "{statuses:?}: {summary}");
            assert!(summary.contains("no issue"), "{statuses:?}: {summary}");
            for s in statuses {
                assert!(summary.contains(s), "{statuses:?}: {summary}");
            }
            // The short-circuit lands before the mint branch, so the log-only
            // summary that branch writes never appears.
            assert!(!summary.contains("log-only"), "{statuses:?}: {summary}");
        }
    }

    #[tokio::test]
    async fn a_run_that_is_not_stopped_still_reaches_the_mint_branch() {
        // Only the all-`{cancelled, skipped}` set short-circuits. A
        // failing job, an every-job-success run under a run-level failure, and a
        // run whose job list did not resolve all continue into triage / mint
        // (here log-only, since no mint context is configured).
        let cases: [(crate::forge_log::RunLogsOutcome, &str); 4] = [
            (jobs_with_statuses(&["cancelled", "failure"]), "failing job"),
            (jobs_with_statuses(&["success", "success"]), "all success"),
            (jobs_with_statuses(&[]), "empty job list"),
            (
                crate::forge_log::RunLogsOutcome::AuthDenied("HTTP 401".to_string()),
                "unresolved job list",
            ),
        ];
        for (outcome, label) in cases {
            let pool = memory_pool().await;
            let out_dir = tempfile::tempdir().unwrap();
            let (tx, rx) = mpsc::channel(4);
            tx.send(event()).await.unwrap();
            drop(tx);

            consume_events(
                rx,
                pool.clone(),
                Arc::new(ScriptedLogs::new(outcome)),
                out_dir.path().to_path_buf(),
                None,
                Arc::new(RwLock::new(WebhookConfig::default())),
                crate::concurrency::KeyGate::new(),
            )
            .await;

            let summary: Option<String> =
                sqlx::query_scalar(&crate::db::q(&pool, "SELECT summary FROM job_outcomes"))
                    .fetch_one(&pool)
                    .await
                    .unwrap();
            let summary = summary.expect("summary recorded");
            assert!(summary.contains("log-only"), "{label}: {summary}");
            assert!(!summary.contains("no job failed"), "{label}: {summary}");
        }
    }

    #[tokio::test]
    async fn consumed_event_records_a_webhook_run() {
        // Drive `consume_events` with one event and no mint context (log-only),
        // then assert the run + outcome rows landed under the synthetic action.
        let pool = memory_pool().await;
        let out_dir = tempfile::tempdir().unwrap();
        let (tx, rx) = mpsc::channel(4);
        tx.send(event()).await.unwrap();
        drop(tx); // close the channel so `consume_events` returns after draining.

        consume_events(
            rx,
            pool.clone(),
            Arc::new(ReadyLogs),
            out_dir.path().to_path_buf(),
            None,
            Arc::new(RwLock::new(WebhookConfig::default())),
            crate::concurrency::KeyGate::new(),
        )
        .await;

        // One finished run, recorded under the synthetic action + webhook source.
        // The finished run carries no stage (cleared by run_finish)
        // and no stale in-flight marker.
        let (action, triggered, jobs, exit, stage): (
            String,
            String,
            i64,
            Option<i64>,
            Option<String>,
        ) = sqlx::query_as(&crate::db::q(
            &pool,
            "SELECT action_name, triggered_by, jobs_processed, exit_code, stage FROM runs",
        ))
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(action, WEBHOOK_MINT_ACTION);
        assert_eq!(triggered, WEBHOOK_TRIGGERED_BY);
        assert_eq!(jobs, 1);
        assert_eq!(exit, Some(crate::exit::OK as i64));
        assert_eq!(stage, None, "stage cleared on finish");
        let inflight: i64 = sqlx::query_scalar(&crate::db::q(
            &pool,
            "SELECT COUNT(*) FROM run_inflight_jobs",
        ))
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(inflight, 0, "in-flight marker cleared on finish");

        // The event's html_url is recorded as the job's outbound
        // link so the run-detail tables can link the key to the Forgejo run.
        let run_id: i64 = sqlx::query_scalar(&crate::db::q(&pool, "SELECT id FROM runs"))
            .fetch_one(&pool)
            .await
            .unwrap();
        let links = state::run_job_links(&pool, run_id).await.unwrap();
        assert_eq!(
            links.get("fabrikam/forgejo-cli#7").map(String::as_str),
            Some("https://dev.a8n.run/fabrikam/forgejo-cli/actions/runs/7"),
            "{links:?}"
        );

        // The outcome carries a summary; with no backend it is the log-only note,
        // and it references the redacted artifact that the ready fetch wrote.
        let (job_key, summary): (String, Option<String>) = sqlx::query_as(&crate::db::q(
            &pool,
            "SELECT job_key, summary FROM job_outcomes",
        ))
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(job_key, "fabrikam/forgejo-cli#7");
        let summary = summary.expect("summary recorded");
        assert!(summary.contains("log-only"), "{summary}");
        assert!(summary.contains("log="), "artifact reference: {summary}");
    }

    // ---- CLAUDE-402: webhook runs close even when the handler panics ------

    /// The regression this issue fixes: a panic inside the webhook-triage
    /// pipeline, after `begin_webhook_run` opened the row, used to leave it
    /// open forever (`consume_events`'s `for t in tasks` loop only ever sees a
    /// `JoinError`, never a chance to finalize the row itself). `begin_webhook_run`
    /// now hands back a `RunGuard` alongside the id, so its `Drop` closes the
    /// row as `exit::UNEXPECTED` however the handler dies.
    #[tokio::test]
    async fn webhook_triage_run_row_closes_when_the_handler_panics() {
        let pool = memory_pool().await;
        let task = tokio::spawn({
            let pool = pool.clone();
            async move {
                let run = begin_webhook_run(
                    &pool,
                    WEBHOOK_MINT_ACTION,
                    "contoso/claude-run#9",
                    "https://example/actions/runs/9",
                )
                .await;
                let _keep_the_guard_alive_across_the_panic = run;
                panic!("simulated: webhook-triage pipeline panicked mid-run");
            }
        });
        assert!(
            task.await.is_err(),
            "the spawned handler was expected to panic"
        );

        let run_id: i64 = sqlx::query_scalar(&crate::db::q(&pool, "SELECT id FROM runs"))
            .fetch_one(&pool)
            .await
            .unwrap();
        let mut ended: Option<String> = None;
        for _ in 0..200 {
            ended = sqlx::query_scalar(&crate::db::q(
                &pool,
                "SELECT ended_at FROM runs WHERE id = ?",
            ))
            .bind(run_id)
            .fetch_one(&pool)
            .await
            .unwrap();
            if ended.is_some() {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
        assert!(
            ended.is_some(),
            "a panicking webhook-triage handler must not leave its run row open"
        );
        let exit: Option<i64> = sqlx::query_scalar(&crate::db::q(
            &pool,
            "SELECT exit_code FROM runs WHERE id = ?",
        ))
        .bind(run_id)
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(exit, Some(crate::exit::UNEXPECTED as i64));
    }

    /// `run_finish` failing once (simulated here by renaming the `runs` table
    /// out from under the first attempt) must stay visible at `error` with the
    /// run id, and the row must still close once the guard's `Drop` retries it
    /// after the table is back.
    #[tokio::test]
    async fn webhook_finish_run_finish_failure_is_logged_and_retried() {
        let pool = memory_pool().await;
        let run_id = state::run_begin(&pool, WEBHOOK_MINT_ACTION, WEBHOOK_TRIGGERED_BY)
            .await
            .unwrap();
        let guard = state::RunGuard::new(&pool, run_id);

        sqlx::query("ALTER TABLE runs RENAME TO runs_hidden_for_test")
            .execute(&pool)
            .await
            .unwrap();

        let buf = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        struct CaptureWriter(std::sync::Arc<std::sync::Mutex<Vec<u8>>>);
        impl std::io::Write for CaptureWriter {
            fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
                self.0.lock().unwrap().extend_from_slice(buf);
                Ok(buf.len())
            }
            fn flush(&mut self) -> std::io::Result<()> {
                Ok(())
            }
        }
        impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for CaptureWriter {
            type Writer = CaptureWriter;
            fn make_writer(&'a self) -> Self::Writer {
                CaptureWriter(self.0.clone())
            }
        }
        let sub = tracing_subscriber::fmt()
            .with_writer(CaptureWriter(buf.clone()))
            .with_ansi(false)
            .finish();
        let dispatch_guard = tracing::subscriber::set_default(sub);

        finish_webhook_run(
            &pool,
            Some((run_id, guard)),
            "job-1",
            "forced failure for CLAUDE-402",
            true,
            None,
            None,
        )
        .await;

        drop(dispatch_guard);
        let logs = String::from_utf8(buf.lock().unwrap().clone()).unwrap();
        assert!(
            logs.contains("ERROR"),
            "expected an error-level log: {logs}"
        );
        assert!(
            logs.contains(&run_id.to_string()),
            "expected the run id in the log: {logs}"
        );

        sqlx::query("ALTER TABLE runs_hidden_for_test RENAME TO runs")
            .execute(&pool)
            .await
            .unwrap();

        let mut ended: Option<String> = None;
        for _ in 0..200 {
            ended = sqlx::query_scalar(&crate::db::q(
                &pool,
                "SELECT ended_at FROM runs WHERE id = ?",
            ))
            .bind(run_id)
            .fetch_one(&pool)
            .await
            .unwrap();
            if ended.is_some() {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
        assert!(
            ended.is_some(),
            "the guard's retry must close the row once the table is back"
        );
        let exit: Option<i64> = sqlx::query_scalar(&crate::db::q(
            &pool,
            "SELECT exit_code FROM runs WHERE id = ?",
        ))
        .bind(run_id)
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(
            exit,
            Some(crate::exit::UNEXPECTED as i64),
            "the retry always closes as UNEXPECTED, not the caller's intended exit code"
        );
    }

    #[test]
    fn decide_mint_reports_stages_in_order_for_an_llm_classification() {
        // A non-transient log goes rules -> LLM -> mint, so the
        // reporter sees the classify stage between triage and mint.
        let classifier = StubClassifier::new(TriageBucket::CodeFix);
        let tracker = StubTracker::new();
        let stages = std::sync::Mutex::new(Vec::new());
        let report = |s: &'static str| stages.lock().unwrap().push(s);
        decide_mint(
            &classifier,
            &tracker,
            &mint_cfg(),
            &route_cache(),
            &caps(),
            &event(),
            "assertion failed: expected 1 got 2".to_string(),
            true,
            None,
            None,
            true,
            true, // auto_fix_enabled
            "",
            None,
            &report,
            &test_spend(),
        )
        .unwrap();
        assert_eq!(
            *stages.lock().unwrap(),
            vec![STAGE_CLASSIFYING, STAGE_MINTING]
        );
    }

    #[test]
    fn decide_mint_skips_the_classify_stage_on_a_rules_short_circuit() {
        // A rules-bucketed event (transient signature) never runs
        // the LLM, so "classifying with claude" must never be reported.
        let classifier = StubClassifier::new(TriageBucket::CodeFix);
        let tracker = StubTracker::new();
        let stages = std::sync::Mutex::new(Vec::new());
        let report = |s: &'static str| stages.lock().unwrap().push(s);
        decide_mint(
            &classifier,
            &tracker,
            &mint_cfg(),
            &route_cache(),
            &caps(),
            &event(),
            "error: no runner available for this job".to_string(),
            true,
            None,
            None,
            true,
            true, // auto_fix_enabled
            "",
            None,
            &report,
            &test_spend(),
        )
        .unwrap();
        assert_eq!(*stages.lock().unwrap(), vec![STAGE_MINTING]);
    }

    #[test]
    fn mint_summary_describes_each_outcome() {
        assert_eq!(
            mint_summary(&MintOutcome::Created {
                key: "CLAUDE-99".to_string(),
                project: "FJ".to_string(),
                bucket: crate::triage::TriageBucket::CodeFix,
                source: crate::mint::RouteSource::Override,
            }),
            "minted CLAUDE-99 in FJ"
        );
        assert!(mint_summary(&MintOutcome::SkippedTransient).contains("transient"));
        assert!(mint_summary(&MintOutcome::SkippedDailyCap).contains("daily run cap"));
        // A decided skip is still an `ok` run (the pipeline completed).
        let s = WebhookRunSummary::decided(
            crate::triage::TriageBucket::Transient,
            &MintOutcome::SkippedTransient,
        );
        assert!(s.ok);
        assert!(s.text.contains("triage=transient"));
        assert!(!WebhookRunSummary::error("boom".to_string()).ok);
    }

    // ---- Route a CI failure on an open PR by who opened it ----

    fn pr(number: u64, head_sha: &str) -> ForgePullRequest {
        ForgePullRequest {
            number,
            title: String::new(),
            head_sha: head_sha.to_string(),
            head_ref: String::new(),
            base_ref: String::new(),
            author: String::new(),
            html_url: String::new(),
            updated_at: String::new(),
        }
    }

    /// Point the process-global key-file env at a fresh (or reused, within one
    /// test) `age` key and store `value` under `name`, the same setup
    /// `comment_webhooks::tests::setup_pool` uses for its own write-token test.
    async fn setup_write_secret(pool: &DbPool, tmp: &std::path::Path, name: &str, value: &str) {
        use age::secrecy::ExposeSecret;
        let key_path = tmp.join("key.txt");
        if !key_path.exists() {
            let identity = age::x25519::Identity::generate();
            std::fs::write(&key_path, identity.to_string().expose_secret()).unwrap();
        }
        std::env::set_var(crate::secrets::KEY_FILE_ENV, &key_path);
        let key = SecretsKey::load().unwrap();
        let ciphertext = key.encrypt(value).unwrap();
        state::secret_create(pool, name, &ciphertext).await.unwrap();
    }

    /// A [`WebhookConfig`] with one source (`store_event`'s `"test-source"`)
    /// carrying `write_token_ref`, so a PR-comment routing decision has a
    /// backend to build.
    fn webhook_cfg_with_write_token(token_ref: Option<&str>) -> WebhookConfig {
        let mut cfg = WebhookConfig::default();
        cfg.sources.insert(
            "test-source".to_string(),
            WebhookSource {
                write_token_ref: token_ref.map(str::to_string),
                ..WebhookSource::default()
            },
        );
        cfg
    }

    #[tokio::test]
    async fn open_pr_lookup_error_fails_the_run_and_mints_nothing() {
        // An open-PR lookup error fails the run
        // with `ok = false` and an `error` log, and nothing is minted or
        // commented - never silently read as "no PR".
        let pool = crate::db::test_pool().await;
        let backend = Arc::new(StoreBackend::new().with_open_prs_error("forge unreachable"));
        let classifier: Arc<dyn FailureClassifier> = Arc::new(CountingClassifier::new());
        let ctx = store_mint_context(pool.clone(), backend.clone(), classifier);
        let ev = store_event(7101);
        let log = failing_test_log("mod_a", "test_one", "feature/x");
        let result = triage_and_mint(
            &ctx,
            None,
            &ev,
            log,
            true,
            None,
            None,
            true,
            "test".to_string(),
            None,
            &crate::concurrency::KeyGate::new(),
            &WebhookConfig::default(),
            &test_spend(),
        )
        .await;
        assert!(
            !result.ok,
            "a lookup error must fail the run: {}",
            result.text
        );
        assert!(
            result.text.contains("open-PR lookup failed"),
            "{}",
            result.text
        );
        assert!(
            !backend.calls().iter().any(|c| c.starts_with("create")),
            "nothing minted: {:?}",
            backend.calls()
        );
    }

    #[tokio::test]
    async fn agent_pr_under_limit_requeues_its_issue_and_bumps_fix_attempts() {
        // A failure recorded in
        // `agent_pull_requests` with `fix_attempts` below the ceiling comments
        // its issue, bumps `fix_attempts`, and sets `AI Agent = Queued` - no PR
        // comment, no new issue.
        let pool = crate::db::test_pool().await;
        let now = "2026-09-01T00:00:00Z";
        ci_failure_store::record_agent_pr(
            &pool,
            "youtrack",
            "CLAUDE-40",
            "dev.a8n.run",
            "acme/widgets",
            17,
            "fix/CLAUDE-40",
            "open",
            now,
        )
        .await
        .unwrap();

        let backend = Arc::new(StoreBackend::new().with_open_prs(vec![pr(17, "commit-7102")]));
        let classifier: Arc<dyn FailureClassifier> = Arc::new(CountingClassifier::new());
        let ctx = store_mint_context(pool.clone(), backend.clone(), classifier);
        let ev = store_event(7102);
        let log = failing_test_log("mod_a", "test_one", "fix/CLAUDE-40");
        let result = triage_and_mint(
            &ctx,
            None,
            &ev,
            log,
            true,
            None,
            None,
            true,
            "test".to_string(),
            None,
            &crate::concurrency::KeyGate::new(),
            &WebhookConfig::default(),
            &test_spend(),
        )
        .await;
        assert!(result.ok, "{}", result.text);
        let calls = backend.calls();
        assert!(
            !calls.iter().any(|c| c.starts_with("create")),
            "no new issue: {calls:?}"
        );
        assert!(
            calls.iter().any(|c| c.starts_with("comment CLAUDE-40")),
            "{calls:?}"
        );
        assert!(
            calls
                .iter()
                .any(|c| c == "set_field CLAUDE-40 AI Agent Queued"),
            "{calls:?}"
        );

        let record = ci_failure_store::agent_pr(&pool, "dev.a8n.run", "acme/widgets", 17)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(record.fix_attempts, 1);
    }

    #[tokio::test]
    async fn agent_pr_at_limit_flags_needs_review_listing_every_attempt() {
        // At the fix-attempt ceiling, the next
        // failure sets `AI Agent = Needs Review` with every attempt's run
        // listed, instead of re-queuing again.
        let pool = crate::db::test_pool().await;
        let now = "2026-09-01T00:00:00Z";
        let agent_pr_id = ci_failure_store::record_agent_pr(
            &pool,
            "youtrack",
            "CLAUDE-41",
            "dev.a8n.run",
            "acme/widgets",
            18,
            "fix/CLAUDE-41",
            "open",
            now,
        )
        .await
        .unwrap();
        for _ in 0..3 {
            ci_failure_store::bump_fix_attempts(&pool, agent_pr_id, now)
                .await
                .unwrap();
        }

        let backend = Arc::new(StoreBackend::new().with_open_prs(vec![pr(18, "commit-7103")]));
        let classifier: Arc<dyn FailureClassifier> = Arc::new(CountingClassifier::new());
        let ctx = store_mint_context(pool.clone(), backend.clone(), classifier);
        let ev = store_event(7103);
        let log = failing_test_log("mod_a", "test_one", "fix/CLAUDE-41");
        let result = triage_and_mint(
            &ctx,
            None,
            &ev,
            log,
            true,
            None,
            None,
            true,
            "test".to_string(),
            None,
            &crate::concurrency::KeyGate::new(),
            &WebhookConfig::default(),
            &test_spend(),
        )
        .await;
        assert!(result.ok, "{}", result.text);
        let calls = backend.calls();
        assert!(
            calls
                .iter()
                .any(|c| c == "set_field CLAUDE-41 AI Agent Needs Review"),
            "{calls:?}"
        );
        assert!(
            calls
                .iter()
                .any(|c| c.starts_with("comment CLAUDE-41") && c.contains("7103")),
            "the comment names this attempt's run: {calls:?}"
        );

        let record = ci_failure_store::agent_pr(&pool, "dev.a8n.run", "acme/widgets", 18)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            record.fix_attempts, 3,
            "a ceiling hit does not itself bump the counter further"
        );
    }

    #[tokio::test]
    async fn agent_pr_repeated_failure_stops_the_loop_without_requeue() {
        // CLAUDE-371: two consecutive attempts failing the exact same test
        // set stop the loop - no re-queue, one comment naming the repeated
        // tests and both runs, `AI Agent = Needs Review` - well under the
        // fix-attempt ceiling.
        let pool = crate::db::test_pool().await;
        let now = "2026-09-01T00:00:00Z";
        ci_failure_store::record_agent_pr(
            &pool,
            "youtrack",
            "CLAUDE-50",
            "dev.a8n.run",
            "acme/widgets",
            20,
            "fix/CLAUDE-50",
            "open",
            now,
        )
        .await
        .unwrap();

        let backend = Arc::new(StoreBackend::new().with_open_prs(vec![pr(20, "commit-7200")]));
        let classifier: Arc<dyn FailureClassifier> = Arc::new(CountingClassifier::new());
        let ctx = store_mint_context(pool.clone(), backend.clone(), classifier);

        let first = triage_and_mint(
            &ctx,
            None,
            &store_event(7200),
            failing_test_log("mod_a", "test_one", "fix/CLAUDE-50"),
            true,
            None,
            None,
            true,
            "test".to_string(),
            None,
            &crate::concurrency::KeyGate::new(),
            &WebhookConfig::default(),
            &test_spend(),
        )
        .await;
        assert!(first.ok, "{}", first.text);

        *backend.open_prs.lock().unwrap() = vec![pr(20, "commit-7201")];
        let second = triage_and_mint(
            &ctx,
            None,
            &store_event(7201),
            failing_test_log("mod_a", "test_one", "fix/CLAUDE-50"),
            true,
            None,
            None,
            true,
            "test".to_string(),
            None,
            &crate::concurrency::KeyGate::new(),
            &WebhookConfig::default(),
            &test_spend(),
        )
        .await;
        assert!(second.ok, "{}", second.text);

        let calls = backend.calls();
        assert_eq!(
            calls
                .iter()
                .filter(|c| c.starts_with("set_field CLAUDE-50 AI Agent Queued"))
                .count(),
            1,
            "only the first attempt re-queues: {calls:?}"
        );
        assert!(
            calls
                .iter()
                .any(|c| c == "set_field CLAUDE-50 AI Agent Needs Review"),
            "{calls:?}"
        );
        let repeat_comment = calls
            .iter()
            .find(|c| {
                c.starts_with("comment CLAUDE-50") && c.contains("stopping the fix-attempt loop")
            })
            .unwrap_or_else(|| panic!("expected a stop comment: {calls:?}"));
        assert!(
            repeat_comment.contains("test_one") && repeat_comment.contains("mod_a"),
            "names the repeated test: {repeat_comment}"
        );
        assert!(
            repeat_comment.contains("7200") && repeat_comment.contains("7201"),
            "names both runs: {repeat_comment}"
        );

        let record = ci_failure_store::agent_pr(&pool, "dev.a8n.run", "acme/widgets", 20)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            record.fix_attempts, 1,
            "the stop does not bump the fix-attempt counter"
        );
    }

    #[tokio::test]
    async fn agent_pr_shrinking_failure_set_is_requeued() {
        // CLAUDE-371: a strict subset of the previous attempt's failing tests
        // is progress, not a cycle, so it is re-queued as before.
        let pool = crate::db::test_pool().await;
        let now = "2026-09-01T00:00:00Z";
        ci_failure_store::record_agent_pr(
            &pool,
            "youtrack",
            "CLAUDE-51",
            "dev.a8n.run",
            "acme/widgets",
            21,
            "fix/CLAUDE-51",
            "open",
            now,
        )
        .await
        .unwrap();

        let backend = Arc::new(StoreBackend::new().with_open_prs(vec![pr(21, "commit-7210")]));
        let classifier: Arc<dyn FailureClassifier> = Arc::new(CountingClassifier::new());
        let ctx = store_mint_context(pool.clone(), backend.clone(), classifier);

        let first = triage_and_mint(
            &ctx,
            None,
            &store_event(7210),
            failing_tests_log("mod_a", &["test_one", "test_two"], "fix/CLAUDE-51"),
            true,
            None,
            None,
            true,
            "test".to_string(),
            None,
            &crate::concurrency::KeyGate::new(),
            &WebhookConfig::default(),
            &test_spend(),
        )
        .await;
        assert!(first.ok, "{}", first.text);

        *backend.open_prs.lock().unwrap() = vec![pr(21, "commit-7211")];
        let second = triage_and_mint(
            &ctx,
            None,
            &store_event(7211),
            failing_tests_log("mod_a", &["test_one"], "fix/CLAUDE-51"),
            true,
            None,
            None,
            true,
            "test".to_string(),
            None,
            &crate::concurrency::KeyGate::new(),
            &WebhookConfig::default(),
            &test_spend(),
        )
        .await;
        assert!(second.ok, "{}", second.text);

        let calls = backend.calls();
        assert_eq!(
            calls
                .iter()
                .filter(|c| c.starts_with("set_field CLAUDE-51 AI Agent Queued"))
                .count(),
            2,
            "shrinking sets keep re-queueing: {calls:?}"
        );
        assert!(
            !calls
                .iter()
                .any(|c| c == "set_field CLAUDE-51 AI Agent Needs Review"),
            "{calls:?}"
        );

        let record = ci_failure_store::agent_pr(&pool, "dev.a8n.run", "acme/widgets", 21)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(record.fix_attempts, 2);
    }

    #[tokio::test]
    async fn agent_pr_different_failure_set_is_requeued() {
        // CLAUDE-371: a different, non-subset failing set is a new problem,
        // not a repeat, so it is re-queued as before.
        let pool = crate::db::test_pool().await;
        let now = "2026-09-01T00:00:00Z";
        ci_failure_store::record_agent_pr(
            &pool,
            "youtrack",
            "CLAUDE-52",
            "dev.a8n.run",
            "acme/widgets",
            22,
            "fix/CLAUDE-52",
            "open",
            now,
        )
        .await
        .unwrap();

        let backend = Arc::new(StoreBackend::new().with_open_prs(vec![pr(22, "commit-7220")]));
        let classifier: Arc<dyn FailureClassifier> = Arc::new(CountingClassifier::new());
        let ctx = store_mint_context(pool.clone(), backend.clone(), classifier);

        let first = triage_and_mint(
            &ctx,
            None,
            &store_event(7220),
            failing_test_log("mod_a", "test_one", "fix/CLAUDE-52"),
            true,
            None,
            None,
            true,
            "test".to_string(),
            None,
            &crate::concurrency::KeyGate::new(),
            &WebhookConfig::default(),
            &test_spend(),
        )
        .await;
        assert!(first.ok, "{}", first.text);

        *backend.open_prs.lock().unwrap() = vec![pr(22, "commit-7221")];
        let second = triage_and_mint(
            &ctx,
            None,
            &store_event(7221),
            failing_test_log("mod_b", "test_two", "fix/CLAUDE-52"),
            true,
            None,
            None,
            true,
            "test".to_string(),
            None,
            &crate::concurrency::KeyGate::new(),
            &WebhookConfig::default(),
            &test_spend(),
        )
        .await;
        assert!(second.ok, "{}", second.text);

        let calls = backend.calls();
        assert_eq!(
            calls
                .iter()
                .filter(|c| c.starts_with("set_field CLAUDE-52 AI Agent Queued"))
                .count(),
            2,
            "a different failing set keeps re-queueing: {calls:?}"
        );
        assert!(
            !calls
                .iter()
                .any(|c| c == "set_field CLAUDE-52 AI Agent Needs Review"),
            "{calls:?}"
        );
    }

    #[tokio::test]
    async fn agent_pr_empty_fingerprint_falls_back_to_cap_behavior() {
        // CLAUDE-371: an empty/unparseable fingerprint never counts as "same
        // as before", so two consecutive unparseable failures still just
        // re-queue under the ordinary attempt cap instead of falsely
        // stopping the loop.
        let pool = crate::db::test_pool().await;
        let now = "2026-09-01T00:00:00Z";
        ci_failure_store::record_agent_pr(
            &pool,
            "youtrack",
            "CLAUDE-53",
            "dev.a8n.run",
            "acme/widgets",
            23,
            "fix/CLAUDE-53",
            "open",
            now,
        )
        .await
        .unwrap();

        let backend = Arc::new(StoreBackend::new().with_open_prs(vec![pr(23, "commit-7230")]));
        let classifier: Arc<dyn FailureClassifier> = Arc::new(CountingClassifier::new());
        let ctx = store_mint_context(pool.clone(), backend.clone(), classifier);

        let first = triage_and_mint(
            &ctx,
            None,
            &store_event(7230),
            unparseable_log("fix/CLAUDE-53"),
            true,
            None,
            None,
            true,
            "test".to_string(),
            None,
            &crate::concurrency::KeyGate::new(),
            &WebhookConfig::default(),
            &test_spend(),
        )
        .await;
        assert!(first.ok, "{}", first.text);

        *backend.open_prs.lock().unwrap() = vec![pr(23, "commit-7231")];
        let second = triage_and_mint(
            &ctx,
            None,
            &store_event(7231),
            unparseable_log("fix/CLAUDE-53"),
            true,
            None,
            None,
            true,
            "test".to_string(),
            None,
            &crate::concurrency::KeyGate::new(),
            &WebhookConfig::default(),
            &test_spend(),
        )
        .await;
        assert!(second.ok, "{}", second.text);

        let calls = backend.calls();
        assert_eq!(
            calls
                .iter()
                .filter(|c| c.starts_with("set_field CLAUDE-53 AI Agent Queued"))
                .count(),
            2,
            "an unparseable fingerprint never matches a previous one: {calls:?}"
        );
        assert!(
            !calls
                .iter()
                .any(|c| c == "set_field CLAUDE-53 AI Agent Needs Review"),
            "{calls:?}"
        );

        let record = ci_failure_store::agent_pr(&pool, "dev.a8n.run", "acme/widgets", 23)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(record.fix_attempts, 2);
    }

    #[tokio::test]
    async fn human_pr_no_match_comments_the_pr_and_records_a_pr_only_row() {
        // A human PR with no store match gets
        // one PR comment carrying the analysis, a `pr_only` store row, and no
        // issue.
        let _env = crate::secrets::key_file_env_lock().await;
        let tmp = tempfile::tempdir().unwrap();
        let pool = crate::db::test_pool().await;
        setup_write_secret(&pool, tmp.path(), "write-ref", "secret-pat").await;

        let paths = test_paths(tmp.path());
        let body_file = pr_comment_body_path(&paths, "acme/widgets", 17);
        let runner = Arc::new(crate::provider::cli::testutil::StubRunner::new().ok(
            "fj",
            &[
                "--host",
                "dev.a8n.run",
                "pr",
                "comment",
                "acme/widgets#17",
                "--body-file",
                body_file.to_str().unwrap(),
            ],
            "",
        ));

        let backend = Arc::new(StoreBackend::new().with_open_prs(vec![pr(17, "commit-7104")]));
        let classifier: Arc<dyn FailureClassifier> = Arc::new(CountingClassifier::new());
        let ctx = store_mint_context_in(
            pool.clone(),
            backend.clone(),
            classifier,
            tmp.path(),
            runner.clone(),
        );
        let ev = store_event(7104);
        let log = failing_test_log("mod_a", "test_one", "human/branch");
        let result = triage_and_mint(
            &ctx,
            None,
            &ev,
            log,
            true,
            None,
            None,
            true,
            "test".to_string(),
            None,
            &crate::concurrency::KeyGate::new(),
            &webhook_cfg_with_write_token(Some("write-ref")),
            &test_spend(),
        )
        .await;
        assert!(result.ok, "{}", result.text);
        assert!(
            runner
                .calls
                .lock()
                .unwrap()
                .iter()
                .any(|c| c.contains("pr comment acme/widgets#17")),
            "{:?}",
            runner.calls.lock().unwrap()
        );
        assert!(
            !backend.calls().iter().any(|c| c.starts_with("create")),
            "no issue: {:?}",
            backend.calls()
        );

        let m = ci_failure_store::find_match(
            &pool,
            "acme/widgets",
            "ci.yml",
            "test",
            &["mod_a::test_one".to_string()],
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(m.state, "pr_only");
        assert_eq!(m.issue_key, None);
    }

    #[tokio::test]
    async fn human_pr_sharing_an_open_failure_comments_and_attaches_with_no_analysis() {
        // A human PR sharing a failing test
        // with an open failure gets a PR comment naming the issue, the run is
        // attached, and no analysis runs.
        let _env = crate::secrets::key_file_env_lock().await;
        let tmp = tempfile::tempdir().unwrap();
        let pool = crate::db::test_pool().await;
        setup_write_secret(&pool, tmp.path(), "write-ref", "secret-pat").await;

        let now = "2026-09-01T00:00:00Z";
        let failure_id = ci_failure_store::record_failure(
            &pool,
            "acme/widgets",
            "ci.yml",
            "test",
            None,
            Some("CLAUDE-42"),
            "open",
            now,
        )
        .await
        .unwrap();
        ci_failure_store::add_tests(&pool, failure_id, &["mod_a::test_one".to_string()])
            .await
            .unwrap();

        let paths = test_paths(tmp.path());
        let body_file = pr_comment_body_path(&paths, "acme/widgets", 19);
        let runner = Arc::new(crate::provider::cli::testutil::StubRunner::new().ok(
            "fj",
            &[
                "--host",
                "dev.a8n.run",
                "pr",
                "comment",
                "acme/widgets#19",
                "--body-file",
                body_file.to_str().unwrap(),
            ],
            "",
        ));

        let backend = Arc::new(StoreBackend::new().with_open_prs(vec![pr(19, "commit-7105")]));
        let counting = Arc::new(CountingClassifier::new());
        let classifier: Arc<dyn FailureClassifier> = counting.clone();
        let ctx = store_mint_context_in(
            pool.clone(),
            backend.clone(),
            classifier,
            tmp.path(),
            runner.clone(),
        );
        let ev = store_event(7105);
        let log = failing_test_log("mod_a", "test_one", "human/branch2");
        let result = triage_and_mint(
            &ctx,
            None,
            &ev,
            log,
            true,
            None,
            None,
            true,
            "test".to_string(),
            None,
            &crate::concurrency::KeyGate::new(),
            &webhook_cfg_with_write_token(Some("write-ref")),
            &test_spend(),
        )
        .await;
        assert!(result.ok, "{}", result.text);
        assert_eq!(
            counting.call_count(),
            0,
            "no analysis runs: {}",
            result.text
        );
        assert!(
            runner
                .calls
                .lock()
                .unwrap()
                .iter()
                .any(|c| c.contains("pr comment acme/widgets#19")),
            "{:?}",
            runner.calls.lock().unwrap()
        );
        let body = std::fs::read_to_string(&body_file).unwrap();
        assert!(body.contains("CLAUDE-42"), "{body}");
    }

    /// On a human PR, an `open` store row
    /// whose issue's live `State` already equals the workflow's resolved
    /// state is a regression: reopen instead of a PR-only attach comment.
    #[tokio::test]
    async fn human_pr_sharing_an_already_resolved_open_match_reopens_as_a_regression() {
        let pool = crate::db::test_pool().await;
        let now = "2026-09-01T00:00:00Z";
        let failure_id = ci_failure_store::record_failure(
            &pool,
            "acme/widgets",
            "ci.yml",
            "test",
            None,
            Some("CLAUDE-44"),
            "open",
            now,
        )
        .await
        .unwrap();
        ci_failure_store::add_tests(&pool, failure_id, &["mod_a::test_one".to_string()])
            .await
            .unwrap();

        let backend = Arc::new(
            StoreBackend::new()
                .with_open_prs(vec![pr(20, "commit-7106")])
                .with_state_field("Done"),
        );
        let counting = Arc::new(CountingClassifier::new());
        let classifier: Arc<dyn FailureClassifier> = counting.clone();
        let ctx = store_mint_context(pool.clone(), backend.clone(), classifier);
        let ev = store_event(7106);
        let log = failing_test_log("mod_a", "test_one", "human/branch3");
        let result = triage_and_mint(
            &ctx,
            None,
            &ev,
            log,
            true,
            None,
            None,
            true,
            "test".to_string(),
            None,
            &crate::concurrency::KeyGate::new(),
            &WebhookConfig::default(),
            &test_spend(),
        )
        .await;
        assert!(result.ok, "{}", result.text);
        assert_eq!(counting.call_count(), 1, "the reopen triages");
        let calls = backend.calls();
        assert!(
            calls.iter().any(|c| c == "get_field CLAUDE-44 State"),
            "{calls:?}"
        );
        assert!(
            calls
                .iter()
                .any(|c| c.starts_with("comment CLAUDE-44 | Regression")),
            "{calls:?}"
        );
        assert!(
            calls.iter().any(|c| c.starts_with("set_state CLAUDE-44")),
            "regression reopen writes State: {calls:?}"
        );

        let failure = ci_failure_store::get_failure(&pool, failure_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(failure.state, "open");
    }

    /// On a human PR, a failed live-`State`
    /// read never silently attaches; it fails the run with no PR comment.
    #[tokio::test]
    async fn human_pr_sharing_an_open_match_state_read_failure_fails_the_run() {
        let pool = crate::db::test_pool().await;
        let now = "2026-09-01T00:00:00Z";
        let failure_id = ci_failure_store::record_failure(
            &pool,
            "acme/widgets",
            "ci.yml",
            "test",
            None,
            Some("CLAUDE-45"),
            "open",
            now,
        )
        .await
        .unwrap();
        ci_failure_store::add_tests(&pool, failure_id, &["mod_a::test_one".to_string()])
            .await
            .unwrap();

        let backend = Arc::new(
            StoreBackend::new()
                .with_open_prs(vec![pr(21, "commit-7107")])
                .with_state_field_error("yt: not found"),
        );
        let counting = Arc::new(CountingClassifier::new());
        let classifier: Arc<dyn FailureClassifier> = counting.clone();
        let ctx = store_mint_context(pool.clone(), backend.clone(), classifier);
        let ev = store_event(7107);
        let log = failing_test_log("mod_a", "test_one", "human/branch4");
        let result = triage_and_mint(
            &ctx,
            None,
            &ev,
            log,
            true,
            None,
            None,
            true,
            "test".to_string(),
            None,
            &crate::concurrency::KeyGate::new(),
            &WebhookConfig::default(),
            &test_spend(),
        )
        .await;
        assert!(!result.ok, "a failed state read must fail the run");
        assert_eq!(counting.call_count(), 0, "no triage on a failed read");
        let calls = backend.calls();
        assert!(
            !calls.iter().any(|c| c.starts_with("comment")),
            "no comment on a failed read: {calls:?}"
        );
        assert!(
            !calls.iter().any(|c| c.starts_with("set_state")),
            "no reopen on a failed read: {calls:?}"
        );

        let failure = ci_failure_store::get_failure(&pool, failure_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            failure.state, "open",
            "store row untouched on a failed read"
        );
    }

    #[tokio::test]
    async fn human_pr_on_a_second_branch_mints_one_issue_and_comments_both_prs() {
        // The same failing tests on a second
        // branch's PR create one issue, and both PRs get the issue link.
        let _env = crate::secrets::key_file_env_lock().await;
        let tmp = tempfile::tempdir().unwrap();
        let pool = crate::db::test_pool().await;
        setup_write_secret(&pool, tmp.path(), "write-ref", "secret-pat").await;

        let paths = test_paths(tmp.path());
        let body_file_a = pr_comment_body_path(&paths, "acme/widgets", 21);
        let body_file_b = pr_comment_body_path(&paths, "acme/widgets", 22);
        let runner = Arc::new(
            crate::provider::cli::testutil::StubRunner::new()
                .ok(
                    "fj",
                    &[
                        "--host",
                        "dev.a8n.run",
                        "pr",
                        "comment",
                        "acme/widgets#21",
                        "--body-file",
                        body_file_a.to_str().unwrap(),
                    ],
                    "",
                )
                .ok(
                    "fj",
                    &[
                        "--host",
                        "dev.a8n.run",
                        "pr",
                        "comment",
                        "acme/widgets#22",
                        "--body-file",
                        body_file_b.to_str().unwrap(),
                    ],
                    "",
                ),
        );

        let backend = Arc::new(
            StoreBackend::new().with_open_prs(vec![pr(21, "commit-7106"), pr(22, "commit-7107")]),
        );
        let classifier: Arc<dyn FailureClassifier> = Arc::new(CountingClassifier::new());
        let ctx = store_mint_context_in(
            pool.clone(),
            backend.clone(),
            classifier,
            tmp.path(),
            runner.clone(),
        );
        let webhook_cfg = webhook_cfg_with_write_token(Some("write-ref"));

        let ev_a = store_event(7106);
        let log_a = failing_test_log("mod_a", "test_one", "branch/a");
        let result_a = triage_and_mint(
            &ctx,
            None,
            &ev_a,
            log_a,
            true,
            None,
            None,
            true,
            "test".to_string(),
            None,
            &crate::concurrency::KeyGate::new(),
            &webhook_cfg,
            &test_spend(),
        )
        .await;
        assert!(result_a.ok, "{}", result_a.text);

        let ev_b = store_event(7107);
        let log_b = failing_test_log("mod_a", "test_one", "branch/b");
        let result_b = triage_and_mint(
            &ctx,
            None,
            &ev_b,
            log_b,
            true,
            None,
            None,
            true,
            "test".to_string(),
            None,
            &crate::concurrency::KeyGate::new(),
            &webhook_cfg,
            &test_spend(),
        )
        .await;
        assert!(result_b.ok, "{}", result_b.text);

        let calls = backend.calls();
        let creates: Vec<_> = calls.iter().filter(|c| c.starts_with("create")).collect();
        assert_eq!(creates.len(), 1, "exactly one issue created: {calls:?}");

        let runner_calls = runner.calls.lock().unwrap();
        assert!(
            runner_calls
                .iter()
                .any(|c| c.contains("pr comment acme/widgets#21")),
            "{runner_calls:?}"
        );
        assert!(
            runner_calls
                .iter()
                .any(|c| c.contains("pr comment acme/widgets#22")),
            "{runner_calls:?}"
        );
    }

    #[tokio::test]
    async fn missing_write_token_ref_fails_the_run_and_records_a_run_event() {
        // A source without `write_token_ref`
        // logs at `error`, writes a `run_events` row, and fails the run.
        let pool = crate::db::test_pool().await;
        let backend = Arc::new(StoreBackend::new().with_open_prs(vec![pr(23, "commit-7108")]));
        let classifier: Arc<dyn FailureClassifier> = Arc::new(CountingClassifier::new());
        let ctx = store_mint_context(pool.clone(), backend.clone(), classifier);
        let ev = store_event(7108);
        let log = failing_test_log("mod_a", "test_one", "human/branch3");
        let result = triage_and_mint(
            &ctx,
            None,
            &ev,
            log,
            true,
            None,
            None,
            true,
            "test".to_string(),
            None,
            &crate::concurrency::KeyGate::new(),
            &webhook_cfg_with_write_token(None),
            &test_spend(),
        )
        .await;
        assert!(!result.ok, "no write_token_ref must fail the run");
        assert!(result.text.contains("PR comment"), "{}", result.text);
        let events = state::run_events_for_action(&pool, WEBHOOK_MINT_ACTION, 10)
            .await
            .unwrap();
        assert!(
            events.iter().any(|e| e.message.contains("write_token_ref")),
            "a run_events row names the missing token: {events:?}"
        );
    }

    // ---- Merge-close: PR merges with the failing workflow green ----
    mod merge_close {
        use super::*;

        fn merge_event(repo: &str, number: i64, head_sha: &str) -> VerifiedPullRequestEvent {
            VerifiedPullRequestEvent {
                repo: repo.to_string(),
                number,
                merged: true,
                head_sha: head_sha.to_string(),
                head_ref: "fix-branch".to_string(),
                html_url: format!("https://dev.a8n.run/{repo}/pulls/{number}"),
                source: "test-source".to_string(),
            }
        }

        fn closed_unmerged_event(repo: &str, number: i64) -> VerifiedPullRequestEvent {
            VerifiedPullRequestEvent {
                merged: false,
                ..merge_event(repo, number, "deadbeef")
            }
        }

        #[tokio::test]
        async fn merged_agent_pr_is_marked_merged_and_makes_no_tracker_call() {
            // The merged-PR handler still
            // sets `agent_pull_requests.state = merged` and makes no tracker
            // call at all - the YouTrack workflow resolves the issue, not the
            // runner.
            let pool = crate::db::test_pool().await;
            let now = "2026-09-01T00:00:00Z";
            ci_failure_store::record_agent_pr(
                &pool,
                "youtrack",
                "CLAUDE-50",
                "dev.a8n.run",
                "acme/widgets",
                99,
                "fix/CLAUDE-50",
                "open",
                now,
            )
            .await
            .unwrap();

            let backend = Arc::new(StoreBackend::new());
            let classifier: Arc<dyn FailureClassifier> = Arc::new(CountingClassifier::new());
            let ctx = store_mint_context(pool.clone(), backend.clone(), classifier);

            let ev = merge_event("acme/widgets", 99, "deadbeefcafefeed");
            process_pr_event(
                ev,
                pool.clone(),
                Some(Arc::new(ctx)),
                crate::concurrency::KeyGate::new(),
            )
            .await;

            assert!(
                backend.calls().is_empty(),
                "the merged-PR handler must make no tracker call: {:?}",
                backend.calls()
            );

            let row = ci_failure_store::agent_pr(&pool, "dev.a8n.run", "acme/widgets", 99)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(row.state, "merged");
        }

        #[tokio::test]
        async fn a_queued_pr_event_is_skipped_while_paused() {
            // Neither the store row nor the tracker is touched.
            let pool = crate::db::test_pool().await;
            ci_failure_store::record_agent_pr(
                &pool,
                "youtrack",
                "CLAUDE-50",
                "dev.a8n.run",
                "acme/widgets",
                99,
                "fix/CLAUDE-50",
                "open",
                "2026-09-01T00:00:00Z",
            )
            .await
            .unwrap();
            state::global_pause_set(&pool, Some("t")).await.unwrap();
            let backend = Arc::new(StoreBackend::new());
            let classifier: Arc<dyn FailureClassifier> = Arc::new(CountingClassifier::new());
            let ctx = store_mint_context(pool.clone(), backend.clone(), classifier);

            process_pr_event(
                merge_event("acme/widgets", 99, "deadbeefcafefeed"),
                pool.clone(),
                Some(Arc::new(ctx)),
                crate::concurrency::KeyGate::new(),
            )
            .await;

            assert!(backend.calls().is_empty(), "{:?}", backend.calls());
            let row = ci_failure_store::agent_pr(&pool, "dev.a8n.run", "acme/widgets", 99)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(row.state, "open");
        }

        #[tokio::test]
        async fn a_merged_pr_with_no_agent_pull_request_row_makes_no_tracker_call() {
            // A human's merged PR (no `agent_pull_requests` row) changes
            // nothing and makes no tracker call.
            let pool = crate::db::test_pool().await;
            let backend = Arc::new(StoreBackend::new());
            let classifier: Arc<dyn FailureClassifier> = Arc::new(CountingClassifier::new());
            let ctx = store_mint_context(pool.clone(), backend.clone(), classifier);

            let ev = merge_event("acme/widgets", 200, "deadbeefcafefeed");
            process_pr_event(
                ev,
                pool.clone(),
                Some(Arc::new(ctx)),
                crate::concurrency::KeyGate::new(),
            )
            .await;

            assert!(backend.calls().is_empty(), "{:?}", backend.calls());
        }

        #[tokio::test]
        async fn closed_unmerged_agent_pr_is_marked_closed_and_changes_no_issue() {
            // A closed, unmerged agent PR sets `agent_pull_requests.state =
            // closed` and changes no issue.
            let pool = crate::db::test_pool().await;
            let now = "2026-09-01T00:00:00Z";
            ci_failure_store::record_agent_pr(
                &pool,
                "youtrack",
                "CLAUDE-53",
                "dev.a8n.run",
                "acme/widgets",
                102,
                "fix/CLAUDE-53",
                "open",
                now,
            )
            .await
            .unwrap();

            let backend = Arc::new(StoreBackend::new());
            let classifier: Arc<dyn FailureClassifier> = Arc::new(CountingClassifier::new());
            let ctx = store_mint_context(pool.clone(), backend.clone(), classifier);

            let ev = closed_unmerged_event("acme/widgets", 102);
            process_pr_event(
                ev,
                pool.clone(),
                Some(Arc::new(ctx)),
                crate::concurrency::KeyGate::new(),
            )
            .await;

            assert!(
                backend.calls().is_empty(),
                "no issue write for a closed-unmerged PR: {:?}",
                backend.calls()
            );
            let row = ci_failure_store::agent_pr(&pool, "dev.a8n.run", "acme/widgets", 102)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(row.state, "closed");
        }

        #[tokio::test]
        async fn no_usable_backend_drops_the_event_with_no_panic() {
            let pool = crate::db::test_pool().await;
            let ev = merge_event("acme/widgets", 103, "deadbeef");
            // `mint: None` is what the daemon passes when no `yt`/`fj` backend is
            // usable; the event must simply be dropped, not panic.
            process_pr_event(ev, pool, None, crate::concurrency::KeyGate::new()).await;
        }
    }

    // ---- Every auth-rejection branch is reachable ----
    //
    // Drive `forgejo_webhook` directly so each rejection branch (and the
    // accepted path that emits the debug line) is exercised at the status-code
    // level, including per-source secret isolation, unknown-source 404,
    // and per-source rate buckets. These set the process-global key-file env, so
    // they take the shared lock with the secrets-UI test in `web::mod`.
    mod handler_auth {
        use super::*;
        use age::secrecy::ExposeSecret;
        use axum::http::HeaderValue;

        // Two sources, each with its own secret and allow-list, exercise
        // per-source isolation.
        const SOURCE_A: &str = "contoso";
        const SECRET_A: &str = "hmac-value-for-contoso";
        const SOURCE_B: &str = "fabrikam";
        const SECRET_B: &str = "hmac-value-for-fabrikam";

        /// Point the key-file env at a fresh key under `tmp` and store both
        /// sources' HMAC secrets, returning the backing pool. The caller holds
        /// [`crate::secrets::key_file_env_lock`] so the env stays stable across
        /// the `resolve_secret` call inside the handler.
        async fn setup_pool(tmp: &std::path::Path) -> DbPool {
            let key_path = tmp.join("key.txt");
            let identity = age::x25519::Identity::generate();
            std::fs::write(&key_path, identity.to_string().expose_secret()).unwrap();
            std::env::set_var(crate::secrets::KEY_FILE_ENV, &key_path);

            let pool = memory_pool().await;
            let key = SecretsKey::load().unwrap();
            for (name, value) in [("hmac-contoso", SECRET_A), ("hmac-fabrikam", SECRET_B)] {
                let ciphertext = key.encrypt(value).unwrap();
                state::secret_create(&pool, name, &ciphertext)
                    .await
                    .unwrap();
            }
            pool
        }

        /// A two-source config: `contoso` (exact repo) and `fabrikam`
        /// (owner wildcard), each rate-limited at `rate_per_min`.
        fn two_source_config(rate_per_min: u32) -> WebhookConfig {
            let sources = [
                (
                    SOURCE_A.to_string(),
                    WebhookSource {
                        secret_ref: Some("hmac-contoso".to_string()),
                        allowed_repos: vec!["contoso/claude-run".to_string()],
                        rate_limit_per_min: rate_per_min,
                        log_token_ref: None,
                        write_token_ref: None,
                        provider: WebhookProvider::Forgejo,
                    },
                ),
                (
                    SOURCE_B.to_string(),
                    WebhookSource {
                        secret_ref: Some("hmac-fabrikam".to_string()),
                        allowed_repos: vec!["fabrikam/*".to_string()],
                        rate_limit_per_min: rate_per_min,
                        log_token_ref: None,
                        write_token_ref: None,
                        provider: WebhookProvider::Forgejo,
                    },
                ),
            ]
            .into_iter()
            .collect();
            WebhookConfig {
                enabled: true,
                replay_window_secs: 300,
                sources,
                auto_fix: false,
                analysis_timeout_secs: 600,
                analysis_concurrency: 2,
                max_runs_per_day: 20,
                agent_pr_fix_attempts: 3,
            }
        }

        /// A [`WebhookState`] over [`two_source_config`] at the default rate.
        async fn state(
            tmp: &std::path::Path,
        ) -> (WebhookState, mpsc::Receiver<VerifiedFailureEvent>) {
            let pool = setup_pool(tmp).await;
            let (tx, rx) = mpsc::channel(8);
            (WebhookState::new(pool, two_source_config(60), tx), rx)
        }

        fn headers(signature: Option<&str>, delivery: Option<&str>) -> HeaderMap {
            let mut h = HeaderMap::new();
            if let Some(s) = signature {
                h.insert(SIGNATURE_HEADER, HeaderValue::from_str(s).unwrap());
            }
            if let Some(d) = delivery {
                h.insert(DELIVERY_HEADER, HeaderValue::from_str(d).unwrap());
            }
            h
        }

        const SOURCE_GH: &str = "gh-contoso";
        const SECRET_GH: &str = "hmac-value-for-gh-contoso";

        /// [`two_source_config`] plus a third, `Github`-provider source
        /// (`SOURCE_GH`), so a mixed Forgejo/GitHub config shares one
        /// `max_runs_per_day` cap.
        fn three_source_config(rate_per_min: u32) -> WebhookConfig {
            let mut cfg = two_source_config(rate_per_min);
            cfg.sources.insert(
                SOURCE_GH.to_string(),
                WebhookSource {
                    secret_ref: Some("hmac-gh-contoso".to_string()),
                    allowed_repos: vec!["contoso/claude-run".to_string()],
                    rate_limit_per_min: rate_per_min,
                    log_token_ref: None,
                    write_token_ref: None,
                    provider: WebhookProvider::Github,
                },
            );
            cfg
        }

        /// Like [`setup_pool`], plus the `SOURCE_GH` secret.
        async fn setup_pool_with_github(tmp: &std::path::Path) -> DbPool {
            let pool = setup_pool(tmp).await;
            let key = SecretsKey::load().unwrap();
            let ciphertext = key.encrypt(SECRET_GH).unwrap();
            state::secret_create(&pool, "hmac-gh-contoso", &ciphertext)
                .await
                .unwrap();
            pool
        }

        /// A [`WebhookState`] over [`three_source_config`] at the default rate.
        async fn state_with_github(
            tmp: &std::path::Path,
        ) -> (WebhookState, mpsc::Receiver<VerifiedFailureEvent>) {
            let pool = setup_pool_with_github(tmp).await;
            let (tx, rx) = mpsc::channel(8);
            (WebhookState::new(pool, three_source_config(60), tx), rx)
        }

        fn github_headers(signature: Option<&str>, delivery: Option<&str>) -> HeaderMap {
            let mut h = HeaderMap::new();
            if let Some(s) = signature {
                h.insert(GITHUB_SIGNATURE_HEADER, HeaderValue::from_str(s).unwrap());
            }
            if let Some(d) = delivery {
                h.insert(GITHUB_DELIVERY_HEADER, HeaderValue::from_str(d).unwrap());
            }
            h
        }

        fn github_headers_with_event(
            signature: Option<&str>,
            delivery: Option<&str>,
            event: Option<&str>,
        ) -> HeaderMap {
            let mut h = github_headers(signature, delivery);
            if let Some(e) = event {
                h.insert(GITHUB_EVENT_HEADER, HeaderValue::from_str(e).unwrap());
            }
            h
        }

        fn github_sign(secret: &[u8], body: &[u8]) -> String {
            format!("sha256={}", sign(secret, body))
        }

        /// A minimal completed `workflow_run` payload (`Webhook events and
        /// payloads / workflow_run`, docs.github.com) with the given
        /// `conclusion`.
        fn github_workflow_run_payload(repo: &str, conclusion: &str) -> String {
            format!(
                r#"{{"action":"completed","workflow_run":{{"run_number":7,
                "head_sha":"deadbeefcafefeed","path":".github/workflows/ci.yml",
                "html_url":"https://github.com/{repo}/actions/runs/123",
                "conclusion":"{conclusion}","actor":{{"login":"octocat"}}}},
                "repository":{{"full_name":"{repo}","private":false}}}}"#
            )
        }

        #[tokio::test]
        async fn unknown_source_is_404() {
            let _env = crate::secrets::key_file_env_lock().await;
            let tmp = tempfile::tempdir().unwrap();
            let (st, _rx) = state(tmp.path()).await;
            let raw = failure_payload("contoso/claude-run");
            let sig = sign(SECRET_A.as_bytes(), raw.as_bytes());
            // A path segment that names no configured source short-circuits to 404
            // before any rate / crypto work.
            let resp = forgejo_webhook(
                State(st),
                Path(("forgejo".to_string(), "no-such-source".to_string())),
                headers(Some(&sig), Some("d-0")),
                Bytes::from(raw),
            )
            .await;
            assert_eq!(resp.status(), StatusCode::NOT_FOUND);
        }

        // An unrecognized `:provider` segment 404s before the config is even
        // read, the same treatment an unconfigured `:source` gets.
        #[tokio::test]
        async fn unrecognized_provider_is_404() {
            let _env = crate::secrets::key_file_env_lock().await;
            let tmp = tempfile::tempdir().unwrap();
            let (st, _rx) = state(tmp.path()).await;
            let raw = failure_payload("contoso/claude-run");
            let sig = sign(SECRET_A.as_bytes(), raw.as_bytes());
            let resp = forgejo_webhook(
                State(st),
                Path(("gitlab".to_string(), SOURCE_A.to_string())),
                headers(Some(&sig), Some("d-provider-unknown")),
                Bytes::from(raw),
            )
            .await;
            assert_eq!(resp.status(), StatusCode::NOT_FOUND);
        }

        // A source configured for one provider 404s a request made against
        // the other provider's URL segment, now that `Github` is a second
        // real variant to mismatch against.
        #[tokio::test]
        async fn provider_source_mismatch_is_404() {
            let _env = crate::secrets::key_file_env_lock().await;
            let tmp = tempfile::tempdir().unwrap();
            let (st, _rx) = state(tmp.path()).await;
            let raw = failure_payload("contoso/claude-run");
            let sig = sign(SECRET_A.as_bytes(), raw.as_bytes());
            // SOURCE_A is configured with `provider: WebhookProvider::Forgejo`.
            let resp = forgejo_webhook(
                State(st),
                Path(("github".to_string(), SOURCE_A.to_string())),
                headers(Some(&sig), Some("d-provider-mismatch")),
                Bytes::from(raw),
            )
            .await;
            assert_eq!(resp.status(), StatusCode::NOT_FOUND);
        }

        #[test]
        fn webhook_source_provider_defaults_to_forgejo_and_round_trips() {
            let src: WebhookSource = serde_json::from_str(r#"{"secret_ref":"x"}"#).unwrap();
            assert_eq!(src.provider, WebhookProvider::Forgejo);
            assert_eq!(WebhookSource::default().provider, WebhookProvider::Forgejo);
            let json = serde_json::to_string(&WebhookProvider::Forgejo).unwrap();
            assert_eq!(json, "\"forgejo\"");
            assert_eq!(
                serde_json::from_str::<WebhookProvider>("\"forgejo\"").unwrap(),
                WebhookProvider::Forgejo
            );
        }

        // GitHub: signature scheme, delivery/event headers, and the
        // `workflow_run` -> `ActionPayload` mapping, end to end through mint.

        #[tokio::test]
        async fn github_triage_delivery_dispatches_verified_failure_event() {
            let _env = crate::secrets::key_file_env_lock().await;
            let tmp = tempfile::tempdir().unwrap();
            let (st, mut rx) = state_with_github(tmp.path()).await;
            let raw = github_workflow_run_payload("contoso/claude-run", "failure");
            let sig = github_sign(SECRET_GH.as_bytes(), raw.as_bytes());
            let resp = forgejo_webhook(
                State(st),
                Path(("github".to_string(), SOURCE_GH.to_string())),
                github_headers_with_event(
                    Some(&sig),
                    Some("gh-d-1"),
                    Some(GITHUB_WORKFLOW_RUN_EVENT_TYPE),
                ),
                Bytes::from(raw),
            )
            .await;
            assert_eq!(resp.status(), StatusCode::ACCEPTED);
            let event = rx.recv().await.expect("a verified failure event");
            assert_eq!(event.repo, "contoso/claude-run");
            assert_eq!(event.index_in_repo, 7);
            assert_eq!(event.commit_sha, "deadbeefcafefeed");
            assert_eq!(event.workflow_id, "ci.yml");
            assert_eq!(event.trigger_user, "octocat");
            assert_eq!(event.status, RUN_STATUS_FAILURE);
        }

        /// GitHub's default webhook Content type delivers the JSON payload
        /// form-wrapped (`payload=<url-encoded JSON>`); the signature covers
        /// the form body as sent, and decoding happens only after it verifies.
        #[tokio::test]
        async fn github_triage_form_urlencoded_delivery_dispatches_same_event() {
            let _env = crate::secrets::key_file_env_lock().await;
            let tmp = tempfile::tempdir().unwrap();
            let (st, mut rx) = state_with_github(tmp.path()).await;
            let raw = github_workflow_run_payload("contoso/claude-run", "failure");
            let form_body = serde_urlencoded::to_string([("payload", raw.as_str())]).unwrap();
            let sig = github_sign(SECRET_GH.as_bytes(), form_body.as_bytes());
            let mut headers = github_headers_with_event(
                Some(&sig),
                Some("gh-d-form-1"),
                Some(GITHUB_WORKFLOW_RUN_EVENT_TYPE),
            );
            headers.insert(
                axum::http::header::CONTENT_TYPE,
                HeaderValue::from_static("application/x-www-form-urlencoded"),
            );
            let resp = forgejo_webhook(
                State(st),
                Path(("github".to_string(), SOURCE_GH.to_string())),
                headers,
                Bytes::from(form_body),
            )
            .await;
            assert_eq!(resp.status(), StatusCode::ACCEPTED);
            let event = rx.recv().await.expect("a verified failure event");
            assert_eq!(event.repo, "contoso/claude-run");
            assert_eq!(event.commit_sha, "deadbeefcafefeed");
        }

        #[tokio::test]
        async fn github_form_urlencoded_delivery_with_no_payload_field_is_400() {
            let _env = crate::secrets::key_file_env_lock().await;
            let tmp = tempfile::tempdir().unwrap();
            let (st, _rx) = state_with_github(tmp.path()).await;
            let form_body = serde_urlencoded::to_string([("not_payload", "x")]).unwrap();
            let sig = github_sign(SECRET_GH.as_bytes(), form_body.as_bytes());
            let mut headers = github_headers_with_event(
                Some(&sig),
                Some("gh-d-form-2"),
                Some(GITHUB_WORKFLOW_RUN_EVENT_TYPE),
            );
            headers.insert(
                axum::http::header::CONTENT_TYPE,
                HeaderValue::from_static("application/x-www-form-urlencoded"),
            );
            let resp = forgejo_webhook(
                State(st),
                Path(("github".to_string(), SOURCE_GH.to_string())),
                headers,
                Bytes::from(form_body),
            )
            .await;
            assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
        }

        /// Forgejo always sends JSON; a form-content-typed Forgejo delivery is
        /// not decoded (the helper is a GitHub-only no-op for other providers)
        /// and still fails the JSON parse the same way an unrecognized body did
        /// before this change.
        #[tokio::test]
        async fn forgejo_form_content_type_delivery_is_still_rejected() {
            let _env = crate::secrets::key_file_env_lock().await;
            let tmp = tempfile::tempdir().unwrap();
            let pool = setup_pool(tmp.path()).await;
            let (tx, _rx) = mpsc::channel(8);
            let st = WebhookState::new(pool, two_source_config(60), tx);
            let form_body = serde_urlencoded::to_string([("payload", "{}")]).unwrap();
            let sig = sign(SECRET_A.as_bytes(), form_body.as_bytes());
            let mut hdrs = headers(Some(&sig), Some("fj-form-1"));
            hdrs.insert(
                axum::http::header::CONTENT_TYPE,
                HeaderValue::from_static("application/x-www-form-urlencoded"),
            );
            let resp = forgejo_webhook(
                State(st),
                Path(("forgejo".to_string(), SOURCE_A.to_string())),
                hdrs,
                Bytes::from(form_body),
            )
            .await;
            assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
        }

        #[tokio::test]
        async fn github_bad_signature_is_401() {
            let _env = crate::secrets::key_file_env_lock().await;
            let tmp = tempfile::tempdir().unwrap();
            let (st, _rx) = state_with_github(tmp.path()).await;
            let raw = github_workflow_run_payload("contoso/claude-run", "failure");
            // Well-formed `sha256=`-prefixed hex, but the wrong MAC.
            let resp = forgejo_webhook(
                State(st),
                Path(("github".to_string(), SOURCE_GH.to_string())),
                github_headers_with_event(
                    Some("sha256=deadbeef"),
                    Some("gh-d-2"),
                    Some(GITHUB_WORKFLOW_RUN_EVENT_TYPE),
                ),
                Bytes::from(raw),
            )
            .await;
            assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
        }

        #[tokio::test]
        async fn github_signature_missing_sha256_prefix_is_401() {
            // GitHub's deprecated SHA1 `X-Hub-Signature` is never read; a
            // signature value with no `sha256=` prefix fails closed rather
            // than falling through to an algorithm mismatch.
            let _env = crate::secrets::key_file_env_lock().await;
            let tmp = tempfile::tempdir().unwrap();
            let (st, _rx) = state_with_github(tmp.path()).await;
            let raw = github_workflow_run_payload("contoso/claude-run", "failure");
            let bare_sig = sign(SECRET_GH.as_bytes(), raw.as_bytes());
            let resp = forgejo_webhook(
                State(st),
                Path(("github".to_string(), SOURCE_GH.to_string())),
                github_headers_with_event(
                    Some(&bare_sig),
                    Some("gh-d-3"),
                    Some(GITHUB_WORKFLOW_RUN_EVENT_TYPE),
                ),
                Bytes::from(raw),
            )
            .await;
            assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
        }

        #[tokio::test]
        async fn github_success_conclusion_is_not_a_failure_event() {
            let _env = crate::secrets::key_file_env_lock().await;
            let tmp = tempfile::tempdir().unwrap();
            let (st, mut rx) = state_with_github(tmp.path()).await;
            let raw = github_workflow_run_payload("contoso/claude-run", "success");
            let sig = github_sign(SECRET_GH.as_bytes(), raw.as_bytes());
            let resp = forgejo_webhook(
                State(st),
                Path(("github".to_string(), SOURCE_GH.to_string())),
                github_headers_with_event(
                    Some(&sig),
                    Some("gh-d-4"),
                    Some(GITHUB_WORKFLOW_RUN_EVENT_TYPE),
                ),
                Bytes::from(raw),
            )
            .await;
            // No `mint` wiring in this fixture's state, so a success delivery
            // is acked and dropped rather than routed to the automerge path.
            assert_eq!(resp.status(), StatusCode::OK);
            assert!(rx.try_recv().is_err());
        }

        #[tokio::test]
        async fn github_non_workflow_run_event_is_acked_and_dropped() {
            let _env = crate::secrets::key_file_env_lock().await;
            let tmp = tempfile::tempdir().unwrap();
            let (st, mut rx) = state_with_github(tmp.path()).await;
            let raw = github_workflow_run_payload("contoso/claude-run", "failure");
            let sig = github_sign(SECRET_GH.as_bytes(), raw.as_bytes());
            let resp = forgejo_webhook(
                State(st),
                Path(("github".to_string(), SOURCE_GH.to_string())),
                github_headers_with_event(Some(&sig), Some("gh-d-5"), Some("issues")),
                Bytes::from(raw),
            )
            .await;
            assert_eq!(resp.status(), StatusCode::OK);
            assert!(rx.try_recv().is_err());
        }

        /// CLAUDE-389's sweep: the CI-failure listener's GitHub arm already
        /// acks-and-drops any non-`workflow_run` event, so a `ping` delivery
        /// (sent on webhook creation / "Redeliver") never 400s here either.
        /// No code change needed; this just confirms it by test.
        #[tokio::test]
        async fn github_ping_is_acked_and_dropped() {
            let _env = crate::secrets::key_file_env_lock().await;
            let tmp = tempfile::tempdir().unwrap();
            let (st, mut rx) = state_with_github(tmp.path()).await;
            let raw = r#"{"zen":"Non-blocking is better than blocking.","hook_id":1,
                "hook":{"type":"Repository","id":1,"events":["workflow_run"]}}"#;
            let sig = github_sign(SECRET_GH.as_bytes(), raw.as_bytes());
            let resp = forgejo_webhook(
                State(st),
                Path(("github".to_string(), SOURCE_GH.to_string())),
                github_headers_with_event(
                    Some(&sig),
                    Some("gh-ping-1"),
                    Some(GITHUB_PING_EVENT_TYPE),
                ),
                Bytes::from(raw),
            )
            .await;
            assert_eq!(resp.status(), StatusCode::OK);
            assert!(rx.try_recv().is_err());
        }

        // A Forgejo source and a GitHub source coexist in one config and share
        // one `max_runs_per_day` cap.
        #[tokio::test]
        async fn forgejo_and_github_sources_coexist_in_one_config() {
            let _env = crate::secrets::key_file_env_lock().await;
            let tmp = tempfile::tempdir().unwrap();
            let (st, mut rx) = state_with_github(tmp.path()).await;
            assert_eq!(
                st.config.read().await.max_runs_per_day,
                three_source_config(60).max_runs_per_day,
                "both sources are read off the same WebhookConfig, so one cap covers both"
            );

            let fj_raw = failure_payload("contoso/claude-run");
            let fj_sig = sign(SECRET_A.as_bytes(), fj_raw.as_bytes());
            let fj_resp = forgejo_webhook(
                State(st.clone()),
                Path(("forgejo".to_string(), SOURCE_A.to_string())),
                headers(Some(&fj_sig), Some("mixed-fj-1")),
                Bytes::from(fj_raw),
            )
            .await;
            assert_eq!(fj_resp.status(), StatusCode::ACCEPTED);
            assert!(rx.recv().await.is_some());

            let gh_raw = github_workflow_run_payload("contoso/claude-run", "failure");
            let gh_sig = github_sign(SECRET_GH.as_bytes(), gh_raw.as_bytes());
            let gh_resp = forgejo_webhook(
                State(st),
                Path(("github".to_string(), SOURCE_GH.to_string())),
                github_headers_with_event(
                    Some(&gh_sig),
                    Some("mixed-gh-1"),
                    Some(GITHUB_WORKFLOW_RUN_EVENT_TYPE),
                ),
                Bytes::from(gh_raw),
            )
            .await;
            assert_eq!(gh_resp.status(), StatusCode::ACCEPTED);
            assert!(rx.recv().await.is_some());
        }

        // Router-level: the new route accepts a valid delivery and the old,
        // pre-CLAUDE-361 route no longer exists at all.
        #[tokio::test]
        async fn router_serves_new_route_and_404s_the_old_one() {
            let _env = crate::secrets::key_file_env_lock().await;
            let tmp = tempfile::tempdir().unwrap();
            let (st, mut rx) = state(tmp.path()).await;
            let raw = failure_payload("contoso/claude-run");
            let sig = sign(SECRET_A.as_bytes(), raw.as_bytes());

            let mut app = router(st);

            let req = axum::http::Request::builder()
                .method("POST")
                .uri(format!("/webhooks/forgejo/triage/{SOURCE_A}"))
                .header(SIGNATURE_HEADER, sig.clone())
                .header(DELIVERY_HEADER, "d-router-new")
                .body(axum::body::Body::from(raw.clone()))
                .unwrap();
            let resp = tower::Service::call(&mut app, req).await.unwrap();
            assert_eq!(resp.status(), StatusCode::ACCEPTED);
            assert!(
                rx.recv().await.is_some(),
                "the new route dispatched a verified failure event"
            );

            let req_old = axum::http::Request::builder()
                .method("POST")
                .uri(format!("/webhooks/forgejo/{SOURCE_A}"))
                .header(SIGNATURE_HEADER, sig)
                .header(DELIVERY_HEADER, "d-router-old")
                .body(axum::body::Body::from(raw))
                .unwrap();
            let resp_old = tower::Service::call(&mut app, req_old).await.unwrap();
            assert_eq!(
                resp_old.status(),
                StatusCode::NOT_FOUND,
                "the pre-CLAUDE-361 route no longer exists"
            );
        }

        #[tokio::test]
        async fn disabled_listener_404s_even_with_a_configured_source() {
            // CLAUDE-360: `enabled: false` 404s every request regardless of
            // whether the source is fully configured (a valid signature),
            // ahead of the source lookup.
            let _env = crate::secrets::key_file_env_lock().await;
            let tmp = tempfile::tempdir().unwrap();
            let pool = setup_pool(tmp.path()).await;
            let cfg = WebhookConfig {
                enabled: false,
                ..two_source_config(60)
            };
            let (tx, _rx) = mpsc::channel(8);
            let st = WebhookState::new(pool, cfg, tx);
            let raw = failure_payload("contoso/claude-run");
            let sig = sign(SECRET_A.as_bytes(), raw.as_bytes());
            let resp = forgejo_webhook(
                State(st),
                Path(("forgejo".to_string(), SOURCE_A.to_string())),
                headers(Some(&sig), Some("d-disabled")),
                Bytes::from(raw),
            )
            .await;
            assert_eq!(resp.status(), StatusCode::NOT_FOUND);
        }

        #[tokio::test]
        async fn missing_signature_header_is_401() {
            let _env = crate::secrets::key_file_env_lock().await;
            let tmp = tempfile::tempdir().unwrap();
            let (st, _rx) = state(tmp.path()).await;
            let body = Bytes::from(failure_payload("contoso/claude-run"));
            let resp = forgejo_webhook(
                State(st),
                Path(("forgejo".to_string(), SOURCE_A.to_string())),
                headers(None, Some("d-1")),
                body,
            )
            .await;
            assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
        }

        #[tokio::test]
        async fn bad_signature_is_401() {
            let _env = crate::secrets::key_file_env_lock().await;
            let tmp = tempfile::tempdir().unwrap();
            let (st, _rx) = state(tmp.path()).await;
            let body = Bytes::from(failure_payload("contoso/claude-run"));
            // Well-formed hex but the wrong MAC: the signature value never leaks
            // into a log; only the delivery id and body length are recorded.
            let resp = forgejo_webhook(
                State(st),
                Path(("forgejo".to_string(), SOURCE_A.to_string())),
                headers(Some("deadbeef"), Some("d-2")),
                body,
            )
            .await;
            assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
        }

        #[tokio::test]
        async fn cross_source_signature_is_401() {
            // A payload validly signed with source A's secret must NOT
            // authenticate on source B's path. Source B verifies against its own
            // secret only, so the otherwise-valid signature mismatches -> 401.
            let _env = crate::secrets::key_file_env_lock().await;
            let tmp = tempfile::tempdir().unwrap();
            let (st, mut rx) = state(tmp.path()).await;
            let raw = failure_payload("fabrikam/gizmo");
            // Sign with A's secret, deliver to B.
            let sig = sign(SECRET_A.as_bytes(), raw.as_bytes());
            let resp = forgejo_webhook(
                State(st),
                Path(("forgejo".to_string(), SOURCE_B.to_string())),
                headers(Some(&sig), Some("x-1")),
                Bytes::from(raw),
            )
            .await;
            assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
            assert!(rx.try_recv().is_err());
        }

        #[tokio::test]
        async fn unset_secret_ref_fails_closed_401() {
            // A source whose secret_ref is unset cannot authenticate;
            // it must fail closed with 401, not accept.
            let _env = crate::secrets::key_file_env_lock().await;
            let tmp = tempfile::tempdir().unwrap();
            let pool = setup_pool(tmp.path()).await;
            let sources = [(
                "secretless".to_string(),
                WebhookSource {
                    secret_ref: None,
                    allowed_repos: vec!["*".to_string()],
                    rate_limit_per_min: 60,
                    log_token_ref: None,
                    write_token_ref: None,
                    provider: WebhookProvider::Forgejo,
                },
            )]
            .into_iter()
            .collect();
            let config = WebhookConfig {
                enabled: true,
                replay_window_secs: 300,
                sources,
                auto_fix: false,
                analysis_timeout_secs: 600,
                analysis_concurrency: 2,
                max_runs_per_day: 20,
                agent_pr_fix_attempts: 3,
            };
            let (tx, _rx) = mpsc::channel(8);
            let st = WebhookState::new(pool, config, tx);
            let raw = failure_payload("contoso/claude-run");
            let sig = sign(SECRET_A.as_bytes(), raw.as_bytes());
            let resp = forgejo_webhook(
                State(st),
                Path(("forgejo".to_string(), "secretless".to_string())),
                headers(Some(&sig), Some("s-1")),
                Bytes::from(raw),
            )
            .await;
            assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
        }

        #[tokio::test]
        async fn missing_delivery_id_is_400() {
            let _env = crate::secrets::key_file_env_lock().await;
            let tmp = tempfile::tempdir().unwrap();
            let (st, _rx) = state(tmp.path()).await;
            let raw = failure_payload("contoso/claude-run");
            let sig = sign(SECRET_A.as_bytes(), raw.as_bytes());
            let resp = forgejo_webhook(
                State(st),
                Path(("forgejo".to_string(), SOURCE_A.to_string())),
                headers(Some(&sig), None),
                Bytes::from(raw),
            )
            .await;
            assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
        }

        #[tokio::test]
        async fn authenticated_delivery_is_accepted() {
            let _env = crate::secrets::key_file_env_lock().await;
            let tmp = tempfile::tempdir().unwrap();
            let (st, mut rx) = state(tmp.path()).await;
            let raw = failure_payload("contoso/claude-run");
            let sig = sign(SECRET_A.as_bytes(), raw.as_bytes());
            let resp = forgejo_webhook(
                State(st),
                Path(("forgejo".to_string(), SOURCE_A.to_string())),
                headers(Some(&sig), Some("d-3")),
                Bytes::from(raw),
            )
            .await;
            // Accepted path: the debug "authenticated" line fires here and the
            // verified failure reaches the event channel.
            assert_eq!(resp.status(), StatusCode::ACCEPTED);
            assert!(rx.try_recv().is_ok());
        }

        #[tokio::test]
        async fn paused_action_accepts_and_drops_without_emitting() {
            // With the synthetic `webhook-forgejo` action paused, an
            // otherwise-valid authenticated delivery is acked with 2xx (so Forgejo
            // does not retry) but no event is emitted (no mint/queue). The
            // signature/replay/rate-limit checks still ran to get here.
            let _env = crate::secrets::key_file_env_lock().await;
            let tmp = tempfile::tempdir().unwrap();
            let (st, mut rx) = state(tmp.path()).await;
            // Pause the synthetic webhook action in the shared control table.
            state::action_pause_set(&st.pool, WEBHOOK_MINT_ACTION, Some("maintenance"))
                .await
                .unwrap();
            let raw = failure_payload("contoso/claude-run");
            let sig = sign(SECRET_A.as_bytes(), raw.as_bytes());
            let resp = forgejo_webhook(
                State(st),
                Path(("forgejo".to_string(), SOURCE_A.to_string())),
                headers(Some(&sig), Some("p-1")),
                Bytes::from(raw),
            )
            .await;
            // 2xx (accept-and-drop), and nothing reached the event channel.
            assert_eq!(resp.status(), StatusCode::OK);
            assert!(
                rx.try_recv().is_err(),
                "paused action must not emit a mint event"
            );
        }

        /// While the Claude login needs renewing, a delivery is
        /// accepted and dropped exactly like a pause (2xx, no classifier, no
        /// mint), AND a `run_events` row records why, so the drop is visible
        /// on the action page and not only in the daemon log.
        #[tokio::test]
        async fn login_required_accepts_and_drops_and_records_a_run_event() {
            let _env = crate::secrets::key_file_env_lock().await;
            let tmp = tempfile::tempdir().unwrap();
            let (mut st, mut rx) = state(tmp.path()).await;
            let readiness = crate::readiness::ReadinessGate::new(
                tempfile::tempdir().unwrap().path().to_path_buf(),
                None,
            );
            readiness.trip_login_required("OAuth session expired", "invocation");
            st = st.with_readiness(readiness);

            let raw = failure_payload("contoso/claude-run");
            let sig = sign(SECRET_A.as_bytes(), raw.as_bytes());
            let resp = forgejo_webhook(
                State(st.clone()),
                Path(("forgejo".to_string(), SOURCE_A.to_string())),
                headers(Some(&sig), Some("lr-1")),
                Bytes::from(raw),
            )
            .await;
            assert_eq!(
                resp.status(),
                StatusCode::OK,
                "2xx so Forgejo does not retry"
            );
            assert!(
                rx.try_recv().is_err(),
                "a login-required delivery must not emit a mint event"
            );

            let events = state::run_events_for_action(&st.pool, WEBHOOK_MINT_ACTION, 10)
                .await
                .unwrap();
            assert!(
                events
                    .iter()
                    .any(|e| e.message.contains("Claude login required")),
                "the drop must be recorded as a run_events row: {events:?}"
            );
        }

        /// The GLOBAL pause gets the same accept-and-drop treatment
        /// as the per-action one. Before this the listener only checked the
        /// synthetic action, so a globally-paused daemon still minted from a
        /// webhook. Resuming restores the emit.
        #[tokio::test]
        async fn global_pause_accepts_and_drops_without_emitting() {
            let _env = crate::secrets::key_file_env_lock().await;
            let tmp = tempfile::tempdir().unwrap();
            let (st, mut rx) = state(tmp.path()).await;
            state::global_pause_set(&st.pool, Some("rebooting"))
                .await
                .unwrap();
            let raw = failure_payload("contoso/claude-run");
            let sig = sign(SECRET_A.as_bytes(), raw.as_bytes());
            let resp = forgejo_webhook(
                State(st.clone()),
                Path(("forgejo".to_string(), SOURCE_A.to_string())),
                headers(Some(&sig), Some("gp-1")),
                Bytes::from(raw.clone()),
            )
            .await;
            assert_eq!(
                resp.status(),
                StatusCode::OK,
                "2xx so Forgejo does not retry"
            );
            assert!(
                rx.try_recv().is_err(),
                "a globally paused daemon must not emit a mint event"
            );

            state::global_pause_clear(&st.pool).await.unwrap();
            let resp = forgejo_webhook(
                State(st),
                Path(("forgejo".to_string(), SOURCE_A.to_string())),
                headers(Some(&sig), Some("gp-2")),
                Bytes::from(raw),
            )
            .await;
            assert_eq!(resp.status(), StatusCode::ACCEPTED);
            assert!(rx.try_recv().is_ok(), "resume restores the emit");
        }

        #[tokio::test]
        async fn resumed_action_emits_again() {
            // Clearing the pause row restores the accepted+emit path,
            // proving the pause is a live toggle, not a permanent disable.
            let _env = crate::secrets::key_file_env_lock().await;
            let tmp = tempfile::tempdir().unwrap();
            let (st, mut rx) = state(tmp.path()).await;
            state::action_pause_set(&st.pool, WEBHOOK_MINT_ACTION, None)
                .await
                .unwrap();
            state::action_pause_clear(&st.pool, WEBHOOK_MINT_ACTION)
                .await
                .unwrap();
            let raw = failure_payload("contoso/claude-run");
            let sig = sign(SECRET_A.as_bytes(), raw.as_bytes());
            let resp = forgejo_webhook(
                State(st),
                Path(("forgejo".to_string(), SOURCE_A.to_string())),
                headers(Some(&sig), Some("p-2")),
                Bytes::from(raw),
            )
            .await;
            assert_eq!(resp.status(), StatusCode::ACCEPTED);
            assert!(rx.try_recv().is_ok(), "resumed action emits again");
        }

        #[tokio::test]
        async fn each_source_authenticates_on_its_own_path() {
            // Both sources accept a delivery signed with their own
            // secret and allow-listed for their own repos, proving four (here two)
            // endpoints work independently off one daemon.
            let _env = crate::secrets::key_file_env_lock().await;
            let tmp = tempfile::tempdir().unwrap();
            let (st, mut rx) = state(tmp.path()).await;

            let raw_a = failure_payload("contoso/claude-run");
            let sig_a = sign(SECRET_A.as_bytes(), raw_a.as_bytes());
            let resp_a = forgejo_webhook(
                State(st.clone()),
                Path(("forgejo".to_string(), SOURCE_A.to_string())),
                headers(Some(&sig_a), Some("a-1")),
                Bytes::from(raw_a),
            )
            .await;
            assert_eq!(resp_a.status(), StatusCode::ACCEPTED);

            let raw_b = failure_payload("fabrikam/gizmo");
            let sig_b = sign(SECRET_B.as_bytes(), raw_b.as_bytes());
            let resp_b = forgejo_webhook(
                State(st),
                Path(("forgejo".to_string(), SOURCE_B.to_string())),
                headers(Some(&sig_b), Some("b-1")),
                Bytes::from(raw_b),
            )
            .await;
            assert_eq!(resp_b.status(), StatusCode::ACCEPTED);

            assert_eq!(rx.recv().await.unwrap().repo, "contoso/claude-run");
            assert_eq!(rx.recv().await.unwrap().repo, "fabrikam/gizmo");
        }

        #[tokio::test]
        async fn rate_limit_is_per_source() {
            // Exhausting source A's budget (rate=1) must not 429
            // source B. A's second delivery is 429; B's first still 202.
            let _env = crate::secrets::key_file_env_lock().await;
            let tmp = tempfile::tempdir().unwrap();
            let pool = setup_pool(tmp.path()).await;
            let (tx, _rx) = mpsc::channel(8);
            let st = WebhookState::new(pool, two_source_config(1), tx);

            let raw1 = failure_payload("contoso/claude-run");
            let sig1 = sign(SECRET_A.as_bytes(), raw1.as_bytes());
            let r1 = forgejo_webhook(
                State(st.clone()),
                Path(("forgejo".to_string(), SOURCE_A.to_string())),
                headers(Some(&sig1), Some("r-1")),
                Bytes::from(raw1),
            )
            .await;
            assert_eq!(r1.status(), StatusCode::ACCEPTED);

            // Source A's budget (1/min) is spent: the next A delivery is 429.
            let raw2 = failure_payload("contoso/claude-run");
            let sig2 = sign(SECRET_A.as_bytes(), raw2.as_bytes());
            let r2 = forgejo_webhook(
                State(st.clone()),
                Path(("forgejo".to_string(), SOURCE_A.to_string())),
                headers(Some(&sig2), Some("r-2")),
                Bytes::from(raw2),
            )
            .await;
            assert_eq!(r2.status(), StatusCode::TOO_MANY_REQUESTS);

            // Source B's independent bucket is untouched: its first delivery 202s.
            let raw3 = failure_payload("fabrikam/gizmo");
            let sig3 = sign(SECRET_B.as_bytes(), raw3.as_bytes());
            let r3 = forgejo_webhook(
                State(st),
                Path(("forgejo".to_string(), SOURCE_B.to_string())),
                headers(Some(&sig3), Some("r-3")),
                Bytes::from(raw3),
            )
            .await;
            assert_eq!(r3.status(), StatusCode::ACCEPTED);
        }

        #[tokio::test]
        async fn replay_store_is_shared_across_sources() {
            // Delivery ids are globally unique, so a replay of one
            // source's delivery id on another source is rejected (409) by the
            // shared store. The first delivery to A authenticates and records the
            // id; the same id delivered to B (validly signed for B) is a replay.
            let _env = crate::secrets::key_file_env_lock().await;
            let tmp = tempfile::tempdir().unwrap();
            let (st, _rx) = state(tmp.path()).await;

            let raw_a = failure_payload("contoso/claude-run");
            let sig_a = sign(SECRET_A.as_bytes(), raw_a.as_bytes());
            let r1 = forgejo_webhook(
                State(st.clone()),
                Path(("forgejo".to_string(), SOURCE_A.to_string())),
                headers(Some(&sig_a), Some("dup-1")),
                Bytes::from(raw_a),
            )
            .await;
            assert_eq!(r1.status(), StatusCode::ACCEPTED);

            let raw_b = failure_payload("fabrikam/gizmo");
            let sig_b = sign(SECRET_B.as_bytes(), raw_b.as_bytes());
            let r2 = forgejo_webhook(
                State(st),
                Path(("forgejo".to_string(), SOURCE_B.to_string())),
                headers(Some(&sig_b), Some("dup-1")),
                Bytes::from(raw_b),
            )
            .await;
            assert_eq!(r2.status(), StatusCode::CONFLICT);
        }

        #[tokio::test]
        async fn non_failure_action_is_acked_and_dropped() {
            let _env = crate::secrets::key_file_env_lock().await;
            let tmp = tempfile::tempdir().unwrap();
            let (st, mut rx) = state(tmp.path()).await;
            let raw = failure_payload("contoso/claude-run")
                .replace(r#""action":"failure""#, r#""action":"recover""#);
            let sig = sign(SECRET_A.as_bytes(), raw.as_bytes());
            let resp = forgejo_webhook(
                State(st),
                Path(("forgejo".to_string(), SOURCE_A.to_string())),
                headers(Some(&sig), Some("d-4")),
                Bytes::from(raw),
            )
            .await;
            // Drop path ("non-failure action"): 200 so Forgejo does not retry,
            // the info "event dropped" line fires, nothing reaches the channel.
            assert_eq!(resp.status(), StatusCode::OK);
            assert!(rx.try_recv().is_err());
        }

        #[tokio::test]
        async fn unlisted_repo_is_acked_and_dropped() {
            // Source A's allow-list is `contoso/claude-run` only, so
            // a validly signed delivery for a different repo is acked + dropped.
            let _env = crate::secrets::key_file_env_lock().await;
            let tmp = tempfile::tempdir().unwrap();
            let (st, mut rx) = state(tmp.path()).await;
            let raw = failure_payload("someone-else/other-repo");
            let sig = sign(SECRET_A.as_bytes(), raw.as_bytes());
            let resp = forgejo_webhook(
                State(st),
                Path(("forgejo".to_string(), SOURCE_A.to_string())),
                headers(Some(&sig), Some("d-5")),
                Bytes::from(raw),
            )
            .await;
            // Drop path ("repo not in allowed_repos"): same 200 ack, no event.
            assert_eq!(resp.status(), StatusCode::OK);
            assert!(rx.try_recv().is_err());
        }

        // The pull-request event-type dispatch and its gates.

        fn headers_with_event(
            signature: Option<&str>,
            delivery: Option<&str>,
            event: Option<&str>,
        ) -> HeaderMap {
            let mut h = headers(signature, delivery);
            if let Some(e) = event {
                h.insert(EVENT_HEADER, HeaderValue::from_str(e).unwrap());
            }
            h
        }

        fn pull_request_payload(repo: &str, number: i64, action: &str, merged: bool) -> String {
            format!(
                r#"{{"action":"{action}","number":{number},"pull_request":{{"merged":{merged},
                "head":{{"ref":"fix-branch","sha":"deadbeefcafefeed"}},
                "html_url":"https://dev.a8n.run/{repo}/pulls/{number}"}},
                "repository":{{"full_name":"{repo}"}}}}"#
            )
        }

        /// A [`WebhookState`] over [`two_source_config`], with a `pr_events`
        /// channel attached so a `pull_request` delivery dispatches instead of
        /// being acked-and-dropped for lack of a configured sender.
        async fn state_with_pr_events(
            tmp: &std::path::Path,
        ) -> (
            WebhookState,
            mpsc::Receiver<VerifiedFailureEvent>,
            mpsc::Receiver<VerifiedPullRequestEvent>,
        ) {
            let pool = setup_pool(tmp).await;
            let (tx, rx) = mpsc::channel(8);
            let (pr_tx, pr_rx) = mpsc::channel(8);
            let st = WebhookState::new(pool, two_source_config(60), tx).with_pr_events(pr_tx);
            (st, rx, pr_rx)
        }

        #[tokio::test]
        async fn unrecognized_event_type_is_acked_and_dropped() {
            // A header value that is neither an Action-run kind nor
            // `pull_request` (e.g. "issues") is acked and dropped without being
            // parsed as either payload shape.
            let _env = crate::secrets::key_file_env_lock().await;
            let tmp = tempfile::tempdir().unwrap();
            let (st, mut rx) = state(tmp.path()).await;
            let raw = failure_payload("contoso/claude-run");
            let sig = sign(SECRET_A.as_bytes(), raw.as_bytes());
            let resp = forgejo_webhook(
                State(st),
                Path(("forgejo".to_string(), SOURCE_A.to_string())),
                headers_with_event(Some(&sig), Some("u-1"), Some("issues")),
                Bytes::from(raw),
            )
            .await;
            assert_eq!(resp.status(), StatusCode::OK);
            assert!(rx.try_recv().is_err());
        }

        #[tokio::test]
        async fn action_run_event_header_still_takes_the_failure_path() {
            // An explicit `action_run_failure` header (the value the deployed
            // Forgejo's Action-run notifier sends) keeps the pre-existing path.
            let _env = crate::secrets::key_file_env_lock().await;
            let tmp = tempfile::tempdir().unwrap();
            let (st, mut rx) = state(tmp.path()).await;
            let raw = failure_payload("contoso/claude-run");
            let sig = sign(SECRET_A.as_bytes(), raw.as_bytes());
            let resp = forgejo_webhook(
                State(st),
                Path(("forgejo".to_string(), SOURCE_A.to_string())),
                headers_with_event(Some(&sig), Some("ar-1"), Some("action_run_failure")),
                Bytes::from(raw),
            )
            .await;
            assert_eq!(resp.status(), StatusCode::ACCEPTED);
            assert!(rx.try_recv().is_ok());
        }

        #[tokio::test]
        async fn pull_request_event_missing_signature_is_401() {
            let _env = crate::secrets::key_file_env_lock().await;
            let tmp = tempfile::tempdir().unwrap();
            let (st, _rx, _pr_rx) = state_with_pr_events(tmp.path()).await;
            let raw = pull_request_payload("contoso/claude-run", 42, "closed", true);
            let resp = forgejo_webhook(
                State(st),
                Path(("forgejo".to_string(), SOURCE_A.to_string())),
                headers_with_event(None, Some("pr-1"), Some(PULL_REQUEST_EVENT_TYPE)),
                Bytes::from(raw),
            )
            .await;
            assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
        }

        #[tokio::test]
        async fn pull_request_event_replay_is_rejected() {
            let _env = crate::secrets::key_file_env_lock().await;
            let tmp = tempfile::tempdir().unwrap();
            let (st, _rx, mut pr_rx) = state_with_pr_events(tmp.path()).await;
            let raw = pull_request_payload("contoso/claude-run", 42, "closed", true);
            let sig = sign(SECRET_A.as_bytes(), raw.as_bytes());
            let headers1 =
                headers_with_event(Some(&sig), Some("pr-dup"), Some(PULL_REQUEST_EVENT_TYPE));
            let r1 = forgejo_webhook(
                State(st.clone()),
                Path(("forgejo".to_string(), SOURCE_A.to_string())),
                headers1,
                Bytes::from(raw.clone()),
            )
            .await;
            assert_eq!(r1.status(), StatusCode::ACCEPTED);
            assert!(pr_rx.try_recv().is_ok());

            let headers2 =
                headers_with_event(Some(&sig), Some("pr-dup"), Some(PULL_REQUEST_EVENT_TYPE));
            let r2 = forgejo_webhook(
                State(st),
                Path(("forgejo".to_string(), SOURCE_A.to_string())),
                headers2,
                Bytes::from(raw),
            )
            .await;
            assert_eq!(r2.status(), StatusCode::CONFLICT);
        }

        #[tokio::test]
        async fn pull_request_event_paused_action_accepts_and_drops() {
            let _env = crate::secrets::key_file_env_lock().await;
            let tmp = tempfile::tempdir().unwrap();
            let (st, _rx, mut pr_rx) = state_with_pr_events(tmp.path()).await;
            state::action_pause_set(&st.pool, WEBHOOK_MINT_ACTION, Some("maintenance"))
                .await
                .unwrap();
            let raw = pull_request_payload("contoso/claude-run", 42, "closed", true);
            let sig = sign(SECRET_A.as_bytes(), raw.as_bytes());
            let resp = forgejo_webhook(
                State(st),
                Path(("forgejo".to_string(), SOURCE_A.to_string())),
                headers_with_event(Some(&sig), Some("pr-pause"), Some(PULL_REQUEST_EVENT_TYPE)),
                Bytes::from(raw),
            )
            .await;
            assert_eq!(resp.status(), StatusCode::OK);
            assert!(pr_rx.try_recv().is_err());
        }

        #[tokio::test]
        async fn pull_request_event_non_terminal_action_is_acked_and_dropped() {
            let _env = crate::secrets::key_file_env_lock().await;
            let tmp = tempfile::tempdir().unwrap();
            let (st, _rx, mut pr_rx) = state_with_pr_events(tmp.path()).await;
            let raw = pull_request_payload("contoso/claude-run", 42, "synchronize", false);
            let sig = sign(SECRET_A.as_bytes(), raw.as_bytes());
            let resp = forgejo_webhook(
                State(st),
                Path(("forgejo".to_string(), SOURCE_A.to_string())),
                headers_with_event(Some(&sig), Some("pr-sync"), Some(PULL_REQUEST_EVENT_TYPE)),
                Bytes::from(raw),
            )
            .await;
            assert_eq!(resp.status(), StatusCode::OK);
            assert!(pr_rx.try_recv().is_err());
        }

        #[tokio::test]
        async fn pull_request_event_unlisted_repo_is_acked_and_dropped() {
            let _env = crate::secrets::key_file_env_lock().await;
            let tmp = tempfile::tempdir().unwrap();
            let (st, _rx, mut pr_rx) = state_with_pr_events(tmp.path()).await;
            let raw = pull_request_payload("someone-else/other-repo", 42, "closed", true);
            let sig = sign(SECRET_A.as_bytes(), raw.as_bytes());
            let resp = forgejo_webhook(
                State(st),
                Path(("forgejo".to_string(), SOURCE_A.to_string())),
                headers_with_event(
                    Some(&sig),
                    Some("pr-unlisted"),
                    Some(PULL_REQUEST_EVENT_TYPE),
                ),
                Bytes::from(raw),
            )
            .await;
            assert_eq!(resp.status(), StatusCode::OK);
            assert!(pr_rx.try_recv().is_err());
        }

        #[tokio::test]
        async fn merged_pull_request_event_is_accepted_and_dispatched() {
            let _env = crate::secrets::key_file_env_lock().await;
            let tmp = tempfile::tempdir().unwrap();
            let (st, _rx, mut pr_rx) = state_with_pr_events(tmp.path()).await;
            let raw = pull_request_payload("contoso/claude-run", 42, "closed", true);
            let sig = sign(SECRET_A.as_bytes(), raw.as_bytes());
            let resp = forgejo_webhook(
                State(st),
                Path(("forgejo".to_string(), SOURCE_A.to_string())),
                headers_with_event(Some(&sig), Some("pr-merged"), Some(PULL_REQUEST_EVENT_TYPE)),
                Bytes::from(raw),
            )
            .await;
            assert_eq!(resp.status(), StatusCode::ACCEPTED);
            let ev = pr_rx.try_recv().unwrap();
            assert_eq!(ev.repo, "contoso/claude-run");
            assert_eq!(ev.number, 42);
            assert!(ev.merged);
            assert_eq!(ev.head_sha, "deadbeefcafefeed");
        }

        /// The `pull_request` path is shared by both providers; a GitHub
        /// delivery sent with the default form Content type must decode the
        /// same as the JSON delivery above.
        #[tokio::test]
        async fn github_pull_request_form_urlencoded_delivery_is_accepted_and_dispatched() {
            let _env = crate::secrets::key_file_env_lock().await;
            let tmp = tempfile::tempdir().unwrap();
            let pool = setup_pool_with_github(tmp.path()).await;
            let (tx, _rx) = mpsc::channel(8);
            let (pr_tx, mut pr_rx) = mpsc::channel(8);
            let st = WebhookState::new(pool, three_source_config(60), tx).with_pr_events(pr_tx);
            let raw = pull_request_payload("contoso/claude-run", 42, "closed", true);
            let form_body = serde_urlencoded::to_string([("payload", raw.as_str())]).unwrap();
            let sig = github_sign(SECRET_GH.as_bytes(), form_body.as_bytes());
            let mut hdrs = github_headers_with_event(
                Some(&sig),
                Some("gh-pr-form-1"),
                Some(PULL_REQUEST_EVENT_TYPE),
            );
            hdrs.insert(
                axum::http::header::CONTENT_TYPE,
                HeaderValue::from_static("application/x-www-form-urlencoded"),
            );
            let resp = forgejo_webhook(
                State(st),
                Path(("github".to_string(), SOURCE_GH.to_string())),
                hdrs,
                Bytes::from(form_body),
            )
            .await;
            assert_eq!(resp.status(), StatusCode::ACCEPTED);
            let ev = pr_rx.try_recv().unwrap();
            assert_eq!(ev.repo, "contoso/claude-run");
            assert_eq!(ev.number, 42);
            assert!(ev.merged);
        }

        #[tokio::test]
        async fn github_pull_request_form_urlencoded_delivery_with_no_payload_field_is_400() {
            let _env = crate::secrets::key_file_env_lock().await;
            let tmp = tempfile::tempdir().unwrap();
            let pool = setup_pool_with_github(tmp.path()).await;
            let (tx, _rx) = mpsc::channel(8);
            let (pr_tx, _pr_rx) = mpsc::channel(8);
            let st = WebhookState::new(pool, three_source_config(60), tx).with_pr_events(pr_tx);
            let form_body = serde_urlencoded::to_string([("not_payload", "x")]).unwrap();
            let sig = github_sign(SECRET_GH.as_bytes(), form_body.as_bytes());
            let mut hdrs = github_headers_with_event(
                Some(&sig),
                Some("gh-pr-form-2"),
                Some(PULL_REQUEST_EVENT_TYPE),
            );
            hdrs.insert(
                axum::http::header::CONTENT_TYPE,
                HeaderValue::from_static("application/x-www-form-urlencoded"),
            );
            let resp = forgejo_webhook(
                State(st),
                Path(("github".to_string(), SOURCE_GH.to_string())),
                hdrs,
                Bytes::from(form_body),
            )
            .await;
            assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
        }
    }

    // ---- Event-driven automerge on a success/recover event -----
    mod automerge_success {
        use super::*;

        /// Writes a minimal youtrack action YAML under `dir/actions/<name>.yml`
        /// (the layout `crate::action::load` resolves against), with automerge
        /// set to `enabled`.
        pub(super) fn write_youtrack_action(
            dir: &std::path::Path,
            name: &str,
            automerge_enabled: bool,
        ) {
            let actions_dir = dir.join("actions");
            std::fs::create_dir_all(&actions_dir).unwrap();
            let yaml = format!(
                "name: {name}\nkind: youtrack\nnative:\n  youtrack:\n    automerge:\n      enabled: {automerge_enabled}\n"
            );
            std::fs::write(actions_dir.join(format!("{name}.yml")), yaml).unwrap();
        }

        fn ctx_for(
            pool: DbPool,
            backend: Arc<StoreBackend>,
            paths_dir: &std::path::Path,
        ) -> MintContext {
            let classifier: Arc<dyn FailureClassifier> = Arc::new(CountingClassifier::new());
            store_mint_context_in(
                pool,
                backend,
                classifier,
                paths_dir,
                Arc::new(crate::provider::cli::testutil::StubRunner::new()),
            )
        }

        #[tokio::test]
        async fn success_and_mergeable_merges_and_comments_with_no_state_or_field_write() {
            // A success event on a recorded agent PR, with
            // automerge enabled and pr_status success+mergeable, merges the PR
            // and comments, but writes no State or AI Agent field: a YouTrack
            // workflow resolves the issue once the merge lands.
            let pool = crate::db::test_pool().await;
            let dir = tempfile::tempdir().unwrap();
            write_youtrack_action(dir.path(), "yt", true);
            let now = "2026-09-01T00:00:00Z";
            ci_failure_store::record_agent_pr(
                &pool,
                "yt",
                "MK-16",
                "dev.a8n.run",
                "contoso/claude-run",
                7,
                "fix/mk-16",
                "open",
                now,
            )
            .await
            .unwrap();
            let backend = Arc::new(StoreBackend::new().with_pr_status("success", true));
            let ctx = ctx_for(pool.clone(), backend.clone(), dir.path());

            let result = handle_action_success_for_pr(
                &ctx,
                &pool,
                "dev.a8n.run",
                "contoso/claude-run",
                7,
                "https://dev.a8n.run/contoso/claude-run/pulls/7",
            )
            .await
            .unwrap();
            assert!(result.contains("merged"), "{result}");
            let calls = backend.calls();
            assert!(calls.iter().any(|c| c == "merge_pr 7"), "{calls:?}");
            assert!(
                calls.iter().any(|c| c.starts_with("comment MK-16")),
                "{calls:?}"
            );
            assert!(
                !calls.iter().any(|c| c.starts_with("set_state")),
                "no State write: {calls:?}"
            );
            assert!(
                !calls.iter().any(|c| c.starts_with("set_field")),
                "no AI Agent write: {calls:?}"
            );
        }

        #[tokio::test]
        async fn pending_does_nothing_then_a_later_success_merges() {
            // Acceptance criterion 2: a success event while other checks are
            // pending does nothing, and a later success event (once the forge
            // reports green + mergeable) merges.
            let pool = crate::db::test_pool().await;
            let dir = tempfile::tempdir().unwrap();
            write_youtrack_action(dir.path(), "yt", true);
            let now = "2026-09-01T00:00:00Z";
            ci_failure_store::record_agent_pr(
                &pool,
                "yt",
                "MK-16",
                "dev.a8n.run",
                "contoso/claude-run",
                7,
                "fix/mk-16",
                "open",
                now,
            )
            .await
            .unwrap();
            let backend = Arc::new(StoreBackend::new().with_pr_status("pending", false));
            let ctx = ctx_for(pool.clone(), backend.clone(), dir.path());

            let result = handle_action_success_for_pr(
                &ctx,
                &pool,
                "dev.a8n.run",
                "contoso/claude-run",
                7,
                "https://dev.a8n.run/contoso/claude-run/pulls/7",
            )
            .await
            .unwrap();
            assert!(
                result.contains("not action") || result.contains("no action"),
                "{result}"
            );
            let calls = backend.calls();
            assert!(
                !calls.iter().any(|c| c.starts_with("merge_pr")),
                "{calls:?}"
            );
            assert!(!calls.iter().any(|c| c.contains("Done")), "{calls:?}");

            // A later success event: the forge now reports green + mergeable.
            *backend.pr_status.lock().unwrap() = Some(crate::provider::PrStatus {
                state: "success".to_string(),
                mergeable: Some(true),
                behind_base: Some(false),
                head_sha: None,
            });
            let result2 = handle_action_success_for_pr(
                &ctx,
                &pool,
                "dev.a8n.run",
                "contoso/claude-run",
                7,
                "https://dev.a8n.run/contoso/claude-run/pulls/7",
            )
            .await
            .unwrap();
            assert!(result2.contains("merged"), "{result2}");
            let calls = backend.calls();
            assert!(calls.iter().any(|c| c == "merge_pr 7"), "{calls:?}");
        }

        #[tokio::test]
        async fn not_a_recorded_pr_changes_nothing() {
            // Acceptance criterion 3 (part 1): a success event on a PR that is
            // not a recorded agent PR changes nothing.
            let pool = crate::db::test_pool().await;
            let dir = tempfile::tempdir().unwrap();
            let backend = Arc::new(StoreBackend::new().with_pr_status("success", true));
            let ctx = ctx_for(pool.clone(), backend.clone(), dir.path());

            let result = handle_action_success_for_pr(
                &ctx,
                &pool,
                "dev.a8n.run",
                "contoso/claude-run",
                999,
                "https://dev.a8n.run/contoso/claude-run/pulls/999",
            )
            .await
            .unwrap();
            assert!(result.contains("not a recorded agent PR"), "{result}");
            assert!(backend.calls().is_empty(), "{:?}", backend.calls());
        }

        #[tokio::test]
        async fn automerge_disabled_action_changes_nothing() {
            // Acceptance criterion 3 (part 2): a recorded agent PR whose action
            // has automerge disabled changes nothing.
            let pool = crate::db::test_pool().await;
            let dir = tempfile::tempdir().unwrap();
            write_youtrack_action(dir.path(), "yt", false);
            let now = "2026-09-01T00:00:00Z";
            ci_failure_store::record_agent_pr(
                &pool,
                "yt",
                "MK-16",
                "dev.a8n.run",
                "contoso/claude-run",
                7,
                "fix/mk-16",
                "open",
                now,
            )
            .await
            .unwrap();
            let backend = Arc::new(StoreBackend::new().with_pr_status("success", true));
            let ctx = ctx_for(pool.clone(), backend.clone(), dir.path());

            let result = handle_action_success_for_pr(
                &ctx,
                &pool,
                "dev.a8n.run",
                "contoso/claude-run",
                7,
                "https://dev.a8n.run/contoso/claude-run/pulls/7",
            )
            .await
            .unwrap();
            assert!(result.contains("automerge disabled"), "{result}");
            assert!(backend.calls().is_empty(), "{:?}", backend.calls());
        }

        #[tokio::test]
        async fn merged_pr_with_unknown_mergeable_takes_no_action() {
            // fj reports `mergeable: null` for a merged PR; the row still gets marked merged.
            let pool = crate::db::test_pool().await;
            let dir = tempfile::tempdir().unwrap();
            write_youtrack_action(dir.path(), "yt", true);
            ci_failure_store::record_agent_pr(
                &pool,
                "yt",
                "MK-16",
                "dev.a8n.run",
                "contoso/claude-run",
                7,
                "fix/mk-16",
                "open",
                "2026-09-01T00:00:00Z",
            )
            .await
            .unwrap();
            let backend = Arc::new(StoreBackend::new());
            *backend.pr_status.lock().unwrap() = Some(crate::provider::PrStatus {
                state: "merged".to_string(),
                mergeable: None,
                behind_base: None,
                head_sha: None,
            });
            let ctx = ctx_for(pool.clone(), backend.clone(), dir.path());

            let result = handle_action_success_for_pr(
                &ctx,
                &pool,
                "dev.a8n.run",
                "contoso/claude-run",
                7,
                "https://dev.a8n.run/contoso/claude-run/pulls/7",
            )
            .await
            .unwrap();
            assert!(result.contains("marked merged"), "{result}");
            let calls = backend.calls();
            assert!(
                !calls.iter().any(|c| c.starts_with("comment MK-16")
                    || c.starts_with("merge_pr")
                    || c.starts_with("update_pr_branch")),
                "{calls:?}"
            );
            // An observed merge retires the row.
            let row = ci_failure_store::agent_pr(&pool, "dev.a8n.run", "contoso/claude-run", 7)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(row.state, "merged");
        }

        #[tokio::test]
        async fn not_mergeable_comments_and_leaves_code_review() {
            // Acceptance criterion 4: a success event that is not mergeable
            // comments and leaves the issue at Code Review (no State / AI Agent
            // write).
            let pool = crate::db::test_pool().await;
            let dir = tempfile::tempdir().unwrap();
            write_youtrack_action(dir.path(), "yt", true);
            let now = "2026-09-01T00:00:00Z";
            ci_failure_store::record_agent_pr(
                &pool,
                "yt",
                "MK-16",
                "dev.a8n.run",
                "contoso/claude-run",
                7,
                "fix/mk-16",
                "open",
                now,
            )
            .await
            .unwrap();
            let backend = Arc::new(StoreBackend::new().with_pr_status("success", false));
            let ctx = ctx_for(pool.clone(), backend.clone(), dir.path());

            let result = handle_action_success_for_pr(
                &ctx,
                &pool,
                "dev.a8n.run",
                "contoso/claude-run",
                7,
                "https://dev.a8n.run/contoso/claude-run/pulls/7",
            )
            .await
            .unwrap();
            assert!(result.contains("not mergeable"), "{result}");
            let calls = backend.calls();
            assert!(
                calls.iter().any(|c| c.starts_with("comment MK-16")),
                "{calls:?}"
            );
            assert!(
                !calls.iter().any(|c| c.starts_with("merge_pr")),
                "{calls:?}"
            );
            assert!(!calls.iter().any(|c| c.contains("set_state")), "{calls:?}");
            assert!(!calls.iter().any(|c| c.contains("Done")), "{calls:?}");
        }

        #[tokio::test]
        async fn behind_base_updates_branch_and_leaves_code_review() {
            // Success and behind_base=true updates the branch
            // instead of merging, comments, and does not touch State/AI Agent.
            let pool = crate::db::test_pool().await;
            let dir = tempfile::tempdir().unwrap();
            write_youtrack_action(dir.path(), "yt", true);
            let now = "2026-09-01T00:00:00Z";
            ci_failure_store::record_agent_pr(
                &pool,
                "yt",
                "MK-16",
                "dev.a8n.run",
                "contoso/claude-run",
                7,
                "fix/mk-16",
                "open",
                now,
            )
            .await
            .unwrap();
            let backend =
                Arc::new(StoreBackend::new().with_pr_status_behind("success", true, Some(true)));
            let ctx = ctx_for(pool.clone(), backend.clone(), dir.path());

            let result = handle_action_success_for_pr(
                &ctx,
                &pool,
                "dev.a8n.run",
                "contoso/claude-run",
                7,
                "https://dev.a8n.run/contoso/claude-run/pulls/7",
            )
            .await
            .unwrap();
            assert!(result.contains("branch updated"), "{result}");
            let calls = backend.calls();
            assert!(calls.iter().any(|c| c == "update_pr_branch 7"), "{calls:?}");
            assert!(
                !calls.iter().any(|c| c.starts_with("merge_pr")),
                "{calls:?}"
            );
            assert!(
                calls.iter().any(|c| c.starts_with("comment MK-16")),
                "{calls:?}"
            );
            assert!(!calls.iter().any(|c| c.contains("set_state")), "{calls:?}");
            assert!(!calls.iter().any(|c| c.contains("Done")), "{calls:?}");
        }

        #[tokio::test]
        async fn behind_base_update_failure_comments_error_and_fails_run() {
            // An update_pr_branch failure (e.g. a 409 conflict)
            // comments the error and fails the run; State/AI Agent unchanged.
            let pool = crate::db::test_pool().await;
            let dir = tempfile::tempdir().unwrap();
            write_youtrack_action(dir.path(), "yt", true);
            let now = "2026-09-01T00:00:00Z";
            ci_failure_store::record_agent_pr(
                &pool,
                "yt",
                "MK-16",
                "dev.a8n.run",
                "contoso/claude-run",
                7,
                "fix/mk-16",
                "open",
                now,
            )
            .await
            .unwrap();
            let backend = Arc::new(
                StoreBackend::new()
                    .with_pr_status_behind("success", true, Some(true))
                    .with_update_pr_branch_error(
                        "409 Conflict: unable to fast-forward: not up to date",
                    ),
            );
            let ctx = ctx_for(pool.clone(), backend.clone(), dir.path());

            let result = handle_action_success_for_pr(
                &ctx,
                &pool,
                "dev.a8n.run",
                "contoso/claude-run",
                7,
                "https://dev.a8n.run/contoso/claude-run/pulls/7",
            )
            .await
            .unwrap_err();
            assert!(
                result.contains("unable to fast-forward: not up to date"),
                "{result}"
            );
            let calls = backend.calls();
            assert!(calls.iter().any(|c| c == "update_pr_branch 7"), "{calls:?}");
            assert!(
                !calls.iter().any(|c| c.starts_with("merge_pr")),
                "{calls:?}"
            );
            let comment = calls
                .iter()
                .find(|c| c.starts_with("comment MK-16"))
                .unwrap_or_else(|| panic!("expected a comment, got {calls:?}"));
            assert!(
                comment.contains("unable to fast-forward: not up to date"),
                "{comment}"
            );
            assert!(!calls.iter().any(|c| c.contains("set_state")), "{calls:?}");
            assert!(!calls.iter().any(|c| c.contains("Done")), "{calls:?}");
        }

        #[tokio::test]
        async fn behind_base_null_comments_and_fails_run() {
            // `behind_base: null` (an `fj` predating the field's introduction) takes no
            // merge action, comments, and fails the run so the gap is visible.
            let pool = crate::db::test_pool().await;
            let dir = tempfile::tempdir().unwrap();
            write_youtrack_action(dir.path(), "yt", true);
            let now = "2026-09-01T00:00:00Z";
            ci_failure_store::record_agent_pr(
                &pool,
                "yt",
                "MK-16",
                "dev.a8n.run",
                "contoso/claude-run",
                7,
                "fix/mk-16",
                "open",
                now,
            )
            .await
            .unwrap();
            let backend =
                Arc::new(StoreBackend::new().with_pr_status_behind("success", true, None));
            let ctx = ctx_for(pool.clone(), backend.clone(), dir.path());

            let result = handle_action_success_for_pr(
                &ctx,
                &pool,
                "dev.a8n.run",
                "contoso/claude-run",
                7,
                "https://dev.a8n.run/contoso/claude-run/pulls/7",
            )
            .await
            .unwrap_err();
            assert!(result.contains("behind_base"), "{result}");
            let calls = backend.calls();
            assert!(
                !calls.iter().any(|c| c.starts_with("merge_pr")),
                "{calls:?}"
            );
            assert!(
                !calls.iter().any(|c| c.starts_with("update_pr_branch")),
                "{calls:?}"
            );
            assert!(
                calls.iter().any(|c| c.starts_with("comment MK-16")),
                "{calls:?}"
            );
        }

        #[tokio::test]
        async fn merge_error_comments_error_and_fails_run() {
            // A merge failure (e.g. Forgejo's 405 refusal) comments
            // the error and fails the run; State/AI Agent unchanged.
            let pool = crate::db::test_pool().await;
            let dir = tempfile::tempdir().unwrap();
            write_youtrack_action(dir.path(), "yt", true);
            let now = "2026-09-01T00:00:00Z";
            ci_failure_store::record_agent_pr(
                &pool,
                "yt",
                "MK-16",
                "dev.a8n.run",
                "contoso/claude-run",
                7,
                "fix/mk-16",
                "open",
                now,
            )
            .await
            .unwrap();
            let backend = Arc::new(
                StoreBackend::new().with_pr_status("success", true).with_merge_pr_error(
                    "405 Method Not Allowed: not allowed to merge [reason: The head branch is behind the base branch]",
                ),
            );
            let ctx = ctx_for(pool.clone(), backend.clone(), dir.path());

            let result = handle_action_success_for_pr(
                &ctx,
                &pool,
                "dev.a8n.run",
                "contoso/claude-run",
                7,
                "https://dev.a8n.run/contoso/claude-run/pulls/7",
            )
            .await
            .unwrap_err();
            assert!(result.contains("not allowed to merge"), "{result}");
            let calls = backend.calls();
            assert!(calls.iter().any(|c| c == "merge_pr 7"), "{calls:?}");
            let comment = calls
                .iter()
                .find(|c| c.starts_with("comment MK-16"))
                .unwrap_or_else(|| panic!("expected a comment, got {calls:?}"));
            assert!(comment.contains("not allowed to merge"), "{comment}");
            assert!(!calls.iter().any(|c| c.contains("set_state")), "{calls:?}");
            assert!(!calls.iter().any(|c| c.contains("Done")), "{calls:?}");
        }
    }

    // ---- The reconciler backstop, its fault visibility, and its
    // agreement with the webhook entry point on the shared decision function ---
    mod automerge_reconciler {
        use super::automerge_success::write_youtrack_action;
        use super::*;

        fn ctx_for(
            pool: DbPool,
            backend: Arc<StoreBackend>,
            paths_dir: &std::path::Path,
        ) -> MintContext {
            let classifier: Arc<dyn FailureClassifier> = Arc::new(CountingClassifier::new());
            store_mint_context_in(
                pool,
                backend,
                classifier,
                paths_dir,
                Arc::new(crate::provider::cli::testutil::StubRunner::new()),
            )
        }

        /// The reconciler (unlike the webhook entry point) lists open PRs
        /// itself first, to confirm the row's PR is still open before
        /// deciding; that extra `list_open_prs` call is excluded here so a
        /// webhook-vs-reconciler call-shape comparison stays scoped to the
        /// shared decision function's own calls (`pr_status`,
        /// `merge_pr`/`update_pr_branch`, `comment`).
        fn without_listing(calls: Vec<String>) -> Vec<String> {
            calls
                .into_iter()
                .filter(|c| !c.starts_with("list_open_prs"))
                .collect()
        }

        /// One (state, mergeable, behind_base) combination, driven through both
        /// [`handle_action_success_for_pr`] (the webhook entry point) and
        /// [`reconcile_automerge`] (the tick-driven entry point),
        /// against two otherwise-identical setups, asserting the two entry
        /// points make IDENTICAL backend calls: this is the "one shared decision
        /// function" acceptance criterion.
        async fn assert_same_decision(state: &str, mergeable: bool, behind_base: Option<bool>) {
            let now = "2026-09-01T00:00:00Z";

            let pool_a = crate::db::test_pool().await;
            let dir_a = tempfile::tempdir().unwrap();
            write_youtrack_action(dir_a.path(), "yt", true);
            ci_failure_store::record_agent_pr(
                &pool_a,
                "yt",
                "MK-16",
                "dev.a8n.run",
                "contoso/claude-run",
                7,
                "fix/mk-16",
                "open",
                now,
            )
            .await
            .unwrap();
            let backend_a = Arc::new(
                StoreBackend::new()
                    .with_open_prs(vec![pr(7, "")])
                    .with_pr_status_behind(state, mergeable, behind_base),
            );
            let ctx_a = ctx_for(pool_a.clone(), backend_a.clone(), dir_a.path());
            let _ = handle_action_success_for_pr(
                &ctx_a,
                &pool_a,
                "dev.a8n.run",
                "contoso/claude-run",
                7,
                "https://dev.a8n.run/contoso/claude-run/pulls/7",
            )
            .await;

            let pool_b = crate::db::test_pool().await;
            let dir_b = tempfile::tempdir().unwrap();
            write_youtrack_action(dir_b.path(), "yt", true);
            ci_failure_store::record_agent_pr(
                &pool_b,
                "yt",
                "MK-16",
                "dev.a8n.run",
                "contoso/claude-run",
                7,
                "fix/mk-16",
                "open",
                now,
            )
            .await
            .unwrap();
            let backend_b = Arc::new(
                StoreBackend::new()
                    .with_open_prs(vec![pr(7, "")])
                    .with_pr_status_behind(state, mergeable, behind_base),
            );
            let ctx_b = ctx_for(pool_b.clone(), backend_b.clone(), dir_b.path());
            reconcile_automerge(
                &pool_b,
                &ctx_b,
                &crate::concurrency::KeyGate::new(),
                &mut std::collections::HashMap::new(),
            )
            .await;

            assert_eq!(
                without_listing(backend_a.calls()),
                without_listing(backend_b.calls()),
                "webhook path and reconciler diverged for state={state} mergeable={mergeable} behind_base={behind_base:?}"
            );
        }

        #[tokio::test]
        async fn webhook_and_reconciler_agree_on_every_decision_shape() {
            assert_same_decision("success", true, Some(false)).await; // green + mergeable
            assert_same_decision("success", true, Some(true)).await; // green + behind base
            assert_same_decision("success", false, Some(false)).await; // green + not mergeable
            assert_same_decision("pending", false, Some(false)).await; // not green
            assert_same_conflict_decision().await; // behind base + update conflict (CLAUDE-380)
        }

        /// The CLAUDE-380 conflict-dispatch decision shape: behind base, and
        /// `update_pr_branch` fails with a conflict. With no comment-webhook
        /// dispatch wired (same as every other case in this test, and every
        /// `ctx_for` here), both entry points fall back to the same human
        /// comment, with identical backend calls.
        async fn assert_same_conflict_decision() {
            let now = "2026-09-01T00:00:00Z";

            let pool_a = crate::db::test_pool().await;
            let dir_a = tempfile::tempdir().unwrap();
            write_youtrack_action(dir_a.path(), "yt", true);
            ci_failure_store::record_agent_pr(
                &pool_a,
                "yt",
                "MK-16",
                "dev.a8n.run",
                "contoso/claude-run",
                7,
                "fix/mk-16",
                "open",
                now,
            )
            .await
            .unwrap();
            let backend_a = Arc::new(
                StoreBackend::new()
                    .with_open_prs(vec![pr(7, "")])
                    .with_pr_status_full("success", Some(false), Some(true), Some("sha-1"))
                    .with_update_pr_branch_error("merge conflict"),
            );
            let ctx_a = ctx_for(pool_a.clone(), backend_a.clone(), dir_a.path());
            let _ = handle_action_success_for_pr(
                &ctx_a,
                &pool_a,
                "dev.a8n.run",
                "contoso/claude-run",
                7,
                "https://dev.a8n.run/contoso/claude-run/pulls/7",
            )
            .await;

            let pool_b = crate::db::test_pool().await;
            let dir_b = tempfile::tempdir().unwrap();
            write_youtrack_action(dir_b.path(), "yt", true);
            ci_failure_store::record_agent_pr(
                &pool_b,
                "yt",
                "MK-16",
                "dev.a8n.run",
                "contoso/claude-run",
                7,
                "fix/mk-16",
                "open",
                now,
            )
            .await
            .unwrap();
            let backend_b = Arc::new(
                StoreBackend::new()
                    .with_open_prs(vec![pr(7, "")])
                    .with_pr_status_full("success", Some(false), Some(true), Some("sha-1"))
                    .with_update_pr_branch_error("merge conflict"),
            );
            let ctx_b = ctx_for(pool_b.clone(), backend_b.clone(), dir_b.path());
            reconcile_automerge(
                &pool_b,
                &ctx_b,
                &crate::concurrency::KeyGate::new(),
                &mut std::collections::HashMap::new(),
            )
            .await;

            assert_eq!(
                without_listing(backend_a.calls()),
                without_listing(backend_b.calls()),
                "webhook path and reconciler diverged on the conflict decision shape"
            );
        }

        #[tokio::test]
        async fn reconciler_merges_open_automerge_pr_with_no_webhook_event() {
            // Acceptance: an open agent PR with automerge on and status success
            // is merged by the reconciler alone, with no webhook event at all.
            let pool = crate::db::test_pool().await;
            let dir = tempfile::tempdir().unwrap();
            write_youtrack_action(dir.path(), "yt", true);
            let now = "2026-09-01T00:00:00Z";
            ci_failure_store::record_agent_pr(
                &pool,
                "yt",
                "MK-16",
                "dev.a8n.run",
                "contoso/claude-run",
                7,
                "fix/mk-16",
                "open",
                now,
            )
            .await
            .unwrap();
            let backend = Arc::new(
                StoreBackend::new()
                    .with_open_prs(vec![pr(7, "")])
                    .with_pr_status("success", true),
            );
            let ctx = ctx_for(pool.clone(), backend.clone(), dir.path());

            reconcile_automerge(
                &pool,
                &ctx,
                &crate::concurrency::KeyGate::new(),
                &mut std::collections::HashMap::new(),
            )
            .await;

            let calls = backend.calls();
            assert!(calls.iter().any(|c| c == "merge_pr 7"), "{calls:?}");
            // The reconciler makes no `State` or `AI Agent` write; a
            // YouTrack workflow resolves the issue once it observes the merge.
            assert!(!calls.iter().any(|c| c.contains("set_state")), "{calls:?}");
        }

        #[tokio::test]
        async fn reconciler_leaves_automerge_disabled_pr_alone() {
            let pool = crate::db::test_pool().await;
            let dir = tempfile::tempdir().unwrap();
            write_youtrack_action(dir.path(), "yt", false);
            let now = "2026-09-01T00:00:00Z";
            ci_failure_store::record_agent_pr(
                &pool,
                "yt",
                "MK-16",
                "dev.a8n.run",
                "contoso/claude-run",
                7,
                "fix/mk-16",
                "open",
                now,
            )
            .await
            .unwrap();
            let backend = Arc::new(
                StoreBackend::new()
                    .with_open_prs(vec![pr(7, "")])
                    .with_pr_status("success", true),
            );
            let ctx = ctx_for(pool.clone(), backend.clone(), dir.path());

            reconcile_automerge(
                &pool,
                &ctx,
                &crate::concurrency::KeyGate::new(),
                &mut std::collections::HashMap::new(),
            )
            .await;

            // The reconciler still lists open PRs for the (host, repo)
            // group before checking any row's own action config.
            assert_eq!(
                backend.calls(),
                vec!["list_open_prs dev.a8n.run contoso/claude-run"],
                "{:?}",
                backend.calls()
            );
        }

        #[tokio::test]
        async fn reconciler_leaves_non_open_pr_alone() {
            let pool = crate::db::test_pool().await;
            let dir = tempfile::tempdir().unwrap();
            write_youtrack_action(dir.path(), "yt", true);
            let now = "2026-09-01T00:00:00Z";
            ci_failure_store::record_agent_pr(
                &pool,
                "yt",
                "MK-16",
                "dev.a8n.run",
                "contoso/claude-run",
                7,
                "fix/mk-16",
                "merged",
                now,
            )
            .await
            .unwrap();
            let backend = Arc::new(
                StoreBackend::new()
                    .with_open_prs(vec![pr(7, "")])
                    .with_pr_status("success", true),
            );
            let ctx = ctx_for(pool.clone(), backend.clone(), dir.path());

            reconcile_automerge(
                &pool,
                &ctx,
                &crate::concurrency::KeyGate::new(),
                &mut std::collections::HashMap::new(),
            )
            .await;

            assert!(backend.calls().is_empty(), "{:?}", backend.calls());
        }

        async fn seed_open_row(pool: &DbPool) {
            ci_failure_store::record_agent_pr(
                pool,
                "yt",
                "MK-16",
                "dev.a8n.run",
                "contoso/claude-run",
                7,
                "fix/mk-16",
                "open",
                "2026-09-01T00:00:00Z",
            )
            .await
            .unwrap();
        }

        async fn row_state(pool: &DbPool) -> String {
            ci_failure_store::agent_pr(pool, "dev.a8n.run", "contoso/claude-run", 7)
                .await
                .unwrap()
                .unwrap()
                .state
        }

        /// CLAUDE-397: count of `runs` rows for `WEBHOOK_MINT_ACTION`, used to
        /// assert a no-action reconciler pass opens zero of them and an
        /// acted/failed pass opens exactly one.
        async fn webhook_triage_runs_count(pool: &DbPool) -> i64 {
            sqlx::query_scalar(&crate::db::q(
                pool,
                "SELECT COUNT(*) FROM runs WHERE action_name = ?",
            ))
            .bind(WEBHOOK_MINT_ACTION)
            .fetch_one(pool)
            .await
            .unwrap()
        }

        #[tokio::test]
        async fn reconciler_marks_merged_pr_merged_and_never_walks_it_again() {
            // `pr_status` reporting `merged` must retire the row.
            let pool = crate::db::test_pool().await;
            let dir = tempfile::tempdir().unwrap();
            write_youtrack_action(dir.path(), "yt", true);
            seed_open_row(&pool).await;
            let backend = Arc::new(
                StoreBackend::new()
                    .with_open_prs(vec![pr(7, "")])
                    .with_pr_status("merged", false),
            );
            let ctx = ctx_for(pool.clone(), backend.clone(), dir.path());
            let gate = crate::concurrency::KeyGate::new();

            reconcile_automerge(&pool, &ctx, &gate, &mut std::collections::HashMap::new()).await;
            assert_eq!(row_state(&pool).await, "merged");
            assert!(ci_failure_store::open_agent_prs(&pool)
                .await
                .unwrap()
                .is_empty());

            let calls_after_first = backend.calls().len();
            reconcile_automerge(&pool, &ctx, &gate, &mut std::collections::HashMap::new()).await;
            assert_eq!(backend.calls().len(), calls_after_first);
        }

        #[tokio::test]
        async fn runner_merge_marks_the_row_merged() {
            // The runner's own successful merge records it too.
            let pool = crate::db::test_pool().await;
            let dir = tempfile::tempdir().unwrap();
            write_youtrack_action(dir.path(), "yt", true);
            seed_open_row(&pool).await;
            let backend = Arc::new(
                StoreBackend::new()
                    .with_open_prs(vec![pr(7, "")])
                    .with_pr_status("success", true),
            );
            let ctx = ctx_for(pool.clone(), backend.clone(), dir.path());

            reconcile_automerge(
                &pool,
                &ctx,
                &crate::concurrency::KeyGate::new(),
                &mut std::collections::HashMap::new(),
            )
            .await;

            assert!(backend.calls().iter().any(|c| c == "merge_pr 7"));
            assert_eq!(row_state(&pool).await, "merged");
        }

        #[tokio::test]
        async fn reconciler_retires_row_for_pr_no_longer_listed_open() {
            // A PR closed without merge is absent from the open
            // listing; it is marked closed with no `pr_status` call.
            let pool = crate::db::test_pool().await;
            let dir = tempfile::tempdir().unwrap();
            write_youtrack_action(dir.path(), "yt", true);
            seed_open_row(&pool).await;
            let backend = Arc::new(StoreBackend::new().with_open_prs(vec![pr(99, "")]));
            let ctx = ctx_for(pool.clone(), backend.clone(), dir.path());

            reconcile_automerge(
                &pool,
                &ctx,
                &crate::concurrency::KeyGate::new(),
                &mut std::collections::HashMap::new(),
            )
            .await;

            assert_eq!(row_state(&pool).await, "closed");
            assert_eq!(
                backend.calls(),
                vec!["list_open_prs dev.a8n.run contoso/claude-run"],
                "retiring the row makes no `pr_status` call: {:?}",
                backend.calls()
            );
            assert!(ci_failure_store::open_agent_prs(&pool)
                .await
                .unwrap()
                .is_empty());
        }

        #[tokio::test]
        async fn reconciler_leaves_rows_open_when_the_listing_fails() {
            // An unknown is never read as "closed".
            let pool = crate::db::test_pool().await;
            let dir = tempfile::tempdir().unwrap();
            write_youtrack_action(dir.path(), "yt", true);
            seed_open_row(&pool).await;
            let backend = Arc::new(StoreBackend::new().with_open_prs_error("fj: connection reset"));
            let ctx = ctx_for(pool.clone(), backend.clone(), dir.path());

            reconcile_automerge(
                &pool,
                &ctx,
                &crate::concurrency::KeyGate::new(),
                &mut std::collections::HashMap::new(),
            )
            .await;

            assert_eq!(row_state(&pool).await, "open");
            assert_eq!(
                backend.calls(),
                vec!["list_open_prs dev.a8n.run contoso/claude-run"],
                "{:?}",
                backend.calls()
            );
        }

        async fn recovered_events(pool: &DbPool) -> Vec<String> {
            state::run_events_for_action(pool, WEBHOOK_MINT_ACTION, 50)
                .await
                .unwrap()
                .iter()
                .filter(|e| e.message.contains("recovered after"))
                .map(|e| e.message.clone())
                .collect()
        }

        #[tokio::test]
        async fn listing_recovery_after_failure_is_recorded_once() {
            // A failing listing then a succeeding one writes exactly one
            // `recovered` row naming the repo and the failed-pass count.
            let pool = crate::db::test_pool().await;
            let dir = tempfile::tempdir().unwrap();
            write_youtrack_action(dir.path(), "yt", true);
            seed_open_row(&pool).await;
            let backend = Arc::new(StoreBackend::new().with_open_prs_error("fj: connection reset"));
            let ctx = ctx_for(pool.clone(), backend.clone(), dir.path());
            let gate = crate::concurrency::KeyGate::new();
            let mut listing_failures = std::collections::HashMap::new();

            reconcile_automerge(&pool, &ctx, &gate, &mut listing_failures).await;
            assert!(recovered_events(&pool).await.is_empty());

            *backend.open_prs_error.lock().unwrap() = None;
            *backend.open_prs.lock().unwrap() = vec![pr(7, "")];
            reconcile_automerge(&pool, &ctx, &gate, &mut listing_failures).await;

            let recovered = recovered_events(&pool).await;
            assert_eq!(recovered.len(), 1, "{recovered:?}");
            assert!(
                recovered[0].contains("contoso/claude-run")
                    && recovered[0].contains("1 failed pass"),
                "{recovered:?}"
            );

            // A second consecutive success writes no second row.
            reconcile_automerge(&pool, &ctx, &gate, &mut listing_failures).await;
            assert_eq!(recovered_events(&pool).await.len(), 1);
        }

        #[tokio::test]
        async fn listing_failure_alone_writes_no_recovery_row() {
            let pool = crate::db::test_pool().await;
            let dir = tempfile::tempdir().unwrap();
            write_youtrack_action(dir.path(), "yt", true);
            seed_open_row(&pool).await;
            let backend = Arc::new(StoreBackend::new().with_open_prs_error("fj: connection reset"));
            let ctx = ctx_for(pool.clone(), backend.clone(), dir.path());
            let mut listing_failures = std::collections::HashMap::new();

            reconcile_automerge(
                &pool,
                &ctx,
                &crate::concurrency::KeyGate::new(),
                &mut listing_failures,
            )
            .await;

            assert!(recovered_events(&pool).await.is_empty());
        }

        #[tokio::test]
        async fn listing_success_with_no_prior_failure_writes_no_recovery_row() {
            let pool = crate::db::test_pool().await;
            let dir = tempfile::tempdir().unwrap();
            write_youtrack_action(dir.path(), "yt", true);
            seed_open_row(&pool).await;
            let backend = Arc::new(
                StoreBackend::new()
                    .with_open_prs(vec![pr(7, "")])
                    .with_pr_status("pending", false),
            );
            let ctx = ctx_for(pool.clone(), backend.clone(), dir.path());
            let mut listing_failures = std::collections::HashMap::new();

            reconcile_automerge(
                &pool,
                &ctx,
                &crate::concurrency::KeyGate::new(),
                &mut listing_failures,
            )
            .await;

            assert!(recovered_events(&pool).await.is_empty());
        }

        #[tokio::test]
        async fn reconciler_does_nothing_while_globally_paused() {
            let pool = crate::db::test_pool().await;
            let dir = tempfile::tempdir().unwrap();
            write_youtrack_action(dir.path(), "yt", true);
            seed_open_row(&pool).await;
            state::global_pause_set(&pool, Some("test")).await.unwrap();
            let backend = Arc::new(StoreBackend::new().with_pr_status("success", true));
            let ctx = ctx_for(pool.clone(), backend.clone(), dir.path());

            reconcile_automerge(
                &pool,
                &ctx,
                &crate::concurrency::KeyGate::new(),
                &mut std::collections::HashMap::new(),
            )
            .await;

            assert!(backend.calls().is_empty(), "{:?}", backend.calls());
            assert_eq!(row_state(&pool).await, "open");
        }

        #[tokio::test]
        async fn reconciler_does_nothing_while_webhook_action_paused() {
            let pool = crate::db::test_pool().await;
            let dir = tempfile::tempdir().unwrap();
            write_youtrack_action(dir.path(), "yt", true);
            seed_open_row(&pool).await;
            state::action_pause_set(&pool, WEBHOOK_MINT_ACTION, Some("test"))
                .await
                .unwrap();
            let backend = Arc::new(StoreBackend::new().with_pr_status("success", true));
            let ctx = ctx_for(pool.clone(), backend.clone(), dir.path());

            reconcile_automerge(
                &pool,
                &ctx,
                &crate::concurrency::KeyGate::new(),
                &mut std::collections::HashMap::new(),
            )
            .await;
            assert!(backend.calls().is_empty(), "{:?}", backend.calls());
        }

        #[tokio::test]
        async fn reconciler_leaves_non_success_pr_alone() {
            let pool = crate::db::test_pool().await;
            let dir = tempfile::tempdir().unwrap();
            write_youtrack_action(dir.path(), "yt", true);
            let now = "2026-09-01T00:00:00Z";
            ci_failure_store::record_agent_pr(
                &pool,
                "yt",
                "MK-16",
                "dev.a8n.run",
                "contoso/claude-run",
                7,
                "fix/mk-16",
                "open",
                now,
            )
            .await
            .unwrap();
            let backend = Arc::new(
                StoreBackend::new()
                    .with_open_prs(vec![pr(7, "")])
                    .with_pr_status("pending", false),
            );
            let ctx = ctx_for(pool.clone(), backend.clone(), dir.path());

            reconcile_automerge(
                &pool,
                &ctx,
                &crate::concurrency::KeyGate::new(),
                &mut std::collections::HashMap::new(),
            )
            .await;

            assert!(
                !backend.calls().iter().any(|c| c.starts_with("merge_pr")),
                "{:?}",
                backend.calls()
            );
        }

        #[tokio::test]
        async fn identical_pr_status_failure_comments_once_but_records_every_pass() {
            // An unchanged fault is recorded each tick, commented once;
            // a changed message comments again.
            let pool = crate::db::test_pool().await;
            let dir = tempfile::tempdir().unwrap();
            write_youtrack_action(dir.path(), "yt", true);
            seed_open_row(&pool).await;
            // No canned pr_status: every call fails with the same message.
            let backend = Arc::new(StoreBackend::new().with_open_prs(vec![pr(7, "")]));
            let ctx = ctx_for(pool.clone(), backend.clone(), dir.path());
            let gate = crate::concurrency::KeyGate::new();
            let comments = |b: &StoreBackend| {
                b.calls()
                    .iter()
                    .filter(|c| c.starts_with("comment MK-16"))
                    .count()
            };
            let events = || async {
                state::run_events_for_action(&pool, WEBHOOK_MINT_ACTION, 50)
                    .await
                    .unwrap()
                    .iter()
                    .filter(|e| e.message.contains("pr_status failed"))
                    .count()
            };

            reconcile_automerge(&pool, &ctx, &gate, &mut std::collections::HashMap::new()).await;
            reconcile_automerge(&pool, &ctx, &gate, &mut std::collections::HashMap::new()).await;
            assert_eq!(comments(&backend), 1, "{:?}", backend.calls());
            assert_eq!(events().await, 2);

            // A different failure message posts a new comment.
            // Simulate a different message having been recorded last.
            state::run_events_record(
                &pool,
                WEBHOOK_MINT_ACTION,
                "2999-01-01T00:00:00Z",
                &[state::InputEvent {
                    level: "error".into(),
                    context: None,
                    message: "PR contoso/claude-run#7: pr_status failed: something else".into(),
                }],
            )
            .await
            .unwrap();
            reconcile_automerge(&pool, &ctx, &gate, &mut std::collections::HashMap::new()).await;
            assert_eq!(comments(&backend), 2, "{:?}", backend.calls());
        }

        #[tokio::test]
        async fn identical_behind_base_missing_comments_once_but_records_every_pass() {
            let pool = crate::db::test_pool().await;
            let dir = tempfile::tempdir().unwrap();
            write_youtrack_action(dir.path(), "yt", true);
            seed_open_row(&pool).await;
            let backend = Arc::new(
                StoreBackend::new()
                    .with_open_prs(vec![pr(7, "")])
                    .with_pr_status_behind("success", true, None),
            );
            let ctx = ctx_for(pool.clone(), backend.clone(), dir.path());
            let gate = crate::concurrency::KeyGate::new();
            let comments = |b: &StoreBackend| {
                b.calls()
                    .iter()
                    .filter(|c| c.starts_with("comment MK-16"))
                    .count()
            };
            let events = || async {
                state::run_events_for_action(&pool, WEBHOOK_MINT_ACTION, 50)
                    .await
                    .unwrap()
                    .iter()
                    .filter(|e| e.message.contains("behind_base missing"))
                    .count()
            };

            reconcile_automerge(&pool, &ctx, &gate, &mut std::collections::HashMap::new()).await;
            reconcile_automerge(&pool, &ctx, &gate, &mut std::collections::HashMap::new()).await;
            assert_eq!(comments(&backend), 1, "{:?}", backend.calls());
            assert_eq!(events().await, 2);
        }

        #[tokio::test]
        async fn identical_update_failed_comments_once_but_records_every_pass() {
            let pool = crate::db::test_pool().await;
            let dir = tempfile::tempdir().unwrap();
            write_youtrack_action(dir.path(), "yt", true);
            seed_open_row(&pool).await;
            let backend = Arc::new(
                StoreBackend::new()
                    .with_open_prs(vec![pr(7, "")])
                    .with_pr_status_behind("success", false, Some(true))
                    .with_update_pr_branch_error("boom"),
            );
            let ctx = ctx_for(pool.clone(), backend.clone(), dir.path());
            let gate = crate::concurrency::KeyGate::new();
            let comments = |b: &StoreBackend| {
                b.calls()
                    .iter()
                    .filter(|c| c.starts_with("comment MK-16"))
                    .count()
            };
            let events = || async {
                state::run_events_for_action(&pool, WEBHOOK_MINT_ACTION, 50)
                    .await
                    .unwrap()
                    .iter()
                    .filter(|e| e.message.contains("update_pr_branch failed"))
                    .count()
            };

            reconcile_automerge(&pool, &ctx, &gate, &mut std::collections::HashMap::new()).await;
            reconcile_automerge(&pool, &ctx, &gate, &mut std::collections::HashMap::new()).await;
            assert_eq!(comments(&backend), 1, "{:?}", backend.calls());
            assert_eq!(events().await, 2);
        }

        #[tokio::test]
        async fn identical_not_mergeable_comments_once_but_records_every_pass() {
            let pool = crate::db::test_pool().await;
            let dir = tempfile::tempdir().unwrap();
            write_youtrack_action(dir.path(), "yt", true);
            seed_open_row(&pool).await;
            let backend = Arc::new(
                StoreBackend::new()
                    .with_open_prs(vec![pr(7, "")])
                    .with_pr_status_behind("success", false, Some(false)),
            );
            let ctx = ctx_for(pool.clone(), backend.clone(), dir.path());
            let gate = crate::concurrency::KeyGate::new();
            let comments = |b: &StoreBackend| {
                b.calls()
                    .iter()
                    .filter(|c| c.starts_with("comment MK-16"))
                    .count()
            };
            let events = || async {
                state::run_events_for_action(&pool, WEBHOOK_MINT_ACTION, 50)
                    .await
                    .unwrap()
                    .iter()
                    .filter(|e| e.message.contains("not mergeable"))
                    .count()
            };

            reconcile_automerge(&pool, &ctx, &gate, &mut std::collections::HashMap::new()).await;
            reconcile_automerge(&pool, &ctx, &gate, &mut std::collections::HashMap::new()).await;
            assert_eq!(comments(&backend), 1, "{:?}", backend.calls());
            assert_eq!(events().await, 2);
        }

        #[tokio::test]
        async fn identical_merge_failed_comments_once_but_records_every_pass() {
            let pool = crate::db::test_pool().await;
            let dir = tempfile::tempdir().unwrap();
            write_youtrack_action(dir.path(), "yt", true);
            seed_open_row(&pool).await;
            let backend = Arc::new(
                StoreBackend::new()
                    .with_open_prs(vec![pr(7, "")])
                    .with_pr_status("success", true)
                    .with_merge_pr_error("boom"),
            );
            let ctx = ctx_for(pool.clone(), backend.clone(), dir.path());
            let gate = crate::concurrency::KeyGate::new();
            let comments = |b: &StoreBackend| {
                b.calls()
                    .iter()
                    .filter(|c| c.starts_with("comment MK-16"))
                    .count()
            };
            let events = || async {
                state::run_events_for_action(&pool, WEBHOOK_MINT_ACTION, 50)
                    .await
                    .unwrap()
                    .iter()
                    .filter(|e| e.message.contains("complete_automerge failed"))
                    .count()
            };

            reconcile_automerge(&pool, &ctx, &gate, &mut std::collections::HashMap::new()).await;
            reconcile_automerge(&pool, &ctx, &gate, &mut std::collections::HashMap::new()).await;
            assert_eq!(comments(&backend), 1, "{:?}", backend.calls());
            assert_eq!(events().await, 2);
        }

        #[tokio::test]
        async fn two_prs_with_the_same_outcome_each_get_their_own_comment() {
            let pool = crate::db::test_pool().await;
            let dir = tempfile::tempdir().unwrap();
            write_youtrack_action(dir.path(), "yt", true);
            seed_open_row(&pool).await;
            ci_failure_store::record_agent_pr(
                &pool,
                "yt",
                "MK-17",
                "dev.a8n.run",
                "contoso/claude-run",
                8,
                "fix/mk-17",
                "open",
                "2026-09-01T00:00:00Z",
            )
            .await
            .unwrap();
            let backend = Arc::new(
                StoreBackend::new()
                    .with_open_prs(vec![pr(7, ""), pr(8, "")])
                    .with_pr_status_behind("success", false, Some(false)),
            );
            let ctx = ctx_for(pool.clone(), backend.clone(), dir.path());
            let gate = crate::concurrency::KeyGate::new();

            reconcile_automerge(&pool, &ctx, &gate, &mut std::collections::HashMap::new()).await;

            let calls = backend.calls();
            assert_eq!(
                calls
                    .iter()
                    .filter(|c| c.starts_with("comment MK-16"))
                    .count(),
                1,
                "{calls:?}"
            );
            assert_eq!(
                calls
                    .iter()
                    .filter(|c| c.starts_with("comment MK-17"))
                    .count(),
                1,
                "{calls:?}"
            );
        }

        #[tokio::test]
        async fn a_change_of_outcome_type_posts_a_new_comment() {
            // Not just a different error text for the same outcome: a
            // different outcome entirely (not-mergeable, then update-failed)
            // also posts again, since both share the same `PR {repo}#{pr}: `
            // dedupe prefix and only their message text differs.
            let pool = crate::db::test_pool().await;
            let dir = tempfile::tempdir().unwrap();
            write_youtrack_action(dir.path(), "yt", true);
            seed_open_row(&pool).await;
            let backend = Arc::new(
                StoreBackend::new()
                    .with_open_prs(vec![pr(7, "")])
                    .with_pr_status_behind("success", false, Some(false)),
            );
            let ctx = ctx_for(pool.clone(), backend.clone(), dir.path());
            let gate = crate::concurrency::KeyGate::new();
            let comments = |b: &StoreBackend| {
                b.calls()
                    .iter()
                    .filter(|c| c.starts_with("comment MK-16"))
                    .count()
            };

            reconcile_automerge(&pool, &ctx, &gate, &mut std::collections::HashMap::new()).await;
            assert_eq!(comments(&backend), 1, "{:?}", backend.calls());

            *backend.pr_status.lock().unwrap() = Some(crate::provider::PrStatus {
                state: "success".to_string(),
                mergeable: Some(false),
                behind_base: Some(true),
                head_sha: None,
            });
            *backend.update_pr_branch_error.lock().unwrap() = Some("boom".to_string());
            reconcile_automerge(&pool, &ctx, &gate, &mut std::collections::HashMap::new()).await;
            assert_eq!(comments(&backend), 2, "{:?}", backend.calls());
        }

        #[tokio::test]
        async fn action_load_failure_logs_errors_records_event_and_comments() {
            // Acceptance: an action-load failure for a known, recorded,
            // automerge-eligible PR is loud: error log (asserted implicitly by
            // the run_events row below, which is written from the same call
            // site), a `run_events` row, and a best-effort issue comment.
            let pool = crate::db::test_pool().await;
            let dir = tempfile::tempdir().unwrap();
            // Deliberately no `write_youtrack_action`: the row names an action
            // that does not exist on disk, so `crate::action::load` fails.
            let now = "2026-09-01T00:00:00Z";
            ci_failure_store::record_agent_pr(
                &pool,
                "does-not-exist",
                "MK-16",
                "dev.a8n.run",
                "contoso/claude-run",
                7,
                "fix/mk-16",
                "open",
                now,
            )
            .await
            .unwrap();
            let backend = Arc::new(StoreBackend::new());
            let ctx = ctx_for(pool.clone(), backend.clone(), dir.path());

            let result = handle_action_success_for_pr(
                &ctx,
                &pool,
                "dev.a8n.run",
                "contoso/claude-run",
                7,
                "https://dev.a8n.run/contoso/claude-run/pulls/7",
            )
            .await
            .unwrap_err();
            assert!(result.contains("loading action"), "{result}");

            let comment = backend
                .calls()
                .into_iter()
                .find(|c| c.starts_with("comment MK-16"))
                .unwrap_or_else(|| panic!("expected an issue comment, got {:?}", backend.calls()));
            assert!(comment.contains("automerge status"), "{comment}");

            let events = state::run_events_for_action(&pool, WEBHOOK_MINT_ACTION, 10)
                .await
                .unwrap();
            assert!(
                events.iter().any(|e| e.message.contains("loading action")),
                "{events:?}"
            );
        }

        #[tokio::test]
        async fn missing_host_notifies_every_open_automerge_pr_for_the_repo() {
            // Acceptance: a fault before the commit is matched to a PR (host
            // derivation) still concerns this repo, so every currently open,
            // automerge-enabled agent PR recorded for it is notified: error
            // log, `run_events` row, and a best-effort issue comment.
            let pool = crate::db::test_pool().await;
            let dir = tempfile::tempdir().unwrap();
            write_youtrack_action(dir.path(), "yt", true);
            let now = "2026-09-01T00:00:00Z";
            ci_failure_store::record_agent_pr(
                &pool,
                "yt",
                "MK-16",
                "dev.a8n.run",
                "contoso/claude-run",
                7,
                "fix/mk-16",
                "open",
                now,
            )
            .await
            .unwrap();
            let backend = Arc::new(StoreBackend::new());
            let ctx = ctx_for(pool.clone(), backend.clone(), dir.path());
            let mint = Arc::new(ctx);

            let ev = VerifiedActionSuccessEvent {
                repo: "contoso/claude-run".to_string(),
                commit_sha: "deadbeef".to_string(),
                // No `scheme://host` prefix: `forge_host_from_url` yields `None`.
                html_url: String::new(),
                source: "test".to_string(),
            };
            process_action_success_event(
                ev,
                pool.clone(),
                Some(mint),
                crate::concurrency::KeyGate::new(),
            )
            .await;

            let comment = backend
                .calls()
                .into_iter()
                .find(|c| c.starts_with("comment MK-16"))
                .unwrap_or_else(|| panic!("expected an issue comment, got {:?}", backend.calls()));
            assert!(comment.contains("automerge status"), "{comment}");

            let events = state::run_events_for_action(&pool, WEBHOOK_MINT_ACTION, 10)
                .await
                .unwrap();
            assert!(
                events
                    .iter()
                    .any(|e| e.message.contains("could not derive forge host")),
                "{events:?}"
            );
        }

        #[tokio::test]
        async fn list_open_prs_failure_notifies_every_open_automerge_pr_for_the_repo() {
            let pool = crate::db::test_pool().await;
            let dir = tempfile::tempdir().unwrap();
            write_youtrack_action(dir.path(), "yt", true);
            let now = "2026-09-01T00:00:00Z";
            ci_failure_store::record_agent_pr(
                &pool,
                "yt",
                "MK-16",
                "dev.a8n.run",
                "contoso/claude-run",
                7,
                "fix/mk-16",
                "open",
                now,
            )
            .await
            .unwrap();
            let backend = Arc::new(StoreBackend::new().with_open_prs_error("fj: connection reset"));
            let ctx = ctx_for(pool.clone(), backend.clone(), dir.path());
            let mint = Arc::new(ctx);

            let ev = VerifiedActionSuccessEvent {
                repo: "contoso/claude-run".to_string(),
                commit_sha: "deadbeef".to_string(),
                html_url: "https://dev.a8n.run/contoso/claude-run/actions/runs/9".to_string(),
                source: "test".to_string(),
            };
            process_action_success_event(
                ev,
                pool.clone(),
                Some(mint),
                crate::concurrency::KeyGate::new(),
            )
            .await;

            let comment = backend
                .calls()
                .into_iter()
                .find(|c| c.starts_with("comment MK-16"))
                .unwrap_or_else(|| panic!("expected an issue comment, got {:?}", backend.calls()));
            assert!(comment.contains("automerge status"), "{comment}");

            let events = state::run_events_for_action(&pool, WEBHOOK_MINT_ACTION, 10)
                .await
                .unwrap();
            assert!(
                events
                    .iter()
                    .any(|e| e.message.contains("list_open_prs failed")),
                "{events:?}"
            );
        }

        // ---- GitHub-hosted PRs must route through `GithubForge`,
        // never `mint.backend` (`fj`, which 410s against GitHub's API).
        mod github_hosted {
            use super::*;

            fn gh_list_open_prs_args(repo: &str) -> Vec<&str> {
                vec![
                    "pr",
                    "list",
                    "--repo",
                    repo,
                    "--state",
                    "open",
                    "--limit",
                    "1000",
                    "--json",
                    "number,title,headRefOid,headRefName,baseRefName,author,url",
                ]
            }

            #[tokio::test]
            async fn list_open_pr_numbers_routes_github_host_through_github_forge() {
                // The exact shape of the reported bug: an `agent_pull_requests`
                // row with `host = "github.com"`. `mint.backend` (`fj`) is
                // configured to fail if it is ever reached, so a `Some` result
                // here can only have come from `GithubForge`'s `gh pr list`.
                let pool = crate::db::test_pool().await;
                let backend = Arc::new(StoreBackend::new().with_open_prs_error("fj: 410 Gone"));
                let runner = Arc::new(crate::provider::cli::testutil::StubRunner::new().ok(
                    "gh",
                    &gh_list_open_prs_args("octo/widget"),
                    r#"[{"number":42,"title":"fix","headRefOid":"abc123","headRefName":"fix/x","baseRefName":"main","author":{"login":"claude-run"},"url":"https://github.com/octo/widget/pull/42"}]"#,
                ));
                let classifier: Arc<dyn FailureClassifier> = Arc::new(CountingClassifier::new());
                let dir = tempfile::tempdir().unwrap();
                let ctx = store_mint_context_in(
                    pool.clone(),
                    backend.clone(),
                    classifier,
                    dir.path(),
                    runner.clone(),
                );

                let result = list_open_pr_numbers(&pool, &ctx, "github.com", "octo/widget").await;

                assert_eq!(result, Some(vec![42]));
                assert!(
                    runner
                        .calls
                        .lock()
                        .unwrap()
                        .iter()
                        .any(|c| c.starts_with("gh pr list")),
                    "{:?}",
                    runner.calls.lock().unwrap()
                );
            }

            /// `gh pr view`'s JSON for a green, clean (or behind-base) PR.
            fn gh_pr_view_json(merge_state_status: &str) -> String {
                format!(
                    r#"{{"state":"OPEN","mergeable":"MERGEABLE","mergeStateStatus":"{merge_state_status}","headRefOid":"abc123","statusCheckRollup":[{{"__typename":"CheckRun","status":"COMPLETED","conclusion":"SUCCESS"}}]}}"#
                )
            }

            fn gh_pr_view_args(repo: &str) -> Vec<&str> {
                vec![
                    "pr",
                    "view",
                    "42",
                    "--repo",
                    repo,
                    "--json",
                    "state,mergeable,mergeStateStatus,headRefOid,statusCheckRollup",
                ]
            }

            async fn seeded_github_row(pool: &DbPool) -> ci_failure_store::AgentPullRequest {
                ci_failure_store::record_agent_pr(
                    pool,
                    "yt",
                    "MK-16",
                    "github.com",
                    "octo/widget",
                    42,
                    "fix/x",
                    "open",
                    "2026-09-01T00:00:00Z",
                )
                .await
                .unwrap();
                ci_failure_store::agent_pr(pool, "github.com", "octo/widget", 42)
                    .await
                    .unwrap()
                    .unwrap()
            }

            #[tokio::test]
            async fn apply_automerge_decision_routes_github_host_through_github_forge_to_merge() {
                // `mint.backend` (`fj`) would merge the PR if ever reached; the
                // assertion is that it never is, because a `github.com` host
                // dispatches `pr_status` / `merge_pr` to `GithubForge` (backed
                // by `gh`), while the issue comment still goes through
                // `mint.backend` (the YouTrack `IssueTracker` half every
                // backend forwards to `CliBackend`/`yt`).
                let pool = crate::db::test_pool().await;
                let dir = tempfile::tempdir().unwrap();
                write_youtrack_action(dir.path(), "yt", true);
                let backend = Arc::new(StoreBackend::new().with_pr_status("success", true));
                let classifier: Arc<dyn FailureClassifier> = Arc::new(CountingClassifier::new());
                let runner = Arc::new(
                    crate::provider::cli::testutil::StubRunner::new()
                        .ok(
                            "gh",
                            &gh_pr_view_args("octo/widget"),
                            &gh_pr_view_json("CLEAN"),
                        )
                        .ok(
                            "gh",
                            &[
                                "pr",
                                "merge",
                                "42",
                                "--repo",
                                "octo/widget",
                                "--merge",
                                "--delete-branch",
                            ],
                            "",
                        ),
                );
                let ctx = store_mint_context_in(
                    pool.clone(),
                    backend.clone(),
                    classifier,
                    dir.path(),
                    runner.clone(),
                );
                let row = seeded_github_row(&pool).await;

                let result = apply_automerge_decision(
                    &ctx,
                    &pool,
                    "github.com",
                    "octo/widget",
                    42,
                    "https://github.com/octo/widget/pulls/42",
                    &row,
                )
                .await;

                assert!(
                    matches!(result, Ok(AutomergeOutcome::Acted(_))),
                    "{result:?}"
                );
                let calls = runner.calls.lock().unwrap();
                assert!(
                    calls.iter().any(|c| c.starts_with("gh pr merge 42")),
                    "{calls:?}"
                );
                drop(calls);
                assert!(
                    !backend
                        .calls()
                        .iter()
                        .any(|c| c.starts_with("pr_status") || c.starts_with("merge_pr")),
                    "fj must never be consulted for a GitHub host: {:?}",
                    backend.calls()
                );
                assert!(
                    backend.calls().iter().any(|c| c.starts_with("comment")),
                    "the post-merge comment still goes through the YouTrack backend: {:?}",
                    backend.calls()
                );
            }

            #[tokio::test]
            async fn apply_automerge_decision_routes_github_host_through_github_forge_to_update_branch(
            ) {
                // A behind-base `github.com` PR dispatches `update_pr_branch`
                // to `GithubForge`, never `mint.backend`.
                let pool = crate::db::test_pool().await;
                let dir = tempfile::tempdir().unwrap();
                write_youtrack_action(dir.path(), "yt", true);
                let backend = Arc::new(StoreBackend::new());
                let classifier: Arc<dyn FailureClassifier> = Arc::new(CountingClassifier::new());
                let runner = Arc::new(
                    crate::provider::cli::testutil::StubRunner::new()
                        .ok(
                            "gh",
                            &gh_pr_view_args("octo/widget"),
                            &gh_pr_view_json("BEHIND"),
                        )
                        .ok(
                            "gh",
                            &["pr", "update-branch", "42", "--repo", "octo/widget"],
                            "",
                        ),
                );
                let ctx = store_mint_context_in(
                    pool.clone(),
                    backend.clone(),
                    classifier,
                    dir.path(),
                    runner.clone(),
                );
                let row = seeded_github_row(&pool).await;

                let result = apply_automerge_decision(
                    &ctx,
                    &pool,
                    "github.com",
                    "octo/widget",
                    42,
                    "https://github.com/octo/widget/pulls/42",
                    &row,
                )
                .await;

                assert!(
                    matches!(result, Ok(AutomergeOutcome::Acted(_))),
                    "{result:?}"
                );
                let calls = runner.calls.lock().unwrap();
                assert!(
                    calls
                        .iter()
                        .any(|c| c.starts_with("gh pr update-branch 42")),
                    "{calls:?}"
                );
                drop(calls);
                assert!(
                    !backend
                        .calls()
                        .iter()
                        .any(|c| c.starts_with("pr_status") || c.starts_with("update_pr_branch")),
                    "fj must never be consulted for a GitHub host: {:?}",
                    backend.calls()
                );
            }
        }

        // CLAUDE-397: a reconciler pass whose decision is a no-action creates
        // zero `runs` rows, for every no-action shape; an acted or failed
        // decision still creates exactly one, with the same summary as before.
        mod no_op_run_rows {
            use super::*;

            #[tokio::test]
            async fn checks_not_green_creates_no_run_row() {
                let pool = crate::db::test_pool().await;
                let dir = tempfile::tempdir().unwrap();
                write_youtrack_action(dir.path(), "yt", true);
                seed_open_row(&pool).await;
                let backend = Arc::new(
                    StoreBackend::new()
                        .with_open_prs(vec![pr(7, "")])
                        .with_pr_status("pending", false),
                );
                let ctx = ctx_for(pool.clone(), backend.clone(), dir.path());

                reconcile_automerge(
                    &pool,
                    &ctx,
                    &crate::concurrency::KeyGate::new(),
                    &mut std::collections::HashMap::new(),
                )
                .await;

                assert_eq!(webhook_triage_runs_count(&pool).await, 0);
            }

            #[tokio::test]
            async fn automerge_disabled_creates_no_run_row() {
                let pool = crate::db::test_pool().await;
                let dir = tempfile::tempdir().unwrap();
                write_youtrack_action(dir.path(), "yt", false);
                seed_open_row(&pool).await;
                let backend = Arc::new(
                    StoreBackend::new()
                        .with_open_prs(vec![pr(7, "")])
                        .with_pr_status("success", true),
                );
                let ctx = ctx_for(pool.clone(), backend.clone(), dir.path());

                reconcile_automerge(
                    &pool,
                    &ctx,
                    &crate::concurrency::KeyGate::new(),
                    &mut std::collections::HashMap::new(),
                )
                .await;

                assert_eq!(webhook_triage_runs_count(&pool).await, 0);
            }

            #[tokio::test]
            async fn non_youtrack_action_creates_no_run_row() {
                let pool = crate::db::test_pool().await;
                let dir = tempfile::tempdir().unwrap();
                let actions_dir = dir.path().join("actions");
                std::fs::create_dir_all(&actions_dir).unwrap();
                std::fs::write(
                    actions_dir.join("reporting.yml"),
                    "name: reporting\nkind: closed-issue-summary\nnative:\n  enabled: true\n  parents: []\n  query: \"\"\n  custom_field: \"AI Agent\"\n  done_label: Done\n  failed_label: Failed\n  projects:\n    - CLAUDE\n  window_days: 7\n",
                )
                .unwrap();
                ci_failure_store::record_agent_pr(
                    &pool,
                    "reporting",
                    "MK-16",
                    "dev.a8n.run",
                    "contoso/claude-run",
                    7,
                    "fix/mk-16",
                    "open",
                    "2026-09-01T00:00:00Z",
                )
                .await
                .unwrap();
                let backend = Arc::new(
                    StoreBackend::new()
                        .with_open_prs(vec![pr(7, "")])
                        .with_pr_status("success", true),
                );
                let ctx = ctx_for(pool.clone(), backend.clone(), dir.path());

                reconcile_automerge(
                    &pool,
                    &ctx,
                    &crate::concurrency::KeyGate::new(),
                    &mut std::collections::HashMap::new(),
                )
                .await;

                assert_eq!(webhook_triage_runs_count(&pool).await, 0);
            }

            #[tokio::test]
            async fn not_mergeable_creates_no_run_row() {
                let pool = crate::db::test_pool().await;
                let dir = tempfile::tempdir().unwrap();
                write_youtrack_action(dir.path(), "yt", true);
                seed_open_row(&pool).await;
                let backend = Arc::new(
                    StoreBackend::new()
                        .with_open_prs(vec![pr(7, "")])
                        .with_pr_status_behind("success", false, Some(false)),
                );
                let ctx = ctx_for(pool.clone(), backend.clone(), dir.path());

                reconcile_automerge(
                    &pool,
                    &ctx,
                    &crate::concurrency::KeyGate::new(),
                    &mut std::collections::HashMap::new(),
                )
                .await;

                assert_eq!(webhook_triage_runs_count(&pool).await, 0);
                // The existing `run_events` row for `not mergeable` is unchanged.
                let events = state::run_events_for_action(&pool, WEBHOOK_MINT_ACTION, 10)
                    .await
                    .unwrap();
                assert!(
                    events.iter().any(|e| e.message.contains("not mergeable")),
                    "{events:?}"
                );
            }

            #[tokio::test]
            async fn merged_pr_creates_exactly_one_run_row_with_the_merge_summary() {
                let pool = crate::db::test_pool().await;
                let dir = tempfile::tempdir().unwrap();
                write_youtrack_action(dir.path(), "yt", true);
                seed_open_row(&pool).await;
                let backend = Arc::new(
                    StoreBackend::new()
                        .with_open_prs(vec![pr(7, "")])
                        .with_pr_status("success", true),
                );
                let ctx = ctx_for(pool.clone(), backend.clone(), dir.path());

                reconcile_automerge(
                    &pool,
                    &ctx,
                    &crate::concurrency::KeyGate::new(),
                    &mut std::collections::HashMap::new(),
                )
                .await;

                assert_eq!(webhook_triage_runs_count(&pool).await, 1);
                let (ok, summary): (i64, String) =
                    sqlx::query_as(&crate::db::q(&pool, "SELECT ok, summary FROM job_outcomes"))
                        .fetch_one(&pool)
                        .await
                        .unwrap();
                assert_eq!(ok, 1);
                assert_eq!(
                    summary,
                    "merged PR #7; issue MK-16 left for the YouTrack workflow to resolve"
                );
            }

            #[tokio::test]
            async fn already_merged_pr_creates_exactly_one_run_row_with_the_same_summary() {
                let pool = crate::db::test_pool().await;
                let dir = tempfile::tempdir().unwrap();
                write_youtrack_action(dir.path(), "yt", true);
                seed_open_row(&pool).await;
                let backend = Arc::new(
                    StoreBackend::new()
                        .with_open_prs(vec![pr(7, "")])
                        .with_pr_status("merged", true),
                );
                let ctx = ctx_for(pool.clone(), backend.clone(), dir.path());

                reconcile_automerge(
                    &pool,
                    &ctx,
                    &crate::concurrency::KeyGate::new(),
                    &mut std::collections::HashMap::new(),
                )
                .await;

                assert_eq!(webhook_triage_runs_count(&pool).await, 1);
                let (ok, summary): (i64, String) =
                    sqlx::query_as(&crate::db::q(&pool, "SELECT ok, summary FROM job_outcomes"))
                        .fetch_one(&pool)
                        .await
                        .unwrap();
                assert_eq!(ok, 1);
                assert_eq!(summary, "agent PR #7 already merged; marked merged");
            }

            #[tokio::test]
            async fn branch_updated_creates_exactly_one_run_row_with_the_same_summary() {
                let pool = crate::db::test_pool().await;
                let dir = tempfile::tempdir().unwrap();
                write_youtrack_action(dir.path(), "yt", true);
                seed_open_row(&pool).await;
                let backend = Arc::new(
                    StoreBackend::new()
                        .with_open_prs(vec![pr(7, "")])
                        .with_pr_status_behind("success", true, Some(true)),
                );
                let ctx = ctx_for(pool.clone(), backend.clone(), dir.path());

                reconcile_automerge(
                    &pool,
                    &ctx,
                    &crate::concurrency::KeyGate::new(),
                    &mut std::collections::HashMap::new(),
                )
                .await;

                assert_eq!(webhook_triage_runs_count(&pool).await, 1);
                let (ok, summary): (i64, String) =
                    sqlx::query_as(&crate::db::q(&pool, "SELECT ok, summary FROM job_outcomes"))
                        .fetch_one(&pool)
                        .await
                        .unwrap();
                assert_eq!(ok, 1);
                assert_eq!(
                    summary,
                    "PR #7 branch updated from base; will automerge on next green run"
                );
            }

            #[tokio::test]
            async fn decision_err_still_creates_one_failed_run_row() {
                let pool = crate::db::test_pool().await;
                let dir = tempfile::tempdir().unwrap();
                write_youtrack_action(dir.path(), "yt", true);
                seed_open_row(&pool).await;
                let backend = Arc::new(
                    StoreBackend::new()
                        .with_open_prs(vec![pr(7, "")])
                        .with_pr_status("success", true)
                        .with_merge_pr_error("boom"),
                );
                let ctx = ctx_for(pool.clone(), backend.clone(), dir.path());

                reconcile_automerge(
                    &pool,
                    &ctx,
                    &crate::concurrency::KeyGate::new(),
                    &mut std::collections::HashMap::new(),
                )
                .await;

                assert_eq!(webhook_triage_runs_count(&pool).await, 1);
                let (ok, summary): (i64, String) =
                    sqlx::query_as(&crate::db::q(&pool, "SELECT ok, summary FROM job_outcomes"))
                        .fetch_one(&pool)
                        .await
                        .unwrap();
                assert_eq!(ok, 0);
                assert!(summary.contains("complete_automerge failed"), "{summary}");

                // The existing `run_events` row and comment for a merge
                // failure are unchanged.
                let events = state::run_events_for_action(&pool, WEBHOOK_MINT_ACTION, 10)
                    .await
                    .unwrap();
                assert!(
                    events
                        .iter()
                        .any(|e| e.message.contains("complete_automerge failed")),
                    "{events:?}"
                );
            }
        }
    }

    // CLAUDE-406: the triage path's own open-PR lookups (the routing
    // decision in `resolve_open_pr` and the advisory `has_open_pr` signal in
    // `resolve_has_open_pr`) must route a `github.com` failure through
    // `GithubForge`, never `mint.backend` (`fj`, which 410s against GitHub's
    // API), exactly like the automerge path already does.
    mod triage_github_hosted {
        use super::*;

        fn gh_list_open_prs_args(repo: &str) -> Vec<&str> {
            vec![
                "pr",
                "list",
                "--repo",
                repo,
                "--state",
                "open",
                "--limit",
                "1000",
                "--json",
                "number,title,headRefOid,headRefName,baseRefName,author,url",
            ]
        }

        #[tokio::test]
        async fn open_pr_routing_for_a_github_host_uses_github_forge_not_fj() {
            // An agent's own PR, on `github.com`: the routing lookup must
            // find it through `GithubForge`'s `gh pr list`, re-queuing its
            // issue, and never touch `mint.backend` (`fj`, configured to
            // fail if it is ever reached).
            let pool = crate::db::test_pool().await;
            let now = "2026-09-01T00:00:00Z";
            ci_failure_store::record_agent_pr(
                &pool,
                "youtrack",
                "CLAUDE-40",
                "github.com",
                "octo/widget",
                42,
                "fix/CLAUDE-40",
                "open",
                now,
            )
            .await
            .unwrap();

            let backend = Arc::new(StoreBackend::new().with_open_prs_error("fj: 410 Gone"));
            let runner = Arc::new(crate::provider::cli::testutil::StubRunner::new().ok(
                "gh",
                &gh_list_open_prs_args("octo/widget"),
                r#"[{"number":42,"title":"fix","headRefOid":"commit-9201","headRefName":"fix/CLAUDE-40","baseRefName":"main","author":{"login":"claude-run"},"url":"https://github.com/octo/widget/pull/42"}]"#,
            ));
            let classifier: Arc<dyn FailureClassifier> = Arc::new(CountingClassifier::new());
            let dir = tempfile::tempdir().unwrap();
            let ctx = store_mint_context_in(
                pool.clone(),
                backend.clone(),
                classifier,
                dir.path(),
                runner.clone(),
            );
            let ev = github_store_event(9201, "octo/widget");
            let log = failing_test_log("mod_a", "test_one", "fix/CLAUDE-40");

            let result = triage_and_mint(
                &ctx,
                None,
                &ev,
                log,
                true,
                None,
                None,
                true,
                "test".to_string(),
                None,
                &crate::concurrency::KeyGate::new(),
                &WebhookConfig::default(),
                &test_spend(),
            )
            .await;

            assert!(result.ok, "{}", result.text);
            let calls = backend.calls();
            assert!(
                !calls.iter().any(|c| c.starts_with("list_open_prs")),
                "fj must never be consulted for a GitHub host: {calls:?}"
            );
            assert!(
                calls.iter().any(|c| c.starts_with("comment CLAUDE-40")),
                "the agent's own PR re-queues its issue: {calls:?}"
            );
            assert!(
                calls
                    .iter()
                    .any(|c| c == "set_field CLAUDE-40 AI Agent Queued"),
                "{calls:?}"
            );
            assert!(
                runner
                    .calls
                    .lock()
                    .unwrap()
                    .iter()
                    .any(|c| c.starts_with("gh pr list")),
                "the routing lookup ran through GithubForge: {:?}",
                runner.calls.lock().unwrap()
            );
        }

        #[tokio::test]
        async fn has_open_pr_signal_for_a_github_host_uses_github_forge_not_fj() {
            // No recorded agent PR and no open PR matches this commit, so
            // this reaches the ordinary classify + mint path (routing's own
            // `resolve_open_pr` finds nothing and the human-PR-comment
            // branch never triggers). Its advisory `has_open_pr` signal must
            // still route through `GithubForge`, never `fj`.
            let pool = crate::db::test_pool().await;
            let backend = Arc::new(StoreBackend::new().with_open_prs_error("fj: 410 Gone"));
            let runner = Arc::new(crate::provider::cli::testutil::StubRunner::new().ok(
                "gh",
                &gh_list_open_prs_args("octo/widget"),
                r#"[{"number":42,"title":"fix","headRefOid":"commit-other","headRefName":"fix/x","baseRefName":"main","author":{"login":"claude-run"},"url":"https://github.com/octo/widget/pull/42"}]"#,
            ));
            let classifier: Arc<dyn FailureClassifier> = Arc::new(CountingClassifier::new());
            let dir = tempfile::tempdir().unwrap();
            let ctx = store_mint_context_in(
                pool.clone(),
                backend.clone(),
                classifier,
                dir.path(),
                runner.clone(),
            );
            let ev = github_store_event(9301, "octo/widget");
            let log = failing_test_log("mod_a", "test_one", "main");

            let result = triage_and_mint(
                &ctx,
                None,
                &ev,
                log,
                true,
                None,
                None,
                true,
                "test".to_string(),
                None,
                &crate::concurrency::KeyGate::new(),
                &WebhookConfig::default(),
                &test_spend(),
            )
            .await;

            assert!(result.ok, "{}", result.text);
            let calls = backend.calls();
            assert!(
                !calls.iter().any(|c| c.starts_with("list_open_prs")),
                "fj must never be consulted for a GitHub host: {calls:?}"
            );
            assert!(
                runner
                    .calls
                    .lock()
                    .unwrap()
                    .iter()
                    .any(|c| c.starts_with("gh pr list")),
                "the advisory has_open_pr signal ran through GithubForge: {:?}",
                runner.calls.lock().unwrap()
            );
        }

        #[tokio::test]
        async fn open_pr_lookup_failure_for_a_github_host_still_fails_the_routing_run() {
            // A failed `gh pr list` call for a GitHub host must still fail
            // the routing run exactly like a failed `fj` lookup does - never
            // silently read as "no PR".
            let pool = crate::db::test_pool().await;
            let backend = Arc::new(StoreBackend::new());
            let runner = Arc::new(crate::provider::cli::testutil::StubRunner::new().fail(
                "gh",
                &gh_list_open_prs_args("octo/widget"),
                1,
                "gh: 401 Unauthorized",
            ));
            let classifier: Arc<dyn FailureClassifier> = Arc::new(CountingClassifier::new());
            let dir = tempfile::tempdir().unwrap();
            let ctx = store_mint_context_in(
                pool.clone(),
                backend.clone(),
                classifier,
                dir.path(),
                runner,
            );
            let ev = github_store_event(9401, "octo/widget");
            let log = failing_test_log("mod_a", "test_one", "main");

            let result = triage_and_mint(
                &ctx,
                None,
                &ev,
                log,
                true,
                None,
                None,
                true,
                "test".to_string(),
                None,
                &crate::concurrency::KeyGate::new(),
                &WebhookConfig::default(),
                &test_spend(),
            )
            .await;

            assert!(
                !result.ok,
                "a lookup error must fail the run: {}",
                result.text
            );
            assert!(
                !backend.calls().iter().any(|c| c.starts_with("create")),
                "nothing minted: {:?}",
                backend.calls()
            );
        }

        #[tokio::test]
        async fn resolve_open_pr_fails_for_a_github_host_lookup_failure() {
            // Direct test of `resolve_open_pr` (the routing decision): a
            // failed `gh pr list` call is an error, never read as "no PR",
            // for a GitHub host exactly as it already is for an `fj` one.
            let pool = crate::db::test_pool().await;
            let backend = Arc::new(StoreBackend::new());
            let runner = Arc::new(crate::provider::cli::testutil::StubRunner::new().fail(
                "gh",
                &gh_list_open_prs_args("octo/widget"),
                1,
                "gh: 401 Unauthorized",
            ));
            let classifier: Arc<dyn FailureClassifier> = Arc::new(CountingClassifier::new());
            let dir = tempfile::tempdir().unwrap();
            let ctx = store_mint_context_in(pool, backend, classifier, dir.path(), runner);
            let ev = github_store_event(9402, "octo/widget");

            let forge = forge_for_event(&ctx, &ev);
            let result = resolve_open_pr(forge.as_ref(), &ev);

            assert!(result.is_err(), "{result:?}");
        }

        #[tokio::test]
        async fn resolve_has_open_pr_degrades_to_false_for_a_github_host_lookup_failure() {
            // Direct test of `resolve_has_open_pr` (the advisory signal): the
            // same kind of failure degrades to `false` instead of erroring,
            // for a GitHub host exactly as it already does for an `fj` one.
            let pool = crate::db::test_pool().await;
            let backend = Arc::new(StoreBackend::new());
            let runner = Arc::new(crate::provider::cli::testutil::StubRunner::new().fail(
                "gh",
                &gh_list_open_prs_args("octo/widget"),
                1,
                "gh: 401 Unauthorized",
            ));
            let classifier: Arc<dyn FailureClassifier> = Arc::new(CountingClassifier::new());
            let dir = tempfile::tempdir().unwrap();
            let ctx = store_mint_context_in(pool, backend, classifier, dir.path(), runner);
            let ev = github_store_event(9403, "octo/widget");

            let forge = forge_for_event(&ctx, &ev);
            let has_open_pr = resolve_has_open_pr(forge.as_ref(), &ev);

            assert!(!has_open_pr, "a lookup failure degrades to false");
        }
    }

    /// Acceptance criteria 1, 2, 4, 5 for CLAUDE-380: a conflict
    /// `update_pr_branch` failure dispatches the same `/claude` conflict-resolve
    /// run a human's PR comment would trigger, at most once per
    /// `(repo, pr_number, head_sha)`.
    mod automerge_conflict_dispatch {
        use super::automerge_success::write_youtrack_action;
        use super::*;

        fn dispatch_cfg() -> crate::config::CommentWebhookConfig {
            let mut cfg = crate::config::CommentWebhookConfig {
                enabled: true,
                ..Default::default()
            };
            cfg.sources.insert(
                "fj".to_string(),
                crate::config::CommentWebhookSource {
                    allowed_repos: vec!["contoso/claude-run".to_string()],
                    ..Default::default()
                },
            );
            cfg
        }

        fn ctx_for(
            pool: DbPool,
            backend: Arc<StoreBackend>,
            paths_dir: &std::path::Path,
            dispatch: Option<CommentDispatch>,
        ) -> MintContext {
            let classifier: Arc<dyn FailureClassifier> = Arc::new(CountingClassifier::new());
            let mut ctx = store_mint_context_in(
                pool,
                backend,
                classifier,
                paths_dir,
                Arc::new(crate::provider::cli::testutil::StubRunner::new()),
            );
            ctx.comment_dispatch = dispatch;
            ctx
        }

        async fn seed_open_row(pool: &DbPool) {
            ci_failure_store::record_agent_pr(
                pool,
                "yt",
                "MK-16",
                "dev.a8n.run",
                "contoso/claude-run",
                7,
                "fix/mk-16",
                "open",
                "2026-09-01T00:00:00Z",
            )
            .await
            .unwrap();
        }

        #[tokio::test]
        async fn conflict_with_a_wired_dispatch_sends_once_per_head_sha_and_allows_a_new_head() {
            let pool = crate::db::test_pool().await;
            let dir = tempfile::tempdir().unwrap();
            write_youtrack_action(dir.path(), "yt", true);
            seed_open_row(&pool).await;
            let (tx, mut rx) =
                mpsc::channel::<crate::web::comment_webhooks::VerifiedCommentCommand>(8);
            let dispatch = CommentDispatch {
                tx,
                cfg: Arc::new(RwLock::new(dispatch_cfg())),
            };
            let backend = Arc::new(
                StoreBackend::new()
                    .with_open_prs(vec![pr(7, "")])
                    .with_pr_status_full("success", Some(false), Some(true), Some("sha-1"))
                    .with_update_pr_branch_error("merge conflict in src/lib.rs"),
            );
            let ctx = ctx_for(pool.clone(), backend.clone(), dir.path(), Some(dispatch));
            let gate = crate::concurrency::KeyGate::new();

            // Acceptance 1: one reconciler tick for a conflict with a known
            // head_sha sends exactly one synthetic command, and the issue
            // comment says the runner is attempting the resolve itself.
            reconcile_automerge(&pool, &ctx, &gate, &mut std::collections::HashMap::new()).await;
            let sent = rx.try_recv().expect("expected one dispatched command");
            assert!(sent.synthetic);
            assert_eq!(sent.repo, "contoso/claude-run");
            assert_eq!(sent.number, 7);
            assert!(
                sent.comment_id < 0,
                "dispatch id must not collide with a real (positive) comment id"
            );
            assert!(
                rx.try_recv().is_err(),
                "only one command should have been sent"
            );
            let comments = backend
                .calls()
                .into_iter()
                .filter(|c| c.starts_with("comment MK-16"))
                .collect::<Vec<_>>();
            assert_eq!(comments.len(), 1, "{comments:?}");
            assert!(
                comments[0].contains("attempting to resolve the conflict automatically"),
                "{comments:?}"
            );

            // The dispatched command shares the same (repo, pr, comment_id)
            // attempt counter `serve_command` records once it actually runs
            // the command; simulate that completed run here rather than
            // draining the channel through the full comment-webhook pipeline.
            let key =
                crate::web::comment_webhooks::command_key("contoso/claude-run", 7, sent.comment_id);
            state::attempts_increment(
                &pool,
                crate::web::comment_webhooks::WEBHOOK_COMMENT_ACTION,
                &key,
            )
            .await
            .unwrap();

            // Acceptance 1 (continued): a second tick for the SAME head_sha,
            // once an attempt is recorded for it, sends no further command and
            // falls back to the human comment.
            reconcile_automerge(&pool, &ctx, &gate, &mut std::collections::HashMap::new()).await;
            assert!(
                rx.try_recv().is_err(),
                "a second tick for the same head must not dispatch again"
            );
            let comments = backend
                .calls()
                .into_iter()
                .filter(|c| c.starts_with("comment MK-16"))
                .collect::<Vec<_>>();
            assert_eq!(comments.len(), 2, "{comments:?}");
            assert!(
                comments[1].contains("A human needs to resolve this"),
                "second failure for the same head must fall back to the human comment: {comments:?}"
            );

            // Acceptance 2: a new head_sha (after a push) allows one new attempt.
            *backend.pr_status.lock().unwrap() = Some(crate::provider::PrStatus {
                state: "success".to_string(),
                mergeable: Some(false),
                behind_base: Some(true),
                head_sha: Some("sha-2".to_string()),
            });
            reconcile_automerge(&pool, &ctx, &gate, &mut std::collections::HashMap::new()).await;
            assert!(
                rx.try_recv().is_ok(),
                "a new head commit must get its own dispatch attempt"
            );
        }

        #[tokio::test]
        async fn non_conflict_error_never_dispatches() {
            let pool = crate::db::test_pool().await;
            let dir = tempfile::tempdir().unwrap();
            write_youtrack_action(dir.path(), "yt", true);
            seed_open_row(&pool).await;
            let (tx, mut rx) =
                mpsc::channel::<crate::web::comment_webhooks::VerifiedCommentCommand>(8);
            let dispatch = CommentDispatch {
                tx,
                cfg: Arc::new(RwLock::new(dispatch_cfg())),
            };
            let backend = Arc::new(
                StoreBackend::new()
                    .with_open_prs(vec![pr(7, "")])
                    .with_pr_status_full("success", Some(false), Some(true), Some("sha-1"))
                    .with_update_pr_branch_error("network timeout"),
            );
            let ctx = ctx_for(pool.clone(), backend.clone(), dir.path(), Some(dispatch));
            let gate = crate::concurrency::KeyGate::new();

            reconcile_automerge(&pool, &ctx, &gate, &mut std::collections::HashMap::new()).await;
            assert!(
                rx.try_recv().is_err(),
                "a non-conflict error must never dispatch"
            );
            let comments = backend
                .calls()
                .into_iter()
                .filter(|c| c.starts_with("comment MK-16"))
                .collect::<Vec<_>>();
            assert_eq!(comments.len(), 1);
            assert!(
                comments[0].contains("A human needs to resolve this"),
                "{comments:?}"
            );
        }

        #[tokio::test]
        async fn conflict_with_no_head_sha_never_dispatches() {
            let pool = crate::db::test_pool().await;
            let dir = tempfile::tempdir().unwrap();
            write_youtrack_action(dir.path(), "yt", true);
            seed_open_row(&pool).await;
            let (tx, mut rx) =
                mpsc::channel::<crate::web::comment_webhooks::VerifiedCommentCommand>(8);
            let dispatch = CommentDispatch {
                tx,
                cfg: Arc::new(RwLock::new(dispatch_cfg())),
            };
            let backend = Arc::new(
                StoreBackend::new()
                    .with_open_prs(vec![pr(7, "")])
                    .with_pr_status_full("success", Some(false), Some(true), None)
                    .with_update_pr_branch_error("merge conflict"),
            );
            let ctx = ctx_for(pool.clone(), backend.clone(), dir.path(), Some(dispatch));
            let gate = crate::concurrency::KeyGate::new();

            reconcile_automerge(&pool, &ctx, &gate, &mut std::collections::HashMap::new()).await;
            assert!(
                rx.try_recv().is_err(),
                "an fj predating head_sha must never dispatch"
            );
        }

        #[tokio::test]
        async fn conflict_with_no_wired_sender_falls_back_to_the_human_comment() {
            let pool = crate::db::test_pool().await;
            let dir = tempfile::tempdir().unwrap();
            write_youtrack_action(dir.path(), "yt", true);
            seed_open_row(&pool).await;
            let backend = Arc::new(
                StoreBackend::new()
                    .with_open_prs(vec![pr(7, "")])
                    .with_pr_status_full("success", Some(false), Some(true), Some("sha-1"))
                    .with_update_pr_branch_error("merge conflict"),
            );
            // No `comment_dispatch` wired: exactly today's existing behavior.
            let ctx = ctx_for(pool.clone(), backend.clone(), dir.path(), None);
            let gate = crate::concurrency::KeyGate::new();

            reconcile_automerge(&pool, &ctx, &gate, &mut std::collections::HashMap::new()).await;
            let comments = backend
                .calls()
                .into_iter()
                .filter(|c| c.starts_with("comment MK-16"))
                .collect::<Vec<_>>();
            assert_eq!(comments.len(), 1);
            assert!(
                comments[0].contains("A human needs to resolve this"),
                "{comments:?}"
            );
        }

        #[tokio::test]
        async fn a_dispatch_send_failure_logs_an_event_and_falls_back_to_the_human_comment() {
            let pool = crate::db::test_pool().await;
            let dir = tempfile::tempdir().unwrap();
            write_youtrack_action(dir.path(), "yt", true);
            seed_open_row(&pool).await;
            // A zero-capacity channel with no live receiver: `tx.send` fails.
            let (tx, rx) = mpsc::channel::<crate::web::comment_webhooks::VerifiedCommentCommand>(1);
            drop(rx);
            let dispatch = CommentDispatch {
                tx,
                cfg: Arc::new(RwLock::new(dispatch_cfg())),
            };
            let backend = Arc::new(
                StoreBackend::new()
                    .with_open_prs(vec![pr(7, "")])
                    .with_pr_status_full("success", Some(false), Some(true), Some("sha-1"))
                    .with_update_pr_branch_error("merge conflict"),
            );
            let ctx = ctx_for(pool.clone(), backend.clone(), dir.path(), Some(dispatch));
            let gate = crate::concurrency::KeyGate::new();

            reconcile_automerge(&pool, &ctx, &gate, &mut std::collections::HashMap::new()).await;

            let events = state::run_events_for_action(&pool, WEBHOOK_MINT_ACTION, 50)
                .await
                .unwrap();
            assert!(
                events
                    .iter()
                    .any(|e| e.message.contains("dispatch send failed")),
                "{events:?}"
            );
            let comments = backend
                .calls()
                .into_iter()
                .filter(|c| c.starts_with("comment MK-16"))
                .collect::<Vec<_>>();
            assert_eq!(comments.len(), 1);
            assert!(
                comments[0].contains("A human needs to resolve this"),
                "{comments:?}"
            );
        }

        #[tokio::test]
        async fn conflict_with_no_source_covering_the_repo_falls_back_to_the_human_comment() {
            let pool = crate::db::test_pool().await;
            let dir = tempfile::tempdir().unwrap();
            write_youtrack_action(dir.path(), "yt", true);
            seed_open_row(&pool).await;
            let (tx, mut rx) =
                mpsc::channel::<crate::web::comment_webhooks::VerifiedCommentCommand>(8);
            // Configured, but no source's allow-list covers this repo.
            let mut cfg = dispatch_cfg();
            cfg.sources.get_mut("fj").unwrap().allowed_repos = vec!["other/repo".to_string()];
            let dispatch = CommentDispatch {
                tx,
                cfg: Arc::new(RwLock::new(cfg)),
            };
            let backend = Arc::new(
                StoreBackend::new()
                    .with_open_prs(vec![pr(7, "")])
                    .with_pr_status_full("success", Some(false), Some(true), Some("sha-1"))
                    .with_update_pr_branch_error("merge conflict"),
            );
            let ctx = ctx_for(pool.clone(), backend.clone(), dir.path(), Some(dispatch));
            let gate = crate::concurrency::KeyGate::new();

            reconcile_automerge(&pool, &ctx, &gate, &mut std::collections::HashMap::new()).await;
            assert!(rx.try_recv().is_err());
            let comments = backend
                .calls()
                .into_iter()
                .filter(|c| c.starts_with("comment MK-16"))
                .collect::<Vec<_>>();
            assert_eq!(comments.len(), 1);
            assert!(
                comments[0].contains("A human needs to resolve this"),
                "{comments:?}"
            );
        }
    }
}
