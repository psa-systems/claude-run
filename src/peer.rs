//! Optional peering between independently operating instances (`SPEC-3.17`).
//!
//! This module holds the instance identity helper, the peer state endpoint
//! (`GET /api/peer/state`), the claim endpoint and client (`POST
//! /api/peer/claim`), the per-peer liveness registry and the poller. A peer is never a dependency of a run: every call
//! here is bounded and its failure is recorded, shown and logged, never
//! propagated into a run.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use axum::extract::State;
use axum::http::{header, HeaderMap, StatusCode};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;
use tracing::{debug, error, info, warn};

use crate::config::{PeerConfig, PeersConfig};
use crate::db::DbPool;
use crate::error::{Error, Result};

/// A peer whose reported clock differs from ours by more than this is logged at
/// `warn` (`SPEC-3.17` item 5). No decision compares two instances' clocks.
const CLOCK_SKEW_WARN_SECS: i64 = 60;
/// Upper bound on one pull, further capped by `poll_secs`.
const PULL_TIMEOUT_MAX_SECS: u64 = 10;
/// How long a claim bypass is counted on the Status page.
const BYPASS_WINDOW: chrono::Duration = chrono::Duration::hours(24);

/// A random (version 4) UUID in its canonical text form. `SPEC-3.17` item 2.
pub fn new_uuid_v4() -> Result<String> {
    let mut b = [0u8; 16];
    getrandom::getrandom(&mut b)
        .map_err(|e| Error::Other(anyhow::anyhow!("generating the instance id: {e}")))?;
    b[6] = (b[6] & 0x0f) | 0x40;
    b[8] = (b[8] & 0x3f) | 0x80;
    let h: String = b.iter().map(|x| format!("{x:02x}")).collect();
    Ok(format!(
        "{}-{}-{}-{}-{}",
        &h[0..8],
        &h[8..12],
        &h[12..16],
        &h[16..20],
        &h[20..32]
    ))
}

/// One in-flight job as served to peers: action, job key and the run's start.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct InFlightEntry {
    pub action: String,
    pub job_key: String,
    pub started_at: String,
}

/// The body of `GET /api/peer/state` (`SPEC-3.17` item 4). Carries no secret,
/// prompt, log text or comment body.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PeerStateDoc {
    pub instance_id: String,
    pub instance_name: String,
    pub version: String,
    /// The server's current time, RFC 3339.
    pub time: String,
    /// The count of in-flight jobs.
    pub load: u64,
    pub in_flight: Vec<InFlightEntry>,
}

/// Liveness of one peer. A peer is never called dead (`SPEC-3.17` item 5): a
/// partition and a crash look the same from here.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum PeerState {
    Reachable,
    Unknown,
}

impl PeerState {
    pub fn as_str(self) -> &'static str {
        match self {
            PeerState::Reachable => "reachable",
            PeerState::Unknown => "unknown",
        }
    }
}

/// Everything the poller remembers about one peer.
#[derive(Debug, Clone, Default)]
struct PeerStatus {
    last_success: Option<DateTime<Utc>>,
    last_error: Option<String>,
    snapshot: Option<PeerStateDoc>,
    /// The state last logged, so only a change is logged.
    logged_state: Option<PeerState>,
    /// A `401` or `403` is logged once per transition into the fault.
    auth_fault: bool,
    self_ref: bool,
    skewed: bool,
    /// When each claim bypass happened, pruned to the last 24 hours.
    bypasses: Vec<DateTime<Utc>>,
}

/// One Status page row (`SPEC-3.17` item 11, less the columns later issues add).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PeerRow {
    pub name: String,
    pub url: String,
    pub state: PeerState,
    pub last_success: Option<String>,
    pub last_error: Option<String>,
    /// The peer's in-flight count, only while `reachable`.
    pub in_flight: Option<u64>,
    /// Event claims bypassed for this peer in the last 24 hours (`SPEC-3.17`
    /// item 11). Kept in memory, so a restart resets it.
    pub claims_bypassed: u64,
}

/// Why one pull failed.
#[derive(Debug)]
pub enum PullError {
    /// The peer answered `401` or `403`: a token mismatch, a configuration fault.
    Auth(u16),
    /// Anything else: transport error, other status, undecodable body, no token.
    Other(String),
}

/// Shared per-peer liveness state. Cheap to clone; the lock is never held
/// across an await.
#[derive(Debug, Clone, Default)]
pub struct PeerRegistry {
    inner: Arc<Mutex<BTreeMap<String, PeerStatus>>>,
}

fn state_at(st: &PeerStatus, now: DateTime<Utc>, stale_after_secs: u64) -> PeerState {
    match st.last_success {
        Some(t) if now.signed_duration_since(t).num_seconds() < stale_after_secs as i64 => {
            PeerState::Reachable
        }
        _ => PeerState::Unknown,
    }
}

impl PeerRegistry {
    fn lock(&self) -> std::sync::MutexGuard<'_, BTreeMap<String, PeerStatus>> {
        // A poisoned lock only means a holder panicked; the map is still a
        // valid last-known state, so keep serving it (the panic is logged).
        self.inner.lock().unwrap_or_else(|p| {
            error!("peer registry lock was poisoned by a panic; continuing with its last state");
            p.into_inner()
        })
    }

    /// Drop state for peers no longer configured.
    pub fn retain(&self, names: &HashSet<&str>) {
        self.lock().retain(|k, _| names.contains(k.as_str()));
    }

    /// Record the outcome of one pull and log any state or fault transition.
    pub fn record(
        &self,
        name: &str,
        own_id: &str,
        outcome: std::result::Result<PeerStateDoc, PullError>,
        now: DateTime<Utc>,
        stale_after_secs: u64,
    ) {
        let mut map = self.lock();
        let st = map.entry(name.to_string()).or_default();
        match outcome {
            Ok(doc) if doc.instance_id == own_id => {
                let msg = format!(
                    "peer reports this instance's own id `{own_id}` (its url points back at this instance)"
                );
                if !st.self_ref {
                    error!(peer = %name, "peer refused: {msg}");
                }
                st.self_ref = true;
                st.auth_fault = false;
                st.last_error = Some(msg);
            }
            Ok(doc) => {
                st.self_ref = false;
                st.auth_fault = false;
                match DateTime::parse_from_rfc3339(&doc.time) {
                    Ok(t) => {
                        let skew = now.signed_duration_since(t).num_seconds().abs();
                        let skewed = skew > CLOCK_SKEW_WARN_SECS;
                        if skewed && !st.skewed {
                            warn!(peer = %name, skew_secs = skew, "peer clock differs from the local clock by more than {CLOCK_SKEW_WARN_SECS}s");
                        }
                        st.skewed = skewed;
                    }
                    Err(e) => {
                        warn!(peer = %name, time = %doc.time, error = %e, "peer reported a time that does not parse; clock skew unchecked");
                    }
                }
                st.last_success = Some(now);
                st.last_error = None;
                st.snapshot = Some(doc);
            }
            Err(PullError::Auth(status)) => {
                let msg = format!(
                    "peer answered {status}: token mismatch (check this peer's token_ref against the peer's own web auth token)"
                );
                if !st.auth_fault {
                    error!(peer = %name, status, "{msg}");
                }
                st.auth_fault = true;
                st.self_ref = false;
                st.last_error = Some(msg);
            }
            Err(PullError::Other(msg)) => {
                st.auth_fault = false;
                st.self_ref = false;
                st.last_error = Some(msg);
            }
        }
        let new = state_at(st, now, stale_after_secs);
        if st.logged_state != Some(new) {
            match new {
                PeerState::Reachable => info!(peer = %name, "peer state: reachable"),
                PeerState::Unknown => {
                    warn!(peer = %name, last_error = st.last_error.as_deref().unwrap_or("none"), "peer state: unknown")
                }
            }
            st.logged_state = Some(new);
        }
    }

    /// Status page rows for the configured peers, in configured order.
    pub fn rows(&self, cfg: &PeersConfig, now: DateTime<Utc>) -> Vec<PeerRow> {
        let map = self.lock();
        cfg.peers
            .iter()
            .map(|p| {
                let st = map.get(&p.name).cloned().unwrap_or_default();
                let state = state_at(&st, now, cfg.stale_after_secs);
                PeerRow {
                    name: p.name.clone(),
                    url: p.url.clone(),
                    state,
                    last_success: st.last_success.map(|t| t.to_rfc3339()),
                    last_error: st.last_error.clone(),
                    in_flight: (state == PeerState::Reachable)
                        .then(|| st.snapshot.as_ref().map(|d| d.load))
                        .flatten(),
                    claims_bypassed: st
                        .bypasses
                        .iter()
                        .filter(|t| now.signed_duration_since(**t) < BYPASS_WINDOW)
                        .count() as u64,
                }
            })
            .collect()
    }

    /// The configured peers whose last pull succeeded within
    /// `stale_after_secs`, in configured order. Empty while peering is disabled.
    pub fn reachable_peers(&self, cfg: &PeersConfig, now: DateTime<Utc>) -> Vec<PeerConfig> {
        if !cfg.enabled {
            return Vec::new();
        }
        let map = self.lock();
        cfg.peers
            .iter()
            .filter(|p| {
                map.get(&p.name).is_some_and(|st| {
                    state_at(st, now, cfg.stale_after_secs) == PeerState::Reachable
                })
            })
            .cloned()
            .collect()
    }

    /// Count one bypassed claim against `name` for the Status page.
    pub fn record_bypass(&self, name: &str, now: DateTime<Utc>) {
        let mut map = self.lock();
        let st = map.entry(name.to_string()).or_default();
        st.bypasses
            .retain(|t| now.signed_duration_since(*t) < BYPASS_WINDOW);
        st.bypasses.push(now);
    }

    /// `(action, job_key)` pairs listed in flight by every `reachable` peer
    /// whose snapshot is no older than `stale_after_secs` (`SPEC-3.17` item 6).
    /// Empty while peering is disabled.
    pub fn live_peer_in_flight(
        &self,
        cfg: &PeersConfig,
        now: DateTime<Utc>,
    ) -> HashMap<(String, String), String> {
        let mut out = HashMap::new();
        if !cfg.enabled {
            return out;
        }
        let map = self.lock();
        for p in &cfg.peers {
            let Some(st) = map.get(&p.name) else { continue };
            if state_at(st, now, cfg.stale_after_secs) != PeerState::Reachable {
                continue;
            }
            if let Some(doc) = &st.snapshot {
                for e in &doc.in_flight {
                    out.insert((e.action.clone(), e.job_key.clone()), p.name.clone());
                }
            }
        }
        out
    }
}

/// One bounded `GET <url>/api/peer/state` pull.
pub async fn pull_peer(
    client: &reqwest::Client,
    peer: &PeerConfig,
    token: &str,
    timeout: Duration,
) -> std::result::Result<PeerStateDoc, PullError> {
    let url = format!("{}/api/peer/state", peer.url.trim_end_matches('/'));
    let resp = client
        .get(&url)
        .bearer_auth(token)
        .timeout(timeout)
        .send()
        .await
        .map_err(|e| PullError::Other(format!("request to {url} failed: {e}")))?;
    let status = resp.status();
    if status == StatusCode::UNAUTHORIZED || status == StatusCode::FORBIDDEN {
        return Err(PullError::Auth(status.as_u16()));
    }
    if !status.is_success() {
        return Err(PullError::Other(format!("{url} answered {status}")));
    }
    resp.json::<PeerStateDoc>()
        .await
        .map_err(|e| PullError::Other(format!("{url} returned an undecodable body: {e}")))
}

/// Pull every configured peer once, concurrently, and record each outcome.
/// `tokens` maps a peer name to its bearer token or the reason it could not be
/// resolved.
pub async fn poll_with_tokens(
    cfg: &PeersConfig,
    registry: &PeerRegistry,
    client: &reqwest::Client,
    own_id: &str,
    tokens: HashMap<String, std::result::Result<String, String>>,
) {
    let timeout = Duration::from_secs(cfg.poll_secs.min(PULL_TIMEOUT_MAX_SECS));
    let mut set = tokio::task::JoinSet::new();
    for p in &cfg.peers {
        let peer = p.clone();
        let client = client.clone();
        let token = tokens.get(&p.name).cloned();
        set.spawn(async move {
            let outcome = match token {
                Some(Ok(t)) => pull_peer(&client, &peer, &t, timeout).await,
                Some(Err(e)) => Err(PullError::Other(format!(
                    "token_ref `{}` could not be resolved: {e}",
                    peer.token_ref
                ))),
                None => Err(PullError::Other("no token was resolved".to_string())),
            };
            (peer.name, outcome)
        });
    }
    while let Some(joined) = set.join_next().await {
        match joined {
            Ok((name, outcome)) => {
                registry.record(&name, own_id, outcome, Utc::now(), cfg.stale_after_secs)
            }
            Err(e) => error!(error = %e, "a peer pull task panicked or was cancelled"),
        }
    }
}

async fn resolve_token(pool: &DbPool, name: &str) -> std::result::Result<String, String> {
    let key = crate::secrets::SecretsKey::load().map_err(|e| e.to_string())?;
    let token = crate::secrets::resolve_secret(pool, &key, name)
        .await
        .map_err(|e| e.to_string())?;
    if token.is_empty() {
        return Err(format!("secret `{name}` is empty"));
    }
    Ok(token)
}

/// The poller: every `poll_secs`, pull each peer's state (`SPEC-3.17` item 5).
/// Reads the live config each tick, so a Settings save applies on the next one.
/// With peering disabled it makes no outbound call.
pub async fn poll_loop(pool: DbPool, cfg: Arc<RwLock<PeersConfig>>, registry: PeerRegistry) {
    let client = reqwest::Client::new();
    loop {
        let snapshot = cfg.read().await.clone();
        if snapshot.enabled && !snapshot.peers.is_empty() {
            match crate::settings::ensure_instance_id(&pool).await {
                Ok(own_id) => {
                    registry.retain(&snapshot.peers.iter().map(|p| p.name.as_str()).collect());
                    let mut tokens = HashMap::new();
                    for p in &snapshot.peers {
                        tokens.insert(p.name.clone(), resolve_token(&pool, &p.token_ref).await);
                    }
                    poll_with_tokens(&snapshot, &registry, &client, &own_id, tokens).await;
                }
                Err(e) => {
                    error!(error = %e, "peer poll skipped: reading the instance id failed")
                }
            }
        } else {
            registry.retain(&HashSet::new());
        }
        tokio::time::sleep(Duration::from_secs(snapshot.poll_secs.max(5))).await;
    }
}

/// State of the peer API router (`SPEC-3.17` item 4).
#[derive(Clone)]
pub struct PeerApiState {
    pub pool: DbPool,
    pub cfg: Arc<RwLock<PeersConfig>>,
    /// The instance's own web bearer token (`SPEC-7.6`); `None` when it did not
    /// resolve, in which case every request is refused.
    pub token: Option<Arc<String>>,
}

/// `/api/peer/*`: TCP listener only. While peering is disabled every request is
/// a `404`; otherwise a bearer token is required even on a loopback bind, and a
/// session cookie is not accepted.
pub fn router(state: PeerApiState) -> Router {
    Router::new()
        .route("/api/peer/state", get(peer_state))
        .route("/api/peer/claim", post(peer_claim))
        .layer(axum::middleware::from_fn_with_state(
            state.clone(),
            require_peer_bearer,
        ))
        .with_state(state)
}

async fn require_peer_bearer(
    State(s): State<PeerApiState>,
    request: axum::extract::Request,
    next: Next,
) -> Response {
    if !s.cfg.read().await.enabled {
        return StatusCode::NOT_FOUND.into_response();
    }
    let Some(expected) = s.token.as_ref() else {
        error!("peer API request refused: this instance has no resolved web auth token, so no peer can authenticate");
        return unauthorized();
    };
    match bearer(request.headers()) {
        Some(p) if constant_time_eq(p.as_bytes(), expected.as_bytes()) => next.run(request).await,
        _ => unauthorized(),
    }
}

fn unauthorized() -> Response {
    (
        StatusCode::UNAUTHORIZED,
        [(header::WWW_AUTHENTICATE, "Bearer")],
        "missing or invalid bearer token",
    )
        .into_response()
}

fn bearer(headers: &HeaderMap) -> Option<String> {
    let raw = headers.get(header::AUTHORIZATION)?.to_str().ok()?;
    let (scheme, rest) = raw.split_once(' ')?;
    let t = rest.trim();
    (scheme.eq_ignore_ascii_case("bearer") && !t.is_empty()).then(|| t.to_string())
}

fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    a.iter().zip(b).fold(0u8, |d, (x, y)| d | (x ^ y)) == 0
}

async fn peer_state(State(s): State<PeerApiState>) -> Response {
    let name = s.cfg.read().await.instance_name.clone();
    let id = match crate::settings::ensure_instance_id(&s.pool).await {
        Ok(id) => id,
        Err(e) => {
            error!(error = %e, "peer state: reading the instance id failed");
            return (StatusCode::INTERNAL_SERVER_ERROR, "instance id unavailable").into_response();
        }
    };
    let rows = match crate::state::run_inflight_with_start(&s.pool).await {
        Ok(r) => r,
        Err(e) => {
            error!(error = %e, "peer state: reading the in-flight jobs failed");
            return (StatusCode::INTERNAL_SERVER_ERROR, "in-flight read failed").into_response();
        }
    };
    let in_flight: Vec<InFlightEntry> = rows
        .into_iter()
        .map(|r| InFlightEntry {
            action: r.action_name,
            job_key: r.job_key,
            started_at: r.started_at,
        })
        .collect();
    Json(PeerStateDoc {
        instance_id: id,
        instance_name: name,
        version: crate::VERSION.to_string(),
        time: Utc::now().to_rfc3339(),
        load: in_flight.len() as u64,
        in_flight,
    })
    .into_response()
}

/// The body of `POST /api/peer/claim` (`SPEC-3.17` item 4).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClaimRequest {
    pub event_key: String,
    pub claimant_id: String,
    pub claimant_load: u64,
}

/// The reply to a claim: whether it was granted, and the holder's pair.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClaimResponse {
    pub granted: bool,
    pub holder_id: String,
    pub holder_load: u64,
}

/// Decide a peer's claim against this instance's `peer_claims` row
/// (`SPEC-3.17` item 7). Both sides compare the same two `(load, id)` pairs, so
/// simultaneous deliveries yield exactly one holder.
pub async fn respond_to_claim(
    pool: &DbPool,
    own_id: &str,
    req: &ClaimRequest,
) -> Result<ClaimResponse> {
    let grant = ClaimResponse {
        granted: true,
        holder_id: req.claimant_id.clone(),
        holder_load: req.claimant_load,
    };
    if crate::state::peer_claim_insert(pool, &req.event_key, &req.claimant_id, req.claimant_load)
        .await?
    {
        return Ok(grant);
    }
    let Some(row) = crate::state::peer_claim_get(pool, &req.event_key).await? else {
        return Err(Error::Other(anyhow::anyhow!(
            "peer claim row for `{}` vanished between insert and read",
            req.event_key
        )));
    };
    let refuse = |row: crate::state::PeerClaimRow| ClaimResponse {
        granted: false,
        holder_id: row.holder_id,
        holder_load: row.holder_load,
    };
    if row.holder_id == req.claimant_id {
        return Ok(grant);
    }
    if row.holder_id != own_id {
        return Ok(refuse(row));
    }
    let own_pair = (row.holder_load, row.holder_id.as_str());
    let their_pair = (req.claimant_load, req.claimant_id.as_str());
    if their_pair < own_pair
        && crate::state::peer_claim_reassign(
            pool,
            &req.event_key,
            own_id,
            &req.claimant_id,
            req.claimant_load,
        )
        .await?
    {
        info!(event_key = %req.event_key, holder = %req.claimant_id, "peer claim granted: the claimant has the lower load, so this instance drops its own delivery");
        return Ok(grant);
    }
    // Re-read: a concurrent handover may have changed the holder since.
    match crate::state::peer_claim_get(pool, &req.event_key).await? {
        Some(now_row) if now_row.holder_id == req.claimant_id => Ok(grant),
        Some(now_row) => Ok(refuse(now_row)),
        None => Err(Error::Other(anyhow::anyhow!(
            "peer claim row for `{}` vanished during a claim",
            req.event_key
        ))),
    }
}

async fn peer_claim(State(s): State<PeerApiState>, Json(req): Json<ClaimRequest>) -> Response {
    if req.event_key.is_empty() || req.claimant_id.is_empty() {
        return (
            StatusCode::BAD_REQUEST,
            "event_key and claimant_id are required",
        )
            .into_response();
    }
    let own_id = match crate::settings::ensure_instance_id(&s.pool).await {
        Ok(id) => id,
        Err(e) => {
            error!(error = %e, "peer claim: reading the instance id failed");
            return (StatusCode::INTERNAL_SERVER_ERROR, "instance id unavailable").into_response();
        }
    };
    match respond_to_claim(&s.pool, &own_id, &req).await {
        Ok(resp) => Json(resp).into_response(),
        Err(e) => {
            error!(event_key = %req.event_key, claimant = %req.claimant_id, error = %e, "peer claim failed");
            (StatusCode::INTERNAL_SERVER_ERROR, "claim failed").into_response()
        }
    }
}

/// What the webhook pipeline does with a delivery after the claim step.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ClaimDecision {
    /// No claim ran, every peer granted, or every failed peer was bypassed.
    Proceed,
    /// Another instance holds the event: answer `200` and drop it.
    Drop { holder: String },
}

/// The claim step of the webhook pipeline (`SPEC-3.17` item 7). Cheap to clone.
#[derive(Clone)]
pub struct PeerClaims {
    pool: DbPool,
    cfg: Arc<RwLock<PeersConfig>>,
    registry: PeerRegistry,
    client: reqwest::Client,
}

impl PeerClaims {
    pub fn new(pool: DbPool, cfg: Arc<RwLock<PeersConfig>>, registry: PeerRegistry) -> Self {
        Self {
            pool,
            cfg,
            registry,
            client: reqwest::Client::new(),
        }
    }

    /// Claim `event_key` for this instance. `event_type` names the event for
    /// the log when no key could be built. With peering disabled or no peer
    /// configured this writes nothing and calls no peer.
    pub async fn claim(
        &self,
        event_key: Option<&str>,
        event_type: &str,
        source: &str,
        delivery: &str,
    ) -> ClaimDecision {
        let cfg = self.cfg.read().await.clone();
        if !cfg.enabled || cfg.peers.is_empty() {
            return ClaimDecision::Proceed;
        }
        let Some(event_key) = event_key else {
            warn!(
                source,
                delivery,
                event = event_type,
                "webhook event identity could not be built; processing without a peer claim"
            );
            return ClaimDecision::Proceed;
        };
        let own_id = match crate::settings::ensure_instance_id(&self.pool).await {
            Ok(id) => id,
            Err(e) => {
                error!(source, delivery, event_key, error = %e, "peer claim skipped: reading the instance id failed; processing without a claim");
                return ClaimDecision::Proceed;
            }
        };
        let load = match crate::state::run_inflight_count(&self.pool).await {
            Ok(n) => n,
            Err(e) => {
                error!(source, delivery, event_key, error = %e, "peer claim skipped: reading the in-flight count failed; processing without a claim");
                return ClaimDecision::Proceed;
            }
        };
        match crate::state::peer_claim_insert(&self.pool, event_key, &own_id, load).await {
            Ok(true) => {}
            Ok(false) => return self.drop_for_existing(event_key, source, delivery).await,
            Err(e) => {
                error!(source, delivery, event_key, error = %e, "peer claim skipped: writing the claim row failed; processing without a claim");
                return ClaimDecision::Proceed;
            }
        }
        let peers = self.registry.reachable_peers(&cfg, Utc::now());
        let mut tokens = HashMap::new();
        for p in &peers {
            tokens.insert(
                p.name.clone(),
                resolve_token(&self.pool, &p.token_ref).await,
            );
        }
        let req = ClaimRequest {
            event_key: event_key.to_string(),
            claimant_id: own_id.clone(),
            claimant_load: load,
        };
        let decision = self
            .ask_peers(&cfg, peers, tokens, &req, source, delivery)
            .await;
        if decision != ClaimDecision::Proceed {
            return decision;
        }
        // A peer's claim may have taken the row while ours was in flight.
        match crate::state::peer_claim_get(&self.pool, event_key).await {
            Ok(Some(row)) if row.holder_id != own_id => {
                info!(source, delivery, event_key, holder = %row.holder_id, "event claimed by a peer meanwhile; delivery dropped");
                ClaimDecision::Drop {
                    holder: row.holder_id,
                }
            }
            Ok(_) => ClaimDecision::Proceed,
            Err(e) => {
                error!(source, delivery, event_key, error = %e, "re-reading the claim row failed; processing the event");
                ClaimDecision::Proceed
            }
        }
    }

    async fn drop_for_existing(
        &self,
        event_key: &str,
        source: &str,
        delivery: &str,
    ) -> ClaimDecision {
        let holder = match crate::state::peer_claim_get(&self.pool, event_key).await {
            Ok(Some(row)) => row.holder_id,
            Ok(None) => "unknown".to_string(),
            Err(e) => {
                error!(event_key, error = %e, "reading the existing claim row failed");
                "unknown".to_string()
            }
        };
        info!(source, delivery, event_key, holder = %holder, "event already claimed; delivery dropped");
        ClaimDecision::Drop { holder }
    }

    /// Send the claim to every peer in `peers`, bounded by `claim_timeout_secs`.
    /// `tokens` maps a peer name to its bearer token or why it did not resolve.
    pub async fn ask_peers(
        &self,
        cfg: &PeersConfig,
        peers: Vec<PeerConfig>,
        tokens: HashMap<String, std::result::Result<String, String>>,
        req: &ClaimRequest,
        source: &str,
        delivery: &str,
    ) -> ClaimDecision {
        let timeout = Duration::from_secs(cfg.claim_timeout_secs);
        let mut set = tokio::task::JoinSet::new();
        for p in peers {
            let client = self.client.clone();
            let token = tokens.get(&p.name).cloned();
            let req = req.clone();
            set.spawn(async move {
                let outcome = match token {
                    Some(Ok(t)) => send_claim(&client, &p, &t, &req, timeout).await,
                    Some(Err(e)) => Err(format!(
                        "token_ref `{}` could not be resolved: {e}",
                        p.token_ref
                    )),
                    None => Err("no token was resolved".to_string()),
                };
                (p.name, outcome)
            });
        }
        let mut refused: Option<(String, ClaimResponse)> = None;
        while let Some(joined) = set.join_next().await {
            match joined {
                Ok((name, Ok(resp))) if resp.granted => {
                    debug!(peer = %name, event_key = %req.event_key, "peer granted the claim");
                }
                Ok((name, Ok(resp))) => {
                    if refused.is_none() {
                        refused = Some((name, resp));
                    }
                }
                Ok((name, Err(e))) => {
                    warn!(peer = %name, event_key = %req.event_key, error = %e, "peer claim bypassed: the peer did not answer; processing the event");
                    self.registry.record_bypass(&name, Utc::now());
                }
                Err(e) => error!(error = %e, "a peer claim task panicked or was cancelled"),
            }
        }
        if let Some((name, resp)) = refused {
            info!(source, delivery, event_key = %req.event_key, peer = %name, holder = %resp.holder_id, holder_load = resp.holder_load, "claim refused; delivery dropped");
            return ClaimDecision::Drop {
                holder: resp.holder_id,
            };
        }
        ClaimDecision::Proceed
    }
}

/// One bounded `POST <url>/api/peer/claim`.
async fn send_claim(
    client: &reqwest::Client,
    peer: &PeerConfig,
    token: &str,
    req: &ClaimRequest,
    timeout: Duration,
) -> std::result::Result<ClaimResponse, String> {
    let url = format!("{}/api/peer/claim", peer.url.trim_end_matches('/'));
    let resp = client
        .post(&url)
        .bearer_auth(token)
        .timeout(timeout)
        .json(req)
        .send()
        .await
        .map_err(|e| format!("request to {url} failed: {e}"))?;
    let status = resp.status();
    if !status.is_success() {
        return Err(format!("{url} answered {status}"));
    }
    resp.json::<ClaimResponse>()
        .await
        .map_err(|e| format!("{url} returned an undecodable body: {e}"))
}

/// Two-instance harness shared by the peer and webhook tests: each [`Node`] has
/// its own store, a peer API on a random port and the claim step.
#[cfg(test)]
pub(crate) mod testkit {
    use super::*;

    fn peer(name: &str, url: &str) -> PeerConfig {
        PeerConfig {
            name: name.into(),
            url: url.into(),
            token_ref: format!("{name}-token"),
        }
    }

    fn doc(id: &str, now: DateTime<Utc>) -> PeerStateDoc {
        PeerStateDoc {
            instance_id: id.into(),
            instance_name: "remote".into(),
            version: "t".into(),
            time: now.to_rfc3339(),
            load: 0,
            in_flight: Vec::new(),
        }
    }

    /// One in-process instance: its own store, peer API on a random port, and
    /// the claim step. `link` wires two instances as each other's peers.
    pub(crate) struct Node {
        pub name: String,
        pub pool: DbPool,
        pub id: String,
        pub addr: String,
        pub token: String,
        pub cfg: Arc<RwLock<PeersConfig>>,
        pub registry: PeerRegistry,
        pub claims: PeerClaims,
    }

    pub(crate) async fn set_key_file(tmp: &std::path::Path) {
        use age::secrecy::ExposeSecret;
        let key_path = tmp.join("key.txt");
        let identity = age::x25519::Identity::generate();
        std::fs::write(&key_path, identity.to_string().expose_secret()).unwrap();
        std::env::set_var(crate::secrets::KEY_FILE_ENV, &key_path);
    }

    pub(crate) async fn node(name: &str, load: usize) -> Node {
        node_on(crate::db::test_pool().await, name, load).await
    }

    pub(crate) async fn node_on(pool: DbPool, name: &str, load: usize) -> Node {
        let id = crate::settings::ensure_instance_id(&pool).await.unwrap();
        if load > 0 {
            let run = crate::state::run_begin(&pool, "act", "scheduler")
                .await
                .unwrap();
            for i in 0..load {
                crate::state::run_inflight_add(&pool, run, &format!("MK-{i}"), "MK")
                    .await
                    .unwrap();
            }
        }
        let cfg = Arc::new(RwLock::new(PeersConfig {
            enabled: true,
            instance_name: name.into(),
            claim_timeout_secs: 1,
            ..PeersConfig::default()
        }));
        let token = format!("tok-{name}");
        let app = router(PeerApiState {
            pool: pool.clone(),
            cfg: cfg.clone(),
            token: Some(Arc::new(token.clone())),
        });
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = format!("http://{}", listener.local_addr().unwrap());
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let registry = PeerRegistry::default();
        let claims = PeerClaims::new(pool.clone(), cfg.clone(), registry.clone());
        Node {
            name: name.into(),
            pool,
            id,
            addr,
            token,
            cfg,
            registry,
            claims,
        }
    }

    /// Make `a` list `b` as a reachable peer, with `b`'s token stored in `a`'s
    /// secret store.
    pub(crate) async fn link(a: &Node, b: &Node) {
        a.cfg.write().await.peers.push(peer(&b.name, &b.addr));
        let key = crate::secrets::SecretsKey::load().unwrap();
        crate::state::secret_create(
            &a.pool,
            &format!("{}-token", b.name),
            &key.encrypt(&b.token).unwrap(),
        )
        .await
        .unwrap();
        let now = Utc::now();
        a.registry
            .record(&b.name, &a.id, Ok(doc(&b.id, now)), now, 120);
    }

    pub(crate) async fn pair(load_a: usize, load_b: usize) -> (Node, Node) {
        let a = node("a", load_a).await;
        let b = node("b", load_b).await;
        link(&a, &b).await;
        link(&b, &a).await;
        (a, b)
    }
}

#[cfg(test)]
mod tests {
    use super::testkit::*;
    use super::*;
    use axum::body::Body;
    use axum::http::Request;
    use tower::ServiceExt;

    fn cfg_for(peers: Vec<PeerConfig>) -> PeersConfig {
        PeersConfig {
            enabled: true,
            instance_name: "local".into(),
            peers,
            ..PeersConfig::default()
        }
    }

    fn peer(name: &str, url: &str) -> PeerConfig {
        PeerConfig {
            name: name.into(),
            url: url.into(),
            token_ref: format!("{name}-token"),
        }
    }

    fn doc(id: &str, keys: &[(&str, &str)], time: DateTime<Utc>) -> PeerStateDoc {
        PeerStateDoc {
            instance_id: id.into(),
            instance_name: "remote".into(),
            version: "t".into(),
            time: time.to_rfc3339(),
            load: keys.len() as u64,
            in_flight: keys
                .iter()
                .map(|(a, k)| InFlightEntry {
                    action: (*a).into(),
                    job_key: (*k).into(),
                    started_at: "2026-01-01T00:00:00Z".into(),
                })
                .collect(),
        }
    }

    async fn api(enabled: bool, token: Option<&str>) -> (Router, DbPool) {
        let pool = crate::db::test_pool().await;
        let mut cfg = cfg_for(vec![]);
        cfg.enabled = enabled;
        let router = router(PeerApiState {
            pool: pool.clone(),
            cfg: Arc::new(RwLock::new(cfg)),
            token: token.map(|t| Arc::new(t.to_string())),
        });
        (router, pool)
    }

    fn get_state(bearer: Option<&str>) -> Request<Body> {
        let mut b = Request::builder().uri("/api/peer/state");
        if let Some(t) = bearer {
            b = b.header(header::AUTHORIZATION, format!("Bearer {t}"));
        }
        b.body(Body::empty()).unwrap()
    }

    #[test]
    fn uuid_v4_has_the_canonical_shape() {
        let a = new_uuid_v4().unwrap();
        assert_eq!(a.len(), 36);
        assert_eq!(a.as_bytes()[14], b'4');
        assert_ne!(a, new_uuid_v4().unwrap());
    }

    #[tokio::test]
    async fn disabled_peering_answers_404_even_with_a_valid_token() {
        let (r, _pool) = api(false, Some("tok")).await;
        let resp = r.oneshot(get_state(Some("tok"))).await.unwrap();
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn enabled_peer_state_requires_the_bearer_token() {
        let (r, _pool) = api(true, Some("tok")).await;
        assert_eq!(
            r.clone().oneshot(get_state(None)).await.unwrap().status(),
            StatusCode::UNAUTHORIZED
        );
        assert_eq!(
            r.clone()
                .oneshot(get_state(Some("wrong")))
                .await
                .unwrap()
                .status(),
            StatusCode::UNAUTHORIZED
        );
        let resp = r.oneshot(get_state(Some("tok"))).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let body = axum::body::to_bytes(resp.into_body(), 1 << 20)
            .await
            .unwrap();
        let d: PeerStateDoc = serde_json::from_slice(&body).unwrap();
        assert_eq!(d.instance_name, "local");
        assert_eq!(d.load, 0);
    }

    #[tokio::test]
    async fn an_unresolved_own_token_refuses_every_request() {
        let (r, _pool) = api(true, None).await;
        let resp = r.oneshot(get_state(Some("anything"))).await.unwrap();
        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn peer_state_lists_an_open_runs_in_flight_job() {
        let (r, pool) = api(true, Some("tok")).await;
        let run = crate::state::run_begin(&pool, "act", "scheduler")
            .await
            .unwrap();
        crate::state::run_inflight_add(&pool, run, "MK-1", "MK")
            .await
            .unwrap();
        let resp = r.oneshot(get_state(Some("tok"))).await.unwrap();
        let body = axum::body::to_bytes(resp.into_body(), 1 << 20)
            .await
            .unwrap();
        let d: PeerStateDoc = serde_json::from_slice(&body).unwrap();
        assert_eq!(d.load, 1);
        assert_eq!(d.in_flight[0].action, "act");
        assert_eq!(d.in_flight[0].job_key, "MK-1");
    }

    #[test]
    fn a_reachable_peer_turns_unknown_after_stale_after_secs() {
        let reg = PeerRegistry::default();
        let cfg = cfg_for(vec![peer("b", "http://b.example.org")]);
        let t0 = Utc::now();
        reg.record(
            "b",
            "me",
            Ok(doc("them", &[("act", "MK-1")], t0)),
            t0,
            cfg.stale_after_secs,
        );
        let rows = reg.rows(&cfg, t0);
        assert_eq!(rows[0].state, PeerState::Reachable);
        assert_eq!(rows[0].in_flight, Some(1));
        assert_eq!(reg.live_peer_in_flight(&cfg, t0).len(), 1);

        let later = t0 + chrono::Duration::seconds(cfg.stale_after_secs as i64 + 1);
        reg.record(
            "b",
            "me",
            Err(PullError::Other("timeout".into())),
            later,
            cfg.stale_after_secs,
        );
        let rows = reg.rows(&cfg, later);
        assert_eq!(rows[0].state, PeerState::Unknown);
        assert_eq!(rows[0].last_error.as_deref(), Some("timeout"));
        assert_eq!(rows[0].in_flight, None);
        assert!(reg.live_peer_in_flight(&cfg, later).is_empty());
    }

    #[test]
    fn auth_replies_and_a_self_reference_never_count_as_success() {
        let reg = PeerRegistry::default();
        let cfg = cfg_for(vec![peer("b", "http://b.example.org")]);
        let now = Utc::now();
        reg.record("b", "me", Err(PullError::Auth(401)), now, 120);
        let r = &reg.rows(&cfg, now)[0];
        assert_eq!(r.state, PeerState::Unknown);
        assert!(r.last_error.as_deref().unwrap().contains("401"));
        assert!(r.last_error.as_deref().unwrap().contains("token mismatch"));

        reg.record("b", "me", Ok(doc("me", &[], now)), now, 120);
        let r = &reg.rows(&cfg, now)[0];
        assert_eq!(r.state, PeerState::Unknown);
        assert!(r.last_error.as_deref().unwrap().contains("own id"));
    }

    /// Serves a fixed state doc, or `status` when set, on a random port.
    async fn stub_peer(doc: PeerStateDoc, status: StatusCode) -> String {
        let app = Router::new().route(
            "/api/peer/state",
            get(move || {
                let doc = doc.clone();
                async move {
                    if status == StatusCode::OK {
                        Json(doc).into_response()
                    } else {
                        status.into_response()
                    }
                }
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        format!("http://{addr}")
    }

    #[tokio::test]
    async fn the_poller_marks_a_stub_peer_reachable_and_a_401_peer_unknown() {
        let good = stub_peer(doc("them", &[("act", "MK-9")], Utc::now()), StatusCode::OK).await;
        let bad = stub_peer(doc("x", &[], Utc::now()), StatusCode::UNAUTHORIZED).await;
        let cfg = cfg_for(vec![peer("good", &good), peer("bad", &bad)]);
        let reg = PeerRegistry::default();
        let mut tokens = HashMap::new();
        tokens.insert("good".to_string(), Ok("t".to_string()));
        tokens.insert("bad".to_string(), Ok("t".to_string()));
        poll_with_tokens(&cfg, &reg, &reqwest::Client::new(), "me", tokens).await;

        let rows = reg.rows(&cfg, Utc::now());
        assert_eq!(rows[0].name, "good");
        assert_eq!(rows[0].state, PeerState::Reachable);
        assert_eq!(rows[1].name, "bad");
        assert_eq!(rows[1].state, PeerState::Unknown);
        assert!(rows[1].last_error.as_deref().unwrap().contains("401"));
        let live = reg.live_peer_in_flight(&cfg, Utc::now());
        assert_eq!(
            live.get(&("act".to_string(), "MK-9".to_string()))
                .map(String::as_str),
            Some("good")
        );
    }

    #[tokio::test]
    async fn an_unresolvable_token_is_a_recorded_failure() {
        let cfg = cfg_for(vec![peer("b", "http://127.0.0.1:1")]);
        let reg = PeerRegistry::default();
        let mut tokens = HashMap::new();
        tokens.insert(
            "b".to_string(),
            Err("no secret named `b-token`".to_string()),
        );
        poll_with_tokens(&cfg, &reg, &reqwest::Client::new(), "me", tokens).await;
        let r = &reg.rows(&cfg, Utc::now())[0];
        assert_eq!(r.state, PeerState::Unknown);
        assert!(r.last_error.as_deref().unwrap().contains("b-token"));
    }

    // ---- Event claims (SPEC-3.17 item 7) ----

    use crate::testutil::LogCapture;

    fn proceeds(d: &ClaimDecision) -> bool {
        *d == ClaimDecision::Proceed
    }

    #[tokio::test]
    async fn the_lower_load_instance_wins_a_simultaneous_claim() {
        let _env = crate::secrets::key_file_env_lock().await;
        let tmp = tempfile::tempdir().unwrap();
        set_key_file(tmp.path()).await;
        let (a, b) = pair(2, 1).await;
        for i in 0..25 {
            let key = format!("forgejo:o/r:action_run:{i}:failure");
            let (da, db) = tokio::join!(
                a.claims.claim(Some(&key), "failure", "src", "d-a"),
                b.claims.claim(Some(&key), "failure", "src", "d-b"),
            );
            assert!(!proceeds(&da) && proceeds(&db), "key {key}: {da:?} {db:?}");
        }
    }

    #[tokio::test]
    async fn equal_loads_are_decided_by_the_lower_instance_id() {
        let _env = crate::secrets::key_file_env_lock().await;
        let tmp = tempfile::tempdir().unwrap();
        set_key_file(tmp.path()).await;
        let (a, b) = pair(0, 0).await;
        let a_wins = a.id < b.id;
        for i in 0..25 {
            let key = format!("github:o/r:comment:{i}");
            let (da, db) = tokio::join!(
                a.claims.claim(Some(&key), "comment", "src", "d-a"),
                b.claims.claim(Some(&key), "comment", "src", "d-b"),
            );
            assert_eq!(proceeds(&da), a_wins, "key {key}: {da:?} {db:?}");
            assert_eq!(proceeds(&db), !a_wins, "key {key}: {da:?} {db:?}");
        }
    }

    #[tokio::test]
    async fn a_refused_claim_drops_and_logs_the_holder_at_info() {
        let _env = crate::secrets::key_file_env_lock().await;
        let tmp = tempfile::tempdir().unwrap();
        set_key_file(tmp.path()).await;
        let (a, b) = pair(0, 5).await;
        let key = "forgejo:o/r:pull_request:7:closed:abc";
        assert!(proceeds(
            &a.claims.claim(Some(key), "pull_request", "s", "d1").await
        ));
        let logs = LogCapture::start();
        let d = b.claims.claim(Some(key), "pull_request", "s", "d2").await;
        assert_eq!(
            d,
            ClaimDecision::Drop {
                holder: a.id.clone()
            }
        );
        let text = logs.text();
        assert!(text.contains("INFO") && text.contains(&a.id), "{text}");
        // The same event delivered to b again hits its own row and is dropped
        // without another peer call.
        let again = b.claims.claim(Some(key), "pull_request", "s", "d3").await;
        assert_eq!(
            again,
            ClaimDecision::Drop {
                holder: a.id.clone()
            }
        );
    }

    /// A peer answering `claim` with `reply`, after `delay`.
    async fn claim_stub(status: StatusCode, body: &'static str, delay: Duration) -> String {
        let app = Router::new().route(
            "/api/peer/claim",
            post(move || async move {
                tokio::time::sleep(delay).await;
                (status, [(header::CONTENT_TYPE, "application/json")], body)
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        format!("http://{addr}")
    }

    /// `a` with one reachable peer `bad` at `url`.
    async fn node_with_stub_peer(url: &str) -> Node {
        let a = node("a", 0).await;
        a.cfg.write().await.peers.push(peer("bad", url));
        let key = crate::secrets::SecretsKey::load().unwrap();
        crate::state::secret_create(&a.pool, "bad-token", &key.encrypt("t").unwrap())
            .await
            .unwrap();
        let now = Utc::now();
        a.registry
            .record("bad", &a.id, Ok(doc("them", &[], now)), now, 120);
        a
    }

    async fn assert_bypassed(url: &str, what: &str) {
        let a = node_with_stub_peer(url).await;
        let logs = LogCapture::start();
        let key = format!("forgejo:o/r:action_run:1:{what}");
        let d = a.claims.claim(Some(&key), "failure", "s", "d").await;
        assert!(proceeds(&d), "{what}: {d:?}");
        let text = logs.text();
        assert!(
            text.contains("WARN") && text.contains("bad") && text.contains(&key),
            "{what}: {text}"
        );
        let cfg = a.cfg.read().await.clone();
        assert_eq!(
            a.registry.rows(&cfg, Utc::now())[0].claims_bypassed,
            1,
            "{what}"
        );
    }

    #[tokio::test]
    async fn a_peer_that_times_out_errors_or_returns_garbage_is_bypassed() {
        let _env = crate::secrets::key_file_env_lock().await;
        let tmp = tempfile::tempdir().unwrap();
        set_key_file(tmp.path()).await;
        let slow = claim_stub(StatusCode::OK, "{}", Duration::from_secs(3)).await;
        assert_bypassed(&slow, "timeout").await;
        let err = claim_stub(StatusCode::INTERNAL_SERVER_ERROR, "boom", Duration::ZERO).await;
        assert_bypassed(&err, "error").await;
        let garbage = claim_stub(StatusCode::OK, "not json", Duration::ZERO).await;
        assert_bypassed(&garbage, "garbage").await;
        assert_bypassed("http://127.0.0.1:1", "refused").await;
    }

    #[tokio::test]
    async fn a_failed_peer_does_not_block_a_granting_peer() {
        let _env = crate::secrets::key_file_env_lock().await;
        let tmp = tempfile::tempdir().unwrap();
        set_key_file(tmp.path()).await;
        let (a, b) = pair(0, 0).await;
        let dead = "http://127.0.0.1:1";
        a.cfg.write().await.peers.push(peer("dead", dead));
        let key = crate::secrets::SecretsKey::load().unwrap();
        crate::state::secret_create(&a.pool, "dead-token", &key.encrypt("t").unwrap())
            .await
            .unwrap();
        let now = Utc::now();
        a.registry
            .record("dead", &a.id, Ok(doc("x", &[], now)), now, 120);
        let d = a
            .claims
            .claim(Some("k:o/r:comment:1"), "comment", "s", "d")
            .await;
        assert!(proceeds(&d));
        let row = crate::state::peer_claim_get(&b.pool, "k:o/r:comment:1")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(row.holder_id, a.id, "the live peer recorded the claimant");
    }

    #[tokio::test]
    async fn disabled_peering_or_no_peer_writes_no_row_and_calls_no_peer() {
        let _env = crate::secrets::key_file_env_lock().await;
        let tmp = tempfile::tempdir().unwrap();
        set_key_file(tmp.path()).await;
        let (a, b) = pair(0, 0).await;
        a.cfg.write().await.enabled = false;
        assert!(proceeds(&a.claims.claim(Some("k1"), "e", "s", "d").await));
        assert!(crate::state::peer_claim_get(&a.pool, "k1")
            .await
            .unwrap()
            .is_none());
        assert!(crate::state::peer_claim_get(&b.pool, "k1")
            .await
            .unwrap()
            .is_none());
        a.cfg.write().await.enabled = true;
        a.cfg.write().await.peers.clear();
        assert!(proceeds(&a.claims.claim(Some("k2"), "e", "s", "d").await));
        assert!(crate::state::peer_claim_get(&a.pool, "k2")
            .await
            .unwrap()
            .is_none());
        assert!(crate::state::peer_claim_get(&b.pool, "k2")
            .await
            .unwrap()
            .is_none());
    }

    #[tokio::test]
    async fn an_unbuildable_identity_is_processed_without_a_claim_and_logged() {
        let _env = crate::secrets::key_file_env_lock().await;
        let tmp = tempfile::tempdir().unwrap();
        set_key_file(tmp.path()).await;
        let (a, b) = pair(0, 0).await;
        let logs = LogCapture::start();
        assert!(proceeds(
            &a.claims.claim(None, "pull_request", "s", "d").await
        ));
        let text = logs.text();
        assert!(
            text.contains("WARN") && text.contains("pull_request"),
            "{text}"
        );
        assert!(crate::state::peer_claim_get(&b.pool, "pull_request")
            .await
            .unwrap()
            .is_none());
    }

    #[tokio::test]
    async fn respond_to_claim_covers_each_row_state() {
        let pool = crate::db::test_pool().await;
        let req = |load: u64, id: &str| ClaimRequest {
            event_key: "k".into(),
            claimant_id: id.into(),
            claimant_load: load,
        };
        // No row: grant and record the claimant.
        assert!(
            respond_to_claim(&pool, "me", &req(3, "x"))
                .await
                .unwrap()
                .granted
        );
        // The row names a third party: a different claimant is refused.
        let r = respond_to_claim(&pool, "me", &req(0, "y")).await.unwrap();
        assert!(!r.granted && r.holder_id == "x" && r.holder_load == 3);
        // The same claimant again is granted (idempotent).
        assert!(
            respond_to_claim(&pool, "me", &req(3, "x"))
                .await
                .unwrap()
                .granted
        );
        // Own row: a higher claimant is refused, a lower one takes it.
        crate::state::peer_claim_insert(&pool, "own", "me", 2)
            .await
            .unwrap();
        let own = |load: u64, id: &str| ClaimRequest {
            event_key: "own".into(),
            claimant_id: id.into(),
            claimant_load: load,
        };
        let r = respond_to_claim(&pool, "me", &own(3, "x")).await.unwrap();
        assert!(!r.granted && r.holder_id == "me");
        assert!(
            respond_to_claim(&pool, "me", &own(1, "x"))
                .await
                .unwrap()
                .granted
        );
        let row = crate::state::peer_claim_get(&pool, "own")
            .await
            .unwrap()
            .unwrap();
        assert_eq!((row.holder_id.as_str(), row.holder_load), ("x", 1));
    }

    #[tokio::test]
    async fn the_claim_endpoint_requires_the_bearer_token_and_a_key() {
        let (r, _pool) = api(true, Some("tok")).await;
        let post_claim = |bearer: Option<&str>, body: &str| {
            let mut b = Request::builder()
                .method("POST")
                .uri("/api/peer/claim")
                .header(header::CONTENT_TYPE, "application/json");
            if let Some(t) = bearer {
                b = b.header(header::AUTHORIZATION, format!("Bearer {t}"));
            }
            b.body(Body::from(body.to_string())).unwrap()
        };
        let ok = r#"{"event_key":"k","claimant_id":"x","claimant_load":1}"#;
        assert_eq!(
            r.clone()
                .oneshot(post_claim(None, ok))
                .await
                .unwrap()
                .status(),
            StatusCode::UNAUTHORIZED
        );
        let resp = r
            .clone()
            .oneshot(post_claim(Some("tok"), ok))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let empty = r#"{"event_key":"","claimant_id":"x","claimant_load":1}"#;
        assert_eq!(
            r.oneshot(post_claim(Some("tok"), empty))
                .await
                .unwrap()
                .status(),
            StatusCode::BAD_REQUEST
        );
    }
}
