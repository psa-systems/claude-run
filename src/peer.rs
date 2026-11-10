//! Optional peering between independently operating instances (`SPEC-3.17`).
//!
//! This module holds the instance identity helper, the read-only peer state
//! endpoint (`GET /api/peer/state`), the peer client, the per-peer liveness
//! registry and the poller. A peer is never a dependency of a run: every call
//! here is bounded and its failure is recorded, shown and logged, never
//! propagated into a run.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use axum::extract::State;
use axum::http::{header, HeaderMap, StatusCode};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::{Json, Router};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;
use tracing::{error, info, warn};

use crate::config::{PeerConfig, PeersConfig};
use crate::db::DbPool;
use crate::error::{Error, Result};

/// A peer whose reported clock differs from ours by more than this is logged at
/// `warn` (`SPEC-3.17` item 5). No decision compares two instances' clocks.
const CLOCK_SKEW_WARN_SECS: i64 = 60;
/// Upper bound on one pull, further capped by `poll_secs`.
const PULL_TIMEOUT_MAX_SECS: u64 = 10;

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
                }
            })
            .collect()
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

#[cfg(test)]
mod tests {
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
}
