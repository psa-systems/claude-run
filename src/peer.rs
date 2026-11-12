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

use axum::extract::{Query, State};
use axum::http::{header, HeaderMap, StatusCode};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
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
/// Pages one summary sync fetches from one peer per tick; a peer with more
/// history than this is `behind` until a later tick finishes it.
const SUMMARY_PAGES_PER_TICK: usize = 20;
/// How long a claim bypass is counted on the Status page.
const BYPASS_WINDOW: chrono::Duration = chrono::Duration::hours(24);

/// The prefix every generated peer API key starts with (CLAUDE-636).
pub const API_KEY_PREFIX: &str = "crk_";

/// Generate a new peer API key: [`API_KEY_PREFIX`] followed by 64 lowercase
/// hex characters from 32 bytes of randomness. The plaintext is never stored
/// or logged; only [`hash_api_key`] of it is. CLAUDE-636.
pub fn generate_api_key() -> Result<String> {
    let mut b = [0u8; 32];
    getrandom::getrandom(&mut b)
        .map_err(|e| Error::Other(anyhow::anyhow!("generating an API key: {e}")))?;
    let hex: String = b.iter().map(|x| format!("{x:02x}")).collect();
    Ok(format!("{API_KEY_PREFIX}{hex}"))
}

/// Lowercase hex SHA-256 of `key`, for storage and lookup; never the
/// plaintext itself. CLAUDE-636.
pub fn hash_api_key(key: &str) -> String {
    let mut h = Sha256::new();
    h.update(key.as_bytes());
    hex::encode(h.finalize())
}

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
    /// The outcome of the last summary sync (`SPEC-3.17` item 9).
    sync: SummarySync,
}

/// Where a peer's run-summary replication stands (`SPEC-3.17` items 9 and 11).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub enum SummarySync {
    /// No sync has run since this process started.
    #[default]
    Pending,
    /// The last sync reached the end of the peer's rows.
    CaughtUp,
    /// The last sync applied rows and the peer has more.
    Behind,
    /// The peer sent a batch that failed validation; the cursor did not move.
    Rejected(String),
    /// The sync could not complete (transport, status or local store error).
    Failed(String),
}

impl SummarySync {
    /// The Status page and report note text.
    pub fn label(&self) -> String {
        match self {
            SummarySync::Pending => "not yet synced".to_string(),
            SummarySync::CaughtUp => "caught up".to_string(),
            SummarySync::Behind => "behind".to_string(),
            SummarySync::Rejected(m) => format!("batch rejected: {m}"),
            SummarySync::Failed(m) => format!("failed: {m}"),
        }
    }

    pub fn is_caught_up(&self) -> bool {
        matches!(self, SummarySync::CaughtUp)
    }
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
    /// Summary replication state (`SPEC-3.17` item 11).
    pub summary_sync: SummarySync,
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
                    "peer answered {status}: token mismatch (check that this peer's token_ref secret holds a key generated in the other instance's API Keys section)"
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
                    summary_sync: st.sync.clone(),
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

    /// The `instance_id` the peer last reported on a successful state pull,
    /// kept across later failures. `None` until the first success.
    pub fn peer_instance_id(&self, name: &str) -> Option<String> {
        self.lock()
            .get(name)
            .and_then(|st| st.snapshot.as_ref().map(|d| d.instance_id.clone()))
    }

    /// The peer's current liveness state and summary sync state.
    pub fn liveness_and_sync(
        &self,
        name: &str,
        cfg: &PeersConfig,
        now: DateTime<Utc>,
    ) -> (PeerState, SummarySync) {
        let map = self.lock();
        match map.get(name) {
            Some(st) => (state_at(st, now, cfg.stale_after_secs), st.sync.clone()),
            None => (PeerState::Unknown, SummarySync::Pending),
        }
    }

    /// Record the outcome of one summary sync and log a change of it.
    pub fn record_sync(&self, name: &str, sync: SummarySync) {
        let mut map = self.lock();
        let st = map.entry(name.to_string()).or_default();
        if st.sync != sync {
            match &sync {
                SummarySync::Rejected(m) => {
                    error!(peer = %name, "peer summary batch rejected, cursor unchanged: {m}")
                }
                SummarySync::Failed(m) => warn!(peer = %name, "peer summary sync failed: {m}"),
                SummarySync::CaughtUp | SummarySync::Behind => {
                    info!(peer = %name, state = %sync.label(), "peer summary sync")
                }
                SummarySync::Pending => {}
            }
        }
        st.sync = sync;
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
                    poll_with_tokens(&snapshot, &registry, &client, &own_id, tokens.clone()).await;
                    sync_summaries_with_tokens(
                        &pool, &snapshot, &registry, &client, &own_id, tokens,
                    )
                    .await;
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

/// The `instance` filter of the report routes (`SPEC-3.17` item 10).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum InstanceFilter {
    All,
    Local,
    Peer(String),
}

impl InstanceFilter {
    /// Omitted, empty or `all` merges; `local` is this instance; anything else
    /// names a peer.
    pub fn from_param(raw: Option<&str>) -> Self {
        match raw.map(str::trim) {
            None | Some("") | Some("all") => InstanceFilter::All,
            Some("local") => InstanceFilter::Local,
            Some(name) => InstanceFilter::Peer(name.to_string()),
        }
    }

    /// The canonical query-param spelling.
    pub fn as_param(&self) -> &str {
        match self {
            InstanceFilter::All => "all",
            InstanceFilter::Local => "local",
            InstanceFilter::Peer(n) => n,
        }
    }
}

/// A peer whose figures may lack recent runs: it is `unknown`, or its summary
/// sync is not caught up (`SPEC-3.17` item 10).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PeerNote {
    pub name: String,
    pub state: PeerState,
    pub sync: SummarySync,
    /// The last successful summary sync, or `None` when there never was one.
    pub last_sync: Option<String>,
}

/// What a report route reads and what the page must say about it.
#[derive(Debug, Clone, PartialEq)]
pub struct ReportView {
    pub scope: crate::report::Scope,
    /// Whether peering is enabled, so the instance selector and notes show.
    pub peering: bool,
    pub instance: InstanceFilter,
    /// The configured peer names, for the instance selector.
    pub peer_names: Vec<String>,
    pub notes: Vec<PeerNote>,
}

/// Why a report view could not be resolved.
#[derive(Debug)]
pub enum ViewError {
    /// The `instance` value is not usable: a client error (`400`).
    BadInstance(String),
    /// The named peer has not been reached since this process started, so its
    /// origin id is unknown: `409`.
    PeerUnresolved(String),
    Store(Error),
}

impl std::fmt::Display for ViewError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ViewError::BadInstance(m) | ViewError::PeerUnresolved(m) => f.write_str(m),
            ViewError::Store(e) => write!(f, "{e}"),
        }
    }
}

impl From<Error> for ViewError {
    fn from(e: Error) -> Self {
        ViewError::Store(e)
    }
}

/// Resolve the `instance` parameter against the live peer config and registry
/// (`SPEC-3.17` item 10). With peering disabled the series read as before and
/// only `all` and `local` are accepted.
pub async fn resolve_report_view(
    pool: &DbPool,
    cfg: &PeersConfig,
    registry: &PeerRegistry,
    raw: Option<&str>,
    now: DateTime<Utc>,
) -> std::result::Result<ReportView, ViewError> {
    let instance = InstanceFilter::from_param(raw);
    if !cfg.enabled {
        if let InstanceFilter::Peer(n) = &instance {
            return Err(ViewError::BadInstance(format!(
                "instance `{n}` is not available: peering is not enabled"
            )));
        }
        return Ok(ReportView {
            scope: crate::report::Scope::Ledger,
            peering: false,
            instance,
            peer_names: Vec::new(),
            notes: Vec::new(),
        });
    }
    let peer_names: Vec<String> = cfg.peers.iter().map(|p| p.name.clone()).collect();
    let (scope, watched): (_, Vec<&str>) = match &instance {
        InstanceFilter::All => (
            crate::report::Scope::Summaries { instance_id: None },
            peer_names.iter().map(String::as_str).collect(),
        ),
        InstanceFilter::Local => (
            crate::report::Scope::Summaries {
                instance_id: Some(crate::settings::ensure_instance_id(pool).await?),
            },
            Vec::new(),
        ),
        InstanceFilter::Peer(n) => {
            if !peer_names.contains(n) {
                return Err(ViewError::BadInstance(format!(
                    "instance `{n}` is not `all`, `local` or a configured peer ({})",
                    peer_names.join(", ")
                )));
            }
            let id = registry.peer_instance_id(n).ok_or_else(|| {
                ViewError::PeerUnresolved(format!(
                    "peer `{n}` has not been reached since this instance started, so its rows cannot be selected yet"
                ))
            })?;
            (
                crate::report::Scope::Summaries {
                    instance_id: Some(id),
                },
                vec![n.as_str()],
            )
        }
    };
    let mut notes = Vec::new();
    for name in watched {
        let (state, sync) = registry.liveness_and_sync(name, cfg, now);
        if state == PeerState::Reachable && sync.is_caught_up() {
            continue;
        }
        let last_sync = match registry.peer_instance_id(name) {
            Some(id) => crate::state::peer_cursor_get(pool, &id)
                .await?
                .and_then(|c| c.last_success_at),
            None => None,
        };
        notes.push(PeerNote {
            name: name.to_string(),
            state,
            sync,
            last_sync,
        });
    }
    Ok(ReportView {
        scope,
        peering: true,
        instance,
        peer_names,
        notes,
    })
}

/// The body of `GET /api/peer/summaries` (`SPEC-3.17` item 4).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SummariesResponse {
    pub instance_id: String,
    pub rows: Vec<crate::state::RunSummary>,
    /// The cursor to pass as `after` next: the last row's `seq`, or the
    /// request's `after` when there are no rows.
    pub next: i64,
    /// True when rows beyond this page exist.
    pub more: bool,
}

/// Query of `GET /api/peer/summaries`.
#[derive(Debug, Deserialize)]
struct SummariesQuery {
    after: Option<i64>,
    limit: Option<i64>,
}

async fn peer_summaries(
    State(s): State<PeerApiState>,
    Query(q): Query<SummariesQuery>,
) -> Response {
    let batch = s.cfg.read().await.summary_batch.max(1) as i64;
    let after = q.after.unwrap_or(0);
    let limit = q.limit.unwrap_or(batch);
    if after < 0 || limit < 1 {
        return (
            StatusCode::BAD_REQUEST,
            "after must be >= 0 and limit must be >= 1",
        )
            .into_response();
    }
    let cap = limit.min(batch);
    let own_id = match crate::settings::ensure_instance_id(&s.pool).await {
        Ok(id) => id,
        Err(e) => {
            error!(error = %e, "peer summaries: reading the instance id failed");
            return (StatusCode::INTERNAL_SERVER_ERROR, "instance id unavailable").into_response();
        }
    };
    // One extra row tells `more` without a second query.
    let mut rows = match crate::state::run_summaries_since(&s.pool, &own_id, after, cap + 1).await {
        Ok(r) => r,
        Err(e) => {
            error!(error = %e, after, "peer summaries: reading the rows failed");
            return (StatusCode::INTERNAL_SERVER_ERROR, "summary read failed").into_response();
        }
    };
    let more = rows.len() as i64 > cap;
    rows.truncate(cap as usize);
    let next = rows.last().map_or(after, |r| r.seq);
    Json(SummariesResponse {
        instance_id: own_id,
        rows,
        next,
        more,
    })
    .into_response()
}

/// Check one pulled page before any of it is applied (`SPEC-3.17` item 9). The
/// whole batch is refused on the first bad row; the message names its
/// `run_uuid`.
pub fn validate_summaries(
    peer_id: &str,
    after: i64,
    limit: i64,
    resp: &SummariesResponse,
) -> std::result::Result<(), String> {
    if resp.instance_id != peer_id {
        return Err(format!(
            "response instance_id `{}` differs from the id `{peer_id}` the peer reported on its state pull",
            resp.instance_id
        ));
    }
    if resp.rows.len() as i64 > limit {
        return Err(format!(
            "{} rows returned for a limit of {limit}",
            resp.rows.len()
        ));
    }
    let mut prev = after;
    for r in &resp.rows {
        let bad = |why: String| Err(format!("row `{}`: {why}", r.run_uuid));
        if !is_uuid(&r.run_uuid) {
            return bad("run_uuid is not a UUID".to_string());
        }
        if r.instance_id != peer_id {
            return bad(format!(
                "instance_id `{}` is not the peer's own id (a peer never relays another's rows)",
                r.instance_id
            ));
        }
        if r.seq <= prev {
            return bad(format!("seq {} does not ascend past {prev}", r.seq));
        }
        prev = r.seq;
        if r.action_name.is_empty() || r.job_key.is_empty() {
            return bad("action_name and job_key must not be empty".to_string());
        }
        if r.attempt < 0 {
            return bad(format!("attempt {} is negative", r.attempt));
        }
        if crate::attempt_steps::Outcome::parse(&r.outcome).is_none() {
            return bad(format!("outcome `{}` is not a known outcome", r.outcome));
        }
        let started = match DateTime::parse_from_rfc3339(&r.started_at) {
            Ok(t) => t,
            Err(e) => return bad(format!("started_at `{}` does not parse: {e}", r.started_at)),
        };
        let ended = match DateTime::parse_from_rfc3339(&r.ended_at) {
            Ok(t) => t,
            Err(e) => return bad(format!("ended_at `{}` does not parse: {e}", r.ended_at)),
        };
        if ended < started {
            return bad("ended_at is before started_at".to_string());
        }
        if r.duration_ms < 0 {
            return bad(format!("duration_ms {} is negative", r.duration_ms));
        }
        if !r.cost_usd.is_finite() || r.cost_usd < 0.0 {
            return bad(format!(
                "cost_usd {} is not a finite non-negative number",
                r.cost_usd
            ));
        }
    }
    if resp.next != prev {
        return Err(format!(
            "next {} does not match the last row's seq {prev}",
            resp.next
        ));
    }
    if resp.more && resp.rows.is_empty() {
        return Err("more is true on an empty page".to_string());
    }
    Ok(())
}

/// The canonical 8-4-4-4-12 hex UUID text form.
fn is_uuid(s: &str) -> bool {
    s.len() == 36
        && s.bytes().enumerate().all(|(i, b)| match i {
            8 | 13 | 18 | 23 => b == b'-',
            _ => b.is_ascii_hexdigit(),
        })
}

/// One bounded `GET <url>/api/peer/summaries` pull.
pub async fn fetch_summaries(
    client: &reqwest::Client,
    peer: &PeerConfig,
    token: &str,
    after: i64,
    limit: i64,
    timeout: Duration,
) -> std::result::Result<SummariesResponse, PullError> {
    let url = format!("{}/api/peer/summaries", peer.url.trim_end_matches('/'));
    let resp = client
        .get(&url)
        .query(&[("after", after), ("limit", limit)])
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
    resp.json::<SummariesResponse>()
        .await
        .map_err(|e| PullError::Other(format!("{url} returned an undecodable body: {e}")))
}

/// Remember a failed sync on the peer's cursor row. A failure to do even that
/// is logged, never dropped.
async fn note_sync_error(pool: &DbPool, peer_id: &str, msg: &str) {
    if let Err(e) = crate::state::peer_cursor_record_error(pool, peer_id, msg).await {
        error!(peer_instance_id = %peer_id, error = %e, "recording a summary sync error on the peer cursor failed");
    }
}

/// Pull one peer's rows after its stored cursor until it has no more (or the
/// per-tick page cap), applying each page and the cursor advance in one
/// transaction (`SPEC-3.17` item 9).
async fn sync_peer(
    pool: &DbPool,
    client: &reqwest::Client,
    peer: &PeerConfig,
    token: &str,
    peer_id: &str,
    batch: i64,
    timeout: Duration,
) -> SummarySync {
    for _ in 0..SUMMARY_PAGES_PER_TICK {
        let after = match crate::state::peer_cursor_get(pool, peer_id).await {
            Ok(c) => c.map_or(0, |c| c.after_seq),
            Err(e) => return SummarySync::Failed(format!("reading the cursor failed: {e}")),
        };
        let resp = match fetch_summaries(client, peer, token, after, batch, timeout).await {
            Ok(r) => r,
            Err(PullError::Auth(st)) => {
                let msg = format!("peer answered {st}: token mismatch");
                note_sync_error(pool, peer_id, &msg).await;
                return SummarySync::Failed(msg);
            }
            Err(PullError::Other(msg)) => {
                note_sync_error(pool, peer_id, &msg).await;
                return SummarySync::Failed(msg);
            }
        };
        if let Err(msg) = validate_summaries(peer_id, after, batch, &resp) {
            note_sync_error(pool, peer_id, &msg).await;
            return SummarySync::Rejected(msg);
        }
        if let Err(e) =
            crate::state::run_summaries_apply(pool, peer_id, &resp.rows, resp.next).await
        {
            let msg = format!("applying the batch failed: {e}");
            note_sync_error(pool, peer_id, &msg).await;
            return SummarySync::Failed(msg);
        }
        if !resp.more {
            return SummarySync::CaughtUp;
        }
    }
    SummarySync::Behind
}

/// Sync every configured peer's summaries once, concurrently, and record each
/// outcome. A peer never reached has no known `instance_id`, so it stays
/// `Pending`. `tokens` is as for [`poll_with_tokens`].
pub async fn sync_summaries_with_tokens(
    pool: &DbPool,
    cfg: &PeersConfig,
    registry: &PeerRegistry,
    client: &reqwest::Client,
    own_id: &str,
    tokens: HashMap<String, std::result::Result<String, String>>,
) {
    let timeout = Duration::from_secs(cfg.poll_secs.min(PULL_TIMEOUT_MAX_SECS));
    let batch = cfg.summary_batch.max(1) as i64;
    let mut set = tokio::task::JoinSet::new();
    for p in &cfg.peers {
        let Some(peer_id) = registry.peer_instance_id(&p.name) else {
            continue;
        };
        if peer_id == own_id {
            continue;
        }
        let token = match tokens.get(&p.name) {
            Some(Ok(t)) => t.clone(),
            Some(Err(e)) => {
                registry.record_sync(
                    &p.name,
                    SummarySync::Failed(format!(
                        "token_ref `{}` could not be resolved: {e}",
                        p.token_ref
                    )),
                );
                continue;
            }
            None => {
                registry.record_sync(
                    &p.name,
                    SummarySync::Failed("no token was resolved".to_string()),
                );
                continue;
            }
        };
        let (pool, client, peer) = (pool.clone(), client.clone(), p.clone());
        set.spawn(async move {
            let out = sync_peer(&pool, &client, &peer, &token, &peer_id, batch, timeout).await;
            (peer.name, out)
        });
    }
    while let Some(joined) = set.join_next().await {
        match joined {
            Ok((name, out)) => registry.record_sync(&name, out),
            Err(e) => error!(error = %e, "a peer summary sync task panicked or was cancelled"),
        }
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
    /// Keys this instance already started, so a lower-load claim is refused.
    pub dispatched: DispatchedClaims,
}

/// `/api/peer/*`: TCP listener only. While peering is disabled every request is
/// a `404`; otherwise a bearer token is required even on a loopback bind, and a
/// session cookie is not accepted.
pub fn router(state: PeerApiState) -> Router {
    Router::new()
        .route("/api/peer/state", get(peer_state))
        .route("/api/peer/claim", post(peer_claim))
        .route("/api/peer/summaries", get(peer_summaries))
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
    if s.token.is_none() {
        match crate::state::api_key_any_exists(&s.pool).await {
            Ok(false) => {
                error!("peer API request refused: this instance has no resolved web auth token and no API key is configured; generate one in the Secrets page's API Keys section");
                return unauthorized();
            }
            Ok(true) => {}
            Err(e) => {
                error!(error = %e, "peer API request refused: checking for a configured API key failed");
                return (StatusCode::INTERNAL_SERVER_ERROR, "api key lookup failed")
                    .into_response();
            }
        }
    }
    let Some(presented) = bearer(request.headers()) else {
        return unauthorized();
    };
    if let Some(expected) = s.token.as_ref() {
        if constant_time_eq(presented.as_bytes(), expected.as_bytes()) {
            return next.run(request).await;
        }
    }
    match crate::state::api_key_touch_if_matches(&s.pool, &hash_api_key(&presented)).await {
        Ok(true) => next.run(request).await,
        Ok(false) => unauthorized(),
        Err(e) => {
            error!(error = %e, "peer API request refused: looking up the presented key failed");
            (StatusCode::INTERNAL_SERVER_ERROR, "api key lookup failed").into_response()
        }
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
/// (`SPEC-3.17` item 7). A row naming a peer is final: this instance granted
/// it, so that peer may already be working the event. An own row that has not
/// started yields to a lower `(load, id)` pair; one that has started refuses.
pub async fn respond_to_claim(
    pool: &DbPool,
    dispatched: &DispatchedClaims,
    own_id: &str,
    req: &ClaimRequest,
) -> Result<ClaimResponse> {
    let grant = ClaimResponse {
        granted: true,
        holder_id: req.claimant_id.clone(),
        holder_load: req.claimant_load,
    };
    // Our own claim step for this exact key may be mid-flight with no row
    // written yet. Granting the inbound claimant outright here would race
    // our own insert and, since a granted row is final, the loser of that
    // race would wrongly settle the tie instead of the lower `(load, id)`
    // pair. Weigh precedence now so the trivial grant below only fires when
    // there is truly no contender.
    if dispatched.is_pending(&req.event_key) {
        let own_load = crate::state::run_inflight_count(pool).await?;
        let own_pair = (own_load, own_id);
        let their_pair = (req.claimant_load, req.claimant_id.as_str());
        if own_pair < their_pair {
            return Ok(ClaimResponse {
                granted: false,
                holder_id: own_id.to_string(),
                holder_load: own_load,
            });
        }
    }
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
    if their_pair < own_pair {
        // The lock makes "has not started" and the handover one step: the
        // claim step marks a key started under the same lock before it
        // settles on Proceed.
        let started = dispatched.keys.lock().await;
        if started.iter().any(|k| k == &req.event_key) {
            info!(event_key = %req.event_key, claimant = %req.claimant_id, "peer claim refused: this instance already started the event");
            return Ok(refuse(row));
        }
        if crate::state::peer_claim_reassign(
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
    match respond_to_claim(&s.pool, &s.dispatched, &own_id, &req).await {
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

/// Most event keys [`DispatchedClaims`] remembers. A claim for a key older
/// than this many later events cannot still be in flight.
const DISPATCHED_KEYS_MAX: usize = 4096;

/// The event keys this instance has started, shared between the claim step
/// and the peer API so a started event refuses a lower-load claim
/// (`SPEC-3.17` item 7). Cheap to clone; in process only.
#[derive(Clone, Default)]
pub struct DispatchedClaims {
    keys: Arc<tokio::sync::Mutex<std::collections::VecDeque<String>>>,
    /// Event keys this instance's own claim step is currently negotiating,
    /// set before its first DB write so an inbound claim for the same key
    /// that reaches the brand-new-row grant first still yields to precedence
    /// instead of winning the race outright.
    pending: Arc<Mutex<HashSet<String>>>,
}

impl DispatchedClaims {
    async fn mark(&self, key: &str) {
        let mut keys = self.keys.lock().await;
        if keys.iter().any(|k| k == key) {
            return;
        }
        if keys.len() >= DISPATCHED_KEYS_MAX {
            keys.pop_front();
        }
        keys.push_back(key.to_string());
    }

    /// Record `key` as being negotiated by our own claim step until the
    /// returned guard drops (every return path of [`PeerClaims::claim`]).
    fn begin_pending(&self, key: &str) -> PendingGuard {
        self.pending.lock().unwrap().insert(key.to_string());
        PendingGuard {
            pending: self.pending.clone(),
            key: key.to_string(),
        }
    }

    fn is_pending(&self, key: &str) -> bool {
        self.pending.lock().unwrap().contains(key)
    }
}

/// Clears a [`DispatchedClaims::begin_pending`] entry when the claim step
/// that started it returns, on every path.
struct PendingGuard {
    pending: Arc<Mutex<HashSet<String>>>,
    key: String,
}

impl Drop for PendingGuard {
    fn drop(&mut self) {
        self.pending.lock().unwrap().remove(&self.key);
    }
}

/// The claim step of the webhook pipeline (`SPEC-3.17` item 7). Cheap to clone.
#[derive(Clone)]
pub struct PeerClaims {
    pool: DbPool,
    cfg: Arc<RwLock<PeersConfig>>,
    registry: PeerRegistry,
    client: reqwest::Client,
    dispatched: DispatchedClaims,
}

impl PeerClaims {
    pub fn new(pool: DbPool, cfg: Arc<RwLock<PeersConfig>>, registry: PeerRegistry) -> Self {
        Self {
            pool,
            cfg,
            registry,
            client: reqwest::Client::new(),
            dispatched: DispatchedClaims::default(),
        }
    }

    /// The handle the peer API uses to refuse a claim for an event this
    /// instance already started.
    pub fn dispatched(&self) -> DispatchedClaims {
        self.dispatched.clone()
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
        // Held until this call returns on any path: an inbound claim for the
        // same key that beats our own insert still has to weigh our (load,
        // id) pair instead of granting on an empty row (`contend_for_existing`
        // below is otherwise final, so that race would settle wrong).
        let _pending = self.dispatched.begin_pending(event_key);
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
            Ok(false) => {
                if let Some(decision) = self
                    .contend_for_existing(event_key, &own_id, source, delivery)
                    .await
                {
                    return decision;
                }
            }
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
        // Mark the event started before the last read of our row: a peer's
        // takeover either landed before this mark (the read sees it) or is
        // refused after it (`respond_to_claim` checks the mark under a lock).
        self.dispatched.mark(event_key).await;
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

    /// Our own local insert lost to a row already in the table, written by an
    /// inbound peer claim this instance granted. That peer may already be
    /// working the event, so the row is final whatever the loads: drop. A row
    /// that already names this instance (a repeated delivery) returns `None`
    /// so `claim` carries on asking peers; `Some` is the final decision.
    async fn contend_for_existing(
        &self,
        event_key: &str,
        own_id: &str,
        source: &str,
        delivery: &str,
    ) -> Option<ClaimDecision> {
        let row = match crate::state::peer_claim_get(&self.pool, event_key).await {
            Ok(Some(row)) => row,
            Ok(None) => return Some(self.drop_unknown(event_key, source, delivery)),
            Err(e) => {
                error!(source, delivery, event_key, error = %e, "reading the existing claim row failed");
                return Some(self.drop_unknown(event_key, source, delivery));
            }
        };
        if row.holder_id == own_id {
            return None;
        }
        info!(source, delivery, event_key, holder = %row.holder_id, "event already claimed; delivery dropped");
        Some(ClaimDecision::Drop {
            holder: row.holder_id,
        })
    }

    fn drop_unknown(&self, event_key: &str, source: &str, delivery: &str) -> ClaimDecision {
        info!(
            source,
            delivery,
            event_key,
            holder = "unknown",
            "event already claimed; delivery dropped"
        );
        ClaimDecision::Drop {
            holder: "unknown".to_string(),
        }
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
            claim_timeout_secs: 10,
            ..PeersConfig::default()
        }));
        let token = format!("tok-{name}");
        let registry = PeerRegistry::default();
        let claims = PeerClaims::new(pool.clone(), cfg.clone(), registry.clone());
        let app = router(PeerApiState {
            pool: pool.clone(),
            cfg: cfg.clone(),
            token: Some(Arc::new(token.clone())),
            dispatched: claims.dispatched(),
        });
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = format!("http://{}", listener.local_addr().unwrap());
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
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
            dispatched: DispatchedClaims::default(),
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
    async fn a_generated_api_key_authenticates_with_no_legacy_token() {
        let (r, pool) = api(true, None).await;
        let key = generate_api_key().unwrap();
        crate::state::api_key_create(&pool, "peer-b", &hash_api_key(&key))
            .await
            .unwrap();
        assert_eq!(
            r.clone()
                .oneshot(get_state(Some("unknown")))
                .await
                .unwrap()
                .status(),
            StatusCode::UNAUTHORIZED
        );
        let resp = r.oneshot(get_state(Some(&key))).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn a_revoked_api_key_stops_authenticating() {
        let (r, pool) = api(true, None).await;
        let key = generate_api_key().unwrap();
        crate::state::api_key_create(&pool, "peer-b", &hash_api_key(&key))
            .await
            .unwrap();
        assert_eq!(
            r.clone()
                .oneshot(get_state(Some(&key)))
                .await
                .unwrap()
                .status(),
            StatusCode::OK
        );
        assert!(crate::state::api_key_revoke(&pool, "peer-b").await.unwrap());
        let resp = r.oneshot(get_state(Some(&key))).await.unwrap();
        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn a_legacy_token_still_authenticates_alongside_generated_keys() {
        let (r, pool) = api(true, Some("tok")).await;
        let key = generate_api_key().unwrap();
        crate::state::api_key_create(&pool, "peer-b", &hash_api_key(&key))
            .await
            .unwrap();
        assert_eq!(
            r.clone()
                .oneshot(get_state(Some("tok")))
                .await
                .unwrap()
                .status(),
            StatusCode::OK
        );
        let resp = r.oneshot(get_state(Some(&key))).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
    }

    /// CLAUDE-636: the full flow the Secrets page's API Keys section exists
    /// for. B has accounts and no `auth_token_ref` (`token: None`); A has
    /// never generated a token for B either, so the only credential in play
    /// is the key B's operator generates and A's operator pastes into a
    /// secret named by `peers[].token_ref`, with no `config.toml` edit and no
    /// restart of either instance.
    #[tokio::test]
    async fn two_instances_peer_end_to_end_with_a_generated_api_key() {
        let _env = crate::secrets::key_file_env_lock().await;
        let tmp = tempfile::tempdir().unwrap();
        set_key_file(tmp.path()).await;

        // Instance B: peering enabled, no legacy token, one generated key.
        let pool_b = crate::db::test_pool().await;
        let b_id = crate::settings::ensure_instance_id(&pool_b).await.unwrap();
        let cfg_b = Arc::new(RwLock::new(PeersConfig {
            enabled: true,
            instance_name: "b".into(),
            ..PeersConfig::default()
        }));
        let app_b = router(PeerApiState {
            pool: pool_b.clone(),
            cfg: cfg_b,
            token: None,
            dispatched: DispatchedClaims::default(),
        });
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr_b = format!("http://{}", listener.local_addr().unwrap());
        tokio::spawn(async move { axum::serve(listener, app_b).await.unwrap() });

        let generated = generate_api_key().unwrap();
        crate::state::api_key_create(&pool_b, "a", &hash_api_key(&generated))
            .await
            .unwrap();

        // Instance A: the operator pastes B's generated key into the secret
        // named by `peers[].token_ref` for peer "b" (`b-token`, per `peer()`
        // above), exactly as the issue's acceptance criterion describes.
        let pool_a = crate::db::test_pool().await;
        let a_id = crate::settings::ensure_instance_id(&pool_a).await.unwrap();
        let key = crate::secrets::SecretsKey::load().unwrap();
        crate::state::secret_create(&pool_a, "b-token", &key.encrypt(&generated).unwrap())
            .await
            .unwrap();
        let cfg_a = cfg_for(vec![peer("b", &addr_b)]);
        let reg_a = PeerRegistry::default();
        let mut tokens = HashMap::new();
        tokens.insert("b".to_string(), Ok(generated.clone()));
        poll_with_tokens(&cfg_a, &reg_a, &reqwest::Client::new(), &a_id, tokens).await;

        let rows = reg_a.rows(&cfg_a, Utc::now());
        assert_eq!(rows[0].name, "b");
        assert_eq!(rows[0].state, PeerState::Reachable, "{rows:?}");
        assert_eq!(reg_a.peer_instance_id("b"), Some(b_id));

        // `last_used_at` on B's key row proves the presented bearer, not the
        // (absent) legacy token, is what authenticated the pull.
        let b_keys = crate::state::api_key_list(&pool_b).await.unwrap();
        assert!(b_keys[0].last_used_at.is_some());
    }

    /// CLAUDE-647: the daemon wraps the merged peer router in the web gate, which
    /// never reads `api_keys`. A generated key must still reach the peer gate,
    /// and must stay refused on the rest of `/api/*`.
    #[tokio::test]
    async fn a_generated_key_passes_the_web_gate_on_peer_routes_only() {
        let (peer_router, pool) = api(true, Some("legacy")).await;
        let key = generate_api_key().unwrap();
        crate::state::api_key_create(&pool, "k", &hash_api_key(&key))
            .await
            .unwrap();
        let app = crate::auth::protect(
            peer_router.route("/api/status", get(|| async { "ok" })),
            crate::auth::TokenGate::new("legacy".to_string()),
        );
        let call = |uri: &str, bearer: Option<&str>| {
            let mut b = Request::builder().uri(uri.to_string());
            if let Some(t) = bearer {
                b = b.header(header::AUTHORIZATION, format!("Bearer {t}"));
            }
            app.clone().oneshot(b.body(Body::empty()).unwrap())
        };

        let ok = call("/api/peer/state", Some(&key)).await.unwrap();
        assert_eq!(ok.status(), StatusCode::OK, "generated key on a peer route");
        let b_keys = crate::state::api_key_list(&pool).await.unwrap();
        assert!(b_keys[0].last_used_at.is_some(), "the key row was touched");

        let legacy = call("/api/peer/state", Some("legacy")).await.unwrap();
        assert_eq!(legacy.status(), StatusCode::OK, "legacy token");

        for bearer in [None, Some("wrong")] {
            let r = call("/api/peer/state", bearer).await.unwrap();
            assert_eq!(r.status(), StatusCode::UNAUTHORIZED, "{bearer:?}");
        }

        let other = call("/api/status", Some(&key)).await.unwrap();
        assert_eq!(
            other.status(),
            StatusCode::UNAUTHORIZED,
            "a generated key stays refused outside /api/peer/"
        );
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
        a.cfg.write().await.claim_timeout_secs = 1;
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
        let logs = LogCapture::start();
        let d = a
            .claims
            .claim(Some("k:o/r:comment:1"), "comment", "s", "d")
            .await;
        assert!(proceeds(&d));
        let row = crate::state::peer_claim_get(&b.pool, "k:o/r:comment:1")
            .await
            .unwrap();
        let row = row.unwrap_or_else(|| panic!("no claim row recorded: {}", logs.text()));
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
            respond_to_claim(&pool, &DispatchedClaims::default(), "me", &req(3, "x"))
                .await
                .unwrap()
                .granted
        );
        // The row names a third party: a different claimant is refused.
        let r = respond_to_claim(&pool, &DispatchedClaims::default(), "me", &req(0, "y"))
            .await
            .unwrap();
        assert!(!r.granted && r.holder_id == "x" && r.holder_load == 3);
        // The same claimant again is granted (idempotent).
        assert!(
            respond_to_claim(&pool, &DispatchedClaims::default(), "me", &req(3, "x"))
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
        let r = respond_to_claim(&pool, &DispatchedClaims::default(), "me", &own(3, "x"))
            .await
            .unwrap();
        assert!(!r.granted && r.holder_id == "me");
        assert!(
            respond_to_claim(&pool, &DispatchedClaims::default(), "me", &own(1, "x"))
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
    async fn a_started_event_refuses_a_lower_load_claimant() {
        let pool = crate::db::test_pool().await;
        let dispatched = DispatchedClaims::default();
        let req = |key: &str| ClaimRequest {
            event_key: key.into(),
            claimant_id: "x".into(),
            claimant_load: 1,
        };
        for (key, started) in [("started", true), ("pending", false)] {
            crate::state::peer_claim_insert(&pool, key, "me", 2)
                .await
                .unwrap();
            if started {
                dispatched.mark(key).await;
            }
            let r = respond_to_claim(&pool, &dispatched, "me", &req(key))
                .await
                .unwrap();
            assert_eq!(r.granted, !started, "{key}");
            let row = crate::state::peer_claim_get(&pool, key)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(row.holder_id, if started { "me" } else { "x" }, "{key}");
        }
    }

    #[tokio::test]
    async fn a_row_a_peer_already_holds_is_final_whatever_the_loads() {
        let _env = crate::secrets::key_file_env_lock().await;
        let tmp = tempfile::tempdir().unwrap();
        set_key_file(tmp.path()).await;
        let (a, b) = pair(0, 5).await;
        let key = "forgejo:o/r:action_run:900:failure";
        // `b` claims first and `a` grants; `a`'s own copy arrives later with
        // the lower load and must still drop.
        let req = ClaimRequest {
            event_key: key.into(),
            claimant_id: b.id.clone(),
            claimant_load: 5,
        };
        let r = respond_to_claim(&a.pool, &a.claims.dispatched(), &a.id, &req)
            .await
            .unwrap();
        assert!(r.granted);
        let d = a.claims.claim(Some(key), "failure", "src", "d-a").await;
        assert_eq!(
            d,
            ClaimDecision::Drop {
                holder: b.id.clone()
            }
        );
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

/// Summary replication and the merged Reports view (`SPEC-3.17` items 9 and 10).
#[cfg(test)]
mod summary_tests {
    use super::testkit::*;
    use super::*;
    use crate::report::{self, Range, Scope};
    use crate::state::{self, JobOutcome, RunSummary};
    use axum::body::Body;
    use axum::http::Request;
    use sqlx::Executor;
    use tower::ServiceExt;

    async fn outcome(pool: &DbPool, run: i64, key: &str, cost: f64) {
        state::job_outcome_insert(
            pool,
            &JobOutcome {
                run_id: run,
                action_name: "act",
                job_key: key,
                attempt: 1,
                ok: true,
                claude_ran: true,
                claude_ok: true,
                claude_cost_usd: cost,
                attempt_counted: true,
                summary: None,
                stream_log_path: None,
                duration_ms: 90_000,
                timed_out: false,
                stage: "initial",
                outcome: "success",
            },
        )
        .await
        .unwrap();
    }

    async fn outcomes(pool: &DbPool, n: usize, cost: f64) {
        let run = state::run_begin(pool, "act", "scheduler").await.unwrap();
        for i in 0..n {
            outcome(pool, run, &format!("K-{i}"), cost).await;
        }
    }

    async fn own_rows(pool: &DbPool) -> Vec<RunSummary> {
        let id = crate::settings::ensure_instance_id(pool).await.unwrap();
        state::run_summaries_since(pool, &id, 0, 10_000)
            .await
            .unwrap()
    }

    async fn count(pool: &DbPool) -> i64 {
        sqlx::query_scalar(sqlx::AssertSqlSafe(
            crate::db::q(pool, "SELECT COUNT(*) FROM run_summaries").into_owned(),
        ))
        .fetch_one(pool)
        .await
        .unwrap()
    }

    fn tokens_for(peer: &Node) -> HashMap<String, std::result::Result<String, String>> {
        HashMap::from([(peer.name.clone(), Ok(peer.token.clone()))])
    }

    async fn sync(from: &Node, peer: &Node) {
        let cfg = from.cfg.read().await.clone();
        sync_summaries_with_tokens(
            &from.pool,
            &cfg,
            &from.registry,
            &reqwest::Client::new(),
            &from.id,
            tokens_for(peer),
        )
        .await;
    }

    fn summary(peer_id: &str, seq: i64) -> RunSummary {
        RunSummary {
            run_uuid: new_uuid_v4().unwrap(),
            instance_id: peer_id.into(),
            seq,
            action_name: "act".into(),
            job_key: format!("P-{seq}"),
            attempt: 1,
            outcome: "success".into(),
            ok: true,
            started_at: "2026-01-01T00:00:00Z".into(),
            ended_at: "2026-01-01T00:01:00Z".into(),
            duration_ms: 60_000,
            cost_usd: 1.0,
            timed_out: false,
        }
    }

    #[tokio::test]
    async fn every_outcome_writes_exactly_one_summary_with_a_rising_seq() {
        let pool = crate::db::test_pool().await;
        outcomes(&pool, 4, 0.5).await;
        let rows = own_rows(&pool).await;
        assert_eq!(rows.len(), 4);
        assert_eq!(
            rows.iter().map(|r| r.seq).collect::<Vec<_>>(),
            vec![1, 2, 3, 4]
        );
        let outcomes_n: i64 = sqlx::query_scalar(sqlx::AssertSqlSafe(
            crate::db::q(&pool, "SELECT COUNT(*) FROM job_outcomes").into_owned(),
        ))
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(outcomes_n, 4);
        assert!(rows.iter().all(|r| is_uuid(&r.run_uuid)));
        let r = &rows[0];
        assert_eq!((r.action_name.as_str(), r.job_key.as_str()), ("act", "K-0"));
        assert_eq!(
            (r.outcome.as_str(), r.ok, r.duration_ms),
            ("success", true, 90_000)
        );
        assert_eq!(r.cost_usd, 0.5);
    }

    #[tokio::test]
    async fn the_backfill_covers_every_existing_outcome_in_time_order() {
        if std::env::var("CLAUDE_RUN_TEST_DATABASE_URL").is_ok() {
            eprintln!("SKIP: the backfill is replayed on the SQLite chain only");
            return;
        }
        sqlx::any::install_default_drivers();
        let pool = sqlx::any::AnyPoolOptions::new()
            .max_connections(1)
            .connect("sqlite::memory:")
            .await
            .unwrap();
        let dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("migrations/sqlite");
        let mut files: Vec<_> = std::fs::read_dir(&dir)
            .unwrap()
            .map(|e| e.unwrap().path())
            .filter(|p| p.extension().is_some_and(|x| x == "sql"))
            .collect();
        files.sort();
        let (before, after): (Vec<_>, Vec<_>) = files.into_iter().partition(|p| {
            !p.file_name()
                .unwrap()
                .to_str()
                .unwrap()
                .starts_with("0034_")
        });
        for f in &before {
            pool.execute(sqlx::AssertSqlSafe(std::fs::read_to_string(f).unwrap()))
                .await
                .unwrap_or_else(|e| panic!("{}: {e}", f.display()));
        }
        pool.execute(
            "INSERT INTO runs (id, action_name, started_at) VALUES (1, 'act', '2026-01-01T00:00:00Z')",
        )
        .await
        .unwrap();
        for (i, ts) in [
            "2026-01-03T10:00:30Z",
            "2026-01-01T10:00:00Z",
            "2026-01-02T10:00:00Z",
        ]
        .iter()
        .enumerate()
        {
            pool.execute(sqlx::AssertSqlSafe(format!(
                "INSERT INTO job_outcomes (run_id, action_name, job_key, attempt, ts, ok, claude_ran, \
                 claude_ok, claude_cost_usd, attempt_counted, duration_ms, timed_out, stage, outcome) \
                 VALUES (1, 'act', 'K-{i}', 1, '{ts}', 1, 1, 1, 0.25, 1, 90000, 0, 'initial', 'success')"
            )))
            .await
            .unwrap();
        }
        assert_eq!(after.len(), 1, "exactly one 0034 migration");
        pool.execute(sqlx::AssertSqlSafe(
            std::fs::read_to_string(&after[0]).unwrap(),
        ))
        .await
        .unwrap();

        let id = crate::settings::ensure_instance_id(&pool).await.unwrap();
        let rows = state::run_summaries_since(&pool, &id, 0, 100)
            .await
            .unwrap();
        assert_eq!(rows.len(), 3);
        // seq follows ts, not insertion order.
        assert_eq!(
            rows.iter().map(|r| r.job_key.as_str()).collect::<Vec<_>>(),
            vec!["K-1", "K-2", "K-0"]
        );
        assert_eq!(
            rows.iter().map(|r| r.seq).collect::<Vec<_>>(),
            vec![1, 2, 3]
        );
        let uuids: HashSet<_> = rows.iter().map(|r| r.run_uuid.as_str()).collect();
        assert_eq!(uuids.len(), 3);
        assert!(rows.iter().all(|r| is_uuid(&r.run_uuid)));
        assert_eq!(rows[2].ended_at, "2026-01-03T10:00:30Z");
        assert_eq!(rows[2].started_at, "2026-01-03T09:59:00Z");
        assert_eq!(rows[2].cost_usd, 0.25);
    }

    #[tokio::test]
    async fn a_joining_peer_gets_all_history_and_a_repull_inserts_nothing_twice() {
        let tmp = tempfile::tempdir().unwrap();
        set_key_file(tmp.path()).await;
        let (a, b) = pair(0, 0).await;
        outcomes(&b.pool, 5, 1.0).await;
        a.cfg.write().await.summary_batch = 2;
        b.cfg.write().await.summary_batch = 2;

        sync(&a, &b).await;
        let got = state::run_summaries_since(&a.pool, &b.id, 0, 100)
            .await
            .unwrap();
        assert_eq!(got.len(), 5);
        assert_eq!(got, own_rows(&b.pool).await);
        let cursor = state::peer_cursor_get(&a.pool, &b.id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(cursor.after_seq, 5);
        assert!(cursor.last_error.is_none() && cursor.last_success_at.is_some());
        let cfg = a.cfg.read().await.clone();
        assert!(a.registry.rows(&cfg, Utc::now())[0]
            .summary_sync
            .is_caught_up());

        // A second pull and a replayed batch both insert nothing.
        sync(&a, &b).await;
        assert_eq!(count(&a.pool).await, 5);
        assert_eq!(
            state::run_summaries_apply(&a.pool, &b.id, &got, 5)
                .await
                .unwrap(),
            0
        );
        assert_eq!(count(&a.pool).await, 5);
    }

    #[tokio::test]
    async fn a_peer_returning_after_an_outage_resumes_from_its_stored_cursor() {
        let tmp = tempfile::tempdir().unwrap();
        set_key_file(tmp.path()).await;
        let (a, b) = pair(0, 0).await;
        outcomes(&b.pool, 3, 1.0).await;
        sync(&a, &b).await;
        assert_eq!(count(&a.pool).await, 3);

        outcomes(&b.pool, 2, 1.0).await;
        let live_url = a.cfg.read().await.peers[0].url.clone();
        a.cfg.write().await.peers[0].url = "http://127.0.0.1:1".to_string();
        sync(&a, &b).await;
        let cursor = state::peer_cursor_get(&a.pool, &b.id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(cursor.after_seq, 3, "the outage left the cursor");
        assert!(cursor.last_error.is_some(), "the outage is recorded");
        let cfg = a.cfg.read().await.clone();
        assert!(matches!(
            a.registry.rows(&cfg, Utc::now())[0].summary_sync,
            SummarySync::Failed(_)
        ));

        a.cfg.write().await.peers[0].url = live_url;
        sync(&a, &b).await;
        assert_eq!(count(&a.pool).await, 5);
        let cursor = state::peer_cursor_get(&a.pool, &b.id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!((cursor.after_seq, cursor.last_error), (5, None));
    }

    #[tokio::test]
    async fn a_new_instance_id_for_a_peer_starts_at_cursor_zero() {
        let pool = crate::db::test_pool().await;
        state::run_summaries_apply(&pool, "old-id", &[], 9)
            .await
            .unwrap();
        assert!(state::peer_cursor_get(&pool, "new-id")
            .await
            .unwrap()
            .is_none());
    }

    /// A stub peer that serves one fixed page of summaries.
    async fn stub_peer(resp: SummariesResponse) -> String {
        let app = Router::new().route(
            "/api/peer/summaries",
            get(move || {
                let r = resp.clone();
                async move { Json(r) }
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = format!("http://{}", listener.local_addr().unwrap());
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        addr
    }

    #[tokio::test]
    async fn a_batch_with_an_invalid_row_is_rejected_whole_and_leaves_the_cursor() {
        let tmp = tempfile::tempdir().unwrap();
        set_key_file(tmp.path()).await;
        let a = node("a", 0).await;
        let peer_id = new_uuid_v4().unwrap();
        let good = summary(&peer_id, 1);
        let mut bad = summary(&peer_id, 2);
        bad.outcome = "bogus".into();
        let url = stub_peer(SummariesResponse {
            instance_id: peer_id.clone(),
            rows: vec![good, bad.clone()],
            next: 2,
            more: false,
        })
        .await;
        a.cfg.write().await.peers.push(PeerConfig {
            name: "p".into(),
            url,
            token_ref: "p-token".into(),
        });
        let now = Utc::now();
        a.registry.record(
            "p",
            &a.id,
            Ok(PeerStateDoc {
                instance_id: peer_id.clone(),
                instance_name: "p".into(),
                version: "t".into(),
                time: now.to_rfc3339(),
                load: 0,
                in_flight: vec![],
            }),
            now,
            120,
        );
        let cfg = a.cfg.read().await.clone();
        sync_summaries_with_tokens(
            &a.pool,
            &cfg,
            &a.registry,
            &reqwest::Client::new(),
            &a.id,
            HashMap::from([("p".to_string(), Ok("t".to_string()))]),
        )
        .await;

        assert_eq!(
            count(&a.pool).await,
            0,
            "the valid row is not applied either"
        );
        let cursor = state::peer_cursor_get(&a.pool, &peer_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(cursor.after_seq, 0);
        assert!(cursor.last_error.unwrap().contains(&bad.run_uuid));
        let row = &a.registry.rows(&cfg, Utc::now())[0];
        let SummarySync::Rejected(m) = &row.summary_sync else {
            panic!("expected Rejected, got {:?}", row.summary_sync);
        };
        assert!(m.contains(&bad.run_uuid), "{m}");
    }

    type Mutation = Box<dyn Fn(&mut RunSummary)>;

    #[test]
    fn validation_refuses_each_kind_of_bad_row() {
        let id = "peer";
        let ok = |rows: Vec<RunSummary>, next: i64, more: bool| SummariesResponse {
            instance_id: id.into(),
            rows,
            next,
            more,
        };
        assert!(validate_summaries(id, 0, 10, &ok(vec![summary(id, 1)], 1, false)).is_ok());
        let cases: Vec<(&str, Mutation)> = vec![
            ("not a UUID", Box::new(|r| r.run_uuid = "x".into())),
            (
                "not the peer's own id",
                Box::new(|r| r.instance_id = "other".into()),
            ),
            ("does not ascend", Box::new(|r| r.seq = 0)),
            ("empty", Box::new(|r| r.job_key.clear())),
            ("negative", Box::new(|r| r.duration_ms = -1)),
            (
                "does not parse",
                Box::new(|r| r.started_at = "yesterday".into()),
            ),
            (
                "before started_at",
                Box::new(|r| r.ended_at = "2025-01-01T00:00:00Z".into()),
            ),
            ("finite", Box::new(|r| r.cost_usd = f64::NAN)),
        ];
        for (needle, mutate) in cases {
            let mut r = summary(id, 1);
            mutate(&mut r);
            let seq = r.seq;
            let err = validate_summaries(id, 0, 10, &ok(vec![r], seq, false)).unwrap_err();
            assert!(err.contains(needle), "{needle}: {err}");
        }
        assert!(validate_summaries(id, 0, 10, &ok(vec![summary(id, 1)], 7, false)).is_err());
        assert!(validate_summaries(id, 0, 10, &ok(vec![], 0, true)).is_err());
        assert!(validate_summaries(id, 0, 0, &ok(vec![summary(id, 1)], 1, false)).is_err());
        let mut wrong = ok(vec![], 0, false);
        wrong.instance_id = "other".into();
        assert!(validate_summaries(id, 0, 10, &wrong).is_err());
    }

    async fn get_summaries(app: Router, query: &str, token: &str) -> (StatusCode, Vec<u8>) {
        let resp = app
            .oneshot(
                Request::builder()
                    .uri(format!("/api/peer/summaries{query}"))
                    .header(header::AUTHORIZATION, format!("Bearer {token}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let status = resp.status();
        (
            status,
            axum::body::to_bytes(resp.into_body(), 1 << 22)
                .await
                .unwrap()
                .to_vec(),
        )
    }

    #[tokio::test]
    async fn the_endpoint_serves_only_own_rows_capped_by_the_batch_setting() {
        let pool = crate::db::test_pool().await;
        let own = crate::settings::ensure_instance_id(&pool).await.unwrap();
        outcomes(&pool, 5, 1.0).await;
        state::run_summaries_apply(&pool, "peer-x", &[summary("peer-x", 1)], 1)
            .await
            .unwrap();
        let mut cfg = PeersConfig {
            enabled: true,
            instance_name: "me".into(),
            ..PeersConfig::default()
        };
        cfg.summary_batch = 2;
        let app = router(PeerApiState {
            pool: pool.clone(),
            cfg: Arc::new(RwLock::new(cfg)),
            token: Some(Arc::new("tok".to_string())),
            dispatched: DispatchedClaims::default(),
        });
        let (st, body) = get_summaries(app.clone(), "?after=0&limit=100", "tok").await;
        assert_eq!(st, StatusCode::OK);
        let r: SummariesResponse = serde_json::from_slice(&body).unwrap();
        assert_eq!(r.instance_id, own);
        assert_eq!(r.rows.iter().map(|x| x.seq).collect::<Vec<_>>(), vec![1, 2]);
        assert!(r.rows.iter().all(|x| x.instance_id == own));
        assert_eq!((r.next, r.more), (2, true));
        let (_, body) = get_summaries(app.clone(), "?after=4&limit=1", "tok").await;
        let r: SummariesResponse = serde_json::from_slice(&body).unwrap();
        assert_eq!((r.rows.len(), r.next, r.more), (1, 5, false));
        let (_, body) = get_summaries(app.clone(), "?after=5", "tok").await;
        let r: SummariesResponse = serde_json::from_slice(&body).unwrap();
        assert_eq!((r.rows.len(), r.next, r.more), (0, 5, false));
        assert_eq!(
            get_summaries(app.clone(), "?limit=0", "tok").await.0,
            StatusCode::BAD_REQUEST
        );
        assert_eq!(
            get_summaries(app, "?after=0", "wrong").await.0,
            StatusCode::UNAUTHORIZED
        );
    }

    #[tokio::test]
    async fn merged_totals_are_the_sum_and_local_is_this_instances_rows() {
        let pool = crate::db::test_pool().await;
        let own = crate::settings::ensure_instance_id(&pool).await.unwrap();
        outcomes(&pool, 2, 1.5).await;
        let now = Utc::now();
        let peer_rows: Vec<RunSummary> = (1..=3)
            .map(|i| RunSummary {
                ended_at: now.to_rfc3339_opts(chrono::SecondsFormat::Secs, true),
                started_at: (now - chrono::Duration::minutes(10))
                    .to_rfc3339_opts(chrono::SecondsFormat::Secs, true),
                duration_ms: 600_000,
                cost_usd: 4.0,
                ..summary("peer-1", i)
            })
            .collect();
        state::run_summaries_apply(&pool, "peer-1", &peer_rows, 3)
            .await
            .unwrap();

        let all = Scope::Summaries { instance_id: None };
        let local = Scope::Summaries {
            instance_id: Some(own),
        };
        let peer = Scope::Summaries {
            instance_id: Some("peer-1".into()),
        };
        let sum_all = report::summary_scoped(&pool, Range::All, None, &all)
            .await
            .unwrap();
        let sum_local = report::summary_scoped(&pool, Range::All, None, &local)
            .await
            .unwrap();
        let sum_peer = report::summary_scoped(&pool, Range::All, None, &peer)
            .await
            .unwrap();
        assert_eq!((sum_local.total_runs, sum_local.total_cost_usd), (2, 3.0));
        assert_eq!((sum_peer.total_runs, sum_peer.total_cost_usd), (3, 12.0));
        assert_eq!(
            sum_all.total_runs,
            sum_local.total_runs + sum_peer.total_runs
        );
        assert_eq!(
            sum_all.total_cost_usd,
            sum_local.total_cost_usd + sum_peer.total_cost_usd
        );
        assert_eq!(
            sum_all.total_run_minutes,
            sum_local.total_run_minutes + sum_peer.total_run_minutes
        );
        let daily_all = report::daily_scoped(&pool, Range::All, None, &all)
            .await
            .unwrap();
        assert_eq!(daily_all.iter().map(|d| d.run_count).sum::<i64>(), 5);
        let hours = report::by_hour_scoped(&pool, Range::All, None, &all)
            .await
            .unwrap();
        assert_eq!(hours.iter().map(|h| h.job_count).sum::<i64>(), 5);
        assert_eq!(hours.iter().map(|h| h.run_starts).sum::<i64>(), 5);
        let days = report::by_weekday_scoped(&pool, Range::All, None, &all)
            .await
            .unwrap();
        assert_eq!(days.iter().map(|d| d.run_count).sum::<i64>(), 5);
        let weeks = report::weekly_scoped(&pool, Range::All, None, &all)
            .await
            .unwrap();
        assert_eq!(weeks.iter().map(|w| w.cost_usd).sum::<f64>(), 15.0);
        let none = report::summary_scoped(&pool, Range::All, Some("other"), &all)
            .await
            .unwrap();
        assert_eq!(none.total_runs, 0);
    }

    #[tokio::test]
    async fn without_peer_data_the_summary_totals_equal_the_ledger_totals() {
        let pool = crate::db::test_pool().await;
        let run = state::run_begin(&pool, "act", "scheduler").await.unwrap();
        for (i, cost) in [0.25, 0.5, 1.0].into_iter().enumerate() {
            outcome(&pool, run, &format!("K-{i}"), cost).await;
            // What `job.rs` writes next to every outcome whose claude ran.
            state::daily_cost_add(&pool, "act", cost).await.unwrap();
            state::daily_count_increment(&pool, "act").await.unwrap();
        }
        let all = Scope::Summaries { instance_id: None };
        let legacy = report::summary_scoped(&pool, Range::All, None, &Scope::Ledger)
            .await
            .unwrap();
        let merged = report::summary_scoped(&pool, Range::All, None, &all)
            .await
            .unwrap();
        assert_eq!(merged.total_runs, legacy.total_runs);
        assert_eq!(merged.total_cost_usd, legacy.total_cost_usd);
        let legacy_daily = report::daily_scoped(&pool, Range::All, None, &Scope::Ledger)
            .await
            .unwrap();
        let merged_daily = report::daily_scoped(&pool, Range::All, None, &all)
            .await
            .unwrap();
        assert_eq!(merged_daily.len(), legacy_daily.len());
        for (m, l) in merged_daily.iter().zip(&legacy_daily) {
            assert_eq!(
                (&m.date, m.run_count, m.cost_usd),
                (&l.date, l.run_count, l.cost_usd)
            );
        }
    }

    fn two_peer_cfg(enabled: bool) -> PeersConfig {
        PeersConfig {
            enabled,
            instance_name: "me".into(),
            peers: vec![
                PeerConfig {
                    name: "east".into(),
                    url: "http://127.0.0.1:1".into(),
                    token_ref: "t".into(),
                },
                PeerConfig {
                    name: "west".into(),
                    url: "http://127.0.0.1:1".into(),
                    token_ref: "t".into(),
                },
            ],
            ..PeersConfig::default()
        }
    }

    fn state_doc(id: &str, now: DateTime<Utc>) -> PeerStateDoc {
        PeerStateDoc {
            instance_id: id.into(),
            instance_name: "x".into(),
            version: "t".into(),
            time: now.to_rfc3339(),
            load: 0,
            in_flight: vec![],
        }
    }

    #[tokio::test]
    async fn the_view_names_a_stale_or_unknown_peer_with_its_last_sync() {
        let pool = crate::db::test_pool().await;
        let own = crate::settings::ensure_instance_id(&pool).await.unwrap();
        let cfg = two_peer_cfg(true);
        let reg = PeerRegistry::default();
        let now = Utc::now();
        // east: reachable and caught up. west: reached once, long ago, with a stored cursor.
        reg.record("east", &own, Ok(state_doc("east-id", now)), now, 120);
        reg.record_sync("east", SummarySync::CaughtUp);
        let long_ago = now - chrono::Duration::hours(2);
        reg.record(
            "west",
            &own,
            Ok(state_doc("west-id", long_ago)),
            long_ago,
            120,
        );
        state::run_summaries_apply(&pool, "west-id", &[], 4)
            .await
            .unwrap();

        let v = resolve_report_view(&pool, &cfg, &reg, None, now)
            .await
            .unwrap();
        assert_eq!(v.scope, Scope::Summaries { instance_id: None });
        assert_eq!(v.notes.len(), 1);
        assert_eq!(v.notes[0].name, "west");
        assert_eq!(v.notes[0].state, PeerState::Unknown);
        assert!(v.notes[0].last_sync.is_some());

        let v = resolve_report_view(&pool, &cfg, &reg, Some("east"), now)
            .await
            .unwrap();
        assert_eq!(
            v.scope,
            Scope::Summaries {
                instance_id: Some("east-id".into())
            }
        );
        assert!(v.notes.is_empty());
        let v = resolve_report_view(&pool, &cfg, &reg, Some("west"), now)
            .await
            .unwrap();
        assert_eq!(v.notes.len(), 1);
        let v = resolve_report_view(&pool, &cfg, &reg, Some("local"), now)
            .await
            .unwrap();
        assert_eq!(
            v.scope,
            Scope::Summaries {
                instance_id: Some(own)
            }
        );
        assert!(v.notes.is_empty());

        assert!(matches!(
            resolve_report_view(&pool, &cfg, &reg, Some("nope"), now).await,
            Err(ViewError::BadInstance(_))
        ));
        let fresh = PeerRegistry::default();
        assert!(matches!(
            resolve_report_view(&pool, &cfg, &fresh, Some("east"), now).await,
            Err(ViewError::PeerUnresolved(_))
        ));
    }

    #[tokio::test]
    async fn with_peering_disabled_the_series_read_the_ledger() {
        let pool = crate::db::test_pool().await;
        let reg = PeerRegistry::default();
        let cfg = two_peer_cfg(false);
        let v = resolve_report_view(&pool, &cfg, &reg, None, Utc::now())
            .await
            .unwrap();
        assert_eq!(
            (v.scope, v.peering, v.notes.is_empty()),
            (Scope::Ledger, false, true)
        );
        assert!(matches!(
            resolve_report_view(&pool, &cfg, &reg, Some("east"), Utc::now()).await,
            Err(ViewError::BadInstance(_))
        ));
    }

    #[tokio::test]
    async fn the_poller_makes_no_summary_call_while_peering_is_disabled() {
        let hits = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let h = hits.clone();
        let app = Router::new().fallback(move || {
            let h = h.clone();
            async move {
                h.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                StatusCode::NOT_FOUND
            }
        });
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let pool = crate::db::test_pool().await;
        let mut cfg = two_peer_cfg(false);
        cfg.peers.truncate(1);
        cfg.peers[0].url = url;
        let task = tokio::spawn(poll_loop(
            pool,
            Arc::new(RwLock::new(cfg)),
            PeerRegistry::default(),
        ));
        tokio::time::sleep(Duration::from_millis(400)).await;
        task.abort();
        assert_eq!(hits.load(std::sync::atomic::Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn retention_prunes_summaries_by_ended_at_and_keeps_each_origins_newest() {
        let pool = crate::db::test_pool().await;
        let old: Vec<RunSummary> = (1..=3).map(|i| summary("peer-1", i)).collect();
        state::run_summaries_apply(&pool, "peer-1", &old, 3)
            .await
            .unwrap();
        let mut fresh = summary("peer-2", 1);
        fresh.ended_at = "2999-01-01T00:00:00Z".into();
        fresh.started_at = "2999-01-01T00:00:00Z".into();
        state::run_summaries_apply(&pool, "peer-2", &[fresh], 1)
            .await
            .unwrap();

        let deleted = crate::retention::prune_run_summaries(&pool, "2026-06-01T00:00:00Z")
            .await
            .unwrap();
        assert_eq!(
            deleted, 2,
            "seq 1 and 2 of peer-1 go; its newest stays as the seq anchor"
        );
        let left = state::run_summaries_since(&pool, "peer-1", 0, 10)
            .await
            .unwrap();
        assert_eq!(left.iter().map(|r| r.seq).collect::<Vec<_>>(), vec![3]);
        assert_eq!(count(&pool).await, 2);
    }
}
