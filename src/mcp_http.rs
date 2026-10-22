//! Streamable-HTTP MCP listener (CLAUDE-442).
//!
//! Serves the same seven read-only tools as `claude-run mcp stdio`
//! (`src/mcp.rs`, CLAUDE-437) over `/mcp`, behind the `mcp-web` crate's
//! local-account + OAuth 2.1 bearer layer, so a remote client can reach them
//! without `docker compose exec`. `McpServer` (`src/mcp.rs`) is already
//! transport-agnostic: this module only wraps it in an `rmcp`
//! `StreamableHttpService` and merges that behind `mcp_web::require_bearer`
//! into the router `mcp_web::build_router` assembles.
//!
//! Off by default (`[mcp].enabled = false`): nothing here runs, and neither
//! key file nor `mcp-web.db` is ever created, until an operator opts in. Once
//! enabled, a missing `public_url` or a corrupted key file fails this
//! listener alone with an `error` log naming the problem; the rest of the
//! daemon keeps running, the same non-fatal shape as `secrets::ensure_key`.

use std::collections::HashSet;
use std::io::Write as _;
use std::os::unix::fs::OpenOptionsExt;
use std::path::Path;
use std::sync::Arc;

use anyhow::Context as _;
use axum::middleware::from_fn_with_state;
use axum::Router;
use base64::engine::general_purpose::STANDARD as B64_STANDARD;
use base64::Engine as _;
use rmcp::transport::streamable_http_server::session::local::LocalSessionManager;
use rmcp::transport::{StreamableHttpServerConfig, StreamableHttpService};

use crate::config as cfg;
use crate::db::DbPool;
use crate::error::{Error, Result};
use crate::mcp::McpServer;

/// File names under `Paths::config_dir`. Config, like `secrets.key`: an
/// operator's own backup/restore tooling targets the config dir, not the
/// data dir that workdirs and `state.db` live under.
const MASTER_KEY_FILE: &str = "mcp-master.key";
const SESSION_KEY_FILE: &str = "mcp-session.key";
/// File name under `Paths::data_dir`, beside `state.db`: generated state,
/// not configuration, so it is never backed up or restored like the keys.
const DB_FILE: &str = "mcp-web.db";

/// There is no per-account upstream instance here: every tool reads the one
/// shared `claude-run` state store regardless of which account called it, so
/// every account is "connected".
struct AlwaysConnected;

#[async_trait::async_trait]
impl mcp_web::UpstreamDirectory for AlwaysConnected {
    async fn is_connected(
        &self,
        _pool: &sqlx::SqlitePool,
        _account_id: i64,
    ) -> anyhow::Result<bool> {
        Ok(true)
    }

    async fn connected_account_ids(&self, pool: &sqlx::SqlitePool) -> anyhow::Result<HashSet<i64>> {
        let ids: Vec<i64> = sqlx::query_scalar("SELECT id FROM accounts")
            .fetch_all(pool)
            .await?;
        Ok(ids.into_iter().collect())
    }

    fn settings_path(&self) -> &str {
        "/"
    }

    fn not_configured_message(&self, _public_url: &str) -> String {
        // Unreachable: `is_connected` always returns true, so the generic
        // account/admin pages never ask for this message.
        "every account is connected".to_string()
    }
}

/// Pull the base64 key value out of a file written by [`generate_key_file`]:
/// blank lines and `#` comments are skipped, the key is the first line left.
/// A present file that yields nothing readable is a hard error naming the
/// path; never a silent regeneration.
fn read_key_file(path: &Path) -> Result<String> {
    let raw = std::fs::read_to_string(path)?;
    raw.lines()
        .map(str::trim)
        .find(|line| !line.is_empty() && !line.starts_with('#'))
        .map(str::to_string)
        .ok_or_else(|| Error::Other(anyhow::anyhow!("{} has no key line", path.display())))
}

/// Create a new base64-encoded 32-byte key at `path`, mode `0600`. Fails
/// rather than overwrites when the file already exists, so a restart racing
/// a first-boot generation cannot clobber a key mid-flight.
fn generate_key_file(path: &Path, label: &str) -> Result<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let mut bytes = [0u8; 32];
    getrandom::getrandom(&mut bytes).map_err(|e| {
        Error::Other(anyhow::anyhow!(
            "reading random bytes for the {label} key: {e}"
        ))
    })?;
    let encoded = B64_STANDARD.encode(bytes);
    let mut f = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(path)?;
    writeln!(
        f,
        "# created by claude-run for the MCP HTTP listener's {label} key."
    )
    .and_then(|()| {
        writeln!(
            f,
            "# back this up: every session and encrypted value under mcp-web.db is"
        )
    })
    .and_then(|()| {
        writeln!(
            f,
            "# unrecoverable without it, and a replacement orphans them all."
        )
    })
    .and_then(|()| writeln!(f, "{encoded}"))?;
    Ok(())
}

/// Load the key at `path`, generating one on first start. Unlike
/// `secrets::ensure_key`, there is no pre-existing store to check for
/// emptiness: `mcp-web.db` and this key are born together, so "the file is
/// missing" always means "first start", never "key lost, data orphaned".
fn ensure_key_file(path: &Path, label: &str) -> Result<String> {
    if !path.exists() {
        generate_key_file(path, label)?;
        tracing::info!(
            path = %path.display(),
            %label,
            "generated a new MCP key; back it up"
        );
    }
    read_key_file(path)
}

/// Resolve both MCP key files under `config_dir`, generating either on first
/// start. A present-but-corrupt file (not valid base64, wrong length) fails
/// loudly naming the file: never silently regenerated, because a fresh key
/// would invalidate every live session and orphan every encrypted value
/// under `mcp-web.db`.
fn ensure_mcp_keys(config_dir: &Path) -> Result<(String, String)> {
    let master = ensure_key_file(&config_dir.join(MASTER_KEY_FILE), "master")?;
    let session = ensure_key_file(&config_dir.join(SESSION_KEY_FILE), "session")?;
    Ok((master, session))
}

/// Assemble the generic `AppState` plus the full router (generic account /
/// admin / OAuth surface merged with the bearer-gated `/mcp` surface), or
/// fail naming the first unmet precondition: an unset `public_url`, a
/// corrupt key file, or an unopenable `mcp-web.db`.
async fn build(
    mcp_cfg: &cfg::McpConfig,
    paths: &cfg::Paths,
    pool: DbPool,
) -> Result<(Arc<mcp_web::AppState>, Router)> {
    let public_url = mcp_cfg.public_url.clone().ok_or_else(|| {
        Error::Other(anyhow::anyhow!(
            "[mcp].public_url is required when [mcp].enabled = true"
        ))
    })?;

    let (master_b64, session_b64) = ensure_mcp_keys(&paths.config_dir)?;
    let master_key = mcp_web::crypto::decode_master_key(&master_b64)
        .with_context(|| format!("{}", paths.config_dir.join(MASTER_KEY_FILE).display()))?;
    let session_key = mcp_web::web::session::decode_session_key(&session_b64)
        .with_context(|| format!("{}", paths.config_dir.join(SESSION_KEY_FILE).display()))?;
    let crypto = mcp_web::crypto::Crypto::new(&master_key);
    let signer = mcp_web::web::session::SessionSigner::new(session_key);

    let db_path = paths.data_dir.join(DB_FILE);
    if let Some(parent) = db_path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let database_url = format!("sqlite://{}?mode=rwc", db_path.display());
    let mcp_pool = mcp_web::db::open(&database_url)
        .await
        .with_context(|| format!("opening {}", db_path.display()))?;

    let http = mcp_web::HttpConfig::new(
        mcp_cfg.bind.clone(),
        database_url,
        public_url.clone(),
        mcp_cfg.admin_email.clone(),
    );
    let state = Arc::new(mcp_web::AppState::new(
        mcp_pool,
        crypto,
        signer,
        http,
        Arc::new(AlwaysConnected),
    ));

    // The public URL's own authority is always allowed, since every OAuth
    // and resource-metadata URL anchors on it; `[mcp].allowed_hosts` adds any
    // extra `Host` value a reverse proxy or alternate name needs.
    let mut allowed_hosts = mcp_cfg.allowed_hosts.clone();
    if let Some(authority) = mcp_web::web::public_authority(&public_url) {
        allowed_hosts.push(authority);
    }

    let session_manager = Arc::new(LocalSessionManager::default());
    let factory_pool = pool.clone();
    let factory_paths = paths.clone();
    let service = StreamableHttpService::new(
        move || Ok(McpServer::new(factory_pool.clone(), factory_paths.clone())),
        session_manager,
        StreamableHttpServerConfig::default().with_allowed_hosts(allowed_hosts),
    );
    let mcp_router = Router::new()
        .nest_service("/mcp", service)
        .layer(from_fn_with_state(state.clone(), mcp_web::require_bearer));

    let router = mcp_web::build_router(state.clone(), mcp_router, Router::new());
    Ok((state, router))
}

/// Spawn the listener as its own task when `[mcp].enabled` is true, else do
/// nothing. A misconfigured or unbuildable listener logs at `error` naming
/// the problem and returns `None`; the rest of the daemon is unaffected.
/// Runs until the returned handle is aborted; there is no graceful-shutdown
/// path here, matching every other background task in `daemon.rs`.
pub async fn spawn(
    mcp_cfg: &cfg::McpConfig,
    paths: cfg::Paths,
    pool: DbPool,
) -> Option<tokio::task::JoinHandle<()>> {
    if !mcp_cfg.enabled {
        return None;
    }
    match build(mcp_cfg, &paths, pool).await {
        Ok((state, router)) => Some(tokio::spawn(async move {
            if let Err(e) = mcp_web::serve(state, router, std::future::pending()).await {
                tracing::error!(error = %e, "MCP HTTP listener exited");
            }
        })),
        Err(e) => {
            tracing::error!(error = %e, "MCP HTTP listener disabled this run");
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::fs::PermissionsExt as _;

    fn test_paths(dir: &std::path::Path) -> cfg::Paths {
        cfg::Paths {
            config_dir: dir.join("config"),
            data_dir: dir.join("data"),
            state_dir: dir.join("state"),
            runtime_dir: dir.join("runtime"),
        }
    }

    #[test]
    fn key_file_generated_once_and_reused() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("mcp-master.key");
        let first = ensure_key_file(&path, "master").unwrap();
        assert_eq!(
            std::fs::metadata(&path).unwrap().permissions().mode() & 0o777,
            0o600
        );
        let second = ensure_key_file(&path, "master").unwrap();
        assert_eq!(first, second, "a restart must not generate a new key");
        // The generated value round-trips through the crate's own decoder.
        mcp_web::crypto::decode_master_key(&first).unwrap();
    }

    #[test]
    fn corrupt_key_file_fails_visibly_instead_of_regenerating() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("mcp-session.key");
        std::fs::write(&path, "not valid base64 at all\n").unwrap();
        let err = ensure_key_file(&path, "session")
            .map(|b64| mcp_web::web::session::decode_session_key(&b64))
            .and_then(|r| r.map_err(Error::Other))
            .unwrap_err();
        assert!(err.to_string().contains("base64") || err.to_string().contains("decode"));
    }

    #[tokio::test]
    async fn disabled_by_default_spawns_nothing() {
        let dir = tempfile::tempdir().unwrap();
        let paths = test_paths(dir.path());
        let pool = crate::db::test_pool().await;
        let handle = spawn(&cfg::McpConfig::default(), paths.clone(), pool).await;
        assert!(handle.is_none());
        assert!(!paths.config_dir.join(MASTER_KEY_FILE).exists());
        assert!(!paths.config_dir.join(SESSION_KEY_FILE).exists());
        assert!(!paths.data_dir.join(DB_FILE).exists());
    }

    #[tokio::test]
    async fn enabled_with_no_public_url_fails_without_creating_files() {
        let dir = tempfile::tempdir().unwrap();
        let paths = test_paths(dir.path());
        let pool = crate::db::test_pool().await;
        let mcp_cfg = cfg::McpConfig {
            enabled: true,
            ..cfg::McpConfig::default()
        };
        let handle = spawn(&mcp_cfg, paths.clone(), pool).await;
        assert!(handle.is_none());
    }

    /// A built router for the HTTP-path tests below, against
    /// [`crate::db::test_pool`] so these run on whichever backend the test
    /// run is pinned to (SQLite or, with `CLAUDE_RUN_TEST_DATABASE_URL` set,
    /// PostgreSQL), exactly like every other `DbPool`-backed test here.
    async fn built_router(dir: &std::path::Path) -> (std::sync::Arc<mcp_web::AppState>, Router) {
        let paths = test_paths(dir);
        let pool = crate::db::test_pool().await;
        let mcp_cfg = cfg::McpConfig {
            enabled: true,
            public_url: Some("https://mcp.example.com".to_string()),
            // The authenticated-client test below talks to a real loopback
            // listener, whose `Host` header is `127.0.0.1:<port>`, not the
            // `public_url` authority `build` already allows.
            allowed_hosts: vec!["127.0.0.1".to_string()],
            ..cfg::McpConfig::default()
        };
        build(&mcp_cfg, &paths, pool).await.unwrap()
    }

    /// Register the OAuth client an issued test token is attributed to.
    /// `access_tokens.client_id` has a foreign key into `oauth_clients`, so a
    /// token minted straight into the table (bypassing the real
    /// dynamic-registration + authorize + token-exchange dance) needs this
    /// row to exist first.
    async fn register_test_client(pool: &sqlx::SqlitePool) {
        mcp_web::db::insert_oauth_client(
            pool,
            "test-client",
            None,
            "[]",
            Some("test client"),
            None,
        )
        .await
        .unwrap();
    }

    fn mcp_post_request(body: &str, token: Option<&str>) -> axum::http::Request<axum::body::Body> {
        let mut builder = axum::http::Request::post("/mcp")
            .header(axum::http::header::CONTENT_TYPE, "application/json")
            .header(
                axum::http::header::ACCEPT,
                "application/json, text/event-stream",
            );
        if let Some(token) = token {
            builder = builder.header(axum::http::header::AUTHORIZATION, format!("Bearer {token}"));
        }
        builder
            .body(axum::body::Body::from(body.to_string()))
            .unwrap()
    }

    #[tokio::test]
    async fn unauthenticated_post_mcp_is_rejected_with_a_discovery_challenge() {
        use tower::ServiceExt;

        let dir = tempfile::tempdir().unwrap();
        let (_state, router) = built_router(dir.path()).await;

        let response = router
            .clone()
            .oneshot(mcp_post_request("{}", None))
            .await
            .unwrap();
        assert_eq!(response.status(), axum::http::StatusCode::UNAUTHORIZED);
        let challenge = response
            .headers()
            .get(axum::http::header::WWW_AUTHENTICATE)
            .expect("WWW-Authenticate header")
            .to_str()
            .unwrap()
            .to_string();
        assert!(challenge.contains(".well-known/oauth-protected-resource"));

        for path in [
            "/.well-known/oauth-protected-resource",
            "/.well-known/oauth-authorization-server",
        ] {
            let response = router
                .clone()
                .oneshot(
                    axum::http::Request::get(path)
                        .body(axum::body::Body::empty())
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(response.status(), axum::http::StatusCode::OK, "{path}");
        }
    }

    #[tokio::test]
    async fn an_invalid_or_revoked_bearer_token_is_rejected() {
        use tower::ServiceExt;

        let dir = tempfile::tempdir().unwrap();
        let (state, router) = built_router(dir.path()).await;
        let hash_pw = mcp_web::password::hash_password("irrelevant").unwrap();
        let account =
            mcp_web::db::create_account(&state.pool, "revoke-test@example.com", &hash_pw, false)
                .await
                .unwrap();
        register_test_client(&state.pool).await;

        // Never issued.
        let response = router
            .clone()
            .oneshot(mcp_post_request("{}", Some("not-a-real-token")))
            .await
            .unwrap();
        assert_eq!(response.status(), axum::http::StatusCode::UNAUTHORIZED);

        // Issued, then revoked.
        let hash = mcp_web::crypto::sha256_hex("a-revoked-token");
        mcp_web::db::insert_access_token(
            &state.pool,
            &hash,
            "test-client",
            account.id,
            None,
            mcp_web::db::now_unix() + 3600,
        )
        .await
        .unwrap();
        mcp_web::db::revoke_access_token(&state.pool, &hash)
            .await
            .unwrap();
        let response = router
            .clone()
            .oneshot(mcp_post_request("{}", Some("a-revoked-token")))
            .await
            .unwrap();
        assert_eq!(response.status(), axum::http::StatusCode::UNAUTHORIZED);

        // Issued, but already expired.
        let hash = mcp_web::crypto::sha256_hex("an-expired-token");
        mcp_web::db::insert_access_token(
            &state.pool,
            &hash,
            "test-client",
            account.id,
            None,
            mcp_web::db::now_unix() - 1,
        )
        .await
        .unwrap();
        let response = router
            .clone()
            .oneshot(mcp_post_request("{}", Some("an-expired-token")))
            .await
            .unwrap();
        assert_eq!(response.status(), axum::http::StatusCode::UNAUTHORIZED);
    }

    /// Full round trip over real TCP: mint an account and a bearer token the
    /// way the OAuth token endpoint would, then drive an actual `rmcp`
    /// Streamable HTTP client through `initialize` / `tools/list` /
    /// `tools/call`, proving the listener serves real MCP traffic end to end
    /// rather than just passing the bearer check.
    #[tokio::test]
    async fn an_authenticated_client_lists_and_calls_the_seven_tools() {
        use rmcp::model::CallToolRequestParams;
        use rmcp::transport::streamable_http_client::StreamableHttpClientTransportConfig;
        use rmcp::transport::StreamableHttpClientTransport;
        use rmcp::ServiceExt;

        let dir = tempfile::tempdir().unwrap();
        let (state, router) = built_router(dir.path()).await;

        let hash = mcp_web::password::hash_password("irrelevant").unwrap();
        let account =
            mcp_web::db::create_account(&state.pool, "mcp-client@example.com", &hash, false)
                .await
                .unwrap();
        register_test_client(&state.pool).await;
        let raw_token = "a-live-test-token";
        mcp_web::db::insert_access_token(
            &state.pool,
            &mcp_web::crypto::sha256_hex(raw_token),
            "test-client",
            account.id,
            Some(mcp_web::ADVERTISED_SCOPE),
            mcp_web::db::now_unix() + 3600,
        )
        .await
        .unwrap();

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            axum::serve(listener, router).await.unwrap();
        });

        let config = StreamableHttpClientTransportConfig::with_uri(format!("http://{addr}/mcp"))
            .auth_header(raw_token);
        let transport = StreamableHttpClientTransport::from_config(config);
        let client = ().serve(transport).await.expect("client handshake");

        let tools = client.list_all_tools().await.expect("tools/list");
        let mut names: Vec<&str> = tools.iter().map(|t| t.name.as_ref()).collect();
        names.sort_unstable();
        assert_eq!(
            names,
            vec![
                "get_attempts",
                "get_ci_failure",
                "get_run",
                "get_status",
                "get_stream_log",
                "list_ci_failures",
                "list_runs",
            ]
        );

        let result = client
            .call_tool(CallToolRequestParams::new("get_status"))
            .await
            .expect("tools/call get_status");
        assert_ne!(result.is_error, Some(true));
        let text = result
            .content
            .first()
            .and_then(|c| c.as_text())
            .map(|t| t.text.clone())
            .expect("text content");
        let value: serde_json::Value = serde_json::from_str(&text).unwrap();
        assert_eq!(value["source"], "database");

        client.cancel().await.ok();
        server.abort();
    }
}
