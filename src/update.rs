//! CLAUDE-49: `claude-run version` self-update.
//!
//! `show` prints the build-stamped [`crate::VERSION`] banner. `check` resolves
//! the newest `vX.Y.Z` build published to the Forgejo Generic Package registry
//! and compares it against the running `CARGO_PKG_VERSION`. `update` downloads a
//! build (the newest, or a `--version` pin), verifies it against the published
//! `.sha256` sidecar, and atomically swaps the running binary in place.
//!
//! The published coordinates mirror `build-binary.yml`:
//! `https://dev.a8n.run/api/packages/psa-systems/generic/claude-run/<tag>/claude-run-linux-x86_64`.
//! Available versions come from Forgejo's owner package list
//! (`/api/v1/packages/<owner>?type=generic&q=<package>`); the registry is the
//! source of truth for what is installable, so consuming its API here builds
//! the canonical capability rather than working around a missing one.
//!
//! Authentication is anonymous-first: on a 401/403 the caller may supply an
//! `fj` token for the host, otherwise the operation fails with an actionable
//! message and never swaps the binary.

use std::io::{IsTerminal, Write as _};
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};

use serde::Deserialize;
use sha2::{Digest, Sha256};

use crate::error::{Error, Result};
use crate::exit;

/// Default registry host / owner / package, matching `build-binary.yml`.
const DEFAULT_BASE: &str = "https://dev.a8n.run";
const DEFAULT_OWNER: &str = "psa-systems";
const DEFAULT_PACKAGE: &str = "claude-run";
/// Asset filename published per tag (and its sha256 sidecar, CLAUDE-49).
const ASSET: &str = "claude-run-linux-x86_64";

/// Registry coordinates. Overridable by env so tests can point at a stub and
/// operators can retarget a mirror without a rebuild.
#[derive(Debug, Clone)]
pub struct Registry {
    /// Base URL, no trailing slash (e.g. `https://dev.a8n.run`).
    pub base: String,
    /// Hostname parsed from `base`, used to look up `fj` credentials.
    pub host: String,
    pub owner: String,
    pub package: String,
}

impl Registry {
    /// Resolve from env, falling back to the `build-binary.yml` defaults.
    /// `CLAUDE_RUN_REGISTRY_URL` overrides the base (the test/stub hook),
    /// `CLAUDE_RUN_REGISTRY_OWNER` / `CLAUDE_RUN_REGISTRY_PACKAGE` the rest.
    pub fn from_env() -> Self {
        let base = env_or("CLAUDE_RUN_REGISTRY_URL", DEFAULT_BASE)
            .trim_end_matches('/')
            .to_string();
        let host = host_of(&base);
        Self {
            base,
            host,
            owner: env_or("CLAUDE_RUN_REGISTRY_OWNER", DEFAULT_OWNER),
            package: env_or("CLAUDE_RUN_REGISTRY_PACKAGE", DEFAULT_PACKAGE),
        }
    }

    /// Owner package-list endpoint, filtered to generic packages named `package`.
    fn list_url(&self) -> String {
        format!(
            "{}/api/v1/packages/{}?type=generic&q={}",
            self.base, self.owner, self.package
        )
    }

    /// Download URL for the binary at `tag`.
    fn asset_url(&self, tag: &str) -> String {
        format!(
            "{}/api/packages/{}/generic/{}/{}/{}",
            self.base, self.owner, self.package, tag, ASSET
        )
    }

    /// Download URL for the sha256 sidecar at `tag`.
    fn sidecar_url(&self, tag: &str) -> String {
        format!("{}.sha256", self.asset_url(tag))
    }
}

fn env_or(var: &str, default: &str) -> String {
    match std::env::var(var) {
        Ok(v) if !v.trim().is_empty() => v.trim().to_string(),
        _ => default.to_string(),
    }
}

/// Extract the host from a base URL for credential lookup. Best-effort string
/// slicing (no url crate in the tree); falls back to the whole string.
fn host_of(base: &str) -> String {
    let after_scheme = base.split("://").nth(1).unwrap_or(base);
    after_scheme
        .split('/')
        .next()
        .unwrap_or(after_scheme)
        .split('@')
        .next_back()
        .unwrap_or(after_scheme)
        .to_string()
}

/// Credential sent with a registry request.
#[derive(Debug, Clone)]
pub enum Cred {
    Anonymous,
    Token(String),
}

/// Distinguishes an auth challenge (so the caller can escalate credentials)
/// from a successful body.
enum AuthOutcome<T> {
    Ok(T),
    Unauthorized,
}

/// One entry from Forgejo's owner package list. Extra fields are ignored.
#[derive(Debug, Deserialize)]
struct Package {
    name: String,
    version: String,
}

/// How current compares to the newest available build.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UpdateStatus {
    UpToDate,
    UpdateAvailable,
    /// Running build is newer than anything in the registry (local/dev build).
    Ahead,
}

/// Outbound registry client over the existing tokio runtime.
pub struct Updater {
    client: reqwest::Client,
    pub registry: Registry,
}

impl Updater {
    pub fn new() -> Result<Self> {
        Self::with_registry(Registry::from_env())
    }

    pub fn with_registry(registry: Registry) -> Result<Self> {
        let client = reqwest::Client::builder()
            .user_agent(concat!("claude-run/", env!("CARGO_PKG_VERSION")))
            .build()
            .map_err(|e| anyhow::anyhow!("build http client: {e}"))?;
        Ok(Self { client, registry })
    }

    fn apply(&self, req: reqwest::RequestBuilder, cred: &Cred) -> reqwest::RequestBuilder {
        match cred {
            Cred::Anonymous => req,
            Cred::Token(t) => req.header("Authorization", format!("token {t}")),
        }
    }

    async fn try_list(&self, cred: &Cred) -> Result<AuthOutcome<Vec<String>>> {
        let url = self.registry.list_url();
        let resp = self
            .apply(self.client.get(&url), cred)
            .send()
            .await
            .map_err(|e| anyhow::anyhow!("registry request to {url}: {e}"))?;
        let status = resp.status();
        if status.as_u16() == 401 || status.as_u16() == 403 {
            return Ok(AuthOutcome::Unauthorized);
        }
        if !status.is_success() {
            return Err(anyhow::anyhow!("registry list returned {status} for {url}").into());
        }
        let text = resp
            .text()
            .await
            .map_err(|e| anyhow::anyhow!("read registry body: {e}"))?;
        let pkgs: Vec<Package> = serde_json::from_str(&text)
            .map_err(|e| anyhow::anyhow!("parse registry package list: {e}"))?;
        let versions = pkgs
            .into_iter()
            .filter(|p| p.name == self.registry.package)
            .map(|p| p.version)
            .collect();
        Ok(AuthOutcome::Ok(versions))
    }

    /// List published versions, escalating from anonymous to a caller-supplied
    /// token on 401/403. `authorize` is invoked ONLY when anonymous access is
    /// refused; it returns `Some(token)` to retry or `None` to give up. Returns
    /// the credential that worked (so a download can reuse it) and the versions.
    pub async fn list_versions(
        &self,
        authorize: impl FnOnce() -> Option<String>,
    ) -> Result<(Cred, Vec<String>)> {
        if let AuthOutcome::Ok(v) = self.try_list(&Cred::Anonymous).await? {
            return Ok((Cred::Anonymous, v));
        }
        match authorize() {
            Some(token) => {
                let cred = Cred::Token(token);
                match self.try_list(&cred).await? {
                    AuthOutcome::Ok(v) => Ok((cred, v)),
                    AuthOutcome::Unauthorized => Err(anyhow::anyhow!(
                        "registry at {} rejected the fj token (401/403); check the token scope",
                        self.registry.host
                    )
                    .into()),
                }
            }
            None => Err(anyhow::anyhow!(
                "registry at {0} requires authentication (401/403) and no credentials were \
                 supplied. Re-run with --use-fj-token (needs an fj token for {0} in \
                 ~/.local/share/forgejo-cli/keys.json), or set CLAUDE_RUN_REGISTRY_URL to an \
                 anonymous mirror.",
                self.registry.host
            )
            .into()),
        }
    }

    async fn get_bytes(&self, url: &str, cred: &Cred) -> Result<Vec<u8>> {
        let resp = self
            .apply(self.client.get(url), cred)
            .send()
            .await
            .map_err(|e| anyhow::anyhow!("download {url}: {e}"))?;
        let status = resp.status();
        if !status.is_success() {
            return Err(anyhow::anyhow!("download {url} returned {status}").into());
        }
        let bytes = resp
            .bytes()
            .await
            .map_err(|e| anyhow::anyhow!("read body from {url}: {e}"))?;
        Ok(bytes.to_vec())
    }

    /// Download the binary and its sidecar for `tag`.
    pub async fn download(&self, cred: &Cred, tag: &str) -> Result<(Vec<u8>, String)> {
        let bin = self.get_bytes(&self.registry.asset_url(tag), cred).await?;
        let sidecar_bytes = self
            .get_bytes(&self.registry.sidecar_url(tag), cred)
            .await?;
        let sidecar = String::from_utf8(sidecar_bytes)
            .map_err(|e| anyhow::anyhow!("sha256 sidecar is not valid UTF-8: {e}"))?;
        Ok((bin, sidecar))
    }
}

/// Strip a leading `v` and parse as semver, ignoring anything that is not a
/// clean `X.Y.Z` (with optional pre-release/build) tag.
pub fn parse_tag(tag: &str) -> Option<semver::Version> {
    let trimmed = tag.strip_prefix('v').unwrap_or(tag);
    semver::Version::parse(trimmed).ok()
}

/// Newest `vX.Y.Z` release among `versions`, ignoring `latest` and any tag that
/// is not valid semver.
pub fn select_newest(versions: &[String]) -> Option<semver::Version> {
    versions.iter().filter_map(|v| parse_tag(v)).max()
}

/// Normalize a user-supplied pin to the published tag form (`vX.Y.Z`).
pub fn normalize_tag(tag: &str) -> String {
    if tag.starts_with('v') {
        tag.to_string()
    } else {
        format!("v{tag}")
    }
}

/// Compare the running version against the newest available build.
pub fn compare(current: &semver::Version, available: &semver::Version) -> UpdateStatus {
    match available.cmp(current) {
        std::cmp::Ordering::Greater => UpdateStatus::UpdateAvailable,
        std::cmp::Ordering::Equal => UpdateStatus::UpToDate,
        std::cmp::Ordering::Less => UpdateStatus::Ahead,
    }
}

/// Parse a `sha256sum`-style sidecar (`<64-hex>  <filename>`), returning the
/// lowercased hex digest if the first token is a valid SHA-256.
pub fn parse_sha256_sidecar(contents: &str) -> Option<String> {
    let token = contents.split_whitespace().next()?.to_lowercase();
    if token.len() == 64 && token.bytes().all(|b| b.is_ascii_hexdigit()) {
        Some(token)
    } else {
        None
    }
}

/// Verify `bytes` hash to `expected_hex` (case-insensitive). Errors otherwise.
pub fn verify_sha256(bytes: &[u8], expected_hex: &str) -> Result<()> {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    let actual = hex::encode(hasher.finalize());
    if actual.eq_ignore_ascii_case(expected_hex) {
        Ok(())
    } else {
        Err(anyhow::anyhow!(
            "checksum mismatch: expected {}, got {actual}; refusing to install",
            expected_hex.to_lowercase()
        )
        .into())
    }
}

/// The running version, parsed from `CARGO_PKG_VERSION`.
pub fn current_version() -> Result<semver::Version> {
    semver::Version::parse(env!("CARGO_PKG_VERSION"))
        .map_err(|e| anyhow::anyhow!("parse CARGO_PKG_VERSION: {e}").into())
}

/// Atomically replace the running binary with `bytes`.
///
/// Writes to a temp file in the SAME directory as the live binary (same
/// filesystem, so the rename is atomic), `chmod 0o755`, then renames over the
/// live path. On Linux a running executable is replaced by rename; the process
/// keeps its old inode. A permission error (root-owned install path) leaves the
/// existing binary untouched and surfaces an actionable message. The temp file
/// is removed on any failure. No force flags.
pub fn install_binary(bytes: &[u8]) -> Result<PathBuf> {
    let current =
        std::env::current_exe().map_err(|e| anyhow::anyhow!("resolve current executable: {e}"))?;
    let dir = current
        .parent()
        .ok_or_else(|| anyhow::anyhow!("current executable has no parent directory"))?;
    let tmp = dir.join(format!(".{ASSET}.update-{}.tmp", std::process::id()));

    if let Err(e) = write_temp(&tmp, bytes) {
        let _ = std::fs::remove_file(&tmp);
        return Err(install_err(&current, dir, e));
    }
    if let Err(e) = std::fs::rename(&tmp, &current) {
        let _ = std::fs::remove_file(&tmp);
        return Err(install_err(&current, dir, e));
    }
    Ok(current)
}

fn write_temp(tmp: &Path, bytes: &[u8]) -> std::io::Result<()> {
    let mut f = std::fs::File::create(tmp)?;
    f.write_all(bytes)?;
    f.flush()?;
    let mut perms = f.metadata()?.permissions();
    perms.set_mode(0o755);
    f.set_permissions(perms)?;
    f.sync_all()?;
    Ok(())
}

/// Map an io error from the swap into an actionable message, calling out the
/// elevated-privileges path for a root-owned install location.
fn install_err(current: &Path, dir: &Path, e: std::io::Error) -> Error {
    if e.kind() == std::io::ErrorKind::PermissionDenied {
        anyhow::anyhow!(
            "permission denied replacing {} (directory {} is not writable). Re-run with elevated \
             privileges (e.g. via sudo) or install the binary manually. Existing binary left intact.",
            current.display(),
            dir.display()
        )
        .into()
    } else {
        anyhow::anyhow!("install to {}: {e}", current.display()).into()
    }
}

// ---------------------------------------------------------------------------
// CLI orchestration
// ---------------------------------------------------------------------------

/// `claude-run version check`.
pub async fn check(use_fj_token: bool) -> Result<i32> {
    let updater = Updater::new()?;
    let host = updater.registry.host.clone();
    let (_, versions) = updater
        .list_versions(authorizer(&host, use_fj_token))
        .await?;
    let current = current_version()?;
    println!("current:   v{current}");
    match select_newest(&versions) {
        Some(newest) => {
            println!("available: v{newest}");
            match compare(&current, &newest) {
                UpdateStatus::UpdateAvailable => {
                    println!("update available; run `claude-run version update`");
                }
                UpdateStatus::UpToDate => println!("up to date"),
                UpdateStatus::Ahead => {
                    println!("running a newer build than the registry; nothing to do");
                }
            }
        }
        None => println!("available: (no vX.Y.Z releases found in registry)"),
    }
    Ok(exit::OK)
}

/// `claude-run version update [--version <tag>]`.
pub async fn update(pin: Option<String>, use_fj_token: bool, yes: bool) -> Result<i32> {
    let updater = Updater::new()?;
    let host = updater.registry.host.clone();
    let (cred, versions) = updater
        .list_versions(authorizer(&host, use_fj_token))
        .await?;

    let target = match pin {
        Some(tag) => {
            let tag = normalize_tag(&tag);
            if !versions.iter().any(|v| v == &tag) {
                return Err(anyhow::anyhow!(
                    "tag {tag} is not published in the registry. Available: {}",
                    available_list(&versions)
                )
                .into());
            }
            tag
        }
        None => {
            let newest = select_newest(&versions)
                .ok_or_else(|| anyhow::anyhow!("no vX.Y.Z releases found in registry"))?;
            format!("v{newest}")
        }
    };

    if !yes && std::io::stdin().is_terminal() && !confirm(&format!("Install {target}?")) {
        println!("aborted");
        return Ok(exit::OK);
    }

    println!("downloading {target} ...");
    let (bin, sidecar) = updater.download(&cred, &target).await?;
    let expected = parse_sha256_sidecar(&sidecar).ok_or_else(|| {
        anyhow::anyhow!("could not parse the published sha256 sidecar for {target}")
    })?;
    verify_sha256(&bin, &expected)?;
    let path = install_binary(&bin)?;
    println!(
        "installed {target} to {} (checksum verified). Run `claude-run version show` to confirm.",
        path.display()
    );
    Ok(exit::OK)
}

fn available_list(versions: &[String]) -> String {
    if versions.is_empty() {
        "(none)".to_string()
    } else {
        versions.join(", ")
    }
}

/// Build the `authorize` callback for [`Updater::list_versions`]: only consulted
/// on a 401/403. Confirmation is granted by `--use-fj-token`, the
/// `CLAUDE_RUN_USE_FJ_TOKEN` env opt-in, or an interactive prompt; the token
/// itself comes from `fj`'s `keys.json` for the host.
fn authorizer(host: &str, use_fj_token: bool) -> impl FnOnce() -> Option<String> + '_ {
    move || {
        let opt_in = use_fj_token
            || matches!(
                std::env::var("CLAUDE_RUN_USE_FJ_TOKEN").as_deref(),
                Ok("1") | Ok("true")
            );
        let confirmed = opt_in
            || (std::io::stdin().is_terminal()
                && confirm(&format!(
                    "Registry requires authentication. Use the fj token for {host}?"
                )));
        if !confirmed {
            return None;
        }
        fj_token_for_host(host)
    }
}

/// Look up the `fj` token for `host` in `~/.local/share/forgejo-cli/keys.json`.
pub fn fj_token_for_host(host: &str) -> Option<String> {
    let path = dirs::data_dir()?.join("forgejo-cli").join("keys.json");
    let contents = std::fs::read_to_string(path).ok()?;
    let json: serde_json::Value = serde_json::from_str(&contents).ok()?;
    json.get("hosts")?
        .get(host)?
        .get("token")?
        .as_str()
        .map(str::to_string)
}

/// Interactive y/N prompt. Returns false on any read error or non-tty.
fn confirm(question: &str) -> bool {
    print!("{question} [y/N] ");
    if std::io::stdout().flush().is_err() {
        return false;
    }
    let mut line = String::new();
    if std::io::stdin().read_line(&mut line).is_err() {
        return false;
    }
    matches!(line.trim().to_lowercase().as_str(), "y" | "yes")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn select_newest_picks_semver_max() {
        let versions = vec![
            "v0.1.0".to_string(),
            "v0.2.0".to_string(),
            "v0.10.0".to_string(),
            "latest".to_string(),
            "v0.2.0-rc.1".to_string(),
        ];
        assert_eq!(
            select_newest(&versions),
            Some(semver::Version::parse("0.10.0").unwrap())
        );
    }

    #[test]
    fn select_newest_ignores_non_semver() {
        assert_eq!(
            select_newest(&["latest".to_string(), "nightly".to_string()]),
            None
        );
        assert_eq!(select_newest(&[]), None);
    }

    #[test]
    fn compare_covers_newer_equal_older() {
        let v = |s| semver::Version::parse(s).unwrap();
        assert_eq!(
            compare(&v("0.2.0"), &v("0.3.0")),
            UpdateStatus::UpdateAvailable
        );
        assert_eq!(compare(&v("0.2.0"), &v("0.2.0")), UpdateStatus::UpToDate);
        assert_eq!(compare(&v("0.3.0"), &v("0.2.0")), UpdateStatus::Ahead);
    }

    #[test]
    fn normalize_tag_adds_v_prefix() {
        assert_eq!(normalize_tag("0.2.0"), "v0.2.0");
        assert_eq!(normalize_tag("v0.2.0"), "v0.2.0");
    }

    #[test]
    fn sidecar_parses_sha256sum_format() {
        let hex = "e".repeat(64);
        assert_eq!(
            parse_sha256_sidecar(&format!("{hex}  claude-run-linux-x86_64\n")),
            Some(hex.clone())
        );
        // uppercase is normalized to lowercase
        assert_eq!(parse_sha256_sidecar(&"A".repeat(64)), Some("a".repeat(64)));
        // wrong length / garbage rejected
        assert_eq!(parse_sha256_sidecar("deadbeef  file"), None);
        assert_eq!(parse_sha256_sidecar(""), None);
    }

    #[test]
    fn verify_sha256_pass_and_fail() {
        // sha256("") = e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855
        let empty = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";
        assert!(verify_sha256(b"", empty).is_ok());
        assert!(verify_sha256(b"", &empty.to_uppercase()).is_ok());
        assert!(verify_sha256(b"corrupted", empty).is_err());
    }

    #[test]
    fn host_of_extracts_hostname() {
        assert_eq!(host_of("https://dev.a8n.run"), "dev.a8n.run");
        assert_eq!(host_of("https://dev.a8n.run/api/x"), "dev.a8n.run");
        assert_eq!(host_of("http://127.0.0.1:8080"), "127.0.0.1:8080");
    }

    // ---- stub registry (axum) for auth-fallback ordering ----

    use axum::{
        extract::State,
        http::{HeaderMap, StatusCode},
        routing::get,
        Json, Router,
    };
    use std::sync::Arc;

    struct Stub {
        /// When true, anonymous (no Authorization header) gets 401.
        require_auth: bool,
        good_token: String,
    }

    async fn list_handler(
        State(stub): State<Arc<Stub>>,
        headers: HeaderMap,
    ) -> (StatusCode, Json<serde_json::Value>) {
        let auth = headers
            .get("authorization")
            .and_then(|v| v.to_str().ok())
            .unwrap_or("");
        let authed = auth == format!("token {}", stub.good_token);
        if stub.require_auth && !authed {
            return (
                StatusCode::UNAUTHORIZED,
                Json(serde_json::json!({"message":"auth"})),
            );
        }
        let body = serde_json::json!([
            {"name": "claude-run", "type": "generic", "version": "v0.1.0"},
            {"name": "claude-run", "type": "generic", "version": "v0.2.0"},
            {"name": "claude-run", "type": "generic", "version": "latest"},
            {"name": "other", "type": "generic", "version": "v9.9.9"},
        ]);
        (StatusCode::OK, Json(body))
    }

    /// Spin up a stub on an ephemeral port; returns its base URL and a shutdown
    /// guard (the JoinHandle is detached via the returned task).
    async fn spawn_stub(require_auth: bool) -> (String, tokio::task::JoinHandle<()>) {
        let stub = Arc::new(Stub {
            require_auth,
            good_token: "GOODTOKEN".to_string(),
        });
        let app = Router::new()
            .route("/api/v1/packages/Contoso", get(list_handler))
            .with_state(stub);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let handle = tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        (format!("http://{addr}"), handle)
    }

    fn registry_for(base: &str) -> Registry {
        Registry {
            base: base.to_string(),
            host: host_of(base),
            owner: "Contoso".to_string(),
            package: "claude-run".to_string(),
        }
    }

    #[tokio::test]
    async fn anonymous_succeeds_when_registry_is_open() {
        let (base, _h) = spawn_stub(false).await;
        let updater = Updater::with_registry(registry_for(&base)).unwrap();
        let (cred, versions) = updater
            .list_versions(|| panic!("authorize must not be called when anonymous works"))
            .await
            .unwrap();
        assert!(matches!(cred, Cred::Anonymous));
        // only `claude-run` packages, `other` filtered out
        assert_eq!(
            select_newest(&versions),
            Some(semver::Version::parse("0.2.0").unwrap())
        );
    }

    #[tokio::test]
    async fn falls_back_to_token_after_401() {
        let (base, _h) = spawn_stub(true).await;
        let updater = Updater::with_registry(registry_for(&base)).unwrap();
        let (cred, versions) = updater
            .list_versions(|| Some("GOODTOKEN".to_string()))
            .await
            .unwrap();
        assert!(matches!(cred, Cred::Token(_)));
        assert_eq!(versions.len(), 3);
    }

    #[tokio::test]
    async fn fails_when_token_declined() {
        let (base, _h) = spawn_stub(true).await;
        let updater = Updater::with_registry(registry_for(&base)).unwrap();
        let err = updater.list_versions(|| None).await.unwrap_err();
        assert!(err.to_string().contains("requires authentication"));
    }

    #[tokio::test]
    async fn fails_when_token_is_wrong() {
        let (base, _h) = spawn_stub(true).await;
        let updater = Updater::with_registry(registry_for(&base)).unwrap();
        let err = updater
            .list_versions(|| Some("WRONG".to_string()))
            .await
            .unwrap_err();
        assert!(err.to_string().contains("rejected the fj token"));
    }
}
