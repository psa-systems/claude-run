//! `claude-run version` self-update.
//!
//! `show` prints the build-stamped [`crate::VERSION`] banner. `check` resolves
//! the newest `vX.Y.Z` build published to the Forgejo Generic Package registry
//! and compares it against the running `CARGO_PKG_VERSION`. `update` downloads a
//! build (the newest, or a `--version` pin), verifies it against the published
//! `.sha256` sidecar, and atomically swaps the running binary in place.
//!
//! The published coordinates mirror `build-binary.yml`:
//! `https://dev.a8n.run/api/packages/psa-systems-private/generic/claude-run/<tag>/claude-run-linux-x86_64`.
//! Available versions come from Forgejo's owner package list
//! (`/api/v1/packages/<owner>?type=generic&q=<package>`); the registry is the
//! source of truth for what is installable, so consuming its API here builds
//! the canonical capability rather than working around a missing one.
//!
//! Authentication is anonymous-first: on a 401/403 the caller may supply a
//! registry token (resolved from `[update].token_ref` in the secrets
//! store) or an `fj` token for the host, otherwise the operation fails with an
//! actionable message and never swaps the binary.
//!
//! Release trains: `release` (the default) keeps the original
//! behavior: the newest semver `vX.Y.Z` package, ignoring the moving `latest`
//! tag. `latest` tracks the moving `latest` package; since that tag is non-semver
//! and re-published on every push to main, "update available" is decided by
//! comparing the running binary's SHA-256 against the published `latest.sha256`
//! sidecar rather than by semver.

use std::io::{IsTerminal, Write as _};
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};

use serde::Deserialize;
use sha2::{Digest, Sha256};

use crate::error::{Error, Result};
use crate::exit;
use crate::paint;

/// Default registry host / owner / package, matching `build-binary.yml`.
const DEFAULT_BASE: &str = "https://dev.a8n.run";
const DEFAULT_OWNER: &str = "psa-systems-private";
const DEFAULT_PACKAGE: &str = "claude-run";
/// Asset filename published per tag (and its sha256 sidecar).
const ASSET: &str = "claude-run-linux-x86_64";
/// The moving tag CI re-publishes on every push to main (the `latest`
/// train). Excluded from the `release` train's semver pick.
pub const LATEST_TAG: &str = "latest";

/// The self-update channel, mirroring yotun's `UpdateTrain`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, clap::ValueEnum)]
pub enum UpdateTrain {
    /// Newest semver `vX.Y.Z` package, excluding the moving `latest` tag.
    #[default]
    Release,
    /// The moving `latest` package (most recent main build).
    Latest,
}

impl UpdateTrain {
    /// Parse the `[update].train` config string. Case-insensitive; any other
    /// value is a configuration error rather than a silent fallback.
    pub fn from_config_str(s: &str) -> Result<Self> {
        match s.trim().to_lowercase().as_str() {
            "release" => Ok(Self::Release),
            "latest" => Ok(Self::Latest),
            other => Err(anyhow::anyhow!(
                "invalid [update].train `{other}`; expected `release` or `latest`"
            )
            .into()),
        }
    }

    fn label(self) -> &'static str {
        match self {
            Self::Release => "release",
            Self::Latest => "latest",
        }
    }
}

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

/// The resolved state of an on-demand check, covering both trains.
/// `Ahead` only arises on the `release` train (semver comparison); `NoneFound`
/// covers an empty `release` registry or an absent `latest` build.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CheckState {
    UpToDate,
    UpdateAvailable,
    /// Running a newer build than the registry (release train only).
    Ahead,
    /// Nothing comparable is published (no `vX.Y.Z` release / no `latest` build).
    NoneFound,
}

/// Structured, printing-free result of a registry check. Shared by
/// the CLI `version check` (which formats it for the terminal) and the web
/// About page (which renders `current` / `available` / `state`). For the
/// `release` train `current`/`available` are `vX.Y.Z` strings; for `latest`
/// they are the installed / published SHA-256 digests (the moving tag is
/// non-semver, so availability is decided by digest).
#[derive(Debug, Clone)]
pub struct CheckOutcome {
    pub train: UpdateTrain,
    pub current: String,
    pub available: Option<String>,
    pub state: CheckState,
}

impl CheckOutcome {
    /// Whether a newer build is installable.
    pub fn update_available(&self) -> bool {
        matches!(self.state, CheckState::UpdateAvailable)
    }
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
    /// refused; its future resolves to `Ok(Some(token))` to retry, `Ok(None)` to
    /// give up, or `Err` when the escalation credential itself cannot be produced
    /// (e.g. the configured `token_ref` secret cannot be decrypted).
    /// Because the callback is async and lazy, any key-file load / secret decrypt
    /// happens only on the escalation path and never on the anonymous-first path.
    /// Returns the credential that worked (so a download can reuse it) and the
    /// versions.
    pub async fn list_versions<F, Fut>(&self, authorize: F) -> Result<(Cred, Vec<String>)>
    where
        F: FnOnce() -> Fut,
        Fut: std::future::Future<Output = Result<Option<String>>>,
    {
        if let AuthOutcome::Ok(v) = self.try_list(&Cred::Anonymous).await? {
            return Ok((Cred::Anonymous, v));
        }
        match authorize().await? {
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

    /// Fetch just the SHA-256 sidecar text for `tag` (the `latest`-train
    /// check, which compares digests without downloading the whole binary).
    pub async fn fetch_sidecar(&self, cred: &Cred, tag: &str) -> Result<String> {
        let bytes = self
            .get_bytes(&self.registry.sidecar_url(tag), cred)
            .await?;
        String::from_utf8(bytes)
            .map_err(|e| anyhow::anyhow!("sha256 sidecar is not valid UTF-8: {e}").into())
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

/// SHA-256 (lowercased hex) of the running executable. On the `latest` train the
/// published tag is non-semver and moving, so update-availability is decided by
/// comparing this digest against the published `latest.sha256` sidecar.
pub fn current_exe_sha256() -> Result<String> {
    let path =
        std::env::current_exe().map_err(|e| anyhow::anyhow!("resolve current executable: {e}"))?;
    let bytes = std::fs::read(&path)
        .map_err(|e| anyhow::anyhow!("read current executable {}: {e}", path.display()))?;
    let mut hasher = Sha256::new();
    hasher.update(&bytes);
    Ok(hex::encode(hasher.finalize()))
}

/// Resolve the tag to install for `train` from the published `versions`. An
/// exact `--version <tag>` pin overrides the train entirely: it is
/// normalized to `vX.Y.Z` and must be published. With no pin, `release` picks
/// the newest semver and `latest` picks the moving `latest` package.
pub fn select_target(train: UpdateTrain, versions: &[String], pin: Option<&str>) -> Result<String> {
    if let Some(tag) = pin {
        let tag = normalize_tag(tag);
        if !versions.iter().any(|v| v == &tag) {
            return Err(anyhow::anyhow!(
                "tag {tag} is not published in the registry. Available: {}",
                available_list(versions)
            )
            .into());
        }
        return Ok(tag);
    }
    match train {
        UpdateTrain::Release => {
            let newest = select_newest(versions)
                .ok_or_else(|| anyhow::anyhow!("no vX.Y.Z releases found in registry"))?;
            Ok(format!("v{newest}"))
        }
        UpdateTrain::Latest => {
            if !versions.iter().any(|v| v == LATEST_TAG) {
                return Err(
                    anyhow::anyhow!("no `{LATEST_TAG}` build published in registry").into(),
                );
            }
            Ok(LATEST_TAG.to_string())
        }
    }
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
// Non-interactive core
//
// These functions do no printing and no prompting, returning structured
// results so both the CLI commands below and the web About handlers call the
// same logic. The web path is anonymous-first, escalating only to the
// configured `[update].token_ref`; the interactive `--use-fj-token` escalation
// stays CLI-only (see `authorize`).
// ---------------------------------------------------------------------------

/// Resolve the check outcome from an already-listed registry. `updater`/`cred`
/// are reused for the `latest`-train sidecar fetch. Does no printing.
pub async fn check_core(
    train: UpdateTrain,
    updater: &Updater,
    cred: &Cred,
    versions: &[String],
) -> Result<CheckOutcome> {
    match train {
        UpdateTrain::Release => {
            let current = current_version()?;
            match select_newest(versions) {
                Some(newest) => {
                    let state = match compare(&current, &newest) {
                        UpdateStatus::UpdateAvailable => CheckState::UpdateAvailable,
                        UpdateStatus::UpToDate => CheckState::UpToDate,
                        UpdateStatus::Ahead => CheckState::Ahead,
                    };
                    Ok(CheckOutcome {
                        train,
                        current: format!("v{current}"),
                        available: Some(format!("v{newest}")),
                        state,
                    })
                }
                None => Ok(CheckOutcome {
                    train,
                    current: format!("v{current}"),
                    available: None,
                    state: CheckState::NoneFound,
                }),
            }
        }
        UpdateTrain::Latest => {
            let installed = current_exe_sha256()?;
            if !versions.iter().any(|v| v == LATEST_TAG) {
                return Ok(CheckOutcome {
                    train,
                    current: installed,
                    available: None,
                    state: CheckState::NoneFound,
                });
            }
            // The `latest` tag is moving and non-semver, so compare digests: the
            // running binary's SHA-256 vs the published `latest.sha256` sidecar.
            let sidecar = updater.fetch_sidecar(cred, LATEST_TAG).await?;
            let published = parse_sha256_sidecar(&sidecar).ok_or_else(|| {
                anyhow::anyhow!("could not parse the published sha256 sidecar for `{LATEST_TAG}`")
            })?;
            let state = if installed.eq_ignore_ascii_case(&published) {
                CheckState::UpToDate
            } else {
                CheckState::UpdateAvailable
            };
            Ok(CheckOutcome {
                train,
                current: installed,
                available: Some(published),
                state,
            })
        }
    }
}

/// Download the newest build on `target`, verify its SHA-256 against the
/// published sidecar, and atomically install it. Returns the installed path.
/// On a checksum mismatch the swap never runs, so the live binary is untouched.
async fn download_verify_install(updater: &Updater, cred: &Cred, target: &str) -> Result<PathBuf> {
    let (bin, sidecar) = updater.download(cred, target).await?;
    let expected = parse_sha256_sidecar(&sidecar).ok_or_else(|| {
        anyhow::anyhow!("could not parse the published sha256 sidecar for {target}")
    })?;
    verify_sha256(&bin, &expected)?;
    install_binary(&bin)
}

/// Apply the newest build on `train` from an already-listed registry: resolve
/// the target tag, download, verify, and atomically install. Returns the
/// applied tag. No confirmation prompt, no version pinning, and no restart (the
/// web button click is the confirmation; the operator restarts the service).
pub async fn apply_core(
    train: UpdateTrain,
    updater: &Updater,
    cred: &Cred,
    versions: &[String],
) -> Result<String> {
    let target = select_target(train, versions, None)?;
    download_verify_install(updater, cred, &target).await?;
    Ok(target)
}

/// Lazily decrypts the configured `[update].token_ref` secret only
/// when the registry demands escalation. Holds the DB pool and the secret name;
/// [`ConfiguredToken::resolve`] loads the age key file and decrypts the secret,
/// and it runs ONLY inside the `list_versions` authorizer, i.e. only on a
/// 401/403 and only when the configured-token branch is taken.
///
/// This keeps `version check` / `version update` anonymous-first. A
/// configured `token_ref` no longer forces `CLAUDE_RUN_SECRETS_KEY_FILE` to be
/// present up front, so an anonymously-readable registry succeeds with no key
/// file even when `token_ref` is set, and `--use-fj-token` can escalate without
/// touching the age store at all.
pub struct ConfiguredToken {
    pool: crate::db::DbPool,
    name: String,
}

impl ConfiguredToken {
    /// Build a lazy resolver for the secret named `name` (from
    /// `[update].token_ref`) against `pool`. Nothing is decrypted here.
    pub fn new(pool: crate::db::DbPool, name: String) -> Self {
        Self { pool, name }
    }

    /// Decrypt the configured secret. Runs only on the registry escalation path.
    /// When the key file env var is unset, returns an actionable error naming
    /// both remedies rather than the bare `CLAUDE_RUN_SECRETS_KEY_FILE is not
    /// set`; other load failures (file missing / unparseable) pass
    /// through with their already-actionable messages.
    async fn resolve(&self) -> Result<String> {
        if std::env::var_os(crate::secrets::KEY_FILE_ENV).is_none() {
            return Err(Error::Secret(format!(
                "the registry requires authentication and `[update].token_ref` is configured, \
                 but {KEY} is not set in this process, so the configured token cannot be \
                 decrypted. Export {KEY} to the same age key file the daemon uses (see the \
                 daemon unit's Environment=), or re-run with --use-fj-token to escalate via the \
                 operator fj keys.json instead.",
                KEY = crate::secrets::KEY_FILE_ENV
            )));
        }
        let key = crate::secrets::SecretsKey::load()?;
        crate::secrets::resolve_secret(&self.pool, &key, &self.name).await
    }
}

/// Web entry for the on-demand check. Builds the updater from env,
/// lists versions anonymous-first (escalating only to the `configured` token),
/// and returns the structured outcome. Used by `POST /about/check`.
pub async fn web_check(
    train: UpdateTrain,
    configured: Option<ConfiguredToken>,
) -> Result<CheckOutcome> {
    let updater = Updater::new()?;
    let (cred, versions) = updater
        .list_versions(|| resolve_configured(configured))
        .await?;
    check_core(train, &updater, &cred, &versions).await
}

/// Web entry for the apply action. Builds the updater from env,
/// lists versions anonymous-first (escalating only to the `configured` token),
/// and applies the newest build on `train`. Returns the applied tag. Does NOT
/// restart the daemon. Used by `POST /about/update`.
pub async fn web_update(train: UpdateTrain, configured: Option<ConfiguredToken>) -> Result<String> {
    let updater = Updater::new()?;
    let (cred, versions) = updater
        .list_versions(|| resolve_configured(configured))
        .await?;
    apply_core(train, &updater, &cred, &versions).await
}

/// The web escalation path has no `--use-fj-token` / interactive fallback: it
/// uses the configured `token_ref` if set, otherwise gives up (the daemon runs
/// with the key file in its environment, so a configured token always resolves).
async fn resolve_configured(configured: Option<ConfiguredToken>) -> Result<Option<String>> {
    match configured {
        Some(ct) => ct.resolve().await.map(Some),
        None => Ok(None),
    }
}

// ---------------------------------------------------------------------------
// CLI orchestration
// ---------------------------------------------------------------------------

/// `claude-run version check [--train <release|latest>]`.
///
/// `configured` lazily resolves `[update].token_ref` on escalation,
/// or `None` when no `token_ref` is set. It is decrypted only if the registry
/// answers 401/403 and the configured-token branch wins.
pub async fn check(
    train: UpdateTrain,
    use_fj_token: bool,
    configured: Option<ConfiguredToken>,
) -> Result<i32> {
    let updater = Updater::new()?;
    let host = updater.registry.host.clone();
    let (cred, versions) = updater
        .list_versions(|| authorize(&host, use_fj_token, configured))
        .await?;
    let outcome = check_core(train, &updater, &cred, &versions).await?;
    print_check_outcome(&outcome);
    Ok(exit::OK)
}

/// Format a [`CheckOutcome`] for the terminal, preserving the original CLI
/// output (labels and update-hint text differ per train).
fn print_check_outcome(o: &CheckOutcome) {
    anstream::println!("train:     {}", paint::paint(paint::VALUE, o.train.label()));
    match o.train {
        UpdateTrain::Release => {
            anstream::println!("current:   {}", paint::paint(paint::VALUE, &o.current));
            match &o.available {
                Some(a) => {
                    anstream::println!("available: {}", paint::paint(paint::VALUE, a));
                    match o.state {
                        CheckState::UpdateAvailable => {
                            paint::notice("update available; run `claude-run version update`");
                        }
                        CheckState::UpToDate => paint::success("up to date"),
                        CheckState::Ahead => {
                            paint::notice("running a newer build than the registry; nothing to do");
                        }
                        CheckState::NoneFound => {}
                    }
                }
                None => anstream::println!(
                    "available: {}",
                    paint::paint(paint::VALUE, "(no vX.Y.Z releases found in registry)")
                ),
            }
        }
        UpdateTrain::Latest => {
            if o.available.is_none() {
                anstream::println!(
                    "available: {}",
                    paint::paint(
                        paint::VALUE,
                        &format!("(no `{LATEST_TAG}` build published in registry)")
                    )
                );
                return;
            }
            anstream::println!("installed: {}", paint::paint(paint::VALUE, &o.current));
            if let Some(published) = &o.available {
                anstream::println!("published: {}", paint::paint(paint::VALUE, published));
            }
            if o.update_available() {
                paint::notice(&format!(
                    "update available; run `claude-run version update --train {}`",
                    UpdateTrain::Latest.label()
                ));
            } else {
                paint::success("up to date");
            }
        }
    }
}

/// `claude-run version update [--train <release|latest>] [--version <tag>]`.
///
/// `configured` lazily resolves `[update].token_ref` on escalation,
/// or `None` when no `token_ref` is set. Shares the identical lazy-resolution
/// path with [`check`].
pub async fn update(
    train: UpdateTrain,
    pin: Option<String>,
    use_fj_token: bool,
    configured: Option<ConfiguredToken>,
    yes: bool,
) -> Result<i32> {
    let updater = Updater::new()?;
    let host = updater.registry.host.clone();
    let (cred, versions) = updater
        .list_versions(|| authorize(&host, use_fj_token, configured))
        .await?;

    let target = select_target(train, &versions, pin.as_deref())?;

    if !yes && std::io::stdin().is_terminal() && !confirm(&format!("Install {target}?")) {
        paint::notice("aborted");
        return Ok(exit::OK);
    }

    paint::notice(&format!("downloading {target} ..."));
    let path = download_verify_install(&updater, &cred, &target).await?;
    paint::success(&format!(
        "installed {target} to {} (checksum verified). Run `claude-run version show` to confirm.",
        path.display()
    ));
    Ok(exit::OK)
}

fn available_list(versions: &[String]) -> String {
    if versions.is_empty() {
        "(none)".to_string()
    } else {
        versions.join(", ")
    }
}

/// The `authorize` callback for [`Updater::list_versions`]: only consulted on a
/// 401/403.
///
/// Precedence (unchanged from the eager path when the key file is
/// present): a configured `token_ref` is preferred and is decrypted
/// lazily here via [`ConfiguredToken::resolve`], so the age key file is loaded
/// only at escalation time. When it resolves, it wins.
///
/// When the configured token cannot be produced (e.g. `CLAUDE_RUN_SECRETS_KEY_FILE`
/// is unset), an explicit `--use-fj-token` / `CLAUDE_RUN_USE_FJ_TOKEN` opt-in acts
/// as the escalation fallback and pulls the token from `fj`'s `keys.json` without
/// touching the age store, so `--use-fj-token` works with `token_ref` set and no
/// key file. Without that opt-in the decrypt error (already actionable, naming
/// both remedies) surfaces.
///
/// With no `token_ref` configured, escalation is granted by the opt-in or an
/// interactive prompt, and the token comes from `fj`'s `keys.json` for the host.
async fn authorize(
    host: &str,
    use_fj_token: bool,
    configured: Option<ConfiguredToken>,
) -> Result<Option<String>> {
    let opt_in = use_fj_token
        || matches!(
            std::env::var("CLAUDE_RUN_USE_FJ_TOKEN").as_deref(),
            Ok("1") | Ok("true")
        );
    if let Some(ct) = configured {
        match ct.resolve().await {
            Ok(token) => return Ok(Some(token)),
            Err(_) if opt_in => {
                // The configured token could not be decrypted, but the operator
                // explicitly opted into the fj store: escalate via it instead.
                return Ok(fj_token_for_host(host));
            }
            Err(e) => return Err(e),
        }
    }
    let confirmed = opt_in
        || (std::io::stdin().is_terminal()
            && confirm(&format!(
                "Registry requires authentication. Use the fj token for {host}?"
            )));
    if !confirmed {
        return Ok(None);
    }
    Ok(fj_token_for_host(host))
}

/// Look up the `fj` token for `host` in `~/.local/share/forgejo-cli/keys.json`.
/// A missing store is a legitimate "no entry" and logs nothing; any other read
/// error or a parse error is logged at `warn` (naming the path and the cause)
/// before falling back to `None`, so a corrupted or unreadable store is never
/// silently indistinguishable from a host with no token.
pub fn fj_token_for_host(host: &str) -> Option<String> {
    let path = dirs::data_dir()?.join("forgejo-cli").join("keys.json");
    read_fj_token_for_host(&path, host)
}

fn read_fj_token_for_host(path: &Path, host: &str) -> Option<String> {
    let contents = match std::fs::read_to_string(path) {
        Ok(c) => c,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return None,
        Err(e) => {
            tracing::warn!(path = %path.display(), error = %e, "reading the fj key store failed");
            return None;
        }
    };
    let json: serde_json::Value = match serde_json::from_str(&contents) {
        Ok(v) => v,
        Err(e) => {
            tracing::warn!(path = %path.display(), error = %e, "parsing the fj key store failed");
            return None;
        }
    };
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

    /// Collects the log lines emitted while a closure runs (mirrors
    /// `action.rs`'s `capture_logs`).
    #[derive(Clone)]
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
            self.clone()
        }
    }

    fn capture_logs<T>(f: impl FnOnce() -> T) -> (T, String) {
        let buf = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        let sub = tracing_subscriber::fmt()
            .with_writer(CaptureWriter(buf.clone()))
            .with_ansi(false)
            .finish();
        let out = tracing::subscriber::with_default(sub, f);
        let logs = String::from_utf8(buf.lock().unwrap().clone()).unwrap();
        (out, logs)
    }

    /// A missing fj key store is a legitimate "no entry": no warning is
    /// logged, and the lookup returns `None`.
    #[test]
    fn fj_token_for_host_missing_file_logs_nothing() {
        let tmp = tempfile::tempdir().unwrap();
        let missing = tmp.path().join("no-such-store.json");
        let (token, logs) = capture_logs(|| read_fj_token_for_host(&missing, "dev.a8n.run"));
        assert!(token.is_none());
        assert!(logs.is_empty(), "expected no log output, got:\n{logs}");
    }

    /// Malformed JSON in the key store is logged at `warn`, naming the path,
    /// before falling back to `None`.
    #[test]
    fn fj_token_for_host_malformed_json_is_logged() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("keys.json");
        std::fs::write(&path, "not valid json").unwrap();
        let (token, logs) = capture_logs(|| read_fj_token_for_host(&path, "dev.a8n.run"));
        assert!(token.is_none());
        assert!(logs.contains("WARN"), "expected a warn log, got:\n{logs}");
        assert!(
            logs.contains(&path.display().to_string()),
            "expected the path in the log, got:\n{logs}"
        );
    }

    /// A valid store with no entry for the requested host returns `None` and
    /// is not itself a failure worth logging.
    #[test]
    fn fj_token_for_host_valid_file_no_entry_for_host() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("keys.json");
        std::fs::write(&path, r#"{"hosts":{"other.host":{"token":"y"}}}"#).unwrap();
        let (token, logs) = capture_logs(|| read_fj_token_for_host(&path, "dev.a8n.run"));
        assert!(token.is_none());
        assert!(logs.is_empty(), "expected no log output, got:\n{logs}");
    }

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
    fn train_parses_release_and_latest_case_insensitively() {
        assert_eq!(
            UpdateTrain::from_config_str("release").unwrap(),
            UpdateTrain::Release
        );
        assert_eq!(
            UpdateTrain::from_config_str("  LATEST ").unwrap(),
            UpdateTrain::Latest
        );
        assert_eq!(UpdateTrain::default(), UpdateTrain::Release);
        assert!(UpdateTrain::from_config_str("nightly").is_err());
    }

    #[test]
    fn select_target_picks_per_train() {
        let versions = vec![
            "v0.1.0".to_string(),
            "v0.2.0".to_string(),
            LATEST_TAG.to_string(),
        ];
        // release -> newest semver, ignoring the moving `latest` tag.
        assert_eq!(
            select_target(UpdateTrain::Release, &versions, None).unwrap(),
            "v0.2.0"
        );
        // latest -> the moving `latest` package.
        assert_eq!(
            select_target(UpdateTrain::Latest, &versions, None).unwrap(),
            LATEST_TAG
        );
    }

    #[test]
    fn select_target_pin_overrides_train_and_must_be_published() {
        let versions = vec!["v0.1.0".to_string(), "v0.2.0".to_string()];
        // A pin is honored regardless of train and normalized to vX.Y.Z.
        assert_eq!(
            select_target(UpdateTrain::Latest, &versions, Some("0.1.0")).unwrap(),
            "v0.1.0"
        );
        // An unpublished pin is rejected.
        assert!(select_target(UpdateTrain::Release, &versions, Some("9.9.9")).is_err());
    }

    #[test]
    fn select_target_latest_errors_when_not_published() {
        let versions = vec!["v0.1.0".to_string()];
        assert!(select_target(UpdateTrain::Latest, &versions, None).is_err());
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

    /// The asset bytes the stub serves for any tag (the apply path).
    const STUB_ASSET: &[u8] = b"fake-claude-run-binary";
    /// A well-formed but deliberately non-matching sha256 sidecar: it is neither
    /// the digest of [`STUB_ASSET`] nor of the running test binary, so the apply
    /// path fails verification before install and the `latest` check reports an
    /// update available.
    const STUB_BAD_SIDECAR: &str = "ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff  claude-run-linux-x86_64\n";

    async fn asset_handler() -> Vec<u8> {
        STUB_ASSET.to_vec()
    }

    async fn sidecar_handler() -> &'static str {
        STUB_BAD_SIDECAR
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
            // Asset + sidecar for any tag (check/apply cores). The
            // `.sha256` route is registered first so it is not shadowed by the
            // bare-asset route's `:tag` capture for the sibling segment.
            .route(
                "/api/packages/Contoso/generic/claude-run/:tag/claude-run-linux-x86_64.sha256",
                get(sidecar_handler),
            )
            .route(
                "/api/packages/Contoso/generic/claude-run/:tag/claude-run-linux-x86_64",
                get(asset_handler),
            )
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
            .list_versions(|| async { panic!("authorize must not be called when anonymous works") })
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
            .list_versions(|| async { Ok(Some("GOODTOKEN".to_string())) })
            .await
            .unwrap();
        assert!(matches!(cred, Cred::Token(_)));
        assert_eq!(versions.len(), 3);
    }

    #[tokio::test]
    async fn fails_when_token_declined() {
        let (base, _h) = spawn_stub(true).await;
        let updater = Updater::with_registry(registry_for(&base)).unwrap();
        let err = updater
            .list_versions(|| async { Ok(None) })
            .await
            .unwrap_err();
        assert!(err.to_string().contains("requires authentication"));
    }

    #[tokio::test]
    async fn fails_when_token_is_wrong() {
        let (base, _h) = spawn_stub(true).await;
        let updater = Updater::with_registry(registry_for(&base)).unwrap();
        let err = updater
            .list_versions(|| async { Ok(Some("WRONG".to_string())) })
            .await
            .unwrap_err();
        assert!(err.to_string().contains("rejected the fj token"));
    }

    // ---- lazy configured-token resolution ----------------------------------

    async fn mem_pool() -> crate::db::DbPool {
        crate::db::test_pool().await
    }

    fn write_test_key(dir: &Path) -> PathBuf {
        use age::secrecy::ExposeSecret;
        let identity = age::x25519::Identity::generate();
        let path = dir.join("key.txt");
        std::fs::write(&path, identity.to_string().expose_secret()).unwrap();
        path
    }

    /// AC1/AC6: with `token_ref` set but the registry anonymously readable, the
    /// authorizer never runs, so the key file is never consulted even when
    /// `CLAUDE_RUN_SECRETS_KEY_FILE` is unset. Anonymous-first is preserved.
    #[tokio::test]
    async fn anonymous_success_with_token_ref_and_no_key_file() {
        let _env = crate::secrets::key_file_env_lock().await;
        std::env::remove_var(crate::secrets::KEY_FILE_ENV);
        let (base, _h) = spawn_stub(false).await;
        let updater = Updater::with_registry(registry_for(&base)).unwrap();
        let host = updater.registry.host.clone();
        // The pool is never queried and the key file never loaded because the
        // anonymous request succeeds first.
        let pool = mem_pool().await;
        let configured = Some(ConfiguredToken::new(pool, "registry-token".to_string()));
        let (cred, versions) = updater
            .list_versions(|| authorize(&host, false, configured))
            .await
            .unwrap();
        assert!(matches!(cred, Cred::Anonymous));
        assert_eq!(
            select_newest(&versions),
            Some(semver::Version::parse("0.2.0").unwrap())
        );
    }

    /// AC4: with `token_ref` set, the key file present, and a 401 registry, the
    /// configured secret is decrypted lazily on escalation and used. Behavior
    /// matches the original eager path.
    #[tokio::test]
    async fn escalation_uses_configured_token_when_key_file_present() {
        let _env = crate::secrets::key_file_env_lock().await;
        let tmp = tempfile::tempdir().unwrap();
        let key_path = write_test_key(tmp.path());
        std::env::set_var(crate::secrets::KEY_FILE_ENV, &key_path);

        let key = crate::secrets::SecretsKey::load_from_file(&key_path).unwrap();
        let pool = mem_pool().await;
        let ciphertext = key.encrypt("GOODTOKEN").unwrap();
        crate::state::secret_create(&pool, "registry-token", &ciphertext)
            .await
            .unwrap();

        let (base, _h) = spawn_stub(true).await;
        let updater = Updater::with_registry(registry_for(&base)).unwrap();
        let host = updater.registry.host.clone();
        let configured = Some(ConfiguredToken::new(pool, "registry-token".to_string()));
        let (cred, versions) = updater
            .list_versions(|| authorize(&host, false, configured))
            .await
            .unwrap();
        assert!(matches!(cred, Cred::Token(_)));
        assert_eq!(versions.len(), 3);
        std::env::remove_var(crate::secrets::KEY_FILE_ENV);
    }

    /// AC3: with `token_ref` set, the key file unset, no `--use-fj-token`, and a
    /// 401 registry, escalation fails with an actionable message naming both
    /// remedies rather than the bare `CLAUDE_RUN_SECRETS_KEY_FILE is not set`.
    #[tokio::test]
    async fn escalation_without_key_file_yields_actionable_error() {
        let _env = crate::secrets::key_file_env_lock().await;
        std::env::remove_var(crate::secrets::KEY_FILE_ENV);
        let (base, _h) = spawn_stub(true).await;
        let updater = Updater::with_registry(registry_for(&base)).unwrap();
        let host = updater.registry.host.clone();
        let pool = mem_pool().await;
        let configured = Some(ConfiguredToken::new(pool, "registry-token".to_string()));
        let err = updater
            .list_versions(|| authorize(&host, false, configured))
            .await
            .unwrap_err();
        let msg = err.to_string();
        // Names both remedies, and is not the bare "is not set" error.
        assert!(msg.contains(crate::secrets::KEY_FILE_ENV), "msg: {msg}");
        assert!(msg.contains("--use-fj-token"), "msg: {msg}");
        assert!(
            msg != format!("secret: {} is not set", crate::secrets::KEY_FILE_ENV),
            "should not be the bare error, got: {msg}"
        );
    }

    /// AC2: with `token_ref` set, the key file unset, and `--use-fj-token`
    /// passed, escalation falls back to the `fj` `keys.json` token and never
    /// fails on the missing key file. The age store is not touched.
    #[tokio::test]
    async fn use_fj_token_escalates_when_key_file_absent() {
        let _env = crate::secrets::key_file_env_lock().await;
        std::env::remove_var(crate::secrets::KEY_FILE_ENV);

        // Point `fj`'s data dir at a temp keys.json holding the stub's token.
        let tmp = tempfile::tempdir().unwrap();
        let host = "registry.example";
        let keys_dir = tmp.path().join("forgejo-cli");
        std::fs::create_dir_all(&keys_dir).unwrap();
        std::fs::write(
            keys_dir.join("keys.json"),
            serde_json::json!({"hosts": {host: {"token": "GOODTOKEN"}}}).to_string(),
        )
        .unwrap();
        let prev_xdg = std::env::var_os("XDG_DATA_HOME");
        std::env::set_var("XDG_DATA_HOME", tmp.path());

        let pool = mem_pool().await;
        let configured = Some(ConfiguredToken::new(pool, "registry-token".to_string()));
        // Missing key file -> configured resolve fails, but the explicit fj
        // opt-in escalates via keys.json instead of surfacing the decrypt error.
        let resolved = authorize(host, true, configured).await.unwrap();
        assert_eq!(resolved.as_deref(), Some("GOODTOKEN"));

        match prev_xdg {
            Some(v) => std::env::set_var("XDG_DATA_HOME", v),
            None => std::env::remove_var("XDG_DATA_HOME"),
        }
    }

    // ---- non-interactive cores ----------------------------------------------

    /// `check_core` on the `release` train reports the newest published semver
    /// as `available` and the running build as `current`, with no printing.
    #[tokio::test]
    async fn check_core_release_reports_newest() {
        let (base, _h) = spawn_stub(false).await;
        let updater = Updater::with_registry(registry_for(&base)).unwrap();
        let (cred, versions) = updater.list_versions(|| async { Ok(None) }).await.unwrap();
        let outcome = check_core(UpdateTrain::Release, &updater, &cred, &versions)
            .await
            .unwrap();
        assert_eq!(outcome.train, UpdateTrain::Release);
        // Newest semver in the stub list, ignoring the moving `latest` tag.
        assert_eq!(outcome.available.as_deref(), Some("v0.2.0"));
        assert_eq!(outcome.current, format!("v{}", current_version().unwrap()));
        // The running build is at/after v0.2.0, so no update is offered.
        assert!(!outcome.update_available());
    }

    /// `check_core` on the `latest` train compares the running exe digest against
    /// the published `latest.sha256`. The stub serves a non-matching digest, so
    /// an update is reported available with both digests surfaced.
    #[tokio::test]
    async fn check_core_latest_compares_digests() {
        let (base, _h) = spawn_stub(false).await;
        let updater = Updater::with_registry(registry_for(&base)).unwrap();
        let (cred, versions) = updater.list_versions(|| async { Ok(None) }).await.unwrap();
        let outcome = check_core(UpdateTrain::Latest, &updater, &cred, &versions)
            .await
            .unwrap();
        assert_eq!(outcome.train, UpdateTrain::Latest);
        assert_eq!(outcome.current, current_exe_sha256().unwrap());
        assert_eq!(outcome.available.as_deref(), Some(&"f".repeat(64)[..]));
        assert!(outcome.update_available());
    }

    /// `apply_core` verifies the downloaded build against the published sidecar
    /// BEFORE swapping the binary. The stub's sidecar does not match the asset,
    /// so apply fails with a checksum-mismatch error and never calls
    /// `install_binary`, leaving the running binary untouched.
    #[tokio::test]
    async fn apply_core_refuses_on_checksum_mismatch() {
        let (base, _h) = spawn_stub(false).await;
        let updater = Updater::with_registry(registry_for(&base)).unwrap();
        let (cred, versions) = updater.list_versions(|| async { Ok(None) }).await.unwrap();
        let err = apply_core(UpdateTrain::Release, &updater, &cred, &versions)
            .await
            .unwrap_err();
        assert!(
            err.to_string().contains("checksum mismatch"),
            "expected a checksum-mismatch error, got: {err}"
        );
    }
}
