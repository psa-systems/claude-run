//! Encrypted action secrets.
//!
//! Actions reference a credential by name (`token_ref`) instead of carrying a
//! raw token in their YAML. The named secret lives encrypted at rest in the
//! `secrets` table; the symmetric key is an `age` X25519 identity read from a
//! FILE whose path comes from `CLAUDE_RUN_SECRETS_KEY_FILE` (never stored in the
//! DB beside the ciphertext). A reference is resolved to a decrypted token only
//! at run time, and the plaintext is materialized into per-job credential files
//! the `yt` / `fj` CLIs read, scoped to one job's lifetime.
//!
//! Auth model: this extends the host-auth design (the CLIs read their
//! own stores) so an action can carry its own credentials. When no block sets a
//! `token_ref`, nothing is materialized and the CLIs fall back to host auth
//! exactly as before.

use std::io::{Read, Write};
use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};
use std::path::{Path, PathBuf};
use std::str::FromStr;

use crate::db::DbPool;
use age::secrecy::ExposeSecret;

use crate::action::{ForgejoTarget, GithubTarget, YoutrackSource};
use crate::error::{Error, Result};
use crate::state;

/// Env var naming the file that holds the `age` X25519 secret key.
pub const KEY_FILE_ENV: &str = "CLAUDE_RUN_SECRETS_KEY_FILE";

/// Test-only serialization of the process-global [`KEY_FILE_ENV`] var. `cargo
/// test` runs the lib tests in parallel, so any test that sets the env and then
/// resolves a secret must hold this lock; otherwise a concurrent setter can swap
/// the key file out between `set_var` and [`SecretsKey::load`], decrypting under
/// the wrong key. Every such test acquires it, so they take turns. A tokio mutex
/// (not `std`) so the guard can be held across the handler's `.await` points.
#[cfg(test)]
pub async fn key_file_env_lock() -> tokio::sync::MutexGuard<'static, ()> {
    static LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());
    LOCK.lock().await
}

/// The symmetric secrets key: an `age` X25519 identity loaded from a key file.
pub struct SecretsKey {
    identity: age::x25519::Identity,
}

impl SecretsKey {
    /// Load the key from `path`. The file holds an `age` secret key line
    /// (`AGE-SECRET-KEY-1...`); blank lines and `#` comments are ignored, so a
    /// file written by `age-keygen` works as-is. Fails closed when the file is
    /// missing, unreadable, or carries no parseable key line.
    // SPEC-7.1: key file resolution and fail-closed loading.
    pub fn load_from_file(path: &Path) -> Result<Self> {
        let raw = std::fs::read_to_string(path)
            .map_err(|e| Error::Secret(format!("reading key file {}: {e}", path.display())))?;
        let line = extract_key(&raw).ok_or_else(|| {
            Error::Secret(format!(
                "no AGE-SECRET-KEY- value in key file {}",
                path.display()
            ))
        })?;
        warn_if_readable_by_others(path);
        let identity = age::x25519::Identity::from_str(&line)
            .map_err(|e| Error::Secret(format!("parsing key file {}: {e}", path.display())))?;
        Ok(Self { identity })
    }

    /// Read the key at [`key_file_path`]. One behaviour: the file is there or
    /// this fails. Creating it is [`ensure_key`]'s job, which runs at serve
    /// start and before the CLI stores anything.
    // SPEC-7.1: the file is there or this fails; creating it is `ensure_key`'s job.
    pub fn load() -> Result<Self> {
        Self::load_from_file(&key_file_path()?)
    }

    /// Encrypt `plaintext` to this key's recipient, returning the age ciphertext.
    // SPEC-7.1: age X25519 encryption under the loaded identity.
    pub fn encrypt(&self, plaintext: &str) -> Result<Vec<u8>> {
        let recipient = self.identity.to_public();
        let encryptor =
            age::Encryptor::with_recipients(std::iter::once(&recipient as &dyn age::Recipient))
                .map_err(|e| Error::Secret(format!("encrypt init: {e}")))?;
        let mut out = Vec::new();
        let mut writer = encryptor
            .wrap_output(&mut out)
            .map_err(|e| Error::Secret(format!("encrypt: {e}")))?;
        writer
            .write_all(plaintext.as_bytes())
            .map_err(|e| Error::Secret(format!("encrypt write: {e}")))?;
        writer
            .finish()
            .map_err(|e| Error::Secret(format!("encrypt finish: {e}")))?;
        Ok(out)
    }

    /// Decrypt an age `ciphertext` produced by [`SecretsKey::encrypt`].
    // SPEC-7.1: age X25519 decryption under the loaded identity.
    pub fn decrypt(&self, ciphertext: &[u8]) -> Result<String> {
        let decryptor = age::Decryptor::new(ciphertext)
            .map_err(|e| Error::Secret(format!("decrypt init: {e}")))?;
        let mut reader = decryptor
            .decrypt(std::iter::once(&self.identity as &dyn age::Identity))
            .map_err(|e| Error::Secret(format!("decrypt: {e}")))?;
        let mut buf = Vec::new();
        reader
            .read_to_end(&mut buf)
            .map_err(|e| Error::Secret(format!("decrypt read: {e}")))?;
        String::from_utf8(buf).map_err(|e| Error::Secret(format!("decrypt not utf-8: {e}")))
    }
}

/// Resolve the key file path from the environment, fail-closed when unset.
/// File name of the age key inside the config dir.
pub const DEFAULT_KEY_FILE: &str = "secrets.key";

/// The path of the age key file: `CLAUDE_RUN_SECRETS_KEY_FILE` when set, else
/// `<CLAUDE_RUN_CONFIG_DIR>/secrets.key`.
///
/// The key is CONFIG, not data. Two reasons, and the first is the one that
/// matters: the ciphertext it decrypts lives in `state.db` under the data dir,
/// and a key stored beside its ciphertext buys nothing. Encrypting the store is
/// worth doing precisely so the database can be copied, snapshotted and backed
/// up without carrying the tokens with it, and that property only exists while
/// the key sits somewhere else. Second, this is the instance's identity: an
/// operator may want to supply it, rotate it, or restore it from their own
/// secret store, which is what the config dir is for. The data dir holds what
/// the runner generates and can rebuild (workdirs are literally garbage
/// collected); the key is neither.
// SPEC-7.1: key file path resolution (env override, else config-dir default).
pub fn key_file_path() -> Result<PathBuf> {
    if let Ok(v) = std::env::var(KEY_FILE_ENV) {
        return Ok(PathBuf::from(crate::config::expand_path(&v)));
    }
    let paths = crate::config::Paths::resolve()?;
    Ok(paths.config_dir.join(DEFAULT_KEY_FILE))
}

/// Where the key used to live: beside `state.db` under the data dir.
/// [`ensure_key`] moves it to [`key_file_path`] on first start after the change,
/// so an existing deployment keeps decrypting what it already stored.
// SPEC-7.1: where an older key is migrated from, see `ensure_key`.
fn legacy_key_file_path() -> Result<PathBuf> {
    Ok(crate::config::Paths::resolve()?
        .data_dir
        .join(DEFAULT_KEY_FILE))
}

/// Make sure a key exists, generating one when the instance has nothing to lose.
///
/// The runner owns its key: an operator should never have to run `age-keygen`
/// before the web UI will store a token. But generating one is only safe while
/// the store is empty. A missing key file next to EXISTING ciphertext means the
/// key was lost, and quietly minting a replacement would turn every stored
/// secret into permanently undecryptable bytes while the UI kept reporting them
/// as present. That case is a hard error naming the path.
///
/// Called once at serve start and by the CLI before it stores a secret, so
/// [`SecretsKey::load`] elsewhere is a plain read with one behaviour.
// SPEC-7.1: generate-only-when-nothing-to-lose key lifecycle, legacy migration,
// and the hard refusal to replace a key while ciphertext still depends on it.
pub async fn ensure_key(pool: &DbPool) -> Result<PathBuf> {
    let path = key_file_path()?;
    if path.exists() {
        return Ok(path);
    }

    // Move a pre-existing key from the old data-dir location rather than
    // generating a new one, or the upgrade would orphan every stored secret.
    let legacy = legacy_key_file_path()?;
    if legacy.exists() && legacy != path {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        std::fs::rename(&legacy, &path).map_err(|e| {
            Error::Secret(format!(
                "moving the secrets key from {} to {}: {e}",
                legacy.display(),
                path.display()
            ))
        })?;
        tracing::info!(
            from = %legacy.display(),
            to = %path.display(),
            "moved the secrets key out of the data dir; it is configuration, not data, and must \
             not sit beside the ciphertext it decrypts"
        );
        return Ok(path);
    }

    let stored = state::secret_list(pool).await?;
    if !stored.is_empty() {
        let names: Vec<&str> = stored.iter().map(|s| s.name.as_str()).collect();
        return Err(Error::Secret(format!(
            "the secrets key {} is missing but {} secret(s) are stored under it ({}). Restore that \
             file from your backup; generating a new key here would leave every one of them \
             permanently undecryptable",
            path.display(),
            stored.len(),
            names.join(", ")
        )));
    }

    generate_key_file(&path)?;
    tracing::info!(
        path = %path.display(),
        "generated a new secrets key; BACK IT UP. Every stored secret is encrypted under it and \
         is unrecoverable without it"
    );
    Ok(path)
}

/// Pull the age secret key out of `raw`, wherever on its line it sits.
///
/// An `age-keygen` file has the key alone on a line, but a key delivered by an
/// operator's own tooling usually arrives inside a document: a SOPS-decrypted
/// `data: AGE-SECRET-KEY-...`, or an env-style `CLAUDE_RUN_AGE_KEY=AGE-...`.
/// Requiring the line to START with the key rejected all of those for no reason,
/// so the token is found anywhere and read to the first whitespace or quote.
fn extract_key(raw: &str) -> Option<String> {
    for line in raw.lines() {
        let line = line.trim();
        if line.starts_with('#') {
            continue;
        }
        let Some(start) = line.find("AGE-SECRET-KEY-") else {
            continue;
        };
        let rest = &line[start..];
        let end = rest
            .find(|c: char| c.is_whitespace() || c == '"' || c == '\'')
            .unwrap_or(rest.len());
        return Some(rest[..end].to_string());
    }
    None
}

/// Warn when the key file is readable by group or other. Not an error: an
/// operator's own delivery mechanism may hand it over `0644` deliberately (the
/// docker repo's `app config update` does), and refusing to start would be worse
/// than saying so. But every process in the container can then read the key that
/// decrypts every stored token, including the agent, so it must not pass in
/// silence.
// SPEC-7.1: a key readable beyond its owner loads anyway but is warned about.
fn warn_if_readable_by_others(path: &Path) {
    match std::fs::metadata(path) {
        Ok(meta) => {
            let mode = meta.permissions().mode() & 0o077;
            if mode != 0 {
                tracing::warn!(
                    path = %path.display(),
                    mode = format!("{:o}", meta.permissions().mode() & 0o777),
                    "the secrets key is readable beyond its owner; every process that can read it \
                     can decrypt every stored token"
                );
            }
        }
        // Unreadable metadata on a file we just read is odd but not fatal here;
        // the read itself already succeeded.
        Err(e) => tracing::warn!(path = %path.display(), error = %e, "cannot stat the secrets key"),
    }
}

/// Create a new age identity at `path` with `0600` permissions, creating the
/// parent directory. Fails rather than overwrites when the file already exists,
/// so a race between two callers cannot destroy a key mid-flight.
// SPEC-7.1: new key files are mode 0600 and never overwrite an existing key.
fn generate_key_file(path: &Path) -> Result<()> {
    use std::io::Write as _;
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).map_err(|e| {
            Error::Secret(format!(
                "creating the secrets key directory {}: {e}",
                parent.display()
            ))
        })?;
    }
    let identity = age::x25519::Identity::generate();
    let mut f = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(path)
        .map_err(|e| {
            Error::Secret(format!(
                "creating the secrets key file {}: {e}",
                path.display()
            ))
        })?;
    writeln!(
        f,
        "# created by claude-run. Back this up: every stored secret is encrypted\n         # under this key and is unrecoverable without it."
    )
    .and_then(|()| writeln!(f, "{}", identity.to_string().expose_secret()))
    .map_err(|e| {
        Error::Secret(format!(
            "writing the secrets key file {}: {e}",
            path.display()
        ))
    })?;
    Ok(())
}

/// Decrypt the secret named `name`, bumping its `last_used_at`. Errors when the
/// name is unknown or the ciphertext does not decrypt under the loaded key.
// SPEC-7.1: unknown name or bad ciphertext fails closed, never an empty token.
pub async fn resolve_secret(pool: &DbPool, key: &SecretsKey, name: &str) -> Result<String> {
    let ciphertext = state::secret_get_ciphertext(pool, name)
        .await?
        .ok_or_else(|| Error::Secret(format!("no secret named `{name}`")))?;
    let plaintext = key.decrypt(&ciphertext)?;
    state::secret_touch(pool, name).await?;
    Ok(plaintext)
}

/// Credentials resolved for a run from an action's `token_ref`s. An all-`None`
/// set means no block referenced a secret: the host-auth path, no materialization.
#[derive(Debug, Clone, Default)]
pub struct ResolvedCredentials {
    /// `(base_url, token)` for the temp `yt` config, when `youtrack.token_ref`
    /// is set. `base_url` is required to build a usable single-file `yt` config.
    pub yt: Option<(String, String)>,
    /// Decrypted forge token, when `forgejo.token_ref` is set. The forge host is
    /// supplied per job (it comes from the issue's resolved VCS integration).
    pub fj_token: Option<String>,
    /// Decrypted GitHub PAT, when `github.token_ref` is set. Used as
    /// the bearer for the MCP call, the `gh` child's `GH_TOKEN`, and the
    /// authenticated push. `None` falls back to host `gh` auth / `GH_TOKEN`.
    pub github_token: Option<String>,
}

impl ResolvedCredentials {
    /// True when no block referenced a secret (host-auth path).
    pub fn is_empty(&self) -> bool {
        self.yt.is_none() && self.fj_token.is_none() && self.github_token.is_none()
    }
}

/// Resolve a YouTrack action's `token_ref`s (under `youtrack` / `forgejo`) to
/// decrypted credentials. The key file is loaded only when at least one block
/// references a secret, so the host-auth path needs no key configured.
// SPEC-7.2: the key is loaded only when a block references a secret, so the
// host-auth path needs no key configured.
pub async fn resolve_youtrack_credentials(
    pool: &DbPool,
    youtrack: &YoutrackSource,
    forgejo: &ForgejoTarget,
    github: &GithubTarget,
) -> Result<ResolvedCredentials> {
    if youtrack.token_ref.is_none() && forgejo.token_ref.is_none() && github.token_ref.is_none() {
        return Ok(ResolvedCredentials::default());
    }
    let key = SecretsKey::load()?;
    let mut out = ResolvedCredentials::default();
    if let Some(name) = &youtrack.token_ref {
        let base_url = youtrack.base_url.as_deref().ok_or_else(|| {
            Error::Secret(
                "youtrack.token_ref is set but youtrack.base_url is missing (needed to build the yt config)".to_string(),
            )
        })?;
        let token = resolve_secret(pool, &key, name).await?;
        out.yt = Some((base_url.to_string(), token));
    }
    if let Some(name) = &forgejo.token_ref {
        out.fj_token = Some(resolve_secret(pool, &key, name).await?);
    }
    if let Some(name) = &github.token_ref {
        out.github_token = Some(resolve_secret(pool, &key, name).await?);
    }
    Ok(out)
}

/// Per-job credential files materialized from [`ResolvedCredentials`]. The
/// plaintext lives in a `0600` file inside a `0700` per-job directory; the guard
/// removes that directory on drop, so the plaintext is scoped to the job's
/// lifetime and concurrent jobs never share a credential file.
pub struct JobCredentials {
    dir: PathBuf,
    /// Env injected onto the `yt` child only (`YOUTRACK_CLI_CONFIG`).
    pub yt_env: Vec<(String, String)>,
    /// Env injected onto the `fj` child only (`XDG_DATA_HOME` override).
    pub fj_env: Vec<(String, String)>,
    /// Temp `keys.json` path, so `Forge::clone_url` reads the same store the
    /// `fj` child does.
    pub fj_keys_path: Option<PathBuf>,
    /// Decrypted GitHub PAT, passed to the GitHub `Forge` for the MCP
    /// bearer, the `gh` child's `GH_TOKEN`, and the authenticated push. Not
    /// written to a file (an env var leaves no on-disk plaintext to scrub); it is
    /// dropped with this guard when the job ends. `None` leaves host `gh` auth.
    pub github_token: Option<String>,
}

impl JobCredentials {
    /// Write the temp credential files into `dir`. `fj_host` is the forge host
    /// the temp `keys.json` is keyed by (the issue's resolved VCS host); pass
    /// `None` for a yt-only credential set (e.g. the gather step, which never
    /// invokes `fj`).
    // SPEC-7.2: per-job credential files scoped to this job's lifetime; see
    // `Drop` below for the removal side of that scope.
    pub fn materialize(
        dir: &Path,
        creds: &ResolvedCredentials,
        fj_host: Option<&str>,
    ) -> Result<Self> {
        std::fs::create_dir_all(dir)?;
        std::fs::set_permissions(dir, std::fs::Permissions::from_mode(0o700))?;

        let mut yt_env = Vec::new();
        let mut fj_env = Vec::new();
        let mut fj_keys_path = None;

        if let Some((url, token)) = &creds.yt {
            // Single-file `yt` config: `--config <path>` (env YOUTRACK_CLI_CONFIG)
            // bypasses the instance store, reading `url` + `token` from this file.
            let cfg_path = dir.join("yt-config.yml");
            write_0600(
                &cfg_path,
                format!("url: {url}\ntoken: {token}\n").as_bytes(),
            )?;
            yt_env.push((
                "YOUTRACK_CLI_CONFIG".to_string(),
                cfg_path.to_string_lossy().into_owned(),
            ));
        }

        if let (Some(token), Some(host)) = (&creds.fj_token, fj_host) {
            // `fj` reads `$XDG_DATA_HOME/forgejo-cli/keys.json`; point its child
            // at this dir and write the store with the host's token.
            let keys_path = write_fj_keys(dir, host, token)?;

            fj_env.push((
                "XDG_DATA_HOME".to_string(),
                dir.to_string_lossy().into_owned(),
            ));
            fj_keys_path = Some(keys_path);
        }

        Ok(Self {
            dir: dir.to_path_buf(),
            yt_env,
            fj_env,
            fj_keys_path,
            github_token: creds.github_token.clone(),
        })
    }
}

// SPEC-7.2: the whole per-job credential directory is scrubbed on drop.
impl Drop for JobCredentials {
    fn drop(&mut self) {
        // Best-effort: scrub the plaintext credential files when the job ends.
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

/// Write a `forgejo-cli/keys.json` under `xdg_data_home` carrying `host` ->
/// `token` and NOTHING else, and return its path. The directory is `0700` and
/// the file `0600`. Shared by the per-job credential store and the
/// per-call Action-log store, so both stay on one schema.
///
/// CLAUDE-123: the current `fj` (`forgejo-cli`) deserializes each host entry
/// with required `type`/`name` fields, so a `token`-only entry fails to parse
/// (``missing field `type` ``) before `fj` ever contacts the server. The entry
/// is built complete, cloning the operator's real store entry when present so
/// `fj`'s schema is never re-derived by hand.
// SPEC-7.2: the written `keys.json` carries `host` -> `token` and nothing else.
pub(crate) fn write_fj_keys(xdg_data_home: &Path, host: &str, token: &str) -> Result<PathBuf> {
    let fj_dir = xdg_data_home.join("forgejo-cli");
    std::fs::create_dir_all(&fj_dir)?;
    std::fs::set_permissions(&fj_dir, std::fs::Permissions::from_mode(0o700))?;
    let keys_path = fj_dir.join("keys.json");

    let host_obj = fj_host_entry(&operator_fj_keys_path(), host, token);
    let mut hosts = serde_json::Map::new();
    hosts.insert(host.to_string(), serde_json::Value::Object(host_obj));
    let payload = serde_json::json!({ "hosts": hosts });
    write_0600(&keys_path, serde_json::to_vec(&payload)?.as_slice())?;
    Ok(keys_path)
}

/// CLAUDE-209: a process-wide unique path segment, `{pid}-{seq}`. The pid alone
/// is NOT unique: two concurrent polls in one daemon would build the same path
/// and the first guard to drop would `remove_dir_all` the other's live files.
// SPEC-7.2: a process-wide unique path segment for a per-job/per-call temp dir.
pub(crate) fn unique_suffix() -> String {
    use std::sync::atomic::{AtomicU64, Ordering};
    static SEQ: AtomicU64 = AtomicU64::new(0);
    format!(
        "{}-{}",
        std::process::id(),
        SEQ.fetch_add(1, Ordering::Relaxed)
    )
}

/// CLAUDE-204: a throwaway `$XDG_DATA_HOME` holding a single-host `keys.json`,
/// removed on drop so a decrypted `log_token_ref` lives only for the one `fj`
/// call that needs it.
pub(crate) struct TempFjKeys {
    dir: PathBuf,
}

impl TempFjKeys {
    /// Create the temp store for `host` -> `token`.
    // SPEC-7.2: a throwaway single-host `fj` store, removed with its guard.
    pub(crate) fn create(host: &str, token: &str) -> Result<Self> {
        let dir = std::env::temp_dir().join(format!("claude-run-fj-{}", unique_suffix()));
        std::fs::create_dir_all(&dir)?;
        std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(0o700))?;
        write_fj_keys(&dir, host, token)?;
        Ok(Self { dir })
    }

    /// The value to set as the `fj` child's `XDG_DATA_HOME`.
    pub(crate) fn xdg_data_home(&self) -> String {
        self.dir.to_string_lossy().into_owned()
    }
}

impl Drop for TempFjKeys {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

/// Write `bytes` to `path`, creating/truncating it mode `0600`.
// SPEC-7.1 / SPEC-7.2 / SPEC-7.3: shared mode-0600 write primitive behind
// every on-disk credential file this module group writes.
pub(crate) fn write_0600(path: &Path, bytes: &[u8]) -> Result<()> {
    use std::os::unix::fs::OpenOptionsExt;
    let mut f = std::fs::OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .mode(0o600)
        .open(path)?;
    f.write_all(bytes)?;
    Ok(())
}

/// Path to the operator's real `fj` key store
/// (`~/.local/share/forgejo-cli/keys.json`), the same store `CliBackend::new`
/// reads. Used as the template source for a per-job entry.
fn operator_fj_keys_path() -> PathBuf {
    // Tests never read the host's real key store.
    if cfg!(test) {
        return PathBuf::from("/nonexistent/forgejo-cli/keys.json");
    }
    dirs::data_dir()
        .unwrap_or_else(|| {
            tracing::warn!("could not resolve the platform data dir; falling back to `.`");
            PathBuf::from(".")
        })
        .join("forgejo-cli")
        .join("keys.json")
}

/// Build the per-job `fj` host entry for `host`, carrying `token`.
///
/// Preferred (CLAUDE-123): clone the operator's real key-store entry for `host`
/// from `operator_keys_path`, so whatever non-token fields the installed `fj`
/// requires (`type`, `name`, and any future additions) carry over verbatim, and
/// overwrite only `token` with the per-job decrypted value. This avoids
/// re-encoding `fj`'s store schema in claude-run (Tooling Gap Discipline).
///
/// Fallback (no operator store, e.g. the container runtime where host auth
/// arrives via read-only bind mounts and the store may be absent): a complete
/// synthetic entry mirroring what `fj auth add-key` writes. `type =
/// "Application"`, `name = "claude-run"`. `fj` accepts a host entry of exactly
/// `{type, name, token}` with no top-level `aliases`/`default_ssh` (both carry
/// serde defaults), verified against the pinned `forgejo-cli`.
// SPEC-7.2: clone the operator's template for `host` and substitute only the
// token; with no template, synthesize a complete entry `fj` can parse.
fn fj_host_entry(
    operator_keys_path: &Path,
    host: &str,
    token: &str,
) -> serde_json::Map<String, serde_json::Value> {
    let mut host_obj = operator_fj_host_entry(operator_keys_path, host).unwrap_or_else(|| {
        let mut m = serde_json::Map::new();
        m.insert(
            "type".into(),
            serde_json::Value::String("Application".into()),
        );
        m.insert(
            "name".into(),
            serde_json::Value::String("claude-run".into()),
        );
        m
    });
    // Always substitute the per-job token; the operator template's token (if
    // any) must never leak into a per-job store.
    host_obj.insert("token".into(), serde_json::Value::String(token.to_string()));
    host_obj
}

/// Read the operator's key store and return a clone of its host entry for
/// `host`, or `None` when the store is missing, unparseable, or has no entry for
/// `host`. Best-effort: any failure falls back to the synthetic entry. A
/// missing store is a legitimate "no entry" and logs nothing; any other read
/// error or a parse error is logged at `warn` (naming the path and the cause)
/// before the fallback is used, so a corrupted or unreadable store is never
/// silently indistinguishable from a host with no credential.
fn operator_fj_host_entry(
    path: &Path,
    host: &str,
) -> Option<serde_json::Map<String, serde_json::Value>> {
    let raw = match std::fs::read_to_string(path) {
        Ok(raw) => raw,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return None,
        Err(e) => {
            tracing::warn!(
                path = %path.display(),
                error = %e,
                "reading the operator fj key store failed"
            );
            return None;
        }
    };
    let parsed: serde_json::Value = match serde_json::from_str(&raw) {
        Ok(v) => v,
        Err(e) => {
            tracing::warn!(
                path = %path.display(),
                error = %e,
                "parsing the operator fj key store failed"
            );
            return None;
        }
    };
    let entry = parsed.get("hosts")?.get(host)?.as_object()?;
    Some(entry.clone())
}

#[cfg(test)]
mod tests {
    use super::*;
    use age::secrecy::ExposeSecret;

    /// Write a fresh age identity to a key file and return its path + the key dir.
    fn write_key(dir: &Path) -> PathBuf {
        let identity = age::x25519::Identity::generate();
        let path = dir.join("key.txt");
        std::fs::write(&path, identity.to_string().expose_secret()).unwrap();
        path
    }

    /// The key is found wherever it sits on its line, so a SOPS-decrypted
    /// document or an env-style file works as a key file, not just the bare
    /// `age-keygen` output.
    #[test]
    fn the_key_is_extracted_from_whatever_shape_delivers_it() {
        let bare = "AGE-SECRET-KEY-1QQQQQQQQQQ";
        for (label, body) in [
            ("age-keygen", format!("# created by age-keygen\n{bare}\n")),
            ("sops yaml", format!("data: {bare}\n")),
            ("env style", format!("CLAUDE_RUN_AGE_KEY={bare}\n")),
            ("quoted yaml", format!("key: \"{bare}\"\n")),
            ("indented", format!("services:\n  env:\n    K: {bare}\n")),
        ] {
            assert_eq!(
                extract_key(&body).as_deref(),
                Some(bare),
                "{label} should yield the key"
            );
        }
        assert_eq!(extract_key("# AGE-SECRET-KEY-1COMMENTED\n"), None);
        assert_eq!(extract_key("nothing here\n"), None);
    }

    /// A key file delivered `0644` by an operator's own tooling still loads, but
    /// does not pass in silence.
    #[test]
    fn a_group_readable_key_still_loads() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("delivered.yml");
        let identity = age::x25519::Identity::generate();
        std::fs::write(
            &path,
            format!("data: {}\n", identity.to_string().expose_secret()),
        )
        .unwrap();
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o644)).unwrap();

        let key = SecretsKey::load_from_file(&path).unwrap();
        let ct = key.encrypt("token").unwrap();
        assert_eq!(key.decrypt(&ct).unwrap(), "token");
    }

    /// The auto-generated key is a usable identity, owner-only, and carries the
    /// back-it-up notice a human will meet in the file.
    #[test]
    fn generated_key_file_is_usable_and_owner_only() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("nested").join("secrets.key");
        generate_key_file(&path).unwrap();

        let mode = std::fs::metadata(&path).unwrap().permissions().mode() & 0o777;
        assert_eq!(mode, 0o600, "the key must not be group- or world-readable");
        let body = std::fs::read_to_string(&path).unwrap();
        assert!(body.contains("Back this up"), "{body}");

        // It round-trips as a real key, comments and all.
        let key = SecretsKey::load_from_file(&path).unwrap();
        let ct = key.encrypt("hello").unwrap();
        assert_eq!(key.decrypt(&ct).unwrap(), "hello");
    }

    /// Generating never clobbers: a second call fails rather than replacing a key
    /// that existing ciphertext depends on.
    #[test]
    fn generating_refuses_to_overwrite_an_existing_key() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("secrets.key");
        generate_key_file(&path).unwrap();
        let before = std::fs::read_to_string(&path).unwrap();

        let err = generate_key_file(&path).unwrap_err();
        assert!(
            err.to_string().contains("creating the secrets key file"),
            "{err}"
        );
        assert_eq!(
            std::fs::read_to_string(&path).unwrap(),
            before,
            "the original key must survive a second generate attempt"
        );
    }

    /// Fixture: point the process at throwaway config/data dirs and clear any
    /// explicit key path, so `key_file_path` resolves to the tested default.
    fn use_temp_dirs(tmp: &Path) {
        std::env::remove_var(KEY_FILE_ENV);
        std::env::set_var("CLAUDE_RUN_CONFIG_DIR", tmp.join("config"));
        std::env::set_var("CLAUDE_RUN_DATA_DIR", tmp.join("data"));
        std::env::set_var("CLAUDE_RUN_STATE_DIR", tmp.join("state"));
        std::env::set_var("CLAUDE_RUN_RUNTIME_DIR", tmp.join("run"));
    }

    fn clear_temp_dirs() {
        for v in [
            "CLAUDE_RUN_CONFIG_DIR",
            "CLAUDE_RUN_DATA_DIR",
            "CLAUDE_RUN_STATE_DIR",
            "CLAUDE_RUN_RUNTIME_DIR",
        ] {
            std::env::remove_var(v);
        }
    }

    /// CLAUDE-478: an action's `secrets` resolve from the store into its
    /// snippets' environment, redacted on the way out; an unknown one fails,
    /// naming it, before anything runs.
    #[tokio::test]
    async fn an_action_secret_reaches_its_snippets_and_an_unknown_one_fails() {
        let _guard = key_file_env_lock().await;
        let tmp = tempfile::tempdir().unwrap();
        use_temp_dirs(tmp.path());
        let pool = mem_pool().await;
        ensure_key(&pool).await.unwrap();
        let key = SecretsKey::load().unwrap();
        crate::state::secret_create(&pool, "my-token", &key.encrypt("mzm_value").unwrap())
            .await
            .unwrap();

        let action = |secret: &str| {
            let raw = format!(
                "name: a\ninput:\n  command: \"[]\"\nprompt: \"x\"\nenv:\n  SITE: here\nsecrets:\n  TOKEN: {secret}\n"
            );
            crate::action::validate_str(&raw, "t").unwrap().action
        };
        let base = crate::snippet::SnippetRunner::new(None);

        let runner = base
            .for_action(&action("my-token"), &pool)
            .await
            .unwrap()
            .expect("a scoped runner");
        let out = runner
            .run(
                r#"echo "$SITE ${#TOKEN} $TOKEN""#,
                &crate::snippet::Bindings::new(),
                tmp.path(),
                "t",
                Some("bash"),
            )
            .unwrap();
        assert_eq!(out.stdout.trim(), "here 9 [REDACTED]");

        let err = base
            .for_action(&action("missing"), &pool)
            .await
            .err()
            .expect("an unknown secret fails")
            .to_string();
        assert!(err.contains("missing"), "{err}");
    }

    /// The key is configuration: it belongs beside `config.toml`, not beside the
    /// `state.db` ciphertext it decrypts.
    #[tokio::test]
    async fn a_fresh_instance_gets_a_key_in_the_config_dir() {
        let _guard = key_file_env_lock().await;
        let tmp = tempfile::tempdir().unwrap();
        use_temp_dirs(tmp.path());
        let pool = mem_pool().await;

        let path = ensure_key(&pool).await.unwrap();
        assert_eq!(path, tmp.path().join("config").join(DEFAULT_KEY_FILE));
        assert!(path.exists());
        assert!(
            !tmp.path().join("data").join(DEFAULT_KEY_FILE).exists(),
            "nothing may be left in the data dir"
        );
        // Idempotent: a second call reuses the same key rather than replacing it.
        let before = std::fs::read_to_string(&path).unwrap();
        assert_eq!(ensure_key(&pool).await.unwrap(), path);
        assert_eq!(std::fs::read_to_string(&path).unwrap(), before);
        clear_temp_dirs();
    }

    /// An existing deployment keeps its key: the data-dir file is moved, not
    /// replaced, or every secret already stored would become unreadable.
    #[tokio::test]
    async fn an_existing_key_is_moved_out_of_the_data_dir() {
        let _guard = key_file_env_lock().await;
        let tmp = tempfile::tempdir().unwrap();
        use_temp_dirs(tmp.path());
        let pool = mem_pool().await;

        let legacy = tmp.path().join("data").join(DEFAULT_KEY_FILE);
        std::fs::create_dir_all(legacy.parent().unwrap()).unwrap();
        generate_key_file(&legacy).unwrap();
        let original = std::fs::read_to_string(&legacy).unwrap();

        let path = ensure_key(&pool).await.unwrap();
        assert_eq!(path, tmp.path().join("config").join(DEFAULT_KEY_FILE));
        assert_eq!(
            std::fs::read_to_string(&path).unwrap(),
            original,
            "the same identity must survive the move"
        );
        assert!(!legacy.exists(), "the old copy is not left behind");
        clear_temp_dirs();
    }

    /// A lost key with ciphertext still in the database is a hard error. Minting
    /// a replacement would leave every stored secret permanently undecryptable
    /// while the UI kept listing them as present.
    #[tokio::test]
    async fn a_lost_key_with_stored_secrets_refuses_to_generate() {
        let _guard = key_file_env_lock().await;
        let tmp = tempfile::tempdir().unwrap();
        use_temp_dirs(tmp.path());
        let pool = mem_pool().await;

        // Store something, then lose the key.
        let path = ensure_key(&pool).await.unwrap();
        let key = SecretsKey::load_from_file(&path).unwrap();
        state::secret_create(
            &pool,
            "auth-youtrack-token",
            &key.encrypt("perm:x").unwrap(),
        )
        .await
        .unwrap();
        std::fs::remove_file(&path).unwrap();

        let err = ensure_key(&pool).await.unwrap_err().to_string();
        assert!(
            err.contains("auth-youtrack-token"),
            "names what is at risk: {err}"
        );
        assert!(err.contains("undecryptable"), "{err}");
        assert!(!path.exists(), "no replacement key may be written");
        clear_temp_dirs();
    }

    /// An operator-named key path is never auto-created: naming it says a
    /// specific key is meant, and inventing a different one would leave every
    /// stored secret undecryptable.
    #[tokio::test]
    async fn an_explicit_missing_key_path_errors_instead_of_generating() {
        let _guard = key_file_env_lock().await;
        let tmp = tempfile::tempdir().unwrap();
        let missing = tmp.path().join("nowhere").join("key.txt");
        std::env::set_var(KEY_FILE_ENV, &missing);
        let err = match SecretsKey::load() {
            Ok(_) => panic!("an explicit missing key path must not load"),
            Err(e) => e,
        };
        std::env::remove_var(KEY_FILE_ENV);
        assert!(
            err.to_string().contains("reading key file"),
            "expected a read failure naming the path, got: {err}"
        );
        assert!(
            !missing.exists(),
            "nothing may be created at an explicit path"
        );
    }

    #[test]
    fn encrypt_then_decrypt_round_trips() {
        let tmp = tempfile::tempdir().unwrap();
        let key = SecretsKey::load_from_file(&write_key(tmp.path())).unwrap();
        let ciphertext = key.encrypt("perm:secret-token-value").unwrap();
        // Ciphertext is not the plaintext.
        assert_ne!(ciphertext, b"perm:secret-token-value");
        assert_eq!(key.decrypt(&ciphertext).unwrap(), "perm:secret-token-value");
    }

    #[test]
    fn missing_key_file_fails_closed() {
        let tmp = tempfile::tempdir().unwrap();
        let missing = tmp.path().join("nope.txt");
        assert!(matches!(
            SecretsKey::load_from_file(&missing),
            Err(Error::Secret(_))
        ));
    }

    #[test]
    fn key_file_without_secret_line_fails_closed() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("garbage.txt");
        std::fs::write(&path, "# just a comment\nnot a key\n").unwrap();
        assert!(matches!(
            SecretsKey::load_from_file(&path),
            Err(Error::Secret(_))
        ));
    }

    #[test]
    fn materialize_writes_0600_files_and_drop_removes_them() {
        let tmp = tempfile::tempdir().unwrap();
        let creds = ResolvedCredentials {
            yt: Some(("https://yt.example".into(), "perm:yt".into())),
            fj_token: Some("fj-token".into()),
            github_token: None,
        };
        let job_dir = tmp.path().join("creds").join("MK-16");
        let cfg_path;
        let keys_path;
        {
            let guard = JobCredentials::materialize(&job_dir, &creds, Some("dev.a8n.run")).unwrap();
            cfg_path = job_dir.join("yt-config.yml");
            keys_path = guard.fj_keys_path.clone().unwrap();

            // yt config carries the url + token and is mode 0600.
            let body = std::fs::read_to_string(&cfg_path).unwrap();
            assert!(body.contains("url: https://yt.example"));
            assert!(body.contains("token: perm:yt"));
            assert_eq!(
                std::fs::metadata(&cfg_path).unwrap().permissions().mode() & 0o777,
                0o600
            );

            // fj keys.json is keyed by host and is mode 0600.
            let keys = std::fs::read_to_string(&keys_path).unwrap();
            assert!(keys.contains("dev.a8n.run"));
            assert!(keys.contains("fj-token"));
            // CLAUDE-123: the host entry must deserialize under `fj`'s schema,
            // which requires `type` and `name` (a `token`-only entry fails to
            // parse with `missing field \`type\``). Mirror that schema here so a
            // regression to a token-only entry fails CI.
            #[derive(serde::Deserialize)]
            struct FjStore {
                hosts: std::collections::BTreeMap<String, FjHostKey>,
            }
            #[derive(serde::Deserialize)]
            struct FjHostKey {
                r#type: String,
                name: String,
                token: String,
            }
            let store: FjStore = serde_json::from_str(&keys).unwrap();
            let entry = store.hosts.get("dev.a8n.run").unwrap();
            assert!(!entry.r#type.is_empty());
            assert!(!entry.name.is_empty());
            assert_eq!(entry.token, "fj-token");
            assert_eq!(
                std::fs::metadata(&keys_path).unwrap().permissions().mode() & 0o777,
                0o600
            );

            // The yt child is pointed at the config; the fj child at the dir.
            assert!(guard
                .yt_env
                .iter()
                .any(|(k, v)| k == "YOUTRACK_CLI_CONFIG" && v == cfg_path.to_str().unwrap()));
            assert!(guard
                .fj_env
                .iter()
                .any(|(k, v)| k == "XDG_DATA_HOME" && v == job_dir.to_str().unwrap()));
        }
        // Drop scrubbed the whole per-job dir.
        assert!(!job_dir.exists());
        assert!(!cfg_path.exists());
        assert!(!keys_path.exists());
    }

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

    /// A missing operator key store is a legitimate "no entry": no warning is
    /// logged, and the lookup returns `None`.
    #[test]
    fn operator_fj_host_entry_missing_file_logs_nothing() {
        let tmp = tempfile::tempdir().unwrap();
        let missing = tmp.path().join("no-such-store.json");
        let (entry, logs) = capture_logs(|| operator_fj_host_entry(&missing, "dev.a8n.run"));
        assert!(entry.is_none());
        assert!(logs.is_empty(), "expected no log output, got:\n{logs}");
    }

    /// Malformed JSON in the operator store is logged at `warn`, naming the
    /// path, before falling back to `None`.
    #[test]
    fn operator_fj_host_entry_malformed_json_is_logged() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("keys.json");
        std::fs::write(&path, "not valid json").unwrap();
        let (entry, logs) = capture_logs(|| operator_fj_host_entry(&path, "dev.a8n.run"));
        assert!(entry.is_none());
        assert!(logs.contains("WARN"), "expected a warn log, got:\n{logs}");
        assert!(
            logs.contains(&path.display().to_string()),
            "expected the path in the log, got:\n{logs}"
        );
    }

    /// A valid store with no entry for the requested host returns `None` and
    /// is not itself a failure worth logging.
    #[test]
    fn operator_fj_host_entry_valid_file_no_entry_for_host() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("keys.json");
        std::fs::write(
            &path,
            r#"{"hosts":{"other.host":{"type":"Application","name":"x","token":"y"}}}"#,
        )
        .unwrap();
        let (entry, logs) = capture_logs(|| operator_fj_host_entry(&path, "dev.a8n.run"));
        assert!(entry.is_none());
        assert!(logs.is_empty(), "expected no log output, got:\n{logs}");
    }

    #[test]
    fn fj_host_entry_falls_back_to_complete_synthetic_entry() {
        // CLAUDE-123: with no operator store, a complete entry is synthesized so
        // `fj` parses it (it requires `type`/`name`, not just `token`).
        let tmp = tempfile::tempdir().unwrap();
        let missing = tmp.path().join("no-store.json");
        let entry = fj_host_entry(&missing, "dev.a8n.run", "per-job-token");
        assert_eq!(
            entry.get("type").and_then(|v| v.as_str()),
            Some("Application")
        );
        assert_eq!(
            entry.get("name").and_then(|v| v.as_str()),
            Some("claude-run")
        );
        assert_eq!(
            entry.get("token").and_then(|v| v.as_str()),
            Some("per-job-token")
        );
    }

    #[test]
    fn fj_host_entry_clones_operator_template_and_swaps_token() {
        // CLAUDE-123: when the operator store has the host, reuse its non-token
        // fields (including any beyond type/name) and substitute only the token,
        // so the operator's own token never leaks into the per-job store.
        let tmp = tempfile::tempdir().unwrap();
        let store = tmp.path().join("keys.json");
        std::fs::write(
            &store,
            r#"{"hosts":{"dev.a8n.run":{"type":"Application","name":"operator-label","token":"operator-token","extra":"keep-me"}},"aliases":{},"default_ssh":[]}"#,
        )
        .unwrap();

        let entry = fj_host_entry(&store, "dev.a8n.run", "per-job-token");
        // Non-token fields come from the operator template verbatim...
        assert_eq!(
            entry.get("type").and_then(|v| v.as_str()),
            Some("Application")
        );
        assert_eq!(
            entry.get("name").and_then(|v| v.as_str()),
            Some("operator-label")
        );
        assert_eq!(entry.get("extra").and_then(|v| v.as_str()), Some("keep-me"));
        // ...but the token is the per-job one, never the operator's.
        assert_eq!(
            entry.get("token").and_then(|v| v.as_str()),
            Some("per-job-token")
        );

        // A host with no operator entry falls back to the synthetic shape.
        let other = fj_host_entry(&store, "other.host", "per-job-token");
        assert_eq!(
            other.get("name").and_then(|v| v.as_str()),
            Some("claude-run")
        );
    }

    #[test]
    fn materialize_yt_only_skips_fj_when_no_host() {
        let tmp = tempfile::tempdir().unwrap();
        let creds = ResolvedCredentials {
            yt: Some(("https://yt.example".into(), "perm:yt".into())),
            fj_token: Some("fj-token".into()),
            github_token: None,
        };
        let dir = tmp.path().join("gather");
        let guard = JobCredentials::materialize(&dir, &creds, None).unwrap();
        // No host -> no fj store, no fj env, even though a token was resolved.
        assert!(guard.fj_keys_path.is_none());
        assert!(guard.fj_env.is_empty());
        assert!(!dir.join("forgejo-cli").exists());
    }

    #[test]
    fn github_token_ref_set_is_carried_env_only() {
        // CLAUDE-50: a resolved GitHub PAT makes the set non-empty and is carried
        // through to the guard for the GitHub `Forge`, with no on-disk file (it
        // travels as an env var / push-URL component only).
        let tmp = tempfile::tempdir().unwrap();
        let creds = ResolvedCredentials {
            yt: None,
            fj_token: None,
            github_token: Some("ghp_secret".into()),
        };
        assert!(!creds.is_empty());
        let dir = tmp.path().join("gh");
        let guard = JobCredentials::materialize(&dir, &creds, Some("github.com")).unwrap();
        assert_eq!(guard.github_token.as_deref(), Some("ghp_secret"));
        // No yt/fj credential files materialized for a github-only token.
        assert!(guard.yt_env.is_empty());
        assert!(guard.fj_env.is_empty());
        assert!(guard.fj_keys_path.is_none());
        assert!(!dir.join("forgejo-cli").exists());
    }

    async fn mem_pool() -> DbPool {
        crate::db::test_pool().await
    }

    #[tokio::test]
    async fn token_ref_resolves_to_decrypted_secret() {
        // CLAUDE-55: a `token_ref` names a secret; resolving it decrypts
        // the stored ciphertext under the loaded age key and returns the token.
        let tmp = tempfile::tempdir().unwrap();
        let key = SecretsKey::load_from_file(&write_key(tmp.path())).unwrap();
        let pool = mem_pool().await;

        let ciphertext = key.encrypt("registry-token-value").unwrap();
        state::secret_create(&pool, "registry-token", &ciphertext)
            .await
            .unwrap();

        let resolved = resolve_secret(&pool, &key, "registry-token").await.unwrap();
        assert_eq!(resolved, "registry-token-value");

        // An unknown ref name fails closed rather than returning an empty token.
        assert!(matches!(
            resolve_secret(&pool, &key, "no-such-ref").await,
            Err(Error::Secret(_))
        ));
    }

    #[test]
    fn host_auth_fallback_when_no_token_refs() {
        // No block set a token_ref: the host-auth path. Nothing resolved, the set
        // is empty, and the github token is absent (falls back to host gh auth).
        let creds = ResolvedCredentials::default();
        assert!(creds.is_empty());
        assert!(creds.github_token.is_none());
    }
}
