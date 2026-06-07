//! Encrypted action secrets (CLAUDE-26).
//!
//! Actions reference a credential by name (`token_ref`) instead of carrying a
//! raw token in their YAML. The named secret lives encrypted at rest in the
//! `secrets` table; the symmetric key is an `age` X25519 identity read from a
//! FILE whose path comes from `CLAUDE_RUN_SECRETS_KEY_FILE` (never stored in the
//! DB beside the ciphertext). A reference is resolved to a decrypted token only
//! at run time, and the plaintext is materialized into per-job credential files
//! the `yt` / `fj` CLIs read, scoped to one job's lifetime.
//!
//! Auth model: this extends CLAUDE-25's host-auth design (the CLIs read their
//! own stores) so an action can carry its own credentials. When no block sets a
//! `token_ref`, nothing is materialized and the CLIs fall back to host auth
//! exactly as before.

use std::io::{Read, Write};
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::str::FromStr;

use sqlx::SqlitePool;

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
    pub fn load_from_file(path: &Path) -> Result<Self> {
        let raw = std::fs::read_to_string(path)
            .map_err(|e| Error::Secret(format!("reading key file {}: {e}", path.display())))?;
        let line = raw
            .lines()
            .map(str::trim)
            .find(|l| l.starts_with("AGE-SECRET-KEY-"))
            .ok_or_else(|| {
                Error::Secret(format!(
                    "no AGE-SECRET-KEY- line in key file {}",
                    path.display()
                ))
            })?;
        let identity = age::x25519::Identity::from_str(line)
            .map_err(|e| Error::Secret(format!("parsing key file {}: {e}", path.display())))?;
        Ok(Self { identity })
    }

    /// Load the key from the path named by `CLAUDE_RUN_SECRETS_KEY_FILE`.
    pub fn load() -> Result<Self> {
        Self::load_from_file(&key_file_path()?)
    }

    /// Encrypt `plaintext` to this key's recipient, returning the age ciphertext.
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
pub fn key_file_path() -> Result<PathBuf> {
    std::env::var(KEY_FILE_ENV)
        .map(|v| PathBuf::from(crate::config::expand_path(&v)))
        .map_err(|_| Error::Secret(format!("{KEY_FILE_ENV} is not set")))
}

/// Decrypt the secret named `name`, bumping its `last_used_at`. Errors when the
/// name is unknown or the ciphertext does not decrypt under the loaded key.
pub async fn resolve_secret(pool: &SqlitePool, key: &SecretsKey, name: &str) -> Result<String> {
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
    /// Decrypted GitHub PAT, when `github.token_ref` is set (CLAUDE-50). Used as
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
pub async fn resolve_youtrack_credentials(
    pool: &SqlitePool,
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
    /// Decrypted GitHub PAT (CLAUDE-50), passed to the GitHub `Forge` for the MCP
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
            let fj_dir = dir.join("forgejo-cli");
            std::fs::create_dir_all(&fj_dir)?;
            std::fs::set_permissions(&fj_dir, std::fs::Permissions::from_mode(0o700))?;
            let keys_path = fj_dir.join("keys.json");

            let mut host_obj = serde_json::Map::new();
            host_obj.insert("token".into(), serde_json::Value::String(token.clone()));
            let mut hosts = serde_json::Map::new();
            hosts.insert(host.to_string(), serde_json::Value::Object(host_obj));
            let payload = serde_json::json!({ "hosts": hosts });
            write_0600(&keys_path, serde_json::to_vec(&payload)?.as_slice())?;

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

impl Drop for JobCredentials {
    fn drop(&mut self) {
        // Best-effort: scrub the plaintext credential files when the job ends.
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

/// Write `bytes` to `path`, creating/truncating it mode `0600`.
fn write_0600(path: &Path, bytes: &[u8]) -> Result<()> {
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

    async fn mem_pool() -> SqlitePool {
        use sqlx::sqlite::SqlitePoolOptions;
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect("sqlite::memory:")
            .await
            .unwrap();
        crate::db::MIGRATOR.run(&pool).await.unwrap();
        pool
    }

    #[tokio::test]
    async fn token_ref_resolves_to_decrypted_secret() {
        // CLAUDE-55: a `[update].token_ref` names a secret; resolving it decrypts
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
