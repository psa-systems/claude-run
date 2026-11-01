//! CLI-side client that talks to the daemon over its unix socket.
//!
//! Each call opens a connection to `$RUNTIME_DIR/daemon.sock`, speaks
//! HTTP/1.1 via `hyper`, and parses the JSON response. CLI commands fall back
//! to operating directly on the DB when no daemon is reachable, so the binary
//! is useful both standalone and as a daemon control plane.
//!
//! SPEC-4.1 / SPEC-4.2: the unix-socket side of "CLI mutations delegate to
//! the daemon over the unix socket when it is reachable ... otherwise
//! operate on the database"; served at the socket root (4.2).

use std::path::{Path, PathBuf};

use http_body_util::{BodyExt, Full};
use hyper::body::Bytes;
use hyper_util::client::legacy::Client;
use serde::de::DeserializeOwned;
use serde_json::Value;

use crate::error::Result;

/// SPEC-4.1 / SPEC-4.2: CLI-side client for the daemon's unix-socket API.
pub struct DaemonClient {
    socket_path: PathBuf,
    client: Client<hyperlocal::UnixConnector, Full<Bytes>>,
}

impl DaemonClient {
    pub fn new(socket_path: &Path) -> Self {
        let client =
            Client::builder(hyper_util::rt::TokioExecutor::new()).build(hyperlocal::UnixConnector);
        Self {
            socket_path: socket_path.to_path_buf(),
            client,
        }
    }

    /// SPEC-4.1: "reachable" means a bare `connect()` to the socket path
    /// succeeds; this does not confirm the daemon answers HTTP.
    pub fn is_reachable(socket_path: &Path) -> bool {
        std::os::unix::net::UnixStream::connect(socket_path).is_ok()
    }

    fn uri(&self, path: &str) -> hyper::Uri {
        hyperlocal::Uri::new(&self.socket_path, path).into()
    }

    /// SPEC-4.2: parses the JSON API's HTTP response.
    pub async fn get<T: DeserializeOwned>(&self, path: &str) -> Result<T> {
        let req = hyper::Request::builder()
            .method("GET")
            .uri(self.uri(path))
            .body(Full::new(Bytes::new()))
            .map_err(|e| anyhow::anyhow!("build request: {e}"))?;
        let resp = self
            .client
            .request(req)
            .await
            .map_err(|e| anyhow::anyhow!("daemon request: {e}"))?;
        read_json(resp).await
    }

    /// SPEC-4.2: parses the JSON API's HTTP response.
    pub async fn post<T: DeserializeOwned>(&self, path: &str, body: Option<&Value>) -> Result<T> {
        let bytes = match body {
            Some(b) => Bytes::from(serde_json::to_vec(b)?),
            None => Bytes::new(),
        };
        let req = hyper::Request::builder()
            .method("POST")
            .uri(self.uri(path))
            .header("content-type", "application/json")
            .body(Full::new(bytes))
            .map_err(|e| anyhow::anyhow!("build request: {e}"))?;
        let resp = self
            .client
            .request(req)
            .await
            .map_err(|e| anyhow::anyhow!("daemon request: {e}"))?;
        read_json(resp).await
    }

    /// SPEC-4.1 / SPEC-4.2: surfaces raw status so callers map 404/409 to the
    /// exit-code table in 4.1.
    ///
    /// POST that surfaces the HTTP status alongside the parsed JSON
    /// body, instead of collapsing every non-2xx into an opaque error. The CLI
    /// uses it to translate the delete handler's 404 / 409 into the matching
    /// `exit::CONFIG_ERROR` / `exit::CONFLICT` codes.
    pub async fn post_status(&self, path: &str, body: Option<&Value>) -> Result<(u16, Value)> {
        let bytes = match body {
            Some(b) => Bytes::from(serde_json::to_vec(b)?),
            None => Bytes::new(),
        };
        let req = hyper::Request::builder()
            .method("POST")
            .uri(self.uri(path))
            .header("content-type", "application/json")
            .body(Full::new(bytes))
            .map_err(|e| anyhow::anyhow!("build request: {e}"))?;
        let resp = self
            .client
            .request(req)
            .await
            .map_err(|e| anyhow::anyhow!("daemon request: {e}"))?;
        let status = resp.status().as_u16();
        let body = resp
            .into_body()
            .collect()
            .await
            .map_err(|e| anyhow::anyhow!("collect response body: {e}"))?
            .to_bytes();
        let value: Value = if body.is_empty() {
            Value::Null
        } else {
            serde_json::from_slice(&body).unwrap_or(Value::Null)
        };
        Ok((status, value))
    }
}

/// SPEC-4.2: parses the JSON API's HTTP response.
async fn read_json<T: DeserializeOwned>(resp: hyper::Response<hyper::body::Incoming>) -> Result<T> {
    let status = resp.status();
    let body = resp
        .into_body()
        .collect()
        .await
        .map_err(|e| anyhow::anyhow!("collect response body: {e}"))?
        .to_bytes();
    if !status.is_success() {
        return Err(anyhow::anyhow!(
            "daemon returned {}: {}",
            status,
            String::from_utf8_lossy(&body)
        )
        .into());
    }
    Ok(serde_json::from_slice(&body)?)
}
