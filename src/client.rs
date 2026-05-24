//! CLI-side client that talks to the daemon over its unix socket.
//!
//! Each call opens a connection to `$RUNTIME_DIR/daemon.sock`, speaks
//! HTTP/1.1 via `hyper`, and parses the JSON response. CLI commands fall back
//! to operating directly on the DB when no daemon is reachable, so the binary
//! is useful both standalone and as a daemon control plane.

use std::path::{Path, PathBuf};

use http_body_util::{BodyExt, Full};
use hyper::body::Bytes;
use hyper_util::client::legacy::Client;
use serde::de::DeserializeOwned;
use serde_json::Value;

use crate::error::Result;

pub struct DaemonClient {
    socket_path: PathBuf,
    client: Client<hyperlocal::UnixConnector, Full<Bytes>>,
}

impl DaemonClient {
    pub fn new(socket_path: &Path) -> Self {
        let client = Client::builder(hyper_util::rt::TokioExecutor::new())
            .build(hyperlocal::UnixConnector);
        Self {
            socket_path: socket_path.to_path_buf(),
            client,
        }
    }

    pub fn is_reachable(socket_path: &Path) -> bool {
        std::os::unix::net::UnixStream::connect(socket_path).is_ok()
    }

    fn uri(&self, path: &str) -> hyper::Uri {
        hyperlocal::Uri::new(&self.socket_path, path).into()
    }

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
}

async fn read_json<T: DeserializeOwned>(
    resp: hyper::Response<hyper::body::Incoming>,
) -> Result<T> {
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
