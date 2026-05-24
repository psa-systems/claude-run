//! Invoke `claude` with `--output-format=stream-json` and stream the NDJSON
//! events to a per-attempt log file. After exit, parse the final `result`
//! event for cost + success.
//!
//! Matches the semantics of the old `claude-invoke` def in
//! `scripts/claude-run.nu`. Two quirks carry over from there:
//!
//! - `--allowed-tools` is variadic in claude's CLI parser, so we pass
//!   `--allowed-tools=<value>` (single arg) and the prompt as the trailing
//!   positional. Same for `--permission-mode=`.
//! - We give claude an explicitly empty stdin (`/dev/null`) so it doesn't
//!   wait several seconds for stdin data and ignore the prompt arg.
//!
//! Transient classification (rate-limit only) reads the final `result`
//! event's `api_error_status` / `error` fields. We do NOT substring-match
//! the whole stream log - that previously false-positived on MCP "needs-auth"
//! strings in the `system/init` event.

use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::time::Duration;

use serde::Serialize;
use serde_json::Value;

use crate::action::ClaudeBlock;
use crate::error::Result;

#[derive(Debug, Clone, Serialize)]
pub struct ClaudeResult {
    pub ok: bool,
    pub summary: String,
    pub cost: f64,
    pub raw: Value,
    pub error: String,
    pub transient: bool,
}

impl ClaudeResult {
    pub fn synthesized(error: impl Into<String>, summary: impl Into<String>) -> Self {
        Self {
            ok: false,
            summary: summary.into(),
            cost: 0.0,
            raw: Value::Object(Default::default()),
            error: error.into(),
            transient: false,
        }
    }
}

pub struct ClaudeInvoker {
    pub binary: String,
}

impl ClaudeInvoker {
    pub fn new(binary: Option<&str>) -> Self {
        Self {
            binary: binary.unwrap_or("claude").to_string(),
        }
    }

    pub fn stream_log_path(logs_dir: &Path, action: &str, key: &str, attempt: u32) -> PathBuf {
        logs_dir
            .join(action)
            .join("streams")
            .join(format!("{key}-attempt-{attempt}.jsonl"))
    }

    /// Spawn claude, redirect stdout+stderr to `stream_log`, wait, then parse
    /// the last NDJSON line for the final result event.
    pub fn invoke(
        &self,
        prompt: &str,
        claude_cfg: &ClaudeBlock,
        timeout: Duration,
        default_cwd: &Path,
        stream_log: &Path,
    ) -> Result<ClaudeResult> {
        if let Some(parent) = stream_log.parent() {
            std::fs::create_dir_all(parent)?;
        }
        if stream_log.exists() {
            std::fs::remove_file(stream_log)?;
        }

        let cwd = claude_cfg
            .cwd
            .clone()
            .unwrap_or_else(|| default_cwd.display().to_string());
        let allowed_tools = claude_cfg
            .allowed_tools
            .clone()
            .unwrap_or_else(|| "Read,Edit,Write,Bash,Glob,Grep".to_string());
        let permission_mode = claude_cfg
            .permission_mode
            .clone()
            .unwrap_or_else(|| "acceptEdits".to_string());

        let log_file = std::fs::File::create(stream_log)?;
        let log_err = log_file.try_clone()?;

        // We delegate the wall-clock timeout to coreutils `timeout(1)` (same as
        // the nu implementation) so the exit-code-124 contract is preserved.
        // Inside it we run claude with closed stdin and merged stdout+stderr
        // redirected to the stream log.
        let status = Command::new("timeout")
            .arg(format!("{}s", timeout.as_secs()))
            .arg(&self.binary)
            .arg("--print")
            .arg("--verbose")
            .arg("--output-format=stream-json")
            .arg(format!("--permission-mode={}", permission_mode))
            .arg(format!("--allowed-tools={}", allowed_tools))
            .arg(prompt)
            .current_dir(&cwd)
            .stdin(Stdio::null())
            .stdout(Stdio::from(log_file))
            .stderr(Stdio::from(log_err))
            .status()?;

        let exit_code = status.code().unwrap_or(-1);
        let final_event = parse_final_result(stream_log).unwrap_or(Value::Null);

        if exit_code != 0 {
            let tail = read_tail(stream_log, 2000).unwrap_or_default();
            return Ok(ClaudeResult {
                ok: false,
                summary: tail,
                cost: final_event
                    .get("total_cost_usd")
                    .and_then(|v| v.as_f64())
                    .unwrap_or(0.0),
                raw: final_event.clone(),
                error: format!(
                    "claude exited {}, see {}",
                    exit_code,
                    stream_log.display()
                ),
                transient: classify_transient(exit_code, &final_event),
            });
        }

        if final_event.get("type").and_then(|v| v.as_str()) != Some("result") {
            let stream = read_full(stream_log).unwrap_or_default();
            return Ok(ClaudeResult {
                ok: false,
                summary: stream,
                cost: 0.0,
                raw: final_event,
                error: format!(
                    "claude stream did not end with a result event, see {}",
                    stream_log.display()
                ),
                transient: false,
            });
        }

        Ok(ClaudeResult {
            ok: !final_event
                .get("is_error")
                .and_then(|v| v.as_bool())
                .unwrap_or(false),
            summary: final_event
                .get("result")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string(),
            cost: final_event
                .get("total_cost_usd")
                .and_then(|v| v.as_f64())
                .unwrap_or(0.0),
            error: final_event
                .get("error")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string(),
            raw: final_event,
            transient: false,
        })
    }
}

/// Transient = "the API itself told us to back off; a retry might succeed".
/// Only rate-limit signals count. Timeout (exit 124) is never transient.
fn classify_transient(exit_code: i32, final_event: &Value) -> bool {
    if exit_code == 124 {
        return false;
    }
    let api_status = final_event
        .get("api_error_status")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_ascii_lowercase();
    let event_err = final_event
        .get("error")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_ascii_lowercase();
    api_status.contains("rate")
        || api_status.contains("throttl")
        || api_status.contains("429")
        || event_err.contains("rate limit")
        || event_err.contains("usage limit")
}

fn parse_final_result(stream_log: &Path) -> Option<Value> {
    let raw = std::fs::read_to_string(stream_log).ok()?;
    raw.lines()
        .rev()
        .find(|l| !l.trim().is_empty())
        .and_then(|l| serde_json::from_str::<Value>(l).ok())
}

fn read_tail(p: &Path, max: usize) -> std::io::Result<String> {
    let raw = std::fs::read_to_string(p)?;
    if raw.len() <= max {
        Ok(raw)
    } else {
        Ok(raw[raw.len() - max..].to_string())
    }
}

fn read_full(p: &Path) -> std::io::Result<String> {
    std::fs::read_to_string(p)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn transient_only_on_rate_limit_signal() {
        // timeout: never transient
        assert!(!classify_transient(
            124,
            &json!({"type":"result","error":"rate limit"})
        ));
        // hard error, no rate-limit signal: not transient
        assert!(!classify_transient(
            1,
            &json!({"type":"result","is_error":true,"error":"model error"})
        ));
        // api_error_status mentions rate: transient
        assert!(classify_transient(
            1,
            &json!({"type":"result","api_error_status":"rate_limited"})
        ));
        // event error mentions usage limit: transient
        assert!(classify_transient(
            1,
            &json!({"type":"result","error":"usage limit exceeded"})
        ));
    }
}
