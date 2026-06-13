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

use std::io::{BufRead, BufReader, Write};
use std::os::unix::process::CommandExt;
use std::path::{Path, PathBuf};
use std::process::{Child, ChildStdout, Command, ExitStatus, Stdio};
use std::time::{Duration, Instant};

use chrono::SecondsFormat;

use serde::Serialize;
use serde_json::Value;
use tracing::warn;

use crate::action::ClaudeBlock;
use crate::cancel::CancellationToken;
use crate::error::Result;

/// Grace window the claude process group gets after SIGTERM, before the
/// canceller escalates to SIGKILL.
const CANCEL_GRACE: Duration = Duration::from_millis(2000);
/// How often the cancellable wait loop polls the child and the token.
const CANCEL_POLL: Duration = Duration::from_millis(100);
/// CLAUDE-53: once claude emits its terminal `result` event the turn is
/// functionally done. Give a well-behaved claude this long to flush and exit on
/// its own; if it doesn't (a leftover background task is holding it open), the
/// runner terminates the group and finalizes from the captured result rather
/// than waiting out the full `timeout`. A plain constant for now; expose via
/// config only if real runs show a legitimate longer tail.
const RESULT_EXIT_GRACE: Duration = Duration::from_secs(30);

#[derive(Debug, Clone, Serialize)]
pub struct ClaudeResult {
    pub ok: bool,
    pub summary: String,
    pub cost: f64,
    pub raw: Value,
    pub error: String,
    pub transient: bool,
    /// CLAUDE-52: the `timeout <N>s claude` wrapper SIGKILLed the run (exit 124).
    /// A timeout is distinct from a hard failure: the agent may have completed
    /// all, some, or none of the work before being killed, so the handler
    /// surfaces it as a "Timed Out" outcome rather than "Failed". Always paired
    /// with `ok = false`.
    pub timed_out: bool,
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
            timed_out: false,
        }
    }
}

pub struct ClaudeInvoker {
    pub binary: String,
    /// CLAUDE-53: how long to wait for claude to exit on its own after its
    /// terminal `result` event before force-terminating the group. Defaults to
    /// `RESULT_EXIT_GRACE`; tests override it to keep the hang case fast.
    result_exit_grace: Duration,
}

impl ClaudeInvoker {
    pub fn new(binary: Option<&str>) -> Self {
        Self {
            binary: binary.unwrap_or("claude").to_string(),
            result_exit_grace: RESULT_EXIT_GRACE,
        }
    }

    pub fn stream_log_path(logs_dir: &Path, action: &str, key: &str, attempt: u32) -> PathBuf {
        logs_dir
            .join(action)
            .join("streams")
            .join(format!("{key}-attempt-{attempt}.jsonl"))
    }

    /// Spawn claude, relay its stdout to `stream_log` stamping each JSON event
    /// line with an arrival-time `ts` (stderr is a direct, unstamped redirect to
    /// the same file), wait, then scan the stream for the terminal `result`
    /// event by type (CLAUDE-107: not by line position).
    ///
    /// CLAUDE-9: the child is spawned in its own process group and the wait is
    /// cancellable. When `cancel` trips, the whole group (the `timeout`
    /// wrapper, `claude`, and anything they spawned) is terminated with
    /// SIGTERM, a short grace period, then SIGKILL. The caller (`job.rs`)
    /// detects the tripped token and synthesizes a cancelled `$claude_result`
    /// so the action's `post` phase still runs.
    pub fn invoke(
        &self,
        prompt: &str,
        claude_cfg: &ClaudeBlock,
        timeout: Duration,
        default_cwd: &Path,
        stream_log: &Path,
        cancel: &CancellationToken,
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

        // CLAUDE-63: the relay thread (below) owns `log_file` and writes
        // per-event-stamped stdout lines to it; stderr keeps a direct fd
        // redirect via a `try_clone`. Both clones share the same open file
        // description (and therefore a single append cursor), so stamped stdout
        // and unstamped stderr interleave in arrival order without clobbering.
        let log_file = std::fs::File::create(stream_log)?;
        let log_err = log_file.try_clone()?;

        // We delegate the wall-clock timeout to coreutils `timeout(1)` (same as
        // the nu implementation) so the exit-code-124 contract is preserved.
        // Inside it we run claude with closed stdin. `process_group(0)` puts the
        // child (and therefore claude + descendants) in a fresh group whose pgid
        // equals the child's pid, so cancellation can signal the whole tree at
        // once.
        //
        // CLAUDE-63: stdout is piped (not a direct fd redirect) so a relay
        // thread can stamp each JSON event line with its arrival time before it
        // lands in the stream log. stderr stays a direct redirect: claude's
        // stderr is incidental non-JSON noise and is left unstamped by design.
        //
        // Tradeoff: the old direct fd redirect let the child keep writing even
        // if the runner died; the pipe relay means a dead runner blocks the
        // child's stdout pipe. Accepted because the runner already owns the
        // child's lifecycle (timeout kill, CLAUDE-52 / CLAUDE-53), so a dead
        // runner already implies a dead run.
        let mut child = Command::new("timeout")
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
            .stdout(Stdio::piped())
            .stderr(Stdio::from(log_err))
            .process_group(0)
            .spawn()?;

        // Relay claude's piped stdout to the stream log, stamping each JSON
        // object line with its arrival time. Runs on its own thread next to the
        // blocking wait below and drains to EOF (the pipe closes when the child
        // group exits, including a timeout/cancel kill, so all output up to the
        // kill is captured).
        let relay = child
            .stdout
            .take()
            .map(|stdout| std::thread::spawn(move || relay_stamped_stdout(stdout, log_file)));

        let pgid = child.id() as i32;
        let outcome = wait_or_cancel(&mut child, pgid, stream_log, self.result_exit_grace, cancel)?;
        let status = outcome.status;

        // Drain the relay: the child group has exited, so the stdout pipe is at
        // EOF and the thread has flushed every line it read up to the kill.
        if let Some(relay) = relay {
            let _ = relay.join();
        }

        let exit_code = status.code().unwrap_or(-1);
        let final_event = parse_final_result(stream_log).unwrap_or(Value::Null);

        // CLAUDE-53: when we force-terminated a hung-but-done group after its
        // terminal `result` event, the wrapper exits non-zero (SIGTERM 143 /
        // SIGKILL 137). The captured result event is the authoritative outcome,
        // so skip the exit-code failure branch and build from the result below.
        // The exit-code / timeout path is reserved for runs where NO terminal
        // result was ever observed (a genuine hang or crash).
        if exit_code != 0 && !outcome.finalized_on_result {
            let summary = friendly_failure_summary(&final_event, stream_log);
            return Ok(ClaudeResult {
                ok: false,
                summary,
                cost: final_event
                    .get("total_cost_usd")
                    .and_then(|v| v.as_f64())
                    .unwrap_or(0.0),
                raw: final_event.clone(),
                error: format!("claude exited {}, see {}", exit_code, stream_log.display()),
                transient: classify_transient(exit_code, &final_event),
                // CLAUDE-52: exit 124 is the `timeout(1)` wrapper's SIGKILL code.
                timed_out: exit_code == 124,
            });
        }

        if final_event.get("type").and_then(|v| v.as_str()) != Some("result") {
            let summary = friendly_failure_summary(&final_event, stream_log);
            return Ok(ClaudeResult {
                ok: false,
                summary,
                cost: 0.0,
                raw: final_event,
                error: format!(
                    "claude stream did not end with a result event, see {}",
                    stream_log.display()
                ),
                transient: false,
                timed_out: false,
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
            timed_out: false,
        })
    }
}

/// Outcome of the cancellable wait loop.
struct WaitOutcome {
    status: ExitStatus,
    /// CLAUDE-53: true when claude emitted its terminal `result` event but did
    /// not exit within the grace window, so the loop force-terminated the (hung)
    /// group. The caller must build the outcome from the captured result event,
    /// not from the signal-induced exit code.
    finalized_on_result: bool,
}

/// Wait for the child to exit, watching `cancel` and the stream log. Returns the
/// child's exit status on normal completion; on cancellation, terminates the
/// child's process group and returns the status of the terminated `timeout`
/// wrapper.
///
/// CLAUDE-53: additionally watches the stream log for claude's terminal `result`
/// event. Once that event appears the turn is functionally done; the child gets
/// `result_exit_grace` to exit on its own, after which the loop terminates the
/// group and flags `finalized_on_result` so the caller finalizes from the
/// captured result instead of waiting out the full `timeout`.
///
/// Polling (rather than blocking on `wait`) is deliberate: `invoke` is a
/// synchronous call already blocking its worker thread on the child, so a
/// 100ms poll adds responsiveness to cancellation without changing the
/// threading model.
fn wait_or_cancel(
    child: &mut Child,
    pgid: i32,
    stream_log: &Path,
    result_exit_grace: Duration,
    cancel: &CancellationToken,
) -> std::io::Result<WaitOutcome> {
    // Set once claude's terminal `result` event is first observed; the child is
    // force-terminated if it is still alive at this instant.
    let mut result_deadline: Option<Instant> = None;
    loop {
        if let Some(status) = child.try_wait()? {
            return Ok(WaitOutcome {
                status,
                finalized_on_result: false,
            });
        }
        // Cancellation wins over the result-event path and terminates immediately.
        if cancel.is_cancelled() {
            let status = terminate_group(child, pgid)?;
            return Ok(WaitOutcome {
                status,
                finalized_on_result: false,
            });
        }
        match result_deadline {
            None => {
                if stream_has_terminal_result(stream_log) {
                    result_deadline = Some(Instant::now() + result_exit_grace);
                }
            }
            Some(deadline) => {
                if Instant::now() >= deadline {
                    warn!(
                        pid = child.id(),
                        pgid,
                        grace_secs = result_exit_grace.as_secs(),
                        "claude emitted its terminal result but did not exit within the grace window; terminating the process group (leftover background task?)"
                    );
                    let status = terminate_group(child, pgid)?;
                    return Ok(WaitOutcome {
                        status,
                        finalized_on_result: true,
                    });
                }
            }
        }
        std::thread::sleep(CANCEL_POLL);
    }
}

/// Whether the stream log contains a terminal `result` event. CLAUDE-107:
/// `parse_final_result` selects the result event by `type` regardless of
/// position, so a result event followed by trailing `system` events still reads
/// as "terminal result observed" here. A partial trailing write fails to parse
/// and is skipped, so the grace timer only starts once a full `result` event has
/// landed.
fn stream_has_terminal_result(stream_log: &Path) -> bool {
    parse_final_result(stream_log).is_some()
}

/// Terminate the child's process group: SIGTERM, a short grace window, then
/// SIGKILL. Signalling `-pgid` hits every process in the group (`timeout`,
/// `claude`, and anything they spawned), so nothing is left orphaned.
fn terminate_group(child: &mut Child, pgid: i32) -> std::io::Result<ExitStatus> {
    unsafe {
        libc::kill(-pgid, libc::SIGTERM);
    }
    let deadline = Instant::now() + CANCEL_GRACE;
    while Instant::now() < deadline {
        if let Some(status) = child.try_wait()? {
            return Ok(status);
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    unsafe {
        libc::kill(-pgid, libc::SIGKILL);
    }
    child.wait()
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

/// CLAUDE-63: relay the child's piped stdout to `sink`, stamping each line that
/// parses as a JSON object with a top-level RFC 3339 `ts` (arrival time). Lines
/// that do not parse as a JSON object pass through byte-identically (defensive:
/// never lose output). Flush per line so the SSE tail and the terminal-result
/// poller see events promptly.
fn relay_stamped_stdout(stdout: ChildStdout, mut sink: std::fs::File) {
    let mut reader = BufReader::new(stdout);
    let mut line = Vec::new();
    loop {
        line.clear();
        match reader.read_until(b'\n', &mut line) {
            Ok(0) => break, // EOF
            Ok(_) => {}
            Err(_) => break,
        }
        let stamped = stamp_line(&line);
        if sink.write_all(&stamped).is_err() {
            break;
        }
        let _ = sink.flush();
    }
}

/// Augment a single stream line with an arrival-time `ts` key. `raw` is the line
/// as read, including its trailing `\n` if present. A line whose content parses
/// as a JSON object gets a top-level `ts` (RFC 3339, millis, UTC) inserted and
/// is re-serialized, preserving every original key; the trailing newline (if
/// any) is reattached. Anything else (a non-object JSON value, a partial line,
/// or non-JSON noise) is returned unchanged, byte for byte.
fn stamp_line(raw: &[u8]) -> Vec<u8> {
    let (content, newline): (&[u8], &[u8]) = match raw.last() {
        Some(b'\n') => (&raw[..raw.len() - 1], &raw[raw.len() - 1..]),
        _ => (raw, b""),
    };
    let Ok(Value::Object(mut map)) = serde_json::from_slice::<Value>(content) else {
        return raw.to_vec();
    };
    let ts = chrono::Utc::now().to_rfc3339_opts(SecondsFormat::Millis, true);
    map.insert("ts".to_string(), Value::String(ts));
    match serde_json::to_vec(&Value::Object(map)) {
        Ok(mut out) => {
            out.extend_from_slice(newline);
            out
        }
        // Re-serialization should never fail for a value we just deserialized;
        // if it somehow does, fall back to the original bytes.
        Err(_) => raw.to_vec(),
    }
}

/// CLAUDE-107: select the authoritative terminal `result` event by event type,
/// not by line position. Claude Code may emit trailing `system` events
/// (`task_updated`, `task_notification`, or any future post-result event) after
/// the `result` event, so the last non-empty line is not reliably the result.
/// Scan the stream backward and return the last line that parses as a JSON
/// object with `type == "result"`, skipping any non-`result` trailing lines.
/// Returns `None` only when the stream contains no `result` event anywhere
/// (the genuine no-terminal-result case the `:228` guard catches).
fn parse_final_result(stream_log: &Path) -> Option<Value> {
    let raw = std::fs::read_to_string(stream_log).ok()?;
    raw.lines()
        .rev()
        .filter_map(|l| serde_json::from_str::<Value>(l).ok())
        .find(|v| v.get("type").and_then(|t| t.as_str()) == Some("result"))
}

fn read_tail(p: &Path, max: usize) -> std::io::Result<String> {
    let raw = std::fs::read_to_string(p)?;
    if raw.len() <= max {
        Ok(raw)
    } else {
        Ok(raw[raw.len() - max..].to_string())
    }
}

/// CLAUDE-56: Extract a short, human-readable failure summary from the stream
/// log instead of pasting the raw NDJSON tail into failure comments. Preference
/// order:
/// 1. A terminal `result` event's `result` text (the message claude composed,
///    e.g. `API Error: 529 Overloaded...`), falling back to its `error` string.
/// 2. The concatenated text blocks of the last `assistant` message event, which
///    captures the human-readable error text when no result event ever landed.
/// 3. The bounded 2000-byte tail of the stream log (the prior behavior).
fn friendly_failure_summary(final_event: &Value, stream_log: &Path) -> String {
    if final_event.get("type").and_then(|v| v.as_str()) == Some("result") {
        let result = final_event
            .get("result")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        if !result.is_empty() {
            return result.to_string();
        }
        let error = final_event
            .get("error")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        if !error.is_empty() {
            return error.to_string();
        }
    }
    if let Some(text) = last_assistant_text(stream_log) {
        if !text.is_empty() {
            return text;
        }
    }
    read_tail(stream_log, 2000).unwrap_or_default()
}

/// Scan the stream log backwards for the last `assistant` message event and
/// concatenate the `text` of its text content blocks.
fn last_assistant_text(stream_log: &Path) -> Option<String> {
    let raw = std::fs::read_to_string(stream_log).ok()?;
    for line in raw.lines().rev() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        let Ok(event) = serde_json::from_str::<Value>(line) else {
            continue;
        };
        if event.get("type").and_then(|v| v.as_str()) != Some("assistant") {
            continue;
        }
        let Some(content) = event
            .get("message")
            .and_then(|m| m.get("content"))
            .and_then(|c| c.as_array())
        else {
            continue;
        };
        let text: String = content
            .iter()
            .filter(|b| b.get("type").and_then(|v| v.as_str()) == Some("text"))
            .filter_map(|b| b.get("text").and_then(|v| v.as_str()))
            .collect::<Vec<_>>()
            .join("");
        return Some(text);
    }
    None
}

/// CLAUDE-56: render the final result event as a collapsed, pretty-printed JSON
/// block for failure comments (YouTrack and Forgejo both render Markdown + the
/// `<details>` HTML element). Returns an empty string for synthesized results
/// (`Value::Null` or an empty object) so callers can append it unconditionally.
pub fn raw_event_details(raw: &Value) -> String {
    match raw {
        Value::Null => return String::new(),
        Value::Object(obj) if obj.is_empty() => return String::new(),
        _ => {}
    }
    let Ok(pretty) = serde_json::to_string_pretty(raw) else {
        return String::new();
    };
    format!(
        "\n\n<details>\n<summary>Raw result event</summary>\n\n```json\n{pretty}\n```\n\n</details>"
    )
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

    /// CLAUDE-52: a `claude` that outlives the wall-clock budget is SIGKILLed by
    /// the `timeout(1)` wrapper (exit 124). The result carries `timed_out = true`
    /// alongside `ok = false`, distinguishing a timeout from a hard failure.
    #[test]
    fn invoke_timeout_sets_timed_out() {
        use std::io::Write;
        use std::os::unix::fs::PermissionsExt;

        let dir = tempfile::tempdir().unwrap();
        // Stub "claude": sleep well past the 1s budget so the wrapper kills it.
        let stub = dir.path().join("claude-stub.sh");
        {
            let mut f = std::fs::File::create(&stub).unwrap();
            writeln!(f, "#!/bin/sh\nexec sleep 30").unwrap();
        }
        std::fs::set_permissions(&stub, std::fs::Permissions::from_mode(0o755)).unwrap();

        let invoker = ClaudeInvoker::new(Some(stub.to_str().unwrap()));
        let cancel = CancellationToken::new();
        let log = dir.path().join("stream.jsonl");
        let cfg = ClaudeBlock::default();
        let res = invoker
            .invoke(
                "hi",
                &cfg,
                Duration::from_secs(1),
                dir.path(),
                &log,
                &cancel,
            )
            .unwrap();
        assert!(res.timed_out, "exit 124 should set timed_out");
        assert!(!res.ok, "a timeout is not ok");
        assert!(!res.transient, "a timeout is never transient");
    }

    const RESULT_LINE: &str = r#"{"type":"result","subtype":"success","is_error":false,"result":"done","total_cost_usd":0.42}"#;

    fn write_stub(path: &Path, body: &str) {
        use std::os::unix::fs::PermissionsExt;
        std::fs::write(path, body).unwrap();
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o755)).unwrap();
    }

    /// CLAUDE-53: claude emits its terminal result event and then exits on its
    /// own within the grace window. Behavior is unchanged from today: the result
    /// event is parsed for the success outcome, and nothing is force-terminated.
    #[test]
    fn invoke_result_then_clean_exit_succeeds() {
        let dir = tempfile::tempdir().unwrap();
        let stub = dir.path().join("claude-stub.sh");
        write_stub(
            &stub,
            &format!("#!/bin/sh\nprintf '%s\\n' '{RESULT_LINE}'\nexit 0\n"),
        );

        let invoker = ClaudeInvoker {
            binary: stub.to_str().unwrap().to_string(),
            result_exit_grace: Duration::from_secs(30),
        };
        let cancel = CancellationToken::new();
        let log = dir.path().join("stream.jsonl");
        let cfg = ClaudeBlock::default();
        let res = invoker
            .invoke(
                "hi",
                &cfg,
                Duration::from_secs(10),
                dir.path(),
                &log,
                &cancel,
            )
            .unwrap();
        assert!(res.ok, "a success result event finalizes as ok");
        assert_eq!(res.summary, "done");
        assert_eq!(res.cost, 0.42);
        assert!(!res.timed_out, "a clean exit is not a timeout");
    }

    /// CLAUDE-53: claude emits its terminal result event but does NOT exit (a
    /// leftover background task holds it open). The runner force-terminates the
    /// group after the grace window and finalizes from the captured result, so a
    /// success result is preserved as success - not reported as a timeout - and
    /// the wait is bounded by the grace, not the full `timeout`.
    #[test]
    fn invoke_result_then_hang_terminated_at_grace_preserves_success() {
        let dir = tempfile::tempdir().unwrap();
        let stub = dir.path().join("claude-stub.sh");
        write_stub(
            &stub,
            &format!("#!/bin/sh\nprintf '%s\\n' '{RESULT_LINE}'\nsleep 30\n"),
        );

        let invoker = ClaudeInvoker {
            binary: stub.to_str().unwrap().to_string(),
            // Short grace so the test doesn't wait the production 30s.
            result_exit_grace: Duration::from_millis(300),
        };
        let cancel = CancellationToken::new();
        let log = dir.path().join("stream.jsonl");
        let cfg = ClaudeBlock::default();
        let start = Instant::now();
        let res = invoker
            // Wall-clock timeout far exceeds the grace, so finalization is driven
            // by the result event, not the `timeout(1)` wrapper.
            .invoke(
                "hi",
                &cfg,
                Duration::from_secs(30),
                dir.path(),
                &log,
                &cancel,
            )
            .unwrap();
        let elapsed = start.elapsed();

        assert!(
            res.ok,
            "captured success result is preserved despite SIGTERM"
        );
        assert_eq!(res.summary, "done");
        assert_eq!(res.cost, 0.42);
        assert!(!res.timed_out, "result-driven finalize is not a timeout");
        assert!(
            elapsed < Duration::from_secs(5),
            "should finalize at the grace window, not the 30s timeout (took {elapsed:?})"
        );
    }

    /// CLAUDE-107: claude emits its terminal `result/success` event and then
    /// trailing `system` events (`task_updated`, `task_notification`) before
    /// exiting 0 - the real PMS-258 stream shape. The run must classify as a
    /// success (`ok = true`) by selecting the result event by type, not by line
    /// position, so a successful run is not misread as a failure.
    #[test]
    fn invoke_result_then_trailing_system_events_succeeds() {
        let dir = tempfile::tempdir().unwrap();
        let stub = dir.path().join("claude-stub.sh");
        write_stub(
            &stub,
            &format!(
                "#!/bin/sh\n\
                 printf '%s\\n' '{RESULT_LINE}'\n\
                 printf '%s\\n' '{{\"type\":\"system\",\"subtype\":\"task_updated\"}}'\n\
                 printf '%s\\n' '{{\"type\":\"system\",\"subtype\":\"task_notification\"}}'\n\
                 exit 0\n"
            ),
        );

        let invoker = ClaudeInvoker {
            binary: stub.to_str().unwrap().to_string(),
            result_exit_grace: Duration::from_secs(30),
        };
        let cancel = CancellationToken::new();
        let log = dir.path().join("stream.jsonl");
        let cfg = ClaudeBlock::default();
        let res = invoker
            .invoke(
                "hi",
                &cfg,
                Duration::from_secs(10),
                dir.path(),
                &log,
                &cancel,
            )
            .unwrap();
        assert!(
            res.ok,
            "result/success followed by trailing system events finalizes as ok"
        );
        assert_eq!(res.summary, "done");
        assert_eq!(res.cost, 0.42);
        assert!(res.error.is_empty(), "no error on a success run");
    }

    /// CLAUDE-9: tripping the token mid-run terminates the child's process
    /// group, so `invoke` returns long before the (30s) stub/timeout would.
    #[test]
    fn invoke_cancellation_terminates_promptly() {
        use std::io::Write;
        use std::os::unix::fs::PermissionsExt;

        let dir = tempfile::tempdir().unwrap();
        // Stub "claude": ignore the flags invoke passes and sleep well past the
        // test budget. SIGTERM to the group ends it immediately.
        let stub = dir.path().join("claude-stub.sh");
        {
            let mut f = std::fs::File::create(&stub).unwrap();
            writeln!(f, "#!/bin/sh\nexec sleep 30").unwrap();
        }
        std::fs::set_permissions(&stub, std::fs::Permissions::from_mode(0o755)).unwrap();

        let invoker = ClaudeInvoker::new(Some(stub.to_str().unwrap()));
        let cancel = CancellationToken::new();
        let trip = cancel.clone();
        let canceller = std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(300));
            trip.cancel();
        });

        let log = dir.path().join("stream.jsonl");
        let cfg = ClaudeBlock::default();
        let start = Instant::now();
        let res = invoker.invoke(
            "hi",
            &cfg,
            Duration::from_secs(30),
            dir.path(),
            &log,
            &cancel,
        );
        let elapsed = start.elapsed();
        canceller.join().unwrap();

        assert!(res.is_ok(), "invoke returns Ok even when cancelled");
        // SIGTERM + at most a 2s grace + poll slop; comfortably under the 30s
        // stub. A regression that ignored the token would block ~30s here.
        assert!(
            elapsed < Duration::from_secs(5),
            "cancel should terminate fast, took {elapsed:?}"
        );
    }

    /// CLAUDE-56: a terminal `result` event yields its human-readable `result`
    /// text as the friendly summary.
    #[test]
    fn friendly_summary_prefers_result_text() {
        let dir = tempfile::tempdir().unwrap();
        let log = dir.path().join("stream.jsonl");
        std::fs::write(
            &log,
            "{\"type\":\"assistant\",\"message\":{\"content\":[{\"type\":\"text\",\"text\":\"ignored\"}]}}\n\
             {\"type\":\"result\",\"is_error\":true,\"result\":\"API Error: 529 Overloaded.\"}\n",
        )
        .unwrap();
        let final_event = parse_final_result(&log).unwrap();
        assert_eq!(
            friendly_failure_summary(&final_event, &log),
            "API Error: 529 Overloaded."
        );
    }

    /// CLAUDE-56: a `result` event with an empty `result` falls back to `error`.
    #[test]
    fn friendly_summary_falls_back_to_result_error() {
        let dir = tempfile::tempdir().unwrap();
        let log = dir.path().join("stream.jsonl");
        std::fs::write(
            &log,
            "{\"type\":\"result\",\"is_error\":true,\"result\":\"\",\"error\":\"model overloaded\"}\n",
        )
        .unwrap();
        let final_event = parse_final_result(&log).unwrap();
        assert_eq!(
            friendly_failure_summary(&final_event, &log),
            "model overloaded"
        );
    }

    /// CLAUDE-56: with no terminal `result` event, the concatenated text blocks
    /// of the last `assistant` message become the summary.
    #[test]
    fn friendly_summary_uses_last_assistant_text() {
        let dir = tempfile::tempdir().unwrap();
        let log = dir.path().join("stream.jsonl");
        std::fs::write(
            &log,
            "{\"type\":\"assistant\",\"message\":{\"content\":[{\"type\":\"text\",\"text\":\"old\"}]}}\n\
             {\"type\":\"assistant\",\"message\":{\"content\":[{\"type\":\"text\",\"text\":\"API Error: \"},{\"type\":\"text\",\"text\":\"529 Overloaded.\"}]}}\n",
        )
        .unwrap();
        // CLAUDE-107: no result event in the stream, so `parse_final_result`
        // returns None and the caller falls back to `Value::Null` (mirrors the
        // production call at the `invoke` site).
        let final_event = parse_final_result(&log).unwrap_or(Value::Null);
        assert_eq!(
            friendly_failure_summary(&final_event, &log),
            "API Error: 529 Overloaded."
        );
    }

    /// CLAUDE-56: neither a result event nor an assistant message - fall back to
    /// the bounded raw tail.
    #[test]
    fn friendly_summary_falls_back_to_tail() {
        let dir = tempfile::tempdir().unwrap();
        let log = dir.path().join("stream.jsonl");
        std::fs::write(&log, "{\"type\":\"system\",\"subtype\":\"init\"}\n").unwrap();
        // CLAUDE-107: no result event, so the caller falls back to `Value::Null`.
        let final_event = parse_final_result(&log).unwrap_or(Value::Null);
        assert_eq!(
            friendly_failure_summary(&final_event, &log),
            "{\"type\":\"system\",\"subtype\":\"init\"}\n"
        );
    }

    /// CLAUDE-56: a non-zero exit whose stream ends with a `result` event makes
    /// `summary` the friendly result text, not the raw JSON line.
    #[test]
    fn invoke_failure_summary_is_friendly_result_text() {
        let dir = tempfile::tempdir().unwrap();
        let stub = dir.path().join("claude-stub.sh");
        let line = r#"{"type":"result","is_error":true,"api_error_status":"529","result":"API Error: 529 Overloaded. Try again in a moment.","total_cost_usd":0.01}"#;
        write_stub(
            &stub,
            &format!("#!/bin/sh\nprintf '%s\\n' '{line}'\nexit 1\n"),
        );

        let invoker = ClaudeInvoker {
            binary: stub.to_str().unwrap().to_string(),
            result_exit_grace: Duration::from_secs(30),
        };
        let cancel = CancellationToken::new();
        let log = dir.path().join("stream.jsonl");
        let cfg = ClaudeBlock::default();
        let res = invoker
            .invoke(
                "hi",
                &cfg,
                Duration::from_secs(10),
                dir.path(),
                &log,
                &cancel,
            )
            .unwrap();
        assert!(!res.ok, "non-zero exit is a failure");
        assert_eq!(
            res.summary,
            "API Error: 529 Overloaded. Try again in a moment."
        );
        assert!(
            !res.summary.contains("\"type\":\"result\""),
            "summary must not be the raw JSON line"
        );
        // raw still carries the parsed event for the comment builders.
        assert_eq!(
            res.raw.get("api_error_status").and_then(|v| v.as_str()),
            Some("529")
        );
    }

    /// CLAUDE-63: a JSON object line is augmented with a top-level `ts` that
    /// parses as RFC 3339, every original key is preserved, and the trailing
    /// newline is kept.
    #[test]
    fn stamp_line_augments_json_object() {
        let raw = b"{\"type\":\"system\",\"subtype\":\"init\",\"model\":\"opus\"}\n";
        let stamped = stamp_line(raw);
        assert_eq!(stamped.last(), Some(&b'\n'), "trailing newline preserved");
        let v: Value = serde_json::from_slice(&stamped).unwrap();
        assert_eq!(v.get("type").and_then(|t| t.as_str()), Some("system"));
        assert_eq!(v.get("subtype").and_then(|t| t.as_str()), Some("init"));
        assert_eq!(v.get("model").and_then(|t| t.as_str()), Some("opus"));
        let ts = v.get("ts").and_then(|t| t.as_str()).expect("ts present");
        chrono::DateTime::parse_from_rfc3339(ts).expect("ts is RFC 3339");
    }

    /// CLAUDE-63: a non-JSON line (and a non-object JSON value) passes through
    /// byte-identically - we never lose or rewrite output we can't stamp.
    #[test]
    fn stamp_line_passes_non_object_through_unchanged() {
        for raw in [
            &b"not json at all\n"[..],
            &b"[1,2,3]\n"[..],             // valid JSON, but not an object
            &b"\"a string\"\n"[..],        // valid JSON scalar
            &b"{partial line no newl"[..], // no trailing newline, unparseable
            &b"\n"[..],                    // a bare blank line
        ] {
            assert_eq!(stamp_line(raw), raw.to_vec(), "{:?} must pass through", raw);
        }
    }

    /// CLAUDE-63: a JSON object line without a trailing newline still gets a
    /// `ts` and stays newline-free (no spurious newline is appended).
    #[test]
    fn stamp_line_no_trailing_newline() {
        let raw = b"{\"type\":\"result\"}";
        let stamped = stamp_line(raw);
        assert_ne!(stamped.last(), Some(&b'\n'), "no newline added");
        let v: Value = serde_json::from_slice(&stamped).unwrap();
        assert!(v.get("ts").is_some());
        assert_eq!(v.get("type").and_then(|t| t.as_str()), Some("result"));
    }

    /// CLAUDE-63: `parse_final_result` still finds the terminal result event in a
    /// stamped log (the added `ts` key does not disturb the `type` lookup).
    #[test]
    fn parse_final_result_finds_result_in_stamped_log() {
        let dir = tempfile::tempdir().unwrap();
        let log = dir.path().join("stream.jsonl");
        let stamped = String::from_utf8(stamp_line(format!("{RESULT_LINE}\n").as_bytes())).unwrap();
        std::fs::write(&log, stamped).unwrap();
        let final_event = parse_final_result(&log).unwrap();
        assert_eq!(
            final_event.get("type").and_then(|t| t.as_str()),
            Some("result")
        );
        assert!(
            final_event.get("ts").is_some(),
            "stamped result keeps its ts"
        );
    }

    /// CLAUDE-107: Claude Code emits trailing `system` events (`task_updated`,
    /// `task_notification`) after the terminal `result` event. The result event
    /// must be selected by `type`, not by line position, so these trailing lines
    /// cannot flip a successful run into a recorded failure.
    #[test]
    fn parse_final_result_skips_trailing_system_events() {
        let dir = tempfile::tempdir().unwrap();
        let log = dir.path().join("stream.jsonl");
        std::fs::write(
            &log,
            format!(
                "{RESULT_LINE}\n\
                 {{\"type\":\"system\",\"subtype\":\"task_updated\"}}\n\
                 {{\"type\":\"system\",\"subtype\":\"task_notification\"}}\n"
            ),
        )
        .unwrap();
        let final_event =
            parse_final_result(&log).expect("result event selected past trailing system events");
        assert_eq!(
            final_event.get("type").and_then(|t| t.as_str()),
            Some("result"),
            "the result event is selected, not the last system line"
        );
        assert_eq!(
            final_event.get("is_error").and_then(|v| v.as_bool()),
            Some(false),
            "the selected result reports success"
        );
    }

    /// CLAUDE-107: a stream with no `result` event anywhere yields `None`, so the
    /// genuine no-terminal-result case keeps its existing failure handling.
    #[test]
    fn parse_final_result_none_without_result_event() {
        let dir = tempfile::tempdir().unwrap();
        let log = dir.path().join("stream.jsonl");
        std::fs::write(
            &log,
            "{\"type\":\"system\",\"subtype\":\"init\"}\n\
             {\"type\":\"assistant\",\"message\":{\"content\":[]}}\n\
             {\"type\":\"system\",\"subtype\":\"task_notification\"}\n",
        )
        .unwrap();
        assert!(
            parse_final_result(&log).is_none(),
            "no result event anywhere yields None"
        );
    }

    /// CLAUDE-63 (smoke): a run through a stub claude that emits several JSON
    /// events produces a stream file where every JSON line carries a `ts` and
    /// the timestamps are monotonically non-decreasing.
    #[test]
    fn invoke_stamps_every_json_line_monotonically() {
        let dir = tempfile::tempdir().unwrap();
        let stub = dir.path().join("claude-stub.sh");
        // Emit an init event, a tool_use, then the terminal result, with a small
        // gap so arrival times advance.
        write_stub(
            &stub,
            "#!/bin/sh\n\
             printf '%s\\n' '{\"type\":\"system\",\"subtype\":\"init\"}'\n\
             printf '%s\\n' '{\"type\":\"assistant\",\"message\":{\"content\":[]}}'\n\
             sleep 0.05\n\
             printf '%s\\n' '{\"type\":\"result\",\"subtype\":\"success\",\"is_error\":false,\"result\":\"done\",\"total_cost_usd\":0.1}'\n\
             exit 0\n",
        );

        let invoker = ClaudeInvoker {
            binary: stub.to_str().unwrap().to_string(),
            result_exit_grace: Duration::from_secs(30),
        };
        let cancel = CancellationToken::new();
        let log = dir.path().join("stream.jsonl");
        let cfg = ClaudeBlock::default();
        let res = invoker
            .invoke(
                "hi",
                &cfg,
                Duration::from_secs(10),
                dir.path(),
                &log,
                &cancel,
            )
            .unwrap();
        assert!(res.ok, "stub run succeeds");

        let raw = std::fs::read_to_string(&log).unwrap();
        let mut prev: Option<chrono::DateTime<chrono::FixedOffset>> = None;
        let mut json_lines = 0;
        for line in raw.lines().filter(|l| !l.trim().is_empty()) {
            let v: Value = serde_json::from_str(line).expect("every line is JSON");
            let ts = v
                .get("ts")
                .and_then(|t| t.as_str())
                .unwrap_or_else(|| panic!("line missing ts: {line}"));
            let parsed = chrono::DateTime::parse_from_rfc3339(ts).expect("ts is RFC 3339");
            if let Some(p) = prev {
                assert!(parsed >= p, "timestamps non-decreasing");
            }
            prev = Some(parsed);
            json_lines += 1;
        }
        assert_eq!(json_lines, 3, "all three emitted events are stamped");
    }

    /// CLAUDE-56: a populated event renders a collapsed, pretty-printed JSON
    /// block; a synthesized (null / empty) event renders nothing.
    #[test]
    fn raw_event_details_collapses_or_omits() {
        let populated = raw_event_details(&json!({"type":"result","is_error":true}));
        assert!(populated.contains("<details>"));
        assert!(populated.contains("<summary>Raw result event</summary>"));
        assert!(populated.contains("```json"));
        // pretty-printed: spans multiple lines.
        assert!(populated.contains("\"type\": \"result\""));

        assert_eq!(raw_event_details(&Value::Null), "");
        assert_eq!(raw_event_details(&json!({})), "");
    }
}
