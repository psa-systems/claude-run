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
use std::process::{Child, Command, ExitStatus, Stdio};
use std::time::{Duration, Instant};

use chrono::SecondsFormat;

use serde::Serialize;
use serde_json::Value;
use tracing::{error, warn};

use crate::action::ClaudeBlock;
use crate::cancel::CancellationToken;
use crate::config::ClaudeConfigHandle;
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
/// CLAUDE-259: the `--effort` levels the `claude` CLI documents. ADVISORY only:
/// a resolved value outside this set is warned about and then passed through, so
/// a level Anthropic adds later needs no runner release. Nothing here rejects or
/// substitutes a value.
pub const EFFORT_LEVELS: [&str; 5] = ["low", "medium", "high", "xhigh", "max"];
/// DEV-706: the compaction window every invocation runs with when neither the
/// action's `claude.autocompact` nor the global default sets one. The `claude`
/// CLI accepts `auto` or 100k-1M tokens; `auto` scales to the model's whole
/// context window, which on a 1M-context model means a long session never
/// compacts and resends every completed operation's tool payloads on every
/// turn. A fixed window bounds what one turn resends, so cumulative spend grows
/// linearly in the number of tool calls rather than quadratically.
pub const DEFAULT_AUTOCOMPACT: &str = "200000";

#[derive(Debug, Clone, Serialize)]
pub struct ClaudeResult {
    pub ok: bool,
    pub summary: String,
    pub cost: f64,
    /// DEV-707: tokens by category, per-tool attribution and context growth,
    /// recovered from this invocation's stream log. Empty when the log could not
    /// be read; accounting never decides a run's outcome.
    pub usage: crate::usage::StreamUsage,
    pub raw: Value,
    pub error: String,
    pub transient: bool,
    /// CLAUDE-52: the `timeout <N>s claude` wrapper SIGKILLed the run (exit 124).
    /// A timeout is distinct from a hard failure: the agent may have completed
    /// all, some, or none of the work before being killed, so the handler
    /// surfaces it as a "Timed Out" outcome rather than "Failed". Always paired
    /// with `ok = false`.
    pub timed_out: bool,
    /// CLAUDE-269: the CLI reported a structured `authentication_failed`, on
    /// this invocation's own terminal event or via three-or-more
    /// `system`/`api_retry` events. Always paired with `transient = true` (so
    /// no attempt is spent) and `ok = false`. Callers that want to react
    /// specifically (rather than just retry later, the generic transient path)
    /// branch on this field.
    pub login_required: bool,
}

impl ClaudeResult {
    pub fn synthesized(error: impl Into<String>, summary: impl Into<String>) -> Self {
        Self {
            ok: false,
            summary: summary.into(),
            cost: 0.0,
            usage: crate::usage::StreamUsage::default(),
            raw: Value::Object(Default::default()),
            error: error.into(),
            transient: false,
            timed_out: false,
            login_required: false,
        }
    }
}

pub struct ClaudeInvoker {
    pub binary: String,
    /// CLAUDE-53: how long to wait for claude to exit on its own after its
    /// terminal `result` event before force-terminating the group. Defaults to
    /// `RESULT_EXIT_GRACE`; tests override it to keep the hang case fast.
    result_exit_grace: Duration,
    /// CLAUDE-165: when set, spawned `claude` children get
    /// `CLAUDE_CONFIG_DIR=<dir>` so they read the slim bot `CLAUDE.md` and the
    /// linked host credentials instead of the operator's global `~/.claude`.
    /// `HOME` is left inherited so git/ssh keep working. `None` (the default and
    /// every test) inherits the ambient environment unchanged.
    config_dir: Option<PathBuf>,
    /// CLAUDE-239: live handle to the global `config.claude` defaults, shared
    /// with `LiveSettings` so a Settings save applies to the next invocation.
    /// `None` (the default) means no global default: the action's own
    /// `claude.model` decides, and with neither set no `--model` flag is passed.
    default_claude: Option<ClaudeConfigHandle>,
    /// Live Anthropic API key from Settings -> Auth, exported to the child as
    /// `ANTHROPIC_API_KEY`. `None` (the default and every test) leaves the
    /// child's auth to whatever its config dir holds.
    api_key: Option<crate::service_auth::ApiKeyHandle>,
    /// CLAUDE-269: tripped when the CLI itself reports an authentication
    /// failure, so the scheduler/dispatcher/webhooks stop starting new Claude
    /// work until a fresh sign-in. `None` (the default and every test that does
    /// not build one) means detection is a no-op: `invoke` still reports
    /// `login_required` on the result, but nothing daemon-wide changes.
    readiness: Option<crate::readiness::ReadinessGate>,
}

impl ClaudeInvoker {
    pub fn new(binary: Option<&str>) -> Self {
        Self {
            binary: binary.unwrap_or("claude").to_string(),
            result_exit_grace: RESULT_EXIT_GRACE,
            config_dir: None,
            default_claude: None,
            api_key: None,
            readiness: None,
        }
    }

    /// CLAUDE-165: route spawned children at `dir` via `CLAUDE_CONFIG_DIR`.
    /// A builder so `new` stays source-compatible with existing callers/tests.
    pub fn with_config_dir(mut self, dir: Option<PathBuf>) -> Self {
        self.config_dir = dir;
        self
    }

    /// CLAUDE-239: take the live global `config.claude` defaults from
    /// [`crate::settings::LiveSettings::claude_handle`]. A builder for the same
    /// reason `with_config_dir` is one: existing callers/tests stay unchanged.
    pub fn with_default_claude(mut self, handle: ClaudeConfigHandle) -> Self {
        self.default_claude = Some(handle);
        self
    }

    /// Live Anthropic API key from the Settings Auth card. When set, every child
    /// gets `ANTHROPIC_API_KEY`, which is how a container authenticates the
    /// agent without an interactive `claude auth login`. An OAuth login in the
    /// agent config dir keeps working and takes over when no key is stored.
    pub fn with_api_key(mut self, handle: Option<crate::service_auth::ApiKeyHandle>) -> Self {
        self.api_key = handle;
        self
    }

    /// CLAUDE-269: the readiness gate this invoker trips when the CLI reports
    /// an authentication failure. A builder for the same reason `with_api_key`
    /// is one: existing callers/tests stay unchanged with detection a no-op.
    pub fn with_readiness(mut self, gate: crate::readiness::ReadinessGate) -> Self {
        self.readiness = Some(gate);
        self
    }

    /// CLAUDE-239: the model this invocation runs with: the action's own
    /// `claude.model`, else the live global default, else `None` (no `--model`
    /// flag, so the `claude` CLI's own default applies). The value is passed
    /// through untouched; the CLI validates it.
    fn resolve_model(&self, claude_cfg: &ClaudeBlock) -> Option<String> {
        claude_cfg.model.clone().or_else(|| {
            let handle = self.default_claude.as_ref()?;
            crate::settings::read_claude(handle).model.clone()
        })
    }

    /// CLAUDE-259: the effort level this invocation runs with, resolved exactly
    /// like the model: the action's own `claude.effort`, else the live global
    /// default, else `None` (no `--effort` flag).
    ///
    /// A value outside [`EFFORT_LEVELS`] is passed through unchanged after one
    /// `warn` naming it, where it came from, and the known set. Never a
    /// rejection and never a substitution: the CLI silently ignores an unknown
    /// level and uses its default, so without this warn a typo would downgrade
    /// effort for every run with nothing in the log to say so.
    fn resolve_effort(&self, claude_cfg: &ClaudeBlock) -> Option<String> {
        let (effort, source) = match claude_cfg.effort.clone() {
            Some(v) => (v, "the action's claude.effort"),
            None => {
                let handle = self.default_claude.as_ref()?;
                let global = crate::settings::read_claude(handle).effort.clone()?;
                (global, "the global config.claude.effort")
            }
        };
        if !EFFORT_LEVELS.contains(&effort.as_str()) {
            warn!(
                effort = %effort,
                source,
                known = %EFFORT_LEVELS.join(", "),
                "unknown claude effort level; passing it through unchanged (the CLI ignores an \
                 unknown value and uses its own default effort)"
            );
        }
        Some(effort)
    }

    /// DEV-706: the compaction window this invocation runs with. Resolved like
    /// the model and the effort level (action, then the live global default),
    /// falling back to [`DEFAULT_AUTOCOMPACT`] rather than to the CLI's own
    /// `auto`, so the bound holds for every action with no per-action config.
    /// Values are passed through unchanged; the CLI owns their validation.
    fn resolve_autocompact(&self, claude_cfg: &ClaudeBlock) -> String {
        claude_cfg
            .autocompact
            .clone()
            .or_else(|| {
                let handle = self.default_claude.as_ref()?;
                crate::settings::read_claude(handle).autocompact.clone()
            })
            .unwrap_or_else(|| DEFAULT_AUTOCOMPACT.to_string())
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
    /// DEV-705: `budget_usd` is what the run has left to spend. It is passed to
    /// the child as `--max-budget-usd`, so ONE runaway session cannot overshoot
    /// the run's ceiling between the runner's own between-invocation checks.
    /// `None` leaves the child uncapped (the pre-DEV-705 command line).
    #[allow(clippy::too_many_arguments)]
    pub fn invoke(
        &self,
        prompt: &str,
        claude_cfg: &ClaudeBlock,
        timeout: Duration,
        default_cwd: &Path,
        stream_log: &Path,
        cancel: &CancellationToken,
        budget_usd: Option<f64>,
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
        let mut command = Command::new("timeout");
        command
            .arg(format!("{}s", timeout.as_secs()))
            .arg(&self.binary)
            .arg("--print")
            .arg("--verbose")
            .arg("--output-format=stream-json")
            .arg(format!("--permission-mode={}", permission_mode))
            .arg(format!("--allowed-tools={}", allowed_tools));
        // CLAUDE-239: the resolved model, if any. Added before the trailing
        // prompt positional; omitted entirely when nothing resolves, which is
        // the pre-CLAUDE-239 command line.
        if let Some(model) = self.resolve_model(claude_cfg) {
            command.arg(format!("--model={}", model));
        }
        // CLAUDE-259: the resolved effort level, on the same terms as the model.
        if let Some(effort) = self.resolve_effort(claude_cfg) {
            command.arg(format!("--effort={}", effort));
        }
        // DEV-706: bound what one turn resends, so a long tool-heavy session
        // compacts instead of carrying every completed operation's payloads.
        command.arg(format!(
            "--autocompact={}",
            self.resolve_autocompact(claude_cfg)
        ));
        // DEV-705: the run's remaining ceiling, enforced inside the session.
        if let Some(budget) = budget_usd {
            command.arg(format!("--max-budget-usd={:.4}", budget.max(0.0)));
        }
        command
            .arg(prompt)
            .current_dir(&cwd)
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::from(log_err))
            .process_group(0);
        // CLAUDE-165: point the agent at its dedicated user-level config dir
        // (slim bot CLAUDE.md + linked host credentials). HOME is left inherited.
        if let Some(dir) = &self.config_dir {
            command.env("CLAUDE_CONFIG_DIR", dir);
        }
        // The Settings Auth card's Anthropic key, when one is stored. Read live
        // so a save applies to the next invocation. A poisoned lock is logged
        // rather than swallowed: the child would otherwise fail to authenticate
        // with no clue why.
        if let Some(handle) = &self.api_key {
            match handle.read() {
                Ok(slot) => {
                    if let Some(key) = slot.as_ref() {
                        command.env("ANTHROPIC_API_KEY", key);
                    }
                }
                Err(e) => {
                    warn!(error = %e, "Anthropic key handle is poisoned; the child runs without ANTHROPIC_API_KEY")
                }
            }
        }
        // CLAUDE-228: the agent session runs for up to `claude_timeout_seconds`
        // (3600 in the shipped actions) and the wait below is a poll loop, so
        // this whole region hands the worker's core back to the runtime. Left
        // inline it pinned one tokio worker per concurrent job for the entire
        // session, which is what starved the sqlx pool.
        let (outcome, relay_error) =
            crate::blocking::in_place(|| -> std::io::Result<(WaitOutcome, Option<String>)> {
                let mut child = command.spawn()?;

                // Relay claude's piped stdout to the stream log, stamping each JSON
                // object line with its arrival time. Runs on its own thread next to
                // the blocking wait below and drains to EOF (the pipe closes when
                // the child group exits, including a timeout/cancel kill, so all
                // output up to the kill is captured).
                let relay = child.stdout.take().map(|stdout| {
                    std::thread::spawn(move || relay_stamped_stdout(stdout, log_file))
                });

                let pgid = child.id() as i32;
                let outcome =
                    wait_or_cancel(&mut child, pgid, stream_log, self.result_exit_grace, cancel)?;

                // Drain the relay: the child group has exited, so the stdout pipe is
                // at EOF and the thread has flushed every line it read up to the kill.
                // CLAUDE-233: both ways the relay can stop early (an I/O error and a
                // panic) surface as a reason string the caller reports; neither is
                // dropped, because either one truncates the stream log.
                let relay_error = match relay.map(|relay| relay.join()) {
                    None | Some(Ok(Ok(()))) => None,
                    Some(Ok(Err(err))) => Some(err.to_string()),
                    Some(Err(_)) => Some("the relay thread panicked".to_string()),
                };
                Ok((outcome, relay_error))
            })?;

        // CLAUDE-269: the wait loop terminated the group after repeated
        // authentication-retry events, well below `claude_timeout_seconds`.
        // Finalize directly from that rather than the signal-induced exit
        // code, and trip the readiness gate so nothing else starts new Claude
        // work until a fresh sign-in.
        if let Some(message) = &outcome.login_required_message {
            if let Some(gate) = &self.readiness {
                gate.trip_login_required(message.clone(), "invocation");
            }
            return Ok(login_required_result(message));
        }

        let status = outcome.status;

        let exit_code = status.code().unwrap_or(-1);
        // CLAUDE-256: a stream log the runner cannot READ decides nothing, so it
        // is reported as the runner's own fault and never as "claude produced no
        // result event", which is a non-transient failure that spends one of the
        // job's `max_attempts_per_key`. A relay error that preceded it is named
        // too: a failed write is the likeliest reason the read now fails.
        let final_event = match parse_final_result(stream_log) {
            Ok(event) => event.unwrap_or(Value::Null),
            Err(err) => {
                let reason = match &relay_error {
                    Some(relay) => format!("{err} (the relay had already failed: {relay})"),
                    None => err.to_string(),
                };
                warn!(
                    stream_log = %stream_log.display(),
                    error = %err,
                    relay_error = relay_error.as_deref().unwrap_or(""),
                    exit_code,
                    "could not read the stream log; the run outcome cannot be recovered from it"
                );
                return Ok(read_failure_result(&reason, stream_log, exit_code));
            }
        };
        let have_result = final_event.get("type").and_then(|v| v.as_str()) == Some("result");
        // DEV-707: the stream log is readable here, so replay it once for this
        // invocation's tokens, per-tool attribution and context growth. Every
        // outcome below carries it, including the failure ones: a run that
        // failed still spent what it spent.
        let usage = crate::usage::parse_stream_usage(stream_log);

        // CLAUDE-233: the relay is the stream log's only writer, so a relay that
        // stopped early means the log is missing every event after that point.
        // With the terminal `result` already captured the run is still decidable
        // and we finalize from it below, noting the gap. Without it the outcome
        // is simply unknown, and the honest report names the runner's own write
        // failure rather than blaming the agent for a silence it did not produce.
        // Transient because a full disk or a failing device is not the job's
        // fault: a retry on a healthy host is exactly the right response, and
        // burning an attempt against `max_attempts_per_key` for it is not.
        if let Some(reason) = &relay_error {
            if have_result {
                warn!(
                    stream_log = %stream_log.display(),
                    reason = %reason,
                    "stream-log relay stopped early; the log is truncated but the terminal result was captured, so the run is finalized from it"
                );
            } else {
                error!(
                    stream_log = %stream_log.display(),
                    reason = %reason,
                    exit_code,
                    "stream-log relay failed before claude's terminal result; the run outcome is unrecoverable"
                );
                return Ok(relay_failure_result(reason, stream_log, exit_code));
            }
        }

        // CLAUDE-53: when we force-terminated a hung-but-done group after its
        // terminal `result` event, the wrapper exits non-zero (SIGTERM 143 /
        // SIGKILL 137). The captured result event is the authoritative outcome,
        // so skip the exit-code failure branch and build from the result below.
        // The exit-code / timeout path is reserved for runs where NO terminal
        // result was ever observed (a genuine hang or crash).
        if exit_code != 0 && !outcome.finalized_on_result {
            // CLAUDE-269: a structured `authentication_failed`, anywhere in the
            // stream (the terminal `result` event or an `assistant` event that
            // preceded it), means the CLI itself is reporting the login is
            // dead. Trip the gate and finalize as transient (no attempt spent)
            // rather than falling into `classify_transient`'s rate-limit check,
            // which would otherwise report this as a plain non-transient
            // failure and burn one of the job's `max_attempts_per_key`.
            if let Ok(Some(message)) = find_authentication_failure(stream_log) {
                if let Some(gate) = &self.readiness {
                    gate.trip_login_required(message.clone(), "invocation");
                }
                return Ok(login_required_result(&message));
            }
            let summary = friendly_failure_summary(&final_event, stream_log);
            return Ok(ClaudeResult {
                ok: false,
                summary,
                cost: final_event
                    .get("total_cost_usd")
                    .and_then(|v| v.as_f64())
                    .unwrap_or(0.0),
                usage,
                raw: final_event.clone(),
                error: format!("claude exited {}, see {}", exit_code, stream_log.display()),
                transient: classify_transient(exit_code, &final_event),
                // CLAUDE-52: exit 124 is the `timeout(1)` wrapper's SIGKILL code.
                timed_out: exit_code == 124,
                login_required: false,
            });
        }

        if !have_result {
            let summary = friendly_failure_summary(&final_event, stream_log);
            return Ok(ClaudeResult {
                ok: false,
                summary,
                cost: 0.0,
                usage,
                raw: final_event,
                error: format!(
                    "claude stream did not end with a result event, see {}",
                    stream_log.display()
                ),
                transient: false,
                timed_out: false,
                login_required: false,
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
            usage,
            error: final_event
                .get("error")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string(),
            raw: final_event,
            transient: false,
            timed_out: false,
            login_required: false,
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
    /// CLAUDE-269: set when the loop force-terminated the group after seeing
    /// [`AUTH_RETRY_LIMIT`] `system`/`api_retry` events carrying
    /// `"error":"authentication_failed"`, far below `claude_timeout_seconds`.
    /// The caller finalizes directly from this rather than the exit code.
    login_required_message: Option<String>,
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
    // CLAUDE-256: this loop polls the log every 100ms, so a persistently
    // unreadable log is reported once rather than a hundred times a second.
    let mut read_error_logged = false;
    loop {
        if let Some(status) = child.try_wait()? {
            return Ok(WaitOutcome {
                status,
                finalized_on_result: false,
                login_required_message: None,
            });
        }
        // Cancellation wins over the result-event path and terminates immediately.
        if cancel.is_cancelled() {
            let status = terminate_group(child, pgid)?;
            return Ok(WaitOutcome {
                status,
                finalized_on_result: false,
                login_required_message: None,
            });
        }
        match result_deadline {
            None => match stream_has_terminal_result(stream_log) {
                Ok(true) => result_deadline = Some(Instant::now() + result_exit_grace),
                Ok(false) => {}
                // CLAUDE-256: an unreadable log is not "no result yet". The
                // grace timer cannot arm from a file we cannot read, so the
                // child runs to its own exit or the `timeout(1)` wrapper; the
                // reason is logged instead of discarded, and `invoke` finalizes
                // the run as a read failure once the child is gone.
                Err(err) => {
                    if !read_error_logged {
                        read_error_logged = true;
                        warn!(
                            stream_log = %stream_log.display(),
                            error = %err,
                            "could not read the stream log while waiting for claude's terminal result; the result-exit grace timer cannot arm"
                        );
                    }
                }
            },
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
                        login_required_message: None,
                    });
                }
            }
        }
        // CLAUDE-269: no terminal result yet (the branch above only arms the
        // grace timer once one lands), so also watch for repeated
        // authentication-retry events. Three is assumed low enough that a
        // single retry during a token-refresh race never trips this, while a
        // genuinely dead login (which the CLI retries indefinitely, per the
        // invalid-API-key case in the issue) is caught far below
        // `claude_timeout_seconds` instead of waiting out the full timeout.
        if result_deadline.is_none() {
            if let Ok(count) = count_auth_retry_events(stream_log) {
                if count >= AUTH_RETRY_LIMIT {
                    let message = find_authentication_failure(stream_log)
                        .ok()
                        .flatten()
                        .unwrap_or_else(|| "authentication_failed".to_string());
                    warn!(
                        pid = child.id(),
                        pgid,
                        count,
                        "repeated authentication-retry events; terminating the process group rather than waiting out the timeout"
                    );
                    let status = terminate_group(child, pgid)?;
                    return Ok(WaitOutcome {
                        status,
                        finalized_on_result: false,
                        login_required_message: Some(message),
                    });
                }
            }
        }
        std::thread::sleep(CANCEL_POLL);
    }
}

/// CLAUDE-269: the number of `system`/`api_retry` events carrying
/// `"error":"authentication_failed"` observed before the child is
/// force-terminated rather than left to exhaust `claude_timeout_seconds`. A
/// single retry during a token-refresh race is not evidence of a dead login;
/// three in a row is.
const AUTH_RETRY_LIMIT: usize = 3;

/// Count `system`/`api_retry` events in the stream log whose top-level `error`
/// is `authentication_failed`. Detection keys ONLY on the structured `error`
/// field, never on human-readable text, which the CLI rewords between
/// releases.
fn count_auth_retry_events(stream_log: &Path) -> std::io::Result<usize> {
    let raw = std::fs::read_to_string(stream_log)?;
    Ok(raw
        .lines()
        .filter(|line| {
            let Ok(event) = serde_json::from_str::<Value>(line) else {
                return false;
            };
            event.get("type").and_then(|v| v.as_str()) == Some("system")
                && event.get("subtype").and_then(|v| v.as_str()) == Some("api_retry")
                && event.get("error").and_then(|v| v.as_str()) == Some("authentication_failed")
        })
        .count())
}

/// CLAUDE-269: scan the whole stream log for the first event (of any type)
/// whose top-level `error` is `authentication_failed`, and pull a
/// human-readable message out of it for the operator-facing banner: the
/// `result` string if present, else the concatenated text of an `assistant`
/// message's content blocks, else the literal `authentication_failed`.
/// Detection keys ONLY on the `error` field.
fn find_authentication_failure(stream_log: &Path) -> std::io::Result<Option<String>> {
    let raw = std::fs::read_to_string(stream_log)?;
    for line in raw.lines() {
        let Ok(event) = serde_json::from_str::<Value>(line) else {
            continue;
        };
        if event.get("error").and_then(|v| v.as_str()) != Some("authentication_failed") {
            continue;
        }
        if let Some(result) = event.get("result").and_then(|v| v.as_str()) {
            if !result.is_empty() {
                return Ok(Some(result.to_string()));
            }
        }
        if let Some(text) = event
            .get("message")
            .and_then(|m| m.get("content"))
            .and_then(|c| c.as_array())
            .map(|blocks| {
                blocks
                    .iter()
                    .filter(|b| b.get("type").and_then(|v| v.as_str()) == Some("text"))
                    .filter_map(|b| b.get("text").and_then(|v| v.as_str()))
                    .collect::<Vec<_>>()
                    .join("")
            })
        {
            if !text.is_empty() {
                return Ok(Some(text));
            }
        }
        return Ok(Some("authentication_failed".to_string()));
    }
    Ok(None)
}

/// Whether the stream log contains a terminal `result` event. CLAUDE-107:
/// `parse_final_result` selects the result event by `type` regardless of
/// position, so a result event followed by trailing `system` events still reads
/// as "terminal result observed" here. A partial trailing write fails to parse
/// and is skipped, so the grace timer only starts once a full `result` event has
/// landed.
///
/// CLAUDE-256: a read failure is carried out rather than read as "no result
/// yet", so the caller can say why the grace timer never armed.
fn stream_has_terminal_result(stream_log: &Path) -> std::io::Result<bool> {
    Ok(parse_final_result(stream_log)?.is_some())
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
///
/// CLAUDE-233: an I/O error is returned, never swallowed. This relay is the
/// stream log's only writer, so stopping early silently discards every event
/// claude emits afterwards, including the terminal `result`, and the run then
/// reads as "claude produced no result" when in fact the runner stopped
/// recording. 33 stream logs on host-a ended that way, in four bursts that
/// line up with the log partition filling: `write_all` returned `ENOSPC` on a
/// short write, the old `break` dropped it, and the jobs were failed for the
/// agent's supposed silence.
///
/// Generic over both ends so the failure paths are unit-testable without a real
/// child process or a real full disk.
fn relay_stamped_stdout<R: std::io::Read, W: Write>(stdout: R, mut sink: W) -> std::io::Result<()> {
    let mut reader = BufReader::new(stdout);
    let mut line = Vec::new();
    loop {
        line.clear();
        if reader.read_until(b'\n', &mut line)? == 0 {
            break; // EOF
        }
        let stamped = stamp_line(&line);
        sink.write_all(&stamped)?;
        sink.flush()?;
    }
    Ok(())
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
/// Returns `Ok(None)` only when the stream was read end to end and contains no
/// `result` event anywhere (the genuine no-terminal-result case the `!have_result`
/// guard catches).
///
/// CLAUDE-256: a read FAILURE comes back as `Err`, never as that same `None`.
/// The two are different facts - the runner could not read the file, versus
/// claude never wrote a result - and collapsing them blamed the agent for a log
/// the runner could not open. `read_to_string` fails with `InvalidData` on a
/// file ending inside a multi-byte UTF-8 sequence, which is exactly what the
/// CLAUDE-233 disk-full truncation produced.
fn parse_final_result(stream_log: &Path) -> std::io::Result<Option<Value>> {
    let raw = std::fs::read_to_string(stream_log)?;
    Ok(raw
        .lines()
        .rev()
        .filter_map(|l| serde_json::from_str::<Value>(l).ok())
        .find(|v| v.get("type").and_then(|t| t.as_str()) == Some("result")))
}

fn read_tail(p: &Path, max: usize) -> std::io::Result<String> {
    let raw = std::fs::read_to_string(p)?;
    if raw.len() <= max {
        Ok(raw)
    } else {
        // CLAUDE-263: `raw.len() - max` can land inside a multi-byte UTF-8
        // character; advance to the next char boundary so the slice never
        // panics. The tail then comes back up to three bytes shorter than
        // `max`, which is the cost of staying inside `max` bytes.
        let mut start = raw.len() - max;
        while !raw.is_char_boundary(start) {
            start += 1;
        }
        Ok(raw[start..].to_string())
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
    // CLAUDE-256: both remaining sources read the same file, so a read failure
    // ends the search here. It used to produce an empty summary
    // (`unwrap_or_default`), which is indistinguishable from an empty log and
    // leaves the operator looking at nothing with no explanation.
    match last_assistant_text(stream_log) {
        Ok(Some(text)) if !text.is_empty() => return text,
        Ok(_) => {}
        Err(err) => return unreadable_log_summary(stream_log, &err),
    }
    match read_tail(stream_log, 2000) {
        Ok(tail) => tail,
        Err(err) => unreadable_log_summary(stream_log, &err),
    }
}

/// CLAUDE-256: the operator-facing summary for a stream log that could not be
/// read. Never empty, and it names the path so the file can be inspected.
fn unreadable_log_summary(stream_log: &Path, err: &std::io::Error) -> String {
    warn!(
        stream_log = %stream_log.display(),
        error = %err,
        "could not read the stream log for the failure summary"
    );
    format!(
        "The runner could not read this run's stream log ({}): {}. \
         No failure detail could be recovered from it.",
        stream_log.display(),
        err
    )
}

/// CLAUDE-233: the outcome reported when the runner's own stream-log relay died
/// before claude's terminal `result` event. The summary has to say three things,
/// because the operator reading it on the issue would otherwise draw the wrong
/// conclusion: the fault is the runner's, the agent's real outcome is unknown
/// rather than bad, and no attempt was spent. The disk hint is named because
/// `ENOSPC` on the log partition is what produced every occurrence so far.
///
/// `transient` is the load-bearing field: it is what keeps a runner-side I/O
/// fault from spending one of the job's `max_attempts_per_key`.
/// CLAUDE-269: the outcome reported when the CLI itself reported an
/// authentication failure (a structured `authentication_failed`, on the
/// terminal result, an `assistant` event, or repeated `api_retry` events).
/// `transient` is the load-bearing field: it is what keeps a dead login from
/// spending one of the job's `max_attempts_per_key`, exactly like the
/// `relay_failure_result` / `read_failure_result` runner-fault outcomes above.
/// `login_required` additionally lets a caller that wants to react
/// specifically (rather than just retry later) branch on it.
fn login_required_result(message: &str) -> ClaudeResult {
    ClaudeResult {
        ok: false,
        summary: format!(
            "Claude login required: {message}. Sign in again at Settings -> Auth; \
             this attempt was not counted, so the job is picked up again once signed in."
        ),
        cost: 0.0,
        usage: crate::usage::StreamUsage::default(),
        raw: Value::Null,
        error: format!("claude reported authentication_failed: {message}"),
        transient: true,
        timed_out: false,
        login_required: true,
    }
}

fn relay_failure_result(reason: &str, stream_log: &Path, exit_code: i32) -> ClaudeResult {
    ClaudeResult {
        ok: false,
        summary: format!(
            "The runner could not write this run's stream log ({}): {}. \
             Recording stopped there, so every later event, including claude's terminal result, was lost. \
             The agent session itself may well have succeeded; its outcome is unknown, not failed. \
             Check free space on the log partition (a full disk is the usual cause). \
             This attempt was not counted, so the job is picked up again on the next poll.",
            stream_log.display(),
            reason
        ),
        cost: 0.0,
        usage: crate::usage::StreamUsage::default(),
        raw: Value::Null,
        error: format!(
            "the runner could not write the stream log {}: {}",
            stream_log.display(),
            reason
        ),
        transient: true,
        // CLAUDE-52: exit 124 is the `timeout(1)` wrapper's SIGKILL code.
        timed_out: exit_code == 124,
        login_required: false,
    }
}

/// CLAUDE-256: the outcome reported when the runner could not READ the stream
/// log it just wrote. Same shape and same reasoning as `relay_failure_result`:
/// the fault is the runner's, claude's real outcome is unknown rather than bad,
/// and `transient` is what stops a file the runner could not read from spending
/// one of the job's `max_attempts_per_key`. Without it the run finalized as
/// "claude stream did not end with a result event", a non-transient failure
/// pinned on the agent.
fn read_failure_result(reason: &str, stream_log: &Path, exit_code: i32) -> ClaudeResult {
    ClaudeResult {
        ok: false,
        summary: format!(
            "The runner could not read this run's stream log ({}): {}. \
             Claude's outcome is recorded only in that file, so it is unknown, not failed. \
             A log truncated mid-character by a full disk, a permissions problem, or an I/O error on the log partition all read this way. \
             This attempt was not counted, so the job is picked up again on the next poll.",
            stream_log.display(),
            reason
        ),
        cost: 0.0,
        usage: crate::usage::StreamUsage::default(),
        raw: Value::Null,
        error: format!(
            "the runner could not read the stream log {}: {}",
            stream_log.display(),
            reason
        ),
        transient: true,
        // CLAUDE-52: exit 124 is the `timeout(1)` wrapper's SIGKILL code.
        timed_out: exit_code == 124,
        login_required: false,
    }
}

/// Scan the stream log backwards for the last `assistant` message event and
/// concatenate the `text` of its text content blocks. CLAUDE-256: `Ok(None)`
/// means the log was read and holds no assistant message; a read failure is
/// `Err`, so the caller can say so instead of silently degrading the summary.
fn last_assistant_text(stream_log: &Path) -> std::io::Result<Option<String>> {
    let raw = std::fs::read_to_string(stream_log)?;
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
        return Ok(Some(text));
    }
    Ok(None)
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
    use crate::testutil::write_stub;
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
    /// Ensure a coreutils-faithful `timeout` is first on PATH for the test
    /// process. Production resolves `timeout` via PATH and relies on its
    /// exit-124-on-timeout contract (a documented coreutils host dependency).
    /// The pre-commit dev image (`rust-builder-musl:*-alpine`) ships only
    /// busybox `timeout`, which exits 143 (SIGTERM) instead of 124 on timeout,
    /// so a test that asserts the 124 contract is non-deterministic across hosts.
    /// We install a tiny POSIX-sh shim that reproduces coreutils' behaviour
    /// (TERM after N seconds, exit 124 if it had to kill) and prepend it to PATH
    /// once, so the exit-code contract is exercised deterministically regardless
    /// of which `timeout` the host happens to ship. For non-timeout runs the shim
    /// forwards the child's own exit status, so other `invoke` tests are
    /// unaffected.
    fn ensure_coreutils_timeout_on_path() {
        use std::sync::OnceLock;

        static SHIM_DIR: OnceLock<PathBuf> = OnceLock::new();
        let dir = SHIM_DIR.get_or_init(|| {
            let dir = std::env::temp_dir().join("claude-run-test-timeout-shim");
            std::fs::create_dir_all(&dir).unwrap();
            let shim = dir.join("timeout");
            // Parse a leading `<N>s` (or bare `<N>`) duration like coreutils, run
            // the command in the background, and SIGTERM it after the budget. A
            // marker file records that the watchdog fired: exit 124 when it did
            // (the run was killed for exceeding the budget), otherwise forward the
            // child's own exit status. This reproduces coreutils' contract that
            // the only `invoke` test asserting on it (`invoke_timeout_sets_timed_out`)
            // depends on.
            let body = r#"#!/bin/sh
secs=$1; shift
secs=${secs%s}
fired="$(mktemp)"
rm -f "$fired"
"$@" &
child=$!
( sleep "$secs"; : > "$fired"; kill -TERM "$child" 2>/dev/null; sleep 1; kill -KILL "$child" 2>/dev/null ) &
watcher=$!
wait "$child"
code=$?
kill "$watcher" 2>/dev/null
wait "$watcher" 2>/dev/null
if [ -e "$fired" ]; then
    rm -f "$fired"
    exit 124
fi
rm -f "$fired"
exit "$code"
"#;
            write_stub(&shim, body);
            dir
        });
        let current = std::env::var_os("PATH").unwrap_or_default();
        let prefix = dir.as_os_str().to_owned();
        // Idempotent: only prepend if our shim dir is not already leading.
        let already = std::env::split_paths(&current).next().map(|p| p == *dir);
        if already != Some(true) {
            let mut paths = vec![dir.clone()];
            paths.extend(std::env::split_paths(&current));
            let joined = std::env::join_paths(paths).unwrap();
            // Safe under edition 2021; the init is effectively single-shot and
            // only ever prepends the same shim dir.
            std::env::set_var("PATH", &joined);
            let _ = prefix;
        }
    }

    #[test]
    fn invoke_timeout_sets_timed_out() {
        // Pin a coreutils-faithful `timeout` (exit 124 on timeout) so the assertion
        // holds even on hosts whose `timeout` is busybox (exits 143). See the
        // helper for the full rationale.
        ensure_coreutils_timeout_on_path();

        let dir = tempfile::tempdir().unwrap();
        // Stub "claude": sleep well past the 1s budget so the wrapper kills it.
        let stub = dir.path().join("claude-stub.sh");
        write_stub(&stub, "#!/bin/sh\nexec sleep 30\n");

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
                None,
            )
            .unwrap();
        assert!(res.timed_out, "exit 124 should set timed_out");
        assert!(!res.ok, "a timeout is not ok");
        assert!(!res.transient, "a timeout is never transient");
    }

    /// CLAUDE-269 acceptance criterion 1: a stub that emits an `assistant`
    /// event with `"error":"authentication_failed"` and a terminal `result`
    /// with `is_error: true`, exiting 1, makes `invoke` return `transient: true`
    /// with `login_required` flagged, and trips the gate.
    #[test]
    fn invoke_detects_authentication_failed_and_trips_the_gate() {
        let dir = tempfile::tempdir().unwrap();
        let stub = dir.path().join("claude-stub.sh");
        write_stub(
            &stub,
            "#!/bin/sh\n\
             printf '%s\\n' '{\"type\":\"assistant\",\"error\":\"authentication_failed\",\"message\":{\"content\":[{\"type\":\"text\",\"text\":\"Failed to authenticate: OAuth session expired\"}]}}'\n\
             printf '%s\\n' '{\"type\":\"result\",\"is_error\":true,\"error\":\"authentication_failed\"}'\n\
             exit 1\n",
        );

        let readiness = crate::readiness::ReadinessGate::new(dir.path().to_path_buf(), None);
        let invoker =
            ClaudeInvoker::new(Some(stub.to_str().unwrap())).with_readiness(readiness.clone());
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
                None,
            )
            .unwrap();
        assert!(res.transient, "an auth failure must not spend an attempt");
        assert!(res.login_required, "login_required must be flagged");
        assert!(!res.ok);
        assert!(
            readiness.is_login_required(),
            "the readiness gate must be tripped"
        );
        assert_eq!(
            readiness.check().kind,
            Some(crate::readiness::NotReadyKind::LoginRequired)
        );
    }

    /// CLAUDE-269 acceptance criterion 2: a stub that emits repeated
    /// `system`/`api_retry` events with `"error":"authentication_failed"` and
    /// never exits is terminated after the third such event, far below its
    /// configured timeout.
    #[test]
    fn invoke_terminates_after_the_third_auth_retry_event_well_below_timeout() {
        let dir = tempfile::tempdir().unwrap();
        let stub = dir.path().join("claude-stub.sh");
        write_stub(
            &stub,
            "#!/bin/sh\n\
             i=0\n\
             while [ $i -lt 6 ]; do\n\
               printf '%s\\n' '{\"type\":\"system\",\"subtype\":\"api_retry\",\"error\":\"authentication_failed\"}'\n\
               i=$((i+1))\n\
               sleep 0.05\n\
             done\n\
             sleep 30\n",
        );

        let readiness = crate::readiness::ReadinessGate::new(dir.path().to_path_buf(), None);
        let invoker =
            ClaudeInvoker::new(Some(stub.to_str().unwrap())).with_readiness(readiness.clone());
        let cancel = CancellationToken::new();
        let log = dir.path().join("stream.jsonl");
        let cfg = ClaudeBlock::default();
        let start = Instant::now();
        let res = invoker
            .invoke(
                "hi",
                &cfg,
                // Configured timeout the fix must beat by a wide margin.
                Duration::from_secs(30),
                dir.path(),
                &log,
                &cancel,
                None,
            )
            .unwrap();
        let elapsed = start.elapsed();
        assert!(res.transient);
        assert!(res.login_required);
        assert!(
            elapsed < Duration::from_secs(5),
            "should terminate on the third retry, not wait out the 30s timeout (took {elapsed:?})"
        );
        assert!(readiness.is_login_required());
    }

    const RESULT_LINE: &str = r#"{"type":"result","subtype":"success","is_error":false,"result":"done","total_cost_usd":0.42}"#;

    /// CLAUDE-165: when the invoker carries a `config_dir`, the spawned child
    /// gets `CLAUDE_CONFIG_DIR=<dir>` and still inherits `HOME` unchanged. The
    /// stub records both env vars as seen by the child so the assertion checks
    /// the real spawn path, not a rebuilt Command.
    #[test]
    fn invoke_sets_config_dir_env_and_inherits_home() {
        let dir = tempfile::tempdir().unwrap();
        let marker = dir.path().join("child-env.txt");
        let stub = dir.path().join("claude-stub.sh");
        write_stub(
            &stub,
            &format!(
                "#!/bin/sh\n\
                 printf 'CFG=%s\\nHOME=%s\\n' \"$CLAUDE_CONFIG_DIR\" \"$HOME\" > '{}'\n\
                 printf '%s\\n' '{RESULT_LINE}'\n\
                 exit 0\n",
                marker.display()
            ),
        );

        let cfg_dir = dir.path().join("claude-agent-home");
        let invoker =
            ClaudeInvoker::new(Some(stub.to_str().unwrap())).with_config_dir(Some(cfg_dir.clone()));
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
                None,
            )
            .unwrap();
        assert!(res.ok, "stub run succeeds");

        let seen = std::fs::read_to_string(&marker).unwrap();
        assert!(
            seen.contains(&format!("CFG={}", cfg_dir.display())),
            "child must see CLAUDE_CONFIG_DIR=<dir>, got:\n{seen}"
        );
        let expected_home = std::env::var("HOME").unwrap_or_default();
        assert!(
            seen.contains(&format!("HOME={expected_home}")),
            "child must inherit HOME unchanged, got:\n{seen}"
        );
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
            config_dir: None,
            default_claude: None,
            api_key: None,
            readiness: None,
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
                None,
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
            config_dir: None,
            default_claude: None,
            api_key: None,
            readiness: None,
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
                None,
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
            config_dir: None,
            default_claude: None,
            api_key: None,
            readiness: None,
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
                None,
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
        let dir = tempfile::tempdir().unwrap();
        // Stub "claude": ignore the flags invoke passes and sleep well past the
        // test budget. SIGTERM to the group ends it immediately.
        let stub = dir.path().join("claude-stub.sh");
        write_stub(&stub, "#!/bin/sh\nexec sleep 30\n");

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
            None,
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

    // ---- CLAUDE-239: model selection ---------------------------------------

    /// Run a stub that records the exact argv it was spawned with, and return
    /// those arguments one per line. Asserts on the real spawn path rather than
    /// on a rebuilt `Command`.
    fn spawn_and_capture_args(invoker: &ClaudeInvoker, cfg: &ClaudeBlock, dir: &Path) -> String {
        spawn_and_capture_args_with_budget(invoker, cfg, dir, None)
    }

    /// DEV-705: the same helper with an explicit remaining-budget argument.
    fn spawn_and_capture_args_with_budget(
        invoker: &ClaudeInvoker,
        cfg: &ClaudeBlock,
        dir: &Path,
        budget_usd: Option<f64>,
    ) -> String {
        let marker = dir.join("child-argv.txt");
        let res = invoker
            .invoke(
                "hi",
                cfg,
                Duration::from_secs(10),
                dir,
                &dir.join("stream.jsonl"),
                &CancellationToken::new(),
                budget_usd,
            )
            .unwrap();
        assert!(res.ok, "stub run succeeds");
        std::fs::read_to_string(&marker).unwrap()
    }

    /// Stub that writes its argv (one per line) to `child-argv.txt` next to it,
    /// then emits a terminal success result event.
    fn argv_recording_stub(dir: &Path) -> PathBuf {
        let stub = dir.join("claude-stub.sh");
        write_stub(
            &stub,
            &format!(
                "#!/bin/sh\n\
                 for a in \"$@\"; do printf '%s\\n' \"$a\"; done > '{}'\n\
                 printf '%s\\n' '{RESULT_LINE}'\n\
                 exit 0\n",
                dir.join("child-argv.txt").display()
            ),
        );
        stub
    }

    fn claude_defaults(model: Option<&str>) -> crate::config::ClaudeConfigHandle {
        std::sync::Arc::new(std::sync::RwLock::new(crate::config::ClaudeConfig {
            model: model.map(|m| m.to_string()),
            effort: None,
            autocompact: None,
        }))
    }

    /// CLAUDE-259: the same handle with a global default effort level.
    fn effort_defaults(effort: Option<&str>) -> crate::config::ClaudeConfigHandle {
        std::sync::Arc::new(std::sync::RwLock::new(crate::config::ClaudeConfig {
            model: None,
            effort: effort.map(|e| e.to_string()),
            autocompact: None,
        }))
    }

    /// Neither the action nor the global default names a model: no `--model`
    /// flag is passed at all, which is the pre-CLAUDE-239 command line.
    #[test]
    fn invoke_omits_model_flag_when_nothing_is_set() {
        let dir = tempfile::tempdir().unwrap();
        let stub = argv_recording_stub(dir.path());
        let invoker = ClaudeInvoker::new(Some(stub.to_str().unwrap()))
            .with_default_claude(claude_defaults(None));
        let argv = spawn_and_capture_args(&invoker, &ClaudeBlock::default(), dir.path());
        assert!(
            !argv.contains("--model"),
            "no model anywhere must pass no --model flag, got:\n{argv}"
        );
    }

    /// Global default set, action silent: the global model is passed through.
    #[test]
    fn invoke_uses_the_global_default_model() {
        let dir = tempfile::tempdir().unwrap();
        let stub = argv_recording_stub(dir.path());
        let invoker = ClaudeInvoker::new(Some(stub.to_str().unwrap()))
            .with_default_claude(claude_defaults(Some("sonnet")));
        let argv = spawn_and_capture_args(&invoker, &ClaudeBlock::default(), dir.path());
        assert!(
            argv.lines().any(|l| l == "--model=sonnet"),
            "the global default must reach the CLI, got:\n{argv}"
        );
    }

    /// The action's own model wins over the global default, and an arbitrary
    /// model id is passed through untouched (the CLI validates it, not us).
    #[test]
    fn invoke_action_model_overrides_the_global_default() {
        let dir = tempfile::tempdir().unwrap();
        let stub = argv_recording_stub(dir.path());
        let invoker = ClaudeInvoker::new(Some(stub.to_str().unwrap()))
            .with_default_claude(claude_defaults(Some("sonnet")));
        let cfg = ClaudeBlock {
            model: Some("claude-opus-4-5-20251101".to_string()),
            ..Default::default()
        };
        let argv = spawn_and_capture_args(&invoker, &cfg, dir.path());
        assert!(
            argv.lines()
                .any(|l| l == "--model=claude-opus-4-5-20251101"),
            "the action model must win and pass through untouched, got:\n{argv}"
        );
        assert!(
            !argv.contains("--model=sonnet"),
            "the global default must not also be passed, got:\n{argv}"
        );
        // The prompt stays the trailing positional, after every flag.
        assert_eq!(argv.lines().last(), Some("hi"), "argv:\n{argv}");
    }

    /// DEV-706: with nothing configured the invocation still runs with a
    /// BOUNDED compaction window, so a tool-heavy session cannot resend every
    /// completed operation's payloads on every turn.
    #[test]
    fn invoke_always_passes_a_bounded_autocompact_window() {
        let dir = tempfile::tempdir().unwrap();
        let stub = argv_recording_stub(dir.path());
        let invoker = ClaudeInvoker::new(Some(stub.to_str().unwrap()));
        let argv = spawn_and_capture_args(&invoker, &ClaudeBlock::default(), dir.path());
        assert!(
            argv.lines()
                .any(|l| l == format!("--autocompact={DEFAULT_AUTOCOMPACT}")),
            "an unconfigured action must still compact, got:\n{argv}"
        );
    }

    /// DEV-706: the action's window wins over the global default, and both win
    /// over the runner default. Values pass through untouched.
    #[test]
    fn invoke_autocompact_resolves_action_then_global_then_default() {
        let dir = tempfile::tempdir().unwrap();
        let stub = argv_recording_stub(dir.path());
        let handle = claude_defaults(None);
        handle.write().unwrap().autocompact = Some("400000".to_string());
        let invoker =
            ClaudeInvoker::new(Some(stub.to_str().unwrap())).with_default_claude(handle.clone());

        let argv = spawn_and_capture_args(&invoker, &ClaudeBlock::default(), dir.path());
        assert!(
            argv.lines().any(|l| l == "--autocompact=400000"),
            "the global default must reach the CLI, got:\n{argv}"
        );

        let cfg = ClaudeBlock {
            autocompact: Some("auto".to_string()),
            ..Default::default()
        };
        let argv = spawn_and_capture_args(&invoker, &cfg, dir.path());
        assert!(
            argv.lines().any(|l| l == "--autocompact=auto"),
            "the action window must win, got:\n{argv}"
        );
        assert!(
            !argv.contains("--autocompact=400000"),
            "the global default must not also be passed, got:\n{argv}"
        );
    }

    /// DEV-705: the run's remaining ceiling reaches the child, so one session
    /// cannot overshoot it between the runner's own between-invocation checks.
    /// `None` (the pre-DEV-705 command line) passes no flag at all.
    #[test]
    fn invoke_passes_the_remaining_budget_as_max_budget_usd() {
        let dir = tempfile::tempdir().unwrap();
        let stub = argv_recording_stub(dir.path());
        let invoker = ClaudeInvoker::new(Some(stub.to_str().unwrap()));
        let cfg = ClaudeBlock::default();

        let argv = spawn_and_capture_args(&invoker, &cfg, dir.path());
        assert!(
            !argv.contains("--max-budget-usd"),
            "no budget means no flag, got:\n{argv}"
        );

        let argv = spawn_and_capture_args_with_budget(&invoker, &cfg, dir.path(), Some(1.5));
        assert!(
            argv.lines().any(|l| l == "--max-budget-usd=1.5000"),
            "the remaining budget must reach the CLI, got:\n{argv}"
        );
        // An exhausted ledger never reports a negative ceiling.
        let argv = spawn_and_capture_args_with_budget(&invoker, &cfg, dir.path(), Some(-3.0));
        assert!(
            argv.lines().any(|l| l == "--max-budget-usd=0.0000"),
            "an exhausted budget floors at zero, got:\n{argv}"
        );
    }

    /// A live save through the shared handle is picked up by the next
    /// invocation: the invoker reads the handle per call, never a startup copy.
    #[test]
    fn invoke_reads_the_global_default_per_invocation() {
        let dir = tempfile::tempdir().unwrap();
        let stub = argv_recording_stub(dir.path());
        let handle = claude_defaults(None);
        let invoker =
            ClaudeInvoker::new(Some(stub.to_str().unwrap())).with_default_claude(handle.clone());
        let argv = spawn_and_capture_args(&invoker, &ClaudeBlock::default(), dir.path());
        assert!(!argv.contains("--model"), "argv:\n{argv}");

        handle.write().unwrap().model = Some("opus".to_string());
        let argv = spawn_and_capture_args(&invoker, &ClaudeBlock::default(), dir.path());
        assert!(
            argv.lines().any(|l| l == "--model=opus"),
            "a settings save must apply to the next invocation, got:\n{argv}"
        );
    }

    // ---- CLAUDE-259: effort selection --------------------------------------

    /// Collects the log lines emitted while a closure runs, so the warn path can
    /// be asserted on rather than assumed.
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

    /// Neither the action nor the global default names an effort: no `--effort`
    /// flag at all, which is the pre-CLAUDE-259 command line.
    #[test]
    fn invoke_omits_effort_flag_when_nothing_is_set() {
        let dir = tempfile::tempdir().unwrap();
        let stub = argv_recording_stub(dir.path());
        let invoker = ClaudeInvoker::new(Some(stub.to_str().unwrap()))
            .with_default_claude(effort_defaults(None));
        let argv = spawn_and_capture_args(&invoker, &ClaudeBlock::default(), dir.path());
        assert!(
            !argv.contains("--effort"),
            "no effort anywhere must pass no --effort flag, got:\n{argv}"
        );
    }

    /// Global default set, action silent: the global effort is passed through.
    #[test]
    fn invoke_uses_the_global_default_effort() {
        let dir = tempfile::tempdir().unwrap();
        let stub = argv_recording_stub(dir.path());
        let invoker = ClaudeInvoker::new(Some(stub.to_str().unwrap()))
            .with_default_claude(effort_defaults(Some("low")));
        let argv = spawn_and_capture_args(&invoker, &ClaudeBlock::default(), dir.path());
        assert!(
            argv.lines().any(|l| l == "--effort=low"),
            "the global default must reach the CLI, got:\n{argv}"
        );
    }

    /// The action's own effort wins over the global default, and the prompt
    /// stays the trailing positional after every flag.
    #[test]
    fn invoke_action_effort_overrides_the_global_default() {
        let dir = tempfile::tempdir().unwrap();
        let stub = argv_recording_stub(dir.path());
        let invoker = ClaudeInvoker::new(Some(stub.to_str().unwrap()))
            .with_default_claude(effort_defaults(Some("low")));
        let cfg = ClaudeBlock {
            effort: Some("xhigh".to_string()),
            ..Default::default()
        };
        let argv = spawn_and_capture_args(&invoker, &cfg, dir.path());
        assert!(
            argv.lines().any(|l| l == "--effort=xhigh"),
            "the action effort must win, got:\n{argv}"
        );
        assert!(
            !argv.contains("--effort=low"),
            "the global default must not also be passed, got:\n{argv}"
        );
        assert_eq!(argv.lines().last(), Some("hi"), "argv:\n{argv}");
    }

    /// An effort outside the documented set is warned about exactly once, naming
    /// the value and the known set, and is then passed through unchanged: no
    /// layer rejects it and no layer substitutes a different value.
    #[test]
    fn invoke_warns_once_and_passes_an_unknown_effort_through() {
        let dir = tempfile::tempdir().unwrap();
        let stub = argv_recording_stub(dir.path());
        let invoker = ClaudeInvoker::new(Some(stub.to_str().unwrap()))
            .with_default_claude(effort_defaults(None));
        let cfg = ClaudeBlock {
            effort: Some("colossal".to_string()),
            ..Default::default()
        };
        let (argv, logs) = capture_logs(|| spawn_and_capture_args(&invoker, &cfg, dir.path()));
        assert!(
            argv.lines().any(|l| l == "--effort=colossal"),
            "the unknown value must pass through unchanged, got:\n{argv}"
        );
        assert_eq!(
            logs.matches("unknown claude effort level").count(),
            1,
            "exactly one warn, got:\n{logs}"
        );
        assert!(logs.contains("WARN"), "at warn level, got:\n{logs}");
        assert!(logs.contains("colossal"), "naming the value, got:\n{logs}");
        assert!(
            logs.contains("low, medium, high, xhigh, max"),
            "and the known set, got:\n{logs}"
        );

        // A documented level is silent.
        let cfg = ClaudeBlock {
            effort: Some("max".to_string()),
            ..Default::default()
        };
        let (_, logs) = capture_logs(|| spawn_and_capture_args(&invoker, &cfg, dir.path()));
        assert!(
            !logs.contains("unknown claude effort level"),
            "a known level must not warn, got:\n{logs}"
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
        let final_event = parse_final_result(&log).unwrap().unwrap();
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
        let final_event = parse_final_result(&log).unwrap().unwrap();
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
        let final_event = parse_final_result(&log).unwrap().unwrap_or(Value::Null);
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
        let final_event = parse_final_result(&log).unwrap().unwrap_or(Value::Null);
        assert_eq!(
            friendly_failure_summary(&final_event, &log),
            "{\"type\":\"system\",\"subtype\":\"init\"}\n"
        );
    }

    /// CLAUDE-263: the tail fallback used to slice the raw log at a fixed byte
    /// offset from the end, which panics when that offset lands inside a
    /// multi-byte character. This log is over the 2000-byte cap with a euro
    /// sign (3 bytes) straddling the cut point, so a fix that just clamped
    /// the index instead of walking to a char boundary would still panic.
    #[test]
    fn friendly_summary_tail_cut_inside_multibyte_char_does_not_panic() {
        let dir = tempfile::tempdir().unwrap();
        let log = dir.path().join("stream.jsonl");
        // Length 2003: the cut at len - 2000 = byte 3 falls inside the euro
        // sign, which occupies bytes 1..4.
        let raw = format!("a€{}", "a".repeat(1999));
        assert_eq!(raw.len(), 2003);
        std::fs::write(&log, &raw).unwrap();
        // CLAUDE-107: no result event, so the caller falls back to `Value::Null`.
        let final_event = parse_final_result(&log).unwrap().unwrap_or(Value::Null);
        let summary = friendly_failure_summary(&final_event, &log);
        assert_eq!(summary, "a".repeat(1999));
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
            config_dir: None,
            default_claude: None,
            api_key: None,
            readiness: None,
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
                None,
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
        let final_event = parse_final_result(&log).unwrap().unwrap();
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
        let final_event = parse_final_result(&log)
            .unwrap()
            .expect("result event selected past trailing system events");
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
            parse_final_result(&log).unwrap().is_none(),
            "no result event anywhere yields None"
        );
    }

    /// CLAUDE-233: a sink that accepts `remaining` bytes and then fails,
    /// modelling the log partition filling mid-run. The real kernel behaviour it
    /// stands in for is a short buffered write that lands on a page boundary and
    /// then returns `ENOSPC`, which is what every truncated stream log on
    /// host-a shows: the file ends mid-line at an exact multiple of 4096.
    struct FullDisk {
        remaining: usize,
    }

    impl Write for FullDisk {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            if self.remaining == 0 {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::StorageFull,
                    "No space left on device",
                ));
            }
            let n = buf.len().min(self.remaining);
            self.remaining -= n;
            Ok(n)
        }

        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    const RELAY_EVENTS: &str = "{\"type\":\"system\",\"subtype\":\"init\"}\n\
                                {\"type\":\"assistant\",\"message\":{\"content\":[]}}\n\
                                {\"type\":\"result\",\"subtype\":\"success\",\"is_error\":false,\"result\":\"done\"}\n";

    /// CLAUDE-233: the relay reports a write failure instead of stopping
    /// silently. The old `if sink.write_all(..).is_err() { break }` discarded the
    /// error, so a full disk truncated the stream log with no log line anywhere
    /// and the run was failed for "no result event" that the runner had simply
    /// stopped recording.
    #[test]
    fn relay_reports_a_write_failure_instead_of_truncating_silently() {
        let err = relay_stamped_stdout(RELAY_EVENTS.as_bytes(), FullDisk { remaining: 40 })
            .expect_err("a sink that runs out of space must surface the error");
        assert_eq!(
            err.kind(),
            std::io::ErrorKind::StorageFull,
            "the underlying cause reaches the caller unchanged, not a generic string"
        );
    }

    /// CLAUDE-233: the same for a read failure on the child's stdout. Both of the
    /// relay's `break`-on-error arms discarded the cause.
    #[test]
    fn relay_reports_a_read_failure_instead_of_truncating_silently() {
        struct FailingRead;
        impl std::io::Read for FailingRead {
            fn read(&mut self, _buf: &mut [u8]) -> std::io::Result<usize> {
                Err(std::io::Error::new(
                    std::io::ErrorKind::BrokenPipe,
                    "pipe went away",
                ))
            }
        }
        let err = relay_stamped_stdout(FailingRead, Vec::new())
            .expect_err("a failing read must surface the error");
        assert_eq!(err.kind(), std::io::ErrorKind::BrokenPipe);
    }

    /// CLAUDE-233: the success path is unchanged: drain to EOF, write every
    /// stamped line, report `Ok`.
    #[test]
    fn relay_reports_ok_when_it_drains_to_eof() {
        let mut sink = Vec::new();
        relay_stamped_stdout(RELAY_EVENTS.as_bytes(), &mut sink)
            .expect("clean EOF is not an error");
        assert_eq!(
            String::from_utf8(sink).unwrap().lines().count(),
            3,
            "every event reaches the sink"
        );
    }

    /// CLAUDE-233: a relay failure is reported as the runner's own I/O fault and
    /// is transient, so it does NOT spend one of the job's
    /// `max_attempts_per_key`. The three 2026-08-11 BUNYIP runs each burned an
    /// attempt and were moved to `Failed` for this condition; `transient` is what
    /// prevents that, so it is asserted explicitly rather than inferred.
    #[test]
    fn relay_failure_is_transient_and_names_the_runner_as_the_cause() {
        let result = relay_failure_result(
            "No space left on device (os error 28)",
            Path::new("/logs/YouTrack/streams/BUNYIP-511-attempt-1.jsonl"),
            0,
        );
        assert!(!result.ok, "the run did not succeed");
        assert!(
            result.transient,
            "a runner-side write failure must not count an attempt against the job"
        );
        assert!(!result.timed_out, "exit 0 is not a timeout");
        assert!(
            result.error.contains("No space left on device"),
            "the underlying cause is named, got: {}",
            result.error
        );
        assert!(
            result.error.contains("BUNYIP-511-attempt-1.jsonl"),
            "the affected stream log is named, got: {}",
            result.error
        );
        assert!(
            !result.error.contains("did not end with a result event"),
            "the failure is no longer attributed to claude, got: {}",
            result.error
        );
        assert!(
            result.summary.contains("could not write")
                && result.summary.contains("outcome is unknown"),
            "the operator-facing summary names the runner and does not call the session failed, got: {}",
            result.summary
        );
    }

    /// CLAUDE-233: a relay failure that happens to coincide with the `timeout(1)`
    /// wrapper's SIGKILL still reports as a timeout, so the "Timed Out" outcome
    /// (CLAUDE-52) is not silently reclassified.
    #[test]
    fn relay_failure_preserves_the_timeout_flag() {
        let result = relay_failure_result("write failed", Path::new("/logs/s.jsonl"), 124);
        assert!(result.timed_out, "exit 124 is still surfaced as a timeout");
    }

    /// CLAUDE-256: a stream log ending inside a multi-byte UTF-8 sequence (what a
    /// disk-full short write at a page boundary leaves behind) fails to read.
    /// That failure must not produce the same value as a log that was read end to
    /// end and holds no `result` event.
    #[test]
    fn parse_final_result_reports_a_read_failure_instead_of_no_result_event() {
        let dir = tempfile::tempdir().unwrap();

        let truncated = dir.path().join("truncated.jsonl");
        let mut bytes = format!("{RESULT_LINE}\n").into_bytes();
        // A euro sign cut after its second byte: valid so far, incomplete.
        bytes.extend_from_slice(&[0xe2, 0x82]);
        std::fs::write(&truncated, &bytes).unwrap();
        let err = parse_final_result(&truncated).expect_err("an unreadable log is an error");
        assert_eq!(err.kind(), std::io::ErrorKind::InvalidData);

        let no_result = dir.path().join("no-result.jsonl");
        std::fs::write(&no_result, "{\"type\":\"system\",\"subtype\":\"init\"}\n").unwrap();
        assert!(
            parse_final_result(&no_result).unwrap().is_none(),
            "a readable log with no result event is still Ok(None)"
        );
    }

    /// CLAUDE-256: the end-to-end outcome. A run whose stream log cannot be read
    /// finalizes as a runner-side read failure - transient, so it spends none of
    /// the job's `max_attempts_per_key` - and NOT as the non-transient "claude
    /// stream did not end with a result event", which blamed the agent for a file
    /// the runner could not open.
    #[test]
    fn invoke_reports_an_unreadable_stream_log_as_a_transient_read_failure() {
        let dir = tempfile::tempdir().unwrap();
        let stub = dir.path().join("claude-stub.sh");
        // Emit one clean event, then a line cut inside a multi-byte character, so
        // the log on disk is exactly the shape CLAUDE-233 found on host-a.
        write_stub(
            &stub,
            "#!/bin/sh\n\
             printf '%s\\n' '{\"type\":\"system\",\"subtype\":\"init\"}'\n\
             printf 'truncated: \\342\\202'\n\
             exit 0\n",
        );

        let invoker = ClaudeInvoker {
            binary: stub.to_str().unwrap().to_string(),
            result_exit_grace: Duration::from_secs(30),
            config_dir: None,
            default_claude: None,
            api_key: None,
            readiness: None,
        };
        let cancel = CancellationToken::new();
        let log = dir.path().join("stream.jsonl");
        let cfg = ClaudeBlock::default();
        let (res, logs) = capture_logs(|| {
            invoker
                .invoke(
                    "hi",
                    &cfg,
                    Duration::from_secs(10),
                    dir.path(),
                    &log,
                    &cancel,
                    None,
                )
                .unwrap()
        });

        assert!(!res.ok, "an undecidable run is not a success");
        assert!(
            res.transient,
            "a log the runner could not read must not count an attempt against the job"
        );
        assert!(
            !res.error.contains("did not end with a result event"),
            "the failure is no longer attributed to claude, got: {}",
            res.error
        );
        assert!(
            res.error.contains("could not read the stream log")
                && res.error.contains("stream.jsonl"),
            "the reason names the read failure and the log path, got: {}",
            res.error
        );
        assert!(
            res.summary.contains("stream.jsonl") && res.summary.contains("unknown, not failed"),
            "the operator-facing summary names the log and does not call the session failed, got: {}",
            res.summary
        );
        assert!(logs.contains("WARN"), "logged at warn, got:\n{logs}");
        assert!(
            logs.contains("could not read the stream log") && logs.contains("stream.jsonl"),
            "the log line carries the cause and the path, got:\n{logs}"
        );
    }

    /// CLAUDE-256: the same reasoning as `relay_failure_is_transient...`, for the
    /// read side. `transient` is the load-bearing field, so it is asserted
    /// explicitly rather than inferred, and the timeout flag survives.
    #[test]
    fn read_failure_is_transient_and_names_the_runner_as_the_cause() {
        let result = read_failure_result(
            "stream did not contain valid UTF-8 (os error 22)",
            Path::new("/logs/YouTrack/streams/CLAUDE-256-attempt-1.jsonl"),
            0,
        );
        assert!(!result.ok, "the run did not succeed");
        assert!(
            result.transient,
            "a runner-side read failure must not count an attempt against the job"
        );
        assert!(!result.timed_out, "exit 0 is not a timeout");
        assert!(
            result.error.contains("valid UTF-8")
                && result.error.contains("CLAUDE-256-attempt-1.jsonl"),
            "the cause and the log are named, got: {}",
            result.error
        );
        assert!(
            !result.error.contains("did not end with a result event"),
            "the failure is not attributed to claude, got: {}",
            result.error
        );
        assert!(
            read_failure_result("read failed", Path::new("/logs/s.jsonl"), 124).timed_out,
            "exit 124 is still surfaced as a timeout"
        );
    }

    /// CLAUDE-256: the last-resort summary path. `read_tail(..).unwrap_or_default()`
    /// returned an empty string for an unreadable log, which the operator sees as
    /// a run that said nothing at all. It now says the log was unreadable and
    /// names it, at `warn`.
    #[test]
    fn friendly_summary_names_a_log_it_could_not_read() {
        let dir = tempfile::tempdir().unwrap();
        let log = dir.path().join("stream.jsonl");
        std::fs::write(&log, [0xe2, 0x82]).unwrap();

        let (summary, logs) = capture_logs(|| friendly_failure_summary(&Value::Null, &log));
        assert!(!summary.is_empty(), "never an empty summary");
        assert!(
            summary.contains("could not read") && summary.contains("stream.jsonl"),
            "the summary says the log was unreadable and names it, got: {summary}"
        );
        assert!(logs.contains("WARN"), "logged at warn, got:\n{logs}");
        assert!(
            logs.contains("stream.jsonl"),
            "the log line names the path, got:\n{logs}"
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
            config_dir: None,
            default_claude: None,
            api_key: None,
            readiness: None,
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
                None,
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
