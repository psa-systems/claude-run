//! Per-run token / cost accounting (DEV-707) and the per-run spend ceiling
//! (DEV-705).
//!
//! Every `claude` invocation writes a `--output-format=stream-json` NDJSON log.
//! [`parse_stream_usage`] replays that log to recover what the invocation spent:
//! input / output / cache-read / cache-write tokens per turn, tool call counts,
//! and which tool's result was in context when each turn was billed. That gives
//! the per-tool attribution DEV-707 asks for without a provider console.
//!
//! [`RunSpend`] accumulates those figures across every invocation of ONE run and
//! carries the run's spend ceiling. The ceiling is a [`RunBudget`] snapshotted
//! from the action's configuration BEFORE the run starts and moved into the
//! per-job tasks by value, so nothing mid-run can raise it: raising it means
//! editing the action and starting a new run. There is no interactive path that
//! offers to raise the limit, which is the behavior DEV-705 removes.
//!
//! DEV-706: `peak_context_tokens` is the accounting side of the context-clearing
//! fix. A conversation that never compacts resends every completed operation's
//! tool payloads, so its per-turn context grows with the turn index and its
//! cumulative spend grows quadratically in the number of tool calls. A
//! conversation with a bounded compaction window (`--autocompact`, see
//! [`crate::claude`]) holds per-turn context under that window, so cumulative
//! spend grows linearly. `context_per_tool_call` is that ratio, and it is what
//! the per-run record makes comparable across runs.

use std::collections::{BTreeMap, HashMap};
use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Instant;

use serde::Serialize;
use serde_json::{json, Value};
use tracing::{info, warn};

use crate::claude::ClaudeResult;

/// Attribution bucket for turns billed with no tool result in context: the
/// opening turn and any pure-reasoning turn between operations.
pub const NO_TOOL: &str = "(prompt)";

/// Default fraction of the budget at which the run warns. DEV-705 fixes the
/// default at 80 percent; an action overrides it with
/// `limits.budget_warn_fraction`.
pub const DEFAULT_WARN_FRACTION: f64 = 0.80;

/// Token counts for one turn, one invocation, or a whole run.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize)]
pub struct TokenUsage {
    pub input: u64,
    pub output: u64,
    pub cache_read: u64,
    pub cache_write: u64,
}

impl TokenUsage {
    pub fn total(&self) -> u64 {
        self.input + self.output + self.cache_read + self.cache_write
    }

    /// Input-side tokens: what the turn resent. This is the figure DEV-706 is
    /// about, since output tokens do not accumulate across turns.
    pub fn context(&self) -> u64 {
        self.input + self.cache_read + self.cache_write
    }

    pub fn add(&mut self, other: TokenUsage) {
        self.input += other.input;
        self.output += other.output;
        self.cache_read += other.cache_read;
        self.cache_write += other.cache_write;
    }

    /// Read one API response's `usage` object. Absent fields read as zero: a
    /// turn with no cache breakpoints reports no cache counters at all.
    fn from_event(usage: &Value) -> Self {
        let n = |k: &str| usage.get(k).and_then(|v| v.as_u64()).unwrap_or(0);
        Self {
            input: n("input_tokens"),
            output: n("output_tokens"),
            cache_read: n("cache_read_input_tokens"),
            cache_write: n("cache_creation_input_tokens"),
        }
    }
}

/// What one tool cost this run: how often it was called, and the tokens billed
/// on turns where its result was the one in context.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize)]
pub struct ToolUsage {
    pub calls: u64,
    pub tokens: TokenUsage,
}

/// What one `claude` invocation spent, recovered from its stream log.
#[derive(Debug, Clone, Default, Serialize)]
pub struct StreamUsage {
    /// Billed assistant turns seen in the log.
    pub turns: u64,
    pub tokens: TokenUsage,
    /// Per-tool call counts and attributed tokens, keyed by tool name.
    pub by_tool: BTreeMap<String, ToolUsage>,
    /// Largest single-turn input side. Bounded means the conversation compacts.
    pub peak_context_tokens: u64,
    /// Sum of every turn's input side: what the whole conversation resent.
    pub cumulative_context_tokens: u64,
}

impl StreamUsage {
    pub fn tool_calls(&self) -> u64 {
        self.by_tool.values().map(|t| t.calls).sum()
    }

    /// DEV-706's acceptance measurement: resent context per tool call. Flat as N
    /// grows is the linear signature; rising with N is the quadratic one.
    pub fn context_per_tool_call(&self) -> u64 {
        let calls = self.tool_calls();
        if calls == 0 {
            return self.cumulative_context_tokens;
        }
        self.cumulative_context_tokens / calls
    }

    fn merge(&mut self, other: &StreamUsage) {
        self.turns += other.turns;
        self.tokens.add(other.tokens);
        self.cumulative_context_tokens += other.cumulative_context_tokens;
        self.peak_context_tokens = self.peak_context_tokens.max(other.peak_context_tokens);
        for (name, usage) in &other.by_tool {
            let slot = self.by_tool.entry(name.clone()).or_default();
            slot.calls += usage.calls;
            slot.tokens.add(usage.tokens);
        }
    }
}

/// Replay a stream-json log and recover its usage.
///
/// Attribution: an assistant turn is billed for the context it was sent, so its
/// tokens are attributed to the tool whose result most recently entered that
/// context. Turns before any tool result land in [`NO_TOOL`]. An unreadable or
/// absent log yields an empty [`StreamUsage`]; usage accounting must never turn
/// a completed run into a failed one.
pub fn parse_stream_usage(stream_log: &Path) -> StreamUsage {
    let file = match std::fs::File::open(stream_log) {
        Ok(f) => f,
        Err(e) => {
            warn!(path = %stream_log.display(), error = %e, "usage accounting could not read the stream log; this run reports no tokens");
            return StreamUsage::default();
        }
    };
    let mut out = StreamUsage::default();
    // tool_use id -> tool name, so a `tool_result` can name the tool it answers.
    let mut names: HashMap<String, String> = HashMap::new();
    let mut in_context = NO_TOOL.to_string();
    for line in BufReader::new(file).lines() {
        let Ok(line) = line else { break };
        let Ok(event) = serde_json::from_str::<Value>(&line) else {
            continue;
        };
        match event.get("type").and_then(|v| v.as_str()) {
            Some("assistant") => {
                for block in content_blocks(&event) {
                    if block.get("type").and_then(|v| v.as_str()) == Some("tool_use") {
                        let name = block
                            .get("name")
                            .and_then(|v| v.as_str())
                            .unwrap_or("(unnamed)")
                            .to_string();
                        if let Some(id) = block.get("id").and_then(|v| v.as_str()) {
                            names.insert(id.to_string(), name.clone());
                        }
                        out.by_tool.entry(name).or_default().calls += 1;
                    }
                }
                let Some(usage) = event.pointer("/message/usage") else {
                    continue;
                };
                let tokens = TokenUsage::from_event(usage);
                out.turns += 1;
                out.tokens.add(tokens);
                let context = tokens.context();
                out.cumulative_context_tokens += context;
                out.peak_context_tokens = out.peak_context_tokens.max(context);
                out.by_tool
                    .entry(in_context.clone())
                    .or_default()
                    .tokens
                    .add(tokens);
            }
            Some("user") => {
                for block in content_blocks(&event) {
                    if block.get("type").and_then(|v| v.as_str()) != Some("tool_result") {
                        continue;
                    }
                    let id = block
                        .get("tool_use_id")
                        .and_then(|v| v.as_str())
                        .unwrap_or_default();
                    in_context = names
                        .get(id)
                        .cloned()
                        .unwrap_or_else(|| NO_TOOL.to_string());
                }
            }
            _ => {}
        }
    }
    out
}

/// The content blocks of an `assistant` / `user` stream event. A string
/// `content` (a plain text turn) carries no blocks.
fn content_blocks(event: &Value) -> Vec<&Value> {
    event
        .pointer("/message/content")
        .and_then(|c| c.as_array())
        .map(|a| a.iter().collect())
        .unwrap_or_default()
}

/// A run's spend ceiling, frozen before the run starts.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct RunBudget {
    /// Dollars this ONE run may spend. Reached or exceeded, the run stops.
    pub budget_usd: f64,
    /// Fraction of `budget_usd` at which the run warns once.
    pub warn_fraction: f64,
}

impl RunBudget {
    pub fn new(budget_usd: f64, warn_fraction: f64) -> Self {
        Self {
            budget_usd,
            // A fraction outside (0, 1] cannot warn before the ceiling, so it
            // falls back to the default rather than silently never firing.
            warn_fraction: if warn_fraction > 0.0 && warn_fraction <= 1.0 {
                warn_fraction
            } else {
                DEFAULT_WARN_FRACTION
            },
        }
    }

    pub fn warn_at_usd(&self) -> f64 {
        self.budget_usd * self.warn_fraction
    }
}

/// What a recorded invocation did to the run's budget.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Verdict {
    /// Under the warning threshold.
    Under,
    /// At or past the warning threshold, still under the ceiling.
    Warned,
    /// At or past the ceiling. The run stops; no further job is picked up.
    Exceeded,
}

#[derive(Debug, Default)]
struct Inner {
    usage: StreamUsage,
    claude_calls: u64,
    cost_usd: f64,
    warned: bool,
    reported: bool,
}

/// Shared per-run spend ledger. Cloned into every per-job task, so one run's
/// parallel jobs bill against one ceiling.
#[derive(Debug, Clone)]
pub struct RunSpend {
    budget: RunBudget,
    action: String,
    /// Where the machine-readable per-run record is appended. `None` in tests
    /// and on paths with no configured log dir.
    record_path: Option<PathBuf>,
    started: Instant,
    inner: Arc<Mutex<Inner>>,
}

impl RunSpend {
    pub fn new(action: impl Into<String>, budget: RunBudget, record_path: Option<PathBuf>) -> Self {
        Self {
            budget,
            action: action.into(),
            record_path,
            started: Instant::now(),
            inner: Arc::new(Mutex::new(Inner::default())),
        }
    }

    pub fn budget(&self) -> RunBudget {
        self.budget
    }

    /// Dollars left before the ceiling, floored at zero.
    pub fn remaining_usd(&self) -> f64 {
        (self.budget.budget_usd - self.cost_usd()).max(0.0)
    }

    pub fn cost_usd(&self) -> f64 {
        self.lock().cost_usd
    }

    /// True once the run has reached its ceiling. Checked before each job pickup
    /// and before each invocation.
    pub fn exceeded(&self) -> bool {
        self.cost_usd() >= self.budget.budget_usd
    }

    /// The reason string a stopped run reports. Names the budget, per DEV-705.
    pub fn stop_reason(&self) -> String {
        format!(
            "run spend ${:.4} reached the per-run budget of ${:.2} (limits.run_budget_usd); the run stops here. Raise it by editing the action and starting a new run.",
            self.cost_usd(),
            self.budget.budget_usd
        )
    }

    /// Fold one completed invocation into the run's ledger and report where the
    /// run now stands against its ceiling.
    pub fn record(&self, result: &ClaudeResult) -> Verdict {
        let (verdict, cost, warn_once) = {
            let mut inner = self.lock();
            inner.claude_calls += 1;
            inner.cost_usd += result.cost;
            inner.usage.merge(&result.usage);
            let cost = inner.cost_usd;
            if cost >= self.budget.budget_usd {
                (Verdict::Exceeded, cost, false)
            } else if cost >= self.budget.warn_at_usd() {
                let first = !inner.warned;
                inner.warned = true;
                (Verdict::Warned, cost, first)
            } else {
                (Verdict::Under, cost, false)
            }
        };
        match verdict {
            Verdict::Exceeded => warn!(
                action = %self.action,
                cost_usd = cost,
                budget_usd = self.budget.budget_usd,
                "{}",
                self.stop_reason()
            ),
            Verdict::Warned if warn_once => warn!(
                action = %self.action,
                cost_usd = cost,
                budget_usd = self.budget.budget_usd,
                warn_fraction = self.budget.warn_fraction,
                "run spend passed {:.0} percent of its ${:.2} budget",
                self.budget.warn_fraction * 100.0,
                self.budget.budget_usd
            ),
            _ => {}
        }
        verdict
    }

    /// The run's exit code. `base` is what the run would otherwise report
    /// (OK / PAUSED / CANCELLED); an over-budget stop only overrides a run that
    /// would otherwise have finished cleanly, so a cancel still reads as a
    /// cancel.
    pub fn exit_code(&self, base: i32) -> i32 {
        if base == crate::exit::OK && self.exceeded() {
            crate::exit::OVER_BUDGET
        } else {
            base
        }
    }

    pub fn snapshot(&self) -> StreamUsage {
        self.lock().usage.clone()
    }

    /// Emit the end-of-run summary and append the machine-readable record.
    /// Idempotent: a driver that finishes twice reports once.
    pub fn finish(&self, run_id: Option<i64>, exit_code: i32) {
        let (usage, calls, cost) = {
            let mut inner = self.lock();
            if inner.reported {
                return;
            }
            inner.reported = true;
            (inner.usage.clone(), inner.claude_calls, inner.cost_usd)
        };
        let duration_ms = self.started.elapsed().as_millis() as u64;
        let tool_calls = usage.tool_calls();
        info!(
            action = %self.action,
            run_id = run_id.unwrap_or(-1),
            exit_code,
            claude_calls = calls,
            cost_usd = cost,
            budget_usd = self.budget.budget_usd,
            duration_ms,
            input_tokens = usage.tokens.input,
            output_tokens = usage.tokens.output,
            cache_read_tokens = usage.tokens.cache_read,
            cache_write_tokens = usage.tokens.cache_write,
            total_tokens = usage.tokens.total(),
            turns = usage.turns,
            tool_calls,
            peak_context_tokens = usage.peak_context_tokens,
            context_per_tool_call = usage.context_per_tool_call(),
            top_tool = %top_tool(&usage),
            "run usage"
        );
        let Some(path) = &self.record_path else {
            return;
        };
        let record = json!({
            "ts": crate::state::now_iso(),
            "run_id": run_id,
            "action": self.action,
            "exit_code": exit_code,
            "duration_ms": duration_ms,
            "claude_calls": calls,
            "cost_usd": cost,
            "budget_usd": self.budget.budget_usd,
            "warn_fraction": self.budget.warn_fraction,
            "tokens": {
                "input": usage.tokens.input,
                "output": usage.tokens.output,
                "cache_read": usage.tokens.cache_read,
                "cache_write": usage.tokens.cache_write,
                "total": usage.tokens.total(),
            },
            "turns": usage.turns,
            "tool_calls": tool_calls,
            "peak_context_tokens": usage.peak_context_tokens,
            "cumulative_context_tokens": usage.cumulative_context_tokens,
            "context_per_tool_call": usage.context_per_tool_call(),
            "by_tool": usage.by_tool,
        });
        append_record(path, &record);
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, Inner> {
        // A poisoned ledger still holds usable counters, and losing accounting
        // must never abort a run.
        self.inner.lock().unwrap_or_else(|e| e.into_inner())
    }
}

/// The tool that consumed the most attributed tokens, so a dominant tool is
/// visible in the one-line summary without opening the record.
fn top_tool(usage: &StreamUsage) -> String {
    usage
        .by_tool
        .iter()
        .max_by_key(|(_, u)| u.tokens.total())
        .map(|(name, u)| format!("{name}={}", u.tokens.total()))
        .unwrap_or_else(|| "none".to_string())
}

/// Append one newline-delimited JSON record. Best effort: a failed write is
/// logged, never propagated.
fn append_record(path: &Path, record: &Value) {
    if let Some(dir) = path.parent() {
        if let Err(e) = std::fs::create_dir_all(dir) {
            warn!(path = %path.display(), error = %e, "usage record dir create failed");
            return;
        }
    }
    let mut line =
        serde_json::to_string(record).unwrap_or_else(|e| format!("{{\"error\":\"{e}\"}}"));
    line.push('\n');
    if let Err(e) = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .and_then(|mut f| f.write_all(line.as_bytes()))
    {
        warn!(path = %path.display(), error = %e, "usage record write failed");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn assistant(usage: (u64, u64, u64, u64), tools: &[(&str, &str)]) -> String {
        let blocks: Vec<Value> = tools
            .iter()
            .map(|(id, name)| json!({"type": "tool_use", "id": id, "name": name}))
            .collect();
        json!({
            "type": "assistant",
            "message": {
                "content": blocks,
                "usage": {
                    "input_tokens": usage.0,
                    "output_tokens": usage.1,
                    "cache_read_input_tokens": usage.2,
                    "cache_creation_input_tokens": usage.3,
                },
            },
        })
        .to_string()
    }

    fn tool_result(id: &str) -> String {
        json!({
            "type": "user",
            "message": {"content": [{"type": "tool_result", "tool_use_id": id, "content": "ok"}]},
        })
        .to_string()
    }

    fn write_log(dir: &Path, name: &str, lines: &[String]) -> PathBuf {
        let path = dir.join(name);
        std::fs::write(&path, format!("{}\n", lines.join("\n"))).unwrap();
        path
    }

    /// A conversation of N tool calls, each turn resending everything before it
    /// unless `window` caps the resent prefix. This is the shape DEV-706 is
    /// about: `window: None` is the un-compacted conversation.
    fn conversation(calls: u64, per_call: u64, window: Option<u64>) -> Vec<String> {
        let mut lines = vec![assistant(
            (per_call, 10, 0, 0),
            &[("t0", "mcp__youtrack__get")],
        )];
        for i in 1..=calls {
            lines.push(tool_result(&format!("t{}", i - 1)));
            let grown = per_call * (i + 1);
            let context = window.map(|w| grown.min(w)).unwrap_or(grown);
            lines.push(assistant(
                (context, 10, 0, 0),
                &[(&format!("t{i}"), "mcp__youtrack__get")],
            ));
        }
        lines
    }

    #[test]
    fn accumulates_every_token_category_across_turns() {
        let dir = tempfile::tempdir().unwrap();
        let log = write_log(
            dir.path(),
            "stream.jsonl",
            &[
                assistant((100, 20, 5, 7), &[("a", "Bash")]),
                tool_result("a"),
                assistant((300, 40, 11, 13), &[]),
            ],
        );
        let usage = parse_stream_usage(&log);
        assert_eq!(usage.turns, 2);
        assert_eq!(usage.tokens.input, 400);
        assert_eq!(usage.tokens.output, 60);
        assert_eq!(usage.tokens.cache_read, 16);
        assert_eq!(usage.tokens.cache_write, 20);
        assert_eq!(usage.tokens.total(), 496);
    }

    #[test]
    fn attributes_tokens_to_the_tool_whose_result_was_in_context() {
        let dir = tempfile::tempdir().unwrap();
        let log = write_log(
            dir.path(),
            "stream.jsonl",
            &[
                // Opening turn: no tool result in context yet.
                assistant((10, 1, 0, 0), &[("a", "mcp__youtrack__get_issue")]),
                tool_result("a"),
                // Billed with the YouTrack result in context.
                assistant((900, 2, 0, 0), &[("b", "Bash")]),
                tool_result("b"),
                assistant((50, 3, 0, 0), &[]),
            ],
        );
        let usage = parse_stream_usage(&log);
        assert_eq!(usage.by_tool[NO_TOOL].tokens.input, 10);
        assert_eq!(usage.by_tool["mcp__youtrack__get_issue"].tokens.input, 900);
        assert_eq!(usage.by_tool["mcp__youtrack__get_issue"].calls, 1);
        assert_eq!(usage.by_tool["Bash"].tokens.input, 50);
        assert_eq!(usage.by_tool["Bash"].calls, 1);
        assert_eq!(usage.tool_calls(), 2);
        // The dominant tool is identifiable from the summary line.
        assert_eq!(top_tool(&usage), "mcp__youtrack__get_issue=902");
    }

    /// DEV-706 acceptance measurement. Without compaction the resent context per
    /// tool call keeps rising as N grows (cumulative spend is quadratic in N);
    /// with a bounded window it settles (cumulative spend is linear in N).
    #[test]
    fn context_per_tool_call_separates_quadratic_growth_from_linear() {
        let dir = tempfile::tempdir().unwrap();
        let measure = |calls: u64, window: Option<u64>, name: &str| {
            let log = write_log(dir.path(), name, &conversation(calls, 1_000, window));
            let usage = parse_stream_usage(&log);
            assert_eq!(usage.tool_calls(), calls + 1);
            usage.context_per_tool_call()
        };

        // Un-compacted: doubling N nearly doubles the cost of every tool call.
        let uncompacted_10 = measure(10, None, "q10.jsonl");
        let uncompacted_40 = measure(40, None, "q40.jsonl");
        assert!(
            uncompacted_40 > uncompacted_10 * 3,
            "un-compacted growth should be quadratic: {uncompacted_10} -> {uncompacted_40}"
        );

        // Compacted to a 5k window: per-tool-call cost stops growing with N, so
        // cumulative usage is linear in N.
        let compacted_10 = measure(10, Some(5_000), "l10.jsonl");
        let compacted_40 = measure(40, Some(5_000), "l40.jsonl");
        assert!(
            compacted_40 <= compacted_10 * 2,
            "compacted growth should be linear: {compacted_10} -> {compacted_40}"
        );
        assert!(compacted_40 < uncompacted_40 / 3);
    }

    #[test]
    fn an_unreadable_stream_log_reports_no_usage_rather_than_failing() {
        let usage = parse_stream_usage(Path::new("/nonexistent/stream.jsonl"));
        assert_eq!(usage.turns, 0);
        assert_eq!(usage.tokens.total(), 0);
    }

    fn result(cost: f64) -> ClaudeResult {
        let mut r = ClaudeResult::synthesized("", "");
        r.cost = cost;
        r
    }

    #[test]
    fn budget_warns_at_the_configured_fraction_then_stops_the_run() {
        let spend = RunSpend::new("demo", RunBudget::new(1.00, 0.80), None);
        assert_eq!(spend.record(&result(0.50)), Verdict::Under);
        assert!(!spend.exceeded());
        assert_eq!(spend.record(&result(0.35)), Verdict::Warned);
        assert!(!spend.exceeded());
        assert_eq!(spend.exit_code(crate::exit::OK), crate::exit::OK);
        assert_eq!(spend.record(&result(0.20)), Verdict::Exceeded);
        assert!(spend.exceeded());
        assert_eq!(spend.remaining_usd(), 0.0);
        assert_eq!(
            spend.exit_code(crate::exit::OK),
            crate::exit::OVER_BUDGET,
            "an over-budget run exits non-zero"
        );
        assert!(spend.stop_reason().contains("$1.00"));
    }

    #[test]
    fn a_cancelled_or_paused_run_keeps_its_own_exit_code() {
        let spend = RunSpend::new("demo", RunBudget::new(0.10, 0.80), None);
        spend.record(&result(1.00));
        assert!(spend.exceeded());
        assert_eq!(
            spend.exit_code(crate::exit::CANCELLED),
            crate::exit::CANCELLED
        );
        assert_eq!(spend.exit_code(crate::exit::PAUSED), crate::exit::PAUSED);
    }

    /// The ceiling is a snapshot taken before the run: a clone handed to a job
    /// sees the same budget, and nothing on the handle can raise it.
    #[test]
    fn the_budget_is_frozen_for_the_life_of_the_run() {
        let spend = RunSpend::new("demo", RunBudget::new(2.00, 0.80), None);
        let job = spend.clone();
        job.record(&result(1.00));
        assert_eq!(job.budget().budget_usd, 2.00);
        assert_eq!(spend.budget().budget_usd, 2.00);
        assert_eq!(spend.cost_usd(), 1.00, "a clone bills the same ledger");
        assert_eq!(spend.remaining_usd(), 1.00);
    }

    #[test]
    fn an_out_of_range_warn_fraction_falls_back_to_the_default() {
        assert_eq!(
            RunBudget::new(1.0, 0.0).warn_fraction,
            DEFAULT_WARN_FRACTION
        );
        assert_eq!(
            RunBudget::new(1.0, 1.5).warn_fraction,
            DEFAULT_WARN_FRACTION
        );
        assert_eq!(RunBudget::new(1.0, 0.5).warn_fraction, 0.5);
    }

    #[test]
    fn finish_appends_one_machine_readable_record_per_run() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("logs").join("usage.jsonl");
        let spend = RunSpend::new("youtrack", RunBudget::new(5.0, 0.8), Some(path.clone()));
        let mut r = result(0.25);
        r.usage = parse_stream_usage(&write_log(
            dir.path(),
            "stream.jsonl",
            &[
                assistant((10, 1, 0, 0), &[("a", "mcp__youtrack__get_issue")]),
                tool_result("a"),
                assistant((90, 2, 3, 4), &[]),
            ],
        ));
        spend.record(&r);
        spend.finish(Some(7), crate::exit::OK);
        // A second finish is a no-op, so a driver that finalizes twice records once.
        spend.finish(Some(7), crate::exit::OK);

        let body = std::fs::read_to_string(&path).unwrap();
        assert_eq!(body.lines().count(), 1);
        let rec: Value = serde_json::from_str(body.lines().next().unwrap()).unwrap();
        assert_eq!(rec["run_id"], 7);
        assert_eq!(rec["action"], "youtrack");
        assert_eq!(rec["claude_calls"], 1);
        assert_eq!(rec["cost_usd"], 0.25);
        assert_eq!(rec["budget_usd"], 5.0);
        assert_eq!(rec["tokens"]["input"], 100);
        assert_eq!(rec["tokens"]["output"], 3);
        assert_eq!(rec["tokens"]["cache_read"], 3);
        assert_eq!(rec["tokens"]["cache_write"], 4);
        assert_eq!(rec["tokens"]["total"], 110);
        assert_eq!(rec["turns"], 2);
        assert_eq!(rec["tool_calls"], 1);
        assert_eq!(rec["by_tool"]["mcp__youtrack__get_issue"]["calls"], 1);
        assert_eq!(
            rec["by_tool"]["mcp__youtrack__get_issue"]["tokens"]["input"],
            90
        );
        assert!(rec["exit_code"].is_i64());
        assert!(rec["duration_ms"].is_u64());
    }

    /// Usage accumulates across invocations, which is what makes a multi-job run
    /// report one total rather than the last job's.
    #[test]
    fn usage_accumulates_across_invocations_of_one_run() {
        let dir = tempfile::tempdir().unwrap();
        let spend = RunSpend::new("demo", RunBudget::new(10.0, 0.8), None);
        for (i, tool) in ["Bash", "mcp__youtrack__get_issue"].iter().enumerate() {
            let mut r = result(0.10);
            r.usage = parse_stream_usage(&write_log(
                dir.path(),
                &format!("s{i}.jsonl"),
                &[
                    assistant((10, 1, 0, 0), &[("a", tool)]),
                    tool_result("a"),
                    assistant((100, 2, 0, 0), &[]),
                ],
            ));
            spend.record(&r);
        }
        let usage = spend.snapshot();
        assert_eq!(usage.turns, 4);
        assert_eq!(usage.tokens.input, 220);
        assert_eq!(usage.by_tool["Bash"].tokens.input, 100);
        assert_eq!(usage.by_tool["mcp__youtrack__get_issue"].tokens.input, 100);
        assert_eq!(usage.by_tool[NO_TOOL].tokens.input, 20);
        assert!((spend.cost_usd() - 0.20).abs() < 1e-9);
    }
}
