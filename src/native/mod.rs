//! SPEC-3.1: "Action kinds: `scripted` (default), `youtrack`,
//! `closed-issue-summary`. A native kind requires a matching `native` block."
//! This module is the dispatch seam a native kind's `native` block routes
//! through.
//!
//! Native action dispatch seam.
//!
//! Scripted actions run the generic nu-snippet pipeline in [`crate::job`].
//! Native actions route here instead: each native [`ActionKind`] maps to a
//! built-in Rust [`NativeHandler`] via the [`handler_for`] registry, which
//! carries the real YouTrack handler in [`youtrack`].
//!
//! Unlike a scripted action (whose run accounting lives in
//! [`crate::daemon::run_action_inner`]), a native handler owns its whole run:
//! it produces typed jobs from the provider backend and drives the same
//! run_begin/run_finish, caps/budget, planned-keys and job-outcome scaffolding
//! itself (see [`youtrack`]). It reuses the [`crate::job`] helpers
//! ([`crate::job::effective_limits`], [`crate::job::ProcessedJob`],
//! [`crate::job::done_record`], [`crate::job::append_output_record`]) so its
//! outcome records and JSONL output stay byte-identical to the scripted path.

pub mod audit_prompt;
pub mod closed_issue_summary;
pub mod youtrack;

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use crate::action::{ActionKind, LoadedAction};
use crate::cancel::RunRegistry;
use crate::claude::ClaudeInvoker;
use crate::concurrency::{KeyGate, Limiter};
use crate::config::Paths;
use crate::db::DbPool;
use crate::error::{Error, Result};
use crate::snippet::SnippetRunner;

/// SPEC-3.1 / SPEC-3.2: carries the per-action limits, concurrency, pause and
/// spend-ceiling inputs a native handler's run must honor exactly as a
/// scripted action's run does.
///
/// Everything a native handler needs, mirroring the arguments
/// [`crate::daemon::run_action_inner`] receives. Passed by value so a handler
/// can move it into its async body.
pub struct NativeContext<'a> {
    pub action: &'a LoadedAction,
    pub paths: &'a Paths,
    pub pool: &'a DbPool,
    /// `Arc` (not `&`) so the native handler can clone it into the per-job
    /// tasks spawned by the keyed executor.
    pub snippet: Arc<SnippetRunner>,
    pub claude: Arc<ClaudeInvoker>,
    pub triggered_by: &'a str,
    pub registry: Option<&'a RunRegistry>,
    /// Daemon-wide per-key gate, used by the handler's executor.
    pub keygate: KeyGate,
    /// Daemon-wide concurrency limiter (global + per-action caps),
    /// used by the handler's executor so caps hold across overlapping runs.
    pub limiter: Limiter,
    /// Daemon-wide global-pause reader. Each handler derives a
    /// run-scoped [`crate::pause::RunPause`] after `run_begin` and consults it
    /// at its per-job checkpoints, so a pause taken mid-run stops the next
    /// pickup and the run finishes with `exit::PAUSED`.
    pub pause: crate::pause::PauseGate,
    /// Daemon-wide per-action gather marker. Each handler holds it
    /// across its gather step (the `yt list` that opens the run) so the
    /// scheduler does not stack another pickup on a gather still in flight.
    pub gather: crate::concurrency::GatherGate,
    /// This run's spend ledger and frozen ceiling, built by
    /// [`crate::daemon::run_action_inner`] from the action's limits BEFORE the
    /// handler starts. A handler bills every `claude` invocation against it,
    /// checks it before each pickup, and finalizes it with its run's exit code.
    pub spend: crate::usage::RunSpend,
}

/// Boxed future returned by a [`NativeHandler`]. Hand-rolled (rather than
/// `async_trait`) to keep the dependency footprint unchanged.
pub type HandlerFuture<'a> = Pin<Box<dyn Future<Output = Result<()>> + Send + 'a>>;

/// SPEC-3.1: the contract every native [`ActionKind`] handler (`youtrack`,
/// `closed-issue-summary`) implements.
///
/// A built-in handler for one native [`ActionKind`].
pub trait NativeHandler: Send + Sync {
    fn run<'a>(&self, ctx: NativeContext<'a>) -> HandlerFuture<'a>;
}

/// SPEC-3.1: "Action kinds: `scripted` (default), `youtrack`,
/// `closed-issue-summary`."
///
/// Registry: resolve a native kind to its handler. Returns `None` for
/// `scripted` (which never dispatches here).
fn handler_for(kind: ActionKind) -> Option<Box<dyn NativeHandler>> {
    match kind {
        ActionKind::Scripted => None,
        ActionKind::Youtrack => Some(Box::new(youtrack::YoutrackHandler)),
        ActionKind::ClosedIssueSummary => {
            Some(Box::new(closed_issue_summary::ClosedIssueSummaryHandler))
        }
    }
}

/// SPEC-3.1: "A native kind requires a matching `native` block."
///
/// Dispatch a native action to its handler. Called by
/// [`crate::daemon::run_action_inner`] when the action's `kind` is native.
pub async fn dispatch(ctx: NativeContext<'_>) -> Result<()> {
    let kind = ctx.action.action.kind;
    match handler_for(kind) {
        Some(handler) => handler.run(ctx).await,
        None => Err(Error::NativeUnimplemented {
            kind: kind.as_str().to_string(),
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scripted_has_no_native_handler() {
        assert!(handler_for(ActionKind::Scripted).is_none());
    }

    #[test]
    fn youtrack_has_a_native_handler() {
        assert!(handler_for(ActionKind::Youtrack).is_some());
    }

    #[test]
    fn closed_issue_summary_has_a_native_handler() {
        assert!(handler_for(ActionKind::ClosedIssueSummary).is_some());
    }
}
