//! Native action dispatch seam (CLAUDE-30).
//!
//! Scripted actions run the generic nu-snippet pipeline in [`crate::job`].
//! Native actions route here instead: each native [`ActionKind`] maps to a
//! built-in Rust [`NativeHandler`] via the [`handler_for`] registry. CLAUDE-30
//! shipped the seam with a stub; CLAUDE-32 (Phase 3) lands the real YouTrack
//! handler in [`youtrack`].
//!
//! Unlike a scripted action (whose run accounting lives in
//! [`crate::daemon::run_action_inner`]), a native handler owns its whole run:
//! it produces typed jobs from the provider backend and drives the same
//! run_begin/run_finish, caps/budget, planned-keys and job-outcome scaffolding
//! itself (see [`youtrack`]). It reuses the [`crate::job`] helpers
//! ([`crate::job::effective_limits`], [`crate::job::ProcessedJob`],
//! [`crate::job::done_record`], [`crate::job::append_output_record`]) so its
//! outcome records and JSONL output stay byte-identical to the scripted path.

pub mod ci_failure_analysis;
pub mod ci_failure_reconcile;
pub mod forgejo_ci_fix;
pub mod youtrack;

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use crate::action::{ActionKind, LoadedAction};
use crate::cancel::RunRegistry;
use crate::claude::ClaudeInvoker;
use crate::concurrency::{KeyGate, Limiter};
use crate::config::Paths;
use crate::error::{Error, Result};
use crate::snippet::SnippetRunner;
use sqlx::SqlitePool;

/// Everything a native handler needs, mirroring the arguments
/// [`crate::daemon::run_action_inner`] receives. Passed by value so a handler
/// can move it into its async body.
pub struct NativeContext<'a> {
    pub action: &'a LoadedAction,
    pub paths: &'a Paths,
    pub pool: &'a SqlitePool,
    /// `Arc` (not `&`) so the native handler can clone it into the per-job
    /// tasks spawned by the CLAUDE-28 keyed executor.
    pub snippet: Arc<SnippetRunner>,
    pub claude: Arc<ClaudeInvoker>,
    pub triggered_by: &'a str,
    pub registry: Option<&'a RunRegistry>,
    /// CLAUDE-28: daemon-wide per-key gate, used by the handler's executor.
    pub keygate: KeyGate,
    /// CLAUDE-162: daemon-wide concurrency limiter (global + per-action caps),
    /// used by the handler's executor so caps hold across overlapping runs.
    pub limiter: Limiter,
}

/// Boxed future returned by a [`NativeHandler`]. Hand-rolled (rather than
/// `async_trait`) to keep the dependency footprint unchanged.
pub type HandlerFuture<'a> = Pin<Box<dyn Future<Output = Result<()>> + Send + 'a>>;

/// A built-in handler for one native [`ActionKind`].
pub trait NativeHandler: Send + Sync {
    fn run<'a>(&self, ctx: NativeContext<'a>) -> HandlerFuture<'a>;
}

/// Registry: resolve a native kind to its handler. Returns `None` for
/// `scripted` (which never dispatches here).
fn handler_for(kind: ActionKind) -> Option<Box<dyn NativeHandler>> {
    match kind {
        ActionKind::Scripted => None,
        ActionKind::Youtrack => Some(Box::new(youtrack::YoutrackHandler)),
        ActionKind::ForgejoCiFix => Some(Box::new(forgejo_ci_fix::ForgejoCiFixHandler)),
        ActionKind::CiFailureAnalysis => {
            Some(Box::new(ci_failure_analysis::CiFailureAnalysisHandler))
        }
        ActionKind::CiFailureReconcile => {
            Some(Box::new(ci_failure_reconcile::CiFailureReconcileHandler))
        }
    }
}

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
    fn forgejo_ci_fix_has_a_native_handler() {
        assert!(handler_for(ActionKind::ForgejoCiFix).is_some());
    }

    #[test]
    fn ci_failure_analysis_has_a_native_handler() {
        assert!(handler_for(ActionKind::CiFailureAnalysis).is_some());
    }

    #[test]
    fn ci_failure_reconcile_has_a_native_handler() {
        assert!(handler_for(ActionKind::CiFailureReconcile).is_some());
    }
}
