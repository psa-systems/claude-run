//! Native action dispatch seam (CLAUDE-30).
//!
//! Scripted actions run the generic nu-snippet pipeline in [`crate::job`].
//! Native actions route here instead: each native [`ActionKind`] maps to a
//! built-in Rust [`NativeHandler`] via the [`handler_for`] registry. This phase
//! ships only the seam, so the youtrack handler is a stub that returns
//! [`Error::NativeUnimplemented`]; the real logic lands in CLAUDE-33 (Phase 4).
//!
//! Shared run accounting (run_begin/run_finish, planned keys, caps/budget,
//! job-outcome inserts) stays in [`crate::daemon::run_action_inner`]; native
//! handlers plug their job production into that common scaffolding once they
//! exist.

use std::future::Future;
use std::pin::Pin;

use crate::action::{ActionKind, LoadedAction};
use crate::cancel::RunRegistry;
use crate::claude::ClaudeInvoker;
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
    pub snippet: &'a SnippetRunner,
    pub claude: &'a ClaudeInvoker,
    pub triggered_by: &'a str,
    pub registry: Option<&'a RunRegistry>,
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
        ActionKind::Youtrack => Some(Box::new(YoutrackHandler)),
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

/// Stub YouTrack handler. Real implementation lands in CLAUDE-33.
struct YoutrackHandler;

impl NativeHandler for YoutrackHandler {
    fn run<'a>(&self, _ctx: NativeContext<'a>) -> HandlerFuture<'a> {
        Box::pin(async move {
            Err(Error::NativeUnimplemented {
                kind: ActionKind::Youtrack.as_str().to_string(),
            })
        })
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
}
