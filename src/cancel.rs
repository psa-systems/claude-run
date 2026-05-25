//! CLAUDE-9: in-flight run cancellation registry.
//!
//! The daemon dispatches each run on a detached task with no stored handle.
//! To cancel a run on demand we keep a per-`run_id` [`CancellationToken`] in a
//! process-wide registry. `run_action_inner` registers its token at
//! `run_begin` and drops it when the run ends; the `POST /runs/:id/cancel`
//! handler (and the `claude-run cancel` CLI that delegates to it) trips the
//! token.
//!
//! Tripping the token does three things downstream, in this order:
//! 1. the per-row pipeline terminates the in-flight claude child (SIGTERM,
//!    grace, then SIGKILL on its process group) - see [`crate::claude`],
//! 2. the in-flight row still runs its `post` phase with a synthesized
//!    cancelled `$claude_result` - see [`crate::row`],
//! 3. the run loop stops launching further rows and finalizes with
//!    [`crate::exit::CANCELLED`].
//!
//! A `std::sync::Mutex` (not the tokio one) guards the map: every operation is
//! a trivial, non-blocking map lookup, so no lock is ever held across an
//! `.await`, and [`RegistryGuard`] can unregister from a `Drop` impl.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

pub use tokio_util::sync::CancellationToken;

/// Process-wide map of in-flight `run_id` -> its cancellation token. Cheap to
/// clone (the map lives behind an `Arc`); the daemon hands the same handle to
/// both the dispatcher (registers) and the API state (cancels).
#[derive(Clone, Default)]
pub struct RunRegistry {
    inner: Arc<Mutex<HashMap<i64, CancellationToken>>>,
}

impl RunRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    /// Register a fresh token for `run_id` and return it. The run holds the
    /// returned clone for the duration of the run.
    pub fn register(&self, run_id: i64) -> CancellationToken {
        let token = CancellationToken::new();
        self.inner.lock().unwrap().insert(run_id, token.clone());
        token
    }

    /// Drop the token for `run_id`. Called when the run finishes (any path).
    pub fn unregister(&self, run_id: i64) {
        self.inner.lock().unwrap().remove(&run_id);
    }

    /// Trip the token for `run_id` if it is currently in flight. Returns
    /// `true` if a run was found and signalled, `false` if no such run is
    /// registered (already finished, never existed, or running in another
    /// process) - the caller treats `false` as an idempotent no-op.
    pub fn cancel(&self, run_id: i64) -> bool {
        match self.inner.lock().unwrap().get(&run_id) {
            Some(token) => {
                token.cancel();
                true
            }
            None => false,
        }
    }
}

/// RAII guard that unregisters a run's token on drop, so the registry is
/// cleaned up even when the run loop returns early via `?`. A `None` registry
/// (the standalone `claude-run run` path, which has no daemon to cancel
/// through) makes the guard a no-op.
pub struct RegistryGuard<'a> {
    pub registry: Option<&'a RunRegistry>,
    pub run_id: i64,
}

impl Drop for RegistryGuard<'_> {
    fn drop(&mut self) {
        if let Some(r) = self.registry {
            r.unregister(self.run_id);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cancel_unknown_run_is_noop() {
        let reg = RunRegistry::new();
        assert!(!reg.cancel(42));
    }

    #[test]
    fn register_then_cancel_trips_token() {
        let reg = RunRegistry::new();
        let token = reg.register(7);
        assert!(!token.is_cancelled());
        assert!(reg.cancel(7));
        assert!(token.is_cancelled());
    }

    #[test]
    fn unregister_drops_the_token() {
        let reg = RunRegistry::new();
        let _ = reg.register(9);
        reg.unregister(9);
        // No longer in flight, so cancel is a no-op.
        assert!(!reg.cancel(9));
    }

    #[test]
    fn guard_unregisters_on_drop() {
        let reg = RunRegistry::new();
        let _token = reg.register(3);
        {
            let _guard = RegistryGuard {
                registry: Some(&reg),
                run_id: 3,
            };
        }
        assert!(!reg.cancel(3));
    }
}
