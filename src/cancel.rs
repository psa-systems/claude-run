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

    /// CLAUDE-11: trip every in-flight run's token at once, for graceful
    /// shutdown. Returns the run ids that were signalled (arbitrary order).
    /// Each tripped run tears down through the same path as a single cancel:
    /// the claude child is terminated, `post` runs with a synthesized cancelled
    /// result, and the run finalizes with `exit::CANCELLED`, unregistering
    /// itself here on the way out.
    pub fn cancel_all(&self) -> Vec<i64> {
        let map = self.inner.lock().unwrap();
        for token in map.values() {
            token.cancel();
        }
        map.keys().copied().collect()
    }

    /// True when no run is registered, i.e. every in-flight run has finalized
    /// and unregistered. The shutdown path polls this to know the cancel
    /// requests it issued have drained.
    pub fn is_empty(&self) -> bool {
        self.inner.lock().unwrap().is_empty()
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
    fn cancel_all_trips_every_token_and_returns_ids() {
        let reg = RunRegistry::new();
        let a = reg.register(1);
        let b = reg.register(2);
        assert!(!reg.is_empty());
        let mut ids = reg.cancel_all();
        ids.sort_unstable();
        assert_eq!(ids, vec![1, 2]);
        assert!(a.is_cancelled());
        assert!(b.is_cancelled());
        // cancel_all signals but does not unregister; the runs unregister
        // themselves as they finalize.
        assert!(!reg.is_empty());
    }

    #[test]
    fn empty_registry_reports_empty() {
        let reg = RunRegistry::new();
        assert!(reg.is_empty());
        assert!(reg.cancel_all().is_empty());
        let _ = reg.register(5);
        assert!(!reg.is_empty());
        reg.unregister(5);
        assert!(reg.is_empty());
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
