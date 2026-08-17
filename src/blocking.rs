//! CLAUDE-228: hand the worker thread back to tokio around a blocking call.
//!
//! The runner is a thin async shell over synchronous work: `yt` / `fj` / `git`
//! children, the `claude` session, `nu` snippets, and the automerge poll loop.
//! Run inline on a tokio worker, each of those pins that worker for its whole
//! duration (up to `claude_timeout_seconds`), and a task holding a sqlx pool
//! connection that cannot be polled holds that connection too. That is how a
//! saturated host turned into `pool timed out while waiting for an open
//! connection` and dropped scheduler ticks, with no statement itself slow.

/// Run `f`, releasing this worker's core to the runtime for its duration.
///
/// [`tokio::task::block_in_place`] is used rather than
/// [`tokio::task::spawn_blocking`] because it takes a bare `FnOnce`: the call
/// sites keep their borrows (`&dyn Backend`, `&Path`) instead of needing
/// `Send + 'static` owned data.
///
/// The flavor check is required, not defensive: `block_in_place` panics on a
/// `current_thread` runtime, which is what `#[tokio::test]` builds by default.
/// The other two reachable contexts need no guard, since tokio itself runs the
/// closure directly there: on a `spawn_blocking` thread (no worker core) and in
/// a nested `block_in_place`.
pub fn in_place<F, R>(f: F) -> R
where
    F: FnOnce() -> R,
{
    match tokio::runtime::Handle::try_current().map(|h| h.runtime_flavor()) {
        Ok(tokio::runtime::RuntimeFlavor::MultiThread) => tokio::task::block_in_place(f),
        _ => f(),
    }
}

#[cfg(test)]
mod tests {
    use super::in_place;

    #[test]
    fn runs_outside_any_runtime() {
        assert_eq!(in_place(|| 7), 7);
    }

    #[tokio::test]
    async fn runs_on_a_current_thread_runtime_without_panicking() {
        // `#[tokio::test]` defaults to the current-thread flavor, where
        // `block_in_place` panics. The guard is what makes this pass.
        assert_eq!(in_place(|| 7), 7);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn runs_on_a_multi_thread_runtime() {
        assert_eq!(in_place(|| 7), 7);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn keeps_the_runtime_polling_while_blocked() {
        // The point of the helper: another task makes progress while this one
        // sits in a blocking call.
        let (tx, rx) = std::sync::mpsc::channel::<()>();
        let handle = tokio::spawn(async move {
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
            let _ = tx.send(());
        });
        // Blocks this worker for up to a second, but the spawned task must
        // still be driven to completion by the runtime.
        let got = in_place(|| rx.recv_timeout(std::time::Duration::from_secs(1)));
        assert!(got.is_ok(), "spawned task never ran while blocked");
        handle.await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn nests_without_panicking() {
        assert_eq!(in_place(|| in_place(|| 7)), 7);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn runs_inside_spawn_blocking() {
        // Reached whenever a wrapped helper is called from one of the existing
        // `spawn_blocking` regions (the webhook paths).
        let v = tokio::task::spawn_blocking(|| in_place(|| 7))
            .await
            .unwrap();
        assert_eq!(v, 7);
    }
}
