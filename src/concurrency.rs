//! CLAUDE-28: keyed, bounded-parallel job execution.
//!
//! A run's jobs used to execute strictly sequentially (one `claude` at a time),
//! serializing independent work. This module replaces that loop with a bounded
//! executor that runs distinct-key jobs in parallel while never running two jobs
//! that share a *concurrency key* (project / repo / workflow) at the same time.
//!
//! Two primitives:
//!
//! - [`KeyGate`]: a process-wide registry of per-key async mutexes. Acquiring a
//!   key serializes every other holder of the same key. Held at the daemon level
//!   (alongside the dispatcher's per-action `in_flight` set) so the
//!   one-execution-per-key guarantee survives a scheduled run overlapping a
//!   manual trigger, not just the jobs within a single run.
//! - [`Executor`]: spawns one task per job, each acquiring (a) its key gate so
//!   same-key jobs serialize, then (b) a permit from a [`Semaphore`] of size
//!   `max_parallel` so at most that many distinct-key jobs run at once.
//!
//! `max_parallel == 1` reproduces the original strictly-sequential behavior.
//!
//! Acquisition order is gate-then-permit on purpose: a job blocked waiting for
//! its key never holds a parallelism permit, so it cannot starve distinct-key
//! jobs that are ready to run.

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};

use tokio::sync::{Mutex as AsyncMutex, OwnedMutexGuard, Semaphore};

pub use tokio_util::sync::CancellationToken;

use crate::error::{Error, Result};

/// The boxed per-job future the [`Executor`] runs. Each resolves to `Ok(true)`
/// when the job was processed, `Ok(false)` when it was skipped before doing work
/// (e.g. a pre-launch cap/budget check tripped), or `Err` on a hard error.
pub type JobFuture = Pin<Box<dyn Future<Output = Result<bool>> + Send>>;

/// Process-wide map of concurrency-key -> a per-key async mutex. Cloning shares
/// the same map (it lives behind an `Arc`), so the daemon hands the same gate to
/// every run; two jobs resolving to the same key serialize against each other
/// regardless of which run or trigger started them.
#[derive(Clone, Default)]
pub struct KeyGate {
    inner: Arc<Mutex<HashMap<String, Arc<AsyncMutex<()>>>>>,
}

impl KeyGate {
    pub fn new() -> Self {
        Self::default()
    }

    /// Acquire the gate for `key`, awaiting until no other holder of the same
    /// key is in flight. The returned guard releases the key on drop. The outer
    /// `std::sync::Mutex` only guards the tiny map lookup, never held across an
    /// `.await`; the per-key `tokio::sync::Mutex` is what callers actually wait
    /// on.
    pub async fn acquire(&self, key: &str) -> OwnedMutexGuard<()> {
        let m = {
            let mut g = self.inner.lock().unwrap();
            g.entry(key.to_string()).or_default().clone()
        };
        m.lock_owned().await
    }
}

/// Bounded keyed executor. Runs each `(key, future)` pair with the
/// gate-then-permit discipline described in the module docs.
pub struct Executor {
    /// Max distinct-key jobs in flight at once. Floored to 1.
    pub max_parallel: usize,
    pub gate: KeyGate,
    /// The run's cancellation token. A tripped token skips jobs that have not
    /// started yet (their future is dropped unpolled); jobs already running tear
    /// down through their own clone of the token inside the job pipeline.
    pub cancel: CancellationToken,
}

impl Executor {
    /// Run all `items` concurrently under the key gate and the parallelism cap,
    /// returning one result per item in input order. A panicked task surfaces as
    /// an `Err` rather than aborting the whole run.
    pub async fn run(&self, items: Vec<(String, JobFuture)>) -> Vec<Result<bool>> {
        let sem = Arc::new(Semaphore::new(self.max_parallel.max(1)));
        let mut handles = Vec::with_capacity(items.len());
        for (key, fut) in items {
            let gate = self.gate.clone();
            let cancel = self.cancel.clone();
            let sem = sem.clone();
            handles.push(tokio::spawn(async move {
                // (a) serialize same-key jobs, then (b) take a parallelism slot.
                let _key_guard = gate.acquire(&key).await;
                if cancel.is_cancelled() {
                    return Ok(false);
                }
                let _permit = match sem.acquire_owned().await {
                    Ok(p) => p,
                    Err(_) => return Ok(false),
                };
                if cancel.is_cancelled() {
                    return Ok(false);
                }
                fut.await
            }));
        }
        let mut out = Vec::with_capacity(handles.len());
        for h in handles {
            match h.await {
                Ok(r) => out.push(r),
                Err(e) => out.push(Err(Error::Other(anyhow::anyhow!(
                    "job task join error: {e}"
                )))),
            }
        }
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    /// Build a future that, while it runs, bumps `live` and records the peak in
    /// `peak`, sleeps briefly, then decrements. Used to observe how many jobs
    /// the executor lets run at once.
    fn probe(
        live: Arc<AtomicUsize>,
        peak: Arc<AtomicUsize>,
        per_key_live: Arc<AtomicUsize>,
        per_key_peak: Arc<AtomicUsize>,
        same_key: bool,
    ) -> JobFuture {
        Box::pin(async move {
            let now = live.fetch_add(1, Ordering::SeqCst) + 1;
            peak.fetch_max(now, Ordering::SeqCst);
            if same_key {
                let k = per_key_live.fetch_add(1, Ordering::SeqCst) + 1;
                per_key_peak.fetch_max(k, Ordering::SeqCst);
            }
            tokio::time::sleep(Duration::from_millis(80)).await;
            if same_key {
                per_key_live.fetch_sub(1, Ordering::SeqCst);
            }
            live.fetch_sub(1, Ordering::SeqCst);
            Ok(true)
        })
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn distinct_keys_run_in_parallel() {
        let live = Arc::new(AtomicUsize::new(0));
        let peak = Arc::new(AtomicUsize::new(0));
        let dummy = Arc::new(AtomicUsize::new(0));
        let items: Vec<(String, JobFuture)> = (0..4)
            .map(|i| {
                (
                    format!("key-{i}"),
                    probe(
                        live.clone(),
                        peak.clone(),
                        dummy.clone(),
                        dummy.clone(),
                        false,
                    ),
                )
            })
            .collect();
        let exec = Executor {
            max_parallel: 4,
            gate: KeyGate::new(),
            cancel: CancellationToken::new(),
        };
        let results = exec.run(items).await;
        assert!(results.iter().all(|r| matches!(r, Ok(true))));
        // All four distinct keys overlapped under a cap of 4.
        assert_eq!(peak.load(Ordering::SeqCst), 4);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn same_key_jobs_serialize() {
        let live = Arc::new(AtomicUsize::new(0));
        let peak = Arc::new(AtomicUsize::new(0));
        let key_live = Arc::new(AtomicUsize::new(0));
        let key_peak = Arc::new(AtomicUsize::new(0));
        // Three jobs all sharing one key, generous parallelism budget.
        let items: Vec<(String, JobFuture)> = (0..3)
            .map(|_| {
                (
                    "same".to_string(),
                    probe(
                        live.clone(),
                        peak.clone(),
                        key_live.clone(),
                        key_peak.clone(),
                        true,
                    ),
                )
            })
            .collect();
        let exec = Executor {
            max_parallel: 4,
            gate: KeyGate::new(),
            cancel: CancellationToken::new(),
        };
        exec.run(items).await;
        // Never more than one same-key job in flight at a time.
        assert_eq!(key_peak.load(Ordering::SeqCst), 1);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn max_parallel_bounds_concurrency() {
        let live = Arc::new(AtomicUsize::new(0));
        let peak = Arc::new(AtomicUsize::new(0));
        let dummy = Arc::new(AtomicUsize::new(0));
        let items: Vec<(String, JobFuture)> = (0..6)
            .map(|i| {
                (
                    format!("key-{i}"),
                    probe(
                        live.clone(),
                        peak.clone(),
                        dummy.clone(),
                        dummy.clone(),
                        false,
                    ),
                )
            })
            .collect();
        let exec = Executor {
            max_parallel: 2,
            gate: KeyGate::new(),
            cancel: CancellationToken::new(),
        };
        exec.run(items).await;
        // Six distinct keys, but the cap holds the peak at 2.
        assert_eq!(peak.load(Ordering::SeqCst), 2);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn cancel_skips_unstarted_jobs() {
        let cancel = CancellationToken::new();
        cancel.cancel();
        let ran = Arc::new(AtomicUsize::new(0));
        let items: Vec<(String, JobFuture)> = (0..3)
            .map(|i| {
                let ran = ran.clone();
                let fut: JobFuture = Box::pin(async move {
                    ran.fetch_add(1, Ordering::SeqCst);
                    Ok(true)
                });
                (format!("key-{i}"), fut)
            })
            .collect();
        let exec = Executor {
            max_parallel: 2,
            gate: KeyGate::new(),
            cancel,
        };
        let results = exec.run(items).await;
        // Pre-cancelled: every job skips before its body runs.
        assert!(results.iter().all(|r| matches!(r, Ok(false))));
        assert_eq!(ran.load(Ordering::SeqCst), 0);
    }
}
