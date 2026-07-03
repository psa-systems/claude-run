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
//! - [`Limiter`] (CLAUDE-162): the daemon-wide, config-rebuilt semaphore pair. A
//!   single global semaphore caps total concurrent jobs across every action; a
//!   per-action semaphore caps concurrent jobs for one action. Both are shared
//!   and daemon-lived, so a cap holds across *overlapping* runs of an action, not
//!   just within one run (the old per-run [`Semaphore`] only bounded a single
//!   run's snapshot).
//! - [`Executor`]: spawns one task per job, each acquiring, in order, (a) a
//!   global permit, (b) a per-action permit, then (c) its key gate so same-key
//!   jobs serialize.
//!
//! A per-action `max_parallel == 1` reproduces the original strictly-sequential
//! behavior, now enforced daemon-wide (at most one job for that action across all
//! its overlapping runs).
//!
//! Acquisition order is semaphores-then-gate on purpose (CLAUDE-162): a held key
//! never blocks on a permit, and the fixed global -> per-action -> key ordering is
//! consistent across every acquirer, so there is no lock-order inversion and the
//! pipeline is deadlock-free.

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};

use tokio::sync::{Mutex as AsyncMutex, OwnedMutexGuard, OwnedSemaphorePermit, Semaphore};

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

/// CLAUDE-162: the daemon-wide concurrency limiter. Holds a global semaphore
/// (total concurrent jobs across all actions) plus one semaphore per action name
/// (concurrent jobs for that action). Shared behind an `Arc`, so the daemon hands
/// the same limiter to every run and a cap holds across overlapping runs. The
/// global size is rebuilt on a config change ([`Limiter::set_global_max`]); a
/// per-action semaphore is created on first use and rebuilt if the action's
/// `max_parallel` changes (a YAML edit). Rebuilding swaps the semaphore, so
/// permits already held drain against the old one and the new size is only exact
/// once those in-flight jobs finish; new jobs acquire against the new size at
/// once. A `max_parallel_global` of 0 is resolved to an effective value by the
/// caller before it reaches here, so the limiter always holds a concrete cap.
#[derive(Clone)]
pub struct Limiter {
    inner: Arc<Mutex<LimiterInner>>,
}

struct LimiterInner {
    global_max: usize,
    global: Arc<Semaphore>,
    /// action name -> (its configured `max_parallel`, its semaphore).
    per_action: HashMap<String, (usize, Arc<Semaphore>)>,
}

impl Limiter {
    /// Build a limiter whose global cap is `global_max` (floored to 1).
    pub fn new(global_max: usize) -> Self {
        let global_max = global_max.max(1);
        Self {
            inner: Arc::new(Mutex::new(LimiterInner {
                global_max,
                global: Arc::new(Semaphore::new(global_max)),
                per_action: HashMap::new(),
            })),
        }
    }

    /// Current global cap.
    pub fn global_max(&self) -> usize {
        self.inner.lock().unwrap().global_max
    }

    /// Rebuild the global semaphore to `n` permits (floored to 1) when it
    /// changed. A no-op when `n` is unchanged, so calling it every scheduler tick
    /// is cheap.
    pub fn set_global_max(&self, n: usize) {
        let n = n.max(1);
        let mut g = self.inner.lock().unwrap();
        if g.global_max != n {
            g.global_max = n;
            g.global = Arc::new(Semaphore::new(n));
        }
    }

    /// Snapshot the current global semaphore.
    fn global(&self) -> Arc<Semaphore> {
        self.inner.lock().unwrap().global.clone()
    }

    /// Get-or-create the per-action semaphore sized `max_parallel` (floored to
    /// 1). Rebuilt when the stored cap differs (a `max_parallel` YAML edit).
    fn action(&self, action: &str, max_parallel: usize) -> Arc<Semaphore> {
        let max_parallel = max_parallel.max(1);
        let mut g = self.inner.lock().unwrap();
        match g.per_action.get(action) {
            Some((cap, sem)) if *cap == max_parallel => sem.clone(),
            _ => {
                let sem = Arc::new(Semaphore::new(max_parallel));
                g.per_action
                    .insert(action.to_string(), (max_parallel, sem.clone()));
                sem
            }
        }
    }

    /// CLAUDE-162: acquire a global then per-action permit, in that fixed order,
    /// returning both owned permits (release together on drop). For a handler
    /// that runs a sequential per-job loop rather than the parallel [`Executor`]
    /// (e.g. `forgejo-ci-fix`), so its jobs are still bounded by the daemon-wide
    /// caps across overlapping runs. The semaphores are never closed in this
    /// crate, so acquisition cannot fail.
    pub async fn acquire(
        &self,
        action: &str,
        max_parallel: usize,
    ) -> (OwnedSemaphorePermit, OwnedSemaphorePermit) {
        let global = self.global();
        let gp = global
            .acquire_owned()
            .await
            .expect("global semaphore is never closed");
        let action_sem = self.action(action, max_parallel);
        let ap = action_sem
            .acquire_owned()
            .await
            .expect("per-action semaphore is never closed");
        (gp, ap)
    }

    /// CLAUDE-162: true when a job for `action` could acquire a permit right now
    /// under both caps, without taking one. A best-effort admission gate the
    /// scheduler consults before starting an overlapping run so it does not pile
    /// up runs that would immediately block on a saturated cap. Racy by nature
    /// (permits can be taken between the check and the acquire); the executor's
    /// real `acquire` is the authority.
    pub fn can_admit(&self, action: &str, max_parallel: usize) -> bool {
        let g = self.inner.lock().unwrap();
        if g.global.available_permits() == 0 {
            return false;
        }
        match g.per_action.get(action) {
            // An action whose semaphore does not exist yet has run nothing, so a
            // permit is trivially free.
            Some((_, sem)) => sem.available_permits() > 0,
            None => max_parallel.max(1) > 0,
        }
    }
}

/// Bounded keyed executor. Runs each `(key, future)` pair with the
/// semaphores-then-gate discipline described in the module docs, acquiring from
/// the shared [`Limiter`] so caps hold daemon-wide across overlapping runs.
pub struct Executor {
    /// This run's action name, used to resolve the per-action semaphore.
    pub action: String,
    /// The action's configured `max_parallel` (per-action cap). Floored to 1.
    pub max_parallel: usize,
    /// Daemon-wide shared limiter (global + per-action semaphores).
    pub limiter: Limiter,
    pub gate: KeyGate,
    /// The run's cancellation token. A tripped token skips jobs that have not
    /// started yet (their future is dropped unpolled); jobs already running tear
    /// down through their own clone of the token inside the job pipeline.
    pub cancel: CancellationToken,
}

impl Executor {
    /// Run all `items` concurrently under the key gate and the daemon-wide caps,
    /// returning one result per item in input order. A panicked task surfaces as
    /// an `Err` rather than aborting the whole run.
    pub async fn run(&self, items: Vec<(String, JobFuture)>) -> Vec<Result<bool>> {
        let mut handles = Vec::with_capacity(items.len());
        for (key, fut) in items {
            let gate = self.gate.clone();
            let cancel = self.cancel.clone();
            let limiter = self.limiter.clone();
            let action = self.action.clone();
            let max_parallel = self.max_parallel;
            handles.push(tokio::spawn(async move {
                // CLAUDE-162: fixed acquisition order, consistent across every
                // acquirer: (a) global permit, (b) per-action permit, then (c)
                // the key gate. Semaphores before the key mutex means a job that
                // is waiting on its key holds no permit against which a distinct
                // key could block, and the single ordering rules out any
                // lock-order inversion.
                let global = limiter.global();
                let _global_permit = match global.acquire_owned().await {
                    Ok(p) => p,
                    Err(_) => return Ok(false),
                };
                if cancel.is_cancelled() {
                    return Ok(false);
                }
                let action_sem = limiter.action(&action, max_parallel);
                let _action_permit = match action_sem.acquire_owned().await {
                    Ok(p) => p,
                    Err(_) => return Ok(false),
                };
                if cancel.is_cancelled() {
                    return Ok(false);
                }
                let _key_guard = gate.acquire(&key).await;
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
            action: "a".into(),
            max_parallel: 4,
            limiter: Limiter::new(4),
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
            action: "a".into(),
            max_parallel: 4,
            limiter: Limiter::new(4),
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
            action: "a".into(),
            max_parallel: 2,
            // Generous global so the per-action cap of 2 is what bounds the peak.
            limiter: Limiter::new(6),
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
            action: "a".into(),
            max_parallel: 2,
            limiter: Limiter::new(2),
            gate: KeyGate::new(),
            cancel,
        };
        let results = exec.run(items).await;
        // Pre-cancelled: every job skips before its body runs.
        assert!(results.iter().all(|r| matches!(r, Ok(false))));
        assert_eq!(ran.load(Ordering::SeqCst), 0);
    }

    // CLAUDE-162: the per-action cap holds across two OVERLAPPING runs (two
    // Executors sharing one Limiter), not just within a single run. With a
    // per-action cap of 2 and a generous global, six distinct-key jobs split
    // across two concurrent runs still never exceed two in flight for the action.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn per_action_cap_holds_across_overlapping_runs() {
        let live = Arc::new(AtomicUsize::new(0));
        let peak = Arc::new(AtomicUsize::new(0));
        let dummy = Arc::new(AtomicUsize::new(0));
        let limiter = Limiter::new(16);
        let gate = KeyGate::new();
        let mk = |run: usize| {
            let items: Vec<(String, JobFuture)> = (0..3)
                .map(|i| {
                    (
                        format!("run{run}-key{i}"),
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
                action: "shared".into(),
                max_parallel: 2,
                limiter: limiter.clone(),
                gate: gate.clone(),
                cancel: CancellationToken::new(),
            };
            async move { exec.run(items).await }
        };
        let (a, b) = tokio::join!(mk(0), mk(1));
        assert!(a.iter().chain(b.iter()).all(|r| matches!(r, Ok(true))));
        // Two overlapping runs, six distinct keys, one shared per-action cap of 2.
        assert_eq!(peak.load(Ordering::SeqCst), 2);
    }

    // CLAUDE-162: the global cap bounds total concurrent jobs across DIFFERENT
    // actions. Two actions each with a per-action cap of 4 (so neither alone
    // limits) share a global cap of 3; eight distinct-key jobs across the two
    // runs never exceed three in flight.
    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    async fn global_cap_bounds_concurrency_across_actions() {
        let live = Arc::new(AtomicUsize::new(0));
        let peak = Arc::new(AtomicUsize::new(0));
        let dummy = Arc::new(AtomicUsize::new(0));
        let limiter = Limiter::new(3);
        let gate = KeyGate::new();
        let mk = |action: &'static str| {
            let items: Vec<(String, JobFuture)> = (0..4)
                .map(|i| {
                    (
                        format!("{action}-key{i}"),
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
                action: action.into(),
                max_parallel: 4,
                limiter: limiter.clone(),
                gate: gate.clone(),
                cancel: CancellationToken::new(),
            };
            async move { exec.run(items).await }
        };
        let (a, b) = tokio::join!(mk("alpha"), mk("beta"));
        assert!(a.iter().chain(b.iter()).all(|r| matches!(r, Ok(true))));
        // Global cap of 3 holds despite two 4-wide actions and eight distinct keys.
        assert_eq!(peak.load(Ordering::SeqCst), 3);
    }

    // CLAUDE-162: same-key jobs stay serialized across two overlapping runs (the
    // daemon-wide KeyGate), so an issue queued into a second run against a repo a
    // first run is still working never runs concurrently with it.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn same_key_serializes_across_overlapping_runs() {
        let live = Arc::new(AtomicUsize::new(0));
        let peak = Arc::new(AtomicUsize::new(0));
        let key_live = Arc::new(AtomicUsize::new(0));
        let key_peak = Arc::new(AtomicUsize::new(0));
        let limiter = Limiter::new(16);
        let gate = KeyGate::new();
        let mk = || {
            let items: Vec<(String, JobFuture)> = vec![(
                "same-repo".to_string(),
                probe(
                    live.clone(),
                    peak.clone(),
                    key_live.clone(),
                    key_peak.clone(),
                    true,
                ),
            )];
            let exec = Executor {
                action: "shared".into(),
                max_parallel: 8,
                limiter: limiter.clone(),
                gate: gate.clone(),
                cancel: CancellationToken::new(),
            };
            async move { exec.run(items).await }
        };
        tokio::join!(mk(), mk());
        // The one shared key never has two holders at once, even across runs.
        assert_eq!(key_peak.load(Ordering::SeqCst), 1);
    }

    // CLAUDE-162: `can_admit` reflects live permit availability and `set_global_max`
    // rebuilds the global cap. A fresh limiter admits; once its only global permit
    // is held, it refuses; raising the cap admits again.
    #[tokio::test]
    async fn limiter_admission_and_global_resize() {
        let limiter = Limiter::new(1);
        assert!(limiter.can_admit("a", 4), "fresh limiter admits");
        let g = limiter.global();
        let _held = g.acquire_owned().await.unwrap();
        assert!(
            !limiter.can_admit("a", 4),
            "no global permit free -> refuse admission"
        );
        // Raising the global cap swaps in a new, unsaturated semaphore.
        limiter.set_global_max(4);
        assert_eq!(limiter.global_max(), 4);
        assert!(limiter.can_admit("a", 4), "resized global admits again");
        // A no-op resize keeps the same size.
        limiter.set_global_max(4);
        assert_eq!(limiter.global_max(), 4);
    }
}
