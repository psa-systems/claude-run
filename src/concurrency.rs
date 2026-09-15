//! CLAUDE-28: keyed, bounded-parallel job execution.
//!
//! A run's jobs used to execute strictly sequentially (one `claude` at a time),
//! serializing independent work. This module replaces that loop with a bounded
//! executor that runs distinct-key jobs in parallel while never running two jobs
//! that share a *concurrency key* (project / repo / workflow) at the same time.
//!
//! The primitives:
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
//! - [`GatherGate`] (CLAUDE-228): a per-action "gathering right now" marker. The
//!   scheduler skips a pickup for an action whose previous gather has not
//!   finished, so a slow `yt list` no longer stacks one gather per tick.
//! - [`Executor`]: spawns one task per job, each acquiring, in order, (a) its key
//!   gate (via a NON-BLOCKING try), then (b) a global permit, then (c) a
//!   per-action permit, then runs.
//!
//! A per-action `max_parallel == 1` reproduces the original strictly-sequential
//! behavior, now enforced daemon-wide (at most one job for that action across all
//! its overlapping runs).
//!
//! CLAUDE-163: acquisition order is gate-then-semaphores, and the gate is a
//! non-blocking `try`. CLAUDE-162 originally ordered it semaphores-then-gate so a
//! held key never blocked on a permit; the side effect was permit starvation. A
//! job that shared its concurrency key with a running sibling still grabbed a
//! global and per-action permit and then parked on the key mutex, holding those
//! permits while doing no work. With one project's backlog gathered into a run,
//! `min(N, cap)` permits were consumed while exactly one job ran, so every other
//! project and action was starved even though its key was free. Gating first with
//! a non-blocking try fixes it: a job whose key is busy takes NO permit and defers
//! (`Ok(false)`), left for the next pickup gather. Only a job that actually holds
//! its key ever holds a permit, so the permits held by an action equal the number
//! of its jobs running, never the number waiting on a key. There is still no
//! lock-order inversion: the key is only ever taken by a non-blocking try, so no
//! job waits on a permit while another waits on that key, and the pipeline stays
//! deadlock-free.

use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};

use tokio::sync::{Mutex as AsyncMutex, OwnedMutexGuard, OwnedSemaphorePermit, Semaphore};

pub use tokio_util::sync::CancellationToken;

use crate::error::{Error, Result};
use crate::pause::RunPause;

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

    /// CLAUDE-163: non-blocking acquire. Returns `Some(guard)` if no other holder
    /// of `key` is in flight, `None` if the key is already held. Used by the
    /// [`Executor`] to gate on the key *before* taking any permit: a job whose key
    /// is busy defers instead of parking on the mutex while holding scarce
    /// permits. Because the key is only ever taken by this try (never a blocking
    /// wait), no job ever waits on a permit while another waits on that key, so the
    /// gate-then-permit order introduces no lock-order inversion.
    pub fn try_acquire(&self, key: &str) -> Option<OwnedMutexGuard<()>> {
        let m = {
            let mut g = self.inner.lock().unwrap();
            g.entry(key.to_string()).or_default().clone()
        };
        m.try_lock_owned().ok()
    }
}

/// CLAUDE-228: a per-action marker for "this action is gathering right now".
///
/// Overlapping RUNS are deliberate (CLAUDE-162); overlapping GATHERS are not. A
/// gather is one `yt list` over the action's whole query, and under load that
/// call has been observed taking minutes, longer than the 60s pickup cadence.
/// Every stacked fire then re-ran the same query, each pinning another thread
/// and re-reading the same issues, so the pileup grew as long as the slowdown
/// lasted.
///
/// Advisory by design, and only for the scheduler: the tick skips a PICKUP for
/// an action already gathering ([`GatherGate::is_gathering`]), while a run that
/// does not win the marker still gathers. A manual trigger is an explicit
/// operator act and is never dropped.
#[derive(Clone, Default)]
pub struct GatherGate {
    inner: Arc<Mutex<HashSet<String>>>,
}

impl GatherGate {
    pub fn new() -> Self {
        Self::default()
    }

    /// Mark `action` as gathering. `None` when it already is; the caller keeps
    /// whatever it gets and gathers either way, so a trigger is never lost.
    pub fn try_acquire(&self, action: &str) -> Option<GatherGuard> {
        let mut g = self.inner.lock().unwrap();
        if !g.insert(action.to_string()) {
            return None;
        }
        Some(GatherGuard {
            inner: self.inner.clone(),
            action: action.to_string(),
        })
    }

    /// Whether a gather for `action` is in flight. The scheduler's fire check.
    pub fn is_gathering(&self, action: &str) -> bool {
        self.inner.lock().unwrap().contains(action)
    }
}

/// Clears its action's gathering marker on drop, including on an error return
/// or an early `?` out of the gather.
pub struct GatherGuard {
    inner: Arc<Mutex<HashSet<String>>>,
    action: String,
}

impl Drop for GatherGuard {
    fn drop(&mut self) {
        self.inner.lock().unwrap().remove(&self.action);
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
    /// that runs a sequential per-job loop rather than the parallel [`Executor`],
    /// so its jobs are still bounded by the daemon-wide caps across overlapping
    /// runs. The semaphores are never closed in this
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
    /// CLAUDE-223: the run-scoped global-pause gate. Checked at the same points
    /// as `cancel`, so a pause taken mid-run stops the NEXT job pickup instead
    /// of letting the run drain every queued job. A skipped job defers exactly
    /// like a key-contended one (`Ok(false)`): no attempt, re-gathered after
    /// Resume. The run finishes with `exit::PAUSED`, not `exit::CANCELLED`.
    pub pause: RunPause,
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
            let pause = self.pause.clone();
            handles.push(tokio::spawn(async move {
                // CLAUDE-163: gate on the concurrency key FIRST, with a
                // non-blocking try, before taking any permit. A job whose key is
                // already held by a running sibling takes no permit and defers
                // (`Ok(false)`); it is left Queued for the next pickup gather. Only
                // once the key is held do we take the global then per-action
                // permit, so a permit is never held by a job merely waiting on a
                // key (the CLAUDE-162 permit-starvation bug). Because the key is
                // only ever taken by this non-blocking try, no job waits on a
                // permit while another waits on that key: no lock-order inversion,
                // the pipeline stays deadlock-free.
                if cancel.is_cancelled() || pause.stop().await {
                    return Ok(false);
                }
                let _key_guard = match gate.try_acquire(&key) {
                    Some(g) => g,
                    None => return Ok(false),
                };
                let global = limiter.global();
                let _global_permit = match global.acquire_owned().await {
                    Ok(p) => p,
                    Err(_) => return Ok(false),
                };
                if cancel.is_cancelled() || pause.stop().await {
                    return Ok(false);
                }
                let action_sem = limiter.action(&action, max_parallel);
                let _action_permit = match action_sem.acquire_owned().await {
                    Ok(p) => p,
                    Err(_) => return Ok(false),
                };
                if cancel.is_cancelled() || pause.stop().await {
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
            pause: RunPause::never(),
        };
        let results = exec.run(items).await;
        assert!(results.iter().all(|r| matches!(r, Ok(true))));
        // All four distinct keys overlapped under a cap of 4.
        assert_eq!(peak.load(Ordering::SeqCst), 4);
    }

    /// CLAUDE-223: a global pause stops the executor from picking up any job.
    /// Each defers as `Ok(false)` (no work, no attempt, re-gathered after
    /// Resume) and the run's gate is left tripped, so it finishes with
    /// `exit::PAUSED` rather than `exit::OK` or `exit::CANCELLED`.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_global_pause_stops_the_next_job_pickup() {
        let pool = crate::db::test_pool().await;
        crate::state::global_pause_set(&pool, Some("reboot"))
            .await
            .unwrap();

        let live = Arc::new(AtomicUsize::new(0));
        let peak = Arc::new(AtomicUsize::new(0));
        let dummy = Arc::new(AtomicUsize::new(0));
        let items: Vec<(String, JobFuture)> = (0..3)
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
        let pause = crate::pause::PauseGate::new(pool).for_action_run("a");
        let exec = Executor {
            action: "a".into(),
            max_parallel: 4,
            limiter: Limiter::new(4),
            gate: KeyGate::new(),
            cancel: CancellationToken::new(),
            pause: pause.clone(),
        };
        let results = exec.run(items).await;
        assert!(
            results.iter().all(|r| matches!(r, Ok(false))),
            "a paused run must pick up nothing: {results:?}"
        );
        assert_eq!(peak.load(Ordering::SeqCst), 0, "no job body ran");
        assert!(pause.tripped());
        assert_eq!(pause.exit_code(false), crate::exit::PAUSED);
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
            pause: RunPause::never(),
        };
        let results = exec.run(items).await;
        // Never more than one same-key job in flight at a time.
        assert_eq!(key_peak.load(Ordering::SeqCst), 1);
        // CLAUDE-163: only one same-key job runs per pass; the other two find the
        // key held and defer (`Ok(false)`) without taking a permit or running.
        assert_eq!(results.iter().filter(|r| matches!(r, Ok(true))).count(), 1);
        assert_eq!(results.iter().filter(|r| matches!(r, Ok(false))).count(), 2);
    }

    // CLAUDE-163: a job whose concurrency key is already held takes NO permit and
    // defers; only one same-key job runs per pass. With N same-key jobs and a
    // global/per-action cap of C, at most one permit is ever held on their
    // account, so the semaphore never drops below C-1 available.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn same_key_backlog_holds_at_most_one_permit() {
        let limiter = Limiter::new(5);
        let min_global = Arc::new(AtomicUsize::new(usize::MAX));
        let ran = Arc::new(AtomicUsize::new(0));
        let n = 7usize;
        let items: Vec<(String, JobFuture)> = (0..n)
            .map(|_| {
                let limiter = limiter.clone();
                let min_global = min_global.clone();
                let ran = ran.clone();
                let fut: JobFuture = Box::pin(async move {
                    ran.fetch_add(1, Ordering::SeqCst);
                    // Sampled while this job holds its permit: never below cap-1.
                    let avail = limiter.global().available_permits();
                    min_global.fetch_min(avail, Ordering::SeqCst);
                    tokio::time::sleep(Duration::from_millis(60)).await;
                    Ok(true)
                });
                ("alpha".to_string(), fut)
            })
            .collect();
        let exec = Executor {
            action: "youtrack".into(),
            max_parallel: 5,
            limiter: limiter.clone(),
            gate: KeyGate::new(),
            cancel: CancellationToken::new(),
            pause: RunPause::never(),
        };
        let results = exec.run(items).await;
        // Exactly one same-key job ran; the other six deferred without a permit.
        assert_eq!(ran.load(Ordering::SeqCst), 1);
        assert_eq!(results.iter().filter(|r| matches!(r, Ok(true))).count(), 1);
        assert_eq!(
            results.iter().filter(|r| matches!(r, Ok(false))).count(),
            n - 1
        );
        // Cap 5, exactly one permit ever held -> min available observed is 4.
        assert_eq!(min_global.load(Ordering::SeqCst), 4);
    }

    // CLAUDE-163: an unrelated key runs immediately alongside a large same-key
    // backlog. The backlog's parked siblings no longer consume permits, so a
    // second project (distinct key) is never starved behind a single serialized
    // key draining.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn distinct_key_runs_alongside_saturated_same_key_backlog() {
        let distinct_ran = Arc::new(AtomicUsize::new(0));
        let mut items: Vec<(String, JobFuture)> = Vec::new();
        // Ten jobs for one project (the concentrated backlog).
        for _ in 0..10 {
            let fut: JobFuture = Box::pin(async move {
                tokio::time::sleep(Duration::from_millis(60)).await;
                Ok(true)
            });
            items.push(("alpha".to_string(), fut));
        }
        // One job for a second project with a free key.
        {
            let distinct_ran = distinct_ran.clone();
            let fut: JobFuture = Box::pin(async move {
                distinct_ran.fetch_add(1, Ordering::SeqCst);
                Ok(true)
            });
            items.push(("beta".to_string(), fut));
        }
        let exec = Executor {
            action: "youtrack".into(),
            max_parallel: 5,
            limiter: Limiter::new(5),
            gate: KeyGate::new(),
            cancel: CancellationToken::new(),
            pause: RunPause::never(),
        };
        let results = exec.run(items).await;
        // The unrelated key ran despite the backlog holding the youtrack key.
        assert_eq!(distinct_ran.load(Ordering::SeqCst), 1);
        // One alpha + one beta ran; the other nine alpha jobs deferred.
        assert_eq!(results.iter().filter(|r| matches!(r, Ok(true))).count(), 2);
    }

    // CLAUDE-163: heavy contention across a few keys with tight caps completes
    // without deadlock. The gate-then-permit order (non-blocking gate) introduces
    // no lock-order inversion, so the run always makes progress and terminates.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn contended_keys_make_progress_without_deadlock() {
        let items: Vec<(String, JobFuture)> = (0..12)
            .map(|i| {
                let fut: JobFuture = Box::pin(async move {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                    Ok(true)
                });
                // Three distinct keys, several jobs each, under a cap of 2.
                (format!("key-{}", i % 3), fut)
            })
            .collect();
        let exec = Executor {
            action: "a".into(),
            max_parallel: 2,
            limiter: Limiter::new(2),
            gate: KeyGate::new(),
            cancel: CancellationToken::new(),
            pause: RunPause::never(),
        };
        let results = tokio::time::timeout(Duration::from_secs(5), exec.run(items))
            .await
            .expect("executor completed without deadlock");
        // Every job resolved (ran or deferred); none hung.
        assert_eq!(results.len(), 12);
        assert!(results.iter().all(|r| r.is_ok()));
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
            pause: RunPause::never(),
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
            pause: RunPause::never(),
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
                pause: RunPause::never(),
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
                pause: RunPause::never(),
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
                pause: RunPause::never(),
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

    // CLAUDE-228: the gather marker is per action, non-blocking, and released on
    // drop, including when the gather returns early with an error.
    #[test]
    fn gather_gate_marks_one_action_at_a_time() {
        let gate = GatherGate::new();
        assert!(!gate.is_gathering("a"));

        let held = gate.try_acquire("a").expect("a free action is acquirable");
        assert!(gate.is_gathering("a"));
        assert!(
            gate.try_acquire("a").is_none(),
            "a second acquire for the same action is refused"
        );
        // A different action is unaffected: one slow gather never defers another
        // action's pickup.
        let other = gate
            .try_acquire("b")
            .expect("distinct actions are independent");
        assert!(gate.is_gathering("b"));

        drop(held);
        assert!(!gate.is_gathering("a"), "released on drop");
        assert!(gate.is_gathering("b"), "dropping one leaves the other held");
        assert!(
            gate.try_acquire("a").is_some(),
            "the action is acquirable again once released"
        );
        drop(other);
        assert!(!gate.is_gathering("b"));
    }

    // The guard is dropped by an early `?` out of a gather, not just a clean
    // return, so a failing gather cannot wedge the action's pickups forever.
    #[test]
    fn gather_gate_releases_when_the_gather_errors() {
        let gate = GatherGate::new();
        fn gather_that_fails(gate: &GatherGate) -> std::result::Result<(), &'static str> {
            let _marker = gate.try_acquire("a");
            Err("gather blew up")
        }
        assert!(gather_that_fails(&gate).is_err());
        assert!(
            !gate.is_gathering("a"),
            "an error return still releases the marker"
        );
    }
}
