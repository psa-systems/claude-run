//! The global-pause gate consulted from inside a running run.
//!
//! `claude-run pause` (and the web Activity card) sets a one-row DB flag that
//! the scheduler and the dispatcher already honour BEFORE starting anything.
//! This module is the other half: a gate the per-job checkpoints consult so a
//! run already in flight stops picking up NEW jobs. That bounds the operator's
//! restart window to the one `claude --print` child already executing, instead
//! of every job still queued in the run.
//!
//! Reads are TTL-cached, because a per-job check must not become a per-job DB
//! round trip, and the last value is kept in an atomic so the blocking native
//! paths (the automerge CI wait loop) can read it with no runtime handle.
//!
//! Error handling: a failed read is logged at `error` and reported as NOT
//! paused. Treating an unreadable gate as paused would silently stop the whole
//! daemon on a transient DB error, which is the worse failure; the log line
//! always comes first, and no read is collapsed into a bare `unwrap_or`.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use crate::db::DbPool;
use tracing::{error, info};

use crate::state;

/// How long one gate read is reused before the DB is consulted again. Short
/// enough that a Pause takes effect within a job or two, long enough that a
/// parallel run does not hammer SQLite once per job checkpoint.
const DEFAULT_TTL: Duration = Duration::from_millis(1500);

/// Daemon-wide reader of the `global_pause` flag, cheap to clone.
#[derive(Clone)]
pub struct PauseGate {
    /// `None` on the inert gate used by tests and by callers with no DB.
    pool: Option<DbPool>,
    /// Last known state, shared so a blocking path can read it without `await`.
    paused: Arc<AtomicBool>,
    /// When `paused` was last read from the DB; `None` means never.
    read_at: Arc<Mutex<Option<Instant>>>,
    ttl: Duration,
    /// Carried into every [`RunPause`] this gate mints, so a run
    /// already in flight stops picking up new jobs when the Claude login needs
    /// renewing, with no new call site at any of the existing per-job
    /// checkpoints. `None` where there is nothing to check (tests, the
    /// `never()` gate).
    readiness: Option<crate::readiness::ReadinessGate>,
}

impl PauseGate {
    pub fn new(pool: DbPool) -> Self {
        Self {
            pool: Some(pool),
            paused: Arc::new(AtomicBool::new(false)),
            read_at: Arc::new(Mutex::new(None)),
            ttl: DEFAULT_TTL,
            readiness: None,
        }
    }

    /// Attach the daemon's readiness gate, so [`RunPause::stop`] /
    /// [`RunPause::stop_cached`] also stop a run when the Claude login needs
    /// renewing. A builder so every existing `PauseGate::new` call site stays
    /// source-compatible.
    pub fn with_readiness(mut self, gate: crate::readiness::ReadinessGate) -> Self {
        self.readiness = Some(gate);
        self
    }

    /// Override the cache TTL, so a test asserting cached reads does not depend
    /// on how long a DB write takes on the backend under test.
    #[cfg(test)]
    fn with_ttl(mut self, ttl: Duration) -> Self {
        self.ttl = ttl;
        self
    }

    /// An inert gate that is never paused. Used where there is no DB to read
    /// (unit tests, the executor's own tests) so the gate is never a reason a
    /// job is skipped.
    pub fn never() -> Self {
        Self {
            pool: None,
            paused: Arc::new(AtomicBool::new(false)),
            read_at: Arc::new(Mutex::new(None)),
            ttl: DEFAULT_TTL,
            readiness: None,
        }
    }

    /// True when the daemon is globally paused, reusing a read younger than the
    /// TTL. A read failure is logged at `error` and reported as not paused.
    pub async fn paused(&self) -> bool {
        let fresh = self
            .read_at
            .lock()
            .expect("pause gate mutex poisoned")
            .is_some_and(|t| t.elapsed() < self.ttl);
        if fresh {
            return self.last_known();
        }
        self.refresh().await
    }

    /// Read the flag from the DB, ignoring the cache, and publish the result.
    /// A read failure is logged at `error` and reported as not paused: an
    /// unreadable gate must not silently stop the whole daemon.
    pub async fn refresh(&self) -> bool {
        let Some(pool) = self.pool.as_ref() else {
            return false;
        };
        let paused = match state::global_pause_status(pool).await {
            Ok(status) => status.is_some(),
            Err(e) => {
                error!(error = %e, "global pause read failed at a job gate; treating as NOT paused so a transient DB error cannot stop the daemon");
                false
            }
        };
        self.paused.store(paused, Ordering::SeqCst);
        *self.read_at.lock().expect("pause gate mutex poisoned") = Some(Instant::now());
        paused
    }

    /// The last value published by [`Self::paused`] / [`Self::refresh`], with
    /// no DB access. For the blocking native paths, which cannot `await`; the
    /// daemon's scheduler tick refreshes the gate every cycle, so the value is
    /// never more than one tick stale.
    pub fn last_known(&self) -> bool {
        self.paused.load(Ordering::SeqCst)
    }

    /// A run-scoped view of this gate. The run finishes with `exit::PAUSED`
    /// when the returned handle ever reported paused.
    pub fn for_run(&self) -> RunPause {
        RunPause {
            gate: self.clone(),
            tripped: Arc::new(AtomicBool::new(false)),
            login_tripped: Arc::new(AtomicBool::new(false)),
            action: None,
        }
    }

    /// [`Self::for_run`] with the action name for the one-time log line.
    pub fn for_action_run(&self, action: &str) -> RunPause {
        RunPause {
            gate: self.clone(),
            tripped: Arc::new(AtomicBool::new(false)),
            login_tripped: Arc::new(AtomicBool::new(false)),
            action: Some(Arc::from(action)),
        }
    }
}

/// Run-scoped gate handle: the shared [`PauseGate`] plus a record of whether
/// the pause actually stopped work in THIS run. Cloned into every per-job task,
/// so any job's trip is visible when the run picks its exit code.
#[derive(Clone)]
pub struct RunPause {
    gate: PauseGate,
    tripped: Arc<AtomicBool>,
    /// Set the first time this run was stopped because the Claude
    /// login needs renewing, rather than by an operator pause. Kept separate
    /// from `tripped` so the run's exit code can tell the two apart.
    login_tripped: Arc<AtomicBool>,
    action: Option<Arc<str>>,
}

impl RunPause {
    /// An inert handle that never stops anything (tests, and the standalone
    /// paths that have already taken the up-front pause check).
    pub fn never() -> Self {
        PauseGate::never().for_run()
    }

    /// True when the run must stop picking up new work: either the operator
    /// paused globally, or the Claude login needs renewing.
    /// Records whichever tripped, so the run finalizes with the right exit
    /// code, and logs the reason once.
    pub async fn stop(&self) -> bool {
        let paused = self.gate.paused().await;
        if paused {
            self.record();
            return true;
        }
        if self.login_required() {
            self.record_login();
            return true;
        }
        false
    }

    /// [`Self::stop`] for the blocking paths: reads the last known value with
    /// no DB access (see [`PauseGate::last_known`]).
    pub fn stop_cached(&self) -> bool {
        let paused = self.gate.last_known();
        if paused {
            self.record();
            return true;
        }
        if self.login_required() {
            self.record_login();
            return true;
        }
        false
    }

    fn login_required(&self) -> bool {
        self.gate
            .readiness
            .as_ref()
            .is_some_and(|g| g.is_login_required())
    }

    /// True when the pause stopped work at least once during this run.
    pub fn tripped(&self) -> bool {
        self.tripped.load(Ordering::SeqCst)
    }

    /// True when the Claude login stopped work at least once during this run.
    pub fn login_tripped(&self) -> bool {
        self.login_tripped.load(Ordering::SeqCst)
    }

    /// The run's exit code: an operator cancel wins over everything, a global
    /// pause wins over a login-required stop, and either wins over a clean
    /// finish. One helper so every runner spells the choice the same way and
    /// each outcome is never mistaken for another in the run list.
    pub fn exit_code(&self, cancelled: bool) -> i32 {
        if cancelled {
            crate::exit::CANCELLED
        } else if self.tripped() {
            crate::exit::PAUSED
        } else if self.login_tripped() {
            crate::exit::LOGIN_REQUIRED
        } else {
            crate::exit::OK
        }
    }

    fn record(&self) {
        if !self.tripped.swap(true, Ordering::SeqCst) {
            match self.action.as_deref() {
                Some(action) => {
                    info!(action, "globally paused; the run stops picking up new jobs")
                }
                None => info!("globally paused; the run stops picking up new jobs"),
            }
        }
    }

    fn record_login(&self) {
        if !self.login_tripped.swap(true, Ordering::SeqCst) {
            match self.action.as_deref() {
                Some(action) => {
                    info!(
                        action,
                        "Claude login required; the run stops picking up new jobs"
                    )
                }
                None => info!("Claude login required; the run stops picking up new jobs"),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn pool() -> DbPool {
        crate::db::test_pool().await
    }

    /// The inert gate is never paused and never trips a run.
    #[tokio::test]
    async fn never_gate_is_never_paused() {
        let run = RunPause::never();
        assert!(!run.stop().await);
        assert!(!run.stop_cached());
        assert!(!run.tripped());
        assert_eq!(run.exit_code(false), crate::exit::OK);
    }

    /// A set flag stops the run, records the trip, and publishes the value to
    /// the blocking (`last_known`) view.
    #[tokio::test]
    async fn a_set_flag_stops_the_run_and_reaches_the_blocking_view() {
        let pool = pool().await;
        let gate = PauseGate::new(pool.clone());
        let run = gate.for_action_run("t");
        assert!(!run.stop().await);
        assert!(!run.tripped());

        state::global_pause_set(&pool, Some("reboot"))
            .await
            .unwrap();
        // A forced refresh bypasses the TTL the first read installed.
        assert!(gate.refresh().await);
        assert!(run.stop().await);
        assert!(run.tripped());
        // Published to the atomic, so the blocking paths see it too.
        assert!(gate.last_known());
        assert!(RunPause::never().gate.pool.is_none());
    }

    /// A tripped run finishes with `exit::PAUSED`, distinct from both a clean
    /// finish and an operator cancel.
    #[tokio::test]
    async fn exit_code_distinguishes_paused_from_cancelled_and_ok() {
        let pool = pool().await;
        state::global_pause_set(&pool, None).await.unwrap();
        let run = PauseGate::new(pool).for_run();
        assert_eq!(run.exit_code(false), crate::exit::OK);
        assert!(run.stop().await);
        assert_eq!(run.exit_code(false), crate::exit::PAUSED);
        // An operator cancel still wins: the two outcomes stay distinct.
        assert_eq!(run.exit_code(true), crate::exit::CANCELLED);
        assert_ne!(crate::exit::PAUSED, crate::exit::CANCELLED);
        assert_ne!(crate::exit::PAUSED, crate::exit::OK);
    }

    /// Within the TTL a second check reuses the cached value; `refresh` always
    /// re-reads. Proves the per-job check is not a per-job DB round trip.
    #[tokio::test]
    async fn reads_are_ttl_cached() {
        let pool = pool().await;
        // An hour-long TTL: the flag write below can take seconds on a loaded
        // Postgres, and a real-time TTL would expire mid-test.
        let gate = PauseGate::new(pool.clone()).with_ttl(Duration::from_secs(3600));
        assert!(!gate.paused().await);
        state::global_pause_set(&pool, None).await.unwrap();
        // Still inside the TTL: the cached `false` is reused.
        assert!(!gate.paused().await);
        assert!(gate.refresh().await);
        assert!(gate.paused().await);
    }

    /// A login-required trip stops a run exactly like a global
    /// pause does, and the run's exit code names it distinctly (`LOGIN_REQUIRED`
    /// rather than `PAUSED`), including against an operator cancel.
    #[tokio::test]
    async fn login_required_stops_the_run_with_its_own_exit_code() {
        let pool = pool().await;
        let readiness = crate::readiness::ReadinessGate::new(
            tempfile::tempdir().unwrap().path().to_path_buf(),
            None,
        );
        let gate = PauseGate::new(pool).with_readiness(readiness.clone());
        let run = gate.for_action_run("t");
        assert!(!run.stop().await);
        assert_eq!(run.exit_code(false), crate::exit::OK);

        readiness.trip_login_required("OAuth session expired", "invocation");
        assert!(run.stop().await);
        assert!(run.login_tripped());
        assert!(!run.tripped(), "a login trip is not a pause trip");
        assert_eq!(run.exit_code(false), crate::exit::LOGIN_REQUIRED);
        // An operator cancel still wins over a login-required stop.
        assert_eq!(run.exit_code(true), crate::exit::CANCELLED);
        assert_ne!(crate::exit::LOGIN_REQUIRED, crate::exit::PAUSED);
    }

    /// A global pause and a login-required trip can both hold; the pause's exit
    /// code wins, matching the "a global pause keeps its code when both hold"
    /// contract.
    #[tokio::test]
    async fn a_global_pause_wins_over_a_login_required_trip() {
        let pool = pool().await;
        let readiness = crate::readiness::ReadinessGate::new(
            tempfile::tempdir().unwrap().path().to_path_buf(),
            None,
        );
        readiness.trip_login_required("expired", "invocation");
        let gate = PauseGate::new(pool.clone()).with_readiness(readiness);
        let run = gate.for_action_run("t");

        state::global_pause_set(&pool, Some("reboot"))
            .await
            .unwrap();
        assert!(gate.refresh().await);
        assert!(run.stop().await);
        assert!(run.tripped());
        assert_eq!(run.exit_code(false), crate::exit::PAUSED);
    }

    /// With no readiness gate attached (the default), a `PauseGate` behaves
    /// exactly as before: login-required can never be the reason a run stops.
    #[tokio::test]
    async fn no_readiness_attached_never_trips_login_required() {
        let pool = pool().await;
        let run = PauseGate::new(pool).for_run();
        assert!(!run.stop().await);
        assert!(!run.login_tripped());
    }
}
