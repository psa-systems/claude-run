//! `claude-run test-postgres`: a throwaway, non-durable Postgres for an agent
//! session's own Postgres-backed test runs (CLAUDE-404).
//!
//! CLAUDE-403 stopped a `claude` child from reaching the runner's live
//! database. This subcommand is the replacement: it starts a private
//! `postgres` container in the Docker-in-Docker sidecar every agent session
//! already has, disables durability (fsync/synchronous_commit/full_page_writes
//! off; a throwaway database does not need to survive a crash, and skipping
//! those writes is most of the measured speedup), runs `<command>` with
//! `DATABASE_URL` / `CLAUDE_RUN_TEST_DATABASE_URL` pointed at it, and tears the
//! container down no matter how the command ends: normal exit, failure,
//! SIGINT/SIGTERM, or a failed startup step. An orphan sweep at the start of
//! every invocation stops any earlier container whose owning pid has since
//! died, since containers started by hand are not reliably removed.

use std::io::Write as _;
use std::os::unix::process::ExitStatusExt;
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicI32, Ordering};
use std::time::{Duration, Instant};

use tracing::warn;

use crate::error::{Error, Result};

pub const DEFAULT_IMAGE: &str = "docker.io/postgres:18.4-alpine3.23";
pub const LABEL_KEY: &str = "claude-run.test-postgres";

const PG_READY_TIMEOUT: Duration = Duration::from_secs(60);
const POLL_INTERVAL: Duration = Duration::from_millis(100);

/// `--image` / `--init-sql` options for a `test-postgres` invocation.
#[derive(Debug, Clone, Default)]
pub struct Opts {
    pub image: Option<String>,
    pub init_sql: Vec<PathBuf>,
}

impl Opts {
    fn image(&self) -> &str {
        self.image.as_deref().unwrap_or(DEFAULT_IMAGE)
    }
}

/// The signal most recently caught by our handler, 0 when none has arrived.
/// Set only from the handler (async-signal-safe: a single atomic store) and
/// polled from every wait loop below.
static CAUGHT_SIGNAL: AtomicI32 = AtomicI32::new(0);

extern "C" fn record_signal(sig: i32) {
    CAUGHT_SIGNAL.store(sig, Ordering::SeqCst);
}

/// Install handlers for SIGINT and SIGTERM so a signal during startup, the
/// readiness wait, or the child's run is observed rather than killing us
/// before teardown runs.
fn install_signal_handlers() {
    unsafe {
        libc::signal(libc::SIGINT, record_signal as *const () as usize);
        libc::signal(libc::SIGTERM, record_signal as *const () as usize);
    }
}

fn caught_signal() -> Option<i32> {
    match CAUGHT_SIGNAL.load(Ordering::SeqCst) {
        0 => None,
        sig => Some(sig),
    }
}

/// Run `docker <args>`, redacting `redact` out of any error. Used for every
/// non-interactive docker call (run, port, exec, stop, ps): no stdin, output
/// captured.
fn docker(args: &[&str], redact: &[&str]) -> Result<std::process::Output> {
    Command::new("docker")
        .args(args)
        .stdin(Stdio::null())
        .output()
        .map_err(|e| docker_error("docker", args, &e.to_string(), redact))
}

/// Run `docker exec -i <name> <rest>` feeding `stdin` to the child, used for
/// every `psql` invocation (the settings script, `SHOW fsync`, and each
/// `--init-sql` file). Never includes the password: `psql` connects over the
/// container's local unix socket, which the official image trusts
/// unconditionally.
fn docker_exec_stdin(name: &str, psql_args: &[&str], stdin: &str) -> Result<std::process::Output> {
    let mut args = vec!["exec", "-i", name];
    args.extend_from_slice(psql_args);
    let mut child = Command::new("docker")
        .args(&args)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .map_err(|e| docker_error("docker", &args, &e.to_string(), &[]))?;
    child
        .stdin
        .take()
        .expect("piped stdin")
        .write_all(stdin.as_bytes())
        .map_err(|e| docker_error("docker", &args, &e.to_string(), &[]))?;
    child
        .wait_with_output()
        .map_err(|e| docker_error("docker", &args, &e.to_string(), &[]))
}

fn redact_str(mut s: String, redact: &[&str]) -> String {
    for secret in redact {
        if !secret.is_empty() {
            s = s.replace(secret, "<redacted>");
        }
    }
    s
}

fn docker_error(binary: &str, args: &[&str], message: &str, redact: &[&str]) -> Error {
    Error::TestPostgresFailed {
        stage: redact_str(format!("{binary} {}", args.join(" ")), redact),
        message: redact_str(message.to_string(), redact),
    }
}

fn fail(stage: impl Into<String>, message: impl Into<String>) -> Error {
    Error::TestPostgresFailed {
        stage: stage.into(),
        message: message.into(),
    }
}

/// Exact `docker run` arguments for the container. Pure and unit-testable
/// without a docker binary; `password` is accepted as an owned string so a
/// test can pick a non-secret value and assert the exact argument list.
pub fn docker_run_args(name: &str, pid: u32, image: &str, password: &str) -> Vec<String> {
    vec![
        "run".into(),
        "--detach".into(),
        "--rm".into(),
        "--name".into(),
        name.into(),
        "--label".into(),
        format!("{LABEL_KEY}={pid}"),
        "--publish".into(),
        "127.0.0.1::5432".into(),
        "--shm-size".into(),
        "256m".into(),
        "--memory".into(),
        "4g".into(),
        "-e".into(),
        "POSTGRES_USER=postgres".into(),
        "-e".into(),
        "POSTGRES_DB=postgres".into(),
        "-e".into(),
        format!("POSTGRES_PASSWORD={password}"),
        image.into(),
    ]
}

/// `claude-run-test-postgres-<pid>-<nonce>`: unique per invocation, and the
/// pid in the name (also carried separately in the label) is what the orphan
/// sweep keys its liveness check on.
pub fn container_name(pid: u32, nonce: &str) -> String {
    format!("claude-run-test-postgres-{pid}-{nonce}")
}

fn random_hex(bytes: usize) -> Result<String> {
    let mut buf = vec![0u8; bytes];
    getrandom::getrandom(&mut buf)
        .map_err(|e| fail("random", format!("reading random bytes: {e}")))?;
    Ok(hex::encode(buf))
}

/// Parse one `docker ps --filter label=claude-run.test-postgres --format
/// {{.Names}}\t{{.Label "claude-run.test-postgres"}}` line into (name, pid).
/// A line whose label value is not a plain integer is skipped rather than
/// failing the whole sweep: a hand-run container sharing the label prefix by
/// coincidence should not abort cleanup of the ones we understand.
pub fn parse_ps_line(line: &str) -> Option<(String, u32)> {
    let (name, pid) = line.split_once('\t')?;
    let pid: u32 = pid.trim().parse().ok()?;
    Some((name.trim().to_string(), pid))
}

pub fn parse_ps_output(output: &str) -> Vec<(String, u32)> {
    output.lines().filter_map(parse_ps_line).collect()
}

/// Whether `pid` still names a live process. `ESRCH` is "no such process";
/// any other `kill(pid, 0)` outcome (success, or `EPERM` for a pid that
/// exists but we may not signal) means it is still alive.
fn pid_is_alive(pid: u32) -> bool {
    let rc = unsafe { libc::kill(pid as libc::pid_t, 0) };
    if rc == 0 {
        return true;
    }
    std::io::Error::last_os_error().raw_os_error() != Some(libc::ESRCH)
}

/// Stop every labelled container whose owning pid is no longer alive, and
/// return the names stopped (for the "one line per container stopped"
/// requirement; the caller prints them).
pub fn sweep_orphans() -> Result<Vec<String>> {
    let out = docker(
        &[
            "ps",
            "--filter",
            &format!("label={LABEL_KEY}"),
            "--format",
            &format!("{{{{.Names}}}}\t{{{{.Label \"{LABEL_KEY}\"}}}}"),
        ],
        &[],
    )?;
    if !out.status.success() {
        return Err(docker_error(
            "docker",
            &["ps", "--filter", &format!("label={LABEL_KEY}")],
            &String::from_utf8_lossy(&out.stderr),
            &[],
        ));
    }
    let listed = parse_ps_output(&String::from_utf8_lossy(&out.stdout));
    let mut stopped = Vec::new();
    for (name, pid) in listed {
        if !pid_is_alive(pid) {
            stop_container(&name)?;
            stopped.push(name);
        }
    }
    Ok(stopped)
}

fn stop_container(name: &str) -> Result<()> {
    let out = docker(&["stop", name], &[])?;
    if !out.status.success() {
        return Err(docker_error(
            "docker stop",
            &[name],
            &String::from_utf8_lossy(&out.stderr),
            &[],
        ));
    }
    Ok(())
}

/// `docker port <name> 5432/tcp` reads back as `127.0.0.1:NNNNN`; take the
/// port after the last `:`.
fn parse_port(output: &str) -> Option<u16> {
    output.trim().rsplit(':').next()?.parse().ok()
}

fn read_host_port(name: &str) -> Result<u16> {
    let out = docker(&["port", name, "5432/tcp"], &[])?;
    if !out.status.success() {
        return Err(docker_error(
            "docker port",
            &[name, "5432/tcp"],
            &String::from_utf8_lossy(&out.stderr),
            &[],
        ));
    }
    let stdout = String::from_utf8_lossy(&out.stdout);
    parse_port(&stdout).ok_or_else(|| fail("docker port", format!("unparseable output: {stdout}")))
}

/// Wait for a connection to succeed, using `psql` itself rather than
/// `pg_isready`: `pg_isready` can report ready during the brief window before
/// the official postgres image's entrypoint restarts the server, and a report
/// from a different client is no guarantee that `psql`'s own connection (the
/// one every step after this one makes) will succeed against the same unix
/// socket. Connecting with `psql` here means a ready report cannot precede
/// the restart that would otherwise break the next step.
fn wait_pg_ready(name: &str) -> Result<()> {
    let deadline = Instant::now() + PG_READY_TIMEOUT;
    loop {
        if let Some(sig) = caught_signal() {
            return Err(fail("pg ready", format!("interrupted by signal {sig}")));
        }
        let out = docker(
            &[
                "exec",
                name,
                "psql",
                "--username",
                "postgres",
                "--command",
                "SELECT 1",
            ],
            &[],
        )?;
        if out.status.success() {
            return Ok(());
        }
        if Instant::now() >= deadline {
            let logs = docker(&["logs", "--tail", "50", name], &[])
                .map(|o| String::from_utf8_lossy(&o.stdout).into_owned())
                .unwrap_or_default();
            return Err(fail(
                "pg ready",
                format!("postgres did not become ready within {PG_READY_TIMEOUT:?}: {logs}"),
            ));
        }
        std::thread::sleep(POLL_INTERVAL);
    }
}

const DISABLE_DURABILITY_SQL: &str = "\
ALTER SYSTEM SET fsync = off;
ALTER SYSTEM SET synchronous_commit = off;
ALTER SYSTEM SET full_page_writes = off;
SELECT pg_reload_conf();
";

fn psql_args() -> [&'static str; 5] {
    ["psql", "--username", "postgres", "--set", "ON_ERROR_STOP=1"]
}

/// How long a connection may keep failing on a restart shape after
/// `pg_isready` already reported ready: "the database system is starting
/// up" / "shutting down", or the socket file briefly disappearing between
/// that readiness check and the next connection attempt. The official
/// postgres image's entrypoint restarts the server once after its own
/// `initdb`, and `pg_isready` can observe the brief ready window before that
/// restart; retrying here rides out that restart instead of treating it as a
/// real failure.
const RESTART_WINDOW: Duration = Duration::from_secs(20);

/// Whether a `psql` connection failure looks like the official postgres
/// image's entrypoint restart rather than a real failure: the two messages
/// `psql` prints while the server is coming up or down, or a missing unix
/// socket, which is what `psql` reports when the restart has torn the socket
/// down between `wait_pg_ready` succeeding and this connection attempt.
fn is_restart_error(stderr: &str) -> bool {
    stderr.contains("the database system is starting up")
        || stderr.contains("the database system is shutting down")
        || (stderr.contains("No such file or directory")
            && stderr.contains("Is the server running locally"))
}

/// Run `docker exec -i <name> <psql_args>` feeding `stdin`, retrying for up
/// to [`RESTART_WINDOW`] while [`is_restart_error`] holds. Returns whatever
/// output `docker_exec_stdin` last produced, success or failure; the caller
/// turns a still-failing output into its own stage-specific error. The first
/// retry is logged at `warn` so a slow restart is visible without failing
/// the run.
fn exec_with_restart_retry(
    name: &str,
    psql_args: &[&str],
    stdin: &str,
) -> Result<std::process::Output> {
    let deadline = Instant::now() + RESTART_WINDOW;
    let mut warned = false;
    loop {
        let out = docker_exec_stdin(name, psql_args, stdin)?;
        if out.status.success() {
            return Ok(out);
        }
        let stderr = String::from_utf8_lossy(&out.stderr).into_owned();
        if !is_restart_error(&stderr) || Instant::now() >= deadline {
            return Ok(out);
        }
        if !warned {
            warn!(
                container = name,
                stderr = %stderr,
                "postgres looks mid-restart; retrying the connection",
            );
            warned = true;
        }
        std::thread::sleep(POLL_INTERVAL);
    }
}

fn apply_sql(name: &str, stage: &str, sql: &str) -> Result<()> {
    let mut args = vec![name];
    args.extend_from_slice(&psql_args());
    args.push("--file");
    args.push("-");

    let out = exec_with_restart_retry(name, &args[1..], sql)?;
    if out.status.success() {
        return Ok(());
    }
    Err(docker_error(
        stage,
        &args,
        &String::from_utf8_lossy(&out.stderr),
        &[],
    ))
}

fn disable_durability(name: &str) -> Result<()> {
    apply_sql(name, "disable durability", DISABLE_DURABILITY_SQL)?;

    let args = vec![
        name,
        "psql",
        "--username",
        "postgres",
        "--no-align",
        "--tuples-only",
        "--command",
        "SHOW fsync",
    ];
    let out = exec_with_restart_retry(name, &args[1..], "")?;
    if !out.status.success() {
        return Err(docker_error(
            "SHOW fsync",
            &args,
            &String::from_utf8_lossy(&out.stderr),
            &[],
        ));
    }
    let reading = String::from_utf8_lossy(&out.stdout).trim().to_string();
    if reading != "off" {
        return Err(fail(
            "SHOW fsync",
            format!("expected `off`, read back `{reading}`"),
        ));
    }
    Ok(())
}

fn apply_init_files(name: &str, files: &[PathBuf]) -> Result<()> {
    for file in files {
        let sql = std::fs::read_to_string(file)
            .map_err(|e| fail("init-sql", format!("reading {}: {e}", file.display())))?;
        apply_sql(name, &format!("init-sql {}", file.display()), &sql)?;
    }
    Ok(())
}

/// Spawn `command` with the Postgres env vars set, inheriting stdio. Returns
/// the spawned [`Child`] so the caller can poll it alongside the signal flag.
fn spawn_command(command: &[String], url: &str, container_name: &str) -> Result<Child> {
    let (program, rest) = command
        .split_first()
        .ok_or_else(|| fail("spawn", "no command given"))?;
    Command::new(program)
        .args(rest)
        .env("DATABASE_URL", url)
        .env("CLAUDE_RUN_TEST_DATABASE_URL", url)
        .env("CLAUDE_RUN_TEST_POSTGRES_CONTAINER", container_name)
        .stdin(Stdio::inherit())
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit())
        .spawn()
        .map_err(|e| fail("spawn", format!("{program}: {e}")))
}

/// Poll `child` to completion, returning its exit code (128 + signal if it was
/// killed by one) unless our own process catches SIGINT/SIGTERM first, in
/// which case the child is terminated and `Err` carries the signal so the
/// caller can still run teardown before propagating it.
fn wait_for_child(mut child: Child) -> Result<i32> {
    loop {
        if let Some(status) = child.try_wait().map_err(Error::Io)? {
            if let Some(sig) = status.signal() {
                return Ok(128 + sig);
            }
            return Ok(status.code().unwrap_or(1));
        }
        if let Some(sig) = caught_signal() {
            unsafe {
                libc::kill(child.id() as libc::pid_t, sig);
            }
            let _ = child.wait();
            return Err(fail("interrupted", format!("caught signal {sig}")));
        }
        std::thread::sleep(POLL_INTERVAL);
    }
}

/// Run `test-postgres`: sweep orphans, start a container, disable durability,
/// apply `--init-sql` files, run `command`, tear the container down, and
/// return the exit code to propagate. Teardown always runs once the container
/// has started, even when an earlier step, the command, or a signal is what
/// ends the run; a teardown failure is folded into the returned error rather
/// than swallowed, and wins over a successful command's own exit code.
pub fn run(opts: &Opts, command: &[String]) -> Result<i32> {
    install_signal_handlers();

    for name in sweep_orphans()? {
        println!("test-postgres: stopped orphaned container {name}");
    }

    if command.is_empty() {
        return Err(fail("test-postgres", "no command given after `--`"));
    }

    let pid = std::process::id();
    let nonce = random_hex(4)?;
    let name = container_name(pid, &nonce);
    let password = random_hex(16)?;
    let image = opts.image().to_string();

    let run_args = docker_run_args(&name, pid, &image, &password);
    let run_args_ref: Vec<&str> = run_args.iter().map(String::as_str).collect();
    let out = docker(&run_args_ref, &[&password])?;
    if !out.status.success() {
        return Err(docker_error(
            "docker run",
            &run_args_ref,
            &String::from_utf8_lossy(&out.stderr),
            &[&password],
        ));
    }

    // From here on the container exists and MUST be torn down, whatever
    // happens next; `result` captures the outcome and teardown always runs
    // before we return it.
    let result = (|| -> Result<i32> {
        let port = read_host_port(&name)?;
        wait_pg_ready(&name)?;
        disable_durability(&name)?;
        apply_init_files(&name, &opts.init_sql)?;

        let url = format!("postgres://postgres:{password}@127.0.0.1:{port}/postgres");
        let child = spawn_command(command, &url, &name)?;
        wait_for_child(child)
    })();

    match stop_container(&name) {
        Ok(()) => result,
        Err(teardown_err) => {
            eprintln!("test-postgres: teardown of {name} failed: {teardown_err}");
            match result {
                Ok(_) => Err(teardown_err),
                Err(run_err) => Err(run_err),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testutil::write_stub;
    use std::path::Path;

    #[test]
    fn docker_run_args_match_the_spec() {
        let args = docker_run_args(
            "claude-run-test-postgres-123-abcd",
            123,
            DEFAULT_IMAGE,
            "sekret",
        );
        assert_eq!(
            args,
            vec![
                "run",
                "--detach",
                "--rm",
                "--name",
                "claude-run-test-postgres-123-abcd",
                "--label",
                "claude-run.test-postgres=123",
                "--publish",
                "127.0.0.1::5432",
                "--shm-size",
                "256m",
                "--memory",
                "4g",
                "-e",
                "POSTGRES_USER=postgres",
                "-e",
                "POSTGRES_DB=postgres",
                "-e",
                "POSTGRES_PASSWORD=sekret",
                DEFAULT_IMAGE,
            ]
        );
        assert!(!args
            .iter()
            .any(|a| a.contains("tmpfs") || a.contains("--volume")));
    }

    #[test]
    fn docker_run_args_honor_image_override() {
        let args = docker_run_args("n", 1, "postgres:18.2-alpine3.23", "x");
        assert_eq!(args.last().unwrap(), "postgres:18.2-alpine3.23");
    }

    #[test]
    fn container_name_is_pid_and_nonce_qualified() {
        assert_eq!(
            container_name(42, "ab12"),
            "claude-run-test-postgres-42-ab12"
        );
    }

    #[test]
    fn parse_ps_output_reads_name_and_pid_pairs() {
        let out = "claude-run-test-postgres-10-aaaa\t10\nclaude-run-test-postgres-20-bbbb\t20\n";
        assert_eq!(
            parse_ps_output(out),
            vec![
                ("claude-run-test-postgres-10-aaaa".to_string(), 10),
                ("claude-run-test-postgres-20-bbbb".to_string(), 20),
            ]
        );
    }

    #[test]
    fn parse_ps_output_skips_a_non_numeric_label() {
        let out = "some-other-container\tnot-a-pid\nclaude-run-test-postgres-20-bbbb\t20\n";
        assert_eq!(
            parse_ps_output(out),
            vec![("claude-run-test-postgres-20-bbbb".to_string(), 20)]
        );
    }

    #[test]
    fn parse_port_reads_the_loopback_publish() {
        assert_eq!(parse_port("127.0.0.1:34567\n"), Some(34567));
        assert_eq!(parse_port(""), None);
        assert_eq!(parse_port("garbage"), None);
    }

    #[test]
    fn pid_is_alive_is_true_for_ourselves_and_false_for_a_reaped_pid() {
        assert!(pid_is_alive(std::process::id()));
        // pid 1 belongs to init in every pid namespace this runs in; a freshly
        // forked-and-waited child's pid is guaranteed dead immediately after.
        let mut child = Command::new("true").spawn().unwrap();
        let pid = child.id();
        child.wait().unwrap();
        assert!(!pid_is_alive(pid));
    }

    /// End-to-end against a stub `docker` on PATH (CLAUDE-224: written with
    /// `testutil::write_stub`, never `fs::write` + `set_permissions`). The stub
    /// logs every invocation's argv (space-joined) to `log_path` and answers
    /// just enough to drive `run()` through start, readiness, durability,
    /// one init-sql file, a successful child, and teardown.
    fn stub_docker(dir: &Path, log_path: &Path) {
        let script = format!(
            r#"#!/bin/sh
echo "$@" >> "{log}"
case "$1" in
  run) echo "stubcontainer" ;;
  port) echo "127.0.0.1:55432" ;;
  exec)
    shift
    if [ "$1" = "-i" ]; then shift; fi
    shift # container name
    case "$*" in
      *"SHOW fsync"*) echo "off" ;;
      *) cat > /dev/null ;;
    esac
    ;;
  stop) : ;;
  ps) : ;;
  logs) : ;;
  *) : ;;
esac
exit 0
"#,
            log = log_path.display()
        );
        write_stub(&dir.join("docker"), &script);
    }

    /// `PATH` is process-global, so every test that stubs a binary on it holds
    /// the crate-wide `testutil::path_env_lock` for its whole body.
    fn lock_path() -> std::sync::MutexGuard<'static, ()> {
        crate::testutil::path_env_lock()
    }

    fn with_stub_on_path<F: FnOnce()>(f: F) {
        let _guard = lock_path();
        let dir = tempfile::tempdir().unwrap();
        let log_path = dir.path().join("calls.log");
        stub_docker(dir.path(), &log_path);
        let old_path = std::env::var("PATH").unwrap_or_default();
        unsafe {
            std::env::set_var("PATH", format!("{}:{old_path}", dir.path().display()));
        }
        f();
        unsafe {
            std::env::set_var("PATH", old_path);
        }
        let _ = std::fs::read_to_string(&log_path);
    }

    #[test]
    fn run_propagates_the_child_exit_code() {
        with_stub_on_path(|| {
            let opts = Opts::default();
            let code = run(&opts, &["sh".into(), "-c".into(), "exit 7".into()]).unwrap();
            assert_eq!(code, 7);
        });
    }

    #[test]
    fn run_sets_the_env_vars_the_child_reads() {
        with_stub_on_path(|| {
            let opts = Opts::default();
            let marker = tempfile::NamedTempFile::new().unwrap();
            let marker_path = marker.path().to_path_buf();
            let code = run(
                &opts,
                &[
                    "sh".into(),
                    "-c".into(),
                    format!(
                        "printf '%s|%s|%s' \"$DATABASE_URL\" \"$CLAUDE_RUN_TEST_DATABASE_URL\" \"$CLAUDE_RUN_TEST_POSTGRES_CONTAINER\" > {}",
                        marker_path.display()
                    ),
                ],
            )
            .unwrap();
            assert_eq!(code, 0);
            let contents = std::fs::read_to_string(&marker_path).unwrap();
            let parts: Vec<&str> = contents.split('|').collect();
            assert_eq!(parts[0], parts[1]);
            assert!(parts[0].starts_with("postgres://postgres:"));
            assert!(parts[2].starts_with("claude-run-test-postgres-"));
        });
    }

    #[test]
    fn run_applies_init_sql_files_in_order() {
        with_stub_on_path(|| {
            let dir = tempfile::tempdir().unwrap();
            let log_path = dir.path().join("exec-order.log");
            // A second stub layer: this one records init-sql file bodies via a
            // marker the generic stub above does not produce, so build it by
            // hand instead of reusing stub_docker.
            let script = format!(
                r#"#!/bin/sh
case "$1" in
  run) echo "stubcontainer" ;;
  port) echo "127.0.0.1:55432" ;;
  exec)
    body=$(cat)
    echo "$body" >> "{log}"
    case "$body" in
      *"SHOW fsync"*) : ;;
    esac
    case "$*" in
      *"SHOW fsync"*) echo "off" ;;
    esac
    ;;
  *) : ;;
esac
exit 0
"#,
                log = log_path.display()
            );
            write_stub(&dir.path().join("docker"), &script);
            let old_path = std::env::var("PATH").unwrap_or_default();
            unsafe {
                std::env::set_var("PATH", format!("{}:{old_path}", dir.path().display()));
            }

            let a = dir.path().join("a.sql");
            let b = dir.path().join("b.sql");
            std::fs::write(&a, "-- A\n").unwrap();
            std::fs::write(&b, "-- B\n").unwrap();

            let opts = Opts {
                image: None,
                init_sql: vec![a, b],
            };
            let code = run(&opts, &["true".into()]).unwrap();
            assert_eq!(code, 0);

            unsafe {
                std::env::set_var("PATH", old_path);
            }

            let log = std::fs::read_to_string(&log_path).unwrap();
            let a_pos = log.find("-- A").unwrap();
            let b_pos = log.find("-- B").unwrap();
            assert!(a_pos < b_pos, "init-sql files were not applied in order");
        });
    }

    #[test]
    fn run_fails_with_no_command() {
        with_stub_on_path(|| {
            let opts = Opts::default();
            let err = run(&opts, &[]).unwrap_err();
            assert!(err.to_string().contains("no command"));
        });
    }

    #[test]
    fn run_tears_down_after_a_failing_command() {
        with_stub_on_path(|| {
            let dir = tempfile::tempdir().unwrap();
            let stop_log = dir.path().join("stop.log");
            let script = format!(
                r#"#!/bin/sh
case "$1" in
  run) echo "stubcontainer" ;;
  port) echo "127.0.0.1:55432" ;;
  exec) cat > /dev/null; case "$*" in *"SHOW fsync"*) echo off ;; esac ;;
  stop) echo "$2" >> "{log}" ;;
  *) : ;;
esac
exit 0
"#,
                log = stop_log.display()
            );
            write_stub(&dir.path().join("docker"), &script);
            let old_path = std::env::var("PATH").unwrap_or_default();
            unsafe {
                std::env::set_var("PATH", format!("{}:{old_path}", dir.path().display()));
            }

            let opts = Opts::default();
            let code = run(&opts, &["sh".into(), "-c".into(), "exit 3".into()]).unwrap();
            assert_eq!(code, 3);

            unsafe {
                std::env::set_var("PATH", old_path);
            }
            let stopped = std::fs::read_to_string(&stop_log).unwrap();
            assert!(stopped.trim().starts_with("claude-run-test-postgres-"));
        });
    }

    #[test]
    fn run_tears_down_after_a_failed_startup_step() {
        with_stub_on_path(|| {
            let dir = tempfile::tempdir().unwrap();
            let stop_log = dir.path().join("stop.log");
            // `exec` (pg_isready) always fails, so wait_pg_ready would spin for
            // 60s; shrink that by making the port step itself fail instead.
            let script = format!(
                r#"#!/bin/sh
case "$1" in
  run) echo "stubcontainer" ;;
  port) exit 1 ;;
  stop) echo "$2" >> "{log}" ;;
  *) : ;;
esac
exit 0
"#,
                log = stop_log.display()
            );
            write_stub(&dir.path().join("docker"), &script);
            let old_path = std::env::var("PATH").unwrap_or_default();
            unsafe {
                std::env::set_var("PATH", format!("{}:{old_path}", dir.path().display()));
            }

            let opts = Opts::default();
            let err = run(&opts, &["true".into()]).unwrap_err();
            assert!(err.to_string().contains("docker port"));

            unsafe {
                std::env::set_var("PATH", old_path);
            }
            let stopped = std::fs::read_to_string(&stop_log).unwrap();
            assert!(stopped.trim().starts_with("claude-run-test-postgres-"));
        });
    }

    #[test]
    fn sweep_orphans_stops_only_dead_pid_containers() {
        with_stub_on_path(|| {
            let dir = tempfile::tempdir().unwrap();
            let stop_log = dir.path().join("stop.log");
            // A dead pid: fork-and-wait a child exactly as in
            // `pid_is_alive_is_true_for_ourselves_and_false_for_a_reaped_pid`.
            let mut child = Command::new("true").spawn().unwrap();
            let dead_pid = child.id();
            child.wait().unwrap();
            let alive_pid = std::process::id();

            let script = format!(
                r#"#!/bin/sh
case "$1" in
  ps) printf 'live-container\t{alive}\ndead-container\t{dead}\n' ;;
  stop) echo "$2" >> "{log}" ;;
  *) : ;;
esac
exit 0
"#,
                alive = alive_pid,
                dead = dead_pid,
                log = stop_log.display()
            );
            write_stub(&dir.path().join("docker"), &script);
            let old_path = std::env::var("PATH").unwrap_or_default();
            unsafe {
                std::env::set_var("PATH", format!("{}:{old_path}", dir.path().display()));
            }

            let stopped = sweep_orphans().unwrap();

            unsafe {
                std::env::set_var("PATH", old_path);
            }
            assert_eq!(stopped, vec!["dead-container".to_string()]);
        });
    }

    #[test]
    fn is_restart_error_matches_the_starting_up_and_shutting_down_messages() {
        assert!(is_restart_error(
            "FATAL:  the database system is starting up"
        ));
        assert!(is_restart_error(
            "FATAL:  the database system is shutting down"
        ));
    }

    #[test]
    fn is_restart_error_matches_the_missing_socket_message() {
        let stderr = "psql: error: connection to server on socket \"/var/run/postgresql/.s.PGSQL.5432\" failed: No such file or directory\n\tIs the server running locally and accepting connections on that socket?\n";
        assert!(is_restart_error(stderr));
    }

    #[test]
    fn is_restart_error_rejects_an_unrelated_failure() {
        assert!(!is_restart_error(
            "psql: error: password authentication failed for user \"postgres\""
        ));
        assert!(!is_restart_error("No such file or directory"));
    }

    #[test]
    fn disable_durability_retries_the_show_fsync_read_back_through_a_restart() {
        with_stub_on_path(|| {
            let dir = tempfile::tempdir().unwrap();
            let state_path = dir.path().join("show-fsync-attempts");
            std::fs::write(&state_path, "0").unwrap();
            let script = format!(
                r#"#!/bin/sh
case "$1" in
  run) echo "stubcontainer" ;;
  port) echo "127.0.0.1:55432" ;;
  exec)
    shift
    if [ "$1" = "-i" ]; then shift; fi
    shift # container name
    case "$*" in
      *"SELECT 1"*) exit 0 ;;
      *"SHOW fsync"*)
        n=$(cat "{state}")
        n=$((n + 1))
        echo "$n" > "{state}"
        if [ "$n" -lt 2 ]; then
          echo 'psql: error: connection to server on socket "/var/run/postgresql/.s.PGSQL.5432" failed: No such file or directory' >&2
          echo '	Is the server running locally and accepting connections on that socket?' >&2
          exit 2
        fi
        echo "off"
        ;;
      *) cat > /dev/null ;;
    esac
    ;;
  stop) : ;;
  ps) : ;;
  logs) : ;;
  *) : ;;
esac
exit 0
"#,
                state = state_path.display()
            );
            write_stub(&dir.path().join("docker"), &script);
            let old_path = std::env::var("PATH").unwrap_or_default();
            unsafe {
                std::env::set_var("PATH", format!("{}:{old_path}", dir.path().display()));
            }

            let result = disable_durability("stubcontainer");

            unsafe {
                std::env::set_var("PATH", old_path);
            }
            result.unwrap();
            assert_eq!(std::fs::read_to_string(&state_path).unwrap().trim(), "2");
        });
    }

    #[test]
    fn password_never_appears_in_a_docker_run_failure() {
        with_stub_on_path(|| {
            let dir = tempfile::tempdir().unwrap();
            let script = r#"#!/bin/sh
case "$1" in
  ps) exit 0 ;;
  run) echo "boom: $*" >&2; exit 1 ;;
  *) exit 0 ;;
esac
"#;
            write_stub(&dir.path().join("docker"), script);
            let old_path = std::env::var("PATH").unwrap_or_default();
            unsafe {
                std::env::set_var("PATH", format!("{}:{old_path}", dir.path().display()));
            }

            let opts = Opts::default();
            let err = run(&opts, &["true".into()]).unwrap_err();

            unsafe {
                std::env::set_var("PATH", old_path);
            }
            let rendered = err.to_string();
            // The password is 32 hex chars (16 random bytes); assert the
            // rendered error contains no run of 32 hex characters at all,
            // which is stronger than checking for one specific value.
            let hex_run = rendered
                .as_bytes()
                .windows(32)
                .any(|w| w.iter().all(|b| b.is_ascii_hexdigit()));
            assert!(
                !hex_run,
                "docker run failure leaked the password: {rendered}"
            );
            assert!(rendered.contains("<redacted>"));
        });
    }
}
