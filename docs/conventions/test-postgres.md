# Throwaway test Postgres (CLAUDE-404)

`claude-run test-postgres [--image <ref>] [--init-sql <file>]... -- <command> [args...]` is how an agent session (or anyone else) runs a Postgres-backed test command: it starts a private, non-durable Postgres container in the Docker-in-Docker sidecar every agent session already has, runs `<command>` against it, and tears the container down however the command ends. CLAUDE-403 stopped the `claude` child from reaching the runner's own live database, which otherwise left Postgres-backed suites (claude-run's own `CLAUDE_RUN_TEST_DATABASE_URL` pass, and other repos' suites that read `DATABASE_URL`) unverified until CI; this subcommand is the replacement.

## What it does

1. Sweeps orphaned containers first: any earlier `claude-run-test-postgres-*` container labelled `claude-run.test-postgres=<pid>` whose owning pid is no longer alive is stopped, since containers started by hand are not reliably removed.
2. Starts `docker run --detach --rm` with a loopback-only ephemeral publish (`--publish 127.0.0.1::5432`, the port read back with `docker port`), `--shm-size 256m`, `--memory 4g`, and a random per-run password. The default image is `docker.io/postgres:18.4-alpine3.23`; `--image` overrides it so a repo whose CI pins a different tag (app-server uses `postgres:18.2-alpine3.23`) can match it. No tmpfs, no volume: the container's own writable layer is discarded by `--rm`, and the data directory is never put on the host's RAM-backed tmpfs (a large suite, about 1,300 cloned databases in app-server, can fill it).
3. Waits for `pg_isready` (up to 60s), then disables durability (`fsync`, `synchronous_commit`, `full_page_writes` all `off`, reloaded with `pg_reload_conf()`, verified by reading `SHOW fsync` back) since a throwaway database does not need to survive a crash: app-server measured this cutting a 229-migration run from 1.12-1.32s to 0.60-0.71s. Each `--init-sql` file is then applied in order, with `ON_ERROR_STOP`.
4. Runs `<command>` with `DATABASE_URL` and `CLAUDE_RUN_TEST_DATABASE_URL` set to the throwaway Postgres and `CLAUDE_RUN_TEST_POSTGRES_CONTAINER` set to the container name, inheriting stdio, and propagates its exit code (128 + signal if it was killed by one).
5. Stops the container on normal exit, on command failure, on SIGINT/SIGTERM, and on any error in steps 2-3. A teardown failure is reported and makes the exit code non-zero even when the command itself succeeded.

The password never appears in a log line or an error message.

## Using it

- `just test-postgres` runs `cargo run -- test-postgres -- cargo test`, the recipe for this repo's own Postgres-backed suite.
- In another repo, the same binary and the same pattern apply, e.g. `claude-run test-postgres --init-sql scripts/test-db-roles.sql -- cargo test` for a suite that needs extra roles set up first.
- `deploy/bot-CLAUDE.md` tells the agent to use this subcommand and never point `DATABASE_URL` / `CLAUDE_RUN_TEST_DATABASE_URL` at any other database.

## Why not a shared service

A Postgres service in `compose.yml` was considered and rejected: it would be one long-lived server shared by every session, the test harness never drops its schemas, and parallel runs would share it. Telling the agent to `docker run postgres` itself was also rejected: the cleanup, the settings, and the image choice would then be reinvented (and inconsistently cleaned up) by every run.
