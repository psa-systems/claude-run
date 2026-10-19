# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

# claude-run

YAML-driven runner for Claude Code: poll an external source (YouTrack issues, Forgejo PRs, ...), feed each job to `claude --print`, do something with the result. Shipped only as a Docker image around a single Rust binary (`claude-run`) that doubles as a CLI and a long-running daemon with a unix-socket API.

## Shape

- Rust binary, library + bin. `clap` derive. Subcommand tree: `claude-run {run,serve,stop,restart,trigger,cancel,pause,resume,status,action,access,secret,mcp,migrate,import,test-postgres}`.
- Actions are YAML files under `$CLAUDE_RUN_CONFIG_DIR/actions/` (`/config/actions/` in the container). Each action's `input.command` / `filter` / `pre[]` / `prompt` / `post[]` is a nu snippet shelled out via the host's `nu` binary.
- Runtime state in SQLite at `$CLAUDE_RUN_DATA_DIR/state.db` by default, or in PostgreSQL when the operator sets one (CLAUDE-250; see the `[database]` note below). Per-attempt stream logs at `$CLAUDE_RUN_STATE_DIR/logs/<action>/streams/<key>-attempt-<n>.jsonl`, bounded by the CLAUDE-245 sweep (see below).
- Daemon (`claude-run serve`) owns scheduling and exposes an HTTP API over `$CLAUDE_RUN_RUNTIME_DIR/daemon.sock`. CLI subcommands delegate to the daemon when reachable, else operate directly on the DB. The socket is the CLI's control plane and nothing else:
  `api::router` is built twice from one `api_state`, bare-path on the socket and nested under `/api` on the TCP listener, which is what the web UI's own buttons POST to.
  `serve --no-socket` skips the bind for a deployment with no CLI operator, and the container image runs that way (`CMD ["serve", "--no-socket"]`) because the socket has no auth gate (the CLAUDE-48 token wraps only the TCP router) and would otherwise sit in the same container as the agent, running as the same user.
  `trigger` and `cancel` then fail naming that reason instead of "no daemon"; the DB-direct subcommands are unaffected.
- Runtime tool dependencies ship in the runtime image, not in the binary: `nu`, `claude` (Claude Code CLI), `timeout` (coreutils), `yt`, `fj`, `gh`, `git`. Docker is the only supported install.
- Forgejo home: `psa-systems/claude-run`.

## Commands

- `just check`: fmt + clippy + build + tests + builder-stage docker compile (the full CI gate). It starts with the CLAUDE.md guard, `just check-claude-md` (CLAUDE-370). `just pre-commit` runs only a fmt-only check inside the builder image via `docker run` (common's default `hook_layout := "pre-push"`); `just pre-push` runs the full suite (fmt, clippy, compile, tests) there.
- CI's exact steps (`.forgejo/workflows/check.yml`): `cargo fmt --all --check`, `cargo clippy --all-targets -- -D warnings`, `cargo build --all-targets`, then `cargo test` twice: once on SQLite, once with `CLAUDE_RUN_TEST_DATABASE_URL=postgres://...` set, which switches every test fixture onto Postgres (one schema per test). A DB change must pass both.
- Single test: `cargo test <name_substring>` (e.g. `cargo test shipped_action_queries_are_disjoint`); one module: `cargo test native::youtrack::`. Unit tests live inline in `src/**` under `mod tests` on the library target; `tests/fixtures/` holds data only.
- `just run <args>` = `cargo run -- <args>` (debug build).
- Any test that execs a stub script must create it with `testutil::write_stub`, never `fs::write` + `set_permissions` (ETXTBSY race across test threads, CLAUDE-224 / CLAUDE-255).

## Code map

- Entry: `src/bin/claude-run.rs` (clap) -> `client.rs` (talk to the daemon socket) or direct DB ops. `daemon.rs` hosts `serve`: `scheduler.rs` ticks, `Dispatcher::execute` runs an action via `run_action_inner`.
- Two action kinds (`ActionKind` in `action.rs`): `scripted` runs the generic nu pipeline in `job.rs` (`snippet.rs` execs `nu`, `claude.rs` execs `claude`); native kinds route through `native::handler_for` to a Rust handler in `src/native/` (`youtrack`, `closed_issue_summary`) that owns its whole run.
  Adding a native kind means a new `ActionKind` variant, a handler, a `handler_for` arm, and usually a structured editor in `web/native_forms.rs` + `templates/*_editor.html`.
- Native handlers reach YouTrack / Forgejo / GitHub only through the `provider` traits (`IssueTracker`, `Forge`); `provider/cli.rs` shells out to `yt` / `fj`, `provider/github.rs` covers GitHub, and `provider/audit.rs` wraps the tracker to cap and attribute a run's issue writes (CLAUDE-234). No raw REST in handlers.
- Web: `web/mod.rs` (axum + askama templates from `templates/`, compiled into the binary) mounts `api::router` under `/api` plus the two webhook listeners. `settings.rs` owns DB-backed runtime config; `config.rs` holds the serde schema and bootstrap file.
- DB: `db.rs` holds the pool and two embedded migrators, `migrations/sqlite/` and `migrations/postgres/`. Every schema change needs a matching migration in BOTH directories. Queries are runtime `sqlx::query` (no compile-time macros, no offline `.sqlx` data).
- Shipped actions in `actions/*.yml` are `include_str!`'d by tests in `action.rs`, so editing one can break a unit test.

## Conventions

Each line names one rule and links its full text. A new convention adds one index line here (at most 200 characters) and its full text in `docs/conventions/`; `scripts/check-claude-md.nu` enforces the size and link limits.

### Governance and build

- Governance lives at `niceguyit/governance`; only claude-run deviations are recorded here ([details](docs/conventions/governance.md#governance-source-of-truth))
- Builder: rust-builder-musl. Runtime image: FROM opensuse-dev, slim bot CLAUDE.md ([CLAUDE-37, CLAUDE-287, CLAUDE-94](docs/conventions/governance.md#runtime-images-claude-37-claude-287-claude-94))
- Governance `.env.example`, HOST_IP/PORT split and fixed ports do not apply; UI binds `127.0.0.1:7321` ([CLAUDE-37](docs/conventions/governance.md#template-env-and-ports-do-not-apply-claude-37))
- `actions/SCHEMA.md` is the authoritative action schema; a change updates runner and doc together ([details](docs/conventions/governance.md#action-schema))

### Web UI authentication

- An exposed instance with no credential serves only `/setup`, which writes the argon2id admin ([details](docs/conventions/web-auth.md#setup-bootstrap))
- TOTP is mandatory for every `web_admin`/`web_accounts` row: recovery codes, replay guard, pending-2FA cookie ([CLAUDE-368](docs/conventions/web-auth.md#mandatory-totp-claude-368))
- An invite's email goes through `src/mailer.rs`; its link is built from `public_url`, never the `Host` header ([CLAUDE-394](docs/conventions/web-auth.md#invite-by-email-claude-394))

### Credentials and logins

- Nothing runs without a Claude credential; `ReadinessGate` also trips on an expired login ([CLAUDE-269](docs/conventions/credentials.md#readiness-gate-claude-269))
- `claude auth login` is brokered from the Settings Auth card (`src/claude_login.rs`) ([details](docs/conventions/credentials.md#brokered-claude-login))
- Daemon holds its own yt/fj/gh/API-key logins; runner owns `secrets.key` in the config dir ([CLAUDE-307](docs/conventions/credentials.md#service-logins-and-secrets-key-claude-307))

### Forge and git

- Every `git` subprocess authenticates from the forge key store; failures carry redacted stderr ([CLAUDE-252](docs/conventions/forge.md#git-authentication-and-errors-claude-252))
- Read-only `fj` and `gh` calls retry up to 4 attempts on non-zero exit; writes never retry ([CLAUDE-366, CLAUDE-407](docs/conventions/forge.md#read-only-fj-retries-claude-366))
- Every git network call, including push, retries up to 4 attempts; the AGit `fj pr create -aA` path stays excluded ([CLAUDE-396](docs/conventions/forge.md#git-network-retries-claude-396))

### Container runtime

- Container layout is flat: `/config`, `/data`, `/state`, tmpfs `/run/claude-run` ([CLAUDE-253](docs/conventions/container.md#flat-container-layout-claude-253))
- No host bind mounts: logins live inside the container; catatonit is PID 1 ([CLAUDE-247, CLAUDE-287, CLAUDE-310](docs/conventions/container.md#container-identity-claude-247-claude-287-claude-310))
- `test-postgres` runs a throwaway, non-durable Postgres in the Docker sidecar for agent test runs ([CLAUDE-404](docs/conventions/test-postgres.md#throwaway-test-postgres-claude-404))
- A sidecar `buildx` session can die to an upstream dockerd bug; no setting here fixes it ([CLAUDE-417](docs/conventions/container.md#buildkit-session-healthcheck-claude-417))

### Actions

- `closed-issue-summary` is a reporting action; shipped action queries stay disjoint ([CLAUDE-218, CLAUDE-215](docs/conventions/actions.md#reporting-actions-claude-218-claude-215))
- Every YouTrack write goes through run-scoped `AuditedTracker`, which caps and attributes it ([CLAUDE-234](docs/conventions/actions.md#audited-youtrack-writes-claude-234))
- `claude-run mcp stdio` is a read-only MCP server; stdout carries only MCP frames, no tool ever writes ([CLAUDE-437](docs/conventions/mcp.md))

### State store and config

- SQLite or Postgres via one `AnyPool` and `db::q`; a schema change adds a migration in BOTH dirs ([CLAUDE-250](docs/conventions/state.md#state-store-backend-claude-250))
- Switching backends carries the data over with `claude-run import` ([CLAUDE-251](docs/conventions/state.md#backend-import-claude-251))
- `config.toml` holds only bootstrap keys; runtime settings are DB-backed and edited in the UI ([CLAUDE-101](docs/conventions/state.md#bootstrap-vs-runtime-config-claude-101))
- Global pause is one DB flag honored at EVERY entry point ([CLAUDE-223](docs/conventions/state.md#global-pause-claude-223))

### CI failures and webhooks

- The CI-failure store: `ci_failures`, its tests and runs, and `agent_pull_requests` ([CLAUDE-272](docs/conventions/ci-failures.md#ci-failure-store-claude-272))
- A re-queued issue continues its own open PR branch instead of opening a second PR ([CLAUDE-275](docs/conventions/ci-failures.md#re-queued-issues-continue-their-pr-claude-275))
- CI-failure dedupe uses failing-test overlap in the store, never a `yt` search ([CLAUDE-273](docs/conventions/ci-failures.md#failing-test-dedupe-claude-273))
- The CI webhook runs one `claude` analysis pass over the full redacted log ([CLAUDE-274](docs/conventions/ci-failures.md#single-analysis-pass-claude-274))
- A CI failure on an open PR routes by who opened the PR ([CLAUDE-276](docs/conventions/ci-failures.md#route-by-pr-opener-claude-276))
- Two separate webhook listeners, each with its own DB-backed daily run cap ([CLAUDE-323, CLAUDE-364](docs/conventions/ci-failures.md#webhook-listeners-and-run-caps-claude-323-claude-364))

### Merge and automerge

- `pull_request` webhook deliveries record a merged or closed agent PR ([CLAUDE-277](docs/conventions/merge.md#pr-merge-deliveries-claude-277))
- `finalize` never waits for CI; automerge is driven by Action Run webhook events ([CLAUDE-278](docs/conventions/merge.md#event-driven-automerge-claude-278))
- A reconciler backstops webhook automerge so no green PR sits unwatched ([CLAUDE-328](docs/conventions/merge.md#automerge-reconciler-claude-328))
- `PrStatus.behind_base` separates a behind-base PR from a real conflict ([CLAUDE-309, FJ-99](docs/conventions/merge.md#behind-base-prs-claude-309-fj-99))
- The runner writes no `State` on merge; a YouTrack workflow resolves the issue ([CLAUDE-327](docs/conventions/merge.md#state-on-merge-is-a-youtrack-workflow-claude-327))

### Claude invocation and runtime

- Every sync subprocess or sleep reached from async code runs through `blocking::in_place` ([CLAUDE-228](docs/conventions/claude-runtime.md#no-blocking-on-tokio-workers-claude-228))
- Every run resolves a spend ceiling first, bills one ledger, and exits `OVER_BUDGET` (6) ([DEV-705, DEV-707](docs/conventions/claude-runtime.md#per-run-spend-budget-dev-705-dev-707))
- Every `claude` invocation records its plan rate-limit reading and `api_retry` counts ([CLAUDE-315](docs/conventions/claude-runtime.md#plan-rate-limit-readings-claude-315))
- `claude.autocompact` defaults to 200000 so spend stays linear in tool calls ([DEV-706](docs/conventions/claude-runtime.md#bounded-context-dev-706))
- The stream-log relay reports its own I/O failures instead of truncating silently ([CLAUDE-233](docs/conventions/claude-runtime.md#stream-log-write-failures-claude-233))
- Stream-log readers return `io::Result`; an unreadable log finalizes as transient ([CLAUDE-256](docs/conventions/claude-runtime.md#stream-log-read-failures-claude-256))
- No `claude` child can leave work running past its own turn ([CLAUDE-280](docs/conventions/claude-runtime.md#no-background-work-claude-280))

### Log and state retention

- `logs/` stream logs are bounded by their own age sweep ([CLAUDE-245](docs/conventions/retention.md#stream-log-sweep-claude-245))
- `daemon.log`, `output.file` and `usage.jsonl` are bounded by SIZE on the same tick ([CLAUDE-257, CLAUDE-268](docs/conventions/retention.md#size-bounded-log-files-claude-257-claude-268))
- `snippets/` and `webhook-logs/` are bounded by AGE on the same tick ([CLAUDE-262](docs/conventions/retention.md#snippet-and-webhook-log-sweeps-claude-262))

## Phases

The Rust port is complete: all six phases below are merged. This table is the historical record of that rollout, kept because code comments across the tree still cite these phase numbers.

| Phase | Scope                                                                | Status                          |
|-------|----------------------------------------------------------------------|---------------------------------|
| 1     | `run` / `migrate`, all core modules, SQLite-backed state             | merged                          |
| 2     | `serve [--detach]`, scheduler, unix-socket API, lock, pause-in-DB    | merged                          |
| 3     | Web UI (axum templates + HTMX + SSE), TCP listener, log streaming    | merged                          |
| 4     | Action edit / import / export through the API                        | merged (CLAUDE-7)               |
| 5     | Token auth for non-loopback binds                                    | merged (CLAUDE-48)              |
| 6     | Retention policy enforcement on the unbounded history tables, polish | merged (CLAUDE-172, CLAUDE-175) |

Two unrelated numbering schemes both say "Phase N", and a bare `grep 'Phase 3'` returns hits from both. The roadmap phases in this table are one (e.g. `src/web/mod.rs` "Phase 3" = Web UI, `src/auth.rs` "Phase 5" = token auth). The separate native-actions epic (CLAUDE-30..CLAUDE-34, tracked under CLAUDE-25) is the other;
its comments carry the "native-action" qualifier (e.g. `src/native/youtrack.rs` "Phase 3 of the native-action epic" = CLAUDE-32, which is NOT roadmap Phase 3). Tell them apart by that qualifier and the CLAUDE-30..34 / CLAUDE-25 issue range; a bare "Phase N" with no "native-action" wording refers to this table.

Cross-cutting (not a numbered phase): a container runtime (CLAUDE-37, made self-contained in CLAUDE-247) runs the daemon and its spawned `claude` children in an isolated `$HOME` with a slim bot CLAUDE.md and its own logins, decoupled from the host's operator config. See `oci-build/Dockerfile` (`runtime` target), `oci-build/entrypoint.sh`, `compose.yml`, and `deploy/bot-CLAUDE.md`.

## Working on this repo

- Branch off `main` per `governance/GIT_WORKFLOW.md`. Run `just pre-push` (or rely on the installed pre-push hook) before pushing; `just install-hooks` writes both the `pre-commit` (fmt-only) and `pre-push` (full suite) stubs, so re-run it once after pulling a `common` bump to pick up the `pre-push` hook.
- `just check` runs the same fmt + clippy + build + tests CI runs. CI also runs the CLAUDE.md guard as its own `claude-md` job.
- The task runner is the shared `psa-systems/common` recipe set, vendored as the `common` submodule (`git submodule update --init` after a fresh clone).
  `justfile` configures it through variables (`pre_commit_mode := "docker"` + `dev_image` because there is no `compose.dev.yml`, `test_args := ""` so the library-target unit tests are not skipped) and overrides only `default`, `check`, and `run`. Prefer setting a common variable over shadowing a shared recipe.
  `default` is redefined locally (as the file's first recipe) because just resolves the default recipe by source order and a top-level definition outranks the imported one, so without it a bare `just` runs whichever local recipe comes first instead of common's `just --list` (CLAUDE-201).
  CI keeps its own `.forgejo/workflows/check.yml` rather than calling common's reusable one, which hardcodes `cargo test --bins`; making that configurable is tracked in #PC-6. It also deviates from governance `CI.md` ("Check runs on every push / PR"):
  the workflow still runs on every push and PR to `main`, but its compile job is skipped (via a leading `changes` job and an `if:`, not a workflow-level `paths-ignore`) when only `docs/**`, `README.md`, or `CLAUDE.md` changed (CLAUDE-286).
- Daemon smoke-test pattern: `CLAUDE_RUN_*_DIR=/tmp/...` env vars + a stub `claude` on `PATH` (see `src/bin/claude-run.rs` smoke tests for the shape).
- The legacy `scripts/claude-run.nu` is gone as of the governance-conformance PR. The Rust binary is the only runner.
