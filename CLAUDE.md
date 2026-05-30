# claude-run

YAML-driven runner for Claude Code: poll an external source (YouTrack issues, Forgejo PRs, ...), feed each job to `claude --print`, do something with the result. Shipped as a single Rust binary (`claude-run`) that doubles as a CLI and a long-running daemon with a unix-socket API.

## Shape

- Rust binary, library + bin. `clap` derive. Subcommand tree: `claude-run {run,serve,stop,restart,trigger,pause,resume,status,action,migrate}`.
- Actions are YAML files under `$CLAUDE_RUN_CONFIG_DIR/actions/` (default `~/.config/claude-run/actions/`). Each action's `input.command` / `filter` / `pre[]` / `prompt` / `post[]` is a nu snippet shelled out via the host's `nu` binary.
- Runtime state in SQLite at `$CLAUDE_RUN_DATA_DIR/state.db`. Per-attempt stream logs at `$CLAUDE_RUN_STATE_DIR/logs/<action>/streams/<key>-attempt-<n>.jsonl`.
- Daemon (`claude-run serve`) owns scheduling and exposes an HTTP API over `$CLAUDE_RUN_RUNTIME_DIR/daemon.sock`. CLI subcommands delegate to the daemon when reachable, else operate directly on the DB.
- Runtime tool dependencies (host-installed, not bundled in the binary): `nu`, `claude` (Claude Code CLI), `timeout` (coreutils). Optional: `yt`, `fj`, `git` if the actions need them.
- Forgejo home: `psa-systems/claude-run`.

## Conventions

- Governance lives at `niceguyit/governance`. `CHECKLIST.md`, `CI.md`, `PRE_COMMIT.md`, `FORGEJO_ORGS.md`, `GIT_WORKFLOW.md` are the source of truth; this file documents only claude-run-specific deviations.
- Builder image: `dev.a8n.run/pandoras-box/rust-builder-musl:v1.0.0-rust1.94-alpine`. No runtime image - the binary ships as a Generic Package only. Users install `nu` + `claude` themselves.
- Compose files, `.env.example`, HOST_IP/PORT, port assignments do NOT apply. The daemon binds a unix socket by default (Phase 2); the TCP web UI is Phase 3 and will be reachable on `127.0.0.1:7321` by default.
- Action schema is documented in `actions/SCHEMA.md`. Authoritative; changes here ripple to both the runner and the doc.

## Phases

The Rust port is rolling out in phases against the plan agreed in PR review:

| Phase | Scope | Status |
|---|---|---|
| 1 | `run` / `migrate`, all core modules, SQLite-backed state | merged |
| 2 | `serve [--detach]`, scheduler, unix-socket API, lock, pause-in-DB | merged |
| 3 | Web UI (axum templates + HTMX + SSE), TCP listener, log streaming | TBD |
| 4 | Action edit / import / export through the API | TBD |
| 5 | Token auth for non-loopback binds | TBD |
| 6 | Retention policy enforcement on `job_outcomes`, polish | TBD |

## Working on this repo

- Branch off `main` per `governance/GIT_WORKFLOW.md`. Run `just pre-commit` (or rely on the installed hook) before pushing.
- `just check` runs the same fmt + clippy + build + tests CI runs.
- Daemon smoke-test pattern: `CLAUDE_RUN_*_DIR=/tmp/...` env vars + a stub `claude` on `PATH` (see `src/bin/claude-run.rs` smoke tests for the shape).
- The legacy `scripts/claude-run.nu` is gone as of the governance-conformance PR. The Rust binary is the only runner.
