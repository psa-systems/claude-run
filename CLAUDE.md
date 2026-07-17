# claude-run

YAML-driven runner for Claude Code: poll an external source (YouTrack issues, Forgejo PRs, ...), feed each job to `claude --print`, do something with the result. Shipped as a single Rust binary (`claude-run`) that doubles as a CLI and a long-running daemon with a unix-socket API.

## Shape

- Rust binary, library + bin. `clap` derive. Subcommand tree: `claude-run {run,serve,stop,restart,trigger,pause,resume,status,action,migrate,version}`.
- Actions are YAML files under `$CLAUDE_RUN_CONFIG_DIR/actions/` (default `~/.config/claude-run/actions/`). Each action's `input.command` / `filter` / `pre[]` / `prompt` / `post[]` is a nu snippet shelled out via the host's `nu` binary.
- Runtime state in SQLite at `$CLAUDE_RUN_DATA_DIR/state.db`. Per-attempt stream logs at `$CLAUDE_RUN_STATE_DIR/logs/<action>/streams/<key>-attempt-<n>.jsonl`.
- Daemon (`claude-run serve`) owns scheduling and exposes an HTTP API over `$CLAUDE_RUN_RUNTIME_DIR/daemon.sock`. CLI subcommands delegate to the daemon when reachable, else operate directly on the DB.
- Runtime tool dependencies (host-installed, not bundled in the binary): `nu`, `claude` (Claude Code CLI), `timeout` (coreutils). Optional: `yt`, `fj`, `git` if the actions need them.
- Forgejo home: `psa-systems/claude-run`.

## Conventions

- Governance lives at `niceguyit/governance`. `CHECKLIST.md`, `CI.md`, `PRE_COMMIT.md`, `FORGEJO_ORGS.md`, `GIT_WORKFLOW.md` are the source of truth; this file documents only claude-run-specific deviations.
- Builder image: `dev.a8n.run/pandoras-box/rust-builder-musl:v1.0.0-rust1.94-alpine`. The binary still ships as a Generic Package for host installs. There is also a runtime image (CLAUDE-37): `oci-build/Dockerfile --target runtime` builds the daemon plus its pinned toolchain (`nu`, `claude`, `yt`, `fj`, `timeout`, `git`) on the builder image; CI publishes it to the Forgejo OCI registry as `dev.a8n.run/psa-systems/claude-run:{vX.Y.Z,latest}` with the same tag set as the binary, from its own workflow `.forgejo/workflows/build-image.yml` (split out of `build-binary.yml` in CLAUDE-94), and `compose.yml` runs the published image. The container user's `~/.claude/CLAUDE.md` is the slim bot file `deploy/bot-CLAUDE.md`, so the agent never inherits the operator's interactive global CLAUDE.md. Host auth reaches the container through read-only bind mounts; `~/.claude` is never mounted wholesale. See README "Containerized deployment".
- Compose files, `.env.example`, HOST_IP/PORT, port assignments do NOT apply. The daemon binds a unix socket by default (Phase 2); the TCP web UI is Phase 3 and will be reachable on `127.0.0.1:7321` by default.
- Action schema is documented in `actions/SCHEMA.md`. Authoritative; changes here ripple to both the runner and the doc.
- Config splits into bootstrap vs runtime settings (CLAUDE-101). `config.toml` holds only bootstrap keys the daemon needs before it can open the DB and bind: `[web]` and `[daemon]` binary overrides (`nu_binary`, `claude_binary`). Everything runtime-tunable, the `[mint]` / `[update]` / `[webhook]` sections and `[daemon].log_filter`, is DB-backed (one JSON document per section in the `settings` table, keyed `config.<section>`) and edited on the web Settings page (`/settings`) or via `GET`/`PUT /api/settings/<section>`. The serde structs in `src/config.rs` stay the schema; `src/settings.rs` owns the store, the live-apply (`LiveSettings`), and the one-time import then breaking-on-conflict migration (`migrate_config_file`, run at serve start). A moved section present in both `config.toml` and the DB refuses to start, naming it.

## Phases

Feature delivery landed in numbered phases; this table is the status list. Phases 1-5 have shipped; Phase 6 is the only row with outstanding work.

| Phase | Scope | Status |
|---|---|---|
| 1 | `run` / `migrate`, all core modules, SQLite-backed state | merged |
| 2 | `serve [--detach]`, scheduler, unix-socket API, lock, pause-in-DB | merged |
| 3 | Web UI (axum templates + HTMX + SSE), TCP listener, log streaming | merged |
| 4 | Action edit / import / export through the API | merged (CLAUDE-7) |
| 5 | Token auth for non-loopback binds | merged (CLAUDE-48) |
| 6 | Retention policy enforcement on the unbounded history tables, polish | `job_outcomes` shipped (CLAUDE-175); `run_events` shipped (CLAUDE-172) |

Two unrelated numbering schemes both say "Phase N", and a bare `grep 'Phase 3'` returns hits from both. The roadmap phases in this table are one (e.g. `src/web/mod.rs` "Phase 3" = Web UI, `src/auth.rs` "Phase 5" = token auth). The separate native-actions epic (CLAUDE-30..CLAUDE-34, tracked under CLAUDE-25) is the other; its comments carry the "native-action" qualifier (e.g. `src/native/youtrack.rs` "Phase 3 of the native-action epic" = CLAUDE-32, which is NOT roadmap Phase 3). Tell them apart by that qualifier and the CLAUDE-30..34 / CLAUDE-25 issue range; a bare "Phase N" with no "native-action" wording refers to this table.

Cross-cutting (not a numbered phase): a container runtime (CLAUDE-37) runs the daemon and its spawned `claude` children in an isolated `$HOME` with a slim bot CLAUDE.md, decoupled from the host's operator config. See `oci-build/Dockerfile` (`runtime` target), `compose.yml`, and `deploy/bot-CLAUDE.md`.

## Working on this repo

- Branch off `main` per `governance/GIT_WORKFLOW.md`. Run `just pre-commit` (or rely on the installed hook) before pushing.
- `just check` runs the same fmt + clippy + build + tests CI runs.
- Daemon smoke-test pattern: `CLAUDE_RUN_*_DIR=/tmp/...` env vars + a stub `claude` on `PATH` (see `src/bin/claude-run.rs` smoke tests for the shape).
- The legacy `scripts/claude-run.nu` is gone as of the governance-conformance PR. The Rust binary is the only runner.
