# claude-run

YAML-driven runner for Claude Code. Poll an external source (YouTrack issues, Forgejo PRs, whatever produces a JSON array), feed each row to `claude --print`, and do something with the result. Single Rust binary, doubles as a CLI and a long-running daemon.

## What it gives you

- **Per-action YAML files** describe input (a nu snippet that emits a JSON array), an optional filter, pre and post nu snippets, and the prompt sent to claude. See [`actions/SCHEMA.md`](actions/SCHEMA.md).
- **Scheduling**: each action declares `schedule.kind: interval | cron | manual`; the daemon's built-in scheduler fires on time without external cron.
- **Per-row streaming logs**: claude is invoked with `--output-format stream-json` and the NDJSON events land in `$STATE_DIR/logs/<action>/streams/<key>-attempt-<n>.jsonl`. `tail -f` to watch live.
- **Cost ledger + caps**: per-day Claude-invocation cap, per-day USD budget, per-row attempt cap. All in SQLite.
- **Skip-attempt marker**: an action's post step can write `$workdir/.claude-run-skip-attempt` for outcomes that park a row pending human input (so the attempt cap doesn't trip).
- **Pause / resume**: globally or per-action; persisted in SQLite, takes effect on the next scheduler tick.
- **HTTP API over a unix socket**: the daemon exposes `/status`, `/actions`, `/actions/<n>/trigger`, `/pause`, `/resume`, ... The CLI client speaks that API when the daemon is reachable, else operates on the DB directly.

## Install

Pull the latest binary from the Forgejo Generic Packages registry:

```nu
http get https://dev.a8n.run/api/packages/psa-systems/generic/claude-run/latest/claude-run-linux-x86_64 | save claude-run
chmod +x claude-run
sudo mv claude-run /usr/local/bin/
```

Runtime dependencies (host-installed, not bundled):

- `nu` 0.112.2+ - executes the action snippets.
- `claude` (Claude Code CLI) - the AI invocation itself.
- `timeout` (coreutils) - wall-clock cap around claude.
- Whatever CLIs the actions themselves invoke (`yt`, `fj`, `git`, ...).

## Use

```nu
# One-shot manual run (cli-driven; standalone). Same exit codes as the old nu script.
claude-run run youtrack

# Long-running daemon: scheduler + unix-socket API. --detach forks into the background.
claude-run serve --detach

# Manual out-of-cycle run via the daemon (counts as triggered_by=trigger in the runs table).
claude-run trigger youtrack

# Status (delegates to daemon if it's up, else reads DB directly).
claude-run status

# Pause / resume.
claude-run pause youtrack --reason "addressing a blocker"
claude-run resume youtrack

# Action management.
claude-run action list
claude-run action show youtrack

# Import legacy ~/.local/share/claude-run/state/*.json into SQLite (one-time migration off the nu runner).
claude-run migrate
```

## Filesystem layout

XDG-compliant; each path overridable by the matching env var.

| Default | Env override | What |
|---|---|---|
| `$XDG_CONFIG_HOME/claude-run/` | `CLAUDE_RUN_CONFIG_DIR` | `config.toml`, `actions/*.yml` |
| `$XDG_DATA_HOME/claude-run/` | `CLAUDE_RUN_DATA_DIR` | `state.db`, `workdirs/` |
| `$XDG_STATE_HOME/claude-run/` | `CLAUDE_RUN_STATE_DIR` | `logs/<action>/YYYY-MM-DD.jsonl`, `logs/<action>/streams/<key>-attempt-<n>.jsonl`, `daemon.log` |
| `$XDG_RUNTIME_DIR/claude-run/` | `CLAUDE_RUN_RUNTIME_DIR` | `daemon.pid`, `daemon.sock`, `locks/<action>.lock` |

`workdir_base` (action YAML field or `CLAUDE_RUN_WORKDIR_BASE` env) overrides where per-row workdirs live; defaults to `$DATA_DIR/workdirs/`. Supports `~`, `$VAR`, `${VAR}` expansion.

## Development

```nu
just --list           # show all recipes
just check            # fmt + clippy + build + tests (what CI runs)
just test             # cargo test
just build            # debug build into target/debug/
just build-static     # musl-static binary via the Docker builder into ./output/claude-run
just build-docker     # full OCI build (builder stage)
just build-docker-export   # extract the static binary out of the OCI build
just install-hooks    # one-time: install pre-commit hook that runs CI's checks
```

See [CLAUDE.md](CLAUDE.md) for the project-specific deviations from `niceguyit/governance` and the phased rollout plan.

## License

MIT. See [LICENSE.md](LICENSE.md).
