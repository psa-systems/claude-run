# claude-run

A YAML-driven runner for Claude Code: poll a source (YouTrack issues, Forgejo or GitHub CI failures, pull request comments, anything that emits a JSON array), hand each job to `claude --print`, and act on the result. It runs as a Docker container: one Rust binary that schedules every action, serves a web UI, and listens for webhooks.

## What it is

- **Actions** are YAML files under `$CLAUDE_RUN_CONFIG_DIR/actions/`. A scripted action runs nu snippets for its input, filter, pre and post steps; a native action (`youtrack`, `closed-issue-summary`) is a Rust handler that owns the whole run. The schema is [`actions/SCHEMA.md`](actions/SCHEMA.md).
- **Guard rails**: per-run and per-day spend ceilings, attempt caps, a global pause honored at every entry point, and audited, capped tracker writes.
- **State** lives in SQLite by default, or PostgreSQL when the operator sets one.

## Install

Docker is the only supported install. `compose.yml` runs the published image, which carries the tools the runner calls (`nu`, `claude`, `yt`, `fj`, `gh`, `git`, `timeout`). The image is published under the private `psa-systems-private` package owner, so pulling it needs a Forgejo PAT that can read it. The full guide is [docs/self-hosting.md](docs/self-hosting.md).

## Documentation

Operator and developer docs live in [`docs/`](docs/):

- [overview.md](docs/overview.md) - what the runner does, feature by feature
- [forge-tokens.md](docs/forge-tokens.md) - forge token scopes, and resuming an open PR on re-queue
- [usage.md](docs/usage.md) - the CLI, the web UI pages, and the access check
- [configuration.md](docs/configuration.md) - filesystem layout, SQLite or PostgreSQL, bootstrap file vs Settings page, logging
- [operations.md](docs/operations.md) - workdirs, retention, concurrency caps, global pause, restart safety
- [agent-writes-and-audits.md](docs/agent-writes-and-audits.md) - what the agent may write to the tracker, and scheduled audits
- [auth.md](docs/auth.md) - web UI and API auth, service logins, the secrets store
- [webhooks.md](docs/webhooks.md) - CI-failure webhooks, automerge, auto-fix, GitHub setup
- [comment-commands.md](docs/comment-commands.md) - commands posted as pull request or issue comments
- [self-hosting.md](docs/self-hosting.md) - install: the container image and `compose.yml`
- [ROADMAP.md](docs/ROADMAP.md) - planned work

Conventions for AI agents working in this repository are in [CLAUDE.md](CLAUDE.md), with the full text of each in [docs/conventions/](docs/conventions/).

## Contributing

- Work happens in `feat/` / `fix/` / `chore/<short-descriptive-name>` branches off `main`; the merge target is `main` via PR.
- Clone with `git clone --recurse-submodules`, or run `git submodule update --init` in an existing clone: the `justfile` imports the shared recipes from the `common` submodule ([psa-systems/common](https://dev.a8n.run/psa-systems/common)).
- `just check` runs fmt + clippy + build + tests + the docker builder stage, the same gate CI runs. `cargo test` runs on SQLite; set `CLAUDE_RUN_TEST_DATABASE_URL` to a disposable PostgreSQL server to run it on PostgreSQL too (the harness refuses a database that already has its own `public._sqlx_migrations`), or run `just test-postgres` / `claude-run test-postgres -- cargo test` for a fresh, throwaway, non-durable one with no setup. A schema change needs a migration in both `migrations/sqlite/` and `migrations/postgres/`.
- `just install-hooks` writes the git `pre-commit` (fmt only) and `pre-push` (full suite) hooks.

## Development happens on Forgejo

The development home for this repository is <https://dev.a8n.run/psa-systems/claude-run>. File issues and open pull requests there.

## Security

Please do not report a suspected vulnerability through the public issue tracker: filing it there publishes it. Contact a maintainer privately instead. A published disclosure address and a `SECURITY.md` are being set up and this section will link to them.

## License

MIT. See [LICENSE](LICENSE).

## Authors and credits

Copyright Nice Guy IT, LLC; see [LICENSE](LICENSE).

Built on [Rust](https://www.rust-lang.org/), [axum](https://github.com/tokio-rs/axum), [Askama](https://github.com/askama-rs/askama), [htmx](https://htmx.org/) and [SQLx](https://github.com/launchbadge/sqlx), with action snippets run by [Nushell](https://www.nushell.sh/) and the agent itself by [Claude Code](https://claude.com/claude-code).
