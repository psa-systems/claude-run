# Configuration

## Filesystem layout

The image sets each root with its env var; [self-hosting.md](self-hosting.md) maps them to volumes.

| Container path      | Env var                  | What                                                                                            |
|---------------------|--------------------------|-------------------------------------------------------------------------------------------------|
| `/config`           | `CLAUDE_RUN_CONFIG_DIR`  | `config.toml`, `actions/*.yml`, `secrets.key`                                                   |
| `/data`             | `CLAUDE_RUN_DATA_DIR`    | `state.db` (SQLite backend only; see "State store backend"), `workdirs/`                        |
| `/state`            | `CLAUDE_RUN_STATE_DIR`   | `logs/daemon.log`, `logs/<action>/<output.file>`, `logs/<action>/streams/<key>-attempt-<n>.jsonl`, `snippets/`, `webhook-logs/` |
| `/run/claude-run`   | `CLAUDE_RUN_RUNTIME_DIR` | `daemon.pid`, `locks/<action>.lock` (no `daemon.sock`: the image runs `serve --no-socket`)      |

`workdir_base` (action YAML field or `CLAUDE_RUN_WORKDIR_BASE` env) overrides where per-job workdirs live; defaults to
`$CLAUDE_RUN_DATA_DIR/workdirs` (`/data/workdirs` in the container). Supports `~`, `$VAR`, `${VAR}` expansion.

## State store backend: SQLite or PostgreSQL (CLAUDE-250)

Runtime state (runs, job outcomes, caps, settings, the encrypted secrets store) lives in SQLite at
`<CLAUDE_RUN_DATA_DIR>/state.db` by default. A deployment that already runs a database server can point the daemon at
PostgreSQL instead. Both backends run through one sqlx `Any` pool and one set of statements, so nothing else about the
runner changes with the choice.

**Three inputs, highest precedence first.** The backend has to be known before the DB is open, so this is a bootstrap
setting, never one of the DB-backed Settings sections:

| Input | Form |
|---|---|
| `CLAUDE_RUN_DATABASE_URL_FILE` | path to a file whose contents are the URL |
| `CLAUDE_RUN_DATABASE_URL` | the URL itself |
| `[database] url` in `config.toml` | the URL itself |

All three unset means SQLite at `<CLAUDE_RUN_DATA_DIR>/state.db`, exactly as before this existed: an existing install
needs no config edit, moves no data, and re-runs no migration. A `postgres://` or `postgresql://` URL selects
PostgreSQL; `sqlite://<path>` selects SQLite explicitly. Any other scheme stops startup with an error naming the scheme,
and an unreachable PostgreSQL is a startup error naming the host and the driver's own message, retried on a bounded
deadline (compose can start the daemon first) and then fatal. Neither case ever falls back to SQLite, which would
silently start a second, empty state store.

The file form exists because the URL carries the password, and an environment value is readable through
`docker inspect` and `/proc/<pid>/environ`. `CLAUDE_RUN_DATABASE_URL` remains available for a deployment where that
exposure is acceptable.

```toml
# config.toml
[database]
url = "postgres://claude_run:secret@db.internal:5432/claude_run"
```

**Compose.** `compose.yml` carries the server behind a `postgres` profile, so `docker compose up` starts exactly the
service set it started before, and `docker compose --profile postgres up` adds `claude-bot-postgres` (its data on the
`claude-bot-postgres` volume, its password from a compose secret, a `pg_isready` healthcheck the daemon waits on).
Point the daemon at it by uncommenting the `CLAUDE_RUN_DATABASE_URL_FILE` environment line and the
`claude-run-database-url` secret in the `claude-bot` service, then creating the two secret files:

```nu
openssl rand -base64 32 | save secrets/postgres-password.txt
"postgres://claude_run:<that password>@claude-bot-postgres:5432/claude_run" | save secrets/database-url.txt
docker compose --profile postgres up --detach
```

**Nothing is carried over by switching alone.** Pointing an existing install at PostgreSQL and starting it opens a
fresh, empty instance: no run history, no caps ledger, no stored secrets, and no web accounts, so the browser lands
on `/setup` again. Move the data first with `claude-run import`.

**Adding a table.** `migrations/sqlite/` is a chain (each file byte-identical once shipped, so an existing `state.db`
matches its recorded checksums); `migrations/postgres/0001_init.sql` is a squash of that chain's end state, spelled per
the type mapping at the top of that file. A schema change is therefore a new incremental file in EACH directory (e.g.
`migrations/sqlite/0021_ci_failures.sql` and `migrations/postgres/0002_ci_failures.sql` for CLAUDE-272's CI-failure
store), with the same end schema on both sides, never an edit to an existing file. `db::tests::the_two_migration_sets_end_on_the_same_schema`
is what catches the two directories drifting apart.

### Importing an existing SQLite store (CLAUDE-251)

`claude-run import` copies every table out of a SQLite state store and into the database the configuration resolves to,
so a switched backend keeps the same history, the same attempt and daily caps, the same settings documents, the same
encrypted secrets and the same web accounts. Stop the daemon, point the configuration at the new database, and run
it as a one-off container (the full sequence is at the end of this section):

```nu
docker compose stop claude-bot
docker compose --profile postgres run --rm claude-bot import
```

```
table                  source      copied      target
-----------------  ----------  ----------  ----------
actions                     3           3           3
attempt_counters          412         412         412
...
```

- **The target is the configured database**, never a flag of its own, so the import and the daemon cannot disagree about
  where the rows went. Set `[database] url`, `CLAUDE_RUN_DATABASE_URL` or `CLAUDE_RUN_DATABASE_URL_FILE` first; the
  import runs the target's migrations itself, so the database only has to exist and be reachable.
- **`--from <file>` reads any SQLite store**, defaulting to `<CLAUDE_RUN_DATA_DIR>/state.db`. Importing a file into
  itself is refused, which is what the default invocation with no database URL set would otherwise be.
- **The source is opened read-only** and is never written, so a refused or failed import leaves you exactly where you
  started, still able to run the daemon off the same file. Stop the daemon first: a read-only open cannot recover a
  write-ahead log left behind by a killed process, and the open fails naming the file if one is there.
- **A target that already holds rows is refused**, naming each populated table and its count, unless you pass
  `--allow-nonempty`. The whole copy is one transaction, so a failure at any table commits nothing.
- **Both sides are printed per table** (source rows, rows copied, rows the target holds afterwards) rather than a bare
  success line, and a table whose counts do not add up aborts the import before the commit. On PostgreSQL the identity
  sequences behind `runs`, `job_outcomes`, `run_events` and `secrets` are advanced past the copied ids, so the daemon's
  first insert does not collide with imported history.
- **The two schemas must match exactly.** A source older than the build doing the import is refused, listing every
  missing column; open that file once with this build (`CLAUDE_RUN_DATABASE_URL=sqlite://<file> claude-run status`) to
  apply its pending SQLite migrations, then import.

The age key is not in the database and is not copied: back it up as usual and keep it where the new deployment reads it
(`<CLAUDE_RUN_CONFIG_DIR>/secrets.key`), or the imported ciphertext cannot be decrypted.

The full sequence, with the daemon stopped so nothing writes either side:

```nu
docker compose stop claude-bot
docker compose --profile postgres run --rm claude-bot import
docker compose --profile postgres up --detach
```

## Settings: bootstrap file vs the Settings page (CLAUDE-101)

Configuration splits in two. `config.toml` is the **bootstrap** file: only what the daemon must know before it can open
the DB and bind the listener, namely `[database]` (CLAUDE-250: which state store to open, see
[State store backend](#state-store-backend-sqlite-or-postgresql-claude-250)), `[web]` (bind / port / `auth_token_ref` /
login) and `[daemon]` binary overrides (
`nu_binary`, `claude_binary`) plus `[daemon].claude_config_dir` (CLAUDE-165: where the spawned agent reads its
user-level `CLAUDE.md` / credentials from; defaults to `<CLAUDE_RUN_CONFIG_DIR>/claude-agent-home`, seeded at serve
start with the slim bot `CLAUDE.md`, and with a symlink to the host `~/.claude/.credentials.json` when the host has
one). Everything runtime-tunable, namely issue minting (`[mint]`), the webhook sources (`[webhook]`), and the log filter (`[daemon].log_filter`), now lives in the database
and is edited in the app on the **Settings** page (`/settings`, in the nav next to Dashboard), behind the same auth gate
as the rest of the web UI. A `config.toml` containing only `[web]` and `[daemon]` binary overrides is a complete, valid
configuration, and so is an empty one.

Each section is one JSON document validated against the same serde struct the file parser used (`deny_unknown_fields`),
so an invalid edit is rejected inline with the parser's own error and nothing is persisted. Secret values are never
shown or stored on the page: fields ending in `_ref` (`auth_token_ref`, a webhook source's `secret_ref` /
`log_token_ref` / `write_token_ref`, a comment source's `write_token_ref`) name a credential in
the Secrets store and are rendered as that name only. Each
section (and field) carries an apply badge: `live` for the log filter and webhook source edits (allow-list, rate limit,
secret refs, adding a source), `next event` for mint, and `restart` for the webhook
`replay_window_secs` (the replay store is built once at start). The same documents round-trip over the JSON API at
`GET` / `PUT /api/settings/{mint,webhook,webhook_comment,log_filter,workdir_gc,stream_log_gc,preferences,concurrency,db_retention,claude,email}`.
`email` (CLAUDE-394: the SMTP transport an invite is mailed over) is `live`-applied like the Auth logins, and for the
same reason as Auth gets its own hand-written **Email (SMTP)** card on the Settings page rather than the generic JSON
editor: a password field needs a real `type="password"` input and "leave blank to keep the current password"
semantics, not a JSON textarea. Its fields are `smtp_host`, `smtp_port`, `smtp_encryption` (`starttls` | `tls` |
`none`), `smtp_username`, `smtp_password_ref` (name-only, like the other secret-ref fields above), `from`, and
`public_url`; the card's own "Send test email" button exercises the saved document exactly as a real invite send
would. `EmailConfig::configured()` requires `smtp_host`, `from`, and `public_url` all set before the Accounts page
offers emailing an invite instead of showing its link.
The `webhook_comment` (CLAUDE-220), `workdir_gc` (CLAUDE-106), `stream_log_gc` (CLAUDE-245), `preferences` (CLAUDE-154),
`concurrency` (CLAUDE-162), `db_retention` (CLAUDE-175), and `claude` (CLAUDE-239 `model`, CLAUDE-259 `effort`: the instance-wide `--model` and
`--effort` defaults an action inherits when it pins neither in its own `claude:` block, each one null by default, which passes no flag and leaves
the `claude` CLI's own default in force) sections are DB-only from birth (never `config.toml` keys), so
they are not part of the one-time import below; each simply defaults until edited. `webhook_comment` is additionally rejected by the file parser:
a `config.toml` declaring `[webhook_comment]` fails to start with serde's unknown-field error naming the section, so
there is no way to end up with two sources of truth for it.

**One-time import, then breaking.** The first daemon start after upgrading imports any of the four moved sections still
present in `config.toml` into the database and logs a warning naming each one and telling you to remove it from the
file. After that, remove the migrated sections from `config.toml`: a later start that finds a section in **both** the
file and the database refuses to run, naming the section, so there is never silent drift between two sources of truth (
the same breaking-migration signal CLAUDE-87 used). A `config.toml` with no moved sections starts cleanly with the
database as the only source.

## Logging

Log verbosity is a [tracing
`EnvFilter`](https://docs.rs/tracing-subscriber/latest/tracing_subscriber/filter/struct.EnvFilter.html#directives)
directive string. Precedence is `CLAUDE_RUN_LOG` (env var) > the stored log filter (Settings page, formerly
`[daemon].log_filter`) > the built-in default `info,claude_run=info,sqlx=warn`.

The persistent filter is now edited on the **Settings** page (the `Log filter` section, or`PUT /api/settings/log_filter`
with `{"filter": "info,claude_run=debug,sqlx=warn"}`). Saving applies it live: the daemon swaps the active filter
through the tracing reload handle and logs a `log filter applied from settings` line, no restart needed. Set
`CLAUDE_RUN_LOG=...` for one-shot debugging: when it is present it wins, and a saved filter persists but is not applied
for that run. An invalid filter is rejected inline (and at startup logs a warning naming the value and the parse error,
keeping the current filter rather than aborting).
