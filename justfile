# claude-run task runner.
#
# Most recipes come from the shared psa-systems task runner, vendored as the
# `common` git submodule and imported below. After cloning this repo run
# `git submodule update --init` so the import resolves. To pull a newer common:
# update the submodule and commit the new gitlink.
#
# claude-run configures the shared recipes through variables wherever common
# exposes one (per common's blast-radius rule), so the shared implementation
# stays the single source of truth:
#   - app: image tags (claude-run:check|local) and cargo cache volume names.
#   - pre_commit_mode = "docker" + dev_image: claude-run has no compose.dev.yml
#     (daemon plus CLI, not a web service), so `pre-push` runs its fmt check in
#     the rust-builder-musl image via `docker run`, not a compose service.
#   - dockerfile defaults to oci-build/Dockerfile, which is where it lives here.
#
# Inherited from common/common.just: install-hooks, pre-commit, pre-push
# (+ -compose / -docker), check-fmt, check-clippy, check-build, check-docker,
# fmt, lint, typecheck, build, test, build-docker, build-docker-export,
# dev-clean, dev-clean-all, create-release (+ -generic / -workspace). Cargo.toml
# has no [features], so common's `--all-features` on clippy/typecheck is a no-op
# here; package.name == app, so create-release's `cargo update --package` in the
# default "generic" layout resolves.
#
# common also ships compose-only web-service recipes (dev-local*, dev-logs,
# ensure-env) that assume a compose.dev.yml `app` service. claude-run has no
# compose stack, so those inherited recipes are inapplicable here (harmless if
# left unused); dev-clean detects the missing compose.dev.yml and skips its
# teardown, so it needs no override.
#
# Three recipes below override a common recipe (`default`, `check`, `run`); the
# rest have no common counterpart.
#
# Recipe ORDER matters here: just picks the default recipe by source order and a
# top-level definition outranks an imported one, so `default` must stay the
# FIRST recipe in this file. Do not move it down or add a recipe above it.

# A recipe defined here overrides the imported one of the same name. common
# already sets allow-duplicate-variables (which lets `app` below override its
# placeholder), so only the recipe setting is needed here.
set allow-duplicate-recipes := true

import 'common/common.just'

# Drives image tags ({{app}}:check/:local) and the cargo cache naming in the
# inherited recipes. Overrides common's placeholder default.
app := "claude-run"

# No compose.dev.yml: run the pre-commit/pre-push checks in the builder image
# via `docker run`. Tracks the rust-builder-musl tag baked into
# oci-build/Dockerfile so the local toolchain matches CI.
pre_commit_mode := "docker"
dev_image := "dev.a8n.run/pandoras-box/rust-builder-musl:v1.2.0-rust1.98.1-alpine"

# docs/self-hosting.md states an issue reference inline in prose on the same
# line as the RUNS_ON_* variable list a rename touches, so editing that list
# re-stages the pre-existing reference and trips check-internal-names; adds
# the common default's own entries back since this overrides rather than
# extends.
internal_names_exempt_paths := ".forgejo/workflows/ .gitmodules renovate.json Dockerfile* compose*.yml docs/self-hosting.md"

# compose.yml carries a placeholder Postgres URI example that CI's own
# kingfisher scan already allowlists by regex and that .noseyparker-ignore.txt
# already exempts by path; the local commit-msg hook has no regex input for
# kingfisher that survives this recipe's own shell quoting (a `\$` in
# `{{ secrets_kingfisher_skip_regex }}` is unescaped by the recipe's double
# quotes before kingfisher sees it), so exempt the same file by path instead,
# matching the noseyparker ignore already in place for it.
secrets_kingfisher_excludes := "compose.yml"

# Placeholder credentials in redaction-test fixtures, the same ones CI's
# kingfisher_skip_regex allowlists; the alternatives with a literal `$` or
# backslash stay CI-only because they do not survive this recipe's quoting.
secrets_kingfisher_skip_regex := "claude_run:(claude_run|secret)@|oauth2:(secret|tok|tok-123)@|ghp_(secret|from_login|action)@|user:s3cr3t@"

# -- Default (override) -------------------------------------------------------

# List available recipes. Must stay the FIRST recipe in this file: just resolves the default recipe by source order, and a top-level definition outranks the imported one, so whatever appears first here wins over common's `default`.
[group: 'dev']
default:
    @just --list

# -- Checks (override) --------------------------------------------------------

# Umbrella check: CLAUDE.md guard + fmt + clippy + build + tests + builder-stage docker compile. Overrides common's `check`, which omits tests; reuses the inherited `test` recipe so tests run as part of `just check`.
[group: 'check']
check: check-claude-md check-fmt check-clippy check-build test check-docker

# Guard CLAUDE.md: 20,000-byte cap, 400-character lines, resolvable docs/ links. Runs its self-test first.
[group: 'check']
check-claude-md:
    nu scripts/check-claude-md.nu --self-test
    nu scripts/check-claude-md.nu

# -- Development --------------------------------------------------------------

# Run the CLI with arguments (e.g. just run status, just run trigger youtrack). Overrides common's `run`, which builds --release; the debug build keeps CLI iteration fast.
[group: 'dev']
run *args:
    cargo run -- {{ args }}

# cargo-watch loop for continuous compilation.
[group: 'dev']
watch:
    cargo watch --clear

# Run the test suite against a fresh, throwaway, non-durable Postgres.
[group: 'dev']
test-postgres:
    cargo run -- test-postgres -- cargo test

# -- Migrations ---------------------------------------------------------------

# Apply pending sqlx migrations against the local state.db (uses the same XDG paths the binary uses at runtime).
[group: 'migrations']
migrate:
    cargo run -- migrate

# Re-apply legacy state.json import against the SQLite tables (idempotent).
[group: 'migrations']
migrate-revert:
    @echo "no-op: schema-only migrations are forward-only via sqlx::migrate!; use a fresh state.db if you need to revert."
