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
#     (daemon plus CLI, not a web service), so `pre-commit` and `pre-push` run
#     their checks in the rust-builder-musl image via `docker run`, not a
#     compose service. `hook_layout` stays at common's default `"pre-push"`:
#     `pre-commit` runs a fmt-only check and `pre-push` runs the full suite.
#   - test_args = "": run the full suite (in `pre-push`, not `pre-commit`,
#     under the default hook_layout). The unit tests live in src/** behind
#     `mod tests` on the library target, which common's default `--bins` skips.
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

# Run the full test suite (in pre-push under the default hook_layout): the
# unit tests are library-target tests in src/**, which common's default
# `--bins` would skip.
test_args := ""

# -- Default (override) -------------------------------------------------------

# List available recipes. Must stay the FIRST recipe in this file: just resolves the default recipe by source order, and a top-level definition outranks the imported one, so whatever appears first here wins over common's `default`.
[group: 'dev']
default:
    @just --list

# -- Checks (override) --------------------------------------------------------

# Umbrella check: CLAUDE.md guard + fmt + clippy + build + tests + builder-stage docker compile. Overrides common's `check`, which omits tests; reuses the inherited `test` recipe so tests run as part of `just check`.
[group: 'check']
check: check-claude-md check-fmt check-clippy check-build test check-docker

# Guard CLAUDE.md: 20,000-byte cap, 400-character lines, resolvable docs/ links (CLAUDE-370). Runs its self-test first.
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

# -- Migrations ---------------------------------------------------------------

# Apply pending sqlx migrations against the local state.db (uses the same XDG paths the binary uses at runtime).
[group: 'migrations']
migrate:
    cargo run -- migrate

# Re-apply legacy state.json import against the SQLite tables (idempotent).
[group: 'migrations']
migrate-revert:
    @echo "no-op: schema-only migrations are forward-only via sqlx::migrate!; use a fresh state.db if you need to revert."
