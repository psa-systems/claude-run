# General Task Runner
#
# Most recipes are inherited from the shared psa-systems task runner, vendored
# as the `common` git submodule and imported below. Only the recipes that
# differ for claude-run (a unix-socket daemon plus CLI, not a web service) are
# defined here; a recipe defined here overrides the imported one of the same
# name.
#
# Inherited from common/common.just: default, install-hooks, check-fmt,
# check-clippy, check-build, check-docker, typecheck, lint, fmt, build, test,
# create-release. (Cargo.toml has no [features], so common's `--all-features`
# on clippy/typecheck is a no-op here; package.name == app == claude-run, so
# common's `cargo update --package {{app}}` in create-release resolves.)
#
# common also ships web-service dev recipes (dev-local*, dev-logs, dev-clean*,
# ensure-env) that assume a compose.dev.yml `app` service. claude-run has no
# such service, so those recipes are inapplicable here; splitting them out of
# the shared core so non-web repos stop inheriting them is tracked in DI-13.

# A recipe defined here overrides the imported one of the same name. common
# already sets allow-duplicate-variables (which lets `app` below override its
# placeholder), so only the recipe setting is needed here.
set allow-duplicate-recipes := true

import 'common/common.just'

# Drives image tags ({{app}}:check/:local) and the cargo cache naming in the
# inherited recipes. Overrides common's placeholder default.
app := "claude-run"

# Toolchain image for the local pre-commit hook (matches Forgejo CI).
dev_image := "dev.a8n.run/pandoras-box/rust-builder-musl:v1.0.0-rust1.94-alpine"

# -- Hooks (override) ---------------------------------------------------------

# Run the same checks as .forgejo/workflows/check.yml inside the rust-builder-musl image so the toolchain matches CI. Overrides common's compose.dev.yml-based pre-commit: claude-run has no compose.dev.yml and runs cargo via `docker run` on the builder image.
[group: 'hooks']
pre-commit:
    #!/usr/bin/env nu
    let img = "{{ dev_image }}"
    let vol_target = "dev-claude-run-cargo-target"
    let vol_reg = "dev-claude-run-cargo-registry"
    let docker_args = [
        --rm
        --volume $"($env.PWD):/build"
        --workdir /build
        # Mount the cargo target cache OUTSIDE the /build bind mount and point
        # cargo at it with CARGO_TARGET_DIR. Nesting a named volume at
        # /build/target makes the Docker daemon create a root-owned target/ on
        # the host that needs sudo to remove (DEV-371). The cache still persists
        # in the named volume; only its mount path moves off the bind mount.
        --volume $"($vol_target):/cargo-target"
        --env CARGO_TARGET_DIR=/cargo-target
        --volume $"($vol_reg):/usr/local/cargo/registry"
    ]
    print "\n[pre-commit] cargo fmt --check"
    ^docker run ...$docker_args $img cargo fmt --check
    print "\n[pre-commit] cargo clippy --all-targets -- -D warnings"
    ^docker run ...$docker_args $img cargo clippy --all-targets -- -D warnings
    print "\n[pre-commit] cargo build --all-targets"
    ^docker run ...$docker_args $img cargo build --all-targets
    print "\n[pre-commit] cargo test"
    ^docker run ...$docker_args $img cargo test
    print "\n[pre-commit] all checks passed"

# -- Checks (override) --------------------------------------------------------

# Umbrella check: fmt + clippy + build + tests + builder-stage docker compile. Overrides common's `check`, which omits tests; reuses the inherited `test` recipe so tests run as part of `just check`.
[group: 'checks']
check: check-fmt check-clippy check-build test check-docker

# -- Development --------------------------------------------------------------

# Run the CLI with arguments (e.g. just run status, just run trigger youtrack).
[group: 'dev']
run *args:
    cargo run -- {{ args }}

# cargo-watch loop for continuous compilation.
[group: 'dev']
watch:
    cargo watch --clear

# -- Build (override) ---------------------------------------------------------

# Build the static musl binary through the Docker builder image (delegates to build-docker-export). Writes ./output/claude-run; no host musl target or linker required.
[group: 'build']
build-static: build-docker-export

# Build the full OCI build. Overrides common's build-docker: no build-args and writes a claude-run:local tag from the Dockerfile's final (binary) stage.
[group: 'build']
build-docker:
    docker buildx build -t claude-run:local -f oci-build/Dockerfile .

# Extract the static binary out of the OCI build via the `binary` scratch stage. Writes ./output/claude-run. Overrides common's build-docker-export, which writes ./dist.
[group: 'build']
build-docker-export:
    #!/usr/bin/env nu
    mkdir output
    ^docker buildx build --target binary --output type=local,dest=./output -f oci-build/Dockerfile .
    print $"Wrote ./output/claude-run"

# -- Migrations ---------------------------------------------------------------

# Apply pending sqlx migrations against the local state.db (uses the same XDG paths the binary uses at runtime).
[group: 'migrations']
migrate:
    cargo run -- migrate

# Re-apply legacy state.json import against the SQLite tables (idempotent).
[group: 'migrations']
migrate-revert:
    @echo "no-op: schema-only migrations are forward-only via sqlx::migrate!; use a fresh state.db if you need to revert."
