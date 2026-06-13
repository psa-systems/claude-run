# claude-run - Task Runner.
#
# Conventions per `niceguyit/governance` CHECKLIST.md. Recipes are grouped
# (`just --list` clusters them by `[group: '...']`) and use the rust-builder
# image both for the pre-commit hook and for `check-docker`, so the
# toolchain matches Forgejo CI.

dev_image := "dev.a8n.run/pandoras-box/rust-builder-musl:v1.0.0-rust1.94-alpine"

default:
    @just --list

# ── Hooks ────────────────────────────────────────────────────────────────────

# Install the git pre-commit hook (run once per fresh clone). Writes a stub at .git/hooks/pre-commit that execs `just pre-commit`. Bypass with `git commit --no-verify`.
[group: 'hooks']
install-hooks:
    #!/usr/bin/env nu
    let hook = ".git/hooks/pre-commit"
    try { rm $hook }
    "#!/usr/bin/env sh\nexec just pre-commit\n" | save $hook
    ^chmod +x $hook
    print $"Wrote ($hook) -> just pre-commit"

# Run the same checks as .forgejo/workflows/check.yml inside the rust-builder-musl image so the toolchain matches CI.
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
        --volume $"($vol_target):/build/target"
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

# ── Development ──────────────────────────────────────────────────────────────

# Run the CLI with arguments (e.g. just run status, just run trigger youtrack).
[group: 'dev']
run *args:
    cargo run -- {{ args }}

# cargo-watch loop for continuous compilation.
[group: 'dev']
watch:
    cargo watch --clear

# ── Checks ───────────────────────────────────────────────────────────────────

# Umbrella check: fmt + clippy + build + tests + builder-stage docker compile.
[group: 'checks']
check: check-fmt check-clippy check-build check-test check-docker

# Cargo fmt in check mode.
[group: 'checks']
check-fmt:
    cargo fmt --check

# Cargo clippy with warnings denied.
[group: 'checks']
check-clippy:
    cargo clippy --all-targets -- -D warnings

# Cargo build --all-targets (matches check.yml).
[group: 'checks']
check-build:
    cargo build --all-targets

# Cargo test (matches check.yml "Unit tests" step).
[group: 'checks']
check-test:
    cargo test

# Builder-stage docker compile (no final image produced).
[group: 'checks']
check-docker:
    docker buildx build --target builder -t claude-run:check -f oci-build/Dockerfile .

# Cargo type-check, no codegen.
[group: 'checks']
typecheck:
    cargo check --all-targets

# ── Single-step shortcuts ────────────────────────────────────────────────────

# Cargo fmt in write mode.
[group: 'single-step']
fmt:
    cargo fmt

# Alias for check-clippy.
[group: 'single-step']
lint: check-clippy

# Run tests.
[group: 'single-step']
test:
    cargo test

# ── Build ────────────────────────────────────────────────────────────────────

# Build release binary (debug profile is just `cargo build`).
[group: 'build']
build:
    cargo build --release

# Build the static musl binary through the Docker builder image (delegates to build-docker-export). Writes ./output/claude-run; no host musl target or linker required.
[group: 'build']
build-static: build-docker-export

# Build the full OCI build (builder stage in cache, no runtime image since this repo's Dockerfile has no runtime stage).
[group: 'build']
build-docker:
    docker buildx build -t claude-run:local -f oci-build/Dockerfile .

# Extract the static binary out of the OCI build via the `binary` scratch stage. Writes ./output/claude-run.
[group: 'build']
build-docker-export:
    #!/usr/bin/env nu
    mkdir output
    ^docker buildx build --target binary --output type=local,dest=./output -f oci-build/Dockerfile .
    print $"Wrote ./output/claude-run"

# ── Migrations ───────────────────────────────────────────────────────────────

# Apply pending sqlx migrations against the local state.db (uses the same XDG paths the binary uses at runtime).
[group: 'migrations']
migrate:
    cargo run -- migrate

# Re-apply legacy state.json import against the SQLite tables (idempotent).
[group: 'migrations']
migrate-revert:
    @echo "no-op: schema-only migrations are forward-only via sqlx::migrate!; use a fresh state.db if you need to revert."

# ── Release ──────────────────────────────────────────────────────────────────

# Create a release: bump major (vx.0.0), minor (v0.x.0), or hotfix (v0.0.x), push branch, and open the PR via fj. After the PR merges, .forgejo/workflows/create-release.yml tags and publishes.
[group: 'release']
create-release bump:
    #!/usr/bin/env nu
    let bump = "{{ bump }}"

    # Abort if there are uncommitted changes
    let status = git status --porcelain | str trim
    if ($status | is-not-empty) {
        print $"(ansi red)Working tree is dirty. Please stash or commit your changes first.(ansi reset)"
        exit 1
    }

    # Switch to main if not already there
    let branch = git branch --show-current | str trim
    if $branch != "main" {
        print $"Switching from ($branch) to main..."
        git checkout main
    }

    # Pull latest changes
    git pull --rebase origin main

    # Calculate next version
    let current = (open Cargo.toml | get package.version | split row "." | each { into int })
    let next = match $bump {
        "major" => [$"($current.0 + 1)" "0" "0"],
        "minor" => [$"($current.0)" $"($current.1 + 1)" "0"],
        "hotfix" => [$"($current.0)" $"($current.1)" $"($current.2 + 1)"],
        _ => { print $"(ansi red)Usage: just create-release <major|minor|hotfix>(ansi reset)"; exit 1 }
    }
    let bare = ($next | str join ".")
    let tag = $"v($bare)"
    let release_branch = $"release/($tag)"

    # Create release branch, bump version, and commit
    git checkout -b $release_branch
    open Cargo.toml | update package.version $bare | to toml | collect | save --force Cargo.toml
    ^cargo update --package claude-run --precise $bare
    git add Cargo.toml Cargo.lock
    git commit --signoff --message $"Release ($tag)"

    # Push release branch
    git push --set-upstream origin $release_branch

    # Open the release PR via fj. Body lives in a tempfile so a future
    # changelog can grow without inline escaping pain.
    let body_file = (mktemp --tmpdir --suffix .md)
    [
        $"Automated release PR for ($tag)."
        ""
        $"After merge, `.forgejo/workflows/create-release.yml` tags and publishes ($tag) to the Generic Packages registry."
    ] | str join "\n" | save --force $body_file
    let fj_result = (^fj --host dev.a8n.run pr create $"Release ($tag)" --body-file $body_file | complete)
    rm $body_file
    if $fj_result.exit_code != 0 {
        print $"(ansi red)fj pr create failed(ansi reset)"
        print $fj_result.stderr
        exit 1
    }

    let pr_num = (
        $fj_result.stdout
        | str trim
        | parse --regex 'created pull request #(?P<num>\d+)'
        | get num.0?
    )
    let remote = (git remote get-url origin | str trim)
    let base_url = if ($remote | str starts-with "ssh://") {
        $remote | str replace "ssh://git@" "https://" | str replace "git.a8n.run" "dev.a8n.run" | str replace ".git" ""
    } else {
        $remote | str replace --regex "git@([^:]+):" "https://$1/" | str replace "git.a8n.run" "dev.a8n.run" | str replace ".git" ""
    }
    print $"(ansi green)Pushed ($release_branch)(ansi reset)"
    if ($pr_num | is-not-empty) {
        print $"PR: ($base_url)/pulls/($pr_num)"
    } else {
        print $"fj output: ($fj_result.stdout | str trim)"
    }
    print $"After merging, the create-release workflow will tag and release ($tag) automatically."
