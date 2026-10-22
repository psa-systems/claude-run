# Build a static `claude-run` binary against the musl rust-builder image and
# assemble a self-contained runtime image around it. The image is the only
# supported distribution; no standalone binary is published.
#
# The `runtime` stage (CLAUDE-37) exists so the daemon, and every `claude`
# child it spawns, runs inside a container with a clean `$HOME`. The container
# user's `~/.claude/CLAUDE.md` is the slim bot file baked from
# `deploy/bot-CLAUDE.md`, never the operator's interactive global CLAUDE.md, so
# the agent stops inheriting the operator's Git/YouTrack workflow instructions.
#
# Targets:
#   --target builder  -> compile only, for `just check-docker`
#   --target runtime  -> daemon runtime image, used by `compose.yml`

# syntax=docker/dockerfile:1.6
FROM dev.a8n.run/pandoras-box/rust-builder-musl:v1.2.0-rust1.98.1-alpine AS builder

WORKDIR /build

# musl-dev: claude-run depends on sqlx-sqlite via rusqlite's bundled sqlite,
# which needs the musl C headers to compile. The base image bundles the
# sqlite-static lib but not musl-dev.
RUN apk add --no-cache musl-dev

# The runner or caller owns parallelism; `default` is cargo's own CPU count (DEV-769).
ARG CARGO_BUILD_JOBS=default
ENV CARGO_BUILD_JOBS=${CARGO_BUILD_JOBS}
RUN echo "build parallelism: CARGO_BUILD_JOBS=${CARGO_BUILD_JOBS}"

# Prime the dep cache with a dummy lib + bin so cargo builds only the
# dependency graph in this layer. BuildKit cache mounts catch what the layer
# cache misses (eg. a Cargo.toml tweak).
COPY Cargo.toml Cargo.lock ./
# .cargo/config.toml defines the pandoras-box-cargo registry alias mcp-web
# resolves through; cargo needs it before the dependency-only build below can
# fetch that crate.
COPY .cargo ./.cargo
RUN mkdir -p src/bin \
    && echo "pub fn placeholder() {}" > src/lib.rs \
    && echo "fn main() {}" > src/bin/claude-run.rs
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/usr/local/cargo/git \
    cargo build --release --locked \
    && rm -rf src \
       target/release/deps/libclaude_run* \
       target/release/deps/claude_run* \
       target/release/deps/claude-run* \
       target/release/.fingerprint/claude_run-* \
       target/release/.fingerprint/claude-run-*

# Real source + migrations + templates + assets + deploy. The binary embeds,
# all at compile time: migrations via sqlx::migrate!, the web UI's askama
# templates via #[template(path = ...)], assets/htmx.min.js via include_bytes!,
# and deploy/bot-CLAUDE.md via include_str! (src/agent_home.rs). All resolve
# from these crate-root directories, so each must be present.
COPY src ./src
COPY migrations ./migrations
COPY templates ./templates
COPY assets ./assets
COPY deploy ./deploy
COPY build.rs ./

# Bake build metadata into the binary. build.rs reads these env vars; the
# workflow passes the real values via --build-arg (CLAUDE-2).
ARG GIT_SHA=unknown
ARG BUILD_DATE=unknown
ENV CLAUDE_RUN_GIT_SHA=${GIT_SHA}
ENV CLAUDE_RUN_BUILD_DATE=${BUILD_DATE}
RUN echo "CLAUDE_RUN_GIT_SHA=${CLAUDE_RUN_GIT_SHA} CLAUDE_RUN_BUILD_DATE=${CLAUDE_RUN_BUILD_DATE}"

RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/usr/local/cargo/git \
    cargo build --release --locked

# Runtime stage (CLAUDE-37, rebased onto opensuse-dev in CLAUDE-287): the daemon
# plus its full runtime toolchain in one image. The base carries the whole
# toolchain already (nu, claude, yt, fj, gh, just, kingfisher, docker, git,
# coreutils, cargo/rustc), so this stage only creates the runtime user, copies
# the binary and shipped config, and smoke-checks the tools it did not install.
# Moving a tool means bumping the base image tag, not editing a RUN block here.
#
# CLAUDE-287: the base also carries Docker, so `claude` sessions the daemon
# spawns can run `docker`, `docker compose` and `docker buildx` against the
# `claude-bot-docker` sidecar in compose.yml (DOCKER_HOST=tcp://127.0.0.1:2375).
# That is what makes `just check` (ending in `check-docker`) and
# `just pre-commit` (docker-mode) work inside this container.
#
# CLAUDE-247: the image is self-contained. It mounts nothing from the host, and
# the operator logs claude / yt / fj / git in INSIDE the container, onto named
# volumes. See docs/self-hosting.md.
#
# CLAUDE-417: a `docker buildx build` run from this image against the
# claude-bot-docker sidecar can have its build session killed mid-build by the
# sidecar's own BuildKit session healthcheck ("healthcheck failed fatally",
# "only one connection allowed"). That is an upstream Docker 29.x defect in the
# sidecar's dockerd, not something this image or compose.yml can avoid; see
# docs/conventions/container.md.
FROM dev.a8n.run/pandoras-box/opensuse-dev:v1.14.0-leap-16.0 AS runtime

USER root

# Smoke-check the toolchain the base image is expected to carry. A base that
# loses a tool fails the image build here instead of the first job.
RUN nu --version \
    && claude --version \
    && yt --version \
    && fj version \
    && fj auth git-credential --help \
    && gh --version \
    && just --version \
    && kingfisher --version \
    && docker --version \
    && docker compose version \
    && docker buildx version \
    && git --version \
    && timeout --version \
    && cargo --version \
    && catatonit -V \
    && usermod --help >/dev/null \
    && groupmod --help >/dev/null

# gosu (CLAUDE-443): Leap has no `gosu` package, so install the upstream
# release binary and verify it against the checksums published in that
# release's SHA256SUMS. `setpriv` stays installed by the base (nothing else
# in this image uses it; it is simply not removed).
ARG GOSU_VERSION=1.19
ARG TARGETARCH
RUN set -eu; \
    case "${TARGETARCH}" in \
        amd64) GOSU_SHA256=52c8749d0142edd234e9d6bd5237dff2d81e71f43537e2f4f66f75dd4b243dd0 ;; \
        arm64) GOSU_SHA256=3a8ef022d82c0bc4a98bcb144e77da714c25fcfa64dccc57f6aba7ae47ff1a44 ;; \
        *) echo "unsupported TARGETARCH: ${TARGETARCH}" >&2; exit 1 ;; \
    esac; \
    curl -fsSL -o /usr/local/bin/gosu "https://github.com/tianon/gosu/releases/download/${GOSU_VERSION}/gosu-${TARGETARCH}"; \
    echo "${GOSU_SHA256}  /usr/local/bin/gosu" | sha256sum -c -; \
    chmod 0755 /usr/local/bin/gosu; \
    gosu --version; \
    gosu nobody true

# claude-run itself, from the builder stage above.
COPY --from=builder /build/target/release/claude-run /usr/local/bin/claude-run

# CLAUDE-247: the shipped action YAML, baked read-only. The entrypoint seeds it
# into the config volume the first time that volume has no actions, so a fresh
# container starts with the actions this repo ships instead of an empty dir (the
# `./actions` bind that used to supply them is gone with the host mounts). It
# never overwrites afterwards, so edits made in the web UI survive an image
# upgrade; a changed shipped action reaches an existing deployment through that
# same editor.
COPY actions/*.yml /usr/share/claude-run/actions/

# Bootstrap config seeded the same way, and for the same reason the actions are:
# the container binds 0.0.0.0 because a loopback bind is reachable from nothing
# outside the container, and the operator owns the file once it lands on the
# volume.
COPY deploy/container-config.toml /usr/share/claude-run/config.toml

# Runtime user (CLAUDE-443): uid/gid 1001, the id every first-party image's
# gosu-entrypoint.sh falls back to when PUID/PGID are unset and the first data
# dir has no owner to adopt. Home directory path kept at /home/claudebot
# (CLAUDE-287's name) even though the user is now `appuser`, so ENV HOME below
# and every store under it keep their paths. $HOME is deliberately NOT where
# anything persists (CLAUDE-253): it is scratch, and every store the operator
# cares about lives under the three volumes below.
RUN groupadd --gid 1001 appuser \
    && useradd --uid 1001 --gid 1001 --create-home --home-dir /home/claudebot appuser
COPY deploy/bot-CLAUDE.md /home/claudebot/.claude/CLAUDE.md

# CLAUDE-253: three flat mounts instead of seven XDG paths under a home
# directory. An operator mounting a volume should not have to know that config
# lives in `.config/claude-run` while credentials split across
# `.config/youtrack-cli` and `.local/share/forgejo-cli`.
#
#   /config  config.toml, actions, secrets.key, the agent config dir, the yt
#            store, the git identity
#   /data    state.db, workdirs, the fj key store
#   /state   logs and per-attempt stream JSONL
#   /run/claude-run  pidfile (tmpfs; the socket is off, see CMD below)
#
# The tool stores follow their own tools rather than being relabelled: `yt`
# reads XDG_CONFIG_HOME, `fj` reads XDG_DATA_HOME (it has no config-dir variable
# of its own, and claude-run resolves its key store the same way through
# `dirs::data_dir()`), `claude` reads CLAUDE_CONFIG_DIR and git reads
# GIT_CONFIG_GLOBAL. Outside the container (tests, local development) none of
# these variables is set and XDG defaults apply.
#
# CLAUDE_CONFIG_DIR names the SAME directory the daemon seeds and points its
# agent children at (`<config dir>/claude-agent-home`, CLAUDE-165), so an
# operator running `claude auth login` through `docker compose exec` writes the
# credential exactly where the agent reads it. Two Claude config dirs is the
# failure this avoids: a login that reports success and leaves the runner still
# unauthenticated.
ENV CLAUDE_RUN_CONFIG_DIR=/config \
    CLAUDE_RUN_DATA_DIR=/data \
    CLAUDE_RUN_STATE_DIR=/state \
    CLAUDE_RUN_RUNTIME_DIR=/run/claude-run \
    XDG_CONFIG_HOME=/config \
    XDG_DATA_HOME=/data \
    CLAUDE_CONFIG_DIR=/config/claude-agent-home \
    GIT_CONFIG_GLOBAL=/config/git/config
RUN mkdir -p /config/actions /config/claude-agent-home /config/git /config/youtrack-cli \
    /data/forgejo-cli /state /run/claude-run \
    && chown -R appuser:appuser /config /data /state /run/claude-run \
    && chown -R appuser:appuser /home/claudebot

# CLAUDE-443: gosu-entrypoint.sh's PUID/PGID remap and chown apply only to
# these three (config, state, runtime dir, in that order); /data is excluded
# because it holds workdirs/, a tree of job git clones the runtime user made
# itself, which a recursive chown every boot would walk (handled instead by
# the 20-data-dir.sh hook's shallow, workdirs-skipping chown).
ENV APP_USER=appuser \
    DATA_DIRS="/config /state /run/claude-run"

# HOME is set explicitly because the runtime stage stays on USER root (see the
# entrypoint note below): without it a root `docker compose exec` would write to
# /root. Nothing persists here; it is the scratch home the agent's own tooling
# uses.
ENV HOME=/home/claudebot
WORKDIR /home/claudebot

# CLAUDE-443: the shared, byte-identical entrypoint (CONTAINER_UID_GID.md).
# The image-specific root steps that used to live in entrypoint.sh (config
# seeding, the data-dir chown) now run as hooks under /etc/entrypoint.d/.
COPY oci-build/gosu-entrypoint.sh /usr/local/bin/gosu-entrypoint.sh
RUN chmod 0755 /usr/local/bin/gosu-entrypoint.sh
COPY oci-build/entrypoint.d/ /etc/entrypoint.d/
RUN chmod 0755 /etc/entrypoint.d/*.sh

# The container starts as root so gosu-entrypoint.sh can run the hooks above
# and chown the mounted volumes, then drops to appuser with gosu. A compose
# `user:` pin disables the remap and chown; see gosu-entrypoint.sh for what it
# requires instead (every DATA_DIRS entry already writable).
#
# catatonit is PID 1 so orphaned processes an agent session leaves behind are
# reaped instead of accumulating as zombies for the life of the container
# (CLAUDE-310); it execs gosu-entrypoint.sh as its only child and forwards
# signals.
#
# Foreground daemon (no --detach), and no unix-socket API. That socket is the
# CLI's control plane; this image has no CLI operator (the web
# UI and its /api routes are the whole interface), so binding it would only leave
# an unauthenticated second copy of the control plane inside the container,
# reachable by the agent the daemon spawns. Override the command to `serve` if a
# deployment really does want it.
ENTRYPOINT ["/usr/bin/catatonit", "--", "/usr/local/bin/gosu-entrypoint.sh", "/usr/local/bin/claude-run"]
CMD ["serve", "--no-socket"]
