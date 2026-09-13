# Build a static `claude-run` binary against the musl rust-builder image and
# either export it via a scratch stage or assemble a self-contained runtime
# image around it.
#
# The `runtime` stage (CLAUDE-37) exists so the daemon, and every `claude`
# child it spawns, runs inside a container with a clean `$HOME`. The container
# user's `~/.claude/CLAUDE.md` is the slim bot file baked from
# `deploy/bot-CLAUDE.md`, never the operator's interactive global CLAUDE.md, so
# the agent stops inheriting the operator's Git/YouTrack workflow instructions.
# The Generic-Package binary is still published independently for host installs.
#
# Targets:
#   --target builder  -> compile only, for `just check-docker`
#   --target runtime  -> daemon runtime image, used by `compose.yml`
#   --target binary   -> scratch stage, used by `just build-docker-export`
#                        and the build-binary.yml workflow

# syntax=docker/dockerfile:1.6
FROM dev.a8n.run/pandoras-box/rust-builder-musl:v1.0.0-rust1.94-alpine AS builder

WORKDIR /build

# musl-dev: claude-run depends on sqlx-sqlite via rusqlite's bundled sqlite,
# which needs the musl C headers to compile. The base image bundles the
# sqlite-static lib but not musl-dev.
RUN apk add --no-cache musl-dev

ARG CARGO_BUILD_JOBS=2
ENV CARGO_BUILD_JOBS=${CARGO_BUILD_JOBS}
RUN echo "build parallelism: CARGO_BUILD_JOBS=${CARGO_BUILD_JOBS} (host nproc=$(nproc))"

# Prime the dep cache with a dummy lib + bin so cargo builds only the
# dependency graph in this layer. BuildKit cache mounts catch what the layer
# cache misses (eg. a Cargo.toml tweak).
COPY Cargo.toml Cargo.lock ./
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

# Runtime stage (CLAUDE-37): the daemon plus its full runtime toolchain in one
# image. Based on the same musl rust-builder image so cargo/rust and the C
# toolchain are present if a target-repo check needs them; the runtime tools
# (nu, claude, yt, fj, just, kingfisher, timeout, git) are added and pinned via
# build args so the image is reproducible instead of depending on whatever the
# operator installed.
#
# CLAUDE-247: the image is self-contained. It mounts nothing from the host, and
# the operator logs claude / yt / fj / git in INSIDE the container, onto named
# volumes. See README "Containerized deployment".
FROM dev.a8n.run/pandoras-box/rust-builder-musl:v1.0.0-rust1.94-alpine AS runtime

# Version pins. Override at build time with --build-arg to move a single tool.
ARG NU_VERSION=0.112.2
ARG CLAUDE_CODE_VERSION=latest
ARG YT_VERSION=v0.13.0
ARG FJ_VERSION=v0.15.0
# CLAUDE-247: yt and fj come from this org's own Forgejo generic packages, which
# publish prebuilt static binaries. Both are anonymous downloads.
ARG PKG_BASE=https://dev.a8n.run/api/packages/pandoras-box/generic
# CLAUDE-282: just and kingfisher come from the org's Dufs binary mirror. The
# checksum is pinned here rather than read from Dufs' own `?hash` at build
# time, because a hash served by the same host as the file verifies nothing.
ARG BINARIES_BASE=https://files.example.com/public/linux/x86_64
ARG JUST_VERSION=1.57.0
ARG JUST_SHA256=8545d0880e84bbd6c9d6a5dc2681df2a6315455bc2a2f1d7bca52085d6adb8e3
ARG KINGFISHER_VERSION=1.59.0
ARG KINGFISHER_SHA256=ae94fdc3fad5ad18b39c7f3c0742a34b98f7359eea9eaa39c22ecc3129577155

# Runtime dependencies:
#   coreutils -> the `timeout(1)` wall-clock wrapper src/claude.rs delegates to
#   git       -> clone/branch/commit inside each job workdir
#   nodejs/npm-> the `claude` CLI ships as the @anthropic-ai/claude-code package
#   su-exec   -> drop root to claudebot in the entrypoint (CLAUDE-247)
#   curl/tar  -> fetch the pinned nu / yt / fj release binaries
RUN apk add --no-cache coreutils git nodejs npm su-exec curl tar

# nu: pinned musl release tarball (apk's nushell version drifts from the
# 0.112.2 the actions are written against).
RUN curl -fsSL "https://github.com/nushell/nushell/releases/download/${NU_VERSION}/nu-${NU_VERSION}-x86_64-unknown-linux-musl.tar.gz" \
    | tar -xz -C /tmp \
    && install -m 0755 "/tmp/nu-${NU_VERSION}-x86_64-unknown-linux-musl/nu" /usr/local/bin/nu \
    && rm -rf /tmp/nu-* \
    && nu --version

# claude: the Claude Code CLI as a global npm package.
RUN npm install --global "@anthropic-ai/claude-code@${CLAUDE_CODE_VERSION}" \
    && claude --version

# yt: the org's youtrack-cli (pandoras-box/youtrack-cli), a prebuilt static
# binary from the Forgejo generic package registry. NOT the unrelated PyPI
# project of the same name, whose command surface the actions are not written
# against. Verified against the checksum published beside it.
RUN cd /tmp \
    && curl -fsSLO "${PKG_BASE}/youtrack-cli/${YT_VERSION}/yt-linux-x86_64" \
    && curl -fsSLO "${PKG_BASE}/youtrack-cli/${YT_VERSION}/yt-linux-x86_64.sha256" \
    && sha256sum -c yt-linux-x86_64.sha256 \
    && install -m 0755 yt-linux-x86_64 /usr/local/bin/yt \
    && rm -f yt-linux-x86_64 yt-linux-x86_64.sha256 \
    && yt --version

# fj: forgejo-cli (pandoras-box/forgejo-cli), the musl-static build from the same
# registry. Replaces a `cargo install` that recompiled the whole crate on every
# image build. The smoke check is `--help`: fj has no `--version` flag.
RUN cd /tmp \
    && curl -fsSLO "${PKG_BASE}/forgejo-cli/${FJ_VERSION}/fj-linux-x86_64-static" \
    && curl -fsSLO "${PKG_BASE}/forgejo-cli/${FJ_VERSION}/fj-linux-x86_64-static.sha256" \
    && sha256sum -c fj-linux-x86_64-static.sha256 \
    && install -m 0755 fj-linux-x86_64-static /usr/local/bin/fj \
    && rm -f fj-linux-x86_64-static fj-linux-x86_64-static.sha256 \
    && fj --help >/dev/null

# just: the check-suite runner target repos are built against (src/native/youtrack.rs
# step 6, "the repo's 'just check'"), from the org's Dufs binary mirror
# (NiceGuyIT/binaries). Static musl binary, verified against its pinned checksum.
RUN cd /tmp \
    && curl -fsSLo just "${BINARIES_BASE}/just/${JUST_VERSION}/just" \
    && echo "${JUST_SHA256}  just" | sha256sum -c \
    && install -m 0755 just /usr/local/bin/just \
    && rm -f just \
    && just --version

# kingfisher: the secret scanner org repos gate on (a8n-run/docker's
# check-secrets recipe, another repo's justfile), from the same Dufs mirror.
# Static musl binary, verified against its pinned checksum.
RUN cd /tmp \
    && curl -fsSLo kingfisher "${BINARIES_BASE}/kingfisher/${KINGFISHER_VERSION}/kingfisher" \
    && echo "${KINGFISHER_SHA256}  kingfisher" | sha256sum -c \
    && install -m 0755 kingfisher /usr/local/bin/kingfisher \
    && rm -f kingfisher \
    && kingfisher --version

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

# Non-root container user. $HOME is deliberately NOT where anything persists
# (CLAUDE-253): it is scratch, and every store the operator cares about lives
# under the three volumes below.
RUN addgroup -S claudebot \
    && adduser -S -G claudebot -h /home/claudebot claudebot
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
# GIT_CONFIG_GLOBAL. Nothing here changes how paths resolve for a HOST install,
# where none of these variables is set and XDG defaults still apply.
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
    && chown -R claudebot:claudebot /config /data /state /run/claude-run \
    && chown -R claudebot:claudebot /home/claudebot

# HOME is set explicitly because the runtime stage has no USER directive (see the
# entrypoint note below): without it a root `docker compose exec` would write to
# /root. Nothing persists here; it is the scratch home the agent's own tooling
# uses.
ENV HOME=/home/claudebot
WORKDIR /home/claudebot

# CLAUDE-247: the container STARTS as root so the entrypoint can chown the
# mounted volumes, then drops to claudebot with su-exec. This is a deliberate,
# recorded exception to BUILD.md's non-root-runtime mandate (pattern 3 in
# governance/CONTAINER_UID_GID.md); the daemon itself runs non-root. An operator
# `user:` override in compose disables the chown and must not be set.
COPY oci-build/entrypoint.sh /usr/local/bin/entrypoint.sh
RUN chmod 0755 /usr/local/bin/entrypoint.sh

# Foreground daemon (no --detach), and no unix-socket API. That socket is the
# CLI's control plane on a host install; this image has no CLI operator (the web
# UI and its /api routes are the whole interface), so binding it would only leave
# an unauthenticated second copy of the control plane inside the container,
# reachable by the agent the daemon spawns. Override the command to `serve` if a
# deployment really does want it.
ENTRYPOINT ["/usr/local/bin/entrypoint.sh"]
CMD ["serve", "--no-socket"]

# Export stage: pull just the binary out, no runtime base.
FROM scratch AS binary
COPY --from=builder /build/target/release/claude-run /claude-run
