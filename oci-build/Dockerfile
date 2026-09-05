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
# (nu, claude, yt, fj, timeout, git) are added and pinned via build args so the
# image is reproducible instead of depending on whatever the operator installed.
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

# Non-root container user with a dedicated, clean $HOME. The agent's user memory
# is the slim bot CLAUDE.md, never the operator's interactive one.
RUN addgroup -S claudebot \
    && adduser -S -G claudebot -h /home/claudebot claudebot
COPY deploy/bot-CLAUDE.md /home/claudebot/.claude/CLAUDE.md

# Runtime XDG dirs the compose named volumes / tmpfs back. Keep them under the
# container user's $HOME so the clean-home guarantee holds end to end.
ENV CLAUDE_RUN_CONFIG_DIR=/home/claudebot/.config/claude-run \
    CLAUDE_RUN_DATA_DIR=/home/claudebot/.local/share/claude-run \
    CLAUDE_RUN_STATE_DIR=/home/claudebot/.local/state/claude-run \
    CLAUDE_RUN_RUNTIME_DIR=/home/claudebot/.runtime/claude-run
RUN mkdir -p "${CLAUDE_RUN_CONFIG_DIR}/actions" "${CLAUDE_RUN_DATA_DIR}" \
    "${CLAUDE_RUN_STATE_DIR}" "${CLAUDE_RUN_RUNTIME_DIR}" \
    /home/claudebot/.config/youtrack-cli /home/claudebot/.config/git \
    /home/claudebot/.local/share/forgejo-cli \
    && chown -R claudebot:claudebot /home/claudebot

# CLAUDE-247: the operator logs the three CLIs in INSIDE the container, so git
# needs a global config file on a volume rather than a bind of the host's
# ~/.gitconfig. GIT_CONFIG_GLOBAL names it explicitly instead of relying on the
# XDG fallback.
ENV GIT_CONFIG_GLOBAL=/home/claudebot/.config/git/config

# HOME is set explicitly because the runtime stage has no USER directive (see the
# entrypoint note below): without it a root `docker compose exec` would write to
# /root and the operator's in-container login would land nowhere useful.
ENV HOME=/home/claudebot
WORKDIR /home/claudebot

# CLAUDE-247: the container STARTS as root so the entrypoint can chown the
# mounted volumes, then drops to claudebot with su-exec. This is a deliberate,
# recorded exception to BUILD.md's non-root-runtime mandate (pattern 3 in
# governance/CONTAINER_UID_GID.md); the daemon itself runs non-root. An operator
# `user:` override in compose disables the chown and must not be set.
COPY oci-build/entrypoint.sh /usr/local/bin/entrypoint.sh
RUN chmod 0755 /usr/local/bin/entrypoint.sh

# Foreground daemon (no --detach): claude-run owns the socket under
# CLAUDE_RUN_RUNTIME_DIR exactly as on the host.
ENTRYPOINT ["/usr/local/bin/entrypoint.sh"]
CMD ["serve"]

# Export stage: pull just the binary out, no runtime base.
FROM scratch AS binary
COPY --from=builder /build/target/release/claude-run /claude-run
