# Build a static `claude-run` binary against the musl rust-builder image and
# export it via a scratch stage. No runtime image is published: claude-run is
# a CLI / daemon binary that the user installs alongside host-side `nu` and
# `claude` tools, so a containerized runtime would be the wrong unit of
# distribution.
#
# Targets:
#   --target builder  -> compile only, for `just check-docker`
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

# Real source + migrations + templates + assets. The binary embeds, all at
# compile time: migrations via sqlx::migrate!, the web UI's askama templates
# via #[template(path = ...)], and assets/htmx.min.js via include_bytes!. All
# resolve from these crate-root directories, so each must be present.
COPY src ./src
COPY migrations ./migrations
COPY templates ./templates
COPY assets ./assets
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

# Export stage: pull just the binary out, no runtime base.
FROM scratch AS binary
COPY --from=builder /build/target/release/claude-run /claude-run
