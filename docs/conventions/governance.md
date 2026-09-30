# Governance and build

Full text of the `CLAUDE.md` conventions on this topic, moved there verbatim by CLAUDE-370. The index in `CLAUDE.md` links each heading below.

"This file" in the first section means `CLAUDE.md` together with these convention files.

## Governance source of truth

Governance lives at `niceguyit/governance`. `CHECKLIST.md`, `CI.md`, `PRE_COMMIT.md`, `FORGEJO_ORGS.md`, `GIT_WORKFLOW.md` are the source of truth; this file documents only claude-run-specific deviations.

## Runtime images (CLAUDE-37, CLAUDE-287, CLAUDE-94)

Builder image: `dev.a8n.run/pandoras-box/rust-builder-musl:v1.1.0-rust1.98.1-alpine`. The binary still ships as a Generic Package for host installs. There is also a runtime image (CLAUDE-37, rebased onto the org's dev image in CLAUDE-287): `oci-build/Dockerfile --target runtime` is `FROM dev.a8n.run/pandoras-box/opensuse-dev:v1.12-leap-16.0`, which already carries the pinned toolchain (`nu`, `claude`, `yt`, `fj`, `gh`, `just`, `kingfisher`, `timeout`, `git`, `docker`) at the versions the base tag names; moving a tool means bumping that tag, not editing a Dockerfile `RUN` block. CI publishes the built image to the Forgejo OCI registry as `dev.a8n.run/psa-systems/claude-run:{vX.Y.Z,latest}` with the same tag set as the binary, from its own workflow `.forgejo/workflows/build-image.yml` (split out of `build-binary.yml` in CLAUDE-94), and `compose.yml` runs the published image. The container user's `~/.claude/CLAUDE.md` is the slim bot file `deploy/bot-CLAUDE.md`, so the agent never inherits the operator's interactive global CLAUDE.md. See README "Containerized deployment".

## Template env and ports do not apply (CLAUDE-37)

The governance template's `.env.example`, HOST_IP/PORT split, and fixed per-service port assignments do NOT apply. The daemon binds a unix socket by default (not in the container image, which runs `serve --no-socket`); the TCP web UI is reachable on `127.0.0.1:7321` by default (`WebConfig`, `src/config.rs`). The container runtime ships its own `compose.yml` (CLAUDE-37; see the builder-image note above).

## Action schema

Action schema is documented in `actions/SCHEMA.md`. Authoritative; changes here ripple to both the runner and the doc.
