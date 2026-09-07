#!/bin/sh
# CLAUDE-247: pattern 3 from governance/CONTAINER_UID_GID.md. The container
# starts as root so this script can chown the mounted volumes to the runtime
# user, then drops to claudebot via su-exec. Starting as root is a deliberate,
# recorded exception to BUILD.md's non-root-runtime mandate: the workload itself
# still runs non-root. Do NOT set `user:` in compose, or the chown never happens.
#
# Why chown at all when every mount is a named volume: Docker seeds an EMPTY
# named volume with the image mountpoint's ownership, but a volume the operator
# provisioned ahead of time, or one another uid has written to, arrives owned by
# someone else and the container cannot recover from inside.
set -e

# First-run config seed: the image ships a bootstrap config.toml that binds
# 0.0.0.0 (a loopback bind is unreachable from outside the container, so a
# reverse proxy could never see the web UI). Copied only when the volume has
# none, so an operator edit survives every restart and image upgrade.
if [ -f /usr/share/claude-run/config.toml ] && [ -n "${CLAUDE_RUN_CONFIG_DIR}" ]; then
    mkdir -p "${CLAUDE_RUN_CONFIG_DIR}"
    if [ ! -f "${CLAUDE_RUN_CONFIG_DIR}/config.toml" ]; then
        cp /usr/share/claude-run/config.toml "${CLAUDE_RUN_CONFIG_DIR}/config.toml"
        echo "entrypoint: seeded config.toml (web bind 0.0.0.0) into an empty config volume"
    fi
fi

# First-run action seed: the config volume comes up empty, and with no host bind
# supplying action YAML the daemon would scan an empty dir. Copy the baked set in
# only when nothing is there, so operator edits are never overwritten.
if [ -d /usr/share/claude-run/actions ] && [ -d "${CLAUDE_RUN_CONFIG_DIR}" ]; then
    mkdir -p "${CLAUDE_RUN_CONFIG_DIR}/actions"
    if [ -z "$(find "${CLAUDE_RUN_CONFIG_DIR}/actions" -maxdepth 1 -name '*.yml' -print -quit)" ]; then
        cp /usr/share/claude-run/actions/*.yml "${CLAUDE_RUN_CONFIG_DIR}/actions/"
        echo "entrypoint: seeded $(ls -1 "${CLAUDE_RUN_CONFIG_DIR}/actions" | wc -l) action files into an empty config volume"
    fi
fi

# Credential and config stores, plus the runner's own state. $CLAUDE_RUN_DATA_DIR
# is handled separately below.
# CLAUDE-253: three flat roots, so the tool stores (yt under $XDG_CONFIG_HOME,
# fj under $XDG_DATA_HOME, git, the agent config dir) are subdirectories that
# come along with them rather than mount points of their own.
for dir in \
    "${CLAUDE_RUN_CONFIG_DIR}" \
    "${CLAUDE_RUN_STATE_DIR}" \
    "${CLAUDE_RUN_RUNTIME_DIR}"; do
    if [ -d "${dir}" ]; then
        chown -R claudebot:claudebot "${dir}"
    fi
done

# The data dir holds state.db next to workdirs/, which is a tree of job git
# clones the runtime user created itself. Chowning it every boot would walk every
# clone, so take the dir and its non-workdir entries only.
if [ -d "${CLAUDE_RUN_DATA_DIR}" ]; then
    chown claudebot:claudebot "${CLAUDE_RUN_DATA_DIR}"
    find "${CLAUDE_RUN_DATA_DIR}" -mindepth 1 -maxdepth 1 ! -name workdirs \
        -exec chown -R claudebot:claudebot {} +
    if [ -d "${CLAUDE_RUN_DATA_DIR}/workdirs" ]; then
        chown claudebot:claudebot "${CLAUDE_RUN_DATA_DIR}/workdirs"
    fi
fi

# su-exec sets HOME from the target user's passwd entry. Nothing persistent hangs
# off $HOME any more (CLAUDE-253): the stores are named by CLAUDE_RUN_*_DIR,
# XDG_CONFIG_HOME, XDG_DATA_HOME, CLAUDE_CONFIG_DIR and GIT_CONFIG_GLOBAL, all
# set in the image and all inherited by the dropped process.
exec su-exec claudebot /usr/local/bin/claude-run "$@"
