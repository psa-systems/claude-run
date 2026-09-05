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
for dir in \
    "${CLAUDE_RUN_CONFIG_DIR}" \
    "${CLAUDE_RUN_STATE_DIR}" \
    "${CLAUDE_RUN_RUNTIME_DIR}" \
    "${HOME}/.claude" \
    "${HOME}/.config/youtrack-cli" \
    "${HOME}/.config/git" \
    "${HOME}/.local/share/forgejo-cli"; do
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

# su-exec sets HOME from the target user's passwd entry, so the dropped process
# resolves $HOME/.claude, $HOME/.config and $HOME/.local/share correctly.
exec su-exec claudebot /usr/local/bin/claude-run "$@"
