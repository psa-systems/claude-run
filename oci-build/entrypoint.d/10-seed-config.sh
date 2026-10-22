#!/bin/sh
# Root hook, run by gosu-entrypoint.sh before the chown. Seeds the config
# volume on its first boot; never overwrites an operator edit afterwards.
set -eu

# The image ships a bootstrap config.toml that binds 0.0.0.0 (a loopback bind
# is unreachable from outside the container, so a reverse proxy could never
# see the web UI). Copied only when the volume has none, so an operator edit
# survives every restart and image upgrade.
if [ -f /usr/share/claude-run/config.toml ] && [ -n "${CLAUDE_RUN_CONFIG_DIR:-}" ]; then
    mkdir -p "${CLAUDE_RUN_CONFIG_DIR}"
    if [ ! -f "${CLAUDE_RUN_CONFIG_DIR}/config.toml" ]; then
        cp /usr/share/claude-run/config.toml "${CLAUDE_RUN_CONFIG_DIR}/config.toml"
        echo "entrypoint: seeded config.toml (web bind 0.0.0.0) into an empty config volume" >&2
    fi
fi

# The config volume comes up empty, and with no host bind supplying action
# YAML the daemon would scan an empty dir. Copy the baked set in only when
# nothing is there, so operator edits are never overwritten.
if [ -d /usr/share/claude-run/actions ] && [ -d "${CLAUDE_RUN_CONFIG_DIR:-}" ]; then
    mkdir -p "${CLAUDE_RUN_CONFIG_DIR}/actions"
    if [ -z "$(find "${CLAUDE_RUN_CONFIG_DIR}/actions" -maxdepth 1 -name '*.yml' -print -quit)" ]; then
        cp /usr/share/claude-run/actions/*.yml "${CLAUDE_RUN_CONFIG_DIR}/actions/"
        echo "entrypoint: seeded $(ls -1 "${CLAUDE_RUN_CONFIG_DIR}/actions" | wc -l) action files into an empty config volume" >&2
    fi
fi
