#!/bin/sh
# Root hook, run by gosu-entrypoint.sh before the chown. CLAUDE_RUN_DATA_DIR
# is not in DATA_DIRS (it holds workdirs/, a tree of job git clones the
# runtime user created itself, which a recursive chown every boot would walk),
# so it gets its own shallow chown here instead of the generic DATA_DIRS loop.
set -eu

if [ -d "${CLAUDE_RUN_DATA_DIR:-}" ]; then
    chown "${RESOLVED_UID}:${RESOLVED_GID}" "${CLAUDE_RUN_DATA_DIR}"
    find "${CLAUDE_RUN_DATA_DIR}" -mindepth 1 -maxdepth 1 ! -name workdirs \
        -exec chown -R "${RESOLVED_UID}:${RESOLVED_GID}" {} +
    if [ -d "${CLAUDE_RUN_DATA_DIR}/workdirs" ]; then
        chown "${RESOLVED_UID}:${RESOLVED_GID}" "${CLAUDE_RUN_DATA_DIR}/workdirs"
    fi
fi
