# Self-hosting

## Containerized deployment

Running the daemon on the host means every `claude` child it spawns inherits the operator's `$HOME`, so Claude Code
loads the operator's interactive `~/.claude/CLAUDE.md` (which prescribes a full Git + YouTrack workflow) and contradicts
the local-only agent contract. The container runtime (CLAUDE-37) fixes that: the whole daemon runs in a container with a
clean `$HOME`, so each child inherits a slim, bot-specific user memory instead.

**Host (non-container) deployments (CLAUDE-165).** When the daemon runs directly on the host it now achieves the same
isolation without a clean `$HOME`: at serve start it seeds `[daemon].claude_config_dir` (default
`<CLAUDE_RUN_CONFIG_DIR>/claude-agent-home`) with the embedded slim bot `CLAUDE.md` (the same `deploy/bot-CLAUDE.md` the
container bakes in) and, on Linux, a symlink `<dir>/.credentials.json` to the host login at `~/.claude/.credentials.json`,
then spawns every `claude` child with `CLAUDE_CONFIG_DIR=<dir>`. That relocates where Claude Code reads the user-level
`CLAUDE.md` and credentials, so the child loads the bot memory instead of the operator's global `~/.claude/CLAUDE.md`
while staying authenticated with the shared host login; `HOME` is left inherited so git/ssh keep working, and (unlike
`--bare`) MCP servers, hooks, and skills stay enabled. If the host credential file is absent at serve start the daemon logs a
`warn` naming the path and continues (CLAUDE-247, so a container the operator has not logged into yet still comes up);
run `claude` once to log in, or set `ANTHROPIC_API_KEY`. A regular `.credentials.json` already in the agent config dir
is left alone rather than replaced by the link, because in a container that file is the login itself. On macOS
credentials live in the Keychain rather than `.credentials.json`, so the symlink step is Linux-only.

**Image.** `oci-build/Dockerfile` has a `runtime` target built on `dev.a8n.run/pandoras-box/opensuse-dev:v1.13.0-leap-16.0`
(CLAUDE-287), the org's dev image. It already carries the whole runtime toolchain: `nu`, the `claude` CLI, coreutils
`timeout`, `git`, `yt` (youtrack-cli), `fj` (forgejo-cli), `just`, `kingfisher`, and `docker` (client, Compose, buildx).
Tool versions come from the base image tag; moving a tool means bumping that tag, not editing a Dockerfile `RUN` block.
The runtime stage smoke-checks every one of those tools in a single `RUN`, so a base image that loses a tool fails the
image build instead of the first job. The container runs as the non-root user `claudebot` (uid 100, gid 101, matching
the ids the earlier Alpine-based image used) with `$HOME=/home/claudebot`, which holds nothing persistent: everything
the operator cares about lives on the three flat volumes below.

**Published image (CLAUDE-88).** CI publishes the runtime image to the Forgejo OCI registry on every push to main (
`:latest`) and every release tag (`:vX.Y.Z`), the only published artifact. The image publish runs
in its own workflow, `.forgejo/workflows/build-image.yml`. It is published under the private `psa-systems-private` package owner, so pulling
needs a one-time login with a Forgejo PAT that can read it:

```nu
docker login dev.a8n.run --username <forgejo-user>
docker pull dev.a8n.run/psa-systems-private/claude-run:latest
```

Pin `:vX.Y.Z` in production; `:latest` tracks main. Container deployments update by pulling a newer tag and recreating
the service:

```nu
docker compose pull
docker compose up --detach
```

**Slim bot CLAUDE.md.** `deploy/bot-CLAUDE.md` is versioned in this repo, `COPY`d to
`/home/claudebot/.claude/CLAUDE.md` in the image, and written into the agent's own config dir
(`/config/claude-agent-home/CLAUDE.md`) on every serve start, which is the copy the agent actually reads. It contains only the local-only agent contract (no push, no PR; leave the feature
branch as HEAD; the runner finalizes), the bounded tracker writes from
[What the agent may write to the tracker](agent-writes-and-audits.md#what-the-agent-may-write-to-the-tracker-claude-214-claude-297), the em-dash ban, the
Nushell 0.112.2 note, and the simplest-solution preference. It has no Git workflow and no lifecycle section, so the
agent is never told to push, open PRs, or transition issue state.

**compose.yml.** Consumes the published image (`image: dev.a8n.run/psa-systems-private/claude-run:latest`; the local `build:` block stays as a commented development alternative). App-prefixed names (service `claude-bot`, network `claude-bot-private`, volumes `claude-bot-config`, `claude-bot-data` and `claude-bot-state`, plus `claude-bot-postgres` which only the `postgres` profile mounts; prefix each with `dev-` for a development stack). Each tool's store is a subdirectory of one of the three roots rather than a volume of its own (CLAUDE-253); see the volume table below.

**Self-contained by design (CLAUDE-247, image and isolation superseded by CLAUDE-287).** The container bind-mounts nothing from the host and reads no host credential. The operator logs Claude, YouTrack, Forgejo and git in INSIDE the container once; each login lands in its own subdirectory of the config or data volume (`claude-agent-home/`, `youtrack-cli/` and `git/` under `/config`, `forgejo-cli/` under `/data`, per the volume table below) and survives restarts and image upgrades. Two reasons: the runner's identity stays separate from the operator's, and the stack deploys on a host where neither `$HOME` nor the invocation directory is available to the compose file. The default stack reads no path relative to this repo at all, including the secrets key, which the daemon generates itself; the only repo-relative paths in the file are the two `secrets/` files the `postgres` profile needs. CLAUDE-287 supersedes CLAUDE-247's image decisions (the Alpine base, the per-tool downloads, `su-exec`) and ends the property that the agent has no path to anything privileged (see "Docker access" above); the no-host-mount and in-container-login properties themselves are unchanged.

**Volumes.** Three, flat, named for what they hold:

| Volume              | Container path   | Purpose                                                                                                    |
|---------------------|------------------|------------------------------------------------------------------------------------------------------------|
| `claude-bot-config` | `/config`        | `config.toml`, `actions/`, `secrets.key`, `claude-agent-home/` (the agent's Claude login), `youtrack-cli/` (`yt`), `git/` (commit identity) |
| `claude-bot-data`   | `/data`          | `state.db` (runs, outcomes, the encrypted secrets store; unused on the `postgres` profile), per-job `workdirs/`, `forgejo-cli/` (`fj` keys) |
| `claude-bot-state`  | `/state`         | logs and per-attempt stream JSONL                                                                          |
| tmpfs               | `/run/claude-run`| pidfile; ephemeral so nothing stale survives a restart                                                     |

Each tool's store is a subdirectory rather than a mount of its own, because the image points every tool at these roots: `CLAUDE_RUN_{CONFIG,DATA,STATE,RUNTIME}_DIR` for the runner, `XDG_CONFIG_HOME=/config` for `yt`, `XDG_DATA_HOME=/data` for `fj` (which has no config-dir variable of its own, and which claude-run resolves the same way through `dirs::data_dir()`), `CLAUDE_CONFIG_DIR=/config/claude-agent-home` for `claude`, and `GIT_CONFIG_GLOBAL=/config/git/config` for git. `$HOME` holds nothing persistent; it is scratch.

`CLAUDE_CONFIG_DIR` deliberately names the same directory the daemon seeds and points its agent children at, so a `claude auth login` run through `docker compose exec` writes the credential where the agent reads it rather than into a second Claude config dir that looks fine and authenticates nothing.

**CI images (CLAUDE-383).** `compose.yml` also sets seven `RUNS_ON_*` variables on `claude-bot`, one per image CI uses (`RUNS_ON_OPENSUSE_BASE_HEAVY`, `RUNS_ON_OPENSUSE_BASE_MASSIVE`, `RUNS_ON_OPENSUSE_BASE_LIGHT`, `RUNS_ON_OPENSUSE_BASE_MEDIUM`, `RUNS_ON_RUST_BUILDER_MUSL`, `RUNS_ON_RUST_BUILDER_GLIBC`, `RUNS_ON_RUST_BUILDER_GLIBC_WINDOWS`). A workflow's `runs-on: ${{ vars.RUNS_ON_* }}` resolves the same name to a runner LABEL; the container's environment variable of the same name is the pullable IMAGE that label's runner runs, so an agent session can `docker run "$RUNS_ON_RUST_BUILDER_MUSL"` or reference `${RUNS_ON_RUST_BUILDER_MUSL}` in a compose file and get the exact image CI uses, with nothing from the GitHub Container Registry involved. See `deploy/bot-CLAUDE.md`'s "CI images" section for what the agent is told.

**Running user.** The runtime stage stays on `USER root` (the base image's last `USER` is `dev`) so `oci-build/entrypoint.sh` can chown the mounted volumes to `claudebot`, then drops to that user with `setpriv --reuid=claudebot --regid=claudebot --init-groups` (pattern 3 in `governance/CONTAINER_UID_GID.md`); Leap packages neither `su-exec` nor `gosu`, and `setpriv` (util-linux, already in the base) is the equivalent gosu's own README names. The daemon itself runs non-root. `catatonit` (already in the base image) is PID 1 and execs `entrypoint.sh` as its child, so any process an agent session leaves running when its own session exits is reaped instead of accumulating as a zombie for the life of the container (CLAUDE-310). Do NOT set `user:` in compose: the entrypoint would lose the privileges the chown needs and a volume owned by another uid could never be repaired from inside. Two consequences for the operator:

- `docker compose exec` lands as root unless you pass `--user claudebot`. Always pass it for the login commands below, or the credentials are written root-owned and only become usable after the next restart's chown.
- The chown skips `$CLAUDE_RUN_DATA_DIR/workdirs`, which is a tree of job git clones the runtime user created itself; walking it on every boot would be slow for no gain.

**Docker access (CLAUDE-287).** Agent sessions run against a Docker-in-Docker sidecar, `claude-bot-docker`
(`docker.io/docker:29.6.2-dind`, the same pin the org's Forgejo runners use), rather than the host's own daemon: a
mounted host socket resolves bind paths against the host filesystem, where the volume in `/data/workdirs/...` does not
exist. The sidecar shares `claude-bot`'s network namespace (`network_mode: service:claude-bot`) and listens on
`tcp://127.0.0.1:2375` with no TLS, since nothing outside that shared namespace can reach it; `claude-bot` sets
`DOCKER_HOST` to that address, and every `claude` child inherits it (nothing in `src/` calls `env_clear` or
`env_remove`). A port a job publishes through that daemon answers on `claude-bot`'s own `127.0.0.1`. The sidecar mounts
the same `claude-bot-data` volume at `/data` that `claude-bot` does, which is what makes a bind like
`docker run --volume /data/workdirs/<job>:/build ...` from inside a job's workdir resolve to the right files; its own
Docker state lives on a separate named volume, `claude-bot-docker` at `/var/lib/docker`, so its growth is visible and
prunable:

```nu
docker compose exec --env DOCKER_HOST=tcp://127.0.0.1:2375 claude-bot-docker docker system df
docker compose exec --env DOCKER_HOST=tcp://127.0.0.1:2375 claude-bot-docker docker system prune
```

The `--env` override is required because `-H` in the sidecar's own `dockerd` command replaces its default unix socket.
There is no `depends_on` between the two services (the sidecar joins `claude-bot`'s namespace, so the dependency would
be a cycle); a job that runs `docker` before the sidecar's healthcheck passes simply fails with the CLI's own
connection error in that job's log. This ends the property CLAUDE-247 established that the agent has no path to
anything privileged: the agent now drives a privileged Docker daemon, which is root-equivalent on the host if a
container escapes it. That trade was accepted when CLAUDE-287 was filed; the container still bind-mounts nothing from
the host, and logins still happen inside the container.

**Actions.** The `./actions` bind is gone with the other host mounts. The image bakes this repo's action YAML at `/usr/share/claude-run/actions`, and the entrypoint copies it into the config volume the first time that volume holds no `*.yml`, so a fresh container starts with the shipped action set. It never overwrites after that: an action edited in the web UI survives an image upgrade, and a changed shipped action reaches an existing deployment through the same editor (`/actions`), not by recreating the container.

**No unix-socket API.** The image runs `serve --no-socket`, so the daemon binds only the TCP listener. That socket is the CLI's control plane, and this container has no CLI operator: the web UI and its `/api` routes are the whole interface, and they carry the same handlers (`/api/actions/<n>/trigger`, `/api/pause`, `/api/resume`, `/settings/<section>`). Binding it here would only leave a second copy of that control plane with no token gate on it, reachable by the `claude` agent the daemon spawns as the same user. `claude-run trigger` and `claude-run cancel` inside the container therefore fail, naming the reason rather than claiming no daemon is running; every other subcommand (`secret set`, `action list`, `status`) works as before because it goes to the database directly. Override `command: ["serve"]` in compose if a deployment really does want the socket.

**Web bind and first-run setup.** The image seeds a bootstrap `config.toml` (from `deploy/container-config.toml`) that binds `0.0.0.0:7321`, because a loopback bind inside a container is reachable from nothing outside it, published port or reverse proxy included. A non-loopback bind requires auth, and that credential is created in the browser: open the URL and the daemon serves `/setup`, which takes a username and a password and creates the first account. No token, no key file, no CLI. Every account after that comes from an invite minted on the Accounts page, never from `/setup` again. The seeded `config.toml` is copied only when the config volume has none, so operator edits survive restarts and image upgrades. See [Web UI / API auth](auth.md#web-ui--api-auth) for what setup is and is not (it is trust-on-first-use; bring the instance up behind the proxy).

**First-run logins.** Everything is in the browser: after creating the first account, open **Settings -> Auth**, press **Sign in with Anthropic** (or store an API key), and enter the YouTrack base URL plus token, the Forgejo host plus token, and the GitHub host plus token. No restart, and no CLI. Until the Claude credential exists the scheduler runs nothing and says so on the dashboard. Git commit identity (`user.name` / `user.email` for the commits the agent makes inside a job's clone) is on the same page: leave it unset and the daemon defaults it to `claude-run <claude-run@a8n.run>`, or type your own name and email to override it, materialized to the path `GIT_CONFIG_GLOBAL` names with no `exec` step required (CLAUDE-358). The `exec` route below remains for operators who would rather the CLIs own their own config. The daemon comes up before any login exists (a missing Claude credential is a `warn`, not a startup failure), so the container is there to exec into:

```nu
docker compose up --detach

# Create the first web account at https://<your-host>/setup, then the tool logins:
docker compose exec --user claudebot claude-bot claude auth login
docker compose exec --user claudebot claude-bot yt auth login --base-url https://yt.example.com
docker compose exec --user claudebot claude-bot fj auth add-key
docker compose exec --user claudebot claude-bot gh auth login

docker compose restart
```

`claude auth login` needs the interactive TTY that `docker compose exec` provides; `claude setup-token` is the long-lived-token alternative. Verify with `claude auth status`, `yt auth list`, `fj auth list` and `gh auth status` through the same `exec --user claudebot` form.

To use an API key instead of an interactive Claude login, set `ANTHROPIC_API_KEY` in the service `environment:` and skip `claude auth login`. Nothing else changes: since CLAUDE-247 the daemon no longer requires a credentials file to start.

**Secrets.** The store itself (web auth token, webhook HMACs, token refs) lives encrypted in the state store,
`state.db` on the data volume or the `secrets` table on a PostgreSQL backend. The age identity that decrypts it needs no
injection: the daemon generates it at `/config/secrets.key` (mode `0600`) on the `claude-bot-config` volume the first
time it starts without one, so there is no `age-keygen` step before first start and the shipped `compose.yml` declares
no age-key secret. **Back that key up separately**, off the volume: every stored secret is unrecoverable without it, and
a key the operator never created by hand is easier to lose than one they did. Generation happens only while the store is
empty; a missing key with secrets already in the database is a hard error naming them rather than a silent replacement
that would orphan every one (see [Secrets store](auth.md#secrets-store)).

`CLAUDE_RUN_SECRETS_KEY_FILE` is the bring-your-own-key path, for a key that comes from your own secret store or has to
be shared with another instance. Its value is a PATH, never the key itself: key material in a plain environment variable
is deliberately unsupported (visible to `docker inspect` and `/proc/<pid>/environ`), so deliver the file with a bind
mount or a compose `secrets:` entry you add yourself, and set the variable to where it lands.
`ANTHROPIC_API_KEY` is the one secret that MAY go through plain env, as the Claude-auth alternative above.

```yaml
services:
  claude-bot:
    environment:
      CLAUDE_RUN_SECRETS_KEY_FILE: /run/secrets/claude-run-age-key
    secrets:
      - claude-run-age-key

secrets:
  claude-run-age-key:
    file: ./secrets/age-key.txt
```

A deployment that cannot use a path relative to the compose file (one whose config is piped through `docker compose --file -`, where relative paths resolve against the caller's working directory) declares that same secret from an environment value instead, and supplies the value from its own secret store. The key still reaches the container as a file at `/run/secrets/claude-run-age-key`; only the source changes:

```yaml
secrets:
  claude-run-age-key:
    environment: CLAUDE_RUN_AGE_KEY
```

**Networking.** The seeded `config.toml` already binds `0.0.0.0:7321`; publish the port (`ports:` mapping or a
reverse-proxy network) to reach it. A non-loopback bind requires auth, which the first-run `/setup` page creates. A
bearer token for API clients stays optional and separate (`web.auth_token_ref`, see [Web UI / API auth](auth.md#web-ui--api-auth)).

```nu
docker compose up --detach      # start the daemon from the published image
docker compose logs --follow    # watch the scheduler
