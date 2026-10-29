# Weekly release notes into Mirzam (CLAUDE-474)

`actions/release-notes.yml` turns the week's releases into a draft newsletter. Every Monday at 09:00 it reads the releases published in the last 7 days in `psa-systems/bunyip` and `psa-systems/mirzam` through the Forgejo API (with the token fj already holds for `dev.a8n.run`), has Claude write one customer-facing edition from them, and creates it as a **draft** on a Mirzam list with `POST /api/v1/editions`. It never sends or schedules. A week without releases runs nothing; a week with nothing customer-facing creates no draft.

## Setup

1. In Mirzam, on the target list (default `product-updates`), switch on "Editions need approval": Mirzam then refuses any send or schedule, from the pages or the API, until an administrator approves.
2. In Mirzam admin, API, create a read and write token named `claude-run`.
3. Give the runner `MIRZAM_URL` (the site's base URL), `MIRZAM_API_TOKEN` and optionally `MIRZAM_LIST` in its environment (the container's `environment:` block, the token from a secret). Scripted actions see only the runner's environment; nothing is injected.
4. Copy the file into the actions directory and try it:

```nushell
docker compose exec --user appuser claude-bot claude-run trigger release-notes
```

## One draft per week

Each ISO week is one job (`dedup_key: week`) with `max_attempts_per_key: 1`, so a second trigger in the same week does nothing, a failure included. To redraft a week, reset it and trigger again:

```nushell
docker compose exec --user appuser claude-bot claude-run action reset-attempts release-notes 2026-W44
```

The draft then waits in Mirzam, Editions, for an editor to read, ask for review and an administrator to approve.
