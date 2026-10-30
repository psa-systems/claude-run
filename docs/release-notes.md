# Weekly release notes into Mirzam (CLAUDE-474)

`actions/release-notes.yml` turns the week's releases into a draft newsletter. Every Monday at 09:00 it reads the releases published in the last 7 days in `psa-systems/bunyip`, `mokosh-apps`, `mokosh-server`, `lets-chat` and `mirzam` through the Forgejo API (with the token fj already holds for `dev.a8n.run`), has Claude write one customer-facing edition from them, and creates it as a **draft** on a Mirzam list with `POST /api/v1/editions`. It never sends or schedules. A week without releases runs nothing; a week with nothing customer-facing creates no draft.

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

## A failed week carries over

The input looks back 7 days, or to the last draft or skip for this Mirzam site if that is earlier. The post step records that time in `$CLAUDE_RUN_DATA_DIR/release-notes/<host>.last-draft`, so a week that failed leaves its releases for the next draft. The file is per site, so a run against staging does not move production's window.

## Products

Each repo in the action's list carries the product customers know it as, and the edition has one section per product. `mokosh-apps` and `mokosh-server` are both **Mokosh**: a change is described once however many repos it touched, and a server change with no visible effect is dropped.

A release is not always live on production when the draft is written, so the reviewer checks that the changes in a draft are deployed before approving it.

## What the draft leaves out

The prompt keeps only what a subscribed customer would notice and care about. It leaves out internal work, consent and mailing mechanics, signup promotion, fixes that make readers doubt past behaviour, and anything the changelog does not show from a customer's side. One or two items is a normal edition.
