# Example action: weekly release notes into a newsletter system (CLAUDE-474)

`actions/examples/release-notes.yml` turns the week's releases into a draft newsletter. Every Monday at 09:00 it reads the releases published in the last 7 days in the repos you list, through the Forgejo API (with the token fj already holds for your forge), has Claude write one customer-facing edition from them, and creates it as a **draft** with `POST <NEWSLETTER_URL>/api/v1/editions`. It never sends or schedules. A week without releases runs nothing; a week with nothing customer-facing creates no draft.

It is an example, so a new install does not get it: `actions/examples/` is not seeded. The post step expects an API that takes `{"list", "subject", "preview_text", "body_md"}` with a bearer token and answers `201`; change that step for another newsletter system.

## Setup

Everything is done in the web UI; the deployment needs no change.

1. In your newsletter system, require approval before a draft can be sent, if it can, so nothing goes out unreviewed.
2. Create an API token that can create drafts.
3. On the **Secrets** page, store the token as `newsletter-api-token`.
4. **New action**, raw YAML (`/actions/new/raw`): paste the example and replace the placeholders in its `env` block. The run stops with a message until `FORGE_HOST` and `NEWSLETTER_URL` are yours.

| Variable | Value |
|---|---|
| `FORGE_HOST` | your Forgejo host; fj must hold a token for it |
| `RELEASE_SOURCES` | JSON list of `{"repo": "owner/name", "product": "Product name"}` |
| `PRODUCT_NOTES` | one line per product: what it is, as a customer sees it |
| `SENDER_NAME` | the name the newsletter goes out under |
| `NEWSLETTER_URL` | your newsletter system's base URL |
| `NEWSLETTER_LIST` | the list to draft into |

5. Trigger it once from the action's page and read the draft in your newsletter system.

To rotate the token, replace the secret on the Secrets page; the next run uses it, with no restart.

To try it against another site without touching the stored action, run a copy of the file with its `env` values changed and `secrets` pointing at a secret for that site:

```nushell
docker compose exec --user appuser claude-bot claude-run run /tmp/release-notes.yml
```

## One draft per week

Each ISO week is one job (`dedup_key: week`) with `max_attempts_per_key: 1`, so a second trigger in the same week does nothing, a failure included. To redraft a week, reset it and trigger again:

```nushell
docker compose exec --user appuser claude-bot claude-run action reset-attempts release-notes 2026-W44
```

## A failed week carries over

The input looks back 7 days, or to the last draft or skip for this newsletter site if that is earlier. The post step records that time in `$CLAUDE_RUN_DATA_DIR/release-notes/<host>.last-draft`, so a week that failed leaves its releases for the next draft. The file is per site, so a run against staging does not move production's window.

## Products

Each repo in `RELEASE_SOURCES` carries the product customers know it as, and the edition has one section per product. Repos sharing a product, such as a UI and a server, are written as one: a change is described once however many repos it touched, and a server change with no visible effect is dropped.

A release is not always live on production when the draft is written, so the reviewer checks that the changes in a draft are deployed before approving it.

## What the draft leaves out

The prompt keeps only what a subscribed customer would notice and care about. It leaves out internal work, consent and mailing mechanics, signup promotion, fixes that make readers doubt past behaviour, and anything the changelog does not show from a customer's side. One or two items is a normal edition.
