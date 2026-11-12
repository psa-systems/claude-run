# Example action: a newsletter draft for each new blog post (CLAUDE-644)

`actions/examples/blog-newsletter.yml` turns each new blog post into a newsletter email that a person approves. It reads the blog's newsletter feed, and for each post it has not seen before it has Claude write a short summary of the post, creates it as a **draft** with `POST <NEWSLETTER_URL>/api/v1/editions`, and then asks for review with `POST <NEWSLETTER_URL>/api/v1/editions/{id}/review`, so the newsletter's administrators are told a draft is waiting. It never sends or schedules: an administrator approves and sends it in the newsletter system.

It is an example, so a new install does not get it: `actions/examples/` is not seeded. The post step expects the same API as [release-notes](release-notes.md): `{"list", "subject", "preview_text", "body_md"}` with a bearer token, answering `201` with the edition's `id`. A newsletter system without the review call (`404` or `405`) keeps the plain draft, and the run logs a warning; any other answer to the review call is logged the same way and the draft stays.

## The newsletter feed

Authors choose per post whether it goes to the newsletter. The storefront lists only those posts in its newsletter feed, `/blog/newsletter.xml` (`/newsletter.xml` on the blog's own host), in the same RSS shape as the public `/blog/feed.xml`. Point `BLOG_FEED_URL` at the newsletter feed: the public feed would draft every post. A post marked for the newsletter after it was published appears in the feed then and is drafted on the next check.

## What the email says

Claude reads the post's feed entry (title, link, summary, author, categories) and the published post page, and writes the email mainly from the page. A post with no summary has no `description` in the feed; the action then works from the page alone. It writes:

- `subject`: the post title or a short newsletter-style version of it, under 70 characters;
- `preview_text`: one sentence, at most 150 characters (the post step cuts a longer one, and uses the post title when there is none);
- `body_md`: one short paragraph of two or three sentences on what the post covers.

The post step ends the body with `[Read the full post](<post URL>)`, so the email is just that summary and the link: no cover image, no headings, never text copied from the article. The prompt also forbids inventing anything that is not in the post. A page that is not a readable article (an error page, a login wall) makes no draft.

## Setup

Everything is done in the web UI; the deployment needs no change.

1. In your newsletter system, require approval before a draft can be sent, if it can, so nothing goes out unreviewed.
2. Create an API token that can create drafts and ask for review. The release-notes example uses the same kind of token; one secret can serve both when they draft into the same newsletter system.
3. On the **Secrets** page, store the token as `newsletter-api-token`.
4. **New action**, raw YAML (`/actions/new/raw`): paste the example and replace the placeholders in its `env` block. The run stops with a message until `BLOG_FEED_URL` and `NEWSLETTER_URL` are yours.

| Variable | Value |
|---|---|
| `BLOG_FEED_URL` | the blog's newsletter feed (only posts marked for the newsletter), for example `https://blog.example.com/blog/newsletter.xml`; not the public `/blog/feed.xml` |
| `BLOG_NAME` | the blog's name, as readers know it |
| `SENDER_NAME` | the name the newsletter goes out under |
| `NEWSLETTER_URL` | your newsletter system's base URL |
| `NEWSLETTER_LIST` | the list to draft into |

5. Wait for the first check (or trigger it from the action's page). It only records the posts already in the feed, so it drafts nothing. Publish a post marked for the newsletter and the next check drafts it.

## The first run drafts nothing

The input keeps the guids it has seen in `$CLAUDE_RUN_DATA_DIR/blog-newsletter/<blog host>--<newsletter host>.seen`, one per line. When that file does not exist, the run writes every guid now in the feed to it and yields no job, so enabling the action on a blog with existing posts mails none of them. The file is per blog and per newsletter site, so a test against a staging newsletter does not stop the production one from seeding on its own.

## One draft per post

Each feed item is one job keyed on its `guid` (`dedup_key: guid`, `max_attempts_per_key: 1`), oldest first. The post step adds the guid to the seen file before anything else, whether a draft was made, skipped or failed, so a re-run, a restart or an edit to the post never creates a second draft. Editing a post keeps its guid; the feed's guid must not change when a post is edited.

A run whose Claude call hit a rate limit or a login problem skips the post step and is retried on the next check.

To redraft one post, for example after a failed run, delete its draft in the newsletter system if there is one, remove its guid line from the seen file, reset its attempt and trigger the action:

```nushell
docker compose exec --user appuser claude-bot claude-run action reset-attempts blog-newsletter <guid>
```

## When it runs

The interval schedule makes the action due 15 minutes after its last run. A check that finds no new post starts no run, so a due action checks the feed again every `regather_interval` (default 60s) until a post appears. Each check is one feed request; it makes no newsletter API call.

## Rotating the token

Replace the secret on the Secrets page; the next run uses it, with no restart.

To try it against another site without touching the stored action, run a copy of the file with its `env` values changed and `secrets` pointing at a secret for that site:

```nushell
docker compose exec --user appuser claude-bot claude-run run /tmp/blog-newsletter.yml
```

## Turning it off

Disable the action on its page, or delete it. Nothing else changes: drafts it already made stay ordinary drafts in the newsletter system, where they can be approved or deleted. The seen file stays, so enabling the action again drafts each post published while it was off; delete the file first to seed afresh instead.
