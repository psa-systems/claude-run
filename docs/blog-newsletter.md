# Example action: a newsletter draft for each new blog post (CLAUDE-644)

`actions/examples/blog-newsletter.yml` turns each new blog post into a newsletter email that a person approves. It reads the blog's newsletter feed, and for each post it has not handled before it has Claude write a short summary of the post, checks it, creates it as a **draft** with `POST <NEWSLETTER_URL>/api/v1/editions`, and then asks for review with `POST <NEWSLETTER_URL>/api/v1/editions/{id}/review`, so the newsletter's administrators are told a draft is waiting. It never sends or schedules: an administrator approves and sends it in the newsletter system.

It is an example, so a new install does not get it: `actions/examples/` is not seeded. The post step expects the same API as [release-notes](release-notes.md): `{"list", "subject", "preview_text", "body_md"}` with a bearer token, answering `201` with the edition's `id`. A newsletter system without the review call (`404` or `405`) keeps the plain draft, and the run logs a warning; any other answer to the review call is logged the same way and the draft stays. Every HTTP call (the feed, the post page, the draft and the review request) gives up after 30 seconds.

## The newsletter feed

Authors choose per post whether it goes to the newsletter. The storefront lists only those posts in its newsletter feed, `/blog/newsletter.xml` (`/newsletter.xml` on the blog's own host), in the same RSS shape as the public `/blog/feed.xml`. Point `BLOG_FEED_URL` at the newsletter feed: the public feed would draft every post. A post marked for the newsletter after it was published appears in the feed then and is drafted on the next check, unless it was published before the action's first run (see below).

## What the email says

Claude reads the post's feed entry (title, link, summary, author, categories) and the published post page, and writes the email mainly from the page. A post with no summary has no `description` in the feed; the action then works from the page alone. The pre step drops the page's scripts, styles and HTML comments before Claude reads it, which saves tokens and changes nothing in the article. Claude writes:

- `subject`: the post title as it is, reworded only when it is over 70 characters or makes no sense outside the blog; no "New post:" prefix, emoji or exclamation mark. An empty subject falls back to the post title.
- `preview_text`: one sentence that adds to the subject, at most 150 characters (the post step cuts a longer one). There is no fallback: with no preview, or one that repeats the subject, the field is left out and mail clients show the start of the summary instead.
- `body_md`: one paragraph of two or three sentences on what the post covers, in the post's language, opening with its subject rather than "In this post".

The prompt asks for the tone of a colleague pointing a friend to a good article: no marketing words, exclamation marks, questions to the reader or calls to action, nothing invented, no sentence copied from the article. The post step ends the body with `[Read the full post](<post URL>)`, with spaces, `(`, `)`, `<` and `>` in the URL percent-encoded, so the email is just that summary and the link. A page that is not a readable article (an error page, a login wall) makes no draft: Claude writes `{"skip": true, "reason": ...}`, and the post counts as handled.

### What the post step checks

Before any request, the post step refuses Claude's output, posts nothing and records a failed attempt (see [Retries](#retries)) when:

- `edition.json` is missing or is not a JSON object;
- `subject` is not a string, or is empty and the post has no title;
- `body_md` is not a string, is empty, is 600 characters or longer, has a blank line (more than one paragraph), or contains `![`, `](`, `<` or `http` (an image, a link, HTML or a URL).

Line breaks inside the paragraph are joined into one line.

### What the reviewer sees

The draft in the newsletter system has the subject, the preview text and the body: the summary paragraph and the link to the post, which is how a reviewer tells which post it came from. The edition API has no field for a private note, and anything put in the subject, preview or body reaches subscribers, so the action adds none. The claude-run log names both sides: `draft edition <id> created on <list> from <post URL>: <subject>`.

## Setup

Everything is done in the web UI; the deployment needs no change.

1. In your newsletter system, require approval before a draft can be sent, if it can, so nothing goes out unreviewed.
2. Create an API token that can create drafts and ask for review, and nothing more. The release-notes example uses the same kind of token; one secret can serve both when they draft into the same newsletter system.
3. On the **Secrets** page, store the token as `newsletter-api-token`.
4. **New action**, raw YAML (`/actions/new/raw`): paste the example and replace the placeholders in its `env` block. Until every placeholder is replaced and the token is stored, each check stops before it creates any job or state file, naming what is left: `BLOG_FEED_URL` or `NEWSLETTER_URL` on an `example.com` host, `NEWSLETTER_LIST` still `your-list-slug`, `BLOG_NAME` still `Your Blog`, `SENDER_NAME` still `Your Company`, or an empty token.

| Variable | Value |
|---|---|
| `BLOG_FEED_URL` | the blog's newsletter feed (only posts marked for the newsletter), for example `https://blog.example.com/blog/newsletter.xml`; not the public `/blog/feed.xml` |
| `BLOG_NAME` | the blog's name, as readers know it |
| `SENDER_NAME` | the name the newsletter goes out under |
| `NEWSLETTER_URL` | your newsletter system's base URL |
| `NEWSLETTER_LIST` | the slug of the list to draft into |

5. Wait for the first check (or trigger it from the action's page). It only records the posts already in the feed, so it drafts nothing; the log says `first run, <n> existing posts marked as seen`. Publish a post marked for the newsletter and the next check drafts it.
6. Open the draft in the newsletter system and read it before you approve it, at least for the first few posts.

To try it against a staging newsletter first, use a copy with a different `NEWSLETTER_URL`: the state files are per blog and per newsletter host, so the test does not touch production's.

## The first run drafts nothing

The input keeps its state in `$CLAUDE_RUN_DATA_DIR/blog-newsletter/<blog host>--<newsletter host>.*`, one guid per line:

| File | Holds |
|---|---|
| `.seen` | a `# seeded <time>` line, then every post drafted or deliberately skipped, plus the posts already in the feed at the first run |
| `.failed` | one line per failed attempt of a post not yet drafted |
| `.dropped` | posts given up on after 3 failed attempts |

When `.seen` does not exist, the run writes the seed time and every guid now in the feed to it and yields no job, so enabling the action on a blog with existing posts mails none of them. Later, an unseen post whose `pubDate` is older than the seed time (one marked for the newsletter after the fact, or an old post that slides back into a trimmed feed) is added to `.seen` without a draft, and the log names it. A post with no readable `pubDate` is not filtered this way.

## One draft per post

Each feed item is one job keyed on its `guid` (`dedup_key: guid`), oldest first, at most 5 per check; when more are waiting, the log says so and the rest are drafted on later checks. Editing a post keeps its guid; the feed's guid must not change when a post is edited.

The post step adds the guid to `.seen` only once the newsletter system answered `201` to the draft request, or when Claude skipped the post on purpose. Before anything else, a job's `filter` re-reads `.seen` and `.dropped` and skips a post already there without counting an attempt.

Why the filter is needed: the daemon can start a check while an earlier run's job is still in its post step. Runs of an action overlap (SPEC-2.2, CLAUDE-162): this one is due 15 minutes after the last run started, and a manual trigger starts one at any time. Two runs never work the same post at once: the later run drops a gathered post whose job is in flight, and a post's key is held by one job at a time. But the later run reads the feed and `.seen` before that check, so when the earlier job finishes in between, the later run would draft the post again; with up to 3 attempts per post, the attempt cap would not stop it. The filter runs once the job holds the post's key, after the earlier job's post step is done, and sees the guid in `.seen`.

### Retries

A post gets up to 3 attempts (`max_attempts_per_key: 3`, and `MAX_TRIES` in the post step, which must match it). Each of these counts one, records a line in `.failed`, and leaves the guid out of `.seen`, so a later check tries again:

- the post page does not answer `200` or times out (a failed pre step counts an attempt and still runs the post step);
- Claude fails, writes no `edition.json`, or writes one the post step refuses;
- the draft request fails, times out or is answered with anything but `201` (for example an outage or a rotated token).

A draft request that timed out may still have created a draft; the log says so, and the next attempt can create a second one, so check the newsletter system first. When the answer is `201` but has no usable edition id, the post is marked seen, no review is asked for, and the run fails with a message saying a draft may already exist.

These do not count: a Claude rate limit or login problem (the post step does not run), and a cancelled or paused run (the post step logs `no draft yet` and the runner does not count the attempt).

After the third failure the post step adds the guid to `.dropped` and logs `is dropped`; the post is not tried again. To redraft a dropped post, or one that was drafted or skipped:

1. Delete its draft in the newsletter system, if there is one.
2. Remove its guid line from `.dropped` (or from `.seen`) in `$CLAUDE_RUN_DATA_DIR/blog-newsletter/`.
3. Reset its attempts, then trigger the action from its page:

```nushell
docker compose exec --user appuser claude-bot claude-run action reset-attempts blog-newsletter <guid>
```

## When it runs

The interval schedule makes the action due 15 minutes after its last run started. A check that finds no new post starts no run, so a due action checks the feed again every `regather_interval` (default 60s) until a post appears. Each check is one feed request; it makes no newsletter API call. Whatever the input logs (the first run, posts held back by the cap of 5, posts older than the seed time) appears in the daemon log as `input.command stderr`.

## Rotating the token

Replace the secret on the Secrets page; the next run uses it, with no restart. A post that failed while the token was wrong is retried on a later check, within its 3 attempts.

To try it against another site without touching the stored action, run a copy of the file with its `env` values changed and `secrets` pointing at a secret for that site:

```nushell
docker compose exec --user appuser claude-bot claude-run run /tmp/blog-newsletter.yml
```

## Turning it off

Disable the action on its page, or delete it. Nothing else changes: drafts it already made stay ordinary drafts in the newsletter system, where they can be approved or deleted. The state files stay, so enabling the action again drafts each post published while it was off, at most 5 per check; delete the three files first to seed afresh instead.
