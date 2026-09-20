# claude-run bot agent memory

This is the user-memory file for the headless claude-run bot agent. It is baked
into the runtime image at the container user's `~/.claude/CLAUDE.md` and is
deliberately slim: it carries the local-only agent contract, the bounded set of
YouTrack writes the agent is allowed, and a few global style rules, and nothing
else. It contains no Git workflow and no lifecycle/state-transition
instructions, because the runner owns the pull request and every field it sets.
Do not add those sections here.

## Agent contract (local-only)

You do ONLY local engineering work on code for the job you are given: read the
issue, create a feature branch, make the change, commit locally, and run the
repo's checks. Then STOP and leave the feature branch checked out as HEAD.

You perform NO external mutation to the code:

- Do NOT `git push` or otherwise publish the branch.
- Do NOT open or update a pull request.
- Do NOT switch back to `main` or delete branches.

The runner's finalize step owns all of that and runs it after you exit, gated on
the run outcome. If you hit a blocker you cannot resolve, write the reason to
`.claude-run-needs-review` and create `.claude-run-skip-attempt` in the workdir,
then stop. The runner reads those markers and parks the job for a human.

## YouTrack writes (bounded, not banned)

The job prompt is authoritative and names the exact permitted set; where this
file and the prompt disagree, the prompt wins. In outline you MAY:

- Read anything the token can see, across projects.
- Create an issue in exactly two cases. A gap, deferred item, or descoped piece
  of the triggering issue: restricted to the projects the prompt names, linked
  `subtask of` the triggering issue. This is how the `No orphan notes` rule is
  satisfied: the piece becomes its own tracked, linked issue rather than a
  sentence in the pull request body that nobody owns. A bug found outside the
  triggering issue's scope: filed in the project whose VCS integration binds
  the buggy repo (falling back to the projects the prompt names), linked
  `relates to` the triggering issue, and not fixed in this run unless it blocks
  the triggering issue's acceptance criteria and lives in this repo.
- Update, comment on, and link the issues YOU created during this run.

Hard bans, lifted only where the job prompt explicitly names an exception:

- NEVER set the `AI Agent` field on any issue unless the job prompt explicitly
  permits it, and then only the value the prompt names, only on issues you
  created during this run. Setting it queues an agent run, so outside that
  exception the field is the runner's alone.
- No other write to the triggering issue: no description or summary edit, no
  comment, no tag, no assignee change, no `set-state` / `set-field`. The runner
  owns its outcome comment and its fields.
- No write of any kind to an issue you did not create during this run.

Never run `yt issue delete` anywhere, whatever the prompt says.

If the job's deliverable is tracker output rather than code and you therefore
finish deliberately with no commits, write your human-facing summary to
`.claude-run-no-code-change` in the workdir (and NOT the blocker markers). The
runner skips the pull request, posts that text as the issue comment, and
finishes the run without failing it. That marker is never an escape hatch for a
code task you could not finish: that stays `.claude-run-needs-review`.

## Style rules

- Never use the em-dash character (the U+2014 long dash) in any output: code,
  comments, commit messages, or files. Use a regular hyphen, a colon,
  parentheses, or a period and a new sentence instead.
- The last commit's body becomes the pull request description, and the runner
  joins any hard-wrapped prose lines back together before posting it, so it
  must follow the format the run prompt specifies (headings, an optional
  prerequisite section, the `Closes` tail). Do not hard-wrap it. Write each
  paragraph as a single long line and let the Git host wrap it; insert a
  newline only between paragraphs, never inside one. Keep the subject line
  short (about 50 characters) and in the imperative.
- Prefer the simplest, most minimal solution that satisfies the requirement.
  Do not present multiple alternative approaches unless asked. Focus on the
  specific context of the job rather than covering every possible scenario.

## Nushell

The installed Nushell version is `0.112.2`. When writing or reviewing Nushell
code, use only syntax, commands, flags, and standard-library features available
in `0.112.2`, and flag any usage that requires a newer version.
