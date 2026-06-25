# claude-run bot agent memory

This is the user-memory file for the headless claude-run bot agent. It is baked
into the runtime image at the container user's `~/.claude/CLAUDE.md` and is
deliberately slim: it carries the local-only agent contract and a few global
style rules, and nothing else. It contains no Git workflow, no YouTrack
workflow, and no lifecycle/state-transition instructions, because the runner
owns every external mutation. Do not add those sections here.

## Agent contract (local-only)

You do ONLY local engineering work for the job you are given: read the issue,
create a feature branch, make the change, commit locally, and run the repo's
checks. Then STOP and leave the feature branch checked out as HEAD.

You perform NO external mutation:

- Do NOT `git push` or otherwise publish the branch.
- Do NOT open or update a pull request.
- Do NOT write to YouTrack (no field changes, comments, state transitions, or
  assignee changes) and do NOT run `yt`/`fj` to mutate anything.
- Do NOT switch back to `main` or delete branches.

The runner's finalize step owns all of that and runs it after you exit, gated on
the run outcome. If you hit a blocker you cannot resolve, write the reason to
`.claude-run-needs-review` and create `.claude-run-skip-attempt` in the workdir,
then stop. The runner reads those markers and parks the job for a human.

## Style rules

- Never use the em-dash character (the U+2014 long dash) in any output: code,
  comments, commit messages, or files. Use a regular hyphen, a colon,
  parentheses, or a period and a new sentence instead.
- Do not hard-wrap commit message bodies. Write each paragraph as a single long
  line and let the Git host wrap it; insert a newline only between paragraphs,
  never inside one. Keep the subject line short (about 50 characters) and in the
  imperative.
- Prefer the simplest, most minimal solution that satisfies the requirement.
  Do not present multiple alternative approaches unless asked. Focus on the
  specific context of the job rather than covering every possible scenario.

## Nushell

The installed Nushell version is `0.112.2`. When writing or reviewing Nushell
code, use only syntax, commands, flags, and standard-library features available
in `0.112.2`, and flag any usage that requires a newer version.
