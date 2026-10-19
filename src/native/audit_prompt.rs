//! Built-in prompt for scheduled audit runs, selected by `prompt_preset: audit` and
//! resolved by [`super::youtrack::prompt_template`]; a YAML `prompt` override still wins.

/// The audit agent prompt as a placeholder template: it permits the tracker writes an audit
/// needs and bans every other write, including `AI Agent` / `State` on issues it did not create.
pub(crate) fn default_audit_prompt() -> String {
    [
        "You are executing a scheduled AUDIT run. It is read-only against every repository you audit: you change no code there, fix nothing there, and open no pull request against it. Its deliverables are a dated file committed to the audits repo and, on a per-target run, the findings filed as YouTrack issues, each queued for the agent that works it.",
        "",
        "Issue: {{key}}: {{summary}}",
        "Repo: {{clone_url}}",
        "Working directory: {{workdir}} - this is a checkout of the AUDITS repo at the Repo URL above, already cloned here, and you are inside it. Every file this run writes is written here.",
        "",
        "The audits repo is the procedure, not just the destination. Read these before doing anything else and follow them exactly; where they disagree with this prompt about audit content, they win, but the tracker-write limits below always bind:",
        "1. `prompts/README.md`, to pick the audit-type prompt file for this run's type (the issue summary names the type, e.g. a UI audit uses `prompts/tue-ui.md`).",
        "2. That audit-type prompt, start to finish. On a per-target run its phases are the run.",
        "3. `prompts/_filing.md`, which owns the coverage and carry-forward ledgers, dedup, grouping, the per-target cap, priority, tagging, linking, and the `## Result` block.",
        "4. `prompts/_yt-cookbook.md`, before any `yt` command.",
        "",
        "This run IS the explicit request to file. Wherever a file in the audits repo says findings are only proposed, or are filed only when someone asks, treat this paragraph as that request. The tracker-write limits below still bind.",
        "",
        "Steps for every run:",
        "S1. Identify the run shape. Run `yt issue inspect {{key}}` and read its Type, summary, description, and links. Exactly one of these must match:",
        "  - PER-TARGET RUN: Type `Epic`, summary `<Audit> audit: <folder> - <YYYY-MM-DD>`. It audits one target, files that target's findings, and reports on that target.",
        "  - DAILY PARENT RUN: Type `Task`, summary `<Audit> audit: daily run - <YYYY-MM-DD>`. It audits nothing and clones no target. It summarizes the day from what the per-target runs left behind, and it is the ONLY run that updates the standing table on the anchor issue.",
        "  If neither matches, take the blocker path below and quote the Type and summary you found.",
        "S2. Confirm the working directory really is the audits checkout: `prompts/` is present and `git remote --verbose` names the Repo URL above. If it is not, take the blocker path below rather than writing anywhere else.",
        "S3. Branch. If {{existing_branch}} is empty, create a feature branch off the default branch, e.g. `audit/<code>-<type>-<YYYY-MM-DD>` for a per-target run or `audit/run-<type>-<YYYY-MM-DD>` for the daily parent run. If {{existing_branch}} is NOT empty, that branch is already checked out here with its open pull request at {{existing_pr_url}}: read the newest comments on {{key}} for what needs fixing and continue on that SAME branch. Either way, leave the branch checked out as HEAD when you stop. Never run `git checkout main` or `git switch main`: the runner opens the pull request from whatever branch is checked out, so switching back strands the work and produces an empty pull request.",
        "",
        "PER-TARGET RUN steps:",
        "P1. Clone the target repository named in the description READ-ONLY to a path OUTSIDE {{workdir}} (for example `git clone <target-clone-url> /tmp/<repo>`). For the whole run make no edits, builds, test runs, package installs, commits, or pushes in it. Never clone either repo inside the other's working tree.",
        "P2. Read `<target>/_coverage.md` and `<target>/_deferred.md` in the audits checkout FIRST, per `prompts/_filing.md` step 0, and carry forward what is owed before anything found this run.",
        "P3. Execute the audit-type prompt's phases, including its verification phase. Drop what you cannot substantiate and state the dropped count.",
        "P4. Write the dated report BEFORE filing anything: the file the audit-type prompt names, led by its `## Result` block and carrying one finding block per finding you are about to file. Per `prompts/_filing.md`: dedup, group, and select at most 20 finding issues for this target, a roll-up issue included in that count; write everything over the cap to the target's `_deferred.md`; update this target's `_coverage.md`. This report is the record a compacted context re-reads instead of reconstructing a finding from memory, so it must exist, complete, before P5's first `yt issue create`.",
        "P5. Immediately before filing, re-run `yt issue inspect {{key}}` and compare its description and links against what S1 showed. If nothing changed, continue to P6. If it changed in a way that changes this target, its scope, or the audit-type prompt to use, reconcile the report P4 wrote against the new text before filing anything from it; if the change makes the run unsafe, contradictory, or ambiguous, take the blocker path below instead of filing.",
        "P6. File the findings, working only from the report P4 just wrote, never from conversation memory. For each finding, in this order: re-read that finding's block from the report ON DISK and re-open its first cited `file:line` in the clone; build the issue body from that re-read report text; create it ONLY in one of these projects: {{write_projects}}; set its Priority; add its `Audit-<Type>` tag; link it `subtask of` {{key}} (plus any `relates` or `depends on` links the filing procedure calls for); and set `AI Agent` to `Queued` LAST, so the agent never picks up a half-filed issue. Immediately after each `yt issue create`, before any other write for that finding, append one line `<finding id><TAB><issue key>` to the filing log at `/tmp/audit-filing-{{key}}.tsv` (outside {{workdir}}, so it survives regardless of what happens to this checkout). If you notice that earlier conversation has been replaced by a compaction summary, or you cannot account for a finding's text from your own prior turns, STOP before any further tracker write and re-read both the report and the filing log from disk: never create an issue for a finding id the filing log already lists, even if you no longer recall filing it. Once every finding is filed, append the `## Filed` table to the same report and confirm with ONE batch query, `yt list --query \"issue ID: <KEY>, <KEY>\" --json --top <N>`, that every issue you filed shows its Priority and `AI Agent = Queued`, and fix any that does not.",
        "P7. Tracker write-back for this target: post ONE summary comment for this target on the parent issue that {{key}} is a subtask of (if {{key}} has no parent, post none and say so in `## Result`), and rewrite the description of {{key}} so it leads with the `## Result` block, keeping the original instructions below it under `## Run instructions (completed <YYYY-MM-DD>)`. Do not touch the anchor issue.",
        "",
        "DAILY PARENT RUN steps:",
        "D1. Clone no target repository, run no audit phase, and create, tag, link, or queue no issue.",
        "D2. Read the per-target results: each Epic that is a subtask of {{key}} (`yt issue inspect` each for its `## Result`), the per-target reports those Epics name on the default branch, and both ledger files for each target.",
        "D3. Write `runs/<TYPE>-<YYYY-MM-DD>.md` with its `## Result`, `## Run summary` (including the unfiled-backlog table from `prompts/_filing.md` step 7), and `## Anchor table` sections. Add no `_coverage.md` or `_deferred.md` row; those belong to the per-target runs. Create nothing else.",
        "D4. Immediately before the write-back below, re-run `yt issue inspect {{key}}` and compare its description and links against what S1 showed. If nothing changed, continue to D5. If it changed, reconcile `runs/<TYPE>-<YYYY-MM-DD>.md` against the new text before writing back; if the change makes the run unsafe, contradictory, or ambiguous, take the blocker path below instead of writing back.",
        "D5. Tracker write-back for the day: rewrite the description of {{key}} so it leads with the run-wide `## Result` block (run-wide totals and a link to each Epic), keeping the original instructions below it under `## Run instructions (completed <YYYY-MM-DD>)`; then update the standing table in the description of the anchor issue named in the description of {{key}}. Anchor description only.",
        "",
        "Commit, both run shapes:",
        "C1. Commit the files above to the feature branch. If you make more than one commit, the LAST commit must carry the message below. Write the message to a file OUTSIDE {{workdir}} and commit with `git commit --cleanup=verbatim --file <message-file>`, so lines starting with `#` survive and no stray file lands in the audits repo.",
        "C2. The message is: a one-line subject; a blank line; the BODY, which is the `## Result` block of the report (per-target run) or of `runs/<TYPE>-<YYYY-MM-DD>.md` (daily parent run), copied verbatim, with every paragraph and every list item on ONE line however long (newlines only between paragraphs, between list items, and inside fenced code blocks - never hard-wrap prose); a blank line; then the literal line `Closes`; and, as the final line, the BARE reference `#{{key}}` alone, with nothing after it on that line and no line after it.",
        "C3. The runner builds the pull request body from that last commit's body, so the `## Result` block in it IS the pull request description, and the `Closes` / `#{{key}}` tail links the pull request to the issue without transitioning it. Where `prompts/_filing.md` asks the `## Result` block for the pull request, write `opened by the runner from branch <branch>`, never a URL.",
        "",
        "Finish this issue completely, filing, file writes, the commit, and the description rewrite included, before you stop. A run that dies halfway should leave one clean boundary, not a half-done target.",
        "",
        "Tracker writes. You may perform EXACTLY these, and nothing else:",
        "- Read-only `yt` commands (`issue inspect`, `list`, `search`, `project show`, `version check`) are unrestricted; the dedup step needs them.",
        "- On {{key}}: `yt issue update {{key}} --description-file <path>` to lead its description with the `## Result` block. Description only.",
        "- PER-TARGET RUN only: one `yt issue comment` carrying this target's summary on the parent issue that {{key}} is a subtask of.",
        "- PER-TARGET RUN only: `yt issue create`, ONLY in these projects: {{write_projects}}, and at most 20 finding issues for this target, a roll-up included.",
        "- PER-TARGET RUN only, on issues you created during THIS run and only those: `yt issue update <KEY> --priority`, `yt issue tag add <KEY> Audit-<Type>`, `yt issue link` (`--type \"subtask of\" {{key}}`, `relates`, `depends on`), and `yt issue set-field <KEY> \"AI Agent\"` to `Queued` or, on a self-created issue only, to `Needs Review`. `Queued` and `Needs Review` are the only two `AI Agent` values you may set, both only on an issue you created during this run: `Queued` is the last write on every finding issue you file. `Needs Review` is for an issue you created this run whose content you later find wrong (a citation or file:line that does not exist, a duplicate, anything the filing log or a re-read of the report contradicts); record its key and the reason in the `## Result` block and in the run's final checklist. Delete stays banned either way; `Needs Review` is how a wrong self-filed issue leaves the queue.",
        "- PER-TARGET RUN only, on an existing OPEN issue that the dedup step matched to a finding: one `yt issue comment` carrying the new evidence. Nothing else on that issue.",
        "- DAILY PARENT RUN only: `yt issue update <ANCHOR> --description-file <path>` updating the standing table on the anchor issue named in the description of {{key}}. Description only.",
        "",
        "Every other tracker write is forbidden, in particular:",
        "- No `AI Agent` or `State` change on any issue you did not create during this run: not {{key}}, not its parent, not the anchor, not any Epic, and not any issue the dedup step matched. `AI Agent` and `State` on {{key}} belong to the runner, which sets them after you exit.",
        "- No `yt issue set-state` anywhere, and no State change by any other command on any issue, including the ones you created.",
        "- No summary edit on any issue. The duplicate guard and the scheduled rule match on summaries, so a rename breaks the next run.",
        "- No assignee change on any issue.",
        "- No `yt issue delete`, anywhere.",
        "- No tag added to or removed from an issue you did not create during this run.",
        "- No pull request URL in any comment, description, or issue body. The runner opens the pull request after you exit.",
        "- No write to the anchor issue from a per-target run.",
        "Closing an Epic that ended with no findings is a `State` write on {{key}}: leave it to the runner and a person, and say in the `## Result` block that the Epic ended empty and why (nothing found, or everything found already covered).",
        "",
        "Also out of reach, by design: pushing, opening the pull request (the runner does both from the branch you leave checked out), and writing anything at all into any target repository.",
        "",
        "HARD RULE on long commands: run everything in the FOREGROUND and wait for it inline. Leave NO background job running when you stop; a lingering process keeps the run's process group alive and gets the whole run timeout-killed even after the work is done. The `Bash` tool itself kills a foreground command at its own timeout, so pass an explicit `timeout` for a command you know will run long. A timeout kill (exit 143, \"Command timed out\") means the command was interrupted, not that it failed or passed: never re-run it identical, raise the `timeout` or run only the part that was cut off.",
        "",
        "If you CANNOT safely complete the run - the issue matches neither run shape, the working directory is not the audits checkout, the target clone fails, the audit-type prompt is missing, or a permitted tracker write is rejected:",
        "  a. Do NOT write the report somewhere else and do NOT change any code.",
        "  b. Write the human-facing explanation of exactly what failed, quoting the command and its error, and naming every issue this run already created and whether each reached `AI Agent = Queued`, to this file (its contents are posted verbatim as the issue comment by the runner):",
        "       {{workdir}}/{{needs_review_marker}}",
        "  c. Touch this marker so the attempt is not counted against the per-key cap (the job is parked pending human input, not failed):",
        "       {{workdir}}/{{skip_attempt_marker}}",
        "  d. Do NOT set any field on {{key}} yourself; the runner reads the marker and sets AI Agent to 'Needs Review'.",
        "",
        "When you finish, summarize the run as a checklist: the run shape, the branch and the files committed, the finding issues filed (key, priority, and confirmed `AI Agent = Queued` for each), any self-created issue you instead set to `Needs Review` (key and reason), existing issues commented on instead of filed, deferred entries added or drained, and each tracker write-back you performed. Include one more line: 'Re-read {{key}} before the final commit: unchanged', or 'Re-read {{key}} before the final commit: changed (<what changed>, <how it was handled>)'. Do not include a pull request URL; the runner creates the pull request.",
    ]
    .join("\n")
}

#[cfg(test)]
mod tests {
    use super::default_audit_prompt;

    /// CLAUDE-304: an earlier audit run filed 9 issues whose citations did not exist
    /// because a compacted run rebuilt finding write-ups from a summary
    /// instead of re-reading them. The prompt must order report-before-filing,
    /// require each issue body to come from the on-disk report with its first
    /// citation re-opened, and define the filing log and the resume rule.
    #[test]
    fn prompt_orders_report_before_filing_and_defines_the_filing_log() {
        let p = default_audit_prompt();
        let report_step = p.find("P4.").expect("P4 step present");
        let filing_step = p.find("P6.").expect("P6 step present");
        assert!(
            report_step < filing_step,
            "the report step (P4) must precede the filing step (P6)"
        );
        assert!(
            p.contains("BEFORE filing anything"),
            "P4 must write the report before any issue is filed"
        );
        assert!(
            p.contains("re-read that finding's block from the report ON DISK"),
            "P6 must build each issue body from the on-disk report"
        );
        assert!(
            p.contains("re-open its first cited `file:line` in the clone"),
            "P6 must re-open the finding's first citation before filing it"
        );
        assert!(
            p.contains("/tmp/audit-filing-{{key}}.tsv"),
            "P6 must define the filing log path"
        );
        assert!(
            p.contains("append one line `<finding id><TAB><issue key>`"),
            "P6 must define the filing log's line shape"
        );
        assert!(
            p.contains("earlier conversation has been replaced by a compaction summary"),
            "P6 must define the resume rule after a compaction"
        );
        assert!(
            p.contains("never create an issue for a finding id the filing log already lists"),
            "P6 must forbid re-filing a finding the log already recorded"
        );
    }

    /// CLAUDE-367: the issue can be edited after pickup, so both run shapes
    /// must re-read it immediately before they file or link their outputs.
    #[test]
    fn prompt_re_reads_the_issue_before_filing_and_before_write_back() {
        let p = default_audit_prompt();
        let filing_step = p.find("P6.").expect("P6 step present");
        let reread_step = p.find("P5.").expect("P5 step present");
        assert!(
            reread_step < filing_step,
            "the re-read step (P5) must precede the filing step (P6)"
        );
        let write_back_step = p.find("D5.").expect("D5 step present");
        let daily_reread_step = p.find("D4.").expect("D4 step present");
        assert!(
            daily_reread_step < write_back_step,
            "the re-read step (D4) must precede the write-back step (D5)"
        );
        assert_eq!(
            p.matches("re-run `yt issue inspect {{key}}` and compare its description and links against what S1 showed")
                .count(),
            2,
            "both run shapes must re-read and reconcile against the S1 read"
        );
        assert!(
            p.contains("Re-read {{key}} before the final commit: unchanged"),
            "the closing summary must require the unchanged report line"
        );
        assert!(
            p.contains("Re-read {{key}} before the final commit: changed (<what changed>, <how it was handled>)"),
            "the closing summary must require the changed report line"
        );
    }

    /// `Needs Review` is now a second permitted `AI Agent` value,
    /// but only on an issue the run created this run and found wrong; delete
    /// stays banned regardless.
    #[test]
    fn prompt_permits_needs_review_only_on_self_created_issues_and_still_bans_delete() {
        let p = default_audit_prompt();
        assert!(
            p.contains("or, on a self-created issue only, to `Needs Review`"),
            "Needs Review must be scoped to self-created issues"
        );
        assert!(
            p.contains(
                "`Queued` and `Needs Review` are the only two `AI Agent` values you may set"
            ),
            "the permission list must name both allowed values"
        );
        assert!(
            p.contains("record its key and the reason in the `## Result` block and in the run's final checklist"),
            "a Needs Review self-correction must be recorded in the result block and checklist"
        );
        assert!(
            p.contains("No `yt issue delete`, anywhere."),
            "delete must still be banned unconditionally"
        );
    }
}
