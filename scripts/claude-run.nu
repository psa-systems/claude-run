#!/usr/bin/env nu
# claude-fix.nu
#
# Unattended Claude Fix cron job.
# Polls Forgejo for failed CI runs in allow-listed repos, runs Claude Code
# on each failure to attempt a fix, commits and pushes via the bot PAT,
# posts a summary comment on the PR. No human in the loop.
#
# Intended to run from cron on host-b every 30 minutes, but designed to be
# portable: depends only on the Forgejo REST API, git, and Claude Code.
# Should work on any Linux server with the dependencies below.
#
# Working version as of 2026-05-18, tested end-to-end on host-b against
# contoso/claude-fix-sandbox PR #3. Auth: a Max subscription
# (temporary, see YouTrack for dedicated automation account todo).
#
# ----------------------------------------------------------------------------
# DEPENDENCIES
# ----------------------------------------------------------------------------
#   - nu (Nushell)
#   - git
#   - claude (Claude Code CLI), authenticated via subscription on this host
#   - timeout (from coreutils, usually present)
#   - Network egress to dev.a8n.run and api.anthropic.com
#
# DOES NOT depend on `fj` (Forgejo CLI). We deliberately use the REST API
# directly because `fj` is not consistently installed across our servers
# (present on host-b, absent on host-c/host-d as of 2026-05-18).
#
# ----------------------------------------------------------------------------
# REQUIRED FILES
# ----------------------------------------------------------------------------
#   ~/.local/share/claude-fix/config.toml   - forgejo_pat = "gto_..."
#   ~/.local/share/claude-fix/state.json    - auto-created on first run
#   ~/.local/share/claude-fix/PAUSED        - presence pauses all runs
#
# ----------------------------------------------------------------------------
# KNOWN FRAGILITIES
# ----------------------------------------------------------------------------
#   - Subscription auth tokens expire periodically and require a browser
#     OAuth flow to refresh. When that happens, cron will keep firing but
#     all Claude runs will fail until someone re-logs in on a machine with
#     a browser. Monitor the daily logs for repeated auth errors.
#
#   - Currently using a shared Max account. Create dedicated
#     automation account before broader rollout.
#
# ----------------------------------------------------------------------------
# EXIT CODES
# ----------------------------------------------------------------------------
#   0 - ran successfully (whether or not it found work to do)
#   1 - paused (PAUSED file present)
#   2 - lock contention (previous run still going)
#   3 - config error (missing or invalid config)
#   4 - daily cap reached
#   5 - unexpected error (see logs)

# ============================================================================
# CONFIG
# ============================================================================

const STATE_DIR = "~/.local/share/claude-fix"
const LOG_DIR = "~/.local/share/claude-fix/logs"
const CONFIG_FILE = "~/.local/share/claude-fix/config.toml"
const STATE_FILE = "~/.local/share/claude-fix/state.json"
const PAUSE_FILE = "~/.local/share/claude-fix/PAUSED"
const LOCK_FILE = "~/.local/share/claude-fix/run.lock"

const REPO_ALLOWLIST = [
    "contoso/claude-fix-sandbox"
]

const POLL_WINDOW_MINUTES = 45
const MAX_ATTEMPTS_PER_COMMIT = 3
const MAX_RUNS_PER_DAY = 20
const MAX_DIFF_FILES = 10
const MAX_DIFF_LINES = 300
const CLAUDE_TIMEOUT_SECONDS = 600

const FORGEJO_HOST = "https://dev.a8n.run"
const GIT_AUTHOR_NAME = "claude-bot"
const GIT_AUTHOR_EMAIL = "claude-bot@a8n.run"
const WORKSPACE = "/tmp/claude-fix-workspace"

# ============================================================================
# UTILITIES
# ============================================================================

def expand-path [p: string] {
    $p | path expand
}

def now-iso [] {
    date now | format date "%Y-%m-%dT%H:%M:%S%z"
}

def today-date [] {
    date now | format date "%Y-%m-%d"
}

def log [level: string, message: string, extra: record = {}] {
    let log_dir = (expand-path $LOG_DIR)
    if not ($log_dir | path exists) {
        mkdir $log_dir
    }
    let log_file = $"($log_dir)/(today-date).log"
    let entry = {
        ts: (now-iso),
        level: $level,
        message: $message
    } | merge $extra
    let line = ($entry | to json --raw)
    $line | save --append $log_file
    print $line
}

def log-info [message: string, extra: record = {}] {
    log "info" $message $extra
}

def log-warn [message: string, extra: record = {}] {
    log "warn" $message $extra
}

def log-error [message: string, extra: record = {}] {
    log "error" $message $extra
}

def acquire-lock [] {
    let lock = (expand-path $LOCK_FILE)
    if ($lock | path exists) {
        let pid = (try { open $lock | str trim | into int } catch { 0 })
        if $pid > 0 {
            let running = (try {
                ps | where pid == $pid | length
            } catch { 0 })
            if $running > 0 {
                return false
            }
        }
        log-warn "removing stale lock file" {stale_pid: $pid}
        rm $lock
    }
    let parent = ($lock | path dirname)
    if not ($parent | path exists) {
        mkdir $parent
    }
    $"($nu.pid)" | save --force $lock
    return true
}

def release-lock [] {
    let lock = (expand-path $LOCK_FILE)
    if ($lock | path exists) {
        rm $lock
    }
}

# ============================================================================
# STATE
# ============================================================================

def load-state [] {
    let f = (expand-path $STATE_FILE)
    if not ($f | path exists) {
        return {
            attempts: {},
            daily_count: {},
        }
    }
    open $f
}

def save-state [state: record] {
    let f = (expand-path $STATE_FILE)
    let parent = ($f | path dirname)
    if not ($parent | path exists) {
        mkdir $parent
    }
    $state | to json | save --force $f
}

def get-attempts [state: record, commit_sha: string] {
    let a = ($state.attempts | get -o $commit_sha)
    if $a == null { 0 } else { $a.count }
}

def record-attempt [state: record, commit_sha: string, pr_url: string] {
    let prev = (get-attempts $state $commit_sha)
    let updated_attempts = ($state.attempts | upsert $commit_sha {
        count: ($prev + 1),
        last_tried_at: (now-iso),
        last_pr: $pr_url,
    })
    $state | upsert attempts $updated_attempts
}

def get-daily-count [state: record] {
    let today = (today-date)
    let c = ($state.daily_count | get -o $today)
    if $c == null { 0 } else { $c }
}

def increment-daily-count [state: record] {
    let today = (today-date)
    let prev = (get-daily-count $state)
    let updated = ($state.daily_count | upsert $today ($prev + 1))
    $state | upsert daily_count $updated
}

# ============================================================================
# CONFIG LOADING
# ============================================================================

def load-config [] {
    let f = (expand-path $CONFIG_FILE)
    if not ($f | path exists) {
        log-error "config file missing" {path: $f}
        exit 3
    }
    let c = (open $f)
    if ($c.forgejo_pat? | is-empty) {
        log-error "config missing forgejo_pat" {}
        exit 3
    }
    $c
}

# ============================================================================
# FORGEJO API
# ============================================================================

def list-failing-prs [repo: string, pat: string, window_minutes: int] {
    let cutoff = (date now) - ($window_minutes * 1min)
    let url = $"($FORGEJO_HOST)/api/v1/repos/($repo)/pulls?state=open"

    let prs = (try {
        http get --headers [Authorization $"token ($pat)"] $url
    } catch { |e|
        log-warn "failed to list PRs" {repo: $repo, error: ($e | get msg)}
        return []
    })

    log-info "open PRs found" {repo: $repo, count: ($prs | length)}

    $prs | each { |pr|
        let sha = $pr.head.sha
        let status_url = $"($FORGEJO_HOST)/api/v1/repos/($repo)/commits/($sha)/status"
        let status = (try {
            http get --headers [Authorization $"token ($pat)"] $status_url
        } catch { return null })
        if $status == null { return null }
        if $status.state != "failure" { return null }

        let updated = (try { $pr.updated_at | into datetime } catch { return null })
        if $updated < $cutoff { return null }

        {
            pr: $pr,
            commit_sha: $sha,
            status: $status,
        }
    } | compact
}

def post-pr-comment [repo: string, pr_number: int, body: string, pat: string] {
    let url = $"($FORGEJO_HOST)/api/v1/repos/($repo)/issues/($pr_number)/comments"
    try {
        http post --content-type "application/json" --headers [Authorization $"token ($pat)"] $url {body: $body}
    } catch { |e|
        log-warn "failed to post comment" {repo: $repo, pr: $pr_number, error: ($e | get msg)}
    }
}

# ============================================================================
# CLAUDE INVOCATION + GIT OPS
# ============================================================================

def clone-pr-branch [repo: string, branch: string, pat: string, dest: string] {
    let url = $"https://oauth2:($pat)@dev.a8n.run/($repo).git"

    cd /tmp

    if ($dest | path exists) {
        rm -rf $dest
    }

    let parent = ($dest | path dirname)
    if not ($parent | path exists) {
        mkdir $parent
    }

    git clone --branch $branch --depth 50 $url $dest
}

def run-claude [working_dir: string, pr_info: record] {
    let prompt = $"You are fixing a failing CI build on a pull request.\n\nThe repository is checked out in the current working directory.\nThe CI workflow files are in .forgejo/workflows/.\n\nPR: #($pr_info.number): ($pr_info.title)\nBranch: ($pr_info.branch)\nCommit: ($pr_info.head_sha)\n\nYour job:\n  1. Read the CI workflow files to understand what checks run\n  2. Investigate the code to find the cause of the failure\n  3. Make the MINIMUM change needed to fix it\n  4. Do NOT weaken tests, add retries to flaky tests, or skip checks\n  5. If the failure is something you cannot safely fix - for example a flaky test, infra issue, or ambiguous requirement - explain why and make no changes\n\nWhen you are done, summarize what you changed and why in plain text."

    cd $working_dir

    let result = (do {
        ^timeout $CLAUDE_TIMEOUT_SECONDS claude --print --permission-mode acceptEdits --allowed-tools "Read,Edit,Write,Bash,Glob,Grep" --output-format json $prompt
    } | complete)

    if $result.exit_code != 0 {
        let stderr_text = ($result.stderr | default "")
        let stdout_text = ($result.stdout | default "")
        let combined = $"($stderr_text)\n($stdout_text)"
        let is_rate_limit = ($combined | str contains -i "rate limit") or ($combined | str contains -i "usage limit")
        let is_auth = ($combined | str contains -i "unauthorized") or ($combined | str contains -i "auth")
        return {
            ok: false,
            error: $"claude exited ($result.exit_code)",
            summary: $combined,
            cost: 0.0,
            transient: ($is_rate_limit or $is_auth),
        }
    }

    let parsed = (try { $result.stdout | from json } catch {
        return {
            ok: false,
            error: "claude output was not valid JSON",
            summary: $result.stdout,
            cost: 0.0,
            transient: false,
        }
    })

    {
        ok: (not ($parsed.is_error? | default false)),
        summary: ($parsed.result? | default ""),
        cost: ($parsed.total_cost_usd? | default 0.0),
        transient: false,
    }
}

def check-diff-size [working_dir: string] {
    cd $working_dir
    let raw = (^git diff --numstat | complete)
    let lines_list = ($raw.stdout | lines | where ($it | str trim | is-not-empty))
    let stats = ($lines_list | each { |l|
        let parts = ($l | split row "\t")
        let added = (try { $parts.0 | into int } catch { 0 })
        let deleted = (try { $parts.1 | into int } catch { 0 })
        {added: $added, deleted: $deleted, file: ($parts | get -o 2 | default "")}
    })
    let files = ($stats | length)
    let lines_changed = (if $files == 0 { 0 } else { $stats | each { |s| $s.added + $s.deleted } | math sum })
    {files: $files, lines: $lines_changed, too_big: ($files > $MAX_DIFF_FILES or $lines_changed > $MAX_DIFF_LINES)}
}

def commit-and-push [working_dir: string, branch: string, summary: string, pr_number: int, pat: string] {
    cd $working_dir
    git config user.name $GIT_AUTHOR_NAME
    git config user.email $GIT_AUTHOR_EMAIL

    let status = (^git status --porcelain | complete)
    if ($status.stdout | str trim | is-empty) {
        return {pushed: false, reason: "no changes"}
    }

    git add -A
    let msg_file = "/tmp/claude-fix-commit-msg"
    $"fix: Claude auto-fix for PR #($pr_number)\n\n($summary)\n\n[skip claude]" | save --force $msg_file
    ^git commit --file $msg_file
    git push origin $branch
    {pushed: true, reason: "ok"}
}

# ============================================================================
# PER-FAILURE HANDLER
# ============================================================================

def handle-failure [repo: string, failure: record, pat: string, state: record] {
    let pr = $failure.pr
    let head_sha = $failure.commit_sha
    let pr_number = $pr.number
    let pr_url = $pr.html_url
    let branch = $pr.head.ref
    let author = ($pr | get -o user | get -o login | default "unknown")

    if $author == $GIT_AUTHOR_NAME {
        log-info "skipping PR authored by bot" {repo: $repo, pr: $pr_number, author: $author}
        return $state
    }

    let attempts = (get-attempts $state $head_sha)
    if $attempts >= $MAX_ATTEMPTS_PER_COMMIT {
        log-info "skipping commit, attempt cap reached" {repo: $repo, pr: $pr_number, commit: $head_sha, attempts: $attempts}
        return $state
    }

    log-info "handling failure" {repo: $repo, pr: $pr_number, commit: $head_sha, branch: $branch, attempt: ($attempts + 1)}

    let workdir = $"($WORKSPACE)/(($repo | str replace -a '/' '-'))-($pr_number)"
    try {
        clone-pr-branch $repo $branch $pat $workdir
    } catch { |e|
        log-error "clone failed" {repo: $repo, pr: $pr_number, error: ($e | get msg)}
        return $state
    }

    let pr_info = {
        number: $pr_number,
        title: $pr.title,
        branch: $branch,
        head_sha: $head_sha,
    }
    let claude_result = (run-claude $workdir $pr_info)

    if not $claude_result.ok {
        log-warn "claude run failed" {
            repo: $repo,
            pr: $pr_number,
            error: $claude_result.error,
            transient: ($claude_result | get -o transient | default false),
        }
        if ($claude_result | get -o transient | default false) {
            log-info "transient claude error, not counting as attempt" {pr: $pr_number}
            return $state
        }
        post-pr-comment $repo $pr_number $"🤖 Claude attempted a fix but encountered an error: ($claude_result.error)" $pat
        return (increment-daily-count (record-attempt $state $head_sha $pr_url))
    }

    log-info "claude finished" {repo: $repo, pr: $pr_number, cost: $claude_result.cost}

    let diff = (check-diff-size $workdir)
    # If Claude reports making no changes, or the diff is empty, post a "no changes" comment
    # and don't push. Catches the case where Claude correctly refuses to fix something.
    let claude_says_no_changes = ($claude_result.summary | str contains -i "changes made: none") or ($claude_result.summary | str contains -i "no changes" | default false) or ($claude_result.summary | str contains -i "making no changes")
    if $claude_says_no_changes or $diff.files == 0 {
        log-info "no real changes from claude, skipping push" {pr: $pr_number, files: $diff.files}
        let body = $"🤖 **Claude ran but made no changes.**\n\n($claude_result.summary)"
        post-pr-comment $repo $pr_number $body $pat
        return (increment-daily-count (record-attempt $state $head_sha $pr_url))
    }
    if $diff.too_big {
        log-warn "diff too large, not pushing" {repo: $repo, pr: $pr_number, files: $diff.files, lines: $diff.lines}
        let body = $"🤖 **Claude attempted a fix but the diff is too large for unattended push.**\n\nFiles: ($diff.files), Lines: ($diff.lines)\n\nSummary:\n($claude_result.summary)\n\nThis needs human review."
        post-pr-comment $repo $pr_number $body $pat
        return (increment-daily-count (record-attempt $state $head_sha $pr_url))
    }

    let push_result = (commit-and-push $workdir $branch $claude_result.summary $pr_number $pat)
    let body = if $push_result.pushed {
        $"🤖 **Claude pushed a fix.**\n\n($claude_result.summary)\n\nCI will re-run on the new commit. Review the diff before merging."
    } else {
        $"🤖 **Claude ran but made no changes.**\n\n($claude_result.summary)"
    }
    post-pr-comment $repo $pr_number $body $pat

    log-info "fix complete" {repo: $repo, pr: $pr_number, pushed: $push_result.pushed, cost: $claude_result.cost}

    increment-daily-count (record-attempt $state $head_sha $pr_url)
}

# ============================================================================
# MAIN
# ============================================================================

def main [] {
    let sd = (expand-path $STATE_DIR)
    if not ($sd | path exists) {
        mkdir $sd
    }

    if ((expand-path $PAUSE_FILE) | path exists) {
        log-info "paused, exiting" {}
        exit 1
    }

    if not (acquire-lock) {
        log-info "previous run still going, exiting" {}
        exit 2
    }

    try {
        let config = (load-config)
        mut state = (load-state)

        let daily = (get-daily-count $state)
        if $daily >= $MAX_RUNS_PER_DAY {
            log-warn "daily cap reached" {count: $daily, cap: $MAX_RUNS_PER_DAY}
            release-lock
            exit 4
        }

        log-info "starting run" {repos: $REPO_ALLOWLIST, daily_count: $daily, window_minutes: $POLL_WINDOW_MINUTES}

        for repo in $REPO_ALLOWLIST {
            let failures = (list-failing-prs $repo $config.forgejo_pat $POLL_WINDOW_MINUTES)
            log-info "found failing PRs" {repo: $repo, count: ($failures | length)}

            for failure in $failures {
                let current = (get-daily-count $state)
                if $current >= $MAX_RUNS_PER_DAY {
                    log-warn "daily cap reached mid-run, stopping" {count: $current}
                    break
                }
                $state = (handle-failure $repo $failure $config.forgejo_pat $state)
                save-state $state
            }
        }

        save-state $state
        release-lock
        log-info "run complete" {final_daily_count: (get-daily-count $state)}
    } catch { |e|
        log-error "unhandled error" {error: ($e | get msg)}
        release-lock
        exit 5
    }
}