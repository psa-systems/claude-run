#!/usr/bin/env nu
# claude-run.nu
#
# Generic, YAML-driven Claude Code runner.
#
# Usage:
#   claude-run.nu <action.yml>          # explicit path
#   claude-run.nu <action-name>         # resolved to $CLAUDE_RUN_ACTIONS_DIR/<name>.yml
#
# The action YAML defines the input command, prompt, and pre/post steps.
# The runner provides locking, state, daily caps, per-row attempt caps,
# Claude timeout, and a cost-based usage check.
#
# See actions/SCHEMA.md for the action YAML schema and exit codes.
#
# ----------------------------------------------------------------------------
# DEPENDENCIES
# ----------------------------------------------------------------------------
#   - nu (Nushell) 0.112.2+
#   - claude (Claude Code CLI), authenticated on this host
#   - timeout (from coreutils)
#   - Whatever CLIs the action snippets invoke (yt, fj, git, ...)
#
# ----------------------------------------------------------------------------
# EXIT CODES
# ----------------------------------------------------------------------------
#   0 - ran successfully (with or without finding work)
#   1 - paused (PAUSED file present)
#   2 - lock contention (previous run still going)
#   3 - config error (action YAML missing or invalid)
#   4 - daily cap reached
#   5 - unexpected error (see logs)
#   6 - over daily budget
#
# ----------------------------------------------------------------------------
# NAMING CONVENTION
# ----------------------------------------------------------------------------
# Function names follow nu's noun-verb grouping convention (`url join`,
# `path expand`, `str trim`). The noun comes first so commands with the same
# subject cluster together when scanned. Logging is a true subcommand group:
# `log info`, `log warn`, `log error`.

# ============================================================================
# ENV DEFAULTS
# ============================================================================

def env-default [name: string, default: string] {
    let v = ($env | get -o $name)
    if ($v | is-empty) { $default } else { $v }
}

def env-default-int [name: string, default: int] {
    let v = ($env | get -o $name)
    if ($v | is-empty) { $default } else { ($v | into int) }
}

def env-default-float [name: string, default: float] {
    let v = ($env | get -o $name)
    if ($v | is-empty) { $default } else { ($v | into float) }
}

def runner-config [] {
    let state_dir = (env-default "CLAUDE_RUN_STATE_DIR" "~/.local/share/claude-run" | path expand)
    {
        state_dir: $state_dir,
        log_dir: (env-default "CLAUDE_RUN_LOG_DIR" $"($state_dir)/logs" | path expand),
        actions_dir: (env-default "CLAUDE_RUN_ACTIONS_DIR" "./actions" | path expand),
        workdir_base: (env-default "CLAUDE_RUN_WORKDIR_BASE" "/tmp/claude-run"),
        max_runs_per_day: (env-default-int "CLAUDE_RUN_MAX_RUNS_PER_DAY" 20),
        max_attempts_per_key: (env-default-int "CLAUDE_RUN_MAX_ATTEMPTS_PER_KEY" 3),
        claude_timeout_seconds: (env-default-int "CLAUDE_RUN_CLAUDE_TIMEOUT_SECONDS" 600),
        daily_budget_usd: (env-default-float "CLAUDE_RUN_DAILY_BUDGET_USD" 5.00),
    }
}

# ============================================================================
# UTILITIES
# ============================================================================

def now-iso [] {
    date now | format date "%Y-%m-%dT%H:%M:%S%z"
}

def today-date [] {
    date now | format date "%Y-%m-%d"
}

def dir-ensure [p: string] {
    if not ($p | path exists) {
        mkdir $p
    }
}

# Resolve a path string. Leading `$` means treat the value as a nu expression
# (e.g. `$nu.home-dir`, `$"($env.HOME)/foo"`, `$nu.home-dir | path join workspace`)
# and use its evaluated result. Otherwise treat as a literal path. Either way
# the result is `path expand`-ed so `~` still works.
def path-resolve [raw: string] {
    let resolved = if ($raw | str starts-with "$") {
        let r = (^nu --commands $raw | complete)
        if $r.exit_code != 0 {
            error make {msg: $"path expression failed: ($raw) - ($r.stderr)"}
        }
        $r.stdout | str trim
    } else {
        $raw
    }
    $resolved | path expand
}

# ============================================================================
# LOGGING (nu subcommand group: `log info`, `log warn`, `log error`)
# ============================================================================

def log [cfg: record, action_name: string, level: string, message: string, extra: record = {}] {
    let dir = $"($cfg.log_dir)/($action_name)"
    dir-ensure $dir
    let log_file = $"($dir)/(today-date).log"
    let entry = ({ts: (now-iso), level: $level, message: $message} | merge $extra)
    let line = ($entry | to json --raw)
    $"($line)\n" | save --append $log_file
    print $line
}

def "log info"  [cfg: record, name: string, msg: string, extra: record = {}] { log $cfg $name "info"  $msg $extra }
def "log warn"  [cfg: record, name: string, msg: string, extra: record = {}] { log $cfg $name "warn"  $msg $extra }
def "log error" [cfg: record, name: string, msg: string, extra: record = {}] { log $cfg $name "error" $msg $extra }

# ============================================================================
# LOCK
# ============================================================================

def lock-acquire [cfg: record, name: string] {
    let lock_dir = $"($cfg.state_dir)/locks"
    dir-ensure $lock_dir
    let lock = $"($lock_dir)/($name).lock"
    if ($lock | path exists) {
        let pid = (try { open $lock | str trim | into int } catch { 0 })
        if $pid > 0 {
            let running = (try { ps | where pid == $pid | length } catch { 0 })
            if $running > 0 {
                return false
            }
        }
        log warn $cfg $name "removing stale lock file" {stale_pid: $pid}
        rm $lock
    }
    # Stale lock cleared above; if anything else is racing we want save to fail.
    $"($nu.pid)" | save $lock
    return true
}

def lock-release [cfg: record, name: string] {
    let lock = $"($cfg.state_dir)/locks/($name).lock"
    if ($lock | path exists) {
        rm $lock
    }
}

# ============================================================================
# STATE
# ============================================================================

def state-file [cfg: record, name: string] {
    $"($cfg.state_dir)/state/($name).json"
}

def state-load [cfg: record, name: string] {
    let f = (state-file $cfg $name)
    if not ($f | path exists) {
        return {attempts: {}, daily_count: {}, daily_cost_usd: {}}
    }
    open $f
}

def state-save [cfg: record, name: string, state: record] {
    let f = (state-file $cfg $name)
    dir-ensure ($f | path dirname)
    if ($f | path exists) { rm $f }
    $state | to json | save $f
}

def attempts-get [state: record, key: string] {
    let a = ($state.attempts | get -o $key)
    if $a == null { 0 } else { $a.count }
}

def attempt-record [state: record, key: string] {
    let prev = (attempts-get $state $key)
    let updated = ($state.attempts | upsert $key {count: ($prev + 1), last_at: (now-iso)})
    $state | upsert attempts $updated
}

def daily-count-get [state: record] {
    let today = (today-date)
    let c = ($state.daily_count | get -o $today)
    if $c == null { 0 } else { $c }
}

def daily-count-increment [state: record] {
    let today = (today-date)
    let updated = ($state.daily_count | upsert $today ((daily-count-get $state) + 1))
    $state | upsert daily_count $updated
}

def daily-cost-get [state: record] {
    let today = (today-date)
    let c = ($state.daily_cost_usd | get -o $today)
    if $c == null { 0.0 } else { ($c | into float) }
}

def daily-cost-add [state: record, amount: float] {
    let today = (today-date)
    let updated = ($state.daily_cost_usd | upsert $today ((daily-cost-get $state) + $amount))
    $state | upsert daily_cost_usd $updated
}

# ============================================================================
# ACTION YAML LOADING
# ============================================================================

def action-path-resolve [cfg: record, arg: string] {
    if ($arg | path exists) {
        return ($arg | path expand)
    }
    let in_dir = $"($cfg.actions_dir)/($arg).yml"
    if ($in_dir | path exists) {
        return ($in_dir | path expand)
    }
    let in_dir_yaml = $"($cfg.actions_dir)/($arg).yaml"
    if ($in_dir_yaml | path exists) {
        return ($in_dir_yaml | path expand)
    }
    return null
}

def action-load [path: string] {
    let raw = (open $path)
    if ($raw.name? | is-empty) {
        error make {msg: $"action YAML missing required field: name (($path))"}
    }
    if ($raw.input?.command? | is-empty) {
        error make {msg: $"action YAML missing required field: input.command (($path))"}
    }
    if ($raw.prompt? | is-empty) {
        error make {msg: $"action YAML missing required field: prompt (($path))"}
    }
    $raw
}

def limits-effective [cfg: record, action: record] {
    let l = ($action.limits? | default {})
    {
        max_attempts_per_key: ($l.max_attempts_per_key? | default $cfg.max_attempts_per_key),
        max_runs_per_day: ($l.max_runs_per_day? | default $cfg.max_runs_per_day),
        claude_timeout_seconds: ($l.claude_timeout_seconds? | default $cfg.claude_timeout_seconds),
        daily_budget_usd: ($l.daily_budget_usd? | default $cfg.daily_budget_usd),
    }
}

# ============================================================================
# SNIPPET EXECUTION
# ============================================================================

# Build the prelude that injects bindings into a snippet's scope. Each binding
# is written to a temp JSON file and opened in the prelude, which avoids any
# quoting issues with embedding values inside nu source.
def snippet-write [
    body: string,
    bindings: record,
    snippet_dir: string,
    phase: string,
] {
    dir-ensure $snippet_dir
    mut prelude = ""
    for col in ($bindings | columns) {
        let val_path = $"($snippet_dir)/binding-($col).json"
        if ($val_path | path exists) { rm $val_path }
        ($bindings | get $col) | to json | save $val_path
        $prelude = $"($prelude)let ($col) = \(open --raw '($val_path)' | from json\)\n"
    }
    let script = $"($prelude)\n($body)"
    let script_path = $"($snippet_dir)/($phase).nu"
    if ($script_path | path exists) { rm $script_path }
    $script | save $script_path
    $script_path
}

# Run a snippet, returning a {ok, stdout, stderr, exit_code} record.
def snippet-run [
    body: string,
    bindings: record,
    snippet_dir: string,
    phase: string,
] {
    let script_path = (snippet-write $body $bindings $snippet_dir $phase)
    let result = (^nu $script_path | complete)
    {
        ok: ($result.exit_code == 0),
        stdout: ($result.stdout | default ""),
        stderr: ($result.stderr | default ""),
        exit_code: $result.exit_code,
    }
}

# ============================================================================
# CLAUDE INVOCATION
# ============================================================================

def claude-invoke [
    prompt: string,
    claude_cfg: record,
    timeout_s: int,
    default_cwd: string,
] {
    let cwd = ($claude_cfg.cwd? | default $default_cwd)
    let allowed_tools = ($claude_cfg.allowed_tools? | default "Read,Edit,Write,Bash,Glob,Grep")
    let permission_mode = ($claude_cfg.permission_mode? | default "acceptEdits")
    let output_format = ($claude_cfg.output_format? | default "json")

    cd $cwd

    let result = (do {
        ^timeout $timeout_s claude --print --permission-mode $permission_mode --allowed-tools $allowed_tools --output-format $output_format $prompt
    } | complete)

    if $result.exit_code != 0 {
        let stderr_text = ($result.stderr | default "")
        let stdout_text = ($result.stdout | default "")
        let combined = $"($stderr_text)\n($stdout_text)"
        let is_rate_limit = ($combined | str contains --ignore-case "rate limit") or ($combined | str contains --ignore-case "usage limit")
        let is_auth = ($combined | str contains --ignore-case "unauthorized") or ($combined | str contains --ignore-case "authentication")
        return {
            ok: false,
            summary: $combined,
            cost: 0.0,
            raw: {},
            error: $"claude exited ($result.exit_code)",
            transient: ($is_rate_limit or $is_auth),
        }
    }

    let parsed = (try { $result.stdout | from json } catch {
        return {
            ok: false,
            summary: ($result.stdout | default ""),
            cost: 0.0,
            raw: {},
            error: "claude output was not valid JSON",
            transient: false,
        }
    })

    {
        ok: (not ($parsed.is_error? | default false)),
        summary: ($parsed.result? | default ""),
        cost: ($parsed.total_cost_usd? | default 0.0 | into float),
        raw: $parsed,
        error: ($parsed.error? | default ""),
        transient: false,
    }
}

# ============================================================================
# PER-ROW PIPELINE
# ============================================================================

def row-key [action: record, row: any, idx: int] {
    let f = ($action.dedup_key? | default "")
    if ($f | is-empty) {
        return $"row-($idx)"
    }
    let v = (try { $row | get $f } catch { null })
    if $v == null {
        return $"row-($idx)"
    }
    ($v | into string)
}

def output-record-append [cfg: record, action: record, record_data: record] {
    let f = ($action.output?.file? | default "")
    if ($f | is-empty) { return }
    let dir = $"($cfg.log_dir)/($action.name)"
    dir-ensure $dir
    let path = $"($dir)/($f)"
    let line = ($record_data | to json --raw)
    $"($line)\n" | save --append $path
}

def row-process [
    cfg: record,
    action: record,
    limits: record,
    row: any,
    idx: int,
    state: record,
] {
    let name = $action.name
    let key = (row-key $action $row $idx)
    let attempts = (attempts-get $state $key)

    if $attempts >= $limits.max_attempts_per_key and ($action.dedup_key? | is-not-empty) {
        log info $cfg $name "skipping row, attempt cap reached" {key: $key, attempts: $attempts}
        return $state
    }

    log info $cfg $name "processing row" {key: $key, attempt: ($attempts + 1)}

    let workdir_base = (path-resolve ($action.workdir_base? | default $cfg.workdir_base))
    let workdir = $"($workdir_base)/($name)/($key)"
    let snippet_dir = $"($cfg.state_dir)/snippets/($name)/($key)"
    let base_bindings = {row: $row, workdir: $workdir}

    # ---- filter ----
    if ($action.filter? | is-not-empty) {
        let r = (snippet-run $action.filter $base_bindings $snippet_dir "filter")
        if not $r.ok {
            log info $cfg $name "row filtered out" {key: $key, stderr: $r.stderr, exit_code: $r.exit_code}
            return $state
        }
    }

    dir-ensure $workdir

    # ---- pre steps ----
    mut step_failed = false
    let pre_steps = ($action.pre? | default [])
    for i in 0..<($pre_steps | length) {
        if $step_failed { break }
        let step = ($pre_steps | get $i)
        let r = (snippet-run $step $base_bindings $snippet_dir $"pre-($i)")
        if not $r.ok {
            log warn $cfg $name "pre step failed" {key: $key, step: $i, exit_code: $r.exit_code, stderr: $r.stderr}
            output-record-append $cfg $action {ts: (now-iso), key: $key, attempt: ($attempts + 1), phase: $"pre-($i)", ok: false, stderr: $r.stderr}
            $step_failed = true
        }
    }
    if $step_failed {
        return (attempt-record $state $key)
    }

    # ---- prompt ----
    let prompt_r = (snippet-run $action.prompt $base_bindings $snippet_dir "prompt")
    if not $prompt_r.ok {
        log warn $cfg $name "prompt snippet failed" {key: $key, exit_code: $prompt_r.exit_code, stderr: $prompt_r.stderr}
        output-record-append $cfg $action {ts: (now-iso), key: $key, attempt: ($attempts + 1), phase: "prompt", ok: false, stderr: $prompt_r.stderr}
        return (attempt-record $state $key)
    }
    let prompt = ($prompt_r.stdout | str trim)
    if ($prompt | is-empty) {
        log warn $cfg $name "prompt snippet produced empty output" {key: $key}
        output-record-append $cfg $action {ts: (now-iso), key: $key, attempt: ($attempts + 1), phase: "prompt", ok: false, stderr: "empty prompt"}
        return (attempt-record $state $key)
    }

    # ---- claude ----
    let claude_cfg = ($action.claude? | default {})
    let claude_result = (claude-invoke $prompt $claude_cfg $limits.claude_timeout_seconds $workdir)

    mut state2 = $state
    if $claude_result.transient {
        log info $cfg $name "transient claude error, not counting attempt" {key: $key, error: $claude_result.error}
        return $state2
    }

    $state2 = (daily-cost-add $state2 $claude_result.cost)

    if not $claude_result.ok {
        log warn $cfg $name "claude run failed" {key: $key, error: $claude_result.error}
        output-record-append $cfg $action {ts: (now-iso), key: $key, attempt: ($attempts + 1), phase: "claude", ok: false, error: $claude_result.error, summary: $claude_result.summary, cost: $claude_result.cost}
        return (daily-count-increment (attempt-record $state2 $key))
    }

    log info $cfg $name "claude completed" {key: $key, cost: $claude_result.cost}

    # ---- post steps ----
    let post_bindings = ($base_bindings | merge {claude_result: $claude_result})
    let post_steps = ($action.post? | default [])
    mut post_failed = false
    for i in 0..<($post_steps | length) {
        if $post_failed { break }
        let step = ($post_steps | get $i)
        let r = (snippet-run $step $post_bindings $snippet_dir $"post-($i)")
        if not $r.ok {
            log warn $cfg $name "post step failed" {key: $key, step: $i, exit_code: $r.exit_code, stderr: $r.stderr}
            output-record-append $cfg $action {ts: (now-iso), key: $key, attempt: ($attempts + 1), phase: $"post-($i)", ok: false, stderr: $r.stderr}
            $post_failed = true
        }
    }

    output-record-append $cfg $action {
        ts: (now-iso),
        key: $key,
        attempt: ($attempts + 1),
        phase: (if $post_failed { "post" } else { "done" }),
        ok: (not $post_failed),
        claude_ok: $claude_result.ok,
        claude_cost: $claude_result.cost,
        summary: $claude_result.summary,
    }

    daily-count-increment (attempt-record $state2 $key)
}

# ============================================================================
# INPUT
# ============================================================================

def rows-gather [cfg: record, action: record] {
    let snippet_dir = $"($cfg.state_dir)/snippets/($action.name)/_input"
    let r = (snippet-run $action.input.command {} $snippet_dir "input")
    if not $r.ok {
        log error $cfg $action.name "input.command failed" {exit_code: $r.exit_code, stderr: $r.stderr}
        error make {msg: $"input.command exited ($r.exit_code)"}
    }
    let parsed = (try { $r.stdout | from json } catch { |e|
        log error $cfg $action.name "input.command output was not valid JSON" {error: ($e | get msg), stdout: $r.stdout}
        error make {msg: "input.command stdout is not JSON"}
    })
    let d = ($parsed | describe)
    if ($d | str starts-with "list") or ($d | str starts-with "table") {
        return $parsed
    }
    log error $cfg $action.name "input.command output was not a JSON array" {describe: $d}
    error make {msg: "input.command stdout must be a JSON array"}
}

# ============================================================================
# MAIN
# ============================================================================

def main [
	action_arg: string			# Action to run from actions/
] {
    let cfg = (runner-config)
    dir-ensure $cfg.state_dir
    dir-ensure $cfg.log_dir

    let action_path = (action-path-resolve $cfg $action_arg)
    if $action_path == null {
        print --stderr $"action not found: ($action_arg) \(checked path + ($cfg.actions_dir)\)"
        exit 3
    }

    let action = (try { action-load $action_path } catch { |e|
        print --stderr $"action YAML invalid: ($e | get msg)"
        exit 3
    })
    let name = $action.name
    let limits = (limits-effective $cfg $action)

    # ---- pause checks ----
    let global_pause = $"($cfg.state_dir)/PAUSED"
    let action_pause = $"($cfg.state_dir)/($name).PAUSED"
    if ($global_pause | path exists) {
        log info $cfg $name "global PAUSED, exiting" {}
        exit 1
    }
    if ($action_pause | path exists) {
        log info $cfg $name "action PAUSED, exiting" {}
        exit 1
    }

    # ---- lock ----
    if not (lock-acquire $cfg $name) {
        log info $cfg $name "previous run still going, exiting" {}
        exit 2
    }

    try {
        mut state = (state-load $cfg $name)

        # ---- daily cap ----
        let daily = (daily-count-get $state)
        if $daily >= $limits.max_runs_per_day {
            log warn $cfg $name "daily cap reached" {count: $daily, cap: $limits.max_runs_per_day}
            lock-release $cfg $name
            exit 4
        }

        # ---- budget ----
        let cost_today = (daily-cost-get $state)
        if $cost_today >= $limits.daily_budget_usd {
            log warn $cfg $name "daily budget reached" {cost_usd: $cost_today, budget_usd: $limits.daily_budget_usd}
            lock-release $cfg $name
            exit 6
        }

        log info $cfg $name "starting run" {action_path: $action_path, daily_count: $daily, cost_usd: $cost_today, budget_usd: $limits.daily_budget_usd}

        # ---- gather input ----
        let rows = (rows-gather $cfg $action)
        log info $cfg $name "input rows gathered" {count: ($rows | length)}

        # ---- iterate ----
        for idx in 0..<($rows | length) {
            let current_count = (daily-count-get $state)
            if $current_count >= $limits.max_runs_per_day {
                log warn $cfg $name "daily cap reached mid-run, stopping" {count: $current_count}
                break
            }
            let current_cost = (daily-cost-get $state)
            if $current_cost >= $limits.daily_budget_usd {
                log warn $cfg $name "daily budget reached mid-run, stopping" {cost_usd: $current_cost}
                break
            }

            let row = ($rows | get $idx)
            $state = (row-process $cfg $action $limits $row $idx $state)
            state-save $cfg $name $state
        }

        state-save $cfg $name $state
        lock-release $cfg $name
        log info $cfg $name "run complete" {final_daily_count: (daily-count-get $state), final_daily_cost_usd: (daily-cost-get $state)}
    } catch { |e|
        log error $cfg $name "unhandled error" {error: ($e | get msg)}
        lock-release $cfg $name
        exit 5
    }
}
