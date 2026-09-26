#!/usr/bin/env nu
# Remove claude-run workdir entries from the "projects" map of ~/.claude.json.
# Backs the file up first and aborts if it changes while the script runs.

def main [
    --file: path                                 # file to prune (default: ~/.claude.json)
    --pattern: string = "/claude-run/workdirs/"  # substring that marks a workdir project key
    --dry-run                                    # report only, write nothing
] {
    let target = ($file | default ($nu.home-dir | path join ".claude.json") | path expand)
    if not ($target | path exists) {
        error make { msg: $"file not found: ($target)" }
    }

    let modified_before = (ls $target | get 0.modified)
    let doc = (open --raw $target | from json)
    let projects = ($doc | get --optional projects)
    if $projects == null {
        error make { msg: $"no 'projects' key in ($target)" }
    }

    let keys = ($projects | columns)
    let stale = ($keys | where { |key| $key | str contains $pattern })
    print $"($target): ($keys | length) projects, ($stale | length) match '($pattern)', (($keys | length) - ($stale | length)) kept"
    if ($stale | is-empty) {
        print "nothing to prune"
        return
    }
    if $dry_run {
        print "dry run, nothing written"
        return
    }

    let stamp = (date now | format date "%Y%m%d-%H%M%S")
    let backup = $"($target).bak-($stamp)"
    let tmp = $"($target).tmp-($stamp)"
    cp $target $backup

    # Rebuild the map instead of `reject`: keys contain dots, which cell paths split on.
    let kept = (
        $projects
        | items { |key, value| { key: $key, value: $value } }
        | where { |row| not ($row.key | str contains $pattern) }
        | reduce --fold {} { |row, acc| $acc | merge { ($row.key): $row.value } }
    )
    $doc | update projects $kept | to json --indent 2 | save $tmp

    # A running claude rewrites this file; do not clobber its newer write.
    if (ls $target | get 0.modified) != $modified_before {
        rm $tmp
        error make { msg: $"($target) changed while pruning; nothing replaced. Backup: ($backup)" }
    }

    ^chmod --reference $target $tmp
    if $env.LAST_EXIT_CODE != 0 {
        rm $tmp
        error make { msg: $"could not copy permissions onto ($tmp); nothing replaced" }
    }
    mv $tmp $target
    print $"pruned ($stale | length) entries. Backup: ($backup)"
}
