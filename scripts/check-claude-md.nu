#!/usr/bin/env nu
# Guard CLAUDE.md against regrowth (CLAUDE-370): a byte cap, a per-line cap outside
# fenced code, and every docs/ link and anchor from it must resolve.

const MAX_BYTES = 20000
const MAX_LINE = 400

# GitHub/Forgejo-style heading anchor.
def slug [heading: string] {
    $heading | str trim | str downcase
    | str replace --all --regex '[^\w\- ]' ''
    | str replace --all ' ' '-'
}

# Lines outside ``` / ~~~ fences, as records of line number and text.
def unfenced [text: string] {
    let rows = ($text | lines | enumerate)
    let fence_flags = ($rows | each {|r|
        let t = ($r.item | str trim)
        ($t starts-with '```') or ($t starts-with '~~~')
    })
    $rows | zip $fence_flags | reduce --fold {inside: false, out: []} {|pair, acc|
        let row = $pair.0
        if $pair.1 {
            {inside: (not $acc.inside), out: $acc.out}
        } else if $acc.inside {
            $acc
        } else {
            {inside: false, out: ($acc.out | append {line: ($row.index + 1), text: $row.item})}
        }
    } | get out
}

# Anchors a markdown file defines, with -1, -2 suffixes for repeated headings.
def anchors [text: string] {
    unfenced $text
    | where {|r| $r.text =~ '^#{1,6} ' }
    | each {|r| slug ($r.text | str replace --regex '^#{1,6} ' '') }
    | reduce --fold {seen: {}, out: []} {|s, acc|
        let n = ($acc.seen | get --optional $s | default 0)
        let name = if $n == 0 { $s } else { $"($s)-($n)" }
        {seen: ($acc.seen | upsert $s ($n + 1)), out: ($acc.out | append $name)}
    } | get out
}

# Every violation in <root>/CLAUDE.md, as a list of strings.
def violations [root: path] {
    let file = ($root | path join CLAUDE.md)
    if not ($file | path exists) { return ["CLAUDE.md is missing"] }
    let text = (open --raw $file)
    let size = ($text | encode utf-8 | bytes length)
    let body = (unfenced $text)

    let size_err = if $size > $MAX_BYTES { [$"CLAUDE.md is ($size) bytes, over the ($MAX_BYTES)-byte cap"] } else { [] }
    let line_errs = ($body | where {|r| ($r.text | str length) > $MAX_LINE }
        | each {|r| $"CLAUDE.md:($r.line) is ($r.text | str length) characters, over the ($MAX_LINE)-character cap" })

    let link_errs = ($body | each {|r|
        $r.text | parse --regex '\]\((?<target>docs/[^)\s#]*)(?:#(?<anchor>[^)\s]*))?\)'
        | each {|m|
            let target = ($root | path join $m.target)
            if not ($target | path exists) {
                $"CLAUDE.md:($r.line) links missing file ($m.target)"
            } else if ($m.anchor | is-not-empty) and ($m.anchor not-in (anchors (open --raw $target))) {
                $"CLAUDE.md:($r.line) links missing anchor ($m.target)#($m.anchor)"
            }
        }
    } | flatten | compact)

    $size_err ++ $line_errs ++ $link_errs
}

# Fixture tree for the self-test: a CLAUDE.md plus docs/conventions/a.md.
def fixture [claude_md: string] {
    let dir = (^mktemp --directory --tmpdir claude-md-guard.XXXXXX | str trim)
    mkdir ($dir | path join docs conventions)
    "# A\n\n## Some rule (CLAUDE-1)\n\ntext\n\n```\n## Not a heading\n```\n\n## Some rule (CLAUDE-1)\n" | save ($dir | path join docs conventions a.md)
    $claude_md | save ($dir | path join CLAUDE.md)
    $dir
}

def expect [name: string, claude_md: string, pattern: string] {
    let dir = (fixture $claude_md)
    let errs = (violations $dir)
    rm --recursive $dir
    let ok = if ($pattern | is-empty) { $errs | is-empty } else { ($errs | length) == 1 and ($errs.0 =~ $pattern) }
    if not $ok {
        error make {msg: $"self-test '($name)' failed: expected '($pattern)', got ($errs | to nuon)"}
    }
    print $"ok: ($name)"
}

def self_test [] {
    let good = "# X\n\n- rule ([CLAUDE-1](docs/conventions/a.md#some-rule-claude-1))\n- dup ([d](docs/conventions/a.md#some-rule-claude-1-1))\n"
    expect "clean file passes" $good ""
    expect "size cap" ($good + (1..60 | each { "" | fill --character 'x' --width 399 } | str join "\n")) "bytes, over"
    expect "line cap" ($good + ("" | fill --character 'y' --width 401)) "CLAUDE.md:5 is 401 characters"
    expect "fenced long line is exempt" ($good + "```\n" + ("" | fill --character 'z' --width 900) + "\n```\n") ""
    expect "missing file" ($good + "- [b](docs/conventions/b.md)\n") "missing file docs/conventions/b.md"
    expect "missing anchor" ($good + "- [c](docs/conventions/a.md#nope)\n") "missing anchor docs/conventions/a.md#nope"
    expect "fenced heading is not an anchor" ($good + "- [c](docs/conventions/a.md#not-a-heading)\n") "missing anchor"
    print "self-test passed"
}

def main [
    --self-test  # run the guard against built-in fixtures instead of the repo
] {
    if $self_test { self_test; return }
    let root = ($env.FILE_PWD | path dirname)
    let errs = (violations $root)
    if ($errs | is-not-empty) {
        $errs | each {|e| print --stderr $"FAIL: ($e)" } | ignore
        exit 1
    }
    let size = (open --raw ($root | path join CLAUDE.md) | encode utf-8 | bytes length)
    print $"CLAUDE.md ok: ($size) bytes, lines and docs/ links within limits"
}
