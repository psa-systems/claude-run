# Read-only troubleshooting MCP server (CLAUDE-437)

`claude-run mcp stdio` is the only `mcp` subcommand, and it is DB-direct: it opens the configured `DbPool` the same way every other DB-direct subcommand does and needs no daemon running. Its one job is to let an MCP-speaking client (an editor, another agent) inspect this instance's own run history without ever being able to change it.

## Stdout carries only MCP frames

Over `mcp stdio`, stdout is the JSON-RPC transport (`rmcp::transport::stdio()`); nothing else may write to it. `main()` pre-scans `argv` for the literal `mcp stdio` positional pair, before clap parses `Cli::cmd`, and routes `init_tracing`'s writer to stderr in that case only, since the tracing subscriber is process-global and can only be installed once, ahead of parsing. Every other subcommand keeps logging to stdout as before.

## Tool set

`src/mcp.rs` registers exactly seven tools through rmcp's `#[tool_router]` / `#[tool]` macros, each annotated `read_only_hint = true, destructive_hint = false`: `get_status`, `list_runs`, `get_run`, `get_attempts`, `get_stream_log`, `list_ci_failures`, `get_ci_failure`. No tool executes a write; the read logic lives in `state.rs` and `ci_failure/store.rs` alongside the other read queries those modules already expose, not duplicated ad hoc in `mcp.rs`.

`get_status`'s block assembly (`state::status_blocks`) is shared with the CLI: `claude-run status`'s daemonless path calls the same function and layers its own `daemon` / `socket_expected_at` keys on top, while the MCP tool adds `"source": "database"` instead. The two can no longer drift in content.

A `limit` or `tail_lines` argument above its max errors rather than clamping, so a caller is told its request was too large instead of silently getting a truncated answer it might mistake for the whole picture.

## Stream log path safety

`get_stream_log` canonicalizes the recorded `stream_log_path` and verifies it resolves under the canonicalized logs directory before reading it, rejecting a symlink escape the same way a write path would have to. A row with no recorded path, a path outside the logs directory, and a file the retention sweep already removed are three distinct error cases, each with its own message, none of them a bare "not found".

## Dependency footprint

`rmcp` (server + macros + stdio transport features) and `schemars` are the only new dependencies. Pin and feature set live in `[dependencies.rmcp]` in `Cargo.toml`; a `[dev-dependencies.rmcp]` entry adds the `client` feature so the in-process client/server test can drive the server as a real MCP client would, without pulling that feature into the shipped binary. Neither crate introduces a second version of `sqlx`, `sqlx-sqlite`, `libsqlite3-sys`, or `axum`; check with `cargo tree --duplicates` after touching either dependency.
