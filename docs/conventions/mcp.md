# Read-only troubleshooting MCP server (CLAUDE-437, CLAUDE-442)

`claude-run mcp stdio` is the only `mcp` subcommand, and it is DB-direct: it opens the configured `DbPool` the same way every other DB-direct subcommand does and needs no daemon running. Its one job is to let an MCP-speaking client (an editor, another agent) inspect this instance's own run history without ever being able to change it. The same tool set is also reachable from a running daemon over Streamable HTTP; see "Serving the same tools over HTTP" below. There is still no second `mcp` subcommand: the HTTP path is a `serve`-time listener, not a new CLI entry point.

## Stdout carries only MCP frames

Over `mcp stdio`, stdout is the JSON-RPC transport (`rmcp::transport::stdio()`); nothing else may write to it. `main()` pre-scans `argv` for the literal `mcp stdio` positional pair, before clap parses `Cli::cmd`, and routes `init_tracing`'s writer to stderr in that case only, since the tracing subscriber is process-global and can only be installed once, ahead of parsing. Every other subcommand keeps logging to stdout as before.

## Tool set

`src/mcp.rs` registers exactly seven tools through rmcp's `#[tool_router]` / `#[tool]` macros, each annotated `read_only_hint = true, destructive_hint = false`: `get_status`, `list_runs`, `get_run`, `get_attempts`, `get_stream_log`, `list_ci_failures`, `get_ci_failure`. No tool executes a write; the read logic lives in `state.rs` and `ci_failure/store.rs` alongside the other read queries those modules already expose, not duplicated ad hoc in `mcp.rs`.

`get_status`'s block assembly (`state::status_blocks`) is shared with the CLI: `claude-run status`'s daemonless path calls the same function and layers its own `daemon` / `socket_expected_at` keys on top, while the MCP tool adds `"source": "database"` instead. The two can no longer drift in content.

A `limit` or `tail_lines` argument above its max errors rather than clamping, so a caller is told its request was too large instead of silently getting a truncated answer it might mistake for the whole picture.

## Stream log path safety

`get_stream_log` canonicalizes the recorded `stream_log_path` and verifies it resolves under the canonicalized logs directory before reading it, rejecting a symlink escape the same way a write path would have to. A row with no recorded path, a path outside the logs directory, and a file the retention sweep already removed are three distinct error cases, each with its own message, none of them a bare "not found".

## Dependency footprint

`rmcp` (server + macros + stdio and Streamable HTTP server transport features), `schemars`, and `mcp-web` are the only new dependencies the shipped binary carries. Pin and feature set live in `[dependencies.rmcp]` in `Cargo.toml`; a `[dev-dependencies.rmcp]` entry adds `client` and `transport-streamable-http-client-reqwest` so the client/server tests (both the in-process stdio one and the real-TCP HTTP one, CLAUDE-442) can drive the server as a real MCP client would, without pulling either feature into the shipped binary. The HTTP client feature brings `reqwest` and `wasm-streams` into `Cargo.lock` as transitive dev-only dependencies alongside it (the shipped binary already depends on its own, separately-configured `reqwest`, so this is not a second copy of anything shipped). Neither `rmcp` nor `schemars` introduces a second version of `sqlx`, `sqlx-sqlite`, `libsqlite3-sys`, or `axum`; check with `cargo tree --duplicates` after touching either dependency.

## Serving the same tools over HTTP (CLAUDE-442)

`src/mcp_http.rs` wraps the same `McpServer` (`src/mcp.rs`) in `rmcp`'s Streamable HTTP transport and nests it at `/mcp` on an opt-in listener spawned from `daemon.rs`'s `serve`, gated on `[mcp].enabled` (see [configuration.md](../configuration.md#mcp-over-http-claude-442)). `McpServer` itself needed no change for this: it was already transport-agnostic (`pool`, `paths`, `tool_router` only), so stdio and HTTP are two thin wrappers around one tool implementation that cannot drift apart.

**Auth model: a different gate than `[web]`.** The HTTP listener's bearer-token check is the external `mcp-web` crate's own `require_bearer` middleware, backed by `mcp-web`'s own OAuth 2.1 authorization server, local accounts and mandatory TOTP. This is deliberately a separate account store from claude-run's own `web_admin` / `web_accounts` (see [web-auth.md](web-auth.md)): an operator granting someone MCP access is a narrower, read-only grant than a full dashboard login, and the two should not be forced to share credentials or session state.

**Key files.** `mcp-master.key` (encrypts `mcp-web.db` at rest) and `mcp-session.key` (signs mcp-web's own sessions) are generated once under `CLAUDE_RUN_CONFIG_DIR` on first start with the listener enabled, mode `0600`, via `create_new` so an existing file is read rather than regenerated; a corrupt file fails startup loudly instead of silently rotating (the same `secrets.rs` discipline `secrets.key` already follows, applied to a second, unrelated key pair). The MCP account/session/OAuth state itself lives in its own SQLite file, `mcp-web.db` under `CLAUDE_RUN_DATA_DIR`, entirely separate from claude-run's own `state.db` or `[database]` backend; nothing about enabling this listener touches claude-run's own schema or migrations.

**Two account stores, by design.** A deployment can therefore have claude-run dashboard accounts (`web_admin`/`web_accounts`, TOTP mandatory per CLAUDE-368) and mcp-web MCP accounts (also TOTP mandatory) that are entirely disjoint; the same email can exist in both independently, logging into one grants nothing in the other.

**Upstream directory.** mcp-web's `UpstreamDirectory` trait exists so a product can gate which of its own upstream accounts an mcp-web account may act as. claude-run's tools act only on this instance's own local state (nothing per-upstream-account to gate), so `mcp_http::AlwaysConnected` implements it trivially: every mcp-web account is reported connected, and `connected_account_ids` is simply every row in mcp-web's own `accounts` table.

**Raw log content over the network.** `get_stream_log` returns a job's raw stream log verbatim (see "Stream log path safety" above), which can contain anything a `claude` child printed or a tool call echoed, including secrets a prompt mishandled. Over stdio that content never leaves the host; over this HTTP listener it is one authenticated tool call away from anyone mcp-web has issued a bearer token to. Treat an MCP HTTP grant as equivalent in sensitivity to a dashboard login, not as a lesser, "read-only so it's fine" grant, when deciding who gets one.
