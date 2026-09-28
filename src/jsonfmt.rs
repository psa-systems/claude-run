//! Syntax-highlighted JSON for human-facing CLI output.
//!
//! [`highlight`] takes the canonical [`serde_json::to_string_pretty`] string and
//! wraps each token in an [`anstyle`] style, copying all other bytes (whitespace,
//! punctuation, structure) verbatim. The styled string always carries ANSI
//! codes; it is meant to be written through `anstream` (see [`print_json`]),
//! which strips the codes when color is off (piped output, `NO_COLOR`,
//! `--color never`). Because only escape codes are added, stripping the result
//! yields exactly the original `to_string_pretty` bytes, so scripted consumers
//! (`claude-run status | from json`) are unaffected.
//!
//! It highlights the already-serialized string rather than walking a
//! [`serde_json::Value`] on purpose: serde_json is built here without
//! `preserve_order`, so round-tripping a struct through `Value` would re-sort
//! object keys and diverge from the current direct-struct output. Lexing the
//! canonical string keeps the bytes identical regardless of the source type.

use std::fmt::Write as _;

use anstyle::{AnsiColor, Style};
use serde::Serialize;

/// Object keys.
const KEY: Style = AnsiColor::Cyan.on_default();
/// String scalars.
const STRING: Style = AnsiColor::Green.on_default();
/// Numeric scalars.
const NUMBER: Style = AnsiColor::Yellow.on_default();
/// `true` / `false` / `null`.
const LITERAL: Style = AnsiColor::Magenta.on_default();

/// Serialize `value` to pretty JSON and return the syntax-highlighted form.
///
/// The result always contains ANSI escape codes; emit it through `anstream`
/// (e.g. [`print_json`]) so they are stripped when color is disabled. Stripping
/// the result yields exactly [`serde_json::to_string_pretty`] output.
pub fn to_colored_string<T: Serialize + ?Sized>(value: &T) -> serde_json::Result<String> {
    Ok(highlight(&serde_json::to_string_pretty(value)?))
}

/// Print `value` as highlighted pretty JSON plus a trailing newline, through
/// `anstream::stdout()` so ANSI codes are stripped when color is off. Drop-in
/// replacement for `println!("{}", serde_json::to_string_pretty(value)?)`.
pub fn print_json<T: Serialize + ?Sized>(value: &T) -> serde_json::Result<()> {
    anstream::println!("{}", to_colored_string(value)?);
    Ok(())
}

/// Wrap each JSON token in `pretty` with an [`anstyle`] color; copy everything
/// else byte-for-byte. `pretty` must be well-formed JSON as produced by
/// [`serde_json::to_string_pretty`].
pub fn highlight(pretty: &str) -> String {
    let mut out = String::with_capacity(pretty.len());
    let bytes = pretty.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'"' => {
                let end = string_end(bytes, i);
                let token = &pretty[i..end];
                // A string immediately followed by `:` is an object key.
                let style = if bytes.get(end) == Some(&b':') {
                    KEY
                } else {
                    STRING
                };
                paint(&mut out, style, token);
                i = end;
            }
            b'-' | b'0'..=b'9' => {
                let end = number_end(bytes, i);
                paint(&mut out, NUMBER, &pretty[i..end]);
                i = end;
            }
            b't' | b'f' | b'n' => {
                let end = literal_end(bytes, i);
                paint(&mut out, LITERAL, &pretty[i..end]);
                i = end;
            }
            // Whitespace, punctuation ({ } [ ] , :) pass through uncolored.
            _ => {
                out.push(bytes[i] as char);
                i += 1;
            }
        }
    }
    out
}

/// Index just past the closing quote of the JSON string starting at `start`.
fn string_end(bytes: &[u8], start: usize) -> usize {
    let mut i = start + 1;
    while i < bytes.len() {
        match bytes[i] {
            b'\\' => i += 2, // skip the escaped byte
            b'"' => return i + 1,
            _ => i += 1,
        }
    }
    bytes.len()
}

/// Index just past the JSON number starting at `start`.
fn number_end(bytes: &[u8], start: usize) -> usize {
    let mut i = start;
    while i < bytes.len() && matches!(bytes[i], b'-' | b'+' | b'.' | b'e' | b'E' | b'0'..=b'9') {
        i += 1;
    }
    i
}

/// Index just past the JSON literal (`true`/`false`/`null`) starting at `start`.
fn literal_end(bytes: &[u8], start: usize) -> usize {
    let mut i = start;
    while i < bytes.len() && bytes[i].is_ascii_lowercase() {
        i += 1;
    }
    i
}

fn paint(out: &mut String, style: Style, token: &str) {
    let _ = write!(out, "{}{token}{}", style.render(), style.render_reset());
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    /// Stripping the colored output must equal `to_string_pretty` exactly.
    fn assert_plain_matches(value: &serde_json::Value) {
        let colored = to_colored_string(value).unwrap();
        let stripped = anstream::adapter::strip_str(&colored).to_string();
        assert_eq!(stripped, serde_json::to_string_pretty(value).unwrap());
    }

    #[test]
    fn plain_mode_matches_to_string_pretty() {
        // Nested objects, arrays, every scalar type, and tricky escaping.
        assert_plain_matches(&json!({
            "name": "claude-run",
            "count": 3,
            "ratio": 1.5,
            "negative": -7,
            "exp": 1.0e10,
            "active": true,
            "inactive": false,
            "missing": null,
            "tags": ["a", "b", "c"],
            "nested": {
                "inner": [1, 2, {"deep": false}],
                "empty_obj": {},
                "empty_arr": []
            },
            // Escapes, a colon and braces inside a string value, and a digit-led
            // value that must not be mistaken for a number token.
            "escaped": "line1\nline2\ttab\"quote\"\\slash {x}: 5"
        }));
        // Bare scalars and empty containers at the top level.
        assert_plain_matches(&json!([]));
        assert_plain_matches(&json!({}));
        assert_plain_matches(&json!("just a string"));
        assert_plain_matches(&json!(42));
        assert_plain_matches(&json!(true));
        assert_plain_matches(&json!(null));
    }

    #[test]
    fn struct_output_matches_declaration_order() {
        // Structs serialize in declaration order (serde_json has no
        // preserve_order here); highlighting must not reorder keys.
        #[derive(Serialize)]
        struct Row {
            name: String,
            paused: bool,
            count: i64,
        }
        let row = Row {
            name: "triage".into(),
            paused: false,
            count: 7,
        };
        let colored = to_colored_string(&row).unwrap();
        let stripped = anstream::adapter::strip_str(&colored).to_string();
        assert_eq!(stripped, serde_json::to_string_pretty(&row).unwrap());
    }

    #[test]
    fn status_shaped_output_has_no_escapes_when_stripped() {
        // Mirrors the `status` (no daemon) report shape.
        let report = json!({
            "daemon": "not running",
            "socket_expected_at": "/run/user/1000/claude-run/daemon.sock",
            "global_paused": null,
            "actions": [
                {"name": "triage", "schedule": "*/5 * * * *", "paused": false}
            ]
        });
        let colored = to_colored_string(&report).unwrap();
        assert!(
            colored.contains('\x1b'),
            "colored output should carry ANSI codes"
        );
        let stripped = anstream::adapter::strip_str(&colored).to_string();
        assert!(
            !stripped.contains('\x1b'),
            "stripped (piped / --color never) output must contain zero escape bytes"
        );
        assert_eq!(stripped, serde_json::to_string_pretty(&report).unwrap());
    }
}
