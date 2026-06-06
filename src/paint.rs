//! Shared color palette and helpers for human-facing CLI messages (CLAUDE-60).
//!
//! One place for the success / notice / error palette so the binary and
//! [`crate::update`] style their output identically. Styles are emitted
//! unconditionally as ANSI codes; the strings are written through `anstream`,
//! which strips them when color is off (piped output, `NO_COLOR`,
//! `--color never`). JSON highlighting has its own palette in [`crate::jsonfmt`].

use anstyle::{AnsiColor, Style};

/// Success / confirmation lines (`ok`, `created secret`, `up to date`).
pub const SUCCESS: Style = AnsiColor::Green.on_default();
/// Non-fatal notices (`daemon not running`, `update available`, `downloading`).
pub const NOTICE: Style = AnsiColor::Yellow.on_default();
/// Errors on stderr.
pub const ERROR: Style = AnsiColor::Red.on_default().bold();
/// Emphasis for the value in a neutral key/value line (default fg, bold).
pub const VALUE: Style = Style::new().bold();

/// Wrap `text` in `style`'s ANSI codes. Always emits codes; rely on `anstream`
/// to strip them when color is disabled.
pub fn paint(style: Style, text: &str) -> String {
    format!("{}{text}{}", style.render(), style.render_reset())
}

/// Print a success line (green) to stdout via `anstream`.
pub fn success(msg: &str) {
    anstream::println!("{}", paint(SUCCESS, msg));
}

/// Print a notice line (yellow) to stdout via `anstream`.
pub fn notice(msg: &str) {
    anstream::println!("{}", paint(NOTICE, msg));
}

/// Print an error line (bold red) to stderr via `anstream`.
pub fn error(msg: &str) {
    anstream::eprintln!("{}", paint(ERROR, msg));
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn paint_wraps_text_and_resets() {
        let painted = paint(SUCCESS, "ok");
        assert!(painted.contains('\x1b'), "should carry ANSI codes");
        assert_eq!(anstream::adapter::strip_str(&painted).to_string(), "ok");
    }
}
