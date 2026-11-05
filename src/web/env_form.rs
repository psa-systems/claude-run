//! The Environment form on a scripted action's edit page (CLAUDE-571,
//! SPEC-4.3.3): change the values of the action's `env` block without
//! editing YAML.
//!
//! The file is the operator's, comments included (the shipped examples
//! explain each key in a comment above it), so saving rewrites only the value
//! of each changed key, in place. Every other line, key order and comment
//! stays byte for byte as it was. The caller validates the result like any
//! other edit and checks its parsed `env` equals what was submitted.

use std::collections::BTreeMap;

/// Leading spaces of `line`.
fn indent(line: &str) -> usize {
    line.len() - line.trim_start_matches(' ').len()
}

/// The key a `KEY: value` line at the block's key indent names.
fn key_of(line: &str) -> Option<&str> {
    let rest = line.trim_start_matches(' ');
    let (key, _) = rest.split_once(':')?;
    (!key.is_empty() && key.chars().all(|c| c.is_ascii_alphanumeric() || c == '_')).then_some(key)
}

/// `value` as YAML text following `KEY: `, with lines after the first
/// indented to `child_indent`. A multi-line value is a literal block scalar
/// when it can be one; anything else is a JSON string, which is always a
/// valid YAML double-quoted scalar.
fn render_value(value: &str, child_indent: usize) -> String {
    let block_safe = value.contains('\n')
        && !value.starts_with('\n')
        && value
            .lines()
            .all(|l| !l.starts_with([' ', '\t']) && !l.ends_with([' ', '\t']));
    if !block_safe {
        return format!(
            "{}\n",
            serde_json::to_string(value).expect("a string serializes")
        );
    }
    let chomp = if value.ends_with('\n') { "|" } else { "|-" };
    let pad = " ".repeat(child_indent);
    let mut out = format!("{chomp}\n");
    for line in value.trim_end_matches('\n').split('\n') {
        if line.is_empty() {
            out.push('\n');
        } else {
            out.push_str(&pad);
            out.push_str(line);
            out.push('\n');
        }
    }
    out
}

/// Rewrite the values of `changes` in the top-level `env:` block of `yaml`,
/// leaving every other line as it was. Each key must already be in the
/// block: adding or removing keys stays a raw-YAML edit.
pub fn set_env_values(yaml: &str, changes: &BTreeMap<String, String>) -> Result<String, String> {
    let lines: Vec<&str> = yaml.split_inclusive('\n').collect();
    let start = lines
        .iter()
        .position(|l| l.trim_end() == "env:")
        .ok_or("the action has no top-level `env:` block")?;
    // The block runs to the next top-level line (a key, not a comment or a
    // blank line).
    let end = lines[start + 1..]
        .iter()
        .position(|l| {
            let t = l.trim_end();
            !t.is_empty() && !t.starts_with([' ', '\t', '#'])
        })
        .map_or(lines.len(), |p| start + 1 + p);
    let key_indent = lines[start + 1..end]
        .iter()
        .find(|l| {
            let t = l.trim();
            !t.is_empty() && !t.starts_with('#')
        })
        .map(|l| indent(l))
        .ok_or("the `env:` block is empty")?;

    let mut out: Vec<String> = lines[..=start].iter().map(|l| l.to_string()).collect();
    let mut seen = Vec::new();
    let mut i = start + 1;
    while i < end {
        let line = lines[i];
        let key = (indent(line) == key_indent && !line.trim_start().starts_with('#'))
            .then(|| key_of(line))
            .flatten();
        let Some(key) = key else {
            out.push(line.to_string());
            i += 1;
            continue;
        };
        // The value's own lines: everything indented deeper, blank lines
        // inside a block scalar included.
        let mut last = i;
        let mut j = i + 1;
        while j < end {
            if lines[j].trim().is_empty() {
                j += 1;
            } else if indent(lines[j]) > key_indent {
                last = j;
                j += 1;
            } else {
                break;
            }
        }
        match changes.get(key) {
            Some(value) => {
                seen.push(key.to_string());
                out.push(format!(
                    "{}{key}: {}",
                    " ".repeat(key_indent),
                    render_value(value, key_indent + 2)
                ));
            }
            None => out.extend(lines[i..=last].iter().map(|l| l.to_string())),
        }
        i = last + 1;
    }
    if let Some(unknown) = changes.keys().find(|k| !seen.contains(k)) {
        return Err(format!("`{unknown}` is not in the action's `env:` block"));
    }
    out.extend(lines[end..].iter().map(|l| l.to_string()));
    Ok(out.concat())
}

#[cfg(test)]
mod tests {
    use super::*;

    const YAML: &str = "\
name: release-notes
kind: scripted
env:
  # Replace all of these.
  FORGE_HOST: forge.example.com
  # Which repos to read.
  RELEASE_SOURCES: '[{\"repo\": \"a/b\", \"product\": \"B\"}]'
  NOTES: |
    first
    second
  DRAFT_ANY_DAY: \"false\"
secrets:
  TOKEN: token-name
input:
  command: |
    [] | to json
";

    fn changes(pairs: &[(&str, &str)]) -> BTreeMap<String, String> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    fn env_of(yaml: &str) -> BTreeMap<String, String> {
        let v: serde_yaml::Value = serde_yaml::from_str(yaml).unwrap();
        serde_yaml::from_value(v["env"].clone()).unwrap()
    }

    #[test]
    fn a_single_line_value_changes_in_place_and_comments_stay() {
        let out = set_env_values(YAML, &changes(&[("FORGE_HOST", "dev.a8n.run")])).unwrap();
        assert_eq!(
            out,
            YAML.replace(
                "FORGE_HOST: forge.example.com",
                "FORGE_HOST: \"dev.a8n.run\""
            )
        );
        assert_eq!(env_of(&out)["FORGE_HOST"], "dev.a8n.run");
    }

    #[test]
    fn a_multi_line_value_becomes_a_block_and_replaces_the_old_one_whole() {
        let out = set_env_values(
            YAML,
            &changes(&[
                ("RELEASE_SOURCES", "org/ui = App\norg/server = App"),
                ("NOTES", "only"),
            ]),
        )
        .unwrap();
        let env = env_of(&out);
        assert_eq!(env["RELEASE_SOURCES"], "org/ui = App\norg/server = App");
        assert_eq!(env["NOTES"], "only");
        assert_eq!(env["FORGE_HOST"], "forge.example.com");
        assert_eq!(env["DRAFT_ANY_DAY"], "false");
        assert!(out.contains("  RELEASE_SOURCES: |-\n    org/ui = App\n    org/server = App\n"));
        assert!(!out.contains("second"), "the old block's lines are gone");
        for kept in [
            "  # Replace all of these.\n",
            "  # Which repos to read.\n",
            "secrets:\n  TOKEN: token-name\n",
            "  command: |\n    [] | to json\n",
        ] {
            assert!(out.contains(kept), "{kept:?} kept in:\n{out}");
        }
    }

    #[test]
    fn values_yaml_would_misread_are_quoted() {
        for value in [
            "true",
            "08",
            "a: b",
            "# not a comment",
            "  leading",
            "x\n  indented",
        ] {
            let out = set_env_values(YAML, &changes(&[("DRAFT_ANY_DAY", value)])).unwrap();
            assert_eq!(env_of(&out)["DRAFT_ANY_DAY"], value, "{value:?} in:\n{out}");
        }
    }

    #[test]
    fn a_key_not_in_the_block_is_refused() {
        let err = set_env_values(YAML, &changes(&[("TOKEN", "x")])).unwrap_err();
        assert!(err.contains("`TOKEN` is not in"), "{err}");
        assert!(set_env_values("name: x\n", &changes(&[])).is_err());
    }
}
