//! Snapshot tests for CLI help output.
//!
//! Automatically discovers all subcommands from the clap `Command` tree and
//! snapshots each one's `--help` text. This catches regressions in help text,
//! option descriptions, headings, and flag ordering without requiring any
//! privileges, and ensures new subcommands get help tests without manual work.

use btrfs_cli::Arguments;
use clap::CommandFactory;

/// Render the long help text for a subcommand path (e.g. &["subvolume", "list"]).
fn help(path: &[&str]) -> String {
    let mut cmd = Arguments::command();
    for &name in path {
        cmd = cmd
            .find_subcommand(name)
            .unwrap_or_else(|| panic!("subcommand '{name}' not found"))
            .clone();
    }
    cmd.term_width(80).render_long_help().to_string()
}

/// Recursively collect all subcommand paths from the clap Command tree.
fn collect_subcommands(
    cmd: &clap::Command,
    prefix: &[&str],
    out: &mut Vec<(String, Vec<String>)>,
) {
    let name = if prefix.is_empty() {
        "toplevel".to_string()
    } else {
        prefix
            .iter()
            .map(|s| s.replace('-', "_"))
            .collect::<Vec<_>>()
            .join("_")
    };
    out.push((name, prefix.iter().map(|s| s.to_string()).collect()));

    for sub in cmd.get_subcommands() {
        if sub.get_name() == "help" {
            continue;
        }
        let mut path: Vec<&str> = prefix.to_vec();
        path.push(sub.get_name());
        collect_subcommands(sub, &path, out);
    }
}

#[test]
fn help_all() {
    let cmd = Arguments::command();
    let mut cases = Vec::new();
    collect_subcommands(&cmd, &[], &mut cases);

    for (snap_name, path) in &cases {
        let path_refs: Vec<&str> = path.iter().map(|s| s.as_str()).collect();
        let text = help(&path_refs);
        insta::assert_snapshot!(format!("btrfs_{snap_name}"), text);
    }
}
