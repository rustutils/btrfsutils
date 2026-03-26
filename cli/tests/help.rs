//! Snapshot tests for CLI help output.
//!
//! Each test renders the `--help` text for a subcommand and snapshots it.
//! This catches regressions in help text, option descriptions, headings, and
//! flag ordering without requiring any privileges.

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

// ── top level ───────────────────────────────────────────────────────

#[test]
fn toplevel() {
    insta::assert_snapshot!(help(&[]));
}

// ── balance ─────────────────────────────────────────────────────────

#[test]
fn balance() {
    insta::assert_snapshot!(help(&["balance"]));
}

#[test]
fn balance_start() {
    insta::assert_snapshot!(help(&["balance", "start"]));
}

#[test]
fn balance_pause() {
    insta::assert_snapshot!(help(&["balance", "pause"]));
}

#[test]
fn balance_cancel() {
    insta::assert_snapshot!(help(&["balance", "cancel"]));
}

#[test]
fn balance_resume() {
    insta::assert_snapshot!(help(&["balance", "resume"]));
}

#[test]
fn balance_status() {
    insta::assert_snapshot!(help(&["balance", "status"]));
}

// ── device ──────────────────────────────────────────────────────────

#[test]
fn device() {
    insta::assert_snapshot!(help(&["device"]));
}

#[test]
fn device_add() {
    insta::assert_snapshot!(help(&["device", "add"]));
}

#[test]
fn device_remove() {
    insta::assert_snapshot!(help(&["device", "remove"]));
}

#[test]
fn device_stats() {
    insta::assert_snapshot!(help(&["device", "stats"]));
}

#[test]
fn device_scan() {
    insta::assert_snapshot!(help(&["device", "scan"]));
}

#[test]
fn device_ready() {
    insta::assert_snapshot!(help(&["device", "ready"]));
}

#[test]
fn device_usage() {
    insta::assert_snapshot!(help(&["device", "usage"]));
}

// ── filesystem ──────────────────────────────────────────────────────

#[test]
fn filesystem() {
    insta::assert_snapshot!(help(&["filesystem"]));
}

#[test]
fn filesystem_df() {
    insta::assert_snapshot!(help(&["filesystem", "df"]));
}

#[test]
fn filesystem_du() {
    insta::assert_snapshot!(help(&["filesystem", "du"]));
}

#[test]
fn filesystem_show() {
    insta::assert_snapshot!(help(&["filesystem", "show"]));
}

#[test]
fn filesystem_sync() {
    insta::assert_snapshot!(help(&["filesystem", "sync"]));
}

#[test]
fn filesystem_defragment() {
    insta::assert_snapshot!(help(&["filesystem", "defragment"]));
}

#[test]
fn filesystem_resize() {
    insta::assert_snapshot!(help(&["filesystem", "resize"]));
}

#[test]
fn filesystem_label() {
    insta::assert_snapshot!(help(&["filesystem", "label"]));
}

#[test]
fn filesystem_usage() {
    insta::assert_snapshot!(help(&["filesystem", "usage"]));
}

#[test]
fn filesystem_mkswapfile() {
    insta::assert_snapshot!(help(&["filesystem", "mkswapfile"]));
}

#[test]
fn filesystem_commit_stats() {
    insta::assert_snapshot!(help(&["filesystem", "commit-stats"]));
}

// ── inspect-internal ────────────────────────────────────────────────

#[test]
fn inspect() {
    insta::assert_snapshot!(help(&["inspect"]));
}

#[test]
fn inspect_rootid() {
    insta::assert_snapshot!(help(&["inspect", "rootid"]));
}

#[test]
fn inspect_inode_resolve() {
    insta::assert_snapshot!(help(&["inspect", "inode-resolve"]));
}

#[test]
fn inspect_logical_resolve() {
    insta::assert_snapshot!(help(&["inspect", "logical-resolve"]));
}

#[test]
fn inspect_subvolid_resolve() {
    insta::assert_snapshot!(help(&["inspect", "subvolid-resolve"]));
}

#[test]
fn inspect_min_dev_size() {
    insta::assert_snapshot!(help(&["inspect", "min-dev-size"]));
}

#[test]
fn inspect_list_chunks() {
    insta::assert_snapshot!(help(&["inspect", "list-chunks"]));
}

#[test]
fn inspect_dump_super() {
    insta::assert_snapshot!(help(&["inspect", "dump-super"]));
}

#[test]
fn inspect_dump_tree() {
    insta::assert_snapshot!(help(&["inspect", "dump-tree"]));
}

#[test]
fn inspect_tree_stats() {
    insta::assert_snapshot!(help(&["inspect", "tree-stats"]));
}

#[test]
fn inspect_map_swapfile() {
    insta::assert_snapshot!(help(&["inspect", "map-swapfile"]));
}

// ── property ────────────────────────────────────────────────────────

#[test]
fn property() {
    insta::assert_snapshot!(help(&["property"]));
}

#[test]
fn property_get() {
    insta::assert_snapshot!(help(&["property", "get"]));
}

#[test]
fn property_set() {
    insta::assert_snapshot!(help(&["property", "set"]));
}

#[test]
fn property_list() {
    insta::assert_snapshot!(help(&["property", "list"]));
}

// ── quota ───────────────────────────────────────────────────────────

#[test]
fn quota() {
    insta::assert_snapshot!(help(&["quota"]));
}

#[test]
fn quota_enable() {
    insta::assert_snapshot!(help(&["quota", "enable"]));
}

#[test]
fn quota_disable() {
    insta::assert_snapshot!(help(&["quota", "disable"]));
}

#[test]
fn quota_rescan() {
    insta::assert_snapshot!(help(&["quota", "rescan"]));
}

#[test]
fn quota_status() {
    insta::assert_snapshot!(help(&["quota", "status"]));
}

// ── qgroup ──────────────────────────────────────────────────────────

#[test]
fn qgroup() {
    insta::assert_snapshot!(help(&["qgroup"]));
}

#[test]
fn qgroup_create() {
    insta::assert_snapshot!(help(&["qgroup", "create"]));
}

#[test]
fn qgroup_destroy() {
    insta::assert_snapshot!(help(&["qgroup", "destroy"]));
}

#[test]
fn qgroup_assign() {
    insta::assert_snapshot!(help(&["qgroup", "assign"]));
}

#[test]
fn qgroup_remove() {
    insta::assert_snapshot!(help(&["qgroup", "remove"]));
}

#[test]
fn qgroup_limit() {
    insta::assert_snapshot!(help(&["qgroup", "limit"]));
}

#[test]
fn qgroup_show() {
    insta::assert_snapshot!(help(&["qgroup", "show"]));
}

#[test]
fn qgroup_clear_stale() {
    insta::assert_snapshot!(help(&["qgroup", "clear-stale"]));
}

// ── scrub ───────────────────────────────────────────────────────────

#[test]
fn scrub() {
    insta::assert_snapshot!(help(&["scrub"]));
}

#[test]
fn scrub_start() {
    insta::assert_snapshot!(help(&["scrub", "start"]));
}

#[test]
fn scrub_cancel() {
    insta::assert_snapshot!(help(&["scrub", "cancel"]));
}

#[test]
fn scrub_resume() {
    insta::assert_snapshot!(help(&["scrub", "resume"]));
}

#[test]
fn scrub_status() {
    insta::assert_snapshot!(help(&["scrub", "status"]));
}

#[test]
fn scrub_limit() {
    insta::assert_snapshot!(help(&["scrub", "limit"]));
}

// ── replace ─────────────────────────────────────────────────────────

#[test]
fn replace() {
    insta::assert_snapshot!(help(&["replace"]));
}

#[test]
fn replace_start() {
    insta::assert_snapshot!(help(&["replace", "start"]));
}

#[test]
fn replace_status() {
    insta::assert_snapshot!(help(&["replace", "status"]));
}

#[test]
fn replace_cancel() {
    insta::assert_snapshot!(help(&["replace", "cancel"]));
}

// ── send / receive ──────────────────────────────────────────────────

#[test]
fn send() {
    insta::assert_snapshot!(help(&["send"]));
}

#[test]
fn receive() {
    insta::assert_snapshot!(help(&["receive"]));
}

// ── subvolume ───────────────────────────────────────────────────────

#[test]
fn subvolume() {
    insta::assert_snapshot!(help(&["subvolume"]));
}

#[test]
fn subvolume_create() {
    insta::assert_snapshot!(help(&["subvolume", "create"]));
}

#[test]
fn subvolume_delete() {
    insta::assert_snapshot!(help(&["subvolume", "delete"]));
}

#[test]
fn subvolume_snapshot() {
    insta::assert_snapshot!(help(&["subvolume", "snapshot"]));
}

#[test]
fn subvolume_show() {
    insta::assert_snapshot!(help(&["subvolume", "show"]));
}

#[test]
fn subvolume_list() {
    insta::assert_snapshot!(help(&["subvolume", "list"]));
}

#[test]
fn subvolume_get_default() {
    insta::assert_snapshot!(help(&["subvolume", "get-default"]));
}

#[test]
fn subvolume_set_default() {
    insta::assert_snapshot!(help(&["subvolume", "set-default"]));
}

#[test]
fn subvolume_find_new() {
    insta::assert_snapshot!(help(&["subvolume", "find-new"]));
}

#[test]
fn subvolume_sync() {
    insta::assert_snapshot!(help(&["subvolume", "sync"]));
}

#[test]
fn subvolume_get_flags() {
    insta::assert_snapshot!(help(&["subvolume", "get-flags"]));
}

#[test]
fn subvolume_set_flags() {
    insta::assert_snapshot!(help(&["subvolume", "set-flags"]));
}

// ── check / rescue / restore (stubs) ───────────────────────────────

#[test]
fn check() {
    insta::assert_snapshot!(help(&["check"]));
}

#[test]
fn rescue() {
    insta::assert_snapshot!(help(&["rescue"]));
}

#[test]
fn restore() {
    insta::assert_snapshot!(help(&["restore"]));
}
