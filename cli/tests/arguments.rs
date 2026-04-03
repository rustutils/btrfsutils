//! Snapshot tests for CLI argument parsing.
//!
//! Each test calls `Arguments::try_parse_from` with a simulated argv and
//! snapshots either the parsed struct (valid args) or the error message
//! (invalid args) using insta.

use btrfs_cli::Arguments;
use clap::Parser;

fn parse(args: &[&str]) -> Arguments {
    // Prevent BTRFS_OUTPUT_FORMAT from the outer environment from affecting
    // parsed snapshots (clap reads env vars even with try_parse_from).
    unsafe { std::env::remove_var("BTRFS_OUTPUT_FORMAT") };
    Arguments::try_parse_from(args).unwrap()
}

fn parse_err(args: &[&str]) -> String {
    unsafe { std::env::remove_var("BTRFS_OUTPUT_FORMAT") };
    let err = Arguments::try_parse_from(args).unwrap_err();
    let rendered = err.render().to_string();
    // Only keep the error lines, not the full usage.
    rendered
        .lines()
        .take_while(|l| !l.starts_with("Usage:"))
        .collect::<Vec<_>>()
        .join("\n")
}

// ── subvolume ────────────────────────────────────────────────────────

#[test]
fn subvolume_create() {
    insta::assert_debug_snapshot!(parse(&[
        "btrfs",
        "subvolume",
        "create",
        "/mnt/subvol"
    ]));
}

#[test]
fn subvolume_create_missing_path() {
    insta::assert_snapshot!(parse_err(&["btrfs", "subvolume", "create"]));
}

#[test]
fn subvolume_create_parents() {
    insta::assert_debug_snapshot!(parse(&[
        "btrfs",
        "subvolume",
        "create",
        "-p",
        "/mnt/a/b/subvol"
    ]));
}

#[test]
fn subvolume_create_qgroup() {
    insta::assert_debug_snapshot!(parse(&[
        "btrfs",
        "subvolume",
        "create",
        "-i",
        "0/100",
        "/mnt/subvol"
    ]));
}

#[test]
fn subvolume_create_multiple_qgroups() {
    insta::assert_debug_snapshot!(parse(&[
        "btrfs",
        "subvolume",
        "create",
        "-i",
        "0/100",
        "-i",
        "1/0",
        "/mnt/subvol"
    ]));
}

#[test]
fn subvolume_create_parents_and_qgroup() {
    insta::assert_debug_snapshot!(parse(&[
        "btrfs",
        "subvolume",
        "create",
        "-p",
        "-i",
        "0/100",
        "/mnt/a/subvol"
    ]));
}

#[test]
fn subvolume_delete() {
    insta::assert_debug_snapshot!(parse(&[
        "btrfs",
        "subvolume",
        "delete",
        "/mnt/subvol"
    ]));
}

#[test]
fn subvolume_delete_commit_after() {
    insta::assert_debug_snapshot!(parse(&[
        "btrfs",
        "subvolume",
        "delete",
        "-c",
        "/mnt/subvol"
    ]));
}

#[test]
fn subvolume_delete_commit_each() {
    insta::assert_debug_snapshot!(parse(&[
        "btrfs",
        "subvolume",
        "delete",
        "-C",
        "/mnt/subvol1",
        "/mnt/subvol2"
    ]));
}

#[test]
fn subvolume_delete_by_subvolid() {
    insta::assert_debug_snapshot!(parse(&[
        "btrfs",
        "subvolume",
        "delete",
        "-i",
        "256",
        "/mnt"
    ]));
}

#[test]
fn subvolume_delete_recursive() {
    insta::assert_debug_snapshot!(parse(&[
        "btrfs",
        "subvolume",
        "delete",
        "-R",
        "/mnt/subvol"
    ]));
}

#[test]
fn subvolume_delete_verbose() {
    insta::assert_debug_snapshot!(parse(&[
        "btrfs",
        "subvolume",
        "delete",
        "-v",
        "/mnt/subvol"
    ]));
}

#[test]
fn subvolume_delete_commit_conflict() {
    // -c and -C are mutually exclusive
    insta::assert_snapshot!(parse_err(&[
        "btrfs",
        "subvolume",
        "delete",
        "-c",
        "-C",
        "/mnt/subvol"
    ]));
}

#[test]
fn subvolume_delete_subvolid_recursive_conflict() {
    // --subvolid and --recursive are mutually exclusive
    insta::assert_snapshot!(parse_err(&[
        "btrfs",
        "subvolume",
        "delete",
        "-i",
        "256",
        "-R",
        "/mnt"
    ]));
}

#[test]
fn subvolume_delete_missing_path() {
    insta::assert_snapshot!(parse_err(&["btrfs", "subvolume", "delete"]));
}

#[test]
fn subvolume_snapshot() {
    insta::assert_debug_snapshot!(parse(&[
        "btrfs",
        "subvolume",
        "snapshot",
        "/mnt/src",
        "/mnt/dst"
    ]));
}

#[test]
fn subvolume_snapshot_readonly() {
    insta::assert_debug_snapshot!(parse(&[
        "btrfs",
        "subvolume",
        "snapshot",
        "-r",
        "/mnt/src",
        "/mnt/dst"
    ]));
}

#[test]
fn subvolume_snapshot_qgroup() {
    insta::assert_debug_snapshot!(parse(&[
        "btrfs",
        "subvolume",
        "snapshot",
        "-i",
        "0/100",
        "/mnt/src",
        "/mnt/dst"
    ]));
}

#[test]
fn subvolume_snapshot_readonly_qgroup() {
    insta::assert_debug_snapshot!(parse(&[
        "btrfs",
        "subvolume",
        "snapshot",
        "-r",
        "-i",
        "0/100",
        "-i",
        "1/0",
        "/mnt/src",
        "/mnt/dst"
    ]));
}

#[test]
fn subvolume_snapshot_missing_dest() {
    insta::assert_snapshot!(parse_err(&[
        "btrfs",
        "subvolume",
        "snapshot",
        "/mnt/src"
    ]));
}

#[test]
fn subvolume_show() {
    insta::assert_debug_snapshot!(parse(&[
        "btrfs",
        "subvolume",
        "show",
        "/mnt/subvol"
    ]));
}

#[test]
fn subvolume_show_rootid() {
    insta::assert_debug_snapshot!(parse(&[
        "btrfs",
        "subvolume",
        "show",
        "-r",
        "256",
        "/mnt"
    ]));
}

#[test]
fn subvolume_show_uuid() {
    insta::assert_debug_snapshot!(parse(&[
        "btrfs",
        "subvolume",
        "show",
        "-u",
        "550e8400-e29b-41d4-a716-446655440000",
        "/mnt"
    ]));
}

#[test]
fn subvolume_show_rootid_uuid_conflict() {
    insta::assert_snapshot!(parse_err(&[
        "btrfs",
        "subvolume",
        "show",
        "-r",
        "256",
        "-u",
        "550e8400-e29b-41d4-a716-446655440000",
        "/mnt"
    ]));
}

#[test]
fn subvolume_list() {
    insta::assert_debug_snapshot!(parse(&[
        "btrfs",
        "subvolume",
        "list",
        "/mnt"
    ]));
}

#[test]
fn subvolume_list_with_flags() {
    insta::assert_debug_snapshot!(parse(&[
        "btrfs",
        "subvolume",
        "list",
        "-purR",
        "/mnt"
    ]));
}

#[test]
fn subvolume_list_only_below() {
    insta::assert_debug_snapshot!(parse(&[
        "btrfs",
        "subvolume",
        "list",
        "-o",
        "/mnt"
    ]));
}

#[test]
fn subvolume_list_table() {
    insta::assert_debug_snapshot!(parse(&[
        "btrfs",
        "subvolume",
        "list",
        "-t",
        "/mnt"
    ]));
}

#[test]
fn subvolume_list_gen_filter_exact() {
    insta::assert_debug_snapshot!(parse(&[
        "btrfs",
        "subvolume",
        "list",
        "-G",
        "100",
        "/mnt"
    ]));
}

#[test]
fn subvolume_list_gen_filter_atleast() {
    insta::assert_debug_snapshot!(parse(&[
        "btrfs",
        "subvolume",
        "list",
        "-G",
        "+50",
        "/mnt"
    ]));
}

#[test]
fn subvolume_list_gen_filter_atmost() {
    insta::assert_debug_snapshot!(parse(&[
        "btrfs",
        "subvolume",
        "list",
        "-G",
        "-200",
        "/mnt"
    ]));
}

#[test]
fn subvolume_list_ogen_filter() {
    insta::assert_debug_snapshot!(parse(&[
        "btrfs",
        "subvolume",
        "list",
        "-C",
        "+10",
        "/mnt"
    ]));
}

#[test]
fn subvolume_list_sort() {
    insta::assert_debug_snapshot!(parse(&[
        "btrfs",
        "subvolume",
        "list",
        "--sort",
        "gen,-path",
        "/mnt"
    ]));
}

#[test]
fn subvolume_list_sort_invalid() {
    insta::assert_snapshot!(parse_err(&[
        "btrfs",
        "subvolume",
        "list",
        "--sort",
        "bogus",
        "/mnt"
    ]));
}

#[test]
fn subvolume_list_combined() {
    insta::assert_debug_snapshot!(parse(&[
        "btrfs",
        "subvolume",
        "list",
        "-otc",
        "-G",
        "+5",
        "--sort",
        "-gen",
        "/mnt"
    ]));
}

#[test]
fn subvolume_list_all() {
    insta::assert_debug_snapshot!(parse(&[
        "btrfs",
        "subvolume",
        "list",
        "-a",
        "/mnt"
    ]));
}

#[test]
fn subvolume_list_deleted() {
    insta::assert_debug_snapshot!(parse(&[
        "btrfs",
        "subvolume",
        "list",
        "-d",
        "/mnt"
    ]));
}

#[test]
fn subvolume_get_default() {
    insta::assert_debug_snapshot!(parse(&[
        "btrfs",
        "subvolume",
        "get-default",
        "/mnt"
    ]));
}

#[test]
fn subvolume_set_default() {
    insta::assert_debug_snapshot!(parse(&[
        "btrfs",
        "subvolume",
        "set-default",
        "256",
        "/mnt"
    ]));
}

#[test]
fn subvolume_find_new() {
    insta::assert_debug_snapshot!(parse(&[
        "btrfs",
        "subvolume",
        "find-new",
        "/mnt/subvol",
        "100"
    ]));
}

#[test]
fn subvolume_find_new_missing_gen() {
    insta::assert_snapshot!(parse_err(&[
        "btrfs",
        "subvolume",
        "find-new",
        "/mnt/subvol"
    ]));
}

#[test]
fn subvolume_sync() {
    insta::assert_debug_snapshot!(parse(&[
        "btrfs",
        "subvolume",
        "sync",
        "/mnt"
    ]));
}

#[test]
fn subvolume_sync_with_ids() {
    insta::assert_debug_snapshot!(parse(&[
        "btrfs",
        "subvolume",
        "sync",
        "/mnt",
        "256",
        "257"
    ]));
}

#[test]
fn subvolume_sync_with_sleep() {
    insta::assert_debug_snapshot!(parse(&[
        "btrfs",
        "subvolume",
        "sync",
        "-s",
        "5",
        "/mnt",
        "256"
    ]));
}

// ── filesystem ───────────────────────────────────────────────────────

#[test]
fn filesystem_df() {
    insta::assert_debug_snapshot!(parse(&[
        "btrfs",
        "filesystem",
        "df",
        "/mnt"
    ]));
}

#[test]
fn filesystem_df_missing_path() {
    insta::assert_snapshot!(parse_err(&["btrfs", "filesystem", "df"]));
}

#[test]
fn filesystem_show() {
    insta::assert_debug_snapshot!(parse(&[
        "btrfs",
        "filesystem",
        "show",
        "/mnt"
    ]));
}

#[test]
fn filesystem_show_no_args() {
    insta::assert_debug_snapshot!(parse(&["btrfs", "filesystem", "show"]));
}

#[test]
fn filesystem_sync() {
    insta::assert_debug_snapshot!(parse(&[
        "btrfs",
        "filesystem",
        "sync",
        "/mnt"
    ]));
}

#[test]
fn filesystem_resize() {
    insta::assert_debug_snapshot!(parse(&[
        "btrfs",
        "filesystem",
        "resize",
        "+1G",
        "/mnt"
    ]));
}

#[test]
fn filesystem_resize_missing_path() {
    insta::assert_snapshot!(parse_err(&[
        "btrfs",
        "filesystem",
        "resize",
        "+1G"
    ]));
}

#[test]
fn filesystem_label_get() {
    insta::assert_debug_snapshot!(parse(&[
        "btrfs",
        "filesystem",
        "label",
        "/dev/sda1"
    ]));
}

#[test]
fn filesystem_label_set() {
    insta::assert_debug_snapshot!(parse(&[
        "btrfs",
        "filesystem",
        "label",
        "/dev/sda1",
        "my-label"
    ]));
}

#[test]
fn filesystem_usage() {
    insta::assert_debug_snapshot!(parse(&[
        "btrfs",
        "filesystem",
        "usage",
        "/mnt"
    ]));
}

#[test]
fn filesystem_du() {
    insta::assert_debug_snapshot!(parse(&[
        "btrfs",
        "filesystem",
        "du",
        "/mnt/file"
    ]));
}

#[test]
fn filesystem_defrag() {
    insta::assert_debug_snapshot!(parse(&[
        "btrfs",
        "filesystem",
        "defragment",
        "/mnt/file"
    ]));
}

#[test]
fn filesystem_defrag_compress() {
    insta::assert_debug_snapshot!(parse(&[
        "btrfs",
        "filesystem",
        "defragment",
        "-czstd",
        "/mnt/file"
    ]));
}

#[test]
fn filesystem_mkswapfile() {
    insta::assert_debug_snapshot!(parse(&[
        "btrfs",
        "filesystem",
        "mkswapfile",
        "/mnt/swap"
    ]));
}

#[test]
fn filesystem_mkswapfile_with_size() {
    insta::assert_debug_snapshot!(parse(&[
        "btrfs",
        "filesystem",
        "mkswapfile",
        "-s",
        "4G",
        "/mnt/swap"
    ]));
}

// ── device ───────────────────────────────────────────────────────────

#[test]
fn device_add() {
    insta::assert_debug_snapshot!(parse(&[
        "btrfs", "device", "add", "/dev/sdb", "/mnt"
    ]));
}

#[test]
fn device_add_force() {
    insta::assert_debug_snapshot!(parse(&[
        "btrfs", "device", "add", "-f", "/dev/sdb", "/mnt"
    ]));
}

#[test]
fn device_add_nodiscard() {
    insta::assert_debug_snapshot!(parse(&[
        "btrfs", "device", "add", "-K", "/dev/sdb", "/mnt"
    ]));
}

#[test]
fn device_add_enqueue() {
    insta::assert_debug_snapshot!(parse(&[
        "btrfs",
        "device",
        "add",
        "--enqueue",
        "/dev/sdb",
        "/mnt"
    ]));
}

#[test]
fn device_add_all_flags() {
    insta::assert_debug_snapshot!(parse(&[
        "btrfs",
        "device",
        "add",
        "-f",
        "-K",
        "--enqueue",
        "/dev/sdb",
        "/dev/sdc",
        "/mnt"
    ]));
}

#[test]
fn device_add_missing_mount() {
    insta::assert_snapshot!(parse_err(&["btrfs", "device", "add", "/dev/sdb"]));
}

#[test]
fn device_remove() {
    insta::assert_debug_snapshot!(parse(&[
        "btrfs", "device", "remove", "/dev/sdb", "/mnt"
    ]));
}

#[test]
fn device_stats() {
    insta::assert_debug_snapshot!(parse(&["btrfs", "device", "stats", "/mnt"]));
}

#[test]
fn device_stats_check() {
    insta::assert_debug_snapshot!(parse(&[
        "btrfs", "device", "stats", "-c", "/mnt"
    ]));
}

#[test]
fn device_scan() {
    insta::assert_debug_snapshot!(parse(&["btrfs", "device", "scan"]));
}

#[test]
fn device_ready() {
    insta::assert_debug_snapshot!(parse(&[
        "btrfs",
        "device",
        "ready",
        "/dev/sda1"
    ]));
}

// ── balance ──────────────────────────────────────────────────────────

#[test]
fn balance_start() {
    insta::assert_debug_snapshot!(parse(&[
        "btrfs", "balance", "start", "/mnt"
    ]));
}

#[test]
fn balance_start_with_filters() {
    insta::assert_debug_snapshot!(parse(&[
        "btrfs", "balance", "start", "-d", "usage=50", "-f", "/mnt"
    ]));
}

#[test]
fn balance_start_missing_path() {
    insta::assert_snapshot!(parse_err(&["btrfs", "balance", "start"]));
}

#[test]
fn balance_pause() {
    insta::assert_debug_snapshot!(parse(&[
        "btrfs", "balance", "pause", "/mnt"
    ]));
}

#[test]
fn balance_cancel() {
    insta::assert_debug_snapshot!(parse(&[
        "btrfs", "balance", "cancel", "/mnt"
    ]));
}

#[test]
fn balance_resume() {
    insta::assert_debug_snapshot!(parse(&[
        "btrfs", "balance", "resume", "/mnt"
    ]));
}

#[test]
fn balance_status() {
    insta::assert_debug_snapshot!(parse(&[
        "btrfs", "balance", "status", "/mnt"
    ]));
}

// ── scrub ────────────────────────────────────────────────────────────

#[test]
fn scrub_start() {
    insta::assert_debug_snapshot!(parse(&["btrfs", "scrub", "start", "/mnt"]));
}

#[test]
fn scrub_start_readonly() {
    insta::assert_debug_snapshot!(parse(&[
        "btrfs", "scrub", "start", "-r", "/mnt"
    ]));
}

#[test]
fn scrub_cancel() {
    insta::assert_debug_snapshot!(parse(&["btrfs", "scrub", "cancel", "/mnt"]));
}

#[test]
fn scrub_status() {
    insta::assert_debug_snapshot!(parse(&["btrfs", "scrub", "status", "/mnt"]));
}

#[test]
fn scrub_resume() {
    insta::assert_debug_snapshot!(parse(&["btrfs", "scrub", "resume", "/mnt"]));
}

// ── inspect-internal ─────────────────────────────────────────────────

#[test]
fn inspect_rootid() {
    insta::assert_debug_snapshot!(parse(&[
        "btrfs",
        "inspect-internal",
        "rootid",
        "/mnt/file"
    ]));
}

#[test]
fn inspect_inode_resolve() {
    insta::assert_debug_snapshot!(parse(&[
        "btrfs",
        "inspect-internal",
        "inode-resolve",
        "256",
        "/mnt"
    ]));
}

#[test]
fn inspect_inode_resolve_missing_args() {
    insta::assert_snapshot!(parse_err(&[
        "btrfs",
        "inspect-internal",
        "inode-resolve"
    ]));
}

#[test]
fn inspect_logical_resolve() {
    insta::assert_debug_snapshot!(parse(&[
        "btrfs",
        "inspect-internal",
        "logical-resolve",
        "12345",
        "/mnt"
    ]));
}

#[test]
fn inspect_subvolid_resolve() {
    insta::assert_debug_snapshot!(parse(&[
        "btrfs",
        "inspect-internal",
        "subvolid-resolve",
        "256",
        "/mnt"
    ]));
}

#[test]
fn inspect_min_dev_size() {
    insta::assert_debug_snapshot!(parse(&[
        "btrfs",
        "inspect-internal",
        "min-dev-size",
        "/mnt"
    ]));
}

#[test]
fn inspect_list_chunks() {
    insta::assert_debug_snapshot!(parse(&[
        "btrfs",
        "inspect-internal",
        "list-chunks",
        "/mnt"
    ]));
}

#[test]
fn inspect_dump_super() {
    insta::assert_debug_snapshot!(parse(&[
        "btrfs",
        "inspect-internal",
        "dump-super",
        "/dev/sda1"
    ]));
}

#[test]
fn inspect_dump_super_full_all() {
    insta::assert_debug_snapshot!(parse(&[
        "btrfs",
        "inspect-internal",
        "dump-super",
        "-f",
        "-a",
        "/dev/sda1"
    ]));
}

// ── quota ────────────────────────────────────────────────────────────

#[test]
fn quota_enable() {
    insta::assert_debug_snapshot!(parse(&["btrfs", "quota", "enable", "/mnt"]));
}

#[test]
fn quota_enable_simple() {
    insta::assert_debug_snapshot!(parse(&[
        "btrfs", "quota", "enable", "-s", "/mnt"
    ]));
}

#[test]
fn quota_disable() {
    insta::assert_debug_snapshot!(parse(&[
        "btrfs", "quota", "disable", "/mnt"
    ]));
}

#[test]
fn quota_rescan() {
    insta::assert_debug_snapshot!(parse(&["btrfs", "quota", "rescan", "/mnt"]));
}

#[test]
fn quota_rescan_status() {
    insta::assert_debug_snapshot!(parse(&[
        "btrfs", "quota", "rescan", "-s", "/mnt"
    ]));
}

#[test]
fn quota_status() {
    insta::assert_debug_snapshot!(parse(&["btrfs", "quota", "status", "/mnt"]));
}

// ── qgroup ───────────────────────────────────────────────────────────

#[test]
fn qgroup_create() {
    insta::assert_debug_snapshot!(parse(&[
        "btrfs", "qgroup", "create", "1/100", "/mnt"
    ]));
}

#[test]
fn qgroup_create_missing_path() {
    insta::assert_snapshot!(parse_err(&["btrfs", "qgroup", "create", "1/100"]));
}

#[test]
fn qgroup_destroy() {
    insta::assert_debug_snapshot!(parse(&[
        "btrfs", "qgroup", "destroy", "1/100", "/mnt"
    ]));
}

#[test]
fn qgroup_assign() {
    insta::assert_debug_snapshot!(parse(&[
        "btrfs", "qgroup", "assign", "0/256", "1/100", "/mnt"
    ]));
}

#[test]
fn qgroup_assign_no_rescan() {
    insta::assert_debug_snapshot!(parse(&[
        "btrfs",
        "qgroup",
        "assign",
        "--no-rescan",
        "0/256",
        "1/100",
        "/mnt"
    ]));
}

#[test]
fn qgroup_remove() {
    insta::assert_debug_snapshot!(parse(&[
        "btrfs", "qgroup", "remove", "0/256", "1/100", "/mnt"
    ]));
}

#[test]
fn qgroup_limit() {
    insta::assert_debug_snapshot!(parse(&[
        "btrfs", "qgroup", "limit", "1G", "0/256", "/mnt"
    ]));
}

#[test]
fn qgroup_limit_none() {
    insta::assert_debug_snapshot!(parse(&[
        "btrfs", "qgroup", "limit", "none", "0/256"
    ]));
}

#[test]
fn qgroup_show() {
    insta::assert_debug_snapshot!(parse(&["btrfs", "qgroup", "show", "/mnt"]));
}

#[test]
fn qgroup_show_with_flags() {
    insta::assert_debug_snapshot!(parse(&[
        "btrfs", "qgroup", "show", "-pcre", "/mnt"
    ]));
}

#[test]
fn qgroup_clear_stale() {
    insta::assert_debug_snapshot!(parse(&[
        "btrfs",
        "qgroup",
        "clear-stale",
        "/mnt"
    ]));
}

// ── replace ──────────────────────────────────────────────────────────

#[test]
fn replace_start() {
    insta::assert_debug_snapshot!(parse(&[
        "btrfs", "replace", "start", "/dev/sda", "/dev/sdb", "/mnt"
    ]));
}

#[test]
fn replace_start_missing_args() {
    insta::assert_snapshot!(parse_err(&[
        "btrfs", "replace", "start", "/dev/sda"
    ]));
}

#[test]
fn replace_status() {
    insta::assert_debug_snapshot!(parse(&[
        "btrfs", "replace", "status", "/mnt"
    ]));
}

#[test]
fn replace_cancel() {
    insta::assert_debug_snapshot!(parse(&[
        "btrfs", "replace", "cancel", "/mnt"
    ]));
}

// ── send ─────────────────────────────────────────────────────────────

#[test]
fn send_basic() {
    insta::assert_debug_snapshot!(parse(&["btrfs", "send", "/mnt/snap"]));
}

#[test]
fn send_incremental() {
    insta::assert_debug_snapshot!(parse(&[
        "btrfs",
        "send",
        "-p",
        "/mnt/parent",
        "/mnt/snap"
    ]));
}

#[test]
fn send_to_file() {
    insta::assert_debug_snapshot!(parse(&[
        "btrfs",
        "send",
        "-f",
        "/tmp/stream",
        "/mnt/snap"
    ]));
}

#[test]
fn send_no_data() {
    insta::assert_debug_snapshot!(parse(&[
        "btrfs",
        "send",
        "--no-data",
        "/mnt/snap"
    ]));
}

#[test]
fn send_missing_subvol() {
    insta::assert_snapshot!(parse_err(&["btrfs", "send"]));
}

// ── receive ──────────────────────────────────────────────────────────

#[test]
fn receive_basic() {
    insta::assert_debug_snapshot!(parse(&["btrfs", "receive", "/mnt/dest"]));
}

#[test]
fn receive_from_file() {
    insta::assert_debug_snapshot!(parse(&[
        "btrfs",
        "receive",
        "-f",
        "/tmp/stream",
        "/mnt/dest"
    ]));
}

#[test]
fn receive_dump() {
    insta::assert_debug_snapshot!(parse(&["btrfs", "receive", "--dump"]));
}

// ── property ─────────────────────────────────────────────────────────

#[test]
fn property_get() {
    insta::assert_debug_snapshot!(parse(&[
        "btrfs",
        "property",
        "get",
        "/mnt/subvol"
    ]));
}

#[test]
fn property_get_named() {
    insta::assert_debug_snapshot!(parse(&[
        "btrfs",
        "property",
        "get",
        "/mnt/subvol",
        "ro"
    ]));
}

#[test]
fn property_set() {
    insta::assert_debug_snapshot!(parse(&[
        "btrfs",
        "property",
        "set",
        "/mnt/subvol",
        "ro",
        "true"
    ]));
}

#[test]
fn property_set_missing_value() {
    insta::assert_snapshot!(parse_err(&[
        "btrfs",
        "property",
        "set",
        "/mnt/subvol",
        "ro"
    ]));
}

#[test]
fn property_list() {
    insta::assert_debug_snapshot!(parse(&[
        "btrfs",
        "property",
        "list",
        "/mnt/subvol"
    ]));
}

// ── global options ───────────────────────────────────────────────────

#[test]
fn global_verbose() {
    insta::assert_debug_snapshot!(parse(&[
        "btrfs",
        "-v",
        "filesystem",
        "df",
        "/mnt"
    ]));
}

#[test]
fn global_quiet() {
    insta::assert_debug_snapshot!(parse(&[
        "btrfs",
        "-q",
        "filesystem",
        "df",
        "/mnt"
    ]));
}

#[test]
fn no_subcommand() {
    insta::assert_snapshot!(parse_err(&["btrfs"]));
}

#[test]
fn unknown_subcommand() {
    insta::assert_snapshot!(parse_err(&["btrfs", "frobnicate"]));
}
