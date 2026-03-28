//! Privileged CLI integration tests.
//!
//! - `fixture`: read-only snapshot tests against a pre-built filesystem image
//! - `live`: tests that create and mutate real filesystems (assertion-based
//!   and snapshot-based)

#[path = "common.rs"]
mod common;

use std::process::Command;

/// Path to the `btrfs` binary built by cargo.
fn btrfs_bin() -> String {
    env!("CARGO_BIN_EXE_btrfs").to_string()
}

/// Fixed timezone used for snapshot tests that include formatted timestamps.
/// Ensures output is deterministic regardless of the host's local timezone.
const SNAPSHOT_TZ: &str = "CET-1";

/// Run `btrfs <args>` and return (stdout, stderr, exit_code).
fn btrfs(args: &[&str]) -> (String, String, i32) {
    let output = Command::new(btrfs_bin())
        .args(args)
        .env("TZ", SNAPSHOT_TZ)
        .output()
        .expect("failed to run btrfs binary");
    let stdout = String::from_utf8_lossy(&output.stdout).into_owned();
    let stderr = String::from_utf8_lossy(&output.stderr).into_owned();
    let code = output.status.code().unwrap_or(-1);
    (stdout, stderr, code)
}

/// Run `btrfs <args>` and assert success (exit 0), returning stdout.
fn btrfs_ok(args: &[&str]) -> String {
    let (stdout, stderr, code) = btrfs(args);
    assert_eq!(code, 0, "btrfs {args:?} failed (exit {code}):\n{stderr}");
    stdout
}

/// Replace volatile values in output with stable placeholders.
fn redact(output: &str, mnt: &common::Mount) -> String {
    let mp = mnt.path().to_str().unwrap();
    let dev = mnt.loopback().path().to_str().unwrap();

    let mut s = output.to_string();
    s = s.replace(mp, "<MOUNT>");
    s = s.replace(dev, "<DEV>");

    let re_transid = regex_lite::Regex::new(r"transid[=:]\s*\d+").unwrap();
    s = re_transid
        .replace_all(&s, "transid: <TRANSID>")
        .into_owned();

    let re_timestamp = regex_lite::Regex::new(
        r"\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2} [+-]\d{4}",
    )
    .unwrap();
    s = re_timestamp.replace_all(&s, "<TIMESTAMP>").into_owned();

    let re_epoch = regex_lite::Regex::new(r"\d{10,}\.\d+").unwrap();
    s = re_epoch.replace_all(&s, "<EPOCH>").into_owned();

    let re_gen_at = regex_lite::Regex::new(r"Gen at creation:\s+\d+").unwrap();
    s = re_gen_at
        .replace_all(&s, "Gen at creation: \t<GEN>")
        .into_owned();

    // Filter out SELinux xattr lines (not present on all systems).
    s = s
        .lines()
        .filter(|l| !l.contains("security.selinux"))
        .collect::<Vec<_>>()
        .join("\n");
    if !s.ends_with('\n') && output.ends_with('\n') {
        s.push('\n');
    }

    s
}

/// Replace only mount/device paths (for fixture tests where everything
/// else is deterministic).
fn redact_paths(output: &str, mnt: &common::Mount) -> String {
    let mp = mnt.path().to_str().unwrap();
    let dev = mnt.loopback().path().to_str().unwrap();
    output.replace(mp, "<MOUNT>").replace(dev, "<DEV>")
}

/// Snapshot helper that sets the description to the btrfs command.
macro_rules! snap {
    ($cmd:expr, $output:expr) => {
        insta::with_settings!({ description => $cmd }, {
            insta::assert_snapshot!($output);
        });
    };
}

// Submodules must come after the macro definition.
#[path = "commands/fixture.rs"]
mod fixture;
#[path = "commands/live.rs"]
mod live;
