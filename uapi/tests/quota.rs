use crate::common::single_mount;
use btrfs_uapi::quota::{quota_disable, quota_enable, quota_rescan_wait};
use nix::errno::Errno;

/// quota enable, rescan, and disable should all succeed.
#[test]
#[ignore = "requires elevated privileges"]
fn quota_enable_disable_rescan() {
    let (_td, mnt) = single_mount();

    quota_enable(mnt.fd(), false).expect("quota_enable failed");
    // The kernel auto-starts a rescan when quotas are first enabled, so just
    // wait for it rather than starting a new one (which would fail EINPROGRESS).
    quota_rescan_wait(mnt.fd()).expect("quota_rescan_wait failed");
    quota_disable(mnt.fd()).expect("quota_disable failed");
}

/// Enabling quotas twice should not fail (idempotent).
#[test]
#[ignore = "requires elevated privileges"]
fn quota_double_enable() {
    let (_td, mnt) = single_mount();

    quota_enable(mnt.fd(), false).expect("first quota_enable failed");
    // Second enable should succeed or return a benign error.
    match quota_enable(mnt.fd(), false) {
        Ok(()) => {}
        Err(Errno::EEXIST) => { /* some kernels return EEXIST */ }
        Err(e) => panic!("second quota_enable returned unexpected error: {e}"),
    }
    quota_disable(mnt.fd()).expect("quota_disable failed");
}
