use crate::common::{single_mount, write_test_data};
use btrfs_uapi::sync::sync;

/// sync should succeed without error (smoke test).
#[test]
#[ignore = "requires elevated privileges"]
fn sync_basic() {
    let (_td, mnt) = single_mount();

    write_test_data(mnt.path(), "data.bin", 1_000_000);
    sync(mnt.fd()).expect("sync failed");

    // A second sync should also succeed.
    sync(mnt.fd()).expect("second sync failed");
}
