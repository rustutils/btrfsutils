use crate::common::{single_mount, write_test_data};
use btrfs_uapi::{
    filesystem::fs_info,
    quota::{quota_disable, quota_enable, quota_rescan_wait},
    sync::sync,
    sysfs::SysfsBtrfs,
};

/// SysfsBtrfs should read filesystem properties that match fs_info.
#[test]
#[ignore = "requires elevated privileges"]
fn sysfs_read_info() {
    let (_td, mnt) = single_mount();

    let info = fs_info(mnt.fd()).expect("fs_info failed");
    let sysfs = SysfsBtrfs::new(&info.uuid);

    let nodesize = sysfs.nodesize().expect("sysfs nodesize failed");
    assert_eq!(
        nodesize, info.nodesize as u64,
        "sysfs nodesize should match fs_info",
    );

    let sectorsize = sysfs.sectorsize().expect("sysfs sectorsize failed");
    assert_eq!(
        sectorsize, info.sectorsize as u64,
        "sysfs sectorsize should match fs_info",
    );

    let metadata_uuid = sysfs.metadata_uuid().expect("sysfs metadata_uuid failed");
    // metadata_uuid equals fsid when no separate metadata UUID is set.
    assert_eq!(
        metadata_uuid, info.uuid,
        "sysfs metadata_uuid should match fs_info uuid (no separate metadata uuid set)",
    );
}

/// SysfsBtrfs commit_stats should show commits after writes + sync.
#[test]
#[ignore = "requires elevated privileges"]
fn sysfs_commit_stats() {
    let (_td, mnt) = single_mount();

    write_test_data(mnt.path(), "data.bin", 1_000_000);
    sync(mnt.fd()).unwrap();

    let info = fs_info(mnt.fd()).expect("fs_info failed");
    let sysfs = SysfsBtrfs::new(&info.uuid);

    let stats = sysfs.commit_stats().expect("commit_stats failed");
    assert!(
        stats.commits > 0,
        "should have at least one commit after write+sync: {stats:?}",
    );
}

/// Quota status should be visible via sysfs after enabling/disabling quotas.
#[test]
#[ignore = "requires elevated privileges"]
fn sysfs_quota_status() {
    let (_td, mnt) = single_mount();

    let info = fs_info(mnt.fd()).expect("fs_info failed");
    let sysfs = SysfsBtrfs::new(&info.uuid);

    // Quotas should be disabled initially.
    let status = sysfs.quota_status().expect("quota_status failed");
    assert!(!status.enabled, "quotas should be disabled initially");

    // Enable quotas.
    quota_enable(mnt.fd(), false).expect("quota_enable failed");
    quota_rescan_wait(mnt.fd()).expect("quota_rescan_wait failed");

    let status = sysfs
        .quota_status()
        .expect("quota_status after enable failed");
    assert!(
        status.enabled,
        "quotas should be enabled after quota_enable"
    );

    // Disable quotas.
    quota_disable(mnt.fd()).expect("quota_disable failed");

    let status = sysfs
        .quota_status()
        .expect("quota_status after disable failed");
    assert!(
        !status.enabled,
        "quotas should be disabled after quota_disable"
    );
}
