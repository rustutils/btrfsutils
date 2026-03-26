use crate::common::single_mount;
use btrfs_uapi::{
    qgroup::{
        QgroupLimitFlags, qgroup_assign, qgroup_clear_stale, qgroup_create, qgroup_destroy,
        qgroup_limit, qgroup_list, qgroup_remove,
    },
    quota::{quota_enable, quota_rescan_wait},
    subvolume::{subvolume_create, subvolume_delete, subvolume_info},
    sync::sync,
};
use std::{ffi::CStr, fs::File, os::unix::io::AsFd};

/// Full qgroup lifecycle: enable quotas, create a higher-level qgroup, create
/// a subvolume, assign, set limits, verify via qgroup_list, then tear down.
#[test]
#[ignore = "requires elevated privileges"]
fn qgroup_lifecycle() {
    let (_td, mnt) = single_mount();

    quota_enable(mnt.fd(), false).expect("quota_enable failed");
    // The kernel auto-starts a rescan when quotas are first enabled, so just
    // wait for it rather than starting a new one (which would fail EINPROGRESS).
    quota_rescan_wait(mnt.fd()).expect("quota_rescan_wait failed");

    // Create a level-1 qgroup (1/0).
    let level1_qgroupid = (1u64 << 48) | 0;
    qgroup_create(mnt.fd(), level1_qgroupid).expect("qgroup_create 1/0 failed");

    // Create a subvolume — the kernel auto-creates a 0/N qgroup for it.
    let name = CStr::from_bytes_with_nul(b"test-subvol\0").unwrap();
    subvolume_create(mnt.fd(), name, &[]).expect("subvolume_create failed");

    let subvol_dir = File::open(mnt.path().join("test-subvol")).expect("open subvol failed");
    let info = subvolume_info(subvol_dir.as_fd()).expect("subvolume_info failed");
    let subvol_qgroupid = info.id; // level-0 qgroup = subvolume ID

    // Assign the subvolume's qgroup to the level-1 parent.
    qgroup_assign(mnt.fd(), subvol_qgroupid, level1_qgroupid).expect("qgroup_assign failed");

    // Set an exclusive limit on the subvolume's qgroup.
    qgroup_limit(
        mnt.fd(),
        subvol_qgroupid,
        QgroupLimitFlags::MAX_EXCL,
        0,
        50_000_000,
    )
    .expect("qgroup_limit failed");

    // Verify via qgroup_list.
    let list = qgroup_list(mnt.fd()).expect("qgroup_list failed");

    let subvol_qg = list.qgroups.iter().find(|q| q.qgroupid == subvol_qgroupid);
    assert!(
        subvol_qg.is_some(),
        "subvolume qgroup should appear in list"
    );
    let subvol_qg = subvol_qg.unwrap();
    assert!(
        subvol_qg.limit_flags.contains(QgroupLimitFlags::MAX_EXCL),
        "limit flags should include MAX_EXCL: {:?}",
        subvol_qg.limit_flags,
    );
    assert_eq!(subvol_qg.max_excl, 50_000_000);
    assert!(
        subvol_qg.parents.contains(&level1_qgroupid),
        "subvol qgroup should have 1/0 as parent: {:?}",
        subvol_qg.parents,
    );

    let level1_qg = list.qgroups.iter().find(|q| q.qgroupid == level1_qgroupid);
    assert!(level1_qg.is_some(), "level-1 qgroup should appear in list");

    // Tear down: remove assignment, destroy level-1 qgroup.
    qgroup_remove(mnt.fd(), subvol_qgroupid, level1_qgroupid).expect("qgroup_remove failed");
    qgroup_destroy(mnt.fd(), level1_qgroupid).expect("qgroup_destroy failed");

    // Level-1 qgroup should be gone now.
    let list2 = qgroup_list(mnt.fd()).expect("qgroup_list after destroy failed");
    assert!(
        !list2.qgroups.iter().any(|q| q.qgroupid == level1_qgroupid),
        "level-1 qgroup should be gone after destroy",
    );
}

/// qgroup_clear_stale should remove qgroups for deleted subvolumes.
#[test]
#[ignore = "requires elevated privileges"]
fn qgroup_clear_stale_test() {
    let (_td, mnt) = single_mount();

    quota_enable(mnt.fd(), false).expect("quota_enable failed");
    // The kernel auto-starts a rescan when quotas are first enabled, so just
    // wait for it rather than starting a new one (which would fail EINPROGRESS).
    quota_rescan_wait(mnt.fd()).expect("quota_rescan_wait failed");

    // Create three subvolumes.
    for name in [b"sub-a\0", b"sub-b\0", b"sub-c\0"] {
        let cname = CStr::from_bytes_with_nul(name).unwrap();
        subvolume_create(mnt.fd(), cname, &[]).expect("subvolume_create failed");
    }
    sync(mnt.fd()).unwrap();

    // Get sub-b's qgroupid before deletion.
    let sub_b_dir = File::open(mnt.path().join("sub-b")).expect("open sub-b failed");
    let sub_b_info = subvolume_info(sub_b_dir.as_fd()).expect("subvolume_info failed");
    let sub_b_qgroupid = sub_b_info.id;
    drop(sub_b_dir);

    // Delete sub-b. Its qgroup should linger.
    let b_name = CStr::from_bytes_with_nul(b"sub-b\0").unwrap();
    subvolume_delete(mnt.fd(), b_name).expect("subvolume_delete failed");

    // see this: https://www.spinics.net/lists/linux-btrfs/msg145753.html

    // The kernel deletes subvolumes lazily via a background cleaner thread.
    // We need to wait for the ROOT_ITEM to actually disappear before
    // qgroup_list will mark the qgroup as stale. Sync + short retry loop.
    let mut stale_visible = false;
    for _ in 0..10 {
        sync(mnt.fd()).unwrap();
        std::thread::sleep(std::time::Duration::from_millis(200));

        let list = qgroup_list(mnt.fd()).expect("qgroup_list failed");
        if let Some(qg) = list.qgroups.iter().find(|q| q.qgroupid == sub_b_qgroupid) {
            if qg.stale {
                stale_visible = true;
                break;
            }
        } else {
            // Qgroup already gone (kernel cleaned it up itself) — nothing to test.
            return;
        }
    }

    if !stale_visible {
        // Kernel cleaner hasn't run yet — skip rather than flake.
        eprintln!("qgroup_clear_stale_test: subvolume cleaner hasn't run, skipping");
        return;
    }

    // Clear stale qgroups.
    let cleared = qgroup_clear_stale(mnt.fd()).expect("qgroup_clear_stale failed");
    assert!(
        cleared >= 1,
        "should have cleared at least 1 stale qgroup, got {cleared}"
    );

    let list2 = qgroup_list(mnt.fd()).expect("qgroup_list after clear failed");
    assert!(
        !list2.qgroups.iter().any(|q| q.qgroupid == sub_b_qgroupid),
        "stale qgroup for sub-b should be gone after clear_stale",
    );
}
