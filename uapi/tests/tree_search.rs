use crate::common::single_mount;
use btrfs_uapi::{
    filesystem::sync,
    subvolume::{subvolume_create, subvolume_info},
    tree_search::{SearchKey, tree_search, tree_search_v2},
};
use std::{
    ffi::{CStr, CString},
    fs::File,
    os::unix::io::AsFd,
};

/// tree_search for ROOT_ITEM_KEY should find root items for created subvolumes.
#[test]
#[ignore = "requires elevated privileges"]
fn tree_search_enumerate_root_items() {
    let (_td, mnt) = single_mount();

    // Create a few subvolumes and record their IDs.
    let mut subvol_ids = Vec::new();
    for name in [
        CStr::from_bytes_with_nul(b"ts-a\0").unwrap(),
        CStr::from_bytes_with_nul(b"ts-b\0").unwrap(),
        CStr::from_bytes_with_nul(b"ts-c\0").unwrap(),
    ] {
        subvolume_create(mnt.fd(), name, &[]).expect("subvolume_create failed");
        let dir = File::open(mnt.path().join(name.to_str().unwrap()))
            .expect("open failed");
        let info = subvolume_info(dir.as_fd()).expect("subvolume_info failed");
        subvol_ids.push(info.id);
    }
    sync(mnt.fd()).unwrap();

    let mut found_ids = Vec::new();
    tree_search(
        mnt.fd(),
        SearchKey::for_type(
            btrfs_uapi::raw::BTRFS_ROOT_TREE_OBJECTID as u64,
            btrfs_uapi::raw::BTRFS_ROOT_ITEM_KEY as u32,
        ),
        |hdr, _data| {
            found_ids.push(hdr.objectid);
            Ok(())
        },
    )
    .expect("tree_search failed");

    for id in &subvol_ids {
        assert!(
            found_ids.contains(id),
            "tree_search should find subvolume {id} in root items: found {found_ids:?}",
        );
    }
}

/// tree_search with an objectid range should only return items within that range.
#[test]
#[ignore = "requires elevated privileges"]
fn tree_search_objectid_range() {
    let (_td, mnt) = single_mount();

    // Create subvolumes to populate the root tree.
    let mut subvol_ids = Vec::new();
    for name in [
        CStr::from_bytes_with_nul(b"range-a\0").unwrap(),
        CStr::from_bytes_with_nul(b"range-b\0").unwrap(),
        CStr::from_bytes_with_nul(b"range-c\0").unwrap(),
    ] {
        subvolume_create(mnt.fd(), name, &[]).expect("subvolume_create failed");
        let dir = File::open(mnt.path().join(name.to_str().unwrap()))
            .expect("open failed");
        let info = subvolume_info(dir.as_fd()).expect("subvolume_info failed");
        subvol_ids.push(info.id);
    }
    sync(mnt.fd()).unwrap();

    // Search for only the first subvolume's objectid.
    let target_id = subvol_ids[0];
    let mut found = Vec::new();
    tree_search(
        mnt.fd(),
        SearchKey::for_objectid_range(
            btrfs_uapi::raw::BTRFS_ROOT_TREE_OBJECTID as u64,
            btrfs_uapi::raw::BTRFS_ROOT_ITEM_KEY as u32,
            target_id,
            target_id,
        ),
        |hdr, _data| {
            found.push(hdr.objectid);
            Ok(())
        },
    )
    .expect("tree_search failed");

    assert!(
        found.contains(&target_id),
        "should find the target objectid {target_id}: {found:?}",
    );
    // Should not contain the other subvolumes.
    for &other_id in &subvol_ids[1..] {
        assert!(
            !found.contains(&other_id),
            "should not find objectid {other_id} outside the range: {found:?}",
        );
    }
}

/// tree_search for a non-existent item type should return Ok(()) with the
/// callback never invoked.
#[test]
#[ignore = "requires elevated privileges"]
fn tree_search_empty_result() {
    let (_td, mnt) = single_mount();

    let mut invoked = false;
    // Objectid 0 is never used for ROOT_ITEM_KEY entries.
    tree_search(
        mnt.fd(),
        SearchKey::for_objectid_range(
            btrfs_uapi::raw::BTRFS_ROOT_TREE_OBJECTID as u64,
            btrfs_uapi::raw::BTRFS_ROOT_ITEM_KEY as u32,
            0,
            0,
        ),
        |_hdr, _data| {
            invoked = true;
            Ok(())
        },
    )
    .expect("tree_search should succeed even with no results");

    assert!(
        !invoked,
        "callback should not be invoked when no items match"
    );
}

/// tree_search with many subvolumes (forcing multiple ioctl batches) should
/// complete without duplicates or infinite loops.
#[test]
#[ignore = "requires elevated privileges"]
fn tree_search_large_result_no_duplicates() {
    let (_td, mnt) = single_mount();

    // Create 60 subvolumes to force multiple search batches.
    let mut expected_ids = Vec::new();
    for i in 0..60 {
        let name = CString::new(format!("sub-{i:03}")).unwrap();
        subvolume_create(mnt.fd(), &name, &[])
            .expect("subvolume_create failed");
        let dir = File::open(mnt.path().join(name.to_str().unwrap()))
            .expect("open failed");
        let info = subvolume_info(dir.as_fd()).expect("subvolume_info failed");
        expected_ids.push(info.id);
    }
    sync(mnt.fd()).unwrap();

    let mut found_items: Vec<(u64, u64)> = Vec::new();
    tree_search(
        mnt.fd(),
        SearchKey::for_type(
            btrfs_uapi::raw::BTRFS_ROOT_TREE_OBJECTID as u64,
            btrfs_uapi::raw::BTRFS_ROOT_ITEM_KEY as u32,
        ),
        |hdr, _data| {
            found_items.push((hdr.objectid, hdr.offset));
            Ok(())
        },
    )
    .expect("tree_search failed");

    // All created subvolumes should appear.
    for id in &expected_ids {
        assert!(
            found_items.iter().any(|(oid, _)| oid == id),
            "tree_search should find subvolume {id}",
        );
    }

    // No duplicates — each (objectid, offset) pair should be unique.
    // Duplicate pairs would indicate the cursor advance bug (infinite loop
    // re-yielding already-seen items).
    let mut seen = std::collections::HashSet::new();
    let mut dup_count = 0;
    for &key in &found_items {
        if !seen.insert(key) {
            dup_count += 1;
        }
    }
    assert_eq!(
        dup_count, 0,
        "should have no duplicate (objectid, offset) pairs, found {dup_count} duplicates"
    );
}

/// tree_search_v2 should return the same items as tree_search (v1).
#[test]
#[ignore = "requires elevated privileges"]
fn tree_search_v2_matches_v1() {
    let (_td, mnt) = single_mount();

    for name in [
        CStr::from_bytes_with_nul(b"v2-a\0").unwrap(),
        CStr::from_bytes_with_nul(b"v2-b\0").unwrap(),
        CStr::from_bytes_with_nul(b"v2-c\0").unwrap(),
    ] {
        subvolume_create(mnt.fd(), name, &[]).expect("subvolume_create failed");
    }
    sync(mnt.fd()).unwrap();

    let key = SearchKey::for_type(
        btrfs_uapi::raw::BTRFS_ROOT_TREE_OBJECTID as u64,
        btrfs_uapi::raw::BTRFS_ROOT_ITEM_KEY as u32,
    );

    let mut v1_items: Vec<(u64, u32, u64)> = Vec::new();
    tree_search(mnt.fd(), key.clone(), |hdr, _data| {
        v1_items.push((hdr.objectid, hdr.item_type, hdr.offset));
        Ok(())
    })
    .expect("tree_search v1 failed");

    let mut v2_items: Vec<(u64, u32, u64)> = Vec::new();
    tree_search_v2(mnt.fd(), key, None, |hdr, _data| {
        v2_items.push((hdr.objectid, hdr.item_type, hdr.offset));
        Ok(())
    })
    .expect("tree_search_v2 failed");

    assert_eq!(v1_items, v2_items, "v2 should return the same items as v1");
}

/// tree_search_v2 with a small buffer should still return all results
/// (automatic retry on EOVERFLOW).
#[test]
#[ignore = "requires elevated privileges"]
fn tree_search_v2_small_buffer() {
    let (_td, mnt) = single_mount();

    for name in [
        CStr::from_bytes_with_nul(b"sb-a\0").unwrap(),
        CStr::from_bytes_with_nul(b"sb-b\0").unwrap(),
    ] {
        subvolume_create(mnt.fd(), name, &[]).expect("subvolume_create failed");
    }
    sync(mnt.fd()).unwrap();

    let key = SearchKey::for_type(
        btrfs_uapi::raw::BTRFS_ROOT_TREE_OBJECTID as u64,
        btrfs_uapi::raw::BTRFS_ROOT_ITEM_KEY as u32,
    );

    // Use a small buffer (one root item is ~439 bytes, so 512 bytes can
    // hold at most one item per batch, forcing multiple batches).
    let mut items: Vec<u64> = Vec::new();
    tree_search_v2(mnt.fd(), key, Some(4096), |hdr, _data| {
        items.push(hdr.objectid);
        Ok(())
    })
    .expect("tree_search_v2 with small buffer failed");

    assert!(
        !items.is_empty(),
        "should find at least some root items even with small buffer"
    );
}

/// tree_search_v2 with a large custom buffer should work.
#[test]
#[ignore = "requires elevated privileges"]
fn tree_search_v2_large_buffer() {
    let (_td, mnt) = single_mount();

    let key = SearchKey::for_type(
        btrfs_uapi::raw::BTRFS_ROOT_TREE_OBJECTID as u64,
        btrfs_uapi::raw::BTRFS_ROOT_ITEM_KEY as u32,
    );

    let mut items: Vec<u64> = Vec::new();
    tree_search_v2(mnt.fd(), key, Some(256 * 1024), |hdr, _data| {
        items.push(hdr.objectid);
        Ok(())
    })
    .expect("tree_search_v2 with large buffer failed");

    // Should find at least FS_TREE (5).
    assert!(
        items.contains(&5),
        "should find FS_TREE objectid 5: {items:?}"
    );
}
