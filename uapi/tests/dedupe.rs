use crate::common::{single_mount, write_test_data};
use btrfs_uapi::{
    dedupe::{DedupeResult, DedupeTarget, file_extent_same},
    filesystem::sync,
};
use std::{
    fs::{self, File},
    os::{
        fd::AsRawFd,
        unix::io::{AsFd, BorrowedFd},
    },
};

/// Deduplicating identical files should report success with bytes deduped.
#[test]
#[ignore = "requires elevated privileges"]
fn dedupe_identical_files() {
    let (_td, mnt) = single_mount();

    // Write two files with identical content.
    write_test_data(mnt.path(), "src.bin", 128 * 1024);
    write_test_data(mnt.path(), "dst.bin", 128 * 1024);
    sync(mnt.fd()).unwrap();

    let src = File::open(mnt.path().join("src.bin")).unwrap();
    let dst = File::options()
        .read(true)
        .write(true)
        .open(mnt.path().join("dst.bin"))
        .unwrap();

    // SAFETY: we need a 'static BorrowedFd for DedupeTarget. The fd lives
    // for the duration of this test, so this is safe.
    let dst_fd = unsafe { BorrowedFd::borrow_raw(AsRawFd::as_raw_fd(&dst)) };

    let targets = [DedupeTarget {
        fd: dst_fd,
        logical_offset: 0,
    }];

    let results = file_extent_same(src.as_fd(), 0, 128 * 1024, &targets)
        .expect("file_extent_same failed");

    assert_eq!(results.len(), 1);
    match results[0] {
        DedupeResult::Deduped(n) => {
            assert!(
                n > 0,
                "expected bytes_deduped > 0 for identical files, got {n}"
            );
        }
        other => panic!("expected Deduped, got {other:?}"),
    }
}

/// Deduplicating files with different content should report DataDiffers.
#[test]
#[ignore = "requires elevated privileges"]
fn dedupe_different_files() {
    let (_td, mnt) = single_mount();

    // Write two files with different content.
    write_test_data(mnt.path(), "src.bin", 128 * 1024);

    // Write a file with all-zero content (different from the pattern in write_test_data).
    let path = mnt.path().join("dst.bin");
    let buf = vec![0u8; 128 * 1024];
    fs::write(&path, &buf).unwrap();
    sync(mnt.fd()).unwrap();

    let src = File::open(mnt.path().join("src.bin")).unwrap();
    let dst = File::options().read(true).write(true).open(&path).unwrap();

    let dst_fd = unsafe { BorrowedFd::borrow_raw(AsRawFd::as_raw_fd(&dst)) };

    let targets = [DedupeTarget {
        fd: dst_fd,
        logical_offset: 0,
    }];

    let results = file_extent_same(src.as_fd(), 0, 128 * 1024, &targets)
        .expect("file_extent_same failed");

    assert_eq!(results.len(), 1);
    assert_eq!(
        results[0],
        DedupeResult::DataDiffers,
        "different data should report DataDiffers"
    );
}

/// Deduplicating with multiple targets should return one result per target.
#[test]
#[ignore = "requires elevated privileges"]
fn dedupe_multiple_targets() {
    let (_td, mnt) = single_mount();

    write_test_data(mnt.path(), "src.bin", 128 * 1024);
    write_test_data(mnt.path(), "dst1.bin", 128 * 1024); // identical
    let different_path = mnt.path().join("dst2.bin");
    fs::write(&different_path, &vec![0u8; 128 * 1024]).unwrap(); // different
    sync(mnt.fd()).unwrap();

    let src = File::open(mnt.path().join("src.bin")).unwrap();
    let dst1 = File::options()
        .read(true)
        .write(true)
        .open(mnt.path().join("dst1.bin"))
        .unwrap();
    let dst2 = File::options()
        .read(true)
        .write(true)
        .open(&different_path)
        .unwrap();

    let dst1_fd = unsafe { BorrowedFd::borrow_raw(AsRawFd::as_raw_fd(&dst1)) };
    let dst2_fd = unsafe { BorrowedFd::borrow_raw(AsRawFd::as_raw_fd(&dst2)) };

    let targets = [
        DedupeTarget {
            fd: dst1_fd,
            logical_offset: 0,
        },
        DedupeTarget {
            fd: dst2_fd,
            logical_offset: 0,
        },
    ];

    let results = file_extent_same(src.as_fd(), 0, 128 * 1024, &targets)
        .expect("file_extent_same failed");

    assert_eq!(results.len(), 2, "should return one result per target");
    match results[0] {
        DedupeResult::Deduped(n) => {
            assert!(n > 0, "identical file should dedupe")
        }
        other => panic!("expected Deduped for dst1, got {other:?}"),
    }
    assert_eq!(
        results[1],
        DedupeResult::DataDiffers,
        "different file should report DataDiffers"
    );
}
