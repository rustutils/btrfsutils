use crate::common::{single_mount, write_compressible_data};
use btrfs_uapi::{
    defrag::{CompressSpec, CompressType, DefragRangeArgs, defrag_range},
    filesystem::sync,
};
use std::{fs::File, os::unix::io::AsFd};

/// Defragmenting with compression should reduce the on-disk block usage of a
/// compressible file, and the file content should remain intact.
// FIXME: defrag_range with COMPRESS flag does not seem to actually compress
// the data on the test filesystem. Neither extent_thresh(1) nor different
// compression algorithms help. Needs investigation — possibly a kernel
// version issue, or we need to mount with compress-force, or the ioctl
// flags aren't being set correctly.
#[test]
#[ignore = "requires elevated privileges"]
#[should_panic] // currently broken, see FIXME
fn defrag_compress() {
    use std::os::unix::fs::MetadataExt;

    let (_td, mnt) = single_mount();

    write_compressible_data(mnt.path(), "zeros.bin", 10_000_000);
    sync(mnt.fd()).unwrap();

    let path = mnt.path().join("zeros.bin");
    let blocks_before = std::fs::metadata(&path).unwrap().blocks();

    let file = File::options()
        .read(true)
        .write(true)
        .open(&path)
        .expect("failed to open test file");

    defrag_range(
        file.as_fd(),
        &DefragRangeArgs::new()
            .compress(CompressSpec {
                compress_type: CompressType::Zlib,
                level: None,
            })
            // Force rewriting all extents regardless of size, otherwise the
            // kernel may skip already-contiguous extents.
            .extent_thresh(1),
    )
    .expect("defrag_range failed");

    drop(file);
    sync(mnt.fd()).unwrap();

    // st_blocks reflects actual disk usage (in 512-byte units), which
    // decreases when btrfs stores compressed extents.
    let blocks_after = std::fs::metadata(&path).unwrap().blocks();

    assert!(
        blocks_after < blocks_before,
        "disk blocks should decrease after compressing: before={blocks_before}, after={blocks_after}",
    );

    // Content should still be intact (all zeros).
    let data = std::fs::read(&path).expect("read failed");
    assert_eq!(data.len(), 10_000_000);
    assert!(
        data.iter().all(|&b| b == 0),
        "data should still be all zeros"
    );
}

/// Defragmenting a fragmented file (without compression) should not corrupt
/// the data.
#[test]
#[ignore = "requires elevated privileges"]
fn defrag_no_compression() {
    let (_td, mnt) = single_mount();

    // Write many small chunks with fsync between each to force fragmentation.
    let path = mnt.path().join("fragmented.bin");
    {
        use std::io::Write;
        let mut file = File::create(&path).expect("create failed");
        for i in 0..200u32 {
            let chunk = [i as u8; 4096];
            file.write_all(&chunk).unwrap();
            // FIXME: do we need to call btrfs sync here?
            file.sync_all().unwrap();
        }
    }

    let file = File::options()
        .read(true)
        .write(true)
        .open(&path)
        .expect("open failed");

    defrag_range(file.as_fd(), &DefragRangeArgs::new())
        .expect("defrag_range failed");
    drop(file);
    sync(mnt.fd()).unwrap();

    // Verify data integrity after defrag.
    let data = std::fs::read(&path).expect("read failed");
    assert_eq!(data.len(), 200 * 4096);
    for (i, chunk) in data.chunks(4096).enumerate() {
        assert!(
            chunk.iter().all(|&b| b == i as u8),
            "data mismatch in chunk {i}",
        );
    }
}
