use crate::common::{single_mount, write_compressible_data};
use btrfs_uapi::{
    defrag,
    filesystem::sync,
    send_receive::encoded_read,
    subvolume::{SubvolumeFlags, subvolume_create, subvolume_flags_set},
};
use std::{ffi::CStr, fs::File, os::unix::io::AsFd};

/// encoded_read on a file with uncompressed data should return compression=0.
#[test]
#[ignore = "requires elevated privileges"]
fn encoded_read_uncompressed() {
    let (_td, mnt) = single_mount();

    let name = CStr::from_bytes_with_nul(b"snap-uncompressed\0").unwrap();
    subvolume_create(mnt.fd(), name, &[]).expect("subvolume_create failed");

    let subvol_path = mnt.path().join("snap-uncompressed");

    // Write some data and sync so it's on disk.
    let file_path = subvol_path.join("data.bin");
    std::fs::write(&file_path, &vec![42u8; 4096]).unwrap();
    sync(mnt.fd()).unwrap();

    // Make the subvolume read-only (encoded_read requires it).
    let subvol_dir = File::open(&subvol_path).unwrap();
    subvolume_flags_set(subvol_dir.as_fd(), SubvolumeFlags::RDONLY)
        .expect("set readonly failed");

    let file = File::open(&file_path).unwrap();
    let mut buf = vec![0u8; 128 * 1024];

    let result = encoded_read(file.as_fd(), &mut buf, 0, 4096);

    match result {
        Ok(r) => {
            assert!(r.bytes_read > 0, "should read some data");
            // Uncompressed data should have compression=0.
            assert_eq!(
                r.compression, 0,
                "uncompressed data should have compression=0"
            );
        }
        Err(nix::errno::Errno::ENOTTY) => {
            eprintln!("encoded_read not supported on this kernel, skipping");
        }
        Err(e) => panic!("encoded_read failed unexpectedly: {e}"),
    }
}

/// encoded_read on a file with compressed data should return the compression type.
#[test]
#[ignore = "requires elevated privileges"]
fn encoded_read_compressed() {
    let (_td, mnt) = single_mount();

    let name = CStr::from_bytes_with_nul(b"snap-compressed\0").unwrap();
    subvolume_create(mnt.fd(), name, &[]).expect("subvolume_create failed");

    let subvol_path = mnt.path().join("snap-compressed");

    // Enable compression on the subvolume by setting the compress property.
    // We do this by mounting with compress option or using defrag.
    // Simplest: write compressible data, then defrag with compression.
    write_compressible_data(&subvol_path, "zeros.bin", 128 * 1024);
    sync(mnt.fd()).unwrap();

    // Defrag with zstd compression to force data to be stored compressed.
    let file = File::options()
        .read(true)
        .write(true)
        .open(subvol_path.join("zeros.bin"))
        .unwrap();
    let _ = defrag::defrag_range(
        file.as_fd(),
        &defrag::DefragRangeArgs::new().compress(defrag::CompressSpec {
            compress_type: defrag::CompressType::Zstd,
            level: None,
        }),
    );
    drop(file);
    sync(mnt.fd()).unwrap();

    // Make subvolume read-only.
    let subvol_dir = File::open(&subvol_path).unwrap();
    subvolume_flags_set(subvol_dir.as_fd(), SubvolumeFlags::RDONLY)
        .expect("set readonly failed");

    let file = File::open(subvol_path.join("zeros.bin")).unwrap();
    let mut buf = vec![0u8; 128 * 1024];

    let result = encoded_read(file.as_fd(), &mut buf, 0, 128 * 1024);

    match result {
        Ok(r) => {
            assert!(r.bytes_read > 0, "should read some data");
            // The data should be compressed (type > 0). The exact algorithm
            // depends on kernel defaults and mount options so we only check
            // that it is not uncompressed.
            assert!(
                r.compression > 0,
                "compressible data after defrag should be stored compressed, got compression={}",
                r.compression,
            );
            assert!(
                r.unencoded_len >= r.unencoded_file_len,
                "unencoded_len ({}) should be >= unencoded_file_len ({})",
                r.unencoded_len,
                r.unencoded_file_len,
            );
        }
        Err(nix::errno::Errno::ENOTTY) => {
            eprintln!("encoded_read not supported on this kernel, skipping");
        }
        Err(e) => panic!("encoded_read failed unexpectedly: {e}"),
    }
}
