use crate::common::single_mount;
use btrfs_uapi::label::{label_get, label_set};
use std::ffi::{CStr, CString};

/// label_get and label_set should round-trip correctly.
#[test]
#[ignore = "requires elevated privileges"]
fn label_get_set() {
    let (_td, mnt) = single_mount();

    // Fresh filesystem should have an empty label.
    let initial = label_get(mnt.fd()).expect("label_get failed");
    assert!(
        initial.to_bytes().is_empty(),
        "initial label should be empty, got {initial:?}"
    );

    let test_label = CStr::from_bytes_with_nul(b"test-label\0").unwrap();
    label_set(mnt.fd(), test_label).expect("label_set failed");

    let got = label_get(mnt.fd()).expect("label_get after set failed");
    assert_eq!(got.as_c_str(), test_label, "label should round-trip");
}

/// Setting a label at exactly 255 bytes should work, and 256 bytes should fail.
#[test]
#[ignore = "requires elevated privileges"]
fn label_max_length() {
    let (_td, mnt) = single_mount();

    // 255 bytes is the max (BTRFS_LABEL_SIZE is 256 including nul).
    let max_label = "a".repeat(255);
    let max_cstr = CString::new(max_label.clone()).unwrap();
    label_set(mnt.fd(), &max_cstr).expect("label_set with 255 bytes should succeed");

    let got = label_get(mnt.fd()).expect("label_get failed");
    assert_eq!(got.to_bytes().len(), 255);

    // 256 bytes should fail.
    let too_long = "b".repeat(256);
    let too_long_cstr = CString::new(too_long).unwrap();
    let err = label_set(mnt.fd(), &too_long_cstr);
    assert!(err.is_err(), "label_set with 256 bytes should fail");
}

/// Clearing a label by setting it to empty should work.
#[test]
#[ignore = "requires elevated privileges"]
fn label_clear() {
    let (_td, mnt) = single_mount();

    let test_label = CStr::from_bytes_with_nul(b"some-label\0").unwrap();
    label_set(mnt.fd(), test_label).expect("label_set failed");

    let got = label_get(mnt.fd()).expect("label_get failed");
    assert_eq!(got.as_c_str(), test_label);

    let empty = CStr::from_bytes_with_nul(b"\0").unwrap();
    label_set(mnt.fd(), empty).expect("label_set empty failed");

    let cleared = label_get(mnt.fd()).expect("label_get after clear failed");
    assert!(
        cleared.to_bytes().is_empty(),
        "label should be empty after clearing, got {cleared:?}"
    );
}
