//! Multi-subvolume traversal coverage.
//!
//! Fixture layout (built by `mkfs.btrfs --rootdir --subvol`):
//!
//! ```text
//! /                              (default FS_TREE, id 5)
//! ├── top.txt                    (regular file in default subvol)
//! ├── sub1/                      (subvolume — id assigned by mkfs)
//! │   ├── inside.txt
//! │   └── nested/                (subvolume nested under sub1)
//! │       └── deep.txt
//! └── sub2/                      (subvolume)
//!     └── other.txt
//! ```
//!
//! Tests resolve names → ids via `list_subvolumes` rather than
//! hard-coding ids; mkfs's id assignment order is implementation
//! defined (it's alphabetical-by-source-inode in practice, not arg
//! order). What matters is that the *relationships* are right:
//! sub1 and sub2 are top-level, nested is nested under sub1.

use btrfs_fs::{Filesystem, Inode, SubvolId, SubvolInfo};
use std::{
    fs::{self, File},
    path::{Path, PathBuf},
    sync::OnceLock,
};

fn build_fixture(base: &Path) -> PathBuf {
    let src = base.join("src");
    fs::create_dir(&src).unwrap();

    fs::write(src.join("top.txt"), b"in default subvol\n").unwrap();

    let sub1 = src.join("sub1");
    fs::create_dir(&sub1).unwrap();
    fs::write(sub1.join("inside.txt"), b"inside sub1\n").unwrap();

    let nested = sub1.join("nested");
    fs::create_dir(&nested).unwrap();
    fs::write(nested.join("deep.txt"), b"deep in nested\n").unwrap();

    let sub2 = src.join("sub2");
    fs::create_dir(&sub2).unwrap();
    fs::write(sub2.join("other.txt"), b"in sub2\n").unwrap();

    let img = base.join("test.img");
    File::create(&img)
        .unwrap()
        .set_len(128 * 1024 * 1024)
        .unwrap();
    btrfs_test_utils::run(
        "mkfs.btrfs",
        &[
            "-f",
            "--rootdir",
            src.to_str().unwrap(),
            // Order matters: a parent subvolume must be declared
            // before its nested children.
            "--subvol",
            "sub1",
            "--subvol",
            "sub1/nested",
            "--subvol",
            "sub2",
            img.to_str().unwrap(),
        ],
    );
    img
}

fn fixture_path() -> &'static Path {
    static INIT: OnceLock<(tempfile::TempDir, PathBuf)> = OnceLock::new();
    let (_td, path) = INIT.get_or_init(|| {
        let td = tempfile::tempdir().unwrap();
        let img = build_fixture(td.path());
        (td, img)
    });
    path
}

fn open() -> Filesystem<File> {
    Filesystem::open(File::open(fixture_path()).unwrap()).unwrap()
}

/// Look up a subvolume by name from the result of `list_subvolumes`.
fn find_by_name<'a>(list: &'a [SubvolInfo], name: &[u8]) -> &'a SubvolInfo {
    list.iter()
        .find(|s| s.name == name)
        .unwrap_or_else(|| panic!("subvol {:?} not in list", name))
}

// ── list_subvolumes ────────────────────────────────────────────────

#[tokio::test]
async fn list_subvolumes_includes_default_and_user_subvols() {
    let fs = open();
    let subvols = fs.list_subvolumes().await.unwrap();

    // Default FS_TREE plus three user subvols.
    assert_eq!(subvols.len(), 4, "expected 4 subvols, got {subvols:#?}");

    let default = subvols.iter().find(|s| s.id.0 == 5).unwrap();
    assert!(default.parent.is_none());
    assert!(default.name.is_empty());
    assert!(!default.readonly);

    // Top-level user subvols (sub1, sub2) live under FS_TREE → parent
    // should be SubvolId(5). The nested subvol lives under sub1.
    let sub1 = find_by_name(&subvols, b"sub1");
    let sub2 = find_by_name(&subvols, b"sub2");
    let nested = find_by_name(&subvols, b"nested");

    assert_eq!(sub1.parent, Some(SubvolId(5)));
    assert_eq!(sub2.parent, Some(SubvolId(5)));
    assert_eq!(
        nested.parent,
        Some(sub1.id),
        "nested should be under sub1 ({:?}); got parent={:?}",
        sub1.id,
        nested.parent,
    );
}

// ── lookup crossing ───────────────────────────────────────────────

#[tokio::test]
async fn lookup_into_subvol_returns_inode_in_new_tree() {
    let fs = open();
    let root = fs.root();

    let subvols = fs.list_subvolumes().await.unwrap();
    let sub1 = find_by_name(&subvols, b"sub1");

    let (sub1_ino, _) = fs.lookup(root, b"sub1").await.unwrap().unwrap();

    // Crossed into sub1: child inode is the subvol root (objectid 256).
    assert_eq!(sub1_ino.subvol, sub1.id);
    assert_eq!(sub1_ino.ino, 256);

    // Reading inside.txt should now use sub1's tree, not the default.
    let (file_ino, _) =
        fs.lookup(sub1_ino, b"inside.txt").await.unwrap().unwrap();
    assert_eq!(file_ino.subvol, sub1.id);
    let content = fs.read(file_ino, 0, 64).await.unwrap();
    assert_eq!(content, b"inside sub1\n");
}

#[tokio::test]
async fn lookup_into_nested_subvol() {
    let fs = open();
    let root = fs.root();
    let subvols = fs.list_subvolumes().await.unwrap();
    let sub1 = find_by_name(&subvols, b"sub1");
    let nested = find_by_name(&subvols, b"nested");

    let (sub1_ino, _) = fs.lookup(root, b"sub1").await.unwrap().unwrap();
    let (nested_ino, _) =
        fs.lookup(sub1_ino, b"nested").await.unwrap().unwrap();

    assert_eq!(nested_ino.subvol, nested.id);
    assert_eq!(nested_ino.ino, 256);
    let _ = sub1; // ensure sub1 was looked up; ID matched above

    let (deep_ino, _) =
        fs.lookup(nested_ino, b"deep.txt").await.unwrap().unwrap();
    assert_eq!(deep_ino.subvol, nested.id);
    let content = fs.read(deep_ino, 0, 64).await.unwrap();
    assert_eq!(content, b"deep in nested\n");
}

// ── readdir + .. across subvols ───────────────────────────────────

#[tokio::test]
async fn readdir_subvol_root_lists_subvol_contents() {
    let fs = open();
    let root = fs.root();
    let (sub1_ino, _) = fs.lookup(root, b"sub1").await.unwrap().unwrap();
    let entries = fs.readdir(sub1_ino, 0).await.unwrap();
    let names: Vec<&[u8]> = entries.iter().map(|e| e.name.as_slice()).collect();

    assert!(names.iter().any(|&n| n == b"."));
    assert!(names.iter().any(|&n| n == b".."));
    assert!(names.iter().any(|&n| n == b"inside.txt"));
    assert!(
        names.iter().any(|&n| n == b"nested"),
        "subvol root should expose the nested subvol entry; got {names:?}",
    );
}

#[tokio::test]
async fn dotdot_from_subvol_root_resolves_via_root_backref() {
    let fs = open();
    let root = fs.root();
    let (sub1_ino, _) = fs.lookup(root, b"sub1").await.unwrap().unwrap();
    let entries = fs.readdir(sub1_ino, 0).await.unwrap();
    let dotdot = entries.iter().find(|e| e.name == b"..").unwrap();

    // sub1's parent is the FS_TREE root that contains sub1.
    assert_eq!(
        dotdot.ino, root,
        "expected `..` in sub1 to resolve to FS_TREE root; got {:?}",
        dotdot.ino,
    );
}

#[tokio::test]
async fn dotdot_from_nested_subvol_root_resolves_to_parent_subvol() {
    let fs = open();
    let root = fs.root();
    let subvols = fs.list_subvolumes().await.unwrap();
    let sub1 = find_by_name(&subvols, b"sub1");

    let (sub1_ino, _) = fs.lookup(root, b"sub1").await.unwrap().unwrap();
    let (nested_ino, _) =
        fs.lookup(sub1_ino, b"nested").await.unwrap().unwrap();

    let entries = fs.readdir(nested_ino, 0).await.unwrap();
    let dotdot = entries.iter().find(|e| e.name == b"..").unwrap();

    // nested's parent is the directory inside sub1 that contains it
    // — the sub1 root (objectid 256 in sub1's tree).
    assert_eq!(
        dotdot.ino,
        Inode {
            subvol: sub1.id,
            ino: 256,
        },
        "expected `..` in nested to resolve to sub1 root; got {:?}",
        dotdot.ino,
    );
}

// ── open_subvol ────────────────────────────────────────────────────

#[tokio::test]
async fn open_subvol_uses_alternative_root() {
    // Bootstrap once with the default subvol to discover sub1's id,
    // then re-open targeting that subvol directly.
    let probe = open();
    let subvols = probe.list_subvolumes().await.unwrap();
    let sub1 = find_by_name(&subvols, b"sub1");
    drop(probe);

    let fs =
        Filesystem::open_subvol(File::open(fixture_path()).unwrap(), sub1.id)
            .unwrap();
    let root = fs.root();
    assert_eq!(root.subvol, sub1.id);
    assert_eq!(root.ino, 256);

    // From this root, `inside.txt` is reachable directly (no need to
    // descend through `sub1` first).
    let (ino, _) = fs.lookup(root, b"inside.txt").await.unwrap().unwrap();
    let content = fs.read(ino, 0, 64).await.unwrap();
    assert_eq!(content, b"inside sub1\n");
}

#[tokio::test]
async fn open_subvol_unknown_id_errors() {
    let result = Filesystem::open_subvol(
        File::open(fixture_path()).unwrap(),
        SubvolId(999_999),
    );
    assert!(result.is_err());
    let err = result.err().unwrap();
    assert_eq!(err.kind(), std::io::ErrorKind::NotFound);
}

#[tokio::test]
async fn open_subvol_invalid_id_errors() {
    // 1 is the root tree id — not a subvolume.
    let result = Filesystem::open_subvol(
        File::open(fixture_path()).unwrap(),
        SubvolId(1),
    );
    assert!(result.is_err());
    let err = result.err().unwrap();
    assert_eq!(err.kind(), std::io::ErrorKind::InvalidInput);
}
