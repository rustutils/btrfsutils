//! Integration tests for [`btrfs_fs::Filesystem::send`].
//!
//! Builds a fixture image via `mkfs.btrfs --rootdir`, asks
//! `Filesystem::send` to emit a v1 send stream into a `Vec<u8>`,
//! then re-parses the stream with `btrfs_stream::StreamReader` and
//! asserts the expected sequence of commands appears.
//!
//! Doesn't pipe the stream to a real `btrfs receive` — that needs
//! root and lives in the CLI's privileged round-trip test. The
//! parser-level check catches encoder/walker bugs without
//! privileges.

use btrfs_fs::Filesystem;
use btrfs_stream::{StreamCommand, StreamReader};
use std::{
    fs::{self, File},
    path::{Path, PathBuf},
    sync::OnceLock,
};

/// Build a small fixture: a regular file, an empty file, a larger
/// file, a directory with a nested file, and a symlink. Mirrors
/// `basic.rs::build_fixture` so we exercise the same shapes.
fn build_fixture(base: &Path) -> PathBuf {
    let src = base.join("src");
    fs::create_dir(&src).unwrap();

    fs::write(src.join("hello.txt"), b"hello, world\n").unwrap();
    fs::write(src.join("empty.txt"), b"").unwrap();
    // 100 KiB so we exercise multi-chunk WRITE emission (chunk
    // size is 48 KiB, so we expect at least 3 WRITE commands).
    fs::write(src.join("large.bin"), vec![0x42u8; 100_000]).unwrap();

    let sub = src.join("subdir");
    fs::create_dir(&sub).unwrap();
    fs::write(sub.join("nested.txt"), b"nested content\n").unwrap();

    std::os::unix::fs::symlink("hello.txt", src.join("link")).unwrap();

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

fn open_fixture() -> Filesystem<File> {
    let file = File::open(fixture_path()).unwrap();
    Filesystem::open(file).unwrap()
}

/// Send the default subvolume into a buffer, parse it back, and
/// classify the commands by path so the per-file assertions can
/// inspect each entry's command sequence.
async fn send_and_collect() -> Vec<StreamCommand> {
    let fs = open_fixture();
    let buf = Vec::new();
    let stream = fs.send(fs.default_subvol(), buf).await.expect("send");
    let mut reader =
        StreamReader::new(stream.as_slice()).expect("parse header");
    assert_eq!(reader.version(), 1);
    let mut cmds = Vec::new();
    while let Some(cmd) = reader.next_command().expect("next_command") {
        cmds.push(cmd);
    }
    cmds
}

#[tokio::test]
async fn stream_starts_with_subvol_and_ends_with_end() {
    let cmds = send_and_collect().await;
    assert!(
        matches!(cmds.first(), Some(StreamCommand::Subvol { .. })),
        "first command must be Subvol; got {:?}",
        cmds.first(),
    );
    assert!(
        matches!(cmds.last(), Some(StreamCommand::End)),
        "last command must be End; got {:?}",
        cmds.last(),
    );
}

#[tokio::test]
async fn each_top_level_file_gets_a_creation_command() {
    let cmds = send_and_collect().await;

    let mut has_hello = false;
    let mut has_empty = false;
    let mut has_large = false;
    let mut has_subdir = false;
    let mut has_link = false;
    for cmd in &cmds {
        match cmd {
            StreamCommand::Mkfile { path } if path == "hello.txt" => {
                has_hello = true;
            }
            StreamCommand::Mkfile { path } if path == "empty.txt" => {
                has_empty = true;
            }
            StreamCommand::Mkfile { path } if path == "large.bin" => {
                has_large = true;
            }
            StreamCommand::Mkdir { path } if path == "subdir" => {
                has_subdir = true;
            }
            StreamCommand::Symlink { path, target }
                if path == "link" && target == "hello.txt" =>
            {
                has_link = true;
            }
            _ => {}
        }
    }
    assert!(has_hello, "missing Mkfile for hello.txt");
    assert!(has_empty, "missing Mkfile for empty.txt");
    assert!(has_large, "missing Mkfile for large.bin");
    assert!(has_subdir, "missing Mkdir for subdir");
    assert!(has_link, "missing Symlink for link");
}

#[tokio::test]
async fn nested_file_emitted_under_its_directory() {
    let cmds = send_and_collect().await;
    assert!(
        cmds.iter().any(|c| matches!(
            c,
            StreamCommand::Mkfile { path } if path == "subdir/nested.txt"
        )),
        "missing Mkfile for subdir/nested.txt",
    );
}

#[tokio::test]
async fn small_file_writes_full_content() {
    let cmds = send_and_collect().await;

    let writes: Vec<_> = cmds
        .iter()
        .filter_map(|c| match c {
            StreamCommand::Write { path, offset, data }
                if path == "hello.txt" =>
            {
                Some((*offset, data.clone()))
            }
            _ => None,
        })
        .collect();
    assert_eq!(writes.len(), 1, "expected one Write for hello.txt");
    assert_eq!(writes[0].0, 0);
    assert_eq!(writes[0].1, b"hello, world\n");
}

#[tokio::test]
async fn large_file_chunks_into_multiple_writes() {
    let cmds = send_and_collect().await;

    let mut total_bytes = 0u64;
    let mut chunks = 0usize;
    for cmd in &cmds {
        if let StreamCommand::Write { path, data, .. } = cmd {
            if path == "large.bin" {
                assert!(data.iter().all(|&b| b == 0x42));
                total_bytes += data.len() as u64;
                chunks += 1;
            }
        }
    }
    assert_eq!(total_bytes, 100_000, "expected full 100 KiB written");
    // Chunk size is 48 KiB, so 100 KiB needs >=3 chunks.
    assert!(chunks >= 3, "expected ≥3 chunks for 100 KiB, got {chunks}");
}

#[tokio::test]
async fn empty_file_has_no_writes_but_still_creates_inode() {
    let cmds = send_and_collect().await;

    let writes: Vec<_> = cmds
        .iter()
        .filter(|c| {
            matches!(
                c,
                StreamCommand::Write { path, .. } if path == "empty.txt"
            )
        })
        .collect();
    assert!(writes.is_empty(), "empty file should not emit Write");
    assert!(
        cmds.iter().any(|c| matches!(
            c,
            StreamCommand::Mkfile { path } if path == "empty.txt"
        )),
        "but Mkfile must still appear",
    );
    assert!(
        cmds.iter().any(|c| matches!(
            c,
            StreamCommand::Truncate { path, size } if path == "empty.txt" && *size == 0
        )),
        "and Truncate to 0 must appear",
    );
}

#[tokio::test]
async fn each_inode_gets_chmod_chown_utimes() {
    let cmds = send_and_collect().await;
    for path in [
        "hello.txt",
        "empty.txt",
        "large.bin",
        "subdir",
        "subdir/nested.txt",
        "link",
    ] {
        assert!(
            cmds.iter().any(|c| matches!(
                c,
                StreamCommand::Chmod { path: p, .. } if p == path
            )),
            "missing Chmod for {path}",
        );
        assert!(
            cmds.iter().any(|c| matches!(
                c,
                StreamCommand::Chown { path: p, .. } if p == path
            )),
            "missing Chown for {path}",
        );
        assert!(
            cmds.iter().any(|c| matches!(
                c,
                StreamCommand::Utimes { path: p, .. } if p == path
            )),
            "missing Utimes for {path}",
        );
    }
}
