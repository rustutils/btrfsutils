//! Compression coverage: zlib, zstd, and LZO read paths.
//!
//! Each algorithm gets its own fixture image built once per test
//! process via `mkfs.btrfs --rootdir --compress <algo>`. The fixture
//! contains files that exercise:
//!
//! - inline compressed extents (small file, well below `max_inline`)
//! - regular compressed extents on highly-compressible data (`zeros`)
//! - regular extents on incompressible data (`random`): btrfs may
//!   store these uncompressed even with `--compress`, so this also
//!   covers extents with compression `None` on a compressed image
//! - multi-extent files (large enough to span several 128 KiB
//!   compression chunks)
//!
//! The `compression_suite!` macro generates the tests for each
//! algorithm, so the assertions aren't duplicated by hand. LZO has
//! more framing edge cases than zlib and zstd, so a bug in
//! `decompress_lzo` should show up in the `lzo` suite first.

use btrfs_fs::Filesystem;
use std::{
    fs::{self, File},
    path::{Path, PathBuf},
    sync::OnceLock,
};

/// 1 MiB of zeros. Highly compressible, so it should produce a tiny
/// compressed extent.
const ZEROS_LEN: usize = 1024 * 1024;

/// 1 MiB of pseudo-random bytes. Doesn't compress meaningfully, so
/// btrfs usually stores it uncompressed.
const RANDOM_LEN: usize = 1024 * 1024;

/// 16 MiB. btrfs compresses in 128 KiB chunks, so any larger file has
/// multiple compressed extents that a single read can straddle.
const PATTERN_LEN: usize = 16 * 1024 * 1024;

/// Inline file payload: short and highly compressible. The default
/// `max_inline` is 2048, so 240 bytes is comfortably inline.
const INLINE_PAYLOAD: &[u8] =
    b"compress me. compress me. compress me. compress me. \
      compress me. compress me. compress me. compress me. \
      compress me. compress me. compress me. compress me. \
      compress me. compress me. compress me. compress me. \
      compress me.";

/// Generate the random-data fixture deterministically so tests assert
/// against a stable byte sequence.
fn random_bytes() -> Vec<u8> {
    let mut out = vec![0u8; RANDOM_LEN];
    let mut state: u32 = 0x1234_5678;
    for byte in &mut out {
        state = state.wrapping_mul(1_103_515_245).wrapping_add(12345);
        *byte = (state >> 16) as u8;
    }
    out
}

/// 16 MiB pattern with a different byte value in each 1 MiB block,
/// so a straddling read can verify both sides of the boundary.
fn pattern_bytes() -> Vec<u8> {
    let mut out = Vec::with_capacity(PATTERN_LEN);
    for i in 0..16u8 {
        out.extend(std::iter::repeat_n(i, 1024 * 1024));
    }
    out
}

fn build_compression_fixture(base: &Path, algo: &str) -> PathBuf {
    let src = base.join("src");
    fs::create_dir(&src).unwrap();

    fs::write(src.join("inline.txt"), INLINE_PAYLOAD).unwrap();
    fs::write(src.join("zeros_1m.bin"), vec![0u8; ZEROS_LEN]).unwrap();
    fs::write(src.join("random_1m.bin"), random_bytes()).unwrap();
    fs::write(src.join("pattern_16m.bin"), pattern_bytes()).unwrap();

    let img = base.join("test.img");
    File::create(&img)
        .unwrap()
        .set_len(256 * 1024 * 1024)
        .unwrap();
    btrfs_test_utils::run(
        "mkfs.btrfs",
        &[
            "-f",
            "--rootdir",
            src.to_str().unwrap(),
            "--compress",
            algo,
            img.to_str().unwrap(),
        ],
    );
    img
}

/// Per-algorithm `OnceLock` fixture cache. Each algorithm builds its
/// fixture on first use and shares it across its suite.
fn fixture_for(algo: &str) -> &'static Path {
    static ZLIB: OnceLock<(tempfile::TempDir, PathBuf)> = OnceLock::new();
    static ZSTD: OnceLock<(tempfile::TempDir, PathBuf)> = OnceLock::new();
    static LZO: OnceLock<(tempfile::TempDir, PathBuf)> = OnceLock::new();
    let lock = match algo {
        "zlib" => &ZLIB,
        "zstd" => &ZSTD,
        "lzo" => &LZO,
        _ => panic!("unknown compression algorithm: {algo}"),
    };
    let (_td, path) = lock.get_or_init(|| {
        let td = tempfile::tempdir().unwrap();
        let img = build_compression_fixture(td.path(), algo);
        (td, img)
    });
    path
}

fn open(algo: &str) -> Filesystem<File> {
    Filesystem::open(File::open(fixture_for(algo)).unwrap()).unwrap()
}

/// Generate the same suite of tests for each compression algorithm.
/// Test bodies are identical. Only the fixture differs.
macro_rules! compression_suite {
    ($module:ident, $algo:literal) => {
        mod $module {
            use super::*;

            #[tokio::test]
            async fn read_inline_full() {
                let fs = open($algo);
                let root = fs.root();
                let (ino, _) =
                    fs.lookup(root, b"inline.txt").await.unwrap().unwrap();
                let data = fs
                    .read(ino, 0, INLINE_PAYLOAD.len() as u32 + 16)
                    .await
                    .unwrap();
                assert_eq!(data, INLINE_PAYLOAD);
            }

            #[tokio::test]
            async fn read_inline_partial_offset() {
                let fs = open($algo);
                let root = fs.root();
                let (ino, _) =
                    fs.lookup(root, b"inline.txt").await.unwrap().unwrap();
                let data = fs.read(ino, 10, 20).await.unwrap();
                assert_eq!(data, &INLINE_PAYLOAD[10..30]);
            }

            #[tokio::test]
            async fn read_zeros_full() {
                let fs = open($algo);
                let root = fs.root();
                let (ino, _) =
                    fs.lookup(root, b"zeros_1m.bin").await.unwrap().unwrap();
                #[allow(clippy::cast_possible_truncation)]
                let data = fs.read(ino, 0, ZEROS_LEN as u32).await.unwrap();
                assert_eq!(data.len(), ZEROS_LEN);
                assert!(data.iter().all(|&b| b == 0));
            }

            #[tokio::test]
            async fn read_zeros_partial_offset() {
                let fs = open($algo);
                let root = fs.root();
                let (ino, _) =
                    fs.lookup(root, b"zeros_1m.bin").await.unwrap().unwrap();
                // 100 KiB read from the middle of the file. With
                // 128 KiB compression chunks this likely straddles
                // chunk boundary 4 (512 KiB), which exercises the
                // decompress-then-slice path.
                let data = fs.read(ino, 500_000, 100_000).await.unwrap();
                assert_eq!(data.len(), 100_000);
                assert!(data.iter().all(|&b| b == 0));
            }

            #[tokio::test]
            async fn read_random_full() {
                let fs = open($algo);
                let root = fs.root();
                let (ino, _) =
                    fs.lookup(root, b"random_1m.bin").await.unwrap().unwrap();
                #[allow(clippy::cast_possible_truncation)]
                let data = fs.read(ino, 0, RANDOM_LEN as u32).await.unwrap();
                assert_eq!(data, random_bytes());
            }

            #[tokio::test]
            async fn read_random_partial_offset() {
                let fs = open($algo);
                let root = fs.root();
                let (ino, _) =
                    fs.lookup(root, b"random_1m.bin").await.unwrap().unwrap();
                let data = fs.read(ino, 200_000, 50_000).await.unwrap();
                assert_eq!(data, &random_bytes()[200_000..250_000]);
            }

            #[tokio::test]
            async fn read_pattern_full() {
                let fs = open($algo);
                let root = fs.root();
                let (ino, _) =
                    fs.lookup(root, b"pattern_16m.bin").await.unwrap().unwrap();
                #[allow(clippy::cast_possible_truncation)]
                let data = fs.read(ino, 0, PATTERN_LEN as u32).await.unwrap();
                assert_eq!(data, pattern_bytes());
            }

            /// Read straddling a 1 MiB pattern boundary and a 128 KiB
            /// compression chunk boundary at the same time. Checks the
            /// decompress-and-slice math across both kinds of boundary.
            #[tokio::test]
            async fn read_pattern_straddle_boundary() {
                let fs = open($algo);
                let root = fs.root();
                let (ino, _) =
                    fs.lookup(root, b"pattern_16m.bin").await.unwrap().unwrap();
                // Start 100 KiB before the 1 MiB mark and read 200 KiB.
                // The first half is byte 0x00, the second half 0x01.
                let off = 1_048_576 - 100_000;
                let data = fs.read(ino, off as u64, 200_000).await.unwrap();
                assert_eq!(data.len(), 200_000);
                let pattern = pattern_bytes();
                assert_eq!(&data[..], &pattern[off..off + 200_000]);
            }

            #[tokio::test]
            async fn read_pattern_last_byte() {
                let fs = open($algo);
                let root = fs.root();
                let (ino, _) =
                    fs.lookup(root, b"pattern_16m.bin").await.unwrap().unwrap();
                let data =
                    fs.read(ino, (PATTERN_LEN - 1) as u64, 16).await.unwrap();
                assert_eq!(data, &[0x0fu8]);
            }

            #[tokio::test]
            async fn read_at_eof_returns_empty() {
                let fs = open($algo);
                let root = fs.root();
                let (ino, _) =
                    fs.lookup(root, b"zeros_1m.bin").await.unwrap().unwrap();
                #[allow(clippy::cast_possible_truncation)]
                let data = fs.read(ino, ZEROS_LEN as u64, 1024).await.unwrap();
                assert!(data.is_empty());
            }

            #[tokio::test]
            async fn read_past_eof_returns_empty() {
                let fs = open($algo);
                let root = fs.root();
                let (ino, _) =
                    fs.lookup(root, b"zeros_1m.bin").await.unwrap().unwrap();
                let data = fs
                    .read(ino, (ZEROS_LEN as u64) + 4096, 1024)
                    .await
                    .unwrap();
                assert!(data.is_empty());
            }
        }
    };
}

compression_suite!(zlib, "zlib");
compression_suite!(zstd, "zstd");
compression_suite!(lzo, "lzo");
