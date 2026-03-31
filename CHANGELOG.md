# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

## [Unreleased]

### Added
- `btrfs-tune` crate: offline superblock tuning tool with feature flag
  enabling (`-r`, `-x`, `-n`), seeding flag management (`-S`),
  metadata UUID change (`-m`, `-M UUID`), and full fsid rewrite
  (`-u`, `-U UUID`)
- Man page generation for `btrfs-tune`
- `btrfs-disk`: `BlockReader::write_block` for writing tree blocks by
  logical address, `csum_tree_block` for recomputing tree block checksums

## [0.6.0] — 2026-03-30

### Added
- `btrfs inspect-internal tree-stats`: walk any B-tree and report node counts,
  seek statistics, cluster sizes, and inline data bytes
- `btrfs rescue super-recover`: scan all superblock mirrors and restore the
  highest-generation copy to all mirror locations
- `btrfs rescue zero-log`: clear the log tree root pointer to allow mounting
  filesystems with corrupted log trees
- `btrfs rescue create-control-device`: create `/dev/btrfs-control` if missing
- Shell completion generation for bash, fish, and zsh via `btrfs-gen completions`
- `nix flake check` now runs clippy, rustfmt, and unit tests
- taplo for TOML formatting and linting
- cargo-deny for dependency license and vulnerability auditing
- Developer and user documentation (mdBook)
- `LICENSE.md` files for all crates
- `BlockGroupFlags` and five additional bitflags types in `btrfs-disk` for
  typed on-disk field parsing
- Integration tests adapted from btrfs-progs cli-tests (mkfs validation,
  on-disk format verification, nodesize/sectorsize matrix)
- mkfs library now validates nodesize, sectorsize, and mixed-bg constraints
  (previously only validated in the CLI entry point)

### Changed
- CI migrated from manual cargo invocations to `nix flake check`
- `mangen` binary renamed to `btrfs-gen` to consolidate generation tools
- `btrfs-mkfs` binary renamed from `mkfs-btrfs` for consistency
- `btrfs-disk` API renamed: `Dev` -> `Device`, `Fs` -> `Filesystem`, function
  names follow `noun_verb` convention
- LZO decompression switched from `lzo1x` to `lzokay` crate
- Pedantic clippy lints fixed across `btrfs-disk`

### Fixed
- LZO decompression sector alignment in `btrfs receive` for streams with
  non-4096-byte sectors

### Changed (licensing)
- `btrfs-uapi`, `btrfs-disk`, and `btrfs-stream` relicensed from GPL-2.0-only
  to MIT OR Apache-2.0 so they can be used as library dependencies by non-GPL projects

---

## [0.5.0] — 2026-03-29

### Added
- `mkfs.btrfs` multi-device support with RAID1 metadata (Phase 5): per-device
  superblocks, `DEV_ITEM` and `DEV_EXTENT` entries for all devices, block group
  flags derived from configured profiles
- `mkfs.btrfs` all four checksum algorithms: crc32c, xxhash, sha256, blake2b (Phase 7)
- `mkfs.btrfs` block-group-tree feature flag enabled by default (Phase 4)
- `mkfs.btrfs` RAID0 data profile support
- `mkfs.btrfs` RAID1C3 and RAID1C4 metadata profile support
- `mkfs.btrfs` writes superblock to all three mirror locations
- `mkfs.btrfs` man page generation
- Integration tests for `btrfs replace start/status/cancel`
- `btrfs device remove --enqueue` flag
- `btrfs inspect-internal list-chunks --sort` flag
- `btrfs filesystem df/show`, `list-chunks`, `scrub status/limit` now support unit
  flags (`-b`, `-H`, `--iec`, `--si`, `-k/-m/-g/-t`)
- Multi-level `-v`/`-q` verbose/quiet flags via `env_logger`

### Changed
- `btrfs filesystem defrag` and `btrfs subvolume delete` no longer declare their
  own `-v` flag; verbosity is controlled by the global flag
- Stream parser now uses typed `StreamError` instead of `anyhow::Error`
- Size formatting unified under a single `SizeFormat` enum and `fmt_size()` helper
- Time formatting consolidated to use `chrono` throughout
- LE reader helpers deduplicated into `uapi/src/util.rs` and `disk/src/util.rs`

### Fixed
- Nix build: include fixture image, add gzip dependency, pin test timezone

---

## [0.4.0] — 2026-03-28

### Added
- `btrfs inspect-internal dump-tree`: full on-disk tree dumper reading directly from
  block device or image file (no `CAP_SYS_ADMIN` required); bootstrap via
  superblock → sys_chunk_array → chunk tree → root tree; 30+ item type formatters
  matching the C reference output exactly; `-t`, `-b`, `--follow`, `--bfs`/`--dfs`,
  `--hide-names`, `--csum-headers`, `--csum-items`, `-e/-d/-u/-r/-R` flags
- `mkfs.btrfs` initial implementation (Phases 1–3): valid mountable single-device
  filesystem with metadata DUP and data SINGLE block groups; device validation and
  feature flag wiring; writes 8 tree blocks + superblock with CRC32C checksums
- `btrfs scrub start/resume` missing flags: `-B`, `-d`, `-R`, `-f`, `--limit`,
  `-c`/`-n` (ioprio class/classdata)
- Ioctl wrappers: `BTRFS_IOC_INO_LOOKUP_USER`, `BTRFS_IOC_GET_FEATURES`,
  `BTRFS_IOC_GET_SUPPORTED_FEATURES`, `BTRFS_IOC_SUBVOL_SYNC_WAIT`
- Nix flake for reproducible builds and dev shell
- CI configuration
- Fixture snapshot tests for `subvolume list/show`, `dump-super`, and quota commands

### Changed
- `dump-super` display logic moved from `disk/` to `cli/` to keep the disk crate
  free of formatting code

---

## [0.3.0] — 2026-03-26

### Added
- `btrfs-stream` extracted as a standalone crate with platform-independent send
  stream parser and CRC32C validation; `receive` feature (Linux-only) adds
  `ReceiveContext`
- `btrfs receive`: v2 stream commands (`ENCODED_WRITE` with decompression fallback
  for zlib/zstd/lzo, `FALLOCATE`, `FILEATTR`); v3 `ENABLE_VERITY`; `--chroot` mode
- `btrfs subvolume create`: `-i`/`--qgroup` and `-p`/`--parents` flags
- `btrfs subvolume delete`: `-c`/`--commit-after`, `-C`/`--commit-each`,
  `-i`/`--subvolid`, `-R`/`--recursive`, `-v`/`--verbose` flags
- `btrfs subvolume snapshot`: `-r`/`--readonly`, `-i`/`--qgroup` flags
- `btrfs device add`: `-f`/`--force`, `-K`/`--nodiscard`, `--enqueue` flags
- `btrfs device usage`: full per-device allocation breakdown via chunk tree walk
- Help text snapshot tests covering all subcommands

### Fixed
- `btrfs property set ro` on a subvolume

---

## [0.2.0] — 2026-03-26

### Added
- `btrfs send`: pipe + reader thread architecture; full and incremental sends;
  `-e`, `-p`, `-c`, `-f`, `--no-data`, `--proto`, `--compressed-data` flags;
  protocol version negotiation via sysfs
- `btrfs receive`: full v1 stream processing; all 22 command types; `--dump` mode;
  `--chroot`; `-E`/`--max-errors`
- `btrfs balance`: filter string parsing for all filter types (`profiles`, `usage`,
  `devid`, `drange`, `vrange`, `convert`, `soft`, `limit`, `stripes`); range syntax
  (`min..max`); `|`-separated profile names
- `btrfs device scan --all-devices` / `-d` flag
- Man page generation via `btrfs-mangen` binary
- CLI argument parsing tests

---

## [0.1.0] — 2026-03-25

Initial release.

### Added
- `btrfs filesystem df` — space usage by chunk type
- `btrfs filesystem defrag` — single file and recursive directory defragmentation
- `btrfs filesystem resize` — online resize
- `btrfs filesystem mkswapfile` — swapfile creation
- `btrfs filesystem show/sync/label/usage/du/commit-stats`
- `btrfs scrub start/cancel/resume/status/limit`
- `btrfs balance start/pause/cancel/resume/status`
- `btrfs device add/remove/stats/scan/ready`
- `btrfs subvolume list/show/create/delete/snapshot/get-default/set-default/get-flags/set-flags/find-new/sync`
- `btrfs inspect-internal rootid/inode-resolve/logical-resolve/subvolid-resolve/min-dev-size/list-chunks/dump-super`
- `btrfs quota enable/disable/rescan/status`
- `btrfs qgroup create/destroy/assign/remove/limit/show/clear-stale`
- `btrfs property get/set/list`
- `btrfs replace start/status/cancel`
- `btrfs send` (initial)
- `btrfs receive` (initial)
- Argument parsing stubs for `btrfs check`, `btrfs restore`, all `btrfs rescue` subcommands
- `btrfs-uapi` safe ioctl wrappers
- `btrfs-disk` on-disk format parser (superblock, tree nodes, chunk tree)
