# Changelog

All notable changes to this project will be documented in this file.

## Unreleased

### Added

- `btrfs rescue clear-uuid-tree`: initial implementation that walks the UUID
  tree, drops extent refs for every block, deletes the ROOT_ITEM, and commits.
  Currently disabled in tests because the transaction commit hangs when
  processing the bulk drop_refs — needs transaction crate hardening before
  it can be relied upon.

## 0.9.0

### Added

- `mkfs.btrfs`: all RAID profiles (RAID0, RAID1C3, RAID1C4, RAID10,
  RAID5, RAID6) for both metadata and data block groups, with
  profile-specific stripe mapping in `logical_to_physical()`
- `mkfs.btrfs -O quota`: create quota tree with status, info, and limit
  items for the filesystem tree (INCONSISTENT flag, requires rescan)
- `mkfs.btrfs -O squota`: create simple quota tree with SIMPLE_MODE
  flag, `enable_gen` field, and pre-populated qgroup usage info
- `btrfs device stats --offline`: read device error statistics directly
  from the on-disk device tree without requiring a mounted filesystem
- `btrfs subvolume show`: display quota group usage and limits
  (referenced/exclusive bytes) with unit flag support
  (`--raw`, `--iec`, `--si`, `--kbytes`, `--mbytes`, `--gbytes`, `--tbytes`)
- `--format json` support for `device stats`, `filesystem df`,
  `filesystem du`, `qgroup show`, `subvolume show`, `subvolume list`,
  and `subvolume get-default`, using the btrfs-progs wrapper format
  with `__header`
- `--dry-run` support for `subvolume delete`: print what would be
  deleted without actually removing subvolumes. Using `--dry-run`
  with commands that do not support it now returns an error instead
  of silently doing nothing
- `btrfs device stats -T`: tabular output format with columns for
  device ID, path, and error counters (uses `cols` crate)
- Multicall binary support: with the `multicall` cargo feature, the
  `btrfs` binary dispatches by program name (`mkfs.btrfs`,
  `btrfs-mkfs`, `btrfstune`, `btrfs-tune`)
- Sysfs module unit tests covering all accessors (numeric, string,
  boolean, commit stats, features, quota status, scrub speed,
  qgroup entry parsing)
- `--format modern` output mode (`BTRFS_OUTPUT_FORMAT=modern` env):
  opt-in improved formatting with adaptive column widths via `cols`
- `btrfs subvolume list --format modern`: tree-view output with
  unicode connectors showing the subvolume parent-child hierarchy
- `btrfs inspect list-chunks --format modern`: cols-based adaptive
  table with right-aligned numeric columns
- `btrfs inspect list-chunks --offline`: read chunks directly from
  an unmounted device or image file by walking the on-disk chunk and
  block group trees. Does not require CAP_SYS_ADMIN
- `btrfs inspect min-dev-size --offline`: compute minimum device
  size from an unmounted device or image file by walking the device
  tree directly. Does not require CAP_SYS_ADMIN
- `btrfs filesystem du --format modern`: tree-view output with
  unicode connectors showing directory hierarchy via `cols`
- `btrfs filesystem du --format json`: structured JSON output with
  per-entry total/exclusive/set_shared byte counts
- `btrfs filesystem du --depth N`: limit display depth while still
  computing full totals (0 is equivalent to --summarize)
- `btrfs filesystem du --sort`: sort entries within each directory
  by path, total, exclusive, or shared (modern output only)
- `btrfs filesystem df --format modern`: cols-based adaptive table
- `btrfs quota status --format modern`: key-value table with UUID
  and rescan progress (extensions not in the text output)
- `btrfs qgroup show --format modern`: tree-view output showing the
  qgroup hierarchy with all columns (rfer, excl, max_rfer, max_excl)
  always visible
- `btrfs scrub start --format modern`: live progress on stderr with
  terminal-aware updates (200ms terminal, 1s non-terminal), summary
  tree table with per-device data/metadata/error stats, and raw mode
  tree with all kernel counters per device
- `btrfs scrub status --format modern`: cols table with per-device
  scrubbed/allocated bytes and error summary
- `btrfs filesystem show --format modern`: clean header with device
  cols table (DEVID, SIZE, USED, PATH)
- `btrfs device usage --format modern`: tree-view with each device as
  a root node and per-profile allocations as children
- `btrfs filesystem usage --format modern`: three-section layout with
  key-value overall stats, profile summary table, and dynamic
  per-device allocation table with runtime-generated profile columns
- `RunContext` struct for passing runtime options through commands
- `Runnable::supported_formats()`: commands declare which formats
  they support; unsupported formats produce a clear error

### Changed

- Running `btrfs` or any command group (`btrfs filesystem`, `btrfs device`,
  etc.) without a subcommand now shows help instead of an error
- `BTRFS_OUTPUT_FORMAT` env var is now resolved manually instead of via
  clap's `env` attribute, so it no longer interferes with
  `arg_required_else_help`
- `SearchKey` renamed to `SearchFilter` with compound `Key` struct:
  the `(objectid, item_type, offset)` triple is now a single `Key`
  type, and `SearchFilter` uses `start`/`end` keys instead of six
  flat min/max fields, making the compound key semantics explicit

### Fixed

- `btrfs subvolume list`: snapshots and subvolumes with non-zero
  ROOT_ITEM key offsets now show the correct parent ID and name.
  The tree search callback was not filtering on `hdr.item_type`,
  causing ROOT_BACKREF and ROOT_REF items to be misinterpreted as
  ROOT_ITEM data

## 0.8.0

### Added

- `btrfs check`: read-only filesystem verification with all 7 phases:
  superblock validation, tree structure checks, extent reference verification,
  chunk/block group cross-checks, FS tree inode consistency, checksum tree
  validation (with optional `--check-data-csum`), and ROOT_REF/ROOT_BACKREF
  consistency checking
- `btrfs check`: directory inode size validation, file nbytes validation,
  missing extent item detection, and bidirectional backref owner cross-checks
- `mkfs --rootdir`: populate a new filesystem from an existing directory
  tree with support for regular files (inline + regular extents up to 1 MiB),
  directories, symlinks, hardlinks, xattrs, and special files
- `mkfs --rootdir --compress`: zlib, zstd, and LZO compression for
  rootdir population (LZO uses per-sector framed format for regular
  extents and single-segment format for inline extents)
- `mkfs --rootdir --subvol`: create subdirectories as separate btrfs
  subvolumes with independent FS trees, ROOT_REF/ROOT_BACKREF linkage,
  read-only support (`ro:`, `default-ro:`), default subvolume
  designation, and nested subvolume support
- `mkfs --rootdir --reflink`: clone file extents via FICLONERANGE
  instead of copying bytes (requires source and image on same filesystem)
- `mkfs --rootdir --inode-flags`: set NODATACOW/NODATASUM flags on
  specific paths during rootdir population
- `mkfs --rootdir --shrink`: truncate the image to the actual used size
  after populating from rootdir
- `btrfs mkfs` and `btrfs tune` optional CLI subcommands: enable with
  cargo features `mkfs` and `tune` for a single-binary experience
- `btrfs-disk`: `tree_walk_mut` for mutable DFS tree traversal with
  automatic checksum recomputation (used by tune fsid rewrite)
- `btrfs-uapi`: `filesystem::is_mounted` as the canonical mount check,
  now returns `Result<bool>` for proper error propagation
- Comprehensive btrfs internals specification documents in `docs/spec/`:
  on-disk format, chunk/block group system, extent tree and backrefs,
  check phases, and mkfs process
- `#![warn(clippy::pedantic)]` enabled across all crates
- Comprehensive rustdoc for all public types in `btrfs-disk` and
  `btrfs-uapi`, including detailed btrfs on-disk format explanations
- End-to-end integration tests for mkfs --rootdir (basic, compressed,
  shrink) with mount + data verification
- uapi-based effect verification added to 8 existing integration tests
  (label, resize, quota, subvolume, property, device, qgroup)

### Changed

- `mkfs` argument help organized into headings: Block layout, Features,
  Identity, and Rootdir population
- `mkfs` Profile, ChecksumArg, and Feature enums migrated from manual
  `FromStr` to clap `ValueEnum` with backward-compatible aliases
- `mkfs` `--verbose` changed from bool to u8 (count-based) to match
  the CLI's global `--verbose` type
- `btrfs-uapi`: `device_remove` and `replace_start` take references
  instead of owned values
- `btrfs-tune`: refactored to use `tree_walk` and `tree_walk_mut`
  from `btrfs-disk` instead of manual recursive tree traversal
- Unified duplicated tree builder functions in mkfs (block-group,
  free-space, superblock) via `UsedBytes` and `SuperblockParams` structs
- Inode field patching in rootdir uses `offset_of!` instead of
  hardcoded byte offsets
- Workspace dependencies consolidated: flate2, zstd, lzokay moved
  to workspace level
- Stream CRC validation uses incremental `crc32c_append` instead of
  allocating a contiguous buffer per command

### Fixed

- `btrfs-disk`: CRC32C checksum computation for superblocks and tree blocks
  now uses standard CRC32C (matching the kernel's `hash_crc32c`) instead of
  raw CRC32C with seed=0
- `btrfs check`: item data extraction now correctly accounts for the tree
  block header offset
- `mkfs --rootdir`: removed spurious CHUNK_TREE ROOT_ITEM that confused
  the C btrfs-progs check's backref validation
- `mkfs --rootdir`: include data extent bytes in superblock `bytes_used`
- `mkfs --rootdir --shrink`: update `total_bytes` in chunk tree DEV_ITEM
  and all superblock mirrors, not just the primary superblock

## 0.7.0

### Added
- `btrfs restore`: file recovery from damaged/unmounted filesystems with support
  for regular files (inline/regular/prealloc extents), directories, symlinks (`-S`),
  extended attributes (`-x`), metadata restoration (`-m`), compressed extent
  decompression (zlib/zstd/lzo), path regex filtering, snapshot restoration (`-s`),
  tree root listing (`-l`), and superblock mirror fallback
- `btrfs-tune` crate: offline superblock tuning tool with feature flag
  enabling (`-r`, `-x`, `-n`), seeding flag management (`-S`),
  metadata UUID change (`-m`, `-M UUID`), and full fsid rewrite
  (`-u`, `-U UUID`)
- Man page generation for `btrfs-tune`
- `btrfs-disk`: comprehensive rustdoc for all public types, fields, and the
  crate-level overview (filesystem layout, tree descriptions, usage guide)
- `btrfs-tune`: crate-level and module-level rustdoc
- `btrfs-disk`: `BlockReader::write_block` for writing tree blocks by
  logical address, `csum_tree_block` for recomputing tree block checksums

## 0.6.0

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

## 0.5.0

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

## 0.4.0

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

## 0.3.0

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

## 0.2.0

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

## 0.1.0

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
