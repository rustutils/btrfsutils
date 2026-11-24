# Changelog

All notable changes to this project will be documented in this file.

## 0.13.0

btrfsutils 0.13 completes the read side. A new `btrfs-fs` crate takes
the read-only filesystem API out of `btrfs-fuse` and adds
multi-subvolume traversal, an async API, and three-layer caching. It
drives both the FUSE driver and a new `btrfs send --offline IMAGE`,
which streams snapshots from unmounted images without privileges.
`btrfs-fuse` gains `lseek SEEK_HOLE` and `SEEK_DATA`, `readdirplus`,
eager `forget`, and eight btrfs ioctls. A `tree_search_auto` fallback
from v2 to v1 works around the kernel's `FUSE_IOCTL_RETRY`
restriction. The driver ships as a standalone `btrfs-fuse` binary and
as an opt-in `btrfs fuse` subcommand. `btrfs reflink clone` is new
(the btrfs-progs version is a stub that returns `EOPNOTSUPP`).
Releases now come from a real pipeline: static musl `.deb`, `.rpm`,
`.tar.zst`, and bare zstd artifacts for `x86_64`, `aarch64`, and
`riscv64gc`, published to GitLab and mirrored to GitHub on every
`vX.Y.Z` tag.

### Added

- `btrfs-cli`: `btrfs reflink clone [-r SRCOFF:LENGTH:DESTOFF]... SRC DST`
  makes lightweight file copies with the `FICLONERANGE` ioctl. The
  destination shares the source's extents, no data is copied, and later
  changes are copy-on-write. Without `-r` the whole source is cloned
  into the target, which is created or truncated. With one or more `-r`
  flags only those ranges are cloned and the rest of the target is
  kept. SRCOFF, LENGTH, and DESTOFF accept the k, m, g, t, p, and e
  size suffixes. The btrfs-progs version is a stub that always returns
  `EOPNOTSUPP`.
- `btrfs-uapi`: `reflink::clone_range(src, src_off, length, dst, dst_off)`
  wraps `BTRFS_IOC_CLONE_RANGE`, which has the same encoding as the VFS
  `FICLONERANGE`. A `length` of zero clones from `src_off` to the end
  of the source, per the kernel ABI.
- `btrfs-cli`: `btrfs send --offline IMAGE [-f OUT]` generates a v1
  send stream from an unmounted image without the `BTRFS_IOC_SEND`
  ioctl. It needs no `CAP_SYS_ADMIN` and no kernel mount, and the FUSE
  retry restriction doesn't apply. Select the subvolume with
  `--offline-subvol PATH` or `--offline-subvolid ID` (default: the
  default subvolume). Only full sends are supported. Clap rejects
  incremental sends, clone sources, `--no-data`, other protocol
  versions, and compressed-data passthrough in offline mode.
- `btrfs-fs`: `Filesystem::resolve_subvol_path(path) -> Option<SubvolId>`
  resolves a slash-separated path, relative to the filesystem root, to
  a subvolume tree id. An empty path or `"/"` resolves to the default
  `FS_TREE`. Used by `btrfs send --offline` and the `btrfs-fuse`
  `--subvol` flag.
- `btrfs-fs`: `Filesystem::send(snapshot, output) -> Result<output>`
  writes a v1 send stream for a snapshot to any `Write`, such as a pipe
  to `btrfs receive` or a backup file. Only full sends are supported:
  no parent comparison, clone sources, or encoded-write passthrough.
  Each inode emits `Mkfile`, `Mkdir`, `Symlink`, `Mknod`, `Mkfifo`, or
  `Mksock`, then `SetXattr` per xattr, `Write` chunks (at most 48 KiB
  to fit v1's u16 length field), `Truncate`, `Chown`, `Chmod`, and
  `Utimes`. Extra hardlinks emit `Link` instead of recreating the
  inode. Nested subvolumes are skipped, so call `send` once per
  subvolume. File data is sent decompressed. Compressed passthrough
  will come with v2 `EncodedWrite` support.
- `btrfs-stream`: `StreamWriter<W>` encodes `StreamCommand` values into
  the TLV wire format that kernel `btrfs send` produces. It mirrors
  `StreamReader`: every v1, v2, and v3 command round-trips through
  write and parse. It handles the `BTRFS_SEND_A_DATA` quirk in v2 and
  later (no length field, data runs to the end of the payload), so
  `Write` and `EncodedWrite` commands can carry more than v1's 64 KiB.
  CRC32C uses the same raw (init=0) variant the parser checks.
  Wire-format constants moved to a new `consts` module shared by the
  encoder and parser.
- `btrfs-cli`: `btrfs fuse <IMAGE> <MOUNTPOINT> [OPTIONS]` subcommand
  behind a new opt-in `fuse` cargo feature. It matches the standalone
  `btrfs-fuse` binary exactly and shares its argument parsing and
  mount logic through the new public `btrfs_fuse::args::MountArgs` and
  `btrfs_fuse::run::run_mount`. The standalone binary remains for
  `mount.fuse.btrfs` and fstab integration. The feature is opt-in while
  the FUSE driver is experimental. Build with
  `cargo build --features fuse` (or `--all-features`) to include it.
- `btrfs-fs`: new crate with a high-level read-only filesystem API on
  top of `btrfs-disk`. `Filesystem<R>` provides `lookup`, `readdir`,
  `readdirplus`, `read`, `readlink`, `getattr`, `xattr_get`,
  `xattr_list`, `statfs`, `seek_hole_data` (POSIX `SEEK_HOLE` and
  `SEEK_DATA` over the cached extent map), `forget`, `tree_search`,
  `dev_info`, `ino_lookup`, `ino_paths`, `list_subvolumes`,
  `get_subvol_info`, `superblock`, `send`, and `resolve_subvol_path`.
  It doesn't depend on FUSE and drives the `btrfs-fuse` mount as well
  as other embedders. Inodes are `(SubvolId, ino)` pairs, so traversal
  works across subvolumes. The handle is `Clone` (an `Arc` bump) and
  every operation is an `async fn`, so several tokio tasks can share
  one filesystem. Blocking I/O runs in `tokio::task::spawn_blocking`.
  The reader bound is `R: Read + Seek + Send + 'static`, which `File`
  and `Cursor<Vec<u8>>` already satisfy.
- `btrfs-fs`: multi-subvolume traversal. `Filesystem::lookup` detects
  subvolume crossings (a `DirItem` whose `location.key_type` is
  `ROOT_ITEM`) and returns an `Inode` with the new subvolume id and
  `objectid 256`. Reads, `readdir`, `readlink`, and xattr operations
  follow into the new subvolume's tree. `..` at the root of a
  non-default subvolume resolves through `ROOT_BACKREF` to the
  containing directory in the parent subvolume.
  `Filesystem::open_subvol(reader, SubvolId)` opens with a non-default
  subvolume as root, and `Filesystem::default_subvol() -> SubvolId`
  reports which subvolume `root()` points at. `SubvolInfo` carries
  `dirid`, `otime`, `ctransid`, `otransid`, `uuid`, `parent_uuid`, and
  `received_uuid`, which is everything `BTRFS_IOC_GET_SUBVOL_INFO`
  needs.
- `btrfs-fs`: `CacheConfig` and the `Filesystem::open_with_caches` and
  `Filesystem::open_subvol_with_caches` constructors override the
  default cache sizes (4096 tree blocks, 4096 inodes, 1024 extent
  maps). `CacheConfig::no_cache()` uses single-entry caches, for
  benchmarking the cold path or for memory-constrained embedders.
  `Filesystem::open` and `Filesystem::open_subvol` keep the defaults.
- `btrfs-disk`: pluggable tree block cache through the `TreeBlockCache`
  trait. `BlockReader::read_tree_block` returns `Arc<TreeBlock>`,
  checks the attached cache before reading from disk, and fills it on
  a miss. The trait is `Send + Sync` with `&self` methods, so a cache
  can be shared across threads. `btrfs-disk` ships only the trait, with
  no LRU implementation and no extra dependencies. `btrfs-fs` provides
  `LruTreeBlockCache` as the default and exposes
  `Filesystem::tree_block_cache_stats() -> CacheStats` (lock-free hit,
  miss, and insertion counters) for tests and benchmarks.
- `btrfs-fuse`: btrfs ioctls over the FUSE mount:
  `BTRFS_IOC_FS_INFO`, `BTRFS_IOC_GET_FEATURES`,
  `BTRFS_IOC_GET_SUBVOL_INFO`, `BTRFS_IOC_DEV_INFO`,
  `BTRFS_IOC_INO_LOOKUP`, `BTRFS_IOC_TREE_SEARCH` (v1, fixed 4 KiB
  struct), `BTRFS_IOC_GET_SUBVOL_ROOTREF`, and
  `BTRFS_IOC_TREE_SEARCH_V2`. The last one returns `ENOPROTOOPT` so
  `btrfs-uapi::tree_search_auto` falls back to v1. Each FUSE_IOCTL
  request runs in its own tokio task, which owns the `ReplyIoctl` and
  serialises the response into the kernel's C struct layout. No
  bindgen types leak into the public API. Unknown ioctls return
  `ENOTTY`.
- `btrfs-fuse`: new callbacks. `lseek` supports POSIX `SEEK_HOLE` and
  `SEEK_DATA`. `readdirplus` lets the kernel combine
  `readdir + lookup-per-entry`, a large speedup for `ls -l`. `init`
  advertises `FUSE_DO_READDIRPLUS`, `FUSE_AUTO_INVAL_DATA`,
  `FUSE_SPLICE_READ`, and `FUSE_SPLICE_WRITE`. `forget` evicts inode
  and extent map cache entries eagerly. `BtrfsFuse::open_with_caches`
  and `BtrfsFuse::open_subvol_with_caches` mirror the `btrfs-fs`
  constructors.
- `btrfs-fuse` CLI: `--subvol PATH` or `--subvolid ID` mounts a
  specific subvolume as the FUSE root. The two are mutually exclusive,
  and the default is `FS_TREE`. New `--cache-tree-blocks N` (default
  4096), `--cache-inodes N` (default 4096), and
  `--cache-extent-maps N` (default 1024) flags. The mount now enables
  the kernel `default_permissions` option by default, so stored
  ownership is enforced the way kernel btrfs does.
  `--no-default-permissions` turns this off for inspecting images
  whose UIDs don't match the local system.
- `btrfs-fuse`: 10 unprivileged integration tests in
  `fuse/tests/mount.rs` spawn the `btrfs-fuse` binary, mount a fixture
  image, and exercise it through ordinary POSIX calls (`std::fs`,
  `xattr` crate). They cover mount and unmount, root listing, small,
  large, and nested file reads, `stat` (including the 1↔256 inode
  swap), readlink, xattr get and list, and a 16-thread, 50-iteration
  concurrent read stress test that catches double replies, dropped
  replies, and deadlocks in task dispatch. A `MountedFuse` RAII guard
  unmounts lazily so a panicking test doesn't wedge the mountpoint.

### Changed

- Packaging: `just package` cross-compiles static release builds for
  the `x86_64`, `aarch64`, and `riscv64gc` Linux musl targets with
  `cargo-zigbuild`. For each architecture it produces a `.deb`
  (`cargo-deb`), an `.rpm` (`cargo-generate-rpm`), a relocatable
  `.tar.zst`, and a bare `.zst`-compressed multicall binary. The
  binary is built with `--features mkfs,tune,fuse,multicall`, so one
  `btrfs` binary covers every subcommand and dispatches on argv[0] for
  `mkfs.btrfs`, `btrfstune`, `btrfs-mkfs`, and `btrfs-tune` symlinks.
  This replaces the nfpm-on-nix recipe. The nix flake remains for the
  dev shell and `nix flake check`. Package metadata lives in
  `[package.metadata.deb]` and `[package.metadata.generate-rpm]` in
  `cli/Cargo.toml`, and `nfpm.yaml` is removed.
- CI: a new GitLab pipeline lints, tests (the workspace minus
  `btrfs-fuse`, which hangs on shared runners), and packages with
  `just package`. On a `vX.Y.Z` tag it publishes a GitLab release and a
  mirrored GitHub release with every artifact under `target/dist/`
  attached.
- `btrfs-uapi`: new `tree_search_auto` tries `BTRFS_IOC_TREE_SEARCH_V2`
  first and falls back to `BTRFS_IOC_TREE_SEARCH` (v1) when the driver
  doesn't support v2. It falls back on `ENOPROTOOPT` (sent by
  `btrfs-fuse`), `ENOTSUP`, `EOPNOTSUPP`, `ENOTTY`, and `ENOSYS` (old
  kernels without v2). It only falls back if v2 failed before calling
  the user callback, so a transient error mid-walk can't duplicate
  items.
- `btrfs-disk`: `BlockReader::read_tree_block` returns `Arc<TreeBlock>`
  instead of an owned `TreeBlock`. Code that used `match &block { ... }`
  needs `match &*block`, and code that consumed the block (e.g.
  `into_iter()`) must borrow instead (`iter()`). Internal `walk_stats`
  takes `&TreeBlock`. All callers in `cli/`, `tune/`, `fs/`, `mkfs/`,
  `transaction/`, and the integration tests are updated.
- `btrfs-fuse` is now a thin `fuser::Filesystem` adapter on top of
  `btrfs-fs`. The 19 read-path integration tests moved with the logic
  to `fs/tests/basic.rs`. The fuse library API
  (`BtrfsFuse::lookup_entry` and related methods) is removed. New
  embedders should depend on `btrfs-fs` directly. The `read.rs`,
  `xattr.rs`, `dir.rs`, and `stat.rs` modules are gone from `fuse/`,
  along with its dependencies on `flate2`, `zstd`, `lzokay`,
  `btrfs-disk`, and `libc`. The outer `Mutex<Filesystem<File>>` in
  `BtrfsFuse` is gone because `Filesystem` methods take `&self`.
  `btrfs-fuse` runs an internal multi-thread tokio runtime. Each FUSE
  callback spawns a task that owns the `Reply*` handle, awaits the
  filesystem operation, and replies. The FUSE worker thread returns
  immediately, so concurrent callbacks don't serialise on a single
  I/O.

### Fixed

- `btrfs-fuse`: the FUSE root inode (`1`) now maps to the selected
  mount subvolume instead of always `SubvolId(5)`. Previously
  `BtrfsFuse::open_subvol` opened the right subvolume, but every FUSE
  callback ignored it and served `FS_TREE`. Found by the new
  `--subvol` mount tests.
- `btrfs-fs`: zstd-compressed extents in multi-chunk files now
  decompress correctly. Btrfs splits each compressed extent into
  independent 128 KiB frames, and `zstd::bulk::decompress` rejects
  bytes after the first frame. The streaming decoder
  (`zstd::stream::read::Decoder`) handles concatenated frames and
  trailing sector padding.
- `btrfs-fs`: compressed inline extents now read correctly. The read
  range used `inline_size`, the compressed on-disk length, as the
  logical length, so reads came back too short. Reads now clamp
  against `ram_bytes`, the uncompressed length.

### Known limitations

- Variable-size btrfs ioctls that need `FUSE_IOCTL_RETRY`
  (`BTRFS_IOC_INO_PATHS`, `BTRFS_IOC_LOGICAL_INO_V2`,
  `BTRFS_IOC_SPACE_INFO`, `BTRFS_IOC_ENCODED_READ`) can't complete
  over the FUSE mount. Linux's `fuse_do_ioctl` only honours a retry
  reply when the request set `FUSE_IOCTL_UNRESTRICTED`, which plain
  `ioctl(2)` callers never do. `BTRFS_IOC_TREE_SEARCH_V2` avoids this
  by returning `ENOPROTOOPT`, and `btrfs-uapi::tree_search_auto` then
  falls back to v1. The other variable-size ioctls aren't dispatched,
  since they would always fail.

## 0.12.0

### Removed

- `btrfs-mkfs`: removed the legacy `--rootdir` walker and its helpers.
  Every `--rootdir` feature (the simple case, subvolumes, reflink,
  shrink, and inode flags) now runs through the transactional path, so
  the old code was dead. `make_btrfs_with_rootdir` now only delegates
  to `make_btrfs_with_rootdir_via_transaction`. Removed from `mkfs.rs`:
  `converge_extent_tree_block_count`, `write_rootdir_trees`,
  `metadata_extent_item`, `add_block_group_items`,
  `RootTreeRootdirArgs`, `patch_root_item_fs`,
  `build_root_tree_rootdir`, `build_free_space_tree_with_used`,
  `build_block_group_tree_with_used`, and the `UsedBytes` struct.
  Removed from `rootdir.rs`: `walk_directory`, `walk_single_tree`,
  `write_file_data`, `RootdirPlan`, `SubvolPlan`, `SubvolMeta`,
  `FileAllocation`, `DataOutput`, `apply_nbytes_updates`,
  `fixup_inode_nlink`, `fixup_inode_size`, `fixup_inline_nbytes`,
  `patch_inode_field`, `try_compress_inline`, `try_compress_regular`,
  `lzo_compress_inline`, `lzo_compress_extent`, `max_inline_data_size`,
  `CompressConfig::extent_type_byte`, `is_enabled`, and the LZO
  round-trip tests. In total 2228 lines removed and 31 added. The item
  serializers in `mkfs/src/items.rs` and the `tree.rs` and
  `treebuilder.rs` code stay because the bootstrap path without
  `--rootdir` still uses them.

### Added

- `btrfs-mkfs`: `--rootdir --inode-flags` now uses the transactional
  walker. `walk_to_transaction` builds the same
  `path → (nodatacow, nodatasum)` map as the legacy walker and sets
  `InodeFlags::NODATACOW` and `InodeFlags::NODATASUM` on the
  `InodeArgs` passed to `Transaction::create_inode`. The `nodatasum`
  value is also passed to `write_file_data` and the reflink path, so
  `NODATASUM` files get no per-sector csums. `make_btrfs_with_rootdir`
  now always takes the transactional path. Covered by the existing
  `rootdir_inode_flags_nodatacow_nodatasum` test.
- `btrfs-mkfs`: `--rootdir --reflink` and `--rootdir --shrink` now use
  the transactional walker. After this, only `--inode-flags` remained
  on the legacy path.

  `--shrink` (single device only) computes the smallest size that
  covers the chunk layout with `ChunkLayout::new`, then updates
  `DEV_ITEM.total_bytes` and the superblock through the new
  `Transaction::set_device_total_bytes` before commit. The image is
  truncated after `fs.sync()`. Covered by the existing
  `mkfs_rootdir_shrink` integration test.

  `--reflink` adds `Transaction::reserve_data_extent`, which allocates
  a data extent address and queues the `+1 EXTENT_DATA_REF` without
  writing data. `alloc_data_extent` now wraps it. The walker opens
  writable device handles up front. For each file chunk it reserves an
  extent, looks up the per-stripe `(devid, physical)` placements, and
  issues `FICLONERANGE` from the source file into each device. It reads
  the cloned bytes back with `BlockReader::read_data` to compute CRC32C
  csums, inserts the `EXTENT_DATA` and `EXTENT_CSUM` items, and updates
  `INODE.nbytes`. RAID5 and RAID6 chunks are rejected because
  FICLONERANGE doesn't recompute parity. Compression is silently
  disabled with reflink, since the recorded compression byte wouldn't
  match the cloned source data. A new privileged test,
  `mkfs_rootdir_reflink_on_btrfs`, runs `mkfs --rootdir --reflink`
  with source and destination inside a mounted btrfs and checks the
  file contents after mounting the result.
- `btrfs-mkfs`: `--rootdir --subvol` now uses the transactional walker.
  All four subvolume types (`rw`, `ro`, `default`, `default-ro`) and
  arbitrary nesting work. `walk_to_transaction` maps paths to
  `(subvol_id, type)` from `subvol_args`, walks the main FS tree first,
  and queues each subvolume boundary as a `DeferredSubvol`. For each
  queued subvolume, `post_bootstrap::create_subvolume_shape` creates
  the tree (a leaf, inode 256 with `INODE_ROOT_ITEM_INIT`, a ".."
  `INODE_REF`, and a `ROOT_ITEM` with a fresh v4 UUID).
  `Transaction::insert_root_ref` records the parent link, the walker
  fills in the contents, and `set_root_readonly` and
  `set_default_subvol` apply the ro and default flags. Nested
  subvolumes join the queue as the walker finds them. After this, the
  legacy path ran only for `--reflink`, `--shrink`, or
  `--inode-flags`. The new `Transaction::link_subvol_entry` (sibling of
  `link_dir_entry`) adds the parent's `DIR_ITEM` and `DIR_INDEX`
  pointing at a `ROOT_ITEM` and updates the parent directory size. It
  adds no `INODE_REF`, because subvolumes link to their parent through
  `ROOT_REF` and `ROOT_BACKREF` in the root tree. Both `link_*` helpers
  share `mirror_root_item_size`, which copies the inode 256 size into
  the `ROOT_ITEM`. Covered by the existing unprivileged `subvol_*`
  tests and a new privileged `mkfs_rootdir_subvols` test that mounts
  the result and checks `btrfs subvol list`, write rejection on ro
  subvolumes, and nested subvolume visibility.

  Also fixes an existing bug: `direct_subvol_boundaries` received an
  absolute `parent_subvol_path`, but `subvol_id_map` keys are relative
  to `rootdir`, so nested subvolumes were silently not detected. The
  new walker strips the rootdir prefix first. The legacy walker has
  the same bug but no test covers it, and it was left alone because it
  was due for deletion.
- `btrfs-mkfs`: `--rootdir` without `--subvol`, `--reflink`,
  `--shrink`, or `--inode-flags` now goes through the transaction
  crate. `make_btrfs_with_rootdir` calls `make_btrfs` to create the
  empty filesystem (which already runs `post_bootstrap` to create the
  FS, csum, data-reloc, and UUID trees), opens the image, and starts a
  transaction. A new `rootdir::walk_to_transaction` walks the source
  directory depth-first and emits `INODE_ITEM`, `DIR_ITEM`,
  `DIR_INDEX`, `INODE_REF`, `XATTR_ITEM`, and inline or regular
  `EXTENT_DATA` items through `Transaction::create_inode`,
  `link_dir_entry`, `set_xattr`, `insert_inline_extent`, and
  `write_file_data`. Hardlinks across directories are merged and get
  their `nlink` from `set_inode_nlink` after the walk. The legacy
  `walk_directory`, `TreeBuilder`, and `pwrite` pipeline still ran when
  any of the four legacy-only features was requested. Single-device
  and multi-device (`Filesystem::open_multi`) paths use the same
  walker. Covered by the four existing `mkfs_rootdir_*` integration
  tests and a new `mkfs_rootdir_hardlinks_and_xattrs` test that mounts
  the result and checks shared inode numbers, `nlink`, and xattrs.
- `btrfs-transaction`: subvolume creation helpers, groundwork for
  moving `--rootdir` onto the transaction crate. Five new helpers, all
  built on `search_slot`, `insert_item`, and `update_item`.
  `Transaction::set_inode_nlink(tree, ino, nlink)` patches an inode's
  `nlink` in place (like `update_inode_nbytes`).
  `Transaction::insert_root_ref(parent_root, child_root, dirid,
  dir_index, name)` inserts a `ROOT_REF` and `ROOT_BACKREF` pair into
  the root tree (id 1) using the new `RootRef::serialize` from
  `btrfs-disk`. `Transaction::set_root_readonly(tree_id)` sets
  `RootItemFlags::RDONLY` on an existing `ROOT_ITEM`.
  `Transaction::set_default_subvol(subvol_id)` inserts or updates the
  `"default"` `DIR_ITEM` under `BTRFS_ROOT_TREE_DIR_OBJECTID`. It
  overwrites mkfs's default in place, because the payload size doesn't
  depend on `subvol_id`. Each helper has a privileged integration test
  on a fresh mkfs image, and the readonly and default tests also pass
  `btrfs check`.
- `btrfs-disk`: `RootRef::serialize(dirid, sequence, name)` is the
  inverse of `parse`. It produces the on-disk `btrfs_root_ref` bytes
  (an 18-byte header followed by the name).
- `btrfs-disk`: `chunk::ChunkTreeCache::plan_write` routes RAID5 and
  RAID6 chunks through a new `WritePlan::Parity(ParityPlan)` variant.
  It names every data column slot of each touched row plus the rotating
  parity columns. The executor in `BlockReader` reads each data slot,
  overlays the new bytes, computes parity, and writes data and parity.
  `plan_read` sends RAID5 and RAID6 reads to the data column that holds
  each row's bytes and ignores parity. Other profiles are unchanged
  (`WritePlan::Plain`).
- `btrfs-disk`: new `raid56` module with `compute_p` (XOR) and
  `compute_p_q` (XOR plus Reed-Solomon over GF(2^8) with reduction
  polynomial `x^8 + x^4 + x^3 + x^2 + 1`). It is self-contained
  original code with exhaustive unit tests, including reconstruction
  round-trips and hand-checked GF table values.
- `btrfs-transaction`: integration tests
  `multi_device_raid5_metadata_cow_round_trip` and
  `multi_device_raid6_metadata_cow_round_trip` exercise the
  parity-aware write path end to end. Each creates a RAID5 or RAID6
  image with mkfs, inserts an item with COW, commits, reopens, and
  checks the item is reachable.
- `btrfs-transaction`: `convert::seed_free_space_tree(trans, fs_info)`
  walks every block group, derives free ranges from the extent tree,
  and inserts one `FREE_SPACE_INFO` and one `FREE_SPACE_EXTENT` per
  range into tree 10. Block groups that already have a
  `FREE_SPACE_INFO` are skipped, and existing bitmap-layout entries are
  rejected. Split out of `convert_to_free_space_tree` so mkfs's
  `post_bootstrap` can call it after creating the free space tree in a
  transaction. `read_free_space_info` is now `pub(crate)` for the skip
  check.
- `btrfs-transaction`: `convert::create_block_group_tree(trans, fs_info)`
  collects the `BLOCK_GROUP_ITEM` rows from the extent tree, pins block
  group routing to the extent tree, creates tree 11 if needed, moves
  each item into the block group tree (skipping ones already moved),
  and sets the `BLOCK_GROUP_TREE` compat_ro flag. Split out of
  `convert_to_block_group_tree` so mkfs's `post_bootstrap` can build
  the block group tree from a bootstrap that left the items in the
  extent tree. Each item moves idempotently, so an interrupted
  conversion can resume.
- `btrfs-disk`: `ChunkProfile` enum and `ChunkTreeCache::plan_write`
  and `plan_read` for stripe-aware per-device routing across all RAID
  profiles except RAID5 and RAID6. Writes larger than `stripe_len` are
  split into per-row segments. These replace `resolve_all` for routing
  in `BlockReader::write_block`, `read_block`, and `read_data`.
- `btrfs-mkfs`: post-bootstrap transaction. After writing the bootstrap
  layout, mkfs reopens the image with `btrfs-transaction` and runs one
  transaction that creates the empty UUID tree (objectid 9).
  btrfs-progs creates this tree by default, but our bootstrap omitted
  it. The kernel adds UUID tree entries lazily on snapshot and send, so
  an empty tree is the correct starting state.

  This is the first use of the transaction crate in mkfs. It only runs
  for SINGLE, DUP, RAID1, RAID1C3, and RAID1C4 metadata and data, with
  the free space tree enabled and CRC32C checksums. Other
  configurations skip it silently because the transaction crate
  doesn't handle them yet or has known compatibility gaps with mkfs's
  output. See `mkfs/PLAN.md`.
- `btrfs check`: `--backup`, `--tree-root`, and `--chunk-root` flags
  for recovering from damaged root or chunk tree pointers.
- `btrfs-transaction`: structural invariant assertions.
  `debug_assert!` checks validate tree block structure (key ordering,
  data layout, bytenr consistency) at every modification. Hard
  `assert!` checks catch catastrophic errors (superblock corruption,
  inconsistent generations) in release builds too. Adds the
  `ExtentBuffer::check_leaf()`, `check_node()`, and `check()` methods.
- `rescue chunk-recover`: scans the raw device for surviving chunk tree
  leaves, resolves conflicts, rebuilds the chunk map, and prints a
  detailed report. `--apply` writes the rebuilt chunk tree through the
  transaction crate.
- `btrfs-disk`: `BlockReader::new` and `filesystem_open_with_cache`
  open a filesystem with a prebuilt chunk cache (used by
  chunk-recover).
- `btrfs-transaction`: `Filesystem::open_with_chunk_cache` and
  `Transaction::rebuild_chunk_tree` for chunk tree recovery.
- `btrfs-transaction`: data extent ref creation. `create_data_extent`
  inserts `EXTENT_ITEM` entries with inline `EXTENT_DATA_REF` backrefs
  through the delayed ref pipeline, and updates data block group
  accounting and the free space tree. Adds the
  `ExtentItem::to_bytes_data()` serializer to btrfs-disk.
- `btrfs-transaction`: data extent write path.
  `BlockGroupKind::Data` and `Transaction::alloc_data_extent` find space
  in a DATA block group, write the bytes to all stripe copies right
  away, and queue a `+1` `EXTENT_DATA_REF` delayed ref.
  `Transaction::insert_file_extent` inserts an `EXTENT_DATA` item into
  an FS tree pointing at the extent. `Transaction::insert_csums`
  computes standard CRC32C per sector and inserts `EXTENT_CSUM` items
  into the csum tree, splitting large payloads so each item fits in a
  leaf. Verified with `btrfs check`.
- `btrfs-disk`: `FileExtentItem::to_bytes_regular` (53-byte regular or
  prealloc body) and `FileExtentItem::to_bytes_inline` (21-byte header
  plus raw payload) serializers, and the `HEADER_SIZE` and
  `REGULAR_SIZE` constants.
- `btrfs-transaction`: inode and directory entry helpers, so mkfs
  doesn't repeat the same boilerplate for every file. A new `inode`
  module provides `InodeArgs`, a full-field counterpart to
  `btrfs_disk::items::InodeItemArgs` with `flags`, `rdev`, `sequence`,
  and four separate timestamps. `Transaction::create_inode(tree, ino,
  args)` inserts an `INODE_ITEM`. `Transaction::link_dir_entry(tree,
  parent, child, name, ft, dir_index, time)` inserts `INODE_REF`,
  `DIR_ITEM`, and `DIR_INDEX`, and updates the parent's `size`,
  `transid`, `ctime`, and `mtime` in place. When the parent is the
  subvolume root directory, it also copies the size into the
  `ROOT_ITEM`'s embedded inode. `Transaction::set_xattr(tree, ino,
  name, value)` inserts an `XATTR_ITEM`. Six end-to-end tests that
  built directory entries by hand shrank from about 200 lines of setup
  each to about 15.
- `btrfs-disk` and `btrfs-transaction`: multi-device write support.
  `BlockReader<R>` stores a `BTreeMap<u64, R>` keyed by device id.
  `read_block` and `read_data` pick the device through the chunk
  cache, and `write_block` writes each stripe to its own device. New
  `filesystem_open_multi(devices)` and
  `Filesystem::open_multi(devices)` take a `devid -> handle` map. The
  single-handle functions are thin wrappers that read the superblock
  to find the primary devid. Each device's `dev_item` is captured at
  open time and written into that device's superblock at commit, so
  other devices don't get the primary's identity. Bootstrap checks that
  every devid the chunk tree references has a handle. Tested on a
  2-device RAID1 image: open, COW transaction, data extent through
  `write_file_data`, and a missing-handle error, all passing
  `btrfs check`.

  API changes (pre-1.0):

  - `ChunkTreeCache::resolve` and `resolve_all` return
    `(devid, physical)` instead of only `physical`.
  - `BlockReader::new` takes a `devid` argument.
    `BlockReader::new_multi` takes a `BTreeMap<u64, R>`.
  - `BlockReader::inner_mut` and `into_inner` are replaced by
    `devices()` and `devices_mut()`, plus `single_device_mut()` for
    offline tools that work on one device at a time.
  - `OpenFilesystem` has a new `per_device_dev_items` field.
- `btrfs-transaction`: data write helpers.
  `Transaction::update_inode_nbytes` patches an inode's `nbytes` in
  place and leaves every other field alone (including `flags`, `rdev`,
  and `sequence`). `Transaction::write_file_data` is the single entry
  point for writing file content. It splits input into chunks of at
  most 1 MiB, allocates each as a regular data extent, inserts the
  `EXTENT_DATA` items, computes per-sector CRC32C csums (unless
  `nodatasum`), and updates the inode's `nbytes`.
- `btrfs-transaction`: inline extents.
  `Transaction::insert_inline_extent` stores small payloads in the FS
  tree leaf as an inline `EXTENT_DATA` item, with no extent tree or
  csum entries. `INODE.nbytes` grows by the unaligned payload length,
  per the on-disk convention. `write_file_data` picks inline
  automatically when `file_offset == 0` and
  `data.len() <= max_inline_data_size(sectorsize, nodesize)` (4095
  bytes with the default 16K nodesize and 4K sectorsize).
- `btrfs-transaction`: zlib and zstd compression for data writes. The
  new `try_compress(data, algorithm)` returns compressed bytes only
  when they are smaller, and callers fall back to raw otherwise.
  `Transaction::write_file_data` and `insert_inline_extent` take a
  `compression: Option<CompressionType>` parameter. Regular extents
  are compressed chunk by chunk, and each chunk falls back to raw on
  its own. `disk_num_bytes` shrinks, while `num_bytes`, `ram_bytes`,
  and `INODE.nbytes` track the logical, sector-aligned size. Csums
  always cover the compressed on-disk bytes.
- `btrfs-transaction`: LZO compression. `try_compress` produces the
  inline LZO framing (`[4B total_len LE] [4B seg_len LE] [lzo
  bytes]`). The new `try_compress_regular(data, algorithm, sectorsize)`
  applies the per-sector regular framing (`[4B total_len LE] { [4B
  seg_len LE] [lzo bytes] [zero pad] }*`) with padding to sector
  boundaries and the usual early exit (give up after 4 sectors if the
  framed output exceeds 3 sectors). `write_file_data` compresses each
  chunk with `try_compress_regular`, so regular LZO extents work. Both
  inline and regular LZO files pass `btrfs check`.
- `btrfs-disk`: `ChunkItem::to_mapping` conversion method.
- `btrfs-fuse`: library split and integration test harness. The crate
  is now both a library and a binary. `fuse/src/lib.rs` exposes
  `BtrfsFuse` and its operations (`lookup_entry`, `get_attr`,
  `read_dir`, `read_symlink`, `read_data`, `list_xattrs`, `get_xattr`,
  `stat_fs`), which return plain `io::Result` or `Option` values and
  don't depend on `fuser`. The `fuser::Filesystem` impl is a narrow
  adapter that calls them and maps the results to `Reply*` calls.
  Integration tests in `fuse/tests/basic.rs` call the operations
  directly without a FUSE mount, so they run unprivileged under plain
  `cargo test`. Each test process builds a fresh image with
  `mkfs.btrfs --rootdir` from a known directory tree. The tests cover
  lookup, getattr, readdir (including pagination and `..`
  resolution), read_data (inline, empty, multi-extent, offset, past
  EOF, nested), symlinks, xattrs, and statfs.
- `btrfs-fuse`: `listxattr`, `getxattr`, and `statfs`. Xattrs come from
  `XATTR_ITEM` entries, which use the same `DirItem` format as
  directory entries. Hash collisions are handled by a linear name scan.
  `statfs` reports `total_bytes` and `bytes_used` from the superblock
  in sectorsize blocks. Inode counts are 0 for now.
- `btrfs-fuse`: compression support. `read_file` and `read_symlink`
  decompress zlib, zstd, and LZO extents, both inline and regular. LZO
  uses the btrfs per-sector framing (a 4-byte total size header and a
  4-byte header per segment, padded to `sectorsize` boundaries). Adds
  the `flate2`, `zstd`, and `lzokay` dependencies to `btrfs-fuse`.
- `btrfs-fuse`: `readlink`, `read` for inline and regular uncompressed
  extents, and correct `..` resolution through `INODE_REF`. Prealloc
  extents and sparse holes read as zeros. Symlinks, hardlinks, and
  large multi-extent files work.
- New `btrfs-test-utils` crate at `util/testing/` with the shared RAII
  test harness (`BackingFile`, `LoopbackDevice`, `Mount`), data helpers
  (`write_test_data`, `verify_test_data`, `write_compressible_data`),
  and `single_mount`, `deterministic_mount`, `cache_gzipped_image`, and
  `mount_existing_readonly`. Dev-dependency only, not published.

### Changed

- `btrfs-mkfs`: RAID5 and RAID6 now go through `post_bootstrap`. The
  `profile_supported` allowlist accepts every RAID profile, and the
  `bootstrap_creates_post_trees` fallback in `make_btrfs`,
  `build_root_tree`, and `build_extent_tree` is gone. The bootstrap now
  writes only the four trees that always exist (root, extent, chunk,
  dev) for every profile and feature set. All other trees (FS, csum,
  data-reloc, UUID, and the optional free space, block group, and quota
  trees) are created by the post-bootstrap transaction.
  `build_empty_tree`, `build_root_dir_tree`, and `build_quota_tree`,
  which only the fallback used, are deleted. The other `build_*`
  helpers stay because the rootdir path still uses them. New regression
  tests `raid5_metadata_uuid_tree_created_by_post_bootstrap` and
  `raid6_metadata_uuid_tree_created_by_post_bootstrap` check that the
  UUID tree is at generation 2 for both profiles, using a new
  `walk_root_tree_items_multi` helper for non-mirrored metadata.
- `btrfs-mkfs`: free space tree and block group tree creation move into
  the post-bootstrap transaction for supported profiles (SINGLE, DUP,
  RAID0, RAID1*, RAID10). The bootstrap leaves every
  `BLOCK_GROUP_ITEM` in the extent tree and doesn't create either tree.
  Post-bootstrap calls `convert::create_block_group_tree`, which moves
  the items into the block group tree under the routing override, and
  `convert::seed_free_space_tree`, which derives the initial
  `FREE_SPACE_INFO` and `FREE_SPACE_EXTENT` items from the extent tree.
  RAID5 and RAID6 keep mkfs's hand-built trees as a fallback.
- `btrfs-mkfs`: `--features ^free-space-tree` also clears the
  `BLOCK_GROUP_TREE` `compat_ro` bit, since the kernel requires the free
  space tree for the block group tree. Previously, disabling only the
  free space tree produced an image the kernel rejects.
- `btrfs-mkfs`: quota tree creation (`-O quota`, `-O squota`) moves
  into the post-bootstrap transaction for supported profiles. A new
  `insert_raw_item` helper writes the three qgroup items (STATUS, INFO,
  LIMIT) for the FS tree's qgroupid (0/5). Regular quota gets the
  INCONSISTENT flag and zeroed info, so the kernel rescans. Squota gets
  the SIMPLE_MODE flag and info pre-filled with FS tree usage. RAID5
  and RAID6 fall back to the mkfs-built quota tree.
  `MkfsConfig::now_secs` is now `pub(crate)`, and the post-bootstrap
  `apply_in_transaction` takes the full `MkfsConfig` so later
  feature-gated trees can read their settings the same way.
- `btrfs-mkfs`: FS tree creation moves into the post-bootstrap
  transaction when there is no `--rootdir` and the profile is supported
  (SINGLE, DUP, RAID0, RAID1*, RAID10). It follows the data-reloc tree
  pattern, with FS-tree-specific `ROOT_ITEM` changes: the embedded
  inode flags get `BTRFS_INODE_ROOT_ITEM_INIT`, the `ROOT_ITEM` `uuid`
  is derived from the fsid by flipping bits (as btrfs-progs does), and
  ctime and otime use `cfg.now_secs()`. Verified by mounting a
  default-profile image and writing and reading back a file. RAID5,
  RAID6, and the rootdir path still build the FS tree directly.
- `btrfs-mkfs`: data-reloc tree creation moves into the post-bootstrap
  transaction for supported profiles (SINGLE, DUP, RAID0, RAID1*,
  RAID10). As with the csum tree, `make_btrfs` skips it at bootstrap.
  Post-bootstrap creates the empty tree, inserts the root directory
  `INODE_ITEM` with `Transaction::create_inode`, adds a
  `(256, INODE_REF, 256)` ".." self-reference, and patches the
  `ROOT_ITEM` to make it a subvolume-shaped tree (`root_dirid = 256`,
  embedded `inode_data` matching the inode). The rootdir path, RAID5,
  and RAID6 keep the hand-built tree.
- `btrfs-mkfs`: csum tree creation moves into the post-bootstrap
  transaction for supported profiles (SINGLE, DUP, RAID0, RAID1*,
  RAID10). Without `--rootdir`, `make_btrfs` skips the csum tree at
  bootstrap and `post_bootstrap` creates it idempotently with
  `Transaction::create_empty_tree`. The rootdir path still creates it
  directly because it has csum items to insert during bootstrap. RAID5
  and RAID6, which don't support post-bootstrap, keep the hand-built
  csum tree. This is the first tree moved out of mkfs's bootstrap and
  sets the pattern for the other always-present trees.
- `btrfs-disk`: `csum_tree_block` and `csum_superblock` dispatch on
  `ChecksumType` (CRC32C, xxhash64, SHA-256, BLAKE2b). The transaction
  crate's `ExtentBuffer::update_checksum` and commit flush read the
  algorithm from `Filesystem::superblock.csum_type`, so commits on
  non-CRC32C filesystems now write the right checksum. `xxhash-rust`,
  `sha2`, and `blake2` move from `btrfs-mkfs` to `btrfs-disk`.
- `btrfs-mkfs`: `ChecksumType` is a re-export of the `btrfs-disk` enum.
  `write::fill_csum` is removed, and callers use
  `btrfs_disk::util::csum_tree_block` and
  `btrfs_disk::superblock::csum_superblock` directly. The
  post-bootstrap transaction no longer requires CRC32C, so xxhash,
  SHA-256, and BLAKE2b images also get a UUID tree.
- `cli`, `uapi`, and `stream` integration tests use the mount, loopback,
  and backing-file helpers from `btrfs-test-utils` instead of their own
  copies. Each crate's `tests/common.rs` is a thin re-export plus
  crate-local fixture image paths. `BackingFile::mkfs_rootdir` no
  longer hard-codes `env!("CARGO_BIN_EXE_btrfs")`. Callers pass the
  `btrfs-mkfs` binary path.

### Fixed

- `btrfs-mkfs`: `--features ^free-space-tree` no longer writes an
  unused empty free space tree leaf and its `ROOT_ITEM`. The free space
  tree is now an optional slot in `BlockLayout`, like the block group
  and quota trees, gated on `cfg.has_free_space_tree()`. This saves one
  `nodesize` block per `^free-space-tree` image and matches what
  btrfs-progs `mkfs.btrfs` produces.
- `btrfs-transaction`: writes now route correctly on RAID0 and RAID10.
  Previously every commit used `resolve_all`, which writes to all
  stripes. That is right for DUP and RAID1*, but on RAID0 it copied
  each row to every device (corruption) and on RAID10 it wrote to every
  mirror pair instead of the row's own pair. mkfs's `post_bootstrap`
  UUID tree creation no longer skips RAID0 and RAID10 images.
- `btrfs-transaction`: `update_free_space_tree` respects the
  `FREE_SPACE_TREE` compat_ro flag. When the flag is clear, the update
  is skipped even if tree 10 exists on disk. This lets
  `btrfs-mkfs --features ^free-space-tree` run `post_bootstrap`
  cleanly. In that case mkfs leaves a stale free space tree leaf, which
  the kernel ignores because the flag is clear.
- `btrfs-transaction`: `DelayedRefQueue` uses a `BTreeMap` instead of a
  `HashMap`, so `flush_delayed_refs` processes refs in a fixed key
  order. Previously, hash randomization could make commits of the same
  input produce different bytes, which showed up as flaky snapshot
  tests. `DelayedRefKey` now derives `PartialOrd, Ord`.
- `mkfs --rootdir`: `EXTENT_DATA` items for files whose size isn't a
  multiple of sectorsize now use `align_up(extent_size, sectorsize)`
  for `num_bytes` and `ram_bytes`, and add the same value to the
  inode's `nbytes`. Previously the unaligned `extent_size` produced
  images that failed `btrfs check` with `bad file extent, nbytes wrong`.
  Compressed extents where `aligned_disk != aligned_logical` were also
  affected.
- `btrfs-transaction`: fixed a fall-through in `flush_delayed_refs`
  where the data ref drop path ran unconditionally after the add path.
  A `todo!()` in the add branch had hidden it.

## 0.11.0

### Added

- `btrfs-fuse` (experimental): new MIT or Apache-2.0 crate with a
  userspace FUSE driver built on `btrfs-disk`. This first version
  implements `lookup`, `getattr`, and `readdir` on the default FS tree
  of an unmounted image or block device. File reads, xattrs, multiple
  subvolumes, and key-based tree descent will follow.

- `btrfs-transaction`: building blocks for whole-tree conversion, and
  two conversions built on them:

  - **`Transaction::create_empty_tree(fs_info, tree_id)`**: allocates
    a metadata block, initialises it as an empty leaf with the
    filesystem's fsid and chunk_tree_uuid, records the
    `(tree_id -> bytenr)` mapping in memory, and inserts a `ROOT_ITEM`
    into the root tree. Rejects bootstrap ids `0..=3` and existing tree
    ids.
  - **`extent_walk` module**: read-only `walk_block_group_extents`, a
    callback scanner over `EXTENT_ITEM` and `METADATA_ITEM` keys in a
    block group's range, and `derive_free_ranges` to compute the free
    ranges between them. Detects overlapping and out-of-bounds extents.
  - **`convert::convert_to_free_space_tree`**: in one transaction,
    creates the free space tree root, walks every block group, derives
    free ranges, inserts `FREE_SPACE_INFO` and `FREE_SPACE_EXTENT`
    items, sets the `FREE_SPACE_TREE` and `FREE_SPACE_TREE_VALID`
    `compat_ro` bits, and zeroes `cache_generation`. Simple cases only.
  - **`Filesystem::block_group_tree_id` and `bg_tree_override`**: one
    accessor for routing block group reads, replacing duplicated logic
    in `allocation::load_block_groups` and
    `Transaction::update_block_group_used`. The override keeps the
    allocator off the new block group tree while the conversion fills
    it.
  - **`convert::convert_to_block_group_tree`**: in one transaction,
    creates the block group tree root, copies every `BLOCK_GROUP_ITEM`
    from the extent tree into it, deletes the originals, and sets the
    `BLOCK_GROUP_TREE` `compat_ro` bit. `bg_tree_override` stays pinned
    to tree 2 throughout, so the allocator keeps reading the extent
    tree. Handles block group counts that span multiple leaves.

- `btrfs-tune --convert-to-block-group-tree`: converts an unmounted
  filesystem to the block group tree (`BLOCK_GROUP_TREE` compat_ro
  feature) in a single transaction, using
  `convert_to_block_group_tree`. The free space tree must be enabled
  first (a kernel requirement). Can be combined with
  `--convert-to-free-space-tree`, in which case both conversions run
  in order. Privileged tests cover the conversion (checked with
  `btrfs check` and `dump-super`) and the free space tree
  precondition.

- `btrfs-tune --convert-to-free-space-tree`: converts an unmounted
  filesystem to the v2 free space tree (`FREE_SPACE_TREE` compat_ro
  feature) in a single transaction, using
  `convert_to_free_space_tree`. Only simple cases are handled. It
  refuses if the free space tree is already enabled, if a stale free
  space tree root exists, or if v1 free space cache items remain in the
  root tree (remove them first with `btrfs rescue clear-space-cache`).
  A privileged test converts a `^free-space-tree` image and checks
  `btrfs check` and the resulting `compat_ro` bits.

- `btrfs filesystem resize --offline`: grows an unmounted
  single-device image or block device in place. Shrinking is rejected.
  Updates the `DEV_ITEM` in the chunk tree and `superblock.total_bytes`
  in a transaction, then extends the backing file for image files.
  Honors `--dry-run`. Accepts `max` (image files only), absolute sizes,
  and `+<amount>`. `cancel`, shrinks, and multi-device filesystems are
  rejected with clear errors. Privileged tests cover growing (with
  `btrfs check`) and shrink rejection.

## 0.10.0

### Added

- `btrfs-transaction`: dropping data extent refs. The delayed ref queue
  keys metadata and data backrefs separately. The flush path finds
  `EXTENT_DATA_REF` records (inline or standalone), decrements
  `EXTENT_ITEM.refs`, deletes extents that are no longer referenced,
  and trims overlapping `EXTENT_CSUM` items, handling head, tail, and
  middle splits. New `btrfs-disk` helpers: public
  `extent_data_ref_hash` and `inline_ref_size`. New
  `transaction::items::shrink_item`.
- `btrfs-transaction`: chunk tree COW. The allocator separates
  `BlockGroupKind::Metadata` from `BlockGroupKind::System` with a
  cursor for each, `alloc_tree_block` puts chunk tree blocks in a
  SYSTEM block group, and `ensure_in_sys_chunk_array` adds newly
  allocated SYSTEM chunks to the superblock's bootstrap array. New
  `btrfs-disk` helpers: the `chunk_item_bytes` serializer,
  `sys_chunk_array_contains`, and `sys_chunk_array_append`.
- `btrfs rescue clear-ino-cache`: walks every fs tree, finds items
  under `BTRFS_FREE_INO_OBJECTID` (and `BTRFS_FREE_SPACE_OBJECTID`,
  which old kernels used for the per-inode bitmap), drops every
  referenced data extent through delayed refs, and deletes the items.
  Each subvolume is cleaned in its own transaction. A privileged test
  runs the no-op case on a fresh filesystem with several subvolumes. A
  real fixture can't be built on modern systems because the kernel no
  longer has the `inode_cache` mount option.
- `btrfs rescue clear-space-cache --version v1`: for every block
  group, deletes the `FREE_SPACE_HEADER` from the root tree, drops each
  data extent referenced by the cache inode's `EXTENT_DATA` items,
  deletes those items and the `INODE_ITEM`, and bumps the superblock
  `cache_generation` to invalidate the cache. A privileged test covers
  a full rw remount with `space_cache=v1`.
- `btrfs rescue clear-space-cache --version v2`: walks the
  FREE_SPACE_TREE, drops every block, removes its ROOT_ITEM, clears the
  `FREE_SPACE_TREE` and `FREE_SPACE_TREE_VALID` compat_ro flags, and
  commits. The kernel rebuilds the tree on the next mount. Refuses to
  run when `BLOCK_GROUP_TREE` is enabled, since the block group tree
  requires the free space tree. A privileged test covers a full rw
  remount.
- `btrfs rescue fix-data-checksum`: walks the csum tree, recomputes
  CRC32C for every covered sector, and reports mismatches in
  `--readonly` mode (the default). With `--mirror 1`, mismatched csums
  are rewritten in place with `item_data_mut` and committed in one
  transaction. Multiple mirrors, `--interactive`, and checksum types
  other than CRC32C are not supported yet. Tests cover a clean scan and
  a corrupt-and-repair round trip.
- `btrfs rescue fix-device-size`: walks DEV_ITEMs, rounds misaligned
  `total_bytes` down, shrinks values larger than the real device when
  no DEV_EXTENT would be lost, copies the change into the superblock's
  embedded dev_item, and recomputes the superblock `total_bytes`. A
  privileged test covers the shrink case and checks the result with
  `btrfs check`.
- `btrfs rescue clear-uuid-tree`: walks the UUID tree, drops extent
  refs for every block, deletes the ROOT_ITEM, and commits. An
  end-to-end test round-trips through a rw mount.
- transaction: every commit updates the free space tree.
  `FREE_SPACE_EXTENT` and `FREE_SPACE_INFO` items stay consistent with
  the extent tree, so rw mounts no longer hang on stale entries.
  Bitmap-layout block groups are detected and rejected with a clear
  error.

### Changed

- transaction: the convergence loop is now `flush_delayed_refs →
  update_root_items → snapshot_roots → update_free_space_tree`. The
  snapshot is taken before the free space tree update so the next
  pass's `update_root_items` sees its root changes. The pass limit
  went from 16 to 32.
- transaction tests: every `btrfs check` helper runs in strict mode,
  and the old `free space` and `cache` filters are removed. The
  proptest harness passes strict checks at `PROPTEST_CASES=1000`.

## 0.9.0

### Added

- `mkfs.btrfs`: all RAID profiles (RAID0, RAID1C3, RAID1C4, RAID10,
  RAID5, RAID6) for both metadata and data block groups, with
  profile-specific stripe mapping in `logical_to_physical()`
- `mkfs.btrfs -O quota`: create a quota tree with status, info, and
  limit items for the filesystem tree (INCONSISTENT flag, requires a
  rescan)
- `mkfs.btrfs -O squota`: create a simple quota tree with the
  SIMPLE_MODE flag, the `enable_gen` field, and pre-populated qgroup
  usage
- `btrfs device stats --offline`: read device error statistics from
  the on-disk device tree without a mounted filesystem
- `btrfs subvolume show`: display quota group usage and limits
  (referenced and exclusive bytes) with unit flag support
  (`--raw`, `--iec`, `--si`, `--kbytes`, `--mbytes`, `--gbytes`, `--tbytes`)
- `--format json` for `device stats`, `filesystem df`,
  `filesystem du`, `qgroup show`, `subvolume show`, `subvolume list`,
  and `subvolume get-default`, using the btrfs-progs wrapper format
  with `__header`
- `--dry-run` for `subvolume delete`: print what would be deleted
  without removing anything. `--dry-run` on commands that don't
  support it now returns an error instead of being ignored
- `btrfs device stats -T`: table output with columns for device ID,
  path, and error counters (uses the `cols` crate)
- Multicall binary: with the `multicall` cargo feature, the `btrfs`
  binary dispatches on program name (`mkfs.btrfs`, `btrfs-mkfs`,
  `btrfstune`, `btrfs-tune`)
- Sysfs module unit tests covering all accessors (numeric, string,
  boolean, commit stats, features, quota status, scrub speed, qgroup
  entry parsing)
- `--format modern` output mode (`BTRFS_OUTPUT_FORMAT=modern` env):
  opt-in improved formatting with adaptive column widths via `cols`
- `btrfs subvolume list --format modern`: tree view with Unicode
  connectors showing the subvolume hierarchy
- `btrfs inspect list-chunks --format modern`: cols-based adaptive
  table with right-aligned numeric columns
- `btrfs inspect list-chunks --offline`: read chunks from an unmounted
  device or image file by walking the on-disk chunk and block group
  trees. Does not require CAP_SYS_ADMIN
- `btrfs inspect min-dev-size --offline`: compute the minimum device
  size from an unmounted device or image file by walking the device
  tree. Does not require CAP_SYS_ADMIN
- `btrfs filesystem du --format modern`: tree view with Unicode
  connectors showing the directory hierarchy via `cols`
- `btrfs filesystem du --format json`: JSON output with per-entry
  total, exclusive, and set_shared byte counts
- `btrfs filesystem du --depth N`: limit display depth while still
  computing full totals (0 is equivalent to --summarize)
- `btrfs filesystem du --sort`: sort entries within each directory by
  path, total, exclusive, or shared (modern output only)
- `btrfs filesystem df --format modern`: cols-based adaptive table
- `btrfs quota status --format modern`: key-value table with the UUID
  and rescan progress, which the text output doesn't show
- `btrfs qgroup show --format modern`: tree view of the qgroup
  hierarchy with all columns (rfer, excl, max_rfer, max_excl) always
  visible
- `btrfs scrub start --format modern`: live progress on stderr
  (updated every 200ms on a terminal, every 1s otherwise), a summary
  table with per-device data, metadata, and error stats, and a raw mode
  tree with all kernel counters per device
- `btrfs scrub status --format modern`: cols table with per-device
  scrubbed and allocated bytes and an error summary
- `btrfs filesystem show --format modern`: clean header with a device
  table (DEVID, SIZE, USED, PATH)
- `btrfs device usage --format modern`: tree view with each device as
  a root node and its per-profile allocations as children
- `btrfs filesystem usage --format modern`: three sections with
  overall stats as key-value pairs, a profile summary table, and a
  per-device allocation table with one column per profile in use
- `RunContext` struct for passing runtime options to commands
- `Runnable::supported_formats()`: commands declare which formats they
  support, and unsupported formats produce a clear error

### Changed

- Running `btrfs` or a command group (`btrfs filesystem`,
  `btrfs device`, etc.) without a subcommand shows help instead of an
  error
- The `BTRFS_OUTPUT_FORMAT` env var is resolved manually instead of
  through clap's `env` attribute, so it no longer interferes with
  `arg_required_else_help`
- `SearchKey` is renamed to `SearchFilter`, with a compound `Key`
  struct. The `(objectid, item_type, offset)` triple is now one `Key`
  type, and `SearchFilter` uses `start` and `end` keys instead of six
  separate min and max fields, which makes the compound key semantics
  explicit

### Fixed

- `btrfs subvolume list`: snapshots and subvolumes with non-zero
  ROOT_ITEM key offsets now show the correct parent ID and name. The
  tree search callback didn't filter on `hdr.item_type`, so ROOT_BACKREF
  and ROOT_REF items were parsed as ROOT_ITEM data

## 0.8.0

### Added

- `btrfs check`: read-only filesystem verification in 7 phases:
  superblock validation, tree structure checks, extent reference
  verification, chunk and block group cross-checks, FS tree inode
  consistency, checksum tree validation (with optional
  `--check-data-csum`), and ROOT_REF and ROOT_BACKREF consistency
- `btrfs check`: directory inode size validation, file nbytes
  validation, missing extent item detection, and backref owner
  cross-checks in both directions
- `mkfs --rootdir`: populate a new filesystem from a directory tree,
  with regular files (inline and regular extents up to 1 MiB),
  directories, symlinks, hardlinks, xattrs, and special files
- `mkfs --rootdir --compress`: zlib, zstd, and LZO compression for
  rootdir population (LZO uses the per-sector framed format for regular
  extents and the single-segment format for inline extents)
- `mkfs --rootdir --subvol`: create subdirectories as separate btrfs
  subvolumes with their own FS trees, ROOT_REF and ROOT_BACKREF links,
  read-only support (`ro:`, `default-ro:`), default subvolume
  selection, and nesting
- `mkfs --rootdir --reflink`: clone file extents with FICLONERANGE
  instead of copying bytes (source and image must be on the same
  filesystem)
- `mkfs --rootdir --inode-flags`: set NODATACOW and NODATASUM flags on
  specific paths during rootdir population
- `mkfs --rootdir --shrink`: truncate the image to the used size after
  populating from rootdir
- `btrfs mkfs` and `btrfs tune` optional subcommands: enable them with
  the `mkfs` and `tune` cargo features for a single binary
- `btrfs-disk`: `tree_walk_mut` for mutable DFS tree traversal with
  automatic checksum recomputation (used by the tune fsid rewrite)
- `btrfs-uapi`: `filesystem::is_mounted` is the canonical mount check
  and returns `Result<bool>` so errors propagate
- Specification documents for btrfs internals in `docs/spec/`: on-disk
  format, chunks and block groups, extent tree and backrefs, check
  phases, and the mkfs process
- `#![warn(clippy::pedantic)]` enabled across all crates
- Rustdoc for all public types in `btrfs-disk` and `btrfs-uapi`, with
  explanations of the btrfs on-disk format
- End-to-end integration tests for mkfs --rootdir (basic, compressed,
  shrink) with mount and data verification
- 8 existing integration tests now verify their effects through uapi
  (label, resize, quota, subvolume, property, device, qgroup)

### Changed

- `mkfs` argument help is grouped under headings: Block layout,
  Features, Identity, and Rootdir population
- `mkfs` Profile, ChecksumArg, and Feature enums use clap `ValueEnum`
  instead of manual `FromStr`, with backward-compatible aliases
- `mkfs` `--verbose` changed from bool to a u8 count to match the
  CLI's global `--verbose` type
- `btrfs-uapi`: `device_remove` and `replace_start` take references
  instead of owned values
- `btrfs-tune`: uses `tree_walk` and `tree_walk_mut` from `btrfs-disk`
  instead of its own recursive tree traversal
- Unified duplicated tree builder functions in mkfs (block group, free
  space, superblock) with the `UsedBytes` and `SuperblockParams`
  structs
- Inode field patching in rootdir uses `offset_of!` instead of
  hard-coded byte offsets
- flate2, zstd, and lzokay moved to workspace dependencies
- Stream CRC validation uses incremental `crc32c_append` instead of
  allocating a contiguous buffer per command

### Fixed

- `btrfs-disk`: CRC32C checksums for superblocks and tree blocks now
  use standard CRC32C (matching the kernel's `hash_crc32c`) instead of
  raw CRC32C with seed=0
- `btrfs check`: item data extraction now accounts for the tree block
  header offset
- `mkfs --rootdir`: removed a spurious CHUNK_TREE ROOT_ITEM that
  confused the backref validation in btrfs-progs check
- `mkfs --rootdir`: include data extent bytes in superblock
  `bytes_used`
- `mkfs --rootdir --shrink`: update `total_bytes` in the chunk tree
  DEV_ITEM and all superblock mirrors, not only the primary superblock

## 0.7.0

### Added
- `btrfs restore`: file recovery from damaged or unmounted filesystems,
  with support for regular files (inline, regular, and prealloc
  extents), directories, symlinks (`-S`), extended attributes (`-x`),
  metadata restoration (`-m`), compressed extents (zlib, zstd, lzo),
  path regex filtering, snapshot restoration (`-s`), tree root listing
  (`-l`), and superblock mirror fallback
- `btrfs-tune` crate: offline superblock tuning tool with feature flag
  enabling (`-r`, `-x`, `-n`), seeding flag management (`-S`),
  metadata UUID change (`-m`, `-M UUID`), and full fsid rewrite
  (`-u`, `-U UUID`)
- Man page generation for `btrfs-tune`
- `btrfs-disk`: rustdoc for all public types and fields, and a
  crate-level overview (filesystem layout, tree descriptions, usage
  guide)
- `btrfs-tune`: crate-level and module-level rustdoc
- `btrfs-disk`: `BlockReader::write_block` for writing tree blocks by
  logical address, and `csum_tree_block` for recomputing tree block
  checksums

## 0.6.0

### Added

- `btrfs inspect-internal tree-stats`: walk any B-tree and report node
  counts, seek statistics, cluster sizes, and inline data bytes
- `btrfs rescue super-recover`: scan all superblock mirrors and copy
  the highest-generation one to every mirror location
- `btrfs rescue zero-log`: clear the log tree root pointer so
  filesystems with a corrupted log tree can mount
- `btrfs rescue create-control-device`: create `/dev/btrfs-control` if
  missing
- Shell completion generation for bash, fish, and zsh with
  `btrfs-gen completions`
- `nix flake check` runs clippy, rustfmt, and unit tests
- taplo for TOML formatting and linting
- cargo-deny for dependency license and vulnerability auditing
- Developer and user documentation (mdBook)
- `LICENSE.md` files for all crates
- `BlockGroupFlags` and five more bitflags types in `btrfs-disk` for
  typed on-disk field parsing
- Integration tests adapted from the btrfs-progs cli-tests (mkfs
  validation, on-disk format verification, nodesize and sectorsize
  matrix)
- The mkfs library validates nodesize, sectorsize, and mixed-bg
  constraints (previously only the CLI entry point did)

### Changed

- CI uses `nix flake check` instead of manual cargo invocations
- `mangen` binary renamed to `btrfs-gen`, which holds all generation
  tools
- `btrfs-mkfs` binary renamed from `mkfs-btrfs` for consistency
- `btrfs-disk` API renamed: `Dev` -> `Device`, `Fs` -> `Filesystem`,
  and function names follow the `noun_verb` convention
- LZO decompression switched from the `lzo1x` crate to `lzokay`
- Pedantic clippy lints fixed across `btrfs-disk`

### Fixed

- LZO decompression sector alignment in `btrfs receive` for streams
  with sectors other than 4096 bytes

### Changed (licensing)

- `btrfs-uapi`, `btrfs-disk`, and `btrfs-stream` relicensed from
  GPL-2.0-only to MIT OR Apache-2.0 so non-GPL projects can depend on
  them

## 0.5.0

### Added

- `mkfs.btrfs` multi-device support with RAID1 metadata: superblocks on
  every device, `DEV_ITEM` and `DEV_EXTENT` entries for all devices,
  and block group flags derived from the configured profiles
- `mkfs.btrfs` supports all four checksum algorithms: crc32c, xxhash,
  sha256, and blake2b
- `mkfs.btrfs` enables the block-group-tree feature by default
- `mkfs.btrfs` RAID0 data profile support
- `mkfs.btrfs` RAID1C3 and RAID1C4 metadata profile support
- `mkfs.btrfs` writes the superblock to all three mirror locations
- `mkfs.btrfs` man page generation
- Integration tests for `btrfs replace start/status/cancel`
- `btrfs device remove --enqueue` flag
- `btrfs inspect-internal list-chunks --sort` flag
- `btrfs filesystem df/show`, `list-chunks`, and `scrub status/limit`
  support unit flags (`-b`, `-H`, `--iec`, `--si`, `-k/-m/-g/-t`)
- Multi-level `-v` and `-q` flags through `env_logger`

### Changed

- `btrfs filesystem defrag` and `btrfs subvolume delete` no longer
  declare their own `-v` flag. The global flag controls verbosity
- The stream parser uses a typed `StreamError` instead of
  `anyhow::Error`
- Size formatting unified under one `SizeFormat` enum and the
  `fmt_size()` helper
- Time formatting uses `chrono` throughout
- LE reader helpers deduplicated into `uapi/src/util.rs` and
  `disk/src/util.rs`

### Fixed

- Nix build: include the fixture image, add the gzip dependency, and
  pin the test timezone

## 0.4.0

### Added

- `btrfs inspect-internal dump-tree`: dumps on-disk trees straight
  from a block device or image file, without `CAP_SYS_ADMIN`. It
  bootstraps from superblock → sys_chunk_array → chunk tree → root
  tree and has 30+ item type formatters whose output matches the C
  reference exactly. Supports `-t`, `-b`, `--follow`, `--bfs`, `--dfs`,
  `--hide-names`, `--csum-headers`, `--csum-items`, and
  `-e/-d/-u/-r/-R`
- `mkfs.btrfs` initial implementation: creates a mountable
  single-device filesystem with DUP metadata and SINGLE data block
  groups. Validates devices, wires up feature flags, and writes 8 tree
  blocks and the superblock with CRC32C checksums
- `btrfs scrub start/resume` missing flags: `-B`, `-d`, `-R`, `-f`,
  `--limit`, and `-c` and `-n` (ioprio class and classdata)
- Ioctl wrappers: `BTRFS_IOC_INO_LOOKUP_USER`, `BTRFS_IOC_GET_FEATURES`,
  `BTRFS_IOC_GET_SUPPORTED_FEATURES`, `BTRFS_IOC_SUBVOL_SYNC_WAIT`
- Nix flake for reproducible builds and a dev shell
- CI configuration
- Fixture snapshot tests for `subvolume list/show`, `dump-super`, and
  quota commands

### Changed

- `dump-super` display logic moved from `disk/` to `cli/` to keep
  formatting code out of the disk crate

## 0.3.0

### Added

- `btrfs-stream` split out as a standalone crate with a
  platform-independent send stream parser and CRC32C validation. The
  Linux-only `receive` feature adds `ReceiveContext`
- `btrfs receive`: v2 stream commands (`ENCODED_WRITE` with zlib, zstd,
  and lzo decompression fallback, `FALLOCATE`, `FILEATTR`), v3
  `ENABLE_VERITY`, and `--chroot` mode
- `btrfs subvolume create`: `-i`/`--qgroup` and `-p`/`--parents` flags
- `btrfs subvolume delete`: `-c`/`--commit-after`, `-C`/`--commit-each`,
  `-i`/`--subvolid`, `-R`/`--recursive`, `-v`/`--verbose` flags
- `btrfs subvolume snapshot`: `-r`/`--readonly`, `-i`/`--qgroup` flags
- `btrfs device add`: `-f`/`--force`, `-K`/`--nodiscard`, `--enqueue`
  flags
- `btrfs device usage`: full per-device allocation breakdown from a
  chunk tree walk
- Help text snapshot tests covering all subcommands

### Fixed

- `btrfs property set ro` on a subvolume

## 0.2.0

### Added

- `btrfs send`: full and incremental sends using a pipe and a reader
  thread, with the `-e`, `-p`, `-c`, `-f`, `--no-data`, `--proto`, and
  `--compressed-data` flags. The protocol version is negotiated
  through sysfs
- `btrfs receive`: full v1 stream processing with all 22 command
  types, `--dump` mode, `--chroot`, and `-E`/`--max-errors`
- `btrfs balance`: filter string parsing for all filter types
  (`profiles`, `usage`, `devid`, `drange`, `vrange`, `convert`, `soft`,
  `limit`, `stripes`), `min..max` ranges, and `|`-separated profile
  names
- `btrfs device scan --all-devices` (`-d`) flag
- Man page generation with the `btrfs-mangen` binary
- CLI argument parsing tests

## 0.1.0

Initial release.

### Added

- `btrfs filesystem df`: space usage by chunk type
- `btrfs filesystem defrag`: single file and recursive directory
  defragmentation
- `btrfs filesystem resize`: online resize
- `btrfs filesystem mkswapfile`: swapfile creation
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
- Argument parsing stubs for `btrfs check`, `btrfs restore`, and all
  `btrfs rescue` subcommands
- `btrfs-uapi` safe ioctl wrappers
- `btrfs-disk` on-disk format parser (superblock, tree nodes, chunk
  tree)
