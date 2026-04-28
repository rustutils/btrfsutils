# Changelog

All notable changes to this project will be documented in this file.

## Unreleased

### Added

- `btrfs-fs`: `CacheConfig` struct and `Filesystem::open_with_caches`
  / `Filesystem::open_subvol_with_caches` constructors that let
  embedders override the default cache sizes (4096 tree blocks, 4096
  inodes, 1024 extent maps). `CacheConfig::no_cache()` provides the
  minimum-viable single-entry caches for benchmarking the cold path
  or memory-constrained embedders. Existing `Filesystem::open` /
  `Filesystem::open_subvol` continue to use the defaults.
- `btrfs-fs`: `Filesystem::forget(Inode)` evicts a single inode from
  both the inode and extent-map caches. Embedders that observe
  inode-level invalidation events can call this to release memory
  ahead of LRU eviction.
- `btrfs-fuse`: `init` callback negotiates kernel capabilities at
  mount time. Currently opts into `FUSE_AUTO_INVAL_DATA` (kernel
  page-cache invalidation when `getattr` reports changes) and
  `FUSE_SPLICE_READ` / `FUSE_SPLICE_WRITE` (zero-copy data path).
  `FUSE_DO_READDIRPLUS` will land in the follow-up commit that adds
  the `readdirplus` callback.
- `btrfs-fuse`: `forget` callback wired through to
  `Filesystem::forget`. The default `batch_forget` impl in fuser
  iterates over each `ForgetOne` and calls `forget`, so we don't
  override `batch_forget` separately.
- `btrfs-fuse`: `BtrfsFuse::open_with_caches` /
  `BtrfsFuse::open_subvol_with_caches` constructors mirroring the
  `btrfs-fs` additions.
- `btrfs-fuse` CLI: `--cache-tree-blocks N` (default 4096),
  `--cache-inodes N` (default 4096), `--cache-extent-maps N`
  (default 1024), and `--no-default-permissions` to bypass the
  kernel's per-file mode/uid/gid checks. Default behaviour now
  enables the kernel `default_permissions` mount option so the FUSE
  mount enforces stored ownership the way kernel btrfs does;
  `--no-default-permissions` opts out for image-inspection scenarios
  where stored UIDs don't match the local system.

### Changed

- `btrfs-uapi`: new `tree_search_auto` runs `BTRFS_IOC_TREE_SEARCH_V2`
  first and transparently falls back to `BTRFS_IOC_TREE_SEARCH` (v1)
  when the underlying driver doesn't support v2. Triggers on
  `ENOPROTOOPT` (our `btrfs-fuse` driver's signal — see below),
  `ENOTSUP`/`EOPNOTSUPP`, and `ENOTTY`/`ENOSYS` (very old kernels
  pre-dating v2). Fallback is only attempted when v2 errored before
  invoking the user callback, so a transient mid-walk error can't
  duplicate items.
- `btrfs-fuse`: `BTRFS_IOC_TREE_SEARCH_V2` now returns `ENOPROTOOPT`
  rather than attempting a `FUSE_IOCTL_RETRY` round-trip the kernel
  won't honour for restricted ioctls. `tree_search_auto` in
  `btrfs-uapi` recognises this signal and falls back to v1
  transparently. `ENOPROTOOPT` was picked over `ENOTSUP` because
  nothing else in the btrfs ioctl surface returns it, so it acts as
  a private channel that can't be confused with generic
  "unsupported op" errors. See `fs/PLAN.md` § F6.4.
- `btrfs-fuse`: switched from the git pin on `xfbs/fuser` back to
  `fuser = "0.17"` from crates.io. The patched `ReplyIoctl::retry`
  API and the extra `arg: u64` parameter on `Filesystem::ioctl` are
  no longer needed — F6.4's uapi-level fallback supplants the
  kernel-side retry handshake. Companion changes: `publish = false`
  removed from `fuse/Cargo.toml` so the crate can publish to
  crates.io alongside the rest of the workspace, and the
  corresponding `allow-git` entry in `deny.toml` is gone. The
  `IoctlOutcome::Retry` variant is removed.

### Added

- `btrfs-fs`: new crate exposing a high-level read-only filesystem
  API on top of `btrfs-disk`: `Filesystem<R>` with `lookup`, `readdir`,
  `read`, `readlink`, `getattr`, `xattr_get`/`xattr_list`, and
  `statfs`. FUSE-independent — drives the `btrfs-fuse` mount and any
  other embedder. Inodes are modelled as `(SubvolId, ino)` to leave
  room for multi-subvolume traversal. The handle is `Clone` (cheap
  `Arc` bump) and all operations are `async fn`, so multiple tokio
  tasks can drive the same filesystem concurrently. Sync I/O is
  wrapped in `tokio::task::spawn_blocking` so the runtime is never
  blocked on disk reads. Future work (a native async I/O backend,
  per-thread readers, lock-free cache hits) won't change the API.
  `R: Read + Seek + Send + 'static` is the bound; for `File` and
  `Cursor<Vec<u8>>` it's free.

### Changed

- `btrfs-fuse` shrinks to a thin `fuser::Filesystem` adapter on top
  of `btrfs-fs`. The 19 read-path integration tests move with the
  logic to `fs/tests/basic.rs`. The fuse public library API
  (`BtrfsFuse::lookup_entry` and friends) is removed; new embedders
  should depend on `btrfs-fs` directly. The `read.rs`, `xattr.rs`,
  `dir.rs`, and `stat.rs` modules — and the dependencies on
  `flate2`/`zstd`/`lzokay`/`btrfs-disk`/`libc` — are gone from
  `fuse/`. The outer `Mutex<Filesystem<File>>` in `BtrfsFuse` is also
  gone now that `Filesystem` is `&self`-callable.
- `btrfs-fuse` carries an internal multi-thread tokio runtime. Each
  FUSE callback spawns a task that owns the `Reply*` handle, awaits
  the async filesystem op, and replies from the task — the FUSE
  worker thread returns immediately, so concurrent FUSE callbacks
  don't serialise on a single in-flight I/O.
- `btrfs-fuse` integration tests: 10 unprivileged tests in
  `fuse/tests/mount.rs` that spawn the `btrfs-fuse` binary, mount it
  against a fixture image, and exercise the mounted filesystem
  through ordinary POSIX calls (`std::fs`, `xattr` crate). Coverage:
  mount/unmount lifecycle, root listing, file reads (small, large,
  nested), `stat` (verifies the 1↔256 inode swap), readlink, xattr
  get/list, and a 16-thread × 50-iteration concurrent-read stress
  test that catches double-reply, dropped-reply, and deadlock bugs in
  the spawn-task dispatch path. `MountedFuse` RAII guard handles
  cleanup with lazy unmount so a panicked test doesn't wedge the
  mountpoint.
- `btrfs-disk`: pluggable tree-block cache via `TreeBlockCache`
  trait. `BlockReader::read_tree_block` returns `Arc<TreeBlock>`,
  consults the attached cache before disk, and populates on miss.
  Trait is `Send + Sync` with `&self` methods (interior mutability)
  so the cache is shareable across threads. `btrfs-disk` ships the
  trait only — no LRU implementation, no extra deps; embedders
  provide their own.
- `btrfs-fs`: three-layer caching wired into `Filesystem`.
  `LruTreeBlockCache` (the `TreeBlockCache` impl) plus inode and
  per-inode extent-map caches. Default capacities: 4096 tree blocks
  (~64 MiB), 4096 inodes, 1024 extent maps. `Filesystem::open`
  builds and attaches the caches automatically.
  `Filesystem::tree_block_cache_stats() -> CacheStats` exposes
  hit/miss/insertion counters via lock-free atomics for tests,
  benchmarks, and observability.

### Changed

- `btrfs-disk`: `BlockReader::read_tree_block` now returns
  `Arc<TreeBlock>` instead of owning `TreeBlock`. Callers that
  pattern-matched `match &block { ... }` need `match &*block`;
  callers that consumed the block (e.g. `into_iter()`) borrow
  instead (`iter()`). Internal `walk_stats` switched to
  `&TreeBlock`. All ripple sites in `cli/`, `tune/`, `fs/`,
  `mkfs/`, `transaction/`, and the integration tests updated.

- `btrfs-fs`: multi-subvolume traversal. `Filesystem::lookup` now
  detects subvolume crossings (a `DirItem` whose `location.key_type`
  is `ROOT_ITEM`) and returns an `Inode` carrying the new subvol id
  and `objectid 256`. Reads, `readdir`, `readlink`, and xattr ops
  follow into the new subvolume's tree automatically. `..` from a
  non-default subvolume root resolves via `ROOT_BACKREF` in the
  root tree, returning the directory in the parent subvolume that
  contains the current one.
- `btrfs-fs`: `Filesystem::list_subvolumes() -> Vec<SubvolInfo>`
  walks the root tree and returns id, parent, name, ctime,
  generation, and read-only flag for every subvolume (default
  `FS_TREE` plus user subvolumes 256..LAST_FREE). System trees
  (CSUM, UUID, BLOCK_GROUP, etc.) are filtered out.
- `btrfs-fs`: `Filesystem::open_subvol(reader, SubvolId)` opens the
  filesystem with a non-default subvolume as the
  [`Filesystem::root`]. Validates the id is in the subvolume range
  (errors `InvalidInput` otherwise) and that the tree exists in the
  root tree (errors `NotFound` otherwise).
- `btrfs-fs`: `Filesystem::default_subvol() -> SubvolId` getter for
  embedders that need to know which subvolume `root()` points at.
- `btrfs-fuse`: `--subvol PATH` and `--subvolid ID` CLI flags on the
  `btrfs-fuse` binary mount the named subvolume as the FUSE root.
  `--subvol PATH` resolves to a [`SubvolId`] by walking each
  subvolume's parent chain to build a slash-separated full path,
  matched against the user's argument. The two flags are mutually
  exclusive; absent both, the default `FS_TREE` is used as before.
  `BtrfsFuse::open_subvol(file, SubvolId)` is the new library entry
  point.

- `btrfs-fs`: `SubvolInfo` is now `#[non_exhaustive]` and gained
  `dirid`, `otime`, `ctransid`, `otransid`, `uuid`, `parent_uuid`,
  and `received_uuid` fields — everything `BTRFS_IOC_GET_SUBVOL_INFO`
  needs. The default `FS_TREE` reports `dirid: 0` (no parent
  directory).
- `btrfs-fs`: `Filesystem::get_subvol_info(SubvolId)` returns
  metadata for a single subvolume (filtered `list_subvolumes`).
- `btrfs-fs`: `Filesystem::superblock() -> &Superblock` getter for
  embedders/ioctl handlers that need format-level fields.
  `Superblock` and `Uuid` are re-exported from the crate root.
- `btrfs-fuse`: F6.1 ioctl plumbing. Implements the
  `fuser::Filesystem::ioctl` callback and dispatches:
  `BTRFS_IOC_FS_INFO`, `BTRFS_IOC_GET_FEATURES`,
  `BTRFS_IOC_GET_SUBVOL_INFO`. Each FUSE_IOCTL request runs in a
  spawned tokio task that owns the `ReplyIoctl`, awaits the
  filesystem call, and serialises the response into the kernel's
  on-disk C struct layout (no bindgen types leak into the public
  API). Unknown ioctls return `ENOTTY`. `fuse/src/ioctl.rs` re-derives
  the ioctl numbers via const `_IOR` helpers since bindgen doesn't
  expand the macro family.
- `btrfs-fuse` now depends on a `fuser` git fork
  ([xfbs/fuser PR](https://github.com/xfbs/fuser), pinned to commit
  `37bfb7f`) carrying a `ReplyIoctl::retry(in_iovs, out_iovs)` API
  plus a new `arg: u64` parameter on the `Filesystem::ioctl`
  callback. This unblocks variable-size btrfs ioctls
  (`TREE_SEARCH_V2`, `LOGICAL_INO_V2`, `INO_PATHS`,
  `GET_SUBVOL_ROOTREF`) that exceed the 14-bit size field encoded
  in the ioctl number. The git dep means `btrfs-fuse` is marked
  `publish = false` until upstream merges and ships a release;
  the other workspace crates publish normally because none depend
  on `fuser`. `cargo deny`'s `allow-git` list explicitly permits
  the fork URL.
- `btrfs-fs`: `Filesystem::tree_search(filter, max_buf_size)`
  walks any subvolume tree (or the root tree, id 1) and returns
  matching items. Mirrors the kernel
  `BTRFS_IOC_TREE_SEARCH_V2` semantics: items are returned where
  the `(objectid, type, offset)` compound key falls in
  `[min, max]` and the leaf generation falls in `[min_transid,
  max_transid]`, capped by `max_items` and `max_buf_size`. New
  public types `SearchFilter` and `SearchItem`.
- `btrfs-fuse`: F6.3 lands `BTRFS_IOC_TREE_SEARCH` (v1, fixed
  4096-byte struct, no retry needed) and `BTRFS_IOC_TREE_SEARCH_V2`
  (variable-size, uses the new `ReplyIoctl::retry` API). Both
  share parsing/serialisation helpers; v2's two-call protocol
  reads `buf_size` from the initial 112-byte header, requests
  retry covering `arg..arg + 112 + buf_size`, then writes the
  populated key + items in the second call.
- `btrfs-fuse`: `BTRFS_IOC_GET_SUBVOL_ROOTREF` (fixed 4096-byte
  struct) on top of `Filesystem::tree_search`. Walks `ROOT_REF`
  entries in the root tree where `objectid == current_subvol` and
  `offset >= min_treeid`, emits up to 255 `(treeid, dirid)` pairs,
  and updates `min_treeid` to the next id past the last entry so
  callers can page through. Test verifies the multi-subvol fixture
  reports its single child.
- `btrfs-fs`: `Filesystem::ino_paths(subvol, objectid) -> Vec<Vec<u8>>`
  resolves every path that names the given inode within its
  subvolume — one entry per hardlink — by walking `INODE_REF` and
  `INODE_EXTREF` and joining each parent's `ino_lookup` result with
  the link name. Returns an empty vector for orphans.

### Known limitations

- Variable-size btrfs ioctls (`BTRFS_IOC_TREE_SEARCH_V2`,
  `BTRFS_IOC_INO_PATHS`, `BTRFS_IOC_LOGICAL_INO_V2`) cannot complete
  over our FUSE mount: Linux's `fuse_do_ioctl` only honours a
  `FUSE_IOCTL_RETRY` reply when the original request set
  `FUSE_IOCTL_UNRESTRICTED`, which standard `ioctl(2)` callers never
  do. The `TREE_SEARCH_V2` handler stays in-tree (it'd work for any
  caller that opts in), but `INO_PATHS` and `LOGICAL_INO_V2` aren't
  wired into dispatch since they would unconditionally fail. v1
  `TREE_SEARCH` (4096-byte fixed struct) is what the upstream
  `btrfs` CLI actually uses, so end-to-end coverage is intact for
  that path. See `fs/PLAN.md` § F6.3 for unblock options.
- `btrfs-fuse`: F6.2 (fixed-size subset). Two more ioctl handlers:
  `BTRFS_IOC_DEV_INFO` (per-device geometry) and
  `BTRFS_IOC_INO_LOOKUP` (objectid → path resolution by walking
  the `INODE_REF` chain in the inode's subvolume tree).
  `Filesystem::dev_info(devid)` and `Filesystem::ino_lookup(subvol,
  objectid)` are the new public entry points. `DeviceItem` is
  re-exported from `btrfs-fs`. Variable-size ioctls
  (`TREE_SEARCH_V2`, `LOGICAL_INO_V2`, `INO_PATHS`,
  `GET_SUBVOL_ROOTREF`) remain blocked on fuser 0.17 not exposing
  `FUSE_IOCTL_RETRY` in its reply API — these all carry struct
  buffers larger than the 14-bit size field encoded in the ioctl
  number, so without retry support FUSE silently truncates them.
  Will land after upstreaming the retry API to fuser, or by
  forking.

### Fixed

- `btrfs-fuse`: the FUSE root inode (`1`) now maps onto the
  filesystem's selected mount subvolume rather than always
  `SubvolId(5)`. Without this fix, `BtrfsFuse::open_subvol` would
  open the right subvolume internally but every FUSE callback would
  ignore it and serve content from `FS_TREE` regardless. Surfaced
  by the new `--subvol` mount tests.

### Fixed

- `btrfs-fs`: zstd-compressed extents on multi-chunk files now
  decompress correctly. Btrfs splits each compressed extent into
  independent 128 KiB frames; `zstd::bulk::decompress` rejects
  trailing bytes after the first frame. Switched to the streaming
  decoder (`zstd::stream::read::Decoder`) which handles concatenated
  frames and trailing sector padding. Surfaced by the F4 compression
  sweep — every zstd test on `pattern_16m.bin` was failing.
- `btrfs-fs`: inline compressed extents now read correctly.
  `inline_size` is the on-disk (compressed) payload length, but the
  read range math was using it as the logical extent length, so any
  read of a compressed inline extent returned a slice that was too
  short. Fixed to clamp against `ram_bytes` (the uncompressed length)
  instead. Surfaced by the F4 sweep on the inline file with all
  three algorithms.

## 0.12.0

### Removed

- `btrfs-mkfs`: legacy `--rootdir` walker and the helpers it called.
  All five `--rootdir` features (subvols, reflink, shrink,
  inode-flags, plus the simple case) now run exclusively through
  the transactional path landed in earlier commits, so the legacy
  body of `make_btrfs_with_rootdir` and its supporting machinery
  are dead code. Deleted in `mkfs.rs`: the entire post-validation
  body of `make_btrfs_with_rootdir` (now just delegates to
  `make_btrfs_with_rootdir_via_transaction`),
  `converge_extent_tree_block_count`, `write_rootdir_trees`,
  `metadata_extent_item`, `add_block_group_items`,
  `RootTreeRootdirArgs`, `patch_root_item_fs`,
  `build_root_tree_rootdir`, `build_free_space_tree_with_used`,
  `build_block_group_tree_with_used`, and the `UsedBytes` struct.
  Deleted in `rootdir.rs`: `walk_directory`, `walk_single_tree`,
  `write_file_data`, `RootdirPlan`, `SubvolPlan`, `SubvolMeta`,
  `FileAllocation`, `DataOutput`, `apply_nbytes_updates`,
  `fixup_inode_nlink`/`fixup_inode_size`/`fixup_inline_nbytes`,
  `patch_inode_field`, `try_compress_inline`,
  `try_compress_regular`, `lzo_compress_inline`,
  `lzo_compress_extent`, `max_inline_data_size`, plus
  `CompressConfig::extent_type_byte` / `is_enabled` and the LZO
  round-trip tests. Net: +31 / -2228 LOC across two files. Items
  serializers in `mkfs/src/items.rs` and the `tree.rs` /
  `treebuilder.rs` machinery are kept because they still serve
  the no-rootdir bootstrap path; deleting them needs a bootstrap
  migration first.

### Added

- `btrfs-mkfs`: `--rootdir --inode-flags` now goes through the new
  transactional walker. `walk_to_transaction` builds the same
  `path → (nodatacow, nodatasum)` map the legacy walker maintains,
  looks each entry up by its rootdir-relative path, and sets
  `InodeFlags::NODATACOW` / `InodeFlags::NODATASUM` on the
  `InodeArgs` passed to `Transaction::create_inode`. The same
  `nodatasum` value is plumbed through to `write_file_data` and
  the reflink path so per-sector csums are skipped on
  `NODATASUM` files. The dispatcher in `make_btrfs_with_rootdir`
  now unconditionally routes through the transactional path; the
  legacy walker is unreachable dead code, slated for deletion in
  the next commit. Verified by the existing
  `rootdir_inode_flags_nodatacow_nodatasum` test (now exercising
  the new path).
- `btrfs-mkfs`: `--rootdir --reflink` and `--rootdir --shrink` both
  go through the new transactional walker now. Together with the
  earlier subvol migration, this leaves only `--inode-flags` on the
  legacy path.

  `--shrink` (single-device only) computes the smallest size that
  still covers the on-disk chunk layout via `ChunkLayout::new`,
  patches `DEV_ITEM.total_bytes` and the superblock via the new
  `Transaction::set_device_total_bytes` helper before the
  transaction commits, then truncates the image after `fs.sync()`.
  Verified by the existing `mkfs_rootdir_shrink` integration test
  (now exercising the new path).

  `--reflink` adds `Transaction::reserve_data_extent` (allocate a
  data extent address + queue the `+1 EXTENT_DATA_REF` without
  writing any bytes — `alloc_data_extent` now wraps it). The walker
  opens a separate set of writeable device handles up front, then
  for each file chunk: reserves an extent, walks the chunk cache
  for per-stripe `(devid, physical)` placements, issues
  `FICLONERANGE` from the source file into each device handle,
  reads the cloned bytes back via `BlockReader::read_data` for
  CRC32C csumming, inserts the matching `EXTENT_DATA` and
  `EXTENT_CSUM` items, and bumps `INODE.nbytes`. RAID5/6 chunks
  are rejected (parity isn't recomputed by FICLONERANGE);
  compression is silently disabled when reflink is on (the recorded
  compression byte and the cloned source bytes wouldn't agree).
  New privileged test `mkfs_rootdir_reflink_on_btrfs` mounts a
  btrfs workspace, runs `mkfs --rootdir --reflink` with both
  source and destination inside it, and verifies the file reads
  back identical after mounting the destination.
- `btrfs-mkfs`: `--rootdir --subvol` now goes through the new
  transactional walker, in addition to the previously-migrated
  simple case. All four subvolume types (`rw` / `ro` / `default` /
  `default-ro`) and arbitrary nesting are supported.
  `walk_to_transaction` builds a path → `(subvol_id, type)` map from
  `subvol_args`, walks the main FS tree first, queues each
  subvolume boundary as a `DeferredSubvol`, and (after the main
  walk) processes the queue: `post_bootstrap::create_subvolume_shape`
  materialises the subvol tree (allocate leaf, populate inode 256
  with `INODE_ROOT_ITEM_INIT`, ".." `INODE_REF`, `ROOT_ITEM` patch
  with a fresh v4 UUID), `Transaction::insert_root_ref` records the
  parent linkage, the per-subvol walk emits the contents, and ro /
  default flags land via `set_root_readonly` / `set_default_subvol`.
  Nested subvols append back into the queue as the walker
  encounters them. The legacy path now only runs when `--reflink`,
  `--shrink`, or `--inode-flags` is requested. New helper
  `Transaction::link_subvol_entry` (sister of `link_dir_entry`)
  emits the parent's `DIR_ITEM` + `DIR_INDEX` pointing at a
  `ROOT_ITEM` location and bumps parent dir size, with no
  `INODE_REF` (subvol parent linkage uses `ROOT_REF` /
  `ROOT_BACKREF` in the root tree instead). Both `link_*` helpers
  now share a `mirror_root_item_size` helper for the inode-256 →
  `ROOT_ITEM` size mirroring. Verified by the existing unprivileged
  `subvol_*` mkfs tests (now exercising the new path) plus a new
  privileged `mkfs_rootdir_subvols` test that mounts the result and
  asserts `btrfs subvol list` listing, ro write rejection, and
  nested-subvol visibility.

  Pre-existing bug fixed in passing: `direct_subvol_boundaries` was
  given an absolute `parent_subvol_path` while `subvol_id_map` keys
  are relative to `rootdir`, so nested subvolume detection silently
  no-op'd. The new walker strips the rootdir prefix before the
  call. The legacy walker has the same gap but no test exercises
  it; left untouched here since it would require touching legacy
  code we're aiming to delete.
- `btrfs-mkfs`: `--rootdir` for the simple case (no `--subvol`, no
  `--reflink`, no `--shrink`, no `--inode-flags`) now goes through
  the transaction crate end-to-end. `make_btrfs_with_rootdir` calls
  `make_btrfs` for the empty-filesystem bootstrap (which already runs
  `post_bootstrap` to materialise the FS / csum / data-reloc / UUID
  trees), then opens the resulting image, starts a transaction, and
  drives a new `rootdir::walk_to_transaction` that walks the source
  directory depth-first emitting `INODE_ITEM` / `DIR_ITEM` /
  `DIR_INDEX` / `INODE_REF` / `XATTR_ITEM` and inline / regular
  `EXTENT_DATA` records via `Transaction::create_inode` /
  `link_dir_entry` / `set_xattr` / `insert_inline_extent` /
  `write_file_data`. Hardlinks (cross-parent) are coalesced and
  patched with `set_inode_nlink` after the walk completes. The legacy
  `walk_directory` + `TreeBuilder` + `pwrite` pipeline still runs
  when any of the four legacy-only features is requested. Single-
  device and multi-device (`Filesystem::open_multi`) paths both
  exercise the same walker. Verified by all four existing
  `mkfs_rootdir_*` integration tests plus a new
  `mkfs_rootdir_hardlinks_and_xattrs` test that mounts the result
  and asserts inode-number sharing, `nlink`, and xattr round-trip.
- `btrfs-transaction`: subvolume-creation helpers groundwork for the
  rootdir → transaction migration (Phase 3, Implementation Phase 1).
  Five new helpers, all built on existing `search_slot` /
  `insert_item` / `update_item` plumbing:
  `Transaction::set_inode_nlink(tree, ino, nlink)` patches the
  inode's `nlink` field in place (modelled on
  `update_inode_nbytes`). `Transaction::insert_root_ref(parent_root,
  child_root, dirid, dir_index, name)` inserts paired `ROOT_REF` +
  `ROOT_BACKREF` records into the root tree (id 1) using the new
  `RootRef::serialize` from `btrfs-disk`.
  `Transaction::set_root_readonly(tree_id)` ORs `RootItemFlags::RDONLY`
  into the existing `ROOT_ITEM`'s flags field.
  `Transaction::set_default_subvol(subvol_id)` upserts a `"default"`
  `DIR_ITEM` under `BTRFS_ROOT_TREE_DIR_OBJECTID` (overwriting
  mkfs's bootstrap default in place because the payload size is
  independent of `subvol_id`). Each helper has a privileged
  integration test against a fresh mkfs image; the readonly /
  default ones additionally pass `btrfs check` end-to-end.
- `btrfs-disk`: `RootRef::serialize(dirid, sequence, name)` mirrors
  the existing `parse`, producing the on-disk `btrfs_root_ref`
  byte sequence (18-byte fixed header plus the raw name).
- `btrfs-disk`: `chunk::ChunkTreeCache::plan_write` now routes RAID5
  and RAID6 chunks via a new `WritePlan::Parity(ParityPlan)` variant
  that names every data column slot of every touched physical row
  plus the rotating parity column(s). The executor (in `BlockReader`)
  prereads each data slot, overlays the caller's bytes, computes
  parity, then issues data + parity writes. `plan_read` likewise
  routes RAID5/6 reads to the data column owning each row's bytes,
  ignoring parity. Non-parity profiles are unchanged
  (`WritePlan::Plain`).
- `btrfs-disk`: new `raid56` module with `compute_p` (XOR) and
  `compute_p_q` (XOR + Reed-Solomon over GF(2^8) with reduction
  polynomial `x^8 + x^4 + x^3 + x^2 + 1`). Self-contained, no
  GPL-derived code; backed by exhaustive unit tests including
  reconstruction round-trips and hand-verified GF table values.
- `btrfs-transaction`: integration tests
  `multi_device_raid5_metadata_cow_round_trip` and
  `multi_device_raid6_metadata_cow_round_trip` exercise the new
  parity-aware write executor end-to-end (mkfs RAID5/6 image, COW
  insert, commit, reopen, verify item is reachable).
- `btrfs-transaction`: `convert::seed_free_space_tree(trans, fs_info)`
  helper. Walks every block group, derives free ranges from the
  extent tree, and inserts one `FREE_SPACE_INFO` plus one
  `FREE_SPACE_EXTENT` per range into tree id 10. Idempotent at the
  per-block-group level (skips block groups whose `FREE_SPACE_INFO`
  is already present), rejects pre-existing bitmap-layout entries.
  Extracted from `convert_to_free_space_tree` so mkfs's
  `post_bootstrap` can call it after creating the FST mid-transaction.
  `read_free_space_info` lifted to `pub(crate)` for the helper's
  idempotency probe.
- `btrfs-transaction`: `convert::create_block_group_tree(trans, fs_info)`
  helper. Snapshots `BLOCK_GROUP_ITEM` rows from the extent tree,
  pins the BG-tree-id routing override to the extent tree, creates
  tree id 11 if absent, moves each BG item from extent tree to BGT
  (skipping per-key when already migrated), and sets the
  `BLOCK_GROUP_TREE` compat_ro flag if not already set. Extracted
  from `convert_to_block_group_tree` so mkfs's `post_bootstrap` can
  call it directly to materialise BGT from a bootstrap that left
  BG items in the extent tree. Per-item idempotent so a partial
  conversion can be resumed.
- `btrfs-disk`: `ChunkProfile` enum and `ChunkTreeCache::plan_write` /
  `plan_read` for stripe-aware per-device routing across all RAID
  profiles except RAID5/RAID6. Multi-row writes (buffers larger than
  `stripe_len`) split into per-row segments automatically. Replaces
  `resolve_all` for the routing decision in `BlockReader::write_block`,
  `read_block`, and `read_data`.
- `btrfs-mkfs`: post-bootstrap transaction step. After the in-memory
  bootstrap layout is written to disk, mkfs reopens the image with
  `btrfs-transaction` and runs a single transaction that fills in
  the empty UUID tree (objectid 9). btrfs-progs creates this tree
  by default but our mkfs's hand-built bootstrap omitted it
  (PLAN B.3). The kernel populates UUID-tree entries lazily on
  snapshot/send, so an empty tree is the correct initial state.

  This is the first integration of the transaction crate into
  mkfs's write path. It's gated on a profile + feature allowlist
  (SINGLE/DUP/RAID1/RAID1C3/RAID1C4 metadata + data, FST enabled,
  CRC32C csum) — other configurations are skipped silently because
  the transaction crate doesn't yet handle them or has known
  compatibility gaps with mkfs's existing output. The skip list
  shrinks as the migration progresses; see `mkfs/PLAN.md`.
- `btrfs check`: `--backup`, `--tree-root`, `--chunk-root` flags for
  recovery from damaged root/chunk tree pointers.
- `btrfs-transaction`: structural invariant assertions throughout the
  crate. `debug_assert!` checks validate tree block structure (key
  ordering, data layout, bytenr consistency) at every modification
  point. Hard `assert!` checks guard against catastrophic errors
  (superblock corruption, generation inconsistency) even in release
  builds. Includes `ExtentBuffer::check_leaf()`, `check_node()`, and
  `check()` validation methods.
- `rescue chunk-recover`: raw device scan for surviving chunk-tree
  leaves, conflict resolution and chunk map reconstruction, and detailed
  text report. `--apply` writes the reconstructed chunk tree via the
  transaction crate.
- `btrfs-disk`: `BlockReader::new` constructor and `filesystem_open_with_cache`
  for opening filesystems with a pre-built chunk cache (used by chunk-recover).
- `btrfs-transaction`: `Filesystem::open_with_chunk_cache` and
  `Transaction::rebuild_chunk_tree` for chunk tree recovery.
- `btrfs-transaction`: data extent ref creation. `create_data_extent` inserts
  `EXTENT_ITEM` entries with inline `EXTENT_DATA_REF` backrefs through the
  delayed ref pipeline, with proper data block group accounting and free space
  tree updates. `ExtentItem::to_bytes_data()` serializer in btrfs-disk.
- `btrfs-transaction`: data extent write path foundation.
  `BlockGroupKind::Data` and `Transaction::alloc_data_extent` find space in
  a DATA block group, write the bytes immediately to all stripe copies, and
  queue a `+1` `EXTENT_DATA_REF` delayed ref. `Transaction::insert_file_extent`
  inserts an `EXTENT_DATA` item into an FS tree pointing at the allocated
  extent. `Transaction::insert_csums` computes per-sector standard CRC32C
  checksums and inserts `EXTENT_CSUM` items into the csum tree, splitting
  large payloads across multiple items so each fits in one leaf. Verified
  end-to-end against `btrfs check`.
- `btrfs-disk`: `FileExtentItem::to_bytes_regular` (53-byte regular/prealloc
  body) and `FileExtentItem::to_bytes_inline` (21-byte header + raw payload)
  serializers, plus `HEADER_SIZE` and `REGULAR_SIZE` constants.
- `btrfs-transaction`: high-level inode and directory-entry helpers
  to dedupe the boilerplate that mkfs migration would otherwise
  repeat per file. New `inode` module owns `InodeArgs` (full-fields
  counterpart to `btrfs_disk::items::InodeItemArgs` with `flags`,
  `rdev`, `sequence`, and four distinct timestamps).
  `Transaction::create_inode(tree, ino, args)` inserts an `INODE_ITEM`.
  `Transaction::link_dir_entry(tree, parent, child, name, ft,
  dir_index, time)` inserts `INODE_REF` + `DIR_ITEM` + `DIR_INDEX`,
  bumps the parent dir's `size`/`transid`/`ctime`/`mtime` in place,
  and (when the parent is the canonical subvolume root dir) mirrors
  the size into the `ROOT_ITEM`'s embedded inode.
  `Transaction::set_xattr(tree, ino, name, value)` inserts an
  `XATTR_ITEM`. The 6 existing end-to-end tests that built dir
  entries by hand collapsed from ~200 lines of boilerplate per test
  to ~15.
- `btrfs-disk` + `btrfs-transaction`: multi-device write support
  (transaction PLAN J.5). `BlockReader<R>` now stores a
  `BTreeMap<u64, R>` keyed by device id; `read_block` / `read_data`
  route by devid via the chunk cache, and `write_block` fans out to
  every stripe's correct device. New `filesystem_open_multi(devices)`
  and `Filesystem::open_multi(devices)` constructors take a
  `devid -> handle` map; the existing single-handle entry points are
  thin wrappers that read the superblock to learn the primary devid.
  Per-device `dev_item` snapshots are captured at open time and
  spliced into the appropriate device's superblock at commit so a
  multi-device filesystem doesn't get clobbered with the primary's
  identity. Bootstrap validates every chunk-tree-referenced devid is
  in the handle map. End-to-end coverage on a 2-device RAID1 image:
  open, COW transaction, data extent via `write_file_data`, and a
  missing-handle error case all pass `btrfs check`.

  API changes (pre-1.0):

  - `ChunkTreeCache::resolve` and `resolve_all` now return
    `(devid, physical)` instead of just `physical`.
  - `BlockReader::new` takes a `devid` argument; `BlockReader::new_multi`
    takes a `BTreeMap<u64, R>`.
  - `BlockReader::inner_mut` and `into_inner` removed in favour of
    `devices()` / `devices_mut()` (and `single_device_mut()` for
    offline tools that operate on one device at a time).
  - `OpenFilesystem` gained a `per_device_dev_items` field.
- `btrfs-transaction`: high-level data write helpers.
  `Transaction::update_inode_nbytes` patches an inode's `nbytes` field
  in place at the fixed struct offset, preserving all other fields
  (including `flags`, `rdev`, `sequence`). `Transaction::write_file_data`
  is the single entry point for writing file content: splits the input
  into ≤1 MiB chunks, allocates each as a regular data extent, inserts
  the `EXTENT_DATA` items, computes per-sector CRC32C csums (unless
  `nodatasum`), and bumps the inode's `nbytes`.
- `btrfs-transaction`: inline extent support.
  `Transaction::insert_inline_extent` embeds small payloads directly in
  the FS tree leaf as an inline `EXTENT_DATA` item with no extent-tree
  entry and no csum entries; `INODE.nbytes` is bumped by the unaligned
  payload length per the on-disk convention. `write_file_data` now
  picks inline automatically when `file_offset == 0` and
  `data.len() <= max_inline_data_size(sectorsize, nodesize)` (4095
  bytes on a default 16K nodesize / 4K sectorsize filesystem).
- `btrfs-transaction`: zlib + zstd compression for the data write path.
  New `try_compress(data, algorithm)` function returns the compressed
  bytes only when they shrink (callers fall back to raw otherwise).
  `Transaction::write_file_data` and `insert_inline_extent` gain a
  `compression: Option<CompressionType>` parameter. For regular extents
  each chunk is compressed independently with per-chunk fallback to
  raw; `disk_num_bytes` shrinks while `num_bytes`/`ram_bytes`/
  `INODE.nbytes` track the logical (sector-aligned) size. Csums always
  cover the on-disk (compressed) bytes.
- `btrfs-transaction`: LZO compression. `try_compress` now produces the
  inline LZO framing format (`[4B total_len LE] [4B seg_len LE] [lzo
  bytes]`). New `try_compress_regular(data, algorithm, sectorsize)`
  applies the per-sector regular framing (`[4B total_len LE] { [4B
  seg_len LE] [lzo bytes] [zero pad] }*`) with sector-boundary padding
  and the standard early-exit heuristic (abandon after 4 sectors if the
  framed buffer exceeds 3 sectors). `write_file_data` routes its
  per-chunk compression through `try_compress_regular` so LZO regular
  extents work end-to-end. Both inline and regular LZO files pass
  `btrfs check`.
- `btrfs-disk`: `ChunkItem::to_mapping` conversion method.
- `btrfs-fuse`: milestone M6 — library split and integration test harness.
  The crate is now a hybrid lib + bin: `fuse/src/lib.rs` exposes
  `BtrfsFuse` and its operation layer (`lookup_entry`, `get_attr`,
  `read_dir`, `read_symlink`, `read_data`, `list_xattrs`, `get_xattr`,
  `stat_fs`), each of which returns plain `io::Result` / `Option` values
  and is independent of `fuser`. The `fuser::Filesystem` trait impl is
  now a narrow adapter that calls these inherent methods and maps their
  results to `Reply*` calls. Integration tests in `fuse/tests/basic.rs`
  drive the operation layer directly without a FUSE mount, so they are
  unprivileged and run under plain `cargo test`. The tests build a fresh
  btrfs image per test process via `mkfs.btrfs --rootdir` over a known
  directory tree and cover lookup, getattr, readdir (including
  pagination and parent resolution for `..`), read_data (inline, empty,
  multi-extent, offset, past-EOF, nested), symlinks, xattrs, and statfs.
- `btrfs-fuse`: milestone M5 — `listxattr`, `getxattr`, and `statfs`.
  Xattrs are read from `XATTR_ITEM` entries (same `DirItem` wire format as
  directory entries; hash collisions handled by linear name scan). `statfs`
  reports `total_bytes` / `bytes_used` from the superblock in sectorsize
  blocks; inode counts are left as 0 for v1.
- `btrfs-fuse`: milestone M4 — full compression support. `read_file` and
  `read_symlink` now decompress zlib, zstd, and lzo extents (both inline
  and regular). LZO uses btrfs per-sector framing (4-byte total-size header
  + per-sector 4-byte segment headers, padded to `sectorsize` boundaries).
  Adds `flate2`, `zstd`, and `lzokay` dependencies to `btrfs-fuse`.
- `btrfs-fuse`: milestones M2 and M3 — `readlink`, `read` for inline and
  regular uncompressed extents, and correct `..` parent resolution via
  `INODE_REF`. Prealloc extents and sparse holes return zeros. Symlinks,
  hardlinks, and large multi-extent files now work end-to-end.
- New `btrfs-test-utils` crate at `util/testing/` consolidating the RAII
  test harness (`BackingFile`, `LoopbackDevice`, `Mount`) and shared data
  helpers (`write_test_data`, `verify_test_data`, `write_compressible_data`)
  plus `single_mount`, `deterministic_mount`, `cache_gzipped_image`, and
  `mount_existing_readonly`. Dev-dependency only, not published.

### Changed

- `btrfs-mkfs`: RAID5 and RAID6 now go through `post_bootstrap`. The
  `profile_supported` allowlist accepts every defined RAID profile,
  and the `bootstrap_creates_post_trees` fallback path in
  `make_btrfs` / `build_root_tree` / `build_extent_tree` is gone.
  mkfs's bootstrap now writes only the four always-present trees
  (Root, Extent, Chunk, Dev) for every profile and feature
  combination — every other tree (FS, csum, data-reloc, UUID, plus
  optional FST / BG tree / quota) is created by the post-bootstrap
  transaction. Three `build_*` helpers that only the fallback called
  (`build_empty_tree`, `build_root_dir_tree`, `build_quota_tree`)
  are deleted; the rest stay because the rootdir path still uses
  them. New regression tests
  `raid5_metadata_uuid_tree_created_by_post_bootstrap` and
  `raid6_metadata_uuid_tree_created_by_post_bootstrap` assert the
  UUID tree is at gen 2 (post-bootstrap) for both profiles, with a
  new `walk_root_tree_items_multi` helper for non-mirror metadata
  layouts.
- `btrfs-mkfs`: free-space-tree and block-group-tree creation move
  into the post-bootstrap transaction for supported profiles
  (SINGLE / DUP / RAID0 / RAID1* / RAID10), closing Phase 2 of the
  mkfs migration plan. mkfs's bootstrap leaves all `BLOCK_GROUP_ITEM`
  rows in the extent tree and skips creating the FST/BG-tree leaves
  themselves; post-bootstrap calls `convert::create_block_group_tree`
  (which migrates the items into BGT under the routing-override
  guard) and `convert::seed_free_space_tree` (which derives initial
  `FREE_SPACE_INFO` + `FREE_SPACE_EXTENT` items from the extent tree
  state). RAID5/RAID6 keeps mkfs's hand-built versions of both trees
  as the legacy fallback.
- `btrfs-mkfs`: `--features ^free-space-tree` now also clears the
  `BLOCK_GROUP_TREE` `compat_ro` bit (kernel requires FST for BGT).
  Previously these two flags could disagree if the user disabled
  only FST, producing an image that the kernel rejects.
- `btrfs-mkfs`: quota tree creation (`-O quota` / `-O squota`) moves
  into the post-bootstrap transaction for supported profiles. Uses a
  new `insert_raw_item` helper to write the three qgroup items
  (STATUS + INFO + LIMIT) for the FS tree's qgroupid (0/5).
  Distinguishes regular quota (INCONSISTENT flag, zero info — kernel
  will rescan) from squota (SIMPLE_MODE flag, info pre-populated
  with FS tree usage). RAID5/RAID6 falls back to mkfs-built quota.
  `MkfsConfig::now_secs` is now `pub(crate)` and the post-bootstrap
  `apply_in_transaction` takes the full `MkfsConfig` so future
  feature-gated trees can read their settings the same way.
- `btrfs-mkfs`: FS tree creation moves into the post-bootstrap
  transaction for the no-rootdir path with supported profiles
  (SINGLE / DUP / RAID0 / RAID1* / RAID10). Same pattern as the
  data-reloc tree, plus FS-tree-specific `ROOT_ITEM` patches: the
  embedded inode flags get `BTRFS_INODE_ROOT_ITEM_INIT`, the
  `ROOT_ITEM` `uuid` is derived from the fsid by bit-flipping
  (matches btrfs-progs convention), and ctime/otime track
  `cfg.now_secs()`. Verified by mounting a default-profile image,
  writing a file, and reading it back. RAID5/RAID6 and the rootdir
  path still build the FS tree directly.
- `btrfs-mkfs`: data-reloc tree creation moves into the post-bootstrap
  transaction for profile/feature combinations that go through it
  (SINGLE / DUP / RAID0 / RAID1* / RAID10). Same pattern as the csum
  tree migration — `make_btrfs` skips it at bootstrap time, post-
  bootstrap creates the empty tree, inserts the root dir `INODE_ITEM`
  via `Transaction::create_inode`, inserts a `(256, INODE_REF, 256)`
  ".." self-reference, and patches the `ROOT_ITEM` to mark it as a
  subvolume-shaped tree (`root_dirid = 256`, embedded `inode_data`
  mirrors the standalone inode). The rootdir path and RAID5/RAID6
  keep mkfs's hand-built tree as before.
- `btrfs-mkfs`: csum tree creation moves into the post-bootstrap
  transaction for profile/feature combinations that go through it
  (SINGLE / DUP / RAID0 / RAID1* / RAID10). The `make_btrfs` no-rootdir
  path skips the csum tree at bootstrap time; `post_bootstrap`
  idempotently creates it via `Transaction::create_empty_tree`. The
  rootdir path keeps creating the csum tree directly because it has
  csum items to insert at bootstrap time. RAID5/RAID6 (post-bootstrap
  not supported) keeps mkfs's hand-built csum tree as a fallback.
  First Phase 2 step from `mkfs/PLAN.md` — establishes the pattern
  for migrating other always-present trees out of mkfs's bootstrap.
- `btrfs-disk`: `csum_tree_block` and `csum_superblock` now dispatch on
  `ChecksumType` (CRC32C, xxhash64, SHA-256, BLAKE2b). The transaction
  crate's `ExtentBuffer::update_checksum` and the per-commit flush path
  pull the algorithm from `Filesystem::superblock.csum_type`, so commits
  on non-CRC32C filesystems now write the correct hash. `xxhash-rust`,
  `sha2`, and `blake2` move from `btrfs-mkfs` into `btrfs-disk`.
- `btrfs-mkfs`: `ChecksumType` is now a re-export of the disk-crate
  enum; `write::fill_csum` is gone (callers use
  `btrfs_disk::util::csum_tree_block` and
  `btrfs_disk::superblock::csum_superblock` directly). The
  post-bootstrap transaction is no longer gated on CRC32C, so xxhash /
  SHA-256 / BLAKE2b images now get a UUID tree like CRC32C ones do.
- `cli`, `uapi`, and `stream` integration tests now pull their mount /
  loopback / backing-file helpers from `btrfs-test-utils` instead of
  maintaining per-crate copies. Each crate's `tests/common.rs` is now a
  thin re-export plus crate-local glue for fixture image paths. The
  duplicate `BackingFile::mkfs_rootdir` no longer embeds a hard-coded
  `env!("CARGO_BIN_EXE_btrfs")` lookup; callers pass the `btrfs-mkfs`
  binary path explicitly.

### Fixed

- `btrfs-mkfs`: `--features ^free-space-tree` no longer writes a stale
  empty FST leaf or its `ROOT_ITEM`. The free-space tree is now an
  optional slot in `BlockLayout` (alongside block-group and quota
  trees) — gated on `cfg.has_free_space_tree()` everywhere instead of
  having a hardcoded slot. Saves one `nodesize`-sized tree block per
  `^free-space-tree` image and aligns the on-disk layout with what
  btrfs-progs `mkfs.btrfs` produces.
- `btrfs-transaction` write path now routes correctly on RAID0 / RAID10
  filesystems. Previously every commit went through `resolve_all` which
  fans out to all stripes — correct for DUP/RAID1*, but for RAID0 it
  duplicated the same row to every device (corruption) and for RAID10
  it wrote to every mirror pair instead of just the row's pair. Surfaces
  in mkfs's `post_bootstrap` UUID-tree creation: RAID0/RAID10 images are
  no longer skipped by the profile allowlist.
- `btrfs-transaction`: `update_free_space_tree` now respects the
  `FREE_SPACE_TREE` compat_ro flag — when the flag is cleared, the
  FST update is skipped even if a tree-id-10 root is present on disk.
  Lets `btrfs-mkfs --features ^free-space-tree` run through
  `post_bootstrap` cleanly (mkfs leaves a stale FST leaf around in
  that case; the kernel ignores it because the flag is cleared).
- `btrfs-transaction`: `DelayedRefQueue` now uses `BTreeMap` (was
  `HashMap`), so `flush_delayed_refs` iterates queued refs in
  deterministic key order. Without this, successive transaction
  commits over the same input could produce byte-different output
  due to hash randomization, surfacing as flaky snapshot tests once
  the mkfs migration started piping commit output through snapshot
  comparison. `DelayedRefKey` gained `PartialOrd, Ord` derives.
- `mkfs --rootdir`: `EXTENT_DATA` items for files whose size is not a
  sectorsize multiple now use `align_up(extent_size, sectorsize)` for
  `num_bytes` and `ram_bytes`, and accumulate the same value into the
  inode's `nbytes`. Previously the unaligned `extent_size` was passed
  directly, producing images that fail `btrfs check` with
  `bad file extent, nbytes wrong`. The bug also affected compressed
  extents where `aligned_disk != aligned_logical`.
- `btrfs-transaction`: fix fall-through bug in `flush_delayed_refs`
  where the data ref drop path executed unconditionally after the
  add path. Previously masked by `todo!()` in the add branch.

## 0.11.0

### Added

- `btrfs-fuse` (experimental): new MIT/Apache-2.0 crate providing a
  userspace FUSE driver built on `btrfs-disk`. M1 sketch implements
  `lookup`, `getattr`, and `readdir` against the default FS tree of an
  unmounted image or block device. File reads, xattrs, multi-subvolume
  support, and key-based tree descent are tracked as follow-ups.

- `btrfs-transaction`: full-tree conversion primitives. Lands
  four new transaction-crate building blocks plus two whole-tree
  conversions:

  - **`Transaction::create_empty_tree(fs_info, tree_id)`**:
    allocate one metadata block, initialise it as an empty level-0
    leaf with the inherited fsid + chunk_tree_uuid, register the new
    `(tree_id -> bytenr)` mapping in the in-memory roots map, and
    insert a `ROOT_ITEM` into the root tree. Rejects bootstrap ids
    `0..=3` and any tree id already present.
  - **`extent_walk` module**: read-only
    `walk_block_group_extents` callback-style scanner over
    `EXTENT_ITEM` and `METADATA_ITEM` keys within a block group's
    logical range, plus `derive_free_ranges` for computing
    complementary free ranges. Detects overlap and out-of-bounds
    extents.
  - **`convert::convert_to_free_space_tree`**: single-
    transaction path that creates the FST root, walks every block
    group, derives free ranges, inserts `FREE_SPACE_INFO` and
    `FREE_SPACE_EXTENT` items, sets the `FREE_SPACE_TREE` +
    `FREE_SPACE_TREE_VALID` `compat_ro` bits, and zeros
    `cache_generation`. Simple-case only.
  - **`Filesystem::block_group_tree_id` + `bg_tree_override`**: 
    single accessor for routing block-group reads,
    replacing the duplicated inline routing in
    `allocation::load_block_groups` and
    `Transaction::update_block_group_used`. The override pin is the
    load-bearing primitive that lets the BGT conversion populate the
    new tree without the allocator routing to it mid-flight.
  - **`convert::convert_to_block_group_tree`**:
    single-transaction path that creates the BGT root via the
    primitive, copies every `BLOCK_GROUP_ITEM` from the
    extent tree into BGT, deletes the originals from the extent
    tree, and sets the `BLOCK_GROUP_TREE` `compat_ro` bit. The
    `bg_tree_override` is held on tree id 2 for the duration of the
    function so allocator calls keep reading from the extent tree.
    Lifts the initial single-leaf cap: the conversion now handles
    block group counts that span multiple BGT leaves.

- `btrfs-tune --convert-to-block-group-tree`: convert an unmounted
  filesystem to use the block group tree (`BLOCK_GROUP_TREE`
  compat_ro feature). Wraps the transaction crate's
  `convert_to_block_group_tree` and commits in a
  single transaction. Requires the free space tree to be enabled
  first (kernel invariant). Can be combined with
  `--convert-to-free-space-tree` in one invocation; both
  conversions then run in sequence. Privileged integration tests
  cover the basic conversion (verified with `btrfs check` and
  `dump-super`) and the FST-required precondition.

- `btrfs-tune --convert-to-free-space-tree`: convert an unmounted
  filesystem to use the v2 free space tree (`FREE_SPACE_TREE`
  compat_ro feature). Wraps the transaction crate's
  `convert_to_free_space_tree` and commits in a single
  transaction. Simple-case only: refuses if FST is already enabled,
  if a stale FST root is present, or if any v1 free-space-cache
  items remain in the root tree (clear them with
  `btrfs rescue clear-space-cache` first). Privileged integration
  test runs the conversion against a `^free-space-tree` image and
  verifies `btrfs check` plus the resulting `compat_ro` bits.

- `btrfs filesystem resize --offline`: resize an unmounted single-device
  btrfs image or block device in place. Grow-only (shrinking is
  rejected). Updates the `DEV_ITEM` in the chunk tree and
  `superblock.total_bytes` inside a transaction, then extends the
  backing file for regular-file images. Honors `--dry-run`. Accepts
  `max` (regular files only), absolute sizes, and `+<amount>` deltas;
  `cancel`, shrinks, and multi-device filesystems are rejected with
  clear errors. Privileged integration tests cover the grow path
  (with `btrfs check`) and shrink rejection.

## 0.10.0

### Added

- `btrfs-transaction`: data extent ref drop support. The delayed-ref
  queue keys metadata vs. data backrefs separately, the flush path
  locates `EXTENT_DATA_REF` records inline or as standalone items,
  decrements `EXTENT_ITEM.refs`, deletes fully-freed extents, and
  trims overlapping `EXTENT_CSUM` items (with full head/tail/split
  handling). New `btrfs-disk` helpers: pub `extent_data_ref_hash`,
  `inline_ref_size`. New `transaction::items::shrink_item`.
- `btrfs-transaction`: chunk tree COW support. The allocator now
  distinguishes `BlockGroupKind::Metadata` from `BlockGroupKind::System`
  with separate per-kind cursors, `alloc_tree_block` routes the
  chunk tree to a SYSTEM block group, and `ensure_in_sys_chunk_array`
  registers freshly-allocated SYSTEM chunks in the superblock
  bootstrap snippet. New `btrfs-disk` helpers: `chunk_item_bytes`
  serializer, `sys_chunk_array_contains`, `sys_chunk_array_append`.
- `btrfs rescue clear-ino-cache`: walks every fs tree, finds items
  keyed under `BTRFS_FREE_INO_OBJECTID` (and historically also
  `BTRFS_FREE_SPACE_OBJECTID`, used by old kernels for the per-inode
  bitmap), drops every referenced data extent via the data-ref
  delayed-ref path, and deletes the items. Each subvolume's worth of
  cleanup commits in its own transaction. Privileged integration
  test runs the no-op path against a fresh filesystem with multiple
  subvolumes (the `inode_cache` mount option has been removed from
  the kernel, so a real test fixture is impossible to construct on
  modern systems).
- `btrfs rescue clear-space-cache --version v1`: for every block
  group, deletes the `FREE_SPACE_HEADER` from the root tree, walks
  the cache inode's `EXTENT_DATA` items dropping each referenced
  data extent, deletes the `EXTENT_DATA` items and the
  `INODE_ITEM`, and bumps the superblock `cache_generation` to
  invalidate the cache. Privileged integration test exercises the
  full rw remount round-trip with `space_cache=v1`.
- `btrfs rescue clear-space-cache --version v2`: walks the
  FREE_SPACE_TREE, drops every block, removes its ROOT_ITEM, clears
  the `FREE_SPACE_TREE` + `FREE_SPACE_TREE_VALID` compat_ro flags,
  and commits. The kernel rebuilds the tree on the next mount.
  Refuses to run on filesystems with `BLOCK_GROUP_TREE` enabled
  (BGT requires FST). Privileged integration test exercises the
  full rw remount round-trip.
- `btrfs rescue fix-data-checksum`: walks the csum tree, recomputes
  CRC32C for every covered sector, and reports mismatches in
  `--readonly` mode (default). With `--mirror 1`, mismatched csum
  bytes are rewritten in place via `item_data_mut` and committed in
  a single transaction. Multi-mirror reads, `--interactive`, and
  csum types other than CRC32C are not yet supported. Tests cover
  both the clean-fs scan and a corrupt-and-repair round trip.
- `btrfs rescue fix-device-size`: walks DEV_ITEMs, rounds down
  misaligned `total_bytes`, shrinks past the actual device size
  when no DEV_EXTENT would be lost, mirrors the change into the
  embedded superblock dev_item, and recomputes superblock
  `total_bytes`. Privileged integration test exercises the
  shrink case end-to-end and verifies via `btrfs check`.
- `btrfs rescue clear-uuid-tree`: walks the UUID tree, drops extent refs
  for every block, deletes the ROOT_ITEM, and commits. End-to-end test
  round-trips through a rw mount.
- transaction: free space tree update during commit. Every
  commit now leaves `FREE_SPACE_EXTENT` and `FREE_SPACE_INFO` items
  consistent with the extent tree, so rw mounts no longer hang on
  stale FST entries. Bitmap-layout block groups are detected and
  rejected with a clear error.

### Changed

- transaction: convergence loop is now `flush_delayed_refs →
  update_root_items → snapshot_roots → update_free_space_tree`,
  with the snapshot taken before the FST update so the next pass's
  `update_root_items` picks up FST root changes. Pass cap raised
  from 16 to 32.
- transaction tests: every `btrfs check` helper now runs in strict
  mode; the previous `free space`/`cache` filters are gone. The
  proptest harness at `PROPTEST_CASES=1000` passes with strict
  check.

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
