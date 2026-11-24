# btrfs-fs: Userspace btrfs Filesystem Crate

## Goal

A high-level Rust filesystem API on top of `btrfs-disk` (read) and
`btrfs-transaction` (write), exposed as `Filesystem<R>`. It provides the
operations a userspace driver needs (`lookup`, `readdir`, `read`, `write`,
`getattr`, `xattr_*`) plus btrfs-specific operations such as subvolume
creation and send stream generation through ioctl passthrough.

The crate is the base for `btrfs-fuse` (the FUSE driver) and for any other
embedder that reads or writes a btrfs filesystem without the kernel:

- offline tools
- tests
- alternate FUSE bindings
- network-mounted images

## Design principles

- **FUSE-independent.** The crate exposes plain `io::Result` and
  `Filesystem` ops. Nothing depends on `fuser`. The FUSE protocol mapping
  (inode translation, `Stat` → `FileAttr`, `reply.*`) lives in
  `btrfs-fuse`. New embedders depend on `btrfs-fs` directly.

- **Async API from F2 onwards.** All ops are `async fn`. Sync internals run
  in `tokio::task::spawn_blocking` until there is an async I/O backend. The
  `Filesystem` handle is `Clone` (a cheap `Arc` bump), so several worker
  tasks can drive it. The FUSE adapter spawns a tokio task per callback,
  moves the `Reply*` handle in, awaits the async op, and replies from the
  task. No FUSE worker thread blocks on disk I/O.

- **Single source of truth: `btrfs-disk` for reads, `btrfs-transaction`
  for writes.** This layer does not reimplement parsing or write-path
  logic. `btrfs-fs` composes the lower-level primitives into
  filesystem-level operations and adds caching, dirty tracking, and
  multi-subvolume bookkeeping.

- **Inode is `(SubvolId, ino)`.** Multiple subvolumes are the default
  model from the start, even before F5 implements crossing. FUSE adapters
  translate to a flat `u64` at the boundary.

- **Cache hits go lock-free.** `Filesystem` operations use
  `RwLock<LruCache<...>>` for shared read-side caches. Cache misses fall
  back to a serialized I/O path. The split is internal: the `&self` API
  stays the same as the cache layer evolves.

- **Happy path only.** No degraded RAID mounts and no partial-recovery
  modes. If `btrfs-disk` can open the filesystem, `btrfs-fs` operates on
  it. Otherwise it returns an error. Recovery tooling lives in `btrfs cli`
  (e.g. `btrfs rescue`).

- **Correctness over performance,** especially for the write path. The
  acceptance test for write phases is cross-validation with kernel btrfs:
  `btrfs check` after a FUSE session, and a kernel mount after FUSE
  modifications.

- **Tests at every phase.** Unit tests for pure logic, integration tests
  against `mkfs.btrfs --rootdir` fixture images (unprivileged), and from
  F12 onward cross-validation against kernel btrfs.

## Existing infrastructure

From `btrfs-disk`:
- `BlockReader<R>` with `read_data`, `read_tree_block`
- `filesystem_open()` → `OpenFilesystem` (superblock, chunk cache, root map)
- `TreeBlock` (Node and Leaf), `Header`, `Item`, `DiskKey`
- `tree_walk()` and `tree_walk_tolerant()` with visitor callbacks
- All on-disk item parsers (`InodeItem`, `DirItem`, `FileExtentItem`, ...)
- `Superblock` parsing
- `ChunkTreeCache` for logical→physical resolution

From `btrfs-transaction` (used from F9 onward):
- `Transaction` with `commit()`, B-tree CoW, delayed refs
- High-level helpers: `create_inode`, `link_dir_entry`, `set_xattr`,
  `write_file_data`, `insert_inline_extent`, `set_root_readonly`,
  `set_default_subvol`, `insert_root_ref`, `reserve_data_extent`
- `Filesystem::create_subvolume_shape` for new subvolume bootstrap
- Free space tree, block group accounting, csum tree updates

From `btrfs-stream`:
- `StreamReader` and TLV command and attribute encoding (used in F7 send)

## Crate structure (target)

```
fs/
  Cargo.toml          # btrfs-fs, MIT/Apache-2.0
  src/
    lib.rs            # public API re-exports
    filesystem.rs     # Filesystem<R>, Inner<R>, ops
    inode.rs          # Inode, SubvolId types
    dir.rs            # Entry, FileKind
    stat.rs           # Stat
    read.rs           # extent resolution, decompression
    xattr.rs          # xattr enumeration / lookup
    cache/            # tree-block, inode, extent-map caches (F3)
    subvol.rs         # multi-subvol traversal (F5)
    ioctl/            # FUSE_IOCTL decode + dispatch (F6, F11)
    send.rs           # send stream generation (F7)
    write/            # write-path operations (F9-F11)
      mod.rs
      tx.rs           # TxnHandle wrapping btrfs-transaction
      ops.rs          # POSIX ops: create, write, truncate, ...
  tests/
    basic.rs          # F1: read-path integration tests
    compression.rs    # F4: zlib/zstd/LZO sweep
    multisubvol.rs    # F5: subvol traversal
    ioctl.rs          # F6/F11: ioctl behavior
    send.rs           # F7: send stream round-trips
    write.rs          # F10: POSIX write ops
    durability.rs     # F12: cross-validation with kernel mount
```

## Phases

Each phase ends with a green test suite and a single commit. Tests land
with the feature, not after.

### F1: Crate extraction ✅

Done. `btrfs-fs` was carved out of `btrfs-fuse`. `Filesystem<R>` exposes
read ops, 19 read-path integration tests pass, and `btrfs-fuse` is now a
thin adapter.

### F1.5: `&self` and `Arc<Inner>` handle ✅

Done. `Filesystem<R: Read + Seek + Send>` is `Clone` (a cheap `Arc` bump).
All ops take `&self`. The FUSE adapter no longer needs its outer Mutex. A
compile-time `Send + Sync` assertion and a multithread test cover this.

### F2: Async refactor ✅

Done. All `Filesystem<R>` ops are `async fn`. Sync I/O runs in
`tokio::task::spawn_blocking`. The sync `Mutex` is held only inside the
blocking task, never across `.await`. Bound: `R: Read + Seek + Send +
'static`.

`btrfs-fuse` carries an internal multi-thread tokio runtime. Each FUSE
callback spawns a task that owns the `Reply*`, awaits the async op, and
replies from the task. FUSE worker threads return immediately.

All 19 read-path tests run under `#[tokio::test]`. The
`concurrent_async_reads` test spawns one task per fixture entry on a
4-worker multi-thread runtime and checks that parallel
`lookup → read → getattr` chains all complete correctly. The compile-time
`Send + Sync` assertion is still in place.

Native async I/O is out of scope. It is deferred to F8 if profiling
justifies it.

### F3: Caches ✅

Done. Three caches sit on the read path:

- `LruTreeBlockCache`: `Mutex<LruCache<u64, Arc<TreeBlock>>>` keyed by
  logical address, plugged into `BlockReader` via the `TreeBlockCache`
  trait added to `btrfs-disk`. Default 4096 entries (~64 MiB).
- `InodeCache`: `Mutex<LruCache<Inode, Arc<InodeItem>>>`, populated on
  `lookup`, `getattr`, `read_inode_item`, and `readlink`. Default 4096
  entries.
- `ExtentMapCache`: `Mutex<LruCache<Inode, Arc<ExtentMap>>>`, built lazily
  on the first `read` of a file. Later reads skip the FS tree walk
  entirely. Default 1024 entries.

The caches use `Mutex` rather than `RwLock` because every access mutates
the LRU order. Even a read needs exclusive access to the cache structure.

`Filesystem::tree_block_cache_stats() -> CacheStats` exposes lock-free
atomic hit, miss, and insertion counters for tests and observability.

The trait and invalidation methods are wired up. The generation counter
for invalidation on transaction commit is deferred to F9, since there is
no write path yet.

Deferred: a persistent cache (across `Filesystem` instances) and
benchmarks under `fs/benches/`. The two-test cache suite
(`fs/tests/cache.rs`) checks effectiveness through the stats API rather
than through timing.

### F4: Compression test sweep ✅

Done. `fs/tests/compression.rs` has 33 tests: 11 per algorithm across 3
algorithms, generated by the `compression_suite!` macro. Each algorithm's
fixture is built once via `mkfs.btrfs --rootdir --compress <algo>` and
shared across the suite via `OnceLock`.

Coverage per algorithm:
- inline compressed extent (full read, and partial read with offset)
- regular compressed extent on highly compressible 1 MiB of zeros (full
  read, and partial read at an offset inside a 128 KiB chunk)
- regular extent on 1 MiB of pseudo-random bytes. This is incompressible
  and exercises the path where the compress flag is set but the extent
  says None.
- 16 MiB multi-extent file with a per-MiB byte pattern: full read, a read
  that straddles both an extent boundary and a 128 KiB compression-chunk
  boundary, and a last-byte read
- read at or past EOF returns empty

Bugs the sweep caught:
- zstd: `bulk::decompress` rejects trailing bytes after the first zstd
  frame, so multi-frame compressed extents (anything over 128 KiB
  uncompressed) failed. Switched to the streaming decoder.
- Inline compressed extents: the read range was clamped against
  `inline_size` (the on-disk compressed length) rather than `ram_bytes`
  (the logical length), so reads of a compressed inline extent returned a
  slice that was too short. Fixed.

LZO had no new bugs. `decompress_lzo` handled all its per-sector framing
edge cases correctly.

### F5: Multi-subvolume traversal ✅

Done. `Filesystem::lookup` detects subvolume crossings (a DirItem with
`location.key_type == ROOT_ITEM`) and returns an `Inode` carrying the new
subvol id and objectid 256. Reads, readdir, readlink, and xattr ops follow
into the new subvolume's tree via `tree_root_for(subvol)`. The synthesized
`..` at a subvolume root resolves via `ROOT_BACKREF` in the root tree.

`Filesystem::list_subvolumes() -> Vec<SubvolInfo>` walks the root tree and
returns the id, parent, name, ctime, generation, and read-only flag of
every subvolume. System trees are filtered out by `is_subvolume_id`
(id == 5 OR 256 ≤ id ≤ u64::MAX - 256).

`Filesystem::open_subvol(reader, SubvolId)` opens with a non-default
subvolume as `root()`. `Filesystem::default_subvol() -> SubvolId` exposes
the choice.

Fixture: `mkfs.btrfs --rootdir --subvol sub1 --subvol sub1/nested
--subvol sub2`. mkfs does not assign ids in argument order, so tests
resolve names to ids through `list_subvolumes`. 9 tests cover:

- lookup crossing
- nested subvolume crossing
- readdir of a subvolume root
- `..` resolving via ROOT_BACKREF to FS_TREE
- `..` resolving via ROOT_BACKREF to a non-default parent subvolume
- the shape of `list_subvolumes` output
- `open_subvol` happy path
- `open_subvol` with an unknown id (NotFound)
- `open_subvol` with an invalid id (InvalidInput)

Follow-up (landed): `btrfs-fuse` accepts `--subvol PATH` and
`--subvolid ID`, which are mutually exclusive. `--subvol` matches the path
against each subvolume's full parent-chain path. `--subvolid` takes the
tree id directly. `BtrfsFuse::open_subvol` is the matching library entry
point. The FUSE adapter gained its `mount_subvol` field at the same time,
so the FUSE root inode (`1`) maps to whichever subvolume the `Filesystem`
was opened with, not always `SubvolId(5)`.

### F6.1: Read-only ioctls (fixed-size) ✅

Done. `FUSE_IOCTL` plumbing landed in `btrfs-fuse`, along with three
fixed-size read-only ioctls:

- `BTRFS_IOC_FS_INFO`: superblock geometry, UUIDs, csum type
- `BTRFS_IOC_GET_FEATURES`: compat, compat_ro, and incompat words
- `BTRFS_IOC_GET_SUBVOL_INFO`: full subvolume metadata

`btrfs-fs` added the `Filesystem::superblock()` and
`Filesystem::get_subvol_info(SubvolId)` accessors. `SubvolInfo` gained
`dirid`, `uuid`, `parent_uuid`, `received_uuid`, `otime`, and the transids,
and is marked `#[non_exhaustive]`.

`fuse/src/ioctl.rs` derives the kernel ioctl numbers with const `_IOR`
helpers, because bindgen doesn't expand that macro family. It serializes
responses into the C struct layout without leaking `btrfs_disk::raw`
types into the public API.

5 new tests in `fuse/tests/ioctl.rs`:
- 3 tests driven by `libc::ioctl`, one per ioctl
- 1 test checking that an unknown ioctl returns `ENOTTY`
- 1 CLI end-to-end test that runs our `btrfs subvolume show` against the
  FUSE mount. It exercises `BTRFS_IOC_GET_SUBVOL_INFO` through real CLI
  code.

### F6.2: Read-only ioctls (fixed-size subset) ✅

Done. Two more ioctls landed on top of F6.1:

- `BTRFS_IOC_DEV_INFO`: per-device geometry. Returns the primary device's
  `dev_item` from the superblock. Multi-device images need a dev tree walk
  (deferred). An unknown devid returns ENODEV.
- `BTRFS_IOC_INO_LOOKUP`: `(treeid, objectid)` → path within the
  subvolume. Walks the `INODE_REF` chain up from `objectid` to the
  subvolume root. A 4096-iteration bound guards against corrupted ref
  cycles. `treeid == 0` resolves against the file's own subvolume.

`btrfs-fs` gained `Filesystem::dev_info(devid)`,
`Filesystem::ino_lookup(subvol, objectid)`, and a re-export of
`DeviceItem`.

End-to-end CLI test: `btrfs inspect-internal rootid <mount>` uses
`lookup_path_rootid`, which calls `BTRFS_IOC_INO_LOOKUP` with
objectid=`BTRFS_FIRST_FREE_OBJECTID`. It now succeeds against our FUSE
mount and returns the default subvol id 5.

### F6.3: Variable-size ioctls ✅ (with kernel-imposed scope limit)

`BTRFS_IOC_TREE_SEARCH` (v1, fixed-size 4096) is implemented. The
upstream `btrfs` CLI uses it for `subvolume list`, so this gives a working
end-to-end path. `Filesystem::tree_search(filter, max_buf_size)` in
`btrfs-fs` does the tree walk with compound-key range filtering, matching
kernel semantics.

`BTRFS_IOC_GET_SUBVOL_ROOTREF` (fixed 4096) is implemented on top of
`tree_search` against the root tree. It pages through children in
255-entry batches via `min_treeid`, like the kernel ioctl.
`btrfs-fs` exposes `Filesystem::ino_paths(subvol, objectid) ->
Vec<Vec<u8>>` for embedders that want every hardlink path.

`BTRFS_IOC_TREE_SEARCH_V2` has a handler that returns
`IoctlOutcome::Retry`, but it cannot complete in practice because of the
FUSE_IOCTL_RETRY restriction below. The same applies to
`BTRFS_IOC_INO_PATHS` and `BTRFS_IOC_LOGICAL_INO_V2`. They are not wired
into dispatch, since the retry round trip can't happen from a normal libc
`ioctl(2)` caller.

**FUSE_IOCTL_RETRY restriction.** Linux's `fuse_do_ioctl` accepts a
`FUSE_IOCTL_RETRY` reply only when the original request had
`FUSE_IOCTL_UNRESTRICTED` set. The standard `fuse_file_ioctl` and
`fuse_dir_ioctl` paths do not set that flag. Userspace ioctls that reach
us through libc are rejected with `-EIO` after the first retry response,
and the kernel never reissues the call. This was confirmed locally and
matches the `xfbs/fuser` PR review. As a result, every variable-size btrfs
ioctl that needs retry to go past the 14-bit size encoded in the command
is blocked at the kernel boundary.

Options for unblocking it at the kernel layer:
1. Get the kernel to relax the restriction (unlikely, for security
   reasons).
2. Have the FUSE driver implement a CUSE-style init that opts the fd into
   `FUSE_IOCTL_UNRESTRICTED`. This requires CUSE_INIT plumbing in fuser,
   which doesn't exist yet. The upstream PR review noted this as a
   separate gap.
3. Skip fuser and write our own FUSE protocol implementation that sets
   up the fd as unrestricted from the start.

None of these are planned for this cycle. Instead, F6.4 routes around the
restriction at the userspace boundary.

### F6.4: uapi-level fallback for FUSE-restricted ioctls ✅ (a landed, b and c future)

**Status:** F6.4a (the `ENOPROTOOPT` contract and the `tree_search_auto`
fallback) shipped in commits `6aa4016` and `25a4af2`. The second commit
dropped the patched `xfbs/fuser` git dependency. `btrfs-fuse` is back on
the released `fuser = "0.17"` from crates.io and can be published again.

The `ino_paths` and `logical_ino` fallbacks (F6.4b and F6.4c) are
specified below but not implemented. Those ioctls return ENOPROTOOPT and
have no userspace fallback yet.

The kernel won't relax the retry restriction on our timeline, but we own
both ends of the call. Our `btrfs` CLI calls the affected ioctls through
wrappers in `btrfs-uapi`, and our FUSE driver chooses what each ioctl
returns. Pairing the two makes the round trip through libc → kernel →
FUSE → uapi self-healing.

**Signal.** For each ioctl that needs retry but can't get it, the FUSE
driver returns `ENOPROTOOPT` up front instead of attempting
`IoctlOutcome::Retry`. The meaning fits: the driver recognizes the ioctl,
just not in this protocol form (the indirect, variable-size variant).

The practical reason for `ENOPROTOOPT` over the more obvious `ENOTSUP` is
that nothing else in the btrfs ioctl surface returns it. Neither does the
VFS for an unsupported op on the wrong filesystem type. It works as a
private channel: if uapi sees it from one of these ioctls, it almost
certainly comes from our FUSE driver, not from a generic "unsupported op"
error in the kernel or another driver. `ENOTSUP` would also work. The
choice is for clarity, not correctness, since the v1 fallback would
surface a real error anyway if the underlying filesystem weren't btrfs.

**Fallback.** Each `btrfs-uapi` wrapper for an ioctl that is restricted on
FUSE catches `ENOPROTOOPT` from its first ioctl call. It then reruns the
operation by composing v1 or fixed-size ioctls that the FUSE driver
supports. The fallback is a plain Rust function over the existing
wrappers, with no new ioctl interfaces.

**Per-ioctl plan:**

- `tree_search_v2(fd, filter, buf_size)`: on `ENOTSUP`, call
  `tree_search` (v1) with the same filter. v1 paginates internally with a
  4 KiB buffer. Semantics are identical, only slower.

- `ino_paths(fd, inum)`: on `ENOTSUP`:
  1. `lookup_path_rootid(fd)` to get the subvol id.
  2. `tree_search` for `objectid=inum, type ∈ {INODE_REF=12,
     INODE_EXTREF=13}`. For each ref, extract `(parent_dirid, name)`.
     `INODE_REF`'s parent is `key.offset`. `INODE_EXTREF` stores it in
     the parsed struct.
  3. For each parent, `BTRFS_IOC_INO_LOOKUP(parent)` returns the path
     string. This works on FUSE because it fits in 4 KiB.
  4. Join `parent_path + "/" + name` for each link.

- `logical_ino` and `logical_ino_v2(fd, logical, ...)`: on `ENOTSUP`:
  1. `tree_search` on tree id 2 (extent tree) for `objectid=logical, type
     ∈ {EXTENT_ITEM=168, METADATA_ITEM=169}`.
  2. Parse `EXTENT_ITEM` to list its inline backrefs (`EXTENT_DATA_REF`,
     `SHARED_DATA_REF`).
  3. Optionally walk standalone `EXTENT_DATA_REF_KEY=178` and
     `SHARED_DATA_REF_KEY=184` keys for the same logical address when the
     inline backref pool is full.
  4. For each `EXTENT_DATA_REF`, emit `(inum, offset, root)`.
  5. `SHARED_DATA_REF` requires following the parent backref. Skipping it
     at first is reasonable.
  6. Needs an `ExtentItem` parser in `btrfs-disk` (likely a new module).

- `space_info` is the one read-side ioctl with no v1 fallback. No
  fixed-size ioctl reaches the chunk tree it summarizes. It stays
  unsupported on FUSE for now. Users who need it can read the backing
  image directly through `btrfs-disk`.

**Optional widening.** Other FUSE btrfs implementations (none exist
today) wouldn't return `ENOPROTOOPT`. The kernel would reject their retry
response with `EIO` instead. To support that case, widen the fallback
trigger to `ENOPROTOOPT || EIO`. Genuine disk errors on those specific
ioctls would then also trigger the fallback. The risk is low, because the
fallback would itself fail with a meaningful error.

**Effect on the fuser dependency.** With F6.4 in place, our CLI never
issues the affected ioctls against our FUSE mount, so our FUSE driver
never needs `ReplyIoctl::retry`. The git pin on xfbs/fuser is no longer
needed:

- Drop the `tree_search_v2` retry handler from `fuse/src/ioctl.rs`. No
  consumer reaches it anymore.
- Stop using the `arg: u64` parameter everywhere. None of the remaining
  handlers need it.
- Switch `fuse/Cargo.toml` back to the released `fuser = "0.17"` from
  crates.io.
- Drop the `allow-git` entry in `deny.toml`.
- Re-enable `publish = true` on `btrfs-fuse`.

**Test plan.** Each shim gets a uapi-level integration test that runs
against our `btrfs-fuse` mount. It currently fails with EIO and passes
once the shim lands. A hidden env var, `BTRFS_FORCE_FUSE_FALLBACK=1`, lets
the same test exercise the fallback against a kernel mount, where it is
the only path under test. The standalone parsers get unit tests, extent
item backrefs in particular.

**Recommended sequencing:**
- F6.4a: detection plumbing, `ENOTSUP` returns, and the `tree_search_v2`
  fallback. Smallest, and proves the pattern.
- F6.4b: `ino_paths` fallback (~50 lines).
- F6.4c: `logical_ino` fallback (~150 lines). Needs an extent item
  backref parser. Defer if no current CLI command needs it.

### F6.3-historical (blocker resolved)

**Scope:**
- `BTRFS_IOC_TREE_SEARCH_V2`: generic tree search. The args struct has a
  flexible `buf[0]` array that exceeds the 14-bit size encoded in the
  ioctl number.
- `BTRFS_IOC_LOGICAL_INO_V2`: logical address → inode (extent tree walk),
  with a variable-size inodes buffer.
- `BTRFS_IOC_INO_PATHS`: inode → all paths (hardlink resolution), with a
  variable-size paths buffer.
- `BTRFS_IOC_GET_SUBVOL_ROOTREF`: subvolume parent backrefs. A 69 KiB
  fixed struct, but it still exceeds the 14-bit cap and needs retry.
- `BTRFS_IOC_LOGICAL_INO` (the older variant) and other admin ioctls whose
  buffers exceed 16 383 bytes.

**Blocker:** `fuser` 0.17's `ReplyIoctl` only exposes `ioctl(result,
data)`. It has no `retry(in_iovs, out_iovs)` method. Without it, FUSE
silently truncates input and output to the 14-bit size encoded in the
ioctl number. This can land after one of:

1. Upstreaming `ReplyIoctl::retry(...)` to fuser
2. Forking fuser locally
3. Skipping fuser entirely with a custom FUSE protocol implementation

Test plan once unblocked:
- `btrfs subvolume list <fuse-mount>` for TREE_SEARCH_V2
- `btrfs inspect-internal inode-resolve <ino>` for INO_PATHS
- `btrfs filesystem show <fuse-mount>` for DEV_INFO. This already works
  with F6.2's fixed-size subset, but full multi-device coverage needs a
  dev tree walk.

**Out of scope:** write ioctls (F11) and admin ioctls (balance, scrub,
qgroup). Those are kernel-managed operations that users run against a real
kernel mount.

### F7: Send stream generation (tier 1 ✅, tiers 2 and 3 future)

Split into tiers. Tier 1 has shipped and the rest are specified. The
detailed rationale was worked out in a design session. In short, there
are three independent axes (full vs incremental, stream version, output
target), and the smallest end-to-end loop came first.

**Tier 1: full v1 sends ✅**

Shipped in commits `38446e6` (encoder), `6742cb8` (walker), and `904da27`
(CLI):

- `btrfs_stream::StreamWriter<W>`: the mirror image of `StreamReader`.
  Encodes any `StreamCommand` variant for v1, v2, and v3, including the
  v2+ DATA attribute quirk. Round-trips through the parser.
- `btrfs_fs::Filesystem::send(snapshot, output) -> Result<output>`: walks
  the subvolume tree path-first. For each inode it emits one of Mkfile,
  Mkdir, Symlink, Mknod, Mkfifo, or Mksock, then xattrs, Write chunks
  (48 KiB cap for v1), Truncate, Chown, Chmod, and Utimes. Hardlinks after
  the first ref emit Link instead of re-creating the file. Subvolume
  crossings are skipped. v1 streams only.
- `btrfs send --offline IMAGE [-f OUT]
  [--offline-subvol PATH | --offline-subvolid ID]`: bypasses the kernel
  `BTRFS_IOC_SEND` entirely. Needs no `CAP_SYS_ADMIN` and no kernel
  mount, and works against FUSE-mounted images. clap's
  `conflicts_with_all` enforces the tier 1 limits: `-p`, `-c`,
  `--no-data`, `--proto`, and `--compressed-data` are rejected in offline
  mode.

The privileged round-trip test
(`send_offline_round_trips_through_kernel_receive`) generates a stream
offline, pipes it to the real kernel `btrfs receive`, and diffs file
contents on the receive side.

**Tier 2: incremental sends (future)**

`btrfs send --offline -p PARENT IMAGE`. The walker takes `parent:
Option<SubvolId>` and diffs the parent and snapshot trees item by item at
each `(objectid, key_type, offset)` triple. It emits SNAPSHOT (not SUBVOL)
and then only the deltas. Rolling snapshot backups are a common use case,
so this is the natural next step.

**Tier 3: v2 EncodedWrite passthrough (future)**

For compressed extents, emit `EncodedWrite` with the on-disk compressed
bytes instead of decompressing and re-emitting plain Write. This saves
CPU and bandwidth on the receive side. It requires reading raw compressed
extent payloads. btrfs-fs currently always decompresses.

**Tier 4: clone sources (`-c`) and v3 verity (future)**

Lower priority. Defer until tiers 1 to 3 are solid.

**Online dispatch (future)**

Plain `btrfs send` (without `--offline`) goes through `BTRFS_IOC_SEND`. A
future enhancement is to detect FUSE mounts (or add an `--auto` flag) and
route to the in-process path transparently. This unblocks
`btrfs send <fuse-mount>/snap`, where the ioctl currently fails.

### F8: True parallel I/O

**Scope:**
- Replace `Mutex<BlockReader<R>>` with a small reader pool. For
  `R = File`, the pool holds `BlockReader<File>` instances, each owning a
  `File::try_clone()` (a cheap `dup(2)` on Linux). Each `spawn_blocking`
  task checks out a reader, runs its I/O, and returns the reader.
- For `R != File` (test cursors and similar), a single mutex-protected
  reader remains.
- Cache hits are already lock-free from F3. Misses now run in parallel on
  different fds. `pread` on different fds is truly concurrent in the
  kernel.

**Test plan:**
- Benchmark: random reads from N tokio tasks. Throughput should scale
  until the disk queue depth saturates.
- Stress: a 1000-task fan-out repeated 10 times, with no deadlocks or
  corrupted data.

**Out of scope:** native async I/O via `tokio-uring` or `monoio`. If
profiling shows `spawn_blocking` overhead is the bottleneck, add it later.
The API doesn't change.

### F9: Write foundation

**Scope:**
- `Filesystem::open_rw(reader: R, writer: W) -> io::Result<Self>`.
  Internally replaces `OpenFilesystem<R>` with `RwOpenFilesystem<R, W>`.
- `Filesystem::tx() -> TxnHandle` opens a write transaction backed by
  `btrfs_transaction::Transaction`. It holds the write lock for its
  lifetime and commits on drop or on an explicit `commit().await`.
- Dirty inode tracking: `RwLock<HashMap<Inode, DirtyInode>>` on `Inner`.
  Write ops populate it and commit drains it.
- Cache invalidation: commit bumps a generation counter. Readers see
  stale entries and re-fetch.
- An empty `TxnHandle` that only commits, with no operations yet. This
  proves the plumbing.

**Test plan:**
- Integration test: open read-write, take a transaction, commit, and
  check that the generation was bumped. No on-disk changes, since the
  transaction was empty.
- `btrfs check` passes after an empty commit. This proves that opening
  for write alone doesn't corrupt anything.

### F10: POSIX write operations

**Scope (one PR per group):**
- Directory ops: `create`, `mkdir`, `unlink`, `rmdir`
- File data: `write` (small inline, large extents), `truncate` (up and
  down)
- Naming: `rename`, `link`, `symlink`
- Metadata: `chmod`, `chown`, `utimens`
- Xattrs: `setxattr`, `removexattr`
- Each op needs an implementation in `fs/src/write/ops.rs`, a test in
  `fs/tests/write.rs`, and a FUSE adapter mapping in `fuse/src/fs.rs`.

**Test plan:**
- Per op: write via `btrfs-fs`, read back via `btrfs-fs`, and assert.
- Per op: write via `btrfs-fs`, mount with kernel btrfs, read back, and
  assert.
- `btrfs check` passes after every write op.

### F11: Write ioctls

**Scope:**
- `BTRFS_IOC_SUBVOL_CREATE_V2`: create a subvolume
- `BTRFS_IOC_SNAP_CREATE_V2`: create a snapshot
- `BTRFS_IOC_SNAP_DESTROY_V2`: delete a subvolume
- `BTRFS_IOC_FICLONE` and `FICLONERANGE`: reflink
- `BTRFS_IOC_DEFRAG_RANGE`: defrag (lower priority, can defer)
- `BTRFS_IOC_SET_FEATURES`: feature flag changes (compat_ro etc.)
- `BTRFS_IOC_SET_RECEIVED_SUBVOL`: used by `btrfs receive`
- Inode flag ioctls: `FS_IOC_GETFLAGS` and `SETFLAGS`,
  `FS_IOC_FSGETXATTR` and `FSSETXATTR`

**Test plan:**
- Per ioctl: drive it through the `btrfs-fs` API in a tokio test, then
  cross-validate by mounting with kernel btrfs.
- End to end: `cp --reflink=always` works against a FUSE mount (it uses
  `FICLONERANGE`).

### F12: fsync semantics and cross-validation

**Scope:**
- `Filesystem::fsync(ino: Inode) -> io::Result<()>` and
  `fdatasync(ino)`.
- Tree-log integration if needed for performance. Defer until benchmarks
  show it matters.
- Crash-safety harness: run a sequence of ops with an interrupt injected
  at random points. Check that the recovered state is consistent: no torn
  writes, and all committed ops visible.
- Acceptance test: run a corpus of write sequences (POSIX ops and btrfs
  ioctls) via `btrfs-fs`, then `btrfs check`. It must pass every time.
- Same or better: mount the result with kernel btrfs, read it back, and
  compare. It must match what `btrfs-fs` reads.

**Out of scope:** O_DIRECT and mmap consistency. Those are FUSE protocol
concerns. If they matter, they go in F13.

### F13: Hardening, benchmarks, and docs

**Scope:**
- Stress tests: large files, deep directory trees, snapshots during
  writes, concurrent read-write with snapshots.
- Benchmarks against kernel btrfs on standard workloads: sequential read,
  random read, sequential write, metadata-heavy.
- Documentation: architecture overview and embedder examples (offline
  image inspection, custom filters, server-side embedder).
- Performance tuning based on profiling.

## Time estimate

- F2 to F8 (full read support): **6 to 10 weeks** of focused effort
- F9 to F12 (full write support): **3 to 6 months**, depending heavily on
  `btrfs-transaction` stability
- F13: **as needed**

## Tracking

This file is the source of truth. Update it as phases land:
- Mark phases ✅ when complete.
- Adjust the scope or test plan if reality diverges.
- Add follow-up issues at the bottom as they come up.

When a phase needs work outside this crate (e.g. F7 adds an encoder to
`btrfs-stream`), call it out in the phase scope.
