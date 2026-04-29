# btrfs-fs: Userspace btrfs Filesystem Crate

## Goal

A high-level Rust filesystem API on top of `btrfs-disk` (read) and
`btrfs-transaction` (write), exposed as `Filesystem<R>` with all the
operations a userspace driver needs: `lookup`, `readdir`, `read`,
`write`, `getattr`, `xattr_*`, plus btrfs-specific operations like
subvolume creation and send stream generation through ioctl
passthrough.

The crate is the substrate for `btrfs-fuse` (the FUSE driver) and any
other embedder that wants to read or write a btrfs filesystem without
talking to the kernel — offline tools, tests, alternate FUSE bindings,
network-mounted images.

## Design principles

- **FUSE-independent.** The crate exposes plain `io::Result` /
  `Filesystem` ops; nothing depends on `fuser`. The FUSE protocol
  mapping (inode translation, `Stat` → `FileAttr`, `reply.*`) lives
  in `btrfs-fuse`. New embedders depend on `btrfs-fs` directly.

- **Async API from F2 onwards.** All ops become `async fn`. Sync
  internals get wrapped in `tokio::task::spawn_blocking` until we
  have an async I/O backend. The `Filesystem` handle is `Clone` (cheap
  `Arc` bump) so multiple worker tasks can drive it. The FUSE adapter
  spawns a tokio task per callback, moves the `Reply*` handle in,
  awaits the async op, and replies from the task — no FUSE worker
  thread blocked on disk I/O.

- **Single source of truth: `btrfs-disk` for reads, `btrfs-transaction`
  for writes.** No parsing or write-path logic re-implemented at this
  layer. `btrfs-fs` composes the lower-level primitives into
  filesystem-level operations and adds caching, dirty tracking, and
  multi-subvolume bookkeeping.

- **Inode is `(SubvolId, ino)`.** Multi-subvolume support is the
  default mental model from the start, even before F5 implements
  crossing. FUSE adapters translate to a flat `u64` at the boundary.

- **Cache hits go lock-free.** `Filesystem` operations use
  `RwLock<LruCache<...>>` for shared read-side caches; cache misses
  fall back to a serialized I/O path. The split is internal — the
  `&self` API surface stays the same as the cache layer evolves.

- **Happy path only.** No degraded RAID mounts, no partial-recovery
  modes. If `btrfs-disk` can open the filesystem, `btrfs-fs` operates
  on it; if not, it errors out. Recovery tooling lives in
  `btrfs cli` (e.g. `btrfs rescue`).

- **Correctness over performance.** Especially for the write path.
  Cross-validation with kernel btrfs (`btrfs check` after a fuse
  session, mount-as-kernel after fuse modifications) is the
  acceptance test for write phases.

- **Tests at every phase.** Unit tests for pure logic, integration
  tests against `mkfs.btrfs --rootdir` fixture images (unprivileged),
  and from F12 onward cross-validation against kernel btrfs.

## Existing infrastructure we build on

From `btrfs-disk`:
- `BlockReader<R>` with `read_data`, `read_tree_block`
- `filesystem_open()` → `OpenFilesystem` (superblock + chunk cache + root map)
- `TreeBlock` (Node/Leaf), `Header`, `Item`, `DiskKey`
- `tree_walk()` / `tree_walk_tolerant()` with visitor callbacks
- All on-disk item parsers (`InodeItem`, `DirItem`, `FileExtentItem`, ...)
- `Superblock` parsing
- `ChunkTreeCache` for logical→physical resolution

From `btrfs-transaction` (used from F9 onward):
- `Transaction` with `commit()`, B-tree CoW, delayed refs
- High-level helpers: `create_inode`, `link_dir_entry`, `set_xattr`,
  `write_file_data`, `insert_inline_extent`, `set_root_readonly`,
  `set_default_subvol`, `insert_root_ref`, `reserve_data_extent`
- `Filesystem::create_subvolume_shape` for new subvolume bootstrap
- Free-space tree, block group accounting, csum tree updates

From `btrfs-stream`:
- `StreamReader` and TLV command/attribute encoding (used in F7 send)

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

Each phase ends with a green test suite and a single commit. Tests
land *with* the feature, not after.

### F1 — Crate extraction ✅

Done. `btrfs-fs` carved out of `btrfs-fuse`. `Filesystem<R>` exposes
read ops; 19 read-path integration tests pass; fuse shrinks to a thin
adapter.

### F1.5 — `&self` / `Arc<Inner>` handle ✅

Done. `Filesystem<R: Read + Seek + Send>` is `Clone` (cheap `Arc`
bump). All ops take `&self`. Fuse adapter loses its outer Mutex.
Compile-time `Send + Sync` assertion + multithread test.

### F2 — Async refactor ✅

Done. `Filesystem<R>` ops are all `async fn`. Sync I/O wrapped in
`tokio::task::spawn_blocking`; sync `Mutex` held only inside the
blocking task, never across `.await`. Bound: `R: Read + Seek + Send +
'static`.

`btrfs-fuse` carries an internal multi-thread tokio runtime; each
FUSE callback spawns a task that owns the `Reply*`, awaits the async
op, and replies from the task. FUSE worker threads return
immediately.

All 19 read-path tests under `#[tokio::test]`. New
`concurrent_async_reads` test spawns one task per fixture entry on a
4-worker multi-thread runtime and verifies parallel
`lookup → read → getattr` chains all complete correctly. Compile-time
`Send + Sync` assertion still in place.

Native async I/O remains out of scope (deferred to F8 if profiling
justifies it).

### F3 — Caches ✅

Done. Three caches sit on the read path:

- `LruTreeBlockCache`: `Mutex<LruCache<u64, Arc<TreeBlock>>>` keyed
  by logical address, plugged into `BlockReader` via the
  `TreeBlockCache` trait added to `btrfs-disk`. Default 4096 entries
  (~64 MiB).
- `InodeCache`: `Mutex<LruCache<Inode, Arc<InodeItem>>>`, populated
  on `lookup` / `getattr` / `read_inode_item` / `readlink`. Default
  4096 entries.
- `ExtentMapCache`: `Mutex<LruCache<Inode, Arc<ExtentMap>>>` built
  lazily on first `read` of a file; subsequent reads skip the FS
  tree walk entirely. Default 1024 entries.

`Mutex` rather than `RwLock` because LRU mutation happens on every
access (touching MRU order) — even a "read" needs exclusive access
to the cache structure.

`Filesystem::tree_block_cache_stats() -> CacheStats` exposes lock-free
atomic hit/miss/insertion counters for tests and observability.

The trait + invalidation methods are wired up but the generation
counter for transaction-commit invalidation is deferred to F9 (no
write path yet, no invalidation yet).

Out of scope (deferred): persistent cache (across `Filesystem`
instances), benchmarks under `fs/benches/`. The two-test cache suite
(`fs/tests/cache.rs`) verifies effectiveness directly via the stats
API rather than via timing.

### F4 — Compression test sweep ✅

Done. 33 tests in `fs/tests/compression.rs` (11 per algorithm × 3
algorithms via `compression_suite!` macro). Per-algorithm fixture
built once via `mkfs.btrfs --rootdir --compress <algo>` and shared
across the suite via `OnceLock`.

Coverage per algorithm:
- inline compressed extent (full read + partial-with-offset)
- regular compressed extent on highly-compressible 1 MiB zeros
  (full + partial-offset that lands inside a 128 KiB chunk)
- regular extent on 1 MiB pseudo-random bytes (incompressible —
  exercises the "compress flag set, but extent says None" path)
- 16 MiB multi-extent file with a per-MiB byte pattern (full +
  straddling read across both an inter-extent boundary and a 128 KiB
  internal compression-chunk boundary + last-byte read)
- read at EOF / past EOF returns empty

Bugs the sweep caught:
- zstd: `bulk::decompress` rejects trailing bytes after the first
  zstd frame, so multi-frame compressed extents (anything > 128 KiB
  uncompressed) failed. Switched to the streaming decoder.
- inline compressed extents: the read range math was clamped against
  `inline_size` (on-disk compressed length) rather than `ram_bytes`
  (logical length), so any read of a compressed inline extent
  returned a slice too short. Fixed.

LZO had no new bugs — `decompress_lzo` survived the sweep with all
its per-sector framing edge cases handled correctly.

### F5 — Multi-subvolume traversal ✅

Done. `Filesystem::lookup` detects subvolume crossings (DirItem with
`location.key_type == ROOT_ITEM`) and returns an `Inode` carrying
the new subvol id and objectid 256. Reads, readdir, readlink, and
xattr ops automatically follow into the new subvolume's tree via
`tree_root_for(subvol)`. The `..` synthesised at subvolume roots
resolves via `ROOT_BACKREF` in the root tree.

`Filesystem::list_subvolumes() -> Vec<SubvolInfo>` walks the root
tree, returning id, parent, name, ctime, generation, and read-only
flag for every subvolume. System trees are filtered out via
`is_subvolume_id` (id == 5 OR 256 ≤ id ≤ u64::MAX - 256).

`Filesystem::open_subvol(reader, SubvolId)` opens with a non-default
subvolume as `root()`. `Filesystem::default_subvol() -> SubvolId`
exposes the choice.

Fixture: `mkfs.btrfs --rootdir --subvol sub1 --subvol sub1/nested
--subvol sub2`. Tests resolve names → ids dynamically via
`list_subvolumes` since mkfs id assignment isn't argument-order
deterministic. 9 tests cover lookup crossing, nested-subvol
crossing, readdir of subvol root, `..` resolving via ROOT_BACKREF
to FS_TREE, `..` resolving via ROOT_BACKREF to a non-default parent
subvol, list_subvolumes shape, open_subvol happy path,
open_subvol unknown id (NotFound), open_subvol invalid id
(InvalidInput).

Follow-up (now landed): `btrfs-fuse` exposes `--subvol PATH` and
`--subvolid ID`, mutually exclusive. `--subvol` resolves the path
against each subvolume's full parent-chain path; `--subvolid` takes
the tree id directly. `BtrfsFuse::open_subvol` is the matching
library entry point. The fuse adapter learned its
`mount_subvol` field at the same time — the FUSE root inode (`1`)
now maps onto whatever subvolume the `Filesystem` was opened with,
not unconditionally `SubvolId(5)`.

### F6.1 — Read-only ioctls (fixed-size) ✅

Done. `FUSE_IOCTL` plumbing landed in `btrfs-fuse` and three
fixed-size read-only ioctls dispatched through it:

- `BTRFS_IOC_FS_INFO` — superblock geometry, UUIDs, csum type
- `BTRFS_IOC_GET_FEATURES` — compat / compat_ro / incompat words
- `BTRFS_IOC_GET_SUBVOL_INFO` — full subvolume metadata

`btrfs-fs` grew the supporting `Filesystem::superblock()` and
`Filesystem::get_subvol_info(SubvolId)` accessors, and `SubvolInfo`
gained `dirid`/`uuid`/`parent_uuid`/`received_uuid`/`otime`/transids
(marked `#[non_exhaustive]` for future-proofing).

`fuse/src/ioctl.rs` re-derives the kernel ioctl numbers via const
`_IOR` helpers (bindgen doesn't expand the macro family) and
serialises responses into the on-disk C struct layout without
leaking `btrfs_disk::raw` types into the public API.

5 new tests in `fuse/tests/ioctl.rs`:
- 3 libc::ioctl-driven tests (one per ioctl)
- 1 unknown-ioctl test verifying `ENOTTY`
- 1 CLI E2E test that runs our `btrfs subvolume show` against the
  fuse mount — exercises `BTRFS_IOC_GET_SUBVOL_INFO` end-to-end
  through real CLI consumer code

### F6.2 — Read-only ioctls (fixed-size subset) ✅

Done. Two more ioctls landed on top of F6.1:

- `BTRFS_IOC_DEV_INFO` — per-device geometry. Returns the primary
  device's `dev_item` from the superblock; multi-device images need
  a dev-tree walk (deferred). Unknown devid returns ENODEV.
- `BTRFS_IOC_INO_LOOKUP` — `(treeid, objectid)` → path within the
  subvolume. Walks the `INODE_REF` chain upwards from `objectid`
  until the subvol root, with a 4096-iteration loop bound to defend
  against corrupted ref cycles. `treeid == 0` resolves against the
  file's containing subvolume.

`btrfs-fs` gained `Filesystem::dev_info(devid)` and
`Filesystem::ino_lookup(subvol, objectid)` plus a re-export of
`DeviceItem`.

End-to-end CLI tests: `btrfs inspect-internal rootid <mount>` uses
`lookup_path_rootid` (which calls `BTRFS_IOC_INO_LOOKUP` with
objectid=`BTRFS_FIRST_FREE_OBJECTID`) and now succeeds against our
fuse mount, returning the default subvol id 5.

### F6.3 — Variable-size ioctls ✅ (with kernel-imposed scope limit)

`BTRFS_IOC_TREE_SEARCH` (v1, fixed-size 4096) is implemented and is
what the upstream `btrfs` CLI actually uses for `subvolume list`,
giving us a working E2E path. `Filesystem::tree_search(filter,
max_buf_size)` in `btrfs-fs` does the underlying tree walk with
compound-key range filtering (matching kernel semantics).

`BTRFS_IOC_GET_SUBVOL_ROOTREF` (fixed 4096) is implemented on top of
`tree_search` against the root tree; pages through children in
255-entry batches via `min_treeid` (matches the kernel ioctl).
`Filesystem::ino_paths(subvol, objectid) -> Vec<Vec<u8>>` is
exposed by `btrfs-fs` for embedders that want every hardlink path.

`BTRFS_IOC_TREE_SEARCH_V2` has a handler that returns
`IoctlOutcome::Retry`, but in practice it cannot complete — see
the FUSE_IOCTL_RETRY restriction below. Same story for
`BTRFS_IOC_INO_PATHS` and `BTRFS_IOC_LOGICAL_INO_V2`: not wired into
dispatch since the retry round-trip can't happen from a normal
libc `ioctl(2)` caller.

**FUSE_IOCTL_RETRY restriction.** Linux's `fuse_do_ioctl` only
accepts a `FUSE_IOCTL_RETRY` reply when the original request had
`FUSE_IOCTL_UNRESTRICTED` set. The standard
`fuse_file_ioctl` / `fuse_dir_ioctl` paths do not set that flag,
so user-space ioctls reaching us via libc get rejected with
`-EIO` after the first retry response — the kernel never re-issues
the call. Confirmed locally and corroborated by the `xfbs/fuser`
PR review. This means every variable-size btrfs ioctl that needs
retry to extend past the cmd-encoded 14-bit size is blocked at
the kernel boundary today.

Unblock options at the kernel layer:
1. Get the kernel to relax the restriction (unlikely; security).
2. Have the FUSE driver implement a CUSE-style init that opts the
   fd into `FUSE_IOCTL_UNRESTRICTED`. Requires plumbing CUSE_INIT
   in fuser (not implemented today; the upstream PR review noted
   this as a separate gap).
3. Skip fuser and roll our own FUSE protocol implementation that
   sets up the fd as unrestricted from the start.

None of the kernel-layer fixes are pursuing this cycle; instead we
route around the restriction at the userspace boundary — see F6.4.

### F6.4 — uapi-level fallback for FUSE-restricted ioctls ✅ (a landed; b/c future)

**Status:** F6.4a (the foundational ENOPROTOOPT contract +
`tree_search_auto` fallback) shipped in commits `6aa4016` and
`25a4af2`. The latter dropped the patched `xfbs/fuser` git
dependency entirely — `btrfs-fuse` is back on released
`fuser = "0.17"` from crates.io and is publishable again.

`ino_paths` and `logical_ino` fallbacks (F6.4b/c) are still
specced below but not implemented; the corresponding ioctls
return ENOPROTOOPT today and have no userspace fallback yet.

The kernel can't relax the retry restriction in our timeline, but
we own both ends of the call: our `btrfs` CLI calls the broken
ioctls through wrappers in `btrfs-uapi`, and our FUSE driver
chooses what each ioctl returns. Pair the two so the round trip
through libc → kernel → FUSE → uapi is self-healing.

**Signal.** For each ioctl that needs retry but can't get it, the
FUSE driver returns `ENOPROTOOPT` up front instead of attempting
`IoctlOutcome::Retry`. The semantic fit is "we recognise this
ioctl, just not in this protocol form" — i.e. not the
indirected/variable-size variant. The pragmatic reason for
`ENOPROTOOPT` specifically (vs. the more obvious `ENOTSUP`):
nothing else in the btrfs ioctl surface ever returns it, and
neither does the VFS for an unsupported op on the wrong fs type,
so it functions as a private channel. If uapi sees it from one of
these specific ioctls, that's overwhelmingly *our* FUSE driver
speaking — we don't risk falling back on a generic
"unsupported op" error from the kernel or another driver.
(`ENOTSUP` would also work; the choice is for clarity, not
correctness — the v1 fallback would surface a real error anyway
if the underlying fs weren't btrfs.)

**Fallback.** Each `btrfs-uapi` wrapper for a restricted-on-FUSE
ioctl catches `ENOPROTOOPT` from its first ioctl call and re-runs
the operation through composition of v1-/fixed-size ioctls that
the FUSE driver does support. The fallback path is a normal Rust
function over the existing wrappers — no new ioctl interfaces.

**Per-ioctl plan:**

- `tree_search_v2(fd, filter, buf_size)` → on `ENOTSUP`, call
  `tree_search` (v1) with the same filter. v1 paginates internally
  with a 4 KiB buffer; semantics are identical, only slower.

- `ino_paths(fd, inum)` → on `ENOTSUP`:
  1. `lookup_path_rootid(fd)` to get the subvol id.
  2. `tree_search` for `objectid=inum, type ∈ {INODE_REF=12,
     INODE_EXTREF=13}`. For each ref extract `(parent_dirid,
     name)` (`INODE_REF`'s parent is `key.offset`; `INODE_EXTREF`
     stores it in the parsed struct).
  3. For each parent: `BTRFS_IOC_INO_LOOKUP(parent)` → path
     string (works on FUSE — fits in 4 KiB).
  4. Concat `parent_path + "/" + name` per link.

- `logical_ino` / `logical_ino_v2(fd, logical, ...)` → on
  `ENOTSUP`:
  1. `tree_search` on tree id 2 (extent tree) for
     `objectid=logical, type ∈ {EXTENT_ITEM=168,
     METADATA_ITEM=169}`.
  2. Parse `EXTENT_ITEM` to enumerate inline backrefs
     (`EXTENT_DATA_REF`, `SHARED_DATA_REF`).
  3. Optionally walk standalone `EXTENT_DATA_REF_KEY=178` /
     `SHARED_DATA_REF_KEY=184` keys for the same logical addr
     when the inline backref pool is full.
  4. For each `EXTENT_DATA_REF`, emit `(inum, offset, root)`.
  5. `SHARED_DATA_REF` requires following the parent backref;
     skipping initially is reasonable.
  6. Needs an `ExtentItem` parser in `btrfs-disk` (likely a new
     module).

- `space_info` is the one read-side ioctl with no v1 fallback —
  the chunk tree it summarises isn't reachable through any
  fixed-size ioctl. Stays unsupported on FUSE for now; the user
  can read the backing image directly via `btrfs-disk` if they
  need this.

**Optional widening.** Other FUSE-btrfs implementations (none
exist today) wouldn't return `ENOPROTOOPT` — the kernel rejects
their retry response with `EIO` instead. If we ever care about
that case, widen the fallback trigger to `ENOPROTOOPT || EIO`,
accepting that genuine disk errors on those specific ioctls would
also trigger the fallback (low risk; the fallback would then
itself fail with a meaningful error).

**Effect on the fuser dependency.** With F6.4 in place, our CLI
never issues the broken ioctls against our FUSE mount, so our
FUSE driver never needs `ReplyIoctl::retry`. The git pin on
xfbs/fuser becomes unnecessary:

- Drop the `tree_search_v2` retry handler from
  `fuse/src/ioctl.rs` (no longer reachable from any consumer).
- Drop the `arg: u64` parameter use everywhere — none of the
  remaining handlers need it.
- Switch `fuse/Cargo.toml` back to released `fuser = "0.17"`
  from crates.io.
- Drop the `allow-git` entry in `deny.toml`.
- Re-enable `publish = true` on `btrfs-fuse`.

**Test plan.** Each shim gets a uapi-level integration test that
runs against our `btrfs-fuse` mount (currently fails with EIO;
passes after the shim). A hidden env var
`BTRFS_FORCE_FUSE_FALLBACK=1` lets the same test exercise the
fallback path against a kernel mount, where it's the only path
under test. Unit tests for the standalone parsers (extent-item
backrefs in particular).

**Recommended sequencing.** F6.4a: detection plumbing + `ENOTSUP`
returns + `tree_search_v2` fallback (smallest, proves the
pattern). F6.4b: `ino_paths` fallback (~50 lines). F6.4c:
`logical_ino` fallback (~150 lines, needs extent-item backref
parser; defer if not needed by any current CLI command).

### F6.3-historical (blocker resolved)

**Scope:**
- `BTRFS_IOC_TREE_SEARCH_V2` — generic tree search; the args struct
  has a flexible `buf[0]` array that exceeds the 14-bit size encoded
  in the ioctl number
- `BTRFS_IOC_LOGICAL_INO_V2` — logical → inode (extent-tree walk);
  variable-size inodes buffer
- `BTRFS_IOC_INO_PATHS` — inode → all paths (hardlink resolution);
  variable-size paths buffer
- `BTRFS_IOC_GET_SUBVOL_ROOTREF` — subvol parent backrefs; 69 KiB
  fixed struct, but still exceeds the 14-bit cap and needs retry
- `BTRFS_IOC_LOGICAL_INO` (older variant) and other admin ioctls
  whose buffers exceed 16 383 bytes

**Blocker:** `fuser` 0.17's `ReplyIoctl` only exposes `ioctl(result,
data)` — there's no `retry(in_iovs, out_iovs)` method. Without it,
FUSE silently truncates input/output to the size encoded in the
ioctl number's 14-bit size field. We can land this after one of:

1. Upstreaming a `ReplyIoctl::retry(...)` to fuser
2. Forking fuser locally
3. Skipping fuser entirely with a custom FUSE protocol implementation

Test plan once unblocked: `btrfs subvolume list <fuse-mount>` for
TREE_SEARCH_V2, `btrfs inspect-internal inode-resolve <ino>` for
INO_PATHS, `btrfs filesystem show <fuse-mount>` for DEV_INFO (already
works with F6.2's fixed-size subset, but fuller multi-device coverage
needs a dev-tree walk).

**Out of scope:** write ioctls (F11), admin ioctls (balance, scrub,
qgroup) — those are kernel-managed operations; users run them
against a real kernel mount.

### F7 — Send stream generation (tier 1 ✅; tier 2/3 future)

Decomposed into three tiers, with tier 1 shipped and the rest
spec'd. Decomposition rationale and trade-offs are in the
session conversation; the short version is on three orthogonal
axes (full vs incremental, stream version, output target) we
prioritised the smallest end-to-end loop first.

**Tier 1 — full v1 sends ✅**

Shipped in commits `38446e6` (encoder), `6742cb8` (walker),
`904da27` (CLI). Surface:

- `btrfs_stream::StreamWriter<W>`: mirror image of `StreamReader`.
  Encodes any `StreamCommand` variant for v1/v2/v3; v2+ DATA
  attribute quirk handled. Roundtrips through the parser.
- `btrfs_fs::Filesystem::send(snapshot, output) -> Result<output>`:
  walks the subvolume tree path-first, emits per-inode
  Mkfile/Mkdir/Symlink/Mknod/Mkfifo/Mksock + xattrs + Write
  chunks (48 KiB cap for v1) + Truncate + Chown/Chmod/Utimes.
  Hardlinks beyond the first ref emit Link rather than
  re-creating. Subvolume crossings skipped. v1 stream only.
- `btrfs send --offline IMAGE [-f OUT]
  [--offline-subvol PATH | --offline-subvolid ID]`: bypasses
  kernel `BTRFS_IOC_SEND` entirely. No `CAP_SYS_ADMIN`, no kernel
  mount, works against FUSE-mounted images. Tier 1 limitations
  enforced via clap's `conflicts_with_all`: `-p`, `-c`,
  `--no-data`, `--proto`, `--compressed-data` all rejected in
  offline mode.

Round-trip test (`send_offline_round_trips_through_kernel_receive`,
privileged) generates a stream offline → pipes to real kernel
`btrfs receive` → diffs file contents on the receive side.

**Tier 2 — incremental sends (future)**

`btrfs send --offline -p PARENT IMAGE`. Walker takes
`parent: Option<SubvolId>` and emits a coordinated diff between
the parent and snapshot trees (item-by-item at each
`(objectid, key_type, offset)` triple). Emits SNAPSHOT (not
SUBVOL), then only the deltas. Common operational use case
(rolling snapshot backups), so this is the natural next step.

**Tier 3 — v2 EncodedWrite passthrough (future)**

For compressed extents, emit `EncodedWrite` directly with the
on-disk compressed bytes rather than decompressing → re-emitting
as plain Write. Saves CPU + bandwidth on the receive side.
Requires reading raw compressed extent payloads (currently
btrfs-fs always decompresses).

**Tier 4 — clone sources (`-c`) and v3 verity (future)**

Lower priority; defer until tier 1/2/3 are solid.

**Online dispatch (future)**

The existing `btrfs send` (no `--offline`) goes through
`BTRFS_IOC_SEND`. A future enhancement: detect FUSE mounts (or
add an `--auto` flag) and route to the in-process path
transparently. Unblocks `btrfs send <fuse-mount>/snap` where
the ioctl currently fails.

### F8 — True parallel I/O

**Scope:**
- Replace `Mutex<BlockReader<R>>` with a small reader pool. For
  `R = File`: pool of `BlockReader<File>` instances each owning a
  `File::try_clone()` (cheap `dup(2)` on Linux). Each
  `spawn_blocking` task checks out a reader from the pool, runs its
  I/O, returns the reader.
- For `R != File` (test cursors etc.): a single mutex'd reader
  remains.
- Cache hits already lock-free from F3; misses now run in parallel
  on different fds. `pread` on different fds is genuinely
  concurrent at the kernel level.

**Test plan:**
- Benchmark: random reads from N tokio tasks; throughput should
  scale until disk QD saturates.
- Stress: 1000-task fan-out + 10× repeat, no deadlocks or
  wrong-data corruption.

**Out of scope:** native async I/O via `tokio-uring` / `monoio`. If
profiling shows `spawn_blocking` overhead is the bottleneck, do it
later — the API surface doesn't change.

### F9 — Write foundation

**Scope:**
- `Filesystem::open_rw(reader: R, writer: W) -> io::Result<Self>`.
  Replaces `OpenFilesystem<R>` with a `RwOpenFilesystem<R, W>`
  internally.
- `Filesystem::tx() -> TxnHandle` — opens a write transaction
  backed by `btrfs_transaction::Transaction`. Holds the write lock
  for the duration; commits on drop or explicit `commit().await`.
- Dirty inode tracking: `RwLock<HashMap<Inode, DirtyInode>>` on
  `Inner`. Populated by write ops, drained by commit.
- Cache invalidation: on commit, bump generation counter; readers
  see stale entries and re-fetch.
- Empty `TxnHandle` that just commits — no operations yet. Proves
  the plumbing.

**Test plan:**
- Integration test: open rw, take tx, commit, verify generation
  bumped. No on-disk changes (tx was empty).
- `btrfs check` passes after empty commit (proves we don't corrupt
  anything by just opening for write).

### F10 — POSIX write operations

**Scope (one PR per group):**
- Directory ops: `create`, `mkdir`, `unlink`, `rmdir`
- File data: `write` (small inline, large extents), `truncate` (up
  and down)
- Naming: `rename`, `link`, `symlink`
- Metadata: `chmod`, `chown`, `utimens`
- Xattrs: `setxattr`, `removexattr`
- Each op: implementation in `fs/src/write/ops.rs` + test in
  `fs/tests/write.rs` + fuse adapter mapping in `fuse/src/fs.rs`.

**Test plan:**
- Per op: write via `btrfs-fs`, read back via `btrfs-fs`, assert.
- Per op: write via `btrfs-fs`, mount with kernel btrfs, read back,
  assert.
- `btrfs check` passes after every write op.

### F11 — Write ioctls

**Scope:**
- `BTRFS_IOC_SUBVOL_CREATE_V2` — create a new subvolume
- `BTRFS_IOC_SNAP_CREATE_V2` — create a snapshot
- `BTRFS_IOC_SNAP_DESTROY_V2` — delete a subvolume
- `BTRFS_IOC_FICLONE` / `FICLONERANGE` — reflink
- `BTRFS_IOC_DEFRAG_RANGE` — defrag (lower priority; can defer)
- `BTRFS_IOC_SET_FEATURES` — feature flag changes (compat_ro etc.)
- `BTRFS_IOC_SET_RECEIVED_SUBVOL` — used by `btrfs receive`
- inode flags ioctls: `FS_IOC_GETFLAGS` / `SETFLAGS`,
  `FS_IOC_FSGETXATTR` / `FSSETXATTR`

**Test plan:**
- Per ioctl: drive via `btrfs-fs` API in a tokio test;
  cross-validate with kernel btrfs by mounting after.
- End-to-end: `cp --reflink=always` against a fuse mount works
  (uses `FICLONERANGE` under the hood).

### F12 — fsync semantics + cross-validation

**Scope:**
- `Filesystem::fsync(ino: Inode) -> io::Result<()>` /
  `fdatasync(ino)`.
- Tree-log integration if needed for performance (defer until
  benchmarks show it matters).
- Crash-safety harness: write a sequence of ops with an interrupt
  injected at random points; verify post-recovery state is
  consistent (no torn writes, all committed ops visible).
- Acceptance test: a corpus of write sequences (POSIX ops + btrfs
  ioctls), run via `btrfs-fs`, then `btrfs check` — must pass
  every time.
- Same-or-better: mount the resulting filesystem with kernel btrfs,
  read back, compare — must match what `btrfs-fs` would read.

**Out of scope:** O_DIRECT, mmap consistency. Those are FUSE-protocol
concerns; if they matter, they go in F13.

### F13 — Hardening + benchmarks + docs

**Scope:**
- Stress tests: large files, deep dir trees, snapshot during write,
  concurrent rw + snapshot.
- Benchmarks vs kernel btrfs on standard workloads (sequential
  read, random read, sequential write, metadata-heavy).
- Documentation: architecture overview, embedder examples (offline
  image inspection, custom filters, server-side embedder).
- Performance tuning per profiling.

## Time estimate

- F2–F8 (read fully): **6–10 weeks** of focused effort
- F9–F12 (write fully): **3–6 months**, depends heavily on
  `btrfs-transaction` stability
- F13: **as needed**

## Tracking

This file is the source of truth. Update it as phases land:
- Mark phases ✅ when complete
- Adjust scope/test plan if reality diverges
- Add follow-up issues at the bottom as they're discovered

When a phase introduces work outside this crate (e.g. F7 adds an
encoder to `btrfs-stream`), call it out in the phase scope.
