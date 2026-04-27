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

### F6.2 — Read-only ioctls (variable-size + walks)

**Scope:**
- `BTRFS_IOC_TREE_SEARCH_V2` — generic tree search with
  variable-size out buffer; needs the FUSE retry-with-bigger-buffer
  dance
- `BTRFS_IOC_INO_LOOKUP` / `INO_LOOKUP_USER` — inode → path
- `BTRFS_IOC_LOGICAL_INO_V2` — logical → inode (extent-tree walk)
- `BTRFS_IOC_DEV_INFO` — per-device info
- `BTRFS_IOC_GET_SUBVOL_ROOTREF` — subvol parent backrefs
- `BTRFS_IOC_INO_PATHS` — inode → all paths (hardlink resolution)

**Test plan:**
- libc::ioctl-driven tests for each new ioctl
- End-to-end: `btrfs subvolume list <fuse-mount>` exercises
  TREE_SEARCH_V2; `btrfs filesystem show <fuse-mount>` exercises
  DEV_INFO

**Out of scope:** write ioctls (F11), admin ioctls (balance, scrub,
qgroup) — those are kernel-managed operations; users run them
against a real kernel mount.

### F7 — Send stream generation

**Scope:**
- `Filesystem::send(parent: Option<SubvolId>, snapshot: SubvolId,
  output: impl AsyncWrite) -> Result<()>`
- For full sends (no parent): walk the snapshot's FS tree, emit
  `MKDIR`, `MKFILE`, `WRITE`, `SYMLINK`, `MKNOD`, `MKFIFO`,
  `MKSOCK`, `LINK`, `RENAME` (if snapshot has hardlinks),
  `SET_XATTR`, `CHOWN`, `CHMOD`, `UTIMES`, `END_CMD`.
- For incremental sends: tree-diff between parent and snapshot,
  emit only items that changed. Diff is a coordinated walk of
  both subvolume FS trees, comparing items at each `(objectid,
  key_type, offset)` triple.
- TLV encoding via `btrfs-stream` primitives; we likely need to
  add an encoder there if only the parser exists today.
- `BTRFS_IOC_SEND` ioctl handler in fuse that decodes the
  send request and calls into `Filesystem::send`.

**Test plan:**
- Round-trip: generate a send stream, feed it to `btrfs receive`
  (or our own `btrfs-stream` receive context) into a fresh
  filesystem, compare with the source.
- Incremental: parent + snapshot, diff has expected commands.
- Cross-validation: stream generated by `btrfs-fs` is byte-equal
  (or semantically-equal — accounting for command ordering
  flexibility) to one generated by kernel btrfs.

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
