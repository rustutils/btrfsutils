# Differences from btrfs-progs

btrfsutils aims to be a drop-in replacement for btrfs-progs. Most commands
produce identical output and accept the same flags. This page lists the known
gaps and the features btrfs-progs doesn't have.

## What's not yet supported

These btrfs-progs features are not implemented yet:

- `btrfs check --repair` and related write-mode flags (`--init-csum-tree`,
  `--init-extent-tree`), `--qgroup-report`, and `--subvol-extents`. Read-only
  checking works.
- `btrfs check --mode lowmem`. Only the default mode is supported.
- `btrfs balance start --background`. Balance always runs in the foreground.
- `btrfs filesystem resize --enqueue`.
- `btrfs filesystem show --all-devices`.
- `btrfs rescue fix-data-checksum --interactive`.
- `btrfs-mkfs` zoned device support.

## What's added beyond btrfs-progs

These features are not in the C tools:

- `--format modern` (or `BTRFS_OUTPUT_FORMAT=modern`): opt-in output with
  adaptive column widths and tree views. Most tabular commands support it,
  including `device stats`, `device usage`, `subvolume list`,
  `inspect list-chunks`, `filesystem du`, `filesystem df`, `filesystem show`,
  `filesystem usage`, `qgroup show`, `quota status`, `scrub start`, and
  `scrub status`.
- `btrfs filesystem du --depth N`: limit display depth while computing full totals.
- `btrfs filesystem du --sort`: sort entries by path, total, exclusive, or shared.
- `btrfs inspect list-chunks --offline`: read chunks directly from an unmounted
  device or image file without CAP_SYS_ADMIN.
- `btrfs inspect min-dev-size --offline`: compute minimum device size from an
  unmounted device or image file.
- `btrfs device stats --offline`: read device error statistics from the
  on-disk device tree without mounting the filesystem.
- `btrfs send --offline IMAGE`: generate a v1 send stream from an unmounted
  btrfs image without the kernel ioctl. It needs no `CAP_SYS_ADMIN` and no
  mount. Only full sends are supported.
- `btrfs reflink clone [-r SRCOFF:LENGTH:DESTOFF]... SRC DST`: lightweight
  file copy using `FICLONERANGE`. The btrfs-progs command of the same name is
  a stub that returns `EOPNOTSUPP`. Ours actually clones.
- `btrfs fuse <IMAGE> <MOUNTPOINT>`: mount any btrfs image or block device
  read-only through FUSE. It is a subcommand behind the opt-in `fuse` cargo
  feature and matches the standalone `btrfs-fuse` binary.
