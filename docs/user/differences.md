# Differences from btrfs-progs

btrfsutils aims to be a drop-in replacement for btrfs-progs. Most commands
produce identical output and accept the same flags. This page lists the
known gaps and the features that go beyond what btrfs-progs offers.

## What's not yet supported

These features from btrfs-progs are not yet implemented:

- `btrfs check --repair` and related write-mode flags (`--init-csum-tree`,
  `--init-extent-tree`, etc.). Read-only checking works.
- `btrfs check --mode lowmem` (currently only the default mode is supported).
- `btrfs rescue chunk-recover`. Other write-mode rescue subcommands
  (`fix-device-size`, `clear-space-cache`, `clear-uuid-tree`,
  `clear-ino-cache`, `fix-data-checksum`) are implemented.
- `btrfs filesystem resize --offline`.
- `btrfs-mkfs` zoned device support.
- `btrfs-tune --convert-to-free-space-tree` and `--convert-to-block-group-tree`.

## What's added beyond btrfs-progs

These features are original additions not present in the C tools:

- `--format modern` (or `BTRFS_OUTPUT_FORMAT=modern`): opt-in improved output
  with adaptive column widths and tree views. Supported by most tabular
  commands including `device stats`, `device usage`, `subvolume list`,
  `inspect list-chunks`, `filesystem du/df/show/usage`, `qgroup show`,
  `quota status`, `scrub start/status`.
- `btrfs filesystem du --depth N`: limit display depth while computing full totals.
- `btrfs filesystem du --sort`: sort entries by path, total, exclusive, or shared.
- `btrfs inspect list-chunks --offline`: read chunks directly from an unmounted
  device or image file without CAP_SYS_ADMIN.
- `btrfs inspect min-dev-size --offline`: compute minimum device size from an
  unmounted device or image file.
- `btrfs device stats --offline`: read device error statistics from the on-disk
  device tree without requiring a mounted filesystem.
