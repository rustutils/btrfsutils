# Commands

btrfsutils implements the same command structure as the upstream `btrfs` tool.
Commands are organized into groups:

## btrfs filesystem

Manage and inspect mounted filesystems.

| Command | Description |
|---------|-------------|
| `btrfs filesystem show [path]` | Show filesystem info and devices |
| `btrfs filesystem df <path>` | Show space usage by chunk type |
| `btrfs filesystem usage <path>` | Detailed space usage with per-device breakdown |
| `btrfs filesystem du <path>` | Show disk usage including shared extents |
| `btrfs filesystem sync <path>` | Sync the filesystem |
| `btrfs filesystem defrag <path>` | Defragment a file or directory |
| `btrfs filesystem resize <size> <path>` | Resize a mounted filesystem |
| `btrfs filesystem label <path> [label]` | Get or set the filesystem label |
| `btrfs filesystem mkswapfile <path>` | Create a swapfile |
| `btrfs filesystem commit-stats <path>` | Show commit statistics |

## btrfs subvolume

Create and manage subvolumes and snapshots.

| Command | Description |
|---------|-------------|
| `btrfs subvolume create <path>` | Create a subvolume |
| `btrfs subvolume delete <path>` | Delete a subvolume |
| `btrfs subvolume snapshot <src> <dst>` | Create a snapshot |
| `btrfs subvolume list <path>` | List subvolumes |
| `btrfs subvolume show <path>` | Show subvolume details |
| `btrfs subvolume get-default <path>` | Show the default subvolume |
| `btrfs subvolume set-default <id> <path>` | Set the default subvolume |
| `btrfs subvolume get-flags <path>` | Show subvolume flags |
| `btrfs subvolume set-flags <path>` | Set subvolume flags |
| `btrfs subvolume find-new <path> <gen>` | Find files modified since a generation |
| `btrfs subvolume sync <path>` | Wait for deleted subvolumes to be cleaned up |

## btrfs device

Manage devices in a multi-device filesystem.

| Command | Description |
|---------|-------------|
| `btrfs device add <dev> <path>` | Add a device |
| `btrfs device remove <dev> <path>` | Remove a device |
| `btrfs device stats <path>` | Show per-device error statistics |
| `btrfs device scan [dev]` | Scan for btrfs devices |
| `btrfs device ready <dev>` | Check if a multi-device filesystem is ready |
| `btrfs device usage <path>` | Show per-device allocation details |

## btrfs balance

Rebalance data and metadata across devices or profiles.

| Command | Description |
|---------|-------------|
| `btrfs balance start <path>` | Start a balance |
| `btrfs balance pause <path>` | Pause a running balance |
| `btrfs balance resume <path>` | Resume a paused balance |
| `btrfs balance cancel <path>` | Cancel a running or paused balance |
| `btrfs balance status <path>` | Show balance status |

Balance filters (`-d`, `-m`, `-s`) accept filter strings such as
`usage=50,profiles=raid1|single`.

## btrfs scrub

Verify data and metadata checksums.

| Command | Description |
|---------|-------------|
| `btrfs scrub start <path>` | Start a scrub |
| `btrfs scrub cancel <path>` | Cancel a running scrub |
| `btrfs scrub resume <path>` | Resume a cancelled scrub |
| `btrfs scrub status <path>` | Show scrub status |
| `btrfs scrub limit <path>` | Get or set scrub throughput limit |

## btrfs replace

Replace a device in a filesystem.

| Command | Description |
|---------|-------------|
| `btrfs replace start <srcdev> <tgtdev> <path>` | Start a device replacement |
| `btrfs replace status <path>` | Show replacement status |
| `btrfs replace cancel <path>` | Cancel a running replacement |

## btrfs send / receive

Stream filesystem data between systems.

| Command | Description |
|---------|-------------|
| `btrfs send <subvol>` | Send a subvolume as a stream |
| `btrfs receive <path>` | Receive a stream into a directory |

`btrfs send` supports full sends and incremental sends (`-p` parent, `-c` clone
sources). `btrfs receive` supports v1, v2 (compressed data), and v3 (fs-verity)
stream formats.

## btrfs inspect-internal

Low-level inspection tools.

| Command | Description |
|---------|-------------|
| `btrfs inspect-internal rootid <path>` | Show the subvolume ID for a path |
| `btrfs inspect-internal inode-resolve <ino> <path>` | Resolve an inode to paths |
| `btrfs inspect-internal logical-resolve <addr> <path>` | Resolve a logical address to paths |
| `btrfs inspect-internal subvolid-resolve <id> <path>` | Resolve a subvolume ID to a path |
| `btrfs inspect-internal min-dev-size <path>` | Show the minimum safe device size |
| `btrfs inspect-internal list-chunks <path>` | List all chunk allocations |
| `btrfs inspect-internal dump-super <dev>` | Dump the superblock |
| `btrfs inspect-internal dump-tree <dev>` | Dump raw B-tree contents |
| `btrfs inspect-internal tree-stats <dev>` | Walk a B-tree and report node/leaf statistics |
| `btrfs inspect-internal map-swapfile <path>` | Show physical extent map of a swapfile |

`dump-super` and `dump-tree` read directly from a block device or image file and
do not require a mounted filesystem or elevated privileges.

## btrfs quota / qgroup

Manage filesystem quotas.

| Command | Description |
|---------|-------------|
| `btrfs quota enable <path>` | Enable quotas |
| `btrfs quota disable <path>` | Disable quotas |
| `btrfs quota rescan <path>` | Rescan quota usage |
| `btrfs quota status <path>` | Show quota status |
| `btrfs qgroup show <path>` | Show qgroup usage |
| `btrfs qgroup create <id> <path>` | Create a qgroup |
| `btrfs qgroup destroy <id> <path>` | Destroy a qgroup |
| `btrfs qgroup assign <src> <dst> <path>` | Assign a qgroup to a parent |
| `btrfs qgroup remove <src> <dst> <path>` | Remove a qgroup assignment |
| `btrfs qgroup limit <size> <id> <path>` | Set a qgroup size limit |
| `btrfs qgroup clear-stale <path>` | Remove stale qgroups |

## btrfs property

Get and set filesystem object properties.

| Command | Description |
|---------|-------------|
| `btrfs property get <path> [name]` | Get a property |
| `btrfs property set <path> <name> <value>` | Set a property |
| `btrfs property list <path>` | List available properties |

Supported properties: `ro` (subvolumes), `label` (filesystem/device),
`compression` (inodes).

## btrfs restore

Recover files from a damaged or unmounted filesystem by reading on-disk
structures directly.

| Command | Description |
|---------|-------------|
| `btrfs restore <dev> <path>` | Restore files to a destination directory |
| `btrfs restore -l <dev>` | List available tree roots |

Supports regular files, directories, symlinks (`-S`), extended attributes (`-x`),
metadata (owner/mode/times with `-m`), and compressed extents (zlib/zstd/lzo).
Use `--path-regex` to filter restored files and `-s` to include snapshots.

## btrfs rescue

Emergency recovery tools for damaged filesystems.

| Command | Description |
|---------|-------------|
| `btrfs rescue super-recover <dev>` | Restore superblock from mirrors |
| `btrfs rescue zero-log <dev>` | Clear the log tree pointer |
| `btrfs rescue create-control-device` | Create `/dev/btrfs-control` if missing |

Six additional subcommands (`fix-device-size`, `chunk-recover`,
`clear-space-cache`, `clear-uuid-tree`, `clear-ino-cache`,
`clear-free-space-tree`) have argument parsing scaffolded but are not yet
implemented.

## mkfs.btrfs

Create a new btrfs filesystem on a block device or image file.

```
btrfs-mkfs [options] <device> [device...]
```

Supports single-device and multi-device filesystems, metadata DUP and RAID1
profiles, data SINGLE/RAID0/RAID1 profiles, all four checksum algorithms
(crc32c, xxhash, sha256, blake2b), custom nodesize/sectorsize, labels, UUIDs,
and feature flags.

## btrfstune

Modify btrfs filesystem parameters on an unmounted device.

```
btrfs-tune [options] <device>
```

| Flag | Description |
|------|-------------|
| `-r` | Enable extended inode refs (extref) |
| `-x` | Enable skinny metadata extent refs |
| `-n` | Enable no-holes feature |
| `-S 0` / `-S 1` | Clear or set the seeding flag |
| `-m` | Change fsid to a random UUID (metadata_uuid mechanism) |
| `-M <uuid>` | Change fsid to a specific UUID (metadata_uuid mechanism) |
| `-u` | Rewrite fsid to a random UUID (patches all tree blocks) |
| `-U <uuid>` | Rewrite fsid to a specific UUID (patches all tree blocks) |

## Global flags

These flags are accepted by all `btrfs` commands:

| Flag | Description |
|------|-------------|
| `-v` / `--verbose` | Increase verbosity (repeatable) |
| `-q` / `--quiet` | Suppress non-error output |
