# Concepts

This page defines the terms used throughout the btrfs documentation and command
output.

## Filesystem

A btrfs filesystem is a single logical storage pool. It has a UUID and an optional
human-readable label, and it can span one or more physical block devices. All data
and metadata stored in the filesystem is distributed across its devices according
to the configured RAID profiles.

A filesystem is accessed by mounting it at a path. Most `btrfs` commands take that
mount point (or any path within it) as their argument.

## Device

A device is a block device — a disk partition or a whole disk — that belongs to a
filesystem. Every filesystem has at least one device. Additional devices can be
added or removed while the filesystem is mounted, allowing online capacity changes.

## Subvolume

A subvolume is an independently managed subtree within a filesystem. It looks like
a directory, but it has its own inode namespace and can be snapshotted, sent, or
deleted independently from the rest of the filesystem.

When you mount a btrfs filesystem, you are mounting one of its subvolumes (the
default subvolume, unless you specify otherwise). Other subvolumes appear as
directories within it but can also be mounted directly with the `subvol=` or
`subvolid=` mount options.

## Snapshot

A snapshot is a copy-on-write copy of a subvolume taken at a point in time. It
initially shares all of its data with the source subvolume; pages diverge as
either copy is written. Snapshots can be read-write or read-only. Read-only
snapshots are required for `btrfs send`.

## Chunk

btrfs divides storage into chunks — large, contiguous regions of logical address
space (typically 256 MiB for metadata, 1 GiB for data). Each chunk is backed by
one or more physical stripes on the underlying devices, according to the RAID
profile in use. The mapping from logical addresses to physical device locations
is stored in the chunk tree.

## Extent

An extent is a contiguous run of bytes within a chunk. File data is stored in
data extents; the B-trees that make up btrfs metadata are stored in metadata
extents. btrfs uses copy-on-write: modifying data creates a new extent rather
than overwriting the old one, which is what makes snapshots cheap.

## Generation

Every committed transaction increments the filesystem's generation number.
Subvolumes track the generation at which they were last modified (their
*generation*) and the generation at which they were originally created (their
*ogeneration*, or original generation). These are used by tools like
`btrfs subvolume find-new` to identify recently changed files, and by `btrfs send`
to select an appropriate incremental parent.

## qgroup

A quota group (qgroup) tracks and optionally limits the amount of space used by a
set of subvolumes. qgroups can be nested into a hierarchy, which allows shared
space (space that would not be freed even if one subvolume were deleted) to be
accounted at the group level. Quotas must be enabled on the filesystem before
qgroups can be used.
