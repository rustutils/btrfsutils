# Concepts

This page defines the terms used throughout the btrfs documentation and command
output.

## Filesystem

A btrfs filesystem is a single logical storage pool. It has a UUID and an
optional label, and it can span one or more block devices. Data and metadata
are spread across the devices according to the configured RAID profiles.

You access a filesystem by mounting it at a path. Most `btrfs` commands take
that mount point, or any path within it, as their argument.

## Device

A device is a block device (a partition or a whole disk) that belongs to a
filesystem. Every filesystem has at least one device. Devices can be added or
removed while the filesystem is mounted, so capacity can change online.

## Subvolume

A subvolume is an independently managed subtree within a filesystem. It looks
like a directory, but it has its own inode namespace and can be snapshotted,
sent, or deleted on its own.

Mounting a btrfs filesystem mounts one of its subvolumes, the default one unless
you pick another. Other subvolumes appear as directories within it. You can
also mount them directly with the `subvol=` or `subvolid=` mount options.

## Snapshot

A snapshot is a copy-on-write copy of a subvolume at a point in time. It starts
out sharing all of its data with the source. The two diverge as either one is
written. Snapshots can be read-write or read-only. `btrfs send` requires a
read-only snapshot.

## Chunk

btrfs divides storage into chunks: large, contiguous regions of logical address
space, typically 256 MiB for metadata and 1 GiB for data. Each chunk is backed
by one or more physical stripes on the devices, according to the RAID profile.
The chunk tree stores the mapping from logical addresses to physical device
locations.

## Extent

An extent is a contiguous run of bytes within a chunk. File data lives in data
extents. The B-trees that hold btrfs metadata live in metadata extents. btrfs
uses copy-on-write: modifying data creates a new extent instead of overwriting
the old one. This is what makes snapshots cheap.

## Generation

Every committed transaction increments the filesystem's generation number.
Each subvolume records the generation of its last change (its *generation*)
and the generation it was created in (its *ogeneration*, or original
generation). `btrfs subvolume find-new` uses these to find recently changed
files, and `btrfs send` uses them to pick an incremental parent.

## qgroup

A quota group (qgroup) tracks, and optionally limits, the space used by a set
of subvolumes. qgroups can be nested into a hierarchy. This lets shared space
(space that deleting one subvolume would not free) be accounted at the group
level. Quotas must be enabled on the filesystem before qgroups can be used.
