# Kernel Interfaces

All kernel communication lives in `btrfs-uapi`. This page describes how it
wraps the three main kernel interfaces: ioctls, sysfs, and tree search.

## Binding ioctls

Raw bindgen output is in `uapi::raw`, generated from `uapi/src/raw/btrfs.h` and
`btrfs_tree.h`. Ioctl wrappers are declared in `uapi/src/raw.rs` with nix macros:

```rust
ioctl_write_ptr!(btrfs_ioc_resize, BTRFS_IOCTL_MAGIC, 3, btrfs_ioctl_vol_args);
ioctl_read!(btrfs_ioc_fs_info, BTRFS_IOCTL_MAGIC, 31, btrfs_ioctl_fs_info_args);
ioctl_readwrite!(btrfs_ioc_balance_v2, BTRFS_IOCTL_MAGIC, 32, btrfs_ioctl_balance_args);
ioctl_none!(btrfs_ioc_scrub_cancel, BTRFS_IOCTL_MAGIC, 28);
ioctl_write_int!(btrfs_ioc_balance_ctl, BTRFS_IOCTL_MAGIC, 33);
```

The ioctl direction in the C header decides which macro to use:

| C macro | nix macro | Direction |
|---------|-----------|-----------|
| `_IOW`  | `ioctl_write_ptr!` | userspace → kernel (pointer to struct) |
| `_IOR`  | `ioctl_read!` | kernel → userspace |
| `_IOWR` | `ioctl_readwrite!` | both directions |
| `_IO`   | `ioctl_none!` | no data |
| `_IOW` (integer) | `ioctl_write_int!` | value passed directly in arg slot |

## Flexible array member ioctls

Some ioctls return variable-length arrays (for example `btrfs_ioctl_space_args`
with a trailing `spaces[]` field). These use a two-phase call:

1. Call with zero slots to get the count from the kernel.
2. Allocate a `Vec<u64>` (for 8-byte alignment) sized to `base_size + count * item_size`.
3. Cast the vec's pointer to the struct type, set the slot count, and call again.
4. Read the results with `__IncompleteArrayField::as_slice(count)`.

See `uapi/src/space.rs` for a worked example.

## The `btrfs_ioctl_vol_args_v2` union

Several subvolume and device ioctls share `btrfs_ioctl_vol_args_v2`. Bindgen
generates two anonymous union fields:

- `__bindgen_anon_1`: the union of `{size, qgroup_inherit}` and `unused[4]`
- `__bindgen_anon_2`: the union of `name[4040]`, `devid`, and `subvolid`

```rust
// Set a name:
let name_buf: &mut [c_char] = unsafe { &mut args.__bindgen_anon_2.name };

// Set devid (no unsafe needed for plain integer writes):
args.flags = BTRFS_DEVICE_SPEC_BY_ID as u64;
args.__bindgen_anon_2.devid = devid;
```

## Tree search (`BTRFS_IOC_TREE_SEARCH`)

Tree search is the main way to read btrfs B-trees from userspace. It is wrapped
in `uapi/src/tree_search.rs` as a callback-based cursor:

```rust
tree_search(fd, SearchFilter::for_type(tree_id, item_type), |hdr, data| {
    // hdr: SearchHeader: objectid, offset, item_type, len (host byte order)
    // data: &[u8]: raw on-disk item payload (little-endian)
    Ok(())
})?;
```

`tree_search_v2` uses `BTRFS_IOC_TREE_SEARCH_V2` with a larger buffer.
`tree_search_auto` tries v2 first and falls back to v1 when the kernel (or the
`btrfs-fuse` driver) reports v2 as unsupported. Both take the same callback.

Common `SearchFilter` constructors:

```rust
// All items of a specific type across all objectids:
SearchFilter::for_type(raw::BTRFS_CHUNK_TREE_OBJECTID as u64,
                       raw::BTRFS_CHUNK_ITEM_KEY as u32)

// Items of a specific type within an objectid range:
SearchFilter::for_objectid_range(tree_id, item_type, min_oid, max_oid)
```

To search several item types at once (for example the quota tree walk, which
reads STATUS, INFO, LIMIT, and RELATION keys in one pass), build `SearchFilter`
directly with `start` and `end` `Key` values that span the type range.

**Important:** `start` and `end` are bounds on the compound B-tree key
`(objectid, item_type, offset)`. They are not separate per-field filters.
Items of other types can appear if their compound key falls between `start`
and `end`. Callbacks that need a single type should filter on `hdr.item_type`.

### Bindgen type note

Tree objectid constants from `btrfs_tree.h` bind as `u32` in Rust even though
they are `ULL` in C (for example `BTRFS_ROOT_TREE_OBJECTID: u32 = 1`). Always
cast at the use site. `BTRFS_LAST_FREE_OBJECTID` binds as `i32 = -256`. Cast to
`u64`, it gives `0xFFFFFFFF_FFFFFF00` as expected.

### Cursor advancement

This is the most common source of tree search bugs. The kernel treats
`(min_objectid, min_type, min_offset)` as a **compound tuple key**, not three
separate range filters. After each batch, all three fields must move together
past the last returned item:

- **Normal case** (offset does not overflow `u64`):
  set `min_objectid = last.objectid`, `min_type = last.item_type`,
  `min_offset = last.offset + 1`.
- **Offset overflow**: set `min_offset = 0`, keep `min_objectid = last.objectid`,
  set `min_type = last.item_type + 1`.
- **Type also overflows `u32`**: set `min_offset = 0`, `min_type = 0`,
  `min_objectid = last.objectid + 1`.

If only `min_offset` advances and `min_objectid` stays put, items from lower
objectids match the new minimum on every batch, and the search loops forever.

## Sysfs

Some data comes from sysfs rather than ioctls, such as scrub throughput limits
and quota state. The `SysfsBtrfs` type in `uapi/src/sysfs.rs` gives typed
access to `/sys/fs/btrfs/<uuid>/`. The filesystem UUID comes from `filesystem_info()`
(`BTRFS_IOC_FS_INFO`).
