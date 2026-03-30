//! # Generic B-tree search: walking any internal btrfs tree via `BTRFS_IOC_TREE_SEARCH`
//!
//! The kernel's tree search ioctl lets userspace read any internal btrfs tree
//! (chunk, root, quota, …) by specifying a key range.  Items are returned in
//! batches; [`tree_search`] advances the cursor automatically and calls a
//! closure once per item until the range is exhausted.
//!
//! # Byte order
//!
//! [`SearchHeader`] fields (objectid, offset, type) are in host byte order:
//! the kernel fills them in through the ioctl layer.  The `data` slice passed
//! to the callback contains the raw on-disk item payload, which is
//! **little-endian**; callers must use `u64::from_le_bytes` and friends when
//! interpreting it.
//!
//! # Ioctl version
//!
//! This module provides two variants:
//!
//! - [`tree_search`] uses `BTRFS_IOC_TREE_SEARCH` (v1) with its fixed
//!   3992-byte result buffer. Sufficient for all item types used by this crate.
//! - [`tree_search_v2`] uses `BTRFS_IOC_TREE_SEARCH_V2` with a caller-chosen
//!   buffer size. Useful when items may be larger than what v1 can return in a
//!   single batch.

use crate::raw::{
    btrfs_ioc_tree_search, btrfs_ioc_tree_search_v2, btrfs_ioctl_search_args,
    btrfs_ioctl_search_args_v2, btrfs_ioctl_search_header,
    btrfs_ioctl_search_key,
};
use std::{
    mem,
    os::{fd::AsRawFd, unix::io::BorrowedFd},
};

/// Parameters specifying which items to return from a tree search.
///
/// The kernel searches a 136-bit key space ordered as
/// `(objectid << 72) | (type << 64) | offset`.
/// All items whose key falls in the inclusive range `[min_key, max_key]` are
/// returned.
///
/// Build a key for common cases with [`SearchKey::for_type`] or
/// [`SearchKey::for_objectid_range`].
///
/// Tree IDs and item type codes are the `BTRFS_*_OBJECTID` and `BTRFS_*_KEY`
/// constants from `crate::raw`, cast to `u64` and `u32` respectively at the
/// call site.
#[derive(Debug, Clone)]
pub struct SearchKey {
    /// Tree to search — use a `BTRFS_*_TREE_OBJECTID` constant from `crate::raw`.
    pub tree_id: u64,
    pub min_objectid: u64,
    pub max_objectid: u64,
    /// Item type — use a `BTRFS_*_KEY` constant from `crate::raw`.
    pub min_type: u32,
    pub max_type: u32,
    pub min_offset: u64,
    pub max_offset: u64,
    /// Filter on the transaction ID of the *metadata block* that holds the
    /// item, not the transaction that created the item itself.
    pub min_transid: u64,
    pub max_transid: u64,
}

impl SearchKey {
    /// Return all items of `item_type` in `tree_id`, across every objectid
    /// and offset.
    #[must_use]
    pub fn for_type(tree_id: u64, item_type: u32) -> Self {
        Self {
            tree_id,
            min_objectid: 0,
            max_objectid: u64::MAX,
            min_type: item_type,
            max_type: item_type,
            min_offset: 0,
            max_offset: u64::MAX,
            min_transid: 0,
            max_transid: u64::MAX,
        }
    }

    /// Return all items of `item_type` in `tree_id` whose objectid falls in
    /// `[min_objectid, max_objectid]`.
    #[must_use]
    pub fn for_objectid_range(
        tree_id: u64,
        item_type: u32,
        min_objectid: u64,
        max_objectid: u64,
    ) -> Self {
        Self {
            min_objectid,
            max_objectid,
            ..Self::for_type(tree_id, item_type)
        }
    }
}

/// Metadata returned for each item found by [`tree_search`].
///
/// The header fields are in host byte order (the kernel fills them in through
/// the ioctl layer).  The accompanying `data` slice passed to the callback is
/// the raw on-disk item payload and is in **little-endian** byte order.
#[derive(Debug, Clone, Copy)]
pub struct SearchHeader {
    pub transid: u64,
    pub objectid: u64,
    pub offset: u64,
    /// Item type (the `type` field of the btrfs key).
    pub item_type: u32,
    /// Length in bytes of the item's data payload.
    pub len: u32,
}

/// Number of items to request per ioctl call.
const ITEMS_PER_BATCH: u32 = 4096;

/// Size of `btrfs_ioctl_search_header` as a compile-time constant.
const SEARCH_HEADER_SIZE: usize = mem::size_of::<btrfs_ioctl_search_header>();

/// Walk every item in the tree that falls within `key`, calling `f` once for
/// each one.
///
/// `f` receives:
/// * a reference to the item's [`SearchHeader`] (objectid, offset, type, …)
/// * a byte slice of the item's raw on-disk data payload (little-endian)
///
/// The search stops early and the error is returned if:
/// * `f` returns `Err(_)`
/// * the underlying `BTRFS_IOC_TREE_SEARCH` ioctl fails
///
/// Returns `Ok(())` when the entire requested range has been scanned.
///
/// # Privileges
///
/// Most trees require `CAP_SYS_ADMIN`.  The subvolume tree of the inode
/// belonging to `fd` can be searched without elevated privileges by setting
/// `key.tree_id = 0`.
pub fn tree_search(
    fd: BorrowedFd,
    key: SearchKey,
    mut f: impl FnMut(&SearchHeader, &[u8]) -> nix::Result<()>,
) -> nix::Result<()> {
    let mut args: btrfs_ioctl_search_args = unsafe { mem::zeroed() };

    fill_search_key(&mut args.key, &key);

    loop {
        args.key.nr_items = ITEMS_PER_BATCH;

        unsafe { btrfs_ioc_tree_search(fd.as_raw_fd(), &raw mut args) }?;

        let nr = args.key.nr_items;
        if nr == 0 {
            break;
        }

        // Walk the result buffer.  We use raw pointer reads to avoid borrow
        // checker conflicts: args.buf (read-only after the ioctl) and
        // args.key (mutated below for cursor advancement) are separate fields,
        // but a Rust reference to one would prevent mutation of the other.
        //
        // SAFETY:
        // * The ioctl has returned successfully, so args.buf contains nr valid
        //   (header, data) pairs totalling at most args.buf.len() bytes.
        // * buf_base is derived from args.buf which lives for the entire loop
        //   body; it is not invalidated until args is dropped.
        // * All raw reads are bounds-checked before dereferencing.
        // * The `data` slices passed to `f` do not outlive this loop
        //   iteration — `f` takes `&[u8]`, not `&'static [u8]`.
        let buf_base: *const u8 = args.buf.as_ptr().cast();
        let buf_cap = args.buf.len();

        let mut off = 0usize;
        let mut last = SearchHeader {
            transid: 0,
            objectid: 0,
            offset: 0,
            item_type: 0,
            len: 0,
        };

        for _ in 0..nr {
            if off + SEARCH_HEADER_SIZE > buf_cap {
                return Err(nix::errno::Errno::EOVERFLOW);
            }
            let raw_hdr: btrfs_ioctl_search_header = unsafe {
                buf_base
                    .add(off)
                    .cast::<btrfs_ioctl_search_header>()
                    .read_unaligned()
            };
            let hdr = SearchHeader {
                transid: raw_hdr.transid,
                objectid: raw_hdr.objectid,
                offset: raw_hdr.offset,
                item_type: raw_hdr.type_,
                len: raw_hdr.len,
            };
            off += SEARCH_HEADER_SIZE;

            let data_len = hdr.len as usize;
            if off + data_len > buf_cap {
                return Err(nix::errno::Errno::EOVERFLOW);
            }
            let data: &[u8] = unsafe {
                std::slice::from_raw_parts(buf_base.add(off), data_len)
            };
            off += data_len;

            f(&hdr, data)?;
            last = hdr;
        }

        if !advance_cursor(&mut args.key, &last) {
            break;
        }
    }

    Ok(())
}

/// Default buffer size for [`tree_search_v2`]: 64 KiB.
const DEFAULT_V2_BUF_SIZE: usize = 64 * 1024;

/// Like [`tree_search`] but uses `BTRFS_IOC_TREE_SEARCH_V2` with a larger
/// result buffer.
///
/// `buf_size` controls the buffer size in bytes (default 64 KiB if `None`).
/// The v2 ioctl is otherwise identical to v1 but can return more data per
/// batch, reducing the number of round-trips for large result sets.
///
/// If `buf_size` is too small for even a single item, the kernel returns
/// `EOVERFLOW` and sets `buf_size` to the required minimum. This function
/// automatically retries with the larger buffer.
pub fn tree_search_v2(
    fd: BorrowedFd,
    key: SearchKey,
    buf_size: Option<usize>,
    mut f: impl FnMut(&SearchHeader, &[u8]) -> nix::Result<()>,
) -> nix::Result<()> {
    let base_size = mem::size_of::<btrfs_ioctl_search_args_v2>();
    let mut capacity = buf_size.unwrap_or(DEFAULT_V2_BUF_SIZE);

    // Allocate as Vec<u64> for 8-byte alignment.
    let alloc_bytes = base_size + capacity;
    let num_u64s = alloc_bytes.div_ceil(mem::size_of::<u64>());
    let mut buf = vec![0u64; num_u64s];

    // SAFETY: buf is correctly sized and aligned for btrfs_ioctl_search_args_v2.
    let args_ptr = buf.as_mut_ptr().cast::<btrfs_ioctl_search_args_v2>();
    unsafe {
        fill_search_key(&mut (*args_ptr).key, &key);
    }

    loop {
        unsafe {
            (*args_ptr).key.nr_items = ITEMS_PER_BATCH;
            (*args_ptr).buf_size = capacity as u64;
        }

        match unsafe {
            btrfs_ioc_tree_search_v2(fd.as_raw_fd(), &raw mut *args_ptr)
        } {
            Ok(_) => {}
            Err(nix::errno::Errno::EOVERFLOW) => {
                // Kernel tells us the needed size via buf_size.
                let needed = unsafe { (*args_ptr).buf_size } as usize;
                if needed <= capacity {
                    return Err(nix::errno::Errno::EOVERFLOW);
                }
                capacity = needed;
                let alloc_bytes = base_size + capacity;
                let num_u64s = alloc_bytes.div_ceil(mem::size_of::<u64>());
                buf.resize(num_u64s, 0);
                // args_ptr must be refreshed after reallocation.
                let args_ptr_new =
                    buf.as_mut_ptr().cast::<btrfs_ioctl_search_args_v2>();
                unsafe {
                    (*args_ptr_new).key.nr_items = ITEMS_PER_BATCH;
                    (*args_ptr_new).buf_size = capacity as u64;
                    btrfs_ioc_tree_search_v2(
                        fd.as_raw_fd(),
                        &raw mut *args_ptr_new,
                    )?;
                }
                // Fall through to process results with the new pointer.
                // Update our local for the rest of the loop.
                let _ = args_ptr_new;
            }
            Err(e) => return Err(e),
        }

        // Re-derive pointer after potential reallocation.
        let args_ptr = buf.as_mut_ptr().cast::<btrfs_ioctl_search_args_v2>();

        let nr = unsafe { (*args_ptr).key.nr_items };
        if nr == 0 {
            break;
        }

        // The result data starts right after the base struct (at the
        // flexible array member `buf[]`).
        let data_base: *const u8 =
            unsafe { (args_ptr as *const u8).add(base_size) };

        let mut off = 0usize;
        let mut last = SearchHeader {
            transid: 0,
            objectid: 0,
            offset: 0,
            item_type: 0,
            len: 0,
        };

        for _ in 0..nr {
            if off + SEARCH_HEADER_SIZE > capacity {
                return Err(nix::errno::Errno::EOVERFLOW);
            }
            let raw_hdr: btrfs_ioctl_search_header = unsafe {
                data_base
                    .add(off)
                    .cast::<btrfs_ioctl_search_header>()
                    .read_unaligned()
            };
            let hdr = SearchHeader {
                transid: raw_hdr.transid,
                objectid: raw_hdr.objectid,
                offset: raw_hdr.offset,
                item_type: raw_hdr.type_,
                len: raw_hdr.len,
            };
            off += SEARCH_HEADER_SIZE;

            let data_len = hdr.len as usize;
            if off + data_len > capacity {
                return Err(nix::errno::Errno::EOVERFLOW);
            }
            let data: &[u8] = unsafe {
                std::slice::from_raw_parts(data_base.add(off), data_len)
            };
            off += data_len;

            f(&hdr, data)?;
            last = hdr;
        }

        if !advance_cursor(unsafe { &mut (*args_ptr).key }, &last) {
            break;
        }
    }

    Ok(())
}

fn fill_search_key(sk: &mut btrfs_ioctl_search_key, key: &SearchKey) {
    sk.tree_id = key.tree_id;
    sk.min_objectid = key.min_objectid;
    sk.max_objectid = key.max_objectid;
    sk.min_type = key.min_type;
    sk.max_type = key.max_type;
    sk.min_offset = key.min_offset;
    sk.max_offset = key.max_offset;
    sk.min_transid = key.min_transid;
    sk.max_transid = key.max_transid;
}

/// Advance the search cursor past `last` so the next batch begins from the
/// item immediately after it in the 136-bit key space
/// `(objectid << 72) | (type << 64) | offset`.
///
/// The kernel interprets `(min_objectid, min_type, min_offset)` as a compound
/// tuple key, so all three fields must be updated together to point past the
/// last returned item.  Advancing only `min_offset` while leaving
/// `min_objectid` at its original value would cause items from lower objectids
/// that were already returned to satisfy the new minimum and be yielded again.
///
/// Returns `false` when the objectid also overflows, meaning the full key
/// space has been exhausted.
fn advance_cursor(
    sk: &mut btrfs_ioctl_search_key,
    last: &SearchHeader,
) -> bool {
    let (new_offset, offset_overflow) = last.offset.overflowing_add(1);
    if !offset_overflow {
        sk.min_objectid = last.objectid;
        sk.min_type = last.item_type;
        sk.min_offset = new_offset;
        return true;
    }

    sk.min_offset = 0;
    let (new_type, type_overflow) = last.item_type.overflowing_add(1);
    if !type_overflow {
        sk.min_objectid = last.objectid;
        sk.min_type = new_type;
        return true;
    }

    sk.min_type = 0;
    let (new_oid, oid_overflow) = last.objectid.overflowing_add(1);
    if oid_overflow {
        return false;
    }
    sk.min_objectid = new_oid;
    true
}

#[cfg(test)]
mod tests {
    use super::*;

    fn header(objectid: u64, item_type: u32, offset: u64) -> SearchHeader {
        SearchHeader {
            transid: 0,
            objectid,
            offset,
            item_type,
            len: 0,
        }
    }

    fn zeroed_search_key() -> btrfs_ioctl_search_key {
        unsafe { mem::zeroed() }
    }

    // --- SearchKey constructors ---

    #[test]
    fn for_type_covers_all_objectids_and_offsets() {
        let sk = SearchKey::for_type(5, 132);
        assert_eq!(sk.tree_id, 5);
        assert_eq!(sk.min_objectid, 0);
        assert_eq!(sk.max_objectid, u64::MAX);
        assert_eq!(sk.min_type, 132);
        assert_eq!(sk.max_type, 132);
        assert_eq!(sk.min_offset, 0);
        assert_eq!(sk.max_offset, u64::MAX);
        assert_eq!(sk.min_transid, 0);
        assert_eq!(sk.max_transid, u64::MAX);
    }

    #[test]
    fn for_objectid_range_restricts_objectids() {
        let sk = SearchKey::for_objectid_range(1, 84, 100, 200);
        assert_eq!(sk.tree_id, 1);
        assert_eq!(sk.min_objectid, 100);
        assert_eq!(sk.max_objectid, 200);
        assert_eq!(sk.min_type, 84);
        assert_eq!(sk.max_type, 84);
        assert_eq!(sk.min_offset, 0);
        assert_eq!(sk.max_offset, u64::MAX);
    }

    // --- fill_search_key ---

    #[test]
    fn fill_search_key_copies_all_fields() {
        let key = SearchKey {
            tree_id: 1,
            min_objectid: 10,
            max_objectid: 20,
            min_type: 30,
            max_type: 40,
            min_offset: 50,
            max_offset: 60,
            min_transid: 70,
            max_transid: 80,
        };
        let mut sk = zeroed_search_key();
        fill_search_key(&mut sk, &key);
        assert_eq!(sk.tree_id, 1);
        assert_eq!(sk.min_objectid, 10);
        assert_eq!(sk.max_objectid, 20);
        assert_eq!(sk.min_type, 30);
        assert_eq!(sk.max_type, 40);
        assert_eq!(sk.min_offset, 50);
        assert_eq!(sk.max_offset, 60);
        assert_eq!(sk.min_transid, 70);
        assert_eq!(sk.max_transid, 80);
    }

    // --- advance_cursor: normal case ---

    #[test]
    fn advance_increments_offset() {
        let mut sk = zeroed_search_key();
        let last = header(256, 132, 100);
        assert!(advance_cursor(&mut sk, &last));
        assert_eq!(sk.min_objectid, 256);
        assert_eq!(sk.min_type, 132);
        assert_eq!(sk.min_offset, 101);
    }

    #[test]
    fn advance_tracks_objectid_from_last_item() {
        // This is the bug that was fixed: the cursor must track the last
        // item's objectid, not leave min_objectid at its original value.
        let mut sk = zeroed_search_key();
        sk.min_objectid = 100; // original search started at 100
        let last = header(300, 132, 50); // batch ended at objectid 300
        assert!(advance_cursor(&mut sk, &last));
        assert_eq!(sk.min_objectid, 300, "must track last item's objectid");
        assert_eq!(sk.min_type, 132);
        assert_eq!(sk.min_offset, 51);
    }

    #[test]
    fn advance_tracks_type_from_last_item() {
        let mut sk = zeroed_search_key();
        let last = header(256, 180, 42);
        assert!(advance_cursor(&mut sk, &last));
        assert_eq!(sk.min_objectid, 256);
        assert_eq!(sk.min_type, 180);
        assert_eq!(sk.min_offset, 43);
    }

    // --- advance_cursor: offset overflow ---

    #[test]
    fn advance_offset_overflow_bumps_type() {
        let mut sk = zeroed_search_key();
        let last = header(256, 132, u64::MAX);
        assert!(advance_cursor(&mut sk, &last));
        assert_eq!(sk.min_objectid, 256);
        assert_eq!(sk.min_type, 133);
        assert_eq!(sk.min_offset, 0);
    }

    // --- advance_cursor: type overflow ---

    #[test]
    fn advance_type_overflow_bumps_objectid() {
        let mut sk = zeroed_search_key();
        let last = header(256, u32::MAX, u64::MAX);
        assert!(advance_cursor(&mut sk, &last));
        assert_eq!(sk.min_objectid, 257);
        assert_eq!(sk.min_type, 0);
        assert_eq!(sk.min_offset, 0);
    }

    // --- advance_cursor: full keyspace exhaustion ---

    #[test]
    fn advance_all_overflow_returns_false() {
        let mut sk = zeroed_search_key();
        let last = header(u64::MAX, u32::MAX, u64::MAX);
        assert!(!advance_cursor(&mut sk, &last));
    }

    // --- advance_cursor: edge cases ---

    #[test]
    fn advance_zero_key() {
        let mut sk = zeroed_search_key();
        let last = header(0, 0, 0);
        assert!(advance_cursor(&mut sk, &last));
        assert_eq!(sk.min_objectid, 0);
        assert_eq!(sk.min_type, 0);
        assert_eq!(sk.min_offset, 1);
    }

    #[test]
    fn advance_objectid_max_type_zero_offset_max() {
        // offset overflows, type bumps to 1
        let mut sk = zeroed_search_key();
        let last = header(u64::MAX, 0, u64::MAX);
        assert!(advance_cursor(&mut sk, &last));
        assert_eq!(sk.min_objectid, u64::MAX);
        assert_eq!(sk.min_type, 1);
        assert_eq!(sk.min_offset, 0);
    }

    #[test]
    fn advance_preserves_unrelated_search_key_fields() {
        let mut sk = zeroed_search_key();
        sk.max_objectid = 999;
        sk.max_type = 888;
        sk.max_offset = 777;
        sk.max_transid = 666;
        let last = header(10, 20, 30);
        advance_cursor(&mut sk, &last);
        assert_eq!(sk.max_objectid, 999);
        assert_eq!(sk.max_type, 888);
        assert_eq!(sk.max_offset, 777);
        assert_eq!(sk.max_transid, 666);
    }
}
