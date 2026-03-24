//! Safe wrapper for `BTRFS_IOC_TREE_SEARCH`.
//!
//! The kernel exposes a generalised B-tree search ioctl that lets userspace
//! walk any internal btrfs tree (chunk, root, extent, …) one batch of items
//! at a time.  This module provides:
//!
//! * [`SearchKey`] — the search range parameters.
//! * [`SearchHeader`] — per-item metadata returned alongside each item.
//! * [`tree_search`] — the cursor-advancing loop; calls a closure for every
//!   matching item until the range is exhausted or the closure returns `Err`.
//!
//! # Buffer and ioctl version
//!
//! This module uses `BTRFS_IOC_TREE_SEARCH` (v1), which has a fixed 3 992-byte
//! result buffer embedded in the ioctl struct.  This is sufficient for all
//! common item types (chunk, root-item, root-ref, …); the v2 variant that
//! supports larger buffers is not needed here.
//!
//! # Item data byte order
//!
//! The `btrfs_ioctl_search_header` fields are written by the kernel in
//! host byte order (they pass through the ioctl layer).  The item *data*
//! bytes, however, are the raw on-disk representation and are therefore
//! **little-endian** — callers must use `u64::from_le_bytes` and friends when
//! interpreting them.

use std::{mem, os::fd::AsRawFd, os::unix::io::BorrowedFd};

use crate::raw::{
    btrfs_ioc_tree_search, btrfs_ioctl_search_args, btrfs_ioctl_search_header,
    btrfs_ioctl_search_key,
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

        unsafe { btrfs_ioc_tree_search(fd.as_raw_fd(), &mut args) }?;

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
            let raw_hdr: btrfs_ioctl_search_header =
                unsafe { (buf_base.add(off) as *const btrfs_ioctl_search_header).read_unaligned() };
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
            let data: &[u8] = unsafe { std::slice::from_raw_parts(buf_base.add(off), data_len) };
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
/// Returns `false` when the objectid also overflows, meaning the full key
/// space has been exhausted.
fn advance_cursor(sk: &mut btrfs_ioctl_search_key, last: &SearchHeader) -> bool {
    let (new_offset, offset_overflow) = last.offset.overflowing_add(1);
    if !offset_overflow {
        sk.min_offset = new_offset;
        return true;
    }

    sk.min_offset = 0;
    let (new_type, type_overflow) = last.item_type.overflowing_add(1);
    if !type_overflow {
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
