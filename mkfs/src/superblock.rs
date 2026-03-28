//! # Superblock: construct and serialize the btrfs superblock
//!
//! The superblock is a 4096-byte structure at offset 64 KiB on disk. It
//! contains root pointers, device info, the sys_chunk_array for bootstrap,
//! and feature flags.

use crate::write::SUPER_INFO_SIZE;
use btrfs_disk::{
    raw,
    util::{write_le_u16, write_le_u32, write_le_u64, write_uuid},
};
use std::mem;
use uuid::Uuid;

/// Build a superblock byte buffer.
pub struct SuperblockBuilder {
    buf: [u8; SUPER_INFO_SIZE],
}

impl Default for SuperblockBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl SuperblockBuilder {
    pub fn new() -> Self {
        Self {
            buf: [0u8; SUPER_INFO_SIZE],
        }
    }

    pub fn set_fsid(&mut self, fsid: &Uuid) -> &mut Self {
        write_uuid(&mut self.buf, off::FSID, fsid);
        self
    }

    pub fn set_bytenr(&mut self, bytenr: u64) -> &mut Self {
        write_le_u64(&mut self.buf, off::BYTENR, bytenr);
        self
    }

    pub fn set_magic(&mut self) -> &mut Self {
        write_le_u64(&mut self.buf, off::MAGIC, raw::BTRFS_MAGIC);
        self
    }

    pub fn set_generation(&mut self, generation: u64) -> &mut Self {
        write_le_u64(&mut self.buf, off::GENERATION, generation);
        self
    }

    pub fn set_root(&mut self, root_bytenr: u64) -> &mut Self {
        write_le_u64(&mut self.buf, off::ROOT, root_bytenr);
        self
    }

    pub fn set_chunk_root(&mut self, chunk_root_bytenr: u64) -> &mut Self {
        write_le_u64(&mut self.buf, off::CHUNK_ROOT, chunk_root_bytenr);
        self
    }

    pub fn set_total_bytes(&mut self, total: u64) -> &mut Self {
        write_le_u64(&mut self.buf, off::TOTAL_BYTES, total);
        self
    }

    pub fn set_bytes_used(&mut self, used: u64) -> &mut Self {
        write_le_u64(&mut self.buf, off::BYTES_USED, used);
        self
    }

    pub fn set_root_dir_objectid(&mut self, oid: u64) -> &mut Self {
        write_le_u64(&mut self.buf, off::ROOT_DIR_OBJECTID, oid);
        self
    }

    pub fn set_num_devices(&mut self, n: u64) -> &mut Self {
        write_le_u64(&mut self.buf, off::NUM_DEVICES, n);
        self
    }

    pub fn set_sectorsize(&mut self, size: u32) -> &mut Self {
        write_le_u32(&mut self.buf, off::SECTORSIZE, size);
        self
    }

    pub fn set_nodesize(&mut self, size: u32) -> &mut Self {
        write_le_u32(&mut self.buf, off::NODESIZE, size);
        // Also set the legacy leafsize field to match nodesize.
        write_le_u32(&mut self.buf, off::LEAFSIZE, size);
        self
    }

    pub fn set_stripesize(&mut self, size: u32) -> &mut Self {
        write_le_u32(&mut self.buf, off::STRIPESIZE, size);
        self
    }

    pub fn set_chunk_root_generation(&mut self, generation: u64) -> &mut Self {
        write_le_u64(&mut self.buf, off::CHUNK_ROOT_GENERATION, generation);
        self
    }

    pub fn set_incompat_flags(&mut self, flags: u64) -> &mut Self {
        write_le_u64(&mut self.buf, off::INCOMPAT_FLAGS, flags);
        self
    }

    pub fn set_compat_ro_flags(&mut self, flags: u64) -> &mut Self {
        write_le_u64(&mut self.buf, off::COMPAT_RO_FLAGS, flags);
        self
    }

    pub fn set_csum_type(&mut self, csum_type: u16) -> &mut Self {
        write_le_u16(&mut self.buf, off::CSUM_TYPE, csum_type);
        self
    }

    pub fn set_cache_generation(&mut self, generation: u64) -> &mut Self {
        write_le_u64(&mut self.buf, off::CACHE_GENERATION, generation);
        self
    }

    /// Copy the device item bytes into the superblock's embedded dev_item.
    pub fn set_dev_item(&mut self, dev_item_bytes: &[u8]) -> &mut Self {
        let size = mem::size_of::<raw::btrfs_dev_item>();
        assert_eq!(dev_item_bytes.len(), size);
        self.buf[off::DEV_ITEM..off::DEV_ITEM + size]
            .copy_from_slice(dev_item_bytes);
        self
    }

    /// Set the filesystem label (max 255 bytes, NUL-terminated).
    pub fn set_label(&mut self, label: &str) -> &mut Self {
        let bytes = label.as_bytes();
        let max = 255.min(bytes.len());
        self.buf[off::LABEL..off::LABEL + max].copy_from_slice(&bytes[..max]);
        // Ensure NUL terminator
        self.buf[off::LABEL + max] = 0;
        self
    }

    /// Copy the sys_chunk_array: a disk_key + chunk_item for the system chunk.
    pub fn set_sys_chunk_array(&mut self, data: &[u8]) -> &mut Self {
        write_le_u32(
            &mut self.buf,
            off::SYS_CHUNK_ARRAY_SIZE,
            data.len() as u32,
        );
        self.buf[off::SYS_CHUNK_ARRAY..off::SYS_CHUNK_ARRAY + data.len()]
            .copy_from_slice(data);
        self
    }

    /// Finalize: the csum field is left zeroed — caller must compute it.
    pub fn finish(self) -> [u8; SUPER_INFO_SIZE] {
        self.buf
    }
}

/// Field offsets within `btrfs_super_block`, derived from `offset_of!`.
mod off {
    use btrfs_disk::raw::btrfs_super_block;
    use std::mem;

    pub const FSID: usize = mem::offset_of!(btrfs_super_block, fsid);
    pub const BYTENR: usize = mem::offset_of!(btrfs_super_block, bytenr);
    pub const MAGIC: usize = mem::offset_of!(btrfs_super_block, magic);
    pub const GENERATION: usize =
        mem::offset_of!(btrfs_super_block, generation);
    pub const ROOT: usize = mem::offset_of!(btrfs_super_block, root);
    pub const CHUNK_ROOT: usize =
        mem::offset_of!(btrfs_super_block, chunk_root);
    pub const TOTAL_BYTES: usize =
        mem::offset_of!(btrfs_super_block, total_bytes);
    pub const BYTES_USED: usize =
        mem::offset_of!(btrfs_super_block, bytes_used);
    pub const ROOT_DIR_OBJECTID: usize =
        mem::offset_of!(btrfs_super_block, root_dir_objectid);
    pub const NUM_DEVICES: usize =
        mem::offset_of!(btrfs_super_block, num_devices);
    pub const SECTORSIZE: usize =
        mem::offset_of!(btrfs_super_block, sectorsize);
    pub const NODESIZE: usize = mem::offset_of!(btrfs_super_block, nodesize);
    pub const LEAFSIZE: usize =
        mem::offset_of!(btrfs_super_block, __unused_leafsize);
    pub const STRIPESIZE: usize =
        mem::offset_of!(btrfs_super_block, stripesize);
    pub const SYS_CHUNK_ARRAY_SIZE: usize =
        mem::offset_of!(btrfs_super_block, sys_chunk_array_size);
    pub const CHUNK_ROOT_GENERATION: usize =
        mem::offset_of!(btrfs_super_block, chunk_root_generation);
    pub const COMPAT_RO_FLAGS: usize =
        mem::offset_of!(btrfs_super_block, compat_ro_flags);
    pub const INCOMPAT_FLAGS: usize =
        mem::offset_of!(btrfs_super_block, incompat_flags);
    pub const CSUM_TYPE: usize = mem::offset_of!(btrfs_super_block, csum_type);
    pub const DEV_ITEM: usize = mem::offset_of!(btrfs_super_block, dev_item);
    pub const LABEL: usize = mem::offset_of!(btrfs_super_block, label);
    pub const CACHE_GENERATION: usize =
        mem::offset_of!(btrfs_super_block, cache_generation);
    pub const SYS_CHUNK_ARRAY: usize =
        mem::offset_of!(btrfs_super_block, sys_chunk_array);
}

#[cfg(test)]
mod tests {
    use super::*;
    use btrfs_disk::superblock;

    #[test]
    fn offset_sanity_checks() {
        // Verify a few key offsets match the known values from the C layout.
        assert_eq!(off::FSID, 32);
        assert_eq!(off::BYTENR, 48);
        assert_eq!(off::MAGIC, 64);
        assert_eq!(off::GENERATION, 72);
        assert_eq!(off::ROOT, 80);
        assert_eq!(off::CHUNK_ROOT, 88);
        assert_eq!(off::SECTORSIZE, 144);
        assert_eq!(off::NODESIZE, 148);
        assert_eq!(off::DEV_ITEM, 201);
        assert_eq!(off::LABEL, 299);
    }

    #[test]
    fn roundtrip_via_disk_parser() {
        let fsid =
            Uuid::parse_str("deadbeef-dead-beef-dead-beefdeadbeef").unwrap();

        let dev_item_bytes = crate::items::dev_item(
            1,
            512 * 1024 * 1024,
            4 * 1024 * 1024,
            4096,
            &Uuid::new_v4(),
            &fsid,
        );

        let mut sb = SuperblockBuilder::new();
        sb.set_fsid(&fsid)
            .set_bytenr(crate::write::SUPER_INFO_OFFSET)
            .set_magic()
            .set_generation(1)
            .set_root(0x100000)
            .set_chunk_root(0x108000)
            .set_total_bytes(512 * 1024 * 1024)
            .set_bytes_used(7 * 16384)
            .set_num_devices(1)
            .set_sectorsize(4096)
            .set_nodesize(16384)
            .set_stripesize(4096)
            .set_chunk_root_generation(1)
            .set_csum_type(0)
            .set_cache_generation(0)
            .set_label("test-label")
            .set_dev_item(&dev_item_bytes);

        let mut buf = sb.finish();

        // Compute checksum so the parser accepts it.
        crate::write::fill_csum(&mut buf);

        // read_superblock expects the superblock at mirror 0 offset (64 KiB).
        // Build a buffer large enough to place our superblock at the right offset.
        let mut image =
            vec![0u8; crate::write::SUPER_INFO_OFFSET as usize + buf.len()];
        image[crate::write::SUPER_INFO_OFFSET as usize..].copy_from_slice(&buf);
        let parsed = superblock::read_superblock(
            &mut std::io::Cursor::new(&image[..]),
            0,
        )
        .unwrap();

        assert_eq!(parsed.fsid, fsid);
        assert_eq!(parsed.generation, 1);
        assert_eq!(parsed.root, 0x100000);
        assert_eq!(parsed.chunk_root, 0x108000);
        assert_eq!(parsed.total_bytes, 512 * 1024 * 1024);
        assert_eq!(parsed.nodesize, 16384);
        assert_eq!(parsed.sectorsize, 4096);
        assert_eq!(parsed.label, "test-label");
        assert_eq!(parsed.num_devices, 1);
    }
}
