use crate::util::{SizeFormat, fmt_size};
use std::fmt;

/// A single check error found during verification.
#[allow(dead_code)] // Variants used incrementally as check phases are added.
pub enum CheckError {
    SuperblockInvalid {
        mirror: u32,
        detail: String,
    },
    TreeBlockChecksumMismatch {
        tree: &'static str,
        logical: u64,
    },
    TreeBlockBadFsid {
        tree: &'static str,
        logical: u64,
    },
    TreeBlockBadBytenr {
        tree: &'static str,
        logical: u64,
        header_bytenr: u64,
    },
    TreeBlockBadGeneration {
        tree: &'static str,
        logical: u64,
        block_gen: u64,
        super_gen: u64,
    },
    TreeBlockBadLevel {
        tree: &'static str,
        logical: u64,
        detail: String,
    },
    KeyOrderViolation {
        tree: &'static str,
        logical: u64,
        index: usize,
    },
    ExtentRefMismatch {
        bytenr: u64,
        expected: u64,
        found: u64,
    },
    MissingExtentItem {
        bytenr: u64,
    },
    OverlappingExtent {
        bytenr: u64,
        length: u64,
        prev_end: u64,
    },
    ChunkMissingBlockGroup {
        logical: u64,
    },
    BlockGroupMissingChunk {
        logical: u64,
    },
    DeviceExtentOverlap {
        devid: u64,
        offset: u64,
    },
    InodeMissing {
        tree: u64,
        ino: u64,
    },
    NlinkMismatch {
        tree: u64,
        ino: u64,
        expected: u32,
        found: u32,
    },
    FileExtentOverlap {
        tree: u64,
        ino: u64,
        offset: u64,
    },
    DirItemOrphan {
        tree: u64,
        parent_ino: u64,
        name: String,
    },
    CsumMismatch {
        logical: u64,
    },
    ReadError {
        logical: u64,
        detail: String,
    },
}

impl fmt::Display for CheckError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::SuperblockInvalid { mirror, detail } => {
                write!(f, "superblock mirror {mirror}: {detail}")
            }
            Self::TreeBlockChecksumMismatch { tree, logical } => {
                write!(f, "{tree}: checksum mismatch at bytenr {logical}")
            }
            Self::TreeBlockBadFsid { tree, logical } => {
                write!(f, "{tree}: bad fsid at bytenr {logical}")
            }
            Self::TreeBlockBadBytenr {
                tree,
                logical,
                header_bytenr,
            } => {
                write!(
                    f,
                    "{tree}: header bytenr {header_bytenr} does not \
                     match logical address {logical}"
                )
            }
            Self::TreeBlockBadGeneration {
                tree,
                logical,
                block_gen,
                super_gen,
            } => {
                write!(
                    f,
                    "{tree}: block generation {block_gen} exceeds \
                     superblock generation {super_gen} at bytenr {logical}"
                )
            }
            Self::TreeBlockBadLevel {
                tree,
                logical,
                detail,
            } => {
                write!(f, "{tree}: bad level at bytenr {logical}: {detail}")
            }
            Self::KeyOrderViolation {
                tree,
                logical,
                index,
            } => {
                write!(
                    f,
                    "{tree}: key ordering violation at bytenr {logical}, \
                     item index {index}"
                )
            }
            Self::ExtentRefMismatch {
                bytenr,
                expected,
                found,
            } => {
                write!(
                    f,
                    "extent ref mismatch at bytenr {bytenr}: \
                     expected {expected} refs, found {found}"
                )
            }
            Self::MissingExtentItem { bytenr } => {
                write!(f, "missing extent item for bytenr {bytenr}")
            }
            Self::OverlappingExtent {
                bytenr,
                length,
                prev_end,
            } => {
                write!(
                    f,
                    "overlapping extent at bytenr {bytenr} \
                     length {length}, previous extent ends at {prev_end}"
                )
            }
            Self::ChunkMissingBlockGroup { logical } => {
                write!(f, "chunk at {logical} has no matching block group item")
            }
            Self::BlockGroupMissingChunk { logical } => {
                write!(f, "block group at {logical} has no matching chunk")
            }
            Self::DeviceExtentOverlap { devid, offset } => {
                write!(
                    f,
                    "overlapping device extent on devid {devid} \
                     at offset {offset}"
                )
            }
            Self::InodeMissing { tree, ino } => {
                write!(
                    f,
                    "root {tree}: inode {ino} referenced but \
                     has no INODE_ITEM"
                )
            }
            Self::NlinkMismatch {
                tree,
                ino,
                expected,
                found,
            } => {
                write!(
                    f,
                    "root {tree}: inode {ino} nlink mismatch: \
                     inode says {expected}, found {found} refs"
                )
            }
            Self::FileExtentOverlap { tree, ino, offset } => {
                write!(
                    f,
                    "root {tree}: inode {ino} file extent overlap \
                     at offset {offset}"
                )
            }
            Self::DirItemOrphan {
                tree,
                parent_ino,
                name,
            } => {
                write!(
                    f,
                    "root {tree}: dir item in inode {parent_ino} \
                     references non-existent inode: '{name}'"
                )
            }
            Self::CsumMismatch { logical } => {
                write!(f, "data checksum mismatch at bytenr {logical}")
            }
            Self::ReadError { logical, detail } => {
                write!(f, "read error at bytenr {logical}: {detail}")
            }
        }
    }
}

/// Accumulated results from all check passes.
pub struct CheckResults {
    pub error_count: u64,
    pub total_csum_bytes: u64,
    pub total_tree_bytes: u64,
    pub total_fs_tree_bytes: u64,
    pub total_extent_tree_bytes: u64,
    pub btree_space_waste: u64,
    pub data_bytes_allocated: u64,
    pub data_bytes_referenced: u64,
    pub bytes_used: u64,
}

impl CheckResults {
    pub fn new(bytes_used: u64) -> Self {
        Self {
            error_count: 0,
            total_csum_bytes: 0,
            total_tree_bytes: 0,
            total_fs_tree_bytes: 0,
            total_extent_tree_bytes: 0,
            btree_space_waste: 0,
            data_bytes_allocated: 0,
            data_bytes_referenced: 0,
            bytes_used,
        }
    }

    /// Record an error, printing it to stderr immediately.
    pub fn report(&mut self, error: CheckError) {
        eprintln!("ERROR: {error}");
        self.error_count += 1;
    }

    /// Print the final summary to stdout.
    pub fn print_summary(&self) {
        let status = if self.error_count == 0 {
            "no error found".to_string()
        } else {
            format!("{} error(s) found", self.error_count)
        };
        let used = fmt_size(self.bytes_used, &SizeFormat::Raw);
        println!("found {used} bytes used, {status}");
        println!("total csum bytes: {}", self.total_csum_bytes);
        println!("total tree bytes: {}", self.total_tree_bytes);
        println!("total fs tree bytes: {}", self.total_fs_tree_bytes);
        println!("total extent tree bytes: {}", self.total_extent_tree_bytes);
        println!("btree space waste bytes: {}", self.btree_space_waste);
        println!("file data blocks allocated: {}", self.data_bytes_allocated);
        println!(" referenced {}", self.data_bytes_referenced);
    }

    pub fn has_errors(&self) -> bool {
        self.error_count > 0
    }
}
