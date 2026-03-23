//! Raw BTRFS bindings.
//!
//! These are the raw ioctl bindings for the BTRFS filesystem. The types
//! are automatically generated using bindgen, and wrapper functions are
//! provided for convenience. Generally, you should use the higher-level
//! bindings provided by this crate rather than the types and functions
//! of this module directly.

use nix::libc::{c_char, c_int};
use nix::{ioctl_none, ioctl_read, ioctl_readwrite, ioctl_write_int, ioctl_write_ptr};

// this is in a nested module so that we can suppress warnings from bindgen.
mod bindings {
    #![allow(warnings)]
    include!(concat!(env!("OUT_DIR"), "/bindings.rs"));
}

pub use bindings::*;

ioctl_write_ptr!(
    btrfs_ioc_snap_create,
    BTRFS_IOCTL_MAGIC,
    1,
    btrfs_ioctl_vol_args
);

ioctl_write_ptr!(btrfs_ioc_defrag, BTRFS_IOCTL_MAGIC, 2, btrfs_ioctl_vol_args);

ioctl_write_ptr!(btrfs_ioc_resize, BTRFS_IOCTL_MAGIC, 3, btrfs_ioctl_vol_args);

ioctl_write_ptr!(
    btrfs_ioc_scan_dev,
    BTRFS_IOCTL_MAGIC,
    4,
    btrfs_ioctl_vol_args
);

ioctl_write_ptr!(
    btrfs_ioc_forget_dev,
    BTRFS_IOCTL_MAGIC,
    5,
    btrfs_ioctl_vol_args
);

ioctl_none!(btrfs_ioc_sync, BTRFS_IOCTL_MAGIC, 8);

ioctl_write_ptr!(btrfs_ioc_clone, BTRFS_IOCTL_MAGIC, 9, c_int);

ioctl_write_ptr!(
    btrfs_ioc_add_dev,
    BTRFS_IOCTL_MAGIC,
    10,
    btrfs_ioctl_vol_args
);

ioctl_write_ptr!(
    btrfs_ioc_rm_dev,
    BTRFS_IOCTL_MAGIC,
    11,
    btrfs_ioctl_vol_args
);

ioctl_write_ptr!(
    btrfs_ioc_balance,
    BTRFS_IOCTL_MAGIC,
    12,
    btrfs_ioctl_vol_args
);

ioctl_write_ptr!(
    btrfs_ioc_clone_range,
    BTRFS_IOCTL_MAGIC,
    13,
    btrfs_ioctl_clone_range_args
);

ioctl_write_ptr!(
    btrfs_ioc_subvol_create,
    BTRFS_IOCTL_MAGIC,
    14,
    btrfs_ioctl_vol_args
);

ioctl_write_ptr!(
    btrfs_ioc_snap_destroy,
    BTRFS_IOCTL_MAGIC,
    15,
    btrfs_ioctl_vol_args
);

ioctl_write_ptr!(
    btrfs_ioc_defrag_range,
    BTRFS_IOCTL_MAGIC,
    16,
    btrfs_ioctl_defrag_range_args
);

ioctl_readwrite!(
    btrfs_ioc_tree_search,
    BTRFS_IOCTL_MAGIC,
    17,
    btrfs_ioctl_search_args
);

ioctl_readwrite!(
    btrfs_ioc_tree_search_v2,
    BTRFS_IOCTL_MAGIC,
    17,
    btrfs_ioctl_search_args_v2
);

ioctl_readwrite!(
    btrfs_ioc_ino_lookup,
    BTRFS_IOCTL_MAGIC,
    18,
    btrfs_ioctl_ino_lookup_args
);

ioctl_write_ptr!(btrfs_ioc_default_subvol, BTRFS_IOCTL_MAGIC, 19, u64);

ioctl_readwrite!(
    btrfs_ioc_space_info,
    BTRFS_IOCTL_MAGIC,
    20,
    btrfs_ioctl_space_args
);

ioctl_write_ptr!(btrfs_ioc_wait_sync, BTRFS_IOCTL_MAGIC, 22, u64);

ioctl_write_ptr!(
    btrfs_ioc_snap_create_v2,
    BTRFS_IOCTL_MAGIC,
    23,
    btrfs_ioctl_vol_args_v2
);

// NOTE: number 24 is shared between START_SYNC (_IOR) and SUBVOL_CREATE_V2 (_IOW)
// in the kernel header — both are translated faithfully here.
ioctl_read!(btrfs_ioc_start_sync, BTRFS_IOCTL_MAGIC, 24, u64);

ioctl_write_ptr!(
    btrfs_ioc_subvol_create_v2,
    BTRFS_IOCTL_MAGIC,
    24,
    btrfs_ioctl_vol_args_v2
);

ioctl_read!(btrfs_ioc_subvol_getflags, BTRFS_IOCTL_MAGIC, 25, u64);

ioctl_write_ptr!(btrfs_ioc_subvol_setflags, BTRFS_IOCTL_MAGIC, 26, u64);

ioctl_readwrite!(
    btrfs_ioc_scrub,
    BTRFS_IOCTL_MAGIC,
    27,
    btrfs_ioctl_scrub_args
);

ioctl_none!(btrfs_ioc_scrub_cancel, BTRFS_IOCTL_MAGIC, 28);

ioctl_readwrite!(
    btrfs_ioc_scrub_progress,
    BTRFS_IOCTL_MAGIC,
    29,
    btrfs_ioctl_scrub_args
);

ioctl_readwrite!(
    btrfs_ioc_dev_info,
    BTRFS_IOCTL_MAGIC,
    30,
    btrfs_ioctl_dev_info_args
);

ioctl_read!(
    btrfs_ioc_fs_info,
    BTRFS_IOCTL_MAGIC,
    31,
    btrfs_ioctl_fs_info_args
);

ioctl_readwrite!(
    btrfs_ioc_balance_v2,
    BTRFS_IOCTL_MAGIC,
    32,
    btrfs_ioctl_balance_args
);

ioctl_write_int!(btrfs_ioc_balance_ctl, BTRFS_IOCTL_MAGIC, 33);

ioctl_read!(
    btrfs_ioc_balance_progress,
    BTRFS_IOCTL_MAGIC,
    34,
    btrfs_ioctl_balance_args
);

ioctl_readwrite!(
    btrfs_ioc_ino_paths,
    BTRFS_IOCTL_MAGIC,
    35,
    btrfs_ioctl_ino_path_args
);

ioctl_readwrite!(
    btrfs_ioc_logical_ino,
    BTRFS_IOCTL_MAGIC,
    36,
    btrfs_ioctl_logical_ino_args
);

ioctl_readwrite!(
    btrfs_ioc_set_received_subvol,
    BTRFS_IOCTL_MAGIC,
    37,
    btrfs_ioctl_received_subvol_args
);
ioctl_write_ptr!(btrfs_ioc_send, BTRFS_IOCTL_MAGIC, 38, btrfs_ioctl_send_args);

ioctl_read!(
    btrfs_ioc_devices_ready,
    BTRFS_IOCTL_MAGIC,
    39,
    btrfs_ioctl_vol_args
);

ioctl_readwrite!(
    btrfs_ioc_quota_ctl,
    BTRFS_IOCTL_MAGIC,
    40,
    btrfs_ioctl_quota_ctl_args
);

ioctl_write_ptr!(
    btrfs_ioc_qgroup_assign,
    BTRFS_IOCTL_MAGIC,
    41,
    btrfs_ioctl_qgroup_assign_args
);

ioctl_write_ptr!(
    btrfs_ioc_qgroup_create,
    BTRFS_IOCTL_MAGIC,
    42,
    btrfs_ioctl_qgroup_create_args
);

ioctl_read!(
    btrfs_ioc_qgroup_limit,
    BTRFS_IOCTL_MAGIC,
    43,
    btrfs_ioctl_qgroup_limit_args
);

ioctl_write_ptr!(
    btrfs_ioc_quota_rescan,
    BTRFS_IOCTL_MAGIC,
    44,
    btrfs_ioctl_quota_rescan_args
);

ioctl_read!(
    btrfs_ioc_quota_rescan_status,
    BTRFS_IOCTL_MAGIC,
    45,
    btrfs_ioctl_quota_rescan_args
);

ioctl_none!(btrfs_ioc_quota_rescan_wait, BTRFS_IOCTL_MAGIC, 46);

// NOTE: GET_FSLABEL/SET_FSLABEL may alias FS_IOC_GETFSLABEL/FS_IOC_SETFSLABEL
// on kernels that define those; these are the btrfs-native variants.
ioctl_read!(btrfs_ioc_get_fslabel, BTRFS_IOCTL_MAGIC, 49, [c_char; 256]);

ioctl_write_ptr!(btrfs_ioc_set_fslabel, BTRFS_IOCTL_MAGIC, 50, [c_char; 256]);

ioctl_readwrite!(
    btrfs_ioc_get_dev_stats,
    BTRFS_IOCTL_MAGIC,
    52,
    btrfs_ioctl_get_dev_stats
);

ioctl_readwrite!(
    btrfs_ioc_dev_replace,
    BTRFS_IOCTL_MAGIC,
    53,
    btrfs_ioctl_dev_replace_args
);

ioctl_readwrite!(
    btrfs_ioc_file_extent_same,
    BTRFS_IOCTL_MAGIC,
    54,
    btrfs_ioctl_same_args
);

// NOTE: number 57 is shared among GET_FEATURES (_IOR), SET_FEATURES (_IOW),
// and GET_SUPPORTED_FEATURES (_IOR) with array types [1], [2], [3].
ioctl_read!(
    btrfs_ioc_get_features,
    BTRFS_IOCTL_MAGIC,
    57,
    btrfs_ioctl_feature_flags
);

ioctl_write_ptr!(
    btrfs_ioc_set_features,
    BTRFS_IOCTL_MAGIC,
    57,
    [btrfs_ioctl_feature_flags; 2]
);

ioctl_read!(
    btrfs_ioc_get_supported_features,
    BTRFS_IOCTL_MAGIC,
    57,
    [btrfs_ioctl_feature_flags; 3]
);

ioctl_write_ptr!(
    btrfs_ioc_rm_dev_v2,
    BTRFS_IOCTL_MAGIC,
    58,
    btrfs_ioctl_vol_args_v2
);

ioctl_readwrite!(
    btrfs_ioc_logical_ino_v2,
    BTRFS_IOCTL_MAGIC,
    59,
    btrfs_ioctl_logical_ino_args
);

ioctl_read!(
    btrfs_ioc_get_subvol_info,
    BTRFS_IOCTL_MAGIC,
    60,
    btrfs_ioctl_get_subvol_info_args
);

ioctl_readwrite!(
    btrfs_ioc_get_subvol_rootref,
    BTRFS_IOCTL_MAGIC,
    61,
    btrfs_ioctl_get_subvol_rootref_args
);

ioctl_readwrite!(
    btrfs_ioc_ino_lookup_user,
    BTRFS_IOCTL_MAGIC,
    62,
    btrfs_ioctl_ino_lookup_user_args
);

ioctl_write_ptr!(
    btrfs_ioc_snap_destroy_v2,
    BTRFS_IOCTL_MAGIC,
    63,
    btrfs_ioctl_vol_args_v2
);

// NOTE: number 64 is shared between ENCODED_READ (_IOR) and ENCODED_WRITE (_IOW).
ioctl_read!(
    btrfs_ioc_encoded_read,
    BTRFS_IOCTL_MAGIC,
    64,
    btrfs_ioctl_encoded_io_args
);

ioctl_write_ptr!(
    btrfs_ioc_encoded_write,
    BTRFS_IOCTL_MAGIC,
    64,
    btrfs_ioctl_encoded_io_args
);

ioctl_write_ptr!(
    btrfs_ioc_subvol_sync_wait,
    BTRFS_IOCTL_MAGIC,
    65,
    btrfs_ioctl_subvol_wait
);

#[cfg(test)]
mod size_tests {
    use super::*;
    use std::mem::size_of;

    #[test]
    #[cfg(target_pointer_width = "64")]
    fn assert_struct_sizes() {
        // Sizes marked [H] are taken directly from _static_assert() in btrfs.h.
        // Sizes marked [C] are computed from the field layout.
        // Structs with flexible array members are tested without the array.

        // [H] btrfs_ioctl_vol_args: s64 fd + char name[4087] = 4096
        assert_eq!(size_of::<btrfs_ioctl_vol_args>(), 4096);
        // [H] btrfs_qgroup_limit: 5 x u64 = 40
        assert_eq!(size_of::<btrfs_qgroup_limit>(), 40);
        // [H] btrfs_qgroup_inherit: base fields only (qgroups[] excluded) = 72
        assert_eq!(size_of::<btrfs_qgroup_inherit>(), 72);
        // [H] btrfs_ioctl_qgroup_limit_args: u64 + btrfs_qgroup_limit(40) = 48
        assert_eq!(size_of::<btrfs_ioctl_qgroup_limit_args>(), 48);
        // [H] btrfs_ioctl_vol_args_v2 = 4096
        assert_eq!(size_of::<btrfs_ioctl_vol_args_v2>(), 4096);
        // [H] btrfs_ioctl_scrub_args = 1024
        assert_eq!(size_of::<btrfs_ioctl_scrub_args>(), 1024);
        // [H] btrfs_ioctl_dev_replace_start_params = 2072
        assert_eq!(size_of::<btrfs_ioctl_dev_replace_start_params>(), 2072);
        // [H] btrfs_ioctl_dev_replace_status_params: 6 x u64 = 48
        assert_eq!(size_of::<btrfs_ioctl_dev_replace_status_params>(), 48);
        // [H] btrfs_ioctl_dev_replace_args = 2600
        assert_eq!(size_of::<btrfs_ioctl_dev_replace_args>(), 2600);
        // [H] btrfs_ioctl_dev_info_args = 4096
        assert_eq!(size_of::<btrfs_ioctl_dev_info_args>(), 4096);
        // [H] btrfs_ioctl_fs_info_args = 1024
        assert_eq!(size_of::<btrfs_ioctl_fs_info_args>(), 1024);
        // [H] btrfs_ioctl_feature_flags: 3 x u64 = 24
        assert_eq!(size_of::<btrfs_ioctl_feature_flags>(), 24);
        // [H] btrfs_ioctl_balance_args = 1024
        assert_eq!(size_of::<btrfs_ioctl_balance_args>(), 1024);
        // [H] btrfs_ioctl_ino_lookup_args = 4096
        assert_eq!(size_of::<btrfs_ioctl_ino_lookup_args>(), 4096);
        // [H] btrfs_ioctl_ino_lookup_user_args = 4096
        assert_eq!(size_of::<btrfs_ioctl_ino_lookup_user_args>(), 4096);
        // [H] btrfs_ioctl_search_args_v2: base fields only (buf[] excluded) = 112
        assert_eq!(size_of::<btrfs_ioctl_search_args_v2>(), 112);
        // [H] btrfs_ioctl_clone_range_args: s64 + 3 x u64 = 32
        assert_eq!(size_of::<btrfs_ioctl_clone_range_args>(), 32);
        // [H] btrfs_ioctl_defrag_range_args = 48
        assert_eq!(size_of::<btrfs_ioctl_defrag_range_args>(), 48);
        // [H] btrfs_ioctl_same_args: base fields only (info[] excluded) = 24
        assert_eq!(size_of::<btrfs_ioctl_same_args>(), 24);
        // [H] btrfs_ioctl_space_args: base fields only (spaces[] excluded) = 16
        assert_eq!(size_of::<btrfs_ioctl_space_args>(), 16);
        // [H] btrfs_ioctl_ino_path_args: inum + size + reserved[4] + fspath = 56
        assert_eq!(size_of::<btrfs_ioctl_ino_path_args>(), 56);
        // [H] btrfs_ioctl_get_dev_stats = 1032
        assert_eq!(size_of::<btrfs_ioctl_get_dev_stats>(), 1032);
        // [H] btrfs_ioctl_quota_ctl_args: 2 x u64 = 16
        assert_eq!(size_of::<btrfs_ioctl_quota_ctl_args>(), 16);
        // [H] btrfs_ioctl_quota_rescan_args: flags + progress + reserved[6] = 64
        assert_eq!(size_of::<btrfs_ioctl_quota_rescan_args>(), 64);
        // [H] btrfs_ioctl_qgroup_create_args: 2 x u64 = 16
        assert_eq!(size_of::<btrfs_ioctl_qgroup_create_args>(), 16);
        // [H] btrfs_ioctl_received_subvol_args = 200
        assert_eq!(size_of::<btrfs_ioctl_received_subvol_args>(), 200);
        // [H] btrfs_ioctl_received_subvol_args_32 (packed) = 192
        assert_eq!(size_of::<btrfs_ioctl_received_subvol_args_32>(), 192);
        // [H] btrfs_ioctl_send_args: 72 on 64-bit (sizeof(__u64 *) == 8)
        assert_eq!(size_of::<btrfs_ioctl_send_args>(), 72);
        // [H] btrfs_ioctl_send_args_64 (packed) = 72
        assert_eq!(size_of::<btrfs_ioctl_send_args_64>(), 72);
        // [H] btrfs_ioctl_get_subvol_rootref_args = 4096
        assert_eq!(size_of::<btrfs_ioctl_get_subvol_rootref_args>(), 4096);

        // [C] btrfs_scrub_progress: 15 x u64 = 120
        assert_eq!(size_of::<btrfs_scrub_progress>(), 120);
        // [C] btrfs_balance_args (packed): 10 x u64 + 2 x u32 + 6 x u64 = 136
        assert_eq!(size_of::<btrfs_balance_args>(), 136);
        // [C] btrfs_balance_progress: 3 x u64 = 24
        assert_eq!(size_of::<btrfs_balance_progress>(), 24);
        // [C] btrfs_ioctl_search_key: 7 x u64 + 4 x u32 + 4 x u64 = 104
        assert_eq!(size_of::<btrfs_ioctl_search_key>(), 104);
        // [C] btrfs_ioctl_search_header: 3 x u64 + 2 x u32 = 32
        assert_eq!(size_of::<btrfs_ioctl_search_header>(), 32);
        // [C] btrfs_ioctl_search_args: key(104) + buf[3992] = 4096
        assert_eq!(size_of::<btrfs_ioctl_search_args>(), 4096);
        // [C] btrfs_ioctl_same_extent_info: s64 + 2 x u64 + s32 + u32 = 32
        assert_eq!(size_of::<btrfs_ioctl_same_extent_info>(), 32);
        // [C] btrfs_ioctl_space_info: 3 x u64 = 24
        assert_eq!(size_of::<btrfs_ioctl_space_info>(), 24);
        // [C] btrfs_data_container: 4 x u32 = 16
        assert_eq!(size_of::<btrfs_data_container>(), 16);
        // [C] btrfs_ioctl_logical_ino_args: logical + size + reserved[3] + flags + inodes = 7 x u64 = 56
        assert_eq!(size_of::<btrfs_ioctl_logical_ino_args>(), 56);
        // [C] btrfs_ioctl_qgroup_assign_args: 3 x u64 = 24
        assert_eq!(size_of::<btrfs_ioctl_qgroup_assign_args>(), 24);
        // [C] btrfs_ioctl_timespec: u64 + u32 + 4 bytes padding = 16
        assert_eq!(size_of::<btrfs_ioctl_timespec>(), 16);
        // [C] btrfs_ioctl_get_subvol_info_args: treeid(8) + name[256] + 4 x u64
        //     + 3 x uuid[16] + 4 x u64 + 4 x timespec(16) + reserved[8](64) = 504
        assert_eq!(size_of::<btrfs_ioctl_get_subvol_info_args>(), 504);
        // [C] btrfs_ioctl_encoded_io_args: *iov(8) + iovcnt(8) + offset(8) + flags(8)
        //     + len(8) + unencoded_len(8) + unencoded_offset(8) + compression(4)
        //     + encryption(4) + reserved[64] = 128
        assert_eq!(size_of::<btrfs_ioctl_encoded_io_args>(), 128);
        // [C] btrfs_ioctl_subvol_wait: u64 + u32 + u32 = 16
        assert_eq!(size_of::<btrfs_ioctl_subvol_wait>(), 16);
    }
}
