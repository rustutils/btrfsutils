/* Portable shim for <linux/fs.h>.
 *
 * btrfs.h includes this header but only references FS_IOC_GETFSLABEL /
 * FS_IOC_SETFSLABEL inside `#ifndef` guards — leaving them undefined
 * lets btrfs.h fall back to its own BTRFS_IOC_GET_FSLABEL definition.
 */
#ifndef _LINUX_FS_H
#define _LINUX_FS_H

#endif /* _LINUX_FS_H */
