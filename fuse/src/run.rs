//! Entry point that wires [`crate::args::MountArgs`] up to
//! [`crate::BtrfsFuse`] and `fuser::mount2`.

use crate::{BtrfsFuse, args::MountArgs};
use anyhow::{Context, Result, anyhow};
use btrfs_fs::{CacheConfig, Filesystem, SubvolId};
use fuser::{Config, MountOption, SessionACL};
use std::{fs::File, path::Path};

/// Mount a btrfs image read-only via FUSE according to `args`.
///
/// Resolves `--subvol` / `--subvolid` to a [`SubvolId`], constructs
/// the [`BtrfsFuse`] adapter with the requested cache sizes, builds
/// the FUSE [`Config`] from `--allow-other` and
/// `--no-default-permissions`, and hands the result to
/// `fuser::mount2`. Blocks the calling thread until the mount is
/// torn down.
///
/// # Errors
///
/// Returns an error if the image can't be opened, the requested
/// subvolume doesn't exist, the FUSE mount fails, or the underlying
/// filesystem bootstrap fails.
pub fn run_mount(args: &MountArgs) -> Result<()> {
    let target_subvol = if let Some(id) = args.subvolid {
        Some(SubvolId(id))
    } else if let Some(path) = args.subvol.as_deref() {
        Some(resolve_subvol_path(&args.image, path)?)
    } else {
        None
    };

    let caches = CacheConfig {
        tree_blocks: args.cache_tree_blocks,
        inodes: args.cache_inodes,
        extent_maps: args.cache_extent_maps,
    };

    let file = File::open(&args.image)
        .with_context(|| format!("opening {}", args.image.display()))?;
    let fs = match target_subvol {
        Some(id) => BtrfsFuse::open_subvol_with_caches(file, id, caches)
            .with_context(|| format!("opening subvolume {}", id.0))?,
        None => BtrfsFuse::open_with_caches(file, caches)
            .context("bootstrapping btrfs filesystem")?,
    };

    // `Config` is `#[non_exhaustive]`, so we can't use a struct
    // literal even with `..default()` from outside the crate;
    // mutate fields instead.
    let mut config = Config::default();
    let mut mount_options = vec![
        MountOption::RO,
        MountOption::FSName("btrfs-fuse".to_string()),
        MountOption::Subtype("btrfs".to_string()),
    ];
    if !args.no_default_permissions {
        mount_options.push(MountOption::DefaultPermissions);
    }
    config.mount_options = mount_options;
    config.acl = if args.allow_other {
        SessionACL::All
    } else {
        SessionACL::Owner
    };

    // TODO: respect `--foreground=false` once we add a daemonize path.
    let _ = args.foreground;
    fuser::mount2(fs, &args.mountpoint, &config).with_context(|| {
        format!("mounting at {}", args.mountpoint.display())
    })?;
    Ok(())
}

/// Resolve a slash-separated subvolume path (relative to the FS root)
/// to its [`SubvolId`] via [`Filesystem::resolve_subvol_path`].
///
/// Opens the image in a temporary capacity for the lookup; the FUSE
/// mount itself reopens the file. (Collapsing to a single open would
/// require threading a `Filesystem` into `BtrfsFuse::from_filesystem`
/// — left as future work.)
fn resolve_subvol_path(image: &Path, path: &str) -> Result<SubvolId> {
    let file = File::open(image)
        .with_context(|| format!("opening {}", image.display()))?;
    let fs = Filesystem::open(file)
        .context("bootstrapping btrfs filesystem to resolve --subvol")?;

    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .context("creating temporary tokio runtime")?;
    runtime
        .block_on(fs.resolve_subvol_path(path))
        .context("resolving subvolume path")?
        .ok_or_else(|| {
            anyhow!("subvolume path {path:?} not found on {}", image.display())
        })
}
