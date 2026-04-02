use anyhow::Result;
use clap::Parser;

fn main() -> Result<()> {
    #[cfg(feature = "multicall")]
    match binary_name().as_deref() {
        #[cfg(feature = "mkfs")]
        Some("mkfs.btrfs" | "btrfs-mkfs") => {
            let args = btrfs_mkfs::args::Arguments::parse();
            return btrfs_mkfs::run::run(&args);
        }
        #[cfg(feature = "tune")]
        Some("btrfstune" | "btrfs-tune") => {
            let args = btrfs_tune::args::Arguments::parse();
            return btrfs_tune::run::run(&args);
        }
        _ => {}
    }

    let args = btrfs_cli::Arguments::parse();
    args.run()
}

/// Extract the file name of the current executable from `argv[0]`.
#[cfg(feature = "multicall")]
fn binary_name() -> Option<String> {
    std::env::args()
        .next()
        .as_deref()
        .and_then(|s| std::path::Path::new(s).file_name())
        .map(|n| n.to_string_lossy().into_owned())
}
