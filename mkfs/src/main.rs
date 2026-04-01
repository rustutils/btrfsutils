use anyhow::Result;
use btrfs_mkfs::args::Arguments;
use clap::Parser;

fn main() -> Result<()> {
    let args = Arguments::parse();
    btrfs_mkfs::run::run(&args)
}
