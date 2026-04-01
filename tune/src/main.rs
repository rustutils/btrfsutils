use anyhow::Result;
use btrfs_tune::args::Arguments;
use clap::Parser;

fn main() -> Result<()> {
    let args = Arguments::parse();
    btrfs_tune::run::run(&args)
}
