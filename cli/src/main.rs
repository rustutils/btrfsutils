use anyhow::Result;
use btrfs_cli::Arguments;
use clap::Parser;

fn main() -> Result<()> {
    let args = Arguments::parse();
    args.run()
}
