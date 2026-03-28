use anyhow::Result;
use clap::Parser;

mod args;

use args::Arguments;

fn main() -> Result<()> {
    let _args = Arguments::parse();
    todo!("mkfs.btrfs not yet implemented")
}
