use anyhow::Result;
use clap::Parser;

mod args;
pub mod items;
pub mod tree;
pub mod write;

use args::Arguments;

fn main() -> Result<()> {
    let _args = Arguments::parse();
    todo!("mkfs.btrfs not yet implemented")
}
