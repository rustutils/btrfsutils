use clap::CommandFactory;
use std::{env, fs, io, path::PathBuf};

fn main() -> io::Result<()> {
    let out_dir = env::args_os()
        .nth(1)
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("target/man"));
    fs::create_dir_all(&out_dir)?;

    let btrfs = btrfs_cli::Arguments::command().name("btrfs");
    clap_mangen::generate_to(btrfs, &out_dir)?;

    let mkfs = btrfs_mkfs::args::Arguments::command().name("mkfs.btrfs");
    clap_mangen::generate_to(mkfs, &out_dir)?;

    println!("man pages written to {}", out_dir.display());
    Ok(())
}
