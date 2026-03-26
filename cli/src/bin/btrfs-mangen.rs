use btrfs_cli::Arguments;
use clap::CommandFactory;
use std::{fs, io, path::Path};

fn main() -> io::Result<()> {
    let out_dir = Path::new("target/man");
    fs::create_dir_all(out_dir)?;

    let cmd = Arguments::command()
        .name("btrfs")
        .disable_help_flag(true);
    clap_mangen::generate_to(cmd, out_dir)?;

    println!("man pages written to {}", out_dir.display());
    Ok(())
}
