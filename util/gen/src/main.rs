use clap::CommandFactory;
use clap_complete::Shell;
use std::{env, fs, io, path::PathBuf};

fn main() -> io::Result<()> {
    let out_dir = env::args_os()
        .nth(1)
        .map_or_else(|| PathBuf::from("target/gen"), PathBuf::from);

    let man_dir = out_dir.join("man");
    let completions_dir = out_dir.join("completions");
    fs::create_dir_all(&man_dir)?;
    fs::create_dir_all(&completions_dir)?;

    let btrfs = btrfs_cli::Arguments::command().name("btrfs");
    let mkfs = btrfs_mkfs::args::Arguments::command().name("mkfs.btrfs");
    let tune = btrfs_tune::args::Arguments::command().name("btrfs-tune");

    // Man pages.
    clap_mangen::generate_to(btrfs.clone(), &man_dir)?;
    clap_mangen::generate_to(mkfs.clone(), &man_dir)?;
    clap_mangen::generate_to(tune, &man_dir)?;
    println!("man pages written to {}", man_dir.display());

    // Shell completions.
    for shell in [Shell::Bash, Shell::Zsh, Shell::Fish, Shell::Elvish] {
        clap_complete::generate_to(
            shell,
            &mut btrfs.clone(),
            "btrfs",
            &completions_dir,
        )?;
        clap_complete::generate_to(
            shell,
            &mut mkfs.clone(),
            "mkfs.btrfs",
            &completions_dir,
        )?;
    }
    println!("completions written to {}", completions_dir.display());

    Ok(())
}
