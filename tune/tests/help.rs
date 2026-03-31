use btrfs_tune::args::Arguments;
use clap::CommandFactory;

#[test]
fn help() {
    let mut cmd = Arguments::command().term_width(80);
    let text = cmd.render_long_help().to_string();
    insta::assert_snapshot!("btrfs_tune", text);
}
