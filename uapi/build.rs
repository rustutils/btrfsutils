use std::{env, path::PathBuf};

fn main() {
    let btrfs_bindings = bindgen::Builder::default()
        .header("src/raw/btrfs_tree.h")
        .parse_callbacks(Box::new(bindgen::CargoCallbacks::new()))
        .allowlist_file("src/raw/btrfs.h")
        .allowlist_file("src/raw/btrfs_tree.h")
        .generate()
        .expect("Unable to generate bindings");

    let out_path = PathBuf::from(env::var("OUT_DIR").unwrap());
    btrfs_bindings
        .write_to_file(out_path.join("bindings.rs"))
        .expect("Couldn't write bindings!");
}
