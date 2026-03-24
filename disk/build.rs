use std::{env, path::PathBuf};

fn main() {
    let mut builder = bindgen::Builder::default()
        .header("src/raw/btrfs_tree.h")
        .parse_callbacks(Box::new(bindgen::CargoCallbacks::new()))
        .allowlist_file("src/raw/btrfs.h")
        .allowlist_file("src/raw/btrfs_tree.h");

    // On non-Linux platforms, use our portable linux/types.h shim.
    // On Linux the system header works fine.
    if env::var("CARGO_CFG_TARGET_OS").as_deref() != Ok("linux") {
        builder = builder.clang_arg("-Isrc/raw");
    }

    let bindings = builder.generate().expect("Unable to generate bindings");

    let out_path = PathBuf::from(env::var("OUT_DIR").unwrap());
    bindings
        .write_to_file(out_path.join("bindings.rs"))
        .expect("Couldn't write bindings!");
}
