# Building from Source

## Prerequisites

You need the Rust toolchain named in `rust-toolchain.toml`. Running
`rustup toolchain install` in the project directory installs it. You also need
`clang` and `libclang` for bindgen, which generates Rust bindings from the
kernel UAPI headers at build time.

On Fedora or RHEL:

```sh
sudo dnf install clang
```

On Debian or Ubuntu:

```sh
sudo apt install clang libclang-dev
```

## Building with Cargo

```sh
cargo build --release
```

This builds `target/release/btrfs`, `target/release/btrfs-mkfs`,
`target/release/btrfs-tune`, and `target/release/btrfs-fuse`.

## Building with Nix

The Nix flake gives a reproducible build with all dependencies pinned:

```sh
nix build
```

Outputs land in `result/bin/` (`btrfs`, `btrfs-mkfs`, `btrfs-tune`, and
`btrfs-fuse`) and `result/share/man/man1/`.

To enter a development shell with all tools, including nightly rustfmt,
cargo-insta, and cargo-llvm-cov:

```sh
nix develop
```

To run the full lint sweep (`just check`) outside Nix, you may also need a
musl cross-compiler for the host architecture. See "Static checks" in the
[testing guide](../dev/testing.md) for setup.

## Cross-compiling release artifacts

`just package` cross-compiles statically linked release artifacts for all
supported architectures into `target/dist/`. For each of `x86_64`, `aarch64`,
and `riscv64gc` Linux musl, it produces:

- a zstd-compressed multicall binary
- a relocatable tarball
- a `.deb`
- a `.rpm`

It requires `cargo-zigbuild`, `cargo-deb`, `cargo-generate-rpm`, `zstd`, and
Zig 0.11 or later on `PATH`. The CI release pipeline uses the same recipe, so
a local run produces artifacts identical to a tagged CI build, apart from
timestamps.
