# Building from Source

## Prerequisites

You need a Rust toolchain matching the version in `rust-toolchain.toml` — running
`rustup toolchain install` in the project directory will pick it up automatically.
You also need `clang` and `libclang` for bindgen, which generates Rust bindings
from the kernel UAPI headers at build time.

On Fedora/RHEL:

```sh
sudo dnf install clang
```

On Debian/Ubuntu:

```sh
sudo apt install clang libclang-dev
```

## Building with Cargo

```sh
cargo build --release
```

The resulting binaries are `target/release/btrfs`, `target/release/btrfs-mkfs`,
and `target/release/btrfs-tune`.

## Building with Nix

The project includes a Nix flake that provides a fully reproducible build with
all dependencies pinned:

```sh
nix build
```

Outputs land in `result/bin/btrfs`, `result/bin/btrfs-mkfs`,
`result/bin/btrfs-tune`, and `result/share/man/man1/`.

To enter a development shell with all tools available (including nightly rustfmt,
cargo-insta, and cargo-llvm-cov):

```sh
nix develop
```

Contributors who want to run the full lint sweep (`just check`) on a
non-Nix machine may also need a host-arch musl cross-compiler — see
the "Static checks" section of the
[testing guide](../dev/testing.md) for setup instructions.
