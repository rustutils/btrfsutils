# List available recipes
default:
    @just --list

# Build integration test binaries and run them with root privileges.
# Arguments are passed on to the test binaries. To only run a specific
# test, run `just test <test-name>`. To show standard output/error, use
# `just test --nocapture`.
#
# Requires: jq, sudo, btrfs-progs
test *ARGS:
    #!/usr/bin/env bash
    set -euo pipefail

    # clear all env variables that influence the btrfs command output
    # format.
    unset BTRFS_OUTPUT_FORMAT

    # Build first so the user sees compile progress/warnings on stderr.
    cargo test --all-features -- {{ARGS}}

    # Then extract the binary paths from the JSON output.
    mapfile -t binaries < <(
        cargo test \
            --all-features \
            --no-run \
            --message-format=json 2>/dev/null \
        | jq -r 'select(.profile.test == true) | .executable'
    )

    if [[ ${#binaries[@]} -eq 0 ]]; then
        echo "error: no test binaries found" >&2
        exit 1
    fi

    # --preserve-env=LLVM_PROFILE_FILE is a no-op when the var is unset,
    # but allows `just coverage` to forward it through sudo.
    export INSTA_WORKSPACE_ROOT="$PWD"
    failed=0
    for binary in "${binaries[@]}"; do
        sudo --preserve-env=LLVM_PROFILE_FILE,INSTA_WORKSPACE_ROOT \
            "$binary" --ignored --test-threads=1 {{ARGS}} || failed=1
    done

    # anything that we might write when running tests, change back to us.
    sudo chown -R "$(id -u):$(id -g)" \
        cli/tests/snapshots/ \
        cli/tests/commands/snapshots \
        target/test-fixtures

    if [[ $failed -ne 0 ]]; then
        echo ""
        echo "error: one or more test binaries failed" >&2
        exit 1
    fi

# Build instrumented test binaries, run them with root privileges, and
# generate an HTML coverage report.
#
# Requires: jq, sudo, btrfs-progs, cargo-llvm-cov
coverage:
    #!/usr/bin/env bash
    set -euo pipefail
    # ran into an issue where building coverage left instrumented binaries in target,
    # which would then spam .profraw files when I run regular tests, so we use a separate
    # target dir for coverage builds.
    export CARGO_TARGET_DIR=target/coverage
    eval "$(cargo llvm-cov show-env --sh --no-cfg-coverage)"
    cargo llvm-cov clean --workspace
    just test
    sudo chown -R "$(id -u):$(id -g)" target/coverage/
    cargo llvm-cov report --html

# Generate man pages and shell completions to target/gen/
gen:
    cargo run --package btrfs-gen

# run code formatter
format:
    cargo +nightly fmt --all
    taplo fmt

# run static linters
check:
    #!/usr/bin/env bash
    set -euo pipefail
    cargo +nightly fmt --all --check
    cargo deny check
    taplo check
    RUSTDOCFLAGS="-Dwarnings" cargo doc --no-deps
    cargo clippy --all-features -- -Dwarnings

    # Detect the host arch and check both libc variants. The musl
    # variant needs `<arch>-linux-musl-gcc` because zstd-sys / lzo-sys
    # build scripts invoke a C compiler; skip musl when that compiler
    # isn't on PATH so `just check` works on a stock developer machine.
    host_arch=$(uname -m)
    case "$host_arch" in
      x86_64)  triple_prefix=x86_64-unknown-linux  ;;
      aarch64) triple_prefix=aarch64-unknown-linux ;;
      *) echo "unsupported host arch: $host_arch" >&2; exit 1 ;;
    esac
    cargo check --target "${triple_prefix}-gnu"
    if command -v "${host_arch}-linux-musl-gcc" >/dev/null 2>&1; then
      cargo check --target "${triple_prefix}-musl"
    else
      echo "skipping ${triple_prefix}-musl check: ${host_arch}-linux-musl-gcc not on PATH"
    fi

    cargo check --features mkfs
    cargo check --features tune
    cargo check --features fuse

    # Verify each crate's declared `rust-version` actually compiles.
    # cargo-msrv's verify subcommand reads `rust-version` from each
    # workspace member's Cargo.toml and tries `cargo +<that-version>
    # check`. Skip dev-only / non-published crates (test-utils, gen).
    for member in disk uapi transaction stream mkfs cli tune fs fuse; do
      cargo msrv verify --manifest-path "${member}/Cargo.toml"
    done

# Build deb and rpm packages from the nix build output.
#
# Requires: nix, nfpm (available via `nix run nixpkgs#nfpm`)
package:
    #!/usr/bin/env bash
    set -euo pipefail
    nix build . -L
    version=$(cargo metadata --no-deps --format-version=1 | jq -r '.packages[] | select(.name == "btrfs-cli") | .version')
    mkdir -p target/package
    VERSION="$version" nix run nixpkgs#nfpm -- package --packager deb --target target/package/
    VERSION="$version" nix run nixpkgs#nfpm -- package --packager rpm --target target/package/
    echo ""
    ls -lh target/package/

branding:
  typst compile docs/branding/logo.typ docs/branding/logo.svg
  typst compile docs/branding/logo.typ docs/branding/logo.png --ppi 300
  typst compile docs/branding/banner.typ docs/branding/banner-dark.svg  --input theme=dark
  typst compile docs/branding/banner.typ docs/branding/banner-dark.png  --input theme=dark  --ppi 300
  typst compile docs/branding/banner.typ docs/branding/banner-light.svg --input theme=light
  typst compile docs/branding/banner.typ docs/branding/banner-light.png --input theme=light --ppi 300

alias fmt := format
alias lint := check
