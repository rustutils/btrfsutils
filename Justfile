# List available recipes
default:
    @just --list

# Run unit tests, then integration tests as root. Arguments go to the
# test binaries, e.g. `just test <test-name>` or `just test --nocapture`.
#
# Requires: jq, sudo, btrfs-progs
test *ARGS:
    #!/usr/bin/env bash
    set -euo pipefail

    # Clear env variables that change btrfs output format.
    unset BTRFS_OUTPUT_FORMAT

    # Build first so compile progress and warnings show on stderr.
    cargo test --all-features -- {{ ARGS }}

    # Collect the test binary paths.
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

    # Lets `just coverage` pass LLVM_PROFILE_FILE through sudo. No-op
    # when unset.
    export INSTA_WORKSPACE_ROOT="$PWD"
    failed=0
    for binary in "${binaries[@]}"; do
        sudo --preserve-env=LLVM_PROFILE_FILE,INSTA_WORKSPACE_ROOT \
            "$binary" --ignored --test-threads=1 {{ ARGS }} || failed=1
    done

    # Give files written as root back to the user.
    sudo chown -R "$(id -u):$(id -g)" \
        cli/tests/snapshots/ \
        cli/tests/commands/snapshots \
        target/test-fixtures

    if [[ $failed -ne 0 ]]; then
        echo ""
        echo "error: one or more test binaries failed" >&2
        exit 1
    fi

# Run all tests with coverage instrumentation and write an HTML report.
#
# Requires: jq, sudo, btrfs-progs, cargo-llvm-cov
coverage:
    #!/usr/bin/env bash
    set -euo pipefail
    # Separate target dir, so instrumented binaries don't leave .profraw
    # files during regular test runs.
    export CARGO_TARGET_DIR=target/coverage
    eval "$(cargo llvm-cov show-env --sh --no-cfg-coverage)"
    cargo llvm-cov clean --workspace
    just test
    sudo chown -R "$(id -u):$(id -g)" target/coverage/
    cargo llvm-cov report --html

# Generate man pages and shell completions to target/gen/
gen:
    cargo run --package btrfs-gen

# Format code
format:
    cargo +nightly fmt --all
    taplo fmt

# Run static checks
check:
    #!/usr/bin/env bash
    set -euo pipefail
    cargo +nightly fmt --all --check
    cargo deny check
    taplo check
    RUSTDOCFLAGS="-Dwarnings" cargo doc --no-deps
    cargo +stable clippy --all-features -- -Dwarnings

    # Check both libc variants for the host arch. musl needs
    # `<arch>-linux-musl-gcc` for the zstd-sys and lzo-sys build
    # scripts, so skip it when that compiler isn't on PATH.
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

    # Verify each published crate builds with its declared
    # `rust-version`. Skips test-utils and gen.
    for member in disk uapi transaction stream mkfs cli tune fs fuse; do
      cargo msrv verify --manifest-path "${member}/Cargo.toml"
    done

# Cross-compile static musl release artifacts into target/dist/. Per
# arch:
#   - btrfs-<arch>.zst          (multicall binary, zstd-compressed)
#   - btrfsutils-<arch>.tar.zst (binary, symlinks, docs)
#   - btrfsutils_<ver>_<arch>.deb
#   - btrfsutils-<ver>-1.<arch>.rpm
#
# Requires: cargo-zigbuild, cargo-deb, cargo-generate-rpm, zstd
package:
    #!/usr/bin/env bash
    set -euo pipefail

    rm -rf target/dist
    mkdir -p target/dist

    # Generate man pages and completions once into target/gen/. Each
    # arch gets a copy in target/<triple>/gen/ for the packagers.
    cargo run --quiet --package btrfs-gen

    for arch in x86_64 aarch64 riscv64gc; do
      target="$arch-unknown-linux-musl"

      # Static multicall binary with all features.
      cargo zigbuild \
        --release \
        --target "$target" \
        --package btrfs-cli \
        --features mkfs,tune,fuse,multicall

      # Symlinks for cargo-deb to install into /usr/bin/.
      rm -rf "target/$target/symlinks"
      mkdir -p "target/$target/symlinks"
      for tool in mkfs.btrfs btrfs-mkfs btrfstune btrfs-tune; do
        ln -s ./btrfs "target/$target/symlinks/$tool"
      done

      # cargo-deb `--target` rewrites target/release/ to
      # target/<triple>/release/, so gen/ must exist there. Copied, not
      # symlinked, because the assets list expects plain files.
      rm -rf "target/$target/gen"
      cp -r target/gen "target/$target/gen"

      # .deb (cargo-deb takes the crate name)
      cargo deb \
        --package btrfs-cli \
        --target "$target" \
        --no-build \
        --output target/dist/

      # .rpm (cargo-generate-rpm takes the manifest directory, not the
      # crate name)
      cargo generate-rpm \
        --package cli \
        --target "$target" \
        --output target/dist/

      # Standalone compressed binary.
      cp "target/$target/release/btrfs" "target/dist/btrfs-$arch"
      zstd --rm "target/dist/btrfs-$arch"

      # Tarball with bin/btrfs, symlinks, LICENSE, README, and
      # CHANGELOG. Extracts into `btrfsutils-<arch>/`.
      stage="target/dist/btrfsutils-$arch"
      rm -rf "$stage"
      mkdir -p "$stage/bin"
      install -m 0755 "target/$target/release/btrfs" "$stage/bin/"
      cp -P "target/$target/symlinks/"* "$stage/bin/"
      install -m 0644 README.md "$stage/README.md"
      install -m 0644 LICENSE.md "$stage/LICENSE.md"
      install -m 0644 CHANGELOG.md "$stage/CHANGELOG.md"
      tar -C target/dist -cf "target/dist/btrfsutils-$arch.tar" "btrfsutils-$arch"
      rm -rf "$stage"
      zstd --rm "target/dist/btrfsutils-$arch.tar"
    done

    echo ""
    ls -lh target/dist/

branding:
    typst compile docs/branding/logo.typ docs/branding/logo.svg
    typst compile docs/branding/logo.typ docs/branding/logo.png --ppi 300
    typst compile docs/branding/banner.typ docs/branding/banner-dark.svg  --input theme=dark
    typst compile docs/branding/banner.typ docs/branding/banner-dark.png  --input theme=dark  --ppi 300
    typst compile docs/branding/banner.typ docs/branding/banner-light.svg --input theme=light
    typst compile docs/branding/banner.typ docs/branding/banner-light.png --input theme=light --ppi 300

alias fmt := format
alias lint := check
