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

# Cross-compile static musl release artifacts into target/dist/ for
# every supported architecture. For each arch, produces:
#   - btrfs-<arch>.zst         (raw multicall binary, zstd-compressed)
#   - btrfsutils-<arch>.tar.zst (tarball: bin/btrfs + symlinks + docs)
#   - btrfsutils_<ver>_<arch>.deb
#   - btrfsutils-<ver>-1.<arch>.rpm
#
# Requires: cargo-zigbuild, cargo-deb, cargo-generate-rpm, zstd
package:
    #!/usr/bin/env bash
    set -euo pipefail

    rm -rf target/dist
    mkdir -p target/dist

    # Generate man pages and shell completions once (arch-independent).
    # The output goes to target/gen/ which is arch-shared; the just
    # script copies it into target/<triple>/gen/ per arch so the
    # cargo-deb / cargo-generate-rpm path substitution finds it.
    cargo run --quiet --package btrfs-gen

    for arch in x86_64 aarch64 riscv64gc; do
      target="$arch-unknown-linux-musl"

      # Build the multicall btrfs binary statically with all features.
      cargo zigbuild \
        --release \
        --target "$target" \
        --package btrfs-cli \
        --features mkfs,tune,fuse,multicall

      # Per-target symlinks dir for cargo-deb to scoop into /usr/bin/.
      rm -rf "target/$target/symlinks"
      mkdir -p "target/$target/symlinks"
      for tool in mkfs.btrfs btrfs-mkfs btrfstune btrfs-tune; do
        ln -s ./btrfs "target/$target/symlinks/$tool"
      done

      # Per-target gen/ dir so the cargo-deb `--target` path
      # substitution (`target/release/` → `target/<triple>/release/`)
      # resolves to a real path. cp instead of ln -s because cargo-deb
      # follows symlinks but the assets list expects plain files.
      rm -rf "target/$target/gen"
      cp -r target/gen "target/$target/gen"

      # .deb (cargo-deb takes the cargo crate name)
      cargo deb \
        --package btrfs-cli \
        --target "$target" \
        --no-build \
        --output target/dist/

      # .rpm (cargo-generate-rpm takes the manifest directory name,
      # not the crate name — the two only line up by accident in
      # most projects)
      cargo generate-rpm \
        --package cli \
        --target "$target" \
        --output target/dist/

      # Standalone compressed binary.
      cp "target/$target/release/btrfs" "target/dist/btrfs-$arch"
      zstd --rm "target/dist/btrfs-$arch"

      # Tarball: bin/btrfs + multicall symlinks + LICENSE + README +
      # CHANGELOG. Staged into a per-arch directory so the tarball
      # extracts cleanly into `btrfsutils-<arch>/`.
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
