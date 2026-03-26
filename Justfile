# List available recipes
default:
    @just --list

# Build integration test binaries and run them with root privileges.
#
# Requires: jq, sudo, btrfs-progs
test:
    #!/usr/bin/env bash
    set -euo pipefail

    # Build first so the user sees compile progress/warnings on stderr.
    cargo test

    # Then extract the binary paths from the JSON output.
    mapfile -t binaries < <(
        cargo test \
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
            "$binary" --ignored --test-threads=1 || failed=1
    done

    # new snapshots will be owned by root, so we fix that here.
    sudo chown -R "$(id -u):$(id -g)" cli/tests/snapshots/ cli/tests/commands/snapshots

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

# Generate man pages to target/man/
man:
    cargo run --package btrfs-mangen

format:
    cargo +nightly fmt --all

check:
    cargo +nightly fmt --all --check
    RUSTDOCFLAGS="-Dwarnings" cargo doc --no-deps
    #cargo clippy
