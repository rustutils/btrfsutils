# List available recipes
default:
    @just --list

# Run unit tests (no privileges required)
test:
    cargo test --workspace

# Build integration test binaries and run them with root privileges.
#
# Requires: jq, sudo, btrfs-progs
test-priviledged:
    #!/usr/bin/env bash
    set -euo pipefail

    echo "--- Building test binaries ---"
    mapfile -t binaries < <(
        cargo test -p btrfs-uapi \
            --no-run \
            --message-format=json 2>/dev/null \
        | jq -r 'select(.profile.test == true) | .executable'
    )

    if [[ ${#binaries[@]} -eq 0 ]]; then
        echo "error: no test binaries found" >&2
        exit 1
    fi

    echo "--- Running as root ---"
    failed=0
    for binary in "${binaries[@]}"; do
        echo ""
        echo "--- $binary ---"
        sudo "$binary" --ignored --test-threads=1 || failed=1
    done

    if [[ $failed -ne 0 ]]; then
        echo ""
        echo "error: one or more test binaries failed" >&2
        exit 1
    fi

format:
    cargo +nightly fmt --all
