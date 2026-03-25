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
    cargo test -p btrfs-uapi --no-run

    # Then extract the binary paths from the JSON output.
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

    failed=0
    for binary in "${binaries[@]}"; do
        sudo "$binary" --ignored --test-threads=1 || failed=1
    done

    if [[ $failed -ne 0 ]]; then
        echo ""
        echo "error: one or more test binaries failed" >&2
        exit 1
    fi

format:
    cargo +nightly fmt --all
