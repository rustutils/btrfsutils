# Testing

## Running tests

```sh
just test
```

This runs unit tests (unprivileged, as your normal user) followed by integration
tests (privileged, via `sudo`). The integration tests build as your user and only
the test runner is executed with elevated privileges, so no `sudo cargo` is
involved.

For unit tests only:

```sh
cargo test
```

For a coverage report (requires `cargo-llvm-cov`):

```sh
just coverage
# open target/llvm-cov/html/index.html
```

## Unit tests

Unit tests live as `#[cfg(test)] mod tests` blocks within the module they test.
They require no privileges and run with `cargo test`.

Coverage spans all pure logic across the four crates: LE readers, struct size
assertions, tree search cursor arithmetic, stream parsing (all 22 v1 command
types, CRC validation), superblock parsing, B-tree node parsing, size/time
formatting, argument parsing helpers, balance filter parsing, and property
classification.

When adding a new feature, add unit tests for any logic that doesn't require a
real kernel or filesystem.

## Integration tests

Integration tests live in `uapi/tests/` and `cli/tests/commands/` and are marked:

```rust
#[ignore = "requires elevated privileges"]
```

They are skipped by `cargo test` and run only via `just test`.

### Fixture tests (`commands/fixture.rs`)

Read-only snapshot tests against a pre-built filesystem image
(`cli/tests/commands/fixture.img.gz`). The image has a fixed UUID, label, and
subvolume layout, so output is fully deterministic. These tests cover all
read-only commands: `filesystem df/show/usage/label/du`, `subvolume list/show`,
`device stats/usage`, all `inspect-internal` commands, and `property get/list`.

`dump-tree` and `dump-super` tests read the image file directly and do not
require mounting, so they run without elevated privileges even within the
privileged test suite.

### Live tests (`commands/live.rs`)

Tests that create and mutate real btrfs filesystems on loopback devices. These
cover all mutating commands: subvolume create/delete/snapshot, send/receive,
scrub, balance, device add/remove, quota, qgroup, label set, resize, defrag,
replace, and more.

### Test helpers

`cli/tests/common.rs` provides RAII helpers that clean up automatically on drop:

```
BackingFile → LoopbackDevice → Mount
```

Convenience functions:

| Function | Description |
|----------|-------------|
| `single_mount()` | 512 MiB single-device filesystem in a tempdir |
| `deterministic_mount()` | Same, with a fixed UUID and label |
| `fixture_mount()` | Mounts the pre-built fixture image read-only |
| `write_test_data(path, n)` | Write deterministic byte-pattern files |
| `verify_test_data(path, n)` | Verify previously written test data |

## Snapshot testing with insta

CLI output tests use [insta](https://insta.rs/) for snapshot testing. Snapshots
live in `cli/tests/snapshots/` and are checked in to the repository.

Three snapshot categories:

| Pattern | Privileges | Description |
|---------|-----------|-------------|
| `arguments__*.snap` | none | Argument parsing output |
| `help__*.snap` | none | Help text for every subcommand |
| `commands__fixture__*.snap` | root | Read-only CLI output (fixture image) |
| `commands__live__*.snap` | root | CLI output from live filesystem tests |

### Snapshot workflow

```sh
# Run tests; fails if any snapshot has changed:
cargo test

# Run tests and collect pending snapshot changes:
cargo insta test

# Interactively review each changed snapshot:
cargo insta review

# Accept all pending changes at once:
cargo insta accept --all
```

After running privileged tests via `just test`, the Justfile fixes ownership of
any root-owned snapshot files and sets `INSTA_WORKSPACE_ROOT` so snapshots land
in the right directory.

### Adding tests for a new subcommand

1. **Argument parsing**: add cases to `cli/tests/arguments.rs` following the
   existing pattern.
2. **Help text**: `cli/tests/help.rs` auto-discovers all subcommands by walking
   the clap tree — no changes needed.
3. **Read-only output**: if the fixture image has suitable content, add snapshot
   tests to `commands/fixture.rs`.
4. **Mutating commands**: add tests to `commands/live.rs` using the RAII helpers.

Use the `snap!("description", output)` macro for snapshot tests — the description
appears in the snapshot file header.
