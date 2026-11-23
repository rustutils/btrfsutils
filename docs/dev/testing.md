# Testing

The project aims for high test coverage to make sure the tools work correctly.

## Running tests

Many btrfs operations talk directly to the kernel and need elevated
privileges, which makes running the tests more involved.

`cargo test` runs all unprivileged tests. It still builds the privileged
tests but skips them.

```sh
cargo test
```

To run the privileged tests, use the `just test` target. It builds the tests
and runs only the test binaries (not `cargo` itself) under `sudo`. This is the
recommended way to run the full suite.

```sh
just test
```

The `coverage` target builds a coverage report of the full suite. It requires
`cargo-llvm-cov`.

```sh
just coverage
# open target/coverage/llvm-cov/html/index.html
```

## Static checks

Run `just check` before committing. It runs:

- the formatter check (nightly `rustfmt`)
- `cargo deny`
- `taplo` for `Cargo.toml` formatting
- `cargo doc` with `-Dwarnings`
- `cargo clippy --all-features`
- `cargo check` for each libc on the host architecture
- `cargo check` for the optional CLI features
- `cargo msrv verify` against each publishable crate's declared `rust-version`

Because it detects the host architecture, `just check` works on both x86_64
and aarch64. The musl check (`<host>-unknown-linux-musl`) needs a matching C
cross-compiler on PATH, because the `zstd-sys` and `lzo-sys` build scripts
compile C code:

- **Nix devshell** (`nix develop`) provides everything. You don't need any of
  the steps below.
- **Fedora aarch64**: `dnf install musl-gcc` installs `musl-gcc`, a thin
  wrapper around the host gcc with musl specs. `cc-rs` looks for the
  target-prefixed name `aarch64-linux-musl-gcc`, so create a symlink once:

  ```sh
  sudo ln -s /usr/bin/musl-gcc /usr/local/bin/aarch64-linux-musl-gcc
  ```

  To avoid touching `/usr/local/bin`, set
  `CC_aarch64_unknown_linux_musl=musl-gcc` and
  `AR_aarch64_unknown_linux_musl=ar` instead.
- **Debian and Ubuntu**: `apt install musl-tools` for the host architecture,
  or one of the `gcc-<arch>-linux-musl-cross` packages for cross builds. The
  same target-prefix handling applies if `cc-rs` doesn't find the compiler on
  its own.

If the cross C compiler isn't on PATH, `just check` prints
`skipping <triple> check: <prefix>-linux-musl-gcc not on PATH` and continues.
Only CI is expected to fail on a missing musl toolchain.

## Unit tests

Unit tests live in `#[cfg(test)] mod tests` blocks inside the module they
test. They need no privileges and run with `cargo test`.

They cover the pure logic across the crates:

- LE readers
- struct size assertions
- tree search cursor arithmetic
- stream parsing (all 22 v1 command types, CRC validation)
- superblock and B-tree node parsing
- size and time formatting
- argument parsing helpers
- balance filter parsing
- property classification

When adding a feature, add unit tests for any logic that doesn't need a real
kernel or filesystem.

## Integration tests

Integration tests live in `uapi/tests/` and `cli/tests/commands/` and are
marked:

```rust
#[ignore = "requires elevated privileges"]
```

`cargo test` skips them. They run only through `just test`.

### Fixture tests (`commands/fixture.rs`)

These are read-only snapshot tests against a pre-built filesystem image
(`cli/tests/commands/fixture.img.gz`). The image has a fixed UUID, label, and
subvolume layout, so the output is fully deterministic. They cover all
read-only commands:

- `filesystem df`, `show`, `usage`, `label`, and `du`
- `subvolume list` and `show`
- `device stats` and `usage`
- all `inspect-internal` commands
- `property get` and `list`

The `dump-tree` and `dump-super` tests read the image file directly without
mounting it, so they don't need elevated privileges even though they are part
of the privileged suite.

### Live tests (`commands/live.rs`)

These tests create and modify real btrfs filesystems on loopback devices. They
cover all mutating commands, including subvolume create, delete, and snapshot,
send and receive, scrub, balance, device add and remove, quota, qgroup, label
set, resize, defrag, and replace.

### Test helpers

`cli/tests/common.rs` provides RAII helpers that clean up on drop:

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

CLI output tests use [insta](https://insta.rs/) for snapshot testing.
Snapshots live in `cli/tests/snapshots/` and are checked in to the repository.

There are four snapshot categories:

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

When privileged tests run through `just test`, the Justfile sets
`INSTA_WORKSPACE_ROOT` so snapshots land in the right directory, and fixes the
ownership of any root-owned snapshot files afterwards.

### Adding tests for a new subcommand

1. **Argument parsing**: add cases to `cli/tests/arguments.rs` following the
   existing pattern.
2. **Help text**: `cli/tests/help.rs` finds all subcommands by walking the clap
   tree. No changes needed.
3. **Read-only output**: if the fixture image has suitable content, add
   snapshot tests to `commands/fixture.rs`.
4. **Mutating commands**: add tests to `commands/live.rs` using the RAII
   helpers.

Use the `snap!("description", output)` macro for snapshot tests. The
description appears in the snapshot file header.
