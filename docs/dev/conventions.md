# Conventions

The goal is idiomatic Rust that is consistent across the codebase. The crates
have different roles (kernel interface wrappers, on-disk parsers, CLI tools),
and each has its own patterns. Following these conventions makes unfamiliar
code easier to navigate and makes it clear at a glance what a function or type
is responsible for.

Use the Rust ecosystem rather than reinventing things: `uuid` for UUIDs,
`bitflags` for flag sets, `nix` for syscalls and ioctls, `anyhow` for error
context in the CLI. This keeps the code readable to anyone who knows those
crates.

## Naming

Module names are usually generic nouns. In the `uapi` crate, for example, the
ioctl wrappers are grouped by what they operate on, in modules like
`filesystem`, `device`, and `sync`.

In `btrfs-cli`, the module structure matches the subcommand hierarchy. For
example, `btrfs subvolume create` is implemented in
`cli/src/subvolume/create.rs`.

Types put the general concept first: `SysfsBtrfs`, `BlockGroupFlags`,
`BalanceArgs`. Never `BtrfsSysfs`.

Functions follow a `noun_verb` pattern: `label_get`, `label_set`. Never
`get_label`. Ioctl wrappers match the lowercased C macro name:
`btrfs_ioc_balance_v2`.

Avoid abbreviations. For example, use `ChecksumType` instead of `CsumType`.

## Types

Always prefer typed values. For example, use `Uuid` from the `uuid` crate,
never `[u8; 16]`. In the CLI, an argument that takes one of several options
should be an enum deriving `clap::ValueEnum`, not a string.

Null-terminated kernel strings (labels, device paths) use `CString` and
`CStr`. Make sure allocation and deallocation are handled correctly.

File descriptors passed to uapi functions use `BorrowedFd`.

Kernel flag fields use `bitflags!`. Display formatting for them belongs in the
CLI crate, not on the type, because different commands format the same flags
differently.

Complex argument structs (`BalanceArgs`, `DefragRangeArgs`) use the builder
pattern with `new()`, chained setters, and `Default`.

Never expose bindgen types (`btrfs_ioctl_*`) in public uapi APIs. Create
idiomatic Rust structs instead.

## Error handling

In `uapi/`, most functions perform a single syscall, so they return the raw
`nix::Result<T>`. Where possible, list the possible error codes and their
meanings in the doc comments.

Map specific errnos to `Option` or a typed error at the call site where it
makes sense (`ENODEV` → `None`, etc.).

In `cli/`, `mkfs/`, and `tune/`, use `anyhow::Result<T>` and convert at the
uapi boundary with `.with_context()`. Always include the relevant path or
resource in the error message.

## Constants

All `BTRFS_*` constants are available through `crate::raw::*` in the `uapi`
and `disk` crates. Import them from `crate::raw` and don't define local copies
unless you have a good reason. Size constants like `SZ_1M` that are not part
of the btrfs UAPI headers are the exception. Define those locally with a
comment.

Avoid stray constants. Use `std::mem::offset_of!()` or `std::mem::size_of()`
to compute offsets and sizes, and give any remaining magic numbers a name.

Don't redefine anything that already exists in `crate::raw::*`.

## Parsing on-disk structures

In `disk/` and `mkfs/`, use `bytes::Buf` to read and `bytes::BufMut` to write
on-disk fields. Sequential `get_u64_le()` and `put_u64_le()` calls advance the
cursor, so there is no manual offset arithmetic. See [Parsing](parsing.md) for
details.

In `uapi/`, tree search callbacks receive the raw item payload. Parse it with
the typed parsers in `btrfs_disk::items` (for example `ChunkItem::parse` or
`RootItem::parse`) rather than reading fields at hand-written offsets.

## Style

Keep `unsafe` blocks as small as possible. Non-trivial ones get a
`// SAFETY:` comment. For packed structs, copy fields to locals before taking
references, to avoid misaligned reference UB. Use `escape_ascii()` when
printing byte strings that may not be UTF-8. Import symbols used more than once
rather than qualifying them at every call site. Single-use qualified paths are
fine.

Shared CLI helpers live in `cli/src/util.rs`. They format sizes, bytes, and
times, and parse various argument types.

## Doc comments

In `uapi/`, module-level docs start with a `#` heading that describes the
module's purpose. Function docs explain what the function does and why. The
ioctl name goes in a parenthetical or in the implementation, not in the main
description.

In `cli/`, don't put doc comments on subcommand enum variants. Clap prefers the
variant doc over the struct doc, which forces duplication. Don't use Markdown
in clap struct doc comments: `wrap_help` reflows all text and destroys the
formatting. Use plain prose paragraphs instead.
