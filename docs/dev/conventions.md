# Conventions

The goal is to write idiomatic Rust code that is consistent across the whole
codebase. btrfsutils spans several crates with different roles (kernel
interface wrappers, on-disk parsers, CLI tools) and each has its own patterns.
Following these conventions makes it easier to navigate unfamiliar code and to
understand what a function or type is responsible for at a glance.

Where possible, lean on the Rust ecosystem rather than reinventing things:
`uuid` for UUIDs, `bitflags` for flag sets, `nix` for syscalls and ioctls,
`anyhow` for error context in the CLI. This keeps the code readable to anyone
already familiar with those crates.

## Naming

Module names are usually generic nouns. For example, in the `uapi` crate,
the ioctl call wrappers are organized by the thing they operate on, and
live in modules like `filesystem`, `device`, `sync`. 

For the `btrfs-cli` crate, the module naming structure matches the subcommand
hierarchy. Meaning: the `btrfs subvolume create` command is implemented in
`cli/src/subvolume/create.rs`.

Types are named with the general concept first: `SysfsBtrfs`,
`BlockGroupFlags`, `BalanceArgs` — never `BtrfsSysfs`. 

Functions follow a `noun_verb` pattern: `label_get`, `label_set` — never
`get_label`. Ioctl wrapper functions match the lowercased C macro name:
`btrfs_ioc_balance_v2`.

Avoid abbreviations. For example, use `ChecksumType` instead of `CsumType`.

## Types

Always prefer proper typed values. For example, use `Uuid` from the `uuid`
crate, never `[u8; 16]`. In the CLI, if there is an argument that can take
one of multiple options, don't represent it as a string, but instead create
an enum and derive `clap::ValueEnum`.

Null-terminated kernel strings (labels, device paths) use `CString`/`CStr`.
Make sure that allocation and deallocation is handled properly.

File descriptors passed to uapi functions use `BorrowedFd`. 

Kernel flag fields use `bitflags!`, usually with a `Display` implementation so
they can be formatted with `{}`.

Complex argument structs (`BalanceArgs`, `DefragRangeArgs`) use the builder
pattern with `new()`, chained setters, and `Default`. 

Never expose bindgen types (`btrfs_ioctl_*`) in public uapi APIs, instead
create idiomatic Rust structs.

## Error handling

In `uapi/`, almost every function just performs a single syscall, so we return
the raw `nix::Result<T>`. Where possible, list potential error codes and their
meanings in the documentation comments.

Map specific errnos to `Option` or a typed
error at the call site where appropriate (`ENODEV` → `None`, etc.). 

In `cli/` and `mkfs/`, use `anyhow::Result<T>` and convert at the uapi boundary
with `.with_context()`. Always include the relevant path or resource in the
error message.

## Constants

All `BTRFS_*` constants are available via `crate::raw::*` in the `uapi` and
`disk` crates. Unless you have a good reason to, import from `crate::raw` and
don't define local copies. Size constants like `SZ_1M` that are not part of the
btrfs UAPI headers are the exception; define those locally with a comment.

There should not be any stray constants in the code. For example, use
`std::mem::offset_of!()` or `std::mem::size_of!()` macros to compute offsets
and sizes, and if there are any magic constants, give them a name.

Don't redefine things that are already defined in `crate::raw::*`.

## Style

Keep `unsafe` blocks as small as possible; non-trivial ones get a `// SAFETY:`
comment. For packed structs, copy fields to locals before taking references to
avoid misaligned reference UB. Use `escape_ascii()` when printing byte strings
that may be non-UTF-8. Import symbols used more than once rather than
qualifying them at every call site (single-use qualified paths are fine).

Shared CLI helpers live in `cli/src/util.rs`, these include utilities to format
sizes, bytes, time, and parse various types.

## Doc comments

In `uapi/`, module-level docs start with a `#` heading describing the module's
purpose. Function docs explain what the function does and why; the ioctl name is
a parenthetical in the implementation, not the primary description.

In `cli/`, don't put doc comments on subcommand enum variants — clap uses the
variant doc in preference to the struct doc, forcing duplication. Don't use
Markdown in clap struct doc comments: `wrap_help` reflows all text and destroys
formatting. Use plain prose paragraphs instead.
