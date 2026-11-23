# How Commands Work

Every command is split across two layers: a safe kernel interface wrapper in
`btrfs-uapi` and a CLI command in `btrfs-cli`. This page walks through
`btrfs filesystem label` to show how the layers fit together and why they are
split.

## The uapi layer

The uapi layer lives in `uapi/src/`. It translates between Rust types and the
raw kernel interfaces. It allocates ioctl argument buffers, calls the ioctl,
and converts the result into something the rest of the code can use without
`unsafe` code or bindgen types.

For `btrfs filesystem label`, it looks like this (from `uapi/src/filesystem.rs`):

```rust
pub fn label_get(fd: BorrowedFd) -> nix::Result<CString> {
    let mut buf = [0i8; BTRFS_LABEL_SIZE as usize];
    unsafe { btrfs_ioc_get_fslabel(fd.as_raw_fd(), &mut buf) }?;
    let cstr = unsafe { CStr::from_ptr(buf.as_ptr()) };
    Ok(cstr.to_owned())
}

pub fn label_set(fd: BorrowedFd, label: &CStr) -> nix::Result<()> {
    let bytes = label.to_bytes();
    if bytes.len() >= BTRFS_LABEL_SIZE as usize {
        return Err(nix::errno::Errno::EINVAL);
    }
    let mut buf = [0i8; BTRFS_LABEL_SIZE as usize];
    for (i, &b) in bytes.iter().enumerate() {
        buf[i] = b as c_char;
    }
    unsafe { btrfs_ioc_set_fslabel(fd.as_raw_fd(), &buf) }?;
    Ok(())
}
```

The signatures use `BorrowedFd` rather than a raw integer, `CString` rather
than a byte array, and `nix::Result` rather than manual `errno` checks. The
caller never sees `btrfs_ioctl_*` types. The `unsafe` is confined to the ioctl
call. The logic around it is safe and testable.

## The cli layer

The CLI layer lives in `cli/src/`. It parses arguments, calls the uapi
function, and formats the output. It never calls ioctls directly.

The same command in `cli/src/filesystem/label.rs`:

```rust
#[derive(Parser, Debug)]
pub struct FilesystemLabelCommand {
    /// The device or mount point to operate on
    pub path: PathBuf,
    /// The new label to set (if omitted, the current label is printed)
    pub new_label: Option<OsString>,
}

impl Runnable for FilesystemLabelCommand {
    fn run(&self, _format: Format, _dry_run: bool) -> Result<()> {
        let file = open_path(&self.path)?;
        match &self.new_label {
            None => {
                let label = label_get(file.as_fd())
                    .with_context(|| format!("failed to get label for '{}'", self.path.display()))?;
                println!("{}", label.to_bytes().escape_ascii());
            }
            Some(new_label) => {
                let cstring = CString::new(new_label.as_bytes())
                    .context("label must not contain null bytes")?;
                label_set(file.as_fd(), &cstring)
                    .with_context(|| format!("failed to set label for '{}'", self.path.display()))?;
            }
        }
        Ok(())
    }
}
```

The struct derives clap's `Parser`, and the field doc comments become the help
text. `Runnable::run` handles both cases (get and set). It opens the path,
calls the matching uapi function, and either prints the result or reports an
error. Error messages include the path so the user knows which filesystem
failed.

## Why the split

Each layer stays focused and can be tested on its own. The uapi layer can be
tested with unit tests that mock the ioctl, or with integration tests on a real
filesystem, without any CLI code. The CLI layer can be tested with snapshot
tests for argument parsing and help text, which need no filesystem at all.

The split also keeps the library crates clean. `btrfs-uapi`, `btrfs-disk`, and
`btrfs-stream` contain no CLI logic and no GPL-derived code, so they can be
licensed MIT or Apache-2.0 and used by other projects without the CLI tools.

## Routing

Each top-level command group has a router in `cli/src/` (for example
`cli/src/filesystem.rs`). It defines an enum such as `FilesystemCommand` with
one variant per subcommand. The router's `Runnable` implementation matches on
the variant and calls the subcommand's own `run` method. To add a subcommand,
add a variant to the enum, a `mod` declaration, and a dispatch arm.
