# How Commands Work

Every command in btrfs-progrs is implemented across two layers: a safe kernel
interface wrapper in `btrfs-uapi`, and a CLI command in `btrfs-cli`. This page
walks through a concrete example — `btrfs filesystem label` — to show how the
two layers fit together and why the split exists.

## The uapi layer

The uapi layer lives in `uapi/src/`. Its job is to translate between Rust types
and the raw kernel interfaces — allocating ioctl argument buffers, calling the
ioctl, and converting the result into something the rest of the code can use
without touching any `unsafe` code or bindgen types.

For `btrfs filesystem label`, that looks like this (from `uapi/src/filesystem.rs`):

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

The function signatures use `BorrowedFd` rather than a raw integer, `CString`
rather than a byte array, and `nix::Result` rather than checking `errno` manually.
The caller never sees `btrfs_ioctl_*` types. The `unsafe` is contained to the
ioctl call itself, with surrounding logic that is safe and testable.

## The cli layer

The CLI layer lives in `cli/src/`. Its job is to parse arguments, call the uapi
function, and format the output. It never calls ioctls directly.

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

The struct derives `Parser` from clap — the field doc comments become the help
text. `Runnable::run` handles the two cases (get and set) by opening the path,
calling the appropriate uapi function, and either printing the result or reporting
an error. Error messages include the path so the user knows which filesystem
failed.

## Why the split

The separation keeps each layer focused and independently testable. The uapi
layer can be tested with unit tests that mock the ioctl, or with integration
tests that operate on a real filesystem, without any CLI machinery involved. The
CLI layer can be tested with argument parsing snapshot tests (no filesystem
needed at all) and help text snapshot tests.

It also keeps the library crates clean. Because `btrfs-uapi` and `btrfs-disk`
contain no CLI logic and no GPL-derived code, they can be licensed MIT/Apache-2.0
and used by other projects independently of the CLI tools.

## Routing

Each top-level command group has a router in `cli/src/` (e.g.
`cli/src/filesystem.rs`) that defines a `FilesystemCommand` enum with a variant
per subcommand. The `Runnable` implementation for the router matches on the
variant and delegates to the subcommand's own `run` method. Adding a new
subcommand means adding a variant to the enum, a `mod` declaration, and a `run`
dispatch arm.
