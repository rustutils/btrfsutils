#![doc = include_str!("../README.md")]

mod stream;

#[cfg(feature = "receive")]
mod receive;

pub use stream::{StreamCommand, StreamReader, Timespec};

#[cfg(feature = "receive")]
pub use receive::ReceiveContext;
