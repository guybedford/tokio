//! Unix specific network types.
// This module does not currently provide any public API, but it was
// unintentionally defined as a public module. Hide it from the documentation
// instead of changing it to a private module to avoid breakage.
// Datagram AF_UNIX is unavailable on emscripten's node backend (stream-only),
// and `ucred` relies on `SO_PEERCRED`, also unsupported there.
#[doc(hidden)]
#[cfg(not(target_os = "emscripten"))]
pub mod datagram;

pub(crate) mod listener;

pub(crate) mod socket;

pub(crate) mod stream;
pub(crate) use stream::UnixStream;

mod split;
pub use split::{ReadHalf, WriteHalf};

mod split_owned;
pub use split_owned::{OwnedReadHalf, OwnedWriteHalf, ReuniteError};

mod socketaddr;
pub use socketaddr::SocketAddr;

#[cfg(not(target_os = "emscripten"))]
mod ucred;
#[cfg(not(target_os = "emscripten"))]
pub use ucred::UCred;

// FIFO pipes are mio-backed; not provided on emscripten.
#[cfg(not(target_os = "emscripten"))]
pub mod pipe;

/// A type representing user ID.
#[allow(non_camel_case_types)]
pub type uid_t = u32;

/// A type representing group ID.
#[allow(non_camel_case_types)]
pub type gid_t = u32;

/// A type representing process and process group IDs.
#[allow(non_camel_case_types)]
pub type pid_t = i32;
