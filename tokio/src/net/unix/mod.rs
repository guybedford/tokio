//! Unix specific network types.
// This module does not currently provide any public API, but it was
// unintentionally defined as a public module. Hide it from the documentation
// instead of changing it to a private module to avoid breakage.
// mio/socket2-backed socket types (every Unix target except emscripten).
#[cfg(not(target_os = "emscripten"))]
#[doc(hidden)]
pub mod datagram;

#[cfg(not(target_os = "emscripten"))]
pub(crate) mod listener;

#[cfg(not(target_os = "emscripten"))]
pub(crate) mod socket;

#[cfg(not(target_os = "emscripten"))]
pub(crate) mod stream;
#[cfg(not(target_os = "emscripten"))]
pub(crate) use stream::UnixStream;

// Reactor-backed socket types on emscripten (no mio); same public API.
#[cfg(target_os = "emscripten")]
pub(crate) mod emscripten;

mod split;
pub use split::{ReadHalf, WriteHalf};

mod split_owned;
pub use split_owned::{OwnedReadHalf, OwnedWriteHalf, ReuniteError};

mod socketaddr;
pub use socketaddr::SocketAddr;

mod ucred;
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
