//! Emscripten platform support.
//!
//! Async Rust on emscripten runs on the host JS event loop, no ASYNCIFY/JSPI.
//! The public ambient surface is [`event_loop`] (`schedule` / `drive`); this
//! module also holds the driving kernel (in [`event_loop`]), FFI bindings, the
//! `#[tokio::test]` worker harness, and unbuffered debug.

#[cfg(feature = "rt")]
pub mod event_loop;

pub(crate) mod ffi;

// The `#[tokio::test]` worker harness; its entry points are re-exported (renamed)
// through the doc-hidden `crate::macros::support`, like `select!`/`join!` reach
// their helpers.
#[cfg(feature = "rt")]
pub(crate) mod test_worker;
