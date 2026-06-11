//! Emscripten platform support.
//!
//! Async Rust on emscripten runs on the host JS event loop. The public ambient
//! surface is [`event_loop`] (`schedule` / `drive`); this module also holds the
//! host glue for the driving kernel (in [`event_loop`]) and the FFI bindings,
//! including emscripten's promise API whose `promise_await` (suspending under
//! `-sJSPI`) lets `block_on` park the calling stack on the host loop.

#[cfg(feature = "rt")]
pub mod event_loop;

pub(crate) mod ffi;
