//! Emscripten FFI bindings: timers, runtime keepalive, the epoll readiness
//! callback, and the `emscripten/promise.h` API whose `promise_await`
//! (suspending under `-sJSPI`) is tokio's park primitive. The runtime-facing
//! host glue lives in `crate::runtime::jspi`.

pub(crate) mod ffi;
