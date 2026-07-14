//! Emscripten FFI bindings: timers, runtime keepalive, the epoll readiness
//! callback, and the `emscripten/promise.h` API whose `promise_await`
//! (suspending under `-sJSPI`) is tokio's park primitive. The runtime-facing
//! host glue lives in `crate::runtime::hosted`; the public surface lives on
//! [`HostedRuntime`](crate::runtime::HostedRuntime) (`schedule` / `drive`).

pub(crate) mod ffi;
