//! FFI to emscripten's async primitives: timers, runtime keepalive, socket
//! callbacks, and the `emscripten/promise.h` API whose `promise_await`
//! (suspending under `-sJSPI`) is tokio's park primitive.

/// Opaque `em_promise_t` handle.
#[cfg(feature = "rt")]
pub(crate) type EmPromise = *mut std::ffi::c_void;

/// `em_promise_result_t::EM_PROMISE_FULFILL`.
#[cfg(feature = "rt")]
pub(crate) const EM_PROMISE_FULFILL: std::ffi::c_int = 0;

/// `em_settled_result_t`: how the awaited promise settled.
#[cfg(feature = "rt")]
#[repr(C)]
pub(crate) struct EmSettledResult {
    pub(crate) result: std::ffi::c_int,
    pub(crate) value: *mut std::ffi::c_void,
}

extern "C" {
    /// Schedule `cb` after `msecs`, returning a timer id for
    /// `emscripten_clear_timeout`. `user_data` is passed back to `cb`.
    #[cfg(feature = "time")]
    pub(crate) fn emscripten_set_timeout(
        // `C-unwind` so a panic while driving inside the callback can unwind
        // through emscripten's dispatch instead of aborting at a `nounwind`
        // boundary.
        cb: Option<unsafe extern "C-unwind" fn(*mut std::ffi::c_void)>,
        msecs: f64,
        user_data: *mut std::ffi::c_void,
    ) -> i32;

    /// High-resolution time in ms since page load.
    #[cfg(feature = "time")]
    pub(crate) fn emscripten_get_now() -> f64;

    /// Cancel a timer from `emscripten_set_timeout`.
    #[cfg(feature = "time")]
    pub(crate) fn emscripten_clear_timeout(id: i32);

    /// Increment the keepalive counter; while non-zero, emscripten won't tear
    /// the runtime down when `main` returns, so async callbacks keep firing.
    #[cfg(feature = "rt")]
    pub(crate) fn emscripten_runtime_keepalive_push();

    /// Decrement the runtime keepalive counter.
    #[cfg(feature = "rt")]
    pub(crate) fn emscripten_runtime_keepalive_pop();

    /// Create a promise handle (`emscripten/promise.h`).
    #[cfg(feature = "rt")]
    pub(crate) fn emscripten_promise_create() -> EmPromise;

    /// Release a promise handle. Any armed callback referencing it must be
    /// cancelled first.
    #[cfg(feature = "rt")]
    pub(crate) fn emscripten_promise_destroy(promise: EmPromise);

    /// Settle a promise; `result` is an `em_promise_result_t`
    /// ([`EM_PROMISE_FULFILL`]). Settling an already-settled promise is a no-op
    /// (JS promise semantics).
    #[cfg(feature = "rt")]
    pub(crate) fn emscripten_promise_resolve(
        promise: EmPromise,
        result: std::ffi::c_int,
        value: *mut std::ffi::c_void,
    );

    /// Suspend the calling wasm stack until `promise` settles — tokio's park
    /// primitive. Requires linking with `-sJSPI`.
    #[cfg(feature = "rt")]
    pub(crate) fn emscripten_promise_await(promise: EmPromise) -> EmSettledResult;

    /// `epoll_create1(2)`: create an epoll fd. Not bound by `libc` for the
    /// emscripten target, so declared here (emscripten's libc exports it).
    #[cfg(feature = "net")]
    pub(crate) fn epoll_create1(flags: std::ffi::c_int) -> std::ffi::c_int;

    /// `epoll_ctl(2)`: add/modify/remove `fd` on the epoll set.
    #[cfg(feature = "net")]
    pub(crate) fn epoll_ctl(
        epfd: std::ffi::c_int,
        op: std::ffi::c_int,
        fd: std::ffi::c_int,
        event: *mut libc::epoll_event,
    ) -> std::ffi::c_int;

    /// `epoll_wait(2)`: drain ready events. The reactor only ever calls it with
    /// `timeout == 0` (a non-blocking probe — emscripten routes a zero timeout
    /// through a plain non-suspending syscall, callable on a host-callback
    /// frame; a blocking wait would need JSPI/ASYNCIFY).
    #[cfg(feature = "net")]
    pub(crate) fn epoll_wait(
        epfd: std::ffi::c_int,
        events: *mut libc::epoll_event,
        maxevents: std::ffi::c_int,
        timeout: std::ffi::c_int,
    ) -> std::ffi::c_int;

    /// Register a persistent, non-blocking readiness callback on an epoll fd
    /// (`emscripten/emscripten.h`): instead of blocking in `epoll_wait`, the
    /// runtime delivers up to `maxevents` ready events to `callback` on a fresh
    /// host tick whenever the set makes progress. Armed once and reused; a `None`
    /// callback unregisters. Needs no JSPI/ASYNCIFY. The reactor's wake source.
    #[cfg(feature = "net")]
    pub(crate) fn emscripten_epoll_set_callback(
        epfd: std::ffi::c_int,
        maxevents: std::ffi::c_int,
        callback: Option<
            unsafe extern "C-unwind" fn(
                epfd: std::ffi::c_int,
                events: *mut libc::epoll_event,
                nready: std::ffi::c_int,
                userdata: *mut std::ffi::c_void,
            ),
        >,
        userdata: *mut std::ffi::c_void,
    ) -> std::ffi::c_int;

    /// Current wasm heap size in bytes; used by tests to detect leaks.
    #[allow(dead_code)]
    pub(crate) fn emscripten_get_heap_size() -> usize;
}
