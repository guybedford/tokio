//! FFI to emscripten's async primitives: timers, runtime keepalive, the
//! epoll readiness callback, and the `emscripten/promise.h` API whose
//! `promise_await` (suspending under `-sJSPI`) is tokio's park primitive.

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
    /// Which suspension mechanism the binary was linked with: 0 for none,
    /// 1 for asyncify (`-sASYNCIFY`), 2 for JSPI (`-sJSPI`).
    #[cfg(feature = "rt")]
    pub(crate) fn emscripten_has_asyncify() -> std::ffi::c_int;

    /// Schedule `cb` after `msecs`, returning a timer id for
    /// `emscripten_clear_timeout`. `user_data` is passed back to `cb`.
    #[cfg(feature = "rt")]
    pub(crate) fn emscripten_set_timeout(
        // `C-unwind` so a panic while driving inside the callback can unwind
        // through emscripten's dispatch instead of aborting at a `nounwind`
        // boundary.
        cb: Option<unsafe extern "C-unwind" fn(*mut std::ffi::c_void)>,
        msecs: f64,
        user_data: *mut std::ffi::c_void,
    ) -> i32;

    /// High-resolution time in ms since page load.
    #[cfg(feature = "rt")]
    pub(crate) fn emscripten_get_now() -> f64;

    /// Cancel a timer from `emscripten_set_timeout`.
    #[cfg(feature = "rt")]
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

    /// Arm a persistent readiness callback on an epoll fd (mio's reactor set,
    /// exposed via `AsRawFd`): the runtime invokes `callback` on a fresh host
    /// tick while the set has uncollected ready events, instead of the caller
    /// blocking in `epoll_wait`. Pure signal — the callback collects the events
    /// itself with a zero-timeout `epoll_wait`. A `None` callback disarms.
    /// Returns 0 or a positive errno.
    #[cfg(feature = "net")]
    pub(crate) fn emscripten_epoll_set_callback(
        epfd: std::ffi::c_int,
        // `C-unwind` for the same reason as the timer callback above.
        callback: Option<unsafe extern "C-unwind" fn(user_data: *mut std::ffi::c_void)>,
        user_data: *mut std::ffi::c_void,
    ) -> std::ffi::c_int;

    /// Current wasm heap size in bytes; used by tests to detect leaks.
    #[allow(dead_code)]
    pub(crate) fn emscripten_get_heap_size() -> usize;
}
