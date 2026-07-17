//! FFI to emscripten's host-loop primitives (`guybedford/emscripten` cf
//! fork for the epoll callback): the signals and timers behind the net
//! driver turn and the hosted event-loop runtimes.

extern "C" {
    /// Arm a persistent readiness callback on an epoll fd (mio's reactor set,
    /// exposed via `AsRawFd`): the runtime invokes `callback` on a fresh host
    /// tick while the set has uncollected ready events, instead of the caller
    /// blocking in `epoll_wait`. Pure signal — the events are collected with a
    /// zero-timeout `epoll_wait`. A `None` callback disarms. Returns 0 or a
    /// positive errno.
    #[cfg(feature = "net")]
    pub(crate) fn emscripten_epoll_set_callback(
        epfd: std::ffi::c_int,
        // `C-unwind` so a panic can unwind through emscripten's dispatch
        // instead of aborting at a `nounwind` boundary.
        callback: Option<unsafe extern "C-unwind" fn(user_data: *mut std::ffi::c_void)>,
        user_data: *mut std::ffi::c_void,
    ) -> std::ffi::c_int;

    /// Schedule `cb` after `msecs`, returning a timer id for
    /// `emscripten_clear_timeout`. `user_data` is passed back to `cb`. Its
    /// keepalive accounting is internal and `EXIT_RUNTIME`-dependent —
    /// callers needing liveness must hold their own refs.
    #[cfg(all(tokio_unstable, feature = "rt"))]
    pub(crate) fn emscripten_set_timeout(
        cb: Option<unsafe extern "C-unwind" fn(*mut std::ffi::c_void)>,
        msecs: f64,
        user_data: *mut std::ffi::c_void,
    ) -> i32;

    /// Cancel a timer from `emscripten_set_timeout`.
    #[cfg(all(tokio_unstable, feature = "rt"))]
    pub(crate) fn emscripten_clear_timeout(id: i32);

    /// Increment the keepalive counter; while non-zero, emscripten won't tear
    /// the runtime down when `main` returns, so async callbacks keep firing.
    #[cfg(all(tokio_unstable, feature = "rt"))]
    pub(crate) fn emscripten_runtime_keepalive_push();

    /// Decrement the runtime keepalive counter.
    #[cfg(all(tokio_unstable, feature = "rt"))]
    pub(crate) fn emscripten_runtime_keepalive_pop();
}
