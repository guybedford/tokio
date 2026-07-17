//! FFI to emscripten's epoll readiness callback (`guybedford/emscripten` cf
//! fork): the signal that resolves the net driver's suspended driver turn.

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
}
