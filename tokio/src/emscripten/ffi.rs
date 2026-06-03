//! FFI to emscripten's async primitives (non-blocking, no ASYNCIFY) plus the
//! tokio-specific `__tokio_emscripten_*` library defined in `worker.js`.

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

    /// Force-exit the runtime even if there are outstanding async callbacks.
    #[cfg(feature = "rt")]
    pub(crate) fn emscripten_force_exit(status: i32) -> !;

    /// Spawn a Node `worker_threads.Worker`, instantiate the same wasm factory
    /// in it, invoke table index `fn_index`, and block via `Atomics.wait` until
    /// it completes. Writes the outcome into `outcome_out` and returns its
    /// status. Implemented in `worker.js`.
    #[cfg(feature = "rt")]
    pub(crate) fn __tokio_emscripten_block_in_worker(
        fn_index: i32,
        outcome_out: *mut u8,
        outcome_capacity: i32,
    ) -> i32;

    /// Worker-side: on success, write status=0 to the parent's SAB and notify.
    #[cfg(feature = "rt")]
    pub(crate) fn __tokio_emscripten_worker_notify_done(status: i32);

    /// Worker-side: report failure with a UTF-8 message into the SAB and notify.
    /// `message_ptr`/`message_len` are read synchronously by the JS side.
    #[cfg(feature = "rt")]
    pub(crate) fn __tokio_emscripten_worker_notify_failure(
        status: i32,
        message_ptr: *const u8,
        message_len: i32,
    );

    /// Synchronous `debugger;` trampoline: pauses DevTools when attached, else
    /// a no-op. Only referenced from debug builds of the worker shim.
    #[cfg(all(feature = "rt", debug_assertions))]
    pub(crate) fn __tokio_emscripten_debugger();

    /// Global socket *readable* handler (data arrived); `None` deregisters. The
    /// reactor's "now readable" signal.
    #[cfg(feature = "net")]
    #[allow(dead_code)]
    pub(crate) fn emscripten_set_socket_message_callback(
        user_data: *mut std::ffi::c_void,
        callback: Option<unsafe extern "C-unwind" fn(fd: i32, user_data: *mut std::ffi::c_void)>,
    );

    /// Global handler for outgoing-connection completion (writable/connected).
    #[cfg(feature = "net")]
    #[allow(dead_code)]
    pub(crate) fn emscripten_set_socket_open_callback(
        user_data: *mut std::ffi::c_void,
        callback: Option<unsafe extern "C-unwind" fn(fd: i32, user_data: *mut std::ffi::c_void)>,
    );

    /// Global handler for a listener with an incoming connection (listener
    /// read-readiness).
    #[cfg(feature = "net")]
    #[allow(dead_code)]
    pub(crate) fn emscripten_set_socket_connection_callback(
        user_data: *mut std::ffi::c_void,
        callback: Option<unsafe extern "C-unwind" fn(fd: i32, user_data: *mut std::ffi::c_void)>,
    );

    /// Global handler for peer close (EOF / read+write closed).
    #[cfg(feature = "net")]
    #[allow(dead_code)]
    pub(crate) fn emscripten_set_socket_close_callback(
        user_data: *mut std::ffi::c_void,
        callback: Option<unsafe extern "C-unwind" fn(fd: i32, user_data: *mut std::ffi::c_void)>,
    );

    /// Global handler for socket errors: fd, errno, and a UTF-8 message valid
    /// only for the call.
    #[cfg(feature = "net")]
    #[allow(dead_code)]
    pub(crate) fn emscripten_set_socket_error_callback(
        user_data: *mut std::ffi::c_void,
        callback: Option<
            unsafe extern "C-unwind" fn(
                fd: i32,
                err: i32,
                msg: *const std::ffi::c_char,
                user_data: *mut std::ffi::c_void,
            ),
        >,
    );

    /// Current wasm heap size in bytes; used by tests to detect leaks.
    #[allow(dead_code)]
    pub(crate) fn emscripten_get_heap_size() -> usize;
}
