//! A JavaScript exception thrown by an import during a hosted drive unwinds
//! the Rust frames, which `catch_unwind` does not catch. From a microtask
//! drive the host promise would take it as a rejection and the task would
//! stay running with nothing left to wake it; it must abort instead.
//!
//! `harness = false`, so `main` can return into the host loop; the binary
//! passes only by aborting (`Module.tokioExpectAbort`, see
//! `rt_emscripten_pre.js`).

#[cfg(all(
    target_os = "emscripten",
    not(target_feature = "atomics"),
    tokio_unstable,
    feature = "rt"
))]
mod emscripten {
    use tokio::runtime::Builder;

    extern "C" {
        fn emscripten_run_script(script: *const std::ffi::c_char);
    }

    fn run_js(script: &str) {
        let script = std::ffi::CString::new(script).unwrap();
        // SAFETY: a NUL-terminated script evaluated on the host.
        unsafe { emscripten_run_script(script.as_ptr()) }
    }

    pub(super) fn main() {
        run_js("Module.tokioExpectAbort = true");
        let el = Box::leak(Box::new(
            Builder::new_current_thread()
                .build_hosted_local_event_loop(Default::default())
                .unwrap(),
        ));
        // The spawn's wake schedules a microtask drive.
        el.spawn_local(async {
            run_js("throw new Error('import threw during a drive')");
        });
    }
}

fn main() {
    #[cfg(all(
        target_os = "emscripten",
        not(target_feature = "atomics"),
        tokio_unstable,
        feature = "rt"
    ))]
    emscripten::main();
}
