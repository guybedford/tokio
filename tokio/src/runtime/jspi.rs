//! JSPI park primitive and timer plumbing over `emscripten::ffi`. A `block_on`
//! (or blocking API) that can't make progress suspends its calling stack on the
//! host JS event loop here, resumed by `setTimeout` wakes and external wakers.
//! The scheduler-coupled kernel lives at
//! `runtime/scheduler/current_thread/event_loop.rs`.

use crate::emscripten::ffi::{
    emscripten_clear_timeout, emscripten_promise_await, emscripten_promise_create,
    emscripten_promise_destroy, emscripten_promise_resolve, emscripten_runtime_keepalive_pop,
    emscripten_runtime_keepalive_push, emscripten_set_timeout, EmPromise, EM_PROMISE_FULFILL,
};

use std::cell::RefCell;
use std::ffi::c_void;

/// Polls per JSPI drive before yielding to the host, so a self-rewaking task
/// cannot starve the host loop. Amortizes the host-turn round-trip without
/// stalling a frame.
pub(crate) const HOST_DRIVE_BUDGET: u32 = 4096;

/// Whether this binary can park. JSPI is a link-time choice (`-sJSPI`); when
/// absent a would-suspend `block_on` panics rather than park, the only sound
/// semantics when the host loop can't run while wasm is on the stack.
pub(crate) fn jspi_linked() -> bool {
    unsafe { crate::emscripten::ffi::emscripten_has_asyncify() == 2 }
}

/// This thread's host-loop state, held in tokio's per-thread
/// [`runtime::context`](crate::runtime::context) rather than a parallel
/// thread-local.
pub(crate) struct JspiContext {
    parked: RefCell<Vec<Parked>>,
}

/// One JSPI-parked stack: the promise its `emscripten_promise_await` is
/// suspended on, and its deadline timer (`None` once fired or when deadline-less).
struct Parked {
    promise: EmPromise,
    timer: Option<i32>,
}

impl JspiContext {
    pub(crate) const fn new() -> JspiContext {
        JspiContext {
            parked: RefCell::new(Vec::new()),
        }
    }
}

fn with_host<R>(f: impl FnOnce(&JspiContext) -> R) -> R {
    crate::runtime::context::with_jspi(f)
}

/// Resolve every parked stack's promise. Each resumes as a microtask, re-checks
/// its fixed point, and re-parks if its wake hasn't arrived. Re-resolving a
/// settled promise is a no-op. Never runs tasks on the caller's stack, so
/// `Waker::wake` stays O(1).
pub(crate) fn unpark_all() {
    with_host(|h| {
        for parked in h.parked.borrow().iter() {
            // SAFETY: the handle lives until its stack resumes and destroys it,
            // which can't happen while this callback runs.
            unsafe {
                emscripten_promise_resolve(parked.promise, EM_PROMISE_FULFILL, std::ptr::null_mut())
            };
        }
    });
}

/// A parked stack's deadline timer fired: settle its promise. Forget the spent
/// timer id so the resume path won't clear it or double-release its keepalive ref.
unsafe extern "C-unwind" fn park_timeout_entry(promise: *mut c_void) {
    with_host(|h| {
        if let Some(parked) = h
            .parked
            .borrow_mut()
            .iter_mut()
            .find(|p| p.promise == promise)
        {
            parked.timer = None;
        }
    });
    unsafe { emscripten_promise_resolve(promise, EM_PROMISE_FULFILL, std::ptr::null_mut()) };
}

/// Suspend the current drive on the host event loop via JSPI until a wake
/// ([`unpark_all`]) or `timeout_ms` elapses (negative = no timer). The kernel's
/// park primitive: called at a `block_on` cliff (after checking its core back
/// in) and by `ParkThread::park` for the blocking APIs.
///
/// Across the suspension the per-stack runtime-entered flag is swapped out, so
/// reentrant host callbacks can drive runtimes (including the suspended one's,
/// whose core is back in its slot) as if this stack weren't there. Restored on
/// resume, which runs as a microtask: never mid-fixed-point.
pub(crate) fn park_on_host(timeout_ms: f64) {
    struct ParkGuard {
        prev_enter: crate::runtime::context::EnterRuntime,
    }
    impl Drop for ParkGuard {
        fn drop(&mut self) {
            crate::runtime::context::jspi_restore_runtime_after_park(self.prev_enter);
            // SAFETY: paired with the push below.
            unsafe { emscripten_runtime_keepalive_pop() };
        }
    }

    let promise = unsafe { emscripten_promise_create() };
    let timer = if timeout_ms >= 0.0 {
        // SAFETY: the resume path clears the timer (or it has fired) before the
        // promise is destroyed, so the callback's `user_data` never dangles.
        Some(unsafe { emscripten_set_timeout(Some(park_timeout_entry), timeout_ms, promise) })
    } else {
        None
    };
    with_host(|h| h.parked.borrow_mut().push(Parked { promise, timer }));

    {
        // A suspended stack is pending work emscripten can't see: under
        // `EXIT_RUNTIME` with no keepalive, a managed callback firing
        // mid-suspension would `maybeExit` and tear the runtime down under us.
        // Hold a keepalive ref for the suspension's lifetime.
        unsafe { emscripten_runtime_keepalive_push() };
        let _guard = ParkGuard {
            prev_enter: crate::runtime::context::jspi_exit_runtime_for_park(),
        };
        // SAFETY: suspends this stack until the promise settles.
        let _ = unsafe { emscripten_promise_await(promise) };
    }

    // Deregister and tear down. JS is run-to-completion, so the timer can't fire
    // between the resume microtask and here.
    let parked = with_host(|h| {
        let mut parked = h.parked.borrow_mut();
        let i = parked
            .iter()
            .position(|p| p.promise == promise)
            .expect("parked entry present at resume");
        parked.remove(i)
    });
    if let Some(timer) = parked.timer {
        clear_timeout(timer);
    }
    unsafe { emscripten_promise_destroy(promise) };
}

/// Cancel an armed host timeout, releasing its keepalive ref. `clearTimeout`
/// alone never releases the ref `emscripten_set_timeout` took, so the explicit
/// pop is needed to avoid pinning the instance alive under `EXIT_RUNTIME`.
fn clear_timeout(id: i32) {
    unsafe {
        emscripten_clear_timeout(id);
        emscripten_runtime_keepalive_pop();
    }
}
